/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "adnl/utils.hpp"
#include "auto/tl/ton_api.h"
#include "block/block.h"
#include "block/validator-set.h"
#include "consensus/simplex/bus.h"
#include "consensus/utils.h"
#include "td/actor/BusRuntime.h"
#include "td/actor/coro_utils.h"
#include "td/db/MemoryKeyValue.h"
#include "td/utils/OptionParser.h"
#include "td/utils/Random.h"
#include "td/utils/port/signals.h"

#include "block-auto.h"

using namespace ton;
using namespace ton::validator;
using namespace ton::validator::consensus;

namespace {
td::Bits256 from_hex(td::Slice s) {
  td::Bits256 x;
  CHECK(x.from_hex(s) == 256);
  return x;
}

td::Ref<vm::Cell> gen_shard_state(BlockSeqno seqno) {
  return vm::CellBuilder().store_long(0xabcdabcdU, 32).store_long(seqno, 32).finalize_novm();
}

td::Result<std::pair<double, double>> parse_range(td::Slice s) {
  auto pos = s.find(':');
  if (pos == td::Slice::npos) {
    double x = td::to_double(s);
    return std::make_pair(x, x);
  }
  double x = td::to_double(s.substr(0, pos));
  double y = td::to_double(s.substr(pos + 1, s.size()));
  if (x > y) {
    return td::Status::Error(PSTRING() << "invalid range " << s);
  }
  return std::make_pair(x, y);
}

template <typename T>
td::Result<std::pair<T, T>> parse_int_range(td::Slice s) {
  auto pos = s.find(':');
  if (pos == td::Slice::npos) {
    TRY_RESULT(x, td::to_integer_safe<T>(s));
    return std::make_pair(x, x);
  }
  TRY_RESULT(x, td::to_integer_safe<T>(s.substr(0, pos)));
  TRY_RESULT(y, td::to_integer_safe<T>(s.substr(pos + 1, s.size())));
  if (x > y) {
    return td::Status::Error(PSTRING() << "invalid range " << s);
  }
  return std::make_pair(x, y);
}

enum class TestCase {
  Smoke,
  NoDoubleNotar,
  CandidateResolutionRecovery,
  NotarRequiresParentNotar,
  EmptyCandidatesConsensus,
  StandstillRebroadcastContents,
  FinalRequiresOwnNotar,
  NoSkipFinalConflict,
  CertificateRequiresQuorum,
  CertificateRebroadcast,
  FinalRequiresObservedNotar,
  LeaderWindowSchedule,
  NoMisbehaviorReports,
};

td::Result<TestCase> parse_test_case(td::Slice s) {
  if (s == "smoke") {
    return TestCase::Smoke;
  }
  if (s == "no-double-notar") {
    return TestCase::NoDoubleNotar;
  }
  if (s == "candidate-resolution-recovery") {
    return TestCase::CandidateResolutionRecovery;
  }
  if (s == "notar-requires-parent-notar") {
    return TestCase::NotarRequiresParentNotar;
  }
  if (s == "empty-candidates-consensus") {
    return TestCase::EmptyCandidatesConsensus;
  }
  if (s == "standstill-rebroadcast-contents") {
    return TestCase::StandstillRebroadcastContents;
  }
  if (s == "final-requires-own-notar") {
    return TestCase::FinalRequiresOwnNotar;
  }
  if (s == "no-skip-final-conflict") {
    return TestCase::NoSkipFinalConflict;
  }
  if (s == "certificate-requires-quorum") {
    return TestCase::CertificateRequiresQuorum;
  }
  if (s == "certificate-rebroadcast") {
    return TestCase::CertificateRebroadcast;
  }
  if (s == "final-requires-observed-notar") {
    return TestCase::FinalRequiresObservedNotar;
  }
  if (s == "leader-window-schedule") {
    return TestCase::LeaderWindowSchedule;
  }
  if (s == "no-misbehavior-reports") {
    return TestCase::NoMisbehaviorReports;
  }
  return td::Status::Error(PSTRING() << "unknown test case " << s);
}

Ref<vm::Cell> make_ext_blk_ref(BlockIdExt block_id, LogicalTime lt) {
  vm::CellBuilder cb;
  cb.store_long_bool(lt, 64);
  cb.store_long_bool(block_id.seqno(), 32);
  cb.store_bits_bool(block_id.root_hash);
  cb.store_bits_bool(block_id.file_hash);
  return cb.finalize_novm();
}

CatchainSeqno CC_SEQNO = 123;
BlockIdExt MIN_MC_BLOCK_ID{masterchainId, shardIdAll, 0,
                           from_hex("AAAAAAAABBBBBBBBCCCCCCCCDDDDDDDDAAAAAAAABBBBBBBBCCCCCCCCDDDDDDDD"),
                           from_hex("0123456012345601234560123456012345601234560123456777777701234567")};
td::Bits256 SESSION_ID = from_hex("00001234000012340000123400001234aaaaaaaabbbbbbbbcccccccceeeeeeee");

ShardIdFull SHARD{basechainId, shardIdAll};
BlockIdExt FIRST_PARENT{basechainId, shardIdAll, 0, td::Bits256(gen_shard_state(0)->get_hash().bits()),
                        from_hex("89abcde89abcde89abcde89abcde89abcde89abcde89abcdefffffff89abcdef")};

std::pair<double, double> NET_PING = {0.05, 0.1};
double NET_LOSS = 0.0;

size_t N_NODES = 8;
size_t N_DOUBLE_NODES = 0;

double DURATION = 60.0;
td::uint32 TARGET_RATE_MS = 1000;
td::uint32 SLOTS_PER_LEADER_WINDOW = 4;
TestCase TEST_CASE = TestCase::Smoke;
double STANDSTILL_TIMEOUT_S = 10.0;

std::pair<double, double> GREMLIN_PERIOD = {-1.0, -1.0};
std::pair<double, double> GREMLIN_DOWNTIME = {1.0, 1.0};
std::pair<size_t, size_t> GREMLIN_N = {1, 1};
size_t GREMLIN_TIMES = 1000000000;
bool GREMLIN_KILLS_LEADER = false;

std::pair<double, double> NET_GREMLIN_PERIOD = {-1.0, -1.0};
std::pair<double, double> NET_GREMLIN_DOWNTIME = {10.0, 10.0};
std::pair<size_t, size_t> NET_GREMLIN_N = {1, 1};
size_t NET_GREMLIN_TIMES = 1000000000;
bool NET_GREMLIN_KILLS_LEADER = false;

std::pair<double, double> DB_DELAY = {0.0, 0.0};
std::pair<double, double> COLLATION_TIME = {0.0, 0.0};
std::pair<double, double> VALIDATION_TIME = {0.0, 0.0};

struct TestMessageFilters {
  std::optional<size_t> drop_final_cert_dst_node;
  std::optional<td::uint32> drop_final_cert_from_slot;
  std::optional<size_t> drop_candidate_dst_node;
  std::optional<td::uint32> drop_candidate_start_slot;
  std::optional<td::uint32> drop_candidate_end_slot;
  std::optional<size_t> force_candidate_request_src_node;
  std::optional<size_t> force_candidate_request_src_instance;
  std::optional<size_t> force_candidate_request_dst_node;
};

std::mutex test_message_filters_mutex;
TestMessageFilters test_message_filters;

struct TestExpectations {
  std::optional<CandidateId> candidate_resolution_target;
};

std::mutex test_expectations_mutex;
TestExpectations test_expectations;

struct ProtocolVoteTraceRecord {
  double ts = 0.0;
  size_t src_node_idx = 0;
  size_t src_instance_idx = 0;
  std::optional<size_t> dst_node_idx;
  simplex::Vote vote;
};

struct ProtocolCertificateTraceRecord {
  double ts = 0.0;
  size_t src_node_idx = 0;
  size_t src_instance_idx = 0;
  std::optional<size_t> dst_node_idx;
  simplex::Vote vote;
  std::vector<PeerValidatorId> signers;
};

struct OverlayRequestTraceRecord {
  double ts = 0.0;
  size_t src_node_idx = 0;
  size_t src_instance_idx = 0;
  size_t dst_node_idx = 0;
  double timeout_s = 0.0;
  std::optional<CandidateId> candidate_id;
  bool want_candidate = false;
  bool want_notar = false;
};

struct CandidateDeliveryTraceRecord {
  double ts = 0.0;
  size_t src_node_idx = 0;
  size_t src_instance_idx = 0;
  size_t dst_node_idx = 0;
  size_t dst_instance_idx = 0;
  CandidateId candidate_id;
  ParentId parent_id;
  PeerValidatorId leader;
  bool is_empty = false;
  BlockIdExt block_id;
};

struct LeaderWindowObservedTraceRecord {
  double ts = 0.0;
  size_t node_idx = 0;
  size_t instance_idx = 0;
  td::uint32 start_slot = 0;
  ParentId base;
};

struct LeaderWindowStartedTraceRecord {
  double ts = 0.0;
  size_t node_idx = 0;
  size_t instance_idx = 0;
  td::uint32 start_slot = 0;
  td::uint32 end_slot = 0;
  ParentId base;
};

struct NotarizationObservedTraceRecord {
  double ts = 0.0;
  size_t node_idx = 0;
  size_t instance_idx = 0;
  CandidateId id;
  std::vector<PeerValidatorId> signers;
};

struct FinalizationObservedTraceRecord {
  double ts = 0.0;
  size_t node_idx = 0;
  size_t instance_idx = 0;
  CandidateId id;
  std::vector<PeerValidatorId> signers;
};

struct CandidateGeneratedTraceRecord {
  double ts = 0.0;
  size_t node_idx = 0;
  size_t instance_idx = 0;
  CandidateId candidate_id;
  ParentId parent_id;
  PeerValidatorId leader;
  bool is_empty = false;
  BlockIdExt block_id;
};

struct AcceptedBlockTraceRecord {
  double ts = 0.0;
  size_t node_idx = 0;
  size_t instance_idx = 0;
  BlockIdExt block_id;
};

struct MisbehaviorTraceRecord {
  double ts = 0.0;
  size_t node_idx = 0;
  size_t instance_idx = 0;
  PeerValidatorId offender;
};

struct NetworkToggleTraceRecord {
  double ts = 0.0;
  size_t node_idx = 0;
  size_t instance_idx = 0;
  bool disabled = false;
};

struct LifecycleTraceRecord {
  double ts = 0.0;
  size_t node_idx = 0;
  size_t instance_idx = 0;
  bool started = false;
};

struct CandidateResolvedTraceRecord {
  double ts = 0.0;
  size_t node_idx = 0;
  size_t instance_idx = 0;
  CandidateId id;
};

struct TraceSnapshot {
  std::vector<ProtocolVoteTraceRecord> protocol_votes;
  std::vector<ProtocolCertificateTraceRecord> protocol_certificates;
  std::vector<OverlayRequestTraceRecord> overlay_requests;
  std::vector<CandidateDeliveryTraceRecord> candidate_deliveries;
  std::vector<LeaderWindowObservedTraceRecord> leader_windows_observed;
  std::vector<LeaderWindowStartedTraceRecord> leader_windows_started;
  std::vector<NotarizationObservedTraceRecord> notarizations_observed;
  std::vector<FinalizationObservedTraceRecord> finalizations_observed;
  std::vector<CandidateGeneratedTraceRecord> candidates_generated;
  std::vector<AcceptedBlockTraceRecord> accepted_blocks;
  std::vector<MisbehaviorTraceRecord> misbehavior_reports;
  std::vector<NetworkToggleTraceRecord> network_toggles;
  std::vector<LifecycleTraceRecord> lifecycle;
  std::vector<CandidateResolvedTraceRecord> candidates_resolved;
};

std::string certificate_trace_key(const ProtocolCertificateTraceRecord& record);
std::string vote_trace_key(const simplex::Vote& vote);

td::Status verify_no_double_notar(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md §1.4 local Notar uniqueness and Lemma 2.2:
  // an honest validator must never cast Notar(s, h1) and Notar(s, h2) for the same slot with h1 != h2.
  std::map<std::pair<size_t, td::uint32>, CandidateId> first_vote_by_validator_and_slot;
  for (const auto& record : snapshot.protocol_votes) {
    auto* notar = std::get_if<simplex::NotarizeVote>(&record.vote.vote);
    if (notar == nullptr) {
      continue;
    }
    auto key = std::make_pair(record.src_node_idx, notar->id.slot);
    auto [it, inserted] = first_vote_by_validator_and_slot.emplace(key, notar->id);
    if (!inserted && it->second != notar->id) {
      return td::Status::Error(PSTRING() << "validator #" << record.src_node_idx
                                         << " sent conflicting Notar votes for slot " << notar->id.slot
                                         << ": first=" << it->second << ", later=" << notar->id);
    }
  }
  return td::Status::OK();
}

td::Status verify_candidate_resolution_recovery(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md Rule 2:
  // after restart and local data loss, a node retries requestCandidate with exponential backoff,
  // then successfully resolves the requested notarized candidate once a serving peer returns.
  std::optional<CandidateId> expected_target;
  {
    std::scoped_lock lock(test_expectations_mutex);
    expected_target = test_expectations.candidate_resolution_target;
  }
  if (!expected_target.has_value()) {
    return td::Status::Error("recovery scenario did not register the target candidate id");
  }

  std::vector<OverlayRequestTraceRecord> requests;
  for (const auto& record : snapshot.overlay_requests) {
    if (record.src_node_idx == 0 && record.src_instance_idx == 0 && record.candidate_id == expected_target) {
      requests.push_back(record);
    }
  }
  if (requests.empty()) {
    return td::Status::Error(PSTRING() << "recovery scenario did not produce requestCandidate traffic for target "
                                       << *expected_target);
  }
  if (requests.size() < 3) {
    return td::Status::Error(PSTRING() << "recovery scenario did not retry target " << *expected_target
                                       << " enough times to show backoff");
  }
  std::sort(requests.begin(), requests.end(), [](const auto& a, const auto& b) { return a.ts < b.ts; });
  if (requests.front().timeout_s < 0.35 || requests.front().timeout_s > 0.65) {
    return td::Status::Error(PSTRING() << "unexpected initial requestCandidate timeout for target "
                                       << *expected_target << ": " << requests.front().timeout_s << "s");
  }
  bool saw_backoff = false;
  double prev_timeout = requests.front().timeout_s;
  for (size_t i = 1; i < requests.size(); ++i) {
    double min_expected_timeout = std::min(prev_timeout * 1.35, 30.0);
    if (requests[i].timeout_s + 0.05 < min_expected_timeout) {
      return td::Status::Error(PSTRING() << "requestCandidate timeout for target " << *expected_target
                                         << " failed to back off from " << prev_timeout << "s to at least "
                                         << min_expected_timeout << "s, got " << requests[i].timeout_s
                                         << "s");
    }
    saw_backoff = saw_backoff || requests[i].timeout_s > prev_timeout + 1e-9;
    prev_timeout = requests[i].timeout_s;
  }
  if (!saw_backoff) {
    return td::Status::Error(PSTRING() << "recovery scenario did not show exponential requestCandidate backoff for "
                                       << *expected_target);
  }

  std::optional<CandidateResolvedTraceRecord> resolved;
  for (const auto& record : snapshot.candidates_resolved) {
    if (record.node_idx == 0 && record.instance_idx == 0) {
      resolved = record;
      break;
    }
  }
  if (!resolved.has_value()) {
    return td::Status::Error(PSTRING() << "recovery scenario showed backoff for candidate "
                                       << *expected_target
                                       << " but validator #0.0 never resolved any candidate after restart; saw "
                                       << requests.size() << " request(s) for the target");
  }
  if (resolved->id != *expected_target) {
    return td::Status::Error(PSTRING() << "recovery scenario resolved " << resolved->id << " instead of target "
                                       << *expected_target);
  }

  std::optional<double> serving_peer_restart_ts;
  for (const auto& record : snapshot.lifecycle) {
    if (record.node_idx == 3 && record.instance_idx == 0 && record.started) {
      serving_peer_restart_ts = record.ts;
      break;
    }
  }
  if (!serving_peer_restart_ts.has_value()) {
    return td::Status::Error("recovery scenario did not record the restart of the only full-data peer");
  }
  if (resolved->ts <= *serving_peer_restart_ts) {
    return td::Status::Error("recovery scenario resolved state before the full-data peer restarted");
  }

  size_t requests_before_serving_peer_restart = 0;
  for (size_t i = 0; i < requests.size(); ++i) {
    if (requests[i].ts < *serving_peer_restart_ts) {
      ++requests_before_serving_peer_restart;
    }
  }
  if (requests_before_serving_peer_restart < 2) {
    return td::Status::Error("recovery scenario did not retry while the full-data peer was still offline");
  }

  return td::Status::OK();
}

td::Status verify_notar_requires_parent_notar(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md Rule 4(2):
  // even if a child candidate arrives before its parent is notarized, an honest validator must
  // not vote Notar for the child until it has observed the parent's notarization certificate.
  std::map<std::tuple<size_t, size_t, CandidateId>, double> first_candidate_available_ts;
  std::map<CandidateId, ParentId> parent_by_candidate;
  for (const auto& record : snapshot.candidates_generated) {
    auto key = std::make_tuple(record.node_idx, record.instance_idx, record.candidate_id);
    auto [it, inserted] = first_candidate_available_ts.emplace(key, record.ts);
    if (!inserted) {
      it->second = std::min(it->second, record.ts);
    }
    parent_by_candidate.emplace(record.candidate_id, record.parent_id);
  }
  for (const auto& record : snapshot.candidate_deliveries) {
    auto key = std::make_tuple(record.dst_node_idx, record.dst_instance_idx, record.candidate_id);
    auto [it, inserted] = first_candidate_available_ts.emplace(key, record.ts);
    if (!inserted) {
      it->second = std::min(it->second, record.ts);
    }
    parent_by_candidate.emplace(record.candidate_id, record.parent_id);
  }

  std::map<std::tuple<size_t, size_t, CandidateId>, double> first_parent_notar_ts;
  for (const auto& record : snapshot.notarizations_observed) {
    auto key = std::make_tuple(record.node_idx, record.instance_idx, record.id);
    auto [it, inserted] = first_parent_notar_ts.emplace(key, record.ts);
    if (!inserted) {
      it->second = std::min(it->second, record.ts);
    }
  }

  bool saw_candidate_before_parent_notar = false;
  bool saw_checked_child_vote = false;
  for (const auto& record : snapshot.protocol_votes) {
    auto* notar = std::get_if<simplex::NotarizeVote>(&record.vote.vote);
    if (notar == nullptr) {
      continue;
    }
    auto parent_it = parent_by_candidate.find(notar->id);
    if (parent_it == parent_by_candidate.end() || !parent_it->second.has_value()) {
      continue;
    }
    saw_checked_child_vote = true;
    CandidateId parent_id = *parent_it->second;
    auto parent_key = std::make_tuple(record.src_node_idx, record.src_instance_idx, parent_id);
    auto notar_it = first_parent_notar_ts.find(parent_key);
    if (notar_it == first_parent_notar_ts.end()) {
      return td::Status::Error(PSTRING() << "validator #" << record.src_node_idx << "." << record.src_instance_idx
                                         << " voted Notar for child " << notar->id
                                         << " without observing parent notarization for " << parent_id);
    }
    if (notar_it->second > record.ts + 1e-9) {
      return td::Status::Error(PSTRING() << "validator #" << record.src_node_idx << "." << record.src_instance_idx
                                         << " voted Notar for child " << notar->id
                                         << " before observing parent notarization for " << parent_id
                                         << ": parent_notar_ts=" << notar_it->second
                                         << ", child_notar_ts=" << record.ts);
    }

    auto availability_key = std::make_tuple(record.src_node_idx, record.src_instance_idx, notar->id);
    auto available_it = first_candidate_available_ts.find(availability_key);
    if (available_it != first_candidate_available_ts.end() && available_it->second < notar_it->second - 1e-9) {
      saw_candidate_before_parent_notar = true;
    }
  }

  if (!saw_checked_child_vote) {
    return td::Status::Error("scenario did not produce any child-candidate Notar votes");
  }
  if (!saw_candidate_before_parent_notar) {
    return td::Status::Error("scenario did not deliver any child candidate before its parent notarization");
  }
  return td::Status::OK();
}

td::Status verify_empty_candidates_consensus(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md §4.4:
  // empty candidates are still notarized and finalized, they point to an already-accepted block,
  // and consensus continues past the empty slot.
  std::map<CandidateId, BlockIdExt> empty_block_by_candidate;
  for (const auto& record : snapshot.candidates_generated) {
    if (record.is_empty) {
      empty_block_by_candidate.emplace(record.candidate_id, record.block_id);
    }
  }
  if (empty_block_by_candidate.empty()) {
    return td::Status::Error("scenario did not produce any empty candidates");
  }

  std::set<CandidateId> notarized_empty_candidates;
  for (const auto& record : snapshot.notarizations_observed) {
    if (empty_block_by_candidate.contains(record.id)) {
      notarized_empty_candidates.insert(record.id);
    }
  }
  if (notarized_empty_candidates.empty()) {
    return td::Status::Error("no generated empty candidate was notarized");
  }

  std::map<CandidateId, double> finalized_empty_ts;
  for (const auto& record : snapshot.finalizations_observed) {
    if (notarized_empty_candidates.contains(record.id)) {
      auto [it, inserted] = finalized_empty_ts.emplace(record.id, record.ts);
      if (!inserted) {
        it->second = std::min(it->second, record.ts);
      }
    }
  }
  if (finalized_empty_ts.empty()) {
    return td::Status::Error("no notarized empty candidate was finalized");
  }

  std::map<BlockIdExt, double> first_accepted_ts_by_block;
  for (const auto& record : snapshot.accepted_blocks) {
    auto [it, inserted] = first_accepted_ts_by_block.emplace(record.block_id, record.ts);
    if (!inserted) {
      it->second = std::min(it->second, record.ts);
    }
  }

  bool saw_later_finalized_descendant = false;
  for (const auto& [empty_id, finalization_ts] : finalized_empty_ts) {
    auto block_it = empty_block_by_candidate.find(empty_id);
    CHECK(block_it != empty_block_by_candidate.end());
    auto accepted_it = first_accepted_ts_by_block.find(block_it->second);
    if (accepted_it == first_accepted_ts_by_block.end()) {
      return td::Status::Error(PSTRING() << "referenced block " << block_it->second.to_str()
                                         << " for finalized empty candidate " << empty_id
                                         << " was never accepted");
    }
    if (accepted_it->second > finalization_ts + 1e-9) {
      return td::Status::Error(PSTRING() << "empty candidate " << empty_id
                                         << " finalized before its referenced block " << block_it->second.to_str()
                                         << " was accepted");
    }
    for (const auto& record : snapshot.finalizations_observed) {
      if (record.id.slot > empty_id.slot && record.ts >= finalization_ts - 1e-9) {
        saw_later_finalized_descendant = true;
        break;
      }
    }
  }

  if (!saw_later_finalized_descendant) {
    return td::Status::Error("scenario did not finalize any descendant after an empty candidate");
  }
  return td::Status::OK();
}

td::Status verify_standstill_rebroadcast_contents(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md Rule 8:
  // after finalization stalls, a validator rebroadcasts its highest final cert, all later certs,
  // and all of its later votes on each standstill-resolution attempt.
  constexpr size_t node_idx = 0;
  constexpr size_t instance_idx = 0;

  double last_finalization_ts = -1.0;
  CandidateId last_finalized_id;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == node_idx && record.instance_idx == instance_idx && record.ts > last_finalization_ts) {
      last_finalization_ts = record.ts;
      last_finalized_id = record.id;
    }
  }
  if (last_finalization_ts < 0.0) {
    return td::Status::Error("scenario did not produce any finalization on validator #0.0");
  }

  double first_standstill_attempt_ts = last_finalization_ts + STANDSTILL_TIMEOUT_S * 0.75;

  bool saw_highest_final_cert_after_standstill = false;
  std::set<std::string> later_cert_keys_before_standstill;
  for (const auto& record : snapshot.protocol_certificates) {
    if (record.src_node_idx != node_idx || record.src_instance_idx != instance_idx) {
      continue;
    }
    auto* final = std::get_if<simplex::FinalizeVote>(&record.vote.vote);
    if (final != nullptr && final->id == last_finalized_id && record.ts >= first_standstill_attempt_ts) {
      saw_highest_final_cert_after_standstill = true;
    }
    if (record.ts < first_standstill_attempt_ts && record.vote.referenced_slot() > last_finalized_id.slot) {
      later_cert_keys_before_standstill.insert(certificate_trace_key(record));
    }
  }
  if (!saw_highest_final_cert_after_standstill) {
    return td::Status::Error(PSTRING() << "validator #0.0 did not rebroadcast its highest final certificate for "
                                       << last_finalized_id << " after standstill timeout");
  }

  std::set<std::string> later_vote_keys_before_standstill;
  for (const auto& record : snapshot.protocol_votes) {
    if (record.src_node_idx != node_idx || record.src_instance_idx != instance_idx) {
      continue;
    }
    if (record.ts < first_standstill_attempt_ts && record.vote.referenced_slot() > last_finalized_id.slot) {
      later_vote_keys_before_standstill.insert(vote_trace_key(record.vote));
    }
  }
  if (later_vote_keys_before_standstill.empty()) {
    return td::Status::Error("scenario did not produce any later own votes before the first standstill attempt");
  }

  std::set<std::string> rebroadcast_cert_keys;
  for (const auto& record : snapshot.protocol_certificates) {
    if (record.src_node_idx == node_idx && record.src_instance_idx == instance_idx &&
        record.ts >= first_standstill_attempt_ts) {
      rebroadcast_cert_keys.insert(certificate_trace_key(record));
    }
  }
  for (const auto& key : later_cert_keys_before_standstill) {
    if (!rebroadcast_cert_keys.contains(key)) {
      return td::Status::Error(PSTRING() << "validator #0.0 did not rebroadcast later certificate " << key
                                         << " after standstill timeout");
    }
  }

  std::set<std::string> rebroadcast_vote_keys;
  for (const auto& record : snapshot.protocol_votes) {
    if (record.src_node_idx == node_idx && record.src_instance_idx == instance_idx &&
        record.ts >= first_standstill_attempt_ts) {
      rebroadcast_vote_keys.insert(vote_trace_key(record.vote));
    }
  }
  for (const auto& key : later_vote_keys_before_standstill) {
    if (!rebroadcast_vote_keys.contains(key)) {
      return td::Status::Error(PSTRING() << "validator #0.0 did not rebroadcast later own vote " << key
                                         << " after standstill timeout");
    }
  }

  return td::Status::OK();
}

td::Status verify_finalize_requires_own_notar(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md Rule 5(1):
  // an honest validator may vote Final(s, h) only after it has voted Notar(s, h) itself.
  std::map<std::pair<size_t, CandidateId>, double> first_notar_ts;
  for (const auto& record : snapshot.protocol_votes) {
    auto* notar = std::get_if<simplex::NotarizeVote>(&record.vote.vote);
    if (notar == nullptr) {
      continue;
    }
    auto key = std::make_pair(record.src_node_idx, notar->id);
    auto [it, inserted] = first_notar_ts.emplace(key, record.ts);
    if (!inserted) {
      it->second = std::min(it->second, record.ts);
    }
  }

  for (const auto& record : snapshot.protocol_votes) {
    auto* final = std::get_if<simplex::FinalizeVote>(&record.vote.vote);
    if (final == nullptr) {
      continue;
    }
    auto key = std::make_pair(record.src_node_idx, final->id);
    auto it = first_notar_ts.find(key);
    if (it == first_notar_ts.end()) {
      return td::Status::Error(PSTRING() << "validator #" << record.src_node_idx
                                         << " sent Final without an own prior Notar for " << final->id);
    }
    if (it->second > record.ts + 1e-9) {
      return td::Status::Error(PSTRING() << "validator #" << record.src_node_idx
                                         << " sent Final before its own Notar for " << final->id
                                         << ": notar_ts=" << it->second << ", final_ts=" << record.ts);
    }
  }
  return td::Status::OK();
}

td::Status verify_no_skip_final_conflict(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md §1.4 and Rule 5(3):
  // an honest validator must never cast both Skip(s) and Final(s, h) for the same slot.
  std::map<size_t, std::set<td::uint32>> skip_slots_by_validator;
  std::map<size_t, std::set<td::uint32>> final_slots_by_validator;
  bool saw_skip_vote = false;
  bool saw_final_vote = false;

  for (const auto& record : snapshot.protocol_votes) {
    if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote)) {
      saw_skip_vote = true;
      skip_slots_by_validator[record.src_node_idx].insert(skip->slot);
      continue;
    }
    if (auto* final = std::get_if<simplex::FinalizeVote>(&record.vote.vote)) {
      saw_final_vote = true;
      final_slots_by_validator[record.src_node_idx].insert(final->id.slot);
    }
  }

  for (const auto& [validator, skip_slots] : skip_slots_by_validator) {
    auto final_it = final_slots_by_validator.find(validator);
    if (final_it == final_slots_by_validator.end()) {
      continue;
    }
    for (td::uint32 slot : skip_slots) {
      if (final_it->second.contains(slot)) {
        return td::Status::Error(PSTRING() << "validator #" << validator
                                           << " sent both Skip and Final for slot " << slot);
      }
    }
  }

  if (!saw_skip_vote) {
    return td::Status::Error("scenario did not produce any Skip votes");
  }
  if (!saw_final_vote) {
    return td::Status::Error("scenario did not produce any Final votes");
  }
  return td::Status::OK();
}

td::Status verify_certificate_requires_quorum(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md Rule 7 (certificate formation):
  // a certificate may be formed only after votes with total weight at least q have been received.
  constexpr td::uint64 validator_weight = 11;
  td::uint64 total_weight = validator_weight * N_NODES;
  td::uint64 quorum_weight = (2 * total_weight) / 3 + 1;

  if (snapshot.protocol_certificates.empty()) {
    return td::Status::Error("scenario did not produce any certificates");
  }

  for (const auto& record : snapshot.protocol_certificates) {
    std::set<size_t> unique_signers;
    for (PeerValidatorId signer : record.signers) {
      unique_signers.insert(signer.value());
    }
    td::uint64 signer_weight = validator_weight * unique_signers.size();
    if (signer_weight < quorum_weight) {
      return td::Status::Error(PSTRING() << "certificate for " << record.vote
                                         << " had insufficient signer weight: got=" << signer_weight
                                         << ", quorum=" << quorum_weight);
    }
  }
  return td::Status::OK();
}

std::string certificate_trace_key(const ProtocolCertificateTraceRecord& record) {
  std::vector<size_t> signers;
  for (PeerValidatorId signer : record.signers) {
    signers.push_back(signer.value());
  }
  std::sort(signers.begin(), signers.end());

  std::string key = PSTRING() << record.vote;
  for (size_t signer : signers) {
    key += PSTRING() << "|" << signer;
  }
  return key;
}

std::string vote_trace_key(const simplex::Vote& vote) {
  return PSTRING() << vote;
}

td::Status verify_certificate_rebroadcast(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md Rule 7 (certificate rebroadcast):
  // upon forming or receiving any certificate, a validator broadcasts it.
  if (snapshot.protocol_certificates.empty()) {
    return td::Status::Error("scenario did not produce any certificates");
  }

  std::map<std::string, std::set<size_t>> broadcasters_by_certificate;
  for (const auto& record : snapshot.protocol_certificates) {
    broadcasters_by_certificate[certificate_trace_key(record)].insert(record.src_node_idx);
  }

  for (const auto& [certificate, broadcasters] : broadcasters_by_certificate) {
    if (broadcasters.size() >= 2) {
      return td::Status::OK();
    }
  }
  return td::Status::Error("no certificate was broadcast by more than one validator");
}

td::Status verify_finalize_requires_observed_notar(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md Rule 5(2):
  // an honest validator may vote Final(s, h) only after it can prove Notar(s, h) is reached.
  std::map<std::tuple<size_t, size_t, CandidateId>, double> first_notarization_observed_ts;
  for (const auto& record : snapshot.notarizations_observed) {
    auto key = std::make_tuple(record.node_idx, record.instance_idx, record.id);
    auto [it, inserted] = first_notarization_observed_ts.emplace(key, record.ts);
    if (!inserted) {
      it->second = std::min(it->second, record.ts);
    }
  }

  bool saw_final_vote = false;
  for (const auto& record : snapshot.protocol_votes) {
    auto* final = std::get_if<simplex::FinalizeVote>(&record.vote.vote);
    if (final == nullptr) {
      continue;
    }
    saw_final_vote = true;
    auto key = std::make_tuple(record.src_node_idx, record.src_instance_idx, final->id);
    auto it = first_notarization_observed_ts.find(key);
    if (it == first_notarization_observed_ts.end()) {
      return td::Status::Error(PSTRING() << "validator #" << record.src_node_idx << "." << record.src_instance_idx
                                         << " sent Final without observing notarization for " << final->id);
    }
    if (it->second > record.ts + 1e-9) {
      return td::Status::Error(PSTRING() << "validator #" << record.src_node_idx << "." << record.src_instance_idx
                                         << " sent Final before observing notarization for " << final->id
                                         << ": notar_ts=" << it->second << ", final_ts=" << record.ts);
    }
  }

  if (!saw_final_vote) {
    return td::Status::Error("scenario did not produce any Final votes");
  }
  return td::Status::OK();
}

td::Status verify_leader_window_schedule(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md leader-window scheduling:
  // window k starts at slot kL, spans exactly L slots, and the designated leader is k mod n.
  if (snapshot.leader_windows_started.empty()) {
    return td::Status::Error("scenario did not produce any leader windows");
  }

  for (const auto& record : snapshot.leader_windows_started) {
    if (record.start_slot % SLOTS_PER_LEADER_WINDOW != 0) {
      return td::Status::Error(PSTRING() << "leader window started at non-boundary slot " << record.start_slot);
    }
    td::uint32 expected_end_slot = record.start_slot + SLOTS_PER_LEADER_WINDOW;
    if (record.end_slot != expected_end_slot) {
      return td::Status::Error(PSTRING() << "leader window [" << record.start_slot << ", " << record.end_slot
                                         << ") had wrong exclusive end slot, expected " << expected_end_slot);
    }

    size_t expected_leader = (record.start_slot / SLOTS_PER_LEADER_WINDOW) % N_NODES;
    if (record.node_idx != expected_leader) {
      return td::Status::Error(PSTRING() << "window starting at slot " << record.start_slot
                                         << " was announced by validator #" << record.node_idx
                                         << ", expected validator #" << expected_leader);
    }
  }
  return td::Status::OK();
}

td::Status verify_no_misbehavior_reports(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md honest-validator safety assumptions:
  // a normal honest run should not trigger local misbehavior proofs.
  if (snapshot.protocol_votes.empty()) {
    return td::Status::Error("scenario did not produce any votes");
  }
  if (!snapshot.misbehavior_reports.empty()) {
    const auto& report = snapshot.misbehavior_reports.front();
    return td::Status::Error(PSTRING() << "validator #" << report.node_idx << "." << report.instance_idx
                                       << " emitted a misbehavior report for validator #" << report.offender);
  }
  return td::Status::OK();
}

td::Status verify_test_case(const TraceSnapshot& snapshot) {
  switch (TEST_CASE) {
    case TestCase::Smoke:
      return td::Status::OK();
    case TestCase::NoDoubleNotar:
      return verify_no_double_notar(snapshot);
    case TestCase::CandidateResolutionRecovery:
      return verify_candidate_resolution_recovery(snapshot);
    case TestCase::NotarRequiresParentNotar:
      return verify_notar_requires_parent_notar(snapshot);
    case TestCase::EmptyCandidatesConsensus:
      return verify_empty_candidates_consensus(snapshot);
    case TestCase::StandstillRebroadcastContents:
      return verify_standstill_rebroadcast_contents(snapshot);
    case TestCase::FinalRequiresOwnNotar:
      return verify_finalize_requires_own_notar(snapshot);
    case TestCase::NoSkipFinalConflict:
      return verify_no_skip_final_conflict(snapshot);
    case TestCase::CertificateRequiresQuorum:
      return verify_certificate_requires_quorum(snapshot);
    case TestCase::CertificateRebroadcast:
      return verify_certificate_rebroadcast(snapshot);
    case TestCase::FinalRequiresObservedNotar:
      return verify_finalize_requires_observed_notar(snapshot);
    case TestCase::LeaderWindowSchedule:
      return verify_leader_window_schedule(snapshot);
    case TestCase::NoMisbehaviorReports:
      return verify_no_misbehavior_reports(snapshot);
  }
  UNREACHABLE();
}

class TestTraceSink : public td::actor::Actor {
 public:
  void record_protocol_vote(size_t src_node_idx, size_t src_instance_idx, std::optional<size_t> dst_node_idx,
                            simplex::Vote vote) {
    protocol_votes_.push_back(ProtocolVoteTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .src_node_idx = src_node_idx,
        .src_instance_idx = src_instance_idx,
        .dst_node_idx = dst_node_idx,
        .vote = std::move(vote),
    });
  }

  void record_protocol_certificate(size_t src_node_idx, size_t src_instance_idx, std::optional<size_t> dst_node_idx,
                                   simplex::Vote vote, std::vector<PeerValidatorId> signers) {
    protocol_certificates_.push_back(ProtocolCertificateTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .src_node_idx = src_node_idx,
        .src_instance_idx = src_instance_idx,
        .dst_node_idx = dst_node_idx,
        .vote = std::move(vote),
        .signers = std::move(signers),
    });
  }

  void record_overlay_request(size_t src_node_idx, size_t src_instance_idx, size_t dst_node_idx, double timeout_s,
                              std::optional<CandidateId> candidate_id, bool want_candidate, bool want_notar) {
    overlay_requests_.push_back(OverlayRequestTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .src_node_idx = src_node_idx,
        .src_instance_idx = src_instance_idx,
        .dst_node_idx = dst_node_idx,
        .timeout_s = timeout_s,
        .candidate_id = std::move(candidate_id),
        .want_candidate = want_candidate,
        .want_notar = want_notar,
    });
  }

  void record_candidate_delivery(size_t src_node_idx, size_t src_instance_idx, size_t dst_node_idx,
                                 size_t dst_instance_idx, CandidateId candidate_id, ParentId parent_id,
                                 PeerValidatorId leader, bool is_empty, BlockIdExt block_id) {
    candidate_deliveries_.push_back(CandidateDeliveryTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .src_node_idx = src_node_idx,
        .src_instance_idx = src_instance_idx,
        .dst_node_idx = dst_node_idx,
        .dst_instance_idx = dst_instance_idx,
        .candidate_id = std::move(candidate_id),
        .parent_id = std::move(parent_id),
        .leader = leader,
        .is_empty = is_empty,
        .block_id = std::move(block_id),
    });
  }

  void record_leader_window_observed(size_t node_idx, size_t instance_idx, td::uint32 start_slot, ParentId base) {
    leader_windows_observed_.push_back(LeaderWindowObservedTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .node_idx = node_idx,
        .instance_idx = instance_idx,
        .start_slot = start_slot,
        .base = std::move(base),
    });
  }

  void record_leader_window_started(size_t node_idx, size_t instance_idx, td::uint32 start_slot, td::uint32 end_slot,
                                    ParentId base) {
    leader_windows_started_.push_back(LeaderWindowStartedTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .node_idx = node_idx,
        .instance_idx = instance_idx,
        .start_slot = start_slot,
        .end_slot = end_slot,
        .base = std::move(base),
    });
  }

  void record_notarization_observed(size_t node_idx, size_t instance_idx, CandidateId id,
                                    std::vector<PeerValidatorId> signers) {
    notarizations_observed_.push_back(NotarizationObservedTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .node_idx = node_idx,
        .instance_idx = instance_idx,
        .id = std::move(id),
        .signers = std::move(signers),
    });
  }

  void record_finalization_observed(size_t node_idx, size_t instance_idx, CandidateId id,
                                    std::vector<PeerValidatorId> signers) {
    finalizations_observed_.push_back(FinalizationObservedTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .node_idx = node_idx,
        .instance_idx = instance_idx,
        .id = std::move(id),
        .signers = std::move(signers),
    });
  }

  void record_candidate_generated(size_t node_idx, size_t instance_idx, CandidateId candidate_id, ParentId parent_id,
                                  PeerValidatorId leader, bool is_empty, BlockIdExt block_id) {
    candidates_generated_.push_back(CandidateGeneratedTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .node_idx = node_idx,
        .instance_idx = instance_idx,
        .candidate_id = std::move(candidate_id),
        .parent_id = std::move(parent_id),
        .leader = leader,
        .is_empty = is_empty,
        .block_id = std::move(block_id),
    });
  }

  void record_accepted_block(size_t node_idx, size_t instance_idx, BlockIdExt block_id) {
    accepted_blocks_.push_back(AcceptedBlockTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .node_idx = node_idx,
        .instance_idx = instance_idx,
        .block_id = std::move(block_id),
    });
  }

  void record_misbehavior(size_t node_idx, size_t instance_idx, PeerValidatorId offender) {
    misbehavior_reports_.push_back(MisbehaviorTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .node_idx = node_idx,
        .instance_idx = instance_idx,
        .offender = offender,
    });
  }

  void record_network_toggle(size_t node_idx, size_t instance_idx, bool disabled) {
    network_toggles_.push_back(NetworkToggleTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .node_idx = node_idx,
        .instance_idx = instance_idx,
        .disabled = disabled,
    });
  }

  void record_lifecycle(size_t node_idx, size_t instance_idx, bool started) {
    lifecycle_.push_back(LifecycleTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .node_idx = node_idx,
        .instance_idx = instance_idx,
        .started = started,
    });
  }

  void record_candidate_resolved(size_t node_idx, size_t instance_idx, CandidateId id) {
    candidates_resolved_.push_back(CandidateResolvedTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .node_idx = node_idx,
        .instance_idx = instance_idx,
        .id = std::move(id),
    });
  }

  void clear() {
    protocol_votes_.clear();
    protocol_certificates_.clear();
    overlay_requests_.clear();
    candidate_deliveries_.clear();
    leader_windows_observed_.clear();
    leader_windows_started_.clear();
    notarizations_observed_.clear();
    finalizations_observed_.clear();
    candidates_generated_.clear();
    accepted_blocks_.clear();
    misbehavior_reports_.clear();
    network_toggles_.clear();
    lifecycle_.clear();
    candidates_resolved_.clear();
  }

  td::actor::Task<TraceSnapshot> snapshot() {
    co_return TraceSnapshot{
        .protocol_votes = protocol_votes_,
        .protocol_certificates = protocol_certificates_,
        .overlay_requests = overlay_requests_,
        .candidate_deliveries = candidate_deliveries_,
        .leader_windows_observed = leader_windows_observed_,
        .leader_windows_started = leader_windows_started_,
        .notarizations_observed = notarizations_observed_,
        .finalizations_observed = finalizations_observed_,
        .candidates_generated = candidates_generated_,
        .accepted_blocks = accepted_blocks_,
        .misbehavior_reports = misbehavior_reports_,
        .network_toggles = network_toggles_,
        .lifecycle = lifecycle_,
        .candidates_resolved = candidates_resolved_,
    };
  }

 private:
  std::vector<ProtocolVoteTraceRecord> protocol_votes_;
  std::vector<ProtocolCertificateTraceRecord> protocol_certificates_;
  std::vector<OverlayRequestTraceRecord> overlay_requests_;
  std::vector<CandidateDeliveryTraceRecord> candidate_deliveries_;
  std::vector<LeaderWindowObservedTraceRecord> leader_windows_observed_;
  std::vector<LeaderWindowStartedTraceRecord> leader_windows_started_;
  std::vector<NotarizationObservedTraceRecord> notarizations_observed_;
  std::vector<FinalizationObservedTraceRecord> finalizations_observed_;
  std::vector<CandidateGeneratedTraceRecord> candidates_generated_;
  std::vector<AcceptedBlockTraceRecord> accepted_blocks_;
  std::vector<MisbehaviorTraceRecord> misbehavior_reports_;
  std::vector<NetworkToggleTraceRecord> network_toggles_;
  std::vector<LifecycleTraceRecord> lifecycle_;
  std::vector<CandidateResolvedTraceRecord> candidates_resolved_;
};

class TestSimplexBus : public simplex::Bus {
 public:
  using Parent = simplex::Bus;
  size_t instance_idx = 0;
  td::actor::ActorId<TestTraceSink> trace_sink;
};

class TestSimplexDb : public td::actor::SpawnsWith<simplex::Bus>, public td::actor::ConnectsTo<simplex::Bus> {
 public:
  using BusHandle = simplex::BusHandle;

  TON_RUNTIME_DEFINE_EVENT_HANDLER();

  explicit TestSimplexDb(simplex::Bus& bus) {
    init_pool_state(bus);
    init_votes(bus);
  }

  template <>
  void handle(BusHandle, std::shared_ptr<const StopRequested>) {
    stop();
  }

  template <>
  td::actor::Task<> process(BusHandle, std::shared_ptr<simplex::BroadcastVote> event) {
    auto vote = event->vote.to_tl();
    auto hash = sha256_bits256(serialize_tl_object(vote, true));

    if (saved_votes_.contains(hash)) {
      co_return td::Status::Error(cancelled, "Vote was already casted");
    }
    saved_votes_.insert(hash);

    auto key = create_serialize_tl_object<ton_api::consensus_simplex_db_key_vote>(hash);
    auto value =
        create_serialize_tl_object<ton_api::consensus_simplex_db_ourVote>(std::move(vote), next_seqno_++);
    co_return co_await owning_bus()->db->set(std::move(key), std::move(value));
  }

  template <>
  td::actor::Task<> process(BusHandle, std::shared_ptr<simplex::SaveCertificate> event) {
    auto cert = event->cert->to_tl();
    auto hash = sha256_bits256(serialize_tl_object(cert, true));

    if (saved_votes_.contains(hash)) {
      co_return td::Status::Error(cancelled, "Certificate was already saved");
    }
    saved_votes_.insert(hash);

    auto key = create_serialize_tl_object<ton_api::consensus_simplex_db_key_vote>(hash);
    auto value = create_serialize_tl_object<ton_api::consensus_simplex_db_cert>(std::move(cert));
    co_return co_await owning_bus()->db->set(std::move(key), std::move(value));
  }

  template <>
  td::actor::Task<> process(BusHandle, std::shared_ptr<simplex::LeaderWindowObserved> event) {
    auto window = event->start_slot / owning_bus()->simplex_config.slots_per_leader_window;
    CHECK(first_nonannounced_window_ <= window);
    first_nonannounced_window_ = window + 1;

    auto value =
        create_serialize_tl_object<ton_api::consensus_simplex_db_poolState>(first_nonannounced_window_);
    co_return co_await owning_bus()->db->set(pool_state_key_.clone(), std::move(value));
  }

 private:
  void init_pool_state(simplex::Bus& bus) {
    auto pool_state_str = bus.db->get(pool_state_key_);
    if (pool_state_str.has_value()) {
      auto pool_state =
          fetch_tl_object<ton_api::consensus_simplex_db_poolState>(*pool_state_str, true).move_as_ok();
      first_nonannounced_window_ = pool_state->first_nonannounced_window_;
      bus.first_nonannounced_window = first_nonannounced_window_;
    }
  }

  void init_votes(simplex::Bus& bus) {
    struct OurVote {
      td::int64 seqno;
      simplex::Vote vote;

      std::strong_ordering operator<=>(const OurVote& other) const {
        return seqno <=> other.seqno;
      }
    };

    std::vector<OurVote> our_votes;
    std::vector<simplex::CertificateRef<simplex::Vote>> certs;

    auto votes = bus.db->get_by_prefix(ton_api::consensus_simplex_db_key_vote::ID);
    for (auto& [key_str, value_str] : votes) {
      auto key = fetch_tl_object<ton_api::consensus_simplex_db_key_vote>(key_str, true).move_as_ok();
      saved_votes_.insert(key->vote_hash_);

      auto value = fetch_tl_object<ton_api::consensus_simplex_db_Vote>(value_str, true).move_as_ok();

      auto legacy_fn = [&](ton_api::consensus_simplex_db_voteLegacy& vote) {
        if (static_cast<size_t>(vote.node_idx_) == bus.local_id.idx.value()) {
          auto signed_vote =
              simplex::Signed<simplex::Vote>::deserialize(vote.data_, bus.local_id.idx, bus).move_as_ok();
          our_votes.push_back(OurVote{-1, signed_vote.vote});
        }
      };
      auto our_vote_fn = [&](ton_api::consensus_simplex_db_ourVote& vote) {
        our_votes.push_back(OurVote{vote.seqno_, simplex::Vote::from_tl(*vote.vote_)});
      };
      auto cert_fn = [&](ton_api::consensus_simplex_db_cert& vote) {
        certs.push_back(simplex::Certificate<simplex::Vote>::from_tl(std::move(*vote.cert_), bus).move_as_ok());
      };
      ton_api::downcast_call(*value, td::overloaded(legacy_fn, our_vote_fn, cert_fn));
    }

    auto notar_certs = bus.db->get_by_prefix(ton_api::consensus_simplex_db_key_candidateResolver_notarCert::ID);
    for (auto& [key_str, value_str] : notar_certs) {
      auto key =
          fetch_tl_object<ton_api::consensus_simplex_db_key_candidateResolver_notarCert>(key_str, true).move_as_ok();
      CandidateId id = CandidateId::from_tl(key->candidateId_);

      auto value =
          fetch_tl_object<ton_api::consensus_simplex_db_candidateResolver_notarCert>(value_str, true).move_as_ok();
      auto cert =
          simplex::NotarCert::from_tl(std::move(*value->notar_), simplex::NotarizeVote{id}, bus).move_as_ok();
      certs.push_back(std::move(cert.unique_write()).consume_and_upcast());
    }

    std::sort(our_votes.begin(), our_votes.end());
    if (!our_votes.empty()) {
      next_seqno_ = our_votes.back().seqno + 1;
    }

    bus.bootstrap_certificates = std::move(certs);
    bus.bootstrap_votes = td::transform(our_votes, [](const OurVote& vote) { return vote.vote; });
  }

  const td::BufferSlice pool_state_key_ =
      create_serialize_tl_object<ton_api::consensus_simplex_db_key_poolState>();
  std::set<Bits256> saved_votes_;
  td::uint32 first_nonannounced_window_ = 0;
  td::int64 next_seqno_ = 0;
};

std::optional<ProtocolVoteTraceRecord> try_decode_protocol_vote(const TestSimplexBus& bus, size_t src_node_idx,
                                                                size_t src_instance_idx, size_t dst_node_idx,
                                                                td::Slice data) {
  auto maybe_tl_vote = fetch_tl_object<simplex::tl::vote>(data, true);
  if (maybe_tl_vote.is_error()) {
    return std::nullopt;
  }
  auto maybe_vote =
      simplex::Signed<simplex::Vote>::from_tl(std::move(*maybe_tl_vote.move_as_ok()), PeerValidatorId{src_node_idx}, bus);
  if (maybe_vote.is_error()) {
    return std::nullopt;
  }
  return ProtocolVoteTraceRecord{
      .ts = td::Time::now_unadjusted(),
      .src_node_idx = src_node_idx,
      .src_instance_idx = src_instance_idx,
      .dst_node_idx = dst_node_idx,
      .vote = maybe_vote.move_as_ok().vote,
  };
}

std::optional<ProtocolCertificateTraceRecord> try_decode_protocol_certificate(const TestSimplexBus& bus,
                                                                              size_t src_node_idx,
                                                                              size_t src_instance_idx,
                                                                              size_t dst_node_idx, td::Slice data) {
  auto maybe_tl_certificate = fetch_tl_object<simplex::tl::certificate>(data, true);
  if (maybe_tl_certificate.is_error()) {
    return std::nullopt;
  }
  auto maybe_certificate =
      simplex::Certificate<simplex::Vote>::from_tl(std::move(*maybe_tl_certificate.move_as_ok()), bus);
  if (maybe_certificate.is_error()) {
    return std::nullopt;
  }
  auto certificate = maybe_certificate.move_as_ok();
  std::vector<PeerValidatorId> signers;
  for (const auto& signature : certificate->signatures) {
    signers.push_back(signature.validator);
  }
  return ProtocolCertificateTraceRecord{
      .ts = td::Time::now_unadjusted(),
      .src_node_idx = src_node_idx,
      .src_instance_idx = src_instance_idx,
      .dst_node_idx = dst_node_idx,
      .vote = certificate->vote,
      .signers = std::move(signers),
  };
}

OverlayRequestTraceRecord decode_overlay_request(size_t src_node_idx, size_t src_instance_idx, size_t dst_node_idx,
                                                 td::Timestamp timeout, td::Slice data) {
  OverlayRequestTraceRecord record{
      .ts = td::Time::now_unadjusted(),
      .src_node_idx = src_node_idx,
      .src_instance_idx = src_instance_idx,
      .dst_node_idx = dst_node_idx,
      .timeout_s = timeout ? std::max(0.0, timeout.at() - td::Time::now()) : 0.0,
      .candidate_id = std::nullopt,
      .want_candidate = false,
      .want_notar = false,
  };
  if (auto maybe_request = fetch_tl_object<ton_api::consensus_simplex_requestCandidate>(data, true);
      maybe_request.is_ok()) {
    auto request = maybe_request.move_as_ok();
    record.candidate_id = CandidateId::from_tl(request->id_);
    record.want_candidate = request->want_candidate_;
    record.want_notar = request->want_notar_;
  }
  return record;
}

bool should_drop_protocol_message(const TestSimplexBus& bus, size_t dst_node_idx, td::Slice data) {
  auto maybe_certificate = try_decode_protocol_certificate(bus, bus.local_id.idx.value(), 0, dst_node_idx, data);
  if (!maybe_certificate.has_value()) {
    return false;
  }
  auto* final = std::get_if<simplex::FinalizeVote>(&maybe_certificate->vote.vote);
  if (final == nullptr) {
    return false;
  }

  std::scoped_lock lock(test_message_filters_mutex);
  return test_message_filters.drop_final_cert_dst_node == dst_node_idx &&
         test_message_filters.drop_final_cert_from_slot.has_value() &&
         final->id.slot >= *test_message_filters.drop_final_cert_from_slot;
}

bool should_drop_candidate_delivery(size_t dst_node_idx, const CandidateRef& candidate) {
  std::scoped_lock lock(test_message_filters_mutex);
  if (test_message_filters.drop_candidate_dst_node != dst_node_idx ||
      !test_message_filters.drop_candidate_start_slot.has_value() ||
      !test_message_filters.drop_candidate_end_slot.has_value()) {
    return false;
  }
  td::uint32 slot = candidate->id.slot;
  return slot >= *test_message_filters.drop_candidate_start_slot &&
         slot < *test_message_filters.drop_candidate_end_slot;
}

size_t rewrite_candidate_request_destination(size_t src_node_idx, size_t src_instance_idx, size_t dst_node_idx,
                                             td::Slice data) {
  auto maybe_request = fetch_tl_object<ton_api::consensus_simplex_requestCandidate>(data, true);
  if (maybe_request.is_error()) {
    return dst_node_idx;
  }

  std::scoped_lock lock(test_message_filters_mutex);
  if (test_message_filters.force_candidate_request_src_node != src_node_idx ||
      test_message_filters.force_candidate_request_src_instance != src_instance_idx ||
      !test_message_filters.force_candidate_request_dst_node.has_value()) {
    return dst_node_idx;
  }
  return *test_message_filters.force_candidate_request_dst_node;
}

class TestOverlayNode;

class TestOverlay : public td::actor::Actor {
 public:
  explicit TestOverlay(td::actor::ActorId<TestTraceSink> trace_sink) : trace_sink_(trace_sink) {
  }

  void register_node(size_t idx, size_t instance_idx, td::actor::ActorId<TestOverlayNode> node) {
    Instance &inst = get_inst(idx, instance_idx);
    CHECK(inst.actor.empty());
    inst.actor = std::move(node);
  }

  void unregister_node(size_t idx, size_t instance_idx) {
    Instance &inst = get_inst(idx, instance_idx);
    CHECK(!inst.actor.empty());
    inst.actor = {};
  }

  td::actor::Task<> set_instance_disabled(size_t idx, size_t instance_idx, bool value) {
    get_inst(idx, instance_idx).disabled = value;
    if (!trace_sink_.empty()) {
      td::actor::send_closure(trace_sink_, &TestTraceSink::record_network_toggle, idx, instance_idx, value);
    }
    LOG(ERROR) << "Node #" << idx << "." << instance_idx << ": " << (value ? "disable" : "enable") << " network";
    co_return td::Unit{};
  }

  td::actor::Task<> send_message(PeerValidator src, size_t src_instance_idx, size_t dst_idx, td::BufferSlice message);
  td::actor::Task<> send_candidate(PeerValidator src, size_t src_instance_idx, size_t dst_idx, CandidateRef candidate);
  td::actor::Task<td::BufferSlice> send_query(PeerValidator src, size_t src_instance_idx, size_t dst_idx,
                                              td::BufferSlice message);

 private:
  struct Instance {
    td::actor::ActorId<TestOverlayNode> actor;
    bool disabled = false;
  };
  std::vector<std::vector<Instance>> nodes_;
  td::actor::ActorId<TestTraceSink> trace_sink_;

  Instance &get_inst(size_t idx, size_t instance_idx) {
    if (nodes_.size() <= idx) {
      nodes_.resize(idx + 1);
    }
    if (nodes_[idx].size() <= instance_idx) {
      nodes_[idx].resize(instance_idx + 1);
    }
    return nodes_[idx][instance_idx];
  }

  td::actor::Task<> before_receive(size_t src_idx, size_t src_instance_idx, size_t dst_idx, bool no_loss) {
    if (get_inst(src_idx, src_instance_idx).disabled) {
      co_return td::Status::Error("src is disabled");
    }
    if (!no_loss && td::Random::fast(0.0, 1.0) < NET_LOSS) {
      co_return td::Status::Error("packet lost");
    }
    co_await td::actor::coro_sleep(td::Timestamp::in(td::Random::fast(NET_PING.first, NET_PING.second)));
    co_return td::Unit{};
  }
};

td::actor::ActorOwn<TestOverlay> test_overlay;

class TestOverlayNode : public td::actor::SpawnsWith<Bus>, public td::actor::ConnectsTo<Bus> {
 public:
  TON_RUNTIME_DEFINE_EVENT_HANDLER();

  void start_up() override {
    instance_idx_ = dynamic_cast<const TestSimplexBus &>(*owning_bus()).instance_idx;
    td::actor::send_closure(test_overlay, &TestOverlay::register_node, owning_bus()->local_id.idx.value(),
                            instance_idx_, actor_id(this));
  }

  void tear_down() override {
    td::actor::send_closure(test_overlay, &TestOverlay::unregister_node, owning_bus()->local_id.idx.value(),
                            instance_idx_);
  }

  template <>
  void handle(BusHandle, std::shared_ptr<const StopRequested>) {
    stop();
  }

  template <>
  void handle(BusHandle bus, std::shared_ptr<const OutgoingProtocolMessage> message) {
    auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
    auto trace_message = [&](size_t dst_idx) {
      if (test_bus.trace_sink.empty()) {
        return;
      }
      if (auto vote =
              try_decode_protocol_vote(test_bus, bus->local_id.idx.value(), instance_idx_, dst_idx, message->message.data.as_slice())) {
        td::actor::send_closure(test_bus.trace_sink, &TestTraceSink::record_protocol_vote, vote->src_node_idx,
                                vote->src_instance_idx, vote->dst_node_idx, std::move(vote->vote));
        return;
      }
      if (auto cert = try_decode_protocol_certificate(test_bus, bus->local_id.idx.value(), instance_idx_, dst_idx,
                                                      message->message.data.as_slice())) {
        td::actor::send_closure(test_bus.trace_sink, &TestTraceSink::record_protocol_certificate, cert->src_node_idx,
                                cert->src_instance_idx, cert->dst_node_idx, std::move(cert->vote),
                                std::move(cert->signers));
      }
    };
    if (message->recipient.has_value()) {
      CHECK(message->recipient.value() != bus->local_id.idx);
      if (should_drop_protocol_message(test_bus, message->recipient->value(), message->message.data.as_slice())) {
        return;
      }
      trace_message(message->recipient->value());
      td::actor::ask(test_overlay, &TestOverlay::send_message, bus->local_id, instance_idx_,
                     message->recipient->value(), message->message.data.clone())
          .detach_silent();
    } else {
      for (size_t i = 0; i < bus->validator_set.size(); ++i) {
        if (bus->local_id.idx.value() != i) {
          if (should_drop_protocol_message(test_bus, i, message->message.data.as_slice())) {
            continue;
          }
          trace_message(i);
          td::actor::ask(test_overlay, &TestOverlay::send_message, bus->local_id, instance_idx_, i,
                         message->message.data.clone())
              .detach_silent();
        }
      }
    }
  }

  template <>
  void handle(BusHandle bus, std::shared_ptr<const CandidateGenerated> event) {
    for (size_t i = 0; i < bus->validator_set.size(); ++i) {
      if (bus->local_id.idx.value() != i) {
        td::actor::ask(test_overlay, &TestOverlay::send_candidate, bus->local_id, instance_idx_, i, event->candidate)
            .detach_silent();
      }
    }
  }

  template <>
  td::actor::Task<ProtocolMessage> process(BusHandle bus, std::shared_ptr<OutgoingOverlayRequest> message) {
    auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
    size_t dst_node_idx = rewrite_candidate_request_destination(bus->local_id.idx.value(), instance_idx_,
                                                                message->destination.value(), message->request.data.as_slice());
    if (!test_bus.trace_sink.empty()) {
      auto record = decode_overlay_request(bus->local_id.idx.value(), instance_idx_, dst_node_idx,
                                           message->timeout, message->request.data.as_slice());
      td::actor::send_closure(test_bus.trace_sink, &TestTraceSink::record_overlay_request, record.src_node_idx,
                              record.src_instance_idx, record.dst_node_idx, record.timeout_s, record.candidate_id,
                              record.want_candidate, record.want_notar);
    }
    auto [task, promise] = td::actor::StartedTask<ProtocolMessage>::make_bridge();
    auto promise_ptr = std::make_shared<td::Promise<ProtocolMessage>>(std::move(promise));
    process_query_inner1(bus, message, promise_ptr).start().detach();
    process_query_inner2(bus, message, promise_ptr, dst_node_idx).start().detach();
    co_return co_await std::move(task);
  }

  td::actor::Task<> process_query_inner1(BusHandle bus, std::shared_ptr<OutgoingOverlayRequest> message,
                                         std::shared_ptr<td::Promise<ProtocolMessage>> promise_ptr) {
    if (message->timeout) {
      co_await td::actor::coro_sleep(message->timeout);
      if (*promise_ptr) {
        promise_ptr->set_error(td::Status::Error(ErrorCode::timeout, "timeout"));
      }
    }
    co_return {};
  }

  td::actor::Task<> process_query_inner2(BusHandle bus, std::shared_ptr<OutgoingOverlayRequest> message,
                                         std::shared_ptr<td::Promise<ProtocolMessage>> promise_ptr,
                                         size_t dst_node_idx) {
    auto r_response = co_await td::actor::ask(test_overlay, &TestOverlay::send_query, bus->local_id, instance_idx_,
                                              dst_node_idx, message->request.data.clone())
                          .wrap();
    if (r_response.is_ok() && *promise_ptr) {
      td::BufferSlice response = r_response.move_as_ok();
      if (fetch_tl_object<ton_api::consensus_requestError>(response, true).is_ok()) {
        promise_ptr->set_error(td::Status::Error("Peer returned an error"));
      } else {
        promise_ptr->set_value(ProtocolMessage{std::move(response)});
      }
    }
    co_return {};
  }

  void receive_message(PeerValidator src, td::BufferSlice data) {
    owning_bus().publish<IncomingProtocolMessage>(src.idx, std::move(data));
  }

  void receive_candidate(CandidateRef candidate) {
    owning_bus().publish<CandidateReceived>(candidate);
  }

  td::actor::Task<td::BufferSlice> receive_query(PeerValidator src, td::BufferSlice query) {
    auto request = std::make_shared<IncomingOverlayRequest>(src.idx, std::move(query));
    auto response = co_await owning_bus().publish(std::move(request)).wrap();
    if (response.is_ok()) {
      co_return std::move(response.move_as_ok().data);
    }
    co_return create_serialize_tl_object<ton_api::consensus_requestError>();
  }

 private:
  size_t instance_idx_ = 0;
};

class TestSimplexObserver : public td::actor::SpawnsWith<simplex::Bus>, public td::actor::ConnectsTo<simplex::Bus> {
 public:
  using BusHandle = simplex::BusHandle;

  TON_RUNTIME_DEFINE_EVENT_HANDLER();

  void start_up() override {
    instance_idx_ = dynamic_cast<const TestSimplexBus&>(*owning_bus()).instance_idx;
  }

  template <>
  void handle(BusHandle, std::shared_ptr<const StopRequested>) {
    stop();
  }

  template <>
  void handle(BusHandle bus, std::shared_ptr<const simplex::LeaderWindowObserved> event) {
    auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
    if (!test_bus.trace_sink.empty()) {
      td::actor::send_closure(test_bus.trace_sink, &TestTraceSink::record_leader_window_observed,
                              bus->local_id.idx.value(), instance_idx_, event->start_slot, event->base);
    }
  }

  template <>
  void handle(BusHandle bus, std::shared_ptr<const OurLeaderWindowStarted> event) {
    auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
    if (!test_bus.trace_sink.empty()) {
      td::actor::send_closure(test_bus.trace_sink, &TestTraceSink::record_leader_window_started,
                              bus->local_id.idx.value(), instance_idx_, event->start_slot, event->end_slot,
                              event->base);
    }
  }

  template <>
  void handle(BusHandle bus, std::shared_ptr<const CandidateGenerated> event) {
    auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
    if (!test_bus.trace_sink.empty()) {
      td::actor::send_closure(test_bus.trace_sink, &TestTraceSink::record_candidate_generated,
                              bus->local_id.idx.value(), instance_idx_, event->candidate->id, event->candidate->parent_id,
                              event->candidate->leader, event->candidate->is_empty(), event->candidate->block_id());
    }
  }

  template <>
  void handle(BusHandle bus, std::shared_ptr<const simplex::NotarizationObserved> event) {
    auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
    if (!test_bus.trace_sink.empty()) {
      std::vector<PeerValidatorId> signers;
      for (const auto& signature : event->certificate->signatures) {
        signers.push_back(signature.validator);
      }
      td::actor::send_closure(test_bus.trace_sink, &TestTraceSink::record_notarization_observed,
                              bus->local_id.idx.value(), instance_idx_, event->id, std::move(signers));
    }
  }

  template <>
  void handle(BusHandle bus, std::shared_ptr<const simplex::FinalizationObserved> event) {
    auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
    if (!test_bus.trace_sink.empty()) {
      std::vector<PeerValidatorId> signers;
      for (const auto& signature : event->certificate->signatures) {
        signers.push_back(signature.validator);
      }
      td::actor::send_closure(test_bus.trace_sink, &TestTraceSink::record_finalization_observed,
                              bus->local_id.idx.value(), instance_idx_, event->id, std::move(signers));
    }
  }

  template <>
  void handle(BusHandle bus, std::shared_ptr<const MisbehaviorReport> event) {
    auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
    if (!test_bus.trace_sink.empty()) {
      td::actor::send_closure(test_bus.trace_sink, &TestTraceSink::record_misbehavior, bus->local_id.idx.value(),
                              instance_idx_, event->id);
    }
  }

 private:
  size_t instance_idx_ = 0;
};

td::actor::Task<> TestOverlay::send_message(PeerValidator src, size_t src_instance_idx, size_t dst_idx,
                                            td::BufferSlice message) {
  co_await before_receive(src.idx.value(), src_instance_idx, dst_idx, false);
  for (const auto &instance : nodes_[dst_idx]) {
    if (instance.actor.empty() || instance.disabled) {
      continue;
    }
    td::actor::send_closure(instance.actor, &TestOverlayNode::receive_message, src, message.clone());
  }
  co_return td::Unit{};
}

td::actor::Task<> TestOverlay::send_candidate(PeerValidator src, size_t src_instance_idx, size_t dst_idx,
                                              CandidateRef candidate) {
  co_await before_receive(src.idx.value(), src_instance_idx, dst_idx, true);
  if (should_drop_candidate_delivery(dst_idx, candidate)) {
    co_return td::Unit{};
  }
  for (size_t dst_instance_idx = 0; dst_instance_idx < nodes_[dst_idx].size(); ++dst_instance_idx) {
    const auto& instance = nodes_[dst_idx][dst_instance_idx];
    if (instance.actor.empty() || instance.disabled) {
      continue;
    }
    if (!trace_sink_.empty()) {
      td::actor::send_closure(trace_sink_, &TestTraceSink::record_candidate_delivery, src.idx.value(), src_instance_idx,
                              dst_idx, dst_instance_idx, candidate->id, candidate->parent_id, candidate->leader,
                              candidate->is_empty(), candidate->block_id());
    }
    td::actor::send_closure(instance.actor, &TestOverlayNode::receive_candidate, candidate);
  }
  co_return td::Unit{};
}

td::actor::Task<td::BufferSlice> TestOverlay::send_query(PeerValidator src, size_t src_instance_idx, size_t dst_idx,
                                                         td::BufferSlice message) {
  if (nodes_[dst_idx].empty()) {
    co_return td::Status::Error("no instances");
  }
  auto dst_instance_idx = (size_t)td::Random::fast(0, (int)nodes_[dst_idx].size() - 1);
  const auto &instance = nodes_[dst_idx][dst_instance_idx];
  co_await before_receive(src.idx.value(), src_instance_idx, dst_idx, true);
  if (instance.actor.empty() || instance.disabled) {
    co_return td::Status::Error("instance is stopped/disabled");
  }
  auto response = co_await td::actor::ask(instance.actor, &TestOverlayNode::receive_query, src, std::move(message));
  co_await before_receive(dst_idx, dst_instance_idx, src.idx.value(), true);
  co_return response;
}

class TestConsensus;

class CandidateStorage : public td::actor::Actor {
 public:
  td::actor::Task<BlockCandidate> load_block_candidate(PublicKey source, BlockIdExt block_id,
                                                       FileHash collated_data_hash) {
    auto it = candidates_.find({source.ed25519_value().raw(), block_id, collated_data_hash});
    if (it == candidates_.end()) {
      co_return td::Status::Error("no candidate in db");
    }
    co_return it->second.clone();
  }

  td::actor::Task<> store_block_candidate(BlockCandidate candidate) {
    std::tuple key{candidate.pubkey.as_bits256(), candidate.id, candidate.collated_file_hash};
    candidates_.emplace(key, std::move(candidate));
    co_return {};
  }

  td::actor::Task<> clear() {
    candidates_.clear();
    co_return td::Unit{};
  }

 private:
  std::map<std::tuple<td::Bits256, BlockIdExt, FileHash>, BlockCandidate> candidates_;
};

class TestManagerFacade : public ManagerFacade {
 public:
  explicit TestManagerFacade(size_t node_idx, size_t instance_idx, Ref<block::ValidatorSet> validator_set,
                             td::actor::ActorId<TestConsensus> test_consensus,
                             td::actor::ActorId<CandidateStorage> candidate_storage)
      : node_idx_(node_idx)
      , instance_idx_(instance_idx)
      , validator_set_(validator_set)
      , test_consensus_(test_consensus)
      , candidate_storage_(candidate_storage) {
  }

  td::actor::Task<GeneratedCandidate> collate_block(CollateParams params,
                                                    td::CancellationToken cancellation_token) override {
    CHECK(params.prev.size() == 1);
    uint32_t prev_seqno = params.prev[0].seqno();
    LOG(WARNING) << "Collate block #" << prev_seqno + 1;
    CHECK(params.shard == SHARD);
    CHECK(params.min_masterchain_block_id == MIN_MC_BLOCK_ID);

    CHECK(params.prev_block_state_roots.size() == 1 &&
          params.prev_block_state_roots[0]->get_hash() == gen_shard_state(prev_seqno)->get_hash());
    if (prev_seqno != 0) {
      CHECK(params.prev_block_data.size() == 1 && params.prev_block_data[0]->block_id() == params.prev[0]);
    }
    double gen_utime = params.utime ? params.utime.value() : td::Clocks::system();

    block::gen::BlockInfo::Record info;
    info.version = 0;
    info.not_master = !SHARD.is_masterchain();
    info.after_merge = info.before_split = info.after_split = false;
    info.want_split = info.want_merge = false;
    info.key_block = info.vert_seqno_incr = false;
    info.flags = 0;
    info.seq_no = prev_seqno + 1;
    info.vert_seq_no = 0;

    vm::CellBuilder cb;
    block::ShardId{SHARD}.serialize(cb);
    info.shard = cb.as_cellslice_ref();

    info.gen_utime = (UnixTime)gen_utime;
    info.start_lt = (LogicalTime)info.seq_no * 1000;
    info.end_lt = (LogicalTime)info.seq_no * 1000 + 1;
    info.gen_validator_list_hash_short = validator_set_->get_validator_set_hash();
    info.gen_catchain_seqno = validator_set_->get_catchain_seqno();
    info.min_ref_mc_seqno = MIN_MC_BLOCK_ID.seqno();
    info.prev_key_block_seqno = MIN_MC_BLOCK_ID.seqno();
    if (!SHARD.is_masterchain()) {
      info.master_ref = make_ext_blk_ref(MIN_MC_BLOCK_ID, 0);
    }
    info.prev_ref = make_ext_blk_ref(params.prev[0], (LogicalTime)prev_seqno * 1000 + 1);
    td::Ref<vm::Cell> block_info;
    CHECK(block::gen::pack_cell(block_info, info));

    td::Ref<vm::Cell> value_flow = vm::CellBuilder{}.finalize_novm();
    td::Ref<vm::Cell> merkle_update =
        vm::CellBuilder::create_merkle_update(gen_shard_state(prev_seqno), gen_shard_state(prev_seqno + 1));

    td::Bits256 rand_data;
    td::Random::secure_bytes(rand_data.as_slice());
    td::Ref<vm::Cell> block_extra = vm::CellBuilder{}.store_bytes(rand_data.as_slice()).finalize_novm();

    td::Ref<vm::Cell> block_root = vm::CellBuilder{}
                                       .store_long(0x11ef55aa, 32)
                                       .store_long(-111, 32)
                                       .store_ref(block_info)
                                       .store_ref(value_flow)
                                       .store_ref(merkle_update)
                                       .store_ref(block_extra)
                                       .finalize_novm();
    td::BufferSlice data = vm::std_boc_serialize(block_root, 31).move_as_ok();

    std::vector<td::Ref<vm::Cell>> collated_roots;
    // consensus_extra_data#638eb292 flags:# gen_utime_ms:uint64 = ConsensusExtraData;
    auto cell = vm::CellBuilder{}
                    .store_long(0x638eb292, 32)
                    .store_long(0, 32)
                    .store_long((td::uint64)(gen_utime * 1000.0), 64)
                    .finalize_novm();
    collated_roots.push_back(std::move(cell));
    td::BufferSlice collated_data = co_await vm::std_boc_serialize_multi(collated_roots, 2);

    co_await td::actor::coro_sleep(td::Timestamp::in(td::Random::fast(COLLATION_TIME.first, COLLATION_TIME.second)));

    BlockCandidate candidate(
        params.creator,
        BlockIdExt(BlockId(params.shard, prev_seqno + 1), block_root->get_hash().bits(), td::sha256_bits256(data)),
        td::sha256_bits256(collated_data), data.clone(), collated_data.clone());
    if (!params.skip_store_candidate) {
      co_await store_block_candidate(candidate.clone());
    }
    co_return GeneratedCandidate{.candidate = std::move(candidate), .is_cached = false, .self_collated = true};
  }

  td::actor::Task<ValidateCandidateResult> validate_block_candidate(BlockCandidate candidate, ValidateParams params,
                                                                    td::Timestamp timeout) override {
    CHECK(params.prev.size() == 1);
    uint32_t prev_seqno = params.prev[0].seqno();
    LOG(WARNING) << "Validate block #" << candidate.id.seqno();
    CHECK(params.prev[0].shard_full() == SHARD);
    CHECK(candidate.id.shard_full() == SHARD);
    CHECK(candidate.id.seqno() == prev_seqno + 1);
    CHECK(params.prev_block_state_roots.size() == 1 &&
          params.prev_block_state_roots[0]->get_hash() == gen_shard_state(prev_seqno)->get_hash());
    co_await td::actor::coro_sleep(td::Timestamp::in(td::Random::fast(VALIDATION_TIME.first, VALIDATION_TIME.second)));
    co_await store_block_candidate(candidate.clone());
    co_return CandidateAccept{.ok_from_utime = co_await get_candidate_gen_utime_exact(candidate)};
  }

  td::actor::Task<> accept_block(BlockIdExt id, td::Ref<BlockData> data, size_t creator_idx,
                                 td::Ref<block::BlockSignatureSet> signatures, int send_broadcast_mode,
                                 bool apply) override;

  td::actor::Task<td::Ref<vm::Cell>> wait_block_state_root(BlockIdExt block_id, td::Timestamp timeout) override;
  td::actor::Task<td::Ref<BlockData>> wait_block_data(BlockIdExt block_id, td::Timestamp timeout) override;

  td::actor::Task<BlockCandidate> load_block_candidate(PublicKey source, BlockIdExt block_id,
                                                       FileHash collated_data_hash) override {
    co_return co_await td::actor::ask(candidate_storage_, &CandidateStorage::load_block_candidate, source, block_id,
                                      collated_data_hash);
  }

  td::actor::Task<> store_block_candidate(BlockCandidate candidate) override {
    candidate.out_msg_queue_proof_broadcasts = {};
    co_return co_await td::actor::ask(candidate_storage_, &CandidateStorage::store_block_candidate,
                                      std::move(candidate));
  }

 private:
  size_t node_idx_;
  size_t instance_idx_;
  Ref<block::ValidatorSet> validator_set_;
  td::actor::ActorId<TestConsensus> test_consensus_;
  td::actor::ActorId<CandidateStorage> candidate_storage_;
};

class TestDbImpl : public consensus::Db {
 public:
  struct DbInner {
    std::map<td::BufferSlice, td::BufferSlice> map;
    std::mutex mutex;
  };

  explicit TestDbImpl(std::shared_ptr<DbInner> db) : db_(std::move(db)) {
    std::scoped_lock lock(db_->mutex);
    for (auto &[key, value] : db_->map) {
      snapshot_.emplace(key.clone(), value.clone());
    }
  }
  ~TestDbImpl() override = default;

  void disable() {
    std::scoped_lock lock(db_->mutex);
    disabled_ = true;
  }

  std::optional<td::BufferSlice> get(td::Slice key) const override {
    auto it = snapshot_.find(td::BufferSlice{key});
    if (it == snapshot_.end()) {
      return std::nullopt;
    }
    return it->second.clone();
  }
  std::vector<std::pair<td::BufferSlice, td::BufferSlice>> get_by_prefix(td::uint32 prefix) const override {
    std::vector<std::pair<td::BufferSlice, td::BufferSlice>> result;
    td::BufferSlice begin{(const char *)&prefix, 4};
    td::uint32 prefix2 = prefix + 1;
    td::BufferSlice end{(const char *)&prefix2, 4};
    for (auto it = snapshot_.lower_bound(begin); it != snapshot_.end() && it->first < end; ++it) {
      result.emplace_back(it->first.clone(), it->second.clone());
    }
    return result;
  }
  td::actor::Task<> set(td::BufferSlice key, td::BufferSlice value) override {
    co_await td::actor::coro_sleep(td::Timestamp::in(td::Random::fast(DB_DELAY.first, DB_DELAY.second)));
    std::scoped_lock lock(db_->mutex);
    if (disabled_) {
      co_return td::Status::Error("db is disabled");
    }
    db_->map[std::move(key)] = std::move(value);
    co_return {};
  }

 private:
  std::map<td::BufferSlice, td::BufferSlice> snapshot_;
  std::shared_ptr<DbInner> db_;
  bool disabled_ = false;
};

class TestConsensus : public td::actor::Actor {
 public:
  td::actor::Task<> run() {
    auto result = co_await run_inner().wrap();
    if (result.is_error()) {
      LOG(FATAL) << "Test consensus error: " << result.move_as_error();
    }
    LOG(WARNING) << "Test finished";
    std::exit(0);
  }

  td::actor::Task<> on_block_accepted(size_t node_idx, size_t instance_idx, td::Ref<BlockData> block,
                                      size_t creator_idx, td::Ref<block::BlockSignatureSet> signatures) {
    BlockIdExt block_id = block->block_id();
    if (signatures->is_final()) {
      signatures->check_signatures(validator_set_, block_id).ensure();
    } else {
      CHECK(!SHARD.is_masterchain());
      signatures->check_approve_signatures(validator_set_, block_id).ensure();
    }
    BlockSeqno seqno = block_id.seqno();
    if (accepted_blocks_.contains(seqno)) {
      LOG_CHECK(accepted_blocks_[seqno]->block_id() == block_id) << "Accepted different blocks for seqno " << seqno;
    } else {
      accepted_blocks_[seqno] = block;
    }
    Instance &inst = nodes_[node_idx].instances[instance_idx];
    inst.last_accepted_block = std::max(inst.last_accepted_block, seqno);
    td::actor::send_closure(trace_sink_, &TestTraceSink::record_accepted_block, node_idx, instance_idx, block_id);
    if (last_accepted_block_.seqno() < seqno && signatures->is_final()) {
      last_accepted_block_ = block_id;
      last_accepted_block_leader_idx_ = creator_idx;
      for (Node &node : nodes_) {
        for (Instance &inst : node.instances) {
          if (inst.status == Instance::Running) {
            inst.bus.publish<BlockFinalizedInMasterchain>(block_id);
          }
        }
      }
    }
    co_return {};
  }

  td::actor::Task<> wait_block_accepted(BlockIdExt block_id) {
    if (block_id == FIRST_PARENT) {
      co_return {};
    }
    td::Timestamp timeout = td::Timestamp::in(10.0);
    while (!timeout.is_in_past()) {
      auto it = accepted_blocks_.find(block_id.seqno());
      if (it != accepted_blocks_.end() && it->second->block_id() == block_id) {
        co_return {};
      }
      co_await td::actor::coro_sleep(td::Timestamp::in(0.1));
    }
    co_return td::Status::Error(ErrorCode::timeout, "timeout");
  }

  td::actor::Task<td::Ref<vm::Cell>> wait_block_state_root(BlockIdExt block_id) {
    co_await wait_block_accepted(block_id);
    co_return gen_shard_state(block_id.seqno());
  }

  td::actor::Task<td::Ref<BlockData>> wait_block_data(BlockIdExt block_id) {
    CHECK(block_id != FIRST_PARENT);
    co_await wait_block_accepted(block_id);
    auto it = accepted_blocks_.find(block_id.seqno());
    CHECK(it != accepted_blocks_.end());
    CHECK(it->second->block_id() == block_id);
    co_return it->second;
  }

  td::actor::Task<TraceSnapshot> get_trace_snapshot() {
    co_return co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
  }

  td::actor::Task<> clear_traces() {
    co_return co_await td::actor::ask(trace_sink_, &TestTraceSink::clear);
  }

  td::actor::Task<> clear_test_message_filters() {
    std::scoped_lock lock(test_message_filters_mutex);
    test_message_filters = {};
    co_return td::Unit{};
  }

  td::actor::Task<> set_drop_final_certs_for_test(size_t dst_node_idx, td::uint32 from_slot) {
    std::scoped_lock lock(test_message_filters_mutex);
    test_message_filters.drop_final_cert_dst_node = dst_node_idx;
    test_message_filters.drop_final_cert_from_slot = from_slot;
    co_return td::Unit{};
  }

  td::actor::Task<> set_drop_candidates_for_test(size_t dst_node_idx, td::uint32 start_slot, td::uint32 end_slot) {
    std::scoped_lock lock(test_message_filters_mutex);
    test_message_filters.drop_candidate_dst_node = dst_node_idx;
    test_message_filters.drop_candidate_start_slot = start_slot;
    test_message_filters.drop_candidate_end_slot = end_slot;
    co_return td::Unit{};
  }

  td::actor::Task<> set_force_candidate_request_destination_for_test(size_t src_node_idx, size_t src_instance_idx,
                                                                     size_t dst_node_idx) {
    std::scoped_lock lock(test_message_filters_mutex);
    test_message_filters.force_candidate_request_src_node = src_node_idx;
    test_message_filters.force_candidate_request_src_instance = src_instance_idx;
    test_message_filters.force_candidate_request_dst_node = dst_node_idx;
    co_return td::Unit{};
  }

  td::actor::Task<> set_candidate_resolution_target_for_test(CandidateId id) {
    std::scoped_lock lock(test_expectations_mutex);
    test_expectations.candidate_resolution_target = id;
    co_return td::Unit{};
  }

  td::actor::Task<> clear_instance_candidate_storage(size_t node_idx, size_t instance_idx) {
    co_return co_await td::actor::ask(nodes_[node_idx].instances[instance_idx].candidate_storage, &CandidateStorage::clear);
  }

  td::actor::Task<> clear_instance_db(size_t node_idx, size_t instance_idx) {
    auto& db_inner = nodes_[node_idx].instances[instance_idx].db_inner;
    std::scoped_lock lock(db_inner->mutex);
    db_inner->map.clear();
    co_return td::Unit{};
  }

  td::actor::Task<> clear_instance_candidate_resolver_candidates(size_t node_idx, size_t instance_idx) {
    auto& db_inner = nodes_[node_idx].instances[instance_idx].db_inner;
    std::scoped_lock lock(db_inner->mutex);
    auto erase_by_prefix = [&](td::uint32 prefix) {
      std::vector<td::BufferSlice> keys_to_erase;
      td::BufferSlice begin{reinterpret_cast<const char*>(&prefix), 4};
      td::uint32 prefix2 = prefix + 1;
      td::BufferSlice end{reinterpret_cast<const char*>(&prefix2), 4};
      for (auto it = db_inner->map.lower_bound(begin); it != db_inner->map.end() && it->first < end; ++it) {
        keys_to_erase.push_back(it->first.clone());
      }
      for (auto& key : keys_to_erase) {
        db_inner->map.erase(key);
      }
    };
    erase_by_prefix(ton_api::consensus_simplex_db_key_candidateResolver_candidateInfo::ID);
    erase_by_prefix(ton_api::consensus_simplex_db_key_candidate::ID);
    co_return td::Unit{};
  }

  td::actor::Task<> set_instance_network_disabled_for_test(size_t node_idx, size_t instance_idx, bool value) {
    co_return co_await td::actor::ask(test_overlay, &TestOverlay::set_instance_disabled, node_idx, instance_idx, value);
  }

  td::actor::Task<> start_instance_for_test(size_t node_idx, size_t instance_idx) {
    start_instance(node_idx, instance_idx);
    co_return td::Unit{};
  }

  td::actor::Task<> stop_instance_for_test(size_t node_idx, size_t instance_idx) {
    co_return co_await stop_instance(node_idx, instance_idx);
  }

  td::actor::Task<> resolve_candidate_for_test(size_t node_idx, size_t instance_idx, CandidateId id) {
    auto resolved = co_await nodes_[node_idx].instances[instance_idx].bus.publish<simplex::ResolveCandidate>(id);
    (void)resolved;
    td::actor::send_closure(trace_sink_, &TestTraceSink::record_candidate_resolved, node_idx, instance_idx, id);
    co_return td::Unit{};
  }

 private:
  void apply_test_case_defaults() {
    if (TEST_CASE == TestCase::NotarRequiresParentNotar) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (DURATION == 60.0) {
        DURATION = 3.0;
      }
      if (NET_PING == std::pair<double, double>{0.05, 0.1}) {
        NET_PING = {0.25, 0.3};
      }
    }
    if (TEST_CASE == TestCase::EmptyCandidatesConsensus) {
      SHARD = ShardIdFull{masterchainId};
      FIRST_PARENT.id.workchain = masterchainId;
      FIRST_PARENT.id.shard = shardIdAll;
      MIN_MC_BLOCK_ID = FIRST_PARENT;
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (DURATION == 60.0) {
        DURATION = 3.0;
      }
    }
    if (TEST_CASE == TestCase::StandstillRebroadcastContents) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 200;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
      STANDSTILL_TIMEOUT_S = 1.0;
    }
    if (TEST_CASE == TestCase::CandidateResolutionRecovery) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 200;
      }
      if (DURATION == 60.0) {
        DURATION = 5.0;
      }
    }
  }

  td::actor::Task<> wait_for_finalization_on(size_t node_idx, size_t instance_idx, double timeout_s) {
    td::Timestamp deadline = td::Timestamp::in(timeout_s);
    while (!deadline.is_in_past()) {
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == node_idx && record.instance_idx == instance_idx) {
          co_return td::Unit{};
        }
      }
      co_await td::actor::coro_sleep(td::Timestamp::in(0.05));
    }
    co_return td::Status::Error(PSTRING() << "timeout waiting for finalization on validator #" << node_idx << "."
                                          << instance_idx);
  }

  bool skip_finalize_for_test_case() const {
    return TEST_CASE == TestCase::StandstillRebroadcastContents || TEST_CASE == TestCase::CandidateResolutionRecovery;
  }

  td::actor::Task<> run_test_case_scenario() {
    if (TEST_CASE == TestCase::StandstillRebroadcastContents) {
      // Drives simplex_docs.md Rule 8 into a standstill:
      // after one finalization, stop two validators so no new quorum can form,
      // then wait past T_s for the surviving node to rebroadcast its cached state.
      co_await wait_for_finalization_on(0, 0, 5.0);
      co_await stop_instance(2, 0);
      co_await stop_instance(3, 0);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::CandidateResolutionRecovery) {
      // Drives simplex_docs.md Rule 2:
      // after a node loses its local simplex state, it must recover by repeatedly requesting a
      // notarized candidate, backing off between retries, and then resolving that candidate once the only
      // peer with the candidate body becomes reachable again.
      co_await wait_for_finalization_on(1, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      std::optional<CandidateId> target_id;
      for (auto it = snapshot.protocol_votes.rbegin(); it != snapshot.protocol_votes.rend(); ++it) {
        if (it->src_node_idx != 3 || it->src_instance_idx != 0) {
          continue;
        }
        if (auto* notar = std::get_if<simplex::NotarizeVote>(&it->vote.vote)) {
          target_id = notar->id;
          break;
        }
      }
      if (!target_id.has_value()) {
        co_return td::Status::Error("could not find a candidate notarized by the serving peer for Rule 2 recovery test");
      }
      co_await set_candidate_resolution_target_for_test(*target_id);

      co_await stop_instance(0, 0);
      co_await stop_instance(1, 0);
      co_await stop_instance(2, 0);
      co_await stop_instance(3, 0);
      co_await clear_instance_db(0, 0);
      co_await clear_instance_candidate_storage(0, 0);
      co_await clear_instance_candidate_resolver_candidates(1, 0);
      co_await clear_instance_candidate_resolver_candidates(2, 0);
      start_instance(1, 0);
      start_instance(2, 0);
      co_await set_force_candidate_request_destination_for_test(0, 0, 3);
      co_await clear_traces();
      start_instance(0, 0);
      co_await td::actor::coro_sleep(td::Timestamp::in(0.2));
      resolve_candidate_for_test(0, 0, *target_id).start().detach();
      co_await td::actor::coro_sleep(td::Timestamp::in(1.5));
      start_instance(3, 0);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }

    co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
    if (TEST_CASE == TestCase::NotarRequiresParentNotar || TEST_CASE == TestCase::EmptyCandidatesConsensus) {
      co_await td::actor::coro_sleep(td::Timestamp::in(1.0));
    }
    co_return td::Unit{};
  }

  td::actor::Task<> run_inner() {
    apply_test_case_defaults();
    {
      std::scoped_lock lock(test_message_filters_mutex);
      test_message_filters = {};
    }
    {
      std::scoped_lock lock(test_expectations_mutex);
      test_expectations = {};
    }
    keyring_ = keyring::Keyring::create("");

    for (size_t i = 0; i < N_NODES; ++i) {
      Node node;

      PrivateKey node_pk{privkeys::Ed25519::random()};
      node.public_key = node_pk.compute_public_key();
      node.node_id = node.public_key.compute_short_id();
      td::actor::send_closure(keyring_, &keyring::Keyring::add_key, std::move(node_pk), true, [](td::Result<>) {});

      PrivateKey adnl_pk{privkeys::Ed25519::random()};
      node.adnl_id_full = adnl::AdnlNodeIdFull{adnl_pk.compute_public_key()};
      node.adnl_id = node.adnl_id_full.compute_short_id();
      td::actor::send_closure(keyring_, &keyring::Keyring::add_key, std::move(adnl_pk), true, [](td::Result<>) {});

      node.weight = 11;

      nodes_.push_back(std::move(node));
    }

    std::vector<ValidatorDescr> validator_descrs;
    for (size_t idx = 0; idx < nodes_.size(); ++idx) {
      Node &node = nodes_[idx];
      validator_descrs.push_back(ValidatorDescr(Ed25519_PublicKey{node.public_key.ed25519_value().raw()}, node.weight,
                                                node.adnl_id.bits256_value()));
      validators_.push_back(PeerValidator{.idx = PeerValidatorId((int)idx),
                                          .key = node.public_key,
                                          .short_id = node.node_id,
                                          .adnl_id = node.adnl_id,
                                          .weight = node.weight});
      total_weight_ += node.weight;
    }
    validator_set_ = td::Ref<block::ValidatorSet>{true, CC_SEQNO, SHARD, std::move(validator_descrs)};

    trace_sink_ = td::actor::create_actor<TestTraceSink>("test-trace-sink");
    test_overlay = td::actor::create_actor<TestOverlay>("test-overlay", trace_sink_.get());

    for (size_t idx = 0; idx < N_NODES; ++idx) {
      Node &node = nodes_[idx];
      size_t n_instances = idx < N_DOUBLE_NODES ? 2 : 1;
      for (size_t i = 0; i < n_instances; ++i) {
        Instance inst;
        inst.db_inner = std::make_shared<TestDbImpl::DbInner>();
        inst.candidate_storage =
            td::actor::create_actor<CandidateStorage>(PSTRING() << "ManagerFacade." << idx << "." << i);
        node.instances.push_back(std::move(inst));
      }
    }

    for (size_t idx = 0; idx < N_NODES; ++idx) {
      for (size_t i = 0; i < nodes_[idx].instances.size(); ++i) {
        start_instance(idx, i);
      }
    }

    if (GREMLIN_PERIOD.first >= 0.0) {
      run_gremlin().start().detach();
    }
    if (NET_GREMLIN_PERIOD.first >= 0.0) {
      run_net_gremlin().start().detach();
    }

    co_await run_test_case_scenario();

    if (skip_finalize_for_test_case()) {
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      co_return verify_test_case(snapshot);
    }
    co_return co_await finalize();
  }

  void start_instance(size_t node_idx, size_t instance_idx) {
    Node &node = nodes_[node_idx];
    Instance &inst = node.instances[instance_idx];
    CHECK(inst.status == Instance::Stopped);
    auto &runtime = inst.runtime;
    BlockAccepter::register_in(runtime);
    BlockProducer::register_in(runtime);
    BlockValidator::register_in(runtime);
    runtime.register_actor<TestOverlayNode>("PrivateOverlay");
    runtime.register_actor<TestSimplexObserver>("TestSimplexObserver");
    simplex::CandidateResolver::register_in(runtime);
    simplex::Consensus::register_in(runtime);
    runtime.register_actor<TestSimplexDb>("SimplexDb");
    simplex::Pool::register_in(runtime);
    simplex::StateResolver::register_in(runtime);

    inst.manager_facade = td::actor::create_actor<TestManagerFacade>(
        PSTRING() << "ManagerFacade." << node_idx << "." << instance_idx, node_idx, instance_idx, validator_set_,
        actor_id(this), inst.candidate_storage.get());
    auto [stop_task, stop_promise] = td::actor::StartedTask<>::make_bridge();
    auto bus = std::make_shared<TestSimplexBus>();
    inst.stop_waiter = std::move(stop_task);
    bus->instance_idx = instance_idx;
    bus->trace_sink = trace_sink_.get();
    bus->stop_promise = std::move(stop_promise);
    bus->shard = SHARD;
    bus->manager = inst.manager_facade.get();
    bus->keyring = keyring_.get();
    bus->validator_opts = ValidatorManagerOptions::create(BlockIdExt{}, BlockIdExt{});
    bus->validator_set = validators_;
    bus->total_weight = total_weight_;
    bus->local_id = validators_[node_idx];
    bus->config = NewConsensusConfig{
        .target_rate_ms = TARGET_RATE_MS,
        .max_block_size = 1 << 20,
        .max_collated_data_size = 1 << 20,
        .consensus = NewConsensusConfig::Simplex{.slots_per_leader_window = SLOTS_PER_LEADER_WINDOW}};
    bus->simplex_config = bus->config.consensus.get<NewConsensusConfig::Simplex>();
    bus->session_id = SESSION_ID;
    bus->cc_seqno = CC_SEQNO;
    bus->validator_set_hash = validator_set_->get_validator_set_hash();
    bus->populate_collator_schedule();
    bus->standstill_timeout_s = STANDSTILL_TIMEOUT_S;
    bus->db = std::make_unique<TestDbImpl>(inst.db_inner);
    inst.bus = runtime.start(std::static_pointer_cast<simplex::Bus>(bus),
                             PSTRING() << "consensus." << node_idx << "." << instance_idx);
    inst.status = Instance::Running;
    td::actor::send_closure(trace_sink_, &TestTraceSink::record_lifecycle, node_idx, instance_idx, true);
    inst.bus.publish<BlockFinalizedInMasterchain>(last_accepted_block_);
    inst.bus.publish<Start>(
        td::make_ref<ChainState>(ChainState::ZerostateTip{FIRST_PARENT, gen_shard_state(0)}, MIN_MC_BLOCK_ID));
    LOG(ERROR) << "Starting node #" << node_idx << "." << instance_idx;
  }

  td::actor::Task<> stop_instance(size_t node_idx, size_t instance_idx) {
    Node &node = nodes_[node_idx];
    Instance &inst = node.instances[instance_idx];
    if (inst.status == Instance::Stopped) {
      co_return td::Unit{};
    }
    if (inst.status == Instance::Stopping) {
      auto [task, promise] = td::actor::StartedTask<>::make_bridge();
      inst.extra_stop_waiters.push_back(std::move(promise));
      co_return co_await std::move(task);
    }
    LOG(ERROR) << "Stopping node #" << node_idx << "." << instance_idx;
    inst.bus.publish<StopRequested>();
    dynamic_cast<TestDbImpl &>(*inst.bus->db).disable();
    inst.bus = {};
    inst.status = Instance::Stopping;
    co_await std::move(*inst.stop_waiter);
    //std::move(inst.stop_waiter.value()).detach();
    //co_await td::actor::coro_sleep(td::Timestamp::in(0.5));
    inst.status = Instance::Stopped;
    inst.runtime = {};
    td::actor::send_closure(trace_sink_, &TestTraceSink::record_lifecycle, node_idx, instance_idx, false);
    LOG(ERROR) << "Stopped node #" << node_idx << "." << instance_idx;
    for (auto &promise : inst.extra_stop_waiters) {
      promise.set_value(td::Unit{});
    }
    inst.extra_stop_waiters.clear();
    co_return {};
  }

  td::actor::Task<> run_gremlin() {
    for (size_t i = 0; i < GREMLIN_TIMES && !finishing_; ++i) {
      co_await td::actor::coro_sleep(td::Timestamp::in(td::Random::fast(GREMLIN_PERIOD.first, GREMLIN_PERIOD.second)));
      int cnt = td::Random::fast((int)GREMLIN_N.first, (int)GREMLIN_N.second);
      for (int i = 0; i < cnt; ++i) {
        run_gremlin_once().start().detach();
      }
    }
    co_return {};
  }

  td::actor::Task<> run_gremlin_once() {
    if (finishing_) {
      co_return {};
    }
    size_t kill_node_idx = 0, kill_inst_idx = 0;
    int cnt = 0;
    for (size_t node_idx = 0; node_idx < N_NODES; ++node_idx) {
      if (GREMLIN_KILLS_LEADER &&
          (!last_accepted_block_leader_idx_ || last_accepted_block_leader_idx_.value() != node_idx)) {
        continue;
      }
      for (size_t inst_idx = 0; inst_idx < nodes_[node_idx].instances.size(); ++inst_idx) {
        if (nodes_[node_idx].instances[inst_idx].status == Instance::Running) {
          ++cnt;
          if (td::Random::fast(1, cnt) == 1) {
            kill_node_idx = node_idx;
            kill_inst_idx = inst_idx;
          }
        }
      }
    }
    if (cnt == 0) {
      co_return {};
    }
    co_await stop_instance(kill_node_idx, kill_inst_idx);
    co_await td::actor::coro_sleep(
        td::Timestamp::in(td::Random::fast(GREMLIN_DOWNTIME.first, GREMLIN_DOWNTIME.second)));
    if (finishing_) {
      co_return {};
    }
    start_instance(kill_node_idx, kill_inst_idx);
    co_return {};
  }

  td::actor::Task<> run_net_gremlin() {
    for (size_t i = 0; i < NET_GREMLIN_TIMES && !finishing_; ++i) {
      co_await td::actor::coro_sleep(
          td::Timestamp::in(td::Random::fast(NET_GREMLIN_PERIOD.first, NET_GREMLIN_PERIOD.second)));
      int cnt = td::Random::fast((int)NET_GREMLIN_N.first, (int)NET_GREMLIN_N.second);
      for (int i = 0; i < cnt; ++i) {
        run_net_gremlin_once().start().detach();
      }
    }
    co_return {};
  }

  td::actor::Task<> run_net_gremlin_once() {
    if (finishing_) {
      co_return {};
    }
    size_t selected_node_idx = 0, selected_inst_idx = 0;
    int cnt = 0;
    for (size_t node_idx = 0; node_idx < N_NODES; ++node_idx) {
      if (NET_GREMLIN_KILLS_LEADER &&
          (!last_accepted_block_leader_idx_ || last_accepted_block_leader_idx_.value() != node_idx)) {
        continue;
      }
      for (size_t inst_idx = 0; inst_idx < nodes_[node_idx].instances.size(); ++inst_idx) {
        if (!nodes_[node_idx].instances[inst_idx].net_gremlin_active) {
          ++cnt;
          if (td::Random::fast(1, cnt) == 1) {
            selected_node_idx = node_idx;
            selected_inst_idx = inst_idx;
          }
        }
      }
    }
    if (cnt == 0) {
      co_return {};
    }
    nodes_[selected_node_idx].instances[selected_inst_idx].net_gremlin_active = true;
    co_await td::actor::ask(test_overlay, &TestOverlay::set_instance_disabled, selected_node_idx, selected_inst_idx,
                            true);
    co_await td::actor::coro_sleep(
        td::Timestamp::in(td::Random::fast(NET_GREMLIN_DOWNTIME.first, NET_GREMLIN_DOWNTIME.second)));
    co_await td::actor::ask(test_overlay, &TestOverlay::set_instance_disabled, selected_node_idx, selected_inst_idx,
                            false);
    nodes_[selected_node_idx].instances[selected_inst_idx].net_gremlin_active = false;
    co_return {};
  }

  td::actor::Task<> finalize() {
    finishing_ = true;
    LOG(WARNING) << "TEST FINISHED";
    std::vector<td::actor::Task<>> tasks;
    for (size_t idx = 0; idx < N_NODES; ++idx) {
      for (size_t i = 0; i < nodes_[idx].instances.size(); ++i) {
        tasks.push_back(stop_instance(idx, i));
      }
    }
    co_await td::actor::all(std::move(tasks));
    LOG(WARNING) << "TEST RESULTS:";
    for (size_t idx = 0; idx < N_NODES; ++idx) {
      for (size_t inst_idx = 0; inst_idx < nodes_[idx].instances.size(); ++inst_idx) {
        Instance &inst = nodes_[idx].instances[inst_idx];
        LOG(WARNING) << "Node #" << idx << " instance #" << inst_idx << " : synced up to block "
                     << inst.last_accepted_block;
      }
    }
    auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
    auto status = verify_test_case(snapshot);
    if (status.is_error()) {
      co_return status.move_as_error();
    }
    co_return td::Unit{};
  }

  struct Instance {
    td::actor::Runtime runtime;
    td::actor::ActorOwn<TestManagerFacade> manager_facade;
    simplex::BusHandle bus;

    BlockSeqno last_accepted_block = FIRST_PARENT.seqno();
    std::shared_ptr<TestDbImpl::DbInner> db_inner;
    td::actor::ActorOwn<CandidateStorage> candidate_storage;

    enum Status { Stopped, Running, Stopping };
    Status status = Stopped;
    td::optional<td::actor::StartedTask<>> stop_waiter;
    std::vector<td::Promise<td::Unit>> extra_stop_waiters;

    bool net_gremlin_active = false;
  };
  struct Node {
    PublicKey public_key;
    PublicKeyHash node_id;
    adnl::AdnlNodeIdFull adnl_id_full;
    adnl::AdnlNodeIdShort adnl_id;
    ValidatorWeight weight = 0;
    std::vector<Instance> instances;
  };
  std::vector<Node> nodes_;
  td::Ref<block::ValidatorSet> validator_set_;
  std::vector<PeerValidator> validators_;
  ValidatorWeight total_weight_ = 0;

  td::actor::ActorOwn<keyring::Keyring> keyring_;
  td::actor::ActorOwn<TestTraceSink> trace_sink_;

  std::map<BlockSeqno, td::Ref<BlockData>> accepted_blocks_;
  BlockIdExt last_accepted_block_ = FIRST_PARENT;
  td::optional<size_t> last_accepted_block_leader_idx_;
  bool finishing_ = false;
};

td::actor::Task<> TestManagerFacade::accept_block(BlockIdExt id, td::Ref<BlockData> data, size_t creator_idx,
                                                  td::Ref<block::BlockSignatureSet> signatures, int send_broadcast_mode,
                                                  bool apply) {
  CHECK(id.shard_full() == SHARD);
  LOG(WARNING) << "Accept block #" << id.seqno() << " (" << (signatures->is_final() ? "final" : "notarize")
               << " signatures), creator_idx=" << creator_idx;
  CHECK(id == data->block_id());
  td::actor::ask(test_consensus_, &TestConsensus::on_block_accepted, node_idx_, instance_idx_, data, creator_idx,
                 signatures)
      .detach();
  co_return td::Unit{};
}

td::actor::Task<td::Ref<vm::Cell>> TestManagerFacade::wait_block_state_root(BlockIdExt block_id,
                                                                            td::Timestamp timeout) {
  co_return co_await td::actor::ask(test_consensus_, &TestConsensus::wait_block_state_root, block_id);
}

td::actor::Task<td::Ref<BlockData>> TestManagerFacade::wait_block_data(BlockIdExt block_id, td::Timestamp timeout) {
  co_return co_await td::actor::ask(test_consensus_, &TestConsensus::wait_block_data, block_id);
}

}  // namespace

int main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_WARNING);
  td::set_default_failure_signal_handler().ensure();

  td::OptionParser p;
  p.set_description("test consensus");
  p.add_option('h', "help", "prints_help", [&]() {
    std::cout << (PSLICE() << p).c_str();
    std::exit(2);
  });
  p.add_option('v', "verbosity", "set verbosity level", [&](td::Slice arg) {
    int v = VERBOSITY_NAME(FATAL) + (td::to_integer<int>(arg));
    SET_VERBOSITY_LEVEL(v);
  });
  p.add_checked_option('d', "duration", "test duration in seconds (default: 60)", [&](td::Slice arg) {
    DURATION = td::to_double(arg);
    if (DURATION < 0.0) {
      return td::Status::Error(PSTRING() << "invalid duration value " << arg);
    }
    return td::Status::OK();
  });
  p.add_option('m', "masterchain", "masterchain consensus (default is shardchain)", [&]() {
    SHARD = ShardIdFull{masterchainId};
    FIRST_PARENT.id.workchain = masterchainId;
    FIRST_PARENT.id.shard = shardIdAll;
    MIN_MC_BLOCK_ID = FIRST_PARENT;
  });
  p.add_checked_option('n', "n-nodes", "number of nodes (default: 8)", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(N_NODES, td::to_integer_safe<td::uint32>(arg));
    if (N_NODES == 0) {
      return td::Status::Error(PSTRING() << "invalid n-nodes value " << arg);
    }
    return td::Status::OK();
  });
  p.add_checked_option('\0', "n-double-nodes", "number of nodes with two instances (default: 0)", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(N_DOUBLE_NODES, td::to_integer_safe<td::uint32>(arg));
    return td::Status::OK();
  });
  p.add_checked_option('\0', "target-rate-ms", "target block rate in milliseconds (default: 1000)", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(TARGET_RATE_MS, td::to_integer_safe<td::uint32>(arg));
    return td::Status::OK();
  });
  p.add_checked_option('\0', "slots-per-leader-window", "slots per leader window (default: 4)", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(SLOTS_PER_LEADER_WINDOW, td::to_integer_safe<td::uint32>(arg));
    return td::Status::OK();
  });
  p.add_checked_option('\0', "net-ping", "network ping (range, default: 0.05:0.1)", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(NET_PING, parse_range(arg));
    if (NET_PING.first < 0.0) {
      return td::Status::Error(PSTRING() << "invalid ping value " << arg);
    }
    return td::Status::OK();
  });
  p.add_checked_option('\0', "net-loss", "packet loss probability (default: 0)", [&](td::Slice arg) {
    NET_LOSS = td::to_double(arg);
    if (NET_LOSS < 0.0 || NET_LOSS > 1.0) {
      return td::Status::Error(PSTRING() << "invalid loss value " << arg);
    }
    return td::Status::OK();
  });

  p.add_checked_option('\0', "gremlin-period", "gremlin period (range, default: no gremlin)", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(GREMLIN_PERIOD, parse_range(arg));
    if (GREMLIN_PERIOD.first < 0.0 || GREMLIN_PERIOD.second <= 0.0) {
      return td::Status::Error(PSTRING() << "invalid gremlin period value " << arg);
    }
    return td::Status::OK();
  });
  p.add_checked_option('\0', "gremlin-downtime", "gremlin downtime duration (range, default: 1)", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(GREMLIN_DOWNTIME, parse_range(arg));
    if (GREMLIN_DOWNTIME.first < 0.0) {
      return td::Status::Error(PSTRING() << "invalid gremlin downtime value " << arg);
    }
    return td::Status::OK();
  });
  p.add_checked_option('\0', "gremlin-n", "how many nodes gremlin restarts at once (range, default: 1)",
                       [&](td::Slice arg) {
                         TRY_RESULT_ASSIGN(GREMLIN_N, parse_int_range<size_t>(arg));
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "gremlin-times", "how many times gremlin runs (default: unlimited)", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(GREMLIN_TIMES, td::to_integer_safe<size_t>(arg));
    return td::Status::OK();
  });
  p.add_option('\0', "gremlin-kills-leader", "gremlin always restarts the current leader",
               [&]() { GREMLIN_KILLS_LEADER = true; });

  p.add_checked_option('\0', "net-gremlin-period", "network gremlin period (range, default: no gremlin)",
                       [&](td::Slice arg) {
                         TRY_RESULT_ASSIGN(NET_GREMLIN_PERIOD, parse_range(arg));
                         if (NET_GREMLIN_PERIOD.first < 0.0 || NET_GREMLIN_PERIOD.second <= 0.0) {
                           return td::Status::Error(PSTRING() << "invalid net gremlin period value " << arg);
                         }
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "net-gremlin-downtime", "network gremlin downtime duration (range, default: 10)",
                       [&](td::Slice arg) {
                         TRY_RESULT_ASSIGN(NET_GREMLIN_DOWNTIME, parse_range(arg));
                         if (NET_GREMLIN_DOWNTIME.first < 0.0) {
                           return td::Status::Error(PSTRING() << "invalid network gremlin downtime value " << arg);
                         }
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "net-gremlin-n", "how many nodes network gremlin disables at once (range, default: 1)",
                       [&](td::Slice arg) {
                         TRY_RESULT_ASSIGN(NET_GREMLIN_N, parse_int_range<size_t>(arg));
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "net-gremlin-times", "how many times network gremlin runs (default: unlimited)",
                       [&](td::Slice arg) {
                         TRY_RESULT_ASSIGN(NET_GREMLIN_TIMES, td::to_integer_safe<size_t>(arg));
                         return td::Status::OK();
                       });
  p.add_option('\0', "net-gremlin-kills-leader", "network gremlin always disables the current leader",
               [&]() { NET_GREMLIN_KILLS_LEADER = true; });
  p.add_checked_option('\0', "db-delay", "delay before db values are stored to disk (range, default: 0)",
                       [&](td::Slice arg) {
                         TRY_RESULT_ASSIGN(DB_DELAY, parse_range(arg));
                         if (DB_DELAY.first < 0.0) {
                           return td::Status::Error(PSTRING() << "invalid db delay value " << arg);
                         }
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "collation-time", "time it takes to collate a block (range, default: 0)",
                       [&](td::Slice arg) {
                         TRY_RESULT_ASSIGN(COLLATION_TIME, parse_range(arg));
                         if (COLLATION_TIME.first < 0.0) {
                           return td::Status::Error(PSTRING() << "invalid collation time " << arg);
                         }
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "validation-time", "time it takes to validate a block (range, default: 0)",
                       [&](td::Slice arg) {
                         TRY_RESULT_ASSIGN(VALIDATION_TIME, parse_range(arg));
                         if (VALIDATION_TIME.first < 0.0) {
                           return td::Status::Error(PSTRING() << "invalid validation time " << arg);
                         }
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "test-case", "named consensus assertion to run (default: smoke)",
                       [&](td::Slice arg) {
                         TRY_RESULT_ASSIGN(TEST_CASE, parse_test_case(arg));
                         return td::Status::OK();
                       });

  p.run(argc, argv).ensure();
  CHECK(N_DOUBLE_NODES <= N_NODES);

  td::actor::Scheduler scheduler({7});
  td::actor::ActorOwn<TestConsensus> test;

  scheduler.run_in_context([&] {
    test = td::actor::create_actor<TestConsensus>("test-consensus");
    td::actor::ask(test, &TestConsensus::run).detach();
  });
  while (scheduler.run(1)) {
  }

  return 0;
}
