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

#include <cmath>
#include <limits>

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
  SkipTimeoutUsesLastFinalization,
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
  SilentLeaderWindowSkipsButNextHonestWindowFinalizes,
  SilentValidatorDoesNotStopProgress,
  BadCandidateResolutionResponderDoesNotStopCatchup,
  ConsecutiveSilentLeadersThenProgress,
  LeaderCandidateToTooLittleWeightSkipsThenProgress,
  DuplicateInstanceSameKeySplitBrainStillProgresses,
  SleepingValidatorRecoversAfterHonestProgress,
  StandstillWithByzantineWithholdingStillRecovers,
  EmptyCandidatesWithSilentValidatorStillFinalize,
  NoNotarObservedSkipWins,
  LaterSlotWaitsForEarlierNotarProof,
  ReplayStaleCertificateDoesNotDisturbProgress,
  NotarizedAndSkippedSlotCoexistWithoutFinalization,
  FinalAndSkipRacePreservesLocalDiscipline,
  EquivocatingLeaderDoesNotCreateTwoNotarsOrFinals,
  DelayedFinalizationObservationPreservesPrefix,
  ForkDescendantsCollapseAfterLaterFinalization,
  MissingAncestorChainStillResolves,
  LaterSlotWaitsForGapSkipProof,
  InvalidCandidateSignatureDoesNotResolve,
  MalformedNotarCertificateDoesNotResolve,
  LongStallClearsSkippedSlotsThenHonestLeaderFinalizes,
  MultiSlotEquivocationWindowPreservesSafety,
  InvalidFullCandidateDoesNotNotarize,
  SkipTimeoutCapPlateausUnderStall,
  CountThresholdMismatchPreservesSafety,
  StandstillFloodStillRecovers,
  SeveralWindowsFullySkippedThenProgressResumes,
  AdjacentWindowForkRollsBackAfterLateAlternateFinalization,
  RestartAfterEightNonMcFinalizedBlocksReacceptsPendingCandidates,
  StandstillWithoutCertificateRebroadcastRequiresRecovery,
  GoodCandidateWithWrongSlotParentHashDoesNotResolve,
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
  if (s == "skip-timeout-uses-last-finalization") {
    return TestCase::SkipTimeoutUsesLastFinalization;
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
  if (s == "silent-leader-window-skips-but-next-honest-window-finalizes") {
    return TestCase::SilentLeaderWindowSkipsButNextHonestWindowFinalizes;
  }
  if (s == "silent-validator-does-not-stop-progress") {
    return TestCase::SilentValidatorDoesNotStopProgress;
  }
  if (s == "bad-candidate-resolution-responder-does-not-stop-catchup") {
    return TestCase::BadCandidateResolutionResponderDoesNotStopCatchup;
  }
  if (s == "consecutive-silent-leaders-then-progress") {
    return TestCase::ConsecutiveSilentLeadersThenProgress;
  }
  if (s == "leader-candidate-to-too-little-weight-skips-then-progress") {
    return TestCase::LeaderCandidateToTooLittleWeightSkipsThenProgress;
  }
  if (s == "duplicate-instance-same-key-split-brain-still-progresses") {
    return TestCase::DuplicateInstanceSameKeySplitBrainStillProgresses;
  }
  if (s == "sleeping-validator-recovers-after-honest-progress") {
    return TestCase::SleepingValidatorRecoversAfterHonestProgress;
  }
  if (s == "standstill-with-byzantine-withholding-still-recovers") {
    return TestCase::StandstillWithByzantineWithholdingStillRecovers;
  }
  if (s == "empty-candidates-with-silent-validator-still-finalize") {
    return TestCase::EmptyCandidatesWithSilentValidatorStillFinalize;
  }
  if (s == "no-notar-observed-skip-wins") {
    return TestCase::NoNotarObservedSkipWins;
  }
  if (s == "later-slot-waits-for-earlier-notar-proof") {
    return TestCase::LaterSlotWaitsForEarlierNotarProof;
  }
  if (s == "replay-stale-certificate-does-not-disturb-progress") {
    return TestCase::ReplayStaleCertificateDoesNotDisturbProgress;
  }
  if (s == "notarized-and-skipped-slot-coexist-without-finalization") {
    return TestCase::NotarizedAndSkippedSlotCoexistWithoutFinalization;
  }
  if (s == "final-and-skip-race-preserves-local-discipline") {
    return TestCase::FinalAndSkipRacePreservesLocalDiscipline;
  }
  if (s == "equivocating-leader-does-not-create-two-notars-or-finals") {
    return TestCase::EquivocatingLeaderDoesNotCreateTwoNotarsOrFinals;
  }
  if (s == "delayed-finalization-observation-preserves-prefix") {
    return TestCase::DelayedFinalizationObservationPreservesPrefix;
  }
  if (s == "fork-descendants-collapse-after-later-finalization") {
    return TestCase::ForkDescendantsCollapseAfterLaterFinalization;
  }
  if (s == "missing-ancestor-chain-still-resolves") {
    return TestCase::MissingAncestorChainStillResolves;
  }
  if (s == "later-slot-waits-for-gap-skip-proof") {
    return TestCase::LaterSlotWaitsForGapSkipProof;
  }
  if (s == "invalid-candidate-signature-does-not-resolve") {
    return TestCase::InvalidCandidateSignatureDoesNotResolve;
  }
  if (s == "malformed-notar-certificate-does-not-resolve") {
    return TestCase::MalformedNotarCertificateDoesNotResolve;
  }
  if (s == "long-stall-clears-skipped-slots-then-honest-leader-finalizes") {
    return TestCase::LongStallClearsSkippedSlotsThenHonestLeaderFinalizes;
  }
  if (s == "multi-slot-equivocation-window-preserves-safety") {
    return TestCase::MultiSlotEquivocationWindowPreservesSafety;
  }
  if (s == "invalid-full-candidate-does-not-notarize") {
    return TestCase::InvalidFullCandidateDoesNotNotarize;
  }
  if (s == "skip-timeout-cap-plateaus-under-stall") {
    return TestCase::SkipTimeoutCapPlateausUnderStall;
  }
  if (s == "count-threshold-mismatch-preserves-safety") {
    return TestCase::CountThresholdMismatchPreservesSafety;
  }
  if (s == "standstill-flood-still-recovers") {
    return TestCase::StandstillFloodStillRecovers;
  }
  if (s == "several-windows-fully-skipped-then-progress-resumes") {
    return TestCase::SeveralWindowsFullySkippedThenProgressResumes;
  }
  if (s == "adjacent-window-fork-rolls-back-after-late-alternate-finalization") {
    return TestCase::AdjacentWindowForkRollsBackAfterLateAlternateFinalization;
  }
  if (s == "restart-after-eight-non-mc-finalized-blocks-reaccepts-pending-candidates") {
    return TestCase::RestartAfterEightNonMcFinalizedBlocksReacceptsPendingCandidates;
  }
  if (s == "standstill-without-certificate-rebroadcast-requires-recovery") {
    return TestCase::StandstillWithoutCertificateRebroadcastRequiresRecovery;
  }
  if (s == "good-candidate-with-wrong-slot-parent-hash-does-not-resolve") {
    return TestCase::GoodCandidateWithWrongSlotParentHashDoesNotResolve;
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
double FIRST_BLOCK_TIMEOUT_S = 1.0;
double FIRST_BLOCK_TIMEOUT_MULTIPLIER = 1.05;
double FIRST_BLOCK_MAX_TIMEOUT_S = 100.0;
std::vector<ValidatorWeight> NODE_WEIGHTS_OVERRIDE;

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
  enum class MalformedCandidateResponseKind { Junk, CorruptCandidate, CorruptNotar };
  enum class ProtocolKind {
    NotarVote,
    SkipVote,
    FinalVote,
    NotarCert,
    SkipCert,
    FinalCert,
  };
  struct ProtocolDropRule {
    std::optional<size_t> src_node_idx;
    std::optional<size_t> src_instance_idx;
    std::set<size_t> dst_node_indices;
    td::uint32 start_slot = 0;
    td::uint32 end_slot = 0;
    ProtocolKind kind = ProtocolKind::NotarVote;
  };
  struct CandidateDropRule {
    std::optional<size_t> src_node_idx;
    std::optional<size_t> src_instance_idx;
    std::set<size_t> dst_node_indices;
    td::uint32 start_slot = 0;
    td::uint32 end_slot = 0;
  };
  std::vector<ProtocolDropRule> protocol_drop_rules;
  std::vector<CandidateDropRule> candidate_drop_rules;
  std::optional<size_t> force_candidate_request_src_node;
  std::optional<size_t> force_candidate_request_src_instance;
  std::optional<size_t> force_candidate_request_dst_node;
  std::optional<size_t> malformed_candidate_response_src_node;
  std::optional<size_t> malformed_candidate_response_src_instance;
  std::optional<size_t> malformed_candidate_response_dst_node;
  size_t malformed_candidate_response_budget = 0;
  MalformedCandidateResponseKind malformed_candidate_response_kind = MalformedCandidateResponseKind::Junk;
};

std::mutex test_message_filters_mutex;
TestMessageFilters test_message_filters;

ValidatorWeight configured_node_weight(size_t idx) {
  if (!NODE_WEIGHTS_OVERRIDE.empty()) {
    CHECK(idx < NODE_WEIGHTS_OVERRIDE.size());
    return NODE_WEIGHTS_OVERRIDE[idx];
  }
  return 11;
}

ValidatorWeight configured_total_weight() {
  ValidatorWeight total_weight = 0;
  for (size_t idx = 0; idx < N_NODES; ++idx) {
    total_weight += configured_node_weight(idx);
  }
  return total_weight;
}

ValidatorWeight configured_quorum_weight() {
  return (configured_total_weight() * 2) / 3 + 1;
}

struct TestExpectations {
  std::optional<CandidateId> candidate_resolution_target;
  std::optional<td::uint32> skip_timeout_last_finalized_window;
  std::optional<td::uint32> skip_timeout_stalled_window;
  std::optional<td::uint32> skip_timeout_cap_start_slot;
  size_t skip_timeout_cap_slot_count = 0;
  std::vector<size_t> adversarial_nodes;
  std::optional<td::uint32> consecutive_skip_start_slot;
  size_t consecutive_skip_count = 0;
  std::optional<td::uint32> candidate_starvation_slot;
  std::vector<size_t> candidate_starvation_dropped_nodes;
  std::optional<td::uint32> no_notarization_slot;
  std::optional<td::uint32> parent_gating_slot;
  std::optional<td::uint32> replayed_stale_slot;
  std::optional<td::uint32> forked_slot;
  std::optional<td::uint32> equivocation_slot;
  std::vector<CandidateId> equivocation_candidates;
  std::optional<td::uint32> delayed_finalization_from_slot;
  std::optional<size_t> delayed_finalization_node_idx;
  std::optional<double> delayed_finalization_release_ts;
  std::optional<td::uint32> gap_skip_slot;
  std::optional<td::uint32> long_stall_start_slot;
  size_t long_stall_slot_count = 0;
  std::optional<td::uint32> multi_slot_equivocation_start_slot;
  size_t multi_slot_equivocation_slot_count = 0;
  std::optional<td::uint32> invalid_full_candidate_slot;
  std::optional<td::uint32> count_weight_mismatch_slot;
  std::optional<td::uint32> standstill_recovery_slot;
  std::optional<double> standstill_recovery_release_ts;
  std::optional<CandidateId> wrong_parent_hash_candidate_id;
  std::optional<td::uint32> restart_reaccept_stop_slot;
};

std::mutex test_expectations_mutex;
TestExpectations test_expectations;

struct ProtocolVoteTraceRecord {
  double ts = 0.0;
  size_t src_node_idx = 0;
  size_t src_instance_idx = 0;
  std::optional<size_t> dst_node_idx;
  simplex::Vote vote;
  std::string raw_message;
};

struct ProtocolCertificateTraceRecord {
  double ts = 0.0;
  size_t src_node_idx = 0;
  size_t src_instance_idx = 0;
  std::optional<size_t> dst_node_idx;
  simplex::Vote vote;
  std::vector<PeerValidatorId> signers;
  std::string raw_message;
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

struct MalformedCandidateResponseTraceRecord {
  double ts = 0.0;
  size_t requester_node_idx = 0;
  size_t requester_instance_idx = 0;
  size_t responder_node_idx = 0;
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
  std::vector<MalformedCandidateResponseTraceRecord> malformed_candidate_responses;
};

std::string certificate_trace_key(const ProtocolCertificateTraceRecord& record);
std::string vote_trace_key(const simplex::Vote& vote);
std::set<size_t> adversarial_nodes();
td::Status verify_adversarial_invariants(const TraceSnapshot& snapshot);

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

std::set<size_t> adversarial_nodes() {
  switch (TEST_CASE) {
    case TestCase::SilentLeaderWindowSkipsButNextHonestWindowFinalizes:
      return {0};
    case TestCase::SilentValidatorDoesNotStopProgress:
      return {3};
    case TestCase::BadCandidateResolutionResponderDoesNotStopCatchup:
      return {1};
    case TestCase::ConsecutiveSilentLeadersThenProgress: {
      std::scoped_lock lock(test_expectations_mutex);
      return {test_expectations.adversarial_nodes.begin(), test_expectations.adversarial_nodes.end()};
    }
    case TestCase::LeaderCandidateToTooLittleWeightSkipsThenProgress: {
      std::scoped_lock lock(test_expectations_mutex);
      return {test_expectations.adversarial_nodes.begin(), test_expectations.adversarial_nodes.end()};
    }
    case TestCase::DuplicateInstanceSameKeySplitBrainStillProgresses:
      return {0};
    case TestCase::SleepingValidatorRecoversAfterHonestProgress:
      return {3};
    case TestCase::StandstillWithByzantineWithholdingStillRecovers:
      return {1};
    case TestCase::EmptyCandidatesWithSilentValidatorStillFinalize:
      return {3};
    case TestCase::NoNotarObservedSkipWins: {
      std::scoped_lock lock(test_expectations_mutex);
      return {test_expectations.adversarial_nodes.begin(), test_expectations.adversarial_nodes.end()};
    }
    case TestCase::ReplayStaleCertificateDoesNotDisturbProgress:
      return {1};
    case TestCase::NotarizedAndSkippedSlotCoexistWithoutFinalization:
      return {0};
    case TestCase::FinalAndSkipRacePreservesLocalDiscipline:
      return {};
    case TestCase::EquivocatingLeaderDoesNotCreateTwoNotarsOrFinals:
      return {0};
    case TestCase::DelayedFinalizationObservationPreservesPrefix:
      return {};
    case TestCase::ForkDescendantsCollapseAfterLaterFinalization:
      return {0};
    case TestCase::MissingAncestorChainStillResolves:
      return {1};
    case TestCase::LaterSlotWaitsForGapSkipProof:
      return {1};
    case TestCase::InvalidCandidateSignatureDoesNotResolve:
      return {1};
    case TestCase::MalformedNotarCertificateDoesNotResolve:
      return {1};
    case TestCase::LongStallClearsSkippedSlotsThenHonestLeaderFinalizes:
      return {};
    case TestCase::MultiSlotEquivocationWindowPreservesSafety:
      return {0};
    case TestCase::InvalidFullCandidateDoesNotNotarize: {
      std::scoped_lock lock(test_expectations_mutex);
      return {test_expectations.adversarial_nodes.begin(), test_expectations.adversarial_nodes.end()};
    }
    case TestCase::SkipTimeoutCapPlateausUnderStall: {
      std::scoped_lock lock(test_expectations_mutex);
      return {test_expectations.adversarial_nodes.begin(), test_expectations.adversarial_nodes.end()};
    }
    case TestCase::CountThresholdMismatchPreservesSafety:
      return {3, 4, 5};
    case TestCase::StandstillFloodStillRecovers:
      return {1};
    case TestCase::RestartAfterEightNonMcFinalizedBlocksReacceptsPendingCandidates:
      return {0};
    default:
      return {};
  }
}

td::Status verify_adversarial_invariants(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md §1.4 and the system model:
  // with Byzantine weight < W/3, honest validators must keep local vote discipline and the protocol
  // must not form conflicting quorum certificates.
  auto byzantine = adversarial_nodes();

  std::map<std::pair<size_t, td::uint32>, CandidateId> first_honest_notar_by_slot;
  std::map<std::pair<size_t, td::uint32>, CandidateId> first_honest_final_by_slot;
  std::map<size_t, std::set<td::uint32>> honest_skip_slots;
  std::map<td::uint32, CandidateId> notar_certificate_by_slot;
  std::map<td::uint32, CandidateId> final_certificate_by_slot;
  std::set<td::uint32> skipped_slots;

  for (const auto& record : snapshot.protocol_votes) {
    if (byzantine.contains(record.src_node_idx)) {
      continue;
    }
    if (auto* notar = std::get_if<simplex::NotarizeVote>(&record.vote.vote)) {
      auto key = std::make_pair(record.src_node_idx, notar->id.slot);
      auto [it, inserted] = first_honest_notar_by_slot.emplace(key, notar->id);
      if (!inserted && it->second != notar->id) {
        return td::Status::Error(PSTRING() << "honest validator #" << record.src_node_idx
                                           << " sent conflicting Notar votes for slot " << notar->id.slot
                                           << ": first=" << it->second << ", later=" << notar->id);
      }
      continue;
    }
    if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote)) {
      auto key = std::make_pair(record.src_node_idx, skip->slot);
      honest_skip_slots[record.src_node_idx].insert(skip->slot);
      if (first_honest_final_by_slot.contains(key)) {
        return td::Status::Error(PSTRING() << "honest validator #" << record.src_node_idx
                                           << " sent both Skip and Final for slot " << skip->slot);
      }
      continue;
    }
    if (auto* final = std::get_if<simplex::FinalizeVote>(&record.vote.vote)) {
      auto key = std::make_pair(record.src_node_idx, final->id.slot);
      auto [it, inserted] = first_honest_final_by_slot.emplace(key, final->id);
      if (!inserted && it->second != final->id) {
        return td::Status::Error(PSTRING() << "honest validator #" << record.src_node_idx
                                           << " sent conflicting Final votes for slot " << final->id.slot
                                           << ": first=" << it->second << ", later=" << final->id);
      }
      if (honest_skip_slots[record.src_node_idx].contains(final->id.slot)) {
        return td::Status::Error(PSTRING() << "honest validator #" << record.src_node_idx
                                           << " sent both Final and Skip for slot " << final->id.slot);
      }
    }
  }

  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* notar = std::get_if<simplex::NotarizeVote>(&record.vote.vote)) {
      auto [it, inserted] = notar_certificate_by_slot.emplace(notar->id.slot, notar->id);
      if (!inserted && it->second != notar->id) {
        return td::Status::Error(PSTRING() << "conflicting notarization certificates for slot "
                                           << notar->id.slot << ": first=" << it->second
                                           << ", later=" << notar->id);
      }
      continue;
    }
    if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote)) {
      skipped_slots.insert(skip->slot);
      continue;
    }
    if (auto* final = std::get_if<simplex::FinalizeVote>(&record.vote.vote)) {
      auto [it, inserted] = final_certificate_by_slot.emplace(final->id.slot, final->id);
      if (!inserted && it->second != final->id) {
        return td::Status::Error(PSTRING() << "conflicting finalization certificates for slot "
                                           << final->id.slot << ": first=" << it->second
                                           << ", later=" << final->id);
      }
    }
  }

  for (const auto& [slot, id] : final_certificate_by_slot) {
    if (skipped_slots.contains(slot)) {
      return td::Status::Error(PSTRING() << "slot " << slot
                                         << " reached both Final and Skip certificates; finalized candidate=" << id);
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
  td::uint64 quorum_weight = configured_quorum_weight();

  if (snapshot.protocol_certificates.empty()) {
    return td::Status::Error("scenario did not produce any certificates");
  }

  for (const auto& record : snapshot.protocol_certificates) {
    std::set<size_t> unique_signers;
    for (PeerValidatorId signer : record.signers) {
      unique_signers.insert(signer.value());
    }
    td::uint64 signer_weight = 0;
    for (size_t signer : unique_signers) {
      signer_weight += configured_node_weight(signer);
    }
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

td::uint32 vote_trace_slot(const simplex::Vote& vote) {
  auto slot_fn = [&](const auto& typed_vote) -> td::uint32 {
    using T = std::decay_t<decltype(typed_vote)>;
    if constexpr (std::same_as<T, simplex::SkipVote>) {
      return typed_vote.slot;
    } else {
      return typed_vote.id.slot;
    }
  };
  return std::visit(slot_fn, vote.vote);
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

td::Status verify_skip_timeout_uses_last_finalization(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md Rule 6 and §4.2:
  // skip timeout in window k must be lower-bounded by T0 * alpha^(k-k*-1), where k* is determined
  // by the last observed finalization when window k becomes active.
  std::optional<td::uint32> last_finalized_window;
  std::optional<td::uint32> earliest_expected_skip_window;
  {
    std::scoped_lock lock(test_expectations_mutex);
    last_finalized_window = test_expectations.skip_timeout_last_finalized_window;
    earliest_expected_skip_window = test_expectations.skip_timeout_stalled_window;
  }
  if (!last_finalized_window.has_value() || !earliest_expected_skip_window.has_value()) {
    return td::Status::Error("Rule 6 scenario did not register the expected windows");
  }

  std::optional<td::uint32> actual_skip_window;
  double first_skip_ts = 0.0;
  for (const auto& record : snapshot.protocol_votes) {
    if (record.src_node_idx != 0 || record.src_instance_idx != 0) {
      continue;
    }
    auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote);
    if (skip == nullptr) {
      continue;
    }
    td::uint32 window = skip->slot / SLOTS_PER_LEADER_WINDOW;
    if (window < *earliest_expected_skip_window) {
      continue;
    }
    if (!actual_skip_window.has_value() || window < *actual_skip_window ||
        (window == *actual_skip_window && record.ts < first_skip_ts)) {
      actual_skip_window = window;
      first_skip_ts = record.ts;
    }
  }
  if (!actual_skip_window.has_value()) {
    std::set<td::uint32> skip_windows;
    for (const auto& record : snapshot.protocol_votes) {
      if (record.src_node_idx != 0 || record.src_instance_idx != 0) {
        continue;
      }
      auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote);
      if (skip == nullptr) {
        continue;
      }
      skip_windows.insert(skip->slot / SLOTS_PER_LEADER_WINDOW);
    }
    td::StringBuilder sb;
    sb << "[";
    bool first = true;
    for (td::uint32 window : skip_windows) {
      if (!first) {
        sb << ",";
      }
      first = false;
      sb << window;
    }
    sb << "]";
    return td::Status::Error(PSTRING() << "scenario did not produce any SkipVote in window "
                                       << *earliest_expected_skip_window << " or later; observed skip windows="
                                       << sb.as_cslice());
  }

  std::optional<double> stalled_window_start_ts;
  td::uint32 stalled_window_start_slot = *actual_skip_window * SLOTS_PER_LEADER_WINDOW;
  for (const auto& record : snapshot.leader_windows_observed) {
    if (record.node_idx == 0 && record.instance_idx == 0 && record.start_slot == stalled_window_start_slot) {
      stalled_window_start_ts = record.ts;
      break;
    }
  }
  if (!stalled_window_start_ts.has_value()) {
    return td::Status::Error(PSTRING() << "scenario did not observe skip window " << *actual_skip_window
                                       << " on validator #0.0");
  }

  double observed_delay_s = first_skip_ts - *stalled_window_start_ts;
  double spec_lower_bound_s =
      FIRST_BLOCK_TIMEOUT_S *
      std::pow(FIRST_BLOCK_TIMEOUT_MULTIPLIER, *actual_skip_window - *last_finalized_window - 1);
  if (observed_delay_s + 0.15 < spec_lower_bound_s) {
    return td::Status::Error(PSTRING() << "validator #0.0 skipped window " << *actual_skip_window << " after "
                                       << observed_delay_s << "s, below Rule 6 lower bound " << spec_lower_bound_s
                                       << "s from last finalized window " << *last_finalized_window);
  }

  return td::Status::OK();
}

td::Status verify_skip_timeout_cap_plateaus_under_stall(const TraceSnapshot& snapshot) {
  // Covers D5 from the adversarial plan in a scaled form:
  // under repeated silent-leader windows, the local skip timeout should stop growing once it reaches
  // the configured cap, while safety invariants still hold during the prolonged stall.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> start_slot;
  size_t slot_count = 0;
  {
    std::scoped_lock lock(test_expectations_mutex);
    start_slot = test_expectations.skip_timeout_cap_start_slot;
    slot_count = test_expectations.skip_timeout_cap_slot_count;
  }
  if (!start_slot.has_value() || slot_count < 3) {
    return td::Status::Error("scenario did not register the skip-timeout cap slot range");
  }

  std::vector<double> observed_delays;
  for (size_t offset = 0; offset < slot_count; ++offset) {
    td::uint32 slot = *start_slot + static_cast<td::uint32>(offset);
    std::optional<double> window_ts;
    for (const auto& record : snapshot.leader_windows_observed) {
      if (record.node_idx == 0 && record.instance_idx == 0 && record.start_slot == slot) {
        window_ts = record.ts;
        break;
      }
    }
    if (!window_ts.has_value()) {
      return td::Status::Error(PSTRING() << "validator #0.0 never observed attacked window " << slot);
    }

    std::optional<double> skip_ts;
    for (const auto& record : snapshot.protocol_votes) {
      if (record.src_node_idx != 0 || record.src_instance_idx != 0) {
        continue;
      }
      if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote);
          skip != nullptr && skip->slot == slot) {
        skip_ts = record.ts;
        break;
      }
    }
    if (!skip_ts.has_value()) {
      return td::Status::Error(PSTRING() << "validator #0.0 never emitted Skip for attacked slot " << slot);
    }
    observed_delays.push_back(*skip_ts - *window_ts);
  }

  if (observed_delays[1] <= observed_delays[0] + 0.05) {
    return td::Status::Error(PSTRING() << "skip timeout did not materially grow before hitting the cap: first delay="
                                       << observed_delays[0] << "s, second delay=" << observed_delays[1] << "s");
  }

  double expected_capped_delay_s = FIRST_BLOCK_MAX_TIMEOUT_S + TARGET_RATE_MS / 1000.0;
  double capped_gap_s = std::abs(observed_delays[slot_count - 1] - observed_delays[slot_count - 2]);
  if (capped_gap_s > 0.12) {
    return td::Status::Error(PSTRING() << "skip timeout did not plateau at the configured cap: last delays="
                                       << observed_delays[slot_count - 2] << "s and "
                                       << observed_delays[slot_count - 1] << "s");
  }
  if (std::abs(observed_delays[slot_count - 1] - expected_capped_delay_s) > 0.12) {
    return td::Status::Error(PSTRING() << "capped skip delay " << observed_delays[slot_count - 1]
                                       << "s did not match configured cap-based expectation "
                                       << expected_capped_delay_s << "s");
  }

  td::uint32 last_attacked_slot = *start_slot + static_cast<td::uint32>(slot_count) - 1;
  bool saw_later_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 0 && record.instance_idx == 0 && record.id.slot > last_attacked_slot) {
      saw_later_finalization = true;
      break;
    }
  }
  if (!saw_later_finalization) {
    return td::Status::Error(PSTRING() << "validator #0.0 never finalized past capped-stall slot "
                                       << last_attacked_slot);
  }
  return td::Status::OK();
}

td::Status verify_silent_leader_window_skips_but_next_honest_window_finalizes(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md Rule 6 and the liveness argument:
  // if one leader is silent while Byzantine weight stays below W/3, its window should skip and a
  // later honest leader window should still finalize.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<double> stop_ts;
  for (const auto& record : snapshot.lifecycle) {
    if (record.node_idx == 0 && record.instance_idx == 0 && !record.started) {
      stop_ts = record.ts;
      break;
    }
  }
  if (!stop_ts.has_value()) {
    return td::Status::Error("scenario did not record the silent leader stop event");
  }

  std::optional<td::uint32> attacked_slot;
  for (const auto& record : snapshot.leader_windows_observed) {
    if (record.ts <= *stop_ts + 1e-9) {
      continue;
    }
    if ((record.start_slot / SLOTS_PER_LEADER_WINDOW) % N_NODES == 0) {
      if (!attacked_slot.has_value() || record.start_slot < *attacked_slot) {
        attacked_slot = record.start_slot;
      }
    }
  }
  if (!attacked_slot.has_value()) {
    return td::Status::Error("scenario did not observe any post-stop leader window for the silent validator");
  }

  for (const auto& record : snapshot.candidate_deliveries) {
    if (record.src_node_idx == 0 && record.src_instance_idx == 0 && record.candidate_id.slot == *attacked_slot &&
        record.ts > *stop_ts + 1e-9) {
      return td::Status::Error(PSTRING() << "silent leader still delivered candidate " << record.candidate_id
                                         << " after stop");
    }
  }

  bool saw_skip_cert = false;
  for (const auto& record : snapshot.protocol_certificates) {
    auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote);
    if (skip != nullptr && skip->slot == *attacked_slot) {
      saw_skip_cert = true;
      break;
    }
  }
  if (!saw_skip_cert) {
    return td::Status::Error(PSTRING() << "silent leader slot " << *attacked_slot
                                       << " did not reach a Skip certificate");
  }

  td::uint32 highest_finalized_slot = 0;
  bool saw_later_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 0) {
      continue;
    }
    highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
    if (record.id.slot > *attacked_slot) {
      saw_later_finalization = true;
    }
  }
  if (!saw_later_finalization) {
    return td::Status::Error(PSTRING() << "honest validators never finalized past silent leader slot "
                                       << *attacked_slot << "; highest finalized slot=" << highest_finalized_slot);
  }

  return td::Status::OK();
}

td::Status verify_silent_validator_does_not_stop_progress(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md system model and liveness:
  // with one silent validator and honest weight still above quorum, certificates and later
  // finalizations must keep forming.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<double> stop_ts;
  for (const auto& record : snapshot.lifecycle) {
    if (record.node_idx == 3 && record.instance_idx == 0 && !record.started) {
      stop_ts = record.ts;
      break;
    }
  }
  if (!stop_ts.has_value()) {
    return td::Status::Error("scenario did not record the silent validator stop event");
  }

  td::uint32 highest_finalized_slot = 0;
  bool saw_post_stop_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 3) {
      continue;
    }
    if (record.ts <= *stop_ts + 1e-9) {
      continue;
    }
    highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
    saw_post_stop_finalization = true;
  }
  if (!saw_post_stop_finalization) {
    return td::Status::Error("honest validators observed no finalization after the silent validator stopped");
  }

  bool saw_post_stop_certificate = false;
  for (const auto& record : snapshot.protocol_certificates) {
    if (record.src_node_idx == 3 || record.ts <= *stop_ts + 1e-9) {
      continue;
    }
    saw_post_stop_certificate = true;
    break;
  }
  if (!saw_post_stop_certificate) {
    return td::Status::Error("honest validators formed no certificates after the silent validator stopped");
  }

  if (highest_finalized_slot == 0) {
    return td::Status::Error("scenario only finalized slot 0 after the silent validator stopped");
  }
  return td::Status::OK();
}

td::Status verify_bad_candidate_resolution_responder_does_not_stop_catchup(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md Rule 2 under Byzantine responses:
  // malformed peer replies must not poison later honest recovery, and consensus should keep
  // advancing while the malicious responder still has weight below W/3.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<CandidateId> expected_target;
  {
    std::scoped_lock lock(test_expectations_mutex);
    expected_target = test_expectations.candidate_resolution_target;
  }
  if (!expected_target.has_value()) {
    return td::Status::Error("scenario did not register a candidate-resolution target");
  }

  size_t requests_to_malicious_peer = 0;
  size_t requests_to_honest_peer = 0;
  for (const auto& record : snapshot.overlay_requests) {
    if (record.src_node_idx != 0 || record.src_instance_idx != 0 || record.candidate_id != expected_target) {
      continue;
    }
    if (record.dst_node_idx == 1) {
      ++requests_to_malicious_peer;
    }
    if (record.dst_node_idx == 3) {
      ++requests_to_honest_peer;
    }
  }
  if (requests_to_malicious_peer == 0) {
    return td::Status::Error("scenario never sent requestCandidate traffic to the malicious responder");
  }
  if (requests_to_honest_peer == 0) {
    return td::Status::Error("scenario never retried requestCandidate against an honest responder");
  }

  std::optional<double> last_malformed_response_ts;
  for (const auto& record : snapshot.malformed_candidate_responses) {
    if (record.requester_node_idx == 0 && record.requester_instance_idx == 0 && record.responder_node_idx == 1 &&
        record.id == *expected_target) {
      last_malformed_response_ts = std::max(last_malformed_response_ts.value_or(0.0), record.ts);
    }
  }
  if (!last_malformed_response_ts.has_value()) {
    return td::Status::Error("scenario did not inject any malformed candidate response from the malicious peer");
  }

  std::optional<double> resolved_ts;
  for (const auto& record : snapshot.candidates_resolved) {
    if (record.node_idx == 0 && record.instance_idx == 0 && record.id == *expected_target) {
      resolved_ts = record.ts;
      break;
    }
  }
  if (!resolved_ts.has_value()) {
    return td::Status::Error(PSTRING() << "validator #0.0 never resolved target " << *expected_target
                                       << " after malformed candidate responses");
  }
  if (*resolved_ts <= *last_malformed_response_ts + 1e-9) {
    return td::Status::Error("candidate resolution completed before the last malformed response was injected");
  }

  bool saw_later_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 1) {
      continue;
    }
    if (record.ts > *last_malformed_response_ts + 1e-9) {
      saw_later_finalization = true;
      break;
    }
  }
  if (!saw_later_finalization) {
    return td::Status::Error("honest validators observed no finalization after the malicious response phase");
  }

  return td::Status::OK();
}

td::Status verify_consecutive_silent_leaders_then_progress(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md Rule 6 and the liveness argument under f < W/3:
  // even after back-to-back silent leader slots, honest validators should clear both slots by Skip
  // and later honest leaders should still finalize.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> start_slot;
  size_t skip_count = 0;
  std::vector<size_t> byzantine_nodes;
  {
    std::scoped_lock lock(test_expectations_mutex);
    start_slot = test_expectations.consecutive_skip_start_slot;
    skip_count = test_expectations.consecutive_skip_count;
    byzantine_nodes = test_expectations.adversarial_nodes;
  }
  if (!start_slot.has_value() || skip_count < 2 || byzantine_nodes.size() < 2) {
    return td::Status::Error("scenario did not register the expected consecutive-silent-leader window");
  }

  std::map<size_t, double> stop_ts_by_node;
  for (const auto& record : snapshot.lifecycle) {
    if (!record.started) {
      stop_ts_by_node.emplace(record.node_idx, record.ts);
    }
  }
  for (size_t node_idx : byzantine_nodes) {
    if (!stop_ts_by_node.contains(node_idx)) {
      return td::Status::Error(PSTRING() << "scenario did not record stop event for silent leader #" << node_idx);
    }
  }

  for (size_t i = 0; i < skip_count; ++i) {
    td::uint32 slot = *start_slot + static_cast<td::uint32>(i);
    bool saw_skip_cert = false;
    for (const auto& record : snapshot.protocol_certificates) {
      if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote); skip != nullptr && skip->slot == slot) {
        saw_skip_cert = true;
        break;
      }
    }
    if (!saw_skip_cert) {
      return td::Status::Error(PSTRING() << "silent leader slot " << slot << " did not reach a Skip certificate");
    }
  }

  for (size_t i = 0; i < skip_count; ++i) {
    size_t node_idx = byzantine_nodes[i];
    td::uint32 slot = *start_slot + static_cast<td::uint32>(i);
    double stop_ts = stop_ts_by_node[node_idx];
    for (const auto& record : snapshot.candidate_deliveries) {
      if (record.src_node_idx == node_idx && record.candidate_id.slot == slot && record.ts > stop_ts + 1e-9) {
        return td::Status::Error(PSTRING() << "silent leader #" << node_idx
                                           << " still delivered candidate " << record.candidate_id
                                           << " after stop");
      }
    }
  }

  td::uint32 last_skipped_slot = *start_slot + static_cast<td::uint32>(skip_count - 1);
  bool saw_later_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (std::find(byzantine_nodes.begin(), byzantine_nodes.end(), record.node_idx) != byzantine_nodes.end()) {
      continue;
    }
    if (record.id.slot > last_skipped_slot) {
      saw_later_finalization = true;
      break;
    }
  }
  if (!saw_later_finalization) {
    return td::Status::Error(PSTRING() << "honest validators never finalized past consecutive skipped slots ["
                                       << *start_slot << ", " << (last_skipped_slot + 1) << ")");
  }

  return td::Status::OK();
}

td::Status verify_leader_candidate_to_too_little_weight_skips_then_progress(const TraceSnapshot& snapshot) {
  // Covers simplex_docs.md Rule 4 and Rule 6 under Byzantine delivery:
  // if a leader only reveals its candidate to too little weight, the slot must not notarize, should
  // eventually skip, and later honest slots must still finalize.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> attacked_slot;
  std::vector<size_t> byzantine_nodes;
  std::vector<size_t> dropped_nodes;
  {
    std::scoped_lock lock(test_expectations_mutex);
    attacked_slot = test_expectations.candidate_starvation_slot;
    byzantine_nodes = test_expectations.adversarial_nodes;
    dropped_nodes = test_expectations.candidate_starvation_dropped_nodes;
  }
  if (!attacked_slot.has_value() || byzantine_nodes.size() != 1 || dropped_nodes.size() < 2) {
    return td::Status::Error("scenario did not register the candidate-starvation expectations");
  }
  size_t leader_node_idx = byzantine_nodes.front();

  std::set<size_t> recipients;
  for (const auto& record : snapshot.candidate_deliveries) {
    if (record.src_node_idx == leader_node_idx && record.candidate_id.slot == *attacked_slot) {
      recipients.insert(record.dst_node_idx);
    }
  }
  for (size_t dropped_node : dropped_nodes) {
    if (recipients.contains(dropped_node)) {
      return td::Status::Error(PSTRING() << "malicious leader #" << leader_node_idx
                                         << " still delivered candidate for slot " << *attacked_slot
                                         << " to dropped validator #" << dropped_node);
    }
  }
  if (recipients.size() != 1) {
    return td::Status::Error(PSTRING() << "expected the malicious leader to reach exactly one peer for slot "
                                       << *attacked_slot << ", got " << recipients.size());
  }

  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* notar = std::get_if<simplex::NotarizeVote>(&record.vote.vote);
        notar != nullptr && notar->id.slot == *attacked_slot) {
      return td::Status::Error(PSTRING() << "slot " << *attacked_slot
                                         << " still reached a notarization certificate under candidate starvation");
    }
  }

  bool saw_skip_cert = false;
  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote); skip != nullptr && skip->slot == *attacked_slot) {
      saw_skip_cert = true;
      break;
    }
  }
  if (!saw_skip_cert) {
    return td::Status::Error(PSTRING() << "candidate-starved slot " << *attacked_slot
                                       << " did not reach a Skip certificate");
  }

  bool saw_later_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == leader_node_idx) {
      continue;
    }
    if (record.id.slot > *attacked_slot) {
      saw_later_finalization = true;
      break;
    }
  }
  if (!saw_later_finalization) {
    return td::Status::Error(PSTRING() << "honest validators never finalized past candidate-starved slot "
                                       << *attacked_slot);
  }

  return td::Status::OK();
}

td::Status verify_count_threshold_mismatch_preserves_safety(const TraceSnapshot& snapshot) {
  // Covers D6 from the adversarial plan under the fake-overlay harness:
  // when candidate availability is biased by recipient count rather than validator weight, a slot that
  // reaches many low-weight nodes must still fail safely if the reachable total weight stays below quorum.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> attacked_slot;
  {
    std::scoped_lock lock(test_expectations_mutex);
    attacked_slot = test_expectations.count_weight_mismatch_slot;
  }
  if (!attacked_slot.has_value()) {
    return td::Status::Error("scenario did not register the count-vs-weight attacked slot");
  }

  std::set<size_t> candidate_holders;
  for (const auto& record : snapshot.candidates_generated) {
    if (record.candidate_id.slot == *attacked_slot) {
      candidate_holders.insert(record.node_idx);
    }
  }
  for (const auto& record : snapshot.candidate_deliveries) {
    if (record.candidate_id.slot == *attacked_slot) {
      candidate_holders.insert(record.dst_node_idx);
    }
  }
  if (candidate_holders.size() <= N_NODES / 2) {
    return td::Status::Error(PSTRING() << "attacked slot " << *attacked_slot
                                       << " never reached a count-majority of validators; holders="
                                       << candidate_holders.size());
  }

  ValidatorWeight holder_weight = 0;
  for (size_t node_idx : candidate_holders) {
    holder_weight += configured_node_weight(node_idx);
  }
  if (holder_weight >= configured_quorum_weight()) {
    return td::Status::Error(PSTRING() << "attacked slot " << *attacked_slot
                                       << " unexpectedly reached quorum weight " << holder_weight);
  }

  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* notar = std::get_if<simplex::NotarizeVote>(&record.vote.vote);
        notar != nullptr && notar->id.slot == *attacked_slot) {
      return td::Status::Error(PSTRING() << "count-vs-weight slot " << *attacked_slot
                                         << " still formed a notar certificate");
    }
    if (auto* final = std::get_if<simplex::FinalizeVote>(&record.vote.vote);
        final != nullptr && final->id.slot == *attacked_slot) {
      return td::Status::Error(PSTRING() << "count-vs-weight slot " << *attacked_slot
                                         << " still formed a finalization certificate");
    }
  }

  bool saw_skip_cert = false;
  bool saw_later_finalization = false;
  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote);
        skip != nullptr && skip->slot == *attacked_slot) {
      saw_skip_cert = true;
    }
  }
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 0 && record.instance_idx == 0 && record.id.slot > *attacked_slot) {
      saw_later_finalization = true;
      break;
    }
  }
  if (!saw_skip_cert) {
    return td::Status::Error(PSTRING() << "count-vs-weight attacked slot " << *attacked_slot
                                       << " never cleared by Skip");
  }
  if (!saw_later_finalization) {
    return td::Status::Error(PSTRING() << "validator #0.0 never finalized after count-vs-weight slot "
                                       << *attacked_slot);
  }
  return td::Status::OK();
}

td::Status verify_duplicate_instance_same_key_split_brain_still_progresses(const TraceSnapshot& snapshot) {
  // Covers the adversarial plan's duplicate-instance same-key case:
  // two live instances under one validator identity may diverge temporarily, but honest validators
  // should still preserve safety and keep finalizing.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<double> disable_ts;
  std::optional<double> reenable_ts;
  for (const auto& record : snapshot.network_toggles) {
    if (record.node_idx == 0 && record.instance_idx == 1 && record.disabled) {
      disable_ts = record.ts;
    }
    if (record.node_idx == 0 && record.instance_idx == 1 && !record.disabled) {
      reenable_ts = record.ts;
      break;
    }
  }
  if (!disable_ts.has_value() || !reenable_ts.has_value() || *reenable_ts <= *disable_ts) {
    return td::Status::Error("scenario did not record a valid split-brain isolation interval for validator #0.1");
  }

  bool saw_primary_instance_vote = false;
  bool saw_isolated_instance_vote = false;
  for (const auto& record : snapshot.protocol_votes) {
    if (record.src_node_idx != 0 || record.ts < *disable_ts - 1e-9 || record.ts > *reenable_ts + 1e-9) {
      continue;
    }
    if (record.src_instance_idx == 0) {
      saw_primary_instance_vote = true;
    }
    if (record.src_instance_idx == 1) {
      saw_isolated_instance_vote = true;
    }
  }
  if (!saw_primary_instance_vote || !saw_isolated_instance_vote) {
    return td::Status::Error("scenario did not show both same-key instances acting during the split-brain interval");
  }

  bool saw_post_reenable_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 0) {
      continue;
    }
    if (record.ts > *reenable_ts + 1e-9) {
      saw_post_reenable_finalization = true;
      break;
    }
  }
  if (!saw_post_reenable_finalization) {
    return td::Status::Error("honest validators observed no finalization after the duplicate instance rejoined");
  }

  return td::Status::OK();
}

td::Status verify_sleeping_validator_recovers_after_honest_progress(const TraceSnapshot& snapshot) {
  // Covers C4 from the adversarial plan:
  // honest validators keep finalizing while one validator sleeps, and the sleeping validator later
  // catches back up after rejoining the network.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<double> disable_ts;
  std::optional<double> reenable_ts;
  for (const auto& record : snapshot.network_toggles) {
    if (record.node_idx == 3 && record.instance_idx == 0 && record.disabled) {
      disable_ts = record.ts;
    }
    if (record.node_idx == 3 && record.instance_idx == 0 && !record.disabled) {
      reenable_ts = record.ts;
      break;
    }
  }
  if (!disable_ts.has_value() || !reenable_ts.has_value() || *reenable_ts <= *disable_ts) {
    return td::Status::Error("scenario did not record a valid sleep interval for validator #3.0");
  }

  td::uint32 highest_honest_slot_while_sleeping = 0;
  bool saw_honest_progress_during_sleep = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 3) {
      continue;
    }
    if (record.ts > *disable_ts + 1e-9 && record.ts < *reenable_ts - 1e-9) {
      highest_honest_slot_while_sleeping = std::max(highest_honest_slot_while_sleeping, record.id.slot);
      saw_honest_progress_during_sleep = true;
    }
  }
  if (!saw_honest_progress_during_sleep) {
    return td::Status::Error("honest validators did not finalize any slot while validator #3.0 was sleeping");
  }

  bool saw_sleeping_validator_recovery = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx != 3 || record.instance_idx != 0) {
      continue;
    }
    if (record.ts > *reenable_ts + 1e-9 && record.id.slot >= highest_honest_slot_while_sleeping) {
      saw_sleeping_validator_recovery = true;
      break;
    }
  }
  if (!saw_sleeping_validator_recovery) {
    return td::Status::Error(PSTRING() << "sleeping validator #3.0 never caught up to finalized slot "
                                       << highest_honest_slot_while_sleeping << " after waking");
  }

  return td::Status::OK();
}

td::Status verify_standstill_with_byzantine_withholding_still_recovers(const TraceSnapshot& snapshot) {
  // Covers C5/C9/C11 from the adversarial plan:
  // one Byzantine validator withholds throughout, one honest validator disappears long enough to
  // break quorum, honest nodes rebroadcast during the stall, and finalization resumes after quorum returns.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<double> byzantine_disable_ts;
  std::optional<double> honest_disable_ts;
  std::optional<double> honest_reenable_ts;
  for (const auto& record : snapshot.network_toggles) {
    if (record.node_idx == 1 && record.instance_idx == 0 && record.disabled && !byzantine_disable_ts.has_value()) {
      byzantine_disable_ts = record.ts;
      continue;
    }
    if (record.node_idx == 2 && record.instance_idx == 0 && record.disabled && !honest_disable_ts.has_value()) {
      honest_disable_ts = record.ts;
      continue;
    }
    if (record.node_idx == 2 && record.instance_idx == 0 && !record.disabled && honest_disable_ts.has_value() &&
        record.ts > *honest_disable_ts + 1e-9) {
      honest_reenable_ts = record.ts;
      break;
    }
  }
  if (!byzantine_disable_ts.has_value() || !honest_disable_ts.has_value() || !honest_reenable_ts.has_value()) {
    return td::Status::Error("scenario did not record the expected standstill interval");
  }

  double standstill_rebroadcast_deadline = *honest_disable_ts + STANDSTILL_TIMEOUT_S * 0.75;
  std::set<std::string> honest_message_keys_before_deadline;
  std::set<std::string> honest_message_keys_during_stall;
  for (const auto& record : snapshot.protocol_votes) {
    if (record.src_node_idx == 1) {
      continue;
    }
    if (record.ts < standstill_rebroadcast_deadline) {
      honest_message_keys_before_deadline.insert(vote_trace_key(record.vote));
    }
    if (record.ts >= standstill_rebroadcast_deadline && record.ts <= *honest_reenable_ts + 1e-9) {
      honest_message_keys_during_stall.insert(vote_trace_key(record.vote));
    }
  }
  for (const auto& record : snapshot.protocol_certificates) {
    if (record.src_node_idx == 1) {
      continue;
    }
    if (record.ts < standstill_rebroadcast_deadline) {
      honest_message_keys_before_deadline.insert(certificate_trace_key(record));
    }
    if (record.ts >= standstill_rebroadcast_deadline && record.ts <= *honest_reenable_ts + 1e-9) {
      honest_message_keys_during_stall.insert(certificate_trace_key(record));
    }
  }

  bool saw_rebroadcast = false;
  for (const auto& key : honest_message_keys_during_stall) {
    if (honest_message_keys_before_deadline.contains(key)) {
      saw_rebroadcast = true;
      break;
    }
  }
  if (!saw_rebroadcast) {
    return td::Status::Error("honest validators did not rebroadcast any previously seen votes/certificates during the standstill");
  }

  bool saw_post_reenable_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 1) {
      continue;
    }
    if (record.ts > *honest_reenable_ts + 1e-9) {
      saw_post_reenable_finalization = true;
      break;
    }
  }
  if (!saw_post_reenable_finalization) {
    return td::Status::Error("honest validators observed no finalization after the honest validator returned");
  }

  return td::Status::OK();
}

td::Status verify_empty_candidates_with_silent_validator_still_finalize(const TraceSnapshot& snapshot) {
  // Covers D8 from the adversarial plan:
  // masterchain empty candidates should still finalize even when one validator is silent.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::set<CandidateId> empty_candidates;
  for (const auto& record : snapshot.candidates_generated) {
    if (record.is_empty) {
      empty_candidates.insert(record.candidate_id);
    }
  }
  if (empty_candidates.empty()) {
    return td::Status::Error("scenario did not produce any empty candidates");
  }

  std::set<CandidateId> finalized_empty_candidates;
  for (const auto& record : snapshot.finalizations_observed) {
    if (empty_candidates.contains(record.id)) {
      finalized_empty_candidates.insert(record.id);
    }
  }
  if (finalized_empty_candidates.empty()) {
    return td::Status::Error("no empty candidate finalized while validator #3 was silent");
  }

  td::uint32 highest_empty_slot = 0;
  for (const auto& id : finalized_empty_candidates) {
    highest_empty_slot = std::max(highest_empty_slot, id.slot);
  }
  bool saw_later_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 3) {
      continue;
    }
    if (record.id.slot > highest_empty_slot) {
      saw_later_finalization = true;
      break;
    }
  }
  if (!saw_later_finalization) {
    return td::Status::Error("no later slot finalized after the finalized empty candidate");
  }

  return td::Status::OK();
}

td::Status verify_no_notar_observed_skip_wins(const TraceSnapshot& snapshot) {
  // Covers C6 from the adversarial plan:
  // if honest validators never observe notarization for a slot, that slot should eventually clear by Skip
  // and later progress should continue.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> attacked_slot;
  std::vector<size_t> byzantine_nodes;
  {
    std::scoped_lock lock(test_expectations_mutex);
    attacked_slot = test_expectations.no_notarization_slot;
    byzantine_nodes = test_expectations.adversarial_nodes;
  }
  if (!attacked_slot.has_value() || byzantine_nodes.size() != 1) {
    return td::Status::Error("scenario did not register the no-notarization target slot");
  }
  size_t leader_node_idx = byzantine_nodes.front();

  for (const auto& record : snapshot.notarizations_observed) {
    if (record.node_idx == leader_node_idx) {
      continue;
    }
    if (record.id.slot == *attacked_slot) {
      return td::Status::Error(PSTRING() << "honest validator #" << record.node_idx
                                         << " still observed notarization for slot " << *attacked_slot);
    }
  }
  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* notar = std::get_if<simplex::NotarizeVote>(&record.vote.vote);
        notar != nullptr && notar->id.slot == *attacked_slot) {
      return td::Status::Error(PSTRING() << "slot " << *attacked_slot
                                         << " still reached a notarization certificate");
    }
  }

  bool saw_skip_cert = false;
  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote); skip != nullptr && skip->slot == *attacked_slot) {
      saw_skip_cert = true;
      break;
    }
  }
  if (!saw_skip_cert) {
    return td::Status::Error(PSTRING() << "slot " << *attacked_slot
                                       << " did not reach a Skip certificate without notarization");
  }

  bool saw_later_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == leader_node_idx) {
      continue;
    }
    if (record.id.slot > *attacked_slot) {
      saw_later_finalization = true;
      break;
    }
  }
  if (!saw_later_finalization) {
    return td::Status::Error(PSTRING() << "honest validators did not finalize any later slot after skipped slot "
                                       << *attacked_slot);
  }

  return td::Status::OK();
}

td::Status verify_later_slot_waits_for_earlier_notar_proof(const TraceSnapshot& snapshot) {
  // Covers A8, D2, and D3 from the adversarial plan:
  // with eager whole-window production, a receiver may see later-slot candidates early, but it must
  // not notarize them before it has proof that the earlier parent slot is notarized.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> parent_slot;
  {
    std::scoped_lock lock(test_expectations_mutex);
    parent_slot = test_expectations.parent_gating_slot;
  }
  if (!parent_slot.has_value()) {
    return td::Status::Error("scenario did not register the parent-gating slot");
  }
  td::uint32 window_end_slot = *parent_slot + SLOTS_PER_LEADER_WINDOW;
  std::optional<double> earliest_later_candidate_delivery_ts;
  std::optional<td::uint32> earliest_later_candidate_slot;
  for (const auto& record : snapshot.candidate_deliveries) {
    if (record.dst_node_idx == 0 && record.dst_instance_idx == 0 && record.candidate_id.slot > *parent_slot &&
        record.candidate_id.slot < window_end_slot &&
        (!earliest_later_candidate_delivery_ts.has_value() || record.ts < *earliest_later_candidate_delivery_ts)) {
      earliest_later_candidate_delivery_ts = record.ts;
      earliest_later_candidate_slot = record.candidate_id.slot;
    }
  }
  if (!earliest_later_candidate_delivery_ts.has_value() || !earliest_later_candidate_slot.has_value()) {
    return td::Status::Error(PSTRING() << "validator #0.0 never received any later-slot candidate in window ["
                                       << *parent_slot << ", " << window_end_slot << ")");
  }

  std::optional<double> parent_notar_observed_ts;
  for (const auto& record : snapshot.notarizations_observed) {
    if (record.node_idx == 0 && record.instance_idx == 0 && record.id.slot == *parent_slot) {
      parent_notar_observed_ts = std::min(parent_notar_observed_ts.value_or(record.ts), record.ts);
    }
  }
  if (!parent_notar_observed_ts.has_value()) {
    return td::Status::Error(PSTRING() << "validator #0.0 never observed notarization for parent slot " << *parent_slot);
  }
  if (*earliest_later_candidate_delivery_ts >= *parent_notar_observed_ts - 1e-9) {
    return td::Status::Error(PSTRING() << "later-slot candidate " << *earliest_later_candidate_slot
                                       << " did not arrive before parent notarization for slot " << *parent_slot);
  }

  for (const auto& record : snapshot.protocol_votes) {
    if (record.src_node_idx != 0 || record.src_instance_idx != 0) {
      continue;
    }
    auto* notar = std::get_if<simplex::NotarizeVote>(&record.vote.vote);
    if (notar == nullptr || notar->id.slot <= *parent_slot || notar->id.slot >= window_end_slot) {
      continue;
    }
    if (record.ts < *parent_notar_observed_ts - 1e-9) {
      return td::Status::Error(PSTRING() << "validator #0.0 voted Notar for later slot " << notar->id.slot
                                         << " before observing parent notarization for slot " << *parent_slot);
    }
  }

  bool saw_later_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 0 && record.instance_idx == 0 && record.id.slot > *parent_slot) {
      saw_later_finalization = true;
      break;
    }
  }
  if (!saw_later_finalization) {
    return td::Status::Error(PSTRING() << "validator #0.0 never finalized any slot later than " << *parent_slot);
  }

  return td::Status::OK();
}

td::Status verify_replay_stale_certificate_does_not_disturb_progress(const TraceSnapshot& snapshot) {
  // Covers C13 from the adversarial plan:
  // replaying an old certificate after later finalization must not restart voting on the stale slot,
  // and honest progress must continue on newer slots.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> stale_slot;
  {
    std::scoped_lock lock(test_expectations_mutex);
    stale_slot = test_expectations.replayed_stale_slot;
  }
  if (!stale_slot.has_value()) {
    return td::Status::Error("scenario did not register the replayed stale slot");
  }

  for (const auto& record : snapshot.protocol_votes) {
    if (record.src_node_idx == 1) {
      continue;
    }
    auto slot = vote_trace_slot(record.vote);
    if (slot == *stale_slot) {
      return td::Status::Error(PSTRING() << "honest validator #" << record.src_node_idx
                                         << " resumed voting on stale slot " << *stale_slot
                                         << " after replay");
    }
  }

  for (const auto& record : snapshot.protocol_certificates) {
    if (record.src_node_idx == 1) {
      continue;
    }
    auto slot = vote_trace_slot(record.vote);
    if (slot == *stale_slot) {
      return td::Status::Error(PSTRING() << "honest validator #" << record.src_node_idx
                                         << " rebroadcast stale slot " << *stale_slot
                                         << " after Byzantine replay");
    }
  }

  bool saw_later_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 1) {
      continue;
    }
    if (record.id.slot > *stale_slot) {
      saw_later_finalization = true;
      break;
    }
  }
  if (!saw_later_finalization) {
    return td::Status::Error(PSTRING() << "honest validators did not finalize any slot after stale replay of slot "
                                       << *stale_slot);
  }

  return td::Status::OK();
}

td::Status verify_notarized_and_skipped_slot_coexist_without_finalization(const TraceSnapshot& snapshot) {
  // Covers B1 from the adversarial plan:
  // a slot may become both notarized and skipped, but it must still avoid finalization and later
  // honest output must remain prefix-consistent.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> forked_slot;
  {
    std::scoped_lock lock(test_expectations_mutex);
    forked_slot = test_expectations.forked_slot;
  }
  if (!forked_slot.has_value()) {
    return td::Status::Error("scenario did not register the forked slot");
  }

  bool saw_notar_cert = false;
  bool saw_skip_cert = false;
  for (const auto& record : snapshot.notarizations_observed) {
    if (record.id.slot == *forked_slot) {
      saw_notar_cert = true;
      break;
    }
  }
  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote); skip != nullptr && skip->slot == *forked_slot) {
      saw_skip_cert = true;
    }
    if (auto* final = std::get_if<simplex::FinalizeVote>(&record.vote.vote);
        final != nullptr && final->id.slot == *forked_slot) {
      return td::Status::Error(PSTRING() << "forked slot " << *forked_slot
                                         << " still reached a finalization certificate");
    }
  }
  if (!saw_notar_cert) {
    return td::Status::Error(PSTRING() << "forked slot " << *forked_slot
                                       << " never reached a notarization certificate");
  }
  if (!saw_skip_cert) {
    return td::Status::Error(PSTRING() << "forked slot " << *forked_slot
                                       << " never reached a skip certificate");
  }

  bool saw_later_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.id.slot > *forked_slot) {
      saw_later_finalization = true;
      break;
    }
  }
  if (!saw_later_finalization) {
    return td::Status::Error(PSTRING() << "honest validators did not finalize any later slot after forked slot "
                                       << *forked_slot);
  }

  return td::Status::OK();
}

td::Status verify_final_and_skip_race_preserves_local_discipline(const TraceSnapshot& snapshot) {
  // Covers A3 and A4 from the adversarial plan:
  // when one honest view is ready to Final while another reaches Skip on the same slot, the slot
  // must still avoid finalizing, and no honest validator may cast both votes.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> forked_slot;
  {
    std::scoped_lock lock(test_expectations_mutex);
    forked_slot = test_expectations.forked_slot;
  }
  if (!forked_slot.has_value()) {
    return td::Status::Error("scenario did not register the raced slot");
  }

  bool saw_honest_final_vote = false;
  bool saw_honest_skip_vote = false;
  for (const auto& record : snapshot.protocol_votes) {
    if (adversarial_nodes().contains(record.src_node_idx)) {
      continue;
    }
    if (auto* final = std::get_if<simplex::FinalizeVote>(&record.vote.vote);
        final != nullptr && final->id.slot == *forked_slot) {
      saw_honest_final_vote = true;
    }
    if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote);
        skip != nullptr && skip->slot == *forked_slot) {
      saw_honest_skip_vote = true;
    }
  }
  if (!saw_honest_final_vote) {
    return td::Status::Error(PSTRING() << "no honest validator cast Final for raced slot " << *forked_slot);
  }
  if (!saw_honest_skip_vote) {
    return td::Status::Error(PSTRING() << "no honest validator cast Skip for raced slot " << *forked_slot);
  }

  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* final = std::get_if<simplex::FinalizeVote>(&record.vote.vote);
        final != nullptr && final->id.slot == *forked_slot) {
      return td::Status::Error(PSTRING() << "raced slot " << *forked_slot
                                         << " still reached a finalization certificate");
    }
  }
  return td::Status::OK();
}

td::Status verify_equivocating_leader_does_not_create_two_notars_or_finals(const TraceSnapshot& snapshot) {
  // Covers A1 and A2 from the adversarial plan:
  // two live instances under the Byzantine leader's key push different candidates for one slot and
  // try to drive separate branches, but honest validators must not create two notar or final certificates.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> attacked_slot;
  std::vector<CandidateId> candidates;
  {
    std::scoped_lock lock(test_expectations_mutex);
    attacked_slot = test_expectations.equivocation_slot;
    candidates = test_expectations.equivocation_candidates;
  }
  if (!attacked_slot.has_value() || candidates.size() != 2 || candidates[0] == candidates[1]) {
    return td::Status::Error("scenario did not register two distinct equivocation candidates");
  }

  std::set<CandidateId> delivered_candidates;
  for (const auto& record : snapshot.candidate_deliveries) {
    if (record.candidate_id.slot == *attacked_slot) {
      delivered_candidates.insert(record.candidate_id);
    }
  }
  for (const auto& id : candidates) {
    if (!delivered_candidates.contains(id)) {
      return td::Status::Error(PSTRING() << "equivocation candidate " << id << " was never delivered");
    }
  }

  std::map<size_t, std::set<CandidateId>> notarized_by_instance;
  std::map<size_t, std::set<CandidateId>> finalized_by_instance;
  for (const auto& record : snapshot.protocol_votes) {
    if (record.src_node_idx != 0) {
      continue;
    }
    if (auto* notar = std::get_if<simplex::NotarizeVote>(&record.vote.vote); notar != nullptr && notar->id.slot == *attacked_slot) {
      notarized_by_instance[record.src_instance_idx].insert(notar->id);
    }
    if (auto* final = std::get_if<simplex::FinalizeVote>(&record.vote.vote); final != nullptr && final->id.slot == *attacked_slot) {
      finalized_by_instance[record.src_instance_idx].insert(final->id);
    }
  }
  if (!notarized_by_instance.contains(0) || !notarized_by_instance.contains(1) ||
      notarized_by_instance[0].empty() || notarized_by_instance[1].empty()) {
    return td::Status::Error(PSTRING() << "split-brain leader did not show both instances notarizing attacked slot "
                                       << *attacked_slot);
  }
  std::set<CandidateId> union_notars = notarized_by_instance[0];
  union_notars.insert(notarized_by_instance[1].begin(), notarized_by_instance[1].end());
  if (union_notars.size() < 2) {
    return td::Status::Error(PSTRING() << "split-brain leader never attempted two branches on attacked slot "
                                       << *attacked_slot);
  }
  std::set<CandidateId> union_finals = finalized_by_instance[0];
  union_finals.insert(finalized_by_instance[1].begin(), finalized_by_instance[1].end());
  if (union_finals.size() < 2) {
    return td::Status::Error(PSTRING() << "split-brain leader never attempted Final votes for both branches on slot "
                                       << *attacked_slot);
  }

  bool saw_later_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.id.slot > *attacked_slot) {
      saw_later_finalization = true;
      break;
    }
  }
  if (!saw_later_finalization) {
    return td::Status::Error(PSTRING() << "honest validators did not finalize any later slot after equivocation on "
                                       << *attacked_slot);
  }

  return td::Status::OK();
}

td::Status verify_delayed_finalization_observation_preserves_prefix(const TraceSnapshot& snapshot) {
  // Covers B4 from the adversarial plan:
  // one honest node observes later finalizations after another due to delayed certificate delivery, but
  // their finalized suffixes remain prefix-consistent and never rewind.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> from_slot;
  std::optional<size_t> delayed_node_idx;
  std::optional<double> release_ts;
  {
    std::scoped_lock lock(test_expectations_mutex);
    from_slot = test_expectations.delayed_finalization_from_slot;
    delayed_node_idx = test_expectations.delayed_finalization_node_idx;
    release_ts = test_expectations.delayed_finalization_release_ts;
  }
  if (!from_slot.has_value() || !delayed_node_idx.has_value() || !release_ts.has_value()) {
    return td::Status::Error("scenario did not register delayed-finalization expectations");
  }

  td::uint32 reference_max_before_release = 0;
  td::uint32 delayed_max_before_release = 0;
  td::uint32 delayed_max_after_release = 0;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.id.slot < *from_slot) {
      continue;
    }
    if (record.node_idx == 2 && record.ts < *release_ts - 1e-9) {
      reference_max_before_release = std::max(reference_max_before_release, record.id.slot);
    }
    if (record.node_idx == *delayed_node_idx && record.ts < *release_ts - 1e-9) {
      delayed_max_before_release = std::max(delayed_max_before_release, record.id.slot);
    }
    if (record.node_idx == *delayed_node_idx && record.ts > *release_ts + 1e-9) {
      delayed_max_after_release = std::max(delayed_max_after_release, record.id.slot);
    }
  }
  if (reference_max_before_release < *from_slot) {
    return td::Status::Error(PSTRING() << "reference node did not observe any finalization from slot " << *from_slot
                                       << " while delivery to node #" << *delayed_node_idx << " was delayed");
  }
  if (reference_max_before_release <= delayed_max_before_release) {
    return td::Status::Error(PSTRING() << "delayed node #" << *delayed_node_idx
                                       << " was never behind the reference node before release");
  }
  if (delayed_max_after_release < reference_max_before_release) {
    return td::Status::Error(PSTRING() << "delayed node #" << *delayed_node_idx
                                       << " never caught up to the reference tip after certificate release");
  }

  return td::Status::OK();
}

td::Status verify_fork_descendants_collapse_after_later_finalization(const TraceSnapshot& snapshot) {
  // Covers B3 from the adversarial plan:
  // after one slot is both notarized and skipped, a later honest finalization must collapse the fork
  // without any conflicting final certificate.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> forked_slot;
  {
    std::scoped_lock lock(test_expectations_mutex);
    forked_slot = test_expectations.forked_slot;
  }
  if (!forked_slot.has_value()) {
    return td::Status::Error("scenario did not register the forked slot");
  }

  std::optional<CandidateId> forked_candidate_id;
  bool saw_skip_cert = false;
  for (const auto& record : snapshot.notarizations_observed) {
    if (record.id.slot == *forked_slot) {
      forked_candidate_id = record.id;
      break;
    }
  }
  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote); skip != nullptr && skip->slot == *forked_slot) {
      saw_skip_cert = true;
    }
  }
  if (!forked_candidate_id.has_value() || !saw_skip_cert) {
    return td::Status::Error(PSTRING() << "forked slot " << *forked_slot
                                       << " did not produce both the notarized and skipped sides");
  }

  std::map<size_t, CandidateId> highest_honest_finalization;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 0 || record.id.slot <= *forked_slot) {
      continue;
    }
    auto& best = highest_honest_finalization[record.node_idx];
    if (best.slot < record.id.slot) {
      best = record.id;
    }
  }
  if (highest_honest_finalization.size() < 3) {
    return td::Status::Error(PSTRING() << "not enough honest validators finalized past forked slot " << *forked_slot);
  }
  auto it = highest_honest_finalization.begin();
  const CandidateId& expected = it->second;
  ++it;
  for (; it != highest_honest_finalization.end(); ++it) {
    if (it->second != expected) {
      return td::Status::Error(PSTRING() << "honest validators did not collapse to one later finalized tip after slot "
                                         << *forked_slot << ": first=" << expected << ", later=" << it->second);
    }
  }
  return td::Status::OK();
}

td::Status verify_missing_ancestor_chain_still_resolves(const TraceSnapshot& snapshot) {
  // Covers C8 from the adversarial plan:
  // when a node only learns about a head candidate first, it must recursively request missing ancestors
  // instead of treating the head as resolved immediately.
  TRY_STATUS(verify_bad_candidate_resolution_responder_does_not_stop_catchup(snapshot));

  std::optional<CandidateId> expected_target;
  {
    std::scoped_lock lock(test_expectations_mutex);
    expected_target = test_expectations.candidate_resolution_target;
  }
  if (!expected_target.has_value()) {
    return td::Status::Error("scenario did not register the recursive-resolution target candidate id");
  }

  std::set<CandidateId> requested_ids;
  for (const auto& record : snapshot.overlay_requests) {
    if (record.src_node_idx == 0 && record.src_instance_idx == 0 && record.candidate_id.has_value()) {
      requested_ids.insert(*record.candidate_id);
    }
  }
  if (!requested_ids.contains(*expected_target)) {
    return td::Status::Error(PSTRING() << "recursive-resolution scenario never requested the head candidate "
                                       << *expected_target);
  }
  if (requested_ids.size() < 2) {
    return td::Status::Error(PSTRING() << "recursive-resolution scenario never requested any ancestor while resolving "
                                       << *expected_target);
  }
  return td::Status::OK();
}

td::Status verify_later_slot_waits_for_gap_skip_proof(const TraceSnapshot& snapshot) {
  // Covers A7 from the adversarial plan:
  // a later-slot candidate must not be notarized until the intermediate skipped slot is proven cleared.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> gap_slot;
  {
    std::scoped_lock lock(test_expectations_mutex);
    gap_slot = test_expectations.gap_skip_slot;
  }
  if (!gap_slot.has_value()) {
    return td::Status::Error("scenario did not register the gap slot");
  }
  td::uint32 attacked_slot = *gap_slot + 1;

  bool saw_later_candidate_delivery = false;
  for (const auto& record : snapshot.candidate_deliveries) {
    if (record.dst_node_idx == 0 && record.dst_instance_idx == 0 && record.candidate_id.slot == attacked_slot) {
      saw_later_candidate_delivery = true;
      break;
    }
  }
  if (!saw_later_candidate_delivery) {
    return td::Status::Error(PSTRING() << "scenario never delivered later-slot candidate " << attacked_slot
                                       << " to node #0");
  }

  std::optional<double> first_skip_cert_ts;
  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote);
        skip != nullptr && skip->slot == *gap_slot &&
        (!record.dst_node_idx.has_value() || *record.dst_node_idx == 0)) {
      first_skip_cert_ts = std::min(first_skip_cert_ts.value_or(record.ts), record.ts);
    }
  }
  if (!first_skip_cert_ts.has_value()) {
    return td::Status::Error(PSTRING() << "node #0 never received a skip certificate for gap slot " << *gap_slot);
  }

  std::optional<double> first_notar_ts;
  for (const auto& record : snapshot.protocol_votes) {
    if (record.src_node_idx == 0 && record.src_instance_idx == 0) {
      if (auto* notar = std::get_if<simplex::NotarizeVote>(&record.vote.vote);
          notar != nullptr && notar->id.slot == attacked_slot) {
        first_notar_ts = record.ts;
        break;
      }
    }
  }
  if (!first_notar_ts.has_value()) {
    return td::Status::Error(PSTRING() << "node #0 never notarized later slot " << attacked_slot
                                       << " after the gap was cleared");
  }
  if (*first_notar_ts <= *first_skip_cert_ts + 1e-9) {
    return td::Status::Error(PSTRING() << "node #0 notarized later slot " << attacked_slot
                                       << " before observing the gap skip certificate for slot " << *gap_slot);
  }
  return td::Status::OK();
}

td::Status verify_invalid_candidate_signature_does_not_resolve(const TraceSnapshot& snapshot) {
  // Covers A5 from the adversarial plan:
  // a Byzantine peer may answer Rule 2 with a candidate body whose signature no longer matches the
  // requested candidate id, but honest recovery must ignore it and later resolve from an honest peer.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<CandidateId> expected_target;
  {
    std::scoped_lock lock(test_expectations_mutex);
    expected_target = test_expectations.candidate_resolution_target;
  }
  if (!expected_target.has_value()) {
    return td::Status::Error("scenario did not register a candidate-resolution target");
  }

  bool saw_request_for_candidate = false;
  for (const auto& record : snapshot.overlay_requests) {
    if (record.src_node_idx == 0 && record.src_instance_idx == 0 && record.dst_node_idx == 1 &&
        record.candidate_id == expected_target && record.want_candidate) {
      saw_request_for_candidate = true;
      break;
    }
  }
  if (!saw_request_for_candidate) {
    return td::Status::Error("scenario never requested the candidate body from the malicious responder");
  }

  std::optional<double> malformed_response_ts;
  for (const auto& record : snapshot.malformed_candidate_responses) {
    if (record.requester_node_idx == 0 && record.requester_instance_idx == 0 && record.responder_node_idx == 1 &&
        record.id == *expected_target) {
      malformed_response_ts = std::max(malformed_response_ts.value_or(0.0), record.ts);
    }
  }
  if (!malformed_response_ts.has_value()) {
    return td::Status::Error("scenario did not inject any corrupted candidate response from the malicious peer");
  }

  size_t requests_to_honest_peer = 0;
  for (const auto& record : snapshot.overlay_requests) {
    if (record.src_node_idx == 0 && record.src_instance_idx == 0 && record.dst_node_idx == 3 &&
        record.candidate_id == expected_target) {
      ++requests_to_honest_peer;
    }
  }

  std::optional<double> resolved_ts;
  for (const auto& record : snapshot.candidates_resolved) {
    if (record.node_idx == 0 && record.instance_idx == 0 && record.id == *expected_target) {
      resolved_ts = record.ts;
      break;
    }
  }
  if (requests_to_honest_peer == 0) {
    if (resolved_ts.has_value() && *resolved_ts > *malformed_response_ts + 1e-9) {
      return td::Status::Error(PSTRING() << "validator #0.0 resolved target " << *expected_target
                                         << " after only corrupted candidate bytes; no honest response was requested");
    }
    return td::Status::Error(PSTRING() << "validator #0.0 neither retried an honest peer nor resolved target "
                                       << *expected_target << " after corrupted candidate bytes");
  }
  if (!resolved_ts.has_value()) {
    return td::Status::Error(PSTRING() << "validator #0.0 never resolved target " << *expected_target
                                       << " after retrying the honest responder");
  }
  if (*resolved_ts <= *malformed_response_ts + 1e-9) {
    return td::Status::Error("candidate resolution completed before the corrupted candidate response was injected");
  }
  return td::Status::OK();
}

td::Status verify_malformed_notar_certificate_does_not_resolve(const TraceSnapshot& snapshot) {
  // Covers A9 from the adversarial plan:
  // a Byzantine peer may answer Rule 2 with a candidate plus an invalid notar certificate,
  // but honest recovery must reject that cert and later resolve from an honest peer.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<CandidateId> expected_target;
  {
    std::scoped_lock lock(test_expectations_mutex);
    expected_target = test_expectations.candidate_resolution_target;
  }
  if (!expected_target.has_value()) {
    return td::Status::Error("scenario did not register a candidate-resolution target");
  }

  bool saw_request_for_notar = false;
  for (const auto& record : snapshot.overlay_requests) {
    if (record.src_node_idx == 0 && record.src_instance_idx == 0 && record.dst_node_idx == 1 &&
        record.candidate_id == expected_target && record.want_notar) {
      saw_request_for_notar = true;
      break;
    }
  }
  if (!saw_request_for_notar) {
    return td::Status::Error("scenario never requested the notar certificate from the malicious responder");
  }

  std::optional<double> malformed_response_ts;
  for (const auto& record : snapshot.malformed_candidate_responses) {
    if (record.requester_node_idx == 0 && record.requester_instance_idx == 0 && record.responder_node_idx == 1 &&
        record.id == *expected_target) {
      malformed_response_ts = std::max(malformed_response_ts.value_or(0.0), record.ts);
    }
  }
  if (!malformed_response_ts.has_value()) {
    return td::Status::Error("scenario did not inject any corrupted notar response from the malicious peer");
  }

  size_t requests_to_honest_peer = 0;
  for (const auto& record : snapshot.overlay_requests) {
    if (record.src_node_idx == 0 && record.src_instance_idx == 0 && record.dst_node_idx == 3 &&
        record.candidate_id == expected_target) {
      ++requests_to_honest_peer;
    }
  }
  if (requests_to_honest_peer == 0) {
    return td::Status::Error(PSTRING() << "validator #0.0 never retried target " << *expected_target
                                       << " against an honest responder after corrupted notar bytes");
  }

  std::optional<double> resolved_ts;
  for (const auto& record : snapshot.candidates_resolved) {
    if (record.node_idx == 0 && record.instance_idx == 0 && record.id == *expected_target) {
      resolved_ts = record.ts;
      break;
    }
  }
  if (!resolved_ts.has_value()) {
    return td::Status::Error(PSTRING() << "validator #0.0 never resolved target " << *expected_target
                                       << " after retrying the honest responder");
  }
  if (*resolved_ts <= *malformed_response_ts + 1e-9) {
    return td::Status::Error("candidate resolution completed before the corrupted notar response was injected");
  }
  return td::Status::OK();
}

td::Status verify_long_stall_clears_skipped_slots_then_honest_leader_finalizes(const TraceSnapshot& snapshot) {
  // Covers C10 and C11 from the adversarial plan:
  // a long run of slots with no delivered candidates must still clear slot-by-slot by Skip, and a
  // later honest leader must finalize again once the stall ends.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> start_slot;
  size_t slot_count = 0;
  {
    std::scoped_lock lock(test_expectations_mutex);
    start_slot = test_expectations.long_stall_start_slot;
    slot_count = test_expectations.long_stall_slot_count;
  }
  if (!start_slot.has_value() || slot_count < 3) {
    return td::Status::Error("scenario did not register the long-stall slot range");
  }

  for (td::uint32 slot = *start_slot; slot < *start_slot + slot_count; ++slot) {
    bool saw_skip_cert = false;
    for (const auto& record : snapshot.protocol_certificates) {
      if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote); skip != nullptr && skip->slot == slot) {
        saw_skip_cert = true;
        break;
      }
    }
    if (!saw_skip_cert) {
      return td::Status::Error(PSTRING() << "stalled slot " << slot << " never reached a Skip certificate");
    }
  }

  td::uint32 stall_end_slot = *start_slot + static_cast<td::uint32>(slot_count) - 1;
  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* final = std::get_if<simplex::FinalizeVote>(&record.vote.vote);
        final != nullptr && final->id.slot >= *start_slot && final->id.slot <= stall_end_slot) {
      return td::Status::Error(PSTRING() << "stalled slot " << final->id.slot
                                         << " unexpectedly reached a finalization certificate");
    }
  }

  bool saw_later_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.id.slot > stall_end_slot) {
      saw_later_finalization = true;
      break;
    }
  }
  if (!saw_later_finalization) {
    return td::Status::Error(PSTRING() << "honest validators never finalized past the stalled range ending at slot "
                                       << stall_end_slot);
  }
  return td::Status::OK();
}

td::Status verify_multi_slot_equivocation_window_preserves_safety(const TraceSnapshot& snapshot) {
  // Covers B2 and D1 from the adversarial plan:
  // two same-key leader instances may generate different descendants across a whole leader window,
  // but honest vote/certificate safety must still hold slot-by-slot and later honest progress must continue.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> start_slot;
  size_t slot_count = 0;
  {
    std::scoped_lock lock(test_expectations_mutex);
    start_slot = test_expectations.multi_slot_equivocation_start_slot;
    slot_count = test_expectations.multi_slot_equivocation_slot_count;
  }
  if (!start_slot.has_value() || slot_count < 2) {
    return td::Status::Error("scenario did not register the multi-slot equivocation window");
  }

  size_t divergent_slots = 0;
  for (td::uint32 slot = *start_slot; slot < *start_slot + slot_count; ++slot) {
    std::map<size_t, CandidateId> candidates_by_instance;
    for (const auto& record : snapshot.candidates_generated) {
      if (record.node_idx == 0 && record.candidate_id.slot == slot) {
        candidates_by_instance.emplace(record.instance_idx, record.candidate_id);
      }
    }
    if (candidates_by_instance.size() == 2 && candidates_by_instance.at(0) != candidates_by_instance.at(1)) {
      ++divergent_slots;
    }
  }
  if (divergent_slots < 2) {
    return td::Status::Error(PSTRING() << "same-key leader generated divergent descendants on only " << divergent_slots
                                       << " slots inside the attacked window starting at " << *start_slot);
  }

  bool saw_later_honest_finalization = false;
  td::uint32 window_end_slot = *start_slot + static_cast<td::uint32>(slot_count) - 1;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx != 0 && record.id.slot > window_end_slot) {
      saw_later_honest_finalization = true;
      break;
    }
  }
  if (!saw_later_honest_finalization) {
    return td::Status::Error(PSTRING() << "honest validators never finalized past multi-slot equivocation window ending at slot "
                                       << window_end_slot);
  }

  return td::Status::OK();
}

td::Status verify_invalid_full_candidate_does_not_notarize(const TraceSnapshot& snapshot) {
  // Covers A6 from the adversarial plan:
  // a Byzantine leader may deliver a signed candidate whose chain state fails validation, but honest
  // validators must reject it and never cast Notar votes for that candidate.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> attacked_slot;
  std::vector<size_t> byzantine_nodes;
  {
    std::scoped_lock lock(test_expectations_mutex);
    attacked_slot = test_expectations.invalid_full_candidate_slot;
    byzantine_nodes = test_expectations.adversarial_nodes;
  }
  if (!attacked_slot.has_value() || byzantine_nodes.empty()) {
    return td::Status::Error("scenario did not register the invalid full-candidate attack");
  }

  std::set<CandidateId> attacked_candidate_ids;
  for (const auto& record : snapshot.candidate_deliveries) {
    if (record.candidate_id.slot == *attacked_slot) {
      attacked_candidate_ids.insert(record.candidate_id);
    }
  }
  for (const auto& record : snapshot.candidates_generated) {
    if (record.candidate_id.slot == *attacked_slot &&
        std::find(byzantine_nodes.begin(), byzantine_nodes.end(), record.node_idx) != byzantine_nodes.end()) {
      attacked_candidate_ids.insert(record.candidate_id);
    }
  }
  if (attacked_candidate_ids.empty()) {
    return td::Status::Error(PSTRING() << "scenario never observed any candidate for invalid slot " << *attacked_slot);
  }

  for (const auto& record : snapshot.protocol_votes) {
    if (std::find(byzantine_nodes.begin(), byzantine_nodes.end(), record.src_node_idx) != byzantine_nodes.end()) {
      continue;
    }
    if (auto* notar = std::get_if<simplex::NotarizeVote>(&record.vote.vote);
        notar != nullptr && attacked_candidate_ids.contains(notar->id)) {
      return td::Status::Error(PSTRING() << "honest validator #" << record.src_node_idx
                                         << " notarized invalid full-candidate slot " << *attacked_slot);
    }
  }

  bool saw_skip_cert = false;
  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote); skip != nullptr && skip->slot == *attacked_slot) {
      saw_skip_cert = true;
      break;
    }
  }
  if (!saw_skip_cert) {
    return td::Status::Error(PSTRING() << "invalid full-candidate slot " << *attacked_slot
                                       << " never cleared by Skip");
  }
  return td::Status::OK();
}

td::Status verify_standstill_flood_still_recovers(const TraceSnapshot& snapshot) {
  // Covers D7 from the adversarial plan:
  // prolonged standstill rebroadcast pressure under a silent Byzantine validator must not break safety,
  // and honest finalization must resume once honest quorum connectivity is restored.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<double> first_reenable_ts;
  for (const auto& record : snapshot.network_toggles) {
    if ((record.node_idx == 2 || record.node_idx == 3) && !record.disabled) {
      first_reenable_ts = std::min(first_reenable_ts.value_or(record.ts), record.ts);
    }
  }
  if (!first_reenable_ts.has_value()) {
    return td::Status::Error("scenario never restored the honest quorum after the standstill flood");
  }

  size_t post_reenable_messages = 0;
  for (const auto& record : snapshot.protocol_votes) {
    if (record.ts > *first_reenable_ts + 1e-9) {
      ++post_reenable_messages;
    }
  }
  for (const auto& record : snapshot.protocol_certificates) {
    if (record.ts > *first_reenable_ts + 1e-9) {
      ++post_reenable_messages;
    }
  }
  if (post_reenable_messages < 8) {
    return td::Status::Error(PSTRING() << "standstill flood scenario only observed " << post_reenable_messages
                                       << " post-reconnect protocol messages");
  }

  bool saw_later_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.ts > *first_reenable_ts + 1e-9 && record.node_idx != 1) {
      saw_later_finalization = true;
      break;
    }
  }
  if (!saw_later_finalization) {
    return td::Status::Error("honest validators never finalized again after the standstill flood reconnect");
  }
  return td::Status::OK();
}

td::Status verify_several_windows_fully_skipped_then_progress_resumes(const TraceSnapshot& snapshot) {
  // Covers Kernel-28's missing "several fully skipped windows" regression:
  // under < 1/3 silent leaders, several consecutive windows should clear by Skip and later honest
  // windows should still finalize.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  size_t skip_count = 0;
  std::vector<size_t> byzantine_nodes;
  {
    std::scoped_lock lock(test_expectations_mutex);
    skip_count = test_expectations.consecutive_skip_count;
    byzantine_nodes = test_expectations.adversarial_nodes;
  }
  if (skip_count < 3 || byzantine_nodes.size() < 3) {
    return td::Status::Error("scenario did not register the expected fully skipped-window expectations");
  }

  std::set<td::uint32> skipped_slots;
  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote); skip != nullptr) {
      skipped_slots.insert(skip->slot);
    }
  }
  if (skipped_slots.empty()) {
    return td::Status::Error("scenario did not produce any Skip certificates");
  }

  td::uint32 first_skipped_slot = 0;
  td::uint32 last_skipped_slot = 0;
  bool found_consecutive_run = false;
  td::uint32 current_run_start = 0;
  td::uint32 previous_slot = 0;
  bool have_previous = false;
  for (td::uint32 slot : skipped_slots) {
    if (!have_previous || slot != previous_slot + 1) {
      current_run_start = slot;
    }
    if (slot - current_run_start + 1 >= skip_count) {
      found_consecutive_run = true;
      first_skipped_slot = current_run_start;
      last_skipped_slot = slot;
      break;
    }
    previous_slot = slot;
    have_previous = true;
  }
  if (!found_consecutive_run) {
    return td::Status::Error(PSTRING() << "scenario did not produce a consecutive run of " << skip_count
                                       << " skipped slots; observed skip slots=" << skipped_slots);
  }

  bool saw_later_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (std::find(byzantine_nodes.begin(), byzantine_nodes.end(), record.node_idx) != byzantine_nodes.end()) {
      continue;
    }
    if (record.id.slot > last_skipped_slot) {
      saw_later_finalization = true;
      break;
    }
  }
  if (!saw_later_finalization) {
    return td::Status::Error(PSTRING() << "honest validators never finalized past fully skipped window range ["
                                       << first_skipped_slot << ", " << (last_skipped_slot + 1) << ")");
  }

  return td::Status::OK();
}

td::Status verify_adjacent_window_fork_rolls_back_after_late_alternate_finalization(const TraceSnapshot& snapshot) {
  // Covers Kernel-28's narrower rollback regression:
  // after an adjacent-window fork is both notarized and skipped, later honest finalization must
  // collapse the fork and honest validators must not later emit Final votes for the forked branch.
  TRY_STATUS(verify_fork_descendants_collapse_after_later_finalization(snapshot));

  std::optional<td::uint32> forked_slot;
  {
    std::scoped_lock lock(test_expectations_mutex);
    forked_slot = test_expectations.forked_slot;
  }
  if (!forked_slot.has_value()) {
    return td::Status::Error("scenario did not register the adjacent fork slot");
  }

  std::optional<CandidateId> forked_candidate_id;
  double first_honest_later_finalization_ts = std::numeric_limits<double>::infinity();
  for (const auto& record : snapshot.notarizations_observed) {
    if (record.id.slot == *forked_slot) {
      forked_candidate_id = record.id;
      break;
    }
  }
  if (!forked_candidate_id.has_value()) {
    return td::Status::Error(PSTRING() << "adjacent fork slot " << *forked_slot << " was never notarized");
  }

  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 0 || record.id.slot <= *forked_slot) {
      continue;
    }
    first_honest_later_finalization_ts = std::min(first_honest_later_finalization_ts, record.ts);
  }
  if (!std::isfinite(first_honest_later_finalization_ts)) {
    return td::Status::Error(PSTRING() << "no honest validator finalized after adjacent fork slot " << *forked_slot);
  }

  for (const auto& record : snapshot.protocol_votes) {
    if (record.src_node_idx == 0 || record.ts <= first_honest_later_finalization_ts + 1e-9) {
      continue;
    }
    if (auto* final = std::get_if<simplex::FinalizeVote>(&record.vote.vote);
        final != nullptr && final->id == *forked_candidate_id) {
      return td::Status::Error(PSTRING() << "honest validator #" << record.src_node_idx
                                         << " emitted late Final for forked candidate " << *forked_candidate_id
                                         << " after later finalization had already collapsed the fork");
    }
  }

  return td::Status::OK();
}

td::Status verify_restart_after_eight_non_mc_finalized_blocks_reaccepts_pending_candidates(
    const TraceSnapshot& snapshot) {
  // Covers Kernel-28's restart/reaccept regression:
  // once shardchain production crosses the eight-block non-MC-finalized threshold and switches to
  // empty candidates, a restarted validator must still reaccept the missing pending block chain.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> stop_slot;
  {
    std::scoped_lock lock(test_expectations_mutex);
    stop_slot = test_expectations.restart_reaccept_stop_slot;
  }
  if (!stop_slot.has_value()) {
    return td::Status::Error("scenario did not register the restart/reaccept stop slot");
  }

  double restart_ts = std::numeric_limits<double>::infinity();
  for (const auto& record : snapshot.lifecycle) {
    if (record.node_idx == 0 && record.instance_idx == 0 && record.started) {
      restart_ts = std::min(restart_ts, record.ts);
    }
  }
  if (!std::isfinite(restart_ts)) {
    return td::Status::Error("scenario never restarted validator #0.0");
  }

  std::set<CandidateId> empty_candidates;
  for (const auto& record : snapshot.candidates_generated) {
    if (record.is_empty && record.candidate_id.slot > *stop_slot) {
      empty_candidates.insert(record.candidate_id);
    }
  }
  for (const auto& record : snapshot.candidate_deliveries) {
    if (record.is_empty && record.candidate_id.slot > *stop_slot) {
      empty_candidates.insert(record.candidate_id);
    }
  }

  double first_empty_finalization_ts = std::numeric_limits<double>::infinity();
  td::uint32 first_empty_slot = 0;
  for (const auto& record : snapshot.finalizations_observed) {
    if (!empty_candidates.contains(record.id)) {
      continue;
    }
    if (record.ts < first_empty_finalization_ts) {
      first_empty_finalization_ts = record.ts;
      first_empty_slot = record.id.slot;
    }
  }
  if (!std::isfinite(first_empty_finalization_ts)) {
    td::uint32 highest_accepted_seqno = 0;
    for (const auto& record : snapshot.accepted_blocks) {
      highest_accepted_seqno = std::max(highest_accepted_seqno, record.block_id.seqno());
    }
    return td::Status::Error(PSTRING() << "scenario never reached empty-candidate finalization after validator #0.0 "
                                       << "fell behind; highest accepted shard block seqno observed was "
                                       << highest_accepted_seqno);
  }

  std::map<td::uint32, double> accepted_ts_by_seqno;
  for (const auto& record : snapshot.accepted_blocks) {
    if (record.node_idx != 0 || record.instance_idx != 0 || record.ts <= restart_ts + 1e-9) {
      continue;
    }
    auto [it, inserted] = accepted_ts_by_seqno.emplace(record.block_id.seqno(), record.ts);
    if (!inserted) {
      it->second = std::min(it->second, record.ts);
    }
  }

  bool saw_accept_after_empty = false;
  for (td::uint32 seqno = *stop_slot + 1; seqno <= *stop_slot + 8; ++seqno) {
    auto it = accepted_ts_by_seqno.find(seqno);
    if (it == accepted_ts_by_seqno.end()) {
      return td::Status::Error(PSTRING() << "restarted validator #0.0 never reaccepted pending shard block seqno "
                                         << seqno);
    }
    if (it->second > first_empty_finalization_ts + 1e-9) {
      saw_accept_after_empty = true;
    }
  }
  if (!saw_accept_after_empty) {
    return td::Status::Error("restarted validator #0.0 reaccepted blocks only before any empty slot finalized");
  }

  bool saw_node0_catchup_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 0 && record.instance_idx == 0 && record.ts > first_empty_finalization_ts + 1e-9 &&
        record.id.slot >= first_empty_slot) {
      saw_node0_catchup_finalization = true;
      break;
    }
  }
  if (!saw_node0_catchup_finalization) {
    return td::Status::Error("restarted validator #0.0 never observed finalization after the empty-slot catch-up point");
  }

  return td::Status::OK();
}

td::Status verify_standstill_without_certificate_rebroadcast_requires_recovery(const TraceSnapshot& snapshot) {
  // Covers Kernel-28's explicit "no cert rebroadcast => standstill recovery required" regression:
  // if one validator misses normal certificate traffic for a slot, it should only catch up after
  // standstill rebroadcast resumes once the filter is lifted.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<td::uint32> attacked_slot;
  std::optional<double> release_ts;
  {
    std::scoped_lock lock(test_expectations_mutex);
    attacked_slot = test_expectations.standstill_recovery_slot;
    release_ts = test_expectations.standstill_recovery_release_ts;
  }
  if (!attacked_slot.has_value() || !release_ts.has_value()) {
    return td::Status::Error("scenario did not register standstill certificate-recovery expectations");
  }

  bool saw_cert_to_node0_before_release = false;
  for (const auto& record : snapshot.protocol_certificates) {
    if (record.dst_node_idx == 0 && record.vote.referenced_slot() == *attacked_slot &&
        record.ts < *release_ts - 1e-9) {
      saw_cert_to_node0_before_release = true;
      break;
    }
  }
  if (saw_cert_to_node0_before_release) {
    return td::Status::Error(PSTRING() << "validator #0 received ordinary certificate traffic for slot "
                                       << *attacked_slot << " before the standstill-recovery filter was lifted");
  }

  bool saw_post_release_cert_to_node0 = false;
  for (const auto& record : snapshot.protocol_certificates) {
    if (record.dst_node_idx == 0 && record.vote.referenced_slot() >= *attacked_slot &&
        record.ts > *release_ts + 1e-9) {
      saw_post_release_cert_to_node0 = true;
      break;
    }
  }
  if (!saw_post_release_cert_to_node0) {
    return td::Status::Error(PSTRING() << "validator #0 never received any standstill-era certificate for slot >= "
                                       << *attacked_slot << " after filters were lifted");
  }

  double first_node0_finalization_ts = std::numeric_limits<double>::infinity();
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 0 && record.instance_idx == 0 && record.id.slot >= *attacked_slot) {
      first_node0_finalization_ts = std::min(first_node0_finalization_ts, record.ts);
    }
  }
  if (!std::isfinite(first_node0_finalization_ts)) {
    return td::Status::Error(PSTRING() << "validator #0 never caught up to slot " << *attacked_slot
                                       << " after standstill recovery");
  }
  if (first_node0_finalization_ts <= *release_ts + 1e-9) {
    return td::Status::Error("validator #0 caught up before standstill recovery traffic was allowed through");
  }

  return td::Status::OK();
}

td::Status verify_good_candidate_with_wrong_slot_parent_hash_does_not_resolve(const TraceSnapshot& snapshot) {
  // Covers Kernel-28's wrong-slot-parent-hash regression:
  // a structurally valid full candidate with a parent id whose slot/hash chain is impossible must
  // not be notarized, and honest progress should continue past the attacked slot.
  TRY_STATUS(verify_adversarial_invariants(snapshot));

  std::optional<CandidateId> attacked_candidate_id;
  {
    std::scoped_lock lock(test_expectations_mutex);
    attacked_candidate_id = test_expectations.wrong_parent_hash_candidate_id;
  }
  if (!attacked_candidate_id.has_value()) {
    return td::Status::Error("scenario did not register the wrong-parent-hash candidate");
  }

  bool saw_delivery = false;
  for (const auto& record : snapshot.candidate_deliveries) {
    if (record.candidate_id == *attacked_candidate_id) {
      saw_delivery = true;
      break;
    }
  }
  if (!saw_delivery) {
    return td::Status::Error(PSTRING() << "wrong-parent-hash candidate " << *attacked_candidate_id
                                       << " was never delivered to any validator");
  }

  for (const auto& record : snapshot.protocol_votes) {
    if (auto* notar = std::get_if<simplex::NotarizeVote>(&record.vote.vote);
        notar != nullptr && notar->id == *attacked_candidate_id) {
      return td::Status::Error(PSTRING() << "honest validator #" << record.src_node_idx
                                         << " voted Notar for wrong-parent-hash candidate "
                                         << *attacked_candidate_id);
    }
  }
  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* notar = std::get_if<simplex::NotarizeVote>(&record.vote.vote);
        notar != nullptr && notar->id == *attacked_candidate_id) {
      return td::Status::Error(PSTRING() << "wrong-parent-hash candidate " << *attacked_candidate_id
                                         << " still reached a notarization certificate");
    }
  }

  bool saw_skip_cert = false;
  for (const auto& record : snapshot.protocol_certificates) {
    if (auto* skip = std::get_if<simplex::SkipVote>(&record.vote.vote);
        skip != nullptr && skip->slot == attacked_candidate_id->slot) {
      saw_skip_cert = true;
      break;
    }
  }
  if (!saw_skip_cert) {
    return td::Status::Error(PSTRING() << "attacked slot " << attacked_candidate_id->slot
                                       << " never cleared by Skip");
  }

  bool saw_later_finalization = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx != 1 && record.id.slot > attacked_candidate_id->slot) {
      saw_later_finalization = true;
      break;
    }
  }
  if (!saw_later_finalization) {
    return td::Status::Error(PSTRING() << "honest validators never finalized past wrong-parent-hash slot "
                                       << attacked_candidate_id->slot);
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
    case TestCase::SkipTimeoutUsesLastFinalization:
      return verify_skip_timeout_uses_last_finalization(snapshot);
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
    case TestCase::SilentLeaderWindowSkipsButNextHonestWindowFinalizes:
      return verify_silent_leader_window_skips_but_next_honest_window_finalizes(snapshot);
    case TestCase::SilentValidatorDoesNotStopProgress:
      return verify_silent_validator_does_not_stop_progress(snapshot);
    case TestCase::BadCandidateResolutionResponderDoesNotStopCatchup:
      return verify_bad_candidate_resolution_responder_does_not_stop_catchup(snapshot);
    case TestCase::ConsecutiveSilentLeadersThenProgress:
      return verify_consecutive_silent_leaders_then_progress(snapshot);
    case TestCase::LeaderCandidateToTooLittleWeightSkipsThenProgress:
      return verify_leader_candidate_to_too_little_weight_skips_then_progress(snapshot);
    case TestCase::DuplicateInstanceSameKeySplitBrainStillProgresses:
      return verify_duplicate_instance_same_key_split_brain_still_progresses(snapshot);
    case TestCase::SleepingValidatorRecoversAfterHonestProgress:
      return verify_sleeping_validator_recovers_after_honest_progress(snapshot);
    case TestCase::StandstillWithByzantineWithholdingStillRecovers:
      return verify_standstill_with_byzantine_withholding_still_recovers(snapshot);
    case TestCase::EmptyCandidatesWithSilentValidatorStillFinalize:
      return verify_empty_candidates_with_silent_validator_still_finalize(snapshot);
    case TestCase::NoNotarObservedSkipWins:
      return verify_no_notar_observed_skip_wins(snapshot);
    case TestCase::LaterSlotWaitsForEarlierNotarProof:
      return verify_later_slot_waits_for_earlier_notar_proof(snapshot);
    case TestCase::ReplayStaleCertificateDoesNotDisturbProgress:
      return verify_replay_stale_certificate_does_not_disturb_progress(snapshot);
    case TestCase::NotarizedAndSkippedSlotCoexistWithoutFinalization:
      return verify_notarized_and_skipped_slot_coexist_without_finalization(snapshot);
    case TestCase::FinalAndSkipRacePreservesLocalDiscipline:
      return verify_final_and_skip_race_preserves_local_discipline(snapshot);
    case TestCase::EquivocatingLeaderDoesNotCreateTwoNotarsOrFinals:
      return verify_equivocating_leader_does_not_create_two_notars_or_finals(snapshot);
    case TestCase::DelayedFinalizationObservationPreservesPrefix:
      return verify_delayed_finalization_observation_preserves_prefix(snapshot);
    case TestCase::ForkDescendantsCollapseAfterLaterFinalization:
      return verify_fork_descendants_collapse_after_later_finalization(snapshot);
    case TestCase::MissingAncestorChainStillResolves:
      return verify_missing_ancestor_chain_still_resolves(snapshot);
    case TestCase::LaterSlotWaitsForGapSkipProof:
      return verify_later_slot_waits_for_gap_skip_proof(snapshot);
    case TestCase::InvalidCandidateSignatureDoesNotResolve:
      return verify_invalid_candidate_signature_does_not_resolve(snapshot);
    case TestCase::MalformedNotarCertificateDoesNotResolve:
      return verify_malformed_notar_certificate_does_not_resolve(snapshot);
    case TestCase::LongStallClearsSkippedSlotsThenHonestLeaderFinalizes:
      return verify_long_stall_clears_skipped_slots_then_honest_leader_finalizes(snapshot);
    case TestCase::MultiSlotEquivocationWindowPreservesSafety:
      return verify_multi_slot_equivocation_window_preserves_safety(snapshot);
    case TestCase::InvalidFullCandidateDoesNotNotarize:
      return verify_invalid_full_candidate_does_not_notarize(snapshot);
    case TestCase::SkipTimeoutCapPlateausUnderStall:
      return verify_skip_timeout_cap_plateaus_under_stall(snapshot);
    case TestCase::CountThresholdMismatchPreservesSafety:
      return verify_count_threshold_mismatch_preserves_safety(snapshot);
    case TestCase::StandstillFloodStillRecovers:
      return verify_standstill_flood_still_recovers(snapshot);
    case TestCase::SeveralWindowsFullySkippedThenProgressResumes:
      return verify_several_windows_fully_skipped_then_progress_resumes(snapshot);
    case TestCase::AdjacentWindowForkRollsBackAfterLateAlternateFinalization:
      return verify_adjacent_window_fork_rolls_back_after_late_alternate_finalization(snapshot);
    case TestCase::RestartAfterEightNonMcFinalizedBlocksReacceptsPendingCandidates:
      return verify_restart_after_eight_non_mc_finalized_blocks_reaccepts_pending_candidates(snapshot);
    case TestCase::StandstillWithoutCertificateRebroadcastRequiresRecovery:
      return verify_standstill_without_certificate_rebroadcast_requires_recovery(snapshot);
    case TestCase::GoodCandidateWithWrongSlotParentHashDoesNotResolve:
      return verify_good_candidate_with_wrong_slot_parent_hash_does_not_resolve(snapshot);
  }
  UNREACHABLE();
}

class TestTraceSink : public td::actor::Actor {
 public:
  void record_protocol_vote(size_t src_node_idx, size_t src_instance_idx, std::optional<size_t> dst_node_idx,
                            simplex::Vote vote, std::string raw_message) {
    protocol_votes_.push_back(ProtocolVoteTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .src_node_idx = src_node_idx,
        .src_instance_idx = src_instance_idx,
        .dst_node_idx = dst_node_idx,
        .vote = std::move(vote),
        .raw_message = std::move(raw_message),
    });
  }

  void record_protocol_certificate(size_t src_node_idx, size_t src_instance_idx, std::optional<size_t> dst_node_idx,
                                   simplex::Vote vote, std::vector<PeerValidatorId> signers, std::string raw_message) {
    protocol_certificates_.push_back(ProtocolCertificateTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .src_node_idx = src_node_idx,
        .src_instance_idx = src_instance_idx,
        .dst_node_idx = dst_node_idx,
        .vote = std::move(vote),
        .signers = std::move(signers),
        .raw_message = std::move(raw_message),
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

  void record_malformed_candidate_response(size_t requester_node_idx, size_t requester_instance_idx,
                                           size_t responder_node_idx, CandidateId id) {
    malformed_candidate_responses_.push_back(MalformedCandidateResponseTraceRecord{
        .ts = td::Time::now_unadjusted(),
        .requester_node_idx = requester_node_idx,
        .requester_instance_idx = requester_instance_idx,
        .responder_node_idx = responder_node_idx,
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
    malformed_candidate_responses_.clear();
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
        .malformed_candidate_responses = malformed_candidate_responses_,
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
  std::vector<MalformedCandidateResponseTraceRecord> malformed_candidate_responses_;
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
  (void)bus;
  auto maybe_tl_vote = fetch_tl_object<simplex::tl::vote>(data, true);
  if (maybe_tl_vote.is_error()) {
    return std::nullopt;
  }
  auto tl_vote = maybe_tl_vote.move_as_ok();
  return ProtocolVoteTraceRecord{
      .ts = td::Time::now_unadjusted(),
      .src_node_idx = src_node_idx,
      .src_instance_idx = src_instance_idx,
      .dst_node_idx = dst_node_idx,
      .vote = simplex::Vote::from_tl(std::move(*tl_vote->vote_)),
      .raw_message = data.str(),
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
  auto tl_certificate = maybe_tl_certificate.move_as_ok();
  std::vector<PeerValidatorId> signers;
  for (const auto& signature : tl_certificate->signatures_->votes_) {
    auto who = static_cast<td::uint32>(signature->who_);
    if (who < bus.validator_set.size()) {
      signers.push_back(PeerValidatorId{who});
    }
  }
  return ProtocolCertificateTraceRecord{
      .ts = td::Time::now_unadjusted(),
      .src_node_idx = src_node_idx,
      .src_instance_idx = src_instance_idx,
      .dst_node_idx = dst_node_idx,
      .vote = simplex::Vote::from_tl(std::move(*tl_certificate->vote_)),
      .signers = std::move(signers),
      .raw_message = data.str(),
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

std::optional<std::pair<td::uint32, TestMessageFilters::ProtocolKind>> classify_protocol_message(const TestSimplexBus& bus,
                                                                                                  size_t src_node_idx,
                                                                                                  size_t src_instance_idx,
                                                                                                  size_t dst_node_idx,
                                                                                                  td::Slice data) {
  if (auto maybe_vote = try_decode_protocol_vote(bus, src_node_idx, src_instance_idx, dst_node_idx, data);
      maybe_vote.has_value()) {
    auto classify_vote = [&](const simplex::NotarizeVote& vote) {
      return std::make_pair(vote.id.slot, TestMessageFilters::ProtocolKind::NotarVote);
    };
    auto classify_skip = [&](const simplex::SkipVote& vote) {
      return std::make_pair(vote.slot, TestMessageFilters::ProtocolKind::SkipVote);
    };
    auto classify_final = [&](const simplex::FinalizeVote& vote) {
      return std::make_pair(vote.id.slot, TestMessageFilters::ProtocolKind::FinalVote);
    };
    return std::visit(td::overloaded(classify_vote, classify_skip, classify_final), maybe_vote->vote.vote);
  }
  if (auto maybe_certificate = try_decode_protocol_certificate(bus, src_node_idx, src_instance_idx, dst_node_idx, data);
      maybe_certificate.has_value()) {
    auto classify_vote = [&](const simplex::NotarizeVote& vote) {
      return std::make_pair(vote.id.slot, TestMessageFilters::ProtocolKind::NotarCert);
    };
    auto classify_skip = [&](const simplex::SkipVote& vote) {
      return std::make_pair(vote.slot, TestMessageFilters::ProtocolKind::SkipCert);
    };
    auto classify_final = [&](const simplex::FinalizeVote& vote) {
      return std::make_pair(vote.id.slot, TestMessageFilters::ProtocolKind::FinalCert);
    };
    return std::visit(td::overloaded(classify_vote, classify_skip, classify_final), maybe_certificate->vote.vote);
  }
  return std::nullopt;
}

bool should_drop_protocol_message(const TestSimplexBus& bus, size_t src_node_idx, size_t src_instance_idx,
                                  size_t dst_node_idx, td::Slice data) {
  auto maybe_classified = classify_protocol_message(bus, src_node_idx, src_instance_idx, dst_node_idx, data);
  if (!maybe_classified.has_value()) {
    return false;
  }

  auto [slot, kind] = *maybe_classified;
  std::scoped_lock lock(test_message_filters_mutex);
  for (const auto& rule : test_message_filters.protocol_drop_rules) {
    if (rule.src_node_idx.has_value() && *rule.src_node_idx != src_node_idx) {
      continue;
    }
    if (rule.src_instance_idx.has_value() && *rule.src_instance_idx != src_instance_idx) {
      continue;
    }
    if (!rule.dst_node_indices.contains(dst_node_idx)) {
      continue;
    }
    if (slot < rule.start_slot || slot >= rule.end_slot) {
      continue;
    }
    if (rule.kind != kind) {
      continue;
    }
    return true;
  }
  return false;
}

bool should_drop_candidate_delivery(size_t src_node_idx, size_t src_instance_idx, size_t dst_node_idx,
                                    const CandidateRef& candidate) {
  std::scoped_lock lock(test_message_filters_mutex);
  td::uint32 slot = candidate->id.slot;
  for (const auto& rule : test_message_filters.candidate_drop_rules) {
    if (rule.src_node_idx.has_value() && *rule.src_node_idx != src_node_idx) {
      continue;
    }
    if (rule.src_instance_idx.has_value() && *rule.src_instance_idx != src_instance_idx) {
      continue;
    }
    if (!rule.dst_node_indices.contains(dst_node_idx)) {
      continue;
    }
    if (slot < rule.start_slot || slot >= rule.end_slot) {
      continue;
    }
    return true;
  }
  return false;
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

struct TamperedCandidateResponse {
  CandidateId id;
  td::BufferSlice data;
};

std::optional<TamperedCandidateResponse> maybe_make_malformed_candidate_response(size_t requester_node_idx,
                                                                                 size_t requester_instance_idx,
                                                                                 size_t responder_node_idx,
                                                                                 td::Slice request_data,
                                                                                 td::Slice response_data) {
  auto maybe_request = fetch_tl_object<ton_api::consensus_simplex_requestCandidate>(request_data, true);
  if (maybe_request.is_error()) {
    return std::nullopt;
  }
  auto request = maybe_request.move_as_ok();

  std::scoped_lock lock(test_message_filters_mutex);
  if (test_message_filters.malformed_candidate_response_src_node != requester_node_idx ||
      test_message_filters.malformed_candidate_response_src_instance != requester_instance_idx ||
      test_message_filters.malformed_candidate_response_dst_node != responder_node_idx ||
      test_message_filters.malformed_candidate_response_budget == 0) {
    return std::nullopt;
  }
  --test_message_filters.malformed_candidate_response_budget;

  if (test_message_filters.malformed_candidate_response_kind == TestMessageFilters::MalformedCandidateResponseKind::Junk) {
    td::BufferSlice bad_candidate;
    td::BufferSlice bad_notar;
    if (request->want_candidate_) {
      bad_candidate = td::BufferSlice("malformed-candidate");
    } else if (request->want_notar_) {
      bad_notar = td::BufferSlice("malformed-notar");
    } else {
      return std::nullopt;
    }

    return TamperedCandidateResponse{
        .id = CandidateId::from_tl(request->id_),
        .data = serialize_tl_object(
            create_tl_object<ton_api::consensus_simplex_candidateAndCert>(std::move(bad_candidate), std::move(bad_notar)),
            true),
    };
  }

  auto maybe_response = fetch_tl_object<ton_api::consensus_simplex_candidateAndCert>(response_data, true);
  if (maybe_response.is_error()) {
    return std::nullopt;
  }
  auto response = maybe_response.move_as_ok();
  if (test_message_filters.malformed_candidate_response_kind ==
      TestMessageFilters::MalformedCandidateResponseKind::CorruptCandidate) {
    if (response->candidate_.empty()) {
      return std::nullopt;
    }
    auto maybe_candidate = fetch_tl_object<tl::CandidateData>(response->candidate_.as_slice(), true);
    if (maybe_candidate.is_error()) {
      return std::nullopt;
    }
    auto candidate = maybe_candidate.move_as_ok();
    bool corrupted = false;
    ton_api::downcast_call(
        *candidate,
        td::overloaded(
            [&](tl::empty& empty_candidate) {
              if (!empty_candidate.signature_.empty()) {
                auto signature = empty_candidate.signature_.clone();
                signature.as_slice()[0] ^= 0x01;
                empty_candidate.signature_ = std::move(signature);
                corrupted = true;
              }
            },
            [&](tl::block& full_candidate) {
              if (!full_candidate.signature_.empty()) {
                auto signature = full_candidate.signature_.clone();
                signature.as_slice()[0] ^= 0x01;
                full_candidate.signature_ = std::move(signature);
                corrupted = true;
              }
            }));
    if (!corrupted) {
      return std::nullopt;
    }
    auto serialized_candidate = serialize_tl_object(candidate, true);
    auto tampered = create_tl_object<ton_api::consensus_simplex_candidateAndCert>(std::move(serialized_candidate),
                                                                                  std::move(response->notar_));
    return TamperedCandidateResponse{
        .id = CandidateId::from_tl(request->id_),
        .data = serialize_tl_object(std::move(tampered), true),
    };
  }
  if (test_message_filters.malformed_candidate_response_kind ==
      TestMessageFilters::MalformedCandidateResponseKind::CorruptNotar) {
    if (response->notar_.empty()) {
      return std::nullopt;
    }
    auto maybe_notar = fetch_tl_object<simplex::tl::voteSignatureSet>(response->notar_.as_slice(), true);
    if (maybe_notar.is_error()) {
      return std::nullopt;
    }
    auto notar = maybe_notar.move_as_ok();
    if (notar->votes_.empty() || notar->votes_[0]->signature_.empty()) {
      return std::nullopt;
    }
    auto signature = notar->votes_[0]->signature_.clone();
    signature.as_slice()[0] ^= 0x01;
    notar->votes_[0]->signature_ = std::move(signature);
    auto serialized_notar = serialize_tl_object(notar, true);
    auto tampered = create_tl_object<ton_api::consensus_simplex_candidateAndCert>(std::move(response->candidate_),
                                                                                  std::move(serialized_notar));
    return TamperedCandidateResponse{
        .id = CandidateId::from_tl(request->id_),
        .data = serialize_tl_object(std::move(tampered), true),
    };
  }
  return std::nullopt;
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
                                vote->src_instance_idx, vote->dst_node_idx, std::move(vote->vote),
                                std::move(vote->raw_message));
        return;
      }
      if (auto cert = try_decode_protocol_certificate(test_bus, bus->local_id.idx.value(), instance_idx_, dst_idx,
                                                      message->message.data.as_slice())) {
        td::actor::send_closure(test_bus.trace_sink, &TestTraceSink::record_protocol_certificate, cert->src_node_idx,
                                cert->src_instance_idx, cert->dst_node_idx, std::move(cert->vote),
                                std::move(cert->signers), std::move(cert->raw_message));
      }
    };
    if (message->recipient.has_value()) {
      CHECK(message->recipient.value() != bus->local_id.idx);
      if (should_drop_protocol_message(test_bus, bus->local_id.idx.value(), instance_idx_,
                                       message->recipient->value(), message->message.data.as_slice())) {
        return;
      }
      trace_message(message->recipient->value());
      td::actor::ask(test_overlay, &TestOverlay::send_message, bus->local_id, instance_idx_,
                     message->recipient->value(), message->message.data.clone())
          .detach_silent();
    } else {
      for (size_t i = 0; i < bus->validator_set.size(); ++i) {
        if (bus->local_id.idx.value() != i) {
          if (should_drop_protocol_message(test_bus, bus->local_id.idx.value(), instance_idx_, i,
                                           message->message.data.as_slice())) {
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
  if (should_drop_candidate_delivery(src.idx.value(), src_instance_idx, dst_idx, candidate)) {
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
  td::BufferSlice request_copy = message.clone();
  auto dst_instance_idx = (size_t)td::Random::fast(0, (int)nodes_[dst_idx].size() - 1);
  co_await before_receive(src.idx.value(), src_instance_idx, dst_idx, true);
  // Re-read the instance after suspension instead of keeping a reference across co_await.
  if (dst_instance_idx >= nodes_[dst_idx].size()) {
    co_return td::Status::Error("instance disappeared");
  }
  auto instance_actor = nodes_[dst_idx][dst_instance_idx].actor;
  bool instance_disabled = nodes_[dst_idx][dst_instance_idx].disabled;
  if (instance_actor.empty() || instance_disabled) {
    co_return td::Status::Error("instance is stopped/disabled");
  }
  auto response = co_await td::actor::ask(instance_actor, &TestOverlayNode::receive_query, src, std::move(message));
  co_await before_receive(dst_idx, dst_instance_idx, src.idx.value(), true);
  if (auto tampered = maybe_make_malformed_candidate_response(src.idx.value(), src_instance_idx, dst_idx,
                                                              request_copy.as_slice(), response.as_slice());
      tampered.has_value()) {
    if (!trace_sink_.empty()) {
      td::actor::send_closure(trace_sink_, &TestTraceSink::record_malformed_candidate_response, src.idx.value(),
                              src_instance_idx, dst_idx, tampered->id);
    }
    co_return std::move(tampered->data);
  }
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
    {
      std::scoped_lock lock(test_expectations_mutex);
      if (test_expectations.invalid_full_candidate_slot.has_value() &&
          candidate.id.seqno() == *test_expectations.invalid_full_candidate_slot) {
        co_return CandidateReject{.reason = "synthetic invalid candidate", .proof = td::BufferSlice()};
      }
    }
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
    test_message_filters.protocol_drop_rules.push_back(TestMessageFilters::ProtocolDropRule{
        .src_node_idx = std::nullopt,
        .src_instance_idx = std::nullopt,
        .dst_node_indices = {dst_node_idx},
        .start_slot = from_slot,
        .end_slot = std::numeric_limits<td::uint32>::max(),
        .kind = TestMessageFilters::ProtocolKind::FinalCert,
    });
    co_return td::Unit{};
  }

  td::actor::Task<> set_drop_notar_certs_for_test(size_t dst_node_idx, td::uint32 start_slot, td::uint32 end_slot) {
    std::scoped_lock lock(test_message_filters_mutex);
    test_message_filters.protocol_drop_rules.push_back(TestMessageFilters::ProtocolDropRule{
        .src_node_idx = std::nullopt,
        .src_instance_idx = std::nullopt,
        .dst_node_indices = {dst_node_idx},
        .start_slot = start_slot,
        .end_slot = end_slot,
        .kind = TestMessageFilters::ProtocolKind::NotarCert,
    });
    co_return td::Unit{};
  }

  td::actor::Task<> set_drop_notar_votes_for_test(size_t dst_node_idx, td::uint32 start_slot, td::uint32 end_slot) {
    std::scoped_lock lock(test_message_filters_mutex);
    test_message_filters.protocol_drop_rules.push_back(TestMessageFilters::ProtocolDropRule{
        .src_node_idx = std::nullopt,
        .src_instance_idx = std::nullopt,
        .dst_node_indices = {dst_node_idx},
        .start_slot = start_slot,
        .end_slot = end_slot,
        .kind = TestMessageFilters::ProtocolKind::NotarVote,
    });
    co_return td::Unit{};
  }

  td::actor::Task<> set_selective_protocol_drop_for_test(size_t src_node_idx, size_t src_instance_idx,
                                                         std::vector<size_t> dst_node_indices,
                                                         td::uint32 start_slot, td::uint32 end_slot,
                                                         TestMessageFilters::ProtocolKind kind) {
    std::scoped_lock lock(test_message_filters_mutex);
    test_message_filters.protocol_drop_rules.push_back(TestMessageFilters::ProtocolDropRule{
        .src_node_idx = src_node_idx,
        .src_instance_idx = src_instance_idx,
        .dst_node_indices = {dst_node_indices.begin(), dst_node_indices.end()},
        .start_slot = start_slot,
        .end_slot = end_slot,
        .kind = kind,
    });
    co_return td::Unit{};
  }

  td::actor::Task<> set_drop_candidates_for_test(size_t dst_node_idx, td::uint32 start_slot, td::uint32 end_slot) {
    std::scoped_lock lock(test_message_filters_mutex);
    test_message_filters.candidate_drop_rules.push_back(TestMessageFilters::CandidateDropRule{
        .src_node_idx = std::nullopt,
        .src_instance_idx = std::nullopt,
        .dst_node_indices = {dst_node_idx},
        .start_slot = start_slot,
        .end_slot = end_slot,
    });
    co_return td::Unit{};
  }

  td::actor::Task<> set_drop_candidates_for_test(std::vector<size_t> dst_node_indices, td::uint32 start_slot,
                                                 td::uint32 end_slot) {
    std::scoped_lock lock(test_message_filters_mutex);
    test_message_filters.candidate_drop_rules.push_back(TestMessageFilters::CandidateDropRule{
        .src_node_idx = std::nullopt,
        .src_instance_idx = std::nullopt,
        .dst_node_indices = {dst_node_indices.begin(), dst_node_indices.end()},
        .start_slot = start_slot,
        .end_slot = end_slot,
    });
    co_return td::Unit{};
  }

  td::actor::Task<> set_selective_candidate_drop_for_test(size_t src_node_idx, size_t src_instance_idx,
                                                          std::vector<size_t> dst_node_indices,
                                                          td::uint32 start_slot, td::uint32 end_slot) {
    std::scoped_lock lock(test_message_filters_mutex);
    test_message_filters.candidate_drop_rules.push_back(TestMessageFilters::CandidateDropRule{
        .src_node_idx = src_node_idx,
        .src_instance_idx = src_instance_idx,
        .dst_node_indices = {dst_node_indices.begin(), dst_node_indices.end()},
        .start_slot = start_slot,
        .end_slot = end_slot,
    });
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

  td::actor::Task<> set_malformed_candidate_responses_for_test(size_t src_node_idx, size_t src_instance_idx,
                                                               size_t dst_node_idx, size_t budget,
                                                               TestMessageFilters::MalformedCandidateResponseKind kind =
                                                                   TestMessageFilters::MalformedCandidateResponseKind::Junk) {
    std::scoped_lock lock(test_message_filters_mutex);
    test_message_filters.malformed_candidate_response_src_node = src_node_idx;
    test_message_filters.malformed_candidate_response_src_instance = src_instance_idx;
    test_message_filters.malformed_candidate_response_dst_node = dst_node_idx;
    test_message_filters.malformed_candidate_response_budget = budget;
    test_message_filters.malformed_candidate_response_kind = kind;
    co_return td::Unit{};
  }

  td::actor::Task<> set_candidate_resolution_target_for_test(CandidateId id) {
    std::scoped_lock lock(test_expectations_mutex);
    test_expectations.candidate_resolution_target = id;
    co_return td::Unit{};
  }

  td::actor::Task<> set_skip_timeout_expectations_for_test(td::uint32 last_finalized_window,
                                                           td::uint32 stalled_window) {
    std::scoped_lock lock(test_expectations_mutex);
    test_expectations.skip_timeout_last_finalized_window = last_finalized_window;
    test_expectations.skip_timeout_stalled_window = stalled_window;
    co_return td::Unit{};
  }

  td::actor::Task<> set_consecutive_silent_leaders_expectations_for_test(std::vector<size_t> node_indices,
                                                                         td::uint32 start_slot, size_t skip_count) {
    std::scoped_lock lock(test_expectations_mutex);
    test_expectations.adversarial_nodes = std::move(node_indices);
    test_expectations.consecutive_skip_start_slot = start_slot;
    test_expectations.consecutive_skip_count = skip_count;
    co_return td::Unit{};
  }

  td::actor::Task<> set_candidate_starvation_expectations_for_test(std::vector<size_t> node_indices,
                                                                   td::uint32 slot,
                                                                   std::vector<size_t> dropped_nodes) {
    std::scoped_lock lock(test_expectations_mutex);
    test_expectations.adversarial_nodes = std::move(node_indices);
    test_expectations.candidate_starvation_slot = slot;
    test_expectations.candidate_starvation_dropped_nodes = std::move(dropped_nodes);
    co_return td::Unit{};
  }

  td::actor::Task<> set_no_notarization_expectations_for_test(std::vector<size_t> node_indices, td::uint32 slot) {
    std::scoped_lock lock(test_expectations_mutex);
    test_expectations.adversarial_nodes = std::move(node_indices);
    test_expectations.no_notarization_slot = slot;
    co_return td::Unit{};
  }

  td::actor::Task<> set_parent_gating_slot_for_test(td::uint32 slot) {
    std::scoped_lock lock(test_expectations_mutex);
    test_expectations.parent_gating_slot = slot;
    co_return td::Unit{};
  }

  td::actor::Task<> set_replayed_stale_slot_for_test(td::uint32 slot) {
    std::scoped_lock lock(test_expectations_mutex);
    test_expectations.replayed_stale_slot = slot;
    co_return td::Unit{};
  }

  td::actor::Task<> set_forked_slot_for_test(td::uint32 slot) {
    std::scoped_lock lock(test_expectations_mutex);
    test_expectations.forked_slot = slot;
    co_return td::Unit{};
  }

  td::actor::Task<> set_equivocation_expectations_for_test(td::uint32 slot, std::vector<CandidateId> candidates) {
    std::scoped_lock lock(test_expectations_mutex);
    test_expectations.equivocation_slot = slot;
    test_expectations.equivocation_candidates = std::move(candidates);
    co_return td::Unit{};
  }

  td::actor::Task<> set_delayed_finalization_expectations_for_test(td::uint32 from_slot, size_t node_idx) {
    std::scoped_lock lock(test_expectations_mutex);
    test_expectations.delayed_finalization_from_slot = from_slot;
    test_expectations.delayed_finalization_node_idx = node_idx;
    co_return td::Unit{};
  }

  td::actor::Task<> set_delayed_finalization_release_ts_for_test(double ts) {
    std::scoped_lock lock(test_expectations_mutex);
    test_expectations.delayed_finalization_release_ts = ts;
    co_return td::Unit{};
  }

  td::actor::Task<> set_gap_skip_slot_for_test(td::uint32 slot) {
    std::scoped_lock lock(test_expectations_mutex);
    test_expectations.gap_skip_slot = slot;
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

  td::actor::Task<> inject_protocol_message_for_test(size_t src_node_idx, std::vector<size_t> dst_node_indices,
                                                     std::string raw_message) {
    for (size_t dst_node_idx : dst_node_indices) {
      for (auto& inst : nodes_[dst_node_idx].instances) {
        if (inst.status != Instance::Running) {
          continue;
        }
        inst.bus.publish<IncomingProtocolMessage>(PeerValidatorId(static_cast<td::uint32>(src_node_idx)),
                                                  td::BufferSlice(raw_message));
      }
    }
    co_return td::Unit{};
  }

  td::actor::Task<td::BufferSlice> sign_vote_for_test(size_t node_idx, const simplex::Vote& vote) {
    auto vote_to_sign = serialize_tl_object(vote.to_tl(), true);
    auto data_to_sign = create_serialize_tl_object<consensus::tl::dataToSign>(SESSION_ID, std::move(vote_to_sign));
    co_return co_await td::actor::ask(keyring_, &keyring::Keyring::sign_message, nodes_[node_idx].node_id,
                                      std::move(data_to_sign));
  }

  td::actor::Task<> inject_vote_for_test(size_t src_node_idx, std::vector<size_t> dst_node_indices, simplex::Vote vote) {
    auto signature = co_await sign_vote_for_test(src_node_idx, vote);
    simplex::Signed<simplex::Vote> signed_vote{PeerValidatorId{src_node_idx}, std::move(vote), std::move(signature)};
    td::BufferSlice serialized = signed_vote.serialize();
    std::string raw_message = serialized.as_slice().str();
    for (size_t dst_node_idx : dst_node_indices) {
      td::actor::send_closure(trace_sink_, &TestTraceSink::record_protocol_vote, src_node_idx, 0, dst_node_idx,
                              signed_vote.vote, raw_message);
    }
    co_return co_await inject_protocol_message_for_test(src_node_idx, std::move(dst_node_indices), std::move(raw_message));
  }

  BlockCandidate make_synthetic_block_candidate_for_test(td::uint32 slot, BlockSeqno prev_seqno, td::uint64 tag,
                                                         PeerValidatorId leader) const {
    double gen_utime = td::Clocks::system();

    block::gen::BlockInfo::Record info;
    info.version = 0;
    info.not_master = !SHARD.is_masterchain();
    info.after_merge = false;
    info.before_split = false;
    info.after_split = false;
    info.want_split = false;
    info.want_merge = false;
    info.key_block = false;
    info.vert_seqno_incr = false;
    info.flags = 0;
    info.seq_no = prev_seqno + 1;
    info.vert_seq_no = 0;
    vm::CellBuilder cb;
    block::ShardId{SHARD}.serialize(cb);
    info.shard = cb.as_cellslice_ref();
    info.gen_utime = static_cast<UnixTime>(gen_utime);
    info.start_lt = (LogicalTime)(prev_seqno + 1) * 1000;
    info.end_lt = (LogicalTime)(prev_seqno + 1) * 1000 + 1;
    info.gen_validator_list_hash_short = validator_set_->get_validator_set_hash();
    info.gen_catchain_seqno = validator_set_->get_catchain_seqno();
    info.min_ref_mc_seqno = MIN_MC_BLOCK_ID.seqno();
    info.prev_key_block_seqno = MIN_MC_BLOCK_ID.seqno();
    if (!SHARD.is_masterchain()) {
      info.master_ref = make_ext_blk_ref(MIN_MC_BLOCK_ID, 0);
    }
    info.prev_ref = make_ext_blk_ref(BlockIdExt(BlockId(SHARD, prev_seqno), gen_shard_state(prev_seqno)->get_hash().bits(),
                                                td::Bits256{}),
                                     ((LogicalTime)prev_seqno) * 1000 + 1);
    td::Ref<vm::Cell> block_info;
    CHECK(block::gen::pack_cell(block_info, info));

    td::Ref<vm::Cell> value_flow = vm::CellBuilder{}.finalize_novm();
    td::Ref<vm::Cell> merkle_update =
        vm::CellBuilder::create_merkle_update(gen_shard_state(prev_seqno), gen_shard_state(prev_seqno + 1));
    td::Bits256 rand_data;
    rand_data.as_array().fill(0);
    rand_data.as_slice().truncate(sizeof(tag)).copy_from(
        td::Slice{reinterpret_cast<const td::uint8*>(&tag), sizeof(tag)});
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
    auto consensus_extra = vm::CellBuilder{}
                               .store_long(0x638eb292, 32)
                               .store_long(0, 32)
                               .store_long(static_cast<td::uint64>(gen_utime * 1000.0), 64)
                               .store_long(tag, 64)
                               .finalize_novm();
    collated_roots.push_back(std::move(consensus_extra));
    td::BufferSlice collated_data = vm::std_boc_serialize_multi(collated_roots, 2).move_as_ok();

    return BlockCandidate(
        Ed25519_PublicKey{nodes_[leader.value()].public_key.ed25519_value().raw()},
        BlockIdExt(BlockId(SHARD, prev_seqno + 1), block_root->get_hash().bits(), td::sha256_bits256(data)),
        td::sha256_bits256(collated_data), std::move(data), std::move(collated_data));
  }

  CandidateRef make_synthetic_full_candidate_for_test(td::uint32 slot, ParentId parent, PeerValidatorId leader,
                                                      BlockSeqno prev_seqno, td::uint64 tag) const {
    auto block = make_synthetic_block_candidate_for_test(slot, prev_seqno, tag, leader);
    auto id = CandidateHashData::create_full(block, parent).build_id_with(slot);
    return td::make_ref<Candidate>(id, std::move(parent), leader, std::move(block), td::BufferSlice("synthetic-sign"));
  }

  td::actor::Task<> inject_candidate_for_test(size_t src_node_idx, std::vector<size_t> dst_node_indices,
                                              CandidateRef candidate) {
    for (size_t dst_node_idx : dst_node_indices) {
      size_t dst_instance_idx = 0;
      for (auto& inst : nodes_[dst_node_idx].instances) {
        if (inst.status != Instance::Running) {
          ++dst_instance_idx;
          continue;
        }
        td::actor::send_closure(trace_sink_, &TestTraceSink::record_candidate_delivery, src_node_idx, 0, dst_node_idx,
                                dst_instance_idx, candidate->id, candidate->parent_id, candidate->leader,
                                candidate->is_empty(), candidate->block_id());
        inst.bus.publish<CandidateReceived>(candidate);
        ++dst_instance_idx;
      }
    }
    co_return td::Unit{};
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
    NODE_WEIGHTS_OVERRIDE.clear();
    if (TEST_CASE == TestCase::SilentLeaderWindowSkipsButNextHonestWindowFinalizes ||
        TEST_CASE == TestCase::SilentValidatorDoesNotStopProgress) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 3.0;
      }
    }
    if (TEST_CASE == TestCase::BadCandidateResolutionResponderDoesNotStopCatchup ||
        TEST_CASE == TestCase::InvalidCandidateSignatureDoesNotResolve ||
        TEST_CASE == TestCase::MalformedNotarCertificateDoesNotResolve) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 200;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
    if (TEST_CASE == TestCase::LongStallClearsSkippedSlotsThenHonestLeaderFinalizes) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 8.0;
      }
      STANDSTILL_TIMEOUT_S = 0.5;
    }
    if (TEST_CASE == TestCase::LongStallClearsSkippedSlotsThenHonestLeaderFinalizes) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 5.0;
      }
    }
    if (TEST_CASE == TestCase::MissingAncestorChainStillResolves) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 200;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
    if (TEST_CASE == TestCase::LaterSlotWaitsForGapSkipProof) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
    if (TEST_CASE == TestCase::ConsecutiveSilentLeadersThenProgress) {
      if (N_NODES == 8) {
        N_NODES = 7;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 5.0;
      }
    }
    if (TEST_CASE == TestCase::LeaderCandidateToTooLittleWeightSkipsThenProgress) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
    if (TEST_CASE == TestCase::DuplicateInstanceSameKeySplitBrainStillProgresses) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (N_DOUBLE_NODES == 0) {
        N_DOUBLE_NODES = 1;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
    if (TEST_CASE == TestCase::SleepingValidatorRecoversAfterHonestProgress) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
    if (TEST_CASE == TestCase::StandstillWithByzantineWithholdingStillRecovers) {
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
    if (TEST_CASE == TestCase::EmptyCandidatesWithSilentValidatorStillFinalize) {
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
    if (TEST_CASE == TestCase::NoNotarObservedSkipWins) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
    if (TEST_CASE == TestCase::LaterSlotWaitsForEarlierNotarProof) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
    if (TEST_CASE == TestCase::ReplayStaleCertificateDoesNotDisturbProgress) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
    if (TEST_CASE == TestCase::NotarizedAndSkippedSlotCoexistWithoutFinalization) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
    if (TEST_CASE == TestCase::FinalAndSkipRacePreservesLocalDiscipline) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
    if (TEST_CASE == TestCase::ForkDescendantsCollapseAfterLaterFinalization) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
    if (TEST_CASE == TestCase::EquivocatingLeaderDoesNotCreateTwoNotarsOrFinals) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (N_DOUBLE_NODES == 0) {
        N_DOUBLE_NODES = 1;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
    if (TEST_CASE == TestCase::MultiSlotEquivocationWindowPreservesSafety) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (N_DOUBLE_NODES == 0) {
        N_DOUBLE_NODES = 1;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 3;
      }
      if (DURATION == 60.0) {
        DURATION = 5.0;
      }
    }
    if (TEST_CASE == TestCase::InvalidFullCandidateDoesNotNotarize) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 10.0;
      }
    }
    if (TEST_CASE == TestCase::SkipTimeoutCapPlateausUnderStall) {
      if (N_NODES == 8) {
        N_NODES = 10;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 5.0;
      }
      FIRST_BLOCK_TIMEOUT_S = 0.05;
      FIRST_BLOCK_TIMEOUT_MULTIPLIER = 4.0;
      FIRST_BLOCK_MAX_TIMEOUT_S = 0.2;
    }
    if (TEST_CASE == TestCase::CountThresholdMismatchPreservesSafety) {
      if (N_NODES == 8) {
        N_NODES = 6;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
      NODE_WEIGHTS_OVERRIDE = {25, 25, 25, 8, 8, 8};
    }
    if (TEST_CASE == TestCase::StandstillFloodStillRecovers) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 200;
      }
      if (DURATION == 60.0) {
        DURATION = 6.0;
      }
      STANDSTILL_TIMEOUT_S = 0.5;
    }
    if (TEST_CASE == TestCase::SeveralWindowsFullySkippedThenProgressResumes) {
      if (N_NODES == 8) {
        N_NODES = 13;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 7.0;
      }
    }
    if (TEST_CASE == TestCase::AdjacentWindowForkRollsBackAfterLateAlternateFinalization) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
    if (TEST_CASE == TestCase::RestartAfterEightNonMcFinalizedBlocksReacceptsPendingCandidates) {
      if (N_NODES == 8) {
        N_NODES = 10;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 6.0;
      }
    }
    if (TEST_CASE == TestCase::StandstillWithoutCertificateRebroadcastRequiresRecovery) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 200;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
      STANDSTILL_TIMEOUT_S = 0.5;
    }
    if (TEST_CASE == TestCase::GoodCandidateWithWrongSlotParentHashDoesNotResolve) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
    if (TEST_CASE == TestCase::DelayedFinalizationObservationPreservesPrefix) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (SLOTS_PER_LEADER_WINDOW == 4) {
        SLOTS_PER_LEADER_WINDOW = 1;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
    }
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
    if (TEST_CASE == TestCase::SkipTimeoutUsesLastFinalization) {
      if (N_NODES == 8) {
        N_NODES = 4;
      }
      if (TARGET_RATE_MS == 1000) {
        TARGET_RATE_MS = 100;
      }
      if (DURATION == 60.0) {
        DURATION = 4.0;
      }
      if (FIRST_BLOCK_TIMEOUT_MULTIPLIER == 1.05) {
        FIRST_BLOCK_TIMEOUT_MULTIPLIER = 2.0;
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

  td::actor::Task<> wait_for_finalization_past_slot_on(size_t node_idx, size_t instance_idx, td::uint32 slot,
                                                       double timeout_s) {
    td::Timestamp deadline = td::Timestamp::in(timeout_s);
    while (!deadline.is_in_past()) {
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == node_idx && record.instance_idx == instance_idx && record.id.slot > slot) {
          co_return td::Unit{};
        }
      }
      co_await td::actor::coro_sleep(td::Timestamp::in(0.05));
    }
    co_return td::Status::Error(PSTRING() << "timeout waiting for finalization past slot " << slot
                                          << " on validator #" << node_idx << "." << instance_idx);
  }

  td::actor::Task<> wait_for_accepted_block_seqno_on(size_t node_idx, size_t instance_idx, td::uint32 seqno,
                                                     double timeout_s) {
    td::Timestamp deadline = td::Timestamp::in(timeout_s);
    while (!deadline.is_in_past()) {
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      for (const auto& record : snapshot.accepted_blocks) {
        if (record.node_idx == node_idx && record.instance_idx == instance_idx && record.block_id.seqno() >= seqno) {
          co_return td::Unit{};
        }
      }
      co_await td::actor::coro_sleep(td::Timestamp::in(0.05));
    }
    co_return td::Status::Error(PSTRING() << "timeout waiting for accepted block seqno >= " << seqno
                                          << " on validator #" << node_idx << "." << instance_idx);
  }

  td::actor::Task<> wait_for_candidate_resolved_on(size_t node_idx, size_t instance_idx, CandidateId id,
                                                   double timeout_s) {
    td::Timestamp deadline = td::Timestamp::in(timeout_s);
    while (!deadline.is_in_past()) {
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      for (const auto& record : snapshot.candidates_resolved) {
        if (record.node_idx == node_idx && record.instance_idx == instance_idx && record.id == id) {
          co_return td::Unit{};
        }
      }
      co_await td::actor::coro_sleep(td::Timestamp::in(0.05));
    }
    co_return td::Status::Error(PSTRING() << "timeout waiting for candidate resolution of " << id << " on validator #"
                                          << node_idx << "." << instance_idx);
  }

  td::actor::Task<> wait_for_leader_window_observed_on(size_t node_idx, size_t instance_idx, td::uint32 start_slot,
                                                       double timeout_s) {
    td::Timestamp deadline = td::Timestamp::in(timeout_s);
    while (!deadline.is_in_past()) {
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      for (const auto& record : snapshot.leader_windows_observed) {
        if (record.node_idx == node_idx && record.instance_idx == instance_idx && record.start_slot == start_slot) {
          co_return td::Unit{};
        }
      }
      co_await td::actor::coro_sleep(td::Timestamp::in(0.05));
    }
    co_return td::Status::Error(PSTRING() << "timeout waiting for leader window " << start_slot << " on validator #"
                                          << node_idx << "." << instance_idx);
  }

  bool skip_finalize_for_test_case() const {
    return TEST_CASE == TestCase::StandstillRebroadcastContents ||
           TEST_CASE == TestCase::CandidateResolutionRecovery ||
           TEST_CASE == TestCase::BadCandidateResolutionResponderDoesNotStopCatchup ||
           TEST_CASE == TestCase::InvalidCandidateSignatureDoesNotResolve ||
           TEST_CASE == TestCase::MalformedNotarCertificateDoesNotResolve ||
           TEST_CASE == TestCase::LongStallClearsSkippedSlotsThenHonestLeaderFinalizes ||
           TEST_CASE == TestCase::MissingAncestorChainStillResolves ||
           TEST_CASE == TestCase::SkipTimeoutUsesLastFinalization ||
           TEST_CASE == TestCase::LaterSlotWaitsForEarlierNotarProof ||
           TEST_CASE == TestCase::LaterSlotWaitsForGapSkipProof ||
           TEST_CASE == TestCase::EquivocatingLeaderDoesNotCreateTwoNotarsOrFinals ||
           TEST_CASE == TestCase::MultiSlotEquivocationWindowPreservesSafety ||
           TEST_CASE == TestCase::StandstillFloodStillRecovers ||
           TEST_CASE == TestCase::StandstillWithoutCertificateRebroadcastRequiresRecovery;
  }

  td::actor::Task<> run_test_case_scenario() {
    if (TEST_CASE == TestCase::SilentLeaderWindowSkipsButNextHonestWindowFinalizes) {
      // Covers simplex_docs.md Rule 6 and the honest-window liveness claim:
      // after one leader goes silent, its window should skip and later honest windows should still finalize.
      co_await wait_for_finalization_on(0, 0, 5.0);
      co_await clear_traces();
      co_await stop_instance(0, 0);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::SilentValidatorDoesNotStopProgress) {
      // Covers simplex_docs.md system model under f < W/3:
      // one silent validator must not stop quorum certificates and later finalization.
      co_await wait_for_finalization_on(0, 0, 5.0);
      co_await clear_traces();
      co_await stop_instance(3, 0);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::BadCandidateResolutionResponderDoesNotStopCatchup) {
      // Covers simplex_docs.md Rule 2 under a Byzantine peer:
      // a malformed candidate response must be ignored, later honest responses must still resolve
      // the target candidate, and the live network should keep finalizing meanwhile.
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
        co_return td::Status::Error(
            "could not find a candidate notarized by the honest serving peer for the Byzantine Rule 2 scenario");
      }
      co_await set_candidate_resolution_target_for_test(*target_id);

      co_await stop_instance(0, 0);
      co_await clear_instance_db(0, 0);
      co_await clear_instance_candidate_storage(0, 0);
      co_await set_force_candidate_request_destination_for_test(0, 0, 1);
      co_await set_malformed_candidate_responses_for_test(0, 0, 1, 1000);
      co_await clear_traces();
      start_instance(0, 0);
      co_await td::actor::coro_sleep(td::Timestamp::in(0.2));
      resolve_candidate_for_test(0, 0, *target_id).start().detach();
      co_await td::actor::coro_sleep(td::Timestamp::in(1.5));
      co_await set_force_candidate_request_destination_for_test(0, 0, 3);
      resolve_candidate_for_test(0, 0, *target_id).start().detach();
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::InvalidCandidateSignatureDoesNotResolve) {
      // Covers A5 from the adversarial plan:
      // a candidate-resolution responder returns the right candidate id but with corrupted candidate
      // bytes/signature, which must not resolve until a later honest response arrives.
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
        co_return td::Status::Error(
            "could not find a candidate notarized by the honest serving peer for the invalid-signature Rule 2 scenario");
      }
      co_await set_candidate_resolution_target_for_test(*target_id);

      co_await stop_instance(0, 0);
      co_await clear_instance_db(0, 0);
      co_await clear_instance_candidate_storage(0, 0);
      co_await set_force_candidate_request_destination_for_test(0, 0, 1);
      co_await set_malformed_candidate_responses_for_test(
          0, 0, 1, 1000, TestMessageFilters::MalformedCandidateResponseKind::CorruptCandidate);
      co_await clear_traces();
      start_instance(0, 0);
      co_await td::actor::coro_sleep(td::Timestamp::in(0.2));
      resolve_candidate_for_test(0, 0, *target_id).start().detach();
      {
        td::Timestamp deadline = td::Timestamp::in(2.0);
        bool saw_malformed_response = false;
        while (!deadline.is_in_past()) {
          snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
          for (const auto& record : snapshot.malformed_candidate_responses) {
            if (record.requester_node_idx == 0 && record.requester_instance_idx == 0 &&
                record.responder_node_idx == 1 && record.id == *target_id) {
              saw_malformed_response = true;
              break;
            }
          }
          if (saw_malformed_response) {
            break;
          }
          co_await td::actor::coro_sleep(td::Timestamp::in(0.05));
        }
        if (!saw_malformed_response) {
          co_return td::Status::Error("malicious responder never returned a corrupted candidate response");
        }
      }
      co_await set_force_candidate_request_destination_for_test(0, 0, 3);
      resolve_candidate_for_test(0, 0, *target_id).start().detach();
      co_await td::actor::coro_sleep(td::Timestamp::in(0.4));
      resolve_candidate_for_test(0, 0, *target_id).start().detach();
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::MalformedNotarCertificateDoesNotResolve) {
      // Covers A9 from the adversarial plan:
      // a candidate-resolution responder returns the right candidate body with a corrupted notar
      // certificate, which must not resolve until a later honest response arrives.
      co_await wait_for_finalization_on(1, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 1 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      td::uint32 attacked_slot = highest_finalized_slot + static_cast<td::uint32>(N_NODES) + 1;
      while (attacked_slot % N_NODES == 1) {
        ++attacked_slot;
      }

      co_await set_drop_notar_votes_for_test(0, attacked_slot, attacked_slot + 1);
      co_await set_drop_notar_certs_for_test(0, attacked_slot, attacked_slot + 1);
      co_await clear_traces();

      std::optional<CandidateId> target_id;
      td::Timestamp deadline = td::Timestamp::in(5.0);
      while (!deadline.is_in_past()) {
        snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
        for (const auto& record : snapshot.candidate_deliveries) {
          if (record.dst_node_idx == 0 && record.dst_instance_idx == 0 && record.candidate_id.slot == attacked_slot) {
            target_id = record.candidate_id;
            break;
          }
        }
        bool saw_notar_elsewhere = false;
        for (const auto& record : snapshot.protocol_certificates) {
          if (auto* notar = std::get_if<simplex::NotarizeVote>(&record.vote.vote);
              notar != nullptr && notar->id.slot == attacked_slot &&
              (!record.dst_node_idx.has_value() || *record.dst_node_idx != 0)) {
            saw_notar_elsewhere = true;
            break;
          }
        }
        if (target_id.has_value() && saw_notar_elsewhere) {
          break;
        }
        co_await td::actor::coro_sleep(td::Timestamp::in(0.05));
      }
      if (!target_id.has_value()) {
        co_return td::Status::Error(PSTRING() << "could not find a delivered candidate for malformed-notar slot "
                                              << attacked_slot);
      }
      co_await set_candidate_resolution_target_for_test(*target_id);
      co_await set_force_candidate_request_destination_for_test(0, 0, 1);
      co_await set_malformed_candidate_responses_for_test(
          0, 0, 1, 1000, TestMessageFilters::MalformedCandidateResponseKind::CorruptNotar);
      co_await clear_traces();
      resolve_candidate_for_test(0, 0, *target_id).start().detach();
      {
        td::Timestamp deadline = td::Timestamp::in(2.0);
        bool saw_malformed_response = false;
        while (!deadline.is_in_past()) {
          snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
          for (const auto& record : snapshot.malformed_candidate_responses) {
            if (record.requester_node_idx == 0 && record.requester_instance_idx == 0 &&
                record.responder_node_idx == 1 && record.id == *target_id) {
              saw_malformed_response = true;
              break;
            }
          }
          if (saw_malformed_response) {
            break;
          }
          co_await td::actor::coro_sleep(td::Timestamp::in(0.05));
        }
        if (!saw_malformed_response) {
          co_return td::Status::Error("malicious responder never returned a corrupted notar response");
        }
      }
      co_await set_force_candidate_request_destination_for_test(0, 0, 3);
      resolve_candidate_for_test(0, 0, *target_id).start().detach();
      co_await td::actor::coro_sleep(td::Timestamp::in(0.4));
      resolve_candidate_for_test(0, 0, *target_id).start().detach();
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::LongStallClearsSkippedSlotsThenHonestLeaderFinalizes) {
      // Covers C10 and C11 from the adversarial plan:
      // drop every candidate across a long slot range so the range clears by Skip, then require
      // later honest finalization after the stall ends.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 0 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      td::uint32 start_slot = highest_finalized_slot + static_cast<td::uint32>(N_NODES) + 1;
      size_t slot_count = 3;
      {
        std::scoped_lock lock(test_expectations_mutex);
        test_expectations.long_stall_start_slot = start_slot;
        test_expectations.long_stall_slot_count = slot_count;
      }
      std::vector<size_t> dropped_nodes;
      for (size_t node_idx = 0; node_idx < N_NODES; ++node_idx) {
        dropped_nodes.push_back(node_idx);
      }
      co_await clear_traces();
      co_await set_drop_candidates_for_test(dropped_nodes, start_slot,
                                            start_slot + static_cast<td::uint32>(slot_count));
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::InvalidFullCandidateDoesNotNotarize) {
      // Covers A6 from the adversarial plan:
      // let the real leader produce its normal candidate for an attacked slot, but make manager
      // validation deterministically reject that slot on all honest validators. Honest nodes must
      // not notarize the resulting full candidate.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);

      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 0 && record.instance_idx == 0 && record.id.slot > highest_finalized_slot) {
          highest_finalized_slot = record.id.slot;
        }
      }
      td::uint32 attacked_slot = highest_finalized_slot + static_cast<td::uint32>(N_NODES) + 1;
      if (attacked_slot % N_NODES == 0) {
        ++attacked_slot;
      }
      size_t leader_node_idx = static_cast<size_t>(attacked_slot % N_NODES);
      {
        std::scoped_lock lock(test_expectations_mutex);
        test_expectations.adversarial_nodes = {leader_node_idx};
        test_expectations.invalid_full_candidate_slot = attacked_slot;
      }
      co_await td::actor::coro_sleep(td::Timestamp::in(0.4));
      co_await clear_traces();
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::SkipTimeoutCapPlateausUnderStall) {
      // Covers D5 from the adversarial plan in a scaled configuration:
      // keep three consecutive leaders silent while still staying below one-third adversarial weight,
      // then verify that skip delays on validator #0.0 stop growing once they hit the configured cap.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 0 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      td::uint32 start_slot = highest_finalized_slot + static_cast<td::uint32>(N_NODES) + 1;
      while (start_slot % N_NODES == 0 || (start_slot + 1) % N_NODES == 0 || (start_slot + 2) % N_NODES == 0) {
        ++start_slot;
      }
      std::vector<size_t> silent_nodes = {static_cast<size_t>(start_slot % N_NODES),
                                          static_cast<size_t>((start_slot + 1) % N_NODES),
                                          static_cast<size_t>((start_slot + 2) % N_NODES)};
      {
        std::scoped_lock lock(test_expectations_mutex);
        test_expectations.adversarial_nodes = silent_nodes;
        test_expectations.skip_timeout_cap_start_slot = start_slot;
        test_expectations.skip_timeout_cap_slot_count = silent_nodes.size();
      }
      co_await clear_traces();
      for (size_t node_idx : silent_nodes) {
        co_await stop_instance(node_idx, 0);
      }
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::CountThresholdMismatchPreservesSafety) {
      // Covers D6 from the adversarial plan under the fake-overlay harness:
      // the attacked slot reaches a count-majority of mostly low-weight validators, but not enough
      // weight for quorum, so it must still fail safely and later honest progress must continue.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 0 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      td::uint32 attacked_slot = highest_finalized_slot + 1;
      while (attacked_slot % N_NODES != 3) {
        ++attacked_slot;
      }
      {
        std::scoped_lock lock(test_expectations_mutex);
        test_expectations.count_weight_mismatch_slot = attacked_slot;
      }
      co_await clear_traces();
      co_await set_drop_candidates_for_test({1, 2}, attacked_slot, attacked_slot + 1);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::StandstillFloodStillRecovers) {
      // Covers D7 from the adversarial plan:
      // keep one Byzantine validator silent, remove enough additional honest validators to prolong
      // standstill rebroadcasts, then reconnect quorum in stages and require later recovery.
      co_await wait_for_finalization_on(0, 0, 5.0);
      co_await clear_traces();
      co_await set_instance_network_disabled_for_test(1, 0, true);
      co_await td::actor::coro_sleep(td::Timestamp::in(0.2));
      co_await set_instance_network_disabled_for_test(2, 0, true);
      co_await set_instance_network_disabled_for_test(3, 0, true);
      co_await td::actor::coro_sleep(td::Timestamp::in(1.6));
      co_await set_instance_network_disabled_for_test(2, 0, false);
      co_await td::actor::coro_sleep(td::Timestamp::in(0.6));
      co_await set_instance_network_disabled_for_test(3, 0, false);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::SeveralWindowsFullySkippedThenProgressResumes) {
      // Covers Kernel-28's explicit fully-skipped-window regression:
      // three consecutive silent leaders should yield three skipped slots, then later honest
      // windows should resume finalization.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 0 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      td::uint32 first_silent_slot = highest_finalized_slot + 2 * static_cast<td::uint32>(N_NODES);
      std::vector<size_t> silent_nodes = {static_cast<size_t>(first_silent_slot % N_NODES),
                                          static_cast<size_t>((first_silent_slot + 1) % N_NODES),
                                          static_cast<size_t>((first_silent_slot + 2) % N_NODES)};
      co_await set_consecutive_silent_leaders_expectations_for_test(silent_nodes, first_silent_slot, 3);
      co_await wait_for_finalization_past_slot_on(1, 0, first_silent_slot - static_cast<td::uint32>(N_NODES) + 2,
                                                  5.0);
      co_await clear_traces();
      for (size_t node_idx : silent_nodes) {
        co_await stop_instance(node_idx, 0);
      }
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::AdjacentWindowForkRollsBackAfterLateAlternateFinalization) {
      // Covers Kernel-28's adjacent-window rollback regression:
      // keep one slot forked across adjacent single-slot windows, then require later honest
      // finalization to collapse the fork without any forbidden late Final votes on the fork.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
      }
      td::uint32 attacked_slot = highest_finalized_slot + static_cast<td::uint32>(N_NODES) + 1;
      while (attacked_slot % N_NODES != 0) {
        ++attacked_slot;
      }
      co_await set_forked_slot_for_test(attacked_slot);
      co_await clear_traces();
      for (size_t dst_node_idx : {1UL, 2UL, 3UL}) {
        co_await set_drop_notar_votes_for_test(dst_node_idx, attacked_slot, attacked_slot + 1);
        co_await set_drop_notar_certs_for_test(dst_node_idx, attacked_slot, attacked_slot + 1);
      }
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::RestartAfterEightNonMcFinalizedBlocksReacceptsPendingCandidates) {
      // Covers Kernel-28's restart-after-eight-non-MC-finalized-blocks regression:
      // validator #0 misses eight shardchain blocks while peers keep finalizing, then restarts just
      // before empty-candidate mode and must reaccept the pending full-block chain once empties land.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 0 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      {
        std::scoped_lock lock(test_expectations_mutex);
        test_expectations.restart_reaccept_stop_slot = highest_finalized_slot;
      }

      co_await stop_instance(0, 0);
      co_await clear_traces();
      co_await wait_for_accepted_block_seqno_on(1, 0, highest_finalized_slot + 8, 10.0);
      co_await set_force_candidate_request_destination_for_test(0, 0, 1);
      start_instance(0, 0);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::StandstillWithoutCertificateRebroadcastRequiresRecovery) {
      // Covers Kernel-28's explicit standstill-recovery regression:
      // validator #0 misses normal cert traffic for a slot, then only catches up after standstill
      // rebroadcast once the certificate filter is lifted.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 0 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      td::uint32 attacked_slot = highest_finalized_slot + static_cast<td::uint32>(N_NODES);
      while (attacked_slot % N_NODES != 1) {
        ++attacked_slot;
      }
      td::uint32 blocked_until_slot = attacked_slot + 8;
      for (auto kind : {TestMessageFilters::ProtocolKind::NotarVote, TestMessageFilters::ProtocolKind::FinalVote,
                        TestMessageFilters::ProtocolKind::NotarCert, TestMessageFilters::ProtocolKind::FinalCert}) {
        for (size_t src_node_idx = 1; src_node_idx < N_NODES; ++src_node_idx) {
          co_await set_selective_protocol_drop_for_test(src_node_idx, 0, {0}, attacked_slot, blocked_until_slot, kind);
        }
      }
      co_await clear_traces();
      co_await wait_for_finalization_past_slot_on(1, 0, attacked_slot, 5.0);
      co_await stop_instance(2, 0);
      co_await stop_instance(3, 0);
      co_await td::actor::coro_sleep(td::Timestamp::in(0.8));
      {
        std::scoped_lock lock(test_expectations_mutex);
        test_expectations.standstill_recovery_slot = attacked_slot;
        test_expectations.standstill_recovery_release_ts = td::Time::now_unadjusted();
      }
      co_await clear_test_message_filters();
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::GoodCandidateWithWrongSlotParentHashDoesNotResolve) {
      // Covers Kernel-28's wrong-slot-parent-hash regression:
      // inject an otherwise well-formed full candidate whose parent id hash cannot match the slot
      // chain, and require the attacked slot to clear by Skip instead of Notar.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 0 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      td::uint32 attacked_slot = highest_finalized_slot + static_cast<td::uint32>(N_NODES) + 1;
      while (attacked_slot % N_NODES != 1) {
        ++attacked_slot;
      }
      co_await clear_traces();
      co_await stop_instance(1, 0);
      ParentId wrong_parent =
          CandidateId{attacked_slot - 1, from_hex("1111111122222222333333334444444455555555666666667777777788888888")};
      auto candidate =
          make_synthetic_full_candidate_for_test(attacked_slot, wrong_parent, PeerValidatorId{1}, attacked_slot - 1,
                                                 0xC0FFEE00ULL + attacked_slot);
      {
        std::scoped_lock lock(test_expectations_mutex);
        test_expectations.wrong_parent_hash_candidate_id = candidate->id;
      }
      co_await inject_candidate_for_test(1, {0}, candidate);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::MissingAncestorChainStillResolves) {
      // Covers C8 from the adversarial plan:
      // restart a lagging node on a head candidate whose ancestor chain is also missing locally, force
      // early requests to a malicious peer, then verify that honest responses still let the node recurse
      // through missing ancestors and catch up.
      co_await wait_for_finalization_on(1, 0, 5.0);
      co_await td::actor::coro_sleep(td::Timestamp::in(1.0));
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      std::optional<CandidateId> target_id;
      size_t honest_responder = 3;
      for (auto it = snapshot.protocol_votes.rbegin(); it != snapshot.protocol_votes.rend(); ++it) {
        if ((it->src_node_idx != 2 && it->src_node_idx != 3) || it->src_instance_idx != 0) {
          continue;
        }
        if (auto* notar = std::get_if<simplex::NotarizeVote>(&it->vote.vote); notar != nullptr && notar->id.slot >= 2) {
          target_id = notar->id;
          honest_responder = it->src_node_idx;
          break;
        }
      }
      if (!target_id.has_value()) {
        co_return td::Status::Error("could not find a head candidate far enough ahead to require ancestor resolution");
      }
      co_await set_candidate_resolution_target_for_test(*target_id);

      co_await stop_instance(0, 0);
      co_await clear_instance_db(0, 0);
      co_await clear_instance_candidate_storage(0, 0);
      co_await set_force_candidate_request_destination_for_test(0, 0, 1);
      co_await set_malformed_candidate_responses_for_test(0, 0, 1, 1000);
      co_await clear_traces();
      start_instance(0, 0);
      co_await td::actor::coro_sleep(td::Timestamp::in(0.2));
      resolve_candidate_for_test(0, 0, *target_id).start().detach();
      co_await td::actor::coro_sleep(td::Timestamp::in(1.5));
      co_await set_force_candidate_request_destination_for_test(0, 0, honest_responder);
      resolve_candidate_for_test(0, 0, *target_id).start().detach();
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::LaterSlotWaitsForGapSkipProof) {
      // Covers A7 from the adversarial plan:
      // leader #1 eagerly produces a later-slot candidate, but the receiver must wait until the
      // intermediate slot is cleared by Skip before voting Notar on the later slot.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 0 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      td::uint32 attacked_window = highest_finalized_slot / SLOTS_PER_LEADER_WINDOW + 1;
      while (attacked_window % N_NODES != 1) {
        ++attacked_window;
      }
      td::uint32 start_slot = attacked_window * SLOTS_PER_LEADER_WINDOW;
      td::uint32 gap_slot = start_slot + 1;
      co_await set_gap_skip_slot_for_test(gap_slot);
      co_await clear_traces();
      co_await set_drop_candidates_for_test({0, 2, 3}, gap_slot, gap_slot + 1);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::ConsecutiveSilentLeadersThenProgress) {
      // Covers simplex_docs.md Rule 6 and liveness under consecutive faults:
      // two upcoming silent leaders with total weight still below W/3 should yield two skipped slots,
      // then a later honest leader should finalize again.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 0 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      td::uint32 first_silent_slot = highest_finalized_slot + static_cast<td::uint32>(N_NODES);
      std::vector<size_t> silent_nodes = {
          static_cast<size_t>(first_silent_slot % N_NODES),
          static_cast<size_t>((first_silent_slot + 1) % N_NODES),
      };
      co_await set_consecutive_silent_leaders_expectations_for_test(silent_nodes, first_silent_slot, 2);
      co_await clear_traces();
      co_await stop_instance(silent_nodes[0], 0);
      co_await stop_instance(silent_nodes[1], 0);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::LeaderCandidateToTooLittleWeightSkipsThenProgress) {
      // Covers simplex_docs.md Rule 4 and Rule 6 under a Byzantine leader:
      // the leader reveals its candidate to only one peer, so too little weight can ever notarize it;
      // the slot should skip, and later honest slots should still finalize.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 0 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      td::uint32 attacked_slot = highest_finalized_slot + static_cast<td::uint32>(N_NODES) + 1;
      while (attacked_slot % N_NODES != 1) {
        ++attacked_slot;
      }
      size_t leader_node_idx = static_cast<size_t>(attacked_slot % N_NODES);
      size_t allowed_recipient = (leader_node_idx + 1) % N_NODES;
      std::vector<size_t> dropped_nodes;
      for (size_t node_idx = 0; node_idx < N_NODES; ++node_idx) {
        if (node_idx != leader_node_idx && node_idx != allowed_recipient) {
          dropped_nodes.push_back(node_idx);
        }
      }
      co_await set_candidate_starvation_expectations_for_test({leader_node_idx}, attacked_slot, dropped_nodes);
      co_await clear_traces();
      co_await set_drop_candidates_for_test(dropped_nodes, attacked_slot, attacked_slot + 1);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::DuplicateInstanceSameKeySplitBrainStillProgresses) {
      // Covers the adversarial plan's split-brain same-key case:
      // keep two live instances for validator #0, isolate one instance's network temporarily, then
      // require later honest finalization after it rejoins.
      co_await wait_for_finalization_on(0, 0, 5.0);
      co_await clear_traces();
      {
        std::scoped_lock lock(test_expectations_mutex);
        test_expectations.adversarial_nodes = {0};
      }
      co_await set_instance_network_disabled_for_test(0, 1, true);
      co_await td::actor::coro_sleep(td::Timestamp::in(1.5));
      co_await set_instance_network_disabled_for_test(0, 1, false);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::SleepingValidatorRecoversAfterHonestProgress) {
      // Covers the adversarial plan's sleeping validator recovery case:
      // isolate validator #3.0 for a while, require honest progress during the sleep, then require
      // finalization catch-up after re-enable.
      co_await wait_for_finalization_on(0, 0, 5.0);
      co_await clear_traces();
      co_await set_instance_network_disabled_for_test(3, 0, true);
      co_await td::actor::coro_sleep(td::Timestamp::in(1.5));
      co_await set_instance_network_disabled_for_test(3, 0, false);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::StandstillWithByzantineWithholdingStillRecovers) {
      // Covers the adversarial plan's standstill recovery case:
      // node #1 withholds for the whole run, node #2 disappears long enough to break quorum, and
      // honest nodes must recover finalization after node #2 returns.
      co_await wait_for_finalization_on(0, 0, 5.0);
      co_await clear_traces();
      co_await set_instance_network_disabled_for_test(1, 0, true);
      co_await td::actor::coro_sleep(td::Timestamp::in(0.2));
      co_await set_instance_network_disabled_for_test(2, 0, true);
      co_await td::actor::coro_sleep(td::Timestamp::in(1.8));
      co_await set_instance_network_disabled_for_test(2, 0, false);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::EmptyCandidatesWithSilentValidatorStillFinalize) {
      // Covers the adversarial plan's empty-candidate Byzantine scheduling case:
      // on masterchain, even with one silent validator, empty candidates should still finalize and
      // later slots should continue progressing.
      co_await wait_for_finalization_on(0, 0, 5.0);
      co_await clear_traces();
      co_await stop_instance(3, 0);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::NoNotarObservedSkipWins) {
      // Covers the adversarial plan's no-notarization case:
      // a Byzantine leader reveals the slot to nobody else, so no honest validator observes a notarization
      // and the slot must clear by Skip instead.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 0 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      td::uint32 attacked_slot = highest_finalized_slot + static_cast<td::uint32>(N_NODES);
      while (attacked_slot % N_NODES != 1) {
        ++attacked_slot;
      }
      size_t leader_node_idx = static_cast<size_t>(attacked_slot % N_NODES);
      std::vector<size_t> dropped_nodes;
      for (size_t node_idx = 0; node_idx < N_NODES; ++node_idx) {
        if (node_idx != leader_node_idx) {
          dropped_nodes.push_back(node_idx);
        }
      }
      co_await set_no_notarization_expectations_for_test({leader_node_idx}, attacked_slot);
      co_await clear_traces();
      co_await set_drop_candidates_for_test(dropped_nodes, attacked_slot, attacked_slot + 1);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::LaterSlotWaitsForEarlierNotarProof) {
      // Covers A8, D2, and D3 from the adversarial plan:
      // with eager whole-window production, validator #0.0 should see a later-slot candidate before
      // the parent slot's notar proof, but must not vote Notar for the later slot until that proof arrives.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 0 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      td::uint32 attacked_window = highest_finalized_slot / SLOTS_PER_LEADER_WINDOW + 1;
      while (attacked_window % N_NODES == 0) {
        ++attacked_window;
      }
      td::uint32 parent_slot = attacked_window * SLOTS_PER_LEADER_WINDOW;
      co_await set_parent_gating_slot_for_test(parent_slot);
      co_await clear_traces();
      co_await set_drop_notar_votes_for_test(0, parent_slot, parent_slot + 1);
      co_await wait_for_leader_window_observed_on(0, 0, parent_slot, 5.0);
      co_await td::actor::coro_sleep(td::Timestamp::in(0.8));
      co_await clear_test_message_filters();
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::ReplayStaleCertificateDoesNotDisturbProgress) {
      // Covers C13 from the adversarial plan:
      // after later slots finalize, a Byzantine validator replays an old finalization certificate and
      // honest nodes must ignore it while continuing to make progress.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      std::optional<td::uint32> stale_slot;
      std::string stale_certificate;
      for (const auto& record : snapshot.protocol_certificates) {
        if (auto* final = std::get_if<simplex::FinalizeVote>(&record.vote.vote)) {
          if (!stale_slot.has_value() || final->id.slot < *stale_slot) {
            stale_slot = final->id.slot;
            stale_certificate = record.raw_message;
          }
        }
      }
      if (!stale_slot.has_value() || stale_certificate.empty()) {
        co_return td::Status::Error("could not find a stale final certificate to replay");
      }
      co_await set_replayed_stale_slot_for_test(*stale_slot);
      co_await clear_traces();
      co_await wait_for_finalization_past_slot_on(0, 0, *stale_slot, 5.0);
      co_await clear_traces();
      co_await inject_protocol_message_for_test(1, {0, 2, 3}, std::move(stale_certificate));
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::NotarizedAndSkippedSlotCoexistWithoutFinalization) {
      // Covers B1 from the adversarial plan:
      // let one node form a notarization certificate for a slot while the rest never learn that
      // cert and later clear the same slot by Skip; the slot must not finalize.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
      }
      td::uint32 attacked_slot = highest_finalized_slot + static_cast<td::uint32>(N_NODES) + 1;
      while (attacked_slot % N_NODES != 0) {
        ++attacked_slot;
      }
      co_await set_forked_slot_for_test(attacked_slot);
      co_await clear_traces();
      for (size_t dst_node_idx : {1UL, 2UL, 3UL}) {
        co_await set_drop_notar_votes_for_test(dst_node_idx, attacked_slot, attacked_slot + 1);
        co_await set_drop_notar_certs_for_test(dst_node_idx, attacked_slot, attacked_slot + 1);
      }
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::FinalAndSkipRacePreservesLocalDiscipline) {
      // Covers A3 and A4 from the adversarial plan:
      // one honest validator should get far enough to vote Final on the slot while others reach Skip,
      // but the slot must still avoid a finalization certificate and no honest node may cast both.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
      }
      td::uint32 attacked_slot = highest_finalized_slot + static_cast<td::uint32>(N_NODES) + 1;
      while (attacked_slot % N_NODES != 0) {
        ++attacked_slot;
      }
      co_await set_forked_slot_for_test(attacked_slot);
      co_await clear_traces();
      for (size_t dst_node_idx : {1UL, 2UL, 3UL}) {
        co_await set_drop_notar_votes_for_test(dst_node_idx, attacked_slot, attacked_slot + 1);
        co_await set_drop_notar_certs_for_test(dst_node_idx, attacked_slot, attacked_slot + 1);
      }
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::ForkDescendantsCollapseAfterLaterFinalization) {
      // Covers B3 from the adversarial plan:
      // keep the forked slot notarized only on node #0 while the rest skip it, then let later honest
      // windows continue until a later finalization collapses the fork.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
      }
      td::uint32 attacked_slot = highest_finalized_slot + static_cast<td::uint32>(N_NODES) + 1;
      while (attacked_slot % N_NODES != 0) {
        ++attacked_slot;
      }
      co_await set_forked_slot_for_test(attacked_slot);
      co_await clear_traces();
      for (size_t dst_node_idx : {1UL, 2UL, 3UL}) {
        co_await set_drop_notar_votes_for_test(dst_node_idx, attacked_slot, attacked_slot + 1);
        co_await set_drop_notar_certs_for_test(dst_node_idx, attacked_slot, attacked_slot + 1);
      }
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::EquivocatingLeaderDoesNotCreateTwoNotarsOrFinals) {
      // Covers A1 and A2 from the adversarial plan:
      // two same-key leader instances see different honest subsets, so if an honest overlap validator ever
      // double-votes, the slot could wrongly produce two certifiable branches.
      co_await wait_for_finalization_on(1, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 1 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      td::uint32 attacked_slot = highest_finalized_slot + 1;
      while (attacked_slot % N_NODES != 0) {
        ++attacked_slot;
      }
      for (auto kind : {TestMessageFilters::ProtocolKind::NotarVote, TestMessageFilters::ProtocolKind::FinalVote,
                        TestMessageFilters::ProtocolKind::NotarCert, TestMessageFilters::ProtocolKind::FinalCert}) {
        co_await set_selective_protocol_drop_for_test(0, 0, {3}, attacked_slot, attacked_slot + 1, kind);
        co_await set_selective_protocol_drop_for_test(0, 1, {1}, attacked_slot, attacked_slot + 1, kind);
      }
      co_await set_selective_candidate_drop_for_test(0, 0, {3}, attacked_slot, attacked_slot + 1);
      co_await set_selective_candidate_drop_for_test(0, 1, {1}, attacked_slot, attacked_slot + 1);

      co_await clear_traces();
      td::Timestamp deadline = td::Timestamp::in(5.0);
      while (!deadline.is_in_past()) {
        snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
        std::map<size_t, CandidateId> candidates_by_instance;
        for (const auto& record : snapshot.candidates_generated) {
          if (record.node_idx == 0 && record.candidate_id.slot == attacked_slot) {
            candidates_by_instance.emplace(record.instance_idx, record.candidate_id);
          }
        }
        if (candidates_by_instance.size() == 2 && candidates_by_instance.at(0) != candidates_by_instance.at(1)) {
          co_await set_equivocation_expectations_for_test(
              attacked_slot, {candidates_by_instance.at(0), candidates_by_instance.at(1)});
          co_await td::actor::coro_sleep(td::Timestamp::in(0.4));
          co_await inject_vote_for_test(0, {1, 2, 3}, simplex::FinalizeVote{candidates_by_instance.at(0)});
          co_await inject_vote_for_test(0, {1, 2, 3}, simplex::FinalizeVote{candidates_by_instance.at(1)});
          co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
          co_return td::Unit{};
        }
        co_await td::actor::coro_sleep(td::Timestamp::in(0.05));
      }
      co_return td::Status::Error(PSTRING() << "timeout waiting for two distinct same-key candidates on slot "
                                            << attacked_slot);
    }
    if (TEST_CASE == TestCase::MultiSlotEquivocationWindowPreservesSafety) {
      // Covers B2 and D1 from the adversarial plan:
      // keep two live same-key leader instances across a multi-slot window, partition their candidate and
      // protocol traffic to different honest subsets, and require later honest finalization after the window.
      co_await wait_for_finalization_on(1, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 1 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      td::uint32 attacked_window = highest_finalized_slot / SLOTS_PER_LEADER_WINDOW + 1;
      while (attacked_window % N_NODES != 0) {
        ++attacked_window;
      }
      td::uint32 start_slot = attacked_window * SLOTS_PER_LEADER_WINDOW;
      td::uint32 end_slot = start_slot + SLOTS_PER_LEADER_WINDOW;
      {
        std::scoped_lock lock(test_expectations_mutex);
        test_expectations.multi_slot_equivocation_start_slot = start_slot;
        test_expectations.multi_slot_equivocation_slot_count = SLOTS_PER_LEADER_WINDOW;
      }
      for (auto kind : {TestMessageFilters::ProtocolKind::NotarVote, TestMessageFilters::ProtocolKind::FinalVote,
                        TestMessageFilters::ProtocolKind::NotarCert, TestMessageFilters::ProtocolKind::FinalCert}) {
        co_await set_selective_protocol_drop_for_test(0, 0, {3}, start_slot, end_slot, kind);
        co_await set_selective_protocol_drop_for_test(0, 1, {1}, start_slot, end_slot, kind);
      }
      co_await set_selective_candidate_drop_for_test(0, 0, {3}, start_slot, end_slot);
      co_await set_selective_candidate_drop_for_test(0, 1, {1}, start_slot, end_slot);
      co_await clear_traces();
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
    if (TEST_CASE == TestCase::DelayedFinalizationObservationPreservesPrefix) {
      // Covers B4 from the adversarial plan:
      // node #1 observes a finalized suffix later than node #2 because it is temporarily isolated,
      // but the resulting finalized suffixes must stay prefix-consistent.
      co_await wait_for_finalization_on(2, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      td::uint32 highest_finalized_slot = 0;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 2 && record.instance_idx == 0) {
          highest_finalized_slot = std::max(highest_finalized_slot, record.id.slot);
        }
      }
      td::uint32 from_slot = highest_finalized_slot + 1;
      co_await set_delayed_finalization_expectations_for_test(from_slot, 1);
      co_await set_instance_network_disabled_for_test(1, 0, true);
      co_await clear_traces();
      co_await td::actor::coro_sleep(td::Timestamp::in(1.0));
      co_await set_delayed_finalization_release_ts_for_test(td::Time::now_unadjusted());
      co_await set_instance_network_disabled_for_test(1, 0, false);
      co_await td::actor::coro_sleep(td::Timestamp::in(DURATION));
      co_return td::Unit{};
    }
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
    if (TEST_CASE == TestCase::SkipTimeoutUsesLastFinalization) {
      // Drives simplex_docs.md Rule 6 and the §4.2 known implementation deviation:
      // freeze validator #0.0's last observed finalization after one finalized window, let the
      // next window clear normally, then withhold all candidates in the following window and check
      // whether Skip is delayed according to the last reached finalization rather than reset.
      co_await wait_for_finalization_on(0, 0, 5.0);
      auto snapshot = co_await td::actor::ask(trace_sink_, &TestTraceSink::snapshot);
      std::optional<td::uint32> finalized_slot;
      for (const auto& record : snapshot.finalizations_observed) {
        if (record.node_idx == 0 && record.instance_idx == 0) {
          finalized_slot = std::max(finalized_slot.value_or(0), record.id.slot);
        }
      }
      if (!finalized_slot.has_value()) {
        co_return td::Status::Error("Rule 6 scenario did not observe any finalized slot on validator #0.0");
      }

      td::uint32 last_finalized_window = *finalized_slot / SLOTS_PER_LEADER_WINDOW;
      td::uint32 clear_without_skip_start_slot = (last_finalized_window + 1) * SLOTS_PER_LEADER_WINDOW;
      td::uint32 stalled_window = last_finalized_window + 2;
      td::uint32 stalled_window_start_slot = stalled_window * SLOTS_PER_LEADER_WINDOW;
      co_await set_skip_timeout_expectations_for_test(last_finalized_window, stalled_window);
      co_await set_drop_final_certs_for_test(0, clear_without_skip_start_slot);
      co_await clear_traces();
      co_await wait_for_leader_window_observed_on(0, 0, clear_without_skip_start_slot, 5.0);
      co_await set_drop_candidates_for_test(0, stalled_window_start_slot,
                                            stalled_window_start_slot + SLOTS_PER_LEADER_WINDOW);
      co_await set_drop_notar_certs_for_test(0, stalled_window_start_slot,
                                             stalled_window_start_slot + SLOTS_PER_LEADER_WINDOW);
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

    if (!NODE_WEIGHTS_OVERRIDE.empty() && NODE_WEIGHTS_OVERRIDE.size() != N_NODES) {
      co_return td::Status::Error(PSTRING() << "configured " << NODE_WEIGHTS_OVERRIDE.size()
                                            << " node weights for " << N_NODES << " validators");
    }

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

      node.weight = configured_node_weight(i);

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
        .consensus = NewConsensusConfig::Simplex{
            .slots_per_leader_window = SLOTS_PER_LEADER_WINDOW,
            .first_block_timeout_ms = static_cast<td::uint32>(FIRST_BLOCK_TIMEOUT_S * 1000.0),
        }};
    bus->simplex_config = bus->config.consensus.get<NewConsensusConfig::Simplex>();
    bus->session_id = SESSION_ID;
    bus->cc_seqno = CC_SEQNO;
    bus->validator_set_hash = validator_set_->get_validator_set_hash();
    bus->populate_collator_schedule();
    bus->first_block_timeout_multipler = FIRST_BLOCK_TIMEOUT_MULTIPLIER;
    bus->first_block_max_timeout_s = FIRST_BLOCK_MAX_TIMEOUT_S;
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
  p.set_description("test consensus adversarial");
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
