/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */
#include "consensus/simplex/votes.h"

#include "invariants.h"

namespace ton::validator::consensus::test {

// --- Helpers ---

std::string certificate_trace_key(const Traced<ProtocolCertificateSent>& record) {
  std::vector<size_t> signers;
  for (PeerValidatorId signer : record.event.signers) {
    signers.push_back(signer.value());
  }
  std::sort(signers.begin(), signers.end());
  std::string key = PSTRING() << record.event.vote;
  for (size_t signer : signers) {
    key += PSTRING() << "|" << signer;
  }
  return key;
}

std::string vote_trace_key(const simplex::Vote& vote) {
  return PSTRING() << vote;
}

// --- Unconditional invariants ---

td::Status verify_no_double_notar(const TraceSnapshot& snapshot) {
  // Lemma 2.2: no honest validator casts Notar(s, h1) and Notar(s, h2) for the same slot.
  std::map<std::pair<size_t, td::uint32>, CandidateId> first_vote;
  for (const auto& r : snapshot.protocol_votes) {
    auto* notar = std::get_if<simplex::NotarizeVote>(&r.event.vote.vote);
    if (!notar) {
      continue;
    }
    auto key = std::make_pair(r.node_idx, notar->id.slot);
    auto [it, inserted] = first_vote.emplace(key, notar->id);
    if (!inserted && it->second != notar->id) {
      return td::Status::Error(PSTRING() << "validator #" << r.node_idx << " sent conflicting Notar votes for slot "
                                         << notar->id.slot << ": first=" << it->second << ", later=" << notar->id);
    }
  }
  return td::Status::OK();
}

td::Status verify_adversarial_invariants(const TraceSnapshot& snapshot, std::set<size_t> byzantine) {
  // §1.4 safety core: honest vote discipline + no conflicting quorum certificates.

  std::map<std::pair<size_t, td::uint32>, CandidateId> first_honest_notar;
  std::map<std::pair<size_t, td::uint32>, CandidateId> first_honest_final;
  std::map<size_t, std::set<td::uint32>> honest_skip_slots;
  std::map<td::uint32, CandidateId> notar_cert_by_slot;
  std::map<td::uint32, CandidateId> final_cert_by_slot;
  std::set<td::uint32> skipped_slots;

  for (const auto& r : snapshot.protocol_votes) {
    if (byzantine.contains(r.node_idx)) {
      continue;
    }
    if (auto* notar = std::get_if<simplex::NotarizeVote>(&r.event.vote.vote)) {
      auto key = std::make_pair(r.node_idx, notar->id.slot);
      auto [it, ins] = first_honest_notar.emplace(key, notar->id);
      if (!ins && it->second != notar->id) {
        return td::Status::Error(PSTRING() << "honest validator #" << r.node_idx << " sent conflicting Notar for slot "
                                           << notar->id.slot);
      }
    } else if (auto* skip = std::get_if<simplex::SkipVote>(&r.event.vote.vote)) {
      honest_skip_slots[r.node_idx].insert(skip->slot);
      if (first_honest_final.contains({r.node_idx, skip->slot})) {
        return td::Status::Error(PSTRING() << "honest validator #" << r.node_idx
                                           << " sent both Skip and Final for slot " << skip->slot);
      }
    } else if (auto* fin = std::get_if<simplex::FinalizeVote>(&r.event.vote.vote)) {
      auto key = std::make_pair(r.node_idx, fin->id.slot);
      auto [it, ins] = first_honest_final.emplace(key, fin->id);
      if (!ins && it->second != fin->id) {
        return td::Status::Error(PSTRING() << "honest validator #" << r.node_idx << " sent conflicting Final for slot "
                                           << fin->id.slot);
      }
      if (honest_skip_slots[r.node_idx].contains(fin->id.slot)) {
        return td::Status::Error(PSTRING() << "honest validator #" << r.node_idx
                                           << " sent both Final and Skip for slot " << fin->id.slot);
      }
    }
  }

  for (const auto& r : snapshot.protocol_certificates) {
    if (auto* notar = std::get_if<simplex::NotarizeVote>(&r.event.vote.vote)) {
      auto [it, ins] = notar_cert_by_slot.emplace(notar->id.slot, notar->id);
      if (!ins && it->second != notar->id) {
        return td::Status::Error(PSTRING() << "conflicting notarization certificates for slot " << notar->id.slot);
      }
    } else if (auto* skip = std::get_if<simplex::SkipVote>(&r.event.vote.vote)) {
      skipped_slots.insert(skip->slot);
    } else if (auto* fin = std::get_if<simplex::FinalizeVote>(&r.event.vote.vote)) {
      auto [it, ins] = final_cert_by_slot.emplace(fin->id.slot, fin->id);
      if (!ins && it->second != fin->id) {
        return td::Status::Error(PSTRING() << "conflicting finalization certificates for slot " << fin->id.slot);
      }
    }
  }

  for (const auto& [slot, id] : final_cert_by_slot) {
    if (skipped_slots.contains(slot)) {
      return td::Status::Error(PSTRING() << "slot " << slot << " reached both Final and Skip certificates");
    }
  }
  return td::Status::OK();
}

td::Status verify_notar_requires_parent_notar(const TraceSnapshot& snapshot) {
  // Rule 4(2): child candidate not notarized until parent notarization observed.
  std::map<std::pair<size_t, CandidateId>, double> first_candidate_ts;
  std::map<CandidateId, ParentId> parent_by_candidate;

  for (const auto& r : snapshot.candidates_generated) {
    auto key = std::make_pair(r.node_idx, r.event.candidate->id);
    auto [it, ins] = first_candidate_ts.emplace(key, r.ts);
    if (!ins) {
      it->second = std::min(it->second, r.ts);
    }
    parent_by_candidate.emplace(r.event.candidate->id, r.event.candidate->parent_id);
  }
  for (const auto& r : snapshot.candidate_deliveries) {
    auto key = std::make_pair(r.event.dst_node_idx, r.event.candidate_id);
    auto [it, ins] = first_candidate_ts.emplace(key, r.ts);
    if (!ins) {
      it->second = std::min(it->second, r.ts);
    }
    parent_by_candidate.emplace(r.event.candidate_id, r.event.parent_id);
  }

  std::map<std::pair<size_t, CandidateId>, double> first_parent_notar_ts;
  for (const auto& r : snapshot.notarizations_observed) {
    auto key = std::make_pair(r.node_idx, r.event.id);
    auto [it, ins] = first_parent_notar_ts.emplace(key, r.ts);
    if (!ins) {
      it->second = std::min(it->second, r.ts);
    }
  }

  bool saw_candidate_before_parent = false;
  bool saw_checked = false;
  for (const auto& r : snapshot.protocol_votes) {
    auto* notar = std::get_if<simplex::NotarizeVote>(&r.event.vote.vote);
    if (!notar) {
      continue;
    }
    auto parent_it = parent_by_candidate.find(notar->id);
    if (parent_it == parent_by_candidate.end() || !parent_it->second.has_value()) {
      continue;
    }
    saw_checked = true;
    CandidateId parent_id = *parent_it->second;
    auto notar_it = first_parent_notar_ts.find({r.node_idx, parent_id});
    if (notar_it == first_parent_notar_ts.end()) {
      return td::Status::Error(PSTRING() << "validator #" << r.node_idx << " voted Notar for child " << notar->id
                                         << " without observing parent notarization for " << parent_id);
    }
    if (notar_it->second > r.ts + 1e-9) {
      return td::Status::Error(PSTRING() << "validator #" << r.node_idx << " voted Notar for child " << notar->id
                                         << " before observing parent notarization");
    }
    auto avail_it = first_candidate_ts.find({r.node_idx, notar->id});
    if (avail_it != first_candidate_ts.end() && avail_it->second < notar_it->second - 1e-9) {
      saw_candidate_before_parent = true;
    }
  }
  if (!saw_checked) {
    return td::Status::Error("scenario did not produce any child-candidate Notar votes");
  }
  if (!saw_candidate_before_parent) {
    return td::Status::Error("scenario did not deliver any child candidate before its parent notarization");
  }
  return td::Status::OK();
}

td::Status verify_finalize_requires_own_notar(const TraceSnapshot& snapshot) {
  // Rule 5(1): Final(s,h) only after own Notar(s,h).
  std::map<std::pair<size_t, CandidateId>, double> first_notar_ts;
  for (const auto& r : snapshot.protocol_votes) {
    auto* notar = std::get_if<simplex::NotarizeVote>(&r.event.vote.vote);
    if (!notar) {
      continue;
    }
    auto key = std::make_pair(r.node_idx, notar->id);
    auto [it, ins] = first_notar_ts.emplace(key, r.ts);
    if (!ins) {
      it->second = std::min(it->second, r.ts);
    }
  }
  for (const auto& r : snapshot.protocol_votes) {
    auto* fin = std::get_if<simplex::FinalizeVote>(&r.event.vote.vote);
    if (!fin) {
      continue;
    }
    auto it = first_notar_ts.find({r.node_idx, fin->id});
    if (it == first_notar_ts.end()) {
      return td::Status::Error(PSTRING() << "validator #" << r.node_idx << " sent Final without own prior Notar for "
                                         << fin->id);
    }
    if (it->second > r.ts + 1e-9) {
      return td::Status::Error(PSTRING() << "validator #" << r.node_idx << " sent Final before own Notar for "
                                         << fin->id);
    }
  }
  return td::Status::OK();
}

td::Status verify_no_skip_final_conflict(const TraceSnapshot& snapshot) {
  // §1.4 + Rule 5(3): never both Skip(s) and Final(s,h).
  std::map<size_t, std::set<td::uint32>> skip_by_validator;
  std::map<size_t, std::set<td::uint32>> final_by_validator;
  for (const auto& r : snapshot.protocol_votes) {
    if (auto* skip = std::get_if<simplex::SkipVote>(&r.event.vote.vote)) {
      skip_by_validator[r.node_idx].insert(skip->slot);
    } else if (auto* fin = std::get_if<simplex::FinalizeVote>(&r.event.vote.vote)) {
      final_by_validator[r.node_idx].insert(fin->id.slot);
    }
  }
  for (const auto& [v, skip_slots] : skip_by_validator) {
    auto it = final_by_validator.find(v);
    if (it == final_by_validator.end()) {
      continue;
    }
    for (td::uint32 slot : skip_slots) {
      if (it->second.contains(slot)) {
        return td::Status::Error(PSTRING() << "validator #" << v << " sent both Skip and Final for slot " << slot);
      }
    }
  }
  return td::Status::OK();
}

td::Status verify_certificate_requires_quorum(const TraceSnapshot& snapshot, const TestConfig& config) {
  // Rule 7: certificate requires quorum weight.
  td::uint64 quorum = config.quorum_weight();
  for (const auto& r : snapshot.protocol_certificates) {
    std::set<size_t> unique_signers;
    for (PeerValidatorId signer : r.event.signers) {
      unique_signers.insert(signer.value());
    }
    td::uint64 w = 0;
    for (size_t s : unique_signers) {
      w += config.node_weight(s);
    }
    if (w < quorum) {
      return td::Status::Error(PSTRING() << "certificate for " << r.event.vote << " had insufficient weight: " << w
                                         << " < " << quorum);
    }
  }
  return td::Status::OK();
}

td::Status verify_certificate_rebroadcast(const TraceSnapshot& snapshot) {
  // Rule 7: upon forming/receiving certificate, validator broadcasts it.
  // Only meaningful if certificates were produced; skip silently otherwise.
  if (snapshot.protocol_certificates.empty()) {
    return td::Status::OK();
  }
  std::map<std::string, std::set<size_t>> broadcasters;
  for (const auto& r : snapshot.protocol_certificates) {
    broadcasters[certificate_trace_key(r)].insert(r.node_idx);
  }
  for (const auto& [cert, nodes] : broadcasters) {
    if (nodes.size() >= 2) {
      return td::Status::OK();
    }
  }
  return td::Status::Error("no certificate was broadcast by more than one validator");
}

td::Status verify_finalize_requires_observed_notar(const TraceSnapshot& snapshot) {
  // Rule 5(2): Final(s,h) only after proving Notar(s,h) reached.
  std::map<std::pair<size_t, CandidateId>, double> first_notar_obs_ts;
  for (const auto& r : snapshot.notarizations_observed) {
    auto key = std::make_pair(r.node_idx, r.event.id);
    auto [it, ins] = first_notar_obs_ts.emplace(key, r.ts);
    if (!ins) {
      it->second = std::min(it->second, r.ts);
    }
  }
  for (const auto& r : snapshot.protocol_votes) {
    auto* fin = std::get_if<simplex::FinalizeVote>(&r.event.vote.vote);
    if (!fin) {
      continue;
    }
    auto it = first_notar_obs_ts.find({r.node_idx, fin->id});
    if (it == first_notar_obs_ts.end()) {
      return td::Status::Error(PSTRING() << "validator #" << r.node_idx
                                         << " sent Final without observing notarization for " << fin->id);
    }
    if (it->second > r.ts + 1e-9) {
      return td::Status::Error(PSTRING() << "validator #" << r.node_idx
                                         << " sent Final before observing notarization for " << fin->id);
    }
  }
  // No Final votes is fine -- not an invariant violation, just nothing to check.
  return td::Status::OK();
}

td::Status verify_leader_window_schedule(const TraceSnapshot& snapshot, const TestConfig& config) {
  // Window scheduling: window k starts at slot kL, spans L slots, leader = k mod n.
  td::uint32 L = config.consensus_config.slots_per_leader_window;
  for (const auto& r : snapshot.leader_windows_started) {
    if (r.event.start_slot % L != 0) {
      return td::Status::Error(PSTRING() << "leader window started at non-boundary slot " << r.event.start_slot);
    }
    td::uint32 expected_end = r.event.start_slot + L;
    if (r.event.end_slot != expected_end) {
      return td::Status::Error(PSTRING() << "leader window [" << r.event.start_slot << ", " << r.event.end_slot
                                         << ") wrong end, expected " << expected_end);
    }
    size_t expected_leader = (r.event.start_slot / L) % config.n_nodes;
    if (r.node_idx != expected_leader) {
      return td::Status::Error(PSTRING() << "window at slot " << r.event.start_slot << " announced by #" << r.node_idx
                                         << ", expected #" << expected_leader);
    }
  }
  return td::Status::OK();
}

// --- Conditional invariants ---

td::Status verify_no_misbehavior_reports(const TraceSnapshot& snapshot) {
  if (!snapshot.misbehavior_reports.empty()) {
    const auto& r = snapshot.misbehavior_reports.front();
    return td::Status::Error(PSTRING() << "validator #" << r.node_idx << " emitted misbehavior report for #"
                                       << r.event.id);
  }
  return td::Status::OK();
}

td::Status verify_empty_candidates_consensus(const TraceSnapshot& snapshot) {
  std::map<CandidateId, BlockIdExt> empty_by_id;
  for (const auto& r : snapshot.candidates_generated) {
    if (r.event.candidate->is_empty()) {
      empty_by_id.emplace(r.event.candidate->id, r.event.candidate->block_id());
    }
  }
  if (empty_by_id.empty()) {
    return td::Status::Error("no empty candidates produced");
  }

  std::set<CandidateId> notarized;
  for (const auto& r : snapshot.notarizations_observed) {
    if (empty_by_id.contains(r.event.id)) {
      notarized.insert(r.event.id);
    }
  }
  if (notarized.empty()) {
    return td::Status::Error("no empty candidate was notarized");
  }

  std::map<CandidateId, double> finalized_ts;
  for (const auto& r : snapshot.finalizations_observed) {
    if (notarized.contains(r.event.id)) {
      auto [it, ins] = finalized_ts.emplace(r.event.id, r.ts);
      if (!ins) {
        it->second = std::min(it->second, r.ts);
      }
    }
  }
  if (finalized_ts.empty()) {
    return td::Status::Error("no empty candidate was finalized");
  }

  std::map<BlockIdExt, double> first_accepted;
  for (const auto& r : snapshot.accepted_blocks) {
    auto [it, ins] = first_accepted.emplace(r.event.block_id, r.ts);
    if (!ins) {
      it->second = std::min(it->second, r.ts);
    }
  }

  bool saw_later = false;
  for (const auto& [id, ts] : finalized_ts) {
    auto block_it = empty_by_id.find(id);
    CHECK(block_it != empty_by_id.end());
    auto acc_it = first_accepted.find(block_it->second);
    if (acc_it == first_accepted.end()) {
      return td::Status::Error(PSTRING() << "block " << block_it->second.to_str() << " for empty candidate " << id
                                         << " never accepted");
    }
    if (acc_it->second > ts + 1e-9) {
      return td::Status::Error(PSTRING() << "empty candidate " << id << " finalized before block accepted");
    }
    for (const auto& r : snapshot.finalizations_observed) {
      if (r.event.id.slot > id.slot && r.ts >= ts - 1e-9) {
        saw_later = true;
        break;
      }
    }
  }
  if (!saw_later) {
    return td::Status::Error("no descendant finalized after empty candidate");
  }
  return td::Status::OK();
}

td::Status verify_standstill_rebroadcast_contents(const TraceSnapshot& snapshot, const TestConfig& config) {
  // Rule 8: after stall, rebroadcast highest final cert + later certs/votes.
  constexpr size_t node_idx = 0;
  double last_fin_ts = -1.0;
  CandidateId last_fin_id;
  for (const auto& r : snapshot.finalizations_observed) {
    if (r.node_idx == node_idx && r.ts > last_fin_ts) {
      last_fin_ts = r.ts;
      last_fin_id = r.event.id;
    }
  }
  if (last_fin_ts < 0.0) {
    return td::Status::Error("no finalization observed on validator #0");
  }

  double standstill_ts =
      last_fin_ts +
      static_cast<double>(config.consensus_config.noncritical_params.standstill_timeout.count()) / 1000.0 * 0.75;

  bool saw_final_cert = false;
  std::set<std::string> later_certs_before;
  for (const auto& r : snapshot.protocol_certificates) {
    if (r.node_idx != node_idx) {
      continue;
    }
    auto* fin = std::get_if<simplex::FinalizeVote>(&r.event.vote.vote);
    if (fin && fin->id == last_fin_id && r.ts >= standstill_ts) {
      saw_final_cert = true;
    }
    if (r.ts < standstill_ts && r.event.vote.referenced_slot() > last_fin_id.slot) {
      later_certs_before.insert(certificate_trace_key(r));
    }
  }
  if (!saw_final_cert) {
    return td::Status::Error("validator #0 did not rebroadcast highest final cert after standstill");
  }

  std::set<std::string> later_votes_before;
  for (const auto& r : snapshot.protocol_votes) {
    if (r.node_idx != node_idx) {
      continue;
    }
    if (r.ts < standstill_ts && r.event.vote.referenced_slot() > last_fin_id.slot) {
      later_votes_before.insert(vote_trace_key(r.event.vote));
    }
  }
  if (later_votes_before.empty()) {
    return td::Status::Error("no later own votes before standstill attempt");
  }

  std::set<std::string> rebroadcast_certs;
  for (const auto& r : snapshot.protocol_certificates) {
    if (r.node_idx == node_idx && r.ts >= standstill_ts) {
      rebroadcast_certs.insert(certificate_trace_key(r));
    }
  }
  for (const auto& k : later_certs_before) {
    if (!rebroadcast_certs.contains(k)) {
      return td::Status::Error(PSTRING() << "validator #0 did not rebroadcast later cert " << k);
    }
  }

  std::set<std::string> rebroadcast_votes;
  for (const auto& r : snapshot.protocol_votes) {
    if (r.node_idx == node_idx && r.ts >= standstill_ts) {
      rebroadcast_votes.insert(vote_trace_key(r.event.vote));
    }
  }
  for (const auto& k : later_votes_before) {
    if (!rebroadcast_votes.contains(k)) {
      return td::Status::Error(PSTRING() << "validator #0 did not rebroadcast later vote " << k);
    }
  }
  return td::Status::OK();
}

// --- Run all ---

td::Status run_all_invariants(const TraceSnapshot& snapshot, const TestConfig& config) {
  TRY_STATUS(verify_no_double_notar(snapshot));
  TRY_STATUS(verify_adversarial_invariants(snapshot));
  // verify_notar_requires_parent_notar is scenario-specific (needs specific setup),
  // so it's not run unconditionally.
  TRY_STATUS(verify_finalize_requires_own_notar(snapshot));
  TRY_STATUS(verify_no_skip_final_conflict(snapshot));
  TRY_STATUS(verify_certificate_requires_quorum(snapshot, config));
  TRY_STATUS(verify_certificate_rebroadcast(snapshot));
  TRY_STATUS(verify_finalize_requires_observed_notar(snapshot));
  TRY_STATUS(verify_leader_window_schedule(snapshot, config));
  TRY_STATUS(verify_no_misbehavior_reports(snapshot));
  return td::Status::OK();
}

}  // namespace ton::validator::consensus::test
