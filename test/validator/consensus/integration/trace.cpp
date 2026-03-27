/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */
#include "trace.h"

namespace ton::validator::consensus::test {

// Each record_* appends the event and then checks all waiters.
#define DEFINE_RECORD(method, field, Type)                                       \
  void TraceSink::method(size_t node_idx, Type e) {                              \
    data_.field.push_back({td::Time::now_unadjusted(), node_idx, std::move(e)}); \
    check_waiters();                                                             \
  }

DEFINE_RECORD(record_leader_window_observed, leader_windows_observed, simplex::LeaderWindowObserved)
DEFINE_RECORD(record_leader_window_started, leader_windows_started, OurLeaderWindowStarted)
DEFINE_RECORD(record_notarization_observed, notarizations_observed, simplex::NotarizationObserved)
DEFINE_RECORD(record_finalization_observed, finalizations_observed, simplex::FinalizationObserved)
DEFINE_RECORD(record_candidate_generated, candidates_generated, CandidateGenerated)
DEFINE_RECORD(record_misbehavior, misbehavior_reports, MisbehaviorReport)
DEFINE_RECORD(record_protocol_vote, protocol_votes, ProtocolVoteSent)
DEFINE_RECORD(record_protocol_certificate, protocol_certificates, ProtocolCertificateSent)
DEFINE_RECORD(record_overlay_request, overlay_requests, OverlayRequestSent)
DEFINE_RECORD(record_candidate_delivery, candidate_deliveries, CandidateDelivered)
DEFINE_RECORD(record_accepted_block, accepted_blocks, AcceptedBlock)
DEFINE_RECORD(record_network_toggle, network_toggles, NetworkToggle)
DEFINE_RECORD(record_lifecycle, lifecycle, Lifecycle)
DEFINE_RECORD(record_candidate_resolved, candidates_resolved, CandidateResolved)
DEFINE_RECORD(record_malformed_response, malformed_candidate_responses, MalformedCandidateResponse)
DEFINE_RECORD(record_duplicate_local_vote_persistence, duplicate_local_vote_persistence, DuplicateLocalVotePersistence)

#undef DEFINE_RECORD

void TraceSink::wait_for(std::string description, std::unique_ptr<TracePredicate> predicate,
                         td::Promise<td::Unit> promise) {
  // Check immediately in case the condition is already satisfied.
  if (predicate->check(data_)) {
    LOG(WARNING) << "Done waiting (immediate): " << description;
    promise.set_value(td::Unit{});
    return;
  }
  LOG(WARNING) << "Waiting: " << description;
  waiters_.push_back(TraceWaiter{
      .description = std::move(description),
      .predicate = std::move(predicate),
      .promise = std::move(promise),
  });
}

void TraceSink::check_waiters() {
  for (auto it = waiters_.begin(); it != waiters_.end();) {
    if (it->predicate->check(data_)) {
      LOG(WARNING) << "Done waiting: " << it->description;
      it->promise.set_value(td::Unit{});
      it = waiters_.erase(it);
    } else {
      ++it;
    }
  }
}

void TraceSink::clear() {
  data_ = {};
}

td::actor::Task<TraceSnapshot> TraceSink::snapshot() {
  co_return data_;
}

// --- Built-in incremental predicates ---
// All predicates ignore events in slots <= after.slot.

namespace predicates {

class AcceptedSeqnoGe final : public TracePredicate {
 public:
  AcceptedSeqnoGe(td::uint32 after_slot, size_t node_idx, BlockSeqno target)
      : after_slot_(after_slot), node_idx_(node_idx), target_(target) {
  }
  bool check(const TraceSnapshot& snap) override {
    for (; offset_ < snap.accepted_blocks.size(); ++offset_) {
      const auto& r = snap.accepted_blocks[offset_];
      if (r.node_idx == node_idx_ && r.event.block_id.seqno() >= target_) {
        return true;
      }
    }
    return false;
  }

 private:
  td::uint32 after_slot_;
  size_t node_idx_;
  BlockSeqno target_;
  size_t offset_ = 0;
};

std::unique_ptr<TracePredicate> accepted_seqno_ge(TimePoint after, size_t node_idx, BlockSeqno target) {
  return std::make_unique<AcceptedSeqnoGe>(after.slot, node_idx, target);
}

class FinalizationOn final : public TracePredicate {
 public:
  FinalizationOn(td::uint32 after_slot, size_t node_idx) : after_slot_(after_slot), node_idx_(node_idx) {
  }
  bool check(const TraceSnapshot& snap) override {
    for (; offset_ < snap.finalizations_observed.size(); ++offset_) {
      const auto& r = snap.finalizations_observed[offset_];
      if (r.node_idx == node_idx_ && r.event.id.slot > after_slot_) {
        return true;
      }
    }
    return false;
  }

 private:
  td::uint32 after_slot_;
  size_t node_idx_;
  size_t offset_ = 0;
};

std::unique_ptr<TracePredicate> finalization_on(TimePoint after, size_t node_idx) {
  return std::make_unique<FinalizationOn>(after.slot, node_idx);
}

class FinalizationPast final : public TracePredicate {
 public:
  FinalizationPast(td::uint32 after_slot, std::set<size_t> exclude)
      : after_slot_(after_slot), exclude_(std::move(exclude)) {
  }
  bool check(const TraceSnapshot& snap) override {
    for (; offset_ < snap.finalizations_observed.size(); ++offset_) {
      const auto& r = snap.finalizations_observed[offset_];
      if (!exclude_.contains(r.node_idx) && r.event.id.slot > after_slot_) {
        return true;
      }
    }
    return false;
  }

 private:
  td::uint32 after_slot_;
  std::set<size_t> exclude_;
  size_t offset_ = 0;
};

std::unique_ptr<TracePredicate> finalization_past(TimePoint after, std::set<size_t> exclude) {
  return std::make_unique<FinalizationPast>(after.slot, std::move(exclude));
}

class SkipAndFinalizationPastLeader final : public TracePredicate {
 public:
  SkipAndFinalizationPastLeader(td::uint32 after_slot, size_t leader_idx, size_t n_nodes)
      : after_slot_(after_slot), leader_idx_(leader_idx), n_nodes_(n_nodes) {
  }
  bool check(const TraceSnapshot& snap) override {
    for (; cert_offset_ < snap.protocol_certificates.size(); ++cert_offset_) {
      const auto& r = snap.protocol_certificates[cert_offset_];
      if (auto* skip = std::get_if<simplex::SkipVote>(&r.event.vote.vote)) {
        if (skip->slot > after_slot_ && skip->slot % n_nodes_ == leader_idx_) {
          skipped_slot_ = std::max(skipped_slot_, skip->slot);
        }
      }
    }
    if (skipped_slot_ == 0) {
      return false;
    }
    for (; final_offset_ < snap.finalizations_observed.size(); ++final_offset_) {
      const auto& r = snap.finalizations_observed[final_offset_];
      if (r.node_idx != leader_idx_ && r.event.id.slot > skipped_slot_) {
        return true;
      }
    }
    return false;
  }

 private:
  td::uint32 after_slot_;
  size_t leader_idx_;
  size_t n_nodes_;
  td::uint32 skipped_slot_ = 0;
  size_t cert_offset_ = 0;
  size_t final_offset_ = 0;
};

std::unique_ptr<TracePredicate> skip_and_finalization_past_leader(TimePoint after, size_t leader_idx, size_t n_nodes) {
  return std::make_unique<SkipAndFinalizationPastLeader>(after.slot, leader_idx, n_nodes);
}

class EmptyCandidateFinalized final : public TracePredicate {
 public:
  explicit EmptyCandidateFinalized(td::uint32 after_slot) : after_slot_(after_slot) {
  }
  bool check(const TraceSnapshot& snap) override {
    for (; gen_offset_ < snap.candidates_generated.size(); ++gen_offset_) {
      const auto& r = snap.candidates_generated[gen_offset_];
      if (r.event.candidate->is_empty() && r.event.candidate->id.slot > after_slot_) {
        empty_ids_.insert(r.event.candidate->id);
      }
    }
    if (empty_ids_.empty()) {
      return false;
    }
    for (; final_offset_ < snap.finalizations_observed.size(); ++final_offset_) {
      if (empty_ids_.contains(snap.finalizations_observed[final_offset_].event.id)) {
        return true;
      }
    }
    return false;
  }

 private:
  td::uint32 after_slot_;
  std::set<CandidateId> empty_ids_;
  size_t gen_offset_ = 0;
  size_t final_offset_ = 0;
};

std::unique_ptr<TracePredicate> empty_candidate_finalized(TimePoint after) {
  return std::make_unique<EmptyCandidateFinalized>(after.slot);
}

}  // namespace predicates

}  // namespace ton::validator::consensus::test
