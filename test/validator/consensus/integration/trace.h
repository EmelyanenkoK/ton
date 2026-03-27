/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */
#pragma once

#include <functional>

#include "consensus/bus.h"
#include "consensus/simplex/bus.h"

#include "config.h"

namespace ton::validator::consensus::test {

// Wrapper that stamps a Bus event with the node it came from and a timestamp.
template <typename Event>
struct Traced {
  double ts = 0.0;
  size_t node_idx = 0;
  Event event;
};

// Events that don't come from the Bus and need their own structs.

struct ProtocolVoteSent {
  std::optional<size_t> dst_node_idx;
  simplex::Vote vote;
  std::string raw_message;
};

struct ProtocolCertificateSent {
  std::optional<size_t> dst_node_idx;
  simplex::Vote vote;
  std::vector<PeerValidatorId> signers;
  std::string raw_message;
};

struct OverlayRequestSent {
  size_t dst_node_idx = 0;
  double timeout_s = 0.0;
  std::optional<CandidateId> candidate_id;
  bool want_candidate = false;
  bool want_notar = false;
};

struct CandidateDelivered {
  size_t dst_node_idx = 0;
  CandidateId candidate_id;
  ParentId parent_id;
  PeerValidatorId leader;
  bool is_empty = false;
  BlockIdExt block_id;
};

struct AcceptedBlock {
  BlockIdExt block_id;
};

struct NetworkToggle {
  bool disabled = false;
};

struct Lifecycle {
  bool started = false;
};

struct CandidateResolved {
  CandidateId id;
};

struct MalformedCandidateResponse {
  size_t responder_node_idx = 0;
  CandidateId id;
};

struct DuplicateLocalVotePersistence {
  simplex::Vote vote;
};

struct TraceSnapshot {
  // Bus events wrapped with metadata
  std::vector<Traced<simplex::LeaderWindowObserved>> leader_windows_observed;
  std::vector<Traced<OurLeaderWindowStarted>> leader_windows_started;
  std::vector<Traced<simplex::NotarizationObserved>> notarizations_observed;
  std::vector<Traced<simplex::FinalizationObserved>> finalizations_observed;
  std::vector<Traced<CandidateGenerated>> candidates_generated;
  std::vector<Traced<MisbehaviorReport>> misbehavior_reports;

  // Network/overlay events (non-Bus)
  std::vector<Traced<ProtocolVoteSent>> protocol_votes;
  std::vector<Traced<ProtocolCertificateSent>> protocol_certificates;
  std::vector<Traced<OverlayRequestSent>> overlay_requests;
  std::vector<Traced<CandidateDelivered>> candidate_deliveries;

  // Harness control events
  std::vector<Traced<AcceptedBlock>> accepted_blocks;
  std::vector<Traced<NetworkToggle>> network_toggles;
  std::vector<Traced<Lifecycle>> lifecycle;
  std::vector<Traced<CandidateResolved>> candidates_resolved;
  std::vector<Traced<MalformedCandidateResponse>> malformed_candidate_responses;
  std::vector<Traced<DuplicateLocalVotePersistence>> duplicate_local_vote_persistence;
};

// An incremental predicate over the trace. Evaluated on the TraceSink's thread
// each time a new event is recorded. Implementations should only scan new events
// (use the offsets pattern) so that total cost is O(total_events), not O(events^2).
class TracePredicate {
 public:
  virtual ~TracePredicate() = default;
  // Called after each new event. Return true when the condition is satisfied.
  virtual bool check(const TraceSnapshot& snapshot) = 0;
};

// A registered waiter: predicate + promise to resolve when satisfied.
struct TraceWaiter {
  std::string description;
  std::unique_ptr<TracePredicate> predicate;
  td::Promise<td::Unit> promise;
};

// Actor that collects all trace records and notifies waiters.
class TraceSink : public td::actor::Actor {
 public:
  void record_leader_window_observed(size_t node_idx, simplex::LeaderWindowObserved e);
  void record_leader_window_started(size_t node_idx, OurLeaderWindowStarted e);
  void record_notarization_observed(size_t node_idx, simplex::NotarizationObserved e);
  void record_finalization_observed(size_t node_idx, simplex::FinalizationObserved e);
  void record_candidate_generated(size_t node_idx, CandidateGenerated e);
  void record_misbehavior(size_t node_idx, MisbehaviorReport e);
  void record_protocol_vote(size_t node_idx, ProtocolVoteSent e);
  void record_protocol_certificate(size_t node_idx, ProtocolCertificateSent e);
  void record_overlay_request(size_t node_idx, OverlayRequestSent e);
  void record_candidate_delivery(size_t node_idx, CandidateDelivered e);
  void record_accepted_block(size_t node_idx, AcceptedBlock e);
  void record_network_toggle(size_t node_idx, NetworkToggle e);
  void record_lifecycle(size_t node_idx, Lifecycle e);
  void record_candidate_resolved(size_t node_idx, CandidateResolved e);
  void record_malformed_response(size_t node_idx, MalformedCandidateResponse e);
  void record_duplicate_local_vote_persistence(size_t node_idx, DuplicateLocalVotePersistence e);

  // Register a waiter that will be checked on each new event.
  void wait_for(std::string description, std::unique_ptr<TracePredicate> predicate, td::Promise<td::Unit> promise);

  void clear();
  td::actor::Task<TraceSnapshot> snapshot();

  const TraceSnapshot& data() const {
    return data_;
  }

 private:
  TraceSnapshot data_;
  std::vector<TraceWaiter> waiters_;

  void check_waiters();
};

// --- Built-in incremental predicates ---
// All predicates take a TimePoint `after` — they only consider events
// in slots > after.slot.

namespace predicates {

// True when any AcceptedBlock on `node_idx` has seqno >= target.
std::unique_ptr<TracePredicate> accepted_seqno_ge(TimePoint after, size_t node_idx, BlockSeqno target);

// True when any FinalizationObserved arrives on `node_idx` past `after`.
std::unique_ptr<TracePredicate> finalization_on(TimePoint after, size_t node_idx);

// True when any FinalizationObserved has slot > after.slot, excluding nodes in `exclude`.
std::unique_ptr<TracePredicate> finalization_past(TimePoint after, std::set<size_t> exclude = {});

// True when a SkipVote certificate exists for any slot where slot % n_nodes == leader_idx
// (and slot > after.slot), AND a finalization exists past that skipped slot.
std::unique_ptr<TracePredicate> skip_and_finalization_past_leader(TimePoint after, size_t leader_idx, size_t n_nodes);

// True when an empty candidate generated after `after` has been finalized.
std::unique_ptr<TracePredicate> empty_candidate_finalized(TimePoint after);

}  // namespace predicates

}  // namespace ton::validator::consensus::test
