/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */
#include "harness.h"

namespace ton::validator::consensus::test {
namespace {

TestConfig silent_node_config() {
  TestConfig cfg;
  cfg.n_nodes = 4;
  cfg.consensus_config.slots_per_leader_window = 1;
  cfg.consensus_config.noncritical_params.target_rate = std::chrono::milliseconds{100};
  return cfg;
}

// ---------------------------------------------------------------------------
// SilentValidatorDoesNotStopProgress
// ---------------------------------------------------------------------------

td::actor::Task<> silent_validator_scenario(HarnessHandle& h) {
  co_await h.wait_for("initial finalization", predicates::finalization_on(TimePoint::genesis(), 0));
  auto tp = co_await h.stop_instance(3);
  co_await h.wait_for("finalization after node #3 stopped", predicates::finalization_past(tp, {3}));
  co_return {};
}

td::Status silent_validator_verify(const TraceSnapshot&) {
  return td::Status::OK();
}

RegisterTestCase _silent_validator{TestCaseDescriptor{
    .name = "silent-validator-does-not-stop-progress",
    .config = silent_node_config(),
    .scenario = silent_validator_scenario,
    .verify = silent_validator_verify,
}};

// ---------------------------------------------------------------------------
// ConsecutiveSilentLeadersThenProgress
// ---------------------------------------------------------------------------

TestConfig consecutive_silent_config() {
  TestConfig cfg;
  cfg.n_nodes = 7;
  cfg.consensus_config.slots_per_leader_window = 1;
  cfg.consensus_config.noncritical_params.target_rate = std::chrono::milliseconds{100};
  return cfg;
}

td::actor::Task<> consecutive_silent_scenario(HarnessHandle& h) {
  co_await h.wait_for("initial finalization", predicates::finalization_on(TimePoint::genesis(), 0));

  // Stop two nodes. Their next leader windows will be skipped.
  auto tp0 = co_await h.stop_instance(0);
  auto tp1 = co_await h.stop_instance(1);
  // Use the later timepoint — both nodes are now stopped.
  TimePoint tp = tp0.slot > tp1.slot ? tp0 : tp1;

  // Wait for finalization past a full round-robin cycle, so that the protocol
  // must have encountered (and skipped) the stopped nodes' leader windows.
  size_t n = h.config().n_nodes;
  td::uint32 full_cycle_slot = tp.slot + static_cast<td::uint32>(n);
  co_await h.wait_for("finalization past full cycle",
                      predicates::finalization_past(TimePoint{full_cycle_slot}, {0, 1}));
  co_return {};
}

td::Status consecutive_silent_verify(const TraceSnapshot& snapshot) {
  bool saw_skip = false;
  for (const auto& r : snapshot.protocol_certificates) {
    if (std::get_if<simplex::SkipVote>(&r.event.vote.vote)) {
      saw_skip = true;
      break;
    }
  }
  if (!saw_skip) {
    return td::Status::Error("no Skip certificate produced despite full cycle");
  }
  return td::Status::OK();
}

RegisterTestCase _consecutive_silent{TestCaseDescriptor{
    .name = "consecutive-silent-leaders-then-progress",
    .config = consecutive_silent_config(),
    .scenario = consecutive_silent_scenario,
    .verify = consecutive_silent_verify,
}};

// ---------------------------------------------------------------------------
// LeaderCandidateToTooLittleWeightSkipsThenProgress
// ---------------------------------------------------------------------------

td::actor::Task<> candidate_starvation_scenario(HarnessHandle& h) {
  co_await h.wait_for("initial finalization", predicates::finalization_on(TimePoint::genesis(), 0));
  size_t n = h.config().n_nodes;
  auto tp = co_await h.now();

  // Pick a slot past the current frontier for node #1's window.
  td::uint32 attacked_slot = tp.slot + static_cast<td::uint32>(n);
  while (attacked_slot % n != 1) {
    ++attacked_slot;
  }
  size_t leader = attacked_slot % n;
  size_t allowed = (leader + 1) % n;

  // Drop candidates to everyone except one peer.
  for (size_t i = 0; i < n; ++i) {
    if (i != leader && i != allowed) {
      h.message_filters().candidate_drop_rules.push_back(TestMessageFilters::CandidateDropRule{
          .src_node_idx = leader,
          .dst_node_indices = {i},
          .start_slot = attacked_slot,
          .end_slot = attacked_slot + 1,
      });
    }
  }

  co_await h.wait_for("finalization past starved slot", predicates::finalization_past(TimePoint{attacked_slot}));
  co_return {};
}

td::Status candidate_starvation_verify(const TraceSnapshot&) {
  return td::Status::OK();
}

RegisterTestCase _candidate_starvation{TestCaseDescriptor{
    .name = "leader-candidate-to-too-little-weight-skips-then-progress",
    .config = silent_node_config(),
    .scenario = candidate_starvation_scenario,
    .verify = candidate_starvation_verify,
}};

// ---------------------------------------------------------------------------
// EmptyCandidatesWithSilentValidatorStillFinalize
// ---------------------------------------------------------------------------

TestConfig empty_silent_config() {
  TestConfig cfg;
  cfg.n_nodes = 4;
  cfg.shard = ShardIdFull{masterchainId};
  cfg.consensus_config.noncritical_params.target_rate = std::chrono::milliseconds{100};
  // Slow validation so the leader generates empty blocks while validation is pending.
  cfg.validation_time = {0.3, 0.3};
  return cfg;
}

td::actor::Task<> empty_silent_scenario(HarnessHandle& h) {
  co_await h.wait_for("initial finalization", predicates::finalization_on(TimePoint::genesis(), 0));
  auto tp = co_await h.stop_instance(3);
  co_await h.wait_for("empty candidate finalized after stop", predicates::empty_candidate_finalized(tp));
  co_return {};
}

td::Status empty_silent_verify(const TraceSnapshot&) {
  return td::Status::OK();
}

RegisterTestCase _empty_silent{TestCaseDescriptor{
    .name = "empty-candidates-with-silent-validator-still-finalize",
    .config = empty_silent_config(),
    .scenario = empty_silent_scenario,
    .verify = empty_silent_verify,
}};

}  // namespace
}  // namespace ton::validator::consensus::test
