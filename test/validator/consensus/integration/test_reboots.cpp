/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */
#include <algorithm>
#include <array>
#include <cmath>
#include <functional>
#include <limits>

#include "harness.h"

namespace ton::validator::consensus::test {
namespace {

constexpr size_t kRestartedNode = 3;
constexpr size_t kStateLostNode = 3;
constexpr std::array<size_t, 4> kAllNodes = {0, 1, 2, 3};

TestConfig reboot_config() {
  TestConfig cfg;
  cfg.n_nodes = 4;
  cfg.consensus_config.slots_per_leader_window = 1;
  cfg.consensus_config.noncritical_params.target_rate = std::chrono::milliseconds{100};
  cfg.net_ping = {0.1, 0.15};
  return cfg;
}

struct RestartCycle {
  double stop_ts = 0.0;
  double start_ts = 0.0;
};

td::Result<std::vector<RestartCycle>> collect_restart_cycles(const TraceSnapshot& snapshot, size_t node_idx,
                                                             size_t expected_cycles) {
  std::vector<RestartCycle> cycles;
  std::optional<double> pending_stop_ts;
  for (const auto& record : snapshot.lifecycle) {
    if (record.node_idx != node_idx) {
      continue;
    }
    if (!record.event.started) {
      pending_stop_ts = record.ts;
      continue;
    }
    if (pending_stop_ts.has_value() && record.ts > *pending_stop_ts + 1e-9) {
      cycles.push_back(RestartCycle{.stop_ts = *pending_stop_ts, .start_ts = record.ts});
      pending_stop_ts.reset();
    }
  }

  if (cycles.size() != expected_cycles) {
    return td::Status::Error(PSTRING() << "expected " << expected_cycles << " restart cycle(s) for node #" << node_idx
                                       << ", got " << cycles.size());
  }
  return cycles;
}

td::uint32 highest_finalized_slot(const TraceSnapshot& snapshot, std::optional<size_t> node_idx = std::nullopt) {
  td::uint32 highest = 0;
  for (const auto& record : snapshot.finalizations_observed) {
    if (node_idx.has_value() && record.node_idx != *node_idx) {
      continue;
    }
    highest = std::max(highest, record.event.id.slot);
  }
  return highest;
}

td::Status ensure_no_duplicate_local_vote_persistence(const TraceSnapshot& snapshot, double start_ts,
                                                      std::optional<double> end_ts, std::string context) {
  for (const auto& record : snapshot.duplicate_local_vote_persistence) {
    if (record.ts <= start_ts + 1e-9) {
      continue;
    }
    if (end_ts.has_value() && record.ts >= *end_ts - 1e-9) {
      continue;
    }
    return td::Status::Error(PSTRING() << context << ": node #" << record.node_idx
                                       << " retried persisting local vote " << record.event.vote
                                       << " after restart");
  }
  return td::Status::OK();
}

td::Status verify_restart_rejoin_cycles(const TraceSnapshot& snapshot, size_t node_idx, size_t expected_cycles) {
  TRY_RESULT(cycles, collect_restart_cycles(snapshot, node_idx, expected_cycles));

  for (size_t i = 0; i < cycles.size(); ++i) {
    const auto& cycle = cycles[i];

    bool saw_honest_finalization_while_down = false;
    td::uint32 highest_honest_slot = 0;
    for (const auto& record : snapshot.finalizations_observed) {
      if (record.node_idx == node_idx || record.ts <= cycle.stop_ts + 1e-9 || record.ts >= cycle.start_ts - 1e-9) {
        continue;
      }
      highest_honest_slot = std::max(highest_honest_slot, record.event.id.slot);
      saw_honest_finalization_while_down = true;
    }
    if (!saw_honest_finalization_while_down) {
      return td::Status::Error(PSTRING() << "cycle #" << (i + 1) << ": quorum never finalized while node #"
                                         << node_idx << " was down");
    }

    bool saw_rejoined_finalization = false;
    for (const auto& record : snapshot.finalizations_observed) {
      if (record.node_idx == node_idx && record.ts > cycle.start_ts + 1e-9) {
        saw_rejoined_finalization = true;
        break;
      }
    }
    if (!saw_rejoined_finalization) {
      return td::Status::Error(PSTRING() << "cycle #" << (i + 1) << ": node #" << node_idx
                                         << " never observed finalization after restart");
    }

    bool saw_rejoined_vote = false;
    for (const auto& record : snapshot.protocol_votes) {
      if (record.node_idx == node_idx && record.ts > cycle.start_ts + 1e-9) {
        saw_rejoined_vote = true;
        break;
      }
    }
    if (!saw_rejoined_vote) {
      return td::Status::Error(PSTRING() << "cycle #" << (i + 1) << ": node #" << node_idx
                                         << " never rejoined protocol voting after restart");
    }

    std::optional<double> next_stop_ts;
    if (i + 1 < cycles.size()) {
      next_stop_ts = cycles[i + 1].stop_ts;
    }
    TRY_STATUS(ensure_no_duplicate_local_vote_persistence(snapshot, cycle.start_ts, next_stop_ts,
                                                          PSTRING() << "cycle #" << (i + 1)));

    static_cast<void>(highest_honest_slot);
  }

  return td::Status::OK();
}

td::Status verify_full_network_clean_restart(const TraceSnapshot& snapshot) {
  std::array<RestartCycle, kAllNodes.size()> cycles;
  double all_down_ts = 0.0;
  double first_restart_ts = std::numeric_limits<double>::infinity();
  double last_restart_ts = 0.0;

  for (size_t idx = 0; idx < kAllNodes.size(); ++idx) {
    size_t node_idx = kAllNodes[idx];
    TRY_RESULT(node_cycles, collect_restart_cycles(snapshot, node_idx, 1));
    cycles[idx] = node_cycles.front();
    all_down_ts = std::max(all_down_ts, cycles[idx].stop_ts);
    first_restart_ts = std::min(first_restart_ts, cycles[idx].start_ts);
    last_restart_ts = std::max(last_restart_ts, cycles[idx].start_ts);
  }

  if (!std::isfinite(first_restart_ts) || first_restart_ts <= all_down_ts + 1e-9) {
    return td::Status::Error("scenario did not record a real all-down interval before restart");
  }
  TRY_STATUS(ensure_no_duplicate_local_vote_persistence(snapshot, first_restart_ts, std::nullopt,
                                                        "full-network clean restart"));

  for (const auto& record : snapshot.finalizations_observed) {
    if (record.ts > all_down_ts + 1e-9 && record.ts < first_restart_ts - 1e-9) {
      return td::Status::Error(PSTRING() << "observed finalization for node #" << record.node_idx
                                         << " while the whole network was supposed to be down");
    }
  }

  for (size_t idx = 0; idx < kAllNodes.size(); ++idx) {
    size_t node_idx = kAllNodes[idx];
    const auto& cycle = cycles[idx];
    bool saw_vote_after_restart = false;
    bool saw_finalization_after_restart = false;
    for (const auto& record : snapshot.protocol_votes) {
      if (record.node_idx == node_idx && record.ts > cycle.start_ts + 1e-9) {
        saw_vote_after_restart = true;
        break;
      }
    }
    for (const auto& record : snapshot.finalizations_observed) {
      if (record.node_idx == node_idx && record.ts > cycle.start_ts + 1e-9) {
        saw_finalization_after_restart = true;
        break;
      }
    }
    if (!saw_vote_after_restart) {
      return td::Status::Error(PSTRING() << "validator #" << node_idx
                                         << " never voted after the full-network restart");
    }
    if (!saw_finalization_after_restart) {
      return td::Status::Error(PSTRING() << "validator #" << node_idx
                                         << " never finalized after the full-network restart");
    }
  }

  std::set<td::uint32> node0_post_restart_slots;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == 0 && record.ts > last_restart_ts + 1e-9) {
      node0_post_restart_slots.insert(record.event.id.slot);
    }
  }
  if (node0_post_restart_slots.size() < 2) {
    return td::Status::Error("node #0 did not show steady-state finalization after the full-network restart");
  }

  return td::Status::OK();
}

td::Status verify_full_network_restart_with_state_loss(const TraceSnapshot& snapshot) {
  std::array<RestartCycle, kAllNodes.size()> cycles;
  double all_down_ts = 0.0;
  double first_survivor_restart_ts = std::numeric_limits<double>::infinity();
  double latest_survivor_restart_ts = 0.0;

  for (size_t idx = 0; idx < kAllNodes.size(); ++idx) {
    size_t node_idx = kAllNodes[idx];
    TRY_RESULT(node_cycles, collect_restart_cycles(snapshot, node_idx, 1));
    cycles[idx] = node_cycles.front();
    all_down_ts = std::max(all_down_ts, cycles[idx].stop_ts);
    if (node_idx != kStateLostNode) {
      first_survivor_restart_ts = std::min(first_survivor_restart_ts, cycles[idx].start_ts);
      latest_survivor_restart_ts = std::max(latest_survivor_restart_ts, cycles[idx].start_ts);
    }
  }

  double state_lost_restart_ts = cycles[kStateLostNode].start_ts;
  if (!std::isfinite(first_survivor_restart_ts)) {
    return td::Status::Error("scenario did not record surviving-quorum restart events");
  }
  if (state_lost_restart_ts <= latest_survivor_restart_ts + 1e-9) {
    return td::Status::Error("state-lost validator restarted before the surviving quorum had its own restart window");
  }
  TRY_STATUS(ensure_no_duplicate_local_vote_persistence(snapshot, first_survivor_restart_ts, std::nullopt,
                                                        "full-network restart with state loss"));

  for (const auto& record : snapshot.finalizations_observed) {
    if (record.ts > all_down_ts + 1e-9 && record.ts < first_survivor_restart_ts - 1e-9) {
      return td::Status::Error(PSTRING() << "observed finalization for node #" << record.node_idx
                                         << " while the whole network was down before the surviving quorum restarted");
    }
  }

  td::uint32 highest_honest_slot_while_state_lost_node_down = 0;
  bool saw_honest_progress_without_state_lost_node = false;
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == kStateLostNode || record.ts <= first_survivor_restart_ts + 1e-9 ||
        record.ts >= state_lost_restart_ts - 1e-9) {
      continue;
    }
    highest_honest_slot_while_state_lost_node_down =
        std::max(highest_honest_slot_while_state_lost_node_down, record.event.id.slot);
    saw_honest_progress_without_state_lost_node = true;
  }
  if (!saw_honest_progress_without_state_lost_node) {
    return td::Status::Error("surviving quorum never finalized while the state-lost validator stayed down");
  }

  bool saw_state_lost_vote_after_restart = false;
  bool saw_state_lost_finalization_after_restart = false;
  std::set<td::uint32> state_lost_post_restart_slots;
  for (const auto& record : snapshot.protocol_votes) {
    if (record.node_idx == kStateLostNode && record.ts > state_lost_restart_ts + 1e-9) {
      saw_state_lost_vote_after_restart = true;
      break;
    }
  }
  for (const auto& record : snapshot.finalizations_observed) {
    if (record.node_idx == kStateLostNode && record.ts > state_lost_restart_ts + 1e-9) {
      state_lost_post_restart_slots.insert(record.event.id.slot);
      if (record.event.id.slot > highest_honest_slot_while_state_lost_node_down) {
        saw_state_lost_finalization_after_restart = true;
      }
    }
  }
  if (!saw_state_lost_vote_after_restart) {
    return td::Status::Error("state-lost validator never rejoined protocol voting after restart");
  }
  if (!saw_state_lost_finalization_after_restart) {
    return td::Status::Error(PSTRING()
                             << "state-lost validator never finalized beyond the honest frontier reached while it was down (slot "
                             << highest_honest_slot_while_state_lost_node_down << ")");
  }
  if (state_lost_post_restart_slots.size() < 3) {
    return td::Status::Error("state-lost validator did not show steady-state finalization after catching up");
  }

  for (size_t idx = 0; idx < kAllNodes.size(); ++idx) {
    size_t node_idx = kAllNodes[idx];
    if (node_idx == kStateLostNode) {
      continue;
    }
    bool saw_vote_after_restart = false;
    bool saw_finalization_after_restart = false;
    for (const auto& record : snapshot.protocol_votes) {
      if (record.node_idx == node_idx && record.ts > cycles[idx].start_ts + 1e-9) {
        saw_vote_after_restart = true;
        break;
      }
    }
    for (const auto& record : snapshot.finalizations_observed) {
      if (record.node_idx == node_idx && record.ts > cycles[idx].start_ts + 1e-9) {
        saw_finalization_after_restart = true;
        break;
      }
    }
    if (!saw_vote_after_restart || !saw_finalization_after_restart) {
      return td::Status::Error(PSTRING() << "surviving validator #" << node_idx
                                         << " did not fully resume after the full-network restart");
    }
  }

  return td::Status::OK();
}

class AllNodesFinalizedAfter final : public TracePredicate {
 public:
  explicit AllNodesFinalizedAfter(td::uint32 after_slot) : after_slot_(after_slot) {
  }

  bool check(const TraceSnapshot& snap) override {
    std::array<bool, kAllNodes.size()> seen{};
    size_t remaining = kAllNodes.size();
    for (const auto& record : snap.finalizations_observed) {
      if (record.event.id.slot <= after_slot_) {
        continue;
      }
      auto it = std::find(kAllNodes.begin(), kAllNodes.end(), record.node_idx);
      if (it == kAllNodes.end()) {
        continue;
      }
      size_t idx = static_cast<size_t>(std::distance(kAllNodes.begin(), it));
      if (!seen[idx]) {
        seen[idx] = true;
        if (--remaining == 0) {
          return true;
        }
      }
    }
    return false;
  }

 private:
  td::uint32 after_slot_;
};

// Covers a clean single-node stop/start:
// the rest of the quorum must keep finalizing during the downtime, and the
// restarted validator must later rejoin voting and observe finalization again.
td::actor::Task<> single_restart_rejoins_scenario(HarnessHandle& h) {
  co_await h.wait_for("initial finalization", predicates::finalization_on(TimePoint::genesis(), 0));

  auto stop_tp = co_await h.stop_instance(kRestartedNode);
  co_await h.wait_for("honest quorum finalizes while node #3 is down",
                      predicates::finalization_past(stop_tp, {kRestartedNode}));

  auto restart_tp = co_await h.start_instance(kRestartedNode);
  co_await h.wait_for("node #3 finalizes again after restart", predicates::finalization_on(restart_tp, kRestartedNode));
  auto rejoin_tp = co_await h.now();
  co_await h.wait_for("node #3 keeps finalizing after restart", predicates::finalization_on(rejoin_tp, kRestartedNode));
  co_return {};
}

td::Status single_restart_rejoins_verify(const TraceSnapshot& snapshot) {
  return verify_restart_rejoin_cycles(snapshot, kRestartedNode, 1);
}

RegisterTestCase _single_restart_rejoins{TestCaseDescriptor{
    .name = "single-validator-restart-rejoins-consensus",
    .config = reboot_config(),
    .scenario = single_restart_rejoins_scenario,
    .verify = single_restart_rejoins_verify,
}};

// Covers repeated stop/start of the same validator:
// both downtimes must leave the remaining quorum live, and the validator must
// rejoin consensus after the second reboot instead of only after the first.
td::actor::Task<> repeated_restart_rejoins_scenario(HarnessHandle& h) {
  co_await h.wait_for("initial finalization", predicates::finalization_on(TimePoint::genesis(), 0));

  auto stop_tp1 = co_await h.stop_instance(kRestartedNode);
  co_await h.wait_for("honest quorum finalizes during first downtime",
                      predicates::finalization_past(stop_tp1, {kRestartedNode}));

  auto restart_tp1 = co_await h.start_instance(kRestartedNode);
  co_await h.wait_for("node #3 finalizes after first restart", predicates::finalization_on(restart_tp1, kRestartedNode));
  auto steady_tp1 = co_await h.now();
  co_await h.wait_for("node #3 stays active after first restart", predicates::finalization_on(steady_tp1, kRestartedNode));

  auto stop_tp2 = co_await h.stop_instance(kRestartedNode);
  co_await h.wait_for("honest quorum finalizes during second downtime",
                      predicates::finalization_past(stop_tp2, {kRestartedNode}));

  auto restart_tp2 = co_await h.start_instance(kRestartedNode);
  co_await h.wait_for("node #3 finalizes after second restart",
                      predicates::finalization_on(restart_tp2, kRestartedNode));
  auto steady_tp2 = co_await h.now();
  co_await h.wait_for("node #3 stays active after second restart",
                      predicates::finalization_on(steady_tp2, kRestartedNode));
  co_return {};
}

td::Status repeated_restart_rejoins_verify(const TraceSnapshot& snapshot) {
  return verify_restart_rejoin_cycles(snapshot, kRestartedNode, 2);
}

RegisterTestCase _repeated_restart_rejoins{TestCaseDescriptor{
    .name = "repeated-validator-restarts-rejoin-after-second-downtime",
    .config = reboot_config(),
    .scenario = repeated_restart_rejoins_scenario,
    .verify = repeated_restart_rejoins_verify,
}};

// Covers a clean cold restart of the whole validator set:
// every validator is stopped on intact state, the network stays fully idle while all are down,
// then all validators restart and each one must vote/finalize again before steady-state resumes.
td::actor::Task<> full_network_clean_restart_scenario(HarnessHandle& h) {
  co_await h.wait_for("initial finalization", predicates::finalization_on(TimePoint::genesis(), 0));

  for (size_t node_idx : kAllNodes) {
    co_await h.stop_instance(node_idx);
  }

  for (size_t node_idx : kAllNodes) {
    co_await h.start_instance(node_idx);
  }

  auto restart_tp = co_await h.now();
  co_await h.wait_for("all validators finalize after full-network restart",
                      std::make_unique<AllNodesFinalizedAfter>(restart_tp.slot));

  auto steady_tp = co_await h.now();
  co_await h.wait_for("node #0 keeps finalizing after full-network restart", predicates::finalization_on(steady_tp, 0));
  co_return {};
}

RegisterTestCase _full_network_clean_restart{TestCaseDescriptor{
    .name = "full-network-clean-restart-still-finalizes",
    .config = reboot_config(),
    .scenario = full_network_clean_restart_scenario,
    .verify = verify_full_network_clean_restart,
}};

// Covers a full-network cold restart where one validator loses its simplex DB:
// restart a surviving quorum first, require them to finalize a newer slot without the wiped node,
// then restart the wiped node on empty local consensus state and require it to catch up and
// keep finalizing beyond its first post-restart observation.
td::actor::Task<> full_network_restart_with_state_loss_scenario(HarnessHandle& h) {
  co_await h.wait_for("initial finalization", predicates::finalization_on(TimePoint::genesis(), 0));
  auto pre_restart_snapshot = co_await h.get_trace_snapshot();
  td::uint32 pre_restart_finalized_slot = highest_finalized_slot(pre_restart_snapshot);

  for (size_t node_idx : kAllNodes) {
    co_await h.stop_instance(node_idx);
  }
  co_await h.clear_instance_db(kStateLostNode);

  for (size_t node_idx : kAllNodes) {
    if (node_idx == kStateLostNode) {
      continue;
    }
    co_await h.start_instance(node_idx);
  }
  co_await h.wait_for(PSTRING() << "surviving quorum finalizes while state-lost node #" << kStateLostNode
                                << " stays down",
                      predicates::finalization_past(TimePoint{pre_restart_finalized_slot}, {kStateLostNode}));
  co_await h.start_instance(kStateLostNode);
  auto state_lost_restart_snapshot = co_await h.get_trace_snapshot();
  auto state_lost_restart_slot = highest_finalized_slot(state_lost_restart_snapshot);
  co_await h.wait_for(PSTRING() << "state-lost node #" << kStateLostNode << " finalizes after restart",
                      predicates::finalization_on(TimePoint{state_lost_restart_slot}, kStateLostNode));

  auto post_rejoin_snapshot = co_await h.get_trace_snapshot();
  auto post_rejoin_slot = highest_finalized_slot(post_rejoin_snapshot);
  co_await h.wait_for("all validators finalize after the state-lost node catches up",
                      std::make_unique<AllNodesFinalizedAfter>(post_rejoin_slot));

  auto steady_snapshot = co_await h.get_trace_snapshot();
  auto steady_slot = highest_finalized_slot(steady_snapshot, kStateLostNode);
  co_await h.wait_for(PSTRING() << "state-lost node #" << kStateLostNode << " keeps finalizing after catch-up",
                      predicates::finalization_on(TimePoint{steady_slot}, kStateLostNode));
  co_return {};
}

RegisterTestCase _full_network_restart_with_state_loss{TestCaseDescriptor{
    .name = "full-network-restart-with-state-loss-still-recovers",
    .config = reboot_config(),
    .scenario = full_network_restart_with_state_loss_scenario,
    .verify = verify_full_network_restart_with_state_loss,
    .timeout = 90.0,
}};

}  // namespace
}  // namespace ton::validator::consensus::test
