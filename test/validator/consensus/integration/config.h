/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */
#pragma once

#include <optional>
#include <set>
#include <vector>

#include "ton/ton-types.h"
#include "validator/consensus/types.h"

namespace ton::validator::consensus::test {

using Range = std::pair<double, double>;

struct TestConfig {
  // Validator setup
  size_t n_nodes = 8;
  std::vector<ValidatorWeight> node_weights_override;
  ShardIdFull shard{basechainId, shardIdAll};

  // Consensus config (passed directly to Bus::config)
  NewConsensusConfig consensus_config = {
      .max_block_size = 1 << 20,
      .max_collated_data_size = 1 << 20,
      .slots_per_leader_window = 4,
      .noncritical_params = {.target_rate = std::chrono::milliseconds{1000},
                             .first_block_timeout = std::chrono::milliseconds{1000},
                             .first_block_timeout_multiplier = 1.05,
                             .first_block_timeout_cap = std::chrono::milliseconds{100'000},
                             .standstill_timeout = std::chrono::milliseconds{10'000}}};

  // Network simulation (initial values, changeable at runtime via HarnessHandle)
  Range net_ping = {0.05, 0.1};
  double net_loss = 0.0;
  Range db_delay = {0.0, 0.0};
  Range collation_time = {0.0, 0.0};
  Range validation_time = {0.0, 0.0};

  // Helpers
  ValidatorWeight node_weight(size_t idx) const {
    if (!node_weights_override.empty()) {
      CHECK(idx < node_weights_override.size());
      return node_weights_override[idx];
    }
    return 11;
  }

  ValidatorWeight total_weight() const {
    ValidatorWeight w = 0;
    for (size_t i = 0; i < n_nodes; ++i) {
      w += node_weight(i);
    }
    return w;
  }

  ValidatorWeight quorum_weight() const {
    return (total_weight() * 2) / 3 + 1;
  }
};

// A point in the protocol's timeline, captured when an externally-observable
// action (stop, start, filter change) happens. Predicates use it to ignore
// events that occurred before the action.
struct TimePoint {
  td::uint32 slot = 0;

  static TimePoint genesis() {
    return {0};
  }
};

// Message filtering rules applied by the test network.
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
    std::set<size_t> dst_node_indices;
    td::uint32 start_slot = 0;
    td::uint32 end_slot = 0;
    ProtocolKind kind = ProtocolKind::NotarVote;
  };

  struct CandidateDropRule {
    std::optional<size_t> src_node_idx;
    std::set<size_t> dst_node_indices;
    td::uint32 start_slot = 0;
    td::uint32 end_slot = 0;
  };

  std::vector<ProtocolDropRule> protocol_drop_rules;
  std::vector<CandidateDropRule> candidate_drop_rules;
  std::optional<size_t> force_candidate_request_src_node;
  std::optional<size_t> force_candidate_request_dst_node;
  std::optional<size_t> malformed_candidate_response_src_node;
  std::optional<size_t> malformed_candidate_response_dst_node;
  size_t malformed_candidate_response_budget = 0;
  MalformedCandidateResponseKind malformed_candidate_response_kind = MalformedCandidateResponseKind::Junk;
};

}  // namespace ton::validator::consensus::test
