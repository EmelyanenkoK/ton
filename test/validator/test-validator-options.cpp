/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include <chrono>
#include <vector>

#include "td/utils/tests.h"
#include "ton/ton-shard.h"
#include "validator/validator-options.hpp"

namespace ton::validator {
namespace {

using namespace std::chrono_literals;

NewConsensusConfig::NoncriticalParams make_base_params() {
  NewConsensusConfig::NoncriticalParams params;
  params.target_rate = 111ms;
  params.first_block_timeout = 222ms;
  params.first_block_timeout_multiplier = 1.25;
  params.first_block_timeout_cap = 333ms;
  params.candidate_resolve_timeout = 444ms;
  params.candidate_resolve_timeout_multiplier = 1.5;
  params.candidate_resolve_timeout_cap = 555ms;
  params.candidate_resolve_cooldown = 666ms;
  params.standstill_timeout = 777ms;
  params.standstill_max_egress_bytes_per_s = 888;
  params.max_leader_window_desync = 999;
  params.bad_signature_ban_duration = 1111ms;
  params.candidate_resolve_rate_limit = 1234;
  return params;
}

ValidatorManagerOptionsImpl make_options(std::vector<NoncriticalParamsOverride> overrides = {}) {
  ValidatorManagerOptionsImpl opts(BlockIdExt{}, BlockIdExt{}, false, 3600, 86400, 86400, 999999, 86400.0 * 7,
                                   86400.0 * 3650, false);
  opts.set_noncritical_params_overrides(std::move(overrides));
  return opts;
}

NoncriticalParamsOverride make_override(ShardIdFull shard, td::uint32 from_seqno, td::uint32 to_seqno) {
  NoncriticalParamsOverride override;
  override.shard = shard;
  override.from_seqno = from_seqno;
  override.to_seqno = to_seqno;
  return override;
}

// Query exactly at both range boundaries and just outside them to prove the
// override seqno interval is inclusive.
TEST(ValidatorOptions, NoncriticalParamsOverrideUsesInclusiveSeqnoBounds) {
  auto shard = ShardIdFull{basechainId};
  auto override = make_override(shard, 10, 20);
  override.params.target_rate = 1500ms;

  auto opts = make_options({override});
  auto base = make_base_params();

  auto at_from = opts.get_noncritical_params(shard, 10, base);
  auto at_to = opts.get_noncritical_params(shard, 20, base);
  auto before = opts.get_noncritical_params(shard, 9, base);
  auto after = opts.get_noncritical_params(shard, 21, base);

  auto expected = base;
  expected.target_rate = 1500ms;

  EXPECT_EQ(at_from, expected);
  EXPECT_EQ(at_to, expected);
  EXPECT_EQ(before, base);
  EXPECT_EQ(after, base);
}

// Apply an override to a parent shard and resolve params for a child shard to
// prove shard ancestry matching follows shard_is_ancestor semantics.
TEST(ValidatorOptions, NoncriticalParamsOverrideMatchesDescendantShards) {
  auto parent = shard_child(ShardIdFull{basechainId}, true);
  auto descendant = shard_child(parent, false);

  auto override = make_override(parent, 7, 7);
  override.params.target_rate = 1600ms;

  auto opts = make_options({override});
  auto base = make_base_params();
  auto expected = base;
  expected.target_rate = 1600ms;

  EXPECT_EQ(opts.get_noncritical_params(descendant, 7, base), expected);
}

// Resolve params for a shard outside the override subtree to prove unrelated
// overrides leave the base config untouched.
TEST(ValidatorOptions, NonMatchingShardLeavesConfigUnchanged) {
  auto matching_parent = shard_child(ShardIdFull{basechainId}, true);
  auto non_matching_shard = shard_child(ShardIdFull{basechainId}, false);

  auto override = make_override(matching_parent, 3, 9);
  override.params.target_rate = 1700ms;

  auto opts = make_options({override});
  auto base = make_base_params();

  EXPECT_EQ(opts.get_noncritical_params(non_matching_shard, 5, base), base);
}

// Set only a subset of override fields and compare the whole struct to prove
// unspecified noncritical params are preserved from the base config.
TEST(ValidatorOptions, PartialOverridePreservesUnspecifiedFields) {
  auto shard = ShardIdFull{basechainId};
  auto override = make_override(shard, 1, 1);
  override.params.target_rate = 1800ms;
  override.params.max_leader_window_desync = 321;

  auto opts = make_options({override});
  auto base = make_base_params();
  auto expected = base;
  expected.target_rate = 1800ms;
  expected.max_leader_window_desync = 321;

  EXPECT_EQ(opts.get_noncritical_params(shard, 1, base), expected);
}

// Install overlapping overrides and resolve a matching seqno to lock in the
// current first-match-wins precedence rule.
TEST(ValidatorOptions, FirstMatchingOverrideWinsForOverlappingEntries) {
  auto shard = ShardIdFull{basechainId};

  auto first = make_override(shard, 4, 8);
  first.params.target_rate = 1900ms;
  first.params.candidate_resolve_rate_limit = 44;

  auto second = make_override(shard, 1, 10);
  second.params.target_rate = 2900ms;
  second.params.candidate_resolve_rate_limit = 99;

  auto opts = make_options({first, second});
  auto base = make_base_params();
  auto expected = base;
  expected.target_rate = 1900ms;
  expected.candidate_resolve_rate_limit = 44;

  EXPECT_EQ(opts.get_noncritical_params(shard, 5, base), expected);
}

}  // namespace
}  // namespace ton::validator
