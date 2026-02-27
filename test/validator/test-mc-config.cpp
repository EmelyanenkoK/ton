/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "block/block-auto.h"
#include "block/mc-config.h"
#include "crypto/vm/cells/CellBuilder.h"
#include "td/utils/tests.h"

namespace ton {
namespace {

using namespace block;
using vm::CellBuilder;

td::Ref<vm::Cell> make_simplex_config_v2(bool use_quic, td::uint32 slots_per_leader_window,
                                          std::vector<std::pair<int, td::uint32>> noncritical_params) {
  // Build noncritical_params HashmapE 8 uint32 using a regular Dictionary for mutation.
  vm::Dictionary dict{8};
  for (auto& [key, value] : noncritical_params) {
    CellBuilder val_cb;
    val_cb.store_long(value, 32);
    dict.set_builder(td::BitArray<8>(key), val_cb);
  }

  // Pack simplex_config_v2#22
  gen::NewConsensusConfig::Record_simplex_config_v2 rec;
  rec.flags = 0;
  rec.use_quic = use_quic;
  rec.slots_per_leader_window = slots_per_leader_window;
  rec.noncritical_params = dict.get_root();

  td::Ref<vm::Cell> cell;
  CHECK(gen::t_NewConsensusConfig.cell_pack(cell, rec));
  return cell;
}

td::Ref<vm::Cell> make_new_consensus_config_all(td::Ref<vm::Cell> mc, td::Ref<vm::Cell> shard) {
  // Pack new_consensus_config_all#10 mc:(Maybe ^NewConsensusConfig) shard:(Maybe ^NewConsensusConfig)
  CellBuilder cb;
  cb.store_long(0x10, 8);  // tag

  if (mc.not_null()) {
    cb.store_bool_bool(true);
    cb.store_ref(std::move(mc));
  } else {
    cb.store_bool_bool(false);
  }

  if (shard.not_null()) {
    cb.store_bool_bool(true);
    cb.store_ref(std::move(shard));
  } else {
    cb.store_bool_bool(false);
  }

  return cb.finalize_novm();
}

td::Ref<vm::Cell> make_consensus_config_v4() {
  // consensus_config_v4#d9 ...
  CellBuilder cb;
  cb.store_long(0xd9, 8);       // tag
  cb.store_long(1, 8);          // round_candidates
  cb.store_long(3, 8);          // next_candidate_delay_ms
  cb.store_long(2000, 32);      // consensus_timeout_ms
  cb.store_long(16000, 32);     // fast_attempts
  cb.store_long(3, 32);         // attempt_duration
  cb.store_long(8, 32);         // catchain_max_deps
  cb.store_long(4, 32);         // max_block_bytes
  cb.store_long(4 << 20, 32);   // max_block_size
  cb.store_long(4 << 20, 32);   // max_collated_data_size
  cb.store_long(5, 16);         // proto_version
  cb.store_long(0, 32);         // catchain_max_blocks_coeff
  return cb.finalize_novm();
}

std::unique_ptr<Config> make_config_with_params(td::Ref<vm::Cell> param29, td::Ref<vm::Cell> param30) {
  vm::Dictionary dict{32};
  if (param29.not_null()) {
    dict.set_ref(td::BitArray<32>{29}, param29);
  }
  if (param30.not_null()) {
    dict.set_ref(td::BitArray<32>{30}, param30);
  }

  auto dict_root = dict.get_root_cell();
  // Config constructor stores config_root; unpack() creates config_dict from it.
  auto config = std::make_unique<Config>(std::move(dict_root));
  auto status = config->unpack();
  CHECK(status.is_ok());
  return config;
}

TEST(McConfig, SimplexConfigV2ParsesNoncriticalParams) {
  auto mc_cell = make_simplex_config_v2(
      /*use_quic=*/true,
      /*slots_per_leader_window=*/2,
      {
          {0, 500},    // target_rate: 500ms
          {1, 2000},   // first_block_timeout: 2000ms
          {7, 50},     // candidate_resolve_cooldown: 50ms
          {10, 100},   // max_leader_window_desync: 100
      });

  auto param30 = make_new_consensus_config_all(mc_cell, {});
  auto param29 = make_consensus_config_v4();
  auto config = make_config_with_params(param29, param30);

  auto result = config->get_new_consensus_config(masterchainId);
  ASSERT_TRUE(bool(result));
  auto& c = result.value();

  EXPECT_EQ(c.use_quic, true);
  EXPECT_EQ(c.slots_per_leader_window, static_cast<td::uint32>(2));
  EXPECT_EQ(c.max_block_size, static_cast<td::uint32>(4 << 20));
  EXPECT_EQ(c.max_collated_data_size, static_cast<td::uint32>(4 << 20));

  using namespace std::chrono_literals;
  EXPECT_EQ(c.noncritical_params.target_rate, 500ms);
  EXPECT_EQ(c.noncritical_params.first_block_timeout, 2000ms);
  EXPECT_EQ(c.noncritical_params.candidate_resolve_cooldown, 50ms);
  EXPECT_EQ(c.noncritical_params.max_leader_window_desync, static_cast<td::uint32>(100));

  // Params not in the dictionary should retain defaults.
  EXPECT_EQ(c.noncritical_params.standstill_timeout, 10'000ms);
  EXPECT_EQ(c.noncritical_params.candidate_resolve_timeout, 1'000ms);
}

TEST(McConfig, SimplexConfigV2EmptyDictUsesDefaults) {
  auto mc_cell = make_simplex_config_v2(/*use_quic=*/false, /*slots_per_leader_window=*/4, {});

  auto param30 = make_new_consensus_config_all(mc_cell, {});
  auto param29 = make_consensus_config_v4();
  auto config = make_config_with_params(param29, param30);

  auto result = config->get_new_consensus_config(masterchainId);
  ASSERT_TRUE(bool(result));
  auto& c = result.value();

  EXPECT_EQ(c.use_quic, false);
  EXPECT_EQ(c.slots_per_leader_window, static_cast<td::uint32>(4));

  // All noncritical params should be at their defaults.
  NewConsensusConfig::NoncriticalParams defaults;
  EXPECT(c.noncritical_params == defaults);
}

TEST(McConfig, SimplexConfigV2ShardAndMcParseSeparately) {
  auto mc_cell = make_simplex_config_v2(true, 1, {{0, 200}});       // mc: target_rate=200ms
  auto shard_cell = make_simplex_config_v2(false, 4, {{0, 800}});   // shard: target_rate=800ms

  auto param30 = make_new_consensus_config_all(mc_cell, shard_cell);
  auto param29 = make_consensus_config_v4();
  auto config = make_config_with_params(param29, param30);

  auto mc = config->get_new_consensus_config(masterchainId);
  ASSERT_TRUE(bool(mc));

  using namespace std::chrono_literals;
  EXPECT_EQ(mc.value().noncritical_params.target_rate, 200ms);
  EXPECT_EQ(mc.value().use_quic, true);
  EXPECT_EQ(mc.value().slots_per_leader_window, static_cast<td::uint32>(1));

  auto shard = config->get_new_consensus_config(basechainId);
  ASSERT_TRUE(bool(shard));
  EXPECT_EQ(shard.value().noncritical_params.target_rate, 800ms);
  EXPECT_EQ(shard.value().use_quic, false);
  EXPECT_EQ(shard.value().slots_per_leader_window, static_cast<td::uint32>(4));
}

TEST(McConfig, SimplexConfigV2DoubleParamEncodedAsFloat) {
  // The first_block_timeout_multiplier (param 2) is a double stored as float bits.
  float multiplier = 1.5f;
  td::uint32 multiplier_bits;
  memcpy(&multiplier_bits, &multiplier, sizeof(float));

  auto mc_cell = make_simplex_config_v2(true, 1, {{2, multiplier_bits}});

  auto param30 = make_new_consensus_config_all(mc_cell, {});
  auto param29 = make_consensus_config_v4();
  auto config = make_config_with_params(param29, param30);

  auto result = config->get_new_consensus_config(masterchainId);
  ASSERT_TRUE(bool(result));
  EXPECT_EQ(result.value().noncritical_params.first_block_timeout_multiplier, 1.5);
}

TEST(McConfig, MissingParam30ReturnsEmpty) {
  auto param29 = make_consensus_config_v4();
  auto config = make_config_with_params(param29, {});

  auto result = config->get_new_consensus_config(masterchainId);
  EXPECT(!bool(result));
}

}  // namespace
}  // namespace ton
