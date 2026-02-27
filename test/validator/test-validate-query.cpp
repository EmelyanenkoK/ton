/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "common/checksum.h"
#include "validator/impl/validate-query.hpp"

#include "td/actor/core/ActorExecuteContext.h"
#include "td/utils/tests.h"

namespace ton::validator {
namespace {

td::Bits256 bits256_pattern(td::uint32 seed) {
  td::Bits256 bits;
  bits.as_array().fill(0);
  auto slice = bits.as_slice();
  slice.truncate(sizeof(seed)).copy_from(td::Slice{reinterpret_cast<const td::uint8*>(&seed), sizeof(seed)});
  return bits;
}

BlockCandidate make_minimal_block_candidate(ShardIdFull shard, BlockSeqno seqno) {
  auto cell = vm::CellBuilder{}.store_long(seqno, 32).finalize_novm();
  auto data = vm::std_boc_serialize(cell, 31).move_as_ok();
  BlockIdExt id(BlockId(shard, seqno), td::Bits256(cell->get_hash().bits()), td::sha256_bits256(data));
  return BlockCandidate(Ed25519_PublicKey(bits256_pattern(42)), id, td::sha256_bits256(td::BufferSlice("collated")),
                        std::move(data), td::BufferSlice("collated"));
}

std::unique_ptr<ValidateQuery> make_query_for_utime_check(bool allow_same_timestamp, UnixTime now, UnixTime prev_utime,
                                                          UnixTime mc_utime) {
  ShardIdFull shard{basechainId, shardIdAll};

  ValidateParams params;
  params.shard = shard;
  params.min_masterchain_block_id = BlockIdExt{};
  params.prev.push_back(make_minimal_block_candidate(shard, 1).id);

  auto query = std::make_unique<ValidateQuery>(
      make_minimal_block_candidate(shard, 2), std::move(params), td::actor::ActorId<ValidatorManager>{},
      td::Timestamp::never(), td::Promise<ValidateCandidateResult>());
  query->perf_timer_.start_at_ = 0;
  query->perf_timer_.callback_ = {};
  query->perf_timer_.name_.clear();
  query->allow_same_timestamp_ = allow_same_timestamp;
  query->now_ = now;
  query->start_lt_ = 101;
  query->end_lt_ = 101;
  query->ps_.utime_ = prev_utime;
  query->ps_.lt_ = 100;
  query->config_ = std::make_unique<block::ConfigInfo>(td::Ref<vm::Cell>{}, 0);
  query->config_->utime = mc_utime;
  query->config_->lt = 50;
  query->block_limits_ = std::make_unique<block::BlockLimits>();
  query->block_limits_->lt_delta = block::ParamLimits(1, 1000, 1000);
  return query;
}

struct SameTimestampAsParentFailsBeforeGlobalVersion13 : td::Test {
  bool step() override {
    // Covers tracker Kernel-29 at the owning validation gate:
    // before protocol v13, a block with the same timestamp as its parent must be rejected.
    auto query = make_query_for_utime_check(false, 100, 100, 99);
    td::actor::core::ActorExecuteContext ctx(query.get());
    td::actor::core::ActorExecuteContext::Guard guard(&ctx);
    EXPECT(!query->check_utime_lt());
    return false;
  }
};
REGISTER_TEST(ValidateQuery, SameTimestampAsParentFailsBeforeGlobalVersion13);

struct SameTimestampAsParentPassesFromGlobalVersion13 : td::Test {
  bool step() override {
    // Covers tracker Kernel-29 at the owning validation gate:
    // from protocol v13 onward, equality with the parent timestamp is allowed.
    auto query = make_query_for_utime_check(true, 100, 100, 99);
    td::actor::core::ActorExecuteContext ctx(query.get());
    td::actor::core::ActorExecuteContext::Guard guard(&ctx);
    EXPECT(query->check_utime_lt());
    return false;
  }
};
REGISTER_TEST(ValidateQuery, SameTimestampAsParentPassesFromGlobalVersion13);

}  // namespace
}  // namespace ton::validator
