/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#pragma once

#include "crypto/block/fixtures.h"
#include "crypto/vm/boc.h"
#include "crypto/vm/cells/CellBuilder.h"
#include "td/actor/Mocks.h"
#include "validator/consensus/bus.h"
#include "validator/consensus/chain-state.h"
#include "validator/fabric.h"

namespace ton::validator::consensus::test {

// =============================================================================
// Test data helpers
// =============================================================================

struct TestOptions {
  ShardIdFull shard = {0, 0xC000'0000'0000'0000ULL};
  std::vector<ValidatorWeight> weight_distribution = {1};
  td::uint32 slots_per_leader_window = 4;
  td::uint32 target_rate_ms = 1000;
};

inline Bits256 bits256_pattern(td::uint64 x) {
  auto data = std::to_array({x, x, x, x});
  unsigned char raw_bytes[32];
  memcpy(raw_bytes, data.data(), 32);
  return Bits256{raw_bytes};
}

inline td::Ref<vm::Cell> create_cell(td::uint32 tag) {
  return vm::CellBuilder().store_long(tag, 32).finalize_novm();
}

inline const BlockIdExt min_mc_block_id{masterchainId, shardIdAll, 0, bits256_pattern(200), bits256_pattern(201)};

class ValidatorSetup {
 public:
  ValidatorSetup() : ValidatorSetup(TestOptions{}) {
  }

  ValidatorSetup(const TestOptions& options) : options_(options) {
    size_t n_validators = options.weight_distribution.size();
    CHECK(n_validators > 0);

    for (size_t i = 0; i < n_validators; ++i) {
      PrivateKey validator_key{privkeys::Ed25519::random()};
      auto validator_public_key = validator_key.compute_public_key();

      validator_set_.push_back(PeerValidator{
          .idx = PeerValidatorId{i},
          .key = validator_public_key,
          .short_id = validator_public_key.compute_short_id(),
          .adnl_id = adnl::AdnlNodeIdShort{bits256_pattern(i + 256)},
          .weight = options.weight_distribution[i],
      });

      keys_.push_back(std::move(validator_key));
      total_weight_ += options.weight_distribution[i];
    }
  }

  ValidatorSetup(const ValidatorSetup&) = delete;

  ShardIdFull shard() const {
    return options_.shard;
  }

  const PrivateKey& key(size_t idx) const {
    return keys_[idx];
  }

  td::actor::ActorOwn<keyring::Keyring> create_keyring(size_t idx) const {
    auto keyring = keyring::Keyring::create("");
    td::actor::send_closure(keyring, &keyring::Keyring::add_key, keys_[idx], true,
                            td::PromiseCreator::lambda([](td::Result<td::Unit> R) { R.ensure(); }));
    return keyring;
  }

  void fill(consensus::Bus& bus, size_t idx) const {
    bus.session_id = bits256_pattern(1);
    bus.shard = shard();
    bus.validator_set = validator_set_;
    bus.total_weight = total_weight_;
    bus.cc_seqno = 42;
    bus.validator_set_hash = 0xDEADBEEF;
    bus.local_id = validator_set_[idx];
    bus.config = NewConsensusConfig{
        .target_rate_ms = options_.target_rate_ms,
        .consensus =
            NewConsensusConfig::Simplex{
                .slots_per_leader_window = options_.slots_per_leader_window,
            },
    };
    bus.stop_promise = [](td::Result<>) {};
  }

 private:
  TestOptions options_;
  std::vector<PeerValidator> validator_set_;
  std::vector<PrivateKey> keys_;
  ValidatorWeight total_weight_{0};
};

// =============================================================================
// Block / state construction helpers
// =============================================================================

inline td::Ref<BlockData> make_block_data(ShardIdFull shard, BlockSeqno seqno, td::Ref<vm::Cell> state_root,
                                          bool before_split = false) {
  auto state_update = block::test::make_merkle_update(state_root, state_root);
  auto cell = block::test::make_block_cell(shard, seqno, std::move(state_update), before_split);
  auto data = vm::std_boc_serialize(cell, 31).move_as_ok();
  BlockIdExt id(BlockId(shard, seqno), td::Bits256(cell->get_hash().bits()), td::sha256_bits256(data));
  return create_block(id, data.clone()).move_as_ok();
}

inline ChainStateRef make_normal_state(ShardIdFull shard, BlockSeqno seqno, BlockIdExt mc_block_id,
                                       bool before_split = false) {
  auto state = create_cell(seqno);
  auto block = make_block_data(shard, seqno, state, before_split);
  return td::make_ref<ChainState>(ChainState::NormalTip{std::move(block), std::move(state)}, mc_block_id);
}

inline GeneratedCandidate make_collation_result(const ChainStateRef& old_state, ShardIdFull shard, BlockSeqno seqno) {
  auto old_root = old_state->state()[0];
  auto new_root = create_cell(0xAE000000 + seqno);
  auto state_update = block::test::make_merkle_update(old_root, new_root);
  auto cell = block::test::make_block_cell(shard, seqno, std::move(state_update));
  auto data = vm::std_boc_serialize(cell, 31).move_as_ok();
  BlockIdExt id(BlockId(shard, seqno), td::Bits256(cell->get_hash().bits()), td::sha256_bits256(data));
  return GeneratedCandidate{
      .candidate =
          BlockCandidate{
              .pubkey = Ed25519_PublicKey{bits256_pattern(42)},
              .id = id,
              .collated_file_hash = td::sha256_bits256(td::BufferSlice("collated")),
              .data = std::move(data),
              .collated_data = td::BufferSlice("collated"),
          },
  };
}

// =============================================================================
// Candidate construction helpers
// =============================================================================

inline BlockCandidate make_block_candidate(ShardIdFull shard, BlockSeqno seqno) {
  auto cell = create_cell(0xCAFE0000 + seqno);
  auto data = vm::std_boc_serialize(cell, 31).move_as_ok();
  BlockIdExt id(BlockId(shard, seqno), td::Bits256(cell->get_hash().bits()), td::sha256_bits256(data));
  return BlockCandidate(Ed25519_PublicKey(bits256_pattern(42)), id, td::sha256_bits256(td::BufferSlice("collated")),
                        std::move(data), td::BufferSlice("collated"));
}

inline CandidateRef make_empty_candidate(BlockIdExt referenced_block, PeerValidatorId leader) {
  CandidateId parent{.slot = 0, .hash = bits256_pattern(100)};
  CandidateId id{.slot = 1, .hash = bits256_pattern(101)};
  return td::Ref<Candidate>(true, id, parent, leader, std::move(referenced_block), td::BufferSlice());
}

inline CandidateRef make_full_candidate(const BlockCandidate& bc, PeerValidatorId leader) {
  CandidateId id{.slot = 1, .hash = bits256_pattern(102)};
  return td::Ref<Candidate>(true, id, std::optional<CandidateId>{}, leader, bc.clone(), td::BufferSlice());
}

// =============================================================================
// Simple test bus (no event capture)
// =============================================================================

struct TestBus : consensus::Bus {
  using Parent = consensus::Bus;
  using Events = td::TypeList<>;

  TestBus() = default;

  void populate_collator_schedule() override {
    UNREACHABLE();
  }
};

// =============================================================================
// MockManagerFacade
// =============================================================================

struct MockManagerFacade : ManagerFacade {
  td::actor::MockAsync<ValidateCandidateResult, BlockCandidate, ValidateParams, td::Timestamp> validate;
  td::actor::MockAsync<GeneratedCandidate, CollateParams, td::CancellationToken> collate;
  std::vector<BlockIdExt> cached_candidate_ids;

  td::actor::Task<ValidateCandidateResult> validate_block_candidate(BlockCandidate c, ValidateParams p,
                                                                    td::Timestamp t) override {
    co_return co_await validate.call(std::move(c), std::move(p), t);
  }

  td::actor::Task<GeneratedCandidate> collate_block(CollateParams p, td::CancellationToken t) override {
    co_return co_await collate.call(std::move(p), std::move(t));
  }

  td::actor::Task<> accept_block(BlockIdExt, td::Ref<BlockData>, size_t, td::Ref<block::BlockSignatureSet>, int,
                                 bool) override {
    co_return td::Status::Error("not mocked");
  }

  td::actor::Task<td::Ref<vm::Cell>> wait_block_state_root(BlockIdExt, td::Timestamp) override {
    co_return td::Status::Error("not mocked");
  }

  td::actor::Task<td::Ref<BlockData>> wait_block_data(BlockIdExt, td::Timestamp) override {
    co_return td::Status::Error("not mocked");
  }

  td::actor::Task<BlockCandidate> load_block_candidate(PublicKey, BlockIdExt, FileHash) override {
    co_return td::Status::Error("not mocked");
  }

  td::actor::Task<> store_block_candidate(BlockCandidate) override {
    co_return td::Status::Error("not mocked");
  }

  void cache_block_candidate(BlockCandidate candidate) override {
    cached_candidate_ids.push_back(candidate.id);
  }
};

}  // namespace ton::validator::consensus::test
