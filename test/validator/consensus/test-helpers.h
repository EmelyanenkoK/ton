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
#include "validator/consensus/simplex/bus.h"
#include "validator/consensus/simplex/certificate.h"
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
  td::uint32 first_block_timeout_ms = 1000;
  td::uint32 max_leader_window_desync = 1;
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
                .first_block_timeout_ms = options_.first_block_timeout_ms,
                .max_leader_window_desync = options_.max_leader_window_desync,
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

inline void fill_simplex_bus(const ValidatorSetup& setup, simplex::Bus& bus, size_t idx) {
  setup.fill(bus, idx);
  bus.simplex_config = bus.config.consensus.get<NewConsensusConfig::Simplex>();
  bus.populate_collator_schedule();
}

inline td::BufferSlice sign_consensus_payload(const ValidatorSetup& setup, const consensus::Bus& bus, size_t idx,
                                              td::Slice payload) {
  auto signed_data = create_serialize_tl_object<consensus::tl::dataToSign>(bus.session_id, td::BufferSlice(payload));
  auto decryptor = setup.key(idx).create_decryptor().move_as_ok();
  return decryptor->sign(signed_data.as_slice()).move_as_ok();
}

template <typename VoteT>
simplex::Signed<VoteT> make_signed_simplex_vote(const ValidatorSetup& setup, const consensus::Bus& bus, size_t idx,
                                                VoteT vote) {
  auto vote_to_sign = serialize_tl_object(vote.to_tl(), true);
  auto signature = sign_consensus_payload(setup, bus, idx, vote_to_sign.as_slice());
  return simplex::Signed<VoteT>{PeerValidatorId{idx}, std::move(vote), std::move(signature)};
}

template <typename VoteT>
td::Ref<simplex::Certificate<VoteT>> make_simplex_certificate(const ValidatorSetup& setup, const consensus::Bus& bus,
                                                              VoteT vote, std::initializer_list<size_t> signers) {
  std::vector<typename simplex::Certificate<VoteT>::VoteSignature> signatures;
  auto vote_to_sign = serialize_tl_object(vote.to_tl(), true);
  for (size_t idx : signers) {
    signatures.push_back(typename simplex::Certificate<VoteT>::VoteSignature{
        .validator = PeerValidatorId{idx},
        .signature = sign_consensus_payload(setup, bus, idx, vote_to_sign.as_slice()),
    });
  }
  return td::make_ref<simplex::Certificate<VoteT>>(std::move(vote), std::move(signatures));
}

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

inline CandidateRef make_serializable_empty_candidate(const ValidatorSetup& setup, const consensus::Bus& bus,
                                                      td::uint32 slot, CandidateId parent, BlockIdExt referenced_block,
                                                      PeerValidatorId leader) {
  auto hash_data = CandidateHashData::create_empty(referenced_block, parent);
  auto id = hash_data.build_id_with(slot);
  auto id_to_sign = serialize_tl_object(id.to_tl(), true);
  auto signature = sign_consensus_payload(setup, bus, leader.value(), id_to_sign.as_slice());
  return td::make_ref<Candidate>(id, ParentId{parent}, leader, std::move(referenced_block), std::move(signature));
}

inline CandidateRef make_serializable_full_candidate(const ValidatorSetup& setup, const consensus::Bus& bus,
                                                     td::uint32 slot, ParentId parent, const BlockCandidate& block,
                                                     PeerValidatorId leader) {
  auto hash_data = CandidateHashData::create_full(block, parent);
  auto id = hash_data.build_id_with(slot);
  auto id_to_sign = serialize_tl_object(id.to_tl(), true);
  auto signature = sign_consensus_payload(setup, bus, leader.value(), id_to_sign.as_slice());
  return td::make_ref<Candidate>(id, std::move(parent), leader, block.clone(), std::move(signature));
}

inline CandidateId make_candidate_id(td::uint32 slot, td::uint64 pattern) {
  return CandidateId{.slot = slot, .hash = bits256_pattern(pattern)};
}

inline CandidateRef make_wait_candidate(CandidateId id, ParentId parent, PeerValidatorId leader = PeerValidatorId{0}) {
  return td::make_ref<Candidate>(id, std::move(parent), leader, min_mc_block_id, td::BufferSlice("sig"));
}

inline td::Result<simplex::Signed<simplex::Vote>> decode_simplex_signed_vote(const ProtocolMessage& message,
                                                                             PeerValidatorId src,
                                                                             const consensus::Bus& bus) {
  return simplex::Signed<simplex::Vote>::deserialize(message.data.as_slice(), src, bus);
}

inline td::Result<td::Ref<simplex::Certificate<simplex::Vote>>> decode_simplex_certificate(
    const ProtocolMessage& message, const consensus::Bus& bus) {
  TRY_RESULT(tl_cert, fetch_tl_object<simplex::tl::certificate>(message.data, true));
  return simplex::Certificate<simplex::Vote>::from_tl(std::move(*tl_cert), bus);
}

class TestDbImpl : public consensus::Db {
 public:
  void seed(td::BufferSlice key, td::BufferSlice value) {
    map_[std::move(key)] = std::move(value);
  }

  void seed_all(const std::vector<std::pair<td::BufferSlice, td::BufferSlice>>& entries) {
    for (const auto& [key, value] : entries) {
      seed(key.clone(), value.clone());
    }
  }

  std::vector<std::pair<td::BufferSlice, td::BufferSlice>> entries() const {
    std::vector<std::pair<td::BufferSlice, td::BufferSlice>> result;
    result.reserve(map_.size());
    for (const auto& [key, value] : map_) {
      result.emplace_back(key.clone(), value.clone());
    }
    return result;
  }

  size_t size() const {
    return map_.size();
  }

  std::optional<td::BufferSlice> get(td::Slice key) const override {
    auto it = map_.find(td::BufferSlice{key});
    if (it == map_.end()) {
      return std::nullopt;
    }
    return it->second.clone();
  }

  std::vector<std::pair<td::BufferSlice, td::BufferSlice>> get_by_prefix(td::uint32 prefix) const override {
    std::vector<std::pair<td::BufferSlice, td::BufferSlice>> result;
    td::BufferSlice begin{reinterpret_cast<const char*>(&prefix), 4};
    td::uint32 prefix2 = prefix + 1;
    td::BufferSlice end{reinterpret_cast<const char*>(&prefix2), 4};
    for (auto it = map_.lower_bound(begin); it != map_.end() && it->first < end; ++it) {
      result.emplace_back(it->first.clone(), it->second.clone());
    }
    return result;
  }

  td::actor::Task<> set(td::BufferSlice key, td::BufferSlice value) override {
    map_[std::move(key)] = std::move(value);
    co_return {};
  }

 private:
  std::map<td::BufferSlice, td::BufferSlice> map_;
};

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

// Allows tests to publish FinalizeBlock requests so listeners can observe the event,
// without pulling in the full block acceptance actor and its extra side effects.
class TestFinalizeBlockResponderImpl : public td::actor::SpawnsWith<consensus::Bus>,
                                       public td::actor::ConnectsTo<consensus::Bus> {
 public:
  TON_RUNTIME_DEFINE_EVENT_HANDLER();

  template <>
  void handle(BusHandle, std::shared_ptr<const StopRequested>) {
    stop();
  }

  template <>
  td::actor::Task<> process(BusHandle, std::shared_ptr<FinalizeBlock>) {
    co_return td::Unit{};
  }
};

struct TestFinalizeBlockResponder {
  static void register_in(td::actor::Runtime& runtime) {
    runtime.register_actor<TestFinalizeBlockResponderImpl>("TestFinalizeBlockResponder");
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
