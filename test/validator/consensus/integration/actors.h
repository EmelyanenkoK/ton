/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */
#pragma once

#include "block/block.h"
#include "consensus/bus.h"
#include "consensus/simplex/bus.h"
#include "td/actor/BusRuntime.h"

#include "config.h"
#include "network.h"
#include "trace.h"

namespace ton::validator::consensus::test {

class TestConsensus;

// SimplexDb actor: persists votes and certificates, restores bootstrap state.
class TestSimplexDb : public td::actor::SpawnsWith<simplex::Bus>, public td::actor::ConnectsTo<simplex::Bus> {
 public:
  using BusHandle = simplex::BusHandle;
  TON_RUNTIME_DEFINE_EVENT_HANDLER();

  explicit TestSimplexDb(simplex::Bus& bus);

  template <>
  void handle(BusHandle, std::shared_ptr<const StopRequested>);
  template <>
  td::actor::Task<> process(BusHandle, std::shared_ptr<simplex::BroadcastVote> event);
  template <>
  td::actor::Task<> process(BusHandle, std::shared_ptr<simplex::SaveCertificate> event);
  template <>
  td::actor::Task<> process(BusHandle, std::shared_ptr<simplex::LeaderWindowObserved> event);

 private:
  void init_pool_state(simplex::Bus& bus);
  void init_votes(simplex::Bus& bus);

  const td::BufferSlice pool_state_key_ = create_serialize_tl_object<ton_api::consensus_simplex_db_key_poolState>();
  std::set<Bits256> saved_votes_;
  td::uint32 first_nonannounced_window_ = 0;
  td::int64 next_seqno_ = 0;
};

// Observer actor: forwards Bus events to TraceSink.
class TestSimplexObserver : public td::actor::SpawnsWith<simplex::Bus>, public td::actor::ConnectsTo<simplex::Bus> {
 public:
  using BusHandle = simplex::BusHandle;
  TON_RUNTIME_DEFINE_EVENT_HANDLER();

  template <>
  void handle(BusHandle, std::shared_ptr<const StopRequested>);
  template <>
  void handle(BusHandle bus, std::shared_ptr<const simplex::LeaderWindowObserved> event);
  template <>
  void handle(BusHandle bus, std::shared_ptr<const OurLeaderWindowStarted> event);
  template <>
  void handle(BusHandle bus, std::shared_ptr<const CandidateGenerated> event);
  template <>
  void handle(BusHandle bus, std::shared_ptr<const simplex::NotarizationObserved> event);
  template <>
  void handle(BusHandle bus, std::shared_ptr<const simplex::FinalizationObserved> event);
  template <>
  void handle(BusHandle bus, std::shared_ptr<const MisbehaviorReport> event);
};

// In-memory DB with snapshot isolation and configurable latency.
class TestDbImpl : public consensus::Db {
 public:
  struct DbInner {
    std::map<td::BufferSlice, td::BufferSlice> map;
    std::mutex mutex;
  };

  explicit TestDbImpl(std::shared_ptr<DbInner> db, Range db_delay);
  ~TestDbImpl() override = default;

  void disable();

  std::optional<td::BufferSlice> get(td::Slice key) const override;
  std::vector<std::pair<td::BufferSlice, td::BufferSlice>> get_by_prefix(td::uint32 prefix) const override;
  td::actor::Task<> set(td::BufferSlice key, td::BufferSlice value) override;
  td::actor::Task<> close() override;

 private:
  std::map<td::BufferSlice, td::BufferSlice> snapshot_;
  std::shared_ptr<DbInner> db_;
  Range db_delay_;
  bool disabled_ = false;
};

// Manager facade: handles collation and validation with synthetic blocks.
class TestManagerFacade : public ManagerFacade {
 public:
  TestManagerFacade(size_t node_idx, td::Ref<block::ValidatorSet> validator_set,
                    td::actor::ActorId<TestConsensus> test_consensus, const TestConfig& config);

  td::actor::Task<GeneratedCandidate> collate_block(CollateParams params,
                                                    td::CancellationToken cancellation_token) override;
  td::actor::Task<ValidateCandidateResult> validate_block_candidate(BlockCandidate candidate, ValidateParams params,
                                                                    td::Timestamp timeout) override;
  td::actor::Task<> accept_block(BlockIdExt id, td::Ref<BlockData> data, size_t creator_idx,
                                 td::Ref<block::BlockSignatureSet> signatures, int send_broadcast_mode,
                                 bool apply) override;
  td::actor::Task<td::Ref<vm::Cell>> wait_block_state_root(BlockIdExt block_id, td::Timestamp timeout) override;
  td::actor::Task<td::Ref<BlockData>> wait_block_data(BlockIdExt block_id, td::Timestamp timeout) override;

 private:
  size_t node_idx_;
  td::Ref<block::ValidatorSet> validator_set_;
  td::actor::ActorId<TestConsensus> test_consensus_;
  const TestConfig& config_;
};

// Helpers used by TestManagerFacade.
td::Ref<vm::Cell> gen_shard_state(BlockSeqno seqno);
td::Ref<vm::Cell> make_ext_blk_ref(BlockIdExt block_id, LogicalTime lt);

// Well-known constants.
extern const CatchainSeqno CC_SEQNO;
extern const BlockIdExt MIN_MC_BLOCK_ID;
extern const td::Bits256 SESSION_ID;
BlockIdExt first_parent(ShardIdFull shard);

}  // namespace ton::validator::consensus::test
