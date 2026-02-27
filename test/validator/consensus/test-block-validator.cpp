/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "crypto/vm/boc.h"
#include "crypto/vm/cells/CellBuilder.h"
#include "crypto/vm/cellslice.h"
#include "td/actor/BusRuntime.h"
#include "td/actor/TestScheduler.h"
#include "td/utils/tests.h"
#include "validator/consensus/bus.h"
#include "validator/consensus/chain-state.h"
#include "validator/fabric.h"

namespace ton::validator::consensus {
namespace {

// =============================================================================
// MockAsync<R, Args...>
// =============================================================================

template <typename R, typename... Args>
class MockAsync {
 public:
  using CallArgs = std::tuple<std::decay_t<Args>...>;

  struct PendingCall {
    CallArgs args;
    typename td::actor::StartedTask<R>::ExternalPromise respond;
  };

  void returns(R result) {
    results_.push_back(std::move(result));
  }

  td::actor::StartedTask<PendingCall> expect() {
    CHECK(!has_pending_expect_);
    auto [task, promise] = td::actor::StartedTask<PendingCall>::make_bridge();
    pending_expect_ = std::move(promise);
    has_pending_expect_ = true;
    return std::move(task);
  }

  td::actor::Task<R> call(Args... args) {
    call_count_++;
    if (has_pending_expect_) {
      has_pending_expect_ = false;
      auto [result_task, result_promise] = td::actor::StartedTask<R>::make_bridge();
      auto pe = std::exchange(pending_expect_, {});
      pe.set_value(PendingCall{CallArgs(std::move(args)...), std::move(result_promise)});
      co_return co_await std::move(result_task);
    }
    CHECK(!results_.empty());
    auto result = std::move(results_.front());
    results_.pop_front();
    co_return std::move(result);
  }

  int call_count() const {
    return call_count_;
  }

 private:
  std::deque<R> results_;
  typename td::actor::StartedTask<PendingCall>::ExternalPromise pending_expect_;
  bool has_pending_expect_ = false;
  int call_count_ = 0;
};

// =============================================================================
// MockManagerFacade
// =============================================================================

struct MockManagerFacade : ManagerFacade {
  MockAsync<ValidateCandidateResult, BlockCandidate, ValidateParams, td::Timestamp> validate;

  td::actor::Task<ValidateCandidateResult> validate_block_candidate(BlockCandidate c, ValidateParams p,
                                                                    td::Timestamp t) override {
    co_return co_await validate.call(std::move(c), std::move(p), t);
  }

  td::actor::Task<GeneratedCandidate> collate_block(CollateParams, td::CancellationToken) override {
    co_return td::Status::Error("not mocked");
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
};

// =============================================================================
// Test helpers
// =============================================================================

struct TestOptions {
  ShardIdFull shard = {0, 0xC000'0000'0000'0000ULL};
  std::vector<ValidatorWeight> weight_distribution = {1};
  td::uint32 slots_per_leader_window = 4;
};

Bits256 bits256_pattern(td::uint64 x) {
  auto data = std::to_array({x, x, x, x});
  unsigned char raw_bytes[32];
  memcpy(raw_bytes, data.data(), 32);
  return Bits256{raw_bytes};
}

td::Ref<vm::Cell> create_cell(td::uint32 tag) {
  return vm::CellBuilder().store_long(tag, 32).finalize_novm();
}

td::Ref<BlockData> make_block_data(ShardIdFull shard, BlockSeqno seqno) {
  auto cell = create_cell(0x11ef55aa + seqno);
  auto data = vm::std_boc_serialize(cell, 31).move_as_ok();
  BlockIdExt id(BlockId(shard, seqno), td::Bits256(cell->get_hash().bits()), td::sha256_bits256(data));
  return create_block(id, data.clone()).move_as_ok();
}

ChainStateRef make_normal_state(ShardIdFull shard, BlockSeqno seqno, BlockIdExt min_mc_block_id) {
  auto block = make_block_data(shard, seqno);
  auto state = create_cell(seqno);
  return td::make_ref<ChainState>(ChainState::NormalTip{std::move(block), std::move(state)}, min_mc_block_id);
}

BlockCandidate make_block_candidate(ShardIdFull shard, BlockSeqno seqno) {
  auto cell = create_cell(0xCAFE0000 + seqno);
  auto data = vm::std_boc_serialize(cell, 31).move_as_ok();
  BlockIdExt id(BlockId(shard, seqno), td::Bits256(cell->get_hash().bits()), td::sha256_bits256(data));
  return BlockCandidate(Ed25519_PublicKey(bits256_pattern(42)), id, td::sha256_bits256(td::BufferSlice("collated")),
                        std::move(data), td::BufferSlice("collated"));
}

CandidateRef make_empty_candidate(BlockIdExt referenced_block, PeerValidatorId leader) {
  CandidateId parent{.slot = 0, .hash = bits256_pattern(100)};
  CandidateId id{.slot = 1, .hash = bits256_pattern(101)};
  return td::Ref<Candidate>(true, id, parent, leader, std::move(referenced_block), td::BufferSlice());
}

CandidateRef make_full_candidate(const BlockCandidate& bc, PeerValidatorId leader) {
  CandidateId id{.slot = 1, .hash = bits256_pattern(102)};
  return td::Ref<Candidate>(true, id, std::optional<CandidateId>{}, leader, bc.clone(), td::BufferSlice());
}

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

      total_weight_ += options.weight_distribution[i];
    }
  }

  ValidatorSetup(const ValidatorSetup&) = delete;

  ShardIdFull shard() const {
    return options_.shard;
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
  ValidatorWeight total_weight_{0};
};

struct TestBus : consensus::Bus {
  using Parent = consensus::Bus;
  using Events = td::TypeList<>;

  TestBus() = default;

  void populate_collator_schedule() override {
    UNREACHABLE();
  }
};

const BlockIdExt min_mc_block_id{masterchainId, shardIdAll, 0, bits256_pattern(200), bits256_pattern(201)};

// =============================================================================
// BlockValidatorTest base class
// =============================================================================

class BlockValidatorTest : public td::Test {
 protected:
  ValidatorSetup ctx_{TestOptions{}};
  td::actor::TestScheduler ts_;
  MockManagerFacade* mock_{};
  td::actor::BusHandle<consensus::Bus> handle_;

  void run() final {
    ts_.run([this]() -> td::actor::Task<td::Unit> {
      auto mock_own = td::actor::create_actor<MockManagerFacade>("mock");
      mock_ = &mock_own.get_actor_unsafe();

      auto bus = std::make_shared<TestBus>();
      ctx_.fill(*bus, 0);
      bus->manager = mock_own.get();

      td::actor::Runtime runtime;
      BlockValidator::register_in(runtime);
      handle_ = runtime.start<consensus::Bus>(std::move(bus));

      co_return co_await run_test();
    });
  }

  virtual td::actor::Task<td::Unit> run_test() = 0;
};

// =============================================================================
// Tests
// =============================================================================

struct AcceptsGoodEmptyCandidates : BlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    auto state = make_normal_state(ctx_.shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto block_id = state->as_normal().value();
    auto candidate = make_empty_candidate(block_id, PeerValidatorId{0});

    auto result = co_await handle_.publish<ValidationRequest>(state, candidate);

    EXPECT(result.has<CandidateAccept>());
    EXPECT_EQ(mock_->validate.call_count(), 0);

    co_return {};
  }
};
REGISTER_TEST(BlockValidator, AcceptsGoodEmptyCandidates);

struct RejectsEmptyCandidatesWithWrongBlock : BlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    auto state = make_normal_state(ctx_.shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto wrong_block = make_block_data(ctx_.shard(), 99)->block_id();
    auto candidate = make_empty_candidate(wrong_block, PeerValidatorId{0});

    auto result = co_await handle_.publish<ValidationRequest>(state, candidate);

    EXPECT(result.has<CandidateReject>());
    EXPECT_EQ(mock_->validate.call_count(), 0);

    co_return {};
  }
};
REGISTER_TEST(BlockValidator, RejectsEmptyCandidatesWithWrongBlock);

struct AcceptsFullCandidates : BlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    mock_->validate.returns(CandidateAccept{});

    auto state = make_normal_state(ctx_.shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto bc = make_block_candidate(ctx_.shard(), 2);
    auto candidate = make_full_candidate(bc, PeerValidatorId{0});

    auto result = co_await handle_.publish<ValidationRequest>(state, candidate);

    EXPECT(result.has<CandidateAccept>());
    EXPECT_EQ(mock_->validate.call_count(), 1);

    co_return {};
  }
};
REGISTER_TEST(BlockValidator, AcceptsFullCandidates);

struct RejectsFullCandidates : BlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    mock_->validate.returns(CandidateReject{.reason = "bad block", .proof = td::BufferSlice()});

    auto state = make_normal_state(ctx_.shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto bc = make_block_candidate(ctx_.shard(), 2);
    auto candidate = make_full_candidate(bc, PeerValidatorId{0});

    auto result = co_await handle_.publish<ValidationRequest>(state, candidate);

    EXPECT(result.has<CandidateReject>());
    EXPECT_EQ(result.get<CandidateReject>().reason, "bad block");

    co_return {};
  }
};
REGISTER_TEST(BlockValidator, RejectsFullCandidates);

struct WaitsUntilGenUtime : BlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    double future_utime = td::Time::system_now() + 5.0;
    mock_->validate.returns(CandidateAccept{.ok_from_utime = future_utime});

    auto state = make_normal_state(ctx_.shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto bc = make_block_candidate(ctx_.shard(), 2);
    auto candidate = make_full_candidate(bc, PeerValidatorId{0});

    auto task = handle_.publish<ValidationRequest>(state, candidate).start();

    co_await ts_.wait_sync_work();

    EXPECT(!task.await_ready());
    EXPECT(ts_.next_timeout_in() == 5.0);

    ts_.advance_time(ts_.next_timeout_in());

    auto result = co_await std::move(task);
    EXPECT(result.has<CandidateAccept>());

    co_return {};
  }
};
REGISTER_TEST(BlockValidator, WaitsUntilGenUtime);

}  // namespace
}  // namespace ton::validator::consensus
