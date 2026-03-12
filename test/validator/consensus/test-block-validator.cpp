/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "td/actor/TestScheduler.h"
#include "td/utils/tests.h"

#include "test-helpers.h"

namespace ton::validator::consensus::test {
namespace {

// =============================================================================
// BlockValidatorTest base class
// =============================================================================

class BlockValidatorTest : public td::Test {
 protected:
  std::unique_ptr<ValidatorSetup> ctx_;
  td::actor::TestScheduler ts_;
  MockManagerFacade* mock_{};
  td::actor::BusHandle<consensus::Bus> handle_;

  virtual TestOptions options() const {
    return TestOptions{};
  }

  ValidatorSetup& ctx() {
    CHECK(ctx_ != nullptr);
    return *ctx_;
  }

  void run() final {
    ts_.run([this]() -> td::actor::Task<td::Unit> {
      ctx_ = std::make_unique<ValidatorSetup>(options());

      auto mock_own = td::actor::create_actor<MockManagerFacade>("mock");
      mock_ = &mock_own.get_actor_unsafe();

      auto bus = std::make_shared<TestBus>();
      ctx().fill(*bus, 0);
      bus->manager = mock_own.get();

      td::actor::Runtime runtime;
      BlockValidator::register_in(runtime);
      handle_ = runtime.start<consensus::Bus>(std::move(bus));

      co_await run_test();

      handle_.publish<StopRequested>();
      co_await ts_.wait_sync_work();

      handle_ = {};
      runtime = {};
      mock_own = {};
      mock_ = nullptr;
      ctx_.reset();

      co_return td::Unit{};
    });
  }

  virtual td::actor::Task<td::Unit> run_test() = 0;
};

class MasterchainBlockValidatorTest : public BlockValidatorTest {
 protected:
  TestOptions options() const override {
    return TestOptions{.shard = ShardIdFull{masterchainId, shardIdAll}};
  }
};

// =============================================================================
// Tests
// =============================================================================

struct AcceptsGoodEmptyCandidates : BlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
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
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto wrong_block = make_block_data(ctx().shard(), 99, create_cell(99))->block_id();
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

    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto bc = make_block_candidate(ctx().shard(), 2);
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

    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto bc = make_block_candidate(ctx().shard(), 2);
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

    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto bc = make_block_candidate(ctx().shard(), 2);
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

struct AcceptsMasterchainEmptyCandidatesWithoutWaiting : MasterchainBlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    // Supports simplex_docs.md §4.4:
    // empty masterchain candidates participate in validation through the ordinary empty-candidate
    // path and must not be blocked on the full-candidate wait for the previous accepted block.
    auto accepted_state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto state = make_normal_state(ctx().shard(), 2, min_mc_block_id);
    handle_.publish<Start>(accepted_state);

    auto candidate = make_empty_candidate(state->as_normal().value(), PeerValidatorId{0});
    auto task = handle_.publish<ValidationRequest>(state, candidate).start();

    co_await ts_.wait_sync_work();

    EXPECT(task.await_ready());
    EXPECT_EQ(mock_->validate.call_count(), 0);

    auto result = co_await std::move(task);
    EXPECT(result.has<CandidateAccept>());

    co_return {};
  }
};
REGISTER_TEST(BlockValidator, AcceptsMasterchainEmptyCandidatesWithoutWaiting);

struct WaitsForPreviousMasterchainBlockBeforeValidating : MasterchainBlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    // Supports the masterchain finalization dependency described in simplex_docs.md §4.4 case 2:
    // before validating a new full masterchain candidate, the validator waits until the block it
    // builds upon has been accepted locally.
    mock_->validate.returns(CandidateAccept{});

    auto accepted_state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto state = make_normal_state(ctx().shard(), 2, min_mc_block_id);
    handle_.publish<Start>(accepted_state);

    auto bc = make_block_candidate(ctx().shard(), 3);
    auto candidate = make_full_candidate(bc, PeerValidatorId{0});

    auto task = handle_.publish<ValidationRequest>(state, candidate).start();
    co_await ts_.wait_sync_work();

    EXPECT(!task.await_ready());
    EXPECT_EQ(mock_->validate.call_count(), 0);

    handle_.publish<BlockFinalizedInMasterchain>(state->as_normal().value());
    co_await ts_.wait_sync_work();

    auto result = co_await std::move(task);
    EXPECT(result.has<CandidateAccept>());
    EXPECT_EQ(mock_->validate.call_count(), 1);

    co_return {};
  }
};
REGISTER_TEST(BlockValidator, WaitsForPreviousMasterchainBlockBeforeValidating);

struct RejectsStaleMasterchainCandidates : MasterchainBlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    // Supports the same masterchain dependency from simplex_docs.md §4.4 case 2:
    // once the validator has already finalized past a candidate's base block, that candidate is
    // stale and must not be accepted for validation.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto finalized_block = make_block_data(ctx().shard(), 2, create_cell(2))->block_id();
    handle_.publish<BlockFinalizedInMasterchain>(finalized_block);

    auto bc = make_block_candidate(ctx().shard(), 2);
    auto candidate = make_full_candidate(bc, PeerValidatorId{0});

    auto result = co_await handle_.publish<ValidationRequest>(state, candidate).wrap();

    ASSERT_TRUE(result.is_error());
    EXPECT(result.error().message().str().find("already finalized") != std::string::npos);
    EXPECT_EQ(mock_->validate.call_count(), 0);

    co_return {};
  }
};
REGISTER_TEST(BlockValidator, RejectsStaleMasterchainCandidates);

}  // namespace
}  // namespace ton::validator::consensus::test
