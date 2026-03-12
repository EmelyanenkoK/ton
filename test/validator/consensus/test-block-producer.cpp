/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "td/actor/Mocks.h"
#include "td/actor/TestScheduler.h"
#include "td/utils/tests.h"

#include "test-helpers.h"

namespace ton::validator::consensus::test {
namespace {

using td::actor::count_events;
using td::actor::events_of;

// =============================================================================
// Test bus and base class
// =============================================================================

using MockBus = td::actor::MockBus<consensus::Bus, CandidateGenerated, CandidateReceived, TraceEvent>;

struct ProducerBus : MockBus {
  using Parent = MockBus;
  using Events = td::TypeList<>;

  void populate_collator_schedule() override {
    UNREACHABLE();
  }
};

class BlockProducerTest : public td::Test {
 protected:
  std::unique_ptr<ValidatorSetup> ctx_;
  td::actor::TestScheduler ts_;
  MockManagerFacade* mock_{};
  td::actor::BusHandle<ProducerBus> handle_;

  virtual TestOptions options() const {
    return TestOptions{.shard = ShardIdFull{masterchainId, shardIdAll}};
  }

  ValidatorSetup& ctx() {
    CHECK(ctx_ != nullptr);
    return *ctx_;
  }

  auto& bus_mock() {
    return *handle_->actor;
  }

  void run() final {
    ts_.run([this]() -> td::actor::Task<td::Unit> {
      ctx_ = std::make_unique<ValidatorSetup>(options());

      auto mock_own = td::actor::create_actor<MockManagerFacade>("mock_manager");
      mock_ = &mock_own.get_actor_unsafe();

      auto keyring_own = ctx().create_keyring(0);

      auto bus = std::make_shared<ProducerBus>();
      ctx().fill(*bus, 0);
      bus->manager = mock_own.get();
      bus->keyring = keyring_own.get();

      auto runtime = ProducerBus::create_runtime();
      BlockProducer::register_in(runtime);
      handle_ = runtime.start(std::move(bus));

      co_await ts_.wait_sync_work();
      co_await run_test();

      handle_.publish<StopRequested>();
      co_await ts_.wait_sync_work();

      handle_ = {};
      runtime = {};
      keyring_own = {};
      mock_own = {};

      co_await ts_.wait_sync_work();

      mock_ = nullptr;
      ctx_.reset();

      co_return td::Unit{};
    });
  }

  virtual td::actor::Task<td::Unit> run_test() = 0;
};

// =============================================================================
// Tests
// =============================================================================

struct GeneratesFullCandidate : BlockProducerTest {
  td::actor::Task<td::Unit> run_test() override {
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();

    mock_->collate.returns(make_collation_result(state, ctx().shard(), 2));

    handle_.publish<OurLeaderWindowStarted>(ParentId{}, state, 0, 4, td::Timestamp::now());

    co_await ts_.wait_sync_work();

    auto& events = bus_mock().events_;
    EXPECT_EQ(count_events<CandidateGenerated>(events), static_cast<size_t>(1));

    auto generated = events_of<CandidateGenerated>(events);
    EXPECT_EQ(generated[0]->candidate->id.slot, static_cast<td::uint32>(0));
    EXPECT(!generated[0]->candidate->is_empty());
    EXPECT_EQ(mock_->collate.call_count(), 1);

    co_return {};
  }
};
REGISTER_TEST(BlockProducer, GeneratesFullCandidate);

struct SignsGeneratedCandidates : BlockProducerTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers the "Candidates" definition in simplex_docs.md:
    // every produced candidate includes sigma, a valid leader signature.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();

    mock_->collate.returns(make_collation_result(state, ctx().shard(), 2));

    handle_.publish<OurLeaderWindowStarted>(ParentId{}, state, 0, 4, td::Timestamp::now());
    co_await ts_.wait_sync_work();

    auto generated = events_of<CandidateGenerated>(bus_mock().events_);
    ASSERT_EQ(generated.size(), static_cast<size_t>(1));

    auto candidate = generated[0]->candidate;
    auto id_to_sign = serialize_tl_object(candidate->id.to_tl(), true);
    EXPECT(handle_->local_id.check_signature(handle_->session_id, id_to_sign, candidate->signature));

    co_return {};
  }
};
REGISTER_TEST(BlockProducer, SignsGeneratedCandidates);

struct GeneratesChainedCandidatesForWholeLeaderWindow : BlockProducerTest {
  TestOptions options() const override {
    auto opts = TestOptions{};
    opts.target_rate_ms = 0;
    return opts;
  }

  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md §4.2:
    // "The implementation produces L: generate_candidates loops over all slots in the window,
    // producing one candidate per slot with each referencing the previous as its parent."
    // This also covers the next sentence there: later candidates are generated without waiting
    // for earlier slots to be notarized, because this test publishes no consensus progress events.
    //
    // Use a shardchain setup here to avoid the separate masterchain empty-candidate policy from
    // simplex_docs.md §4.4 interfering with the whole-window full-candidate behavior.
    auto state1 = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto result2 = make_collation_result(state1, ctx().shard(), 2);
    auto state2 = state1->apply(result2.candidate);
    auto result3 = make_collation_result(state2, ctx().shard(), 3);
    auto state3 = state2->apply(result3.candidate);
    auto result4 = make_collation_result(state3, ctx().shard(), 4);
    auto state4 = state3->apply(result4.candidate);
    auto result5 = make_collation_result(state4, ctx().shard(), 5);

    handle_.publish<Start>(state1);
    co_await ts_.wait_sync_work();

    mock_->collate.returns(std::move(result2));
    mock_->collate.returns(std::move(result3));
    mock_->collate.returns(std::move(result4));
    mock_->collate.returns(std::move(result5));

    handle_.publish<OurLeaderWindowStarted>(ParentId{}, state1, 0, 4, td::Timestamp::now());
    co_await ts_.wait_sync_work();

    auto generated = events_of<CandidateGenerated>(bus_mock().events_);
    EXPECT_EQ(generated.size(), static_cast<size_t>(4));
    EXPECT_EQ(mock_->collate.call_count(), 4);

    EXPECT_EQ(generated[0]->candidate->id.slot, static_cast<td::uint32>(0));
    EXPECT_EQ(generated[1]->candidate->id.slot, static_cast<td::uint32>(1));
    EXPECT_EQ(generated[2]->candidate->id.slot, static_cast<td::uint32>(2));
    EXPECT_EQ(generated[3]->candidate->id.slot, static_cast<td::uint32>(3));

    EXPECT_EQ(generated[0]->candidate->parent_id, ParentId{});
    EXPECT_EQ(generated[1]->candidate->parent_id, ParentId{generated[0]->candidate->id});
    EXPECT_EQ(generated[2]->candidate->parent_id, ParentId{generated[1]->candidate->id});
    EXPECT_EQ(generated[3]->candidate->parent_id, ParentId{generated[2]->candidate->id});

    EXPECT(!generated[0]->candidate->is_empty());
    EXPECT(!generated[1]->candidate->is_empty());
    EXPECT(!generated[2]->candidate->is_empty());
    EXPECT(!generated[3]->candidate->is_empty());

    co_return {};
  }
};
REGISTER_TEST(BlockProducer, GeneratesChainedCandidatesForWholeLeaderWindow);

struct GeneratesEmptyMasterchainCandidatesWhenConsensusFinalityLags : BlockProducerTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md §4.4, case 2:
    // a masterchain leader generates an empty candidate when the next block to produce would be
    // more than one seqno ahead of the last consensus-finalized block.
    auto finalized_state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto ahead_state = make_normal_state(ctx().shard(), 2, min_mc_block_id);
    auto parent = CandidateId{.slot = 77, .hash = bits256_pattern(770)};

    handle_.publish<Start>(finalized_state);
    co_await ts_.wait_sync_work();

    handle_.publish<OurLeaderWindowStarted>(ParentId{parent}, ahead_state, 0, 4, td::Timestamp::now());
    co_await ts_.wait_sync_work();

    auto generated = events_of<CandidateGenerated>(bus_mock().events_);
    ASSERT_EQ(generated.size(), static_cast<size_t>(1));

    auto candidate = generated[0]->candidate;
    EXPECT(candidate->is_empty());
    EXPECT_EQ(candidate->parent_id, ParentId{parent});
    EXPECT_EQ(candidate->block_id(), ahead_state->assert_normal());
    EXPECT_EQ(mock_->collate.call_count(), 0);

    co_return {};
  }
};
REGISTER_TEST(BlockProducer, GeneratesEmptyMasterchainCandidatesWhenConsensusFinalityLags);

}  // namespace
}  // namespace ton::validator::consensus::test
