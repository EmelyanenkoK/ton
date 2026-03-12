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

}  // namespace
}  // namespace ton::validator::consensus::test
