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

td::Ref<block::BlockSignatureSet> make_nonfinal_signatures_for(const CandidateRef& candidate,
                                                               const consensus::Bus& bus) {
  auto& block = std::get<BlockCandidate>(candidate->block);
  return block::BlockSignatureSet::create_simplex_approve(
      {}, bus.cc_seqno, bus.validator_set_hash, bus.session_id, candidate->id.slot,
      CandidateHashData::create_full(block, candidate->parent_id).to_tl());
}

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
      TestFinalizeBlockResponder::register_in(runtime);
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

struct GeneratesFullCandidateFromProvidedBase : BlockProducerTest {
  TestOptions options() const override {
    return TestOptions{};
  }

  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md Rule 3 for a non-genesis base:
    // when the leader window starts from a proved base (s_p, h_p), the first produced candidate
    // must keep that base as its parent reference instead of resetting to consensus genesis.
    auto finalized_state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto state = make_normal_state(ctx().shard(), 2, min_mc_block_id);
    auto base = CandidateId{.slot = 7, .hash = bits256_pattern(707)};

    handle_.publish<Start>(finalized_state);
    co_await ts_.wait_sync_work();

    mock_->collate.returns(make_collation_result(state, ctx().shard(), 3));

    handle_.publish<OurLeaderWindowStarted>(ParentId{base}, state, 8, 12, td::Timestamp::now());
    co_await ts_.wait_sync_work();

    auto generated = events_of<CandidateGenerated>(bus_mock().events_);
    ASSERT_EQ(generated.size(), static_cast<size_t>(1));

    auto candidate = generated[0]->candidate;
    EXPECT_EQ(candidate->id.slot, static_cast<td::uint32>(8));
    EXPECT_EQ(candidate->parent_id, ParentId{base});
    EXPECT(!candidate->is_empty());
    EXPECT_EQ(mock_->collate.call_count(), 1);

    co_return {};
  }
};
REGISTER_TEST(BlockProducer, GeneratesFullCandidateFromProvidedBase);

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

struct SignsEmptyCandidates : BlockProducerTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers the "Candidates" definition in simplex_docs.md for the empty-candidate path:
    // even when the payload is just a referenced block, the produced candidate still includes
    // sigma, a valid leader signature over the candidate id and parent reference.
    auto finalized_state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto ahead_state = make_normal_state(ctx().shard(), 2, min_mc_block_id);
    auto parent = CandidateId{.slot = 17, .hash = bits256_pattern(1717)};

    handle_.publish<Start>(finalized_state);
    co_await ts_.wait_sync_work();

    handle_.publish<OurLeaderWindowStarted>(ParentId{parent}, ahead_state, 8, 12, td::Timestamp::now());
    co_await ts_.wait_sync_work();

    auto generated = events_of<CandidateGenerated>(bus_mock().events_);
    ASSERT_EQ(generated.size(), static_cast<size_t>(1));

    auto candidate = generated[0]->candidate;
    EXPECT(candidate->is_empty());
    auto id_to_sign = serialize_tl_object(candidate->id.to_tl(), true);
    EXPECT(handle_->local_id.check_signature(handle_->session_id, id_to_sign, candidate->signature));

    co_return {};
  }
};
REGISTER_TEST(BlockProducer, SignsEmptyCandidates);

struct GeneratesFullMasterchainCandidatesAtConsensusFinalityBoundary : BlockProducerTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers the exact-boundary negative for the masterchain empty-candidate rule:
    // when consensus finality has caught up to exactly one block behind the local tip, production
    // must still collate a full candidate instead of switching to the empty fallback.
    auto finalized_state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto state = make_normal_state(ctx().shard(), 2, min_mc_block_id);
    auto finalized_block = make_block_candidate(ctx().shard(), 2);
    auto finalized_candidate = make_full_candidate(finalized_block, PeerValidatorId{1});
    auto final_signatures =
        block::BlockSignatureSet::create_ordinary({}, handle_->cc_seqno, handle_->validator_set_hash);

    handle_.publish<Start>(finalized_state);
    co_await ts_.wait_sync_work();

    co_await handle_.publish<FinalizeBlock>(finalized_candidate, final_signatures);
    co_await ts_.wait_sync_work();

    mock_->collate.returns(make_collation_result(state, ctx().shard(), 3));

    handle_.publish<OurLeaderWindowStarted>(ParentId{}, state, 0, 1, td::Timestamp::now());
    co_await ts_.wait_sync_work();

    auto generated = events_of<CandidateGenerated>(bus_mock().events_);
    ASSERT_EQ(generated.size(), static_cast<size_t>(1));

    EXPECT(!generated[0]->candidate->is_empty());
    EXPECT_EQ(generated[0]->candidate->id.slot, static_cast<td::uint32>(0));
    EXPECT_EQ(mock_->collate.call_count(), 1);

    co_return {};
  }
};
REGISTER_TEST(BlockProducer, GeneratesFullMasterchainCandidatesAtConsensusFinalityBoundary);

struct DoesNotAdvanceConsensusFinalityOnNonFinalFinalizeBlock : BlockProducerTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers the negative FinalizeBlock path:
    // approve/notarization signatures must not advance last_consensus_finalized_seqno_, otherwise
    // the producer would wrongly leave the empty-candidate safety fallback too early.
    auto finalized_state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto ahead_state = make_normal_state(ctx().shard(), 2, min_mc_block_id);
    auto parent = CandidateId{.slot = 23, .hash = bits256_pattern(2323)};
    auto candidate_block = make_block_candidate(ctx().shard(), 2);
    auto candidate = make_full_candidate(candidate_block, PeerValidatorId{1});
    auto approve_signatures = make_nonfinal_signatures_for(candidate, *handle_.operator->());

    handle_.publish<Start>(finalized_state);
    co_await ts_.wait_sync_work();

    co_await handle_.publish<FinalizeBlock>(candidate, approve_signatures);
    co_await ts_.wait_sync_work();

    handle_.publish<OurLeaderWindowStarted>(ParentId{parent}, ahead_state, 0, 1, td::Timestamp::now());
    co_await ts_.wait_sync_work();

    auto generated = events_of<CandidateGenerated>(bus_mock().events_);
    ASSERT_EQ(generated.size(), static_cast<size_t>(1));

    EXPECT(generated[0]->candidate->is_empty());
    EXPECT_EQ(generated[0]->candidate->parent_id, ParentId{parent});
    EXPECT_EQ(mock_->collate.call_count(), 0);

    co_return {};
  }
};
REGISTER_TEST(BlockProducer, DoesNotAdvanceConsensusFinalityOnNonFinalFinalizeBlock);

struct GeneratesFullShardchainCandidatesAtMasterchainLagBoundary : BlockProducerTest {
  TestOptions options() const override {
    return TestOptions{};
  }

  td::actor::Task<td::Unit> run_test() override {
    // Covers the shardchain off-by-one negative:
    // exactly 8 blocks of lag from the stored masterchain-finalized seqno is still allowed to
    // collate a full candidate; the empty fallback starts only after that boundary is exceeded.
    auto finalized_state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto boundary_state = make_normal_state(ctx().shard(), 8, min_mc_block_id);

    handle_.publish<Start>(finalized_state);
    co_await ts_.wait_sync_work();

    mock_->collate.returns(make_collation_result(boundary_state, ctx().shard(), 9));

    handle_.publish<OurLeaderWindowStarted>(ParentId{}, boundary_state, 0, 1, td::Timestamp::now());
    co_await ts_.wait_sync_work();

    auto generated = events_of<CandidateGenerated>(bus_mock().events_);
    ASSERT_EQ(generated.size(), static_cast<size_t>(1));

    EXPECT(!generated[0]->candidate->is_empty());
    EXPECT_EQ(mock_->collate.call_count(), 1);

    co_return {};
  }
};
REGISTER_TEST(BlockProducer, GeneratesFullShardchainCandidatesAtMasterchainLagBoundary);

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

struct PassesRemainingCollationBudgetToManager : BlockProducerTest {
  TestOptions options() const override {
    auto opts = TestOptions{};
    opts.target_rate_ms = 1000;
    return opts;
  }

  td::actor::Task<td::Unit> run_test() override {
    // Covers Kernel-22 / the producer time-budget contract from tracker-test-plan.md:
    // the collator should receive the remaining time in the current slot, not a fresh full
    // target-rate budget when the leader window is already late at the moment collation starts.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();

    auto pending_collation = mock_->collate.expect();
    handle_.publish<OurLeaderWindowStarted>(ParentId{}, state, 0, 1, td::Timestamp::in(-0.75));
    co_await ts_.wait_sync_work();

    auto pending_call = co_await std::move(pending_collation);
    const auto& params = std::get<0>(pending_call.args);

    EXPECT(params.soft_timeout.in() <= 0.35);

    pending_call.respond.set_value(make_collation_result(state, ctx().shard(), 2));
    co_await ts_.wait_sync_work();

    co_return {};
  }
};
// FIXME: This is a reasonable expectation, probably fixed by SpyCheese's collator changes.
// REGISTER_TEST(BlockProducer, PassesRemainingCollationBudgetToManager);

struct DoesNotPublishCandidatesFromSupersededLeaderWindow : BlockProducerTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers the producer's stale-window guard:
    // if a later OurLeaderWindowStarted supersedes the current window while collation is still
    // in flight, the old window must not publish a stale CandidateGenerated event after it
    // eventually finishes.
    auto finalized_state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto replacement_state = make_normal_state(ctx().shard(), 2, min_mc_block_id);
    auto replacement_parent = CandidateId{.slot = 41, .hash = bits256_pattern(4141)};

    handle_.publish<Start>(finalized_state);
    co_await ts_.wait_sync_work();

    auto pending_collation = mock_->collate.expect();
    handle_.publish<OurLeaderWindowStarted>(ParentId{}, finalized_state, 0, 1, td::Timestamp::now());
    co_await ts_.wait_sync_work();

    auto pending_call = co_await std::move(pending_collation);

    handle_.publish<OurLeaderWindowStarted>(ParentId{replacement_parent}, replacement_state, 4, 5,
                                            td::Timestamp::now());
    co_await ts_.wait_sync_work();

    pending_call.respond.set_value(make_collation_result(finalized_state, ctx().shard(), 2));
    co_await ts_.wait_sync_work();

    auto generated = events_of<CandidateGenerated>(bus_mock().events_);
    ASSERT_EQ(generated.size(), static_cast<size_t>(1));

    auto candidate = generated[0]->candidate;
    EXPECT_EQ(candidate->id.slot, static_cast<td::uint32>(4));
    EXPECT(candidate->is_empty());
    EXPECT_EQ(candidate->parent_id, ParentId{replacement_parent});
    EXPECT_EQ(mock_->collate.call_count(), 1);

    co_return {};
  }
};
REGISTER_TEST(BlockProducer, DoesNotPublishCandidatesFromSupersededLeaderWindow);

struct DoesNotPublishCandidatesAfterStopRequested : BlockProducerTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers the producer stop/cancellation path:
    // once StopRequested is processed, an in-flight collation must not still publish a candidate
    // afterward even if the manager eventually responds.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();

    auto pending_collation = mock_->collate.expect();
    handle_.publish<OurLeaderWindowStarted>(ParentId{}, state, 0, 1, td::Timestamp::now());
    co_await ts_.wait_sync_work();

    auto pending_call = co_await std::move(pending_collation);

    handle_.publish<StopRequested>();
    co_await ts_.wait_sync_work();

    pending_call.respond.set_value(make_collation_result(state, ctx().shard(), 2));
    co_await ts_.wait_sync_work();

    EXPECT_EQ(count_events<CandidateGenerated>(bus_mock().events_), static_cast<size_t>(0));
    EXPECT_EQ(count_events<CandidateReceived>(bus_mock().events_), static_cast<size_t>(0));
    EXPECT_EQ(mock_->collate.call_count(), 1);

    co_return {};
  }
};
REGISTER_TEST(BlockProducer, DoesNotPublishCandidatesAfterStopRequested);

struct SwitchesToEmptyCandidatesMidMasterchainWindow : BlockProducerTest {
  TestOptions options() const override {
    auto opts = BlockProducerTest::options();
    opts.target_rate_ms = 0;
    return opts;
  }

  td::actor::Task<td::Unit> run_test() override {
    // Covers the edge case at the boundary of simplex_docs.md §4.2 and §4.4:
    // the implementation keeps producing through the whole leader window, but on masterchain the
    // window can start with a full candidate and then switch to empty candidates once consensus
    // finality stops keeping up with the advancing local state.
    auto finalized_state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto first_result = make_collation_result(finalized_state, ctx().shard(), 2);
    auto referenced_state = finalized_state->apply(first_result.candidate);

    handle_.publish<Start>(finalized_state);
    co_await ts_.wait_sync_work();

    mock_->collate.returns(std::move(first_result));

    handle_.publish<OurLeaderWindowStarted>(ParentId{}, finalized_state, 0, 3, td::Timestamp::now());
    co_await ts_.wait_sync_work();

    auto generated = events_of<CandidateGenerated>(bus_mock().events_);
    ASSERT_EQ(generated.size(), static_cast<size_t>(3));

    EXPECT(!generated[0]->candidate->is_empty());
    EXPECT(generated[1]->candidate->is_empty());
    EXPECT(generated[2]->candidate->is_empty());
    EXPECT_EQ(mock_->collate.call_count(), 1);

    EXPECT_EQ(generated[1]->candidate->parent_id, ParentId{generated[0]->candidate->id});
    EXPECT_EQ(generated[2]->candidate->parent_id, ParentId{generated[1]->candidate->id});
    EXPECT_EQ(generated[1]->candidate->block_id(), referenced_state->assert_normal());
    EXPECT_EQ(generated[2]->candidate->block_id(), referenced_state->assert_normal());

    co_return {};
  }
};
REGISTER_TEST(BlockProducer, SwitchesToEmptyCandidatesMidMasterchainWindow);

struct GeneratesEmptyCandidatesBeforeSplit : BlockProducerTest {
  TestOptions options() const override {
    return TestOptions{};
  }

  td::actor::Task<td::Unit> run_test() override {
    // Covers the producer's before_split guard:
    // once the local tip is marked before_split, production must switch to empty candidates even
    // if masterchain/shardchain finality lag would otherwise still allow a full collation.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id, true);
    auto parent = CandidateId{.slot = 55, .hash = bits256_pattern(5555)};

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();

    handle_.publish<OurLeaderWindowStarted>(ParentId{parent}, state, 0, 1, td::Timestamp::now());
    co_await ts_.wait_sync_work();

    auto generated = events_of<CandidateGenerated>(bus_mock().events_);
    ASSERT_EQ(generated.size(), static_cast<size_t>(1));

    auto candidate = generated[0]->candidate;
    EXPECT(candidate->is_empty());
    EXPECT_EQ(candidate->parent_id, ParentId{parent});
    EXPECT_EQ(candidate->block_id(), state->assert_normal());
    EXPECT_EQ(mock_->collate.call_count(), 0);

    co_return {};
  }
};
REGISTER_TEST(BlockProducer, GeneratesEmptyCandidatesBeforeSplit);

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

struct GeneratesEmptyShardchainCandidatesWhenMasterchainFinalityLags : BlockProducerTest {
  TestOptions options() const override {
    return TestOptions{};
  }

  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md §4.4, case 1:
    // a shardchain leader generates an empty candidate when the last masterchain finalized seqno
    // lags by more than 8 blocks behind the shardchain tip.
    auto finalized_state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto ahead_state = make_normal_state(ctx().shard(), 9, min_mc_block_id);
    auto parent = CandidateId{.slot = 88, .hash = bits256_pattern(880)};

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
REGISTER_TEST(BlockProducer, GeneratesEmptyShardchainCandidatesWhenMasterchainFinalityLags);

// =============================================================================
// NoncriticalParamsUpdated tests
// =============================================================================

struct NoncriticalParamsUpdateChangesTargetRate : BlockProducerTest {
  TestOptions options() const override {
    auto opts = TestOptions{};
    opts.target_rate_ms = 1000;
    return opts;
  }

  td::actor::Task<td::Unit> run_test() override {
    // After NoncriticalParamsUpdated halves target_rate, the next collation
    // should receive a proportionally shorter soft_timeout.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();

    // Update target_rate from 1000ms to 500ms.
    auto new_params = handle_->config.noncritical_params;
    new_params.target_rate = std::chrono::milliseconds{500};
    handle_.publish<NoncriticalParamsUpdated>(new_params);
    co_await ts_.wait_sync_work();

    auto pending_collation = mock_->collate.expect();
    handle_.publish<OurLeaderWindowStarted>(ParentId{}, state, 0, 1, td::Timestamp::now());
    co_await ts_.wait_sync_work();

    auto pending_call = co_await std::move(pending_collation);
    const auto& params = std::get<0>(pending_call.args);

    // With 500ms target_rate starting at now(), soft_timeout should be ~0.5s (not ~1.0s).
    EXPECT(params.soft_timeout.in() <= 0.6);
    EXPECT(params.soft_timeout.in() > 0.3);

    pending_call.respond.set_value(make_collation_result(state, ctx().shard(), 2));
    co_await ts_.wait_sync_work();

    co_return {};
  }
};
REGISTER_TEST(BlockProducer, NoncriticalParamsUpdateChangesTargetRate);

}  // namespace
}  // namespace ton::validator::consensus::test
