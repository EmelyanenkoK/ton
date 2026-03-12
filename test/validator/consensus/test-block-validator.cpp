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

using td::actor::count_events;
using td::actor::events_of;

td::Ref<block::BlockSignatureSet> make_nonfinal_signatures_for(const CandidateRef& candidate,
                                                               const consensus::Bus& bus) {
  return block::BlockSignatureSet::create_simplex_approve(
      {}, bus.cc_seqno, bus.validator_set_hash, bus.session_id, candidate->id.slot,
      candidate->hash_data().to_tl());
}

// =============================================================================
// BlockValidatorTest base class
// =============================================================================

using MockBus = td::actor::MockBus<consensus::Bus, MisbehaviorReport, TraceEvent>;

struct ValidatorBus : MockBus {
  using Parent = MockBus;
  using Events = td::TypeList<>;

  void populate_collator_schedule() override {
    UNREACHABLE();
  }
};

class BlockValidatorTest : public td::Test {
 protected:
  std::unique_ptr<ValidatorSetup> ctx_;
  td::actor::TestScheduler ts_;
  MockManagerFacade* mock_{};
  td::actor::BusHandle<ValidatorBus> handle_;
  bool stop_requested_{false};

  virtual TestOptions options() const {
    return TestOptions{};
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
      stop_requested_ = false;

      auto mock_own = td::actor::create_actor<MockManagerFacade>("mock");
      mock_ = &mock_own.get_actor_unsafe();

      auto bus = std::make_shared<ValidatorBus>();
      ctx().fill(*bus, 0);
      bus->manager = mock_own.get();

      auto runtime = ValidatorBus::create_runtime();
      TestFinalizeBlockResponder::register_in(runtime);
      BlockValidator::register_in(runtime);
      handle_ = runtime.start(std::move(bus));

      co_await run_test();

      if (!stop_requested_) {
        handle_.publish<StopRequested>();
        co_await ts_.wait_sync_work();
      }

      handle_ = {};
      runtime = {};
      mock_own = {};

      co_await ts_.wait_sync_work();

      mock_ = nullptr;
      ctx_.reset();

      co_return td::Unit{};
    });
  }

  td::actor::Task<td::Unit> request_stop() {
    if (!stop_requested_) {
      stop_requested_ = true;
      handle_.publish<StopRequested>();
      co_await ts_.wait_sync_work();
    }
    co_return {};
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

struct RejectsEmptyCandidateWithWrongParentLink : BlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers tracker Kernel-23 / Kernel-30 structural empty-candidate validation:
    // an empty candidate that references the current block but carries an unrelated parent link
    // should still be rejected instead of being treated as a valid chain extension.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto candidate = td::make_ref<Candidate>(CandidateId{.slot = 1, .hash = bits256_pattern(1701)},
                                             ParentId{make_candidate_id(77, 1777)}, PeerValidatorId{0},
                                             state->as_normal().value(), td::BufferSlice());

    auto result = co_await handle_.publish<ValidationRequest>(state, candidate);

    EXPECT(result.has<CandidateReject>());
    EXPECT_EQ(mock_->validate.call_count(), 0);

    co_return {};
  }
};
REGISTER_TEST(BlockValidator, RejectsEmptyCandidateWithWrongParentLink);

struct RejectsEmptyCandidateWithWrongBlockIdExtReference : BlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers tracker Kernel-23 / Kernel-30 exact BlockIdExt matching:
    // empty-candidate validation must reject a referenced block that keeps the same shard/seqno
    // but changes the root/file hashes.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto expected = state->as_normal().value();
    auto wrong_block =
        BlockIdExt{BlockId(ctx().shard(), expected.seqno()), bits256_pattern(1900), bits256_pattern(1901)};
    auto candidate = make_empty_candidate(wrong_block, PeerValidatorId{0});

    auto result = co_await handle_.publish<ValidationRequest>(state, candidate);

    EXPECT(result.has<CandidateReject>());
    EXPECT_EQ(mock_->validate.call_count(), 0);

    co_return {};
  }
};
REGISTER_TEST(BlockValidator, RejectsEmptyCandidateWithWrongBlockIdExtReference);

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

struct RejectsCandidateFromWrongLeader : BlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers tracker Kernel-23 / Kernel-30 at the validator boundary:
    // CandidateReceived already guarantees a leader-valid signature upstream, so if
    // validate_block_candidate rejects a candidate as coming from the wrong leader, BlockValidator
    // must propagate that rejection without caching or emitting misbehavior side effects.
    mock_->validate.returns(CandidateReject{.reason = "wrong leader", .proof = td::BufferSlice()});

    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto bc = make_block_candidate(ctx().shard(), 2);
    auto candidate = make_full_candidate(bc, PeerValidatorId{1});

    auto result = co_await handle_.publish<ValidationRequest>(state, candidate);

    EXPECT(result.has<CandidateReject>());
    EXPECT_EQ(result.get<CandidateReject>().reason, "wrong leader");
    EXPECT_EQ(mock_->cached_candidate_ids.size(), static_cast<size_t>(0));
    EXPECT_EQ(count_events<MisbehaviorReport>(bus_mock().events_), static_cast<size_t>(0));

    co_return {};
  }
};
REGISTER_TEST(BlockValidator, RejectsCandidateFromWrongLeader);

struct CachesAcceptedFullCandidates : BlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers the validator's accepted-candidate cache hook:
    // once manager validation accepts a full candidate, the validator stores it for later local
    // reuse instead of only returning the acceptance result.
    mock_->validate.returns(CandidateAccept{});

    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto bc = make_block_candidate(ctx().shard(), 2);
    auto candidate = make_full_candidate(bc, PeerValidatorId{0});

    auto result = co_await handle_.publish<ValidationRequest>(state, candidate);
    EXPECT(result.has<CandidateAccept>());

    co_await ts_.wait_sync_work();

    ASSERT_EQ(mock_->cached_candidate_ids.size(), static_cast<size_t>(1));
    EXPECT_EQ(mock_->cached_candidate_ids[0], bc.id);

    co_return {};
  }
};
REGISTER_TEST(BlockValidator, CachesAcceptedFullCandidates);

struct DoesNotCacheRejectedFullCandidates : BlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers the opposite cache edge case:
    // rejected full candidates must not be inserted into the local cache, because later stages
    // must not be able to resolve them as if they had passed validation.
    mock_->validate.returns(CandidateReject{.reason = "bad block", .proof = td::BufferSlice()});

    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto bc = make_block_candidate(ctx().shard(), 2);
    auto candidate = make_full_candidate(bc, PeerValidatorId{0});

    auto result = co_await handle_.publish<ValidationRequest>(state, candidate);
    EXPECT(result.has<CandidateReject>());

    co_await ts_.wait_sync_work();

    EXPECT_EQ(mock_->cached_candidate_ids.size(), static_cast<size_t>(0));

    co_return {};
  }
};
REGISTER_TEST(BlockValidator, DoesNotCacheRejectedFullCandidates);

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

struct RejectsRunValidateFailureWithoutVoteOrMisbehavior : BlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers tracker Kernel-23 rejection side effects:
    // if run_validate_query rejects the block, BlockValidator must return the rejection as-is and
    // must not cache the candidate or emit MisbehaviorReport on its own.
    mock_->validate.returns(CandidateReject{.reason = "validate query failed", .proof = td::BufferSlice("proof")});

    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto bc = make_block_candidate(ctx().shard(), 2);
    auto candidate = make_full_candidate(bc, PeerValidatorId{0});

    auto result = co_await handle_.publish<ValidationRequest>(state, candidate);

    EXPECT(result.has<CandidateReject>());
    EXPECT_EQ(result.get<CandidateReject>().reason, "validate query failed");
    EXPECT_EQ(mock_->cached_candidate_ids.size(), static_cast<size_t>(0));
    EXPECT_EQ(count_events<MisbehaviorReport>(bus_mock().events_), static_cast<size_t>(0));

    co_return {};
  }
};
REGISTER_TEST(BlockValidator, RejectsRunValidateFailureWithoutVoteOrMisbehavior);

struct StoppingDuringValidationDoesNotReportMisbehavior : BlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers tracker Kernel-23 / Kernel-30 shutdown-mid-validation behavior:
    // stopping the validator while a full-candidate validation is in flight must cancel the work
    // without turning shutdown into a local misbehavior report or a cached candidate.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);

    auto pending_validate = mock_->validate.expect();

    auto bc = make_block_candidate(ctx().shard(), 2);
    auto candidate = make_full_candidate(bc, PeerValidatorId{0});
    auto task = handle_.publish<ValidationRequest>(state, candidate).start();

    co_await ts_.wait_sync_work();
    EXPECT(!task.await_ready());

    auto pending_call = co_await std::move(pending_validate);

    co_await request_stop();

    pending_call.respond.set_value(CandidateAccept{});
    for (int i = 0; i < 10 && !task.await_ready(); ++i) {
      co_await ts_.wait_sync_work();
    }

    EXPECT(task.await_ready());
    auto result = co_await std::move(task).wrap();
    ASSERT_TRUE(result.is_error());
    EXPECT_EQ(mock_->cached_candidate_ids.size(), static_cast<size_t>(0));
    EXPECT_EQ(count_events<MisbehaviorReport>(bus_mock().events_), static_cast<size_t>(0));

    co_return {};
  }
};
REGISTER_TEST(BlockValidator, StoppingDuringValidationDoesNotReportMisbehavior);

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

struct IgnoresIrrelevantMasterchainFinalizationNotifications : MasterchainBlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers the validator's masterchain wake-up guard:
    // while waiting for the previous accepted masterchain block, notifications for another shard
    // or for seqno 0 must not release the waiter.
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

    auto wrong_shard_block = make_block_data(ShardIdFull{0, 0xC000'0000'0000'0000ULL}, 2, create_cell(20))->block_id();
    handle_.publish<BlockFinalizedInMasterchain>(wrong_shard_block);
    co_await ts_.wait_sync_work();

    EXPECT(!task.await_ready());
    EXPECT_EQ(mock_->validate.call_count(), 0);

    auto seqno_zero_block =
        BlockIdExt{ctx().shard().workchain, ctx().shard().shard, 0, bits256_pattern(600), bits256_pattern(601)};
    handle_.publish<BlockFinalizedInMasterchain>(seqno_zero_block);
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
REGISTER_TEST(BlockValidator, IgnoresIrrelevantMasterchainFinalizationNotifications);

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

struct RejectsMasterchainCandidateWhoseParentIsOnlyNotarized : MasterchainBlockValidatorTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers tracker Kernel-20 / Kernel-23 at the validator boundary:
    // only a truly accepted/finalized previous masterchain block may unlock child validation; a
    // parent that is merely notarized/approved must not wake the waiter.
    mock_->validate.returns(CandidateAccept{});

    auto accepted_state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto state = make_normal_state(ctx().shard(), 2, min_mc_block_id);
    handle_.publish<Start>(accepted_state);

    auto child_block = make_block_candidate(ctx().shard(), 3);
    auto child_candidate = make_full_candidate(child_block, PeerValidatorId{0});
    auto task = handle_.publish<ValidationRequest>(state, child_candidate).start();

    co_await ts_.wait_sync_work();
    EXPECT(!task.await_ready());
    EXPECT_EQ(mock_->validate.call_count(), 0);

    auto parent_candidate =
        td::make_ref<Candidate>(CandidateId{.slot = 1, .hash = bits256_pattern(2601)}, ParentId{make_candidate_id(0, 2600)},
                                PeerValidatorId{1}, state->as_normal().value(), td::BufferSlice());
    auto approve_signatures = make_nonfinal_signatures_for(parent_candidate, *handle_.operator->());
    co_await handle_.publish<FinalizeBlock>(parent_candidate, approve_signatures);
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
REGISTER_TEST(BlockValidator, RejectsMasterchainCandidateWhoseParentIsOnlyNotarized);

}  // namespace
}  // namespace ton::validator::consensus::test
