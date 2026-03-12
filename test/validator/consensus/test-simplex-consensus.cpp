/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "td/actor/TestScheduler.h"
#include "td/utils/tests.h"
#include "validator/consensus/simplex/misbehavior.h"

#include "test-helpers.h"

namespace ton::validator::consensus::test {
namespace {

class ConsensusObserverImpl;

struct ConsensusBus : simplex::Bus {
  using Parent = simplex::Bus;
  using Events = td::TypeList<>;

  ConsensusObserverImpl* observer = nullptr;
};

template <typename VoteT>
std::vector<VoteT> published_votes(const std::vector<simplex::Vote>& votes) {
  std::vector<VoteT> result;
  for (const auto& vote : votes) {
    if (auto* typed = std::get_if<VoteT>(&vote.vote)) {
      result.push_back(*typed);
    }
  }
  return result;
}

class ConsensusObserverImpl : public td::actor::SpawnsWith<ConsensusBus>, public td::actor::ConnectsTo<ConsensusBus> {
 public:
  TON_RUNTIME_DEFINE_EVENT_HANDLER();

  std::vector<simplex::Vote> broadcast_votes_;
  std::vector<ParentId> resolve_state_requests_;
  std::vector<CandidateId> wait_for_parent_candidate_ids_;
  std::vector<CandidateId> stored_candidate_ids_;
  std::vector<CandidateId> validation_candidate_ids_;
  std::vector<simplex::LeaderWindowObserved> observed_windows_;
  std::vector<OurLeaderWindowStarted> leader_windows_;
  std::vector<MisbehaviorReport> misbehavior_reports_;

  std::deque<simplex::ResolveState::Result> resolve_state_results_;
  std::deque<std::optional<MisbehaviorRef>> wait_for_parent_results_;
  std::deque<ValidateCandidateResult> validation_results_;

  void start_up() override {
    const_cast<ConsensusBus&>(*owning_bus()).observer = this;
  }

  template <>
  void handle(td::actor::BusHandle<ConsensusBus>, std::shared_ptr<const StopRequested>) {
    stop();
  }

  template <>
  void handle(td::actor::BusHandle<ConsensusBus>, std::shared_ptr<const OurLeaderWindowStarted> event) {
    leader_windows_.push_back(*event);
  }

  template <>
  void handle(td::actor::BusHandle<ConsensusBus>, std::shared_ptr<const MisbehaviorReport> event) {
    misbehavior_reports_.push_back(*event);
  }

  template <>
  td::actor::Task<td::Unit> process(td::actor::BusHandle<ConsensusBus>, std::shared_ptr<simplex::BroadcastVote> event) {
    broadcast_votes_.push_back(event->vote);
    co_return td::Unit{};
  }

  template <>
  td::actor::Task<td::Unit> process(td::actor::BusHandle<ConsensusBus>,
                                    std::shared_ptr<simplex::LeaderWindowObserved> event) {
    observed_windows_.push_back(*event);
    co_return td::Unit{};
  }

  template <>
  td::actor::Task<simplex::ResolveState::Result> process(td::actor::BusHandle<ConsensusBus>,
                                                         std::shared_ptr<simplex::ResolveState> event) {
    resolve_state_requests_.push_back(event->id);
    CHECK(!resolve_state_results_.empty());
    auto result = std::move(resolve_state_results_.front());
    resolve_state_results_.pop_front();
    co_return result;
  }

  template <>
  td::actor::Task<std::optional<MisbehaviorRef>> process(td::actor::BusHandle<ConsensusBus>,
                                                         std::shared_ptr<simplex::WaitForParent> event) {
    wait_for_parent_candidate_ids_.push_back(event->candidate->id);
    if (wait_for_parent_results_.empty()) {
      co_return std::nullopt;
    }
    auto result = std::move(wait_for_parent_results_.front());
    wait_for_parent_results_.pop_front();
    co_return result;
  }

  template <>
  td::actor::Task<td::Unit> process(td::actor::BusHandle<ConsensusBus>,
                                    std::shared_ptr<simplex::StoreCandidate> event) {
    stored_candidate_ids_.push_back(event->candidate->id);
    co_return td::Unit{};
  }

  template <>
  td::actor::Task<ValidateCandidateResult> process(td::actor::BusHandle<ConsensusBus>,
                                                   std::shared_ptr<ValidationRequest> event) {
    validation_candidate_ids_.push_back(event->candidate->id);
    CHECK(!validation_results_.empty());
    auto result = std::move(validation_results_.front());
    validation_results_.pop_front();
    co_return result;
  }
};

class SimplexConsensusTest : public td::Test {
 protected:
  std::unique_ptr<ValidatorSetup> ctx_;
  td::actor::TestScheduler ts_;
  td::actor::BusHandle<ConsensusBus> handle_;
  ConsensusBus* bus_{};

  virtual TestOptions options() const {
    return TestOptions{.weight_distribution = {1, 1}};
  }

  ValidatorSetup& ctx() {
    CHECK(ctx_ != nullptr);
    return *ctx_;
  }

  ConsensusObserverImpl& observer() {
    CHECK(bus_ != nullptr);
    CHECK(bus_->observer != nullptr);
    return *bus_->observer;
  }

  void run() final {
    ts_.run([this]() -> td::actor::Task<td::Unit> {
      ctx_ = std::make_unique<ValidatorSetup>(options());

      auto bus = std::make_shared<ConsensusBus>();
      fill_simplex_bus(ctx(), *bus, 0);
      bus->db = std::make_unique<TestDbImpl>();
      bus_ = bus.get();

      td::actor::Runtime runtime;
      runtime.register_actor<ConsensusObserverImpl>("ConsensusObserver");
      simplex::Consensus::register_in(runtime);
      handle_ = runtime.start(std::move(bus));

      co_await ts_.wait_sync_work();
      co_await run_test();

      handle_.publish<StopRequested>();
      co_await ts_.wait_sync_work();

      handle_ = {};
      bus_ = nullptr;
      runtime = {};
      ctx_.reset();

      co_await ts_.wait_sync_work();
      co_return td::Unit{};
    });
  }

  virtual td::actor::Task<td::Unit> run_test() = 0;
};

struct LeaderWindowStartsGenerationOnlyForExpectedLeader : SimplexConsensusTest {
  TestOptions options() const override {
    auto opts = SimplexConsensusTest::options();
    opts.slots_per_leader_window = 2;
    return opts;
  }

  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md Rule 3:
    // when a window becomes active, only the expected leader starts generation, and the start
    // event must carry the proved base and exact window bounds.
    auto first_base = ParentId{make_candidate_id(3, 3003)};
    auto second_base = ParentId{make_candidate_id(5, 5005)};
    auto resolved = simplex::ResolveState::Result{.state = make_normal_state(ctx().shard(), 7, min_mc_block_id)};

    observer().resolve_state_results_.push_back(resolved);

    co_await handle_.publish<simplex::LeaderWindowObserved>(0, first_base);
    co_await ts_.wait_sync_work();

    co_await handle_.publish<simplex::LeaderWindowObserved>(2, second_base);
    co_await ts_.wait_sync_work();

    ASSERT_EQ(observer().resolve_state_requests_.size(), static_cast<size_t>(1));
    EXPECT_EQ(observer().resolve_state_requests_[0], first_base);

    ASSERT_EQ(observer().leader_windows_.size(), static_cast<size_t>(1));
    EXPECT_EQ(observer().leader_windows_[0].base, first_base);
    EXPECT_EQ(observer().leader_windows_[0].start_slot, static_cast<td::uint32>(0));
    EXPECT_EQ(observer().leader_windows_[0].end_slot, static_cast<td::uint32>(2));
    EXPECT_EQ(observer().leader_windows_[0].state, resolved.state);

    co_return td::Unit{};
  }
};
REGISTER_TEST(SimplexConsensus, LeaderWindowStartsGenerationOnlyForExpectedLeader);

struct WaitForParentMisbehaviorShortCircuitsPipeline : SimplexConsensusTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md Rule 4:
    // if parent/gap proofing reports conflicting evidence, consensus must emit misbehavior and
    // stop before state resolution, validation, or local voting.
    auto candidate =
        make_wait_candidate(make_candidate_id(1, 1001), ParentId{make_candidate_id(0, 999)}, PeerValidatorId{1});

    observer().wait_for_parent_results_.push_back(simplex::ConflictingCandidateAndCertificate::create());

    handle_.publish<CandidateReceived>(candidate);
    co_await ts_.wait_sync_work();

    ASSERT_EQ(observer().wait_for_parent_candidate_ids_.size(), static_cast<size_t>(1));
    EXPECT_EQ(observer().wait_for_parent_candidate_ids_[0], candidate->id);
    EXPECT_EQ(observer().resolve_state_requests_.size(), static_cast<size_t>(0));
    EXPECT_EQ(observer().validation_candidate_ids_.size(), static_cast<size_t>(0));
    EXPECT_EQ(observer().broadcast_votes_.size(), static_cast<size_t>(0));

    ASSERT_EQ(observer().misbehavior_reports_.size(), static_cast<size_t>(1));
    EXPECT_EQ(observer().misbehavior_reports_[0].id, candidate->leader);

    co_return td::Unit{};
  }
};
REGISTER_TEST(SimplexConsensus, WaitForParentMisbehaviorShortCircuitsPipeline);

struct ValidCandidateNotarizesAndFinalizesOnMatchingNotarCert : SimplexConsensusTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md Rule 4 and Rule 5:
    // once parent proofing, state resolution, and validation succeed, consensus votes Notar; once
    // the matching notarization certificate is observed and no local skip exists, it votes Final.
    auto candidate =
        make_wait_candidate(make_candidate_id(1, 2001), ParentId{make_candidate_id(0, 1999)}, PeerValidatorId{0});
    auto parent_state = make_normal_state(ctx().shard(), 10, min_mc_block_id);

    observer().resolve_state_results_.push_back(simplex::ResolveState::Result{.state = parent_state});
    observer().validation_results_.push_back(CandidateAccept{});

    handle_.publish<CandidateReceived>(candidate);
    co_await ts_.wait_sync_work();

    ASSERT_EQ(observer().wait_for_parent_candidate_ids_.size(), static_cast<size_t>(1));
    EXPECT_EQ(observer().wait_for_parent_candidate_ids_[0], candidate->id);
    ASSERT_EQ(observer().resolve_state_requests_.size(), static_cast<size_t>(1));
    EXPECT_EQ(observer().resolve_state_requests_[0], candidate->parent_id);
    ASSERT_EQ(observer().stored_candidate_ids_.size(), static_cast<size_t>(1));
    EXPECT_EQ(observer().stored_candidate_ids_[0], candidate->id);
    ASSERT_EQ(observer().validation_candidate_ids_.size(), static_cast<size_t>(1));
    EXPECT_EQ(observer().validation_candidate_ids_[0], candidate->id);

    auto notar_votes = published_votes<simplex::NotarizeVote>(observer().broadcast_votes_);
    auto final_votes = published_votes<simplex::FinalizeVote>(observer().broadcast_votes_);
    ASSERT_EQ(notar_votes.size(), static_cast<size_t>(1));
    EXPECT_EQ(notar_votes[0].id, candidate->id);
    EXPECT_EQ(final_votes.size(), static_cast<size_t>(0));

    ConsensusBus seed_bus;
    fill_simplex_bus(ctx(), seed_bus, 0);
    auto notar_cert = make_simplex_certificate(ctx(), seed_bus, simplex::NotarizeVote{candidate->id}, {0, 1});

    handle_.publish<simplex::NotarizationObserved>(candidate->id, notar_cert);
    co_await ts_.wait_sync_work();

    final_votes = published_votes<simplex::FinalizeVote>(observer().broadcast_votes_);
    ASSERT_EQ(final_votes.size(), static_cast<size_t>(1));
    EXPECT_EQ(final_votes[0].id, candidate->id);

    co_return td::Unit{};
  }
};
REGISTER_TEST(SimplexConsensus, ValidCandidateNotarizesAndFinalizesOnMatchingNotarCert);

struct ObservedNotarBeforeTryNotarCompletionStillFinalizesOnce : SimplexConsensusTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers tracker Kernel-15 against simplex_docs.md Rule 5:
    // if NotarCert is observed before local try_notar() finishes its WaitForParent/ResolveState/
    // validation pipeline, the later local Notar vote must still trigger exactly one FinalizeVote.
    auto candidate =
        make_wait_candidate(make_candidate_id(1, 2051), ParentId{make_candidate_id(0, 2050)}, PeerValidatorId{0});
    auto parent_state = make_normal_state(ctx().shard(), 12, min_mc_block_id);

    observer().resolve_state_results_.push_back(simplex::ResolveState::Result{.state = parent_state});
    observer().validation_results_.push_back(CandidateAccept{});

    ConsensusBus seed_bus;
    fill_simplex_bus(ctx(), seed_bus, 0);
    auto notar_cert = make_simplex_certificate(ctx(), seed_bus, simplex::NotarizeVote{candidate->id}, {0, 1});

    handle_.publish<CandidateReceived>(candidate);
    handle_.publish<simplex::NotarizationObserved>(candidate->id, notar_cert);
    co_await ts_.wait_sync_work();

    auto notar_votes = published_votes<simplex::NotarizeVote>(observer().broadcast_votes_);
    auto final_votes = published_votes<simplex::FinalizeVote>(observer().broadcast_votes_);
    ASSERT_EQ(notar_votes.size(), static_cast<size_t>(1));
    EXPECT_EQ(notar_votes[0].id, candidate->id);
    ASSERT_EQ(final_votes.size(), static_cast<size_t>(1));
    EXPECT_EQ(final_votes[0].id, candidate->id);

    handle_.publish<simplex::NotarizationObserved>(candidate->id, notar_cert);
    co_await ts_.wait_sync_work();

    final_votes = published_votes<simplex::FinalizeVote>(observer().broadcast_votes_);
    ASSERT_EQ(final_votes.size(), static_cast<size_t>(1));
    EXPECT_EQ(final_votes[0].id, candidate->id);

    co_return {};
  }
};
REGISTER_TEST(SimplexConsensus, ObservedNotarBeforeTryNotarCompletionStillFinalizesOnce);

struct ValidationRejectDoesNotCastVoteOrReportMisbehavior : SimplexConsensusTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers the Kernel-23 validation-reject side-effect guard:
    // a failed ValidationRequest must stop the pipeline without emitting local votes or accusing
    // the candidate leader of misbehavior, because rejection alone is not contradictory proof.
    auto candidate =
        make_wait_candidate(make_candidate_id(2, 2152), ParentId{make_candidate_id(1, 2151)}, PeerValidatorId{1});
    auto parent_state = make_normal_state(ctx().shard(), 13, min_mc_block_id);

    observer().resolve_state_results_.push_back(simplex::ResolveState::Result{.state = parent_state});
    observer().validation_results_.push_back(
        CandidateReject{.reason = "synthetic validation failure", .proof = td::BufferSlice()});

    handle_.publish<CandidateReceived>(candidate);
    co_await ts_.wait_sync_work();

    EXPECT_EQ(observer().wait_for_parent_candidate_ids_.size(), static_cast<size_t>(1));
    EXPECT_EQ(observer().resolve_state_requests_.size(), static_cast<size_t>(1));
    EXPECT_EQ(observer().validation_candidate_ids_.size(), static_cast<size_t>(1));
    EXPECT_EQ(observer().broadcast_votes_.size(), static_cast<size_t>(0));
    EXPECT_EQ(observer().misbehavior_reports_.size(), static_cast<size_t>(0));

    co_return {};
  }
};
REGISTER_TEST(SimplexConsensus, ValidationRejectDoesNotCastVoteOrReportMisbehavior);

struct DuplicateCandidateReceivedDoesNotDoubleNotar : SimplexConsensusTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers the duplicate CandidateReceived edge from Kernel-23 / Kernel-28:
    // replaying the same candidate before try_notar() finishes must not start a second pipeline or
    // emit duplicate local Notar votes.
    auto candidate =
        make_wait_candidate(make_candidate_id(3, 2253), ParentId{make_candidate_id(2, 2252)}, PeerValidatorId{0});
    auto parent_state = make_normal_state(ctx().shard(), 14, min_mc_block_id);

    observer().resolve_state_results_.push_back(simplex::ResolveState::Result{.state = parent_state});
    observer().validation_results_.push_back(CandidateAccept{});

    handle_.publish<CandidateReceived>(candidate);
    handle_.publish<CandidateReceived>(candidate);
    co_await ts_.wait_sync_work();

    ASSERT_EQ(observer().wait_for_parent_candidate_ids_.size(), static_cast<size_t>(1));
    ASSERT_EQ(observer().resolve_state_requests_.size(), static_cast<size_t>(1));
    ASSERT_EQ(observer().stored_candidate_ids_.size(), static_cast<size_t>(1));
    ASSERT_EQ(observer().validation_candidate_ids_.size(), static_cast<size_t>(1));

    auto notar_votes = published_votes<simplex::NotarizeVote>(observer().broadcast_votes_);
    ASSERT_EQ(notar_votes.size(), static_cast<size_t>(1));
    EXPECT_EQ(notar_votes[0].id, candidate->id);

    co_return {};
  }
};
REGISTER_TEST(SimplexConsensus, DuplicateCandidateReceivedDoesNotDoubleNotar);

struct SkipVoteIncreasesNextWindowFirstBlockTimeout : SimplexConsensusTest {
  TestOptions options() const override {
    auto opts = SimplexConsensusTest::options();
    opts.weight_distribution = {1, 1};
    opts.slots_per_leader_window = 1;
    opts.target_rate_ms = 1000;
    opts.first_block_timeout_ms = 1000;
    return opts;
  }

  td::actor::Task<td::Unit> run_test() override {
    // Covers tracker Kernel-31 commit 2bd39cea8 in the actor that actually owns it:
    // after a skipped window, ConsensusImpl should increase the next window's first-block timeout
    // instead of resetting it to the default immediately.
    auto base = ParentId{make_candidate_id(0, 2290)};
    auto parent_state = make_normal_state(ctx().shard(), 15, min_mc_block_id);

    observer().resolve_state_results_.push_back(simplex::ResolveState::Result{.state = parent_state});
    observer().resolve_state_results_.push_back(simplex::ResolveState::Result{.state = parent_state});

    co_await handle_.publish<simplex::LeaderWindowObserved>(0, base);
    co_await ts_.wait_sync_work();
    double window0_timeout_s = ts_.next_timeout_in();

    ts_.advance_time(window0_timeout_s);
    co_await ts_.wait_sync_work();

    auto skip_votes = published_votes<simplex::SkipVote>(observer().broadcast_votes_);
    ASSERT_EQ(skip_votes.size(), static_cast<size_t>(1));
    EXPECT_EQ(skip_votes[0].slot, static_cast<td::uint32>(0));

    co_await handle_.publish<simplex::LeaderWindowObserved>(1, base);
    co_await ts_.wait_sync_work();
    double window1_timeout_s = ts_.next_timeout_in();

    EXPECT(window1_timeout_s > window0_timeout_s);
    EXPECT(window1_timeout_s >= 2.0);

    co_return {};
  }
};
REGISTER_TEST(SimplexConsensus, SkipVoteIncreasesNextWindowFirstBlockTimeout);

struct Rule6SkipTimeoutUsesLastObservedFinalization : SimplexConsensusTest {
  TestOptions options() const override {
    auto opts = SimplexConsensusTest::options();
    opts.weight_distribution = {1, 1, 1};
    opts.slots_per_leader_window = 1;
    opts.target_rate_ms = 1000;
    opts.first_block_timeout_ms = 1000;
    return opts;
  }

  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md Rule 6 as a red spec test:
    // skip timeout growth must key off the last observed finalized window, so after a finalization
    // in window 0 the timeout for window 2 must be strictly larger than the timeout for window 1.
    auto finalized_id = make_candidate_id(0, 3000);

    ConsensusBus seed_bus;
    fill_simplex_bus(ctx(), seed_bus, 0);
    auto final_cert = make_simplex_certificate(ctx(), seed_bus, simplex::FinalizeVote{finalized_id}, {0, 1});

    handle_.publish<simplex::FinalizationObserved>(finalized_id, final_cert);
    co_await ts_.wait_sync_work();

    co_await handle_.publish<simplex::LeaderWindowObserved>(1, ParentId{finalized_id});
    co_await ts_.wait_sync_work();
    double window1_timeout_s = ts_.next_timeout_in();

    co_await handle_.publish<simplex::LeaderWindowObserved>(2, ParentId{finalized_id});
    co_await ts_.wait_sync_work();
    double window2_timeout_s = ts_.next_timeout_in();

    double expected_window2_min_s = options().target_rate_ms / 1000. +
                                    options().first_block_timeout_ms / 1000. * handle_->first_block_timeout_multipler;

    EXPECT(window2_timeout_s >= expected_window2_min_s);
    EXPECT(window2_timeout_s > window1_timeout_s);

    co_return td::Unit{};
  }
};
// FIXME: Enable once we are spec compliant. The deviation is documented in section 4.2.
// REGISTER_TEST(SimplexConsensus, Rule6SkipTimeoutUsesLastObservedFinalization);

struct SkipPreventsLaterFinalizeForSameSlot : SimplexConsensusTest {
  TestOptions options() const override {
    auto opts = SimplexConsensusTest::options();
    opts.weight_distribution = {1, 1};
    opts.slots_per_leader_window = 1;
    opts.target_rate_ms = 1000;
    opts.first_block_timeout_ms = 1000;
    return opts;
  }

  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md Rule 5 negative gating:
    // once the local validator has already skipped a slot, later notarization evidence for that
    // same slot must not allow it to cast FinalizeVote.
    auto base = ParentId{make_candidate_id(0, 4100)};
    auto candidate = make_wait_candidate(make_candidate_id(1, 4101), base, PeerValidatorId{1});
    auto parent_state = make_normal_state(ctx().shard(), 11, min_mc_block_id);

    co_await handle_.publish<simplex::LeaderWindowObserved>(1, base);
    co_await ts_.wait_sync_work();

    ts_.advance_time(ts_.next_timeout_in());
    co_await ts_.wait_sync_work();

    auto skip_votes = published_votes<simplex::SkipVote>(observer().broadcast_votes_);
    ASSERT_EQ(skip_votes.size(), static_cast<size_t>(1));
    EXPECT_EQ(skip_votes[0].slot, static_cast<td::uint32>(1));

    observer().resolve_state_results_.push_back(simplex::ResolveState::Result{.state = parent_state});
    observer().validation_results_.push_back(CandidateAccept{});

    handle_.publish<CandidateReceived>(candidate);
    co_await ts_.wait_sync_work();

    ConsensusBus seed_bus;
    fill_simplex_bus(ctx(), seed_bus, 0);
    auto notar_cert = make_simplex_certificate(ctx(), seed_bus, simplex::NotarizeVote{candidate->id}, {0, 1});

    handle_.publish<simplex::NotarizationObserved>(candidate->id, notar_cert);
    co_await ts_.wait_sync_work();

    auto notar_votes = published_votes<simplex::NotarizeVote>(observer().broadcast_votes_);
    auto final_votes = published_votes<simplex::FinalizeVote>(observer().broadcast_votes_);
    ASSERT_EQ(notar_votes.size(), static_cast<size_t>(1));
    EXPECT_EQ(notar_votes[0].id, candidate->id);
    EXPECT_EQ(final_votes.size(), static_cast<size_t>(0));

    co_return td::Unit{};
  }
};
REGISTER_TEST(SimplexConsensus, SkipPreventsLaterFinalizeForSameSlot);

struct FinalizationPrunesOldSlotsAndSuppressesLateMessages : SimplexConsensusTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md Rule 4 and Rule 5 negative behavior:
    // once a later slot is finalized, late candidate and notarization events for older slots must
    // be ignored instead of restarting parent proofing, validation, or local voting.
    auto finalized_id = make_candidate_id(3, 4203);
    auto late_candidate =
        make_wait_candidate(make_candidate_id(2, 4202), ParentId{make_candidate_id(1, 4201)}, PeerValidatorId{1});

    ConsensusBus seed_bus;
    fill_simplex_bus(ctx(), seed_bus, 0);
    auto final_cert = make_simplex_certificate(ctx(), seed_bus, simplex::FinalizeVote{finalized_id}, {0, 1});
    auto late_notar = make_simplex_certificate(ctx(), seed_bus, simplex::NotarizeVote{late_candidate->id}, {0, 1});

    handle_.publish<simplex::FinalizationObserved>(finalized_id, final_cert);
    co_await ts_.wait_sync_work();

    handle_.publish<CandidateReceived>(late_candidate);
    handle_.publish<simplex::NotarizationObserved>(late_candidate->id, late_notar);
    co_await ts_.wait_sync_work();

    EXPECT_EQ(observer().wait_for_parent_candidate_ids_.size(), static_cast<size_t>(0));
    EXPECT_EQ(observer().resolve_state_requests_.size(), static_cast<size_t>(0));
    EXPECT_EQ(observer().stored_candidate_ids_.size(), static_cast<size_t>(0));
    EXPECT_EQ(observer().validation_candidate_ids_.size(), static_cast<size_t>(0));
    EXPECT_EQ(observer().broadcast_votes_.size(), static_cast<size_t>(0));

    co_return td::Unit{};
  }
};
REGISTER_TEST(SimplexConsensus, FinalizationPrunesOldSlotsAndSuppressesLateMessages);

}  // namespace
}  // namespace ton::validator::consensus::test
