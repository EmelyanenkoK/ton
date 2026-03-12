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

using ObservedEvents = std::vector<
    std::variant<std::shared_ptr<const simplex::SaveCertificate>, std::shared_ptr<const simplex::LeaderWindowObserved>,
                 std::shared_ptr<const OutgoingProtocolMessage>, std::shared_ptr<const simplex::NotarizationObserved>,
                 std::shared_ptr<const simplex::FinalizationObserved>, std::shared_ptr<const MisbehaviorReport>>>;

class PoolObserverImpl;

struct PoolBus : simplex::Bus {
  using Parent = simplex::Bus;
  using Events = td::TypeList<>;

  PoolObserverImpl* observer = nullptr;
};

class PoolObserverImpl : public td::actor::SpawnsWith<PoolBus>, public td::actor::ConnectsTo<PoolBus> {
 public:
  TON_RUNTIME_DEFINE_EVENT_HANDLER();

  ObservedEvents events_;

  void start_up() override {
    const_cast<PoolBus&>(*owning_bus()).observer = this;
  }

  template <>
  void handle(td::actor::BusHandle<PoolBus>, std::shared_ptr<const StopRequested>) {
    stop();
  }

  template <>
  void handle(td::actor::BusHandle<PoolBus>, std::shared_ptr<const OutgoingProtocolMessage> event) {
    events_.emplace_back(std::move(event));
  }

  template <>
  void handle(td::actor::BusHandle<PoolBus>, std::shared_ptr<const simplex::NotarizationObserved> event) {
    events_.emplace_back(std::move(event));
  }

  template <>
  void handle(td::actor::BusHandle<PoolBus>, std::shared_ptr<const simplex::FinalizationObserved> event) {
    events_.emplace_back(std::move(event));
  }

  template <>
  void handle(td::actor::BusHandle<PoolBus>, std::shared_ptr<const MisbehaviorReport> event) {
    events_.emplace_back(std::move(event));
  }

  template <>
  td::actor::Task<td::Unit> process(td::actor::BusHandle<PoolBus>, std::shared_ptr<simplex::SaveCertificate> event) {
    events_.emplace_back(std::move(event));
    co_return td::Unit{};
  }

  template <>
  td::actor::Task<td::Unit> process(td::actor::BusHandle<PoolBus>,
                                    std::shared_ptr<simplex::LeaderWindowObserved> event) {
    events_.emplace_back(std::move(event));
    co_return td::Unit{};
  }
};

class PoolTest : public td::Test {
 protected:
  std::unique_ptr<ValidatorSetup> ctx_;
  td::actor::TestScheduler ts_;
  td::actor::BusHandle<PoolBus> handle_;
  PoolBus* bus_{};

  virtual TestOptions options() const {
    return TestOptions{.weight_distribution = {1, 1, 1, 1}};
  }

  ValidatorSetup& ctx() {
    CHECK(ctx_ != nullptr);
    return *ctx_;
  }

  auto& events() {
    return bus_->observer->events_;
  }

  virtual void configure_bus(PoolBus&) {
  }

  void run() final {
    ts_.run([this]() -> td::actor::Task<td::Unit> {
      ctx_ = std::make_unique<ValidatorSetup>(options());

      auto keyring_own = ctx().create_keyring(0);

      auto bus = std::make_shared<PoolBus>();
      fill_simplex_bus(ctx(), *bus, 0);
      bus->keyring = keyring_own.get();
      bus->db = std::make_unique<TestDbImpl>();
      configure_bus(*bus);

      bus_ = bus.get();

      td::actor::Runtime runtime;
      runtime.register_actor<PoolObserverImpl>("PoolObserver");
      simplex::Pool::register_in(runtime);
      handle_ = runtime.start(std::move(bus));

      co_await ts_.wait_sync_work();
      co_await run_test();

      handle_.publish<StopRequested>();
      co_await ts_.wait_sync_work();

      handle_ = {};
      bus_ = nullptr;
      runtime = {};
      keyring_own = {};
      ctx_.reset();

      co_await ts_.wait_sync_work();

      co_return td::Unit{};
    });
  }

  virtual td::actor::Task<td::Unit> run_test() = 0;
};

struct ResolvesWaitForParentFromBootstrapProofs : PoolTest {
  CandidateId parent_id_ = make_candidate_id(1, 1001);

  void configure_bus(PoolBus& bus) override {
    // Covers simplex_docs.md Rule 4:
    // once the parent notarization and the skipped-gap certificates are already available locally,
    // WaitForParent should resolve immediately without reporting misbehavior.
    auto parent_notar = make_simplex_certificate(ctx(), bus, simplex::NotarizeVote{parent_id_}, {0, 1, 2});
    auto gap_skip = make_simplex_certificate(ctx(), bus, simplex::SkipVote{2}, {0, 1, 2});
    bus.bootstrap_certificates.push_back(std::move(parent_notar.unique_write()).consume_and_upcast());
    bus.bootstrap_certificates.push_back(std::move(gap_skip.unique_write()).consume_and_upcast());
  }

  td::actor::Task<td::Unit> run_test() override {
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto candidate = make_wait_candidate(make_candidate_id(3, 3003), ParentId{parent_id_});

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();

    auto result = co_await handle_.publish<simplex::WaitForParent>(candidate).wrap();
    ASSERT_TRUE(result.is_ok());
    EXPECT(!result.ok().has_value());
    EXPECT_EQ(count_events<simplex::SaveCertificate>(events()), static_cast<size_t>(0));

    co_return {};
  }
};
REGISTER_TEST(Pool, ResolvesWaitForParentFromBootstrapProofs);

struct AnnouncesNextLeaderWindowWithBootstrapBase : PoolTest {
  CandidateId base_id_ = make_candidate_id(0, 4000);

  TestOptions options() const override {
    auto opts = PoolTest::options();
    opts.slots_per_leader_window = 2;
    return opts;
  }

  void configure_bus(PoolBus& bus) override {
    // Covers simplex_docs.md Rule 1 and Rule 3:
    // once notarization and skips clear the frontier, the next announced leader window must carry
    // the latest available base through the skipped range.
    auto notar = make_simplex_certificate(ctx(), bus, simplex::NotarizeVote{base_id_}, {0, 1, 2});
    auto skip = make_simplex_certificate(ctx(), bus, simplex::SkipVote{1}, {0, 1, 2});
    bus.bootstrap_certificates.push_back(std::move(notar.unique_write()).consume_and_upcast());
    bus.bootstrap_certificates.push_back(std::move(skip.unique_write()).consume_and_upcast());
  }

  td::actor::Task<td::Unit> run_test() override {
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();

    auto observed = events_of<simplex::LeaderWindowObserved>(events());
    ASSERT_EQ(observed.size(), static_cast<size_t>(1));
    EXPECT_EQ(observed[0]->start_slot, static_cast<td::uint32>(2));
    EXPECT_EQ(observed[0]->base, ParentId{base_id_});

    co_return {};
  }
};
REGISTER_TEST(Pool, AnnouncesNextLeaderWindowWithBootstrapBase);

struct RejectsCandidateWhoseParentFallsBeforeFinalizedFrontier : PoolTest {
  CandidateId finalized_id_ = make_candidate_id(2, 5002);

  void configure_bus(PoolBus& bus) override {
    auto final = make_simplex_certificate(ctx(), bus, simplex::FinalizeVote{finalized_id_}, {0, 1, 2});
    bus.bootstrap_certificates.push_back(std::move(final.unique_write()).consume_and_upcast());
  }

  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md Rule 4 negative gating:
    // if a candidate's parent would place its chain behind the finalized frontier, WaitForParent
    // must reject it as conflicting instead of letting notarization proceed.
    auto state = make_normal_state(ctx().shard(), 3, min_mc_block_id);
    auto candidate = make_wait_candidate(make_candidate_id(4, 5004), ParentId{make_candidate_id(1, 5001)});

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();

    auto result = co_await handle_.publish<simplex::WaitForParent>(candidate).wrap();
    ASSERT_TRUE(result.is_ok());
    ASSERT_TRUE(result.ok().has_value());
    EXPECT_EQ(count_events<simplex::SaveCertificate>(events()), static_cast<size_t>(0));

    co_return {};
  }
};
REGISTER_TEST(Pool, RejectsCandidateWhoseParentFallsBeforeFinalizedFrontier);

struct WaitsForMissingGapSkipCertificates : PoolTest {
  CandidateId parent_id_ = make_candidate_id(0, 6000);
  CandidateId candidate_id_ = make_candidate_id(3, 6003);

  void configure_bus(PoolBus& bus) override {
    auto parent_notar = make_simplex_certificate(ctx(), bus, simplex::NotarizeVote{parent_id_}, {0, 1, 2});
    auto first_gap_skip = make_simplex_certificate(ctx(), bus, simplex::SkipVote{1}, {0, 1, 2});
    bus.bootstrap_certificates.push_back(std::move(parent_notar.unique_write()).consume_and_upcast());
    bus.bootstrap_certificates.push_back(std::move(first_gap_skip.unique_write()).consume_and_upcast());
  }

  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md Rule 4 negative gating:
    // when a candidate spans skipped slots, WaitForParent must stay blocked until every missing
    // skip certificate in the gap is locally known, then resume once the final missing skip cert arrives.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto candidate = make_wait_candidate(candidate_id_, ParentId{parent_id_});

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();

    std::optional<td::Result<std::optional<MisbehaviorRef>>> wait_result;
    bool completed = false;
    auto wait_for_parent_request = [&]() -> td::actor::Task<td::Unit> {
      wait_result = co_await handle_.publish<simplex::WaitForParent>(candidate).wrap();
      completed = true;
      co_return td::Unit{};
    };
    wait_for_parent_request().start().detach();

    co_await ts_.wait_sync_work();
    EXPECT(!completed);

    PoolBus seed_bus;
    fill_simplex_bus(ctx(), seed_bus, 1);
    auto second_gap_skip = make_simplex_certificate(ctx(), seed_bus, simplex::SkipVote{2}, {0, 1, 2});
    handle_.publish<IncomingProtocolMessage>(PeerValidatorId{1}, ProtocolMessage{second_gap_skip->serialize()});
    co_await ts_.wait_sync_work();
    EXPECT(completed);

    EXPECT_EQ(count_events<simplex::SaveCertificate>(events()), static_cast<size_t>(1));
    ASSERT_TRUE(completed);
    ASSERT_TRUE(wait_result.has_value());
    ASSERT_TRUE(wait_result->is_ok());
    EXPECT(!wait_result->ok().has_value());

    co_return {};
  }
};
REGISTER_TEST(Pool, WaitsForMissingGapSkipCertificates);

struct BootstrapVoteReplayDoesNotLeakToNetwork : PoolTest {
  // Regression test: handle_our_vote() is async (awaits key signing), so when
  // start_up() detaches it for each bootstrap vote and then sets is_inited_ = true
  // synchronously, the coroutine resumes after init and hits the network send path,
  // causing stale historical votes to be broadcast.

  CandidateId id_ = make_candidate_id(0, 7000);

  void configure_bus(PoolBus& bus) override {
    // Seed a notarize vote as if it were persisted from a prior session.
    bus.bootstrap_votes.push_back(simplex::Vote{simplex::NotarizeVote{id_}});
  }

  td::actor::Task<td::Unit> run_test() override {
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();

    // Bootstrap vote replay should NOT produce any OutgoingProtocolMessage.
    auto outgoing = events_of<OutgoingProtocolMessage>(events());
    EXPECT_EQ(outgoing.size(), static_cast<size_t>(0));

    co_return {};
  }
};
REGISTER_TEST(Pool, BootstrapVoteReplayDoesNotLeakToNetwork);

struct BootstrapCertificateReplayDoesNotBroadcast : PoolTest {
  // Certificates from bootstrap_certificates must not be broadcast.
  CandidateId id_ = make_candidate_id(0, 7100);

  void configure_bus(PoolBus& bus) override {
    auto notar = make_simplex_certificate(ctx(), bus, simplex::NotarizeVote{id_}, {0, 1, 2});
    bus.bootstrap_certificates.push_back(std::move(notar.unique_write()).consume_and_upcast());
  }

  td::actor::Task<td::Unit> run_test() override {
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();

    EXPECT_EQ(count_events<OutgoingProtocolMessage>(events()), static_cast<size_t>(0));
    co_return {};
  }
};
REGISTER_TEST(Pool, BootstrapCertificateReplayDoesNotBroadcast);

struct PostInitMessagesBroadcastNormally : PoolTest {
  // After init, certificates assembled from vote quorum and certificates received
  // from the network must be broadcast as OutgoingProtocolMessages.
  CandidateId quorum_id_ = make_candidate_id(0, 7200);

  td::actor::Task<td::Unit> run_test() override {
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();

    // Feed 3 foreign notarize votes for slot 0 to form a quorum (threshold = 3).
    for (size_t i = 0; i < 3; ++i) {
      auto signed_vote = make_signed_simplex_vote(ctx(), *bus_, i, simplex::NotarizeVote{quorum_id_});
      auto serialized =
          simplex::Signed<simplex::Vote>{signed_vote.validator, signed_vote.vote, signed_vote.signature.clone()}
              .serialize();
      handle_.publish<IncomingProtocolMessage>(PeerValidatorId{i}, ProtocolMessage{std::move(serialized)});
    }
    co_await ts_.wait_sync_work();

    // Also feed a foreign skip certificate for slot 1 directly from the network.
    PoolBus seed_bus;
    fill_simplex_bus(ctx(), seed_bus, 1);
    auto skip_cert = make_simplex_certificate(ctx(), seed_bus, simplex::SkipVote{1}, {0, 1, 2});
    handle_.publish<IncomingProtocolMessage>(PeerValidatorId{1}, ProtocolMessage{skip_cert->serialize()});
    co_await ts_.wait_sync_work();

    // Expect at least 2 broadcasts: the quorum-assembled notarize certificate and the skip certificate.
    auto outgoing = events_of<OutgoingProtocolMessage>(events());
    EXPECT(outgoing.size() >= static_cast<size_t>(2));
    co_return {};
  }
};
REGISTER_TEST(Pool, PostInitMessagesBroadcastNormally);

}  // namespace
}  // namespace ton::validator::consensus::test
