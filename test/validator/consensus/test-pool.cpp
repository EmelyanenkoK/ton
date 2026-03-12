/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "td/actor/Mocks.h"
#include "td/actor/TestScheduler.h"
#include "td/utils/tests.h"

#include <random>

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
  bool delay_save_certificate_ = false;
  std::vector<td::Promise<td::Unit>> delayed_save_certificate_promises_;

  void clear_events() {
    events_.clear();
  }

  void release_delayed_save_certificates() {
    auto promises = std::move(delayed_save_certificate_promises_);
    delayed_save_certificate_promises_.clear();
    for (auto& promise : promises) {
      promise.set_value(td::Unit{});
    }
  }

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
    if (delay_save_certificate_) {
      auto [task, promise] = td::actor::StartedTask<td::Unit>::make_bridge();
      delayed_save_certificate_promises_.push_back(std::move(promise));
      co_await std::move(task);
    }
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

  auto& observer() {
    CHECK(bus_ != nullptr);
    CHECK(bus_->observer != nullptr);
    return *bus_->observer;
  }

  void clear_events() {
    observer().clear_events();
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

bool contains_outgoing_payload(const ObservedEvents& events, td::Slice payload) {
  auto outgoing = events_of<OutgoingProtocolMessage>(events);
  for (const auto& event : outgoing) {
    if (event->message.data.as_slice() == payload) {
      return true;
    }
  }
  return false;
}

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

struct StandstillTriggersAtConfiguredTimeout : PoolTest {
  td::BufferSlice final_payload_;

  void configure_bus(PoolBus& bus) override {
    // Covers tracker Kernel-24 standstill timing:
    // once the pool is started, the standstill alarm must fire at the configured timeout even if
    // the pool state only comes from bootstrap certificates.
    bus.standstill_timeout_s = 5.0;
    auto final = make_simplex_certificate(ctx(), bus, simplex::FinalizeVote{make_candidate_id(0, 6100)}, {0, 1, 2});
    final_payload_ = final->serialize();
    bus.bootstrap_certificates.push_back(std::move(final.unique_write()).consume_and_upcast());
  }

  td::actor::Task<td::Unit> run_test() override {
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();
    clear_events();

    auto initial_timeout = ts_.next_timeout_in();
    EXPECT(initial_timeout > 4.0);

    ts_.advance_time(initial_timeout - 0.1);
    co_await ts_.wait_sync_work();
    EXPECT_EQ(count_events<OutgoingProtocolMessage>(events()), static_cast<size_t>(0));

    ts_.advance_time(0.1);
    co_await ts_.wait_sync_work();

    EXPECT(contains_outgoing_payload(events(), final_payload_.as_slice()));

    co_return {};
  }
};
REGISTER_TEST(Pool, StandstillTriggersAtConfiguredTimeout);

struct StandstillBroadcastContainsExpectedVotesAndCertificates : PoolTest {
  td::BufferSlice final_payload_;
  td::BufferSlice skip_payload_;

  void configure_bus(PoolBus& bus) override {
    // Covers tracker Kernel-24 standstill contents:
    // standstill rebroadcast must include the last final cert, later certificates, and our later
    // local votes that do not yet have certificates.
    bus.standstill_timeout_s = 3.0;
    auto final = make_simplex_certificate(ctx(), bus, simplex::FinalizeVote{make_candidate_id(0, 6200)}, {0, 1, 2});
    auto skip = make_simplex_certificate(ctx(), bus, simplex::SkipVote{1}, {0, 1, 2});
    final_payload_ = final->serialize();
    skip_payload_ = skip->serialize();
    bus.bootstrap_certificates.push_back(std::move(final.unique_write()).consume_and_upcast());
    bus.bootstrap_certificates.push_back(std::move(skip.unique_write()).consume_and_upcast());
  }

  td::actor::Task<td::Unit> run_test() override {
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto local_vote = simplex::NotarizeVote{make_candidate_id(2, 6202)};

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();

    (void)handle_.publish<simplex::BroadcastVote>(local_vote);
    co_await ts_.wait_sync_work();
    clear_events();

    ts_.advance_time(ts_.next_timeout_in());
    co_await ts_.wait_sync_work();

    EXPECT(contains_outgoing_payload(events(), final_payload_.as_slice()));
    EXPECT(contains_outgoing_payload(events(), skip_payload_.as_slice()));
    bool saw_local_vote = false;
    for (const auto& event : events_of<OutgoingProtocolMessage>(events())) {
      auto decoded = decode_simplex_signed_vote(event->message, PeerValidatorId{0}, *bus_);
      if (decoded.is_ok()) {
        if (auto* notar = std::get_if<simplex::NotarizeVote>(&decoded.ok().vote.vote);
            notar != nullptr && *notar == local_vote) {
          saw_local_vote = true;
          break;
        }
      }
    }
    EXPECT(saw_local_vote);

    co_return {};
  }
};
REGISTER_TEST(Pool, StandstillBroadcastContainsExpectedVotesAndCertificates);

struct WaitsForPersistentStoreBeforeUsingLocalVote : PoolTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers tracker Kernel-24 persistence-before-progress:
    // once local knowledge reaches quorum and would create a certificate, the pool must wait for
    // SaveCertificate to complete before broadcasting or observing the certificate.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto id = make_candidate_id(0, 6300);
    auto expected_cert = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{id}, {0, 1, 2});

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();
    clear_events();

    observer().delay_save_certificate_ = true;
    for (size_t i = 0; i < 3; ++i) {
      auto signed_vote = make_signed_simplex_vote(ctx(), *bus_, i, simplex::NotarizeVote{id});
      auto serialized =
          simplex::Signed<simplex::Vote>{signed_vote.validator, simplex::Vote{signed_vote.vote}, signed_vote.signature.clone()}
              .serialize();
      handle_.publish<IncomingProtocolMessage>(PeerValidatorId{i}, ProtocolMessage{std::move(serialized)});
    }
    co_await ts_.wait_sync_work();

    EXPECT_EQ(count_events<simplex::SaveCertificate>(events()), static_cast<size_t>(1));
    EXPECT_EQ(count_events<OutgoingProtocolMessage>(events()), static_cast<size_t>(0));
    EXPECT_EQ(count_events<simplex::NotarizationObserved>(events()), static_cast<size_t>(0));

    observer().delay_save_certificate_ = false;
    observer().release_delayed_save_certificates();
    co_await ts_.wait_sync_work();

    EXPECT_EQ(count_events<simplex::NotarizationObserved>(events()), static_cast<size_t>(1));
    EXPECT(contains_outgoing_payload(events(), expected_cert->serialize().as_slice()));

    co_return {};
  }
};
REGISTER_TEST(Pool, WaitsForPersistentStoreBeforeUsingLocalVote);

struct RebroadcastsReceivedCertificateImmediately : PoolTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers tracker Kernel-24 receive-side rebroadcast:
    // once a certificate is received from the network after init, the pool must broadcast it
    // immediately instead of only waiting for standstill recovery.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();
    clear_events();

    PoolBus seed_bus;
    fill_simplex_bus(ctx(), seed_bus, 1);
    auto cert = make_simplex_certificate(ctx(), seed_bus, simplex::SkipVote{1}, {0, 1, 2});
    handle_.publish<IncomingProtocolMessage>(PeerValidatorId{1}, ProtocolMessage{cert->serialize()});
    co_await ts_.wait_sync_work();

    EXPECT(contains_outgoing_payload(events(), cert->serialize().as_slice()));

    co_return {};
  }
};
REGISTER_TEST(Pool, RebroadcastsReceivedCertificateImmediately);

struct RebroadcastsGeneratedCertificateImmediately : PoolTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers tracker Kernel-24 generation-side rebroadcast:
    // once local vote weight reaches quorum and forms a certificate, the pool must broadcast the
    // new certificate immediately after it is saved.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto id = make_candidate_id(0, 6400);
    auto expected_cert = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{id}, {0, 1, 2});

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();
    clear_events();

    for (size_t i = 0; i < 3; ++i) {
      auto signed_vote = make_signed_simplex_vote(ctx(), *bus_, i, simplex::NotarizeVote{id});
      auto serialized =
          simplex::Signed<simplex::Vote>{signed_vote.validator, simplex::Vote{signed_vote.vote}, signed_vote.signature.clone()}
              .serialize();
      handle_.publish<IncomingProtocolMessage>(PeerValidatorId{i}, ProtocolMessage{std::move(serialized)});
    }
    co_await ts_.wait_sync_work();

    EXPECT_EQ(count_events<simplex::NotarizationObserved>(events()), static_cast<size_t>(1));
    EXPECT(contains_outgoing_payload(events(), expected_cert->serialize().as_slice()));

    co_return {};
  }
};
REGISTER_TEST(Pool, RebroadcastsGeneratedCertificateImmediately);

struct ParallelQueriesDoNotCorruptPoolState : PoolTest {
  CandidateId parent_id_ = make_candidate_id(0, 6500);

  void configure_bus(PoolBus& bus) override {
    auto parent_notar = make_simplex_certificate(ctx(), bus, simplex::NotarizeVote{parent_id_}, {0, 1, 2});
    auto first_gap_skip = make_simplex_certificate(ctx(), bus, simplex::SkipVote{1}, {0, 1, 2});
    bus.bootstrap_certificates.push_back(std::move(parent_notar.unique_write()).consume_and_upcast());
    bus.bootstrap_certificates.push_back(std::move(first_gap_skip.unique_write()).consume_and_upcast());
  }

  td::actor::Task<td::Unit> run_test() override {
    // Covers tracker Kernel-24 concurrent request handling:
    // multiple WaitForParent requests that share overlapping skipped gaps must all resolve
    // correctly when the missing certificates arrive, without corrupting the pending-request list.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto candidate_a = make_wait_candidate(make_candidate_id(3, 6503), ParentId{parent_id_});
    auto candidate_b = make_wait_candidate(make_candidate_id(4, 6504), ParentId{parent_id_});

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();

    std::optional<td::Result<std::optional<MisbehaviorRef>>> result_a1;
    std::optional<td::Result<std::optional<MisbehaviorRef>>> result_a2;
    std::optional<td::Result<std::optional<MisbehaviorRef>>> result_b;

    auto wait_a1 = [&]() -> td::actor::Task<td::Unit> {
      result_a1 = co_await handle_.publish<simplex::WaitForParent>(candidate_a).wrap();
      co_return td::Unit{};
    };
    auto wait_a2 = [&]() -> td::actor::Task<td::Unit> {
      result_a2 = co_await handle_.publish<simplex::WaitForParent>(candidate_a).wrap();
      co_return td::Unit{};
    };
    auto wait_b = [&]() -> td::actor::Task<td::Unit> {
      result_b = co_await handle_.publish<simplex::WaitForParent>(candidate_b).wrap();
      co_return td::Unit{};
    };

    wait_a1().start().detach();
    wait_a2().start().detach();
    wait_b().start().detach();
    co_await ts_.wait_sync_work();

    EXPECT(!result_a1.has_value());
    EXPECT(!result_a2.has_value());
    EXPECT(!result_b.has_value());

    PoolBus seed_bus;
    fill_simplex_bus(ctx(), seed_bus, 1);
    auto skip2 = make_simplex_certificate(ctx(), seed_bus, simplex::SkipVote{2}, {0, 1, 2});
    handle_.publish<IncomingProtocolMessage>(PeerValidatorId{1}, ProtocolMessage{skip2->serialize()});
    co_await ts_.wait_sync_work();

    ASSERT_TRUE(result_a1.has_value());
    ASSERT_TRUE(result_a2.has_value());
    EXPECT(!result_b.has_value());
    EXPECT(result_a1->is_ok() && !result_a1->ok().has_value());
    EXPECT(result_a2->is_ok() && !result_a2->ok().has_value());

    auto skip3 = make_simplex_certificate(ctx(), seed_bus, simplex::SkipVote{3}, {0, 1, 2});
    handle_.publish<IncomingProtocolMessage>(PeerValidatorId{1}, ProtocolMessage{skip3->serialize()});
    co_await ts_.wait_sync_work();

    ASSERT_TRUE(result_b.has_value());
    EXPECT(result_b->is_ok() && !result_b->ok().has_value());
    EXPECT_EQ(count_events<MisbehaviorReport>(events()), static_cast<size_t>(0));

    co_return {};
  }
};
REGISTER_TEST(Pool, ParallelQueriesDoNotCorruptPoolState);

struct LongForkRollbackDoesNotBreakFinalizedFrontier : PoolTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers tracker Kernel-24 rollback/frontier recovery:
    // once a later final cert advances the finalized frontier, late certificates from an older fork
    // must not reopen cleared slots or move the frontier backwards. The current frontier should
    // still accept descendants of the finalized block, while stale parents from the old fork are
    // rejected as conflicting.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto final_id = make_candidate_id(0, 6600);
    auto stale_parent = make_candidate_id(0, 6611);

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();
    clear_events();

    PoolBus seed_bus;
    fill_simplex_bus(ctx(), seed_bus, 1);
    auto final_cert = make_simplex_certificate(ctx(), seed_bus, simplex::FinalizeVote{final_id}, {0, 1, 2});
    handle_.publish<IncomingProtocolMessage>(PeerValidatorId{1}, ProtocolMessage{final_cert->serialize()});
    co_await ts_.wait_sync_work();

    auto finalized = events_of<simplex::FinalizationObserved>(events());
    ASSERT_EQ(finalized.size(), static_cast<size_t>(1));
    EXPECT_EQ(finalized[0]->id, final_id);

    auto current_candidate = make_wait_candidate(make_candidate_id(1, 6601), ParentId{final_id});
    auto current_result = co_await handle_.publish<simplex::WaitForParent>(current_candidate).wrap();
    ASSERT_TRUE(current_result.is_ok());
    EXPECT(!current_result.ok().has_value());

    clear_events();

    auto stale_notar = make_simplex_certificate(ctx(), seed_bus, simplex::NotarizeVote{stale_parent}, {0, 1, 2});
    handle_.publish<IncomingProtocolMessage>(PeerValidatorId{1}, ProtocolMessage{stale_notar->serialize()});
    co_await ts_.wait_sync_work();

    EXPECT_EQ(count_events<simplex::SaveCertificate>(events()), static_cast<size_t>(0));
    EXPECT_EQ(count_events<simplex::NotarizationObserved>(events()), static_cast<size_t>(0));
    EXPECT_EQ(count_events<simplex::FinalizationObserved>(events()), static_cast<size_t>(0));

    auto candidate = make_wait_candidate(make_candidate_id(1, 6604), ParentId{stale_parent});
    auto result = co_await handle_.publish<simplex::WaitForParent>(candidate).wrap();
    ASSERT_TRUE(result.is_ok());
    ASSERT_TRUE(result.ok().has_value());

    co_return {};
  }
};
REGISTER_TEST(Pool, LongForkRollbackDoesNotBreakFinalizedFrontier);

struct DeterministicInterleavingStressPreservesPoolState : PoolTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers tracker Kernel-24 deterministic stress/interleaving:
    // a shuffled but fixed sequence of incoming votes must still converge to the same certificate
    // set and frontier proofs without producing misbehavior or duplicate certificate saves.
    auto state = make_normal_state(ctx().shard(), 1, min_mc_block_id);
    auto notar_id = make_candidate_id(0, 6700);
    auto expected_notar = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{notar_id}, {0, 1, 2});
    auto expected_skip = make_simplex_certificate(ctx(), *bus_, simplex::SkipVote{1}, {0, 1, 2});

    handle_.publish<Start>(state);
    co_await ts_.wait_sync_work();
    clear_events();

    std::vector<std::pair<PeerValidatorId, ProtocolMessage>> messages;
    for (size_t i = 0; i < 3; ++i) {
      auto signed_notar = make_signed_simplex_vote(ctx(), *bus_, i, simplex::NotarizeVote{notar_id});
      messages.emplace_back(
          PeerValidatorId{i},
          ProtocolMessage{simplex::Signed<simplex::Vote>{signed_notar.validator, simplex::Vote{signed_notar.vote},
                                                         signed_notar.signature.clone()}
                              .serialize()});
      auto signed_skip = make_signed_simplex_vote(ctx(), *bus_, i, simplex::SkipVote{1});
      messages.emplace_back(
          PeerValidatorId{i},
          ProtocolMessage{simplex::Signed<simplex::Vote>{signed_skip.validator, simplex::Vote{signed_skip.vote},
                                                         signed_skip.signature.clone()}
                              .serialize()});
    }
    // Deterministic duplicates and ordering noise.
    auto dup_notar = make_signed_simplex_vote(ctx(), *bus_, 0, simplex::NotarizeVote{notar_id});
    messages.emplace_back(PeerValidatorId{0},
                          ProtocolMessage{simplex::Signed<simplex::Vote>{dup_notar.validator,
                                                                         simplex::Vote{dup_notar.vote},
                                                                         dup_notar.signature.clone()}
                                              .serialize()});
    auto dup_skip = make_signed_simplex_vote(ctx(), *bus_, 1, simplex::SkipVote{1});
    messages.emplace_back(PeerValidatorId{1},
                          ProtocolMessage{simplex::Signed<simplex::Vote>{dup_skip.validator, simplex::Vote{dup_skip.vote},
                                                                         dup_skip.signature.clone()}
                                              .serialize()});

    std::mt19937 rng(20260312);
    std::shuffle(messages.begin(), messages.end(), rng);

    for (auto& [peer, message] : messages) {
      handle_.publish<IncomingProtocolMessage>(peer, std::move(message));
      co_await ts_.wait_sync_work();
    }

    EXPECT_EQ(count_events<MisbehaviorReport>(events()), static_cast<size_t>(0));
    EXPECT_EQ(count_events<simplex::SaveCertificate>(events()), static_cast<size_t>(2));
    EXPECT_EQ(count_events<simplex::NotarizationObserved>(events()), static_cast<size_t>(1));
    EXPECT_EQ(count_events<simplex::FinalizationObserved>(events()), static_cast<size_t>(0));
    EXPECT(contains_outgoing_payload(events(), expected_notar->serialize().as_slice()));
    EXPECT(contains_outgoing_payload(events(), expected_skip->serialize().as_slice()));

    auto next_candidate = make_wait_candidate(make_candidate_id(2, 6702), ParentId{notar_id});
    auto next_result = co_await handle_.publish<simplex::WaitForParent>(next_candidate).wrap();
    ASSERT_TRUE(next_result.is_ok());
    EXPECT(!next_result.ok().has_value());

    co_return {};
  }
};
REGISTER_TEST(Pool, DeterministicInterleavingStressPreservesPoolState);

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
