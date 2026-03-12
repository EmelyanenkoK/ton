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

using ObservedEvents =
    std::vector<std::variant<std::shared_ptr<const simplex::SaveCertificate>,
                             std::shared_ptr<const simplex::LeaderWindowObserved>,
                             std::shared_ptr<const OutgoingProtocolMessage>,
                             std::shared_ptr<const simplex::NotarizationObserved>,
                             std::shared_ptr<const simplex::FinalizationObserved>,
                             std::shared_ptr<const MisbehaviorReport>>>;

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

}  // namespace
}  // namespace ton::validator::consensus::test
