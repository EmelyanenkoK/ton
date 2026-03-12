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

using ConsensusMockBus =
    td::actor::MockBus<simplex::Bus, simplex::BroadcastVote, simplex::ResolveState, simplex::WaitForParent,
                       simplex::StoreCandidate, ValidationRequest, OurLeaderWindowStarted, MisbehaviorReport>;

template <typename VoteT>
std::vector<VoteT> published_votes(const ConsensusMockBus& bus) {
  std::vector<VoteT> result;
  for (const auto& event : bus.actor->events_) {
    if (auto* vote = std::get_if<std::shared_ptr<const simplex::BroadcastVote>>(&event)) {
      if (auto* typed = std::get_if<VoteT>(&(*vote)->vote.vote)) {
        result.push_back(*typed);
      }
    }
  }
  return result;
}

class SimplexConsensusTest : public td::Test {
 protected:
  std::unique_ptr<ValidatorSetup> ctx_;
  td::actor::TestScheduler ts_;
  td::actor::BusHandle<ConsensusMockBus> handle_;

  virtual TestOptions options() const {
    return TestOptions{.weight_distribution = {1, 1}};
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

      auto bus = std::make_shared<ConsensusMockBus>();
      fill_simplex_bus(ctx(), *bus, 0);
      bus->db = std::make_unique<TestDbImpl>();

      auto runtime = ConsensusMockBus::create_runtime();
      simplex::Consensus::register_in(runtime);
      handle_ = runtime.start(std::move(bus));

      co_await ts_.wait_sync_work();
      co_await run_test();

      handle_.publish<StopRequested>();
      co_await ts_.wait_sync_work();

      handle_ = {};
      runtime = {};
      ctx_.reset();

      co_await ts_.wait_sync_work();
      co_return td::Unit{};
    });
  }

  virtual td::actor::Task<td::Unit> run_test() = 0;
};

}  // namespace
}  // namespace ton::validator::consensus::test
