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

namespace tl {

using db_key_vote = ton_api::consensus_simplex_db_key_vote;
using db_ourVote = ton_api::consensus_simplex_db_ourVote;
using db_voteLegacy = ton_api::consensus_simplex_db_voteLegacy;
using db_cert = ton_api::consensus_simplex_db_cert;
using db_Vote = ton_api::consensus_simplex_db_Vote;
using db_key_poolState = ton_api::consensus_simplex_db_key_poolState;
using db_poolState = ton_api::consensus_simplex_db_poolState;
using db_key_candidateResolver_notarCert = ton_api::consensus_simplex_db_key_candidateResolver_notarCert;
using db_candidateResolver_notarCert = ton_api::consensus_simplex_db_candidateResolver_notarCert;

}  // namespace tl

struct DbBus : simplex::Bus {
  using Parent = simplex::Bus;
  using Events = td::TypeList<>;
};

class SimplexDbTest : public td::Test {
 protected:
  std::unique_ptr<ValidatorSetup> ctx_;
  td::actor::TestScheduler ts_;
  std::unique_ptr<td::actor::Runtime> runtime_;
  td::actor::BusHandle<DbBus> handle_;
  DbBus* bus_{};
  TestDbImpl* db_{};

  virtual TestOptions options() const {
    return TestOptions{.weight_distribution = {1, 1}};
  }

  ValidatorSetup& ctx() {
    CHECK(ctx_ != nullptr);
    return *ctx_;
  }

  TestDbImpl& db() {
    CHECK(db_ != nullptr);
    return *db_;
  }

  DbBus& bus() {
    CHECK(bus_ != nullptr);
    return *bus_;
  }

  td::actor::Task<td::Unit> start_actor(
      const std::vector<std::pair<td::BufferSlice, td::BufferSlice>>& seed_entries = {}) {
    CHECK(!runtime_);
    auto bus = std::make_shared<DbBus>();
    fill_simplex_bus(ctx(), *bus, 0);
    bus->db = std::make_unique<TestDbImpl>();
    static_cast<TestDbImpl*>(bus->db.get())->seed_all(seed_entries);

    bus_ = bus.get();
    db_ = static_cast<TestDbImpl*>(bus->db.get());

    runtime_ = std::make_unique<td::actor::Runtime>();
    simplex::Db::register_in(*runtime_);
    handle_ = runtime_->start(std::move(bus));

    co_await ts_.wait_sync_work();
    co_return td::Unit{};
  }

  td::actor::Task<td::Unit> restart_actor() {
    auto snapshot = db().entries();
    co_await stop_actor();
    co_await start_actor(snapshot);
    co_return td::Unit{};
  }

  td::actor::Task<td::Unit> stop_actor() {
    if (!runtime_) {
      co_return td::Unit{};
    }

    handle_.publish<StopRequested>();
    co_await ts_.wait_sync_work();

    handle_ = {};
    bus_ = nullptr;
    db_ = nullptr;
    runtime_.reset();

    co_await ts_.wait_sync_work();
    co_return td::Unit{};
  }

  void run() final {
    ts_.run([this]() -> td::actor::Task<td::Unit> {
      ctx_ = std::make_unique<ValidatorSetup>(options());
      co_await run_test();
      co_await stop_actor();
      ctx_.reset();
      co_return td::Unit{};
    });
  }

  virtual td::actor::Task<td::Unit> run_test() = 0;
};

}  // namespace
}  // namespace ton::validator::consensus::test
