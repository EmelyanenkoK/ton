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

td::BufferSlice db_vote_key(const simplex::Vote& vote) {
  auto hash = sha256_bits256(serialize_tl_object(vote.to_tl(), true));
  return create_serialize_tl_object<tl::db_key_vote>(hash);
}

template <typename VoteT>
void seed_legacy_vote(TestDbImpl& db, const simplex::Signed<VoteT>& vote) {
  db.seed(db_vote_key(simplex::Vote{vote.vote}), create_serialize_tl_object<tl::db_voteLegacy>(
                                                     vote.serialize(), static_cast<td::int32>(vote.validator.value())));
}

void seed_our_vote(TestDbImpl& db, const simplex::Vote& vote, td::int64 seqno) {
  db.seed(db_vote_key(vote), create_serialize_tl_object<tl::db_ourVote>(vote.to_tl(), seqno));
}

void seed_certificate(TestDbImpl& db, const simplex::CertificateRef<simplex::Vote>& cert) {
  db.seed(db_vote_key(cert->vote), create_serialize_tl_object<tl::db_cert>(cert->to_tl()));
}

void seed_candidate_resolver_notar(TestDbImpl& db, CandidateId id, const simplex::NotarCertRef& cert) {
  db.seed(create_serialize_tl_object<tl::db_key_candidateResolver_notarCert>(id.to_tl()),
          create_serialize_tl_object<tl::db_candidateResolver_notarCert>(cert->to_tl_vote_signature_set()));
}

void seed_pool_state(TestDbImpl& db, td::uint32 first_nonannounced_window) {
  db.seed(create_serialize_tl_object<tl::db_key_poolState>(),
          create_serialize_tl_object<tl::db_poolState>(static_cast<td::int32>(first_nonannounced_window)));
}

template <typename VoteT>
bool has_certificate_for(const std::vector<simplex::CertificateRef<simplex::Vote>>& certs, const VoteT& vote) {
  for (const auto& cert : certs) {
    if (auto* typed = std::get_if<VoteT>(&cert->vote.vote)) {
      if (*typed == vote) {
        return true;
      }
    }
  }
  return false;
}

td::int64 stored_our_vote_seqno(const TestDbImpl& db, const simplex::Vote& vote) {
  auto stored = db.get(db_vote_key(vote).as_slice());
  CHECK(stored.has_value());
  auto value = fetch_tl_object<tl::db_ourVote>(*stored, true).move_as_ok();
  return value->seqno_;
}

td::uint32 stored_first_nonannounced_window(const TestDbImpl& db) {
  auto stored = db.get(create_serialize_tl_object<tl::db_key_poolState>().as_slice());
  CHECK(stored.has_value());
  auto value = fetch_tl_object<tl::db_poolState>(*stored, true).move_as_ok();
  return static_cast<td::uint32>(value->first_nonannounced_window_);
}

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

struct RestoresBootstrapStateFromDb : SimplexDbTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers the restart image that simplex uses for Rule 2 and Rule 4/5 implementation:
    // Db startup must restore local votes in replay order, merge bootstrap certificates from both
    // namespaces, and restore the first unannounced leader window.
    DbBus seed_bus;
    fill_simplex_bus(ctx(), seed_bus, 0);

    auto legacy_vote = make_signed_simplex_vote(ctx(), seed_bus, 0, simplex::SkipVote{2});
    auto foreign_legacy_vote = make_signed_simplex_vote(ctx(), seed_bus, 1, simplex::SkipVote{9});
    auto early_notar = simplex::NotarizeVote{make_candidate_id(4, 4004)};
    auto late_final = simplex::FinalizeVote{make_candidate_id(6, 6006)};
    auto early_vote = simplex::Vote{early_notar};
    auto late_vote = simplex::Vote{late_final};

    auto cert_vote = simplex::NotarizeVote{make_candidate_id(7, 7007)};
    auto cert_ref = make_simplex_certificate(ctx(), seed_bus, cert_vote, {0, 1});
    auto cert = std::move(cert_ref.unique_write()).consume_and_upcast();
    auto resolver_vote = simplex::NotarizeVote{make_candidate_id(8, 8008)};
    auto resolver_cert = make_simplex_certificate(ctx(), seed_bus, resolver_vote, {0, 1});

    auto seed_entries = [&] {
      TestDbImpl seed_db;
      seed_pool_state(seed_db, 5);
      seed_legacy_vote(seed_db, legacy_vote);
      seed_legacy_vote(seed_db, foreign_legacy_vote);
      seed_our_vote(seed_db, late_vote, 11);
      seed_our_vote(seed_db, early_vote, 3);
      seed_certificate(seed_db, cert);
      seed_candidate_resolver_notar(seed_db, resolver_vote.id, resolver_cert);
      return seed_db.entries();
    }();

    co_await start_actor(seed_entries);

    EXPECT_EQ(handle_->first_nonannounced_window, static_cast<td::uint32>(5));

    ASSERT_EQ(handle_->bootstrap_votes.size(), static_cast<size_t>(3));
    EXPECT(std::holds_alternative<simplex::SkipVote>(handle_->bootstrap_votes[0].vote));
    EXPECT_EQ(std::get<simplex::NotarizeVote>(handle_->bootstrap_votes[1].vote), early_notar);
    EXPECT_EQ(std::get<simplex::FinalizeVote>(handle_->bootstrap_votes[2].vote), late_final);

    ASSERT_EQ(handle_->bootstrap_certificates.size(), static_cast<size_t>(2));
    EXPECT(has_certificate_for(handle_->bootstrap_certificates, cert_vote));
    EXPECT(has_certificate_for(handle_->bootstrap_certificates, resolver_vote));

    co_return td::Unit{};
  }
};
REGISTER_TEST(SimplexDb, RestoresBootstrapStateFromDb);

struct PersistsBroadcastVotesWithSequentialSeqno : SimplexDbTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers the durable local-vote replay image used by Rule 4 and Rule 5:
    // every locally broadcast vote must be stored once, in a deterministic sequence order, so
    // restart replay can reconstruct the exact local voting history.
    co_await start_actor();

    auto first = simplex::Vote{simplex::NotarizeVote{make_candidate_id(10, 1010)}};
    auto second = simplex::Vote{simplex::FinalizeVote{make_candidate_id(11, 1111)}};

    co_await handle_.publish<simplex::BroadcastVote>(first);
    co_await handle_.publish<simplex::BroadcastVote>(second);
    EXPECT_EQ(db().size(), static_cast<size_t>(2));
    EXPECT_EQ(stored_our_vote_seqno(db(), first), static_cast<td::int64>(0));
    EXPECT_EQ(stored_our_vote_seqno(db(), second), static_cast<td::int64>(1));

    co_return td::Unit{};
  }
};
REGISTER_TEST(SimplexDb, PersistsBroadcastVotesWithSequentialSeqno);

struct RejectsDuplicateBroadcastVotes : SimplexDbTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers the Db-side local vote uniqueness guard backing simplex's per-slot vote discipline:
    // if the exact same locally broadcast vote is replayed, the actor must reject the duplicate
    // instead of writing a second durable copy.
    co_await start_actor();

    auto vote = simplex::Vote{simplex::NotarizeVote{make_candidate_id(12, 1212)}};

    auto first = co_await handle_.publish<simplex::BroadcastVote>(vote).wrap();
    auto duplicate = co_await handle_.publish<simplex::BroadcastVote>(vote).wrap();

    ASSERT_TRUE(first.is_ok());
    ASSERT_TRUE(duplicate.is_error());
    EXPECT_EQ(db().size(), static_cast<size_t>(1));
    EXPECT_EQ(stored_our_vote_seqno(db(), vote), static_cast<td::int64>(0));

    co_return td::Unit{};
  }
};
REGISTER_TEST(SimplexDb, RejectsDuplicateBroadcastVotes);

struct PersistsLeaderWindowCheckpointAcrossRestart : SimplexDbTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers the frontier checkpoint that lets Rule 1 resume after restart:
    // once a leader window is observed, Db must durably advance first_nonannounced_window and
    // restore the same checkpoint on the next startup.
    co_await start_actor();

    co_await handle_.publish<simplex::LeaderWindowObserved>(8, ParentId{});

    EXPECT_EQ(stored_first_nonannounced_window(db()), static_cast<td::uint32>(3));

    co_await restart_actor();

    EXPECT_EQ(handle_->first_nonannounced_window, static_cast<td::uint32>(3));

    co_return td::Unit{};
  }
};
REGISTER_TEST(SimplexDb, PersistsLeaderWindowCheckpointAcrossRestart);

}  // namespace
}  // namespace ton::validator::consensus::test
