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

using simplex::StoreCandidate;

// =============================================================================
// Test infrastructure
// =============================================================================

struct RecordedRequest {
  PeerValidatorId peer;
  double timeout_in;
  bool want_candidate;
  bool want_notar;
};

class TestOverlayResponder;

struct CandidateResolverBus : simplex::Bus {
  using Parent = simplex::Bus;
  using Events = td::TypeList<>;

  TestOverlayResponder* overlay_responder = nullptr;
};

class TestOverlayResponder : public td::actor::SpawnsWith<CandidateResolverBus>,
                             public td::actor::ConnectsTo<CandidateResolverBus> {
 public:
  TON_RUNTIME_DEFINE_EVENT_HANDLER();

  std::deque<ProtocolMessage> responses;
  std::vector<RecordedRequest> requests;
  td::Timestamp respond_after;

  void start_up() override {
    const_cast<CandidateResolverBus&>(*owning_bus()).overlay_responder = this;
  }

  template <>
  void handle(td::actor::BusHandle<CandidateResolverBus>, std::shared_ptr<const StopRequested>) {
    stop();
  }

  template <>
  td::actor::Task<ProtocolMessage> process(td::actor::BusHandle<CandidateResolverBus>,
                                           std::shared_ptr<OutgoingOverlayRequest> event) {
    auto request_tl =
        fetch_tl_object<ton_api::consensus_simplex_requestCandidate>(event->request.data, true).move_as_ok();
    requests.push_back(RecordedRequest{
        .peer = event->destination,
        .timeout_in = event->timeout.in(),
        .want_candidate = request_tl->want_candidate_,
        .want_notar = request_tl->want_notar_,
    });
    LOG_CHECK(!responses.empty()) << "No pre-loaded response for request #" << requests.size();
    if (respond_after) {
      co_await td::actor::coro_sleep(respond_after);
      respond_after = {};
    }
    auto response = std::move(responses.front());
    responses.pop_front();
    co_return response;
  }
};

ProtocolMessage make_response(std::optional<CandidateRef> candidate = {},
                              std::optional<simplex::NotarCertRef> notar = {}) {
  td::BufferSlice candidate_data;
  if (candidate.has_value()) {
    candidate_data = (*candidate)->serialize();
  }
  td::BufferSlice notar_data;
  if (notar.has_value()) {
    notar_data = serialize_tl_object((*notar)->to_tl_vote_signature_set(), true);
  }
  return ProtocolMessage{
      create_tl_object<ton_api::consensus_simplex_candidateAndCert>(std::move(candidate_data), std::move(notar_data))};
}

ProtocolMessage empty_response() {
  return make_response();
}

class CandidateResolverTest : public td::Test {
 protected:
  std::unique_ptr<ValidatorSetup> ctx_;
  td::actor::TestScheduler ts_;
  td::actor::BusHandle<CandidateResolverBus> handle_;
  CandidateResolverBus* bus_{};
  TestOverlayResponder* responder_{};
  NewConsensusConfig::NoncriticalParams noncritical_params{};

  virtual TestOptions options() const {
    return TestOptions{.weight_distribution = {1, 1}};
  }

  ValidatorSetup& ctx() {
    return *ctx_;
  }

  void enqueue_response(ProtocolMessage response) {
    responder_->responses.push_back(std::move(response));
  }

  const std::vector<RecordedRequest>& requests() const {
    return responder_->requests;
  }

  void run() final {
    ts_.run([this]() -> td::actor::Task<td::Unit> {
      ctx_ = std::make_unique<ValidatorSetup>(options());

      auto bus = std::make_shared<CandidateResolverBus>();
      fill_simplex_bus(ctx(), *bus, 0);
      bus->db = std::make_unique<TestDbImpl>();
      bus_ = bus.get();

      td::actor::Runtime runtime;
      runtime.register_actor<TestOverlayResponder>("OverlayResponder");
      simplex::CandidateResolver::register_in(runtime);
      handle_ = runtime.start(std::move(bus));
      co_await ts_.wait_sync_work();

      responder_ = bus_->overlay_responder;
      CHECK(responder_ != nullptr);

      co_await run_test();

      handle_.publish<StopRequested>();
      co_await ts_.wait_sync_work();

      handle_ = {};
      bus_ = nullptr;
      responder_ = nullptr;
      ctx_.reset();

      co_return td::Unit{};
    });
  }

  void update_noncritical_params(NewConsensusConfig::NoncriticalParams params) {
    noncritical_params = std::move(params);
    handle_.publish<NoncriticalParamsUpdated>(noncritical_params);
  }

  td::actor::Task<> skip_cooldown() {
    std::chrono::duration<double> cooldown_wait = noncritical_params.candidate_resolve_cooldown;
    EXPECT_APPROX(ts_.next_timeout_in(), cooldown_wait.count());
    ts_.advance_time(ts_.next_timeout_in());
    co_await ts_.wait_sync_work();
    co_return {};
  }

  virtual td::actor::Task<td::Unit> run_test() = 0;
};

// =============================================================================
// Tests: serving stored data to peers (incoming requests)
// =============================================================================

struct ServesStoredCandidateToPeers : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    auto candidate = make_serializable_empty_candidate(ctx(), *bus_, 1, make_candidate_id(0, 100), min_mc_block_id,
                                                       PeerValidatorId{0});
    co_await handle_.publish<StoreCandidate>(candidate);
    co_await ts_.wait_sync_work();

    auto request_tl = create_tl_object<ton_api::consensus_simplex_requestCandidate>(candidate->id.to_tl(), true, false);
    auto response =
        co_await handle_.publish<IncomingOverlayRequest>(PeerValidatorId{1}, ProtocolMessage{std::move(request_tl)});

    auto response_tl = fetch_tl_object<ton_api::consensus_simplex_candidateAndCert>(response.data, true).move_as_ok();
    EXPECT(!response_tl->candidate_.empty());
    EXPECT(response_tl->notar_.empty());

    auto decoded = Candidate::deserialize(response_tl->candidate_, *bus_);
    ASSERT_TRUE(decoded.is_ok());
    EXPECT_EQ(decoded.ok()->id, candidate->id);

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, ServesStoredCandidateToPeers);

struct DoesNotLeakUnrequestedCandidateBody : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    auto candidate = make_serializable_empty_candidate(ctx(), *bus_, 1, make_candidate_id(0, 1100), min_mc_block_id,
                                                       PeerValidatorId{0});
    auto notar = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{candidate->id}, {0, 1});

    co_await handle_.publish<StoreCandidate>(candidate);
    handle_.publish<simplex::NotarizationObserved>(candidate->id, notar);
    co_await ts_.wait_sync_work();

    auto request_tl = create_tl_object<ton_api::consensus_simplex_requestCandidate>(candidate->id.to_tl(), false, true);
    auto response =
        co_await handle_.publish<IncomingOverlayRequest>(PeerValidatorId{1}, ProtocolMessage{std::move(request_tl)});

    auto response_tl = fetch_tl_object<ton_api::consensus_simplex_candidateAndCert>(response.data, true).move_as_ok();
    EXPECT(response_tl->candidate_.empty());
    EXPECT(!response_tl->notar_.empty());

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, DoesNotLeakUnrequestedCandidateBody);

// Send repeated incoming requestCandidate RPCs to prove per-peer limiting,
// window expiry, cross-peer independence, and immediate config refresh.
struct EnforcesIngressRateLimitAndRefreshesOnConfigChange : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    auto params = bus_->config.noncritical_params;
    params.candidate_resolve_rate_limit = 1;
    update_noncritical_params(params);
    co_await ts_.wait_sync_work();

    auto request_id = make_candidate_id(0, 1200);
    auto send_request = [&](PeerValidatorId peer) {
      return handle_.publish<IncomingOverlayRequest>(
                     peer,
                     ProtocolMessage{create_tl_object<ton_api::consensus_simplex_requestCandidate>(
                         request_id.to_tl(), true, true)})
          .wrap();
    };

    auto expect_empty_response = [&](const ProtocolMessage& response) {
      auto response_tl =
          fetch_tl_object<ton_api::consensus_simplex_candidateAndCert>(response.data, true).move_as_ok();
      EXPECT(response_tl->candidate_.empty());
      EXPECT(response_tl->notar_.empty());
    };

    auto first = co_await send_request(PeerValidatorId{1});
    ASSERT_TRUE(first.is_ok());
    expect_empty_response(first.ok());

    auto second = co_await send_request(PeerValidatorId{1});
    ASSERT_TRUE(second.is_error());
    EXPECT_EQ(second.error().message().str(), "too many requests");

    auto other_peer = co_await send_request(PeerValidatorId{2});
    ASSERT_TRUE(other_peer.is_ok());
    expect_empty_response(other_peer.ok());

    ts_.advance_time(1.1);
    co_await ts_.wait_sync_work();

    auto after_window = co_await send_request(PeerValidatorId{1});
    ASSERT_TRUE(after_window.is_ok());
    expect_empty_response(after_window.ok());

    params.candidate_resolve_rate_limit = 2;
    update_noncritical_params(params);
    co_await ts_.wait_sync_work();

    auto post_update_1 = co_await send_request(PeerValidatorId{1});
    ASSERT_TRUE(post_update_1.is_ok());
    expect_empty_response(post_update_1.ok());

    auto post_update_2 = co_await send_request(PeerValidatorId{1});
    ASSERT_TRUE(post_update_2.is_ok());
    expect_empty_response(post_update_2.ok());

    auto post_update_3 = co_await send_request(PeerValidatorId{1});
    ASSERT_TRUE(post_update_3.is_error());
    EXPECT_EQ(post_update_3.error().message().str(), "too many requests");

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, EnforcesIngressRateLimitAndRefreshesOnConfigChange);

// Set the ingress limit to zero and issue one request to lock in the reject-all
// boundary behavior.
struct RejectsAllIngressRequestsWhenRateLimitIsZero : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    auto params = bus_->config.noncritical_params;
    params.candidate_resolve_rate_limit = 0;
    update_noncritical_params(params);
    co_await ts_.wait_sync_work();

    auto request_id = make_candidate_id(0, 1201);
    auto request = create_tl_object<ton_api::consensus_simplex_requestCandidate>(request_id.to_tl(), true, true);
    auto result = co_await handle_.publish<IncomingOverlayRequest>(PeerValidatorId{1}, ProtocolMessage{std::move(request)}).wrap();

    ASSERT_TRUE(result.is_error());
    EXPECT_EQ(result.error().message().str(), "too many requests");

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, RejectsAllIngressRequestsWhenRateLimitIsZero);

// =============================================================================
// Tests: resolving candidates from peers (outgoing requests)
// =============================================================================

struct ResolvesFromPeerResponse : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    auto candidate = make_serializable_empty_candidate(ctx(), *bus_, 9, make_candidate_id(8, 9009), min_mc_block_id,
                                                       PeerValidatorId{0});
    auto notar = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{candidate->id}, {0, 1});

    enqueue_response(make_response(candidate, notar));
    auto result = co_await handle_.publish<simplex::ResolveCandidate>(candidate->id);

    EXPECT_EQ(result.candidate->id, candidate->id);
    EXPECT_EQ(result.notar->vote.id, candidate->id);
    ASSERT_EQ(requests().size(), static_cast<size_t>(1));
    EXPECT(requests()[0].want_candidate);
    EXPECT(requests()[0].want_notar);

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, ResolvesFromPeerResponse);

struct ResolvesFromLocalEventsWhileWaiting : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    auto candidate = make_serializable_empty_candidate(ctx(), *bus_, 9, make_candidate_id(8, 9400), min_mc_block_id,
                                                       PeerValidatorId{0});
    auto notar = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{candidate->id}, {0, 1});

    enqueue_response(empty_response());
    auto resolve_task = handle_.publish<simplex::ResolveCandidate>(candidate->id).start();
    co_await ts_.wait_sync_work();
    EXPECT(!resolve_task.await_ready());

    co_await handle_.publish<StoreCandidate>(candidate);
    handle_.publish<simplex::NotarizationObserved>(candidate->id, notar);
    co_await skip_cooldown();

    auto result = co_await std::move(resolve_task);
    EXPECT_EQ(result.candidate->id, candidate->id);
    EXPECT_EQ(result.notar->vote.id, candidate->id);

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, ResolvesFromLocalEventsWhileWaiting);

struct IgnoresWrongIdResponse : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    auto correct = make_serializable_empty_candidate(ctx(), *bus_, 9, make_candidate_id(8, 9900), min_mc_block_id,
                                                     PeerValidatorId{0});
    auto wrong = make_serializable_empty_candidate(
        ctx(), *bus_, 9, make_candidate_id(8, 9911),
        BlockIdExt{BlockId(ctx().shard(), 9), bits256_pattern(42), bits256_pattern(43)}, PeerValidatorId{0});
    auto notar = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{correct->id}, {0, 1});

    enqueue_response(make_response(wrong, std::nullopt));
    enqueue_response(make_response(correct, notar));

    auto resolve_task = handle_.publish<simplex::ResolveCandidate>(correct->id).start();
    co_await ts_.wait_sync_work();
    EXPECT(!resolve_task.await_ready());

    co_await skip_cooldown();

    auto result = co_await std::move(resolve_task);
    EXPECT_EQ(result.candidate->id, correct->id);
    ASSERT_EQ(requests().size(), static_cast<size_t>(2));

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, IgnoresWrongIdResponse);

struct IgnoresBadLeaderCandidate : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    auto candidate = make_serializable_empty_candidate(ctx(), *bus_, 9, make_candidate_id(8, 9300), min_mc_block_id,
                                                       PeerValidatorId{0});
    auto wrong_leader = make_serializable_empty_candidate(ctx(), *bus_, 9, make_candidate_id(8, 9300), min_mc_block_id,
                                                          PeerValidatorId{1});
    auto notar = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{candidate->id}, {0, 1});

    enqueue_response(make_response(wrong_leader, std::nullopt));
    enqueue_response(empty_response());

    auto resolve_task = handle_.publish<simplex::ResolveCandidate>(candidate->id).start();
    co_await ts_.wait_sync_work();
    EXPECT(!resolve_task.await_ready());

    co_await skip_cooldown();
    EXPECT(!resolve_task.await_ready());
    EXPECT_EQ(requests().size(), static_cast<size_t>(2));

    co_await handle_.publish<StoreCandidate>(candidate);
    handle_.publish<simplex::NotarizationObserved>(candidate->id, notar);

    auto result = co_await std::move(resolve_task);
    EXPECT_EQ(result.candidate->id, candidate->id);

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, IgnoresBadLeaderCandidate);

struct RetriesWithBackoff : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    auto candidate = make_serializable_empty_candidate(ctx(), *bus_, 9, make_candidate_id(8, 9200), min_mc_block_id,
                                                       PeerValidatorId{0});
    auto notar = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{candidate->id}, {0, 1});

    enqueue_response(empty_response());
    enqueue_response(make_response(candidate, notar));

    auto resolve_task = handle_.publish<simplex::ResolveCandidate>(candidate->id).start();
    co_await ts_.wait_sync_work();
    EXPECT(!resolve_task.await_ready());
    ASSERT_EQ(requests().size(), static_cast<size_t>(1));

    auto& params = bus_->config.noncritical_params;
    std::chrono::duration<double> expected_first = params.candidate_resolve_timeout;
    EXPECT_APPROX(requests()[0].timeout_in, expected_first.count());

    co_await skip_cooldown();

    auto result = co_await std::move(resolve_task);
    EXPECT_EQ(result.candidate->id, candidate->id);
    ASSERT_EQ(requests().size(), static_cast<size_t>(2));

    auto expected_second = expected_first * params.candidate_resolve_timeout_multiplier;
    EXPECT_APPROX(requests()[1].timeout_in, expected_second.count());

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, RetriesWithBackoff);

struct QueriesDifferentPeers : CandidateResolverTest {
  TestOptions options() const override {
    return TestOptions{.weight_distribution = {1, 1, 1, 1}};
  }

  td::actor::Task<td::Unit> run_test() override {
    auto candidate = make_serializable_empty_candidate(ctx(), *bus_, 9, make_candidate_id(8, 9500), min_mc_block_id,
                                                       PeerValidatorId{2});
    auto notar = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{candidate->id}, {0, 1, 2});

    for (int i = 0; i < 23; ++i) {
      enqueue_response(empty_response());
    }
    enqueue_response(make_response(candidate, notar));

    auto resolve_task = handle_.publish<simplex::ResolveCandidate>(candidate->id).start();
    co_await ts_.wait_sync_work();
    for (int i = 0; i < 23; ++i) {
      co_await skip_cooldown();
    }

    auto result = co_await std::move(resolve_task);
    EXPECT_EQ(result.candidate->id, candidate->id);

    std::set<size_t> seen_peers;
    for (const auto& req : requests()) {
      EXPECT(req.peer != bus_->local_id.idx);
      seen_peers.insert(req.peer.value());
    }
    EXPECT(seen_peers.size() >= 2);

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, QueriesDifferentPeers);

struct SkipsCooldownWhenRequestTakesLonger : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    auto candidate = make_serializable_empty_candidate(ctx(), *bus_, 9, make_candidate_id(8, 7700), min_mc_block_id,
                                                       PeerValidatorId{0});
    auto notar = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{candidate->id}, {0, 1});

    auto cooldown = bus_->config.noncritical_params.candidate_resolve_cooldown;
    auto respond_after = td::Timestamp::in(cooldown * 2);
    responder_->respond_after = respond_after;
    enqueue_response(empty_response());
    enqueue_response(make_response(candidate, notar));

    auto resolve_task = handle_.publish<simplex::ResolveCandidate>(candidate->id).start();
    co_await ts_.wait_sync_work();

    auto delay = ts_.next_timeout_in();
    EXPECT_APPROX(delay, respond_after.in());
    ts_.advance_time(delay);
    co_await ts_.wait_sync_work();

    auto result = co_await std::move(resolve_task);
    EXPECT_EQ(result.candidate->id, candidate->id);
    EXPECT_EQ(requests().size(), static_cast<size_t>(2));

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, SkipsCooldownWhenRequestTakesLonger);

struct TimeoutsRespectAllNoncriticalParams : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    using namespace std::chrono_literals;

    auto check_timeouts = [&](NewConsensusConfig::NoncriticalParams& params, td::uint64 pattern) -> td::actor::Task<> {
      responder_->requests.clear();
      auto candidate = make_serializable_empty_candidate(ctx(), *bus_, 9, make_candidate_id(8, pattern),
                                                         min_mc_block_id, PeerValidatorId{0});
      auto notar = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{candidate->id}, {0, 1});

      for (int i = 0; i < 3; ++i) {
        enqueue_response(empty_response());
      }
      enqueue_response(make_response(candidate, notar));

      auto resolve_task = handle_.publish<simplex::ResolveCandidate>(candidate->id).start();
      co_await ts_.wait_sync_work();
      for (int i = 0; i < 3; ++i) {
        co_await skip_cooldown();
      }
      co_await std::move(resolve_task);
      EXPECT_EQ(requests().size(), static_cast<size_t>(4));

      std::chrono::duration<double> expected = params.candidate_resolve_timeout;
      for (size_t i = 0; i < 3; ++i) {
        EXPECT_APPROX(requests()[i].timeout_in, expected.count());
        expected = std::min<std::chrono::duration<double>>(expected * params.candidate_resolve_timeout_multiplier,
                                                           params.candidate_resolve_timeout_cap);
      }
      co_return {};
    };

    auto params = bus_->config.noncritical_params;
    co_await check_timeouts(params, 7801);

    auto new_params = params;
    new_params.candidate_resolve_timeout = 500ms;
    new_params.candidate_resolve_timeout_multiplier = 3.0;
    new_params.candidate_resolve_timeout_cap = 1200ms;
    new_params.candidate_resolve_cooldown = 120ms;
    update_noncritical_params(new_params);
    co_await ts_.wait_sync_work();

    co_await check_timeouts(new_params, 7802);

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, TimeoutsRespectAllNoncriticalParams);

struct ResolvesWhileQueryInFlight : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    auto candidate = make_serializable_empty_candidate(ctx(), *bus_, 9, make_candidate_id(8, 7900), min_mc_block_id,
                                                       PeerValidatorId{0});
    auto notar = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{candidate->id}, {0, 1});

    responder_->respond_after = td::Timestamp::in(1.0);
    enqueue_response(empty_response());

    auto resolve_task = handle_.publish<simplex::ResolveCandidate>(candidate->id).start();
    co_await ts_.wait_sync_work();
    EXPECT(!resolve_task.await_ready());

    co_await handle_.publish<StoreCandidate>(candidate);
    handle_.publish<simplex::NotarizationObserved>(candidate->id, notar);

    auto result = co_await std::move(resolve_task);
    EXPECT_EQ(result.candidate->id, candidate->id);
    EXPECT_EQ(result.notar->vote.id, candidate->id);

    ts_.advance_time_to(responder_->respond_after);
    co_await ts_.wait_sync_work();

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, ResolvesWhileQueryInFlight);

}  // namespace
}  // namespace ton::validator::consensus::test
