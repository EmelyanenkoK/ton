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

using CandidateAndCertRef = tl_object_ptr<ton_api::consensus_simplex_candidateAndCert>;
using RequestCandidateRef = tl_object_ptr<ton_api::consensus_simplex_requestCandidate>;

class OverlayRequestResponderImpl;

struct CandidateResolverBus : simplex::Bus {
  using Parent = simplex::Bus;
  using Events = td::TypeList<>;

  OverlayRequestResponderImpl* overlay_responder = nullptr;
};

class OverlayRequestResponderImpl : public td::actor::SpawnsWith<CandidateResolverBus>,
                                    public td::actor::ConnectsTo<CandidateResolverBus> {
 public:
  TON_RUNTIME_DEFINE_EVENT_HANDLER();

  td::actor::MockAsync<ProtocolMessage, PeerValidatorId, td::Timestamp, ProtocolMessage> request;

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
    co_return co_await request.call(event->destination, event->timeout, std::move(event->request));
  }
};

class CandidateResolverTest : public td::Test {
 protected:
  std::unique_ptr<ValidatorSetup> ctx_;
  td::actor::TestScheduler ts_;
  td::actor::BusHandle<CandidateResolverBus> handle_;
  CandidateResolverBus* bus_{};
  TestDbImpl* db_{};

  virtual TestOptions options() const {
    return TestOptions{.weight_distribution = {1, 1}};
  }

  ValidatorSetup& ctx() {
    CHECK(ctx_ != nullptr);
    return *ctx_;
  }

  void run() final {
    ts_.run([this]() -> td::actor::Task<td::Unit> {
      ctx_ = std::make_unique<ValidatorSetup>(options());

      auto bus = std::make_shared<CandidateResolverBus>();
      fill_simplex_bus(ctx(), *bus, 0);
      bus->db = std::make_unique<TestDbImpl>();

      bus_ = bus.get();
      db_ = static_cast<TestDbImpl*>(bus->db.get());

      td::actor::Runtime runtime;
      runtime.register_actor<OverlayRequestResponderImpl>("OverlayRequestResponder");
      simplex::CandidateResolver::register_in(runtime);
      handle_ = runtime.start(std::move(bus));

      co_await ts_.wait_sync_work();
      co_await run_test();

      handle_.publish<StopRequested>();
      co_await ts_.wait_sync_work();

      handle_ = {};
      bus_ = nullptr;
      db_ = nullptr;
      ctx_.reset();

      co_return td::Unit{};
    });
  }

  virtual td::actor::Task<td::Unit> run_test() = 0;
};

RequestCandidateRef make_request(CandidateId id, bool want_candidate, bool want_notar) {
  return create_tl_object<ton_api::consensus_simplex_requestCandidate>(id.to_tl(), want_candidate, want_notar);
}

CandidateAndCertRef decode_candidate_and_cert(const ProtocolMessage& message) {
  return fetch_tl_object<ton_api::consensus_simplex_candidateAndCert>(message.data, true).move_as_ok();
}

CandidateAndCertRef make_candidate_and_cert_response(std::optional<CandidateRef> candidate,
                                                     std::optional<simplex::NotarCertRef> notar) {
  td::BufferSlice candidate_data;
  if (candidate.has_value()) {
    candidate_data = (*candidate)->serialize();
  }

  td::BufferSlice notar_data;
  if (notar.has_value()) {
    notar_data = serialize_tl_object((*notar)->to_tl_vote_signature_set(), true);
  }

  return create_tl_object<ton_api::consensus_simplex_candidateAndCert>(std::move(candidate_data),
                                                                       std::move(notar_data));
}

struct ServesStoredCandidateToPeers : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md Rule 2:
    // validators store candidates they know and serve the candidate body to peers that request it.
    auto candidate =
        make_serializable_empty_candidate(ctx(), *bus_, 1, make_candidate_id(0, 100), min_mc_block_id, PeerValidatorId{0});

    co_await handle_.publish<StoreCandidate>(candidate);
    co_await ts_.wait_sync_work();

    auto request = make_request(candidate->id, true, false);
    auto response =
        co_await handle_.publish<IncomingOverlayRequest>(PeerValidatorId{1}, ProtocolMessage{std::move(request)});

    auto response_tl = decode_candidate_and_cert(response);
    EXPECT(!response_tl->candidate_.empty());
    EXPECT(response_tl->notar_.empty());

    auto decoded_candidate = Candidate::deserialize(response_tl->candidate_, *bus_);
    ASSERT_TRUE(decoded_candidate.is_ok());
    EXPECT_EQ(decoded_candidate.ok()->id, candidate->id);

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, ServesStoredCandidateToPeers);

struct RequestsMissingPiecesFromPeers : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md Rule 2:
    // when local state lacks both the candidate body and the notarization certificate, resolution
    // must ask peers for both missing pieces.
    auto id = make_candidate_id(9, 9009);
    auto parent = make_candidate_id(8, 8008);
    auto resolved_candidate = td::make_ref<Candidate>(id, ParentId{parent}, PeerValidatorId{0}, min_mc_block_id,
                                                      td::BufferSlice("sig"));
    auto notar = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{id}, {0, 1});

    auto first_request = bus_->overlay_responder->request.expect();

    [&]() -> td::actor::Task<td::Unit> {
      auto _ = co_await handle_.publish<simplex::ResolveCandidate>(id).wrap();
      co_return td::Unit{};
    }()
        .start()
        .detach();

    auto request = co_await std::move(first_request);

    auto request_tl = fetch_tl_object<ton_api::consensus_simplex_requestCandidate>(std::get<2>(request.args).data, true)
                          .move_as_ok();
    EXPECT(request_tl->want_candidate_);
    EXPECT(request_tl->want_notar_);

    co_await handle_.publish<StoreCandidate>(resolved_candidate);
    handle_.publish<simplex::NotarizationObserved>(id, notar);
    request.respond.set_value(ProtocolMessage{create_tl_object<ton_api::consensus_simplex_candidateAndCert>(
        td::BufferSlice(), td::BufferSlice())});
    co_await ts_.wait_sync_work();
    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, RequestsMissingPiecesFromPeers);

struct DoesNotLeakUnrequestedCandidateBody : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md Rule 2 negative behavior:
    // when a peer requests only the notarization certificate, CandidateResolver must not leak the
    // candidate body in the response.
    auto candidate =
        make_serializable_empty_candidate(ctx(), *bus_, 1, make_candidate_id(0, 1100), min_mc_block_id, PeerValidatorId{0});
    auto notar = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{candidate->id}, {0, 1});

    co_await handle_.publish<StoreCandidate>(candidate);
    handle_.publish<simplex::NotarizationObserved>(candidate->id, notar);
    co_await ts_.wait_sync_work();

    auto response = co_await handle_.publish<IncomingOverlayRequest>(
        PeerValidatorId{1}, ProtocolMessage{make_request(candidate->id, false, true)});

    auto response_tl = decode_candidate_and_cert(response);
    EXPECT(response_tl->candidate_.empty());
    EXPECT(!response_tl->notar_.empty());

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, DoesNotLeakUnrequestedCandidateBody);

struct IgnoresWrongIdResponseAndKeepsWaiting : CandidateResolverTest {
  td::actor::Task<td::Unit> run_test() override {
    // Covers simplex_docs.md Rule 2 negative behavior:
    // if a peer responds with a candidate for the wrong id, CandidateResolver must ignore it and
    // keep requesting the missing candidate instead of resolving with mismatched data.
    auto correct_candidate = make_serializable_empty_candidate(ctx(), *bus_, 9, make_candidate_id(8, 9900),
                                                               min_mc_block_id, PeerValidatorId{0});
    auto wrong_candidate = make_serializable_empty_candidate(ctx(), *bus_, 9, make_candidate_id(8, 9911),
                                                             BlockIdExt{BlockId(ctx().shard(), 9), bits256_pattern(42),
                                                                        bits256_pattern(43)},
                                                             PeerValidatorId{0});
    auto notar = make_simplex_certificate(ctx(), *bus_, simplex::NotarizeVote{correct_candidate->id}, {0, 1});

    std::optional<td::Result<simplex::ResolveCandidate::Result>> resolve_result;
    bool completed = false;
    auto first_request = bus_->overlay_responder->request.expect();
    [&]() -> td::actor::Task<td::Unit> {
      resolve_result = co_await handle_.publish<simplex::ResolveCandidate>(correct_candidate->id).wrap();
      completed = true;
      co_return td::Unit{};
    }()
        .start()
        .detach();

    auto request1 = co_await std::move(first_request);
    auto second_request = bus_->overlay_responder->request.expect();

    auto request1_tl =
        fetch_tl_object<ton_api::consensus_simplex_requestCandidate>(std::get<2>(request1.args).data, true).move_as_ok();
    EXPECT(request1_tl->want_candidate_);
    EXPECT(request1_tl->want_notar_);

    request1.respond.set_value(ProtocolMessage{make_candidate_and_cert_response(wrong_candidate, std::nullopt)});
    co_await ts_.wait_sync_work();
    EXPECT(!completed);

    auto request2 = co_await std::move(second_request);
    auto request2_tl =
        fetch_tl_object<ton_api::consensus_simplex_requestCandidate>(std::get<2>(request2.args).data, true).move_as_ok();
    EXPECT(request2_tl->want_candidate_);
    EXPECT(request2_tl->want_notar_);

    request2.respond.set_value(ProtocolMessage{make_candidate_and_cert_response(correct_candidate, notar)});
    for (int i = 0; i < 10 && !completed; ++i) {
      co_await ts_.wait_sync_work();
    }

    ASSERT_TRUE(completed);
    ASSERT_TRUE(resolve_result.has_value());
    ASSERT_TRUE(resolve_result->is_ok());
    EXPECT_EQ(resolve_result->ok().candidate->id, correct_candidate->id);
    EXPECT_EQ(resolve_result->ok().notar->vote.id, correct_candidate->id);

    co_return {};
  }
};
REGISTER_TEST(CandidateResolver, IgnoresWrongIdResponseAndKeepsWaiting);

}  // namespace
}  // namespace ton::validator::consensus::test
