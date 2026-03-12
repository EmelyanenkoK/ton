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

}  // namespace
}  // namespace ton::validator::consensus::test
