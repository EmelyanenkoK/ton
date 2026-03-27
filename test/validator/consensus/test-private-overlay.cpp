/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "adnl/adnl-sender-ex.h"
#include "overlay/overlays.h"
#include "td/actor/TestScheduler.h"
#include "td/utils/tests.h"
#include "validator/validator.h"

#include "test-helpers.h"

namespace ton::validator::consensus::test {
namespace {

using overlay::OverlayIdFull;
using overlay::OverlayIdShort;
using overlay::Certificate;
using overlay::OverlayMemberCertificate;
using overlay::OverlayOptions;
using overlay::OverlayPrivacyRules;

// Slot 5 is owned by validator #1; all other slots are owned by validator #0.
class SelectiveCollatorSchedule final : public CollatorSchedule {
 public:
  PeerValidatorId expected_collator_for(td::uint32 slot) const override {
    if (slot == 5) {
      return PeerValidatorId{1};
    }
    return PeerValidatorId{0};
  }
};

class PrivateOverlayBusObserver;

struct PrivateOverlayBus : consensus::Bus {
  using Parent = consensus::Bus;
  using Events = td::TypeList<>;

  PrivateOverlayBusObserver* observer = nullptr;

  void populate_collator_schedule() override {
    collator_schedule = td::make_ref<SelectiveCollatorSchedule>();
  }
};

class PrivateOverlayBusObserver final : public td::actor::SpawnsWith<PrivateOverlayBus>,
                                        public td::actor::ConnectsTo<PrivateOverlayBus> {
 public:
  using BusHandle = td::actor::BusHandle<PrivateOverlayBus>;

  TON_RUNTIME_DEFINE_EVENT_HANDLER();

  std::vector<std::shared_ptr<const CandidateGenerated>> candidate_generated;
  std::vector<std::shared_ptr<const CandidateReceived>> candidate_received;
  std::vector<td::uint32> precheck_slots;

  void start_up() override {
    const_cast<PrivateOverlayBus&>(*owning_bus()).observer = this;
  }

  void tear_down() override {
    const_cast<PrivateOverlayBus&>(*owning_bus()).observer = nullptr;
  }

  template <>
  void handle(BusHandle, std::shared_ptr<const StopRequested>) {
    stop();
  }

  template <>
  void handle(BusHandle, std::shared_ptr<const CandidateGenerated> event) {
    candidate_generated.push_back(std::move(event));
  }

  template <>
  void handle(BusHandle, std::shared_ptr<const CandidateReceived> event) {
    candidate_received.push_back(std::move(event));
  }

  template <>
  td::actor::Task<> process(BusHandle, std::shared_ptr<PrecheckCandidateBroadcast> event) {
    precheck_slots.push_back(event->slot);
    co_return td::Unit{};
  }
};

class MockAdnlSender final : public adnl::AdnlSenderEx {
 public:
  std::vector<adnl::AdnlNodeIdShort> added_ids;

  void add_id(adnl::AdnlNodeIdShort local_id) override {
    added_ids.push_back(local_id);
  }

  void send_message(adnl::AdnlNodeIdShort, adnl::AdnlNodeIdShort, td::BufferSlice) override {
  }

  void send_query(adnl::AdnlNodeIdShort, adnl::AdnlNodeIdShort, std::string, td::Promise<td::BufferSlice> promise,
                  td::Timestamp, td::BufferSlice) override {
    promise.set_error(td::Status::Error("not mocked"));
  }

  void send_query_ex(adnl::AdnlNodeIdShort, adnl::AdnlNodeIdShort, std::string, td::Promise<td::BufferSlice> promise,
                     td::Timestamp, td::BufferSlice, td::uint64) override {
    promise.set_error(td::Status::Error("not mocked"));
  }

  void get_conn_ip_str(adnl::AdnlNodeIdShort, adnl::AdnlNodeIdShort, td::Promise<td::string> promise) override {
    promise.set_error(td::Status::Error("not mocked"));
  }

 protected:
  void on_mtu_updated(td::optional<adnl::AdnlNodeIdShort>, td::optional<adnl::AdnlNodeIdShort>) override {
  }
};

class MockOverlays final : public overlay::Overlays {
 public:
  struct BroadcastCall {
    adnl::AdnlNodeIdShort src;
    overlay::OverlayIdShort overlay_id;
    PublicKeyHash send_as;
    td::uint32 flags;
    td::BufferSlice object;
    td::BufferSlice extra;
  };

  std::vector<BroadcastCall> broadcasts;

  void update_dht_node(td::actor::ActorId<dht::Dht>) override {
  }

  void create_public_overlay(adnl::AdnlNodeIdShort, OverlayIdFull, std::unique_ptr<Callback>, OverlayPrivacyRules,
                             td::string) override {
  }

  void create_public_overlay_ex(adnl::AdnlNodeIdShort, OverlayIdFull, std::unique_ptr<Callback>, OverlayPrivacyRules,
                                td::string, OverlayOptions) override {
  }

  void create_semiprivate_overlay(adnl::AdnlNodeIdShort, OverlayIdFull, std::vector<adnl::AdnlNodeIdShort>,
                                  std::vector<PublicKeyHash>, OverlayMemberCertificate, std::unique_ptr<Callback>,
                                  OverlayPrivacyRules, td::string, OverlayOptions) override {
  }

  void create_private_overlay(adnl::AdnlNodeIdShort, OverlayIdFull, std::vector<adnl::AdnlNodeIdShort>,
                              std::unique_ptr<Callback>, OverlayPrivacyRules, std::string) override {
  }

  void create_private_overlay_ex(adnl::AdnlNodeIdShort local_id, OverlayIdFull overlay_id,
                                 std::vector<adnl::AdnlNodeIdShort>, std::unique_ptr<Callback> callback,
                                 OverlayPrivacyRules, std::string, OverlayOptions) override {
    CHECK(!callback_);
    local_id_ = local_id;
    overlay_id_ = overlay_id.compute_short_id();
    callback_ = std::move(callback);
  }

  void delete_overlay(adnl::AdnlNodeIdShort, OverlayIdShort) override {
  }

  void send_query(adnl::AdnlNodeIdShort, adnl::AdnlNodeIdShort, OverlayIdShort, std::string,
                  td::Promise<td::BufferSlice> promise, td::Timestamp, td::BufferSlice) override {
    promise.set_error(td::Status::Error("not mocked"));
  }

  void send_query_via(adnl::AdnlNodeIdShort, adnl::AdnlNodeIdShort, OverlayIdShort, std::string,
                      td::Promise<td::BufferSlice> promise, td::Timestamp, td::BufferSlice, td::uint64,
                      td::actor::ActorId<adnl::AdnlSenderInterface>) override {
    promise.set_error(td::Status::Error("not mocked"));
  }

  void send_message(adnl::AdnlNodeIdShort, adnl::AdnlNodeIdShort, OverlayIdShort, td::BufferSlice) override {
  }

  void send_message_via(adnl::AdnlNodeIdShort, adnl::AdnlNodeIdShort, OverlayIdShort, td::BufferSlice,
                         td::actor::ActorId<adnl::AdnlSenderInterface>) override {
  }

  void send_broadcast(adnl::AdnlNodeIdShort, OverlayIdShort, td::BufferSlice) override {
  }

  void send_broadcast_ex(adnl::AdnlNodeIdShort, OverlayIdShort, PublicKeyHash, td::uint32, td::BufferSlice) override {
  }

  void send_broadcast_fec(adnl::AdnlNodeIdShort, OverlayIdShort, td::BufferSlice) override {
  }

  void send_broadcast_fec_ex(adnl::AdnlNodeIdShort, OverlayIdShort, PublicKeyHash, td::uint32, td::BufferSlice) override {
  }

  void send_broadcast_fec_with_extra(adnl::AdnlNodeIdShort src, OverlayIdShort overlay_id, PublicKeyHash send_as,
                                     td::uint32 flags, td::BufferSlice object, td::BufferSlice extra) override {
    broadcasts.push_back(BroadcastCall{
        .src = src,
        .overlay_id = overlay_id,
        .send_as = send_as,
        .flags = flags,
        .object = std::move(object),
        .extra = std::move(extra),
    });
  }

  void set_privacy_rules(adnl::AdnlNodeIdShort, OverlayIdShort, OverlayPrivacyRules) override {
  }

  void update_certificate(adnl::AdnlNodeIdShort, OverlayIdShort, PublicKeyHash,
                          std::shared_ptr<Certificate>) override {
  }

  void update_member_certificate(adnl::AdnlNodeIdShort, OverlayIdShort, OverlayMemberCertificate) override {
  }

  void update_root_member_list(adnl::AdnlNodeIdShort, OverlayIdShort, std::vector<adnl::AdnlNodeIdShort>,
                               std::vector<PublicKeyHash>, OverlayMemberCertificate) override {
  }

  void get_overlay_random_peers(adnl::AdnlNodeIdShort, OverlayIdShort, td::uint32,
                                td::Promise<std::vector<adnl::AdnlNodeIdShort>> promise) override {
    promise.set_error(td::Status::Error("not mocked"));
  }

  void get_stats(td::Promise<tl_object_ptr<ton_api::engine_validator_overlaysStats>> promise) override {
    promise.set_error(td::Status::Error("not mocked"));
  }

  void forget_peer(adnl::AdnlNodeIdShort, OverlayIdShort, adnl::AdnlNodeIdShort) override {
  }

  td::actor::Task<> deliver_twostep_broadcast(PublicKeyHash src, td::BufferSlice data, td::BufferSlice extra) {
    CHECK(callback_ != nullptr);

    auto [precheck_task, precheck_promise] = td::actor::StartedTask<td::Unit>::make_bridge();
    td::Bits256 broadcast_id = td::sha256_bits256(data.as_slice());
    callback_->precheck_broadcast(src, overlay_id_, broadcast_id, extra.clone(), std::move(precheck_promise));

    auto precheck_result = co_await std::move(precheck_task).wrap();
    if (precheck_result.is_error()) {
      co_return precheck_result.move_as_error();
    }

    callback_->receive_broadcast_with_extra(src, overlay_id_, std::move(data), std::move(extra));
    co_return td::Unit{};
  }

 private:
  adnl::AdnlNodeIdShort local_id_;
  overlay::OverlayIdShort overlay_id_;
  std::unique_ptr<Callback> callback_;
};

CandidateRef make_private_overlay_candidate(const ValidatorSetup& ctx, const consensus::Bus& bus, td::uint32 slot,
                                            td::uint32 parent_slot, PeerValidatorId leader) {
  return make_serializable_empty_candidate(ctx, bus, slot, make_candidate_id(parent_slot, 1000 + slot),
                                           min_mc_block_id, leader);
}

class PrivateOverlayTest : public td::Test {
 protected:
  std::unique_ptr<ValidatorSetup> ctx_;
  td::actor::TestScheduler ts_;
  td::actor::BusHandle<PrivateOverlayBus> handle_;
  td::actor::ActorOwn<MockOverlays> overlays_;
  td::actor::ActorOwn<MockAdnlSender> adnl_sender_;
  PrivateOverlayBusObserver* observer_{};
  MockOverlays* overlays_mock_{};
  MockAdnlSender* adnl_sender_mock_{};

  virtual TestOptions options() const {
    return TestOptions{.weight_distribution = {1, 1}};
  }

  ValidatorSetup& ctx() {
    CHECK(ctx_ != nullptr);
    return *ctx_;
  }

  const PeerValidator& peer(size_t idx) const {
    return handle_->validator_set[idx];
  }

  void run() final {
    ts_.run([this]() -> td::actor::Task<td::Unit> {
      ctx_ = std::make_unique<ValidatorSetup>(options());

      overlays_ = td::actor::create_actor<MockOverlays>("mock_overlays");
      overlays_mock_ = &overlays_.get_actor_unsafe();
      adnl_sender_ = td::actor::create_actor<MockAdnlSender>("mock_adnl_sender");
      adnl_sender_mock_ = &adnl_sender_.get_actor_unsafe();

      auto bus = std::make_shared<PrivateOverlayBus>();
      ctx().fill(*bus, 1);
      bus->validator_opts = ValidatorManagerOptions::create(BlockIdExt{}, BlockIdExt{});
      bus->populate_collator_schedule();
      bus->overlays = overlays_.get();
      bus->adnl_sender = adnl_sender_.get();

      td::actor::Runtime runtime;
      runtime.register_actor<PrivateOverlayBusObserver>("PrivateOverlayBusObserver");
      PrivateOverlay::register_in(runtime);
      handle_ = runtime.start(std::move(bus));

      co_await ts_.wait_sync_work();
      observer_ = handle_->observer;
      CHECK(observer_ != nullptr);
      CHECK(adnl_sender_mock_->added_ids.size() == static_cast<size_t>(1));

      co_await run_test();

      handle_.publish<StopRequested>();
      co_await ts_.wait_sync_work();

      handle_ = {};
      runtime = {};
      adnl_sender_ = {};
      overlays_ = {};
      co_await ts_.wait_sync_work();

      observer_ = nullptr;
      adnl_sender_mock_ = nullptr;
      overlays_mock_ = nullptr;
      ctx_.reset();

      co_return td::Unit{};
    });
  }

  virtual td::actor::Task<td::Unit> run_test() = 0;
};

// =============================================================================
// Tests
// =============================================================================

// Publish a locally generated candidate and decode the outgoing overlay extra to
// prove the slot is carried in consensus_broadcastExtra.
struct BroadcastExtraCarriesCandidateSlot : PrivateOverlayTest {
  td::actor::Task<td::Unit> run_test() override {
    auto candidate = make_private_overlay_candidate(ctx(), *handle_, 6, 5, PeerValidatorId{0});

    handle_.publish<CandidateGenerated>(candidate, std::nullopt);
    co_await ts_.wait_sync_work();

    ASSERT_EQ(observer_->candidate_generated.size(), static_cast<size_t>(1));
    ASSERT_EQ(overlays_mock_->broadcasts.size(), static_cast<size_t>(1));

    const auto& generated = observer_->candidate_generated[0];
    const auto& broadcast = overlays_mock_->broadcasts[0];
    auto parsed_extra = fetch_tl_object<ton_api::consensus_broadcastExtra>(broadcast.extra, true).move_as_ok();

    EXPECT_EQ(generated->candidate->id.slot, candidate->id.slot);
    EXPECT_EQ(broadcast.src, handle_->local_id.adnl_id);
    EXPECT_EQ(broadcast.send_as, handle_->local_id.short_id);
    EXPECT_EQ(parsed_extra->slot_, candidate->id.slot);

    co_return td::Unit{};
  }
};
REGISTER_TEST(PrivateOverlay, BroadcastExtraCarriesCandidateSlot);

// Feed a malformed overlay extra into the two-step receive path and verify the
// candidate is rejected before precheck or CandidateReceived.
struct MalformedExtraRejectedBeforeCandidateReceived : PrivateOverlayTest {
  td::actor::Task<td::Unit> run_test() override {
    auto candidate = make_private_overlay_candidate(ctx(), *handle_, 6, 5, PeerValidatorId{0});
    td::BufferSlice malformed_extra("definitely not a broadcast extra");

    auto result = co_await td::actor::ask(overlays_.get(), &MockOverlays::deliver_twostep_broadcast,
                                          peer(0).short_id, candidate->serialize(),
                                          malformed_extra.clone())
                            .wrap();
    ASSERT_TRUE(result.is_error());
    EXPECT_EQ(observer_->precheck_slots.size(), static_cast<size_t>(0));
    EXPECT_EQ(observer_->candidate_received.size(), static_cast<size_t>(0));

    co_return td::Unit{};
  }
};
REGISTER_TEST(PrivateOverlay, MalformedExtraRejectedBeforeCandidateReceived);

// Deliver a candidate whose serialized slot disagrees with the extra slot and
// verify precheck runs first but CandidateReceived is still suppressed.
struct SlotMismatchInExtraRejectedAfterPrecheck : PrivateOverlayTest {
  td::actor::Task<td::Unit> run_test() override {
    auto candidate = make_private_overlay_candidate(ctx(), *handle_, 6, 5, PeerValidatorId{0});
    auto extra = create_serialize_tl_object<ton_api::consensus_broadcastExtra>(7);

    auto result = co_await td::actor::ask(overlays_.get(), &MockOverlays::deliver_twostep_broadcast,
                                          peer(0).short_id, candidate->serialize(), std::move(extra))
                            .wrap();
    ASSERT_TRUE(result.is_ok());

    EXPECT_EQ(observer_->precheck_slots, std::vector<td::uint32>{7});
    EXPECT_EQ(observer_->candidate_received.size(), static_cast<size_t>(0));

    co_return td::Unit{};
  }
};
REGISTER_TEST(PrivateOverlay, SlotMismatchInExtraRejectedAfterPrecheck);

// Deliver a candidate from a peer that is not the expected collator for the
// extra slot and verify the precheck path rejects it early.
struct UnexpectedCollatorRejectedInPrecheck : PrivateOverlayTest {
  td::actor::Task<td::Unit> run_test() override {
    auto candidate = make_private_overlay_candidate(ctx(), *handle_, 6, 5, PeerValidatorId{0});
    auto extra = create_serialize_tl_object<ton_api::consensus_broadcastExtra>(5);

    auto result = co_await td::actor::ask(overlays_.get(), &MockOverlays::deliver_twostep_broadcast,
                                          peer(0).short_id, candidate->serialize(), std::move(extra))
                            .wrap();
    ASSERT_TRUE(result.is_error());
    EXPECT_EQ(observer_->precheck_slots.size(), static_cast<size_t>(0));
    EXPECT_EQ(observer_->candidate_received.size(), static_cast<size_t>(0));

    co_return td::Unit{};
  }
};
REGISTER_TEST(PrivateOverlay, UnexpectedCollatorRejectedInPrecheck);

// Deliver a well-formed broadcast from the expected collator and verify the
// private-overlay precheck path forwards the candidate into CandidateReceived.
struct ValidBroadcastPrechecksAndDeliversCandidateReceived : PrivateOverlayTest {
  td::actor::Task<td::Unit> run_test() override {
    auto candidate = make_private_overlay_candidate(ctx(), *handle_, 6, 5, PeerValidatorId{0});
    auto extra = create_serialize_tl_object<ton_api::consensus_broadcastExtra>(6);

    auto result = co_await td::actor::ask(overlays_.get(), &MockOverlays::deliver_twostep_broadcast,
                                          peer(0).short_id, candidate->serialize(), std::move(extra))
                            .wrap();
    ASSERT_TRUE(result.is_ok());

    co_await ts_.wait_sync_work();

    EXPECT_EQ(observer_->precheck_slots, std::vector<td::uint32>{6});
    ASSERT_EQ(observer_->candidate_received.size(), static_cast<size_t>(1));
    EXPECT_EQ(observer_->candidate_received[0]->candidate->id, candidate->id);

    co_return td::Unit{};
  }
};
REGISTER_TEST(PrivateOverlay, ValidBroadcastPrechecksAndDeliversCandidateReceived);

}  // namespace
}  // namespace ton::validator::consensus::test
