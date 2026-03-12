/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "adnl/adnl-node-id.hpp"
#include "adnl/adnl-test-loopback-implementation.h"
#include "adnl/adnl.h"
#include "common/errorlog.h"
#include "dht/dht.h"
#include "keys/keys.hpp"
#include "overlay/overlay-manager.h"
#include "overlay/overlay.h"
#include "td/actor/core/Scheduler.h"
#include "td/utils/port/path.h"
#include "td/utils/tests.h"
#include "ton/ton-tl.hpp"

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace ton::overlay::test {
namespace {

struct ReceivedPackets {
  std::vector<std::string> messages;
  std::vector<std::string> queries;
};

struct QueryResultSink {
  std::optional<td::BufferSlice> answer;
  std::optional<td::Status> error;
};

class RecordingCallback final : public Overlays::Callback {
 public:
  explicit RecordingCallback(std::shared_ptr<ReceivedPackets> packets) : packets_(std::move(packets)) {
  }

  void receive_message(adnl::AdnlNodeIdShort, OverlayIdShort, td::BufferSlice data) override {
    packets_->messages.push_back(data.as_slice().str());
  }

  void receive_query(adnl::AdnlNodeIdShort, OverlayIdShort, td::BufferSlice data,
                     td::Promise<td::BufferSlice> promise) override {
    packets_->queries.push_back(data.as_slice().str());
    promise.set_value(td::BufferSlice("buffered-query-answer"));
  }

 private:
  std::shared_ptr<ReceivedPackets> packets_;
};

struct OverlayNodeIds {
  PrivateKey adnl_pk{privkeys::Ed25519::random()};
  PublicKey adnl_pub{adnl_pk.compute_public_key()};
  adnl::AdnlNodeIdFull adnl_full{adnl_pub};
  adnl::AdnlNodeIdShort adnl_short{adnl_pub.compute_short_id()};
};

class OverlayManagerHarness {
 public:
  OverlayManagerHarness() {
    td::rmrf(db_root_).ignore();
    td::mkdir(db_root_).ensure();

    scheduler_.run_in_context([&] {
      errorlog::ErrorLog::create(db_root_);
      keyring_ = keyring::Keyring::create(db_root_);
      network_manager_ = td::actor::create_actor<adnl::TestLoopbackNetworkManager>("overlay-manager-test-net");
      adnl_ = adnl::Adnl::create(db_root_, keyring_.get());
      overlay_manager_ = td::actor::create_actor<OverlayManager>(
          "overlay-manager-under-test", db_root_, keyring_.get(), adnl_.get(), td::actor::ActorId<dht::Dht>{},
          OverlayManagerBufferLimits{.max_packets = 8, .max_data_size = 4096});
      td::actor::send_closure(adnl_, &adnl::Adnl::register_network_manager, network_manager_.get());

      auto addr = adnl::TestLoopbackNetworkManager::generate_dummy_addr_list();
      add_node(local_, addr);
      add_node(remote_, addr);
      td::actor::send_closure(adnl_, &adnl::Adnl::add_peer, local_.adnl_short, remote_.adnl_full, addr);
      td::actor::send_closure(adnl_, &adnl::Adnl::add_peer, remote_.adnl_short, local_.adnl_full, addr);

      auto overlay_id_full =
          create_serialize_tl_object<ton_api::pub_overlay>(td::BufferSlice("overlay-manager-buffering"));
      overlay_id_full_ = OverlayIdFull(overlay_id_full.clone());
      overlay_id_short_ = overlay_id_full_.compute_short_id();

      auto sentinel_overlay_id =
          create_serialize_tl_object<ton_api::pub_overlay>(td::BufferSlice("overlay-manager-sentinel"));
      td::actor::send_closure(
          overlay_manager_, &OverlayManager::create_private_overlay, local_.adnl_short,
          OverlayIdFull(sentinel_overlay_id.clone()), std::vector<adnl::AdnlNodeIdShort>{remote_.adnl_short},
          std::make_unique<RecordingCallback>(std::make_shared<ReceivedPackets>()),
          OverlayPrivacyRules(1 << 20, CertificateFlags::AllowFec | CertificateFlags::Trusted, {}), "");
    });
    drain();
  }

  ~OverlayManagerHarness() {
    scheduler_.run_in_context([&] {
      overlay_manager_.reset();
      adnl_.reset();
      network_manager_.reset();
      keyring_.reset();
    });
    drain();
    td::rmrf(db_root_).ignore();
  }

  void create_overlay(std::shared_ptr<ReceivedPackets> packets) {
    scheduler_.run_in_context([&] {
      td::actor::send_closure(overlay_manager_, &OverlayManager::create_private_overlay, local_.adnl_short,
                              overlay_id_full_.clone(), std::vector<adnl::AdnlNodeIdShort>{remote_.adnl_short},
                              std::make_unique<RecordingCallback>(std::move(packets)),
                              OverlayPrivacyRules(1 << 20,
                                                  CertificateFlags::AllowFec | CertificateFlags::Trusted, {}),
                              "");
    });
    drain();
  }

  void delete_overlay() {
    scheduler_.run_in_context([&] {
      td::actor::send_closure(overlay_manager_, &OverlayManager::delete_overlay, local_.adnl_short, overlay_id_short_);
    });
    drain();
  }

  void deliver_unknown_overlay_message(td::Slice payload) {
    scheduler_.run_in_context([&] {
      auto message = create_serialize_tl_object_suffix<ton_api::overlay_message>(payload, overlay_id_short_.tl());
      td::actor::send_closure(overlay_manager_, &OverlayManager::receive_message, remote_.adnl_short, local_.adnl_short,
                              std::move(message));
    });
    drain();
  }

  std::shared_ptr<QueryResultSink> deliver_unknown_overlay_query(td::Slice payload) {
    auto sink = std::make_shared<QueryResultSink>();
    scheduler_.run_in_context([&] {
      auto query = create_serialize_tl_object_suffix<ton_api::overlay_query>(payload, overlay_id_short_.tl());
      auto promise = td::PromiseCreator::lambda([sink](td::Result<td::BufferSlice> result) {
        if (result.is_ok()) {
          sink->answer = result.move_as_ok();
        } else {
          sink->error = result.move_as_error();
        }
      });
      td::actor::send_closure(overlay_manager_, &OverlayManager::receive_query, remote_.adnl_short, local_.adnl_short,
                              std::move(query), std::move(promise));
    });
    drain();
    return sink;
  }

 private:
  void add_node(OverlayNodeIds& node, const ton::adnl::AdnlAddressList& addr) {
    td::actor::send_closure(keyring_, &keyring::Keyring::add_key, std::move(node.adnl_pk), true, [](td::Result<>) {});
    td::actor::send_closure(adnl_, &adnl::Adnl::add_id, node.adnl_full, addr, static_cast<td::uint8>(0));
    td::actor::send_closure(network_manager_, &adnl::TestLoopbackNetworkManager::add_node_id, node.adnl_short, true,
                            true);
  }

  void drain() {
    for (int i = 0; i < 20; ++i) {
      scheduler_.run(0.01);
    }
  }

  td::actor::Scheduler scheduler_{{2}};
  td::actor::ActorOwn<keyring::Keyring> keyring_;
  td::actor::ActorOwn<adnl::TestLoopbackNetworkManager> network_manager_;
  td::actor::ActorOwn<adnl::Adnl> adnl_;
  td::actor::ActorOwn<OverlayManager> overlay_manager_;
  OverlayNodeIds local_;
  OverlayNodeIds remote_;
  OverlayIdFull overlay_id_full_;
  OverlayIdShort overlay_id_short_;
  std::string db_root_ = "tmp-dir-test-overlay-manager";
};

struct BuffersUnknownOverlayMessagesUntilCreation : td::Test {
  void run() final {
    // Covers tracker Kernel-31 commit a4c2dc4e2:
    // an incoming message for an unknown overlay must be buffered and replayed once that overlay
    // is created on the same local ADNL id.
    OverlayManagerHarness harness;
    auto packets = std::make_shared<ReceivedPackets>();

    harness.deliver_unknown_overlay_message("buffered-message");
    EXPECT(packets->messages.empty());

    harness.create_overlay(packets);
    EXPECT_EQ(packets->messages, std::vector<std::string>{"buffered-message"});
  }
};

REGISTER_TEST(OverlayManager, BuffersUnknownOverlayMessagesUntilCreation);

struct BuffersUnknownOverlayQueriesUntilCreation : td::Test {
  void run() final {
    // Covers tracker Kernel-31 commit a4c2dc4e2 on the query path:
    // an incoming query for an unknown overlay must be buffered and answered after overlay
    // registration instead of being rejected immediately.
    OverlayManagerHarness harness;
    auto packets = std::make_shared<ReceivedPackets>();

    auto buffered = harness.deliver_unknown_overlay_query("buffered-query");
    EXPECT(!buffered->answer.has_value());
    EXPECT(!buffered->error.has_value());

    harness.create_overlay(packets);
    ASSERT_TRUE(buffered->answer.has_value());
    EXPECT_EQ(buffered->answer->as_slice().str(), "buffered-query-answer");
    EXPECT(!buffered->error.has_value());

    auto live = harness.deliver_unknown_overlay_query("live-query");
    ASSERT_TRUE(live->answer.has_value());
    EXPECT(!live->error.has_value());

    EXPECT_EQ(packets->queries, std::vector<std::string>({"buffered-query", "live-query"}));
    EXPECT_EQ(live->answer->as_slice().str(), "buffered-query-answer");
  }
};

REGISTER_TEST(OverlayManager, BuffersUnknownOverlayQueriesUntilCreation);

}  // namespace
}  // namespace ton::overlay::test
