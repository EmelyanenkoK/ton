/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    TON Blockchain Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with TON Blockchain Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2017-2020 Telegram Systems LLP
*/
#pragma once

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "adnl/adnl.h"
#include "crypto/common/bitstring.h"
#include "rldp2/rldp.h"
#include "td/actor/actor.h"
#include "td/fec/raptorq/Decoder.h"
#include "td/utils/Time.h"
#include "td/utils/buffer.h"
#include "tl/generate/auto/tl/ton_api.h"

#include "overlay.h"
#include "overlays.h"

namespace ton {

namespace overlay {

class TwostepOverlayImpl : public Overlay {
 public:
  TwostepOverlayImpl(adnl::AdnlNodeIdShort local_id, OverlayIdShort overlay_id,
                     std::unique_ptr<Overlays::Callback> callback, td::actor::ActorId<Overlays> manager,
                     td::actor::ActorId<rldp2::Rldp> rldp2, std::vector<adnl::AdnlNodeIdShort> nodes);
  void alarm() override;
  void update_dht_node(td::actor::ActorId<dht::Dht> dht) override {
  }
  void receive_message(adnl::AdnlNodeIdShort src, tl_object_ptr<ton_api::overlay_messageExtra> extra,
                       td::BufferSlice data) override;
  void receive_query(adnl::AdnlNodeIdShort src, tl_object_ptr<ton_api::overlay_messageExtra> extra,
                     td::BufferSlice data, td::Promise<td::BufferSlice> promise) override;
  void send_message_to_neighbours(td::BufferSlice data) override {
  }
  void send_broadcast(PublicKeyHash send_as, td::uint32 flags, td::BufferSlice data) override;
  void send_broadcast_fec(PublicKeyHash send_as, td::uint32 flags, td::BufferSlice data) override {
    send_broadcast(send_as, flags, std::move(data));
  }
  void print(td::StringBuilder &sb) override {
  }
  void get_overlay_random_peers(td::uint32 max_peers,
                                td::Promise<std::vector<adnl::AdnlNodeIdShort>> promise) override {
  }
  void add_certificate(PublicKeyHash key, std::shared_ptr<Certificate>) override {
  }
  void set_privacy_rules(OverlayPrivacyRules rules) override {
  }
  void receive_nodes_from_db(tl_object_ptr<ton_api::overlay_nodes> nodes) override {
  }
  void receive_nodes_from_db_v2(tl_object_ptr<ton_api::overlay_nodesV2> nodes) override {
  }
  void get_stats(td::Promise<tl_object_ptr<ton_api::engine_validator_overlayStats>> promise) override {
  }
  void update_throughput_out_ctr(adnl::AdnlNodeIdShort peer_id, td::uint64 msg_size, bool is_query,
                                 bool is_response) override {
  }
  void update_throughput_in_ctr(adnl::AdnlNodeIdShort peer_id, td::uint64 msg_size, bool is_query,
                                bool is_response) override {
  }
  void update_peer_ip_str(adnl::AdnlNodeIdShort peer_id, td::string ip_str) override {
  }
  void update_member_certificate(OverlayMemberCertificate cert) override {
  }
  void update_root_member_list(std::vector<adnl::AdnlNodeIdShort> ids, std::vector<PublicKeyHash> root_public_keys,
                               OverlayMemberCertificate cert) override {
  }
  void forget_peer(adnl::AdnlNodeIdShort peer_id) override {
  }

 private:
  using TransferId = td::Bits256;
  class TransferData {
   public:
    TransferData();

    td::Timestamp timeout_;
    std::unique_ptr<td::raptorq::Decoder> decoder_;
  };

  void deliver_broadcast(const td::BufferSlice &data);

  adnl::AdnlNodeIdShort local_id_;
  OverlayIdShort overlay_id_;
  std::unique_ptr<Overlays::Callback> callback_;
  td::actor::ActorId<Overlays> manager_;
  td::actor::ActorId<rldp2::Rldp> rldp2_;
  std::vector<adnl::AdnlNodeIdShort> other_nodes_;
  std::map<TransferId, TransferData> transfers_;
};

}  // namespace overlay

}  // namespace ton
