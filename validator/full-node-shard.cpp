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
#include "adnl/utils.hpp"
#include "adnl/adnl-address-list.h"
#include "auto/tl/ton_api.h"
#include "auto/tl/ton_api_json.h"
#include "common/delay.h"
#include "http/http-client.h"
#include "impl/out-msg-queue-proof.hpp"
#include "net/download-archive-slice.hpp"
#include "net/download-block-new.hpp"
#include "net/download-proof.hpp"
#include "net/download-state.hpp"
#include "net/get-next-key-blocks.hpp"
#include "td/utils/JsonBuilder.h"
#include "td/utils/HttpUrl.h"
#include "td/utils/Random.h"
#include "td/utils/SharedSlice.h"
#include "td/utils/base64.h"
#include "td/utils/buffer.h"
#include "td/utils/misc.h"
#include "td/utils/overloaded.h"
#include "tl/tl_json.h"
#include "ton/ton-io.hpp"
#include "ton/ton-shard.h"
#include "ton/ton-tl.hpp"

#include "checksum.h"
#include "full-node-serializer.hpp"
#include "full-node-shard-queries.hpp"
#include "full-node-shard.hpp"
#include "overlays.h"

#include <algorithm>

namespace ton {

namespace validator {

namespace fullnode {

namespace {

constexpr const char *k_called_from_public = "public";
constexpr td::uint32 k_heavy_request_cost_unit = 1 << 21;

size_t heavy_request_cost(td::uint64 requested_max_size) {
  size_t cost = static_cast<size_t>((requested_max_size + k_heavy_request_cost_unit - 1) / k_heavy_request_cost_unit);
  return cost == 0 ? 1 : cost;
}

size_t request_cost_for_limiter(ton_api::Function &function) {
  size_t cost = 1;
  ton_api::downcast_call(
      function, td::overloaded(
                    [&](const ton_api::tonNode_getArchiveSlice &query) {
                      cost = heavy_request_cost(query.max_size_ > 0 ? static_cast<td::uint64>(query.max_size_) : 0);
                    },
                    [&](const ton_api::tonNode_downloadPersistentStateSliceV2 &query) {
                      cost = heavy_request_cost(query.max_size_ > 0 ? static_cast<td::uint64>(query.max_size_) : 0);
                    },
                    [&](const ton_api::tonNode_downloadZeroState &) {
                      cost = heavy_request_cost(FullNode::max_zerostate_size());
                    },
                    [&](const auto &) {}));
  return cost;
}

class ForceGoodPeersFetcher : public td::actor::Actor {
 public:
  ForceGoodPeersFetcher(std::string url, adnl::AdnlNodeIdShort local_id, td::actor::ActorId<adnl::Adnl> adnl,
                        td::actor::ActorId<FullNodeShardImpl> parent)
      : url_(std::move(url)), local_id_(local_id), adnl_(adnl), parent_(parent) {
  }

  void start_up() override {
    auto r_url = td::parse_url(url_);
    if (r_url.is_error()) {
      finish(r_url.move_as_error_prefix("bad force-good-peers URL: "));
      return;
    }
    url_info_ = r_url.move_as_ok();
    if (url_info_.protocol_ != td::HttpUrl::Protocol::Http) {
      finish(td::Status::Error("force-good-peers supports only http URLs"));
      return;
    }

    auto domain = url_info_.host_ + ":" + std::to_string(url_info_.port_);
    class Cb : public http::HttpClient::Callback {
     public:
      void on_ready() override {
      }
      void on_stop_ready() override {
      }
    };
    client_ = http::HttpClient::create_multi(domain, td::IPAddress(), 1, 1, std::make_shared<Cb>());

    auto r_request = http::HttpRequest::create("GET", url_info_.query_, "HTTP/1.1");
    if (r_request.is_error()) {
      finish(r_request.move_as_error_prefix("failed to create force-good-peers request: "));
      return;
    }
    auto request = r_request.move_as_ok();
    request->set_keep_alive(false);
    auto S = request->add_header(http::HttpHeader{"Host", domain});
    if (S.is_error()) {
      finish(S.move_as_error_prefix("failed to create force-good-peers request: "));
      return;
    }
    request->add_header(http::HttpHeader{"Accept", "application/json"}).ignore();
    request->add_header(http::HttpHeader{"User-Agent", "ton-validator-force-good-peers"}).ignore();

    auto promise = td::PromiseCreator::lambda(
        [SelfId = actor_id(this)](
            td::Result<std::pair<std::unique_ptr<http::HttpResponse>, std::shared_ptr<http::HttpPayload>>> R) mutable {
          td::actor::send_closure(SelfId, &ForceGoodPeersFetcher::got_response, std::move(R));
        });
    td::actor::send_closure(client_, &http::HttpClient::send_request, std::move(request),
                            std::make_shared<http::HttpPayload>(http::HttpPayload::PayloadType::pt_empty),
                            td::Timestamp::in(10.0), std::move(promise));
  }

  void got_response(
      td::Result<std::pair<std::unique_ptr<http::HttpResponse>, std::shared_ptr<http::HttpPayload>>> R) {
    if (R.is_error()) {
      finish(R.move_as_error_prefix("failed to fetch force-good-peers: "));
      return;
    }
    auto response = R.move_as_ok();
    if (response.first->code() != http::HttpStatusCode::status_ok) {
      finish(td::Status::Error(PSTRING() << "force-good-peers HTTP status " << response.first->code()));
      return;
    }

    auto payload = std::move(response.second);
    if (payload->parse_completed()) {
      got_payload(std::move(payload));
      return;
    }

    class PayloadCallback : public http::HttpPayload::Callback {
     public:
      PayloadCallback(td::actor::ActorId<ForceGoodPeersFetcher> fetcher, std::shared_ptr<http::HttpPayload> payload)
          : fetcher_(fetcher), payload_(std::move(payload)) {
      }
      void run(size_t ready_bytes) override {
      }
      void completed() override {
        td::actor::send_closure(fetcher_, &ForceGoodPeersFetcher::got_payload, payload_);
      }

     private:
      td::actor::ActorId<ForceGoodPeersFetcher> fetcher_;
      std::shared_ptr<http::HttpPayload> payload_;
    };
    auto raw_payload = payload;
    payload->add_callback(std::make_unique<PayloadCallback>(actor_id(this), std::move(payload)));
    if (raw_payload->parse_completed()) {
      got_payload(std::move(raw_payload));
    }
  }

  void got_payload(std::shared_ptr<http::HttpPayload> payload) {
    if (finished_) {
      return;
    }
    auto R = parse_payload(std::move(payload));
    finish(std::move(R));
  }

 private:
  td::Result<std::string> read_payload(http::HttpPayload &payload) {
    static constexpr size_t max_body_size = 1 << 20;
    std::string body;
    while (true) {
      auto chunk = payload.get_slice(max_body_size);
      if (chunk.empty()) {
        break;
      }
      if (body.size() + chunk.size() > max_body_size) {
        return td::Status::Error("force-good-peers response is too large");
      }
      auto slice = chunk.as_slice();
      body.append(slice.begin(), slice.size());
    }
    return body;
  }

  td::Result<adnl::AdnlNodeIdShort> parse_peer(td::Slice declared_name, const td::JsonValue &value) {
    if (value.type() != td::JsonValue::Type::Object) {
      return td::Status::Error("peer entry is not an object");
    }

    TRY_RESULT(decoded_id, td::hex_decode(declared_name));
    if (decoded_id.size() != 32) {
      return td::Status::Error("peer key is not a 32-byte ADNL id");
    }
    adnl::AdnlNodeIdShort declared_id{td::Slice(decoded_id)};

    const auto &obj = value.get_object();
    TRY_RESULT(host, obj.get_required_string_field("host"));
    TRY_RESULT(port, obj.get_required_int_field("port"));
    TRY_RESULT(pub_key_b64, obj.get_required_string_field("pub_key"));
    if (port <= 0 || port > 65535) {
      return td::Status::Error("bad peer port");
    }

    TRY_RESULT(pub_key_raw, td::base64_decode(pub_key_b64));
    if (pub_key_raw.size() != 32) {
      return td::Status::Error("bad peer public key size");
    }
    td::Bits256 pub_key_bits;
    pub_key_bits.as_slice().copy_from(td::Slice(pub_key_raw));
    auto full_id = adnl::AdnlNodeIdFull{PublicKey{pubkeys::Ed25519{pub_key_bits}}};
    auto short_id = full_id.compute_short_id();
    if (short_id != declared_id) {
      return td::Status::Error("peer public key does not match declared ADNL id");
    }
    if (short_id == local_id_) {
      return td::Status::Error("peer is local ADNL id");
    }

    td::IPAddress addr;
    TRY_STATUS(addr.init_host_port(host, port));
    if (!addr.is_ipv4()) {
      return td::Status::Error("only IPv4 force-good-peers addresses are supported");
    }

    adnl::AdnlAddressList addr_list;
    TRY_STATUS(addr_list.add_udp_adnl_address(addr));
    auto now = static_cast<td::uint32>(td::Clocks::system());
    addr_list.set_version(now);
    addr_list.set_reinit_date(adnl::Adnl::adnl_start_time());
    td::actor::send_closure(adnl_, &adnl::Adnl::add_peer, local_id_, std::move(full_id), std::move(addr_list));
    return short_id;
  }

  td::Result<std::vector<adnl::AdnlNodeIdShort>> parse_payload(std::shared_ptr<http::HttpPayload> payload) {
    TRY_RESULT(body, read_payload(*payload));
    auto json = td::json_decode(td::MutableSlice(body.data(), body.size()));
    if (json.is_error()) {
      return json.move_as_error_prefix("failed to parse force-good-peers JSON: ");
    }
    auto root = json.move_as_ok();
    if (root.type() != td::JsonValue::Type::Object) {
      return td::Status::Error("force-good-peers JSON root must be an object");
    }

    std::vector<adnl::AdnlNodeIdShort> peers;
    std::set<adnl::AdnlNodeIdShort> seen;
    size_t skipped = 0;
    root.get_object().foreach([&](td::Slice name, const td::JsonValue &value) {
      auto r_peer = parse_peer(name, value);
      if (r_peer.is_error()) {
        skipped++;
        VLOG(FULL_NODE_DEBUG) << "skipping force-good-peers entry " << name << ": " << r_peer.move_as_error();
        return;
      }
      auto peer = r_peer.move_as_ok();
      if (seen.insert(peer).second) {
        peers.push_back(peer);
      }
    });
    LOG(INFO) << "loaded force-good-peers url=" << url_ << " peers=" << peers.size() << " skipped=" << skipped;
    return peers;
  }

  void finish(td::Result<std::vector<adnl::AdnlNodeIdShort>> R) {
    if (finished_) {
      return;
    }
    finished_ = true;
    td::actor::send_closure(parent_, &FullNodeShardImpl::got_force_good_peers, std::move(R));
    stop();
  }

  void finish(td::Status S) {
    finish(td::Result<std::vector<adnl::AdnlNodeIdShort>>(std::move(S)));
  }

  std::string url_;
  td::HttpUrl url_info_{td::HttpUrl::Protocol::Http, "", "", false, 0, 0, "/"};
  adnl::AdnlNodeIdShort local_id_;
  td::actor::ActorId<adnl::Adnl> adnl_;
  td::actor::ActorId<FullNodeShardImpl> parent_;
  td::actor::ActorOwn<http::HttpClient> client_;
  bool finished_ = false;
};

}  // namespace

Neighbour Neighbour::zero = Neighbour{adnl::AdnlNodeIdShort::zero()};

void Neighbour::update_proto_version(ton_api::tonNode_capabilities &q) {
  version_major = q.version_major_;
  version_minor = q.version_minor_;
  flags = q.flags_;
}

void Neighbour::query_success(double t) {
  unreliability--;
  if (unreliability < 0) {
    unreliability = 0;
  }
  update_roundtrip(t);
}

void Neighbour::query_failed() {
  unreliability++;
}

void Neighbour::update_roundtrip(double t) {
  roundtrip = (t + roundtrip) * 0.5;
}

void FullNodeShardImpl::create_overlay() {
  class Callback : public overlay::Overlays::Callback {
   public:
    void receive_message(adnl::AdnlNodeIdShort src, overlay::OverlayIdShort overlay_id, td::BufferSlice data) override {
      td::actor::send_closure(node_, &FullNodeShardImpl::receive_message, src, std::move(data));
    }
    void receive_query(adnl::AdnlNodeIdShort src, overlay::OverlayIdShort overlay_id, td::BufferSlice data,
                       td::Promise<td::BufferSlice> promise) override {
      td::actor::send_closure(node_, &FullNodeShardImpl::receive_query, src, std::move(data), std::move(promise));
    }
    void receive_broadcast(PublicKeyHash src, overlay::OverlayIdShort overlay_id, td::BufferSlice data) override {
      td::actor::send_closure(node_, &FullNodeShardImpl::receive_broadcast, src, std::move(data));
    }
    void check_broadcast(PublicKeyHash src, overlay::OverlayIdShort overlay_id, td::BufferSlice data,
                         td::Promise<td::Unit> promise) override {
      td::actor::send_closure(node_, &FullNodeShardImpl::check_broadcast, src, std::move(data), std::move(promise));
    }
    void get_stats_extra(td::Promise<std::string> promise) override {
      td::actor::send_closure(node_, &FullNodeShardImpl::get_stats_extra, std::move(promise));
    }
    Callback(td::actor::ActorId<FullNodeShardImpl> node) : node_(node) {
    }

   private:
    td::actor::ActorId<FullNodeShardImpl> node_;
  };
  overlay::OverlayOptions opts;
  opts.name_ = "shard" + shard_.to_str();
  opts.announce_self_ = active_;
  opts.broadcast_speed_multiplier_ = opts_.public_broadcast_speed_multiplier_;
  bool enlarge_neighbour_pool = opts_.rebroadcast_from_custom_.allows_workchain(shard_.workchain);
  if (!enlarge_neighbour_pool && shard_.is_masterchain() && opts_.rebroadcast_from_custom_.enabled_ &&
      opts_.rebroadcast_from_custom_.candidates_enabled_ &&
      !opts_.rebroadcast_from_custom_.allowed_workchains_.empty()) {
    // Public candidate broadcasts are sent through the masterchain overlay even for basechain blocks.
    enlarge_neighbour_pool = true;
  }
  if (enlarge_neighbour_pool) {
    opts.max_neighbours_ = opts_.rebroadcast_from_custom_.peer_target_;
    opts.max_peers_ = std::max<td::uint32>(20, opts_.rebroadcast_from_custom_.peer_target_ * 4);
    opts.nodes_to_send_ = std::max<td::uint32>(4, std::min<td::uint32>(32, opts_.rebroadcast_from_custom_.peer_target_));
  }
  td::actor::send_closure(overlays_, &overlay::Overlays::create_public_overlay_ex, adnl_id_, overlay_id_full_.clone(),
                          std::make_unique<Callback>(actor_id(this)), rules_,
                          PSTRING() << "{ \"type\": \"shard\", \"shard_id\": " << get_shard()
                                    << ", \"workchain_id\": " << get_workchain() << " }",
                          opts);

  td::actor::send_closure(rldp_, &rldp::Rldp::add_id, adnl_id_);
  td::actor::send_closure(rldp2_, &rldp2::Rldp::add_id, adnl_id_);
  if (cert_) {
    td::actor::send_closure(overlays_, &overlay::Overlays::update_certificate, adnl_id_, overlay_id_, local_id_, cert_);
  }
}

bool FullNodeShardImpl::uses_force_good_peers() const {
  if (opts_.rebroadcast_from_custom_.force_good_peers_url_.empty()) {
    return false;
  }
  const bool custom_enabled = opts_.rebroadcast_from_custom_.enabled_;
  const bool downloaded_enabled = opts_.force_download_.rebroadcast_downloaded_block_;
  if (!custom_enabled && !downloaded_enabled) {
    return false;
  }
  if (opts_.rebroadcast_from_custom_.allowed_workchains_.count(shard_.workchain) > 0) {
    return true;
  }
  return shard_.is_masterchain() && !opts_.rebroadcast_from_custom_.allowed_workchains_.empty() &&
         (opts_.rebroadcast_from_custom_.candidates_enabled_ || downloaded_enabled);
}

void FullNodeShardImpl::refresh_force_good_peers() {
  if (!uses_force_good_peers() || force_good_peers_refresh_active_) {
    return;
  }
  force_good_peers_refresh_active_ = true;
  refresh_force_good_peers_at_ = td::Timestamp::never();
  td::actor::create_actor<ForceGoodPeersFetcher>("forcegoodpeers", opts_.rebroadcast_from_custom_.force_good_peers_url_,
                                                 adnl_id_, adnl_, actor_id(this))
      .release();
}

void FullNodeShardImpl::got_force_good_peers(td::Result<std::vector<adnl::AdnlNodeIdShort>> peers) {
  force_good_peers_refresh_active_ = false;
  if (peers.is_error()) {
    LOG(WARNING) << "failed to refresh force-good-peers shard=" << shard_.to_str() << ": " << peers.move_as_error();
    refresh_force_good_peers_at_ = td::Timestamp::in(td::Random::fast(10.0, 20.0));
  } else {
    force_good_peers_ = peers.move_as_ok();
    LOG(INFO) << "using force-good-peers shard=" << shard_.to_str() << " peers=" << force_good_peers_.size();
    refresh_force_good_peers_at_ = td::Timestamp::in(td::Random::fast(50.0, 70.0));
  }
  alarm_timestamp().relax(refresh_force_good_peers_at_);
}

std::vector<adnl::AdnlNodeIdShort> FullNodeShardImpl::choose_force_good_peers(td::uint32 fanout_override) const {
  if (fanout_override == 0 || force_good_peers_.empty()) {
    return {};
  }
  if (force_good_peers_.size() <= fanout_override) {
    return force_good_peers_;
  }

  auto pool = force_good_peers_;
  std::vector<adnl::AdnlNodeIdShort> result;
  result.reserve(fanout_override);
  while (result.size() < fanout_override && !pool.empty()) {
    auto pos = static_cast<size_t>(td::Random::fast(0, static_cast<int>(pool.size() - 1)));
    result.push_back(pool[pos]);
    pool[pos] = pool.back();
    pool.pop_back();
  }
  return result;
}

bool FullNodeShardImpl::uses_force_download_peers() const {
  return client_.empty() && !opts_.force_download_.peers_.empty();
}

std::vector<adnl::AdnlNodeIdShort> FullNodeShardImpl::choose_force_download_peers() const {
  std::vector<adnl::AdnlNodeIdShort> pool;
  pool.reserve(opts_.force_download_.peers_.size());
  for (const auto &peer : opts_.force_download_.peers_) {
    if (!peer.is_zero() && peer != adnl_id_) {
      pool.push_back(peer);
    }
  }
  if (pool.empty()) {
    return {};
  }

  size_t limit = std::min<size_t>(opts_.force_download_.attempts_num_, pool.size());
  std::vector<adnl::AdnlNodeIdShort> result;
  result.reserve(limit);
  while (result.size() < limit && !pool.empty()) {
    auto pos = static_cast<size_t>(td::Random::fast(0, static_cast<int>(pool.size() - 1)));
    result.push_back(pool[pos]);
    pool[pos] = pool.back();
    pool.pop_back();
  }
  return result;
}

void FullNodeShardImpl::finish_download_block(DownloadedBlock block, td::Promise<ReceivedBlock> promise) {
  if (block.from_network) {
    LOG(INFO) << "block download finished block=" << block.block.id.to_str()
              << " force_download_peers_enabled=" << uses_force_download_peers()
              << " rebroadcast_downloaded_enabled=" << opts_.force_download_.rebroadcast_downloaded_block_
              << " is_proof_link=" << block.is_proof_link;
  }
  if (opts_.force_download_.rebroadcast_downloaded_block_ && block.from_network) {
    td::actor::send_closure(full_node_, &FullNode::process_downloaded_block_for_rebroadcast, block.clone());
  }
  promise.set_value(std::move(block.block));
}

void FullNodeShardImpl::check_broadcast(PublicKeyHash src, td::BufferSlice broadcast, td::Promise<td::Unit> promise) {
  TRY_RESULT_PROMISE(promise, message,
                     fetch_tl_object<ton_api::tonNode_externalMessageBroadcast>(std::move(broadcast), true));
  if (opts_.config_.ext_messages_broadcast_disabled_) {
    promise.set_error(td::Status::Error("rebroadcasting external messages is disabled"));
    promise = [](td::Result<td::Unit>) {};
  }
  process_external_message_broadcast(*message, std::move(promise));
}

void FullNodeShardImpl::process_external_message_broadcast(ton_api::tonNode_externalMessageBroadcast &message,
                                                           td::Promise<td::Unit> promise) {
  if (!active_) {
    return promise.set_error(td::Status::Error("cannot process broadcast: shard is not active"));
  }
  auto hash = td::sha256_bits256(message.message_->data_);
  if (!processed_ext_msg_broadcasts_.insert(hash).second) {
    return promise.set_error(td::Status::Error("duplicate external message broadcast"));
  }
  if (my_ext_msg_broadcasts_.contains(hash)) {
    // Don't process messages that were sent by us
    promise.set_result(td::Unit());
    return;
  }
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::new_external_message_broadcast,
                          std::move(message.message_->data_), 0, std::move(promise));
}

void FullNodeShardImpl::remove_neighbour(adnl::AdnlNodeIdShort id) {
  neighbours_.erase(id);
}

void FullNodeShardImpl::update_adnl_id(adnl::AdnlNodeIdShort adnl_id, td::Promise<td::Unit> promise) {
  td::actor::send_closure(overlays_, &ton::overlay::Overlays::delete_overlay, adnl_id_, overlay_id_);
  adnl_id_ = adnl_id;
  local_id_ = adnl_id_.pubkey_hash();
  create_overlay();
  promise.set_value(td::Unit{});
}

void FullNodeShardImpl::set_active(bool active) {
  if (shard_.is_masterchain()) {
    return;
  }
  if (active_ == active) {
    return;
  }
  active_ = active;
  td::actor::send_closure(overlays_, &ton::overlay::Overlays::delete_overlay, adnl_id_, overlay_id_);
  create_overlay();
}

void FullNodeShardImpl::try_get_next_block(td::Timestamp timeout, td::Promise<ReceivedBlock> promise) {
  if (timeout.is_in_past()) {
    promise.set_error(td::Status::Error(ErrorCode::timeout, "timeout"));
    return;
  }

  td::Promise<DownloadedBlock> P = td::PromiseCreator::lambda(
      [SelfId = actor_id(this), promise = std::move(promise)](td::Result<DownloadedBlock> R) mutable {
        if (R.is_error()) {
          promise.set_error(R.move_as_error());
        } else {
          td::actor::send_closure(SelfId, &FullNodeShardImpl::finish_download_block, R.move_as_ok(),
                                  std::move(promise));
        }
      });
  if (uses_force_download_peers()) {
    auto force_peers = choose_force_download_peers();
    LOG(INFO) << "forced block download start mode=next prev_block=" << handle_->id().to_str()
              << " selected_peers=" << force_peers.size()
              << " configured_peers=" << opts_.force_download_.peers_.size()
              << " attempts_target=" << opts_.force_download_.attempts_num_;
    td::actor::create_actor<DownloadBlockNewParallel>("downloadnext-forced", adnl_id_, overlay_id_, handle_->id(),
                                                      std::move(force_peers), download_next_priority(), timeout,
                                                      validator_manager_, rldp_, overlays_, adnl_, client_,
                                                      std::move(P))
        .release();
    return;
  }

  auto &b = choose_neighbour();
  td::actor::create_actor<DownloadBlockNew>("downloadnext", adnl_id_, overlay_id_, handle_->id(), b.adnl_id,
                                            download_next_priority(), timeout, validator_manager_, rldp_, overlays_,
                                            adnl_, client_, create_neighbour_promise(b, std::move(P)))
      .release();
}

void FullNodeShardImpl::got_next_block(td::Result<BlockHandle> R) {
  if (R.is_error()) {
    if (R.error().code() != ErrorCode::timeout && R.error().code() != ErrorCode::notready) {
      LOG(WARNING) << "Failed to get next block: " << R.move_as_error();
    }
    delay_action([SelfId = actor_id(this)]() { td::actor::send_closure(SelfId, &FullNodeShardImpl::get_next_block); },
                 td::Timestamp::in(0.1));
    return;
  }
  attempt_ = 0;
  R.ensure();
  auto old_seqno = handle_->id().id.seqno;
  handle_ = R.move_as_ok();
  CHECK(handle_->id().id.seqno == old_seqno + 1);

  if (promise_) {
    if (handle_->unix_time() > td::Clocks::system() - 300) {
      promise_.set_value(td::Unit());
    } else {
      sync_completed_at_ = td::Timestamp::in(opts_.initial_sync_delay_);
    }
  }
  get_next_block();
}

void FullNodeShardImpl::get_next_block() {
  attempt_++;
  auto P = td::PromiseCreator::lambda([validator_manager = validator_manager_, attempt = attempt_,
                                       block_id = handle_->id(), SelfId = actor_id(this)](td::Result<ReceivedBlock> R) {
    if (R.is_ok()) {
      auto P = td::PromiseCreator::lambda([SelfId](td::Result<BlockHandle> R) {
        td::actor::send_closure(SelfId, &FullNodeShardImpl::got_next_block, std::move(R));
      });
      td::actor::send_closure(validator_manager, &ValidatorManagerInterface::validate_block, R.move_as_ok(),
                              std::move(P));
    } else {
      auto S = R.move_as_error();
      if (S.code() != ErrorCode::notready && S.code() != ErrorCode::timeout) {
        VLOG(FULL_NODE_WARNING) << "failed to download next block after " << block_id << ": " << S;
      } else {
        if ((attempt % 128) == 0) {
          VLOG(FULL_NODE_INFO) << "failed to download next block after " << block_id << ": " << S;
        } else {
          VLOG(FULL_NODE_DEBUG) << "failed to download next block after " << block_id << ": " << S;
        }
      }
      delay_action([SelfId]() mutable { td::actor::send_closure(SelfId, &FullNodeShardImpl::get_next_block); },
                   td::Timestamp::in(0.1));
    }
  });
  try_get_next_block(td::Timestamp::in(2.0), std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_getNextBlockDescription &query,
                                      td::Promise<td::BufferSlice> promise) {
  if (query.prev_block_->workchain_ != masterchainId || static_cast<ShardId>(query.prev_block_->shard_) != shardIdAll) {
    promise.set_error(td::Status::Error(ErrorCode::protoviolation, "next block allowed only for masterchain"));
    return;
  }
  auto P = td::PromiseCreator::lambda([promise = std::move(promise)](td::Result<BlockHandle> R) mutable {
    if (R.is_error()) {
      auto x = create_serialize_tl_object<ton_api::tonNode_blockDescriptionEmpty>();
      promise.set_value(std::move(x));
    } else {
      auto B = R.move_as_ok();
      if (!B->received() || !B->inited_proof()) {
        auto x = create_serialize_tl_object<ton_api::tonNode_blockDescriptionEmpty>();
        promise.set_value(std::move(x));
      } else {
        auto x = create_serialize_tl_object<ton_api::tonNode_blockDescription>(create_tl_block_id(B->id()));
        promise.set_value(std::move(x));
      }
    }
  });
  BlockIdExt block_id = create_block_id(query.prev_block_);
  VLOG(FULL_NODE_DEBUG) << "Got query getNextBlockDescription " << block_id.to_str() << " from " << src;
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_next_block, block_id, std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_prepareBlock &query,
                                      td::Promise<td::BufferSlice> promise) {
  auto P = td::PromiseCreator::lambda([promise = std::move(promise)](td::Result<BlockHandle> R) mutable {
    if (R.is_error()) {
      auto x = create_serialize_tl_object<ton_api::tonNode_notFound>();
      promise.set_value(std::move(x));
    } else {
      auto B = R.move_as_ok();
      if (!B->received()) {
        auto x = create_serialize_tl_object<ton_api::tonNode_notFound>();
        promise.set_value(std::move(x));
      } else {
        auto x = create_serialize_tl_object<ton_api::tonNode_prepared>();
        promise.set_value(std::move(x));
      }
    }
  });
  BlockIdExt block_id = create_block_id(query.block_);
  VLOG(FULL_NODE_DEBUG) << "Got query prepareBlock " << block_id.to_str() << " from " << src;
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_block_handle, block_id, false,
                          std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_downloadBlock &query,
                                      td::Promise<td::BufferSlice> promise) {
  auto P = td::PromiseCreator::lambda([validator_manager = validator_manager_,
                                       promise = std::move(promise)](td::Result<BlockHandle> R) mutable {
    if (R.is_error()) {
      promise.set_error(td::Status::Error(ErrorCode::protoviolation, "unknown block"));
    } else {
      auto B = R.move_as_ok();
      if (!B->received()) {
        promise.set_error(td::Status::Error(ErrorCode::protoviolation, "unknown block"));
      } else {
        td::actor::send_closure(validator_manager, &ValidatorManagerInterface::get_block_data, B, std::move(promise));
      }
    }
  });
  BlockIdExt block_id = create_block_id(query.block_);
  VLOG(FULL_NODE_DEBUG) << "Got query downloadBlock " << block_id.to_str() << " from " << src;
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_block_handle, block_id, false,
                          std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_downloadBlockFull &query,
                                      td::Promise<td::BufferSlice> promise) {
  BlockIdExt block_id = create_block_id(query.block_);
  VLOG(FULL_NODE_DEBUG) << "Got query downloadBlockFull " << block_id.to_str() << " from " << src;
  td::actor::create_actor<BlockFullSender>("sender", block_id, false, validator_manager_, std::move(promise)).release();
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_downloadNextBlockFull &query,
                                      td::Promise<td::BufferSlice> promise) {
  BlockIdExt block_id = create_block_id(query.prev_block_);
  VLOG(FULL_NODE_DEBUG) << "Got query downloadNextBlockFull " << block_id.to_str() << " from " << src;
  td::actor::create_actor<BlockFullSender>("sender", block_id, true, validator_manager_, std::move(promise)).release();
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_prepareBlockProof &query,
                                      td::Promise<td::BufferSlice> promise) {
  if (query.block_->seqno_ == 0) {
    promise.set_error(td::Status::Error(ErrorCode::protoviolation, "cannot download proof for zero state"));
    return;
  }
  auto P = td::PromiseCreator::lambda([allow_partial = query.allow_partial_, promise = std::move(promise),
                                       validator_manager = validator_manager_](td::Result<BlockHandle> R) mutable {
    if (R.is_error()) {
      auto x = create_serialize_tl_object<ton_api::tonNode_preparedProofEmpty>();
      promise.set_value(std::move(x));
      return;
    } else {
      auto handle = R.move_as_ok();
      if (!handle || (!handle->inited_proof() && (!allow_partial || !handle->inited_proof_link()))) {
        auto x = create_serialize_tl_object<ton_api::tonNode_preparedProofEmpty>();
        promise.set_value(std::move(x));
        return;
      }
      if (handle->inited_proof() && handle->id().is_masterchain()) {
        auto x = create_serialize_tl_object<ton_api::tonNode_preparedProof>();
        promise.set_value(std::move(x));
      } else {
        auto x = create_serialize_tl_object<ton_api::tonNode_preparedProofLink>();
        promise.set_value(std::move(x));
      }
    }
  });

  BlockIdExt block_id = create_block_id(query.block_);
  VLOG(FULL_NODE_DEBUG) << "Got query prepareBlockProof " << block_id.to_str() << " from " << src;
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_block_handle, block_id, false,
                          std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_prepareKeyBlockProof &query,
                                      td::Promise<td::BufferSlice> promise) {
  if (query.block_->seqno_ == 0) {
    promise.set_error(td::Status::Error(ErrorCode::protoviolation, "cannot download proof for zero state"));
    return;
  }
  auto P = td::PromiseCreator::lambda(
      [allow_partial = query.allow_partial_, promise = std::move(promise)](td::Result<td::BufferSlice> R) mutable {
        if (R.is_error()) {
          auto x = create_serialize_tl_object<ton_api::tonNode_preparedProofEmpty>();
          promise.set_value(std::move(x));
        } else if (allow_partial) {
          auto x = create_serialize_tl_object<ton_api::tonNode_preparedProofLink>();
          promise.set_value(std::move(x));
        } else {
          auto x = create_serialize_tl_object<ton_api::tonNode_preparedProof>();
          promise.set_value(std::move(x));
        }
      });

  BlockIdExt block_id = create_block_id(query.block_);
  VLOG(FULL_NODE_DEBUG) << "Got query prepareKeyBlockProof " << block_id.to_str() << " " << query.allow_partial_
                        << " from " << src;
  if (query.allow_partial_) {
    td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_key_block_proof_link, block_id,
                            std::move(P));
  } else {
    td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_key_block_proof, block_id,
                            std::move(P));
  }
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_downloadBlockProof &query,
                                      td::Promise<td::BufferSlice> promise) {
  auto P = td::PromiseCreator::lambda(
      [promise = std::move(promise), validator_manager = validator_manager_](td::Result<BlockHandle> R) mutable {
        if (R.is_error()) {
          promise.set_error(td::Status::Error(ErrorCode::protoviolation, "unknown block proof"));
          return;
        } else {
          auto handle = R.move_as_ok();
          if (!handle || !handle->inited_proof()) {
            promise.set_error(td::Status::Error(ErrorCode::protoviolation, "unknown block proof"));
            return;
          }

          td::actor::send_closure(validator_manager, &ValidatorManagerInterface::get_block_proof, handle,
                                  std::move(promise));
        }
      });

  BlockIdExt block_id = create_block_id(query.block_);
  VLOG(FULL_NODE_DEBUG) << "Got query downloadBlockProof " << block_id.to_str() << " from " << src;
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_block_handle, block_id, false,
                          std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_downloadBlockProofLink &query,
                                      td::Promise<td::BufferSlice> promise) {
  auto P = td::PromiseCreator::lambda(
      [promise = std::move(promise), validator_manager = validator_manager_](td::Result<BlockHandle> R) mutable {
        if (R.is_error()) {
          promise.set_error(td::Status::Error(ErrorCode::protoviolation, "unknown block proof"));
          return;
        } else {
          auto handle = R.move_as_ok();
          if (!handle || !handle->inited_proof_link()) {
            promise.set_error(td::Status::Error(ErrorCode::protoviolation, "unknown block proof"));
            return;
          }

          td::actor::send_closure(validator_manager, &ValidatorManagerInterface::get_block_proof_link, handle,
                                  std::move(promise));
        }
      });

  BlockIdExt block_id = create_block_id(query.block_);
  VLOG(FULL_NODE_DEBUG) << "Got query downloadBlockProofLink " << block_id.to_str() << " from " << src;
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_block_handle, block_id, false,
                          std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_downloadKeyBlockProof &query,
                                      td::Promise<td::BufferSlice> promise) {
  if (query.block_->seqno_ == 0) {
    promise.set_error(td::Status::Error(ErrorCode::protoviolation, "cannot download proof for zero state"));
    return;
  }
  auto P = td::PromiseCreator::lambda([promise = std::move(promise)](td::Result<td::BufferSlice> R) mutable {
    if (R.is_error()) {
      promise.set_error(td::Status::Error(ErrorCode::protoviolation, "unknown block proof"));
    } else {
      promise.set_value(R.move_as_ok());
    }
  });

  BlockIdExt block_id = create_block_id(query.block_);
  VLOG(FULL_NODE_DEBUG) << "Got query downloadKeyBlockProof " << block_id.to_str() << " from " << src;
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_key_block_proof, block_id, std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_downloadKeyBlockProofLink &query,
                                      td::Promise<td::BufferSlice> promise) {
  if (query.block_->seqno_ == 0) {
    promise.set_error(td::Status::Error(ErrorCode::protoviolation, "cannot download proof for zero state"));
    return;
  }
  auto P = td::PromiseCreator::lambda([promise = std::move(promise)](td::Result<td::BufferSlice> R) mutable {
    if (R.is_error()) {
      promise.set_error(td::Status::Error(ErrorCode::protoviolation, "unknown block proof"));
    } else {
      promise.set_value(R.move_as_ok());
    }
  });

  BlockIdExt block_id = create_block_id(query.block_);
  VLOG(FULL_NODE_DEBUG) << "Got query downloadKeyBlockProofLink " << block_id.to_str() << " from " << src;
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_key_block_proof_link, block_id,
                          std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_prepareZeroState &query,
                                      td::Promise<td::BufferSlice> promise) {
  auto P =
      td::PromiseCreator::lambda([SelfId = actor_id(this), promise = std::move(promise)](td::Result<bool> R) mutable {
        if (R.is_error() || !R.move_as_ok()) {
          auto x = create_serialize_tl_object<ton_api::tonNode_notFoundState>();
          promise.set_value(std::move(x));
          return;
        }

        auto x = create_serialize_tl_object<ton_api::tonNode_preparedState>();
        promise.set_value(std::move(x));
      });
  auto block_id = create_block_id(query.block_);
  VLOG(FULL_NODE_DEBUG) << "Got query prepareZeroState " << block_id.to_str() << " from " << src;
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::check_zero_state_exists, block_id,
                          std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_preparePersistentState &query,
                                      td::Promise<td::BufferSlice> promise) {
  auto P = td::PromiseCreator::lambda(
      [SelfId = actor_id(this), promise = std::move(promise)](td::Result<td::uint64> R) mutable {
        if (R.is_error()) {
          auto x = create_serialize_tl_object<ton_api::tonNode_notFoundState>();
          promise.set_value(std::move(x));
          return;
        }
        auto x = create_serialize_tl_object<ton_api::tonNode_preparedState>();
        promise.set_value(std::move(x));
      });
  auto block_id = create_block_id(query.block_);
  auto masterchain_block_id = create_block_id(query.masterchain_block_);
  VLOG(FULL_NODE_DEBUG) << "Got query preparePersistentState " << block_id.to_str() << " "
                        << masterchain_block_id.to_str() << " from " << src;
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_persistent_state_size, block_id,
                          masterchain_block_id, UnsplitStateType{}, std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_getNextKeyBlockIds &query,
                                      td::Promise<td::BufferSlice> promise) {
  auto cnt = static_cast<td::uint32>(query.max_size_);
  if (cnt > 8) {
    cnt = 8;
  }
  auto P =
      td::PromiseCreator::lambda([promise = std::move(promise), cnt](td::Result<std::vector<BlockIdExt>> R) mutable {
        if (R.is_error()) {
          if (R.error().code() == ErrorCode::notready) {
            LOG(DEBUG) << "getnextkey: " << R.move_as_error();
          } else {
            LOG(WARNING) << "getnextkey: " << R.move_as_error();
          }
          auto x = create_serialize_tl_object<ton_api::tonNode_keyBlocks>(
              std::vector<tl_object_ptr<ton_api::tonNode_blockIdExt>>{}, false, true);
          promise.set_value(std::move(x));
          return;
        }
        auto res = R.move_as_ok();
        std::vector<tl_object_ptr<ton_api::tonNode_blockIdExt>> v;
        for (auto &b : res) {
          v.emplace_back(create_tl_block_id(b));
        }
        auto x = create_serialize_tl_object<ton_api::tonNode_keyBlocks>(std::move(v), res.size() < cnt, false);
        promise.set_value(std::move(x));
      });
  auto block_id = create_block_id(query.block_);
  VLOG(FULL_NODE_DEBUG) << "Got query getNextKeyBlockIds " << block_id.to_str() << " " << cnt << " from " << src;
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_next_key_blocks, block_id, cnt,
                          std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_downloadZeroState &query,
                                      td::Promise<td::BufferSlice> promise) {
  auto P = td::PromiseCreator::lambda(
      [SelfId = actor_id(this), promise = std::move(promise)](td::Result<td::BufferSlice> R) mutable {
        if (R.is_error()) {
          promise.set_error(R.move_as_error_prefix("failed to get state from db: "));
          return;
        }

        promise.set_value(R.move_as_ok());
      });
  auto block_id = create_block_id(query.block_);
  VLOG(FULL_NODE_DEBUG) << "Got query downloadZeroState " << block_id.to_str() << " from " << src;
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_zero_state, block_id, std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_getCapabilities &query,
                                      td::Promise<td::BufferSlice> promise) {
  VLOG(FULL_NODE_DEBUG) << "Got query getCapabilities from " << src;
  promise.set_value(
      create_serialize_tl_object<ton_api::tonNode_capabilities>(proto_version_major(), proto_version_minor(), 0));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_getArchiveInfo &query,
                                      td::Promise<td::BufferSlice> promise) {
  auto P = td::PromiseCreator::lambda(
      [SelfId = actor_id(this), promise = std::move(promise)](td::Result<td::uint64> R) mutable {
        if (R.is_error()) {
          promise.set_value(create_serialize_tl_object<ton_api::tonNode_archiveNotFound>());
        } else {
          promise.set_value(create_serialize_tl_object<ton_api::tonNode_archiveInfo>(R.move_as_ok()));
        }
      });
  VLOG(FULL_NODE_DEBUG) << "Got query getArchiveInfo " << query.masterchain_seqno_ << " from " << src;
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_archive_id, query.masterchain_seqno_,
                          ShardIdFull{masterchainId}, std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_getShardArchiveInfo &query,
                                      td::Promise<td::BufferSlice> promise) {
  auto P = td::PromiseCreator::lambda(
      [SelfId = actor_id(this), promise = std::move(promise)](td::Result<td::uint64> R) mutable {
        if (R.is_error()) {
          promise.set_value(create_serialize_tl_object<ton_api::tonNode_archiveNotFound>());
        } else {
          promise.set_value(create_serialize_tl_object<ton_api::tonNode_archiveInfo>(R.move_as_ok()));
        }
      });
  ShardIdFull shard_prefix = create_shard_id(query.shard_prefix_);
  VLOG(FULL_NODE_DEBUG) << "Got query getShardArchiveInfo " << query.masterchain_seqno_ << " " << shard_prefix.to_str()
                        << " from " << src;
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_archive_id, query.masterchain_seqno_,
                          shard_prefix, std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_getArchiveSlice &query,
                                      td::Promise<td::BufferSlice> promise) {
  VLOG(FULL_NODE_DEBUG) << "Got query getArchiveSlice " << query.archive_id_ << " " << query.offset_ << " "
                        << query.max_size_ << " from " << src;
  if (query.max_size_ < 0 || query.max_size_ > (1 << 24)) {
    promise.set_error(td::Status::Error(ErrorCode::protoviolation, "invalid max_size"));
    return;
  }
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_archive_slice, query.archive_id_,
                          query.offset_, query.max_size_, std::move(promise));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_getOutMsgQueueProof &query,
                                      td::Promise<td::BufferSlice> promise) {
  promise.set_error(td::Status::Error("not supported yet"));
  /*std::vector<BlockIdExt> blocks;
  for (const auto &x : query.blocks_) {
    BlockIdExt id = create_block_id(x);
    if (!id.is_valid_ext()) {
      promise.set_error(td::Status::Error("invalid block_id"));
      return;
    }
    if (!shard_is_ancestor(shard_, id.shard_full())) {
      promise.set_error(td::Status::Error("query in wrong overlay"));
      return;
    }
    blocks.push_back(create_block_id(x));
  }
  ShardIdFull dst_shard = create_shard_id(query.dst_shard_);
  if (!dst_shard.is_valid_ext()) {
    promise.set_error(td::Status::Error("invalid shard"));
    return;
  }
  block::ImportedMsgQueueLimits limits{(td::uint32)query.limits_->max_bytes_, (td::uint32)query.limits_->max_msgs_};
  FLOG(DEBUG) {
    sb << "Got query getOutMsgQueueProof to shard " << dst_shard.to_str() << " from blocks";
    for (const BlockIdExt &id : blocks) {
      sb << " " << id.id.to_str();
    }
    sb << " from " << src;
  };
  td::actor::send_closure(
      full_node_, &FullNode::get_out_msg_queue_query_token,
      [=, manager = validator_manager_, blocks = std::move(blocks),
       promise = std::move(promise)](td::Result<std::unique_ptr<ActionToken>> R) mutable {
        TRY_RESULT_PROMISE(promise, token, std::move(R));
        auto P =
            td::PromiseCreator::lambda([promise = std::move(promise), token = std::move(token)](
                                           td::Result<tl_object_ptr<ton_api::tonNode_outMsgQueueProof>> R) mutable {
              if (R.is_error()) {
                promise.set_result(create_serialize_tl_object<ton_api::tonNode_outMsgQueueProofEmpty>());
              } else {
                promise.set_result(serialize_tl_object(R.move_as_ok(), true));
              }
            });
        td::actor::create_actor<BuildOutMsgQueueProof>("buildqueueproof", dst_shard, std::move(blocks), limits, manager,
                                                       std::move(P))
            .release();
      });*/
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_downloadPersistentStateSliceV2 &query,
                                      td::Promise<td::BufferSlice> promise) {
  auto [block_id, mc_block_id, state_type] = persistent_state_from_v2_query(query);
  VLOG(FULL_NODE_DEBUG) << "Got query downloadPersistentStateSlice " << block_id.to_str() << " " << mc_block_id.to_str()
                        << " (" << persistent_state_type_to_string(block_id.shard_full(), state_type) << ") "
                        << query.offset_ << " " << query.max_size_ << " from " << src;
  if (query.max_size_ < 0 || query.max_size_ > (1 << 24)) {
    promise.set_error(td::Status::Error(ErrorCode::protoviolation, "invalid max_size"));
    return;
  }
  auto P = td::PromiseCreator::lambda(
      [SelfId = actor_id(this), promise = std::move(promise)](td::Result<td::BufferSlice> R) mutable {
        if (R.is_error()) {
          promise.set_error(R.move_as_error_prefix("failed to get state from db: "));
          return;
        }

        promise.set_value(R.move_as_ok());
      });
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_persistent_state_slice, block_id,
                          mc_block_id, state_type, query.offset_, query.max_size_, std::move(P));
}

void FullNodeShardImpl::process_query(adnl::AdnlNodeIdShort src, ton_api::tonNode_getPersistentStateSizeV2 &query,
                                      td::Promise<td::BufferSlice> promise) {
  auto P = td::PromiseCreator::lambda(
      [SelfId = actor_id(this), promise = std::move(promise)](td::Result<td::uint64> R) mutable {
        if (R.is_error()) {
          promise.set_value(create_serialize_tl_object<ton_api::tonNode_persistentStateSizeNotFound>());
        } else {
          promise.set_value(create_serialize_tl_object<ton_api::tonNode_persistentStateSize>(R.move_as_ok()));
        }
      });
  auto [block_id, mc_block_id, state_type] = persistent_state_from_v2_query(query);
  VLOG(FULL_NODE_DEBUG) << "Got query getPersistentStateSize " << block_id.to_str() << " " << mc_block_id.to_str()
                        << " (" << persistent_state_type_to_string(block_id.shard_full(), state_type) << ") from "
                        << src;
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_persistent_state_size, block_id,
                          mc_block_id, state_type, std::move(P));
}

void FullNodeShardImpl::receive_query(adnl::AdnlNodeIdShort src, td::BufferSlice query,
                                      td::Promise<td::BufferSlice> promise) {
  if (!active_) {
    td::actor::send_closure(overlays_, &overlay::Overlays::send_message, src, adnl_id_, overlay_id_,
                            create_serialize_tl_object<ton_api::tonNode_forgetPeer>());
    promise.set_error(td::Status::Error("shard is inactive"));
    return;
  }
  auto B = fetch_tl_object<ton_api::Function>(std::move(query), true);
  if (B.is_error()) {
    promise.set_error(td::Status::Error(ErrorCode::protoviolation, "cannot parse tonnode query"));
    return;
  }
  auto fun_ptr = B.move_as_ok();
  if (!limiter_->check_in(fun_ptr->get_id(), request_cost_for_limiter(*fun_ptr))) {
    promise.set_error(td::Status::Error(ErrorCode::failure, "too many requests"));
    return;
  }
  ton_api::downcast_call(*fun_ptr.get(), [&](auto &obj) { this->process_query(src, obj, std::move(promise)); });
}

void FullNodeShardImpl::receive_message(adnl::AdnlNodeIdShort src, td::BufferSlice data) {
  auto B = fetch_tl_object<ton_api::tonNode_forgetPeer>(std::move(data), true);
  if (B.is_error()) {
    return;
  }
  VLOG(FULL_NODE_DEBUG) << "Got tonNode.forgetPeer from " << src;
  neighbours_.erase(src);
  td::actor::send_closure(overlays_, &overlay::Overlays::forget_peer, adnl_id_, overlay_id_, src);
}

void FullNodeShardImpl::process_broadcast(PublicKeyHash src, ton_api::tonNode_ihrMessageBroadcast &query) {
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::new_ihr_message,
                          std::move(query.message_->data_));
}

void FullNodeShardImpl::process_broadcast(PublicKeyHash src, ton_api::tonNode_externalMessageBroadcast &query) {
  process_external_message_broadcast(query, [](td::Result<td::Unit>) {});
}

void FullNodeShardImpl::process_broadcast(PublicKeyHash src, ton_api::tonNode_newShardBlockBroadcast &query) {
  BlockIdExt block_id = create_block_id(query.block_->block_);
  VLOG(FULL_NODE_DEBUG) << "Received newShardBlockBroadcast from " << src << ": " << block_id.to_str();
  td::actor::send_closure(full_node_, &FullNode::process_shard_block_info_broadcast, block_id, query.block_->cc_seqno_,
                          std::move(query.block_->data_));
}

void FullNodeShardImpl::process_broadcast(PublicKeyHash src, ton_api::tonNode_newBlockCandidateBroadcast &query) {
  process_block_candidate_broadcast(src, query);
}

void FullNodeShardImpl::process_broadcast(PublicKeyHash src,
                                          ton_api::tonNode_newBlockCandidateBroadcastCompressed &query) {
  process_block_candidate_broadcast(src, query);
}

void FullNodeShardImpl::process_broadcast(PublicKeyHash src,
                                          ton_api::tonNode_newBlockCandidateBroadcastCompressedV2 &query) {
  process_block_candidate_broadcast(src, query);
}

void FullNodeShardImpl::process_block_candidate_broadcast(PublicKeyHash src, ton_api::tonNode_Broadcast &query) {
  BlockIdExt block_id;
  CatchainSeqno cc_seqno;
  td::uint32 validator_set_hash;
  td::BufferSlice data;
  auto S = deserialize_block_candidate_broadcast(query, block_id, cc_seqno, validator_set_hash, data,
                                                 overlay::Overlays::max_fec_broadcast_size(), k_called_from_public);
  if (S.is_error()) {
    VLOG(FULL_NODE_WARNING) << "received bad block candidate from " << src << " : " << S;
    return;
  }
  if (data.size() > FullNode::max_block_size()) {
    VLOG(FULL_NODE_WARNING) << "received block candidate with too big size from " << src;
    return;
  }
  if (td::sha256_bits256(data.as_slice()) != block_id.file_hash) {
    VLOG(FULL_NODE_WARNING) << "received block candidate with incorrect file hash from " << src;
    return;
  }
  VLOG(FULL_NODE_DEBUG) << "Received newBlockCandidate from " << src << ": " << block_id.to_str();
  td::actor::send_closure(full_node_, &FullNode::process_block_candidate_broadcast, block_id, cc_seqno,
                          validator_set_hash, std::move(data));
}

void FullNodeShardImpl::process_broadcast(PublicKeyHash src, ton_api::tonNode_blockBroadcast &query) {
  process_block_broadcast(src, query);
}

void FullNodeShardImpl::process_broadcast(PublicKeyHash src, ton_api::tonNode_blockBroadcastCompressed &query) {
  process_block_broadcast(src, query);
}

void FullNodeShardImpl::process_broadcast(PublicKeyHash src, ton_api::tonNode_blockBroadcastCompressedV2 &query) {
  auto R_requires_state = need_state_for_decompression(query);
  if (R_requires_state.is_error()) {
    LOG(DEBUG) << "Failed to check if state is required for broadcast: " << R_requires_state.move_as_error();
    return;
  }

  if (R_requires_state.move_as_ok()) {
    auto block_wo_data = get_block_broadcast_without_data(query);
    auto P = td::PromiseCreator::lambda(
        [SelfId = actor_id(this), src, query = std::move(query)](td::Result<td::Unit> R) mutable {
          if (R.is_error()) {
            LOG(DEBUG) << "Dropped V2 broadcast because of signatures validation error: " << R.move_as_error();
            return;
          }

          td::actor::send_closure(SelfId, &FullNodeShardImpl::obtain_state_for_decompression, src, std::move(query));
        });
    td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::validate_block_broadcast_signatures,
                            std::move(block_wo_data), std::move(P));
    return;
  }

  process_block_broadcast(src, query);
}

void FullNodeShardImpl::process_block_broadcast(PublicKeyHash src, ton_api::tonNode_Broadcast &query) {
  auto B = deserialize_block_broadcast(query, overlay::Overlays::max_fec_broadcast_size(), k_called_from_public);
  if (B.is_error()) {
    LOG(DEBUG) << "Failed to deserialize block broadcast: " << B.move_as_error();
    return;
  }
  //if (!shard_is_ancestor(shard_, block_id.shard_full())) {
  //  LOG(FULL_NODE_WARNING) << "dropping block broadcast: shard mismatch. overlay=" << shard_.to_str()
  //                         << " block=" << block_id.to_str();
  //  return;
  //}
  VLOG(FULL_NODE_DEBUG) << "Received block broadcast " << (B.ok().sig_set->is_final() ? "" : "(approve signatures) ")
                        << "from " << src << ": " << B.ok().block_id.to_str();
  td::actor::send_closure(full_node_, &FullNode::process_block_broadcast, B.move_as_ok(), false);
}

void FullNodeShardImpl::obtain_state_for_decompression(PublicKeyHash src,
                                                       ton_api::tonNode_blockBroadcastCompressedV2 query) {
  auto id = create_block_id(query.id_);
  auto R_prev = extract_prev_blocks_from_proof(query.proof_.as_slice(), id);
  if (R_prev.is_error()) {
    LOG(DEBUG) << "Failed to extract prev blocks for V2 broadcast: " << R_prev.move_as_error();
    return;
  }
  auto prev_blocks = R_prev.move_as_ok();
  auto P_state = td::PromiseCreator::lambda(
      [SelfId = actor_id(this), src, query = std::move(query)](td::Result<td::Ref<ShardState>> R_state) mutable {
        if (R_state.is_error()) {
          LOG(DEBUG) << "Failed to get state for V2 broadcast: " << R_state.move_as_error();
          return;
        }
        td::actor::send_closure(SelfId, &FullNodeShardImpl::process_block_broadcast_with_state, src, std::move(query),
                                R_state.move_as_ok());
      });
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::wait_state_by_prev_blocks, id,
                          std::move(prev_blocks), std::move(P_state));
}

void FullNodeShardImpl::process_block_broadcast_with_state(PublicKeyHash src,
                                                           ton_api::tonNode_blockBroadcastCompressedV2 query,
                                                           td::Ref<ShardState> state) {
  td::Ref<vm::Cell> state_root = state->root_cell();
  auto B =
      deserialize_block_broadcast(query, overlay::Overlays::max_fec_broadcast_size(), k_called_from_public, state_root);
  if (B.is_error()) {
    LOG(DEBUG) << "Failed to deserialize block broadcast: " << B.move_as_error();
    return;
  }
  VLOG(FULL_NODE_DEBUG) << "Received block broadcast from " << src << ": " << B.ok().block_id.to_str();
  td::actor::send_closure(full_node_, &FullNode::process_block_broadcast, B.move_as_ok(), true);
}

void FullNodeShardImpl::receive_broadcast(PublicKeyHash src, td::BufferSlice broadcast) {
  if (!active_) {
    return;
  }
  auto B = fetch_tl_object<ton_api::tonNode_Broadcast>(std::move(broadcast), true);
  if (B.is_error()) {
    return;
  }

  ton_api::downcast_call(*B.move_as_ok().get(), [src, Self = this](auto &obj) { Self->process_broadcast(src, obj); });
}

void FullNodeShardImpl::send_ihr_message(td::BufferSlice data) {
  if (!client_.empty()) {
    UNREACHABLE();
    return;
  }
  auto B = create_serialize_tl_object<ton_api::tonNode_ihrMessageBroadcast>(
      create_tl_object<ton_api::tonNode_ihrMessage>(std::move(data)));
  if (B.size() <= overlay::Overlays::max_simple_broadcast_size()) {
    td::actor::send_closure(overlays_, &overlay::Overlays::send_broadcast_ex, adnl_id_, overlay_id_, local_id_, 0,
                            std::move(B));
  } else {
    td::actor::send_closure(overlays_, &overlay::Overlays::send_broadcast_fec_ex, adnl_id_, overlay_id_, local_id_, 0,
                            std::move(B));
  }
}

void FullNodeShardImpl::send_external_message(td::BufferSlice data) {
  if (opts_.config_.ext_messages_broadcast_disabled_) {
    return;
  }
  if (!client_.empty()) {
    td::actor::send_closure(client_, &adnl::AdnlExtClient::send_query, "send_ext_query",
                            create_serialize_tl_object_suffix<ton_api::tonNode_query>(
                                create_serialize_tl_object<ton_api::tonNode_slave_sendExtMessage>(
                                    create_tl_object<ton_api::tonNode_externalMessage>(std::move(data)))),
                            td::Timestamp::in(1.0), [](td::Result<td::BufferSlice> R) {
                              if (R.is_error()) {
                                VLOG(FULL_NODE_WARNING) << "failed to send ext message: " << R.move_as_error();
                              }
                            });
    return;
  }
  td::Bits256 hash = td::sha256_bits256(data);
  if (processed_ext_msg_broadcasts_.count(hash)) {
    return;
  }
  my_ext_msg_broadcasts_.insert(hash);
  auto B = create_serialize_tl_object<ton_api::tonNode_externalMessageBroadcast>(
      create_tl_object<ton_api::tonNode_externalMessage>(std::move(data)));
  if (B.size() <= overlay::Overlays::max_simple_broadcast_size()) {
    td::actor::send_closure(overlays_, &overlay::Overlays::send_broadcast_ex, adnl_id_, overlay_id_, local_id_, 0,
                            std::move(B));
  } else {
    td::actor::send_closure(overlays_, &overlay::Overlays::send_broadcast_fec_ex, adnl_id_, overlay_id_, local_id_, 0,
                            std::move(B));
  }
}

void FullNodeShardImpl::send_shard_block_info(BlockIdExt block_id, CatchainSeqno cc_seqno, td::BufferSlice data) {
  if (!client_.empty()) {
    UNREACHABLE();
    return;
  }
  VLOG(FULL_NODE_DEBUG) << "Sending newShardBlockBroadcast: " << block_id.to_str();
  auto B = create_serialize_tl_object<ton_api::tonNode_newShardBlockBroadcast>(
      create_tl_object<ton_api::tonNode_newShardBlock>(create_tl_block_id(block_id), cc_seqno, std::move(data)));
  if (B.size() <= overlay::Overlays::max_simple_broadcast_size()) {
    td::actor::send_closure(overlays_, &overlay::Overlays::send_broadcast_ex, adnl_id_, overlay_id_, local_id_, 0,
                            std::move(B));
  } else {
    td::actor::send_closure(overlays_, &overlay::Overlays::send_broadcast_fec_ex, adnl_id_, overlay_id_, local_id_,
                            overlay::Overlays::BroadcastFlagAnySender(), std::move(B));
  }
}

void FullNodeShardImpl::send_block_candidate(BlockIdExt block_id, CatchainSeqno cc_seqno, td::uint32 validator_set_hash,
                                             td::BufferSlice data) {
  send_block_candidate_with_fanout(block_id, cc_seqno, validator_set_hash, std::move(data), 0);
}

void FullNodeShardImpl::send_block_candidate_with_fanout(BlockIdExt block_id, CatchainSeqno cc_seqno,
                                                         td::uint32 validator_set_hash, td::BufferSlice data,
                                                         td::uint32 fanout_override) {
  if (!client_.empty()) {
    UNREACHABLE();
    return;
  }
  auto B = serialize_block_candidate_broadcast(block_id, cc_seqno, validator_set_hash, data, true,
                                               k_called_from_public);  // compression enabled
  if (B.is_error()) {
    VLOG(FULL_NODE_WARNING) << "failed to serialize block candidate broadcast: " << B.move_as_error();
    return;
  }
  VLOG(FULL_NODE_DEBUG) << "Sending newBlockCandidate: " << block_id.to_str();
  auto payload = B.move_as_ok();
  auto payload_size = payload.size();
  if (fanout_override == 0) {
    td::actor::send_closure(overlays_, &overlay::Overlays::send_broadcast_fec_ex, adnl_id_, overlay_id_, local_id_,
                            overlay::Overlays::BroadcastFlagAnySender(), std::move(payload));
  } else {
    auto force_good_available = force_good_peers_.size();
    auto force_peers = choose_force_good_peers(fanout_override);
    auto force_good_selected = force_peers.size();
    auto discovered_peer_target = fanout_override > force_good_selected ? fanout_override - force_good_selected : 0;
    LOG(INFO) << "public rebroadcast dispatch type=candidate block=" << block_id.to_str()
              << " shard=" << shard_.to_str() << " fanout_target=" << fanout_override
              << " force_good_available=" << force_good_available << " force_good_selected=" << force_good_selected
              << " discovered_peer_target=" << discovered_peer_target << " payload_bytes=" << payload_size;
    td::actor::send_closure(overlays_, &overlay::Overlays::send_broadcast_fec_ex_with_fanout, adnl_id_, overlay_id_,
                            local_id_, overlay::Overlays::BroadcastFlagAnySender(), std::move(payload),
                            fanout_override, std::move(force_peers));
  }
}

void FullNodeShardImpl::send_broadcast(BlockBroadcast broadcast) {
  send_broadcast_with_fanout(std::move(broadcast), 0);
}

void FullNodeShardImpl::send_broadcast_with_fanout(BlockBroadcast broadcast, td::uint32 fanout_override) {
  if (!client_.empty()) {
    UNREACHABLE();
    return;
  }
  VLOG(FULL_NODE_DEBUG) << "Sending block broadcast in private overlay: " << broadcast.block_id.to_str();
  auto B = serialize_block_broadcast(broadcast, k_called_from_public);
  if (B.is_error()) {
    VLOG(FULL_NODE_WARNING) << "failed to serialize block broadcast: " << B.move_as_error();
    return;
  }
  auto payload = B.move_as_ok();
  auto payload_size = payload.size();
  if (fanout_override == 0) {
    td::actor::send_closure(overlays_, &overlay::Overlays::send_broadcast_fec_ex, adnl_id_, overlay_id_, local_id_,
                            overlay::Overlays::BroadcastFlagAnySender(), std::move(payload));
  } else {
    auto force_good_available = force_good_peers_.size();
    auto force_peers = choose_force_good_peers(fanout_override);
    auto force_good_selected = force_peers.size();
    auto discovered_peer_target = fanout_override > force_good_selected ? fanout_override - force_good_selected : 0;
    LOG(INFO) << "public rebroadcast dispatch type=block block=" << broadcast.block_id.to_str()
              << " shard=" << shard_.to_str() << " fanout_target=" << fanout_override
              << " force_good_available=" << force_good_available << " force_good_selected=" << force_good_selected
              << " discovered_peer_target=" << discovered_peer_target << " payload_bytes=" << payload_size;
    td::actor::send_closure(overlays_, &overlay::Overlays::send_broadcast_fec_ex_with_fanout, adnl_id_, overlay_id_,
                            local_id_, overlay::Overlays::BroadcastFlagAnySender(), std::move(payload),
                            fanout_override, std::move(force_peers));
  }
}

void FullNodeShardImpl::download_block(BlockIdExt id, td::uint32 priority, td::Timestamp timeout,
                                       td::Promise<ReceivedBlock> promise) {
  td::Promise<DownloadedBlock> P = td::PromiseCreator::lambda(
      [SelfId = actor_id(this), promise = std::move(promise)](td::Result<DownloadedBlock> R) mutable {
        if (R.is_error()) {
          promise.set_error(R.move_as_error());
        } else {
          td::actor::send_closure(SelfId, &FullNodeShardImpl::finish_download_block, R.move_as_ok(),
                                  std::move(promise));
        }
      });
  if (uses_force_download_peers()) {
    auto force_peers = choose_force_download_peers();
    LOG(INFO) << "forced block download start mode=block block=" << id.to_str()
              << " selected_peers=" << force_peers.size()
              << " configured_peers=" << opts_.force_download_.peers_.size()
              << " attempts_target=" << opts_.force_download_.attempts_num_;
    td::actor::create_actor<DownloadBlockNewParallel>("downloadreq-forced", id, adnl_id_, overlay_id_,
                                                      std::move(force_peers), priority, timeout,
                                                      validator_manager_, rldp_, overlays_, adnl_, client_,
                                                      std::move(P))
        .release();
    return;
  }

  auto &b = choose_neighbour();
  td::actor::create_actor<DownloadBlockNew>("downloadreq", id, adnl_id_, overlay_id_, b.adnl_id, priority, timeout,
                                            validator_manager_, rldp_, overlays_, adnl_, client_,
                                            create_neighbour_promise(b, std::move(P)))
      .release();
}

void FullNodeShardImpl::download_zero_state(BlockIdExt id, td::uint32 priority, td::Timestamp timeout,
                                            td::Promise<td::BufferSlice> promise) {
  td::actor::create_actor<DownloadState>(PSTRING() << "downloadstatereq" << id.id.to_str(), id, BlockIdExt{},
                                         UnsplitStateType{}, adnl_id_, overlay_id_, adnl::AdnlNodeIdShort::zero(),
                                         priority, timeout, validator_manager_, rldp_, overlays_, adnl_, client_,
                                         std::move(promise))
      .release();
}

void FullNodeShardImpl::download_persistent_state(BlockIdExt id, BlockIdExt masterchain_block_id,
                                                  PersistentStateType type, td::uint32 priority, td::Timestamp timeout,
                                                  td::Promise<td::BufferSlice> promise) {
  auto &b = choose_neighbour();
  td::actor::create_actor<DownloadState>(PSTRING() << "downloadstatereq" << id.id.to_str(), id, masterchain_block_id,
                                         type, adnl_id_, overlay_id_, b.adnl_id, priority, timeout, validator_manager_,
                                         rldp2_, overlays_, adnl_, client_, std::move(promise))
      .release();
}

void FullNodeShardImpl::download_block_proof(BlockIdExt block_id, td::uint32 priority, td::Timestamp timeout,
                                             td::Promise<td::BufferSlice> promise) {
  auto &b = choose_neighbour();
  td::actor::create_actor<DownloadProof>("downloadproofreq", block_id, false, false, adnl_id_, overlay_id_, b.adnl_id,
                                         priority, timeout, validator_manager_, rldp_, overlays_, adnl_, client_,
                                         create_neighbour_promise(b, std::move(promise)))
      .release();
}

void FullNodeShardImpl::download_block_proof_link(BlockIdExt block_id, td::uint32 priority, td::Timestamp timeout,
                                                  td::Promise<td::BufferSlice> promise) {
  auto &b = choose_neighbour();
  td::actor::create_actor<DownloadProof>("downloadproofreq", block_id, true, false, adnl_id_, overlay_id_, b.adnl_id,
                                         priority, timeout, validator_manager_, rldp_, overlays_, adnl_, client_,
                                         create_neighbour_promise(b, std::move(promise)))
      .release();
}

void FullNodeShardImpl::get_next_key_blocks(BlockIdExt block_id, td::Timestamp timeout,
                                            td::Promise<std::vector<BlockIdExt>> promise) {
  auto &b = choose_neighbour();
  td::actor::create_actor<GetNextKeyBlocks>("next", block_id, 16, adnl_id_, overlay_id_, b.adnl_id, 1, timeout,
                                            validator_manager_, rldp_, overlays_, adnl_, client_,
                                            create_neighbour_promise(b, std::move(promise)))
      .release();
}

void FullNodeShardImpl::download_archive(BlockSeqno masterchain_seqno, ShardIdFull shard_prefix, std::string tmp_dir,
                                         td::Timestamp timeout, td::Promise<std::string> promise) {
  auto &b = choose_neighbour();
  td::actor::create_actor<DownloadArchiveSlice>(
      "archive", masterchain_seqno, shard_prefix, std::move(tmp_dir), adnl_id_, overlay_id_, b.adnl_id, timeout,
      validator_manager_, rldp2_, overlays_, adnl_, client_, create_neighbour_promise(b, std::move(promise)))
      .release();
}

void FullNodeShardImpl::download_out_msg_queue_proof(ShardIdFull dst_shard, std::vector<BlockIdExt> blocks,
                                                     block::ImportedMsgQueueLimits limits, td::Timestamp timeout,
                                                     td::Promise<std::vector<td::Ref<OutMsgQueueProof>>> promise) {
  // TODO: maybe more complex download (like other requests here)
  auto &b = choose_neighbour(3, 0);  // Required version: 3.0
  if (b.adnl_id == adnl::AdnlNodeIdShort::zero()) {
    promise.set_error(td::Status::Error(ErrorCode::notready, "no nodes"));
    return;
  }
  std::vector<tl_object_ptr<ton_api::tonNode_blockIdExt>> blocks_tl;
  for (const BlockIdExt &id : blocks) {
    blocks_tl.push_back(create_tl_block_id(id));
  }
  td::BufferSlice query = create_serialize_tl_object<ton_api::tonNode_getOutMsgQueueProof>(
      create_tl_shard_id(dst_shard), std::move(blocks_tl),
      create_tl_object<ton_api::tonNode_importedMsgQueueLimits>(limits.max_bytes, limits.max_msgs));

  auto P = td::PromiseCreator::lambda(
      [=, promise = std::move(promise), blocks = std::move(blocks)](td::Result<td::BufferSlice> R) mutable {
        if (R.is_error()) {
          promise.set_result(R.move_as_error());
          return;
        }
        TRY_RESULT_PROMISE(promise, f, fetch_tl_object<ton_api::tonNode_OutMsgQueueProof>(R.move_as_ok(), true));
        ton_api::downcast_call(
            *f, td::overloaded(
                    [&](ton_api::tonNode_outMsgQueueProofEmpty &x) {
                      promise.set_error(td::Status::Error("node doesn't have this block"));
                    },
                    [&](ton_api::tonNode_outMsgQueueProof &x) {
                      delay_action(
                          [=, promise = std::move(promise), blocks = std::move(blocks), x = std::move(x)]() mutable {
                            promise.set_result(OutMsgQueueProof::fetch(dst_shard, blocks, limits, x));
                          },
                          td::Timestamp::now());
                    }));
      });
  td::actor::send_closure(overlays_, &overlay::Overlays::send_query_via, b.adnl_id, adnl_id_, overlay_id_,
                          "get_msg_queue", std::move(P), timeout, std::move(query), 1 << 22, rldp_);
}

void FullNodeShardImpl::set_handle(BlockHandle handle, td::Promise<td::Unit> promise) {
  CHECK(!handle_);
  handle_ = std::move(handle);
  promise_ = std::move(promise);
  get_next_block();

  sync_completed_at_ = td::Timestamp::in(opts_.initial_sync_delay_);
  alarm_timestamp().relax(sync_completed_at_);
}

void FullNodeShardImpl::alarm() {
  if (sync_completed_at_ && sync_completed_at_.is_in_past()) {
    if (promise_) {
      promise_.set_value(td::Unit());
    }
    sync_completed_at_ = td::Timestamp::never();
  }
  if (reload_neighbours_at_ && reload_neighbours_at_.is_in_past()) {
    reload_neighbours();
    reload_neighbours_at_ = td::Timestamp::in(td::Random::fast(10.0, 30.0));
  }
  if (ping_neighbours_at_ && ping_neighbours_at_.is_in_past()) {
    ping_neighbours();
    ping_neighbours_at_ = td::Timestamp::in(td::Random::fast(0.5, 1.0));
  }
  if (update_certificate_at_ && update_certificate_at_.is_in_past()) {
    if (!sign_cert_by_.is_zero()) {
      sign_new_certificate(sign_cert_by_);
      update_certificate_at_ = td::Timestamp::in(30.0);
    } else {
      update_certificate_at_ = td::Timestamp::never();
    }
  }
  if (cleanup_processed_ext_msg_at_ && cleanup_processed_ext_msg_at_.is_in_past()) {
    processed_ext_msg_broadcasts_.clear();
    my_ext_msg_broadcasts_.clear();
    cleanup_processed_ext_msg_at_ = td::Timestamp::in(60.0);
  }
  if (refresh_force_good_peers_at_ && refresh_force_good_peers_at_.is_in_past()) {
    refresh_force_good_peers();
  }
  alarm_timestamp().relax(sync_completed_at_);
  alarm_timestamp().relax(update_certificate_at_);
  alarm_timestamp().relax(reload_neighbours_at_);
  alarm_timestamp().relax(ping_neighbours_at_);
  alarm_timestamp().relax(cleanup_processed_ext_msg_at_);
  alarm_timestamp().relax(refresh_force_good_peers_at_);
}

void FullNodeShardImpl::start_up() {
  if (client_.empty()) {
    auto X = create_hash_tl_object<ton_api::tonNode_shardPublicOverlayId>(get_workchain(), get_shard(),
                                                                          zero_state_file_hash_);
    td::BufferSlice b{32};
    b.as_slice().copy_from(as_slice(X));
    overlay_id_full_ = overlay::OverlayIdFull{std::move(b)};
    overlay_id_ = overlay_id_full_.compute_short_id();
    rules_ = overlay::OverlayPrivacyRules{overlay::Overlays::max_fec_broadcast_size()};

    create_overlay();

    reload_neighbours_at_ = td::Timestamp::now();
    ping_neighbours_at_ = td::Timestamp::now();
    cleanup_processed_ext_msg_at_ = td::Timestamp::now();
    if (uses_force_good_peers()) {
      refresh_force_good_peers_at_ = td::Timestamp::now();
    }
    alarm_timestamp().relax(td::Timestamp::now());
  }
}

void FullNodeShardImpl::tear_down() {
  td::actor::send_closure(overlays_, &ton::overlay::Overlays::delete_overlay, adnl_id_, overlay_id_);
}

void FullNodeShardImpl::sign_new_certificate(PublicKeyHash sign_by) {
  if (sign_by.is_zero()) {
    return;
  }

  ton::overlay::Certificate cert{
      sign_by, static_cast<td::int32>(td::Clocks::system() + 3600), overlay::Overlays::max_fec_broadcast_size(),
      overlay::CertificateFlags::Trusted | overlay::CertificateFlags::AllowFec, td::BufferSlice{}};
  auto to_sign = cert.to_sign(overlay_id_, local_id_);

  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), cert = std::move(cert), local_id = local_id_](
                                          td::Result<std::pair<td::BufferSlice, PublicKey>> R) mutable {
    if (R.is_error()) {
      // ignore
      VLOG(FULL_NODE_WARNING) << "failed to create certificate: failed to sign: " << R.move_as_error();
    } else {
      auto p = R.move_as_ok();
      cert.set_signature(std::move(p.first));
      cert.set_issuer(p.second);
      td::actor::send_closure(SelfId, &FullNodeShardImpl::signed_new_certificate, std::move(cert), local_id);
    }
  });
  td::actor::send_closure(keyring_, &ton::keyring::Keyring::sign_add_get_public_key, sign_by, std::move(to_sign),
                          std::move(P));
}

void FullNodeShardImpl::signed_new_certificate(overlay::Certificate cert, PublicKeyHash local_id) {
  if (local_id != local_id_) {
    return;
  }
  LOG(WARNING) << "updated certificate";
  cert_ = std::make_shared<overlay::Certificate>(std::move(cert));
  td::actor::send_closure(overlays_, &overlay::Overlays::update_certificate, adnl_id_, overlay_id_, local_id_, cert_);
}

void FullNodeShardImpl::sign_overlay_certificate(PublicKeyHash signed_key, td::uint32 expire_at, td::uint32 max_size,
                                                 td::Promise<td::BufferSlice> promise) {
  auto sign_by = sign_cert_by_;
  if (sign_by.is_zero()) {
    promise.set_error(td::Status::Error("Node has no key with signing authority"));
    return;
  }

  ton::overlay::Certificate cert{sign_by, static_cast<td::int32>(expire_at), max_size,
                                 overlay::CertificateFlags::Trusted | overlay::CertificateFlags::AllowFec,
                                 td::BufferSlice{}};
  auto to_sign = cert.to_sign(overlay_id_, signed_key);

  auto P = td::PromiseCreator::lambda(
      [SelfId = actor_id(this), expire_at = expire_at, max_size = max_size,
       promise = std::move(promise)](td::Result<std::pair<td::BufferSlice, PublicKey>> R) mutable {
        if (R.is_error()) {
          promise.set_error(R.move_as_error_prefix("failed to create certificate: failed to sign: "));
        } else {
          auto p = R.move_as_ok();
          auto c = ton::create_serialize_tl_object<ton::ton_api::overlay_certificate>(
              p.second.tl(), static_cast<td::int32>(expire_at), max_size, std::move(p.first));
          promise.set_value(std::move(c));
        }
      });
  td::actor::send_closure(keyring_, &ton::keyring::Keyring::sign_add_get_public_key, sign_by, std::move(to_sign),
                          std::move(P));
}

void FullNodeShardImpl::import_overlay_certificate(PublicKeyHash signed_key,
                                                   std::shared_ptr<ton::overlay::Certificate> cert,
                                                   td::Promise<td::Unit> promise) {
  td::actor::send_closure(overlays_, &ton::overlay::Overlays::update_certificate, adnl_id_, overlay_id_, signed_key,
                          cert);
  promise.set_value(td::Unit());
}

void FullNodeShardImpl::update_validators(std::vector<PublicKeyHash> public_key_hashes, PublicKeyHash local_hash) {
  if (!client_.empty()) {
    return;
  }
  bool update_cert = false;
  if (!local_hash.is_zero() && local_hash != sign_cert_by_) {
    update_cert = true;
  }
  sign_cert_by_ = local_hash;

  std::map<PublicKeyHash, td::uint32> authorized_keys;
  for (auto &key : public_key_hashes) {
    authorized_keys.emplace(key, overlay::Overlays::max_fec_broadcast_size());
  }

  rules_ = overlay::OverlayPrivacyRules{overlay::Overlays::max_fec_broadcast_size(),
                                        overlay::CertificateFlags::AllowFec, std::move(authorized_keys)};
  td::actor::send_closure(overlays_, &overlay::Overlays::set_privacy_rules, adnl_id_, overlay_id_, rules_);

  if (update_cert) {
    sign_new_certificate(sign_cert_by_);
    update_certificate_at_ = td::Timestamp::in(30.0);
    alarm_timestamp().relax(update_certificate_at_);
  }
}

void FullNodeShardImpl::reload_neighbours() {
  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<std::vector<adnl::AdnlNodeIdShort>> R) {
    if (R.is_error()) {
      return;
    }
    auto vec = R.move_as_ok();
    if (vec.size() == 0) {
      return;
    } else {
      td::actor::send_closure(SelfId, &FullNodeShardImpl::got_neighbours, std::move(vec));
    }
  });
  td::actor::send_closure(overlays_, &overlay::Overlays::get_overlay_random_peers, adnl_id_, overlay_id_,
                          max_neighbours(), std::move(P));
}

void FullNodeShardImpl::got_neighbours(std::vector<adnl::AdnlNodeIdShort> vec) {
  bool ex = false;

  for (auto &el : vec) {
    auto it = neighbours_.find(el);
    if (it != neighbours_.end()) {
      continue;
    }
    if (neighbours_.size() == max_neighbours()) {
      adnl::AdnlNodeIdShort a = adnl::AdnlNodeIdShort::zero();
      adnl::AdnlNodeIdShort b = adnl::AdnlNodeIdShort::zero();
      td::uint32 cnt = 0;
      double u = 0;
      for (auto &n : neighbours_) {
        if (n.second.unreliability > u) {
          u = n.second.unreliability;
          a = n.first;
        }
        if (td::Random::fast(0, cnt++) == 0) {
          b = n.first;
        }
      }

      if (u > stop_unreliability()) {
        neighbours_.erase(a);
      } else {
        neighbours_.erase(b);
        ex = true;
      }
    }
    neighbours_.emplace(el, Neighbour{el});
    if (ex) {
      break;
    }
  }
}

const Neighbour &FullNodeShardImpl::choose_neighbour(td::uint32 required_version_major,
                                                     td::uint32 required_version_minor) const {
  if (neighbours_.size() == 0) {
    return Neighbour::zero;
  }
  auto is_eligible = [&](const Neighbour &n) {
    return n.version_major > required_version_major ||
           (n.version_major == required_version_major && n.version_minor >= required_version_minor);
  };

  double min_unreliability = 1e9;
  for (auto &[_, x] : neighbours_) {
    if (!is_eligible(x)) {
      continue;
    }
    min_unreliability = std::min(min_unreliability, x.unreliability);
  }
  const Neighbour *best = nullptr;
  td::uint32 sum = 0;

  for (auto &[_, x] : neighbours_) {
    if (!is_eligible(x)) {
      continue;
    }
    auto unr = static_cast<td::uint32>(x.unreliability - min_unreliability);

    if (x.version_major < proto_version_major()) {
      unr += 4;
    } else if (x.version_major == proto_version_major() && x.version_minor < proto_version_minor()) {
      unr += 2;
    }

    auto f = static_cast<td::uint32>(fail_unreliability());

    if (unr <= f) {
      auto w = 1 << (f - unr);
      sum += w;
      if (td::Random::fast(0, sum - 1) <= w - 1) {
        best = &x;
      }
    }
  }
  if (best) {
    return *best;
  }
  return Neighbour::zero;
}

void FullNodeShardImpl::update_neighbour_stats(adnl::AdnlNodeIdShort adnl_id, double t, bool success) {
  auto it = neighbours_.find(adnl_id);
  if (it != neighbours_.end()) {
    if (success) {
      it->second.query_success(t);
    } else {
      it->second.query_failed();
    }
  }
}

void FullNodeShardImpl::got_neighbour_capabilities(adnl::AdnlNodeIdShort adnl_id, double t, td::BufferSlice data) {
  auto it = neighbours_.find(adnl_id);
  if (it == neighbours_.end()) {
    return;
  }
  auto F = fetch_tl_object<ton_api::tonNode_capabilities>(std::move(data), true);
  if (F.is_error()) {
    it->second.query_failed();
  } else {
    it->second.update_proto_version(*F.ok());
    it->second.query_success(t);
  }
}

void FullNodeShardImpl::ping_neighbours() {
  if (neighbours_.size() == 0) {
    return;
  }
  td::uint32 max_cnt = 6;
  if (max_cnt > neighbours_.size()) {
    max_cnt = td::narrow_cast<td::uint32>(neighbours_.size());
  }
  auto it = neighbours_.lower_bound(last_pinged_neighbour_);
  while (max_cnt > 0) {
    if (it == neighbours_.end()) {
      it = neighbours_.begin();
    }

    auto P = td::PromiseCreator::lambda(
        [SelfId = actor_id(this), start_time = td::Time::now(), id = it->first](td::Result<td::BufferSlice> R) {
          if (R.is_error()) {
            td::actor::send_closure(SelfId, &FullNodeShardImpl::update_neighbour_stats, id,
                                    td::Time::now() - start_time, false);
          } else {
            td::actor::send_closure(SelfId, &FullNodeShardImpl::got_neighbour_capabilities, id,
                                    td::Time::now() - start_time, R.move_as_ok());
          }
        });
    td::BufferSlice q = create_serialize_tl_object<ton_api::tonNode_getCapabilities>();
    td::actor::send_closure(overlays_, &overlay::Overlays::send_query, it->first, adnl_id_, overlay_id_,
                            "get_prepare_block", std::move(P), td::Timestamp::in(1.0), std::move(q));

    last_pinged_neighbour_ = it->first;
    it++;
    max_cnt--;
  }
}

void FullNodeShardImpl::get_stats_extra(td::Promise<std::string> promise) {
  auto res = create_tl_object<ton_api::engine_validator_shardOverlayStats>();
  res->shard_ = shard_.to_str();
  res->active_ = active_;
  for (const auto &p : neighbours_) {
    const auto &n = p.second;
    auto f = create_tl_object<ton_api::engine_validator_shardOverlayStats_neighbour>();
    f->id_ = n.adnl_id.bits256_value().to_hex();
    f->verison_major_ = n.version_major;
    f->version_minor_ = n.version_minor;
    f->flags_ = n.flags;
    f->roundtrip_ = n.roundtrip;
    f->unreliability_ = n.unreliability;
    res->neighbours_.push_back(std::move(f));
  }
  promise.set_result(td::json_encode<std::string>(td::ToJson(*res), true));
}

FullNodeShardImpl::FullNodeShardImpl(
    ShardIdFull shard, PublicKeyHash local_id, adnl::AdnlNodeIdShort adnl_id, FileHash zero_state_file_hash,
    FullNodeOptions opts, std::shared_ptr<RateLimiter<>> limiter, td::actor::ActorId<keyring::Keyring> keyring,
    td::actor::ActorId<adnl::Adnl> adnl, td::actor::ActorId<rldp::Rldp> rldp, td::actor::ActorId<rldp2::Rldp> rldp2,
    td::actor::ActorId<overlay::Overlays> overlays, td::actor::ActorId<ValidatorManagerInterface> validator_manager,
    td::actor::ActorId<adnl::AdnlExtClient> client, td::actor::ActorId<FullNode> full_node, bool active)
    : shard_(shard)
    , local_id_(local_id)
    , adnl_id_(adnl_id)
    , zero_state_file_hash_(zero_state_file_hash)
    , keyring_(keyring)
    , adnl_(adnl)
    , rldp_(rldp)
    , rldp2_(rldp2)
    , overlays_(overlays)
    , validator_manager_(validator_manager)
    , client_(client)
    , full_node_(full_node)
    , active_(active)
    , opts_(opts)
    , limiter_(std::move(limiter)) {
}

td::actor::ActorOwn<FullNodeShard> FullNodeShard::create(
    ShardIdFull shard, PublicKeyHash local_id, adnl::AdnlNodeIdShort adnl_id, FileHash zero_state_file_hash,
    FullNodeOptions opts, std::shared_ptr<RateLimiter<>> limiter, td::actor::ActorId<keyring::Keyring> keyring,
    td::actor::ActorId<adnl::Adnl> adnl, td::actor::ActorId<rldp::Rldp> rldp, td::actor::ActorId<rldp2::Rldp> rldp2,
    td::actor::ActorId<overlay::Overlays> overlays, td::actor::ActorId<ValidatorManagerInterface> validator_manager,
    td::actor::ActorId<adnl::AdnlExtClient> client, td::actor::ActorId<FullNode> full_node, bool active) {
  return td::actor::create_actor<FullNodeShardImpl>(PSTRING() << "tonnode" << shard.to_str(), shard, local_id, adnl_id,
                                                    zero_state_file_hash, opts, std::move(limiter), keyring, adnl, rldp,
                                                    rldp2, overlays, validator_manager, client, full_node, active);
}

}  // namespace fullnode

}  // namespace validator

}  // namespace ton
