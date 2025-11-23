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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "adnl/adnl.h"
#include "auto/tl/ton_api.h"
#include "td/actor/actor.h"
#include "td/fec/raptorq/Decoder.h"
#include "td/fec/raptorq/Encoder.h"
#include "td/utils/Random.h"

#include "overlays.h"
#include "twostep-overlay.hpp"

namespace ton {

namespace overlay {

static constexpr double TIMEOUT = 15.0;
static constexpr double TIMEOUT_CHECK_INTERVAL = 10.0;
static constexpr std::int32_t FLAG_REBROADCAST = 0x1;
static constexpr std::size_t FEC_MIN_BYTES = 513;
static constexpr std::size_t FEC_MIN_OTHER_NODES = 4;

TwostepOverlayImpl::TwostepOverlayImpl(adnl::AdnlNodeIdShort local_id, OverlayIdShort overlay_id,
                                       std::unique_ptr<Overlays::Callback> callback,
                                       td::actor::ActorId<Overlays> manager, td::actor::ActorId<rldp2::Rldp> rldp2,
                                       std::vector<adnl::AdnlNodeIdShort> nodes)
    : local_id_(local_id)
    , overlay_id_(overlay_id)
    , callback_(std::move(callback))
    , manager_(std::move(manager))
    , rldp2_(std::move(rldp2))
    , other_nodes_(std::move(nodes)) {
  other_nodes_.erase(std::remove(other_nodes_.begin(), other_nodes_.end(), local_id_), other_nodes_.end());
}

void TwostepOverlayImpl::alarm() {
  for (auto it = transfers_.begin(); it != transfers_.end();) {
    if (it->second.timeout_.is_in_past()) {
      it = transfers_.erase(it);
    } else {
      ++it;
    }
  }
  if (!transfers_.empty()) {
    alarm_timestamp() = td::Timestamp::in(TIMEOUT_CHECK_INTERVAL);
  }
}

void TwostepOverlayImpl::receive_message(adnl::AdnlNodeIdShort src, tl_object_ptr<ton_api::overlay_messageExtra> extra,
                                         td::BufferSlice data) {
  auto R = fetch_tl_object<ton_api::twostepOverlay_Broadcast>(data, true);
  if (R.is_error()) {
    callback_->receive_message(src, overlay_id_, std::move(data));
  } else {
    ton_api::downcast_call(
        *R.ok(),
        td::overloaded(
            [&](ton_api::twostepOverlay_broadcastSimple &obj) {
              if (obj.flags_ & FLAG_REBROADCAST) {
                td::BufferSlice newobj = create_serialize_tl_object<ton_api::twostepOverlay_broadcastSimple>(
                    obj.transfer_id_, 0, obj.data_.clone());
                for (auto &dst : other_nodes_) {
                  if (dst != src) {
                    td::actor::send_closure(manager_, &Overlays::send_message_via, dst, local_id_, overlay_id_,
                                            newobj.clone(), rldp2_);
                  }
                }
              }
              if (transfers_.empty()) {
                alarm_timestamp() = td::Timestamp::in(TIMEOUT_CHECK_INTERVAL);
              }
              if (transfers_.try_emplace(obj.transfer_id_).second) {
                deliver_broadcast(obj.data_);
              }
            },
            [&](ton_api::twostepOverlay_broadcastFec &obj) {
              if (obj.flags_ & FLAG_REBROADCAST) {
                obj.flags_ = 0;
                td::BufferSlice newobj = serialize_tl_object(&obj, true);
                for (auto &dst : other_nodes_) {
                  if (dst != src) {
                    td::actor::send_closure(manager_, &Overlays::send_message_via, dst, local_id_, overlay_id_,
                                            newobj.clone(), rldp2_);
                  }
                }
              }
              if (transfers_.empty()) {
                alarm_timestamp() = td::Timestamp::in(TIMEOUT_CHECK_INTERVAL);
              }
              auto [it, ins] = transfers_.try_emplace(obj.transfer_id_);
              auto tdata = &it->second;
              if (ins) {
                std::size_t data_size = static_cast<std::size_t>(obj.data_size_);
                std::size_t symbol_size = obj.symbol_.size();
                td::Result<std::unique_ptr<td::raptorq::Decoder>> R2;
                if (symbol_size == 0 || (R2 = td::raptorq::Decoder::create(
                                             {(data_size + symbol_size - 1) / symbol_size, symbol_size, data_size}))
                                            .is_error()) {
                  VLOG(OVERLAY_WARNING) << this << ": invalid FEC parameters";
                  transfers_.erase(it);
                  if (transfers_.empty()) {
                    alarm_timestamp() = td::Timestamp::never();
                  }
                  return;
                }
                tdata->decoder_ = R2.move_as_ok();
              } else if (!tdata->decoder_) {
                return;
              }
              td::Status S =
                  tdata->decoder_->add_symbol({static_cast<std::uint32_t>(obj.part_), obj.symbol_.as_slice()});
              if (S.is_error()) {
                VLOG(OVERLAY_WARNING) << this << ": invalid symbol: " << std::move(S);
                return;
              }
              if (!tdata->decoder_->may_try_decode()) {
                return;
              }
              auto R = tdata->decoder_->try_decode(false);
              if (R.is_error()) {
                VLOG(OVERLAY_WARNING) << this << ": decode_failed: " << R.move_as_error();
                return;
              }
              tdata->decoder_.reset();
              deliver_broadcast(R.ok().data);
            }));
  }
}

void TwostepOverlayImpl::receive_query(adnl::AdnlNodeIdShort src, tl_object_ptr<ton_api::overlay_messageExtra> extra,
                                       td::BufferSlice data, td::Promise<td::BufferSlice> promise) {
  callback_->receive_query(src, overlay_id_, std::move(data), std::move(promise));
}

void TwostepOverlayImpl::send_broadcast(PublicKeyHash send_as, td::uint32 flags, td::BufferSlice data) {
  TransferId transfer_id;
  td::Random::secure_bytes(transfer_id.as_slice());
  transfers_.try_emplace(transfer_id);
  td::BufferSlice inner =
      create_serialize_tl_object<ton_api::twostepOverlay_broadcastInner>(send_as.bits256_value(), std::move(data));
  if (inner.size() >= FEC_MIN_BYTES && other_nodes_.size() >= FEC_MIN_OTHER_NODES) {
    std::size_t data_size = inner.size();
    std::size_t k = (other_nodes_.size() * 2 - 2) / 3;
    std::size_t symbol_size = (data_size + k - 1) / k;
    auto R = td::raptorq::Encoder::create(symbol_size, std::move(inner));
    if (R.is_error()) {
      VLOG(OVERLAY_WARNING) << this << ": cannot create FEC encoder: " << R.move_as_error();
      return;
    }
    auto encoder = R.move_as_ok();
    encoder->precalc();
    for (std::size_t i = 0; i < other_nodes_.size(); i++) {
      td::BufferSlice symbol(symbol_size);
      auto S = encoder->gen_symbol(static_cast<std::uint32_t>(i), symbol.as_slice());
      if (S.is_error()) {
        VLOG(OVERLAY_WARNING) << this << ": cannot generate symbol: " << S;
        continue;
      }
      td::actor::send_closure(manager_, &Overlays::send_message_via, other_nodes_[i], local_id_, overlay_id_,
                              create_serialize_tl_object<ton_api::twostepOverlay_broadcastFec>(
                                  transfer_id, FLAG_REBROADCAST, static_cast<std::int32_t>(data_size),
                                  static_cast<std::int32_t>(i), std::move(symbol)),
                              rldp2_);
    }
  } else {
    td::BufferSlice obj = create_serialize_tl_object<ton_api::twostepOverlay_broadcastSimple>(
        transfer_id, FLAG_REBROADCAST, std::move(inner));
    for (auto &dst : other_nodes_) {
      td::actor::send_closure(manager_, &Overlays::send_message_via, dst, local_id_, overlay_id_, obj.clone(), rldp2_);
    }
  }
}

TwostepOverlayImpl::TransferData::TransferData() : timeout_(td::Timestamp::in(TIMEOUT)) {
}

void TwostepOverlayImpl::deliver_broadcast(const td::BufferSlice &data) {
  auto R = fetch_tl_object<ton_api::twostepOverlay_broadcastInner>(data, true);
  if (R.is_error()) {
    VLOG(OVERLAY_WARNING) << this << ": can not parse broadcast message: " << R.move_as_error();
  } else {
    auto obj = R.move_as_ok();
    callback_->receive_broadcast(PublicKeyHash(obj->send_as_), overlay_id_, std::move(obj->data_));
  }
}

}  // namespace overlay

}  // namespace ton
