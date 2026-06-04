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
#include "block/block-auto.h"
#include "block/block-parse.h"
#include "td/utils/overloaded.h"
#include "ton/ton-io.hpp"
#include "ton/ton-shard.h"
#include "ton/ton-tl.hpp"
#include "validator/full-node.h"
#include "vm/cells/MerkleProof.h"

#include "download-block-new.hpp"
#include "full-node-serializer.hpp"

namespace ton {

namespace validator {

namespace fullnode {

namespace {

std::string block_id_or_none(const BlockIdExt &block_id) {
  return block_id.is_valid() ? block_id.to_str() : "none";
}

const char *download_mode(const BlockIdExt &block_id) {
  return block_id.is_valid() ? "block" : "next";
}

}  // namespace

DownloadBlockNew::DownloadBlockNew(BlockIdExt block_id, adnl::AdnlNodeIdShort local_id,
                                   overlay::OverlayIdShort overlay_id, adnl::AdnlNodeIdShort download_from,
                                   td::uint32 priority, td::Timestamp timeout,
                                   td::actor::ActorId<ValidatorManagerInterface> validator_manager,
                                   td::actor::ActorId<adnl::AdnlSenderInterface> rldp,
                                   td::actor::ActorId<overlay::Overlays> overlays, td::actor::ActorId<adnl::Adnl> adnl,
                                   td::actor::ActorId<adnl::AdnlExtClient> client, td::Promise<DownloadedBlock> promise)
    : block_id_(block_id)
    , local_id_(local_id)
    , overlay_id_(overlay_id)
    , download_from_(download_from)
    , priority_(priority)
    , timeout_(timeout)
    , validator_manager_(validator_manager)
    , rldp_(rldp)
    , overlays_(overlays)
    , adnl_(adnl)
    , client_(client)
    , promise_(std::move(promise))
    , allow_partial_proof_{!block_id_.is_masterchain()} {
  block_.block.id = block_id_;
}

DownloadBlockNew::DownloadBlockNew(adnl::AdnlNodeIdShort local_id, overlay::OverlayIdShort overlay_id,
                                   BlockIdExt prev_id, adnl::AdnlNodeIdShort download_from, td::uint32 priority,
                                   td::Timestamp timeout,
                                   td::actor::ActorId<ValidatorManagerInterface> validator_manager,
                                   td::actor::ActorId<adnl::AdnlSenderInterface> rldp,
                                   td::actor::ActorId<overlay::Overlays> overlays, td::actor::ActorId<adnl::Adnl> adnl,
                                   td::actor::ActorId<adnl::AdnlExtClient> client, td::Promise<DownloadedBlock> promise)
    : local_id_(local_id)
    , overlay_id_(overlay_id)
    , prev_id_(prev_id)
    , download_from_(download_from)
    , priority_(priority)
    , timeout_(timeout)
    , validator_manager_(validator_manager)
    , rldp_(rldp)
    , overlays_(overlays)
    , adnl_(adnl)
    , client_(client)
    , promise_(std::move(promise)) {
}

void DownloadBlockNew::abort_query(td::Status reason) {
  if (promise_) {
    if (reason.code() == ErrorCode::notready || reason.code() == ErrorCode::timeout) {
      VLOG(FULL_NODE_DEBUG) << "failed to download block " << block_id_ << "from " << download_from_ << ": " << reason;
    } else {
      VLOG(FULL_NODE_NOTICE) << "failed to download block " << block_id_ << " from " << download_from_ << ": "
                             << reason;
    }
    promise_.set_error(std::move(reason));
  }
  stop();
}

void DownloadBlockNew::alarm() {
  abort_query(td::Status::Error(ErrorCode::timeout, "timeout"));
}

void DownloadBlockNew::finish_query() {
  if (promise_) {
    promise_.set_value(std::move(block_));
  }
  stop();
}

void DownloadBlockNew::start_up() {
  alarm_timestamp() = timeout_;

  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<BlockHandle> R) {
    R.ensure();
    td::actor::send_closure(SelfId, &DownloadBlockNew::got_block_handle, R.move_as_ok());
  });

  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_block_handle,
                          block_id_.is_valid() ? block_id_ : prev_id_, true, std::move(P));
}

void DownloadBlockNew::got_block_handle(BlockHandle handle) {
  handle_ = std::move(handle);

  if (!block_id_.is_valid()) {
    CHECK(prev_id_.is_valid());
    if (handle_->inited_next_left()) {
      block_id_ = handle_->one_next(true);
      block_.block.id = block_id_;
      handle_ = nullptr;
      start_up();
      return;
    }
  }

  if (block_id_.is_valid() &&
      (handle_->inited_proof() || (handle_->inited_proof_link() && allow_partial_proof_) || skip_proof_) &&
      handle_->received()) {
    CHECK(block_.block.id == block_id_);
    CHECK(handle_->id() == block_id_);
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Ref<BlockData>> R) {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &DownloadBlockNew::abort_query,
                                R.move_as_error_prefix("failed to get from db: "));
      } else {
        td::actor::send_closure(SelfId, &DownloadBlockNew::got_data_from_db, R.move_as_ok()->data());
      }
    });
    td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_block_data_from_db, handle_,
                            std::move(P));
    return;
  }

  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<std::unique_ptr<ActionToken>> R) {
    if (R.is_error()) {
      td::actor::send_closure(SelfId, &DownloadBlockNew::abort_query,
                              R.move_as_error_prefix("failed to get download token: "));
    } else {
      td::actor::send_closure(SelfId, &DownloadBlockNew::got_download_token, R.move_as_ok());
    }
  });
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_download_token, 1, priority_, timeout_,
                          std::move(P));
}

void DownloadBlockNew::got_download_token(std::unique_ptr<ActionToken> token) {
  token_ = std::move(token);

  if (download_from_.is_zero() && client_.empty()) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<std::vector<adnl::AdnlNodeIdShort>> R) {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &DownloadBlockNew::abort_query, R.move_as_error());
      } else {
        auto vec = R.move_as_ok();
        if (vec.size() == 0) {
          td::actor::send_closure(SelfId, &DownloadBlockNew::abort_query,
                                  td::Status::Error(ErrorCode::notready, "no nodes"));
        } else {
          td::actor::send_closure(SelfId, &DownloadBlockNew::got_node_to_download, vec[0]);
        }
      }
    });

    td::actor::send_closure(overlays_, &overlay::Overlays::get_overlay_random_peers, local_id_, overlay_id_, 1,
                            std::move(P));
  } else {
    got_node_to_download(download_from_);
  }
}

void DownloadBlockNew::got_node_to_download(adnl::AdnlNodeIdShort node) {
  download_from_ = node;

  VLOG(FULL_NODE_DEBUG) << "downloading proof for " << block_id_;

  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::BufferSlice> R) mutable {
    if (R.is_error()) {
      td::actor::send_closure(SelfId, &DownloadBlockNew::abort_query, R.move_as_error());
    } else {
      td::actor::send_closure(SelfId, &DownloadBlockNew::got_data, R.move_as_ok());
    }
  });

  td::BufferSlice q;
  if (block_id_.is_valid()) {
    q = create_serialize_tl_object<ton_api::tonNode_downloadBlockFull>(create_tl_block_id(block_id_));
  } else {
    q = create_serialize_tl_object<ton_api::tonNode_downloadNextBlockFull>(create_tl_block_id(prev_id_));
  }
  if (client_.empty()) {
    td::actor::send_closure(overlays_, &overlay::Overlays::send_query_via, download_from_, local_id_, overlay_id_,
                            "get_block_full", std::move(P), td::Timestamp::in(15.0), std::move(q),
                            FullNode::max_proof_size() + FullNode::max_block_size() + 128, rldp_);
  } else {
    td::actor::send_closure(client_, &adnl::AdnlExtClient::send_query, "get_block_full",
                            create_serialize_tl_object_suffix<ton_api::tonNode_query>(std::move(q)),
                            td::Timestamp::in(15.0), std::move(P));
  }
}

void DownloadBlockNew::got_data(td::BufferSlice data) {
  auto F = fetch_tl_object<ton_api::tonNode_DataFull>(std::move(data), true);

  if (F.is_error()) {
    abort_query(F.move_as_error_prefix("received invalid answer: "));
    return;
  }

  auto f = F.move_as_ok();
  if (f->get_id() == ton_api::tonNode_dataFullEmpty::ID) {
    abort_query(td::Status::Error(ErrorCode::notready, "node doesn't have this block"));
    return;
  }

  // Check if state is needed for decompression
  auto R_requires_state = need_state_for_decompression(*f);
  if (R_requires_state.is_error()) {
    abort_query(R_requires_state.move_as_error_prefix("failed to check if state is required: "));
    return;
  }

  if (R_requires_state.move_as_ok()) {
    // Only tonNode_dataFullCompressedV2 may require state
    ton_api::downcast_call(
        *f, td::overloaded(
                [&](ton_api::tonNode_dataFullCompressedV2 &compressed_v2) {
                  BlockIdExt id = create_block_id(compressed_v2.id_);

                  auto R_prev_blocks = extract_prev_blocks_from_proof(compressed_v2.proof_.as_slice(), id);
                  if (R_prev_blocks.is_error()) {
                    abort_query(R_prev_blocks.move_as_error_prefix("failed to extract prev block IDs: "));
                    return;
                  }
                  auto prev_blocks = R_prev_blocks.move_as_ok();
                  if (block_id_.is_valid()) {
                    if (id != block_id_) {
                      abort_query(td::Status::Error("block id mismatch"));
                      return;
                    }
                  } else {
                    if (prev_blocks != std::vector{prev_id_}) {
                      abort_query(td::Status::Error("prev block id mismatch"));
                      return;
                    }
                  }
                  auto P_state = td::PromiseCreator::lambda([SelfId = actor_id(this), data_full = std::move(f)](
                                                                td::Result<td::Ref<ShardState>> R_state) mutable {
                    if (R_state.is_error()) {
                      td::actor::send_closure(
                          SelfId, &DownloadBlockNew::abort_query,
                          R_state.move_as_error_prefix("failed to get state for block full decompression: "));
                      return;
                    }
                    td::actor::send_closure(SelfId, &DownloadBlockNew::got_ready_to_deserialize, std::move(data_full),
                                            R_state.move_as_ok());
                  });
                  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::wait_state_by_prev_blocks, id,
                                          std::move(prev_blocks), std::move(P_state));
                },
                [&](auto &) { UNREACHABLE(); }));
    return;
  }

  // Call got_state_for_block_full without state
  got_ready_to_deserialize(std::move(f));
}

void DownloadBlockNew::got_ready_to_deserialize(tl_object_ptr<ton_api::tonNode_DataFull> data_full,
                                                td::Ref<ShardState> state) {
  td::Ref<vm::Cell> state_root;
  if (state.not_null()) {
    state_root = state->root_cell();
  }

  BlockIdExt id;
  td::BufferSlice proof, block_data;
  bool is_link;
  td::Status S = deserialize_block_full(*data_full, id, proof, block_data, is_link,
                                        overlay::Overlays::max_fec_broadcast_size(), state_root);
  if (S.is_error()) {
    abort_query(S.move_as_error_prefix("cannot deserialize block: "));
    return;
  }

  if (!allow_partial_proof_ && is_link) {
    abort_query(td::Status::Error(ErrorCode::notready, "node doesn't have proof for this block"));
    return;
  }
  if (block_id_.is_valid() && id != block_id_) {
    abort_query(td::Status::Error(ErrorCode::notready, "received data for wrong block"));
    return;
  }
  block_.block.id = id;
  block_.block.data = std::move(block_data);
  if (td::sha256_bits256(block_.block.data.as_slice()) != id.file_hash) {
    abort_query(td::Status::Error(ErrorCode::notready, "received data with bad hash"));
    return;
  }

  S = fill_block_metadata(proof.as_slice(), is_link);
  if (S.is_error()) {
    abort_query(S.move_as_error_prefix("received bad proof metadata: "));
    return;
  }

  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
    if (R.is_error()) {
      td::actor::send_closure(SelfId, &DownloadBlockNew::abort_query, R.move_as_error_prefix("received bad proof: "));
    } else {
      td::actor::send_closure(SelfId, &DownloadBlockNew::checked_block_proof);
    }
  });
  if (block_id_.is_valid()) {
    if (is_link) {
      td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::validate_block_proof_link, block_id_,
                              std::move(proof), std::move(P));
    } else {
      td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::validate_block_proof, block_id_,
                              std::move(proof), std::move(P));
    }
  } else {
    CHECK(!is_link);
    td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::validate_block_is_next_proof, prev_id_, id,
                            std::move(proof), std::move(P));
  }
}

td::Status DownloadBlockNew::fill_block_metadata(td::Slice proof, bool is_proof_link) {
  TRY_RESULT(proof_root, vm::std_boc_deserialize(proof));
  block::gen::BlockProof::Record proof_rec;
  BlockIdExt proof_blk_id;
  if (!(tlb::unpack_cell(proof_root, proof_rec) &&
        block::tlb::t_BlockIdExt.unpack(proof_rec.proof_for.write(), proof_blk_id))) {
    return td::Status::Error("cannot unpack block proof");
  }
  if (proof_blk_id != block_.block.id) {
    return td::Status::Error("block proof is for another block");
  }

  TRY_RESULT(header_root, vm::MerkleProof::virtualize(proof_rec.root));
  block::gen::Block::Record blk;
  block::gen::BlockInfo::Record info;
  if (!(tlb::unpack_cell(std::move(header_root), blk) && tlb::unpack_cell(blk.info, info) && !info.version)) {
    return td::Status::Error("cannot unpack block header in proof");
  }

  block_.proof = td::BufferSlice(proof);
  block_.is_proof_link = is_proof_link;
  block_.cc_seqno = info.gen_catchain_seqno;
  block_.validator_set_hash = info.gen_validator_list_hash_short;
  block_.from_network = true;

  if (!is_proof_link) {
    td::Ref<vm::Cell> sig_root = proof_rec.signatures->prefetch_ref();
    if (sig_root.is_null()) {
      return td::Status::Error("block proof has no signatures");
    }
    ValidatorWeight sig_weight = 0;
    TRY_RESULT(sig_set, block::BlockSignatureSet::fetch(std::move(sig_root), sig_weight));
    block_.sig_set = std::move(sig_set);
  }
  return td::Status::OK();
}

void DownloadBlockNew::got_data_from_db(td::BufferSlice data) {
  block_.block.data = std::move(data);
  finish_query();
}

void DownloadBlockNew::checked_block_proof() {
  finish_query();
}

DownloadBlockNewParallel::DownloadBlockNewParallel(
    BlockIdExt block_id, adnl::AdnlNodeIdShort local_id, overlay::OverlayIdShort overlay_id,
    std::vector<adnl::AdnlNodeIdShort> download_from, td::uint32 priority, td::Timestamp timeout,
    td::actor::ActorId<ValidatorManagerInterface> validator_manager,
    td::actor::ActorId<adnl::AdnlSenderInterface> rldp, td::actor::ActorId<overlay::Overlays> overlays,
    td::actor::ActorId<adnl::Adnl> adnl, td::actor::ActorId<adnl::AdnlExtClient> client,
    td::Promise<DownloadedBlock> promise)
    : block_id_(block_id)
    , local_id_(local_id)
    , overlay_id_(overlay_id)
    , download_from_(std::move(download_from))
    , priority_(priority)
    , timeout_(timeout)
    , validator_manager_(validator_manager)
    , rldp_(rldp)
    , overlays_(overlays)
    , adnl_(adnl)
    , client_(client)
    , promise_(std::move(promise)) {
}

DownloadBlockNewParallel::DownloadBlockNewParallel(
    adnl::AdnlNodeIdShort local_id, overlay::OverlayIdShort overlay_id, BlockIdExt prev_id,
    std::vector<adnl::AdnlNodeIdShort> download_from, td::uint32 priority, td::Timestamp timeout,
    td::actor::ActorId<ValidatorManagerInterface> validator_manager,
    td::actor::ActorId<adnl::AdnlSenderInterface> rldp, td::actor::ActorId<overlay::Overlays> overlays,
    td::actor::ActorId<adnl::Adnl> adnl, td::actor::ActorId<adnl::AdnlExtClient> client,
    td::Promise<DownloadedBlock> promise)
    : prev_id_(prev_id)
    , local_id_(local_id)
    , overlay_id_(overlay_id)
    , download_from_(std::move(download_from))
    , priority_(priority)
    , timeout_(timeout)
    , validator_manager_(validator_manager)
    , rldp_(rldp)
    , overlays_(overlays)
    , adnl_(adnl)
    , client_(client)
    , promise_(std::move(promise)) {
}

void DownloadBlockNewParallel::start_up() {
  alarm_timestamp() = timeout_;
  if (download_from_.empty()) {
    LOG(INFO) << "forced block download parallel start mode=" << download_mode(block_id_)
              << " block=" << block_id_or_none(block_id_) << " prev_block=" << block_id_or_none(prev_id_)
              << " attempts=0";
    abort_query(td::Status::Error(ErrorCode::notready, "no forced download peers"));
    return;
  }

  LOG(INFO) << "forced block download parallel start mode=" << download_mode(block_id_)
            << " block=" << block_id_or_none(block_id_) << " prev_block=" << block_id_or_none(prev_id_)
            << " attempts=" << download_from_.size();
  pending_ = download_from_.size();
  attempts_.reserve(download_from_.size());
  for (size_t i = 0; i < download_from_.size(); ++i) {
    auto peer = download_from_[i];
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), peer](td::Result<DownloadedBlock> R) mutable {
      td::actor::send_closure(SelfId, &DownloadBlockNewParallel::got_result, peer, std::move(R));
    });
    if (block_id_.is_valid()) {
      attempts_.push_back(td::actor::create_actor<DownloadBlockNew>(
          PSTRING() << "downloadreq" << i, block_id_, local_id_, overlay_id_, peer, priority_, timeout_,
          validator_manager_, rldp_, overlays_, adnl_, client_, std::move(P)));
    } else {
      attempts_.push_back(td::actor::create_actor<DownloadBlockNew>(
          PSTRING() << "downloadnext" << i, local_id_, overlay_id_, prev_id_, peer, priority_, timeout_,
          validator_manager_, rldp_, overlays_, adnl_, client_, std::move(P)));
    }
  }
}

void DownloadBlockNewParallel::alarm() {
  abort_query(td::Status::Error(ErrorCode::timeout, "timeout"));
}

void DownloadBlockNewParallel::got_result(adnl::AdnlNodeIdShort peer, td::Result<DownloadedBlock> result) {
  if (finished_) {
    return;
  }
  if (result.is_ok()) {
    finished_ = true;
    auto block = result.move_as_ok();
    LOG(INFO) << "forced block download attempt success mode=" << download_mode(block_id_)
              << " peer=" << peer.bits256_value().to_hex() << " requested_block=" << block_id_or_none(block_id_)
              << " prev_block=" << block_id_or_none(prev_id_) << " downloaded_block=" << block.block.id.to_str()
              << " attempts_started=" << download_from_.size() << " failed_attempts=" << failed_attempts_
              << " cancelled_attempts=" << (pending_ > 0 ? pending_ - 1 : 0)
              << " elapsed=" << perf_timer_.elapsed() << "s";
    if (promise_) {
      promise_.set_value(std::move(block));
    }
    attempts_.clear();
    stop();
    return;
  }

  auto error = result.move_as_error();
  CHECK(pending_ > 0);
  pending_--;
  failed_attempts_++;
  LOG(INFO) << "forced block download attempt failed mode=" << download_mode(block_id_)
            << " peer=" << peer.bits256_value().to_hex() << " requested_block=" << block_id_or_none(block_id_)
            << " prev_block=" << block_id_or_none(prev_id_) << " failed_attempts=" << failed_attempts_ << "/"
            << download_from_.size() << " pending_attempts=" << pending_ << " elapsed=" << perf_timer_.elapsed()
            << "s error=" << error;
  last_error_ = std::move(error);
  if (pending_ == 0) {
    abort_query(std::move(last_error_));
  }
}

void DownloadBlockNewParallel::abort_query(td::Status reason) {
  if (finished_) {
    return;
  }
  finished_ = true;
  if (promise_) {
    LOG(INFO) << "forced block download parallel failed mode=" << download_mode(block_id_)
              << " requested_block=" << block_id_or_none(block_id_) << " prev_block=" << block_id_or_none(prev_id_)
              << " attempts_started=" << download_from_.size() << " failed_attempts=" << failed_attempts_
              << " elapsed=" << perf_timer_.elapsed() << "s error=" << reason;
    promise_.set_error(std::move(reason));
  }
  attempts_.clear();
  stop();
}

}  // namespace fullnode

}  // namespace validator

}  // namespace ton
