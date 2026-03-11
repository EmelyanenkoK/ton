/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "crypto/block/block-auto.h"
#include "crypto/vm/cells/CellBuilder.h"

#include "fixtures.h"

namespace block::test {

td::Ref<vm::Cell> make_ext_blk_ref_cell(td::uint64 end_lt, td::uint32 seq_no) {
  vm::CellBuilder cb;
  cb.store_long(end_lt, 64);
  cb.store_long(seq_no, 32);
  cb.store_zeroes(256);  // root_hash
  cb.store_zeroes(256);  // file_hash
  return cb.finalize_novm();
}

td::Ref<vm::Cell> make_block_info_cell(ton::ShardIdFull shard, ton::BlockSeqno seqno, bool before_split) {
  vm::CellBuilder shard_cb;
  int pfx_bits = ton::shard_pfx_len(shard.shard);
  bool ok = block::gen::t_ShardIdent.pack_shard_ident(shard_cb, pfx_bits, shard.workchain, shard.shard);
  CHECK(ok);
  auto shard_cs = td::Ref<vm::CellSlice>(true, shard_cb.finalize_novm());

  bool not_master = !shard.is_masterchain();

  td::Ref<vm::Cell> master_ref;
  if (not_master) {
    master_ref = make_ext_blk_ref_cell();
  }

  auto prev_ref = make_ext_blk_ref_cell(0, seqno > 0 ? seqno - 1 : 0);

  block::gen::BlockInfo::Record info{
      .version = 0,
      .not_master = not_master,
      .after_merge = false,
      .before_split = before_split,
      .after_split = false,
      .want_split = false,
      .want_merge = false,
      .key_block = false,
      .vert_seqno_incr = false,
      .flags = 0,
      .seq_no = seqno,
      .vert_seq_no = 0,
      .shard = std::move(shard_cs),
      .gen_utime = 0,
      .start_lt = 0,
      .end_lt = 0,
      .gen_validator_list_hash_short = 0,
      .gen_catchain_seqno = 0,
      .min_ref_mc_seqno = 0,
      .prev_key_block_seqno = 0,
      .gen_software = {},
      .master_ref = std::move(master_ref),
      .prev_ref = std::move(prev_ref),
      .prev_vert_ref = {},
  };

  td::Ref<vm::Cell> cell;
  ok = tlb::pack_cell(cell, info);
  CHECK(ok);
  return cell;
}

td::Ref<vm::Cell> make_merkle_update(td::Ref<vm::Cell> from, td::Ref<vm::Cell> to) {
  return vm::CellBuilder::create_merkle_update(from, to);
}

td::Ref<vm::Cell> make_block_cell(ton::ShardIdFull shard, ton::BlockSeqno seqno, td::Ref<vm::Cell> state_update,
                                  bool before_split) {
  block::gen::Block::Record block_rec{
      .global_id = 0,
      .info = make_block_info_cell(shard, seqno, before_split),
      .value_flow = vm::CellBuilder().store_long(0xFACE0001, 32).finalize_novm(),
      .state_update = std::move(state_update),
      .extra = vm::CellBuilder().store_long(0xFACE0002, 32).finalize_novm(),
  };

  td::Ref<vm::Cell> cell;
  bool ok = tlb::pack_cell(cell, block_rec);
  CHECK(ok);
  return cell;
}

}  // namespace block::test
