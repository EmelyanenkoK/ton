/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#pragma once

#include "crypto/vm/cells/Cell.h"
#include "ton/ton-types.h"

namespace block::test {

td::Ref<vm::Cell> make_ext_blk_ref_cell(td::uint64 end_lt = 0, td::uint32 seq_no = 0);
td::Ref<vm::Cell> make_block_info_cell(ton::ShardIdFull shard, ton::BlockSeqno seqno, bool before_split = false);
td::Ref<vm::Cell> make_merkle_update(td::Ref<vm::Cell> from, td::Ref<vm::Cell> to);
td::Ref<vm::Cell> make_block_cell(ton::ShardIdFull shard, ton::BlockSeqno seqno, td::Ref<vm::Cell> state_update,
                                  bool before_split = false);

}  // namespace block::test
