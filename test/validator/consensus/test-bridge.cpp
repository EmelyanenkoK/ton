/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "block/validator-set.h"
#include "td/db/RocksDb.h"
#include "td/utils/filesystem.h"
#include "td/utils/port/path.h"
#include "td/utils/tests.h"

#include "test-helpers.h"

namespace ton::validator::consensus::test {
namespace {

td::Ref<block::ValidatorSet> make_validator_set(CatchainSeqno cc_seqno, ShardIdFull shard, size_t n_validators = 2) {
  std::vector<ValidatorDescr> validators;
  validators.reserve(n_validators);
  for (size_t i = 0; i < n_validators; ++i) {
    auto public_key = PrivateKey{privkeys::Ed25519::random()}.compute_public_key();
    validators.emplace_back(Ed25519_PublicKey{public_key.ed25519_value().raw()}, 1);
  }
  return td::make_ref<block::ValidatorSet>(cc_seqno, shard, std::move(validators));
}

std::string bridge_db_path(const std::string& db_root, ShardIdFull shard,
                           const td::Ref<block::ValidatorSet>& validator_set, ValidatorSessionId session_id) {
  return db_root + "/consensus/consensus." + std::to_string(shard.workchain) + "." +
         std::to_string(shard.shard) + "." + std::to_string(validator_set->get_catchain_seqno()) + "." +
         session_id.to_hex() + "/";
}

}  // namespace

// Resolve params through ValidatorManagerOptions with a no-op override and an
// effective override to prove only real changes alter the effective config.
TEST(Bridge, NoncriticalParamsOverridesOnlyAffectEffectiveChanges) {
  using namespace std::chrono_literals;

  auto opts = ValidatorManagerOptions::create(BlockIdExt{}, BlockIdExt{});
  const auto base = NewConsensusConfig::NoncriticalParams{};
  const auto shard = ShardIdFull{basechainId, shardIdAll};
  const auto cc_seqno = static_cast<td::uint32>(42);

  EXPECT_EQ(opts->get_noncritical_params(shard, cc_seqno, base), base);

  NoncriticalParamsOverride no_op;
  no_op.shard = shard;
  no_op.from_seqno = 0;
  no_op.to_seqno = 100;
  no_op.params.target_rate = base.target_rate;
  opts.write().set_noncritical_params_overrides({no_op});
  EXPECT_EQ(opts->get_noncritical_params(shard, cc_seqno, base), base);

  NoncriticalParamsOverride effective = no_op;
  effective.params.target_rate = 500ms;
  effective.params.first_block_timeout = 1500ms;
  opts.write().set_noncritical_params_overrides({effective});

  const auto resolved = opts->get_noncritical_params(shard, cc_seqno, base);
  EXPECT_EQ(resolved.target_rate, 500ms);
  EXPECT_EQ(resolved.first_block_timeout, 1500ms);
  EXPECT(resolved != base);

  // A seqno outside the override window must fall back to the base config.
  EXPECT_EQ(opts->get_noncritical_params(shard, 101, base), base);
}

// Create, destroy, and recreate the same RocksDB-backed consensus directory to
// prove the bridge DB path can be reused without stale state or lock files.
TEST(Bridge, ConsensusDbCanBeDestroyedAndRecreatedOnSamePath) {
  const std::string db_root = "tmp-dir-test-bridge";
  td::rmrf(db_root).ignore();
  td::mkdir(db_root).ensure();

  const auto shard = ShardIdFull{basechainId, shardIdAll};
  const auto validator_set = make_validator_set(42, shard);
  const auto session_id = bits256_pattern(1);
  const auto consensus_dir = bridge_db_path(db_root, shard, validator_set, session_id);
  const auto rocksdb_dir = consensus_dir + "db/";

  {
    td::mkpath(rocksdb_dir).ensure();
    auto db = td::RocksDb::open(rocksdb_dir).ensure().move_as_ok();
    db.set("bridge-key", "bridge-value").ensure();
    db.flush().ensure();
  }
  td::RocksDb::destroy(rocksdb_dir).ensure();
  td::rmrf(consensus_dir).ignore();
  EXPECT(td::stat(consensus_dir).is_error());

  {
    td::mkpath(rocksdb_dir).ensure();
    auto db = td::RocksDb::open(rocksdb_dir).ensure().move_as_ok();
    std::string value;
    EXPECT_EQ(db.get("bridge-key", value).move_as_ok(), td::RocksDb::GetStatus::NotFound);
    db.set("bridge-key", "bridge-value-2").ensure();
    db.flush().ensure();
  }
  td::RocksDb::destroy(rocksdb_dir).ensure();
  td::rmrf(consensus_dir).ignore();
  EXPECT(td::stat(consensus_dir).is_error());

  td::rmrf(db_root).ignore();
}

}  // namespace ton::validator::consensus::test
