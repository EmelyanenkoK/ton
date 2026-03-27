/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */
#include "adnl/utils.hpp"
#include "auto/tl/ton_api.h"
#include "block/block-auto.h"
#include "block/validator-set.h"
#include "consensus/utils.h"
#include "td/utils/Random.h"

#include "actors.h"

namespace ton::validator::consensus::test {

// --- Well-known constants ---

static td::Bits256 from_hex(td::Slice s) {
  td::Bits256 x;
  CHECK(x.from_hex(s) == 256);
  return x;
}

td::Ref<vm::Cell> gen_shard_state(BlockSeqno seqno) {
  return vm::CellBuilder().store_long(0xabcdabcdU, 32).store_long(seqno, 32).finalize_novm();
}

td::Ref<vm::Cell> make_ext_blk_ref(BlockIdExt block_id, LogicalTime lt) {
  vm::CellBuilder cb;
  cb.store_long_bool(lt, 64);
  cb.store_long_bool(block_id.seqno(), 32);
  cb.store_bits_bool(block_id.root_hash);
  cb.store_bits_bool(block_id.file_hash);
  return cb.finalize_novm();
}

const CatchainSeqno CC_SEQNO = 123;
const BlockIdExt MIN_MC_BLOCK_ID{masterchainId, shardIdAll, 0,
                                 from_hex("AAAAAAAABBBBBBBBCCCCCCCCDDDDDDDDAAAAAAAABBBBBBBBCCCCCCCCDDDDDDDD"),
                                 from_hex("0123456012345601234560123456012345601234560123456777777701234567")};
const td::Bits256 SESSION_ID = from_hex("00001234000012340000123400001234aaaaaaaabbbbbbbbcccccccceeeeeeee");

BlockIdExt first_parent(ShardIdFull shard) {
  return BlockIdExt{shard.workchain, shard.shard, 0, td::Bits256(gen_shard_state(0)->get_hash().bits()),
                    from_hex("89abcde89abcde89abcde89abcde89abcde89abcde89abcdefffffff89abcdef")};
}

// --- TestSimplexDb ---

TestSimplexDb::TestSimplexDb(simplex::Bus& bus) {
  init_pool_state(bus);
  init_votes(bus);
}

template <>
void TestSimplexDb::handle(BusHandle, std::shared_ptr<const StopRequested>) {
  stop();
}

template <>
td::actor::Task<> TestSimplexDb::process(BusHandle bus, std::shared_ptr<simplex::BroadcastVote> event) {
  auto vote = event->vote.to_tl();
  auto hash = sha256_bits256(serialize_tl_object(vote, true));
  if (saved_votes_.contains(hash)) {
    auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
    if (!test_bus.trace_sink.empty()) {
      td::actor::send_closure(test_bus.trace_sink, &TraceSink::record_duplicate_local_vote_persistence,
                              bus->local_id.idx.value(),
                              DuplicateLocalVotePersistence{.vote = event->vote});
    }
    co_return td::Status::Error(cancelled, "Vote was already casted");
  }
  saved_votes_.insert(hash);
  auto key = create_serialize_tl_object<ton_api::consensus_simplex_db_key_vote>(hash);
  auto value = create_serialize_tl_object<ton_api::consensus_simplex_db_ourVote>(std::move(vote), next_seqno_++);
  co_return co_await owning_bus()->db->set(std::move(key), std::move(value));
}

template <>
td::actor::Task<> TestSimplexDb::process(BusHandle, std::shared_ptr<simplex::SaveCertificate> event) {
  auto cert = event->cert->to_tl();
  auto hash = sha256_bits256(serialize_tl_object(cert, true));
  if (saved_votes_.contains(hash)) {
    co_return td::Status::Error(cancelled, "Certificate was already saved");
  }
  saved_votes_.insert(hash);
  auto key = create_serialize_tl_object<ton_api::consensus_simplex_db_key_vote>(hash);
  auto value = create_serialize_tl_object<ton_api::consensus_simplex_db_cert>(std::move(cert));
  co_return co_await owning_bus()->db->set(std::move(key), std::move(value));
}

template <>
td::actor::Task<> TestSimplexDb::process(BusHandle, std::shared_ptr<simplex::LeaderWindowObserved> event) {
  auto window = event->start_slot / owning_bus()->config.slots_per_leader_window;
  CHECK(first_nonannounced_window_ <= window);
  first_nonannounced_window_ = window + 1;
  auto value = create_serialize_tl_object<ton_api::consensus_simplex_db_poolState>(first_nonannounced_window_);
  co_return co_await owning_bus()->db->set(pool_state_key_.clone(), std::move(value));
}

void TestSimplexDb::init_pool_state(simplex::Bus& bus) {
  auto pool_state_str = bus.db->get(pool_state_key_);
  if (pool_state_str.has_value()) {
    auto pool_state = fetch_tl_object<ton_api::consensus_simplex_db_poolState>(*pool_state_str, true).move_as_ok();
    first_nonannounced_window_ = pool_state->first_nonannounced_window_;
    bus.first_nonannounced_window = first_nonannounced_window_;
  }
}

void TestSimplexDb::init_votes(simplex::Bus& bus) {
  struct OurVote {
    td::int64 seqno;
    simplex::Vote vote;
    std::strong_ordering operator<=>(const OurVote& other) const {
      return seqno <=> other.seqno;
    }
  };

  std::vector<OurVote> our_votes;
  std::vector<simplex::CertificateRef<simplex::Vote>> certs;

  auto votes = bus.db->get_by_prefix(ton_api::consensus_simplex_db_key_vote::ID);
  for (auto& [key_str, value_str] : votes) {
    auto key = fetch_tl_object<ton_api::consensus_simplex_db_key_vote>(key_str, true).move_as_ok();
    saved_votes_.insert(key->vote_hash_);
    auto value = fetch_tl_object<ton_api::consensus_simplex_db_Vote>(value_str, true).move_as_ok();
    auto our_vote_fn = [&](ton_api::consensus_simplex_db_ourVote& vote) {
      our_votes.push_back(OurVote{vote.seqno_, simplex::Vote::from_tl(*vote.vote_)});
    };
    auto cert_fn = [&](ton_api::consensus_simplex_db_cert& vote) {
      certs.push_back(simplex::Certificate<simplex::Vote>::from_tl(std::move(*vote.cert_), bus).move_as_ok());
    };
    ton_api::downcast_call(*value, td::overloaded(our_vote_fn, cert_fn));
  }

  auto notar_certs = bus.db->get_by_prefix(ton_api::consensus_simplex_db_key_candidateResolver_notarCert::ID);
  for (auto& [key_str, value_str] : notar_certs) {
    auto key =
        fetch_tl_object<ton_api::consensus_simplex_db_key_candidateResolver_notarCert>(key_str, true).move_as_ok();
    CandidateId id = CandidateId::from_tl(key->candidateId_);
    auto value =
        fetch_tl_object<ton_api::consensus_simplex_db_candidateResolver_notarCert>(value_str, true).move_as_ok();
    auto cert = simplex::NotarCert::from_tl(std::move(*value->notar_), simplex::NotarizeVote{id}, bus).move_as_ok();
    certs.push_back(std::move(cert.unique_write()).consume_and_upcast());
  }

  std::sort(our_votes.begin(), our_votes.end());
  if (!our_votes.empty()) {
    next_seqno_ = our_votes.back().seqno + 1;
  }

  bus.bootstrap_certificates = std::move(certs);
  bus.bootstrap_votes = td::transform(our_votes, [](const OurVote& vote) { return vote.vote; });
}

// --- TestSimplexObserver ---

template <>
void TestSimplexObserver::handle(BusHandle, std::shared_ptr<const StopRequested>) {
  stop();
}

template <>
void TestSimplexObserver::handle(BusHandle bus, std::shared_ptr<const simplex::LeaderWindowObserved> event) {
  auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
  if (!test_bus.trace_sink.empty()) {
    td::actor::send_closure(test_bus.trace_sink, &TraceSink::record_leader_window_observed, bus->local_id.idx.value(),
                            *event);
  }
}

template <>
void TestSimplexObserver::handle(BusHandle bus, std::shared_ptr<const OurLeaderWindowStarted> event) {
  auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
  if (!test_bus.trace_sink.empty()) {
    td::actor::send_closure(test_bus.trace_sink, &TraceSink::record_leader_window_started, bus->local_id.idx.value(),
                            *event);
  }
}

template <>
void TestSimplexObserver::handle(BusHandle bus, std::shared_ptr<const CandidateGenerated> event) {
  auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
  if (!test_bus.trace_sink.empty()) {
    td::actor::send_closure(test_bus.trace_sink, &TraceSink::record_candidate_generated, bus->local_id.idx.value(),
                            *event);
  }
}

template <>
void TestSimplexObserver::handle(BusHandle bus, std::shared_ptr<const simplex::NotarizationObserved> event) {
  auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
  if (!test_bus.trace_sink.empty()) {
    td::actor::send_closure(test_bus.trace_sink, &TraceSink::record_notarization_observed, bus->local_id.idx.value(),
                            *event);
  }
}

template <>
void TestSimplexObserver::handle(BusHandle bus, std::shared_ptr<const simplex::FinalizationObserved> event) {
  auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
  if (!test_bus.trace_sink.empty()) {
    td::actor::send_closure(test_bus.trace_sink, &TraceSink::record_finalization_observed, bus->local_id.idx.value(),
                            *event);
  }
}

template <>
void TestSimplexObserver::handle(BusHandle bus, std::shared_ptr<const MisbehaviorReport> event) {
  auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
  if (!test_bus.trace_sink.empty()) {
    td::actor::send_closure(test_bus.trace_sink, &TraceSink::record_misbehavior, bus->local_id.idx.value(), *event);
  }
}

// --- TestDbImpl ---

TestDbImpl::TestDbImpl(std::shared_ptr<DbInner> db, Range db_delay) : db_(std::move(db)), db_delay_(db_delay) {
  std::scoped_lock lock(db_->mutex);
  for (auto& [key, value] : db_->map) {
    snapshot_.emplace(key.clone(), value.clone());
  }
}

void TestDbImpl::disable() {
  std::scoped_lock lock(db_->mutex);
  disabled_ = true;
}

std::optional<td::BufferSlice> TestDbImpl::get(td::Slice key) const {
  auto it = snapshot_.find(td::BufferSlice{key});
  if (it == snapshot_.end()) {
    return std::nullopt;
  }
  return it->second.clone();
}

std::vector<std::pair<td::BufferSlice, td::BufferSlice>> TestDbImpl::get_by_prefix(td::uint32 prefix) const {
  std::vector<std::pair<td::BufferSlice, td::BufferSlice>> result;
  td::BufferSlice begin{(const char*)&prefix, 4};
  td::uint32 prefix2 = prefix + 1;
  td::BufferSlice end{(const char*)&prefix2, 4};
  for (auto it = snapshot_.lower_bound(begin); it != snapshot_.end() && it->first < end; ++it) {
    result.emplace_back(it->first.clone(), it->second.clone());
  }
  return result;
}

td::actor::Task<> TestDbImpl::set(td::BufferSlice key, td::BufferSlice value) {
  co_await td::actor::coro_sleep(td::Timestamp::in(td::Random::fast(db_delay_.first, db_delay_.second)));
  std::scoped_lock lock(db_->mutex);
  if (disabled_) {
    co_return td::Status::Error("db is disabled");
  }
  db_->map[std::move(key)] = std::move(value);
  co_return {};
}

td::actor::Task<> TestDbImpl::close() {
  co_return {};
}

// --- TestManagerFacade ---

TestManagerFacade::TestManagerFacade(size_t node_idx, td::Ref<block::ValidatorSet> validator_set,
                                     td::actor::ActorId<TestConsensus> test_consensus, const TestConfig& config)
    : node_idx_(node_idx), validator_set_(std::move(validator_set)), test_consensus_(test_consensus), config_(config) {
}

td::actor::Task<GeneratedCandidate> TestManagerFacade::collate_block(CollateParams params,
                                                                     td::CancellationToken cancellation_token) {
  CHECK(params.prev.size() == 1);
  uint32_t prev_seqno = params.prev[0].seqno();
  LOG(WARNING) << "Collate block #" << prev_seqno + 1;
  CHECK(params.prev[0].shard_full() == config_.shard);
  CHECK(params.min_masterchain_block_id == MIN_MC_BLOCK_ID);
  CHECK(params.prev_block_state_roots.size() == 1 &&
        params.prev_block_state_roots[0]->get_hash() == gen_shard_state(prev_seqno)->get_hash());
  if (prev_seqno != 0) {
    CHECK(params.prev_block_data.size() == 1 && params.prev_block_data[0]->block_id() == params.prev[0]);
  }
  double gen_utime = params.utime ? params.utime.value() : td::Clocks::system();

  block::gen::BlockInfo::Record info;
  info.version = 0;
  info.not_master = !config_.shard.is_masterchain();
  info.after_merge = info.before_split = info.after_split = false;
  info.want_split = info.want_merge = false;
  info.key_block = info.vert_seqno_incr = false;
  info.flags = 0;
  info.seq_no = prev_seqno + 1;
  info.vert_seq_no = 0;

  vm::CellBuilder cb;
  block::ShardId{config_.shard}.serialize(cb);
  info.shard = cb.as_cellslice_ref();

  info.gen_utime = (UnixTime)gen_utime;
  info.start_lt = (LogicalTime)info.seq_no * 1000;
  info.end_lt = (LogicalTime)info.seq_no * 1000 + 1;
  info.gen_validator_list_hash_short = validator_set_->get_validator_set_hash();
  info.gen_catchain_seqno = validator_set_->get_catchain_seqno();
  info.min_ref_mc_seqno = MIN_MC_BLOCK_ID.seqno();
  info.prev_key_block_seqno = MIN_MC_BLOCK_ID.seqno();
  if (!config_.shard.is_masterchain()) {
    info.master_ref = make_ext_blk_ref(MIN_MC_BLOCK_ID, 0);
  }
  info.prev_ref = make_ext_blk_ref(params.prev[0], (LogicalTime)prev_seqno * 1000 + 1);
  td::Ref<vm::Cell> block_info;
  CHECK(block::gen::pack_cell(block_info, info));

  td::Ref<vm::Cell> value_flow = vm::CellBuilder{}.finalize_novm();
  td::Ref<vm::Cell> merkle_update =
      vm::CellBuilder::create_merkle_update(gen_shard_state(prev_seqno), gen_shard_state(prev_seqno + 1));

  td::Bits256 rand_data;
  td::Random::secure_bytes(rand_data.as_slice());
  td::Ref<vm::Cell> block_extra = vm::CellBuilder{}.store_bytes(rand_data.as_slice()).finalize_novm();

  td::Ref<vm::Cell> block_root = vm::CellBuilder{}
                                     .store_long(0x11ef55aa, 32)
                                     .store_long(-111, 32)
                                     .store_ref(block_info)
                                     .store_ref(value_flow)
                                     .store_ref(merkle_update)
                                     .store_ref(block_extra)
                                     .finalize_novm();
  td::BufferSlice data = vm::std_boc_serialize(block_root, 31).move_as_ok();

  std::vector<td::Ref<vm::Cell>> collated_roots;
  auto cell = vm::CellBuilder{}
                  .store_long(0x638eb292, 32)
                  .store_long(0, 32)
                  .store_long((td::uint64)(gen_utime * 1000.0), 64)
                  .finalize_novm();
  collated_roots.push_back(std::move(cell));
  td::BufferSlice collated_data = co_await vm::std_boc_serialize_multi(collated_roots, 2);

  co_await td::actor::coro_sleep(
      td::Timestamp::in(td::Random::fast(config_.collation_time.first, config_.collation_time.second)));

  BlockCandidate candidate(
      params.creator,
      BlockIdExt(BlockId(params.shard, prev_seqno + 1), block_root->get_hash().bits(), td::sha256_bits256(data)),
      td::sha256_bits256(collated_data), data.clone(), collated_data.clone());
  CHECK(params.skip_store_candidate);
  co_return GeneratedCandidate{.candidate = std::move(candidate), .is_cached = false, .self_collated = true};
}

td::actor::Task<ValidateCandidateResult> TestManagerFacade::validate_block_candidate(BlockCandidate candidate,
                                                                                     ValidateParams params,
                                                                                     td::Timestamp timeout) {
  CHECK(params.prev.size() == 1);
  uint32_t prev_seqno = params.prev[0].seqno();
  LOG(WARNING) << "Validate block #" << candidate.id.seqno();
  CHECK(params.prev[0].shard_full() == config_.shard);
  CHECK(candidate.id.shard_full() == config_.shard);
  CHECK(candidate.id.seqno() == prev_seqno + 1);
  CHECK(params.prev_block_state_roots.size() == 1 &&
        params.prev_block_state_roots[0]->get_hash() == gen_shard_state(prev_seqno)->get_hash());
  co_await td::actor::coro_sleep(
      td::Timestamp::in(td::Random::fast(config_.validation_time.first, config_.validation_time.second)));
  CHECK(params.skip_store_candidate);
  co_return CandidateAccept{.ok_from_utime = co_await get_candidate_gen_utime_exact(candidate)};
}

// accept_block, wait_block_state_root, wait_block_data are implemented in harness.cpp
// since they need access to the TestConsensus actor.

}  // namespace ton::validator::consensus::test
