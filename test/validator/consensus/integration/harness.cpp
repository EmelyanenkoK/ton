/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */
#include "block/validator-set.h"
#include "consensus/simplex/bus.h"
#include "td/actor/BusRuntime.h"
#include "td/actor/coro_utils.h"
#include "td/utils/OptionParser.h"
#include "td/utils/Random.h"
#include "td/utils/port/signals.h"

#include "harness.h"
#include "invariants.h"
#include "network.h"

namespace ton::validator::consensus::test {

// --- Registry ---

std::vector<TestCaseDescriptor>& test_case_registry() {
  static std::vector<TestCaseDescriptor> registry;
  return registry;
}

// --- TestConsensus ---

class TestConsensus : public td::actor::Actor {
 public:
  explicit TestConsensus(TestCaseDescriptor desc) : desc_(std::move(desc)) {
  }

  td::actor::Task<> run() {
    auto result = co_await run_inner().wrap();
    if (result.is_error()) {
      LOG(FATAL) << "Test error: " << result.move_as_error();
    }
    LOG(WARNING) << "Test finished";
    std::exit(0);
  }

  td::actor::Task<> on_block_accepted(size_t node_idx, td::Ref<BlockData> block, size_t creator_idx,
                                      td::Ref<block::BlockSignatureSet> signatures) {
    BlockIdExt block_id = block->block_id();
    if (signatures->is_final()) {
      signatures->check_signatures(validator_set_, block_id).ensure();
    } else {
      CHECK(!desc_.config.shard.is_masterchain());
      signatures->check_approve_signatures(validator_set_, block_id).ensure();
    }
    BlockSeqno seqno = block_id.seqno();
    if (accepted_blocks_.contains(seqno)) {
      LOG_CHECK(accepted_blocks_[seqno]->block_id() == block_id) << "Accepted different blocks for seqno " << seqno;
    } else {
      accepted_blocks_[seqno] = block;
    }
    nodes_[node_idx].last_accepted_block = std::max(nodes_[node_idx].last_accepted_block, seqno);
    td::actor::send_closure(trace_sink_, &TraceSink::record_accepted_block, node_idx,
                            AcceptedBlock{.block_id = block_id});
    if (last_accepted_block_.seqno() < seqno && signatures->is_final()) {
      last_accepted_block_ = block_id;
      synthetic_mc_finalized_block_ = block_id;
      for (auto& n : nodes_) {
        if (n.status == Node::Running) {
          n.bus.publish<BlockFinalizedInMasterchain>(block_id);
        }
      }
    }
    co_return {};
  }

  td::actor::Task<> wait_block_accepted(BlockIdExt block_id) {
    BlockIdExt fp = first_parent(desc_.config.shard);
    if (block_id == fp) {
      co_return {};
    }
    td::Timestamp timeout = td::Timestamp::in(10.0);
    while (!timeout.is_in_past()) {
      auto it = accepted_blocks_.find(block_id.seqno());
      if (it != accepted_blocks_.end() && it->second->block_id() == block_id) {
        co_return {};
      }
      co_await td::actor::coro_sleep(td::Timestamp::in(0.1));
    }
    co_return td::Status::Error(ErrorCode::timeout, "timeout");
  }

  td::actor::Task<td::Ref<vm::Cell>> wait_block_state_root(BlockIdExt block_id) {
    co_await wait_block_accepted(block_id);
    co_return gen_shard_state(block_id.seqno());
  }

  td::actor::Task<td::Ref<BlockData>> wait_block_data(BlockIdExt block_id) {
    co_await wait_block_accepted(block_id);
    auto it = accepted_blocks_.find(block_id.seqno());
    CHECK(it != accepted_blocks_.end());
    CHECK(it->second->block_id() == block_id);
    co_return it->second;
  }

  // --- Accessors used by HarnessHandle ---

  td::actor::Task<TraceSnapshot> get_trace_snapshot() {
    co_return co_await td::actor::ask(trace_sink_, &TraceSink::snapshot);
  }

  TestMessageFilters& message_filters() {
    return filters_;
  }
  const TestConfig& config() const {
    return desc_.config;
  }

  TimePoint current_timepoint() {
    // The frontier is the highest leader window start observed on any node.
    // We compute it from the trace sink's data synchronously since both actors
    // run on the same scheduler and this is called from harness handle methods.
    const auto& data = trace_sink_.get_actor_unsafe().data();
    td::uint32 highest = 0;
    for (const auto& r : data.leader_windows_observed) {
      highest = std::max(highest, r.event.start_slot);
    }
    for (const auto& r : data.leader_windows_started) {
      highest = std::max(highest, r.event.start_slot);
    }
    return TimePoint{highest};
  }

  void start_node(size_t idx) {
    auto& node = nodes_[idx];
    CHECK(node.status == Node::Stopped);
    auto& runtime = node.runtime;
    BlockAccepter::register_in(runtime);
    BlockProducer::register_in(runtime);
    BlockValidator::register_in(runtime);
    runtime.register_actor<TestOverlayNode>("PrivateOverlay");
    runtime.register_actor<TestSimplexObserver>("TestSimplexObserver");
    simplex::CandidateResolver::register_in(runtime);
    simplex::Consensus::register_in(runtime);
    runtime.register_actor<TestSimplexDb>("SimplexDb");
    simplex::Pool::register_in(runtime);
    simplex::StateResolver::register_in(runtime);

    node.manager_facade = td::actor::create_actor<TestManagerFacade>(PSTRING() << "ManagerFacade." << idx, idx,
                                                                     validator_set_, actor_id(this), desc_.config);
    auto [stop_task, stop_promise] = td::actor::StartedTask<>::make_bridge();
    auto bus = std::make_shared<TestSimplexBus>();
    node.stop_waiter = std::move(stop_task);
    bus->trace_sink = trace_sink_.get();
    bus->overlay = overlay_.get();
    bus->message_filters = &filters_;
    bus->stop_promise = std::move(stop_promise);
    bus->shard = desc_.config.shard;
    bus->manager = node.manager_facade.get();
    bus->keyring = keyring_.get();
    bus->validator_opts = ValidatorManagerOptions::create(BlockIdExt{}, BlockIdExt{});
    bus->validator_set = validators_;
    bus->total_weight = total_weight_;
    bus->local_id = validators_[idx];
    bus->config = desc_.config.consensus_config;
    bus->session_id = SESSION_ID;
    bus->cc_seqno = CC_SEQNO;
    bus->validator_set_hash = validator_set_->get_validator_set_hash();
    bus->populate_collator_schedule();
    bus->db = std::make_unique<TestDbImpl>(node.db_inner, desc_.config.db_delay);
    node.bus = runtime.start(std::static_pointer_cast<simplex::Bus>(bus), PSTRING() << "consensus." << idx);
    node.status = Node::Running;
    td::actor::send_closure(trace_sink_, &TraceSink::record_lifecycle, idx, Lifecycle{.started = true});
    node.bus.publish<BlockFinalizedInMasterchain>(synthetic_mc_finalized_block_);
    BlockIdExt fp = first_parent(desc_.config.shard);
    node.bus.publish<Start>(
        td::make_ref<ChainState>(ChainState::ZerostateTip{fp, gen_shard_state(0)}, MIN_MC_BLOCK_ID));
    LOG(ERROR) << "Starting node #" << idx;
  }

  td::actor::Task<> stop_node(size_t idx) {
    auto& node = nodes_[idx];
    if (node.status == Node::Stopped) {
      co_return {};
    }
    if (node.status == Node::Stopping) {
      auto [task, promise] = td::actor::StartedTask<>::make_bridge();
      node.extra_stop_waiters.push_back(std::move(promise));
      co_return co_await std::move(task);
    }
    LOG(ERROR) << "Stopping node #" << idx;
    node.bus.publish<StopRequested>();
    dynamic_cast<TestDbImpl&>(*node.bus->db).disable();
    node.bus = {};
    node.status = Node::Stopping;
    co_await std::move(*node.stop_waiter);
    node.status = Node::Stopped;
    node.runtime = {};
    td::actor::send_closure(trace_sink_, &TraceSink::record_lifecycle, idx, Lifecycle{.started = false});
    LOG(ERROR) << "Stopped node #" << idx;
    for (auto& p : node.extra_stop_waiters) {
      p.set_value(td::Unit{});
    }
    node.extra_stop_waiters.clear();
    co_return {};
  }

  td::actor::Task<> wait_for(std::string description, std::unique_ptr<TracePredicate> predicate) {
    auto [task, promise] = td::actor::StartedTask<>::make_bridge();
    td::actor::send_closure(trace_sink_, &TraceSink::wait_for, description, std::move(predicate), std::move(promise));
    co_await std::move(task);
    co_return {};
  }

 private:
  TestCaseDescriptor desc_;
  TestMessageFilters filters_;

  td::actor::ActorOwn<keyring::Keyring> keyring_;
  td::actor::ActorOwn<TraceSink> trace_sink_;
  td::actor::ActorOwn<TestOverlay> overlay_;
  std::vector<PeerValidator> validators_;
  ValidatorWeight total_weight_ = 0;
  td::Ref<block::ValidatorSet> validator_set_;

  struct Node {
    td::actor::Runtime runtime;
    td::actor::ActorOwn<TestManagerFacade> manager_facade;
    simplex::BusHandle bus;
    BlockSeqno last_accepted_block = 0;
    std::shared_ptr<TestDbImpl::DbInner> db_inner = std::make_shared<TestDbImpl::DbInner>();
    enum Status { Stopped, Running, Stopping };
    Status status = Stopped;
    td::optional<td::actor::StartedTask<>> stop_waiter;
    std::vector<td::Promise<td::Unit>> extra_stop_waiters;
    PublicKey public_key;
    PublicKeyHash node_id;
    adnl::AdnlNodeIdFull adnl_id_full;
    adnl::AdnlNodeIdShort adnl_id;
    ValidatorWeight weight = 0;
    bool net_disabled = false;
  };
  std::vector<Node> nodes_;

  std::map<BlockSeqno, td::Ref<BlockData>> accepted_blocks_;
  BlockIdExt last_accepted_block_;
  BlockIdExt synthetic_mc_finalized_block_;
  bool finishing_ = false;

  td::actor::Task<> run_inner() {
    filters_ = {};
    keyring_ = keyring::Keyring::create("");

    if (!desc_.config.node_weights_override.empty() &&
        desc_.config.node_weights_override.size() != desc_.config.n_nodes) {
      co_return td::Status::Error("node weights override size mismatch");
    }

    BlockIdExt fp = first_parent(desc_.config.shard);
    last_accepted_block_ = fp;
    synthetic_mc_finalized_block_ = fp;

    for (size_t i = 0; i < desc_.config.n_nodes; ++i) {
      Node node;
      PrivateKey node_pk{privkeys::Ed25519::random()};
      node.public_key = node_pk.compute_public_key();
      node.node_id = node.public_key.compute_short_id();
      td::actor::send_closure(keyring_, &keyring::Keyring::add_key, std::move(node_pk), true, [](td::Result<>) {});
      PrivateKey adnl_pk{privkeys::Ed25519::random()};
      node.adnl_id_full = adnl::AdnlNodeIdFull{adnl_pk.compute_public_key()};
      node.adnl_id = node.adnl_id_full.compute_short_id();
      td::actor::send_closure(keyring_, &keyring::Keyring::add_key, std::move(adnl_pk), true, [](td::Result<>) {});
      node.weight = desc_.config.node_weight(i);
      nodes_.push_back(std::move(node));
    }

    std::vector<ValidatorDescr> validator_descrs;
    for (size_t idx = 0; idx < nodes_.size(); ++idx) {
      auto& node = nodes_[idx];
      validator_descrs.push_back(ValidatorDescr(Ed25519_PublicKey{node.public_key.ed25519_value().raw()}, node.weight,
                                                node.adnl_id.bits256_value()));
      validators_.push_back(PeerValidator{
          .idx = PeerValidatorId((int)idx),
          .key = node.public_key,
          .short_id = node.node_id,
          .adnl_id = node.adnl_id,
          .weight = node.weight,
      });
      total_weight_ += node.weight;
    }
    validator_set_ = td::Ref<block::ValidatorSet>{true, CC_SEQNO, desc_.config.shard, std::move(validator_descrs)};

    trace_sink_ = td::actor::create_actor<TraceSink>("trace-sink");
    overlay_ = td::actor::create_actor<TestOverlay>("test-overlay", trace_sink_.get(), desc_.config, filters_);

    for (size_t idx = 0; idx < desc_.config.n_nodes; ++idx) {
      start_node(idx);
    }

    // Global per-test timeout.
    alarm_timestamp() = td::Timestamp::in(desc_.timeout);

    HarnessHandle handle(actor_id(this));
    co_await desc_.scenario(handle);

    co_return co_await finalize();
  }

  td::actor::Task<> finalize() {
    finishing_ = true;
    LOG(WARNING) << "TEST FINISHED";
    std::vector<td::actor::Task<>> tasks;
    for (size_t idx = 0; idx < nodes_.size(); ++idx) {
      tasks.push_back(stop_node(idx));
    }
    co_await td::actor::all(std::move(tasks));
    LOG(WARNING) << "TEST RESULTS:";
    for (size_t idx = 0; idx < nodes_.size(); ++idx) {
      LOG(WARNING) << "Node #" << idx << ": synced up to block " << nodes_[idx].last_accepted_block;
    }
    auto snapshot = co_await get_trace_snapshot();
    co_await run_all_invariants(snapshot, desc_.config);
    if (desc_.verify) {
      co_await desc_.verify(snapshot);
    }
    co_return {};
  }

  void alarm() override {
    if (!finishing_) {
      LOG(FATAL) << "Test timeout (" << desc_.timeout << "s) expired";
    }
  }
};

// --- TestManagerFacade delegating methods ---

td::actor::Task<> TestManagerFacade::accept_block(BlockIdExt id, td::Ref<BlockData> data, size_t creator_idx,
                                                  td::Ref<block::BlockSignatureSet> signatures, int send_broadcast_mode,
                                                  bool apply) {
  co_return co_await td::actor::ask(test_consensus_, &TestConsensus::on_block_accepted, node_idx_, data, creator_idx,
                                    signatures);
}

td::actor::Task<td::Ref<vm::Cell>> TestManagerFacade::wait_block_state_root(BlockIdExt block_id,
                                                                            td::Timestamp timeout) {
  co_return co_await td::actor::ask(test_consensus_, &TestConsensus::wait_block_state_root, block_id);
}

td::actor::Task<td::Ref<BlockData>> TestManagerFacade::wait_block_data(BlockIdExt block_id, td::Timestamp timeout) {
  co_return co_await td::actor::ask(test_consensus_, &TestConsensus::wait_block_data, block_id);
}

// --- HarnessHandle delegation ---

td::actor::Task<TimePoint> HarnessHandle::stop_instance(size_t node_idx) {
  co_await td::actor::ask(test_, &TestConsensus::stop_node, node_idx);
  co_return td::actor::ActorId<TestConsensus>(test_).get_actor_unsafe().current_timepoint();
}

TimePoint HarnessHandle::start_instance(size_t node_idx) {
  td::actor::send_closure(test_, &TestConsensus::start_node, node_idx);
  return td::actor::ActorId<TestConsensus>(test_).get_actor_unsafe().current_timepoint();
}

td::actor::Task<TraceSnapshot> HarnessHandle::get_trace_snapshot() {
  co_return co_await td::actor::ask(test_, &TestConsensus::get_trace_snapshot);
}

TestMessageFilters& HarnessHandle::message_filters() {
  return td::actor::ActorId<TestConsensus>(test_).get_actor_unsafe().message_filters();
}

const TestConfig& HarnessHandle::config() const {
  return td::actor::ActorId<TestConsensus>(test_).get_actor_unsafe().config();
}

td::actor::Task<TimePoint> HarnessHandle::now() {
  // Need to go through the actor to ensure trace is up to date.
  co_await td::actor::ask(test_, &TestConsensus::get_trace_snapshot);  // sync point
  co_return td::actor::ActorId<TestConsensus>(test_).get_actor_unsafe().current_timepoint();
}

void HarnessHandle::set_validation_time(Range r) {
  // TODO: propagate to running nodes
}
void HarnessHandle::set_collation_time(Range r) {
  // TODO: propagate to running nodes
}
void HarnessHandle::set_db_delay(Range r) {
  // TODO: propagate to running nodes
}

td::actor::Task<TimePoint> HarnessHandle::set_node_network_disabled(size_t node_idx, bool disabled) {
  // TODO: implement via overlay
  co_return td::actor::ActorId<TestConsensus>(test_).get_actor_unsafe().current_timepoint();
}

td::actor::Task<> HarnessHandle::wait_for(std::string description, std::unique_ptr<TracePredicate> predicate) {
  co_return co_await td::actor::ask(test_, &TestConsensus::wait_for, std::move(description), std::move(predicate));
}

}  // namespace ton::validator::consensus::test

// --- main ---

int main(int argc, char* argv[]) {
  using namespace ton::validator::consensus::test;

  SET_VERBOSITY_LEVEL(verbosity_WARNING);
  td::set_default_failure_signal_handler().ensure();

  std::string test_case_name = "smoke";

  td::OptionParser p;
  p.set_description("adversarial consensus integration tests");
  p.add_option('h', "help", "print help", [&]() {
    std::cout << (PSLICE() << p).c_str();
    std::exit(2);
  });
  p.add_option('v', "verbosity", "set verbosity level", [&](td::Slice arg) {
    int v = VERBOSITY_NAME(FATAL) + td::to_integer<int>(arg);
    SET_VERBOSITY_LEVEL(v);
  });
  p.add_checked_option('\0', "test-case", "test case name (default: smoke)", [&](td::Slice arg) {
    test_case_name = arg.str();
    return td::Status::OK();
  });

  p.run(argc, argv).ensure();

  TestCaseDescriptor* found = nullptr;
  for (auto& desc : test_case_registry()) {
    if (desc.name == test_case_name) {
      found = &desc;
      break;
    }
  }
  if (!found) {
    LOG(FATAL) << "Unknown test case: " << test_case_name;
    return 1;
  }

  td::actor::Scheduler scheduler({7});
  td::actor::ActorOwn<TestConsensus> test;

  scheduler.run_in_context([&] {
    test = td::actor::create_actor<TestConsensus>("test-consensus", *found);
    td::actor::ask(test, &TestConsensus::run).detach();
  });
  while (scheduler.run(1)) {
  }
  return 0;
}
