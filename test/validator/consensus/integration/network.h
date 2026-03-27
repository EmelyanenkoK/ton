/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */
#pragma once

#include "consensus/simplex/bus.h"
#include "td/actor/BusRuntime.h"

#include "config.h"
#include "trace.h"

namespace ton::validator::consensus::test {

class TestOverlay;

class TestSimplexBus : public simplex::Bus {
 public:
  using Parent = simplex::Bus;
  td::actor::ActorId<TraceSink> trace_sink;
  td::actor::ActorId<TestOverlay> overlay;
  const TestMessageFilters* message_filters = nullptr;
};

class TestOverlayNode;

// Simulated network connecting all validator nodes. Supports configurable
// latency, packet loss, per-node disable, and message filtering.
class TestOverlay : public td::actor::Actor {
 public:
  TestOverlay(td::actor::ActorId<TraceSink> trace_sink, const TestConfig& config, const TestMessageFilters& filters)
      : trace_sink_(trace_sink), config_(config), filters_(filters) {
  }

  void register_node(size_t idx, td::actor::ActorId<TestOverlayNode> node);
  void unregister_node(size_t idx);
  void set_node_disabled(size_t idx, bool value);

  td::actor::Task<> send_message(PeerValidator src, size_t dst_idx, td::BufferSlice message);
  td::actor::Task<> send_candidate(PeerValidator src, size_t dst_idx, CandidateRef candidate);
  td::actor::Task<td::BufferSlice> send_query(PeerValidator src, size_t dst_idx, td::BufferSlice message);

 private:
  struct NodeSlot {
    td::actor::ActorId<TestOverlayNode> actor;
    bool disabled = false;
    td::uint64 generation = 0;
  };
  std::vector<NodeSlot> nodes_;
  td::actor::ActorId<TraceSink> trace_sink_;
  const TestConfig& config_;
  const TestMessageFilters& filters_;

  NodeSlot& get_node(size_t idx);
  td::actor::Task<> before_receive(size_t src_idx, size_t dst_idx, bool no_loss);
};

// Per-node overlay adapter that sits on the Bus and handles outgoing messages.
class TestOverlayNode : public td::actor::SpawnsWith<simplex::Bus>, public td::actor::ConnectsTo<simplex::Bus> {
 public:
  using BusHandle = simplex::BusHandle;
  TON_RUNTIME_DEFINE_EVENT_HANDLER();

  void start_up() override;
  void tear_down() override;

  // Called by TestOverlay to deliver incoming messages.
  void receive_message(PeerValidator src, td::BufferSlice data);
  void receive_candidate(CandidateRef candidate);
  td::actor::Task<td::BufferSlice> receive_query(PeerValidator src, td::BufferSlice query);

  // Bus event handlers (template specializations defined in network.cpp).
  template <>
  void handle(BusHandle, std::shared_ptr<const StopRequested>);
  template <>
  void handle(BusHandle bus, std::shared_ptr<const OutgoingProtocolMessage> message);
  template <>
  void handle(BusHandle bus, std::shared_ptr<const CandidateGenerated> event);
  template <>
  td::actor::Task<ProtocolMessage> process(BusHandle bus, std::shared_ptr<OutgoingOverlayRequest> message);

 private:
  td::actor::ActorId<TestOverlay> overlay_;

  td::actor::Task<> process_query_timeout(std::shared_ptr<OutgoingOverlayRequest> message,
                                          std::shared_ptr<td::Promise<ProtocolMessage>> promise);
  td::actor::Task<> process_query_send(BusHandle bus, std::shared_ptr<OutgoingOverlayRequest> message,
                                       std::shared_ptr<td::Promise<ProtocolMessage>> promise, size_t dst_node_idx);
};

// Protocol message decode/filter helpers (used by network layer).
std::optional<ProtocolVoteSent> try_decode_protocol_vote(const TestSimplexBus& bus, size_t src_node_idx,
                                                         size_t dst_node_idx, td::Slice data);
std::optional<ProtocolCertificateSent> try_decode_protocol_certificate(const TestSimplexBus& bus, size_t src_node_idx,
                                                                       size_t dst_node_idx, td::Slice data);
OverlayRequestSent decode_overlay_request(size_t src_node_idx, size_t dst_node_idx, td::Timestamp timeout,
                                          td::Slice data);

bool should_drop_protocol_message(const TestSimplexBus& bus, const TestMessageFilters& filters, size_t src_node_idx,
                                  size_t dst_node_idx, td::Slice data);
bool should_drop_candidate_delivery(const TestMessageFilters& filters, size_t src_node_idx, size_t dst_node_idx,
                                    const CandidateRef& candidate);
size_t rewrite_candidate_request_destination(const TestMessageFilters& filters, size_t src_node_idx,
                                             size_t dst_node_idx, td::Slice data);

struct TamperedCandidateResponse {
  CandidateId id;
  td::BufferSlice data;
};

std::optional<TamperedCandidateResponse> maybe_make_malformed_candidate_response(const TestMessageFilters& filters,
                                                                                 size_t requester_node_idx,
                                                                                 size_t responder_node_idx,
                                                                                 td::Slice request_data,
                                                                                 td::Slice response_data);

}  // namespace ton::validator::consensus::test
