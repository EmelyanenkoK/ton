/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */
#include "adnl/utils.hpp"
#include "auto/tl/ton_api.h"
#include "consensus/simplex/votes.h"
#include "consensus/utils.h"
#include "td/utils/Random.h"

#include "network.h"

namespace ton::validator::consensus::test {

// --- TestOverlay ---

TestOverlay::NodeSlot& TestOverlay::get_node(size_t idx) {
  if (nodes_.size() <= idx) {
    nodes_.resize(idx + 1);
  }
  return nodes_[idx];
}

void TestOverlay::register_node(size_t idx, td::actor::ActorId<TestOverlayNode> node) {
  auto& slot = get_node(idx);
  CHECK(slot.actor.empty());
  slot.actor = std::move(node);
}

void TestOverlay::unregister_node(size_t idx) {
  auto& slot = get_node(idx);
  CHECK(!slot.actor.empty());
  slot.actor = {};
}

void TestOverlay::set_node_disabled(size_t idx, bool value) {
  get_node(idx).disabled = value;
  if (!trace_sink_.empty()) {
    td::actor::send_closure(trace_sink_, &TraceSink::record_network_toggle, idx, NetworkToggle{.disabled = value});
  }
  LOG(ERROR) << "Node #" << idx << ": " << (value ? "disable" : "enable") << " network";
}

td::actor::Task<> TestOverlay::before_receive(size_t src_idx, size_t dst_idx, bool no_loss) {
  (void)dst_idx;
  if (get_node(src_idx).disabled) {
    co_return td::Status::Error("src is disabled");
  }
  if (!no_loss && td::Random::fast(0.0, 1.0) < config_.net_loss) {
    co_return td::Status::Error("packet lost");
  }
  co_await td::actor::coro_sleep(td::Timestamp::in(td::Random::fast(config_.net_ping.first, config_.net_ping.second)));
  co_return td::Unit{};
}

td::actor::Task<> TestOverlay::send_message(PeerValidator src, size_t dst_idx, td::BufferSlice message) {
  co_await before_receive(src.idx.value(), dst_idx, false);
  auto& node = get_node(dst_idx);
  if (!node.actor.empty() && !node.disabled) {
    td::actor::send_closure(node.actor, &TestOverlayNode::receive_message, src, std::move(message));
  }
  co_return td::Unit{};
}

td::actor::Task<> TestOverlay::send_candidate(PeerValidator src, size_t dst_idx, CandidateRef candidate) {
  co_await before_receive(src.idx.value(), dst_idx, true);
  if (should_drop_candidate_delivery(filters_, src.idx.value(), dst_idx, candidate)) {
    co_return td::Unit{};
  }
  auto& node = get_node(dst_idx);
  if (!node.actor.empty() && !node.disabled) {
    if (!trace_sink_.empty()) {
      td::actor::send_closure(trace_sink_, &TraceSink::record_candidate_delivery, src.idx.value(),
                              CandidateDelivered{
                                  .dst_node_idx = dst_idx,
                                  .candidate_id = candidate->id,
                                  .parent_id = candidate->parent_id,
                                  .leader = candidate->leader,
                                  .is_empty = candidate->is_empty(),
                                  .block_id = candidate->block_id(),
                              });
    }
    td::actor::send_closure(node.actor, &TestOverlayNode::receive_candidate, candidate);
  }
  co_return td::Unit{};
}

td::actor::Task<td::BufferSlice> TestOverlay::send_query(PeerValidator src, size_t dst_idx, td::BufferSlice message) {
  td::BufferSlice request_copy = message.clone();
  co_await before_receive(src.idx.value(), dst_idx, true);
  auto& node = get_node(dst_idx);
  if (node.actor.empty() || node.disabled) {
    co_return td::Status::Error("instance is stopped/disabled");
  }
  auto response = co_await td::actor::ask(node.actor, &TestOverlayNode::receive_query, src, std::move(message));
  co_await before_receive(dst_idx, src.idx.value(), true);
  if (auto tampered = maybe_make_malformed_candidate_response(filters_, src.idx.value(), dst_idx,
                                                              request_copy.as_slice(), response.as_slice());
      tampered.has_value()) {
    if (!trace_sink_.empty()) {
      td::actor::send_closure(trace_sink_, &TraceSink::record_malformed_response, src.idx.value(),
                              MalformedCandidateResponse{.responder_node_idx = dst_idx, .id = tampered->id});
    }
    co_return std::move(tampered->data);
  }
  co_return response;
}

// --- TestOverlayNode ---

void TestOverlayNode::start_up() {
  overlay_ = dynamic_cast<const TestSimplexBus&>(*owning_bus()).overlay;
  td::actor::send_closure(overlay_, &TestOverlay::register_node, owning_bus()->local_id.idx.value(), actor_id(this));
}

void TestOverlayNode::tear_down() {
  td::actor::send_closure(overlay_, &TestOverlay::unregister_node, owning_bus()->local_id.idx.value());
}

template <>
void TestOverlayNode::handle(BusHandle, std::shared_ptr<const StopRequested>) {
  stop();
}

template <>
void TestOverlayNode::handle(BusHandle bus, std::shared_ptr<const OutgoingProtocolMessage> message) {
  auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
  size_t src_idx = bus->local_id.idx.value();

  auto trace_message = [&](size_t dst_idx) {
    if (test_bus.trace_sink.empty()) {
      return;
    }
    if (auto vote = try_decode_protocol_vote(test_bus, src_idx, dst_idx, message->message.data.as_slice())) {
      td::actor::send_closure(test_bus.trace_sink, &TraceSink::record_protocol_vote, src_idx, std::move(*vote));
      return;
    }
    if (auto cert = try_decode_protocol_certificate(test_bus, src_idx, dst_idx, message->message.data.as_slice())) {
      td::actor::send_closure(test_bus.trace_sink, &TraceSink::record_protocol_certificate, src_idx, std::move(*cert));
    }
  };

  // Access filters through the overlay actor's reference.
  if (message->recipient.has_value()) {
    size_t dst = message->recipient->value();
    CHECK(dst != src_idx);
    if (should_drop_protocol_message(test_bus, *test_bus.message_filters, src_idx, dst,
                                     message->message.data.as_slice())) {
      return;
    }
    trace_message(dst);
    td::actor::ask(overlay_, &TestOverlay::send_message, bus->local_id, dst, message->message.data.clone())
        .detach_silent();
  } else {
    for (size_t i = 0; i < bus->validator_set.size(); ++i) {
      if (i != src_idx) {
        if (should_drop_protocol_message(test_bus, *test_bus.message_filters, src_idx, i,
                                         message->message.data.as_slice())) {
          continue;
        }
        trace_message(i);
        td::actor::ask(overlay_, &TestOverlay::send_message, bus->local_id, i, message->message.data.clone())
            .detach_silent();
      }
    }
  }
}

template <>
void TestOverlayNode::handle(BusHandle bus, std::shared_ptr<const CandidateGenerated> event) {
  size_t src_idx = bus->local_id.idx.value();
  for (size_t i = 0; i < bus->validator_set.size(); ++i) {
    if (i != src_idx) {
      td::actor::ask(overlay_, &TestOverlay::send_candidate, bus->local_id, i, event->candidate).detach_silent();
    }
  }
}

template <>
td::actor::Task<ProtocolMessage> TestOverlayNode::process(BusHandle bus,
                                                          std::shared_ptr<OutgoingOverlayRequest> message) {
  auto& test_bus = dynamic_cast<const TestSimplexBus&>(*bus);
  auto& filters = *test_bus.message_filters;
  size_t src_idx = bus->local_id.idx.value();
  size_t dst_node_idx = rewrite_candidate_request_destination(filters, src_idx, message->destination.value(),
                                                              message->request.data.as_slice());

  if (!test_bus.trace_sink.empty()) {
    auto record = decode_overlay_request(src_idx, dst_node_idx, message->timeout, message->request.data.as_slice());
    td::actor::send_closure(test_bus.trace_sink, &TraceSink::record_overlay_request, src_idx, std::move(record));
  }

  auto [task, promise] = td::actor::StartedTask<ProtocolMessage>::make_bridge();
  auto promise_ptr = std::make_shared<td::Promise<ProtocolMessage>>(std::move(promise));
  process_query_timeout(message, promise_ptr).start().detach();
  process_query_send(bus, message, promise_ptr, dst_node_idx).start().detach();
  co_return co_await std::move(task);
}

td::actor::Task<> TestOverlayNode::process_query_timeout(std::shared_ptr<OutgoingOverlayRequest> message,
                                                         std::shared_ptr<td::Promise<ProtocolMessage>> promise_ptr) {
  if (message->timeout) {
    co_await td::actor::coro_sleep(message->timeout);
    if (*promise_ptr) {
      promise_ptr->set_error(td::Status::Error(ErrorCode::timeout, "timeout"));
    }
  }
  co_return {};
}

td::actor::Task<> TestOverlayNode::process_query_send(BusHandle bus, std::shared_ptr<OutgoingOverlayRequest> message,
                                                      std::shared_ptr<td::Promise<ProtocolMessage>> promise_ptr,
                                                      size_t dst_node_idx) {
  auto r_response = co_await td::actor::ask(overlay_, &TestOverlay::send_query, bus->local_id, dst_node_idx,
                                            message->request.data.clone())
                        .wrap();
  if (r_response.is_ok() && *promise_ptr) {
    td::BufferSlice response = r_response.move_as_ok();
    if (fetch_tl_object<ton_api::consensus_requestError>(response, true).is_ok()) {
      promise_ptr->set_error(td::Status::Error("Peer returned an error"));
    } else {
      promise_ptr->set_value(ProtocolMessage{std::move(response)});
    }
  }
  co_return {};
}

void TestOverlayNode::receive_message(PeerValidator src, td::BufferSlice data) {
  owning_bus().publish<IncomingProtocolMessage>(src.idx, std::move(data));
}

void TestOverlayNode::receive_candidate(CandidateRef candidate) {
  owning_bus().publish<CandidateReceived>(candidate);
}

td::actor::Task<td::BufferSlice> TestOverlayNode::receive_query(PeerValidator src, td::BufferSlice query) {
  auto request = std::make_shared<IncomingOverlayRequest>(src.idx, std::move(query));
  auto response = co_await owning_bus().publish(std::move(request)).wrap();
  if (response.is_ok()) {
    co_return std::move(response.move_as_ok().data);
  }
  co_return create_serialize_tl_object<ton_api::consensus_requestError>();
}

// --- Protocol message decode/filter helpers ---

std::optional<ProtocolVoteSent> try_decode_protocol_vote(const TestSimplexBus& bus, size_t src_node_idx,
                                                         size_t dst_node_idx, td::Slice data) {
  (void)bus;
  auto maybe_tl_vote = fetch_tl_object<simplex::tl::vote>(data, true);
  if (maybe_tl_vote.is_error()) {
    return std::nullopt;
  }
  auto tl_vote = maybe_tl_vote.move_as_ok();
  return ProtocolVoteSent{
      .dst_node_idx = dst_node_idx,
      .vote = simplex::Vote::from_tl(std::move(*tl_vote->vote_)),
      .raw_message = data.str(),
  };
}

std::optional<ProtocolCertificateSent> try_decode_protocol_certificate(const TestSimplexBus& bus, size_t src_node_idx,
                                                                       size_t dst_node_idx, td::Slice data) {
  auto maybe_tl_certificate = fetch_tl_object<simplex::tl::certificate>(data, true);
  if (maybe_tl_certificate.is_error()) {
    return std::nullopt;
  }
  auto tl_certificate = maybe_tl_certificate.move_as_ok();
  std::vector<PeerValidatorId> signers;
  for (const auto& signature : tl_certificate->signatures_->votes_) {
    auto who = static_cast<td::uint32>(signature->who_);
    if (who < bus.validator_set.size()) {
      signers.push_back(PeerValidatorId{who});
    }
  }
  return ProtocolCertificateSent{
      .dst_node_idx = dst_node_idx,
      .vote = simplex::Vote::from_tl(std::move(*tl_certificate->vote_)),
      .signers = std::move(signers),
      .raw_message = data.str(),
  };
}

OverlayRequestSent decode_overlay_request(size_t src_node_idx, size_t dst_node_idx, td::Timestamp timeout,
                                          td::Slice data) {
  OverlayRequestSent record{
      .dst_node_idx = dst_node_idx,
      .timeout_s = timeout ? std::max(0.0, timeout.at() - td::Time::now()) : 0.0,
      .candidate_id = std::nullopt,
      .want_candidate = false,
      .want_notar = false,
  };
  if (auto maybe_request = fetch_tl_object<ton_api::consensus_simplex_requestCandidate>(data, true);
      maybe_request.is_ok()) {
    auto request = maybe_request.move_as_ok();
    record.candidate_id = CandidateId::from_tl(request->id_);
    record.want_candidate = request->want_candidate_;
    record.want_notar = request->want_notar_;
  }
  return record;
}

static std::optional<std::pair<td::uint32, TestMessageFilters::ProtocolKind>> classify_protocol_message(
    const TestSimplexBus& bus, size_t src_node_idx, size_t dst_node_idx, td::Slice data) {
  if (auto maybe_vote = try_decode_protocol_vote(bus, src_node_idx, dst_node_idx, data)) {
    auto classify = [](const auto& v) -> std::pair<td::uint32, TestMessageFilters::ProtocolKind> {
      if constexpr (std::is_same_v<std::decay_t<decltype(v)>, simplex::NotarizeVote>) {
        return {v.id.slot, TestMessageFilters::ProtocolKind::NotarVote};
      } else if constexpr (std::is_same_v<std::decay_t<decltype(v)>, simplex::SkipVote>) {
        return {v.slot, TestMessageFilters::ProtocolKind::SkipVote};
      } else {
        return {v.id.slot, TestMessageFilters::ProtocolKind::FinalVote};
      }
    };
    return std::visit(classify, maybe_vote->vote.vote);
  }
  if (auto maybe_cert = try_decode_protocol_certificate(bus, src_node_idx, dst_node_idx, data)) {
    auto classify = [](const auto& v) -> std::pair<td::uint32, TestMessageFilters::ProtocolKind> {
      if constexpr (std::is_same_v<std::decay_t<decltype(v)>, simplex::NotarizeVote>) {
        return {v.id.slot, TestMessageFilters::ProtocolKind::NotarCert};
      } else if constexpr (std::is_same_v<std::decay_t<decltype(v)>, simplex::SkipVote>) {
        return {v.slot, TestMessageFilters::ProtocolKind::SkipCert};
      } else {
        return {v.id.slot, TestMessageFilters::ProtocolKind::FinalCert};
      }
    };
    return std::visit(classify, maybe_cert->vote.vote);
  }
  return std::nullopt;
}

bool should_drop_protocol_message(const TestSimplexBus& bus, const TestMessageFilters& filters, size_t src_node_idx,
                                  size_t dst_node_idx, td::Slice data) {
  auto maybe_classified = classify_protocol_message(bus, src_node_idx, dst_node_idx, data);
  if (!maybe_classified.has_value()) {
    return false;
  }
  auto [slot, kind] = *maybe_classified;
  for (const auto& rule : filters.protocol_drop_rules) {
    if (rule.src_node_idx.has_value() && *rule.src_node_idx != src_node_idx) {
      continue;
    }
    if (!rule.dst_node_indices.contains(dst_node_idx)) {
      continue;
    }
    if (slot < rule.start_slot || slot >= rule.end_slot) {
      continue;
    }
    if (rule.kind != kind) {
      continue;
    }
    return true;
  }
  return false;
}

bool should_drop_candidate_delivery(const TestMessageFilters& filters, size_t src_node_idx, size_t dst_node_idx,
                                    const CandidateRef& candidate) {
  td::uint32 slot = candidate->id.slot;
  for (const auto& rule : filters.candidate_drop_rules) {
    if (rule.src_node_idx.has_value() && *rule.src_node_idx != src_node_idx) {
      continue;
    }
    if (!rule.dst_node_indices.contains(dst_node_idx)) {
      continue;
    }
    if (slot < rule.start_slot || slot >= rule.end_slot) {
      continue;
    }
    return true;
  }
  return false;
}

size_t rewrite_candidate_request_destination(const TestMessageFilters& filters, size_t src_node_idx,
                                             size_t dst_node_idx, td::Slice data) {
  auto maybe_request = fetch_tl_object<ton_api::consensus_simplex_requestCandidate>(data, true);
  if (maybe_request.is_error()) {
    return dst_node_idx;
  }
  if (filters.force_candidate_request_src_node != src_node_idx ||
      !filters.force_candidate_request_dst_node.has_value()) {
    return dst_node_idx;
  }
  return *filters.force_candidate_request_dst_node;
}

std::optional<TamperedCandidateResponse> maybe_make_malformed_candidate_response(const TestMessageFilters& filters,
                                                                                 size_t requester_node_idx,
                                                                                 size_t responder_node_idx,
                                                                                 td::Slice request_data,
                                                                                 td::Slice response_data) {
  auto maybe_request = fetch_tl_object<ton_api::consensus_simplex_requestCandidate>(request_data, true);
  if (maybe_request.is_error()) {
    return std::nullopt;
  }
  auto request = maybe_request.move_as_ok();

  if (filters.malformed_candidate_response_src_node != requester_node_idx ||
      filters.malformed_candidate_response_dst_node != responder_node_idx ||
      filters.malformed_candidate_response_budget == 0) {
    return std::nullopt;
  }
  // Note: budget is decremented by the caller since filters is const here.

  if (filters.malformed_candidate_response_kind == TestMessageFilters::MalformedCandidateResponseKind::Junk) {
    td::BufferSlice bad_candidate;
    td::BufferSlice bad_notar;
    if (request->want_candidate_) {
      bad_candidate = td::BufferSlice("malformed-candidate");
    } else if (request->want_notar_) {
      bad_notar = td::BufferSlice("malformed-notar");
    } else {
      return std::nullopt;
    }
    return TamperedCandidateResponse{
        .id = CandidateId::from_tl(request->id_),
        .data = serialize_tl_object(create_tl_object<ton_api::consensus_simplex_candidateAndCert>(
                                        std::move(bad_candidate), std::move(bad_notar)),
                                    true),
    };
  }

  auto maybe_response = fetch_tl_object<ton_api::consensus_simplex_candidateAndCert>(response_data, true);
  if (maybe_response.is_error()) {
    return std::nullopt;
  }
  auto response = maybe_response.move_as_ok();

  if (filters.malformed_candidate_response_kind ==
      TestMessageFilters::MalformedCandidateResponseKind::CorruptCandidate) {
    if (response->candidate_.empty()) {
      return std::nullopt;
    }
    auto maybe_candidate = fetch_tl_object<tl::CandidateData>(response->candidate_.as_slice(), true);
    if (maybe_candidate.is_error()) {
      return std::nullopt;
    }
    auto candidate = maybe_candidate.move_as_ok();
    bool corrupted = false;
    ton_api::downcast_call(*candidate, td::overloaded(
                                           [&](tl::empty& e) {
                                             if (!e.signature_.empty()) {
                                               auto sig = e.signature_.clone();
                                               sig.as_slice()[0] ^= 0x01;
                                               e.signature_ = std::move(sig);
                                               corrupted = true;
                                             }
                                           },
                                           [&](tl::block& b) {
                                             if (!b.signature_.empty()) {
                                               auto sig = b.signature_.clone();
                                               sig.as_slice()[0] ^= 0x01;
                                               b.signature_ = std::move(sig);
                                               corrupted = true;
                                             }
                                           }));
    if (!corrupted) {
      return std::nullopt;
    }
    return TamperedCandidateResponse{
        .id = CandidateId::from_tl(request->id_),
        .data = serialize_tl_object(create_tl_object<ton_api::consensus_simplex_candidateAndCert>(
                                        serialize_tl_object(candidate, true), std::move(response->notar_)),
                                    true),
    };
  }

  if (filters.malformed_candidate_response_kind == TestMessageFilters::MalformedCandidateResponseKind::CorruptNotar) {
    if (response->notar_.empty()) {
      return std::nullopt;
    }
    auto maybe_notar = fetch_tl_object<simplex::tl::voteSignatureSet>(response->notar_.as_slice(), true);
    if (maybe_notar.is_error()) {
      return std::nullopt;
    }
    auto notar = maybe_notar.move_as_ok();
    if (notar->votes_.empty() || notar->votes_[0]->signature_.empty()) {
      return std::nullopt;
    }
    auto sig = notar->votes_[0]->signature_.clone();
    sig.as_slice()[0] ^= 0x01;
    notar->votes_[0]->signature_ = std::move(sig);
    return TamperedCandidateResponse{
        .id = CandidateId::from_tl(request->id_),
        .data = serialize_tl_object(create_tl_object<ton_api::consensus_simplex_candidateAndCert>(
                                        std::move(response->candidate_), serialize_tl_object(notar, true)),
                                    true),
    };
  }

  return std::nullopt;
}

}  // namespace ton::validator::consensus::test
