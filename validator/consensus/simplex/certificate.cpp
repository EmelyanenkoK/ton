/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "td/utils/overloaded.h"
#include "validator/consensus/bus.h"
#include "validator/consensus/ed25519_batch_verifier.h"

#include "certificate.h"

namespace ton::validator::consensus::simplex {

template <ValidVote T>
td::Result<td::Ref<Certificate<T>>> Certificate<T>::from_tl(tl::voteSignatureSet&& set, T vote, const Bus& bus) {
  auto vote_to_sign = serialize_tl_object(vote.to_tl(), true);

  std::vector<bool> voted(bus.validator_set.size(), false);
  std::vector<VoteSignature> signatures;
  ValidatorWeight voted_weight = 0;

#if TON_USE_ZEBRA_CONSENSUS_BATCH
  struct PendingVoteSignature {
    const PeerValidator* validator;
    td::Bits256 public_key;
    td::BufferSlice signature;
  };

  std::vector<PendingVoteSignature> pending_signatures;
  pending_signatures.reserve(set.votes_.size());

  for (auto& signature : set.votes_) {
    auto who = static_cast<td::uint32>(signature->who_);
    if (who >= bus.validator_set.size()) {
      return td::Status::Error(PSTRING() << "Invalid validator index " << who << " in certificate");
    }
    if (voted[who]) {
      return td::Status::Error(PSTRING() << "Duplicate validator index " << who << " in certificate");
    }
    voted[who] = true;

    const auto& validator = PeerValidatorId{who}.get_using(bus);
    if (!validator.key.is_ed25519()) {
      return td::Status::Error(PSTRING() << "Non-Ed25519 validator key in Simplex certificate for " << validator);
    }

    pending_signatures.push_back(PendingVoteSignature{
        .validator = &validator,
        .public_key = validator.key.ed25519_value().raw(),
        .signature = std::move(signature->signature_),
    });
    voted_weight += validator.weight;
  }

  std::vector<Ed25519BatchVerifierItem> batch_items;
  batch_items.reserve(pending_signatures.size());
  for (const auto& pending : pending_signatures) {
    batch_items.push_back(Ed25519BatchVerifierItem{
        .public_key = pending.public_key,
        .signature = pending.signature.as_slice(),
    });
  }

  auto signed_data = create_serialize_tl_object<consensus::tl::dataToSign>(bus.session_id, vote_to_sign.clone());
  auto validity = verify_ed25519_batch(signed_data.as_slice(), batch_items);
  if (validity.is_error()) {
    for (auto& pending : pending_signatures) {
      if (!pending.validator->check_signature(bus.session_id, vote_to_sign, pending.signature.as_slice())) {
        return td::Status::Error(PSTRING() << "Invalid vote signature for " << *pending.validator);
      }
    }
  } else {
    auto bitmap = validity.move_as_ok();
    for (size_t i = 0; i < bitmap.size(); ++i) {
      if (bitmap[i] == 0) {
        return td::Status::Error(PSTRING() << "Invalid vote signature for " << *pending_signatures[i].validator);
      }
    }
  }

  for (auto& pending : pending_signatures) {
    signatures.emplace_back(VoteSignature{pending.validator->idx, std::move(pending.signature)});
  }
#else
  for (auto& signature : set.votes_) {
    auto who = static_cast<td::uint32>(signature->who_);
    if (who >= bus.validator_set.size()) {
      return td::Status::Error(PSTRING() << "Invalid validator index " << who << " in certificate");
    }
    if (voted[who]) {
      return td::Status::Error(PSTRING() << "Duplicate validator index " << who << " in certificate");
    }
    voted[who] = true;

    auto validator = PeerValidatorId{who}.get_using(bus);
    if (!validator.check_signature(bus.session_id, vote_to_sign, signature->signature_)) {
      return td::Status::Error(PSTRING() << "Invalid vote signature for " << validator);
    }
    signatures.emplace_back(VoteSignature{validator.idx, std::move(signature->signature_)});
    voted_weight += validator.weight;
  }
#endif

  if (voted_weight < (bus.total_weight * 2) / 3 + 1) {
    return td::Status::Error("Not enough signatures in certificate");
  }

  return td::make_ref<Certificate<T>>(std::move(vote), std::move(signatures));
}

template <ValidVote T>
td::Result<td::Ref<Certificate<Vote>>> Certificate<T>::from_tl(tl::certificate&& cert, const Bus& bus)
  requires std::same_as<T, Vote>
{
  auto vote_to_sign = serialize_tl_object(cert.vote_, true);
  auto vote = Vote::from_tl(std::move(*cert.vote_));
  return from_tl(std::move(*cert.signatures_), std::move(vote), bus);
}

template <ValidVote T>
td::CntObject* Certificate<T>::make_copy() const {
  std::vector<VoteSignature> copied_signatures;
  for (const auto& sig : signatures) {
    copied_signatures.emplace_back(VoteSignature{sig.validator, sig.signature.clone()});
  }
  return new Certificate<T>(vote, std::move(copied_signatures));
}

template <ValidVote T>
tl::VoteSignatureSetRef Certificate<T>::to_tl_vote_signature_set() const {
  std::vector<tl::VoteSignatureRef> tl_sigs;
  for (const auto& [validator, signature] : signatures) {
    tl_sigs.push_back(create_tl_object<tl::voteSignature>(validator.value(), signature.clone()));
  }
  return create_tl_object<tl::voteSignatureSet>(std::move(tl_sigs));
}

template <ValidVote T>
tl::CertificateRef Certificate<T>::to_tl() const {
  return create_tl_object<tl::certificate>(vote.to_tl(), to_tl_vote_signature_set());
}

template <ValidVote T>
td::BufferSlice Certificate<T>::serialize() const {
  return serialize_tl_object(to_tl(), true);
}

template <ValidVote T>
td::Ref<block::BlockSignatureSet> Certificate<T>::to_signature_set(const CandidateRef& candidate, const Bus& bus) const
  requires td::OneOf<T, NotarizeVote, FinalizeVote>
{
  CHECK(candidate->id == vote.id);

  std::vector<ton::BlockSignature> block_signatures;
  for (const auto& [validator, signature] : signatures) {
    block_signatures.emplace_back(validator.get_using(bus).short_id.bits256_value(), signature.clone());
  }

  auto fn = block::BlockSignatureSet::create_simplex_approve;
  if constexpr (std::same_as<T, FinalizeVote>) {
    fn = block::BlockSignatureSet::create_simplex;
  }
  return fn(std::move(block_signatures), bus.cc_seqno, bus.validator_set_hash, bus.session_id, vote.id.slot,
            candidate->hash_data().to_tl());
}

template <ValidVote T>
td::Ref<Certificate<Vote>> Certificate<T>::consume_and_upcast() &&
  requires(!std::same_as<T, Vote>)
{
  std::vector<Certificate<Vote>::VoteSignature> casted_signatures;
  for (auto& sig : signatures) {
    casted_signatures.emplace_back(sig.validator, std::move(sig.signature));
  }
  return td::make_ref<Certificate<Vote>>(vote, std::move(casted_signatures));
}

template struct Certificate<NotarizeVote>;
template struct Certificate<SkipVote>;
template struct Certificate<FinalizeVote>;
template struct Certificate<Vote>;

}  // namespace ton::validator::consensus::simplex
