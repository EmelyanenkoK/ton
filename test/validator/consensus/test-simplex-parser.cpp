/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "td/utils/tests.h"
#include "validator-session/candidate-serializer.h"

#include "test-helpers.h"

namespace ton::validator::consensus::test {
namespace {

using simplex::Certificate;
using simplex::Signed;
using simplex::Vote;

struct ParserBus : simplex::Bus {
  using Parent = simplex::Bus;
  using Events = td::TypeList<>;
};

class SimplexParserTest : public td::Test {
 protected:
  virtual TestOptions options() const {
    return TestOptions{.weight_distribution = {1, 1, 1, 1}};
  }

  void run() final {
    ctx_ = std::make_unique<ValidatorSetup>(options());
    fill_simplex_bus(*ctx_, bus_, 0);
    run_test();
    ctx_.reset();
  }

  ValidatorSetup& ctx() {
    CHECK(ctx_ != nullptr);
    return *ctx_;
  }

  ParserBus& bus() {
    return bus_;
  }

  virtual void run_test() = 0;

 private:
  std::unique_ptr<ValidatorSetup> ctx_;
  ParserBus bus_;
};

template <typename VoteTlT>
td::BufferSlice sign_vote_tl(const ValidatorSetup& setup, const consensus::Bus& bus, size_t idx,
                             const tl_object_ptr<VoteTlT>& vote) {
  auto serialized_vote = serialize_tl_object(vote, true);
  return sign_consensus_payload(setup, bus, idx, serialized_vote.as_slice());
}

template <typename VoteTlT>
simplex::tl::VoteSignatureRef make_vote_signature(const ValidatorSetup& setup, const consensus::Bus& bus, size_t idx,
                                                  const tl_object_ptr<VoteTlT>& vote) {
  return create_tl_object<simplex::tl::voteSignature>(idx, sign_vote_tl(setup, bus, idx, vote));
}

template <typename VoteTlT>
td::Result<td::Ref<Certificate<Vote>>> parse_certificate(const ValidatorSetup& setup, const ParserBus& bus,
                                                         tl_object_ptr<VoteTlT> vote_tl,
                                                         std::vector<simplex::tl::VoteSignatureRef> signatures) {
  auto cert = create_tl_object<simplex::tl::certificate>(std::move(vote_tl),
                                                         create_tl_object<simplex::tl::voteSignatureSet>(
                                                             std::move(signatures)));
  return Certificate<Vote>::from_tl(std::move(*cert), bus);
}

BlockCandidate make_valid_serializable_block_candidate(ShardIdFull shard, BlockSeqno seqno) {
  auto data_cell = create_cell(0xCC000000 + seqno);
  auto collated_cell = create_cell(0xDD000000 + seqno);
  auto data = vm::std_boc_serialize(data_cell, 31).move_as_ok();
  auto collated_data = vm::std_boc_serialize(collated_cell, 31).move_as_ok();
  BlockIdExt id(BlockId(shard, seqno), td::Bits256(data_cell->get_hash().bits()), td::sha256_bits256(data));
  return BlockCandidate(Ed25519_PublicKey(bits256_pattern(42)), id, td::sha256_bits256(collated_data),
                        std::move(data), std::move(collated_data));
}

td::BufferSlice make_full_candidate_broadcast_with_nonzero_src(const ValidatorSetup& setup, const ParserBus& bus,
                                                               td::uint32 slot, ParentId parent,
                                                               const BlockCandidate& block,
                                                               PeerValidatorId leader) {
  auto candidate_tl = create_tl_object<ton_api::validatorSession_candidate>(
      bits256_pattern(999), block.id.seqno(), block.id.root_hash, block.data.clone(), block.collated_data.clone());
  auto serialized_candidate = validatorsession::serialize_candidate(candidate_tl, true).move_as_ok();

  auto hash_data = CandidateHashData::create_full(block, parent);
  auto id = hash_data.build_id_with(slot);
  auto serialized_id = serialize_tl_object(id.to_tl(), true);
  auto signature = sign_consensus_payload(setup, bus, leader.value(), serialized_id.as_slice());

  return create_serialize_tl_object<consensus::tl::block>(slot, CandidateId::parent_id_to_tl(parent),
                                                          std::move(serialized_candidate), std::move(signature));
}

struct SignedDeserializeRejectsMalformedPayload : SimplexParserTest {
  void run_test() override {
    // Covers tracker Kernel-27 malformed-payload parsing:
    // Signed::deserialize must reject bytes that do not decode to a simplex vote object.
    auto result = Signed<Vote>::deserialize(td::Slice("not a tl vote"), PeerValidatorId{0}, bus());

    ASSERT_TRUE(result.is_error());
  }
};
REGISTER_TEST(SimplexParser, SignedDeserializeRejectsMalformedPayload);

struct SignedDeserializeRejectsWrongSignerOrCorruptSignature : SimplexParserTest {
  void run_test() override {
    // Covers tracker Kernel-27 signer/signature validation:
    // Signed::deserialize must reject a vote when the claimed signer is wrong or when the
    // signature bytes are corrupted.
    auto signed_vote = make_signed_simplex_vote(ctx(), bus(), 1, simplex::SkipVote{7});
    auto serialized_vote = signed_vote.serialize();

    auto wrong_signer = Signed<Vote>::deserialize(serialized_vote.as_slice(), PeerValidatorId{0}, bus());
    ASSERT_TRUE(wrong_signer.is_error());

    auto vote_tl = fetch_tl_object<simplex::tl::vote>(serialized_vote.clone(), true).move_as_ok();
    vote_tl->signature_ = td::BufferSlice("corrupt");
    auto corrupted = Signed<Vote>::from_tl(std::move(*vote_tl), PeerValidatorId{1}, bus());
    ASSERT_TRUE(corrupted.is_error());
  }
};
REGISTER_TEST(SimplexParser, SignedDeserializeRejectsWrongSignerOrCorruptSignature);

struct CertificateFromTlRejectsDuplicateSigners : SimplexParserTest {
  void run_test() override {
    // Covers tracker Kernel-27 duplicate signer handling:
    // Certificate::from_tl must reject the same validator index appearing twice in one cert.
    auto vote_tl = create_tl_object<simplex::tl::skipVote>(5);
    std::vector<simplex::tl::VoteSignatureRef> signatures;
    signatures.push_back(make_vote_signature(ctx(), bus(), 0, vote_tl));
    signatures.push_back(make_vote_signature(ctx(), bus(), 0, vote_tl));
    signatures.push_back(make_vote_signature(ctx(), bus(), 1, vote_tl));

    auto result = parse_certificate(ctx(), bus(), std::move(vote_tl), std::move(signatures));

    ASSERT_TRUE(result.is_error());
    EXPECT(result.error().message().str().find("Duplicate validator index") != std::string::npos);
  }
};
REGISTER_TEST(SimplexParser, CertificateFromTlRejectsDuplicateSigners);

struct CertificateFromTlRejectsMixedStatements : SimplexParserTest {
  void run_test() override {
    // Covers tracker Kernel-27 mixed-statement rejection.
    // In the current on-wire format the certificate carries a single shared statement, so the
    // parser-owned equivalent is rejecting signatures that were produced for another statement type.
    auto notar_vote_tl = create_tl_object<simplex::tl::notarizeVote>(make_candidate_id(9, 9009).to_tl());
    auto final_vote_tl = create_tl_object<simplex::tl::finalizeVote>(make_candidate_id(9, 9009).to_tl());

    std::vector<simplex::tl::VoteSignatureRef> signatures;
    signatures.push_back(make_vote_signature(ctx(), bus(), 0, final_vote_tl));
    signatures.push_back(make_vote_signature(ctx(), bus(), 1, final_vote_tl));
    signatures.push_back(make_vote_signature(ctx(), bus(), 2, final_vote_tl));

    auto result = parse_certificate(ctx(), bus(), std::move(notar_vote_tl), std::move(signatures));

    ASSERT_TRUE(result.is_error());
    EXPECT(result.error().message().str().find("Invalid vote signature") != std::string::npos);
  }
};
REGISTER_TEST(SimplexParser, CertificateFromTlRejectsMixedStatements);

struct CertificateFromTlRejectsUnderweightQuorum : SimplexParserTest {
  void run_test() override {
    // Covers tracker Kernel-27 quorum accounting:
    // a certificate below the >2/3 quorum threshold must be rejected even if every included
    // signature is individually valid.
    auto vote_tl = create_tl_object<simplex::tl::skipVote>(8);
    std::vector<simplex::tl::VoteSignatureRef> signatures;
    signatures.push_back(make_vote_signature(ctx(), bus(), 0, vote_tl));
    signatures.push_back(make_vote_signature(ctx(), bus(), 1, vote_tl));

    auto result = parse_certificate(ctx(), bus(), std::move(vote_tl), std::move(signatures));

    ASSERT_TRUE(result.is_error());
    EXPECT(result.error().message().str().find("Not enough signatures") != std::string::npos);
  }
};
REGISTER_TEST(SimplexParser, CertificateFromTlRejectsUnderweightQuorum);

struct CertificateFromTlRejectsOversizedWindowSlot : SimplexParserTest {
  void run_test() override {
    // Covers tracker Kernel-27 and Kernel-31 commit 4c5eda7cb:
    // malformed or oversized window-slot values must be rejected at parse time rather than being
    // silently reinterpreted through uint32 casts.
    auto vote_tl = create_tl_object<simplex::tl::skipVote>(static_cast<td::int32>(-1));
    std::vector<simplex::tl::VoteSignatureRef> signatures;
    signatures.push_back(make_vote_signature(ctx(), bus(), 0, vote_tl));
    signatures.push_back(make_vote_signature(ctx(), bus(), 1, vote_tl));
    signatures.push_back(make_vote_signature(ctx(), bus(), 2, vote_tl));

    auto result = parse_certificate(ctx(), bus(), std::move(vote_tl), std::move(signatures));

    ASSERT_TRUE(result.is_error());
  }
};
REGISTER_TEST(SimplexParser, CertificateFromTlRejectsOversizedWindowSlot);

struct CertificateFromTlAcceptsValidBoundaryCases : SimplexParserTest {
  void run_test() override {
    // Covers tracker Kernel-27 valid boundaries:
    // Certificate::from_tl must accept a certificate at the exact quorum threshold and with the
    // highest validator index participating.
    auto id = make_candidate_id(12, 1212);
    auto vote_tl = create_tl_object<simplex::tl::finalizeVote>(id.to_tl());
    std::vector<simplex::tl::VoteSignatureRef> signatures;
    signatures.push_back(make_vote_signature(ctx(), bus(), 1, vote_tl));
    signatures.push_back(make_vote_signature(ctx(), bus(), 2, vote_tl));
    signatures.push_back(make_vote_signature(ctx(), bus(), 3, vote_tl));

    auto result = parse_certificate(ctx(), bus(), std::move(vote_tl), std::move(signatures));

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.ok()->signatures.size(), static_cast<size_t>(3));
    auto* final_vote = std::get_if<simplex::FinalizeVote>(&result.ok()->vote.vote);
    ASSERT_TRUE(final_vote != nullptr);
    EXPECT_EQ(final_vote->id, id);
  }
};
REGISTER_TEST(SimplexParser, CertificateFromTlAcceptsValidBoundaryCases);

struct CandidateDeserializeRejectsWrongLeaderSource : SimplexParserTest {
  void run_test() override {
    // Covers tracker Kernel-23 / Kernel-30 parser-owned wrong-leader behavior:
    // Candidate::deserialize must reject a candidate when the network source is not the expected
    // leader for the slot, before the block validator sees it.
    td::uint32 slot = 0;
    auto expected_leader = bus().collator_schedule->expected_collator_for(slot);
    auto wrong_source = PeerValidatorId{(expected_leader.value() + 1) % bus().validator_set.size()};
    auto candidate =
        make_serializable_empty_candidate(ctx(), bus(), slot, make_candidate_id(0, 1300), min_mc_block_id,
                                          expected_leader);

    auto result = Candidate::deserialize(candidate->serialize().as_slice(), bus(), wrong_source);

    ASSERT_TRUE(result.is_error());
    EXPECT(result.error().message().str().find("expected leader") != std::string::npos);
  }
};
REGISTER_TEST(SimplexParser, CandidateDeserializeRejectsWrongLeaderSource);

struct CandidateDeserializeRejectsMalformedCandidateBody : SimplexParserTest {
  void run_test() override {
    // Covers tracker Kernel-23 parser-owned malformed-structure behavior:
    // Candidate::deserialize must reject a block candidate broadcast with a non-null src field in
    // the embedded validator-session candidate.
    td::uint32 slot = 0;
    auto leader = bus().collator_schedule->expected_collator_for(slot);
    auto block = make_valid_serializable_block_candidate(bus().shard, 5);
    auto data =
        make_full_candidate_broadcast_with_nonzero_src(ctx(), bus(), slot, ParentId{}, block, leader);

    auto result = Candidate::deserialize(data.as_slice(), bus(), leader);

    ASSERT_TRUE(result.is_error());
    EXPECT(result.error().message().str().find("src field") != std::string::npos);
  }
};
REGISTER_TEST(SimplexParser, CandidateDeserializeRejectsMalformedCandidateBody);

}  // namespace
}  // namespace ton::validator::consensus::test
