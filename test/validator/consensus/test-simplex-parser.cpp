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

template <typename VoteTlT>
simplex::tl::VoteSignatureRef make_vote_signature(const ValidatorSetup& setup, const consensus::Bus& bus, size_t idx,
                                                  const tl_object_ptr<VoteTlT>& vote) {
  auto serialized_vote = serialize_tl_object(vote, true);
  auto signature = sign_consensus_payload(setup, bus, idx, serialized_vote.as_slice());
  return create_tl_object<simplex::tl::voteSignature>(static_cast<td::int32>(idx), std::move(signature));
}

template <typename VoteTlT>
td::Result<td::Ref<Certificate<Vote>>> parse_certificate(const ValidatorSetup& setup, const ParserBus& bus,
                                                         tl_object_ptr<VoteTlT> vote_tl,
                                                         std::vector<simplex::tl::VoteSignatureRef> signatures) {
  auto cert = create_tl_object<simplex::tl::certificate>(
      std::move(vote_tl), create_tl_object<simplex::tl::voteSignatureSet>(std::move(signatures)));
  return Certificate<Vote>::from_tl(std::move(*cert), bus);
}

BlockCandidate make_valid_serializable_block_candidate(ShardIdFull shard, BlockSeqno seqno) {
  auto data_cell = create_cell(0xCC000000 + seqno);
  auto collated_cell = create_cell(0xDD000000 + seqno);
  auto data = vm::std_boc_serialize(data_cell, 31).move_as_ok();
  auto collated_data = vm::std_boc_serialize(collated_cell, 31).move_as_ok();
  BlockIdExt id(BlockId(shard, seqno), td::Bits256(data_cell->get_hash().bits()), td::sha256_bits256(data));
  return BlockCandidate(Ed25519_PublicKey(bits256_pattern(42)), id, td::sha256_bits256(collated_data), std::move(data),
                        std::move(collated_data));
}

TEST(SimplexParser, SignedDeserializeRejectsMalformedPayload) {
  ValidatorSetup setup(TestOptions{.weight_distribution = {1, 1, 1, 1}});
  ParserBus bus;
  fill_simplex_bus(setup, bus, 0);

  auto result = Signed<Vote>::deserialize(td::Slice("not a tl vote"), PeerValidatorId{0}, bus);
  ASSERT_TRUE(result.is_error());
}

TEST(SimplexParser, SignedDeserializeRejectsWrongSignerOrCorruptSignature) {
  ValidatorSetup setup(TestOptions{.weight_distribution = {1, 1, 1, 1}});
  ParserBus bus;
  fill_simplex_bus(setup, bus, 0);

  auto signed_vote = make_signed_simplex_vote(setup, bus, 1, simplex::SkipVote{7});
  auto serialized_vote = signed_vote.serialize();

  auto wrong_signer = Signed<Vote>::deserialize(serialized_vote.as_slice(), PeerValidatorId{0}, bus);
  ASSERT_TRUE(wrong_signer.is_error());

  auto vote_tl = fetch_tl_object<simplex::tl::vote>(serialized_vote.clone(), true).move_as_ok();
  vote_tl->signature_ = td::BufferSlice("corrupt");
  auto corrupted = Signed<Vote>::from_tl(std::move(*vote_tl), PeerValidatorId{1}, bus);
  ASSERT_TRUE(corrupted.is_error());
}

TEST(SimplexParser, CertificateFromTlRejectsDuplicateSigners) {
  ValidatorSetup setup(TestOptions{.weight_distribution = {1, 1, 1, 1}});
  ParserBus bus;
  fill_simplex_bus(setup, bus, 0);

  auto vote_tl = create_tl_object<simplex::tl::skipVote>(5);
  std::vector<simplex::tl::VoteSignatureRef> signatures;
  signatures.push_back(make_vote_signature(setup, bus, 0, vote_tl));
  signatures.push_back(make_vote_signature(setup, bus, 0, vote_tl));
  signatures.push_back(make_vote_signature(setup, bus, 1, vote_tl));

  auto result = parse_certificate(setup, bus, std::move(vote_tl), std::move(signatures));
  ASSERT_TRUE(result.is_error());
}

TEST(SimplexParser, CertificateFromTlRejectsMixedStatements) {
  ValidatorSetup setup(TestOptions{.weight_distribution = {1, 1, 1, 1}});
  ParserBus bus;
  fill_simplex_bus(setup, bus, 0);

  auto notar_vote_tl = create_tl_object<simplex::tl::notarizeVote>(make_candidate_id(9, 9009).to_tl());
  auto final_vote_tl = create_tl_object<simplex::tl::finalizeVote>(make_candidate_id(9, 9009).to_tl());

  std::vector<simplex::tl::VoteSignatureRef> signatures;
  signatures.push_back(make_vote_signature(setup, bus, 0, final_vote_tl));
  signatures.push_back(make_vote_signature(setup, bus, 1, final_vote_tl));
  signatures.push_back(make_vote_signature(setup, bus, 2, final_vote_tl));

  auto result = parse_certificate(setup, bus, std::move(notar_vote_tl), std::move(signatures));
  ASSERT_TRUE(result.is_error());
}

TEST(SimplexParser, CertificateFromTlRejectsUnderweightQuorum) {
  ValidatorSetup setup(TestOptions{.weight_distribution = {1, 1, 1, 1}});
  ParserBus bus;
  fill_simplex_bus(setup, bus, 0);

  auto vote_tl = create_tl_object<simplex::tl::skipVote>(8);
  std::vector<simplex::tl::VoteSignatureRef> signatures;
  signatures.push_back(make_vote_signature(setup, bus, 0, vote_tl));
  signatures.push_back(make_vote_signature(setup, bus, 1, vote_tl));

  auto result = parse_certificate(setup, bus, std::move(vote_tl), std::move(signatures));
  ASSERT_TRUE(result.is_error());
}

TEST(SimplexParser, CertificateFromTlAcceptsValidBoundaryCases) {
  ValidatorSetup setup(TestOptions{.weight_distribution = {1, 1, 1, 1}});
  ParserBus bus;
  fill_simplex_bus(setup, bus, 0);

  auto id = make_candidate_id(12, 1212);
  auto vote_tl = create_tl_object<simplex::tl::finalizeVote>(id.to_tl());
  std::vector<simplex::tl::VoteSignatureRef> signatures;
  signatures.push_back(make_vote_signature(setup, bus, 1, vote_tl));
  signatures.push_back(make_vote_signature(setup, bus, 2, vote_tl));
  signatures.push_back(make_vote_signature(setup, bus, 3, vote_tl));

  auto result = parse_certificate(setup, bus, std::move(vote_tl), std::move(signatures));
  ASSERT_TRUE(result.is_ok());
  ASSERT_EQ(result.ok()->signatures.size(), static_cast<size_t>(3));
  auto* final_vote = std::get_if<simplex::FinalizeVote>(&result.ok()->vote.vote);
  ASSERT_TRUE(final_vote != nullptr);
  EXPECT_EQ(final_vote->id, id);
}

TEST(SimplexParser, CandidateDeserializeRejectsWrongLeaderSource) {
  ValidatorSetup setup(TestOptions{.weight_distribution = {1, 1, 1, 1}});
  ParserBus bus;
  fill_simplex_bus(setup, bus, 0);

  td::uint32 slot = 0;
  auto expected_leader = bus.collator_schedule->expected_collator_for(slot);
  auto wrong_source = PeerValidatorId{(expected_leader.value() + 1) % bus.validator_set.size()};
  auto candidate =
      make_serializable_empty_candidate(setup, bus, slot, make_candidate_id(0, 1300), min_mc_block_id, expected_leader);

  auto result = Candidate::deserialize(candidate->serialize().as_slice(), bus, wrong_source);
  ASSERT_TRUE(result.is_error());
}

// Pass a valid candidate with a mismatched expected_slot to prove the new slot
// authentication gate rejects it during deserialize.
TEST(SimplexParser, CandidateDeserializeRejectsUnexpectedExpectedSlot) {
  ValidatorSetup setup(TestOptions{.weight_distribution = {1, 1, 1, 1}});
  ParserBus bus;
  fill_simplex_bus(setup, bus, 0);

  td::uint32 slot = 0;
  auto leader = bus.collator_schedule->expected_collator_for(slot);
  auto candidate = make_serializable_empty_candidate(setup, bus, slot, make_candidate_id(0, 1400), min_mc_block_id,
                                                     leader);

  auto result = Candidate::deserialize(candidate->serialize().as_slice(), bus, leader, slot + 1);
  ASSERT_TRUE(result.is_error());
}

// Use the same deserialize path with the matching expected_slot to show valid
// candidates still pass the slot check.
TEST(SimplexParser, CandidateDeserializeAcceptsExpectedSlot) {
  ValidatorSetup setup(TestOptions{.weight_distribution = {1, 1, 1, 1}});
  ParserBus bus;
  fill_simplex_bus(setup, bus, 0);

  td::uint32 slot = 0;
  auto leader = bus.collator_schedule->expected_collator_for(slot);
  auto candidate = make_serializable_empty_candidate(setup, bus, slot, make_candidate_id(0, 1500), min_mc_block_id,
                                                     leader);

  auto result = Candidate::deserialize(candidate->serialize().as_slice(), bus, leader, slot);
  ASSERT_TRUE(result.is_ok());
  EXPECT_EQ(result.ok()->id, candidate->id);
  EXPECT_EQ(result.ok()->leader, leader);
}

TEST(SimplexParser, CandidateDeserializeRejectsMalformedCandidateBody) {
  ValidatorSetup setup(TestOptions{.weight_distribution = {1, 1, 1, 1}});
  ParserBus bus;
  fill_simplex_bus(setup, bus, 0);

  td::uint32 slot = 0;
  auto leader = bus.collator_schedule->expected_collator_for(slot);
  auto block = make_valid_serializable_block_candidate(bus.shard, 5);

  auto candidate_tl = create_tl_object<ton_api::validatorSession_candidate>(
      bits256_pattern(999), block.id.seqno(), block.id.root_hash, block.data.clone(), block.collated_data.clone());
  auto serialized_candidate = validatorsession::serialize_candidate(candidate_tl, true).move_as_ok();

  auto hash_data = CandidateHashData::create_full(block, ParentId{});
  auto id = hash_data.build_id_with(slot);
  auto serialized_id = serialize_tl_object(id.to_tl(), true);
  auto signature = sign_consensus_payload(setup, bus, leader.value(), serialized_id.as_slice());

  auto data = create_serialize_tl_object<consensus::tl::block>(slot, CandidateId::parent_id_to_tl(ParentId{}),
                                                               std::move(serialized_candidate), std::move(signature));

  auto result = Candidate::deserialize(data.as_slice(), bus, leader);
  ASSERT_TRUE(result.is_error());
}

}  // namespace
}  // namespace ton::validator::consensus::test
