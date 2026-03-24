/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <vector>

#include "adnl/adnl-node-id.hpp"
#include "consensus/simplex/bus.h"
#include "consensus/simplex/certificate.h"
#include "consensus/types.h"
#include "keys/keys.hpp"
#include "td/utils/OptionParser.h"

using namespace ton;
using namespace ton::validator;
using namespace ton::validator::consensus;
using namespace ton::validator::consensus::simplex;

namespace {

namespace simplex_tl = ton::validator::consensus::simplex::tl;

struct BenchmarkOptions {
  td::uint32 min_votes = 1;
  td::uint32 max_votes = 100;
  td::uint64 target_signature_checks = 500000;
  td::uint32 warmup_iterations = 32;
};

struct BenchmarkCase {
  td::uint32 votes = 0;
  std::unique_ptr<simplex::Bus> bus;
  td::BufferSlice serialized_certificate;
};

struct BenchmarkRow {
  td::uint32 votes = 0;
  td::uint64 iterations = 0;
  double total_ms = 0.0;
  double avg_cert_us = 0.0;
  double avg_signature_us = 0.0;
};

ValidatorSessionId make_session_id(td::uint32 votes) {
  ValidatorSessionId session_id = td::Bits256::zero();
  auto slice = td::MutableSlice{session_id.as_slice()};
  CHECK(slice.size() >= sizeof(votes));
  std::memcpy(slice.begin(), &votes, sizeof(votes));
  return session_id;
}

td::BufferSlice sign_vote(const PrivateKey& key, ValidatorSessionId session_id, const Vote& vote) {
  auto vote_to_sign = serialize_tl_object(vote.to_tl(), true);
  auto signed_data = create_serialize_tl_object<consensus::tl::dataToSign>(session_id, vote_to_sign.clone());
  auto decryptor = key.create_decryptor().move_as_ok();
  return decryptor->sign(signed_data.as_slice()).move_as_ok();
}

BenchmarkCase make_case(td::uint32 votes) {
  BenchmarkCase result;
  result.votes = votes;
  result.bus = std::make_unique<simplex::Bus>();
  result.bus->session_id = make_session_id(votes);
  result.bus->total_weight = votes;
  result.bus->validator_set.reserve(votes);

  auto vote = Vote{SkipVote{1000000U + votes}};
  std::vector<simplex_tl::VoteSignatureRef> signatures;
  signatures.reserve(votes);

  for (td::uint32 i = 0; i < votes; ++i) {
    PrivateKey private_key{privkeys::Ed25519::random()};
    PublicKey public_key = private_key.compute_public_key();
    PublicKeyHash short_id = public_key.compute_short_id();

    result.bus->validator_set.push_back(PeerValidator{
        .idx = PeerValidatorId{i},
        .key = public_key,
        .short_id = short_id,
        .adnl_id = adnl::AdnlNodeIdShort{short_id.bits256_value()},
        .weight = 1,
    });

    signatures.push_back(create_tl_object<simplex_tl::voteSignature>(
        static_cast<td::int32>(i), sign_vote(private_key, result.bus->session_id, vote)));
  }

  CHECK(!result.bus->validator_set.empty());
  result.bus->local_id = result.bus->validator_set.front();

  auto cert = create_tl_object<simplex_tl::certificate>(vote.to_tl(),
                                                        create_tl_object<simplex_tl::voteSignatureSet>(std::move(signatures)));
  result.serialized_certificate = serialize_tl_object(cert, true);
  return result;
}

void verify_certificate(const BenchmarkCase& test_case) {
  auto parsed = fetch_tl_object<simplex_tl::certificate>(test_case.serialized_certificate.as_slice(), true).move_as_ok();
  auto verified = Certificate<Vote>::from_tl(std::move(*parsed), *test_case.bus);
  CHECK(verified.is_ok());
}

BenchmarkRow run_case(const BenchmarkCase& test_case, const BenchmarkOptions& options) {
  td::uint64 iterations = std::max<td::uint64>(
      options.warmup_iterations,
      (options.target_signature_checks + test_case.votes - 1) / std::max<td::uint32>(1, test_case.votes));

  for (td::uint32 i = 0; i < options.warmup_iterations; ++i) {
    verify_certificate(test_case);
  }

  auto started_at = std::chrono::steady_clock::now();
  for (td::uint64 i = 0; i < iterations; ++i) {
    verify_certificate(test_case);
  }
  auto finished_at = std::chrono::steady_clock::now();

  auto elapsed = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(finished_at - started_at).count();
  auto avg_cert_us = elapsed * 1000.0 / static_cast<double>(iterations);
  auto avg_signature_us = avg_cert_us / static_cast<double>(test_case.votes);

  return BenchmarkRow{
      .votes = test_case.votes,
      .iterations = iterations,
      .total_ms = elapsed,
      .avg_cert_us = avg_cert_us,
      .avg_signature_us = avg_signature_us,
  };
}

BenchmarkOptions parse_options(int argc, char** argv) {
  BenchmarkOptions options;
  td::OptionParser parser;
  parser.set_description("Benchmark Simplex certificate verification for validator sets of size N");
  parser.add_option('n', "min-votes", "minimum number of votes in a certificate", [&](td::Slice value) {
    options.min_votes = td::to_integer_safe<td::uint32>(value).move_as_ok();
  });
  parser.add_option('m', "max-votes", "maximum number of votes in a certificate", [&](td::Slice value) {
    options.max_votes = td::to_integer_safe<td::uint32>(value).move_as_ok();
  });
  parser.add_option('t', "target-signature-checks", "target number of signature checks per data point",
                    [&](td::Slice value) { options.target_signature_checks = td::to_integer_safe<td::uint64>(value).move_as_ok(); });
  parser.add_option('w', "warmup-iterations", "number of warmup iterations per data point", [&](td::Slice value) {
    options.warmup_iterations = td::to_integer_safe<td::uint32>(value).move_as_ok();
  });
  parser.run(argc, argv).ensure();
  CHECK(options.min_votes >= 1);
  CHECK(options.max_votes >= options.min_votes);
  return options;
}

}  // namespace

int main(int argc, char** argv) {
  BenchmarkOptions options = parse_options(argc, argv);

  std::vector<BenchmarkRow> rows;
  rows.reserve(options.max_votes - options.min_votes + 1);

  for (td::uint32 votes = options.min_votes; votes <= options.max_votes; ++votes) {
    auto test_case = make_case(votes);
    rows.push_back(run_case(test_case, options));
  }

  std::cout << "votes,iterations,total_ms,avg_cert_us,avg_signature_us\n";
  for (const auto& row : rows) {
    std::cout << row.votes << ',' << row.iterations << ',' << row.total_ms << ',' << row.avg_cert_us << ','
              << row.avg_signature_us << '\n';
  }
  return 0;
}
