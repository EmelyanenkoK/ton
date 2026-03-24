/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "ed25519_batch_verifier.h"

namespace ton::validator::consensus {

#if TON_USE_ZEBRA_CONSENSUS_BATCH
namespace {

struct TonConsensusEd25519Batch {
  const unsigned char* message_ptr;
  size_t message_len;
  const unsigned char* public_keys_ptr;
  const unsigned char* signatures_ptr;
  size_t item_count;
  unsigned char* validity_ptr;
};

enum class BatchVerifyStatus : td::uint32 {
  AllValid = 0,
  HasInvalid = 1,
  InvalidArgument = 2,
  Panic = 3,
};

extern "C" td::uint32 ton_consensus_ed25519_batch_verify(const TonConsensusEd25519Batch* batch);

}  // namespace
#endif

bool ed25519_batch_verifier_enabled() {
#if TON_USE_ZEBRA_CONSENSUS_BATCH
  return true;
#else
  return false;
#endif
}

td::Result<std::vector<td::uint8>> verify_ed25519_batch(td::Slice message,
                                                        const std::vector<Ed25519BatchVerifierItem>& items) {
#if TON_USE_ZEBRA_CONSENSUS_BATCH
  if (items.empty()) {
    return std::vector<td::uint8>{};
  }

  std::vector<td::uint8> public_keys(items.size() * 32);
  std::vector<td::uint8> signatures(items.size() * 64);
  std::vector<td::uint8> validity(items.size(), 0);

  for (size_t i = 0; i < items.size(); ++i) {
    if (items[i].signature.size() != 64) {
      return td::Status::Error("Unexpected ed25519 signature length");
    }

    td::MutableSlice(public_keys.data() + i * 32, 32).copy_from(items[i].public_key.as_slice());
    td::MutableSlice(signatures.data() + i * 64, 64).copy_from(items[i].signature);
  }

  TonConsensusEd25519Batch batch{
      .message_ptr = reinterpret_cast<const unsigned char*>(message.ubegin()),
      .message_len = message.size(),
      .public_keys_ptr = public_keys.data(),
      .signatures_ptr = signatures.data(),
      .item_count = items.size(),
      .validity_ptr = validity.data(),
  };

  auto status = static_cast<BatchVerifyStatus>(ton_consensus_ed25519_batch_verify(&batch));
  switch (status) {
    case BatchVerifyStatus::AllValid:
    case BatchVerifyStatus::HasInvalid:
      return validity;
    case BatchVerifyStatus::InvalidArgument:
      return td::Status::Error("ed25519-zebra batch verifier rejected the batch arguments");
    case BatchVerifyStatus::Panic:
      return td::Status::Error("ed25519-zebra batch verifier panicked");
  }
  return td::Status::Error("ed25519-zebra batch verifier returned an unknown status");
#else
  return td::Status::Error("ed25519-zebra batch verifier is not enabled");
#endif
}

}  // namespace ton::validator::consensus
