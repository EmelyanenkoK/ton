/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#pragma once

#include <vector>

#include "td/utils/Slice.h"
#include "td/utils/Status.h"
#include "ton/ton-types.h"

namespace ton::validator::consensus {

struct Ed25519BatchVerifierItem {
  td::Bits256 public_key;
  td::Slice signature;
};

[[nodiscard]] bool ed25519_batch_verifier_enabled();

td::Result<std::vector<td::uint8>> verify_ed25519_batch(td::Slice message,
                                                        const std::vector<Ed25519BatchVerifierItem>& items);

}  // namespace ton::validator::consensus
