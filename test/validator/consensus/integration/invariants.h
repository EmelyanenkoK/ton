/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */
#pragma once

#include "config.h"
#include "trace.h"

namespace ton::validator::consensus::test {

// Always-valid protocol invariants (run after every test).
td::Status verify_no_double_notar(const TraceSnapshot& snapshot);
td::Status verify_adversarial_invariants(const TraceSnapshot& snapshot, std::set<size_t> adversarial_nodes = {});
td::Status verify_notar_requires_parent_notar(const TraceSnapshot& snapshot);
td::Status verify_finalize_requires_own_notar(const TraceSnapshot& snapshot);
td::Status verify_no_skip_final_conflict(const TraceSnapshot& snapshot);
td::Status verify_certificate_requires_quorum(const TraceSnapshot& snapshot, const TestConfig& config);
td::Status verify_certificate_rebroadcast(const TraceSnapshot& snapshot);
td::Status verify_finalize_requires_observed_notar(const TraceSnapshot& snapshot);
td::Status verify_leader_window_schedule(const TraceSnapshot& snapshot, const TestConfig& config);

// Conditionally-valid invariants.
td::Status verify_no_misbehavior_reports(const TraceSnapshot& snapshot);
td::Status verify_empty_candidates_consensus(const TraceSnapshot& snapshot);
td::Status verify_standstill_rebroadcast_contents(const TraceSnapshot& snapshot, const TestConfig& config);

// Run all unconditional invariants. Returns first failure.
td::Status run_all_invariants(const TraceSnapshot& snapshot, const TestConfig& config);

// Helpers shared with verify functions.
std::string certificate_trace_key(const Traced<ProtocolCertificateSent>& record);
std::string vote_trace_key(const simplex::Vote& vote);

}  // namespace ton::validator::consensus::test
