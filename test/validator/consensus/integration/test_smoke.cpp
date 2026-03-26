/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */
#include "harness.h"

namespace ton::validator::consensus::test {
namespace {

TestConfig smoke_config() {
  TestConfig cfg;
  cfg.consensus_config.noncritical_params.target_rate = std::chrono::milliseconds{200};
  return cfg;
}

td::actor::Task<> smoke_scenario(HarnessHandle& h) {
  co_await h.wait_for("20 blocks accepted on node #0", predicates::accepted_seqno_ge(TimePoint::genesis(), 0, 20));
  co_return {};
}

td::Status smoke_verify(const TraceSnapshot&) {
  return td::Status::OK();
}

RegisterTestCase _smoke{TestCaseDescriptor{
    .name = "smoke",
    .config = smoke_config(),
    .scenario = smoke_scenario,
    .verify = smoke_verify,
}};

}  // namespace
}  // namespace ton::validator::consensus::test
