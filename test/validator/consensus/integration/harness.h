/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */
#pragma once

#include "actors.h"
#include "config.h"
#include "trace.h"

namespace ton::validator::consensus::test {

class TestConsensus;

// Handle passed to scenario coroutines for interacting with the harness.
class HarnessHandle {
 public:
  explicit HarnessHandle(td::actor::ActorId<TestConsensus> test) : test_(test) {
  }

  // Node lifecycle. Returns a TimePoint capturing the protocol frontier at the
  // moment the action completes.
  td::actor::Task<TimePoint> stop_instance(size_t node_idx);
  TimePoint start_instance(size_t node_idx);
  td::actor::Task<> clear_instance_db(size_t node_idx);

  // Wait until a predicate over the trace is satisfied.
  td::actor::Task<> wait_for(std::string description, std::unique_ptr<TracePredicate> predicate);

  // Trace access (for verify functions, not for scenarios).
  td::actor::Task<TraceSnapshot> get_trace_snapshot();

  // Message filter manipulation.
  TestMessageFilters& message_filters();

  // Config (read-only).
  const TestConfig& config() const;

  // Capture the current protocol frontier as a TimePoint.
  td::actor::Task<TimePoint> now();

  // Runtime delay overrides.
  void set_validation_time(Range r);
  void set_collation_time(Range r);
  void set_db_delay(Range r);

  // Network control. Returns TimePoint.
  td::actor::Task<TimePoint> set_node_network_disabled(size_t node_idx, bool disabled);

 private:
  td::actor::ActorId<TestConsensus> test_;
};

// Descriptor for a single test case.
struct TestCaseDescriptor {
  std::string name;
  TestConfig config;
  td::actor::Task<> (*scenario)(HarnessHandle& h);
  td::Status (*verify)(const TraceSnapshot& snapshot);
  double timeout = 60.0;
};

// Test case registry.
std::vector<TestCaseDescriptor>& test_case_registry();

struct RegisterTestCase {
  RegisterTestCase(TestCaseDescriptor desc) {
    test_case_registry().push_back(std::move(desc));
  }
};

}  // namespace ton::validator::consensus::test
