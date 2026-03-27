# Consensus Test Coverage Plan

Scope: compare `testnet...HEAD` on branch `testnet-candidate-with-tests` and identify consensus-side functionality that is new in this branch but still not covered well enough by tests.

## Analyzed production changes

The consensus-relevant production diff falls into these buckets:

| Area | Files | Current coverage status |
| --- | --- | --- |
| Dynamic noncritical params inside consensus actors | `validator/consensus/block-producer.cpp`, `validator/consensus/simplex/consensus.cpp`, `validator/consensus/simplex/pool.cpp`, `validator/consensus/simplex/candidate-resolver.cpp` | Partial |
| Broadcast extra + overlay precheck path | `validator/consensus/private-overlay.cpp`, `validator/consensus/types.cpp`, `validator/consensus/types.h`, `validator/consensus/bus.cpp`, `validator/consensus/bus.h` | Missing |
| Pool-side hardening | `validator/consensus/simplex/pool.cpp`, `validator/consensus/simplex/certificate.cpp` | Partial |
| Candidate resolver ingress hardening | `validator/consensus/simplex/candidate-resolver.cpp` | Missing |
| Noncritical param override selection / runtime plumbing | `validator/validator-options.hpp`, `validator/validator.h`, `validator/consensus/bridge.cpp`, `validator-engine/validator-engine.cpp`, `validator-engine/validator-engine.hpp` | Partial |
| Config schema / parsing | `crypto/block/block.tlb`, `crypto/block/mc-config.cpp` | Covered |
| Mechanical config field moves / refactors | `validator/consensus/simplex/bus.cpp`, `validator/consensus/simplex/bus.h`, `validator/consensus/simplex/db.cpp`, `validator/consensus/chain-state.cpp`, `validator/consensus/chain-state.h` | No extra test work needed beyond existing coverage |

## What is already covered

Existing tests already cover a large part of the new branch:

- `test/validator/consensus/test-block-producer.cpp`
  - empty/full candidate generation rules
  - runtime `target_rate` update affecting collation timeout
- `test/validator/consensus/test-simplex-consensus.cpp`
  - runtime updates for `target_rate`
  - runtime updates for `first_block_timeout`
  - runtime updates for `max_leader_window_desync`
- `test/validator/consensus/test-candidate-resolver.cpp`
  - resolve timeout / multiplier / cap / cooldown behavior
  - runtime update of those retry timings
- `test/validator/consensus/test-pool.cpp`
  - standstill timeout behavior
  - standstill rebroadcast contents
  - certificate rebroadcast after init
- `test/validator/consensus/test-simplex-parser.cpp`
  - malformed vote parsing
  - wrong signer / corrupt signature for signed votes
  - duplicate signers / mixed statements / underweight certificate rejection
- `test/validator/test-mc-config.cpp`
  - `simplex_config_v2`
  - noncritical param dictionary parsing
- `test/integration/test_noncritical_params.py`
  - set/get override control query
  - masterchain target rate override affects runtime behavior
  - shardchain remains unaffected by a masterchain-only override

The remaining work is therefore mostly around newly added hardening paths and runtime plumbing that are not exercised by the current validator consensus tests.

## Missing coverage and test plan

### P0: broadcast precheck and slot-authentication path

Files:

- `validator/consensus/private-overlay.cpp`
- `validator/consensus/types.cpp`
- `validator/consensus/types.h`
- `validator/consensus/simplex/pool.cpp`

Why this matters:

- This branch introduced `consensus_broadcastExtra(slot)` on outgoing broadcasts.
- Incoming broadcasts are now prechecked before full delivery.
- `Candidate::deserialize(...)` now accepts `expected_slot` and rejects slot mismatches.
- Pool now tracks seen broadcasts and rejects duplicates / finalized / too-future slots.

Suggested tests:

- Add `test/validator/consensus/test-private-overlay.cpp`.
- Add parser-level slot-mismatch tests to `test/validator/consensus/test-simplex-parser.cpp`.
- Add pool-side `PrecheckCandidateBroadcast` tests to `test/validator/consensus/test-pool.cpp`.

Scenarios:

1. Outgoing `CandidateGenerated` broadcast includes `consensus_broadcastExtra` with the candidate slot.
2. Incoming broadcast with malformed `extra` is rejected before `CandidateReceived`.
3. Incoming broadcast where `extra.slot` does not match the serialized candidate slot is rejected.
4. Precheck rejects a broadcast whose sender is not the expected collator for that slot.
5. Precheck rejects a duplicate broadcast for the same slot.
6. Precheck rejects a broadcast for a slot already finalized by the pool.
7. Precheck rejects a broadcast beyond `now + max_leader_window_desync * slots_per_leader_window`.
8. First valid broadcast for a slot passes precheck and is delivered as `CandidateReceived`.

Expected outcome:

- This closes the biggest currently untested functionality gap introduced by the branch.

### P0: candidate resolver ingress rate limiting

Files:

- `validator/consensus/simplex/candidate-resolver.cpp`
- `validator/rate-limiter.h`

Why this matters:

- The branch added per-peer request rate limiting for `IncomingOverlayRequest`.
- Current tests only cover outgoing retry timing, not incoming abuse protection.

Suggested tests:

- Extend `test/validator/consensus/test-candidate-resolver.cpp`.

Scenarios:

1. Same peer can serve up to `candidate_resolve_rate_limit` requests in the 1-second window.
2. The next request from the same peer is rejected with an error.
3. Requests from another peer are not blocked by the first peer hitting the limit.
4. Advancing time past the limiter window allows the peer again.
5. `NoncriticalParamsUpdated` changing `candidate_resolve_rate_limit` clears previous limiter state and applies the new limit immediately.
6. Optional boundary: `candidate_resolve_rate_limit = 0` rejects all requests if that is the intended contract.

### P0: pool bans on invalid signatures

Files:

- `validator/consensus/simplex/pool.cpp`
- `validator/consensus/simplex/certificate.cpp`

Why this matters:

- The pool now temporarily bans peers after invalid vote or certificate signatures.
- No current test exercises that quarantine path.

Suggested tests:

- Extend `test/validator/consensus/test-pool.cpp`.

Scenarios:

1. Invalid signed vote causes a ban for `bad_signature_ban_duration`.
2. While banned, a subsequent valid vote from the same peer is dropped.
3. Invalid certificate causes the same ban behavior.
4. After the ban expires, the peer’s valid message is accepted again.
5. `NoncriticalParamsUpdated` changes the duration used for newly created bans.
6. Duplicate already-known vote / certificate is ignored without a false ban, to lock in the new `wants(...)` / `needs(...)` fast path.

### P0: pool regression for already-notarized candidate in `WaitForParent`

Files:

- `validator/consensus/simplex/pool.cpp`

Why this matters:

- The branch changed `WaitForParent` so a candidate whose slot already has the same notarized block is no longer rejected as an error.
- That is a behavior change and currently has no dedicated regression test.

Suggested tests:

- Extend `test/validator/consensus/test-pool.cpp`.

Scenarios:

1. Slot already contains a notarization certificate for candidate `X`; `WaitForParent(X)` resolves successfully.
2. Same setup with a finalization certificate for `X`; `WaitForParent(X)` still resolves successfully.
3. Slot notarized for `Y != X` still reports conflicting evidence.

### P1: standstill rebroadcast pacing and restart behavior

Files:

- `validator/consensus/simplex/pool.cpp`

Why this matters:

- The branch replaced immediate standstill rebroadcast with a background task that:
  - rate-limits egress using `standstill_max_egress_bytes_per_s`
  - replays last final cert first
  - stops the sweep if a newer finalization appears
- Existing tests cover content and timeout, but not pacing or interruption semantics.

Suggested tests:

- Extend `test/validator/consensus/test-pool.cpp`.
- If unit tests become too synthetic, add one integration scenario under `test/validator/consensus/integration/`.

Scenarios:

1. Large standstill replay is paced over multiple scheduler ticks instead of being emitted in one burst.
2. Last final certificate is emitted before later certificates and votes.
3. A new finalization arriving during the replay aborts the old sweep and the next sweep restarts from the new frontier.
4. Updating `standstill_max_egress_bytes_per_s` affects the next standstill cycle.

### P1: override selection semantics in validator options

Files:

- `validator/validator-options.hpp`
- `validator/validator.h`

Why this matters:

- Runtime overrides now depend on shard ancestry, catchain seqno range, and ordered matching.
- The integration test only proves one happy path for a masterchain target-rate override.

Suggested tests:

- Add `test/validator/test-validator-options.cpp`.

Scenarios:

1. Override applies when `cc_seqno` is inside `[from_seqno, to_seqno]`.
2. Both range boundaries are inclusive.
3. Override applies to descendant shards via `shard_is_ancestor(...)`.
4. Non-matching shard leaves the base config unchanged.
5. Partial override changes only flagged fields and preserves the rest.
6. Overlapping overrides follow a deterministic precedence rule.
   - Current code is “first matching override wins”; either lock that in with a test or change the implementation before adding tests.

### P1: validator engine override persistence and reload

Files:

- `validator-engine/validator-engine.cpp`
- `validator-engine/validator-engine.hpp`
- `test/tontester/src/tonlib/engine_console.py`

Why this matters:

- The engine now persists overrides to `noncritical-params-overrides.json` and reloads them at startup.
- Current integration only tests live set/get in one process lifetime.

Suggested tests:

- Extend `test/integration/test_noncritical_params.py` or add a second integration test focused on persistence.

Scenarios:

1. Overrides survive node restart and are reloaded from disk.
2. Setter overwrites the file and getter returns the exact payload after restart.
3. Malformed JSON on disk is handled without crashing and consensus falls back to base config.
4. Shard-specific override affects the intended shard only.
5. If a control-query harness exists, cover “not started” and permission-denied responses.

### P2: bridge lifecycle and effective-update propagation

Files:

- `validator/consensus/bridge.cpp`

Why this matters:

- The bridge now closes the async DB explicitly before destroying RocksDB.
- It also publishes `NoncriticalParamsUpdated` only when the effective params actually change.
- Neither behavior is directly tested.

Suggested tests:

- Add an actor-level test with a temporary directory and real RocksDB, or cover it via a focused integration harness.

Scenarios:

1. Destroying a validator group closes DB cleanly and removes the consensus DB directory.
2. Recreating the group on the same path does not fail with a leftover RocksDB lock / open handle.
3. Updating validator options with no effective noncritical-param change does not publish `NoncriticalParamsUpdated`.
4. Updating options with an effective change publishes exactly one update with the resolved params.

## Current test execution status

Build/run attempt on this branch:

- Passed:
  - `test/validator/test-mc-config`
  - `test/validator/test-validate-query`
  - `test/validator/consensus/test-block-validator`
  - `test/validator/consensus/test-block-producer`
  - `test/validator/consensus/test-simplex-parser`
- Blocked by test-suite maintenance, not by changed consensus behavior:
  - `test/validator/consensus/test-candidate-resolver`
  - `test/validator/consensus/test-pool`
  - `test/validator/consensus/test-simplex-db`
  - `test/validator/consensus/test-simplex-consensus`
  - `test/validator/consensus/test-consensus-adversarial`
  - `test/validator/consensus/integration/test-adversarial` and all registered `adversarial-*` CTest cases
  - all of the targets above fail to build because their `TestDbImpl` test doubles do not implement the newly required `consensus::Db::close()` pure virtual
- Blocked by test target linkage:
  - `test/validator/consensus/test-consensus`
  - current link fails with undefined `validator::create_block(...)` after `validator/libvalidator.a` started using that symbol from `block-accepter.cpp` and `chain-state.cpp`; the test target now needs corrected direct/transitive linkage

Result for this plan:

- After fixing the harness/build issues, `test/validator/consensus/test-pool` now exposes real expectation drift caused by the new standstill rebroadcast behavior.
- The failing cases are about timing semantics, not about broken production logic.

## Update existing tests

Existing tests that should be updated because the branch changed behavior in a valid way:

- `test/validator/consensus/test-pool.cpp`
  - `Pool_StandstillTriggersAtConfiguredTimeout`
  - `Pool_StandstillBroadcastContainsExpectedVotesAndCertificates`
  - `Pool_NoncriticalParamsUpdateChangesStandstillTimeout`

Why these now fail:

- `validator/consensus/simplex/pool.cpp` no longer emits standstill rebroadcasts synchronously from the timeout handler.
- The timeout handler now only starts `standstill_resolution_task()`, which sends messages through a paced background loop and may sleep before the first rebroadcast.
- The old tests assert for outgoing payloads in the same scheduler tick in which standstill is detected, so they race the new asynchronous sender and fail even though the rebroadcast happens immediately after.

What to change in the tests:

1. Replace same-tick assertions with eventual assertions.
   - After advancing time to the standstill timeout, advance once more by the next pending timeout or poll until the expected outgoing message appears.
2. Update content tests to tolerate paced multi-tick delivery.
   - Do not require final certificate, later certificates, and local votes to all appear in one scheduler drain.
   - Collect events across subsequent ticks until the expected standstill replay set is observed.
3. Add assertions for the new behavior while updating these tests.
   - Last final certificate is broadcast first.
   - Replay may span multiple scheduler ticks because of `standstill_max_egress_bytes_per_s`.
4. Keep the timeout assertions, but separate them from delivery assertions.
   - The timeout test should still prove that standstill is detected at the configured time.
   - The delivery assertion should then verify that rebroadcast starts asynchronously after detection.
5. For `NoncriticalParamsUpdateChangesStandstillTimeout`, preserve the existing reschedule check but wait for the first replay message before checking post-rebroadcast state.

## Recommended implementation order

1. `test-private-overlay.cpp` plus parser slot-mismatch tests.
2. Candidate resolver ingress rate-limit tests.
3. Pool ban / duplicate-precheck / same-notarized-candidate regression tests.
4. Standstill pacing tests.
5. Validator options selection tests.
6. Engine persistence / restart tests.
7. Bridge lifecycle tests.

## Notes on files that do not need dedicated new tests

- `validator/consensus/chain-state.cpp` / `.h`
  - refactor only, no new externally visible behavior in this diff
- `validator/consensus/simplex/bus.cpp` / `.h`
  - config field migration already exercised indirectly by the actor tests above
- `validator/consensus/simplex/db.cpp`
  - config field migration already covered indirectly by existing pool / consensus tests
- `crypto/block/mc-config.cpp`
  - already covered well by `test/validator/test-mc-config.cpp`

## Bottom line

The core consensus logic added in this branch is already in much better shape than `testnet`, but the branch still lacks tests for the new hardening paths around broadcast admission, peer abuse resistance, override selection, and persistence. The highest-value missing work is in `private-overlay`, `candidate-resolver`, and `pool`, because those are the new code paths that can now reject, rate-limit, or quarantine network inputs.
