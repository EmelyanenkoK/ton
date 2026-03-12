# Bugs Discovered During Simplex Testing

## `simplex::Db` actor does not stop on `StopRequested`

- Documentation / expectation: `test-consensus` needs to start and stop all registered simplex actors cleanly between runs.
- Observed code: [validator/consensus/simplex/db.cpp](/home/rulon/ton/validator/consensus/simplex/db.cpp) does not handle `StopRequested`, unlike the other test-facing actors used in the harness.
- Trigger: once `test/validator/consensus/test-consensus.cpp` registers the production `simplex::Db` actor so that `LeaderWindowObserved`, `BroadcastVote`, and `SaveCertificate` requests are actually serviced, shutdown hangs waiting for that actor to terminate.
- Testing impact: the consensus harness must currently use a test-local replacement actor with the same persistence behavior plus explicit stop handling. This keeps the bug visible instead of masking it in production code.
- What should be fixed:
  add the same `StopRequested` handler that the other simplex actors already have. This looks like a straightforward production omission, not a test issue.
- Minimal patch:
```diff
template <>
void handle(BusHandle, std::shared_ptr<const StopRequested>) {
  stop();
}
```

## `simplex::Db` marks votes and certificates saved before the write actually succeeds

- Documentation / expectation: local vote and certificate persistence is supposed to be durable state. If `db->set()` fails, the same vote or certificate must remain retryable.
- Observed behavior: the new unit tests [SimplexDb_RetriesBroadcastVoteAfterSetFailure](/home/rulon/ton/test/validator/consensus/test-simplex-db.cpp#L288) and [SimplexDb_RetriesCertificateSaveAfterSetFailure](/home/rulon/ton/test/validator/consensus/test-simplex-db.cpp#L311) both fail. After one injected `db->set()` failure, retrying the exact same object is rejected as already stored.
- Observed code: [validator/consensus/simplex/db.cpp](/home/rulon/ton/validator/consensus/simplex/db.cpp#L54) inserts the vote hash into `saved_votes` and advances `next_seqno_` before awaiting `db->set()`. The certificate path at [validator/consensus/simplex/db.cpp](/home/rulon/ton/validator/consensus/simplex/db.cpp#L69) does the same `saved_votes` pre-mutation.
- Probable cause: the in-memory dedup / seqno state is being treated as committed state instead of post-commit state. A failed write leaves memory saying “already saved” while the backing db never recorded it.
- Testing impact: both retry tests are now deterministic red tests.
- What should be fixed:
  move `saved_votes.insert(...)` and `next_seqno_` advancement to after successful `db->set()`.
- Minimal patch for `BroadcastVote`:
```diff
-    saved_votes.insert(hash);
-
     auto key = create_serialize_tl_object<tl::db_key_vote>(hash);
-    auto value = create_serialize_tl_object<tl::db_ourVote>(std::move(vote), next_seqno_++);
-    co_return co_await owning_bus()->db->set(std::move(key), std::move(value));
+    auto seqno = next_seqno_;
+    auto value = create_serialize_tl_object<tl::db_ourVote>(std::move(vote), seqno);
+    co_await owning_bus()->db->set(std::move(key), std::move(value));
+    saved_votes.insert(hash);
+    next_seqno_ = seqno + 1;
+    co_return {};
```
- Minimal patch for `SaveCertificate`:
```diff
-    saved_votes.insert(hash);
-
     auto key = create_serialize_tl_object<tl::db_key_vote>(hash);
     auto value = create_serialize_tl_object<tl::db_cert>(std::move(cert));
-    co_return co_await owning_bus()->db->set(std::move(key), std::move(value));
+    co_await owning_bus()->db->set(std::move(key), std::move(value));
+    saved_votes.insert(hash);
+    co_return {};
```

## `simplex::Pool::WaitForParent` stays blocked after the last missing gap skip certificate arrives

- Documentation / expectation: Rule 4 says a candidate should proceed once its parent notarization and every skipped slot in the gap are locally proven.
- Observed behavior: the new unit test [Pool_WaitsForMissingGapSkipCertificates](/home/rulon/ton/test/validator/consensus/test-pool.cpp#L241) shows the negative gate works at first, but even after the final missing skip cert is injected, `WaitForParent` never resumes.
- Narrowing evidence:
  the test sees the new skip certificate go through `SaveCertificate`, so the failure is not “certificate never decoded” or “request was resolved too early”. The actor accepts the new cert and still leaves the request pending.
- Probable cause: the bug is likely in the post-save gap bookkeeping around [handle_typed_saved_certificate(SkipCertRef)](/home/rulon/ton/validator/consensus/simplex/pool.cpp#L768) and [maybe_resolve_requests()](/home/rulon/ton/validator/consensus/simplex/pool.cpp#L693). Either `skip_intervals_` is not transitioning to the expected boundary after the new skip cert, or pending `WaitForParent` requests are not being re-resolved against the updated interval state the way Rule 4 expects.
- Testing impact: `test-pool` is now red until this gap-resolution path is fixed.
- What should be fixed:
  instrument `skip_intervals_` and the `Request{id,parent}` vector around `handle_saved_certificate(SkipCertRef)` and confirm that a request with `parent.slot = 0`, `id.slot = 3`, and newly known skips for slots `1` and `2` transitions from pending to resolved.

## `simplex::CandidateResolver` can remain unresolved after ignoring a malformed peer response

- Documentation / expectation: Rule 2 says malformed or irrelevant peer data should be ignored, but later valid data should still allow resolution to complete.
- Observed behavior: the new unit test [CandidateResolver_IgnoresWrongIdResponseAndKeepsWaiting](/home/rulon/ton/test/validator/consensus/test-candidate-resolver.cpp#L216) first receives a response with the wrong candidate id, then a later response with the correct candidate body and notar cert. The actor does issue the second request, but the overall `ResolveCandidate` task still never completes.
- Narrowing evidence:
  this is not the old request-bit bug. The test proves the resolver survives the malformed response far enough to send another request. The failure is in the “accept later good data and finish resolution” part.
- Probable cause: the issue is likely in the completion path around [resolve_candidate_inner()](/home/rulon/ton/validator/consensus/simplex/candidate-resolver.cpp#L320) and [resolve_candidate_task()](/home/rulon/ton/validator/consensus/simplex/candidate-resolver.cpp#L304). After a malformed response has been ignored, either the later `merge(...)` is not making `candidate_and_cert` complete as expected, or the resolve awaiters are not being resumed after the state becomes complete.
- Testing impact: `test-candidate-resolver` is now red until malformed-response recovery is fixed.
- What should be fixed:
  trace `CandidateAndCert::from_tl(...)`, `CandidateAndCert::merge(...)`, and `maybe_resume_resolve_awaiters(...)` for the sequence “wrong-id response, then correct response” and confirm that the second response actually completes both missing pieces and wakes the bridge created in `ResolveCandidate`.

## Restart after wiping simplex state can wedge `test-consensus` shutdown

- Documentation / expectation: Rule 2 candidate-resolution testing needs the harness to stop one node, wipe its local simplex state, restart it, and then shut the whole run down cleanly.
- Observed behavior: when a node is stopped, its simplex DB is cleared, and the node is started again inside `test/validator/consensus/test-consensus.cpp`, the final shutdown can hang while stopping that restarted node. The log shows repeated detached-task failures with `Actor destroyed` during teardown.
- Trigger: a restart/storage-loss scenario intended to exercise candidate resolution.
- Testing impact: this currently blocks a stable `test-consensus` scenario for Rule 2 recovery after restart and DB loss. The blocker needs to be understood before a non-hanging candidate-resolution integration test can be committed.
- What should be fixed:
  the shutdown order in [stop_instance()](/home/rulon/ton/test/validator/consensus/test-consensus.cpp#L2594) is too aggressive for restart scenarios. It publishes `StopRequested`, then immediately disables the DB and clears `inst.bus` before waiting for actor shutdown. That is a good recipe for the detached `Actor destroyed` failures we keep seeing.
- Suggested direction:
  keep the bus and DB alive until `stop_waiter` resolves, then clear `inst.bus`, disable the DB, and drop the runtime. If some actor still needs DB access while reacting to `StopRequested`, the current order can break a clean shutdown even if the actor logic itself is correct.
- Scope:
  this currently looks like a harness/runtime-stop bug, not a protocol bug. I would fix this before drawing any deeper conclusions from restart teardown failures.

## Rule 6 skip timeout resets instead of following the last observed finalization

- Documentation / expectation: Rule 6 requires skip timeout in window `k` to satisfy `T_skip >= T0 * alpha^(k-k*-1)`, where `k*` is the window of the last observed finalization when `k` becomes active.
- Observed behavior: in the new `skip-timeout-uses-last-finalization` `test-consensus` scenario, validator `#0.0` freezes its observed finalization at window `0` and still skips window `4` after about `1.10 s`, even though the documented lower bound with `T0 = 1 s` and `alpha = 2` is `8.0 s`.
- Trigger: drop finalization certificates to one validator after the next window boundary, let one later window clear normally, then suppress candidate and notarization-certificate delivery for a later window from that validator's point of view.
- Testing impact: Rule 6 now has a deterministic red test confirming the timeout-reset bug described in `simplex_docs.md`.
- What should be fixed:
  [ConsensusImpl::handle(LeaderWindowObserved)](/home/rulon/ton/validator/consensus/simplex/consensus.cpp#L111) currently derives the next timeout from `previous_window_had_skip_` and resets to the default if the previous window had no skip. That is exactly the behavior the doc now calls a bug.
- Suggested production change:
  store the most recent finalized window explicitly, update it from [FinalizationObserved](/home/rulon/ton/validator/consensus/simplex/consensus.cpp#L101), and compute `first_block_timeout_s_` from `new_window - last_finalized_window_ - 1`, capped by `first_block_max_timeout_s`. `previous_window_had_skip_` can still exist for other bookkeeping, but it should no longer control the timeout reset policy.
- Expected outcome:
  after the fix, the Rule 6 test should stop failing with the current `1.10 s` vs `8.0 s` mismatch and instead respect the documented lower bound whenever a validator's observed finalization is stale.

## Mid-run standstill scenarios can wedge orderly `test-consensus` teardown

- Documentation / expectation: Rule 8 standstill-resolution testing needs the harness to stop or isolate part of the validator set mid-run, wait for rebroadcasts, and then shut down normally.
- Observed behavior: after inducing a standstill by stopping or isolating validators inside `test/validator/consensus/test-consensus.cpp`, the subsequent global `finalize()` shutdown can hang while stopping the remaining live nodes. The logs again show repeated detached-task failures such as `Actor destroyed` during teardown.
- Trigger: a standstill-resolution scenario that removes quorum after at least one finalization has already happened.
- Testing impact: the Rule 8 test currently has to verify its trace and let the process exit immediately, instead of waiting for orderly actor teardown. The shutdown instability remains visible here and should be understood separately from the protocol assertion.
- What should be fixed:
  most likely the same stop sequencing problem as the restart teardown issue above. The symptom pattern is the same, and both scenarios stress shutdown while detached actor tasks are still in flight.
- Suggested direction:
  fix shutdown ordering once in the harness, then rerun both the restart and standstill scenarios. If the standstill-only failure survives after that, it is worth looking for an actor that still lacks a `StopRequested` handler or keeps detached work alive after stop.
