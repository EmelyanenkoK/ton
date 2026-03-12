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

## Rule 2 recovery retries but does not complete after restart and local data loss

- Documentation / expectation: Rule 2 says that after losing a notarized candidate, a validator retries `requestCandidate` with exponential backoff and eventually recovers once a peer with the candidate body is reachable.
- Observed behavior: in the new `candidate-resolution-recovery` `test-consensus` scenario, validator `#0.0` repeatedly issues `requestCandidate` for the chosen notarized candidate and backs off its timeout, but still never resolves that candidate even after the only full-data peer is restarted.
- Trigger: stop validator `#0.0`, wipe its simplex DB and candidate storage, restart partial peers without candidate bodies, then restart the only peer that still has the candidate body.
- Testing impact: Rule 2 now has a deterministic red test. The failure is no longer “scenario not built”; it is that the documented recovery behavior does not complete under this restart/data-loss setup.
- What should be fixed first:
  [CandidateAndCert::make_request()](/home/rulon/ton/validator/consensus/simplex/candidate-resolver.cpp#L89) appears to set `want_candidate` and `want_notar` to what the node already has, instead of what it is missing. That is the highest-confidence production bug I found in this area, and it is consistent with the observed behavior: endless retries without ever obtaining the missing payload.
- Minimal patch:
```diff
tl::RequestCandidateRef make_request(CandidateId id) const {
  return create_tl_object<tl::requestCandidate>(
      id.to_tl(), !candidate.has_value(), !notar_cert.has_value());
}
```
- After that patch:
  rerun the Rule 2 red test before changing anything else. If recovery still does not complete, the next suspects are the restart/storage lifecycle around candidate metadata/body persistence and the teardown ordering issue above, but the request-flag inversion is the first thing I would fix.

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
