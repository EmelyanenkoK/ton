# Bugs Discovered During Simplex Testing

This file tracks issues that still reproduce on the current branch.
Older entries were removed when they either stopped reproducing or were no longer considered bugs
under the current policy. In particular:

- `simplex::Db` now handles `StopRequested`, so the old shutdown-hang note was stale.
- DB write-failure retry tests are no longer tracked as bugs because current production policy is to
  crash on write failure instead of retrying.
- `test-consensus` `candidate-resolution-recovery` now passes, so the old restart-teardown entry was
  removed.
- `test-consensus` `standstill-rebroadcast-contents` now passes, so the old standstill-teardown
  entry was removed.

## `simplex::Pool::WaitForParent` stays blocked after the last missing gap skip certificate arrives

- Documentation / expectation: Rule 4 says a candidate should proceed once its parent notarization
  and every skipped slot in the gap are locally proven.
- Observed behavior: `Pool_WaitsForMissingGapSkipCertificates` still fails. The negative gate works
  at first, but even after the final missing skip certificate is injected, `WaitForParent` never
  resumes.
- Reproduce:
```sh
timeout 20s ./build/test/validator/consensus/test-pool --filter WaitsForMissingGapSkipCertificates --verbosity 0
```
- Narrowing evidence:
  the test sees the new skip certificate pass through `SaveCertificate`, so the failure is not
  "certificate never decoded" or "request was resolved too early". The actor accepts the new cert
  and still leaves the request pending.
- Probable cause:
  the bug is likely in the post-save gap bookkeeping around
  `validator/consensus/simplex/pool.cpp` `handle_typed_saved_certificate(SkipCertRef)` and
  `maybe_resolve_requests()`. Either `skip_intervals_` is not transitioning to the expected
  boundary after the new skip cert, or pending `WaitForParent` requests are not being re-resolved
  against the updated interval state the way Rule 4 expects.
- What should be fixed:
  instrument `skip_intervals_` and the pending `Request{id,parent}` set around
  `handle_saved_certificate(SkipCertRef)` and confirm that a request with `parent.slot = 0`,
  `id.slot = 3`, and newly known skips for slots `1` and `2` transitions from pending to resolved.

## `simplex::CandidateResolver` can remain unresolved after ignoring a malformed peer response

- Documentation / expectation: Rule 2 says malformed or irrelevant peer data should be ignored, but
  later valid data should still allow resolution to complete.
- Observed behavior: `CandidateResolver_IgnoresWrongIdResponseAndKeepsWaiting` still fails. The test
  first receives a response with the wrong candidate id, then a later response with the correct
  candidate body and notar cert. The actor does issue the second request, but the overall
  `ResolveCandidate` task still never completes.
- Reproduce:
```sh
timeout 20s ./build/test/validator/consensus/test-candidate-resolver --filter IgnoresWrongIdResponseAndKeepsWaiting --verbosity 0
```
- Narrowing evidence:
  this is not the old request-bit bug. The test proves the resolver survives the malformed response
  far enough to send another request. The failure is in the "accept later good data and finish
  resolution" part.
- Probable cause:
  the issue is likely in the completion path around
  `validator/consensus/simplex/candidate-resolver.cpp` `resolve_candidate_inner()` and
  `resolve_candidate_task()`. After a malformed response has been ignored, either the later
  `merge(...)` is not making `candidate_and_cert` complete as expected, or the resolve awaiters are
  not being resumed after the state becomes complete.
- What should be fixed:
  trace `CandidateAndCert::from_tl(...)`, `CandidateAndCert::merge(...)`, and
  `maybe_resume_resolve_awaiters(...)` for the sequence "wrong-id response, then correct response"
  and confirm that the second response actually completes both missing pieces and wakes the bridge
  created in `ResolveCandidate`.

## `simplex::Certificate::from_tl` accepts malformed skip-slot values after signed `int32 -> uint32` wraparound

- Documentation / expectation: the tracker parser row (`Kernel-27`, `Kernel-31` `4c5eda7cb`) says
  malformed or oversized window-slot values must be rejected instead of being silently reinterpreted.
- Observed behavior: `SimplexParser_CertificateFromTlRejectsOversizedWindowSlot` is red. A
  certificate carrying `skipVote(-1)` is accepted and ends up as slot `4294967295` after the
  signed-to-unsigned cast in the vote parser.
- Reproduce:
```sh
./build/test/validator/consensus/test-simplex-parser --verbosity 0
```
- Narrowing evidence:
  `validator/consensus/simplex/votes.cpp` parses `skipVote.slot_` with
  `static_cast<td::uint32>(vote.slot_)`, and the certificate parser then validates signatures and
  quorum against that wrapped value instead of rejecting it as malformed input.
- Probable cause:
  the TL `int32` slot field is trusted too early. Negative values survive the TL parse and then
  wrap into large `uint32` slot numbers before any range check runs.
- What should be fixed:
  validate `skipVote.slot_ >= 0` when converting from TL to `SkipVote` and return a parse error on
  negative / out-of-domain slot values before signature or quorum handling continues.
  Smallest intended production patch shape:
```diff
--- a/validator/consensus/simplex/votes.cpp
+++ b/validator/consensus/simplex/votes.cpp
@@
 SkipVote SkipVote::from_tl(const tl::skipVote& vote) {
+  CHECK(vote.slot_ >= 0);
   return {static_cast<td::uint32>(vote.slot_)};
 }
```

## Rule 6 skip-timeout integration scenario is still red in `test-consensus`

- Documentation / expectation: Rule 6 requires skip timeout in window `k` to be derived from the
  last observed finalization, not from whether the immediately previous window had a skip.
- Observed behavior: the `test-consensus` Rule 6 case still fails on this branch. At the moment it
  aborts before the final lower-bound comparison and reports:
  `scenario did not produce any SkipVote in window 2 or later; observed skip windows=[]`.
- Reproduce:
```sh
timeout 60s ./build/test/validator/consensus/test-consensus --test-case skip-timeout-uses-last-finalization --n-nodes 4 --target-rate-ms 100 --duration 6 --verbosity 0
```
- Narrowing evidence:
  production `ConsensusImpl` still derives the next timeout from `previous_window_had_skip_` in
  `validator/consensus/simplex/consensus.cpp`, while `simplex_docs.md` explicitly calls that reset
  behavior unsupported. The current integration scenario is therefore still red, even though on this
  branch it now fails earlier with "no SkipVote" instead of reaching the older timing-mismatch
  assertion.
- Probable cause:
  there are likely two issues layered together:
  `ConsensusImpl` still implements the documented Rule 6 deviation, and the current
  `test-consensus` scenario is not yet robust enough to drive the run all the way to the intended
  later-window skip on every execution.
- What should be fixed:
  first, update `validator/consensus/simplex/consensus.cpp` so timeout growth is keyed off the last
  observed finalization rather than `previous_window_had_skip_`. Then rerun and tighten the
  `test-consensus` Rule 6 scenario until it reliably reaches the intended timing assertion instead
  of aborting earlier with no later-window `SkipVote`.

## `BlockProducer` does not respect the remaining collation budget of the current slot

- Documentation / expectation: `tracker-test-plan.md` `Kernel-22` says the collator should receive
  the remaining time budget of the current slot, and a collation result that arrives after the slot
  budget expires should not still be published for that slot.
- Observed behavior: both producer time-budget tests are red.
  `BlockProducer_PassesRemainingCollationBudgetToManager` shows the producer still passes a fresh
  full `target_rate_` soft timeout even when the leader window starts late.
  `BlockProducer_StopsProducingWhenCollationBudgetExpires` shows the producer still publishes a
  candidate after collation returns well after the slot deadline.
- Reproduce:
```sh
./build/test/validator/consensus/test-block-producer --verbosity 0
```
- Narrowing evidence:
  `validator/consensus/block-producer.cpp` currently builds `CollateParams` with
  `soft_timeout = td::Timestamp::in(target_rate_)`, independent of `event->start_time` and the
  already elapsed portion of the slot. After `collate_block(...)` returns, the code checks only
  whether the leader window changed, not whether the slot deadline has already passed.
- Probable cause:
  the producer treats collation as if every slot starts with a fresh full `target_rate_` budget and
  lacks a post-collation deadline check before publishing `CandidateGenerated` /
  `CandidateReceived`.
- What should be fixed:
  derive the manager `soft_timeout` from the remaining time until the current slot deadline, not
  from an unconditional full `target_rate_`, and after collation/signing re-check that the result
  is still within the slot budget before publishing it.
