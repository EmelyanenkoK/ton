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

## `simplex::Pool` standstill rebroadcast omits our later local votes

- Documentation / expectation: Rule 8 says standstill rebroadcast must contain the highest final
  certificate, later certificates, and our own later votes that do not yet have certificates.
- Observed behavior: `Pool_StandstillBroadcastContainsExpectedVotesAndCertificates` is red.
  The pool rebroadcasts the expected final certificate and later skip certificate, but omits a
  later local `NotarizeVote` that was broadcast before standstill fired.
- Reproduce:
```sh
./build/test/validator/consensus/test-pool --filter StandstillBroadcastContainsExpectedVotesAndCertificates --verbosity 1
```
- Narrowing evidence:
  the test starts from bootstrap final/skip certificates, publishes one local later
  `BroadcastVote(NotarizeVote{slot=2})`, advances exactly to the configured standstill timeout,
  and then checks outgoing payloads. The outgoing trace contains the expected certificate payloads
  but not the signed local vote payload for slot 2.
- Probable cause:
  `validator/consensus/simplex/pool.cpp` `alarm()` intends to append local later votes through
  `state->votes[bus.local_id.idx.value()].serialize_to(messages, state->certs)`, but the local
  vote state for some later slots is apparently not surviving to the standstill snapshot or is not
  being serialized as expected once certificates are already present for earlier slots.
- What should be fixed:
  inspect the per-slot local `Tsentrizbirkom` state at standstill and confirm that later local
  votes remain present after `BroadcastVote` handling. Then trace why `serialize_to(...)` skips the
  local vote for slot 2 even though no certificate exists for that vote.

## `ConsensusImpl` still notarizes a candidate that names the wrong slot leader

- Documentation / expectation: tracker `Kernel-23` / `Kernel-28` expects an otherwise well-formed
  candidate with the wrong `leader` for the slot to be rejected by the consensus pipeline and to
  never produce `Notar`.
- Observed behavior: `wrong-leader-candidate-never-notarizes` is red in `test-consensus.cpp`.
  Validator `#0.0` still sends `Notar` for a synthetic full candidate whose `leader` field names a
  different validator than the one assigned to that slot.
- Reproduce:
```sh
timeout 40s ./build/test/validator/consensus/test-consensus --test-case wrong-leader-candidate-never-notarizes --verbosity 0
```
- Narrowing evidence:
  `validator/consensus/simplex/consensus.cpp` stores the candidate in `pending_block`, then only
  emits a trace event when `candidate->leader != owning_bus()->local_id.idx` before continuing into
  `try_notarize(...)`. There is no local leader-ownership rejection on the `CandidateReceived`
  path before `WaitForParent`, validation, and `BroadcastVote(NotarizeVote{...})`.
- Probable cause:
  leader mismatch is currently treated as an informational trace condition rather than a consensus
  validity gate, so a wrong-leader candidate can still pass the rest of the notarization pipeline.
- What should be fixed:
  reject or report misbehavior for candidates whose `leader` does not match the expected slot
  leader before they reach `try_notarize(...)`. The wrong-leader integration regression should stay
  red until that guard exists in the live pipeline.

## `ConsensusImpl` leaves a rejected candidate stuck as the slot's pending block

- Documentation / expectation: tracker `Kernel-23` expects a validation rejection to leave no
  residual votes or slot state, so a later replacement candidate for the same slot can still
  progress normally through notarization.
- Observed behavior: `validation-reject-does-not-leave-residual-votes` is red in
  `test-consensus.cpp`. The first candidate is synthetically rejected by validation as intended,
  but the later replacement candidate for the same slot never produces `Notar`.
- Reproduce:
```sh
timeout 40s ./build/test/validator/consensus/test-consensus --test-case validation-reject-does-not-leave-residual-votes --verbosity 0
```
- Narrowing evidence:
  `validator/consensus/simplex/consensus.cpp` sets `slot->state->pending_block = candidate` before
  validation starts. When validation returns `CandidateReject`, the actor logs and returns, but it
  does not clear `pending_block`. A later candidate for the same slot then hits the early
  `pending_block.has_value()` guard in `CandidateReceived` and is ignored.
- Probable cause:
  the slot-level pending-candidate state is mutated before validation is known to have succeeded,
  and the reject path does not roll that state back.
- What should be fixed:
  clear `pending_block` on validation rejection, or delay setting it until the candidate has passed
  the validation gate. The integration regression should remain red until the replacement
  candidate can notarize after an earlier reject.

## Shardchain integration never reaches empty-candidate mode after eight non-MC-finalized blocks

- Documentation / expectation: tracker `Kernel-28` expects shardchain leaders to switch into
  empty-candidate mode after eight accepted shard blocks that are still not finalized in the
  masterchain, and a restarted validator should then reaccept the pending block chain.
- Observed behavior:
  `restart-after-eight-non-mc-finalized-blocks-reaccepts-pending-candidates` is red in
  `test-consensus-adversarial.cpp`.
  The scenario waits until a live validator has accepted eight later shard blocks while validator
  `#0.0` is down, restarts `#0.0`, and then expects empty-candidate finalization plus catch-up.
  Instead the run still aborts with:
  `scenario never reached empty-candidate finalization after validator #0.0 fell behind`.
- Reproduce:
```sh
timeout 80s ./build/test/validator/consensus/test-consensus-adversarial --test-case restart-after-eight-non-mc-finalized-blocks-reaccepts-pending-candidates --verbosity 0
```
- Probable cause:
  either shardchain block production is not switching into the documented empty-candidate mode once
  the accepted-block lag reaches eight, or the later empty candidates are not making it through the
  end-to-end notarization/finalization path that the restarted validator depends on for reaccept.
- What should be fixed:
  inspect the end-to-end shardchain lag path around `validator/consensus/block-producer.cpp` and
  the subsequent state-resolution / acceptance flow. Once honest peers have accepted eight
  non-MC-finalized shard blocks, later leader windows should enter empty-candidate mode and a
  restarted validator should be able to reaccept the pending chain from that point.

## `BlockValidator` accepts empty candidates with an unrelated parent link

- Documentation / expectation: tracker `Kernel-23` / `Kernel-30` still expects malformed empty
  candidates to be rejected rather than accepted as valid chain extensions.
- Observed behavior: `BlockValidator_RejectsEmptyCandidateWithWrongParentLink` is red. An empty
  candidate that references the local `BlockIdExt` but carries an unrelated `parent_id` is still
  accepted.
- Reproduce:
```sh
./build/test/validator/consensus/test-block-validator --verbosity 0
```
- Narrowing evidence:
  `validator/consensus/block-validator.cpp` handles empty candidates by comparing only
  `candidate->block` to `event->state->as_normal()` and never inspects `candidate->parent_id`.
- Probable cause:
  empty-candidate structural validation is incomplete at the validator boundary. The actor accepts
  any empty candidate whose referenced block matches the local state, even if the candidate chain
  link is inconsistent.
- What should be fixed:
  decide which layer owns empty-candidate chain-link validation, then reject inconsistent
  `parent_id` values before acceptance. If the ownership remains in `BlockValidator`, extend the
  empty-candidate branch to validate the parent link against the expected chain context instead of
  checking only `BlockIdExt`.

## `BlockValidator` wakes masterchain child validation on non-final `FinalizeBlock`

- Documentation / expectation: tracker `Kernel-20` / `Kernel-23` expects a masterchain child to
  wait for true parent finalization, not merely notarization / approve-level progress.
- Observed behavior: `BlockValidator_RejectsMasterchainCandidateWhoseParentIsOnlyNotarized` is
  red. Publishing `FinalizeBlock` with a non-final signature set is enough to wake the waiter and
  let the child validate.
- Reproduce:
```sh
./build/test/validator/consensus/test-block-validator --verbosity 0
```
- Narrowing evidence:
  `validator/consensus/block-validator.cpp` handles every `FinalizeBlock` event with
  `on_new_accepted_block(event->candidate->block_id())` and never checks
  `event->signatures->is_final()`.
- Probable cause:
  the validator assumes every `FinalizeBlock` bus event already represents a locally accepted final
  block. That makes it vulnerable to premature wake-up if upstream ever emits the event before the
  signature set is actually final.
- What should be fixed:
  either tighten the event contract so non-final `FinalizeBlock` can never reach `BlockValidator`,
  or add a local `is_final()` guard before advancing `last_accepted_block_` for masterchain waiters.

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

## Simplex vote / certificate parsing still accepts an oversized skip slot

- Documentation / expectation: `tracker-test-plan.md` `Kernel-27` / `Kernel-31` requires malformed
  or oversized window-slot values to be rejected at parse time rather than silently reinterpreted.
- Observed behavior: `SimplexParser_CertificateFromTlRejectsOversizedWindowSlot` is red.
  A certificate carrying `skipVote(-1)` is still accepted and interpreted as slot `0xffffffff`.
- Reproduce:
```sh
./build/test/validator/consensus/test-simplex-parser --verbosity 0
```
- Narrowing evidence:
  `validator/consensus/simplex/votes.cpp` currently does
  `SkipVote::from_tl(const tl::skipVote& vote) { return {static_cast<td::uint32>(vote.slot_)}; }`.
  That means negative `int32` input is silently wrapped into a huge `uint32` slot before later
  certificate logic sees it.
- Probable cause:
  there is no range check when converting TL `skipVote.slot_` into the internal `td::uint32 slot`.
- What should be fixed:
  reject negative or otherwise invalid slot values in the vote parser before constructing
  `SkipVote`, and keep the parser-level regression red until that check is in place.
