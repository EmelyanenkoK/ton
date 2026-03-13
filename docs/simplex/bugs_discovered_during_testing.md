# Bugs Discovered During Simplex Testing

This file tracks still-open issues discovered during Simplex testing on the current branch.

Consolidated duplicates:

- The two wrong-leader entries were the same issue and are merged below.
- The two validation-reject / stuck `pending_block` entries were the same issue and are merged below.
- The two oversized skip-slot parser entries were the same issue and are merged below.

## Confirmed code issues

## `ConsensusImpl` still notarizes a wrong-leader candidate after `CandidateReceived`

- Status: `Confirmed code issue`.
- Coverage note: this merges the earlier "End-to-end consensus still notarizes a wrong-leader candidate after `CandidateReceived`" and "`ConsensusImpl` still notarizes a candidate that names the wrong slot leader" sections.
- Documentation / expectation: tracker `Kernel-23` / `Kernel-28` expects an otherwise well-formed candidate with the wrong leader for its slot to stop before local `Notar`.
- Reproduce:
```sh
timeout 45s ./build/test/validator/consensus/test-consensus --test-case wrong-leader-candidate-never-notarizes --verbosity 0
```
- Current failure:
  `validator #0.0 sent Notar for wrong-leader candidate ...`
- Why this is not just a bad test:
  the network parser already rejects wrong-leader broadcasts in `validator/consensus/types.cpp`, but
  `CandidateReceived` explicitly promises only "a valid signature from `candidate->leader`". That
  means the consensus actor itself still has to decide whether to trust the slot leader field at
  this boundary.
- How the failure arises:
  `validator/consensus/simplex/consensus.cpp` `handle(CandidateReceived)` checks only "too new",
  "future parent", and "slot already has `pending_block`". It does not compare
  `candidate->leader` with `collator_schedule->expected_collator_for(slot_idx)`. After storing the
  candidate in `pending_block`, it only emits a trace if the leader is not us and still starts
  `try_notarize()`. The injected wrong-leader candidate therefore goes through `WaitForParent`,
  `ResolveState`, validation, and finally local `BroadcastVote(NotarizeVote{...})`.
- Bigger-picture impact:
  the normal remote-entry path is partially protective: `PrivateOverlay` and `CandidateResolver`
  both call `Candidate::deserialize(...)`, and that parser already derives the expected leader from
  the slot and rejects a broadcast whose source does not match `expected_collator_for(slot)`.
  `BlockProducer` also self-publishes only locally scheduled candidates. So a remote peer cannot
  exploit this through the standard overlay path today. The residual risk is the internal contract:
  any future actor, replay tool, fuzz harness, DB corruption, or refactor that injects
  `CandidateReceived` directly can make honest validators spend validation work and cast `Notar`
  for an unauthorized leader. If that bypass reached enough validators, the wrong-leader candidate
  could crowd out the scheduled leader for that slot and turn a fairness / schedule violation into a
  real liveness fault or even an accepted block under a leader the wire parser would have rejected.
- Proposed production change:
  reject the candidate before setting `pending_block` if its declared leader does not match the slot
  schedule. If the intent is parser-only enforcement, then the `CandidateReceived` contract should
  be tightened and this integration test should move to the parser boundary instead of staying red
  here.
- Smallest intended patch shape:
```diff
--- a/validator/consensus/simplex/consensus.cpp
+++ b/validator/consensus/simplex/consensus.cpp
@@
   const auto& candidate = event->candidate;
+  auto expected_leader = owning_bus()->collator_schedule->expected_collator_for(slot_idx);
+  if (candidate->leader != expected_leader) {
+    // FIXME: report misbehavior or trace the rejected candidate
+    return;
+  }
 
   if (candidate->parent_id.has_value() && candidate->parent_id->slot >= candidate->id.slot) {
     // FIXME: report misbehavior
     return;
   }
```

## `ConsensusImpl` leaves a rejected candidate stuck as the slot's pending block

- Status: `Confirmed code issue`.
- Coverage note: this merges the earlier "End-to-end consensus leaves a rejected slot wedged and
  ignores a replacement candidate" and "`ConsensusImpl` leaves a rejected candidate stuck as the
  slot's pending block" sections.
- Documentation / expectation: tracker `Kernel-23` expects a validation rejection to leave no
  residual votes or slot state, so a later replacement candidate for the same slot can still
  progress normally through notarization.
- Reproduce:
```sh
timeout 45s ./build/test/validator/consensus/test-consensus --test-case validation-reject-does-not-leave-residual-votes --verbosity 0
```
- Current failure:
  `replacement candidate ... never notarized after the first candidate was rejected`
- How the failure arises:
  `validator/consensus/simplex/consensus.cpp` stores the first candidate in
  `slot->state->pending_block` before validation begins. `try_notarize()` then awaits
  `WaitForParent`, `ResolveState`, and `ValidationRequest`. If validation returns
  `CandidateReject`, the coroutine logs the rejection and returns, but it never clears
  `pending_block`. A later candidate for the same slot but a different id immediately hits the
  early `pending_block.has_value()` guard in `handle(CandidateReceived)` and is ignored.
- Why the current green unit tests were not enough:
  `test-simplex-consensus.cpp` already proves "reject does not cast a vote or report
  misbehavior", but there was no unit test for rolling back slot-local pending state after a
  rejection. The end-to-end test is catching exactly that missing rollback.
- Bigger-picture impact:
  this is a liveness / participation bug, not a direct safety bug. The rejecting validator does not
  vote for the bad candidate, and the later skip path still works because `alarm()` ignores
  `pending_block` and can mark the slot skipped. If only a minority of validators wedge on the
  rejected candidate, the healthy majority can still notarize a replacement and the wedged nodes can
  catch up from later certificates. The worst case is when the same doomed candidate reaches a large
  fraction of validators first: then the whole network can lose the replacement candidate for that
  slot, fail to form quorum there, and fall back to skip / later windows instead of using the valid
  replacement. In other words, the bug turns one rejected candidate into avoidable throughput loss
  and a stronger adversarial stall amplifier, but it does not by itself create conflicting finalized
  blocks.
- Proposed production change:
  keep the current deduplication behavior for an in-flight candidate, but clear `pending_block` on
  rejection so a later replacement can restart the pipeline. Delaying the initial assignment until
  after validation is also possible, but then some other "in flight" marker is still needed to
  avoid duplicate concurrent validation of the same candidate.
- Smallest intended patch shape:
```diff
--- a/validator/consensus/simplex/consensus.cpp
+++ b/validator/consensus/simplex/consensus.cpp
@@
   if (validation_result.has<CandidateReject>()) {
     LOG(WARNING) << "Candidate " << candidate->id
                  << " is rejected: " << validation_result.get<CandidateReject>().reason;
+    if (slot.state->pending_block == candidate) {
+      slot.state->pending_block.reset();
+    }
     // FIXME: Report misbehavior
     co_return {};
   }
```

## `BlockValidator` wakes masterchain child validation on non-final `FinalizeBlock`

- Status: `Confirmed code issue`, but specifically at the actor boundary.
- Documentation / expectation: tracker `Kernel-20` / `Kernel-23` expects a masterchain child to
  wait for true parent finalization, not merely notarization / approve-level progress.
- Reproduce:
```sh
./build/test/validator/consensus/test-block-validator --verbosity 0
```
- Current red test:
  `BlockValidator_RejectsMasterchainCandidateWhoseParentIsOnlyNotarized`
- How the failure arises:
  `validator/consensus/block-validator.cpp` handles every `FinalizeBlock` event by calling
  `on_new_accepted_block(event->candidate->block_id())` without checking
  `event->signatures->is_final()`. That lets a non-final `FinalizeBlock` advance
  `last_accepted_block_`, which can wake a waiting masterchain child early.
- Why this is still worth treating as code, not just test shape:
  `FinalizeBlock` does not carry a contract saying that its signatures must already be final, and
  another production actor, `BlockProducer`, already treats that distinction as important and checks
  `event->signatures->is_final()` before advancing its own masterchain watermark. The missing guard
  in `BlockValidator` is therefore a real boundary inconsistency even if the current Simplex
  publisher usually sends final signatures on masterchain.
- Bigger-picture impact:
  under the current Simplex wiring, the live risk is lower than the raw actor bug: in
  `simplex/state-resolver.cpp`, masterchain `finalize_blocks_inner(...)` returns early when it has
  only a notar certificate, so the normal Simplex publisher does not emit non-final
  `FinalizeBlock` events for masterchain blocks. `BlockProducer` also already distinguishes final
  from non-final signatures. So today this mostly remains an actor-boundary footgun. The worst case
  appears if another publisher or a future refactor violates that implicit contract: then a
  masterchain validator could start validating children of a merely notarized parent, wasting work
  on one node and, if repeated across many validators, allowing child candidates to be considered
  too early relative to true masterchain finality. That would still be a liveness / ordering fault
  first, but it is exactly the kind of boundary inconsistency that can become consensus-visible once
  the surrounding assumptions change.
- Proposed production change:
  guard `handle(FinalizeBlock)` with `is_final()`. If the event is meant to be final-only, also
  document that contract explicitly in `bus.h`.
- Smallest intended patch shape:
```diff
--- a/validator/consensus/block-validator.cpp
+++ b/validator/consensus/block-validator.cpp
@@
   template <>
   void handle(BusHandle, std::shared_ptr<const FinalizeBlock> event) {
-    on_new_accepted_block(event->candidate->block_id());
+    if (event->signatures->is_final()) {
+      on_new_accepted_block(event->candidate->block_id());
+    }
   }
```

## `simplex::Certificate::from_tl` accepts malformed skip-slot values after signed `int32 -> uint32` wraparound

- Status: `Confirmed code issue`.
- Coverage note: this merges the earlier wraparound section and the later "Simplex vote /
  certificate parsing still accepts an oversized skip slot" section.
- Documentation / expectation: tracker parser rows `Kernel-27` / `Kernel-31` require malformed or
  oversized window-slot values to be rejected rather than silently reinterpreted.
- Reproduce:
```sh
./build/test/validator/consensus/test-simplex-parser --verbosity 0
```
- Current red test:
  `SimplexParser_CertificateFromTlRejectsOversizedWindowSlot`
- Current failure:
  `skipVote(-1)` is accepted and becomes slot `4294967295`.
- How the failure arises:
  `validator/consensus/simplex/votes.cpp` converts TL `skipVote.slot_` with
  `static_cast<td::uint32>(vote.slot_)` and performs no range check first. In
  `validator/consensus/simplex/certificate.cpp`, the parser verifies signatures over the raw TL vote
  and only then converts it into the internal `Vote`, so the negative `int32` survives long enough
  to become a wrapped `uint32` instead of a parse error.
- Bigger-picture impact:
  the live-network blast radius is narrower than the parser unit failure alone suggests. Honest
  nodes serialize skip slots from local `uint32` values, so they do not naturally emit negative TL
  slots; a malformed vote also still needs a valid signature from the claimed validator, and a
  malformed certificate needs a real quorum of signatures before `Certificate::from_tl(...)` will
  accept it. A single bad peer therefore cannot cheaply push this through the normal protocol. The
  remaining risk is at corruption / Byzantine boundaries: malformed signed votes are treated as
  gigantic future slots instead of parse failures, and malformed quorum certificates can be saved to
  DB and rebroadcast rather than quarantined. That can pollute persistent local state with
  impossible future-slot certificates and distort recovery / observability, but it is not a cheap
  safety break against an otherwise honest network.
- Proposed production change:
  reject negative slot values before the cast. This should be a parser error, not a `CHECK`, so
  the likely implementation shape is to make `SkipVote::from_tl` and `Vote::from_tl` return
  `td::Result<...>` and propagate that through `Signed::from_tl` and `Certificate::from_tl`.
- Smallest intended patch shape:
```diff
--- a/validator/consensus/simplex/votes.h
+++ b/validator/consensus/simplex/votes.h
@@
-  static SkipVote from_tl(const tl::skipVote& vote);
+  static td::Result<SkipVote> from_tl(const tl::skipVote& vote);

--- a/validator/consensus/simplex/votes.cpp
+++ b/validator/consensus/simplex/votes.cpp
@@
-SkipVote SkipVote::from_tl(const tl::skipVote& vote) {
-  return {static_cast<td::uint32>(vote.slot_)};
+td::Result<SkipVote> SkipVote::from_tl(const tl::skipVote& vote) {
+  if (vote.slot_ < 0) {
+    return td::Status::Error("Negative skip slot");
+  }
+  return SkipVote{static_cast<td::uint32>(vote.slot_)};
 }
```

## `BlockProducer` does not respect the remaining collation budget of the current slot

- Status: `Confirmed code issue`.
- Documentation / expectation: `tracker-test-plan.md` `Kernel-22` says the collator should receive
  the remaining time budget of the current slot, and a collation result that arrives after the slot
  budget expires should not still be published for that slot.
- Reproduce:
```sh
./build/test/validator/consensus/test-block-producer --verbosity 0
```
- Current red tests:
  `BlockProducer_PassesRemainingCollationBudgetToManager`
  `BlockProducer_StopsProducingWhenCollationBudgetExpires`
- How the failure arises:
  `validator/consensus/block-producer.cpp` starts each slot with `target_time = event->start_time`,
  but when it builds `CollateParams` it ignores how much of the slot has already elapsed and passes
  `soft_timeout = td::Timestamp::in(target_rate_)`, effectively giving the collator a fresh full
  budget even if the window started late. After collation returns, the code checks only whether the
  leader window changed; it never re-checks whether the slot deadline itself has already passed.
- Bigger-picture impact:
  this bug does not let arbitrary invalid blocks bypass consensus. A late candidate still goes
  through the normal validation, notarization, and finalization pipeline, and if the leader window
  has already been superseded then the existing `current_leader_window_` guard drops the stale
  result. The real system-level consequence is timing distortion inside an still-active leader
  window: a slow or overloaded collator gets more wall-clock time than configured, can still publish
  a late candidate for the expired slot, and can delay the protocol's intended handoff to skip /
  empty-block fallback. At scale, that hurts throughput and makes performance under overload or
  adversarial slowdown worse, but quorum-based safety is still enforced downstream.
- Proposed production change:
  derive the collator deadline from the actual end of the current slot and drop late collation
  results before publishing `CandidateGenerated` / `CandidateReceived`.
- Smallest intended patch shape:
```diff
--- a/validator/consensus/block-producer.cpp
+++ b/validator/consensus/block-producer.cpp
@@
-      td::Timestamp target_time = event->start_time;
+      td::Timestamp target_time = event->start_time;
@@
-        CollateParams params{
+        td::Timestamp slot_deadline = td::Timestamp::in(target_rate_, target_time);
+        CollateParams params{
             .shard = bus.shard,
@@
-            .soft_timeout = td::Timestamp::in(target_rate_),
+            .soft_timeout = slot_deadline,
@@
         auto block_candidate = co_await td::actor::ask(...);
@@
-      if (current_leader_window_ != window) {
+      if (current_leader_window_ != window || td::Timestamp::now() > td::Timestamp::in(target_rate_, target_time)) {
         break;
       }
```

## `ConsensusImpl` resets skip timeout from `previous_window_had_skip_` instead of the last observed finalization

- Status: `Confirmed code issue`.
- Documentation / expectation: Rule 6 says the skip timeout in window `k` must be lower-bounded by
  the last observed finalization, not by whether the immediately previous window happened to have a
  skip. `simplex_docs.md` already documents the current implementation as wrong here.
- Reproduce:
```sh
timeout 60s ./build/test/validator/consensus/test-consensus --test-case skip-timeout-uses-last-finalization --n-nodes 4 --target-rate-ms 100 --duration 6 --verbosity 0
```
- Current failure:
  `validator #0.0 emitted no SkipVote in stalled window 2 even though finalization stayed frozen after window 0; observed skip windows=[4,8]`
- Why the product bug is real:
  `validator/consensus/simplex/consensus.cpp` still derives timeout growth from
  `previous_window_had_skip_`. On each `LeaderWindowObserved`, a clean window resets
  `first_block_timeout_s_` to the default, which is exactly the deviation called out in
  `simplex_docs.md`.
- Why this is now code-only rather than mixed:
  the integration harness now drops both `FinalizeVote` and final-cert traffic to validator `#0.0`
  from the "clear without skip" window onward, proves that no later finalization was observed,
  proves that validator `#0.0` reached the intended stalled window, and proves that the
  intermediate clear window still had no local skip. With those setup conditions satisfied, the
  current code still fails to emit a `SkipVote` in the first stalled window and only does so much
  later (`[4,8]` in the current run). That is a product failure, not a harness ambiguity.
- Bigger-picture impact:
  this is a recovery / liveness bug, not a direct safety break. The rest of Simplex still enforces
  certificate quorums, single-slot local voting invariants, and finalization-before-state-advance
  rules; the bad state variable only changes when a validator gives up and casts `Skip`. The
  consequence is that fallback timing becomes unpredictable under intermittent progress: some
  patterns reset too aggressively after a clean-but-nonfinal window, while the currently reproduced
  pattern delays the first skip until much later windows (`[4,8]`) after finalization froze. In the
  larger system that means targeted leader outages, packet loss, or temporary validator stalls can
  hold the network in nonfinal progress longer than the protocol model assumes, reducing throughput
  and delaying recovery, even though safety thresholds remain intact.
- Proposed production change:
  track the last window in which a finalization was observed and derive `first_block_timeout_s_`
  from that value instead of from `previous_window_had_skip_`.
- Test tightening now in tree:
  `test-consensus.cpp` and `test-consensus-adversarial.cpp` both choose the first later stalled
  window with a non-local leader, freeze validator `#0.0`'s observed finalization by dropping
  final votes and final certificates from the preceding clear window onward, keep later windows
  stalled long enough that the product must either skip there or visibly fail to do so, and verify
  the setup invariants before judging the skip behavior. If the implementation begins skipping in
  the stalled window, the same verifier still checks the Rule 6 lower-bound timing next.
