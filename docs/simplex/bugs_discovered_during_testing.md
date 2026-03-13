# Bugs Discovered During Simplex Testing

This file tracks red Simplex tests on the current branch, but after rereading the implementation
and rerunning the failing cases it now separates them into five buckets:

- `Confirmed code issue`: the implementation is missing a check or keeps incorrect state.
- `Mixed`: there is a real code issue, but the current test also needs tightening.
- `Test / harness issue`: the red test does not currently demonstrate a live product bug.
- `Hardening opportunity`: the behavior is only wrong if a wider actor contract is allowed.
- `Resolved test / harness update`: the earlier red test was fixed locally and is now green.

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

## Mixed: real code issue, but the current test also needs tightening

## Rule 6 skip-timeout integration scenario is still red in `test-consensus`

- Status: `Mixed`.
- Documentation / expectation: Rule 6 says the skip timeout in window `k` must be lower-bounded by
  the last observed finalization, not by whether the immediately previous window happened to have a
  skip. `simplex_docs.md` already documents the current implementation as wrong here.
- Reproduce:
```sh
timeout 60s ./build/test/validator/consensus/test-consensus --test-case skip-timeout-uses-last-finalization --n-nodes 4 --target-rate-ms 100 --duration 6 --verbosity 0
```
- Current failure:
  `scenario did not produce any SkipVote in window 2 or later; observed skip windows=[]`
- Why the product bug is real:
  `validator/consensus/simplex/consensus.cpp` still derives timeout growth from
  `previous_window_had_skip_`. On each `LeaderWindowObserved`, a clean window resets
  `first_block_timeout_s_` to the default, which is exactly the deviation called out in
  `simplex_docs.md`.
- Why the current end-to-end test also needs work:
  the scenario currently aborts before it reaches the intended lower-bound timing assertion. So it
  still proves "Rule 6 behavior is wrong enough that the scenario breaks", but it no longer cleanly
  isolates the exact late-window timing violation by itself.
- Proposed production change:
  track the last window in which a finalization was observed and derive `first_block_timeout_s_`
  from that value instead of from `previous_window_had_skip_`.
- Proposed test change:
  keep the scenario red until the product fix lands, but tighten it so it first proves that a later
  skip actually occurred and only then compares the observed delay with the Rule 6 lower bound.

## Resolved test / harness updates

## `test-pool`: standstill rebroadcast coverage now passes

- Status: `Resolved test / synchronization issue`.
- Test:
  `StandstillBroadcastContainsExpectedVotesAndCertificates`
- Previous failure mode:
  the standstill snapshot sometimes omitted the locally generated vote because the test published
  `BroadcastVote`, waited once, and then assumed the detached signing path had already stored that
  vote in pool state before the standstill deadline.
- Why the old entry is no longer an active product issue:
  `PoolImpl::alarm()` already serializes local votes that actually reached pool state. The flaky part
  was the test barrier, not standstill serialization itself.
- Fix now in tree:
  the test injects a fully signed local vote deterministically via `IncomingProtocolMessage` and
  checks the outgoing standstill payloads with a decoded-vote helper instead of racing the async
  `BroadcastVote` path.
- Current verification:
```sh
./build/test/validator/consensus/test-pool --filter StandstillBroadcastContainsExpectedVotesAndCertificates --verbosity 0
./build/test/validator/consensus/test-pool --verbosity 0
```
  both pass.

## `test-block-validator`: empty-candidate parent-link case was a contract mismatch, not a live bug

- Status: `Resolved test issue / actor-boundary correction`.
- Previous red test:
  `BlockValidator_RejectsEmptyCandidateWithWrongParentLink`
- Why the old entry is no longer active:
  the live pipeline resolves `candidate->parent_id` upstream and passes the resulting state into
  `ValidationRequest`. `BlockValidator` validates the candidate against that supplied state; it does
  not independently reinterpret the parent link at this boundary. The removed test constructed an
  inconsistent `(state, candidate)` pair that upstream actors do not produce.
- Fix now in tree:
  the old negative test was replaced with
  `AcceptsEmptyCandidateWhenResolvedStateMatchesReference`, which exercises the actual
  `ValidationRequest` contract by proving acceptance when the empty candidate's reference matches
  the already-resolved state.
- Current verification:
```sh
./build/test/validator/consensus/test-block-validator --filter AcceptsEmptyCandidateWhenResolvedStateMatchesReference --verbosity 0
```
  this passes. The remaining red `test-block-validator` case is still
  `BlockValidator_RejectsMasterchainCandidateWhoseParentIsOnlyNotarized`, which stays above in the
  confirmed-code-issue section.

## `test-consensus-adversarial`: restart / empty-mode scenario now reaches the intended state

- Status: `Resolved harness issue`.
- Test:
  `restart-after-eight-non-mc-finalized-blocks-reaccepts-pending-candidates`
- Previous failure mode:
  the harness kept publishing `BlockFinalizedInMasterchain` from generic shard finalization and
  seeded restarts from `last_accepted_block_`, so the synthetic masterchain-finalized watermark
  never actually lagged behind shard progress by eight blocks. The verifier also mapped finalized
  slots to accepted shard seqnos off by one.
- Why the old entry is no longer active:
  the scenario now creates the intended precondition instead of accidentally keeping masterchain and
  shard finality coupled inside the test harness.
- Fix now in tree:
  the harness freezes the synthetic MC-finalization watermark at the chosen stop slot, restarts from
  that synthetic watermark instead of `last_accepted_block_`, and checks reaccepted shard seqnos
  with the corrected slot-to-seqno mapping.
- Current verification:
```sh
timeout 100s ./build/test/validator/consensus/test-consensus-adversarial --test-case restart-after-eight-non-mc-finalized-blocks-reaccepts-pending-candidates --verbosity 0
```
  this passes.
