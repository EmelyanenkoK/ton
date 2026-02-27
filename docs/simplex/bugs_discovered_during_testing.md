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
