## `ConsensusImpl` still notarizes a wrong-leader candidate after `CandidateReceived`

- Status: `Confirmed code issue`.
- Coverage note: this merges the earlier "End-to-end consensus still notarizes a wrong-leader candidate after `CandidateReceived`" and "`ConsensusImpl` still notarizes a candidate that names the wrong slot leader" sections.
- Documentation / expectation: tracker `Kernel-23` / `Kernel-28` expects an otherwise well-formed candidate with the wrong leader for its slot to stop before local `Notar`.
- Reproduce:
  there is no longer a dedicated runnable reproducer on this branch: the old
  `test-consensus --test-case wrong-leader-candidate-never-notarizes` selector was removed from
  `test/validator/consensus/test-consensus.cpp`. The issue is still directly visible in
  `validator/consensus/simplex/consensus.cpp`: `handle(CandidateReceived)` at lines 174-192 stores
  `pending_block` and starts `try_notarize()` without checking the slot's expected leader.
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

## Full-network clean restart replays already-persisted local votes

- Status: `Confirmed code issue`.
- Coverage note: exposed by
  `test/validator/consensus/integration/test_reboots.cpp`
  `full-network-clean-restart-still-finalizes`.
- Reproduce:
```sh
timeout 120 ./build/test/validator/consensus/integration/test-reboots --test-case full-network-clean-restart-still-finalizes
```
- Current failure:
  the reboot verifier fails with
  `full-network clean restart: node #... retried persisting local vote SkipVote{slot=2} after restart`.
- How the failure arises:
  the scenario stops every validator on intact state, restarts the whole set, and then waits for the
  network to resume steady-state finalization. During that restart, the trace now records restarted
  validators trying to persist local votes that are already present in their simplex DB bootstrap state.
  The clean restart run stays live long enough to finish, but the explicit reboot verifier turns that
  duplicate-persistence behavior into a deterministic red result.
- Why this is not just a bad test:
  the duplicate guard is not unique to the integration double. The real simplex DB applies the same
  "our vote already exists" check, so a restart path that re-emits previously persisted local votes is a
  real replay-idempotence regression, not a harness artifact.
- Bigger-picture impact:
  a validator that reboots cleanly on intact local state can re-enter consensus by trying to save old
  local votes again. Current evidence says this likely surfaces in production first as detached-task error
  logs and fragile recovery behavior, not as an immediate validator-process crash. Even so, it makes reboot
  recovery timing-sensitive and risks turning otherwise safe operator restarts into liveness failures.
- Proposed production change:
  make restart / bootstrap idempotent with respect to previously persisted local votes, so replayed local
  protocol state never re-enters the "vote already casted" path.

## Full-network restart with one wiped validator still aborts before recovery completes

- Status: `Confirmed code issue`.
- Coverage note: exposed by
  `test/validator/consensus/integration/test_reboots.cpp`
  `full-network-restart-with-state-loss-still-recovers`.
- Reproduce:
```sh
timeout 180 ./build/test/validator/consensus/integration/test-reboots --test-case full-network-restart-with-state-loss-still-recovers
```
- Current failure:
  the scenario aborts during the recovery handoff instead of reaching the final "all validators finalize
  after the state-lost node catches up" phase. The exact abort point is timing-sensitive: in stronger runs
  it can happen right after the surviving quorum proves progress while the wiped node is still down, and in
  other runs it happens shortly after the wiped node reaches its first post-restart finalization.
- How the failure arises:
  the scenario waits for normal finalization, stops all validators, clears one validator's simplex DB,
  restarts the surviving quorum first, requires them to finalize a newer slot, and then tries to let the
  wiped validator catch up. The branch still aborts before that recovery flow reaches steady state.
- Why this is not just a bad test:
  the scenario is now stricter than the earlier broken reproducer: it records the pre-restart finalized
  frontier and waits for genuinely newer finalization instead of accepting replay of old trace history.
  The old narrow wording about one specific pre-restart `Vote was already casted` symptom was stale and
  has been removed; the corrected test still exposes a real state-loss reboot failure.
- Bigger-picture impact:
  full validator-set restart with one node losing only local simplex state is still not a reliable recovery
  path. Even after the healthy quorum proves it can make forward progress, the branch fails to bring the
  wiped validator back to stable participation.
- Root-cause note:
  this issue is confirmed by the end-to-end reproducer above, but I have not yet reduced it to a single
  precise source line in `validator/consensus/simplex`. The stable fact today is the end-to-end failure to
  complete recovery, not one narrow timing-specific symptom.

## `ConsensusImpl` leaves a rejected candidate stuck as the slot's pending block

- Status: `Confirmed code issue`.
- Coverage note: this merges the earlier "End-to-end consensus leaves a rejected slot wedged and
  ignores a replacement candidate" and "`ConsensusImpl` leaves a rejected candidate stuck as the
  slot's pending block" sections.
- Documentation / expectation: tracker `Kernel-23` expects a validation rejection to leave no
  residual votes or slot state, so a later replacement candidate for the same slot can still
  progress normally through notarization.
- Reproduce:
  there is no longer a dedicated runnable reproducer on this branch: the old
  `test-consensus --test-case validation-reject-does-not-leave-residual-votes` selector was
  removed from `test/validator/consensus/test-consensus.cpp`. The issue is still directly visible
  in `validator/consensus/simplex/consensus.cpp`: after `CandidateReject`, lines 227-231 return
  without clearing `slot.state->pending_block`.
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

## `SimplexPool::WaitForParent` rejects the latest already-finalized candidate for its slot

- Status: `Confirmed code issue`.
- Coverage note: exposed by `test/validator/consensus/test-pool.cpp`
  `Pool_WaitForParentAcceptsLatestFinalizedCandidateAtSameSlot`.
- Reproduce:
```sh
timeout 45s ./build/test/validator/consensus/test-pool --filter Pool_WaitForParentAcceptsLatestFinalizedCandidateAtSameSlot --verbosity 0
```
- Current failure:
  `Expectation failed: result.is_ok()!`
  and the pool logs the response error `Candidate's slot is already finalized`.
- How the failure arises:
  `validator/consensus/simplex/pool.cpp` `maybe_resolve_request()` rejects every request whose
  `candidate.id.slot < first_nonfinalized_slot_` before it checks whether the requested candidate
  is exactly `last_finalized_block_`. The branch already has a same-candidate fast path for an
  already-notarized slot, but the stronger "already finalized to this same block" case still falls
  into the generic error branch.
- Why this is not just a bad test:
  for callers of `WaitForParent`, a finalization certificate for the exact same candidate is
  stronger evidence than a notarization certificate. Returning a hard error for the already-final
  candidate makes "the parent condition is satisfied" depend on whether the slot stopped at notar or
  advanced to finalization, which is not a sensible API contract.
- Bigger-picture impact:
  any actor that asks the pool whether a candidate's parent chain is satisfied can misclassify a
  fully finalized candidate as a conflict and abort the normal pipeline or replay / recovery logic.
  That is a real correctness regression at the pool boundary, not just an overly strict test.
- Proposed production change:
  special-case the exact `last_finalized_block_` match before the generic finalized-slot rejection.
- Smallest intended patch shape:
```diff
--- a/validator/consensus/simplex/pool.cpp
+++ b/validator/consensus/simplex/pool.cpp
@@
   td::uint32 next_slot_after_parent = parent.has_value() ? parent->slot + 1 : 0;
 
   if (id.slot < first_nonfinalized_slot_) {
+    if (last_finalized_block_ == id) {
+      return resolve_with(std::nullopt);
+    }
     return resolve_with(td::Status::Error("Candidate's slot is already finalized"));
   }
```

## `SimplexPool` standstill replay still emits stale messages after a newer finalization

- Status: `Confirmed code issue`.
- Coverage note: exposed by `test/validator/consensus/test-pool.cpp`
  `Pool_StandstillReplayRestartsFromNewFinalization`.
- Reproduce:
```sh
timeout 45s ./build/test/validator/consensus/test-pool --filter Pool_StandstillReplayRestartsFromNewFinalization
```
- Current failure:
  `Expectation failed: count_events<OutgoingProtocolMessage>(events()) is not equal to static_cast<size_t>(0) (1 != 0)`
  followed by
  `Expectation failed: outgoing_payload_position(events(), new_final_payload_.as_slice()) is not equal to static_cast<size_t>(0)`
- How the failure arises:
  `validator/consensus/simplex/pool.cpp` snapshots `last_final_cert_` into
  `last_final_cert_copy` and stops the outer replay loop once the frontier changes, but it does not
  re-check that condition inside the per-slot message loop. If a newer finalization arrives while
  the replay coroutine is sleeping in `send(...)` or iterating the current slot's message vector,
  the old sweep can still emit stale payloads from the previous frontier before it notices the
  change. The failing test catches exactly that: an old `SkipVote{slot=1}` certificate leaks onto
  the wire ahead of the new finalization frontier.
- Why this is not just a bad test:
  the branch changed standstill recovery specifically to restart from the newest finalization. Once
  the frontier advances, continuing to broadcast older replay payload from the superseded sweep is
  contrary to that design and wastes the very bandwidth this path was added to control.
- Bigger-picture impact:
  during recovery after stalls, validators can spend scarce standstill-recovery egress on obsolete
  proofs and may advertise an older replay item ahead of the new frontier certificate. That slows
  convergence and makes recovery ordering nondeterministic exactly when the pool is supposed to be
  helping lagging peers catch up.
- Proposed production change:
  make the replay cancellation frontier-aware at message granularity, not just at slot granularity.
- Smallest intended patch shape:
```diff
--- a/validator/consensus/simplex/pool.cpp
+++ b/validator/consensus/simplex/pool.cpp
@@
         std::vector<ProtocolMessage> messages;
         state->certs.serialize_to(messages);
         state->votes[bus.local_id.idx.value()].serialize_to(messages, state->certs);
 
         for (auto &message : messages) {
+          if (last_final_cert_ != last_final_cert_copy) {
+            break;
+          }
           co_await send(std::move(message));
         }
```

## Standstill egress-rate updates do not cleanly take effect on the next replay cycle

- Status: `Confirmed code issue`.
- Coverage note: exposed by `test/validator/consensus/test-pool.cpp`
  `Pool_NoncriticalParamsUpdateChangesStandstillEgressForNextCycle`.
- Reproduce:
```sh
timeout 45s ./build/test/validator/consensus/test-pool --filter Pool_NoncriticalParamsUpdateChangesStandstillEgressForNextCycle
```
- Current failure:
  `Expectation failed: second_replay_delay < first_replay_delay / 100.0!`
- How the failure arises:
  `validator/consensus/simplex/pool.cpp` updates `params_` on `NoncriticalParamsUpdated`, but
  `alarm()` keeps signaling and rescheduling standstill recovery even while a prior replay is still
  active. Those queued notifications let an old standstill cycle roll directly into another replay,
  so the "next cycle" after an egress-rate update is often not a clean fresh cycle started under
  the new pacing parameters. The failing test sees exactly that: the second replay delay still looks
  like old carried-over work instead of collapsing once `standstill_max_egress_bytes_per_s` is
  raised by many orders of magnitude.
- Why this is not just a bad test:
  runtime noncritical params are explicitly meant to apply without restart, and adjacent pool tests
  already prove that live updates affect ban duration and standstill timeout. The egress-rate knob
  is part of the same runtime surface, so stale queued standstill work should not mask the next
  cycle's pacing.
- Bigger-picture impact:
  operators can raise standstill recovery bandwidth during an incident and still observe old slow
  pacing for extra cycles, which undermines the purpose of the live override and makes recovery
  timing unpredictable.
- Proposed production change:
  coalesce standstill notifications while recovery is already pending / active, or explicitly
  restart the standstill replay loop when standstill pacing params change.
- Smallest intended patch shape:
```diff
--- a/validator/consensus/simplex/pool.cpp
+++ b/validator/consensus/simplex/pool.cpp
@@
-  void alarm() override {
+  void alarm() override {
     ...
-    standstill_resolution_notification_.set_value({});
+    if (!standstill_resolution_pending_) {
+      standstill_resolution_pending_ = true;
+      standstill_resolution_notification_.set_value({});
+    }
     reschedule_standstill_resolution();
   }
@@
     while (true) {
       co_await std::move(standstill_resolution_awaiter_);
+      standstill_resolution_pending_ = false;
       std::tie(standstill_resolution_awaiter_, standstill_resolution_notification_) =
           td::actor::StartedTask<>::make_bridge();
```
