# Reboot Coverage Plan

This plan turns the reboot-review findings into concrete test work for `test/validator/consensus`.

## Current coverage

- `restart-after-eight-non-mc-finalized-blocks-reaccepts-pending-candidates`
  Covers a lagging validator restart after falling behind into empty-candidate mode.
- `candidate-resolution-recovery`
  Covers restart after local simplex-state loss and later candidate-resolution catch-up.
- `offline-cohort-restart-handoff-still-finalizes`
  Covers rotating stop/start of validator cohorts while a live quorum remains online.
- `offline-cohort-restart-handoff-forced-skip-window-still-finalizes`
  Covers the same cohort restart handoff under a long forced-skip window.
- `leader-restart-mid-window-still-rejoins-after-honest-finalization`
  Covers restarting the current leader after it has already put its own slot in flight, then
  requiring honest progress while it is down and later rejoin voting/finalization.
- `standstill-rebroadcast-replayer-restarts-still-recovers`
  Covers rebooting the surviving standstill replayer after rebroadcast starts and requiring replay
  to resume before the lagging validator can catch up.
- `test-reboots: single-validator-restart-rejoins-consensus`
  Covers a clean single-validator stop/start with explicit rejoin verification in the integration harness.
- `test-reboots: repeated-validator-restarts-rejoin-after-second-downtime`
  Covers repeated reboot churn of the same validator and requires rejoin after the second downtime too.
- `test-reboots: full-network-clean-restart-still-finalizes`
  Covers a clean all-validator cold restart on intact state and requires the restarted network to resume
  voting and later steady-state finalization.
- `test_consensus_reboots.py`
  Covers a real validator-engine process restart under live consensus: a stable observer keeps seeing
  masterchain progress while one validator process is down, then the restarted process must catch up and
  keep following the live tip through its own tonlib endpoint.
- `test-simplex-db`
  Covers actor-level restart replay of Db bootstrap state and leader-window checkpoints.
- `test_noncritical_params_persistence.py`
  Covers real node-process restart for engine noncritical-params persistence, not consensus liveness.
- `test-reboots --test-case full-network-restart-with-state-loss-still-recovers`
  Implements the planned all-node restart + one-node state-loss scenario and is kept in CTest even though
  the current branch still aborts before recovery completes.

## Main gaps from the review

- No remaining coverage gaps from the original review.
- Current blockers:
  the clean full-network restart case is now intentionally red because the strengthened verifier catches
  duplicate local-vote persistence on restarted honest validators.
  the all-node cold-restart variant with one wiped validator is also intentionally red because the branch
  still aborts before that recovery scenario reaches steady state.

## Follow-up order

1. Fix the clean-restart duplicate local-vote persistence bug so `reboot-full-network-clean-restart-still-finalizes`
   turns green.
2. Reduce and fix the state-loss restart abort so `reboot-full-network-restart-with-state-loss-still-recovers`
   can complete recovery.
3. Re-run the real process-level Python reboot test in an environment with `Python >= 3.13` and `pytoniq-core`.

## Success criteria

- Each new test has a short comment explaining what it covers.
- Each scenario has an explicit verify function instead of relying only on timeout-free completion.
- Existing unconditional invariants continue to run for every new test.
- New tests run without patching production consensus code.

## Status

- Implemented:
  - adversarial leader reboot coverage in `leader-restart-mid-window-still-rejoins-after-honest-finalization`
  - adversarial standstill-replayer reboot coverage in `standstill-rebroadcast-replayer-restarts-still-recovers`
  - integration single-node reboot coverage in `test-reboots`
  - integration repeated single-node reboot coverage in `test-reboots`
  - integration full-network clean cold-restart coverage in `test-reboots`
  - real process-level reboot liveness coverage in `test/integration/test_consensus_reboots.py`
  - manual all-node restart + state-loss regression coverage in `test-reboots --test-case full-network-restart-with-state-loss-still-recovers`
- Wiring note:
  - reboot integration cases live in the dedicated `test-reboots` binary; duplicating them under `test-adversarial` caused a flaky shutdown hang and was removed.
- Verified:
  - `ctest --output-on-failure -R '^reboot-'`
  - `test-consensus-adversarial --test-case leader-restart-mid-window-still-rejoins-after-honest-finalization`
  - `test-consensus-adversarial --test-case standstill-rebroadcast-replayer-restarts-still-recovers`
  - `python3 -m py_compile test/integration/test_consensus_reboots.py test/tontester/src/tontester/network.py`
- Current CTest state:
  - `reboot-single-validator-restart-rejoins-consensus`: green
  - `reboot-repeated-validator-restarts-rejoin-after-second-downtime`: green
  - `reboot-full-network-clean-restart-still-finalizes`: red, duplicate local-vote persistence after restart
  - `reboot-full-network-restart-with-state-loss-still-recovers`: red, recovery abort before steady state
- Execution note:
  - `test_consensus_reboots.py` is implemented but could not be executed in this workspace because `test/tontester`
    currently requires `Python >= 3.13` and `pytoniq-core`, while this machine still lacks that runtime.
