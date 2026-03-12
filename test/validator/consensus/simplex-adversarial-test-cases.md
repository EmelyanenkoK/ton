# Simplex Adversarial Test Cases

This document is the committed case list for
`test/validator/consensus/test-consensus-adversarial.cpp`.
It replaces the older working plan and keeps the A/B/C/D case IDs that are cited in test comments.

The suite models Byzantine behavior with total adversarial weight strictly below `W / 3`.
Every run is expected to preserve the local honest vote-discipline and global safety properties
described in `simplex_docs.md`.

## Always-On Invariants

These invariants are checked in every adversarial run unless a test explicitly focuses on a known
regression that intentionally violates current implementation behavior.

1. No honest validator casts `Notar(s, h1)` and `Notar(s, h2)` for `h1 != h2`.
2. No honest validator casts both `Skip(s)` and `Final(s, h)`.
3. At most one notarization certificate is formed for any slot.
4. At most one finalization certificate is formed for any slot.
5. `Final(s, h)` and `Skip(s)` are never both reached.
6. Honest finalized output remains prefix-consistent.
7. A finalized slot never disappears from later honest output.
8. Frontier clearing is monotone.

## A. Safety Cases

### A1-A2. Single-slot leader equivocation

Condition:
the leader sends two valid candidates for one slot to different honest subsets and tries to push
both through notarization/finalization.

Checks:
no two notarization certificates, no two finalization certificates, no honest double-`Notar`.

Implemented by:
`equivocating-leader-does-not-create-two-notars-or-finals`

### A3-A4. Final-vs-skip race on one slot

Condition:
one honest path gets close to finalizing while another path reaches skip timeout.

Checks:
no honest validator sends both votes for one slot, and the execution does not reach both
`Final(s, h)` and `Skip(s)`.

Implemented by:
`final-and-skip-race-preserves-local-discipline`

### A5. Invalid leader signature / wrong signed contents

Condition:
a candidate-resolution responder returns a candidate with the right id but a corrupted signature.

Checks:
the candidate is not resolved and later honest data is still required.

Implemented by:
`invalid-candidate-signature-does-not-resolve`

### A6. Invalid payload / invalid chain state

Condition:
the leader proposes a candidate whose chain state fails validation.

Checks:
honest validators do not cast `Notar` for that candidate and the attacked slot clears by `Skip`.

Implemented by:
`invalid-full-candidate-does-not-notarize`

### A7. Candidate skips uncleared slots without proof

Condition:
a later-slot candidate arrives before the intermediate skipped slot is proven.

Checks:
honest validators wait for the missing gap-skip proof before voting `Notar`.

Implemented by:
`later-slot-waits-for-gap-skip-proof`

### A8. Candidate built on a non-notarized parent

Condition:
a later-slot candidate is delivered before the parent notarization proof is observed.

Checks:
honest validators may store the candidate but do not vote `Notar` before the parent proof arrives.

Implemented by:
`later-slot-waits-for-earlier-notar-proof`

### A9. Malformed certificate accounting

Condition:
a candidate-resolution responder returns a candidate with a malformed notar certificate.

Checks:
the malformed response is ignored and later honest responses are still needed for resolution.

Implemented by:
`malformed-notar-certificate-does-not-resolve`

## B. Fork Cases

### B1. Slot both notarized and skipped, but not finalized

Condition:
a slot becomes notarized on one side while honest validators also reach `Skip`.

Checks:
`Notar(s, h)` and `Skip(s)` may coexist, but no `Final(s, h)` is formed for that slot.

Implemented by:
`notarized-and-skipped-slot-coexist-without-finalization`

### B2. Later descendants on both sides of a non-finalized fork

Condition:
same-key leader instances extend different branches through one whole leader window.

Checks:
temporary divergence is allowed, but no conflicting notar/final certificates appear.

Implemented by:
`multi-slot-equivocation-window-preserves-safety`

### B3. Fork collapse after later finalization

Condition:
a later honest finalization chooses one side of a non-finalized fork.

Checks:
honest finalized output collapses to a common prefix on the finalized branch.

Implemented by:
`fork-descendants-collapse-after-later-finalization`

### B4. Delayed observation of already-safe finalizations

Condition:
one honest node is isolated and observes the finalized suffix later than another.

Checks:
honest finalized outputs remain prefix-related and never rewind.

Implemented by:
`delayed-finalization-observation-preserves-prefix`

## C. Liveness and Recovery Cases

### C1. Silent malicious leader

Implemented by:
`silent-leader-window-skips-but-next-honest-window-finalizes`

### C2. Leader reveals candidate to too little weight

Implemented by:
`leader-candidate-to-too-little-weight-skips-then-progress`

### C3. Silent malicious non-leader

Implemented by:
`silent-validator-does-not-stop-progress`

### C4. Sleeping validator with later recovery

Implemented by:
`sleeping-validator-recovers-after-honest-progress`

### C5. Honest standstill rebroadcast recovers hidden data

Implemented by:
`standstill-with-byzantine-withholding-still-recovers`

### C6. No notarization observed, so skip wins

Implemented by:
`no-notar-observed-skip-wins`

### C7. Bad candidate-resolution responder

Implemented by:
`bad-candidate-resolution-responder-does-not-stop-catchup`

### C8. Missing ancestor chain during recovery

Implemented by:
`missing-ancestor-chain-still-resolves`

### C9. Byzantine withholding during standstill

Implemented by:
`standstill-with-byzantine-withholding-still-recovers`

### C10-C11. Long stall, then honest recovery

Condition:
many consecutive candidate-starved slots clear by `Skip`, then a later honest slot finalizes.

Implemented by:
`long-stall-clears-skipped-slots-then-honest-leader-finalizes`

### C12. Multiple consecutive malicious leaders, then progress

Implemented by:
`consecutive-silent-leaders-then-progress`

### C13. Replay / stale-message adversary

Implemented by:
`replay-stale-certificate-does-not-disturb-progress`

### C14. Duplicate-instance same-key split brain

Implemented by:
`duplicate-instance-same-key-split-brain-still-progresses`

## D. TON-Specific Implementation / Regression Cases

### D1. Multi-slot equivocation inside one leader window

Implemented by:
`multi-slot-equivocation-window-preserves-safety`

### D2-D3. Later slot arrives before earlier proof under eager whole-window production

Implemented by:
`later-slot-waits-for-earlier-notar-proof`

### D4. Rule 6 timeout-reset regression

Status:
known red regression until Rule 6 is fixed.

Implemented by:
`skip-timeout-uses-last-finalization`

### D5. Skip-timeout cap plateau

Condition:
repeated stalled windows drive skip timeout growth until the configured cap is reached.

Checks:
delay growth plateaus at the cap and later honest finalization still resumes.

Implemented by:
`skip-timeout-cap-plateaus-under-stall`

### D6. Count-threshold vs weight-threshold dissemination mismatch

Condition:
the attacked slot reaches a count-majority of validators, but not quorum weight.

Checks:
no notar/final certificate forms for that slot, safety holds, and later progress resumes.

Implemented by:
`count-threshold-mismatch-preserves-safety`

### D7. Standstill rebroadcast flood / death spiral

Condition:
prolonged standstill accumulates rebroadcast pressure before honest quorum connectivity is restored.

Checks:
no crash, no safety break, and later honest finalization resumes after reconnect.

Implemented by:
`standstill-flood-still-recovers`

### D8. Empty candidates under Byzantine scheduling

Implemented by:
`empty-candidates-with-silent-validator-still-finalize`

## Core Regression Subset

These are the strongest small-set adversarial cases to run first:

1. `equivocating-leader-does-not-create-two-notars-or-finals`
2. `final-and-skip-race-preserves-local-discipline`
3. `fork-descendants-collapse-after-later-finalization`
4. `long-stall-clears-skipped-slots-then-honest-leader-finalizes`
5. `bad-candidate-resolution-responder-does-not-stop-catchup`
6. `skip-timeout-uses-last-finalization`
