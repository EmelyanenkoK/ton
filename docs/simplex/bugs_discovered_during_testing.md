# Bugs Discovered During Simplex Testing

## `simplex::Db` actor does not stop on `StopRequested`

- Documentation / expectation: `test-consensus` needs to start and stop all registered simplex actors cleanly between runs.
- Observed code: [validator/consensus/simplex/db.cpp](/home/rulon/ton/validator/consensus/simplex/db.cpp) does not handle `StopRequested`, unlike the other test-facing actors used in the harness.
- Trigger: once `test/validator/consensus/test-consensus.cpp` registers the production `simplex::Db` actor so that `LeaderWindowObserved`, `BroadcastVote`, and `SaveCertificate` requests are actually serviced, shutdown hangs waiting for that actor to terminate.
- Testing impact: the consensus harness must currently use a test-local replacement actor with the same persistence behavior plus explicit stop handling. This keeps the bug visible instead of masking it in production code.

## Restart after wiping simplex state can wedge `test-consensus` shutdown

- Documentation / expectation: Rule 2 candidate-resolution testing needs the harness to stop one node, wipe its local simplex state, restart it, and then shut the whole run down cleanly.
- Observed behavior: when a node is stopped, its simplex DB is cleared, and the node is started again inside `test/validator/consensus/test-consensus.cpp`, the final shutdown can hang while stopping that restarted node. The log shows repeated detached-task failures with `Actor destroyed` during teardown.
- Trigger: a restart/storage-loss scenario intended to exercise candidate resolution.
- Testing impact: this currently blocks a stable `test-consensus` scenario for Rule 2 recovery after restart and DB loss. The blocker needs to be understood before a non-hanging candidate-resolution integration test can be committed.

## Rule 2 recovery retries but does not complete after restart and local data loss

- Documentation / expectation: Rule 2 says that after losing a notarized candidate, a validator retries `requestCandidate` with exponential backoff and eventually recovers once a peer with the candidate body is reachable.
- Observed behavior: in the new `candidate-resolution-recovery` `test-consensus` scenario, validator `#0.0` repeatedly issues `requestCandidate` for the chosen notarized candidate and backs off its timeout, but still never resolves that candidate even after the only full-data peer is restarted.
- Trigger: stop validator `#0.0`, wipe its simplex DB and candidate storage, restart partial peers without candidate bodies, then restart the only peer that still has the candidate body.
- Testing impact: Rule 2 now has a deterministic red test. The failure is no longer “scenario not built”; it is that the documented recovery behavior does not complete under this restart/data-loss setup.

## Mid-run standstill scenarios can wedge orderly `test-consensus` teardown

- Documentation / expectation: Rule 8 standstill-resolution testing needs the harness to stop or isolate part of the validator set mid-run, wait for rebroadcasts, and then shut down normally.
- Observed behavior: after inducing a standstill by stopping or isolating validators inside `test/validator/consensus/test-consensus.cpp`, the subsequent global `finalize()` shutdown can hang while stopping the remaining live nodes. The logs again show repeated detached-task failures such as `Actor destroyed` during teardown.
- Trigger: a standstill-resolution scenario that removes quorum after at least one finalization has already happened.
- Testing impact: the Rule 8 test currently has to verify its trace and let the process exit immediately, instead of waiting for orderly actor teardown. The shutdown instability remains visible here and should be understood separately from the protocol assertion.
