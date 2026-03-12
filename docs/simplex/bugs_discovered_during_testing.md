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
