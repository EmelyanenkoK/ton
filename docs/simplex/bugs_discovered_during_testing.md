# Bugs Discovered During Simplex Testing

## `simplex::Db` actor does not stop on `StopRequested`

- Documentation / expectation: `test-consensus` needs to start and stop all registered simplex actors cleanly between runs.
- Observed code: [validator/consensus/simplex/db.cpp](/home/rulon/ton/validator/consensus/simplex/db.cpp) does not handle `StopRequested`, unlike the other test-facing actors used in the harness.
- Trigger: once `test/validator/consensus/test-consensus.cpp` registers the production `simplex::Db` actor so that `LeaderWindowObserved`, `BroadcastVote`, and `SaveCertificate` requests are actually serviced, shutdown hangs waiting for that actor to terminate.
- Testing impact: the consensus harness must currently use a test-local replacement actor with the same persistence behavior plus explicit stop handling. This keeps the bug visible instead of masking it in production code.
