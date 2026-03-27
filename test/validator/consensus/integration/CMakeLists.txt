add_library(adversarial-test-harness STATIC
    trace.cpp
    network.cpp
    actors.cpp
    invariants.cpp
    harness.cpp
)
target_link_libraries(adversarial-test-harness PUBLIC
    adnl
    adnltest
    dht
    overlay
    rldp
    tdactor
    tdutils
    tl_api
    ton_validator
    validator
)

add_executable(test-adversarial
    test_smoke.cpp
    test_silent_nodes.cpp
)
target_link_libraries(test-adversarial PRIVATE adversarial-test-harness)

add_executable(test-reboots
    test_reboots.cpp
)
target_link_libraries(test-reboots PRIVATE adversarial-test-harness)

# Register each test case as a CTest entry.
foreach(CASE
    smoke
    silent-validator-does-not-stop-progress
    consecutive-silent-leaders-then-progress
    leader-candidate-to-too-little-weight-skips-then-progress
    empty-candidates-with-silent-validator-still-finalize
)
    add_test(NAME adversarial-${CASE} COMMAND test-adversarial --test-case ${CASE})
endforeach()

foreach(CASE
    single-validator-restart-rejoins-consensus
    repeated-validator-restarts-rejoin-after-second-downtime
    full-network-clean-restart-still-finalizes
)
    add_test(NAME reboot-${CASE} COMMAND test-reboots --test-case ${CASE})
endforeach()
