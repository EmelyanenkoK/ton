add_executable(test-consensus test-consensus.cpp)
target_link_libraries(test-consensus PRIVATE
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

add_executable(test-consensus-adversarial test-consensus-adversarial.cpp)
target_link_libraries(test-consensus-adversarial PRIVATE
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

foreach(TEST_NAME
    test-block-validator
    test-block-producer
    test-candidate-resolver
    test-pool
    test-simplex-db
    test-simplex-consensus
    test-simplex-parser
)
    add_executable(${TEST_NAME} ${TEST_NAME}.cpp)
    target_link_libraries(${TEST_NAME} PRIVATE
        adnl
        overlay
        tdactor
        tdutils
        tl_api
        ton_validator
        validator
        td-test-main
    )
    ton_test(${TEST_NAME})
endforeach()
