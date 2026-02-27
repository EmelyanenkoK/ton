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

add_executable(test-block-validator test-block-validator.cpp)
target_link_libraries(test-block-validator PRIVATE
    adnl
    overlay
    tdactor
    tdutils
    tl_api
    ton_validator
    validator
    td-test-main
)
