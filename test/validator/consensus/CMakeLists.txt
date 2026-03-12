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

add_executable(test-block-producer test-block-producer.cpp)
target_link_libraries(test-block-producer PRIVATE
    adnl
    overlay
    tdactor
    tdutils
    tl_api
    ton_validator
    validator
    td-test-main
)

add_executable(test-candidate-resolver test-candidate-resolver.cpp)
target_link_libraries(test-candidate-resolver PRIVATE
    adnl
    overlay
    tdactor
    tdutils
    tl_api
    ton_validator
    validator
    td-test-main
)

add_executable(test-pool test-pool.cpp)
target_link_libraries(test-pool PRIVATE
    adnl
    overlay
    tdactor
    tdutils
    tl_api
    ton_validator
    validator
    td-test-main
)

add_executable(test-simplex-db test-simplex-db.cpp)
target_link_libraries(test-simplex-db PRIVATE
    adnl
    overlay
    tdactor
    tdutils
    tl_api
    ton_validator
    validator
    td-test-main
)

add_executable(test-simplex-consensus test-simplex-consensus.cpp)
target_link_libraries(test-simplex-consensus PRIVATE
    adnl
    overlay
    tdactor
    tdutils
    tl_api
    ton_validator
    validator
    td-test-main
)
