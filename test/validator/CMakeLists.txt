add_executable(test-validate-query test-validate-query.cpp)
target_link_libraries(test-validate-query PRIVATE
    adnl
    dht
    overlay
    tdactor
    tdutils
    tl_api
    ton_validator
    validator
    td-test-main
)
target_compile_options(test-validate-query PRIVATE -fno-access-control)

add_executable(test-mc-config test-mc-config.cpp)
target_link_libraries(test-mc-config PRIVATE
    adnl
    dht
    overlay
    tdactor
    tdutils
    tl_api
    ton_validator
    validator
    td-test-main
)
ton_test(test-mc-config)

add_subdirectory(consensus)
