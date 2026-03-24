add_executable(test-consensus test-consensus.cpp)
target_link_libraries(test-consensus overlay tdutils tdactor adnl adnltest rldp tl_api dht validator ton_validator validator)
# add_test(test-consensus test-consensus)

add_executable(benchmark-simplex-cert benchmark-simplex-cert.cpp)
target_link_libraries(benchmark-simplex-cert overlay tdutils tdactor adnl adnltest rldp tl_api dht validator ton_validator validator)
