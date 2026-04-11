add_executable(test-large-account-cache test-large-account-cache.cpp)

target_link_libraries(test-large-account-cache PRIVATE
    tdutils
    tdactor
    tl_api
    tl_lite_api
    ton_db
    ton_validator
    validator
    td-test-main
)

ton_test(test-large-account-cache)

add_subdirectory(consensus)
