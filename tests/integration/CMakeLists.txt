find_package(GTest REQUIRED)

add_executable(kv_integration_tests
    test_cluster.cc
)

target_link_libraries(kv_integration_tests
    PRIVATE
        kv_core
        GTest::gtest
        GTest::gtest_main
)

add_test(NAME kv_integration_tests COMMAND kv_integration_tests)
