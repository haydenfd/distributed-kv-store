find_package(GTest REQUIRED)

add_executable(kv_tests
    test_murmur3.cc
    test_consistent_hash_ring.cc
    test_cluster_view.cc
    test_node_rpc_service.cc
    test_node.cc
)

target_link_libraries(kv_tests
    PRIVATE
        kv_core
        GTest::gtest
        GTest::gtest_main
)


add_test(NAME kv_tests COMMAND kv_tests)
