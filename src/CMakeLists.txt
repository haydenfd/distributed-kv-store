target_sources(kv_core
    PRIVATE
        hash/murmur3.cc
        ring/consistent_hash_ring.cc
        utils/logging.cc
        node/node_rpc_service.cc
        node/node.cc
        cluster/cluster_view.cc
)

target_include_directories(kv_core
    PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}/generated
)

add_executable(kv_node
    node/main.cc
)

target_link_libraries(kv_node
    PRIVATE
        kv_core
)

add_executable(kv_cli
    client/kv_cli.cc
)

target_link_libraries(kv_cli
    PRIVATE
        kv_core
        gRPC::grpc++
        protobuf::libprotobuf
)

target_include_directories(kv_cli
    PRIVATE
        ${CMAKE_CURRENT_BINARY_DIR}/generated
)
