target_sources(kv_core
    PRIVATE
        hash/murmur3.cc
        ring/consistent_hash_ring.cc
        node/kv_service.cc
        node/node.cc
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

add_executable(kv_client
    client/kv_client.cc
)

target_link_libraries(kv_client
    PRIVATE
        kv_core
        gRPC::grpc++
        protobuf::libprotobuf
)

target_include_directories(kv_client
    PRIVATE
        ${CMAKE_CURRENT_BINARY_DIR}/generated
)