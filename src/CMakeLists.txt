target_sources(kv_core
    PRIVATE
        hash/murmur3.cc
        ring/consistent_hash_ring.cc
)

target_include_directories(kv_core
    PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}/generated
)