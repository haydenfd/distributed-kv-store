cmake_minimum_required(VERSION 3.20)
project(distributed-kv VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Dependencies
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)

# -----------------------
# Protobuf / gRPC setup
# -----------------------
set(PROTO_PATH "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(GENERATED_PROTOBUF_PATH "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GENERATED_PROTOBUF_PATH})

set(KV_PROTO "${PROTO_PATH}/kv.proto")

add_custom_command(
    OUTPUT
        "${GENERATED_PROTOBUF_PATH}/kv.pb.cc"
        "${GENERATED_PROTOBUF_PATH}/kv.pb.h"
        "${GENERATED_PROTOBUF_PATH}/kv.grpc.pb.cc"
        "${GENERATED_PROTOBUF_PATH}/kv.grpc.pb.h"
    COMMAND $<TARGET_FILE:protobuf::protoc>
    ARGS
        --cpp_out "${GENERATED_PROTOBUF_PATH}"
        --grpc_out "${GENERATED_PROTOBUF_PATH}"
        -I "${PROTO_PATH}"
        --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
        "${KV_PROTO}"
    DEPENDS "${KV_PROTO}"
    COMMENT "Generating protobuf and gRPC sources"
    VERBATIM
)

# -----------------------
# Core library
# -----------------------
add_library(kv_core STATIC
    "${GENERATED_PROTOBUF_PATH}/kv.pb.cc"
    "${GENERATED_PROTOBUF_PATH}/kv.grpc.pb.cc"
)

target_include_directories(kv_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${GENERATED_PROTOBUF_PATH}
)

target_link_libraries(kv_core
    PUBLIC
        gRPC::grpc++
        protobuf::libprotobuf
)

enable_testing()
# -----------------------
# Subdirectories
# -----------------------
add_subdirectory(src)
add_subdirectory(tests)
# add_subdirectory(benchmarks)