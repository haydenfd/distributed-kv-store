cmake_minimum_required(VERSION 3.20)
project(distributed-kv VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Place all executables in a single bin directory for consistent paths.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Enable comprehensive warnings
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    add_compile_options(
        -Wall
        -Wextra
        -Wpedantic
        -Wshadow
        -Wconversion
        -Wsign-conversion
        -Wunused
        -Wno-unused-parameter
    )
endif()

# ThreadSanitizer — must instrument all TUs in the binary, so apply globally.
# Usage: cmake -DENABLE_TSAN=ON ..
# Incompatible with ASan/UBSan; use a separate build directory.
option(ENABLE_TSAN "Build with ThreadSanitizer instrumentation" OFF)
if(ENABLE_TSAN)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        message(FATAL_ERROR "ENABLE_TSAN requires GCC or Clang")
    endif()
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
    add_link_options(-fsanitize=thread)
endif()

if(MSVC)
    add_compile_options(/W4)
endif()

find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(yaml-cpp REQUIRED)

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
    VERBATIM
)

add_library(kv_core STATIC
    "${GENERATED_PROTOBUF_PATH}/kv.pb.cc"
    "${GENERATED_PROTOBUF_PATH}/kv.grpc.pb.cc"
)

target_include_directories(kv_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${GENERATED_PROTOBUF_PATH}
)

target_link_libraries(kv_core
    PUBLIC
        gRPC::grpc++
        protobuf::libprotobuf
        yaml-cpp::yaml-cpp
)

add_subdirectory(src)
add_subdirectory(benchmarks)
add_subdirectory(tests)
