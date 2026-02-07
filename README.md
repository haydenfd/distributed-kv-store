# Distributed Key-Value Store

A sharded, replicated, AP-oriented key-value store built with gRPC and Protobuf.

## Features

- AP-oriented (eventual consistency)
- gRPC-based networking
- Consistent hashing for sharding
- Modern C++20

## Building

```bash
mkdir build
cd build
cmake ..
make
```

## Requirements

- CMake 3.20+
- C++20 compiler
- Protobuf
- gRPC
- GoogleTest (optional, for tests)

## Project Structure

```
distributed-kv/
├── proto/          # gRPC service definitions
├── src/            # Core library implementation
├── include/        # Public headers
├── tests/          # Unit tests
└── benchmarks/     # Performance benchmarks
```


## Sources

- gRPC/protobuf support on cmake: https://www.f-ax.de/dev/2020/11/08/grpc-plugin-cmake-support.html
- Architecture inspiration: https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf (DynamoDB Research Paper)
