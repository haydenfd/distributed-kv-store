// Adapted from MurmurHash3 by Austin Appleby.
// Original implementation placed in the public domain.
// Source: https://github.com/aappleby/smhasher
//
// Modifications:
// - Trimmed to x64 variant only
// - Wrapped in C++ namespace
// - Exposed 64-bit hash output only

#pragma once

#include <cstdint>
#include <cstddef>
#include <string_view>

namespace kv::hash {
    // Deterministic 64-bit hash for arbitrary bytes.
    // Used for consistent hashing and partitioning.
    uint64_t murmur3_64(const void* data, size_t len, uint64_t seed);

    // Convenience overload
    uint64_t murmur3_64(std::string_view key, uint64_t seed);
}
