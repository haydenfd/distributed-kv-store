#include <gtest/gtest.h>
#include "hash/murmur3.h"

#include <unordered_set>
#include <string>

using kv::hash::murmur3_64;

TEST(MurmurHash, DeterministicForSameInput) {
    auto h1 = murmur3_64("hello world", 42);
    auto h2 = murmur3_64("hello world", 42);
    EXPECT_EQ(h1, h2);
}

TEST(MurmurHash, DifferentSeedsProduceDifferentHashes) {
    auto h1 = murmur3_64("hello world", 1);
    auto h2 = murmur3_64("hello world", 2);
    EXPECT_NE(h1, h2);
}

TEST(MurmurHash, DifferentKeysProduceDifferentHashes) {
    auto h1 = murmur3_64("key1", 0);
    auto h2 = murmur3_64("key2", 0);
    EXPECT_NE(h1, h2);
}

TEST(MurmurHash, EmptyStringIsDeterministic) {
    auto h1 = murmur3_64("", 0);
    auto h2 = murmur3_64("", 0);
    EXPECT_EQ(h1, h2);
}

TEST(MurmurHash, EmptyStringSeedMatters) {
    auto h1 = murmur3_64("", 1);
    auto h2 = murmur3_64("", 2);
    EXPECT_NE(h1, h2);
}

TEST(MurmurHash, NoCollisionsForSmallSet) {
    std::unordered_set<uint64_t> hashes;
    for (int i = 0; i < 10'000; ++i) {
        hashes.insert(
            murmur3_64("key_" + std::to_string(i), 0)
        );
    }
    EXPECT_EQ(hashes.size(), 10'000);
}

TEST(MurmurHash, StableAcrossRuns) {
    uint64_t expected = murmur3_64("consistent-hash-key", 1234);
    uint64_t actual   = murmur3_64("consistent-hash-key", 1234);
    EXPECT_EQ(expected, actual);
}