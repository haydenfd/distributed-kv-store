#include <gtest/gtest.h>
#include "ring/consistent_hash_ring.h"

using kv::ring::ConsistentHashRing;

TEST(ConsistentHashRing, EmptyRingThrows) {
    ConsistentHashRing ring;
    EXPECT_THROW(ring.get_owner_node("key"), std::runtime_error);
}

TEST(ConsistentHashRing, SingleNodeAlwaysWins) {
    ConsistentHashRing ring(10);
    ring.add_node("nodeA");

    for (int i = 0; i < 1000; ++i) {
        EXPECT_EQ(ring.get_owner_node("key_" + std::to_string(i)), "nodeA");
    }
}

TEST(ConsistentHashRing, DeterministicMapping) {
    ConsistentHashRing ring(50);
    ring.add_node("nodeA");
    ring.add_node("nodeB");
    ring.add_node("nodeC");

    std::string owner1 = ring.get_owner_node("user:123");
    std::string owner2 = ring.get_owner_node("user:123");

    EXPECT_EQ(owner1, owner2);
}

TEST(ConsistentHashRing, MinimalDisruptionOnAdd) {
    ConsistentHashRing ring(100);
    ring.add_node("nodeA");
    ring.add_node("nodeB");

    std::vector<std::string> before;
    for (size_t i = 0; i < 1000; ++i) {
        before.push_back(ring.get_owner_node("key_" + std::to_string(i)));
    }

    ring.add_node("nodeC");

    int moved = 0;
    for (size_t i = 0; i < 1000; ++i) {
        if (before[i] != ring.get_owner_node("key_" + std::to_string(i))) {
            moved++;
        }
    }

    // Roughly 1/(N+1) keys should move
    EXPECT_LT(moved, 500);
}

TEST(ConsistentHashRing, PreferenceListIsOrderedAndUnique) {
    ConsistentHashRing ring(50);
    ring.add_node("A");
    ring.add_node("B");
    ring.add_node("C");

    auto prefs = ring.get_preference_list("key", 3);

    EXPECT_EQ(prefs.size(), 3);
    EXPECT_NE(prefs[0], prefs[1]);
    EXPECT_NE(prefs[1], prefs[2]);
    EXPECT_NE(prefs[0], prefs[2]);
}

TEST(ConsistentHashRing, PreferenceListWrapsCorrectly) {
    ConsistentHashRing ring(10);
    ring.add_node("A");
    ring.add_node("B");

    auto prefs = ring.get_preference_list("key", 10);
    EXPECT_EQ(prefs.size(), 2);
}
