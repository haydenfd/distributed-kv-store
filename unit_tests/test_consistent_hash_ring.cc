#include <gtest/gtest.h>
#include <unordered_map>
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

// After a node is removed, every key that was previously owned by it must
// still route to a valid remaining node — no throws, no orphans.
// remove_node on a node_id that was never added must be a no-op.
TEST(ConsistentHashRing, RemoveNonExistentNodeIsNoOp) {
    ConsistentHashRing ring(50);
    ring.add_node("A");
    size_t size_before = ring.size();

    ASSERT_NO_THROW(ring.remove_node("ghost"));
    EXPECT_EQ(ring.size(), size_before);
}

// Each add_node inserts exactly vnodes_ entries; each remove_node removes them.
TEST(ConsistentHashRing, SizeReflectsVnodesPerNode) {
    const size_t kVnodes = 50;
    ConsistentHashRing ring(kVnodes);

    EXPECT_EQ(ring.size(), 0u);

    ring.add_node("A");
    EXPECT_EQ(ring.size(), kVnodes);

    ring.add_node("B");
    EXPECT_EQ(ring.size(), kVnodes * 2);

    ring.remove_node("A");
    EXPECT_EQ(ring.size(), kVnodes);

    ring.remove_node("B");
    EXPECT_EQ(ring.size(), 0u);
}

// The first element of get_preference_list must always equal get_owner_node
// for the same key — they are defined by the same ring walk.
TEST(ConsistentHashRing, PreferenceListHeadMatchesOwnerNode) {
    ConsistentHashRing ring(100);
    ring.add_node("A");
    ring.add_node("B");
    ring.add_node("C");

    for (int i = 0; i < 200; ++i) {
        std::string key = "key_" + std::to_string(i);
        auto prefs = ring.get_preference_list(key, 3);
        ASSERT_FALSE(prefs.empty());
        EXPECT_EQ(prefs[0], ring.get_owner_node(key))
            << "preference list head != owner for key=" << key;
    }
}

TEST(ConsistentHashRing, NodeRemovalRoutesOrphansToRemainingNodes) {
    ConsistentHashRing ring(100);
    ring.add_node("A");
    ring.add_node("B");
    ring.add_node("C");

    const int kKeys = 1000;
    std::vector<std::string> owners_before;
    owners_before.reserve(kKeys);
    for (int i = 0; i < kKeys; ++i) {
        owners_before.push_back(ring.get_owner_node("key_" + std::to_string(i)));
    }

    ring.remove_node("B");

    for (int i = 0; i < kKeys; ++i) {
        std::string owner;
        ASSERT_NO_THROW(owner = ring.get_owner_node("key_" + std::to_string(i)))
            << "key_" << i << " has no owner after removing B";
        EXPECT_NE(owner, "B")
            << "key_" << i << " still routes to removed node B";
        EXPECT_TRUE(owner == "A" || owner == "C")
            << "key_" << i << " routes to unknown node: " << owner;
    }
}

TEST(ConsistentHashRing, KeysDistributeUniformlyAcrossNodes) {
    const int kNodes = 5;
    const int kKeys = 10000;
    const int kVnodes = 150;

    ConsistentHashRing ring(kVnodes);
    for (int i = 0; i < kNodes; ++i) {
        ring.add_node("node_" + std::to_string(i));
    }

    std::unordered_map<std::string, int> counts;
    for (int i = 0; i < kKeys; ++i) {
        counts[ring.get_owner_node("key_" + std::to_string(i))]++;
    }

    const double expected = static_cast<double>(kKeys) / kNodes;
    for (const auto& [node, count] : counts) {
        EXPECT_GT(count, expected * 0.5)
            << node << " is starved: " << count << " keys (expected ~" << expected << ")";
        EXPECT_LT(count, expected * 1.5)
            << node << " is hot: " << count << " keys (expected ~" << expected << ")";
    }
}
