#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "cluster/cluster_view.h"
#include "node/node.h"
#include "node/node_config.h"

using kv::cluster::ClusterView;
using kv::node::Node;
using kv::node::StoreEntry;
using kv::node::Version;
using kv::NodeConfig;

namespace {
struct NodeFixture {
    ClusterView cluster;
    NodeConfig config;
    Node node;

    explicit NodeFixture(size_t replication_factor = 1, int write_quorum = 1)
        : cluster(10),
          config(make_config(replication_factor, write_quorum)),
          node(config, cluster) {
        cluster.add_node_to_cluster(config.node_id, "localhost:5000");
    }

    static NodeConfig make_config(size_t replication_factor, int write_quorum) {
        NodeConfig cfg;
        cfg.node_id = "nodeA";
        cfg.port = 5000;
        cfg.replication_factor = replication_factor;
        cfg.write_quorum = write_quorum;
        return cfg;
    }
};
}  // namespace

TEST(Node, PutGetSingleNode) {
    NodeFixture fixture(1, 1);

    EXPECT_TRUE(fixture.node.put("k1", "v1"));

    auto entry = fixture.node.get("k1");
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->value, "v1");
    EXPECT_EQ(entry->version.writer_id, "nodeA");
    EXPECT_GT(entry->version.write_created_at_us, 0u);
}

TEST(Node, WriteQuorumGreaterThanReplicasFails) {
    NodeFixture fixture(1, 2);

    EXPECT_FALSE(fixture.node.put("k2", "v2"));
}

TEST(Node, LocalGetMissingReturnsNullopt) {
    NodeFixture fixture(1, 1);

    auto entry = fixture.node.local_get("missing");
    EXPECT_FALSE(entry.has_value());
}

TEST(Node, ApplyPutLocalUsesLastWriteWins) {
    NodeFixture fixture(1, 1);

    Version older{100, "writerA"};
    Version newer{200, "writerB"};

    EXPECT_TRUE(fixture.node.apply_put_local("k3", "old", older));
    EXPECT_TRUE(fixture.node.apply_put_local("k3", "new", newer));

    auto entry = fixture.node.local_get("k3");
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->value, "new");
    EXPECT_EQ(entry->version.write_created_at_us, 200u);
    EXPECT_EQ(entry->version.writer_id, "writerB");
}

TEST(Node, ApplyPutLocalTieBreaksByWriterId) {
    NodeFixture fixture(1, 1);

    Version first{100, "A"};
    Version second{100, "Z"};

    EXPECT_TRUE(fixture.node.apply_put_local("k4", "v_a", first));
    EXPECT_TRUE(fixture.node.apply_put_local("k4", "v_z", second));

    auto entry = fixture.node.local_get("k4");
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->value, "v_z");
    EXPECT_EQ(entry->version.writer_id, "Z");
}

// A stale write arriving after a newer one must not overwrite the existing entry.
TEST(Node, ApplyPutLocalRejectsStaleWrite) {
    NodeFixture fixture(1, 1);

    Version newer{200, "writerA"};
    Version older{100, "writerB"};

    EXPECT_TRUE(fixture.node.apply_put_local("k5", "new_value", newer));
    EXPECT_TRUE(fixture.node.apply_put_local("k5", "stale_value", older));

    auto entry = fixture.node.local_get("k5");
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->value, "new_value");
    EXPECT_EQ(entry->version.write_created_at_us, 200u);
}

// Applying the exact same version twice must not overwrite (irreflexivity of is_newer).
TEST(Node, ApplyPutLocalSameVersionIsIdempotent) {
    NodeFixture fixture(1, 1);

    Version v{100, "writerA"};

    EXPECT_TRUE(fixture.node.apply_put_local("k6", "first", v));
    EXPECT_TRUE(fixture.node.apply_put_local("k6", "second", v));

    auto entry = fixture.node.local_get("k6");
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->value, "first");
}

// If A beats B and B beats C, A must beat C (transitivity of is_newer).
TEST(Node, IsNewerIsTransitive) {
    NodeFixture fixture(1, 1);

    Version a{300, "x"};
    Version b{200, "x"};
    Version c{100, "x"};

    // Apply c first, then b (b wins), then a (a wins).
    fixture.node.apply_put_local("k7", "c", c);
    fixture.node.apply_put_local("k7", "b", b);
    fixture.node.apply_put_local("k7", "a", a);

    auto entry = fixture.node.local_get("k7");
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->value, "a");
    EXPECT_EQ(entry->version.write_created_at_us, 300u);
}

// Each successive external put must produce a version >= the previous one.
TEST(Node, VersionMonotonicityAcrossSequentialPuts) {
    NodeFixture fixture(1, 1);

    EXPECT_TRUE(fixture.node.put("mono", "v1"));
    auto e1 = fixture.node.local_get("mono");
    ASSERT_TRUE(e1.has_value());
    uint64_t ts1 = e1->version.write_created_at_us;

    EXPECT_TRUE(fixture.node.put("mono", "v2"));
    auto e2 = fixture.node.local_get("mono");
    ASSERT_TRUE(e2.has_value());
    uint64_t ts2 = e2->version.write_created_at_us;

    EXPECT_GE(ts2, ts1);
    EXPECT_EQ(e2->value, "v2");
}

// write_count increments exactly once per external put.
TEST(Node, MetricsWriteCountIncrementsOnPut) {
    NodeFixture fixture(1, 1);

    EXPECT_EQ(fixture.node.metrics().writes, 0u);
    fixture.node.put("m1", "v1");
    EXPECT_EQ(fixture.node.metrics().writes, 1u);
    fixture.node.put("m1", "v2");
    EXPECT_EQ(fixture.node.metrics().writes, 2u);
}

// read_count increments exactly once per external get.
TEST(Node, MetricsReadCountIncrementsOnGet) {
    NodeFixture fixture(1, 1);

    fixture.node.put("m2", "v1");
    EXPECT_EQ(fixture.node.metrics().reads, 0u);
    fixture.node.get("m2");
    EXPECT_EQ(fixture.node.metrics().reads, 1u);
    fixture.node.get("m2");
    EXPECT_EQ(fixture.node.metrics().reads, 2u);
}

// put() returns false when the node is not registered in any cluster view
// (replica set is empty). write_count_ is still incremented because it fires
// before the empty check — document that here explicitly.
TEST(Node, PutOnEmptyClusterReturnsFalse) {
    ClusterView empty_cluster(10);
    NodeConfig cfg;
    cfg.node_id = "nodeA";
    cfg.port = 5000;
    cfg.replication_factor = 1;
    cfg.write_quorum = 1;
    Node node(cfg, empty_cluster);

    EXPECT_FALSE(node.put("key", "value"));
    EXPECT_EQ(node.metrics().writes, 1u);  // incremented before the empty check
}

// get() on an empty replica set returns nullopt. read_count_ still increments.
TEST(Node, GetOnEmptyClusterReturnsNullopt) {
    ClusterView empty_cluster(10);
    NodeConfig cfg;
    cfg.node_id = "nodeA";
    cfg.port = 5000;
    cfg.replication_factor = 1;
    cfg.write_quorum = 1;
    Node node(cfg, empty_cluster);

    EXPECT_FALSE(node.get("key").has_value());
    EXPECT_EQ(node.metrics().reads, 1u);
}

// forward_put to a node_id not registered in the cluster returns false
// and increments forward_failure_count_.
TEST(Node, ForwardPutUnknownNodeIncrementsForwardFailureCount) {
    NodeFixture fixture(1, 1);

    Version v{100, "nodeA"};
    bool ok = fixture.node.forward_put("ghost_node", "key", "value", v);

    EXPECT_FALSE(ok);
    EXPECT_EQ(fixture.node.metrics().forward_failures, 1u);
}

// forward_get to a node_id not registered in the cluster returns nullopt
// and increments forward_failure_count_.
TEST(Node, ForwardGetUnknownNodeIncrementsForwardFailureCount) {
    NodeFixture fixture(1, 1);

    auto result = fixture.node.forward_get("ghost_node", "key");

    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(fixture.node.metrics().forward_failures, 1u);
}

// Spawn N threads each writing the same key with a distinct timestamp.
// After all threads join, the entry with the highest timestamp must have won.
// Run with -DENABLE_TSAN=ON to catch data races inside apply_put_local.
TEST(Node, ConcurrentApplyPutLocalLWWWins) {
    NodeFixture fixture(1, 1);

    constexpr int kNumThreads = 8;
    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);

    // Thread i writes timestamp (i+1)*100. Thread kNumThreads-1 holds the max.
    for (int i = 0; i < kNumThreads; ++i) {
        threads.emplace_back([&, i]() {
            Version v{static_cast<uint64_t>((i + 1) * 100),
                      "writer_" + std::to_string(i)};
            fixture.node.apply_put_local(
                "contested", "value_" + std::to_string(i), v);
        });
    }
    for (auto& t : threads) t.join();

    auto entry = fixture.node.local_get("contested");
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->version.write_created_at_us,
              static_cast<uint64_t>(kNumThreads * 100));
    EXPECT_EQ(entry->value, "value_" + std::to_string(kNumThreads - 1));
}
