#include <gtest/gtest.h>

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
