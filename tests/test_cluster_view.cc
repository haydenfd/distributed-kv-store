#include <gtest/gtest.h>

#include <algorithm>
#include <unordered_set>

#include "kv/cluster/cluster_view.h"

using kv::cluster::ClusterView;

TEST(ClusterView, EmptyClusterBehavesGracefully) {
    ClusterView view;

    EXPECT_FALSE(view.get_node_address("missing").has_value());

    auto replicas = view.get_replica_set_for_key("key", 3);
    EXPECT_TRUE(replicas.empty());

    auto channel = view.create_grpc_channel_for_node("missing");
    EXPECT_EQ(channel, nullptr);
}

TEST(ClusterView, AddAndLookupNode) {
    ClusterView view;

    view.add_node_to_cluster("nodeA", "localhost:5000");

    auto addr = view.get_node_address("nodeA");
    ASSERT_TRUE(addr.has_value());
    EXPECT_EQ(*addr, "localhost:5000");

    auto ids = view.get_node_ids();
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(ids[0], "nodeA");
}

TEST(ClusterView, DuplicateAddDoesNotOverwrite) {
    ClusterView view;

    view.add_node_to_cluster("nodeA", "localhost:5000");
    view.add_node_to_cluster("nodeA", "localhost:6000");

    auto addr = view.get_node_address("nodeA");
    ASSERT_TRUE(addr.has_value());
    EXPECT_EQ(*addr, "localhost:5000");
}

TEST(ClusterView, RemoveNodeClearsMembershipAndPlacement) {
    ClusterView view;

    view.add_node_to_cluster("nodeA", "localhost:5000");
    view.add_node_to_cluster("nodeB", "localhost:5001");

    view.remove_node_from_cluster("nodeA");

    EXPECT_FALSE(view.get_node_address("nodeA").has_value());

    auto replicas = view.get_replica_set_for_key("key", 2);
    EXPECT_EQ(replicas.size(), 1u);
    EXPECT_EQ(replicas[0], "nodeB");
}

TEST(ClusterView, ReplicaSetIsUniqueAndBoundedByClusterSize) {
    ClusterView view(10);

    view.add_node_to_cluster("A", "localhost:5000");
    view.add_node_to_cluster("B", "localhost:5001");
    view.add_node_to_cluster("C", "localhost:5002");

    auto replicas = view.get_replica_set_for_key("key", 10);

    std::unordered_set<std::string> uniq(replicas.begin(), replicas.end());
    EXPECT_EQ(uniq.size(), replicas.size());
    EXPECT_EQ(replicas.size(), 3u);

    EXPECT_TRUE(uniq.count("A"));
    EXPECT_TRUE(uniq.count("B"));
    EXPECT_TRUE(uniq.count("C"));
}

TEST(ClusterView, ReplicaSetIsDeterministicForSameKey) {
    ClusterView view(20);

    view.add_node_to_cluster("A", "localhost:5000");
    view.add_node_to_cluster("B", "localhost:5001");
    view.add_node_to_cluster("C", "localhost:5002");

    auto first = view.get_replica_set_for_key("key", 2);
    auto second = view.get_replica_set_for_key("key", 2);

    EXPECT_EQ(first, second);
}

TEST(ClusterView, ReplicationFactorZeroReturnsEmpty) {
    ClusterView view;
    view.add_node_to_cluster("A", "localhost:5000");

    auto replicas = view.get_replica_set_for_key("key", 0);
    EXPECT_TRUE(replicas.empty());
}
