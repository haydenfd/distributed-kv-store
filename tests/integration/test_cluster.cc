#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "cluster/cluster_view.h"
#include "node/node.h"
#include "node/node_config.h"
#include "node/node_rpc_service.h"

using kv::NodeConfig;
using kv::NodeRpcService;
using kv::cluster::ClusterView;
using kv::node::Node;
using kv::node::Version;

// ---------------------------------------------------------------------------
// ClusterFixture
//
// Spins up N real gRPC servers in-process on ephemeral ports. All nodes share
// one ClusterView so routing and forwarding work exactly as in production.
//
// kill(i)   — shuts down node i's server but keeps it in the ClusterView,
//             simulating a crash (RPCs to it will fail with UNAVAILABLE).
// ---------------------------------------------------------------------------
namespace {

struct ClusterFixture {
    ClusterView view{100};

    struct Instance {
        std::string id;
        std::unique_ptr<Node> node;
        std::unique_ptr<NodeRpcService> service;
        std::unique_ptr<grpc::Server> server;
        int port{0};
        bool alive{true};
    };

    std::vector<std::unique_ptr<Instance>> instances;
    size_t rf_;
    int wq_;

    explicit ClusterFixture(size_t rf = 3, int wq = 1) : rf_(rf), wq_(wq) {}

    ~ClusterFixture() {
        auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(300);
        for (auto& inst : instances) {
            if (inst->server) inst->server->Shutdown(deadline);
        }
    }

    // Add a node, start its gRPC server, register it in the shared view.
    void add_node(const std::string& id) {
        auto inst = std::make_unique<Instance>();
        inst->id = id;

        NodeConfig cfg;
        cfg.node_id = id;
        cfg.port = 1;  // placeholder — not used for binding
        cfg.replication_factor = rf_;
        cfg.write_quorum = wq_;

        inst->node = std::make_unique<Node>(cfg, view);
        inst->service = std::make_unique<NodeRpcService>(*inst->node);

        grpc::ServerBuilder builder;
        builder.AddListeningPort(
            "localhost:0",
            grpc::InsecureServerCredentials(),
            &inst->port
        );
        builder.RegisterService(inst->service.get());
        inst->server = builder.BuildAndStart();

        view.add_node_to_cluster(id, "localhost:" + std::to_string(inst->port));
        instances.push_back(std::move(inst));
    }

    // Start N nodes and wait for them to be ready.
    void start(size_t count) {
        for (size_t i = 0; i < count; ++i) {
            add_node("n" + std::to_string(i + 1));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    Node& node(size_t i) { return *instances[i]->node; }

    // Crash node i: server stops, but it stays in the ClusterView so routing
    // still targets it and forwarding RPCs will fail with UNAVAILABLE.
    void kill(size_t i) {
        instances[i]->server->Shutdown(
            std::chrono::system_clock::now() + std::chrono::milliseconds(100)
        );
        instances[i]->server.reset();
        instances[i]->alive = false;
    }
};

}  // namespace

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Coordinator fans out a PUT to all RF replicas. After early_write_return is
// disabled (so put() waits for all RPCs to complete), every node's local store
// must contain the written value.
TEST(ClusterIntegration, ForwardingReplicatesValueToAllNodes) {
    ClusterFixture f(3, 1);
    f.start(3);
    f.node(0).set_early_write_return(false);

    ASSERT_TRUE(f.node(0).put("key", "value"));

    for (size_t i = 0; i < 3; ++i) {
        auto entry = f.node(i).local_get("key");
        ASSERT_TRUE(entry.has_value()) << "n" << (i + 1) << " missing key";
        EXPECT_EQ(entry->value, "value") << "n" << (i + 1) << " has wrong value";
    }
}

// Inject a stale version directly into n3's store. The coordinator GET must
// return the newest value and repair n3 synchronously before returning.
TEST(ClusterIntegration, ReadRepairFixesStalReplica) {
    ClusterFixture f(3, 1);
    f.start(3);
    f.node(0).set_early_write_return(false);

    ASSERT_TRUE(f.node(0).put("foo", "fresh"));

    // Inject a stale entry directly — bypasses LWW on purpose.
    f.node(2).apply_put_local("foo", "stale", Version{1, "old"});
    {
        auto check = f.node(2).local_get("foo");
        ASSERT_TRUE(check.has_value());
        ASSERT_EQ(check->value, "stale");  // confirm injection worked
    }

    // Coordinator GET picks the newest across all replicas and repairs n3.
    auto result = f.node(0).get("foo");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->value, "fresh");

    // Read repair is synchronous inside get() — n3 must be patched by now.
    auto repaired = f.node(2).local_get("foo");
    ASSERT_TRUE(repaired.has_value());
    EXPECT_EQ(repaired->value, "fresh");
    EXPECT_GT(f.node(0).metrics().read_repairs, 0u);
}

// With W=2 and RF=3, a PUT to a 3-node cluster succeeds. After killing 2
// nodes only 1 replica is reachable — fewer than W — so PUT must fail.
TEST(ClusterIntegration, QuorumWriteFailsWhenReplicasBelowW) {
    ClusterFixture f(3, 2);
    f.start(3);

    ASSERT_TRUE(f.node(0).put("k", "v"));

    f.kill(1);
    f.kill(2);
    // Let the server ports actually close before issuing the next RPC.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    EXPECT_FALSE(f.node(0).put("k", "v2"));
}

// Kill 2 replicas, issue a PUT that still meets W=1 locally. The 2 failed
// forwards must increment forward_failure_count.
TEST(ClusterIntegration, ForwardFailureCountIncrementsOnDeadReplicas) {
    ClusterFixture f(3, 1);
    f.start(3);

    f.kill(1);
    f.kill(2);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Disable early write return so put() joins all worker threads and the
    // failure counters are guaranteed to be updated before we read them.
    f.node(0).set_early_write_return(false);
    EXPECT_TRUE(f.node(0).put("k", "v"));  // W=1, local ack is enough

    EXPECT_GE(f.node(0).metrics().forward_failures, 2u);
}

// Two sequential PUTs to the same key via the same coordinator. The second
// write has a strictly later timestamp and must win on every replica.
TEST(ClusterIntegration, LWWConvergesAllReplicasToLatestWrite) {
    ClusterFixture f(3, 1);
    f.start(3);
    f.node(0).set_early_write_return(false);

    ASSERT_TRUE(f.node(0).put("k", "first"));
    ASSERT_TRUE(f.node(0).put("k", "second"));

    for (size_t i = 0; i < 3; ++i) {
        auto entry = f.node(i).local_get("k");
        ASSERT_TRUE(entry.has_value()) << "n" << (i + 1) << " missing key";
        EXPECT_EQ(entry->value, "second") << "n" << (i + 1) << " did not converge";
    }
}

// Any node can coordinate a GET regardless of which nodes hold the key.
// With RF=2 and 3 nodes, the key is only on 2 of the 3 nodes. Whichever
// node coordinates must forward to the owners and return the correct value.
TEST(ClusterIntegration, AnyNodeCanCoordinateGet) {
    ClusterFixture f(2, 1);
    f.start(3);
    f.node(0).set_early_write_return(false);

    ASSERT_TRUE(f.node(0).put("k", "v"));

    // All three nodes coordinate a GET — each queries the 2-node preference
    // list, forwarding where needed.
    for (size_t i = 0; i < 3; ++i) {
        auto result = f.node(i).get("k");
        ASSERT_TRUE(result.has_value()) << "n" << (i + 1) << " GET returned nullopt";
        EXPECT_EQ(result->value, "v") << "n" << (i + 1) << " returned wrong value";
    }
}
