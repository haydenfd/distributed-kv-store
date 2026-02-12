#include <gtest/gtest.h>

#include <grpcpp/server_context.h>

#include "cluster/cluster_view.h"
#include "node/node.h"
#include "node/node_config.h"
#include "node/node_rpc_service.h"

using kv::cluster::ClusterView;
using kv::node::Node;
using kv::NodeConfig;
using kv::NodeRpcService;

namespace {
struct ServiceFixture {
    ClusterView cluster;
    Node node;
    NodeRpcService service;

    explicit ServiceFixture(const std::string& node_id = "nodeA",
                            const std::string& address = "localhost:5000")
        : cluster(10),
          node(make_node(node_id, address, cluster)),
          service(node) {}

    static Node make_node(const std::string& node_id,
                          const std::string& address,
                          ClusterView& cluster_view) {
        cluster_view.add_node_to_cluster(node_id, address);
        NodeConfig cfg;
        cfg.node_id = node_id;
        cfg.port = 5000;
        cfg.replication_factor = 1;
        cfg.write_quorum = 1;
        return Node(cfg, cluster_view);
    }
};
}  // namespace

TEST(NodeRpcService, InternalPutThenInternalGetReturnsValueAndVersion) {
    ServiceFixture fixture;

    kvstore::PutRequest put;
    kvstore::PutResponse put_resp;
    grpc::ServerContext put_ctx;

    put.set_key("k1");
    put.set_value("v1");
    put.set_is_internal(true);
    put.mutable_version()->set_write_created_at_us(123);
    put.mutable_version()->set_writer_id("writerA");

    auto put_status = fixture.service.Put(&put_ctx, &put, &put_resp);
    EXPECT_TRUE(put_status.ok());
    EXPECT_TRUE(put_resp.success());

    kvstore::GetRequest get;
    kvstore::GetResponse get_resp;
    grpc::ServerContext get_ctx;

    get.set_key("k1");
    get.set_is_internal(true);

    auto get_status = fixture.service.Get(&get_ctx, &get, &get_resp);
    EXPECT_TRUE(get_status.ok());
    EXPECT_TRUE(get_resp.found());
    EXPECT_EQ(get_resp.value(), "v1");
    EXPECT_EQ(get_resp.version().write_created_at_us(), 123u);
    EXPECT_EQ(get_resp.version().writer_id(), "writerA");
}

TEST(NodeRpcService, ExternalPutThenExternalGetReturnsValue) {
    ServiceFixture fixture;

    kvstore::PutRequest put;
    kvstore::PutResponse put_resp;
    grpc::ServerContext put_ctx;

    put.set_key("k2");
    put.set_value("v2");
    put.set_is_internal(false);

    auto put_status = fixture.service.Put(&put_ctx, &put, &put_resp);
    EXPECT_TRUE(put_status.ok());
    EXPECT_TRUE(put_resp.success());

    kvstore::GetRequest get;
    kvstore::GetResponse get_resp;
    grpc::ServerContext get_ctx;

    get.set_key("k2");
    get.set_is_internal(false);

    auto get_status = fixture.service.Get(&get_ctx, &get, &get_resp);
    EXPECT_TRUE(get_status.ok());
    EXPECT_TRUE(get_resp.found());
    EXPECT_EQ(get_resp.value(), "v2");
}

TEST(NodeRpcService, MissingKeyReturnsNotFound) {
    ServiceFixture fixture;

    kvstore::GetRequest get;
    kvstore::GetResponse get_resp;
    grpc::ServerContext get_ctx;

    get.set_key("missing");
    get.set_is_internal(true);

    auto status = fixture.service.Get(&get_ctx, &get, &get_resp);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(get_resp.found());
}

TEST(NodeRpcService, InternalPutRespectsLwwVersioning) {
    ServiceFixture fixture;

    kvstore::PutRequest put1;
    kvstore::PutResponse put_resp1;
    grpc::ServerContext ctx1;
    put1.set_key("k3");
    put1.set_value("v_new");
    put1.set_is_internal(true);
    put1.mutable_version()->set_write_created_at_us(200);
    put1.mutable_version()->set_writer_id("writerA");
    EXPECT_TRUE(fixture.service.Put(&ctx1, &put1, &put_resp1).ok());

    kvstore::PutRequest put2;
    kvstore::PutResponse put_resp2;
    grpc::ServerContext ctx2;
    put2.set_key("k3");
    put2.set_value("v_old");
    put2.set_is_internal(true);
    put2.mutable_version()->set_write_created_at_us(100);
    put2.mutable_version()->set_writer_id("writerA");
    EXPECT_TRUE(fixture.service.Put(&ctx2, &put2, &put_resp2).ok());

    kvstore::GetRequest get;
    kvstore::GetResponse get_resp;
    grpc::ServerContext get_ctx;
    get.set_key("k3");
    get.set_is_internal(true);

    EXPECT_TRUE(fixture.service.Get(&get_ctx, &get, &get_resp).ok());
    EXPECT_TRUE(get_resp.found());
    EXPECT_EQ(get_resp.value(), "v_new");
    EXPECT_EQ(get_resp.version().write_created_at_us(), 200u);
}

// An internal GET must return the local value only — no coordinator logic,
// no read repair. read_repair_count must stay zero.
TEST(NodeRpcService, InternalGetDoesNotTriggerReadRepair) {
    ServiceFixture fixture;

    kvstore::PutRequest put;
    kvstore::PutResponse put_resp;
    grpc::ServerContext put_ctx;
    put.set_key("k5");
    put.set_value("v5");
    put.set_is_internal(true);
    put.mutable_version()->set_write_created_at_us(100);
    put.mutable_version()->set_writer_id("writerA");
    EXPECT_TRUE(fixture.service.Put(&put_ctx, &put, &put_resp).ok());

    kvstore::GetRequest get;
    kvstore::GetResponse get_resp;
    grpc::ServerContext get_ctx;
    get.set_key("k5");
    get.set_is_internal(true);
    EXPECT_TRUE(fixture.service.Get(&get_ctx, &get, &get_resp).ok());
    EXPECT_TRUE(get_resp.found());

    EXPECT_EQ(fixture.node.metrics().read_repairs, 0u);
}

// An external GET must include the version fields in the response.
TEST(NodeRpcService, ExternalGetIncludesVersionInResponse) {
    ServiceFixture fixture;

    kvstore::PutRequest put;
    kvstore::PutResponse put_resp;
    grpc::ServerContext put_ctx;
    put.set_key("k6");
    put.set_value("v6");
    put.set_is_internal(false);
    EXPECT_TRUE(fixture.service.Put(&put_ctx, &put, &put_resp).ok());

    kvstore::GetRequest get;
    kvstore::GetResponse get_resp;
    grpc::ServerContext get_ctx;
    get.set_key("k6");
    get.set_is_internal(false);
    EXPECT_TRUE(fixture.service.Get(&get_ctx, &get, &get_resp).ok());
    ASSERT_TRUE(get_resp.found());
    EXPECT_GT(get_resp.version().write_created_at_us(), 0u);
    EXPECT_FALSE(get_resp.version().writer_id().empty());
}

// An external GET on a missing key must return found=false via the coordinator path.
TEST(NodeRpcService, ExternalGetMissingKeyReturnsNotFound) {
    ServiceFixture fixture;

    kvstore::GetRequest get;
    kvstore::GetResponse get_resp;
    grpc::ServerContext get_ctx;
    get.set_key("does_not_exist");
    get.set_is_internal(false);

    auto status = fixture.service.Get(&get_ctx, &get, &get_resp);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(get_resp.found());
}

TEST(NodeRpcService, InternalPutTieBreaksByWriterId) {
    ServiceFixture fixture;

    kvstore::PutRequest put1;
    kvstore::PutResponse put_resp1;
    grpc::ServerContext ctx1;
    put1.set_key("k4");
    put1.set_value("v_a");
    put1.set_is_internal(true);
    put1.mutable_version()->set_write_created_at_us(100);
    put1.mutable_version()->set_writer_id("A");
    EXPECT_TRUE(fixture.service.Put(&ctx1, &put1, &put_resp1).ok());

    kvstore::PutRequest put2;
    kvstore::PutResponse put_resp2;
    grpc::ServerContext ctx2;
    put2.set_key("k4");
    put2.set_value("v_z");
    put2.set_is_internal(true);
    put2.mutable_version()->set_write_created_at_us(100);
    put2.mutable_version()->set_writer_id("Z");
    EXPECT_TRUE(fixture.service.Put(&ctx2, &put2, &put_resp2).ok());

    kvstore::GetRequest get;
    kvstore::GetResponse get_resp;
    grpc::ServerContext get_ctx;
    get.set_key("k4");
    get.set_is_internal(true);

    EXPECT_TRUE(fixture.service.Get(&get_ctx, &get, &get_resp).ok());
    EXPECT_TRUE(get_resp.found());
    EXPECT_EQ(get_resp.value(), "v_z");
    EXPECT_EQ(get_resp.version().writer_id(), "Z");
}
