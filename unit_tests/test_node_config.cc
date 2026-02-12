#include <gtest/gtest.h>

#include "node/node_config.h"

using kv::NodeConfig;

namespace {
NodeConfig valid_config() {
    NodeConfig cfg;
    cfg.node_id = "node-1";
    cfg.port = 50051;
    cfg.replication_factor = 3;
    cfg.write_quorum = 1;
    return cfg;
}
}  // namespace

TEST(NodeConfig, ValidConfigPassesValidation) {
    EXPECT_FALSE(valid_config().validate().has_value());
}

TEST(NodeConfig, WriteQuorumEqualsReplicationFactorIsValid) {
    auto cfg = valid_config();
    cfg.write_quorum = static_cast<int>(cfg.replication_factor);
    EXPECT_FALSE(cfg.validate().has_value());
}

TEST(NodeConfig, ReplicationFactorZeroFails) {
    auto cfg = valid_config();
    cfg.replication_factor = 0;
    ASSERT_TRUE(cfg.validate().has_value());
    EXPECT_NE(cfg.validate()->find("replication_factor"), std::string::npos);
}

TEST(NodeConfig, WriteQuorumZeroFails) {
    auto cfg = valid_config();
    cfg.write_quorum = 0;
    ASSERT_TRUE(cfg.validate().has_value());
    EXPECT_NE(cfg.validate()->find("write_quorum"), std::string::npos);
}

TEST(NodeConfig, WriteQuorumExceedsReplicationFactorFails) {
    auto cfg = valid_config();
    cfg.replication_factor = 2;
    cfg.write_quorum = 3;
    ASSERT_TRUE(cfg.validate().has_value());
    EXPECT_NE(cfg.validate()->find("write_quorum"), std::string::npos);
}

TEST(NodeConfig, PortZeroFails) {
    auto cfg = valid_config();
    cfg.port = 0;
    ASSERT_TRUE(cfg.validate().has_value());
    EXPECT_NE(cfg.validate()->find("port"), std::string::npos);
}

TEST(NodeConfig, NegativePortFails) {
    auto cfg = valid_config();
    cfg.port = -1;
    ASSERT_TRUE(cfg.validate().has_value());
    EXPECT_NE(cfg.validate()->find("port"), std::string::npos);
}

TEST(NodeConfig, EmptyNodeIdFails) {
    auto cfg = valid_config();
    cfg.node_id = "";
    ASSERT_TRUE(cfg.validate().has_value());
    EXPECT_NE(cfg.validate()->find("node_id"), std::string::npos);
}

TEST(NodeConfig, NegativeWriteQuorumFails) {
    auto cfg = valid_config();
    cfg.write_quorum = -1;
    ASSERT_TRUE(cfg.validate().has_value());
    EXPECT_NE(cfg.validate()->find("write_quorum"), std::string::npos);
}
