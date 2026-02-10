#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <optional>
#include <chrono>
#include <mutex>
#include <memory>
#include <atomic>

#include "cluster/cluster_view.h"
#include "node/node_config.h"
#include "kv.grpc.pb.h"

namespace kv::node {

struct Version {
    uint64_t write_created_at_us; // write creation time (microseconds since epoch)
    std::string writer_id;  // who wrote the current version?
};

struct StoreEntry {
    std::string value;
    Version version;
};

struct NodeMetrics {
    uint64_t reads = 0;
    uint64_t writes = 0;
    uint64_t read_repairs = 0;
    uint64_t forward_failures = 0;
};

class Node {
public:
    Node(const kv::NodeConfig& config, kv::cluster::ClusterView& cluster);

    bool put(const std::string& key, const std::string& value);
    std::optional<StoreEntry> get(const std::string& key);

    const std::string& node_id() const { return config_.node_id; }
    size_t replication_factor() const { return config_.replication_factor; }
    int write_quorum() const { return config_.write_quorum; }

    bool forward_put(
        const std::string& owner_id,
        const std::string& key,
        const std::string& value,
        const Version& version,
        std::optional<std::chrono::milliseconds> deadline = std::nullopt
    );

    std::optional<StoreEntry> forward_get(
        const std::string& owner_id,
        const std::string& key,
        std::optional<std::chrono::milliseconds> deadline = std::nullopt
    );

    std::optional<StoreEntry> local_get(const std::string& key);

    bool apply_put_local(
        const std::string& key,
        const std::string& value,
        const Version& version
    );

    NodeMetrics metrics() const;

private:
    static bool is_newer(const Version& a, const Version& b);
    kvstore::KeyValue::Stub* get_or_create_stub(const std::string& node_id);

    kv::NodeConfig config_;
    kv::cluster::ClusterView& cluster_;
    std::unordered_map<std::string, StoreEntry> store_;
    std::mutex mu_;
    std::mutex stub_mu_;
    std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channel_cache_;
    std::unordered_map<std::string, std::unique_ptr<kvstore::KeyValue::Stub>> stub_cache_;

    std::atomic<uint64_t> read_count_{0};
    std::atomic<uint64_t> write_count_{0};
    std::atomic<uint64_t> read_repair_count_{0};
    std::atomic<uint64_t> forward_failure_count_{0};

};

}
