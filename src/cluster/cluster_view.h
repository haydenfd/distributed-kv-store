#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <memory>
#include <optional>

#include <grpcpp/grpcpp.h>

#include "ring/consistent_hash_ring.h"

namespace kv::cluster {

class ClusterView {
public:
    explicit ClusterView(size_t vnodes = 100);

    void add_node_to_cluster(const std::string& node_id, const std::string& address);
    void remove_node_from_cluster(const std::string& node_id);

    std::vector<std::string> get_node_ids() const;

    std::optional<std::string> get_node_address(const std::string& node_id) const;

    std::vector<std::string>
    get_replica_set_for_key(const std::string& key, size_t replication_factor) const;
    
    std::shared_ptr<grpc::Channel> create_grpc_channel_for_node(const std::string& node_id) const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::string> nodes_;
    kv::ring::ConsistentHashRing ring_;
};

} 
