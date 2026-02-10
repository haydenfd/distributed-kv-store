#include "kv/cluster/cluster_view.h"

#include <utility>

namespace kv::cluster {

ClusterView::ClusterView(size_t vnodes)
    : ring_(vnodes) {}

void ClusterView::add_node_to_cluster(const std::string& node_id,
                                      const std::string& address) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (nodes_.find(node_id) != nodes_.end()) {
        return;
    }

    nodes_.emplace(node_id, address);
    ring_.add_node(node_id);
}

void ClusterView::remove_node_from_cluster(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        return;
    }

    ring_.remove_node(node_id);
    nodes_.erase(it);
}

std::vector<std::string> ClusterView::get_node_ids() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<std::string> ids;
    ids.reserve(nodes_.size());
    for (const auto& [id, _] : nodes_) {
        ids.push_back(id);
    }
    return ids;
}

std::vector<std::string>
ClusterView::get_replica_set_for_key(const std::string& key,
                                     size_t replication_factor) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return ring_.get_preference_list(key, replication_factor);
}

std::optional<std::string> ClusterView::get_node_address(const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::shared_ptr<grpc::Channel>
ClusterView::create_grpc_channel_for_node(const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        return nullptr;
    }

    return grpc::CreateChannel(
        it->second,
        grpc::InsecureChannelCredentials()
    );
}

} 
