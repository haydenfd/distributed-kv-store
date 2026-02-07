#include "kv/node/node.h"

namespace kv::node {

Node::Node(std::string node_id)
    : node_id_(std::move(node_id)) {}

bool Node::put(std::string key, std::string value) {
    std::lock_guard<std::mutex> lock(mu_);
    store_[std::move(key)] = std::move(value);
    return true;
}

std::optional<std::string> Node::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = store_.find(key);
    if (it == store_.end()) {
        return std::nullopt;
    }
    return it->second;
}

} 