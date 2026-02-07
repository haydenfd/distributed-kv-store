#include "kv/ring/consistent_hash_ring.h"
#include "kv/hash/murmur3.h"

#include <unordered_set>
#include <algorithm>
#include <stdexcept>

namespace kv::ring {

static constexpr uint64_t DEFAULT_SEED = 0xdeadbeef;

ConsistentHashRing::ConsistentHashRing(size_t vnodes)
    : vnodes_(vnodes) {}

void ConsistentHashRing::add_node(const std::string& node_id) {
    for (size_t i = 0; i < vnodes_; ++i) {
        std::string vnode_key = node_id + "#" + std::to_string(i);
        uint64_t h = hash(vnode_key);
        ring_[h] = node_id;
    }
}

void ConsistentHashRing::remove_node(const std::string& node_id) {
    for (auto it = ring_.begin(); it != ring_.end();) {
        if (it->second == node_id) {
            it = ring_.erase(it);
        } else {
            ++it;
        }
    }
}

std::string ConsistentHashRing::get_owner_node(const std::string& key) const {
    if (ring_.empty()) {
        throw std::runtime_error("hash ring is empty");
    }

    uint64_t h = hash(key);
    auto it = ring_.lower_bound(h);

    if (it == ring_.end()) {
        return ring_.begin()->second; // wrap around
    }
    return it->second;
}

std::vector<std::string>
ConsistentHashRing::get_preference_list(const std::string& key,
                                        size_t num_replicas) const {
    std::vector<std::string> result;
    if (ring_.empty() || num_replicas == 0) return result;

    std::unordered_set<std::string> seen;
    const uint64_t key_hash = hash(key);

    auto it = ring_.lower_bound(key_hash);
    if (it == ring_.end()) it = ring_.begin();

    auto start = it;

    do {
        const std::string& node = it->second;

        if (seen.insert(node).second) {
            result.push_back(node);
            if (result.size() == num_replicas) {
                break;
            }
        }

        ++it;
        if (it == ring_.end()) it = ring_.begin();

    } while (it != start && seen.size() < ring_.size());

    return result;
}

size_t ConsistentHashRing::size() {
    return ring_.size();
}

uint64_t ConsistentHashRing::hash(const std::string& key) const {
    return kv::hash::murmur3_64(key, DEFAULT_SEED);
}

} // namespace kv::ring