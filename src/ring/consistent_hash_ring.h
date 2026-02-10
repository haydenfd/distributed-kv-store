#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <cstddef>

/* 
- This module is internally not thread-safe, requires external synchronization if used from multiple threads.
- Called by ClusterView, which itself is thread-safe, so this isn't a real concern for current usage.
*/
namespace kv::ring {

    class ConsistentHashRing {
    public:
        explicit ConsistentHashRing(size_t vnodes = 100);

        // API for handling adding/removing/accessing nodes
        void add_node(const std::string& node_id);
        void remove_node(const std::string& node_id);
        std::string get_owner_node(const std::string& key) const;

        std::vector<std::string> get_preference_list(const std::string& key, size_t num_replicas) const;

       size_t size() const;

    private:
        size_t vnodes_;
        std::map<uint64_t, std::string> ring_;
        uint64_t hash(const std::string& key) const;
    };
}