#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <map>

namespace kv::ring {

    class ConsistentHashRing {
    public:
        explicit ConsistentHashRing(size_t vnodes = 100);

        // API for handling adding/removing/accessing nodes
        void add_node(const std::string& node_id);
        void remove_node(const std::string& node_id);
        std::string get_owner_node(const std::string& key) const;

        // For latter - replication (get a key's preference list aka all the nodes responsible for it)
        std::vector<std::string> get_preference_list(const std::string& key, size_t num_replicas) const;

       size_t size();

    private:
        size_t vnodes_;
        std::map<uint64_t, std::string> ring_; // hash -> node_id
        uint64_t hash(const std::string& key) const;
    };
}