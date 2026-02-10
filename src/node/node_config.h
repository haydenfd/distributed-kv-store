#pragma once

#include <optional>
#include <string>
#include <vector>

namespace kv {

struct NodeConfig {
    std::string node_id;
    std::string bind_addr = "0.0.0.0";
    int port;

    // Replication configuration
    size_t replication_factor = 3;  // RF: number of replicas
    int write_quorum = 1;            // W: writes needed for success

    // Returns an error message if invalid, otherwise std::nullopt.
    std::optional<std::string> validate() const {
        if (replication_factor == 0) {
            return "replication_factor must be >= 1";
        }
        if (write_quorum <= 0) {
            return "write_quorum must be >= 1";
        }
        if (write_quorum > static_cast<int>(replication_factor)) {
            return "write_quorum cannot exceed replication_factor";
        }
        if (port <= 0) {
            return "port must be > 0";
        }
        if (node_id.empty()) {
            return "node_id must not be empty";
        }
        return std::nullopt;
    }
};

} 
