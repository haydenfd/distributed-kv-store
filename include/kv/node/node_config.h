#pragma once

#include <string>
#include <vector>

namespace kv {

struct NodeConfig {
    std::string node_id;
    std::string bind_addr = "0.0.0.0";
    int port;
};

} 