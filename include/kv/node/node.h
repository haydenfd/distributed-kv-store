#pragma once

#include <string>
#include <unordered_map>
#include <optional>
#include <mutex>

namespace kv::node {

class Node {
public:
    explicit Node(std::string node_id);

    bool put(std::string key, std::string value);
    std::optional<std::string> get(const std::string& key);

private:
    std::string node_id_;
    std::unordered_map<std::string, std::string> store_;
    std::mutex mu_;
};

} // namespace kv::node