#include <iostream>
#include <fstream>
#include <string>

#include <grpcpp/grpcpp.h>
#include <yaml-cpp/yaml.h>

#include "kv/node/node.h"
#include "kv/node/kv_service.h"
#include "kv/node/node_config.h"
#include "kv/cluster/cluster_view.h"

/*
CLI:
  --id <node-id>
  --port <port>
  --config <cluster.yaml>
*/

int main(int argc, char** argv) {
    std::string node_id;
    std::string config_path;
    int port = -1;

    // --------------------
    // Parse CLI args
    // --------------------
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--id" && i + 1 < argc) {
            node_id = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = std::stoi(argv[++i]);
        } else if (arg == "--config" && i + 1 < argc) {
            config_path = argv[++i];
        }
    }

    if (node_id.empty() || port <= 0 || config_path.empty()) {
        std::cerr << "Usage: kv_node --id <node-id> --port <port> --config <cluster.yaml>\n";
        return 1;
    }

    // --------------------
    // Load cluster config
    // --------------------
    YAML::Node config = YAML::LoadFile(config_path);

    kv::cluster::ClusterView cluster;

    for (const auto& seed : config["cluster"]["seeds"]) {
        std::string seed_id = seed["node_id"].as<std::string>(); 
        std::string address = seed["address"].as<std::string>();
        cluster.add_node(seed_id, address);
    }

    std::cout << "Cluster nodes:\n";
    for (const auto& id : cluster.node_ids()) {
        std::cout << "  - " << id << "\n";
    }    

    std::vector<std::string> test_keys = {
        "alpha", "beta", "gamma", "delta", "epsilon"
    };

    std::cout << "=== Ownership test ===\n";
    for (const auto& key : test_keys) {
        std::cout << "key=" << key
                << " owner=" << cluster.owner_for_key(key)
                << "\n";
    }
    std::cout << "======================\n";    
    // --------------------
    // Build node + service
    // --------------------
    std::string bind_addr = "0.0.0.0";
    std::string listen_addr = bind_addr + ":" + std::to_string(port);

    kv::node::Node node(node_id, cluster);
    kv::KVService service(node);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

    std::cout << "Node " << node_id
              << " listening on " << listen_addr
              << std::endl;

    server->Wait();
    return 0;
}