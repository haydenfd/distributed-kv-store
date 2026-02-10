#include <iostream>
#include <fstream>
#include <string>

#include <grpcpp/grpcpp.h>
#include <yaml-cpp/yaml.h>

#include "node/node.h"
#include "node/node_rpc_service.h"
#include "node/node_config.h"
#include "cluster/cluster_view.h"
#include "utils/logging.h"

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
    std::string log_level_arg;

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
        } else if (arg == "--log-level" && i + 1 < argc) {
            log_level_arg = argv[++i];
        }
    }

    if (node_id.empty() || port <= 0 || config_path.empty()) {
        std::cerr << "Usage: kv_node --id <node-id> --port <port> --config <cluster.yaml> "
                     "[--log-level <none|info|debug>]\n";
        return 1;
    }

    kv::log::init_from_env();
    if (!log_level_arg.empty()) {
        kv::log::set_level(kv::log::parse_level(log_level_arg));
    }

    // --------------------
    // Load cluster config
    // --------------------
    YAML::Node config = YAML::LoadFile(config_path);

    kv::cluster::ClusterView cluster;

    YAML::Node cluster_nodes = config["cluster"]["seeds"];
    if (!cluster_nodes || !cluster_nodes.IsSequence()) {
        cluster_nodes = config["cluster"]["nodes"];
    }

    // Parse replication settings from cluster config
    size_t replication_factor = 3;  // default
    int write_quorum = 1;           // default

    if (config["cluster"]["replication_factor"]) {
        replication_factor = config["cluster"]["replication_factor"].as<size_t>();
    }
    if (config["cluster"]["write_quorum"]) {
        write_quorum = config["cluster"]["write_quorum"].as<int>();
    }

    LOG_INFO("Cluster config: RF=" << replication_factor
             << " W=" << write_quorum
             << " (reads use LWW)");

    std::string self_address_from_config;
    if (cluster_nodes && cluster_nodes.IsSequence()) {
        for (const auto& seed : cluster_nodes) {
            std::string seed_id = seed["node_id"].as<std::string>();
            std::string address = seed["address"].as<std::string>();
            cluster.add_node_to_cluster(seed_id, address);
            if (seed_id == node_id) {
                self_address_from_config = address;
            }
        }
    }

    // --------------------
    // Build node + service
    // --------------------
    std::string bind_addr = "0.0.0.0";
    std::string listen_addr = bind_addr + ":" + std::to_string(port);

    if (!cluster.get_node_address(node_id).has_value()) {
        std::string self_address = self_address_from_config.empty()
            ? ("localhost:" + std::to_string(port))
            : self_address_from_config;
        cluster.add_node_to_cluster(node_id, self_address);
    }

    // Create node config
    kv::NodeConfig node_config;
    node_config.node_id = node_id;
    node_config.bind_addr = bind_addr;
    node_config.port = port;
    node_config.replication_factor = replication_factor;
    node_config.write_quorum = write_quorum;

    if (auto err = node_config.validate()) {
        std::cerr << "Invalid config: " << *err << "\n";
        return 1;
    }

    kv::node::Node node(node_config, cluster);
    kv::NodeRpcService service(node);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

    LOG_INFO("Node " << node_id << " listening on " << listen_addr);

    server->Wait();
    return 0;
}
