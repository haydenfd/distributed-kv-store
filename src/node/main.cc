#include <iostream>
#include <grpcpp/grpcpp.h>

#include "kv/node/node.h"
#include "kv/node/kv_service.h"
#include "kv/node/node_config.h"

int main(int argc, char** argv) {
    kv::NodeConfig cfg;
    cfg.node_id = "node-1";
    cfg.port = 50051;

    std::string address = cfg.bind_addr + ":" + std::to_string(cfg.port);

    kv::node::Node node(cfg.node_id);
    kv::KVService service(node);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Node " << cfg.node_id << " listening on " << address << std::endl;

    server->Wait();
    return 0;
}