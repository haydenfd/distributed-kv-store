#pragma once
#include "kv.pb.h"
#include "kv.grpc.pb.h"
#include "node/node.h"

namespace kv {

class NodeRpcService final : public kvstore::KeyValue::Service {
public:
    explicit NodeRpcService(kv::node::Node& node);

    grpc::Status Get(
        grpc::ServerContext* context,
        const kvstore::GetRequest* request,
        kvstore::GetResponse* response) override;

    grpc::Status Put(
        grpc::ServerContext* context,
        const kvstore::PutRequest* request,
        kvstore::PutResponse* response) override;

private:
    kv::node::Node& node_ref_;
};

} 
