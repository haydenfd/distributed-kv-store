#pragma once
#include <memory>

#include "kv.pb.h"
#include "kv.grpc.pb.h"
#include "kv/node/node.h"

namespace kv {

class KVService final : public kvstore::KeyValue::Service {
public:
    explicit KVService(kv::node::Node& node);

    grpc::Status Get(
        grpc::ServerContext* context,
        const kvstore::GetRequest* request,
        kvstore::GetResponse* response) override;

    grpc::Status Put(
        grpc::ServerContext* context,
        const kvstore::PutRequest* request,
        kvstore::PutResponse* response) override;

private:
    kv::node::Node& node_;
};

} // namespace kv