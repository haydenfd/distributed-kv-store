#include "kv/node/kv_service.h"

namespace kv {

KVService::KVService(kv::node::Node& node)
    : node_(node) {}

grpc::Status KVService::Put(
    grpc::ServerContext*,
    const kvstore::PutRequest* request,
    kvstore::PutResponse* response) {

    node_.put(request->key(), request->value());
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status KVService::Get(
    grpc::ServerContext*,
    const kvstore::GetRequest* request,
    kvstore::GetResponse* response) {

    auto val = node_.get(request->key());
    if (!val) {
        response->set_found(false);
    } else {
        response->set_found(true);
        response->set_value(*val);
    }
    return grpc::Status::OK;
}

} 