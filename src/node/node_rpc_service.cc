#include "kv/node/node_rpc_service.h"

#include "kv/utils/logging.h"
namespace kv {
namespace {
// Populate GetResponse with entry data or mark as not found.
void fill_get_response(const std::optional<kv::node::StoreEntry>& entry,
                       kvstore::GetResponse* response) {
    if (!entry) {
        response->set_found(false);
        return;
    }
    response->set_found(true);
    response->set_value(entry->value);
    response->mutable_version()->set_write_created_at_us(entry->version.write_created_at_us);
    response->mutable_version()->set_writer_id(entry->version.writer_id);
}
} 

// Construct the RPC service adapter for a specific node instance.
NodeRpcService::NodeRpcService(kv::node::Node& node)
    : node_ref_(node) {}

// Handle Put RPCs; internal requests apply locally, external requests coordinate replication.
grpc::Status NodeRpcService::Put(
    grpc::ServerContext* ,
    const kvstore::PutRequest* request,
    kvstore::PutResponse* response) {

    if (request->is_internal()) {
        LOG_DEBUG("[node=" << node_ref_.node_id()
                  << "] internal PUT (key=" << request->key() << ")");
        kv::node::Version version{
            request->version().write_created_at_us(),
            request->version().writer_id()
        };
        bool ok = node_ref_.apply_put_local(request->key(), request->value(), version);
        response->set_success(ok);
        return grpc::Status::OK;
    }

    bool ok = node_ref_.put(request->key(), request->value());
    response->set_success(ok);
    return grpc::Status::OK;
}


// Handle Get RPCs; internal requests read locally, external requests coordinate reads.
grpc::Status NodeRpcService::Get(
    grpc::ServerContext* /*context*/,
    const kvstore::GetRequest* request,
    kvstore::GetResponse* response) {

    // INTERNAL REPLICA GET: do NOT forward
    if (request->is_internal()) {
        LOG_DEBUG("[node=" << node_ref_.node_id()
                  << "] internal GET (key=" << request->key() << ")");
        auto entry = node_ref_.local_get(request->key());
        fill_get_response(entry, response);
        return grpc::Status::OK;
    }

    // CLIENT GET: coordinator path (may forward)
    auto entry = node_ref_.get(request->key());
    fill_get_response(entry, response);
    return grpc::Status::OK;
}

} 
