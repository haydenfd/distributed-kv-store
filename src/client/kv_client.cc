#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "kv.pb.h"
#include "kv.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using kvstore::KeyValue;
using kvstore::GetRequest;
using kvstore::GetResponse;
using kvstore::PutRequest;
using kvstore::PutResponse;

class KVClient {
public:
    explicit KVClient(std::shared_ptr<Channel> channel)
        : stub_(KeyValue::NewStub(channel)) {}

    bool Put(const std::string& key, const std::string& value) {
        PutRequest req;
        req.set_key(key);
        req.set_value(value);

        PutResponse resp;
        ClientContext ctx;

        Status status = stub_->Put(&ctx, req, &resp);
        if (!status.ok()) {
            std::cerr << "Put failed: " << status.error_message() << std::endl;
            return false;
        }

        return resp.success();
    }

    bool Get(const std::string& key, std::string& out_value) {
        GetRequest req;
        req.set_key(key);

        GetResponse resp;
        ClientContext ctx;

        Status status = stub_->Get(&ctx, req, &resp);
        if (!status.ok()) {
            std::cerr << "Get failed: " << status.error_message() << std::endl;
            return false;
        }

        if (!resp.found()) {
            return false;
        }

        out_value = resp.value();
        return true;
    }

private:
    std::unique_ptr<KeyValue::Stub> stub_;
};

int main() {
    KVClient client(
        grpc::CreateChannel("localhost:50051",
                             grpc::InsecureChannelCredentials())
    );

    std::cout << "Putting key=foo value=bar\n";
    client.Put("foo", "bar");

    std::string value;
    if (client.Get("foo", value)) {
        std::cout << "Got value: " << value << std::endl;
    } else {
        std::cout << "Key not found\n";
    }

    return 0;
}