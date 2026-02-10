#include <iostream>
#include <sstream>
#include <string>

#include <grpcpp/grpcpp.h>

#include "kv.grpc.pb.h"

// Interactive REPL for manual testing and ad-hoc commands.
void run_repl(kvstore::KeyValue::Stub& stub) {
    std::string line;

    while (true) {
        std::cout << "kv> ";
        if (!std::getline(std::cin, line)) {
            break;
        }

        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;

        if (cmd == "exit" || cmd == "quit") {
            break;
        }

        grpc::ClientContext ctx;

        if (cmd == "put") {
            std::string key, value;
            iss >> key >> value;

            if (key.empty() || value.empty()) {
                std::cout << "Usage: put <key> <value>\n";
                continue;
            }

            kvstore::PutRequest req;
            kvstore::PutResponse resp;
            req.set_key(key);
            req.set_value(value);

            auto status = stub.Put(&ctx, req, &resp);

            if (!status.ok()) {
                std::cout << "PUT RPC failed\n";
            } else if (!resp.success()) {
                std::cout << "PUT rejected (acks < W)\n";
            } else {
                std::cout << "PUT ok\n";
            }

        } else if (cmd == "get") {
            std::string key;
            iss >> key;

            if (key.empty()) {
                std::cout << "Usage: get <key>\n";
                continue;
            }

            kvstore::GetRequest req;
            kvstore::GetResponse resp;
            req.set_key(key);

            auto status = stub.Get(&ctx, req, &resp);

            if (!status.ok()) {
                std::cout << "GET RPC failed\n";
            } else if (!resp.found()) {
                std::cout << "Key not found\n";
            } else {
                std::cout << resp.value() << "\n";
            }

        } else if (!cmd.empty()) {
            std::cout << "Unknown command\n";
        }
    }
}

// Sequential batch PUTs used by simple benchmark drivers.
int run_batch_put(kvstore::KeyValue::Stub& stub,
                  const std::string& key_prefix,
                  const std::string& value,
                  int count) {
    kvstore::PutRequest req;
    kvstore::PutResponse resp;
    req.set_value(value);

    for (int i = 0; i < count; ++i) {
        std::string key = key_prefix + "_" + std::to_string(i);
        grpc::ClientContext ctx;

        req.set_key(key);

        resp.Clear();
        auto status = stub.Put(&ctx, req, &resp);
        if (!status.ok() || !resp.success()) {
            std::cerr << "batch_put failed at i=" << i << "\n";
            return 1;
        }
    }
    return 0;
}

// Sequential batch GETs used by simple benchmark drivers.
int run_batch_get(kvstore::KeyValue::Stub& stub,
                  const std::string& key,
                  int count) {
    kvstore::GetRequest req;
    kvstore::GetResponse resp;
    req.set_key(key);

    for (int i = 0; i < count; ++i) {
        grpc::ClientContext ctx;

        resp.Clear();
        auto status = stub.Get(&ctx, req, &resp);
        if (!status.ok()) {
            std::cerr << "batch_get failed at i=" << i << "\n";
            return 1;
        }
    }
    return 0;
}

static void print_usage() {
    std::cerr << "Usage:\n"
              << "  kv_cli <addr> put <key> <value>\n"
              << "  kv_cli <addr> get <key>\n"
              << "  kv_cli <addr> batch_put <key_prefix> <value> <count>\n"
              << "  kv_cli <addr> batch_get <key> <count>\n"
              << "  kv_cli <addr>\n";
}

// CLI entrypoint; routes to REPL or one-shot commands.
int main(int argc, char** argv) {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    if (argc < 2) {
        print_usage();
        return 1;
    }

    std::string address = argv[1];

    auto channel = grpc::CreateChannel(
        address,
        grpc::InsecureChannelCredentials()
    );
    auto stub = kvstore::KeyValue::NewStub(channel);

    // REPL mode
    if (argc == 2) {
        run_repl(*stub);
        return 0;
    }

    // One-shot mode
    if (argc < 4) {
        std::cerr << "Invalid command\n";
        return 1;
    }

    std::string cmd = argv[2];
    std::string key = argv[3];
    grpc::ClientContext ctx;

    if (cmd == "put") {
        if (argc < 5) {
            std::cerr << "put requires a value\n";
            return 1;
        }

        std::string value = argv[4];

        kvstore::PutRequest req;
        kvstore::PutResponse resp;
        req.set_key(key);
        req.set_value(value);

        auto status = stub->Put(&ctx, req, &resp);

        if (!status.ok()) {
            std::cerr << "PUT RPC failed\n";
            return 1;
        }

        if (!resp.success()) {
            std::cerr << "PUT rejected (acks < W)\n";
            return 1;
        }

        std::cout << "PUT ok\n";

    } else if (cmd == "get") {
        kvstore::GetRequest req;
        kvstore::GetResponse resp;
        req.set_key(key);

        auto status = stub->Get(&ctx, req, &resp);

        if (!status.ok()) {
            std::cerr << "GET RPC failed\n";
            return 1;
        }

        if (!resp.found()) {
            std::cout << "Key not found\n";
        } else {
            std::cout << "Got value: " << resp.value() << "\n";
        }

    } else if (cmd == "batch_put") {
        if (argc < 6) {
            std::cerr << "batch_put requires <key_prefix> <value> <count>\n";
            return 1;
        }
        std::string key_prefix = argv[3];
        std::string value = argv[4];
        int count = std::stoi(argv[5]);
        if (count < 0) {
            std::cerr << "count must be non-negative\n";
            return 1;
        }
        return run_batch_put(*stub, key_prefix, value, count);
    } else if (cmd == "batch_get") {
        if (argc < 5) {
            std::cerr << "batch_get requires <key> <count>\n";
            return 1;
        }
        std::string key_arg = argv[3];
        int count = std::stoi(argv[4]);
        if (count < 0) {
            std::cerr << "count must be non-negative\n";
            return 1;
        }
        return run_batch_get(*stub, key_arg, count);
    } else {
        std::cerr << "Unknown command: " << cmd << "\n";
        return 1;
    }

    return 0;
}
