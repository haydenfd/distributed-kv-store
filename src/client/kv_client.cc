#include <iostream>
#include <sstream>
#include <string>

#include <grpcpp/grpcpp.h>

#include "kv.grpc.pb.h"

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
                std::cout << "PUT failed\n";
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
                std::cout << "GET failed\n";
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

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage:\n"
                  << "  kv_client <addr> put <key> <value>\n"
                  << "  kv_client <addr> get <key>\n"
                  << "  kv_client <addr>\n";
        return 1;
    }

    std::string address = argv[1];

    auto channel = grpc::CreateChannel(
        address,
        grpc::InsecureChannelCredentials()
    );
    auto stub = kvstore::KeyValue::NewStub(channel);

    // --------------------
    // REPL mode
    // --------------------
    if (argc == 2) {
        run_repl(*stub);
        return 0;
    }

    // --------------------
    // One-shot mode
    // --------------------
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
            std::cerr << "PUT failed\n";
            return 1;
        }

        std::cout << "PUT ok\n";
    } else if (cmd == "get") {
        kvstore::GetRequest req;
        kvstore::GetResponse resp;
        req.set_key(key);

        auto status = stub->Get(&ctx, req, &resp);
        if (!status.ok()) {
            std::cerr << "GET failed\n";
            return 1;
        }

        if (!resp.found()) {
            std::cout << "Key not found\n";
        } else {
            std::cout << "Got value: " << resp.value() << "\n";
        }
    } else {
        std::cerr << "Unknown command: " << cmd << "\n";
        return 1;
    }

    return 0;
}