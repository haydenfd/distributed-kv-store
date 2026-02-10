#include "node/node.h"

#include <iostream>
#include <chrono>
#include <vector>
#include <sstream>
#include <grpcpp/grpcpp.h>

#include "kv.grpc.pb.h"
#include "utils/logging.h"

namespace kv::node {

namespace {
// Helper to format vector as comma-separated string for logging
std::string format_list(const std::vector<std::string>& items) {
    if (items.empty()) return "";
    std::ostringstream oss;
    for (size_t i = 0; i < items.size(); ++i) {
        if (i > 0) oss << ",";
        oss << items[i];
    }
    return oss.str();
}
}  

Node::Node(const kv::NodeConfig& config, kv::cluster::ClusterView& cluster)
    : config_(config),
      cluster_(cluster) {}

bool Node::put(const std::string& key, const std::string& value) {
    write_count_.fetch_add(1, std::memory_order_relaxed);
    const size_t RF = config_.replication_factor;
    const int W = config_.write_quorum;

    auto replicas = cluster_.get_replica_set_for_key(key, RF);

    auto now = std::chrono::system_clock::now();
    auto write_created_at_us = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()
    ).count();

    Version version{
        static_cast<uint64_t>(write_created_at_us),
        config_.node_id
    };

    LOG_DEBUG("[node=" << config_.node_id << "] PUT version (key=" << key
              << "): write_created_at_us=" << version.write_created_at_us
              << " writer=" << version.writer_id);

    if (kv::log::g_log_level == kv::log::LogLevel::Debug) {
        LOG_DEBUG("[node=" << config_.node_id << "] PUT preference list (key=" << key
                  << "): " << format_list(replicas));
    }

    int acks = 0;

    for (const auto& replica_id : replicas) {
        if (replica_id == config_.node_id) {
            if (apply_put_local(key, value, version)) {
                acks++;
            }
        } else {
            LOG_DEBUG("[node=" << config_.node_id
                      << "] forwarding PUT to " << replica_id
                      << " (key=" << key << ")");

            if (forward_put(replica_id, key, value, version)) {
                acks++;
            }
        }
    }

    LOG_DEBUG("[node=" << config_.node_id << "] PUT key=" << key
              << " acks=" << acks << "/" << replicas.size()
              << " (W=" << W << ")");

    return acks >= W;
}

std::optional<StoreEntry> Node::get(const std::string& key) {
    read_count_.fetch_add(1, std::memory_order_relaxed);
    const size_t RF = config_.replication_factor;
    auto replicas = cluster_.get_replica_set_for_key(key, RF);

    if (kv::log::g_log_level == kv::log::LogLevel::Debug) {
        LOG_DEBUG("[node=" << config_.node_id << "] GET preference list (key=" << key
                  << "): " << format_list(replicas));
    }

    struct ReplicaRead {
        std::string node_id;
        std::optional<StoreEntry> entry;
    };

    std::vector<ReplicaRead> reads;
    reads.reserve(replicas.size());

    for (const auto& replica_id : replicas) {
        std::optional<StoreEntry> entry;

        LOG_DEBUG("[node=" << config_.node_id
                  << "] GET contacting replica " << replica_id);

        if (replica_id == config_.node_id) {
            entry = local_get(key);
        } else {
            entry = forward_get(replica_id, key, std::chrono::milliseconds(50));
        }

        if (!entry) {
            LOG_DEBUG("[node=" << config_.node_id
                      << "] GET miss from " << replica_id);
        }

        reads.push_back(ReplicaRead{replica_id, entry});
    }

    std::optional<StoreEntry> best;
    std::string best_node;

    for (const auto& read : reads) {
        if (read.entry) {
            LOG_DEBUG("[node=" << config_.node_id << "] GET candidate (key=" << key
                      << ") from " << read.node_id
                      << " write_created_at_us=" << read.entry->version.write_created_at_us
                      << " writer=" << read.entry->version.writer_id);

            if (!best || is_newer(read.entry->version, best->version)) {
                best = *read.entry;
                best_node = read.node_id;
            }
        }
    }

    if (!best) {
        return std::nullopt;
    }

    LOG_DEBUG("[node=" << config_.node_id << "] READ_REPAIR winner key=" << key
              << " write_created_at_us=" << best->version.write_created_at_us
              << " writer=" << best->version.writer_id);

    for (const auto& read : reads) {
        if (!read.entry || is_newer(best->version, read.entry->version)) {
            bool ok = false;

            if (read.node_id == config_.node_id) {
                ok = apply_put_local(key, best->value, best->version);
            } else {
                ok = forward_put(
                    read.node_id,
                    key,
                    best->value,
                    best->version,
                    std::chrono::milliseconds(50)
                );
            }

            read_repair_count_.fetch_add(1, std::memory_order_relaxed);
            LOG_DEBUG("[node=" << config_.node_id
                      << "] READ_REPAIR sent to "
                      << read.node_id
                      << " ok=" << (ok ? "true" : "false"));
        }
    }

    return best;
}

kvstore::KeyValue::Stub* Node::get_or_create_stub(const std::string& node_id) {
    // Fast path: check if stub already exists
    {
        std::lock_guard<std::mutex> lock(stub_mu_);
        auto it = stub_cache_.find(node_id);
        if (it != stub_cache_.end()) {
            return it->second.get();
        }
    }

    // Slow path: create channel and stub outside the lock
    auto address = cluster_.get_node_address(node_id);
    if (!address) {
        return nullptr;
    }

    auto channel = grpc::CreateChannel(
        *address,
        grpc::InsecureChannelCredentials()
    );

    auto stub = kvstore::KeyValue::NewStub(channel);

    // Double-checked locking: re-check if another thread created it
    std::lock_guard<std::mutex> lock(stub_mu_);
    auto it = stub_cache_.find(node_id);
    if (it != stub_cache_.end()) {
        // Another thread created it, use theirs
        return it->second.get();
    }

    // We're first, insert our new stub and channel
    kvstore::KeyValue::Stub* stub_ptr = stub.get();
    stub_cache_[node_id] = std::move(stub);
    channel_cache_[node_id] = std::move(channel);
    return stub_ptr;
}

bool Node::forward_put(
    const std::string& owner_id,
    const std::string& key,
    const std::string& value,
    const Version& version,
    std::optional<std::chrono::milliseconds> deadline
) {
    auto* stub = get_or_create_stub(owner_id);
    if (!stub) {
        forward_failure_count_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    kvstore::PutRequest req;
    kvstore::PutResponse resp;
    grpc::ClientContext ctx;

    if (deadline) {
        ctx.set_deadline(std::chrono::system_clock::now() + *deadline);
    }

    req.set_key(key);
    req.set_value(value);
    req.set_is_internal(true);
    req.mutable_version()->set_write_created_at_us(version.write_created_at_us);
    req.mutable_version()->set_writer_id(version.writer_id);

    auto status = stub->Put(&ctx, req, &resp);
    if (!status.ok() || !resp.success()) {
        forward_failure_count_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    return true;
}

std::optional<StoreEntry> Node::forward_get(
    const std::string& owner_id,
    const std::string& key,
    std::optional<std::chrono::milliseconds> deadline
) {
    auto* stub = get_or_create_stub(owner_id);
    if (!stub) {
        forward_failure_count_.fetch_add(1, std::memory_order_relaxed);
        return std::nullopt;
    }

    kvstore::GetRequest req;
    kvstore::GetResponse resp;
    grpc::ClientContext ctx;

    req.set_key(key);
    req.set_is_internal(true);

    if (deadline) {
        ctx.set_deadline(std::chrono::system_clock::now() + *deadline);
    }

    auto status = stub->Get(&ctx, req, &resp);
    if (!status.ok()) {
        forward_failure_count_.fetch_add(1, std::memory_order_relaxed);
        return std::nullopt;
    }
    if (!resp.found()) {
        return std::nullopt;
    }

    Version version{
        resp.version().write_created_at_us(),
        resp.version().writer_id()
    };

    return StoreEntry{resp.value(), version};
}

std::optional<StoreEntry> Node::local_get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = store_.find(key);
    if (it == store_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool Node::apply_put_local(
    const std::string& key,
    const std::string& value,
    const Version& version
) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = store_.find(key);

    if (it == store_.end()) {
        store_[key] = StoreEntry{value, version};
        LOG_DEBUG("[node=" << config_.node_id << "] apply PUT (key=" << key
                  << ") incoming write_created_at_us=" << version.write_created_at_us
                  << " writer=" << version.writer_id
                  << " existing=none overwrite=true");
        return true;
    }

    Version existing = it->second.version;
    bool overwrite = false;
    if (is_newer(version, existing)) {
        it->second = StoreEntry{value, version};
        overwrite = true;
    }
    LOG_DEBUG("[node=" << config_.node_id << "] apply PUT (key=" << key
              << ") incoming write_created_at_us=" << version.write_created_at_us
              << " writer=" << version.writer_id
              << " existing write_created_at_us=" << existing.write_created_at_us
              << " writer=" << existing.writer_id
              << " overwrite=" << (overwrite ? "true" : "false"));

    return true;
}

bool Node::is_newer(const Version& a, const Version& b) {
    if (a.write_created_at_us != b.write_created_at_us) {
        return a.write_created_at_us > b.write_created_at_us;
    }
    return a.writer_id > b.writer_id;
}

NodeMetrics Node::metrics() const {
    NodeMetrics m;
    m.reads = read_count_.load(std::memory_order_relaxed);
    m.writes = write_count_.load(std::memory_order_relaxed);
    m.read_repairs = read_repair_count_.load(std::memory_order_relaxed);
    m.forward_failures = forward_failure_count_.load(std::memory_order_relaxed);
    return m;
}

}
