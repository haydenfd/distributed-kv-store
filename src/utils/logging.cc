#include "kv/utils/logging.h"

#include <cctype>
#include <cstdlib>
#include <string>

namespace kv::log {

std::atomic<LogLevel> g_log_level{LogLevel::Debug};

// Case-insensitive equality without allocations.
static bool iequals(std::string_view value, std::string_view target) {
    if (value.size() != target.size()) {
        return false;
    }
    for (size_t i = 0; i < value.size(); ++i) {
        unsigned char lhs = static_cast<unsigned char>(value[i]);
        unsigned char rhs = static_cast<unsigned char>(target[i]);
        if (std::tolower(lhs) != std::tolower(rhs)) {
            return false;
        }
    }
    return true;
}

LogLevel parse_level(std::string_view value) {
    // Fast path for numeric aliases.
    if (value.size() == 1) {
        if (value[0] == '0') return LogLevel::None;
        if (value[0] == '1') return LogLevel::Info;
        if (value[0] == '2') return LogLevel::Debug;
    }
    if (iequals(value, "none") || iequals(value, "off")) {
        return LogLevel::None;
    }
    if (iequals(value, "info")) {
        return LogLevel::Info;
    }
    if (iequals(value, "debug")) {
        return LogLevel::Debug;
    }
    return LogLevel::Debug;
}

void init_from_env() {
    const char* env = std::getenv("KV_LOG_LEVEL");
    if (env && *env != '\0') {
        // Relaxed store is sufficient: no dependent data.
        g_log_level.store(parse_level(env), std::memory_order_relaxed);
    }
}

void set_level(LogLevel level) {
    // Relaxed store is sufficient: log level is a standalone flag.
    g_log_level.store(level, std::memory_order_relaxed);
}

} 
