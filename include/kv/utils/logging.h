#pragma once

#include <atomic>
#include <iostream>
#include <string_view>

namespace kv::log {

    enum class LogLevel {
        None,
        Info,
        Debug
    };

    // Global log level read by macros; atomic to avoid data races.
    extern std::atomic<LogLevel> g_log_level;

    // Accepts: none/off/0, info/1, debug/2 (case-insensitive).
    LogLevel parse_level(std::string_view value);
    
    // Initializes g_log_level from KV_LOG_LEVEL if set.
    void init_from_env();
    void set_level(LogLevel level);

}  

#define LOG_DEBUG(msg) \
    do { \
        if (kv::log::g_log_level.load(std::memory_order_relaxed) == kv::log::LogLevel::Debug) { \
            std::cout << msg << "\n"; \
        } \
    } while (0)

#define LOG_INFO(msg) \
    do { \
        if (kv::log::g_log_level.load(std::memory_order_relaxed) != kv::log::LogLevel::None) { \
            std::cout << msg << "\n"; \
        } \
    } while (0)
