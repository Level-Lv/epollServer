// log.h — 分级日志（stderr，行缓冲）
// 依据：PRD §11（日志格式/级别/关键日志点）；架构文档 §10、§16 第 3 步。
// 本模块（.h + .cpp）仅依赖标准库：<cstdint> <cstdarg> <cstdio> <ctime> <atomic> <mutex>。
// 被 connection.* / server.* 依赖，不反向依赖它们（纯叶子模块）。
#pragma once

#include <cstdint>

// 日志级别：值越小越严重；低于"全局最低级别"的日志不输出（见 set_log_level）。
enum class LogLevel : int {
    ERROR = 0,
    WARN  = 1,
    INFO  = 2,
    DEBUG = 3,
};

// 全局最低输出级别（对应 CLI --log-level）。线程安全（atomic），默认 INFO。
void set_log_level(LogLevel lv) noexcept;
LogLevel get_log_level() noexcept;

// 全局日志（无连接上下文）：[时间][LEVEL] message\n
// __attribute__((format(printf, …))) 让 g++ 在编译期校验格式串与参数匹配。
void log_impl(LogLevel lv, const char* fmt, ...)
    __attribute__((format(printf, 2, 3)));

// 带连接上下文的日志：[时间][LEVEL][fd=N ip=…] message\n
void log_impl_conn(LogLevel lv, int fd, const char* peer_ip,
                   const char* fmt, ...)
    __attribute__((format(printf, 4, 5)));

// 宏内先做级别过滤（一次原子读）：低于最低级别的调用完全不产生格式化开销。
//
// 写法说明：整串参数（含格式串）直接放进 `...`，而不写成 (fmt, ...) + ##__VA_ARGS__。
// 原因：后者在"无可变参数"的调用（如 LOG_INFO("hello")）下会触发 -Wpedantic 的
// "ISO C++11 requires at least one argument for the '...'" 警告（GCC 13 实测），
// 与"零警告"目标冲突。此写法调用形式完全相同（LOG_INFO("x=%d", x) / LOG_INFO("hello")），
// 且不再依赖 ##__VA_ARGS__ 这个 GNU 逗号扩展；唯一约束是每次调用至少给一个格式串。
// printf 格式校验仍由 log_impl/log_impl_conn 上的 format 属性完成。
#define LOG_ERROR(...) \
    do { if (get_log_level() >= LogLevel::ERROR) \
             log_impl(LogLevel::ERROR, __VA_ARGS__); } while (0)
#define LOG_WARN(...) \
    do { if (get_log_level() >= LogLevel::WARN) \
             log_impl(LogLevel::WARN, __VA_ARGS__); } while (0)
#define LOG_INFO(...) \
    do { if (get_log_level() >= LogLevel::INFO) \
             log_impl(LogLevel::INFO, __VA_ARGS__); } while (0)
#define LOG_DEBUG(...) \
    do { if (get_log_level() >= LogLevel::DEBUG) \
             log_impl(LogLevel::DEBUG, __VA_ARGS__); } while (0)

// 带连接上下文版本（_C = connection-context）
#define LOG_ERROR_C(fd, ip, ...) \
    do { if (get_log_level() >= LogLevel::ERROR) \
             log_impl_conn(LogLevel::ERROR, fd, ip, __VA_ARGS__); } while (0)
#define LOG_WARN_C(fd, ip, ...) \
    do { if (get_log_level() >= LogLevel::WARN) \
             log_impl_conn(LogLevel::WARN, fd, ip, __VA_ARGS__); } while (0)
#define LOG_INFO_C(fd, ip, ...) \
    do { if (get_log_level() >= LogLevel::INFO) \
             log_impl_conn(LogLevel::INFO, fd, ip, __VA_ARGS__); } while (0)
#define LOG_DEBUG_C(fd, ip, ...) \
    do { if (get_log_level() >= LogLevel::DEBUG) \
             log_impl_conn(LogLevel::DEBUG, fd, ip, __VA_ARGS__); } while (0)
