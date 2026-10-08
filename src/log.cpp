// log.cpp — 分级日志实现（stderr 行缓冲）
// 依据：PRD §11（格式/级别/关键日志点）；架构文档 §10、§16 第 3 步。
// 行格式（PRD §11，严格）：
//   [YYYY-MM-DD HH:MM:SS.mmm][LEVEL][fd=12 ip=192.168.1.10] message\n
//   - LEVEL 恒为 5 字符（WARN/INFO 右补一个空格），各列对齐
//   - 全局消息（无连接上下文）省略整个 [fd=… ip=…] 段
//   - 时间戳：CLOCK_REALTIME → localtime_r，毫秒 3 位补零
// 线程模型：单把 std::mutex 覆盖"组装整行 + 单次输出"，多线程日志不会交错；
//           级别过滤已在 log.h 的宏里完成，进到这里的一定是要输出的。
#include "log.h"

#include <atomic>
#include <cstdarg>
#include <cstddef>
#include <cstdio>
#include <ctime>
#include <mutex>

namespace {

// ---- 行缓冲：固定 1024 字节栈缓冲，末字节恒留给 '\n' ----
// 内容上限 = kLogBufSize - 1 = 1023 字节；超长消息按"保留前 1023 字节"截断。
// 设计决策：宁可截断也不动态分配——日志路径不引入分配失败/异常与不可控延迟；
// 本服务的单行日志远小于该上限，截断仅作防御。
constexpr std::size_t kLogBufSize = 1024;

std::mutex g_mu;                                                  // 保护"组装 + 输出"
std::atomic<int> g_min_level{static_cast<int>(LogLevel::INFO)};   // 全局最低输出级别

// 级别标签：恒为 5 字符（WARN/INFO 右补空格），保证各行列对齐
const char* level_tag(LogLevel lv) {
    switch (lv) {
        case LogLevel::ERROR: return "ERROR";
        case LogLevel::WARN:  return "WARN ";
        case LogLevel::INFO:  return "INFO ";
        case LogLevel::DEBUG: return "DEBUG";
    }
    return "?????";
}

// 把 fmt/ap 的结果追加到 buf[off..]；off 为当前内容字节数。
// 内容上限 kLogBufSize-1，写满即截断（off 停在 cap），不做动态分配。
void append_vfmt(char* buf, std::size_t& off, const char* fmt, va_list ap) {
    constexpr std::size_t kCap = kLogBufSize - 1;
    if (off >= kCap) return;
    const std::size_t room = kCap - off;                         // 还可写入的内容字节数
    const int n = std::vsnprintf(buf + off, room + 1, fmt, ap);  // +1 供结尾 '\0'
    if (n <= 0) return;                                          // 0=空串；<0=编码错误
    const std::size_t written = static_cast<std::size_t>(n);
    off += (written < room) ? written : room;                    // 超长：截断停在 cap
}

void append_fmt(char* buf, std::size_t& off, const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    append_vfmt(buf, off, fmt, ap);
    va_end(ap);
}

void log_v(LogLevel lv, bool with_conn, int fd, const char* peer_ip,
           const char* fmt, va_list ap) {
    char buf[kLogBufSize];
    std::size_t off = 0;

    std::lock_guard<std::mutex> lk(g_mu);   // 组装 + 输出全程互斥

    timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    tm ltm{};
    localtime_r(&ts.tv_sec, &ltm);
    append_fmt(buf, off, "[%04d-%02d-%02d %02d:%02d:%02d.%03ld]",
               ltm.tm_year + 1900, ltm.tm_mon + 1, ltm.tm_mday,
               ltm.tm_hour, ltm.tm_min, ltm.tm_sec,
               static_cast<long>(ts.tv_nsec / 1000000L));       // 毫秒 3 位
    append_fmt(buf, off, "[%s]", level_tag(lv));
    if (with_conn) {
        append_fmt(buf, off, "[fd=%d ip=%s]", fd, peer_ip ? peer_ip : "?");
    }
    append_fmt(buf, off, " ");
    append_vfmt(buf, off, fmt, ap);

    buf[off++] = '\n';                      // off ≤ 1023，写下标 ≤1023，安全
    std::fwrite(buf, 1, off, stderr);       // 单次写整行；出错（stderr 关闭等）静默忽略
}

}  // namespace

void set_log_level(LogLevel lv) noexcept {
    g_min_level.store(static_cast<int>(lv), std::memory_order_relaxed);
}

LogLevel get_log_level() noexcept {
    return static_cast<LogLevel>(g_min_level.load(std::memory_order_relaxed));
}

void log_impl(LogLevel lv, const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    log_v(lv, /*with_conn=*/false, -1, nullptr, fmt, ap);
    va_end(ap);
}

void log_impl_conn(LogLevel lv, int fd, const char* peer_ip,
                   const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    log_v(lv, /*with_conn=*/true, fd, peer_ip, fmt, ap);
    va_end(ap);
}
