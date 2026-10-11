// connection.cpp — Connection / FileSession 实现（架构 §16 步骤 4A + 5）
// 步骤 5（路线 C）：实现三个 *_locked 原语（架构 §6.1 公式 / §6.5 语义 A），
// 均假定调用方已持 send_mu；finalize / send_frame / update_interest 归
// server 层，步骤 6 在 server.cpp 落地。

#include "connection.h"

#include <cerrno>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <utility>

// ---------- Connection ----------

Connection::Connection(int fd, std::string peer_ip, uint16_t peer_port)
    : fd(fd),
      peer_ip(std::move(peer_ip)),
      peer_port(peer_port) {}

Connection::~Connection() {
    // 兜底：正常路径下 finalize()（server 层，仅 IO 线程，步骤 6）已 close(fd)
    // 并把 fd 置 -1；这里覆盖未走 finalize 即销毁的情况，避免 fd 泄漏。
    if (fd >= 0) {
        ::close(fd);
    }
    // 不 unlink：临时文件的卸载由 FileSession::cleanup() 负责。
}

// ---------- 三个 *_locked 原语（步骤 5，路线 C） ----------
// 前置条件（架构 §4.2 / §6.5）：调用方已持 send_mu。本层不做存活核对、
// 不调 epoll、不标记断开——那些属 server 层（步骤 6）。
// finalize / send_frame / update_interest 的实现见 server.cpp（步骤 6）。

bool Connection::out_empty_locked() const {
    return out_buf.empty();
}

uint32_t Connection::compute_interest_locked() const {
    // 架构 §6.1 公式：EPOLLRDHUP
    //                | EPOLLIN  当 !closing && !paused_in && !peer_eof
    //                | EPOLLOUT 当 out_buf 非空
    uint32_t events = EPOLLRDHUP;
    if (!closing.load() && !paused_in && !peer_eof.load()) {
        events |= EPOLLIN;
    }
    if (!out_buf.empty()) {
        events |= EPOLLOUT;
    }
    return events;
}

bool Connection::append_and_send_locked(std::string_view bytes) {
    // 语义 A（架构 §6.5）：out_buf 只保存未发送字节；发送游标为局部变量，
    // 每轮 send 成功后把已发前缀从头 erase（响应帧 ≤ 数百字节，memmove 可忽略）。
    out_buf.append(bytes);
    std::size_t sent_total = 0;
    bool hard_error = false;
    while (sent_total < out_buf.size()) {
        const ssize_t n = ::send(fd, out_buf.data() + sent_total,
                                 out_buf.size() - sent_total, MSG_NOSIGNAL);
        if (n > 0) {
            sent_total += static_cast<std::size_t>(n);  // 继续排空
            continue;
        }
        if (n < 0 && errno == EINTR) continue;                          // 重试
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;  // 内核缓冲满
        hard_error = true;                                              // EPIPE/ECONNRESET 等
        break;
    }
    if (sent_total > 0) {
        out_buf.erase(0, sent_total);  // 已发即弃
    }
    // 水位滞环（架构 §6.5，2026-10-12 定）：out_buf 尺寸变化的唯一落点；
    // 摘/复挂 EPOLLIN 由 server 侧 apply_interest_locked 依据新 paused_in 算掩码落地。
    if (out_buf.size() >= OUT_HIGH_WATER) {
        paused_in = true;
    } else if (paused_in && out_buf.size() <= OUT_LOW_WATER) {
        paused_in = false;
    }
    return !hard_error;
}

// ---------- FileSession ----------

FileSession::~FileSession() {
    cleanup();
}

void FileSession::cleanup() {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
    if (!tmp_path.empty()) {
        ::unlink(tmp_path.c_str());
        tmp_path.clear();
    }
    // 重置
    active = false;
    filename.clear();
    final_path.clear();
    expected_size = 0;
    received = 0;
    expected_md5hex.clear();
    md5.reset();
}
