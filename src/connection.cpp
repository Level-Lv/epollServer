// connection.cpp — Connection / FileSession 骨架实现（架构 §16 步骤 4A）
// 本步骤只有构造/析构与 FileSession::cleanup 具备真实行为；
// finalize / update_interest / send_frame / out_empty 为占位，步骤 5 按
// 架构 §6.5 / §7.4 落地。

#include "connection.h"

#include <utility>

// ---------- Connection ----------

Connection::Connection(int fd, std::string peer_ip, uint16_t peer_port)
    : fd(fd),
      peer_ip(std::move(peer_ip)),
      peer_port(peer_port) {}

Connection::~Connection() {
    // 兜底：正常路径下 finalize()（仅 IO 线程，步骤 5）已 close(fd) 并把 fd 置 -1；
    // 这里覆盖未走 finalize 即销毁的情况，避免 fd 泄漏。
    if (fd >= 0) {
        ::close(fd);
    }
    // 不 unlink：临时文件的卸载由 FileSession::cleanup() 负责。
}

void Connection::finalize() {
    // TODO(step 5): 架构 §7.4 —— conns_mu_ 下幂等（finalized 标志）、EPOLL_CTL_DEL、
    // conns_.erase、session 清理、close(fd) 并置 fd = -1（见析构兜底注释）。
}

void Connection::update_interest() {
    // TODO(step 5): 架构 §6.1 —— 在 send_mu 下由水位/peer_eof/closing/out_buf 计算
    // 新掩码，与 epoll_events 不同才 EPOLL_CTL_MOD。
}

void Connection::send_frame(std::string_view /*bytes*/) {
    // TODO(step 5): 架构 §6.5 —— conns_mu_ 下核对存活 → send_mu 下追加 out_buf、
    // 直发到 EAGAIN、更新兴趣位。
}

bool Connection::out_empty() const {
    // TODO(step 5): 语义待定（sent_off == out_buf.size()），先返回 true 让骨架可编译。
    return true;
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
