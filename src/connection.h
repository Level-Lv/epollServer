// connection.h — Connection / FileSession / MsgTask 结构定义（骨架，步骤 4A）
// 依据：架构文档 §7.1（字段清单与所有权）、§5.1（调度状态）、§6.4（解析状态机）；
//       PRD §6（会话状态机）。方法实现见架构 §6.5 / §7.4（步骤 5 落地）。
//
// 并发所有权约定（架构 §4.2 / §7.1）：每个字段组的访问规则标注在字段上方，
// 实现时不得偏离。允许的锁序（只能从左到右）：
//   conns_mu_ → pool.mu_
//   conns_mu_ → state_mu → send_mu
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unistd.h>
#include <vector>

#include "common.h"
#include "md5.h"

// ---------- 状态枚举 ----------

// 帧解析状态机（架构 §6.4）：IO 线程独占字段 parse_state 的取值
enum class ParseState : uint8_t { NEED_HEADER, NEED_BODY };

// 会话状态机（架构 §7.1 / PRD §6）：CONNECTED ↔ RECEIVING
enum class SessionState : uint8_t { CONNECTED, RECEIVING };

// 调度状态（架构 §5.1），受 pool.mu_ 保护
enum class SchedState : uint8_t { IDLE, QUEUED, RUNNING };

// ---------- 完整帧任务（架构 §5.1） ----------
// IO 线程切出完整帧后从 in_buf 拷贝一次（架构 §6.4，决策 D12），投递到每连接 FIFO。
struct MsgTask {
    uint8_t type = 0;  // MsgType 值（common.h）
    std::string body;
};

// ---------- FileSession（架构 §7.1） ----------
// 单次文件接收会话（META → CHUNK×N → END）。
// 访问规则：task_running == true 时被"正在执行任务的 worker"独占，IO 线程绝不触碰；
// task_running == false 时仅 IO 线程的 finalize 路径可清理（架构 §7.1 / §7.4）。
// 因此会话内部的文件操作（write/fsync/close/rename）无需额外加锁。
class FileSession {
public:
    FileSession() = default;
    ~FileSession();  // 调用 cleanup()

    FileSession(const FileSession&) = delete;
    FileSession& operator=(const FileSession&) = delete;
    FileSession(FileSession&&) = delete;
    FileSession& operator=(FileSession&&) = delete;

    // close(fd) + unlink(tmp_path) + 字段重置；幂等（finalize 与析构可能先后调用）。
    void cleanup();

    bool active = false;         // 是否有进行中的会话
    std::string filename;        // 已清洗的文件名（PRD §7.3）
    std::string tmp_path;        // <final>.part.<8位hex>（PRD §7.2）
    std::string final_path;      // <storage>/<ip>/<name>
    int fd = -1;                 // 临时文件 fd（架构 §7.4 伪代码中写作 tmp_fd）
    uint64_t expected_size = 0;  // FILE_META 声明的总字节数
    uint64_t received = 0;       // 已写入字节数
    std::string expected_md5hex; // 客户端声明的 32 字符小写 hex（原样保存，决策 D10）
    MD5 md5;                     // 增量 MD5（md5.h）
};

// ---------- Connection（架构 §7.1） ----------
class Connection {
public:
    Connection(int fd, std::string peer_ip, uint16_t peer_port);
    ~Connection();  // 仅兜底 close(fd)；不 unlink（临时文件属 FileSession）

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    Connection(Connection&&) = delete;
    Connection& operator=(Connection&&) = delete;

    // ---------- 方法（步骤 5 实现） ----------
    void finalize();                          // 仅 IO 线程；幂等（架构 §7.4）
    void update_interest();                   // 需持 send_mu；计算并 MOD epoll 掩码（架构 §6.1）
    void send_frame(std::string_view bytes);  // worker 与 IO 均可（架构 §6.5）
    bool out_empty() const;                   // 需持 send_mu；≡ out_buf.empty()（架构 §6.5 语义 A）

    // ---------- 字段 ----------

    // 1) 不可变（构造后身份不再变化）
    int fd;
    std::string peer_ip;  // 点分十进制 IPv4
    uint16_t peer_port;   // 主机字节序

    // 2) IO 线程独占（无锁）
    std::vector<char> in_buf;              // 接收缓冲 + consumed 偏移（架构 §6.4）
    std::size_t consumed = 0;
    ParseState parse_state = ParseState::NEED_HEADER;
    uint32_t pending_body_len = 0;         // NEED_BODY 时的目标长度
    uint8_t pending_type = 0;              // NEED_BODY 时的帧类型（MsgType 值）
    bool peer_eof = false;                 // RDHUP/读到 0 后置位，不再读

    // 3) 跨线程（受 state_mu）
    mutable std::mutex state_mu;
    SessionState state = SessionState::CONNECTED;
    FileSession session;                   // 访问规则见上方 FileSession 注释
    bool task_running = false;             // 是否有 worker 任务正在执行
    bool disconnect_requested = false;     // IO 已判定断开但任务在跑，待移交收尾

    // 4) 跨线程（atomic；任何 fd 访问前必须先查 closed）
    std::atomic<bool> closed{false};       // 逻辑死亡
    std::atomic<bool> closing{false};      // 冲刷 out_buf 后关闭

    // 5) 受 pool.mu_ 保护（thread_pool.h 的调度器，见架构 §5）
    SchedState sched_state = SchedState::IDLE;
    std::deque<MsgTask> tasks;

    // 6) 受 send_mu 保护（发送路径 + epoll 兴趣位）
    mutable std::mutex send_mu;
    std::string out_buf;                   // 仅未发送字节；已发部分即时 erase（架构 §6.5 语义 A）
    bool paused_in = false;                // 高水位摘 EPOLLIN 标志（架构 §6.1）
    uint32_t epoll_events = 0;             // 当前已注册掩码

    // 7) 受 conns_mu_ 保护（server.h 的连接表锁；finalize 幂等）
    bool finalized = false;
};
