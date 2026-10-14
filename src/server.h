// server.h — Server：监听/epoll/eventfd/signalfd 基础设施与事件循环（6A + 6B + 6C + 6D）
// 依据：架构文档 §6.1（epoll 集合与兴趣位）、§6.2（事件循环骨架）、§6.3（accept）、
//       §6.4（接收与解析）、§6.5（发送路径）、§6.6（唤醒机制）、§7.4（finalize 与
//       延迟收尾移交）、§7.5（四类关闭路径）、§11.1（启动序列）、§11.2（优雅退出）、
//       §15-D2（LT）、§15-D3（退出宽限 3 秒）、§15-D4（IO 线程独占 ADD/DEL）、
//       §15-D5（两个内部 fd）、§15-D11（--threads 回退 4）、§15-D12（body 拷贝一次）、
//       §15-D14（EMFILE 退让）；
//       PRD §10.1（CLI 默认值）、§10.2（退出码 2）、§11（日志点）、§12（生命周期）。
//
// 已落地：
//   - 6A：构造（socket/bind/listen + epoll + eventfd + signalfd + mkdir + 线程池）、
//         run() 主循环骨架、eventfd/signalfd 排空、退出统计。
//   - 6B：连接事件 happy path —— on_accept 建 Connection 入表、on_readable recv 排空 +
//         parse_loop 两阶段切帧（D12 拷贝）+ submit。
//   - 6C1：发送路径 —— send_frame / update_interest / apply_interest_locked。
//   - 6C2：收尾路径 —— finalize（幂等）/ close_connection（defer 移交）/ on_writable /
//         drain_eventfd 消费 pending_finalize_。
//   - 6D：优雅退出 —— shutting_down_/grace_deadline_ 两个字段、handle_signal 软停、
//         run() 的宽限 timeout、on_grace_timeout 强关残余连接 + join 线程池。
// 未落地（TODO 标注）：静态校验与错误帧 send_error（第 8 步）、worker 侧 dispatch（第 7 步）。
#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "connection.h"   // Connection / FileSession（conns_ 的表元素）
#include "thread_pool.h"  // ThreadPool（构造成员，架构 §5）

// 连接表元素类型。shared_ptr 是"延迟收尾移交"的前提（架构 §7.4）：IO 线程把
// 连接从表中摘除后，对象仍由正在跑任务的 worker 保活。
// 暂放 server.h；第 7 步若 execute() 也需要该别名，再议是否移入 connection.h。
using ConnectionPtr = std::shared_ptr<Connection>;

// ---------- 服务端配置（临时定义：第 9 步 config.cpp 时抽到 config.h/.cpp） ----------
// 默认值取 PRD §10.1。n_threads 的"默认"由 main 解析 --threads 时决定
// （hardware_concurrency()，返回 0 时回退 4，架构 §15-D11），成员默认值 4 即该回退值。
struct ServerConfig {
    uint16_t    port           = 8888;
    std::string storage_dir    = "./storage";
    std::size_t n_threads      = 4;
    std::size_t queue_cap      = 1024;
    std::size_t conn_queue_cap = 64;
    int         backlog        = 512;
};

class Server {
public:
    // 构造即完成全部启动动作（架构 §11.1 步骤 2–6）。任一步失败抛
    // std::runtime_error（消息含 strerror），由 main 转成退出码 2（PRD §10.2）。
    explicit Server(const ServerConfig& cfg);

    // 幂等顺序：先 pool_.shutdown()（join 全部 worker，其间 wake_io_cb_ 仍可能写
    // eventfd_），再关闭本类持有的 fd；连接表随成员销毁（~Connection 兜底 close）。
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;
    Server(Server&&) = delete;
    Server& operator=(Server&&) = delete;

    // epoll 主循环。timeout 恒为 -1，直到进入优雅退出（shutting_down_）后改用
    // "距 grace_deadline_ 的剩余毫秒"（架构 §11.2 步骤 2），到期由 on_grace_timeout()
    // 强关残余连接。循环在 running_ 被置 false 时结束（第二次信号 / request_stop() /
    // 宽限到期），返回前 join 线程池并打印退出统计行（PRD §11 / §12）。
    void run();

    // 【硬停】立即退出主循环，不走优雅退出宽限：置 running_ = false 并写 eventfd
    // 唤醒阻塞中的 epoll_wait（架构 §6.6）。信号路径走的是软停（handle_signal 对
    // 第一次信号的处理 → shutting_down_），本函数留给测试 / 强制停止使用。
    // 幂等，可从任意线程调用。
    void request_stop();

    // ---------- 字段 ----------

    // 本类持有的 4 个 fd：-1 表示未创建 / 已关闭（仅构造失败路径与析构会关）
    int epfd_;        // epoll 实例（EPOLL_CLOEXEC）
    int listen_fd_;   // 监听套接字（非阻塞 + CLOEXEC，架构 §11.1 步骤 4）
    int eventfd_;     // worker→IO 唤醒通道（EFD_NONBLOCK|EFD_CLOEXEC，架构 §6.6）
    int signalfd_;    // SIGINT/SIGTERM（SFD_NONBLOCK|SFD_CLOEXEC，本步只建不处理）

    uint16_t    port_;         // 实际监听端口（回显 / 日志用）
    std::string storage_dir_;  // 存储根目录（构造时 mkdir）

    // 声明顺序即初始化顺序：fd 与配置先就位，pool_ 最后启动 worker——
    // 线程一起跑就能看到已完成初始化的 eventfd_（wake_io_cb_ 依赖它）。
    ThreadPool pool_;

    // 连接表：fd → Connection。IO 线程是唯一的 ADD/DEL 方（架构 §15-D4），
    // 所有访问都在 conns_mu_ 下（send_frame/update_interest 也自取本锁）。
    std::mutex conns_mu_;
    std::unordered_map<int, ConnectionPtr> conns_;

    std::atomic<bool> running_{true};  // 主循环开关（信号路径 / request_stop）

    // 优雅退出状态（架构 §11.2 步骤 1–2 / §15-D3）。与 running_ 是**两件事**：
    //   running_ = false  → 完全退出主循环；
    //   shutting_down_    → 只是停止 accept（listen_fd 已摘出 epoll），已建立连接
    //                       照常收/发/处理，主循环改用宽限 timeout。
    // 写者唯一 = IO 线程（handle_signal 在事件分发里就地执行，run() 只读），故
    // grace_deadline_ 无需额外同步；shutting_down_ 用 atomic 是为了让"是否已进入
    // 宽限"能被任意线程安全观察（on_accept 的漏网事件判定也读它）。
    std::atomic<bool> shutting_down_{false};
    std::chrono::steady_clock::time_point grace_deadline_{};  // 宽限截止时刻

    // 统计（PRD §11/§12）：accept 计 total_conns；FILE_END 校验结果计 files_ok/fail。
    struct Stats {
        std::atomic<uint64_t> total_conns{0};
        std::atomic<uint64_t> files_ok{0};
        std::atomic<uint64_t> files_fail{0};
    } stats_;

private:
    // ---------- 事件处理 ----------
    void on_accept();                          // 6A/6B 已实现：accept4 + 建 Connection 入表
    void on_readable(const ConnectionPtr&);    // 6B 已实现：recv 排空 + parse_loop
    void on_writable(const ConnectionPtr&);    // 6C2 已实现：冲刷 out_buf + 冲刷空后收尾
    void parse_loop(const ConnectionPtr&);     // 6B 已实现：两阶段帧状态机（架构 §6.4）

    // ---------- 收尾与发送（归 server 层，架构 §6.5 / §7.4） ----------
    // 6C2 已实现：IO 侧关闭入口 —— 置 closed + 摘 epoll + defer 判定（有任务在跑则
    //             移交 worker，否则就地 finalize）。
    void close_connection(const ConnectionPtr&);
    // 6C1 已实现：conns_mu_ → send_mu → append_and_send_locked → apply_interest_locked；
    // true = 无硬错误（硬错误只返回 false，标记由调用方在锁外完成）。
    bool send_frame(const ConnectionPtr&, std::string_view);
    // 6C1 已实现：自取 conns_mu_ + send_mu，核对存活后调 apply_interest_locked。
    void update_interest(const ConnectionPtr&);
    // 6C1 已实现：假定 conns_mu_ + send_mu 已持（供 send_frame / update_interest 复用）。
    void apply_interest_locked(const ConnectionPtr&);
    // 6C2 已实现：幂等收尾（架构 §7.4）—— 仅 IO 线程调用；全程持 conns_mu_，
    //             DEL + 摘表 + session 清理 + 清扫排队任务 + close(fd)。
    void finalize(const ConnectionPtr&);

    // ---------- 内部 fd 与退出 ----------
    // 6D 已实现：排空 signalfd。第一次信号 → 软停（置 shutting_down_ + 摘 listen_fd
    // + 记宽限截止，架构 §11.2 步骤 1–2）；已在宽限中又收到信号 → 硬停（running_ = false）。
    void handle_signal();
    void drain_eventfd();    // 6C2 已实现：排空 eventfd + 消费 pending_finalize_
    // 6D 已实现：宽限到期 —— 强关残余连接 + pool_.shutdown() + 消费移交队列
    // （架构 §11.2 步骤 3–4）。仅 IO 线程调用（由 run() 的 timeout 分支就地执行）。
    void on_grace_timeout();
};
