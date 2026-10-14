// thread_pool.h — 调度器：每连接 FIFO + 全局任务计数 + 就绪连接队列
// 依据：架构文档 §5（调度器设计，落决策 D1）、§7.4（延迟收尾移交）；
//       PRD §4.1（线程模型）、§8.2（背压：满则阻塞 IO 线程，不丢弃）。
//
// 调度模型一句话：每个连接至多一个任务处于 RUNNING（同连接严格串行），
// worker 每次只取 1 条任务（不同连接交错执行，公平轮转）。
//
// 两个上限（决策 D1，见 submit()）：全局排队任务总数 ≤ queue_cap_，
// 每连接排队任务数 ≤ conn_queue_cap_；任一触顶 → **阻塞调用者**（即 IO 线程）。
// 阻塞不会死锁：不变式 queued_total_ == Σ|conn->tasks| 由"worker 取出"与
// "死连接批量清扫"两条消费路径共同维护，二者都会归还额度并 cv_space_.notify_all()
// （架构 §5.5）。
//
// 本模块不依赖 epoll / eventfd / server（wake_io_cb_ 回调注入，见下），
// 亦不依赖 log（热路径零日志）。
#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "connection.h"

class ThreadPool {
public:
    // n_threads：worker 数（--threads，回退 4 见架构 §15-D11，由 main 决定后传入）。
    // queue_cap：全局排队任务总数上限（--queue-cap，默认 1024）。
    // conn_queue_cap：每连接排队任务数上限（--conn-queue-cap，默认 64）。
    // wake_io_cb：worker→IO 的唤醒通道，每次向 pending_finalize_ 追加后调用一次。
    // execute_cb：任务执行入口（架构 §8 的 execute/dispatch），由 server 侧实现
    //             （步骤 7：Server::execute → dispatch → handle_*）。
    //
    // 两个回调的注入方式同源（决策）：**注入 std::function，不把 eventfd / Server
    // 类型塞进本模块**。理由：eventfd 属 server 层资源（架构 §6.6 / §15-D5），而
    // 消息处理要用到 send_frame / storage_dir_ / stats_（同样是 server 层的东西）。
    // ThreadPool 注入回调后与"怎么唤醒 epoll_wait""任务怎么执行"彻底解耦——本模块
    // 只负责调度（取任务、串行化、收尾移交），测试可用空回调；若注入裸 fd 或让
    // ThreadPool 认识 Server，调度器就被迫知道协议细节与存储布局。
    // 两个回调都只被 worker 调用、都在**不持任何锁**时调用（调用点见 .cpp），
    // 故不会引入锁序问题。
    ThreadPool(std::size_t n_threads, std::size_t queue_cap,
               std::size_t conn_queue_cap, std::function<void()> wake_io_cb,
               std::function<void(const std::shared_ptr<Connection>&, const MsgTask&)>
                   execute_cb);

    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // IO 线程调用（架构 §5.3）。队列满时阻塞当前线程（语义见文件头）。
    // 连接已死（closed）时仍照常入队：执行侧入口会丢弃，额度由清扫路径归还
    // （架构 §7.6 "submit() 期间连接死亡"一行）。
    void submit(const std::shared_ptr<Connection>& c, MsgTask t);

    // 幂等。置 shutdown_ 后 join 全部 worker。
    // 调用前提（由 server 保证，架构 §11.2）：所有连接已被强关（closed==true），
    // 死连接任务走 §5.4 清扫路径快速消化，join 不会久等。
    void shutdown();

    bool is_shutdown() const;

    // ---------- pending_finalize_（架构 §7.4 移交队列、§5 决策 (a)）----------
    // 归属：架构 §4.2/§5.5 把 pend_mu_ 列为全局锁之一、pending_finalize_ 由调度器
    // 出入，故放在 ThreadPool 内（而非 server），server 用 take_pending() 取副本。
    // 锁序：pend_mu_ 不参与任何嵌套——worker 只在**不持其它锁**时 push（见
    // worker_loop 收尾三步），IO 取出副本后立即释放（架构 §4.2 "IO 取出列表副本后
    // 立即释放"），故与 conns_mu_ → pool.mu_ 的顺序约束不构成环。
    void take_pending(std::vector<std::shared_ptr<Connection>>& out);

    // 清扫该连接**仍在排队**的任务并归还额度（架构 §5.4"死连接批量清扫"的同一段
    // 逻辑，只是由 IO 侧 finalize 驱动）。必要性：连接在任务被执行前就死了时，
    // 若不清扫，queued_total_ 会永久偏高，最终把 submit() 的 IO 线程卡死。
    //
    // 调用方不得持 mu_（本方法自取）；允许此时连接不在 ready_ 里，也允许它仍被
    // 某个 worker 持着——不变式 queued_total_ == Σ|conn->tasks| 由 mu_ 串行化保证。
    // 幂等（tasks 空时直接返回）。
    void drain_conn_tasks(const std::shared_ptr<Connection>& c);

private:
    void worker_loop();

    // 声明顺序 = 构造初始化顺序；所有带初值的成员先就位，workers_ 最后启动：
    // 线程一起跑就能看到完全初始化的调度状态。
    // 析构顺序相反：shutdown() 先 join，再销毁其余成员（顺序安全）。
    std::function<void()> wake_io_cb_;
    // 前置（由 Server::execute 自己保证）：进入时在 state_mu 下确认 !closed 并置
    // task_running = true —— 此后 FileSession 被本 worker 独占，IO 线程不会触碰
    // （架构 §7.1）。task_running 的清零由 worker_loop 收尾三步负责，回调内不管。
    std::function<void(const std::shared_ptr<Connection>&, const MsgTask&)> execute_cb_;
    const std::size_t queue_cap_;
    const std::size_t conn_queue_cap_;

    // 受 mu_ 保护（架构 §5.1）
    mutable std::mutex mu_;
    std::condition_variable cv_work_;   // ready_ 非空 / shutdown_ → 唤醒 worker
    std::condition_variable cv_space_;  // 额度归还 → 唤醒阻塞在 submit() 的 IO 线程
    std::deque<std::shared_ptr<Connection>> ready_;  // 有任务的连接，每连接至多一条
    std::size_t queued_total_ = 0;      // Σ conn->tasks.size()，不变式见文件头
    bool shutdown_ = false;

    // 受 pend_mu_ 保护
    mutable std::mutex pend_mu_;
    std::vector<std::shared_ptr<Connection>> pending_finalize_;

    // 垫底：析构时 workers_ 最先销毁 → 触发 std::thread 析构 terminate 之前，
    // 必须先由 shutdown() join（见 ~ThreadPool）。
    std::vector<std::thread> workers_;
};
