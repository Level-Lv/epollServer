// thread_pool.cpp — 调度器实现（架构 §5.3 submit / §5.4 worker_loop / §5.5 不变式与退出）
// 本文件严格对照架构 §5 的伪代码落地，差异仅有两处注释已标明的实现细节：
//   1) 收尾三步各用一个作用域块，保证 state_mu 与 mu_ 不同时持有（架构 §4.2 锁序）；
//   2) 执行入口的 closed 检查与 task_running 置位在同一临界区内完成（架构 §8 execute）。

#include "thread_pool.h"

// --------------------------------------------------------------------------
// 任务执行入口（步骤 4B 占位，仅用于编译验证）
// --------------------------------------------------------------------------
// 占位只做一件事：按架构 §8 在 state_mu 下置 task_running（收尾清除在 worker_loop）。
// 不 requeue：tasks 受 pool.mu_ 保护，本函数在 state_mu 下，不得再去拿 pool.mu_
// （架构 §4.2 锁序：state_mu 与 pool.mu_ 不得同时持有），故任务被取出后即丢弃。
// 后果：§5.5 不变式（queued_total_ == Σ|conn->tasks|）在步骤 7 落地前不成立，
// 本占位版本禁止实际运行（含测试）；仅作编译验证。
void execute(const std::shared_ptr<Connection>& c, const MsgTask& t) {
    std::lock_guard<std::mutex> g(c->state_mu);
    if (c->closed) return;  // I1：入队后、执行前连接已死 → 不碰 session
    c->task_running = true;
    // TODO(step 7): 按架构 §8.2/§8.3/§8.4/§8.5 分发：
    //   switch (static_cast<MsgType>(t.type)) → handle_meta/chunk/end/heartbeat(c, t.body);
    (void)t;  // 占位不读任务体
}

// --------------------------------------------------------------------------
// ThreadPool
// --------------------------------------------------------------------------

ThreadPool::ThreadPool(std::size_t n_threads, std::size_t queue_cap,
                       std::size_t conn_queue_cap, std::function<void()> wake_io_cb)
    : wake_io_cb_(std::move(wake_io_cb)),
      queue_cap_(queue_cap),
      conn_queue_cap_(conn_queue_cap) {
    workers_.reserve(n_threads);
    for (std::size_t i = 0; i < n_threads; ++i) {
        workers_.emplace_back([this] { worker_loop(); });
    }
}

ThreadPool::~ThreadPool() {
    shutdown();  // 幂等；兜底未显式 shutdown 的路径
}

void ThreadPool::submit(const std::shared_ptr<Connection>& c, MsgTask t) {
    std::unique_lock<std::mutex> lk(mu_);
    cv_space_.wait(lk, [&] {
        return shutdown_ ||
               (queued_total_ < queue_cap_ && c->tasks.size() < conn_queue_cap_);
    });
    if (shutdown_) return;  // 退出期丢弃新任务（连接随即被强关，架构 §5.3）
    c->tasks.push_back(std::move(t));
    ++queued_total_;
    if (c->sched_state == SchedState::IDLE) {
        c->sched_state = SchedState::QUEUED;
        ready_.push_back(c);
        cv_work_.notify_one();
    }
    // 若 RUNNING/QUEUED：任务已在队列，runner 收尾或既有 entry 会接手（架构 §5.2）
}

void ThreadPool::shutdown() {
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (shutdown_) return;  // 幂等
        shutdown_ = true;
    }
    cv_work_.notify_all();
    cv_space_.notify_all();  // 可能仍有线程阻塞在 submit() 的 cv_space_ 上
    for (auto& w : workers_) {
        if (w.joinable()) w.join();
    }
}

bool ThreadPool::is_shutdown() const {
    std::lock_guard<std::mutex> lk(mu_);
    return shutdown_;
}

void ThreadPool::take_pending(std::vector<std::shared_ptr<Connection>>& out) {
    std::lock_guard<std::mutex> lk(pend_mu_);
    out.clear();
    out.swap(pending_finalize_);  // 取走副本，挂起列表清空（架构 §4.2：取出后立即释放）
}

void ThreadPool::drain_conn_tasks(const std::shared_ptr<Connection>& c) {
    // 与 worker_loop 收尾里的"死连接批量清扫"分支同源（那里不能调用本函数：
    // 它已持 mu_，再取即自锁）。两处都维持 §5.5 的不变式
    // queued_total_ == Σ|conn->tasks|，且都必须 cv_space_.notify_all()——
    // 可能正有线程阻塞在 submit() 上等这个额度。
    std::lock_guard<std::mutex> lk(mu_);
    if (c->tasks.empty()) return;
    queued_total_ -= c->tasks.size();
    c->tasks.clear();
    c->sched_state = SchedState::IDLE;
    cv_space_.notify_all();
}

void ThreadPool::worker_loop() {
    for (;;) {
        std::shared_ptr<Connection> c;
        MsgTask t;
        {
            std::unique_lock<std::mutex> lk(mu_);
            cv_work_.wait(lk, [&] { return shutdown_ || !ready_.empty(); });
            if (ready_.empty()) return;  // shutdown 且无待处理 → 本 worker 退出
            c = ready_.front();
            ready_.pop_front();
            if (c->tasks.empty()) {  // 竞态：已被清扫（架构 §7.6）
                c->sched_state = SchedState::IDLE;
                continue;
            }
            t = std::move(c->tasks.front());
            c->tasks.pop_front();
            --queued_total_;
            c->sched_state = SchedState::RUNNING;
            cv_space_.notify_all();  // 唤醒可能阻塞的 IO 线程
        }

        execute(c, t);  // 锁外执行；入口先查 closed，再置 task_running（架构 §8）

        // ---- 收尾三步：三个作用域互不嵌套，满足 §4.2 锁序 ----
        bool disc;
        {
            std::lock_guard<std::mutex> g(c->state_mu);
            c->task_running = false;
            disc = c->disconnect_requested;
        }
        {
            std::lock_guard<std::mutex> lk(mu_);
            // ⚠️ 步骤 7 前（execute() 为占位）：任务已被丢弃，tasks 里不可能有残留，
            //    下面的"死连接批量清扫"分支在占位期间恒不触发——其正确性要等真实
            //    dispatch 落地、任务真会积压时才能验证。
            if (c->closed) {  // 死连接：批量清扫剩余任务，释放额度（架构 §5.4）
                queued_total_ -= c->tasks.size();
                c->tasks.clear();
                c->sched_state = SchedState::IDLE;
                cv_space_.notify_all();
            } else if (!c->tasks.empty()) {
                c->sched_state = SchedState::QUEUED;
                ready_.push_back(c);
                cv_work_.notify_one();
            } else {
                c->sched_state = SchedState::IDLE;
            }
        }
        if (disc) {  // 延迟收尾移交回 IO 线程（架构 §7.4）
            {
                std::lock_guard<std::mutex> g(pend_mu_);
                pending_finalize_.push_back(c);
            }
            if (wake_io_cb_) wake_io_cb_();  // eventfd write(1)；不持任何锁调用
        }
    }
}
