# FSync Server 架构设计文档（MVP）

| 项         | 值                                       |
| :--------- | :--------------------------------------- |
| 产品       | FSync Server 核心网络层                  |
| 版本       | v1.0（对应 PRD v0.1 MVP）                |
| 日期       | 2026-10-07                               |
| 状态       | 待审阅                                   |
| 上游文档   | `PRD.md`                                 |
| 下游产物   | 实现计划（writing-plans）→ 代码          |

## 0. 本文档读法

- PRD 是需求与验收契约；本文档是"把 PRD 落成可实现的架构"：模块划分、数据结构、线程/所有权规则、锁序、关键流程伪代码、错误映射。
- 与 PRD 冲突时以 PRD 为准。本文档做出的所有**推断性决策**集中在 §15，**PRD 附录 A 的 2 处字节数勘误**也在 §15，请重点审阅这两部分。
- 协议格式、验收标准、CLI 参数等不在此重复，只引用 PRD 章节号。

## 1. 范围

一次 epoll 事件循环 + 有界工作线程池；帧级协议（PRD §5）；文件落盘 `<storage>/<ip>/<name>`；除 `HEARTBEAT` 外任何错误即"发 ERROR → 冲刷 → 关连接"。不做项见 PRD §1.2。

## 2. 已确认的三项关键决策（2026-10-07 澄清）

| #   | 决策                                                                                              | 落地章节 |
| :-- | :------------------------------------------------------------------------------------------------ | :------- |
| D1  | `--queue-cap 1024` 约束**全部连接排队任务总数**；任务只存在每连接 FIFO（64）里，两处上限任一满 ⇒ IO 线程 `submit()` 阻塞。串行语义由每连接 FIFO 天然保证。 | §5       |
| D2  | epoll 采用**水平触发 LT**。                                                                        | §6       |
| D3  | 优雅退出采用**宽限 3 秒**：停止 accept 后允许已建立连接继续处理当前文件，最长 3 秒；到期或全部完成后强关剩余连接并清理，再停线程池、打统计。 | §11      |

## 3. 模块划分与文件布局

```
epollServer/
├── Makefile
├── PRD.md
├── docs/…（本文档）
├── src/
│   ├── common.h          # 协议常量（magic/version/type/错误码/上限）、大端读写 inline 函数、Task 无关的纯工具
│   ├── log.h .cpp        # 分级日志（stderr，PRD §11 格式）
│   ├── md5.h .cpp        # 自包含增量 MD5（RFC 1321），服务端与 test_client 共用
│   ├── thread_pool.h .cpp# 调度器：每连接 FIFO + 全局任务计数 + 就绪连接队列（§5）
│   ├── connection.h .cpp # Connection / FileSession / MsgTask / 发送路径 / finalize（§7）
│   ├── server.h .cpp     # epoll 主循环、accept/recv/parse、dispatch 调用、信号、生命周期（§6/§8/§11）
│   └── main.cpp          # CLI 解析、启动装配、退出码
└── tools/
    └── test_client.cpp   # 开发/压测工具（§14），-Isrc 复用 common.h / md5.h
```

依赖方向（无环）：

```
main.cpp → server.{h,cpp} → connection.{h,cpp} → common.h
                  │                └→ log.h, md5.h
                  ├→ thread_pool.{h,cpp}（依赖 connection.h 的 Connection 定义）
                  └→ log.h
test_client.cpp → common.h, md5.h
```

各模块一句话职责：

| 模块            | 职责                                                     | 明确不做                     |
| :-------------- | :------------------------------------------------------- | :--------------------------- |
| `common.h`      | 常量、`be_get16/32/64`、`be_put…`、帧头 struct 定义      | 任何状态                   |
| `log.*`         | `LOG_ERROR/WARN/INFO/DEBUG`，带 conn 上下文的重载        | 日志文件、轮转（PRD §1.2）   |
| `md5.*`         | `Md5{update,finalize,hex}` 增量接口                      | 通用哈希框架                 |
| `thread_pool.*` | 调度、背压、worker 生命周期                              | 定时任务、优先级             |
| `connection.*`  | 连接对象、会话状态机、out_buf 与兴趣位、finalize         | epoll_wait/accept（属 server）|
| `server.*`      | IO 线程全部逻辑、消息解析与分发入口、启动/退出           | 文件 IO（属 worker 任务）    |

## 4. 线程模型与锁序

### 4.1 线程

- **main 线程兼任 IO 线程**（唯一执行 `epoll_wait`）：accept、recv、帧切分与静态校验、`submit()`、发送 out_buf、**fd 的 close / epoll_ctl(DEL) / 连接对象销毁**、信号处理（signalfd 读端）、退出流程。
- **N 个 worker**（默认 `hardware_concurrency()`）：处理完整消息（文件 IO、MD5、rename、构造 ACK/ERROR），把响应写入 conn 的 out_buf 并尝试直接 `send()`。

两条全局所有权规则（比 PRD §4.3 更强，论证见 §7）：

- **R1**：连接 fd 与 tmp fd 的 `close(2)` 只发生在 IO 线程（worker 只在"任务运行中"窗口内用它所属 session 的 tmp fd）——唯一的例外是 worker 在 `FILE_END`/错误路径中按业务语义关闭 tmp fd，此时该 fd 仅本 worker 可达（§7.1 说明）。
- **R2**：`epoll_ctl` 只有两种合法调用：IO 线程直接调用；worker 仅允许 `EPOLL_CTL_MOD`，且必须先在 `conns_mu_` 下确认 conn 仍在连接表中且指针一致。`EPOLL_CTL_ADD/DEL` 与 `close(2)` 仅 IO 线程。

### 4.2 锁清单与全局顺序

| 锁             | 保护对象                                                     | 持有者            | 备注                       |
| :------------- | :----------------------------------------------------------- | :---------------- | :------------------------- |
| `conns_mu_`    | `conns_`（fd→shared_ptr）、连接的"存活"判定、finalize 的互斥 | IO 为主，worker 读 | IO 持有期间可做 epoll_ctl  |
| `pool.mu_`     | 就绪队列、各连接任务队列、`queued_total_`、调度状态          | 两者              | 短临界区，绝不在其内跑任务 |
| `conn.state_mu`| 会话状态、`task_running`、`closed`、`disconnect_requested`、finalize 的 tmp 清理判定 | 两者 | 字段语义见 §7.1            |
| `conn.send_mu` | `out_buf`、`sent_off`、`paused_in`、`epoll_events`           | 两者              | 只在 send_frame / 发送冲刷中使用 |
| `pend_mu_`     | `pending_finalize_` 移交列表                                 | 两者              | IO 取出列表副本后立即释放  |

**允许的嵌套（只能从左到右）**：

```
conns_mu_ → pool.mu_
conns_mu_ → conn.state_mu → conn.send_mu
```

其余任何两把锁不得同时持有（例如 worker 不得在持 `state_mu` 时取 `pool.mu_`，反之亦然——先释放再取）。长操作（`fsync`、`write`、`statvfs`、`rename`）**绝不持锁**。

## 5. 调度器设计（落 D1）

### 5.1 结构

```cpp
enum class SchedState { IDLE, QUEUED, RUNNING };   // 受 pool.mu_ 保护

class ThreadPool {
  std::mutex mu_;
  std::condition_variable cv_work_, cv_space_;
  std::deque<std::shared_ptr<Connection>> ready_;  // 有任务的连接，每连接至多一条
  size_t queued_total_ = 0;                        // Σ conn->tasks.size()，受 mu_ 保护
  size_t queue_cap_;                               // --queue-cap 1024
  size_t conn_queue_cap_;                          // --conn-queue-cap 64
  bool shutdown_ = false;
  std::vector<std::thread> workers_;
};

// Connection 侧（同受 pool.mu_ 保护）：
//   SchedState sched_state;
//   std::deque<MsgTask> tasks;         // MsgTask{ uint8_t type; std::string body; }
```

### 5.2 每连接调度状态机

```
          submit()                 worker 取出 1 条任务
IDLE ────────────────► QUEUED ──────────────────────► RUNNING
  ▲                      ▲                              │
  │   任务执行完且队列空    │      执行完且队列非空          │
  └──────────────────────┴──────────────────────────────┘
```

- **至多一条**任务处于 RUNNING ⇒ 同连接严格串行（含跨 submit 时机：执行中到达的新任务只入队，runner 完成后重新入 ready_）。
- worker 每次访问只取 **1 条**任务（轮转公平：不同连接交错执行，单连接大量 CHUNK 不霸占某 worker）。

### 5.3 `submit()`（IO 线程调用，必要时阻塞 IO 线程）

```cpp
void ThreadPool::submit(const std::shared_ptr<Connection>& c, MsgTask t) {
    std::unique_lock lk(mu_);
    cv_space_.wait(lk, [&] {
        return shutdown_ ||
               (queued_total_ < queue_cap_ && c->tasks.size() < conn_queue_cap_);
    });
    if (shutdown_) return;                       // 退出期丢弃新任务（连接随即被强关）
    c->tasks.push_back(std::move(t));
    ++queued_total_;
    if (c->sched_state == SchedState::IDLE) {
        c->sched_state = SchedState::QUEUED;
        ready_.push_back(c);
        cv_work_.notify_one();
    }
    // 若 RUNNING/QUEUED：任务已在队列，runner 收尾或既有 entry 会接手
}
```

两个上限语义（D1）：`queued_total_` 是**所有连接排队任务总数**（不含执行中任务）；`c->tasks.size()` 是每连接上限。任一触顶即在 `cv_space_` 上等——**阻塞的就是 IO 线程本身**，与 PRD §8.2 一致，不做丢弃。

### 5.4 worker 主循环

```cpp
void ThreadPool::worker_loop() {
    for (;;) {
        std::shared_ptr<Connection> c; MsgTask t;
        {
            std::unique_lock lk(mu_);
            cv_work_.wait(lk, [&] { return shutdown_ || !ready_.empty(); });
            if (ready_.empty()) return;          // shutdown 且无待处理
            c = ready_.front(); ready_.pop_front();
            if (c->tasks.empty()) {              // 竞态：已被清扫
                c->sched_state = SchedState::IDLE;
                continue;
            }
            t = std::move(c->tasks.front()); c->tasks.pop_front();
            --queued_total_;
            c->sched_state = SchedState::RUNNING;
            cv_space_.notify_all();              // 唤醒可能阻塞的 IO 线程
        }
        execute(c, t);                           // §7.2：先查 closed，再置 task_running
        // ---- 收尾（三步互不嵌套，满足 §4.2 锁序）----
        bool disc;
        { std::lock_guard g(c->state_mu); c->task_running = false;
          disc = c->disconnect_requested; }
        {
            std::lock_guard g(mu_);
            if (c->closed) {                     // 死连接：批量清扫剩余任务，释放额度
                queued_total_ -= c->tasks.size(); c->tasks.clear();
                c->sched_state = SchedState::IDLE;
                cv_space_.notify_all();
            } else if (!c->tasks.empty()) {
                c->sched_state = SchedState::QUEUED; ready_.push_back(c);
                cv_work_.notify_one();
            } else {
                c->sched_state = SchedState::IDLE;
            }
        }
        if (disc) {                              // 延迟收尾移交回 IO 线程
            { std::lock_guard g(pend_mu_); pending_finalize_.push_back(c); }
            wake_io();                           // eventfd write(1)
        }
    }
}
```

### 5.5 不变式与退出

- **不变式**：在 `pool.mu_` 下恒有 `queued_total_ == Σ(所有 conn->tasks.size())`。任务只会被"worker 取出"或"清扫"两条路径消费，二者都在 `pool.mu_` 下递减计数。
- `shutdown()`：置 `shutdown_=true`、`cv_work_.notify_all()` 后 join 所有 worker。此时所有连接已被强关（§11），死连接的剩余任务由 worker 批量清扫路径快速消化，join 不会久等。

## 6. IO 线程设计（落 D2：LT）

### 6.1 epoll 集合与兴趣位

注册在 epfd 上的 fd：

| fd            | 事件              | 说明                                   |
| :------------ | :---------------- | :------------------------------------- |
| `listen_fd`   | `EPOLLIN`（LT）   | 停止 accept 时 `EPOLL_CTL_DEL`（§11）  |
| `signalfd`    | `EPOLLIN`（LT）   | SIGINT/SIGTERM（全部线程先 `pthread_sigmask` 屏蔽） |
| `eventfd`     | `EPOLLIN`（LT）   | worker→IO 唤醒通道（finalize 移交）    |
| 每个 conn fd  | 见下              | `accept4` 后 `EPOLL_CTL_ADD`           |

每连接兴趣位（由 `update_interest()` 统一计算，在 `conns_mu_` + `send_mu` 下调用）：

```
events = EPOLLRDHUP
       | (EPOLLIN  当 !closing && !paused_in)
       | (EPOLLOUT 当 out_buf 非空)
```

- `paused_in`：高水位（out_buf ≥ 8 MiB）置位 → 摘 EPOLLIN；发送冲刷至 ≤ 4 MiB 清零 → 复挂。LT 下暂停期间到达的数据留在内核缓冲，复挂后按层触发补通知，无丢失风险（这正是选 LT 的理由之一）。
- `closing`：任何关闭路径一旦进入"冲刷中"，不再读新数据（摘 EPOLLIN），但保留 EPOLLOUT 直到 out_buf 清空。
- 与已注册掩码相同则跳过 `epoll_ctl`（`conn->epoll_events` 记录当前掩码）。

### 6.2 事件循环骨架

```cpp
while (running_) {
    int timeout = shutting_down_ ? 剩余宽限毫秒 : -1;     // §11
    int n = epoll_wait(epfd, evs, kMaxEvents, timeout);
    if (n < 0) { if (errno == EINTR) continue; LOG_ERROR(...); break; }
    if (n == 0) { on_grace_timeout(); continue; }         // 仅退出宽限期会走到
    for (each ev) {
        if (fd == listen_fd)   { accept_loop(); continue; }
        if (fd == signalfd)    { on_signal(); continue; }
        if (fd == eventfd)     { drain_eventfd(); process_pending_finalize(); continue; }
        auto c = lookup(ev.data.fd);                      // conns_mu_ 下取 shared_ptr
        if (!c || c->closed) continue;                    // 陈旧事件/死连接：跳过
        if (ev.events & (EPOLLERR | EPOLLHUP)) { disconnect(c, /*immediate=*/true); continue; }
        if (ev.events & EPOLLRDHUP) {                     // 半关：不再读，见下
            c->peer_eof = true;
            if (out_empty(c)) { finalize(c); continue; }
        }
        if (ev.events & EPOLLOUT) {                       // 冲刷 out_buf
            flush_out(c);
            if (c->peer_eof && out_empty(c)) { finalize(c); continue; }
        }
        if (ev.events & EPOLLIN) read_parse(c);           // 含 submit()
    }
}
```

要点：

- **陈旧事件安全**：任何一批 `epoll_wait` 结果中，每 fd 至多一条事件。IO 在某条事件里 `finalize`（DEL + close）后，同批不会再有该 fd 的事件；其它事件按 fd 查表，查不到或 `closed` 即跳过。accept 复用的新 fd 不会命中同批旧事件（批次是快照）。这是"只在 IO 线程 close fd"之所以充分安全的关键。
- `read_parse()` 可能因 `submit()` 阻塞而停住整个 IO 线程——这是 PRD §8.2 的既定语义，接受。

### 6.3 accept

```cpp
for (;;) {
    int fd = accept4(listen_fd, &sa, &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;   // LT：一次事件内排空
        if (errno == EMFILE || errno == ENFILE) { LOG_WARN(...); break; }  // 见 §16 风险表
        if (errno == EINTR) continue;
        LOG_ERROR(...); break;
    }
    auto c = make_shared<Connection>(fd, inet_ntop(sa));      // 记录点分十进制 IPv4
    conns_[fd] = c; 明确 ADD EPOLLIN|EPOLLRDHUP
    LOG_INFO(c, "connected");
}
```

每 IP 的存储子目录**不在 accept 时创建**，推迟到该连接第一条 `FILE_META` 处理时按需 `mkdir`（PRD §7.1）。

### 6.4 接收与解析

- `recv` 循环排空到 `EAGAIN`（LT 下减少 epoll 往返）。零拷贝入 buf：`resize(old + 256KiB)` → `recv(data+old, 256KiB)` → 按实际长度收缩。`256KiB` 为 `kRecvChunk` 常量。
- 解析严格按 PRD §5.5 两阶段状态机：

```
NEED_HEADER: 待 ≥10B → 读头 → 静态校验（§8.1）→ 通过则 NEED_BODY(知道 body_len)
NEED_BODY  : 待 ≥body_len → 切出 body → 组装 MsgTask → submit() → NEED_HEADER
```

- **in_buf 管理**：`std::vector<char>` + `consumed` 偏移；切成完整帧后推进 `consumed`，帧恰好用完则整体 `clear()`；跨帧残留（半帧）留在尾部，下次 recv 追加。不做环形缓冲（半帧最多 4 MiB+10B，偏移法足够；内存核算见 §16 风险表）。`parse_loop` 结束时，若 `consumed > 0`，则 memmove 剩余半帧到头部，`resize(size - consumed)`，`consumed = 0`；下次 recv 追加时从干净的尾部开始。
- **body 拷贝策略**：组装 `MsgTask` 时把 body 从 in_buf `memcpy` 一份（每帧一次拷贝，~10 GB/s 级开销，在 300 MiB/s 目标下占比 <1%），换取任务生命周期与 in_buf 彻底解耦。**接受这次拷贝**，不做零拷贝引用。
- 静态校验失败（§8.1）→ `send_error(c, code, msg)` → 标记 `closing` → 停止解析该连接剩余数据（已解析出的更早任务由 worker 侧 `closed` 检查丢弃）。

### 6.5 发送路径

`send_frame(conn, header+body)`，worker 与 IO 都可能调用（IO 发协议错误帧时）：

```cpp
void send_frame(const shared_ptr<Connection>& c, std::string_view bytes) {
    std::lock_guard g1(conns_mu_);
    auto it = conns_.find(c->fd);
    if (it == conns_.end() || it->second.get() != c.get()) return;  // 已死/复用了 fd 号
    std::lock_guard g2(c->send_mu);
    c->out_buf.append(bytes);
    send_direct_locked(c);              // send(fd, data+sent_off, …, MSG_NOSIGNAL) 循环
    update_interest_locked(c);          // 水位/EPOLLOUT 变化 → 记录新掩码
    if (掩码变了) epoll_ctl(EPOLL_CTL_MOD, c->fd, 新掩码);          // R2 合法
}
```

- `update_interest_locked`：out_buf 非空 → 需要 EPOLLOUT；size ≥ 8 MiB → `paused_in=true`；这之后由 IO 的冲刷路径负责"≤4 MiB 恢复 EPOLLIN"（两处互补，见 §6.1 公式）。
- 直发失败 `EAGAIN` 只意味着残留字节留给 EPOLLOUT 路径，不算错误。
- `EPIPE`/`ECONNRESET` 等发送错误 → 标记该 conn 需断开（equivalent 于 EOF 路径，由 IO 收尾）。
- out_buf 实际会很小：MVP 服务端只发 ACK/ERROR/心跳回执（≤ 320B），8 MiB 高水位本质是为未来服务端推送大数据留的机制；实现仍按 PRD §8.3 全量落地。

### 6.6 唤醒机制

- **signalfd**：启动时 `pthread_sigmask` 屏蔽 SIGINT/SIGTERM（继承给所有线程），IO 线程读 signalfd 触发优雅退出。SIGPIPE 直接 `signal(SIGPIPE, SIG_IGN)`（PRD §8.4），另有 `MSG_NOSIGNAL` 双保险。
- **eventfd**：worker 完成"延迟收尾移交"时 `write(eventfd, 1)` 唤醒可能阻塞在 `epoll_wait(-1)` 的 IO 线程（§7.4）。IO 收到后排空 eventfd，处理 `pending_finalize_` 列表副本。

## 7. 连接生命周期与 fd 安全（核心并发设计）

本节是全文最关键的部分：所有"谁在什么时候可以碰这个 fd / 这块会话状态"的规则都在这里定死，实现时不得偏离。

### 7.1 字段与所有者

```cpp
class Connection {
  // 不可变
  int fd; std::string peer_ip;
  // --- IO 线程独占（无锁）---
  std::vector<char> in_buf; size_t consumed = 0;
  ParseState parse_state = ParseState::NEED_HEADER;  // NEED_HEADER / NEED_BODY
  uint32_t pending_body_len = 0; uint8_t pending_type = 0;
  bool peer_eof = false;                             // RDHUP/读到 0 之后不再读
  // --- 跨线程 ---
  mutable std::mutex state_mu;
  SessionState state = SessionState::CONNECTED;      // CONNECTED / RECEIVING（仅 worker 正常流转时写）
  FileSession  session;                              // 仅"task_running==true"的 worker 与 IO 收尾时访问
  bool task_running = false;                         // state_mu：是否有 worker 任务正在执行
  bool disconnect_requested = false;                 // state_mu：IO 已判定断开但任务在跑，收尾移交
  std::atomic<bool> closed{false};                   // 逻辑死亡：任何 fd 访问前必须检查
  std::atomic<bool> closing{false};                  // 冲刷后关闭（协议错误/会话错误触发）
  // --- pool.mu_ 保护（§5）---
  SchedState sched_state = SchedState::IDLE;
  std::deque<MsgTask> tasks;
  // --- send_mu 保护 ---
  std::string out_buf; size_t sent_off = 0; bool paused_in = false;
  uint32_t epoll_events = 0;                         // 当前已注册掩码
  bool finalized = false;                            // conns_mu_ 保护：finalize 幂等
};
```

`FileSession`（tmp 文件 fd、`final_path`、`tmp_path`、`expected_size`、`received`、`expected_md5hex`、`Md5` 增量器、`active`）的访问规则：**当 `task_running==true` 时，session 被该 worker 独占，IO 线程绝不触碰**（IO 只会置标志位）；当 `task_running==false` 时只有 IO 的 finalize 可能清理 session。于是 session 内部的文件操作（write/fsync/close/rename）**无需额外加锁**，也不违反 §4.2。

### 7.2 四个不变量

- **I1（fd 访问前提）**：任何线程访问 conn fd / tmp fd，必须满足"此前在 `state_mu` 下检查过 `!closed`，且（对 worker）当前任务已置 `task_running=true`"。
- **I2（fd 关闭前提）**：`close(conn fd)` 只由 IO 线程在 `finalize()` 中执行；执行前提是在 `state_mu` 下确认 `closed==true && task_running==false`。
- **I3（finalize 幂等）**：`finalize()` 全程持 `conns_mu_`，以 `finalized` 标志保证只生效一次；重复调用直接返回。
- **I4（陈旧事件）**：同批 `epoll_wait` 结果每 fd 至多一条事件；IO close fd 后同批无该 fd 事件，跨批事件查表时 conn 已摘除或 `closed` → 跳过。

### 7.3 一条连接的一生

```
accept ──► [CONNECTED] ──META ok──► [RECEIVING] ──END ok──► [CONNECTED] ──…
   │            │                       │
   │            └── 错误/EOF/信号 ──► closing / disconnect ──► finalize ──► 对象销毁
   │
   └─ 关闭四情景（§7.5）决定走哪条收尾路径
```

### 7.4 finalize 与"延迟收尾移交"

`finalize(c)`（仅 IO 线程调用，幂等）：

```
持 conns_mu_：
  if (c->finalized) return;  c->finalized = true;
  epoll_ctl(EPOLL_CTL_DEL, c->fd);          // 先摘出 epoll
  conns_.erase(c->fd);                       // 此后 lookup 不再命中
  持 state_mu：                               // 合法嵌套（conns_mu_ → state_mu）
    if (session.active) { close(session.tmp_fd); unlink(session.tmp_path); }   // PRD §6.3
  持 pool.mu_：                               // 合法嵌套（conns_mu_ → pool.mu_）
    清扫 c->tasks，queued_total_ 相应递减，cv_space_.notify_all()
  c->closed = true;                          // （通常早已为 true）
  close(c->fd);
```

**移交时间线**（IO 检测到断开时 worker 恰在执行任务）：

```
worker(执行 task, task_running=true)            IO 线程
                                    ──► 检测 EOF/ERR → disconnect(c)：
                                        state_mu: closed=1, disconnect_requested=1
                                        task_running==true → fd 不关，仅：
                                        epoll DEL（不再收事件）
task 完成 → state_mu: task_running=0
         → pending_finalize_.push_back(c) + eventfd ──►  唤醒 epoll_wait
                                                        drain eventfd → finalize(c)
```

对侧情况（`task_running==false`）：IO 直接就地 finalize，无移交开销。

**为什么需要这个机制**：IO 若不等任务结束就 `close(tmp_fd)`，worker 的 `write()` 可能撞上 fd 号被复用（close 后立刻被 accept/打开其他文件占用），把数据写进无关文件——这是必须消除的真实竞态。做法是"谁最后离开谁负责叫醒 IO 收尾"，且收尾永远落在单一所有者（IO）身上。

### 7.5 四种关闭触发路径（flush 语义）

| 触发                       | 检测层 | 动作序列                                                                                     |
| :------------------------- | :----- | :------------------------------------------------------------------------------------------- |
| 协议错误（magic/ver/type/长度） | IO     | `send_error()` → `closing=true` → 摘 EPOLLIN → out_buf 冲刷空后 finalize（client 若一直不读，该连接滞留，仅占自身资源，MVP 接受） |
| 会话错误（NO_SESSION/BAD_FILENAME/WRITE_FAILED/MD5_MISMATCH/SIZE_MISMATCH/OVERFLOW） | worker | 清理 tmp（如适用）→ `send_error()` → `closing=true` → worker 侧 send_frame 已处理直发/EPOLLOUT；IO 在冲刷空后 finalize |
| 对端半关 / 读到 0（RDHUP/EOF） | IO     | `peer_eof=true` 停止读；out_buf 空 → 立即 finalize；非空 → 冲刷空后 finalize（PRD §8.4）      |
| EPOLLHUP / EPOLLERR        | IO     | 直接 `disconnect()` → finalize（不冲刷，PRD §8.4）                                            |

任何路径 finalize 时若 `session.active`，一律 `close(tmp_fd) + unlink(tmp_path)`（PRD §6.3、AC-4）。

### 7.6 竞态排查清单（实现时的对照表）

| 竞态                                    | 防线                                                        |
| :-------------------------------------- | :---------------------------------------------------------- |
| `write(tmp_fd)` vs `close(tmp_fd)`      | I2 + §7.4 移交：close 只在 `task_running==false` 后发生      |
| worker `epoll_ctl(MOD)` vs IO `close`   | R2：MOD 前在 `conns_mu_` 下核对 fd→指针一致；close 也在 `conns_mu_` 下 |
| fd 号复用导致陈旧事件误作用             | I4 + 关闭仅在 IO 线程 + 事件查表 `closed` 跳过               |
| 双重 finalize                           | I3 幂等标志                                                  |
| worker 执行中任务的连接断开             | `closed` 检查（execute 入口）+ 移交（§7.4）                  |
| IO 阻塞在 `submit()` 时断连积压         | worker 心跳线：死连接任务被取出即丢弃并递减额度（§5.4），额度必然释放 |
| 连接已入 ready_ 但被清扫                | worker 取出时 `tasks.empty()` → 置 IDLE 跳过（§5.4）          |
| `submit()` 期间连接死亡                 | 提交照常入队；执行时 `closed` 检查丢弃；额度由清扫路径归还     |

## 8. 消息处理（worker dispatch）

`execute(c, task)`：

```cpp
{ lock(state_mu); if (c->closed) return; c->task_running = true; }
dispatch(c, task);            // 全程独占 session；文件 IO 不持锁
```

### 8.1 校验分层（IO 静态 vs worker 动态）

| 检查项                                                         | 层     | 失败码        |
| :------------------------------------------------------------- | :----- | :------------ |
| magic、version、type 已知、`body_len ≤ 4MiB`                    | IO     | BAD_MAGIC / BAD_VERSION / BAD_TYPE / BODY_TOO_BIG |
| 类型相关静态长度：CHUNK `1..4MiB`；END/HEARTBEAT `==0`；META `≥43` | IO   | BAD_TYPE      |
| META 精确长度 `body_len == 2+name_len+8+32`                     | worker | BAD_TYPE      |
| 文件名清洗（PRD §7.3）                                          | worker | BAD_FILENAME  |
| 剩余空间（statvfs，PRD §7.4）                                   | worker | WRITE_FAILED  |
| 状态机合法性（NO_SESSION / META-in-RECEIVING）                  | worker | NO_SESSION / BAD_TYPE |

错误优先级（同一帧多处非法时按上表自上而下先命中者胜）。

### 8.2 FILE_META（仅 CONNECTED）

1. `state==RECEIVING` → BAD_TYPE（PRD §6.2）。
2. 解析 `name_len / name / size / md5hex`；精确长度不符 → BAD_TYPE。
3. 文件名清洗 → BAD_FILENAME。（`md5hex` 只做原样保存，**不提前校验格式**，见 §15-D10）
4. `mkdir <storage>/<peer_ip>`（已存在 EEXIST 即通过；0755）。
5. `statvfs(storage_dir)`：`size > f_bavail*f_frsize − 16MiB` → WRITE_FAILED `"no space"`。
6. 生成 `tmp_path = final_path + ".part." + 8位hex随机`，`open(O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW, 0600)`；EEXIST 换随机数重试 ≤5 次，仍失败 → WRITE_FAILED。
7. 建立 FileSession（记录 expected_size / expected_md5hex / 初始化增量 Md5 / received=0 / active=true），`state=RECEIVING`。
8. 日志：文件名、大小、MD5（PRD §11）。
9. 成功路径**不回任何响应**（协议只在 END 后回 ACK）。

### 8.3 FILE_CHUNK（仅 RECEIVING）

1. 非 RECEIVING → NO_SESSION（PRD §6.1/§6.2）。
2. `received + len > expected_size` → 清理 tmp → OVERFLOW。
3. `write()` 循环写满；`ENOSPC` 或其它错误（含 `EINTR` 重试后仍失败）→ 清理 tmp → WRITE_FAILED `"write: <errno>"`。
4. `md5.update()`；`received += len`。

### 8.4 FILE_END（仅 RECEIVING）

1. `received != expected_size` → 清理 tmp → SIZE_MISMATCH。
2. `fsync(fd)` 失败 → 清理 tmp → WRITE_FAILED。
3. `close(tmp_fd)`；`md5.finalize()` 取 32 位小写 hex。
4. hex ≠ expected_md5hex → `unlink(tmp)` → MD5_MISMATCH（msg 带 expected/got）。
5. `rename(tmp, final)` 失败 → `unlink(tmp)` → WRITE_FAILED；成功 → `chmod(final, 0644)`（失败仅 WARN，不致命，§15-D8）。
6. `ACK(OK, msg=服务端 md5hex)`（PRD §5.4）；`state=CONNECTED`；session 复位；成功计数 +1。

### 8.5 HEARTBEAT（仅 CONNECTED）

回 `ACK(OK)`，msg 为空（PRD §5.2/§6.1）。RECEIVING 中收到 → BAD_TYPE（§15-D6）。

### 8.6 协议编解码要点

- `common.h` 提供 `be_get16/32/64`、`be_put16/32/64`（逐字节移位，无别名/对齐问题）。
- 帧构造统一 `build_frame(type, body)` → `10B 头 + body`；ACK/ERROR body = `u32 code | u32 msg_len | msg(≤256B)`。
- **勘误**：PRD 附录 A 示例中两处 `body_len` 与字段定义矛盾——`FILE_META` 应为 `2+9+8+32 = 51 (0x33)`（示例写 0x29=41），`ACK` 应为 `4+4+32 = 40 (0x28)`（示例写 0x24=36）。**以字段定义为准**，实现与 test_client 均按定义值。

## 9. 错误码 → 检测层 → 清理动作 对照（PRD §9 落地）

| 码/名称        | 检测层 | tmp 清理           | 连接处理                    |
| :------------- | :----- | :----------------- | :-------------------------- |
| BAD_MAGIC/VER  | IO     | 若 session.active  | ERROR → 冲刷 → 关（§7.5 路径1） |
| BAD_TYPE/BODY_TOO_BIG | IO 或 worker | 同上       | 同上                        |
| NO_SESSION     | worker | 无（无 session）   | ERROR → 冲刷 → 关           |
| BAD_FILENAME   | worker | 无                 | ERROR → 冲刷 → 关           |
| WRITE_FAILED   | worker | 有 → close+unlink  | ERROR → 冲刷 → 关           |
| MD5_MISMATCH   | worker | 有 → close+unlink  | ERROR → 冲刷 → 关           |
| SIZE_MISMATCH  | worker | 有 → close+unlink  | ERROR → 冲刷 → 关           |
| OVERFLOW       | worker | 有 → close+unlink  | ERROR → 冲刷 → 关           |
| INTERNAL_ERR   | 任意   | 是                 | ERROR → 冲刷 → 关；`try/catch(...)` 兜底：worker 内异常只关该连接（PRD §9.3） |

所有 ERROR 的 `msg` 尽量填可读原因，上限 256B（PRD §9.2）。

## 10. 日志 / CLI / 统计

- 日志（PRD §11）：`[YYYY-MM-DD HH:MM:SS.mmm][LEVEL][fd=12 ip=192.168.1.10] message`；全局消息（启动/退出/信号）省略 `fd=… ip=…` 段。`fprintf(stderr, …)` 单次完整行输出（glibc stdio 内部锁保证行原子）；级别过滤在宏内先判 `level >= min_level` 再格式化。时间戳用 `clock_gettime(CLOCK_REALTIME)` + `localtime_r`。
- 关键日志点照 PRD §11 列表执行；`FILE_END` 成功/失败、每次 ERROR 均有 INFO/WARN/ERROR 行。
- CLI（PRD §10.1）：解析失败 → 用法打 stderr、退出码 1；`--threads` 默认 `hardware_concurrency()`（返回 0 时回退 4，§15-D11）；`--dir` 默认 `./storage`（启动时 `mkdir -p`，失败退出码 2）。
- 统计：`total_conns / ok_files / fail_files` 三个 `std::atomic<uint64_t>`，退出时随统计行打印（PRD §11/§12）。

## 11. 启动序列与优雅退出（落 D3）

### 11.1 启动

1. 解析 CLI（失败退出 1）。
2. `mkdir -p <storage_dir>`（失败退出 2）。
3. 信号：`SIG_IGN` SIGPIPE；屏蔽 SIGINT/SIGTERM 后建 `signalfd`。
4. `socket(AF_INET, SOCK_STREAM|NONBLOCK|CLOEXEC)` + `SO_REUSEADDR` + `bind(INADDR_ANY, port)` + `listen(backlog)`（失败退出 2）。
5. `epoll_create1(CLOEXEC)`；注册 listen/signalfd/eventfd。
6. 启动线程池（N workers）。
7. `LOG_INFO` 监听地址与端口（PRD §11）；进入事件循环。

### 11.2 优雅退出（宽限 3 秒）

```
收到 SIGINT/SIGTERM（signalfd 可读）：
  1. shutting_down_ = true；EPOLL_CTL_DEL(listen_fd) → 不接收新连接
  2. 记录 deadline = now + 3s；此后 epoll_wait 用剩余毫秒作 timeout
     期间：已建立连接照常收/发/处理（"允许完成当前文件"）
  3. deadline 到（或所有连接已自然结束）：
     对所有剩余连接 disconnect(force) → finalize（unlink tmp）
  4. pool.shutdown()（worker 清扫死连接队列后退出）；join
  5. 打印统计（累计连接数、成功文件数、失败文件数）→ 退出码 0
```

超时上限 3 秒与 PRD §12 一致；"不接收新连接"由摘除 listen_fd 实现（内核 backlog 里的半连接在进程退出时被 RST，MVP 接受）。

## 12. 构建（Makefile）

```
make all     # 默认 release：build/fsync-server、build/test-client
make debug   # -O0 -g3 -fsanitize=address,undefined（AC-11 用）
make clean
```

- Release：`-O2 -Wall -Wextra -Wpedantic -pthread -std=c++17`；Debug：`-O0 -g3 -fsanitize=address,undefined -pthread -std=c++17`（PRD §2）。
- 依赖：`-MMD -MP`，`.d` 随对象文件入 `build/`。
- 产物目录 `build/`（o/d/bin 全在其中），`make clean` 删除整个目录。

## 13. tools/test_client（开发用，非验收）

- CLI（PRD §13.4）：`--host --port --file [--concurrency N] [--repeat N]`。
- 流程：流式算文件 MD5 → 连接 → `FILE_META(name=basename, size, md5hex)` → 按 1 MiB 切 `FILE_CHUNK` → `FILE_END` → 阻塞读 ACK/ERROR（`SO_RCVTIMEO` 兜底）→ 打印：耗时、发送 MD5、ACK 的 MD5、是否一致。
- 并发：`--concurrency N` 起 N 线程各自建连；`N>1` 时文件名追加 `.c<i>` 后缀避免同 IP 同名互相覆盖（工具自身策略，不影响验收语义）。
- 为覆盖 AC-3/5/6/8 增加**故障注入开关**（可裁剪，见 §15-D13）：`--bad-md5`、`--bad-magic`、`--name <伪造名>`（如 `../etc/passwd`）、`--oversize-body`（发 5 MiB body_len 的帧）、`--abort-after <bytes>`（中途断开，AC-4）。

## 14. 验收对照（AC-1 ~ AC-12）

| AC        | 架构保障点                                                                 | 验证方式                                   |
| :-------- | :------------------------------------------------------------------------- | :----------------------------------------- |
| AC-1      | §8 全链路 + md5 增量实现                                                    | 1 GiB…100 MiB 传输后双侧 `md5sum` 比对     |
| AC-2      | 每 IP 子目录 + 每连接隔离（§7）                                             | 5 并发（不同文件后缀）互不污染             |
| AC-3      | §8.4 步骤 4 + 清理                                                          | `--bad-md5`，检查响应码与无残留             |
| AC-4      | §7.5 RDHUP/EOF 路径 finalize 清理                                           | `--abort-after`，检查无 `.part.*` 残留     |
| AC-5/6/8  | §8.1 IO 静态校验 / §8.2 清洗 / BODY_TOO_BIG                                 | `--bad-magic` / `--name ../etc/passwd` / `--oversize-body` |
| AC-7      | END 成功后回 CONNECTED（§8.4）                                              | 单连接串行 3 文件                          |
| AC-9      | recv 排空 + 每帧一次拷贝 + 直发路径（§6.4/6.5）                              | `time` 1 GiB 回环，观察 CPU                  |
| AC-10     | 调度器 >100 并发（§5）                                                      | `--concurrency 100` × 1 MiB                 |
| AC-11     | `make debug` + 全 AC 重跑                                                   | ASan/UBSan 无报告                          |
| AC-12     | 全部资源 RAII/shared_ptr + finalize 幂等                                    | 10 分钟循环压测看 RSS                      |

## 15. 解释性决策与勘误清单（请重点审阅）

| #    | 决策/勘误                                                                                                                     |
| :--- | :---------------------------------------------------------------------------------------------------------------------------- |
| D1   | 队列语义（已确认）：全局排队任务总数计数上限 1024 + 每连接 FIFO 64。                                                            |
| D2   | epoll 水平触发 LT（已确认）。                                                                                                   |
| D3   | 退出宽限 3 秒（已确认）。                                                                                                       |
| D4   | **只有 IO 线程 close conn fd / EPoll_CTL_ADD/DEL**；worker 仅允许 `EPOLL_CTL_MOD` 且需 `conns_mu_` 下核对存活。断连时若 worker 在跑任务，采用"延迟收尾 + eventfd 移交"（§7.4）。 |
| D5   | 新增 `signalfd` + `eventfd` 两个内部 fd（不进协议，属实现细节）。                                                               |
| D6   | `HEARTBEAT` 仅 CONNECTED 合法；RECEIVING 中收到 → BAD_TYPE（依据 PRD §9.1"状态机不允许该消息"）。                                |
| D7   | tmp 文件名 `<final>.part.<8位hex>`：PRD §7.2 原文含 `<fd>`，但 fd 在 open 前不可知；以 `O_EXCL`+重试保证唯一，语义等价。       |
| D8   | 每 IP 目录权限 0755；tmp 文件 0600；`chmod(final,0644)` 执行但失败仅 WARN（PRD 标注"可选"）。                                   |
| D9   | 广播发送错误后的连接：**冲刷完 out_buf 再关**（不立即 close），不设 linger 超时；不读的客户端只滞留自身连接，MVP 接受。          |
| D10  | `md5hex` 字段不在 META 阶段做格式预检，END 时按原样字符串比对（非法格式自然落为 MD5_MISMATCH），最贴合 PRD 错误码表。             |
| D11  | `--threads` 默认 `hardware_concurrency()`，返回 0 时回退 4。                                                                    |
| D12  | 任务体 `memcpy` 一次从 in_buf 拷出（换取生命周期解耦，性能代价 <1%）。                                                            |
| D13  | test_client 增加故障注入开关（AC 覆盖所需，工具非验收范围）。                                                                    |
| D14  | `EMFILE` 时的 accept 策略：记 WARN、跳过本轮 accept（不终止进程）。                                                              |
| E1   | **勘误**：PRD 附录 A 的 `FILE_META` 示例 body_len 应为 51（0x33），原文 0x29 有误。                                              |
| E2   | **勘误**：PRD 附录 A 的 `ACK` 示例 body_len 应为 40（0x28），原文 0x24 有误。                                                    |

## 16. 实现顺序与风险

实现顺序（PRD §17 细化落地）：

1. `common.h`（常量 + 大端 helpers）
2. `md5.{h,cpp}`（用 RFC 1321 测试向量自测：`""`、`"abc"` 等）
3. `log.{h,cpp}`
4. `thread_pool.{h,cpp}`（§5 调度器 + 背压）
5. `connection.{h,cpp}`（结构、发送路径、兴趣位、finalize §7）
6. `server.{h,cpp}`（epoll 循环、accept、解析、submit、signalfd/eventfd）
7. dispatch：META/CHUNK/END/HEARTBEAT（§8）
8. 错误映射与四类关闭路径（§7.5/§9）
9. 优雅退出 + CLI + 统计（§10/§11）
10. `tools/test_client.cpp`（§13）→ 跑 AC-1..12

风险表：

| 风险                                | 防线                                                        |
| :---------------------------------- | :---------------------------------------------------------- |
| fd 生命周期竞态（最难）             | §7 的不变量 I1–I4 + 移交时序；ASan 全程跑 AC                 |
| `submit()` 阻塞 IO 线程 → 全局停摆  | PRD 既定语义；worker 心跳线（§5.4）保证额度必然回收           |
| `write()` 阻塞 worker（慢盘）       | 工作线程数独立于 IO；每连接串行不放大阻塞面；MVP 接受         |
| in_buf 内存峰值（4 MiB×100 连接）   | 帧即取即切（不预读多帧）；最坏 400 MiB，可接受，§6.4 记录     |
| LT EPOLLOUT 忙转                   | 仅 out_buf 非空时挂 EPOLLOUT；每次 send 排空到 EAGAIN 才返回   |
| 慢客户端滞留半关连接                | D9 说明的已知权衡；未来加 linger 超时                          |
| WSL2 回环吞吐不达 300 MiB/s         | 先测基线；筹码：256 KiB recv、MSG_NOSIGNAL 直发、无锁热路径    |
