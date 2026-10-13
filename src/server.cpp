// server.cpp — Server 实现（步骤 6A 基础设施 + 6B 连接事件 happy path）
// 依据：架构文档 §6.1（epoll 集合）、§6.2（事件循环）、§6.3（accept）、§6.4（接收与
//       解析）、§6.6（eventfd/signalfd）、§11.1（启动序列）、§11.2（优雅退出）、
//       §15-D2/D4/D5/D8/D12/D14；PRD §8.4（SIGPIPE）、§10.2（退出码）、§11（日志）、
//       §12（生命周期）。
//
// 6B 边界（本步不做，TODO 标注了负责的子步骤）：
//   - 静态校验（magic/version/type/body_len）与错误帧 send_error → 6C；
//   - send_frame / apply_interest_locked / finalize 的正式实现 → 6C
//     （本步关闭连接走 close_connection_brutal，recv 硬错误只置 closing）；
//   - worker 侧 dispatch（execute 仍是占位：任务被取出后丢弃）→ 第 7 步。
// 因此 6B 只保证"帧切割 + submit"这一段链路可验证。

#include "server.h"

#include <arpa/inet.h>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <netinet/in.h>
#include <pthread.h>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include "log.h"

namespace {

// epoll_wait 单批事件上限（架构 §6.2 的 kMaxEvents）
constexpr int kMaxEvents = 256;

// recv 单次读入量（架构 §6.4 的 kRecvChunk）：一次 recv 最多预留并收取 256 KiB。
// 走"resize 预留 → recv 直写 in_buf 尾部 → 按实际长度收缩"，故不需要 256 KiB
// 的栈/thread_local 中转缓冲（架构 §6.4 原文即此写法，一次拷贝入 buf）。
constexpr std::size_t kRecvChunk = 256U * 1024U;

// 写 eventfd：worker→IO 的唤醒通道（架构 §6.6），wake_io_cb_ 与 request_stop 共用。
// 失败不打印日志（worker 侧热路径）：计数已达上限时说明 fd 本就可读，不丢唤醒；
// fd 已关只会发生在析构期（那时 worker 已 join，不会走到这里）。
void write_eventfd(int efd) noexcept {
    const uint64_t one = 1;
    const ssize_t rc = ::write(efd, &one, sizeof(one));
    (void)rc;  // 见上：失败无需补救动作
}

// 信号准备（架构 §11.1 步骤 3 + PRD §8.4），返回 signalfd。
//
// 由成员初始化列表调用（signalfd_ 声明在 pool_ 之前，故先于 worker 线程创建执行）：
// 信号掩码是**每线程**的且在线程创建时继承，若晚于 pool_ 构造，SIGINT/SIGTERM 可能
// 投递到某个 worker 上按默认动作处理 → 进程被直接杀死，优雅退出失效。
// 失败抛 std::runtime_error（此时 pool_ 尚未构造，无线程需要回收）。
int setup_signals() {
    // SIGPIPE 置 IGN 是进程级 disposition（与掩码不同，可晚设），
    // 是 MSG_NOSIGNAL 之外的双保险（PRD §8.4）。
    ::signal(SIGPIPE, SIG_IGN);

    sigset_t mask;
    ::sigemptyset(&mask);
    ::sigaddset(&mask, SIGINT);
    ::sigaddset(&mask, SIGTERM);
    const int rc = ::pthread_sigmask(SIG_BLOCK, &mask, nullptr);
    if (rc != 0) {  // pthread_* 返回错误码本身，不写 errno
        throw std::runtime_error(std::string("Server 启动失败：pthread_sigmask: ") +
                                 std::strerror(rc));
    }
    const int fd = ::signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (fd < 0) {
        throw std::runtime_error(std::string("Server 启动失败：signalfd: ") +
                                 std::strerror(errno));
    }
    return fd;
}

}  // namespace

// ---------------------------------------------------------------------------
// 构造 / 析构
// ---------------------------------------------------------------------------

Server::Server(const ServerConfig& cfg)
    : epfd_(-1),
      listen_fd_(-1),
      eventfd_(-1),
      signalfd_(setup_signals()),
      port_(cfg.port),
      storage_dir_(cfg.storage_dir),
      // wake_io_cb_ 只写 eventfd_、不置 running_（架构 §7.4 的纯唤醒语义）：
      // 收到唤醒的是 IO 线程，退出与否由它自己决定。
      pool_(cfg.n_threads, cfg.queue_cap, cfg.conn_queue_cap,
            [this] { write_eventfd(eventfd_); }) {
    // 构造中途失败：析构函数不会执行，统一经 fail() 回收已开 fd 后再抛，
    // 避免 fd 泄漏（close 会覆写 errno，故先取出 e）。
    auto fail = [this](const char* what) {
        const int e = errno;
        if (listen_fd_ >= 0) ::close(listen_fd_);
        if (epfd_ >= 0)      ::close(epfd_);
        if (eventfd_ >= 0)   ::close(eventfd_);
        if (signalfd_ >= 0)  ::close(signalfd_);
        throw std::runtime_error(std::string("Server 启动失败：") + what + ": " +
                                 std::strerror(e));
    };

    // 1) 监听套接字（架构 §11.1 步骤 4）：非阻塞 + CLOEXEC，accept4 的默认属性同源
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd_ < 0) fail("socket");

    const int one = 1;
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
        fail("setsockopt(SO_REUSEADDR)");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(cfg.port);
    if (::bind(listen_fd_, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
        fail("bind");
    }
    if (::listen(listen_fd_, cfg.backlog) < 0) fail("listen");

    // 2) epoll 实例与 eventfd（架构 §11.1 步骤 5、§6.6）
    epfd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epfd_ < 0) fail("epoll_create1");

    eventfd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (eventfd_ < 0) fail("eventfd");

    // 3) 三个内部 fd 入 epoll：全部 EPOLLIN + LT（架构 §6.1 表 / §15-D2）
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd_;
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0) {
        fail("epoll_ctl(ADD listen_fd)");
    }
    ev.data.fd = eventfd_;
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, eventfd_, &ev) < 0) {
        fail("epoll_ctl(ADD eventfd)");
    }
    ev.data.fd = signalfd_;
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, signalfd_, &ev) < 0) {
        fail("epoll_ctl(ADD signalfd)");
    }

    // 4) 存储根目录（架构 §11.1 步骤 2）：已存在视为成功（EEXIST）。
    //    多级路径的 mkdir -p 语义随第 9 步 --dir 解析一起补，本步只建一层。
    if (::mkdir(storage_dir_.c_str(), 0755) < 0 && errno != EEXIST) {
        fail("mkdir(storage_dir)");
    }

    // 5) 启动完成的日志（架构 §11.1 步骤 7；含端口与存储目录，PRD §11）
    LOG_INFO("监听 0.0.0.0:%u，存储目录 %s，worker %zu",
             static_cast<unsigned>(cfg.port), storage_dir_.c_str(), cfg.n_threads);
}

Server::~Server() {
    // 先停线程池：worker 的 wake_io_cb_ 会写 eventfd_，必须在关 fd 之前 join 完
    // （架构 §11.2 步骤 4）；这一步同时保证没有 worker 再持有 conns_ 里的对象。
    pool_.shutdown();

    // 连接表随本类销毁。此处不做优雅收尾（退出宽限属 6D）：残留对象由
    // ~Connection 兜底 close(fd)，其 FileSession 由 ~FileSession 兜底 unlink。
    conns_.clear();

    if (signalfd_ >= 0)  ::close(signalfd_);
    if (eventfd_ >= 0)   ::close(eventfd_);
    if (listen_fd_ >= 0) ::close(listen_fd_);
    if (epfd_ >= 0)      ::close(epfd_);
}

// ---------------------------------------------------------------------------
// 主循环（架构 §6.2 骨架；宽限超时属 6D，本步 timeout 恒为 -1）
// ---------------------------------------------------------------------------

void Server::run() {
    epoll_event evs[kMaxEvents];
    while (running_) {
        const int n = ::epoll_wait(epfd_, evs, kMaxEvents, -1);
        if (n < 0) {
            if (errno == EINTR) continue;  // 被信号打断：信号本身由 signalfd 接管
            LOG_ERROR("epoll_wait 失败：%s", std::strerror(errno));
            break;
        }
        for (int i = 0; i < n; ++i) {
            const int fd = evs[i].data.fd;
            if (fd == listen_fd_) { on_accept();    continue; }
            if (fd == eventfd_)   { drain_eventfd(); continue; }
            if (fd == signalfd_)  { handle_signal(); continue; }

            // 连接事件：先在 conns_mu_ 下取 shared_ptr；查不到 = 陈旧事件，
            // closed = 本批内已被收尾 —— 一律跳过（架构 §6.2 陈旧事件安全）。
            ConnectionPtr c;
            {
                std::lock_guard<std::mutex> g(conns_mu_);
                const auto it = conns_.find(fd);
                if (it != conns_.end()) c = it->second;
            }
            if (!c || c->closed.load()) continue;

            // 分发顺序照架构 §6.2：EPOLLERR|EPOLLHUP → EPOLLRDHUP → EPOLLOUT → EPOLLIN
            if (evs[i].events & (EPOLLERR | EPOLLHUP)) {
                LOG_WARN_C(c->fd, c->peer_ip.c_str(), "epoll 报错（events=0x%x），断开",
                           evs[i].events);
                close_connection_brutal(c);  // TODO(6C)：改走 finalize(c)
                continue;
            }

            if (evs[i].events & EPOLLRDHUP) {
                // 半关：置 peer_eof 即"停止读"，由 update_interest 摘掉 EPOLLIN
                // （LT 下不再对残留数据重复报 EPOLLIN，架构 §6.1）。
                c->peer_eof = true;
                update_interest(c);
                bool out_empty;
                {
                    std::lock_guard<std::mutex> g(c->send_mu);
                    out_empty = c->out_empty_locked();
                }
                if (out_empty) {
                    close_connection_brutal(c);  // TODO(6C)：改走 finalize(c)
                    continue;
                }
                // out_buf 非空：6B 无发送路径，恒不会走到；6C 由 EPOLLOUT 冲刷后收尾。
            }

            if (evs[i].events & EPOLLOUT) {
                // 6B：out_buf 恒空（worker 侧 execute 是占位，尚无发送路径），EPOLLOUT
                // 本就不该挂上；真收到也只重算一次掩码把它摘掉。
                // TODO(6C)：换成 on_writable(c) 的正式冲刷 + 发空后按 peer_eof 收尾。
                update_interest(c);
            }

            if (evs[i].events & EPOLLIN) {
                on_readable(c);
            }
        }
    }

    // 退出统计行（PRD §11 / §12）
    LOG_INFO("退出：累计连接 %llu，成功文件 %llu，失败文件 %llu",
             static_cast<unsigned long long>(stats_.total_conns.load()),
             static_cast<unsigned long long>(stats_.files_ok.load()),
             static_cast<unsigned long long>(stats_.files_fail.load()));
}

void Server::request_stop() {
    running_ = false;
    write_eventfd(eventfd_);  // 唤醒阻塞在 epoll_wait(-1) 的 IO 线程（架构 §6.6）
}

// ---------------------------------------------------------------------------
// accept 与两个内部 fd（6A/6B：on_accept 已是 6B 的完整版）
// ---------------------------------------------------------------------------

void Server::on_accept() {
    // 架构 §6.3：LT 下一次事件内排空到 EAGAIN。每 IP 的存储子目录不在 accept 阶段
    // 创建，推迟到该连接第一条 FILE_META（PRD §7.1）。
    for (;;) {
        sockaddr_in sa{};
        socklen_t len = sizeof(sa);
        const int fd = ::accept4(listen_fd_, reinterpret_cast<sockaddr*>(&sa), &len,
                                 SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;  // LT：一次事件内排空
            if (errno == EINTR) continue;
            if (errno == EMFILE || errno == ENFILE) {  // 架构 §15-D14：跳过本轮，不终止
                LOG_WARN("accept4：fd 耗尽（%s），本轮不再 accept", std::strerror(errno));
                break;
            }
            LOG_ERROR("accept4 失败：%s", std::strerror(errno));
            break;
        }

        // 点分十进制 IPv4 + 主机字节序端口（架构 §6.3）
        char ip[INET_ADDRSTRLEN] = "?";
        if (::inet_ntop(AF_INET, &sa.sin_addr, ip, sizeof(ip)) == nullptr) {
            ip[0] = '?';  // 理论不可达：退化成占位串，不因日志字段丢掉连接
        }
        auto c = std::make_shared<Connection>(fd, ip, ntohs(sa.sin_port));

        // 入表（acquire 语义：worker 通过 conns_ 只能看到构造完全的 Connection）
        {
            std::lock_guard<std::mutex> g(conns_mu_);
            conns_[fd] = c;
        }

        // 挂 EPOLLIN|EPOLLRDHUP（LT）。ADD 只由 IO 线程做（架构 §15-D4）；
        // 此时对象刚发布、尚无任务投递，worker 触达不到，故直接写 epoll_events。
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLRDHUP;
        ev.data.fd = fd;
        if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
            LOG_ERROR_C(c->fd, c->peer_ip.c_str(), "epoll_ctl(ADD) 失败：%s",
                        std::strerror(errno));
            close_connection_brutal(c);  // 已入表：走同一收尾路径摘除 + close
            continue;
        }
        c->epoll_events = EPOLLIN | EPOLLRDHUP;

        stats_.total_conns.fetch_add(1, std::memory_order_relaxed);
        LOG_INFO_C(fd, c->peer_ip.c_str(), "connected");
    }
}

void Server::drain_eventfd() {
    // LT：必须读空，否则 epoll_wait 立刻再次就绪 → 忙转。
    // eventfd 读满 8 字节即把计数清零，一次成功读取即可；循环只为吃掉 EINTR。
    uint64_t v = 0;
    for (;;) {
        const ssize_t n = ::read(eventfd_, &v, sizeof(v));
        if (n == static_cast<ssize_t>(sizeof(v))) break;
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && errno != EAGAIN) {
            LOG_WARN("eventfd 读取失败：%s", std::strerror(errno));
        }
        break;  // EAGAIN：已排空
    }
    // TODO(6C)：pool_.take_pending(list) → 逐个 finalize(c)（架构 §7.4 延迟收尾）
}

void Server::handle_signal() {
    // 6A 只做两件事：排空 signalfd（LT，不排空会忙转）+ 结束主循环。
    // TODO(6D)：改为 shutting_down_ 语义 —— EPOLL_CTL_DEL(listen_fd) 停止 accept、
    //           epoll_wait 切 3 秒宽限 timeout、到期 on_grace_timeout() 强关残余连接
    //           （架构 §11.2 / PRD §12）。
    unsigned signo = 0;
    for (;;) {
        signalfd_siginfo si{};
        const ssize_t n = ::read(signalfd_, &si, sizeof(si));
        if (n == static_cast<ssize_t>(sizeof(si))) {
            if (signo == 0) signo = si.ssi_signo;  // 只记第一个，够日志用
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        break;  // EAGAIN：已排空
    }
    if (signo != 0) {
        LOG_INFO("收到信号 %u，退出主循环", signo);
    }
    running_ = false;
}

// ---------------------------------------------------------------------------
// 6B：接收、解析、兴趣位与临时收尾（架构 §6.2 / §6.4 / §6.1）
// ---------------------------------------------------------------------------

void Server::on_readable(const ConnectionPtr& c) {
    // 入口自查 peer_eof（架构 §6.2 末条）：与"摘 EPOLLIN"互为双保险，覆盖同批
    // EPOLLIN|EPOLLRDHUP 组合事件、以及摘除 MOD 生效前的窗口。
    if (c->peer_eof.load()) return;

    for (;;) {
        // 直写 in_buf 尾部：resize 预留 → recv → 按实际长度收缩（架构 §6.4 原文写法）。
        // 由此不需要 256 KiB 的栈/thread_local 中转缓冲，也少一次拷贝。
        const std::size_t old = c->in_buf.size();
        c->in_buf.resize(old + kRecvChunk);
        const ssize_t n = ::recv(c->fd, c->in_buf.data() + old, kRecvChunk, 0);
        if (n > 0) {
            c->in_buf.resize(old + static_cast<std::size_t>(n));
            continue;  // LT：排空到 EAGAIN，减少 epoll 往返（架构 §6.4）
        }
        c->in_buf.resize(old);  // 回滚本次预留（-1 与 0 都要回滚）
        if (n == 0) {           // 对端关闭：此后不再读（PRD §8.4）
            c->peer_eof = true;
            // 必须立刻摘 EPOLLIN：否则 LT 会持续上报 EPOLLIN，而 on_readable 入口的
            // peer_eof 自查会立即 return —— 那就成了忙转（直到 ERR/HUP 到达才收场）。
            update_interest(c);
            break;
        }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        // 其它错误（ECONNRESET…）：标记断开，交由关闭路径收尾
        LOG_WARN_C(c->fd, c->peer_ip.c_str(), "recv 失败：%s", std::strerror(errno));
        c->closing = true;  // TODO(6C)：改走正式收尾路径（冲刷 out_buf 后关闭）
        // 同上：closing 置位后 compute_interest_locked() 会摘掉 EPOLLIN，避免这条
        // "只标记、不关闭"的临时路径在 LT 下忙转（6C 接上正式收尾后即可去掉）。
        update_interest(c);
        break;
    }

    // 已收数据照常解析（含 recv 返回 0 前的残留半帧）
    parse_loop(c);
}

void Server::parse_loop(const ConnectionPtr& c) {
    // 两阶段状态机（架构 §6.4 / PRD §5.5）。本步只切帧 + submit，**不做静态校验**
    // （6C 补）：在此之前，任何 magic/version/type/body_len 都被照单全收，
    // 包括超大 body_len 导致的 in_buf 无界增长 —— 仅限开发验证使用。
    for (;;) {
        // 阶段 1：凑满 10 字节头
        if (c->parse_state == ParseState::NEED_HEADER) {
            if (c->in_buf.size() - c->consumed < HEADER_SIZE) break;
            const uint8_t* p =
                reinterpret_cast<const uint8_t*>(c->in_buf.data()) + c->consumed;
            const uint32_t magic = be_get32(p);
            const uint8_t version = p[4];
            const uint8_t type = p[5];
            const uint32_t body_len = be_get32(p + 6);
            LOG_DEBUG_C(c->fd, c->peer_ip.c_str(),
                        "帧头 magic=0x%08x ver=%u type=0x%02x body_len=%u（6B 不校验）",
                        magic, static_cast<unsigned>(version), static_cast<unsigned>(type),
                        body_len);
            // TODO(6C)：静态校验（§8.1）—— magic/version/type 合法性 +
            //           body_len ≤ MAX_BODY_LEN；失败 → send_error + closing + 停止解析。

            c->pending_type = type;
            c->pending_body_len = body_len;
            c->consumed += HEADER_SIZE;
            c->parse_state = ParseState::NEED_BODY;
        }

        // 阶段 2：凑满 body。头刚读完时两个 if 会顺序执行，直接落到这里，正确。
        if (c->parse_state == ParseState::NEED_BODY) {
            if (c->in_buf.size() - c->consumed < c->pending_body_len) break;

            // body 从 in_buf 拷一份（架构 §15-D12）：换取任务生命周期与 in_buf 解耦。
            std::string body(c->in_buf.data() + c->consumed, c->pending_body_len);
            c->consumed += c->pending_body_len;
            c->parse_state = ParseState::NEED_HEADER;

            MsgTask task;
            task.type = c->pending_type;
            task.body = std::move(body);
            LOG_DEBUG_C(c->fd, c->peer_ip.c_str(), "submit type=0x%02x body=%zu B",
                        static_cast<unsigned>(task.type), task.body.size());
            // 队列满时阻塞 IO 线程（PRD §8.2 既定语义，接受）
            pool_.submit(c, std::move(task));
        }
    }

    // in_buf 压缩（架构 §6.4 定死的规则）：半帧残留 memmove 到头部 + resize + consumed=0。
    // 放在 parse_loop 末尾，保证下次 recv 从干净的尾部追加。
    if (c->consumed > 0) {
        if (c->consumed >= c->in_buf.size()) {
            c->in_buf.clear();
        } else {
            const std::size_t rest = c->in_buf.size() - c->consumed;
            std::memmove(c->in_buf.data(), c->in_buf.data() + c->consumed, rest);
            c->in_buf.resize(rest);
        }
        c->consumed = 0;
    }
}

void Server::update_interest(const ConnectionPtr& c) {
    // 6B 临时接线：自取 conns_mu_ + send_mu，按 compute_interest_locked() 算出目标
    // 掩码，变了才 MOD 并回写 epoll_events（架构 §6.1）。
    // TODO(6C)：补"conns_mu_ 下核对存活"，并把掩码落地部分抽成 apply_interest_locked()
    //           供 send_frame 复用（本步唯一调用点在 IO 线程、连接必在表内，故未核对）。
    std::lock_guard<std::mutex> g1(conns_mu_);
    std::lock_guard<std::mutex> g2(c->send_mu);  // 锁序：conns_mu_ → send_mu（§4.2）
    const uint32_t want = c->compute_interest_locked();
    if (want == c->epoll_events) return;
    epoll_event ev{};
    ev.events = want;
    ev.data.fd = c->fd;
    if (::epoll_ctl(epfd_, EPOLL_CTL_MOD, c->fd, &ev) < 0) {
        LOG_WARN_C(c->fd, c->peer_ip.c_str(), "epoll_ctl(MOD 0x%x) 失败：%s", want,
                   std::strerror(errno));
        // MOD 失败后 epoll_events 保持旧值，下次仍会尝试；不视为致命。
        return;
    }
    c->epoll_events = want;
}

void Server::close_connection_brutal(const ConnectionPtr& c) {
    // 6B 临时收尾（TODO(6C)：由 finalize() 取代后删除）：
    //   EPOLL_CTL_DEL + close(fd) + 从 conns_ 摘除 + fd 置 -1，不冲刷、不移交。
    // 只在 IO 线程调用；本步 execute() 还是占位（不建 FileSession），故无临时文件要
    // unlink —— 真有会话时 ~FileSession::cleanup() 仍会兜底。
    // TODO(6C)：finalize() 必须在此基础上前置 FileSession::cleanup()
    //           （close tmp_fd + unlink tmp_path），否则会话中断时残留 .part 文件。
    if (c->closed.exchange(true)) return;  // 幂等：已收尾则直接返回

    const int fd = c->fd;  // 置 -1 前留存，供日志与 close 使用
    {
        std::lock_guard<std::mutex> g(conns_mu_);
        const auto it = conns_.find(fd);
        if (it != conns_.end() && it->second.get() == c.get()) {
            ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
            conns_.erase(it);
        }
        // fd 的 close 与置 -1 都在 conns_mu_ 下完成（架构 §7.1 的所有权约定）；
        // 置 -1 是为了让 ~Connection 的兜底 close 不会二次关闭已被复用的 fd 号。
        if (fd >= 0) {
            ::close(fd);
            c->fd = -1;
        }
    }
    LOG_INFO_C(fd, c->peer_ip.c_str(), "连接关闭");
}

// ---------------------------------------------------------------------------
// 占位实现（6C/6D 落地；本步不参与任何实际路径）
// ---------------------------------------------------------------------------

void Server::on_writable(const ConnectionPtr& /*c*/) {
    // TODO(6C)：直发 out_buf；发空后按 peer_eof 决定 finalize（架构 §6.2/§6.5）
}

void Server::close_connection(const ConnectionPtr& /*c*/) {
    // TODO(6C)：四类关闭路径的收尾动作（架构 §7.5 / §9）
}

bool Server::send_frame(const ConnectionPtr& /*c*/, std::string_view /*bytes*/) {
    // TODO(6C)：conns_mu_ → send_mu → append_and_send_locked → apply_interest_locked
    //           （架构 §6.5）。占位返回 true = "无硬错误"。
    return true;
}

void Server::apply_interest_locked(const ConnectionPtr& /*c*/) {
    // TODO(6C)：按 compute_interest_locked() 结果做 EPOLL_CTL_MOD 并回写
    //           c->epoll_events（架构 §6.1/§6.5）。前置：conns_mu_ + send_mu 已持。
}

void Server::finalize(const ConnectionPtr& /*c*/) {
    // TODO(6C)：幂等收尾 —— EPOLL_CTL_DEL + close + FileSession 清理 + fd 置 -1
    //           + 从 conns_ 摘除（架构 §7.4）
}

void Server::on_grace_timeout() {
    // TODO(6D)：宽限到期 → 强关全部残余连接（架构 §11.2 步骤 3）
}
