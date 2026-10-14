// server.cpp — Server 实现（6A 基础设施 + 6B 连接事件 + 6C1 发送路径 + 6C2 收尾路径
//              + 6D 优雅退出）
// 依据：架构文档 §6.1（epoll 集合）、§6.2（事件循环）、§6.3（accept）、§6.4（接收与
//       解析）、§6.5（发送路径）、§6.6（eventfd/signalfd）、§7.4（finalize 与延迟
//       收尾移交）、§7.5（四类关闭路径）、§11.1（启动序列）、§11.2（优雅退出）、
//       §15-D2（LT）、§15-D3（宽限 3 秒）、§15-D4（IO 线程独占 ADD/DEL）、§15-D5、
//       §15-D8（tmp 权限）、§15-D12（body 拷贝一次）、§15-D14（EMFILE 退让）；
//       PRD §8.4（SIGPIPE）、§10.2（退出码）、§11（日志）、§12（生命周期）。
//
// 6D 范围：handle_signal 软停（第一次信号：shutting_down_ + 摘 listen_fd + 记宽限
// 截止；第二次信号：硬停）/ run() 宽限 timeout / on_grace_timeout（强关残余连接 +
// pool_.shutdown() + 消费待收尾移交）/ run() 退出后 join 线程池。
// 待落地（TODO 标注了负责的子步骤）：
//   - 静态校验与错误帧 send_error（第 8 步；recv 硬错误目前只置 closing）；
//   - worker 侧 dispatch（execute 仍是占位：任务被取出后丢弃）→ 第 7 步。

#include "server.h"

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
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
#include <vector>

#include "log.h"

namespace {

// epoll_wait 单批事件上限（架构 §6.2 的 kMaxEvents）
constexpr int kMaxEvents = 256;

// recv 单次读入量（架构 §6.4 的 kRecvChunk）：一次 recv 最多预留并收取 256 KiB。
// 走"resize 预留 → recv 直写 in_buf 尾部 → 按实际长度收缩"，故不需要 256 KiB
// 的栈/thread_local 中转缓冲（架构 §6.4 原文即此写法，一次拷贝入 buf）。
constexpr std::size_t kRecvChunk = 256U * 1024U;

// 退出宽限秒数（架构 §15-D3 / PRD §12）：从收到第一个 SIGINT/SIGTERM 起算，
// 期间只停止 accept，已建立连接照常处理；到期强关残余连接。
constexpr unsigned kGraceSeconds = 3;

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
// 主循环（架构 §6.2 骨架 + §11.2 宽限 timeout）
// ---------------------------------------------------------------------------

void Server::run() {
    epoll_event evs[kMaxEvents];
    while (running_) {
        // 宽限期内必须用"距 deadline 的剩余毫秒"取代 -1（架构 §11.2 步骤 2）：
        // 已建立连接若一直安静，epoll_wait(-1) 永远不会返回，宽限就永远无法到期。
        // 事件分发本身不变 —— 宽限期内照常收/发/处理（"允许完成当前文件"）。
        int timeout_ms = -1;
        if (shutting_down_) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= grace_deadline_) {
                on_grace_timeout();  // 内部置 running_ = false（架构 §11.2 步骤 3–4）
                break;
            }
            timeout_ms = static_cast<int>(
                std::chrono::duration_cast<std::chrono::milliseconds>(grace_deadline_ - now)
                    .count());
            if (timeout_ms <= 0) timeout_ms = 1;  // 亚毫秒剩余：至少 1ms，避免忙转
        }

        const int n = ::epoll_wait(epfd_, evs, kMaxEvents, timeout_ms);
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
                close_connection(c);  // §7.5：直接断开，不冲刷
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
                    close_connection(c);  // §7.5：out_buf 空 → 立即收尾
                    continue;
                }
                // out_buf 非空：EPOLLOUT 已由上面的 update_interest 按 out_buf 挂上，
                // 交给 on_writable 冲刷空后再收尾（§7.5 的 RDHUP 行）。
            }

            if (evs[i].events & EPOLLOUT) {
                on_writable(c);  // 冲刷 out_buf；发空且 peer_eof → 收尾
            }

            if (evs[i].events & EPOLLIN) {
                on_readable(c);
            }
        }
    }

    // 无条件落地"已退出"状态。epoll_wait 硬错误那条 break 是循环内唯一不置 running_
    // 的退出路径（另外三条：第二次信号 / request_stop / on_grace_timeout 都会置），
    // 这一行补上它，维持不变式 "run() 返回 ⇒ running_ == false"。纯粹为不变式服务：
    // 循环已经结束，没有任何逻辑依赖这一行的取值。
    running_ = false;

    // 退出前的线程池收尾：宽限到期路径已在 on_grace_timeout() 里 shutdown 过（幂等），
    // 这里兜住 request_stop()（硬停）/ epoll_wait 出错这两条**不走宽限**的路径 ——
    // 否则 worker 线程会活着撑到 ~Server，期间仍可能触碰连接对象。
    pool_.shutdown();

    // 退出统计行（PRD §11 / §12）
    LOG_INFO("退出：累计连接 %llu，成功文件 %llu，失败文件 %llu",
             static_cast<unsigned long long>(stats_.total_conns.load()),
             static_cast<unsigned long long>(stats_.files_ok.load()),
             static_cast<unsigned long long>(stats_.files_fail.load()));
}

void Server::request_stop() {
    // 硬停：立即退出，不走宽限。信号路径（handle_signal 的第一次信号）走软停。
    // 测试/强制停止用。
    running_ = false;
    write_eventfd(eventfd_);  // 唤醒阻塞在 epoll_wait(-1) 的 IO 线程（架构 §6.6）
}

// ---------------------------------------------------------------------------
// accept 与两个内部 fd（6A/6B：on_accept 已是 6B 的完整版）
// ---------------------------------------------------------------------------

void Server::on_accept() {
    // 宽限期内不再 accept（架构 §11.2 步骤 1"不接收新连接"）。摘 listen_fd 只能
    // 阻止**后续**的 epoll_wait 报它 —— 同一批事件里可能已经躺着一条更早就绪的
    // EPOLLIN（epoll_wait 早已把事件拷进用户数组），这条漏网事件不能再放进新连接。
    if (shutting_down_) return;

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
            // 已入表但未注册成功：走统一收尾路径（刚 accept 的连接没有 worker 在跑，
            // 故 close_connection 会直通 finalize，不会走移交）
            close_connection(c);
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

    // 消费 worker 移交的延迟收尾（架构 §7.4 时间线）：
    // wake_io_cb_ 写 eventfd ↔ 这里的 take_pending 一一对应；take_pending 是 swap
    // 语义（列表副本已从 ThreadPool 摘走），故 finalize 里做 unlink/close 这类
    // 可能阻塞的动作不会挡住 worker。
    std::vector<ConnectionPtr> pending;
    pool_.take_pending(pending);
    for (const auto& c : pending) {
        finalize(c);
    }
}

void Server::handle_signal() {
    // 排空 signalfd（LT：不排空会忙转），只留第一个信号号给日志用。
    //
    // 关于"第二次 Ctrl+C"：标准信号不排队，同一个信号在 pending 期间不会叠加第二条
    // 记录（内核直接丢弃）。人手连按能否命中取决于 IO 线程是否被卡住：
    //   - 空闲时 IO 线程立刻排空，两次 Ctrl+C 几乎总能读到两条；
    //   - 背压下（IO 线程卡在 pool_.submit()）排空被推迟，两次同号信号可能被内核
    //     合并成一条，只走满宽限。
    // 不同号信号不受此限（INT 和 TERM 是两条不同记录），但本函数的语义只把"已在
    // 宽限中再次收到信号"当强制退出信号。
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
    if (signo == 0) return;  // 竞态下读空：无可处理信号，主循环照常

    if (shutting_down_) {
        // 第二次信号：不等宽限，直接强制退出（架构 §11.2 的软停 + PRD §12）。
        // 连接不做收尾：由 ~Server 的连接表清理兜底（~Connection close fd /
        // ~FileSession unlink tmp）。
        // 这里的 write_eventfd 本可省（本函数就跑在 IO 线程上，返回后
        // while (running_) 自然不再成立），保留它是为了与 request_stop 同形：
        // 无论谁置的 running_，都保证有一个 eventfd 令牌把可能阻塞的 epoll_wait
        // 叫醒，代价只是一次非阻塞 write。
        LOG_WARN("收到第二个信号 %u，强制退出", signo);
        running_ = false;
        write_eventfd(eventfd_);
        return;
    }

    // 第一次信号 → 软停（架构 §11.2 步骤 1–2）：
    //   1) 摘 listen_fd 停止 accept（内核 backlog 里的半连接在进程退出时被 RST，
    //      架构 §11.2 末段 —— MVP 接受）；
    //   2) 记宽限截止；run() 此后用剩余毫秒当 epoll_wait 的 timeout。
    // 已建立连接不在这里动：宽限期内它们照常收/发/处理（PRD §12）。
    LOG_INFO("收到信号 %u，进入 %u 秒优雅退出宽限", signo, kGraceSeconds);
    shutting_down_ = true;
    ::epoll_ctl(epfd_, EPOLL_CTL_DEL, listen_fd_, nullptr);
    grace_deadline_ = std::chrono::steady_clock::now() +
                      std::chrono::seconds(kGraceSeconds);
}

// ---------------------------------------------------------------------------
// 6B/6C1：接收、解析与兴趣位（架构 §6.2 / §6.4 / §6.1）
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
        c->closing = true;
        // TODO(第 8 步)：接上 send_error 后，这里改走 §7.5 的"协议/会话错误"正式路径
        // —— 冲刷 out_buf 空后 close_connection（而不是只置 closing、靠 ERR/HUP 兜底）。
        // 现在这样写是临时态：closing 置位后 compute_interest_locked() 会摘掉 EPOLLIN，
        // 故不会在 LT 下忙转，但连接要等 EPOLLERR|EPOLLHUP 到达才真正收尾。
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
            // TODO(6C2)：静态校验（§8.1）—— magic/version/type 合法性 +
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
    // 架构 §6.5（路线 C）：自取 conns_mu_ → send_mu，先核对存活，再把掩码落地。
    // 存活核对是必需的：fd 号可能在 close 后被复用给新连接，此时 conns_[fd] 里
    // 是**另一个** Connection（it->second.get() != c.get()），MOD 会改错对象。
    std::lock_guard<std::mutex> g1(conns_mu_);
    const auto it = conns_.find(c->fd);
    if (it == conns_.end() || it->second.get() != c.get()) return;
    std::lock_guard<std::mutex> g2(c->send_mu);  // 锁序：conns_mu_ → send_mu（§4.2）
    apply_interest_locked(c);
}

void Server::apply_interest_locked(const ConnectionPtr& c) {
    // 前置：调用方已持 conns_mu_ + send_mu —— 本函数不再取锁，故可被 send_frame
    // 在持锁临界区内直接复用（架构 §6.5 的"避免对 conns_mu_ 递归加锁"）。
    // 掩码计算委托 Connection::compute_interest_locked()（纯计算，§6.1 公式）；
    // 与已注册掩码相同则跳过 epoll_ctl（架构 §6.1 末条）。
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

// ---------------------------------------------------------------------------
// 6C1：发送路径（架构 §6.5）
// ---------------------------------------------------------------------------

bool Server::send_frame(const ConnectionPtr& c, std::string_view bytes) {
    // 架构 §6.5：worker 与 IO 都可能调用。锁序 conns_mu_ → send_mu（§4.2）。
    // 返回 false = 硬发送错误（EPIPE/ECONNRESET…）；**标记动作由调用方在锁外完成**
    // （state_mu 下置 closed / disconnect_requested）—— 持 send_mu 时反向取
    // state_mu 即死锁，故这里只报错、不标记。热路径：正常路径不打日志。
    std::lock_guard<std::mutex> g1(conns_mu_);
    const auto it = conns_.find(c->fd);
    if (it == conns_.end() || it->second.get() != c.get()) {
        return true;  // 已死 / fd 号已复用：无需发送也无需标记，按"非硬错误"返回
    }
    std::lock_guard<std::mutex> g2(c->send_mu);
    if (!c->append_and_send_locked(bytes)) {
        // 硬错误：不在此处标记断开；也不更新兴趣位——连接即将断开，
        // EPOLL_CTL_DEL 由 IO 侧的 finalize 负责（架构 §6.5）。
        return false;
    }
    // 直发后 out_buf/paused_in 可能变化 → 落地新掩码（挂/摘 EPOLLOUT、高水位摘 EPOLLIN）
    apply_interest_locked(c);
    return true;
}

// ---------------------------------------------------------------------------
// 6C2：收尾路径（架构 §7.4 延迟收尾移交 / §7.5 四类关闭触发）
// ---------------------------------------------------------------------------

void Server::close_connection(const ConnectionPtr& c) {
    // IO 侧统一关闭入口。步骤固定为：置 closed → 摘 epoll → defer 判定后二选一。
    bool defer;
    {
        // closed 与 task_running 必须在同一临界区里读改写：execute() 入口也在
        // state_mu 下"先查 closed 再置 task_running"（架构 §8），二者靠这把锁串行化，
        // 因此本函数返回后不可能再有 worker 开始碰这个会话（I1/I2）。
        std::lock_guard<std::mutex> g(c->state_mu);
        c->closed = true;  // 逻辑死亡：worker 取出任务时据此丢弃
        defer = c->task_running;
        if (defer) {
            c->disconnect_requested = true;  // worker 收尾时把 c 移交回 IO（§7.4）
        }
    }

    // 先摘 epoll（无论 defer 与否）：不再收这个 fd 的事件。fd 本身先不关——
    // defer 路径下 worker 可能正在 write(session.tmp_fd) 或 send；若此时 close，
    // fd 号被 accept/打开新文件复用后，worker 会把数据写进无关文件（§7.4 的"为什么"）。
    {
        std::lock_guard<std::mutex> g(conns_mu_);
        const auto it = conns_.find(c->fd);
        if (it != conns_.end() && it->second.get() == c.get()) {
            ::epoll_ctl(epfd_, EPOLL_CTL_DEL, c->fd, nullptr);  // ENOENT 忽略
        }
    }

    if (!defer) {
        finalize(c);  // 无任务在跑：就地收尾，无移交开销（§7.4 末段）
    }
    // defer 路径：等 worker 收尾时 push 进 pending_finalize_ + 写 eventfd，
    // IO 线程在 drain_eventfd() 里 finalize（§7.4 时间线）。
}

void Server::finalize(const ConnectionPtr& c) {
    // 架构 §7.4 / I3：仅 IO 线程调用，幂等。
    // 前提（I2）：调用方已在 state_mu 下确认 closed==true 且 task_running==false
    // —— close_connection 保证前者，worker 的收尾移交保证后者。故此处可以安全触碰
    // FileSession（§7.1：task_running==false 时只有 IO 的 finalize 会清理它）。
    //
    // 关键：EPOLL_CTL_DEL / conns_.erase / close / fd=-1 全部在同一个 conns_mu_
    // 临界区内完成 —— send_frame / update_interest 的存活核对依赖这个原子性，
    // 否则会出现"表外但 fd 仍有效"或"表内但 fd 已 close"的中间态（§7.6 R2）。
    std::lock_guard<std::mutex> g1(conns_mu_);
    if (c->finalized) return;
    c->finalized = true;

    const int fd = c->fd;  // 置 -1 前留存（日志与 close 用）
    if (fd >= 0) {
        ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);  // 可能已 DEL 过：ENOENT 忽略
    }
    const auto it = conns_.find(fd);
    if (it != conns_.end() && it->second.get() == c.get()) {
        conns_.erase(it);  // 此后 lookup 不再命中（I4 的陈旧事件防线）
    }

    // 清理 FileSession：close(tmp_fd) + unlink(tmp_path)（PRD §6.3 / AC-4）。
    // cleanup() 幂等，且不判 active 也安全（active==false 时最坏是一次空转，
    // 但能兜住"tmp_path 已置而 active 未置"这类半初始化残留）。锁序
    // conns_mu_ → state_mu（§4.2 允许的嵌套）。
    {
        std::lock_guard<std::mutex> g2(c->state_mu);
        c->session.cleanup();
    }

    // 清扫仍排在该连接上的任务并归还 queued_total_ 额度（架构 §5.4/§5.5 不变式）。
    // 独立作用域：state_mu 与 pool.mu_ 不同链，不得同时持有（§4.2）。
    // 不移交时若不清扫，额度会永久偏高，最终把 submit() 的 IO 线程卡死。
    pool_.drain_conn_tasks(c);

    c->closed = true;  // 通常早已为 true（drain_eventfd 路径亦然），此处兜底
    if (fd >= 0) {
        ::close(fd);   // I2：conn fd 的唯一 close 点
        c->fd = -1;    // 防 ~Connection 二次 close（fd 号可能已被内核复用）
    }
    LOG_INFO_C(fd, c->peer_ip.c_str(), "连接关闭");
}

// ---------------------------------------------------------------------------
// 6C2：冲刷（架构 §6.2 / §6.5）
// ---------------------------------------------------------------------------

void Server::on_writable(const ConnectionPtr& c) {
    // EPOLLOUT 到来说明内核发送缓冲又有空间：冲刷 out_buf。
    // 复用 send_frame 的发送循环（append 空串是 no-op，只跑直发段），避免重写一份
    // "只发不 append"的实现；它内部顺带落地新掩码（发空即摘 EPOLLOUT）。
    if (!send_frame(c, {})) {
        // 硬错误：send_frame 已返回、此刻不持任何锁，故可安全取 state_mu 标记断开
        // （架构 §6.5：标记动作必须在 send_frame 之外——持 send_mu 时反向取
        // state_mu 即死锁）。
        {
            std::lock_guard<std::mutex> g(c->state_mu);
            c->closed = true;                // 逻辑死亡：worker 丢弃余下任务
            c->disconnect_requested = true;  // worker 收尾时移交 IO
        }
        // 收口走统一入口：无任务在跑就立即 finalize，有任务在跑则保持 fd 打开
        close_connection(c);
        return;
    }

    // 冲刷后检查能否收尾（§7.5 的 RDHUP 行）：peer_eof 且 out_buf 已空 → 关闭。
    //
    // 这里**不**自己判 task_running：close_connection 内部已有 defer 判定（任务在跑
    // → 置 disconnect_requested 移交；不在跑 → 就地 finalize）。提前 return 会漏掉
    // "任务在跑 + peer_eof + out_buf 空"这一组合——那个 worker 此后没有任何唤醒，
    // 连接只剩 EPOLLRDHUP，收尾就得依赖"RCV_SHUTDOWN 保持时内核会持续上报 RDHUP"
    // 这种细微语义兜着。走 defer 则由 worker 收尾时主动把 c 推进 pending_finalize_。
    bool need_close = false;
    {
        std::lock_guard<std::mutex> g(c->state_mu);
        std::lock_guard<std::mutex> g2(c->send_mu);  // 锁序 state_mu → send_mu（§4.2）
        need_close = c->peer_eof.load() && c->out_empty_locked();
    }
    if (need_close) {
        close_connection(c);
    }
}

// ---------------------------------------------------------------------------
// 6D：宽限到期（架构 §11.2 步骤 3–4）
// ---------------------------------------------------------------------------

void Server::on_grace_timeout() {
    // 仅 IO 线程调用（run() 的 timeout 分支就地调用，见 server.h）。
    LOG_INFO("宽限到期，开始强制关闭剩余连接");

    // 1) 快照连接表。必须先快照再逐个处理，两个原因：
    //    ① 后面的 close_connection/finalize 会 erase conns_ —— 持迭代器遍历即失效；
    //    ② finalize 自己会取 conns_mu_ —— 在持锁状态下调用即自锁（std::mutex 不可重入）。
    std::vector<ConnectionPtr> all;
    {
        std::lock_guard<std::mutex> g(conns_mu_);
        all.reserve(conns_.size());
        for (const auto& kv : conns_) all.push_back(kv.second);
    }

    // 2) 逐个走统一关闭入口（§7.5）：无任务在跑的当即 finalize；有任务在跑的置
    //    disconnect_requested，由该 worker 收尾时把连接移交到 pending_finalize_（§7.4）。
    for (const auto& c : all) {
        close_connection(c);
    }

    // 3) 停线程池：join 全部 worker。worker 收尾时会把带 disconnect_requested 的连接
    //    推进 pending_finalize_。**这里可能阻塞**——worker 正在写大文件时要等它做完；
    //    宽限只是"停止 accept 后给已建立连接的机会"，不保证完成（PRD §12），接受。
    pool_.shutdown();

    // 4) 消费宽限期内（含上面 join 期间）worker 移交的全部收尾请求。
    //    take_pending 是 swap 语义：列表副本已从 ThreadPool 摘走，此处的
    //    finalize（close + unlink）不会挡住任何 worker（它们已全部 join 完）。
    std::vector<ConnectionPtr> pending;
    pool_.take_pending(pending);
    for (const auto& c : pending) {
        finalize(c);
    }

    // 5) 保险：conns_ 里理论上已无残留（步骤 2 覆盖了快照里的全部连接，accept 也已
    //    停止，快照之后不可能再入表）。真出现残留也按同一路径清掉；同样先快照再
    //    finalize（理由见步骤 1）。此时 worker 已全部 join，finalize 的前置
    //    "task_running == false"必然成立（I2），故可以直接调 finalize 而不必再过
    //    close_connection 的 defer 判定。
    {
        std::vector<ConnectionPtr> rest;
        {
            std::lock_guard<std::mutex> g(conns_mu_);
            rest.reserve(conns_.size());
            for (const auto& kv : conns_) rest.push_back(kv.second);
        }
        for (const auto& c : rest) {
            finalize(c);
        }
    }

    running_ = false;
}
