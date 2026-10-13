// server.cpp — Server 实现（步骤 6A：启动/退出基础设施 + 主循环骨架）
// 依据：架构文档 §6.1（epoll 集合）、§6.2（事件循环）、§6.6（eventfd/signalfd）、
//       §11.1（启动序列）、§11.2（优雅退出）、§15-D2/D5/D8/D14；
//       PRD §8.4（SIGPIPE）、§10.2（退出码）、§11（日志）、§12（生命周期）。
//
// 本文件在此步只实现构造/析构/run/request_stop 与三个内部 fd 的处理
// （on_accept、drain_eventfd、handle_signal）；其余方法为占位，TODO 标注了
// 负责的子步骤与架构章节。占位期间 run() 不参与任何连接事件的分发。

#include "server.h"

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

    // 连接表随本类销毁（~Connection 兜底 close(fd)）。6C 起 finalize 会先行摘除；
    // 本步 conns_ 恒为空。
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

            // TODO(6B)：按架构 §6.2 顺序分发（6A 阶段连接表恒为空，走不到这里）
            //   EPOLLERR|EPOLLHUP → disconnect(immediate)
            //   EPOLLRDHUP        → peer_eof 置位 + update_interest；out 空则 finalize
            //   EPOLLOUT          → on_writable(c)
            //   EPOLLIN           → on_readable(c)
            (void)evs[i].events;
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
// 本步已实现的三个事件处理
// ---------------------------------------------------------------------------

void Server::on_accept() {
    // 6A 占位：accept4 拿到 fd 后立即 close —— 只验证 accept 通路（LT 排空语义、
    // EMFILE 退让）跑通且不崩；不建 Connection、不进 conns_、不注册 epoll。
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
        stats_.total_conns.fetch_add(1, std::memory_order_relaxed);
        LOG_DEBUG("accept fd=%d（6A 占位：立即关闭）", fd);
        // TODO(6B)：make_shared<Connection>(fd, inet_ntop(sa), ntohs(sa.sin_port))
        //           → conns_[fd] = c → EPOLL_CTL_ADD(EPOLLIN|EPOLLRDHUP) → LOG_INFO_C
        ::close(fd);
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
// 占位实现（6B/6C/6D 落地；本步不参与任何实际路径）
// ---------------------------------------------------------------------------

void Server::on_readable(const ConnectionPtr& /*c*/) {
    // TODO(6B)：recv 排空到 EAGAIN（256 KiB 块）→ parse_loop → submit（架构 §6.2/§6.4）
}

void Server::on_writable(const ConnectionPtr& /*c*/) {
    // TODO(6B)：直发 out_buf；发空后按 peer_eof 决定 finalize（架构 §6.2/§6.5）
}

void Server::parse_loop(const ConnectionPtr& /*c*/) {
    // TODO(6B)：NEED_HEADER / NEED_BODY 两阶段状态机 + 静态校验 + 组 MsgTask（架构 §6.4）
}

void Server::close_connection(const ConnectionPtr& /*c*/) {
    // TODO(6C)：四类关闭路径的收尾动作（架构 §7.5 / §9）
}

bool Server::send_frame(const ConnectionPtr& /*c*/, std::string_view /*bytes*/) {
    // TODO(6C)：conns_mu_ → send_mu → append_and_send_locked → apply_interest_locked
    //           （架构 §6.5）。占位返回 true = "无硬错误"。
    return true;
}

void Server::update_interest(const ConnectionPtr& /*c*/) {
    // TODO(6C)：自取 conns_mu_ + send_mu 后调 apply_interest_locked（架构 §6.1）
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
