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
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <random>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
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

// ERROR/ACK 的 msg 上限（PRD §9.2 / 架构 §8.6）：超长截断，不报错。
constexpr std::size_t kMaxErrMsgLen = 256;

// tmp 文件名里随机前缀的位数（PRD §7.2：8 位十六进制）。
constexpr int kTmpRandHexDigits = 8;

// ACK/ERROR 共用同一 body 布局（架构 §8.6）：u32 code BE | u32 msg_len BE | msg(≤256B)。
// 两种消息只有 type 不同，故 body 构造抽出来共用。
std::string status_body(ErrCode code, const std::string& msg) {
    const std::string m = msg.size() > kMaxErrMsgLen ? msg.substr(0, kMaxErrMsgLen) : msg;
    std::string body(8 + m.size(), '\0');
    auto* p = reinterpret_cast<uint8_t*>(body.data());
    be_put32(p, static_cast<uint32_t>(code));
    be_put32(p + 4, static_cast<uint32_t>(m.size()));
    if (!m.empty()) {
        std::memcpy(body.data() + 8, m.data(), m.size());
    }
    return body;
}

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
      // execute_cb_ 捕获 this：两个回调都只在 Server 构造完成之后才可能被调用
      // （唯一的投递口 submit 只由 IO 线程在 run() 里调用），故捕获时机安全；
      // 详见 server.h 里 pool_ 声明处的说明。
      pool_(cfg.n_threads, cfg.queue_cap, cfg.conn_queue_cap,
            [this] { write_eventfd(eventfd_); },
            [this](const ConnectionPtr& c, const MsgTask& t) { execute(c, t); }) {
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

// ---------------------------------------------------------------------------
// 7A：worker 侧消息处理（架构 §8.1–§8.3）
//
// 本节所有 handler 共用的锁规矩（与 §7.1 / §4.2 一致，各函数内不再重复）：
//   - 状态机字段 state：在 state_mu 下读改写（它按声明属跨线程字段）。
//   - FileSession 的其余字段（fd / tmp_path / expected_* / received / md5 / active …）
//     **不加锁**：任务在跑期间 task_running == true，按 §7.1 会话由本 worker 独占；
//     IO 线程唯一触碰会话的地方是 finalize，而它只在 task_running == false 时发生
//     （I2），二者不可能重叠。
//   - 文件 IO（open/write/close/unlink/statvfs）一律在锁外（§4.2）。
//   - **绝不**在持 state_mu 时调 send_* / update_interest：它们要取 conns_mu_
//     → send_mu，而合法锁序是 conns_mu_ → state_mu → send_mu（§4.2），反向即死锁。
//     故每个 handler 都是"锁内取判定，出锁再动作"。
// ---------------------------------------------------------------------------

void Server::execute(const ConnectionPtr& c, const MsgTask& t) {
    // 架构 §8 的 execute。入口的 closed 检查与 task_running 置位必须在同一临界区内：
    // I1 —— 入队后、执行前连接已死则直接丢弃本任务，绝不碰 session（额度由 worker_loop
    // 的死连接清扫路径归还）。task_running 的清零属 worker_loop 的收尾三步，此处不置。
    {
        std::lock_guard<std::mutex> g(c->state_mu);
        if (c->closed) return;
        c->task_running = true;  // 此后 session 归本 worker 独占（架构 §7.1）
    }
    dispatch(c, t);  // 全程独占 session；文件 IO 不持锁（架构 §4.2）
}

void Server::dispatch(const ConnectionPtr& c, const MsgTask& t) {
    switch (static_cast<MsgType>(t.type)) {
        case MsgType::FILE_META:  handle_file_meta(c, t.body);  break;
        case MsgType::FILE_CHUNK: handle_file_chunk(c, t.body); break;
        case MsgType::FILE_END:   handle_file_end(c, t.body);   break;  // TODO(7B)
        case MsgType::HEARTBEAT:  handle_heartbeat(c);          break;  // TODO(7C)
        default:
            // 未知 type 本应由 IO 侧静态校验拦掉（第 8 步）；这里兜底。
            // 可能发生在会话中途（连接已在 RECEIVING），fail_connection 会清掉 tmp
            // —— 对应 §9 的 BAD_TYPE 行"若 session.active"。
            fail_connection(c, ErrCode::BAD_TYPE, "unknown type");
            break;
    }
}

void Server::handle_file_meta(const ConnectionPtr& c, const std::string& body) {
    // ---- 步骤 1：状态机，仅 CONNECTED 合法（PRD §6.2 / §15-D6）----
    bool in_receiving;
    {
        std::lock_guard<std::mutex> g(c->state_mu);
        in_receiving = (c->state == SessionState::RECEIVING);
    }
    if (in_receiving) {
        fail_connection(c, ErrCode::BAD_TYPE, "meta while receiving");
        return;
    }

    // ---- 步骤 2：解析 + 精确核长（PRD §5.3；精确长度判定按 §8.1 属 worker 层）----
    if (body.size() < 2) {
        fail_connection(c, ErrCode::BAD_TYPE, "meta too short");
        return;
    }
    const auto* p = reinterpret_cast<const uint8_t*>(body.data());
    const std::size_t name_len = be_get16(p);  // ≤ 65535，下面的核长把它钉死在 ≤512
    if (body.size() != 2 + name_len + 8 + MD5_HEX_LEN) {
        fail_connection(c, ErrCode::BAD_TYPE, "meta length mismatch");
        return;
    }
    const std::string name = body.substr(2, name_len);
    const uint64_t expected_size = be_get64(p + 2 + name_len);
    // md5hex 只原样保存、不提前校验格式（架构 §15-D10）：
    // 大小写/长度是否合法留到 FILE_END 比对时自然判负（7B）。
    const std::string expected_md5hex = body.substr(2 + name_len + 8, MD5_HEX_LEN);

    // ---- 步骤 3：文件名清洗（PRD §7.3）----
    std::string safe_name;
    if (!sanitize_filename(name, safe_name)) {
        fail_connection(c, ErrCode::BAD_FILENAME, "bad filename");
        return;
    }

    // ---- 步骤 4：按对端 IP 建目录（PRD §7.1：延迟到首个 META 才建）----
    std::string dir;
    if (!ensure_client_dir(c->peer_ip, dir)) {
        const int e = errno;  // 先留存：std::string 的构造可能覆写 errno
        fail_connection(c, ErrCode::WRITE_FAILED, std::string("mkdir: ") + std::strerror(e));
        return;
    }

    // ---- 步骤 5：剩余空间（PRD §7.4；statvfs 失败也归 WRITE_FAILED）----
    if (!check_disk_space(dir, expected_size)) {
        fail_connection(c, ErrCode::WRITE_FAILED, "no space");
        return;
    }

    // ---- 步骤 6：tmp 文件（PRD §7.2 / §15-D7）：O_EXCL 撞名则换随机数重试 ≤5 次 ----
    const std::string final_path = dir + "/" + safe_name;
    int tmp_fd = -1;
    std::string tmp_path;
    for (int attempt = 0; attempt < 5; ++attempt) {
        tmp_path = gen_tmp_path(final_path);
        tmp_fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0600);
        if (tmp_fd >= 0) break;
        if (errno != EEXIST) break;  // 非撞名错误：换名字重试没有意义
    }
    if (tmp_fd < 0) {
        const int e = errno;
        fail_connection(c, ErrCode::WRITE_FAILED, std::string("open: ") + std::strerror(e));
        return;
    }

    // ---- 步骤 7：建立会话（复用前必已 cleanup，故这里是"整体重写"而非增量改）----
    c->session.active = true;
    c->session.filename = safe_name;
    c->session.tmp_path = tmp_path;
    c->session.final_path = final_path;
    c->session.fd = tmp_fd;
    c->session.expected_size = expected_size;
    c->session.received = 0;
    c->session.expected_md5hex = expected_md5hex;
    c->session.md5.reset();
    {
        std::lock_guard<std::mutex> g(c->state_mu);
        c->state = SessionState::RECEIVING;
    }

    // ---- 步骤 8：日志（PRD §11）；步骤 9：成功路径不回任何响应（只在 END 后回 ACK）----
    LOG_INFO_C(c->fd, c->peer_ip.c_str(), "meta: name=%s size=%llu md5=%s", safe_name.c_str(),
               static_cast<unsigned long long>(expected_size), expected_md5hex.c_str());
}

void Server::handle_file_chunk(const ConnectionPtr& c, const std::string& body) {
    // ---- 步骤 1：仅 RECEIVING（PRD §6.1）；顺带核对 active，防半初始化残留 ----
    bool active_session;
    {
        std::lock_guard<std::mutex> g(c->state_mu);
        active_session = (c->state == SessionState::RECEIVING) && c->session.active;
    }
    if (!active_session) {
        fail_connection(c, ErrCode::NO_SESSION, "no session");
        return;
    }

    // ---- 步骤 2：溢出（PRD §7.5 / 架构 §8.3）----
    if (c->session.received + body.size() > c->session.expected_size) {
        fail_connection(c, ErrCode::OVERFLOW, "chunk overflow");
        return;
    }

    // ---- 步骤 3：循环写满（EINTR 重试；ENOSPC 等一律 WRITE_FAILED）----
    const char* p = body.data();
    std::size_t remain = body.size();
    while (remain > 0) {
        const ssize_t n = ::write(c->session.fd, p, remain);
        if (n > 0) {
            p += n;
            remain -= static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        const int e = (n < 0) ? errno : EIO;  // n == 0：理论不可达，按 EIO 报
        fail_connection(c, ErrCode::WRITE_FAILED, std::string("write: ") + std::strerror(e));
        return;
    }

    // ---- 步骤 4：增量 MD5 + 计数（写满后才更新：失败路径不留下半截状态）----
    c->session.md5.update(body.data(), body.size());
    c->session.received += body.size();
}

void Server::handle_file_end(const ConnectionPtr& c, const std::string& body) {
    // TODO(7B)：架构 §8.4 —— received 核对 → fsync → close → md5 finalize → 比对 →
    //           rename → chmod → ACK(OK, 服务端 md5hex) → 回 CONNECTED；各失败分支
    //           清理 tmp 后 ERROR。本步（7A）不实现：收到 FILE_END 无响应，连接保持
    //           打开，临时文件留在盘上（由 finalize / 对端断开时清理）。
    (void)c;
    (void)body;
}

void Server::handle_heartbeat(const ConnectionPtr& c) {
    // TODO(7C)：架构 §8.5 —— 仅 CONNECTED 合法，回 ACK(OK)（msg 为空）；RECEIVING 中
    //           收到按 §15-D6 判 BAD_TYPE。本步（7A）不实现：收到 HEARTBEAT 无响应。
    (void)c;
}

void Server::fail_connection(const ConnectionPtr& c, ErrCode code, const std::string& msg) {
    // 会话/协议错误的统一出口（§7.5 前两行 / §9 表的"tmp 清理"列）。
    // cleanup() 幂等：NO_SESSION / BAD_FILENAME 这类"无会话"错误到这里是 no-op，
    // 正对应 §9 表里"无"那一格；有会话时它 close(tmp_fd) + unlink(tmp_path)。
    c->session.cleanup();
    {
        std::lock_guard<std::mutex> g(c->state_mu);
        c->state = SessionState::CONNECTED;  // 状态机回位（此后不会再解析到帧：closing 摘 EPOLLIN）
    }

    if (!send_error(c, code, msg)) {
        mark_disconnect_after_send_failure(c);  // 硬发送错误：按 §6.5 标记，不再做后续动作
        return;
    }
    c->closing = true;
    // closing → compute_interest_locked 摘掉 EPOLLIN；out_buf 非空（刚塞入的 ERROR）
    // → 保留 EPOLLOUT，由 on_writable 冲刷。冲刷空后收尾（§7.5 的"冲刷空后 finalize"）
    // 属第 8 步的接线，本步不接：过渡期连接靠对端 RDHUP/ERR 兜底，与 6B 临时行为一致。
    update_interest(c);
}

// ---------------------------------------------------------------------------
// 7A：协议构造与发送（架构 §8.6）
// ---------------------------------------------------------------------------

std::string Server::build_frame(MsgType type, std::string body) {
    // 帧 = 10B 头 + body。body 按值传入，调用方可以 std::move 进来，少一次拷贝。
    std::string frame(HEADER_SIZE + body.size(), '\0');
    auto* p = reinterpret_cast<uint8_t*>(frame.data());
    be_put32(p, PROTOCOL_MAGIC);
    frame[4] = static_cast<char>(PROTOCOL_VERSION);
    frame[5] = static_cast<char>(type);
    be_put32(p + 6, static_cast<uint32_t>(body.size()));
    if (!body.empty()) {
        std::memcpy(frame.data() + HEADER_SIZE, body.data(), body.size());
    }
    return frame;
}

bool Server::send_ack(const ConnectionPtr& c, ErrCode code, const std::string& msg) {
    return send_frame(c, build_frame(MsgType::ACK, status_body(code, msg)));
}

bool Server::send_error(const ConnectionPtr& c, ErrCode code, const std::string& msg) {
    return send_frame(c, build_frame(MsgType::ERROR_MSG, status_body(code, msg)));
}

void Server::mark_disconnect_after_send_failure(const ConnectionPtr& c) {
    // send_frame 返回 false 之后由调用方执行（架构 §6.5：标记动作必须在 send_frame
    // 之外——持 send_mu 时反向取 state_mu 即死锁）。worker 侧**不得**调
    // close_connection（IO 线程专用入口）：这里只标记，收尾由 worker_loop 的
    // "disconnect_requested → pending_finalize_" 移交路径交回 IO 线程执行。
    std::lock_guard<std::mutex> g(c->state_mu);
    c->closed = true;                // 逻辑死亡：后续任务取出即丢弃
    c->disconnect_requested = true;  // worker 收尾时把 c 推进 pending_finalize_（§7.4）
}

// ---------------------------------------------------------------------------
// 7A：文件系统工具（PRD §7.1–§7.4）
// ---------------------------------------------------------------------------

bool Server::sanitize_filename(const std::string& name, std::string& out) {
    // PRD §7.3：长度 1..512；不含 '\0'、'/'、'\\'；不含 <0x20 控制字符；
    // 不等于 "." / ".."；不以 '-' 开头（CLI 误用）。
    //
    // 关于"以 '.' 开头"：PRD §7.3 把它与 '-' 并列列为拒绝项（隐藏文件），本实现
    // **允许** .xxx（如 .gitignore），只拒绝恰好 "." / ".."（§7.3 末条）。
    // 理由：同步工具同步点文件是常见需求，而拒绝它们不带来任何安全收益——路径
    // 分隔符、NUL、控制字符与 "."/".." 都已单独拒绝，穿越无从谈起。
    // 注：这与 PRD §7.3 的"不以下列开头：`.`"一条不一致，需同步修订 PRD。
    if (name.empty() || name.size() > MAX_FILENAME_LEN) return false;
    if (name == "." || name == "..") return false;
    if (name.front() == '-') return false;
    for (const char raw : name) {
        const unsigned char ch = static_cast<unsigned char>(raw);
        if (ch == '\0' || ch == '/' || ch == '\\') return false;
        if (ch < 0x20) return false;  // 控制字符（含 \n \r \t）
    }
    out = name;
    return true;
}

bool Server::ensure_client_dir(const std::string& ip, std::string& out_dir) {
    // PRD §7.1：一级目录按客户端 IPv4 点分十进制命名，首个 META 时按需创建。
    // 已存在（EEXIST）视为成功——多连接/多文件共用同一目录是常态。
    out_dir = storage_dir_ + "/" + ip;
    if (::mkdir(out_dir.c_str(), 0755) < 0 && errno != EEXIST) return false;
    return true;
}

bool Server::check_disk_space(const std::string& dir, uint64_t need_bytes) {
    // PRD §7.4 / 架构 §8.2：size > avail − RESERVE(16MiB) → 拒绝。只检查这一次，
    // 中途不复查（写中途 ENOSPC 由 handle_file_chunk 的 write 错误分支兜住）。
    // 返回 false 有两种含义（statvfs 失败 / 空间不足），调用方统一报 WRITE_FAILED。
    //
    // 按架构原文的"size > avail − RESERVE"写，而**不**写成等价的
    // "need_bytes + RESERVE <= avail"：后者在 need_bytes 接近 UINT64_MAX 时无符号
    // 回绕成一个很小的数，会让声明了天文数字长度的帧（7A 还没有静态校验，body/size
    // 全由客户端给）绕过空间检查。这里先把 RESERVE 从 avail 里扣掉再做比较，两端都
    // 不会溢出。
    struct statvfs vfs {};
    if (::statvfs(dir.c_str(), &vfs) != 0) return false;
    const uint64_t avail = static_cast<uint64_t>(vfs.f_bavail) * vfs.f_frsize;
    constexpr uint64_t kReserve = 16ULL * 1024 * 1024;
    if (avail < kReserve) return false;  // 连保留量都不够，一律拒绝
    return need_bytes <= avail - kReserve;
}

std::string Server::gen_tmp_path(const std::string& final_path) {
    // PRD §7.2 / §15-D7：<final_path>.part.<8 位小写 hex>（D7 把 PRD 原文的 <fd> 去掉：
    // fd 在 open 之前不可知，唯一性改由 O_EXCL + 重试保证，语义等价）。
    // 随机源只需唯一性，不需要密码学强度：thread_local 的 mt19937 每线程只播种一次，
    // 避开每次调用构造引擎的开销；真撞名（同 IP 同文件并发）由调用方换随机数重试。
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    static thread_local std::uniform_int_distribution<uint32_t> dist(0, 0xFFFFFFFFu);
    char suffix[kTmpRandHexDigits + 1];
    std::snprintf(suffix, sizeof(suffix), "%08x", dist(rng));
    return final_path + ".part." + suffix;
}
