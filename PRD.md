# PRD — FSync 局域网文件同步服务器（MVP）

------

## 0. 文档信息

| 项       | 值                                 |
| :------- | :--------------------------------- |
| 产品名   | FSync Server                       |
| 版本     | v0.1 (MVP)                         |
| 状态     | 待实现                             |
| 目标读者 | 实现者（本人）                     |
| 关联     | 原 C# 文件同步系统的网络模块替代品 |

------

## 1. 项目定位与范围

**一句话定位**：基于 epoll 的局域网文件同步服务器的**核心网络层**，C++17 / Linux。不做 Android、不做 UI、不做 SQLite。

### 1.1 交付物（In Scope）

- **正式交付**：服务端二进制 + 源码（一个可 `make` 出可执行文件的工程）。
- **非正式交付**：`tools/test_client.cpp` 是开发/压测用工具，纳入代码库但不属于验收范围。

### 1.2 明确不做（Out of Scope，MVP）

- 断点续传
- 双向同步 / 目录扫描 / 冲突解决策略
- 心跳保活、空闲连接超时
- TLS / 认证 / 授权
- 持久化状态（无 SQLite、无索引文件）
- IPv6 监听
- Windows / macOS 支持
- 日志文件、日志轮转、metrics 端点

### 1.3 未来可扩展（保留设计余地，但 MVP 不实现）

- 断点续传（协议 type 段预留）
- 心跳（type 段预留 `HEARTBEAT`）
- 多存储后端（抽象 `FileSink` 接口，MVP 只实现本地文件）
- 认证握手（在 FILE_META 前插入握手消息）

------

## 2. 目标平台与构建

| 项            | 值                                                         |
| :------------ | :--------------------------------------------------------- |
| OS            | WSL2 + Ubuntu 22.04                                        |
| 编译器        | g++ 11（支持 C++17）                                       |
| 依赖          | 仅 libstdc++ + pthread                                     |
| 第三方库      | 无（MD5 自实现）                                           |
| 构建          | Makefile，目标：`all` / `debug` / `clean`                  |
| Release flags | `-O2 -Wall -Wextra -Wpedantic -pthread -std=c++17`         |
| Debug flags   | `-O0 -g3 -fsanitize=address,undefined -pthread -std=c++17` |

------

## 3. 术语

| 术语               | 定义                                         |
| :----------------- | :------------------------------------------- |
| 帧 (Frame)         | 一条完整协议消息：10B 头 + body              |
| 会话 (FileSession) | 一次完整的文件接收过程：META → CHUNK×N → END |
| IO 线程            | 唯一执行 `epoll_wait` 的线程                 |
| 工作线程           | 从线程池取任务的线程，处理完整消息           |
| 高水位 / 低水位    | out_buf 的流量控制阈值（8MB / 4MB）          |

------

## 4. 系统架构

### 4.1 线程模型

- **IO 线程（1 个）**：唯一线程执行 `epoll_wait`，处理 `EPOLLIN` / `EPOLLOUT` / `EPOLLRDHUP` / `EPOLLHUP` / `EPOLLERR`。
  - 负责：accept、recv 到完整帧、帧头校验（magic/ver/type/body_len）、组装消息、投递任务、发送 out_buf。
- **工作线程池（N 个）**：`N = std::thread::hardware_concurrency()`（可命令行覆盖）。
  - 负责：解析 body、文件 IO、MD5、rename、生成 ACK/ERROR。
- **每连接串行队列**：每条连接有自己的任务队列（FIFO），同一连接的消息严格按到达顺序处理；不同连接可并行。
  - 队列容量 64，满时阻塞 IO 线程 `submit()`。
- **全局线程池队列**：容量 1024（`--queue-cap`），满时阻塞 IO 线程。

### 4.2 数据流

socket → IO 线程 recv → in_buf → parse_loop 切出完整帧
       → 头校验 → 组装 (type, body) → enqueue_task(conn)
       → 工作线程 dispatch() → handle_xxx()
       → send_frame() 写入 conn.out_buf → IO 线程 EPOLLOUT 发送

### 4.3 锁序（全局单向，避免死锁）

conns_mu_  →  conn.state_mu  →  conn.send_mu任何代码路径不得反向获取。

------

## 5. 协议规范

### 5.1 帧格式（10 字节头）

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Magic (0x4653594E)                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   Version     |   MsgType     |         Body Length           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| 字段    | 偏移 | 长度 | 类型   | 说明                                   |
| :------ | :--- | :--- | :----- | :------------------------------------- |
| Magic   | 0    | 4    | u32 BE | 固定 `0x4653594E`（ASCII `"FSYN"`）    |
| Version | 4    | 1    | u8     | 固定 `0x01`                            |
| MsgType | 5    | 1    | u8     | 见 5.2                                 |
| BodyLen | 6    | 4    | u32 BE | `0 ≤ BodyLen ≤ 4 MiB`（`4*1024*1024`） |

**所有多字节整数一律大端。**

### 5.2 消息类型

| type   | 名称         | 方向 | body                                  |
| :----- | :----------- | :--- | :------------------------------------ |
| `0x01` | `FILE_META`  | C→S  | 见 5.3                                |
| `0x02` | `FILE_CHUNK` | C→S  | 原始字节，长度 1..4MiB                |
| `0x03` | `FILE_END`   | C→S  | 空（`BodyLen = 0`）                   |
| `0x20` | `HEARTBEAT`  | C→S  | 空。MVP：服务端回 `ACK(OK)`，msg 为空 |
| `0x80` | `ACK`        | S→C  | `u32 code | u32 msg_len | msg`        |
| `0x81` | `ERROR`      | S→C  | `u32 code | u32 msg_len | msg`        |

`ACK` 与 `ERROR` body 结构相同，`code` 语义见 §9。

### 5.3 FILE_META body

```
+---------------+---------------------+---------------+---------------+
| name_len (u16)| name (name_len 字节)| size (u64 BE) | md5hex (32B)  |
+---------------+---------------------+---------------+---------------+
```

| 字段       | 长度     | 约束                                     |
| :--------- | :------- | :--------------------------------------- |
| `name_len` | 2        | `1 ≤ name_len ≤ 512`                     |
| `name`     | name_len | UTF-8，清洗规则见 §7.3                   |
| `size`     | 8        | 文件总字节数，`0` 合法                   |
| `md5hex`   | 32       | 小写 ASCII hex，客户端期望的最终文件 MD5 |

`body_len` 必须等于 `2 + name_len + 8 + 32`，否则判为畸形帧。

### 5.4 成功 ACK

收到 `FILE_END` 且校验通过后，服务端回：

text

```
ACK { code = OK, msg = <服务端计算的 md5hex，32 字节> }
```



### 5.5 粘包 / 拆包

接收侧严格按：**先读 10B 头 → 校验 → 再读 `body_len` 字节**。IO 线程循环直到 `in_buf` 中不存在完整帧。

### 5.6 协议非法判定

以下任一成立 ⇒ **发 `ERROR(...)` 后立即关闭该连接**：

- `magic` 不匹配 → `BAD_MAGIC`
- `version` 不匹配 → `BAD_VERSION`
- `msg_type` 未知 → `BAD_TYPE`
- `body_len > 4MiB` → `BODY_TOO_BIG`
- `body_len` 与消息类型的最小/精确长度不符 → `BAD_TYPE`

------

## 6. 会话状态机

每条连接维护一个状态：

```
        ┌──────────────────────────────────────┐
        │                                      │
        ▼                                      │
  [CONNECTED] --FILE_META(校验通过)--> [RECEIVING] --FILE_END(校验通过/失败)--> [CONNECTED]
```

### 6.1 CONNECTED 状态

| 收到         | 动作                                                         |
| :----------- | :----------------------------------------------------------- |
| `FILE_META`  | 校验 name / size / 剩余空间 → 建立 FileSession → 进入 RECEIVING |
| `FILE_CHUNK` | 回 `ERROR(NO_SESSION)` 并关连接                              |
| `FILE_END`   | 回 `ERROR(NO_SESSION)` 并关连接                              |
| `HEARTBEAT`  | 回 `ACK(OK)`，msg 为空                                       |

### 6.2 RECEIVING 状态

| 收到         | 动作                                                         |
| :----------- | :----------------------------------------------------------- |
| `FILE_META`  | 回 `ERROR(BAD_TYPE)` 并关连接                                |
| `FILE_CHUNK` | 校验长度 → `write()` 循环写满 → MD5 增量更新 → `received += n`；若 `received > expected_size` → `ERROR(OVERFLOW)` 并关连接 |
| `FILE_END`   | 校验 `received == expected_size` → `fsync` → 关闭 fd → MD5 finalize → 比对 → 通过则 `rename(tmp, final)` → 回 `ACK(OK, md5hex)` 并回到 CONNECTED；失败则清理临时文件回 `ERROR(...)` 并关连接 |

### 6.3 连接断开

任何情况下连接断开（对端关闭 / 出错）：

- 若存在活跃 FileSession：`close(fd)` 并 `unlink(tmp_path)`（MVP 不做断点续传，不留临时文件）。

------

## 7. 文件与存储语义

### 7.1 存储布局

```
<storage_dir>/
  ├── 192.168.1.10/
  │   ├── a.txt
  │   └── b.bin
  └── 192.168.1.11/
      └── c.txt
```

- 一级目录按客户端 IPv4 点分十进制命名，启动时按需 `mkdir`。
- **IPv6 不做**（MVP 只监听 `AF_INET`）。

### 7.2 临时文件与原子替换

- 命名：`<final_path>.part.<fd>.<rand>`（`rand` 为 8 位十六进制随机数，防同 IP 同文件多连接冲突）。
- 打开：`O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW`，权限 `0600`。
- 写满后 `fsync(fd)` → `close(fd)` → `rename(tmp, final)`。
- `rename` 同目录、同文件系统，POSIX 保证原子性；已存在同名文件**原子覆盖**。
- 最终权限：`rename` 后 `chmod(final, 0644)`（可选，MVP 允许不做）。

### 7.3 文件名清洗（防路径穿越）

收到的 `name` 必须满足，否则 `ERROR(BAD_FILENAME)` 并关连接：

- 长度 `1..512` 字节
- 不含 `\0`
- 不含 `/` 与 `\`
- 不等于 `.` 或 `..`
- 不含任何 `< 0x20` 控制字符
- 不以下列开头：`.`（隐藏文件）、`-`（CLI 误用）
- 不允许本身就是 `.` 或 `..`

清洗通过后 `final_path = <storage_dir>/<client_ip>/<name>`。

### 7.4 剩余空间检查

在 `FILE_META` 收到时执行一次：

text

```
statvfs(storage_dir) → avail = f_bavail * f_frsize
若 size > avail - RESERVE(16MiB)  →  ERROR(WRITE_FAILED, "no space")
```



`RESERVE` 常量化，避免写满系统盘。
**中途不重复检查**；CHUNK 写入时若 `write()` 返回 `ENOSPC` → `ERROR(WRITE_FAILED)` 并关连接 + 清理临时文件。

### 7.5 大小溢出

`FILE_CHUNK` 到达后若 `received + chunk_len > expected_size` → `ERROR(OVERFLOW)` 并关连接。
`FILE_END` 时若 `received != expected_size` → `ERROR(SIZE_MISMATCH)` 并关连接。

------

## 8. 并发、背压与流控

### 8.1 并发目标

- 支持 **≥ 100** 个并发连接。
- 单连接传输文件不阻塞其他连接（不同连接的任务在不同工作线程并行）。
- 同连接内消息严格 FIFO。

### 8.2 背压策略（阻塞式）

| 队列           | 容量         | 满时行为                    |
| :------------- | :----------- | :-------------------------- |
| 每连接队列     | 64           | IO 线程 `submit()` 阻塞等待 |
| 线程池全局队列 | 1024（可配） | IO 线程 `submit()` 阻塞等待 |

选阻塞而非丢弃，理由是：丢弃要设计"部分消息已处理"的补偿逻辑，MVP 不值得。

### 8.3 发送侧流控（高低水位）

- `conn.out_buf` 是 `std::string`，由 `send_mu` 保护。
- **高水位 = 8 MiB**：`out_buf.size() ≥ 8MiB` 时，从 epoll 注销该 conn 的 `EPOLLIN`（暂停接收新请求）。
- **低水位 = 4 MiB**：发送后 `out_buf.size() ≤ 4MiB` 时，重新注册 `EPOLLIN`。
- 发送路径：`send_frame()` 在 `send_mu` 下追加到 `out_buf`，尝试直接 `send()`；`EAGAIN` 时注册 `EPOLLOUT`。

### 8.4 信号处理

- 忽略 `SIGPIPE`（`signal(SIGPIPE, SIG_IGN)`）。
- 处理 `EPOLLRDHUP`：对端半关闭 → 停止读，等 out_buf 发完后关闭。
- 处理 `EPOLLHUP` / `EPOLLERR`：直接关闭连接。

------

## 9. 错误码与错误处理

### 9.1 错误码表

| 码   | 名称            | 触发场景                                      | 是否关连接 |
| :--- | :-------------- | :-------------------------------------------- | :--------- |
| `0`  | `OK`            | 成功                                          | 否         |
| `1`  | `BAD_MAGIC`     | magic 不匹配                                  | 是         |
| `2`  | `BAD_VERSION`   | 版本不支持                                    | 是         |
| `3`  | `BAD_TYPE`      | 未知类型 / body 长度不符 / 状态机不允许该消息 | 是         |
| `4`  | `BODY_TOO_BIG`  | `body_len > 4MiB`                             | 是         |
| `5`  | `NO_SESSION`    | CONNECTED 状态收到 CHUNK/END                  | 是         |
| `6`  | `BAD_FILENAME`  | 清洗失败                                      | 是         |
| `7`  | `WRITE_FAILED`  | 剩余空间不足 / `write` 失败 / `rename` 失败   | 是         |
| `8`  | `MD5_MISMATCH`  | 服务端算出的 MD5 ≠ 客户端声明                 | 是         |
| `9`  | `SIZE_MISMATCH` | END 时 `received != expected_size`            | 是         |
| `10` | `OVERFLOW`      | `received + chunk > expected_size`            | 是         |
| `11` | `INTERNAL_ERR`  | 其他（日志记录）                              | 是         |

**MVP 策略**：所有错误码都关连接（会话不可恢复）。这是简化，未来可加"可恢复错误"白名单。

### 9.2 错误消息内容

`ERROR` 的 `msg` 字段尽量填可读原因（例如 `"md5: expected=..., got=..."`），上限 256 字节。

### 9.3 服务端自身的稳健性

- 任何 `malloc` / `std::bad_alloc` 捕获 → 记录日志 → 关该连接，不影响其他连接。
- 工作线程 `task()` 外层 try/catch，防止异常逃逸导致线程退出。
- 不因单连接异常终止进程。

------

## 10. 配置与命令行接口

### 10.1 CLI

```
fsync-server [options]

  --port <u16>            监听端口              (默认 8888)
  --dir <path>            存储根目录            (默认 ./storage)
  --threads <n>           工作线程数            (默认 hardware_concurrency())
  --queue-cap <n>         线程池全局队列容量    (默认 1024)
  --conn-queue-cap <n>    每连接队列容量        (默认 64)
  --backlog <n>           listen backlog        (默认 512)
  --log-level <level>     error|warn|info|debug (默认 info)
  -h, --help              打印用法
```

### 10.2 退出码

| 码   | 含义                                      |
| :--- | :---------------------------------------- |
| 0    | 正常退出                                  |
| 1    | 参数错误                                  |
| 2    | 启动失败（bind/listen/epoll_create 失败） |

------

## 11. 日志

- 输出：**stderr**，行缓冲，格式：
  [YYYY-MM-DD HH:MM:SS.mmm][LEVEL][fd=12 ip=192.168.1.10] message
- 级别：`ERROR` / `WARN` / `INFO` / `DEBUG`。
- 关键日志点：
  - 启动、监听地址与端口
  - 新连接、连接关闭（带原因）
  - FILE_META 收到（文件名、大小、MD5）
  - FILE_END 校验结果（成功 / 失败原因）
  - 任何 ERROR 级错误
  - 关闭时统计（累计连接数、成功文件数、失败文件数）

------

## 12. 生命周期与优雅退出

- 捕获 `SIGINT` / `SIGTERM`：
  1. 置 `running_ = false`，停止 accept。
  2. 关闭所有活跃连接（触发 FileSession 清理，删临时文件）。
  3. `pool_.shutdown()`，等所有工作线程退出。
  4. 打印统计，退出。
- **最长等待 3 秒**，超时直接强制关闭剩余连接。
- 优雅退出期间**不接收新连接**，但允许已建立的连接完成当前文件（最多 3 秒）。

------

## 13. 验收标准

### 13.1 功能验收（必须全过）

- **AC-1** 单客户端传 100 MiB 文件：服务端落盘文件与源文件 `md5sum` 一致，服务端返回 `ACK(OK, md5hex)` 且与服务端本地 `md5sum` 一致。
- **AC-2** 5 个客户端并发各传 100 MiB：全部成功，无交叉污染（每个 IP 目录下文件正确）。
- **AC-3** 客户端故意发错 MD5：服务端返回 `ERROR(MD5_MISMATCH)`，临时文件被删，目标文件不存在。
- **AC-4** 客户端在 CHUNK 中途断开：临时文件被删，无残留。
- **AC-5** 客户端发非法 magic 帧：服务端返回 `ERROR(BAD_MAGIC)` 并关闭连接，进程不崩。
- **AC-6** 客户端发 `name = "../etc/passwd"`：服务端返回 `ERROR(BAD_FILENAME)` 并关闭连接。
- **AC-7** 单连接串行传 3 个不同文件：全部成功。
- **AC-8** `body_len = 5 MiB`：返回 `ERROR(BODY_TOO_BIG)` 并关闭。

### 13.2 性能验收

- **AC-9** WSL2 本机回环，传 1 GiB 文件：吞吐 **≥ 300 MiB/s**，无单核 100% 长期打满。
- **AC-10** 100 并发各传 1 MiB：全部成功，无超时。

### 13.3 内存与稳定性

- **AC-11** ASan/UBSan 下跑 AC-1 ~ AC-8，无报告。
- **AC-12** 连续运行 10 分钟、循环传文件，RSS 无持续增长（无泄漏）。

### 13.4 开发用工具（非验收）

`tools/test_client.cpp`：

```
test-client --host <ip> --port <n> --file <path> [--concurrency N] [--repeat N]
```

输出：每文件耗时、发送 MD5、接收 ACK 的 MD5。

------

## 14. 非目标与约束

- 不实现 IPv6。
- 不实现断点续传 / 心跳 / TLS / 认证。
- 不做日志持久化。
- 不做配置热重载。
- 不做跨平台。
- 单机单进程；不涉及分布式。

------

## 15. Future Work（明确不在 MVP，但协议/模块预留）

1. 断点续传：新增 `RESUME_QUERY` / `RESUME_ACK` type，服务端保留 `<final>.part.*` 文件并记录 offset。
2. 心跳与空闲超时：`HEARTBEAT` 已预留，需加 `last_activity` 时间戳与 `timerfd`。
3. 可恢复错误：区分"关连接错误"与"继续会话错误"（如 `MD5_MISMATCH` 允许客户端重传）。
4. 多存储后端：抽象 `IStorageSink`，支持 S3 / 分片。
5. 目录同步模式：一批文件元信息 + 差量请求。
6. 认证握手：`HELLO` / `AUTH` 前置帧。

------

## 16. 附录 A：协议示例

**发送 `hello.txt`（内容 "hi"，MD5 已算好）**

```
① FILE_META
   头: 4E 59 53 46 | 01 | 01 | 00 00 00 29     (body_len = 41)
   body: 00 09 | 68 65 6C 6C 6F 2E 74 78 74 | 00...00 02 | <32B hex>

② FILE_CHUNK
   头: 4E 59 53 46 | 01 | 02 | 00 00 00 02
   body: 68 69

③ FILE_END
   头: 4E 59 53 46 | 01 | 03 | 00 00 00 00

← ACK
   头: 4E 59 53 46 | 01 | 80 | 00 00 00 24
   body: 00 00 00 00 | 00 00 00 20 | <32B hex>
```

## 17. 附录 B：待办清单（实现顺序建议）

1. `common.h` 协议常量 + 大端读写
2. `md5.h` 自包含增量 MD5
3. `thread_pool.h` 有界队列线程池
4. `connection.h` FileSession + Connection
5. `server.cpp` epoll 主循环 + accept + recv + parse
6. 消息 dispatch + FILE_META/CHUNK/END 处理
7. 发送路径 + 高低水位
8. ACK/ERROR 与错误码映射
9. 优雅退出、日志、CLI
10. `tools/test_client.cpp`