# Windows IOCP runtime 设计与分阶段实施

## 目标

让 NexusDB 在 Windows 上原生支持 **MySQL wire (5434) / PostgreSQL wire (5435) / HTTP REST (6778)**
三种 SQL 门面,性能对等 Linux epoll 路径;同时保留 Binary (5433) / RESP (6379) 的现有支持。

第一个 Windows 版本目标是: **可构建、可启动、SQL 核心路径 (CREATE/INSERT/SELECT/JOIN/事务) 端到端通;
860+ tests 全部通过**。性能暂不追平 Linux (P50 < 5ms 即可),TLS / HTTP/2 / IOCP socket polling extension
留到 P2。

## 现状 (基于 2026-08-13 feat/resp-sql-schema-adapter 提交点)

- `crates/network/src/lib.rs` 用 `#[cfg(target_os = "linux")]` / `#[cfg(not(target_os = "linux"))]`
  隔离两套 runtime: Linux 走 `server.rs + worker/`,非 Linux 走 `portable.rs` (每连接一个 std::net 阻塞线程)。
- `crates/network/src/worker/` (Linux) 包含 50+ 模块,核心是 `worker_coro.rs` (协程 + io_uring) 和
  `worker_epoll.rs` (epoll + 双协议 Binary/RESP)。
- `crates/network/src/portable.rs` 显式拒绝 SQL/PG/HTTP/TLS 协议:
  ```rust
  if !matches!(base.protocol, ProtocolKind::Binary | ProtocolKind::Resp) {
      return Err("portable runtime supports Binary and RESP only");
  }
  ```
- `crates/network/src/protocol/` (5 个协议解析) **总是编译**——MySQL/PG/HTTP 解析层在 Windows 上
  也能用,但没有执行入口 (`worker/sql_*.rs` 是 Linux only)。
- `crates/network/src/tls/` 也是跨平台编译 (代码本身 platform-independent,只是没 runtime 调它)。
- Linux 端 `server.rs` 直接用 `libc::eventfd` / `libc::write` / `libc::close`——这些是 Linux only,
  在 `lib.rs` 层用 `#[cfg(target_os = "linux")]` 隔离掉了,所以 Windows 编译不会失败。
- P0 阶段 (commit `a0a3e25`) 已完成 `crates/platform` 抽象 + 依赖 cfg 拆分 + `scheduler_portable.rs`
  + `network/portable.rs` 的 Binary/RESP MVP。SQL/PG/HTTP/TLS 仍为 Linux only。

## 设计原则

1. **target 决定平台, feature 不允许跨平台实现**——`cfg(target_os)` 选实现,`cargo feature` 不能让
   Linux 二进制调 Windows API (沿用 2026-08-13-windows-portability.md 的硬约束)。
2. **不重写协议解析**——`crates/network/src/protocol/{mysql,pg,http,resp,binary}.rs` 全部复用,SQL 解析
   (sql/ 子模块) 也复用,零协议层改动。
3. **不重写 SQL 逻辑**——`crates/network/src/worker/sql_*.rs` (含 sql_dispatch, sql_dml, sql_agg,
   sql_state, sql_join, sql_unique, sql_fk, sql_cascade, sql_eval, sql_encode, sql_sysquery) 全部复用。
4. **不重写执行层**——`shard_manager::ShardManager` 的 sync/async API 复用,Windows worker 直接调
   sync 入口,不走协程/inbox/reply_bus 那套 (Linux 用 inbox 是为了避免阻塞 worker 线程,Windows IOCP
   worker 不阻塞在 IO,可以用 sync API 直接调 ShardManager,具体看风险节)。
5. **保留 portable.rs**——作为 fallback,每连接线程模型继续支持 Binary/RESP (用于测试 / 嵌入式
   轻量场景)。新建 `runtime_iocp.rs` 作为 Windows 主推路径。
6. **接受 P0 设计文档的边界**——`stdfs` IO、shutdown 走 `console control`、日志走
   `std::sync` + 专用线程 (不打回 io_uring)。WakeHandle 走 channel/Condvar,P2 再上 IOCP post。

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│ Acceptor thread (1)                                         │
│  • TcpListener::bind                                        │
│  • AcceptEx loop → 投递到 IOCP                                │
│  • 每连接关联到 worker (RoundRobin / Hash)                    │
└──────────────────────────┬──────────────────────────────────┘
                           │ 注册 fd 到 IOCP + 投递 WSARecv
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ IOCP completion port (1)                                    │
│  • 关联 worker thread pool                                   │
└──────────────────────────┬──────────────────────────────────┘
                           │ GetQueuedCompletionStatus
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ Worker thread pool (N = config.worker_count)                │
│  • 主循环: GQCS → match overlapped kind                     │
│  • Recv completion:                                          │
│      protocol::decode(buf) → match protocol                  │
│      ├ Binary  → existing process_binary_input (chunk)      │
│      ├ Resp    → existing process_resp_input  (chunk)       │
│      ├ MySQL   → sql_dispatch::sql_dispatch_stmt (sync 入口)│
│      ├ PG      → 同 MySQL, framing 翻译                    │
│      └ HTTP    → existing process_http_input (chunk)        │
│  • Send completion: 出队 write_buf 继续写                  │
│  • Shutdown: PostQueuedCompletionStatus 投递 SHUTDOWN key    │
└──────────────────────────┬──────────────────────────────────┘
                           │ sync 调 ShardManager
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ Shard threads (不变, N = num_shards)                        │
│  • 接受 worker 同步/异步调用 (看 ShardManager API)           │
│  • WAL + GC + 压缩 全部现有逻辑                              │
└─────────────────────────────────────────────────────────────┘
```

## 关键设计选择

### A. OVERLAPPED 模型 (Win32 标准 IOCP 模式)

每条 IO 操作分配一个 `Box<OverlappedData>`,生命周期与该次 IO 绑定。`OverlappedData` 内含:

```rust
struct OverlappedData {
    overlapped: OVERLAPPED,                    // 必须是字段 0 (Windows 要求)
    kind: OverlappedKind,                      // Accept | Recv | Send
    conn: Arc<Mutex<ConnState>>,               // 共享 conn 状态
    buf: Vec<u8>,                              // 这次 IO 的 buffer
}
unsafe impl Send for OverlappedData {}        // 实际不跨线程,但编译器要
```

`OVERLAPPED` 的指针是 GQCS 唯一标识——`overlapped.cast::<OverlappedData>()` 拿回上下文。

### B. 协议分发 (关键)

Windows worker 主循环按 protocol 分发,**复用**现有 `process_*_input` 函数 (这些函数已经是
chunk-based,接收 `&mut ConnState` + `&[u8]`):

```rust
// runtime_iocp.rs worker 主循环 (伪代码)
loop {
    let (bytes, key, overlapped) = gqcs_blocking();
    let o: &mut OverlappedData = unsafe { &mut *overlapped };
    let mut conn = o.conn.lock();
    match o.kind {
        Recv => {
            conn.read_buf.extend_from_slice(&o.buf[..bytes]);
            // drain buf, 按协议分发:
            loop {
                let out = match conn.proto {
                    Binary => process_binary_input(&mut conn, &mut conn.read_buf),
                    Resp   => process_resp_input(&mut conn, &mut conn.read_buf),
                    Sql    => process_sql_input(&mut conn, &mut conn.read_buf),  // ← 复用
                    Pg     => process_pg_input(&mut conn, &mut conn.read_buf),    // ← 复用
                    Http   => process_http_input(&mut conn, &mut conn.read_buf),  // ← 复用
                };
                if out.is_empty() { break; }       // need more data
                queue_send(&mut conn, out);         // append to write_buf, WSASend
            }
            // 投递下一次 WSARecv
            post_wsa_recv(&o.conn, ...);
        }
        Send => { /* 出队 write_buf, 写完或继续 WSASend */ }
        Accept => { /* 完成 accept, 投递新连接的 WSARecv */ }
        Shutdown => return,
    }
}
```

**关键 insight**:`process_*_input` 已经是 chunk-based + 返回 `Vec<u8>` (待发数据) 的接口
(看 `crates/network/src/worker/protocol_io.rs`),**不是 async**——可以直接在 Windows worker
主循环里 sync 调用,不需要重写协议逻辑。

### C. SQL 同步化 (最关键的抽象)

Linux 路径上 `sql_dispatch::sql_dispatch_stmt` 调 `ShardManager` 的 API,API 可能是:
- `async` (要走协程 yield)——不行,Windows worker 不在协程上下文
- `sync` (直接返回 result)——OK,Windows worker 直接调

**目前没看 ShardManager 的 API,需要先 audit**。可能的方案:

**C1. ShardManager 暴露 sync 入口 (最理想)**
- 已经有 sync API (例如 `dispatch_task` 返回 `Result<Response>` 而不是 `Result<impl Future>`)
- Windows worker 直接调,无抽象成本
- **风险**:SQL 跨 shard 操作可能需要 inbox/reply 协调,同步化要保证跨 shard 的 atomic 性

**C2. 单 thread loop 模拟 (workaround)**
- Windows worker 把 SQL 任务 push 到一个 mpsc channel
- 单独的"executor thread"消费 channel,调 ShardManager
- Executor 完成后把 result 通过 channel 回 worker
- worker 收 result → encode → send
- 模拟了 Linux 的 inbox/reply 模式,代码改动小
- **风险**:额外的 channel 跳,延迟 +10-50μs

**C3. SQL facade 抽公共 sync 层 (中期方案)**
- 重构 `sql_dispatch_stmt` 抽出 `fn execute_sync(state, req) -> Response` 公共接口
- Linux 协程 worker 调 `execute_sync` 然后 yield + 等 reply
- Windows IOCP worker 调 `execute_sync` 直接拿 result
- **风险**:SQL 路径逻辑复杂,公共 sync 抽象要小心 (状态机 / 跨 shard 协调 / 事务 / JOIN)

**我推荐先 C2 (workaround) 跑通端到端,再演进 C3 (sync 抽象) 优化延迟。**

### D. Acceptor: AcceptEx vs accept

- IOCP 风格用 `AcceptEx` (不是 `accept`)——避免 acceptor 线程阻塞
- `AcceptEx` 需要 LPFN_ACCEPTEX 函数指针 (通过 WSAIoctl SIO_GET_EXTENSION_FUNCTION_POINTER 拿)
- Buffer: `AcceptEx` 需要预先给一块 buffer 放 local/remote addr
- **Windows-only API**,但 `windows-sys` crate 已经有完整绑定 (项目里没用过,新加依赖)

**风险**:AcceptEx 调通是 IOCP 第一道坎,需要参考 Microsoft 官方 sample。

### E. worker ↔ conn 一一对应 vs 共享

- **Linux 路径**:`worker_main_epoll` 共享 worker (一个 worker 多 conn)
- **Windows IOCP**:**一个 worker 一个 conn** 是错的 (浪费 IOCP)
- **正确**:**多个 worker 共享一个 IOCP** (GetQueuedCompletionStatus 可以多线程等)
- 设计:一个 IOCP 关联到 N 个 worker thread,完成事件自动分到等它的线程
- 跟 Linux 模式对等:worker pool 共享一个 epoll / 一个 IOCP

### F. TLS (P1 不做)

- rustls 本身跨平台
- 但 `rustls::ServerConnection` 跟 `BufRead`/`Write` 集成——需要把 WSARecv/WSASend 的 buffer
  流式化 (用 `BufReader` 包装 OVERLAPPED 的 read 序列)
- **P2 单独实施**——P1 先把 SQL/PG/HTTP 明文跑通

### G. 跟 Linux 端的兼容性

- **不动** `server.rs` / `worker/` 任何已有 Linux 代码
- **新增** `crates/network/src/runtime_iocp.rs` + 在 `lib.rs` 加 `#[cfg(target_os = "windows")]`
- portable.rs 保持现状 (Binary/RESP fallback)
- 三套共存:`#[cfg(target_os = "linux")]` 走 server+worker,`#[cfg(target_os = "windows")]`
  走 runtime_iocp,`#[cfg(not(any(target_os = "linux", target_os = "windows")))]` 走 portable

## 分阶段实施

### M1 — IOCP runtime 骨架 (3-4 天)

**目标**:AcceptEx + IOCP 主循环,echo 服务器端到端通。

- 写 `crates/network/src/runtime_iocp.rs`
- 依赖 `windows-sys` (Windows-only,加在 `[target.'cfg(target_os = "windows")'.dependencies]`)
- Acceptor 线程:`TcpListener::bind` + `AcceptEx` 循环 + 新连接注册到 IOCP
- Worker 线程池:主循环 `GetQueuedCompletionStatus` + `WSARecv` / `WSASend` + echo (把 recv 的
  字节直接 send 回去)
- Shutdown 机制:`PostQueuedCompletionStatus` 投递 SHUTDOWN key,worker 退出
- Cargo.toml 加 windows-sys + cfg 拆分
- lib.rs 加 `#[cfg(target_os = "windows")] pub mod runtime_iocp;`
- NetworkServer API 跟 Linux 端对等:`start(config)`, `local_addr()`, `shutdown()`
- 验证:PowerShell `Test-NetConnection` + 简单 Python socket client 测 echo

### M2 — RESP 协议搬到 IOCP (2-3 天)

**目标**:redis-cli 完整 smoke 在 Windows 走 IOCP 跑通。

- 复用 `process_resp_input` (已经是 chunk-based)
- 接 auth 处理、INCR、DEL 聚合、ZSet 渲染
- 验证:之前完整跑过的 redis-cli smoke (SET/GET/DEL/INCR/HSET/LPUSH/SADD/ZADD/DBSIZE/INFO/CLIENT LIST)

### M3 — Binary 协议 (1 天)

**目标**:Binary 协议 smoke 通。

- 复用 `process_binary_input`
- 简单,主要测 req_id 路由
- 验证:用 PowerShell raw socket 发 Binary 帧,PING / GET / PUT

### M4 — SQL sync 化 + MySQL wire (5-7 天)

**目标**:`mysql` CLI 能连 NexusDB,执行 `CREATE TABLE / INSERT / SELECT / JOIN` 通。

- audit ShardManager API (选 C1 / C2 / C3 路线,定 C2 起步)
- 写 SQL sync 入口 (`sql_dispatch::sql_dispatch_stmt` 的 sync 包装)
- 复用 `process_sql_input` (已经 chunk-based)
- MySQL 协议握手 + auth + 简单 query + 结果集编码 (复用 `protocol/mysql.rs` 的编/解码)
- 验证:`mysql -h127.0.0.1 -P5434 -utest -ptest`,跑错题 schema 的 CREATE/INSERT/SELECT/JOIN

### M5 — PG wire (4-5 天)

**目标**:`psql` 能连 NexusDB,简单 query 通。

- 复用 `process_pg_input`
- PG 协议:StartupMessage + SCRAM-SHA-256 auth + 简单 query (`Q` message) + Parse/Bind/Execute
  (extended protocol) + 结果集编码 (复用 `protocol/pg.rs`)
- 验证:`psql "host=127.0.0.1 port=5435 user=test dbname=default password=test"`,跑 SELECT 1 /

### M6 — HTTP REST (2-3 天)

**目标**:`curl` GET/POST 通。

- 复用 `process_http_input`
- HTTP/1.1 keep-alive + Content-Length (无 chunked / TLS / HTTP/2,跟 Linux 路径一致)
- 验证:`curl http://127.0.0.1:6778/v1/status`,`curl -X PUT .../v1/kv/test`,
  `curl -X POST .../v1/sql -d '{"query":"SELECT 1"}'`

### M7 — 测试 + 性能 (持续)

- `cargo test --workspace --no-fail-fast` 跑全套 860+ tests 在 Windows
- 修 Windows 端 unique 的 bug (Linux-only 假设)
- memtier 压测 RESP,跟 Linux 对比 (P50/P99 差异)
- psql/mysql 客户端连接 + 跑 sql_bigdata e2e suite
- 文档:Windows 部署 README + 已知限制

**合计:18-26 天 (全职)**

## 文件布局

```
crates/network/
├── src/
│   ├── lib.rs                              # 改:加 cfg(windows) runtime_iocp
│   ├── portable.rs                         # 不动,继续 fallback
│   ├── runtime_iocp.rs                     # 新:Windows IOCP 主路径
│   ├── runtime_iocp/
│   │   ├── mod.rs                          # 公共 API (NetworkServer, Config)
│   │   ├── acceptor.rs                     # AcceptEx 循环
│   │   ├── worker.rs                       # IOCP worker 主循环
│   │   ├── conn.rs                         # ConnState 的 Windows 部分 (跟 worker_conn 对等)
│   │   ├── overlapped.rs                   # OVERLAPPED 包装
│   │   ├── sql_sync.rs                     # SQL sync 入口 (M4 落地)
│   │   ├── sql_bridge.rs                   # ShardManager sync API bridge
│   │   └── shutdown.rs                     # PostQueuedCompletionStatus 唤醒
│   ├── protocol/                           # 不动,5 个协议全平台复用
│   ├── server.rs                           # 不动,Linux only
│   ├── worker/                             # 不动,Linux only
│   ├── kv_to_shard.rs                      # 不动
│   ├── reply_bus.rs                        # 不动,Windows 不用
│   ├── tls.rs                              # 不动
│   └── value_codec.rs                      # 不动
└── Cargo.toml                              # 改:加 windows-sys
docs/plans/
└── 2026-08-13-windows-iocp.md              # 本文档
```

## 风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| ShardManager 没有 sync API (M4 大坎) | SQL 同步化受阻 | 先 C2 跑通端到端,再 C3 抽象 |
| AcceptEx 实现复杂,Windows-specific | M1 延后 | 跟 MS 官方 sample 对照,有 1-2 天 buffer |
| `process_*_input` 跟 Linux 协程状态耦合 | 复用失败 | 改 chunk-based 签名,保留 sync 路径 |
| 860+ tests 在 Windows 跑有 surprise | M7 收尾延后 | M1-M6 边做边跑 tests,提前暴露 |
| 性能 Windows 远差 Linux | 用户体验差 | P50 < 5ms 即可;P2 IOCP socket polling / Registered I/O |
| TLS / HTTP/2 / chunked 缺失 | 客户端受限 | P1 文档明示限制;P2 单独规划 |
| worktree / branch 管理 | 多人协作冲突 | 单独 branch `feat/windows-iocp`,每 M 一次 squash |
| windows-sys 版本冲突 | build 失败 | 固定 `windows-sys = "0.59"` 跟现有 deps 对齐 |
| `RawFd` 在 Windows 上语义不同 | API 不匹配 | runtime_iocp 完全用 `SOCKET` (windows-sys) 而非 RawFd |

## 不做的 (P1 明确边界)

- TLS (rustls 集成) — P2
- HTTP/2 / chunked / streaming result sets — P2
- IOCP socket polling extension (内核态 polling) — P2
- Windows 上的协程模拟 (省 work-stealing) — 暂不需要,worker thread pool 足够
- 跟 Linux 路径合并成一个抽象 — 短期不现实,cfg 隔离更安全

## 验收标准 (P1 终点)

- `cargo check --target x86_64-pc-windows-msvc --workspace` 通过
- `cargo build --release --target x86_64-pc-windows-msvc` 产出 NexusDB.exe
- 启动:`NexusDB.exe --config nexusdb-test.toml`,5 端口全部 listen
- `redis-cli -p 6379 PING/SET/GET/DEL` 通 (M2)
- `mysql -h127.0.0.1 -P5434 ...` 跑 CREATE/INSERT/SELECT/JOIN 通 (M4)
- `psql -h127.0.0.1 -p5435 ...` 跑 SELECT 1 通 (M5)
- `curl http://127.0.0.1:6778/v1/status` 200 OK (M6)
- `cargo test --workspace` 在 Windows 端到端通过 (M7)
- memtier RESP P50 < 5ms (单 worker,本地回环) (M7)

## 参考

- 既有 design:`docs/plans/2026-08-13-windows-portability.md` (P0 完成)
- 既有 design:`docs/plans/2026-08-13-coroutine-scheduler-hardening.md`
- Microsoft IOCP 文档:https://learn.microsoft.com/en-us/windows/win32/fileio/i-o-completion-ports
- Microsoft AcceptEx sample:https://learn.microsoft.com/en-us/windows/win32/winsock/using-acceptex
- windows-sys crate:`windows-sys = "0.59"` (跟项目 `windows` 生态兼容)
- lib.rs 现状:`crates/network/src/lib.rs` (cfg 隔离参考)
- 协议层:`crates/network/src/protocol/{mysql,pg,http,resp,binary}.rs`
- SQL 层:`crates/network/src/worker/sql_*.rs`
- Linux 端协程 worker:`crates/network/src/worker/worker_coro.rs` (主循环参考)
- Linux 端 epoll worker:`crates/network/src/worker/worker_epoll.rs` (epoll 主循环参考)
- portable 现状:`crates/network/src/portable.rs` (Binary/RESP fallback)
