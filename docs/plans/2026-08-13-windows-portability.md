# Windows 可运行性与跨平台后端改造计划

## 目标

让 NexusDB 以同一套协议、存储格式和 shard 语义运行在 Windows 与 Linux 上，同时保持
Linux 的 `epoll + eventfd + io_uring` 高性能路径不发生运行时分支或性能回退。

首个 Windows 版本的目标是“可构建、可启动、RESP 基础读写与优雅停止正确”；不以 IOCP
吞吐追平 Linux 为发布前提。WSL 运行 Linux 二进制不属于本计划的 Windows 原生支持。

## 设计原则

1. **target 决定平台，feature 决定可选能力**：`cfg(target_os)` 决定实际实现；Cargo
   feature 不得使一个 Linux 二进制调用 Windows API。
2. **上层无感知**：协议解析、shard 路由、B-tree、页格式、WAL 格式、RESP/SQL 语义不引入
   `cfg`；平台差异只存在于 reactor、wake、文件 I/O 和信号边界。
3. **Linux 零回归**：Linux 继续默认 io_uring；portable 不作为 Linux 默认热路径。
4. **先可移植、再原生加速**：Windows P1 使用线程/同步文件 I/O 和跨平台 socket worker；
   P2 才引入 IOCP。任何阶段都保留 `stdfs` 回退。

## 当前审计

当前代码无法原生编译到 Windows：scheduler/storage/network/logging/main 直接使用
`io_uring`、`epoll`、`eventfd`、`poll`、Unix `RawFd` 与 Unix 信号接口。已有的
`IoBackend::StdFs` 只覆盖部分存储 I/O，不足以绕开 worker、wake 与日志线程的 Unix 依赖。

已完成 P0-1：新增零依赖 `platform` crate，以 `PlatformCapabilities` 集中 target 能力；配置
层会在非 Linux target 上拒绝 `storage.io_backend = "io_uring"`，而不是启动后失败。该 crate
提供 `portable`、`linux-io-uring`、`windows-iocp` capability feature，但 feature 不选择 OS。

首次 `cargo check --workspace --target x86_64-pc-windows-msvc` 已执行（target 已安装），按预期
在 P0-3 之前失败：`io-uring 0.6.4` 被 scheduler/storage 无条件依赖，Windows target 无
`std::os::unix`、`mmap` 与 io_uring syscall。这是当前第一个编译阻断点，未尝试以假 feature
或 stub 绕过；`cargo check -p platform --target x86_64-pc-windows-msvc` 必须保持通过。

## 分阶段执行

### P0：编译边界与 CI 基线

1. `[x]` 新建 `crates/platform`，提供 `CURRENT` target 能力与 backend 支持校验。
2. `[x]` 配置层拒绝当前 target 不支持的 `io_uring`。
3. `[x]` 将 `io-uring`、Linux `libc` 使用改为 target-specific dependencies；scheduler 已
   拆成 Linux io_uring 与 portable task/park/yield 核心，storage 已将 positioned file I/O
   收敛到 `FileAt`（Linux `read_at/write_at`，Windows `seek_read/seek_write`）。`pager_io`
   的 io_uring backend、registered buffers 与 WAL async fsync 均已 Linux cfg 化；非 Linux
   `IoUring` 编程入口安全回退到 `StdFs`，而配置文件仍会拒绝显式 io_uring。
4. 添加 Windows `cargo check --workspace --target x86_64-pc-windows-msvc` CI job；Linux 保留
   全量 test/clippy。

验收：Windows target 可完成 workspace `cargo check`；Linux `cargo test`、clippy 与 memtier
基线无回退。P0 不要求 Windows 二进制启动。

本轮验证：Linux `cargo test -p scheduler`、`cargo test -p storage --lib`（182 passed、8
ignored）通过；Windows `cargo check --target x86_64-pc-windows-msvc` 已通过。

回滚：只撤 target cfg 与 Cargo 依赖分层，不改变持久化格式。

### P1：可移植运行时与 Windows MVP

1. 抽 `WakeHandle`：Linux `eventfd`；Windows 使用 `std::sync::Condvar`/channel 作为 MVP 唤醒。
2. 抽 `NetworkReactor`：保留 Linux epoll；Windows MVP 使用每连接阻塞 socket worker 或有限
   worker-pool，不复用当前 RawFd conn map。
3. 抽 `FileIo`：Windows 用 `std::fs`，禁用 fixed-file、registered buffer、O_DIRECT 与 SQPOLL。
4. 日志改为跨平台同步/专用线程 std::fs 后端；Unix io_uring logger 保持 Linux 专用。
5. 将 `SIGTERM/SIGINT` 封装为 `ShutdownSignal`；Windows 控制台 Ctrl-C 走对应实现。

进度：`[x]` Linux logger 保持 eventfd/io_uring，Windows 改为同一无锁队列 + 批量 std::fs
flush；`[x]` shard inbox/reply bus 在 Windows 使用短时等待而不改变队列/FIFO 语义；`[x]`
Windows portable server 使用 std socket 与每连接线程，支持 Binary 和 RESP 的 AUTH、PING、
GET、SET、DEL。SQL/PG/HTTP/TLS 被明确限制为 Linux 路径，Windows 默认启动会跳过它们，等待
P2 IOCP 及各协议 worker 的独立移植。Windows 原机运行 smoke 与 Ctrl-C handler 仍待 CI/原机
验收，不能由本 Linux 环境的交叉 `check` 替代。

验收：Windows 上启动 RESP 监听、SET/GET/DEL、重启恢复、关闭；Linux 协议 e2e 不变。

#### P1 验收 (2026-08-13)

- `cargo build --release` 0 错误
- `crates/network` 17/17 单元测试通过 (`protocol::resp` / `protocol::crypto` /
  `value_codec`)
- 原机 `nexusdb.exe --config nexusdb-test.toml` 启动监听 Binary 5433 + RESP 6380
- redis-cli PING/SET/GET/DEL/INCR(命令级)/HSET/LPUSH/SADD/ZADD/DBSIZE/INFO/CLIENT LIST
  完整 smoke：PING/SET/GET/DEL 全部返回正确值；INCR/HSET/... 返回 "not yet supported"
  (与 Linux `portable.rs` 行为一致，**是协议层未实现，非 Windows runtime 缺**)
- Ctrl-C 走 `SetConsoleCtrlHandler` → `SHUTDOWN` atomic → 优雅退出（acceptor
  `shutdown(Shutdown::Both)` 唤醒阻塞 read + join 所有 connection threads + `mgr.close()`
  flush WAL）
- WAL 持久化：服务关闭后重启自动 replay 上次 SET/GET
- `nexusdb-test.toml` 默认端口 6380（6379 被 win 自带 `redis-server` 占着，SYSTEM
  账户运行，没 admin 杀不掉；用 6380 是测试方便，正式部署可改回 6379）
- 缺省 config（无 `--config`）自动用 `stdfs` 后端（Linux 默认 `io_uring` 在 Windows 不可用）
- 缺省 `block_root = "./data"`，与 Linux 路径 `"./data"` 语义一致

### P2：Windows 原生 reactor（IOCP）

1. 用 IOCP 替换 P1 Windows 阻塞 worker，支持 accept/read/write completion。
2. Windows `WakeHandle` 统一为 `PostQueuedCompletionStatus`，避免额外唤醒线程。
3. 给 socket/file request 加取消、关闭竞态和背压测试。

验收：Windows 32 客户端 pipeline 测试无丢包/卡死；吞吐以 P1 为基线提升，Linux 不受影响。

#### P2 暂缓说明（2026-08-13）

第一轮 P2 实施按 `[2026-08-13-windows-iocp.md]()` 设计走 IOCP 完成端口路径，M1/M2 已经
跑通编译和 binary echo，但 RESP 端到端碰到了 **Win10/11 上 AcceptEx 同步返回 TRUE
但 child 仍 pre-alloc** 的 OS 行为，详见设计文档的"IOCP / AcceptEx 尝试的踩坑记录"节。

当前决定：

- **保持 P1 (std::net blocking + 每连接 std::thread) 作为 Windows 主推路径**，所有 M3+
  的 SQL/PG/HTTP/TLS 协议补齐都基于这条路径，每连接一个 OS thread 已经能撑住开发 +
  单机 demo 的并发量。
- **M2 增量不再追 IOCP**；标记 M2 当前实现边界 = P1 + RESP dispatch，把 P2 挪到
  P3 之后的 "perf" 阶段。
- 如果未来再上 IOCP / RIO：
  - `#[repr(C)]` + OVERLAPPED 第一个字段
  - 复现问题：先在最小 listener + AcceptEx 投递 + GQCS 上回放，确认 `bytes=0` 是 sync
    success 但 child 真实 accepted 再继续；目前观察到的是 child 永远不 accepted。
  - 备用方案：换 `wepoll` (kernel-bridged epoll) 或 winsock catalog extension，或在
    `wepoll` 上跑 `mio`-style 状态机。

### P3：发布与矩阵

1. 发布 `x86_64-pc-windows-msvc` artifact；文档标明 Windows MVP 的 `stdfs` 限制。
2. 在 Windows CI 跑 RESP/存储恢复 smoke；Linux 继续完整回归与 memtier。
3. 仅在 Windows IOCP 指标稳定后，考虑 SQL/PG/TLS 的真实客户端交叉验证。

## 文件与接口边界

| 边界 | Linux | Windows P1 | Windows P2 |
|---|---|---|---|
| `platform` | capabilities | capabilities | capabilities |
| `WakeHandle` | eventfd | channel/Condvar | IOCP post |
| `NetworkReactor` | epoll | worker-pool | IOCP |
| `FileIo` | io_uring / stdfs | stdfs | stdfs，后续 overlapped file IO |
| shutdown | signal | console control | console control |

## 风险与非目标

- 不把 io_uring scheduler 强行移植为 Windows runtime；它应保留 Linux 专用，Windows 通过
  reactor/file backend 实现相同上层契约。
- 不在 P1 修改页、chunk、WAL 或网络协议编码，避免跨平台引入数据格式分叉。
- 不承诺 Windows P1 的性能；先验证正确性与可维护的隔离边界。
- 对 Windows 文件 rename、flush 语义与锁冲突必须单独测试，不能假设与 Linux 完全一致。
