# Worker Network Layer → 协程调度统一 (feat: worker-coroutine-scheduler)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `crates/network` 的 worker 网络层从「单线程 epoll 事件循环 + 手工连接表 + 同步 TcpStream 读写」重构为「每 worker 一个自研协程 Scheduler + 每连接一个协程 + io_uring 异步收发」, 使网络层与存储层(shard)采用**同一套协程调度方式**, 并在协程内按连接上下文处理不同协议, 消除多协议 worker 线程数随协议数膨胀的问题, 让 worker 数精确贴合用户配置的 CPU 核数。

**Status:** Draft (本分支刚创建, 待逐 Task 实现)

---

## 1. 背景与动机

### 现状 (2026-08, main 分支)

**网络层 worker** (`crates/network/src/worker/`):
- `worker_main_epoll` (`worker_epoll.rs`) 是**单线程 epoll 事件循环**: `epoll_wait` + `HashMap<conn_id, ConnState>` 手工管理多连接 (`worker_epoll.rs:9-217`)
- 连接 `TcpStream` 已设非阻塞 (`worker_conn.rs:23`), 但 `recv()`/`send_bytes()` 是**同步读写**, 遇 `WouldBlock` 用 `std::thread::yield_now()` **自旋重试** (`worker_conn.rs:103-212`)
- **完全没有使用 `scheduler` crate** — 尽管 `crates/network/Cargo.toml:8` 已声明依赖, 代码 0 处调用 (`lib.rs` 注释 "每个 worker own 1 个 Scheduler" 是**未实现的设计意图**)

**存储层 shard** (`crates/shard_manager/` + `crates/scheduler/`):
- 每 shard 单线程 + **自研协程调度器** (`Scheduler`) + io_uring 异步落盘 (`shard_thread.rs:253-280`)
- 这是**唯一真正使用协程调度的地方**

**初始设计**: `docs/plans/2026-07-25-async-network-stack.md` 明确规划了 "1 Acceptor + N Workers, **每 worker 拥有 1 个 Scheduler, task pool 每个 task 是一个连接 handle**" — 即协程化网络层。当前 epoll 实现是**偏离该设计的落地折中**, 本次改造即回归并完成该设计。

### 问题 (用户提出)

1. **线程数随协议数膨胀**: 每协议一个 server、每个 server 各配 `worker_count` 个 worker → 启动 5 个协议 server 各配 3 worker 就是 15 个线程, 而非用户配的 3 个。用户 "十个 core 分三个给 worker" 不想看到 12 个线程。
2. **调度方式不统一**: 网络层 epoll + 存储层协程, 两套模型, 维护和推理成本高。
3. **同步自旋**: `recv`/`send_bytes` 遇 `WouldBlock` 自旋, 是潜在 CPU 浪费与延迟抖动来源。

### 目标 (非目标之外)

- worker 数 = 用户配置数 (不受协议数影响), 每 worker 亲和一个核
- 网络层与存储层共用同一套 `Scheduler` 协程调度 + io_uring
- 每连接一个协程, 协程内按连接的协议上下文 (`ConnState.proto`) 处理
- 多协议仍走多端口 (端口即协议, accept 时零成本区分), 但**共享全局 worker 池**

---

## 2. 目标架构

```
        ┌─────────────────────────────────────────────┐
        │          external clients (多端口)           │
        │   :6379 RESP  :6778 HTTP  :5434 MySQL       │
        │   :5435 PG    :5433 Binary                  │
        └───────────┬────────────────┬────────────────┘
                    │ accept         │ accept
                    ▼                ▼
        ┌─────────────────────────────────────────────┐
        │   Acceptor(s) — 纯接收, 零业务              │
        │   按端口(协议)标记 new_conn → 全局 worker 池 │
        └───────────┬────────────────┬────────────────┘
                    │  RoundRobin (全局共享池)
                    ▼
   ┌──────────────────────────────────────────────────┐
   │  N Worker Threads (N = 用户配置, 每核一个)       │
   │   thread i:                                       │
   │     Scheduler::new()  (own io_uring + registry)   │
   │     协程池:                                       │
   │       ├ 协程[conn_A] (协议 RESP)                  │
   │       ├ 协程[conn_B] (协议 PG)                    │
   │       └ 协程[new_conn_loop] (收新连接→spawn)      │
   │   跨线程通信:                                     │
   │     → push_task → shard_inboxes (ArrayQueue)      │
   │     ← await reply ← reply_bus (per-worker)        │
   └──────────────────────────────────────────────────┘
                    │
                    ▼
   ┌──────────────────────────────────────────────────┐
   │  M Shard Threads (existing, 每 shard 一协程调度)  │
   │   异步落盘 io_uring + 回复 reply_bus.push         │
   └──────────────────────────────────────────────────┘
```

**核心合约 (per-thread scheduler invariant):**
- 每个 `Scheduler` 由 1 个 OS thread 独占, `!Send + !Sync`
- 跨 scheduler/线程通信永远走 `ArrayQueue` / `crossbeam` / reply_bus, 不跨线程 borrow
- 协程 `await` 一个跨线程 future 时, 把当前 `Waker` 注册进 reply_bus 接收端, **不阻塞线程**

---

## 3. 与现状的差异点 (改造清单)

| # | 现状 | 目标 |
|---|---|---|
| D1 | `worker_epoll.rs` epoll 事件循环 + `conn_map` | `worker` 每线程一个 `Scheduler`, 连接为协程 |
| D2 | `worker_conn.rs` `recv()/send_bytes()` 同步 + WouldBlock 自旋 | `async fn` + `scheduler::io_ops::read/write` |
| D3 | reply 用 epoll 监听 reply_bus eventfd + 手工 `drain` | 协程 `await` 本连接 reply (Waker 注册) |
| D4 | 每 server 固定 `protocol`, worker 从 cfg 取固定 `proto_kind` | 全局 worker 池, 连接协程创建时按端口标记协议 |
| D5 | 多 server = 多 worker 池 (线程数 ×协议) | 多端口共享全局 worker 池 |
| D6 | 新连接通知走 eventfd | 协程 `new_conn_loop` 从 inbox 取连接 |

**保持不变 (复用):**
- 协议解析 / 渲染 / SQL 编排 (`process_*_input`, `handle_resp_shard_result`, 聚合状态机) — 同步 CPU 逻辑, 包进协程即可
- `ConnState` 的状态字段 (聚合 HashMap 等) — 每连接协程后成为协程局部变量 (反而简化)
- reply_bus / shard_inboxes 跨线程通道
- 存储层 shard 完全不动

---

## 4. 分阶段路线

> 按"风险从低到高、每阶段可独立编译+测试+提交"排序。**关键原则**: 每个阶段结束时 `cargo build` 全绿 + 现有测试 (860+ unit + 45 sql_e2e + 22 resp_e2e + 7 pg_e2e + bigdata) 不回归。

### Phase 0: 准备与基线确认

- [x] **T0.1** 确认 scheduler 对 socket fd 的 io_uring 支持 ✅ — 新增 `crates/scheduler/tests/socket_io_test.rs`, 两个测试通过:
  - `socket_read_write_roundtrip`: 单协程 io_uring socket 读写往返正确
  - `socket_concurrent_tasks`: 8 协程并发 socket 读写, 数据不串
  - **结论**: io_uring `Read/Write` opcode 对 socket 用 `offset = u64::MAX` (-1, 当前位置) 完全可用。注意现有 `io_ops::read/write` 强制传 offset, worker 调用需传 `u64::MAX` (Phase 1 处理, 可考虑加 socket 专用封装)
- [x] **T0.2** 建立基线 ✅ — network 测试全绿: 77 unit + 45 sql_e2e + 22 resp_e2e + 7 pg_e2e + bigdata 5
- [x] **T0.3** 明确 worker 与 server 的依赖边界 ✅ — 关键发现: `WorkerConfig.protocol` 只在 `worker_epoll.rs:36` 读取一次, 用于初始化新连接 `ConnState.proto`; 而 `ConnState.proto` 在渲染/协议逻辑中被广泛使用 (120+ 处, `conn.proto`)。**协议信息本质是连接级的** — 全局 worker 池只需把 `ConnState::new` 的 `proto_kind` 从"worker 固定值"改为"按端口标记的连接协议", 那 120+ 处 `conn.proto` 用法无需改动。D4/D5 改造比预想容易

### Phase 1: 协程 worker 骨架 (关键调整 — 原设计 P1/P2 耦合)

> **⚠️ 关键发现 (Phase 0 调研)**: 连接"可读事件"要么由 epoll 管 (然后同步读), 要么由 io_uring 管 (然后协程读), **不能混用**。非阻塞 socket 若 epoll 通知可读后协程又去 io_uring read, 会双重事件源冲突。因此**无法"只把 recv 换成 io_uring 而保留 epoll + conn_map"** — P1 必须同时引入协程事件驱动, 即原 P2 的核心。P1 调整为"搭建协程 worker 骨架, 先最小闭环跑通, 不改连接处理语义"。

- [ ] **T1.1** 新建协程 worker 执行入口 `worker_coro.rs`: worker 线程初始化 `Scheduler`, 跑两个协程:
  - `new_conn_loop`: 用 io_uring 读 `conn_eventfd`, 从 inbox 收新连接并 spawn 连接协程
  - `reply_loop`: 用 io_uring 读 `reply_bus.eventfd`, drain 回包并按 conn_id 匹配
- [x] **T1.2** 单连接协程最小闭环 (握手级) ✅ — 新增 `crates/network/tests/worker_coro_handshake_test.rs`, 测试通过:
  - `coro_worker_does_mysql_handshake`: 纯协程 worker 用 io_uring socket 完成真实 MySQL 客户端完整握手 (发 HandshakeV10 → 收 HandshakeResponse41 → 校验 native_password → 发 OK)
  - **结论**: 协程调度 + io_uring socket 收发 + 真实 MySQL 握手链路全通。后续把该原型固化为 `worker_coro.rs` 并接入 server; SQL 查询等 shard 交互留 Phase 2
- [ ] **T1.3** 作为**可回退开关**接入 `server.rs` (env/配置切换协程 worker vs 旧 epoll worker), 旧路径完全保留
- [ ] **验收**: 新协程 worker 单连接跑通真实 SQL; 旧 epoll worker 路径全绿 (network 测试无回归)

### Phase 1b: 协议栈协程化 (方向 B — 用户选定, 一步到位)

> **策略 (调研结论)**: 现有 `process_*_input` 是"纯解析→push→返回"事件驱动, 内部不等待 shard 回包 (回包走 REPLY_TOKEN 事件)。因此**无需 await 链**。改动集中在收发 IO + TLS + 一处 push 自旋。**关键**: 测试全部 `tls_config: None`, TLS 路径无测试覆盖, 可独立后续处理不阻塞核心。**架构**: 每 worker 单线程 executor (保留 `Rc<RefCell>` sql_cache) + 每连接一协程 + 回包事件驱动。

- [ ] **T2.1** 底层 IO 原语: 封装 io_uring socket 异步读写 (复用 `io_ops::read/write` offset=u64::MAX, 已 T0.1 验证) — 提供 `async fn` 级别的 socket read/write
- [ ] **T2.2** `recv`/`send_bytes` async 化 (worker_conn.rs): WouldBlock 自旋改 await io_uring; TLS 路径暂保留同步 fallback (测试未启用, 独立后续)
- [ ] **T2.3** `resp_complete`/`resp_flush_ready`/`send_binary_response` async 化 (worker_conn.rs + 230+ 调用点加 await)
- [ ] **T2.4** 五个 `process_*_input` async 化 + 内部 send_bytes/resp_complete 调用点加 await; 公共 `push_task`/`sql_dispatch_stmt` 适配
- [ ] **T2.5** sql_dml 巨型 INSERT 的 push 自旋 (`drain_replies`+`yield_now`, sql_dml.rs:259-275) 改 async await
- [ ] **T2.6** 新建 `worker_coro.rs`: 每连接一协程 + 协程 main loop (new_conn_loop + reply_loop), 接入 server (可回退开关 env `NEXUS_CORO_WORKER`)
- [ ] **T2.7** 验证: 协程 worker 跑通真实 SQL 查询 (含 shard); 旧 epoll worker 全绿; 全量 network 测试回归
- [ ] **T3** (原 Phase 3) 全局共享 worker 池 + 多协议 (协程 worker 天然支持)
- [ ] **T4** (原 Phase 4) TLS 协程化 + 清理 + 文档

### Phase 2: 每连接一个协程 (替换 epoll 连接管理)

> **目标**: 结构性重构 — worker 从 "epoll + conn_map" 改为 "Scheduler + 连接协程"。

- [ ] **T2.1** worker 主循环改为 `Scheduler::run()`: 内部 spawn 一个 `new_conn_loop` 协程, 从 inbox 收连接并 spawn 每连接协程
- [ ] **T2.2** 每连接一个协程: 协程内 `loop { await read → 协议 parse → push_task → await reply → 渲染 → await write }`
- [ ] **T2.3** `ConnState` 状态从 `HashMap<conn_id, ConnState>` 迁入协程局部变量 (每协程 own 一个 ConnState)
- [ ] **T2.4** reply 协程化: 把 reply_bus 封装为 `await` (按 conn_id 匹配回包, Waker 注册), 替代 epoll 监听 reply_bus eventfd
- [ ] **验收**: 单 worker 多连接并发正确; 各协议 e2e 通过; 无死锁

### Phase 3: 全局共享 worker 池 + 多协议

> **目标**: 解决"线程数随协议数膨胀", 让 worker 数 = 用户配置。

- [ ] **T3.1** `server.rs`: 多端口 acceptor 共享一个全局 worker 池 (而非每 server 各建池)
- [ ] **T3.2** accept 时按端口标记协议, 连接协程创建时用它初始化 `ConnState.proto` (替代从 `cfg.protocol` 取固定值)
- [ ] **T3.3** `WorkerConfig` 去掉固定 `protocol`, 改为每连接传递协议上下文; 移除 per-server worker_count 语义, 改为全局 worker 数
- [ ] **T3.4** 多协议混合压力验证: 同一 worker 池同时处理 RESP + SQL + PG 连接, 验证协程内按 `ConnState.proto` 分发正确
- [ ] **验收**: 线程数 = 全局配置 worker 数 (与协议数无关); 多协议 e2e 全绿

### Phase 4: 清理与文档

- [ ] **T4.1** 清理: 删除不再使用的 epoll 代码路径 (`worker_epoll.rs` 事件循环部分), 移除 WouldBlock 自旋残留
- [ ] **T4.2** 更新 `lib.rs` / 模块注释, 使其与实际实现一致 (去掉"每 worker own 1 Scheduler"的过时注释或不符描述)
- [ ] **T4.3** 更新 README / GUIDE: 网络层协程化、线程模型、多协议共享 worker 池说明
- [ ] **T4.4** 新增/更新测试: 覆盖多协议共享 worker 池的并发场景
- [ ] **验收**: 全量测试通过; 文档与实际一致

---

## 5. 关键风险与对策

| 风险 | 等级 | 对策 |
|---|---|---|
| **reply 协程化 (D3) 最难**, 跨线程回包要按 conn_id 唤醒对应协程, 易死锁/丢失 | 高 | Phase 2 单独做; 用 Waker 注册到 reply_bus receiver; 先实现单 worker 再扩展; 大量并发测试兜底 |
| **socket io_uring 不稳定** (某些内核/配置对 socket 支持有差异) | 中 | Phase 0 T0.1 先验证; 保留同步路径 fallback (可回退开关) |
| **TLS (rustls) 同步路径** 与协程模型冲突 | 中 | 单独处理: 握手可临时同步, 数据收发走 io_uring |
| **ConnState 巨型状态迁移** 出错 | 中 | Phase 2 逐字段迁移 + 每步编译验证; 协程局部变量化后反而更清晰 |
| **多协议混合回归** | 中 | Phase 3 增加混合压力测试; 现有各协议 e2e 全覆盖 |
| **性能回退** (io_uring 对 socket 的 syscall 减少 vs epoll 的成熟) | 低 | Phase 1 末做 A/B 对比; 若 socket io_uring 无收益可考虑仅保留协程调度、socket 走非阻塞 poll |

---

## 6. 明确非目标 (out-of-scope)

- ❌ 不引入 `tokio` / `monoio` 等第三方 async runtime (继续自研 `scheduler`)
- ❌ 不改变存储层 shard 的协程调度 (它已是目标形态)
- ❌ 不做"单端口多协议嗅探" (保留多端口, 端口即协议)
- ❌ 不实现 SO_REUSEPORT / 多 acceptor 调优 (后续单独规划)
- ❌ 不引入 QUIC / gRPC 等新协议
- ❌ 不改变协议语义 / SQL 能力 / 数据格式 (纯架构重构, 功能零变化)

---

## 7. 验收标准 (Definition of Done)

- [ ] `cargo build --workspace` 通过, 0 error
- [ ] `cargo clippy --all-targets` 0 warning
- [ ] `cargo test -p network` 全绿 (77 unit + 45 sql_e2e + 22 resp_e2e + 7 pg_e2e)
- [ ] `cargo test -p network --test sql_bigdata` 全绿 (大数据 5 测试)
- [ ] `cargo test -p storage` / `-p shard_manager` / `-p scheduler` 全绿 (存储层零回归)
- [ ] **线程数验证**: 启动多协议 server, 确认 worker 线程数 = 全局配置数, 而非 ×协议数
- [ ] **多协议混合**: 同一 worker 池同时处理 RESP + SQL + PG 连接, 各协议功能正确
- [ ] README / 注释与实际实现一致

---

## 8. 关联文档

- 初始设计: `docs/plans/2026-07-25-async-network-stack.md` (协程化网络层的最初规划, 本次回归该设计)
- scheduler crate: `crates/scheduler/src/` (`scheduler.rs`, `io_ops.rs`, `park.rs`)
- 当前 worker: `crates/network/src/worker/` (`worker_epoll.rs`, `worker_conn.rs`, `resp_*.rs`, `sql_*.rs`)
- 存储层参考实现: `crates/shard_manager/src/shard_thread.rs` (每 shard 一协程调度的成熟范例)
