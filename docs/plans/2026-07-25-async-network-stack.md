# Async Network Stack with Per-Thread Scheduler

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `scheduler` / `shard_manager` 已有的 per-shard-thread 协程调度基础上, 加入网络层入口, 用 **1 个 acceptor + N 个 worker** 的两层模型, 把 TCP 连接接受、协议解析、请求分发、回复回送全部纳入协程调度, 消除当前 `pollster::block_on` + `Condvar` 引入的 futex syscall 瓶颈, 将同步 `ShardManager` API 抽象为可同步调用也可 async await 的统一协议栈.

**本次计划范围 (in-scope):**
- 自家二进制 codec (`BinaryProtocol`) — 字节 ↔ KV 转换
- KV ↔ ShardManager API 转换层 (Application Layer)
- 1 Acceptor + N Workers TCP 接入框架
- 把 shard reply 通过 `crossbeam mpmc` 路由回 worker

**明确不在本次范围 (out-of-scope, 留待后续):**
- ❌ RESP / Redis 兼容协议 (Phase 6+ 单独规划)
- ❌ 认证 / 登录 / 鉴权 (Phase 7+)
- ❌ TLS / 加密 (Phase 8+)
- ❌ HTTP/gRPC 适配 (Phase 9+)
- ❌ QUIC (Phase 10+)
- ❌ 多 acceptor + `SO_REUSEPORT` (Phase 11+, 单 acceptor 不够时)
- ❌ 任何 sharded waker table / 跨 worker task 调度优化 (Phase 12+)

**非目标 (out-of-scope):**
- 不引入 `tokio` / `monoio` 等第三方 async runtime
- 不修改现有 per-shard-thread 架构 (`Scheduler: !Send + !Sync`)
- 不替换现有 `pollster::block_on` 调用点 (用包装兼容, 不破坏现有 client API)
- 不引入 `quinn` / `tonic` 等网络协议库

**Architecture (after this plan):**

```
              ┌──────────────────────────────────────────────────┐
              │                external clients                  │
              │      (test client, future python/grpc clients)   │
              └────────────────────────┬─────────────────────────┘
                                       │ TCP :9000
                                       ▼
              ┌──────────────────────────────────────────────────┐
              │  1 Acceptor Thread (dedicated, no scheduler)     │
              │   loop:                                          │
              │     listener.accept()                            │
              │     LB = round-robin(next_worker)               │
              │     send (RawFd, conn_meta) → worker LB         │
              │   no scheduler: pure blocking accept + send      │
              └────────────────────────┬─────────────────────────┘
                                       │ std::os::unix::net::UnixStream
                                       │ (per-worker bounded queue)
                                       ▼
              ┌──────────────────────────────────────────────────┐
              │  N Worker Threads (each owns 1 Scheduler)        │
              │   thread i:                                       │
              │     Scheduler::new() (own io_uring, own registry)│
              │     task pool:                                    │
              │       ├ task[0] = conn_A_handle                   │
              │       ├ task[1] = conn_B_handle                   │
              │       └ task[k] = new_conn_spawn_loop            │
              │   cross-thread comm:                              │
              │     mpsc send → shard_request_bus                 │
              │     recv reply  ← reply_bus (mpmc)               │
              └────────────────────────┬─────────────────────────┘
                                       │ crossbeam mpmc
                                       ▼
              ┌──────────────────────────────────────────────────┐
              │  M Shard Threads (existing, per-shard scheduler) │
              │   inline engine:                                  │
              │     read chunk / write chunk / group commit fsync │
              │   reply_bus.push(req_id, response)                │
              └──────────────────────────────────────────────────┘
                                       ▲
                                       │ reply_bus
              ┌────────────────────────┴─────────────────────────┐
              │  ReplyBus: crossbeam::channel::unbounded          │
              │     sender: shard thread (任意时刻)                │
              │     receivers: N worker threads                    │
              │     payload: (req_id, ShardResponse)              │
              └──────────────────────────────────────────────────┘
```

**核心合约 (per-thread scheduler invariant):**
- 每个 `Scheduler` 实例由 1 个 OS thread 独占, 不可 `Send` / `Sync`
- 跨 scheduler 通信永远走 `crossbeam` / `std::sync::mpsc` (lock-free or 边界 mutex)
- 协程 `await` 一个跨线程 future 时, 把当前 `Waker` 注册进 reply_bus 的 receiver 端, **不** 阻塞线程

**Tech Stack:**
- `crossbeam-channel = "0.5"` (新增依赖)
- `libc = "0.2"` (新增, 用于 `SCM_RIGHTS`, `pipe2`)
- 既有 `scheduler`, `shard_manager`, `storage`, `page` 全部不动
- 新 crate: `crates/network/` (新)

**关联 design doc:**
- `crates/scheduler/src/scheduler.rs` §Waker 注册
- `crates/shard_manager/src/manager.rs` §per-shard-thread
- 现有 `ReplyFuture` / `pollster::block_on` 调用点 (兼容层)

---

## Phase 0: 准备与现状摸底

**目的:** 在改动前, 量化当前同步 API 的 futex 开销, 验证假设.

### Task 0.1: 当前 baseline benchmark

- [ ] **文件:** `crates/shard_manager/examples/stress.rs`
- [ ] **动作:** 关闭所有 stderr debug print (`println!` / `eprintln!` for `[leaf]` `[index]` etc)
- [ ] **命令:**
  ```bash
  cargo run --release --example stress -- 1000 6 6
  strace -c -e futex,write,fsync,io_uring_enter ./target/release/examples/stress 1000 6 6
  ```
- [ ] **记录:** baseline ops/sec, futex 总数, write 总数
- [ ] **预期:** ops/sec ≈ 750-1500 (跟之前测试一致)

### Task 0.2: 创建 network crate 骨架

- [ ] **新增:** `crates/network/Cargo.toml`
  ```toml
  [package]
  name = "network"
  version = "0.1.0"
  edition = "2024"
  [dependencies]
  scheduler = { path = "../scheduler" }
  shard_manager = { path = "../shard_manager" }
  crossbeam-channel = "0.5"
  libc = "0.2"
  tokio = { version = "1", features = ["rt", "net", "io-util", "sync"], optional = true }
  [dev-dependencies]
  tempfile = "3"
  ```
- [ ] **新增:** `crates/network/src/lib.rs`
  ```rust
  pub mod acceptor;
  pub mod worker;
  pub mod protocol;
  pub mod reply_bus;
  pub use acceptor::{Acceptor, AcceptorConfig};
  pub use worker::{WorkerPool, WorkerConfig};
  pub use protocol::{Protocol, Request, Response};
  ```
- [ ] **修改:** 根 `Cargo.toml`, 加入 `crates/network`
- [ ] **验证:** `cargo build -p network`

### Task 0.3: Protocol trait 设计 (扩展性预留, 未来兼容多 codec)

- [ ] **决策:** Protocol 抽象为 trait, **与 shard 完全解耦**——纯字节 ↔ KV 转换, 不参与 shard 调度
- [ ] **本次只实现 `BinaryProtocol` 一种 codec**, 不写 RESP, 不写 protobuf
- [ ] **设计原则:** Protocol 是 OSI 7 层中的 Presentation Layer
  - 上层 (Application) = KV ↔ Shard API 转换 (在 worker 里)
  - 下层 (Transport) = TCP/conn fd 处理 (在 worker scheduler 里)
- [ ] **trait 定义:**
  ```rust
  pub trait Protocol: Send + Sync + 'static {
      type Error: std::error::Error + Send + Sync + 'static;

      /// 帧边界信息: 给定 buf, 返回 (consumed_bytes, parsed_request)
      /// 返回 0 表示需要更多字节 (帧不完整)
      fn decode_request(&self, buf: &[u8]) -> Result<DecodeOutcome<Request>, Self::Error>;

      /// Request → 字节流
      fn encode_request(&self, req_id: u64, req: &Request) -> Vec<u8>;

      /// 帧边界信息: 给定 buf, 返回 (consumed_bytes, parsed_response)
      fn decode_response(&self, buf: &[u8]) -> Result<DecodeOutcome<Response>, Self::Error>;

      /// Response → 字节流
      fn encode_response(&self, req_id: u64, resp: &Response) -> Vec<u8>;

      /// 最大消息尺寸 (防 DoS)
      fn max_frame_size(&self) -> usize { 16 * 1024 * 1024 }
  }

  pub enum DecodeOutcome<T> {
      Complete { consumed: usize, value: T },
      NeedMore,
  }
  ```
- [ ] **关键:** `decode_*` 返回 `consumed` 是为了支持"一次 read 多帧"或"半帧缓存"
- [ ] **KV 抽象 (跟 shard 解耦):**
  ```rust
  pub enum Request {
      Put { key: Vec<u8>, value: Vec<u8> },
      Get { key: Vec<u8> },
      Delete { key: Vec<u8> },
  }

  pub enum Response {
      PutOk,
      Get(Option<Vec<u8>>),
      DeleteOk,
      Error(String),
  }
  ```
- [ ] **未来实现:** `BinaryProtocol` (本次, Phase 3.1), `RespProtocol` (Phase 6, 单独 plan, **本次不做**)
- [ ] **本次不做:** RESP 兼容 (Phase 6)
- [ ] **本次不做:** protobuf / flatbuffers (Phase 6+)
- [ ] **本次不做:** 认证 / 登录 / 鉴权 (Phase 7+)
- [ ] **本次不做:** TLS 加密 (Phase 8+)
- [ ] **不做:** Protocol trait 不知道 shard 存在, 不接触 ShardManager

---

## Phase 1: ReplyBus 基础设施 (Phase 1 关键: 让 ReplyFuture 走 Waker)

**目的:** 替换现有 `pollster::block_on + Condvar` 为基于 `Waker` 的协程式等待.

### Task 1.1: ReplyBus 类型定义

- [ ] **新增:** `crates/network/src/reply_bus.rs`
- [ ] **类型:**
  ```rust
  pub struct ReplyBus {
      inner: crossbeam_channel::unbounded::UnboundedSender<ReplyEnvelope>,
      recv_handle: crossbeam_channel::unbounded::UnboundedReceiverHandle,
      // per-worker 接收端持引用计数, 用于按 req_id 路由
  }

  pub struct ReplyEnvelope {
      pub req_id: u64,
      pub shard_id: u32,
      pub response: Result<Vec<u8>, String>,  // 序列化后字节流
  }
  ```
- [ ] **API:**
  ```rust
  impl ReplyBus {
      pub fn new() -> (ReplyBusSender, ReplyBusReceiver);
      pub fn push(&self, env: ReplyEnvelope);
      pub fn pop(&self) -> Option<ReplyEnvelope>;
  }
  ```
- [ ] **关键:** 使用 `crossbeam_channel` 自带的 mpmc, 满足多 shard producer + 多 worker consumer
- [ ] **测试:** `crates/network/tests/reply_bus.rs` — 4 producer, 4 consumer, 1000 条 message
- [ ] **验证:** `cargo test -p network reply_bus`

### Task 1.2: ShardManager 集成 ReplyBus (双模 API)

- [ ] **修改:** `crates/shard_manager/src/manager.rs`
- [ ] **新增字段:** `ShardManager { reply_bus: Option<ReplyBusSender> }`
- [ ] **新增方法:**
  ```rust
  impl ShardManager {
      /// 启用 async 模式: 注册 reply_bus
      pub fn enable_async(&mut self, bus: ReplyBusSender);

      /// 新 API: 返回 req_id, 不阻塞等待 reply
      pub fn put_async(&self, key: &[u8], value: &[u8]) -> u64;

      /// 老 API: 保留兼容, 内部走 async + 阻塞等 reply_bus
      pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;  // 现有签名
  }
  ```
- [ ] **关键:** `put_async` 路径发送请求时附带 `req_id`, shard 完成后写入 `reply_bus.push(...)`
- [ ] **保留:** 现有 `put()` API 行为不变, 内部用 `pollster::block_on` 等 reply_bus (但这一步**已经消除了 Mutex/Condvar**)
- [ ] **验证:** `cargo test --workspace`

### Task 1.3: Shard 端完成路径接 reply_bus

- [ ] **修改:** `crates/shard_manager/src/manager.rs::shard_thread_main`
- [ ] **改动:** shard 处理完一个 op 后:
  ```rust
  // 旧:
  reply_future.complete(response);

  // 新:
  if let Some(bus) = &reply_bus {
      bus.push(ReplyEnvelope { req_id, shard_id, response });
  } else {
      reply_future.complete(response);  // fallback
  }
  ```
- [ ] **关键:** `req_id` 在 shard 端从 `ShardRequest` 字段拿
- [ ] **验证:** 现有 `stress.rs` 应该跟基线一致 (因为 fallback)

### Task 1.4: 重新跑 baseline, 验证 futex 下降

- [ ] **命令:**
  ```bash
  strace -c -e futex ./target/release/examples/stress 1000 6 6
  ```
- [ ] **预期:** futex 调用数应从 ~1.9M 降到 < 500K (因为 reply 路径走了 crossbeam, 无 Mutex/Condvar)

---

## Phase 2: Scheduler 扩展: Waker 注册 API

**目的:** 让 `Scheduler` 显式支持 "park 当前 task + 注册 waker + 等 reply", 这是替换 pollster 的基础.

### Task 2.1: Scheduler 加 park_with_waker

- [ ] **修改:** `crates/scheduler/src/scheduler.rs`
- [ ] **新增方法:**
  ```rust
  impl Scheduler {
      /// 注册 waker, 当前 task 暂停; 等 waker.wake() 后恢复
      /// 返回 false 表示超时或错误
      pub fn park_with_waker(
          &self,
          waker: &core::task::Waker,
          predicate: impl FnMut() -> bool,
          timeout: Option<Duration>,
      ) -> bool;
  }
  ```
- [ ] **关键实现:**
  - 内部 `RefCell<HashMap<TaskId, Waker>>` (per-scheduler)
  - 调用 driver task: `predicate()` true 时立即 wake; false 时 park + 加到 map
  - 内置 `TimerFD` 一次性 timer 实现 timeout
- [ ] **测试:** `crates/scheduler/tests/park_with_waker.rs`
  - 立即满足 predicate: 应该不 park
  - 不满足 predicate, 另一个 task 调 waker.wake(): 应该 resume
  - timeout: 应该返回 false
- [ ] **验证:** `cargo test -p scheduler`

### Task 2.2: ReplyFuture 改写为基于 park_with_waker

- [ ] **修改:** `crates/shard_manager/src/reply.rs::ReplyFuture::poll`
- [ ] **新实现:**
  ```rust
  impl Future for ReplyFuture {
      fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<...> {
          if self.completed.load(Acquire) { return Ready(...); }
          // 注册 waker (per-task 在本 scheduler 内部)
          self.waker.set(cx.waker().clone());
          // 不再调 Condvar.wait
          Pending
      }
  }
  ```
- [ ] **关键:** `waker.set` 后, shard 端完成时**不再**通过 Condvar, 而是通过 `waker.wake_by_ref()` 触发 (此处的 waker 在 worker scheduler 内部)
- [ ] **改动:**
  - `pollster::block_on` 改为单 driver thread + epoll_wait 批量 wake
- [ ] **验证:** 现有所有 integration test 必须通过

---

## Phase 3: 网络层骨架 (Acceptor + Worker Pool)

**目的:** 建立最小可工作的 TCP listener, 通过 crossbeam 把请求分发到 shard.

### Task 3.1: Wire Protocol Codec 实现 (二进制, 与 Shard 解耦)

- [ ] **新增:** `crates/network/src/protocol/mod.rs` (定义 Request/Response/Protocol trait)
- [ ] **新增:** `crates/network/src/protocol/binary.rs` (BinaryProtocol 实现, **本次唯一 codec**)
- [ ] **格式:**
  ```
  | total_len: u32 BE | req_id: u64 BE | op: u8 | key_len: u16 BE | val_len: u32 BE | key: [u8] | val: [u8] |
  ```
- [ ] **BinaryCodec:**
  ```rust
  pub struct BinaryProtocol;

  impl Protocol for BinaryProtocol {
      type Error = BinaryProtocolError;
      
      fn decode_request(&self, buf: &[u8]) -> Result<DecodeOutcome<Request>, Self::Error> {
          if buf.len() < HEADER_LEN { return Ok(DecodeOutcome::NeedMore); }
          let total_len = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
          if buf.len() < total_len { return Ok(DecodeOutcome::NeedMore); }
          let req_id = u64::from_be_bytes(buf[4..12].try_into().unwrap());
          let op = buf[12];
          let key_len = u16::from_be_bytes(buf[13..15].try_into().unwrap()) as usize;
          let val_len = u32::from_be_bytes(buf[15..19].try_into().unwrap()) as usize;
          let key = buf[19..19+key_len].to_vec();
          let val = buf[19+key_len..19+key_len+val_len].to_vec();
          
          let req = match op {
              OP_PUT => Request::Put { key, value: val },
              OP_GET => Request::Get { key },
              OP_DELETE => Request::Delete { key },
              _ => return Err(BinaryProtocolError::UnknownOp(op)),
          };
          Ok(DecodeOutcome::Complete { consumed: total_len, value: req })
      }
      
      // ... encode_request / decode_response / encode_response 类似
  }
  ```
- [ ] **关键约束:** `BinaryProtocol` 不接触 `ShardManager`, 不接触 scheduler, 不接触 IO
- [ ] **测试:** `crates/network/tests/protocol_binary.rs`
  - 完整 round-trip (encode → decode)
  - 半帧返回 NeedMore
  - 多帧连拼返回多次
  - 错误 opcode
  - 超大消息拒绝 (max_frame_size)
  - 空 key/value
  - unicode bytes
- [ ] **验证:** `cargo test -p network protocol_binary`

### Task 3.2: KV → Shard API 转换层 (业务层, 在 worker)

- [ ] **新增:** `crates/network/src/kv_to_shard.rs`
- [ ] **关键:** 这一层是 Application Layer, 把 `Request`/`Response` 翻译成 `ShardManager` 调用
- [ ] **API:**
  ```rust
  pub async fn dispatch_request(
      shard_manager: &ShardManager,
      req: Request,
  ) -> Response {
      match req {
          Request::Put { key, value } => match shard_manager.put(&key, &value).await {
              Ok(_) => Response::PutOk,
              Err(e) => Response::Error(format!("put failed: {e}")),
          },
          Request::Get { key } => match shard_manager.get(&key).await {
              Ok(Some(v)) => Response::Get(Some(v)),
              Ok(None) => Response::Get(None),
              Err(e) => Response::Error(format!("get failed: {e}")),
          },
          Request::Delete { key } => match shard_manager.delete(&key).await {
              Ok(_) => Response::DeleteOk,
              Err(e) => Response::Error(format!("delete failed: {e}")),
          },
      }
  }
  ```
- [ ] **关键:** 这一层独立可测, 完全不知道 codec 长什么样
- [ ] **测试:** `crates/network/tests/kv_to_shard.rs`
  - 真实 ShardManager (临时目录) 上 put/get/delete round-trip
  - 错误 path (key 太大, shard 已关闭)
- [ ] **验证:** `cargo test -p network kv_to_shard`

### Task 3.3: Acceptor 线程实现

- [ ] **新增:** `crates/network/src/acceptor.rs`
- [ ] **类型:**
  ```rust
  pub struct AcceptorConfig {
      pub listen_addr: SocketAddr,
      pub worker_queues: Vec<crossbeam_channel::bounded::Sender<NewConn>>,
      pub lb_strategy: LbStrategy,
  }

  pub enum LbStrategy { RoundRobin, Random, Sticky }

  pub struct Acceptor;

  impl Acceptor {
      pub fn run(config: AcceptorConfig) -> io::Result<()>;
  }
  ```
- [ ] **行为:**
  ```rust
  pub fn run(config: AcceptorConfig) -> io::Result<()> {
      let listener = TcpListener::bind(config.listen_addr)?;
      let mut next_worker = AtomicUsize::new(0);
      loop {
          let (stream, peer) = listener.accept()?;
          let idx = match config.lb_strategy {
              LbStrategy::RoundRobin => next_worker.fetch_add(1, Relaxed) % config.worker_queues.len(),
              LbStrategy::Random => rand::random::<usize>() % config.worker_queues.len(),
              LbStrategy::Sticky => hash(&peer) % config.worker_queues.len(),
          };
          // 转移 fd ownership
          config.worker_queues[idx].send(NewConn { fd: stream.into_raw_fd(), peer })?;
      }
  }
  ```
- [ ] **关键:** `stream.into_raw_fd()` 后用 `send_fd_via_unix_socket` 或者直接把 fd 编号包在 `NewConn` 里让 worker 通过 `unsafe { from_raw_fd }` 重建
- [ ] **简化版:** 用 std `UnixStream` 作为 worker inbox (`SCM_RIGHTS` 自动处理); 这只是 Phase 3 简化实现
- [ ] **测试:** `crates/network/tests/acceptor.rs` — 用 std::net::TcpStream 连一下, 收到自己的 echo

### Task 3.4: Worker 线程 + 自有 Scheduler

- [ ] **新增:** `crates/network/src/worker.rs`
- [ ] **类型:**
  ```rust
  pub struct WorkerConfig {
      pub worker_id: usize,
      pub inbox: crossbeam_channel::Receiver<NewConn>,
      pub shard_manager: ShardManagerHandle,
      pub reply_bus: ReplyBusReceiver,
  }

  pub struct WorkerPool {
      handles: Vec<JoinHandle<()>>,
  }

  impl WorkerPool {
      pub fn start(configs: Vec<WorkerConfig>) -> Self;
      pub fn join(self) -> io::Result<()>;
  }
  ```
- [ ] **行为:**
  ```rust
  fn worker_main(cfg: WorkerConfig) {
      let scheduler = Scheduler::new();
      let conn_table: RefCell<HashMap<u64, ConnState>> = RefCell::new(HashMap::new());
      let mut req_id_gen = AtomicU64::new(0);

      // spawn inbox task
      scheduler.spawn_local(async move {
          while let Ok(new_conn) = cfg.inbox.recv() {
              let conn_id = new_conn.fd as u64;
              let conn = ConnState::new(new_conn.fd);
              conn_table.borrow_mut().insert(conn_id, conn);
              scheduler.spawn_local(handle_conn(conn_id, /* ... */));
          }
      });

      // spawn reply poll task
      scheduler.spawn_local(async move {
          loop {
              if let Ok(env) = cfg.reply_bus.pop() {
                  // 通过 conn_table 找对应 task, wake it
                  // ... 但这需要跨 task 调度
              }
              // 等 reply_bus 进来
              scheduler.park_with_waker(...);
          }
      });

      scheduler.run_until_stopped();
  }
  ```
- [ ] **关键:** `ReplyBus::pop()` 是 blocking 还是 non-blocking 需要决策; **首版** 用 `recv()` blocking on channel (但 channel recv 不是 syscall, 是 spin + futex, 比 mutex 轻), 后续优化可用 `select!`
- [ ] **测试:** `crates/network/tests/worker.rs` — 启动 worker pool, 单连接, send put + get, 验证 response

---

## Phase 4: 集成与端到端测试

**目的:** 把 acceptor + worker pool + shard_manager 拼起来, 跑端到端 test.

### Task 4.1: NetworkServer 顶层组装

- [ ] **新增:** `crates/network/src/server.rs`
- [ ] **类型:**
  ```rust
  pub struct NetworkServer {
      acceptor_handle: JoinHandle<()>,
      worker_handles: Vec<JoinHandle<()>>,
      reply_bus_tx: ReplyBusSender,
      shard_manager: Arc<ShardManager>,
  }

  impl NetworkServer {
      pub fn start(
          listen_addr: SocketAddr,
          shard_manager: Arc<ShardManager>,
          worker_count: usize,
      ) -> io::Result<Self>;

      pub fn shutdown(self) -> io::Result<()>;
  }
  ```
- [ ] **行为:** 内部:
  1. 创建 `ShardManager::enable_async(reply_bus_tx.clone())`
  2. 创建 N 个 `WorkerConfig` (inbox + shard_handle + reply_bus_rx.clone())
  3. 启动 worker threads (`WorkerPool::start`)
  4. 启动 acceptor (`Acceptor::run`)
- [ ] **验证:** `cargo build`

### Task 4.2: 端到端 integration test

- [ ] **新增:** `crates/network/tests/end_to_end.rs`
- [ ] **测试:**
  ```rust
  #[tokio::test]  // 或 std::net
  async fn put_get_roundtrip() {
      let tempdir = tempfile::tempdir().unwrap();
      let mgr = Arc::new(ShardManager::start(tempdir.path(), ShardManagerOptions::default()).await.unwrap());
      let server = NetworkServer::start("127.0.0.1:0".parse().unwrap(), mgr, 3).unwrap();
      let addr = server.local_addr();

      let mut stream = TcpStream::connect(addr).await.unwrap();
      let req = encode_request(1, Request::Put { key: b"hello".to_vec(), value: b"world".to_vec() });
      stream.write_all(&req).await.unwrap();

      let mut buf = [0u8; 1024];
      let n = stream.read(&mut buf).await.unwrap();
      let (id, resp) = decode_response(&buf[..n]).unwrap();
      assert!(matches!(resp, Response::PutOk));
  }
  ```
- [ ] **场景覆盖:**
  - 单 conn 多请求
  - 多 conn 并发
  - shard 切换 (不同 key hash 到不同 shard)
  - 大量请求 (1K+)
  - 关闭: client 中途断开, worker 清理 task
- [ ] **验证:** `cargo test -p network --test end_to_end`

### Task 4.3: stress benchmark 重测

- [ ] **新增:** `crates/network/examples/network_stress.rs`
- [ ] **行为:** 用 network protocol 跑 1000 ops × 6 shard × 3 worker, 测 ops/sec
- [ ] **命令:**
  ```bash
  cargo run --release --example network_stress -- 1000 6 3
  strace -c -e futex ./target/release/examples/network_stress 1000 6 3
  ```
- [ ] **预期:**
  - ops/sec 应当比 baseline 高 (去除 stress.rs 内调用的 `pollster::block_on` futex)
  - futex 调用数应当显著下降
- [ ] **记录:** benchmark JSON `docs/benchmarks/2026-07-25-network-async.json`

---

## Phase 5: 优化与文档

**目的:** 收敛稳定性 + 写文档.

### Task 5.1: 文档 - 网络架构图

- [ ] **新增:** `docs/design/network-layer.md`
- [ ] **内容:**
  - Acceptor/Worker/Shard 三层职责
  - 数据流 (client → acceptor → worker → shard → reply_bus → worker → client)
  - 失败模式 (worker 死机 / shard 死机 / acceptor 死机)
  - LB 策略对比表

### Task 5.2: 文档 - 调度器合约

- [ ] **新增:** `docs/design/scheduler-invariants.md`
- [ ] **内容:**
  - `Scheduler: !Send + !Sync` 的原因
  - 跨 scheduler 通信必须走 channel
  - park_with_waker 用法
  - ReplyFuture 现在没有 Mutex 演示

### Task 5.3: 压测报告

- [ ] **生成:** `docs/benchmarks/2026-07-25-async-network.md`
- [ ] **内容:**
  - baseline (同步 API + pollster) vs Phase 1 (reply_bus) vs Phase 4 (full network stack)
  - ops/sec 对比
  - futex syscall 对比
  - p99 latency

---

## 任务总览表

| Phase | 任务 | 文件 | 工作量 | 风险 |
|------|------|------|--------|------|
| 0 | baseline benchmark | `stress.rs` | 0.5h | 无 |
| 0 | crate 骨架 | `crates/network/` | 1h | 无 |
| 1 | ReplyBus | `reply_bus.rs` | 4h | 中 (双模 API) |
| 1 | ShardManager 双模 | `manager.rs` | 4h | 中 (回退路径) |
| 1 | shard 完成路径 | `manager.rs` | 2h | 低 |
| 1 | baseline 验证 | test | 1h | 无 |
| 2 | park_with_waker | `scheduler.rs` | 4h | 中 (核心 API) |
| 2 | ReplyFuture 改写 | `reply.rs` | 4h | 高 (改动 lock 关键路径) |
| 3 | Wire Protocol | `protocol.rs` | 2h | 无 |
| 3 | Acceptor | `acceptor.rs` | 4h | 中 (fd 转移) |
| 3 | Worker | `worker.rs` | 6h | 中 (跨 task 通信) |
| 4 | 组装 | `server.rs` | 2h | 低 |
| 4 | 端到端 | `end_to_end.rs` | 4h | 中 |
| 4 | network stress | `network_stress.rs` | 2h | 低 |
| 5 | 文档 | 3 个 md | 3h | 无 |

**总计:** ~5 天

---

## 风险与缓解

| 风险 | 缓解 |
|------|------|
| ReplyBus 性能不够 | Phase 1 验证 futex 是否真的下降, 不行就回退 pollster |
| park_with_waker 唤醒 bug | 用现有 task model 单元测试覆盖 |
| fd 转移 (SCM_RIGHTS) 复杂 | 首版用简化 UnixStream inbox |
| 跨 worker ↔ shard 协调复杂度 | 每层都有独立 scheduler + 独立测试 |
| 完全替换 reply.rs 风险大 | 保留旧 Condvar 路径, 新 API 双模 |

---

## 不在本次计划内 (后续 phases)

- TLS / encryption (后续 phase)
- Quic / HTTP/2 (后续 phase)
- 多 acceptor + SO_REUSEPORT (后续 phase)
- sharded waker table (后续 phase)
- 替换所有 call site 的 pollster::block_on (后续 phase)

---

## 成功标准 (definition of done)

- [ ] Phase 0-5 所有 checkbox 全部 ✅
- [ ] `cargo test --workspace` 全部通过
- [ ] `cargo clippy --workspace -- -D warnings` 0 警告
- [ ] baseline vs Phase 4 ops/sec 至少 5× 提升
- [ ] futex syscall 在 Phase 4 至少下降 50%
- [ ] 端到端 test 覆盖: 单/多 conn, shard 切换, error path
- [ ] 文档完整 (`network-layer.md` + `scheduler-invariants.md` + benchmark 报告)
- [ ] 现有 sync API (`ShardManager::put`) 行为不变 (无 regression)