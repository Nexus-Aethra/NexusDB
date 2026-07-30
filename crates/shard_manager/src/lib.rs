//! # ShardManager: 多 shard 统一控制器
//!
//! **职责**: 在 `storage` crate (单 StorageEngine + 单 Scheduler) 之上, 提供:
//! - **多 shard 路由**: 同一 (db, table, key) 永远路由到同一 shard
//! - **per-shard 线程**: 每个 shard 一个独立 `std::thread` + 自己的 Scheduler
//! - **mpsc 跨线程通信**: ShardManager 主线程 → shard 线程 (用 mpsc channel)
//! - **oneshot reply**: 同步 API 把异步结果返回给调用方
//! - **2PC 跨 shard 协调**: create_db / create_table 走两阶段提交 (T14)
//!
//! **架构**:
//! ```text
//!   ShardManager
//!   ├── num_shards: usize
//!   ├── router: Arc<dyn Router>            // hash 路由
//!   ├── shards: Vec<ShardHandle>           // mpsc Sender
//!   ├── _threads: Vec<JoinHandle<()>>      // shard 线程
//!   └── coordinator: TwoPhaseCoordinator  // 2PC 协调器 (T14)
//!
//!   shard 线程 (×N):
//!   ├── Scheduler (独立 io_uring)
//!   ├── StorageEngine (单 db namespace)
//!   ├── Rc<RefCell<StorageEngine>>         // 共享指针解决 engine 共享
//!   └── loop: mpsc.recv() → inline await engine.handle(req)
//! ```
//!
//! **关于 engine 共享 (重要!)**:
//! - `StorageEngine` 是 `&mut self` API, 不能跨协程同时持有
//! - 但我们又要在 `Scheduler::submit` 上下文里 spawn 协程
//! - **解决**: `Rc<RefCell<StorageEngine>>` 单线程共享, inline await (不 spawn 多协程)
//!   - 每次只处理一个请求, 串行访问 engine
//!   - IO 并发由 io_uring 内部 overlap (多 SQE inflight) 解决
//! - 这样**避免 RefCell borrow_mut 跨 await 持有**导致 panic
//!
//! **生命周期契约**:
//! - `ShardManager::open` 创建 N 个 shard 线程 (永远 run)
//! - 跨线程通信用 mpsc (不用 JoinHandle 跨线程, 避免破坏 UnsafeCell 契约)
//! - `ShardManager::close` 发送 Shutdown 给所有 shard, 等 join
//!
//! **设计**:
//! - 路由 key: `(db_name, table_name, key)` 三元组 hash
//! - 跨 db 协调 (create_db 同步 MetaPage): 2PC 路径 (T14 实施)

#![allow(dead_code)] // crate 还在搭骨架

pub mod coordinator;
pub mod error;
pub mod inbox;
pub mod latency_probe;
pub mod manager;
pub mod reply;
pub mod request;
pub mod router;
pub mod task_inbox;
pub mod task_reply_bus;
pub mod value_num;

pub use latency_probe::PROBE;

pub use coordinator::TwoPhaseCoordinator;
pub use error::{ShardError, ShardResult};
pub use inbox::{SharedInbox, ShardInbox};
pub use manager::{DbDirView, ReplySink, ShardManager, ShardManagerOptions};
pub use reply::{PendingReply, ReplyFuture, ReplySender};
pub use request::{BatchOp, BatchResult, ShardId, ShardReply, ShardRequest, ShardResponse, ShardTask, TaskResult};
pub use request::{PredOp, ScanPred};
pub use request::IndexHint;
pub use request::KeySetHint;
pub use router::{HashRouter, Router};
pub use task_inbox::{SharedTaskInbox, TaskInbox};
pub use task_reply_bus::{ReplyBusSet, SharedTaskReplyBus, TaskReplyBus};
