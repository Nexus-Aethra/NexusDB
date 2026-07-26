//! ShardManager: 多 shard 统一控制器.
//!
//! 详细设计见 crate 文档.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::thread;

use storage::OpenOptions;
use storage::StorageEngine;

use crate::coordinator::{TwoPhaseCoordinator, TxnOp};
use crate::error::{ShardError, ShardResult};
use crate::reply::{block_on_v2, PendingReply};
use crate::request::{BatchOp, BatchResult, ShardErrorKind, ShardId, ShardReply, ShardRequest, ShardResponse};
use crate::router::{HashRouter, Router};

/// 异步 2PC 阶段用的 future 集合: `Pin<Box<dyn Future<Output = ShardResponse> + Send>>`.
///
/// 提取出来避免 `complex_type` clippy 警告, 让 `commit_futs` / `abort_futs` 等变量类型更清晰.
pub(crate) type BoxedShardFuture = Pin<Box<dyn Future<Output = ShardResponse> + Send>>;

/// `(shard_id, future)` 对: 用于 abort 阶段需要回填 shard_id.
pub(crate) type ShardFutEntry = (usize, BoxedShardFuture);

/// ShardManager 配置.
#[derive(Debug, Clone)]
pub struct ShardManagerOptions {
    /// shard 数.
    pub num_shards: usize,
    /// 基础 block 目录. 每 shard 独立 `{block_root}/shard_{N}/`.
    pub block_root: std::path::PathBuf,
    /// 创建 block_dir 如果不存在.
    pub create_if_missing: bool,
    /// IO 后端 (默认 StdFs).
    pub io_backend: storage::IoBackend,
    /// T18c: 进阶 IO 后端配置 (FD 池 / 注册缓冲区 / SQPOLL / O_DIRECT).
    pub io_config: storage::IoBackendConfig,
    /// chunk cache size per shard.
    pub chunk_cache_size: usize,
    /// reply bus 数量 (None = num_shards). 多协议 server 并存时
    /// 需要 >= 所有 server 的 worker 总数 (worker_id 空间不重叠).
    pub reply_bus_count: Option<usize>,
}

impl ShardManagerOptions {
    /// 构造默认配置.
    pub fn new(num_shards: usize, block_root: std::path::PathBuf) -> Self {
        Self {
            num_shards,
            block_root,
            create_if_missing: true,
            io_backend: storage::IoBackend::StdFs,
            io_config: storage::IoBackendConfig::default(),
            chunk_cache_size: 4,
            reply_bus_count: None,
        }
    }
}

/// 单 shard 句柄: ShardManager 主线程持有 SharedInbox, shard 线程也持有同一个.
pub struct ShardHandle {
    pub id: ShardId,
    pub inbox: crate::inbox::SharedInbox,
    /// ⭐ 独立服务架构: task inbox (network → shard 直连).
    pub task_inbox: crate::task_inbox::SharedTaskInbox,
}

/// ShardManager: 多 shard 统一控制器.
///
/// **生命周期**:
/// 1. `ShardManager::open(opts)` 创建 N 个 shard 线程
/// 2. 用户调 `put/get/delete` (内部路由 + mpsc 发送)
/// 3. `ShardManager::close()` 发送 Shutdown 给所有 shard, 等 join
///
/// **2PC 协调**: create_db / create_table 走两阶段提交 (T14).
/// coordinator 状态机跟踪 prepare/commit/abort 阶段.
///
/// **T15 异步 API**: 同步 API 内部用 `PendingReply::new()` 拿 future + sender,
/// 然后 `block_on` 跑 (适合同步 caller). 异步 API (`put_async` 等) 直接返回
/// `ReplyFuture`, 不阻塞调用线程 (适合 Tokio/Axum 集成).
///
/// **T19 async network**: `enable_reply_bus()` 注入 `Arc<dyn ReplySink>` 后,
/// shard 端完成 Put/Get/Delete 时**同时**把结果写入 reply_bus (供 worker 异步路由).
/// 旧 `reply.send()` 调用保留, 同步 API 行为不变.
pub struct ShardManager {
    /// 所有 shard 的 sender (主线程持有, 用于发请求).
    shards: Vec<ShardHandle>,
    /// shard 线程 JoinHandle.
    threads: Vec<thread::JoinHandle<()>>,
    /// 路由策略.
    router: Arc<dyn Router>,
    /// num_shards (用于边界检查).
    num_shards: usize,
    /// ⭐ T14: 2PC 协调器.
    /// 用 `RefCell` 包装, 让 `&self` 方法也能访问 (单线程借用).
    coordinator: std::sync::Mutex<TwoPhaseCoordinator>,
    /// ⭐ T19: 网络层 reply sink (None = 旧行为, 仅 channel reply).
    /// ShardManager 和所有 shard 线程共享同一个 Arc<Mutex<...>>,
    /// enable_reply_bus 时写入, 各 shard 读出来 push_reply.
    reply_sink: Arc<StdMutex<Option<Arc<dyn ReplySink>>>>,
    /// ⭐ 独立服务架构: 所有 worker 的 reply bus 集合.
    pub reply_bus_set: Arc<crate::task_reply_bus::ReplyBusSet>,
}

/// ReplySink: shard 完成 KV op 后, 网络层用来接收路由回包的 sink.
///
/// **跨 crate**: `network::reply_bus::ReplyBusSender` 实现这个 trait.
/// shard 端只需克隆 `req_id` + shard_id + response 推入 sink.
pub trait ReplySink: Send + Sync + 'static {
    fn push_reply(&self, req_id: u64, shard_id: u32, resp: ShardResponse);
}

impl ShardManager {
    /// 创建 N 个 shard 线程, 每个 NEW Scheduler + StorageEngine.
    pub fn open(opts: ShardManagerOptions) -> ShardResult<Self> {
        let num_shards = opts.num_shards;
        assert!(num_shards > 0, "num_shards must be > 0");

        // 共享路由表 (不可变, 多线程读 OK)
        let router: Arc<dyn Router> = Arc::new(HashRouter::new(num_shards));

        let mut shards = Vec::with_capacity(num_shards);
        let mut threads = Vec::with_capacity(num_shards);

        // ⭐ T19: 共享 reply_sink (Arc<Mutex<Option<...>>>) 给所有 shard 用.
        let reply_sink_arc = Arc::new(StdMutex::new(None));

        // ⭐ 独立服务架构: 创建 reply bus set (默认 num_shards 个 worker bus)
        let reply_bus_set = Arc::new(crate::task_reply_bus::ReplyBusSet::new(
                    opts.reply_bus_count.unwrap_or(num_shards).max(1),
                ));

        for shard_id in 0..num_shards {
            // 每 shard 独立 block_dir
            let shard_block_dir = opts.block_root.join(format!("shard_{shard_id}"));
            let storage_opts = OpenOptions {
                block_root: shard_block_dir,
                block_dir: None,
                db_name: Some("default".to_string()),
                shard_id: shard_id as u32,
                create_if_missing: opts.create_if_missing,
                chunk_cache_size: opts.chunk_cache_size,
                io_backend: opts.io_backend,
                io_config: opts.io_config,
            };

            // ⭐ T20: ShardInbox 替代 mpsc channel
            let inbox = crate::inbox::new_shared_inbox();
            let task_inbox = crate::task_inbox::new_shared_task_inbox();
            shards.push(ShardHandle {
                id: shard_id,
                inbox: inbox.clone(),
                task_inbox: task_inbox.clone(),
            });

            // ⭐ 4MB 栈
            let router = router.clone();
            let reply_sink_clone = reply_sink_arc.clone();
            let reply_bus_clone = reply_bus_set.clone();
            let join = thread::Builder::new()
                .name(format!("shard-{shard_id}"))
                .stack_size(4 * 1024 * 1024)
                .spawn(move || shard_thread_main(shard_id, storage_opts, router, inbox, task_inbox, reply_sink_clone, reply_bus_clone))
                .map_err(ShardError::Io)?;
            threads.push(join);
        }

        Ok(Self {
            shards,
            threads,
            router,
            num_shards,
            coordinator: std::sync::Mutex::new(TwoPhaseCoordinator::new()),
            reply_sink: reply_sink_arc,
            reply_bus_set,
        })
    }

    /// num_shards.
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// ⭐ T19: 注册网络层 reply sink.
    ///
    /// 调用后, 所有 Put/Get/Delete op 完成时会同时 push 一份到 sink
    /// (req_id > 0 时). 同步/异步 API 行为不变, 旧 reply 仍然走.
    pub fn enable_reply_bus(&self, sink: Arc<dyn ReplySink>) {
        *self.reply_sink.lock().expect("reply_sink lock") = Some(sink);
    }

    /// 测试用: 读取 reply_sink 当前快照.
    #[doc(hidden)]
    pub fn _peek_reply_sink(&self) -> Option<Arc<dyn ReplySink>> {
        self.reply_sink.lock().expect("reply_sink lock").clone()
    }

    /// 路由: 给定 (db, table, key) 决定去哪个 shard.
    pub fn route(&self, db: &str, table: &str, key: &[u8]) -> ShardId {
        self.router.route(db, table, key)
    }

    // =================================================================
    // 公共 API: 同步阻塞 (内部用 block_on_v2 跑 ReplyFuture)
    // =================================================================

    /// Put: 路由到目标 shard, 同步等 reply.
    ///
    /// **注意**: 同步 API 会阻塞调用线程; 异步 caller 请用 `put_async`.
    ///
    /// `req_id` 默认 0, 表示旧行为 (仅 channel reply). 网络层传 `> 0` 让 shard
    /// 同时 push 一份到 reply_bus.
    pub fn put(
        &self,
        db: &str,
        table: &str,
        key: &[u8],
        val: &[u8],
        req_id: u64,
    ) -> ShardResult<()> {
        let shard_id = self.route_db_table_key(db, table, key);
        let (tx, fut) = PendingReply::new();
        self.shards[shard_id]
            .inbox
            .push_spin(ShardRequest::Put {
                db: db.to_string(),
                table: table.to_string(),
                key: key.to_vec(),
                val: val.to_vec(),
                req_id,
                reply: tx,
            });
        let response = block_on_v2(fut);
        match response {
            Ok(ShardReply::PutOk) => Ok(()),
            Ok(other) => Err(ShardError::StorageError(format!(
                "unexpected reply: {other:?}"
            ))),
            Err(kind) => Err(ShardError::from_kind(kind)),
        }
    }

    /// Get.
    pub fn get(
        &self,
        db: &str,
        table: &str,
        key: &[u8],
        req_id: u64,
    ) -> ShardResult<Option<Vec<u8>>> {
        let shard_id = self.route_db_table_key(db, table, key);
        let (tx, fut) = PendingReply::new();
        self.shards[shard_id]
            .inbox
            .push_spin(ShardRequest::Get {
                db: db.to_string(),
                table: table.to_string(),
                key: key.to_vec(),
                req_id,
                reply: tx,
            });
        let response = block_on_v2(fut);
        match response {
            Ok(ShardReply::GetValue(v)) => Ok(v),
            Ok(other) => Err(ShardError::StorageError(format!(
                "unexpected reply: {other:?}"
            ))),
            Err(kind) => Err(ShardError::from_kind(kind)),
        }
    }

    /// Delete.
    pub fn delete(
        &self,
        db: &str,
        table: &str,
        key: &[u8],
        req_id: u64,
    ) -> ShardResult<bool> {
        let shard_id = self.route_db_table_key(db, table, key);
        let (tx, fut) = PendingReply::new();
        self.shards[shard_id]
            .inbox
            .push_spin(ShardRequest::Delete {
                db: db.to_string(),
                table: table.to_string(),
                key: key.to_vec(),
                req_id,
                reply: tx,
            });
        let response = block_on_v2(fut);
        match response {
            Ok(ShardReply::DeleteExisted(b)) => Ok(b),
            Ok(other) => Err(ShardError::StorageError(format!(
                "unexpected reply: {other:?}"
            ))),
            Err(kind) => Err(ShardError::from_kind(kind)),
        }
    }

    // =================================================================
    // ⭐ Batch API: 批量操作, 按 shard 分组后每 shard 一次往返
    // =================================================================

    /// 批量 Put: 按 shard 分组, 每 shard 一次往返.
    pub fn batch_put(
        &self,
        db: &str,
        table: &str,
        entries: &[(&[u8], &[u8])],
    ) -> Vec<ShardResult<()>> {
        let ops: Vec<BatchOp> = entries
            .iter()
            .map(|(k, v)| BatchOp::Put {
                db: db.to_string(),
                table: table.to_string(),
                key: k.to_vec(),
                val: v.to_vec(),
            })
            .collect();
        let results = self.batch_ops_inner(&ops);
        results
            .into_iter()
            .map(|r| match r {
                BatchResult::PutOk => Ok(()),
                BatchResult::Error(e) => Err(ShardError::StorageError(e)),
                _ => Err(ShardError::StorageError("unexpected batch result".into())),
            })
            .collect()
    }

    /// 批量 Get: 按 shard 分组, 每 shard 一次往返.
    pub fn batch_get(
        &self,
        db: &str,
        table: &str,
        keys: &[&[u8]],
    ) -> Vec<ShardResult<Option<Vec<u8>>>> {
        let ops: Vec<BatchOp> = keys
            .iter()
            .map(|k| BatchOp::Get {
                db: db.to_string(),
                table: table.to_string(),
                key: k.to_vec(),
            })
            .collect();
        let results = self.batch_ops_inner(&ops);
        results
            .into_iter()
            .map(|r| match r {
                BatchResult::GetValue(v) => Ok(v),
                BatchResult::Error(e) => Err(ShardError::StorageError(e)),
                _ => Err(ShardError::StorageError("unexpected batch result".into())),
            })
            .collect()
    }

    /// 通用批量操作: 混合 put/get/delete 按 shard 分组.
    pub fn batch_ops(&self, ops: &[BatchOp]) -> Vec<BatchResult> {
        self.batch_ops_inner(ops)
    }

    /// 内部实现: 按 shard 分组 → 每 shard 一次 push + block_on → 重组结果.
    fn batch_ops_inner(&self, ops: &[BatchOp]) -> Vec<BatchResult> {
        if ops.is_empty() {
            return Vec::new();
        }

        // 1. 按 shard 分组, 记录原始索引
        let mut shard_groups: Vec<Vec<(usize, BatchOp)>> = vec![Vec::new(); self.num_shards];
        for (i, op) in ops.iter().enumerate() {
            let (db, table, key) = match op {
                BatchOp::Put { db, table, key, .. } => (db.as_str(), table.as_str(), key.as_slice()),
                BatchOp::Get { db, table, key } => (db.as_str(), table.as_str(), key.as_slice()),
                BatchOp::Delete { db, table, key } => (db.as_str(), table.as_str(), key.as_slice()),
            };
            let shard_id = self.route_db_table_key(db, table, key);
            shard_groups[shard_id].push((i, op.clone()));
        }

        // 2. 每个非空 shard 组: 发送 Batch 请求, 收集 future
        let mut futures: Vec<(usize, Vec<usize>, _)> = Vec::new(); // (shard_id, orig_indices, fut)
        for (shard_id, group) in shard_groups.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let orig_indices: Vec<usize> = group.iter().map(|(i, _)| *i).collect();
            let batch_ops: Vec<BatchOp> = group.into_iter().map(|(_, op)| op).collect();
            let (tx, fut) = PendingReply::new();
            self.shards[shard_id].inbox.push_spin(ShardRequest::Batch {
                ops: batch_ops,
                req_id: 0,
                reply: tx,
            });
            futures.push((shard_id, orig_indices, fut));
        }

        // 3. 等待所有 shard 回复, 按原始索引重组结果
        let mut results: Vec<BatchResult> = vec![BatchResult::Error("pending".into()); ops.len()];
        for (_shard_id, orig_indices, fut) in futures {
            let response = block_on_v2(fut);
            match response {
                Ok(ShardReply::BatchResults(batch_results)) => {
                    for (idx, result) in orig_indices.into_iter().zip(batch_results) {
                        results[idx] = result;
                    }
                }
                Ok(_) => {
                    for idx in orig_indices {
                        results[idx] = BatchResult::Error("unexpected reply".into());
                    }
                }
                Err(kind) => {
                    let err_msg = format!("{kind:?}");
                    for idx in orig_indices {
                        results[idx] = BatchResult::Error(err_msg.clone());
                    }
                }
            }
        }
        results
    }

    // =================================================================
    // ⭐ 独立服务架构: Task 直接提交 API
    // =================================================================

    /// 直接提交 tasks 到 shard (模拟 network worker 的行为).
    /// `caller_id` 用于选择 reply bus (不同 caller 必须用不同 id, 否则结果会混).
    pub fn submit_tasks(&self, ops: &[BatchOp], caller_id: u32) -> Vec<BatchResult> {
        use crate::request::ShardTask;

        if ops.is_empty() {
            return Vec::new();
        }

        let worker_id = caller_id;
        let conn_id = caller_id as u64;

        // 1. 按 shard 分组 push
        let mut expected_count = 0usize;
        for (i, op) in ops.iter().enumerate() {
            let (db, table, key) = match op {
                BatchOp::Put { db, table, key, .. } => (db.as_str(), table.as_str(), key.as_slice()),
                BatchOp::Get { db, table, key } => (db.as_str(), table.as_str(), key.as_slice()),
                BatchOp::Delete { db, table, key } => (db.as_str(), table.as_str(), key.as_slice()),
            };
            let shard_id = self.route_db_table_key(db, table, key);
            self.shards[shard_id].task_inbox.push_spin(ShardTask {
                conn_id,
                req_id: i as u64,
                worker_id,
                op: op.clone(),
            });
            expected_count += 1;
        }

        // 2. spin-poll reply bus 直到收齐所有结果
        let bus = self.reply_bus_set.get(worker_id);
        let mut results: Vec<BatchResult> = vec![BatchResult::Error("pending".into()); ops.len()];
        let mut received = 0usize;
        while received < expected_count {
            let batch = bus.try_drain();
            for r in batch {
                if r.req_id < ops.len() as u64 {
                    results[r.req_id as usize] = r.result;
                    received += 1;
                }
            }
            if received < expected_count {
                std::hint::spin_loop();
            }
        }
        results
    }

    /// 获取指定 shard 的 task inbox (供网络层直接 push).
    pub fn task_inbox(&self, shard_id: usize) -> &crate::task_inbox::SharedTaskInbox {
        &self.shards[shard_id].task_inbox
    }

    // =================================================================
    // ⭐ T15: 异步 API (不阻塞调用线程)
    // =================================================================

    /// 异步 Put: 返回 ReplyFuture, caller await 它 (Tokio/Axum 友好).
    pub fn put_async(
        &self,
        db: &str,
        table: &str,
        key: &[u8],
        val: &[u8],
        req_id: u64,
    ) -> ShardResult<impl Future<Output = ShardResult<()>>> {
        let shard_id = self.route_db_table_key(db, table, key);
        let (tx, fut) = PendingReply::new();
        self.shards[shard_id]
            .inbox
            .push_spin(ShardRequest::Put {
                db: db.to_string(),
                table: table.to_string(),
                key: key.to_vec(),
                val: val.to_vec(),
                req_id,
                reply: tx,
            });
        Ok(async move {
            match fut.await {
                Ok(ShardReply::PutOk) => Ok(()),
                Ok(other) => Err(ShardError::StorageError(format!(
                    "unexpected reply: {other:?}"
                ))),
                Err(kind) => Err(ShardError::from_kind(kind)),
            }
        })
    }

    /// 异步 Get: 返回 ReplyFuture.
    pub fn get_async(
        &self,
        db: &str,
        table: &str,
        key: &[u8],
        req_id: u64,
    ) -> ShardResult<impl Future<Output = ShardResult<Option<Vec<u8>>>>> {
        let shard_id = self.route_db_table_key(db, table, key);
        let (tx, fut) = PendingReply::new();
        self.shards[shard_id]
            .inbox
            .push_spin(ShardRequest::Get {
                db: db.to_string(),
                table: table.to_string(),
                key: key.to_vec(),
                req_id,
                reply: tx,
            });
        Ok(async move {
            match fut.await {
                Ok(ShardReply::GetValue(v)) => Ok(v),
                Ok(other) => Err(ShardError::StorageError(format!(
                    "unexpected reply: {other:?}"
                ))),
                Err(kind) => Err(ShardError::from_kind(kind)),
            }
        })
    }

    /// 异步 Delete: 返回 ReplyFuture.
    pub fn delete_async(
        &self,
        db: &str,
        table: &str,
        key: &[u8],
        req_id: u64,
    ) -> ShardResult<impl Future<Output = ShardResult<bool>>> {
        let shard_id = self.route_db_table_key(db, table, key);
        let (tx, fut) = PendingReply::new();
        self.shards[shard_id]
            .inbox
            .push_spin(ShardRequest::Delete {
                db: db.to_string(),
                table: table.to_string(),
                key: key.to_vec(),
                req_id,
                reply: tx,
            });
        Ok(async move {
            match fut.await {
                Ok(ShardReply::DeleteExisted(b)) => Ok(b),
                Ok(other) => Err(ShardError::StorageError(format!(
                    "unexpected reply: {other:?}"
                ))),
                Err(kind) => Err(ShardError::from_kind(kind)),
            }
        })
    }

    // =================================================================
    // ⭐ T14: 2PC 跨 shard 协调
    // =================================================================

    /// **2PC**: 创建 db (跨所有 shard).
    pub fn create_db(&self, db: &str) -> ShardResult<()> {
        let num_shards = self.num_shards;
        if num_shards == 0 {
            return Ok(());
        }

        // 1. 开始 2PC 事务
        let txn_id = self
            .coordinator
            .lock()
            .unwrap()
            .begin_txn(TxnOp::CreateDb(db.to_string()), num_shards);

        // 2. Prepare 阶段: 向所有 shard 发送 PrepareCreateDb
        let mut results: Vec<ShardResponse> = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            let (tx, fut) = PendingReply::new();
            let req = ShardRequest::PrepareCreateDb {
                db: db.to_string(),
                txn_id,
                reply: tx,
            };
            self.shards[i].inbox.push_spin(req);
            results.push(block_on_v2(fut));
        }

        // 3. 处理所有 shard 的 Prepare 回复
        let mut all_ok = true;
        let mut first_error: Option<(usize, String)> = None;
        for (i, result) in results.iter().enumerate() {
            match result {
                Ok(ShardReply::PrepareOk) => {
                    self.coordinator.lock().unwrap().on_prepare_ack(txn_id, i);
                }
                Ok(other) => {
                    self.coordinator.lock().unwrap().on_prepare_fail(txn_id, i);
                    all_ok = false;
                    if first_error.is_none() {
                        first_error = Some((i, format!("unexpected reply: {other:?}")));
                    }
                }
                Err(kind) => {
                    self.coordinator.lock().unwrap().on_prepare_fail(txn_id, i);
                    all_ok = false;
                    if first_error.is_none() {
                        first_error = Some((i, format!("{kind:?}")));
                    }
                }
            }
        }

        // 4. 决定 Commit 或 Abort
        if all_ok {
            for i in 0..num_shards {
                let (tx, fut) = PendingReply::new();
                let req = ShardRequest::CommitCreateDb {
                    db: db.to_string(),
                    txn_id,
                    reply: tx,
                };
                self.shards[i].inbox.push_spin(req);
                if block_on_v2(fut).is_ok() {
                    self.coordinator.lock().unwrap().on_commit_ack(txn_id, i);
                }
            }
            Ok(())
        } else {
            let (err_shard, err_reason) = first_error.unwrap_or((0, "unknown".into()));
            for (i, result) in results.iter().enumerate() {
                let is_prepare_ok = matches!(result, Ok(ShardReply::PrepareOk));
                if is_prepare_ok {
                    let (tx, fut) = PendingReply::new();
                    let req = ShardRequest::AbortCreateDb {
                        db: db.to_string(),
                        txn_id,
                        reply: tx,
                    };
                    self.shards[i].inbox.push_spin(req);
                    if block_on_v2(fut).is_ok() {
                        self.coordinator.lock().unwrap().on_abort_ack(txn_id, i);
                    }
                }
            }
            Err(ShardError::PrepareFailed {
                op: format!("create_db({db})"),
                shard_id: err_shard,
                reason: err_reason,
            })
        }
    }

    /// **2PC**: 创建表 (跨所有 shard).
    pub fn create_table(&self, db: &str, table: &str) -> ShardResult<u64> {
        let num_shards = self.num_shards;
        if num_shards == 0 {
            return Ok(0);
        }

        let txn_id = self.coordinator.lock().unwrap().begin_txn(
            TxnOp::CreateTable(db.to_string(), table.to_string()),
            num_shards,
        );

        let mut results: Vec<ShardResponse> = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            let (tx, fut) = PendingReply::new();
            let req = ShardRequest::PrepareCreateTable {
                db: db.to_string(),
                table: table.to_string(),
                txn_id,
                reply: tx,
            };
            self.shards[i].inbox.push_spin(req);
            results.push(block_on_v2(fut));
        }

        let mut all_ok = true;
        let mut first_error: Option<(usize, String)> = None;
        for (i, result) in results.iter().enumerate() {
            match result {
                Ok(ShardReply::PrepareOk) => {
                    self.coordinator.lock().unwrap().on_prepare_ack(txn_id, i);
                }
                Ok(other) => {
                    self.coordinator.lock().unwrap().on_prepare_fail(txn_id, i);
                    all_ok = false;
                    if first_error.is_none() {
                        first_error = Some((i, format!("unexpected reply: {other:?}")));
                    }
                }
                Err(kind) => {
                    self.coordinator.lock().unwrap().on_prepare_fail(txn_id, i);
                    all_ok = false;
                    if first_error.is_none() {
                        first_error = Some((i, format!("{kind:?}")));
                    }
                }
            }
        }

        if all_ok {
            for i in 0..num_shards {
                let (tx, fut) = PendingReply::new();
                let req = ShardRequest::CommitCreateTable {
                    db: db.to_string(),
                    table: table.to_string(),
                    txn_id,
                    reply: tx,
                };
                self.shards[i].inbox.push_spin(req);
                if block_on_v2(fut).is_ok() {
                    self.coordinator.lock().unwrap().on_commit_ack(txn_id, i);
                }
            }
            Ok(0)
        } else {
            let (err_shard, err_reason) = first_error.unwrap_or((0, "unknown".into()));
            for (i, result) in results.iter().enumerate() {
                let is_prepare_ok = matches!(result, Ok(ShardReply::PrepareOk));
                if is_prepare_ok {
                    let (tx, fut) = PendingReply::new();
                    let req = ShardRequest::AbortCreateTable {
                        db: db.to_string(),
                        table: table.to_string(),
                        txn_id,
                        reply: tx,
                    };
                    self.shards[i].inbox.push_spin(req);
                    if block_on_v2(fut).is_ok() {
                        self.coordinator.lock().unwrap().on_abort_ack(txn_id, i);
                    }
                }
            }
            Err(ShardError::PrepareFailed {
                op: format!("create_table({db}.{table})"),
                shard_id: err_shard,
                reason: err_reason,
            })
        }
    }

    // =================================================================
    // ⭐ T15: 异步 2PC API
    // =================================================================

    /// 异步 2PC create_db.
    pub async fn create_db_async(&self, db: &str) -> ShardResult<()> {
        let num_shards = self.num_shards;
        if num_shards == 0 {
            return Ok(());
        }
        let txn_id = self
            .coordinator
            .lock()
            .unwrap()
            .begin_txn(TxnOp::CreateDb(db.to_string()), num_shards);

        // Prepare 阶段: 并发发给所有 shard
        let mut prepare_futs: Vec<BoxedShardFuture> = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            let (tx, fut) = PendingReply::new();
            let req = ShardRequest::PrepareCreateDb {
                db: db.to_string(),
                txn_id,
                reply: tx,
            };
            self.shards[i].inbox.push_spin(req);
            let f: BoxedShardFuture = Box::pin(fut);
            prepare_futs.push(f);
        }
        // ⭐ 并发 await 所有 prepare future
        let results: Vec<ShardResponse> = {
            // 用 futures join_all 模式: 逐个 await
            let mut out = Vec::with_capacity(num_shards);
            for f in prepare_futs {
                out.push(f.await);
            }
            out
        };

        // 决定 Commit/Abort (同步逻辑, 因为涉及 coordinator 状态机)
        let mut all_ok = true;
        let mut first_error: Option<(usize, String)> = None;
        for (i, result) in results.iter().enumerate() {
            match result {
                Ok(ShardReply::PrepareOk) => {
                    self.coordinator.lock().unwrap().on_prepare_ack(txn_id, i);
                }
                Ok(other) => {
                    self.coordinator.lock().unwrap().on_prepare_fail(txn_id, i);
                    all_ok = false;
                    if first_error.is_none() {
                        first_error = Some((i, format!("unexpected reply: {other:?}")));
                    }
                }
                Err(kind) => {
                    self.coordinator.lock().unwrap().on_prepare_fail(txn_id, i);
                    all_ok = false;
                    if first_error.is_none() {
                        first_error = Some((i, format!("{kind:?}")));
                    }
                }
            }
        }

        if all_ok {
            // Commit 阶段: 同样并发
            let mut commit_futs: Vec<BoxedShardFuture> = Vec::with_capacity(num_shards);
            for i in 0..num_shards {
                let (tx, fut) = PendingReply::new();
                let req = ShardRequest::CommitCreateDb {
                    db: db.to_string(),
                    txn_id,
                    reply: tx,
                };
                self.shards[i].inbox.push_spin(req);
                let f: BoxedShardFuture = Box::pin(fut);
                commit_futs.push(f);
            }
            for (i, f) in commit_futs.into_iter().enumerate() {
                if f.await.is_ok() {
                    self.coordinator.lock().unwrap().on_commit_ack(txn_id, i);
                }
            }
            Ok(())
        } else {
            let (err_shard, err_reason) = first_error.unwrap_or((0, "unknown".into()));
            let mut abort_futs: Vec<(usize, BoxedShardFuture)> = Vec::new();
            for (i, result) in results.iter().enumerate() {
                let is_prepare_ok = matches!(result, Ok(ShardReply::PrepareOk));
                if is_prepare_ok {
                    let (tx, fut) = PendingReply::new();
                    let req = ShardRequest::AbortCreateDb {
                        db: db.to_string(),
                        txn_id,
                        reply: tx,
                    };
                    self.shards[i].inbox.push_spin(req);
                    let f: BoxedShardFuture = Box::pin(fut);
                    abort_futs.push((i, f));
                }
            }
            for (i, f) in abort_futs {
                if f.await.is_ok() {
                    self.coordinator.lock().unwrap().on_abort_ack(txn_id, i);
                }
            }
            Err(ShardError::PrepareFailed {
                op: format!("create_db({db})"),
                shard_id: err_shard,
                reason: err_reason,
            })
        }
    }

    /// 异步 2PC create_table.
    pub async fn create_table_async(&self, db: &str, table: &str) -> ShardResult<u64> {
        let num_shards = self.num_shards;
        if num_shards == 0 {
            return Ok(0);
        }
        let txn_id = self.coordinator.lock().unwrap().begin_txn(
            TxnOp::CreateTable(db.to_string(), table.to_string()),
            num_shards,
        );

        let mut prepare_futs: Vec<BoxedShardFuture> = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            let (tx, fut) = PendingReply::new();
            let req = ShardRequest::PrepareCreateTable {
                db: db.to_string(),
                table: table.to_string(),
                txn_id,
                reply: tx,
            };
            self.shards[i].inbox.push_spin(req);
            let f: BoxedShardFuture = Box::pin(fut);
            prepare_futs.push(f);
        }
        let results: Vec<ShardResponse> = {
            let mut out = Vec::with_capacity(num_shards);
            for f in prepare_futs {
                out.push(f.await);
            }
            out
        };

        let mut all_ok = true;
        let mut first_error: Option<(usize, String)> = None;
        for (i, result) in results.iter().enumerate() {
            match result {
                Ok(ShardReply::PrepareOk) => {
                    self.coordinator.lock().unwrap().on_prepare_ack(txn_id, i);
                }
                Ok(other) => {
                    self.coordinator.lock().unwrap().on_prepare_fail(txn_id, i);
                    all_ok = false;
                    if first_error.is_none() {
                        first_error = Some((i, format!("unexpected reply: {other:?}")));
                    }
                }
                Err(kind) => {
                    self.coordinator.lock().unwrap().on_prepare_fail(txn_id, i);
                    all_ok = false;
                    if first_error.is_none() {
                        first_error = Some((i, format!("{kind:?}")));
                    }
                }
            }
        }

        if all_ok {
            let mut commit_futs: Vec<BoxedShardFuture> = Vec::with_capacity(num_shards);
            for i in 0..num_shards {
                let (tx, fut) = PendingReply::new();
                let req = ShardRequest::CommitCreateTable {
                    db: db.to_string(),
                    table: table.to_string(),
                    txn_id,
                    reply: tx,
                };
                self.shards[i].inbox.push_spin(req);
                commit_futs.push(Box::pin(fut));
            }
            for (i, f) in commit_futs.into_iter().enumerate() {
                if f.await.is_ok() {
                    self.coordinator.lock().unwrap().on_commit_ack(txn_id, i);
                }
            }
            Ok(0)
        } else {
            let (err_shard, err_reason) = first_error.unwrap_or((0, "unknown".into()));
            let mut abort_futs: Vec<(usize, BoxedShardFuture)> = Vec::new();
            for (i, result) in results.iter().enumerate() {
                let is_prepare_ok = matches!(result, Ok(ShardReply::PrepareOk));
                if is_prepare_ok {
                    let (tx, fut) = PendingReply::new();
                    let req = ShardRequest::AbortCreateTable {
                        db: db.to_string(),
                        table: table.to_string(),
                        txn_id,
                        reply: tx,
                    };
                    self.shards[i].inbox.push_spin(req);
                    abort_futs.push((i, Box::pin(fut)));
                }
            }
            for (i, f) in abort_futs {
                if f.await.is_ok() {
                    self.coordinator.lock().unwrap().on_abort_ack(txn_id, i);
                }
            }
            Err(ShardError::PrepareFailed {
                op: format!("create_table({db}.{table})"),
                shard_id: err_shard,
                reason: err_reason,
            })
        }
    }

    /// 路由: 用 db name + table + key hash.
    fn route_db_table_key(&self, db: &str, table: &str, key: &[u8]) -> ShardId {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        db.hash(&mut hasher);
        table.hash(&mut hasher);
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_shards
    }

    /// 关闭所有 shard 线程, 干净退出.
    pub fn close(self) -> ShardResult<()> {
        // 1. 发送 Shutdown 给所有 shard
        for shard in &self.shards {
            let (tx, fut) = PendingReply::new();
            // 忽略 send 错误 (shard 可能已死)
            shard.inbox.push_spin(ShardRequest::Shutdown { reply: tx });
            // 等 reply (设 timeout, 避免死锁)
            let _ = block_on_v2(fut);
        }

        // 2. drop senders (让 mpsc 关闭, shard loop 也会 break)
        drop(self.shards);

        // 3. join 所有线程
        for (i, join) in self.threads.into_iter().enumerate() {
            join.join().map_err(|_| ShardError::JoinPanic(i))?;
        }
        Ok(())
    }

    /// ⭐ Flush all shards: 把所有 shard 的 nowchunks dirty data 落盘到磁盘并插入 chunk_list.
    ///
    /// 调用后所有数据 durability = disk + chunk_list 命中, 不依赖 nowchunks.
    ///
    /// **用法**: 长时间 batch 写后, 想 verify 数据一致性时调用一次.
    pub fn flush_all(&self) -> ShardResult<()> {
        for shard in &self.shards {
            let (tx, fut) = PendingReply::new();
            shard.inbox.push_spin(ShardRequest::Flush {
                reply: tx,
            });
            if block_on_v2(fut).is_err() {
                return Err(ShardError::ChannelClosed);
            }
        }
        Ok(())
    }
}

// =====================================================================
// shard 线程主函数
// =====================================================================

/// shard 线程专用 block_on: 重复 poll 直到就绪, **不依赖 waker 唤醒**.
///
/// ⚠️ 不能用 pollster: IoUring 后端下 io_ops future 首次 poll 提交 SQE 后返回
/// Pending, pollster 随即 park 线程; 而 CQE 的收割在下次 poll 的 CQ 扫描里
/// (io_ops::poll_cqe) —— 线程睡死后无人再 poll, CQE 永远无人收割 → 死锁.
/// (现象: 写入后 10s 周期刷盘的 fsync 被 punt 到 io-wq, shard 永久 futex 睡死.)
///
/// 这里每次 Pending 后短 spin 再重 poll (poll 内部会 sync CQ 并收割),
/// 长 IO (fsync 毫秒级) 时退化为 yield, 不烧满 CPU.
fn block_on_io<F: std::future::Future>(fut: F) -> F::Output {
    use std::task::{Context, Poll, Waker};
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut fut = std::pin::pin!(fut);
    let mut spins = 0u32;
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        spins += 1;
        if spins < 64 {
            std::hint::spin_loop();
        } else {
            std::thread::yield_now();
        }
    }
}

/// ⭐ 异步落盘完成事件: data chunk / meta window 两类 (Phase M3).
enum FlushDone {
    Data(storage::PageKey, std::io::Result<()>),
    Meta(u32, std::io::Result<()>),
}

/// ⭐ 异步落盘完成槽: 落盘协程 push 完成事件, 主循环收割 (单线程 Rc, 无锁).
type FlushDoneSlot = std::rc::Rc<std::cell::RefCell<Vec<FlushDone>>>;

/// ⭐ 异步落盘驱动 (每轮调用): 收割完成 → spawn 新作业 → 周期检查 → drive 协程.
///
/// 落盘协程零 Pager 借用 (FlushBatch 自带 io/dir/items), 与后续内存写入完全并发;
/// 磁盘 IO 期间 shard 继续处理请求, 仅靠每轮 drive_until_idle 推进写盘.
fn drive_async_flush(
    engine: &std::rc::Rc<std::cell::RefCell<Option<StorageEngine>>>,
    rt: &scheduler::SchedHandle,
    flush_done: &FlushDoneSlot,
) {
    let round_start = std::time::Instant::now();
    {
        let mut e_borrow = engine.borrow_mut();
        if let Some(e) = e_borrow.as_mut() {
            // a. 收割上轮完成的落盘 (data: 成功入 chunk_list, 失败回 pending;
            //    meta: 清 in-flight, 全部确认后 persist pid.state)
            for done in flush_done.borrow_mut().drain(..) {
                let cor_start = std::time::Instant::now();
                match done {
                    FlushDone::Data(key, result) => {
                        if let Err(err) = e.pager_mut().complete_flush(key, result) {
                            nlog::error!("shard", "chunk flush failed (requeued): {err}");
                        }
                    }
                    FlushDone::Meta(window_idx, result) => {
                        if let Err(ref err) = result {
                            nlog::error!("shard", "meta window {window_idx} flush failed (will retry): {err}");
                        }
                        e.pager_mut().complete_meta_flush(window_idx, result);
                    }
                }
                if crate::PROBE.is_enabled() {
                    crate::PROBE
                        .sync_write_coroutine_ns
                        .record(cor_start.elapsed().as_nanos() as u64);
                }
            }
            // b. 提交新作业: 同步入队 + spawn 协程 (SQE 在首次 poll 时提交)
            // Phase C: 按 file 成批, 每批 write ×N + fsync ×1 (长尾对症)
            let batches = e.pager_mut().take_flush_batches();
            if crate::PROBE.is_enabled() && !batches.is_empty() {
                let inflight = e.pager_mut().flush_backlog();
                crate::PROBE
                    .in_flight_peak
                    .fetch_max(inflight as u64, std::sync::atomic::Ordering::Relaxed);
            }
            for batch in batches {
                let done = flush_done.clone();
                scheduler::spawn_on(
                    rt,
                    Box::pin(async move {
                        let items: Vec<(storage::PageKey, &[u8])> = batch
                            .items
                            .iter()
                            .map(|(k, b)| (*k, b.as_slice()))
                            .collect();
                        let r = batch.io.write_chunks_batch(&batch.dir, &items).await;
                        drop(items);
                        // 逐 key push 完成槽 (io::Error 不可 Clone, 用 msg 重建)
                        let mut slot = done.borrow_mut();
                        match r {
                            Ok(()) => {
                                for (key, _) in &batch.items {
                                    slot.push(FlushDone::Data(*key, Ok(())));
                                }
                            }
                            Err(err) => {
                                let msg = err.to_string();
                                for (key, _) in &batch.items {
                                    slot.push(FlushDone::Data(
                                        *key,
                                        Err(std::io::Error::other(msg.clone())),
                                    ));
                                }
                            }
                        }
                    }),
                );
            }
            // b2. ⭐ Phase M3: meta window 异步刷盘 (data backlog 排空后才取得到批,
            // data→meta 顺序不变; fsync 在协程里, 主循环零阻塞)
            if let Some(mb) = e.pager_mut().take_meta_flush_batch() {
                let done = flush_done.clone();
                scheduler::spawn_on(
                    rt,
                    Box::pin(async move {
                        let items: Vec<(u32, &[u8])> = mb
                            .windows
                            .iter()
                            .map(|(w, b)| (*w, b.as_slice()))
                            .collect();
                        let r = mb.io.write_mate_windows(&mb.mate_path, &items).await;
                        drop(items);
                        let mut slot = done.borrow_mut();
                        match r {
                            Ok(()) => {
                                for (w, _) in &mb.windows {
                                    slot.push(FlushDone::Meta(*w, Ok(())));
                                }
                            }
                            Err(err) => {
                                let msg = err.to_string();
                                for (w, _) in &mb.windows {
                                    slot.push(FlushDone::Meta(
                                        *w,
                                        Err(std::io::Error::other(msg.clone())),
                                    ));
                                }
                            }
                        }
                    }),
                );
            }
            // c. 周期/计数刷盘 (内部守卫: 有 in-flight/pending 时自动推迟)
            let pf_start = std::time::Instant::now();
            let _ = block_on_io(e.pager_mut().maybe_periodic_flush());
            if crate::PROBE.is_enabled() {
                crate::PROBE
                    .block_on_io_ns
                    .record(pf_start.elapsed().as_nanos() as u64);
            }
        }
    }
    // d. 推进落盘协程 (提交 SQE / 收割 CQE / 完成时 push 完成槽)
    let di_start = std::time::Instant::now();
    rt.clone().drive_until_idle(256);
    if crate::PROBE.is_enabled() {
        crate::PROBE
            .drive_until_idle_ns
            .record(di_start.elapsed().as_nanos() as u64);
        crate::PROBE
            .drive_round_ns
            .record(round_start.elapsed().as_nanos() as u64);
    }
}

/// ⭐ 排空异步落盘 backlog (flush 请求/shutdown 前调用, 保证 flush() 契约).
fn drain_async_flush(
    engine: &std::rc::Rc<std::cell::RefCell<Option<StorageEngine>>>,
    rt: &scheduler::SchedHandle,
    flush_done: &FlushDoneSlot,
) {
    loop {
        drive_async_flush(engine, rt, flush_done);
        let drained = {
            let mut e_borrow = engine.borrow_mut();
            match e_borrow.as_mut() {
                // ⭐ Phase M3: 含 meta backlog (due/dirty/in-flight) 才算排空
                Some(e) => e.pager_mut().total_async_backlog() == 0,
                None => true,
            }
        };
        if drained && flush_done.borrow().is_empty() {
            break;
        }
        rt.clone().drive_until_idle(1000);
    }
}

/// shard 线程主函数. ⭐ 同时处理 ShardRequest (admin) 和 ShardTask (KV ops).
fn shard_thread_main(
    shard_id: usize,
    storage_opts: OpenOptions,
    _router: Arc<dyn Router>,
    inbox: crate::inbox::SharedInbox,
    task_inbox: crate::task_inbox::SharedTaskInbox,
    reply_sink: Arc<StdMutex<Option<Arc<dyn ReplySink>>>>,
    reply_bus_set: Arc<crate::task_reply_bus::ReplyBusSet>,
) {
    use std::cell::RefCell;
    use std::rc::Rc;

    // T18c: 支持 SQPOLL (sqpoll_ms > 0 时启用内核线程轮询)
    let sqpoll_ms = storage_opts.io_config.sqpoll_ms;
    let scheduler = if sqpoll_ms > 0 {
        scheduler::Scheduler::new_with_sqpoll(sqpoll_ms)
    } else {
        scheduler::Scheduler::new()
    };
    let rt = scheduler::SchedHandle::new(scheduler);
    rt.set_current();

    let engine: Rc<RefCell<Option<StorageEngine>>> = Rc::new(RefCell::new(None));

    let engine_init = engine.clone();
    let init_result: Rc<RefCell<Option<Result<(), storage::StorageError>>>> =
        Rc::new(RefCell::new(None));
    let init_result_clone = init_result.clone();

    let init_fut = Box::pin(async move {
        let result = StorageEngine::open(storage_opts).await;
        match result {
            Ok(e) => {
                *engine_init.borrow_mut() = Some(e);
                *init_result_clone.borrow_mut() = Some(Ok(()));
            }
            Err(e) => {
                *init_result_clone.borrow_mut() = Some(Err(e));
            }
        }
    });
    scheduler::spawn_on(&rt, init_fut);

    while init_result.borrow().is_none() {
        rt.clone().drive_until_idle(1000);
    }
    if init_result.borrow().as_ref().unwrap().is_err() {
        nlog::error!("shard", "shard-{shard_id} engine init failed, exiting");
        return;
    }
    drop(init_result);
    nlog::info!("shard", "shard-{shard_id} engine ready");

    // ⭐ 探针启用: NLOG_PROBE=1 时 dump_all() 可输出各阶段 histogram.
    if std::env::var("NLOG_PROBE").ok().as_deref() == Some("1") {
        crate::PROBE.enable();
        nlog::info!("shard", "probes enabled (NLOG_PROBE=1)");
    }

    // ⭐ 异步落盘完成槽
    let flush_done: FlushDoneSlot = Rc::new(RefCell::new(Vec::new()));

    // ⭐ 主循环: 同时 poll 两个 inbox (ShardRequest + ShardTask)
    //
    // ⭐ 方向 1 优化 (2026-07-24): 慢路径从 yield 自旋改为 poll() 真阻塞双 eventfd.
    // 前提是 drain() 已修复丢唤醒竞态 (先重置 pending 再 pop),
    // 否则睡眠后可能永久错过通知. 10ms timeout 兑底驱动周期刷盘.
    const SPIN_ROUNDS_BEFORE_PARK: u32 = 1024;

    loop {
        // spin poll 两个 inbox, 任一有数据就退出 spin
        let mut spins = 0u32;
        let (batch, tasks) = loop {
            let b = inbox.drain();
            let t = task_inbox.drain();
            if !b.is_empty() || !t.is_empty() {
                break (b, t);
            }
            spins += 1;
            if spins >= SPIN_ROUNDS_BEFORE_PARK {
                // 慢速路径: poll() 阻塞等两个 eventfd (零 CPU, 精确唤醒).
                // timeout 10ms: 周期性醒来驱动自动持久化检查.
                let mut fds = [
                    libc::pollfd {
                        fd: inbox.eventfd(),
                        events: libc::POLLIN,
                        revents: 0,
                    },
                    libc::pollfd {
                        fd: task_inbox.eventfd(),
                        events: libc::POLLIN,
                        revents: 0,
                    },
                ];
                unsafe {
                    libc::poll(fds.as_mut_ptr(), 2, 10);
                }
                // 消耗 eventfd 计数 (仅在 POLLIN 时读; eventfd 是 blocking 模式,
                // 计数为 0 时读会阻塞)
                if fds[0].revents & libc::POLLIN != 0 {
                    let mut v: u64 = 0;
                    unsafe {
                        libc::read(inbox.eventfd(), &mut v as *mut u64 as *mut libc::c_void, 8);
                    }
                }
                if fds[1].revents & libc::POLLIN != 0 {
                    let mut v: u64 = 0;
                    unsafe {
                        libc::read(
                            task_inbox.eventfd(),
                            &mut v as *mut u64 as *mut libc::c_void,
                            8,
                        );
                    }
                }
                let b = inbox.drain();
                let t = task_inbox.drain();
                if !b.is_empty() || !t.is_empty() {
                    break (b, t);
                }
                // timeout 醒来无数据: 驱动异步落盘 + 周期刷盘后继续睡
                drive_async_flush(&engine, &rt, &flush_done);
                spins = 0;
                continue;
            }
            for _ in 0..4 {
                std::hint::spin_loop();
            }
        };

        rt.clone().drive_until_idle(0);

        let mut should_shutdown = false;
        for req in batch {
            match req {
                ShardRequest::Shutdown { reply } => {
                    let _ = reply.send(Ok(ShardReply::ShutdownOk));
                    should_shutdown = true;
                }
                ShardRequest::Flush { reply } => {
                    // ⭐ flush 契约: 先排空异步落盘 backlog (避免同 key 并发写)
                    drain_async_flush(&engine, &rt, &flush_done);
                    let mut e_borrow = engine.borrow_mut();
                    if let Some(e) = e_borrow.as_mut() {
                        let r = block_on_io(e.flush());
                        let _ = reply.send(match r {
                            Ok(()) => Ok(ShardReply::FlushOk),
                            Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
                        });
                    } else {
                        let _ = reply.send(Err(ShardErrorKind::StorageError(
                            "engine not init".into(),
                        )));
                    }
                }
                ShardRequest::Batch { ops, req_id, reply } => {
                    let mut e_borrow = engine.borrow_mut();
                    if let Some(e) = e_borrow.as_mut() {
                        let mut results = Vec::with_capacity(ops.len());
                        for op in ops {
                            let r = match op {
                                BatchOp::Put { db, table, key, val } => {
                                    match block_on_io(e.table_put(&db, &table, &key, &val)) {
                                        Ok(_) => BatchResult::PutOk,
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::Get { db, table, key } => {
                                    match block_on_io(e.table_get(&db, &table, &key)) {
                                        Ok(v) => BatchResult::GetValue(v),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::Delete { db, table, key } => {
                                    match block_on_io(e.table_delete(&db, &table, &key)) {
                                        Ok(b) => BatchResult::DeleteExisted(b),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                            };
                            results.push(r);
                        }
                        let _ = reply.send(Ok(ShardReply::BatchResults(results)));
                        // reply_bus 支持
                        if req_id > 0 {
                            let sink_opt = reply_sink.lock().expect("reply_sink lock").clone();
                            if let Some(sink) = sink_opt {
                                sink.push_reply(req_id, shard_id as u32, Ok(ShardReply::PutOk));
                            }
                        }
                    } else {
                        let _ = reply.send(Err(ShardErrorKind::StorageError(
                            "engine not init".into(),
                        )));
                    }
                }
                req => {
                    handle_request_blocking(&engine, req, shard_id, &reply_sink);
                }
            }
        }
        // ⭐ 退出完整性: break 后置到 tasks 处理之后 — Shutdown 同轮 drain 到的
        // tasks 先执行并回复, 不静默丢弃 (break 在下方 tasks 块之后).

        // ⭐ 处理 ShardTask (从 spin loop 中一起取到的)
        if !tasks.is_empty() {
            let mut e_borrow = engine.borrow_mut();
            if let Some(e) = e_borrow.as_mut() {
                for task in tasks {
                    let result = match task.op {
                        crate::request::BatchOp::Put { ref db, ref table, ref key, ref val } => {
                            match block_on_io(e.table_put(db, table, key, val)) {
                                Ok(_) => crate::request::BatchResult::PutOk,
                                Err(err) => crate::request::BatchResult::Error(err.to_string()),
                            }
                        }
                        crate::request::BatchOp::Get { ref db, ref table, ref key } => {
                            match block_on_io(e.table_get(db, table, key)) {
                                Ok(v) => crate::request::BatchResult::GetValue(v),
                                Err(err) => crate::request::BatchResult::Error(err.to_string()),
                            }
                        }
                        crate::request::BatchOp::Delete { ref db, ref table, ref key } => {
                            match block_on_io(e.table_delete(db, table, key)) {
                                Ok(b) => crate::request::BatchResult::DeleteExisted(b),
                                Err(err) => crate::request::BatchResult::Error(err.to_string()),
                            }
                        }
                    };
                    reply_bus_set.get(task.worker_id).push(crate::request::TaskResult {
                        conn_id: task.conn_id,
                        req_id: task.req_id,
                        result,
                    });
                }
            }
        }

        // ⭐ Shutdown: 同轮 tasks 已处理完, 退出主循环 (随后 engine.close 做最终 flush)
        if should_shutdown {
            break;
        }

        // ⭐ 每轮循环末尾: 驱动异步落盘 (收割/spawn/周期检查/drive).
        // 磁盘 IO 在协程里并发进行, 不阻塞下一轮请求处理.
        drive_async_flush(&engine, &rt, &flush_done);
    }

    // ⭐ 退出完整性: 先排空异步落盘 backlog, 再 final close (flush 契约).
    drain_async_flush(&engine, &rt, &flush_done);

    // ⭐ 退出完整性: final close = drive_write_queue + flush (nowchunks → meta).
    // 用完成标志等待 (非固定预算), 保证 flush 真正做完才退出线程.
    if let Some(e) = engine.borrow_mut().take() {
        let done = std::rc::Rc::new(std::cell::RefCell::new(false));
        let done2 = done.clone();
        let close_fut = Box::pin(async move {
            if let Err(err) = e.close().await {
                nlog::error!("shard", "shard-{shard_id} close flush failed: {err}");
            }
            *done2.borrow_mut() = true;
        });
        scheduler::spawn_on(&rt, close_fut);
        while !*done.borrow() {
            rt.clone().drive_until_idle(1000);
        }
        nlog::info!("shard", "shard-{shard_id} closed (final flush done)");
    }
}

/// **inline 处理请求**: 同步阻塞当前 shard 线程, 跑 engine async API.
fn handle_request_blocking(
    engine: &std::rc::Rc<std::cell::RefCell<Option<StorageEngine>>>,
    req: ShardRequest,
    shard_id: usize,
    reply_sink: &StdMutex<Option<Arc<dyn ReplySink>>>,
) {
    // 从 req 中取出 reply 句柄
    let reply = match &req {
        ShardRequest::Put { reply, .. }
        | ShardRequest::Get { reply, .. }
        | ShardRequest::Delete { reply, .. }
        | ShardRequest::CreateTable { reply, .. }
        | ShardRequest::CreateDb { reply, .. }
        | ShardRequest::PrepareCreateDb { reply, .. }
        | ShardRequest::CommitCreateDb { reply, .. }
        | ShardRequest::AbortCreateDb { reply, .. }
        | ShardRequest::PrepareCreateTable { reply, .. }
        | ShardRequest::CommitCreateTable { reply, .. }
        | ShardRequest::AbortCreateTable { reply, .. }
        | ShardRequest::Shutdown { reply }
        | ShardRequest::Flush { reply }
        | ShardRequest::Batch { reply, .. } => reply.clone(),
    };

    // 辅助: 同时写 reply 和 (如启用) reply_bus
    //
    // ⭐ 顺序修复 (2026-07-26): 先推 sink 再 reply.send —— reply.send 会唤醒
    // client 线程, 若 sink 后推, client 醒来立即读 sink 可能读到缺条目
    // (全量并行测试下 integration_reply_bus 偶发 1/2 失败).
    let send_reply = |resp: ShardResponse, req_id: u64| {
        // 1. 网络 reply bus (req_id > 0 时) — 先于唤醒
        if req_id > 0 {
            let sink_opt = reply_sink.lock().expect("reply_sink lock").clone();
            if let Some(sink) = sink_opt {
                sink.push_reply(req_id, shard_id as u32, resp.clone());
            }
        }
        // 2. 旧 channel reply (兼容) — reply.send 消耗 self, 所以 clone
        let _ = reply.clone().send(resp);
    };

    let mut e_borrow = engine.borrow_mut();
    let e = match e_borrow.as_mut() {
        Some(e) => e,
        None => {
            send_reply(
                Err(ShardErrorKind::StorageError("engine not init".into())),
                0,
            );
            return;
        }
    };
    match req {
        ShardRequest::Put {
            db,
            table,
            key,
            val,
            req_id,
            ..
        } => {
            let r = block_on_io(e.table_put(&db, &table, &key, &val));
            send_reply(
                match r {
                    Ok(_) => Ok(ShardReply::PutOk),
                    Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
                },
                req_id,
            );
        }
        ShardRequest::Get {
            db,
            table,
            key,
            req_id,
            ..
        } => {
            let r = block_on_io(e.table_get(&db, &table, &key));
            send_reply(
                match r {
                    Ok(v) => Ok(ShardReply::GetValue(v)),
                    Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
                },
                req_id,
            );
        }
        ShardRequest::Delete {
            db,
            table,
            key,
            req_id,
            ..
        } => {
            let r = block_on_io(e.table_delete(&db, &table, &key));
            send_reply(
                match r {
                    Ok(b) => Ok(ShardReply::DeleteExisted(b)),
                    Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
                },
                req_id,
            );
        }
        ShardRequest::CreateTable { db, table, .. } => {
            let r = block_on_io(e.create_table(&db, &table));
            let _ = reply.send(match r {
                Ok(vpid) => Ok(ShardReply::CreateTableOk(vpid)),
                Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
            });
        }
        ShardRequest::CreateDb { db, .. } => {
            let r = block_on_io(e.create_db(&db));
            let _ = reply.send(match r {
                Ok(_) => Ok(ShardReply::CreateDbOk),
                Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
            });
        }
        ShardRequest::PrepareCreateDb { db, .. } => {
            let r = block_on_io(e.create_db(&db));
            let _ = reply.send(match r {
                Ok(_) => Ok(ShardReply::PrepareOk),
                Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
            });
        }
        ShardRequest::CommitCreateDb { .. } => {
            let _ = reply.send(Ok(ShardReply::CommitOk));
        }
        ShardRequest::AbortCreateDb { db, .. } => {
            let _ = block_on_io(e.drop_db(&db));
            let _ = reply.send(Ok(ShardReply::AbortOk));
        }
        ShardRequest::PrepareCreateTable { db, table, .. } => {
            let r = block_on_io(e.create_table(&db, &table));
            let _ = reply.send(match r {
                Ok(_) => Ok(ShardReply::PrepareOk),
                Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
            });
        }
        ShardRequest::CommitCreateTable { .. } => {
            let _ = reply.send(Ok(ShardReply::CommitOk));
        }
        ShardRequest::AbortCreateTable { db, table, .. } => {
            let _ = block_on_io(e.drop_table(&db, &table));
            let _ = reply.send(Ok(ShardReply::AbortOk));
        }
        ShardRequest::Shutdown { .. } => {
            let _ = reply.send(Ok(ShardReply::ShutdownOk));
        }
        ShardRequest::Flush { .. } => {
            let _ = reply.send(Err(ShardErrorKind::StorageError(
                "flush should be handled in main loop".into(),
            )));
        }
        ShardRequest::Batch { .. } => {
            let _ = reply.send(Err(ShardErrorKind::StorageError(
                "batch should be handled in main loop".into(),
            )));
        }
    }
}
