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
    /// ⭐ WAL (F60): 预写日志档位 (Off / Periodic 默认 / Strict).
    pub wal_mode: storage::wal::WalMode,
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
            wal_mode: storage::wal::WalMode::default(),
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
    /// ⭐ D2 (分库): KV 数字 id ↔ db name 双向翻译视图 (resolver 内存镜像).
    db_view: Arc<DbDirView>,
}

/// ⭐ D2 (分库): KV 数字 id ↔ db name 双向翻译视图.
/// `DbNameResolver` (MetaPage 持久化, 各 shard 2PC 同序副本) 的内存镜像;
/// open 时从 shard 0 拉取, create_db 成功后全量刷新 (建库低频).
/// 协议层翻译用: RESP `SELECT n` 查 id→name, SQL 门面未来查 name→id.
#[derive(Default)]
pub struct DbDirView {
    inner: std::sync::RwLock<DbDirInner>,
}

#[derive(Default)]
struct DbDirInner {
    by_id: std::collections::HashMap<u32, Arc<str>>,
    by_name: std::collections::HashMap<Arc<str>, u32>,
}

impl DbDirView {
    /// id → name (RESP SELECT n 翻译). Arc 克隆, 零拷贝.
    pub fn name_of(&self, id: u32) -> Option<Arc<str>> {
        self.inner.read().expect("db_view lock").by_id.get(&id).cloned()
    }

    /// name → id (SQL 门面 / 管理面用).
    pub fn id_of(&self, name: &str) -> Option<u32> {
        self.inner.read().expect("db_view lock").by_name.get(name).copied()
    }

    /// ⭐ F66: 全部 db 名 (information_schema.schemata / pg_namespace 合成).
    pub fn all_names(&self) -> Vec<Arc<str>> {
        self.inner.read().expect("db_view lock").by_id.values().cloned().collect()
    }

    /// 当前库数 (测试/诊断).
    pub fn len(&self) -> usize {
        self.inner.read().expect("db_view lock").by_id.len()
    }

    /// 是否空 (clippy 配套).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 全量替换 (open 初始化 / create_db 后刷新).
    fn replace(&self, list: Vec<(u32, String)>) {
        let mut inner = self.inner.write().expect("db_view lock");
        inner.by_id.clear();
        inner.by_name.clear();
        for (id, name) in list {
            let name: Arc<str> = Arc::from(name.as_str());
            inner.by_id.insert(id, name.clone());
            inner.by_name.insert(name, id);
        }
    }
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
                wal_mode: opts.wal_mode,
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

        let mgr = Self {
            shards,
            threads,
            router,
            num_shards,
            coordinator: std::sync::Mutex::new(TwoPhaseCoordinator::new()),
            reply_sink: reply_sink_arc,
            reply_bus_set,
            db_view: Arc::new(DbDirView::default()),
        };
        // ⭐ D2 (分库): 从 shard 0 (resolver SoT 副本) 初始化 id↔name 视图
        mgr.refresh_db_view();
        Ok(mgr)
    }

    /// ⭐ D2 (分库): 从 shard 0 拉 (id, name) 全表刷新翻译视图.
    fn refresh_db_view(&self) {
        let (tx, fut) = PendingReply::new();
        self.shards[0]
            .inbox
            .push_spin(ShardRequest::ListDbsWithIds { reply: tx });
        if let Ok(ShardReply::DbList(list)) = block_on_v2(fut) {
            self.db_view.replace(list);
        }
    }

    /// ⭐ D2 (分库): 协议层翻译视图 (Arc 共享, worker 只读).
    pub fn db_view(&self) -> Arc<DbDirView> {
        self.db_view.clone()
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
                db: std::sync::Arc::from(db),
                table: std::sync::Arc::from(table),
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
                db: std::sync::Arc::from(db),
                table: std::sync::Arc::from(table),
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

    /// ⭐ Q5 (SQL 索引): 设置表 schema — 顺序广播全 shard (控制面低频,
    /// 幂等可重试; 本轮不走 2PC, 失败即返错由 caller 重试).
    pub fn set_table_schema(
        &self,
        db: &str,
        table: &str,
        schema: &storage::schema::TableSchema,
    ) -> ShardResult<()> {
        let bytes = schema.encode();
        for shard in &self.shards {
            let (tx, fut) = PendingReply::new();
            shard.inbox.push_spin(ShardRequest::SetSchema {
                db: db.to_string(),
                table: table.to_string(),
                bytes: bytes.clone(),
                reply: tx,
            });
            block_on_v2(fut).map_err(ShardError::from_kind)?;
        }
        Ok(())
    }

    /// ⭐ Q5 (SQL 索引): 索引扫描 — 广播全 shard (本地索引 + shard 内回表,
    /// 禁止两跳), 聚合后按 (索引值, pk) 归并为全局序, `limit` 截断 (0 = 不限).
    /// 返回 `(索引原值, pk, row_bytes)`; 任一 shard 报错即整体报错.
    #[allow(clippy::too_many_arguments)]
    pub fn index_scan(
        &self,
        db: &str,
        table: &str,
        iid: u32,
        lo: Option<storage::row::ColValue>,
        hi: Option<storage::row::ColValue>,
        limit: u32,
        with_rows: bool,
    ) -> Result<Vec<storage::sql_rows::IndexEntry>, String> {
        // 每 shard 一份 IndexScan (limit 下推: 每 shard 本地 limit 条已足够全局 top-limit)
        let mut futures = Vec::with_capacity(self.num_shards);
        for shard in &self.shards {
            let (tx, fut) = PendingReply::new();
            let op = BatchOp::IndexScan {
                db: std::sync::Arc::from(db),
                table: std::sync::Arc::from(table),
                iid,
                lo: lo.clone(),
                hi: hi.clone(),
                limit,
                with_rows,
            };
            shard.inbox.push_spin(ShardRequest::Batch {
                ops: vec![op],
                req_id: 0,
                reply: tx,
            });
            futures.push(fut);
        }
        // 聚合: 各 shard 已按 (val, pk) 升序, k 路合并简化为 concat + 排序
        // (shard 数小, N log N 足够; 大结果集时可换真 k 路归并)
        let mut merged: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::new();
        for fut in futures {
            match block_on_v2(fut) {
                Ok(ShardReply::BatchResults(mut rs)) => match rs.pop() {
                    Some(BatchResult::Rows(rows)) => merged.extend(rows),
                    Some(BatchResult::Error(e)) => return Err(e),
                    _ => return Err("unexpected index scan result".to_string()),
                },
                Ok(_) => return Err("unexpected reply".to_string()),
                Err(kind) => return Err(format!("{kind:?}")),
            }
        }
        merged.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        if limit > 0 {
            merged.truncate(limit as usize);
        }
        Ok(merged)
    }

    /// 内部实现: 按 shard 分组 → 每 shard 一次 push + block_on → 重组结果.
    fn batch_ops_inner(&self, ops: &[BatchOp]) -> Vec<BatchResult> {
        if ops.is_empty() {
            return Vec::new();
        }

        // 1. 按 shard 分组, 记录原始索引
        let mut shard_groups: Vec<Vec<(usize, BatchOp)>> = vec![Vec::new(); self.num_shards];
        for (i, op) in ops.iter().enumerate() {
            // ⭐ T1: 单源提取 (Multi op 按第一个 key; worker 已预分组)
            let (db, table, key) = op.locator();
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
            // ⭐ T1: 单源提取 (Multi op 按第一个 key; worker 已预分组)
            let (db, table, key) = op.locator();
            let shard_id = self.route_db_table_key(db, table, key);
            self.shards[shard_id].task_inbox.push_spin(ShardTask {
                conn_id,
                req_id: i as u64,
                worker_id,
                group: 0,
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
            // ⭐ D2 (分库): 建库成功 → 刷新 id↔name 翻译视图
            self.refresh_db_view();
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

/// ⭐ String RMW: INCR/DECR (shard 单线程内天然原子).
///
/// ⭐ 数值原生存储 (N2): 按 tag 分派 —
/// - TAG_I64: 直接 LE 读 + checked_add → 写回 TAG_I64 (8B 二进制)
/// - TAG_RAW 十进制文本 (Redis 兼容/存量): parse i64 → **结果升级为 TAG_I64**
/// - TAG_F64/F32: Redis 语义报 "not an integer"
fn exec_incr(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    delta: i64,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    use crate::value_num;
    let old = match block_on_io(e.table_get(db, table, key)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    let cur: i64 = match &old {
        None => 0,
        Some(stored) => match value_num::parse_num_int_only(stored) {
            Some(n) => n,
            None => {
                return BatchResult::Error(
                    "value is not an integer or out of range".to_string(),
                );
            }
        },
    };
    let Some(newv) = cur.checked_add(delta) else {
        return BatchResult::Error("increment or decrement would overflow".to_string());
    };
    // ⭐ 原生二进制写回 (TAG_I64 + 8B LE), 非十进制字符串
    let stored = value_num::encode_i64(newv);
    match block_on_io(e.table_put(db, table, key, &stored)) {
        Ok(_) => BatchResult::Integer(newv),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ String RMW: INCRBYFLOAT (N2). 按 tag 分派, 结果写回 TAG_F64 8B LE.
///
/// Redis 语义: 整数值/文本数字均可参与; 结果 NaN/inf 拒绝.
fn exec_incr_float(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    delta: f64,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    use crate::value_num::{self, NumValue};
    let old = match block_on_io(e.table_get(db, table, key)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    let cur: f64 = match &old {
        None => 0.0,
        Some(stored) => match value_num::parse_num(stored) {
            Some(NumValue::I64(n)) => n as f64,
            Some(NumValue::F64(f)) => f,
            Some(NumValue::F32(f)) => f as f64, // f32 → f64 无损提升
            None => return BatchResult::Error("value is not a valid float".to_string()),
        },
    };
    let newv = cur + delta;
    if !newv.is_finite() {
        return BatchResult::Error("increment would produce NaN or Infinity".to_string());
    }
    // ⭐ 原生二进制写回 (TAG_F64 + 8B LE)
    let stored = value_num::encode_f64(newv);
    match block_on_io(e.table_put(db, table, key, &stored)) {
        Ok(_) => BatchResult::Double(newv),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ String RMW: APPEND. 返回追加后 payload 长度.
///
/// ⭐ N2: 数值 tag 值先 `render` 字符串化再拼接, 结果退回 TAG_RAW
/// (Redis 语义: append 后是普通 string).
fn exec_append(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    suffix: &[u8],
) -> crate::request::BatchResult {
    use crate::request::{BatchResult, VALUE_TAG_RAW};
    use crate::value_num;
    let old = match block_on_io(e.table_get(db, table, key)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    let mut stored = match old {
        Some(s) if s.first() == Some(&VALUE_TAG_RAW) => s,
        Some(s) => {
            // 数值 tag / 无 tag 存量: 渲染字符串化后归一为 RAW
            let rendered = value_num::render(&s);
            let mut t = Vec::with_capacity(1 + rendered.len());
            t.push(VALUE_TAG_RAW);
            t.extend_from_slice(&rendered);
            t
        }
        None => vec![VALUE_TAG_RAW],
    };
    stored.extend_from_slice(suffix);
    let new_len = (stored.len() - 1) as i64;
    match block_on_io(e.table_put(db, table, key, &stored)) {
        Ok(_) => BatchResult::Integer(new_len),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ SETNX: 不存在才写. 返回 1=写入, 0=已存在.
fn exec_setnx(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    val: &[u8],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match block_on_io(e.table_get(db, table, key)) {
        Ok(Some(_)) => BatchResult::Integer(0),
        Ok(None) => match block_on_io(e.table_put(db, table, key, val)) {
            Ok(_) => BatchResult::Integer(1),
            Err(err) => BatchResult::Error(err.to_string()),
        },
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ GETDEL: 返回旧值 + 删除 (table_delete 内部释放溢出链).
fn exec_getdel(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    let old = match block_on_io(e.table_get(db, table, key)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    if old.is_some()
        && let Err(err) = block_on_io(e.table_delete(db, table, key))
    {
        return BatchResult::Error(err.to_string());
    }
    BatchResult::GetValue(old)
}

/// ⭐ GETSET: 写新值 + 返回旧值 (val 已带 tag).
fn exec_getset(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    val: &[u8],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    let old = match block_on_io(e.table_get(db, table, key)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    match block_on_io(e.table_put(db, table, key, val)) {
        Ok(_) => BatchResult::GetValue(old),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Phase B: SETBIT — shard 端 RMW (零扩展到 offset/8+1), 返回旧 bit.
fn exec_setbit(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    offset: u64,
    bit: bool,
) -> crate::request::BatchResult {
    use crate::request::{BatchResult, VALUE_TAG_RAW};
    use crate::value_num;
    let old = match block_on_io(e.table_get(db, table, key)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    let mut base: Vec<u8> = match &old {
        Some(s) => value_num::render(s).into_owned(),
        None => Vec::new(),
    };
    let byte = (offset / 8) as usize;
    let mask = 1u8 << (7 - (offset % 8) as u8);
    if base.len() <= byte {
        base.resize(byte + 1, 0u8); // 零扩展
    }
    let old_bit = (base[byte] & mask) != 0;
    if bit {
        base[byte] |= mask;
    } else {
        base[byte] &= !mask;
    }
    let mut stored = Vec::with_capacity(1 + base.len());
    stored.push(VALUE_TAG_RAW);
    stored.extend_from_slice(&base);
    match block_on_io(e.table_put(db, table, key, &stored)) {
        Ok(_) => BatchResult::Integer(i64::from(old_bit)),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ X2: 数据面 schema 分发 — decode 校验后落 `[$]` 行 + 常驻镜像 (幂等).
/// 表由 exec 前置的惰性建表保证存在.
fn exec_set_schema(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    bytes: &[u8],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match storage::schema::TableSchema::decode(bytes) {
        Ok(schema) => match block_on_io(e.set_schema(db, table, &schema)) {
            Ok(()) => BatchResult::PutOk,
            Err(err) => BatchResult::Error(err.to_string()),
        },
        Err(err) => BatchResult::Error(format!("bad schema bytes: {err}")),
    }
}

/// ⭐ X2: 读表 schema 字节 (worker 缓存 miss 拉取; None = 纯 KV 表).
fn exec_get_schema(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match block_on_io(e.get_schema(db, table)) {
        Ok(s) => BatchResult::GetValue(s.map(|s| s.encode())),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Q5: RowPut — shard 端引擎内部维护 row 行 + 索引行 (同 shard 原子).
fn exec_row_put(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    pk: &[u8],
    values: &[storage::row::ColValue],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match block_on_io(e.row_put(db, table, pk, values)) {
        Ok(()) => BatchResult::PutOk,
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Q5: IndexScan — shard 内闭环 "本地索引扫 → 本地回表" (禁止两跳).
#[allow(clippy::too_many_arguments)]
fn exec_index_scan(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    iid: u32,
    lo: Option<&storage::row::ColValue>,
    hi: Option<&storage::row::ColValue>,
    limit: u32,
    with_rows: bool,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match block_on_io(e.index_scan_entries_local(db, table, iid, lo, hi, limit as usize, with_rows))
    {
        Ok(rows) => BatchResult::Rows(rows),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ 事务 v1 (F61): COMMIT 原子批 — 先验后写.
///
/// 预检 (零部分应用红线):
/// 1. 全部 op 的表 ensure_table (惰性建表)
/// 2. RowPut 逐个 row_put_check (schema/编码/类型/UNIQUE)
/// 3. 批内自冲突: 不同 pk 写同一 unique 值 (互相看不见盘上探测) → 拒
///
/// 应用: 逐 op 执行 (shard 单线程 = 批内零并发穿插); 预检后仅剩 IO 级
/// 失败 (灾难态, 回复标注 partially applied). 完成后无条件 wal_barrier
/// (事务语义: 回复到达 ⇒ 已持久; wal_mode=off 时退化, 文档化).
fn exec_txn_apply(
    e: &mut StorageEngine,
    ops: Vec<crate::request::BatchOp>,
    read_set: Vec<crate::request::ReadCheck>,
) -> crate::request::BatchResult {
    use crate::request::{BatchOp, BatchResult};
    // --- ⭐ v2 (F62): OCC 读集验证 (SERIALIZABLE) — 重读比对指纹,
    // 变了整批拒 (shard 单线程: 验证+应用之间零并发窗口) ---
    for rc in &read_set {
        // 表已删/不存在 → 当作行不存在 (读时若存在则必冲突)
        let cur = block_on_io(e.row_get(&rc.db, &rc.table, &rc.pk)).unwrap_or_default();
        let cur_fp = cur.as_deref().map(storage::wal::crc32);
        if cur_fp != rc.fp {
            return BatchResult::Error(
                "serialization failure: concurrent update detected (retry transaction)".into(),
            );
        }
    }
    // --- 预检 ---
    let mut batch_uniques: std::collections::HashMap<(String, u32, Vec<u8>), Vec<u8>> =
        std::collections::HashMap::new();
    for op in &ops {
        let (db, table, _) = op.locator();
        if let Err(err) = block_on_io(e.ensure_table(db, table)) {
            return BatchResult::Error(err.to_string());
        }
        if let BatchOp::RowPut { db, table, pk, values } = op {
            if let Err(err) = block_on_io(e.row_put_check(db, table, pk, values)) {
                return BatchResult::Error(err.to_string());
            }
            // 批内自冲突 (盘上探测看不见未应用的同批写)
            if let Ok(Some(schema)) = block_on_io(e.get_schema(db, table)) {
                for idx in schema.indexes.iter().filter(|i| i.unique) {
                    let ty = schema.columns[idx.col as usize].ty;
                    if let Some(nv) =
                        storage::sql_rows::index_val_bytes(ty, &values[idx.col as usize])
                    {
                        let key = (format!("{db}\u{0}{table}"), idx.iid, nv);
                        if let Some(prev) = batch_uniques.insert(key, pk.clone())
                            && &prev != pk
                        {
                            return BatchResult::Error(format!(
                                "duplicate key on unique column '{}' (within transaction)",
                                schema.columns[idx.col as usize].name
                            ));
                        }
                    }
                }
            }
        }
    }
    // --- 应用 ---
    let n = ops.len() as u64;
    for op in ops {
        if let BatchResult::Error(err) = exec_task_op(e, op) {
            return BatchResult::Error(format!("txn partially applied (IO-level): {err}"));
        }
    }
    // --- 事务持久化屏障 (独立于 wal_mode strict/periodic) ---
    if let Err(err) = block_on_io(e.wal_barrier()) {
        return BatchResult::Error(format!("txn applied but WAL sync failed: {err}"));
    }
    BatchResult::TxnApplied(n)
}

/// ⭐ 事务 v1 (F61): 单 op 执行 — ShardTask 热路径与 TxnApply 原子批共用.
/// 从 shard_thread_main 的 ShardTask 臂原样提取, 行为零变化.
fn exec_task_op(
    e: &mut StorageEngine,
    op: crate::request::BatchOp,
) -> crate::request::BatchResult {
    match op {
        crate::request::BatchOp::Put { ref db, ref table, ref key, ref val } => {
            match block_on_io(e.table_put(db, table, key, val)) {
                Ok(_) => crate::request::BatchResult::PutOk,
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::Get { ref db, ref table, ref key } => {
            // ⭐ Phase H: 类型感知 (hash key → WRONGTYPE)
            match block_on_io(e.table_get_typed(db, table, key)) {
                Ok(v) => crate::request::BatchResult::GetValue(v),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::Delete { ref db, ref table, ref key } => {
            // ⭐ Phase H: 类型感知 (顺带清 hash 全部行/孤儿行)
            match block_on_io(e.key_delete_any(db, table, key)) {
                Ok(b) => crate::request::BatchResult::DeleteExisted(b),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        // ⭐ MGET/MSET 分片: shard 内 LeafGuide 区间复用批量执行
        crate::request::BatchOp::MultiGet { ref db, ref table, ref keys } => {
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
            match block_on_io(e.table_get_many(db, table, &refs)) {
                Ok(vs) => crate::request::BatchResult::Values(vs),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::MultiPut { ref db, ref table, ref pairs } => {
            match block_on_io(e.table_put_many(db, table, pairs)) {
                Ok(_) => crate::request::BatchResult::MultiPutOk,
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::MultiPutNx { ref db, ref table, ref pairs } => {
            exec_multiputnx(e, db, table, pairs)
        }
        // ⭐ String RMW (shard 单线程内天然原子)
        crate::request::BatchOp::Incr { ref db, ref table, ref key, delta } => {
            exec_incr(e, db, table, key, delta)
        }
        crate::request::BatchOp::IncrFloat { ref db, ref table, ref key, delta } => {
            exec_incr_float(e, db, table, key, delta)
        }
        crate::request::BatchOp::Append { ref db, ref table, ref key, ref suffix } => {
            exec_append(e, db, table, key, suffix)
        }
        crate::request::BatchOp::SetNx { ref db, ref table, ref key, ref val } => {
            exec_setnx(e, db, table, key, val)
        }
        crate::request::BatchOp::GetDel { ref db, ref table, ref key } => {
            exec_getdel(e, db, table, key)
        }
        crate::request::BatchOp::GetSet { ref db, ref table, ref key, ref val } => {
            exec_getset(e, db, table, key, val)
        }
        crate::request::BatchOp::SetRange { ref db, ref table, ref key, offset, ref data } => {
            exec_setrange(e, db, table, key, offset, data)
        }
        // ⭐ M3-2 (CBO): 表近似行数 (内存增量统计; 未统计=0)
        crate::request::BatchOp::EstimateRowCount { ref db, ref table } => {
            crate::request::BatchResult::RowCount(
                e.estimate_row_count(db, table).unwrap_or(0),
            )
        }
        // ⭐ M3-4 (CBO): 索引列 distinct (worker 已算好 iid; 未统计=0)
        crate::request::BatchOp::EstimateDistinct { ref db, ref table, ref iids } => {
            crate::request::BatchResult::DistinctCounts(
                iids.iter()
                    .map(|iid| e.estimate_distinct(db, table, *iid).unwrap_or(0))
                    .collect(),
            )
        }
        // ⭐ M3-5 (CBO): 索引列 (min, max) 有序字节 (未统计 = (None, None))
        crate::request::BatchOp::EstimateRanges { ref db, ref table, ref iids } => {
            crate::request::BatchResult::RangeBounds(
                iids.iter()
                    .map(|iid| {
                        e.estimate_range(db, table, *iid)
                            .map(|(lo, hi)| (Some(lo), Some(hi)))
                            .unwrap_or((None, None))
                    })
                    .collect(),
            )
        }
        // ⭐ Phase H: Hash ops (单 key 单 shard, 无需聚合)
        crate::request::BatchOp::HSet { ref db, ref table, ref key, ref pairs } => {
            match block_on_io(e.hash_set(db, table, key, pairs)) {
                Ok(n) => crate::request::BatchResult::Integer(n),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::HSetNx { ref db, ref table, ref key, ref field, ref val } => {
            match block_on_io(e.hash_set_nx(db, table, key, field, val)) {
                Ok(n) => crate::request::BatchResult::Integer(n),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::HGet { ref db, ref table, ref key, ref field } => {
            match block_on_io(e.hash_get(db, table, key, field)) {
                Ok(v) => crate::request::BatchResult::GetValue(v),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::HMGet { ref db, ref table, ref key, ref fields } => {
            match block_on_io(e.hash_get_many(db, table, key, fields)) {
                Ok(vs) => crate::request::BatchResult::Values(vs),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::HDel { ref db, ref table, ref key, ref fields } => {
            match block_on_io(e.hash_del(db, table, key, fields)) {
                Ok(n) => crate::request::BatchResult::Integer(n),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::HLen { ref db, ref table, ref key } => {
            match block_on_io(e.hash_len(db, table, key)) {
                Ok(n) => crate::request::BatchResult::Integer(n),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::HGetAll { ref db, ref table, ref key } => {
            match block_on_io(e.hash_get_all(db, table, key)) {
                Ok(ps) => crate::request::BatchResult::Pairs(ps),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::HIncrBy { ref db, ref table, ref key, ref field, delta } => {
            exec_hincrby(e, db, table, key, field, delta)
        }
        crate::request::BatchOp::HIncrByFloat { ref db, ref table, ref key, ref field, delta } => {
            exec_hincrbyfloat(e, db, table, key, field, delta)
        }
        // ⭐ Phase Set: Set ops
        crate::request::BatchOp::SAdd { ref db, ref table, ref key, ref members } => {
            match block_on_io(e.set_add(db, table, key, members)) {
                Ok(n) => crate::request::BatchResult::Integer(n),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::SRem { ref db, ref table, ref key, ref members } => {
            match block_on_io(e.set_rem(db, table, key, members)) {
                Ok(n) => crate::request::BatchResult::Integer(n),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::SIsMember { ref db, ref table, ref key, ref member } => {
            match block_on_io(e.set_is_member(db, table, key, member)) {
                Ok(b) => crate::request::BatchResult::Integer(i64::from(b)),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::SCard { ref db, ref table, ref key } => {
            match block_on_io(e.set_card(db, table, key)) {
                Ok(n) => crate::request::BatchResult::Integer(n),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::SMembers { ref db, ref table, ref key } => {
            match block_on_io(e.set_members(db, table, key)) {
                Ok(ms) => crate::request::BatchResult::Members(ms),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::SPop { ref db, ref table, ref key } => {
            exec_spop(e, db, table, key)
        }
        crate::request::BatchOp::SRandMember { ref db, ref table, ref key } => {
            match block_on_io(e.set_pick_one(db, table, key)) {
                Ok(m) => crate::request::BatchResult::Members(m.into_iter().collect()),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        // ⭐ Phase L: List ops
        crate::request::BatchOp::LPush { ref db, ref table, ref key, ref values, left } => {
            match block_on_io(e.list_push(db, table, key, values, left)) {
                Ok(n) => crate::request::BatchResult::Integer(n),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::LPop { ref db, ref table, ref key, left, count } => {
            exec_lpop(e, db, table, key, left, count as usize)
        }
        crate::request::BatchOp::LLen { ref db, ref table, ref key } => {
            match block_on_io(e.list_len(db, table, key)) {
                Ok(n) => crate::request::BatchResult::Integer(n),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::LRange { ref db, ref table, ref key, start, end } => {
            exec_lrange(e, db, table, key, start, end)
        }
        crate::request::BatchOp::LIndex { ref db, ref table, ref key, idx } => {
            match block_on_io(e.list_index(db, table, key, idx)) {
                Ok(v) => crate::request::BatchResult::GetValue(v),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::LSet { ref db, ref table, ref key, idx, ref val } => {
            exec_lset(e, db, table, key, idx, val)
        }
        // ⭐ Phase Z: ZSet ops
        crate::request::BatchOp::ZAdd { ref db, ref table, ref key, ref pairs } => {
            match block_on_io(e.zset_add(db, table, key, pairs)) {
                Ok(n) => crate::request::BatchResult::Integer(n),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::ZRem { ref db, ref table, ref key, ref members } => {
            match block_on_io(e.zset_rem(db, table, key, members)) {
                Ok(n) => crate::request::BatchResult::Integer(n),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::ZScore { ref db, ref table, ref key, ref member } => {
            match block_on_io(e.zset_score(db, table, key, member)) {
                Ok(s) => crate::request::BatchResult::OptMember(s.map(fmt_score)),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::ZCard { ref db, ref table, ref key } => {
            match block_on_io(e.zset_card(db, table, key)) {
                Ok(n) => crate::request::BatchResult::Integer(n),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::ZIncrBy { ref db, ref table, ref key, delta, ref member } => {
            match block_on_io(e.zset_incr(db, table, key, delta, member)) {
                Ok(s) => crate::request::BatchResult::Double(s),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::ZRange { ref db, ref table, ref key, start, end, rev, withscores } => {
            match block_on_io(e.zset_range(db, table, key, start, end, rev)) {
                Ok(rows) => crate::request::BatchResult::Members(zrows_to_members(rows, withscores)),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::ZRangeByScore { ref db, ref table, ref key, min, max, withscores } => {
            match block_on_io(e.zset_range_by_score(db, table, key, min, max)) {
                Ok(rows) => crate::request::BatchResult::Members(zrows_to_members(rows, withscores)),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::ZRank { ref db, ref table, ref key, ref member, rev } => {
            match block_on_io(e.zset_rank(db, table, key, member, rev)) {
                Ok(Some(r)) => crate::request::BatchResult::Integer(r),
                Ok(None) => crate::request::BatchResult::OptMember(None),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::ZCount { ref db, ref table, ref key, min, max } => {
            match block_on_io(e.zset_range_by_score(db, table, key, min, max)) {
                Ok(rows) => crate::request::BatchResult::Integer(rows.len() as i64),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::ZMScore { ref db, ref table, ref key, ref members } => {
            match block_on_io(e.zset_mscore(db, table, key, members)) {
                Ok(scores) => crate::request::BatchResult::Values(
                    scores.into_iter().map(|s| s.map(fmt_score)).collect(),
                ),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::ZPop { ref db, ref table, ref key, rev, count } => {
            match block_on_io(e.zset_pop(db, table, key, rev, count as usize)) {
                Ok(rows) => crate::request::BatchResult::Members(zrows_to_members(rows, true)),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::SMisMember { ref db, ref table, ref key, ref members } => {
            match block_on_io(e.set_mismember(db, table, key, members)) {
                Ok(bs) => crate::request::BatchResult::IntList(
                    bs.into_iter().map(i64::from).collect(),
                ),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::SPopN { ref db, ref table, ref key, count } => {
            match block_on_io(e.set_pop_n(db, table, key, count as usize)) {
                Ok(ms) => crate::request::BatchResult::Members(ms),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::SRandCount { ref db, ref table, ref key, count } => {
            match block_on_io(e.set_rand_n(db, table, key, count as usize)) {
                Ok(ms) => crate::request::BatchResult::Members(ms),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::HRandField { ref db, ref table, ref key, count, .. } => {
            match block_on_io(e.hash_rand(db, table, key, count as usize)) {
                Ok(ps) => crate::request::BatchResult::Pairs(ps),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::LRem { ref db, ref table, ref key, count, ref val } => {
            match block_on_io(e.list_rem(db, table, key, count, val)) {
                Ok(n) => crate::request::BatchResult::Integer(n),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::LTrim { ref db, ref table, ref key, start, stop } => {
            match block_on_io(e.list_trim(db, table, key, start, stop)) {
                Ok(()) => crate::request::BatchResult::Integer(1),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::LPos { ref db, ref table, ref key, ref val, rank, count } => {
            exec_lpos(e, db, table, key, val, rank, count)
        }
        crate::request::BatchOp::LInsert { ref db, ref table, ref key, before, ref pivot, ref val } => {
            match block_on_io(e.list_insert(db, table, key, before, pivot, val)) {
                Ok(n) => crate::request::BatchResult::Integer(n),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::SetBit { ref db, ref table, ref key, offset, bit } => {
            exec_setbit(e, db, table, key, offset, bit)
        }
        // ---- ⭐ Q5: SQL row 表 ----
        crate::request::BatchOp::RowPut { ref db, ref table, ref pk, ref values } => {
            exec_row_put(e, db, table, pk, values)
        }
        crate::request::BatchOp::RowGet { ref db, ref table, ref pk } => {
            match block_on_io(e.row_get(db, table, pk)) {
                Ok(v) => crate::request::BatchResult::GetValue(v),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::RowDelete { ref db, ref table, ref pk } => {
            match block_on_io(e.row_delete(db, table, pk)) {
                Ok(existed) => crate::request::BatchResult::DeleteExisted(existed),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::RowUpdate { ref db, ref table, ref pk, ref sets } => {
            match block_on_io(e.row_update(db, table, pk, sets)) {
                Ok(updated) => crate::request::BatchResult::DeleteExisted(updated),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::DropTableOp { ref db, ref table } => {
            match block_on_io(e.drop_table_sql(db, table)) {
                Ok(_) => crate::request::BatchResult::PutOk,
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::TableScan { ref db, ref table, limit } => {
            exec_table_scan(e, db, table, limit)
        }
        crate::request::BatchOp::ScanFiltered { ref db, ref table, ref preds, ref proj, ref index_hint, ref key_set_hint, limit } => {
            exec_scan_filtered(e, db, table, preds, proj, index_hint.as_ref(), key_set_hint.as_ref(), limit)
        }
        crate::request::BatchOp::IndexScan {
            ref db, ref table, iid, ref lo, ref hi, limit, with_rows,
        } => exec_index_scan(
            e, db, table, iid, lo.as_ref(), hi.as_ref(), limit, with_rows,
        ),
        crate::request::BatchOp::SetSchemaOp { ref db, ref table, ref bytes } => {
            exec_set_schema(e, db, table, bytes)
        }
        crate::request::BatchOp::GetSchemaOp { ref db, ref table } => {
            exec_get_schema(e, db, table)
        }
        // ⭐ 事务 v1 (F61): COMMIT 原子批 — 先验后写 + 逐 op 应用.
        // shard 单线程 = 批内零并发穿插; 预检失败整批拒绝 (零部分应用);
        // wal_barrier 由 caller (ShardTask 臂) 在回复前统一执行.
        crate::request::BatchOp::TxnApply { ops, read_set } => exec_txn_apply(e, ops, read_set),
        // ⭐ F65: 全局 UNIQUE 占坑原语 (email-shard 单线程原子)
        crate::request::BatchOp::ReserveUnique { db, table, iid, enc_val, pk, txn_id } => {
            match block_on_io(e.unique_reserve(&db, &table, iid, &enc_val, &pk, txn_id)) {
                Ok(None) => crate::request::BatchResult::ReserveOk,
                Ok(Some((state, holder_txn, holder_pk))) => {
                    crate::request::BatchResult::ReserveConflict { state, holder_txn, holder_pk }
                }
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::StealUnique { db, table, iid, enc_val, pk, txn_id } => {
            match block_on_io(e.unique_steal(&db, &table, iid, &enc_val, &pk, txn_id)) {
                Ok(()) => crate::request::BatchResult::ReserveOk,
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::ConfirmUnique { db, table, iid, enc_val, pk, txn_id } => {
            match block_on_io(e.unique_confirm(&db, &table, iid, &enc_val, &pk, txn_id)) {
                Ok(()) => crate::request::BatchResult::PutOk,
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::ReleaseUnique { db, table, iid, enc_val, txn_id } => {
            match block_on_io(e.unique_release(&db, &table, iid, &enc_val, txn_id)) {
                Ok(()) => crate::request::BatchResult::PutOk,
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        // ⭐ F66: catalog 快照 — 列当前 db 全表 + schema (任意单 shard).
        crate::request::BatchOp::CatalogDump { db } => {
            let tables = match e.list_tables(&db) {
                Ok(t) => t,
                Err(err) => return crate::request::BatchResult::Error(err.to_string()),
            };
            let mut out = Vec::with_capacity(tables.len());
            for t in tables {
                match block_on_io(e.get_schema(&db, &t)) {
                    Ok(Some(sc)) => out.push((t, sc.encode())),
                    Ok(None) => {} // 无 schema 的纯 KV 表不入 catalog
                    Err(err) => return crate::request::BatchResult::Error(err.to_string()),
                }
            }
            crate::request::BatchResult::Catalog(out)
        }
    }
}

/// ⭐ S2: 全表扫 (广播 op; `[S]` 前缀收 TAG_ROW 行).
fn exec_table_scan(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    limit: u32,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    let mut out = Vec::new();
    match block_on_io(e.table_scan_rows_local(db, table, limit as usize, &mut out)) {
        Ok(()) => BatchResult::Rows(out),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ F67 (JOIN): 带谓词+投影下推的全表扫 → ProjRows.
#[allow(clippy::too_many_arguments)]
fn exec_scan_filtered(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    preds: &[crate::request::ScanPred],
    proj: &[u16],
    index_hint: Option<&storage::sql_rows::IndexHint>,
    key_set_hint: Option<&storage::sql_rows::KeySetHint>,
    limit: u32,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    let mut out = Vec::new();
    match block_on_io(e.table_scan_filtered_local(
        db, table, preds, proj, index_hint, key_set_hint, limit as usize, &mut out,
    )) {
        Ok(()) => BatchResult::ProjRows(out),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ SETRANGE: 从 offset 覆盖写 data (零扩展), 结果归一为 TAG_RAW,
/// 返回新长度. data 空 → 不写, 返回当前长度 (Redis 语义).
fn exec_setrange(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    offset: u32,
    data: &[u8],
) -> crate::request::BatchResult {
    use crate::request::{BatchResult, VALUE_TAG_RAW};
    use crate::value_num;
    let old = match block_on_io(e.table_get(db, table, key)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    let mut base: Vec<u8> = match &old {
        Some(s) => value_num::render(s).into_owned(),
        None => Vec::new(),
    };
    if data.is_empty() {
        return BatchResult::Integer(base.len() as i64);
    }
    let offset = offset as usize;
    let end = offset + data.len();
    if base.len() < end {
        base.resize(end, 0u8); // 零扩展
    }
    base[offset..end].copy_from_slice(data);
    let new_len = base.len() as i64;
    let mut stored = Vec::with_capacity(1 + base.len());
    stored.push(VALUE_TAG_RAW);
    stored.extend_from_slice(&base);
    match block_on_io(e.table_put(db, table, key, &stored)) {
        Ok(_) => BatchResult::Integer(new_len),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ MSETNX 分片: 本组全部 key 不存在才写. 返回 1=写入, 0=有 key 已存在.
/// (跨 shard 非原子: 别组可能已写 — 已记为 gap.)
fn exec_multiputnx(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    pairs: &[(Vec<u8>, Vec<u8>)],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    for (k, _) in pairs {
        match block_on_io(e.table_get(db, table, k)) {
            Ok(Some(_)) => return BatchResult::Integer(0),
            Ok(None) => {}
            Err(err) => return BatchResult::Error(err.to_string()),
        }
    }
    match block_on_io(e.table_put_many(db, table, pairs)) {
        Ok(_) => BatchResult::Integer(1),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Phase H: HINCRBY — field 整数 RMW, 结果写回 TAG_I64.
fn exec_hincrby(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    field: &[u8],
    delta: i64,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    use crate::value_num;
    let old = match block_on_io(e.hash_get(db, table, key, field)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    let cur: i64 = match &old {
        None => 0,
        Some(stored) => match value_num::parse_num_int_only(stored) {
            Some(n) => n,
            None => return BatchResult::Error("hash value is not an integer".into()),
        },
    };
    let Some(new) = cur.checked_add(delta) else {
        return BatchResult::Error("increment or decrement would overflow".into());
    };
    let pair = vec![(field.to_vec(), value_num::encode_i64(new))];
    match block_on_io(e.hash_set(db, table, key, &pair)) {
        Ok(_) => BatchResult::Integer(new),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Phase H: HINCRBYFLOAT — field 浮点 RMW, 结果写回 TAG_F64.
fn exec_hincrbyfloat(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    field: &[u8],
    delta: f64,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    use crate::value_num::{self, NumValue};
    let old = match block_on_io(e.hash_get(db, table, key, field)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    let cur: f64 = match &old {
        None => 0.0,
        Some(stored) => match value_num::parse_num(stored) {
            Some(NumValue::I64(n)) => n as f64,
            Some(NumValue::F32(f)) => f as f64,
            Some(NumValue::F64(f)) => f,
            None => return BatchResult::Error("hash value is not a float".into()),
        },
    };
    let new = cur + delta;
    if !new.is_finite() {
        return BatchResult::Error("increment would produce NaN or Infinity".into());
    }
    let pair = vec![(field.to_vec(), value_num::encode_f64(new))];
    match block_on_io(e.hash_set(db, table, key, &pair)) {
        Ok(_) => BatchResult::Double(new),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Phase Set: SPOP — 取任意成员 + 删除 (shard 内原子).
fn exec_spop(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match block_on_io(e.set_pick_one(db, table, key)) {
        Ok(Some(m)) => match block_on_io(e.set_rem(db, table, key, std::slice::from_ref(&m))) {
            Ok(_) => BatchResult::Members(vec![m]),
            Err(err) => BatchResult::Error(err.to_string()),
        },
        Ok(None) => BatchResult::Members(vec![]),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ C2: LPOS — count 缺省回首位 (Integer / nil), 否则 IntList (count=0 全部).
fn exec_lpos(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    val: &[u8],
    rank: i64,
    count: Option<u32>,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match count {
        None => match block_on_io(e.list_pos(db, table, key, val, rank, 1)) {
            Ok(ps) => match ps.first() {
                Some(&p) => BatchResult::Integer(p),
                None => BatchResult::OptMember(None),
            },
            Err(err) => BatchResult::Error(err.to_string()),
        },
        Some(c) => match block_on_io(e.list_pos(db, table, key, val, rank, c as usize)) {
            Ok(ps) => BatchResult::IntList(ps),
            Err(err) => BatchResult::Error(err.to_string()),
        },
    }
}

/// ⭐ Phase L: LPOP/RPOP — 弹出后 render 剥 tag (与 Set 统一为就绪字节).
fn exec_lpop(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    left: bool,
    count: usize,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    use crate::value_num;
    match block_on_io(e.list_pop(db, table, key, left, count)) {
        Ok(vs) => {
            BatchResult::Members(vs.iter().map(|v| value_num::render(v).into_owned()).collect())
        }
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Phase L: LRANGE — 区间元素 render 剥 tag.
fn exec_lrange(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    start: i64,
    end: i64,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    use crate::value_num;
    match block_on_io(e.list_range(db, table, key, start, end)) {
        Ok(vs) => {
            BatchResult::Members(vs.iter().map(|v| value_num::render(v).into_owned()).collect())
        }
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Phase L: LSET — 越界回 Redis 错误, 否则 Integer(1) (worker 转 +OK).
fn exec_lset(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    idx: i64,
    val: &[u8],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match block_on_io(e.list_set(db, table, key, idx, val)) {
        Ok(true) => BatchResult::Integer(1),
        Ok(false) => BatchResult::Error("index out of range".into()),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Phase Z: score 渲染为 Redis 风格字符串 (3.0→"3", 3.5→"3.5").
fn fmt_score(s: f64) -> Vec<u8> {
    format!("{s}").into_bytes()
}

/// ⭐ Phase Z: (member, score) 列表 → Members (withscores 时 member/score 交替).
fn zrows_to_members(rows: Vec<(Vec<u8>, f64)>, withscores: bool) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(if withscores { rows.len() * 2 } else { rows.len() });
    for (m, sc) in rows {
        out.push(m);
        if withscores {
            out.push(fmt_score(sc));
        }
    }
    out
}

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

/// ⭐ 异步落盘完成事件: data chunk / meta window / compact 两阶段 (G2).
enum FlushDone {
    Data(storage::PageKey, std::io::Result<()>),
    Meta(u32, std::io::Result<()>),
    /// G2 阶段 1 完成: (dst, src, dst_fresh, 读结果 (dst_bytes, src_bytes))
    CompactRead(
        storage::PageKey,
        storage::PageKey,
        bool,
        std::io::Result<(Vec<u8>, Vec<u8>)>,
    ),
    /// G2 阶段 2 完成: (dst, src, moves, 写结果)
    CompactWrite(
        storage::PageKey,
        storage::PageKey,
        Vec<(u64, storage::PidLocation, u8)>,
        std::io::Result<()>,
    ),
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
    // ⭐ DIAG: NLOG_NO_FLUSH=1 禁用异步落盘 (定位数据丢失根因)
    if std::env::var("NLOG_NO_FLUSH").is_ok_and(|v| v == "1") {
        return;
    }
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
                        // ⭐ WAL (F60): meta 全部持久化 → sealed 段可删
                        e.wal_drop_sealed_if_meta_flushed();
                    }
                    // ⭐ G2 阶段 2 (同步): meta 判活 → 组装写作业 → 低优先级协程写盘
                    FlushDone::CompactRead(dst, src, dst_fresh, read_result) => {
                        if let Some(wj) =
                            e.pager_mut().analyze_compact_read(dst, src, dst_fresh, read_result)
                        {
                            let done = flush_done.clone();
                            scheduler::spawn_on_low(
                                rt,
                                Box::pin(async move {
                                    // fresh dst 整 chunk 写 / 常规 dst 死槽批写
                                    let r = wj.execute().await;
                                    done.borrow_mut().push(FlushDone::CompactWrite(
                                        wj.dst, wj.src, wj.moves, r,
                                    ));
                                }),
                            );
                        }
                    }
                    // ⭐ G2 阶段 3 (同步): CAS 提交 (防回滚并发 COW 写)
                    FlushDone::CompactWrite(dst, src, moves, result) => {
                        if let Err(ref err) = result {
                            nlog::warn!("shard", "compact write failed (will retry): {err}");
                        }
                        e.pager_mut().complete_compact(dst, src, moves, result);
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
            // b3. ⭐ G2: 空闲段发起 chunk compact (低优先级协程读 dst+src 字节;
            // 判活用 header 候选 + meta 点查, 零全扫).
            // 触发条件: data backlog == 0; start_compact 内部节流 + 至多 1 个在飞.
            if e.pager_mut().flush_backlog() == 0
                && let Some(rj) = e.pager_mut().start_compact()
            {
                let done = flush_done.clone();
                scheduler::spawn_on_low(
                    rt,
                    Box::pin(async move {
                        // ⭐ B-drain: fresh dst (全新 bump chunk) 磁盘无内容,
                        // 跳过读直接传全零 (analyze 判 64 槽全死槽)
                        let dst_r = if rj.dst_fresh {
                            Ok(vec![0u8; storage::CHUNK_SIZE])
                        } else {
                            rj.io.read_page_chunk(&rj.dir, rj.dst).await
                        };
                        let r = match dst_r {
                            Ok(dst_bytes) => rj
                                .io
                                .read_page_chunk(&rj.dir, rj.src)
                                .await
                                .map(|src_bytes| (dst_bytes, src_bytes)),
                            Err(e) => Err(e),
                        };
                        done.borrow_mut().push(FlushDone::CompactRead(
                            rj.dst, rj.src, rj.dst_fresh, r,
                        ));
                    }),
                );
            }
            // c. 周期/计数刷盘 (内部守卫: 有 in-flight/pending 时自动推迟)
            let pf_start = std::time::Instant::now();
            let pf = block_on_io(e.pager_mut().maybe_periodic_flush());
            // ⭐ WAL (F60): 刷盘快照已入队 → 同轮内 seal 当前段 (无并发写间隙;
            // 段覆盖记录 ⊆ 快照内容, meta 全部落盘后删)
            if matches!(pf, Ok(true)) {
                e.wal_seal();
            }
            // ⭐ WAL (F60): periodic 档每 1s 落盘+fsync (丢失窗口 10s → ~1s)
            if let Err(err) = block_on_io(e.wal_periodic_tick()) {
                nlog::error!("shard", "WAL periodic sync failed: {err}");
            }
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
        let err = init_result.borrow().as_ref().unwrap().as_ref().err().map(|e| format!("{e:?}"));
        nlog::error!("shard", "shard-{shard_id} engine init failed: {err:?}, exiting");
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
                            // ⭐ T1: 惰性建表 (已存在 = registry 纯内存查表)
                            {
                                let (db, table, _) = op.locator();
                                if let Err(err) = block_on_io(e.ensure_table(db, table)) {
                                    results.push(BatchResult::Error(err.to_string()));
                                    continue;
                                }
                            }
                            let r = match op {
                                // ⭐ 事务批 (管理面 Batch 兼容臂; 热路径走 ShardTask)
                                BatchOp::TxnApply { ops, read_set } => exec_txn_apply(e, ops, read_set),
                                // ⭐ M3-2: 行数估计 (只读, 表不存在=0)
                                BatchOp::EstimateRowCount { db, table } => {
                                    BatchResult::RowCount(e.estimate_row_count(&db, &table).unwrap_or(0))
                                }
                                // ⭐ M3-4: distinct 估计 (只读)
                                BatchOp::EstimateDistinct { db, table, iids } => {
                                    BatchResult::DistinctCounts(
                                        iids.iter()
                                            .map(|iid| e.estimate_distinct(&db, &table, *iid).unwrap_or(0))
                                            .collect(),
                                    )
                                }
                                // ⭐ M3-5: min/max 估计 (只读)
                                BatchOp::EstimateRanges { db, table, iids } => {
                                    BatchResult::RangeBounds(
                                        iids.iter()
                                            .map(|iid| {
                                                e.estimate_range(&db, &table, *iid)
                                                    .map(|(lo, hi)| (Some(lo), Some(hi)))
                                                    .unwrap_or((None, None))
                                            })
                                            .collect(),
                                    )
                                }
                                // ⭐ F65: 占坑 op (管理面兼容; 热路径走 ShardTask → exec_task_op)
                                op @ (BatchOp::ReserveUnique { .. }
                                | BatchOp::StealUnique { .. }
                                | BatchOp::ConfirmUnique { .. }
                                | BatchOp::ReleaseUnique { .. }
                                | BatchOp::CatalogDump { .. }) => exec_task_op(e, op),
                                BatchOp::Put { db, table, key, val } => {
                                    match block_on_io(e.table_put(&db, &table, &key, &val)) {
                                        Ok(_) => BatchResult::PutOk,
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::Get { db, table, key } => {
                                    // ⭐ Phase H: 类型感知 (hash key → WRONGTYPE)
                                    match block_on_io(e.table_get_typed(&db, &table, &key)) {
                                        Ok(v) => BatchResult::GetValue(v),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::Delete { db, table, key } => {
                                    // ⭐ Phase H: 类型感知 (顺带清 hash 全部行/孤儿行)
                                    match block_on_io(e.key_delete_any(&db, &table, &key)) {
                                        Ok(b) => BatchResult::DeleteExisted(b),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::MultiGet { db, table, keys } => {
                                    let refs: Vec<&[u8]> =
                                        keys.iter().map(|k| k.as_slice()).collect();
                                    match block_on_io(e.table_get_many(&db, &table, &refs)) {
                                        Ok(vs) => BatchResult::Values(vs),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::MultiPut { db, table, pairs } => {
                                    match block_on_io(e.table_put_many(&db, &table, &pairs)) {
                                        Ok(_) => BatchResult::MultiPutOk,
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::MultiPutNx { db, table, pairs } => {
                                    exec_multiputnx(e, &db, &table, &pairs)
                                }
                                BatchOp::Incr { db, table, key, delta } => {
                                    exec_incr(e, &db, &table, &key, delta)
                                }
                                BatchOp::IncrFloat { db, table, key, delta } => {
                                    exec_incr_float(e, &db, &table, &key, delta)
                                }
                                BatchOp::Append { db, table, key, suffix } => {
                                    exec_append(e, &db, &table, &key, &suffix)
                                }
                                BatchOp::SetNx { db, table, key, val } => {
                                    exec_setnx(e, &db, &table, &key, &val)
                                }
                                BatchOp::GetDel { db, table, key } => {
                                    exec_getdel(e, &db, &table, &key)
                                }
                                BatchOp::GetSet { db, table, key, val } => {
                                    exec_getset(e, &db, &table, &key, &val)
                                }
                                BatchOp::SetRange { db, table, key, offset, data } => {
                                    exec_setrange(e, &db, &table, &key, offset, &data)
                                }
                                BatchOp::HSet { db, table, key, pairs } => {
                                    match block_on_io(e.hash_set(&db, &table, &key, &pairs)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HSetNx { db, table, key, field, val } => {
                                    match block_on_io(e.hash_set_nx(&db, &table, &key, &field, &val)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HGet { db, table, key, field } => {
                                    match block_on_io(e.hash_get(&db, &table, &key, &field)) {
                                        Ok(v) => BatchResult::GetValue(v),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HMGet { db, table, key, fields } => {
                                    match block_on_io(e.hash_get_many(&db, &table, &key, &fields)) {
                                        Ok(vs) => BatchResult::Values(vs),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HDel { db, table, key, fields } => {
                                    match block_on_io(e.hash_del(&db, &table, &key, &fields)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HLen { db, table, key } => {
                                    match block_on_io(e.hash_len(&db, &table, &key)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HGetAll { db, table, key } => {
                                    match block_on_io(e.hash_get_all(&db, &table, &key)) {
                                        Ok(ps) => BatchResult::Pairs(ps),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HIncrBy { db, table, key, field, delta } => {
                                    exec_hincrby(e, &db, &table, &key, &field, delta)
                                }
                                BatchOp::HIncrByFloat { db, table, key, field, delta } => {
                                    exec_hincrbyfloat(e, &db, &table, &key, &field, delta)
                                }
                                BatchOp::SAdd { db, table, key, members } => {
                                    match block_on_io(e.set_add(&db, &table, &key, &members)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SRem { db, table, key, members } => {
                                    match block_on_io(e.set_rem(&db, &table, &key, &members)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SIsMember { db, table, key, member } => {
                                    match block_on_io(e.set_is_member(&db, &table, &key, &member)) {
                                        Ok(b) => BatchResult::Integer(i64::from(b)),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SCard { db, table, key } => {
                                    match block_on_io(e.set_card(&db, &table, &key)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SMembers { db, table, key } => {
                                    match block_on_io(e.set_members(&db, &table, &key)) {
                                        Ok(ms) => BatchResult::Members(ms),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SPop { db, table, key } => exec_spop(e, &db, &table, &key),
                                BatchOp::SRandMember { db, table, key } => {
                                    match block_on_io(e.set_pick_one(&db, &table, &key)) {
                                        Ok(m) => BatchResult::Members(m.into_iter().collect()),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::LPush { db, table, key, values, left } => {
                                    match block_on_io(e.list_push(&db, &table, &key, &values, left)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::LPop { db, table, key, left, count } => {
                                    exec_lpop(e, &db, &table, &key, left, count as usize)
                                }
                                BatchOp::LLen { db, table, key } => {
                                    match block_on_io(e.list_len(&db, &table, &key)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::LRange { db, table, key, start, end } => {
                                    exec_lrange(e, &db, &table, &key, start, end)
                                }
                                BatchOp::LIndex { db, table, key, idx } => {
                                    match block_on_io(e.list_index(&db, &table, &key, idx)) {
                                        Ok(v) => BatchResult::GetValue(v),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::LSet { db, table, key, idx, val } => {
                                    exec_lset(e, &db, &table, &key, idx, &val)
                                }
                                BatchOp::ZAdd { db, table, key, pairs } => {
                                    match block_on_io(e.zset_add(&db, &table, &key, &pairs)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZRem { db, table, key, members } => {
                                    match block_on_io(e.zset_rem(&db, &table, &key, &members)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZScore { db, table, key, member } => {
                                    match block_on_io(e.zset_score(&db, &table, &key, &member)) {
                                        Ok(s) => BatchResult::OptMember(s.map(fmt_score)),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZCard { db, table, key } => {
                                    match block_on_io(e.zset_card(&db, &table, &key)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZIncrBy { db, table, key, delta, member } => {
                                    match block_on_io(e.zset_incr(&db, &table, &key, delta, &member)) {
                                        Ok(s) => BatchResult::Double(s),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZRange { db, table, key, start, end, rev, withscores } => {
                                    match block_on_io(e.zset_range(&db, &table, &key, start, end, rev)) {
                                        Ok(rows) => BatchResult::Members(zrows_to_members(rows, withscores)),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZRangeByScore { db, table, key, min, max, withscores } => {
                                    match block_on_io(e.zset_range_by_score(&db, &table, &key, min, max)) {
                                        Ok(rows) => BatchResult::Members(zrows_to_members(rows, withscores)),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZRank { db, table, key, member, rev } => {
                                    match block_on_io(e.zset_rank(&db, &table, &key, &member, rev)) {
                                        Ok(Some(r)) => BatchResult::Integer(r),
                                        Ok(None) => BatchResult::OptMember(None),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZCount { db, table, key, min, max } => {
                                    match block_on_io(e.zset_range_by_score(&db, &table, &key, min, max)) {
                                        Ok(rows) => BatchResult::Integer(rows.len() as i64),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZMScore { db, table, key, members } => {
                                    match block_on_io(e.zset_mscore(&db, &table, &key, &members)) {
                                        Ok(scores) => BatchResult::Values(
                                            scores.into_iter().map(|s| s.map(fmt_score)).collect(),
                                        ),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZPop { db, table, key, rev, count } => {
                                    match block_on_io(e.zset_pop(&db, &table, &key, rev, count as usize)) {
                                        Ok(rows) => BatchResult::Members(zrows_to_members(rows, true)),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SMisMember { db, table, key, members } => {
                                    match block_on_io(e.set_mismember(&db, &table, &key, &members)) {
                                        Ok(bs) => BatchResult::IntList(
                                            bs.into_iter().map(i64::from).collect(),
                                        ),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SPopN { db, table, key, count } => {
                                    match block_on_io(e.set_pop_n(&db, &table, &key, count as usize)) {
                                        Ok(ms) => BatchResult::Members(ms),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SRandCount { db, table, key, count } => {
                                    match block_on_io(e.set_rand_n(&db, &table, &key, count as usize)) {
                                        Ok(ms) => BatchResult::Members(ms),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HRandField { db, table, key, count, .. } => {
                                    match block_on_io(e.hash_rand(&db, &table, &key, count as usize)) {
                                        Ok(ps) => BatchResult::Pairs(ps),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::LRem { db, table, key, count, val } => {
                                    match block_on_io(e.list_rem(&db, &table, &key, count, &val)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::LTrim { db, table, key, start, stop } => {
                                    match block_on_io(e.list_trim(&db, &table, &key, start, stop)) {
                                        Ok(()) => BatchResult::Integer(1),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::LPos { db, table, key, val, rank, count } => {
                                    exec_lpos(e, &db, &table, &key, &val, rank, count)
                                }
                                BatchOp::LInsert { db, table, key, before, pivot, val } => {
                                    match block_on_io(e.list_insert(&db, &table, &key, before, &pivot, &val)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SetBit { db, table, key, offset, bit } => {
                                    exec_setbit(e, &db, &table, &key, offset, bit)
                                }
                                // ---- ⭐ Q5: SQL row 表 ----
                                BatchOp::RowPut { db, table, pk, values } => {
                                    exec_row_put(e, &db, &table, &pk, &values)
                                }
                                BatchOp::RowGet { db, table, pk } => {
                                    match block_on_io(e.row_get(&db, &table, &pk)) {
                                        Ok(v) => BatchResult::GetValue(v),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::RowDelete { db, table, pk } => {
                                    match block_on_io(e.row_delete(&db, &table, &pk)) {
                                        Ok(existed) => BatchResult::DeleteExisted(existed),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::RowUpdate { db, table, pk, sets } => {
                                    match block_on_io(e.row_update(&db, &table, &pk, &sets)) {
                                        Ok(updated) => BatchResult::DeleteExisted(updated),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::DropTableOp { db, table } => {
                                    match block_on_io(e.drop_table_sql(&db, &table)) {
                                        Ok(_) => BatchResult::PutOk,
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::TableScan { db, table, limit } => {
                                    exec_table_scan(e, &db, &table, limit)
                                }
                                BatchOp::ScanFiltered { db, table, preds, proj, index_hint, key_set_hint, limit } => {
                                    exec_scan_filtered(e, &db, &table, &preds, &proj, index_hint.as_ref(), key_set_hint.as_ref(), limit)
                                }
                                BatchOp::IndexScan { db, table, iid, lo, hi, limit, with_rows } => {
                                    exec_index_scan(
                                        e, &db, &table, iid, lo.as_ref(), hi.as_ref(), limit,
                                        with_rows,
                                    )
                                }
                                BatchOp::SetSchemaOp { db, table, bytes } => {
                                    exec_set_schema(e, &db, &table, &bytes)
                                }
                                BatchOp::GetSchemaOp { db, table } => {
                                    exec_get_schema(e, &db, &table)
                                }
                            };
                            results.push(r);
                        }
                        // ⭐ WAL (F60) strict: 回复前持久化屏障 (一个 Batch 多 op
                        // 天然共享一次 fsync); reply 到达 ⇒ 已落盘
                        if e.wal_mode() == storage::wal::WalMode::Strict
                            && e.wal_needs_sync()
                            && let Err(err) = block_on_io(e.wal_barrier())
                        {
                            nlog::error!("shard", "WAL barrier failed: {err}");
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
                // ⭐ WAL (F60) strict 组提交: 本轮有未 sync 写时回复押后,
                // 轮末一次 fsync 后统一 push (N 个写共享一次 fsync)
                let strict = e.wal_mode() == storage::wal::WalMode::Strict;
                let mut held: Vec<(u32, crate::request::TaskResult)> = Vec::new();
                for task in tasks {
                    // ⭐ T1: 惰性建表 (已存在 = registry 纯内存查表);
                    // ⭐ F66: CatalogDump 等无表名的元 op 跳过 (table 空)
                    {
                        let (db, table, _) = task.op.locator();
                        if !table.is_empty()
                            && let Err(err) = block_on_io(e.ensure_table(db, table))
                        {
                            reply_bus_set.get(task.worker_id).push(crate::request::TaskResult {
                                conn_id: task.conn_id,
                                req_id: task.req_id,
                                group: task.group,
                                result: crate::request::BatchResult::Error(err.to_string()),
                            });
                            continue;
                        }
                    }
                    let result = exec_task_op(e, task.op);
                    // ⭐ WAL (F60) strict: 本轮已有未持久化写 → 回复押到轮末
                    // barrier 后 (读 op 在无待 sync 内容时仍直发)
                    let tr = crate::request::TaskResult {
                        conn_id: task.conn_id,
                        req_id: task.req_id,
                        group: task.group,
                        result,
                    };
                    if strict && e.wal_needs_sync() {
                        held.push((task.worker_id, tr));
                    } else {
                        reply_bus_set.get(task.worker_id).push(tr);
                    }
                }
                if !held.is_empty() {
                    if let Err(err) = block_on_io(e.wal_barrier()) {
                        nlog::error!("shard", "WAL group-commit barrier failed: {err}");
                    }
                    for (wid, tr) in held {
                        reply_bus_set.get(wid).push(tr);
                    }
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
        | ShardRequest::ListDbsWithIds { reply }
        | ShardRequest::SetSchema { reply, .. }
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
            // ⭐ WAL (F60) strict: 同步慢路径也保证回复前持久化
            if e.wal_mode() == storage::wal::WalMode::Strict && e.wal_needs_sync() {
                let _ = block_on_io(e.wal_barrier());
            }
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
            if e.wal_mode() == storage::wal::WalMode::Strict && e.wal_needs_sync() {
                let _ = block_on_io(e.wal_barrier());
            }
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
            // ⭐ WAL (F60): DDL 不进 WAL (catalog 页写), 立即全量落盘保持久
            // (低频; 重放时表必存在)
            if r.is_ok() && e.wal_mode() != storage::wal::WalMode::Off {
                let _ = block_on_io(e.flush());
            }
            let _ = reply.send(match r {
                Ok(vpid) => Ok(ShardReply::CreateTableOk(vpid)),
                Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
            });
        }
        ShardRequest::CreateDb { db, .. } => {
            let r = block_on_io(e.create_db(&db));
            if r.is_ok() && e.wal_mode() != storage::wal::WalMode::Off {
                let _ = block_on_io(e.flush());
            }
            let _ = reply.send(match r {
                Ok(_) => Ok(ShardReply::CreateDbOk),
                Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
            });
        }
        ShardRequest::ListDbsWithIds { .. } => {
            // ⭐ D2 (分库): resolver (id, name) 全表 — DbDirView 初始化/刷新
            let _ = reply.send(Ok(ShardReply::DbList(e.list_dbs_with_ids())));
        }
        ShardRequest::SetSchema { db, table, bytes, .. } => {
            // ⭐ Q5: 反序列化校验后落 [$] 行 + 常驻镜像 (幂等)
            let r = storage::schema::TableSchema::decode(&bytes)
                .map_err(|err| ShardErrorKind::StorageError(err.to_string()))
                .and_then(|schema| {
                    block_on_io(e.set_schema(&db, &table, &schema))
                        .map_err(|err| ShardErrorKind::from_storage_display(&err))
                });
            let _ = reply.send(match r {
                Ok(()) => Ok(ShardReply::PutOk),
                Err(kind) => Err(kind),
            });
        }
        ShardRequest::PrepareCreateDb { db, .. } => {
            let r = block_on_io(e.create_db(&db));
            // ⭐ WAL (F60): DDL 不进 WAL → 立即落盘 (2PC 生产建库路径)
            if r.is_ok() && e.wal_mode() != storage::wal::WalMode::Off {
                let _ = block_on_io(e.flush());
            }
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
            // ⭐ WAL (F60): DDL 不进 WAL → 立即落盘 (2PC 生产建表路径)
            if r.is_ok() && e.wal_mode() != storage::wal::WalMode::Off {
                let _ = block_on_io(e.flush());
            }
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
