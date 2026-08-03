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
use crate::exec_cmds::*;
use crate::reply::{block_on_v2, PendingReply};
use crate::request::{BatchOp, BatchResult, ShardErrorKind, ShardId, ShardReply, ShardRequest, ShardResponse};
use crate::router::{HashRouter, Router};
use crate::shard_thread::shard_thread_main;

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
pub(crate) fn block_on_io<F: std::future::Future>(fut: F) -> F::Output {
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
pub(crate) enum FlushDone {
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
pub(crate) type FlushDoneSlot = std::rc::Rc<std::cell::RefCell<Vec<FlushDone>>>;

