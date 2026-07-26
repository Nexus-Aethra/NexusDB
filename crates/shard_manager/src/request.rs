//! ShardRequest / ShardResponse enum: 跨线程消息.
//!
//! ## 2PC 协议 (T14)
//!
//! 跨 shard 元数据操作 (create_db / create_table) 走两阶段提交:
//!
//! ```text
//! Coordinator                              Shard N
//!    │─── Prepare{op, txn_id} ──────────────→│
//!    │←────────── Ack/Err ───────────────────│
//!    │  (所有 shard ack → Commit)
//!    │  (任一 shard err → Abort)
//!    │─── Commit{op, txn_id} ───────────────→│
//!    │←────────── CommitOk ──────────────────│
//!    │  或
//!    │─── Abort{op, txn_id} ────────────────→│
//!    │←────────── AbortOk ───────────────────│
//! ```
//!
//! **Prepare 语义**: 尝试执行操作 (create_db / create_table).
//! 成功后操作已落盘, 但标记为 "pending" (可见性由 Commit 控制).
//! **Commit 语义**: 将 pending 操作标记为已提交 (可见).
//! **Abort 语义**: 回滚 Prepare 阶段的操作 (drop_db / drop_table).
//!
//! **MVP 简化**: Prepare 直接执行操作 (不区分 pending/committed),
//! Commit 是 no-op, Abort 执行 reverse op. 这仍然提供 all-or-nothing 保证:
//! - 全部 Prepare 成功 → Commit (no-op, 操作已生效)
//! - 任一 Prepare 失败 → Abort 所有已成功的 shard (回滚)
//!
//! ## T15 async API
//!
//! `reply: ReplySender` 替代原来的 `SyncSender<ShardResponse>`.
//! 调用方拿到 `PendingReply::new() -> (ReplySender, ReplyFuture)`,
//! 把 `ReplySender` 塞进 `ShardRequest` 发给 shard, 持有 `ReplyFuture` 等.
//! shard 端 `reply.send(...)` 会自动 wake 调用方.
//!
//! ## T19 async network stack
//!
//! 每个 Put/Get/Delete 加 `req_id: u64` 字段 (默认 0 表示"网络层未启用,
//! 走 channel reply"). 当 `req_id > 0` 且 `ShardManager` 启用了 `reply_bus`
//! 时, shard 端会**同时**写入 reply_bus (用于 worker 异步路由), 原有
//! `reply.send(...)` 仍然调用, 但**不阻塞**任何线程.

use crate::reply::ReplySender;

/// Shard ID = `[0, num_shards)`.
pub type ShardId = usize;

/// 全局唯一事务 ID (单调递增).
pub type TxnId = u64;

/// 单条请求: ShardManager 主线程 → shard 线程.
///
/// **T15 更新**: reply 是 `ReplySender` (waker-based), 替代 `SyncSender` 阻塞 reply.
///   这样 ShardManager 异步 API 不会阻塞调用线程 (适合 Tokio/Axum 集成).
pub enum ShardRequest {
    /// 插入 / 更新 KV.
    Put {
        db: String,
        table: String,
        key: Vec<u8>,
        val: Vec<u8>,
        /// 网络层 req_id (默认 0 表示旧行为: 仅 channel reply).
        /// 当 `> 0` 且 `ShardManager::enable_reply_bus` 被调用过,
        /// shard 完成后会同时 push 一份到 reply_bus.
        req_id: u64,
        reply: ReplySender,
    },
    /// 点查.
    Get {
        db: String,
        table: String,
        key: Vec<u8>,
        req_id: u64,
        reply: ReplySender,
    },
    /// 删除.
    Delete {
        db: String,
        table: String,
        key: Vec<u8>,
        req_id: u64,
        reply: ReplySender,
    },
    /// 在本 shard 创建表 (单 shard 操作, 不跨 shard).
    CreateTable {
        db: String,
        table: String,
        reply: ReplySender,
    },
    /// 在本 shard 创建 db (MVP: 单 shard, 未来 T14 改 2PC 跨 shard).
    CreateDb { db: String, reply: ReplySender },
    // =================================================================
    // ⭐ T14: 2PC 协议消息
    // =================================================================
    /// 2PC Prepare: 准备创建 db (尝试执行, 失败可回滚).
    PrepareCreateDb {
        db: String,
        txn_id: TxnId,
        reply: ReplySender,
    },
    /// 2PC Commit: 确认创建 db (no-op, Prepare 已生效).
    CommitCreateDb {
        db: String,
        txn_id: TxnId,
        reply: ReplySender,
    },
    /// 2PC Abort: 回滚创建 db (drop_db).
    AbortCreateDb {
        db: String,
        txn_id: TxnId,
        reply: ReplySender,
    },

    /// 2PC Prepare: 准备创建表.
    PrepareCreateTable {
        db: String,
        table: String,
        txn_id: TxnId,
        reply: ReplySender,
    },
    /// 2PC Commit: 确认创建表 (no-op, Prepare 已生效).
    CommitCreateTable {
        db: String,
        table: String,
        txn_id: TxnId,
        reply: ReplySender,
    },
    /// 2PC Abort: 回滚创建表 (drop_table).
    AbortCreateTable {
        db: String,
        table: String,
        txn_id: TxnId,
        reply: ReplySender,
    },

    /// 关闭 shard (Shutting down 流程).
    Shutdown { reply: ReplySender },
    /// ⭐ Flush: 把所有 dirty nowchunks 落盘并插入 chunk_list.
    /// 后置: 所有写入数据 durability = disk, chunk_list 命中.
    Flush { reply: ReplySender },
    /// ⭐ 批量操作: 多个 ops 一次性提交, 一次性回复.
    Batch {
        ops: Vec<BatchOp>,
        req_id: u64,
        reply: ReplySender,
    },
}

/// 单个 batch 操作 (不带 reply, batch 整体回复).
#[derive(Debug, Clone)]
pub enum BatchOp {
    Put { db: String, table: String, key: Vec<u8>, val: Vec<u8> },
    Get { db: String, table: String, key: Vec<u8> },
    Delete { db: String, table: String, key: Vec<u8> },
}

/// 单个 batch 操作的结果.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchResult {
    PutOk,
    GetValue(Option<Vec<u8>>),
    DeleteExisted(bool),
    Error(String),
}

// =====================================================================
// ⭐ 独立服务架构: ShardTask / TaskResult (Phase 1)
// =====================================================================

/// 网络层 → shard 的任务单元.
/// 由 worker 解析协议后构造, push 到 shard task queue.
#[derive(Debug)]
pub struct ShardTask {
    /// 来源连接 ID (用于回复路由).
    pub conn_id: u64,
    /// 请求 ID (支持 pipeline/多路复用).
    pub req_id: u64,
    /// worker ID (用于确定 reply 回哪个 worker 的 bus).
    pub worker_id: u32,
    /// 具体操作.
    pub op: BatchOp,
}

/// shard 执行完成后的结果, 写入 TaskReplyBus.
#[derive(Debug, Clone)]
pub struct TaskResult {
    /// 来源连接 ID.
    pub conn_id: u64,
    /// 请求 ID.
    pub req_id: u64,
    /// 执行结果.
    pub result: BatchResult,
}

/// shard 处理完发回的结果.
///
/// `ShardResponse` = `Result<ShardReply, ShardErrorKind>`.
pub type ShardResponse = Result<ShardReply, ShardErrorKind>;

/// 成功的回复: 不同操作返回不同类型, 用 enum 统一.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardReply {
    /// Put 完成 (无返回值).
    PutOk,
    /// Get 返回值.
    GetValue(Option<Vec<u8>>),
    /// Delete 返回是否存在.
    DeleteExisted(bool),
    /// CreateTable 返回 table root vpid.
    CreateTableOk(u64),
    /// CreateDb 完成.
    CreateDbOk,
    /// 2PC Prepare 阶段成功 (操作已准备, 等待 Commit/Abort).
    PrepareOk,
    /// 2PC Commit 阶段成功.
    CommitOk,
    /// 2PC Abort 阶段成功.
    AbortOk,
    /// Shutdown 完成.
    ShutdownOk,
    /// Flush 完成 (所有 dirty data 已落盘).
    FlushOk,
    /// Batch 结果: 与 ops 一一对应.
    BatchResults(Vec<BatchResult>),
}

/// 错误类型. 暂时简化, 后面按 storage 错误细分.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardErrorKind {
    /// db 不存在.
    DbNotFound,
    /// table 不存在.
    TableNotFound,
    /// Storage 内部错误.
    StorageError(String),
    /// channel 关闭 (sender drop / receiver drop).
    ChannelClosed,
    /// Shard 线程 join 时 panic.
    JoinPanic,
}

impl ShardErrorKind {
    pub fn from_storage_display(err: &dyn std::fmt::Display) -> Self {
        let s = format!("{err}");
        if s.contains("DbNotFound") {
            ShardErrorKind::DbNotFound
        } else if s.contains("TableNotFound") {
            ShardErrorKind::TableNotFound
        } else {
            ShardErrorKind::StorageError(s)
        }
    }
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_error_kind_from_storage() {
        let err = std::io::Error::other("DbNotFound");
        let kind = ShardErrorKind::from_storage_display(&err);
        assert!(matches!(kind, ShardErrorKind::DbNotFound));
    }
}
