//! ShardManager 错误类型.

use thiserror::Error;

use crate::request::ShardErrorKind;

/// ShardManager 公共错误.
#[derive(Debug, Error)]
pub enum ShardError {
    /// db 不存在.
    #[error("db not found: {0}")]
    DbNotFound(String),
    /// table 不存在.
    #[error("table not found: {db}.{table}")]
    TableNotFound { db: String, table: String },
    /// Storage 内部错误.
    #[error("storage error: {0}")]
    StorageError(String),
    /// channel 通信失败 (shard 线程可能已退出).
    #[error("shard channel closed")]
    ChannelClosed,
    /// Shard 线程 join 时 panic.
    #[error("shard thread panicked: shard={0}")]
    JoinPanic(usize),
    /// IO 错误.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    // =================================================================
    // ⭐ T14: 2PC 协议错误
    // =================================================================
    /// 2PC Prepare 阶段失败.
    #[error("2PC prepare failed: {op} on shard {shard_id}: {reason}")]
    PrepareFailed {
        /// 操作描述.
        op: String,
        /// 失败的 shard.
        shard_id: usize,
        /// 失败原因.
        reason: String,
    },
    /// 2PC Commit 阶段失败.
    #[error("2PC commit failed: {op} on shard {shard_id}: {reason}")]
    CommitFailed {
        /// 操作描述.
        op: String,
        /// 失败的 shard.
        shard_id: usize,
        /// 失败原因.
        reason: String,
    },
    /// 2PC Abort 阶段失败.
    #[error("2PC abort failed: {op} on shard {shard_id}: {reason}")]
    AbortFailed {
        /// 操作描述.
        op: String,
        /// 失败的 shard.
        shard_id: usize,
        /// 失败原因.
        reason: String,
    },
    /// 2PC 超时.
    #[error("2PC timeout: {op}")]
    TwoPcTimeout {
        /// 超时的操作.
        op: String,
    },
}

impl ShardError {
    pub fn from_kind(kind: ShardErrorKind) -> Self {
        match kind {
            ShardErrorKind::DbNotFound => ShardError::DbNotFound("<unknown>".into()),
            ShardErrorKind::TableNotFound => ShardError::TableNotFound {
                db: "<unknown>".into(),
                table: "<unknown>".into(),
            },
            ShardErrorKind::StorageError(s) => ShardError::StorageError(s),
            ShardErrorKind::ChannelClosed => ShardError::ChannelClosed,
            ShardErrorKind::JoinPanic => ShardError::JoinPanic(0),
        }
    }
}

/// ShardManager Result 类型别名.
pub type ShardResult<T> = Result<T, ShardError>;
