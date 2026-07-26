//! TwoPhaseCoordinator: 跨 shard 2PC 协调器状态机.
//!
//! ## 设计
//!
//! ```text
//! Coordinator                              Shard N
//!    │─── Prepare{op, txn_id} ──────────────→│
//!    │←────────── Ok/Err ────────────────────│
//!    │  (所有 shard Ok → Commit)
//!    │  (任一 shard Err → Abort)
//!    │─── Commit{op, txn_id} ───────────────→│
//!    │←────────── CommitOk ──────────────────│
//!    │  或
//!    │─── Abort{op, txn_id} ────────────────→│
//!    │←────────── AbortOk ───────────────────│
//! ```
//!
//! ## 状态机
//!
//! ```text
//!                 ┌──────────┐
//!                 │  Idle    │
//!                 └────┬─────┘
//!                      │ begin_txn
//!                      v
//!                 ┌──────────┐
//!                 │ Prepare  │ ←── on_prepare_ack (等待所有 shard ack)
//!                 └────┬─────┘
//!                      │
//!            ┌─────────┴──────────┐
//!            v                    v
//!     ┌──────────┐         ┌──────────┐
//!     │  Commit  │         │  Abort   │
//!     └────┬─────┘         └────┬─────┘
//!          │ on_commit_ack      │ on_abort_ack
//!          v                    v
//!     ┌──────────┐         ┌──────────┐
//!     │  Done    │         │  Done    │
//!     └──────────┘         └──────────┘
//! ```
//!
//! ## 线程安全
//!
//! TwoPhaseCoordinator 在 ShardManager 主线程使用 (单线程),
//! 不 Send/Sync, 不需要 Mutex.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::request::ShardId;

/// 事务 ID.
pub type TxnId = u64;

/// 2PC 操作类型.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TxnOp {
    /// 创建 db (在所有 shard).
    CreateDb(String),
    /// 创建表 (在所有 shard).
    CreateTable(String, String),
    /// 删除 db.
    DropDb(String),
    /// 删除表.
    DropTable(String, String),
}

impl TxnOp {
    /// 返回 reverse op (用于 Abort 回滚).
    pub fn reverse(&self) -> Option<Self> {
        match self {
            TxnOp::CreateDb(name) => Some(TxnOp::DropDb(name.clone())),
            TxnOp::CreateTable(db, table) => Some(TxnOp::DropTable(db.clone(), table.clone())),
            // DropDb/DropTable 不可逆 (留人工恢复).
            TxnOp::DropDb(_) | TxnOp::DropTable(_, _) => None,
        }
    }
}

/// 2PC 阶段.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnPhase {
    /// 正在 Prepare (等所有 shard ack).
    Prepare,
    /// 正在 Commit (等所有 shard commit ack).
    Commit,
    /// 正在 Abort (等所有 shard abort ack).
    Abort,
}

/// 2PC 事务结果.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnResult {
    /// 所有 shard 成功提交.
    Committed,
    /// 已回滚 (部分 shard prepare 失败).
    Aborted,
    /// 超时 (未在规定时间内完成).
    Timeout,
}

/// 待处理事务状态.
#[derive(Debug)]
pub struct PendingTxn {
    /// 事务 ID.
    pub id: TxnId,
    /// 操作类型.
    pub op: TxnOp,
    /// 当前阶段.
    pub phase: TxnPhase,
    /// 已 Prepare ack 的 shard 集合.
    pub prepare_acks: HashSet<ShardId>,
    /// 已 Prepare fail 的 shard 集合.
    pub prepare_fails: HashSet<ShardId>,
    /// 已 Commit ack 的 shard 集合.
    pub commit_acks: HashSet<ShardId>,
    /// 已 Abort ack 的 shard 集合.
    pub abort_acks: HashSet<ShardId>,
    /// 开始时间.
    pub started_at: Instant,
    /// 总 shard 数.
    pub total_shards: usize,
}

impl PendingTxn {
    fn new(id: TxnId, op: TxnOp, total_shards: usize) -> Self {
        Self {
            id,
            op,
            phase: TxnPhase::Prepare,
            prepare_acks: HashSet::new(),
            prepare_fails: HashSet::new(),
            commit_acks: HashSet::new(),
            abort_acks: HashSet::new(),
            started_at: Instant::now(),
            total_shards,
        }
    }

    /// 是否所有 shard 都已 Prepare ack.
    fn all_prepared(&self) -> bool {
        self.prepare_acks.len() + self.prepare_fails.len() == self.total_shards
    }

    /// 是否有 shard Prepare 失败.
    fn any_failed(&self) -> bool {
        !self.prepare_fails.is_empty()
    }

    /// 是否所有 shard 都已 Commit ack.
    fn all_committed(&self) -> bool {
        self.commit_acks.len() == self.total_shards
    }

    /// 是否所有 shard 都已 Abort ack.
    fn all_aborted(&self) -> bool {
        self.abort_acks.len() == self.total_shards
    }
}

/// TwoPhaseCoordinator: 2PC 协调器.
///
/// **使用方式**:
/// 1. `begin_txn(op)` → 获取 TxnId
/// 2. 对每个 shard 发送 Prepare 消息
/// 3. 收到每个 shard 的回复后调 `on_prepare_ack` / `on_prepare_fail`
/// 4. 当 `on_prepare_ack` 返回 `Commit` → 发 Commit 给所有 shard
///    当 `on_prepare_ack` 返回 `Abort` → 发 Abort 给所有 shard
/// 5. 收到 Commit/Abort 回复后调 `on_commit_ack` / `on_abort_ack`
///    当 `on_commit_ack` 返回 `true` → 事务完成
///
/// **非 Send/Sync**: 在 ShardManager 主线程单线程使用.
pub struct TwoPhaseCoordinator {
    /// 下一个事务 ID.
    next_txn_id: TxnId,
    /// 待处理事务.
    pending: HashMap<TxnId, PendingTxn>,
    /// 已完成事务历史 (保留最近 N 个, 用于调试).
    history: Vec<(TxnId, TxnResult)>,
    /// 历史保留上限.
    max_history: usize,
    /// prepare 超时时间.
    prepare_timeout: Duration,
    /// commit/abort 超时时间.
    finalize_timeout: Duration,
}

impl TwoPhaseCoordinator {
    /// 创建新协调器.
    pub fn new() -> Self {
        Self {
            next_txn_id: 1,
            pending: HashMap::new(),
            history: Vec::new(),
            max_history: 100,
            prepare_timeout: Duration::from_secs(10),
            finalize_timeout: Duration::from_secs(10),
        }
    }

    /// 开始一个新 2PC 事务. 返回 TxnId.
    ///
    /// **调用方责任**: 对每个 shard 发送 Prepare 消息.
    pub fn begin_txn(&mut self, op: TxnOp, total_shards: usize) -> TxnId {
        let id = self.next_txn_id;
        self.next_txn_id += 1;
        let txn = PendingTxn::new(id, op, total_shards);
        self.pending.insert(id, txn);
        id
    }

    /// 收到某个 shard 的 Prepare ack (成功).
    ///
    /// **返回**: `Some(TxnPhase::Commit)` 如果所有 shard 都 ack 且无失败 → 应发 Commit.
    ///          `Some(TxnPhase::Abort)` 如果所有 shard 都回复但存在失败 → 应发 Abort.
    ///          `None` 仍在等待更多 shard.
    pub fn on_prepare_ack(&mut self, txn_id: TxnId, shard_id: ShardId) -> Option<TxnPhase> {
        let txn = self.pending.get_mut(&txn_id)?;
        debug_assert_eq!(txn.phase, TxnPhase::Prepare);
        txn.prepare_acks.insert(shard_id);

        if txn.all_prepared() {
            if txn.any_failed() {
                txn.phase = TxnPhase::Abort;
                Some(TxnPhase::Abort)
            } else {
                txn.phase = TxnPhase::Commit;
                Some(TxnPhase::Commit)
            }
        } else {
            None
        }
    }

    /// 收到某个 shard 的 Prepare 失败.
    /// 同 `on_prepare_ack`, 但记录为失败.
    pub fn on_prepare_fail(&mut self, txn_id: TxnId, shard_id: ShardId) -> Option<TxnPhase> {
        let txn = self.pending.get_mut(&txn_id)?;
        debug_assert_eq!(txn.phase, TxnPhase::Prepare);
        txn.prepare_fails.insert(shard_id);

        if txn.all_prepared() {
            txn.phase = TxnPhase::Abort;
            Some(TxnPhase::Abort)
        } else {
            None
        }
    }

    /// 收到某个 shard 的 Commit ack.
    ///
    /// **返回**: `true` 表示所有 shard 都 Commit 完成, 事务结束.
    pub fn on_commit_ack(&mut self, txn_id: TxnId, shard_id: ShardId) -> bool {
        let txn = match self.pending.get_mut(&txn_id) {
            Some(t) => t,
            None => return false,
        };
        debug_assert_eq!(txn.phase, TxnPhase::Commit);
        txn.commit_acks.insert(shard_id);

        if txn.all_committed() {
            let _txn = self.pending.remove(&txn_id).unwrap();
            self.history.push((txn_id, TxnResult::Committed));
            if self.history.len() > self.max_history {
                self.history.remove(0);
            }
            true
        } else {
            false
        }
    }

    /// 收到某个 shard 的 Abort ack.
    ///
    /// **返回**: `true` 表示所有 shard 都 Abort 完成, 事务结束.
    pub fn on_abort_ack(&mut self, txn_id: TxnId, shard_id: ShardId) -> bool {
        let txn = match self.pending.get_mut(&txn_id) {
            Some(t) => t,
            None => return false,
        };
        debug_assert_eq!(txn.phase, TxnPhase::Abort);
        txn.abort_acks.insert(shard_id);

        if txn.all_aborted() {
            let _txn = self.pending.remove(&txn_id).unwrap();
            self.history.push((txn_id, TxnResult::Aborted));
            if self.history.len() > self.max_history {
                self.history.remove(0);
            }
            true
        } else {
            false
        }
    }

    /// 检查是否有超时的事务.
    ///
    /// **返回**: 超时的事务 ID 列表.
    pub fn check_timeouts(&mut self) -> Vec<TxnId> {
        let mut timed_out = Vec::new();
        let now = Instant::now();
        let ids: Vec<TxnId> = self.pending.keys().copied().collect();
        for id in ids {
            let txn = match self.pending.get(&id) {
                Some(t) => t,
                None => continue,
            };
            let timeout = match txn.phase {
                TxnPhase::Prepare => self.prepare_timeout,
                TxnPhase::Commit | TxnPhase::Abort => self.finalize_timeout,
            };
            if now - txn.started_at > timeout {
                timed_out.push(id);
            }
        }
        for id in &timed_out {
            if let Some(_txn) = self.pending.remove(id) {
                self.history.push((*id, TxnResult::Timeout));
                if self.history.len() > self.max_history {
                    self.history.remove(0);
                }
            }
        }
        timed_out
    }

    /// 查询待处理事务.
    pub fn get_pending(&self, txn_id: TxnId) -> Option<&PendingTxn> {
        self.pending.get(&txn_id)
    }

    /// 当前是否有待处理事务.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// 待处理事务数.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// 获取已完成事务历史.
    pub fn history(&self) -> &[(TxnId, TxnResult)] {
        &self.history
    }

    /// 设置 prepare 超时.
    pub fn set_prepare_timeout(&mut self, timeout: Duration) {
        self.prepare_timeout = timeout;
    }

    /// 设置 finalize 超时.
    pub fn set_finalize_timeout(&mut self, timeout: Duration) {
        self.finalize_timeout = timeout;
    }
}

impl Default for TwoPhaseCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_txn_assigns_increasing_ids() {
        let mut coord = TwoPhaseCoordinator::new();
        let id1 = coord.begin_txn(TxnOp::CreateDb("a".into()), 3);
        let id2 = coord.begin_txn(TxnOp::CreateDb("b".into()), 3);
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(coord.pending_count(), 2);
    }

    #[test]
    fn all_prepare_acks_triggers_commit() {
        let mut coord = TwoPhaseCoordinator::new();
        let txn_id = coord.begin_txn(TxnOp::CreateDb("test".into()), 2);

        // shard 0 ack — 还在等 shard 1
        let r = coord.on_prepare_ack(txn_id, 0);
        assert_eq!(r, None, "shard 1 还没回复");

        // shard 1 ack — 全部完成, 应 Commit
        let r = coord.on_prepare_ack(txn_id, 1);
        assert_eq!(r, Some(TxnPhase::Commit), "全部 ack 应 Commit");
    }

    #[test]
    fn prepare_fail_triggers_abort() {
        let mut coord = TwoPhaseCoordinator::new();
        let txn_id = coord.begin_txn(TxnOp::CreateDb("test".into()), 2);

        // shard 0 ok
        let _ = coord.on_prepare_ack(txn_id, 0);

        // shard 1 fail
        let r = coord.on_prepare_fail(txn_id, 1);
        assert_eq!(r, Some(TxnPhase::Abort), "有 shard 失败应 Abort");
    }

    #[test]
    fn commit_acks_complete_txn() {
        let mut coord = TwoPhaseCoordinator::new();
        let txn_id = coord.begin_txn(TxnOp::CreateDb("test".into()), 2);

        // 所有 prepare ack → Commit
        let _ = coord.on_prepare_ack(txn_id, 0);
        let phase = coord.on_prepare_ack(txn_id, 1);
        assert_eq!(phase, Some(TxnPhase::Commit));

        // commit ack
        let done = coord.on_commit_ack(txn_id, 0);
        assert!(!done, "shard 1 还没 commit ack");
        let done = coord.on_commit_ack(txn_id, 1);
        assert!(done, "全部 commit ack → 事务完成");

        // 事务应从 pending 移除
        assert!(coord.get_pending(txn_id).is_none());
        assert_eq!(coord.history().len(), 1);
        assert_eq!(coord.history()[0].1, TxnResult::Committed);
    }

    #[test]
    fn abort_acks_complete_txn() {
        let mut coord = TwoPhaseCoordinator::new();
        let txn_id = coord.begin_txn(TxnOp::CreateDb("test".into()), 2);

        // shard 0 ok, shard 1 fail → Abort
        let _ = coord.on_prepare_ack(txn_id, 0);
        let phase = coord.on_prepare_fail(txn_id, 1);
        assert_eq!(phase, Some(TxnPhase::Abort));

        // abort ack
        let done = coord.on_abort_ack(txn_id, 0);
        assert!(!done, "shard 1 还没 abort ack");
        let done = coord.on_abort_ack(txn_id, 1);
        assert!(done, "全部 abort ack → 事务完成");

        assert_eq!(coord.history()[0].1, TxnResult::Aborted);
    }

    #[test]
    fn check_timeouts_removes_stale_txns() {
        let mut coord = TwoPhaseCoordinator::new();
        coord.set_prepare_timeout(Duration::from_millis(1));
        let txn_id = coord.begin_txn(TxnOp::CreateDb("test".into()), 2);

        // 等 2ms 确保超时
        std::thread::sleep(Duration::from_millis(2));

        let timed_out = coord.check_timeouts();
        assert_eq!(timed_out, vec![txn_id], "txn 应超时");
        assert!(coord.get_pending(txn_id).is_none());
        assert_eq!(coord.history()[0].1, TxnResult::Timeout);
    }

    #[test]
    fn reverse_op_create_db_returns_drop_db() {
        let op = TxnOp::CreateDb("app".into());
        assert_eq!(op.reverse(), Some(TxnOp::DropDb("app".into())));
    }

    #[test]
    fn reverse_op_create_table_returns_drop_table() {
        let op = TxnOp::CreateTable("db".into(), "users".into());
        assert_eq!(
            op.reverse(),
            Some(TxnOp::DropTable("db".into(), "users".into()))
        );
    }

    #[test]
    fn reverse_op_drop_db_returns_none() {
        let op = TxnOp::DropDb("app".into());
        assert_eq!(op.reverse(), None);
    }

    #[test]
    fn reverse_op_drop_table_returns_none() {
        let op = TxnOp::DropTable("db".into(), "users".into());
        assert_eq!(op.reverse(), None);
    }
}
