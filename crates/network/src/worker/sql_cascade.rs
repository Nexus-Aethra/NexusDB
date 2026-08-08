//! ⭐ PG 兼容 (FMT_VER 8): 外键级联删除编排 (worker 侧).
//!
//! 设计: 主 DELETE 完成 (DmlAgg remaining==0) 后, 按进程级反向引用
//! (`SqlSharedRoutes::incoming_fks`) 查"谁引用了被删表", 对每个引用表
//! 递归下发改写后的 `DELETE WHERE fk_col IN (被删 pks)` / `UPDATE ... SET
//! fk_col=NULL` 子任务 (伪高位 seq, 回包被 `resp_complete` 拦截不发给客户端);
//! 所有子任务完成 (级联根 active==0) 才回复根 DELETE.
//!
//! v1 边界: visited 防环 (自引用/菱形引用); 跨 shard 引用行全广播删;
//! 引用列类型与父 pk 类型不一致时 IN 匹配退化为字节比较.

use crate::protocol::sql::{CmpOp, Cond, Pred, SqlStmt, SqlValue};
use storage::row::ColValue;
use storage::schema::{ColType, FkAction};
use shard_manager::{DbDirView, SharedTaskInbox};
use super::{col_from_ordered_bytes, ConnState, sql_dispatch_stmt};

/// 级联子任务 (伪 seq → 完成时推进级联).
#[derive(Debug, Clone)]
pub struct CascadeJob {
    pub root_seq: u64,
    pub action: FkAction,
}

/// 级联根状态 (主 DELETE seq → 计数/失败/防环).
#[derive(Debug)]
pub struct CascadeRoot {
    pub db: std::sync::Arc<str>,
    pub active: usize,
    pub affected: u64,
    pub failed: Option<String>,
    pub visited: std::collections::HashSet<(String, Vec<u8>)>,
}

/// 级联 job 伪 seq 高位标志 (客户端 seq 为递增小整数, 高位恒 0).
pub fn is_cascade_seq(seq: u64) -> bool {
    (seq >> 63) == 1
}

/// 主 DELETE 完成 → 检查是否有引用表需级联. 返回 true = 进入级联 (根回复延迟).
#[allow(clippy::too_many_arguments)]
pub fn cascade_kickoff(
    conn: &mut ConnState,
    conn_id: u64,
    root_seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    table: &str,
    pks: Vec<Vec<u8>>,
    affected: u64,
) -> bool {
    let refs = conn.sql_shared.incoming_fks(db, table);
    if refs.is_empty() {
        return false;
    }
    let mut visited = std::collections::HashSet::with_capacity(pks.len());
    for pk in &pks {
        visited.insert((table.to_string(), pk.clone()));
    }
    conn.cascade_roots.insert(
        root_seq,
        CascadeRoot {
            db: (*db).clone(),
            active: 0,
            affected,
            failed: None,
            visited,
        },
    );
    for r in refs {
        spawn_job(
            conn, conn_id, root_seq, worker_id, db, default_db, db_view,
            shard_inboxes, num_shards, table, &r, &pks,
        );
    }
    true
}

/// 级联子任务完成 (DmlAgg remaining==0 / Fire 错误 / 空 pk) → 推进级联.
#[allow(clippy::too_many_arguments)]
pub fn cascade_job_done(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    _affected: u64,
    error: Option<String>,
    job: &CascadeJob,
) {
    // 根状态计数 (借用局部化); db 从根状态取 (同库外键)
    let root_seq = job.root_seq;
    let db = conn
        .cascade_roots
        .get(&root_seq)
        .map(|r| r.db.clone())
        .unwrap_or_default();
    {
        let root = conn
            .cascade_roots
            .get_mut(&root_seq)
            .expect("cascade root 必存在");
        root.active = root.active.saturating_sub(1);
        if let Some(e) = error {
            if root.failed.is_none() {
                root.failed = Some(e);
            }
        }
    }
    // 递归: Cascade 子任务删了引用表行 → 其被删 pk 的更深引用
    // (递归 spawn 会 active += 1 — 必须在最终计数判断前完成)
    if job.action == FkAction::Cascade {
        if let Some((_jdb, ref_table, ref_pks)) = conn.cascade_pending.remove(&seq) {
            let deeper = conn.sql_shared.incoming_fks(&db, &ref_table);
            for r in deeper {
                spawn_job(
                    conn, conn_id, root_seq, worker_id, &db, default_db, db_view,
                    shard_inboxes, num_shards, &ref_table, &r, &ref_pks,
                );
            }
        }
    }
    // 全部级联完成 (递归后实时读 active) → 回复根 DELETE
    let all_done = conn
        .cascade_roots
        .get(&root_seq)
        .map(|r| r.active == 0)
        .unwrap_or(false);
    if all_done {
        let root = conn.cascade_roots.remove(&root_seq).expect("root 必在");
        let bytes = match root.failed {
            Some(e) => super::sql_err_bytes(conn.proto, &e),
            None => super::sql_ok_bytes(conn.proto, root.affected),
        };
        conn.resp_complete(root_seq, bytes);
    }
}

/// 下发一个级联子任务 (伪高位 seq, 递归 `sql_dispatch_stmt`).
#[allow(clippy::too_many_arguments)]
fn spawn_job(
    conn: &mut ConnState,
    conn_id: u64,
    root_seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    parent_table: &str,
    incoming: &super::FkIncoming,
    pks: &[Vec<u8>],
) {
    // visited 去重仅对 Cascade (删行需防环: 自引用/菱形引用同 pk 只删一次).
    // SetNull 更新引用行不删行 → 不过滤 (父章被多引用时每引用各置空一次).
    let fresh: Vec<Vec<u8>> = if incoming.action == FkAction::Cascade {
        let mut f = Vec::with_capacity(pks.len());
        let root = conn
            .cascade_roots
            .get_mut(&root_seq)
            .expect("root 必在");
        for pk in pks {
            if root.visited.insert((incoming.table.clone(), pk.clone())) {
                f.push(pk.clone());
            }
        }
        if f.is_empty() {
            return;
        }
        f
    } else {
        pks.to_vec()
    };
    // 父表 pk 类型 → 被删 pk 编码转 SqlValue (IN 匹配)
    let pk_ty = parent_pk_ty(conn, db, parent_table);
    let sql_vals: Vec<SqlValue> = fresh.iter().map(|e| pk_enc_to_sql(pk_ty, e)).collect();
    let cond = Pred::Leaf(Cond {
        col: incoming.col.clone(),
        op: CmpOp::In,
        val: SqlValue::Null,
        set: sql_vals,
    });
    let stmt = match incoming.action {
        FkAction::Cascade => SqlStmt::Delete {
            table: incoming.table.clone(),
            conds: cond,
        },
        FkAction::SetNull => SqlStmt::Update {
            table: incoming.table.clone(),
            sets: vec![(incoming.col.clone(), SqlValue::Null)],
            conds: cond,
        },
        FkAction::NoAction => return,
    };
    let job_seq = (1u64 << 63) | conn.cascade_seq_ctr;
    conn.cascade_seq_ctr = conn.cascade_seq_ctr.wrapping_add(1);
    conn.cascade_jobs
        .insert(job_seq, CascadeJob { root_seq, action: incoming.action });
    if let Some(root) = conn.cascade_roots.get_mut(&root_seq) {
        root.active += 1;
    }
    // 递归执行 (pk 等值/两阶段/聚合 全走正常路径; 完成在 DmlAgg 拦截点推进)
    sql_dispatch_stmt(
        conn, conn_id, job_seq, worker_id, db, default_db, db_view, shard_inboxes,
        num_shards, stmt,
    );
}

/// 父表 (当前被删的表) 主键列类型 — 用于把 pk 编码转 SqlValue.
fn parent_pk_ty(conn: &ConnState, db: &std::sync::Arc<str>, parent_table: &str) -> ColType {
    conn.sql_cache
        .borrow()
        .schemas
        .get(&(db.to_string(), parent_table.to_string()))
        .map(|s| s.columns[s.pk_col as usize].ty)
        .unwrap_or(ColType::Bytes)
}

/// 被删 pk 编码 (col 保序编码) → SqlValue (IN set 用; eval 时按引用列类型 coerce).
fn pk_enc_to_sql(ty: ColType, enc: &[u8]) -> SqlValue {
    match col_from_ordered_bytes(ty, enc) {
        Some(ColValue::I64(x)) => SqlValue::Int(x),
        Some(ColValue::F64(x)) => SqlValue::Float(x),
        Some(ColValue::Bytes(b)) => SqlValue::Str(b),
        Some(ColValue::Decimal(x, _)) => SqlValue::Str(format!("{x}").into_bytes()),
        _ => SqlValue::Null,
    }
}
