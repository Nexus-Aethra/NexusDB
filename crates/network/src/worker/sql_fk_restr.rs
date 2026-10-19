//! ⭐ PG 兼容 (引用完整性, 审计 P1-4): 父表 DELETE 前置 RESTRICT 预检 (worker 侧).
//!
//! 背景: 外键检查原本只覆盖 INSERT (sql_fk.rs). 父表 DELETE 时, 非 CASCADE
//! 引用 (默认 RESTRICT/NO ACTION) 无任何检查 — 修复前实测两种坏行为:
//! ① 同进程建表后删父行: cascade_kickoff 对 NoAction 引用空转, 根 DELETE
//!    回复被永久延迟 → 客户端挂死; ② 重启后 (incoming_fks 空): 静默删父行
//!    留孤儿. (UPDATE 无需覆盖: PK 列不可更新 — sql_dml.rs 硬拒, 子行引用
//!    的是 pk 值, 非 pk 列更新不影响引用.)
//!
//! 设计: 镜像 INSERT 预检 (sql_fk.rs) 的延迟写模式 —
//!   1. DELETE 求出被删 pks 后 (PkGet 路径即时 / 两阶段路径 phase1 收齐后),
//!      查进程级反向索引 incoming_fks, 过滤 NoAction 引用;
//!   2. 对每个引用表广播 ScanFiltered{fk_col IN (父 pk 值), limit=1} 探针
//!      (group = 引用表序号, 回包据此归属);
//!   3. 任一 shard 回 ≥1 行 → 报错 (父子行均不动); 全净 → 放行原 DELETE.
//!
//! 健壮性 (拒绝静默跳过 — 审计"静默降级"原则):
//! - 子表 schema 未缓存 (多 worker / 重启后首次) → GetSchemaOp 挂起加载后重试;
//! - 本 worker 未 catalog 补注过该 db → CatalogDump 一次性补注全部 FK 后重试
//!   (incoming_fks 是进程级注册表, 重启后为空 — 与 CASCADE 同源同补).

use std::sync::Arc;

use super::{
    ConnState, SqlDmlAction, SqlError, SqlErrorCode, col_from_ordered_bytes, push_task,
    push_task_grouped, run_dml_phase2, sql_dispatch_stmt,
};
use crate::protocol::sql::SqlStmt;
use shard_manager::{BatchOp, BatchResult, PredOp, ScanPred, SharedTaskInbox};
use storage::row::ColValue;
use storage::schema::{ColType, FkAction, TableSchema};

/// 单语句 RESTRICT 引用表数上限 (护栏: 防广播风暴).
pub(crate) const FK_RESTRICT_MAX_REFS: usize = 64;
/// 单语句被删 pk 数上限 (对齐 SUBQ_IN_MAX; 超出提示分批删).
pub(crate) const FK_RESTRICT_MAX_PKS: usize = 65536;

/// 预检恢复方式 (检查通过后如何继续原 DELETE).
pub(crate) enum RestrResume {
    /// 重放原 DELETE 语句 (PkGet 路径; 二次进入时 cleared 放行).
    /// Box: SqlStmt 是最大 variant, 不装箱会让 RestrStage 体积膨胀.
    Redispatch(Box<SqlStmt>),
    /// 直接续两阶段 phase2 (pks 已收齐, 无需重扫).
    Phase2 {
        pks: Vec<Vec<u8>>,
        action: SqlDmlAction,
        target: (Arc<str>, String),
    },
}

/// 预检状态机阶段.
enum RestrStage {
    /// 正在拉子表 schema (GetSchemaOp 挂起; child = 子表名).
    LoadingSchema { child: String },
    /// 正在补注全库 FK (CatalogDump 挂起).
    LoadingCatalog,
    /// 探针广播中: remaining = 待回包数; refs = group 序号 → (子表, 引用列).
    Probing {
        remaining: usize,
        refs: Vec<(String, String)>,
        violated: Option<(String, String)>,
        error: Option<String>,
    },
}

/// on_reply 的 stage 判别快照 (避免 match 期间持有 conn 借用).
enum StageKind {
    Catalog,
    Schema(String),
    Probing,
}

/// RESTRICT 预检状态 (seq →).
pub(crate) struct FkRestrSt {
    /// 语句所属 db (回包路由处只有 default_db, 存一份准的).
    db: std::sync::Arc<str>,
    resume: RestrResume,
    stage: RestrStage,
    /// 已通过检查 (重入放行标记, 防重复预检).
    cleared: bool,
}

/// 父表 schema 的 pk 列类型 (探针值编码用).
fn parent_pk_ty(conn: &ConnState, db: &str, table: &str) -> Option<ColType> {
    conn.sql_cache
        .borrow()
        .schemas
        .get(&(db.to_string(), table.to_string()))
        .map(|s| s.columns[s.pk_col as usize].ty)
}

/// 尝试启动 RESTRICT 预检. 返回 `true` = 已进入预检 (DELETE 延迟, 不回包);
/// `false` = 无需检查 (无 NoAction 引用 / 已 cleared 重入), 调用方按原路径继续.
#[allow(clippy::too_many_arguments)]
pub(crate) fn fk_restr_start(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    table: &str,
    pks: &[Vec<u8>],
    resume: RestrResume,
) -> bool {
    // 重入放行: 预检通过的 DELETE 重放时直接进原路径
    if let Some(st) = conn.fk_restr.get(&seq) {
        if st.cleared {
            conn.fk_restr.remove(&seq);
            return false;
        }
        // 已有未完成预检 (不应发生 — 同 seq 不会重入) → 保持延迟, 由在途
        // 探针完成后恢复, 避免双预检并发把 remaining 算重.
        return true;
    }
    // 反向索引: 只关心 NoAction (RESTRICT/NO ACTION) 引用; CASCADE/SetNull
    // 由父行删完后的 cascade_kickoff 处理.
    let mut refs: Vec<(String, String)> = conn
        .sql_shared
        .incoming_fks(db, table)
        .into_iter()
        .filter(|r| r.action == FkAction::NoAction)
        .map(|r| (r.table, r.col))
        .collect();
    refs.sort();
    refs.dedup();
    if refs.is_empty() {
        // incoming_fks 是进程级注册表 (CREATE TABLE 时注册); 本 worker 可能
        // 从未见过建表 (重启 / 多 worker) → 先 catalog 补注再判断.
        if !conn.fk_catalog_done.contains(db.as_ref()) {
            conn.fk_restr.insert(
                seq,
                FkRestrSt {
                    db: db.clone(),
                    resume,
                    stage: RestrStage::LoadingCatalog,
                    cleared: false,
                },
            );
            let op = BatchOp::CatalogDump { db: db.clone() };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
            return true;
        }
        return false;
    }
    if refs.len() > FK_RESTRICT_MAX_REFS {
        conn.resp_complete(
            seq,
            super::sql_err_bytes(
                conn.proto,
                &format!(
                    "too many referencing tables ({}) for foreign key check (max {FK_RESTRICT_MAX_REFS})",
                    refs.len()
                ),
            ),
        );
        return true;
    }
    if pks.len() > FK_RESTRICT_MAX_PKS {
        conn.resp_complete(
            seq,
            super::sql_err_bytes(
                conn.proto,
                &format!(
                    "too many rows ({}) for foreign key check (max {FK_RESTRICT_MAX_PKS}); delete in batches",
                    pks.len()
                ),
            ),
        );
        return true;
    }
    // 子表 schema 必须都在本 worker 缓存 (取引用列列号) — 缺失则挂起拉取
    {
        let cache = conn.sql_cache.borrow();
        for (child, _) in &refs {
            if !cache.schemas.contains_key(&(db.to_string(), child.clone())) {
                drop(cache);
                conn.fk_restr.insert(
                    seq,
                    FkRestrSt {
                        db: db.clone(),
                        resume,
                        stage: RestrStage::LoadingSchema {
                            child: child.clone(),
                        },
                        cleared: false,
                    },
                );
                let op = BatchOp::GetSchemaOp {
                    db: db.clone(),
                    table: Arc::from(child.as_str()),
                };
                push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                return true;
            }
        }
    }
    start_probes(
        conn,
        conn_id,
        seq,
        worker_id,
        db,
        shard_inboxes,
        num_shards,
        refs,
        pks,
        resume,
    )
}

/// 探针规格: (group 序号, 谓词, 子表, 投影列号).
type ProbeSpec = (u32, ScanPred, Arc<str>, u16);

/// 在 schema 缓存借用内解析全部探针.
/// 错误以 String 带出 — 调用方在借用外回复 (避免借用冲突).
fn resolve_probes(
    conn: &ConnState,
    db: &str,
    refs: &[(String, String)],
    pk_vals: &[ColValue],
) -> Result<Vec<ProbeSpec>, String> {
    let cache = conn.sql_cache.borrow();
    let mut out = Vec::with_capacity(refs.len());
    for (i, (child, col)) in refs.iter().enumerate() {
        let Some(child_schema) = cache.schemas.get(&(db.to_string(), child.clone())) else {
            return Err(format!("foreign key check: schema of '{child}' not loaded"));
        };
        let Some(cidx) = child_schema.columns.iter().position(|c| &c.name == col) else {
            return Err(format!(
                "foreign key check: column '{child}.{col}' not found"
            ));
        };
        out.push((
            i as u32,
            ScanPred {
                col: cidx as u16,
                op: PredOp::In,
                val: ColValue::Null,
                set: pk_vals.to_vec(),
            },
            Arc::from(child.as_str()),
            cidx as u16,
        ));
    }
    Ok(out)
}

/// 全部就绪 → 广播探针, 进入 Probing.
#[allow(clippy::too_many_arguments)]
fn start_probes(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    refs: Vec<(String, String)>,
    pks: &[Vec<u8>],
    resume: RestrResume,
) -> bool {
    let Some(pk_ty) = parent_pk_ty(conn, db, &resume_table(&resume)) else {
        conn.resp_complete(
            seq,
            super::sql_err_bytes(conn.proto, "foreign key check: parent schema missing"),
        );
        return true;
    };
    let pk_vals: Vec<ColValue> = pks
        .iter()
        .filter_map(|e| col_from_ordered_bytes(pk_ty, e))
        .collect();
    if pk_vals.len() != pks.len() {
        conn.resp_complete(
            seq,
            super::sql_err_bytes(conn.proto, "foreign key check: bad parent pk encoding"),
        );
        return true;
    }
    let probes = match resolve_probes(conn, db, &refs, &pk_vals) {
        Ok(p) => p,
        Err(e) => {
            conn.resp_complete(seq, super::sql_err_bytes(conn.proto, &e));
            return true;
        }
    };
    for (group, pred, child, cidx) in probes {
        for sid in 0..num_shards {
            let op = BatchOp::ScanFiltered {
                db: db.clone(),
                table: child.clone(),
                preds: vec![pred.clone()],
                proj: vec![cidx],
                index_hint: None,
                key_set_hint: None,
                limit: 1,
            };
            push_task_grouped(conn_id, seq, worker_id, group, sid, op, shard_inboxes);
        }
    }
    conn.fk_restr.insert(
        seq,
        FkRestrSt {
            db: db.clone(),
            resume,
            stage: RestrStage::Probing {
                remaining: refs.len() * num_shards,
                refs,
                violated: None,
                error: None,
            },
            cleared: false,
        },
    );
    true
}

/// resume 归属的父表名 (取 pk 类型 / 重试用).
fn resume_table(resume: &RestrResume) -> String {
    match resume {
        RestrResume::Redispatch(stmt) => match stmt.as_ref() {
            SqlStmt::Delete { table, .. } => table.clone(),
            _ => String::new(),
        },
        RestrResume::Phase2 { target, .. } => target.1.clone(),
    }
}

/// 挂起 (拉 schema / catalog 补注) 完成后的重试: Redispatch 重放语句 (重新
/// 规划自然带出 pks); Phase2 带 pks 重开预检.
#[allow(clippy::too_many_arguments)]
fn retry_after_load(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    resume: RestrResume,
) {
    match resume {
        RestrResume::Redispatch(stmt) => {
            sql_dispatch_stmt(
                conn,
                conn_id,
                seq,
                worker_id,
                db,
                default_db,
                db_view,
                shard_inboxes,
                num_shards,
                *stmt,
            );
        }
        RestrResume::Phase2 {
            pks,
            action,
            target,
        } => {
            let entered = fk_restr_start(
                conn,
                conn_id,
                seq,
                worker_id,
                db,
                shard_inboxes,
                num_shards,
                &target.1,
                &pks,
                RestrResume::Phase2 {
                    pks: pks.clone(),
                    action: action.clone(),
                    target: target.clone(),
                },
            );
            if !entered {
                run_dml_phase2(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    default_db,
                    db_view,
                    shard_inboxes,
                    num_shards,
                    pks,
                    action,
                    target,
                );
            }
        }
    }
}

/// 探针/挂起回包处理 (在 handle_resp_shard_result 内命中 seq 时调).
/// 返回 true = 已消费.
#[allow(clippy::too_many_arguments)]
pub(crate) fn fk_restr_on_reply(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    group: u32,
    result: &BatchResult,
) -> bool {
    // 快照 stage 判别 + db (避免整个 match 持有 conn 不可变借用)
    let (db, kind) = {
        let Some(st) = conn.fk_restr.get(&seq) else {
            return false;
        };
        // cleared 残留 (重放语句走了非 fk_restr_start 的规划路径) → 丢弃,
        // 勿把后续回包当探针回包消耗 (remaining 下溢 / 吞掉真回包).
        if st.cleared {
            conn.fk_restr.remove(&seq);
            return false;
        }
        let kind = match &st.stage {
            RestrStage::LoadingCatalog => StageKind::Catalog,
            RestrStage::LoadingSchema { child } => StageKind::Schema(child.clone()),
            RestrStage::Probing { .. } => StageKind::Probing,
        };
        (st.db.clone(), kind)
    };
    match kind {
        StageKind::Catalog => match result {
            BatchResult::Catalog(entries) => {
                // 补注全库 FK (与 CREATE TABLE 注册同源; register_fks 幂等去重)
                for (t, b) in entries {
                    if let Ok(s) = TableSchema::decode(b) {
                        conn.sql_shared.register_fks(&db, t, &s);
                    }
                }
                conn.fk_catalog_done.insert(db.to_string());
                let resume = conn.fk_restr.remove(&seq).expect("just checked").resume;
                retry_after_load(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    &db,
                    default_db,
                    db_view,
                    shard_inboxes,
                    num_shards,
                    resume,
                );
                true
            }
            BatchResult::Error(e) => {
                conn.fk_restr.remove(&seq);
                conn.resp_complete(seq, super::sql_err_bytes(conn.proto, e));
                true
            }
            _ => {
                conn.fk_restr.remove(&seq);
                conn.resp_complete(
                    seq,
                    super::sql_err_bytes(conn.proto, "unexpected catalog reply"),
                );
                true
            }
        },
        StageKind::Schema(child) => match result {
            BatchResult::GetValue(Some(bytes)) => match TableSchema::decode(bytes) {
                Ok(s) => {
                    conn.sql_cache
                        .borrow_mut()
                        .schemas
                        .insert((db.to_string(), child.clone()), Arc::new(s));
                    let resume = conn.fk_restr.remove(&seq).expect("just checked").resume;
                    retry_after_load(
                        conn,
                        conn_id,
                        seq,
                        worker_id,
                        &db,
                        default_db,
                        db_view,
                        shard_inboxes,
                        num_shards,
                        resume,
                    );
                    true
                }
                Err(e) => {
                    conn.fk_restr.remove(&seq);
                    conn.resp_complete(
                        seq,
                        super::sql_err_bytes(conn.proto, &format!("bad schema: {e}")),
                    );
                    true
                }
            },
            BatchResult::GetValue(None) => {
                conn.fk_restr.remove(&seq);
                conn.resp_complete(
                    seq,
                    super::sql_err_bytes(
                        conn.proto,
                        &format!("foreign key check: table '{child}' has no schema"),
                    ),
                );
                true
            }
            BatchResult::Error(e) => {
                conn.fk_restr.remove(&seq);
                conn.resp_complete(seq, super::sql_err_bytes(conn.proto, e));
                true
            }
            _ => {
                conn.fk_restr.remove(&seq);
                conn.resp_complete(
                    seq,
                    super::sql_err_bytes(conn.proto, "unexpected schema reply"),
                );
                true
            }
        },
        StageKind::Probing => {
            // 命中归属 (group = 探针序号 → refs[group])
            let hit: Option<(String, String)> = match result {
                BatchResult::ProjRows(rows) if !rows.is_empty() => {
                    let st = conn.fk_restr.get(&seq).expect("just checked");
                    match &st.stage {
                        RestrStage::Probing { refs, .. } => refs.get(group as usize).cloned(),
                        _ => None,
                    }
                }
                _ => None,
            };
            let done = {
                let st = conn.fk_restr.get_mut(&seq).expect("just checked");
                let RestrStage::Probing {
                    remaining,
                    violated,
                    error,
                    ..
                } = &mut st.stage
                else {
                    unreachable!("stage 已判定为 Probing")
                };
                if hit.is_some() && violated.is_none() {
                    *violated = hit;
                }
                if let BatchResult::Error(e) = result
                    && error.is_none()
                {
                    *error = Some(e.clone());
                }
                *remaining -= 1;
                *remaining == 0
            };
            if !done {
                return true;
            }
            // 收齐 → 判定 + 恢复
            let (violated, error) = {
                let st = conn.fk_restr.get(&seq).expect("just checked");
                let RestrStage::Probing {
                    violated, error, ..
                } = &st.stage
                else {
                    unreachable!("stage 已判定为 Probing")
                };
                (violated.clone(), error.clone())
            };
            if let Some(e) = error {
                conn.fk_restr.remove(&seq);
                conn.resp_complete(seq, super::sql_err_bytes(conn.proto, &e));
                return true;
            }
            if let Some((child, col)) = violated {
                conn.fk_restr.remove(&seq);
                conn.resp_complete(
                    seq,
                    super::sql_err_bytes_e(
                        conn.proto,
                        // ⭐ 审计 P3-5/6: 结构化错误码 — 不依赖消息文本被分类器认出.
                        &SqlError::new(
                            SqlErrorCode::FkViolation,
                            format!(
                                "foreign key violation: {child}.{col} still references the deleted row(s)"
                            ),
                        ),
                    ),
                );
                return true;
            }
            // 全净 → 放行原 DELETE
            let is_phase2 = matches!(
                conn.fk_restr.get(&seq).expect("just checked").resume,
                RestrResume::Phase2 { .. }
            );
            if is_phase2 {
                // 无重入: 消费 entry, 直接续 phase2
                let st = conn.fk_restr.remove(&seq).expect("just checked");
                let RestrResume::Phase2 {
                    pks,
                    action,
                    target,
                } = st.resume
                else {
                    unreachable!("已判定 Phase2")
                };
                run_dml_phase2(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    default_db,
                    db_view,
                    shard_inboxes,
                    num_shards,
                    pks,
                    action,
                    target,
                );
            } else {
                // Redispatch: 标记 cleared 后重放语句 — 二次进入 fk_restr_start
                // 时守卫命中 (remove + 放行), 不会再次预检形成环.
                let st = conn.fk_restr.get_mut(&seq).expect("just checked");
                st.cleared = true;
                let RestrResume::Redispatch(stmt) = &st.resume else {
                    unreachable!("已判定非 Phase2")
                };
                let stmt = stmt.clone();
                sql_dispatch_stmt(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    &db,
                    default_db,
                    db_view,
                    shard_inboxes,
                    num_shards,
                    *stmt,
                );
            }
            true
        }
    }
}
