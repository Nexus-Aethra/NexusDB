//! ⭐ PG 兼容 (引用完整性, FMT_VER 8): 外键 INSERT 存在性预检.
//!
//! 设计: INSERT 构建完 RowPut 后, 若本表有 fks, 不直接发写 — 先对每个
//! 非 NULL 外键值向父表发 `RowGet` 存在性检查 (real seq, 聚合于 sql_fk_ins);
//! 全部父行存在 → 才注册 sql_dml_agg 发原 RowPut; 任一缺失 → 拒 (外键违规).
//!
//! 前置: 父表 schema 须已在 worker 缓存 (由调用方在 sql_dispatch_stmt 加载).
//! 跨 shard: RowGet 按父表主键 hash 路由到父表所在 shard (正确性优先).

use super::{ConnState, SqlFkIns, hash_route_op, push_task_grouped};
use shard_manager::{BatchOp, SharedTaskInbox};
use storage::row::ColValue;

/// ⭐ 尝试启动外键预检. 返回 `true` = 已进入预检 (INSERT 延迟, 不回包, ops 已
/// 移入状态); `false` = 未进入 (父表 schema 缺失 / 类型不匹配 / 无 fk),
/// ops 由调用方保留走原 INSERT 路径 (本函数以 `&ops` 只读检查, 仅成功时 clone).
#[allow(clippy::too_many_arguments)]
pub fn sql_fk_start(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    schema: &storage::schema::TableSchema,
    ops: &[BatchOp],
) -> bool {
    // 收集所有非 NULL 外键引用 (父表, 父主键编码).
    // 本表 fks: (col, ref_table, ref_col). 每个 RowPut 的 fk 列值 → 父主键.
    let mut checks: Vec<(String, Vec<u8>)> = Vec::new();
    for op in ops {
        let BatchOp::RowPut {
            db: _,
            table: _,
            pk: _,
            values,
        } = op
        else {
            continue;
        };
        for fk in &schema.fks {
            let i = fk.col as usize;
            if i >= values.len() {
                continue;
            }
            let v = &values[i];
            if *v == ColValue::Null {
                continue; // NULL 外键: 不校验 (PG: 引用完整性允许 NULL)
            }
            // 父表主键编码 — 父表 schema 须在缓存
            let parent_key = (db.to_string(), fk.ref_table.clone());
            let Some(parent_schema) = conn.sql_cache.borrow().schemas.get(&parent_key).cloned()
            else {
                return false; // 父表 schema 缺失 → 调用方走原 INSERT
            };
            // ref_col 应为父表主键列 (或任一列); 用父表 pk 列类型编码
            let pk_col = parent_schema.pk_col as usize;
            let enc = match super::sql_pk_bytes(parent_schema.columns[pk_col].ty, v) {
                Ok(e) => e,
                Err(_) => return false, // 类型不匹配 → 原 INSERT
            };
            checks.push((fk.ref_table.clone(), enc));
        }
    }
    if checks.is_empty() {
        return false; // 无外键或全 NULL → 走原 INSERT
    }
    // 去重 (同父表同 pk 只查一次)
    let mut seen = std::collections::HashSet::new();
    checks.retain(|c| seen.insert(c.clone()));
    conn.sql_fk_ins.insert(
        seq,
        SqlFkIns {
            remaining: checks.len(),
            ok: 0,
            missing: Vec::new(),
            error: None,
            ops: ops.to_vec(), // 成功才 clone 一次
            schema: schema.clone().into(),
            db: db.clone(),
        },
    );
    // 发父表存在性检查 (RowGet, real seq — 回包路由到 sql_fk_ins)
    for (table, pk) in checks {
        let op = BatchOp::RowGet {
            db: db.clone(),
            table: std::sync::Arc::from(table.as_str()),
            pk,
        };
        let sid = hash_route_op(&op, num_shards);
        push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
    }
    true
}

/// ⭐ 外键预检回包处理 (在 sql_dml_agg 之前路由). 返回 true = 已消费.
#[allow(clippy::too_many_arguments)]
pub fn sql_fk_on_reply(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    result: &shard_manager::request::BatchResult,
) -> bool {
    let Some(st) = conn.sql_fk_ins.get_mut(&seq) else {
        return false;
    };
    match result {
        shard_manager::request::BatchResult::GetValue(Some(_)) => st.ok += 1,
        shard_manager::request::BatchResult::GetValue(None) => {
            // 缺失: 记录 (无法从回包拿父表名 — 用剩余计数推断, 但只需知道"有缺失")
            st.missing.push((String::new(), Vec::new()));
        }
        shard_manager::request::BatchResult::Error(e) => {
            if st.error.is_none() {
                st.error = Some(e.clone());
            }
        }
        _ => {}
    }
    st.remaining -= 1;
    if st.remaining > 0 {
        return true;
    }
    // 全部回齐: 取出状态
    let st = conn.sql_fk_ins.remove(&seq).expect("just checked");
    if let Some(e) = st.error {
        conn.resp_complete(seq, super::sql_err_bytes(conn.proto, &e));
        return true;
    }
    if !st.missing.is_empty() {
        conn.resp_complete(
            seq,
            super::sql_err_bytes(
                conn.proto,
                "foreign key violation: referenced row does not exist",
            ),
        );
        return true;
    }
    // 全通过 → 注册 sql_dml_agg 发原 RowPut (与普通 INSERT 一致: bloom + 广播)
    let n = st.ops.len();
    conn.sql_dml_agg.insert(
        seq,
        super::SqlDmlAgg {
            remaining: n,
            affected: 0,
            error: None,
            drop_key: None,
        },
    );
    for op in st.ops {
        let sid = hash_route_op(&op, num_shards);
        let (_, table, _) = op.locator();
        super::feed_route_bloom(conn, st.db.as_ref(), &table, st.schema.as_ref(), &op, sid);
        push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
    }
    true
}

/// ⭐ 判断本表所有外键父表 schema 是否已在 worker 缓存.
pub fn all_parents_cached(
    conn: &ConnState,
    db: &str,
    schema: &storage::schema::TableSchema,
) -> bool {
    for fk in &schema.fks {
        let key = (db.to_string(), fk.ref_table.clone());
        if !conn.sql_cache.borrow().schemas.contains_key(&key) {
            return false;
        }
    }
    true
}
