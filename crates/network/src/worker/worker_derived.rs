//! SQL derived table (子查询) 物化与渲染 (拆自 mod.rs).
//!
//! `sql_subq_start`/`sql_subq_advance`: 谓词子查询收集与推进;
//! `derived_capture_rowctx`/`finish_derived`/`finish_derived_join`: 派生表物化;
//! `derived_render`: 派生表结果渲染 (ORDER BY / OFFSET / LIMIT / 投影).

use super::*;

pub(crate) fn sql_subq_start(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    stmt: &SqlStmt,
) -> bool {
    // ⭐ F74: 先去相关 (单等值关联 EXISTS/NOT EXISTS → 非关联 IN/NOT IN);
    // 不可去相关形态 → 报错 (已消费, 返回 true)
    let decorr;
    let stmt: &SqlStmt = match decorrelate_stmt(stmt) {
        Ok(s) => {
            decorr = s;
            &decorr
        }
        Err(e) => {
            conn.resp_complete(seq, sql_err_bytes(conn.proto, &e));
            return true;
        }
    };
    let mut inners: Vec<SqlStmt> = Vec::new();
    if let Some(p) = stmt_where_conds(stmt) {
        collect_pred_subq(p, &mut inners);
    }
    if inners.is_empty() {
        return false;
    }
    // v1: 内层仅单表 SELECT (非 JOIN, 非嵌套) — 否则会绕过 SqlSelectAgg 拦截
    for inn in &inners {
        if !matches!(inn, SqlStmt::Select { .. }) {
            conn.resp_complete(
                seq,
                sql_err_bytes(conn.proto, "subquery inner must be a simple SELECT (v1)"),
            );
            return true;
        }
        if let Some(p) = stmt_where_conds(inn) {
            let mut nested = Vec::new();
            collect_pred_subq(p, &mut nested);
            if !nested.is_empty() {
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, "nested subquery not supported (v1)"),
                );
                return true;
            }
        }
    }
    let first = inners[0].clone();
    conn.sql_subq.insert(
        seq,
        SubqCtx { outer: stmt.clone(), db: db.clone(), inners, results: Vec::new(), cur: 0 },
    );
    sql_dispatch_stmt(
        conn, conn_id, seq, worker_id, db, default_db, db_view, shard_inboxes, num_shards, first,
    );
    true
}

/// ⭐ F71: 内层完成→存行集→跑下一内层或折叠重跑外层.
#[allow(clippy::too_many_arguments)]
pub(crate) fn sql_subq_advance(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    captured: Vec<Vec<ColValue>>,
) {
    let (next, db) = {
        let ctx = conn.sql_subq.get_mut(&seq).expect("subq ctx");
        ctx.results.push(captured);
        ctx.cur += 1;
        let next = ctx.inners.get(ctx.cur).cloned();
        (next, ctx.db.clone())
    };
    if let Some(inner) = next {
        sql_dispatch_stmt(
            conn, conn_id, seq, worker_id, &db, default_db, db_view, shard_inboxes, num_shards, inner,
        );
        return;
    }
    // 全部内层完 → 折叠 → 重跑外层
    let ctx = conn.sql_subq.remove(&seq).expect("subq ctx");
    let folded = {
        let conds = stmt_where_conds(&ctx.outer).expect("outer has where");
        let mut it = ctx.results.iter();
        fold_pred_subq(conds, &mut it)
    };
    match folded {
        Ok(fp) => {
            let outer = stmt_replace_conds(ctx.outer, fp);
            sql_dispatch_stmt(
                conn, conn_id, seq, worker_id, &db, default_db, db_view, shard_inboxes, num_shards,
                outer,
            );
        }
        Err(e) => conn.resp_complete(seq, sql_err_bytes(conn.proto, &e)),
    }
}

/// ⭐ F72: 派生表内层走 pk 点查 (SqlRowCtx) 完成时的物化 —
/// 从 ctx 合成列定义 (COUNT → 单列; 否则投影列) + 0/1 行行集.
pub(crate) fn derived_capture_rowctx(
    ctx: &SqlRowCtx,
    hit: bool,
    values: &[ColValue],
) -> (Vec<(String, ColType)>, Vec<Vec<ColValue>>) {
    if ctx.count {
        let n = i64::from(hit);
        return (
            vec![("COUNT(*)".to_string(), ColType::I64)],
            vec![vec![ColValue::I64(n)]],
        );
    }
    let cols: Vec<(String, ColType)> = ctx
        .proj
        .iter()
        .map(|&i| {
            let c = &ctx.schema.columns[i as usize];
            (c.name.clone(), c.ty)
        })
        .collect();
    let rows = if hit {
        vec![ctx.proj.iter().map(|&i| values[i as usize].clone()).collect()]
    } else {
        vec![]
    };
    (cols, rows)
}

/// ⭐ F72: 派生表内层物化完成 → 外层在 worker 内存执行并回包.
#[allow(clippy::too_many_arguments)]
pub(crate) fn finish_derived(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    binary: bool,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    cols: Vec<(String, ColType)>,
    rows: Vec<Vec<ColValue>>,
) {
    let ctx = conn.sql_derived.remove(&seq).expect("derived ctx");
    match ctx {
        // ⭐ F72: 单独派生表 → worker 内存执行外层并回包
        DerivedCtx::Standalone { alias, items, conds, order, limit, offset } => {
            let bytes = derived_render(
                conn.proto, binary, &alias, &items, &conds, &order, limit, offset, &cols, rows,
            );
            conn.resp_complete(seq, bytes);
        }
        // ⭐ F75: 派生表作 JOIN 首表 → 预填 tables[0] 后转 JOIN 状态机
        DerivedCtx::JoinFrom { db, join_stmt } => {
            finish_derived_join(
                conn, conn_id, seq, worker_id, shard_inboxes, num_shards, db, join_stmt, cols, rows,
            );
        }
    }
}

/// ⭐ F75: 派生表物化完成 → 建 SqlJoinCtx (tables[0] 预填) → sql_join_kickoff.
#[allow(clippy::too_many_arguments)]
pub(crate) fn finish_derived_join(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    db: std::sync::Arc<str>,
    join_stmt: SqlStmt,
    cols: Vec<(String, ColType)>,
    rows: Vec<Vec<ColValue>>,
) {
    if rows.len() > JOIN_MAX_ROWS {
        conn.resp_complete(seq, sql_err_bytes(conn.proto, "derived table too large (limit 262144 rows)"));
        return;
    }
    let SqlStmt::SelectJoin { from, joins, items, conds, order, limit, offset, .. } = join_stmt else {
        conn.resp_complete(seq, sql_err_bytes(conn.proto, "internal: derived join expects SelectJoin"));
        return;
    };
    // 合成派生表 schema (内层真实列类型); proj = 全列 identity (行已定宽)
    let synth = std::sync::Arc::new(TableSchema {
        version: 1,
        columns: cols
            .iter()
            .map(|(n, t)| storage::schema::Column {
                name: n.clone(),
                ty: *t,
                nullable: true,
                default: None,
            })
            .collect(),
        pk_col: 0,
        indexes: Vec::new(),
        dropped: Vec::new(),
        next_iid: 0,
        version_ncols: Vec::new(),
            fks: Vec::new(),});
    let ncols = cols.len() as u16;
    let mut tables: Vec<JoinTable> = Vec::with_capacity(joins.len() + 1);
    tables.push(JoinTable {
        table: std::sync::Arc::from(from.table.as_str()),
        alias: from.alias.clone(),
        schema: Some(synth),
        proj: (0..ncols).collect(),
        rows,
        prefilled: true,
    });
    for j in &joins {
        let schema = conn
            .sql_cache
            .borrow()
            .schemas
            .get(&(db.to_string(), j.table.table.clone()))
            .cloned();
        tables.push(JoinTable {
            table: std::sync::Arc::from(j.table.table.as_str()),
            alias: j.table.alias.clone(),
            schema,
            proj: Vec::new(),
            rows: Vec::new(),
            prefilled: false,
        });
    }
    let ctx = SqlJoinCtx {
        db,
        tables,
        joins,
        items,
        conds,
        order,
        limit,
        offset,
        phase: JoinPhase::Gather(0),
        remaining: 0,
        swapped: false,
        gather_order: Vec::new(),
        est_phase: 0,
        est_rows: [0, 0],
        join_distinct: Vec::new(),
        join_ranges: Vec::new(),
    };
    conn.sql_join.insert(seq, ctx);
    sql_join_kickoff(conn, conn_id, seq, worker_id, shard_inboxes, num_shards);
}

/// ⭐ F72: 外层内存管线 — 列名解析 (剥 alias 前缀) → eval_pred 过滤 →
/// ORDER → OFFSET/LIMIT → 投影 (COUNT(*) 特判) → 渲染 (sysq_finish 同款先例,
/// 但保留内层真实列类型).
#[allow(clippy::too_many_arguments)]
pub(crate) fn derived_render(
    proto: ProtocolKind,
    binary: bool,
    alias: &str,
    items: &[sql::SelectItem],
    conds_in: &Pred<Cond>,
    order: &[(String, bool)],
    limit: Option<u32>,
    offset: Option<u32>,
    cols: &[(String, ColType)],
    mut rows: Vec<Vec<ColValue>>,
) -> Vec<u8> {
    if rows.len() > JOIN_MAX_ROWS {
        return sql_err_bytes(proto, "derived table too large (limit 262144 rows)");
    }
    // 列名解析: `t.x` / 裸 `x` — qualifier 仅接受 alias
    let resolve = |name: &str| -> Result<usize, String> {
        let qc = QualCol::parse(name);
        if let Some(q) = &qc.qualifier
            && !q.eq_ignore_ascii_case(alias)
        {
            return Err(format!("unknown table '{q}'"));
        }
        cols.iter()
            .position(|(n, _)| n.eq_ignore_ascii_case(&qc.col))
            .ok_or_else(|| format!("unknown column '{}'", qc.col))
    };
    // 合成 schema (内层真实列类型) 供 eval_pred; 叶子列名先剥前缀重写
    let schema = TableSchema {
        version: 1,
        columns: cols
            .iter()
            .map(|(n, t)| storage::schema::Column {
                name: n.clone(),
                ty: *t,
                nullable: true,
                default: None,
            })
            .collect(),
        pk_col: 0,
        indexes: Vec::new(),
        dropped: Vec::new(),
        next_iid: 0,
        version_ncols: Vec::new(),
            fks: Vec::new(),};
    let conds = match conds_in.try_map(&|c: &Cond| {
        let idx = resolve(&c.col)?;
        Ok::<_, String>(Cond {
            col: schema.columns[idx].name.clone(),
            op: c.op,
            val: c.val.clone(),
            set: c.set.clone(),
        })
    }) {
        Ok(p) => p,
        Err(e) => return sql_err_bytes(proto, &e),
    };
    rows.retain(|r| eval_pred(&schema, r, &conds));
    // ORDER BY (逆序叠加稳定排序 = 多键优先级)
    for (name, desc) in order.iter().rev() {
        match resolve(name) {
            Ok(ci) => rows.sort_by(|a, b| {
                let o = cmp_colvalue(&a[ci], &b[ci]);
                if *desc { o.reverse() } else { o }
            }),
            Err(e) => return sql_err_bytes(proto, &e),
        }
    }
    // OFFSET / LIMIT
    let start = (offset.unwrap_or(0) as usize).min(rows.len());
    let end = match limit {
        Some(l) => (start + l as usize).min(rows.len()),
        None => rows.len(),
    };
    let rows = &rows[start..end];
    // COUNT(*) 特判 (parse 已保证含 Agg 时必为孤 COUNT(*))
    if items.iter().any(|i| matches!(i, sql::SelectItem::Agg { .. })) {
        let cref = [("COUNT(*)", ColType::I64)];
        return sql_rows_bytes(proto, binary, &cref, &[vec![ColValue::I64(rows.len() as i64)]]);
    }
    // ⭐ compat: 标量函数投影 (SELECT NOW()/version()) — 常量单行
    if items.iter().all(|i| matches!(i, sql::SelectItem::ScalarFn { .. })) && !items.is_empty() {
        let (cref, row) = match scalar_fn_const_row(items) {
            Ok(v) => v,
            Err(e) => return sql_err_bytes(proto, &e),
        };
        return sql_rows_bytes(proto, binary, &cref, &[row]);
    }
    // 投影: items 空 = 全列
    if items.is_empty() {
        let cref: Vec<(&str, ColType)> = cols.iter().map(|(n, t)| (n.as_str(), *t)).collect();
        return sql_rows_bytes(proto, binary, &cref, rows);
    }
    let mut idxs: Vec<usize> = Vec::with_capacity(items.len());
    for it in items {
        match it {
            sql::SelectItem::Col { name: c, .. } => match resolve(c) {
                Ok(i) => idxs.push(i),
                Err(e) => return sql_err_bytes(proto, &e),
            },
            sql::SelectItem::Agg { .. } => unreachable!("孤 COUNT(*) 已在上方特判"),
            sql::SelectItem::ScalarFn { .. } => unreachable!("标量函数已在上方常量特判"),
            sql::SelectItem::Expr { .. } => {
                return sql_err_bytes(proto, "expression projections in derived tables are not supported (v1)")
            }
        }
    }
    let cref: Vec<(&str, ColType)> = idxs.iter().map(|&i| (cols[i].0.as_str(), cols[i].1)).collect();
    let proj: Vec<Vec<ColValue>> =
        rows.iter().map(|r| idxs.iter().map(|&i| r[i].clone()).collect()).collect();
    sql_rows_bytes(proto, binary, &cref, &proj)
}
