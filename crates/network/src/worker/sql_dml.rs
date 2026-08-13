// ⭐ 解耦 2026-08: DML 执行 (INSERT/UPDATE/DELETE 分布式执行) — 从 sql_dispatch.rs 拆出.
use super::sql_dispatch::conds_to_scan_preds;
use super::*;

/// ⭐ F76: 剥列名的表名限定前缀 (`表.列`/`别名.列` → `列`); 仅当前缀匹配时.
pub(crate) fn strip_col_qual(col: &mut String, table: &str) {
    if let Some((q, c)) = col.split_once('.')
        && q.eq_ignore_ascii_case(table)
    {
        *col = c.to_string();
    }
}

pub(crate) fn strip_pred_qual(pred: &mut Pred<Cond>, table: &str) {
    match pred {
        Pred::Leaf(c) => strip_col_qual(&mut c.col, table),
        Pred::And(v) | Pred::Or(v) => v.iter_mut().for_each(|p| strip_pred_qual(p, table)),
        Pred::Not(b) => strip_pred_qual(b, table),
    }
}

/// ⭐ PG 兼容 (UPDATE SET 表达式): SQL ScalarExpr → storage RowExpr
/// (绑定列名→列号). 未知列/JSONB 取字段 → Err.
pub(crate) fn sql_update_expr_to_row(
    schema: &TableSchema,
    e: &sql::ScalarExpr,
) -> Result<storage::row::RowExpr, String> {
    use storage::row::{RowArith, RowExpr};
    Ok(match e {
        sql::ScalarExpr::Col(name) => {
            let i = schema
                .col_by_name(name)
                .ok_or_else(|| format!("unknown column '{name}'"))?;
            RowExpr::Col(i)
        }
        sql::ScalarExpr::Lit(v) => {
            let cv = match v {
                sql::SqlValue::Int(x) => ColValue::I64(*x),
                sql::SqlValue::Float(x) => ColValue::F64(*x),
                sql::SqlValue::Str(b) => ColValue::Bytes(b.clone()),
                _ => ColValue::Null,
            };
            RowExpr::Lit(cv)
        }
        sql::ScalarExpr::Not(inner) => {
            RowExpr::Not(Box::new(sql_update_expr_to_row(schema, inner)?))
        }
        sql::ScalarExpr::Bin { op, l, r } => {
            let lo = sql_update_expr_to_row(schema, l)?;
            let ro = sql_update_expr_to_row(schema, r)?;
            RowExpr::Bin {
                op: match op {
                    sql::ArithOp::Add => RowArith::Add,
                    sql::ArithOp::Sub => RowArith::Sub,
                    sql::ArithOp::Mul => RowArith::Mul,
                    sql::ArithOp::Div => RowArith::Div,
                },
                l: Box::new(lo),
                r: Box::new(ro),
            }
        }
        sql::ScalarExpr::JsonGet { .. } => {
            return Err("JSONB field expression not supported in UPDATE SET (v1)".into());
        }
    })
}

/// ⭐ compat: 表达式投影 base 列号 (JSONB 表达式根列; v1: 递归取 JsonGet 底层
/// 列引用; Lit 等无列场景回退列 0 — 渲染时求值仍可取到值).
pub(crate) fn bound_base_col(e: &BoundExpr) -> u16 {
    match e {
        BoundExpr::Col(i) => *i,
        BoundExpr::JsonGet { base, .. } => bound_base_col(base),
        BoundExpr::Not(inner) => bound_base_col(inner),
        BoundExpr::Lit(_) | BoundExpr::Bin { .. } => 0,
    }
}

/// ⭐ F76: 单表 Select/Delete/Update 内所有列引用剥表名限定符 (JOIN 走 QualCol 不经此).
pub(crate) fn strip_qual_in_stmt(stmt: &mut SqlStmt) {
    match stmt {
        SqlStmt::Select {
            table,
            items,
            conds,
            order,
            group_by,
            having,
            ..
        } => {
            let t = table.clone();
            for it in items.iter_mut() {
                match it {
                    sql::SelectItem::Col { name, .. } => strip_col_qual(name, &t),
                    // ⭐ F78: 聚合参可为表达式 — 递归剥内部列引用的表限定前缀
                    sql::SelectItem::Agg { arg: Some(e), .. } => {
                        e.for_each_col_mut(&mut |c| strip_col_qual(c, &t));
                    }
                    sql::SelectItem::Agg { .. } => {}
                    sql::SelectItem::ScalarFn { .. } => {}
                    // ⭐ compat: 表达式投影内部列引用剥表限定前缀
                    sql::SelectItem::Expr { expr, .. } => {
                        expr.for_each_col_mut(&mut |c| strip_col_qual(c, &t));
                    }
                }
            }
            strip_pred_qual(conds, &t);
            strip_pred_qual(having, &t);
            for (n, _) in order.iter_mut() {
                strip_col_qual(n, &t);
            }
            for g in group_by.iter_mut() {
                strip_col_qual(g, &t);
            }
        }
        SqlStmt::Delete { table, conds } => {
            let t = table.clone();
            strip_pred_qual(conds, &t);
        }
        SqlStmt::Update { table, sets, conds } => {
            let t = table.clone();
            for (c, _) in sets.iter_mut() {
                strip_col_qual(c, &t);
            }
            strip_pred_qual(conds, &t);
        }
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn sql_run_dml(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    schema: std::sync::Arc<TableSchema>,
    stmt: SqlStmt,
) {
    // ⭐ F76: 单表限定列 `表.列` → 剥为 `列` (ORM 单表查询也带表名限定符)
    let mut stmt = stmt;
    strip_qual_in_stmt(&mut stmt);
    match stmt {
        // ⭐ compat: 无 FROM 标量函数投影在 sql_dispatch_stmt 已处理
        SqlStmt::ScalarSelect { .. } => unreachable!("ScalarSelect 在 sql_dispatch_stmt 处理"),
        SqlStmt::Insert { table, cols, rows } => {
            // ⭐ S1: 多行 VALUES — 逐行 RowPut, DmlAgg 计数 (批内非原子, 文档记录)
            let mut ops: Vec<BatchOp> = Vec::with_capacity(rows.len());
            for vals in &rows {
                let values = match sql_build_row(&schema, &cols, vals) {
                    Ok(v) => v,
                    Err(e) => {
                        conn.resp_complete(seq, sql_err_bytes(conn.proto, &e));
                        return;
                    }
                };
                let pk = match sql_pk_bytes(
                    schema.columns[schema.pk_col as usize].ty,
                    &values[schema.pk_col as usize],
                ) {
                    Ok(p) => p,
                    Err(e) => {
                        conn.resp_complete(seq, sql_err_bytes(conn.proto, &e));
                        return;
                    }
                };
                ops.push(BatchOp::RowPut {
                    db: db.clone(),
                    table: std::sync::Arc::from(table.as_str()),
                    pk,
                    values,
                });
            }
            // ⭐ PG 兼容 (引用完整性, FMT_VER 8): 外键存在性预检.
            // 非事务 autocommit 且本表有 fks 且父表 schema 已缓存 → 先验父行
            // 存在再写 (全存在才发 RowPut). 父表 schema 缺失 / 类型不匹配 →
            // v1 边界跳过引用检查 (Loom 前置校验父实体, 不依赖; 文档化).
            if conn.txn.is_none() && !schema.fks.is_empty() {
                // 父表 schema 均缓存 → 进入外键预检 (全存在才发 RowPut)
                if all_parents_cached(conn, db, &schema) {
                    if sql_fk_start(
                        conn,
                        conn_id,
                        seq,
                        worker_id,
                        db,
                        shard_inboxes,
                        num_shards,
                        &schema,
                        &ops,
                    ) {
                        return;
                    }
                }
            }
            // ⭐ 事务 v1 (F61): 事务中 INSERT 截流进 write_set (喂 bloom
            // 照旧 — rollback 只多假阳性), 立即回 OK, commit 时原子应用
            if conn.txn.is_some() {
                // ⭐ F65 v1 边界: 全局唯一表不支持事务内写 (占坑需在 commit 编排,
                // 未实现; 拒绝而非静默破坏全局唯一性)
                if schema.indexes.iter().any(|i| i.unique && i.global) {
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(
                            conn.proto,
                            "INSERT into GLOBAL UNIQUE table inside a transaction not supported (v1); use autocommit",
                        ),
                    );
                    return;
                }
                let n = ops.len() as u64;
                for op in ops {
                    let sid = hash_route_op(&op, num_shards);
                    feed_route_bloom(conn, db, &table, &schema, &op, sid);
                    if let Err(e) = txn_buffer_op(conn, op) {
                        conn.resp_complete(seq, sql_err_bytes(conn.proto, &e));
                        return;
                    }
                }
                conn.resp_complete(seq, sql_ok_bytes(conn.proto, n));
                return;
            }
            conn.sql_dml_agg.insert(
                seq,
                SqlDmlAgg {
                    remaining: ops.len(),
                    affected: 0,
                    error: None,
                    drop_key: None,
                },
            );
            // ⭐ F65: 含全局唯一列且单行 autocommit → 走占坑编排
            let has_gu = schema.indexes.iter().any(|i| i.unique && i.global);
            if has_gu {
                if ops.len() != 1 {
                    conn.sql_dml_agg.remove(&seq);
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(
                            conn.proto,
                            "multi-row INSERT into GLOBAL UNIQUE table not supported (v1)",
                        ),
                    );
                    return;
                }
                conn.sql_dml_agg.remove(&seq); // 占坑编排自己管回复
                let BatchOp::RowPut { pk, values, .. } = ops.into_iter().next().unwrap() else {
                    unreachable!()
                };
                // 喂 bloom (与普通路径一致)
                let probe = BatchOp::RowPut {
                    db: db.clone(),
                    table: std::sync::Arc::from(table.as_str()),
                    pk: pk.clone(),
                    values: values.clone(),
                };
                let sid = hash_route_op(&probe, num_shards);
                feed_route_bloom(conn, db, &table, &schema, &probe, sid);
                sql_unique_ins_start(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    db,
                    shard_inboxes,
                    num_shards,
                    schema,
                    table,
                    pk,
                    values,
                );
                return;
            }
            for op in ops {
                // ⭐ W2 → ORM-B2: created_here 的表 → 喂进程级路由缓存
                // (value → 所在 shard; bloom 原子只增, 多 worker/门面并发安全)
                let sid = hash_route_op(&op, num_shards);
                feed_route_bloom(conn, db, &table, &schema, &op, sid);
                // ⭐ 巨型 INSERT 防死锁 (2026-08): 非阻塞 push, inbox 满时先
                // drain reply_bus 处理回包 (释放 reply_bus 让 shard 继续消费
                // inbox), 再重试 — 打破 worker↔shard 有界队列循环等待.
                let mut task = shard_manager::request::ShardTask {
                    conn_id,
                    req_id: seq,
                    worker_id,
                    group: sid as u32,
                    op,
                };
                loop {
                    match shard_inboxes[sid].push(task) {
                        Ok(()) => break,
                        Err(rejected) => {
                            conn.drain_replies(conn_id);
                            task = rejected;
                            std::thread::yield_now();
                        }
                    }
                }
            }
        }
        // ⭐ S1: DELETE / UPDATE — pk 等值单发, 其余两阶段 (SELECT 内部路径收 pk)
        SqlStmt::Delete { .. } | SqlStmt::Update { .. } => {
            let (table, conds, action) = match stmt {
                SqlStmt::Delete { table, conds } => (table, conds, SqlDmlAction::Delete),
                SqlStmt::Update { table, conds, sets } => {
                    // 校验 + 转换 sets → (列号, 值或表达式)
                    let mut out: Vec<(u16, storage::row::SetVal)> = Vec::with_capacity(sets.len());
                    for (name, v) in &sets {
                        let Some(i) = schema.col_by_name(name) else {
                            conn.resp_complete(
                                seq,
                                sql_err_bytes(conn.proto, &format!("unknown column '{name}'")),
                            );
                            return;
                        };
                        if i == schema.pk_col {
                            // ⭐ compat: SET pk = pk (RHS 同列引用) — 同值 no-op, 跳过
                            // (PG 允许同值更新; 真实改 pk 值仍拒绝)
                            if let SqlValue::ColRef(r) = v
                                && r.eq_ignore_ascii_case(name)
                            {
                                continue;
                            }
                            conn.resp_complete(
                                seq,
                                sql_err_bytes(conn.proto, "cannot UPDATE PRIMARY KEY column"),
                            );
                            return;
                        }
                        // ⭐ F65 v1 边界: 不支持 UPDATE 全局唯一列 (需輁坑; 未实现)
                        if schema
                            .indexes
                            .iter()
                            .any(|idx| idx.col == i && idx.unique && idx.global)
                        {
                            conn.resp_complete(
                                seq,
                                sql_err_bytes(
                                    conn.proto,
                                    "UPDATE of GLOBAL UNIQUE column not supported (v1); DELETE + INSERT instead",
                                ),
                            );
                            return;
                        }
                        // ⭐ PG 兼容: 表达式 SET → SetVal::Expr (shard 端对旧行求值)
                        if let SqlValue::Expr(e) = v {
                            match sql_update_expr_to_row(&schema, e) {
                                Ok(re) => {
                                    out.push((i, storage::row::SetVal::Expr(re)));
                                    continue;
                                }
                                Err(ee) => {
                                    conn.resp_complete(seq, sql_err_bytes(conn.proto, &ee));
                                    return;
                                }
                            }
                        }
                        let cv = match sql_to_col(schema.columns[i as usize].ty, v) {
                            Ok(c) => c,
                            Err(e) => {
                                conn.resp_complete(seq, sql_err_bytes(conn.proto, &e));
                                return;
                            }
                        };
                        if cv == ColValue::Null && !schema.columns[i as usize].nullable {
                            conn.resp_complete(
                                seq,
                                sql_err_bytes(conn.proto, &format!("column '{name}' is NOT NULL")),
                            );
                            return;
                        }
                        out.push((i, storage::row::SetVal::Val(cv)));
                    }
                    // ⭐ compat: 全部 set 为 pk 同值 (SET pk = pk) → no-op, 直接回 OK
                    if out.is_empty() {
                        conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
                        return;
                    }
                    (table, conds, SqlDmlAction::Update(out))
                }
                _ => unreachable!(),
            };
            match sql_plan_select(&schema, &conds) {
                Err(e) => conn.resp_complete(seq, sql_err_bytes(conn.proto, &e)),
                Ok(SqlPlan::PkGet { pk }) => {
                    // ⭐ 事务 v1 (F61): pk 等值 UPDATE/DELETE 截流进 write_set
                    // (affected 乐观估 1, 真实效果 commit 时定 — 文档化)
                    if conn.txn.is_some() {
                        let op = sql_dml_op(db, &table, pk, &action);
                        match txn_buffer_op(conn, op) {
                            Ok(()) => conn.resp_complete(seq, sql_ok_bytes(conn.proto, 1)),
                            Err(e) => conn.resp_complete(seq, sql_err_bytes(conn.proto, &e)),
                        }
                        return;
                    }
                    // pk 等值 → 单 shard 原子, 直发 phase2
                    // ⭐ FK 级联 (FMT_VER 8): 记录被删 pk (等值单发不走 Fire::Dml)
                    if matches!(action, SqlDmlAction::Delete) {
                        conn.cascade_pending
                            .insert(seq, ((*db).clone(), table.clone(), vec![pk.clone()]));
                    }
                    conn.sql_dml_agg.insert(
                        seq,
                        SqlDmlAgg {
                            remaining: 1,
                            affected: 0,
                            error: None,
                            drop_key: None,
                        },
                    );
                    let op = sql_dml_op(db, &table, pk, &action);
                    push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
                Ok(SqlPlan::Index {
                    iid,
                    lo,
                    hi,
                    limit_push: _,
                    eq_enc: _,
                    pk,
                }) => {
                    // 两阶段 phase1: 复用 SELECT 广播路径收全行 (残余过滤需行值),
                    // 完成点取 pk 发 phase2. limit 不下推 (DML 无 LIMIT).
                    conn.sql_select_agg.insert(
                        seq,
                        SqlSelectAgg {
                            remaining: num_shards,
                            error: None,
                            rows: Vec::new(),
                            schema: schema.clone(),
                            conds,
                            limit: None,
                            proj: Vec::new(),
                            cover: None,
                            unique_early: false, // DML 禁早停 (防同 seq 双 agg 并存)
                            done: false,
                            dml: Some(action),
                            dml_target: Some((db.clone(), table.clone())),
                            order: Vec::new(),
                            sorted: false,
                            offset: 0,
                            count: false,
                            agg_spec: None,
                            out_names: Vec::new(),
                            expr_proj: Vec::new(),
                            down_proj: Vec::new(),
                            plain_rows: Vec::new(),
                        },
                    );
                    let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
                    for sid in 0..num_shards {
                        // ⭐ PG 兼容 (范围查): 主键区间用 ScanFilteredRows(pk hint),
                        // 否则二级索引 IndexScan.
                        // ⭐ 修复 (2026-08): 原 ScanFiltered 返回 ProjRows (仅投影列值, 无
                        // pk/row_bytes), DML phase1 无法经 collect_dml_pks 提取 pk 执行 phase2,
                        // 导致主键范围 UPDATE/DELETE 报 "unexpected reply". 改用返回完整
                        // Rows (索引原值, pk, row_bytes) 的 ScanFilteredRows.
                        let op = if pk {
                            BatchOp::ScanFilteredRows {
                                db: db.clone(),
                                table: table_arc.clone(),
                                index_hint: Some(shard_manager::IndexHint {
                                    iid: 0,
                                    lo: lo.clone(),
                                    hi: hi.clone(),
                                    pk: true,
                                }),
                                limit: 0,
                            }
                        } else {
                            BatchOp::IndexScan {
                                db: db.clone(),
                                table: table_arc.clone(),
                                iid,
                                lo: lo.clone(),
                                hi: hi.clone(),
                                limit: 0,
                                with_rows: true,
                            }
                        };
                        push_task_grouped(
                            conn_id,
                            seq,
                            worker_id,
                            sid as u32,
                            sid,
                            op,
                            shard_inboxes,
                        );
                    }
                }
                // ⭐ S2: 无可用索引的 DML (含无 WHERE 全删/全改) → 全表扫 phase1
                // ⭐ M2: OR 并集对 DML 同样回退全表扫 phase1
                Ok(SqlPlan::FullScan) | Ok(SqlPlan::IndexUnion { .. }) => {
                    conn.sql_select_agg.insert(
                        seq,
                        SqlSelectAgg {
                            remaining: num_shards,
                            error: None,
                            rows: Vec::new(),
                            schema: schema.clone(),
                            conds,
                            limit: None,
                            proj: Vec::new(),
                            cover: None,
                            unique_early: false,
                            done: false,
                            dml: Some(action),
                            dml_target: Some((db.clone(), table.clone())),
                            order: Vec::new(),
                            sorted: false,
                            offset: 0,
                            count: false,
                            agg_spec: None,
                            out_names: Vec::new(),
                            expr_proj: Vec::new(),
                            down_proj: Vec::new(),
                            plain_rows: Vec::new(),
                        },
                    );
                    let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
                    for sid in 0..num_shards {
                        let op = BatchOp::TableScan {
                            db: db.clone(),
                            table: table_arc.clone(),
                            limit: 0,
                        };
                        push_task_grouped(
                            conn_id,
                            seq,
                            worker_id,
                            sid as u32,
                            sid,
                            op,
                            shard_inboxes,
                        );
                    }
                }
            }
        }
        SqlStmt::Select {
            table,
            items,
            mut conds,
            limit,
            order,
            offset,
            group_by,
            having,
            ..
        } => {
            // ⭐ 优化器 M1 (2026-08): 谓词归一 (NOT 下推/恒真恒假短路) 后再规划
            let (ncond, cond_false) = sql::normalize_pred_cond(&conds);
            conds = ncond;
            if cond_false {
                // 恒假谓词 → 直接返回空结果 (短路广播)
                let bin = conn.mysql_binary.remove(&seq);
                conn.resp_complete(
                    seq,
                    sql_rows_bytes(conn.proto, bin, &[("", storage::schema::ColType::I64)], &[]),
                );
                return;
            }
            // ⭐ G1/G2 (F63): 投影分型 — 纯列 / COUNT(*) 特例 (旧路径) /
            // 广义聚合 (分桶完成点)
            let has_agg = items
                .iter()
                .any(|i| matches!(i, sql::SelectItem::Agg { .. }));
            let count = has_agg
                && items.len() == 1
                && group_by.is_empty()
                && having.is_true()
                && order.is_empty()
                && matches!(
                    items[0],
                    sql::SelectItem::Agg {
                        func: sql::AggFn::Count,
                        arg: None,
                        ..
                    }
                );
            if (has_agg || !group_by.is_empty()) && !count {
                sql_run_agg_select(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    db,
                    shard_inboxes,
                    num_shards,
                    schema,
                    table,
                    items,
                    conds,
                    group_by,
                    having,
                    order,
                    limit,
                    offset,
                );
                return;
            }
            // ⭐ O1 + compat: 投影项解析 — 同步构建 (proj 列号, 输出名, 表达式投影).
            // Col → 直出列; Expr (JSONB j->'a') → base 列号进 proj + 绑定表达式
            // (渲染时逐行求值); 空 items (SELECT *) → 全列. 列序 = items 序.
            let mut proj: Vec<u16> = Vec::new();
            let mut out_names: Vec<Option<String>> = Vec::new();
            let mut expr_proj: Vec<Option<BoundExpr>> = Vec::new();
            let mut proj_err: Option<String> = None;
            for it in &items {
                match it {
                    sql::SelectItem::Col { name, alias } => {
                        match schema.col_by_name(name) {
                            Some(i) => proj.push(i),
                            None => {
                                proj_err = Some(format!("unknown column '{name}'"));
                                break;
                            }
                        }
                        out_names.push(alias.clone());
                        expr_proj.push(None);
                    }
                    sql::SelectItem::Expr { expr, alias } => {
                        let bound = match bind_scalar_expr(&schema, expr) {
                            Ok((b, _)) => b,
                            Err(e) => {
                                proj_err = Some(e);
                                break;
                            }
                        };
                        proj.push(bound_base_col(&bound));
                        out_names.push(alias.clone());
                        expr_proj.push(Some(bound));
                    }
                    // ⭐ G1: COUNT(*) 特例 (items 仅 Agg → proj 空走全列);
                    // ScalarFn 纯常量走 ScalarSelect, 与列混合时忽略 (v1 边界)
                    _ => {}
                }
            }
            if let Some(msg) = proj_err {
                conn.resp_complete(seq, sql_err_bytes(conn.proto, &msg));
                return;
            }
            if proj.is_empty() {
                // ⭐ compat: SELECT * — 排除隐藏 __rowid (自动主键表)
                proj = visible_cols(&schema);
            }
            // ⭐ S2: ORDER BY 列名 → 列号
            let mut order_cols: Vec<(u16, bool)> = Vec::with_capacity(order.len());
            for (name, desc) in &order {
                match schema.col_by_name(name) {
                    Some(i) => order_cols.push((i, *desc)),
                    None => {
                        conn.resp_complete(
                            seq,
                            sql_err_bytes(conn.proto, &format!("unknown column '{name}'")),
                        );
                        return;
                    }
                }
            }
            let offset = offset.unwrap_or(0);
            match sql_plan_select(&schema, &conds) {
                Err(e) => conn.resp_complete(seq, sql_err_bytes(conn.proto, &e)),
                Ok(SqlPlan::PkGet { pk }) => {
                    // ⭐ compat: 表达式投影 (JSONB) 不走点查单行路径 (v1 边界)
                    if expr_proj.iter().any(|o| o.is_some()) {
                        conn.resp_complete(
                            seq,
                            sql_err_bytes(
                                conn.proto,
                                "expression projections with point lookup are not supported (v1)",
                            ),
                        );
                        return;
                    }
                    // ⭐ 事务 v1 (F61): RYOW — pk 点查命中本事务 write_set 时
                    // 直接回缓冲内容 (INSERT 见新行 / DELETE 见空; UPDATE 直通
                    // 读已提交版本 — v1 文档化)
                    if let Some(txn) = conn.txn.as_ref() {
                        let tkey = (db.to_string(), table.clone(), pk.clone());
                        match resolve_ryow(txn, &tkey) {
                            Some(RyowState::Resolved(state)) => {
                                let bin = conn.mysql_binary.remove(&seq);
                                let bytes = match state {
                                    Some(values) if eval_pred(&schema, &values, &conds) => {
                                        if count {
                                            render_sql_count(conn.proto, bin, 1)
                                        } else {
                                            render_sql_rows(
                                                conn.proto,
                                                bin,
                                                &schema,
                                                &proj,
                                                &out_names,
                                                std::slice::from_ref(&values),
                                            )
                                        }
                                    }
                                    _ if count => render_sql_count(conn.proto, bin, 0),
                                    _ => render_sql_rows(
                                        conn.proto,
                                        bin,
                                        &schema,
                                        &proj,
                                        &out_names,
                                        &[],
                                    ),
                                };
                                conn.resp_complete(seq, bytes);
                                return;
                            }
                            Some(RyowState::NeedBase(overlay)) => {
                                let read_key = sql_read_key(conn, db, &table, &pk);
                                conn.sql_row_ctx.insert(
                                    seq,
                                    SqlRowCtx {
                                        schema,
                                        conds,
                                        proj,
                                        count,
                                        read_key,
                                        ryow_overlay: overlay,
                                        out_names: out_names.clone(),
                                        row: None,
                                        error: None,
                                    },
                                );
                                let op = BatchOp::RowGet {
                                    db: db.clone(),
                                    table: std::sync::Arc::from(table.as_str()),
                                    pk,
                                };
                                push_task(
                                    conn,
                                    conn_id,
                                    seq,
                                    worker_id,
                                    op,
                                    shard_inboxes,
                                    num_shards,
                                );
                                return;
                            }
                            None => {}
                        }
                    }
                    let read_key = sql_read_key(conn, db, &table, &pk);
                    conn.sql_row_ctx.insert(
                        seq,
                        SqlRowCtx {
                            schema,
                            conds,
                            proj,
                            count,
                            read_key,
                            ryow_overlay: Vec::new(),
                            out_names,
                            row: None,
                            error: None,
                        },
                    );
                    let op = BatchOp::RowGet {
                        db: db.clone(),
                        table: std::sync::Arc::from(table.as_str()),
                        pk,
                    };
                    push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
                // ⭐ M2: OR → 索引并集 — 每个分支一个 IndexScan, 合并到同一聚合 (残余过滤兼底)
                Ok(SqlPlan::IndexUnion { branches }) => {
                    // 每个分支广播 IndexScan; remaining = 分支数 × shard 数
                    let rem = branches.len() * num_shards;
                    conn.sql_select_agg.insert(
                        seq,
                        SqlSelectAgg {
                            remaining: rem,
                            error: None,
                            rows: Vec::new(),
                            schema: schema.clone(),
                            conds: conds.clone(),
                            limit,
                            proj,
                            cover: None,
                            unique_early: false,
                            done: false,
                            dml: None,
                            dml_target: None,
                            order: order_cols,
                            // 并集无序 (跨分支跨 shard) → 不能消排, worker 端照常排序
                            sorted: false,
                            offset,
                            count,
                            agg_spec: None,
                            out_names,
                            expr_proj,
                            down_proj: Vec::new(),
                            plain_rows: Vec::new(),
                        },
                    );
                    let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
                    for (ipos, lo, hi) in branches {
                        let idx = &schema.indexes[ipos as usize];
                        for sid in 0..num_shards {
                            let op = BatchOp::IndexScan {
                                db: db.clone(),
                                table: table_arc.clone(),
                                iid: idx.iid,
                                lo: lo.clone(),
                                hi: hi.clone(),
                                limit: 0,
                                with_rows: true,
                            };
                            push_task_grouped(
                                conn_id,
                                seq,
                                worker_id,
                                sid as u32,
                                sid,
                                op,
                                shard_inboxes,
                            );
                        }
                    }
                }
                // ⭐ S2: 全表扫 — 广播 TableScan + 全条件残余过滤
                Ok(SqlPlan::FullScan) => {
                    // ⭐ M2b: 排序消排 — TableScan 天然按 pk 升序返回 (BTree 遍历),
                    // ORDER BY pk ASC 时免 worker 端全量排序 (sorted = 消排成功).
                    let pk_sorted = !count
                        && order_cols.len() == 1
                        && !order_cols[0].1
                        && order_cols[0].0 == schema.pk_col;
                    // ⭐ P0-2: 投影下推 — 仅简单 SELECT (无排序/聚合/COUNT/DML/覆盖索引).
                    // 下推列集 = SELECT 投影 ∪ WHERE 列 (去重保序); 真子集时启用,
                    // shard 端 (ScanFiltered) 只回这些列, worker 端展开回全列过滤/渲染.
                    let order_empty = order_cols.is_empty();
                    let mut row_cols: Vec<u16> = Vec::new();
                    if !count && order_empty {
                        for &c in &proj {
                            if !row_cols.contains(&c) {
                                row_cols.push(c);
                            }
                        }
                        for l in conds.leaves() {
                            if let Some(ci) = schema.col_by_name(&l.col) {
                                if !row_cols.contains(&ci) {
                                    row_cols.push(ci);
                                }
                            }
                        }
                        // ⭐ P0-2: pk 列始终下推 — worker 端需按 pk 排序保持与 TableScan
                        // 全局 (val,pk) 序一致 (无 ORDER 默认 pk 序; LIMIT 无 ORDER = pk 序前 N).
                        if !row_cols.contains(&schema.pk_col) {
                            row_cols.push(schema.pk_col);
                        }
                    }
                    let downable = !count && order_empty && row_cols.len() < schema.columns.len();
                    // 下推 preds: 仅纯 AND 合取可转 ScanPred (值转换失败跳过该谓词)
                    let down_preds = if downable {
                        conds_to_scan_preds(&schema, &conds)
                    } else {
                        Vec::new()
                    };
                    // limit 下推: 无条件 (零残余过滤, shard 端取行即命中)
                    // 且 (无排序 或 排序已按 pk 消排); 下推额含 offset.
                    // (投影下推路径 downable 时 order 必空, 条件退化为 is_true)
                    let shard_limit =
                        if conds.is_true() && !count && (order_cols.is_empty() || pk_sorted) {
                            limit.map(|l| l + offset).unwrap_or(0)
                        } else {
                            0
                        };
                    conn.sql_select_agg.insert(
                        seq,
                        SqlSelectAgg {
                            remaining: num_shards,
                            error: None,
                            rows: Vec::new(),
                            schema,
                            conds,
                            limit,
                            proj,
                            cover: None,
                            unique_early: false,
                            done: false,
                            dml: None,
                            dml_target: None,
                            order: order_cols,
                            sorted: pk_sorted,
                            offset,
                            count,
                            agg_spec: None,
                            out_names,
                            expr_proj,
                            down_proj: if downable {
                                row_cols.clone()
                            } else {
                                Vec::new()
                            },
                            plain_rows: Vec::new(),
                        },
                    );
                    let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
                    for sid in 0..num_shards {
                        let op = if downable {
                            BatchOp::ScanFiltered {
                                db: db.clone(),
                                table: table_arc.clone(),
                                preds: down_preds.clone(),
                                proj: row_cols.clone(),
                                index_hint: None,
                                key_set_hint: None,
                                limit: shard_limit,
                            }
                        } else {
                            BatchOp::TableScan {
                                db: db.clone(),
                                table: table_arc.clone(),
                                limit: shard_limit,
                            }
                        };
                        push_task_grouped(
                            conn_id,
                            seq,
                            worker_id,
                            sid as u32,
                            sid,
                            op,
                            shard_inboxes,
                        );
                    }
                }
                Ok(SqlPlan::Index {
                    iid: _,
                    lo,
                    hi,
                    limit_push,
                    eq_enc: _,
                    pk: true,
                }) => {
                    // ⭐ PG 兼容 (范围查): 主键区间扫描 — 全 shard 广播 ScanFiltered
                    // (index_hint { pk: true } 走主键 B+Tree 区间). 无覆盖/路由剪枝
                    // (主键范围); 行经残余过滤 (preds 完整下推).
                    let pk_col = schema.pk_col;
                    let cover = (count || proj.iter().all(|&c| c == pk_col))
                        && conds
                            .leaves()
                            .iter()
                            .all(|c| schema.col_by_name(&c.col).is_some_and(|i| i == pk_col));
                    let shard_limit = if limit_push && !count {
                        limit.map(|l| l + offset).unwrap_or(0)
                    } else {
                        0
                    };
                    let scan_preds: Vec<shard_manager::ScanPred> = conds
                        .leaves()
                        .iter()
                        .filter_map(|c| {
                            let Some(ci) = schema.col_by_name(&c.col) else {
                                return None;
                            };
                            let sop = match c.op {
                                CmpOp::Eq => shard_manager::PredOp::Eq,
                                CmpOp::Gt => shard_manager::PredOp::Gt,
                                CmpOp::Ge => shard_manager::PredOp::Ge,
                                CmpOp::Lt => shard_manager::PredOp::Lt,
                                CmpOp::Le => shard_manager::PredOp::Le,
                                _ => return None,
                            };
                            let ty = schema.columns[ci as usize].ty;
                            let v = sql_to_col(ty, &c.val).ok()?;
                            Some(shard_manager::ScanPred {
                                col: ci,
                                op: sop,
                                val: v,
                                set: Vec::new(),
                            })
                        })
                        .collect();
                    // 投影: 覆盖时投影列; 否则全列 (worker 端再按 proj 取)
                    let down_proj: Vec<u16> = if cover {
                        proj.clone()
                    } else {
                        (0..schema.columns.len() as u16).collect()
                    };
                    conn.sql_select_agg.insert(
                        seq,
                        SqlSelectAgg {
                            remaining: num_shards,
                            error: None,
                            rows: Vec::new(),
                            schema: schema.clone(),
                            conds,
                            limit,
                            proj,
                            cover: cover.then_some((pk_col, pk_col)),
                            unique_early: false,
                            done: false,
                            dml: None,
                            dml_target: None,
                            order: order_cols,
                            sorted: false,
                            offset,
                            count,
                            agg_spec: None,
                            out_names,
                            expr_proj,
                            down_proj: down_proj.clone(),
                            plain_rows: Vec::new(),
                        },
                    );
                    let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
                    for sid in 0..num_shards {
                        let op = BatchOp::ScanFiltered {
                            db: db.clone(),
                            table: table_arc.clone(),
                            preds: scan_preds.clone(),
                            proj: down_proj.clone(),
                            index_hint: Some(shard_manager::IndexHint {
                                iid: 0,
                                lo: lo.clone(),
                                hi: hi.clone(),
                                pk: true,
                            }),
                            key_set_hint: None,
                            limit: shard_limit as u32,
                        };
                        push_task_grouped(
                            conn_id,
                            seq,
                            worker_id,
                            sid as u32,
                            sid,
                            op,
                            shard_inboxes,
                        );
                    }
                }
                Ok(SqlPlan::Index {
                    iid,
                    lo,
                    hi,
                    limit_push,
                    eq_enc,
                    pk: false,
                }) => {
                    // ⭐ O1: 覆盖判定 — 投影∪条件∪排序列 ⊆ {索引列, pk 列} → 免回表
                    let idx_col = schema
                        .indexes
                        .iter()
                        .find(|i| i.iid == iid)
                        .map(|i| i.col)
                        .expect("plan 产出的 iid 必在 schema");
                    // ⭐ M2b: 排序消排 — ORDER BY 单列 ASC 且 == 索引列 → 索引序
                    // (val,pk 升序) 即排序序, worker 端免 sql_order_cmp 全量排序.
                    let sorted = !count
                        && order_cols.len() == 1
                        && !order_cols[0].1
                        && order_cols[0].0 == idx_col;
                    // limit 下推: 仅当条件可被闭界完全表达 (零残余过滤, 下推行必命中)
                    // 且 (无排序 或 排序已消排); 下推额含 offset.
                    let shard_limit = if limit_push && !count && (order_cols.is_empty() || sorted) {
                        limit.map(|l| l + offset).unwrap_or(0)
                    } else {
                        0
                    };
                    let pk_col = schema.pk_col;
                    let in_cover = |c: u16| c == idx_col || c == pk_col;
                    let cover = (count || proj.iter().all(|&c| in_cover(c)))
                        && order_cols.iter().all(|&(c, _)| in_cover(c))
                        && conds
                            .leaves()
                            .iter()
                            .all(|c| schema.col_by_name(&c.col).is_some_and(in_cover));
                    // ⭐ W2 → ORM-B2: 等值查询 + created_here 表 → 进程级路由缓存
                    // 候选剪枝 (Arc 克隆锁外读 bloom; 无 entry / 范围查询 → 广播)
                    let candidates: Vec<usize> = {
                        use std::sync::atomic::Ordering::Relaxed;
                        let sh = &conn.sql_shared;
                        let entry = eq_enc.as_ref().and_then(|_| {
                            sh.routes
                                .read()
                                .unwrap()
                                .get(&(db.to_string(), table.clone(), iid))
                                .cloned()
                        });
                        match (eq_enc.as_ref(), entry) {
                            (Some(enc), Some(blooms)) => {
                                let c: Vec<usize> = (0..num_shards)
                                    .filter(|&s| blooms[s].may_contain(enc))
                                    .collect();
                                if c.is_empty() {
                                    sh.route_bypassed.fetch_add(1, Relaxed);
                                } else if c.len() < num_shards {
                                    sh.route_pruned.fetch_add(1, Relaxed);
                                }
                                c
                            }
                            _ => (0..num_shards).collect(),
                        }
                    };
                    if candidates.is_empty() {
                        // 零任务短路: 值从未插入过 (bloom 无假阴性保证)
                        let bin = conn.mysql_binary.remove(&seq);
                        let bytes = if count {
                            render_sql_count(conn.proto, bin, 0)
                        } else {
                            render_sql_rows(conn.proto, bin, &schema, &proj, &out_names, &[])
                        };
                        conn.resp_complete(seq, bytes);
                        return;
                    }
                    conn.sql_select_agg.insert(
                        seq,
                        SqlSelectAgg {
                            remaining: candidates.len(),
                            error: None,
                            rows: Vec::new(),
                            schema: schema.clone(),
                            conds,
                            limit,
                            proj,
                            cover: cover.then_some((idx_col, pk_col)),
                            // ⭐ O3: unique 索引等值 → 首个非空回包即回复
                            // (⭐ S2: 排序/offset/count 与单行早停正交, 保持启用)
                            unique_early: eq_enc.is_some()
                                && schema.indexes.iter().any(|i| i.iid == iid && i.unique),
                            done: false,
                            dml: None,
                            dml_target: None,
                            order: order_cols,
                            sorted,
                            offset,
                            count,
                            agg_spec: None,
                            out_names,
                            expr_proj,
                            down_proj: Vec::new(),
                            plain_rows: Vec::new(),
                        },
                    );
                    let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
                    for sid in candidates {
                        let op = BatchOp::IndexScan {
                            db: db.clone(),
                            table: table_arc.clone(),
                            iid,
                            lo: lo.clone(),
                            hi: hi.clone(),
                            limit: shard_limit,
                            with_rows: !cover, // ⭐ O1: 覆盖 → shard 免回表
                        };
                        push_task_grouped(
                            conn_id,
                            seq,
                            worker_id,
                            sid as u32,
                            sid,
                            op,
                            shard_inboxes,
                        );
                    }
                }
            }
        }
        SqlStmt::CreateTable { .. } => unreachable!("CREATE 在 sql_dispatch_stmt 处理"),
        SqlStmt::DropTable { .. } => unreachable!("DROP 在 sql_dispatch_stmt 处理"),
        // ⭐ compat: 独立 CREATE INDEX / PG 专有 DDL 吞掉 (本不应进入 DML 执行器)
        SqlStmt::CreateIndex { .. } | SqlStmt::DdlStub => {
            conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
        }
        // ⭐ F79: ALTER TABLE ADD COLUMN — 基于旧 schema (参数) 合成新 schema 并广播 SetSchemaOp
        // ⭐ compat: ALTER TABLE DROP COLUMN — 标记删除 (列号/布局/版本不变, 存量零重写)
        SqlStmt::AlterTable {
            table,
            add,
            drop,
            if_not_exists,
        } => {
            // ⭐ compat: DROP COLUMN
            if let Some(drop_col) = drop {
                let Some(col_idx) = schema.col_by_name(&drop_col) else {
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(conn.proto, &format!("unknown column '{drop_col}'")),
                    );
                    return;
                };
                let new_schema = match schema.with_dropped_column(col_idx) {
                    Ok(s) => s,
                    Err(_) => {
                        conn.resp_complete(seq, sql_err_bytes(conn.proto, "drop column failed"));
                        return;
                    }
                };
                let bytes = new_schema.encode();
                let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
                conn.sql_ddl_agg.insert(
                    seq,
                    SqlDdlAgg {
                        remaining: num_shards,
                        error: None,
                        key: (db.to_string(), table),
                        schema: std::sync::Arc::new(new_schema),
                        alter: true,
                    },
                );
                for sid in 0..num_shards {
                    let op = BatchOp::SetSchemaOp {
                        db: db.clone(),
                        table: table_arc.clone(),
                        bytes: bytes.clone(),
                    };
                    push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
                }
                return;
            }
            let add = match add {
                Some(a) => a,
                None => {
                    conn.resp_complete(seq, sql_err_bytes(conn.proto, "ALTER with no action"));
                    return;
                }
            };
            // ⭐ compat: ADD COLUMN IF NOT EXISTS — 列已存在时静默跳过
            if if_not_exists && schema.col_by_name(&add.name).is_some() {
                conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
                return;
            }
            if schema.col_by_name(&add.name).is_some() {
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, &format!("duplicate column name '{}'", add.name)),
                );
                return;
            }
            let new_schema = match schema.with_added_column(add) {
                Ok(s) => s,
                Err(_) => {
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(conn.proto, "too many ALTER TABLE versions (v1 limit)"),
                    );
                    return;
                }
            };
            let bytes = new_schema.encode();
            let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
            conn.sql_ddl_agg.insert(
                seq,
                SqlDdlAgg {
                    remaining: num_shards,
                    error: None,
                    key: (db.to_string(), table),
                    schema: std::sync::Arc::new(new_schema),
                    alter: true,
                },
            );
            for sid in 0..num_shards {
                let op = BatchOp::SetSchemaOp {
                    db: db.clone(),
                    table: table_arc.clone(),
                    bytes: bytes.clone(),
                };
                push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
            }
        }
        // SQL 行表显式开放/关闭 RESP Hash 适配。仅更新 schema 元数据并按 DDL
        // 广播，现有行布局、索引和 KV 表路径均不受影响。
        SqlStmt::SetRespRowAdapter { table, enabled } => {
            let new_schema = schema.with_resp_row_adapter(enabled);
            let bytes = new_schema.encode();
            let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
            conn.sql_ddl_agg.insert(
                seq,
                SqlDdlAgg {
                    remaining: num_shards,
                    error: None,
                    key: (db.to_string(), table),
                    schema: std::sync::Arc::new(new_schema),
                    alter: true,
                },
            );
            for sid in 0..num_shards {
                let op = BatchOp::SetSchemaOp {
                    db: db.clone(),
                    table: table_arc.clone(),
                    bytes: bytes.clone(),
                };
                push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
            }
        }
        SqlStmt::SelectDerived { .. } => unreachable!("派生表在 sql_dispatch_stmt 处理"),
        SqlStmt::Begin { .. }
        | SqlStmt::Commit
        | SqlStmt::Rollback
        | SqlStmt::SetTransaction { .. }
        | SqlStmt::Savepoint { .. }
        | SqlStmt::RollbackTo { .. }
        | SqlStmt::Release { .. } => {
            unreachable!("事务语句在 sql_dispatch_stmt 处理")
        }
        SqlStmt::Use { .. }
        | SqlStmt::SetStub
        | SqlStmt::VersionStub
        | SqlStmt::DatabaseStub
        | SqlStmt::SystemQuery { .. }
        | SqlStmt::SystemVarStub { .. }
        | SqlStmt::ExistsStub { .. }
        | SqlStmt::CreateDb { .. }
        | SqlStmt::SelectJoin { .. } => {
            unreachable!("工具命令在 sql_dispatch_stmt 处理")
        }
        // ⭐ S3: DESCRIBE — schema 本地渲染 (Field/Type/Null/Key); 跳过已删列
        SqlStmt::Describe { .. } => {
            let mut rows: Vec<Vec<ColValue>> = Vec::new();
            for (i, col) in schema.columns.iter().enumerate() {
                if schema.dropped.contains(&(i as u16)) {
                    continue;
                }
                let ty = coltype_sql_name(col.ty);
                let key = if i as u16 == schema.pk_col {
                    "PRI"
                } else if let Some(idx) = schema.indexes.iter().find(|x| x.col == i as u16) {
                    if idx.unique { "UNI" } else { "MUL" }
                } else {
                    ""
                };
                rows.push(vec![
                    ColValue::Bytes(col.name.as_bytes().to_vec()),
                    ColValue::Bytes(ty.as_bytes().to_vec()),
                    ColValue::Bytes(if col.nullable {
                        b"YES".to_vec()
                    } else {
                        b"NO".to_vec()
                    }),
                    ColValue::Bytes(key.as_bytes().to_vec()),
                ]);
            }
            let cols: [(&str, ColType); 4] = [
                ("Field", ColType::Str),
                ("Type", ColType::Str),
                ("Null", ColType::Str),
                ("Key", ColType::Str),
            ];
            let bin = conn.mysql_binary.remove(&seq);
            conn.resp_complete(seq, sql_rows_bytes(conn.proto, bin, &cols, &rows));
        }
    }
}

/// SELECT 访问路径选择 (worker 过滤器核心):
/// 1. pk 等值 → PkGet;
/// 2. 多索引计分选择 (等值 > 范围 > IN, 界最紧者胜) → Index (界下推);
/// 3. 无可用索引 → FullScan (残余过滤兜底).
///
/// ⭐ 优化器增强 (2026-08, M1): 从"首个命中索引"升级为"计分最优索引":
/// 等值命中 +3 / 范围 +2 / IN +1, 得分最高者胜; 平局取靠前 (确定性)。
/// ⭐ M3-3 (代价): IN 集合大小阈值 — 超过则选择性过低, 不走索引 (全扫 + 残余).
pub(crate) const IN_INDEX_MAX_SET: usize = 32;
