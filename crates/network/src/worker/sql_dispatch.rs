//! SQL 语句分派 / 规划 / JOIN / UNIQUE / 系统查询 / DML 执行.
//! 从 worker/mod.rs 拆分 (2026-08) — 核心 SQL 执行路径.

use super::*;

pub(crate) fn sql_dispatch_stmt(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    stmt: SqlStmt,
) {
    crate::metrics::SQL_QUERIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // ⭐ ORM-B2: DDL epoch 检查 — DROP/重建后本 worker 陈旧 schema 缓存整体
    // 失效 (一次 relaxed load 热路径; DDL 低频, 全量清空重拉可接受)
    {
        let ep = conn.sql_shared.ddl_epoch.load(std::sync::atomic::Ordering::Acquire);
        let mut cache = conn.sql_cache.borrow_mut();
        if cache.local_epoch != ep {
            cache.schemas.clear();
            cache.local_epoch = ep;
        }
    }
    match stmt {
        // ⭐ 事务 v1/v2: BEGIN / COMMIT / ROLLBACK / SAVEPOINT (conn 层状态机)
        SqlStmt::Begin { iso, read_only } => {
            if conn.txn.is_some() {
                // PG 行为: 警告+忽略 (不重置已有缓冲)
            } else {
                conn.txn = Some(TxnState::new(
                    iso.unwrap_or(conn.default_iso),
                    read_only.unwrap_or(conn.default_ro),
                ));
                conn.txn_failed = false;
            }
            conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
        }
        // ⭐ v2 (F62): SET [SESSION] TRANSACTION — session 改连接默认,
        // 否则改当前事务 (非事务中也落连接默认 — MySQL "下一个事务" 近似)
        SqlStmt::SetTransaction { iso, read_only, session } => {
            if !session && let Some(txn) = conn.txn.as_mut() {
                if let Some(i) = iso {
                    txn.iso = i;
                }
                if let Some(ro) = read_only {
                    txn.read_only = ro;
                }
            } else {
                if let Some(i) = iso {
                    conn.default_iso = i;
                }
                if let Some(ro) = read_only {
                    conn.default_ro = ro;
                }
            }
            conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
        }
        SqlStmt::Rollback => {
            conn.txn = None;
            conn.txn_failed = false;
            conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
        }
        // ⭐ v2 (F62): ROLLBACK TO — E 态下允许 (SQLAlchemy/psycopg 靠它恢复
        // aborted 子事务), 成功后清 failed 位
        SqlStmt::RollbackTo { name } => {
            let Some(txn) = conn.txn.as_mut() else {
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, &format!("savepoint \"{name}\" does not exist")),
                );
                return;
            };
            let Some(pos) = txn.savepoints.iter().rposition(|(n, _)| n == &name) else {
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, &format!("savepoint \"{name}\" does not exist")),
                );
                return;
            };
            let watermark = txn.savepoints[pos].1;
            txn.ops.truncate(watermark);
            txn.savepoints.truncate(pos + 1); // 保留自身 (PG 语义可重复回滚)
            // index 重建 (截断后下标失效)
            txn.index.clear();
            let entries: Vec<_> = txn
                .ops
                .iter()
                .enumerate()
                .map(|(i, op)| {
                    let (d, t, k) = op.locator();
                    ((d.to_string(), t.to_string(), k.to_vec()), i)
                })
                .collect();
            txn.index.extend(entries);
            conn.txn_failed = false;
            conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
        }
        SqlStmt::Commit => {
            let failed = conn.txn_failed;
            conn.txn_failed = false;
            match conn.txn.take() {
                // failed 事务的 COMMIT = 回滚 (PG 语义); 无事务/空事务 no-op
                None => conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0)),
                Some(_) if failed => conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0)),
                Some(txn) if txn.ops.is_empty() => {
                    // 纯读事务: 序列化点可取 BEGIN 时刻, 无需验证直接成功
                    conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
                }
                Some(txn) => {
                    // 按 shard 分组 → 每 shard 一个 TxnApply 原子批;
                    // ⭐ v2: read_set 同样按 pk 路由分组 (验证与写同批原子)
                    let mut groups: HashMap<usize, Vec<BatchOp>> = HashMap::new();
                    for op in txn.ops {
                        let sid = hash_route_op(&op, num_shards);
                        groups.entry(sid).or_default().push(op);
                    }
                    let mut checks: HashMap<usize, Vec<shard_manager::request::ReadCheck>> =
                        HashMap::new();
                    for ((d, t, pk), fp) in txn.read_set {
                        let sid = hash_route_key(&d, &t, &pk, num_shards);
                        checks.entry(sid).or_default().push(
                            shard_manager::request::ReadCheck { db: d, table: t, pk, fp },
                        );
                    }
                    // 并集 shard: 有写或有验证项都发 (纯验证批 ops 空)
                    let mut sids: Vec<usize> = groups.keys().chain(checks.keys()).copied().collect();
                    sids.sort_unstable();
                    sids.dedup();
                    conn.sql_txn_agg.insert(
                        seq,
                        SqlTxnAgg { remaining: sids.len(), applied: 0, error: None },
                    );
                    for (gidx, sid) in sids.into_iter().enumerate() {
                        push_task_grouped(
                            conn_id,
                            seq,
                            worker_id,
                            gidx as u32,
                            sid,
                            BatchOp::TxnApply {
                                ops: groups.remove(&sid).unwrap_or_default(),
                                read_set: checks.remove(&sid).unwrap_or_default(),
                            },
                            shard_inboxes,
                        );
                    }
                }
            }
        }
        // ⭐ 事务 v1 (F61): failed 事务拒后续 (PG 25P02 语义; MySQL 门面
        // 不置位故此臂仅 PG 命中; ROLLBACK TO 已在上方放行)
        _ if conn.txn_failed => {
            conn.resp_complete(
                seq,
                sql_err_bytes(
                    conn.proto,
                    "current transaction is aborted, commands ignored until end of transaction block",
                ),
            );
        }
        // ⭐ v2 (F62): SAVEPOINT / RELEASE (E 态被上方拦截 — PG 语义)
        SqlStmt::Savepoint { name } => match conn.txn.as_mut() {
            Some(txn) => {
                let watermark = txn.ops.len();
                txn.savepoints.push((name, watermark));
                conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
            }
            None => conn.resp_complete(
                seq,
                sql_err_bytes(conn.proto, "SAVEPOINT can only be used in transaction blocks"),
            ),
        },
        SqlStmt::Release { name } => match conn.txn.as_mut() {
            Some(txn) => match txn.savepoints.iter().rposition(|(n, _)| n == &name) {
                Some(pos) => {
                    txn.savepoints.remove(pos);
                    conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
                }
                None => conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, &format!("savepoint \"{name}\" does not exist")),
                ),
            },
            None => conn.resp_complete(
                seq,
                sql_err_bytes(conn.proto, "RELEASE can only be used in transaction blocks"),
            ),
        },
        // ⭐ v2 (F62): READ ONLY 事务拒写 (25006)
        SqlStmt::Insert { .. } | SqlStmt::Update { .. } | SqlStmt::Delete { .. }
            if conn.txn.as_ref().is_some_and(|t| t.read_only) =>
        {
            conn.resp_complete(
                seq,
                sql_err_bytes(
                    conn.proto,
                    "cannot execute write statement in a read-only transaction",
                ),
            );
        }
        // ⭐ 事务 v1 (F61): DDL 在事务中拒绝 (避免与 2PC 交叉)
        SqlStmt::CreateTable { .. } | SqlStmt::DropTable { .. } | SqlStmt::AlterTable { .. }
            if conn.txn.is_some() =>
        {
            conn.resp_complete(
                seq,
                sql_err_bytes(conn.proto, "DDL is not allowed inside a transaction"),
            );
        }
        // ⭐ compat: 独立 CREATE INDEX — 表内联索引已在建表时支持, 这里 v1 吞掉返回 OK
        SqlStmt::CreateIndex { .. } => {
            conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
        }
        // ⭐ compat: PG 专有 DDL 吞掉 (EXTENSION/FUNCTION/TRIGGER/SEQUENCE/DROP TRIGGER/INDEX)
        SqlStmt::DdlStub => {
            conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
        }
        // ⭐ S3: 工具命令 (worker 本地, 零任务)
        SqlStmt::SetStub => {
            conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
        }
        SqlStmt::VersionStub => {
            let ver: &[u8] = if conn.proto == ProtocolKind::Pg {
                b"PostgreSQL 16.0 (NexusDB)"
            } else {
                b"8.0.35-NexusDB"
            };
            let bin = conn.mysql_binary.remove(&seq);
            conn.resp_complete(
                seq,
                sql_rows_bytes(
                    conn.proto,
                    bin,
                    &[("version()", ColType::Str)],
                    &[vec![ColValue::Bytes(ver.to_vec())]],
                ),
            );
        }
        // ⭐ S5: SELECT DATABASE() — 当前库名单行
        SqlStmt::DatabaseStub => {
            let bin = conn.mysql_binary.remove(&seq);
            conn.resp_complete(
                seq,
                sql_rows_bytes(
                    conn.proto,
                    bin,
                    &[("DATABASE()", ColType::Str)],
                    &[vec![ColValue::Bytes(db.as_bytes().to_vec())]],
                ),
            );
        }
        // ⭐ F66: `SELECT @@var` 系统变量 — 回合理值单行 (SQLAlchemy 初始化)
        SqlStmt::SystemVarStub { vars } => {
            let bin = conn.mysql_binary.remove(&seq);
            let vals: Vec<(String, String)> = vars
                .iter()
                .map(|v| {
                    let key = v.rsplit('.').next().unwrap_or(v).to_ascii_lowercase();
                    let val = match key.as_str() {
                        "transaction_isolation" | "tx_isolation" => "READ-COMMITTED",
                        "version" => "8.0.0-nexusdb",
                        "version_comment" => "NexusDB",
                        "sql_mode" => "",
                        "lower_case_table_names" => "0",
                        "autocommit" => "1",
                        "max_allowed_packet" => "16777216",
                        "character_set_client" | "character_set_connection"
                        | "character_set_results" => "utf8mb4",
                        _ => "",
                    };
                    (format!("@@{v}"), val.to_string())
                })
                .collect();
            let cols: Vec<(&str, ColType)> =
                vals.iter().map(|(n, _)| (n.as_str(), ColType::Str)).collect();
            let row: Vec<ColValue> =
                vals.iter().map(|(_, val)| ColValue::Bytes(val.as_bytes().to_vec())).collect();
            conn.resp_complete(seq, sql_rows_bytes(conn.proto, bin, &cols, &[row]));
        }
        // ⭐ F66: 系统表查询 (information_schema / pg_catalog 虚拟表)
        SqlStmt::SystemQuery { catalog, table, cols, conds, order, limit, offset } => {
            let spec = SysQuerySpec { catalog, table, cols, conds, order, limit, offset };
            // 纯 db 列表的虚拟表 (schemata / pg_namespace) → 零任务直接合成;
            // 需表/列元数据的 → 发 CatalogDump 挂起
            if spec.needs_catalog() {
                conn.sql_sysq.insert(seq, spec);
                let op = BatchOp::CatalogDump { db: db.clone() };
                let sid = hash_route_key(db, "", &[], num_shards);
                push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
            } else {
                let dbs: Vec<String> =
                    db_view.all_names().iter().map(|s| s.to_string()).collect();
                // default 库隐式不入 resolver — 补入
                let mut dbs = dbs;
                if !dbs.iter().any(|d| d.as_str() == default_db.as_ref()) {
                    dbs.push(default_db.to_string());
                }
                let bin = conn.mysql_binary.remove(&seq);
                let bytes = sysq_render_dblist(conn.proto, bin, &spec, &dbs);
                conn.resp_complete(seq, bytes);
            }
        }
        SqlStmt::Use { db: name } => {
            // 校验存在 (default 库隐式不入 resolver, 特判)
            if name.as_str() == default_db.as_ref() || db_view.id_of(&name).is_some() {
                conn.current_db = std::sync::Arc::from(name.as_str());
                conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
            } else {
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, &format!("Unknown database '{name}'")),
                );
            }
        }
        SqlStmt::CreateTable { table, schema, if_not_exists } => {
            // ⭐ IF NOT EXISTS: 表已存在 → 静默跳过 (直接 OK, 不广播)
            if if_not_exists {
                let key = (db.to_string(), table.clone());
                if conn.sql_cache.borrow().schemas.contains_key(&key) {
                    conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
                    return;
                }
            }
            let bytes = schema.encode();
            let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
            conn.sql_ddl_agg.insert(
                seq,
                SqlDdlAgg {
                    remaining: num_shards,
                    error: None,
                    key: (db.to_string(), table),
                    schema: std::sync::Arc::new(schema),
                    alter: false,
                },
            );
            // 数据面广播 (worker 不持控制面); shard 端惰性建表 + set_schema 幂等
            for sid in 0..num_shards {
                let op = BatchOp::SetSchemaOp {
                    db: db.clone(),
                    table: table_arc.clone(),
                    bytes: bytes.clone(),
                };
                push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
            }
        }
        SqlStmt::Insert { ref table, .. }
        | SqlStmt::Select { ref table, .. }
        | SqlStmt::Delete { ref table, .. }
        | SqlStmt::Update { ref table, .. }
        | SqlStmt::AlterTable { ref table, .. }
        | SqlStmt::Describe { ref table } => {
            // ⭐ F71: WHERE 子查询 — 先顺序跑内层折叠, 完后重跑外层 (仅 Select/Delete/Update)
            if matches!(
                stmt,
                SqlStmt::Select { .. } | SqlStmt::Delete { .. } | SqlStmt::Update { .. }
            ) && sql_subq_start(
                conn, conn_id, seq, worker_id, db, default_db, db_view, shard_inboxes,
                num_shards, &stmt,
            ) {
                return;
            }
            let key = (db.to_string(), table.clone());
            // ⭐ W1: worker 级共享缓存 (borrow 局部化: 取 Arc 即还)
            let cached = conn.sql_cache.borrow().schemas.get(&key).cloned();
            if let Some(schema) = cached {
                sql_run_dml(conn, conn_id, seq, worker_id, db, shard_inboxes, num_shards, schema, stmt);
            } else {
                // schema miss: 挂起语句, 先拉 schema (GetSchemaOp 定向单 shard)
                let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
                let table_name = table.clone();
                conn.sql_pending.insert(seq, PendingSql { stmt, db: db.clone(), table: table_name });
                let op = BatchOp::GetSchemaOp { db: db.clone(), table: table_arc };
                push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
            }
        }
        // ⭐ F67 (JOIN): 两表 hash join — 建 ctx → 补 schema/gather 顺序启动
        SqlStmt::SelectJoin { from, from_inner, joins, items, conds, order, limit, offset } => {
            // ⭐ F75: 首表为派生表 → 先物化内层 (同 seq 完成点拦截), 完后 finish_derived 建 JOIN
            if let Some(inner) = from_inner {
                if !matches!(*inner, SqlStmt::Select { .. }) {
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(conn.proto, "derived-table inner must be a simple SELECT (v1)"),
                    );
                    return;
                }
                if let Some(p) = stmt_where_conds(&inner) {
                    let mut nested = Vec::new();
                    collect_pred_subq(p, &mut nested);
                    if !nested.is_empty() {
                        conn.resp_complete(
                            seq,
                            sql_err_bytes(conn.proto, "subquery inside derived table not supported (v1)"),
                        );
                        return;
                    }
                }
                let join_stmt = SqlStmt::SelectJoin {
                    from, from_inner: None, joins, items, conds, order, limit, offset,
                };
                conn.sql_derived.insert(seq, DerivedCtx::JoinFrom { db: db.clone(), join_stmt });
                sql_dispatch_stmt(
                    conn, conn_id, seq, worker_id, db, default_db, db_view, shard_inboxes,
                    num_shards, *inner,
                );
                return;
            }
            // 构建 tables 列表 (from + 各 join.table); schema 命中缓存则填
            let mut tables: Vec<JoinTable> = Vec::with_capacity(joins.len() + 1);
            for tr in std::iter::once(&from).chain(joins.iter().map(|j| &j.table)) {
                let schema = conn
                    .sql_cache
                    .borrow()
                    .schemas
                    .get(&(db.to_string(), tr.table.clone()))
                    .cloned();
                tables.push(JoinTable {
                    table: std::sync::Arc::from(tr.table.as_str()),
                    alias: tr.alias.clone(),
                    schema,
                    proj: Vec::new(),
                    rows: Vec::new(),
                    prefilled: false,
                });
            }
            let ctx = SqlJoinCtx {
                db: db.clone(),
                tables,
                joins,
                items,
                conds,
                order,
                limit,
                offset,
                phase: JoinPhase::Gather(0),
                remaining: 0,
            };
            conn.sql_join.insert(seq, ctx);
            sql_join_kickoff(conn, conn_id, seq, worker_id, shard_inboxes, num_shards);
        }
        // ⭐ F72: FROM 派生表 — 内层先物化 (同 seq 完成点拦截), 完后 finish_derived
        // 在 worker 内存执行外层 (过滤/投影/排序/截断; 不下推 shard)
        SqlStmt::SelectDerived { inner, alias, items, conds, order, limit, offset } => {
            // v1: 内层仅单表 SELECT (非 JOIN/系统表) — 否则绕过完成点拦截
            if !matches!(*inner, SqlStmt::Select { .. }) {
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, "derived-table inner must be a simple SELECT (v1)"),
                );
                return;
            }
            // v1: 内层不得再带 WHERE 子查询 (双层编排留后)
            if let Some(p) = stmt_where_conds(&inner) {
                let mut nested = Vec::new();
                collect_pred_subq(p, &mut nested);
                if !nested.is_empty() {
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(conn.proto, "subquery inside derived table not supported (v1)"),
                    );
                    return;
                }
            }
            conn.sql_derived.insert(seq, DerivedCtx::Standalone { alias, items, conds, order, limit, offset });
            sql_dispatch_stmt(
                conn, conn_id, seq, worker_id, db, default_db, db_view, shard_inboxes,
                num_shards, *inner,
            );
        }
        // ⭐ S1: DROP TABLE — 无需 schema, 数据面广播删表
        SqlStmt::DropTable { table } => {
            conn.sql_dml_agg.insert(
                seq,
                SqlDmlAgg {
                    remaining: num_shards,
                    affected: 0,
                    error: None,
                    drop_key: Some((db.to_string(), table.clone())),
                },
            );
            let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
            for sid in 0..num_shards {
                let op = BatchOp::DropTableOp { db: db.clone(), table: table_arc.clone() };
                push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
            }
        }
    }
}

/// ⭐ F68 (JOIN): 限定列 → (table_index, col_idx). 未知限定符/列/歧义 → Err.
pub(crate) fn sql_join_resolve(ctx: &SqlJoinCtx, qc: &QualCol) -> Result<(usize, u16), String> {
    match &qc.qualifier {
        Some(q) => {
            let ti = ctx
                .tables
                .iter()
                .position(|t| t.alias.eq_ignore_ascii_case(q))
                .ok_or_else(|| format!("unknown table qualifier '{q}'"))?;
            let sc = ctx.tables[ti].schema.as_ref().expect("schema ready");
            sc.col_by_name(&qc.col)
                .map(|i| (ti, i))
                .ok_or_else(|| format!("unknown column '{}.{}'", q, qc.col))
        }
        None => {
            let mut found: Option<(usize, u16)> = None;
            for (ti, t) in ctx.tables.iter().enumerate() {
                let sc = t.schema.as_ref().expect("schema ready");
                if let Some(i) = sc.col_by_name(&qc.col) {
                    if found.is_some() {
                        return Err(format!("ambiguous column '{}' (qualify it)", qc.col));
                    }
                    found = Some((ti, i));
                }
            }
            found.ok_or_else(|| format!("unknown column '{}'", qc.col))
        }
    }
}

/// ⭐ F68 (JOIN): ON 操作数解析 (未限定名优先前序表, 支持 USING 糖糖).
/// rt = 本次新表下标; 限定名 → 常规解析; 未限定 → tables[0..rt] 取最后一个, 否则 rt.
pub(crate) fn sql_join_resolve_on(ctx: &SqlJoinCtx, qc: &QualCol, rt: usize) -> Result<(usize, u16), String> {
    if qc.qualifier.is_some() {
        return sql_join_resolve(ctx, qc);
    }
    let mut found: Option<(usize, u16)> = None;
    for ti in 0..rt {
        let sc = ctx.tables[ti].schema.as_ref().expect("schema ready");
        if let Some(i) = sc.col_by_name(&qc.col) {
            found = Some((ti, i));
        }
    }
    if found.is_none() {
        let sc = ctx.tables[rt].schema.as_ref().expect("schema ready");
        if let Some(i) = sc.col_by_name(&qc.col) {
            found = Some((rt, i));
        }
    }
    found.ok_or_else(|| format!("unknown column '{}'", qc.col))
}

/// ⭐ F68 (JOIN): 规划 — 校验所有限定名 + 算每表下推投影列 (含 items/on/where/order 引用).
/// 返回每表 proj (items 空 `*` → 各表全列). 同时校验每个 ON 等值恰好引用本次新表.
pub(crate) fn sql_join_plan(ctx: &SqlJoinCtx) -> Result<Vec<Vec<u16>>, String> {
    let n = ctx.tables.len();
    let mut sets: Vec<std::collections::BTreeSet<u16>> = vec![Default::default(); n];
    // ON 键/残余 (并校验 Eq 引用新表)
    for (ji, jc) in ctx.joins.iter().enumerate() {
        let rt = ji + 1;
        for on in &jc.on {
            match on {
                sql::OnPred::Eq(l, r) => {
                    let (lt, li) = sql_join_resolve_on(ctx, l, rt)?;
                    let (rtt, ri) = sql_join_resolve_on(ctx, r, rt)?;
                    let one_new = (lt == rt) ^ (rtt == rt);
                    if !one_new {
                        return Err("JOIN ON equality must reference the joined table".into());
                    }
                    sets[lt].insert(li);
                    sets[rtt].insert(ri);
                }
                sql::OnPred::Cmp { left, right, .. } => {
                    let (lt, li) = sql_join_resolve_on(ctx, left, rt)?;
                    let (rt2, ri) = sql_join_resolve_on(ctx, right, rt)?;
                    sets[lt].insert(li);
                    sets[rt2].insert(ri);
                }
            }
        }
    }
    // 投影项
    if ctx.items.is_empty() {
        for (ti, t) in ctx.tables.iter().enumerate() {
            let sc = t.schema.as_ref().expect("schema ready");
            for i in 0..sc.columns.len() as u16 {
                sets[ti].insert(i);
            }
        }
    } else {
        for it in &ctx.items {
            let JoinItem::Col(qc) = it;
            let (ti, i) = sql_join_resolve(ctx, qc)?;
            sets[ti].insert(i);
        }
    }
    // WHERE / ORDER 引用列
    for c in ctx.conds.leaves() {
        let (ti, i) = sql_join_resolve(ctx, &c.col)?;
        sets[ti].insert(i);
    }
    for (qc, _) in &ctx.order {
        let (ti, i) = sql_join_resolve(ctx, qc)?;
        sets[ti].insert(i);
    }
    Ok(sets.into_iter().map(|s| s.into_iter().collect()).collect())
}

/// ⭐ F68 (JOIN): 启动/推进 — 补第一个缺失 schema, 否则规划并从表 0 开始 gather.
pub(crate) fn sql_join_kickoff(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    let need = {
        let c = conn.sql_join.get(&seq).expect("join ctx");
        c.tables.iter().position(|t| t.schema.is_none())
    };
    if let Some(idx) = need {
        let (db, table) = {
            let c = conn.sql_join.get_mut(&seq).unwrap();
            c.phase = JoinPhase::FetchSchema(idx);
            c.remaining = 1;
            (c.db.clone(), c.tables[idx].table.clone())
        };
        let sid = hash_route_key(&db, &table, &[], num_shards);
        let op = BatchOp::GetSchemaOp { db, table };
        push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
        return;
    }
    // schema 全就绪 → 规划
    let plan = sql_join_plan(conn.sql_join.get(&seq).expect("join ctx"));
    match plan {
        Err(e) => {
            conn.sql_join.remove(&seq);
            conn.mysql_binary.remove(&seq);
            conn.resp_complete(seq, sql_err_bytes(conn.proto, &e));
        }
        Ok(projs) => {
            let start = {
                let c = conn.sql_join.get_mut(&seq).unwrap();
                for (t, p) in c.tables.iter_mut().zip(projs) {
                    if t.prefilled {
                        // ⭐ F75: 预填表行已定宽 (全列) → proj 强制 identity, 不清空 rows
                        let ncols = t.schema.as_ref().unwrap().columns.len() as u16;
                        t.proj = (0..ncols).collect();
                    } else {
                        t.proj = p;
                    }
                }
                // ⭐ F75: 从第一个非预填表开始 gather (预填表 0 跳过)
                c.tables.iter().position(|t| !t.prefilled)
            };
            match start {
                Some(idx) => {
                    {
                        let c = conn.sql_join.get_mut(&seq).unwrap();
                        c.phase = JoinPhase::Gather(idx);
                        c.remaining = num_shards;
                        c.tables[idx].rows.clear();
                    }
                    sql_join_broadcast(conn, conn_id, seq, worker_id, shard_inboxes, num_shards, idx);
                }
                // 全部预填 (理论不可达: joins 非空) → 直接 finish
                None => sql_join_finish(conn, seq),
            }
        }
    }
}

/// ⭐ F68 (JOIN): 广播 tables[idx] 的 ScanFiltered (下推该表 WHERE 谓词).
/// 下推仅优化; finish 总会再残余过滤全 WHERE, 故对任何表下推均安全 (含外连接可空侧).
pub(crate) fn sql_join_broadcast(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    idx: usize,
) {
    let (db, table, preds, proj) = {
        let c = conn.sql_join.get(&seq).expect("join ctx");
        let t = &c.tables[idx];
        let schema = t.schema.as_ref().unwrap();
        let mut preds: Vec<shard_manager::ScanPred> = Vec::new();
        // ⭐ F69: 仅纯 AND 合取时下推 (含 OR/NOT → 空 preds 全扫, finish 递归残余保正确)
        for cond in c.conds.as_conjuncts().unwrap_or_default() {
            let Ok((ti, cidx)) = sql_join_resolve(c, &cond.col) else { continue };
            if ti != idx {
                continue;
            }
            let ty = schema.columns[cidx as usize].ty;
            let op = match cond.op {
                CmpOp::Eq => shard_manager::PredOp::Eq,
                CmpOp::Ne => shard_manager::PredOp::Ne,
                CmpOp::Gt => shard_manager::PredOp::Gt,
                CmpOp::Ge => shard_manager::PredOp::Ge,
                CmpOp::Lt => shard_manager::PredOp::Lt,
                CmpOp::Le => shard_manager::PredOp::Le,
                CmpOp::In => shard_manager::PredOp::In,
            };
            if cond.op == CmpOp::In {
                let set: Vec<ColValue> =
                    cond.set.iter().filter_map(|v| sql_to_col(ty, v).ok()).collect();
                if set.len() == cond.set.len() {
                    preds.push(shard_manager::ScanPred { col: cidx, op, val: ColValue::Null, set });
                }
            } else if let Ok(val) = sql_to_col(ty, &cond.val) {
                preds.push(shard_manager::ScanPred { col: cidx, op, val, set: Vec::new() });
            }
        }
        (c.db.clone(), t.table.clone(), preds, t.proj.clone())
    };
    // ⭐ F68: 索引驱动提示 — 该表任一可索引列的 Eq/范围谓词 → 范围扫 (Eq 优先)
    // ⭐ F70: key_set_hint 优先 (前序表 join 键集合 → 索引点查); 命中时不再用 index_hint
    let key_set_hint = sql_join_keyset_hint(conn.sql_join.get(&seq).expect("join ctx"), idx);
    let index_hint = if key_set_hint.is_some() {
        None
    } else {
        let c = conn.sql_join.get(&seq).expect("join ctx");
        let t = &c.tables[idx];
        let schema = t.schema.as_ref().unwrap();
        sql_join_index_hint(c, idx, schema)
    };
    for sid in 0..num_shards {
        let op = BatchOp::ScanFiltered {
            db: db.clone(),
            table: table.clone(),
            preds: preds.clone(),
            proj: proj.clone(),
            index_hint: index_hint.clone(),
            key_set_hint: key_set_hint.clone(),
            limit: 0,
        };
        push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
    }
}

/// ⭐ F70 (JOIN): 键集合下推决策 — idx>=1 且满足安全条件时, 从前序表抽取
/// ON 等值键值集合下推为索引点查. 启用条件:
/// - joins[idx-1].kind ∈ {Inner, Left} (RIGHT/FULL/CROSS 禁用: 语义不能丢未匹配行)
/// - 息含单个 OnPred::Eq (多列组合键 v1 跳过)
/// - Eq 一侧属 idx 表且该列有普通二级索引, 另一侧属前序表 ti<idx
/// - 前序键集合去重后 <= JOIN_KEYSET_MAX (超阈退回全表扫)
pub(crate) fn sql_join_keyset_hint(ctx: &SqlJoinCtx, idx: usize) -> Option<shard_manager::KeySetHint> {
    if idx == 0 {
        return None;
    }
    let jc = &ctx.joins[idx - 1];
    if !matches!(jc.kind, JoinKind::Inner | JoinKind::Left) {
        return None;
    }
    // 息含单个 Eq
    let eqs: Vec<&sql::OnPred> =
        jc.on.iter().filter(|o| matches!(o, sql::OnPred::Eq(..))).collect();
    if eqs.len() != 1 {
        return None;
    }
    let sql::OnPred::Eq(l, r) = eqs[0] else { return None };
    // resolve 两侧 → (表下标, 列号)
    let (lt, li) = sql_join_resolve_on(ctx, l, idx).ok()?;
    let (rt, ri) = sql_join_resolve_on(ctx, r, idx).ok()?;
    // 分辨新表侧 (idx) 与前序表侧 (ti<idx)
    let (new_col, prev_ti, prev_col) = if lt == idx && rt < idx {
        (li, rt, ri)
    } else if rt == idx && lt < idx {
        (ri, lt, li)
    } else {
        return None;
    };
    // 新表 join 列需有普通二级索引
    let schema = ctx.tables[idx].schema.as_ref()?;
    let iid = schema.indexes.iter().find(|i| i.col == new_col).map(|i| i.iid)?;
    // 前序表 prev_col 在其 proj 中的位置
    let prev_tab = &ctx.tables[prev_ti];
    let pos = prev_tab.proj.iter().position(|&c| c == prev_col)?;
    // 抽取去重键值 (跳 NULL); 超阈 → 退回
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut keys: Vec<ColValue> = Vec::new();
    for row in &prev_tab.rows {
        let cv = &row[pos];
        let Some(kb) = join_key(cv) else { continue }; // NULL 不入键集
        if seen.insert(kb) {
            keys.push(cv.clone());
            if keys.len() > JOIN_KEYSET_MAX {
                return None; // 超阈退回全表扫
            }
        }
    }
    Some(shard_manager::KeySetHint { iid, keys })
}

/// ⭐ F68 (JOIN): 为 tables[idx] 选一个可索引谓词产索引提示 (Eq 优先, 否则范围).
/// lo/hi 为过度近似闭界 (Gt/Lt 也用含界, 由残余 preds 精确); 无可用 → None.
pub(crate) fn sql_join_index_hint(
    ctx: &SqlJoinCtx,
    idx: usize,
    schema: &TableSchema,
) -> Option<shard_manager::IndexHint> {
    // 列号 → iid (仅取非全局普通二级索引即可)
    let iid_of = |col: u16| schema.indexes.iter().find(|i| i.col == col).map(|i| i.iid);
    let mut best: Option<shard_manager::IndexHint> = None;
    for cond in ctx.conds.as_conjuncts().unwrap_or_default() {
        let Ok((ti, cidx)) = sql_join_resolve(ctx, &cond.col) else { continue };
        if ti != idx {
            continue;
        }
        let Some(iid) = iid_of(cidx) else { continue };
        let ty = schema.columns[cidx as usize].ty;
        let Ok(v) = sql_to_col(ty, &cond.val) else { continue };
        match cond.op {
            CmpOp::Eq => {
                // Eq 最优: 直接定界返回
                return Some(shard_manager::IndexHint {
                    iid,
                    lo: Some(v.clone()),
                    hi: Some(v),
                });
            }
            CmpOp::Gt | CmpOp::Ge if best.is_none() => {
                best = Some(shard_manager::IndexHint { iid, lo: Some(v), hi: None });
            }
            CmpOp::Lt | CmpOp::Le if best.is_none() => {
                best = Some(shard_manager::IndexHint { iid, lo: None, hi: Some(v) });
            }
            _ => {}
        }
    }
    best
}

/// ⭐ F67 (JOIN): handle_resp 认领 — 按 phase 推进. 返回 true = 已处理此 seq.
pub(crate) fn sql_join_drive(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    result: &BatchResult,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) -> bool {
    if !conn.sql_join.contains_key(&seq) {
        return false;
    }
    // 错误: 直接终止
    if let BatchResult::Error(e) = result {
        let msg = e.clone();
        conn.sql_join.remove(&seq);
        conn.mysql_binary.remove(&seq);
        conn.resp_complete(seq, sql_err_bytes(conn.proto, &msg));
        return true;
    }
    let phase = conn.sql_join.get(&seq).unwrap().phase;
    match phase {
        JoinPhase::FetchSchema(idx) => {
            let bytes = match result {
                BatchResult::GetValue(Some(b)) => b.clone(),
                BatchResult::GetValue(None) => {
                    conn.sql_join.remove(&seq);
                    conn.mysql_binary.remove(&seq);
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(conn.proto, "table has no schema (not a SQL table)"),
                    );
                    return true;
                }
                _ => {
                    conn.sql_join.remove(&seq);
                    conn.mysql_binary.remove(&seq);
                    conn.resp_complete(seq, sql_err_bytes(conn.proto, "unexpected schema reply"));
                    return true;
                }
            };
            match TableSchema::decode(&bytes) {
                Ok(s) => {
                    let schema = std::sync::Arc::new(s);
                    let (db, table) = {
                        let c = conn.sql_join.get_mut(&seq).unwrap();
                        c.tables[idx].schema = Some(schema.clone());
                        (c.db.clone(), c.tables[idx].table.clone())
                    };
                    conn.sql_cache
                        .borrow_mut()
                        .schemas
                        .insert((db.to_string(), table.to_string()), schema);
                    // 继续补下一个或进 gather
                    sql_join_kickoff(conn, conn_id, seq, worker_id, shard_inboxes, num_shards);
                }
                Err(e) => {
                    conn.sql_join.remove(&seq);
                    conn.mysql_binary.remove(&seq);
                    conn.resp_complete(seq, sql_err_bytes(conn.proto, &format!("bad schema: {e}")));
                }
            }
            true
        }
        JoinPhase::Gather(idx) => {
            let rows = match result {
                BatchResult::ProjRows(r) => r.clone(),
                _ => Vec::new(),
            };
            let (done, overflow) = {
                let c = conn.sql_join.get_mut(&seq).unwrap();
                c.tables[idx].rows.extend(rows);
                c.remaining = c.remaining.saturating_sub(1);
                let of = c.tables[idx].rows.len() > JOIN_MAX_ROWS;
                (c.remaining == 0, of)
            };
            if overflow {
                conn.sql_join.remove(&seq);
                conn.mysql_binary.remove(&seq);
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, "JOIN input too large (row cap exceeded)"),
                );
                return true;
            }
            if done {
                let ntables = conn.sql_join.get(&seq).unwrap().tables.len();
                if idx + 1 < ntables {
                    {
                        let c = conn.sql_join.get_mut(&seq).unwrap();
                        c.phase = JoinPhase::Gather(idx + 1);
                        c.remaining = num_shards;
                        c.tables[idx + 1].rows.clear();
                    }
                    sql_join_broadcast(conn, conn_id, seq, worker_id, shard_inboxes, num_shards, idx + 1);
                } else {
                    sql_join_finish(conn, seq);
                }
            }
            true
        }
    }
}

/// ⭐ F68 (JOIN): 各表 gather 完成 → 左深迭代 hash join (右建表、左探测) +
/// 各 kind (Inner/Left/Right/Full/Cross) + ON 残余 + 残余 WHERE + 输出列 + ORDER/OFFSET/LIMIT.
pub(crate) fn sql_join_finish(conn: &mut ConnState, seq: u64) {
    let ctx = conn.sql_join.remove(&seq).expect("join ctx");
    let bin = conn.mysql_binary.remove(&seq);
    let n = ctx.tables.len();
    // 宽行列偏移: col_offset[t] = 表 t 列在宽行的起始; 表宽 = proj.len()
    let mut col_offset = vec![0usize; n + 1];
    for t in 0..n {
        col_offset[t + 1] = col_offset[t] + ctx.tables[t].proj.len();
    }
    let pos_in = |t: usize, cidx: u16| -> usize {
        ctx.tables[t].proj.iter().position(|&c| c == cidx).unwrap()
    };
    let wide_pos = |t: usize, cidx: u16| -> usize { col_offset[t] + pos_in(t, cidx) };

    // acc = 表 0 行 (宽度 = col_offset[1]); 逐 join 折叠
    let mut acc: Vec<Vec<ColValue>> = ctx.tables[0].rows.clone();
    for (ji, jc) in ctx.joins.iter().enumerate() {
        let rt = ji + 1;
        let acc_w = col_offset[rt];
        let right_pw = ctx.tables[rt].proj.len();
        // ON 等值键: (acc 宽位, right proj 位); ON 非等值残余: Cmp
        let mut eq_keys: Vec<(usize, usize)> = Vec::new();
        for on in &jc.on {
            if let sql::OnPred::Eq(l, r) = on {
                let (lt, li) = sql_join_resolve_on(&ctx, l, rt).unwrap();
                let (_rtt, ri) = sql_join_resolve_on(&ctx, r, rt).unwrap();
                if lt == rt {
                    // l 属新表, r 属 acc
                    eq_keys.push((wide_pos(_rtt, ri), pos_in(rt, li)));
                } else {
                    // l 属 acc, r 属新表
                    eq_keys.push((wide_pos(lt, li), pos_in(rt, ri)));
                }
            }
        }
        // 右表建 hash: 组合键 → 右行下标
        let right_rows = &ctx.tables[rt].rows;
        let mut hash: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        if !eq_keys.is_empty() {
            for (ri, row) in right_rows.iter().enumerate() {
                if let Some(k) = join_key_multi(row, eq_keys.iter().map(|&(_, rp)| rp)) {
                    hash.entry(k).or_default().push(ri);
                }
            }
        }
        // ON 残余 Cmp 判定 (acc_row + right_row)
        let on_cmp_pass = |acc_row: &[ColValue], right_row: &[ColValue]| -> bool {
            for on in &jc.on {
                if let sql::OnPred::Cmp { left, op, right } = on {
                    let (lt, li) = sql_join_resolve_on(&ctx, left, rt).unwrap();
                    let (rtt, ri) = sql_join_resolve_on(&ctx, right, rt).unwrap();
                    let lv = if lt == rt { &right_row[pos_in(rt, li)] } else { &acc_row[wide_pos(lt, li)] };
                    let rv = if rtt == rt { &right_row[pos_in(rt, ri)] } else { &acc_row[wide_pos(rtt, ri)] };
                    if !join_cmp_cols(lv, *op, rv) {
                        return false;
                    }
                }
            }
            true
        };
        let extend = |acc_row: &[ColValue], right_row: Option<&Vec<ColValue>>| -> Vec<ColValue> {
            let mut w = Vec::with_capacity(acc_w + right_pw);
            w.extend_from_slice(acc_row);
            match right_row {
                Some(r) => w.extend_from_slice(r),
                None => w.extend(std::iter::repeat_n(ColValue::Null, right_pw)),
            }
            w
        };
        let mut new_acc: Vec<Vec<ColValue>> = Vec::new();
        let mut matched_right = vec![false; right_rows.len()];
        for acc_row in &acc {
            if jc.kind == JoinKind::Cross {
                for right_row in right_rows.iter() {
                    new_acc.push(extend(acc_row, Some(right_row)));
                }
                continue;
            }
            let key = join_key_multi(acc_row, eq_keys.iter().map(|&(ap, _)| ap));
            let mut any = false;
            if let Some(k) = key
                && let Some(cands) = hash.get(&k)
            {
                for &ri in cands {
                    if on_cmp_pass(acc_row, &right_rows[ri]) {
                        new_acc.push(extend(acc_row, Some(&right_rows[ri])));
                        matched_right[ri] = true;
                        any = true;
                    }
                }
            }
            if !any
                && matches!(jc.kind, JoinKind::Left | JoinKind::Full)
            {
                new_acc.push(extend(acc_row, None));
            }
        }
        // RIGHT/FULL: 未匹配右行 → NULL acc 前缀 + 右行
        if matches!(jc.kind, JoinKind::Right | JoinKind::Full) {
            for (ri, m) in matched_right.iter().enumerate() {
                if !*m {
                    let mut w = vec![ColValue::Null; acc_w];
                    w.extend_from_slice(&right_rows[ri]);
                    new_acc.push(w);
                }
            }
        }
        if new_acc.len() > JOIN_MAX_ROWS {
            conn.resp_complete(
                seq,
                sql_err_bytes(conn.proto, "JOIN result too large (row cap exceeded)"),
            );
            return;
        }
        acc = new_acc;
    }

    // 残余 WHERE (全 conds 递归; null 扩展位由 NULL→false 天然过滤, 保外连接标准语义)
    acc.retain(|row| eval_join_pred(&ctx, row, &wide_pos, &ctx.conds));
    // ORDER BY (倒序逐键稳定排序)
    for (qc, desc) in ctx.order.iter().rev() {
        if let Ok((t, idx)) = sql_join_resolve(&ctx, qc) {
            let wp = wide_pos(t, idx);
            acc.sort_by(|a, b| {
                let o = cmp_colvalue(&a[wp], &b[wp]);
                if *desc { o.reverse() } else { o }
            });
        }
    }
    // OFFSET / LIMIT
    let start = (ctx.offset.unwrap_or(0) as usize).min(acc.len());
    let end = match ctx.limit {
        Some(l) => (start + l as usize).min(acc.len()),
        None => acc.len(),
    };
    let out_rows = &acc[start..end];
    // 输出列计划: (列头, wide_pos)
    let mut out_plan: Vec<(String, usize)> = Vec::new();
    if ctx.items.is_empty() {
        for (t, jt) in ctx.tables.iter().enumerate() {
            let sc = jt.schema.as_ref().unwrap();
            for (i, col) in sc.columns.iter().enumerate() {
                out_plan.push((format!("{}.{}", jt.alias, col.name), wide_pos(t, i as u16)));
            }
        }
    } else {
        for it in &ctx.items {
            let JoinItem::Col(qc) = it;
            let (t, idx) = sql_join_resolve(&ctx, qc).unwrap();
            let label = match &qc.qualifier {
                Some(q) => format!("{}.{}", q, qc.col),
                None => qc.col.clone(),
            };
            out_plan.push((label, wide_pos(t, idx)));
        }
    }
    // 列类型: 由 wide_pos 反查所属表/列 (out_plan 已存 wide_pos; 再算 ty)
    // 直接从 out_plan 重算: 找 (t,localpos) s.t. col_offset[t] <= wp < col_offset[t+1]
    let ty_of = |wp: usize| -> ColType {
        let t = (0..n).rev().find(|&t| col_offset[t] <= wp).unwrap();
        let local = wp - col_offset[t];
        let cidx = ctx.tables[t].proj[local];
        ctx.tables[t].schema.as_ref().unwrap().columns[cidx as usize].ty
    };
    let cols: Vec<(&str, ColType)> =
        out_plan.iter().map(|(label, wp)| (label.as_str(), ty_of(*wp))).collect();
    let rows: Vec<Vec<ColValue>> = out_rows
        .iter()
        .map(|row| out_plan.iter().map(|(_, wp)| row[*wp].clone()).collect())
        .collect();
    conn.resp_complete(seq, sql_rows_bytes(conn.proto, bin, &cols, &rows));
}

/// ⭐ F68 (JOIN): 组合键 — 按给定位置序拼接各列 join_key; 任一 NULL → None (不匹配).
pub(crate) fn join_key_multi(
    row: &[ColValue],
    positions: impl Iterator<Item = usize>,
) -> Option<Vec<u8>> {
    let mut key = Vec::new();
    for p in positions {
        let part = join_key(&row[p])?;
        key.extend_from_slice(&(part.len() as u32).to_le_bytes());
        key.extend_from_slice(&part);
    }
    Some(key)
}

/// ⭐ F67 (JOIN): join key 规范化字节 (类型 tag + 值; NULL → None 不匹配).
pub(crate) fn join_key(cv: &ColValue) -> Option<Vec<u8>> {
    match cv {
        ColValue::Null => None,
        ColValue::I64(i) => {
            let mut k = Vec::with_capacity(9);
            k.push(0);
            k.extend_from_slice(&i.to_le_bytes());
            Some(k)
        }
        ColValue::F64(f) => {
            let mut k = Vec::with_capacity(9);
            k.push(1);
            k.extend_from_slice(&f.to_bits().to_le_bytes());
            Some(k)
        }
        ColValue::Bytes(b) => {
            let mut k = Vec::with_capacity(1 + b.len());
            k.push(2);
            k.extend_from_slice(b);
            Some(k)
        }
        // ⭐ F81: Decimal join key (tag 3 + 16B i128 LE)
        ColValue::Decimal(x, _) => {
            let mut k = Vec::with_capacity(17);
            k.push(3);
            k.extend_from_slice(&x.to_le_bytes());
            Some(k)
        }
    }
}

/// ⭐ F67 (JOIN): 单条 WHERE 残余判定 (NULL 列恒 false, 与 sql_eval_conds 同义).
pub(crate) fn join_cond_pass(cv: &ColValue, cond: &JoinCond) -> bool {
    use std::cmp::Ordering;
    if cond.op == CmpOp::In {
        return cond.set.iter().any(|v| sql_cmp(cv, v) == Some(Ordering::Equal));
    }
    match sql_cmp(cv, &cond.val) {
        None => false,
        Some(o) => match cond.op {
            CmpOp::Eq => o == Ordering::Equal,
            CmpOp::Ne => o != Ordering::Equal,
            CmpOp::Gt => o == Ordering::Greater,
            CmpOp::Ge => o != Ordering::Less,
            CmpOp::Lt => o == Ordering::Less,
            CmpOp::Le => o != Ordering::Greater,
            CmpOp::In => unreachable!(),
        },
    }
}

/// ⭐ F69: JOIN WHERE 谓词树递归求值 (叶子 resolve 限定列 → 宽行取值判定).
pub(crate) fn eval_join_pred(
    ctx: &SqlJoinCtx,
    row: &[ColValue],
    wide_pos: &impl Fn(usize, u16) -> usize,
    pred: &Pred<JoinCond>,
) -> bool {
    match pred {
        Pred::Leaf(cond) => match sql_join_resolve(ctx, &cond.col) {
            Ok((t, idx)) => join_cond_pass(&row[wide_pos(t, idx)], cond),
            Err(_) => false,
        },
        Pred::And(v) => v.iter().all(|p| eval_join_pred(ctx, row, wide_pos, p)),
        Pred::Or(v) => v.iter().any(|p| eval_join_pred(ctx, row, wide_pos, p)),
        Pred::Not(b) => !eval_join_pred(ctx, row, wide_pos, b),
    }
}

/// ⭐ F68 (JOIN): col-col 比较 (ON 非等值残余用; 任一 NULL → false).
pub(crate) fn join_cmp_cols(a: &ColValue, op: CmpOp, b: &ColValue) -> bool {
    use std::cmp::Ordering;
    let ord = match (a, b) {
        (ColValue::Null, _) | (_, ColValue::Null) => return false,
        (ColValue::I64(x), ColValue::I64(y)) => x.cmp(y),
        (ColValue::F64(x), ColValue::F64(y)) => match x.partial_cmp(y) {
            Some(o) => o,
            None => return false,
        },
        (ColValue::I64(x), ColValue::F64(y)) => match (*x as f64).partial_cmp(y) {
            Some(o) => o,
            None => return false,
        },
        (ColValue::F64(x), ColValue::I64(y)) => match x.partial_cmp(&(*y as f64)) {
            Some(o) => o,
            None => return false,
        },
        (ColValue::Bytes(x), ColValue::Bytes(y)) => x.as_slice().cmp(y.as_slice()),
        _ => return false,
    };
    match op {
        CmpOp::Eq => ord == Ordering::Equal,
        CmpOp::Ne => ord != Ordering::Equal,
        CmpOp::Gt => ord == Ordering::Greater,
        CmpOp::Ge => ord != Ordering::Less,
        CmpOp::Lt => ord == Ordering::Less,
        CmpOp::Le => ord != Ordering::Greater,
        CmpOp::In => false,
    }
}

/// ⭐ F65: 提取 schema 的全局唯一列 (iid, col); 空 = 无全局唯一.
pub(crate) fn schema_global_unique(schema: &TableSchema) -> Vec<(u32, u16)> {
    schema
        .indexes
        .iter()
        .filter(|i| i.unique && i.global)
        .map(|i| (i.iid, i.col))
        .collect()
}

/// ⭐ F65: 从 row values 算出各全局唯一列的 (iid, enc_val); NULL 列跳过 (不占坑).
pub(crate) fn row_global_unique_encs(
    schema: &TableSchema,
    values: &[ColValue],
) -> Vec<(u32, Vec<u8>)> {
    schema_global_unique(schema)
        .into_iter()
        .filter_map(|(iid, col)| {
            let ty = schema.columns[col as usize].ty;
            storage::sql_rows::index_val_bytes(ty, &values[col as usize]).map(|enc| (iid, enc))
        })
        .collect()
}

/// ⭐ F65: 向 email-shard 发一个占坑 op (按 enc_val 路由).
#[allow(clippy::too_many_arguments)]
pub(crate) fn push_unique_op(
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    table: &str,
    op: BatchOp,
    enc_val: &[u8],
    num_shards: usize,
    shard_inboxes: &[SharedTaskInbox],
) {
    let sid = hash_route_key(db, table, enc_val, num_shards);
    push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
}

/// ⭐ F65: 启动 autocommit 单行 INSERT 的占坑编排 (已知含全局唯一列).
/// 发第一个 ReserveUnique, 后续由 sql_unique_drive 推进.
#[allow(clippy::too_many_arguments)]
pub(crate) fn sql_unique_ins_start(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    schema: std::sync::Arc<TableSchema>,
    table: String,
    pk: Vec<u8>,
    values: Vec<ColValue>,
) {
    let guc = row_global_unique_encs(&schema, &values);
    // guc 不可能为空 (caller 已判 has_global_unique); 但 NULL 值会使其空 —
    // 全局唯一列隐含 NOT NULL, 实际不会空; 防御性处理: 空则直写行
    if guc.is_empty() {
        let op = BatchOp::RowPut {
            db: db.clone(),
            table: std::sync::Arc::from(table.as_str()),
            pk,
            values,
        };
        let sid = hash_route_op(&op, num_shards);
        push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
        conn.sql_dml_agg.insert(
            seq,
            SqlDmlAgg { remaining: 1, affected: 0, error: None, drop_key: None },
        );
        return;
    }
    let txn_id = seq.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1; // 非 0 的伪唯 txn 标记
    let first = guc[0].clone();
    let st = SqlUniqueIns {
        db: db.clone(),
        table,
        schema,
        pk: pk.clone(),
        values,
        guc,
        txn_id,
        phase: UniquePhase::Reserve,
        idx: 0,
        reserved: 0,
    };
    let tbl = st.table.clone();
    conn.sql_unique_ins.insert(seq, st);
    let op = BatchOp::ReserveUnique {
        db: db.clone(),
        table: std::sync::Arc::from(tbl.as_str()),
        iid: first.0,
        enc_val: first.1.clone(),
        pk,
        txn_id,
    };
    push_unique_op(conn_id, seq, worker_id, db, &tbl, op, &first.1, num_shards, shard_inboxes);
}

/// ⭐ F65: 占坑状态机推进 (在 handle_resp_shard_result 内命中 seq 时调).
/// 返回 true = 已处理此 reply (不再走后续聚合器).
#[allow(clippy::too_many_arguments)]
pub(crate) fn sql_unique_drive(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    result: &BatchResult,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) -> bool {
    if !conn.sql_unique_ins.contains_key(&seq) {
        return false;
    }
    let proto = conn.proto;
    let mut st = conn.sql_unique_ins.remove(&seq).expect("just checked");

    // 回滚 helper: release 已占的 guc[0..reserved], 然后回错
    let rollback_and_err = |conn: &mut ConnState, st: &SqlUniqueIns, msg: String| {
        for (iid, enc) in st.guc.iter().take(st.reserved) {
            let op = BatchOp::ReleaseUnique {
                db: st.db.clone(),
                table: std::sync::Arc::from(st.table.as_str()),
                iid: *iid,
                enc_val: enc.clone(),
                txn_id: st.txn_id,
            };
            // fire-and-forget release (seq=0 不等回复; 用专用低优先无聚合)
            let sid = hash_route_key(&st.db, &st.table, enc, num_shards);
            push_task_grouped(conn_id, 0, worker_id, sid as u32, sid, op, shard_inboxes);
        }
        let bin = conn.mysql_binary.remove(&seq);
        let _ = bin;
        conn.resp_complete(seq, sql_err_bytes(proto, &msg));
    };

    match st.phase {
        UniquePhase::Reserve => match result {
            BatchResult::ReserveOk => {
                st.reserved += 1;
                st.idx += 1;
                if st.idx < st.guc.len() {
                    // 发下一列 reserve
                    let (iid, enc) = st.guc[st.idx].clone();
                    let pk = st.pk.clone();
                    let (db, tbl, txn) = (st.db.clone(), st.table.clone(), st.txn_id);
                    conn.sql_unique_ins.insert(seq, st);
                    let op = BatchOp::ReserveUnique {
                        db: db.clone(),
                        table: std::sync::Arc::from(tbl.as_str()),
                        iid,
                        enc_val: enc.clone(),
                        pk,
                        txn_id: txn,
                    };
                    push_unique_op(conn_id, seq, worker_id, &db, &tbl, op, &enc, num_shards, shard_inboxes);
                } else {
                    // 全部 reserve 完→写行
                    st.phase = UniquePhase::Write;
                    let op = BatchOp::RowPut {
                        db: st.db.clone(),
                        table: std::sync::Arc::from(st.table.as_str()),
                        pk: st.pk.clone(),
                        values: st.values.clone(),
                    };
                    let sid = hash_route_op(&op, num_shards);
                    conn.sql_unique_ins.insert(seq, st);
                    push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
                }
            }
            BatchResult::ReserveConflict { state, holder_pk, .. } => {
                if *state == 2 {
                    // COMMITTED 冲突 → Verify: 回查持有者行是否真存在
                    st.phase = UniquePhase::Verify;
                    let hp = holder_pk.clone();
                    let op = BatchOp::RowGet {
                        db: st.db.clone(),
                        table: std::sync::Arc::from(st.table.as_str()),
                        pk: hp,
                    };
                    let sid = hash_route_op(&op, num_shards);
                    conn.sql_unique_ins.insert(seq, st);
                    push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
                } else {
                    // PENDING 冲突 (在飞) → 拒 (客户端重试)
                    rollback_and_err(conn, &st, "duplicate key on global unique column".into());
                }
            }
            BatchResult::Error(e) => rollback_and_err(conn, &st, e.clone()),
            _ => rollback_and_err(conn, &st, "unexpected reserve reply".into()),
        },
        UniquePhase::Verify => {
            // 回查结果: 持有者行存在且含本 enc_val → 真冲突; 否则 stale → 抢占
            let cur = &st.guc[st.idx];
            let holder_has = matches!(result, BatchResult::GetValue(Some(row))
                if row_has_index_val(&st.schema, row, cur.0, &cur.1));
            if holder_has {
                rollback_and_err(conn, &st, "duplicate key on global unique column".into());
            } else {
                // stale 坑 → 抢占, 继续当前列
                st.phase = UniquePhase::Reserve;
                let (iid, enc) = st.guc[st.idx].clone();
                let pk = st.pk.clone();
                let (db, tbl, txn) = (st.db.clone(), st.table.clone(), st.txn_id);
                conn.sql_unique_ins.insert(seq, st);
                let op = BatchOp::StealUnique {
                    db: db.clone(),
                    table: std::sync::Arc::from(tbl.as_str()),
                    iid,
                    enc_val: enc.clone(),
                    pk,
                    txn_id: txn,
                };
                push_unique_op(conn_id, seq, worker_id, &db, &tbl, op, &enc, num_shards, shard_inboxes);
            }
        }
        UniquePhase::Write => match result {
            BatchResult::PutOk => {
                // 写行成功 → 逐列 confirm
                st.phase = UniquePhase::Confirm;
                st.idx = 0;
                let (iid, enc) = st.guc[0].clone();
                let pk = st.pk.clone();
                let (db, tbl, txn) = (st.db.clone(), st.table.clone(), st.txn_id);
                conn.sql_unique_ins.insert(seq, st);
                let op = BatchOp::ConfirmUnique {
                    db: db.clone(),
                    table: std::sync::Arc::from(tbl.as_str()),
                    iid,
                    enc_val: enc.clone(),
                    pk,
                    txn_id: txn,
                };
                push_unique_op(conn_id, seq, worker_id, &db, &tbl, op, &enc, num_shards, shard_inboxes);
            }
            BatchResult::Error(e) => rollback_and_err(conn, &st, e.clone()),
            _ => rollback_and_err(conn, &st, "unexpected rowput reply".into()),
        },
        UniquePhase::Confirm => {
            // confirm ack (PutOk); 逐列推进, 全部完 → 回 OK
            st.idx += 1;
            if st.idx < st.guc.len() {
                let (iid, enc) = st.guc[st.idx].clone();
                let pk = st.pk.clone();
                let (db, tbl, txn) = (st.db.clone(), st.table.clone(), st.txn_id);
                conn.sql_unique_ins.insert(seq, st);
                let op = BatchOp::ConfirmUnique {
                    db: db.clone(),
                    table: std::sync::Arc::from(tbl.as_str()),
                    iid,
                    enc_val: enc.clone(),
                    pk,
                    txn_id: txn,
                };
                push_unique_op(conn_id, seq, worker_id, &db, &tbl, op, &enc, num_shards, shard_inboxes);
            } else {
                let bin = conn.mysql_binary.remove(&seq);
                let _ = bin;
                conn.resp_complete(seq, sql_ok_bytes(proto, 1));
            }
        }
    }
    true
}

/// ⭐ F65: 判断 row 字节的指定 iid 列值是否等于 enc_val (Verify 用).
pub(crate) fn row_has_index_val(schema: &TableSchema, row: &[u8], iid: u32, enc_val: &[u8]) -> bool {
    let Ok(values) = storage::row::decode_row(schema, row) else {
        return false;
    };
    schema.indexes.iter().find(|i| i.iid == iid).is_some_and(|idx| {
        let ty = schema.columns[idx.col as usize].ty;
        storage::sql_rows::index_val_bytes(ty, &values[idx.col as usize])
            .is_some_and(|e| e == enc_val)
    })
}

/// ⭐ F66: 系统表查询规格 (解析产物, worker 合成虚拟表用).
pub(crate) struct SysQuerySpec {
    catalog: String,
    table: String,
    cols: Vec<String>,
    conds: Pred<Cond>,
    order: Vec<(String, bool)>,
    limit: Option<u32>,
    offset: Option<u32>,
}

impl SysQuerySpec {
    /// 需要表/列元数据 (发 CatalogDump); 否则仅 db 列表.
    fn needs_catalog(&self) -> bool {
        !matches!(
            (self.catalog.as_str(), self.table.as_str()),
            ("information_schema", "schemata")
                | ("pg_catalog", "pg_namespace")
                | ("__show__", "databases")
                | ("__show__", "__empty__")
        )
    }
}

/// ⭐ F66: ColType → information_schema.columns 的 data_type 字符串.
pub(crate) fn coltype_sql_name(ty: ColType) -> &'static str {
    match ty {
        ColType::I64 => "bigint",
        ColType::F64 => "double",
        ColType::Str => "text",
        ColType::Bytes => "blob",
        ColType::Bool => "boolean",
        ColType::Date => "date",
        ColType::Time => "time",
        ColType::Timestamp => "timestamp",
        ColType::Json => "json",
        ColType::Uuid => "uuid",
        ColType::Decimal { .. } => "decimal",
    }
}

/// ⭐ F66: 用合成列名+行跑完成点 (过滤/投影/排序/截断) → 三门面渲染.
/// 虚拟列均为 Str; 行值用 ColValue::Bytes (NULL 用 ColValue::Null).
pub(crate) fn sysq_finish(
    proto: ProtocolKind,
    binary: bool,
    spec: &SysQuerySpec,
    all_cols: &[&str],
    mut rows: Vec<Vec<ColValue>>,
) -> Vec<u8> {
    // 合成 schema (全 Str) 用于 WHERE 过滤 + 投影 + 排序列定位
    let schema = TableSchema {
        version: 1,
        columns: all_cols
            .iter()
            .map(|n| storage::schema::Column {
                name: n.to_string(),
                ty: ColType::Str,
                nullable: true,
            })
            .collect(),
        pk_col: 0,
        indexes: Vec::new(),
        next_iid: 0,
        version_ncols: Vec::new(),
    };
    // WHERE 残余过滤 (递归 eval; `__` 前缀的内部标记叶子如 __table__ 视为真,
    // 已在生成器里处理; 未知真实列的条件 → 不匹配则滤掉)
    rows.retain(|r| eval_pred_sysq(&schema, r, &spec.conds));
    // ORDER BY (按输出列字典序; 未知列忽略)
    for (name, desc) in spec.order.iter().rev() {
        if let Some(ci) = all_cols.iter().position(|c| c.eq_ignore_ascii_case(name)) {
            rows.sort_by(|a, b| {
                let o = cmp_colvalue(&a[ci], &b[ci]);
                if *desc { o.reverse() } else { o }
            });
        }
    }
    // OFFSET / LIMIT
    let start = (spec.offset.unwrap_or(0) as usize).min(rows.len());
    let end = match spec.limit {
        Some(l) => (start + l as usize).min(rows.len()),
        None => rows.len(),
    };
    let rows = &rows[start..end];
    // 投影: cols 空 = 全列; 否则按名选 (未知列 → 全 NULL 列)
    if spec.cols.is_empty() {
        let cols: Vec<(&str, ColType)> = all_cols.iter().map(|c| (*c, ColType::Str)).collect();
        sql_rows_bytes(proto, binary, &cols, rows)
    } else {
        let idxs: Vec<Option<usize>> = spec
            .cols
            .iter()
            .map(|c| all_cols.iter().position(|a| a.eq_ignore_ascii_case(c)))
            .collect();
        let cols: Vec<(&str, ColType)> =
            spec.cols.iter().map(|c| (c.as_str(), ColType::Str)).collect();
        let proj: Vec<Vec<ColValue>> = rows
            .iter()
            .map(|r| {
                idxs.iter()
                    .map(|oi| oi.and_then(|i| r.get(i).cloned()).unwrap_or(ColValue::Null))
                    .collect()
            })
            .collect();
        sql_rows_bytes(proto, binary, &cols, &proj)
    }
}

pub(crate) fn sbytes(s: &str) -> ColValue {
    ColValue::Bytes(s.as_bytes().to_vec())
}

/// ⭐ F66: db 列表类虚拟表 (schemata / pg_namespace) — 零任务合成.
pub(crate) fn sysq_render_dblist(
    proto: ProtocolKind,
    binary: bool,
    spec: &SysQuerySpec,
    dbs: &[String],
) -> Vec<u8> {
    let (all_cols, rows): (Vec<&str>, Vec<Vec<ColValue>>) =
        match (spec.catalog.as_str(), spec.table.as_str()) {
            ("information_schema", "schemata") => (
                vec!["catalog_name", "schema_name", "default_character_set_name"],
                dbs.iter()
                    .map(|d| vec![sbytes("def"), sbytes(d), sbytes("utf8mb4")])
                    .collect(),
            ),
            ("pg_catalog", "pg_namespace") => (
                vec!["nspname", "oid"],
                dbs.iter()
                    .enumerate()
                    .map(|(i, d)| vec![sbytes(d), sbytes(&(i as u32 + 1).to_string())])
                    .collect(),
            ),
            // ⭐ F66: SHOW DATABASES — 单列 "Database"
            ("__show__", "databases") => (
                vec!["Database"],
                dbs.iter().map(|d| vec![sbytes(d)]).collect(),
            ),
            // ⭐ F66: 其他 SHOW stub → 空
            ("__show__", "__empty__") => (vec![""], vec![]),
            _ => (vec![], vec![]),
        };
    sysq_finish(proto, binary, spec, &all_cols, rows)
}

/// ⭐ F66: 需 catalog 快照的虚拟表合成 (tables/columns/key_column_usage/pg_*).
/// `entries` = CatalogDump 回的 (table_name, TableSchema).
pub(crate) fn sysq_render_catalog(
    proto: ProtocolKind,
    binary: bool,
    spec: &SysQuerySpec,
    db: &str,
    entries: &[(String, TableSchema)],
) -> Vec<u8> {
    let key = (spec.catalog.as_str(), spec.table.as_str());
    // ⭐ F66: SHOW TABLES 动态列名 (函数级存活, 避免每次查询泄漏)
    let tables_in = format!("Tables_in_{db}");
    let (all_cols, rows): (Vec<&str>, Vec<Vec<ColValue>>) = match key {
        // ⭐ F66: SHOW [FULL] TABLES — 列名 Tables_in_<db> [+ Table_type]
        ("__show__", "tables") | ("__show__", "full_tables") => {
            let full = spec.table == "full_tables";
            let mut rows = Vec::new();
            for (t, _) in entries {
                if full {
                    rows.push(vec![sbytes(t), sbytes("BASE TABLE")]);
                } else {
                    rows.push(vec![sbytes(t)]);
                }
            }
            if full {
                (vec![tables_in.as_str(), "Table_type"], rows)
            } else {
                (vec![tables_in.as_str()], rows)
            }
        }
        // ⭐ F66: SHOW [FULL] COLUMNS FROM t — Field/Type/Null/Key/Default/Extra
        ("__show__", "columns") | ("__show__", "full_columns") => {
            let full = spec.table == "full_columns";
            // 从 __table__ cond 取目标表名
            let target = spec
                .conds
                .leaves()
                .into_iter()
                .find(|c| c.col == "__table__")
                .and_then(|c| match &c.val {
                    crate::protocol::sql::SqlValue::Str(b) => {
                        Some(String::from_utf8_lossy(b).to_string())
                    }
                    _ => None,
                });
            let mut rows = Vec::new();
            for (t, sc) in entries {
                if let Some(tt) = &target
                    && !t.eq_ignore_ascii_case(tt)
                {
                    continue;
                }
                for (i, c) in sc.columns.iter().enumerate() {
                    let key = if i as u16 == sc.pk_col {
                        "PRI"
                    } else if let Some(idx) = sc.indexes.iter().find(|x| x.col == i as u16) {
                        if idx.unique { "UNI" } else { "MUL" }
                    } else {
                        ""
                    };
                    let mut row = vec![
                        sbytes(&c.name),
                        sbytes(coltype_sql_name(c.ty)),
                        sbytes(if c.nullable { "YES" } else { "NO" }),
                        sbytes(key),
                        ColValue::Null, // Default
                        sbytes(""),     // Extra
                    ];
                    if full {
                        row.push(ColValue::Null); // Collation
                        row.push(sbytes("select,insert,update,references")); // Privileges
                        row.push(sbytes("")); // Comment
                    }
                    rows.push(row);
                }
            }
            if full {
                (
                    vec![
                        "Field", "Type", "Null", "Key", "Default", "Extra", "Collation",
                        "Privileges", "Comment",
                    ],
                    rows,
                )
            } else {
                (vec!["Field", "Type", "Null", "Key", "Default", "Extra"], rows)
            }
        }
        // ⭐ F66: SHOW CREATE TABLE t — 重建 MySQL DDL (SQLAlchemy 从此解析列)
        ("__show__", "create_table") => {
            let target = spec
                .conds
                .leaves()
                .into_iter()
                .find(|c| c.col == "__table__")
                .and_then(|c| match &c.val {
                    crate::protocol::sql::SqlValue::Str(b) => {
                        Some(String::from_utf8_lossy(b).to_string())
                    }
                    _ => None,
                })
                .unwrap_or_default();
            let mut rows = Vec::new();
            if let Some((t, sc)) = entries.iter().find(|(t, _)| t.eq_ignore_ascii_case(&target)) {
                let mut lines: Vec<String> = Vec::new();
                for (i, c) in sc.columns.iter().enumerate() {
                    let ty: std::borrow::Cow<str> = match c.ty {
                        ColType::I64 => "int".into(),
                        ColType::F64 => "double".into(),
                        ColType::Str => "text".into(),
                        ColType::Bytes => "blob".into(),
                        ColType::Bool => "tinyint(1)".into(),
                        ColType::Date => "date".into(),
                        ColType::Time => "time".into(),
                        ColType::Timestamp => "timestamp".into(),
                        ColType::Json => "json".into(),
                        ColType::Uuid => "char(36)".into(),
                        ColType::Decimal { precision, scale } => {
                            format!("decimal({precision},{scale})").into()
                        }
                    };
                    let nullness = if i as u16 == sc.pk_col || !c.nullable {
                        " NOT NULL".to_string()
                    } else {
                        " DEFAULT NULL".to_string()
                    };
                    lines.push(format!("  `{}` {}{}", c.name, ty, nullness));
                }
                let pkc = &sc.columns[sc.pk_col as usize].name;
                lines.push(format!("  PRIMARY KEY (`{pkc}`)"));
                for idx in &sc.indexes {
                    let cn = &sc.columns[idx.col as usize].name;
                    if idx.unique {
                        lines.push(format!("  UNIQUE KEY `{cn}` (`{cn}`)"));
                    } else {
                        lines.push(format!("  KEY `{cn}` (`{cn}`)"));
                    }
                }
                let ddl = format!(
                    "CREATE TABLE `{}` (\n{}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
                    t,
                    lines.join(",\n")
                );
                rows.push(vec![sbytes(t), sbytes(&ddl)]);
            }
            (vec!["Table", "Create Table"], rows)
        }
        ("information_schema", "tables") => (
            vec!["table_catalog", "table_schema", "table_name", "table_type"],
            entries
                .iter()
                .map(|(t, _)| {
                    vec![sbytes("def"), sbytes(db), sbytes(t), sbytes("BASE TABLE")]
                })
                .collect(),
        ),
        ("information_schema", "columns") => {
            let cols = vec![
                "table_catalog",
                "table_schema",
                "table_name",
                "column_name",
                "ordinal_position",
                "is_nullable",
                "data_type",
                "column_default",
            ];
            let mut rows = Vec::new();
            for (t, sc) in entries {
                for (i, c) in sc.columns.iter().enumerate() {
                    rows.push(vec![
                        sbytes("def"),
                        sbytes(db),
                        sbytes(t),
                        sbytes(&c.name),
                        sbytes(&(i + 1).to_string()),
                        sbytes(if c.nullable { "YES" } else { "NO" }),
                        sbytes(coltype_sql_name(c.ty)),
                        ColValue::Null,
                    ]);
                }
            }
            (cols, rows)
        }
        ("information_schema", "key_column_usage") => {
            let cols = vec![
                "table_schema",
                "table_name",
                "column_name",
                "constraint_name",
                "ordinal_position",
            ];
            let mut rows = Vec::new();
            for (t, sc) in entries {
                // pk
                let pkc = &sc.columns[sc.pk_col as usize].name;
                rows.push(vec![
                    sbytes(db),
                    sbytes(t),
                    sbytes(pkc),
                    sbytes("PRIMARY"),
                    sbytes("1"),
                ]);
                // unique 索引
                for idx in sc.indexes.iter().filter(|i| i.unique) {
                    let cn = &sc.columns[idx.col as usize].name;
                    rows.push(vec![
                        sbytes(db),
                        sbytes(t),
                        sbytes(cn),
                        sbytes(&format!("uniq_{cn}")),
                        sbytes("1"),
                    ]);
                }
            }
            (cols, rows)
        }
        ("pg_catalog", "pg_class") => (
            vec!["relname", "relkind", "oid"],
            entries
                .iter()
                .enumerate()
                .map(|(i, (t, _))| {
                    vec![sbytes(t), sbytes("r"), sbytes(&(i as u32 + 1).to_string())]
                })
                .collect(),
        ),
        ("pg_catalog", "pg_attribute") => {
            let cols = vec!["attrelid", "attname", "attnum", "attnotnull"];
            let mut rows = Vec::new();
            for (ri, (_, sc)) in entries.iter().enumerate() {
                for (i, c) in sc.columns.iter().enumerate() {
                    rows.push(vec![
                        sbytes(&(ri as u32 + 1).to_string()),
                        sbytes(&c.name),
                        sbytes(&(i + 1).to_string()),
                        sbytes(if c.nullable { "f" } else { "t" }),
                    ]);
                }
            }
            (cols, rows)
        }
        // 未知系统表 → 空结果 (工具探测容错)
        _ => (vec!["unknown"], vec![]),
    };
    sysq_finish(proto, binary, spec, &all_cols, rows)
}

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

/// ⭐ F76: 单表 Select/Delete/Update 内所有列引用剥表名限定符 (JOIN 走 QualCol 不经此).
pub(crate) fn strip_qual_in_stmt(stmt: &mut SqlStmt) {
    match stmt {
        SqlStmt::Select { table, items, conds, order, group_by, having, .. } => {
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
                    conn, conn_id, seq, worker_id, db, shard_inboxes, num_shards, schema, table,
                    pk, values,
                );
                return;
            }
            for op in ops {
                // ⭐ W2 → ORM-B2: created_here 的表 → 喂进程级路由缓存
                // (value → 所在 shard; bloom 原子只增, 多 worker/门面并发安全)
                let sid = hash_route_op(&op, num_shards);
                feed_route_bloom(conn, db, &table, &schema, &op, sid);
                push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
            }
        }
        // ⭐ S1: DELETE / UPDATE — pk 等值单发, 其余两阶段 (SELECT 内部路径收 pk)
        SqlStmt::Delete { .. } | SqlStmt::Update { .. } => {
            let (table, conds, action) = match stmt {
                SqlStmt::Delete { table, conds } => (table, conds, SqlDmlAction::Delete),
                SqlStmt::Update { table, conds, sets } => {
                    // 校验 + 转换 sets → (列号, ColValue)
                    let mut out: Vec<(u16, ColValue)> = Vec::with_capacity(sets.len());
                    for (name, v) in &sets {
                        let Some(i) = schema.col_by_name(name) else {
                            conn.resp_complete(
                                seq,
                                sql_err_bytes(conn.proto, &format!("unknown column '{name}'")),
                            );
                            return;
                        };
                        if i == schema.pk_col {
                            conn.resp_complete(
                                seq,
                                sql_err_bytes(conn.proto, "cannot UPDATE PRIMARY KEY column"),
                            );
                            return;
                        }
                        // ⭐ F65 v1 边界: 不支持 UPDATE 全局唯一列 (需輁坑; 未实现)
                        if schema.indexes.iter().any(|idx| idx.col == i && idx.unique && idx.global)
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
                        out.push((i, cv));
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
                    conn.sql_dml_agg.insert(
                        seq,
                        SqlDmlAgg { remaining: 1, affected: 0, error: None, drop_key: None },
                    );
                    let op = sql_dml_op(db, &table, pk, &action);
                    push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
                Ok(SqlPlan::Index { iid, lo, hi, .. }) => {
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
                            offset: 0,
                            count: false,
                            agg_spec: None,
                            out_names: Vec::new(),
                        },
                    );
                    let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
                    for sid in 0..num_shards {
                        let op = BatchOp::IndexScan {
                            db: db.clone(),
                            table: table_arc.clone(),
                            iid,
                            lo: lo.clone(),
                            hi: hi.clone(),
                            limit: 0,
                            with_rows: true,
                        };
                        push_task_grouped(
                            conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes,
                        );
                    }
                }
                // ⭐ S2: 无可用索引的 DML (含无 WHERE 全删/全改) → 全表扫 phase1
                Ok(SqlPlan::FullScan) => {
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
                            offset: 0,
                            count: false,
                            agg_spec: None,
                            out_names: Vec::new(),
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
                            conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes,
                        );
                    }
                }
            }
        }
        SqlStmt::Select { table, items, conds, limit, order, offset, group_by, having } => {
            // ⭐ G1/G2 (F63): 投影分型 — 纯列 / COUNT(*) 特例 (旧路径) /
            // 广义聚合 (分桶完成点)
            let has_agg = items.iter().any(|i| matches!(i, sql::SelectItem::Agg { .. }));
            let count = has_agg
                && items.len() == 1
                && group_by.is_empty()
                && having.is_true()
                && order.is_empty()
                && matches!(
                    items[0],
                    sql::SelectItem::Agg { func: sql::AggFn::Count, arg: None, .. }
                );
            if (has_agg || !group_by.is_empty()) && !count {
                sql_run_agg_select(
                    conn, conn_id, seq, worker_id, db, shard_inboxes, num_shards, schema,
                    table, items, conds, group_by, having, order, limit, offset,
                );
                return;
            }
            let cols: Vec<String> = items
                .iter()
                .filter_map(|i| match i {
                    sql::SelectItem::Col { name, .. } => Some(name.clone()),
                    sql::SelectItem::Agg { .. } => None, // 仅 COUNT(*) 特例可达
                    sql::SelectItem::ScalarFn { .. } => None, // 由常量特判处理
                })
                .collect();
            // ⭐ F76: 输出列名 (alias 优先) — 与 proj 同序; 空 items (SELECT *) → 全 None
            let out_names: Vec<Option<String>> = items
                .iter()
                .filter_map(|i| match i {
                    sql::SelectItem::Col { alias, .. } => Some(alias.clone()),
                    sql::SelectItem::Agg { .. } => None,
                    sql::SelectItem::ScalarFn { name } => Some(Some(name.clone())),
                })
                .collect();
            // ⭐ O1: 投影列名 → 列号 (空/COUNT = 全列)
            let proj: Vec<u16> = if cols.is_empty() {
                (0..schema.columns.len() as u16).collect()
            } else {
                let mut p = Vec::with_capacity(cols.len());
                for c in &cols {
                    match schema.col_by_name(c) {
                        Some(i) => p.push(i),
                        None => {
                            conn.resp_complete(
                                seq,
                                sql_err_bytes(conn.proto, &format!("unknown column '{c}'")),
                            );
                            return;
                        }
                    }
                }
                p
            };
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
                                _ => render_sql_rows(conn.proto, bin, &schema, &proj, &out_names, &[]),
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
                                },
                            );
                            let op = BatchOp::RowGet {
                                db: db.clone(),
                                table: std::sync::Arc::from(table.as_str()),
                                pk,
                            };
                            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                            return;
                        }
                        None => {}
                    }
                }
                let read_key = sql_read_key(conn, db, &table, &pk);
                conn.sql_row_ctx.insert(
                    seq,
                    SqlRowCtx { schema, conds, proj, count, read_key, ryow_overlay: Vec::new(), out_names },
                );
                let op = BatchOp::RowGet {
                    db: db.clone(),
                    table: std::sync::Arc::from(table.as_str()),
                    pk,
                };
                push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
            }
            // ⭐ S2: 全表扫 — 广播 TableScan + 全条件残余过滤
            Ok(SqlPlan::FullScan) => {
                // limit 下推仅当无条件且无排序 (下推额含 offset)
                let shard_limit = if conds.is_true() && order_cols.is_empty() && !count {
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
                        offset,
                        count,
                        agg_spec: None,
                        out_names,
                    },
                );
                let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
                for sid in 0..num_shards {
                    let op = BatchOp::TableScan {
                        db: db.clone(),
                        table: table_arc.clone(),
                        limit: shard_limit,
                    };
                    push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
                }
            }
            Ok(SqlPlan::Index { iid, lo, hi, limit_push, eq_enc }) => {
                // limit 下推: 仅当条件可被闭界完全表达且无排序
                // (否则残余过滤/全量排序会漏行; 下推额含 offset)
                let shard_limit = if limit_push && order_cols.is_empty() && !count {
                    limit.map(|l| l + offset).unwrap_or(0)
                } else {
                    0
                };
                // ⭐ O1: 覆盖判定 — 投影∪条件∪排序列 ⊆ {索引列, pk 列} → 免回表
                let idx_col = schema
                    .indexes
                    .iter()
                    .find(|i| i.iid == iid)
                    .map(|i| i.col)
                    .expect("plan 产出的 iid 必在 schema");
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
                            && schema
                                .indexes
                                .iter()
                                .any(|i| i.iid == iid && i.unique),
                        done: false,
                        dml: None,
                        dml_target: None,
                        order: order_cols,
                        offset,
                        count,
                        agg_spec: None,
                        out_names,
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
                    push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
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
        SqlStmt::AlterTable { table, add, if_not_exists } => {
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
        | SqlStmt::SelectJoin { .. } => {
            unreachable!("工具命令在 sql_dispatch_stmt 处理")
        }
        // ⭐ S3: DESCRIBE — schema 本地渲染 (Field/Type/Null/Key)
        SqlStmt::Describe { .. } => {
            let mut rows: Vec<Vec<ColValue>> = Vec::new();
            for (i, col) in schema.columns.iter().enumerate() {
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
                    ColValue::Bytes(if col.nullable { b"YES".to_vec() } else { b"NO".to_vec() }),
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
/// 1. pk 等值 → PkGet; 2. 首个命中条件的索引 → Index (界下推);
/// 3. 无可用索引 → 报错 (v1 不做全表扫).
pub(crate) fn sql_plan_select(schema: &TableSchema, pred: &Pred<Cond>) -> Result<SqlPlan, String> {
    // 先校验所有叶子列名 (不论结构)
    for c in pred.leaves() {
        if schema.col_by_name(&c.col).is_none() {
            return Err(format!("unknown column '{}'", c.col));
        }
    }
    // ⭐ F69: 含 OR/NOT → 无单一区间, 回退全表扫 (正确性由完成点 eval_pred 兼底)
    let Some(conds) = pred.as_conjuncts() else {
        return Ok(SqlPlan::FullScan);
    };
    // 1. pk 等值点查
    let pk_col = &schema.columns[schema.pk_col as usize];
    if let Some(c) = conds.iter().find(|c| c.op == CmpOp::Eq && c.col == pk_col.name) {
        let cv = sql_to_col(pk_col.ty, &c.val)?;
        return Ok(SqlPlan::PkGet { pk: sql_pk_bytes(pk_col.ty, &cv)? });
    }
    // 2. 首个有条件命中的索引 (界下推; 开界值多包含由残余过滤兜底)
    for idx in &schema.indexes {
        let col = &schema.columns[idx.col as usize];
        let mut lo: Option<ColValue> = None;
        let mut hi: Option<ColValue> = None;
        let mut hit = false;
        for c in conds.iter().filter(|c| c.col == col.name) {
            let cv_of = |v: &SqlValue| sql_to_col(col.ty, v);
            match c.op {
                CmpOp::Eq => {
                    hit = true;
                    let cv = cv_of(&c.val)?;
                    lo = Some(cv.clone());
                    hi = Some(cv);
                }
                CmpOp::Gt | CmpOp::Ge => {
                    hit = true;
                    if lo.is_none() {
                        lo = Some(cv_of(&c.val)?);
                    }
                }
                CmpOp::Lt | CmpOp::Le => {
                    hit = true;
                    if hi.is_none() {
                        hi = Some(cv_of(&c.val)?);
                    }
                }
                // ⭐ S2: IN → [min, max] 闭界超集 (保序编码字节比较取极值),
                // 残余过滤精确; Ne 无剪枝价值, 不算命中
                CmpOp::In => {
                    hit = true;
                    if lo.is_none() && hi.is_none() {
                        let mut min: Option<ColValue> = None;
                        let mut max: Option<ColValue> = None;
                        for v in &c.set {
                            let cv = cv_of(v)?;
                            let enc = storage::sql_rows::index_val_bytes(col.ty, &cv)
                                .ok_or("bad IN value")?;
                            let replace_min = min
                                .as_ref()
                                .and_then(|m| storage::sql_rows::index_val_bytes(col.ty, m))
                                .is_none_or(|me| enc < me);
                            if replace_min {
                                min = Some(cv.clone());
                            }
                            let replace_max = max
                                .as_ref()
                                .and_then(|m| storage::sql_rows::index_val_bytes(col.ty, m))
                                .is_none_or(|me| enc > me);
                            if replace_max {
                                max = Some(cv);
                            }
                        }
                        lo = min;
                        hi = max;
                    }
                }
                CmpOp::Ne => {}
            }
        }
        if hit {
            // limit 可下推 ⟺ 全部条件都在本索引列且均为闭界算子
            // (Eq/Ge/Le 的闭界下推与过滤语义一致, 不会截掉本应命中的行)
            let limit_push = conds
                .iter()
                .all(|c| c.col == col.name && matches!(c.op, CmpOp::Eq | CmpOp::Ge | CmpOp::Le));
            // ⭐ W2: 等值 (lo == hi) 时算路由缓存键 (与引擎索引值编码同源)
            let eq_enc = match (&lo, &hi) {
                (Some(l), Some(h)) if l == h => {
                    storage::sql_rows::index_val_bytes(col.ty, l)
                }
                _ => None,
            };
            return Ok(SqlPlan::Index { iid: idx.iid, lo, hi, limit_push, eq_enc });
        }
    }
    // ⭐ S2: 无可用索引 → 全表扫 + 残余过滤 (v1 的报错路径退役)
    Ok(SqlPlan::FullScan)
}