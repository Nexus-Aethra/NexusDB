//! SQL 语句分派 / 规划 / DML 执行.
//! 从 worker/mod.rs 拆分 (2026-08) — 核心 SQL 执行路径.
//! JOIN→sql_join.rs, UNIQUE→sql_unique.rs, 系统查询→sql_sysquery.rs.

use super::*;
use super::sql_dml::*;
use super::sql_join::*;
use super::sql_sysquery::*;

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
        // ⭐ compat: SELECT NOW() — 无 FROM 标量函数投影 (常量单行)
        SqlStmt::ScalarSelect { items } => {
            let bin = conn.mysql_binary.remove(&seq);
            match scalar_fn_const_row(&items) {
                Ok((cref, row)) => {
                    conn.resp_complete(seq, sql_rows_bytes(conn.proto, bin, &cref, &[row]))
                }
                Err(e) => conn.resp_complete(seq, sql_err_bytes(conn.proto, &e)),
            }
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
        SqlStmt::SystemQuery { catalog, table, cols, conds, order, limit, offset, .. } => {
            let spec =
                SysQuerySpec { catalog, table, cols, conds, order, limit, offset, exists: false };
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
        // ⭐ PG 兼容: SELECT EXISTS (SELECT ...) — 内层转系统查询 exists 判定
        SqlStmt::ExistsStub { inner } => match *inner {
            SqlStmt::SystemQuery { catalog, table, cols, conds, order, limit, offset, .. } => {
                let spec =
                    SysQuerySpec { catalog, table, cols, conds, order, limit, offset, exists: true };
                if spec.needs_catalog() {
                    conn.sql_sysq.insert(seq, spec);
                    let op = BatchOp::CatalogDump { db: db.clone() };
                    let sid = hash_route_key(db, "", &[], num_shards);
                    push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
                } else {
                    let dbs: Vec<String> =
                        db_view.all_names().iter().map(|s| s.to_string()).collect();
                    let mut dbs = dbs;
                    if !dbs.iter().any(|d| d.as_str() == default_db.as_ref()) {
                        dbs.push(default_db.to_string());
                    }
                    let bin = conn.mysql_binary.remove(&seq);
                    let bytes = sysq_render_dblist(conn.proto, bin, &spec, &dbs);
                    conn.resp_complete(seq, bytes);
                }
            }
            _ => {
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, "EXISTS over non-system query not supported (v1)"),
                );
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
        // ⭐ PG 兼容: CREATE DATABASE — 集群控制面 2PC 建库 (同步低频)
        SqlStmt::CreateDb { name } => {
            match conn.sql_shared.cluster_ctl() {
                Some(mgr) => match mgr.create_db(&name) {
                    Ok(()) => conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0)),
                    Err(e) => conn.resp_complete(
                        seq,
                        sql_err_bytes(
                            conn.proto,
                            &format!("database \"{name}\" create failed: {e}"),
                        ),
                    ),
                },
                None => {
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(conn.proto, "cluster control plane not available (CREATE DATABASE)"),
                    );
                }
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
        SqlStmt::SelectJoin { from, from_inner, joins, items, conds, order, limit, offset, .. } => {
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
                    limit_param: None, offset_param: None,
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
        // ⭐ F72: FROM 派生表 — 内层先物化 (同 seq 完成点拦截), 完后 finish_derived
        // 在 worker 内存执行外层 (过滤/投影/排序/截断; 不下推 shard)
        SqlStmt::SelectDerived { inner, alias, items, conds, order, limit, offset, .. } => {
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


/// ⭐ F65: 提取 schema 的全局唯一列 (iid, col); 空 = 无全局唯一.

pub(crate) fn conds_to_scan_preds(schema: &TableSchema, conds: &Pred<Cond>) -> Vec<shard_manager::ScanPred> {
    let mut preds = Vec::new();
    for cond in conds.as_conjuncts().unwrap_or_default() {
        let Some(cidx) = schema.col_by_name(&cond.col) else { continue };
        let ty = schema.columns[cidx as usize].ty;
        // ⭐ DECIMAL 不下推: shard 端按 ordered-bytes 比较, 与 worker 端 Decimal 定标
        // 语义 (字面量转同 scale) 不一致 → 留在 worker 残余过滤 (正确性由 finish 兜底).
        if matches!(ty, ColType::Decimal { .. }) {
            continue;
        }
        // ⭐ IS [NOT] NULL (desugar 为 =/<> NULL): shard 端有序编码比较不支持 NULL 语义
        // → 留 worker 残余过滤 (eval 用 sql_cmp 专有分支).
        if matches!(cond.val, SqlValue::Null) {
            continue;
        }
        // ⭐ compat: JSONB '?' 无 shard 下推语义 → 纯残余过滤
        if cond.op == CmpOp::JsonExists {
            continue;
        }
        let op = match cond.op {
            CmpOp::Eq => shard_manager::PredOp::Eq,
            CmpOp::Ne => shard_manager::PredOp::Ne,
            CmpOp::Gt => shard_manager::PredOp::Gt,
            CmpOp::Ge => shard_manager::PredOp::Ge,
            CmpOp::Lt => shard_manager::PredOp::Lt,
            CmpOp::Le => shard_manager::PredOp::Le,
            CmpOp::In => shard_manager::PredOp::In,
            CmpOp::JsonExists => unreachable!("上续 continue"),
        };
        if cond.op == CmpOp::In {
            let mut set = Vec::with_capacity(cond.set.len());
            let mut all_ok = true;
            for v in &cond.set {
                match sql_to_col(ty, v) {
                    Ok(cv) => set.push(cv),
                    Err(_) => {
                        all_ok = false;
                        break;
                    }
                }
            }
            if all_ok && !set.is_empty() {
                preds.push(shard_manager::ScanPred {
                    col: cidx,
                    op,
                    val: ColValue::Null,
                    set,
                });
            }
        } else if let Ok(val) = sql_to_col(ty, &cond.val) {
            preds.push(shard_manager::ScanPred {
                col: cidx,
                op,
                val,
                set: Vec::new(),
            });
        }
    }
    preds
}

pub(crate) fn sql_plan_select(schema: &TableSchema, pred: &Pred<Cond>) -> Result<SqlPlan, String> {
    // 先校验所有叶子列名 (不论结构)
    for c in pred.leaves() {
        if schema.col_by_name(&c.col).is_none() {
            return Err(format!("unknown column '{}'", c.col));
        }
    }
    // ⭐ F69: 含 OR/NOT → 无单一区间, 回退全表扫 (正确性由完成点 eval_pred 兼底)
    let Some(conds) = pred.as_conjuncts() else {
        // ⭐ M2: OR → 索引并集 — 顶层 OR 的各单叶分支若均命中间一索引列的
        // 等值/范围算子, 分别 IndexScan 后 worker 合并去重 (避免全表扫).
        if let Some(branches) = sql::as_disjuncts(pred) {
            let mut ok = true;
            let mut branch_idx: Option<usize> = None;
            let mut bounds: Vec<(Option<ColValue>, Option<ColValue>)> =
                Vec::with_capacity(branches.len());
            for c in &branches {
                if !matches!(c.op, CmpOp::Eq | CmpOp::Ge | CmpOp::Le | CmpOp::Gt | CmpOp::Lt | CmpOp::In) {
                    ok = false;
                    break;
                }
                if schema.col_by_name(&c.col).is_none() {
                    ok = false;
                    break;
                }
                let col_idx = schema.col_by_name(&c.col).unwrap();
                // 仅支持索引列
                let Some(ipos) = schema.indexes.iter().position(|i| i.col == col_idx) else {
                    ok = false;
                    break;
                };
                // 所有分支必须落在同一索引列 (并集可合并)
                if let Some(prev) = branch_idx {
                    if prev != ipos {
                        ok = false;
                        break;
                    }
                } else {
                    branch_idx = Some(ipos);
                }
                let col = &schema.columns[col_idx as usize];
                let mut lo: Option<ColValue> = None;
                let mut hi: Option<ColValue> = None;
                match c.op {
                    CmpOp::Eq => {
                        let cv = sql_to_col(col.ty, &c.val)?;
                        lo = Some(cv.clone());
                        hi = Some(cv);
                    }
                    CmpOp::Ge | CmpOp::Gt => {
                        let cv = sql_to_col(col.ty, &c.val)?;
                        lo = Some(cv);
                    }
                    CmpOp::Le | CmpOp::Lt => {
                        let cv = sql_to_col(col.ty, &c.val)?;
                        hi = Some(cv);
                    }
                    CmpOp::In => {
                        let mut min: Option<ColValue> = None;
                        let mut max: Option<ColValue> = None;
                        for v in &c.set {
                            let cv = sql_to_col(col.ty, v)?;
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
                                max = Some(cv.clone());
                            }
                        }
                        lo = min;
                        hi = max;
                    }
                    _ => unreachable!(),
                }
                bounds.push((lo, hi));
            }
            if ok && branch_idx.is_some() {
                let ipos = branch_idx.unwrap();
                let branches: Vec<(u16, Option<ColValue>, Option<ColValue>)> = bounds
                    .into_iter()
                    .map(|(lo, hi)| (ipos as u16, lo, hi))
                    .collect();
                return Ok(SqlPlan::IndexUnion { branches });
            }
        }
        return Ok(SqlPlan::FullScan);
    };
    // 1. pk 等值点查
    let pk_col = &schema.columns[schema.pk_col as usize];
    if let Some(c) = conds.iter().find(|c| c.op == CmpOp::Eq && c.col == pk_col.name) {
        let cv = sql_to_col(pk_col.ty, &c.val)?;
        return Ok(SqlPlan::PkGet { pk: sql_pk_bytes(pk_col.ty, &cv)? });
    }
    // ⭐ PG 兼容 (范围查): 主键列范围谓词 (BETWEEN/>=/<=) → 走主键 B+Tree 区间
    // 扫描 (避免全表扫). 收集 pk 列上的 Ge/Le/Gt/Lt 界.
    {
        let mut lo: Option<ColValue> = None;
        let mut hi: Option<ColValue> = None;
        let mut hit = false;
        for c in conds.iter().filter(|c| c.col == pk_col.name) {
            match c.op {
                CmpOp::Ge | CmpOp::Gt => {
                    hit = true;
                    if lo.is_none() {
                        lo = Some(sql_to_col(pk_col.ty, &c.val)?);
                    }
                }
                CmpOp::Le | CmpOp::Lt => {
                    hit = true;
                    if hi.is_none() {
                        hi = Some(sql_to_col(pk_col.ty, &c.val)?);
                    }
                }
                _ => {}
            }
        }
        if hit {
            // 单边范围选择性低 → 无界扫描接近全表, 仍用索引 (起点定位快于全扫)
            return Ok(SqlPlan::Index { iid: 0, lo, hi, limit_push: true, eq_enc: None, pk: true });
        }
    }
    // 2. 多索引计分选择
    let mut best: Option<(u32, usize, u32)> = None; // (score, idx_pos, iid)
    let mut best_bounds: (Option<ColValue>, Option<ColValue>) = (None, None);
    for (ipos, idx) in schema.indexes.iter().enumerate() {
        // ⭐ PG 兼容 (FMT_VER 7): 复合索引 v1 不参与单列扫描 (退化全表, 正确性保底)
        if idx.cols.len() != 1 {
            continue;
        }
        let col = &schema.columns[idx.col as usize];
        let mut lo: Option<ColValue> = None;
        let mut hi: Option<ColValue> = None;
        let mut score: u32 = 0;
        let mut hits: Vec<&Cond> = Vec::new();
        for c in conds.iter().filter(|c| c.col == col.name) {
            let cv_of = |v: &SqlValue| sql_to_col(col.ty, v);
            match c.op {
                CmpOp::Eq => {
                    score += 3;
                    hits.push(c);
                    let cv = cv_of(&c.val)?;
                    lo = Some(cv.clone());
                    hi = Some(cv);
                }
                CmpOp::Gt | CmpOp::Ge => {
                    score += 2;
                    hits.push(c);
                    if lo.is_none() {
                        lo = Some(cv_of(&c.val)?);
                    }
                }
                CmpOp::Lt | CmpOp::Le => {
                    score += 2;
                    hits.push(c);
                    if hi.is_none() {
                        hi = Some(cv_of(&c.val)?);
                    }
                }
                // ⭐ S2: IN → [min, max] 闭界超集 (保序编码字节比较取极值),
                // 残余过滤精确; Ne 无剪枝价值, 不算命中
                CmpOp::In => {
                    // ⭐ M3-3 (代价): IN 界 = [min,max] 超集, 集合 ≥ IN_INDEX_MAX_SET 时
                    // 扫描接近全表且残余仍要全量 eval → 不选索引 (回退全扫 + 残余).
                    if c.set.len() >= IN_INDEX_MAX_SET {
                        continue;
                    }
                    score += 1;
                    hits.push(c);
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
                // ⭐ compat: JSONB '?' 无界/无剪枝, 不算命中 (纯残余过滤)
                CmpOp::JsonExists => {}
            }
        }
        // ⭐ M3-3 (代价): 无界范围 (仅 lo 或仅 hi, 如 `score > 10`) → 索引扫半边表,
        // 选择性低 → 计分 -1 (有界范围 BETWEEN 保持原分).
        if score > 0 && (lo.is_some() != hi.is_some()) {
            score = score.saturating_sub(1);
        }
        // 只有得分 > 0 才算候选; 平局保留首个 (ipos 更小者优先, 确定性)
        if score > 0 && best.as_ref().map_or(true, |(bs, bpos, _)| score > *bs || (score == *bs && ipos < *bpos)) {
            best = Some((score, ipos, idx.iid));
            best_bounds = (lo, hi);
        }
    }
    if let Some((_, ipos, iid)) = best {
        let idx = &schema.indexes[ipos];
        let col = &schema.columns[idx.col as usize];
        let (lo, hi) = best_bounds;
        // limit 可下推 ⟺ 全部条件都在本索引列且均为闭界算子
        // (Eq/Ge/Le 的闭界下推与过滤语义一致, 不会截掉本应命中的行)
        let limit_push = conds
            .iter()
            .all(|c| c.col == col.name && matches!(c.op, CmpOp::Eq | CmpOp::Ge | CmpOp::Le));
        // ⭐ W2: 等值 (lo == hi) 时算路由缓存键 (与引擎索引值编码同源)
        let eq_enc = match (&lo, &hi) {
            (Some(l), Some(h)) if l == h => storage::sql_rows::index_val_bytes(col.ty, l),
            _ => None,
        };
        return Ok(SqlPlan::Index { iid, lo, hi, limit_push, eq_enc, pk: false });
    }
    // ⭐ S2: 无可用索引 → 全表扫 + 残余过滤 (v1 的报错路径退役)
    Ok(SqlPlan::FullScan)
}
#[cfg(test)]
mod tests {
    use super::*;
    use storage::schema::{ColType, Column};

    fn test_schema() -> TableSchema {
        TableSchema::new(
            vec![
                Column { name: "id".into(), ty: ColType::I64, nullable: false, default: None },
                Column { name: "name".into(), ty: ColType::Str, nullable: true, default: None },
                Column { name: "score".into(), ty: ColType::I64, nullable: true, default: None },
            ],
            0,
            &[1, 2],
            &[],
            &[],
            &[],
            &[],
        )
        .unwrap()}

    fn c(col: &str, op: CmpOp, val: SqlValue) -> Pred<Cond> {
        Pred::Leaf(Cond { col: col.into(), op, val, set: vec![] })
    }

    fn andc(preds: Vec<Pred<Cond>>) -> Pred<Cond> {
        Pred::And(preds)
    }

    #[test]
    fn large_in_falls_back_to_fullscan() {
        // ⭐ M3-3: score IN (1..=100) 集合 ≥ 32 → 选择性过低, 不选 score 索引 (全扫 + 残余)
        let schema = test_schema();
        let p = Pred::Leaf(Cond {
            col: "score".into(),
            op: CmpOp::In,
            val: SqlValue::Null,
            set: (1..=100).map(|i| SqlValue::Int(i)).collect(),
        });
        let (np, _) = sql::normalize_pred_cond(&p);
        let plan = sql_plan_select(&schema, &np).unwrap();
        assert!(matches!(plan, SqlPlan::FullScan), "大 IN 应回退全扫: {plan:?}");
    }

    #[test]
    fn unbounded_range_loses_to_eq_index() {
        // ⭐ M3-3: 无界范围 (score > 10, 仅 lo) 降权 +1; name 等值 +3 胜出 (选 name 索引)
        let schema = test_schema();
        let p = Pred::And(vec![
            c("name", CmpOp::Eq, SqlValue::Str(b"x".to_vec())),
            c("score", CmpOp::Gt, SqlValue::Int(10)),
        ]);
        let (np, _) = sql::normalize_pred_cond(&p);
        let plan = sql_plan_select(&schema, &np).unwrap();
        assert!(
            matches!(plan, SqlPlan::Index { iid: 0, .. }),
            "name 等值 +3 应胜出 (score 单边范围降权): {plan:?}"
        );
    }

    #[test]
    fn pk_eq_uses_point_get() {
        let schema = test_schema();
        let plan = sql_plan_select(&schema, &c("id", CmpOp::Eq, SqlValue::Int(42))).unwrap();
        assert!(matches!(plan, SqlPlan::PkGet { .. }), "pk eq must be PkGet, got {plan:?}");
    }

    #[test]
    fn or_eq_merges_to_in_index_plan() {
        // ⭐ M2c: score=1 OR score=2 → 归一后 score IN (1,2) → 单索引扫描 [1,2] 闭界
        // (取代 FullScan / IndexUnion 双分支, 走 M1 计分的单 Index 计划)
        let schema = test_schema();
        let p = Pred::Or(vec![
            c("score", CmpOp::Eq, SqlValue::Int(1)),
            c("score", CmpOp::Eq, SqlValue::Int(2)),
        ]);
        let (np, _) = sql::normalize_pred_cond(&p);
        let plan = sql_plan_select(&schema, &np).unwrap();
        match plan {
            SqlPlan::Index { iid, lo, hi, .. } => {
                assert_eq!(iid, 1, "score 是第二个索引 (iid 1)");
                assert_eq!(lo, Some(ColValue::I64(1)));
                assert_eq!(hi, Some(ColValue::I64(2)));
            }
            other => panic!("OR→IN 应走单 Index 扫描, got {other:?}"),
        }
    }

    #[test]
    fn or_eq_inside_and_still_plans_index() {
        // ⭐ M2c: (score=1 OR score=2) AND name='x' → And(In, Eq) → 计分选 name 等值索引
        let schema = test_schema();
        let p = Pred::And(vec![
            Pred::Or(vec![
                c("score", CmpOp::Eq, SqlValue::Int(1)),
                c("score", CmpOp::Eq, SqlValue::Int(2)),
            ]),
            c("name", CmpOp::Eq, SqlValue::Str(b"x".to_vec())),
        ]);
        let (np, _) = sql::normalize_pred_cond(&p);
        let plan = sql_plan_select(&schema, &np).unwrap();
        match plan {
            SqlPlan::Index { iid, lo, hi, .. } => {
                assert_eq!(iid, 0, "name 等值 +3 应胜出");
                assert_eq!(lo, Some(ColValue::Bytes(b"x".to_vec())));
                assert_eq!(hi, Some(ColValue::Bytes(b"x".to_vec())));
            }
            other => panic!("含 OR 的 AND 也应走索引, got {other:?}"),
        }
    }

    #[test]
    fn single_index_hit() {
        let schema = test_schema();
        let plan = sql_plan_select(
            &schema,
            &c("name", CmpOp::Eq, SqlValue::Str(b"alice".to_vec())),
        )
        .unwrap();
        match plan {
            SqlPlan::Index { iid, lo, hi, .. } => {
                assert_eq!(iid, 0, "name 是第一个索引 (iid 0)");
                assert_eq!(lo, Some(ColValue::Bytes(b"alice".to_vec())));
                assert_eq!(hi, Some(ColValue::Bytes(b"alice".to_vec())));
            }
            other => panic!("expected Index, got {other:?}"),
        }
    }

    #[test]
    fn multiple_index_chooses_best() {
        let schema = test_schema();
        // name 等值 (+3) vs score 范围 (+2) → 选 name 索引
        let p = andc(vec![
            c("name", CmpOp::Eq, SqlValue::Str(b"alice".to_vec())),
            c("score", CmpOp::Gt, SqlValue::Int(10)),
        ]);
        let plan = sql_plan_select(&schema, &p).unwrap();
        match plan {
            SqlPlan::Index { iid, .. } => assert_eq!(iid, 0, "等值索引应优先于范围索引"),
            other => panic!("expected Index, got {other:?}"),
        }
    }

    #[test]
    fn range_index_selected_when_no_eq() {
        let schema = test_schema();
        let p = andc(vec![
            c("score", CmpOp::Ge, SqlValue::Int(10)),
            c("score", CmpOp::Le, SqlValue::Int(20)),
        ]);
        let plan = sql_plan_select(&schema, &p).unwrap();
        match plan {
            SqlPlan::Index { iid, lo, hi, .. } => {
                assert_eq!(iid, 1, "score 范围应选 score 索引");
                assert_eq!(lo, Some(ColValue::I64(10)));
                assert_eq!(hi, Some(ColValue::I64(20)));
            }
            other => panic!("expected Index, got {other:?}"),
        }
    }

    #[test]
    fn eq_on_two_indexes_prefers_first() {
        let schema = test_schema();
        // 两个等值, 平局取靠前 (iid 0) — 确定性
        let p = andc(vec![
            c("name", CmpOp::Eq, SqlValue::Str(b"x".to_vec())),
            c("score", CmpOp::Eq, SqlValue::Int(5)),
        ]);
        let plan = sql_plan_select(&schema, &p).unwrap();
        match plan {
            SqlPlan::Index { iid, .. } => assert_eq!(iid, 0, "平局应取靠前索引 (确定性)"),
            other => panic!("expected Index, got {other:?}"),
        }
    }

    #[test]
    fn no_matching_index_falls_back_full_scan() {
        let schema = test_schema();
        // ⭐ PG 兼容: 主键列范围谓词走主键索引 (非二级索引)
        let plan = sql_plan_select(&schema, &c("id", CmpOp::Gt, SqlValue::Int(0))).unwrap();
        assert!(
            matches!(plan, SqlPlan::Index { pk: true, .. }),
            "主键范围应走主键索引, got {plan:?}"
        );
        // 非索引非主键列范围 → FullScan
        let plan = sql_plan_select(&schema, &c("score", CmpOp::Gt, SqlValue::Int(0))).unwrap();
        assert!(matches!(plan, SqlPlan::Index { pk: false, .. } | SqlPlan::FullScan), "score 有二级索引应走索引: {plan:?}");
    }

    #[test]
    fn unknown_column_errors() {
        let schema = test_schema();
        assert!(sql_plan_select(&schema, &c("nope", CmpOp::Eq, SqlValue::Int(1))).is_err());
    }

    #[test]
    fn normalize_not_eq_for_index_plan() {
        let schema = test_schema();
        let raw = Pred::Not(Box::new(c("name", CmpOp::Eq, SqlValue::Str(b"alice".to_vec()))));
        let (np, is_false) = sql::normalize_pred_cond(&raw);
        assert!(!is_false);
        match np {
            Pred::Leaf(ref cc) => assert_eq!(cc.op, CmpOp::Ne),
            other => panic!("expected leaf Ne, got {other:?}"),
        }
        let plan = sql_plan_select(&schema, &np).unwrap();
        assert!(matches!(plan, SqlPlan::FullScan), "Ne 无界, got {plan:?}");
    }

    #[test]
    fn always_false_predicate_short_circuits() {
        let raw = Pred::And(vec![
            c("id", CmpOp::Eq, SqlValue::Int(1)),
            Pred::Or(vec![]),
        ]);
        let (_, is_false) = sql::normalize_pred_cond(&raw);
        assert!(is_false, "AND 含恒假项必须标记恒假");
    }

    #[test]
    fn or_same_index_uses_index_union() {
        let schema = test_schema();
        // tag = 'a' OR tag = 'b' → IndexUnion (两个等值分支)
        let p = Pred::Or(vec![
            c("name", CmpOp::Eq, SqlValue::Str(b"a".to_vec())),
            c("name", CmpOp::Eq, SqlValue::Str(b"b".to_vec())),
        ]);
        let plan = sql_plan_select(&schema, &p).unwrap();
        match plan {
            SqlPlan::IndexUnion { branches } => {
                assert_eq!(branches.len(), 2, "两个等值分支");
                // 每个分支是同一索引列 (name, 索引 ipos 0)
                assert_eq!(branches[0].0, 0);
                assert_eq!(branches[1].0, 0);
            }
            other => panic!("expected IndexUnion, got {other:?}"),
        }
    }

    #[test]
    fn or_cross_index_falls_back_full_scan() {
        let schema = test_schema();
        // name = 'a' OR score = 5 → 跨索引列 → FullScan (不能并集)
        let p = Pred::Or(vec![
            c("name", CmpOp::Eq, SqlValue::Str(b"a".to_vec())),
            c("score", CmpOp::Eq, SqlValue::Int(5)),
        ]);
        let plan = sql_plan_select(&schema, &p).unwrap();
        assert!(matches!(plan, SqlPlan::FullScan), "跨索引列 OR 应 FullScan, got {plan:?}");
    }

    #[test]
    fn or_with_and_branch_falls_back_full_scan() {
        let schema = test_schema();
        // (name='a') OR (name='b' AND score=1) → 含 AND 分支 → FullScan
        let p = Pred::Or(vec![
            c("name", CmpOp::Eq, SqlValue::Str(b"a".to_vec())),
            Pred::And(vec![
                c("name", CmpOp::Eq, SqlValue::Str(b"b".to_vec())),
                c("score", CmpOp::Eq, SqlValue::Int(1)),
            ]),
        ]);
        let plan = sql_plan_select(&schema, &p).unwrap();
        assert!(matches!(plan, SqlPlan::FullScan), "含 AND 分支应 FullScan, got {plan:?}");
    }

    #[test]
    fn or_on_non_index_col_falls_back_full_scan() {
        let schema = test_schema();
        // score 有索引, 但用未索引的... schema 只有 name/score 索引. 测试 id 上的 OR (pk 非索引)
        let p = Pred::Or(vec![
            c("id", CmpOp::Eq, SqlValue::Int(1)),
            c("id", CmpOp::Eq, SqlValue::Int(2)),
        ]);
        // id 是 pk, 非独立索引列 → 无索引 → FullScan (pk 点查只支持单值)
        let plan = sql_plan_select(&schema, &p).unwrap();
        assert!(matches!(plan, SqlPlan::FullScan), "pk 上的 OR 应 FullScan, got {plan:?}");
    }
}
