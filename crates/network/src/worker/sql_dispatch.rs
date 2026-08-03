//! SQL 语句分派 / 规划 / DML 执行.
//! 从 worker/mod.rs 拆分 (2026-08) — 核心 SQL 执行路径.
//! JOIN→sql_join.rs, UNIQUE→sql_unique.rs, 系统查询→sql_sysquery.rs.

use super::*;
use super::sql_join::*;
use super::sql_sysquery::*;
use super::sql_unique::*;

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

/// ⭐ F66: 系统表查询规格 (解析产物, worker 合成虚拟表用).
/// `exists=true` (PG 兼容 SELECT EXISTS): 只判定过滤后是否非空, 回单行布尔.

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
fn sql_update_expr_to_row(
    schema: &TableSchema,
    e: &sql::ScalarExpr,
) -> Result<storage::row::RowExpr, String> {
    use storage::row::{RowArith, RowExpr};
    Ok(match e {
        sql::ScalarExpr::Col(name) => {
            let i = schema.col_by_name(name).ok_or_else(|| format!("unknown column '{name}'"))?;
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
            return Err("JSONB field expression not supported in UPDATE SET (v1)".into())
        }
    })
}

/// ⭐ compat: 表达式投影 base 列号 (JSONB 表达式根列; v1: 递归取 JsonGet 底层
/// 列引用; Lit 等无列场景回退列 0 — 渲染时求值仍可取到值).
fn bound_base_col(e: &BoundExpr) -> u16 {
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
                        conn, conn_id, seq, worker_id, db, shard_inboxes, num_shards,
                        &schema, &ops,
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
                        conn.cascade_pending.insert(
                            seq,
                            ((*db).clone(), table.clone(), vec![pk.clone()]),
                        );
                    }
                    conn.sql_dml_agg.insert(
                        seq,
                        SqlDmlAgg { remaining: 1, affected: 0, error: None, drop_key: None },
                    );
                    let op = sql_dml_op(db, &table, pk, &action);
                    push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
                Ok(SqlPlan::Index { iid, lo, hi, limit_push: _, eq_enc: _, pk }) => {
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
                        // ⭐ PG 兼容 (范围查): 主键区间用 ScanFiltered(pk hint),
                        // 否则二级索引 IndexScan.
                        let op = if pk {
                            BatchOp::ScanFiltered {
                                db: db.clone(),
                                table: table_arc.clone(),
                                preds: Vec::new(),
                                proj: Vec::new(),
                                index_hint: Some(shard_manager::IndexHint {
                                    iid: 0,
                                    lo: lo.clone(),
                                    hi: hi.clone(),
                                    pk: true,
                                }),
                                key_set_hint: None,
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
                            conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes,
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
                            conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes,
                        );
                    }
                }
            }
        }
        SqlStmt::Select { table, items, mut conds, limit, order, offset, group_by, having, .. } => {
            // ⭐ 优化器 M1 (2026-08): 谓词归一 (NOT 下推/恒真恒假短路) 后再规划
            let (ncond, cond_false) = sql::normalize_pred_cond(&conds);
            conds = ncond;
            if cond_false {
                // 恒假谓词 → 直接返回空结果 (短路广播)
                let bin = conn.mysql_binary.remove(&seq);
                conn.resp_complete(
                    seq,
                    sql_rows_bytes(
                        conn.proto,
                        bin,
                        &[("", storage::schema::ColType::I64)],
                        &[],
                    ),
                );
                return;
            }
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
                            conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes,
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
                let shard_limit = if conds.is_true() && !count && (order_cols.is_empty() || pk_sorted)
                {
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
                        down_proj: if downable { row_cols.clone() } else { Vec::new() },
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
                    push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
                }
            }
            Ok(SqlPlan::Index { iid, lo, hi, limit_push, eq_enc, pk: true }) => {
                // ⭐ PG 兼容 (范围查): 主键区间扫描 — 全 shard 广播 ScanFiltered
                // (index_hint { pk: true } 走主键 B+Tree 区间). 无覆盖/路由剪枝
                // (主键范围); 行经残余过滤 (preds 完整下推).
                let pk_col = schema.pk_col;
                let cover = (count || proj.iter().all(|&c| c == pk_col))
                    && conds.leaves().iter().all(|c| {
                        schema.col_by_name(&c.col).is_some_and(|i| i == pk_col)
                    });
                let shard_limit = if limit_push && !count {
                    limit.map(|l| l + offset).unwrap_or(0)
                } else {
                    0
                };
                let scan_preds: Vec<shard_manager::ScanPred> = conds
                    .leaves()
                    .iter()
                    .filter_map(|c| {
                        let Some(ci) = schema.col_by_name(&c.col) else { return None };
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
                        Some(shard_manager::ScanPred { col: ci, op: sop, val: v, set: Vec::new() })
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
                        index_hint: Some(shard_manager::IndexHint { iid: 0, lo: lo.clone(), hi: hi.clone(), pk: true }),
                        key_set_hint: None,
                        limit: shard_limit as u32,
                    };
                    push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
                }
            }
            Ok(SqlPlan::Index { iid, lo, hi, limit_push, eq_enc, pk: false }) => {
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
                            && schema
                                .indexes
                                .iter()
                                .any(|i| i.iid == iid && i.unique),
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
        // ⭐ compat: ALTER TABLE DROP COLUMN — 标记删除 (列号/布局/版本不变, 存量零重写)
        SqlStmt::AlterTable { table, add, drop, if_not_exists } => {
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
/// 1. pk 等值 → PkGet;
/// 2. 多索引计分选择 (等值 > 范围 > IN, 界最紧者胜) → Index (界下推);
/// 3. 无可用索引 → FullScan (残余过滤兜底).
///
/// ⭐ 优化器增强 (2026-08, M1): 从"首个命中索引"升级为"计分最优索引":
/// 等值命中 +3 / 范围 +2 / IN +1, 得分最高者胜; 平局取靠前 (确定性)。
/// ⭐ M3-3 (代价): IN 集合大小阈值 — 超过则选择性过低, 不走索引 (全扫 + 残余).
const IN_INDEX_MAX_SET: usize = 32;

/// ⭐ P0-2: 单表 Cond → ScanPred 下推 (仅纯 AND 合取; 值转换失败跳过该谓词).
/// shard 端 ScanFiltered 的 preds 是 AND 语义, 谓词下推只影响过滤位置 (正确性仍
/// 由 worker 端 finish 残余过滤兜底).
fn conds_to_scan_preds(schema: &TableSchema, conds: &Pred<Cond>) -> Vec<shard_manager::ScanPred> {
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
