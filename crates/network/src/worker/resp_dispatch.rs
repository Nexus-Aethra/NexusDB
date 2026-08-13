//! ⭐ 解耦 2026-08: RESP 命令分发 + 跨 shard 回包处理 (拆自 mod.rs).
//! dispatch_resp_command (RESP 命令 → 本地/KV/SQL 分派) +
//! handle_resp_shard_result (shard 回包 → 聚合/渲染/级联).

use super::*;

/// 分发单条 RESP 命令: 本地命令直接回 (占 seq 进重排缓冲), KV 命令进 shard.
#[allow(clippy::too_many_arguments)]
pub(crate) fn push_task(
    conn: &mut ConnState,
    conn_id: u64,
    req_id: u64,
    worker_id: u32,
    mut op: BatchOp,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    // ⭐ T2 (分表): 单 key op 统一在此按 "table:key" 冒号前缀选表 (单点重写;
    // 无前缀保持构造时的 default 表). Multi op 由 dispatch 预分组时已解析.
    if let Some((tbl, key)) = op.table_key_mut()
        && let Some(pos) = split_table_key(key)
    {
        let prefix = key[..pos].to_vec();
        *tbl = conn.table_arc(&prefix);
        key.drain(..=pos);
    }
    let shard_id = hash_route_op(&op, num_shards);
    let task = ShardTask {
        conn_id,
        req_id,
        worker_id,
        group: 0,
        op,
    };
    if conn.proto == ProtocolKind::Resp {
        conn.defer_resp_task(shard_id, task, shard_inboxes.len());
    } else {
        shard_inboxes[shard_id].push_spin(task);
    }
}

/// ⭐ MGET/MSET: 定向 push 到指定 shard, 带组号 (聚合回填用).
pub(crate) fn push_task_grouped(
    conn_id: u64,
    req_id: u64,
    worker_id: u32,
    group: u32,
    shard_id: usize,
    op: BatchOp,
    shard_inboxes: &[SharedTaskInbox],
) {
    shard_inboxes[shard_id].push_spin(ShardTask {
        conn_id,
        req_id,
        worker_id,
        group,
        op,
    });
}

/// key 级路由 (与 hash_route_op 同 hash 逻辑, 分组场景用).
pub(crate) fn hash_route_key(db: &str, table: &str, key: &[u8], num_shards: usize) -> usize {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    db.hash(&mut h);
    table.hash(&mut h);
    key.hash(&mut h);
    (h.finish() as usize) % num_shards
}

/// 尝试把 `HGET table:pk field` 路由为 SQL 行点查。
///
/// 只有目标表 schema 显式开启 RESP adapter 时才接管；KV 表、未开启的 SQL 表、
/// 或不符合 `table:pk` 语法的请求均保持原生 Hash 路径。主键列由 schema 唯一决定，
/// 非主键检索留给后续 `HQUERY`，避免把多行查询伪装成单值 HGET。
#[allow(clippy::too_many_arguments)]
pub(crate) fn try_dispatch_resp_sql_hget(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: std::sync::Arc<str>,
    default_table: std::sync::Arc<str>,
    key: Vec<u8>,
    field: Vec<u8>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) -> bool {
    let fallback = BatchOp::HGet {
        db: db.clone(),
        table: default_table,
        key: key.clone(),
        field: field.clone(),
    };
    try_dispatch_resp_sql_read(
        conn,
        conn_id,
        seq,
        worker_id,
        db,
        key,
        vec![field],
        RespSqlReadMode::Fields,
        fallback,
        shard_inboxes,
        num_shards,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn try_dispatch_resp_sql_read(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: std::sync::Arc<str>,
    key: Vec<u8>,
    fields: Vec<Vec<u8>>,
    mode: RespSqlReadMode,
    fallback: BatchOp,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) -> bool {
    let Some((table, pk_literal)) = parse_resp_sql_pk(&key) else {
        return false;
    };
    let cache_key = (db.to_string(), table.clone());
    let cached = conn.sql_cache.borrow().schemas.get(&cache_key).cloned();
    if let Some(schema) = cached {
        resp_sql_hget_with_schema(
            conn,
            conn_id,
            seq,
            worker_id,
            db,
            table,
            pk_literal,
            fields,
            mode,
            fallback,
            schema,
            shard_inboxes,
            num_shards,
        );
    } else {
        // 先取 schema；它不存在或 adapter 未开时在回包处精确回落原 Hash op。
        conn.resp_sql_pending_hget.insert(
            seq,
            PendingRespSqlHGet {
                db: db.clone(),
                table: table.clone(),
                pk_literal,
                fields,
                mode,
                fallback,
            },
        );
        let op = BatchOp::GetSchemaOp {
            db,
            table: std::sync::Arc::from(table.as_str()),
        };
        // 必须走 RESP 的 defer/flush 路径；直推 inbox 会绕过单连接 pipeline
        // 批次边界，导致首个 schema probe 没有及时投递。
        push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
    }
    true
}

/// 尝试把 `HSET/HMSET table:pk field value ...` 适配为原子 RowUpdate。
/// P1 的缺失 row 返回明确错误，而不是生成绕过 NOT NULL/default/外键校验的半行。
#[allow(clippy::too_many_arguments)]
pub(crate) fn try_dispatch_resp_sql_hset(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: std::sync::Arc<str>,
    default_table: std::sync::Arc<str>,
    key: Vec<u8>,
    pairs: Vec<(Vec<u8>, Vec<u8>)>,
    reply_ok: bool,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) -> bool {
    let Some((table, pk_literal)) = parse_resp_sql_pk(&key) else {
        return false;
    };
    let fallback = BatchOp::HSet {
        db: db.clone(),
        table: default_table,
        key,
        pairs: pairs.clone(),
    };
    let cache_key = (db.to_string(), table.clone());
    let cached = conn.sql_cache.borrow().schemas.get(&cache_key).cloned();
    if let Some(schema) = cached {
        resp_sql_hset_with_schema(
            conn,
            conn_id,
            seq,
            worker_id,
            db,
            table,
            pk_literal,
            pairs,
            reply_ok,
            fallback,
            schema,
            shard_inboxes,
            num_shards,
        );
    } else {
        conn.resp_sql_pending_hset.insert(
            seq,
            PendingRespSqlHSet {
                db: db.clone(),
                table: table.clone(),
                pk_literal,
                pairs,
                reply_ok,
                fallback,
            },
        );
        let op = BatchOp::GetSchemaOp {
            db,
            table: std::sync::Arc::from(table.as_str()),
        };
        push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
    }
    true
}

/// 尝试把 `HDEL table:pk field...` 适配为单 shard 原子 RowUnset。SQL 语义下
/// 只能清空可空、非主键列；表未开启 adapter 时完整回落原生 Hash。
#[allow(clippy::too_many_arguments)]
pub(crate) fn try_dispatch_resp_sql_hdel(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: std::sync::Arc<str>,
    default_table: std::sync::Arc<str>,
    key: Vec<u8>,
    fields: Vec<Vec<u8>>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) -> bool {
    let Some((table, pk_literal)) = parse_resp_sql_pk(&key) else {
        return false;
    };
    let fallback = BatchOp::HDel {
        db: db.clone(),
        table: default_table,
        key,
        fields: fields.clone(),
    };
    let cached = conn
        .sql_cache
        .borrow()
        .schemas
        .get(&(db.to_string(), table.clone()))
        .cloned();
    if let Some(schema) = cached {
        resp_sql_hdel_with_schema(
            conn,
            conn_id,
            seq,
            worker_id,
            db,
            table,
            pk_literal,
            fields,
            fallback,
            schema,
            shard_inboxes,
            num_shards,
        );
    } else {
        conn.resp_sql_pending_hdel.insert(
            seq,
            PendingRespSqlHDel {
                db: db.clone(),
                table: table.clone(),
                pk_literal,
                fields,
                fallback,
            },
        );
        push_task(
            conn,
            conn_id,
            seq,
            worker_id,
            BatchOp::GetSchemaOp {
                db,
                table: std::sync::Arc::from(table.as_str()),
            },
            shard_inboxes,
            num_shards,
        );
    }
    true
}

/// 尝试把 HSETNX table:pk field value 适配为 shard 内 RowSetNx。
#[allow(clippy::too_many_arguments)]
pub(crate) fn try_dispatch_resp_sql_hsetnx(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: std::sync::Arc<str>,
    default_table: std::sync::Arc<str>,
    key: Vec<u8>,
    field: Vec<u8>,
    value: Vec<u8>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) -> bool {
    let Some((table, pk_literal)) = parse_resp_sql_pk(&key) else {
        return false;
    };
    let fallback = BatchOp::HSetNx {
        db: db.clone(),
        table: default_table,
        key,
        field: field.clone(),
        val: value.clone(),
    };
    let cached = conn
        .sql_cache
        .borrow()
        .schemas
        .get(&(db.to_string(), table.clone()))
        .cloned();
    if let Some(schema) = cached {
        resp_sql_hsetnx_with_schema(
            conn,
            conn_id,
            seq,
            worker_id,
            db,
            table,
            pk_literal,
            field,
            value,
            fallback,
            schema,
            shard_inboxes,
            num_shards,
        );
    } else {
        conn.resp_sql_pending_hsetnx.insert(
            seq,
            PendingRespSqlHSetNx {
                db: db.clone(),
                table: table.clone(),
                pk_literal,
                field,
                value,
                fallback,
            },
        );
        push_task(
            conn,
            conn_id,
            seq,
            worker_id,
            BatchOp::GetSchemaOp {
                db,
                table: std::sync::Arc::from(table.as_str()),
            },
            shard_inboxes,
            num_shards,
        );
    }
    true
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn resp_sql_hsetnx_with_schema(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: std::sync::Arc<str>,
    table: String,
    pk_literal: Vec<u8>,
    field: Vec<u8>,
    value: Vec<u8>,
    fallback: BatchOp,
    schema: std::sync::Arc<TableSchema>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    if !matches!(
        schema.resp_row_adapter,
        storage::schema::RespRowAdapter::Enabled
    ) {
        push_task(
            conn,
            conn_id,
            seq,
            worker_id,
            fallback,
            shard_inboxes,
            num_shards,
        );
        return;
    }
    let pk = match resp_sql_pk_bytes(&schema, pk_literal.clone()) {
        Ok(v) => v,
        Err(e) => {
            conn.resp_complete(seq, RespCodec::new().encode_error(&e));
            return;
        }
    };
    let Ok(name) = std::str::from_utf8(&field) else {
        conn.resp_complete(
            seq,
            RespCodec::new().encode_error("SQL column name must be UTF-8"),
        );
        return;
    };
    let Some(col) = schema.col_by_name(name) else {
        conn.resp_complete(seq, RespCodec::new().encode_error("unknown SQL column"));
        return;
    };
    if col == schema.pk_col {
        conn.resp_complete(
            seq,
            RespCodec::new().encode_error("cannot update SQL primary key through HSETNX"),
        );
        return;
    }
    let payload = value.get(1..).unwrap_or(&value);
    let val = match sql_to_col(
        schema.columns[col as usize].ty,
        &SqlValue::Str(payload.to_vec()),
    ) {
        Ok(v) => v,
        Err(e) => {
            conn.resp_complete(seq, RespCodec::new().encode_error(&e));
            return;
        }
    };
    push_task(
        conn,
        conn_id,
        seq,
        worker_id,
        BatchOp::RowSetNx {
            db,
            table: std::sync::Arc::from(table.as_str()),
            pk,
            col,
            val,
        },
        shard_inboxes,
        num_shards,
    );
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn resp_sql_hdel_with_schema(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: std::sync::Arc<str>,
    table: String,
    pk_literal: Vec<u8>,
    fields: Vec<Vec<u8>>,
    fallback: BatchOp,
    schema: std::sync::Arc<TableSchema>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    if !matches!(
        schema.resp_row_adapter,
        storage::schema::RespRowAdapter::Enabled
    ) {
        push_task(
            conn,
            conn_id,
            seq,
            worker_id,
            fallback,
            shard_inboxes,
            num_shards,
        );
        return;
    }
    let pk = match resp_sql_pk_bytes(&schema, pk_literal) {
        Ok(v) => v,
        Err(e) => {
            conn.resp_complete(seq, RespCodec::new().encode_error(&e));
            return;
        }
    };
    let mut cols = Vec::with_capacity(fields.len());
    for field in fields {
        let Ok(name) = std::str::from_utf8(&field) else {
            conn.resp_complete(
                seq,
                RespCodec::new().encode_error("SQL column name must be UTF-8"),
            );
            return;
        };
        let Some(ci) = schema.col_by_name(name) else {
            conn.resp_complete(seq, RespCodec::new().encode_error("unknown SQL column"));
            return;
        };
        let column = &schema.columns[ci as usize];
        if ci == schema.pk_col {
            conn.resp_complete(
                seq,
                RespCodec::new().encode_error("cannot HDEL SQL primary key"),
            );
            return;
        }
        if !column.nullable {
            conn.resp_complete(
                seq,
                RespCodec::new().encode_error("cannot HDEL NOT NULL SQL column"),
            );
            return;
        }
        // Redis HDEL 同一 field 重复出现时只计一次。
        if !cols.contains(&ci) {
            cols.push(ci);
        }
    }
    push_task(
        conn,
        conn_id,
        seq,
        worker_id,
        BatchOp::RowUnset {
            db,
            table: std::sync::Arc::from(table.as_str()),
            pk,
            cols,
        },
        shard_inboxes,
        num_shards,
    );
}

/// `DEL table:pk` → RowDelete。多 key 场景逐个判定后加入原有聚合器，保留 Redis
/// 的部分成功计数语义。
#[allow(clippy::too_many_arguments)]
pub(crate) fn try_dispatch_resp_sql_delete(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: std::sync::Arc<str>,
    default_table: std::sync::Arc<str>,
    key: Vec<u8>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) -> bool {
    let Some((table, pk_literal)) = parse_resp_sql_pk(&key) else {
        return false;
    };
    let fallback = BatchOp::Delete {
        db: db.clone(),
        table: default_table,
        key,
    };
    let cached = conn
        .sql_cache
        .borrow()
        .schemas
        .get(&(db.to_string(), table.clone()))
        .cloned();
    if let Some(schema) = cached {
        resp_sql_delete_with_schema(
            conn,
            conn_id,
            seq,
            worker_id,
            db,
            table,
            pk_literal,
            fallback,
            schema,
            shard_inboxes,
            num_shards,
        );
    } else {
        conn.resp_sql_pending_delete.insert(
            seq,
            PendingRespSqlDelete {
                db: db.clone(),
                table: table.clone(),
                pk_literal,
                fallback,
            },
        );
        push_task(
            conn,
            conn_id,
            seq,
            worker_id,
            BatchOp::GetSchemaOp {
                db,
                table: std::sync::Arc::from(table.as_str()),
            },
            shard_inboxes,
            num_shards,
        );
    }
    true
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn try_dispatch_resp_sql_incr(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: std::sync::Arc<str>,
    default_table: std::sync::Arc<str>,
    key: Vec<u8>,
    field: Vec<u8>,
    delta: RespSqlIncrDelta,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) -> bool {
    let Some((table, pk_literal)) = parse_resp_sql_pk(&key) else {
        return false;
    };
    let fallback = match delta {
        RespSqlIncrDelta::Int(delta) => BatchOp::HIncrBy {
            db: db.clone(),
            table: default_table,
            key,
            field: field.clone(),
            delta,
        },
        RespSqlIncrDelta::Float(delta) => BatchOp::HIncrByFloat {
            db: db.clone(),
            table: default_table,
            key,
            field: field.clone(),
            delta,
        },
    };
    let cached = conn
        .sql_cache
        .borrow()
        .schemas
        .get(&(db.to_string(), table.clone()))
        .cloned();
    if let Some(schema) = cached {
        resp_sql_incr_with_schema(
            conn,
            conn_id,
            seq,
            worker_id,
            db,
            table,
            pk_literal,
            field,
            delta,
            fallback,
            schema,
            shard_inboxes,
            num_shards,
        );
    } else {
        conn.resp_sql_pending_incr.insert(
            seq,
            PendingRespSqlIncr {
                db: db.clone(),
                table: table.clone(),
                pk_literal,
                field,
                delta,
                fallback,
            },
        );
        push_task(
            conn,
            conn_id,
            seq,
            worker_id,
            BatchOp::GetSchemaOp {
                db,
                table: std::sync::Arc::from(table.as_str()),
            },
            shard_inboxes,
            num_shards,
        );
    }
    true
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn resp_sql_incr_with_schema(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: std::sync::Arc<str>,
    table: String,
    pk_literal: Vec<u8>,
    field: Vec<u8>,
    delta: RespSqlIncrDelta,
    fallback: BatchOp,
    schema: std::sync::Arc<TableSchema>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    if !matches!(
        schema.resp_row_adapter,
        storage::schema::RespRowAdapter::Enabled
    ) {
        push_task(
            conn,
            conn_id,
            seq,
            worker_id,
            fallback,
            shard_inboxes,
            num_shards,
        );
        return;
    }
    let Some(col) = std::str::from_utf8(&field)
        .ok()
        .and_then(|n| schema.col_by_name(n))
    else {
        conn.resp_complete(seq, RespCodec::new().encode_error("unknown SQL column"));
        return;
    };
    if col == schema.pk_col {
        conn.resp_complete(
            seq,
            RespCodec::new().encode_error("cannot increment SQL primary key"),
        );
        return;
    }
    let pk = match resp_sql_pk_bytes(&schema, pk_literal) {
        Ok(v) => v,
        Err(e) => {
            conn.resp_complete(seq, RespCodec::new().encode_error(&e));
            return;
        }
    };
    let delta = match delta {
        RespSqlIncrDelta::Int(v) => storage::sql_rows::RowIncrDelta::Int(v),
        RespSqlIncrDelta::Float(v) => storage::sql_rows::RowIncrDelta::Float(v),
    };
    push_task(
        conn,
        conn_id,
        seq,
        worker_id,
        BatchOp::RowIncr {
            db,
            table: std::sync::Arc::from(table.as_str()),
            pk,
            col,
            delta,
        },
        shard_inboxes,
        num_shards,
    );
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn resp_sql_delete_with_schema(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: std::sync::Arc<str>,
    table: String,
    pk_literal: Vec<u8>,
    fallback: BatchOp,
    schema: std::sync::Arc<TableSchema>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    if !matches!(
        schema.resp_row_adapter,
        storage::schema::RespRowAdapter::Enabled
    ) {
        push_task(
            conn,
            conn_id,
            seq,
            worker_id,
            fallback,
            shard_inboxes,
            num_shards,
        );
        return;
    }
    let pk = match resp_sql_pk_bytes(&schema, pk_literal) {
        Ok(v) => v,
        Err(e) => {
            conn.resp_complete(seq, RespCodec::new().encode_error(&e));
            return;
        }
    };
    push_task(
        conn,
        conn_id,
        seq,
        worker_id,
        BatchOp::RowDelete {
            db,
            table: std::sync::Arc::from(table.as_str()),
            pk,
        },
        shard_inboxes,
        num_shards,
    );
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn resp_sql_hset_with_schema(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: std::sync::Arc<str>,
    table: String,
    pk_literal: Vec<u8>,
    pairs: Vec<(Vec<u8>, Vec<u8>)>,
    reply_ok: bool,
    fallback: BatchOp,
    schema: std::sync::Arc<TableSchema>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    if !matches!(
        schema.resp_row_adapter,
        storage::schema::RespRowAdapter::Enabled
    ) {
        push_task(
            conn,
            conn_id,
            seq,
            worker_id,
            fallback,
            shard_inboxes,
            num_shards,
        );
        return;
    }
    let pk = match resp_sql_pk_bytes(&schema, pk_literal.clone()) {
        Ok(v) => v,
        Err(e) => {
            conn.resp_complete(seq, RespCodec::new().encode_error(&e));
            return;
        }
    };
    let mut sets = Vec::with_capacity(pairs.len());
    let mut insert_cols = vec![schema.columns[schema.pk_col as usize].name.clone()];
    let mut insert_vals = vec![SqlValue::Str(pk_literal.clone())];
    for (field, raw) in pairs {
        let Ok(name) = std::str::from_utf8(&field) else {
            conn.resp_complete(
                seq,
                RespCodec::new().encode_error("SQL column name must be UTF-8"),
            );
            return;
        };
        let Some(ci) = schema.col_by_name(name) else {
            conn.resp_complete(seq, RespCodec::new().encode_error("unknown SQL column"));
            return;
        };
        if ci == schema.pk_col {
            conn.resp_complete(
                seq,
                RespCodec::new().encode_error("cannot update SQL primary key through HSET"),
            );
            return;
        }
        // RESP parser 为普通 HSET 预置 TAG_RAW；适配层只把 payload 作 SQL 文本转换。
        let payload = raw.get(1..).unwrap_or(&raw);
        let value = match sql_to_col(
            schema.columns[ci as usize].ty,
            &SqlValue::Str(payload.to_vec()),
        ) {
            Ok(v) => v,
            Err(e) => {
                conn.resp_complete(seq, RespCodec::new().encode_error(&e));
                return;
            }
        };
        // 同一 field 重复出现时遵循 HSET 的最后值生效语义，且回包只计一次。
        if let Some((_, prior)) = sets.iter_mut().find(|(prior_ci, _)| *prior_ci == ci) {
            *prior = storage::row::SetVal::Val(value);
        } else {
            insert_cols.push(name.to_string());
            insert_vals.push(SqlValue::Str(payload.to_vec()));
            sets.push((ci, storage::row::SetVal::Val(value)));
        }
    }
    conn.resp_sql_hset.insert(seq, RespSqlHSetCtx { reply_ok });
    let insert_values = match sql_build_row(&schema, &insert_cols, &insert_vals) {
        Ok(values) => values,
        Err(e) => {
            conn.resp_complete(seq, RespCodec::new().encode_error(&e));
            return;
        }
    };
    let op = BatchOp::RowPatchUpsert {
        db,
        table: std::sync::Arc::from(table.as_str()),
        pk,
        sets,
        insert_values,
    };
    push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
}

fn parse_resp_sql_pk(key: &[u8]) -> Option<(String, Vec<u8>)> {
    let first = split_table_key(key)?;
    let rest = &key[first + 1..];
    let table = std::str::from_utf8(&key[..first]).ok()?.to_string();
    (!rest.is_empty()).then_some((table, rest.to_vec()))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn resp_sql_hget_with_schema(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: std::sync::Arc<str>,
    table: String,
    pk_literal: Vec<u8>,
    fields: Vec<Vec<u8>>,
    mode: RespSqlReadMode,
    fallback: BatchOp,
    schema: std::sync::Arc<TableSchema>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    if !matches!(
        schema.resp_row_adapter,
        storage::schema::RespRowAdapter::Enabled
    ) {
        push_task(
            conn,
            conn_id,
            seq,
            worker_id,
            fallback,
            shard_inboxes,
            num_shards,
        );
        return;
    }
    let pk = match resp_sql_pk_bytes(&schema, pk_literal) {
        Ok(pk) => pk,
        Err(e) => {
            conn.resp_complete(seq, RespCodec::new().encode_error(&e));
            return;
        }
    };
    conn.resp_sql_hget.insert(
        seq,
        RespSqlHGetCtx {
            schema,
            fields,
            mode,
        },
    );
    let op = BatchOp::RowGet {
        db,
        table: std::sync::Arc::from(table.as_str()),
        pk,
    };
    push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
}

fn resp_sql_pk_bytes(schema: &TableSchema, pk_literal: Vec<u8>) -> Result<Vec<u8>, String> {
    let ty = schema.columns[schema.pk_col as usize].ty;
    let value = sql_to_col(ty, &SqlValue::Str(pk_literal))?;
    sql_pk_bytes(ty, &value)
}

/// ⭐ GETRANGE 切片 (Redis 语义): 负索引从尾算, end inclusive, 越界 clamp.
pub(crate) fn getrange_slice(data: &[u8], start: i64, end: i64) -> &[u8] {
    let len = data.len() as i64;
    if len == 0 {
        return &[];
    }
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if end < 0 { len + end } else { end };
    if s < 0 {
        s = 0;
    }
    if e < 0 {
        e = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e {
        return &[];
    }
    &data[s as usize..=e as usize]
}
