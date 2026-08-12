//! shard 回包处理主函数 (拆自 resp_dispatch.rs).
//!
//! `handle_resp_shard_result`: shard 回包 → HTTP/SQL 状态机推进 / 聚合器渲染 /
//! 级联 / 重排缓冲 resp_complete.

use super::*;

pub(crate) fn handle_resp_shard_result(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    group: u32,
    result: &BatchResult,
    worker_id: u32,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    let codec = RespCodec::new();
    // RESP→SQL 行适配的 schema miss 续跑。未建 schema / 非 SQL 表必须原样
    // 回落到 Hash op，而不是把普通 `table:key:subkey` 误判成错误。
    if let Some(pending) = conn.resp_sql_pending_hget.remove(&seq) {
        match result {
            BatchResult::GetValue(Some(bytes)) => match TableSchema::decode(bytes) {
                Ok(schema) => {
                    let schema = std::sync::Arc::new(schema);
                    conn.sql_cache.borrow_mut().schemas.insert(
                        (pending.db.to_string(), pending.table.clone()),
                        schema.clone(),
                    );
                    resp_sql_hget_with_schema(
                        conn,
                        conn_id,
                        seq,
                        worker_id,
                        pending.db,
                        pending.table,
                        pending.pk_literal,
                        pending.fields,
                        pending.mode,
                        pending.fallback,
                        schema,
                        shard_inboxes,
                        num_shards,
                    );
                    // 当前处在 reply 回调而非 socket read 尾部；若续跑产生了
                    // RowGet/fallback，需立即冲刷 RESP 的延迟投递队列。
                    conn.flush_resp_tasks(shard_inboxes);
                }
                Err(e) => {
                    conn.resp_complete(seq, codec.encode_error(&format!("bad SQL schema: {e}")))
                }
            },
            // 无 schema = 原生 KV 表；保留既有 Hash 语义。
            BatchResult::GetValue(None) => {
                push_task(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    pending.fallback,
                    shard_inboxes,
                    num_shards,
                );
                conn.flush_resp_tasks(shard_inboxes);
            }
            BatchResult::Error(e) => conn.resp_complete(seq, codec.encode_error(e)),
            _ => conn.resp_complete(seq, codec.encode_error("unexpected schema reply")),
        }
        return;
    }
    if let Some(pending) = conn.resp_sql_pending_hset.remove(&seq) {
        match result {
            BatchResult::GetValue(Some(bytes)) => match TableSchema::decode(bytes) {
                Ok(schema) => {
                    let schema = std::sync::Arc::new(schema);
                    conn.sql_cache.borrow_mut().schemas.insert(
                        (pending.db.to_string(), pending.table.clone()),
                        schema.clone(),
                    );
                    resp_sql_hset_with_schema(
                        conn,
                        conn_id,
                        seq,
                        worker_id,
                        pending.db,
                        pending.table,
                        pending.pk_literal,
                        pending.pairs,
                        pending.reply_ok,
                        pending.fallback,
                        schema,
                        shard_inboxes,
                        num_shards,
                    );
                    conn.flush_resp_tasks(shard_inboxes);
                }
                Err(e) => {
                    conn.resp_complete(seq, codec.encode_error(&format!("bad SQL schema: {e}")))
                }
            },
            BatchResult::GetValue(None) => {
                push_task(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    pending.fallback,
                    shard_inboxes,
                    num_shards,
                );
                conn.flush_resp_tasks(shard_inboxes);
            }
            BatchResult::Error(e) => conn.resp_complete(seq, codec.encode_error(e)),
            _ => conn.resp_complete(seq, codec.encode_error("unexpected schema reply")),
        }
        return;
    }
    if let Some(pending) = conn.resp_sql_pending_hdel.remove(&seq) {
        match result {
            BatchResult::GetValue(Some(bytes)) => match TableSchema::decode(bytes) {
                Ok(schema) => {
                    let schema = std::sync::Arc::new(schema);
                    conn.sql_cache.borrow_mut().schemas.insert(
                        (pending.db.to_string(), pending.table.clone()),
                        schema.clone(),
                    );
                    resp_sql_hdel_with_schema(
                        conn,
                        conn_id,
                        seq,
                        worker_id,
                        pending.db,
                        pending.table,
                        pending.pk_literal,
                        pending.fields,
                        pending.fallback,
                        schema,
                        shard_inboxes,
                        num_shards,
                    );
                    conn.flush_resp_tasks(shard_inboxes);
                }
                Err(e) => {
                    conn.resp_complete(seq, codec.encode_error(&format!("bad SQL schema: {e}")))
                }
            },
            BatchResult::GetValue(None) => {
                push_task(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    pending.fallback,
                    shard_inboxes,
                    num_shards,
                );
                conn.flush_resp_tasks(shard_inboxes);
            }
            BatchResult::Error(e) => conn.resp_complete(seq, codec.encode_error(e)),
            _ => conn.resp_complete(seq, codec.encode_error("unexpected schema reply")),
        }
        return;
    }
    if let Some(pending) = conn.resp_sql_pending_hsetnx.remove(&seq) {
        match result {
            BatchResult::GetValue(Some(bytes)) => match TableSchema::decode(bytes) {
                Ok(schema) => {
                    let schema = std::sync::Arc::new(schema);
                    conn.sql_cache.borrow_mut().schemas.insert(
                        (pending.db.to_string(), pending.table.clone()),
                        schema.clone(),
                    );
                    resp_sql_hsetnx_with_schema(
                        conn,
                        conn_id,
                        seq,
                        worker_id,
                        pending.db,
                        pending.table,
                        pending.pk_literal,
                        pending.field,
                        pending.value,
                        pending.fallback,
                        schema,
                        shard_inboxes,
                        num_shards,
                    );
                    conn.flush_resp_tasks(shard_inboxes);
                }
                Err(e) => {
                    conn.resp_complete(seq, codec.encode_error(&format!("bad SQL schema: {e}")))
                }
            },
            BatchResult::GetValue(None) => {
                push_task(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    pending.fallback,
                    shard_inboxes,
                    num_shards,
                );
                conn.flush_resp_tasks(shard_inboxes);
            }
            BatchResult::Error(e) => conn.resp_complete(seq, codec.encode_error(e)),
            _ => conn.resp_complete(seq, codec.encode_error("unexpected schema reply")),
        }
        return;
    }
    if let Some(pending) = conn.resp_sql_pending_delete.remove(&seq) {
        match result {
            BatchResult::GetValue(Some(bytes)) => match TableSchema::decode(bytes) {
                Ok(schema) => {
                    let schema = std::sync::Arc::new(schema);
                    conn.sql_cache.borrow_mut().schemas.insert(
                        (pending.db.to_string(), pending.table.clone()),
                        schema.clone(),
                    );
                    resp_sql_delete_with_schema(
                        conn,
                        conn_id,
                        seq,
                        worker_id,
                        pending.db,
                        pending.table,
                        pending.pk_literal,
                        pending.fallback,
                        schema,
                        shard_inboxes,
                        num_shards,
                    );
                    conn.flush_resp_tasks(shard_inboxes);
                }
                Err(e) => {
                    conn.resp_complete(seq, codec.encode_error(&format!("bad SQL schema: {e}")))
                }
            },
            BatchResult::GetValue(None) => {
                push_task(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    pending.fallback,
                    shard_inboxes,
                    num_shards,
                );
                conn.flush_resp_tasks(shard_inboxes);
            }
            BatchResult::Error(e) => conn.resp_complete(seq, codec.encode_error(e)),
            _ => conn.resp_complete(seq, codec.encode_error("unexpected schema reply")),
        }
        return;
    }
    if let Some(pending) = conn.resp_sql_pending_incr.remove(&seq) {
        match result {
            BatchResult::GetValue(Some(bytes)) => match TableSchema::decode(bytes) {
                Ok(schema) => {
                    let schema = std::sync::Arc::new(schema);
                    conn.sql_cache.borrow_mut().schemas.insert(
                        (pending.db.to_string(), pending.table.clone()),
                        schema.clone(),
                    );
                    resp_sql_incr_with_schema(
                        conn,
                        conn_id,
                        seq,
                        worker_id,
                        pending.db,
                        pending.table,
                        pending.pk_literal,
                        pending.field,
                        pending.delta,
                        pending.fallback,
                        schema,
                        shard_inboxes,
                        num_shards,
                    );
                    conn.flush_resp_tasks(shard_inboxes);
                }
                Err(e) => {
                    conn.resp_complete(seq, codec.encode_error(&format!("bad SQL schema: {e}")))
                }
            },
            BatchResult::GetValue(None) => {
                push_task(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    pending.fallback,
                    shard_inboxes,
                    num_shards,
                );
                conn.flush_resp_tasks(shard_inboxes);
            }
            BatchResult::Error(e) => conn.resp_complete(seq, codec.encode_error(e)),
            _ => conn.resp_complete(seq, codec.encode_error("unexpected schema reply")),
        }
        return;
    }
    // SQL RowGet 回包 → RESP HGET 的 field 投影。此处不走 SQL wire 渲染，
    // 避免额外结果集编码/解析，也保持 nil 与 Redis 一致。
    if let Some(ctx) = conn.resp_sql_hget.remove(&seq) {
        let bytes = match result {
            BatchResult::GetValue(Some(row)) => match storage::row::decode_row(&ctx.schema, row) {
                Ok(values) => {
                    resp_sql_project_reply(&codec, &ctx.schema, &values, &ctx.fields, ctx.mode)
                }
                Err(e) => codec.encode_error(&e.to_string()),
            },
            BatchResult::GetValue(None) => resp_sql_missing_reply(&codec, &ctx),
            BatchResult::Error(e) => codec.encode_error(e),
            _ => codec.encode_error("unexpected SQL row reply"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    if let Some(ctx) = conn.resp_sql_hset.remove(&seq) {
        let bytes = match result {
            BatchResult::Integer(added) => {
                if ctx.reply_ok {
                    codec.encode_ok()
                } else {
                    codec.encode_integer(*added)
                }
            }
            BatchResult::Error(e) => codec.encode_error(e),
            _ => codec.encode_error("unexpected SQL row patch reply"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ H2: HTTP KV 回包渲染 (seq 簿记, 与 SQL 钩子互斥)
    if let Some(ctx) = conn.http_ctx.remove(&seq) {
        use crate::protocol::http as h;
        use shard_manager::value_num as vn;
        let cors = crate::http_config::cors_origin();
        let bytes = match (ctx.op, result) {
            (HttpKvOp::Get, BatchResult::GetValue(Some(stored))) => {
                let (tag, payload) = crate::value_codec::decode_value(stored);
                let val = match tag {
                    vn::TAG_I64 if payload.len() == 8 => {
                        serde_json::json!(i64::from_le_bytes(payload.try_into().unwrap()))
                    }
                    vn::TAG_F64 if payload.len() == 8 => {
                        serde_json::json!(f64::from_le_bytes(payload.try_into().unwrap()))
                    }
                    _ => match std::str::from_utf8(payload) {
                        Ok(s) => serde_json::json!(s),
                        Err(_) => serde_json::json!({
                            "b64": h::base64_encode(payload),
                            "encoding": "base64",
                        }),
                    },
                };
                let body =
                    serde_json::to_vec(&serde_json::json!({ "value": val })).unwrap_or_default();
                h::build_response(200, &body, cors, ctx.keep_alive)
            }
            (HttpKvOp::Get, BatchResult::GetValue(None)) => {
                h::build_response(404, &h::error_body("not found"), cors, ctx.keep_alive)
            }
            (HttpKvOp::Put, BatchResult::PutOk) => {
                h::build_response(200, br#"{"ok":true}"#, cors, ctx.keep_alive)
            }
            (HttpKvOp::Delete, BatchResult::DeleteExisted(b)) => {
                let body =
                    serde_json::to_vec(&serde_json::json!({ "deleted": b })).unwrap_or_default();
                h::build_response(200, &body, cors, ctx.keep_alive)
            }
            (_, BatchResult::Error(e)) => {
                crate::metrics::HTTP_ERRORS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                h::build_response(500, &h::error_body(e), cors, ctx.keep_alive)
            }
            _ => h::build_response(
                500,
                &h::error_body("unexpected reply"),
                cors,
                ctx.keep_alive,
            ),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ F65: 全局 UNIQUE 占坑状态机推进 (优先于其他聚合器)
    if sql_unique_drive(
        conn,
        conn_id,
        seq,
        worker_id,
        result,
        shard_inboxes,
        num_shards,
    ) {
        return;
    }
    // ⭐ F66: 系统表 CatalogDump 回调 → 合成虚拟表
    if let Some(spec) = conn.sql_sysq.remove(&seq) {
        let bin = conn.mysql_binary.remove(&seq);
        let bytes = match result {
            BatchResult::Catalog(entries) => {
                // decode schema 字节 (跳过坏的)
                let decoded: Vec<(String, TableSchema)> = entries
                    .iter()
                    .filter_map(|(t, b)| TableSchema::decode(b).ok().map(|s| (t.clone(), s)))
                    .collect();
                sysq_render_catalog(conn.proto, bin, &spec, &conn.current_db.clone(), &decoded)
            }
            BatchResult::Error(e) => sql_err_bytes(conn.proto, e),
            _ => sql_err_bytes(conn.proto, "unexpected catalog reply"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ F67 (JOIN): 两表 hash join 状态机推进 (schema 拉取 / 两轮 gather / 完成点)
    if sql_join_drive(
        conn,
        conn_id,
        seq,
        worker_id,
        group,
        result,
        shard_inboxes,
        num_shards,
    ) {
        return;
    }
    // ⭐ portal: PG 扩展协议 Parse 挂起 (等 schema) 的续跑 — GetSchemaOp 回包到达后
    // 推断参数 OID, 插 pg_stmts, 回 ParseComplete+ParameterDescription+ReadyForQuery.
    if conn.pg_waiting_schema && conn.pg_waiting_schema_seq == seq {
        if let BatchResult::GetValue(Some(bytes)) = result
            && let Ok(s) = TableSchema::decode(bytes)
        {
            let schema = std::sync::Arc::new(s);
            conn.resume_pg_pending_parse(schema);
        } else {
            // schema 拉取失败 → 清挂起并回错
            conn.pg_pending_prepares.clear();
            conn.pg_waiting_schema = false;
            let mut out = std::mem::take(&mut conn.pg_batch).prefix;
            out.extend_from_slice(&crate::protocol::pg::build_error(
                "42P01",
                "relation does not exist",
            ));
            out.extend_from_slice(&crate::protocol::pg::build_ready());
            let s = conn.next_seq;
            conn.next_seq += 1;
            conn.resp_complete(s, out);
        }
        return;
    }
    // ⭐ X3: SQL 钩子 — schema 拉取续跑 (挂起语句在 schema 到达后继续规划)
    if let Some(pending) = conn.sql_pending.remove(&seq) {
        match result {
            BatchResult::GetValue(Some(bytes)) => match TableSchema::decode(bytes) {
                Ok(s) => {
                    let schema = std::sync::Arc::new(s);
                    // ⭐ W1: 存量表 (GetSchemaOp 拉取) 只填 schema, 不建路由
                    // (启用路由需 CREATE 时刻零数据的完备性前提)
                    conn.sql_cache
                        .borrow_mut()
                        .schemas
                        .insert((pending.db.to_string(), pending.table), schema.clone());
                    sql_run_dml(
                        conn,
                        conn_id,
                        seq,
                        worker_id,
                        &pending.db,
                        shard_inboxes,
                        num_shards,
                        schema,
                        pending.stmt,
                    );
                }
                Err(e) => {
                    conn.resp_complete(seq, sql_err_bytes(conn.proto, &format!("bad schema: {e}")));
                }
            },
            BatchResult::GetValue(None) => {
                if conn.resp_hquery.remove(&seq).is_some() {
                    conn.resp_complete(seq, codec.encode_error("HQUERY table has no SQL schema"));
                    return;
                }
                conn.resp_complete(
                    seq,
                    sql_err_bytes(
                        conn.proto,
                        &format!("table '{}' has no schema (not a SQL table)", pending.table),
                    ),
                );
            }
            BatchResult::Error(e) if conn.resp_hquery.remove(&seq).is_some() => {
                conn.resp_complete(seq, codec.encode_error(e))
            }
            BatchResult::Error(e) => conn.resp_complete(seq, sql_err_bytes(conn.proto, e)),
            _ if conn.resp_hquery.remove(&seq).is_some() => {
                conn.resp_complete(seq, codec.encode_error("unexpected HQUERY schema reply"))
            }
            _ => conn.resp_complete(seq, sql_err_bytes(conn.proto, "unexpected schema reply")),
        }
        return;
    }
    // ⭐ X3: SELECT pk 点查 — decode + 全条件过滤 → 0/1 行 (⭐ S2: COUNT → 计数)
    if let Some(ctx) = conn.sql_row_ctx.remove(&seq) {
        let bin = conn.mysql_binary.remove(&seq);
        // ⭐ v2 (F62): SERIALIZABLE 读集记录 — 首读指纹为准 (entry 不覆盖);
        // RYOW 命中 write_set 的读不经此路径 (读自己的写无需验证)
        if let Some(key) = ctx.read_key.clone()
            && let Some(txn) = conn.txn.as_mut()
        {
            let fp = match &result {
                BatchResult::GetValue(Some(row)) => Some(storage::wal::crc32(row)),
                _ => None,
            };
            txn.read_set.entry(key).or_insert(fp);
        }
        let bytes = match result {
            BatchResult::GetValue(Some(row)) => {
                match storage::row::decode_row(&ctx.schema, row) {
                    Ok(mut values) => {
                        // ⭐ RYOW (F63): 事务内 UPDATE 基于此盘行 → 叠加未提交 sets
                        // (表达式对叠加中的 values 求值 — 与 row_update 同语义)
                        for (ci, sv) in &ctx.ryow_overlay {
                            let nv = match sv {
                                storage::row::SetVal::Val(cv) => cv.clone(),
                                storage::row::SetVal::Expr(e) => {
                                    storage::row::eval_row_expr(e, &values)
                                }
                            };
                            if let Some(slot) = values.get_mut(*ci as usize) {
                                *slot = nv;
                            }
                        }
                        let hit = eval_pred(&ctx.schema, &values, &ctx.conds);
                        // ⭐ F71: 内层子查询 → 捕获 0/1 行 (投影/计数) 而非渲染
                        if conn.sql_subq.contains_key(&seq) {
                            let captured: Vec<Vec<ColValue>> = if !hit {
                                vec![]
                            } else if ctx.count {
                                vec![vec![ColValue::I64(1)]]
                            } else {
                                vec![
                                    ctx.proj
                                        .iter()
                                        .map(|&i| values[i as usize].clone())
                                        .collect(),
                                ]
                            };
                            sql_subq_advance(
                                conn,
                                conn_id,
                                seq,
                                worker_id,
                                default_db,
                                db_view,
                                shard_inboxes,
                                num_shards,
                                captured,
                            );
                            return;
                        }
                        // ⭐ F72: 派生表内层 (pk 点查形态) → 物化后 worker 内存执行外层
                        if conn.sql_derived.contains_key(&seq) {
                            let (cols, captured) = derived_capture_rowctx(&ctx, hit, &values);
                            finish_derived(
                                conn,
                                conn_id,
                                seq,
                                worker_id,
                                bin,
                                shard_inboxes,
                                num_shards,
                                cols,
                                captured,
                            );
                            return;
                        }
                        if conn.resp_hquery.remove(&seq).is_some() {
                            let rows = if hit {
                                vec![
                                    ctx.proj
                                        .iter()
                                        .map(|&i| values[i as usize].clone())
                                        .collect(),
                                ]
                            } else {
                                Vec::new()
                            };
                            let cols: Vec<(String, ColType)> = ctx
                                .proj
                                .iter()
                                .map(|&i| {
                                    let c = &ctx.schema.columns[i as usize];
                                    (c.name.clone(), c.ty)
                                })
                                .collect();
                            conn.resp_complete(seq, resp_hquery_rows(&codec, &cols, &rows));
                            return;
                        }
                        if hit {
                            if ctx.count {
                                render_sql_count(conn.proto, bin, 1)
                            } else {
                                render_sql_rows(
                                    conn.proto,
                                    bin,
                                    &ctx.schema,
                                    &ctx.proj,
                                    &ctx.out_names,
                                    &[values],
                                )
                            }
                        } else if ctx.count {
                            render_sql_count(conn.proto, bin, 0)
                        } else {
                            render_sql_rows(
                                conn.proto,
                                bin,
                                &ctx.schema,
                                &ctx.proj,
                                &ctx.out_names,
                                &[],
                            )
                        }
                    }
                    Err(e) => sql_err_bytes(conn.proto, &e.to_string()),
                }
            }
            BatchResult::GetValue(None) if conn.sql_subq.contains_key(&seq) => {
                // ⭐ F71: 内层子查询空结果
                let captured: Vec<Vec<ColValue>> = if ctx.count {
                    vec![vec![ColValue::I64(0)]]
                } else {
                    vec![]
                };
                sql_subq_advance(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    default_db,
                    db_view,
                    shard_inboxes,
                    num_shards,
                    captured,
                );
                return;
            }
            BatchResult::GetValue(None) if conn.sql_derived.contains_key(&seq) => {
                // ⭐ F72: 派生表内层空结果
                let (cols, captured) = derived_capture_rowctx(&ctx, false, &[]);
                finish_derived(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    bin,
                    shard_inboxes,
                    num_shards,
                    cols,
                    captured,
                );
                return;
            }
            BatchResult::GetValue(None) if conn.resp_hquery.remove(&seq).is_some() => {
                b"*0\r\n".to_vec()
            }
            BatchResult::GetValue(None) if ctx.count => render_sql_count(conn.proto, bin, 0),
            BatchResult::GetValue(None) => {
                render_sql_rows(conn.proto, bin, &ctx.schema, &ctx.proj, &ctx.out_names, &[])
            }
            BatchResult::Error(e) => sql_err_bytes(conn.proto, e),
            _ => sql_err_bytes(conn.proto, "unexpected reply"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ X3: SELECT 索引路径广播聚合 (⭐ O3: unique 等值可早停; ⭐ S1: DML phase1)
    if conn.sql_select_agg.contains_key(&seq) {
        let proto = conn.proto;
        let bin = conn.mysql_binary.contains(&seq); // ⭐ P2 (借用前 peek)
        let is_hquery = conn.resp_hquery.contains_key(&seq);
        // ⭐ F71: 此 agg 属内层子查询 → 完成时 materialize 行集而非渲染
        let is_subq_inner = conn.sql_subq.contains_key(&seq);
        // ⭐ F72: 此 agg 属派生表内层 → 完成时物化 (列定义+行集) 交 finish_derived
        let is_derived = conn.sql_derived.contains_key(&seq);
        enum Fire {
            No,
            Reply(Vec<u8>),
            Dml {
                pks: Vec<Vec<u8>>,
                action: SqlDmlAction,
                target: (std::sync::Arc<str>, String),
            },
            SubqInner(Vec<Vec<ColValue>>),
            DerivedDone(MatResult),
            HQuery(MatResult),
        }
        let (fire, drained) = {
            let agg = conn.sql_select_agg.get_mut(&seq).expect("just checked");
            if !agg.done {
                match result {
                    BatchResult::Rows(rows) => agg.rows.extend(rows.iter().cloned()),
                    // ⭐ P0-2: 投影下推路径 — shard 回 row_cols 列, 收进 plain_rows
                    BatchResult::ProjRows(rows) if !agg.down_proj.is_empty() => {
                        agg.plain_rows.extend(rows.iter().cloned());
                    }
                    BatchResult::Error(e) => agg.error = Some(e.clone()),
                    _ => agg.error = Some("unexpected reply".into()),
                }
            }
            agg.remaining -= 1;
            // 回复时机: 全部回齐, 或 unique 等值首个非空/出错即早停 (DML 禁早停)
            let should_fire = !agg.done
                && (agg.remaining == 0
                    || (agg.unique_early && (!agg.rows.is_empty() || agg.error.is_some())));
            let fire = if should_fire {
                agg.done = true;
                match agg.dml.take() {
                    // ⭐ S1: DML phase1 完成 — 过滤取 pk (出错则直接回错)
                    Some(action) if agg.error.is_none() => match collect_dml_pks(agg) {
                        Ok(pks) => Fire::Dml {
                            pks,
                            action,
                            target: agg.dml_target.take().expect("dml 必带 target"),
                        },
                        Err(e) => Fire::Reply(sql_err_bytes(proto, &e)),
                    },
                    Some(_) => Fire::Reply(sql_err_bytes(
                        proto,
                        agg.error.as_deref().unwrap_or("error"),
                    )),
                    // ⭐ F71: 内层子查询 → materialize 行集捕获; 否则正常渲染
                    None if is_subq_inner => match materialize_select_agg(agg) {
                        Ok((_cols, rows)) => Fire::SubqInner(rows),
                        Err(e) => Fire::Reply(sql_err_bytes(proto, &e)),
                    },
                    // ⭐ F72: 派生表内层 → 物化 (含错误; 清理在 fire 处)
                    None if is_derived => Fire::DerivedDone(materialize_select_agg(agg)),
                    None if is_hquery => Fire::HQuery(materialize_select_agg(agg)),
                    None => Fire::Reply(render_select_agg(proto, bin, agg)),
                }
            } else {
                Fire::No
            };
            (fire, agg.remaining == 0)
        };
        // agg 保留至全部回包收齐 (迟到回包只减计数丢结果, 防重复 complete)
        if drained {
            conn.sql_select_agg.remove(&seq);
            conn.mysql_binary.remove(&seq);
        }
        match fire {
            Fire::No => {}
            Fire::Reply(bytes) => conn.resp_complete(seq, bytes),
            Fire::HQuery(res) => {
                conn.resp_hquery.remove(&seq);
                let bytes = match res {
                    Ok((cols, rows)) => resp_hquery_rows(&codec, &cols, &rows),
                    Err(e) => codec.encode_error(&e),
                };
                conn.resp_complete(seq, bytes);
            }
            // ⭐ F71: 内层子查询完成 → 存行集并推进编排 (行数上限护栏)
            Fire::SubqInner(rows) => {
                // ⭐ F73: 捕获阶段 OOM 护栏 (精确语义在 fold_one_subq 按叶子类型);
                // IN >SUBQ_IN_MAX / scalar >1 由 fold 报错, EXISTS 无上限但捕获封顶
                if rows.len() > SUBQ_IN_MAX {
                    conn.sql_subq.remove(&seq);
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(proto, "subquery result too large; rewrite as JOIN"),
                    );
                    return;
                }
                sql_subq_advance(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    default_db,
                    db_view,
                    shard_inboxes,
                    num_shards,
                    rows,
                );
            }
            // ⭐ F72: 派生表内层完成 → worker 内存执行外层 (错误时清理 ctx)
            Fire::DerivedDone(res) => match res {
                Ok((cols, rows)) => finish_derived(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    bin,
                    shard_inboxes,
                    num_shards,
                    cols,
                    rows,
                ),
                Err(e) => {
                    conn.sql_derived.remove(&seq);
                    // ⭐ FK 级联: 级联 job 的 phase1 收集出错 → 推进级联 (不回复)
                    if is_cascade_seq(seq) {
                        if let Some(job) = conn.cascade_jobs.remove(&seq) {
                            cascade_job_done(
                                conn,
                                conn_id,
                                seq,
                                worker_id,
                                default_db,
                                db_view,
                                shard_inboxes,
                                num_shards,
                                0,
                                Some(e),
                                &job,
                            );
                            return;
                        }
                    }
                    conn.resp_complete(seq, sql_err_bytes(proto, &e));
                }
            },
            Fire::Dml {
                pks,
                action,
                target,
            } => {
                // ⭐ PG 兼容 (FMT_VER 8): 记录被删 pk — 主 DELETE 或级联 DELETE
                // 的 phase1 完成即存 (DmlAgg 完成时触发/推进级联); 仅 Delete 需要.
                if matches!(action, SqlDmlAction::Delete) {
                    conn.cascade_pending
                        .insert(seq, (target.0.clone(), target.1.clone(), pks.clone()));
                }
                // ⭐ 事务 v1 (F61): 两阶段 DML 的 phase2 在事务中截流
                // (phase1 读的是已提交态 — v1 文档化语义)
                if conn.txn.is_some() {
                    let n = pks.len() as u64;
                    for pk in pks {
                        let op = sql_dml_op(&target.0, &target.1, pk, &action);
                        if let Err(e) = txn_buffer_op(conn, op) {
                            if is_cascade_seq(seq) {
                                if let Some(job) = conn.cascade_jobs.remove(&seq) {
                                    cascade_job_done(
                                        conn,
                                        conn_id,
                                        seq,
                                        worker_id,
                                        default_db,
                                        db_view,
                                        shard_inboxes,
                                        num_shards,
                                        0,
                                        Some(e),
                                        &job,
                                    );
                                }
                            } else {
                                conn.resp_complete(seq, sql_err_bytes(proto, &e));
                            }
                            return;
                        }
                    }
                    if is_cascade_seq(seq) {
                        if let Some(job) = conn.cascade_jobs.remove(&seq) {
                            cascade_job_done(
                                conn,
                                conn_id,
                                seq,
                                worker_id,
                                default_db,
                                db_view,
                                shard_inboxes,
                                num_shards,
                                n,
                                None,
                                &job,
                            );
                        }
                    } else {
                        conn.resp_complete(seq, sql_ok_bytes(proto, n));
                    }
                    return;
                }
                // phase2: 逐 pk 按路由下发 (DML 禁早停保证此刻 phase1 已 drained,
                // 同 seq 注册 dml_agg 无双聚合并存)
                debug_assert!(drained, "DML phase1 必须全量回齐后才 fire");
                if pks.is_empty() {
                    // ⭐ FK 级联: 级联 job 无匹配引用行 → 推进 (无更深递归)
                    if is_cascade_seq(seq) {
                        if let Some(job) = conn.cascade_jobs.remove(&seq) {
                            cascade_job_done(
                                conn,
                                conn_id,
                                seq,
                                worker_id,
                                default_db,
                                db_view,
                                shard_inboxes,
                                num_shards,
                                0,
                                None,
                                &job,
                            );
                        }
                    } else {
                        conn.resp_complete(seq, sql_ok_bytes(proto, 0));
                    }
                } else {
                    conn.sql_dml_agg.insert(
                        seq,
                        SqlDmlAgg {
                            remaining: pks.len(),
                            affected: 0,
                            error: None,
                            drop_key: None,
                        },
                    );
                    for pk in pks {
                        let op = sql_dml_op(&target.0, &target.1, pk, &action);
                        let sid = hash_route_op(&op, num_shards);
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
        return;
    }
    // ⭐ 事务 v1 (F61): COMMIT 的 TxnApply 多 shard 聚合 — 全 OK 回 commit ok
    // (此刻各 shard 已 wal_barrier, 回复到达 ⇒ 已持久);
    // 任一失败回错 (跨 shard 已应用分片不回滚 — v1 gap 文档化)
    if let Some(agg) = conn.sql_txn_agg.get_mut(&seq) {
        match result {
            BatchResult::TxnApplied(n) => agg.applied += n,
            BatchResult::Error(e) => agg.error = Some(e.clone()),
            _ => agg.error = Some("unexpected reply".into()),
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.sql_txn_agg.remove(&seq).expect("just checked");
            conn.mysql_binary.remove(&seq);
            let bytes = match agg.error {
                Some(e) => sql_err_bytes(conn.proto, &format!("commit failed: {e}")),
                None => sql_ok_bytes(conn.proto, agg.applied),
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // ⭐ PG 兼容 (引用完整性, FMT_VER 8): 外键存在性预检回包
    // (RowGet 对父表; 全存在才在 on_reply 内注册 sql_dml_agg 发原 RowPut)
    if sql_fk_on_reply(
        conn,
        conn_id,
        seq,
        worker_id,
        shard_inboxes,
        num_shards,
        result,
    ) {
        return;
    }
    // ⭐ S1: DML 计数聚合 (INSERT 多行 / DELETE·UPDATE phase2 / DROP 广播)
    if let Some(agg) = conn.sql_dml_agg.get_mut(&seq) {
        match result {
            BatchResult::PutOk => agg.affected += 1,
            BatchResult::DeleteExisted(true) => agg.affected += 1,
            BatchResult::DeleteExisted(false) => {}
            BatchResult::Error(e) => agg.error = Some(e.clone()),
            _ => agg.error = Some("unexpected reply".into()),
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.sql_dml_agg.remove(&seq).expect("just checked");
            conn.mysql_binary.remove(&seq);
            // ⭐ FK 级联 (FMT_VER 8): 级联子任务完成 → 推进级联 (不回复客户端)
            if let Some(job) = conn.cascade_jobs.remove(&seq) {
                cascade_job_done(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    default_db,
                    db_view,
                    shard_inboxes,
                    num_shards,
                    agg.affected,
                    agg.error.clone(),
                    &job,
                );
                return;
            }
            // ⭐ FK 级联: 根 DELETE 完成且无错误 → 有引用表则进入级联 (延迟回复)
            if agg.error.is_none() && agg.drop_key.is_none() {
                if let Some((db, t, pks)) = conn.cascade_pending.remove(&seq) {
                    let affected = agg.affected;
                    if cascade_kickoff(
                        conn,
                        conn_id,
                        seq,
                        worker_id,
                        &db,
                        default_db,
                        db_view,
                        shard_inboxes,
                        num_shards,
                        &t,
                        pks,
                        affected,
                    ) {
                        return;
                    }
                }
            }
            // ⭐ PG 兼容 (multi-statement): 该 seq 属于多语句序列 → 推进下一条
            // (不直接回复客户端)
            if conn.multi_sub_seq.contains_key(&seq) {
                let had_err = agg.error.is_some();
                if had_err {
                    if let Some(orig) = conn.multi_sub_seq.get(&seq).cloned() {
                        if let Some(m) = conn.multi_stmt.get_mut(&orig) {
                            m.error = Some(agg.error.clone().unwrap_or_default());
                            m.stmts.clear();
                        }
                    }
                }
                conn.multi_step(
                    seq,
                    conn_id,
                    worker_id,
                    default_db,
                    db_view,
                    shard_inboxes,
                    num_shards,
                );
                return;
            }
            let bytes = match agg.error {
                Some(e) => sql_err_bytes(conn.proto, &e),
                None => {
                    let affected = if let Some(key) = agg.drop_key {
                        // DROP 完成: 本 worker schema 缓存 + 进程级路由/注册清理,
                        // DDL epoch +1 (其它 worker 靠 epoch 失效重拉)
                        conn.sql_cache.borrow_mut().schemas.remove(&key);
                        let sh = &conn.sql_shared;
                        sh.created_here.write().unwrap().remove(&key);
                        // ⭐ FK 级联 (FMT_VER 8): 移除该表的外键反向引用
                        sh.unregister_fks(&key.0, &key.1);
                        sh.routes
                            .write()
                            .unwrap()
                            .retain(|(d, t, _), _| !(d == &key.0 && t == &key.1));
                        sh.ddl_epoch
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        0
                    } else {
                        agg.affected
                    };
                    sql_ok_bytes(conn.proto, affected)
                }
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // ⭐ X3: CREATE TABLE 广播聚合 (全 shard PutOk → 填缓存 + OK)
    if let Some(agg) = conn.sql_ddl_agg.get_mut(&seq) {
        match result {
            BatchResult::PutOk => {}
            BatchResult::Error(e) => agg.error = Some(e.clone()),
            _ => agg.error = Some("unexpected reply".into()),
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.sql_ddl_agg.remove(&seq).expect("just checked");
            conn.mysql_binary.remove(&seq);
            // ⭐ PG 兼容 (multi-statement): 该 seq 属多语句序列 → 推进下一条.
            // DDL 的 schema 副作用在此处先应用 (CREATE 必须注册), 再 multi_step.
            if conn.multi_sub_seq.contains_key(&seq) {
                if let Some(orig) = conn.multi_sub_seq.get(&seq).cloned() {
                    // 应用 DDL schema 副作用 (CREATE 成功注册)
                    if agg.error.is_none() {
                        // 复用下方逻辑: 提取 key/schema 做注册
                        let mut routes = conn.sql_shared.routes.write().unwrap();
                        for idx in &agg.schema.indexes {
                            routes
                                .entry((agg.key.0.clone(), agg.key.1.clone(), idx.iid))
                                .or_insert_with(|| {
                                    std::sync::Arc::new(
                                        (0..num_shards)
                                            .map(|_| storage::index_bloom::IndexBloom::new())
                                            .collect(),
                                    )
                                });
                        }
                        drop(routes);
                        conn.sql_shared
                            .created_here
                            .write()
                            .unwrap()
                            .insert(agg.key.clone());
                        conn.sql_cache
                            .borrow_mut()
                            .schemas
                            .insert(agg.key.clone(), agg.schema.clone());
                        if !agg.schema.fks.is_empty() {
                            conn.sql_shared
                                .register_fks(&agg.key.0, &agg.key.1, &agg.schema);
                        }
                        if agg.alter {
                            conn.sql_shared
                                .ddl_epoch
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }
                    } else {
                        if let Some(m) = conn.multi_stmt.get_mut(&orig) {
                            m.error = Some(agg.error.clone().unwrap_or_default());
                            m.stmts.clear();
                        }
                    }
                }
                conn.multi_step(
                    seq,
                    conn_id,
                    worker_id,
                    default_db,
                    db_view,
                    shard_inboxes,
                    num_shards,
                );
                return;
            }
            let bytes = match agg.error {
                Some(e) => sql_err_bytes(conn.proto, &e),
                None => {
                    // ⭐ W1/W2 → ORM-B2: CREATE 成功 → schema (本 worker) +
                    // created_here + 空路由 bloom (进程级共享 — 建表时刻零数据,
                    // 空 bloom 即完备; 跨 worker/门面 INSERT 都喂同一实例)
                    {
                        let sh = &conn.sql_shared;
                        let mut routes = sh.routes.write().unwrap();
                        for idx in &agg.schema.indexes {
                            routes
                                .entry((agg.key.0.clone(), agg.key.1.clone(), idx.iid))
                                .or_insert_with(|| {
                                    std::sync::Arc::new(
                                        (0..num_shards)
                                            .map(|_| storage::index_bloom::IndexBloom::new())
                                            .collect(),
                                    )
                                });
                        }
                        drop(routes);
                        sh.created_here.write().unwrap().insert(agg.key.clone());
                    }
                    conn.sql_cache
                        .borrow_mut()
                        .schemas
                        .insert(agg.key.clone(), agg.schema.clone());
                    // ⭐ FK 级联 (FMT_VER 8): 注册外键反向引用 (含 ALTER ADD COLUMN?)
                    if !agg.schema.fks.is_empty() {
                        conn.sql_shared
                            .register_fks(&agg.key.0, &agg.key.1, &agg.schema);
                    }
                    // ⭐ F79: ALTER 递增 ddl_epoch — 其他 worker 下次 dispatch 重拉新 schema,
                    // 避免用旧列数解码新写的行 (同 DROP 先例)
                    if agg.alter {
                        conn.sql_shared
                            .ddl_epoch
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    sql_ok_bytes(conn.proto, 0)
                }
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // ⭐ Y2: SQL conn 的裸结果兜底 (Sql/Pg 共用)
    if matches!(conn.proto, ProtocolKind::Sql | ProtocolKind::Pg) {
        let bytes = match result {
            BatchResult::PutOk => sql_ok_bytes(conn.proto, 1),
            BatchResult::Error(e) => sql_err_bytes(conn.proto, e),
            _ => sql_err_bytes(conn.proto, "unexpected reply"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ Phase G: Geo 渲染钩子 (复用 ZMScore/ZRange 结果, 优先拦截)
    if let Some(ctx) = conn.geo_ctx.remove(&seq) {
        let bytes = render_geo(&codec, ctx, result);
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ Phase B: Bitmap 读渲染钩子 (Get 结果 + 位运算)
    if let Some(ctx) = conn.bit_ctx.remove(&seq) {
        let bytes = render_bit(&codec, ctx, result);
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ MGET 聚合: Values 按组索引表回填原始槽, 全组回齐拼 *N 数组
    if let Some(agg) = conn.mget_agg.get_mut(&seq) {
        match result {
            BatchResult::Values(vs) => {
                if let Some(idxs) = agg.groups.get(group as usize) {
                    for (v, &orig) in vs.iter().zip(idxs.iter()) {
                        agg.slots[orig] = v.clone();
                    }
                }
            }
            BatchResult::Error(e) if agg.error.is_none() => {
                agg.error = Some(e.clone());
            }
            _ => {}
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.mget_agg.remove(&seq).expect("just checked");
            let bytes = if let Some(e) = agg.error {
                codec.encode_error(&e)
            } else {
                let mut out = format!("*{}\r\n", agg.slots.len()).into_bytes();
                for slot in &agg.slots {
                    match slot {
                        Some(stored) => {
                            // ⭐ N3: 按 tag 渲染 (数值二进制 → 字符串)
                            out.extend_from_slice(&codec.encode_bulk(&render(stored)));
                        }
                        None => out.extend_from_slice(b"$-1\r\n"),
                    }
                }
                out
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // ⭐ MSET 聚合: 全组 MultiPutOk → +OK
    if let Some(agg) = conn.mset_agg.get_mut(&seq) {
        if let BatchResult::Error(e) = result
            && agg.error.is_none()
        {
            agg.error = Some(e.clone());
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.mset_agg.remove(&seq).expect("just checked");
            let bytes = match agg.error {
                Some(e) => codec.encode_error(&e),
                None => codec.encode_ok(),
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // ⭐ EXISTS 聚合: GetValue(Some) 计数, 全部回齐回 :n
    if let Some(agg) = conn.exists_agg.get_mut(&seq) {
        if let BatchResult::GetValue(Some(_)) = result {
            agg.count += 1;
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let count = agg.count;
            conn.exists_agg.remove(&seq);
            conn.resp_complete(seq, codec.encode_integer(count));
        }
        return;
    }
    // ⭐ STRLEN/TYPE/HEXISTS: Get 结果语义转换
    if let Some(kind) = conn.get_kind.remove(&seq) {
        let bytes = match (kind, result) {
            (GetKind::Strlen, BatchResult::GetValue(None)) => codec.encode_integer(0),
            (GetKind::Strlen, BatchResult::GetValue(Some(stored))) => {
                // ⭐ N3: 数值 tag 按渲染后字符串计长 (Redis 语义)
                codec.encode_integer(render(stored).len() as i64)
            }
            (GetKind::TypeOf, BatchResult::GetValue(None)) => codec.encode_simple("none"),
            (GetKind::TypeOf, BatchResult::GetValue(Some(_))) => codec.encode_simple("string"),
            // ⭐ Phase H: HEXISTS — HGet 结果转 0/1
            (GetKind::HExists, BatchResult::GetValue(None)) => codec.encode_integer(0),
            (GetKind::HExists, BatchResult::GetValue(Some(_))) => codec.encode_integer(1),
            (_, BatchResult::Error(e)) => codec.encode_error(e),
            _ => codec.encode_error("unexpected result"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ Phase H: HMSET — Integer 结果转 +OK
    if conn.hmset_ok.remove(&seq) {
        let bytes = match result {
            BatchResult::Integer(_) => codec.encode_ok(),
            BatchResult::Error(e) => codec.encode_error(e),
            _ => codec.encode_error("unexpected result"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ GETRANGE: Get 结果渲染后按 (start,end) 切片 (支持负索引)
    if let Some((start, end)) = conn.getrange_ctx.remove(&seq) {
        let bytes = match result {
            BatchResult::GetValue(None) => codec.encode_bulk(b""),
            BatchResult::GetValue(Some(stored)) => {
                let s = render(stored);
                codec.encode_bulk(getrange_slice(s.as_ref(), start, end))
            }
            BatchResult::Error(e) => codec.encode_error(e),
            _ => codec.encode_error("unexpected result"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ MSETNX 聚合: 全组 Integer(1) → :1, 任一非 1 → :0
    if let Some(agg) = conn.msetnx_agg.get_mut(&seq) {
        if !matches!(result, BatchResult::Integer(1)) {
            agg.all_set = false;
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let all = agg.all_set;
            conn.msetnx_agg.remove(&seq);
            conn.resp_complete(seq, codec.encode_integer(i64::from(all)));
        }
        return;
    }
    // ⭐ Phase Set: SINTER/SUNION/SDIFF 聚合 — 全部 key 的成员回齐后求代数
    if let Some(agg) = conn.setalg_agg.get_mut(&seq) {
        match result {
            BatchResult::Members(ms) => {
                if let Some(slot) = agg.sets.get_mut(group as usize) {
                    *slot = Some(ms.clone());
                }
            }
            BatchResult::Error(e) if agg.error.is_none() => {
                agg.error = Some(e.clone());
            }
            _ => {}
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.setalg_agg.remove(&seq).expect("just checked");
            if let Some(e) = agg.error {
                conn.resp_complete(seq, codec.encode_error(&e));
                return;
            }
            use std::collections::HashSet;
            let (card_only, limit) = (agg.card_only, agg.limit);
            let store_dst = agg.store_dst;
            // ⭐ D3: 二阶段任务用命令发起时的 (db, table), 不受后续 SELECT 影响
            let (agg_db, agg_table) = (agg.db.clone(), agg.table.clone());
            let mut sets: Vec<Vec<Vec<u8>>> = agg
                .sets
                .into_iter()
                .map(|s| s.unwrap_or_default())
                .collect();
            let first = if sets.is_empty() {
                Vec::new()
            } else {
                sets.remove(0)
            };
            let out: Vec<Vec<u8>> = match agg.op {
                SetAlgOp::Inter => {
                    let others: Vec<HashSet<&[u8]>> = sets
                        .iter()
                        .map(|s| s.iter().map(|m| m.as_slice()).collect())
                        .collect();
                    first
                        .into_iter()
                        .filter(|m| others.iter().all(|o| o.contains(m.as_slice())))
                        .collect()
                }
                SetAlgOp::Diff => {
                    let others: Vec<HashSet<&[u8]>> = sets
                        .iter()
                        .map(|s| s.iter().map(|m| m.as_slice()).collect())
                        .collect();
                    first
                        .into_iter()
                        .filter(|m| !others.iter().any(|o| o.contains(m.as_slice())))
                        .collect()
                }
                SetAlgOp::Union => {
                    let mut seen: HashSet<Vec<u8>> = HashSet::new();
                    let mut out = Vec::new();
                    for m in first.into_iter().chain(sets.into_iter().flatten()) {
                        if seen.insert(m.clone()) {
                            out.push(m);
                        }
                    }
                    out
                }
            };
            // ⭐ C3: *STORE — 结果写 dst (同 shard FIFO: 先 Delete 再 SAdd), 完成后回 :card
            if let Some(dst) = store_dst {
                let card = out.len() as i64;
                let sid = hash_route_key(agg_db.as_ref(), agg_table.as_ref(), &dst, num_shards);
                let mut remaining = 1usize;
                let del = BatchOp::Delete {
                    db: agg_db.clone(),
                    table: agg_table.clone(),
                    key: dst.clone(),
                };
                push_task_grouped(conn_id, seq, worker_id, 0, sid, del, shard_inboxes);
                if !out.is_empty() {
                    remaining += 1;
                    let sadd = BatchOp::SAdd {
                        db: agg_db,
                        table: agg_table,
                        key: dst,
                        members: out,
                    };
                    push_task_grouped(conn_id, seq, worker_id, 1, sid, sadd, shard_inboxes);
                }
                conn.store_agg.insert(
                    seq,
                    StoreFinishAgg {
                        remaining,
                        card,
                        error: None,
                    },
                );
                return;
            }
            // ⭐ C1: SINTERCARD — 只回势 (LIMIT 截断); 否则回成员数组
            let bytes = if card_only {
                let card = if limit > 0 {
                    out.len().min(limit)
                } else {
                    out.len()
                };
                codec.encode_integer(card as i64)
            } else {
                let mut buf = format!("*{}\r\n", out.len()).into_bytes();
                for m in &out {
                    buf.extend_from_slice(&codec.encode_bulk(m));
                }
                buf
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // ⭐ C3: ZINTERSTORE/ZUNIONSTORE 源聚合 — ZRange(withscores) 交替串还原 (member, score)
    if let Some(agg) = conn.zstore_agg.get_mut(&seq) {
        match result {
            BatchResult::Members(ms) => {
                let mut rows = Vec::with_capacity(ms.len() / 2);
                let mut i = 0;
                while i + 1 < ms.len() {
                    let score = std::str::from_utf8(&ms[i + 1])
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    rows.push((ms[i].clone(), score));
                    i += 2;
                }
                if let Some(slot) = agg.sets.get_mut(group as usize) {
                    *slot = Some(rows);
                }
            }
            BatchResult::Error(e) if agg.error.is_none() => {
                agg.error = Some(e.clone());
            }
            _ => {}
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.zstore_agg.remove(&seq).expect("just checked");
            if let Some(e) = agg.error {
                conn.resp_complete(seq, codec.encode_error(&e));
                return;
            }
            // SUM 聚合 (首现序保序; inter 要求出现在全部源)
            let inter = agg.inter;
            let n_sets = agg.sets.len();
            // ⭐ D3: 二阶段任务用命令发起时的 (db, table)
            let (agg_db, agg_table) = (agg.db.clone(), agg.table.clone());
            let mut acc: Vec<(Vec<u8>, f64, usize)> = Vec::new();
            let mut pos: HashMap<Vec<u8>, usize> = HashMap::new();
            for set in agg.sets.into_iter().map(|s| s.unwrap_or_default()) {
                for (m, sc) in set {
                    match pos.get(&m) {
                        Some(&i) => {
                            acc[i].1 += sc;
                            acc[i].2 += 1;
                        }
                        None => {
                            pos.insert(m.clone(), acc.len());
                            acc.push((m, sc, 1));
                        }
                    }
                }
            }
            let pairs: Vec<(f64, Vec<u8>)> = acc
                .into_iter()
                .filter(|(_, _, cnt)| !inter || *cnt == n_sets)
                .map(|(m, sc, _)| (sc, m))
                .collect();
            let card = pairs.len() as i64;
            let dst = agg.dst;
            let sid = hash_route_key(agg_db.as_ref(), agg_table.as_ref(), &dst, num_shards);
            let mut remaining = 1usize;
            let del = BatchOp::Delete {
                db: agg_db.clone(),
                table: agg_table.clone(),
                key: dst.clone(),
            };
            push_task_grouped(conn_id, seq, worker_id, 0, sid, del, shard_inboxes);
            if !pairs.is_empty() {
                remaining += 1;
                let zadd = BatchOp::ZAdd {
                    db: agg_db,
                    table: agg_table,
                    key: dst,
                    pairs,
                };
                push_task_grouped(conn_id, seq, worker_id, 1, sid, zadd, shard_inboxes);
            }
            conn.store_agg.insert(
                seq,
                StoreFinishAgg {
                    remaining,
                    card,
                    error: None,
                },
            );
        }
        return;
    }
    // ⭐ C3: *STORE 第二阶段 (Delete + SAdd/ZAdd) 全部完成 → 回 :card
    if let Some(agg) = conn.store_agg.get_mut(&seq) {
        if let BatchResult::Error(e) = result
            && agg.error.is_none()
        {
            agg.error = Some(e.clone());
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.store_agg.remove(&seq).expect("just checked");
            let bytes = match agg.error {
                Some(e) => codec.encode_error(&e),
                None => codec.encode_integer(agg.card),
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // DEL 聚合路径
    if let Some(agg) = conn.del_agg.get_mut(&seq) {
        match result {
            BatchResult::DeleteExisted(existed) => {
                if *existed {
                    agg.count += 1;
                }
            }
            BatchResult::Error(_) => {
                // 单 key 失败按未删除计 (Redis DEL 语义: 返回实际删除数)
            }
            _ => {}
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let count = agg.count;
            conn.del_agg.remove(&seq);
            conn.resp_complete(seq, codec.encode_integer(count));
        }
        return;
    }

    let bytes = match result {
        // ⭐ 事务 v1: TxnApplied 只出现在 SQL 门面 (上方 sql_txn_agg 已拦截)
        BatchResult::TxnApplied(_) => codec.encode_error("unexpected txn reply"),
        // ⭐ M3-2: 行数估计只出现在 worker 内部 (JOIN 驱动选择), 门面拦截
        BatchResult::RowCount(_) => codec.encode_error("unexpected rowcount reply"),
        // ⭐ M3-4: distinct 估计只出现在 worker 内部 (JOIN 索引选择)
        BatchResult::DistinctCounts(_) => codec.encode_error("unexpected distinct reply"),
        // ⭐ M3-5: min/max 估计只出现在 worker 内部 (JOIN 范围选择)
        BatchResult::RangeBounds(_) => codec.encode_error("unexpected range reply"),
        // ⭐ F65: 占坑结果只出现在 SQL 门面 (sql_unique_drive 已拦截)
        BatchResult::ReserveOk | BatchResult::ReserveConflict { .. } => {
            codec.encode_error("unexpected unique reply")
        }
        BatchResult::Catalog(_) => codec.encode_error("unexpected catalog reply"),
        BatchResult::ProjRows(_) => codec.encode_error("unexpected join reply"),
        BatchResult::PutOk | BatchResult::MultiPutOk => codec.encode_ok(),
        BatchResult::GetValue(None) => codec.encode_nil(),
        BatchResult::GetValue(Some(stored)) => {
            // ⭐ N3: 按 tag 渲染 (RAW 借用零拷贝; 数值二进制 → 字符串)
            codec.encode_bulk(&render(stored))
        }
        BatchResult::DeleteExisted(existed) => codec.encode_integer(*existed as i64),
        BatchResult::Integer(n) => codec.encode_integer(*n),
        // INCRBYFLOAT: Redis 语义回 bulk string (非 integer)
        BatchResult::Double(f) => codec.encode_bulk(format!("{f}").as_bytes()),
        // ⭐ Phase H: HMGET 单 op 直回 Values → *N 数组 (逐项渲染;
        // ⭐ C1: ZMSCORE 的 Values 已成形, 裸 bulk 直出)
        BatchResult::Values(vs) => {
            let raw = conn.values_raw.remove(&seq);
            let mut out = format!("*{}\r\n", vs.len()).into_bytes();
            for v in vs {
                match v {
                    Some(stored) => {
                        if raw {
                            out.extend_from_slice(&codec.encode_bulk(stored));
                        } else {
                            out.extend_from_slice(&codec.encode_bulk(&render(stored)));
                        }
                    }
                    None => out.extend_from_slice(b"$-1\r\n"),
                }
            }
            out
        }
        // ⭐ Phase H: HGETALL/HKEYS/HVALS/HSCAN 按 pairs_kind 渲染
        BatchResult::Pairs(ps) => {
            let kind = conn.pairs_kind.remove(&seq).unwrap_or(PairsKind::All);
            encode_pairs(&codec, ps, kind)
        }
        // ⭐ Phase Set: SMEMBERS/SSCAN/SPOP/SRANDMEMBER 按 members_kind 渲染
        BatchResult::Members(ms) => {
            let kind = conn.members_kind.remove(&seq).unwrap_or(MembersKind::List);
            match kind {
                MembersKind::List => {
                    let mut out = format!("*{}\r\n", ms.len()).into_bytes();
                    for m in ms {
                        out.extend_from_slice(&codec.encode_bulk(m));
                    }
                    out
                }
                MembersKind::Scan => {
                    let mut out = b"*2\r\n".to_vec();
                    out.extend_from_slice(&codec.encode_bulk(b"0"));
                    out.extend_from_slice(&format!("*{}\r\n", ms.len()).into_bytes());
                    for m in ms {
                        out.extend_from_slice(&codec.encode_bulk(m));
                    }
                    out
                }
                MembersKind::One => match ms.first() {
                    Some(m) => codec.encode_bulk(m),
                    None => codec.encode_nil(),
                },
            }
        }
        // ⭐ Phase Z: ZSCORE/ZRANK 可选成员 (Some→bulk, None→nil)
        BatchResult::OptMember(m) => match m {
            Some(b) => codec.encode_bulk(b),
            None => codec.encode_nil(),
        },
        // ⭐ C1: SMISMEMBER → *N 个 :0/:1
        BatchResult::IntList(ns) => {
            let mut out = format!("*{}\r\n", ns.len()).into_bytes();
            for n in ns {
                out.extend_from_slice(&codec.encode_integer(*n));
            }
            out
        }
        // ⭐ Q5: Rows 是 SQL 门面专属 (RESP 命令不产生; 防御性兜底)
        BatchResult::Rows(_) => codec.encode_error("row results unsupported on RESP"),
        BatchResult::Error(e) => codec.encode_error(e),
    };
    conn.resp_complete(seq, bytes);
}

/// SQL 列值到 RESP bulk 的无损文本/字节表示。
/// Bytes/Str/Json 保持原字节；数值和时间沿 SQL 对外文本格式，UUID/DECIMAL
/// 也不会退化为内部二进制布局。
fn resp_sql_text_value(ty: ColType, value: &ColValue) -> Vec<u8> {
    match (ty, value) {
        (_, ColValue::Null) => Vec::new(),
        (ColType::I64, ColValue::I64(v)) => v.to_string().into_bytes(),
        (ColType::F64, ColValue::F64(v)) => v.to_string().into_bytes(),
        (ColType::Bool, ColValue::I64(v)) => (if *v == 0 { "0" } else { "1" }).into(),
        (ColType::Date, ColValue::I64(v)) => render_date(*v).into_bytes(),
        (ColType::Time, ColValue::I64(v)) => render_time(*v).into_bytes(),
        (ColType::Timestamp, ColValue::I64(v)) => render_timestamp(*v).into_bytes(),
        (ColType::Uuid, ColValue::Bytes(v)) => render_uuid(v).into_bytes(),
        (ColType::Decimal { .. }, ColValue::Decimal(v, scale)) => {
            render_decimal(*v, *scale).into_bytes()
        }
        (_, ColValue::Bytes(v)) => v.clone(),
        (_, ColValue::I64(v)) => v.to_string().into_bytes(),
        (_, ColValue::F64(v)) => v.to_string().into_bytes(),
        (_, ColValue::Decimal(v, scale)) => render_decimal(*v, *scale).into_bytes(),
    }
}

/// HQUERY 成功结果：RESP 二维数组；NULL 保留为 RESP nil，字段顺序由 FIELDS 确定。
fn resp_hquery_rows(
    codec: &RespCodec,
    cols: &[(String, ColType)],
    rows: &[Vec<ColValue>],
) -> Vec<u8> {
    let mut out = format!("*{}\r\n", rows.len()).into_bytes();
    for row in rows {
        out.extend_from_slice(format!("*{}\r\n", cols.len()).as_bytes());
        for (idx, (_, ty)) in cols.iter().enumerate() {
            match row.get(idx) {
                Some(ColValue::Null) | None => out.extend_from_slice(&codec.encode_nil()),
                Some(v) => out.extend_from_slice(&codec.encode_bulk(&resp_sql_text_value(*ty, v))),
            }
        }
    }
    out
}

fn resp_sql_project_reply(
    codec: &RespCodec,
    schema: &TableSchema,
    values: &[ColValue],
    fields: &[Vec<u8>],
    mode: RespSqlReadMode,
) -> Vec<u8> {
    let one = |ci: Option<u16>| match ci.and_then(|i| values.get(i as usize)) {
        Some(ColValue::Null) | None => codec.encode_nil(),
        Some(v) => codec.encode_bulk(&resp_sql_text_value(
            schema.columns[ci.unwrap() as usize].ty,
            v,
        )),
    };
    if matches!(mode, RespSqlReadMode::Length) {
        // SQL NULL 在 Hash 视图中等价于字段不存在：这是 HDEL 的可观察语义，
        // 也避免 HLEN/HGETALL 与 HEXISTS 对同一列给出互相矛盾的答案。
        return codec.encode_integer(
            (0..schema.columns.len())
                .filter(|i| !schema.dropped.contains(&(*i as u16)))
                .filter(|i| !matches!(values[*i], ColValue::Null))
                .count() as i64,
        );
    }
    if matches!(mode, RespSqlReadMode::Exists) {
        let exists = fields
            .first()
            .and_then(|f| std::str::from_utf8(f).ok())
            .and_then(|n| schema.col_by_name(n))
            .and_then(|i| values.get(i as usize))
            .is_some_and(|v| !matches!(v, ColValue::Null));
        return codec.encode_integer(exists as i64);
    }
    if matches!(mode, RespSqlReadMode::Strlen) {
        let len = fields
            .first()
            .and_then(|f| std::str::from_utf8(f).ok())
            .and_then(|n| schema.col_by_name(n))
            .and_then(|i| values.get(i as usize))
            .map(|v| {
                resp_sql_text_value(
                    schema.columns[schema
                        .col_by_name(std::str::from_utf8(&fields[0]).unwrap_or(""))
                        .unwrap() as usize]
                        .ty,
                    v,
                )
                .len()
            })
            .unwrap_or(0);
        return codec.encode_integer(len as i64);
    }
    if let RespSqlReadMode::Rand { count, withvalues } = mode {
        let visible: Vec<u16> = (0..schema.columns.len() as u16)
            .filter(|i| !schema.dropped.contains(i))
            .filter(|i| !matches!(values[*i as usize], ColValue::Null))
            .collect();
        let take = count.unwrap_or(1) as usize;
        if count.is_none() {
            return visible.first().map_or_else(
                || codec.encode_nil(),
                |&ci| codec.encode_bulk(schema.columns[ci as usize].name.as_bytes()),
            );
        }
        let mut out = format!(
            "*{}\r\n",
            visible.len().min(take) * if withvalues { 2 } else { 1 }
        )
        .into_bytes();
        for ci in visible.into_iter().take(take) {
            out.extend_from_slice(&codec.encode_bulk(schema.columns[ci as usize].name.as_bytes()));
            if withvalues {
                out.extend_from_slice(&one(Some(ci)));
            }
        }
        return out;
    }
    if !matches!(mode, RespSqlReadMode::Fields) {
        let visible: Vec<u16> = (0..schema.columns.len() as u16)
            .filter(|i| !schema.dropped.contains(i))
            .filter(|i| !matches!(values[*i as usize], ColValue::Null))
            .collect();
        let width = match mode {
            RespSqlReadMode::AllPairs | RespSqlReadMode::Scan => 2,
            _ => 1,
        };
        let mut out = format!("*{}\r\n", visible.len() * width).into_bytes();
        for ci in visible {
            match mode {
                RespSqlReadMode::AllPairs | RespSqlReadMode::Scan => {
                    out.extend_from_slice(
                        &codec.encode_bulk(schema.columns[ci as usize].name.as_bytes()),
                    );
                    out.extend_from_slice(&one(Some(ci)));
                }
                RespSqlReadMode::Keys => out.extend_from_slice(
                    &codec.encode_bulk(schema.columns[ci as usize].name.as_bytes()),
                ),
                RespSqlReadMode::Values => out.extend_from_slice(&one(Some(ci))),
                RespSqlReadMode::Fields
                | RespSqlReadMode::Length
                | RespSqlReadMode::Exists
                | RespSqlReadMode::Strlen
                | RespSqlReadMode::Rand { .. } => unreachable!(),
            }
        }
        if matches!(mode, RespSqlReadMode::Scan) {
            let mut scan = b"*2\r\n$1\r\n0\r\n".to_vec();
            scan.extend_from_slice(&out);
            return scan;
        }
        return out;
    }
    let replies: Vec<Vec<u8>> = fields
        .iter()
        .map(|f| {
            one(std::str::from_utf8(f)
                .ok()
                .and_then(|n| schema.col_by_name(n)))
        })
        .collect();
    if replies.len() == 1 {
        replies.into_iter().next().unwrap()
    } else {
        let mut out = format!("*{}\r\n", replies.len()).into_bytes();
        for r in replies {
            out.extend_from_slice(&r);
        }
        out
    }
}

fn resp_sql_missing_reply(codec: &RespCodec, ctx: &RespSqlHGetCtx) -> Vec<u8> {
    match ctx.mode {
        RespSqlReadMode::Fields if ctx.fields.len() == 1 => codec.encode_nil(),
        RespSqlReadMode::Fields => {
            let mut out = format!("*{}\r\n", ctx.fields.len()).into_bytes();
            for _ in &ctx.fields {
                out.extend_from_slice(&codec.encode_nil());
            }
            out
        }
        RespSqlReadMode::AllPairs | RespSqlReadMode::Keys | RespSqlReadMode::Values => {
            b"*0\r\n".to_vec()
        }
        RespSqlReadMode::Scan => b"*2\r\n$1\r\n0\r\n*0\r\n".to_vec(),
        RespSqlReadMode::Length | RespSqlReadMode::Exists | RespSqlReadMode::Strlen => {
            codec.encode_integer(0)
        }
        RespSqlReadMode::Rand { count: None, .. } => codec.encode_nil(),
        RespSqlReadMode::Rand { count: Some(_), .. } => b"*0\r\n".to_vec(),
    }
}
