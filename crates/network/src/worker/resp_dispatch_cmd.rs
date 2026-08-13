//! RESP 命令分发主函数 (拆自 resp_dispatch.rs).
//!
//! `dispatch_resp_command`: 单条 RESP 命令 → 本地命令直接回 (占 seq 进重排缓冲),
//! KV/SQL 命令构 BatchOp 进 shard inbox.

use super::*;

// RESP dispatch needs separate connection, auth, and shard-routing state.
#[allow(clippy::too_many_arguments)]
pub(crate) fn dispatch_resp_command(
    conn: &mut ConnState,
    conn_id: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    table: &std::sync::Arc<str>,
    limits: &KvLimits,
    auth_password: &Option<String>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    cmd: RespCommand,
) {
    // ⭐ H4: 命令计数 (relaxed 单次原子加, 热路径零锁)
    crate::metrics::KV_OPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let codec = RespCodec::new();
    let seq = conn.next_seq;
    conn.next_seq += 1;

    // AUTH 门禁: 未认证时只放行 AUTH/HELLO/QUIT
    if !conn.authenticated
        && !matches!(
            cmd,
            RespCommand::Auth { .. } | RespCommand::Hello(_) | RespCommand::Quit
        )
    {
        conn.resp_complete(seq, codec.encode_error("NOAUTH Authentication required."));
        return;
    }

    // These commands push their own shard-grouped tasks directly.  Drain any
    // preceding deferred single-key tasks first, otherwise a same-shard group
    // could overtake an earlier SET/GET from this RESP pipeline.
    if matches!(
        &cmd,
        RespCommand::MGet { .. }
            | RespCommand::MSet { .. }
            | RespCommand::MSetNx { .. }
            | RespCommand::SInterCard { .. }
            | RespCommand::SetAlg { .. }
            | RespCommand::SetAlgStore { .. }
            | RespCommand::ZSetStore { .. }
    ) {
        conn.flush_resp_tasks(shard_inboxes);
    }

    match cmd {
        RespCommand::Set { key, value } => {
            // ⭐ value 已是 [TAG_RAW][payload] 布局 (decode 时预置),
            // 校验扣 1B tag; 直构 BatchOp 免 Request 中转/二次拷贝.
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::Put {
                db: db.clone(),
                table: table.clone(),
                key,
                val: value,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::Get { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::Get {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::Del { keys } => {
            // 逐 key 校验 (借用版, 免 clone); 任一超限整条命令拒绝 (不部分执行)
            for key in &keys {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // 多 key 拆多个 Delete task 共用同一 seq, 聚合计数后回 :N
            conn.del_agg.insert(
                seq,
                DelAgg {
                    remaining: keys.len(),
                    count: 0,
                },
            );
            for key in keys {
                if try_dispatch_resp_sql_delete(
                    conn,
                    conn_id,
                    seq,
                    worker_id,
                    db.clone(),
                    table.clone(),
                    key.clone(),
                    shard_inboxes,
                    num_shards,
                ) {
                    continue;
                }
                let op = BatchOp::Delete {
                    db: db.clone(),
                    table: table.clone(),
                    key,
                };
                push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
            }
        }
        RespCommand::MGet { keys } => {
            for key in &keys {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // ⭐ 按 (shard, 表) 分组: 每组一个 MultiGet (shard 内区间复用),
            // group 号回传后按索引表回填原始槽. ⭐ T2: 每 key 独立冒号选表.
            let n = keys.len();
            type MGroup = ((usize, std::sync::Arc<str>), Vec<Vec<u8>>, Vec<usize>);
            let mut by_shard: Vec<MGroup> = Vec::new();
            for (i, mut key) in keys.into_iter().enumerate() {
                let tbl = conn
                    .resolve_table(&mut key)
                    .unwrap_or_else(|| table.clone());
                let sid = hash_route_key(db.as_ref(), tbl.as_ref(), &key, num_shards);
                match by_shard
                    .iter_mut()
                    .find(|(g, _, _)| g.0 == sid && g.1 == tbl)
                {
                    Some((_, ks, idxs)) => {
                        ks.push(key);
                        idxs.push(i);
                    }
                    None => by_shard.push(((sid, tbl), vec![key], vec![i])),
                }
            }
            let groups: Vec<Vec<usize>> =
                by_shard.iter().map(|(_, _, idxs)| idxs.clone()).collect();
            conn.mget_agg.insert(
                seq,
                MGetAgg {
                    remaining: by_shard.len(),
                    slots: vec![None; n],
                    groups,
                    error: None,
                },
            );
            for (gidx, ((sid, tbl), ks, _)) in by_shard.into_iter().enumerate() {
                let op = BatchOp::MultiGet {
                    db: db.clone(),
                    table: tbl,
                    keys: ks,
                };
                push_task_grouped(conn_id, seq, worker_id, gidx as u32, sid, op, shard_inboxes);
            }
        }
        RespCommand::MSet { pairs } => {
            // value 已带 1B tag, 校验扣除
            for (key, value) in &pairs {
                if let Err(msg) = validate_kv(key, value.len().saturating_sub(1), limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // ⭐ T2: 每 key 独立冒号选表 → 按 (shard, 表) 分组
            type ShardPairs = ((usize, std::sync::Arc<str>), Vec<(Vec<u8>, Vec<u8>)>);
            let mut by_shard: Vec<ShardPairs> = Vec::new();
            for (mut key, value) in pairs {
                let tbl = conn
                    .resolve_table(&mut key)
                    .unwrap_or_else(|| table.clone());
                let sid = hash_route_key(db.as_ref(), tbl.as_ref(), &key, num_shards);
                match by_shard.iter_mut().find(|(g, _)| g.0 == sid && g.1 == tbl) {
                    Some((_, ps)) => ps.push((key, value)),
                    None => by_shard.push(((sid, tbl), vec![(key, value)])),
                }
            }
            conn.mset_agg.insert(
                seq,
                MSetAgg {
                    remaining: by_shard.len(),
                    error: None,
                },
            );
            for (gidx, ((sid, tbl), ps)) in by_shard.into_iter().enumerate() {
                let op = BatchOp::MultiPut {
                    db: db.clone(),
                    table: tbl,
                    pairs: ps,
                };
                push_task_grouped(conn_id, seq, worker_id, gidx as u32, sid, op, shard_inboxes);
            }
        }
        RespCommand::Ping(msg) => {
            let bytes = match msg {
                None => codec.encode_simple("PONG"),
                Some(m) => codec.encode_bulk(&m),
            };
            conn.resp_complete(seq, bytes);
        }
        RespCommand::Incr { key, delta } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::Incr {
                db: db.clone(),
                table: table.clone(),
                key,
                delta,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::IncrFloat { key, delta } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::IncrFloat {
                db: db.clone(),
                table: table.clone(),
                key,
                delta,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::Append { key, suffix } => {
            // suffix 不带 tag (RMW 端拼接); 校验按追加段长度上限保守拦截
            if let Err(msg) = validate_kv(&key, suffix.len(), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::Append {
                db: db.clone(),
                table: table.clone(),
                key,
                suffix,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SetNx { key, value } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SetNx {
                db: db.clone(),
                table: table.clone(),
                key,
                val: value,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::Exists { keys } => {
            for key in &keys {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // N 个 Get 共用 seq, 聚合计数 (Redis EXISTS: 重复 key 重复计)
            conn.exists_agg.insert(
                seq,
                ExistsAgg {
                    remaining: keys.len(),
                    count: 0,
                },
            );
            for key in keys {
                let op = BatchOp::Get {
                    db: db.clone(),
                    table: table.clone(),
                    key,
                };
                push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
            }
        }
        RespCommand::Strlen { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.get_kind.insert(seq, GetKind::Strlen);
            let op = BatchOp::Get {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::TypeOf { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.get_kind.insert(seq, GetKind::TypeOf);
            let op = BatchOp::Get {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GetDel { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::GetDel {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GetSet { key, value } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::GetSet {
                db: db.clone(),
                table: table.clone(),
                key,
                val: value,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SetRange { key, offset, data } => {
            // 新长度 = offset + data.len(), 保守校验不超 value 上限
            if let Err(msg) = validate_kv(&key, offset as usize + data.len(), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SetRange {
                db: db.clone(),
                table: table.clone(),
                key,
                offset,
                data,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GetRange { key, start, end } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // 复用 Get; 结果到达时按 (start,end) 切片 (getrange_ctx)
            conn.getrange_ctx.insert(seq, (start, end));
            let op = BatchOp::Get {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::MSetNx { pairs } => {
            for (key, value) in &pairs {
                if let Err(msg) = validate_kv(key, value.len().saturating_sub(1), limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // 按 (shard, 表) 分组, 每组一个 MultiPutNx; 全部写入 → :1, 否则 :0
            // ⭐ T2: 每 key 独立冒号选表
            type NxPairs = ((usize, std::sync::Arc<str>), Vec<(Vec<u8>, Vec<u8>)>);
            let mut by_shard: Vec<NxPairs> = Vec::new();
            for (mut key, value) in pairs {
                let tbl = conn
                    .resolve_table(&mut key)
                    .unwrap_or_else(|| table.clone());
                let sid = hash_route_key(db.as_ref(), tbl.as_ref(), &key, num_shards);
                match by_shard.iter_mut().find(|(g, _)| g.0 == sid && g.1 == tbl) {
                    Some((_, ps)) => ps.push((key, value)),
                    None => by_shard.push(((sid, tbl), vec![(key, value)])),
                }
            }
            conn.msetnx_agg.insert(
                seq,
                MSetNxAgg {
                    remaining: by_shard.len(),
                    all_set: true,
                },
            );
            for (gidx, ((sid, tbl), ps)) in by_shard.into_iter().enumerate() {
                let op = BatchOp::MultiPutNx {
                    db: db.clone(),
                    table: tbl,
                    pairs: ps,
                };
                push_task_grouped(conn_id, seq, worker_id, gidx as u32, sid, op, shard_inboxes);
            }
        }
        // ---- ⭐ Phase H: Hash (单 key 单 shard, 直推 push_task) ----
        RespCommand::HSet {
            key,
            pairs,
            reply_ok,
        } => {
            for (f, v) in &pairs {
                if let Err(msg) = validate_kv(&key, 0, limits)
                    .and_then(|_| validate_kv(f, v.len().saturating_sub(1), limits))
                {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            if try_dispatch_resp_sql_hset(
                conn,
                conn_id,
                seq,
                worker_id,
                db.clone(),
                table.clone(),
                key.clone(),
                pairs.clone(),
                reply_ok,
                shard_inboxes,
                num_shards,
            ) {
                return;
            }
            if reply_ok {
                conn.hmset_ok.insert(seq);
            } // HMSET 回 +OK (Integer 转换)
            let op = BatchOp::HSet {
                db: db.clone(),
                table: table.clone(),
                key,
                pairs,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HSetNx { key, field, value } => {
            if let Err(msg) = validate_kv(&key, 0, limits)
                .and_then(|_| validate_kv(&field, value.len().saturating_sub(1), limits))
            {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            if try_dispatch_resp_sql_hsetnx(
                conn,
                conn_id,
                seq,
                worker_id,
                db.clone(),
                table.clone(),
                key.clone(),
                field.clone(),
                value.clone(),
                shard_inboxes,
                num_shards,
            ) {
                return;
            }
            let op = BatchOp::HSetNx {
                db: db.clone(),
                table: table.clone(),
                key,
                field,
                val: value,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HQuery {
            table: query_table,
            terms,
            fields,
            limit,
        } => {
            let Ok(table_name) = std::str::from_utf8(&query_table) else {
                conn.resp_complete(seq, codec.encode_error("HQUERY table must be UTF-8"));
                return;
            };
            if fields.is_empty() || terms.len() > 8 || fields.len() > 32 {
                conn.resp_complete(seq, codec.encode_error("HQUERY exceeds v1 limits"));
                return;
            }
            let mut conds = Vec::with_capacity(terms.len());
            for (col, op, val) in terms {
                let (Ok(col), Ok(op)) = (std::str::from_utf8(&col), std::str::from_utf8(&op))
                else {
                    conn.resp_complete(
                        seq,
                        codec.encode_error("HQUERY identifiers/operators must be UTF-8"),
                    );
                    return;
                };
                let op = match op {
                    "=" => CmpOp::Eq,
                    ">" => CmpOp::Gt,
                    ">=" => CmpOp::Ge,
                    "<" => CmpOp::Lt,
                    "<=" => CmpOp::Le,
                    _ => unreachable!("RESP parser checked HQUERY operator"),
                };
                conds.push(Pred::Leaf(Cond {
                    col: col.to_string(),
                    op,
                    val: SqlValue::Str(val),
                    set: Vec::new(),
                }));
            }
            let mut items = Vec::with_capacity(fields.len());
            for field in fields {
                let Ok(field) = std::str::from_utf8(&field) else {
                    conn.resp_complete(seq, codec.encode_error("HQUERY field must be UTF-8"));
                    return;
                };
                items.push(sql::SelectItem::Col {
                    name: field.to_string(),
                    alias: None,
                });
            }
            conn.resp_hquery.insert(seq, RespHQueryCtx);
            sql_dispatch_stmt(
                conn,
                conn_id,
                seq,
                worker_id,
                db,
                db,
                db_view,
                shard_inboxes,
                num_shards,
                SqlStmt::Select {
                    table: table_name.to_string(),
                    items,
                    conds: Pred::And(conds),
                    limit: Some(limit),
                    order: Vec::new(),
                    offset: None,
                    group_by: Vec::new(),
                    having: Pred::And(Vec::new()),
                    limit_param: None,
                    offset_param: None,
                },
            );
        }
        RespCommand::HGet { key, field } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            if try_dispatch_resp_sql_hget(
                conn,
                conn_id,
                seq,
                worker_id,
                db.clone(),
                table.clone(),
                key.clone(),
                field.clone(),
                shard_inboxes,
                num_shards,
            ) {
                return;
            }
            let op = BatchOp::HGet {
                db: db.clone(),
                table: table.clone(),
                key,
                field,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HMGet { key, fields } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let fallback = BatchOp::HMGet {
                db: db.clone(),
                table: table.clone(),
                key: key.clone(),
                fields: fields.clone(),
            };
            if try_dispatch_resp_sql_read(
                conn,
                conn_id,
                seq,
                worker_id,
                db.clone(),
                key,
                fields,
                RespSqlReadMode::Fields,
                fallback.clone(),
                shard_inboxes,
                num_shards,
            ) {
                return;
            }
            let op = fallback;
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HDel { key, fields } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            if try_dispatch_resp_sql_hdel(
                conn,
                conn_id,
                seq,
                worker_id,
                db.clone(),
                table.clone(),
                key.clone(),
                fields.clone(),
                shard_inboxes,
                num_shards,
            ) {
                return;
            }
            let op = BatchOp::HDel {
                db: db.clone(),
                table: table.clone(),
                key,
                fields,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HExists { key, field } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let fallback = BatchOp::HGet {
                db: db.clone(),
                table: table.clone(),
                key: key.clone(),
                field: field.clone(),
            };
            if try_dispatch_resp_sql_read(
                conn,
                conn_id,
                seq,
                worker_id,
                db.clone(),
                key,
                vec![field],
                RespSqlReadMode::Exists,
                fallback.clone(),
                shard_inboxes,
                num_shards,
            ) {
                return;
            }
            conn.get_kind.insert(seq, GetKind::HExists);
            let op = fallback;
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HLen { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let fallback = BatchOp::HLen {
                db: db.clone(),
                table: table.clone(),
                key: key.clone(),
            };
            if try_dispatch_resp_sql_read(
                conn,
                conn_id,
                seq,
                worker_id,
                db.clone(),
                key,
                Vec::new(),
                RespSqlReadMode::Length,
                fallback.clone(),
                shard_inboxes,
                num_shards,
            ) {
                return;
            }
            let op = fallback;
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HGetAll { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let fallback = BatchOp::HGetAll {
                db: db.clone(),
                table: table.clone(),
                key: key.clone(),
            };
            if try_dispatch_resp_sql_read(
                conn,
                conn_id,
                seq,
                worker_id,
                db.clone(),
                key,
                Vec::new(),
                RespSqlReadMode::AllPairs,
                fallback.clone(),
                shard_inboxes,
                num_shards,
            ) {
                return;
            }
            conn.pairs_kind.insert(seq, PairsKind::All);
            let op = fallback;
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HKeys { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let fallback = BatchOp::HGetAll {
                db: db.clone(),
                table: table.clone(),
                key: key.clone(),
            };
            if try_dispatch_resp_sql_read(
                conn,
                conn_id,
                seq,
                worker_id,
                db.clone(),
                key,
                Vec::new(),
                RespSqlReadMode::Keys,
                fallback.clone(),
                shard_inboxes,
                num_shards,
            ) {
                return;
            }
            conn.pairs_kind.insert(seq, PairsKind::Keys);
            let op = fallback;
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HVals { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let fallback = BatchOp::HGetAll {
                db: db.clone(),
                table: table.clone(),
                key: key.clone(),
            };
            if try_dispatch_resp_sql_read(
                conn,
                conn_id,
                seq,
                worker_id,
                db.clone(),
                key,
                Vec::new(),
                RespSqlReadMode::Values,
                fallback.clone(),
                shard_inboxes,
                num_shards,
            ) {
                return;
            }
            conn.pairs_kind.insert(seq, PairsKind::Vals);
            let op = fallback;
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HScan { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let fallback = BatchOp::HGetAll {
                db: db.clone(),
                table: table.clone(),
                key: key.clone(),
            };
            if try_dispatch_resp_sql_read(
                conn,
                conn_id,
                seq,
                worker_id,
                db.clone(),
                key,
                Vec::new(),
                RespSqlReadMode::Scan,
                fallback.clone(),
                shard_inboxes,
                num_shards,
            ) {
                return;
            }
            conn.pairs_kind.insert(seq, PairsKind::Scan);
            let op = fallback;
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HIncrBy { key, field, delta } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            if try_dispatch_resp_sql_incr(
                conn,
                conn_id,
                seq,
                worker_id,
                db.clone(),
                table.clone(),
                key.clone(),
                field.clone(),
                RespSqlIncrDelta::Int(delta),
                shard_inboxes,
                num_shards,
            ) {
                return;
            }
            let op = BatchOp::HIncrBy {
                db: db.clone(),
                table: table.clone(),
                key,
                field,
                delta,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HIncrByFloat { key, field, delta } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            if try_dispatch_resp_sql_incr(
                conn,
                conn_id,
                seq,
                worker_id,
                db.clone(),
                table.clone(),
                key.clone(),
                field.clone(),
                RespSqlIncrDelta::Float(delta),
                shard_inboxes,
                num_shards,
            ) {
                return;
            }
            let op = BatchOp::HIncrByFloat {
                db: db.clone(),
                table: table.clone(),
                key,
                field,
                delta,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ Phase Set: Set (单 key 直推; 代数类跨 shard 聚合) ----
        RespCommand::SAdd { key, members } => {
            for m in &members {
                if let Err(msg) =
                    validate_kv(&key, 0, limits).and_then(|_| validate_kv(m, 0, limits))
                {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            let op = BatchOp::SAdd {
                db: db.clone(),
                table: table.clone(),
                key,
                members,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SRem { key, members } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SRem {
                db: db.clone(),
                table: table.clone(),
                key,
                members,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SIsMember { key, member } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SIsMember {
                db: db.clone(),
                table: table.clone(),
                key,
                member,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SCard { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SCard {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SMembers { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::List);
            let op = BatchOp::SMembers {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SScan { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::Scan);
            let op = BatchOp::SMembers {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SPop { key, count } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // count 缺省 → 单 bulk (One); 显式 count → 数组 (List)
            match count {
                None => {
                    conn.members_kind.insert(seq, MembersKind::One);
                    let op = BatchOp::SPop {
                        db: db.clone(),
                        table: table.clone(),
                        key,
                    };
                    push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
                Some(c) => {
                    conn.members_kind.insert(seq, MembersKind::List);
                    let op = BatchOp::SPopN {
                        db: db.clone(),
                        table: table.clone(),
                        key,
                        count: c,
                    };
                    push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
            }
        }
        RespCommand::SRandMember { key, count } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            match count {
                None => {
                    conn.members_kind.insert(seq, MembersKind::One);
                    let op = BatchOp::SRandMember {
                        db: db.clone(),
                        table: table.clone(),
                        key,
                    };
                    push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
                Some(c) => {
                    conn.members_kind.insert(seq, MembersKind::List);
                    let op = BatchOp::SRandCount {
                        db: db.clone(),
                        table: table.clone(),
                        key,
                        count: c,
                    };
                    push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
            }
        }
        RespCommand::SMisMember { key, members } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SMisMember {
                db: db.clone(),
                table: table.clone(),
                key,
                members,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SInterCard { keys, limit } => {
            for key in &keys {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // 复用 SetAlg 聚合 (Inter), 完成点回 :card 而非数组
            let n = keys.len();
            conn.setalg_agg.insert(
                seq,
                SetAlgAgg {
                    remaining: n,
                    op: SetAlgOp::Inter,
                    sets: vec![None; n],
                    error: None,
                    card_only: true,
                    limit,
                    store_dst: None,
                    db: db.clone(),
                    table: table.clone(),
                },
            );
            for (i, mut key) in keys.into_iter().enumerate() {
                // ⭐ T2: 源 key 逐个冒号选表 (天然支持跨表代数)
                let tbl = conn
                    .resolve_table(&mut key)
                    .unwrap_or_else(|| table.clone());
                let sid = hash_route_key(db.as_ref(), tbl.as_ref(), &key, num_shards);
                let smem = BatchOp::SMembers {
                    db: db.clone(),
                    table: tbl,
                    key,
                };
                push_task_grouped(conn_id, seq, worker_id, i as u32, sid, smem, shard_inboxes);
            }
        }
        RespCommand::SetAlg { op, keys } => {
            for key in &keys {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // 每 key 一个 SMembers (group = key 序号), 全部回齐后求交/并/差
            let n = keys.len();
            conn.setalg_agg.insert(
                seq,
                SetAlgAgg {
                    remaining: n,
                    op,
                    sets: vec![None; n],
                    error: None,
                    card_only: false,
                    limit: 0,
                    store_dst: None,
                    db: db.clone(),
                    table: table.clone(),
                },
            );
            for (i, mut key) in keys.into_iter().enumerate() {
                // ⭐ T2: 源 key 逐个冒号选表 (天然支持跨表代数)
                let tbl = conn
                    .resolve_table(&mut key)
                    .unwrap_or_else(|| table.clone());
                let sid = hash_route_key(db.as_ref(), tbl.as_ref(), &key, num_shards);
                let smem = BatchOp::SMembers {
                    db: db.clone(),
                    table: tbl,
                    key,
                };
                push_task_grouped(conn_id, seq, worker_id, i as u32, sid, smem, shard_inboxes);
            }
        }
        // ---- ⭐ C3: *STORE (源读聚合 + dst 写; 跨 shard 非原子, 记 gap) ----
        RespCommand::SetAlgStore { op, dst, keys } => {
            for key in keys.iter().chain(std::iter::once(&dst)) {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            let n = keys.len();
            // ⭐ T2: dst 冒号选表 (二阶段任务写入 dst 的表)
            let mut dst = dst;
            let dst_tbl = conn
                .resolve_table(&mut dst)
                .unwrap_or_else(|| table.clone());
            conn.setalg_agg.insert(
                seq,
                SetAlgAgg {
                    remaining: n,
                    op,
                    sets: vec![None; n],
                    error: None,
                    card_only: false,
                    limit: 0,
                    store_dst: Some(dst),
                    db: db.clone(),
                    table: dst_tbl,
                },
            );
            for (i, mut key) in keys.into_iter().enumerate() {
                // ⭐ T2: 源 key 逐个冒号选表 (天然支持跨表代数)
                let tbl = conn
                    .resolve_table(&mut key)
                    .unwrap_or_else(|| table.clone());
                let sid = hash_route_key(db.as_ref(), tbl.as_ref(), &key, num_shards);
                let smem = BatchOp::SMembers {
                    db: db.clone(),
                    table: tbl,
                    key,
                };
                push_task_grouped(conn_id, seq, worker_id, i as u32, sid, smem, shard_inboxes);
            }
        }
        RespCommand::ZSetStore { inter, dst, keys } => {
            for key in keys.iter().chain(std::iter::once(&dst)) {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            let n = keys.len();
            // ⭐ T2: dst 冒号选表 (二阶段任务写入 dst 的表)
            let mut dst = dst;
            let dst_tbl = conn
                .resolve_table(&mut dst)
                .unwrap_or_else(|| table.clone());
            conn.zstore_agg.insert(
                seq,
                ZStoreAgg {
                    remaining: n,
                    inter,
                    sets: vec![None; n],
                    error: None,
                    dst,
                    db: db.clone(),
                    table: dst_tbl,
                },
            );
            // 每源 key 取全量 (member, score) — 复用 ZRange withscores 交替串
            for (i, mut key) in keys.into_iter().enumerate() {
                // ⭐ T2: 源 key 逐个冒号选表
                let tbl = conn
                    .resolve_table(&mut key)
                    .unwrap_or_else(|| table.clone());
                let sid = hash_route_key(db.as_ref(), tbl.as_ref(), &key, num_shards);
                let zr = BatchOp::ZRange {
                    db: db.clone(),
                    table: tbl,
                    key,
                    start: 0,
                    end: -1,
                    rev: false,
                    withscores: true,
                };
                push_task_grouped(conn_id, seq, worker_id, i as u32, sid, zr, shard_inboxes);
            }
        }
        // ---- ⭐ Phase L: List (单 key 直推) ----
        RespCommand::LPush { key, values, left } => {
            for v in &values {
                if let Err(msg) = validate_kv(&key, v.len().saturating_sub(1), limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            let op = BatchOp::LPush {
                db: db.clone(),
                table: table.clone(),
                key,
                values,
                left,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LPop { key, left, count } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // count 缺省 → 单 bulk (One); 显式 count → 数组 (List)
            conn.members_kind.insert(
                seq,
                if count.is_none() {
                    MembersKind::One
                } else {
                    MembersKind::List
                },
            );
            let op = BatchOp::LPop {
                db: db.clone(),
                table: table.clone(),
                key,
                left,
                count: count.unwrap_or(1),
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LLen { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::LLen {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LRange { key, start, end } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::List);
            let op = BatchOp::LRange {
                db: db.clone(),
                table: table.clone(),
                key,
                start,
                end,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LIndex { key, idx } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::LIndex {
                db: db.clone(),
                table: table.clone(),
                key,
                idx,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LSet { key, idx, value } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.hmset_ok.insert(seq); // Integer(1) → +OK
            let op = BatchOp::LSet {
                db: db.clone(),
                table: table.clone(),
                key,
                idx,
                val: value,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ C2: List 中段操作 ----
        RespCommand::LRem { key, count, value } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::LRem {
                db: db.clone(),
                table: table.clone(),
                key,
                count,
                val: value,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LTrim { key, start, stop } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.hmset_ok.insert(seq); // Integer(1) → +OK
            let op = BatchOp::LTrim {
                db: db.clone(),
                table: table.clone(),
                key,
                start,
                stop,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LPos {
            key,
            value,
            rank,
            count,
        } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::LPos {
                db: db.clone(),
                table: table.clone(),
                key,
                val: value,
                rank,
                count,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LInsert {
            key,
            before,
            pivot,
            value,
        } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::LInsert {
                db: db.clone(),
                table: table.clone(),
                key,
                before,
                pivot,
                val: value,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ Phase Z: ZSet (单 key 直推) ----
        RespCommand::ZAdd { key, pairs } => {
            for (_, m) in &pairs {
                if let Err(msg) =
                    validate_kv(&key, 0, limits).and_then(|_| validate_kv(m, 0, limits))
                {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            let op = BatchOp::ZAdd {
                db: db.clone(),
                table: table.clone(),
                key,
                pairs,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZRem { key, members } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZRem {
                db: db.clone(),
                table: table.clone(),
                key,
                members,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZScore { key, member } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZScore {
                db: db.clone(),
                table: table.clone(),
                key,
                member,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZCard { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZCard {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZIncrBy { key, delta, member } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZIncrBy {
                db: db.clone(),
                table: table.clone(),
                key,
                delta,
                member,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZRange {
            key,
            start,
            end,
            rev,
            withscores,
        } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::List);
            let op = BatchOp::ZRange {
                db: db.clone(),
                table: table.clone(),
                key,
                start,
                end,
                rev,
                withscores,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZRangeByScore {
            key,
            min,
            max,
            withscores,
        } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::List);
            let op = BatchOp::ZRangeByScore {
                db: db.clone(),
                table: table.clone(),
                key,
                min,
                max,
                withscores,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZRank { key, member, rev } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZRank {
                db: db.clone(),
                table: table.clone(),
                key,
                member,
                rev,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ C1: ZSet/Hash 命令空洞 ----
        RespCommand::ZCount { key, min, max } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZCount {
                db: db.clone(),
                table: table.clone(),
                key,
                min,
                max,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZMScore { key, members } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // Values 已是成形 score 串, 按裸 bulk 渲染 (不走 render tag)
            conn.values_raw.insert(seq);
            let op = BatchOp::ZMScore {
                db: db.clone(),
                table: table.clone(),
                key,
                members,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZPop { key, rev, count } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::List);
            let op = BatchOp::ZPop {
                db: db.clone(),
                table: table.clone(),
                key,
                rev,
                count,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HStrlen { key, field } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // 复用 HGet + Strlen 语义转换 (miss → :0)
            let fallback = BatchOp::HGet {
                db: db.clone(),
                table: table.clone(),
                key: key.clone(),
                field: field.clone(),
            };
            if try_dispatch_resp_sql_read(
                conn,
                conn_id,
                seq,
                worker_id,
                db.clone(),
                key,
                vec![field],
                RespSqlReadMode::Strlen,
                fallback.clone(),
                shard_inboxes,
                num_shards,
            ) {
                return;
            }
            conn.get_kind.insert(seq, GetKind::Strlen);
            let op = fallback;
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HRandField {
            key,
            count,
            withvalues,
        } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let fallback = BatchOp::HRandField {
                db: db.clone(),
                table: table.clone(),
                key: key.clone(),
                count: count.unwrap_or(1),
                withvalues,
            };
            if try_dispatch_resp_sql_read(
                conn,
                conn_id,
                seq,
                worker_id,
                db.clone(),
                key,
                Vec::new(),
                RespSqlReadMode::Rand { count, withvalues },
                fallback.clone(),
                shard_inboxes,
                num_shards,
            ) {
                return;
            }
            let kind = match (count, withvalues) {
                (None, _) => PairsKind::OneKey,
                (Some(_), true) => PairsKind::All,
                (Some(_), false) => PairsKind::Keys,
            };
            conn.pairs_kind.insert(seq, kind);
            let op = fallback;
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ Phase G: Geo (复用 ZSet 链路 + 渲染钩子) ----
        RespCommand::GeoPos { key, members } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.geo_ctx.insert(seq, GeoCtx::Pos);
            let op = BatchOp::ZMScore {
                db: db.clone(),
                table: table.clone(),
                key,
                members,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GeoDist {
            key,
            m1,
            m2,
            factor,
        } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.geo_ctx.insert(seq, GeoCtx::Dist { factor });
            let op = BatchOp::ZMScore {
                db: db.clone(),
                table: table.clone(),
                key,
                members: vec![m1, m2],
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GeoSearch {
            key,
            lon,
            lat,
            radius_m,
            asc,
            count,
            withcoord,
            withdist,
        } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.geo_ctx.insert(
                seq,
                GeoCtx::Search {
                    lon,
                    lat,
                    radius_m,
                    asc,
                    count,
                    withcoord,
                    withdist,
                },
            );
            // 全量 (member, score) — worker 端 geohash 解码 + 距离过滤
            let op = BatchOp::ZRange {
                db: db.clone(),
                table: table.clone(),
                key,
                start: 0,
                end: -1,
                rev: false,
                withscores: true,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ Phase B: Bitmap (String 字节) ----
        RespCommand::SetBit { key, offset, bit } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // 位偏移上限: 落地字节 ≤ max_value_bytes (溢出页上限内)
            if (offset / 8) as usize + 1 > limits.max_value_bytes {
                conn.resp_complete(
                    seq,
                    codec.encode_error("bit offset is not an integer or out of range"),
                );
                return;
            }
            let op = BatchOp::SetBit {
                db: db.clone(),
                table: table.clone(),
                key,
                offset,
                bit,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GetBit { key, offset } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.bit_ctx.insert(seq, BitCtx::GetBit { offset });
            let op = BatchOp::Get {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::BitCount { key, start, end } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.bit_ctx.insert(seq, BitCtx::Count { start, end });
            let op = BatchOp::Get {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::BitPos {
            key,
            bit,
            start,
            end,
        } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.bit_ctx.insert(seq, BitCtx::Pos { bit, start, end });
            let op = BatchOp::Get {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::InvalidInt(_) => {
            conn.resp_complete(
                seq,
                codec.encode_error("value is not an integer or out of range"),
            );
        }
        RespCommand::InvalidFloat(_) => {
            conn.resp_complete(seq, codec.encode_error("value is not a valid float"));
        }
        RespCommand::Echo(m) => {
            conn.resp_complete(seq, codec.encode_bulk(&m));
        }
        RespCommand::Auth { user, pass } => {
            let bytes = match auth_password {
                None => codec.encode_error("ERR Client sent AUTH, but no password is set."),
                Some(expected) => {
                    let user_ok = match &user {
                        None => true,
                        Some(u) => u.as_slice() == b"default",
                    };
                    if user_ok && pass.as_slice() == expected.as_bytes() {
                        conn.authenticated = true;
                        codec.encode_ok()
                    } else {
                        codec.encode_error(
                            "WRONGPASS invalid username-password pair or user is disabled.",
                        )
                    }
                }
            };
            conn.resp_complete(seq, bytes);
        }
        RespCommand::Quit => {
            conn.resp_complete(seq, codec.encode_ok());
            conn.close_after_flush = true;
        }
        RespCommand::Command => {
            conn.resp_complete(seq, codec.encode_empty_array());
        }
        RespCommand::Hello(proto) => {
            let is_v2 = match &proto {
                None => true,
                Some(p) => p.as_slice() == b"2",
            };
            let bytes = if is_v2 {
                // 最小 HELLO 回复: 扁平 key-value 数组 (RESP2 无 map 类型)
                let mut out = Vec::new();
                out.extend_from_slice(b"*6\r\n");
                out.extend_from_slice(&codec.encode_bulk(b"server"));
                out.extend_from_slice(&codec.encode_bulk(b"nexusdb"));
                out.extend_from_slice(&codec.encode_bulk(b"version"));
                out.extend_from_slice(&codec.encode_bulk(b"0.1.0"));
                out.extend_from_slice(&codec.encode_bulk(b"proto"));
                out.extend_from_slice(&codec.encode_integer(2));
                out
            } else {
                codec.encode_error("NOPROTO unsupported protocol version")
            };
            conn.resp_complete(seq, bytes);
        }
        RespCommand::Select { idx } => {
            // ⭐ D3 (分库): idx 经 DbDirView 翻译为 db name, per-connection 生效.
            // (分表维度走 key 冒号前缀, 与 SELECT 正交)
            let bytes = match u32::try_from(idx).ok().and_then(|id| db_view.name_of(id)) {
                Some(name) => {
                    conn.current_db = name;
                    codec.encode_ok()
                }
                None => codec.encode_error("DB index is out of range"),
            };
            conn.resp_complete(seq, bytes);
        }
        RespCommand::Unknown(name) => {
            conn.resp_complete(
                seq,
                codec.encode_error(&format!("unknown command '{name}'")),
            );
        }
        RespCommand::WrongArity(name) => {
            conn.resp_complete(
                seq,
                codec.encode_error(&format!("wrong number of arguments for '{name}' command")),
            );
        }
    }
}
