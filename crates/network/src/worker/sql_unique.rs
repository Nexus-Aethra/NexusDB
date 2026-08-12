// ⭐ F65: 全局 UNIQUE 约束 INSERT 编排 — 从 sql_dispatch.rs 拆出 (解耦 2026-08).
// 职责: 提取全局唯一列 → 跨 shard 唯一性预检 → 通过才落盘.
use super::*;

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
            SqlDmlAgg {
                remaining: 1,
                affected: 0,
                error: None,
                drop_key: None,
            },
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
    push_unique_op(
        conn_id,
        seq,
        worker_id,
        db,
        &tbl,
        op,
        &first.1,
        num_shards,
        shard_inboxes,
    );
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
                    push_unique_op(
                        conn_id,
                        seq,
                        worker_id,
                        &db,
                        &tbl,
                        op,
                        &enc,
                        num_shards,
                        shard_inboxes,
                    );
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
            BatchResult::ReserveConflict {
                state, holder_pk, ..
            } => {
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
                push_unique_op(
                    conn_id,
                    seq,
                    worker_id,
                    &db,
                    &tbl,
                    op,
                    &enc,
                    num_shards,
                    shard_inboxes,
                );
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
                push_unique_op(
                    conn_id,
                    seq,
                    worker_id,
                    &db,
                    &tbl,
                    op,
                    &enc,
                    num_shards,
                    shard_inboxes,
                );
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
                push_unique_op(
                    conn_id,
                    seq,
                    worker_id,
                    &db,
                    &tbl,
                    op,
                    &enc,
                    num_shards,
                    shard_inboxes,
                );
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
pub(crate) fn row_has_index_val(
    schema: &TableSchema,
    row: &[u8],
    iid: u32,
    enc_val: &[u8],
) -> bool {
    let Ok(values) = storage::row::decode_row(schema, row) else {
        return false;
    };
    schema
        .indexes
        .iter()
        .find(|i| i.iid == iid)
        .is_some_and(|idx| {
            storage::sql_rows::index_vals_bytes(schema, idx, &values).is_some_and(|e| e == enc_val)
        })
}
