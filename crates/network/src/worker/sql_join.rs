// ⭐ F67/F68 (JOIN): 两表/多表 hash join 编排 — 从 sql_dispatch.rs 拆出 (解耦 2026-08).
// 职责: JOIN 的 schema 拉取 → 统计估算 → gather → hash join 计算 → 输出.
// 依赖: 通过 `use super::*` 访问 ConnState / SqlJoinCtx / BatchOp 等 worker 级定义.
use super::*;

// ==================== JOIN 编排核心 ====================
/// ⭐ F67 (JOIN): handle_resp 认领 — 按 phase 推进. 返回 true = 已处理此 seq.
/// `group` 仅 EstimateRows 行数批使用 (0=tables[0], 1=tables[1]).
pub(crate) fn sql_join_drive(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    group: u32,
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
        // ⭐ M3-2/M3-4/M3-5 + 方案 A (调优): 统计收集 — 批次 0=两表行数 (合并一轮,
        // group 0=t0, 1=t1), 1=两表 distinct (合并一轮), 2=两表 ranges (合并一轮).
        // 候选索引空表不入批; 行数批收齐后两表均 ≤ EST_SKIP_STATS_ROWS → 直接决策.
        JoinPhase::EstimateRows => {
            {
                let c = conn.sql_join.get_mut(&seq).unwrap();
                // 收当前批次结果 (group 区分表 0/1)
                let ti = group as usize;
                match c.est_phase {
                    0 => {
                        if let BatchResult::RowCount(n) = result {
                            if ti < 2 {
                                c.est_rows[ti] += n;
                            }
                        }
                    }
                    1 => {
                        if let BatchResult::DistinctCounts(ds) = result {
                            if ti < 2 {
                                let cand = join_candidate_eq_iids(&c, ti);
                                if let Some(map) = c.join_distinct.get_mut(ti) {
                                    for ((_, iid), d) in cand.iter().zip(ds.iter()) {
                                        map.insert(*iid, *d);
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        if let BatchResult::RangeBounds(rbs) = result {
                            if ti < 2 {
                                let cand = join_candidate_eq_iids(&c, ti);
                                if let Some(map) = c.join_ranges.get_mut(ti) {
                                    for ((_, iid), (lo, hi)) in cand.iter().zip(rbs.iter()) {
                                        if let (Some(lo), Some(hi)) = (lo, hi) {
                                            map.insert(*iid, (lo.clone(), hi.clone()));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                c.remaining = c.remaining.saturating_sub(1);
                if c.remaining > 0 {
                    return true;
                }
                // 本批收齐 → 推进 (跳过候选空批)
                let mut phase = c.est_phase + 1;
                if c.est_phase == 0
                    && c.est_rows[0] <= EST_SKIP_STATS_ROWS
                    && c.est_rows[1] <= EST_SKIP_STATS_ROWS
                {
                    // ⭐ 方案 A: 小表 JOIN — distinct/ranges 的索引选择收益可忽略,
                    // 跳过统计直接按行数决策 (双表小 JOIN 固定只 1 轮广播).
                    crate::metrics::SQL_JOIN_EST_SKIPPED
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    phase = 3;
                }
                while phase < 3 {
                    // 仅对有候选索引列的表广播 (候选空表跳过)
                    let mut pushes: Vec<(u32, Vec<u32>)> = Vec::new();
                    for t in 0..2 {
                        let cand = join_candidate_eq_iids(&c, t);
                        if !cand.is_empty() {
                            pushes.push((t as u32, cand.iter().map(|&(_, iid)| iid).collect()));
                        }
                    }
                    if pushes.is_empty() {
                        phase += 1;
                        continue;
                    }
                    c.est_phase = phase as u8;
                    c.remaining = pushes.len() * num_shards;
                    let db = c.db.clone();
                    for (t, iids) in pushes {
                        let table = c.tables[t as usize].table.clone();
                        for sid in 0..num_shards {
                            let op = match phase {
                                1 => BatchOp::EstimateDistinct {
                                    db: db.clone(),
                                    table: table.clone(),
                                    iids: iids.clone(),
                                },
                                _ => BatchOp::EstimateRanges {
                                    db: db.clone(),
                                    table: table.clone(),
                                    iids: iids.clone(),
                                },
                            };
                            push_task_grouped(conn_id, seq, worker_id, t, sid, op, shard_inboxes);
                        }
                    }
                    crate::metrics::SQL_JOIN_EST_ROUNDS
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return true;
                }
            }
            // 全部收集齐 / 阈值跳过 → 决策驱动表 → Gather
            sql_join_est_decide(conn, conn_id, seq, worker_id, shard_inboxes, num_shards);
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
                // ⭐ M3-2: 按 gather_order 推进 (默认空 = 数组序 idx+1)
                let (next_idx, is_last) = {
                    let c = conn.sql_join.get(&seq).unwrap();
                    let go = &c.gather_order;
                    if go.is_empty() {
                        (
                            if idx + 1 < ntables {
                                Some(idx + 1)
                            } else {
                                None
                            },
                            idx + 1 >= ntables,
                        )
                    } else {
                        let pos = go.iter().position(|&x| x == idx).unwrap();
                        (go.get(pos + 1).copied(), pos + 1 >= go.len())
                    }
                };
                if let Some(ni) = next_idx {
                    {
                        let c = conn.sql_join.get_mut(&seq).unwrap();
                        c.phase = JoinPhase::Gather(ni);
                        c.remaining = num_shards;
                        c.tables[ni].rows.clear();
                    }
                    sql_join_broadcast(
                        conn,
                        conn_id,
                        seq,
                        worker_id,
                        shard_inboxes,
                        num_shards,
                        ni,
                    );
                } else if is_last {
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
    let mut acc: Vec<Vec<ColValue>>;
    if ctx.swapped {
        // ⭐ M3-2 (连接顺序): 双表 Inner 驱动交换 — 驱动=tables[1] (先 Gather),
        // 被驱动=tables[0] (key_set 点查). 输出行 = [tables[0] 列][tables[1] 列]
        // (col_offset 固定, 保 SELECT * 列序与通用路径一致).
        acc = {
            let jc = &ctx.joins[0];
            let (d_rows, p_rows) = (&ctx.tables[1].rows, &ctx.tables[0].rows);
            // ON 等值键: (tables[1] proj 位, tables[0] proj 位)
            let mut eq_keys: Vec<(usize, usize)> = Vec::new();
            for on in &jc.on {
                if let sql::OnPred::Eq(l, r) = on {
                    let (lt, li) = sql_join_resolve_on(&ctx, l, 1).unwrap();
                    let (rt, ri) = sql_join_resolve_on(&ctx, r, 1).unwrap();
                    if lt == 1 {
                        eq_keys.push((pos_in(1, li), pos_in(0, ri)));
                    } else if rt == 1 {
                        eq_keys.push((pos_in(1, ri), pos_in(0, li)));
                    }
                }
            }
            // 被驱动 (tables[0]) 按 join 键建 hash
            let mut hash: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
            for (ri, row) in p_rows.iter().enumerate() {
                if let Some(k) = join_key_multi(row, eq_keys.iter().map(|&(_, p0)| p0)) {
                    hash.entry(k).or_default().push(ri);
                }
            }
            let mut out: Vec<Vec<ColValue>> = Vec::new();
            for d_row in d_rows {
                if let Some(k) = join_key_multi(d_row, eq_keys.iter().map(|&(p1, _)| p1))
                    && let Some(cands) = hash.get(&k)
                {
                    for &ri in cands {
                        let mut w = Vec::with_capacity(col_offset[2]);
                        w.extend_from_slice(&p_rows[ri]); // tables[0] 列
                        w.extend_from_slice(d_row); // tables[1] 列
                        out.push(w);
                    }
                }
                // Inner: 无匹配丢弃
            }
            out
        };
        if acc.len() > JOIN_MAX_ROWS {
            conn.resp_complete(
                seq,
                sql_err_bytes(conn.proto, "JOIN result too large (row cap exceeded)"),
            );
            return;
        }
    } else {
        // 原左深迭代 hash join (tables[0] 驱动, tables[1..] key_set/全量)
        acc = ctx.tables[0].rows.clone();
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
                        let lv = if lt == rt {
                            &right_row[pos_in(rt, li)]
                        } else {
                            &acc_row[wide_pos(lt, li)]
                        };
                        let rv = if rtt == rt {
                            &right_row[pos_in(rt, ri)]
                        } else {
                            &acc_row[wide_pos(rtt, ri)]
                        };
                        if !join_cmp_cols(lv, *op, rv) {
                            return false;
                        }
                    }
                }
                true
            };
            let extend =
                |acc_row: &[ColValue], right_row: Option<&Vec<ColValue>>| -> Vec<ColValue> {
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
                if !any && matches!(jc.kind, JoinKind::Left | JoinKind::Full) {
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
                // ⭐ compat: SELECT * 排除隐藏 __rowid 与已删列 (内部定位仍用全列)
                if sc.dropped.contains(&(i as u16)) || col.name == HIDDEN_ROWID {
                    continue;
                }
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
    let cols: Vec<(&str, ColType)> = out_plan
        .iter()
        .map(|(label, wp)| (label.as_str(), ty_of(*wp)))
        .collect();
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
        return cond
            .set
            .iter()
            .any(|v| sql_cmp(cv, v) == Some(Ordering::Equal));
    }
    // ⭐ compat: JSONB '?' — 列含顶层键
    if cond.op == CmpOp::JsonExists {
        let key = sql_to_col(ColType::Str, &cond.val).unwrap_or(ColValue::Null);
        return eval_json_exists(cv, &key);
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
            CmpOp::JsonExists => unreachable!(),
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
        CmpOp::JsonExists => false,
    }
}
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
pub(crate) fn sql_join_resolve_on(
    ctx: &SqlJoinCtx,
    qc: &QualCol,
    rt: usize,
) -> Result<(usize, u16), String> {
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
        // ⭐ JOIN 内部列定位 (ON/行重建) 需全列 proj; 隐藏/已删列在渲染输出段过滤
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
                    // ⭐ M3-2 (连接顺序): 双表 Inner + 纯等值 ON + 无预填 → 先 EstimateRows
                    // 收集两表行数, 选小表驱动 (swapped 时先 Gather 右表, 左表 key_set 点查).
                    let est_ok = {
                        let c = conn.sql_join.get(&seq).unwrap();
                        c.tables.len() == 2
                            && c.tables.iter().all(|t| !t.prefilled)
                            && matches!(c.joins[0].kind, JoinKind::Inner)
                            && c.joins[0]
                                .on
                                .iter()
                                .all(|o| matches!(o, sql::OnPred::Eq(..)))
                    };
                    if est_ok {
                        // ⭐ 方案 A (调优): 两表行数合并一轮广播 (group 0=tables[0], 1=tables[1]),
                        // 省 1 轮; 后续 distinct/ranges 仅在有候选索引列时收集, 且小表
                        // (行数 ≤ EST_SKIP_STATS_ROWS) 直接跳过 → 双表小 JOIN 固定只 1 轮.
                        let (db, t0, t1) = {
                            let c = conn.sql_join.get_mut(&seq).unwrap();
                            c.phase = JoinPhase::EstimateRows;
                            c.est_phase = 0;
                            c.est_rows = [0, 0];
                            c.join_distinct = vec![
                                std::collections::HashMap::new(),
                                std::collections::HashMap::new(),
                            ];
                            c.join_ranges = vec![
                                std::collections::HashMap::new(),
                                std::collections::HashMap::new(),
                            ];
                            c.remaining = 2 * num_shards;
                            c.gather_order = vec![0, 1]; // swapped 时改为 [1, 0]
                            (
                                c.db.clone(),
                                c.tables[0].table.clone(),
                                c.tables[1].table.clone(),
                            )
                        };
                        for sid in 0..num_shards {
                            push_task_grouped(
                                conn_id,
                                seq,
                                worker_id,
                                0,
                                sid,
                                BatchOp::EstimateRowCount {
                                    db: db.clone(),
                                    table: t0.clone(),
                                },
                                shard_inboxes,
                            );
                            push_task_grouped(
                                conn_id,
                                seq,
                                worker_id,
                                1,
                                sid,
                                BatchOp::EstimateRowCount {
                                    db: db.clone(),
                                    table: t1.clone(),
                                },
                                shard_inboxes,
                            );
                        }
                        crate::metrics::SQL_JOIN_EST_ROUNDS
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        return;
                    }
                    {
                        let c = conn.sql_join.get_mut(&seq).unwrap();
                        c.phase = JoinPhase::Gather(idx);
                        c.remaining = num_shards;
                        c.tables[idx].rows.clear();
                    }
                    sql_join_broadcast(
                        conn,
                        conn_id,
                        seq,
                        worker_id,
                        shard_inboxes,
                        num_shards,
                        idx,
                    );
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
        // ⭐ M2c: JOIN 叶子是 JoinCond, 做同列等值 OR→IN 合并 (a=1 OR a=2 → a IN (1,2)),
        // 让含 OR 的 AND 谓词重新进入 AND 下推路径
        let nconds = sql::or_eq_to_in::<sql::JoinCond>(&c.conds);
        for cond in nconds.as_conjuncts().unwrap_or_default() {
            let Ok((ti, cidx)) = sql_join_resolve(c, &cond.col) else {
                continue;
            };
            if ti != idx {
                continue;
            }
            // ⭐ compat: JSONB '?' 无 shard 下推语义 → 纯残余过滤
            if cond.op == CmpOp::JsonExists {
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
                CmpOp::JsonExists => unreachable!("上续 continue"),
            };
            if cond.op == CmpOp::In {
                let set: Vec<ColValue> = cond
                    .set
                    .iter()
                    .filter_map(|v| sql_to_col(ty, v).ok())
                    .collect();
                if set.len() == cond.set.len() {
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
/// ⭐ M3-5: 单边范围谓词的扫描占比估计 (0..1, 越小越窄越好; 无 min/max 或非数值 → 1.0).
/// Gt/Ge: (max - v)/(max - min); Lt/Le: (v - min)/(max - min).
fn range_ratio(
    ctx: &SqlJoinCtx,
    idx: usize,
    iid: u32,
    v: &ColValue,
    ty: ColType,
    is_gt: bool,
) -> f64 {
    let Some((lo_b, hi_b)) = ctx.join_ranges.get(idx).and_then(|m| m.get(&iid)) else {
        return 1.0;
    };
    let (Some(lo), Some(hi)) = (
        col_from_ordered_bytes(ty, lo_b),
        col_from_ordered_bytes(ty, hi_b),
    ) else {
        return 1.0;
    };
    let num = |c: &ColValue| -> Option<f64> {
        match c {
            ColValue::I64(n) => Some(*n as f64),
            ColValue::F64(n) => Some(*n),
            _ => None,
        }
    };
    let (Some(l), Some(h), Some(x)) = (num(&lo), num(&hi), num(v)) else {
        return 1.0;
    };
    if h <= l {
        return 1.0;
    }
    let r = if is_gt {
        (h - x) / (h - l)
    } else {
        (x - l) / (h - l)
    };
    r.clamp(0.0, 1.0)
}

/// ⭐ M3-4: 表 idx 的候选 Eq 索引列 (conds 中该表等值谓词列 → (列号, iid), 去重).
fn join_candidate_eq_iids(ctx: &SqlJoinCtx, idx: usize) -> Vec<(u16, u32)> {
    let Some(schema) = ctx.tables[idx].schema.as_ref() else {
        return Vec::new();
    };
    let mut out: Vec<(u16, u32)> = Vec::new();
    for cond in ctx.conds.as_conjuncts().unwrap_or_default() {
        if cond.op != CmpOp::Eq {
            continue;
        }
        let Ok((ti, cidx)) = sql_join_resolve(ctx, &cond.col) else {
            continue;
        };
        if ti != idx {
            continue;
        }
        if let Some(iid) = schema.indexes.iter().find(|i| i.col == cidx).map(|i| i.iid) {
            if !out.iter().any(|&(c, _)| c == cidx) {
                out.push((cidx, iid));
            }
        }
    }
    out
}

/// ⭐ M3-2: EstimateRows 收集完成 → 决策驱动表 (右表更小 → swapped) → 启动 Gather.
fn sql_join_est_decide(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    let gather_idx = {
        let c = conn.sql_join.get_mut(&seq).unwrap();
        let s = c.est_rows[1] < c.est_rows[0];
        c.swapped = s;
        c.gather_order = if s { vec![1, 0] } else { vec![0, 1] };
        c.gather_order[0]
    };
    {
        let c = conn.sql_join.get_mut(&seq).unwrap();
        c.phase = JoinPhase::Gather(gather_idx);
        c.remaining = num_shards;
        c.tables[gather_idx].rows.clear();
    }
    sql_join_broadcast(
        conn,
        conn_id,
        seq,
        worker_id,
        shard_inboxes,
        num_shards,
        gather_idx,
    );
}

pub(crate) fn sql_join_keyset_hint(
    ctx: &SqlJoinCtx,
    idx: usize,
) -> Option<shard_manager::KeySetHint> {
    // ⭐ M3-2: swapped 双表 — 仅被驱动表 tables[0] 用 key_set (来源 joins[0] + tables[1]),
    // 驱动表 tables[1] 保持全量 (idx=1 → None). 普通多表 — idx==0 全量, idx>=1 用 joins[idx-1].
    let (jc, prev_gt_ok) = if ctx.swapped {
        if idx != 0 {
            return None;
        }
        (&ctx.joins[0], true)
    } else {
        if idx == 0 {
            return None;
        }
        (&ctx.joins[idx - 1], false)
    };
    if !matches!(jc.kind, JoinKind::Inner | JoinKind::Left) {
        return None;
    }
    // 息含单个 Eq
    let eqs: Vec<&sql::OnPred> = jc
        .on
        .iter()
        .filter(|o| matches!(o, sql::OnPred::Eq(..)))
        .collect();
    if eqs.len() != 1 {
        return None;
    }
    let sql::OnPred::Eq(l, r) = eqs[0] else {
        return None;
    };
    // resolve 两侧 → (表下标, 列号)
    let (lt, li) = sql_join_resolve_on(ctx, l, idx).ok()?;
    let (rt, ri) = sql_join_resolve_on(ctx, r, idx).ok()?;
    // 分辨新表侧 (idx) 与已 gather 侧 (普通多表 = 前序 ti<idx; swapped 双表 = tables[1])
    let (new_col, prev_ti, prev_col) = if lt == idx && (prev_gt_ok || rt < idx) {
        (li, rt, ri)
    } else if rt == idx && (prev_gt_ok || lt < idx) {
        (ri, lt, li)
    } else {
        return None;
    };
    // 新表 join 列需有普通二级索引
    let schema = ctx.tables[idx].schema.as_ref()?;
    let iid = schema
        .indexes
        .iter()
        .find(|i| i.col == new_col)
        .map(|i| i.iid)?;
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
    // ⭐ M3-4: 多个 Eq 候选 → 选 distinct 最高 (选择性最大; 无 distinct 数据同分 → 取首个)
    let mut best_eq: Option<(u64, shard_manager::IndexHint)> = None;
    // ⭐ M3-5: 多个范围候选 → 选 min/max 区间占比最小 (最窄)
    let mut best_range: Option<(f64, shard_manager::IndexHint)> = None;
    for cond in ctx.conds.as_conjuncts().unwrap_or_default() {
        let Ok((ti, cidx)) = sql_join_resolve(ctx, &cond.col) else {
            continue;
        };
        if ti != idx {
            continue;
        }
        let Some(iid) = iid_of(cidx) else { continue };
        let ty = schema.columns[cidx as usize].ty;
        let Ok(v) = sql_to_col(ty, &cond.val) else {
            continue;
        };
        match cond.op {
            CmpOp::Eq => {
                let d = ctx
                    .join_distinct
                    .get(idx)
                    .and_then(|m| m.get(&iid))
                    .copied()
                    .unwrap_or(u64::MAX / 2);
                if best_eq.is_none() || d > best_eq.as_ref().unwrap().0 {
                    best_eq = Some((
                        d,
                        shard_manager::IndexHint {
                            iid,
                            lo: Some(v.clone()),
                            hi: Some(v),
                            pk: false,
                        },
                    ));
                }
            }
            // ⭐ M3-5: 范围候选用列 min/max 区间占比选最窄 (越小扫描行越少; 无统计 → 1.0)
            CmpOp::Gt | CmpOp::Ge => {
                let ratio = range_ratio(ctx, idx, iid, &v, ty, true);
                if best_range.is_none() || ratio < best_range.as_ref().unwrap().0 {
                    best_range = Some((
                        ratio,
                        shard_manager::IndexHint {
                            iid,
                            lo: Some(v),
                            hi: None,
                            pk: false,
                        },
                    ));
                }
            }
            CmpOp::Lt | CmpOp::Le => {
                let ratio = range_ratio(ctx, idx, iid, &v, ty, false);
                if best_range.is_none() || ratio < best_range.as_ref().unwrap().0 {
                    best_range = Some((
                        ratio,
                        shard_manager::IndexHint {
                            iid,
                            lo: None,
                            hi: Some(v),
                            pk: false,
                        },
                    ));
                }
            }
            _ => {}
        }
    }
    best_eq.map(|(_, h)| h).or(best_range.map(|(_, h)| h))
}
