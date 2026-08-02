//! SQL 聚合执行 (GROUP BY / 聚合函数 / HAVING / COUNT 特判) — 完成点.
//! 从 worker/mod.rs 拆分 (2026-08).

use super::*;

pub(crate) fn sql_run_agg_select(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    schema: std::sync::Arc<TableSchema>,
    table: String,
    items: Vec<sql::SelectItem>,
    conds: Pred<Cond>,
    group_by: Vec<String>,
    having: Pred<Cond>,
    order: Vec<(String, bool)>,
    limit: Option<u32>,
    offset: Option<u32>,
) {
    let fail = |conn: &mut ConnState, msg: String| {
        conn.resp_complete(seq, sql_err_bytes(conn.proto, &msg));
    };
    // 输出项解析 (列号 + label + 输出类型)
    let mut spec_items: Vec<AggItem> = Vec::with_capacity(items.len());
    for it in &items {
        match it {
            sql::SelectItem::Col { name: c, alias } => {
                let Some(i) = schema.col_by_name(c) else {
                    return fail(conn, format!("unknown column '{c}'"));
                };
                spec_items.push(AggItem {
                    label: alias.clone().unwrap_or_else(|| c.clone()),
                    kind: AggItemKind::Col(i),
                    out_ty: schema.columns[i as usize].ty,
                });
            }
            sql::SelectItem::Agg { func, arg, distinct, alias } => {
                // ⭐ F78: 绑定表达式 (裸列退化) → (BoundExpr, 推导类型); COUNT(*) arg=None
                let bound: Option<(BoundExpr, ColType)> = match arg {
                    Some(e) => match bind_scalar_expr(&schema, e) {
                        Ok(bt) => Some(bt),
                        Err(msg) => return fail(conn, msg),
                    },
                    None => None,
                };
                // ⭐ F77: DISTINCT 仅 COUNT(DISTINCT col) (解析已拦; 双保险)
                if *distinct
                    && (*func != sql::AggFn::Count
                        || arg.as_ref().and_then(|e| e.as_col()).is_none())
                {
                    return fail(conn, "DISTINCT is only supported in COUNT(col) (v1)".into());
                }
                let src_ty = bound.as_ref().map(|(_, t)| *t);
                // SUM/AVG 仅数值 (⭐ F81: 含 DECIMAL)
                if matches!(func, sql::AggFn::Sum | sql::AggFn::Avg)
                    && !matches!(
                        src_ty,
                        Some(ColType::I64) | Some(ColType::F64) | Some(ColType::Decimal { .. })
                    )
                {
                    return fail(
                        conn,
                        format!("{} requires a numeric argument", func.label(None)),
                    );
                }
                let out_ty = match func {
                    sql::AggFn::Count => ColType::I64,
                    sql::AggFn::Sum => src_ty.unwrap_or(ColType::I64),
                    sql::AggFn::Avg => ColType::F64,
                    sql::AggFn::Min | sql::AggFn::Max => src_ty.unwrap_or(ColType::Bytes),
                };
                let inner = match arg {
                    None => "*".to_string(),
                    Some(e) => e.render(),
                };
                let default_label = if *distinct {
                    format!("COUNT(DISTINCT {inner})")
                } else {
                    format!("{}({inner})", func.label(None).trim_end_matches("(*)"))
                };
                spec_items.push(AggItem {
                    label: alias.clone().unwrap_or(default_label),
                    kind: AggItemKind::Agg {
                        func: *func,
                        arg: bound.map(|(b, _)| b),
                        distinct: *distinct,
                    },
                    out_ty,
                });
            }
            sql::SelectItem::ScalarFn { .. } => {
                return fail(conn, "scalar functions are not supported in aggregate queries (v1)".into())
            }
        }
    }
    // 组键列号
    let mut group_idx: Vec<u16> = Vec::with_capacity(group_by.len());
    for g in &group_by {
        match schema.col_by_name(g) {
            Some(i) => group_idx.push(i),
            None => return fail(conn, format!("unknown column '{g}'")),
        }
    }
    // 输出列定位 helper: label 大小写归一匹配
    let find_out = |name: &str| -> Option<usize> {
        spec_items.iter().position(|it| it.label.eq_ignore_ascii_case(name))
    };
    // HAVING 谓词树 → (输出下标, op, val) 叶子树
    let having_out = match having.try_map(&|h: &Cond| -> Result<(usize, sql::CmpOp, sql::SqlValue), String> {
        find_out(&h.col)
            .map(|idx| (idx, h.op, h.val.clone()))
            .ok_or_else(|| format!("HAVING column '{}' must appear in the select list", h.col))
    }) {
        Ok(p) => p,
        Err(e) => return fail(conn, e),
    };
    // ORDER BY → (输出下标, desc)
    let mut order_out = Vec::with_capacity(order.len());
    for (name, desc) in &order {
        let Some(idx) = find_out(name) else {
            return fail(
                conn,
                format!("ORDER BY column '{name}' must appear in the select list"),
            );
        };
        order_out.push((idx, *desc));
    }
    let spec = AggSpec { items: spec_items, group_idx, having: having_out, order: order_out };
    // 广播: 索引计划可用则 IndexScan (界下推), 否则 TableScan (含 PkGet 降级)
    let plan = sql_plan_select(&schema, &conds);
    conn.sql_select_agg.insert(
        seq,
        SqlSelectAgg {
            remaining: num_shards,
            error: None,
            rows: Vec::new(),
            schema: schema.clone(),
            conds,
            limit,
            proj: Vec::new(),
            cover: None,
            unique_early: false, // 聚合需全量, 禁早停
            done: false,
            dml: None,
            dml_target: None,
            order: Vec::new(), // 排序在 agg_spec.order (输出列域)
            offset: offset.unwrap_or(0),
            count: false,
            agg_spec: Some(spec),
            out_names: Vec::new(),
        },
    );
    let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
    for sid in 0..num_shards {
        let op = match &plan {
            Ok(SqlPlan::Index { iid, lo, hi, .. }) => BatchOp::IndexScan {
                db: db.clone(),
                table: table_arc.clone(),
                iid: *iid,
                lo: lo.clone(),
                hi: hi.clone(),
                limit: 0,
                with_rows: true,
            },
            _ => BatchOp::TableScan { db: db.clone(), table: table_arc.clone(), limit: 0 },
        };
        push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
    }
}

/// ⭐ G2 (F63): 聚合计划 — dispatch 时列名已解析为列号/输出下标.
pub(crate) struct AggSpec {
    /// 输出列序 (label 供列头与 HAVING/ORDER 匹配).
    items: Vec<AggItem>,
    /// 组键列号 (空 = 全表单桶).
    group_idx: Vec<u16>,
    /// HAVING: (输出列下标, 算子, 右值).
    having: Pred<(usize, sql::CmpOp, sql::SqlValue)>,
    /// ORDER BY: (输出列下标, desc).
    order: Vec<(usize, bool)>,
}

pub(crate) struct AggItem {
    label: String,
    kind: AggItemKind,
    out_ty: ColType,
}

pub(crate) enum AggItemKind {
    /// 组键列直出 (必 ∈ group_by, 解析层已校验).
    Col(u16),
    /// ⭐ F78: arg = 已绑定列号的表达式 (None = COUNT(*)).
    Agg { func: sql::AggFn, arg: Option<BoundExpr>, distinct: bool },
}

/// ⭐ F78: 已绑定 (列名→列号) 的聚合内标量表达式.
pub(crate) enum BoundExpr {
    Col(u16),
    Lit(ColValue),
    Bin { op: sql::ArithOp, l: Box<BoundExpr>, r: Box<BoundExpr> },
}

/// ⭐ F78: 逐行求值 — 任一操作数 NULL/非数值 → NULL; Div 除零 → NULL;
/// 全整型且非 Div → I64 (溢出→NULL); 否则 F64.
pub(crate) fn eval_bound_expr(e: &BoundExpr, row: &[ColValue]) -> ColValue {
    match e {
        BoundExpr::Col(i) => row.get(*i as usize).cloned().unwrap_or(ColValue::Null),
        BoundExpr::Lit(v) => v.clone(),
        BoundExpr::Bin { op, l, r } => {
            let lv = eval_bound_expr(l, row);
            let rv = eval_bound_expr(r, row);
            // 提数: (值, 是否整型); 非数值/NULL → None
            let num = |v: &ColValue| -> Option<(f64, bool)> {
                match v {
                    ColValue::I64(x) => Some((*x as f64, true)),
                    ColValue::F64(x) => Some((*x, false)),
                    _ => None,
                }
            };
            let (Some((lf, li)), Some((rf, ri))) = (num(&lv), num(&rv)) else {
                return ColValue::Null;
            };
            let both_int = li && ri && *op != sql::ArithOp::Div;
            if both_int {
                let (a, b) = (lf as i64, rf as i64);
                let out = match op {
                    sql::ArithOp::Add => a.checked_add(b),
                    sql::ArithOp::Sub => a.checked_sub(b),
                    sql::ArithOp::Mul => a.checked_mul(b),
                    sql::ArithOp::Div => unreachable!(),
                };
                out.map(ColValue::I64).unwrap_or(ColValue::Null)
            } else {
                let out = match op {
                    sql::ArithOp::Add => lf + rf,
                    sql::ArithOp::Sub => lf - rf,
                    sql::ArithOp::Mul => lf * rf,
                    sql::ArithOp::Div => {
                        if rf == 0.0 {
                            return ColValue::Null;
                        }
                        lf / rf
                    }
                };
                ColValue::F64(out)
            }
        }
    }
}

/// ⭐ F78: 将解析期 ScalarExpr 绑定列号 + 推导输出类型 (未知列报错).
pub(crate) fn bind_scalar_expr(
    schema: &TableSchema,
    e: &sql::ScalarExpr,
) -> Result<(BoundExpr, ColType), String> {
    match e {
        sql::ScalarExpr::Col(name) => {
            let i = schema.col_by_name(name).ok_or_else(|| format!("unknown column '{name}'"))?;
            Ok((BoundExpr::Col(i), schema.columns[i as usize].ty))
        }
        sql::ScalarExpr::Lit(v) => {
            let (cv, ty) = match v {
                SqlValue::Int(x) => (ColValue::I64(*x), ColType::I64),
                SqlValue::Float(x) => (ColValue::F64(*x), ColType::F64),
                SqlValue::Str(b) => (ColValue::Bytes(b.clone()), ColType::Str),
                _ => return Err("unsupported literal in aggregate expression".into()),
            };
            Ok((BoundExpr::Lit(cv), ty))
        }
        sql::ScalarExpr::Bin { op, l, r } => {
            let (lb, lt) = bind_scalar_expr(schema, l)?;
            let (rb, rt) = bind_scalar_expr(schema, r)?;
            // 输出类型: Div → F64; 任一 F64 → F64; 否则 I64
            let out_ty = if *op == sql::ArithOp::Div || lt == ColType::F64 || rt == ColType::F64 {
                ColType::F64
            } else {
                ColType::I64
            };
            Ok((BoundExpr::Bin { op: *op, l: Box::new(lb), r: Box::new(rb) }, out_ty))
        }
    }
}

/// ⭐ G2 (F63): 聚合累加器 (NULL 忽略, COUNT(*) 除外; SUM 整列溢出报错).
pub(crate) enum Accum {
    CountStar(u64),
    CountCol(u64),
    /// ⭐ F77: COUNT(DISTINCT col) — 去重集 (类型标记编码, 不计 NULL).
    CountDistinct(std::collections::HashSet<Vec<u8>>),
    SumI { acc: i64, seen: bool },
    SumF { acc: f64, seen: bool },
    /// ⭐ F81: SUM(DECIMAL) → i128 定标累加, 输出同 scale Decimal.
    SumDec { acc: i128, scale: u8, seen: bool },
    Avg { sum: f64, n: u64 },
    Min(Option<ColValue>),
    Max(Option<ColValue>),
}

impl Accum {
    fn new(func: sql::AggFn, is_star: bool, col_ty: Option<ColType>, distinct: bool) -> Self {
        match func {
            // ⭐ F77: COUNT(DISTINCT col) → 去重集
            sql::AggFn::Count if distinct => Accum::CountDistinct(std::collections::HashSet::new()),
            sql::AggFn::Count if is_star => Accum::CountStar(0),
            sql::AggFn::Count => Accum::CountCol(0),
            sql::AggFn::Sum => match col_ty {
                Some(ColType::F64) => Accum::SumF { acc: 0.0, seen: false },
                Some(ColType::Decimal { scale, .. }) => {
                    Accum::SumDec { acc: 0, scale, seen: false }
                }
                _ => Accum::SumI { acc: 0, seen: false },
            },
            sql::AggFn::Avg => Accum::Avg { sum: 0.0, n: 0 },
            sql::AggFn::Min => Accum::Min(None),
            sql::AggFn::Max => Accum::Max(None),
        }
    }

    fn feed(&mut self, v: &ColValue) -> Result<(), String> {
        match self {
            Accum::CountStar(n) => *n += 1,
            Accum::CountCol(n) => {
                if !matches!(v, ColValue::Null) {
                    *n += 1;
                }
            }
            // ⭐ F77: COUNT(DISTINCT) — 非 NULL 值按类型标记编码入集
            Accum::CountDistinct(set) => {
                if !matches!(v, ColValue::Null) {
                    set.insert(encode_col_key(v));
                }
            }
            Accum::SumI { acc, seen } => match v {
                ColValue::I64(x) => {
                    *acc = acc.checked_add(*x).ok_or("SUM overflow (BIGINT)")?;
                    *seen = true;
                }
                ColValue::Null => {}
                _ => return Err("SUM requires a numeric column".into()),
            },
            Accum::SumF { acc, seen } => match v {
                ColValue::F64(x) => {
                    *acc += x;
                    *seen = true;
                }
                ColValue::I64(x) => {
                    *acc += *x as f64;
                    *seen = true;
                }
                ColValue::Null => {}
                _ => return Err("SUM requires a numeric column".into()),
            },
            // ⭐ F81: SUM(DECIMAL) 定标 i128 累加 (同 scale)
            Accum::SumDec { acc, seen, .. } => match v {
                ColValue::Decimal(x, _) => {
                    *acc = acc.checked_add(*x).ok_or("SUM overflow (DECIMAL)")?;
                    *seen = true;
                }
                ColValue::Null => {}
                _ => return Err("SUM requires a numeric column".into()),
            },
            Accum::Avg { sum, n } => match v {
                ColValue::F64(x) => {
                    *sum += x;
                    *n += 1;
                }
                ColValue::I64(x) => {
                    *sum += *x as f64;
                    *n += 1;
                }
                // ⭐ F81: AVG(DECIMAL) → f64 (v1; 精度回退)
                ColValue::Decimal(x, sc) => {
                    *sum += *x as f64 / 10f64.powi(*sc as i32);
                    *n += 1;
                }
                ColValue::Null => {}
                _ => return Err("AVG requires a numeric column".into()),
            },
            Accum::Min(cur) => {
                if !matches!(v, ColValue::Null)
                    && cur.as_ref().is_none_or(|c| cmp_colvalue(v, c).is_lt())
                {
                    *cur = Some(v.clone());
                }
            }
            Accum::Max(cur) => {
                if !matches!(v, ColValue::Null)
                    && cur.as_ref().is_none_or(|c| cmp_colvalue(v, c).is_gt())
                {
                    *cur = Some(v.clone());
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> ColValue {
        match self {
            Accum::CountStar(n) | Accum::CountCol(n) => ColValue::I64(n as i64),
            // ⭐ F77: COUNT(DISTINCT) → 去重集基数
            Accum::CountDistinct(set) => ColValue::I64(set.len() as i64),
            // SUM 空集 → NULL (SQL 语义)
            Accum::SumI { seen: false, .. } | Accum::SumF { seen: false, .. } => ColValue::Null,
            Accum::SumI { acc, .. } => ColValue::I64(acc),
            Accum::SumF { acc, .. } => ColValue::F64(acc),
            // ⭐ F81: SUM(DECIMAL) 空集→NULL; 否则同 scale Decimal
            Accum::SumDec { seen: false, .. } => ColValue::Null,
            Accum::SumDec { acc, scale, .. } => ColValue::Decimal(acc, scale),
            Accum::Avg { n: 0, .. } => ColValue::Null,
            Accum::Avg { sum, n } => ColValue::F64(sum / n as f64),
            Accum::Min(v) | Accum::Max(v) => v.unwrap_or(ColValue::Null),
        }
    }
}

/// 同型 ColValue 全序比较 (Null 最小; 异型按枚举序 — 同列值不会异型).
pub(crate) fn cmp_colvalue(a: &ColValue, b: &ColValue) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (ColValue::Null, ColValue::Null) => Equal,
        (ColValue::Null, _) => Less,
        (_, ColValue::Null) => Greater,
        (ColValue::I64(x), ColValue::I64(y)) => x.cmp(y),
        (ColValue::F64(x), ColValue::F64(y)) => x.partial_cmp(y).unwrap_or(Equal),
        (ColValue::I64(x), ColValue::F64(y)) => (*x as f64).partial_cmp(y).unwrap_or(Equal),
        (ColValue::F64(x), ColValue::I64(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Equal),
        (ColValue::Bytes(x), ColValue::Bytes(y)) => x.cmp(y),
        // ⭐ F81: 同列 Decimal 同 scale → 定标整数直接比较
        (ColValue::Decimal(x, _), ColValue::Decimal(y, _)) => x.cmp(y),
        (ColValue::I64(_) | ColValue::F64(_), ColValue::Bytes(_)) => Less,
        (ColValue::Bytes(_), ColValue::I64(_) | ColValue::F64(_)) => Greater,
        // Decimal 与异型 (同列不会发生): 稳定兜底
        (ColValue::Decimal(_, _), _) => Greater,
        (_, ColValue::Decimal(_, _)) => Less,
    }
}

/// 分桶数上限 (防内存失控).
const AGG_MAX_GROUPS: usize = 64 * 1024;

/// ⭐ G2 (F63): 分桶聚合完成点 — 已过滤行 → 分桶 → 累加 → HAVING →
/// ORDER → OFFSET/LIMIT → 合成结果集 (sql_rows_bytes 三门面统一).
/// ⭐ F69: HAVING 谓词树递归求值 (输出列下标域; NULL 不满足任何比较).
pub(crate) fn eval_having_pred(
    row: &[ColValue],
    pred: &Pred<(usize, sql::CmpOp, sql::SqlValue)>,
) -> bool {
    match pred {
        Pred::Leaf((idx, op, val)) => {
            let rhs = match val {
                sql::SqlValue::Int(x) => ColValue::I64(*x),
                sql::SqlValue::Float(x) => ColValue::F64(*x),
                sql::SqlValue::Str(s) => ColValue::Bytes(s.clone()),
                _ => return false,
            };
            if matches!(row[*idx], ColValue::Null) {
                return false; // NULL 不满足任何比较 (SQL 语义)
            }
            let ord = cmp_colvalue(&row[*idx], &rhs);
            match op {
                sql::CmpOp::Eq => ord.is_eq(),
                sql::CmpOp::Ne => ord.is_ne(),
                sql::CmpOp::Gt => ord.is_gt(),
                sql::CmpOp::Ge => ord.is_ge(),
                sql::CmpOp::Lt => ord.is_lt(),
                sql::CmpOp::Le => ord.is_le(),
                sql::CmpOp::In => false, // HAVING 不支持 IN
            }
        }
        Pred::And(v) => v.iter().all(|p| eval_having_pred(row, p)),
        Pred::Or(v) => v.iter().any(|p| eval_having_pred(row, p)),
        Pred::Not(b) => !eval_having_pred(row, b),
    }
}

/// ⭐ F77: 列值自包含类型标记编码 (只求相等性 + 确定序) —
/// GROUP BY 组键与 COUNT(DISTINCT) 去重集同源, 保证一致. 0=Null/1=I64/2=F64/3=Bytes.
pub(crate) fn encode_col_key_into(key: &mut Vec<u8>, v: &ColValue) {
    match v {
        ColValue::Null => key.push(0u8),
        ColValue::I64(x) => {
            key.push(1u8);
            key.extend_from_slice(&((*x as u64) ^ (1u64 << 63)).to_be_bytes());
        }
        ColValue::F64(x) => {
            key.push(2u8);
            key.extend_from_slice(&x.to_bits().to_be_bytes());
        }
        ColValue::Bytes(b) => {
            key.push(3u8);
            key.extend_from_slice(&(b.len() as u32).to_be_bytes());
            key.extend_from_slice(b);
        }
        // ⭐ F81: Decimal (tag 4 + 16B i128 LE); 同列同 scale → 定标整数唯一
        ColValue::Decimal(x, _) => {
            key.push(4u8);
            key.extend_from_slice(&x.to_le_bytes());
        }
    }
}

pub(crate) fn encode_col_key(v: &ColValue) -> Vec<u8> {
    let mut k = Vec::new();
    encode_col_key_into(&mut k, v);
    k
}

pub(crate) fn materialize_agg_groups(
    spec: &AggSpec,
    rows: Vec<Vec<ColValue>>,
    offset: u32,
    limit: Option<u32>,
) -> MatResult {
    // 分桶: 组键 = 各列保序编码级联 (NULL 归一组, 0x00 标记); BTreeMap =
    // 无 ORDER BY 时输出按组键序 (确定性)
    let mut buckets: std::collections::BTreeMap<Vec<u8>, (Vec<ColValue>, Vec<Accum>)> =
        std::collections::BTreeMap::new();
    let new_accums = |first_row: &[ColValue]| -> Vec<Accum> {
        let _ = first_row;
        spec.items
            .iter()
            .map(|it| match &it.kind {
                AggItemKind::Col(_) => Accum::CountStar(0), // 占位不用 (代表值直出)
                AggItemKind::Agg { func, arg, distinct } => {
                    // ⭐ F81: 直接传 out_ty (含 Decimal{scale}), 让 Accum 选 SumDec/SumF/SumI
                    Accum::new(*func, arg.is_none(), Some(it.out_ty), *distinct)
                }
            })
            .collect()
    };
    // 无 group_by = 全表单桶 (空表也输出一行 — PG 语义)
    if spec.group_idx.is_empty() {
        buckets.insert(Vec::new(), (Vec::new(), new_accums(&[])));
    }
    for values in &rows {
        let mut key = Vec::new();
        for &gi in &spec.group_idx {
            // 自包含类型标记编码 (只求相等性 + 确定序; 代表值另存)
            encode_col_key_into(&mut key, &values[gi as usize]);
        }
        if !buckets.contains_key(&key) && buckets.len() >= AGG_MAX_GROUPS {
            return Err("too many groups (limit 65536)".into());
        }
        let entry = buckets
            .entry(key)
            .or_insert_with(|| (values.clone(), new_accums(values)));
        for (it, acc) in spec.items.iter().zip(entry.1.iter_mut()) {
            if let AggItemKind::Agg { arg, .. } = &it.kind {
                // ⭐ F78: arg=Some → 逐行求值 (裸列/字面量/算术); None(COUNT(*)) → 常量 1
                match arg {
                    Some(e) => {
                        let v = eval_bound_expr(e, values);
                        acc.feed(&v)?;
                    }
                    None => acc.feed(&ColValue::I64(1))?,
                }
            }
        }
    }
    // 桶 → 输出行 (materialize)
    let mut out: Vec<Vec<ColValue>> = Vec::with_capacity(buckets.len());
    for (_, (rep, accums)) in buckets {
        let mut row = Vec::with_capacity(spec.items.len());
        let mut accums = accums.into_iter();
        for it in &spec.items {
            match &it.kind {
                AggItemKind::Col(ci) => {
                    accums.next(); // 跳过占位累加器
                    row.push(rep.get(*ci as usize).cloned().unwrap_or(ColValue::Null));
                }
                AggItemKind::Agg { .. } => {
                    row.push(accums.next().expect("accum 与 items 同长").finish());
                }
            }
        }
        out.push(row);
    }
    // HAVING (输出列比较; ⭐ F69 递归 AND/OR/NOT)
    out.retain(|row| eval_having_pred(row, &spec.having));
    // ORDER BY 输出列
    if !spec.order.is_empty() {
        out.sort_by(|a, b| {
            for (idx, desc) in &spec.order {
                let ord = cmp_colvalue(&a[*idx], &b[*idx]);
                if !ord.is_eq() {
                    return if *desc { ord.reverse() } else { ord };
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    // OFFSET / LIMIT
    let start = (offset as usize).min(out.len());
    let end = match limit {
        Some(l) => (start + l as usize).min(out.len()),
        None => out.len(),
    };
    // 合成结果集 (render_sql_count 同源路径, 三门面统一)
    let cols: Vec<(String, ColType)> =
        spec.items.iter().map(|it| (it.label.clone(), it.out_ty)).collect();
    Ok((cols, out[start..end].to_vec()))
}

/// SELECT 聚合完成渲染: (val, pk) 排序 → 覆盖重建或 decode → 残余过滤
/// → ⭐ S2: ORDER BY → OFFSET → LIMIT → 投影/COUNT 结果集.
/// (⭐ O3: 早停时提前调用, agg.rows 取走清空)
pub(crate) fn render_select_agg(proto: ProtocolKind, binary: bool, agg: &mut SqlSelectAgg) -> Vec<u8> {
    match materialize_select_agg(agg) {
        Ok((cols, rows)) => {
            let cref: Vec<(&str, ColType)> = cols.iter().map(|(n, t)| (n.as_str(), *t)).collect();
            sql_rows_bytes(proto, binary, &cref, &rows)
        }
        Err(e) => sql_err_bytes(proto, &e),
    }
}

/// ⭐ F71: SELECT 完成点物化 (不渲染) — 返回最终投影列定义 + 行集.
/// 供子查询捕获 (materialize) 与正常渲染 (render_select_agg) 共用.
pub(crate) fn materialize_select_agg(
    agg: &mut SqlSelectAgg,
) -> MatResult {
    if let Some(e) = agg.error.take() {
        return Err(e);
    }
    // 全局序: (索引值, pk); 残余过滤全条件 (下推界是超集, 过滤幂等)
    let mut rows = std::mem::take(&mut agg.rows);
    rows.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    let early_cut: Option<usize> = if agg.count || !agg.order.is_empty() || agg.agg_spec.is_some()
    {
        None
    } else {
        agg.limit.map(|l| (l + agg.offset) as usize)
    };
    let mut out_rows: Vec<Vec<ColValue>> = Vec::new();
    for (val, pk, rb) in &rows {
        let decoded = if let Some((idx_col, pk_col)) = agg.cover {
            let n = agg.schema.columns.len();
            let iv = col_from_ordered_bytes(agg.schema.columns[idx_col as usize].ty, val);
            let pv = col_from_ordered_bytes(agg.schema.columns[pk_col as usize].ty, pk);
            match (iv, pv) {
                (Some(iv), Some(pv)) => {
                    let mut values = vec![ColValue::Null; n];
                    values[idx_col as usize] = iv;
                    values[pk_col as usize] = pv;
                    Ok(values)
                }
                _ => Err("bad covered index entry".to_string()),
            }
        } else {
            storage::row::decode_row(&agg.schema, rb).map_err(|e| e.to_string())
        };
        let values = decoded?;
        if eval_pred(&agg.schema, &values, &agg.conds) {
            out_rows.push(values);
            if let Some(cut) = early_cut
                && out_rows.len() >= cut
            {
                break;
            }
        }
    }
    // ⭐ G2: 广义聚合
    if let Some(spec) = agg.agg_spec.take() {
        return materialize_agg_groups(&spec, out_rows, agg.offset, agg.limit);
    }
    // ⭐ S2: COUNT(*)
    if agg.count {
        return Ok((
            vec![("COUNT(*)".to_string(), ColType::I64)],
            vec![vec![ColValue::I64(out_rows.len() as i64)]],
        ));
    }
    if !agg.order.is_empty() {
        out_rows.sort_by(|a, b| sql_order_cmp(a, b, &agg.order));
    }
    let start = (agg.offset as usize).min(out_rows.len());
    let end = match agg.limit {
        Some(l) => (start + l as usize).min(out_rows.len()),
        None => out_rows.len(),
    };
    // 投影到输出列 (与 render_sql_rows 同义); ⭐ F76: out_names 有则用作列名 (AS 别名)
    let cols: Vec<(String, ColType)> = agg
        .proj
        .iter()
        .enumerate()
        .map(|(k, &i)| {
            let c = &agg.schema.columns[i as usize];
            let name = agg
                .out_names
                .get(k)
                .and_then(|o| o.clone())
                .unwrap_or_else(|| c.name.clone());
            (name, c.ty)
        })
        .collect();
    let proj_rows: Vec<Vec<ColValue>> = out_rows[start..end]
        .iter()
        .map(|r| agg.proj.iter().map(|&i| r[i as usize].clone()).collect())
        .collect();
    Ok((cols, proj_rows))
}

// =====================================================================
// ⭐ Z2 (MySQL wire 门面): 帧循环 — 握手/登录状态机 + COM_QUERY
// =====================================================================

