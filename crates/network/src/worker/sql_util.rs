//! SQL 纯工具函数 — 不依赖 ConnState 状态, 仅参数/全局类型.
//! 从 worker/mod.rs 拆分 (2026-08) — 子模块可见父模块私有项.

use super::sql_encode::render_decimal;
use super::*;

/// 语句分派: CREATE 广播 schema; INSERT/SELECT 需 schema (缓存 miss 先拉).
/// ⭐ 事务 v1 (F61): PG 帧流中是否含 ErrorResponse ('E' 帧) —
/// resp_complete 单点检测, 事务内出错置 failed (25P02 语义).
pub(crate) fn pg_frames_contain_error(bytes: &[u8]) -> bool {
    let mut pos = 0usize;
    while pos + 5 <= bytes.len() {
        let ty = bytes[pos];
        let len = u32::from_be_bytes([
            bytes[pos + 1],
            bytes[pos + 2],
            bytes[pos + 3],
            bytes[pos + 4],
        ]) as usize;
        if ty == b'E' {
            return true;
        }
        pos += 1 + len.max(4); // len 含自身 4B
    }
    false
}

/// ColValue → SqlValue (子查询结果折叠回字面量).
pub(crate) fn colval_to_sqlval(cv: &ColValue) -> SqlValue {
    match cv {
        ColValue::Null => SqlValue::Null,
        ColValue::I64(i) => SqlValue::Int(*i),
        ColValue::F64(f) => SqlValue::Float(*f),
        ColValue::Bytes(b) => SqlValue::Str(b.clone()),
        // ⭐ F81: Decimal 折叠回字面量用定点文本 (保精度; 目标列再按 scale 解析)
        ColValue::Decimal(x, scale) => SqlValue::Str(render_decimal(*x, *scale).into_bytes()),
    }
}

/// stmt 的 WHERE conds (Select/Delete/Update) 只读引用.
pub(crate) fn stmt_where_conds(stmt: &SqlStmt) -> Option<&Pred<Cond>> {
    match stmt {
        SqlStmt::Select { conds, .. }
        | SqlStmt::Delete { conds, .. }
        | SqlStmt::Update { conds, .. } => Some(conds),
        _ => None,
    }
}

/// 重建 stmt 替换 conds (折叠后重跑外层用).
pub(crate) fn stmt_replace_conds(stmt: SqlStmt, new: Pred<Cond>) -> SqlStmt {
    match stmt {
        SqlStmt::Select {
            table,
            items,
            limit,
            order,
            offset,
            group_by,
            having,
            limit_param,
            offset_param,
            ..
        } => SqlStmt::Select {
            table,
            items,
            conds: new,
            limit,
            order,
            offset,
            group_by,
            having,
            limit_param,
            offset_param,
        },
        SqlStmt::Delete { table, .. } => SqlStmt::Delete { table, conds: new },
        SqlStmt::Update { table, sets, .. } => SqlStmt::Update {
            table,
            sets,
            conds: new,
        },
        other => other,
    }
}

/// DFS 左右序收集 WHERE 中的子查询内层 stmt (与 fold 同序).
pub(crate) fn collect_pred_subq(pred: &Pred<Cond>, out: &mut Vec<SqlStmt>) {
    match pred {
        Pred::Leaf(c) => {
            if let SqlValue::Subquery(s) = &c.val {
                out.push((**s).clone());
            }
        }
        Pred::And(v) | Pred::Or(v) => v.iter().for_each(|p| collect_pred_subq(p, out)),
        Pred::Not(b) => collect_pred_subq(b, out),
    }
}

pub(crate) fn true_pred() -> Pred<Cond> {
    Pred::And(vec![])
}

pub(crate) fn false_pred() -> Pred<Cond> {
    Pred::Not(Box::new(Pred::And(vec![])))
}

/// ⭐ F74: 该子查询 stmt 的 WHERE 是否含相关列 (ColRef) — 判定关联性.
pub(crate) fn subquery_has_colref(inner: &SqlStmt) -> bool {
    stmt_where_conds(inner).is_some_and(|p| {
        p.leaves()
            .iter()
            .any(|c| matches!(c.val, SqlValue::ColRef(_)))
    })
}

/// ⭐ F74: 相关等值两侧分类 → (外层列名, 内层列名). 一侧外层一侧内层, 否则 Err.
pub(crate) fn classify_corr(
    outer_table: &str,
    inner_table: &str,
    a: &QualCol,
    b: &QualCol,
) -> Result<(String, String), String> {
    let is_outer = |q: &QualCol| {
        q.qualifier
            .as_deref()
            .is_some_and(|x| x.eq_ignore_ascii_case(outer_table))
    };
    let is_inner = |q: &QualCol| match &q.qualifier {
        Some(x) => x.eq_ignore_ascii_case(inner_table),
        None => true, // 无限定 → 默认内层
    };
    if is_outer(a) && !is_outer(b) && is_inner(b) {
        Ok((a.col.clone(), b.col.clone()))
    } else if is_outer(b) && !is_outer(a) && is_inner(a) {
        Ok((b.col.clone(), a.col.clone()))
    } else {
        Err("correlated equality must reference one outer and one inner column (v1)".into())
    }
}

/// ⭐ F74: 单个关联 EXISTS 内层 → 非关联 IN 叶 (`外层列 IN (SELECT 内层列 FROM .. WHERE 剩余)`).
pub(crate) fn decorrelate_exists(outer_table: &str, inner: &SqlStmt) -> Result<Pred<Cond>, String> {
    let SqlStmt::Select {
        table: inner_table,
        conds,
        ..
    } = inner
    else {
        return Err("correlated EXISTS inner must be a simple SELECT (v1)".into());
    };
    let Some(conjuncts) = conds.as_conjuncts() else {
        return Err("correlated EXISTS supports only AND conditions (v1)".into());
    };
    let mut corr: Option<(String, String)> = None;
    let mut remaining: Vec<Cond> = Vec::new();
    for c in conjuncts {
        if let SqlValue::ColRef(rhs) = &c.val {
            if c.op != CmpOp::Eq {
                return Err("correlated condition must be equality (v1)".into());
            }
            if corr.is_some() {
                return Err("correlated EXISTS supports only a single equality (v1)".into());
            }
            let pair = classify_corr(
                outer_table,
                inner_table,
                &QualCol::parse(&c.col),
                &QualCol::parse(rhs),
            )?;
            corr = Some(pair);
        } else {
            remaining.push(c.clone());
        }
    }
    let Some((outer_col, inner_col)) = corr else {
        return Err("correlated EXISTS: no correlation equality found (v1)".into());
    };
    let new_conds = if remaining.is_empty() {
        Pred::And(vec![])
    } else {
        Pred::And(remaining.into_iter().map(Pred::Leaf).collect())
    };
    let new_inner = SqlStmt::Select {
        table: inner_table.clone(),
        items: vec![sql::SelectItem::Col {
            name: inner_col,
            alias: None,
        }],
        conds: new_conds,
        limit: None,
        order: vec![],
        offset: None,
        group_by: vec![],
        having: Pred::And(vec![]),
        limit_param: None,
        offset_param: None,
    };
    Ok(Pred::Leaf(Cond {
        col: outer_col,
        op: CmpOp::In,
        val: SqlValue::Subquery(Box::new(new_inner)),
        set: vec![],
    }))
}

/// ⭐ F74: 单叶去相关. 关联 EXISTS → IN; 非关联原样; 其余含相关形态 → 拒.
pub(crate) fn decorrelate_leaf(outer_table: &str, c: &Cond) -> Result<Pred<Cond>, String> {
    if c.col == sql::EXISTS_SENTINEL_COL
        && let SqlValue::Subquery(inner) = &c.val
    {
        if subquery_has_colref(inner) {
            return decorrelate_exists(outer_table, inner);
        }
        return Ok(Pred::Leaf(c.clone())); // 非关联 EXISTS (F71 处理)
    }
    if matches!(c.val, SqlValue::ColRef(_)) {
        return Err("correlated subquery not supported (v1, only single-equality EXISTS)".into());
    }
    if let SqlValue::Subquery(inner) = &c.val
        && subquery_has_colref(inner)
    {
        return Err("correlated subquery not supported (v1, only single-equality EXISTS)".into());
    }
    Ok(Pred::Leaf(c.clone()))
}

/// ⭐ F74: 递归去相关整个谓词树 (NOT EXISTS 包在 Pred::Not 内, 改写叶后自然成 NOT IN).
pub(crate) fn decorrelate_pred(outer_table: &str, pred: &Pred<Cond>) -> Result<Pred<Cond>, String> {
    match pred {
        Pred::Leaf(c) => decorrelate_leaf(outer_table, c),
        Pred::And(v) => Ok(Pred::And(
            v.iter()
                .map(|p| decorrelate_pred(outer_table, p))
                .collect::<Result<_, _>>()?,
        )),
        Pred::Or(v) => Ok(Pred::Or(
            v.iter()
                .map(|p| decorrelate_pred(outer_table, p))
                .collect::<Result<_, _>>()?,
        )),
        Pred::Not(b) => Ok(Pred::Not(Box::new(decorrelate_pred(outer_table, b)?))),
    }
}

/// ⭐ F74: 去相关整个 stmt 的 WHERE (仅 Select/Delete/Update). 无相关时返回原 stmt.
pub(crate) fn decorrelate_stmt(stmt: &SqlStmt) -> Result<SqlStmt, String> {
    let table = match stmt {
        SqlStmt::Select { table, .. }
        | SqlStmt::Delete { table, .. }
        | SqlStmt::Update { table, .. } => table.clone(),
        _ => return Ok(stmt.clone()),
    };
    let conds = stmt_where_conds(stmt).expect("has where");
    let new = decorrelate_pred(&table, conds)?;
    Ok(stmt_replace_conds(stmt.clone(), new))
}

/// 单个子查询叶子折叠. rows = 内层投影行集.
pub(crate) fn fold_one_subq(c: &Cond, rows: &[Vec<ColValue>]) -> Result<Pred<Cond>, String> {
    // EXISTS: 哨兵空列名 → 非空真/空假
    if c.col == sql::EXISTS_SENTINEL_COL {
        return Ok(if rows.is_empty() {
            false_pred()
        } else {
            true_pred()
        });
    }
    // IN 子查询: 各行首列 → set (跳 NULL); 空集 → 恒假
    if c.op == CmpOp::In {
        let mut set: Vec<SqlValue> = rows
            .iter()
            .filter_map(|r| r.first())
            .map(colval_to_sqlval)
            .filter(|v| *v != SqlValue::Null)
            .collect();
        if set.is_empty() {
            return Ok(false_pred());
        }
        // ⭐ F73: 排序去重 → 大集合求值二分化; 去重后 > SUBQ_IN_MAX 才报错
        sql::sort_in_set(&mut set);
        if set.len() > SUBQ_IN_MAX {
            return Err(format!(
                "IN subquery returns too many rows ({} > {SUBQ_IN_MAX})",
                set.len()
            ));
        }
        return Ok(Pred::Leaf(Cond {
            col: c.col.clone(),
            op: CmpOp::In,
            val: SqlValue::Null,
            set,
        }));
    }
    // 标量子查询: 0 行→假, 1 行→常量, >1→错
    match rows.len() {
        0 => Ok(false_pred()),
        1 => {
            let sv = rows[0]
                .first()
                .map(colval_to_sqlval)
                .unwrap_or(SqlValue::Null);
            if sv == SqlValue::Null {
                return Ok(false_pred());
            }
            Ok(Pred::Leaf(Cond {
                col: c.col.clone(),
                op: c.op,
                val: sv,
                set: vec![],
            }))
        }
        _ => Err("subquery returns more than one row".into()),
    }
}

/// 按 DFS 序消费 results, 子查询叶子 → Cond/恒真恒假子树.
pub(crate) fn fold_pred_subq(
    pred: &Pred<Cond>,
    it: &mut std::slice::Iter<Vec<Vec<ColValue>>>,
) -> Result<Pred<Cond>, String> {
    match pred {
        Pred::Leaf(c) => {
            if matches!(c.val, SqlValue::Subquery(_)) {
                let rows = it.next().ok_or("subquery result missing")?;
                fold_one_subq(c, rows)
            } else {
                Ok(Pred::Leaf(c.clone()))
            }
        }
        Pred::And(v) => Ok(Pred::And(
            v.iter()
                .map(|p| fold_pred_subq(p, it))
                .collect::<Result<_, _>>()?,
        )),
        Pred::Or(v) => Ok(Pred::Or(
            v.iter()
                .map(|p| fold_pred_subq(p, it))
                .collect::<Result<_, _>>()?,
        )),
        Pred::Not(b) => Ok(Pred::Not(Box::new(fold_pred_subq(b, it)?))),
    }
}
