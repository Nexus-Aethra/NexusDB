// ⭐ 解耦 2026-08: WHERE 条件/表达式解析 (从 parser.rs 拆出).
// 职责: WHERE 谓词树 (AND/OR/NOT/比较/IN) + 表达式 atom 求值 + 常量折叠.
use super::ast::*;
use super::parser::{P, Tok};
use super::parser_select::{
    is_join_ahead, parse_join_from, parse_opt_alias, parse_select, parse_select_tail,
};
use storage::schema::{ColType, Column, TableSchema};

pub(crate) fn parse_paren_subselect(p: &mut P) -> Result<Box<SqlStmt>, String> {
    p.expect(&Tok::LParen, "(")?;
    let inner = parse_select(p, false)?;
    p.expect(&Tok::RParen, ")")?;
    Ok(Box::new(inner))
}

/// ⭐ F72: 派生表叶子含子查询判定 (外层 WHERE 不允许嵌套子查询).
pub(crate) fn cond_has_subquery(c: &Cond) -> bool {
    matches!(c.val, SqlValue::Subquery(_))
        || c.set.iter().any(|v| matches!(v, SqlValue::Subquery(_)))
        || c.col == EXISTS_SENTINEL_COL
}

pub(crate) fn pred_has_subquery(pred: &Pred<Cond>) -> bool {
    pred.leaves().iter().any(|c| cond_has_subquery(c))
}

/// ⭐ F72: FROM 派生表 `(SELECT ...) [AS] alias [WHERE ...] [ORDER/LIMIT/OFFSET]`.
/// 外层投影 items 已在 FROM 前解完 (传入). v1: 无聚合投影; 无别名报错;
/// 外层 WHERE 不得含子查询 (双层编排留后).
pub(crate) fn parse_derived(p: &mut P, items: Vec<SelectItem>, top: bool) -> Result<SqlStmt, String> {
    let inner = parse_paren_subselect(p)?;
    let alias =
        parse_opt_alias(p).ok_or_else(|| "every derived table must have its own alias".to_string())?;
    // ⭐ F75: 派生表参与 JOIN — 别名后接 JOIN 子句 → 走 JOIN 主体 (from=派生表)
    if is_join_ahead(p) {
        let from = TableRef { table: alias.clone(), alias };
        return parse_join_from(p, items, from, Some(inner));
    }
    if items.iter().any(|i| matches!(i, SelectItem::Agg { .. })) {
        // v1 特判: 唯一投影项为 COUNT(*) 允许 (行数统计); 其余聚合拒
        let lone_count = items.len() == 1
            && matches!(&items[0], SelectItem::Agg { func: AggFn::Count, arg: None, .. });
        if !lone_count {
            return Err("aggregate on derived table is not supported (v1, except lone COUNT(*))".into());
        }
    }
    let conds = parse_where(p)?;
    if pred_has_subquery(&conds) {
        return Err("subquery in derived-table outer WHERE is not supported (v1)".into());
    }
    let (order, limit, offset, limit_param, offset_param) = parse_select_tail(p)?;
    p.done_if(top)?;
    Ok(SqlStmt::SelectDerived { inner, alias, items, conds, order, limit, offset, limit_param, offset_param })
}

/// WHERE 子句 (AND 平铺; caller 决定是否必带).
/// ⭐ S2: BETWEEN → Ge+Le, LIKE 'p%' → 前缀范围 (解析期 desugar);
/// IN → CmpOp::In (set); `!=`/`<>` → Ne.
pub(crate) fn parse_where(p: &mut P) -> Result<Pred<Cond>, String> {
    if p.try_kw("WHERE") {
        parse_or_expr(p)
    } else {
        Ok(Pred::And(Vec::new())) // 无 WHERE = 恒真
    }
}

/// ⭐ F69: OR 层 (最低优先级).
pub(crate) fn parse_or_expr(p: &mut P) -> Result<Pred<Cond>, String> {
    let mut terms = vec![parse_and_expr(p)?];
    while p.try_kw("OR") {
        terms.push(parse_and_expr(p)?);
    }
    Ok(if terms.len() == 1 { terms.pop().unwrap() } else { Pred::Or(terms) })
}

/// ⭐ F69: AND 层.
pub(crate) fn parse_and_expr(p: &mut P) -> Result<Pred<Cond>, String> {
    let mut terms = vec![parse_not_expr(p)?];
    while p.try_kw("AND") {
        terms.push(parse_not_expr(p)?);
    }
    Ok(if terms.len() == 1 { terms.pop().unwrap() } else { Pred::And(terms) })
}

/// ⭐ F69: NOT 层.
pub(crate) fn parse_not_expr(p: &mut P) -> Result<Pred<Cond>, String> {
    if p.try_kw("NOT") {
        Ok(Pred::Not(Box::new(parse_not_expr(p)?)))
    } else {
        parse_primary(p)
    }
}

/// ⭐ F69: primary = `( <or_expr> )` | EXISTS 子查询 | 单个比较叶子.
pub(crate) fn parse_primary(p: &mut P) -> Result<Pred<Cond>, String> {
    // ⭐ F71: EXISTS (SELECT ...) — 哨兵列名区分; NOT EXISTS 由 parse_not_expr 包 Pred::Not
    if matches!(p.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("EXISTS")) {
        p.next()?;
        let stmt = parse_paren_subselect(p)?;
        return Ok(Pred::Leaf(Cond {
            col: EXISTS_SENTINEL_COL.to_string(),
            op: CmpOp::Eq,
            val: SqlValue::Subquery(stmt),
            set: vec![],
        }));
    }
    if p.peek() == Some(&Tok::LParen) {
        p.next()?;
        let inner = parse_or_expr(p)?;
        p.expect(&Tok::RParen, ")")?;
        Ok(inner)
    } else {
        parse_where_atom(p)
    }
}

/// ⭐ F69: 单个比较谓词 `col op val / IN / BETWEEN / LIKE`.
/// BETWEEN/LIKE desugar 产物 (多条) 包为 `And(vec![Leaf,..])`; 单条 → `Leaf`.
pub(crate) fn parse_where_atom(p: &mut P) -> Result<Pred<Cond>, String> {
    // ⭐ P0-1: 常量比较短路 — `1=1`/`0=1`/`'a'<'b'` (无列引用) → 恒真 (空 AND) /
    // 恒假 (空 OR). 恒假由 dispatch 短路返回空; 恒真由 normalize 消除.
    if matches!(p.peek(), Some(Tok::Num(_)) | Some(Tok::Str(_))) {
        let lhs = p.value()?;
        let op = match p.next()? {
            Tok::Eq => CmpOp::Eq,
            Tok::Gt => CmpOp::Gt,
            Tok::Ge => CmpOp::Ge,
            Tok::Lt => CmpOp::Lt,
            Tok::Le => CmpOp::Le,
            Tok::Ne => CmpOp::Ne,
            other => {
                return Err(format!("expected comparison operator after constant, got {other:?}"))
            }
        };
        let rv = p.value()?;
        let rhs = fold_cond_arith(p, rv)?;
        let truthy = const_cmp(lhs, op, rhs)?;
        return Ok(if truthy { Pred::And(vec![]) } else { Pred::Or(vec![]) });
    }
    let mut conds: Vec<Cond> = Vec::new();
    let col = p.ident()?;
    // ⭐ F71: col [NOT] IN (...) — NOT IN 包 Pred::Not; 子查询与字面量列表两路
    let negated_in = p.try_kw("NOT");
    if p.try_kw("IN") {
        // ⭐ F71: IN (SELECT ...) → Subquery 占位 (dispatch 前折叠为 set)
        if p.peek_paren_select() {
            let stmt = parse_paren_subselect(p)?;
            let leaf = Pred::Leaf(Cond {
                col,
                op: CmpOp::In,
                val: SqlValue::Subquery(stmt),
                set: vec![],
            });
            return Ok(if negated_in { Pred::Not(Box::new(leaf)) } else { leaf });
        }
        // 字面量列表
        p.expect(&Tok::LParen, "(")?;
        let mut set = Vec::new();
        loop {
            let v = p.value()?;
            if v == SqlValue::Null {
                return Err("NULL is not valid in IN list".into());
            }
            set.push(v);
            match p.next()? {
                Tok::Comma => continue,
                Tok::RParen => break,
                other => return Err(format!("expected ',' or ')', got {other:?}")),
            }
        }
        if set.is_empty() {
            return Err("empty IN list".into());
        }
        sort_in_set(&mut set); // ⭐ F73: 大集合求值二分化
        let leaf = Pred::Leaf(Cond { col, op: CmpOp::In, val: SqlValue::Null, set });
        return Ok(if negated_in { Pred::Not(Box::new(leaf)) } else { leaf });
    } else if negated_in {
        return Err("expected IN after NOT".into());
    }
    if p.try_kw("BETWEEN") {
        // BETWEEN a AND b → col >= a AND col <= b (内部 AND 在此消费)
        let a = p.value()?;
        p.kw("AND")?;
        let b = p.value()?;
        if a == SqlValue::Null || b == SqlValue::Null {
            return Err("NULL is not a valid comparison bound".into());
        }
        conds.push(Cond { col: col.clone(), op: CmpOp::Ge, val: a, set: vec![] });
        conds.push(Cond { col, op: CmpOp::Le, val: b, set: vec![] });
    } else if p.try_kw("LIKE") {
        // 仅前缀模式 'p%' → [p, p+1) 字节范围 (与 starts_with 精确等价);
        // 无 '%' → 等值; 其它模式报错 (v1)
        let SqlValue::Str(pat) = p.value()? else {
            return Err("LIKE pattern must be a string".into());
        };
        let pct = pat.iter().position(|&b| b == b'%');
        match pct {
            None => {
                conds.push(Cond { col, op: CmpOp::Eq, val: SqlValue::Str(pat), set: vec![] });
            }
            Some(i) if i == pat.len() - 1 => {
                let prefix = pat[..i].to_vec();
                if prefix.is_empty() {
                    // LIKE '%' = 恒真, 不产条件
                } else {
                    conds.push(Cond {
                        col: col.clone(),
                        op: CmpOp::Ge,
                        val: SqlValue::Str(prefix.clone()),
                        set: vec![],
                    });
                    // 上界 = 前缀末个非 0xFF 字节 +1 截断; 全 0xFF → 无上界
                    let mut hi = prefix;
                    while hi.last() == Some(&0xFF) {
                        hi.pop();
                    }
                    if let Some(last) = hi.last_mut() {
                        *last += 1;
                        conds.push(Cond { col, op: CmpOp::Lt, val: SqlValue::Str(hi), set: vec![] });
                    }
                }
            }
            _ => return Err("LIKE supports only prefix patterns ('abc%')".into()),
        }
    } else if p.try_kw("IS") {
        // ⭐ compat: `col IS [NOT] NULL` — desugar 为 col = NULL / col <> NULL
        let not = p.try_kw("NOT");
        p.kw("NULL")?;
        let leaf = Pred::Leaf(Cond {
            col,
            op: if not { CmpOp::Ne } else { CmpOp::Eq },
            val: SqlValue::Null,
            set: vec![],
        });
        return Ok(leaf);
    } else {
        // ⭐ compat: `j ? 'key'` — 操作符位置 `?` → JSONB 存在 (值位置 `?` 仍为
        // prepared 占位符, 由 p.value() 处理). v1: 键须字面量 (Str/Int), 纯残余过滤.
        if p.peek() == Some(&Tok::Question) {
            p.next()?;
            let key = p.value()?;
            if key == SqlValue::Null || matches!(key, SqlValue::ColRef(_) | SqlValue::Subquery(_))
            {
                return Err("JSONB '?' key must be a literal".into());
            }
            conds.push(Cond { col, op: CmpOp::JsonExists, val: key, set: vec![] });
        } else {
            let op = match p.next()? {
                Tok::Eq => CmpOp::Eq,
                Tok::Gt => CmpOp::Gt,
                Tok::Ge => CmpOp::Ge,
                Tok::Lt => CmpOp::Lt,
                Tok::Le => CmpOp::Le,
                Tok::Ne => CmpOp::Ne,
                other => return Err(format!("expected comparison operator, got {other:?}")),
            };
            // ⭐ F71: col op (SELECT ...) — 标量子查询 (dispatch 前折叠为常量)
            if p.peek_paren_select() {
                let stmt = parse_paren_subselect(p)?;
                return Ok(Pred::Leaf(Cond { col, op, val: SqlValue::Subquery(stmt), set: vec![] }));
            }
            // ⭐ F74: col op ident (非 NULL) → ColRef (关联子查询相关列; decorrelate 前收集)
            // ⭐ F80: 但排除字面量前导关键字 (NULL/TRUE/FALSE 及 DATE|TIME|TIMESTAMP|DATETIME '...'),
            //   它们应落到 value() 解析为字面量而非列引用.
            if let Some(Tok::Ident(s)) = p.peek()
                && !matches!(
                    s.to_ascii_uppercase().as_str(),
                    "NULL" | "TRUE" | "FALSE" | "DATE" | "TIME" | "TIMESTAMP" | "DATETIME"
                )
            {
                let rhs = p.ident()?;
                return Ok(Pred::Leaf(Cond { col, op, val: SqlValue::ColRef(rhs), set: vec![] }));
            }
            // ⭐ P0-1: 字面量算术折叠 (`a = 1+2` → `a = 3`)
            let rv = p.value()?;
            let val = fold_cond_arith(p, rv)?;
            if val == SqlValue::Null {
                return Err("NULL is not a valid comparison bound".into());
            }
            conds.push(Cond { col, op, val, set: vec![] });
        }
    }
    // 单条 → Leaf; 多条 (BETWEEN/LIKE desugar) → And; 空 (LIKE '%') → 恒真
    Ok(match conds.len() {
        1 => Pred::Leaf(conds.pop().unwrap()),
        _ => Pred::And(conds.into_iter().map(Pred::Leaf).collect()),
    })
}

/// ⭐ P0-1: 折叠 cond 右值的字面量算术 (`a = 1+2` → `a = 3`). 仅数值;
/// 含列引用/字符串遇算术符报错 (v1).
pub(crate) fn fold_cond_arith(p: &mut P, first: SqlValue) -> Result<SqlValue, String> {
    let mut acc = first;
    loop {
        let op = match p.peek() {
            Some(Tok::Plus) => ArithOp::Add,
            Some(Tok::Minus) => ArithOp::Sub,
            Some(Tok::Star) => ArithOp::Mul,
            Some(Tok::Slash) => ArithOp::Div,
            _ => break,
        };
        p.next()?;
        let rhs = p.value()?;
        acc = eval_const_bin(op, acc, rhs)?;
    }
    Ok(acc)
}

/// ⭐ P0-1: 常量二元算术求值 (Int/Float; 溢出/除零/非数值报错).
pub(crate) fn eval_const_bin(op: ArithOp, l: SqlValue, r: SqlValue) -> Result<SqlValue, String> {
    use SqlValue::{Float, Int};
    match (l, r) {
        (Int(a), Int(b)) => {
            let v = match op {
                ArithOp::Add => a.checked_add(b),
                ArithOp::Sub => a.checked_sub(b),
                ArithOp::Mul => a.checked_mul(b),
                ArithOp::Div => a.checked_div(b),
            };
            v.map(Int)
                .ok_or_else(|| "integer overflow/div-by-zero in constant expression".into())
        }
        (Float(a), Float(b)) => Ok(Float(match op {
            ArithOp::Add => a + b,
            ArithOp::Sub => a - b,
            ArithOp::Mul => a * b,
            ArithOp::Div => a / b,
        })),
        (Float(a), Int(b)) => eval_const_bin(op, Float(a), Float(b as f64)),
        (Int(a), Float(b)) => eval_const_bin(op, Float(a as f64), Float(b)),
        _ => Err("constant arithmetic requires numeric operands".into()),
    }
}

/// ⭐ P0-1: 常量比较求值 (Int/Float/Str; 混合类型报错).
pub(crate) fn const_cmp(l: SqlValue, op: CmpOp, r: SqlValue) -> Result<bool, String> {
    use std::cmp::Ordering;
    use SqlValue::{Float, Int, Str};
    let ord = match (l, r) {
        (Int(a), Int(b)) => a.cmp(&b),
        (Float(a), Float(b)) => a
            .partial_cmp(&b)
            .ok_or_else(|| "constant comparison with NaN".to_string())?,
        (Float(a), Int(b)) => a
            .partial_cmp(&(b as f64))
            .ok_or_else(|| "constant comparison with NaN".to_string())?,
        (Int(a), Float(b)) => (a as f64)
            .partial_cmp(&b)
            .ok_or_else(|| "constant comparison with NaN".to_string())?,
        (Str(a), Str(b)) => a.cmp(&b),
        _ => return Err("constant comparison requires numeric/string operands".into()),
    };
    Ok(match op {
        CmpOp::Eq => ord == Ordering::Equal,
        CmpOp::Ne => ord != Ordering::Equal,
        CmpOp::Gt => ord == Ordering::Greater,
        CmpOp::Ge => ord != Ordering::Less,
        CmpOp::Lt => ord == Ordering::Less,
        CmpOp::Le => ord != Ordering::Greater,
        CmpOp::In => return Err("IN not valid in constant comparison".into()),
        CmpOp::JsonExists => return Err("JSONB '?' not valid in constant comparison".into()),
    })
}

/// ⭐ S1: `DELETE FROM t WHERE ...`
pub(crate) fn parse_delete(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("DELETE")?;
    p.kw("FROM")?;
    let table = p.table_ident()?;
    let conds = parse_where(p)?;
    p.done()?;
    Ok(SqlStmt::Delete { table, conds })
}

/// ⭐ S1: `UPDATE t SET c = v [, c2 = v2 ...] WHERE ...`
pub(crate) fn parse_update(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("UPDATE")?;
    let table = p.table_ident()?;
    p.kw("SET")?;
    let mut sets: Vec<(String, SqlValue)> = Vec::new();
    loop {
        let col = p.ident()?;
        p.expect(&Tok::Eq, "=")?;
        // ⭐ PG 兼容: SET 值 — 表达式 (`col+1` / `NOT col`) 或 单字面量/列引用
        let val = parse_update_set_value(p)?;
        sets.push((col, val));
        if p.peek() == Some(&Tok::Comma) {
            p.next()?;
        } else {
            break;
        }
    }
    let conds = parse_where(p)?;
    // ⭐ compat: 吞 RETURNING ... (v1 不返回受影响行值)
    if p.try_kw("RETURNING") {
        while !matches!(p.peek(), None) {
            p.i += 1;
        }
    }
    p.done()?;
    Ok(SqlStmt::Update { table, sets, conds })
}

/// ⭐ PG 兼容 (UPDATE SET): 解析 SET 右侧值 — 字面量 / 列引用 / 表达式
/// (`col+1` / `col-1` / `NOT col`). 表达式折叠成 `SqlValue::Expr(ScalarExpr)`.
pub(crate) fn parse_update_set_value(p: &mut P) -> Result<SqlValue, String> {
    use crate::protocol::sql::{ArithOp, ScalarExpr};
    // 解析一个"项" (字面量 / 列引用 / NOT 前缀)
    pub(crate) fn atom(p: &mut P) -> Result<ScalarExpr, String> {
        match p.peek().cloned() {
            Some(Tok::Ident(s)) => {
                let up = s.to_ascii_uppercase();
                match up.as_str() {
                    "NULL" => {
                        p.next()?;
                        Ok(ScalarExpr::Lit(SqlValue::Null))
                    }
                    "TRUE" => {
                        p.next()?;
                        Ok(ScalarExpr::Lit(SqlValue::Int(1)))
                    }
                    "FALSE" => {
                        p.next()?;
                        Ok(ScalarExpr::Lit(SqlValue::Int(0)))
                    }
                    "NOT" => {
                        p.next()?;
                        let e = atom(p)?;
                        Ok(ScalarExpr::Not(Box::new(e)))
                    }
                    _ => {
                        // ⭐ PG 兼容 (UPDATE SET): 函数调用 `NOW()` / `CURRENT_TIMESTAMP`
                        // 等 — 吞掉括号及参数, 解析为当前时间字面量 (时间列默认/更新).
                        // 其他未知函数同样吞掉调用, 回退 Null (避免 `SET c = fn()` 报
                        // "trailing tokens" 卡死后续多列 SET 解析).
                        if p.peek() == Some(&Tok::LParen) {
                            let fname = s.to_ascii_lowercase();
                            p.next()?; // (
                            let mut depth = 1;
                            while depth > 0 {
                                match p.next()? {
                                    Tok::LParen => depth += 1,
                                    Tok::RParen => depth -= 1,
                                    _ => {}
                                }
                            }
                            return match fname.as_str() {
                                "now" | "current_timestamp" | "current_date" | "current_time" => {
                                    Ok(ScalarExpr::Lit(SqlValue::Now))
                                }
                                _ => Ok(ScalarExpr::Lit(SqlValue::Null)),
                            };
                        }
                        // 列引用
                        p.next()?;
                        Ok(ScalarExpr::Col(s))
                    }
                }
            }
            Some(Tok::Num(_)) | Some(Tok::Str(_)) | Some(Tok::Minus) | Some(Tok::LParen) => {
                let v = p.value()?;
                Ok(ScalarExpr::Lit(v))
            }
            // ⭐ P1: 占位符 (MySQL `?` / PG `$n`) — 走 p.value() 产出 SqlValue::Param
            Some(Tok::Question) | Some(Tok::Dollar(_)) => {
                let v = p.value()?;
                Ok(ScalarExpr::Lit(v))
            }
            _ => Ok(ScalarExpr::Lit(SqlValue::Null)),
        }
    }
    let left = match atom(p) {
        Ok(e) => e,
        Err(_) => ScalarExpr::Lit(SqlValue::Null),
    };
    // 链式二元算术: 左结合, 支持 `a + b - c * d` (v1: 无优先级, 从左到右)
    let mut acc = left;
    let mut saw_op = false;
    while let Some(op) = match p.peek() {
        Some(Tok::Plus) => Some(ArithOp::Add),
        Some(Tok::Minus) => Some(ArithOp::Sub),
        Some(Tok::Star) => Some(ArithOp::Mul),
        Some(Tok::Slash) => Some(ArithOp::Div),
        _ => None,
    } {
        p.next()?;
        let Ok(right) = atom(p) else { break };
        acc = ScalarExpr::Bin { op, l: Box::new(acc), r: Box::new(right) };
        saw_op = true;
    }
    // 有算术 → 表达式; 无算术 → 折叠为原 SqlValue (列引用 / 字面量 / NOT)
    if saw_op {
        return Ok(SqlValue::Expr(Box::new(acc)));
    }
    Ok(match acc {
        ScalarExpr::Col(c) => SqlValue::ColRef(c),
        ScalarExpr::Lit(v) => v,
        ScalarExpr::Not(e) => SqlValue::Expr(Box::new(ScalarExpr::Not(e))),
        other => SqlValue::Expr(Box::new(other)),
    })
}
