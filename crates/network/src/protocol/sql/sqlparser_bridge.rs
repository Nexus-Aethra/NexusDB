// ⭐ 渐进式替换手写 parser (sqlparser-rs 前端 → NexusDB SqlStmt).
// 定位: 仅当 sqlparser-rs 能解析且映射层支持时, 产出 SqlStmt; 否则返回 Ok(None)
// 由手写 parser 兜底 (保证正确性). 当前覆盖单表 SELECT 的核心子集.
use super::ast::{self, AggFn, CmpOp, Cond, Pred, ScalarExpr, SelectItem, SqlStmt, SqlValue};
use sqlparser::ast as sp;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// 尝试用 sqlparser-rs 解析一条 SQL; 仅返回 Query(SELECT) 的映射结果 (含参数个数).
/// 非 SELECT / 解析失败 / 映射层不支持 → Ok(None) (调用方回退手写 parser).
pub fn parse_select(input: &[u8]) -> Result<Option<(SqlStmt, u16)>, String> {
    let text = std::str::from_utf8(input).map_err(|_| "statement is not valid UTF-8")?;
    let trimmed = text.trim_start();
    // 快速判断是否 SELECT (避免每条都走 sqlparser 全解析)
    if !trimmed
        .chars()
        .take(6)
        .collect::<String>()
        .to_ascii_uppercase()
        .starts_with("SELECT")
    {
        return Ok(None);
    }
    // ⭐ 回退手写: 含裸 `?` (MySQL 占位符 / JSONB `?` 操作符) — 手写支持而 bridge
    // 的 PG 方言语义不同; `?` 在字符串/`$$` 内不计数.
    if has_bare_question(trimmed) {
        return Ok(None);
    }
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, trimmed)
        .map_err(|e| format!("sqlparser: {e}"))?;
    let Some(stmt) = stmts.into_iter().next() else {
        return Ok(None);
    };
    let sp::Statement::Query(q) = stmt else {
        return Ok(None);
    };
    match map_query(&q)? {
        Some(stmt) => Ok(Some((stmt.clone(), param_count(&stmt)))),
        None => Ok(None),
    }
}

/// 字符串/`$$` 外是否含裸 `?` (MySQL 占位符或 JSONB 操作符). 有 → bridge 回退手写.
fn has_bare_question(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    let mut in_str = false;
    let mut in_dq = false;
    let mut in_dollar = false;
    while i < b.len() {
        match b[i] {
            b'\'' if !in_dq && !in_dollar => in_str = !in_str,
            b'"' if !in_str && !in_dollar => in_dq = !in_dq,
            b'$' if !in_str && !in_dq => {
                // $$ dollar-quote
                if i + 1 < b.len() && b[i + 1] == b'$' {
                    in_dollar = !in_dollar;
                    i += 2;
                    continue;
                }
            }
            b'?' if !in_str && !in_dq && !in_dollar => return true,
            _ => {}
        }
        i += 1;
    }
    false
}

/// 从 SqlStmt 计算参数个数 (最大 Param 索引 + 1; LIMIT/OFFSET 参数含内).
fn param_count(s: &SqlStmt) -> u16 {
    let mut max = 0u16;
    fn scan_val(v: &SqlValue, max: &mut u16) {
        if let SqlValue::Param(i) = v {
            if *i >= *max {
                *max = i + 1;
            }
        }
    }
    match s {
        SqlStmt::Select {
            conds,
            having,
            limit_param,
            offset_param,
            items,
            ..
        } => {
            for leaf in conds.leaves() {
                scan_val(&leaf.val, &mut max);
                for v in &leaf.set {
                    scan_val(v, &mut max);
                }
            }
            for leaf in having.leaves() {
                scan_val(&leaf.val, &mut max);
                for v in &leaf.set {
                    scan_val(v, &mut max);
                }
            }
            if let Some(i) = limit_param {
                if *i >= max {
                    max = *i + 1;
                }
            }
            if let Some(i) = offset_param {
                if *i >= max {
                    max = *i + 1;
                }
            }
            for it in items {
                if let SelectItem::Expr { expr, .. } = it {
                    if let ScalarExpr::Lit(SqlValue::Param(i)) = expr {
                        if *i >= max {
                            max = *i + 1;
                        }
                    }
                }
            }
        }
        _ => {}
    }
    max
}

/// Query → Some(SqlStmt::Select) (单表 SELECT); JOIN/子查询/不支持 → Ok(None) 回退.
fn map_query(q: &sp::Query) -> Result<Option<SqlStmt>, String> {
    let sp::SetExpr::Select(sel) = &*q.body else {
        return Ok(None);
    };
    if sel.from.len() != 1 {
        return Ok(None);
    }
    if sel.from.len() != 1 {
        return Ok(None);
    }
    let twj = &sel.from[0];
    let sp::TableFactor::Table { name, .. } = &twj.relation else {
        return Ok(None);
    };
    let table = name.to_string();

    // 投影
    let mut items = Vec::new();
    for pi in &sel.projection {
        match pi {
            // ⭐ 与手写一致: `*` 表示全列 → 不 push (空 items = 全列)
            sp::SelectItem::Wildcard(_) => {}
            sp::SelectItem::UnnamedExpr(e) => match map_projection_expr(e)? {
                Some(it) => items.push(it),
                None => return Ok(None),
            },
            sp::SelectItem::ExprWithAlias { expr, alias } => match map_projection_expr(expr)? {
                Some(it) => items.push(with_alias(it, &alias.value)),
                None => return Ok(None),
            },
            _ => return Ok(None),
        }
    }

    // WHERE (不支持 → 回退)
    let conds = match &sel.selection {
        Some(e) => match map_expr_to_conds(e)? {
            Some(p) => p,
            None => return Ok(None),
        },
        None => Pred::And(vec![]),
    };

    // ORDER BY (OrderByKind::Expressions)
    let mut order = Vec::new();
    if let Some(ob) = &q.order_by {
        let sp::OrderByKind::Expressions(exprs) = &ob.kind else {
            return Ok(None);
        };
        for o in exprs {
            let sp::Expr::Identifier(id) = &o.expr else {
                return Ok(None);
            };
            // ⭐ 与手写一致: (col, is_desc) — DESC → true
            let is_desc = !o.options.asc.unwrap_or(true);
            order.push((id.value.clone(), is_desc));
        }
    }

    // LIMIT / OFFSET (含参数 $n)
    let mut limit = None;
    let mut offset = None;
    let mut limit_param = None;
    let mut offset_param = None;
    if let Some(lc) = &q.limit_clause {
        let sp::LimitClause::LimitOffset { limit: l, offset: o, .. } = lc else {
            return Ok(None);
        };
        if let Some(l) = l {
            match &l {
                sp::Expr::Value(v) => match &v.value {
                    sp::Value::Number(n, _) => {
                        limit = Some(n.parse::<u32>().map_err(|_| "bad LIMIT".to_string())?)
                    }
                    sp::Value::Placeholder(p) => {
                        limit_param = Some(parse_placeholder(p)?);
                        limit = Some(0);
                    }
                    _ => return Ok(None),
                },
                _ => return Ok(None),
            }
        }
        if let Some(o) = o {
            match &o.value {
                sp::Expr::Value(v) => match &v.value {
                    sp::Value::Number(n, _) => {
                        offset = Some(n.parse::<u32>().map_err(|_| "bad OFFSET".to_string())?)
                    }
                    sp::Value::Placeholder(p) => {
                        offset_param = Some(parse_placeholder(p)?);
                        offset = Some(0);
                    }
                    _ => return Ok(None),
                },
                _ => return Ok(None),
            }
        }
    }

    // GROUP BY / HAVING
    let mut group_by = Vec::new();
    if let sp::GroupByExpr::Expressions(gexprs, _) = &sel.group_by {
        for g in gexprs {
            let sp::Expr::Identifier(id) = g else {
                return Ok(None);
            };
            group_by.push(id.value.clone());
        }
    } else if !matches!(sel.group_by, sp::GroupByExpr::Expressions(_, _)) {
        // All 等形态不支持 → 回退
        return Ok(None);
    }
    let having = match &sel.having {
        Some(e) => match map_expr_to_conds(e)? {
            Some(p) => p,
            None => return Ok(None),
        },
        None => Pred::And(vec![]),
    };

    Ok(Some(SqlStmt::Select {
        table,
        items,
        conds,
        limit,
        order,
        offset,
        group_by,
        having,
        limit_param,
        offset_param,
    }))
}

fn with_alias(it: SelectItem, alias: &str) -> SelectItem {
    match it {
        SelectItem::Col { name, .. } => SelectItem::Col {
            name,
            alias: Some(alias.to_string()),
        },
        SelectItem::Agg { func, arg, distinct, .. } => SelectItem::Agg {
            func,
            arg,
            distinct,
            alias: Some(alias.to_string()),
        },
        SelectItem::Expr { expr, .. } => SelectItem::Expr {
            expr,
            alias: Some(alias.to_string()),
        },
        other => other,
    }
}

/// 投影项表达式 → SelectItem (仅支持列/聚合; 不支持 → None)
fn map_projection_expr(e: &sp::Expr) -> Result<Option<SelectItem>, String> {
    match e {
        sp::Expr::Identifier(id) => Ok(Some(SelectItem::Col {
            name: id.value.clone(),
            alias: None,
        })),
        sp::Expr::CompoundIdentifier(parts) => {
            let name = parts
                .iter()
                .map(|p| p.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            Ok(Some(SelectItem::Col {
                name,
                alias: None,
            }))
        }
        sp::Expr::Function(f) => {
            let fname = f.name.to_string().to_ascii_uppercase();
            let func = match fname.as_str() {
                "COUNT" => AggFn::Count,
                "SUM" => AggFn::Sum,
                "AVG" => AggFn::Avg,
                "MIN" => AggFn::Min,
                "MAX" => AggFn::Max,
                _ => return Ok(None),
            };
            // 聚合参数 + DISTINCT
            let mut distinct = false;
            let arg = match &f.args {
                sp::FunctionArguments::None => None,
                sp::FunctionArguments::List(list) => {
                    if let Some(dt) = &list.duplicate_treatment {
                        if matches!(dt, sp::DuplicateTreatment::Distinct) {
                            distinct = true;
                        }
                    }
                    if list.args.is_empty() {
                        None
                    } else if list.args.len() == 1 {
                        match &list.args[0] {
                            // COUNT(*) → arg=None
                            sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Wildcard)
                            | sp::FunctionArg::Unnamed(sp::FunctionArgExpr::WildcardWithOptions(_)) => {
                                None
                            }
                            sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(a)) => match a {
                                sp::Expr::Identifier(id) => {
                                    Some(ScalarExpr::Col(id.value.clone()))
                                }
                                sp::Expr::CompoundIdentifier(parts) => Some(ScalarExpr::Col(
                                    parts.last().map(|p| p.value.clone()).unwrap_or_default(),
                                )),
                                _ => return Ok(None),
                            },
                            _ => return Ok(None),
                        }
                    } else {
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            };
            Ok(Some(SelectItem::Agg {
                func,
                arg,
                distinct,
                alias: None,
            }))
        }
        _ => Ok(None),
    }
}

/// WHERE/HAVING 表达式 → Some(Pred<Cond>); 不支持 → Ok(None) (调用方回退).
fn map_expr_to_conds(e: &sp::Expr) -> Result<Option<Pred<Cond>>, String> {
    match e {
        sp::Expr::BinaryOp { left, op, right } => match op {
            sp::BinaryOperator::And => {
                let (l, r) = match (map_expr_to_conds(left)?, map_expr_to_conds(right)?) {
                    (Some(l), Some(r)) => (l, r),
                    _ => return Ok(None),
                };
                Ok(Some(Pred::And(vec![l, r])))
            }
            sp::BinaryOperator::Or => {
                let (l, r) = match (map_expr_to_conds(left)?, map_expr_to_conds(right)?) {
                    (Some(l), Some(r)) => (l, r),
                    _ => return Ok(None),
                };
                Ok(Some(Pred::Or(vec![l, r])))
            }
            sp::BinaryOperator::Eq => map_cmp(left, CmpOp::Eq, right),
            sp::BinaryOperator::NotEq => map_cmp(left, CmpOp::Ne, right),
            sp::BinaryOperator::Gt => map_cmp(left, CmpOp::Gt, right),
            sp::BinaryOperator::GtEq => map_cmp(left, CmpOp::Ge, right),
            sp::BinaryOperator::Lt => map_cmp(left, CmpOp::Lt, right),
            sp::BinaryOperator::LtEq => map_cmp(left, CmpOp::Le, right),
            _ => Ok(None),
        },
        sp::Expr::UnaryOp { op: sp::UnaryOperator::Not, expr } => {
            let inner = map_expr_to_conds(expr)?;
            Ok(inner.map(|p| Pred::Not(Box::new(p))))
        }
        sp::Expr::InList {
            expr,
            list,
            negated: false,
            ..
        } => {
            let (col, _) = match expr_to_col(expr) {
                Ok(c) => c,
                Err(_) => return Ok(None),
            };
            let mut set = Vec::new();
            for it in list {
                match expr_to_sqlvalue(it)? {
                    Some(v) => set.push(v),
                    None => return Ok(None),
                }
            }
            Ok(Some(Pred::Leaf(Cond {
                col,
                op: CmpOp::In,
                val: SqlValue::Null,
                set,
            })))
        }
        _ => Ok(None),
    }
}

fn map_cmp(left: &sp::Expr, op: CmpOp, right: &sp::Expr) -> Result<Option<Pred<Cond>>, String> {
    // ⭐ 与手写一致: `1=1` / `true` 等常量比较 → 恒真 And([]) (不入 conds)
    if matches!(left, sp::Expr::Value(_)) && matches!(right, sp::Expr::Value(_)) {
        return Ok(Some(Pred::And(vec![])));
    }
    let (col, _) = match expr_to_col(left) {
        Ok(c) => c,
        Err(_) => return Ok(None),
    };
    let Some(val) = expr_to_sqlvalue(right)? else {
        return Ok(None);
    };
    // ⭐ 与手写一致: `col = NULL` 语义上应 IS NULL, 手写报错 → 回退
    if matches!(val, SqlValue::Null) {
        return Ok(None);
    }
    Ok(Some(Pred::Leaf(Cond {
        col,
        op,
        val,
        set: vec![],
    })))
}

/// 表达式 → 列名 (col / tbl.col). 非列 → Err.
fn expr_to_col(e: &sp::Expr) -> Result<(String, bool), String> {
    match e {
        sp::Expr::Identifier(id) => Ok((id.value.clone(), false)),
        sp::Expr::CompoundIdentifier(parts) => {
            let name = parts
                .iter()
                .map(|p| p.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            Ok((name, true))
        }
        _ => Err("expression is not a column reference".to_string()),
    }
}

/// 表达式 → SqlValue (值/参数). 非字面量 → Ok(None).
fn expr_to_sqlvalue(e: &sp::Expr) -> Result<Option<SqlValue>, String> {
    match e {
        sp::Expr::Value(v) => Ok(Some(map_value(&v.value))),
        sp::Expr::Identifier(id) => Ok(Some(SqlValue::Str(id.value.clone().into_bytes()))),
        _ => Ok(None),
    }
}

fn map_value(v: &sp::Value) -> SqlValue {
    match v {
        sp::Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                SqlValue::Int(i)
            } else {
                SqlValue::Float(n.parse().unwrap_or(0.0))
            }
        }
        sp::Value::SingleQuotedString(s) | sp::Value::DoubleQuotedString(s) => {
            SqlValue::Str(s.as_bytes().to_vec())
        }
        sp::Value::Boolean(b) => SqlValue::Int(*b as i64),
        sp::Value::Null => SqlValue::Null,
        sp::Value::Placeholder(p) => SqlValue::Param(parse_placeholder(p).unwrap_or(0)),
        _ => SqlValue::Null,
    }
}

/// `$1` → 0 (0-based 参数索引)
fn parse_placeholder(p: &str) -> Result<u16, String> {
    let n = p
        .trim_start_matches('$')
        .parse::<u16>()
        .map_err(|_| format!("bad placeholder {p}"))?;
    Ok(n.saturating_sub(1))
}
