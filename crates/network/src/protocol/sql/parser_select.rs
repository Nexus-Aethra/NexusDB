// ⭐ 解耦 2026-08: SELECT 查询解析 (从 parser.rs 拆出).
// 职责: 单表 SELECT / 派生表 / JOIN / SHOW / 系统表查询解析.
use super::ast::*;
use super::parser::{P, Tok, parse_scalar_expr};
use super::parser_where::{parse_derived, parse_paren_subselect, parse_where};

pub(crate) fn split_system_table(name: &str) -> Option<(String, String)> {
    let (cat, tbl) = name.split_once('.')?;
    let cat_l = cat.to_ascii_lowercase();
    if cat_l == "information_schema" || cat_l == "pg_catalog" {
        Some((cat_l, tbl.to_ascii_lowercase()))
    } else {
        None
    }
}

/// ⭐ F66: 解 SELECT 尾部 ORDER BY / LIMIT / OFFSET (系统表与普通表共用子集).
#[allow(clippy::type_complexity)]
pub(crate) fn parse_select_tail(
    p: &mut P,
) -> Result<
    (
        Vec<(String, bool)>,
        Option<u32>,
        Option<u32>,
        Option<u16>,
        Option<u16>,
    ),
    String,
> {
    let mut order: Vec<(String, bool)> = Vec::new();
    if p.try_kw("ORDER") {
        p.kw("BY")?;
        loop {
            let col = p.ident()?;
            let desc = if p.try_kw("DESC") {
                true
            } else {
                p.try_kw("ASC");
                false
            };
            order.push((col, desc));
            if p.peek() == Some(&Tok::Comma) {
                p.next()?;
            } else {
                break;
            }
        }
    }
    let mut limit = None;
    let mut offset = None;
    let mut limit_param = None; // ⭐ PG 兼容: LIMIT $n → 参数索引 (bind 时填)
    let mut offset_param = None; // ⭐ PG 兼容: OFFSET $n → 参数索引
    if p.try_kw("LIMIT") {
        match p.next()? {
            Tok::Num(n) => {
                let a = n.parse::<u32>().map_err(|_| format!("bad LIMIT {n}"))?;
                // ⭐ F76: MySQL `LIMIT offset, count` 逗号形态
                if p.peek() == Some(&Tok::Comma) {
                    p.next()?;
                    match p.next()? {
                        Tok::Num(m) => {
                            offset = Some(a);
                            limit = Some(m.parse::<u32>().map_err(|_| format!("bad LIMIT {m}"))?);
                        }
                        Tok::Dollar(i) => {
                            offset = Some(a);
                            limit = Some(0);
                            limit_param = Some(i - 1); // 0-based 索引 (与 SqlValue::Param 一致)
                            p.next_param = p.next_param.max(i);
                        }
                        other => return Err(format!("expected LIMIT count, got {other:?}")),
                    }
                } else {
                    limit = Some(a);
                }
            }
            Tok::Dollar(i) => {
                p.next_param = p.next_param.max(i);
                limit = Some(0); // 占位, bind_params 用参数值填
                limit_param = Some(i - 1); // 0-based
            }
            other => return Err(format!("expected LIMIT count, got {other:?}")),
        }
    }
    if p.try_kw("OFFSET") {
        match p.next()? {
            Tok::Num(n) => offset = Some(n.parse::<u32>().map_err(|_| format!("bad OFFSET {n}"))?),
            Tok::Dollar(i) => {
                p.next_param = p.next_param.max(i);
                offset = Some(0); // 占位
                offset_param = Some(i - 1); // 0-based
            }
            other => return Err(format!("expected OFFSET count, got {other:?}")),
        }
    }
    Ok((order, limit, offset, limit_param, offset_param))
}

/// ⭐ F66: SHOW [FULL] TABLES [FROM db] / SHOW [FULL] COLUMNS FROM t [FROM db]
/// / SHOW DATABASES|SCHEMAS — MySQL 反射 (SQLAlchemy 方言走此路).
/// 复用 SystemQuery, catalog="__show__", table 编码具体类型.
pub(crate) fn parse_show(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("SHOW")?;
    let full = p.try_kw("FULL");
    let mk = |table: &str, conds: Pred<Cond>| SqlStmt::SystemQuery {
        catalog: "__show__".to_string(),
        table: table.to_string(),
        cols: Vec::new(),
        conds,
        order: Vec::new(),
        limit: None,
        offset: None,
        limit_param: None,
        offset_param: None,
    };
    // 内部标记 __table__ = 单叶子谓词
    let table_leaf = |table: String| {
        Pred::Leaf(Cond {
            col: "__table__".to_string(),
            op: CmpOp::Eq,
            val: SqlValue::Str(table.into_bytes()),
            set: Vec::new(),
        })
    };
    if p.try_kw("TABLES") {
        // [FROM|IN db] 忽略库名 (仅 current_db); 尾部可有 FROM db
        if p.try_kw("FROM") || p.try_kw("IN") {
            let _ = p.ident()?;
        }
        p.done()?;
        Ok(mk(if full { "full_tables" } else { "tables" }, Pred::And(Vec::new())))
    } else if p.try_kw("COLUMNS") || p.try_kw("FIELDS") {
        // FROM|IN t [FROM|IN db]
        if !(p.try_kw("FROM") || p.try_kw("IN")) {
            return Err("expected FROM after SHOW COLUMNS".into());
        }
        let table = p.table_ident()?;
        if p.try_kw("FROM") || p.try_kw("IN") {
            let _ = p.ident()?;
        }
        p.done()?;
        Ok(mk(if full { "full_columns" } else { "columns" }, table_leaf(table)))
    } else if p.try_kw("DATABASES") || p.try_kw("SCHEMAS") {
        p.done()?;
        Ok(mk("databases", Pred::And(Vec::new())))
    } else if p.try_kw("CREATE") {
        // SHOW CREATE TABLE t — SQLAlchemy MySQL 方言从 DDL 解析列
        p.kw("TABLE")?;
        let table = p.table_ident()?;
        p.done()?;
        Ok(mk("create_table", table_leaf(table)))
    } else {
        // 其他 SHOW (STATUS/VARIABLES/…) → 空结果 stub (工具探测容错)
        // 吞剩余 token
        while p.peek().is_some() {
            p.i += 1;
        }
        Ok(mk("__empty__", Pred::And(Vec::new())))
    }
}

/// ⭐ F67/F68 (JOIN): 判断左表名后是否跟着 JOIN (未来 3 token 内有 join 关键字).
/// (不消费; 覆盖 `t JOIN` / `t a JOIN` / `t AS a JOIN` 三种形态)
pub(crate) fn is_join_kw(t: Option<&Tok>) -> bool {
    matches!(t, Some(Tok::Ident(s))
        if s.eq_ignore_ascii_case("JOIN")
            || s.eq_ignore_ascii_case("INNER")
            || s.eq_ignore_ascii_case("LEFT")
            || s.eq_ignore_ascii_case("RIGHT")
            || s.eq_ignore_ascii_case("FULL")
            || s.eq_ignore_ascii_case("CROSS"))
}

pub(crate) fn is_join_ahead(p: &P) -> bool {
    // ⭐ F75: 扫描未来 3 token, 但遇 RParen (子查询边界) 即停 —
    // 防止内层 `(SELECT .. FROM u)` 误视外层 `) t JOIN` 为自身 JOIN.
    for off in 0..3 {
        match p.toks.get(p.i + off) {
            Some(Tok::RParen) => return false,
            t if is_join_kw(t) => return true,
            _ => {}
        }
    }
    false
}

/// ⭐ F67 (JOIN): 可选表别名 — `[AS] alias`; alias 不能是保留子句关键字.
pub(crate) fn parse_opt_alias(p: &mut P) -> Option<String> {
    if p.try_kw("AS") {
        return p.ident().ok();
    }
    if let Some(Tok::Ident(s)) = p.peek() {
        let up = s.to_ascii_uppercase();
        let reserved = matches!(
            up.as_str(),
            "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "OUTER" | "CROSS"
                | "ON" | "WHERE" | "ORDER" | "LIMIT" | "OFFSET" | "GROUP" | "HAVING" | "USING"
        );
        if !reserved {
            let a = s.clone();
            p.i += 1;
            return Some(a);
        }
    }
    None
}

/// ⭐ F76: 投影列输出别名 — `[AS] alias` (仅非保留字; FROM/子句关键字不当别名).
pub(crate) fn parse_col_alias(p: &mut P) -> Option<String> {
    if p.try_kw("AS") {
        return p.ident().ok();
    }
    if let Some(Tok::Ident(s)) = p.peek() {
        let up = s.to_ascii_uppercase();
        let reserved = matches!(
            up.as_str(),
            "FROM" | "AS" | "WHERE" | "ORDER" | "GROUP" | "HAVING" | "LIMIT" | "OFFSET"
                | "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "CROSS" | "ON" | "USING"
        );
        if !reserved {
            let a = s.clone();
            p.i += 1;
            return Some(a);
        }
    }
    None
}

/// ⭐ F68 (JOIN): 试解下一个 JOIN 种类 (已消费到 JOIN 关键字); 无则 None.
pub(crate) fn parse_join_kind(p: &mut P) -> Option<JoinKind> {
    if p.try_kw("CROSS") {
        let _ = p.kw("JOIN");
        Some(JoinKind::Cross)
    } else if p.try_kw("LEFT") {
        let _ = p.try_kw("OUTER");
        let _ = p.kw("JOIN");
        Some(JoinKind::Left)
    } else if p.try_kw("RIGHT") {
        let _ = p.try_kw("OUTER");
        let _ = p.kw("JOIN");
        Some(JoinKind::Right)
    } else if p.try_kw("FULL") {
        let _ = p.try_kw("OUTER");
        let _ = p.kw("JOIN");
        Some(JoinKind::Full)
    } else if p.try_kw("INNER") {
        let _ = p.kw("JOIN");
        Some(JoinKind::Inner)
    } else if p.try_kw("JOIN") {
        Some(JoinKind::Inner)
    } else {
        None
    }
}

/// ⭐ F68 (JOIN): 解 ON 谓词链 (AND 连接的 col op col; = → Eq, 其余 → Cmp).
pub(crate) fn parse_on(p: &mut P) -> Result<Vec<OnPred>, String> {
    let mut preds = Vec::new();
    loop {
        let left = QualCol::parse(&p.ident()?);
        let op = p.cmp_op()?;
        let right = QualCol::parse(&p.ident()?);
        if op == CmpOp::Eq {
            preds.push(OnPred::Eq(left, right));
        } else {
            preds.push(OnPred::Cmp { left, op, right });
        }
        if !p.try_kw("AND") {
            break;
        }
    }
    Ok(preds)
}

/// ⭐ F67/F68 (JOIN): `from [a] { [INNER|LEFT|RIGHT|FULL [OUTER]|CROSS] JOIN t [b]
/// (ON <conj> | USING (c,...)) }+ [WHERE ...] [ORDER BY ...] [LIMIT/OFFSET]`.
/// sel_items/first_table 已由 parse_select 消费.
pub(crate) fn parse_join(
    p: &mut P,
    sel_items: Vec<SelectItem>,
    first_table: String,
) -> Result<SqlStmt, String> {
    let first_alias = parse_opt_alias(p).unwrap_or_else(|| first_table.clone());
    let from = TableRef { table: first_table, alias: first_alias };
    parse_join_from(p, sel_items, from, None)
}

/// ⭐ F75: JOIN 主体 (from 已解析). from_inner=Some 时 from 为派生表.
pub(crate) fn parse_join_from(
    p: &mut P,
    sel_items: Vec<SelectItem>,
    from: TableRef,
    from_inner: Option<Box<SqlStmt>>,
) -> Result<SqlStmt, String> {
    let mut joins: Vec<JoinClause> = Vec::new();
    while let Some(kind) = parse_join_kind(p) {
        // ⭐ F75: JOIN 右侧派生表 v1 拒 (仅 FROM 位支持)
        if p.peek_paren_select() {
            return Err("derived table on JOIN right side is not supported (v1)".into());
        }
        let table = p.table_ident()?;
        let alias = parse_opt_alias(p).unwrap_or_else(|| table.clone());
        let on = if kind == JoinKind::Cross {
            Vec::new()
        } else if p.try_kw("USING") {
            // USING (c[,c]) → Eq(未限定 c, 右.c); 左侧限定由 worker 解析
            p.expect(&Tok::LParen, "(")?;
            let mut preds = Vec::new();
            loop {
                let c = p.ident()?;
                preds.push(OnPred::Eq(
                    QualCol { qualifier: None, col: c.clone() },
                    QualCol { qualifier: Some(alias.clone()), col: c },
                ));
                match p.next()? {
                    Tok::Comma => continue,
                    Tok::RParen => break,
                    other => return Err(format!("expected ',' or ')' in USING, got {other:?}")),
                }
            }
            preds
        } else {
            p.kw("ON")?;
            let preds = parse_on(p)?;
            if !preds.iter().any(|pr| matches!(pr, OnPred::Eq(..))) {
                return Err("JOIN ON requires at least one equality (col = col)".into());
            }
            preds
        };
        joins.push(JoinClause { kind, table: TableRef { table, alias }, on });
    }
    // WHERE / ORDER / LIMIT / OFFSET 复用单表解析后把列名转限定名
    let conds_raw = parse_where(p)?;
    let (order_raw, limit, offset, limit_param, offset_param) = parse_select_tail(p)?;
    p.done()?;
    let items: Vec<JoinItem> = sel_items
        .iter()
        .map(|it| match it {
            SelectItem::Col { name, .. } => Ok(JoinItem::Col(QualCol::parse(name))),
            SelectItem::Agg { .. } => {
                Err("aggregate functions are not supported in JOIN queries".to_string())
            }
            SelectItem::ScalarFn { .. } => {
                Err("scalar functions are not supported in JOIN queries".to_string())
            }
            SelectItem::Expr { .. } => {
                Err("expression projections are not supported in JOIN queries (v1)".to_string())
            }
        })
        .collect::<Result<_, _>>()?;
    let conds = conds_raw.map(&|c: &Cond| JoinCond {
        col: QualCol::parse(&c.col),
        op: c.op,
        val: c.val.clone(),
        set: c.set.clone(),
    });
    let order = order_raw.into_iter().map(|(s, d)| (QualCol::parse(&s), d)).collect();
    Ok(SqlStmt::SelectJoin { from, from_inner, joins, items, conds, order, limit, offset, limit_param, offset_param })
}

/// ⭐ F69: HAVING 谓词树 (OR<AND<NOT<primary; 叶子 = 输出列 label op val).
pub(crate) fn parse_having_or(p: &mut P) -> Result<Pred<Cond>, String> {
    let mut terms = vec![parse_having_and(p)?];
    while p.try_kw("OR") {
        terms.push(parse_having_and(p)?);
    }
    Ok(if terms.len() == 1 { terms.pop().unwrap() } else { Pred::Or(terms) })
}

pub(crate) fn parse_having_and(p: &mut P) -> Result<Pred<Cond>, String> {
    let mut terms = vec![parse_having_not(p)?];
    while p.try_kw("AND") {
        terms.push(parse_having_not(p)?);
    }
    Ok(if terms.len() == 1 { terms.pop().unwrap() } else { Pred::And(terms) })
}

pub(crate) fn parse_having_not(p: &mut P) -> Result<Pred<Cond>, String> {
    if p.try_kw("NOT") {
        Ok(Pred::Not(Box::new(parse_having_not(p)?)))
    } else if p.peek() == Some(&Tok::LParen) {
        p.next()?;
        let inner = parse_having_or(p)?;
        p.expect(&Tok::RParen, ")")?;
        Ok(inner)
    } else {
        // 叶子: label [聚合 (形态)] op val
        let mut label = p.ident()?;
        if p.peek() == Some(&Tok::LParen) {
            p.next()?;
            label = label.to_ascii_uppercase();
            label.push('(');
            if p.peek() == Some(&Tok::Star) {
                p.next()?;
                label.push('*');
            } else {
                label.push_str(&p.ident()?);
            }
            p.expect(&Tok::RParen, ")")?;
            label.push(')');
        }
        let op = p.cmp_op()?;
        let val = p.value()?;
        Ok(Pred::Leaf(Cond { col: label, op, val, set: Vec::new() }))
    }
}

/// `SELECT * | COUNT(*) | c1, c2, ... FROM t [WHERE ...] [ORDER BY c [DESC], ...]
/// [LIMIT n] [OFFSET m]`. ⭐ F71: top=false 为子查询上下文 (不调 done, 不走 stub).
pub(crate) fn parse_select(p: &mut P, top: bool) -> Result<SqlStmt, String> {
    p.kw("SELECT")?;
    // ⭐ F77: SELECT DISTINCT — 在投影前捕获; 后续 desugar 成 GROUP BY 全投影列.
    let distinct = p.try_kw("DISTINCT");
    // ⭐ O1: 投影列表 (Star = 全列); ⭐ G1 (F63): 列/聚合函数混合项
    let mut items: Vec<SelectItem> = Vec::new();
    if p.peek() == Some(&Tok::Star) {
        p.next()?;
    } else if !top && matches!(p.peek(), Some(Tok::Num(_))) {
        // ⭐ F71: 子查询中的字面量投影 (如 EXISTS 的 `SELECT 1`) — 值无关, 视为全列
        p.next()?;
    } else if top && matches!(p.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("VERSION"))
        && p.peek2_is_lparen()
    {
        // ⭐ S3: SELECT version() — psql/驱动探测 stub (仅当 `version(` 是函数调用;
        // `SELECT version FROM t` 中 version 是普通列名, 走常规投影)
        p.next()?;
        p.expect(&Tok::LParen, "(")?;
        p.expect(&Tok::RParen, ")")?;
        p.done()?;
        return Ok(SqlStmt::VersionStub);
    } else if top && matches!(p.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("DATABASE"))
        && p.peek2_is_lparen()
    {
        // ⭐ S5: SELECT DATABASE() — mysql cli USE 后探测
        p.next()?;
        p.expect(&Tok::LParen, "(")?;
        p.expect(&Tok::RParen, ")")?;
        p.done()?;
        return Ok(SqlStmt::DatabaseStub);
    } else if matches!(p.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("FROM")) {
        // ⭐ PG 兼容: 空投影 `SELECT FROM t` — 等价 SELECT * FROM t (migrator 探表)
    } else {
        loop {
            let name = p.ident()?;
            // ⭐ PG 兼容: SELECT EXISTS (SELECT ...) — 标量布尔探测 (migrator 建库/探表)
            if name.eq_ignore_ascii_case("EXISTS") && p.peek_paren_select() {
                let inner = parse_paren_subselect(p)?;
                p.done()?;
                return Ok(SqlStmt::ExistsStub { inner });
            }
            // ⭐ G1: ident( → 聚合函数 COUNT/SUM/AVG/MIN/MAX
            if p.peek() == Some(&Tok::LParen) {
                // ⭐ compat: 标量函数 (NOW()/CURRENT_TIMESTAMP) → ScalarFn (投影常量)
                if matches!(
                    name.to_ascii_uppercase().as_str(),
                    "NOW" | "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME"
                ) {
                    p.next()?; // (
                    let mut depth = 1;
                    while depth > 0 {
                        match p.next()? {
                            Tok::LParen => depth += 1,
                            Tok::RParen => depth -= 1,
                            _ => {}
                        }
                    }
                    items.push(SelectItem::ScalarFn { name: name.to_ascii_lowercase() });
                    break;
                }
                let func = match name.to_ascii_uppercase().as_str() {
                    "COUNT" => AggFn::Count,
                    "SUM" => AggFn::Sum,
                    "AVG" => AggFn::Avg,
                    "MIN" => AggFn::Min,
                    "MAX" => AggFn::Max,
                    other => return Err(format!("unknown function '{other}'")),
                };
                p.next()?; // (
                // ⭐ F77: COUNT(DISTINCT ...) — DISTINCT 仅 COUNT
                let distinct = p.try_kw("DISTINCT");
                if distinct && func != AggFn::Count {
                    return Err("DISTINCT is only supported in COUNT (v1)".into());
                }
                let arg = if p.peek() == Some(&Tok::Star) {
                    if func != AggFn::Count {
                        return Err(format!("{name}(*) is not valid (only COUNT(*))"));
                    }
                    if distinct {
                        return Err("COUNT(DISTINCT *) is not valid".into());
                    }
                    p.next()?;
                    None
                } else {
                    // ⭐ F78: 聚合内标量表达式 (裸列退化为 ScalarExpr::Col)
                    let e = parse_scalar_expr(p)?;
                    // ⭐ F77: DISTINCT 仅允许单裸列
                    if distinct && e.as_col().is_none() {
                        return Err("COUNT(DISTINCT ...) requires a single column (v1)".into());
                    }
                    Some(e)
                };
                p.expect(&Tok::RParen, ")")?;
                let alias = parse_col_alias(p);
                items.push(SelectItem::Agg { func, arg, distinct, alias });
            } else if matches!(p.peek(), Some(Tok::Arrow | Tok::ArrowText)) {
                // ⭐ compat: JSONB 操作符 j->'a' / j->>'a' (v1: 列 + 字面量键, 可链式)
                let mut expr = ScalarExpr::Col(name);
                loop {
                    let as_text = match p.peek() {
                        Some(Tok::Arrow) => false,
                        Some(Tok::ArrowText) => true,
                        _ => break,
                    };
                    p.next()?;
                    let key = p.value()?;
                    expr = ScalarExpr::JsonGet {
                        base: Box::new(expr),
                        key: Box::new(ScalarExpr::Lit(key)),
                        as_text,
                    };
                }
                let alias = parse_col_alias(p);
                items.push(SelectItem::Expr { expr, alias });
            } else {
                let alias = parse_col_alias(p);
                items.push(SelectItem::Col { name, alias });
            }
            if p.peek() == Some(&Tok::Comma) {
                p.next()?;
            } else {
                break;
            }
        }
    }
    if !matches!(p.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("FROM")) {
        // ⭐ compat: 无 FROM 的标量函数投影 (SELECT NOW()/CURRENT_TIMESTAMP) — 常量单行
        if items.iter().all(|i| matches!(i, SelectItem::ScalarFn { .. })) && !items.is_empty() {
            p.done()?;
            return Ok(SqlStmt::ScalarSelect { items });
        }
    }
    p.kw("FROM")?;
    // ⭐ F77: DISTINCT 仅支持单表命名列投影; 派生表/JOIN/系统表 拒
    if distinct && p.peek_paren_select() {
        return Err("DISTINCT with a derived table is not supported (v1)".into());
    }
    // ⭐ F72: FROM 派生表 `(SELECT ...) alias` — items (外层投影) 已解完, 传入.
    if p.peek_paren_select() {
        return parse_derived(p, items, top);
    }
    let table = p.ident()?;
    // ⭐ PG 兼容: 裸名 pg_* 系统表 → 映射 pg_catalog.X (PG search_path 默认含 pg_catalog)
    let table = if !table.contains('.')
        && matches!(
            table.to_ascii_lowercase().as_str(),
            "pg_database"
                | "pg_namespace"
                | "pg_class"
                | "pg_attribute"
                | "pg_tables"
                | "pg_indexes"
                | "pg_views"
                | "pg_settings"
        ) {
        format!("pg_catalog.{table}")
    } else {
        table
    };
    // ⭐ F66: 系统表拦截 — `information_schema.X` / `pg_catalog.X` (大小写不敏)
    // 走虚拟表合成路径; 尾部只解 WHERE/ORDER/LIMIT/OFFSET (不支持 GROUP/HAVING)
    if let Some((cat, tbl)) = split_system_table(&table) {
        if distinct {
            return Err("DISTINCT on system tables is not supported (v1)".into());
        }
        let conds = parse_where(p)?;
        let (order, limit, offset, limit_param, offset_param) = parse_select_tail(p)?;
        if top {
            p.done()?;
        }
        let cols: Vec<String> = items
            .iter()
            .filter_map(|i| match i {
                SelectItem::Col { name, .. } => Some(name.clone()),
                SelectItem::Agg { .. } => None,
                SelectItem::ScalarFn { .. } => None,
                SelectItem::Expr { .. } => None,
            })
            .collect();
        return Ok(SqlStmt::SystemQuery {
            catalog: cat,
            table: tbl,
            cols,
            conds,
            order,
            limit,
            offset,
            limit_param,
            offset_param,
        });
    }
    // ⭐ F76: 非系统表 → 剥 db 限定前缀 (`default.t` → `t`); 系统表已在上方按全名分派
    let table = strip_db_qual(table);
    // ⭐ F67 (JOIN): 表名后 3 token 内出现 JOIN/INNER/LEFT → 转 JOIN 解析
    if is_join_ahead(p) {
        if distinct {
            return Err("DISTINCT with JOIN is not supported (v1)".into());
        }
        return parse_join(p, items, table);
    }
    let conds = parse_where(p)?;
    // ⭐ G1 (F63): GROUP BY col [, col]
    let mut group_by: Vec<String> = Vec::new();
    if p.try_kw("GROUP") {
        p.kw("BY")?;
        loop {
            group_by.push(p.ident()?);
            if p.peek() == Some(&Tok::Comma) {
                p.next()?;
            } else {
                break;
            }
        }
    }
    // ⭐ F77: SELECT DISTINCT desugar → GROUP BY 全投影列 (复用分桶去重路径)
    if distinct {
        if items.iter().any(|i| matches!(i, SelectItem::Agg { .. })) {
            return Err("DISTINCT with aggregate is not supported (v1)".into());
        }
        if !group_by.is_empty() {
            return Err("DISTINCT with GROUP BY is not supported (v1)".into());
        }
        if items.is_empty() {
            return Err("SELECT DISTINCT * is not supported (v1); list columns explicitly".into());
        }
        group_by = items
            .iter()
            .filter_map(|i| match i {
                SelectItem::Col { name, .. } => Some(name.clone()),
                SelectItem::Agg { .. } => None,
                SelectItem::ScalarFn { .. } => None,
                SelectItem::Expr { .. } => None,
            })
            .collect();
    }
    // ⭐ G1 (F63): HAVING — 条件列写聚合原文 (如 SUM(x)) 或 group 列名,
    // 与输出列 label 同规则匹配 (大写归一). ⭐ F69: 支持 OR/NOT/括号.
    let having: Pred<Cond> = if p.try_kw("HAVING") {
        parse_having_or(p)?
    } else {
        Pred::And(Vec::new())
    };
    let has_having = !having.is_true();
    // ⭐ G1 校验: 有 group_by 时非聚合项必须 ∈ group_by (PG 语义);
    // 有聚合项时 * 投影 (items 空) 非法由 worker 拒 (需 schema 不在此层)
    if !group_by.is_empty() {
        for it in &items {
            if let SelectItem::Col { name: c, .. } = it
                && !group_by.iter().any(|g| g.eq_ignore_ascii_case(c))
            {
                return Err(format!(
                    "column '{c}' must appear in the GROUP BY clause or be used in an aggregate function"
                ));
            }
        }
        if items.is_empty() {
            return Err("SELECT * is not valid with GROUP BY".into());
        }
    }
    if has_having && !items.iter().any(|i| matches!(i, SelectItem::Agg { .. }))
        && group_by.is_empty()
    {
        return Err("HAVING requires GROUP BY or aggregate function".into());
    }
    // ⭐ S2: ORDER BY c [ASC|DESC] [, ...]; ⭐ G1: 也允许聚合形态 (SUM(x))
    let mut order: Vec<(String, bool)> = Vec::new();
    if p.try_kw("ORDER") {
        p.kw("BY")?;
        loop {
            let mut col = p.ident()?;
            if p.peek() == Some(&Tok::LParen) {
                // 聚合 label (与输出列/HAVING 同规则: 大写函数名 + 原列名)
                p.next()?;
                col = col.to_ascii_uppercase();
                col.push('(');
                if p.peek() == Some(&Tok::Star) {
                    p.next()?;
                    col.push('*');
                } else {
                    col.push_str(&p.ident()?);
                }
                p.expect(&Tok::RParen, ")")?;
                col.push(')');
            }
            let desc = if p.try_kw("DESC") {
                true
            } else {
                p.try_kw("ASC");
                false
            };
            order.push((col, desc));
            if p.peek() == Some(&Tok::Comma) {
                p.next()?;
            } else {
                break;
            }
        }
    }
    let mut limit = None;
    let mut offset = None;
    let mut limit_param = None; // ⭐ PG 兼容: LIMIT $n
    let mut offset_param = None; // ⭐ PG 兼容: OFFSET $n
    if p.try_kw("LIMIT") {
        match p.next()? {
            Tok::Num(n) => {
                let a = n.parse::<u32>().map_err(|_| format!("bad LIMIT {n}"))?;
                // ⭐ F76: MySQL `LIMIT offset, count` 逗号形态
                if p.peek() == Some(&Tok::Comma) {
                    p.next()?;
                    match p.next()? {
                        Tok::Num(m) => {
                            offset = Some(a);
                            limit = Some(m.parse::<u32>().map_err(|_| format!("bad LIMIT {m}"))?);
                        }
                        Tok::Dollar(i) => {
                            offset = Some(a);
                            limit = Some(0);
                            limit_param = Some(i - 1); // 0-based 索引 (与 SqlValue::Param 一致)
                            p.next_param = p.next_param.max(i);
                        }
                        other => return Err(format!("expected LIMIT count, got {other:?}")),
                    }
                } else {
                    limit = Some(a);
                }
            }
            Tok::Dollar(i) => {
                p.next_param = p.next_param.max(i);
                limit = Some(0);
                limit_param = Some(i - 1); // 0-based
            }
            other => return Err(format!("expected LIMIT count, got {other:?}")),
        }
    }
    // ⭐ S2: OFFSET n (PG/MySQL 通用形态)
    if p.try_kw("OFFSET") {
        match p.next()? {
            Tok::Num(n) => {
                offset = Some(n.parse::<u32>().map_err(|_| format!("bad OFFSET {n}"))?);
            }
            Tok::Dollar(i) => {
                p.next_param = p.next_param.max(i);
                offset = Some(0);
                offset_param = Some(i - 1); // 0-based
            }
            other => return Err(format!("expected OFFSET count, got {other:?}")),
        }
    }
    p.done_if(top)?;
    Ok(SqlStmt::Select { table, items, conds, limit, order, offset, group_by, having, limit_param, offset_param })
}
