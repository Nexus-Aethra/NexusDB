// ⭐ 解耦 2026-08: DDL 解析辅助函数 (从 parser.rs 拆出).
// 职责: CREATE TABLE 的列清单/类型/默认值/外键动作解析.
use super::ast::*;
use super::parser::{P, Tok};
use storage::schema::{ColType, Column, TableSchema};

pub(crate) fn read_col_list(p: &mut P) -> Result<Vec<String>, String> {
    p.expect(&Tok::LParen, "(")?;
    let mut cols = Vec::new();
    loop {
        cols.push(p.ident()?);
        // 排序后缀 (仅索引列场景)
        p.try_kw("ASC");
        p.try_kw("DESC");
        match p.next()? {
            Tok::Comma => continue,
            Tok::RParen => break,
            other => return Err(format!("expected ',' or ')' in column list, got {other:?}")),
        }
    }
    Ok(cols)
}

/// ⭐ PG 兼容 (FMT_VER 8): 外键原始定义 (解析期; 列位转数字后入 TableSchema).
pub(crate) struct FkDefRaw {
    pub(crate) col: String,
    pub(crate) ref_table: String,
    pub(crate) ref_col: String,
    pub(crate) action: storage::schema::FkAction,
}

/// ⭐ PG 兼容: 解析外键 `ON DELETE [CASCADE|SET NULL|NO ACTION|RESTRICT]` 动作,
/// 吞掉后续 `ON UPDATE ...` 子句 (v1 不实现 UPDATE 级联).
pub(crate) fn parse_fk_action(p: &mut P) -> Result<storage::schema::FkAction, String> {
    use storage::schema::FkAction;
    let mut action = FkAction::NoAction;
    loop {
        let Some(first) = p.peek().and_then(|t| match t {
            Tok::Ident(s) => Some(s.to_ascii_lowercase()),
            _ => None,
        }) else {
            break;
        };
        if first != "on" {
            break;
        }
        p.next()?; // ON
        let w = p.ident()?.to_ascii_lowercase(); // DELETE / UPDATE
        let a = p.ident()?.to_ascii_lowercase();
        if w == "delete" {
            action = match a.as_str() {
                "cascade" => FkAction::Cascade,
                "set" => {
                    p.kw("null")?;
                    FkAction::SetNull
                }
                "no" => {
                    let _ = p.ident()?; // ACTION
                    FkAction::NoAction
                }
                _ => FkAction::NoAction, // RESTRICT / 其他 → v1 不检查
            };
        }
        // ON UPDATE 的 action 已吞 (a 已消费; set null 已吞 null)
    }
    Ok(action)
}

/// ⭐ PG 兼容: 解析列 DEFAULT 表达式 → ColDefault (v1: 字面量 / NOW /
/// uuid_generate_v4; 未知函数/表达式 → None, 吞掉不落默认). 含 `::type` 后缀.
pub(crate) fn parse_col_default(
    p: &mut P,
    ty: ColType,
) -> Result<Option<storage::schema::ColDefault>, String> {
    use storage::schema::ColDefault;
    // 函数调用 / 裸标识 (true/false/null/未加引号文本)
    if let Some(Tok::Ident(_)) = p.peek() {
        let name = p.ident()?.to_ascii_lowercase();
        if p.peek() == Some(&Tok::LParen) {
            // 函数: NOW() / uuid_generate_v4() — 吞括号及参数
            p.next()?;
            let mut depth = 1;
            while depth > 0 {
                match p.next()? {
                    Tok::LParen => depth += 1,
                    Tok::RParen => depth -= 1,
                    _ => {}
                }
            }
            let d = match name.as_str() {
                "now" | "current_timestamp" | "current_date" | "current_time" => {
                    Some(ColDefault::Now)
                }
                "uuid_generate_v4" => Some(ColDefault::UuidGenV4),
                _ => None, // 未知函数 → 吞掉不落默认
            };
            // ::type 后缀
            if p.peek() == Some(&Tok::Colon) {
                p.next()?;
                let _ = p.ident()?;
            }
            return Ok(d);
        }
        // 裸标识 (PG 允许 DEFAULT true / 'text')
        let val = match name.as_str() {
            "true" => crate::protocol::sql::SqlValue::Int(1),
            "false" => crate::protocol::sql::SqlValue::Int(0),
            "null" => crate::protocol::sql::SqlValue::Null,
            _ => crate::protocol::sql::SqlValue::Str(name.into_bytes()),
        };
        let cv = crate::worker::sql_to_col(ty, &val)?;
        if p.peek() == Some(&Tok::Colon) {
            p.next()?;
            let _ = p.ident()?;
        }
        return Ok(Some(ColDefault::Lit(cv)));
    }
    // 字面量 (含 '{}'::jsonb 等)
    let val = p.value()?;
    if p.peek() == Some(&Tok::Colon) {
        p.next()?;
        let _ = p.ident()?;
    }
    let cv = crate::worker::sql_to_col(ty, &val)?;
    Ok(Some(ColDefault::Lit(cv)))
}

pub(crate) fn parse_create(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("CREATE")?;
    // ⭐ compat: CREATE OR REPLACE FUNCTION — 吞掉 (v1 不做触发器/函数)
    if p.try_kw("OR") {
        p.kw("REPLACE")?;
        p.kw("FUNCTION")?;
        return Ok(SqlStmt::DdlStub);
    }
    if p.try_kw("INDEX") {
        // CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON t (col, ...) [WHERE ...]
        p.try_kw("UNIQUE");
        let if_not_exists = if p.try_kw("IF") {
            p.kw("NOT")?;
            p.kw("EXISTS")?;
            true
        } else {
            false
        };
        let _ = p.ident()?; // 索引名 (吞)
        p.kw("ON")?;
        let table = p.table_ident()?;
        let cols = read_col_list(p)?;
        // 吞可选 WHERE 部分索引 (至语句结束)
        if p.try_kw("WHERE") {
            while !matches!(p.peek(), None) {
                p.i += 1;
            }
        }
        p.done()?;
        return Ok(SqlStmt::CreateIndex {
            table,
            cols,
            if_not_exists,
        });
    }
    if p.try_kw("EXTENSION") {
        // CREATE EXTENSION [IF NOT EXISTS] "name" — 吞掉 (uuid-ossp 等)
        let _ = p.try_kw("IF");
        let _ = p.try_kw("NOT");
        let _ = p.try_kw("EXISTS");
        let _ = p.ident()?; // 扩展名 (含双引号已去)
        p.done()?;
        return Ok(SqlStmt::DdlStub);
    }
    if p.try_kw("TRIGGER") {
        // CREATE TRIGGER name ... — 吞掉
        return Ok(SqlStmt::DdlStub);
    }
    if p.try_kw("DATABASE") {
        // ⭐ PG 兼容: CREATE DATABASE name — 真实建库 (worker 走 shard 2PC)
        let name = p.ident()?;
        p.done()?;
        return Ok(SqlStmt::CreateDb { name });
    }
    if p.try_kw("SEQUENCE") || p.try_kw("TYPE") {
        return Ok(SqlStmt::DdlStub);
    }
    p.kw("TABLE")?;
    // ⭐ IF NOT EXISTS: `CREATE TABLE IF NOT EXISTS t (...)` — 表已存在时静默跳过
    let if_not_exists = if p.try_kw("IF") {
        p.kw("NOT")?;
        p.kw("EXISTS")?;
        true
    } else {
        false
    };
    let table = p.table_ident()?;
    p.expect(&Tok::LParen, "(")?;

    let mut columns: Vec<Column> = Vec::new();
    let mut pk: Option<u16> = None;
    let mut pk_name: Option<String> = None; // ⭐ F76: 表级 PRIMARY KEY (col)
    let mut index_names: Vec<String> = Vec::new();
    let mut unique_names: Vec<String> = Vec::new(); // ⭐ O3
    let mut global_unique_names: Vec<String> = Vec::new(); // ⭐ F65
    let mut composite_unique_names: Vec<Vec<String>> = Vec::new(); // ⭐ PG 兼容 (FMT_VER 7)
    let mut fks: Vec<FkDefRaw> = Vec::new(); // ⭐ PG 兼容 (FMT_VER 8): 外键 (列级+表级)
    loop {
        if p.try_kw("INDEX") {
            p.expect(&Tok::LParen, "(")?;
            index_names.push(p.ident()?);
            p.expect(&Tok::RParen, ")")?;
        } else if p.try_kw("CONSTRAINT") {
            // ⭐ F76: CONSTRAINT [name] <PRIMARY KEY|UNIQUE|FOREIGN KEY> — 吃可选名后
            // continue 重进循环, 由下方 PRIMARY/UNIQUE/FOREIGN 分支处理实体
            if !matches!(p.peek(), Some(Tok::Ident(s)) if
                s.eq_ignore_ascii_case("PRIMARY") || s.eq_ignore_ascii_case("UNIQUE")
                    || s.eq_ignore_ascii_case("FOREIGN") || s.eq_ignore_ascii_case("KEY"))
            {
                let _ = p.ident()?; // 约束名
            }
            continue;
        } else if p.try_kw("PRIMARY") {
            p.kw("KEY")?;
            let cols = read_col_list(p)?;
            pk_name = cols.into_iter().next(); // v1 单列 pk (复合取首列)
        } else if p.try_kw("UNIQUE") {
            p.try_kw("KEY");
            // 可选索引名 (非左括号时)
            if p.peek() != Some(&Tok::LParen) {
                let _ = p.ident()?;
            }
            let cols = read_col_list(p)?;
            if cols.len() == 1 {
                unique_names.push(cols[0].clone());
            } else {
                // ⭐ PG 兼容 (FMT_VER 7): 复合 UNIQUE → 整组保留 (schema 拼 key 唯一)
                composite_unique_names.push(cols);
            }
        } else if p.try_kw("KEY") {
            if p.peek() != Some(&Tok::LParen) {
                let _ = p.ident()?;
            }
            let cols = read_col_list(p)?;
            if let Some(c) = cols.into_iter().next() {
                index_names.push(c);
            }
        } else if p.try_kw("FOREIGN") {
            // ⭐ PG 兼容 (FMT_VER 8): 表级外键 FOREIGN KEY (col) REFERENCES t(col) [ON ...]
            p.kw("KEY")?;
            let cols = read_col_list(p)?;
            p.kw("REFERENCES")?;
            let ref_table = p.ident()?;
            let ref_cols = if p.peek() == Some(&Tok::LParen) {
                read_col_list(p)?
            } else {
                Vec::new()
            };
            let action = parse_fk_action(p)?;
            // v1: 单列外键 (复合外键取首列)
            if let Some(c) = cols.into_iter().next() {
                fks.push(FkDefRaw {
                    col: c,
                    ref_table,
                    ref_col: ref_cols
                        .into_iter()
                        .next()
                        .unwrap_or_else(|| "id".to_string()),
                    action,
                });
            }
        } else if p.try_kw("CHECK") {
            // ⭐ compat: 表级 CHECK (expr) — v1 吞 (不强制约束)
            if p.peek() == Some(&Tok::LParen) {
                p.next()?;
                let mut depth = 1;
                while depth > 0 {
                    match p.next()? {
                        Tok::LParen => depth += 1,
                        Tok::RParen => depth -= 1,
                        _ => {}
                    }
                }
            }
        } else {
            let name = p.ident()?;
            let (ty, is_serial) = parse_col_type(p)?;
            let mut nullable = true;
            let mut is_pk = false;
            let mut default: Option<storage::schema::ColDefault> = None;
            if is_serial {
                // ⭐ PG 兼容 (portal): SERIAL/BIGSERIAL → 自动递增默认值
                default = Some(storage::schema::ColDefault::Serial);
            }
            loop {
                if p.try_kw("PRIMARY") {
                    p.kw("KEY")?;
                    is_pk = true;
                    nullable = false;
                } else if p.try_kw("NOT") {
                    p.kw("NULL")?;
                    nullable = false;
                } else if p.try_kw("GLOBAL") {
                    // ⭐ F65: 列级 GLOBAL UNIQUE = 跨 shard 全局唯一 (email-shard 占坑)
                    p.kw("UNIQUE")?;
                    global_unique_names.push(name.clone());
                    nullable = false;
                } else if p.try_kw("UNIQUE") {
                    // ⭐ O3: 列级 UNIQUE = 自动建唯一索引 (隐含 NOT NULL —
                    // NULL 不入索引, 无法参与唯一性, 直接拒绝更诚实)
                    unique_names.push(name.clone());
                    nullable = false;
                } else if p.try_kw("AUTO_INCREMENT") || p.try_kw("AUTOINCREMENT") {
                    // ⭐ F76: 吃 AUTO_INCREMENT (v1 不做服务端自增; ORM 提供显式 id)
                } else if p.try_kw("DEFAULT") {
                    // ⭐ PG 兼容: 捕获 DEFAULT 表达式 → Column.default
                    default = parse_col_default(p, ty)?;
                } else if p.try_kw("COMMENT") {
                    // ⭐ F76: 吃 COMMENT '…'
                    let _ = p.value()?;
                } else if p.try_kw("REFERENCES") {
                    // ⭐ PG 兼容 (FMT_VER 8): 列级外键 `REFERENCES t (c) [ON DELETE ...]`
                    let ref_table = p.ident()?;
                    let ref_col = if p.peek() == Some(&Tok::LParen) {
                        p.next()?;
                        let c = p.ident()?;
                        p.expect(&Tok::RParen, ")")?;
                        c
                    } else {
                        // PG: REFERENCES t (无列) → 引用 t 的主键 (v1 记名 "id" 占位,
                        // 级联时按引用表实际 pk 列解析)
                        "id".to_string()
                    };
                    let action = parse_fk_action(p)?;
                    fks.push(FkDefRaw {
                        col: name.clone(),
                        ref_table,
                        ref_col,
                        action,
                    });
                } else if p.try_kw("CHECK") {
                    // ⭐ compat: 列级 CHECK (expr) — v1 吞 (不强制约束)
                    if p.peek() == Some(&Tok::LParen) {
                        p.next()?;
                        let mut depth = 1;
                        while depth > 0 {
                            match p.next()? {
                                Tok::LParen => depth += 1,
                                Tok::RParen => depth -= 1,
                                _ => {}
                            }
                        }
                    }
                } else {
                    break;
                }
            }
            if is_pk {
                if pk.is_some() {
                    return Err("multiple PRIMARY KEY".into());
                }
                pk = Some(columns.len() as u16);
            }
            columns.push(Column {
                name,
                ty,
                nullable,
                default,
            });
        }
        match p.next()? {
            Tok::Comma => continue,
            Tok::RParen => break,
            other => return Err(format!("expected ',' or ')', got {other:?}")),
        }
    }
    p.done()?;

    // ⭐ F76: 表级 PRIMARY KEY (col) → 解析列位 (内联 pk 优先); 并置该列 NOT NULL
    if pk.is_none()
        && let Some(n) = &pk_name
    {
        let i = columns
            .iter()
            .position(|c| c.name == *n)
            .ok_or_else(|| format!("PRIMARY KEY on unknown column {n}"))?;
        columns[i].nullable = false;
        pk = Some(i as u16);
    }
    // ⭐ compat (自动主键): 无 PRIMARY KEY 时降级 —
    // L1: 恰一个单列 UNIQUE → 提升为 PK (零膨胀; 该列不重复建唯一索引);
    // L2: 否则注入隐藏自增列 __rowid BIGINT NOT NULL 为 PK (8B, worker 进程级
    // Atomic 递增, seed=启动时间戳 → 免恢复; SELECT * 对外隐藏该列).
    let pk = match pk {
        Some(p) => p,
        None => {
            if unique_names.len() == 1 && global_unique_names.is_empty() {
                let u = unique_names.pop().unwrap();
                columns
                    .iter()
                    .position(|c| c.name == u)
                    .map(|i| i as u16)
                    .ok_or_else(|| format!("UNIQUE on unknown column {u}"))?
            } else {
                if columns
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case("__rowid"))
                {
                    return Err("column name '__rowid' is reserved for auto rowid".into());
                }
                columns.push(Column {
                    name: "__rowid".to_string(),
                    ty: ColType::I64,
                    nullable: false,
                    default: None,
                });
                (columns.len() - 1) as u16
            }
        }
    };
    let col_pos = |n: &str, what: &str| -> Result<u16, String> {
        columns
            .iter()
            .position(|c| c.name == n)
            .map(|i| i as u16)
            .ok_or_else(|| format!("{what} on unknown column {n}"))
    };
    let mut index_cols: Vec<u16> = Vec::with_capacity(index_names.len());
    for n in &index_names {
        index_cols.push(col_pos(n, "INDEX")?);
    }
    let mut unique_cols: Vec<u16> = Vec::with_capacity(unique_names.len());
    for n in &unique_names {
        unique_cols.push(col_pos(n, "UNIQUE")?);
    }
    let mut global_unique_cols: Vec<u16> = Vec::with_capacity(global_unique_names.len());
    for n in &global_unique_names {
        global_unique_cols.push(col_pos(n, "GLOBAL UNIQUE")?);
    }
    let mut composite_unique_cols: Vec<Vec<u16>> = Vec::with_capacity(composite_unique_names.len());
    for g in &composite_unique_names {
        let mut cols = Vec::with_capacity(g.len());
        for n in g {
            cols.push(col_pos(n, "UNIQUE")?);
        }
        composite_unique_cols.push(cols);
    }
    let mut fk_defs: Vec<storage::schema::FkDef> = Vec::with_capacity(fks.len());
    for fk in &fks {
        let col = col_pos(&fk.col, "FOREIGN KEY")?;
        fk_defs.push(storage::schema::FkDef {
            col,
            ref_table: fk.ref_table.clone(),
            ref_col: fk.ref_col.clone(),
            on_delete: fk.action,
        });
    }
    let schema = TableSchema::new(
        columns,
        pk,
        &index_cols,
        &unique_cols,
        &global_unique_cols,
        &composite_unique_cols,
        &fk_defs,
    )
    .map_err(|e| e.to_string())?;
    Ok(SqlStmt::CreateTable {
        table,
        schema,
        if_not_exists,
    })
}

/// `INSERT INTO t [(c1,...)] VALUES (v1,...)`
pub(crate) fn parse_col_type(p: &mut P) -> Result<(ColType, bool), String> {
    let ty_name = p.ident()?;
    let up = ty_name.to_ascii_uppercase();
    // ⭐ F81: DECIMAL/NUMERIC(p,s) — 捕获精度与标度存入类型
    if up == "DECIMAL" || up == "NUMERIC" || up == "DEC" {
        let (mut precision, mut scale) = (10u8, 0u8);
        if p.peek() == Some(&Tok::LParen) {
            p.next()?;
            precision = match p.next()? {
                Tok::Num(n) => n
                    .parse::<u8>()
                    .map_err(|_| "bad DECIMAL precision".to_string())?,
                other => return Err(format!("expected DECIMAL precision, got {other:?}")),
            };
            if p.peek() == Some(&Tok::Comma) {
                p.next()?;
                scale = match p.next()? {
                    Tok::Num(n) => n
                        .parse::<u8>()
                        .map_err(|_| "bad DECIMAL scale".to_string())?,
                    other => return Err(format!("expected DECIMAL scale, got {other:?}")),
                };
            }
            p.expect(&Tok::RParen, ")")?;
        }
        if scale > 38 || precision > 38 {
            return Err("DECIMAL precision/scale must be <= 38".into());
        }
        return Ok((ColType::Decimal { precision, scale }, false));
    }
    let serial = matches!(up.as_str(), "SERIAL" | "BIGSERIAL" | "SMALLSERIAL");
    let ty = match up.as_str() {
        "INT" | "BIGINT" | "INTEGER" | "SMALLINT" => ColType::I64,
        "BOOLEAN" | "BOOL" => ColType::Bool,
        "DOUBLE" | "FLOAT" | "REAL" => ColType::F64,
        "TEXT" | "VARCHAR" | "CHAR" | "STRING" => ColType::Str,
        "BLOB" | "BYTES" | "BYTEA" => ColType::Bytes,
        "DATE" => ColType::Date,
        "TIME" => ColType::Time,
        "TIMESTAMP" | "DATETIME" | "TIMESTAMPTZ" => ColType::Timestamp, // ⭐ compat: TIMESTAMPTZ 别名
        "JSON" | "JSONB" => ColType::Json,
        "UUID" => ColType::Uuid,
        // ⭐ PG 兼容 (portal): INET — 网络地址存文本
        "INET" => ColType::Str,
        // ⭐ PG 兼容 (portal): SERIAL/BIGSERIAL — 自增整型主键
        "SERIAL" | "BIGSERIAL" | "SMALLSERIAL" => ColType::I64,
        other => return Err(format!("unknown type {other}")),
    };
    if ty_name.eq_ignore_ascii_case("DOUBLE") {
        p.try_kw("PRECISION");
    }
    // 长度参数: (n) — 吞
    if p.peek() == Some(&Tok::LParen) {
        p.next()?;
        loop {
            match p.next()? {
                Tok::Num(_) => {}
                other => return Err(format!("expected type length, got {other:?}")),
            }
            match p.next()? {
                Tok::Comma => continue,
                Tok::RParen => break,
                other => return Err(format!("expected ',' or ')' in type params, got {other:?}")),
            }
        }
    }
    // ⭐ compat: 数组后缀 `TEXT[]` — v1 映射为标量类型 (语义: 存 JSON 序列化数组)
    let is_array = p.try_kw("ARRAY")
        || if matches!(p.peek(), Some(Tok::LBracket)) {
            p.next()?;
            p.expect(&Tok::RBracket, "]")?;
            true
        } else {
            false
        };
    if is_array {
        // 数组类型映射为 Str 列 (值为 JSON 数组文本); 保持类型信息在注释/元数据
        return Ok((
            match ty {
                ColType::I64 => ColType::I64,
                _ => ColType::Str,
            },
            false,
        ));
    }
    Ok((ty, serial))
}

/// ⭐ S1: `DROP TABLE [IF EXISTS] t`
pub(crate) fn parse_drop(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("DROP")?;
    if p.try_kw("TRIGGER") {
        // ⭐ compat: DROP TRIGGER [IF EXISTS] name ON t — 吞掉
        return Ok(SqlStmt::DdlStub);
    }
    if p.try_kw("INDEX") {
        // ⭐ compat: DROP INDEX [IF EXISTS] name — 吞掉
        return Ok(SqlStmt::DdlStub);
    }
    p.kw("TABLE")?;
    let _if_exists = p.try_kw("IF") && {
        p.try_kw("NOT");
        p.try_kw("EXISTS");
        true
    };
    // 支持逗号分隔多表 DROP: 只取首表 (其余吞)
    let table = p.table_ident()?;
    while p.try_kw("CASCADE") || p.try_kw("RESTRICT") {}
    if matches!(p.peek(), Some(Tok::Comma)) {
        while !matches!(p.peek(), None) {
            p.i += 1;
        }
    }
    p.done()?;
    Ok(SqlStmt::DropTable { table })
}

/// ⭐ F79: `ALTER TABLE t ADD [COLUMN] [IF NOT EXISTS] name TYPE [NULL|NOT NULL] [DEFAULT v]`.
/// v1 仅支持追加可空列; DROP/MODIFY/RENAME 拒.
/// ⭐ compat: `ALTER TABLE t DROP [COLUMN] c` — 标记删除 (列号/布局不变, 存量零重写).
pub(crate) fn parse_alter(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("ALTER")?;
    p.kw("TABLE")?;
    let table = p.table_ident()?;
    if p.try_kw("SET") {
        p.kw("RESP")?;
        p.kw("ADAPTER")?;
        let enabled = if p.try_kw("ON") {
            true
        } else if p.try_kw("OFF") {
            false
        } else {
            return Err("ALTER TABLE ... SET RESP ADAPTER requires ON or OFF".into());
        };
        p.done()?;
        return Ok(SqlStmt::SetRespRowAdapter { table, enabled });
    }
    if p.try_kw("DROP") {
        p.try_kw("COLUMN"); // 可选
        let name = p.ident()?;
        p.done()?;
        return Ok(SqlStmt::AlterTable {
            table,
            add: None,
            drop: Some(name),
            if_not_exists: false,
        });
    }
    if !p.try_kw("ADD") {
        return Err("only ALTER TABLE ADD COLUMN / DROP COLUMN is supported (v1)".into());
    }
    p.try_kw("COLUMN"); // 可选
    // ⭐ compat: ADD COLUMN IF NOT EXISTS
    let if_not_exists = p.try_kw("IF") && {
        p.try_kw("NOT");
        p.try_kw("EXISTS");
        true
    };
    let name = p.ident()?;
    let (ty, is_serial) = parse_col_type(p)?;
    // 列属性: NULL/NOT NULL/DEFAULT
    let mut nullable = true;
    let mut default: Option<storage::schema::ColDefault> = None;
    if is_serial {
        default = Some(storage::schema::ColDefault::Serial);
    }
    loop {
        if p.try_kw("NOT") {
            p.kw("NULL")?;
            nullable = false;
        } else if p.try_kw("NULL") {
            nullable = true;
        } else if p.try_kw("DEFAULT") {
            // ⭐ PG 兼容: 捕获 DEFAULT 表达式 (由 worker 对新行求值回填)
            default = parse_col_default(p, ty)?;
        } else {
            break;
        }
    }
    // ⭐ compat: NOT NULL 且无 DEFAULT → 旧行无法回填, 保持 v1 拒绝;
    //   有 DEFAULT (如迁移的 NOT NULL DEFAULT false) → 接受 (由 worker 回填默认).
    if !nullable && default.is_none() {
        return Err(
            "ADD COLUMN NOT NULL requires a DEFAULT (v1: cannot backfill existing rows)".into(),
        );
    }
    p.done()?;
    Ok(SqlStmt::AlterTable {
        table,
        add: Some(Column {
            name,
            ty,
            nullable,
            default,
        }),
        drop: None,
        if_not_exists,
    })
}
