// ⭐ 解耦 2026-08: DDL 解析辅助函数 (从 parser.rs 拆出).
// 职责: CREATE TABLE 的列清单/类型/默认值/外键动作解析.
use super::ast::*;
use super::parser::{P, Tok};
use storage::schema::{ColType, Column, FkAction, ColDefault, TableSchema};

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

