//! 表限定名 (`db.tbl`) 规范化与校验 — 2026-10-16 审计新 P0-3 修复.
//!
//! 背景: 解析器旧实现在 `table_ident()` 静默剥离 db 限定前缀
//! (`db.tbl` → `tbl`), 导致 `USE app; SELECT * FROM otherdb.users` 按当前库
//! 解析 — 读会返回**错误库**的数据 (跨库数据暴露), 写会污染/删除当前库的
//! 同名表, 且无日志、无指标、无告警. 与 2026-08-02 类事故同属"静默降级".
//!
//! 现解析器保留限定名原样, 由本模块在 `sql_dispatch_stmt` 入口统一处理:
//! - 无限定名            → 原样透传 (现有行为不变);
//! - 限定名 == 当前库    → 剥离为裸表名 (`USE app; ... app.kv` 保持工作);
//! - 限定名 != 当前库    → 显式报错:
//!   - 库不存在 → `Unknown database '{db}'` (MySQL 1049 / PG 3D000);
//!   - 库存在   → `Cross-database access is not supported ...` (0A000).
//!
//! 覆盖全部语句形态: 单表语句 / JOIN 的 from+各 join 表 (含默认别名) /
//! 嵌套 inner (派生表/EXISTS) / SHOW 虚拟表的 __table__ 条件.
//!
//! 已知边界: 反引号引用且名字内含 '.' 的表名 (`` `my.table` ``) 与限定名
//! `my.table` 在 tokenizer 层不可区分 — 旧实现会静默截成 `table` (错表),
//! 现实现报错 (loud fail), 方向正确.

use crate::protocol::sql::{Cond, Pred, SqlStmt, SqlValue, TableRef};

/// 规范化语句中的全部表限定名. 见模块文档.
pub(crate) fn normalize_stmt_tables(
    stmt: &mut SqlStmt,
    current_db: &str,
    db_exists: &dyn Fn(&str) -> bool,
) -> Result<(), String> {
    match stmt {
        // 嵌套语句: 递归 (派生表 / EXISTS 子查询)
        SqlStmt::ExistsStub { inner } => normalize_stmt_tables(inner, current_db, db_exists),
        SqlStmt::SelectDerived { inner, .. } => normalize_stmt_tables(inner, current_db, db_exists),
        // JOIN: from + 左深链各表 (from_inner 为派生表时递归)
        SqlStmt::SelectJoin {
            from,
            from_inner,
            joins,
            ..
        } => {
            if let Some(inner) = from_inner {
                normalize_stmt_tables(inner, current_db, db_exists)?;
            }
            resolve_table_ref(from, current_db, db_exists)?;
            for j in joins.iter_mut() {
                resolve_table_ref(&mut j.table, current_db, db_exists)?;
            }
            Ok(())
        }
        // SHOW 虚拟表: 目标表名在 __table__ 条件叶子里 (parser 拼好的 `db.t`)
        SqlStmt::SystemQuery { catalog, conds, .. } if catalog == "__show__" => {
            rewrite_show_target(conds, current_db, db_exists)
        }
        // 其余变体: 单个 table 字段
        SqlStmt::CreateTable { table, .. }
        | SqlStmt::Insert { table, .. }
        | SqlStmt::Select { table, .. }
        | SqlStmt::Delete { table, .. }
        | SqlStmt::Update { table, .. }
        | SqlStmt::DropTable { table }
        | SqlStmt::CreateIndex { table, .. }
        | SqlStmt::AlterTable { table, .. }
        | SqlStmt::SetRespRowAdapter { table, .. }
        | SqlStmt::Describe { table } => resolve_table_name(table, current_db, db_exists),
        // 无表名的语句 (USE/BEGIN/COMMIT/SET/DATABASE()/系统变量/标量投影...)
        _ => Ok(()),
    }
}

/// TableRef: 剥离限定名; 别名若默认取自原限定名 (无显式 AS) 则同步改裸表名.
fn resolve_table_ref(
    r: &mut TableRef,
    current_db: &str,
    db_exists: &dyn Fn(&str) -> bool,
) -> Result<(), String> {
    let old = r.table.clone();
    resolve_table_name(&mut r.table, current_db, db_exists)?;
    if r.alias == old {
        r.alias = r.table.clone();
    }
    Ok(())
}

/// 三态规则: 无限定 → 原样; 同库 → 剥离; 跨库 → 显式报错.
fn resolve_table_name(
    raw: &mut String,
    current_db: &str,
    db_exists: &dyn Fn(&str) -> bool,
) -> Result<(), String> {
    let Some((qual, leaf)) = raw.rsplit_once('.') else {
        return Ok(());
    };
    if qual.is_empty() || leaf.is_empty() {
        return Err(format!("malformed table name '{raw}'"));
    }
    if qual == current_db {
        *raw = leaf.to_string();
        return Ok(());
    }
    if db_exists(qual) {
        Err(format!(
            "Cross-database access is not supported: '{raw}' (session database is '{current_db}')"
        ))
    } else {
        Err(format!("Unknown database '{qual}'"))
    }
}

/// SHOW 虚拟表: `__table__` 条件叶子里的目标表名同样过校验 (递归整棵 Pred).
fn rewrite_show_target(
    conds: &mut Pred<Cond>,
    current_db: &str,
    db_exists: &dyn Fn(&str) -> bool,
) -> Result<(), String> {
    match conds {
        Pred::Leaf(c) if c.col == "__table__" => {
            if let SqlValue::Str(b) = &mut c.val {
                let mut name = String::from_utf8_lossy(b).into_owned();
                resolve_table_name(&mut name, current_db, db_exists)?;
                *b = name.into_bytes();
            }
            Ok(())
        }
        Pred::And(v) | Pred::Or(v) => {
            for p in v.iter_mut() {
                rewrite_show_target(p, current_db, db_exists)?;
            }
            Ok(())
        }
        Pred::Not(p) => rewrite_show_target(p, current_db, db_exists),
        Pred::Leaf(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::sql::{CmpOp, SqlStmt};

    fn no_dbs(_q: &str) -> bool {
        false
    }
    fn dbs_app_other(q: &str) -> bool {
        q == "app" || q == "other"
    }

    #[test]
    fn unqualified_untouched() {
        let mut s = SqlStmt::Select {
            table: "users".into(),
            items: vec![],
            conds: Pred::And(vec![]),
            limit: None,
            order: vec![],
            offset: None,
            group_by: vec![],
            having: Pred::And(vec![]),
            limit_param: None,
            offset_param: None,
        };
        normalize_stmt_tables(&mut s, "app", &no_dbs).unwrap();
        match s {
            SqlStmt::Select { table, .. } => assert_eq!(table, "users"),
            _ => unreachable!(),
        }
    }

    #[test]
    fn same_db_qualifier_stripped() {
        let mut s = SqlStmt::Select {
            table: "app.users".into(),
            items: vec![],
            conds: Pred::And(vec![]),
            limit: None,
            order: vec![],
            offset: None,
            group_by: vec![],
            having: Pred::And(vec![]),
            limit_param: None,
            offset_param: None,
        };
        normalize_stmt_tables(&mut s, "app", &dbs_app_other).unwrap();
        match s {
            SqlStmt::Select { table, .. } => assert_eq!(table, "users"),
            _ => unreachable!(),
        }
    }

    #[test]
    fn cross_db_existing_errors() {
        let mut s = SqlStmt::Select {
            table: "other.users".into(),
            items: vec![],
            conds: Pred::And(vec![]),
            limit: None,
            order: vec![],
            offset: None,
            group_by: vec![],
            having: Pred::And(vec![]),
            limit_param: None,
            offset_param: None,
        };
        let e = normalize_stmt_tables(&mut s, "app", &dbs_app_other).unwrap_err();
        assert!(e.contains("Cross-database access is not supported"), "{e}");
    }

    #[test]
    fn cross_db_missing_errors_unknown_database() {
        let mut s = SqlStmt::Select {
            table: "nosuch.users".into(),
            items: vec![],
            conds: Pred::And(vec![]),
            limit: None,
            order: vec![],
            offset: None,
            group_by: vec![],
            having: Pred::And(vec![]),
            limit_param: None,
            offset_param: None,
        };
        let e = normalize_stmt_tables(&mut s, "app", &dbs_app_other).unwrap_err();
        assert!(e.contains("Unknown database 'nosuch'"), "{e}");
    }

    #[test]
    fn join_tables_and_default_alias_fixed() {
        use crate::protocol::sql::{JoinClause, JoinKind};
        let mut s = SqlStmt::SelectJoin {
            from: TableRef {
                table: "app.users".into(),
                alias: "app.users".into(), // 解析器默认别名 = 原限定名
            },
            from_inner: None,
            joins: vec![JoinClause {
                kind: JoinKind::Inner,
                table: TableRef {
                    table: "other.orders".into(),
                    alias: "other.orders".into(),
                },
                on: vec![],
            }],
            items: vec![],
            conds: Pred::And(vec![]),
            order: vec![],
            limit: None,
            offset: None,
            limit_param: None,
            offset_param: None,
        };
        // from 同库 → 剥离 + 别名同步; join 跨库 → 报错
        let e = normalize_stmt_tables(&mut s, "app", &dbs_app_other).unwrap_err();
        assert!(e.contains("Cross-database"), "{e}");
    }

    #[test]
    fn show_target_cond_rewritten() {
        let mut s = SqlStmt::SystemQuery {
            catalog: "__show__".into(),
            table: "columns".into(),
            cols: vec![],
            conds: Pred::Leaf(Cond {
                col: "__table__".into(),
                op: CmpOp::Eq,
                val: SqlValue::Str(b"app.users".to_vec()),
                set: vec![],
            }),
            order: vec![],
            limit: None,
            offset: None,
            limit_param: None,
            offset_param: None,
        };
        normalize_stmt_tables(&mut s, "app", &dbs_app_other).unwrap();
        match s {
            SqlStmt::SystemQuery {
                conds: Pred::Leaf(c),
                ..
            } => match c.val {
                SqlValue::Str(b) => assert_eq!(b, b"users"),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    #[test]
    fn nested_inner_validated() {
        let inner = SqlStmt::Select {
            table: "other.users".into(),
            items: vec![],
            conds: Pred::And(vec![]),
            limit: None,
            order: vec![],
            offset: None,
            group_by: vec![],
            having: Pred::And(vec![]),
            limit_param: None,
            offset_param: None,
        };
        let mut s = SqlStmt::ExistsStub {
            inner: Box::new(inner),
        };
        let e = normalize_stmt_tables(&mut s, "app", &dbs_app_other).unwrap_err();
        assert!(e.contains("Cross-database"), "{e}");
    }
}
