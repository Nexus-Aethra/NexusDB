//! ⭐ X1 (SQL 落地): 最小 SQL 子集解析器 — 纯函数, 手写 tokenizer, 零依赖.
//!
//! 拆分 (2026-08):
//! - AST 类型定义 → `sql_ast.rs`
//! - tokenizer + parser → `sql_parser.rs`
//! - 本文件作为公共 re-export 薄模块, 保持 `protocol::sql::*` 外部引用不变.
//!
//! **架构说明 (解析器 vs 优化器)**:
//! 当前是 tree-walking parser 直接产出 AST (`SqlStmt`), 无独立优化器层。
//! 访问路径选择 (pk 等值 → 索引界下推 → 全扫) 在 worker 端 `sql_plan_select` 完成,
//! 属执行期规划。后续如需独立优化器 (谓词下推/连接重排/常量折叠), 建议
//! 新增 `sql_planner.rs` 层, 在 parse 与执行之间做 AST → 物理计划转换。

mod ast;
mod parser;

pub use ast::*;
pub use parser::{
    bind_params, parse, parse_prepared, split_sql_statements,
};

#[cfg(test)]
mod tests {
    use super::*;
    use storage::schema::ColType;

    #[test]
    fn create_roundtrip() {
        let s = parse(b"CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, score DOUBLE, INDEX(name), INDEX(score))").unwrap();
        let SqlStmt::CreateTable { table, schema, if_not_exists } = s else { panic!() };
        assert_eq!(table, "users");
        assert!(!if_not_exists);
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.pk_col, 0);
        assert!(!schema.columns[0].nullable);
        assert!(!schema.columns[1].nullable);
        assert!(schema.columns[2].nullable);
        assert_eq!(schema.columns[2].ty, ColType::F64);
        assert_eq!(schema.indexes.len(), 2);
        assert_eq!(schema.indexes[0].col, 1);
        assert_eq!(schema.indexes[1].col, 2);
    }

    #[test]
    fn create_if_not_exists() {
        let s = parse(b"CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY)").unwrap();
        let SqlStmt::CreateTable { table, if_not_exists, .. } = s else { panic!() };
        assert_eq!(table, "users");
        assert!(if_not_exists);

        // 无 IF NOT EXISTS → false
        let s = parse(b"CREATE TABLE users2 (id INT PRIMARY KEY)").unwrap();
        let SqlStmt::CreateTable { if_not_exists, .. } = s else { panic!() };
        assert!(!if_not_exists);
    }

    #[test]
    fn compat_types_and_constraints() {
        // TIMESTAMPTZ 别名 + 数组 + 外键 + CHECK + DEFAULT 函数
        let s = parse(
            b"CREATE TABLE IF NOT EXISTS t (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                uid UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                tags TEXT[] DEFAULT '{}',
                j JSONB NOT NULL DEFAULT '{}'::jsonb,
                n INT,
                UNIQUE(uid, n),
                CHECK (n > 0)
            )",
        )
        .unwrap();
        let SqlStmt::CreateTable { table, if_not_exists, schema } = s else { panic!() };
        assert_eq!(table, "t");
        assert!(if_not_exists);
        assert_eq!(schema.columns.len(), 6);
    }

    #[test]
    fn compat_create_index_extension_drop() {
        let s = parse(b"CREATE INDEX IF NOT EXISTS idx_t ON t (uid) WHERE n > 0").unwrap();
        assert!(matches!(s, SqlStmt::CreateIndex { table, cols, if_not_exists: true }
            if table == "t" && cols == vec!["uid"]));

        let s = parse(b"CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"").unwrap();
        assert!(matches!(s, SqlStmt::DdlStub));

        let s = parse(b"DROP TABLE IF EXISTS t CASCADE").unwrap();
        assert!(matches!(s, SqlStmt::DropTable { ref table } if table == "t"));

        let s = parse(b"ALTER TABLE t ADD COLUMN IF NOT EXISTS is_begin BOOLEAN NOT NULL DEFAULT false")
            .unwrap();
        assert!(matches!(s, SqlStmt::AlterTable { if_not_exists: true, .. }));
    }

    #[test]
    fn compat_split_sql_statements() {
        let parts = split_sql_statements(
            "CREATE TABLE t (a INT); SELECT ';' AS x; -- 注释; 分号\nSELECT 2;",
        );
        assert_eq!(parts.len(), 3, "{parts:?}");

        // dollar-quote 内分号不分割
        let parts = split_sql_statements("CREATE FUNCTION f() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql; SELECT 1;");
        assert_eq!(parts.len(), 2, "{parts:?}");

        // 双引号内分号
        let parts = split_sql_statements("SELECT \"a;b\" FROM t; SELECT 1");
        assert_eq!(parts.len(), 2, "{parts:?}");
    }

    #[test]
    fn create_errors() {
        assert!(parse(b"CREATE TABLE t (a INT)").unwrap_err().contains("PRIMARY KEY"));
        assert!(
            parse(b"CREATE TABLE t (a INT PRIMARY KEY, b INT PRIMARY KEY)")
                .unwrap_err()
                .contains("multiple")
        );
        assert!(
            parse(b"CREATE TABLE t (a INT PRIMARY KEY, INDEX(zzz))")
                .unwrap_err()
                .contains("unknown column")
        );
        assert!(parse(b"CREATE TABLE t (a WAT PRIMARY KEY)").unwrap_err().contains("unknown type"));
    }

    #[test]
    fn insert_roundtrip() {
        // 大小写不敏感 + 引号转义 + 负数/浮点/NULL
        let s = parse(b"insert into t (id, name, score) values (-5, 'it''s ok', NULL)").unwrap();
        let SqlStmt::Insert { table, cols, rows } = s else { panic!() };
        assert_eq!(table, "t");
        assert_eq!(cols, vec!["id", "name", "score"]);
        assert_eq!(
            rows,
            vec![vec![
                SqlValue::Int(-5),
                SqlValue::Str(b"it's ok".to_vec()),
                SqlValue::Null,
            ]]
        );
        // 无列清单
        let s = parse(b"INSERT INTO t VALUES (1, 2.5)").unwrap();
        let SqlStmt::Insert { cols, rows, .. } = s else { panic!() };
        assert!(cols.is_empty());
        assert_eq!(rows, vec![vec![SqlValue::Int(1), SqlValue::Float(2.5)]]);
        // ⭐ S1: 多行 VALUES
        let s = parse(b"INSERT INTO t (a) VALUES (1), (2), (3)").unwrap();
        let SqlStmt::Insert { rows, .. } = s else { panic!() };
        assert_eq!(rows.len(), 3);
        assert!(parse(b"INSERT INTO t VALUES (1), (2, 3)").unwrap_err().contains("inconsistent"));
        // 列数不符
        assert!(parse(b"INSERT INTO t (a) VALUES (1, 2)").is_err());
    }

    #[test]
    fn select_roundtrip() {
        let s = parse(b"SELECT * FROM t WHERE a = 1 AND b >= 2.5 AND c < 'x' LIMIT 10").unwrap();
        let SqlStmt::Select { table, items, conds, limit, order, offset, group_by, having } = s else { panic!() };
        assert!(order.is_empty() && offset.is_none() && group_by.is_empty() && having.is_true());
        assert_eq!(table, "t");
        assert!(items.is_empty(), "* = 全列");
        assert_eq!(limit, Some(10));
        let cj = conds.as_conjuncts().unwrap();
        assert_eq!(cj.len(), 3);
        assert_eq!(cj[0], &Cond { col: "a".into(), op: CmpOp::Eq, val: SqlValue::Int(1), set: vec![] });
        assert_eq!(cj[1].op, CmpOp::Ge);
        assert_eq!(cj[2], &Cond { col: "c".into(), op: CmpOp::Lt, val: SqlValue::Str(b"x".to_vec()), set: vec![] });
        // 无 WHERE / 无 LIMIT
        let s = parse(b"SELECT * FROM t").unwrap();
        let SqlStmt::Select { conds, limit, .. } = s else { panic!() };
        assert!(conds.is_true());
        assert_eq!(limit, None);
    }

    #[test]
    fn select_errors() {
        // ⭐ O1: 投影列
        let s = parse(b"SELECT a, b FROM t WHERE a = 1").unwrap();
        let SqlStmt::Select { items, .. } = s else { panic!() };
        assert_eq!(items, vec![SelectItem::Col { name: "a".into(), alias: None }, SelectItem::Col { name: "b".into(), alias: None }]);
        // ⭐ S2: 新算子/子句
        let s = parse(b"SELECT COUNT(*) FROM t WHERE a IN (1, 2, 3)").unwrap();
        let SqlStmt::Select { items, conds, .. } = s else { panic!() };
        assert_eq!(items, vec![SelectItem::Agg { func: AggFn::Count, arg: None, distinct: false, alias: None }]);
        let cj = conds.as_conjuncts().unwrap();
        assert_eq!(cj[0].op, CmpOp::In);
        assert_eq!(cj[0].set.len(), 3);
        let s = parse(b"SELECT * FROM t WHERE a BETWEEN 1 AND 5 AND b != 'x'").unwrap();
        let SqlStmt::Select { conds, .. } = s else { panic!() };
        let cj = conds.as_conjuncts().unwrap();
        assert_eq!(cj.len(), 3, "BETWEEN desugar 成 Ge+Le");
        assert_eq!(cj[0].op, CmpOp::Ge);
        assert_eq!(cj[1].op, CmpOp::Le);
        assert_eq!(cj[2].op, CmpOp::Ne);
        let s = parse(b"SELECT * FROM t WHERE c LIKE 'ab%' ORDER BY a DESC, b LIMIT 3 OFFSET 6").unwrap();
        let SqlStmt::Select { conds, order, limit, offset, .. } = s else { panic!() };
        let cj = conds.as_conjuncts().unwrap();
        assert_eq!(cj.len(), 2, "LIKE 前缀 desugar 成 Ge+Lt");
        assert_eq!(cj[0].val, SqlValue::Str(b"ab".to_vec()));
        assert_eq!(cj[1].val, SqlValue::Str(b"ac".to_vec()));
        assert_eq!(order, vec![("a".into(), true), ("b".into(), false)]);
        assert_eq!((limit, offset), (Some(3), Some(6)));
        assert!(parse(b"SELECT * FROM t WHERE c LIKE '%ab'").is_err(), "仅前缀模式");
        assert!(parse(b"SELECT * FROM t WHERE a IN ()").is_err());
        // ⭐ S1: DML 解析
        let s = parse(b"DELETE FROM t WHERE a = 1").unwrap();
        assert!(matches!(s, SqlStmt::Delete { .. }));
        let s = parse(b"UPDATE t SET a = 1, b = 'x' WHERE c > 2").unwrap();
        let SqlStmt::Update { sets, conds, .. } = s else { panic!() };
        assert_eq!(sets.len(), 2);
        assert_eq!(conds.as_conjuncts().unwrap().len(), 1);
        assert_eq!(parse(b"DROP TABLE t").unwrap(), SqlStmt::DropTable { table: "t".into() });
        assert!(parse(b"SELECT * FROM t WHERE a = NULL").unwrap_err().contains("NULL"));
        assert!(parse(b"SELECT * FROM t WHERE a ! 1").is_err());
        assert!(parse(b"SELECT * FROM t LIMIT x").is_err());
        assert!(parse(b"SELECT * FROM t garbage").unwrap_err().contains("trailing"));
        assert!(parse(b"SELECT * FROM t WHERE name = 'unterminated").is_err());
    }

    // ⭐ P1: 预处理占位符与绑定
    #[test]
    fn prepared_params() {
        // ? 按序编号
        let (s, n) = parse_prepared(b"INSERT INTO t (a, b) VALUES (?, ?)").unwrap();
        assert_eq!(n, 2);
        let SqlStmt::Insert { ref rows, .. } = s else { panic!() };
        assert_eq!(rows[0], vec![SqlValue::Param(0), SqlValue::Param(1)]);
        // 绑定 roundtrip
        let bound = bind_params(&s, &[SqlValue::Int(7), SqlValue::Str(b"x".to_vec())]).unwrap();
        let SqlStmt::Insert { rows, .. } = bound else { panic!() };
        assert_eq!(rows[0], vec![SqlValue::Int(7), SqlValue::Str(b"x".to_vec())]);
        // $n 显式编号 (乱序引用)
        let (s, n) = parse_prepared(b"SELECT * FROM t WHERE a = $2 AND b = $1").unwrap();
        assert_eq!(n, 2);
        let bound = bind_params(&s, &[SqlValue::Int(10), SqlValue::Int(20)]).unwrap();
        let SqlStmt::Select { conds, .. } = bound else { panic!() };
        let cj = conds.as_conjuncts().unwrap();
        assert_eq!(cj[0].val, SqlValue::Int(20), "$2 → params[1]");
        assert_eq!(cj[1].val, SqlValue::Int(10));
        // IN / UPDATE sets 占位
        let (s, n) = parse_prepared(b"UPDATE t SET a = ? WHERE b IN (?, ?)").unwrap();
        assert_eq!(n, 3);
        let bound =
            bind_params(&s, &[SqlValue::Int(1), SqlValue::Int(2), SqlValue::Int(3)]).unwrap();
        let SqlStmt::Update { sets, conds, .. } = bound else { panic!() };
        assert_eq!(sets[0].1, SqlValue::Int(1));
        assert_eq!(conds.as_conjuncts().unwrap()[0].set, vec![SqlValue::Int(2), SqlValue::Int(3)]);
        // 个数不符 / 混用 / simple query 拒绝 / LIMIT 位置拒绝
        assert!(bind_params(&s, &[SqlValue::Int(1)]).unwrap_err().contains("missing"));
        assert!(parse_prepared(b"SELECT * FROM t WHERE a = ? AND b = $1").is_err());
        assert!(parse(b"SELECT * FROM t WHERE a = ?").unwrap_err().contains("prepared"));
        assert!(parse_prepared(b"SELECT * FROM t LIMIT ?").is_err(), "LIMIT 占位不支持");
    }
}
