// ⭐ sqlparser_bridge 对比测试: 用 sqlparser-rs 映射的 SqlStmt 与手写 parser
// 结果对比, 验证映射层正确性 (接入 parse_prepared 前的门禁).
#[cfg(test)]
mod tests {
    use super::super::ast::SqlStmt;
    use super::super::parser::parse_prepared;
    use super::super::sqlparser_bridge::parse_select;

    fn run(sql: &str) {
        // 手写 parser (权威基准)
        let (hand, hand_n) = parse_prepared(sql.as_bytes()).unwrap_or_else(|e| {
            panic!("手写 parser 失败 [{sql}] => {e}");
        });
        // sqlparser 映射
        let mapped = parse_select(sql.as_bytes()).expect("bridge 不应返回 Err");
        match mapped {
            Some((m, _n)) => {
                let m = normalize(&m);
                let h = normalize(&hand);
                assert_eq!(m, h, "\nSQL: {sql}\n bridge={m:?}\n hand={hand:?}");
                println!("OK  [{sql}]\n  bridge={m:?}");
            }
            None => {
                // 映射层不支持 → 应回退手写 (本测试只断言"支持时的正确性")
                println!("SKIP (bridge 不支持) [{sql}]");
            }
        }
    }

    // 归一化: 忽略 limit_param/offset_param (映射层对字面量 limit 的占位差异)
    fn normalize(s: &SqlStmt) -> SqlStmt {
        match s {
            SqlStmt::Select {
                table,
                items,
                conds,
                limit,
                order,
                offset,
                group_by,
                having,
                ..
            } => SqlStmt::Select {
                table: table.clone(),
                items: items.clone(),
                conds: conds.clone(),
                limit: *limit,
                order: order.clone(),
                offset: *offset,
                group_by: group_by.clone(),
                having: having.clone(),
                limit_param: None,
                offset_param: None,
            },
            other => other.clone(),
        }
    }

    #[test]
    fn basic_select() {
        run("SELECT * FROM users");
        run("SELECT id, name FROM users");
        run("SELECT id, name FROM users WHERE id = 1");
        run("SELECT * FROM users WHERE id = 1 AND name = 'x'");
        run("SELECT * FROM users WHERE id > 5 LIMIT 10");
        run("SELECT * FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 5");
    }

    #[test]
    fn portal_select() {
        run("SELECT * FROM users WHERE 1=1 AND is_admin = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3");
        run("SELECT COUNT(*) FROM users WHERE 1=1");
        run("SELECT * FROM users WHERE username = $1");
        run("SELECT * FROM users WHERE 1=1 AND is_admin = $1");
    }

    #[test]
    fn agg_and_group() {
        run("SELECT COUNT(*) FROM stories WHERE status = 'active'");
        run("SELECT status, COUNT(*) FROM stories GROUP BY status");
        run("SELECT status, COUNT(*) FROM stories GROUP BY status HAVING COUNT(*) > 1");
    }

    #[test]
    fn in_and_not() {
        run("SELECT * FROM users WHERE id IN (1, 2, 3)");
        run("SELECT * FROM users WHERE id IN (1,2) AND name = 'a'");
    }
}
