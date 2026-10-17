# 修复计划+实施记录: parser 递归无深度限制致远程 DoS (新 P1-10)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-10
> 状态: **已完成**

## 1. 问题

WHERE 解析为递归下降且无深度计数: `parse_or→parse_and→parse_not→
parse_primary→(LParen)→parse_or`; 另有 NOT 前缀递归 (parse_not_expr /
UPDATE SET 的 atom) 与子查询递归 (parse_paren_subselect → parse_select).
数万个 `(` 或 `NOT` 即可打爆线程栈 (网络 worker 栈可能仅 2MB) —
**一条 SQL 的远程 DoS**.

## 2. 修复

P 新增两个计数器 + 四个递归点护栏:
- `depth` (MAX_PARSE_DEPTH=128): WHERE 括号 / NOT 前缀 — 帧小, 128 层
  对合法查询 (含 ORM 生成) 极端宽松;
- `subq_depth` (MAX_SUBQUERY_DEPTH=32): EXISTS/派生表子查询 — 每层跑
  完整 select 解析链, 帧代价高 (实测 128 层即可超 2MB 栈, 故单独更严);
- 超深 → 显式 Err ("nesting too deep"), 语句级失败, 不崩溃.

覆盖: parse_primary 括号 / parse_not_expr NOT / atom NOT (UPDATE SET) /
parse_paren_subselect (EXISTS + 派生表共用).

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| 200/5000/50000 层括号 | ✅ 显式报错 "too deep" (修复前 SIGABRT stack overflow) |
| 5000 层 NOT | ✅ 显式报错 |
| 2000 层子查询 | ✅ 显式报错 (实测 128 层即溢出 → 子查询上限定为 32) |
| 20 层括号 (合法量级) | ✅ 正常放行 |
| `cargo test -p network` (串行) | ✅ 217 passed / 0 failed |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |
