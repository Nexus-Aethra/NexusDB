# 修复计划+实施记录: FK 级联 Decimal 编码不对称 (新 P0-4)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P0-4
> 状态: **已完成**

## 1. 问题

`sql_cascade.rs` 的 `pk_enc_to_sql` 把 DECIMAL 主键编码转 SqlValue 时直接用
`format!("{x}")` 输出定标整数裸串: Decimal(12345, scale=2) → "12345".

而 eval 侧比较 (`sql_eval.rs`) 走 `parse_decimal(s, sc)`: 无小数点的串按
"整数部分 + 小数补零"解释 — parse_decimal("12345", 2) = 1234500, 与真实值
12345 **相差 10^scale 倍** → IN 条件永不匹配 → `ON DELETE CASCADE` 对
DECIMAL 主键/外键**静默失效**, 孤儿行残留, 破坏引用完整性且无告警.

(报告原文说"匹配侧要求字符串含小数点" — 实测机制更精确: parse_decimal 不
要求小数点, 但会把无点串按整数×10^scale 解释, 同样导致永不匹配.)

## 2. 修复

编码侧改用与 parse_decimal 互为逆运算的规范格式化器 `render_decimal`
(sql_encode.rs:203, 结果集渲染已在用): Decimal(12345,2) → "123.45",
Decimal(-5,4) → "-0.0005". 一行变更, 语义对称.

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| 修复前 e2e (`mysql_foreign_key_cascade_decimal_pk`) | ❌ DELETE 父行后子表仍 3 行 (应 1) — 孤儿行静默残留 |
| 修复后同 e2e | ✅ scale=2 正值 + scale=4 负值 (-0.0005) 两级联均生效 |
| `cargo test -p network` (串行) | ✅ 212 passed / 0 failed |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |
