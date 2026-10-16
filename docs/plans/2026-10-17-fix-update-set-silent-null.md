# 修复计划+实施记录: UPDATE SET 解析失败静默置 NULL (新 P1-7)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-7
> 状态: **已完成**

## 1. 问题 (比审计描述更严重, 共三处静默路径)

审计原文指向 `parser_where.rs` 函数分支的 `_ => Ok(Lit(Null))`. 实测发现:

1. **该函数分支是死代码**: `atom()` 在 Ident 分支里用 `p.peek() == LParen`
   判断函数调用, 但 `peek()` 返回的是**未消费的当前 token** (即 Ident 本身),
   恒为 false → 整段从未执行. 连带后果: 文档宣称支持的 `SET c = NOW()`
   实际报 "unexpected end of statement" (括号配平从 Ident 开始错位).
2. **真正的静默 NULL 入口**: `parse_update_set_value` 的
   `let left = match atom(p) { Ok(e) => e, Err(_) => Lit(Null) }` —
   atom 的任何解析错误都被吞掉, 列被静默写 NULL (不可逆数据破坏).
3. **另两处同族**: atom 兜底臂 `_ => Ok(Lit(Null))` (`SET c = ,` 写 NULL);
   算术循环 `let Ok(right) = atom(p) else { break }` (右侧解析失败时**静默
   丢弃右侧表达式**, `SET c = a + UPPER(b)` 变成 `c = a`).

## 2. 修复

1. 函数分支改用 `peek2_is_lparen()` (parser 既有 helper, SELECT version()
   同款) 并补上被遗漏的"消费函数名"步骤 → `NOW()`/`CURRENT_TIMESTAMP()` 等
   恢复工作; 未知函数返回明确错误 `unknown function 'f' in UPDATE SET`.
2. `let left = atom(p)?;` — 解析错误传播, 语句级失败 (无部分应用).
3. atom 兜底臂改为 `Err(unexpected token ...)`.
4. 算术循环右侧 `atom(p)?` 传播错误.

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| 修复前 `SET note = UPPER(name)` | 报 "trailing tokens" (函数分支死代码) |
| 修复后同语句 | ✅ `unknown function 'upper' in UPDATE SET expression` |
| 修复前 `SET note = NOW()` (文档宣称支持) | ❌ "unexpected end of statement" |
| 修复后同语句 | ✅ Ok (解析为 Lit(Now)) |
| 多列 SET 混入未知函数 / 畸形值 / 算术右侧夹带未知函数 | ✅ 全部显式报错且**无部分应用** (数据逐字节不变) |
| `cargo test -p network` (串行) | ✅ 213 passed / 0 failed |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |
