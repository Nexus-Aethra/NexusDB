# 修复计划+实施记录: 语句执行无 panic 隔离 (新 P1-9)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-9
> 状态: **已完成**

## 1. 问题

sql_join.rs 等处有 43 个 unwrap/expect (JOIN finish 路径集中在列解析/
段表). worker 是**单线程事件循环** — 任意一处 panic 杀死线程即拖死其上
全部连接, 等效全服 DoS.

实测: 8 种畸形 JOIN 输入 (未知表限定/未知列/错误 ORDER BY 目标等) 目前
都被前置校验拦截为干净 ERR (1054/1064/1105), 未见 panic — 但这些
unwrap 没有任何机制保证未来的代码路径不触达.

## 2. 修复 (审计处方的"每查询 catch_unwind")

- `sql_dispatch_stmt` (全部协议/全部语句的唯一 SQL 入口) 外包
  `guard_stmt_panic` (catch_unwind + AssertUnwindSafe);
- panic → 回 ERR "internal error: statement execution failed" + `close_after_flush`
  (状态可能不完整, 断连该连接不复用) — **同 worker 其他连接不受影响**;
- guard 原语独立成函数并单测 (panic→Err / 正常→Ok).

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| `guard_stmt_panic_isolates_panic` 单测 | ✅ panic 捕获为 Err; 正常值透传 (hook 静音) |
| 畸形 JOIN 探针 (8 种) | ✅ 全部干净 ERR, 无连接中断 |
| `cargo test -p network` (串行) | ✅ 222 passed / 0 failed |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |

注: 这是防御性兜底 — 不改变任何现有行为 (无 panic 时零开销), 但把"未来
任意 panic"的爆炸半径从"整个 worker 的全部连接"收敛到"单连接".
