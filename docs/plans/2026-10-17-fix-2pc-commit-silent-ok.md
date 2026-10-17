# 修复计划+实施记录: 2PC Commit 失败静默 Ok (新 P1-13)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-13
> 状态: **已完成 (commit 失败显式化); 超时部分由 P1-11 覆盖**

## 1. 问题

两半:
1. **Commit 失败静默 Ok**: create_db / create_table (sync+async 共 3 处 2PC)
   的 commit 循环均为 `if block_on_v2(fut).is_ok() { on_commit_ack }` — 失败
   被吞掉后仍 `Ok(())`. 部分 shard 提交成功即返回成功, 客户端以为事务
   完成, 实际元数据处于**跨 shard 不一致状态**. (讽刺的是 error.rs 里
   `CommitFailed` 变体早已存在却从未被使用.)
2. **无超时**: `check_timeouts` 是死代码 (仅测试可达) — prepare 挂起无
   状态机级超时.

## 2. 修复

1. 三处 commit 循环改为 `match`: 失败记录首个 `(shard, reason)`, 全部
   成功才返回 Ok; 有失败返回 `Err(ShardError::CommitFailed { op, shard_id,
   reason })` (启用既有变体). create_db 失败时**不刷新** db_view (db 处于
   不一致状态, 不暴露).
2. 超时部分: 由 P1-11 的 block_on_v2 有界等待覆盖 — sync 路径的 prepare/
   commit/abort 挂起现在最坏 30s 后返回 Err (走 PrepareFailed/CommitFailed
   显式失败), 不再永久挂起. `check_timeouts` 保留 (coordinator 状态机自洽
   性), 其"无超时"危害已消除.

## 3. 测试与验证

| 验证 | 结果 |
|---|---|
| `cargo test -p shard_manager` (串行, 含 two_pc_e2e 8 个用例) | ✅ 65 passed / 0 failed (happy path 行为不变) |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |

**测试设施缺口 (如实记录)**: commit 失败的注入需要"prepare 成功但 commit
失败"的中间态, 现有 harness 无 per-shard 故障注入 (无 kill 单 shard / 无
mock reply), 故该路径按构造验证 (三处循环同型修改 + 编译期类型保证) +
happy path 全量回归. 建议后续为 ShardManager 补 fault-injection 测试设施.

---

## 附: P1-14 (drain_async_flush 无限循环) — 同批次修复

**问题**: `drain_async_flush` 无限循环 — IO 持续故障时 async flush backlog
永远排不空 (完成事件永不到达), shard 线程卡死, 整个 shard 数据面停摆.

**修复**: 轮次 (1024) + 总时限 (10s) 双上限; 超限记 `nlog::error!` 并返回 —
backlog 留给下一轮 drain, 事件循环继续服务; Flush handler 后续的**同步**
flush 仍保证 durability (提前返回不丢数据).

**验证**: shard_manager 串行 65/0; clippy/fmt 全清. (IO 持续故障无注入设施,
按构造验证 — 与 P1-13 同一测试设施缺口.)
