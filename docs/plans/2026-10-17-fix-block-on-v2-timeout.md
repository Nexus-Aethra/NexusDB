# 修复计划+实施记录: block_on_v2 无限 park (新 P1-11)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-11
> 状态: **已完成**

## 1. 问题

`block_on_v2` (shard_manager/src/reply.rs) 末段无限 `thread::park()`:
shard 丢回复或线程无响应时, 调用方**永久挂起**, 无超时无断连感知.

实测危害 (本轮修复期间 4 次命中, gdb 栈取证): 全量测试跑 15-20 分钟后,
`ShardManager::create_db → refresh_db_view → block_on_v2` 随机挂死
(测试 setup 阶段, 所有 shard 线程空闲 poll, 请求永无回复) — 与审计
P1-11/P1-15/"双 inbox 无全序"同族. 单次挂起浪费 45+ 分钟 CI 时间.

## 2. 修复

`block_on_v2` 末段改为**有界等待**: 分段 `park_timeout(250ms)`, 到
deadline 返回 `Err(ShardErrorKind::ReplyTimeout)` (新增变体) 而非无限
挂起. 默认 30s (正常回复为亚毫秒级, 留有 4+ 个数量级余量), 可用
`NEXUSDB_SHARD_REPLY_TIMEOUT_SECS` 环境变量覆盖 (0=不限, 兼容旧行为).

- 迟到回复无害: ReplySlot::drop 释放 ptr/waiter/waker, 无泄漏;
- 全部 18 个调用点本就处理 Result (`if let Ok` / `.is_ok()` / match),
  Err 路径已存在 (如 create_db 2PC 失败走 abort);
- 唤醒语义不变 (park token + double-check 保持).

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| `timeout_returns_error_instead_of_hanging` 单测 | ✅ 永不回复的 slot 120ms 后返回 ReplyTimeout (不再挂死) |
| `reply_within_timeout_ok` 单测 | ✅ 正常回复不受影响 |
| `cargo test -p shard_manager` (串行) | ✅ 63 passed / 0 failed |
| `cargo test -p network` (串行) | ✅ 214 passed / 0 failed, exit 0 |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |

## 4. 顺带发现 (未修, 建议立项)

**ShardManager 没有 Drop**: 只 `drop(mgr)` 而不 `close()` 时, 3 个 shard
线程永不退出, 以 10ms 周期永久 poll. 测试套件中每个测试泄漏 3 线程 —
实测挂起进程累积到 68 线程/22 个 ShardManager, 持续烧 CPU 并放大上述
竞态窗口. 建议: 实现 Drop 时向各 shard 发 Shutdown (close() 保留 join).
