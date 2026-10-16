# 修复计划+实施记录: reply bus 满时 shard 自旋停摆 (新 P1-12)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-12
> 状态: **已完成**

## 1. 问题

`TaskReplyBus::push` 在 ring 满时**无限 `yield_now` 自旋** ("不太可能,
8192 容量"). 当消费端 (worker) 阻塞 (如慢客户端 socket 背压) 时:
shard 线程 100% CPU 空转, 且整个 shard 数据面停摆 (该 shard 上所有
表的所有请求).

## 2. 修复

- push 改**有界自旋后退避**: 前 64 次 yield (瞬满的常见情形), 之后每轮
  `sleep(100µs)` — CPU 归零, worker drain 出空间后自然恢复 (轮询语义,
  无丢唤醒风险, 恢复延迟 ≤100µs);
- 新增 `full_waits` 计数器 + `full_waits()` getter: bus 满等待累计次数
  (消费端过慢的可观测信号);
- 新增 `with_capacity()` 构造器 (测试用小 ring).

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| `push_non_full_no_wait` | ✅ 未满不计等待 |
| `push_full_backs_off_instead_of_spinning` | ✅ 满时记录退避且 push 挂起; drain 后恢复入队 |
| `cargo test -p shard_manager` (串行) | ✅ 65 passed / 0 failed |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |
