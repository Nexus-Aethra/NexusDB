# 修复计划+实施记录: nlog 满队自旋致全局日志死锁 (新 P1-17)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-17
> 状态: **已完成**

## 1. 问题

`LogQueue::push` 队列满时对 Error/Warn **无限 `yield_now` 自旋**
("不可丢"). 后端 log 线程死亡/卡住 (fsync 挂死/panic) 时队列持续满 →
每条 Error/Warn 日志都自旋 → **拖死所有调用线程** (全局日志死锁连锁).

## 2. 修复

- 前 64 次 yield (瞬满的常见情形) 保留;
- 之后 **stderr 直写兜底** + `overflowed` 计数器 — 日志不丢 (可见性甚至
- 更强: 不依赖后端), 但调用方不再挂死;
- Debug/Trace 丢弃语义不变 (`dropped` 计数).

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| `urgent_log_overflow_is_bounded_not_infinite_spin` | ✅ 满队 Error push 快速返回 + overflowed 计数; Debug 仍丢弃 |
| 既有 `queue_backpressure_drops_low_levels_keeps_urgent` | ✅ 不受影响 |
| nlog 全量 | ✅ 7 passed / 0 failed |
| 全 workspace 串行 | ✅ 1100 passed / 0 failed, exit 0 |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |
