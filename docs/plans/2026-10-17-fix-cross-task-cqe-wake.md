# 修复计划+实施记录: poll_cqe 消费他人 CQE 不唤醒 (新 P1-15)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-15
> 状态: **已完成**

## 1. 问题

`io_ops.rs` 的 `poll_cqe!` 批量扫描 CQ 时, 会把**所有** CQE (含其他 task
的) mark 进 registry, 但只 take 自己的结果 — 不唤醒其他 task 的 owner.
而 `drive_once` (run() 的步进单元) 是"先 poll ready、后 drain CQ": 两轮
之间到达的 CQE 都会被下一个被 poll 用户的扫描**消费掉**, 后续
`drain_completions` 看到空 CQ → owner 永远 park → **永久死锁**.

## 2. 修复

与 `drain_completions` 的入队路径对齐: poll_cqe! 扫描时, 对每个 mark 成功
的 ud 取 `inner_peek(ud).slot_id`, `drop(cq)` 后统一 `wake_slot()` 入
ready (新增 `Scheduler::wake_slot`). 借用规则: cq 持有 `s.ring` 借用,
故循环内先收集 slot_id, 释放后再入队.

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| 白盒回归 `cross_task_cqe_mark_enqueues_ready` | ✅ 只 poll B (跳过 A), B 的第二次 poll 扫描消费 A 的 CQE → A 入 ready; 继续驱动 A 完成 |
| **反向验证** (仅暂存 io_ops.rs 修复) | ❌ 断言失败 "A 未入队 ready" — 精确捕获 bug |
| scheduler 全量 | ✅ 通过 |
| 全 workspace 串行 | ✅ 1101 passed / 0 failed, exit 0 |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |

注: 当前 drive_until_idle 每轮重 poll 全 pool 会掩盖该死锁 (marked task
下一轮即被重 poll), 故死锁为潜伏态; 本修复使唤醒正确性不依赖该掩盖,
且消除无谓的全量重 poll 依赖.
