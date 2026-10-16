# 修复计划+实施记录: Read/Write future drop 后 DMA 竞争 (新 P1-16)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-16
> 状态: **已完成**

## 1. 问题

`Read`/`Write` future 持有调用方 buffer 的借用; Drop 时只发送**异步**
cancel (`cancel_submitted`) 即返回. 但内核在 cancel 生效前可能仍在 DMA
读写该 buffer — future 一 drop, 调用方即可复用/释放 buffer, 与在途 DMA
构成**数据竞争 (潜在 UB)**.

## 2. 修复

- 新增 `Scheduler::drain_cqe_until(ud, timeout)`: 有界等待指定 ud 的 CQE
  到达 (正常结果或 -ECANCELED); 等待期间同样 mark/唤醒其他 ud (与
  drain_completions/poll_cqe! 对齐);
- 新增 `IoRegistry::has_result` (非破坏性查询);
- `cancel_submitted` (所有 io future 的 Drop 路径) 改为: 发 cancel 后
  调用 `drain_cqe_until(ud, 50ms)` — DMA 确已结束才交还 buffer;
- scheduler 已销毁/borrow 冲突时 `with_current` 返回 None → 直接返回
  (io_uring 已回收, 无在途 DMA; 不退化为死锁).

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| `dropped_inflight_read_waits_bounded_and_drains_registry` | ✅ 在途 read future drop 有界返回 (<2s), registry 排空 (completed/cancelled 增加), buffer 交还可复用 |
| scheduler 全量 (24 个测试文件) | ✅ 通过 |
| 全 workspace 串行 | ✅ 1102 passed / 0 failed, exit 0 |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |
