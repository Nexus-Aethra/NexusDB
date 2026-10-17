# 修复计划+实施记录: 退出路径跳过 flush 致每次关机丢数据 (新 P1-18)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-18
> 状态: **已完成**

## 1. 问题

main.rs 退出路径:
```rust
match Arc::try_unwrap(mgr) {
    Ok(m) => m.close()...,
    Err(_) => warn!("still referenced, skip close"),   // ← 必走这里
}
```
`sql_shared.cluster_ctl` 等处持有 mgr 的 Arc 克隆 → try_unwrap **必失败**
→ close() 被跳过 → shard 线程被进程退出硬杀 → nowchunks 脏数据
**每次关机都丢** (最近写入窗口).

## 2. 修复

- ShardManager 新增 `shutdown(&self)` (不要求 Arc 独占): ① 向所有 shard
  发 Flush 并等确认 (dirty data 落盘); ② 发 Shutdown 并等确认. 幂等
  (AtomicBool 防重复);
- `close(self)` 重构为 shutdown() + join (既有调用方语义不变);
- main.rs: 先 `mgr.shutdown()` (经 Arc), 再 try_unwrap+close 仅作线程
  join 的尽力清理 (flush 已完成, join 失败不影响 durability).

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| 反向验证 (临时改回旧路径: try_unwrap 失败即跳过关闭) | ❌ reopen 后 key 丢失 (wal=off, 唯一来源是 flush) |
| 修复后 `shutdown_with_held_arc_flushes_data` | ✅ held Arc 存在时 shutdown 仍落盘; reopen 后 100 key 全在 |
| `cargo test -p shard_manager` (串行) | ✅ 63 passed / 0 failed |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |
