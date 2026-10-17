# 修复计划+实施记录: WAL replay 吞全部错误 (新 P1-2)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-2
> 状态: **已完成**

## 1. 问题 (两处静默)

1. **段级**: `let Ok(data) = std::fs::read(seg) else { continue }` — 段文件
   被 purge/损坏时, 该段全部 WAL 记录丢失, 无日志无计数.
2. **记录级**: `Err(_) => skipped += 1` — put/delete/ensure_table 的**一切**
   错误 (IO/页损坏/解码失败) 都被计入 skipped 并归因 "stale db/table".
   真实故障被静默吞掉 → 重启后数据丢失且无迹可查.

## 2. 修复

- 段级: 读失败 → `errors += 1` + eprintln (段路径+错误);
- 记录级: 分类 — `DbNotFound`/`TableNotFound` (drop 后残留, 预期内) 计
  skipped; **其他错误** → `errors += 1` + eprintln 详情 (前 10 条防刷屏);
- 摘要行区分三者: `applied / skipped (stale) / ERRORS`;
- 分类逻辑提取为 `is_stale_replay_err()` 并加单测 (5 类真实错误必须判非
  陈旧).

保留 boot 容错 (不因单条错误拒绝启动), 但故障全部显式可见. 若需更强
语义 (损坏即拒绝启动), 可在后续将 errors>0 改为返回 Err.

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| `stale_replay_err_classification` 单测 | ✅ DbNotFound/TableNotFound → 陈旧; Io/TableOp/WrongType/BadValueSize/NonUtf8 → 必须记录 |
| `cargo test -p storage` (串行, 含 wal_recovery_tests 既有 5 个 replay 用例) | ✅ 505 passed / 0 failed |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |
