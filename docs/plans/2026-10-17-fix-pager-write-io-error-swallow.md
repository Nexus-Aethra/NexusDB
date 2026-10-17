# 修复计划+实施记录: 驻留兜底吞 IO 错误致全零覆盖 (新 P1-1)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-1
> 状态: **已完成**

## 1. 问题

`pager_write.rs:157` 驻留兜底 (chunk 不在 nowchunks 时从磁盘加载完整视图):
```rust
Err(_) => vec![0u8; CHUNK_SIZE],
```
把**一切** IO 错误都当作"新 chunk"退化为全零。若 chunk 已存在但读取失败
(权限/硬件/其他 IO 故障), 全零 chunk 会随后续 flush **落盘覆盖历史数据** —
静默数据毁灭, 且无日志无指标.

## 2. 修复

提取分类器 `chunk_absent(e)`, 仅两种情况退化全零 (新 chunk 的正确初始内容):
- `NotFound` — .block 文件不存在 (StdFs/io_uring 均产该 kind);
- `UnexpectedEof` — 文件比目标 chunk 偏移短 (尾部未写区域).

其他 IO 错误 → `return Err(e)` 显式失败 (submit 本就返回 io::Result, 传播零成本).
全库 grep 确认 `vec![0u8; CHUNK_SIZE]` 无其他吞错点 (其余为正常缓冲分配).

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| `chunk_absent_classification` 单测 | ✅ NotFound/UnexpectedEof → true; PermissionDenied/Other/TimedOut/Interrupted → false |
| `cargo test -p storage` (串行) | ✅ 504 passed / 0 failed (reopen 首写/swap 后重建等依赖 NotFound→零值 的路径全部保持) |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |
