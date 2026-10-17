# 修复计划+实施记录: leaf_update 慢路径缺空间检查 (新 P0-2)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P0-2【实证】
> 状态: **已完成**

## 1. 问题

`leaf_update_with` (crates/page/src/leaf.rs) 慢路径 (new_n != old_n) 直接
`copy_within` 移位后续 items, **无任何空间检查**:
- new_n > old_n 且页接近满时, 目的端越过 checkpoint 区 → 静默覆盖 (页损坏);
- 目的端越过 PAGE_SIZE → panic "dest is out of bounds".

探针实证 (release): 152 key 填满 leaf 页后, 把某 key 的 value 从 100B 改为
4000B (key+value 远在 KvLimits 4060 上限内, **完全合法**) → panic。
生产路径: btree_update / registry table_put 覆盖写 — 一次大 value 覆盖写即崩。

## 2. 修复

慢路径移位前加精确边界检查 (与 leaf_insert 的 2026-08-02 预检同模式):

```rust
let cp_size = checkpoint_area_size(idx.segments.len());
let new_free = free_off_before as isize + delta;
if new_free < 0 || new_free as usize + cp_size + PAGE_FOOTER_SIZE > PAGE_SIZE {
    return Err(PageError::PageFull);
}
```

返回 PageFull 与**既定契约一致**: btree_update 文档明示 "空间不够会返回
PageFull, caller 应改用 delete + insert"; registry.rs:474 已有错误路径
(recycle + 上抛). 即修复前是"崩溃/静默损坏", 修复后是"显式错误", 调用方
契约未变.

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| 修复前探针 (release) | ❌ panic @ leaf.rs:986 `dest is out of bounds` (152 key 填满 + value 100B→4000B) |
| 修复后同探针 | ✅ `Err: PageFull` 干净拒绝 |
| 新增回归 ×2 (`insert_atomicity_tests.rs`) | ✅ 近满页增长→PageFull 且页零改写+全部 key 可读旧值; 放得下的增长仍成功 (防过度拒绝) |
| `cargo test -p page` | ✅ 147 passed / 0 failed |
| `cargo test -p storage` (串行, 覆盖 registry/btree 全部 leaf_update 调用方) | ✅ 503 passed / 0 failed |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |

## 4. 遗留 (调用方增强, 非本条目范围)

btree_update 文档说 PageFull 时 "caller 应改用 delete + insert", 但
registry table_put 当前是直接上抛错误 — SQL 层 UPDATE 大 value 覆盖写会得到
显式错误而非自动 delete+insert 重试. 行为正确 (loud fail) 但可优化,
建议后续在 registry 层补 delete+insert fallback.
