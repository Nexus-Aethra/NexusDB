# 修复计划+实施记录: fix-mate --auto 硬编码布局破坏 slot (新 P1-20)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-20
> 状态: **已完成**

## 1. 问题

`fix-mate --auto` 的三处修复全部硬编码 `PidLocation { file_id: 1,
chunk_idx: 0, .. }`:
1. **孤儿页重定向**: 扫描以**文件内页序号** (0..640) 寻址, 但写回时
   `page_idx = matches[0]` (可达 639) + `chunk_idx = 0` — chunk 0 只有 64
   个页, 修出的 pid 指向**不存在的页**;
2. **file_id 修正**: 同样硬编码 1 (非默认布局, 如 compact 后的 file_id≠1,
   直接写错);
3. **FREED 标记**: 顺手把坐标改成 (1,0).

后果: 非默认布局 / 孤儿页在 chunk>=1 时, **修复工具本身把元数据写坏** —
救援工具造成二次伤害.

## 2. 修复

- 新增 `split_page_idx(file_page_idx) -> (chunk_idx, page_idx)`: 文件内页
  序号 ÷64 / %64 换算;
- 孤儿页重定向与 file_id 修正均用换算值 + **真实 block file_id**
  (来自 `shard.block_files.first().file_id`, 不再硬编码 1);
- FREED 标记保留原坐标 (位置无语义, 但不顺手改坏).

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| `auto_repair_converts_file_page_idx_and_uses_real_file_id` | ✅ 孤儿页在文件页 71 (chunk 1 page 7), file_id=7 → 修复为 (7, chunk 1, page 7); 旧实现错写 (1, 0, 71) |
| `auto_repair_marks_freed_without_rewriting_coordinates` | ✅ 无孤儿副本 → FREED 且保留原坐标 (3,2,40) |
| scanner 全量 (68 测试) | ✅ 通过 |
| 全 workspace 串行 | ✅ 1099 passed / 0 failed, exit 0 |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |
