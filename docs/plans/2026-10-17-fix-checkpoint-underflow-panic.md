# 修复: read_checkpoint 损坏 count 下溢 panic (10-16 报告 P2)

> 日期: 2026-10-17 | 状态: **已完成**

## 问题

`read_checkpoint` 的 `hdr_off - (i+1)*CHECKPOINT_SIZE` 在 i 过大时
usize 下溢 (release 下回绕成巨大下标再越界 panic). `PageIndex::load`
直接 `for i in 0..cp_count` 而 cp_count 来自磁盘 — 损坏页
(checkpoint_count >= 4093) 即可打崩进程.

## 修复

1. `PageIndex::load`: 读 cp_count 后校验容量上界
   (`8 + count*4 <= CHECKPOINT_AREA_END - PAGE_HEADER_SIZE`),
   超限返回 `Err(PageError::InvalidHeader)`;
2. `read_checkpoint`: checked 算术兜底, 越界返回空 entry 而非 panic.

## 测试 (反向验证通过: 修复前两个用例均 panic)

| 用例 | 结果 |
|---|---|
| `corrupt_checkpoint_count_rejected_not_panicked` | ✅ cp_count=4093/u16::MAX → InvalidHeader |
| `read_checkpoint_huge_index_no_panic` | ✅ i=usize::MAX/8 → 空 entry |
| page 全量 / storage 串行 | ✅ 149/0, 507/0 |
