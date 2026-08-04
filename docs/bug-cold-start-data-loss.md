# 冷启动批量 INSERT 数据丢失 Bug 诊断报告

> 日期: 2026-08-02 | 状态: **已修复** | 严重性: P0

## 现象

全新数据目录（冷启动）下，首次批量 INSERT ≥7000 行，~2/3 概率**恰好丢失 32 行**。
偶发 ~1/15 概率触发 `bad page type at vpid 60: expected Leaf/Internal, got Meta`。

## 根因与修复

### 问题一：32 行丢失

**根因**：`leaf_insert` 中 `pre_split_segment` 先修改页数据（item 移位+重编码），然后 `leaf_push_back` 发现空间不足返回 `PageFull`。此时页数据已被修改但 `write_back` 被 `?` 跳过，导致 checkpoint 数组过时（`first_item_off` 全错）。后续 `leaf_split` 的 `PageIndex::load` 读到过时 checkpoint，用错误段边界分裂，静默丢 key。

**修复**（`crates/page/src/leaf.rs`）：在 `pre_split_segment` 之前加**空间预检**——估算新 item 大小 + 分裂后多出的 checkpoint entry，空间不足则**直接返回 PageFull**（页数据未修改，checkpoint 一致）。空间充足才执行 `pre_split_segment` + 插入。

```rust
let need_pre_split = idx.segments[cur_seg_idx].item_count >= MAX_PER_CHECKPOINT;
let est_item = key.len() + value.len() + 20;
let cp_growth = if need_pre_split { CHECKPOINT_SIZE } else { 0 };
let cp_size = checkpoint_area_size(idx.segments.len()) + cp_growth;
if free_off + est_item + cp_size + PAGE_FOOTER_SIZE > PAGE_SIZE {
    return Err(PageError::PageFull);  // 页数据未修改，安全退出
}
if need_pre_split {
    pre_split_segment(...)?;  // 空间够才执行
}
```

### 问题二：bad page type

**根因**：`maybe_periodic_flush` 每轮将驻留 chunk 快照入 write_queue。这些中间快照（4页/14页/59页）经 flush 管道后插入 `chunk_list`，与 swap 的完整数据（64页）产生时序竞态——旧快照在 `complete_flush` 时插入 chunk_list，遮蔽了尚在 flush 管道中的完整数据。读路径命中旧快照（page_idx 60 无数据→全零→误判为 Meta 类型）。

**修复**（`crates/storage/src/pager.rs`）：`maybe_periodic_flush` **不再将驻留 chunk 快照入 write_queue**。读路径优先命中 nowchunks，快照对读毫无价值；持久性由 WAL + swap（满 chunk）+ 显式 flush（shutdown）三重保证。

### 辅助修复

- `swap_full_chunk_to_write_queue` 时 `chunk_list.invalidate` 作废旧快照
- `complete_flush` 插入 chunk_list 前比较有效页数，防止旧版本覆盖新版本

## 验收

- **887/887** 全量测试通过（含 `stress_10000_rows`、`prefix_chaos_realkeys`）
- 冷启动 **10 轮 × 10000 行: 全部 OK**（修复前 ~2/3 轮丢 32 + 偶发 bad page type）

### 2026-08-03 独立复核（v8，确认彻底解决）

- **SQL 层冷启动 3 轮 × 10000 行**：每轮全新 `block_root` + 单条 `INSERT ... VALUES` 10000 行
  （wal=off / 3 shard / stdfs / chunk_cache=8）→ 写入后 COUNT 与**重启后** COUNT 均为 10000，无丢失
- storage 层 `stress_10000_rows`（5 轮 × 10000 行，乱序插入 + 双索引）全过
- page 层 `prefix_chaos_realkeys`（单/多 seed）全过
- `sql_e2e` 串行全绿；并行下 `mysql_join_est_threshold` 偶发失败为指标计数器并发干扰
  （测试注释明示需串行），串行单独运行通过，非回归

## 修改文件

| 文件 | 修改 |
|------|------|
| `crates/page/src/leaf.rs` | `leaf_insert` 空间预检 + `pre_split_segment` 条件执行 |
| `crates/storage/src/pager.rs` | `maybe_periodic_flush` 移除驻留 chunk 快照；`swap` 时 invalidate chunk_list；`complete_flush` 有效页数比较 |
