# 修复计划: internal_insert 缺空间预检导致 internal 页静默损坏

> 日期: 2026-10-16 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P0-1【实证】
> 状态: 已设计, 待实现 | 严重性: P0 (静默丢数据)

## 1. 问题

`internal_insert` (`crates/page/src/internal.rs:101`) 在调用 `pre_split_segment`
(`crates/page/src/index.rs:275`) 时**没有任何空间预检**:

1. `pre_split_segment` 把段内 mid item 重编码为 `shared_prefix_len=0` (full key)。
   对长共享前缀 key (如 600B 共同前缀), 该 item 膨胀量可达整个 key 长度。
2. 重编码后执行**无边界检查的** `page.copy_within(...)` (`index.rs:327`) 并推进
   `free_off`。
3. 之后 `internal_push_back` (internal.rs:713 Step 3) 才做空间检查, 可能返回
   `PageFull`; 该错误路径上 `idx.write_back(page)` (internal.rs:213) 不会执行。

即: **页字节已被改写, 但 checkpoint 数组是旧的**——与 2026-08-02 冷启动丢数据
事故 (`docs/bug-cold-start-data-loss.md`) 完全同一根因。当年只修了 leaf_insert
(`leaf.rs:274-292` 加 est 预检), internal_insert 漏修 ("修复不对称"模式)。

## 2. 实证 (2026-10-16, /tmp 独立探针, release 模式)

| 场景 | 现象 |
|---|---|
| 500B/700B/750B 共享前缀 | `copy_within` panic: `dest is out of bounds` (index.rs:327) —— 合法插入打崩进程 |
| 200B/600B 共享前缀 | 返回 `PageFull` 但页已污染: free_off=16373 越过 checkpoint 区边界 16368, `PageIndex::load` 失败 (`page too small for key`); 路由查询从 `Some(child)` 变 `None` —— **已可达数据静默变不可达** |
| 300B/400B 共享前缀 | 干净拒绝 (该次插入未触发 pre_split, push_back 自身检查拦下) —— 反证污染专属于 pre_split 路径 |

核心不变量被违反: **失败的插入必须保持页字节不变** (探针以逐次快照对比确认被改写)。

## 3. 爆炸半径

`crates/storage/src/btree.rs:668-696`: leaf 分裂后 separator 经 `internal_insert`
提升到父 internal 页; 收到 `PageFull` 后调用方**立即 `internal_split` 并
`batch.add` 写回**该页。因此页级污染会被 split 放大并**持久化**:
- panic 场景: 插入路径崩溃 (worker 无 panic 隔离, 见 P2-7)
- 静默场景: 损坏页被拆分写回 → B-tree 子树永久错路由 → GET 静默丢数据

## 4. 修复设计 (三层, 均在 crates/page 内)

### L1 失败快速预检 (对齐 leaf.rs:274-292 的既定修复模式)

`internal_insert` 在 `pre_split_segment` 之前, 用**只读**方式精确计算 pre_split
字节增量 `delta_mid` (走到 mid item, 按 full key 重编码求长, ≤32 次解码),
连同新增 checkpoint entry (4B) 与估算的新 item 上界做边界检查:

```
free_off + delta_mid + est_new_item + checkpoint_area_size(段数+1) + PAGE_FOOTER_SIZE <= PAGE_SIZE
```

不满足则直接 `Err(PageFull)`, 页数据未修改。est_new_item 取
`sep_key.len() + 8 + 16` (vpid + shared/varint/kind 余量) 作上界;
精确性不由它保证 (见 L2), 它只负责廉价地拦掉常见案例。

### L2 快照回滚 (正确性保证, 不依赖估算)

pre_split 路径上先存 16KiB 页快照; 后续任一步骤返回 Err 时恢复快照再返回错误。
恢复强不变量 "失败的操作不改变页字节", 与历史修复文档的意图一致。
开销: 仅 pre_split 路径 (约 1/32 次插入) 一次 16KiB memcpy, 相对已有的
O(32) 段内扫描可忽略。`idx` 仅存内存, 出错即丢弃, 无需回滚。

备选 (更低成本): 错误路径上补 `idx.write_back(page)` (duplicate-key 分支
internal.rs:180-190 已是这么做的), 页进入"已 pre_split 的有效态"; 但该不变量
弱于 L2, 且边界安全仍依赖 L1/L3。建议 L2 为主。

### L3 pre_split_segment 自身硬化 (C3 模式: debug_assert → 硬错误)

在 `index.rs` 的 `copy_within` 之前校验目标区间不越过
`CHECKPOINT_AREA_END - checkpoint_area_size(段数+1)`, 越界则**在任何字节被修改
前**返回 `Err(PageFull)`。使 pre_split 对任何调用方 (含未来新增) 自防御,
从机制上杜绝同类复发。leaf/internal 两个调用方同时受益。

### 附带 (低风险, 同模式)

`leaf_insert` 存在同样结构洞 (est 预检是启发式, 仍可能 pre_split 后 push_back
失败): 对 leaf_insert 施加同样的 L2 快照回滚。L3 已覆盖其 pre_split。

## 5. 测试

1. `internal_insert` 填页至 PageFull 后, 页字节与插入前快照逐字节相同 (600B 前缀场景)
2. 500B/700B/750B 前缀插入不 panic, 返回 Err
3. PageFull 后全部已插入 key 的路由结果与失败前一致 (探针 route_before/after 场景)
4. `pre_split_segment` 对近满页直接返回 Err(PageFull) 且页不变 (L3 单测)
5. leaf_insert 同型回归 (L2 附带)
6. 回归套件: page crate 全量 + storage `stress_10000_rows` / `prefix_chaos_realkeys`

## 6. 验收

- `cargo test -p page` 全绿; `cargo test -p storage` 全绿 (串行)
- `cargo clippy --workspace --all-targets -- -D warnings` 0 警告; `cargo fmt --check` 0 差异
- 探针场景 (500B-750B 前缀) 从 "panic/页损坏" 变为 "干净 PageFull"

## 7. 影响面

仅 `crates/page` (internal_insert / pre_split_segment / leaf_insert) + 测试。
对外行为变化: 原先 "PageFull + 页污染" 或 panic → 干净的 PageFull (页原样)。
btree.rs 的 PageFull 分裂路径无需改动, 修复后其输入即变为可信。
