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

---

## 8. 实施记录 (2026-10-16, 已完成)

### 8.1 实际改动 (3 个文件 + 2 个测试文件)

| 文件 | 改动 |
|---|---|
| `crates/page/src/index.rs` | `pre_split_segment` 重构为三件套: `plan_pre_split` (只读演算) / `pre_split_fits` (边界判定) / `apply_pre_split` (落字节); `pre_split_segment` = plan + fits(L3 硬自检) + apply, **在任何字节被修改前**拒绝放不下的分裂 |
| `crates/page/src/internal.rs` | L1: pre_split 前 plan + `pre_split_fits` 精确预检, 不足直接 `PageFull` (页不动); L2: pre_split 路径存 16KiB 快照, push_back 失败即回滚, 保证"失败的插入不改变页" |
| `crates/page/src/leaf.rs` | L2 同型快照回滚 (est 预检是启发式, 兜底 est 之后的 push_back 失败窗口) |
| `crates/page/tests/insert_atomicity_tests.rs` | 新增 5 个回归测试 (见 8.3) |
| `crates/storage/tests/internal_split_long_prefix_regression.rs` | 新增存储级 e2e 回归 (600B 前缀 × 8000 行 × 2KB value, 全量读回 + flush + reopen 后全量读回) |

### 8.2 实施中发现并修复的第二个潜伏 bug

`internal_split` Step 5 对左页重建 cp[0] 时硬编码 `item_count = left_kc + 1` ("含哨兵"),
但**无哨兵页** (上一次 split 产生的 right 页再分裂时的左半) 会多数一个**幽灵 item**
(探针实测: cp 总数 199 vs 实际 item 198)。后果: 段内 walk 越过 free_off 踩零区,
表现为 push_back `copy_within` 的 "slice index starts at X but ends at Y" panic
或静默错路由。已按 `has_sentinel` 修正计数。leaf_split 无此问题 (leaf 页必有哨兵)。

该 bug 在修复前**不可达**: 进程在 internal_insert 的 pre_split 阶段就会 panic,
永远走不到 internal_split。属"修复暴露的下一层腐烂", 与审计报告"修复不对称/
同类问题仍有潜伏实例"的判断互相印证。

**实施教训 (重构 plan/apply 的坑)**: plan 阶段在**未移位的原页**上演算, k+1 必须按
原位置 `mid_item_off + mid_old_n` 解码; 若误用移位后位置 `mid_item_off + mid_new_n`
会解码到错误字节, apply 把错误 k+1 写回即毁掉 item 链。该回归由探针在 5 分钟内捕获,
最终形态以 `K1Plan { k1_off_orig, k1_off }` 两个字段区分"解码位置"与"落地位置"。

### 8.3 验证结果 (全部实测)

| 验证 | 结果 |
|---|---|
| 修复前复现 (存储级 e2e, 未修代码) | ❌ 0.12s panic @ `index.rs:327` `dest is out of bounds` |
| 修复后存储级 e2e | ✅ 1.44s 通过 (8000 行全量读回 + reopen 后全量读回) |
| 独立探针 (release, 11 种前缀长度 0–750B) | ✅ 全部干净 `PageFull`: 无 panic / 页零改写 / free_off 不越界 / `PageIndex::load` OK / 路由不变 |
| `cargo test -p page` | ✅ 145 passed / 0 failed (原 140 + 新 5) |
| `cargo test -p storage` | ✅ 503 passed / 0 failed |
| **全 workspace 串行回归** (干净 TMPDIR) | ✅ **1066 passed / 0 failed / 22 ignored, exit 0** (基线 1049+33; 差值 = 本机多跑通 11 个 + 新增 6 个) |
| `cargo clippy --workspace --all-targets -- -D warnings` | ✅ 0 警告 |
| `cargo fmt --all -- --check` | ✅ 0 差异 |
