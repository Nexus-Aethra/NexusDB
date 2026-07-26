# Bug Report: btree_insert leaf split 路由错误导致 key 丢失

**日期**: 2026-07-25  
**严重级别**: High (数据正确性)  
**状态**: 已修复  
**影响范围**: 任何非顺序 key 分布下的 B+Tree leaf split

---

## 1. 现象描述

`crates/shard_manager/examples/stress.rs` 的 phase 4 verify 阶段报告 **1-3/600 keys missing**，每次运行丢失的 key 不同。

**复现步骤**:
```bash
cargo run --release --example stress -p shard_manager
```

**输出示例**:
```
[phase 4] verify: 重读 600 put 的 key...
  [verify] key v2_000047 missing
  [verify] key v0_000083 missing
[phase 4] done in 0.012s, verify errors: 2/600
```

**特征**:
- 非确定性：每次运行丢失不同的 key
- 仅 1-3 个 key 受影响（600 个中）
- 发生在 `flush_all()` 之后的纯读验证阶段

---

## 2. 排除项

| 假设 | 调查结论 |
|------|----------|
| network crate 引入问题 | 排除 — bug 在添加 network crate 之前就存在 |
| shard 内并发竞态 | 排除 — shard 内 `borrow_mut` + `block_on` 完全串行 |
| page 前缀压缩链 (leaf_push_back k+1 重写) | 排除 — 数学证明 shared_prefix_len 正确性 |
| pre_split_segment 段分裂逻辑 | 排除 — 代码分析确认正确 |
| flush 路径数据丢失 (nowchunks→disk→chunk_list) | 排除 — 完整性验证通过 |
| chunk_list LRU 驱逐后读旧数据 | 排除 — 驱逐后从 disk 重新加载，数据一致 |
| root_vpid 跟踪 (table_put → update_table) | 排除 — write-through 协议正确 |

---

## 3. 根因分析

**文件**: `crates/storage/src/btree.rs`  
**位置**: `btree_insert` 函数，leaf split 处理分支 (原 L232-248)

### 错误代码 (修复前)

```rust
Err(page::PageError::PageFull) => {
    let mut right_bytes = leaf_new();
    let split_key = leaf_split(&mut leaf_bytes, &mut right_bytes)?;

    debug_assert!(
        key > split_key.as_slice(),
        "split 触发的 key 必须 > split_key"
    );
    // 无条件插入 right page
    page::leaf_insert(&mut right_bytes, key, value)?;
    // ...
}
```

### 问题

`leaf_split` 在 midpoint 处分裂 leaf page：
- `split_key` = right page 的第一个 key
- 代码**无条件**将触发 split 的 key 插入 right page
- 基于错误假设："触发 key 一定 > split_key"

当 `key < split_key` 时：
1. key 被错误放入 right page
2. 后续 `btree_lookup` 时，`internal_child` 路由逻辑正确地将 `key < split_key` 路由到 **left** page
3. left page 中没有该 key → 返回 None → **key missing**

---

## 4. 触发条件分析

### stress.rs 的 key 分布

- Phase 1 (warmup): `warmup_t{tid}_{i:06}` — 字节序前缀 "w"
- Phase 2 (concurrent): `t{tid}_{i:08}` — 字节序前缀 "t"
- Phase 3 (verify keys): `v{tid}_{i:06}` — 字节序前缀 "v"

字节序: `"t..." < "v..." < "w..."`

### 触发场景

当一个 leaf page 已包含 `[t..., warmup...]` 范围的 keys，插入 `"v..."` key 触发 split：

1. `leaf_split` 在 midpoint 分裂
2. 如果 midpoint 落在 "warmup..." 区域，则 `split_key = "warmup_..."`
3. 此时 `"v..." < "warmup_..." = split_key`
4. key 被错误放入 right page（含 "warmup..." keys）
5. lookup 路由到 left page → miss

### 非确定性来源

- 不同运行中线程调度顺序不同
- 导致不同 leaf 在不同填充状态下触发 split
- 不同 key 在不同 midpoint 处受影响

---

## 5. 为什么单元测试不触发

`crates/storage/src/btree.rs` 的所有 btree 单元测试使用**顺序递增 key**：

```rust
// 示例: btree_insert_triggers_leaf_split
for i in 0..500u32 {
    let key = format!("key_{:04}", i);  // 严格递增
    btree_insert(&mut p, root, key.as_bytes(), val.as_bytes());
}
```

顺序插入时，每次插入的 key 都是当前 tree 中的**最大值**。触发 split 时：
- midpoint 在中间位置
- 触发 key 是最大值 → 一定 > split_key

因此 `debug_assert!(key > split_key)` 在所有现有测试中都成立。

---

## 6. 修复方案

### 修复代码

```rust
Err(page::PageError::PageFull) => {
    let mut right_bytes = leaf_new();
    let split_key = leaf_split(&mut leaf_bytes, &mut right_bytes)?;

    // 条件路由: key 插入正确的 half
    if key > split_key.as_slice() {
        page::leaf_insert(&mut right_bytes, key, value)?;
    } else {
        page::leaf_insert(&mut *leaf_bytes, key, value)?;
    }

    let right_vpid = pager.create(Box::new(right_bytes)).await?;
    batch.add(leaf_vpid, leaf_bytes);
    // ... propagate_split_up ...
}
```

### 正确性论证

- split 后两个 half 均约 50% 填充率，插入一个 key 一定有空间
- `key == split_key` 不会发生：split_key 是已存在的 key，而 `table_put` 在 insert 前已用 `btree_lookup` 确认 key 不存在
- left page 的不变量 "所有 key < split_key" 在 `key < split_key` 插入 left 后仍成立
- right page 的不变量 "所有 key >= split_key" 在 `key > split_key` 插入 right 后仍成立

---

## 7. Diff 摘要

```diff
--- a/crates/storage/src/btree.rs
+++ b/crates/storage/src/btree.rs
@@ -233,16 +233,14 @@
             let mut right_bytes = leaf_new();
             let split_key = leaf_split(&mut leaf_bytes, &mut right_bytes)?;
-            // 4a. 把触发的 key 重新插入到 right page ...
-            //     路由: 触发的 key > split_key 时进 right, 否则进 left (但 left 已经被 split_key
-            //     限定为 < split_key, 所以触发的 key 一定 > split_key).
-            debug_assert!(
-                key > split_key.as_slice(),
-                "split 触发的 key 必须 > split_key, ..."
-            );
-            page::leaf_insert(&mut right_bytes, key, value)?;
+            // 4a. 把触发的 key 插入到正确的 half:
+            //     - key > split_key -> 进 right
+            //     - key <= split_key -> 进 left (split 后 left 有空间)
+            if key > split_key.as_slice() {
+                page::leaf_insert(&mut right_bytes, key, value)?;
+            } else {
+                page::leaf_insert(&mut *leaf_bytes, key, value)?;
+            }
```

---

## 8. 附带发现：MetaCache phantom entry

**文件**: `crates/storage/src/meta_cache.rs`  
**方法**: `pread_slot_from_mate` (L433-444)

```rust
fn pread_slot_from_mate(&self, vpid: u64) -> Option<PidLocation> {
    let mut buf = [0u8; 8];
    let off = vpid * 8;
    match self.mate_file.read_at(&mut buf, off) {
        Ok(0) => None,
        Ok(_) => Some(PidLocation::from_bytes(&buf)),  // 可能返回全零 PidLocation
        // ...
    }
}
```

当 `vpid = 0` 且 `.mate` 文件该位置尚未写入有效数据时，`read_at` 可能读到全零字节，`PidLocation::from_bytes(&[0;8])` 返回一个 `flags() == 0` 的 phantom entry。

上层 `read_db` 用 `if pid.flags() == 0 { None }` 做了防护，所以当前不会导致数据错误，但 phantom entry 仍被缓存，浪费 cache 容量。

**建议**: 在 `pread_slot_from_mate` 内部加判断，全零 slot 直接返回 None。优先级低。

---

## 9. 验证清单

- [ ] `cargo test -p storage --lib -- btree::tests` — 现有测试通过
- [ ] `cargo test --workspace` — 全量回归
- [ ] `cargo run --release --example stress -p shard_manager` — phase 4 verify_errors = 0
- [ ] `cargo clippy --workspace` — 无新警告
