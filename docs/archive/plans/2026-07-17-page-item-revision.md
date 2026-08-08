# page crate — 增量式 prefix-compress 变更方案

> **2026-07-18 修订**: 实际实现与原 plan 在 `key_count` 语义上有偏差. 现以实现为准:
>
> * `header.key_count` **不包含哨兵** (哨兵只是物理 item, 不计入业务 key 数).
>
> * `cp[0].item_count` **包含哨兵** (cp\[0] 段覆盖 "sentinel + 真实 keys").
>
> * 因此对于只有 1 个 cp 段的 page: `cp[0].item_count = header.key_count + 1`.
>
> 原因: `header.key_count` 是业务概念 (多少个真实 key), 与 `internal_child` / `leaf_get` 的外部接口对齐 (返回 N 表示 N 个真实 key); cp.item\_count 是物理布局 (segment 扫描范围). 两者分离更清晰. plan 后续涉及 `key_count` 的描述均按此语义.

## 背景

当前 page crate 在 `leaf_insert` / `leaf_delete` / `leaf_split` 中依赖两个低效/脆弱路径:

1. **`reconstruct_key_at_index(key_count-1)`**: 算前一个 item 的完整 key 需要从头顺序解码所有 item,O(N)。
2. **`rebuild_all_items`**: 删除/分裂后整体重建 item 区 + cp 数组,O(N) 全量重写。

第二个尤其有问题 — 单次删除也是 O(N),在写密集场景下性能不可接受。

## 目标

每次增/删/改/分裂的代价都是 **O(段内 item 数) ≤ O(32)**,即常数级别。

> **2026-07-18 修订**: 实际 split 是 O(N/2) 而非 O(32), 因为 mid+1..end 的 items 全部重新 prefix-compress 到 right page (move + re-encode). 后续可优化为把 mid item 留在 left (shared=0) + 物理切片后续 items + 只重写 mid+1 为 shared=0 (cp 段首).

## 设计: ItemPtr 游标 + 真实哨兵 item + PageIndex

### 核心抽象

```rust
/// ItemPtr 是只读游标, 封装 prefix-compress 解码逻辑.
/// 通过 next() 在段内顺序遍历, 自动维护 prev_key.
struct ItemPtr<'a> {
    page: &'a [u8],
    off: usize,           // 当前 item 字节偏移
    cur_key: Vec<u8>,      // 当前 item 的完整 key (cached)
    cur_n: usize,          // 当前 item 字节长度
}

impl ItemPtr<'_> {
    fn new(page: &[u8], off: usize) -> Self;
    fn create_from_cp(page: &[u8], cp_idx: usize) -> Self;  // 验证 shared=0
    fn key(&self) -> &[u8];
    fn total_len(&self) -> usize;
    fn next(&self) -> Option<ItemPtr>;  // 顺序前进, 内部拼下一个 key
}

struct LeafItemPtr<'a> { /* 同 ItemPtr, 但有 value() */ }
struct InternalItemPtr<'a> { /* 同 ItemPtr, 但有 child_vpid() */ }
```

### 真实哨兵 item

**哨兵作为真实 item 写入 page**,占 item 区第一个位置:

* `shared_prefix_len = 0`

* `key_unshared_len = 0`

* `value = []` (Leaf) / `child_vpid = 0` (Internal)

* `cp[0]` 指向哨兵 (而不是真实业务 item)

**作用**: 把"插入到 item 0 之前"转化为"在哨兵后插入",统一通过 push\_back 处理。

**哨兵在 page 中的布局**:

```
┌─ Page Header (40B) ─────────────────────────┐
│  magic[4] = "LCBP"                          │
│  type[1]                                     │
│  flags[1]                                    │
│  key_count[2]      ← 不含哨兵 (仅真实 keys)   │
│  free_off[2]                                   │
│  ...                                          │
├─ Item Area (向高地址长) ─────────────────────┤
│  [Sentinel] (shared=0, key_len=0)           │  ← PAGE_HEADER_SIZE 处
│  [Real Item 0]                               │
│  [Real Item 1]                               │
│  ...                                          │
├─ Checkpoint Array (向低长) ──────────────────┤
│  cp[0] = { item_count=key_count+1, first_item_off=PAGE_HEADER_SIZE }
│       (cp[0] item_count 含哨兵 = 1 + 真实 keys)
│  cp[1..] = 真实 cp 段 (item_count = 真实 keys in segment)
│  CheckpointHeader
├─ Footer (16B) ──────────────────────────────┘
```

**key\_count 含义** (2026-07-18 修订): **不含哨兵**. 空 page key\_count=0 (只有哨兵), 有 N 个真实 keys 时 key\_count=N. cp.item\_count 含哨兵所以 cp\[0].item\_count = key\_count + 1.

### Page Index (内存)

```rust
struct PageIndex {
    /// segments[0] 是哨兵段 (first_full_key = 空, item_count = 1 if 空 else > 0)
    /// segments[1..] 是真实 cp 段
    segments: Vec<Segment>,
}

struct Segment {
    first_item_off: u16,    // 段首 item 字节偏移
    item_count: u16,         // 段内 item 数
    first_full_key: Vec<u8>, // 段首 item 的完整 key (cached)
}
```

**加载**:

* 读 cp header,得到 cp\_count

* 对每个 cp\[i]:

  * 读 cp\[i].first\_item\_off

  * decode 该处 item (必然 shared=0)

  * 取 key\_unshared 作为 first\_full\_key

  * 取 cp\[i].item\_count 作为 item\_count

* 在 segments\[0] 之前插入虚拟哨兵段 (first\_full\_key = 空)

### push\_back 函数 (page 级别, 接收 ptr)

```rust
/// 在 ptr 之后插入新 item.
/// ptr 可以指向任何位置, 包括哨兵.
/// 内部处理:
///   1. 计算 prev_key = ptr.key()
///   2. 计算 new_n = encode(new_key, value) 字节数
///   3. copy_within(insert_off..free_off, insert_off+new_n) 后移后续
///   4. 在 insert_off 写入新 item
///   5. **关键: 重写下一个 item (insert_off + new_n 处) 的 shared_prefix_len**
///      它的 prev_key 现在是新 item 的 full_key, 必须重写
///   6. 更新 PageIndex.segments (item_count + 1)
///   7. 把 PageIndex 写回 page (cp array + header.key_count)
fn leaf_push_back(page: &mut [u8], ptr: &LeafItemPtr, key: &[u8], value: &[u8]) -> Result<(), PageError>;
```

**ptr 为哨兵时**: insert\_off = PAGE\_HEADER\_SIZE, prev\_key = 空, 新 item 自动 shared=0。这把"插入到最前"转化为"在哨兵后插入"。

### ⚠️ push\_back 关键: 重写紧邻 item 的 shared\_prefix\_len

**重要前提**: item k 的 `shared_prefix_len(k) = common_prefix(full_key(k-1), full_key(k))`。

push\_back 在 ptr 位置 (item k-1) 之后插入新 item 后:

| item idx              | shared\_prefix\_len 是否需要重写? | 原因                                             |
| --------------------- | --------------------------- | ---------------------------------------------- |
| `0..k-1`              | 否                           | 前面的 item, 完全没受影响                               |
| `k-1` (ptr 指向)        | 否                           | 它本身没变, shared\_prefix\_len 也不依赖它后面             |
| `k` (新插入)             | N/A                         | 自己编码 (shared = common\_prefix(ptr.key(), key)) |
| **`k+1`** **(紧邻新插入)** | **✅ 必须重写**                  | 它的 prev\_key 现在是新插入的 item (key), 不是原 item k-1  |
| `k+2, k+3, ...`       | **否**                       | 它们的 prev\_key (full\_key(i-1)) 字节没变, 仍然正确      |

**关键洞察**: 只有 k+1 需要重写。k+2 及之后的 item 的 prev\_key 字节没变, 所以 shared\_prefix\_len 仍然有效。

### push\_back 优化: 只触碰 k+1 和可能的 k+2

**思路**: 提前预知 k+1 的位置和它所在的段, 重写 k+1; 如果 k+1 跨越段边界, 还需要重写 k+2 (新段段首必须 shared=0)。

#### 流程

```
输入: ptr (指向 item k-1), new_key, new_value
1. 计算 new_n = encode_leaf_item(ptr.key(), new_key, new_value) 字节数
2. 把 insert_off..free_off 的内容后移到 insert_off + new_n (一次大块 copy_within)
3. 在 insert_off 写入新 item (new_n 字节)
4. 找到 k+1 位置 (k1_off = insert_off + new_n)
5. 重写 k+1:
   - 还原 k+1 的 full key (基于旧 prev_key = ptr.key())
   - 计算 new_shared_k1 = common_prefix(new_key, k1_full_key)
   - 重新编码 k+1, 字节数 = k1_new_n
   - delta1 = k1_new_n - k1_old_n
6. 如果 k+1 是当前 cp 段的最后一项:
   - 此时 k+2 是新段的首 item
   - k+2 必须重写为 shared=0 (因为它是新段首)
   - k+2 的 prev_key 变成 k1_full_key (重写后)
   - 重写 k+2, delta2 = k2_new_n - k2_old_n
7. total_delta = new_n + delta1 + (delta2 if exists)
8. 用 total_delta 更新 index array:
   - inserted_seg.item_count += 1
   - segments[inserted_seg+1..] 的 first_item_off += (delta1 + delta2)  // 后续 cp 段首偏移
   - (new_n 的部分由 Step 2 的 copy_within 已经处理)
9. PageIndex.write_back(page) 把新 index 写入 cp array
```

#### 优点

* **触碰 item 数量确定**: 最多 k+1 和 k+2 (2 个)

* **不链式触发**: 所有 delta 一次算清

* **index array 增量更新**: 只调整 first\_item\_off, 不重建整个 array

* **3-4 次 page 字节操作**: 1 次大块 copy + 2-3 次小写入 + 1 次 index array 覆写

#### 伪代码

```rust
fn leaf_push_back(page: &mut [u8], idx: &PageIndex, ptr: &LeafItemPtr, key: &[u8], value: &[u8]) {
    let insert_off = ptr.off + ptr.cur_n;
    let free_off_before = page_free_off(page);
    let prev_key = ptr.key();
    
    // Step 1: 编码新 item
    let mut buf0 = [0u8; 4096];
    let new_n = encode_leaf_item(&mut buf0, &prev_key, key, value);
    
    // Step 2: 大块后移 (一次性处理所有后续 items)
    page.copy_within(insert_off..free_off_before, insert_off + new_n);
    page_set_free_off(page, free_off_before + new_n);
    
    // Step 3: 写入新 item
    page[insert_off..insert_off + new_n].copy_from_slice(&buf0[..new_n]);
    
    // Step 4: 重写 k+1
    let mut extra_delta: isize = 0;
    let k1_off = insert_off + new_n;
    if k1_off < page_free_off(page) {
        let (k1_item, k1_old_n) = decode_item(page, k1_off, Leaf);
        let k1_full_key = k1_item.full_key(&prev_key);
        let mut buf1 = [0u8; 4096];
        let k1_new_n = encode_leaf_item(&mut buf1, key, &k1_full_key, k1_item.value)?;
        let delta1 = k1_new_n as isize - k1_old_n as isize;
        extra_delta += delta1;
        
        // Step 5: 检查 k+1 是否段尾, 如果是重写 k+2
        let k2_off = k1_off + k1_new_n;
        let mut k2_full_key_new_prev = None;
        if is_k1_segment_last(idx, ptr, k1_off) && k2_off < page_free_off(page) {
            let (k2_item, k2_old_n) = decode_item(page, k2_off, Leaf);
            let k2_full_key = k2_item.full_key(&k1_full_key);
            // k+2 是新段首, 必须 shared=0
            let mut buf2 = [0u8; 4096];
            let k2_new_n = encode_leaf_item(&mut buf2, &k1_full_key, &k2_full_key, k2_item.value)?;
            let delta2 = k2_new_n as isize - k2_old_n as isize;
            extra_delta += delta2;
            
            // 写入 k+1 和 k+2 (覆盖原位置)
            page[k1_off..k1_off + k1_new_n].copy_from_slice(&buf1[..k1_new_n]);
            page[k2_off..k2_off + k2_new_n].copy_from_slice(&buf2[..k2_new_n]);
        } else {
            // 只重写 k+1
            page[k1_off..k1_off + k1_new_n].copy_from_slice(&buf1[..k1_new_n]);
        }
    }
    
    // Step 6: 更新 index array (segments[inserted_seg+1..] 的 first_item_off += extra_delta)
    // Step 7: PageIndex.write_back(page)
}
```

### 重写 k+1 的细节

设 k+1 处的旧 item 字节:

* `old_shared = old_item.shared_prefix_len`

* `old_key_unshared = old_item.key_unshared` (字节没变)

* `old_value = old_item.value` (字节没变)

* `old_full_key = prev_key + old_key_unshared` (这里 prev\_key 是 ptr.key())

插入新 item 后, prev\_key 变成 `key` (新 item 的 full key):

* `new_shared = common_prefix(key, old_full_key)` (新前缀)

* `new_key_unshared_len = old_full_key.len() - new_shared`

* `new_key_unshared = old_full_key[new_shared..]`

* `new_value = old_value`

* `new_n = 4 + new_key_unshared_len + vint(new_value.len()) + new_value.len()`

**delta 处理**:

* 如果 `new_n == old_n`, 直接覆盖

* 如果 `new_n != old_n`, 需要再次后移 k+2 及之后的 items

  * copy\_within(next\_off + old\_n..free\_off + new\_n, next\_off + new\_n)

  * 更新 free\_off += (new\_n - old\_n)

### 极端情况: 重写后的 k+1 自己也可能引发链式位移

如果 new\_shared 变化很大, new\_n 可能变化大, 触发 delta, 这又会 push 后续 items. 但后续 items 的 shared\_prefix\_len 不需要重写 (因为它们的 prev\_key 字节没变). 所以**最多只有 k+1 一个 item 需要重写**.

但有个 corner case: 如果 k+1 在重写后**跨越了 cp 段边界**, 那 cp 数组需要更新. 这是设计层面的问题 (cp\[i] 段大小变化), 由 PageIndex.write\_back 处理.

### 操作流程

**核心原则**: insert/delete 之前先做 pre\_split/pre\_merge, 确保 push\_back/delete 的核心逻辑不需要处理段边界变化。

#### 预分裂 pre\_split (在 insert 之前)

```
输入: PageIndex, segment_idx i
条件: segments[i].item_count >= MAX_PER_CHECKPOINT (32)

1. mid_offset = segments[i].item_count / 2
   front_count = mid_offset
   back_count = segments[i].item_count - mid_offset
2. 顺序扫描到 mid item 的字节偏移 (mid_off)
3. 还原 mid item 的 full_key (从 segments[i].first_full_key 段内 next() mid_offset 次)
4. 重写 mid item 为 shared=0, full_key=mid_full_key
   - delta_mid = new_n - old_n
5. segments[i].item_count = front_count
6. 插入新 segments[i+1]:
   - first_item_off = mid_off + new_n
   - item_count = back_count
   - first_full_key = mid_full_key
7. segments[i+2..] 的 first_item_off += delta_mid
8. write_back
```

#### 预合并 pre\_merge (在 delete 之前)

```
输入: PageIndex, segment_idx i
条件: segments[i].item_count < MIN_PER_CHECKPOINT (8) 且有右邻

1. total = segments[i].item_count + segments[i+1].item_count
2. 如果 total <= MAX_PER_CHECKPOINT (32):
   - segments[i].item_count = total
   - 删除 segments[i+1]
   - write_back
3. 否则 (total > 32):
   - "借调": 从 i+1 借 k 个 items 到 i, 使得 i ≥ 8 且 i+1 ≥ 8 且都不超 32
   - 借调时需要重写 i (新段尾) 和 i+1 (新段首) 的 shared_prefix_len
   - 类似 push_back 重写 k+1 的逻辑, 但双向

时间: O(32)
```

#### Insert

```
1. load page → PageIndex
2. 段二分: 找 segments[i].first_full_key <= key 的最后一个 i (i >= 1, 跳过哨兵)
3. **pre_split_check**: 如果 segments[i].item_count >= 32, 调 pre_split(i)
   - 分裂后 ptr (如果原本指向 k+1) 的位置可能变化, 重新定位
4. 从 cp[i] 创建 ItemPtr, 顺序 next() 找 key 的位置 (或确认不存在)
   - 找到 prev_ptr (指向 <key 的最后一个 item, 即插入点之前的位置)
5. prev_ptr 已经在"插入点之前"位置
6. 调 leaf_push_back(page, prev_ptr, key, value)
   (如果插入点是 page 最前面, prev_ptr 指向哨兵, 也走同一逻辑)
7. write_back
```

时间: O(log cp\_count) + O(32) + push\_back (O(32))

#### Delete (2026-07-18 实现)

```
1. load page → PageIndex
2. 段二分 + 段内 next() 找 key 所在 ptr + target_seg_idx
3. **pre_merge_check**: 删除后 segments[target_seg_idx].item_count - 1 < MIN_PER_CHECKPOINT (8)
   - 如果有右邻 (target_seg_idx + 1 < segments.len()):
     - 调 pre_merge(target_seg_idx) 合并左右段 (只更新 PageIndex 元数据)
     - pre_merge 真正执行需重新 prefix-compress target_seg_idx 段的 bytes (merge 物理实现见 pre_merge 章节)
   - 否则仅物理删除 (碎片化)
4. 在 ptr 处物理删除:
   - copy_within(ptr.off + ptr.cur_n .. free_off, ptr.off)
   - free_off -= ptr.cur_n
   - key_count -= 1
5. 重写 ptr 位置(下一个 item) 的 shared_prefix_len (prev_key 变了).
   边界: 若 k+1 是 cp[target_seg_idx+1] 段首, 必须用 shared=0 编码;
   若 target 原本是段首, k+1 现在是新段首, 也必须 shared=0.
6. 清理空段 (item_count=0 且非哨兵段)
7. write_back
```

时间: O(log cp\_count) + O(32) + delete (O(32))

> **当前状态**: pre\_merge\_segment 已实现但**未被 leaf\_delete / internal\_delete 调用** (因为物理合并需要重新 prefix-compress, 不是简单调一次就能搞定). 见 pre\_merge 章节.

#### Update (2026-07-18 实现)

```
1. load page → PageIndex
2. locate_segment 找 ptr 指向 key
3. 预解码 old item (value_len 等元数据)
4. 计算 new_n = encode_leaf_item(prev_key, key, new_value)
5. **如果 new_n == old_n**: 就地修改 value 字节 (不重写 shared_prefix_len, 因为 prev_key 不变)
   - 用 copy_within 覆盖 [old_off + prefix_header_n + key_n + vint_prefix_n .. old_off + old_n]
6. **否则 (new_n != old_n)**: 
   - shift 后续 items: copy_within(old_off + old_n .. free_off, old_off + new_n)
   - 写新 item 到 old_off (前缀部分不变, 只重写 value 部分)
   - 调 k+1 重写 (类似 push_back, 因为 old off 变了, 但 prev_key 不变 → shared 也不变, 只是 value 改)
   - 更新 free_off
7. PageIndex.write_back (key_count 不变)
```

时间: O(32) (value 修改 + 后续 items shift)

> **当前状态**: 已实现 `leaf_update` / `internal_update`. 处理 new\_n == old\_n 的快路径 (in-place value 替换) 和 new\_n != old\_n 的一般路径 (shift + 重写 value). 重复 key 报错 (不允许 insert 覆盖, 需 update).

#### Split (right page 已有空 page buffer)

```
1. load left page → PageIndex
2. mid = key_count / 2 (含哨兵)
3. 段二分定位 mid item 所在段, next() 推进到 mid 位置
4. 收集 mid..free_off 的所有 items 到内存 vec<full_key, value>
5. mid item 重写为 shared=0 (因为它成为 right 的 cp[0] 段首)
6. left page:
   - free_off = mid_off
   - key_count = mid (含哨兵)
7. right page:
   - 从 PAGE_HEADER_SIZE 开始 prefix-compress 写入所有 mid.. items
   - key_count = (left_count + right_count + 1)  // 1 = 哨兵, 在 right
   - 哨兵要不要复制到 right? 不需要, 因为 right 是新 page
   - 实际上 split 流程: 哨兵留在 left, right 直接写业务 item
8. 两边都重建 PageIndex
   - left: 只剩 mid 个 items (含哨兵)
   - right: 中间 merge 检测
9. 写回两边 cp array
```

时间: O(32) (只需要处理 mid item 处)

### 哨兵的特殊处理点

1. **哨兵不会被查找/删除**: locate\_item 找 key 时跳过哨兵 (因为 cp\[0] 在哨兵位置, 但 key 不可能 == 空)
2. **merge 时**: 当两个段合并后只剩哨兵 (key\_count = 1), 哨兵仍然在
3. **initial empty page**: key\_count=1 (只有哨兵), cp\_count=1 (cp\[0] 指向哨兵)
4. **哨兵的 page 操作**: 它也是一个 item, 所以 leaf\_insert 第一次会插入到"哨兵之后"(即 item 0 位置), 这就解决了"插入到最前"的问题

### merge 时特殊处理

当 segments\[i].item\_count < MIN\_PER\_CHECKPOINT (8) 且有相邻段:

* 与右邻合并: segments\[i] 和 segments\[i+1] 合并成一个段

* 段首还是同一个 item (来自前一段), 不需要重写

* 但合并后段首的 prev\_key 变了? 段首是 shared=0, 不依赖 prev\_key, 所以不需要重写

合并时不需要重写任何 item. 只需要合并 segments\[i] 和 segments\[i+1].item\_count, 然后调整 cp\_count.

## 实现步骤 (TDD)

### Phase 1: ItemPtr 基础

* [ ] T1.1: 实现 `LeafItemPtr::new(page, off)` - 解码单个 item

* [ ] T1.2: 实现 `LeafItemPtr::key()`, `value()`, `total_len()`

* [ ] T1.3: 实现 `LeafItemPtr::create_from_cp(page, cp_idx)` - 验证 shared=0

* [ ] T1.4: 实现 `LeafItemPtr::next()` - 顺序前进, 拼接下一个 key

* [ ] T1.5: 同样 `InternalItemPtr` 四个方法

* 测试: 单个 item 各种场景, next() 链式调用, create\_from\_cp 验证

### Phase 2: PageIndex

* [ ] T2.1: 定义 `PageIndex` 和 `Segment`

* [ ] T2.2: `PageIndex::load(page, kind) -> PageIndex` - 读 cp array + 哨兵

* [ ] T2.3: `PageIndex::locate(key) -> Option<LeafItemPtr>` - 二分定位

* [ ] T2.4: `PageIndex::write_back(page)` - 写 cp array + header

* [ ] T2.5: 加入哨兵段处理

* 测试: 加载 80 个 keys, locate 各种 key, 写回字节相同

### Phase 3: leaf\_push\_back

* [ ] T3.1: 实现 `leaf_push_back(page, &LeafItemPtr, key, value)`

* [ ] T3.2: 触发 split 检测 (item\_count > 32)

* 测试: 在哨兵后/中间/末尾插入, shared\_prefix\_len 正确

### Phase 4: 替换 leaf\_insert / leaf\_delete / leaf\_split

* [ ] T4.1: 重写 leaf\_insert 用 PageIndex + push\_back

* [ ] T4.2: 重写 leaf\_delete 用 PageIndex + 物理删除

* [ ] T4.3: 重写 leaf\_split 用 PageIndex + 增量 mid 处理

* 测试: 所有 leaf\_tests 和 stress\_tests

### Phase 5: 替换 internal\_\*

* [ ] T5.1: 同样切换 internal\_insert / internal\_delete / internal\_split

### Phase 6: 清理

* [ ] 删除 `rebuild_all_items`, `rebuild_cps`, `split_checkpoint` 旧逻辑

* [ ] 删除 `reconstruct_key_at_index` (ItemPtr 替代)

* [ ] 删除 debug\_repro.rs (空文件可以保留)

## 关键不变量

1. **哨兵总是 item 0**: shared=0, key\_unshared\_len=0
2. **key\_count 不包含哨兵** (2026-07-18 修订): 真实 keys 数 = key\_count. cp\[0].item\_count = key\_count + 1.
3. **每个 cp 段首 shared=0**: 验证在 create\_from\_cp 时
4. **段大小 ≤ MAX\_PER\_CHECKPOINT (32)**: 超了就 split
5. **段大小 ≥ MIN\_PER\_CHECKPOINT (8)**: 少了就 merge (但哨兵可以例外)

## 性能对比

| 操作           | 当前 (rebuild)                                  | 新方案                           |
| ------------ | --------------------------------------------- | ----------------------------- |
| leaf\_insert | O(N) (prev\_key 累加) + 可能 O(N) (split rebuild) | O(32)                         |
| leaf\_delete | O(N) (rebuild\_all\_items)                    | O(32)                         |
| leaf\_split  | O(N) (两侧 rebuild)                             | O(32)                         |
| locate\_item | O(log N) + O(32)                              | O(log cp\_count) + O(32) (不变) |
| leaf\_get    | O(log N) + O(32)                              | O(log cp\_count) + O(32)      |

