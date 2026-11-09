# Leaf Item 指针化改造设计（Phase 1-B）

> **状态：已定义，待评审**
> **前置：** [存储写放大改进框架](./2026-11-06-storage-write-amplification.md)
> **范围：** `crates/storage` 的 leaf item value 槽语义、value 放置层、读取路径。
> **明确不动：** B+Tree 结构本身、COW 语义、`vpid→pid` 映射层、`crates/page` 的 item 字节格式、
> GC 的页粒度机制、协议层。

---

## 1. 目标与动机

### 1.1 要解决什么

当前 leaf item 把 value **内联**在 B+Tree 叶页里：

```
当前 leaf item (16KB 页内):
  [shared u16][key_unshared_len u16][key_unshared][varint value_len][value bytes]
```

后果：
1. **叶页被 value 撑大**：一个 1KB value 的 key 只占叶页 1/16 的空间，页内 key 密度极低；
2. **树大小 ∝ 数据量**，不是 ∝ key 数 → 缓存命中率低、树更深；
3. **改一个 value 重写整个 16KB 叶页**，页内其它 key 的数据被无辜重写；
4. 大 value 只能整体溢出到独立页（`overflow.rs`），页粒度浪费严重。

### 1.2 目标形态

leaf item 只存 **key → value 位置的详细偏移指针**，value 字节移到独立的
**数据放置区（value arena）**：

```
新 leaf item:
  [shared u16][key_unshared_len u16][key_unshared][varint ptr_len][pointer record]

pointer record (15B):
  [0x00]        marker
  [vpid u64]    数据页 vpid
  [offset u16]  页内字节偏移
  [len u32]     value 逻辑长度
```

### 1.3 关键认识：指针本身不降放大，它降的是"树的大小"

必须写进文档避免误解：

| 效果 | 来源 |
|---|---|
| 叶页密度提升（~600 条/页 vs 现在 ~10–100 条/页） | 指针化 |
| 树大小从 ∝数据量 变为 ∝key 数 | 指针化 |
| **每笔写不再重写整页** | **memtable 批量**（见框架文档 Phase 1） |

**指针化和 memtable 是互补的两半，不是二选一。** 指针化让树变小、可缓存；
memtable 让批量写把页 COW 摊销。只做指针化而不做 memtable，放大倍数不会显著下降。

---

## 2. 指针格式设计

### 2.1 与现有格式的兼容点

现有 `overflow.rs` 已定义间接描述符：

```
[0x00][head_vpid u64][total_len u32]   = 13B, DESCRIPTOR_LEN
```

- `0x00` 作 marker 是因为网络门面 value 首字节必为 value_codec tag（0x01–0x05），
  **`0x00` 永不冲突** → 存量 inline 数据零迁移；
- 新指针沿用 `0x00` marker，**同一套 `is_indirect` 判别入口**。

### 2.2 候选格式对比

| 方案 | 字节 | 优点 | 缺点 |
|---|---|---|---|
| **A. 页粒度**（复用现有 13B 描述符，一个 value 一页） | 13 | **零格式变更** | 32B value 占 16KB 页，空间浪费 512×；无打包能力 |
| **B. 页 + 页内偏移**（推荐） | 15 | 支持页内打包；复用 `vpid→pid` map 定位页；`offset u16` 覆盖 16KB 页 | 需格式版本升级；需子页分配器 |
| C. 段 + 段内偏移（Lavik 式） | 13 | 段号算术寻址，为将去 map 铺路 | 需引入定长段抽象，改动更大 |

**推荐 B**：它满足"详细偏移量指针"的诉求（页 + 页内偏移 + 长度），
同时**完全复用现有 `vpid→pid` 映射层与 chunk 机制**，不触发 GC 重构——
这与 Phase 1 "index 走 COW、meta 保留、GC 不重构"的约束一致。

方案 C 留到映射层瘦身阶段（框架文档 Phase 3）再评估。

### 2.3 指针记录详细定义（方案 B）

```rust
/// leaf item value 槽中的指针记录 (15B)
///
/// 布局: [marker u8][vpid u64 LE][offset u16 LE][len u32 LE]
pub const POINTER_MARKER: u8 = 0x00;      // 复用 INDIRECT_MARKER
pub const POINTER_LEN: usize = 15;

// 判别: len == POINTER_LEN 且首字节 == 0x00
//       （与旧 13B 描述符靠长度区分，见 §5 兼容性）
```

**字段语义**：

| 字段 | 类型 | 说明 |
|---|---|---|
| `marker` | u8 | `0x00`，与 value_codec tag 不冲突 |
| `vpid` | u64 | 数据页 vpid。小 value → 打包数据页；大 value（>页容量）→ OverflowIndex 页 |
| `offset` | u16 | 页内字节偏移。打包页 = 该 value 在页内的起始偏移；溢出链 = 0 |
| `len` | u32 | value 逻辑字节数，上限沿用 `MAX_OVERFLOW_VALUE` |

**为什么不放 flags**：当前不需要。若将来要 CRC/压缩，可在 marker 高位做次类标记
（`0x00` 保留给"纯指针"，`0x80` 起作扩展），避免现在就把 15B 撑到 16B。

### 2.4 大 value 的处理

- `len <= PAGE_SIZE - PAGE_HEADER_SIZE - 预留` 的 value：**打包**进数据页，
  多 value 顺序排列，`offset` 各不相同；
- 超大 value：仍走 `OverflowIndex` 链（现有机制不变），`vpid` 指向 index 页，
  `offset = 0`，`len` = 总长。

**判别方式**：靠 `len` 与阈值比较，或让数据页自身记录"本页是打包页还是索引页"
（页类型已可区分：`PageType::Overflow` / `OverflowIndex`）。

---

## 3. Value Arena（数据放置区）

### 3.1 结构

复用现有 `nowchunks`（1MB 内存块）作为 append 区，但**分配单位从"16KB 页槽"改为"字节"**：

```
NowChunk (1MB, 内存, 单 shard 独占)
  ├─ bump pointer: 下一个可分配字节偏移
  ├─ 已写入的 value 记录顺序排列
  └─ 满 → 整块 1MB io_uring 顺序写盘 → 变成磁盘上不可变的 chunk
```

- **分配**：`arena_alloc(len) -> (chunk_idx, offset)`，bump 分配，O(1)；
- **flush**：chunk 满时整块顺序写（比现在"写 64 个页槽"更接近纯顺序写）；
- **读**：指针 → `vpid→pid` 得 `(file_id, chunk_idx, page_idx)` → 但我们需要的是
  **chunk 内字节偏移**，不是页号 → 见 §3.3。

### 3.2 ⚠️ 关键约束：vpid 是页粒度的

现有 `vpid→PidLocation` 映射解析到 **页**（`file_id, chunk_idx, page_idx`），
最小寻址单位是 16KB 页。而指针里的 `offset` 是**页内字节偏移**。

所以：
- 一个"数据页"仍是一个 16KB 的 `vpid`（复用现有页分配/COW/GC/liveness 机制）；
- 该页内可打包多个 value，靠 `offset` 区分；
- **不需要新的映射层**，`vpid→pid` 照旧工作。

这保证 GC 仍可按页粒度判活（一页内只要还有一个 value 被引用，页就活着），
**GC 逻辑零重构**——这正是 Phase 1 的约束。

### 3.3 与 nowchunks 的衔接

现在 `nowchunks` 按页槽管理。要支持页内打包，需要：

1. **数据页的写入从"整页替换"改为"页内 append"**：
   - 数据页在内存中有一个可写实例（当前 nowchunk 中的页）；
   - `arena_append(value) -> (vpid, offset)`：若当前数据页剩余空间足够 → 原地 append，
     返回 `(当前数据页 vpid, 页内偏移)`；否则分配新数据页；
   - 页满/rotate 时该页被 flush 并变为不可变，之后不再接受 append。
2. **数据页必须是 COW 安全的**：页一旦 flush 就不可变；append 只发生在
   "当前活跃数据页"上。这与现有 `nowchunks` 的"驻留页可原位覆盖"语义一致，
   但要注意：**覆盖写在数据页语义下是非法的**（会破坏同页其它 value 的偏移），
   必须改成"只 append，不覆盖"。

> ⚠️ 这是本次改造**最容易出错的地方**：现有 `pager_write.rs` 的"chunk 驻留则
> 原位覆盖同一 page_idx"优化，对数据页必须禁用——数据页只能追加。

---

## 4. 影响面清单

### 4.1 需要修改

| 位置 | 改动 |
|---|---|
| `crates/storage/src/registry.rs` | `table_put`：value 走 `arena_append`，leaf item value 槽写指针记录 |
| `crates/storage/src/registry.rs` | `table_get`：解指针 → 读数据页 → 按 `offset/len` 切片 |
| **新增** `crates/storage/src/value_arena.rs` | 数据页分配 / append / 打包 / 满页 rotate |
| `crates/storage/src/overflow.rs` | `is_indirect` 增加 15B 指针判别；13B 旧描述符兼容读取 |
| `crates/storage/src/engine.rs` | `table_get` 增加指针解引用分支 |
| `crates/storage/src/recover.rs` | 数据页纳入扫描/判活（页类型已是标准 LCBP header，兼容） |
| `crates/storage/src/collections*.rs` | 复合结构行（Hash/Set/ZSet/List）的 value 同样指针化，或第一阶仅 String |

### 4.2 明确不改

| 位置 | 原因 |
|---|---|
| `crates/page/src/item.rs` | value 槽本就是任意字节，指针只是其中一种内容 |
| `crates/page/src/header.rs` | 页头不变 |
| `crates/page/src/lib.rs` (`PidLocation`) | 页级寻址不变 |
| `crates/storage/src/meta_cache.rs` | `vpid→pid` 不变 |
| `crates/storage/src/chunk_liveness.rs` | 页粒度 liveness 不变 |
| `crates/storage/src/wal.rs` | 不变 |
| `crates/storage/src/btree.rs` / `pager*.rs` | B+Tree 与页提交机制不变 |
| `crates/network` | 协议层零感知 |

### 4.3 读路径代价（必须诚实记录）

| | 改造前 | 改造后 |
|---|---|---|
| GET 命中 | 1 次叶页读（value 内联） | 1 次叶页读 + **1 次数据页读** |
| 缓存友好性 | value 挤占叶页缓存 | 叶页密度高、缓存效率提升 |

**净效果取决于缓存命中率**：叶页变小后缓存能装更多 key，多数情况下
"叶页命中 + 数据页未命中" vs 原来"叶页未命中"。需基准验证，不能假定一定更快或更慢。

---

## 5. 兼容性与迁移

### 5.1 磁盘格式不兼容

15B 指针与现有 13B 描述符**长度不同**，`is_indirect` 必须按长度区分：

```rust
pub fn is_indirect(stored: &[u8]) -> bool {
    stored.len() == DESCRIPTOR_LEN_OLD && stored[0] == INDIRECT_MARKER  // 13B 旧格式
 || stored.len() == POINTER_LEN && stored[0] == POINTER_MARKER          // 15B 新格式
}
```

**存量数据库**（v0.1.1 及以前）磁盘上是 13B 描述符或 inline value。
选择：
- **A. 双格式读取 + 新写用新格式**（推荐）：旧数据继续可读，新数据用 15B 指针。
  无需迁移工具，旧数据在自然覆盖写时逐步转换。
- B. 格式版本号 + 全量迁移：需写迁移工具，成本高，且 Phase 1 目标是迅捷。

**推荐 A**，但要在 `recover` 里明确：旧 13B 描述符的 value 仍可读，
且 **GC 不得回收仍被旧描述符引用的溢出页**（现有 `PID_FREED` 墓碑机制已覆盖）。

### 5.2 inline 小 value 是否保留

| 方案 | 说明 |
|---|---|
| 保留 inline（hybrid） | 小 value 仍内联，大 value 走指针。读路径对热点小 key 少一次 I/O |
| 全部指针化 | 语义统一，叶页密度最高，但每个 GET 多一次间接 |

**建议：第一阶保留 inline，仅把"超过某阈值"的 value 指针化**，与现有
`needs_overflow` 的阈值机制合并为一个统一的 `needs_pointer` 判别。
理由：改动面最小、读路径不回归、且能先验证指针机制正确性。
待 memtable + 基准数据出来后再决定是否全量指针化。

---

## 6. 分阶段实施

### 阶段 1-B-1：指针机制落地（仅大 value）
1. 新增 `value_arena.rs`，实现页内 bump append；
2. `table_put` 对 `needs_pointer` 的 value 写 15B 指针；
3. `table_get` 解指针；
4. `is_indirect` 双格式兼容；
5. 回归：大 value 读写 roundtrip、kill -9 恢复、复合结构不受影响。

### 阶段 1-B-2：密度收益验证
- 基准对比：同等 key 数下叶页数量、树深度、缓存命中率；
- 确认"树变小"的收益是否真实存在。

### 阶段 1-B-3：与 memtable 叠加
- 按框架文档 Phase 1 引入 memtable；
- 验证"每批每页只 COW 一次"；
- **到此才是写放大收益的完整兑现点。**

### 阶段 1-B-4（可选）：全量指针化
- 依据 1-B-2/1-B-3 数据决定是否把 inline 小 value 也指针化。

---

## 7. 风险

| 风险 | 缓解 |
|---|---|
| **数据页被误覆盖**（破坏同页其它 value 偏移） | 数据页禁用原位覆盖，只 append；代码评审重点 |
| 15B/13B 判别错误导致读错数据 | `is_indirect` 严格按长度判别 + 双格式单测 |
| 页内碎片（value 长度不一，页尾剩余空间浪费） | 记录页内剩余空间，小于阈值即 rotate；后期可做页内整理 |
| GC 把仍被引用的数据页回收 | liveness 按页粒度，指针引用即活；沿用现有墓碑机制 |
| 读路径多一次 I/O | 保留 inline 小 value；用基准验证净效果 |
| recover 遇到新旧混合格式 | 双格式读取 + 旧格式只读不写 |

---

## 8. 待决问题

1. **是否保留 inline 小 value**（§5.2）？建议保留，但需你确认——
   这决定第一阶的改动面和读路径表现。
2. **数据页的页类型**：复用 `PageType::Overflow(4)` 还是新增 `PageType::Data(6)`？
   新增类型更清晰（可区分"打包数据页"与"溢出索引页"），但 `page_type()` 的
   `from_byte` 与 recover 扫描都要改。
3. **指针是否需要 flags 字节**（为 CRC/压缩预留）？建议不加，需要时用 marker 高位扩展。
4. **复合结构（Hash/Set/ZSet/List）是否同步指针化**？建议第一阶段只做 String + 溢出路径，
   复合结构保持 inline，缩小改动面。
5. **`offset` 用 u16 是否够**：页 16KB → 是。但若将来页变大需重新评估。
6. **数据页是否也纳入 memtable 的批量写**（而非直接 append 到 nowchunk）？
   这影响 memtable 与该机制的叠加方式。

---

## 9. 参考

- 框架文档：[2026-11-06-storage-write-amplification.md](./2026-11-06-storage-write-amplification.md)
- `crates/page/src/item.rs`：item 字节布局与 encode/decode
- `crates/page/src/header.rs`：`PageType` 枚举、页头 40B、`ITEM_AREA_SIZE`
- `crates/page/src/lib.rs`：`PidLocation` 8B packed 布局
- `crates/storage/src/overflow.rs`：13B 描述符、`INDIRECT_MARKER`、`INLINE_LIMIT`
- `crates/storage/src/keyspace.rs`：物理 key 编码（指针化不影响 key 编码）
- `docs/AGENTS.md`：F44 溢出页设计、async 落盘 gotchas
