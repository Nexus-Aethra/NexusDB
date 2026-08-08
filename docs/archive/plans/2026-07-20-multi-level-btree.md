# T15: 多层 BTree 升级 (Table BTree + TableDirectory)

> **For agentic workers:** T15 polish 任务, 升级 storage crate 的两棵 BTree:
> - **Table BTree** (用户 KV 数据) — 从单 leaf page 升级为完整多层 B+Tree
> - **TableDirectory** (table_name → root_vpid) — 从单 leaf page 升级为完整多层 B+Tree
>
> MetaPage 保持单 leaf page (DESIGN 文档明说 db 数量 < 100 够用).
>
> **基础**: T1-T14 全部完成, page crate 提供完整 `leaf_*` / `internal_*` API + PageIndex segment 调度
> **依赖**: T6 Pager (含 PageWriteBatch 16-page 原子写) + TravelTree (RAII guard, task-private map)
>
> **关联 design doc**: [`../../DESIGN.md`](../../DESIGN.md) §3.0.1 (Travel Key Path), §4.5 (StorageEngine), §4.4 (Pager)

---

## 1. 现状分析

### 1.1 三棵 BTree 当前状态

| BTree | 当前实现 | 容量限制 |
|---|---|---|
| **MetaPage** (db_name → table_dir_vpid) | 单 leaf page (`MetaPage::dbs: BTreeMap<String, u64>`) | ~100 db (DESIGN 文档明示, **T15 保持单层**) |
| **TableDirectory** (table_name → root_vpid) | 单 leaf page (复用 `leaf_insert` / `leaf_get`) | ~200 table per db (**T15 升级**) |
| **Table BTree** (user key → value) | 单 leaf page (`table_put` 直接 `leaf_insert`) | ~200 KV per table (**T15 升级**) |

### 1.2 已有基础设施

| 组件 | 状态 | 位置 |
|---|---|---|
| `PageType::Leaf` | ✅ | `page/src/header.rs` |
| `PageType::Internal` | ✅ | `page/src/header.rs` |
| `leaf_split(left, right) -> split_key` | ✅ 完整 | `page/src/leaf.rs:445` |
| `internal_split(left, right) -> split_key` | ✅ 完整 | `page/src/internal.rs` |
| `internal_new / internal_child / internal_insert / internal_delete` | ✅ 完整 | `page/src/internal.rs` |
| `Pager::create` (alloc vpid + write) | ✅ | `pager.rs:228` |
| `Pager::write_page` (覆盖写 + COW) | ✅ | `pager.rs:250` |
| `PageWriteBatch` (16 page 上限) | ✅ | `pager.rs:466` |
| `TravelTree` (key → vpid map) | ✅ | `pager.rs:562` |
| `TravelTreeGuard` (RAII) | ✅ | `pager.rs:626` |

### 1.3 缺什么

1. **BTree 路由器** (`btree.rs` 新建):
   - `travel_to_leaf(root_vpid, key)` — root → internal → ... → leaf
   - `propagate_split_up(...)` — leaf 满 → split → parent insert → parent 满 → ...
   - `propagate_split_root(...)` — root 满 → split → 新 root vpid

2. **重写 `table_put` / `table_get` / `table_delete`** — 走 BTree 路由

3. **重写 `TableDirectory::create_table` / `drop_table`** — 走 BTree 路由

4. **大量测试** — 触发 split / 多层 / 持久化 / stress

---

## 2. 设计方案

### 2.1 BTree 路由 (`crates/storage/src/btree.rs` 新建)

```rust
/// 沿 key 从 root 一路 travel 到 leaf. 返回 leaf 的 vpid + TravelTree snapshot.
pub fn travel_to_leaf(
    pager: &mut Pager,
    root_vpid: Vpid,
    key: &[u8],
) -> Result<(Vpid /* leaf_vpid */, TravelPath /* parent stack */), BTreeError>;

/// 走完后, parent stack 形如:
///   [(internal_vpid, separator_key, child_vpid), ...]
///   顺序: root → ... → leaf 的 parent
///   每条记录: 走到该 internal 时, 用 (separator_key, child_vpid) 选下一层

/// insert/delete 时, leaf 满 → split 传播:
/// 1. leaf split 拿 (left_vpid, right_vpid, split_key)
/// 2. 更新 travel_path 的 "leaf parent" (separator_key → right_vpid)
/// 3. 尝试 insert (split_key, right_vpid) 到 leaf 的 parent
/// 4. parent 满 → 同样 split + 向上传播
/// 5. root 满 → root split + 分配 new_root_vpid + 创建新 root (Internal, first_child=old_left)
/// 6. 返回 Some((new_root_vpid)) (root split 时) 或 None (其他情况)
pub fn propagate_split_up(
    pager: &mut Pager,
    root_vpid: Vpid,
    travel_path: &mut TravelPath,
    split_key: Vec<u8>,
    new_right_vpid: Vpid,
) -> Result<Option<Vpid /* new_root */>, BTreeError>;
```

### 2.2 PageWriteBatch 多页原子写

split 一层涉及 2-3 pages (left + right + parent update). root split 涉及 4 pages (left + right + parent update + new_root).

`PageWriteBatch` 上限 16 pages, split 不会超过. **整个 split 流程必须在同一个 batch 内 add + submit**.

### 2.3 Travel Key Path (DESIGN §3.0.1)

- travel 时记录: `(internal_vpid, separator_key_used_to_go_down, child_vpid_i_went_to)`
- split 传播时: 用 `separator_key` 找到原 child_vpid, 更新 travel_path 里的 child_vpid
- 保证 split 后, 沿 travel_path 仍能正确找到新 child

### 2.4 简化 (第一版)

- **不实现 merge** (T15.5 polish): delete 后 underflow → 仅标记 orphan, 留给 future
- **不实现 range scan** (留 T16+): `table_get` 单点读, `table_put` 单点写
- **不实现 iterator** (留 T16+)

### 2.5 受影响文件

| 文件 | 变更 |
|---|---|
| `crates/storage/src/btree.rs` | **新建** — BTree 路由 + split 传播 |
| `crates/storage/src/registry.rs` | `table_put / table_get / table_delete` 走 btree 路由 |
| `crates/storage/src/table_directory.rs` | `create_table / drop_table` 走 btree 路由 |
| `crates/storage/src/lib.rs` | 导出 `btree` 模块 |
| `crates/storage/src/btree.rs` | tests: 多层 / split / recover / stress |

---

## 3. 实施步骤

### T15.1 — BTree 路由核心 (`btree.rs` 新建)

- [ ] 定义 `TravelPath` 数据结构 (栈式: `Vec<(internal_vpid, sep_key, child_vpid)>`)
- [ ] 实现 `travel_to_leaf(pager, root_vpid, key)` — 沿 tree 一路向下
- [ ] 实现 `propagate_split_up(...)` — leaf → parent → ... → root
- [ ] 实现 `propagate_split_root(...)` — root 满 → 新 root
- [ ] 单元测试: 单层 leaf (root = leaf, no split), 不触发 split

### T15.2 — `table_put` 走 btree 路由

- [ ] 重写 `registry::table_put`: 走 `travel_to_leaf` → `leaf_insert`
- [ ] 处理 `PageFull` 错误: 调 `leaf_split` + `Pager::create` right + `propagate_split_up`
- [ ] 全部走 PageWriteBatch 一次性 add+submit
- [ ] 兼容现有测试 (T11 已有 10+ 测试)

### T15.3 — `table_get` 走 btree 路由

- [ ] 重写 `registry::table_get`: 走 `travel_to_leaf` → `leaf_get`
- [ ] 单元测试: 跨多层 tree 仍能正确路由

### T15.4 — `table_delete` 走 btree 路由

- [ ] 重写 `registry::table_delete`: 走 `travel_to_leaf` → `leaf_delete`
- [ ] 暂不实现 merge (留 polish)
- [ ] 单元测试: delete 后 reopen 数据消失

### T15.5 — `TableDirectory` 升级到多层

- [ ] 重写 `TableDirectory::create_table` / `drop_table` 走 btree 路由
- [ ] 兼容现有测试 (T10 已有 5+ 测试)

### T15.6 — 大量测试 + 提交

- [ ] `tests/multi_level_btree_tests.rs` (新建): 触发 split / 多层 / recover / stress
  - 写 ~500 KV 触发 leaf split
  - 写 ~5000 KV 触发 internal split (多层)
  - reopen 后所有数据完整
  - 跨多层 delete 仍正确
- [ ] 跑全套测试, 确保 0 regression
- [ ] clippy + fmt 0 警告
- [ ] 提交 + 更新 CHANGELOG/AGENTS

---

## 4. 关键决策

| 决策 | 选择 | 理由 |
|---|---|---|
| MetaPage 是否升级多层? | **保持单层** | DESIGN 文档明示 db < 100 够用, 用户已确认 |
| 是否实现 merge? | **暂不实现** | delete 后 underflow 暂不处理, T15.7+ polish |
| 是否实现 range scan? | **暂不实现** | 留 T16+ |
| split 传播使用哪种 PageWriteBatch? | **整个 split 流程单 batch** | 原子性, 防止 split 半完成状态 |
| TravelPath 用 vpid 还是 key? | **key** | DESIGN §3.0.1 明确 (vpid 会因 split 失效) |
| root split 时新 root 走 `create` 还是 `write_page`? | **`create`** | 新 vpid 走 vpid_alloc, root split 后 root_vpid 变更 |

---

## 5. 风险与缓解

| 风险 | 缓解 |
|---|---|
| split 传播可能 panic (多层 deep tree) | 单元测试覆盖多层 + stress 测 |
| PageWriteBatch 跨 split 提交不原子 | **整个 split 流程单 batch**, add 完一起 submit |
| `Pager::create` 在 split 中途调 (right page) 会触发 pid_alloc | 没问题, pid_alloc 是统一的, 内部调度 |
| MetaPage 在 split 传播中被覆盖 | 不会, split 只动 tree 自身 page, MetaPage 只在 create_db/drop_db 改 |
| TravelTree 与 BTree router 冲突 | BTree 路由暂不依赖 TravelTree (用 task-local stack), TravelTree 是未来 concurrent split 钩子 |

---

## 6. 验收标准

- [ ] 单 page BTree 兼容 (root = leaf, 现有测试全过, **零 regression**)
- [ ] 触发 leaf split (~500 KV) 全部正确
- [ ] 触发 internal split (~5000 KV) 全部正确
- [ ] 触发 root split (树高 +1) 全部正确
- [ ] reopen 后多层 tree 数据完整
- [ ] 多 db 多 table 跨多层 tree 隔离
- [ ] clippy -D warnings 0 错误
- [ ] cargo fmt --check 0 差异
- [ ] 全套测试 0 failed (Storage + Page)
- [ ] 提交 + CHANGELOG/AGENTS 同步
