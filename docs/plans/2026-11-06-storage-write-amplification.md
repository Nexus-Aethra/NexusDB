# 存储写放大改进框架（分阶段）

> **状态：Phase 1 已定义，待实施**
> **范围：** `crates/storage` 的持久化写路径（LCB-Tree / NowChunks / meta_cache / WAL / GC）。
> 不改变 Redis/RESP 语义、SQL 语义、对外协议或现有崩溃恢复保证。
> **执行原则：** 每个任务独立可编译、可测试、可基准回退；
> **禁止**为了让数字好看而关闭 WAL、削弱崩溃一致性或放宽错误处理。
> 每个阶段结束必须有一次**同机交错 A/B**（见 §7），不接受单次采样作为门槛结论。

---

## 1. 背景与问题定义

### 1.1 当前写路径

一次 `SET k v`（`table_put`）当前要做：

```
1. WAL append（wal_mode=periodic 默认，每秒 fsync，不在每笔关键路径上）
2. btree_insert：root 逐层 travel 到 leaf
3. leaf_insert 进 16KB 页；页满则 split（left + right + parent，同一 PageWriteBatch）
4. PageWriteBatch.submit：16KB 页 memcpy 进 1MB nowchunk + 写 meta_cache
5. chunk 满（64 页）→ FlushJob 协程 io_uring 写 1MB 落盘
6. GC：chunk 级 liveness 追踪，活页搬新 chunk，改 vpid→pid map
```

### 1.2 核心问题：固定页粒度导致的写放大

实测参照：`docs/plans/2026-08-05-resp-write-path-performance.md` 记录覆盖写
~167K SET/s、p99 ~5.6ms；`AGENTS.md` 记录 memtier 写重 1:1 为 153K ops/s、
p99 16.7ms。与 Lavik（`git@github.com:eloqdata/lavik.git`，v0.3.0，C++23）
同口径公开数据（10M keys / 1KiB value / 16 vCPU：945K SET/s、1.16M GET/s）
存在量级差距。

差距的根因不是语言或成熟度，而是**写入单位与存储结构不匹配**：

| | NexusDB | Lavik |
|---|---|---|
| 写一个 32B value | 定位 16KB 页 → 整页重写（+ WAL 双写） | append 一条 ~70B 记录 + 改内存索引指针 |
| 写放大（稀疏随机写） | 数百 × | ≈ 1× |
| 磁盘访问模式 | 页随机更新 + 1MB chunk 整块刷 | 纯顺序追加 |

三点具体机制：

1. **16KB 页粒度**：`PAGE_SIZE = 16 * 1024`（`crates/page/src/page_io.rs`），
   `CHUNK_SIZE = 1MB`。改 32 字节最终要把整个 16KB 页重写落盘。
2. **页内 key 密度决定摊还**：一页装 ~200–500 个小 entry。
   key 密集 → 一页的写被多个 key 摊还；key 稀疏（随机散布大 keyspace）→
   每页只有一两条更新 → 放大幅接近 `16KB / 单条大小`。
3. **COW 冷页重刷**：`pager_write.rs` 中 chunk 已 flush 换出后，再改该页要走
   "alloc 新 pid"的纯 COW 路径，需要读旧页再写新页；同一逻辑页会被反复写盘。

**NVMe 硬件侧不构成 16KB 的理由**：主机逻辑块为 512B/4KB，NAND flash page
多为 4–16KB 且普遍支持 4KB partial-page program，erase block 为 64KB–数 MB，
FTL 自身写放大典型 ~2–3×，且 FTL **故意对 OS 屏蔽** page/erase-block 细节。
也就是说：**应用层按 4KB 对齐追加小记录完全可行，FTL 会自行合并**。
16KB 页是 LCB-Tree 自身的选择，不是硬件约束。

### 1.3 为什么不能直接删掉映射层

`meta_cache`（`vpid → PidLocation`，8B/项，10MB 常驻）看起来是纯开销，
但它是 **COW 页迁移的承重结构**：

- B+Tree 内部/叶子引用稳定的 `vpid`；
- COW 把页搬到新物理位置时，**只改 `vpid→pid` map，树结构一个字节不动**；
- GC 因此能以**页粒度**批量搬家（搬一页 = 1 次 8 字节 map 更新，捎带该页全部 key）。

若直接删除映射层、改为记录级追加，则：
- 搬家后必须更新索引条目 → 索引页重写（GC 代价上升）；
- 快照失去 COW 免费提供的历史页 → 需 MVCC 或序列号版本；
- **GC 需要全量重构**。

这与"改进要迅捷"的目标冲突，因此 Phase 1 采取保守切分。

---

## 2. 设计空间与结论

### 2.1 评估过的方案

| 方案 | 写放大 | 读路径 | GC | 快照 | 映射层 | 结论 |
|---|---|---|---|---|---|---|
| A. 保留 COW，仅降 page size | 中 | 好 | 需重构 | COW 免费 | **meta 条目数上升** | ✗ meta 压力转嫁 |
| B. 改非 COW in-place B+Tree | 仍高（页级） | 好 | 需 WAL redo | **丢失**，需 MVCC | 可删 | ✗ 不解决核心问题且丢快照 |
| C. 纯 LSM（memtable + 有序 run） | 低 | **读放大** | compaction 归并 | 序列号 | 可删 | ✗ 读路径代价与 SQL 范围扫冲突 |
| D. 每页一个 append buffer（页级 delta） | 低（有局部性时） | 好（局部化） | 半死页折叠 | COW 保留 | 保留 | △ 只吃"局部性"饭，随机写批不动 |
| E. **保留 COW + 前置 memtable** | **中（批量摊还）** | **好** | **保持页粒度 map 更新** | **COW 保留** | **保留** | ✅ **Phase 1 采用** |

### 2.2 为什么选 E

关键认识：**"普通写放大"和"GC 便宜"是两个独立问题，有两个独立解。**

- 普通写放大 → 由 **memtable（内存有序写缓冲）** 解决：随机写先批量落内存，
  折叠时整批有序写入，使每个受影响叶子页**每批只 COW 一次**，而不是每笔一次。
- GC 便宜 → 由 **vpid→pid 映射层** 解决：搬家只改 map，不碰树。

两者不冲突，可叠加。方案 E 因此同时满足：降写放大、GC 不重构、快照不丢、
映射层不动、读路径不变。代价是引入一个**有界内存**的 memtable。

### 2.3 明确不做的事（Phase 1 范围外）

- 不删除 `meta_cache` / vpid 间接层；
- 不改 COW 语义、崩溃恢复顺序不变量（data → meta → pid.state）；
- 不引入 MVCC（当前事务已由 RC + RYOW + OCC 覆盖，见 `AGENTS.md` F61/F62）；
- 不改 RESP/SQL/MySQL/PG/HTTP 任一门面行为；
- 不替换 `scheduler` 协程模型或 io_uring 后端。

---

## 3. Phase 1：memtable + 批量折叠（核心）

### 3.1 目标

把"每笔写触发一次页 COW"变成"每批写触发一次页 COW"，在不触碰映射层与 GC 的
前提下降低随机写放大，并量化收益。

### 3.2 结构

```
写路径:
  SET k v
    ├─ WAL append（保持不变， durability 仍由 WAL 保证）
    └─ memtable.insert(k, v)          ← 新增：内存有序缓冲，有界

memtable 满 / 定时触发:
    fold(memtable) → 有序批量插入 COW B+Tree
      · 同一批内同 key 只保留最后版本
      · 连续有序 key 命中同一 leaf → 该 leaf 每批只 COW 一次
      · split 仍走 PageWriteBatch，批量提交
    → 新 memtable（旧的可按需保留为不可变快照源，Phase 1 可先直接丢弃）

读路径:
  GET k
    ├─ 1. 查 memtable（命中即返回）        ← 新增，一次内存探针
    └─ 2. 未命中 → 查 COW B+Tree（原路径不变）
```

### 3.3 memtable 选型

要求：有序（折叠时可顺序遍历，命中相同 leaf 连续）、O(log n) 插入、
可限容、零外部依赖。

候选：
1. **跳表**（LevelDB/RocksDB 方案）— 实现简单、无 rebalance 尖峰、并发友好；
2. **B+Tree / BTreeMap** — Rust 标准库 `BTreeMap` 零依赖，但插入有页面分裂搬移成本；
3. **有序 Vec + 归并** — 追加 O(1)，折叠时排序；适合"批内有序"场景。

**Phase 1 建议：`BTreeMap` 起步**（零依赖、可立即验证收益），
若 profiling 显示插入成本显著再换跳表。选择依据必须是基准数据，不是偏好。

### 3.4 容量与触发策略

- `memtable_capacity`：按**条目数**与**估算字节数**双阈值（配置项，默认值由 Phase 0 基线决定）。
- 触发折叠：任一侧达阈值即折叠；折叠在 shard 主循环内以协程推进，不阻塞读写
  （参照现有 `FlushJob` 的所有权转移模式，**禁止** `block_on_io` 串行 await）。
- 折叠期间新写进入新 memtable，保证写路径不停。

### 3.5 与现有组件的衔接点

| 现有组件 | Phase 1 变化 |
|---|---|
| `crates/storage/src/engine.rs` | `table_put` 增加 memtable 分支；`table_get` 增加 memtable 前置查询 |
| `crates/storage/src/btree.rs` | 新增 `btree_insert_batch(&[entries])`，复用 `btree_insert_from_leaf` 与 split 传播 |
| `crates/storage/src/pager_write.rs` | **不改**（`PageWriteBatch` 继续承载页级提交） |
| `crates/storage/src/meta_cache.rs` | **不改** |
| `crates/storage/src/chunk_liveness.rs` / GC | **不改** |
| `crates/storage/src/wal.rs` | **不改** |
| `crates/page` | **不改** |
| `crates/network`（协议层） | **不改** |
| `crates/shard_manager` | 仅传递配置，无逻辑变化 |

### 3.6 正确性约束（不可协商）

1. **可见性顺序**：memtable 必须保证"后写覆盖先写"对同一 shard 内可见顺序一致。
   同 `(db, table, key)` 的连续写在 memtable 内去重，保留最后版本。
2. **与异步落盘的交互**：memtable 中的 key 在折叠前不得被 nowchunk 的
   "同 key 去重"逻辑误判为已落盘（见 `AGENTS.md` "同 key 去重（异步落盘）"gotcha）。
   需明确：memtable 是 nowchunks 的**上游**，不是平行缓存。
3. **崩溃恢复**：memtable 是**易失**的，持久化真相仍是 WAL + chunk。
   恢复流程不变（WAL 重放 → recover）。禁止让 memtable 成为恢复依赖。
4. **RYOW**：事务读自己写但尚未折叠的 key，必须能穿透 memtable 读到
   （否则破坏 F61 的 read-your-own-writes 语义）。
5. **多 db / 多表**：memtable 的 key 必须带 `(db_id, table, key)` 完整定位，
   与 `BatchOp::locator()` 的既有约定一致。

### 3.7 Phase 1 验收门槛

- 正确性：`cargo test --workspace` 全绿，clippy 0 警告（沿用现有门槛）。
- 新增回归必须覆盖：memtable 命中/未命中、批内同 key 去重、
  折叠后可见性、RYOW 穿透 memtable、崩溃恢复不受影响。
- 性能：**多轮交错 A/B**（至少 5 轮交替）在同等 key 分布下，
  随机覆盖写吞吐提升 ≥ 20% 且 p99 不劣化；否则回退（保留开关，默认关闭）。
- **注**：参照 `2026-08-05` 计划的经验，微批曾出现"吞吐低 8.8%、p99 更差"而被回退。
  Phase 1 必须先用 perf/flamegraph 证明页 COW 确实是主要成本，再实施。

---

## 4. Phase 2：GC 与折叠的协同（可选，视 Phase 1 数据决定）

Phase 1 之后，折叠会成为新的"批量写入口"。可评估：

1. **折叠即压缩**：折叠时顺手做页内整理（死槽填充），减少后续 GC 压力。
2. **memtable 作为 GC 的写缓冲**：GC 搬移产生的索引更新也走 memtable 批量落，
   避免单独的散落页写。
3. **半死页延迟折叠**：页 liveness 低于阈值时不立即搬，等 memtable 积累到一定量
   再一起搬，提高每次搬移的摊还比。

**这三个都依赖 Phase 1 的真实数据，不预先承诺。**

---

## 5. Phase 3：映射层瘦身（远期，需单独评估）

只有当 Phase 1/2 稳定、且确认要走向记录级追加时才对 `meta_cache` 动刀。
前置条件（缺一不可）：

1. 定长块 + 算术寻址（`block_id × block_size`）落地；
2. 快照隔离改由序列号提供（不依赖 COW 历史页）；
3. GC 重构方案已完整设计并通过评审。

**Phase 1/2 不为此预留任何妥协。**

---

## 6. 风险与回退

| 风险 | 缓解 |
|---|---|
| memtable 插入成本吃掉收益（重蹈 2026-08-05 微批覆辙） | Phase 0 先做 perf 归因；开关默认关闭 |
| 内存增长超出预期 | 双阈值 + 折叠期新写进新 memtable；超限退化同步写树 |
| RYOW / 事务语义回归 | §3.6 约束逐条建回归测试 |
| 与异步落盘竞态 | 明确 memtable 是 nowchunk 上游；复用现有同 key 去重机制的单点 |
| 崩溃恢复行为变化 | memtable 严格易失，恢复流程零改动，用 kill -9 测试验证 |

**回退策略**：`memtable` 由配置项控制（`storage.memtable` = `off` / `on`），
默认 `off`；任一阶段不达门槛即回退到原路径，代码保留但不启用。

---

## 7. 基准方法学（所有阶段通用）

1. **环境固定**：同机、release、io_uring、固定 shard/worker 数、固定连接数与 pipeline；
   数据目录每次**全新**（`2026-08-05` 计划已因复用目录导致数据不可作为 A/B 门槛）。
2. **交错 A/B**：同一脚本内交替运行实验/对照路径，≥ 5 轮，报告均值与方差；
   **不接受单次采样**。
3. **负载矩阵**：至少覆盖
   - 随机覆盖写（固定 keyset）
   - fresh write（keyset 之外随机写）
   - 读混合 1:10
   - 写重 1:1
4. **value 大小至少两档**：32B（对齐历史基线）与 1KiB（对齐 Lavik 口径）。
5. **每次改动记录**：`wal_mode`、`io_backend`、`memtable` 配置、测试通过数。

---

## 8. 待决问题（实施前需确认）

1. memtable 的数据结构：`BTreeMap` 起步 vs 直接跳表？（建议前者，用数据说话）
2. memtable 容量默认值：需 Phase 0 基线后确定。
3. 折叠触发：仅容量阈值，还是也要时间阈值（控制尾延迟）？
4. memtable 是否需要按 `(db, table)` 分片以支持并行折叠，还是单实例顺序折叠即可？
5. Phase 1 是否保留"旧 memtable 作为不可变快照源"（为将来 Phase 3 快照隔离铺垫），
   还是折叠后立即丢弃？

---

## 9. 参考

- `docs/DESIGN.md` §3.4（Per-Shard 调度器）、§4.3–§4.7（Storage）
- `docs/AGENTS.md`：F60 WAL 三档、F61/F62 事务、async 落盘 gotchas
- `docs/CHANGELOG.md`：F33–F44（GC/写路径历史）
- `docs/plans/2026-08-05-resp-write-path-performance.md`：基准方法学与失败教训
- Lavik 对照：`E:\\lavik`（`include/lavik/storage/detail/record_index.h`、
  `record_location.h`、`src/storage/engine/tomb_raider.cpp`）
