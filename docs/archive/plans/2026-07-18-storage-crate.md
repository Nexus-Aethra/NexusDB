# Storage Crate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现一个独立 Rust crate `storage`, 承担 NexusDB 单 shard 内部"虚拟页 → 物理页 → 真实磁盘"的对接层, 在 `scheduler` 的协程 + io_uring 之上, 把 `page` crate 提供的逻辑页操作落到磁盘. 公共 API 可被未来 B+Tree / WAL 模块直接 `use`.

**Architecture:**

```
              ┌─────────────────────────────────────┐
              │   StorageEngine (facade)           │
              │   open / put / get / delete / range │
              └────────────────┬────────────────────┘
                               │
              ┌────────────────┴────────────────────┐
              │  Pager: read/write/create 派发器    │
              │   cache hit → 直接返回              │
              │   miss     → 走 io_uring 异步读     │
              └───┬──────────────────────────┬──────┘
                  │                          │
   ┌──────────────▼─────────┐    ┌───────────▼─────────────┐
   │  MetaCache             │    │  ChunkLRU (in-mem cache)│
   │  vpid→pid 映射表      │    │   chunk 1MB / LRU      │
   │  两层数组+预读+LRU-最近邻│    └───────────┬────────────┘
   └──────────────┬─────────┘                │
                  │                          │
   ┌──────────────▼──────────────────────────▼─────────────┐
   │  ChunkWriter (now_chunk 缓冲)                          │
   │   pending_pages + pending_vpids + writev + fsync       │
   └──────────────────────────┬──────────────────────────────┘
                              │
              ┌───────────────┴────────────────┐
              │  scheduler::io_ops            │
              │   read / write / fsync        │
              └───────────────────────────────┘
```

**Tech Stack:**
- Rust 2024 edition, `page = { path = "../page" }`, `scheduler = { path = "../scheduler" }`
- `tempfile` (dev-dep), `thiserror` (^2)
- 标准库 `std::sync::{Mutex, atomic::{AtomicU64, AtomicU32, AtomicU8, Ordering}}`

**关联 design doc:** [`../../DESIGN.md`](../../DESIGN.md) §4.3 (寻址), §4.4 (写路径), §4.5 (读路径), §4.7 (崩溃恢复)

**Chunk 缓存管理 (三层架构, 与 LSM 思想一致):**

```
            ┌─────────────────────────────────────────────────────┐
            │  ① chunk_list (读 LRU 缓存, 不可修改)               │
            │     通用读取, 本质: 大小 = 1 chunk 的顺序存储        │
            │     替换: LRU (默认 8 个 chunk = 8MB)               │
            └────────────────────────▲────────────────────────────┘
                                     │ 写盘完成后迁入
            ┌────────────────────────┴────────────────────────────┐
            │  ② WriteQueue (写时队列)                            │
            │     nowchunks 触发持久化时创建同大小内存块替换       │
            │     旧 nowchunks 入队, 等 io_uring 写完迁入 chunk_list│
            └────────────────────────▲────────────────────────────┘
                                     │ 触发持久化(时间/计数/满)
            ┌────────────────────────┴────────────────────────────┐
            │  ③ nowchunks (LSM 写缓冲, n×1MB, 默认 n=4)         │
            │     写入重映射: vpid 旧位置在 chunk_list,            │
            │     触发变更时分配 nowchunks 内新 pid, 更新 MetaCache │
            │     持久化触发: 固定时间 / 写次数 / 写满             │
            └─────────────────────────────────────────────────────┘
```

**Catalog 设计 (T9-T11):**
- **三层目录**: MetaPage (db_name → table_dir_root_vpid) → TableDirectory (table_name → table_root_vpid) → Table BTree (用户数据)
- **MetaPage 硬编码 chunk 0 page 0**: 启动时第一个读, 是整个 catalog 树的根
- **Write-through cache**: DbRegistry HashMap 与 BTree 同步, HashMap 是 BTree 的镜像 (cache 永不超前)
- 复用 page crate 的 leaf BTree 编码, 不引入新的存储结构

**关键设计原则:**
- 所有 hot path 在 **单线程** 内 (per-shard 线程), 用 `Rc<RefCell<...>>` 而非 `Arc<Mutex<...>>`.
- 分配计数器 (vpid / pid) 用 `AtomicU64` / `AtomicU8` 提供 `Sync` 跨线程接口, 但 hot path 仅在单线程内.
- 复用 scheduler 的 `io_ops::read/write/fsync`, **不**直接接触 `io_uring`.

**⭐ Scheduler 多线程使用契约 (storage 实施必须严格遵守):**

```
一个 Scheduler = 一个 io_uring = 一个驱动线程.
driver loop 在该线程永久 run_until_idle.
spawn / drive / JoinHandle::poll 全在这同一线程.
跨 shard 通信用 mpsc channel (不用 JoinHandle 跨线程).
```

违反任一条 — `JoinInner::UnsafeCell` 跨线程 race → 永久 hang.

---

## Chunk 缓存管理设计 (核心, T4-T5 实施)

### 3.0 一致性控制: Per-Chunk 协程级锁 (🆕 关键)

**问题:** 每个 task 在无 IO 阻塞时是天然串行的, 但一旦走 `io_ops::read/write/fsync` 触发 await, 当前协程被挂起, 调度器可切到其他协程. 如果另一个协程也想操作**同一个 page**, 会出现"两个协程轮流操作同一 page"的竞态.

**解决思路:** 每个 chunk 一个等待队列, 一个协程成为 owner 去触发 IO, 其他协程加入等待队列. owner 完成 IO 后按 FIFO 顺序唤醒 waiters.

**数据结构:**
- `ChunkWaiter { owner: Option<JoinHandle>, waiters: VecDeque<JoinHandle>, loading: bool }`
- `ChunkLockMap = HashMap<ChunkKey, ChunkWaiter>` (Pager 字段持有)

**关键不变量:**
- 同一 chunk 同一时刻只有一个 owner, 其他协程进入等待队列
- owner 完成 IO 后, 按 FIFO 顺序唤醒 waiters
- 不同 chunk 互不阻塞, 真正的并行
- chunk 已在 cache 时无需加锁 (peek 命中, 走快路径)

**与 scheduler 的协作:**
- `Scheduler::park_current_coroutine()` — 新 API, 协程主动放弃执行权
- `Scheduler::ready_queue.push_back(handle)` — 显式将协程放回 ReadyQueue

**chunk_lock + Split 交互 (完整时序, read-after-write 一致性):**
- Writer 申请 chunk_lock owner → split leaf → PageWriteBatch::submit (memcpy 到 nowchunks) → meta_cache.write → 释放 owner → 唤醒 Reader
- Reader 三源查找: nowchunks > WriteQueue > chunk_list, nowchunks 优先保证读到新数据

**⭐ read-after-write 接受窗口:** Writer 完成 PageWriteBatch::submit 后在 meta_cache.write 之前, Reader 可能读到旧 chunk_list 数据. 与 LSM 一致, 窗口长度微秒级, 用户可显式调 `Pager::get_latest(vpid)` 重读.

### 3.0.1 B+Tree Travel Key Path (🆕 解决 split 传播一致性)

**问题:** B+Tree 的 insert/delete 操作从 root 一路 travel 到 leaf, 沿途记录 vpid 栈. 如果中途另一个协程分裂了栈中某个 page, 向上回推时用错 vpid → 索引永久错位.

**解决思路:** travel path 不再记录 vpid, 而记录"选择的 key". 配套 travel_tree (BTreeMap<key, vpid>) 跟踪最新映射. split 传播时广播更新所有 task 的 travel_tree.

**核心结构:**
- `TravelTree { map: BTreeMap<Vec<u8>, Vpid> }` — per-task, 向下 travel 时 record, 向上回推时 lookup
- split 传播: 用右节点 cp 段首 key 算 right_lo, 遍历所有 task 的 travel_tree, value == old_vpid 且 key >= right_lo 的条目更新为 new_vpid

**⭐ TravelTree RAII Guard:** `TravelTreeGuard<'p>` 创建时自动 register, drop 时自动 unregister (无论正常返回 / panic / cancel).
- Pager 维护 `HashMap<TaskId, TravelTree>`, split 时广播更新
- 左节点 vpid 不动 (page crate 保证), 右节点 vpid 来自 nowchunks 分配

### 3.0.2 Root Split 时的运行时可见性

**问题:** root split 后树高+1, 新 root vpid 与旧 root vpid 不同. 其他正在运行的 task 怎么办?

**答案: 不需要显式通知, 但保证"每次新 travel 读 fresh root_vpid".**

关键洞察: root split 后旧 root vpid 仍然有效 (作为新 root 的 left_child), 其内容被截断但仍在树里. mid-travel 任务不受影响, 因为它们在旧 root 的路径上已选好 child. 每个新 travel 从 `meta_cache.read_root_vpid()` 拿最新值.

**三层并发控制:**
1. chunk_lock: 字节层 — 同 chunk 内串行读 page 字节
2. travel_key_path: tree 逻辑层 — split 传播时更新栈路径
3. fresh root_vpid: 全局入口层 — 每次新 travel 拿最新 root

### 3.0.3 MetaCache 滑动窗口同步机制 (映射表持久化协议)

**MetaCache 物理布局:**
- 10MB Data Array + 10×1MB Index Entry (每个覆盖 128K vpid slot)
- 启动时只预热 index 0, 其余按需加载
- window 替换策略: **最近邻** (找离 current_vpid 最远的 entry 作 victim)

**两个核心同步点:**

**A. Window 替换时的回写协议 (evict-before-load):**
- 选 victim → 如果 dirty, 先 fsync 回 page.mate → 清空 slot → 加载新窗口
- dirty window 永不直接丢弃

**B. nowchunks flush 与 meta flush 的协调 (data → meta 顺序):**
- 严格顺序: page data → .block → fsync → dirty meta windows → page.mate → fsync
- 原因: data 优先, 哪怕 meta 暂时落后, 至少 page data 是对的. 反之先 meta 后 data 会导致读到错数据.

**MetaCache 内部 dirty tracking:** `IndexEntry.dirty` 字段, `flush_all_dirty` 批量写回. 组提交 (group commit): 时间触发 (1000ms) + 写次数触发 (256) + 写满触发.

### 3.0.4 nowchunks / WriteQueue 的 Pin 需求分析

**TL;DR:** 写路径无 pin 需求. 读路径在 in-flight 状态下有等待需求, 但现有 chunk_lock 机制已隐式处理.

**三种 chunk 状态可见性:**
| 状态 | 数据位置 | 处理 |
|---|---|---|
| 已 flush | chunk_list | 直接读, 无等待 |
| nowchunks 中 | nowchunks.current 内存 | peek + clone, 无等待 |
| WriteQueue 中 | WriteQueue.queue | 等 chunk_lock owner 完成, 然后读 chunk_list |
| 在磁盘 | .block 文件 | io_uring read, 需等待 |

**关键不变量:**
- 写路径零等待: nowchunks.write_page 永远 memcpy 写内存
- 读路径只在 in-flight 时等待
- chunk_lock owner 自动串行化, 不引入新的 pin / ref count

### 3.0.5 Page COW 借用/拥有 + Multi-Page 同步写回 (一致性基石)

**两层访问模型:**
- **Layer 1: 查找阶段 — BORROW** (借用, 不复制): `Pager::read_page(vpid) -> PageRef<'_>`, 零拷贝, 共享 chunk_list 字节
- **Layer 2: 写入阶段 — OWN** (拥有, COW 复制): `Pager::take_page_for_write(vpid) -> [u8; PAGE_SIZE]`, 拿到独立副本自由修改

**Multi-Page Sync 接口 (核心):**
- `PageWriteBatch { pages: Vec<(Vpid, [u8; PAGE_SIZE])>, bytes_total: usize }`
- `batch.add(vpid, page)` → `batch.submit(nowchunks) -> Vec<(Vpid, PidLocation)>`
- 硬限制: `MAX_BATCH_BYTES = 256KB` (16 page), 超限 panic
- 原子性: 整个 batch 所有 page 都 memcpy 到 nowchunks 才算完成, 中途 panic 不会半完成

**关键不变量:**
1. Borrow 期间不复制
2. Own 触发 COW: take_page_for_write 必返回独立 `[u8; PAGE_SIZE]`
3. 多 page 写回必走 batch, 不允许单独 write_back
4. batch 内部不中断: 单线程内连续 memcpy, 中途不可 await
5. MetaCache 写回在 batch 完成后 (data → meta 顺序)

**T17b 优化 — 写放大修复:**
- 问题: 原 flush 每 dirty chunk 做 disk_read + merge + disk_write, 写 1 page 触发 64x 写放大
- 修复: `PageWriteBatch::submit` 在 alloc pid 前查 `MetaCache::is_dirty(vpid)`. 如果 dirty (in nowchunks), 复用原 pid; 否则 COW alloc 新 pid
- 收益: 写放大 64x → 1x, 零 disk read, 节省 page_idx 槽位

### 3.3.2 COW 与 meta_cache 的关系 (T17b 决策记录)

**核心结论: 不需要 MVCC** (至少在单线程 runtime 阶段).

**三个关键事实:**
1. meta_cache 是 vpid → pid 映射, write 覆盖映射但不删除旧 pid 的 page
2. COW 保证旧 pid 的 page 永远在 disk
3. Pager::read 走 meta.read(vpid) → 当前最新 pid, 不暴露读历史 pid 的 API

**为什么不需 MVCC:**
- 单线程 runtime + Pager `&mut self` 强制串行, 任意时刻只有一个协程在 Pager 上执行
- COW 已经天然保留历史 page
- 当前没有"长事务 + 跨多次读"需求

**未来 Snapshot API (事务需要):** clone meta_cache 视图 (HashMap<vpid, pid>) 即可, 零额外存储, 零 GC, 零冲突.

### 3.4 三层交互总览

```
       ┌──────────────────────┐
       │   StorageEngine      │
       │   put / get / delete │
       └──────────┬───────────┘
                  │
       ┌──────────▼────────────┐          写路径
       │  Pager                │ ───────▶ nowchunks.write_page(new_data)
       │  create / update / read│              ↓ 满了/时间/计数
       └──────────┬────────────┘           WriteQueue.push(old_nowchunks)
                  │                              ↓ io_ops::write + fsync
       ┌──────────▼────────────┐
       │  MetaCache            │ ◀── meta.write(vpid, new_pid)
       │  vpid→pid 映射        │
       └──────────┬────────────┘         chunk_list.insert(key, bytes)
                  │                              ↓
                  │ 读路径                    满了 LRU 踢出
       ┌──────────▼────────────┐
       │  chunk_list.get_or_load│ ◀─────── 旧 chunk LRU 释放
       │  (命中返回 Arc)        │
       │  (未命中走 io_ops::read)│
       └───────────────────────┘
```

### 3.5 与 DESIGN.md 的对应关系

| 本文档概念 | DESIGN.md 对应 |
|---|---|
| chunk_list | §4.5 读取路径 + §4.3.7 缓存策略 |
| WriteQueue | §4.4 写路径: Group Commit + Chunk Flush |
| nowchunks | §4.3.6 now_chunk 管理 + §4.4 写缓冲 |
| 重映射机制 | §4.4 写入路径的"vpid 永不重用" |
| 持久化触发条件 | §4.4 末尾: "时间 / 写量 / 容量" |

---

## Global Constraints

| 约束 | 值 |
|---|---|
| Rust edition | 2024 |
| Kernel | Linux 5.6+ io_uring |
| 不引入 | `tokio` / `crossbeam` / `async-std` / `futures` / `monoio` |
| 提交粒度 | 每 Task 结束前至少一次 commit |

---

## File Structure

```
NexusDB/
├── Cargo.toml
├── crates/storage/
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs              ← pub use { StorageEngine, Pager, MetaCache, ... }
│   │   ├── types.rs            ← 重新导出 PidLocation + 核心常量
│   │   ├── meta_cache.rs       ← MetaCache: 两层数组 + 预读 + LRU-最近邻
│   │   ├── alloc.rs            ← VpidAllocator / PidAllocator / FreePageQueue
│   │   ├── chunk_writer.rs     ← NowChunks + WriteQueue + ChunkWriter
│   │   ├── chunk_lru.rs        ← ChunkList (读 LRU 缓存)
│   │   ├── pager.rs            ← Pager: read/write/create 派发
│   │   ├── recover.rs          ← 启动恢复: 扫描最后 block + 重建 alloc
│   │   ├── engine.rs           ← StorageEngine facade
│   │   ├── meta_page.rs        ← T9: chunk 0 page 0, db_name → table_dir_root_vpid
│   │   ├── table_directory.rs  ← T10: 每 db 一棵 BTree, table_name → table_root_vpid
│   │   └── registry.rs         ← T11: DbRegistry 内存缓存 (write-through)
│   └── tests/
│       ├── meta_cache_tests.rs, alloc_tests.rs, chunk_writer_tests.rs
│       ├── chunk_lru_tests.rs, pager_round_trip.rs, recover_tests.rs
│       ├── engine_e2e.rs, meta_page_tests.rs, table_directory_tests.rs
│       └── registry_e2e.rs
```

---

## 核心常量与类型 (落地时的参考)

```rust
pub use page::PAGE_SIZE;
pub const CHUNK_SIZE: usize = 1024 * 1024;            // 1 MiB
pub const BLOCK_SIZE: usize = 10 * CHUNK_SIZE;        // 10 MiB
pub const CHUNKS_PER_BLOCK: usize = 10;
pub const PAGES_PER_CHUNK: usize = CHUNK_SIZE / PAGE_SIZE;  // 64
pub const MATE_CACHE_SIZE: usize = 10 * 1024 * 1024;  // 10 MiB
pub const INDEX_SIZE: usize = 1024 * 1024;             // 1 MiB
pub const INDEX_COUNT: usize = 10;
pub const SLOTS_PER_INDEX: usize = INDEX_SIZE / 8;     // 128 K

#[repr(C, packed)]  // 必须 8B, 避免 padding 到 12B
pub struct PidLocation { pub file_id: u32, pub chunk_idx: u8, pub page_idx: u16, pub flags: u8 }

pub fn pid_to_offset(pid: &PidLocation) -> u64;   // O(1) 算术
pub fn offset_to_pid(...) -> PidLocation;
```

---

### Task 1: Workspace + Storage Crate Scaffolding

**⭐ T1 必读:** PidLocation 必须 `#[repr(C, packed)]` 保证 8B.

**Files:** Cargo.toml (members 加 `crates/storage`), crates/storage/{Cargo.toml, src/lib.rs, src/types.rs, tests/sanity.rs}

**Interfaces:** 重新导出 `PidLocation`, `PAGE_SIZE`, 定义 `CHUNK_SIZE`, `BLOCK_SIZE` 等常量, `VpidLogEntry`, `pid_to_offset`/`offset_to_pid`.

- [ ] Step 1-4: 创建相关文件 (types.rs 含 `pid_to_offset_roundtrip` + `chunk_layout_consistent` 测试)
- [ ] Step 5: `cargo build --workspace` + `cargo test -p storage` (3 passed)
- [ ] Step 6: 提交

---

### Task 2: MetaCache (两层数组 + 1MB 预读 + LRU-最近邻替换)

**⭐ T2 必读:** 替换策略用**最近邻** (非 LRU). 启动只预热 index 0. dirty window 必须 fsync 才能丢弃.

**Interface:**
```rust
pub struct MetaCache { data: Vec<u8>, index: Vec<IndexEntry>, mate: File, tick: u64 }
pub struct IndexEntry { start_vpid: u64, end_vpid: u64, data_offset: u16, dirty: bool, valid: bool, last_used: u64 }
impl MetaCache {
    pub fn open(mate_path: &Path) -> Result<Self, StorageError>;
    pub fn read(&mut self, vpid: u64) -> Option<PidLocation>;
    pub fn write(&mut self, vpid: u64, pid: PidLocation);
    pub fn flush_dirty(&mut self) -> Result<(), StorageError>;
}
```

**关键设计:** 初始全 0 (flags=0 不是 ALIVE, 所以 read 返回 None). 替换策略: 距离 current_vpid 最远的 entry 作 victim. TDD 先行.

- [ ] 测试: open 初始化 / read 返回 None / write+read 回读 / 跨 window 预读 / flush_dirty 回写 / 最近邻替换
- [ ] 实现: locate + load_window + evict-before-load + write-back
- [ ] 提交

---

### Task 3: VpidAllocator + PidAllocator + FreePageQueue

**⭐ T3 必读:** vpid 永不重用. pid_alloc 按 (file_id, chunk_idx, page_idx) 递增. chunk 满 64 pages 触发 rotate.

**Interface:**
```rust
impl VpidAllocator { pub fn alloc(&mut self) -> Vpid; }
impl PidAllocator { pub fn alloc(&mut self) -> Option<PidLocation>; pub fn rotate_to_next_chunk(&mut self); }
impl FreePageQueue { pub fn push(&mut self, pid: PidLocation); pub fn drain(&mut self) -> Vec<PidLocation>; }
```

- [ ] 测试: vpid 单调递增 / pid 递增 / chunk 满 64 返回 None / rotate 后继续分配 / FreePageQueue push + drain
- [ ] 实现 + 提交

---

### Task 4: NowChunks + WriteQueue + ChunkWriter (🆕 三层架构)

**职责:** NowChunks (LSM 写缓冲, n×1MB), WriteQueue (持久化排队), ChunkWriter (编排 flush).

**Interface:**
```rust
pub struct NowChunks { buffer: Vec<u8>, used: usize, chunk_count: usize, write_count: u64, last_flush: Instant }
impl NowChunks {
    pub fn write_page(&mut self, data: &[u8; PAGE_SIZE]) -> Result<(file_id, chunk_idx, page_idx), FullError>;
    pub fn should_flush(&self, cfg: &NowChunksConfig) -> bool;
    pub fn take(&mut self) -> Box<[u8]>;
    pub fn peek_chunk(&self, key: ChunkKey) -> Option<&[u8; CHUNK_SIZE]>;
}
```

**触发条件:** 固定时间 (1000ms) / 写次数 (256) / 写满置换. 写入重映射: 已 flush page 走 COW alloc 新 pid; in-nowchunk page 复用原 pid (T17b 优化).

- [ ] 测试: 写入 + 满触发 flush / 时间触发 / 计数触发 / WriteQueue pop / nowchunks.take 后自动清空 / peek_chunk
- [ ] 实现 + 提交

---

### Task 5: ChunkList (🆕 改名, 1MB chunk 只读 LRU 缓存)

**Interface:**
```rust
pub struct ChunkList { capacity: usize, map: HashMap<ChunkKey, Arc<Box<[u8; CHUNK_SIZE]>>>, order: VecDeque<ChunkKey> }
impl ChunkList {
    pub fn get_or_load<F>(&mut self, key: ChunkKey, load_fn: F) -> io::Result<Arc<...>>;
    pub fn insert_from_write_queue(&mut self, key: ChunkKey, data: Box<[u8; CHUNK_SIZE]>);
    pub fn invalidate(&mut self, key: &ChunkKey);
}
```

- [ ] 测试: get_or_load 加载 / LRU 满时 evict / insert_from_write_queue 插入 / invalidate 释放
- [ ] 实现 + 提交

---

### Task 6: Pager (read/write/create 派发器)

**⭐ T6 必读:** 引入 `PageRef<'a>` (借用) + `take_page_for_write` (COW) + `PageWriteBatch` 接口. 读路径走三源查找: nowchunks > WriteQueue > chunk_list > disk.

**Interface:**
```rust
impl Pager {
    pub async fn read_page(&mut self, vpid: Vpid) -> io::Result<PageRef<'_>>;
    pub async fn take_page_for_write(&mut self, vpid: Vpid) -> io::Result<[u8; PAGE_SIZE]>;
    pub fn register_travel_tree(&mut self, id: TaskId, tree: &mut TravelTree);
    pub fn unregister_travel_tree(&mut self, id: TaskId);
    pub fn split_page(&mut self, vpid: Vpid) -> io::Result<()>;  // 包装 page crate split + 广播
    // 写路径
    pub fn write_page(&mut self, vpid: Vpid, data: &[u8; PAGE_SIZE]) -> io::Result<()>;
    pub fn flush(&mut self) -> impl Future<Output = io::Result<()>>;
    pub fn close(&mut self) -> impl Future<Output = io::Result<()>>;
}
pub struct PageWriteBatch { pages: Vec<(Vpid, [u8; PAGE_SIZE])>, bytes_total: usize }
impl PageWriteBatch {
    pub fn add(&mut self, vpid: Vpid, page: [u8; PAGE_SIZE]) -> &mut Self;
    pub fn submit(self, nowchunks: &mut NowChunks, pager: &mut Pager) -> io::Result<Vec<(Vpid, PidLocation)>>;
}
```

**ChunkLock 流程:** chunk_list 命中直接返回 → miss 则申请 owner → 二次检查 → 三源查找 (nowchunks > WriteQueue > disk) → 插入 chunk_list → 唤醒 waiters.

**chunk_lock owner 释放时机:** 必须 PageWriteBatch::submit + meta_cache.write 都完成才释放. 持有期 = 隐式 pin chunk.

- [ ] 测试: write + read 回读 / 多 page roundtrip / 跨 chunk 操作 / create_page + 写满 / PageWriteBatch submit
- [ ] 实现 + 提交

---

### Task 7: recover.rs (扫描最后 block + MetaCache union 重建)

**⭐ T7 必读:** recover 扫描 `block_dir/*.block` (按 file_id 排序), 读 page header 重建 MetaCache. MetaCache union 语义: 加载 page.mate (可能 stale) + scan .block (authoritative). pid_alloc 状态从上次 seen chunk + page_idx 推导.

**简化:** 第一版用 page header 自描述, 不解析 vpid log 格式.

**Interface:**
```rust
pub fn recover(block_dir: &Path, meta_cache: &mut MetaCache, pid_alloc: &mut PidAllocator, vpid_alloc: &mut VpidAllocator) -> io::Result<()>;
```

- [ ] 测试: 空目录 recover / 写 1 page 后 recover / 多 chunk 后 recover / 磁盘损坏 (page header 乱) 优雅失败
- [ ] 实现 + 提交

---

### Task 8: StorageEngine facade + 端到端 put/get/range + 收尾

**Interface:**
```rust
pub struct StorageEngine { pager: Pager, meta_cache: MetaCache, pid_alloc: PidAllocator, vpid_alloc: VpidAllocator, ... }
impl StorageEngine {
    pub async fn open(options: OpenOptions) -> io::Result<Self>;
    pub async fn put(&mut self, vpid: Vpid, data: &[u8; PAGE_SIZE]) -> io::Result<()>;
    pub async fn get(&mut self, vpid: Vpid) -> io::Result<Option<[u8; PAGE_SIZE]>>;
    pub async fn flush(&mut self) -> io::Result<()>;
    pub async fn close(&mut self) -> io::Result<()>;
}
```

- [ ] 测试: put + get 回读 / 写满多 chunk 后 read / 跨 reopen 持久化 / 多 engine 实例隔离
- [ ] 清理 `#![allow(dead_code)]` (逐模块). cargo clippy 零警告.
- [ ] 更新 AGENTS.md 进度表. 提交.

---

## Catalog 层 (在 T1-T8 基础上叠加, 解决"分表"问题)

### Task 9: MetaPage (chunk 0 page 0, db_name → table_dir_root_vpid)

**⭐ T9 必读:** MetaPage 硬编码 chunk 0 page 0, 是整个 catalog 树的根. 用 BTreeMap 镜像 + 整页重写 flush. 用 page crate leaf page 编码.

**Interface:**
```rust
pub struct MetaPage { page_buf: [u8; PAGE_SIZE], entries: BTreeMap<String, Vpid>, dirty: bool }
impl MetaPage {
    pub fn open(pager: &mut Pager) -> impl Future<Output = io::Result<Self>>;
    pub fn resolve(&self, db_name: &str) -> Option<Vpid>;
    pub fn register(&mut self, db_name: &str, dir_root_vpid: Vpid);
    pub fn unregister(&mut self, db_name: &str);
    pub fn flush(&mut self, pager: &mut Pager) -> impl Future<Output = io::Result<()>>;
}
```

- [ ] 测试: open 空文件 / register + resolve / unregister / flush 后 reopen 恢复 / 多 db 项 / list_dbs
- [ ] 实现 + 提交

---

### Task 10: TableDirectory (每个 db 一棵 BTree, table_name → table_root_vpid)

**⭐ T10 必读:** 每个 db 一个 TableDirectory, 单 leaf page BTree. 假设每个 db < ~200 table. 复用 page crate leaf CRUD. TableDirectory 必须不持有 raw pointer 到 Pager (使用 `PhantomData<*mut Pager>` 实现 !Send/!Sync).

**Interface:**
```rust
pub struct TableDirectory { _marker: PhantomData<*mut Pager>, root_vpid: Vpid }
impl TableDirectory {
    pub async fn open(pager: &mut Pager, root_vpid: Vpid) -> io::Result<Self>;
    pub async fn flush(&mut self, pager: &mut Pager) -> io::Result<()>;
    pub async fn resolve(&self, pager: &mut Pager, table_name: &str) -> io::Result<Option<Vpid>>;
    pub async fn register(&mut self, pager: &mut Pager, table_name: &str, root_vpid: Vpid) -> io::Result<()>;
    pub async fn unregister(&mut self, pager: &mut Pager, table_name: &str) -> io::Result<()>;
}
```

- [ ] 测试: open + resolve / register + resolve / 写满后 flush reopen / 跨 reopen 持久化 / list_tables
- [ ] 实现 + 提交

---

### Task 11: DbRegistry (write-through cache) + 多 db / 多表 API + e2e

**⭐ T11 必读:** DbRegistry = write-through cache: HashMap 是 BTree 的镜像, cache 永不超前. 先写 BTree 成功, 再写 HashMap. 多 db 隔离: 不同 db 的 table 名完全独立.

**Interface:**
```rust
pub struct DbRegistry { current_db: Option<String>, db_cache: HashMap<String, DbHandle> }
pub struct DbHandle { meta_page: MetaPage, tables: HashMap<String, TableDirectory> }
impl StorageEngine {
    pub async fn create_db(&mut self, name: &str) -> io::Result<()>;
    pub async fn drop_db(&mut self, name: &str) -> io::Result<()>;
    pub async fn open_db(&mut self, name: &str) -> io::Result<()>;
    pub fn use_db(&mut self, name: &str);
    pub fn list_dbs(&self) -> Vec<String>;
    pub async fn create_table(&mut self, name: &str, schema: &[u8]) -> io::Result<()>;
    pub async fn table_put(&mut self, table: &str, key: &[u8], value: &[u8]) -> io::Result<()>;
    pub async fn table_get(&mut self, table: &str, key: &[u8]) -> io::Result<Option<Vec<u8>>>;
    pub async fn table_delete(&mut self, table: &str, key: &[u8]) -> io::Result<()>;
}
```

- [ ] 测试: create_db + list_dbs / open_db + use_db / create_table + table_put + table_get / 多 db 隔离 / reopen 跨 session 持久化
- [ ] 实现 + 提交

---

## 总体收尾 (T1-T11 全部完成后)

- [ ] 更新 AGENTS.md 进度表 + 已知 issue
- [ ] 提交文档更新

---

## 已知遗留 / 后续 Task (不在本 plan 范围, 留作 polish 阶段)

| 任务 | 备注 |
|---|---|
| Group commit (DESIGN §4.4 真 writev) | 当前每 page 一次 write, 后续统一为 writev |
| vpid free list 持久化格式 | 当前 hack 用 PidLocation 8 字节当 next_free 存储 |
| 真正的 vpid log 解析 | 当前用 page header 自描述 |
| COW: 旧 page 进 FreePageQueue | T3 FreePageQueue 已就位, T6 暂未回收 |
| 多 block (10MB) 切换 | 当前所有 page 在 file 0 |
| Recovery 时 vpid log replay | 当前用 page header 自描述 |
| TableDirectory 单 leaf page 限制 | < ~200 table, 超需 internal page |
| Table BTree 多页分裂 | put 多于 ~200 keys 需 internal page |
| drop_db 旧 page 不回收 | 只删 MetaPage 项 |
| MetaPage 整页重写 | db 数量 < 100 时性能可接受 |

---

## 风险与依赖

| 风险 | 缓解 |
|---|---|
| scheduler 没提供 `block_on` 顶层 API | 用 spawn + drive_until_idle 替代 |
| PidLocation 字段用于 free list 偷存 next_free | T3 hack 跑通, T11 polish 时引入单独结构 |
| MetaPage 在 chunk 0 page 0 硬编码 | StorageEngine::open 强制先写 MetaPage 占位 |
| DbRegistry HashMap 与 BTree 不一致风险 | write-through 协议: 先写 BTree 成功, 再写 HashMap |
| 测试需要 io_uring 实例 | `cargo test --test-threads=1` |

---

## 关联文档

- [`DESIGN.md`](../../DESIGN.md) §4.3-§4.8
- [`../specs/2026-07-17-scheduler-crate-design.md`](../specs/2026-07-17-scheduler-crate-design.md)
- [`../plans/2026-07-17-scheduler-crate.md`](../plans/2026-07-17-scheduler-crate.md)
- [`../plans/2026-07-17-page-item-revision.md`](../plans/2026-07-17-page-item-revision.md)