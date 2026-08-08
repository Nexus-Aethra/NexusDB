# ShardManager Crate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `storage` crate (T1-T11 已完成) 之上构建 `shard_manager` crate, 实现多 db / 多 shard / 2PC 跨 shard 协调的统一管理. 把当前"单 StorageEngine 单 db namespace"扩展为"多 StorageEngine (per shard) + 跨 db metadata 共享 + 2PC 跨 shard 同步".

**关联 design doc:** [`../../DESIGN.md`](../../DESIGN.md) §3.1 (Per-Shard), §3.4 (调度), §4.5 (StorageEngine), §4.6 (Multi-DB)

**关联 storage plan:** [`2026-07-18-storage-crate.md`](2026-07-18-storage-crate.md) — T1-T11 已完成, 提供本 plan 的基础

---

## 1. 核心架构 (3 层物理隔离)

```
block_root/                              ← 根 (由 ShardManager 配置)
├── db_app/                              ← db "app" 物理隔离
│   ├── shard_0/                         ← shard 0 (per-shard 独立)
│   │   ├── 000001.block
│   │   ├── 000002.block
│   │   └── page.mate
│   ├── shard_1/                         ← shard 1
│   │   ├── 000001.block
│   │   └── page.mate
│   ├── shard_2/
│   └── shard_3/
├── db_logs/                             ← db "logs" 物理隔离 (与 app 完全不共享)
│   ├── shard_0/  (独立 StorageEngine 实例 + 独立 8MB ChunkList)
│   ├── shard_1/
│   ├── shard_2/
│   └── shard_3/
└── db_metrics/
    └── shard_0..3/
```

**3 层物理隔离 = 3 层独立：**

| 维度 | 实现 | 路由 | 范围 |
|---|---|---|---|
| **db** | `db_{name}/` 子目录 | `db_name` → DbId → 路径 | 完全独立 |
| **shard** | `shard_{N}/` 子目录 | `hash(key) % N` → shard_id | 单 db 内跨 shard 独立 |
| **block** | `000001.block` 文件 | `pid.file_id` → 文件 | 同 (db, shard) 内独立 |

**单 shard 内部：** 一个 StorageEngine 实例跨所有 db, 内部组件用 (DbId, ...) 复合 key 共享.

```
Shard N (单线程, 单 io_uring):
  ┌──────────────────────────────────────────────────────────────┐
  │ scheduler: Scheduler                                          │
  │ engine: StorageEngine (跨所有 db, 内部 key 都加 db 维度)       │
  │   ├─ pager.meta:        MetaCache        HashMap<(DbId, vpid), PidLocation>│
  │   ├─ pager.vpid_alloc:  VpidAllocator    HashMap<DbId, next_vpid>          │
  │   ├─ pager.pid_alloc:   PidAllocator     HashMap<DbId, (file, chunk, page)> │
  │   ├─ pager.chunk_list:  ChunkList        HashMap<(DbId, fid, cidx), Arc<Vec<u8>>>  ← 8MB LRU 跨 db 共享
  │   ├─ pager.nowchunks:   NowChunks         HashMap<(DbId, fid, cidx), ChunkBuf>    ← 跨 db 共享 buffer pool
  │   ├─ pager.write_queue: WriteQueue        VecDeque<(ChunkKey, Vec<u8>)>           ← 跨 db 共享队列
  │   ├─ pager.writer:      ChunkWriter       HashMap<(DbId, file_id), File>          ← per-(db, file) 句柄
  │   └─ registry:          DbRegistry        跨所有 db 的 catalog 镜像               │
  └──────────────────────────────────────────────────────────────┘
```

**关键不变量 (per shard)：**
- 整个 shard 单线程使用 (无锁)
- `DbId` (u32) 是内部所有 key 的 db 维度
- `db_name → DbId` 映射由 `DbNameResolver` 维护, 持久化到 MetaPage
- `chunk_list` 8MB 跨 db 共享 LRU, 不用 per-db 独立

**关键不变量 (per db)：**
- `pid` 和 `vpid` 在 (db, shard) 命名空间内独立
- vpid 0 = 该 (db, shard) 的 MetaPage
- 不同 db 的 vpid 0 物理上不同 (不同文件), 不会冲突

---

## 2. 与之前决策的差异

**vs 之前"每 db 重复元数据"方案：**

| 维度 | 之前 | 现在 (用户新方案) |
|---|---|---|
| 目录层级 | 2 层 (root/db/) | 3 层 (root/db/shard/) |
| shard 物理化 | 逻辑 (ShardManager 内存路由) | 物理 (shard 目录) |
| 单 shard 实例数 | 1 StorageEngine | 1 StorageEngine (但内部跨 db) |
| 内存 | 8MB × N_dbs × N_shards (100 倍浪费) | 8MB × N_shards (1 倍) |
| 删 db 缓存清理 | 难 | 易 (清 db 维度 entries) |
| 物理隔离性 | 弱 (同一文件) | 强 (独立目录) |

**与之前"2PC over put"决策的差异：**

| 操作 | 之前 (2PC over put) | 现在 (单 shard 路由 put) |
|---|---|---|
| create_db | 2PC | 2PC (仍 2PC, 跨 N shard) |
| drop_db | 2PC | 2PC |
| create_table | 2PC | 2PC (跨 N shard 写 TableDirectory) |
| drop_table | 2PC | 2PC |
| **put** | **2PC** | **1PC (单 shard 写数据, 无 metadata 变更)** |
| **get** | 1PC | 1PC |
| **delete** | 1PC | 1PC |

**理由：** put 只改一个 shard 的 table BTree 内部 (单 shard 内 PageWriteBatch 已保证原子性), TableDirectory 只在 create/drop table 时变. 所以 put 是 1PC, metadata 操作走 2PC.

---

## 3. ShardManager 核心组件

### 3.1 ShardManager

```rust
pub struct ShardManager {
    /// 根目录: block_root/{db_name}/shard_{N}/{*.block, page.mate}
    block_root: PathBuf,
    /// shard 总数
    num_shards: usize,
    /// IO backend
    io: IoBackend,
    /// 每个 shard 一个独立 StorageEngine
    engines: Vec<StorageEngine>,  // length = num_shards
    /// 每个 shard 一个独立 scheduler (T13 集成 scheduler crate)
    schedulers: Vec<Scheduler>,
    /// db_name → DbId 映射 (ShardManager 级共享, 不进 engine)
    name_resolver: DbNameResolver,
    /// 2PC coordinator 状态
    coordinator: TwoPhaseCoordinator,
    /// ShardHandle: mpsc channel for cross-thread comm (T13 实施)
    handles: Vec<ShardHandle>,
}

pub struct ShardConfig {
    pub block_root: PathBuf,
    pub num_shards: Option<usize>,  // None = thread::available_parallelism() / 2
    pub io: IoBackend,
    pub chunk_cache_size: usize,  // 单 shard 的 ChunkList 容量 (默认 8MB)
    pub create_if_missing: bool,
}
```

### 3.2 DbNameResolver

```rust
/// 全局 db name → id 映射, 持久化到所有 shard 的 MetaPage (因为 metadata 重复)
pub struct DbNameResolver {
    names: Vec<String>,              // id → name
    name_to_id: HashMap<String, u32>,
}

impl DbNameResolver {
    pub fn new() -> Self;
    pub fn get_or_create(&mut self, name: &str) -> DbId;
    pub fn resolve(&self, name: &str) -> Option<DbId>;
    pub fn name(&self, id: DbId) -> Option<&str>;
    pub fn list(&self) -> Vec<(DbId, &str)>;
}
```

**持久化：** 序列化到 MetaPage 头部 (固定 64 字节头), 所有 shard 同步 (因为每 shard 都有 MetaPage 副本, write 走 2PC).

### 3.3 ShardHandle (T13 实施后)

```rust
pub struct ShardHandle {
    pub id: ShardId,
    pub req_tx: mpsc::Sender<ShardRequest>,   // ShardManager → Shard
    pub resp_rx: mpsc::Receiver<ShardResponse>,// Shard → ShardManager (2PC reply)
    pub join: JoinHandle<()>,                  // shard thread
}

pub enum ShardRequest {
    Put { db: DbId, table: String, key: Vec<u8>, val: Vec<u8>, reply: oneshot::Sender<Result<()>> },
    Get { db: DbId, table: String, key: Vec<u8>, reply: oneshot::Sender<Result<Option<Vec<u8>>>> },
    Delete { db: DbId, table: String, key: Vec<u8>, reply: oneshot::Sender<Result<bool>> },
    PrepareCreateDb { name: String, reply: oneshot::Sender<Result<()>> },
    CommitCreateDb { name: String, db_id: DbId },
    AbortCreateDb { name: String },
    // ... 其他 2PC 消息
}
```

### 3.4 TwoPhaseCoordinator

```rust
pub struct TwoPhaseCoordinator {
    /// 当前 pending txn 状态
    pending: HashMap<TxnId, PendingTxn>,
}

struct PendingTxn {
    id: TxnId,
    op: TxnOp,                  // CreateDb / DropDb / CreateTable / DropTable
    phase: TxnPhase,            // Prepare / Commit / Abort
    prepare_acks: HashSet<ShardId>,  // 已 ack 的 shard
    started_at: Instant,
}

enum TxnPhase {
    Prepare,   // 已发 Prepare, 等所有 shard ack
    Commit,    // 所有 ack 收到, 已发 Commit
    Abort,     // 任一失败, 已发 Abort
}
```

**2PC 状态机：**
```
coord.prepare(op):
    txn_id = new
    pending[txn_id] = { op, phase=Prepare, prepare_acks={} }
    for shard in shards:
        send ShardRequest::Prepare{ op, txn_id }
    wait all ack (with timeout)

on all ack:
    pending[txn_id].phase = Commit
    for shard in shards:
        send ShardRequest::Commit{ op, txn_id }
    wait all commit ack
    pending.remove(txn_id)

on any shard prepare fail or timeout:
    pending[txn_id].phase = Abort
    for shard in shards:
        send ShardRequest::Abort{ op, txn_id }
    wait all abort ack
    pending.remove(txn_id)
```

---

## 4. 双 IO Backend (T12 实施)

```rust
/// IO backend 抽象. 同 shard 同一 backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoBackend {
    StdFs,    // std::fs 同步 IO (测试 / 调试用)
    IoUring,  // scheduler::io_ops 异步 IO (生产用)
}
```

**所有 IO 调用点加 match：**

```rust
// chunk_writer.rs:
match self.io {
    IoBackend::StdFs => {
        // 现有 std::fs 同步路径
        f.write_all_at(...)?;
        f.sync_all()?;
    }
    IoBackend::IoUring => {
        // scheduler::io_ops 异步路径
        scheduler::io_ops::write(fd, ...).await?;
        scheduler::io_ops::fsync(fd).await?;
    }
}
```

**Async 改造：** StorageEngine 全部 API 从 `fn xxx(&mut self) -> io::Result<...>` 改为 `async fn xxx(&mut self) -> io::Result<...>`. 调用方在 `scheduler::run()` 里 `spawn` future.

---

### 3.5 ⭐ MetaCache v2 — LFU + per-db page.mate (T12.4 重点, 取代原 sliding window)

**问题分析：**
- **当前 MetaCache 设计**: 单 page.mate 文件 + 10MB 数组 + 10 个 1MB sliding window
- **单 db 假设**: window 是"按 vpid 范围划分", 反映 vpid 空间局部性
- **多 db 加入后**: vpid 范围跨 db 拼接不连续, sliding window 失效
- **page.mate 单文件**: 无法多 db 物理隔离 (一个 db 损坏影响所有)

**新设计 (用户 2026-07-20 提出):**

```
MetaCache (per-shard 单例, 跨所有 db 共享):
  ┌────────────────────────────────────────────────────────────────┐
  │ entries: HashMap<MetaKey, CachedPid>           ← 跨 db 共享    │
  │ freq_heap: BinaryHeap<FreqEntry>                ← LFU 排序     │
  │ index_by_key: HashMap<MetaKey, HeapIdx>         ← O(1) 位置查询│
  │ per_db_mates: HashMap<DbId, File>               ← per-db fd    │
  │ decay_tick: Instant                             ← 定期衰减     │
  │ caps: { soft: 1M entries, hard: 1.5M }          ← 动态伸缩     │
  └────────────────────────────────────────────────────────────────┘

CachedPid {
    pid: PidLocation,
    freq: AtomicU32,        // 访问计数
    dirty: bool,            // 写未 flush
    last_decay: Instant,
}

FreqEntry { key: MetaKey, freq: u32, seq: u64 }  // BinaryHeap wrapper
```

**LFU 替换算法 (O(log n)):**
```rust
fn evict_if_needed(&mut self) {
    if self.entries.len() < self.caps.hard { return; }
    loop {
        // 1. 找 freq 最低 entry (BinaryHeap pop)
        let victim = self.freq_heap.pop().expect("non-empty");
        self.index_by_key.remove(&victim.key);
        let entry = self.entries.remove(&victim.key).unwrap();

        // 2. 若 dirty, 先 flush 到对应 db 的 page.mate
        if entry.dirty {
            self.flush_entry_to_mate(&victim.key, &entry.pid);
        }

        // 3. 降到 soft cap 以下退出
        if self.entries.len() <= self.caps.soft { break; }
    }
}
```

**Freq 衰减 (防陈旧热点):**
```rust
fn decay_tick_if_due(&mut self) {
    if self.last_decay.elapsed() < DECAY_INTERVAL { return; }
    for (_, entry) in self.entries.iter_mut() {
        // 每 N 秒所有 freq 除 2
        entry.freq = entry.freq.saturating_div(2);
        // 重入 heap (周期性 rebuild 或 lazy update)
    }
    self.last_decay = Instant::now();
}
```

**为什么 LFU 优于 sliding window:**
| 维度 | sliding window | LFU |
|---|---|---|
| 空间局部性 | ✅ 按 vpid 范围 | ❌ 频次 |
| 时间局部性 | ❌ | ✅ LFU 反映 |
| 跨 db 友好 | ❌ window 范围被打乱 | ✅ 不关心范围 |
| 动态伸缩 | ❌ 固定 10MB | ✅ 软/硬 cap |
| 淘汰公平性 | 范围最近邻 | 全局频次 |

**page.mate 持久化格式 (per-db 独立文件):**
```
dir/{db_name}/shard_{N}/page.mate  ← 单 db 单 file
  byte[vpid*8 .. (vpid+1)*8] = PidLocation  ← 不变
```
- **每 db 独立 page.mate**: 物理隔离, db 关闭直接 drop fd
- **格式不变**: 仍是 raw 8B slot array, recover / write 路径不变
- **无 dirty window tracking**: LFU dirty 单 entry flush, 不整 window fsync

**Recover 流程 (多 db per shard):**
```rust
fn recover(shard_dir) -> RecoveredState {
    for each page.mate in shard_dir/*page.mate:
        parse (db_name from path? no, db_name from outer path) → DbId
        pread 整个 10MB → 填 MetaCache.entries (freq=1, dirty=false)
    同时 scan all .block → on_page_found (db, vpid) pid:
        cache.write(db, vpid, pid) → 覆盖 mate 中 stale entry
}
```

**测试覆盖 (T12.5):**
- `LFU insert 触发淘汰`: 填 1.5M entries, 触发 hard cap, 最低频被淘汰
- `freq++ 更新 O(log n)`: 访问后 freq++, heap 调整, 淘汰顺序改变
- `decay 抗陈旧`: 之前高频 entry, freq /= 2, 让新热点能挤入
- `dirty flush 走对应 db mate`: 写后 dirty, 淘汰时先 flush 到正确 db mate
- `per-db mate 物理隔离`: db 1 mate 损坏不影响 db 2 读
- `multi-db cache 共享 1M cap`: 100 dbs 各 10K entries 都装得下 (LFU 公平)

**实施文件改造:**
- `meta_cache.rs` 全面重写 (~500 LOC)
- `recover.rs` 改: 每个 db 独立 recover (扫自己 page.mate + scan 自己 .block)
- `Pager::new` 改: 单 MetaCache 实例, 持有 per-db mate fd HashMap
- 现有 17 个 meta_cache_tests 大多失效, 重写 (保留 cover `mate_file_roundtrip`, 替换 sliding window 概念)

**完成标准:**
- MetaCache v2 lib tests pass
- 原 17 个 meta_cache_tests 迁移覆盖等价功能
- 集成测试 (catalog_consistency 等) 通过

---

## 5. 2PC + Pending_Txn Log (T14 实施)

**Post-commit 崩溃恢复：**
- 每个 shard 内部持久化 pending_txn log (`shard_dir/pending_txn.log`)
- Log 格式：append-only, 每条记录 `{txn_id, op, phase, timestamp}`
- 重启时 shard 扫描 log, 处理未完结 txn:
  - 只有 Prepare → 自动 Abort (走 reverse op)
  - Prepare + Commit → 自动 Commit (续作)
  - Prepare + Abort → 自动 Abort (续作)

**Reverse Op 设计 (推荐)：**

| Forward Op | Reverse Op | 说明 |
|---|---|---|
| CreateDb | DropDb | 删 db 目录 + 内存清 DbRegistry |
| DropDb | CreateDb | 不可能, drop 不可逆 (留人工恢复) |
| CreateTable | DropTable | 删 TableDirectory BTree |
| DropTable | CreateTable | 重新分配 vpid + 写 TableDirectory (会丢 table 内容) |

**协调器在 commit 完成后才删 pending_txn log, 保证 commit 之前任何 crash 都能 recover.**

---

## 6. 文件改动清单

### 6.1 新建 crates/shard_manager/

| 文件 | 作用 |
|---|---|
| `Cargo.toml` | crate 声明, 依赖 storage / scheduler / page |
| `src/lib.rs` | 公共 API re-export |
| `src/config.rs` | ShardConfig + IoBackend 枚举 |
| `src/db_id.rs` | DbId type + DbNameResolver |
| `src/shard_manager.rs` | ShardManager 主结构 + open/close |
| `src/shard_handle.rs` | ShardHandle + ShardRequest/Response (T13) |
| `src/two_pc.rs` | TwoPhaseCoordinator + TxnId + TxnOp |
| `src/pending_log.rs` | pending_txn log 持久化 + recover |
| `src/router.rs` | hash(key) % num_shards 路由 + DbId 解析 |

### 6.2 改造 crates/storage/

| 文件 | 改造点 |
|---|---|
| `src/types.rs` | 新增 `DbId = u32`, 复合 key 结构体 |
| `src/meta_cache.rs` | 所有内部 `HashMap<u64, PidLocation>` 改 `HashMap<(DbId, u64), PidLocation>` |
| `src/alloc.rs` | `VpidAllocator` / `PidAllocator` 改 `HashMap<DbId, State>` |
| `src/chunk_lru.rs` | `ChunkKey` 加 `db: DbId`, LRU 跨 db 共享 |
| `src/chunk_writer.rs` | `NowChunks` / `WriteQueue` / `ChunkWriter` key 加 db, `IoBackend` match |
| `src/pager.rs` | 所有 page 操作加 `db: DbId` 参数, `IoBackend` match |
| `src/recover.rs` | 扫描 `block_root/*/shard_N/*.block`, 每 (db, shard) 独立 recover |
| `src/meta_page.rs` | 头部加 `DbNameResolver` 序列化段 |
| `src/registry.rs` | `DbHandle` 用 `DbId` 索引 |
| `src/table_directory.rs` | key 加 db 字段 |
| `src/engine.rs` | `OpenOptions` 加 `block_root` + `shard_id`, 构造路径 `block_root/{name}/shard_{N}/` |
| `tests/...` | 全部测试加 `db: DbId` 参数 |

---

## 7. 实施 Checklist (TDD 驱动, 单线程串行测试)

### T12: storage crate 接 IO Backend (单 db → 多 db, 加 DbId 字段)

**执行原则**：T12 拆为 12 个原子步骤, 每步独立 commit + clippy/fmt + 全量测试通过, 不要一口气推. 完成所有步后, 282 个测试应等价通过 (新增 IoBackend 测试).

#### **T12 阶段 1: 类型基础 (新增 + 不破坏现有)**
- [ ] **T12.1**: `types.rs` 新增 `pub type DbId = u32;` + 复合 key 结构体 `MetaKey { db: DbId, vpid: u64 }` (with `#[repr(C, align(16))]` for HashMap UB 修复). 加 types_tests (12 行测试)
- [ ] **T12.2**: types.rs 加 `IoBackend` enum (`StdFs / IoUring`). 加 4 行单元测试 (derive Debug, Clone, Copy, PartialEq, Eq)
- [ ] **T12.3**: 跑通现有 282 测试 (零改动)

#### **T12 阶段 2: 类型迁移 (key + (DbId, ...), 测试迁移)**
- [x] **T12.1**: types.rs 新增 `pub type DbId = u32;` + 复合 key 结构体 `MetaKey { db: DbId, vpid: u64 }` (with `#[repr(C, align(16))]` for HashMap UB 修复). 加 types_tests (12 行测试) — **✅ 2026-07-20 DONE**
- [x] **T12.2**: types.rs 加 `IoBackend` enum (`StdFs / IoUring`). 加 4 行单元测试 (derive Debug, Clone, Copy, PartialEq, Eq) — **✅ 2026-07-20 DONE**
- [x] **T12.3**: 跑通现有 282 测试 (零改动) — **✅ 2026-07-20 DONE (实际 288 通过)**
- [ ] **T12.4**: ❗ **MetaCache 重构为 LFU + per-db page.mate** (取代原 sliding window 设计, 见 plan §3.5)
- [ ] **T12.5**: MetaCache v2 lib 测试 (LFU 替换 / freq 衰减 / dirty flush / per-db mate)
- [ ] **T12.6**: PageKey 加 `db: DbId` (现有 `#[repr(C, align(16))]` 16 字节对齐保留). 跑通现有 17 个 meta_cache_tests + LFU 迁移

#### **T12 阶段 3: 组件改造 (Allocator + ChunkList + ChunkWriter)**
- [ ] **T12.7**: `VpidAllocator` 从单实例 → `HashMap<DbId, VpidState>` (next_vpid + free_list per db). 公共 API `alloc(db) -> Vpid`. 全部 caller 迁移 (Pager::create, recover). 跑通 282 测试
- [ ] **T12.8**: `PidAllocator` 同 T12.7 改造 (per-db pid 状态). 跑通 282 测试
- [ ] **T12.9**: `ChunkKey` / ChunkList: `db: DbId` 字段加. `insert(key, chunk)` / `get(key)` API + caller. LRU 跨 db 共享不变. 跑通 282 测试
- [ ] **T12.10**: `NowChunks` / `WriteQueue` / `ChunkWriter`: key 加 db. **同时 chunk_writer.rs 加 `IoBackend` match** (现在还是默认 StdFs, 但加 enum 字段). 跑通 282 测试
- [ ] **T12.11**: `FreePageQueue` (同 T12.7 per db). 跑通 282 测试

#### **T12 阶段 4: 改 recover + MetaPage**
- [ ] **T12.12**: `Pager::new` + `recover` 签名加 `block_root: PathBuf` + `shard_id: u32`, 路径改为 `block_root/default/shard_{shard_id}/` (单 db 兼容). 跑通 282 测试
- [ ] **T12.13**: `recover` 扫描 `block_root/*/shard_N/*.block` 重建, 发现 db 用 db name → DbId (默认 0). 跑通 282 测试
- [ ] **T12.14**: `MetaPage` 头部加 `DbNameResolver` 序列化段 (默认 1 个 db 名 "default" → DbId 0). 跑通 282 测试

#### **T12 阶段 5: OpenOptions + Multi-DB API**
- [ ] **T12.15**: `OpenOptions` 加 `block_root: PathBuf` + `shard_id: u32`. 跑通 282 测试
- [ ] **T12.16**: `StorageEngine` 加 `current_db: DbId` 字段 (单 db 模式用 0 默认). 所有 public API 加 `db: DbId` 参数 (向后兼容 default 0). 跑通 282 测试
- [ ] **T12.17**: `DbRegistry` 改造支持多 db 真实物理路径 (`block_root/{name}/shard_N/`). 跑通 282 测试 + 12 个 catalog_consistency_tests

#### **T12 阶段 6: 多 db 物理隔离 e2e**
- [ ] **T12.18**: 新增测试 `tests/multi_db_physical_isolation.rs` (5-8 测试):
  - 两个 db 路径完全独立
  - drop db 真实删 `block_root/{name}/`
  - 备份 db 目录 → reopen → 数据完整
  - 单 db 损坏不影响另一 db
- [ ] **T12.19**: 跑通所有 282 + 多 db 物理隔离测试
- [ ] **T12.20**: clippy/fmt 收尾 (T12 全部完成标准: 0 警告 + 0 fmt diff)

**T12 阶段 7: catalog_consistency_tests 重写**
- [ ] **T12.21**: 现有 `catalog_consistency_tests.rs` 12 个测试改用新多 db API (block_root + shard_id, 每个 db 独立路径). 跑通

**T12 完成标准**：
- 所有 282 + 新增测试通过
- clippy 0 警告
- fmt 无差异
- 0 个回归 (功能等价, 仅内部改造)

### T13: shard_manager crate 主体
- [ ] T13.1: workspace 加 shard_manager crate
- [ ] T13.2: ShardConfig + IoBackend 枚举
- [ ] T13.3: DbId + DbNameResolver
- [ ] T13.4: ShardManager 主结构 + open/close
- [ ] T13.5: Router (hash key + 解析 db)
- [ ] T13.6: 单 shard 串行集成测试 (open + put + get + close + reopen + get)

### T14: 2PC 跨 Shard 协调
- [ ] T14.1: TxnId + TxnOp 枚举
- [ ] T14.2: TwoPhaseCoordinator 状态机
- [ ] T14.3: ShardManager::create_db / drop_db 走 2PC
- [ ] T14.4: ShardManager::create_table / drop_table 走 2PC
- [ ] T14.5: 2PC 失败回滚测试 (mock shard 失败)

### T15: Pending_Txn Log + Recover
- [ ] T15.1: pending_txn log 文件格式
- [ ] T15.2: append / mark_complete / recover 接口
- [ ] T15.3: 集成到 ShardHandle (commit 前写 log, commit 后删)
- [ ] T15.4: 模拟 commit 阶段 crash + recover 测试
- [ ] T15.5: 模拟 prepare 阶段 crash + auto-abort 测试

### T16: Scheduler 集成 (可选, T12 同步 IO 已可工作)
- [ ] T16.1: 每个 shard 一个独立 Scheduler thread
- [ ] T16.2: ShardRequest 通过 mpsc 派发
- [ ] T16.3: StorageEngine API 改 async fn
- [ ] T16.4: chunk_writer.rs 中 IoBackend::IoUring 路径走 `scheduler::io_ops`
- [ ] T16.5: 单 shard async e2e 测试
- [ ] T16.6: 多 shard 并发 e2e 测试 (--test-threads=1 串行, 模拟多 shard 线程切换)

---

## 8. 测试计划

### 8.1 Storage crate 改造测试 (T12)
- 所有 282 个现有测试**完全迁移**, 不变功能, 验证改造无回归
- 新增测试: `IoBackend::StdFs` 与 `IoBackend::IoUring` (mock) 路径等价
- 新增测试: 多 db 在单 shard 下不互串 (chunk 共享, 但 db 路径独立)

### 8.2 ShardManager 单元测试 (T13)
- `DbNameResolver::get_or_create` 重复名返回相同 id
- `Router::route(key)` 同一 key 总是同 shard
- `ShardManager::open` 创建 N 个独立 engine
- `ShardManager::put / get` 单 shard 路由
- `ShardManager::close + reopen` 持久化 + 重建

### 8.3 2PC 跨 Shard 测试 (T14)
- `create_db` 在 N shard 同步生效 (所有 shard 都能 get)
- `create_table` 在 N shard 同步
- mock shard 失败 → coord 发 Abort → reverse op 清理
- 多 db + 多 table 混合, create/drop/put 跨 shard 协调

### 8.4 Pending_Txn 恢复测试 (T15)
- 模拟 commit 中 crash: kill 在 coord 发 commit 之后, restart → 自动 commit
- 模拟 prepare 中 crash: restart → 自动 abort
- 模拟 commit 后 crash 但 commit 消息未到某 shard: restart → 该 shard 自动 commit

### 8.5 端到端 e2e (T16)
- 单 shard async e2e (open → put 1000 keys → get → delete → flush → reopen → verify)
- 多 shard 并发 e2e (N=4 shards 并行 put 1000 keys each, 全 get 验证)
- 跨 shard 协调: 2PC 失败的端到端 (kill mid-coord)

---

## 9. 内存预算 (N=8 shards, 100 dbs)

| 组件 | 旧设计 (per db, shard 独立) | 新设计 (per shard 单例) |
|---|---|---|
| ChunkList | 100 × 8 × 8MB = 6.4GB | 8 × 8MB = 64MB |
| NowChunks | 100 × 8 × 4MB = 3.2GB | 8 × 4MB = 32MB |
| MetaCache | 100 × 8 × 10MB = 8GB | 8 × 10MB = 80MB |
| Allocator | 100 × 8 × 数十字节 = 数十 KB | 8 × 数十字节 = 数百字节 |
| DbRegistry | 100 × 8 × 几十 KB = 几十 MB | 8 × 100 × 几十字节 = 几十 KB |
| **总计** | **~17.6GB** | **~176MB** |

**节省 100 倍.**

---

## 10. 实施顺序

**优先级 T12 → T13 → T14 → T15 → T16**

- **T12** 是基础, 改造 storage crate 加 db 字段, 跑通现有 282 个测试. 改动大但机械.
- **T13** 是 shard_manager 主体, 单 shard 串行测试, 跑通 1 个 shard 端到端.
- **T14** 是 2PC 协议, 加多 shard 协调, 跑通 create_db / create_table 跨 shard.
- **T15** 是崩溃恢复, 加 pending_txn log.
- **T16** 是 scheduler 集成 (可选, std fs 已可工作), 改造 async + io_uring.

**每个 T 完成后跑全量测试 + clippy + fmt 收尾, 再开始下一个 T.**

---

## 11. 与 DESIGN §3.1 的关系

DESIGN §3.1 定义 "Per-Shard Thread + Per-Shard io_uring". 本 plan 是其具体实现:
- Per-Shard Thread: T16 实施 (每个 shard 一个 std::thread)
- Per-Shard io_uring: T16 实施 (每个 shard 一个独立 io_uring)
- Per-Shard StorageEngine: T13 实施 (每个 shard 一个 StorageEngine 实例)
- Per-Shard 故障隔离: T13 设计 (单 shard 挂 → ShardStatus::Dead, 其他正常)

**DESIGN §3.1 的核心契约在 T13-T16 全部满足.**

---

> **下一步：**
> 1. 确认本 plan 全部决策
> 2. 开始 T12: 改造 storage crate 加 db 字段 (机械工作, 5-7 个 TDD 子任务)
> 3. T12 完成后所有 282 个现有测试等价通过, 继续 T13
