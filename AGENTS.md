# NexusDB — Agent Handoff Notes

> 给接手这个项目的 agent / 协作者. 读完这份文件你就知道现在到哪、下一步怎么走.
>
> **配套文档**:
> - [`CHANGELOG.md`](./CHANGELOG.md) — 修复历史 (F1-F41) + 测试进度快照 + gotchas + 测试文件清单
> - [`DESIGN.md`](./DESIGN.md) — 项目总设计 (10 节)
> - [`docs/superpowers/plans/`](./docs/superpowers/plans/) — 各 crate 实施 plan
> - [`docs/bug-report-btree-split-routing.md`](./docs/bug-report-btree-split-routing.md) — stress 丢 key 根因调查报告

## 项目是什么

NexusDB: 面向写密集/低延迟/高并发的**独立单机数据库服务** (2026-07-25 从嵌入式引擎定位演进), Rust 2024.
- 设计哲学: Share-Nothing + Per-Core Thread + io_uring + 自实现协程调度器
- 长期目标: 多协议统一接入 (Redis ✅ / PostgreSQL / MySQL / Mongo 待实施) + 数据互联 (统一记录编码, value type tag 已预留)
- 子 crate:
  - `crates/scheduler` — 单线程协程调度器 + io_uring 桥 (✅ 完成)
  - `crates/page` — LCB-Tree 页操作: 叶子/非叶子节点 + checkpoint + 前缀压缩 (✅ 完成)
  - `crates/storage` — 物理持久化层: vpid→pid 映射 / chunk LRU / nowchunks / 崩溃恢复 / 多 db 多表 catalog / **自动持久化 + 异步 chunk 落盘 (F41)** (✅)
  - `crates/network` — 网络层: acceptor + epoll worker + **双协议门面 (Binary + RESP2/Redis 兼容含 AUTH)** + KvLimits 校验 + value type tag (✅)
  - `crates/shard_manager` — 多 shard 控制器 + hash 路由 + 2PC + **TaskInbox/TaskReplyBus 直连架构** (✅)
  - `crates/config` (TOML) / `crates/logging` (nlog, io_uring 协程融合 logger) (✅)
  - 根 binary `src/main.rs` — 服务器入口: `nexusdb --config nexusdb.toml`, Binary(5433) + RESP(6379) 双监听, 信号优雅退出

## 当前进度

### 2026-07-27 会话总览 (F42-F44, 细节见 CHANGELOG)

- **⭐ F42 GC 静默数据丢失修复 (最重要)**: compact 判活曾用 page header vpid 自描述, 但 Internal 页该字段是 first_child → 误判死页 → 高压写后早期 key 静默丢失 (GET nil 不报错). 修复: 判活以 meta 平坦数组全扫为 SoT. **gotcha: compact/GC 判活禁止依赖页头自描述**. 排查探针 `NLOG_GC_DEBUG=1` 保留
- **F43 热路径 9 项优化** (同机 A/B +19%): page_pool 归还闭环 + travel_to_leaf_ro (免 path 分配) + table_put 单 travel + BatchOp Arc<str> + Put.value 预置 tag 免二次拷贝 + 解析游标化. **约定: `Request::Put.value` / `BatchOp::Put.val` 统一 `[TAG_RAW][payload]` 布局 (decode 时预置)**
- **F44 String 命令集**: MGET/MSET (跨 shard 分组聚合 + `LeafGuide` 区间复用批量, `ShardTask.group` 字段) + INCR/DECR/INCRBY/DECRBY/APPEND/SETNX (shard 端原子 RMW) + EXISTS/STRLEN/TYPE; 大 value 溢出页 (~1MB, 13B 描述符, PID_FREED 墓碑防复活)
- **区间 travel 基建**: `internal_child_with_bounds` → `travel_to_leaf_guided` → `LeafGuide [lower, upper)` — range scan / cursor 的直接前置
- 测试快照: **75 suites / 708 passed / 0 failed**, clippy 0

### 2026-07-25/26 会话总览 (F33-F41, 细节见 CHANGELOG)

- **三个关键正确性修复**:
  - F33 btree_insert split 条件路由 (stress 丢 key 根因)
  - F39 pollster 死锁 → `block_on_io` (IoUring 下 shard 永久 futex 睡死)
  - F40 leaf_update 段首 shared=0 损坏 (memtier 长前缀 key 覆盖写必现)
- **独立服务架构** (F36): worker(epoll) → TaskInbox → shard → TaskReplyBus → worker, 零 client 线程; 旧同步 API 保留给测试
- **自动持久化 + 异步落盘** (F35/F41): chunk 满 swap → FlushJob 协程 io_uring 写盘 (与内存写并发); MAX_INFLIGHT=8 超限退化同步 (背压); 周期 10s / 256 写触发; meta 仅在 backlog 排空后刷
- **多协议门面** (F38): RESP2 全链路 (redis-cli/memtier 验证), AUTH / pipeline FIFO 重排 / KvLimits / type tag
- **成品化** (F37): config + nlog + main 服务器化
- **性能快照** (memtier 2t×10c pipe16, io_uring, 真实持久化): 读混合 1:10 = **1.06M ops/s**; 写重 1:1 = **153K ops/s** (p99 16.7ms); 同机 Redis AOF everysec 对照 1.83M / 1.51M
- **下一步 (按 ROI)**: 读路径 PageIndex 缓存 + 零拷贝 → WAL (消 16KB 页写放大) → shard 自包含网络 (消 worker↔shard 两跳 handoff, 读向 1M+)

### ShardManager crate (✅ T13 + T14 完成, T15 async API 待实施)

**T14 (2026-07-22): 2PC 跨 shard 协调 + 同步 API** ✅
- `TwoPhaseCoordinator` 状态机 (`coordinator.rs` ~330 LOC)
- 6 个 2PC 消息: Prepare/Commit/Abort × {Db, Table}
- `ShardManager::create_db/create_table` 走 2PC
- 8 个 2PC e2e 测试, 15 个 lib 单元测试
- **测试 0 failed, clippy 0 警告**
- 同步 API 性能影响已识别: 主线程串行化 (T15 解决)

**T13 (2026-07-22): 基础架构** ✅
- 多 shard 控制器, hash 路由
- per-shard 线程 + Scheduler + StorageEngine
- `Rc<RefCell<Option<StorageEngine>>>` 共享 engine
- 同步 API: put/get/delete

**T15 (待实施: async API + pipeline)**: 网络层已搭建 (NetworkServer), 但 ShardManager 内部仍是同步 API.
- 当前 network crate 的 worker 用同步 `ShardManager::put/get/delete` (阻塞)
- 未来: ShardManager 加 `put_async` / `get_async` / `delete_async` 返回 Future
- 配合 ReplyBus 实现异步 waker 通知, 解决主线程串行化

### 当前能力盘点

**已支持** (T1-T17 + F32-F41):
- **服务化**: `nexusdb --config nexusdb.toml` 启动, Binary(5433) + RESP/Redis(6379) 双协议监听, redis-cli/memtier 可直接使用, SIGINT/SIGTERM 优雅退出 (退出前排空异步落盘 + final flush)
- **元数据**: open/close/flush; create_db/drop_db/open_db/list_dbs/use_db; create_table/drop_table/open_table/list_tables (2PC 跨 shard)
- **KV 数据**: table_put / table_get / table_delete (含覆盖写 leaf_update)
- **持久化**: 多 db 物理隔离 (`{block_root}/{db_name}/shard_{N}/`); reopen recover; **自动持久化** (chunk 满 swap + 周期 10s/256 写); **异步 chunk 落盘** (FlushJob 协程 + 有界背压 MAX_INFLIGHT=8); data→meta 刷盘顺序不变量
- **异步**: 全 async; 自实现协程调度器 + io_uring 后端 (服务器默认 io_uring)
- **多 shard**: hash 路由 + TaskInbox/TaskReplyBus 直连 (worker→shard→worker, 零 client 线程)
- **协议层**: RESP2 (SET/GET/DEL/PING/ECHO/AUTH/QUIT/HELLO/SELECT/COMMAND, pipeline FIFO 重排) + 自家二进制协议; KvLimits (key≤1024/value≤3000, page 编码 4096 硬限); value type tag 预留
- **测试**: workspace 71 suites / 682 passed / 0 failed; clippy 0 警告

**还没支持** (下一步 gap):
- **Range scan / cursor** (`table_range` + iterator) — SQL `WHERE range` 和 `SELECT *` 需要
- **Transaction** (begin/commit/rollback) — 跨多 page ACID
- **Snapshot** — 事务内一致性读 (COW + meta_cache 天然支持, 实现成本低; **不需要 MVCC** 见 §3.3.2 设计决策)
- **WAL** — 消每写 16KB 页 COW 写放大 (写重负载与 Redis 差距的主因)
- **大 key/value (overflow page)** — 当前协议层上限拦截, 正式方案待实施
- **PG/MySQL/Mongo 门面** — 前置: range scan + 统一记录编码 (保序 key 编码 + 表级 schema)
- **shard 自包含网络** (ScyllaDB 模式) — 消 worker↔shard 两跳 handoff 的终局方案

**⭐ 不需要 MVCC 的设计决策** (见 `docs/superpowers/plans/2026-07-18-storage-crate.md` §3.3.2):
- meta_cache 跟随 COW, 写 vpid 只改映射不改数据
- 单线程 runtime + `&mut Pager` 强制串行, 无真并发
- COW 已天然保留历史 page, 未来 Snapshot API 只需 clone meta_cache 视图
- 优势: 零额外存储 (无 version chain), 零 GC (无 version 清理), 零冲突 (Pager 仍串行)

### Storage crate T17 (全 async 重构 + io_uring 集成, 2026-07-21) ✅ **完成**

**T17 范围:**
- T16: PagerIo 抽象层 (StdFs / IoUring Backend 枚举, 通过 `OpenOptions.io_backend` 选)
- T17: Pager / StorageEngine / Registry / TableDirectory / BTree 全部改 async
- 异步测试运行器 (`tests/common/mod.rs::run_async`)
- 栈大小修复 (RUST_MIN_STACK=64MB 启动, 因 storage async fn 内联后 poll frame 含多个 16KB page buffer)
- 386 tests passed (含 19 个新 io_backend / async 测试), 0 failed

### Network crate (✅ Phase 1-4 完成, 2026-07-24)

**Phase 1-4 范围:**
- Protocol trait + BinaryProtocol 实现 (二进制帧 codec)
- Acceptor (非阻塞 accept loop, RoundRobin/Random/Sticky LB)
- WorkerPool (N worker thread, 每个 conn 独立 OS thread)
- NetworkServer 顶层组装 (acceptor + worker pool + 优雅关闭)
- ReplyBus (crossbeam unbounded channel, 实现 ReplySink trait)
- 压力测试工具 (network_stress: 4 阶段, 多 client 多 shard 压测)
- Pager read 路径加固: 四源查找 (nowchunks → WriteQueue → chunk_list → disk)

**missing key 排查 (仍在进行):**
- 高并发下 ~0.2% key 丢失, 已在 storage 层独立复现
- 单线程永不触发, 仅在多 client 并发时出现
- 已实施的修复: Pager read 路径加 WriteQueue 检索
- 待深入: BTree insert 并发 get 的 stale leaf page 问题

**当前测试状态:** Page 131 + Storage **386 passed, 0 failed**, clippy 0 警告.
Workspace: ~549 passed, 0 failed (不含慢 repro 测试).

### Storage crate T12 (ShardManager 集成, 2026-07-20) ✅ **全部 21 子任务完成**

**已完成 (21/21 子任务):**
- T12.1-T12.3: types.rs DbId + MetaKey + IoBackend 基础 ✅
- T12.4-T12.5: MetaCache v2 (LFU + per-db mate), 17→18 测试迁移 ✅
- T12.6: MetaCache 加 DbId 维度 (+13 测试, +evict bug 修复) ✅
- T12.7-T12.8: VpidAllocator + PidAllocator + FreePageQueue per-db (+10 测试) ✅
- T12.9: ChunkList ChunkKey 加 DbId (+5 测试) ✅
- T12.10: ChunkWriter per-(db, file_id) paths (+3 测试) ✅
- T12.12: Pager::new + recover 路径加 block_root + shard_id (+16 测试) ✅
- T12.13: recover 扫描 `{block_root}/{db_name}/shard_N/*.block` ✅
- T12.14: MetaPage 集成 DbNameResolver (+Resolver 段 + COW 修复) ✅
- T12.15: OpenOptions 加 block_root + shard_id ✅ (在 T12.12 提前完成)
- T12.16: StorageEngine 加 current_db 多 db 上下文 (+5 测试) ✅
- T12.17: OpenOptions 加 db_name 参数 + DbRegistry 真实多 db 物理路径 ✅
- T12.18-21: 多 db 物理隔离 e2e (9 测试) + catalog_consistency 重写 + clippy/fmt 收尾 ✅

详细修复历史 (F1-F29) 见 [`CHANGELOG.md`](./CHANGELOG.md).

### Storage crate T1-T11 (✅ 完成)

| # | 任务 | 状态 |
|---|---|---|
| T1 | Workspace + storage scaffold + types.rs | ✅ DONE |
| T2 | MetaCache: 两层数组 (10MB + 10×1MB Index) + LRU-最近邻 | ✅ DONE |
| T3 | VpidAllocator + PidAllocator + FreePageQueue | ✅ DONE |
| T4 | 三层架构: NowChunks + WriteQueue + ChunkWriter | ✅ DONE |
| T5 | ChunkList: 1MB chunk 读 LRU 缓存 (只读不可修改) | ✅ DONE |
| T6 | Pager: read + create + PageWriteBatch + chunk_lock + TravelTree | ✅ DONE |
| T7 | recover: 扫描 block_dir + MetaCache union 语义 | ✅ DONE |
| T8 | StorageEngine facade: open/put/get/flush/close | ✅ DONE |
| T9 | MetaPage: db_name → table_dir_root_vpid BTree | ✅ DONE |
| T10 | TableDirectory: table_name → table_root_vpid BTree (移除 *mut Pager 修复 aliasing UB) | ✅ DONE |
| T11 | DbRegistry: 多 db/多表 API + 镜像 cache | ✅ DONE |

### Scheduler crate (✅ 完成 T1-T10, T11 clippy/fmt polish 暂停)

11 任务 plan: `docs/superpowers/plans/2026-07-17-scheduler-crate.md`.

### ShardManager crate (✅ T13 + T14 完成, T15 async API 待实施)

**T13 (基础架构)**:
- 多 shard 控制器: N 个独立 shard 线程 + Scheduler + StorageEngine
- hash 路由: `(db_name, table_name, key)` 三元组 hash
- 同步 API: put/get/delete/create_db/create_table
- 共享 engine: `Rc<RefCell<Option<StorageEngine>>>`

**T14 (2PC 跨 shard 协调)**:
- `TwoPhaseCoordinator` 状态机: begin_txn → on_prepare_ack/fail → on_commit/abort_ack
- 6 个 2PC 消息: Prepare/Commit/Abort × {Db, Table}
- Abort 是 best-effort: reverse op = drop_db/drop_table
- Coordinator 用 `RefCell` 包装, 让 `&self` 方法能访问

**T15 (待实施: async API + pipeline)**:
- 解决 T14 同步 API 的主线程串行化问题
- 给网络层 (Tokio/Axum) 用

### Page crate (✅ 完成 Phase 1-7 + dump 工具)

7 phases: ItemPtr / PageIndex / push_back / pre_split·merge / leaf CRUD / internal CRUD / 清理旧代码 + dump.rs.

---

## 关键设计原则 (实施时记住)

### 调度器 / IO

- **Scheduler 多线程契约**:
  1. 每个 shard 线程自己 NEW 一个 Scheduler (独立 io_uring), 永久 run() loop
  2. spawn / drive / JoinHandle::poll 全在同一线程
  3. 跨 shard 通信用 mpsc channel (不用 JoinHandle 跨线程)
  4. 违反任一条 → JoinInner::UnsafeCell 跨线程 race → 永久 hang

- **协程 = Rust `async fn` + Future** (不是栈式协程)
- **Waker 全部自实现**, 不依赖 monoio 的 Reactor
- **不引入 tokio / crossbeam / monoio**: 全部走 `scheduler::io_ops::{read, write, fsync}`
- **Future 自取 CQE** via `peek_cqe_by_user_data`, 不走 SharedResult 中转

### Storage crate (T12 阶段, 实施时遵守)

- **三层地址空间**: vpid (u64, 永不重用-COW 友好) → pid (file_id + chunk_idx + page_idx + flags) → byte offset
- **PidLocation 必须 `#[repr(C, packed)]`** 8B (MetaCache 一项 8B 槽)
- **写顺序**: page data → .block → vpid log → .block fsync → dirty .mate window → page.mate fsync (data→meta, 不可调换)
- **vpid 永不重用**: 一旦分配不被回收, COW 由 meta_cache 完成
- **chunk 满 64 pages 触发 rotate**: PidAllocator 返回 None, ChunkWriter 切新 chunk/file
- **Page 二层访问**: `read_page` borrow 零拷贝 / `take_page_for_write` COW 复制
- **PageWriteBatch 必走**: leaf/internal/root split / merge / drop_table 必走 batch (MAX_BATCH_BYTES=256KB, 跨 batch 原子性 caller 自保)
- **chunk_lock owner**: 必须 batch::submit + meta_cache.write 都完成才释放 (持有期 = 隐式 pin chunk)
- **TravelTree RAII**: TravelTreeGuard drop 自动 unregister, 不允许手动
- **recover 第一版用 page header 自描述**: 不解析 vpid log 格式 (T11 polish 时再加)

### Page crate

- **哨兵总是 item 0**: shared=0, key_unshared_len=0
- **key_count 包含哨兵**: 真实 keys 数 = key_count - 1
- **每个 cp 段首 shared=0** (create_from_cp 时验证)
- **段大小 ≤ MAX_PER_CHECKPOINT (32)**: 超了就 split; **≥ MIN_PER_CHECKPOINT (8)**: 少了就 merge (哨兵段例外)
- **只有 k+1 需要重写 shared_prefix_len**: push_back 后紧邻 item 的 prev_key 变了
- **删除后也要重写 k+1**: `leaf_delete` / `internal_delete` 物理删除后, 原来 k+1 的 prev_key 从 target 变成 target-1, 必须用新 prev_key 重新编码
- **删完别越界**: 清理空段后 target_seg_idx 可能失效, 用 `effective_seg_idx = min(target_seg_idx, segments.len()-1)`

### Catalog (T9-T11, 已确认版)

- **MetaPage 硬编码 chunk 0 page 0**: 整个 catalog 树的根, 启动第一个读
- **MetaPage 用 BTreeMap 镜像 + 整页重写 flush**: db 数量少时整页重写性能可接受
- **TableDirectory 单 leaf page BTree**: 复用 page crate leaf, 每个 db < ~200 table, 超需 internal page (留 polish)
- **DbRegistry write-through cache**: HashMap 是 BTree 的镜像, cache 永不超前
- **多 db, 每 db 多表**: db_name + table_name 复合 key, 不同 db 完全隔离

### T12 ShardManager (新增)

- **三层物理隔离**: `block_root/{db_name}/shard_{N}/{*.block, page.mate}` (db 物理隔离 + shard 物理隔离 + block 文件隔离)
- **pid/vpid per-db 命名空间**: 不同 db 的 vpid 0 物理上不同 (独立 .block)
- **DbId(u32) 内部唯一标识**: 替代 String (4B Copy vs 24B + heap alloc)
- **DbNameResolver**: name ↔ id 双向映射, 持久化到 MetaPage
- **MetaCache v2 = LFU + per-db page.mate**: 抛弃 sliding window, freq tracking + 衰减 (抗陈旧热点) + soft/hard cap 动态伸缩
- **compat 策略**: 所有现有 caller 用 compat API (走 db=0) 保持 zero regression

### 三层并发控制 (T6 实施后正交)

1. **chunk_lock** — 字节层, 同 chunk 内串行读 page 字节
2. **travel_key_path + travel_tree** — tree 逻辑层, split 传播时更新栈路径
3. **fresh root_vpid** — 全局入口层, 每次新 travel 拿最新 root

### 2026-07-25/26 增量设计原则 (F33-F41)

**异步 I/O (核心修正)**:
- **❌ 不能在 shard 线程用 `pollster::block_on`** 跑 IoUring 后端的 async — IoUring 下 `io_ops::fsync` 首次 poll 提交 SQE 后 Pending, pollster park 线程; 而 CQE 收割在**下次 poll 的 CQ 扫描**里 — 线程睡死后无人再 poll → 永久死锁. 现象: PING 通、SET 卡死. 用 `block_on_io` (重 poll, Pending 后 spin/yield), poll 内部自带 CQ 收割
- **⭐ flush 不能在 shard 主循环内 `block_on_io` 串行 await** —— 磁盘 IO 应**所有权转移**给独立协程 (`spawn_on`, FlushJob 零 Pager 借用), 与内存写入完全并发; 主循环每轮 `drive_until_idle` 推进收割. 磁盘 IO 满时自然降速 (有界背压, MAX_INFLIGHT_CHUNKS 超限退化同步)
- **`flush()` 契约**: caller 必须先排空 in-flight (debug_assert), 否则同 key 并发写同 offset
- **完成顺序**: shard 端先 push reply_bus 再 `reply.send` (避免 client 醒来读到缺条目的 sink)

**协议层**:
- **value type tag**: 写入 `[tag u8][payload]`, 读时按 tag 解; 空值/未知 tag 容错按 RAW 返回 (兼容早期未打 tag 数据). 多协议数据互联统一编码
- **KvLimits 上限依据**: page 编码路径全用 `[0u8; 4096]` 栈缓冲, 单 item 硬上限; config 校验 `max_key + max_value <= 4060`. 超限在 worker parse 后进 shard 前拦截, 返协议 error
- **RESP FIFO 重排**: RESP 无 req_id, per-conn 递增 seq 作 req_id; 回复经 BTreeMap 严格按序; 本地命令 (PING/AUTH/超限 error) 也占 seq 保证 pipeline 顺序
- **同 key 去重 (异步落盘)**: in-flight 中的 key 跳过新一轮 take, 避免两个协程并发写同 offset 乱序
- **TCP_NODELAY**: server accept 后必设, 否则 pipeline 小回复被 Nagle + delayed-ACK 拖到 40ms (p50 0.26→0.66ms)

**通信层**:
- **drain 丢唤醒竞态修复**: 先 `store(0, Release)` 再 `pop` —— store 前的 push 必被本轮 pop 到; store 后的 push 看到 0 重新写 eventfd. (inbox + task_inbox + reply_bus 均修复)
- **EPOLLT 边缘触发易丢事件**: 改水平触发 (默认), 稳健优先
- **worker FIFO 重排 + BinTreeMap** 是 RESP 正确性的基石
- **accept → worker 通知用 eventfd 精确唤醒**, 避免 worker 1ms epoll 空轮询

**catalog 修复** (F33, stress 丢 key 根因):
- **btree_insert split 后必须按 key 路由**: `if key > split_key { right } else { left }`. 旧代码无条件插 right 假设触发 key 一定 > split_key, 对非顺序插入 (新 key 落在原页 max 之前) 错位
- MetaCache 零槽 phantom entry: pread_slot_from_mate 读到全零返回 None, 不缓存

---

## 关键文档路径

| 路径 | 内容 |
|---|---|
| `DESIGN.md` | 项目总设计 (10 节) — 必读 §3.4 (Per-Shard 调度器), §4.2.3 (Page/Item 设计), §4.3-§4.7 (Storage) |
| `CHANGELOG.md` | 修复历史 (F1-F41) + 测试进度快照 + gotchas + 测试清单 (接手后首选查阅) |
| `docs/bug-report-btree-split-routing.md` | stress 丢 key 根因调查报告 (F33) |
| `docs/superpowers/specs/2026-07-17-scheduler-crate-design.md` | scheduler crate 设计 |
| `docs/superpowers/plans/2026-07-17-scheduler-crate.md` | scheduler crate 11 任务实施 plan |
| `docs/superpowers/plans/2026-07-17-page-item-revision.md` | page crate 增量式 prefix-compress 方案 |
| `docs/superpowers/plans/2026-07-18-storage-crate.md` | storage crate T1-T11 实施 plan |
| `docs/superpowers/plans/2026-07-20-shard-manager.md` | storage T12 + ShardManager plan (21/21 子任务完成) |
| `docs/superpowers/plans/2026-07-25-async-network-stack.md` | async network stack plan (Phase 1-5, 15 任务) |
| `docs/superpowers/plans/2026-07-26-stress-verify-bug-investigation.md` | stress verify bug 排查时间线 (F32 阶段) |
| `crates/page/src/dump.rs` | 调试工具: 解析输出 page 结构 |
| `crates/logging/src/lib.rs` | nlog 模块, 含 io_uring 协程融合 logger 设计说明 |
| `scripts/smoke.toml` + `scripts/smoke_client.py` | 服务器端到端 smoke 测试 (含 redis-cli 验证步骤) |

---

## 提 issue / 改 plan 时

- 设计的总入口是 `DESIGN.md §3.4` (调度) / `§4.2-§4.3` (page) / `§4.3-§4.7` (storage)
- plan 里所有数字 (POOL_SIZE, BATCH_SIZE, MIN/MAX_PER_CHECKPOINT, MATE_CACHE_SIZE, INDEX_SIZE 等) 都从这些章节来
- 改 plan 请同步改 spec, 改 spec 请同步改 plan

---

> 如果你从外部接手, 先读:
> 1. 这份文件 (5 分钟)
> 2. `DESIGN.md §3.4` (15 分钟)
> 3. `docs/superpowers/specs/2026-07-17-scheduler-crate-design.md` (30 分钟)
> 4. `docs/superpowers/plans/2026-07-17-page-item-revision.md` (15 分钟)
> 5. `docs/superpowers/plans/2026-07-18-storage-crate.md` (15 分钟) — T9-T11 catalog 设计
> 6. `docs/superpowers/plans/2026-07-20-shard-manager.md` — T12 ShardManager 计划
> 7. `docs/superpowers/plans/2026-07-25-async-network-stack.md` — async network stack 计划
> 8. `crates/page/src/dump.rs` — 调试工具 (排查问题时很有用)
> 9. `CHANGELOG.md` — 当需要看修复历史 / 测试进度 / gotchas 时按需查阅