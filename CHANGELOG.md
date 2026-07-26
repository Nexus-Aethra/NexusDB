# NexusDB — Changelog & Hindsight

> 详细修复历史 + 测试进度快照 + 环境 gotchas + 测试文件清单.
> 本文件由 `AGENTS.md` 拆分而来 (2026-07-20), AGENTS.md 只保留项目入口与设计原则摘要.

**逆序时间线 (最新在上).**

---

## 2026-07-26 会话 (多协议门面 + 两个关键死锁/损坏修复 + 异步落盘)

### 修复总览

| # | 修复 | 文件 |
|---|------|------|
| F38 | 协议层三件套: value type tag + KV 长度限制 + RESP2 (Redis 兼容) 门面 | `network/src/value_codec.rs` (新), `protocol/resp.rs` (新), `protocol/mod.rs`, `worker.rs`, `server.rs`, `config`, `src/main.rs` |
| F39 | ⭐ **pollster 死锁**: IoUring 下 shard 线程永久 futex 睡死 | `shard_manager/src/manager.rs` (`block_on_io`) |
| F40 | ⭐ **leaf_update 段首损坏**: 覆盖写段首 item 破坏 shared=0 不变量 | `page/src/leaf.rs` |
| F41 | 异步 chunk 落盘 + 有界背压 + reply 通知合并 + send_reply 顺序竞态 | `storage/src/pager.rs`, `pager_io.rs`, `chunk_writer.rs`, `shard_manager/src/manager.rs`, `task_reply_bus.rs` |

### F38: 多协议门面 (Redis 兼容)

- **value type tag**: 存储格式 `[tag u8][payload]`, 本阶段全写 `TAG_RAW=0x01`, 预留 I64/F64/STR/DOC 给 SQL/Mongo 门面 (避免存量迁移); worker Put 打 tag, Get 剥 tag
- **KvLimits**: 默认 key≤1024 / value≤3000, parse 后进 shard 前拦截. 上限依据: page 编码路径 `[0u8; 4096]` 栈缓冲 = 单 item 硬上限; config 校验 `max_key + max_value <= 4060`
- **RESP2 门面**: SET/GET/DEL(多key聚合`:N`)/PING/ECHO/AUTH/QUIT/HELLO/SELECT/COMMAND; AUTH 按 Redis 官方语义 (NOAUTH/WRONGPASS/no password is set), worker 本地处理不进 shard
- **FIFO 重排** (RESP 无 req_id): per-conn 递增 seq 作 req_id, 回复经 BTreeMap 重排严格按序; 本地命令也占 seq 保证 pipeline 顺序
- **双协议 server**: main 同时起 Binary(5433) + RESP(6379), worker_id 空间隔离 (`worker_id_base`), `ShardManagerOptions.reply_bus_count` 扩容
- 验证: 真实 redis-cli 全链路 AUTH/SET/GET/DEL/PING 通过

### F39: pollster 死锁 (现象: PING 通、SET 永久卡死)

- **现象**: 服务器启动后每个 shard 在第一次写入后 ~10s (周期刷盘) 准时卡死在 `futex_do_wait`; Ctrl-C 无 "stopped"
- **根因**: shard 线程用 `pollster::block_on` 跑 engine async; IoUring 下 `io_ops::fsync` 首次 poll 提交 SQE 后 Pending, pollster park 线程; 而 CQE 收割在**下次 poll 的 CQ 扫描**里 — 线程睡死后无人再 poll → 永久死锁. buffered write 常在 submit 内同步完成所以 stress 一直没暴露; fsync 被 punt 到 io-wq 必输竞态
- **修复**: `block_on_io()` — Pending 后短 spin/yield 重 poll (poll 内部自带 CQ 收割), 替换 manager.rs 全部 20 处 pollster. 符合 "Future 自取 CQE" 契约

### F40: leaf_update 段首 shared=0 损坏 (memtier 发现)

- **现象**: memtier 首轮即报 `-ERR ... segment head item must have shared=0, got shared=15`
- **根因**: 被覆盖的 key 恰是 checkpoint 段首时, 段内扫描第一个就命中, `prev_ptr` 被初始化为 target 自身 → `prev_key == key` → 重编码 shared=len-1, 破坏段首自包含不变量. 长公共前缀 key (memtier-XXXXXXXX) + 覆盖写必现; 顺序 key 测试难触发
- **修复**: target 在段首 (`prev_ptr.byte_offset() == old_off`) 时 prev_key 视为空; 回归测试 `update_segment_head_keeps_shared_zero`

### F41: 异步 chunk 落盘 + 有界背压 (写吐出 3.5x)

- **问题**: `drive_write_queue`/`maybe_periodic_flush` 在 `block_on_io` 内串行 write+fsync 阻塞 shard 循环 → 写重 p99 40ms
- **方案**: 所有权转移式异步化 — `FlushJob{key, bytes: Rc, dir, io: Rc<PagerIo>}` 零 Pager 借用, shard 线程 `spawn_on` 协程落盘, 主循环每轮收割 (`complete_flush`) + `drive_until_idle` 推进; 磁盘 IO 与内存写完全并发
- **有界背压**: `MAX_INFLIGHT_CHUNKS=8`, 超限时 swap 退化同步落盘 (写入自然降速到磁盘速度, 零死锁风险)
- **正确性钢筋**: 同 key 去重 (不并发写同 offset); 读路径 in-flight 可见 (五源查找); meta 仅在 backlog 排空后刷 (data→meta 不变); flush/close 前 `drain_async_flush` 排空; 失败回 pending 重试
- **附带**: TaskReplyBus 通知合并 (首条写 eventfd, N 条回复 N→1 次 syscall); send_reply 顺序竞态修复 (先推 sink 再唤醒 client)
- **效果** (memtier, io_uring, 真实持久化): 写重 1:1 44K→**153K** ops/s (p99 40→16.7ms); 读混合 1:10 298K→**1.06M** ops/s (同步刷盘停顿之前连读一起卡)

### Benchmark 快照 (同机 Redis 8.6.2 对照, memtier 2t×10c, 32B)

| 场景 | NexusDB | Redis (AOF everysec) |
|---|---|---|
| pipe=16, 1:10 | 1.06M | 1.83M |
| pipe=16, 1:1 写重 | 153K | 1.51M |

差距构成: worker↔shard 两跳 handoff (可解, 终局方案 shard 自包含网络) + 每写 16KB 页 COW 写放大 (WAL 可解) + 有序 B+Tree vs hash (结构性).

---

## 2026-07-25 会话 (btree 路由修复 + 通信层优化 + 独立服务架构 + 成品化)

### 修复/特性总览

| # | 内容 | 文件 |
|---|------|------|
| F33 | ⭐ **btree_insert leaf split 路由错误** (stress phase4 1-3/600 key 丢失的根因): split 后无条件插 right, 非顺序插入下 key < split_key 时错位 → 改条件路由 `key > split_key ? right : left`; 附带 MetaCache 零槽 phantom entry 修复 | `storage/src/btree.rs`, `meta_cache.rs` |
| F34 | shard 通信层: ShardInbox (无锁 ring + eventfd + coalescing) 替换 mpsc; spin-then-park; adaptive spin-poll; Batch API (`batch_ops`, batch=64 时 +98%) | `shard_manager/src/inbox.rs` (新), `reply.rs`, `manager.rs` |
| F35 | NowChunks 自动持久化: chunk 满 (64 page) 自动 swap 入 WriteQueue; 周期(10s)/计数(256写) 刷盘; **退出完整性** (close 排空 + break 后置); ⭐ WriteQueue stale 快照回滚覆盖新数据修复 (`remove_pending`) | `storage/src/pager.rs`, `chunk_writer.rs`, `engine.rs` |
| F36 | 独立服务架构: TaskInbox/TaskReplyBus (network→shard 直连, 零 client 线程模型), worker 重写为 epoll 事件循环, pipeline 支持; ⭐ EPOLLET 丢事件改水平触发; ⭐ server accept 连接补 TCP_NODELAY (Nagle 延迟 13x); inbox drain 丢唤醒竞态修复 (先重置 pending 再 pop); ReplyFuture waker 修复 | `shard_manager/src/task_inbox.rs` (新), `task_reply_bus.rs` (新), `network/src/worker.rs` (重写), `acceptor.rs`, `server.rs` |
| F37 | 成品化: `config` crate (TOML+serde), `nlog` crate (io_uring 协程融合 logger, 无锁前端 + 专用 log 线程 + 量/时间双阈值), main.rs 服务器化 (信号优雅退出); ⭐ scheduler `io_registry.take_result` 误删未完成 entry 修复 | `crates/config/` (新), `crates/logging/` (新), `src/main.rs`, `scheduler/src/io_registry.rs` |

### 其他要点

- io_backend 切换 io_uring (避免等待期内核切换): stress 192K→368K ops/s
- 多处测试 PRNG seed `tid * 0x9E37...` debug 溢出 → `wrapping_mul` (repro_verify×2, stress×2)
- leaf_split 统一为 checkpoint 段边界 bulk memcpy (字节中点选段 + 整段 copy + `force_split_segment_at_mid`), 无双路径分歧
- 网络压测结论: 单连接 ping-pong 是客户端瓶颈, pipeline=16 + TCP_NODELAY 后 16K→53K→61K (12conn×pipe64)

---

## 2026-07-24 会话 (Async Network Stack + missing key 排查)

### F32: network crate — 异步网络栈骨架搭建

| F32 | **Async Network Stack Phase 1-4: 网络栈骨架 + 压力测试 + missing key 排查** | `crates/network/` (新建 crate, 7 个源文件 + 1 example + 4 个测试文件), `crates/scheduler/src/await_predicate.rs` (新建), `crates/scheduler/src/park.rs` (新建), `crates/storage/src/pager.rs` (read 路径加固), `crates/network/examples/network_stress.rs` (新建), `crates/network/tests/repro_verify.rs` (新建, 9 测试), `crates/network/tests/repro_verify_minimal.rs` (新建, 1 测试), `crates/storage/tests/repro_verify_storage.rs` (新建, 3 测试) | |

#### 架构变动

1. **新建 `network` crate** — 7 个模块:
   - `protocol/` — `Protocol` trait + `BinaryProtocol` 实现 (二进制帧: `|total_len:u32|req_id:u64|op:u8|key_len:u16|val_len:u32|key|val|`). max frame 16MB.
   - `acceptor.rs` — 非阻塞 acceptor loop, 支持 RoundRobin/Random/Sticky LB 策略
   - `worker.rs` — WorkerPool: N 个 worker thread, 每个 conn spawn OS thread, 同步 ShardManager API
   - `server.rs` — NetworkServer 顶层组装: acceptor + worker pool + 优雅关闭 (AtomicBool stop + drop inbox)
   - `kv_to_shard.rs` — Application Layer: Request → ShardManager::put/get/delete → Response
   - `reply_bus.rs` — ReplyBus: crossbeam unbounded channel, 实现 `ReplySink` trait, 支持异步 reply 路由 (Phase 1 完成, Phase 6+ 正式接入)

2. **Scheduler crate 扩展**:
   - `await_predicate.rs` — `AwaitPredicate` future: 基于谓词的协程等待, 配合 `park::register_parked` 实现
   - `park.rs` — park/unpark 机制, 全局 slot 存储 waker

3. **Pager read 路径加固** (⭐ 关键修复):
   - `Pager::read()` 和 `read_into()` 现为**四源查找**: `nowchunks → WriteQueue(pending) → WriteQueue(completed) → chunk_list → disk`
   - 新增 `WriteQueue::peek_chunk_pending()` 和 `peek_chunk_completed()` 方法, 让读路径能看见 WriteQueue 中正在落盘或已完成落盘但尚未插入 chunk_list 的数据
   - 修复: 之前读路径只查 nowchunks + chunk_list + disk, 忽略 WriteQueue, 导致 `flush` 过程中 put 后立即 get 可能读到 stale 数据

#### 压力测试工具

- `crates/network/examples/network_stress.rs` — 多 client 多 shard 完整网络层压测:
  - Phase 1: warmup (N clients × 200 put)
  - Phase 2: mixed workload (50/30/20 put/get/delete, N clients × M ops)
  - Phase 3: setup verify keys (N clients × 100 put)
  - Phase 4: verify (重读所有 verify keys)
  - 输出: ops/sec, error count, verification errors

#### 问题排查: missing key bug

**现象**: 高并发压力测试 (6 clients, 6 shards, ~30K total ops) 下, phase 3 写入的 key 在 phase 4 验证时 ~0.2% 返回 `Get(None)`, 即 key 丢失。

**排查过程**:

1. **旧框架 (原生 ShardManager 同步 API)** — 在 T14 同步 API 下运行 stress.rs, 发现 phase 4 有 missing key
2. **新框架 (NetworkServer + TCP)** — 在 network_stress 下运行, 发现 missing key 仍然存在 (~0.2% 错误率)
   - 性能提升: 50K → 143K ops/sec (因 NetworkServer 多 worker 线程并行处理)
   - 但正确性问题未解决
3. **Storage 层独立复现** — 新建 `repro_verify_storage.rs` 在 storage 层直接模拟 phase 2 + phase 3 流程, 排除网络层干扰
   - 发现 `phase3_put_v0_then_get_v0_works` 在高并发下仍然有 missing key
   - 确认 bug 在 storage 层, 不在网络层或 ShardManager 层
4. **最小化复现** — `repro_verify_minimal.rs` 最小化场景: 6 shard × 6 client × phase 1+2+3+4
   - 假设根因: `nowchunks.peek_chunk` 在并发读写交错时可能返回 stale bytes

**关键发现**:
- 单线程场景下永不触发 (包括 `just_phase1_then_phase3_sequential`, `single_threaded_phase2_then_phase3`)
- 仅在多 client 并发时触发
- Phase 1 (warmup) + Phase 2 (mixed) 组合才触发, 单独 phase 2 不行
- 说明问题与**并发写入 + 存储层数据竞争**相关

**已实施的修复**:
- Pager read 路径加 WriteQueue 检索, 确保 flush 过程中的数据对读路径可见

**待深入排查**:
- BTree insert 过程中, 并发 get 可能读到 stale leaf page (nowchunks 中插入后尚未 meta_cache write)
- 多 shard 间 hash 路由的 key 分布可能在某些 shard 上造成热点, 触发 chunk_full → rotate 期间的竞争
- 建议在 `btree_insert` 和 `btree_lookup` 中添加更细粒度的调试日志, 追踪特定 key 的 put/get 时间线

#### 测试状态

基础测试 (page + storage + shard_manager + network fast tests) 全部通过:
```
page:          131 passed ✅
storage:       386+ passed ✅ (不含 repro_verify_storage 慢测试)
shard_manager: 28+ passed ✅
network:       21 passed ✅ (end_to_end/integration_reply_bus/protocol_binary/reply_bus)
workspace:     ~549 passed ✅ (0 failed, 2026-07-22 快照)
```

> **注意**: `repro_verify_storage` (3 测试) 和 `repro_verify` (9 测试) 和 `repro_verify_minimal` (1 测试) 为高并发复现测试, debug 模式下跑非常慢 (~10 分钟), 建议 `cargo test --release` 运行.

#### clippy 状态

全 clean, 0 警告 (page crate 旧 warning 除外).

---

## 2026-07-22 会话 (T14: ShardManager 2PC 跨 shard 协调 + 同步 API)

### F31 ShardManager 2PC + 同步 API 完成

| F31 | **T14 ShardManager 2PC 跨 shard 协调 + 同步 API + e2e 测试** | `crates/shard_manager/src/coordinator.rs` (新建, ~330 LOC), `crates/shard_manager/src/request.rs` (扩展), `crates/shard_manager/src/manager.rs` (改造, ~250 LOC), `crates/shard_manager/tests/two_pc_e2e.rs` (新建, 8 测试) | (1) **⭐ 2PC 协议消息**: `ShardRequest` 新增 6 个变体: `PrepareCreateDb/CommitCreateDb/AbortCreateDb/PrepareCreateTable/CommitCreateTable/AbortCreateTable`. `ShardReply` 新增 `PrepareOk/CommitOk/AbortOk`. (2) **⭐ TwoPhaseCoordinator 状态机**: `coordinator.rs` 实现完整状态机: `begin_txn` → `on_prepare_ack/fail` → `on_commit_ack` / `on_abort_ack`. 跟踪 `prepare_acks` / `commit_acks` / `abort_acks` 集合. 全 ack 自动转换 phase. `check_timeouts` 处理悬挂事务. `history` 保留最近 64 个事务结果 (调试). (3) **⭐ 同步 API 接入 2PC**: `ShardManager::create_db/create_table` 走 2PC 流程: 1️⃣ `begin_txn` → 2️⃣ 给所有 shard 发 `PrepareXxx` + 收集 reply → 3️⃣ 全部成功 → 给所有 shard 发 `CommitXxx` → 4️⃣ 任一失败 → 给已 Prepare 成功的 shard 发 `AbortXxx` (reverse op: drop_db/drop_table) → 返回 `ShardError::PrepareFailed`. (4) **⭐ Abort 是 best-effort**: 失败也返回 `AbortOk` (人工恢复 / 下次 recover 清理). (5) **⭐ Coordinator `RefCell` 包装**: 让 `&self` 方法能访问 (`create_db/create_table` 同步 API 保持 `&self`). (6) **⭐ `handle_request_blocking` 加 `shard_id` 参数** (调试用, 现为 `_self_shard_id`). (7) **测试**: 15 个 lib 单元测试 (coordinator: 9 + router: 3 + request: 3), 5 个原 e2e 测试, 8 个新 2PC e2e 测试, **全部通过, 0 failed, clippy 0 警告**. |

### 关键测试覆盖 (8 个 2PC e2e)

| 测试 | 验证 |
|---|---|
| `two_pc_create_db_visible_on_all_shards` | 4 shard, create_db 后任何 shard 都能 put (db 同步生效) |
| `two_pc_create_table_visible_on_all_shards` | create_table 后 put/get 跨 shard 正常 |
| `two_pc_create_db_persists_across_reopen` | 关闭重开, db/table/data 全部持久化 |
| `two_pc_create_db_duplicate_triggers_abort` | 重复 create_db 返回 Err, 第一次的 db/table 仍可用 |
| `two_pc_metadata_with_cross_shard_routing` | 多 db 多 table + 40 个 key 跨 4 shard 分布 + 全部能 put/get |
| `two_pc_each_shard_has_independent_state` | 文件系统验证 3 个 shard 目录都独立存在 |
| `two_pc_multiple_dbs_and_tables` | 3 db × 2 table 全部能 put/get |
| `two_pc_error_propagation_via_get` | 不存在 db/table → 错误透传到 ShardManager |

### 2PC 同步 API 性能影响 (已识别, T15 优化)

| 问题 | 严重度 | 缓解 |
|---|---|---|
| **主线程串行化**: 接收线程 `mgr.put` → 阻塞等 reply, 单核瓶颈 | 高 | T15 提供 async API (`put_async` 返回 Future), 接收层不阻塞 |
| **mpsc 序列化 + 跨线程调度延迟**: 增加 ~50μs per request | 中 | T15 pipeline (发 N 请求 + 异步收 N reply) |
| **io_uring 多 SQE overlap 用不到**: 当前一请求一 IO 模型 | 中 | T15 batch + pipeline |

---

## 2026-07-21 会话 (T17: 全 async 重构 + io_uring 集成 + 栈修复)

### F29 T17: Storage 全 async 重构完成

| F29 | **T17 Storage async 重构 + io_uring + 栈修复** | `crates/storage/src/engine.rs`, `crates/storage/src/pager_io.rs`, `crates/storage/src/pager.rs`, `crates/storage/src/btree.rs`, `crates/storage/src/registry.rs`, `crates/storage/src/table_directory.rs`, `crates/storage/tests/common/mod.rs`, `crates/storage/tests/*` (全部 20 个) | (1) **PagerIo 抽象层**: `PagerIo` 枚举 = `StdFs` / `IoUring`, 通过 `PagerIoBackend` trait 选择. `PagerIo::new(IoBackend)` 工厂. (2) **T16 实施**: 把同步 IO 全部走 `PagerIo::read_page_chunk/write_page_chunk/fsync_block`. `OpenOptions` 加 `io_backend: IoBackend` 字段 (默认 `StdFs`). (3) **T17 async 改造**: `Pager::create/read/write_page/flush` + `StorageEngine::open/close/put/get/create_db/drop_db/create_table/drop_table/open_table/table_put/table_get/table_delete/create_table_directory/open_table_directory` + `Registry::create_table/drop_table/open_table/load/refresh_table_cache/create_db/drop_db/flush` + `TableDirectory::create_new/open/create_table/drop_table/get_table/list_tables/load_or_create/update_table/table_count/flush` + `btree::btree_insert/btree_lookup/btree_delete/btree_update/travel_to_leaf` 全部 async. (4) **⭐ Stack Overflow 修复**: Storage async fn 内联后 poll 函数含多个 `[u8; PAGE_SIZE=16KB]` 局部变量, 多个 inline 后单次 poll 占用大量栈. 默认 8MB 线程栈不够. 测试用 `RUST_MIN_STACK=67108864` (64MB) env 启动. `tests/common/mod.rs` 文档化此约束. (5) **obsolete test 修复**: `recover_stops_at_empty_page` / `recover_stops_at_corrupted_page` 旧版本假设"遇坏 page 停止", 新 recover 跳过继续. 改断言. (6) **clippy 清理**: 移除 `let _ = future` warning (用 `drop(future)`), 移除 unused imports. **测试 367 → 386 (+19), 0 failed, clippy 仅剩 page crate 旧 warning**. |

### F30 Pager::flush 写放大修复 (LCB-Tree append-only 优化)

| F30 | **T17b: flush 64x 写放大 → 1x + vpid 复用 (in-nowchunk 原位覆盖)** | `crates/storage/src/pager.rs`, `crates/storage/src/chunk_writer.rs`, `crates/storage/src/meta_cache.rs`, `docs/superpowers/plans/2026-07-18-storage-crate.md` | (1) **⭐ 性能修复 1**: 原 `Pager::flush` 每次做 `disk_read(1MB) + merge + disk_write(1MB)` — 相对 16KB page write 是 **64x 写放大**. 修复后: 直接写 nowchunks 1MB, 不 merge 不读 disk. **写放大 64x → 1x**. (2) **关键技术挑战**: 原 read+merge 不是冗余, 是必要的修补 — `nowchunks.take_chunk` 是 remove 语义, 后续 SUBMIT 同一 key 时 `entry().or_insert_with(ChunkBuf::new)` 重建全 0 ChunkBuf, **丢失 page_idx 0..N-1 历史数据**. read+merge 从 disk 加载旧 chunk 来恢复这些 page. (3) **⭐ 关键 insight**: nowchunks 累积覆盖语义 (每次 write_page 只覆盖 page_idx 那一页, 其他位置保留) + 复用原 pid (vpid 在 nowchunks 中), nowchunks 本身是完整 chunk 视图, flush 直接写即可. (4) **⭐ 性能修复 2 — vpid 复用**: `PageWriteBatch::submit` 在 alloc pid 前查 `MetaCache::is_dirty(vpid)`. 如果 dirty (in nowchunks) → **复用原 pid, 原位覆盖** page_idx. 否则 (新 vpid 或已 flush) → COW alloc 新 pid. 新增 `MetaCache::is_dirty(vpid) -> bool` API (用 entry.dirty 字段). **节省 page_idx 槽位**, 不浪费磁盘空间. (5) **write 路径加固**: nowchunks miss chunk key 时 (reopen 后) 从 chunk_list 或 disk 加载完整视图 (reinsert_clean). (6) **更新设计文档**: `docs/superpowers/plans/2026-07-18-storage-crate.md` §3.3 "核心场景" 增加"场景 2: in-nowchunk page 改写 (T17b 复用优化)". 新增 §3.3.1 "Pager::flush 写放大修复 (T17b ⭐ 性能优化)" 章节. (7) **测试更新**: `batch_add_same_vpid_twice_overwrites` 断言改为"复用同一 pid" (匹配新设计). (8) **测试**: 全部 386 storage tests + 549 workspace tests 通过, clippy 0 警告. |

---

## 2026-07-21 会话 (T15: 多层 BTree + reopen 持久化)

### 实施 T15.1 — chunk_offset 误用全局偏移根因修复

| F28 | **T15.1 修复 reopen 持久化 — chunk_offset 误用全局偏移** | `crates/storage/src/pager.rs`, `crates/storage/src/recover.rs`, `crates/storage/src/btree.rs`, `crates/storage/src/table_directory.rs`, `crates/storage/tests/multi_level_btree_e2e.rs` | (1) **⭐ 根因**: `chunk_offset` 用 `file_id * BLOCK_SIZE + chunk_idx * CHUNK_SIZE` (全局偏移). 但每个 `.block` 文件是独立物理文件, offset 总是从 0 开始. 导致 file 2 (000003.block) 的 page 14 被写到 21.2MB 全局位置 (sparse extension), 而 scan 仍按 0..PAGES_PER_CHUNK 读 page 0..14. 实际上 page 14 数据在文件末尾 (chunk 20 page 14), scan 读 page 0..14 都看到 0. recover 误以为 file 2 chunk 0 没有数据, vpid 1 映射丢失, reopen 后路由到旧的 vpid 2 (left leaf). (2) **修复**: chunk_offset 改为 `chunk_idx * CHUNK_SIZE` (文件内偏移, file_id 通过文件名选择 .block file). load_chunk_from_disk 同样改. scan_block_file 跳过空白 page 继续扫描 (sparse file 容错). test chunk_offset_per_file 更新为新语义. Pager::flush 打开 .block 用 `read(true)` 允许 self-verify. (3) **8 个 e2e 测试通过** (multi_level_btree_e2e). (4) **全 workspace 155+ 测试通过, 0 failed, clippy 0 警告 (storage), fmt 0 差异 (storage)**. |

### 实施 T15.0 — 多层 BTree 路由 + TableDirectory 升级 (WIP 后续被 F28 完整修复)

| F27 | **T15.0 WIP: 多层 BTree 路由 + TableDirectory 升级** | `crates/storage/src/btree.rs` (新建, ~700 LOC), `crates/storage/tests/multi_level_btree_e2e.rs` (新建, 8 测试), `docs/superpowers/plans/2026-07-20-multi-level-btree.md` (新建) | (1) **⭐ Internal page vpid 字段保留** — chunk_writer 区分 Internal/非 Internal page type, 仅对非 Internal 自动覆盖 vpid 字段. Internal page 的 vpid 字段由 caller 自己设置 first_child, Pager 不覆盖 (否则死循环). (2) **⭐ recover Internal page 跳过 vpid 信任** — scan_block_file 对 Internal page (page_type=2) 不写 meta.write, 因 vpid 字段已被 page crate 复用作 first_child. (3) **⭐ propagate_split_up 修复** — split 后 (current_split_key, current_right_vpid) 必须插入正确的 half. 原代码无条件 insert 到 parent_right, 在 current_split_key < new_split_key 时错位. 修复: 比较大小决定进 left 还是 right. (4) **⭐ leaf_split 触发的 key 重新插入 right** — 在 leaf_split 返回 right_bytes 后, 调 leaf_insert 把触发的 key 加进 right page. (5) **⭐ engine::table_put root split 同步 TableDirectory** — 当 btree_insert 返回 new_root 时, 调 TableDirectory::update_table 持久化新 root + 更新 DbHandle.tables 缓存. (6) **⭐ TableDirectory 升级到多层 BTree** — 复用 page crate leaf/internal API, 支持 >200 个 tables. (7) **⭐ Registry::update_table_root + DbHandle.update_table_root** — root split 缓存同步. (8) **⭐ btree_collect_leaves** — 支持多层 BTree 的 list_tables. (9) **7/8 测试通过** (reopen_after_split FAIL, 由 F28 修复). (10) clippy 0 警告, fmt 0 差异 (storage). |

---

## 2026-07-20 会话 (T12: ShardManager 集成)

### 实施 T12.18-T12.21 — 多 db 物理隔离 e2e + catalog_consistency 重写

| F27 | **T12.18-T12.21: 多 db 物理隔离 e2e + catalog_consistency 重写** | `src/engine.rs` (修复路径解析 bug), `tests/multi_db_physical_isolation.rs` (新建, 9 个测试), `tests/catalog_consistency_tests.rs` / `tests/engine_e2e.rs` / `tests/meta_page_tests.rs` (改用新路径模式) | (1) **⭐ 关键 bug 修复**: `StorageEngine::open` 路径解析里 tuple 第二项 `db_name` 被硬编码为 `DEFAULT_DB_NAME.to_string()`, 导致 `recover_for_shard` 扫描 `{block_root}/default/shard_0/` 而非 `recover_for_shard(&opts.block_root, &opts_db_name, ...)` 实际扫描的 `{block_root}/{opts.db_name}/shard_0/` — 两条路径不一致, 多 db 模式 recover 永远走 default 目录. 修复: 重命名局部变量 `opts_db_name`, tuple 第二项用 `opts_db_name` 而非 `DEFAULT_DB_NAME`. (2) **T12.18**: 新建 `tests/multi_db_physical_isolation.rs` 9 个 e2e 测试: 两 db 路径独立 / 独立数据 / 单 db 损坏隔离 / drop_db 不清理物理目录 / 备份恢复 / 同 process 多 engine 隔离 / 不同 shard_id 隔离 / 多次 reopen 稳定 / 路径结构符合 plan §1. (3) **T12.21**: `catalog_consistency_tests` / `engine_e2e` / `meta_page_tests` 的 `setup()` 改用新路径模式 (`block_dir: None, db_name: Some("default")`), 替代 compat `block_dir: Some(tmp.path())`. 修复 `engine_get_after_external_modification` 期望路径从 `tmp/000001.block` 改为 `tmp/default/shard_0/000001.block`. (4) clippy 修复: `&PathBuf` → `&Path`, `&path.to_path_buf()` → `path`. **测试 327 → 367 (+40), 0 failed, clippy 0 警告, fmt 0 差异**. **T12 全部 21 子任务完成**. |

### 实施 T12.14-T12.17 — MetaPage Resolver + StorageEngine current_db + OpenOptions db_name

| F26 | **T12.14-T12.17: DbNameResolver + StorageEngine current_db + OpenOptions db_name** | `src/db_name_resolver.rs` (新建, ~280 LOC), `src/meta_page.rs` (集成 Resolver 段), `src/alloc.rs` (PidAllocator db-aware), `src/pager.rs` (MetaPage COW 修复), `src/engine.rs` (current_db + db_name), `src/registry.rs` (db_id/db_name) | (1) **T12.14**: 新建 `DbNameResolver` 模块, `names: Vec<String>` + `name_to_id: HashMap<String, u32>`, 提供 `new / get_or_create / resolve / name / list`, 序列化到固定 1024 字节段 (兼容 8B align). (2) `MetaPage` 头部 `[40..1064]` 预留给 Resolver 段, `[1064..PAGE_SIZE-16]` 为 item 区. `flush()` 序列化 Resolver + db 镜像, `load()` 反序列化. (3) **⭐ MetaPage COW 修复**: `PageWriteBatch::submit` 对 `META_VPID` 特殊处理直接用 `META_PID`, 避免被 COW 到新位置. (4) **T12.16**: `StorageEngine` 新增 `current_db: DbId` 字段, 方法 `current_db/current_db_name/use_db/set_current_db`. `DbRegistry::db_id/db_name` 走 MetaPage resolver. 5 个新测试. (5) **T12.17**: `OpenOptions` 加 `db_name: Option<String>`, 路径解析 `block_root/{db_name}/shard_{N}/`. (6) 修复: `PidAllocator::alloc_db` 用 `or_insert_with` 保留初始 state; `multi_page_sync_tests::new_pager` 把 `pid_alloc` 起点设为 page 1 跳过 MetaPage; `db_name_resolver` 测试适配 1024B 段大小. (7) 多个 OpenOptions 初始化补全 `db_name` 字段. (8) `multi_db_physical_isolation.rs` 新建 9 个 e2e 测试. (9) 测试 **289 → 327 (+38) + 9 multi_db e2e, 0 failed, clippy 0 警告, fmt 0 差异**. |

### 实施 T12.12+T12.13 — Pager/recover 路径加 block_root + shard_id

| F25 | **T12.12+T12.13: Pager::new + recover 走 `{block_root}/{db_name}/shard_N/` 路径** | `src/types.rs` (+9 LOC), `src/pager.rs` (+68 LOC), `src/recover.rs` (+165 LOC), `src/engine.rs` (+228 LOC), 11 个 integration tests + 5 个 unit tests (compat/fallback/shard-picking) | (1) `types.rs`: `pub type ShardId = u32;` + `DEFAULT_SHARD_ID: ShardId = 0`. (2) `Pager` 新增 `block_root` + `db_name` + `shard_id` 字段, `block_dir` 内部拼装. (3) `Pager::new(block_dir, ...)` 兼容 API 走 db=0 + shard=0. (4) 新 `Pager::new_for_shard(block_root, db_name, shard_id, ...)` 走新路径, `block_dir = recover::shard_dir_path(...)`. (5) `recover` 委托给 `recover_for_shard`. (6) `recover_for_shard(block_root, db_name, shard_id)` 三级 fallback: 优先 `{block_root}/{db_name}/shard_N/`, 否则 `block_root` 直接是 compat block_dir, 否则用 shard_dir. (7) `OpenOptions` 加 `block_root` + `block_dir: Option` + `shard_id` 字段. (8) `StorageEngine::open` 根据 `block_dir` 是否为 `Some` 选择 `Pager::new` 还是 `Pager::new_for_shard`. (9) 11 个 integration tests (含 `open_with_block_root_creates_shard_dir_layout` / `open_with_block_dir_compat_still_works` / `open_different_shard_ids_use_separate_dirs`) + 5 个 unit tests (`shard_dir_path_format` / `recover_for_shard_compat_fallback_to_block_dir` / `recover_for_shard_finds_shard_dir_layout` / `recover_for_shard_picks_correct_shard` / `recover_for_shard_missing_dir_returns_empty`). (10) caller (pager/recover/engine) zero regression — 全部 14 个 storage test 文件 + 31 个 Page test 文件 + 1 个 scheduler doc test 通过. **327 passed + clippy 0 警告 + fmt 0 差异**. **T12.12+T12.13 完成**, 还剩 T12.14-T12.21 (DbNameResolver + 公共 db API + 多 db 物理隔离 e2e + catalog_consistency 重写). |

### 实施 T12.7-T12.10 — 4 个子任务, 17 个新单元测试

| F24 | **T12.7+T12.8+T12.9+T12.10: Allocator / FreePageQueue / ChunkList / ChunkWriter 加 DbId 维度** | `src/alloc.rs`, `src/chunk_lru.rs`, `src/chunk_writer.rs` + 17 个新单元测试 | (1) **T12.7 VpidAllocator per-db**: `HashMap<DbId, VpidState>`, 新 `alloc_db/free_db/current_db/free_count_db/set_initial_db/dbs()`, compat 默认 db=0. (2) **T12.8 PidAllocator per-db**: 同结构, `alloc_db/rotate_to_db/current_db/set_initial_db`, `FreePageQueue` 改 `HashMap<DbId, Vec<u16>>` + `push_db/pop_db/clear_db`. (3) **T12.9 ChunkList ChunkKey 加 db**: `ChunkKey { db, file_id, chunk_idx }`, 单 ChunkList 8MB 跨 db 共享 LRU, 新 `contains_db/keys_for_db`, `PageKey` 转换保留 compat (走 db=0). (4) **T12.10 ChunkWriter per-(db, file_id)**: `block_paths: HashMap<(DbId, u32), PathBuf>`, 新 `register_db_block(db, file_id, path)` + `registered_files()`, `mkdir -p` 父目录兜底, `truncate(false)` 保留已存在内容. (5) `Pager::open`/test 23 处 caller 加 `db: DEFAULT_DB_ID,` 通过 sed 批处理. (6) **17 个新单元测试**: alloc 10 个 (VpidAllocator per-db 4 + PidAllocator per-db 3 + FreePageQueue per-db 2 + compat 1) + chunk_lru 5 (isolation / capacity_shared / lru_within_db / keys_for_db / invalidate) + chunk_writer 3 (register_paths / creates_file / preserves_existing). (7) Caller (pager / recover / engine) zero regression — 仍走 compat API. **319 passed + clippy 0 警告 + fmt 0 差异**. |

### 实施 T12.6 — MetaCache 加 DbId 维度

| F23 | **T12.6: MetaCache 加 DbId 维度 + 13 个新测试 + evict 触发 bug 修复** | `src/meta_cache.rs` (entries → `HashMap<MetaKey, CachedPid>`), `tests/meta_cache_tests.rs` 13 个新测试 | (1) `FreqEntry { key: MetaKey, freq, seq }`. (2) `entries: HashMap<MetaKey, CachedPid>` 跨 db 共享. (3) 新 db-aware API: `read_db(db, vpid)` / `write_db(db, vpid, pid)` / `contains_db(db, vpid)` / `freq_db(db, vpid)` / `len_db(db)`. (4) compat vpid-only API 仍走 db=0 (`DEFAULT_DB_ID`). (5) ⭐ **修复 bug**: 旧版 `evict_if_needed` 用 hard_cap 作触发条件, 导致 `soft_cap < len < hard_cap` 区间的 entries 永不淘汰. 新版直接用 `soft_cap` 作触发条件 (任何 len > soft_cap 都 evict_one). (6) 13 个新测试覆盖: LFU freq 递增 / write 重置 freq / 高频不被 evict / seq tiebreaker / freq per-entry 隔离 / dirty_count flush 重置 / reopen 后数据还在 / 零 caps 行为 / DbId 读写独立 / contains 不串 db / freq 独立 per db / multi-db 共享 cap / compat API 等价 db=0. **meta_cache_tests 18→31 (+13) + 全量 302 passed + clippy 0 警告 + fmt 0 差异**. |

### T12.4 MetaCache v2 重写 + F22 实施

| F22 | **T12.4 实施: MetaCache v2 重写 + 17 个旧测试迁移 v2 语义** | `src/meta_cache.rs` 重写 (~450 LOC), `tests/meta_cache_tests.rs` 17 个测试重命名 + 改 v2 语义 + 新增 `open_with_custom_caps` | (1) `MetaCache::open_with_caps(mate_path, soft, hard)` 默认 soft=1M / hard=1.5M. (2) `CachedPid { pid, freq, dirty, seq }` + `FreqEntry { vpid, freq, seq }`. (3) `BinaryHeap<Reverse<FreqEntry>>` min-heap, O(log n) 淘汰. (4) `maybe_decay()` 每 30s /2, 重建 heap. (5) `evict_if_needed()` 超硬 cap 时循环 evict 到 soft cap, **dirty entry 先 flush 到 mate 再 evict** (evict-before-flush 协议). (6) read on-demand pread 单 8B slot from mate (不再 1MB window). (7) flush_dirty pwrite 单 entry + sync_all 一次. (8) 兼容 API `read(vpid) / write(vpid, pid) / flush_dirty() / cache_size() / get_index(i)` (IndexEntry 保留 compat stub). **18/18 测试通过 + clippy 0 警告 + fmt 0 差异, zero regression (Pager / recover / engine 仍调, 接口不变)**. **总计 420 passed (Storage 289 + Page 131)**. |
| F21 | **T12.4 设计决策: MetaCache v2 = LFU + per-db page.mate** | `plan/2026-07-20-shard-manager.md §3.5` | 用户 2026-07-20 决策: 抛弃原 10MB sliding window 设计 (单 db 假设), 改为 (1) per-shard 单 LFU cache `HashMap<MetaKey, CachedPid>` + BinaryHeap freq tracking + (2) per-db 独立 page.mate fd `HashMap<DbId, File>`. freq 衰减 (每 N 秒 /2) 防陈旧热点. LFU 反映时间局部性, 跨 db 共享 cap, 动态伸缩 (soft/hard cap). 完全取代原 vpid-only 滑动窗口. |

### T12.1+T12.2 类型基础

| F20 | **T12.1+T12.2: types.rs 加 DbId type alias + MetaKey 复合 key 结构体 + IoBackend enum** | `src/types.rs` | (1) `pub type DbId = u32;` (4 字节 Copy, 替代 String 24B+heap). (2) `pub const DEFAULT_DB_ID: DbId = 0` / `DEFAULT_DB_NAME = "default"` 向后兼容. (3) `MetaKey { db: DbId, vpid: u64 }` `#[repr(C, align(16))]` 16B 对齐避开 hashbrown SSE2 UB. (4) `IoBackend { StdFs, IoUring }` + Default → StdFs. (5) 6 个新 types_tests. **零回归, clippy 0 警告, fmt 0 差异**. |

---

## 2026-07-19 会话 (Storage T1-T11 + F17-F19)

| F19 | **新增 `catalog_consistency_tests.rs` 12 个测试** | `tests/catalog_consistency_tests.rs` | 覆盖: MetaPage+TableDirectory 多 page 写回原子性 / close+reopen 数据持久化 / 多 db 隔离 / 大量 db+table 持久化 (50x20) / drop+recreate vpid 不重用 / cache miss BTree 同步 / drop_db 留 orphan / 空 TableDirectory 持久化 / 特殊字符名 / MetaPage 整页重写 50+ dbs. **同时 `lib.rs` re-export `RegistryError`**. |
| F18 | **clippy 自动修复: 加 `.truncate(true)` 导致数据丢失** | `chunk_writer.rs:323`, `meta_cache.rs:85`, `pager.rs:262`, `engine.rs:422` | (1) `cargo clippy --fix` 自动给 `OpenOptions::new().create(true)` 加了 `.truncate(true)`. (2) 严重后果: `truncate(true)` 在 reopen 已存在 .block / page.mate 时会清空文件, 抹掉所有 chunk data + MetaPage, 导致 reopen 读 vpid 0 时 `MetaPage::load` panic "bad magic", catalog_consistency_tests 全 12 测试失败. (3) 修复: 所有 4 处改为 `.truncate(false)` — 我们要保留已存在内容, 用 pwrite (pager.rs) / write_all_at (engine init_meta_page) 写到具体偏移, 不 truncate. (4) `meta_cache::open` 改为 `truncate(false)` 同时解决 meta_cache_tests::flush_with_no_dirty_windows_is_noop 失败 (test helper 创建 8KB mate, truncate(true) 会清空). |
| F17 | **T10/T11 重大重构: 移除 `TableDirectory` 的 `*mut Pager` 字段, 修复 aliasing UB** | `table_directory.rs`, `registry.rs`, `engine.rs` | (1) 根因: `DbRegistry` 持有 `*mut Pager` 指向 `StorageEngine::pager`, 同时 `StorageEngine` 持有 `&mut self.pager` — aliasing 触发 `vec::pop` 内 `hint::assert_unchecked` UB, 使 MetaCache / Allocator 内部 Vec 状态损坏 (后续 index 越界 panic). (2) 修复: `TableDirectory` 移除 `pager: *mut Pager` 字段, 改 `PhantomData<*mut Pager>` 保持 !Send/!Sync. 所有方法 (`create_table`/`drop_table`/`get_table`/`list_tables`/`table_count`/`flush`) 接收 `&mut Pager` 参数. (3) `DbHandle::create_table` / `drop_table` / `open_table` / `refresh_table_cache` 接收 `&mut Pager` 并向下传. (4) `DbRegistry::load` 调用 `table_dir.get_table(pager, ...)` / `list_tables(pager)`. (5) `StorageEngine` 增 `split_pager_and_registry()` / `pager_and_db(name)` 辅助 split borrow, 解决 caller 同时要 pager 和 DbHandle. (6) `registry_e2e::registry_db_handle_direct_api` 改用新 API. |

---

## 2026-07-18 会话 (Storage T8-T11)

| F16 | **T8: NowChunks vpid_map 跟踪 + Pager::flush disk-in-memory merge** | `chunk_writer.rs`, `pager.rs` | (1) NowChunks 加 `vpid_maps: HashMap<PageKey, HashMap<page_idx, vpid>>` 跟踪每个 chunk 写过的 vpid. (2) Pager::flush 改用 vpid_map 精确覆盖新写 page, 不再依赖"全 0 检测". (3) disk 与 nowchunks 合并, 保留历史 page 字节, 仅新写 page 替换. |
| F15 | **T8: engine_e2e 编译错误修复** | `engine_e2e.rs` | (1) `first_v.iter()` 错误: tuple → array `[u64; 3]`. (2) `opts` 移动: 添加 `.clone()`. (3) close 方法 `drop(self.pager)` 触发 cannot move out: 移除显式 drop, 依赖自动析构. |
| F14 | **T8: page header 区域 [0..0x28] (40B) 覆盖 caller data 字节 [0]** | `multi_page_sync_tests.rs`, `pager_round_trip.rs`, `travel_tree_tests.rs` | 数据验证从 `r[0]` 改为 `r[0x28]` 以匹配 page header 布局 (magic + page_type + version + vpid). caller 字节应假设 [0..0x28] 是 header 区域, 数据写在 [0x28..PAGE_SIZE]. |
| F13 | **T7 recover: page header 自描述 + MetaCache union 语义** | `recover.rs` (新建), `tests/recover_tests.rs` (新建, 11 测试) | 流程: 加载 page.mate (初值, 可能 stale) → scan block_dir 内所有 `.block` (按 file_id 升序) → 对每个 page 校验 magic + page_type → 调 `meta_cache.write(vpid, pid)` 覆盖 mate 同 vpid → 推导 next_vpid = max(seen)+1 / next_file_id / pid_alloc 状态. 简化版: 遇到 empty / corrupted page 停止扫描, 不解析 vpid log 格式 (T11 polish). |

---

## 2026-07-17 会话 (Page F1-F12)

| F12 | **新增 `apply_pre_merge_steal` 4 个单元测试** | `tests/steal_tests.rs` | 覆盖: steal 触发 (left 达 MIN) / left>=MIN 不触发 / 无右邻不触发 / right 太小不触发 |
| F11 | **`internal_delete` 缺少 k+1 重写 + `target_seg_idx` 越界 (B1 修复 + B3 修复)** | `internal.rs`, `leaf.rs` | (1) 复制 `leaf_delete` 的 k+1 shared_prefix_len 重写逻辑到 `internal_delete`, 避免删除后下一个 separator 的 shared 错位 (decoded as "1" instead of "k_0001" 之类); (2) `effective_seg_idx = min(target_seg_idx, segments.len()-1)` 处理"清理空段时把 target_seg_idx 移除"导致的越界 panic, 同样修复 `leaf_delete` |
| F10 | **新增 `pre_merge_segment` 4 个单元测试** | `src/index.rs` | 覆盖: 合并触发条件 / 不触发场景 (left >= MIN / 无右邻 / total > MAX) |
| F9 | **新增 `split_boundary_tests.rs` 12 个测试** | `tests/split_boundary_tests.rs` | 覆盖: internal_split child_vpid 流转 / split 后 PageIndex load / pre_split cp 段首 shared=0 / 段边界插入 / 空段清理 / 多轮 split child routing / 空 page 操作 / 增删 churn / internal separator 边界 routing |
| F8 | **`internal_split` 对无哨兵页(right page from previous split) 边界偏移 bug** | `internal.rs` | 检测源 page 是否有 sentinel, 动态调整 mid_off 和 mid_full_key 的 i 索引: 有哨兵 → i=mid 取左半最后, i=mid+1 取右半第一; 无哨兵 → i=mid-1 取左半最后, i=mid 取右半第一. 修复多轮 split 时丢 key 的 bug. |
| F7 | **`leaf_split_delete_split_delete_chaos` 测试 B2 (right_base 计算错误)** | `tests/stress_tests.rs` | `right_base` 必须在 left 删除前快照 (== split 时 left 的 key_count), 而不是删除后 (== 剩余 left keys). 修复后该测试通过. |
| F6 | **internal_push_back 在 cp 边界插入时 seg_idx 错位 (B1 修复)** | `internal.rs` | 用 prev_ptr.byte_offset() 而不是 insert_off 找段, 避免 find_segment_by_offset 把新 item 算到 seg[N+1] 里. 接口签名变更为 `internal_push_back(..., seg_idx: usize)`, 由 `internal_insert` 调用方传入. |
| F5 | **新增 `dump.rs` 调试模块** | `dump.rs` (新建), `lib.rs` | 两个调试入口解析 page 输出 header/items/cp 数组 |
| F4 | **internal_delete 完全缺少 PageIndex 更新** | `internal.rs` | 添加 item_count/first_item_off 更新 + write_back |
| F3 | **leaf_insert / internal_insert 中 pre_split 后 key 已存在未 write_back** | `leaf.rs`, `internal.rs` | 防止 pre_split 修改 page 后不 write_back 导致 page 字节与 cp array 不一致 |
| F2 | **leaf_push_back / internal_push_back total_delta `wrapping_add` → `checked_add` + panic** | `leaf.rs`, `internal.rs` | 防止 first_item_off 在负 delta 时 wrap 到非法地址 |
| F1 | **pre_split_segment 漏重写 k+1: 重编码 mid item 为 shared=0 后, 需用 `mid_full_key`(不是 mid-1) 还原并重编码 k+1** | `index.rs` | 修复 cp 段首 shared!=0 的根本原因 |

---

## 环境 gotchas (读这段能省 30 分钟)

1. **cargo 镜像被换成了 aliyun**, 原配置:
   ```
   ~/.cargo/config.toml 原:
   [source.crates-io]
   replace-with = 'aliyun'
   [source.aliyun]
   registry = "sparse+https://mirrors.aliyun.com/crates.io-index/"
   [http] proxy = "http://127.0.0.1:7897"
   [https] proxy = "http://127.0.0.1:7897"
   ```
   `mirrors.aliyun.com` 与代理 `127.0.0.1:7897` 都不可达. 当前已替换为最小配置:
   ```
   [net] git-fetch-with-cli = true
   ```
   原配置备份在 `~/.cargo/config.toml.bak`. 用前请确认你这边网络情况再决定要不要还原.

2. **Rust edition 是 2024**, 不是 2021. 别写成 `mod` 系统里不允许的形式 (允许的是稳定部分).

3. **monoio 版本实际拉到的是 0.2.4**. plan 里写的 `monoio = "0.2"` 解析到这个版本. 真实 API 与 plan 推测的 (`opcode::Read::new`, `ring.push_opcode`) **可能有差异**, 见 T7 step 4 的 fallback 块.

4. **io_uring 测试需串行执行**: 内核限制 io_uring 实例数, `real_world` 测试已合并为串行. 整体 scheduler 测试建议 `--test-threads=1`.

5. **`JoinInner` UnsafeCell 跨线程 race (已知)**: `tests/io_chain.rs::all_io_chain_scenarios` 被 `#[ignore]` (`--skip io_chain` 跳过). 根因: driver 线程 set_result 跨内存屏障写 state, 主线程 JoinHandle::poll 看不到更新 → 永久 hang. **NexusDB 模型自然不触发**: 每 shard 一个 Scheduler, run() 永久 loop, spawn/drive/poll 同线程. 详细契约见 `docs/superpowers/plans/2026-07-18-storage-crate.md` 顶部 "Scheduler 多线程使用契约" 段.

---

## 测试文件清单

### Page crate (`crates/page/tests/`)

| 文件 | 用途 |
|---|---|
| `tests/debug_repro.rs` | 可重现的 chaos replay + dump on failure |
| `tests/chaos_replay.rs` | 每步 PageIndex 校验的 chaos 重放 |
| `tests/chaos_363.rs` | 最小化 op 363 复现测试 |
| `tests/chaos_363_debug.rs` (即 internal_debug_102) | internal 的最小化复现 |
| `tests/dump_demo.rs` | dump 工具演示 |
| `tests/internal_tests.rs` | internal_insert / internal_delete / split |
| `tests/leaf_tests.rs` | leaf_insert / leaf_delete / split |
| `tests/split_boundary_tests.rs` | split 边界条件 |
| `tests/steal_tests.rs` | pre_merge steal 单元测试 |
| `tests/stress_tests.rs` | chaos 压力测试 |
| `tests/repro_split.rs` | split bug 复现 |
| `tests/complex_tests.rs` | 综合测试 |

### Storage crate (`crates/storage/tests/` + `src/`)

| 文件 | 用途 |
|---|---|
| 🆕 `tests/sanity.rs` (T1) | workspace + storage 编译占位 |
| 🆕 `tests/meta_cache_tests.rs` (T2/T12.4/T12.6) | MetaCache v1 sliding window + v2 LFU + db-aware |
| 🆕 `tests/alloc_tests.rs` (T3/T12.7/T12.8) | VpidAllocator/PidAllocator/FreePageQueue + per-db |
| 🆕 `tests/chunk_writer_tests.rs` (T4/T12.10) | NowChunks 触发条件 + WriteQueue 入队 + ChunkWriter 编排 flush + per-(db,file_id) paths |
| 🆕 `tests/chunk_lru_tests.rs` (T5/T12.9) | ChunkList LRU + per-db ChunkKey |
| 🆕 `tests/pager_round_trip.rs` (T6) | put → flush → reopen → get round_trip |
| 🆕 `tests/recover_tests.rs` (T7) | crash recover: 扫描 block 重建 alloc |
| 🆕 `tests/engine_e2e.rs` (T8) | StorageEngine put/get/close 重启 e2e |
| 🆕 `tests/meta_page_tests.rs` (T9) | MetaPage: empty/add/remove/flush/list/magic-position |
| 🆕 `tests/table_directory_tests.rs` (T10) | TableDirectory: create/get/drop/flush-persists |
| 🆕 `tests/registry_e2e.rs` (T11) | DbRegistry: 多 db 多表 create/put/get/drop/重启 |
| 🆕 `tests/chunk_lock_tests.rs` (T6) | chunk_lock: 同一 chunk FIFO / 不同 chunk 并行 / owner 唤醒 waiter / split+reader 协调 |
| 🆕 `tests/travel_tree_tests.rs` (T6) | travel_tree: record+lookup / split range_update / 多 task 广播 / root split 可见性 |
| 🆕 `tests/cow_tests.rs` (T6) | COW: 借用读零拷贝 (指针相等性) / take_page_for_write COW / 三源查找 nowchunks 优先 |
| 🆕 `tests/multi_page_sync_tests.rs` (T6) | multi_page_sync: leaf split 3 page 原子 / panic 中段 recover / 跨 chunk batch / 与 group commit 协同 |
| 🆕 `tests/catalog_consistency_tests.rs` (T9-T11) | catalog 一致性: MetaPage + TableDirectory 多 page 写回原子 / catalog 写回崩在中间 recover 一致 |

### Lib 单元测试 (`src/`)

| 模块 | 数量 | 备注 |
|---|---|---|
| `types` | 14 | 包含 T12.1+T12.2 新增 DbId / MetaKey / IoBackend 测试 |
| `meta_cache` | 4 | v2 LFU lib tests |
| `alloc` | 12 | T12.7+T12.8 新增 10 个 per-db 测试 |
| `chunk_writer` | 6 | T12.10 新增 3 个 per-db path 测试 |
| `chunk_lru` | 7 | T12.9 新增 5 个 per-db 测试 |
| `chunk_lock` | (内部用) | k helper |
| `meta_page` | (内部) | |
| `table_directory` | (内部) | |
| 其他 helpers | ~20 | |

### Network crate (`crates/network/tests/`)

| 文件 | 用途 |
|---|---|
| `tests/end_to_end.rs` | 端到端: put/get/delete roundtrip, 多请求单连接, 多连接并发 |
| `tests/integration_reply_bus.rs` | ReplyBus 集成: sink 收集, drain, 并发 put, bus sender |
| `tests/protocol_binary.rs` | BinaryProtocol 编解码: 正常/边界/错误 |
| `tests/reply_bus.rs` | ReplyBus 单元: pop/try_pop/drain/mpmc |
| 🆕 `tests/repro_verify.rs` (9 测试) | 高并发复现: phase 1+2+3 组合, 单线程/多线程/纯 put 对比 |
| 🆕 `tests/repro_verify_minimal.rs` (1 测试) | 最小化复现: 6 shard × 6 client × phase 1+2+3+4 |

### Storage crate 复现测试 (`crates/storage/tests/`)

| 文件 | 用途 |
|---|---|
| 🆕 `tests/repro_verify_storage.rs` (3 测试) | Storage 层独立复现: 排除网络层干扰, 直接模拟 phase 2+3+4 |

---

## 调试工具

```rust
use page::dump::dump_leaf_page_to_stderr;
// 或获取字符串:
let dump_text = page::dump::dump_leaf_page(&page);
eprintln!("{}", dump_text);

// Internal page:
use page::dump::dump_internal_page_to_stderr;
dump_internal_page_to_stderr(&page);
```

输出内容: Page Header(40B) → 完整 Items(含 offset/shared/full_key/value/child_vpid) → Checkpoint Array(含段首 key 还原). 出错时保留已解析部分 + 报错 + 原始字节.

---

## T12 ShardManager 推进进度 (本会话)

```
T12.1  types.rs 加 DbId + MetaKey                  ✅ DONE
T12.2  types.rs 加 IoBackend enum                   ✅ DONE
T12.3  验证现有 282 测试 (零回归)                   ✅ DONE
T12.4  MetaCache v2 (LFU + per-db mate) + 迁移      ✅ DONE (18→31 测试)
T12.5  MetaCache v2 lib 测试                        ✅ DONE (含 T12.4)
T12.6  MetaCache 加 DbId 维度                       ✅ DONE (31 测试)
T12.7  VpidAllocator per-db                         ✅ DONE (alloc 16→26)
T12.8  PidAllocator + FreePageQueue per-db          ✅ DONE
T12.9  ChunkList key 加 DbId                       ✅ DONE (chunk_lru 18→23)
T12.10 ChunkWriter per-(db, file_id) paths         ✅ DONE (chunk_writer 20→23)
T12.11 (留待 T12.16 polish)
T12.12 Pager::new + recover 加 block_root+shard_id  ⏸️ 后续会话
T12.13 recover 扫描 block_root/*/shard_N/*.block   ⏸️
T12.14 MetaPage 加 DbNameResolver                   ⏸️
T12.15 OpenOptions 加 block_root + shard_id        ⏸️
T12.16 StorageEngine 加 db 参数公共 API             ⏸️
T12.17 DbRegistry 真实多 db 物理路径                ⏸️
T12.18 多 db 物理隔离 e2e 测试 (新)                 ⏸️
T12.19 全量测试 + clippy/fmt                        ⏸️
T12.20 clippy/fmt 收尾                              ⏸️
T12.21 catalog_consistency_tests 重写              ⏸️
```

**完成标准 (T12 全部):**
- 所有 282 + 新增测试通过
- clippy 0 警告
- fmt 无差异
- 0 个回归 (功能等价, 仅内部改造)

**当前进度 (2026-07-20 收尾): 319 passed + clippy 0 + fmt 0, 已完成 8/21 子任务 (T12.1-T12.10).**

---

## 整体测试状态快照

### 2026-07-26 (F38-F41: 多协议门面 + 异步落盘 + 两个关键修复)

```
workspace 全量:            71 suites / 682 passed, 0 failed ✅
clippy:                     0 警告 ✅
新增测试:
  ─ network/value_codec:    5 (type tag roundtrip/容错)
  ─ network/protocol::resp: 8 (解析/分片/pipeline/编码)
  ─ network/resp_e2e:       6 (roundtrip/AUTH流程/pipeline顺序/超限/杂项)
  ─ storage/async_flush:    3 (去重/完成/失败重试/背压退化)
  ─ page/leaf_tests:       +1 (update_segment_head_keeps_shared_zero)
Benchmark (memtier, io_uring, 真实持久化):
  ─ 读混合 1:10 pipe16:   1.06M ops/s (前 298K)
  ─ 写重 1:1 pipe16:     153K ops/s, p99 16.7ms (前 44K / 40ms)
  ─ stress 10000×6:       verify 0/600 PASS
```

### 2026-07-24 (F32: Network crate + missing key 排查)

```
Page crate:                 131 passed ✅
Storage crate:              386+ passed ✅ (不含 repro_verify_storage 慢测试)
  ├─ sanity:                17 passed
  ├─ meta_cache:            31 passed
  ├─ alloc:                 26 passed
  ├─ chunk_writer:          23 passed
  ├─ chunk_lru:             23 passed
  ├─ pager_round_trip:      10 passed
  ├─ recover:                9 passed
  ├─ engine_e2e:            14 passed
  ├─ chunk_lock:            17 passed
  ├─ cow:                   19 passed
  ├─ travel_tree:           15 passed
  ├─ multi_page_sync:       13 passed
  ├─ meta_page:              5 passed
  ├─ table_directory:       14 passed
  ├─ registry_e2e:          16 passed
  ├─ catalog_consistency:   12 passed
  ├─ multi_db_physical:      9 passed
  ├─ multi_level_btree:      8 passed
  ├─ lib unit:              62+ passed
  └─ cli config:             1 passed
ShardManager crate:         28+ passed ✅ (15 lib + 5 e2e + 8 2pc e2e)
Network crate:              21 passed ✅ (3 end2end + 5 reply_bus int + 11 proto + 2 reply_bus unit)
  └─ repro tests:           13 tests (慢, 需 --release)
———————————————————————————————
总计:                       ~566+ passed, 0 failed
```

### 2026-07-20 (T12.10 完成)

```
Page crate:                 131 passed ✅
Storage crate (T1-T12):     319 passed ✅  (79 lib + 240 integration)
  ├─ sanity:                11 passed
  ├─ meta_cache:            31 passed  (T12.6 +13 LFU+DbId)
  ├─ alloc:                 26 passed  (🆕 T12.7+T12.8 +10 per-db)
  ├─ chunk_writer:          23 passed  (🆕 T12.10 +3 per-db paths)
  ├─ chunk_lru:             23 passed  (🆕 T12.9 +5 per-db)
  ├─ pager_round_trip:      10 passed
  ├─ recover:                9 passed
  ├─ engine_e2e:            14 passed
  ├─ chunk_lock:            17 passed
  ├─ cow:                   19 passed
  ├─ travel_tree:           15 passed
  ├─ multi_page_sync:       13 passed
  ├─ meta_page:              5 passed
  ├─ table_directory:       14 passed
  ├─ registry_e2e:          16 passed
  ├─ catalog_consistency:   12 passed
  ├─ lib unit:              62 passed  (含 F20: DbId / MetaKey / IoBackend)
  └─ cli config:             1 passed
Scheduler crate (T1-T11):   TBD
———————————————————————————————
总计:                       450 passed, 0 failed
```

### 2026-07-19 (T1-T11 完成 + clippy/fmt 收尾)

```
Page crate:                 131 passed ✅
Storage crate (T1-T11):     282 passed ✅  (56 lib + 226 integration)
  ├─ sanity:                11 passed
  ├─ meta_cache:            17 passed
  ├─ alloc:                 16 passed
  ├─ chunk_writer:          20 passed
  ├─ chunk_lru:             18 passed
  ├─ pager_round_trip:      10 passed
  ├─ recover:                9 passed
  ├─ engine_e2e:            14 passed
  ├─ chunk_lock:            17 passed
  ├─ cow:                   19 passed
  ├─ travel_tree:           15 passed
  ├─ multi_page_sync:       13 passed
  ├─ meta_page:              5 passed
  ├─ table_directory:       14 passed
  ├─ registry_e2e:          16 passed
  ├─ catalog_consistency:   12 passed (🆕)
  └─ lib unit:              56 passed
Scheduler crate (T1-T11):   TBD
———————————————————————————————
总计:                       413 passed, 0 failed
```

> **✅ Storage crate T1-T11 全部完成 + clippy/fmt 收尾:**
> ```
> 核心层 (T1-T8):           256 passed
> Catalog 层 (T9-T11):       35 passed (5 + 14 + 16)
> Catalog 一致性 (🆕):       12 passed
> clippy 警告:              0 (lib + 所有 tests)
> fmt 差异:                 0
> 修复:                     F18 修复 truncate(true) 清空文件导致数据丢失
> ```