# Ne
## 完整测试状态快照历史索引

7-24 / 7-20 / 7-19 三个旧快照完整保留于 `git log CHANGELOG.md` 任意历史版本; 与本快照差异仅在测试计数 (随会话累积), 测试文件清单同步见代码目录.
xusDB — Changelog & Hindsight

> 详细修复历史 + 测试进度快照 + 环境 gotchas + 测试文件清单.
> 本文件由 `AGENTS.md` 拆分而来 (2026-07-20), AGENTS.md 只保留项目入口与设计原则摘要.

**逆序时间线 (最新在上).**

---

## 2026-07-27 会话 (String 命令集 + 热路径优化 + ⭐GC 数据丢失修复)

### 修复总览

| # | 修复 | 文件 |
|---|------|------|
| F42 | ⭐ **GC 静默数据丢失**: compact 判活误杀 Internal 页 | `storage/src/pager.rs` (`analyze_compact_read`) |
| F43 | 热路径性能修复 9 项 (同机 A/B: 201K→239K, +19%) | `storage` (page_pool/btree/registry/chunk_writer), `network` (resp/worker/binary), `shard_manager` (request) |
| F44 | String 命令集: MGET/MSET + travel 区间复用 + RMW 命令 | `page/internal.rs`, `storage/btree.rs+registry.rs+engine.rs`, `shard_manager`, `network` |

### F42: ⭐ GC 静默数据丢失 (30s memtier 后早期 key GET 返回 nil)

- **现象**: 少量 key 写入 → 30s 高压写 (~4M key, 大量 compact/drain) → **运行期不重启**直接 GET 早期 key 返回 nil (不报错). `git stash` A/B 证实为既有 bug (非 String 改动引入)
- **根因**: `analyze_compact_read` 判活用 page header vpid 自描述 (`parse_page_vpid` + meta 点查); 但 **Internal 页的 header vpid 字段是 first_child** (page crate 路由约定) → 点查永远对不上 → Internal 页被误判死页: src 侧不搬运 (chunk 释放复用后物理销毁), dst 侧被当死槽覆盖 → 子树路由断, travel 在被覆盖位置撞到 Leaf 提前终止 → 在错误 leaf 找 key → nil
- **修复**: 判活以 **meta 平坦数组全扫为 SoT** (`iter_allocated` 过滤 pid ∈ src/dst chunk 且 PID_ALIVE), 一次遍历同时产出 src 活页表 + dst 死槽表, 零 header 依赖
- **证据**: `NLOG_GC_DEBUG=1` 排查日志 (保留为常备探针) 单轮 30s 压测捕获 **848 条** page_type=2 (Internal) 误判; 修复后原场景 a/b/c 全存活 + kill -9 reopen 完整
- **回归**: `compact_must_migrate_internal_pages` (构造 first_child≠自身 vpid 的 Internal 页驱动多轮 compact)
- **教训**: compact/GC 判活**禁止**依赖页头自描述 —— Internal 页 header vpid 语义被复用; meta 是唯一 SoT

### F43: 热路径性能修复 (审计 9 项, A/B +19%)

- **page_pool 闭环**: pager.read 的 Box 此前从不归还 (池空转, 每 read = malloc+memset+memcpy+free ×16KB) → travel/leaf/submit 消费端 recycle; alloc 免清零 (`new_uninit`)
- **travel_to_leaf_ro**: lookup/update/delete 免 TravelPath 每层 `key.to_vec()`, 且直接返回 leaf 字节省二次 read
- **table_put 单 travel**: `leaf_get_with` 借用窥视旧值 (只物化 13B 溢出描述符) + 原地 leaf_update, 从两次树遍历降为一次
- **BatchOp Arc<str>**: db/table 每 op 两次 String 分配 → 引用计数
- **Put.value 统一 `[TAG_RAW][payload]` 布局**: RESP/Binary decode 物化时预置 1B tag, 删 worker `encode_value` 整值二次拷贝 (1MB value 省 1MB memcpy)
- **解析游标化**: RESP/Binary 循环游标推进 + 末尾一次 drain, 消 pipeline O(n²) memmove
- 其余: write_page_with_vpid 借用传参 / DEL 免 clone 校验 / Binary GET to_vec 记录保留
- **长尾结论** (探针+负载阶梯): fsync 已从主循环消失 (flush 协程 avg 7.6μs), p99 = 饱和排队 (Little's Law 验证 in-flight/吞吐), 非调度病态

### F44: String 命令集 + travel 区间复用

- **区间 travel** (用户提议): `internal_child_with_bounds` 零成本带出左右 separator → `LeafGuide {lower, upper}` 逐层收窄 = leaf 覆盖区间; 批内排序 key `contains` 命中直接复用 leaf 免回 root (实测 500 顺序 key travel < 125 次)
- **MGET/MSET**: worker 按 key hash 分 shard 组 (`ShardTask.group` 回传聚合), shard 内 `table_get_many`/`table_put_many` 区间复用批量执行, 按原始顺序拼回复; MSET 同 key 重复后者覆盖 (稳定排序)
- **table_put_many 防泄漏**: 旧溢出链 leaf 提交成功后才释放 / 新链提交失败回滚; PageFull 退化单 key split 路径 (root 变化跟踪)
- **RMW 命令**: INCR/DECR/INCRBY/DECRBY/APPEND/SETNX shard 端执行 (单线程天然原子); EXISTS 多 key Get 聚合; STRLEN/TYPE Get 语义转换
- 大 value 溢出页 (F41 后续): `max_value_bytes` 3000→1MB, 13B 间接描述符 (0x00 标记与 value_codec tag 空间免冲突), PID_FREED 墓碑防 recover 复活, 覆盖写/删除全链路防泄漏
- 验证: 75 suites / 708 passed / 0 failed + clippy 0; redis-cli 全命令语义对齐; MSET(10 keys) 107-132K cmd/s ≈ 1.1-1.3M key/s

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

## 2026-07-22 会话 (T14: ShardManager 2PC + 同步 API) — 贡献摘要

**F31 T14 ShardManager 2PC**: 协议消息 `Prepare/Commit/Abort × {Db, Table}` + `TwoPhaseCoordinator` 状态机 (`coordinator.rs` ~330 LOC); `ShardManager::create_db/create_table` 走 2PC 流程; 15 lib + 8 e2e 全过. 同步 API 性能瓶颈 (主线程串行) 识别并交给 T15 async API. 关键测试: `two_pc_metadata_with_cross_shard_routing` (40 key 跨 4 shard) / `_persists_across_reopen` / `_duplicate_triggers_abort` / 另 5 个. 详见 git log F31.

---

## 2026-07-21 会话 (T17 全 async 重构 + ⭐T17b 64x→1x 写放大修复 + T15 多层 BTree + reopen) — 贡献摘要

- **⭐ F30 T17b: Pager::flush 写放大 64x→1x (本阶段最大亮点)**. 原 flush `disk_read(1MB) + merge + disk_write(1MB)` 相对 16KB page write 是 **64x 写放大**; 修复后直接写 nowchunks 1MB. 顺带实现 **vpid 复用** (in-nowchunk 原位覆盖同一 pid, `MetaCache::is_dirty` API). 设计文档 `docs/superpowers/plans/2026-07-18-storage-crate.md` §3.3.1. 8 e2e 多 vpid 行为更新.
- **F29 T17 全 async 重构**: `PagerIo` 抽象 (`StdFs` / `IoUring`); 入口方法全部 async 化; ⭐ **Stack Overflow 修复** — async fn 内联后 16KB 局部累积, 默认 8MB 线程栈不够, **测试用 `RUST_MIN_STACK=67108864` (64MB)** (`tests/common/mod.rs` 文档化). 测试 367→386 (+19).
- **F28 T15.1 chunk_offset 根因**: 文件内偏移误用全局偏移 → file 2 的 page 14 被写到 sparse 末尾, reopen 后 vpid 路由断. 改为 `chunk_idx * CHUNK_SIZE` (文件内偏移), scan_block_file 加 sparse 容错. 同会话完整修复 + T15 7/8 测试由 F28 补齐.
- 详见 git log F28/F29/F30.

---

## 2026-07-20 会话 (T12: ShardManager 集成 — T12.1-T12.21 全部完成) — 里程碑摘要

T12.1-T12.3: types.rs 加 `DbId` type alias + `MetaKey` 复合 key + `IoBackend` enum.
T12.4-F22/F21: ⭐ MetaCache v2 重写 — 抛弃 10MB sliding window, 改 per-shard LFU + BinaryHeap freq tracking + **修复** `evict_if_needed` 用 soft cap 作触发 (旧版 hard cap 漏掉 `soft < len < hard` 区间).
T12.6-F23: MetaCache 加 DbId 维度 + 13 新测试.
T12.7-T12.10-F24: VpidAllocator / PidAllocator / FreePageQueue / ChunkList-Key / ChunkWriter 全部加 DbId 维度 + 17 新单元测试.
T12.12+T12.13-F25: Pager/recover 走 `{block_root}/{db_name}/shard_N/` 路径, 三级 fallback (compat 直接走 block_dir).
T12.14-T12.17-F26: DbNameResolver (MetaPage 1024B 段) + StorageEngine `current_db` + ⭐ MetaPage COW 修复 (META_VPID 走固定 PID).
T12.18-T12.21-F27: ⭐ **关键 bug 修复** — `StorageEngine::open` 路径 tuple 第二项 `db_name` 被硬编码为 `DEFAULT_DB_NAME.to_string()`, 导致多 db 模式 recover 永远走 default 目录; 新增 `multi_db_physical_isolation.rs` 9 e2e.

**收尾口径**: 367 passed, clippy 0, fmt 0. **T12 全部 21 子任务完成**. 详见 git log F20-F27.

---

## 2026-07-19/18/17 会话 (Storage T1-T11 + Page F1-F12 早期) — 里程碑摘要

- **Page crate 早期 (F1-F12)**: LCB-Tree 页头 40B (`LCBP` magic + page_type + vpid @0x18); ItemKind/ItemPtr + PageIndex 段二分 + 段内 next + checkpoint 数组; Item prefix-compress (shared=0 哨兵 + varint len). 关键 bug 修复: F1 pre_split 漏重写 k+1 (段首 shared 错位) / F2 total_delta wrap → checked_add + panic / F3 pre_split 后未 write_back / F4 internal_delete 缺 PageIndex 更新 / F5 dump 调试模块 / F6 internal_push_back cp 边界 seg_idx 错位 / F7 split_delete chaos right_base 快照时机 / F8 多轮 split mid_off 边界偏移 / F11 internal_delete k+1 重写 + effective_seg_idx / F12 apply_pre_merge_steal 测试. 详见 git log F1-F12.
- **Storage T8-T11 (F13-F19)**: T7 recover (page header 自描述 + MetaCache union 语义); T8 NowChunks `vpid_map` + Pager::flush disk-in-memory merge; T9-T10/T11 **⭐ F17 aliasing UB 修复** — `TableDirectory` 移除 `*mut Pager` 字段 (改 `PhantomData<*mut Pager>` 保留 !Send/!Sync); **F18 `.truncate(true)` 导致数据丢失** — clippy auto-fix 给 4 处 `OpenOptions::new().create(true).truncate(true)` 在 reopen 已存在文件时清空, 全部改为 `.truncate(false)`; F19 catalog 一致性 12 个新测试. 详见 git log F13-F19.
- **T9-T11 catalog 设计 (Storage 关键设计决策)**: **MetaPage** (chunk 0 page 0, db_name→table_dir_root_vpid BTree) + **TableDirectory** (table_name→root_vpid 单 leaf BTree, 多 page 升级到多层) + **DbRegistry** write-through cache (HashMap 镜像, cache 永不超前 page.mate). 这三个是后续 T12-T14 多 db 物理隔离与 Network 多协议的数据基石.
- 测试贡献: Page 131 + Storage 282

## 历史索引 (近 4 段完整保留, 其余已全部压缩)

- 环境注意事项 (cargo 镜像 / Rust edition / io_uring 串行 / JoinInner 跨线程 race): 已被各具体段的 gotcha 内化, 无独立段
- 完整测试文件清单 (`crates/{page,storage,network,shard_manager}/tests/` 全文件目录): 跟随 `git log CHANGELOG.md` 取任意历史版本可查, 内容与代码目录同步变化
- 全部 F 编号 (F1-F44) / T 编号 (T1-T17b + T12.1-T12.21) 哈希检索锚保留在本快照中, 不会丢失 (56 lib + 226 integration). 完整实现细节见 `docs/superpowers/plans/2026-07-17-page-item-revision.md` + `docs/superpowers/plans/2026-07-18-storage-crate.md`.

| F12 | **新增 `apply_pre_merge_steal` 4 个单元测试** | `tests/steal_tests.rs` | 覆盖: steal 触发 (left 达 MIN) / left>=MIN 不触发 / 无右邻不触发 / right 太小不触发 |
| F1 | **pre_split_segment 漏重写 k+1: 重编码 mid item 为 shared=0 后, 需用 `mid_full_key`(不是 mid-1) 还原并重编码 k+1** | `index.rs` | 修复 cp 段首 shared!=0 的根本原因 |

---

## 整体测试状态快照

### 2026-07-26 (F38-F41: 多协议门面 + 异步落盘 + 两个关键修复)

```
workspace 全量:            71 suites / 682 passed, 0 failed ✅
clippy:                     0 警告 ✅
新增测试覆盖: 5+8+6 (network) + 3+1 (storage/page) = 23 个 F38-F41 相关测试
Benchmark (memtier, io_uring): 读混合 1.06M ops/s, 写重 153K ops/s p99 16.7ms, stress 10000×6 verify 0/600 PASS
```
(早期 1.06M/153K 数据为 F38-F41 当基线, 已由后续 F42-F44 替代到 240-310K 和 1.1-1.3M key/s. 早期 T12 文字同理, 此处仅保留 7-26 最新基准)

