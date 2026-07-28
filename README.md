# `NexusDB`

> 面向写密集/低延迟的单机数据库 —— Share-Nothing + per-core thread + io_uring + 自实现协程调度器, 多协议统一接入 (Redis 兼容 ✅, PG/MySQL/Mongo 设计路线中).

[![Linux](https://img.shields.io/badge/OS-Linux-blue)]() [![Rust](https://img.shields.io/badge/Rust-2024-orange)]() [![Tests](https://img.shields.io/badge/tests-700%20passed-success)]() [![Clippy](https://img.shields.io/badge/clippy-0%20warnings-success)]() [![License](https://img.shields.io/badge/license-MIT-lightgrey)]()

> 设计架构见 [DESIGN.md](./DESIGN.md); 接手 / 进度 见 [AGENTS.md](./AGENTS.md); 修复历史 见 [CHANGELOG.md](./CHANGELOG.md).

---

**[核心特性](#核心特性) · [快速开始](#快速开始) · [性能](#性能) · [架构总览](#架构总览) · [GC 与空间回收](#gc-与空间回收) · [大 value 溢出页](#大-value-溢出页) · [支持的协议](#支持的协议) · [配置](#配置) · [开发命令](#开发命令) · [故障排查](#故障排查)**

---

## 核心特性

**协议层**
- **双协议监听**: Binary 自研协议 5433 端口 + RESP2 (Redis 兼容) 6379 端口, 均含 `KvLimits` 协议层长度拦截, RESP 层开启 `TCP_NODELAY`
- **统一记录编码 + value type tag**: 网络门面写入统一附加 tag, storage 层不感知, 新协议接入零存储改动
- **多 db / 多表物理隔离**: `{block_root}/{db_name}/shard_{N}/` 目录; db 切换走 MetaPage vpid 0 索引

**IO 与调度**
- **io_uring 落盘**: StdFs 后端 fallback, 生产路径走 `scheduler::io_ops` 直提交 SQE
- **T18 零拷贝**: `IOSQE_FIXED_FILE` + `RegisteredBufPool` 双优化, 移除 SQE/FdPool 上的热路径 memcpy
- **自实现协程调度**: `crates/scheduler` 单线程 park/unpark + 优先级分区 (`spawn_on_low` 给 GC/drain 让出前台 wave)
- **异步落盘 + 有界背压**: `FlushBatch`/`MetaFlushBatch` 按 file 分组, write ×N + fsync ×1; `MAX_INFLIGHT_CHUNKS=8` 超限退化同步; 主循环零阻塞 fsync

**存储引擎**
- **NowChunks 数组化 + 纯 COW**: 无 dirty 标记, 驻留即待写; 满 chunk swap 后同 chunk 内 vpid 走 alloc 新 pid
- **全量平坦 meta 缓存**: `meta : data = 1 : 2048` (1TB 数据 ≈ 512MB meta/shard-db), open 整读 + 1MB dirty window 异步刷盘
- **数据→meta→pid.state 刷盘顺序不变量**: meta window 仅在 data backlog 排空后入队, pid.state 仅在 meta window 确认后写

**空间与回收**
- **GC compact (chunk 死槽填充 + 主动 block drain)**: 原位填充, 不开新 chunk; 半空 block 被逐轮排空并 unlink
- **PID_FREED 墓碑 + 防复活**: 大 value 覆盖写释放旧链写墓碑, recover 不回填死页
- **大 value 溢出页 (≤ 1MB)**: 超 inline 阈值 (~4KB) 自动切成 16KB 溢出页 + 13B 描述符, 0 拷贝到现有 GC

**质量**
- **700+ 单元/集成测试, `cargo clippy --all-targets` 0 警告**

---

## 快速开始

```bash
git clone <repo-url> && cd NexusDB
cargo build --release --workspace        # 首次 ~2min
cp nexusdb.toml /tmp/nexus.toml          # 按需修改 listen_addr / block_root
./target/release/NexusDB --config /tmp/nexus.toml
```

另一终端验证 (Redis 兼容):

```bash
redis-cli -p 6379 PING                          # PONG
redis-cli -p 6379 SET hello world
redis-cli -p 6379 GET hello                     # "world"
redis-cli -p 6379 MGET hello nosuchkey          # 1) "world"  2) (nil)

# 大 value 自动溢出页 (>4KB)
redis-cli -p 6379 -x SET bigkey < some_100k_blob
redis-cli -p 6379 GET bigkey | head -c 102400 | md5sum   # 逐字节一致
```

Binary 协议端口 (5433) 测试用例见 [`crates/network/tests/end_to_end.rs`](./crates/network/tests/end_to_end.rs).

5 分钟自检脚本:

```bash
cargo test --workspace --no-fail-fast    # ~30s, 0 failed 预期
```

---

## 性能

**测试硬件**: Linux 7.0 / AMD Ryzen AI 9 H 365 / 32 GB RAM / io_uring + NVMe SSD / 本机 loopback.
不同硬件/内核/IO backend 测值会有差异, 表中为本机当下快照, 不可作为唯一采购依据.

### 表 1 — 小 value 写密集基线

`memtier_benchmark --ratio=1:1 --pipeline=16 --threads=4 --clients=8 --data-size=64 --test-time=30`

| 指标 | 当前 (String 命令集 + 热路径优化后) | 热路径优化前 (同机 A/B) | GC 启用前 |
|---|---|---|---|
| 吞吐 | **240-310K ops/s** (运行间波动) | 201K | 198K |
| p50 | **1.8-2.0ms** | 2.46ms | 2.5ms |
| p99 | **3.4-4.7ms** | 5.34ms | 5.4ms |
| MSET (10 keys, redis-benchmark -P 16) | **107-132K cmd/s ≈ 1.1-1.3M key/s** | - | - |

### 表 1b — 多场景快照 (2026-07-28, 五大结构落地后; 4线程×25连接 pipeline=16)

| 场景 | 吞吐 | 备注 |
|---|---|---|
| GET (预热后) | **395K ops/s** | 读路径三源查找 |
| SET pipeline=1 | 174K, **p50 0.51ms** | 单请求延迟视角 |
| 混合 1:9 (读多) | 315K | |
| 混合 1:1 | 249K | 五大结构接入后 String 热路径无回退 |
| HSET/SADD/ZADD/LPUSH (百万 key 分散) | 44-97K | 复合 op 多次 BTree 遍历 (类型检查+meta 维护), 优化方向已识别 |

### 表 2 — 热路径延迟分布 (`NLOG_PROBE=1` 启动, SIGTERM dump)

| 阶段 | 1-10μs 桶占比 | 2-5ms 桶占比 | 说明 |
|---|---|---|---|
| `flush_coroutine_total_ns` | 主导 | ≈ 0% | fsync 全部从主循环搬出 |
| `drive_async_flush_round_ns` | 主导 | ≈ 0% | shard 事件循环零阻塞 |
| `drive_until_idle_ns` | 主导 | ≈ 0% | 协程调度低开销 |
| `block_on_io_ns` | 主导 | ≈ 0% | 同步 IO 路径未被触发 |
| `backpressure_sync_write_ns` | 主导 | ≈ 0% | 背压未退化 (in_flight_peak < 8) |

### 表 3 — 大 value (memtier `--data-size=65536 --pipeline=4 --threads=2 --clients=4 --test-time=20`)

| 指标 | 数值 |
|---|---|
| 吞吐 | **31K ops/s (~2 GB/s 写带宽)** |
| p50 | **0.74ms** |
| p99 | **5.15ms** |
| 单 key 上限 | 1 MB (64+1 = 65 溢出页) |

### 表 4 — 崩溃恢复与 GC 空间观测

| 场景 | 结果 |
|---|---|
| 周期刷盘后 `kill -9` → reopen | 数据完整; pid.state 快速路径跳过 header 扫描 |
| 20 × 覆盖写 512 KB → reopen | 活页数与上轮持平 (防复活 + 防泄漏) |
| 半空 block → drain 排空 | block 文件 unlink, data 目录稳定 |
| 1MB × 20 SET → reopen | data 不发散, du ≈ 17 MB / 6 block |

### 调优指南

| 参数 | 建议 |
|---|---|
| `io_backend` | `io_uring` (NVMe 上 +30-50% vs `stdfs`); 容器/沙箱无支持改 `stdfs` |
| `chunk_cache_size` | 默认 16 (16 MB hot cache); GC 后**一般无需调大**——死页已 unlink, 热集自然缩小 |
| `num_shards` | ≤ CPU 物理核数; 多于核心数会导致线程跨核调度毛刺 |
| `max_key_bytes` | 维持 1024 B (key 参与 internal page 路由) |
| `max_value_bytes` | 默认 1 MB; 单 value ≤ 4 KB 自动 inline, 否则溢出页链 |

**调优方法**: `NLOG_PROBE=1` 压几圈 → 看 `flush_coroutine_total_ns` / `drive_async_flush_round_ns` 直方图定位最长埋脚 → 改参 → 复跑 memtier 对比. 调优前后必跑 `cargo test --workspace` 保不退步.

---

## 架构总览

**Share-Nothing = 每个 shard 一个 OS 线程独占所有数据结构 + 一个 io_uring 实例 + 一个 Scheduler 实例.** 跨 shard 仅走 mpsc / Inbox / TaskReplyBus, 无锁无 race.

```text
  Binary 5433 (TCP) ─┐
                     ├── NetworkServer ── KvLimits 协议校验 ── shard_manager::Router
  RESP  6379 (TCP) ─┘                                                  │
                                                                         ▼
                  shard_n thread (per-core, 单线程事件循环)
                                                                         │
                +--------+-----------+-------------------+----------------+
                ▼        ▼           ▼                   ▼                ▼
            LCB-Tree   NowChunks   WriteQueue         ChunkList       MetaCache
             (page)   (活跃 chunk) (in-flight 8)     (LRU 1MB×16)    (平坦 Vec + dirty window)
                │        │           │                   │                │
                +--------+-----+-----+-------------------+----------------+
                                  ▼
                          pager_io (io_uring / StdFs)
                                  ▼
                          .block + page.mate (per-db-per-shard)

          ┌──────┐   ┌──────┐
          │ GC   │   │      │   ChunkLiveness (纯内存活页计数)
          │协程  │◄─►│ 后台 │   Low-Priority 调度（spawn_on_low）
          └──────┘   └──────┘   Compact / Drain / 墓碑回收
```

关键设计点:

- **chunk = 1 MB = 64 page × 16 KB**; NowChunks 索引 = `chunk_idx` 懒扩容, 无 dirty 标记 — 驻留即待写
- **meta = `Vec<PidLocation>`, 索引 = vpid**; open 整读 page.mate 进内存 (无 pread miss); flush 按 1 MB window 走协程异步
- **异步落盘零阻塞主循环**: `complete_flush` 只置 `due=true`, fsync 由协程执行; 收割路径 ≈ 1.5μs
- **顺序不变量**: data chunk 写盘确认 → meta window 写盘 → pid.state 三段闭环, 任何一段失败均可重试
- **GC 与大 value**: 见下两节

完整分节: [DESIGN.md](./DESIGN.md) (10 节).

### 平台依赖

- **OS**: Linux (glibc / musl); io_uring 后端要求内核 ≥ 5.6 (推荐 ≥ 5.15)
- **栈大小**: `RUST_MIN_STACK=8388608` (8 MB), 默认栈 IO 密集调用会深递归
- **磁盘**: NVMe SSD 提供 fsync ≤ 100μs; SATA SSD ≈ 1ms
- **io_uring capability**: `cat /proc/sys/kernel/io_uring_disabled` 应 = 0

---

## GC 与空间回收

### 活性计数 (纯内存, 重启反推)

vpid **永不回收**, 但底层 chunk/block 物理空间会被 GC 收回:
- `ChunkLiveness::live[]`: 每 chunk 1B 活页计数 (0..64), `Vec<u16> block_active[]` 聚合到 file
- **写路径推进**: COW alloc / delete / compact 增减; chunk 活页归零 → `pending_free`
- **重启反推**: `rebuild_from_meta` 遍历全量平坦 meta 重建 (vpid 数组红利, 几十 ms 完成)

### chunk compact (死槽填充)

```text
victim B → dead slot in A → fill A's empty 16KB pages → fsync
                                       ↓
                  meta CAS (vpid == B.pid ? → A.pid)  → meta fsync  → promote(B)
```

- **原地**: 不开新 chunk; A 的活页一字不动, 整 1MB 重写会损己活页 (不做)
- **CAS 提交**: 防并发 COW 写回滚已搬页
- **延迟释放**: B 进 `pending_free`, meta window 确认后才 → `free_chunks` (避免旧位置未读前被复用)
- **低优先级协程**: 通过 `spawn_on_low` 跑在 wave 末尾, 每 wave 限额 1 poll, 不影响前台

### block drain (主动排空半空 block)

- chunk compact 完成后**扫候选**: 找 `0 < block_active ≤ 3` 的半空 block (全空走 unlink 路径)
- **状态机分片**: `drain_block_target` 记录目标, 每轮只迁一个 chunk (复用 chunk compact 三阶段管道)
- **fresh bump dst**: 无可用死槽时开全新 chunk 作宿主, 一次整 1MB 写回 (常规 dst 仍是死槽批写)
- **完成 = 全死**: meta 确认点触发 `maybe_drop_free_blocks`, 逐出 fd_cache + FdPool 固定槽 → unlink

### recover 主源切换

- **page.mate 为主**: vpid→pid 映射以 meta 为 SoT
- **扫描仅补缺**: .block 扫描发现 meta 缺失的 vpid 才回填 (meta 墓碑 / 已记录 vpid 不动 — 否则磁盘残留旧页 header 会**复活**死页)
- **pid.state 快速路径**: 上次 flush 持久化的 8B `PidLocation`, 与扫描取较大值, 落后安全

实现: [`crates/storage/src/chunk_liveness.rs`](./crates/storage/src/chunk_liveness.rs) · [`crates/scheduler/src/scheduler.rs:spawn_on_low`](./crates/scheduler/src/scheduler.rs) · [`crates/storage/src/pager.rs:start_compact`](./crates/storage/src/pager.rs)

---

## 大 value 溢出页

### 数据格式

```text
leaf item value:
  inline:   [原始字节]                                  (首字节 != 0x00)
  indirect: [0x00][head_vpid u64 LE][total_len u32 LE]  (13B 描述符)

OverflowIndex 页 (head_vpid):
  [0..0x28]   标准页头 (page_type = 5)
  [0x28..0x2A] count u16 LE
  [0x2A.. ]   count × vpid u64 LE

Overflow 数据页:
  [0..0x28]   标准页头 (page_type = 4)
  [0x28.. ]   payload 切片 (末页截断)
```

### 设计要点

| 项 | 决定 | 理由 |
|---|---|---|
| 间接标记 | **0x00** (首字节) | value_codec tag 0x01+ 永不冲突, 存量数据零迁移 |
| 阈值 | `key_len + value_len > 4000` | 与 page item 4096 缓冲对齐 |
| 单层间接 | 1 index 页 + 64 数据页 ≈ 1 MB | 单寻址 inline-buf 富余, 多层间接流式预留 |
| 标准页头 | 溢出页带完整 LCBP header | recover / compact 零改动兼容 (按 vpid+page_type 识别) |

### 防泄漏不变量 (修改场景核心)

- **覆盖写成功 → 释放旧链**: 旧值是描述符则 `free_overflow` 逐页 `Pager::free_overflow_vpid` (活性递减 → chunk/block GC 收回)
- **新链已写但 leaf 提交失败 → 回滚释放新链**: 错误路径不留孤儿
- **PID_FREED 墓碑**: 释放**不是清零 slot**, 而是写 `PID_FREED` 墓碑并随 dirty window 持久化
- **recover 不回填墓碑**: `has_record` 判据代替 `peek`, 磁盘残留旧页 header 不会复活死页

实现: [`crates/storage/src/overflow.rs`](./crates/storage/src/overflow.rs) · [`crates/storage/src/pager.rs:free_overflow_vpid`](./crates/storage/src/pager.rs) · [`crates/storage/src/meta_cache.rs:free_slot / has_record`](./crates/storage/src/meta_cache.rs)

---

## 支持的协议

| 协议 | 端口 | 状态 | 说明 |
|---|---|---|---|
| RESP2 (Redis 兼容) | 6379 | ✅ 完整 | **五大数据结构 + Geo + Bitmap** 全命令面, 清单见下表; 大 value 溢出页自动走 |
| Binary (自研) | 5433 | ✅ 完整 | Request/Response + BatchOp (Put/Get/Delete 同 key) + TravelTree; 多客户端 + ReplyBus |
| PostgreSQL (wire) | - | 🚧 设计路线 | 见 [DESIGN.md §10](./DESIGN.md) |
| MySQL (wire) | - | 🚧 设计路线 | 同上 |
| MongoDB (BSON) | - | 🚧 设计路线 | 同上 |

### RESP 命令面 (2026-07-28)

| 结构 | 命令 |
|---|---|
| String | SET/GET/DEL/EXISTS/STRLEN/TYPE, MGET/MSET/MSETNX (跨 shard 分组聚合 + leaf 区间复用), INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT/APPEND/SETNX (shard 端原子 RMW), GETRANGE/SETRANGE/GETDEL/GETSET |
| Hash | HSET/HMSET/HSETNX/HGET/HMGET/HDEL/HEXISTS/HLEN/HGETALL/HKEYS/HVALS/HSCAN/HINCRBY/HINCRBYFLOAT/HSTRLEN/HRANDFIELD |
| Set | SADD/SREM/SISMEMBER/SMISMEMBER/SCARD/SMEMBERS/SSCAN/SPOP/SRANDMEMBER (含 count), SINTER/SUNION/SDIFF/SINTERCARD/SINTERSTORE/SUNIONSTORE/SDIFFSTORE |
| List | LPUSH/RPUSH/LPOP/RPOP (含 count)/LLEN/LRANGE/LINDEX/LSET, LREM/LTRIM/LPOS/LINSERT (中段操作) |
| ZSet | ZADD/ZREM/ZSCORE/ZMSCORE/ZCARD/ZCOUNT/ZINCRBY, ZRANGE/ZREVRANGE/ZRANGEBYSCORE/ZRANK/ZREVRANK (双索引), ZPOPMIN/ZPOPMAX, ZINTERSTORE/ZUNIONSTORE (SUM, 无 weights) |
| Geo | GEOADD/GEOPOS/GEODIST/GEOSEARCH (FROMLONLAT+BYRADIUS; 52-bit geohash 复用 ZSet 双索引) |
| Bitmap | SETBIT/GETBIT/BITCOUNT/BITPOS (BYTE 粒度; 复用 String 字节) |
| 连接 | PING/ECHO/AUTH/QUIT/HELLO/SELECT/COMMAND (pipeline FIFO 重排, TCP_NODELAY) |

> 未支持 (记录在案): TTL 族 (EXPIRE/SET EX·PX·NX·XX)、跨 key 原子 (BITOP/SMOVE/LMOVE/BLPOP)、ZSTORE 的 WEIGHTS/AGGREGATE、Stream、HyperLogLog; MSETNX/Set 代数/*STORE 跨 shard 非原子.

设计哲学: **统一记录编码 + value type tag** 已预留 (`TAG_RAW/TAG_I64/TAG_F64/TAG_STR/TAG_DOC`), 新协议接入**无需改 storage 层**, 只需添加 `crates/network/src/protocol/<x>/` parser + adapter.

---

## 配置

完整字段注释见 [`nexusdb.toml`](./nexusdb.toml). 重点 section:

```toml
[server]
listen_addr = "0.0.0.0:5433"     # Binary 协议
redis_addr = "0.0.0.0:6379"      # RESP (空字符串 = 禁用)
redis_password = ""              # AUTH 密码
max_key_bytes = 1024             # key 上限 (协议层拦截)
max_value_bytes = 1048576        # value 上限 (>4KB 自动走溢出页)

[storage]
block_root = "./data"
num_shards = 6                   # 跨 shard 仅 hash(key), 无锁
io_backend = "io_uring"          # "stdfs" | "io_uring"
chunk_cache_size = 16            # ChunkList LRU 容量
create_if_missing = true
default_db = "default"           # 启动默认 db
default_table = "default"        # 启动默认 table

[log]
level = "info"                   # error|warn|info|debug|trace
dir = "./logs"                   # 空 = 仅 stderr
buffer_kb = 64
flush_interval_ms = 500
stderr = true
```

`max_value_bytes` 默认从 3 KB 提到 **1 MB** (1 MiB + 64 B 余量, 留 tag 字节空间); `max_key_bytes` 维持 1024 B (key 参与 internal page 路由, 不走溢出).

---

## crate 职责

| crate | 职责 | 状态 |
|---|---|---|
| [`crates/scheduler`](./crates/scheduler) | 单线程协程调度器 + io_uring 桥 (`SchedHandle`/`drive_until_idle`/`io_ops`/`FdPool`/`spawn_on_low`) | ✅ |
| [`crates/page`](./crates/page) | LCB-Tree 页 (leaf/insert/split/delete/checkpoint/前缀压缩/`ItemKind` 编码) | ✅ |
| [`crates/storage`](./crates/storage) | 物理持久化层: `Pager`/`MetaCache` v3 全量平坦 + dirty window/`NowChunks` 数组化/`ChunkList` LRU/`ChunkLiveness` + GC`/overflow` 大值溢出/`recover` 主源 + 墓碑防复活 | ✅ |
| [`crates/network`](./crates/network) | 双协议门面 (`acceptor` + `epoll worker` + RESP2 + Binary + `KvLimits` + `value_codec`) | ✅ |
| [`crates/shard_manager`](./crates/shard_manager) | 多 shard 控制器 (`ShardManager`/`Router`/`Inbox`/`TaskReplyBus`) + `latency_probe` 探针 + stress 基准 | ✅ |
| [`crates/config`](./crates/config) | TOML 配置加载 | ✅ |
| 根 `src/main.rs` | 服务器入口: `nexusdb --config nexusdb.toml`, 信号优雅退出 | ✅ |

各 crate 实施细节: [`docs/plans/`](./docs/plans/) (plan 索引见各文件头部).

---

## 开发命令

```bash
# 全量回归 (700+ 测试, ~30s)
cargo test --workspace --no-fail-fast

# clippy (0 警告硬约束)
cargo clippy --workspace --all-targets

# release 构建 (性能测试前必须)
cargo build --release

# 启动 (生产)
RUST_MIN_STACK=8388608 ./target/release/NexusDB --config nexusdb.toml

# 启动 + 探针 (性能调优, 直方图 dump 到 stderr, SIGTERM 时)
NLOG_PROBE=1 ./target/release/NexusDB --config nexusdb.toml

# 单 crate 测 (开发时快速迭代)
cargo test -p storage --lib
cargo test -p shard_manager --lib

# 单 testcase 跑 (调试)
cargo test -p storage --test recover_tests -- --exact some_test_name

# 大 value e2e (RESP)
redis-cli -p 6379 -x SET bigkey < /dev/urandom   # 1024B..1MB 自动溢出
redis-cli -p 6379 GET bigkey                     # 字节一致回
```

调试技巧与 gotchas 见 [AGENTS.md](./AGENTS.md).

---

## 故障排查

| 现象 | 可能原因 / 处置 |
|---|---|
| 启动报 `permission denied` / `disk full` | `block_root` 路径权限 / 磁盘空间; 检查 [nexusdb.toml](./nexusdb.toml) `[storage].block_root` |
| 启动 hang 在 io_uring 初始化 | 容器 / 沙箱无 io_uring 支持; 改 `io_backend = "stdfs"` 临时规避 |
| `RST_STREAM` 长尾突增 | 网络层 TCP_NODELAY 注意事项; 见 [AGENTS.md](./AGENTS.md) |
| p99 突刺 ~ ms 级 | 多为磁盘 fsync 排队; 切换 NVMe / `NLOG_PROBE=1` 拿探针对照 |
| 大 value GET 拿到 `ERR ... value too long` | payload 超过 `max_value_bytes` (默认 1 MB); 检查 [nexusdb.toml](./nexusdb.toml) 或 `client->server` 中 |
| p99 从 3 ms 跳到 6 ms | 多为 in-flight 8 触顶退化同步写; 降 `[storage].num_shards` 或升 SSD |
| 数据读不到 | 多 db 切换: 确认 SET 时使用的 db 名 (`SELECT dbname`); 默认 db 始终有效 |

### 已知 gap (DESIGN/AGENTS 中已记录)

- **vpid 回收**: vpid 不回收, 大量删除工作负载下 `Vec<PidLocation>` 按最大 vpid 占内存
- **per-db per-mate**: 当前全 db 共用单 mate 文件 (off = `vpid*8`); 多 db + 大 vpid 场景可拆
- **PG / MySQL / MongoDB 协议**: DESIGN §10 roadmap; 统一记录编码与 value tag 已就绪
- **Range scan / cursor**: List/ZSet/Stream 依赖项, 计划下阶段
- **Transaction / MVCC**: 单线程 Pager 串行天然无并发, 现不紧急

### 调试探针

`NLOG_PROBE=1` 启动 → SIGTERM 时 16 桶直方图 dump 到 stderr:

- `flush_coroutine_total_ns` — 单个落盘协程总耗时 (write + fsync)
- `drive_async_flush_round_ns` / `drive_until_idle_ns` — shard 事件循环阶段
- `block_on_io_ns` / `poll_wait_ns` — 同步等待 / poll 唤醒
- `backpressure_sync_write_ns` — 背压退化同步写 (≈ 0 表示未触发)
- `in_flight_peak` — 异步落盘深度峰值

---

## 文档索引

| 读者 | 文档 |
|---|---|
| 评估 / 第一天 | 本 README |
| 架构理解 | [DESIGN.md](./DESIGN.md) (10 节) |
| 接手开发 (进度 / gotchas / 待办) | [AGENTS.md](./AGENTS.md) |
| 修复历史 (F1-F…) | [CHANGELOG.md](./CHANGELOG.md) |
| 各 crate 分阶段实施 plan | [`docs/plans/`](./docs/plans/), [`docs/specs/`](./docs/specs/) |
| Bug 根因调查示例 | [`docs/bug-report-btree-split-routing.md`](./docs/bug-report-btree-split-routing.md) |

---

## 许可证

NexusDB 源码采用 [LICENSE](./LICENSE) (见仓库根).

致谢: 协议层借鉴 [monoio](https://github.com/bytedance/monoio) / `tokio` io_uring 实验分支; 性能基线对比参照 [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark).
