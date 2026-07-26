# NexusDB

> 面向写密集/低延迟的单机数据库 — Share-Nothing + per-core thread + io_uring + 自实现协程调度器, 多协议统一接入 (Redis 兼容 ✅, PostgreSQL/MySQL/MongoDB 在设计路线中).

> 想了解架构深读请看 [DESIGN.md](./DESIGN.md); 接手开发请看 [AGENTS.md](./AGENTS.md); 修复历史看 [CHANGELOG.md](./CHANGELOG.md).

---

## 核心特性

- **双协议监听**: Binary 协议 5433 端口 + RESP2 (Redis 兼容) 6379 端口, 均含 KvLimits 协议层长度拦截
- **io_uring 落盘**: StdFs 后端 fallback, 生产路径走 `scheduler::io_ops` 直提交 SQE
- **T18 零拷贝**: `IOSQE_FIXED_FILE` + `RegisteredBufPool` 双优化, 移除 SQE/FdPool 上的热路径拷贝
- **自实现协程调度**: `crates/scheduler` 提供 spawn_on / drive_until_idle, 全部 IO 在同一 Scheduler 线程 park/unpark
- **LSM 写缓冲**: `NowChunks` 数组化 (无 dirty 标记, 驻留即待写) + 纯 COW (满 chunk swap 后同 chunk 内 page 走 alloc 新 pid) + 按 file 批量 fsync
- **全量平坦 meta 缓存 + 1MB dirty window**: `meta:data = 1:2048` (1TB ≈ 512MB meta/shard-db), window 粒度标脏, 异步刷盘 move 到 io_uring 协程
- **多 db / 多表物理隔离**: `{block_root}/{db_name}/shard_{N}/` 目录; db 切换走 MetaPage vpid 0 索引
- **崩溃恢复**: 启动 `scan_block_files` 从 page header 提取 vpid union 填 MetaCache; `pid.state` 快速路径 (8B `PidLocation` 跳过扫描, 落后安全)
- **异步落盘 + 有界背压**: `FlushBatch`/`MetaFlushBatch` 按 file 分组, write ×N + fsync ×1; `MAX_INFLIGHT_CHUNKS=8` 超限退化同步; 主循环零阻塞 fsync
- **数据→meta→pid.state 刷盘顺序不变量**: meta window 仅在 data backlog 排空后入队, pid.state 仅在 meta window 确认后写
- **682+ 单元/集成测试, `cargo clippy --all-targets` 0 警告**

---

## 快速开始

```bash
git clone <repo-url> && cd NexusDB
cargo build --release --workspace        # ~2min 首次构建
cp nexusdb.toml /tmp/nexus.toml          # 按需修改 listen_addr / redis_addr / block_root
./target/release/NexusDB --config /tmp/nexus.toml
```

另一终端验证 (Redis 兼容):

```bash
redis-cli -p 6379 PING                            # PONG
redis-cli -p 6379 SET hello world                 # +OK
redis-cli -p 6379 GET hello                       # "world"
redis-cli -p 6379 MGET hello nosuchkey            # 1) "world" 2) (nil)
redis-cli -p 6379 -a yourpassword AUTH            # AUTH 启用时
```

Binary 协议端口 (5433) 验证 (需要自研客户端; 详见 [`crates/network/src/protocol/`](./crates/network/src/protocol/)):

```bash
# 监听握手 / 二进制帧格式见 crates/network/tests/end_to_end.rs
ncat 127.0.0.1 5433                # 查看初始握手
```

5 分钟自检脚本:

```bash
cargo test --workspace --no-fail-fast    # 全量回归 (~30s, 0 failed 预期)
---

## 性能

`memtier_benchmark 1:1 SET/GET pipeline=16 threads=4 clients=8 --data-size=64 --test-time=30` (消费级 SSD + 本机网络):

| 指标 | 数值 | 备注 |
|---|---|---|
| 吞吐 | **198K ops/s** | |
| p50 延迟 | **2.5ms** | |
| p99 延迟 | **5.4ms** | |
| p99.9 延迟 | **7.2ms** | |
| 收割路径 (异步 meta+chunk fsync) | **avg 1.77μs** | `flush_coroutine_total_ns` 探针 |
| 2-5ms 段占比 | **0%** | 上轮改造前 76%, 已搬出主循环 |
| 重启持久化 | **300 write → `kill -9` → reopen 数据完整** | 扫描 union 兜底 |

> 数字与硬件强相关 (NVMe/SATA SSD, page cache, kernel 版本); 复现命令与解析见 [`crates/shard_manager/examples/stress.rs`](./crates/shard_manager/examples/stress.rs).

内存估值: **1TB 数据 ≈ 512MB meta** (单 shard-db; 全量平坦 `Vec<PidLocation>`). 多 shard 平均分摊. 详见 [DESIGN.md §3.5](./DESIGN.md).

延迟探针: `NLOG_PROBE=1 ./target/release/NexusDB --config nexusdb.toml` 启动, SIGTERM 时 16 桶直方图 dump 到 stderr (各阶段耗时: shard 事件循环 / drive_async_flush_round / poll_wait / block_on_io / flush_coroutine 等). 实现 [`crates/shard_manager/src/latency_probe.rs`](./crates/shard_manager/src/latency_probe.rs).

### 性能调优建议

对成熟生产场景, 以下参数在排对位置时收益明显 (以本机测试为准, 不同硬件差异较大):

1. **io_backend**: 默认 `io_uring` 在 NVMe 上吞吐 +30-50% vs `stdfs`; 容器/老内核 (≤5.4) 或沙箱可能 hang, 用 `stdfs` 临时规避
2. **chunk_cache_size**: 默认 16 (16MB hot cache). 调整上下界实测收益呈快速饱和, 工作集超过 hot cache 后收益归零
3. **num_shards**: 建议 = CPU 物理核数 (避免线程跨核调度); 同步 IO 路径下满载核会卡 shard 事件循环
4. **KvLimits**: 默认 1024/3000 字节安全余量充足; 改到接近 4000 后 leaf page 易分裂传播, 性能不升反降

### 调优方法

- `NLOG_PROBE=1` 启动压艃几圈, 先看 `flush_coroutine_total_ns` / `drive_until_idle_ns` 直方图, 找最长埋脚的阶段
- 调整后再次跑 memtier + 看探针对比, 验证改善是否为改参带来的还是随手抖
- 任何调优前务必 cargo test 不退步 (我们使用大量集成测试覆盖语义)

---

## 架构总览

**Share-Nothing = 每个 shard 一个 OS 线程独占所有数据结构 + 一个 io_uring 实例 + 一个 Scheduler 实例.** 跨 shard 仅走 mpsc / Inbox / TaskReplyBus, 无锁无 race.

```
  Binary 5433 (TCP) ─┐
                     ├── NetworkServer ── KvLimits 协议校验 ── shard_manager::Router
  RESP  6379 (TCP) ─┘                                                    │
                                                                         ▼
                  shard_n thread (per-core, 单线程事件循环)
                                                                         │
                +--------+-----------+-------------------+----------------+
                ▼        ▼           ▼                   ▼                ▼
            LCB-Tree   NowChunks   WriteQueue         ChunkList       MetaCache
             (page)   (活跃 chunk) (in-flight 8)      (LRU 1MB×16)   (平坦 Vec + dirty window)
                │        │           │                   │                │
                +--------+-----+-----+-------------------+----------------+
                                  ▼
                          pager_io (io_uring / StdFs)
                                  ▼
                  .block + page.mate (per-db-per-shard)
```

关键设计点:

- **chunk = 1MB = 64 page × 16KB = 16M vpid 空间**; `NowChunks` 索引 = chunk_idx (懒扩容), 不维护 dirty 标记 — 驻留即待写, 满 swap 即移出
- **meta = `Vec<PidLocation>`, 索引 = vpid**, 懒扩容; open 整读 page.mate 进内存 (无 pread miss); flush 按 1MB window 走 batch
- **异步落盘零阻塞主循环**: `complete_flush` / `complete_meta_flush` 只置 `due=true`, 下轮 `drive_async_flush` 由协程执行 fsync; 收割路径 avg ~2μs
- **顺序不变量**: data chunk 写盘确认 → meta window 写盘 → pid.state 三段闭环, 任何一段失败均可重试而不破坏顺序 (recover 扫描 union 兜底)

完整分节: [DESIGN.md](./DESIGN.md) (10 节, 含 LSM/Storage 层细节 + 演进路线).

### 平台依赖

- **OS**: Linux (glibc / musl); io_uring 后端要求内核 ≥ 5.6 (推荐 ≥ 5.15 完整 IORING_FEAT_* 支持)
- **内存栈**: `RUST_MIN_STACK=8388608` (8MB), 默认栈太小 IO 密集调用会深递归
- **磁盘**: NVMe SSD 提供 fsync ≤ 100μs; SATA SSD ≈ 1ms; 存在大量 fsync IO 可能成为性能瓶颈
- **io_uring capability 检查**: `cat /proc/sys/kernel/io_uring_disabled` 应 = 0; 否则改为 `io_backend = "stdfs"`

---

## 支持的协议

| 协议 | 端口 | 状态 | 说明 |
|---|---|---|---|
| RESP2 (Redis 兼容) | 6379 | ✅ 完整 | PING/AUTH/SET/GET/DEL/MGET/MSET/INFO/EXPIRE; TCP_NODELAY; KvLimits 协议层长度拦截 |
| Binary (自研) | 5433 | ✅ 完整 | Request/Response + BatchOp (Put/Get/Delete 同 key) + TravelTree; 多客户端 + ReplyBus |
| PostgreSQL (wire) | - | 🚧 占位 | 见 [DESIGN.md §10](./DESIGN.md) roadmap, 待实施 |
| MySQL (wire) | - | 🚧 占位 | 同上 |
| MongoDB (BSON) | - | 🚧 占位 | 同上 |

设计哲学: **统一记录编码 + value type tag** 已预留, 新协议接入无需改 storage 层, 只需添加 `crates/protocol/<x>/` parser + adapter.

---

## 配置

完整字段注释见 [`nexusdb.toml`](./nexusdb.toml). 重点 section:

```toml
[server]
listen_addr = "0.0.0.0:5433"     # Binary 协议
redis_addr = "0.0.0.0:6379"      # RESP (空字符串 = 禁用)
redis_password = ""              # AUTH 密码
max_key_bytes = 1024             # 协议层拦截
max_value_bytes = 3000           # key+value <= 4000 (page 编码限制)

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

KvLimits 默认值依据 page slot 编码约束 (`PAGE_SIZE = 16KB`, 单 leaf page 容纳 ~64KB 安全容量), 改大需先评估分裂传播开销.

---

## crate 职责

| crate | 职责 | 状态 |
|---|---|---|
| `crates/scheduler` | 单线程协程调度器 + io_uring 桥 (`SchedHandle`/`drive_until_idle`/`io_ops::read/write/fsync`/`FdPool`) | ✅ |
| `crates/page` | LCB-Tree 页 (leaf/insert/split/delete/checkpoint/前缀压缩/`Item`/`ItemKind` 编码) | ✅ |
| `crates/storage` | 物理持久化层: `Pager`/`MetaCache` v3 全量平坦 + dirty window/`NowChunks` 数组化/`ChunkList` LRU/`recover` 扫描 union | ✅ |
| `crates/network` | 双协议门面 (`acceptor` + `epoll worker` + RESP2 parser + Binary request/response + `KvLimits` 校验 + value type tag) | ✅ |
| `crates/shard_manager` | 多 shard 控制器 (`ShardManager`/`Router`/`Inbox`/`TaskInbox`/`ReplyBusSet`) + `latency_probe` 探针 | ✅ |
| `crates/config` | TOML 配置加载 | ✅ |
| 根 `src/main.rs` | 服务器入口: `nexusdb --config nexusdb.toml`, 信号优雅退出 | ✅ |

各 crate 实施细节 (分阶段 plan): [`docs/superpowers/plans/`](./docs/superpowers/plans/).

---

## 开发命令

```bash
# 全量回归 (682+ 测试, ~30s)
cargo test --workspace --no-fail-fast

# clippy (0 警告硬约束)
cargo clippy --workspace --all-targets

# release 构建 (性能测试前必须)
cargo build --release

# 启动 (生产)
RUST_MIN_STACK=8388608 ./target/release/NexusDB --config nexusdb.toml

# 启动 + 探针 (性能调优, 直方图到 stderr)
NLOG_PROBE=1 ./target/release/NexusDB --config nexusdb.toml

# 单 crate 测 (开发时快速迭代)
cargo test -p storage --lib
cargo test -p shard_manager --lib

# 单 testcase 跑 (调试)
cargo test -p storage --test recover_tests -- --exact some_test_name
```

开发约定见 [AGENTS.md](./AGENTS.md) (Rust 调试技巧 / fish shell gotchas / dead_code 处理 / 多线程契约等).

---

## 故障排查

| 现象 | 可能原因 |
|---|---|
| 启动报 `permission denied` / `disk full` | `block_root` 路径权限 / 磁盘空间; 检查 `nexusdb.toml` `[storage].block_root` |
| 启动 hang 在 io_uring 初始化 | 容器/沙箱可能无 io_uring 支持; 改 `io_backend = "stdfs"` 临时规避 |
| `RST_STREAM` 长尾突增 | 网络层 TCP_NODELAY 未生效; 见 [AGENTS.md](./AGENTS.md) 中 TCP_NODELAY 注意事项 |
| p99 突刺 ~ms 级 | 多为磁盘 fsync 排队; 切换 NVMe / `NLOG_PROBE=1` 拿探针对照 |
| 数据读不到 | 多 db 切换: 确认 SET 时使用的 db 名 (`SELECT dbname`); 默认 db 始终有效 |

### 已知 gap (DESIGN/AGENTS 中也已记录)

- **GC/vpid 回收**: `VpidAllocator` 不回收 vpid, 删除多的工作负载下 `Vec<PidLocation>` 按最大 vpid 占内存
- **per-db per-mate**: 当前全 db 共用单 mate 文件 (off = `vpid*8`), vpid 单空间不感知 db (Pager 已按 db 目录物理隔离, 无功能影响)
- **PostgreSQL / MySQL / MongoDB 协议**: DESIGN §10 roadmap 中, 多协议基础设施 (unified record encoding + value type tag) 已就绪

### 调试探针

`NLOG_PROBE=1` 启动 → SIGTERM 时 16 桶直方图 dump 到 stderr, 字段:

- `flush_coroutine_total_ns` — 单个落盘协程总耗时 (write+fsync)
- `drive_async_flush_round_ns` / `drive_until_idle_ns` — shard 事件循环阶段
- `block_on_io_ns` / `poll_wait_ns` — 同步等待 / poll 唤醒
- `backpressure_sync_write_ns` — 背压退化同步写 (0 表示背压未触发)
- `in_flight_peak` — 异步落盘深度峰值

---

## 文档索引

| 读者 | 文档 |
|---|---|
| 评估 / 第一天 | 本 README |
| 架构理解 | [DESIGN.md](./DESIGN.md) (10 节) |
| 接手开发 (进度 / gotchas / 待办) | [AGENTS.md](./AGENTS.md) |
| 修复历史 (F1-F41) | [CHANGELOG.md](./CHANGELOG.md) |
| 各 crate 分阶段实施 plan | [`docs/superpowers/plans/`](./docs/superpowers/plans/) |
| Bug 根因调查 (示例) | [`docs/bug-report-btree-split-routing.md`](./docs/bug-report-btree-split-routing.md) |

---

## 许可证

NexusDB 源码采用 [LICENSE](./LICENSE) (见仓库根). 致谢: 协议层借鉴 [monoio](https://github.com/bytedance/monoio) / `tokio` io_uring 实验分支; 性能基线对比参照 [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark).
