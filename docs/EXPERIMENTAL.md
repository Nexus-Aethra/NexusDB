# 实验特性登记册 (Experimental Features)

> 状态快照: 2026-10-23。本文件登记所有**非默认、未定级或带回退开关**的特性。
> 实验特性不纳入默认兼容性/持久性承诺，行为与磁盘格式可能在无通知的情况下调整。

## 登记与晋升规则

1. **登记先于合并**: 新增实验特性必须在本文件登记（开关、默认值、作用域、
   测试证据、退出判据），否则不进入任何分支的合并流程。
2. **晋升默认**需同时满足：
   - 单测 + 集成测试 + 崩溃/恢复窗口覆盖；
   - 同机背靠背 A/B 或长稳压测，收益可复现；
   - 有文档与回退开关；
   - 开关从环境变量转为 `[storage]` / `[server]` 正式配置字段。
3. **退役**: 删除开关、代码与该登记项；不得留下"已死"的环境变量。
4. **磁盘格式**: 实验特性不得悄悄改变默认磁盘格式；格式变化必须带显式版本
   与迁移路径（Meta log 已遵守：默认关闭，关闭时遇到残留段会先合并再删）。

---

## 1. 实验特性（默认关闭，显式开启）

### 1.1 Meta log — meta 追加式日志

- **开关**: `NEXUSDB_META_LOG=1`（关闭时回到原地写 `page.mate`）
- **节奏参数**:
  - `NEXUSDB_META_LOG_DRAIN_MS`（默认 `1000`）: pending 缓冲 → 日志追加间隔
  - `NEXUSDB_META_LOG_MERGE_MS`（默认 `10000`）: 日志 → `page.mate` 合并最小间隔
  - `NEXUSDB_META_LOG_MERGE_BYTES`（默认 `32MB`）: 未合并日志字节阈值
  - `NEXUSDB_META_LOG_DEBUG=1`: 回收状态诊断输出
- **分支**: `Feat/meta-log`（未合并）；提交 `2e44943` + `a93c602`
- **作用**: 热路径只按 vpid 去重追加 20B 日志记录（定长 4MB 段），
  pending 缓冲在内存中合并热 key；后台合并把日志前缀写回 `page.mate` 后删段；
  启动 = 读 `page.mate` + 重放剩余段。
- **适用场景**: 大 keyspace（10M+）的稀疏随机覆写。小 keyspace 稠密写入时
  原地 8KB 子区更省字节（8B/槽 vs 20B/条），收益不明显。
- **已验证**:
  - 存储层 10 项（去重/回滚/截断/合并/崩溃重放/迁移/前缀 promote）+ engine 1 项 +
    shard 异步链路 1 项；
  - 全量 workspace 测试、`clippy -D warnings`、`fmt` 全绿；
  - 100K 键与 1M 键 A/B：100K 持平；1M 受 SSD 写节流/顺序效应干扰，
    无法判定胜负（先跑的一侧稳定慢 ~20%）。
- **已知限制**:
  - meta 持久性窗口 = drain 间隔（1s，对齐 WAL periodic）；
  - 合并快照为同步 memcpy（1M+ 键约 8MB，需分片以削 p99）；
  - 开关是环境变量，未转 config；
  - scanner 离线工具未重放日志（legacy `recover` 会先合并写回 mate）；
  - 大 keyspace 合并的 p99 尚未在干净环境验证。
- **退出判据**: 干净环境 10M+ 键随机覆写 A/B 稳定 ≥5% 收益 → 转正式 config
  并进入默认候选；否则保留 opt-in 或移除。

### 1.2 协程网络 worker

- **开关**: `NEXUS_CORO_WORKER=1`（默认 epoll worker）
- **作用**: 每连接一协程 + io_uring 的 network worker 实现，与 epoll worker
  共享同一协议分派与 shard 后端。
- **证据**: `crates/network/tests/worker_modes_parity.rs`、
  `worker_coro_e2e_test.rs`、`shared_workers_test.rs`
- **风险**: epoll 是稳定默认路径；协程路径需持续跑 parity 与长稳压测。
- **退出判据**: parity/长稳/压测达标后评估是否默认。

### 1.3 Put 微批处理

- **开关**: `NEXUS_PUT_BATCH=1`（压测脚本 `--enable-put-batch`）
- **作用**: shard 内相邻同表 SET 合并为 `put_many`，减少 BTree 遍历次数。
- **状态**: 代码注释明确为 opt-in，等待代表性 workload 证明净收益。
- **退出判据**: 代表性 workload 净收益 + 顺序/语义测试通过 → 默认。

### 1.4 Windows port (Beta)

- **状态**: Beta；`std::net` + 每连接一线程；无 io_uring/epoll；
  MySQL/PostgreSQL/HTTP/TLS 面为 Linux-only。
- **文档**: README "Windows (Beta)" 章节 +
  `docs/plans/2026-08-13-windows-portability.md`、`2026-08-13-windows-iocp.md`
- **退出判据**: IOCP/RIO 运行时 + 完整测试矩阵。

---

## 2. 默认开启但保留回退开关

这些不是实验特性，但允许运维/压测一键回退；登记以便排查。

| 开关 | 默认 | 作用 | 代码 |
|---|---|---|---|
| `NEXUS_LEAF_CACHE=0` | 开 | BTree leaf guide 缓存；0 = 回退对比 | `crates/storage/src/btree.rs` |
| `NEXUS_ADAPTIVE_TASK_WINDOW=0` | 开 | shard 自适应前台任务窗口 | `crates/shard_manager/src/shard_thread.rs` |
| `NEXUS_EPOLL_WRITE_BUFFER=0` | 开 | epoll worker 连接写缓冲 | `crates/network/src/worker/worker_epoll.rs` |

---

## 3. 调优参数（默认值已可用）

| 环境变量 | 默认 | 作用 |
|---|---|---|
| `NEXUS_PAGE_CACHE_PAGES` | `2048`（32MiB/shard） | chunk LRU 容量；`0` = 关闭 |
| `NEXUS_CHUNK_PROMOTE_MISSES` | `4`（2..16） | chunk 预读晋升所需 miss 页数 |
| `NEXUS_CHUNK_PROMOTE_MISS_GAP` | `512` | 晋升候选 miss 间隔上限 |
| `NEXUS_CHUNK_PROMOTE_MIN_INTERVAL_MS` | `25` | 晋升最小间隔 |
| `NEXUS_TASK_WINDOW_TARGET_US` | `2000` | 自适应窗口目标轮时长 |
| `NEXUS_FAIR_TURN_BUDGET_US` | 关闭 | 前台公平轮预算（50..10000μs） |
| `NEXUSDB_SHARD_REPLY_TIMEOUT_SECS` | `30`；`0` = 不限 | shard reply 等待超时 |

---

## 4. 诊断开关（不影响默认行为）

| 开关 | 作用 |
|---|---|
| `NLOG_PROBE=1` | 启用延迟直方图，SIGTERM 时 dump |
| `NLOG_GC_DEBUG=1` | GC 判活/搬移差异日志 |
| `NLOG_NO_FLUSH=1` | 禁用异步落盘（定位数据丢失根因） |
| `DEBUG_PAGE` / `DEBUG_PAGE_PROBE` | 编译期页面探针（零运行时开销） |
| `NEXUSDB_META_LOG_DEBUG=1` | Meta log 回收/promote 状态日志 |
| `NEXUSDB_BIN` | 仅压测脚本：指定被测二进制 |

---

## 5. 版本记录

- **2026-10-23**: 创建登记册；登记 Meta log（实验，分支 `Feat/meta-log`）、
  协程 worker、Put 微批、Windows Beta；补全回退/调优/诊断开关清单。
