# 协程 Worker 改造 — 状况分析报告

> 日期：2026-08-04
> 分支：`feat/worker-coroutine-scheduler`
> 用途：与其他开发者对接，说明当前进度、遗留问题与后续目标

---

## 1. 背景与目标

### 目标
实现 **NexusDB 全面协程化**：整个项目（存储层、日志层、网络层 worker）统一采用
「协程 + io_uring 事件驱动」的调度方式，替换传统 epoll worker，最终达到：

- 线程模型简化：worker 线程数 = 用户配置，不随连接/协议数膨胀
- 统一调度内核：所有异步 IO 走同一套 `scheduler`（协程调度 + io_uring 批量提交）
- 性能不劣于现有 epoll worker（当前瓶颈，见 §5）

### 改造范围
| 层 | 状态 |
|---|---|
| 存储层 shard | ✅ 协程化（每 shard 一个 Scheduler + io_uring 异步落盘，成熟范例） |
| 日志层 log | ✅ 协程化（log 线程自建 Scheduler + io_uring 批量落盘） |
| 网络层 worker | ⚠️ 协程实现已完成，**生产默认仍走 epoll**（`NEXUS_CORO_WORKER=1` 才启用） |

---

## 2. 已完成工作

### 2.1 提交清单（本分支近期）
| Commit | 内容 |
|---|---|
| `e1dbf65` | **fix(worker)**: 协程 worker shutdown 卡死 — 唤醒 idle 连接协程检查 stop |
| `ee29a5b` | **fix(scheduler)**: 批量提交修复 — 攒批 submit 前统一 `flush_sq`，解决 shard-0 忙循环 |
| `f314ca9` / `fe8bfa8` 等 | 6 个 io_uring 实验测试（批量提交/唤醒/阻塞语义研究） |
| 更早 | 协程 worker 全 5 协议接入、全局共享 worker 池（`SharedWorkerPool`）、协议 per-conn 配置 |

### 2.2 核心架构（已落地）
- **协程 worker**（`crates/network/src/worker/worker_coro.rs`）：每 worker 一个 `Scheduler` +
  每连接一协程 + io_uring 事件驱动；含 `reply_dispatch`（回包分发）+ `new_conn_loop`（新连接）
- **全局共享 worker 池**（`crates/network/src/server.rs`）：多协议 server 共享 worker 线程，
  线程数 = 用户配置，不再随协议数膨胀
- **协议 per-conn**：`NewConn` 携带 protocol + default_db/limits/auth/tls 配置
- **io_uring 批量提交**（`crates/scheduler/src/io_ops.rs`）：`submit_sqe!` 只 push 不逐 submit，
  驱动循环统一 `flush_sq()` 一次提交，实验基准 enter 次数减少 **64 倍**（128→2）

### 2.3 功能验证（协程模式 `NEXUS_CORO_WORKER=1`）
| 测试套件 | 结果 |
|---|---|
| `end_to_end`（3 个，含曾卡死的单连接多请求） | ✅ 全过 |
| `worker_coro_e2e` | ✅ 通过 |
| `resp_e2e`（22 个） | ✅ 全过 |
| `protocol_binary`（11 个） | ✅ 全过 |
| `shared_workers_test`（3 个，串行） | ✅ 全过 |
| scheduler 全部测试 + storage 全部测试 | ✅ 全过 |
| 默认 epoll 模式（回归确认） | ✅ 无回归 |

---

## 3. 当前问题（按优先级）

### 问题 1（核心）：协程 worker 性能比 epoll 慢 40%
严格对比（同数据规模、预热后，memtier 20c×4t×10k）：

| 模式 | Ops/sec | 平均延迟 | p99.9 |
|---|---|---|---|
| **epoll**（默认） | **233,100** | 0.34ms | 1.9ms |
| **coro**（协程） | **140,491** | 0.60ms | 6.3ms |

**瓶颈分析**（已验证推理）：
1. **请求串行 → 批量提交收益有限**：memtier 每连接请求串行，一轮 drive 通常只有
   1 个连接的 1-2 个 SQE 待提交，攒批空间小
2. **每请求 syscall 数约为 epoll 的 2-3 倍**：协程每请求 3 轮 select_read（各 2 个
   PollAdd）+ 1 次 read ≈ 3-4 次 `io_uring_enter`；epoll 仅 ~1 次
3. **reply 中转链路长**：shard → reply_bus → reply_dispatch 协程 → per-conn eventfd →
   conn_coro，比 epoll 的 REPLY_TOKEN 直通多一跳
4. **80 连接 = 160 个常驻 PollAdd**（socket + eventfd），io_uring poll 每事件唤醒开销
   大于共享 epoll fd
5. **驱动循环 `sleep 50us`** 累积长尾延迟（p99.9 6.3ms vs 1.9ms）

### 问题 2（遗留）：协程 worker 默认未开启
`NEXUS_CORO_WORKER` 环境变量控制，默认 epoll。因性能 −40%，**暂不建议改为默认**。

### 问题 3（遗留，预先存在）：测试并行卡死
- `shared_workers_test` 并行（`--test-threads>1`）时会卡住；串行全过。
- 已用 `git stash` 回退验证：**原始代码同样卡**，非本次改动引入。
- 疑似多 `SharedWorkerPool` + `mem::forget(tempdir)` 泄漏 / 测试间资源竞争。

### 问题 4（遗留，预先存在）：`sql_index_e2e.rs` 编译失败
- 仅测试文件：`Column` 缺 `default` 字段、`TableSchema::new` 参数不符（SQL 兼容分支
  新增 API，测试未同步）。**不影响 lib 编译**（`cargo build` 正常）。

---

## 4. 关键修复记录（供参考）

### 4.1 批量提交正确性（`ee29a5b`）
- **现象**：批量提交重构后完整 nexusdb 的 shard-0 100% CPU 忙循环卡死。
- **根因**：shard 的 `block_on_io`（同步忙等，不经过驱动循环）只 poll future 不 flush SQ
  → SQE 滞留 → CQE 永不出现 → 无限忙等。
- **修复**：`submit_sqe!` 只 push + 置 `sq_pending`；所有 CQ 扫描路径（`poll_cqe` /
  驱动循环 drain）扫描前统一 `flush_sq()`。回归测试 `batch_submit_correctness_test`（2 个）。

### 4.2 协程 worker shutdown 卡死（`e1dbf65`）
- **现象**：协程模式下 `end_to_end::multi_request_single_connection` 卡死。
- **根因**：`server.shutdown()` 的 `pool.join()` 依赖 worker 退出条件 `stop && active==0`；
  客户端不关连接时连接协程挂在 `select_read` 上 → `active` 不归零 → 无限等待。
- **修复**：`conn_coro` 持 stop 标志检查退出 + worker 主循环 stop 后写 per-conn eventfd
  唤醒 idle 连接。

---

## 5. 后续优化方向（按预期收益排序）

1. **去掉 reply_dispatch 中转**：每 worker 直读 reply_bus，按 `conn_id` 路由到连接协程
   队列，省一层 eventfd 唤醒（需解决之前的多连接抢包问题）
2. **unpark 替代 per-conn eventfd**：此前实验已验证多连接场景 +37%，需组合 poll+park
   解决调度器阻塞问题
3. **减少 PollAdd 次数**：连接生命周期内复用 poll 注册，而非每请求重建
4. 长尾优化：缩短驱动循环 sleep / 动态退避

**达成标准**：协程模式性能 ≥ epoll 的 90% 后，将 `NEXUS_CORO_WORKER` 默认开启，
完成全面协程应用。

---

## 6. 对接信息

### 环境
- 工作区：`/home/wpp/nexus/NexusDB`
- 分支：`feat/worker-coroutine-scheduler`（工作区干净）
- 数据目录：默认 `./data`；性能基准用独立配置 `/tmp/nexus_bench.toml`（数据 `/tmp/nexus_bench_data`）

### 常用命令
```bash
# 协程模式功能测试
NEXUS_CORO_WORKER=1 cargo test -p network --test end_to_end -- --test-threads=1

# scheduler 全部测试（含批量提交回归）
cargo test -p scheduler

# 严格性能对比（脚本：预热 + 正式 + 优雅关闭）
/tmp/run_bench.sh epoll
/tmp/run_bench.sh coro

# 启动 server
./target/release/NexusDB --config /tmp/nexus_bench.toml          # epoll
NEXUS_CORO_WORKER=1 ./target/release/NexusDB --config /tmp/nexus_bench.toml  # coro
```

### 注意事项
- 基准测试前确认端口干净：`ss -ltnp | grep -E "6379|5433"`；残留进程会干扰结果
  （本次已停掉预置 Docker 容器 `nexus-postgres`，其占用 13% CPU 与 15435/6778 端口）
- 环境中有遗留 4.8G 的 `./data`（含损坏 shard），性能对比**务必用独立临时目录**
- 批量提交的 `trace!` 日志需开启 scheduler 的 trace feature 才可见
