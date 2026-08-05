# RESP 随机写性能改进计划（pipeline 批处理）

> **状态：Phase 1 已验证，默认关闭实验实现**
> **范围：** RESP String `SET`/`GET` 热路径；不改变 Redis 语义、数据格式或持久化保证。
> **执行原则：** 每一个任务独立可编译、可测试、可基准回退；禁止为了吞吐关闭 WAL、异步 flush 或错误处理。

## 1. 背景与目标

2026-08-05 在本机 loopback、release、`io_uring`、4 shard / 4 network worker、
`wal_mode=off`、32 连接、pipeline=16、64B value 下的初步 memtier 结果：

| 负载 | 吞吐 | 延迟 |
|---|---:|---:|
| SET:GET = 1:1，20s | 266K ops/s | p50 1.78ms，p99 4.19ms |
| SET:GET = 1:9，15s | 318K ops/s | p50 1.55ms，p99 3.39ms |
| 纯 SET，15s | 175K ops/s | p50 2.96ms，p99 6.82ms，p99.9 15.9ms |

纯写从约 203K 降至约 139K ops/s，说明优化重点是随机写，而非读路径。
这些数字是方向性基线：当时 `bench_tmp/data` 并非全新目录，且一次诊断过程留下
了多个服务进程，因此**不能作为严格 A/B 门槛**。本计划的 Phase 0 会建立可重复的
基线后，才确定正式目标值。

### 2026-08-05 执行记录

已实现同 shard、同 `(db, table)` 的连续 `Put` 微批：非 `Put` 是严格顺序屏障，单项
保持原路径，多项复用 `table_put_many`，并逐一回复原请求；`NEXUS_PUT_BATCH=1` 才会启用。
新增 RESP 回归覆盖连续 pipeline 的可见性和同 key 最后写入。
`cargo test -p network --test resp_e2e -- --test-threads=1` 在默认路径及
`NEXUS_PUT_BATCH=1` 实验路径下均为 **24/24 通过**；release build 成功。

在全新临时数据目录、release、io_uring、4 shard / 4 worker、32 客户端、pipeline=16、
预载 100,000 个 64B key、`wal_mode=off` 的 10 秒覆盖写 A/B 中：

| 路径 | SET ops/s | 平均延迟 | p99 |
|---|---:|---:|---:|
| 实验微批 (`NEXUS_PUT_BATCH=1`) | 124,129 | 4.12ms | 8.90ms |
| 原单写路径 | 136,062 | 3.76ms | 7.23ms |

微批吞吐低 8.8%，且 p99 更差，因此不满足门槛，**默认关闭**。这表明在当前 key
分布和批大小下，`table_put_many` 的排序/临时分配成本未被叶页复用抵消。后续 Phase 2
应先用 perf/flamegraph 证明 IPC 或存储页遍历是主耗时，再开始实现；不能仅扩大批次。

### 2026-08-05 覆盖写 leaf 定位复用（已实现，待交错 A/B 复验）

阶段探针排除了同步写回压（`backpressure_fallbacks=0`，`block_on_io` 99% 小于 1us）。
随后将 inline value 的已有 key 覆盖写从 `leaf_get_with` + `leaf_update` 两次 PageIndex
解析/段内扫描，收敛为 `leaf_update_with` 的一次定位；溢出 value 仍使用原保守路径。

相同脚本、100,000 key 预载、64B value、10 秒覆盖写的两次观察值为 **166.8K / 163.4K
SET/s**，改动前该脚本一次为 **141.5K SET/s**，约 +16% 至 +18%，p99 从 6.82ms 降至
5.63ms / 5.73ms。由于尚未做交错的多轮 A/B，这一结果是强正向信号而非最终门槛结论。
页级回归 16/16、RESP E2E 24/24 通过；storage lib 的唯一失败是既有 LeafCache split
测试断言，与此改动无关。

### 2026-08-05 PageIndex 与 IPC 后续优化

- `PageIndex::locate_segment` 已从线性扫描改为真正的二分查找；页级测试覆盖首段、命中段、
  中间段和尾段。合入后一次 RESP 覆盖写观察为 **173.9K SET/s**, p99 **5.57ms**。
  该 workload 的 leaf checkpoint 段数有限，收益应以多轮交错 A/B 为准。
- `TaskInbox::push_batch_spin` 已实现，并接入 Binary protocol 的同 recv-buffer pipeline：
  worker 按 shard 收集 task，保持每 shard 输入顺序，批量仅做一次 pending 原子累加与至多一次
  eventfd 唤醒。Binary E2E 3/3 通过。
- RESP 不直接复用“延迟到 buffer 尾再发”的机制：RESP 的多 key 聚合命令可与单 key 命令
  交错，若不先刷新暂存任务会反转 shard 内可见顺序。完整 RESP 批化需引入显式 dispatcher
  batching context，并使所有 grouped dispatch 先刷新该 context；这是独立后续任务。

### 2026-08-05 新 key 单次 B+Tree travel

`table_put` 过去先用无 path travel 判断 key 缺失，再由 `btree_insert` 从根走一次。
现改为 `travel_to_leaf_with_path` 返回 leaf bytes 与 split path，并由
`btree_insert_from_leaf` 直接插入/传播 split，消除新 key 的第二次 root-to-leaf travel。

基准脚本新增 `fresh-write`（在预载 key range 之外随机写入），并修正当前 memtier 对
`key-minimum > 0` 的参数要求。10 秒 fresh-write 为 **254.3K SET/s**, p99 **4.32ms**；
同一 release 的覆盖写为 **168.8K SET/s**, p99 **5.86ms**，仍在此前覆盖写波动范围内。
RESP E2E 24/24 通过；storage lib 测试目前被工作区既有 pager 测试遗漏 `chunk_offset`
导入阻断，生产库 `cargo check -p storage` 通过。

### 2026-08-05 回包缓冲复用

`TaskReplyBus::drain_into` 允许 worker 复用 `Vec<TaskResult>`；默认 epoll worker 与协程
reply dispatcher 都已接入，避免每一次 reply eventfd 唤醒分配一个新 Vec。RESP E2E 24/24 和
Binary E2E 3/3 通过。一次 release 覆盖写的最终统计为 156.7K SET/s、p99 5.60ms，未显示
稳定吞吐增益（短基准波动较大），因此将它作为低风险的分配优化保留，不单独归因性能收益。

### 2026-08-05 LeafCache 读路径集成（已实现）

接管工作区已有的 `LeafCache` 后，已将其接入 `btree_lookup_with`：命中时跳过 root 到
leaf 的 internal-page travel，但仍通过 `Pager::read` 读取 leaf，因此不绕过 NowChunks、
COW 或页池生命周期。任何 leaf/internal split 都按 root 主动失效 cache；cache 仅按 shard
持有，未引入跨线程共享或锁。默认启用，`NEXUS_LEAF_CACHE=0`（脚本
`--disable-leaf-cache`）可作运行时回退。

同一台机器、release、4 shard / 4 worker、32 客户端、pipeline=16、100,000 个预载 64B
key、`wal_mode=off`、10 秒单轮的交错 A/B：

| workload | LeafCache 默认 | 关闭缓存 | 吞吐变化 | p99 变化 |
|---|---:|---:|---:|---:|
| hot-read（1% 热键，SET:GET=1:99） | 461,067 ops/s | 340,430 ops/s | **+35.4%** | 2.287ms → **3.119ms** |
| read-heavy（随机 100K 键，SET:GET=1:9） | 395,565 ops/s | 284,571 ops/s | **+39.0%** | 2.751ms → **3.839ms** |

这证明读路径 root-to-leaf travel 是当前可真实优化的热点；缓存既提升吞吐也降低尾延迟。
存储单元测试 180/180（另 8 个手动 benchmark ignored）和 RESP E2E 24/24 已通过。

### 2026-08-05 RESP 单次接收批量投递（已实现）

单 key RESP 命令在一次 `recv` 的解析期间按 shard 暂存，结束时以
`TaskInbox::push_batch_spin` 批量投递；这将每任务的 `pending.fetch_add` 与 eventfd 唤醒
收敛到每 shard 至多一次。每个 shard 的暂存 Vec 保持 parser 输入顺序。MGET、MSET、MSETNX
及集合/ZSet 的直接分组任务前先 flush 暂存任务，构成顺序屏障，避免同 shard 的新任务越过
先前 SET/GET。

新增 E2E 覆盖 `SET before; MGET; SET after; GET`，验证 MGET 必须读到 before。RESP E2E
**25/25**、共享 worker E2E **3/3** 通过。release 单轮（同一 10 秒基准）覆盖写为
**174.8K ops/s，p99 5.63ms**；读重负载 **387.5K ops/s，p99 2.88ms**。后者相对 LeafCache
基线约 -2%，处于单轮噪声范围，未显示尾延迟退化；后续以三轮中位数决定可量化归因。

### 2026-08-05 同叶页写批处理复验（未默认启用）

`table_put_many` 已补齐 inline 覆盖写的一次定位更新（`leaf_update_with`），消除原先
`leaf_get_with + leaf_update` 的重复 PageIndex/段扫描；storage 180/180 与 RESP 25/25 均通过。
但启用 `NEXUS_PUT_BATCH=1` 的随机覆盖写仍仅 **143.1K SET/s，p99 8.16ms**，低于当前单写
路径 **174.8K SET/s，p99 5.63ms**。根因仍是跨叶页的排序、编码与批量临时对象成本，未被有限的
同叶复用抵消。因此实验开关保持默认关闭；只有在能按已命中叶页直接分桶、避免全局排序后才重启该项。

### 2026-08-05 慢客户端回包背压（已实现）

epoll worker 的明文连接在 `WouldBlock` 时不再 `yield_now` 自旋：未发送字节进入每连接
4MiB 上限缓冲，注册 `EPOLLOUT` 后续写；缓冲超限关闭该慢客户端，保护 worker 内存。事件掩码
只在输出缓存空/非空状态切换时 `EPOLL_CTL_MOD`，正常小回复直写成功时没有额外 syscall。TLS 与
协程 worker 保留原有发送路径，避免改变其握手/调度语义；`NEXUS_EPOLL_WRITE_BUFFER=0` 可作
运行时回退。

RESP E2E 25/25、SQL E2E 通过。10 秒 read-heavy 同期 A/B 为：开启 **248.1K ops/s**、关闭
**248.0K ops/s**，p99 均约 4.3ms；该时段整体性能低于早先快照，但开/关无差别，故归类为尾延迟
与隔离保护，而非峰值吞吐收益。

### 目标

1. 在固定数据集的**覆盖写** workload 中，让 pipeline=16 的纯 SET 吞吐提升至少 20%，
   并且 p99.9 不恶化超过 10%。
2. 对 `SET:GET=1:1`，吞吐提升至少 10%，p99 不恶化超过 10%。
3. `wal_mode=periodic` 的提升方向与 `off` 一致；`strict` 的 fsync 成本不作为本计划优化目标。
4. 全量测试、崩溃恢复和顺序回复语义保持正确。

### 根因假设

当前单条 SET 的完整链路是：

```text
RESP parser → push_spin(每条一个 ShardTask) → TaskInbox
  → ensure_table + block_on_io(table_put) → B+Tree travel + 叶页写回
  → TaskReplyBus → eventfd → worker 回包重排
```

虽然 `table_put` 已将“查旧值 + 更新”合并为一次 B+Tree travel，pipeline 内的 16
条独立 SET 仍无法复用同一叶页。存储层已经有 `table_put_many`，它会排序并按 LeafGuide
复用路由/批量提交；但它仅服务显式的 MSET。因此先把**同一 shard drain 中的普通 SET
融合为内部批量写**，预期收益最高。

## 2. 非目标与约束

- 不启用或默认切换 `NEXUS_CORO_WORKER`；其当前性能仍低于 epoll，见
  `docs/2026-08-04-coro-worker-status-report.md`。
- LeafCache 已完成正确性回归与读路径 A/B，默认启用；写入与 split 仍须按既有根失效规则
  维护，不能将 cache 用于 insert 路径。
- 不改变跨 shard 的执行顺序；每个 shard 内只允许优化可证明等价的 SET 子集。
- 不关闭异步 flush。此前“关闭驱动”的对照并不构成有效产品基准，而且异步驱动是
  chunk 写进展所必需的。
- 不在正常读写路径引入锁、全局排序或跨 shard 协调。

## 3. Phase 0：可重复基准与退出问题（先完成）

### T0.1 建立独占的 benchmark 生命周期脚本 ✅（2026-08-05）

- [x] 新建 `scripts/run_memtier_bench.sh`：
  - 用 `mktemp -d` 创建**每次独立**的数据目录与日志目录；不复用 `bench_tmp`。
  - 生成临时 TOML：仅启用 RESP，监听一个可配置的 loopback 端口；默认
    `num_shards=4`、`worker_count=4`、`io_uring`。
  - 记录 server PID；启动后轮询 `redis-cli PING`；结束时发送 SIGTERM 并设定 30s
    上限，超时输出线程栈/日志后以非零状态退出。
  - `trap` 确保异常时仅清理该次脚本创建的 PID 和临时目录。
- [x] 增加参数：`--wal-mode off|periodic|strict`、`--workers`、`--shards`、
  `--duration`、`--port`、`--keep-data`。

已实现为 `scripts/run_memtier_bench.sh`：每次运行创建独立目录、记录 PID、轮询
RESP 就绪并在退出时清理。首次验证发现 memtier 的 `--requests` 是每连接计数，脚本已
按 32 客户端折算，并使用 parallel-sequential preload 覆盖完整 key range。

**验收：** 连续运行三次后，无残留 `NexusDB` 进程、无端口占用、数据目录彼此独立。

### T0.2 固化 workload，区分装载与覆盖写

- [ ] 在脚本中先执行固定 keyspace 的 preload（100,000 keys、64B）；preload 完成后等待
  5 秒使后台工作稳定。
- [ ] 正式测量三组场景，各运行 30 秒、重复三次并报告中位数：
  - overwrite-only：`--ratio=1:0 --pipeline=16`；
  - mixed：`--ratio=1:1 --pipeline=16`；
  - read-heavy：`--ratio=1:9 --pipeline=16`。
- [ ] 保存完整 memtier 输出到时间戳目录；报告 ops/s、p50、p99、p99.9、CPU 使用率。

**验收：** 同一配置三次 overwrite-only 的中位数偏差不超过 10%；否则先记录并解释
噪声来源，不进入性能实现阶段。

### T0.3 排查 SIGTERM 不能及时退出

- [ ] 复现：空闲、持续 SET、客户端未关闭连接三种场景分别发送 SIGTERM。
- [ ] 为 `main`、`NetworkServer::shutdown`、`ShardManager::close` 加临时结构化日志，
  标出进入/退出与等待对象；不提交永久热路径打点。
- [ ] 若确认卡在 shard drain，检查 `TaskInbox`、`TaskReplyBus` eventfd 消费与
  `drain_async_flush` 的退出条件；若卡在网络，检查 acceptor/worker join 顺序。
- [ ] 修复后增加一个有 10 秒超时的 integration test，覆盖“有 keep-alive RESP 客户端时
  server shutdown 返回”。

**验收：** 三种场景均在 5 秒内完成优雅退出；不需要 SIGKILL。

## 4. Phase 1：Shard 内普通 SET 微批处理（主优化）

### 设计

在 `shard_thread_main` 对一次 `TaskInbox::drain()` 得到的任务做稳定分段：

```text
同一 drain 的 tasks
  ├─ 不可合并任务 / 非 Put：沿用逐条执行
  └─ 连续、同 (db, table) 的 Put：最多 BATCH_MAX 条的 PutRun
       ├─ 去重同 key：保留最后一次 value，记录所有原任务的回复位置
       ├─ 调用 table_put_many（排序、LeafGuide 复用）
       └─ 按原任务顺序生成 PutOk 回复
```

`PutRun` 必须是**稳定的连续段**，不能跨越其他命令。对同 key 的多个 SET，执行一次最后
value 的写入与逐一 `+OK` 等价；对错误，要回退为逐条执行，以维持现有错误定位与部分成功
语义。首版不混入事务、TTL（未来）、复合类型或 SQL 行操作。

### T1.1 提取可测试的 batch planner（实现简化为 shard loop 内私有分段）

- [x] 在 `shard_thread.rs` 的 drain loop 中以 `VecDeque` 做私有稳定分段，避免新增
  API 与额外抽象；`PutReply` 保存原 task 的 `(conn_id, req_id, worker_id, group)`。
- [x] `PutRun` 保留每条原始 `(key, value)`（不去重），以保留现有
  `table_put_many` 的稳定排序和同 key 后写覆盖语义。
- [x] 常量 `PUT_RUN_MAX=128`，并在注释中说明它受 `TaskInbox` 默认批容量、延迟与内存
  约束；后续通过 benchmark 决定是否配置化。
- [ ] 单元测试：同表合并、跨表不合并、非 Put 截断、超上限分段、同 key 保留最后写入、
  回复顺序不变。

### T1.2 实现 `exec_put_run`

- [x] 在 shard 主循环内实现 PutRun：一次 `ensure_table`，一次
  `block_on_io(e.table_put_many(...))`。
- [x] 成功时为 run 内每个原 task 写入 `BatchResult::PutOk`；不得按去重后的 key 数少发回复。
- [x] 批量失败时不重试可能已部分写入的 batch；将同一存储错误返回 run 中每个原请求，
  避免二次执行扩大副作用。
- [ ] 保持 strict WAL 的“轮末 group commit 后再回复”规则：PutRun 的结果进入现有
  `held` 队列，不自行发送回复。

### T1.3 接入 shard 主循环

- [x] 将当前 `for task in tasks` 改为遍历 `TaskBatchPlan`；`Single` 使用既有路径，
  `PutRun` 使用 T1.2。
- [x] 保留 `TaskInbox`/`TaskReplyBus` API，先不混合网络层改动，降低回归面。
- [ ] 仅对 `BatchOp::Put` 且 table 非空启用；RESP 默认表以及 `table:key` 路由后的普通
  表都应受益。

### T1.4 正确性测试

- [ ] shard_manager 单元测试：同一 key 在一个 pipeline 中连续 SET 多次，最终值为最后一条，
  且每条都有 `PutOk`。
- [x] network RESP e2e：单 socket pipeline 混合 `SET a 1; SET a 2; GET a`；检查三条回复
  顺序与最终值。
- [ ] 追加随机 property/stress：随机 Put/Get/Delete 混合，用 `HashMap` 参考模型对拍。
- [ ] `TMPDIR=$PWD/target/nxtmp cargo test --workspace --no-fail-fast` 与 clippy 通过（当前被既有
  `sql_index_e2e` 的 `TableSchema::new` API 漂移编译错误阻断，和本改动无关）。

### T1.5 性能验收与调参

- [ ] 运行 Phase 0 固化的三次×三 workload；比较中位数。
- [ ] 依次测试 `PUT_RUN_MAX=32/64/128/256`；选择满足 p99.9 目标的最高吞吐值。
- [ ] 在 `wal_mode=periodic` 重跑 overwrite-only，确认收益不是仅依赖 `off`。
- [ ] 若 overwrite-only 提升 <20%，保留正确性实现但不默认启用；收集 flamegraph 或 perf
  采样后进入 Phase 2，而不是盲目扩大 batch。

## 5. Phase 2：投递与回复路径的批化（仅在 Phase 1 达标后）

### T2.1 网络输入按 shard 批量投递

- [ ] 在单次 `process_resp_input` 解析完成后，暂存 `ShardTask` 并按 shard 分组；调用一次
  新的 `TaskInbox::push_batch`，而不是每条 `push_spin`。
- [ ] `push_batch` 必须维持输入顺序，队满时以整体/剩余项安全重试，禁止丢任务。
- [ ] 只有在 parser 已经完整获得同一 recv buffer 的多个命令时启用；单命令延迟不得增加。

### T2.2 降低回复端的短生命周期分配

- [ ] 评估 `TaskReplyBus::drain()` 每次创建 `Vec` 的成本；若 perf 显示显著，改为
  `drain_into(&mut Vec<TaskResult>)`，由 worker 循环复用缓冲。
- [ ] 同样评估 `TaskInbox::drain()`；只在 profile 证明有效时复用 Vec，避免复杂化 API。

### T2.3 验收

- [ ] 所有协议 e2e 与共享 worker 测试通过。
- [ ] Phase 0 的 read-heavy 不得下降超过 5%；mixed 至少较 Phase 1 再提升 5%，否则不合入。

## 6. Phase 3：尾延迟与背压（独立后续）

### T3.1 用写缓冲替换 socket WouldBlock 自旋

- [ ] 在 `ConnState` 添加有界输出缓冲，socket `WouldBlock` 时注册 `EPOLLOUT` 后返回事件循环；
  不在 worker 线程中 `yield_now` 自旋。
- [ ] 设置每连接高水位线；超过上限时暂停读，避免慢客户端耗尽内存。
- [ ] TLS 仍经同一缓冲抽象写出，避免两套背压行为。

### T3.2 验收

- [ ] 加入慢读客户端测试：一个连接停止读取，其他连接的 SET/GET 仍完成。
- [ ] 高并发 benchmark 下 p99.9 至少不高于 Phase 2；吞吐不低于 Phase 2 的 95%。

## 7. 文件映射

| 文件 | 责任 |
|---|---|
| `crates/shard_manager/src/shard_thread.rs` | task drain、计划执行、strict WAL 回复时机 |
| `crates/shard_manager/src/exec_cmds.rs` | 普通 Put 与新 PutRun 执行器 |
| `crates/shard_manager/src/task_inbox.rs` | Phase 2 的批量投递与缓冲复用 |
| `crates/shard_manager/src/task_reply_bus.rs` | Phase 2 的回复 drain 复用 |
| `crates/storage/src/engine_io.rs` | 复用现有 `table_put_many` |
| `crates/storage/src/registry.rs` | 复用 LeafGuide 批量写，不改变 B+Tree 格式 |
| `crates/network/src/worker/resp_dispatch.rs` | Phase 2 的 RESP 投递聚合 |
| `crates/network/src/worker/worker_conn.rs` | Phase 3 的写缓冲与背压 |

## 8. 提交与回滚策略

- 一 task 一提交；每提交包含测试和 benchmark 摘要。
- Phase 1 可以由 feature/config 开关 `resp_put_batching` 灰度；默认值在达到 T1.5 指标后再改为开启。
- 任何正确性失败、p99.9 超过门槛或 `periodic` 回归 >10% 时，默认关闭该开关并保留基准材料。
- 性能报告必须同时附上：git SHA、CPU/内核、配置文件、数据规模、三次原始输出和中位数。
