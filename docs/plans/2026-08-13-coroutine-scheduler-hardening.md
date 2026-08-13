# 协程调度器与 Coroutine Worker 加固优化计划

## 目标与范围

将 `crates/scheduler` 与 `NEXUS_CORO_WORKER=1` 的网络 worker 从“实验性、可回退”
状态推进到可受控压测的候选实现。首要目标是正确性、容量边界和尾延迟；吞吐优化只能
建立在这些不变量已验证的基础上。

本计划仅覆盖单 worker、单调度线程内的协程运行时及网络接入路径：

- `crates/scheduler/src/{scheduler.rs,pool.rs,ready.rs,waker.rs,io_ops.rs,io_registry.rs}`
- `crates/network/src/worker/worker_coro.rs`

不改变 shard 路由、存储编码、RESP/SQL 协议语义，也不在验收前改变默认 epoll worker。

## 进度（2026-08-13）

- [x] **P0-A 容量边界与单线程所有权**：已取消 `Pool` 的活跃 slot 回绕复用；满载 task
  保留在 admission queue；移除 `SchedHandle::Sync` 并在首次 drive 绑定线程，第二 driver
  被明确拒绝。跨线程 sender API 作为后续兼容性增强，不阻塞当前单线程 worker 契约。
- [x] **P0-B IO 生命周期与取消**：全部 scheduler IO future 在 drop 时提交
  `IORING_OP_ASYNC_CANCEL`；`select_read` 落选 PollAdd、TLS 析构安全、registry 的
  register/complete/cancel/unknown-CQE 指标与回归均已覆盖。
- [x] **P1-A 回包直达与无 eventfd 唤醒**：每连接 reply queue 改为 worker 线程本地
  `Rc<RefCell<_>>`；reply dispatcher 直接 `unpark(task_id)`，连接仅 poll socket，不再
  分配 per-connection eventfd 或持有第二个 PollAdd。组合等待先注册 park waker，并在
  socket 胜出时清理残留 waker；同时修复 `ParkCurrent` 将任意重 poll 误判成 unpark 的
  错误唤醒缺陷。
- [↩] **P1-B ready queue 与 driver 事件化**：已完成实验实现与回归，但默认 epoll 的
  mixed A/B 未优于 P1-A，且拆分 CQE 阻塞策略后仍不稳定；代码已回退，保留测试数据与设计
  结论，后续须以隔离指标重新立项。
- [ ] **P2 公平性、后台维护与参数自适应**：尚未进入结果版本；继续使用 P1-A 的固定低
  优先级预算，等待长时 queue-wait 与 compact 饥饿证据后再实现 aging。

## 已知基线与问题

历史同机基线中，协程 worker 的吞吐约为 epoll 的 60%，且 p99.9 更高；默认运行路径
仍是 epoll，只有 `NEXUS_CORO_WORKER=1` 启用协程 worker。因此后续比较必须显式指定
两种模式、同一份配置、独立数据目录和相同预热过程。

2026-08-13 首轮 P1 验收（4 worker / 4 shard / 32 client / pipeline 16 / 64B /
100K preload / WAL off / mixed 1:1 / 10s，单轮，仅作方向性证据）：

| 模式 | Ops/s | p99 | p99.9 |
|---|---:|---:|---:|
| epoll | 240.5K | 4.32ms | 19.07ms |
| coroutine（P1-A 提交基线） | 160.4K（66.7%） | 6.72ms | 29.18ms |
| coroutine（P1-B 实验） | 168.6K（70.1%，相对 P1-A +5.1%） | 5.95ms | 39.42ms |

协程仍未达到晋级门槛，保持 opt-in。另试验过“PollAdd 后同步 recv”以去掉 io_uring
Read，但吞吐进一步降至 148.6K ops/s，已在同轮回退；后续需聚焦 poll 重臂与任务批处理，
不能把同步 recv 当作热路径优化。

当前差距的已验证归因是网络协程热路径的固定事件成本：`PollAdd` 默认 one-shot，连接每
次 socket 事件都要重新提交 poll；随后 `recv_async` 还要提交一次 io_uring Read。因此一
次输入至少经过两次 SQE/CQE、两次 registry 状态迁移和两次 task 唤醒。epoll worker 则由
一次共享 epoll wait 取得就绪事件后直接消费 socket。

已试验 Linux 5.13+ multishot `PollAdd`（当前内核 7.0 满足前提）：单 fd 回归可连续交付
两次 CQE，但真实 32-client preload 在约 32% 停滞，说明当前“await 一个就绪后立刻继续
协议处理”的协程模型与 level-triggered multishot 的背压/重新消费语义不兼容。实验已回退。
结论是 multishot 作为方向正确，但只能随每连接的显式 read-ready 状态机、CQE `MORE` 队列
和接收背压一起重构，不能作为当前 PollFd 的直接替换。

2026-08-13 补充了一项低风险实验：协程路径在已收到 `POLLIN` 后，使用 socket 专用的
`IORING_OP_RECV` 取代通用 `IORING_OP_READ(offset=-1)`。socketpair 回归及 coroutine
协议 e2e 均通过；紧邻的 30 秒 A/B 为 RECV 136.5K、READ 132.9K ops/s，p99/p99.9 为
7.49/29.18ms vs 7.78/34.30ms。差异只有约 2.7%，而此前 10 秒样本波动达
151.2K–171.4K，尚不足以证明改善。因此该试验已回退，不能据此宣称吞吐提升。它也无法消除一条
输入的两阶段等待成本；更关键的是，nonblocking socket 上直接提交单次 RECV 会收到 EAGAIN，
仍需 readiness 机制，不能安全地作为 `PollAdd` 的直接替换。

同日完成 shard 侧初步 A/B：默认 epoll 网络层、4 worker / 4 shard / 32 client /
pipeline 16 / 64B / 100K preload / WAL off / overwrite、每侧连续三轮 10 秒。P1-A 基线
为 125.5K、124.0K、118.6K ops/s（中位数 124.0K）；当前 P1-B/P2 为 124.2K、125.8K、
129.9K（中位数 125.8K，约 +1.5%）。说明通用 scheduler 改动对 shard 路径无回退，
但收益尚低于环境波动，不能作为独立吞吐优化成果。`NLOG_PROBE=1` 在该高压场景会输出
过量文本并触发临时目录配额，后续应把 scheduler/shard 探针改为采样或仅 shutdown dump，
再进行长时尾延迟与 flush backlog 验收。

默认 epoll 网络路径的 mixed 1:1 回归也完成了相邻三轮对照（同样为 4 worker / 4 shard /
32 client / pipeline 16 / 64B / 100K preload / WAL off / 10 秒）：P1-A 为 254.5K、237.1K、
232.9K（中位数 237.1K）；当前为 229.4K、233.4K、264.4K（中位数 233.4K，约 -1.6%）。
当前 p99.9 中位数为 6.14ms，基线为 5.18ms。样本量不足以归因，却未满足“确定无回退”标准；
在扩展 scheduler 改动前，应先隔离 `drive_until_idle` 的 CQE 阻塞策略和 ready queue 改造，
对 shard flush 驱动分别做 A/B。

隔离后，让 shard 保持非阻塞 drive、仅 coroutine worker 在 idle 时等待 CQE，三轮 mixed
结果为 241.8K、220.4K、205.6K（中位数 220.4K），仍低于 P1-A。故本轮最终结果版本选择
`43158c8` 的 P1-A；P1-B/P2 所有未提交代码已回退。P1-B 的 coroutine 局部收益保留为后续
专用网络 runtime 重构的依据，但不能以默认路径性能为代价合入通用 scheduler。

本轮审计发现以下必须先处理的问题：

| 优先级 | 问题 | 风险 |
|---|---|---|
| P0 | `Pool` 在 1024 个活跃 slot 后回绕复用 | 覆盖仍在运行的 future、CQE 唤醒错误任务 |
| P0 | `select_read` 胜出一侧返回时，另一侧 `PollAdd` 无显式取消 | registry/CQE 残留、ring 容量逐步耗尽 |
| P1 | 每连接 reply eventfd + 双 PollAdd + reply mutex 中转 | syscall、锁竞争和常驻 SQE 放大 |
| P1 | ready queue 重复入队、每 poll 新建 Rc waker | 无效 poll、分配和引用计数开销 |
| P1 | `drive_until_idle(2048)` 加固定 50us sleep | 空转、固定延迟下界和不可控批处理 |
| P2 | 固定低优先级预算 | 前台持续负载下后台任务可能饥饿 |
| P2 | `SchedHandle` 对 `Rc<RefCell<_>>` 声明 `Send + Sync` | API 可被误用为多线程并发驱动，破坏单线程契约 |

## 全局不变量

后续每个阶段都必须保持：

1. 一个 `Scheduler` 只有一个 driver 线程；跨线程仅允许线程安全的 submit/stop/wake
   句柄，不能跨线程共享 `Rc<RefCell<Scheduler>>`。
2. 一个活跃 task 独占一个 slot；slot 只能在 future 完成并清理全部 IO 注册后复用。
3. 一个挂起的 io_uring 操作必须有且仅有一个终态：CQE 被 future 消费，或已取消并从
   registry 清理。future drop 不能遗留内核 PollAdd。
4. 同一 task 同时在 ready queue 中至多出现一次。
5. 回包不得丢失、串到其他连接或在连接关闭后访问已释放的状态。
6. epoll 路径、存储 scheduler 和既有协议行为零回归。

## 阶段 P0-A：容量边界与单线程所有权

### 实施

1. 将 `Pool::acquire()` 改为可失败的分配接口；移除 RR 回绕复用活跃 slot 的语义。
2. 网络 worker 在 task/连接达到容量时施加明确背压：暂停接收新连接或关闭新连接并返回
   可观测的 overload 错误。容量应来自配置或与 io_uring entries 联动，而非隐式常量。
3. 使用位图/slot state 明确表示 `Free / Running / Queued / Completing`，release 前断言
   task future 与该 slot 的全部 registry entry 已清空。
4. 移除 `SchedHandle` 的 `Sync`；将可跨线程使用的能力拆为独立 `SchedulerSender` 或
   `WakeHandle`。若仍需“move driver 到线程”测试，使用唯一所有权 move 而非 clone 后共享。

### 新增测试

- 1024、1025、容量两倍的 task 创建：不能覆盖、不能重复释放，满载行为可预测。
- 多线程只允许 sender 提交，编译期拒绝两个 driver；运行期断言 driver thread id 一致。
- 长连接数超过 ring 安全水位时：新连接被背压，已有连接仍可完成读写。

### 验收与回滚

- scheduler 全量测试、network coroutine e2e、默认 epoll e2e 均通过。
- 10 分钟连接风暴中 `in_use <= capacity`，无 slot ownership 断言、无 task 丢失。
- 若 API 拆分导致 storage 使用方无法无锁接入，保留旧 API 的 `#[deprecated]` 单线程
  wrapper 一个版本，但不得保留不安全的并发 `Sync` 语义。

## 阶段 P0-B：IO 生命周期与取消

### 实施

1. 为每个有 `user_data` 的 IO future 定义 drop 清理：从 `IoRegistry` 注销，并为已提交的
   请求提交 `IORING_OP_ASYNC_CANCEL`；取消 CQE 必须被识别为正常终态。
2. 将 registry 状态扩展为 `Submitted / Completed / Cancelling`，使重复 CQE、晚到 CQE 和
   slot 回收均可安全忽略且有计数。
3. 重写 `select_read`：二选一结束时取消未胜出的 poll；或直接在 P1 改为
   `select_fd_or_unpark`，但 P0 仍需保证任意 future drop 都能清理 IO。
4. 为 registry 添加指标：注册数、完成数、取消数、未知 CQE、峰值 in-flight；仅在 debug
   或 metrics 暴露，避免热路径字符串格式化。

### 新增测试

- 反复让 fd1 先就绪、fd2 永不就绪，循环至少 10 万次；registry 在每轮后回到稳定上界。
- 相反方向、两个 fd 同时就绪、future 在 pending 时 drop、连接关闭与 shutdown 并发。
- 人工压满 SQ/CQ 后取消：无无限 submit 重试、无错误 slot 被唤醒。

### 验收与回滚

- soak 期间 registry 峰值随活跃连接数线性有界，不随请求数增长。
- `unknown_cqe == 0`（主动取消对应的规范化取消 CQE 除外），无 hang、无泄漏 FD。
- 若目标内核不支持可靠 AsyncCancel，则此阶段停止，不启用 coroutine worker；不得以仅
  删除 registry 条目替代真正取消内核请求。

## 阶段 P1-A：回包直达与无 eventfd 唤醒

### 实施

1. 为每个连接记录其 coroutine task id 与本地 reply queue；reply bus 被 worker 的单个
   consumer 批量 drain 后，按 `conn_id` 投递并调用 `scheduler::unpark(task_id)`。
2. 连接协程用 `select_fd_or_unpark(socket_fd)` 替代 `select_read(socket_fd, reply_eventfd)`；
   socket 继续使用 io_uring poll，回包不再创建 per-connection eventfd/PollAdd。
3. registry 改为 scheduler 线程本地所有；只有确实跨线程的 reply bus 保持其原有同步。
4. 关闭流程通过 worker 级 wake/stop 解除 park，不遍历并 write 每个连接 eventfd。

### 新增测试

- 多连接交叉回包与 pipeline FIFO：回包只被目标连接消费，顺序与 epoll 一致。
- 连接在回包前关闭、回包与 socket readable 同轮发生、shutdown 时有 idle/active 连接。
- 1K/4K 空闲连接容量测试：常驻 PollAdd 近似为连接数而非两倍，FD 数不随回包增长。

### 验收与回滚

- 每条 shard reply 不再产生 per-connection eventfd write；每连接不再持有 reply eventfd。
- 同负载下 coroutine CPU 使用率与 p99.9 均优于改造前协程基线。
- 该阶段独立 feature flag（例如 `NEXUS_CORO_REPLY_UNPARK=1`）灰度；任何错投、丢包或
  关闭卡死立即关闭 flag，回到已验证的 eventfd 实现。

## 阶段 P1-B：ready queue 与 driver 事件化

### 实施

1. 在 slot 增加 `queued` 标记，封装 `enqueue_once(slot)`；所有 waker、CQE、unpark 和
   新任务提交都走同一入口。出队时先清标记，允许 poll 中合法自唤醒。
2. 将稳定的 task header/waker 缓存在 slot 中，避免每次 poll 分配 `Rc<SlotWakerInner>`。
3. 将 `drive_until_idle` 拆成“运行有限 ready budget”和“等待 CQE/外部唤醒”两种路径；
   无 runnable task 时使用 `submit_and_wait` 或带 timer 的 io_uring wait，而非固定 50us
   sleep。
4. ready budget 设为可观测且可配置的上限；每轮先处理所有已完成 CQE，再处理新 ready
   task，避免 I/O completion 在大批 CPU task 后长期滞留。

### 新增测试

- 对同一 task 连续 wake 10 万次：实际 poll 次数不超过完成所需次数加常数。
- 空闲、单连接、pipeline、高连接数下的 CPU idle 占用和 CQE 到 task poll 延迟。
- 自唤醒、CQE 唤醒与 unpark 同轮发生：无遗漏且不递归爆栈。

### 验收与回滚

- ready queue 长度不因重复 wake 无界增长；无效 slot poll 计数接近 0。
- 空闲 worker 不再以 2048 轮 busy-drive 加 50us sleep 运行。
- 若新 blocking driver 影响存储层主动轮询契约，先仅用于 network coroutine worker，
  scheduler 通用 API 保持兼容。

## 阶段 P2：公平性、后台维护与参数自适应

### 实施

1. 将固定 `LOW_PRIO_BUDGET=1` 改为 token/aging 策略：每轮有前台预算，后台 task 等待
   时间越长权重越高；对单次 poll 设置 cooperative yield 约束。
2. 分离 I/O completion、前台协议、flush/compact 三类队列并记录 queue-wait 直方图。
3. 根据 ready backlog、CQE backlog 和后台等待时间动态调整 batch/budget；参数保留配置
   覆盖，禁止在首版引入不可解释的黑盒自调。
4. 为超长 task 打点：连续运行时间、yield 次数、IO wait、持有资源；只对有证据的
   长任务引入轮切，不改变短任务顺序。

### 验收与回滚

- 持续前台压测下，后台 flush/compact 有可证明的最大等待上界。
- 混合负载 p99.9 不高于 P1 完成后的基线，后台吞吐不因饥饿降为零。
- 新策略通过配置开关启用；若公平性改善以吞吐或前台 p99.9 明显恶化为代价，恢复固定
  策略并保留观测数据。

## 基准与发布门槛

### 统一方法

每项性能结论都使用 release 构建、同机 CPU governor、独立数据目录、相同 shard/worker
数、预载和预热；epoll 与 coroutine 交替运行至少三轮，以中位数比较。不得以默认 epoll
的 memtier 结果证明 coroutine scheduler 优化有效。

建议场景：

1. 小值混合：`memtier_benchmark --ratio=1:1 --pipeline=16 --threads=4 --clients=8 --data-size=64 --test-time=30`
2. 读重与写重：1:10、10:1，分别测吞吐、p50/p99/p99.9。
3. 连接容量：1K、4K idle keepalive，再叠加 10% 活跃 pipeline。
4. 生命周期 soak：连接/断连、pipeline、后台 flush 并行运行至少 10 分钟；记录 FD、
   registry、CQE、ready queue、RSS 与错误数。

### 晋级标准

- 正确性：全量 scheduler/network 回归、coroutine 专属 e2e 和 soak 均无丢包、卡死、
  slot 重用、registry 增长或 FD 泄漏。
- 性能：协程 worker 在小值混合及读重场景的三轮中位数达到同配置 epoll 的至少 90%；
  p99.9 不高于 epoll 的 1.2 倍。写重场景须不低于 epoll 的 85%，并解释存储侧瓶颈。
- 容量：达到目标连接数时仍有明确 admission/backpressure，不发生 SQ 满忙循环。
- 发布：先保持环境变量 opt-in，观察一个版本；只有上述标准持续满足才考虑默认启用。

## 建议提交边界

1. `fix(scheduler): enforce slot ownership and single-driver handles`
2. `fix(scheduler): cancel dropped io_uring operations`
3. `feat(worker): route coroutine replies through scheduler unpark`
4. `perf(scheduler): deduplicate ready tasks and event-drive worker loop`
5. `feat(scheduler): add aged background scheduling and latency metrics`

每个提交必须附带对应的回归测试和一份前后基准记录；P0 与 P1 不应混入协议、存储或
格式变更，确保出现问题可按阶段独立回退。
