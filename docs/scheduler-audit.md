# Scheduler Crate 架构与并发保护审计

> 对象: `crates/scheduler/`
> 状态: **全部任务完成 (T1–T11)**, 22/22 测试通过 (默认 + `--features scheduler-trace`).
> 最后更新: 2026-07-17.

## 1. 整体数据流

```
外部线程
  │ scheduler::spawn(fut) / spawn_on(handle, fut)
  ▼
Scheduler.submit() ─── task_queue: Mutex<VecDeque<InternalMessage>>   ← 唯一跨线程同步点
                                       │
                       ┌───────────────┘
                       ▼
SchedHandle::drive_until_idle(max_iters)
  │
  │ 每次迭代临时 borrow_mut Scheduler, release borrow → wrapper poll 时无冲突.
  │
  ▼
Phase A: drain_task_queue_phase → pool.acquire → ready.push(slot_id)
  │
  ▼ extract_pending (drain ready + 兜底扫描 pool.future)
  │
Phase B: poll 每个 future (不持有 self borrow, 允许 io_ops::with_current borrow_mut)
  │   - Pending → requeue (pool.slot.future = Some(fut))
  │   - Ready   → completed (slot release)
  │
  ▼
Phase C: drain_task_queue + drain_completions_with_submit
            ↑ 内部: ring.submit() + SQ.len() 判断 + submit_and_wait(1) if in-flight
            ↑ mark_completed + ready.push (不 take IoOpState, 让 io_ops 自己 take_result)
  │
  ↑ (回到 Phase A / 下一轮)
  │
  退出条件: has_work() 全 false (task_queue 空 + ready 空 + pool.in_use()==0 + registry 空)
```

## 2. 同步原语最终盘点

| 原语 | 位置 | 类型 | 必要性 | 备注 |
|---|---|---|---|---|
| `Mutex<VecDeque<InternalMessage>>` | `Scheduler.task_queue` | 跨线程 MPSC | ✅ 必要 | 唯一保留的 Mutex |
| `AtomicBool` | `Scheduler.stop_flag` | 跨线程信号 | ✅ 必要 | |
| `AtomicU64` | `IoRegistry.next_user_data` | user_data 单调递增 | ✅ 必要 | |
| `UnsafeCell<JoinState<T>>` | `JoinInner.state` | 单线程 poll | ✅ | 重构自 Mutex |
| `UnsafeCell<Option<Waker>>` | `JoinInner.waiter` | 单线程 poll | ✅ | 重构自 Mutex |
| `Rc<RefCell<VecDeque<usize>>>` | `Scheduler.ready` | waker 共享队列 | ✅ | 零运行时开销 |
| `Pool` (owned) | `Scheduler.pool` | &mut self | ✅ | 无 Mutex |
| `IoRegistry` (owned HashMap) | `Scheduler.registry` | &mut self | ✅ | 无 Mutex |
| `IoUring` (owned) | `Scheduler.ring` | &mut self | ✅ | 无 Mutex |
| `Rc<RefCell<Scheduler>>` | `SchedHandle` | 跨线程句柄 | ✅ | unsafe Send/Sync 标注 |

**结论**: Mutex 数量从原 **6 个 → 1 个** (仅保留 `task_queue`, 真正 MPSC 跨线程)。

## 3. 已完成的 4 个核心 bug 修复

### 3.1 `has_work()` 漏算 in-flight registry

**症状**: io_ops 注册到 registry 但还没 CQE, ready 和 pool 看似空, driver 误判空闲退出, await 永远不返回.

**修复**: `has_work()` 增加 `!self.registry.is_empty()` 检查.

### 3.2 `drive_until_idle` 必须在 extract_pending 前 drain_task_queue

**症状**: Phase A `extract_pending` drain ready 永远拿到 0 future, 因为 task_queue 里的 task 没搬到 pool.

**修复**: Phase A 入口先 `drain_task_queue_phase()`, 再 `extract_pending`.

### 3.3 `drain_completions` 必须 mark_completed 不 take, 让 io_ops 自己取

**症状**: 原实现 `drain_completions` 调用 `registry.take(ud)`, 把 CQE 结果从 registry 偷走. 但 io_ops.poll_cqe 第二次 poll 时还要从 registry 拿结果, 拿到 None → 永远 Pending.

**修复**: `drain_completions` 改用 `registry.mark_completed(ud, result)` (存结果但不删除) + `inner_peek(ud)` (取 slot_id 用于 wake). 新增 `IoRegistry::take_result(ud)` 供 io_ops.poll_cqe 自己消费.

### 3.4 `drain_completions_with_submit` 用 SQ ring len 而非 registry 判定 in-flight

**症状**: close CQE 到达后 mark_completed(ud=3, 0), 但 ud 仍在 registry 里 (等 io_ops.poll_cqe 二次 poll take_result). `if !self.registry.is_empty()` 仍为 true, 调 `submit_and_wait(1)` 永久阻塞 (没新 SQE).

**修复**: 用 `ring.submission().len()` (SQ ring 真实 tail-head) 替代 `!registry.is_empty()` 判定. SQ ring 真的没 SQE 时不调 submit_and_wait.

### 3.5 兜底: extract_pending 在 ready 空时扫描 pool

**症状**: 多 task 并发场景下, drain_completions wake slot 但 ready 已经被消费, pool.slot.future 还 Some 但 ready 空, driver 死循环.

**修复**: `extract_pending` ready 为空时, 兜底扫描 pool 中所有 `future: Some` 的 slot.

## 4. 已完成的 Mutex 重构

| # | 位置 | 原 | 现 |
|---|---|---|---|
| 1 | `Scheduler.pool` | `Mutex<Pool>` | owned `Pool` |
| 2 | `Scheduler.ring` | `Mutex<IoUring>` | owned `IoUring` |
| 3 | `Scheduler.registry` | `Mutex<IoRegistry>` | owned `IoRegistry` |
| 4 | `Scheduler.ready` | `Rc<ReadyQueue>` (with Mutex inside) | `Rc<RefCell<VecDeque<usize>>>` |
| 5 | `JoinInner.state` | `Mutex<JoinState<T>>` | `UnsafeCell<JoinState<T>>` + `unsafe impl Send` |
| 6 | `JoinInner.waiter` | `Mutex<Option<Waker>>` | `UnsafeCell<Option<Waker>>` |
| 7 | `task_queue` | `Mutex<VecDeque>` | **保留** (真正 MPSC 跨线程) |

**热路径性能**: 单线程调度器内部零 Mutex lock/unlock, 仅有 `task_queue` 一次锁/IO 入队 (跨线程必要).

## 5. 测试矩阵

### 5.1 默认构建 (零 trace 开销)

| 测试文件 | 测试数 | 覆盖点 |
|---|---|---|
| `lib.rs` (inline) | 0 | — |
| `io_chain.rs` | 3 | 顺序 read+write, write+read, write+fsync+close |
| `real_world.rs` | 4 | disk→内存→disk, 并发多 task, 交错 disk/memory, detached task |
| `lifecycle.rs` | 3 | spawn 完整生命周期 |
| `waker_ready.rs` | 2 | waker wake/wake_by_ref + 共享队列 |
| `pool_logic.rs` | 3 | Pool acquire/release/free/rr |
| `registry.rs` | 5 | IoOpState register/take/refresh/cancel |
| `oneshot.rs` | 2 | JoinHandle 一次性 future |
| **总计** | **22** | |

### 5.2 `--features scheduler-trace`

同 22/22 全过, 额外输出 `[trace] ...` 日志.

### 5.3 测试超时保护

每个 IO 测试用 `run_with_timeout(5000ms)` 包裹, 超时直接 `process::exit(1)`, 避免 io_uring 死锁时 cargo test 无限 hang.

## 6. 调度器内部 Trace 日志 (可选 feature)

启用: `cargo test -p scheduler --features scheduler-trace -- --nocapture`.

| Trace 事件 | 含义 |
|---|---|
| `iter=N phase=A has_work=...` | 每轮 Phase A 入口, 显示 task_queue / ready / registry / pool 状态 |
| `iter=N extract_pending got=N futures` | 本轮提取 future 数 |
| `iter=N phase=A work_empty ...` | work 空, 进入 CQE drain |
| `iter=N drive complete after N iters` | 调度器退出 |
| `iter=N phase=B polling N futures` | Phase B 开始 |
| `iter=N phase=B slot=K → Ready/Pending` | 每个 future poll 结果 |
| `io_ops submit_sqe ud=N slot=K` | io_ops 首次 poll 注册 SQE |
| `io_ops CQE ud=N result=N` | CQE 到达并 mark |
| `io_ops take_result(ud=N) → N/None` | io_ops.poll_cqe 取结果 |

### 示例输出

```
[trace] iter=1 phase=A has_work=true ready_len=1 registry=0 in_use=1
[trace] iter=1 extract_pending got=1 futures
[trace] iter=1 phase=B polling 1 futures
[trace] io_ops submit_sqe ud=1 slot=0
[trace] iter=1 phase=B slot=0 → Pending
[trace] iter=2 phase=A has_work=true ready_len=1 registry=1 in_use=1
[trace] iter=2 extract_pending got=1 futures
[trace] iter=2 phase=B polling 1 futures
[trace] io_ops take_result(ud=1) → 5
[trace] iter=2 phase=B slot=0 → Ready
[trace] iter=3 phase=A has_work=false ready_len=0 registry=0 in_use=0
[trace] iter=3 drive complete after 3 iters
```

## 7. 新增公开 API

| API | 用途 |
|---|---|
| `scheduler::yield_now().await` | 让当前协程主动让出, 调度器下一轮重新 poll (测试 + 内存计算交错) |
| `scheduler::SpawnHandle::detach()` | detached task, drop handle 后 task 仍可完成 |
| `scheduler::SpawnHandle::poll_wait` | JoinHandle Future 实现, 返回 `Result<T, JoinError>` |

## 8. 未来可选优化

| 优先级 | 优化 | 影响 |
|---|---|---|
| P2 | `Mutex<task_queue>` → `crossbeam::queue::ArrayQueue` | 高并发 spawn 提升 |
| P3 | `SchedHandle::unsafe impl Send/Sync` → 通过 `Rc<RefCell>` + `Send/Sync` 自动推导 | 移除 unsafe |
| P3 | `Waker` 用 `Arc<W: Wake>` 标准实现 | 移除手写 RawWaker 维护成本 |
| P3 | 多 worker 线程 (worker pool) | 真正的多核并行 |