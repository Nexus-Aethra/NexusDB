# Scheduler Crate 设计文档

**日期**: 2026-07-17
**范围**: 单线程内部协程调度器 + 任务封装 + 任务接口 + io_uring 协程异步结合
**关联**: [`../../DESIGN.md`](../../DESIGN.md) §3.4 (Per-Shard 调度器)

---

## 一、目标与背景

NexusDB 的整体架构（见 `DESIGN.md`）是一个多 shard 的 KV 引擎，每个 shard 内部跑一个独立线程 + 独立 `io_uring` + 协程池。本文档聚焦**单一 shard 内部**的调度机制，将该机制抽离成一个独立的 Rust crate，以便：

1. **可独立测试**：在 `tests/` 目录下验证调度正确性、协程池复用、IO 异步结合
2. **可独立演进**：上层 page 模块只需要 `use scheduler::io_ops;` 等公共 API，不直接接触 io_uring 与调度循环
3. **对齐 DESIGN.md §3.4**：未来分片化时，多 shard 间的实现差异仅为外层线程包装，内核一致

本 crate **不做**的事：
- 不实现跨 shard 路由（`DESIGN.md §3.1`）
- 不实现 MPSC 任务队列的跨线程部分（本期用普通 `Mutex<VecDeque>`，单线程入口足够，未来分片化时升级为 `crossbeam::ArrayQueue`）
- 不实现 store/LCB-Tree（后续 `crates/page` 模块）

---

## 二、crate 布局

```
NexusDB/                          ← repo 根
├── Cargo.toml                    ← 改为 [workspace]
├── crates/
│   ├── scheduler/                ← 本次新写的 crate（lib only）
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   │   ├── lib.rs            ← 公开 re-export
│   │   │   ├── scheduler.rs      ← Scheduler struct + run()
│   │   │   ├── task.rs           ← spawn / JoinHandle / JoinInner
│   │   │   ├── pool.rs           ← Pool / Slot / acquire/release
│   │   │   ├── ready.rs          ← ReadyQueue (Mutex<VecDeque<usize>>)
│   │   │   ├── waker.rs          ← SlotWaker + make_waker
│   │   │   └── io_ops/
│   │   │       ├── mod.rs        ← re-export read/write/fsync
│   │   │       ├── registry.rs   ← IoRegistry (user_data ↔ state)
│   │   │       ├── read.rs       ← Read Future
│   │   │       ├── write.rs      ← Write Future
│   │   │       └── fsync.rs      ← Fsync Future
│   │   └── tests/
│   │       ├── lifecycle.rs      ← spawn + run + exit
│   │       ├── io_chain.rs       ← 多 IO 串行 async chain
│   │       └── pool_reuse.rs     ← 槽位复用与替换策略
│   └── page/                     ← 空占位（future storage module）
├── src/main.rs                   ← 保留作 demo（暂时 hello world）
├── DESIGN.md                     ← 已有
└── docs/superpowers/specs/
    └── 2026-07-17-scheduler-crate-design.md   ← 本文档
```

### 模块依赖方向（避免循环）

```
           io_ops/{read,write,fsync}
                       │
                       ▼
            io_ops/registry ──► waker
                                   │
                                   ▼
                                 ready ──► pool
                                   │       │
                                   ▼       ▼
                                 scheduler ◄─ task (spawn / JoinHandle)
```

- `waker`/`ready` 是最底层（`Waker` + 把虚拟 id 映射到 ready queue）
- `pool` 用 `waker`/`ready`
- `io_ops/registry` 管 user_data ↔ (slot, waker) 映射
- `io_ops/{read,write,fsync}` 用 `registry` + `waker`
- `scheduler` 是顶层 orchestrator
- `task` 是公开 API（`spawn` / `JoinHandle`），对外暴露

### 依赖

| crate | 用途 |
|---|---|
| `monoio` (`^0.2`) | 仅用其 `IoUring` raw API 与 opcode builder；**不**用其 `spawn`/`Runtime`/`Reactor` |
| （无） | 标准库足够；oneshot、waker 都是自实现 |
| `tokio` / `crossbeam` | **不引入** —— 与 monoio 的非阻塞假设冲突 |

---

## 三、任务接口与封装（task.rs）

### 公开 API

```rust
/// 把一个 Future 交给调度器。返回 JoinHandle，可 await 或 detach。
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static;

/// JoinHandle 既是 handle 又是 Future:
impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<...> { ... }
}

/// 当 JoinHandle 被 drop 但 future 仍在跑 → JoinError::Detached。
/// 当 future panic → JoinError::Panicked（v1 不做 catch_unwind，留给上层决定）
pub struct JoinError;
```

### 内部机制

**oneshot 通道**自实现（约 50 行）：
```rust
struct JoinInner<T> {
    state: Mutex<JoinState<T>>,
    // 等待方放进来一个 Waker；持有方的 Future 完成时把它 wake 出来
    // 用 Option<Waker> 而非 std::task::AtomicWaker:
    // - 我们这里只有一条 Waker（spawn 内部创建）, 不需要 atomic 优化
    // - 保留 Waker 类型即可, set/wake 流程由持有方 (Future wrapper) 触发
    waiter: Option<Waker>,
}

enum JoinState<T> {
    Pending,
    Done(Result<T, JoinError>),
}
```

**`spawn` 内部包装**：
```
用户的 future F ─┐
                │  Box::pin(async move {
                │      let r = fut.await;
                │      inner.set_result(r);   // 自动 wake JoinHandle
                │  })
                ▼
           wrapper: BoxFuture<'static, ()>
                │
                ▼
        scheduler.submit(InternalMessage::Task(TaskRequest { future: wrapper }))
                │
                ▼
        scheduler.run() 主循环 poll 它
```

**注意**：`spawn` 不直接推 `TaskRequest`，而是用 `InternalMessage::Task(...)` 装进 task_queue（§四），与 `InternalMessage::Stop` 共用同一条队列。

### 与未来 page 模块的衔接

不引入 `Task` trait 或 `enum Task`。理由：本 crate 只提供"调度 + Waker + io_uring 桥"，不做应用语义抽象。Page 模块后续会自己定义 `enum PageOp { Read, Write, ... }` 并 `spawn(op.into_future())` 即可。

### 错误处理

- `JoinError` 仅两个变体：`Detached`（handle 被 drop 但 future 仍在跑）、`Panicked`（v1 不实现 panic catching）
- 不把 io_uring 错误塞进 spawn 路径——spawn 只关心"调度能不能接受"，业务错误在 future 内部返回

---

## 四、调度循环与协程池（scheduler.rs + pool.rs）

### 常量

```rust
const POOL_SIZE: usize = 1024;      // 与 DESIGN.md §3.4.6 对齐
const BATCH_SIZE: usize = 200;      // 每轮最多接受 200 个新任务
const PARK_TIMEOUT: Duration = Duration::from_micros(100);  // 空闲时让 CPU 休息
```

> 1024 / 200 = 5，留 4 批 buffer，避免一个 batch 全部挂在 IO 上时挤压已就绪任务。

### Scheduler 与 Pool 内部结构

```rust
pub struct Scheduler {
    pool: Pool,
    ready: Arc<ReadyQueue>,
    task_queue: Mutex<VecDeque<InternalMessage>>,
    registry: Arc<IoRegistry>,
    ring: monoio::IoUring,
    stop_flag: AtomicBool,
}

pub struct TaskRequest {
    pub future: BoxFuture<'static, ()>,
}

pub(crate) enum InternalMessage {
    Task(TaskRequest),
    Stop,
}

struct Pool {
    slots: Box<[Slot; POOL_SIZE]>,
    free: VecDeque<usize>,                  // 空闲 slot 链表
    rr: usize,                              // free 为空时 round-robin 复用
    in_use: usize,
}

struct Slot {
    future: Option<BoxFuture<'static, ()>>, // Some=占用, None=空闲
    /// 该 slot 在 IoRegistry 注册过的 user_data 集合, 用于:
    /// - RR 强制复用时反向清理 registry (cancel_slot)
    /// - 任务完成时确认无遗漏的 IO
    /// CQE 的 i32 result 由 Future 自己用 peek_cqe_by_user_data 拿取, 不进 slot.
    pending_io: HashSet<u64>,
}

pub struct ReadyQueue {
    queue: Mutex<VecDeque<usize>>,         // Mutex 保证 wake-by-callback 安全
}
```

### Waker 机制（waker.rs）

```rust
struct SlotWaker {
    slot_id: usize,
    ready: Arc<ReadyQueue>,
}

impl Wake for SlotWaker {
    fn wake(self: Arc<Self>)  { self.ready.push(self.slot_id); }
    fn wake_by_ref(&self)     { self.ready.push(self.slot_id); }
}

fn make_waker(slot_id: usize, ready: &Arc<ReadyQueue>) -> Waker {
    Arc::new(SlotWaker { slot_id, ready: ready.clone() }).into()
}
```

- `Arc` 是标准 `Waker` 契约要求；多次 clone 共享同一 `ready`，重复 wake 幂等
- `Mutex<VecDeque>` 包装 `ready` 是因为 Future 契约允许从 poll 内部触发 wake

### 主循环

```rust
pub fn run(mut self) {
    while !self.stop_flag.load(Acquire) {
        // === Phase 1: drain task_queue (最多 BATCH_SIZE) ===
        // pop InternalMessage::Task 入 batch; 见到 InternalMessage::Stop 设 flag 后继续 drain
        let mut batch: Vec<TaskRequest> = Vec::with_capacity(BATCH_SIZE);
        {
            let mut q = self.task_queue.lock().unwrap();
            while batch.len() < BATCH_SIZE {
                match q.pop_front() {
                    Some(InternalMessage::Task(req)) => batch.push(req),
                    Some(InternalMessage::Stop) => {
                        self.stop_flag.store(true, Release);
                    }
                    None => break,
                }
            }
        }

        // === Phase 2: 装入 Pool, push 到 ready ===
        for req in batch {
            let slot_id = self.pool.acquire();
            self.pool[slot_id].future = Some(req.future);
            self.ready.push(slot_id);
        }

        // === Phase 3: 把 ready 全部 poll 直到空 ===
        loop {
            let mut wave = self.ready.drain();           // take ownership
            if wave.is_empty() { break; }
            for slot_id in wave.drain(..) {
                let slot = &mut self.pool[slot_id];
                let Some(fut) = slot.future.as_mut() else { continue };
                let waker = make_waker(slot_id, &self.ready);
                let mut cx = Context::from_waker(&waker);
                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(()) => {
                        slot.future = None;
                        slot.pending_io.clear();
                        self.pool.release(slot_id);
                    }
                    Poll::Pending => { /* waker 已 push slot */ }
                }
            }
        }

        // === Phase 4: 处理已就绪的 CQE, 没活则 park ===
        self.drain_completions();
        if !self.has_work() {
            std::thread::park_timeout(PARK_TIMEOUT);
        }
    }

    // 退出前: 等待在飞的 IO 完成 (或不等待, 直接清理)
    // 设计选择: 不主动等待, 让 CQE 自然回来被 registry.take 丢弃
}
```

**关于 `stop_flag`**：`run()` 不再 `-> !`，外部调用 `Scheduler::stop()` 后会向 task_queue 推 `InternalMessage::Stop`；主循环的 Phase 1 见到它就置 `stop_flag`，本轮跑完后 `while` 跳出。返回值是 `()`。

### `Scheduler::stop()`

```rust
impl Scheduler {
    pub fn stop(&self) {
        self.task_queue.lock().unwrap().push_back(InternalMessage::Stop);
    }
}
```

与 `spawn` 共用同一 mutex + queue，**不**引入第二个同步点。

### 关于 DESIGN.md §3.4.3 的一个修正

原 DESIGN.md 里有：
```rust
match fut.as_mut().poll(...) {
    Poll::Ready(()) => { ... }
    Poll::Pending => { made_progress = false; }     // ← 不正确
}
```

把 `Pending` 等同于"没进展"会让某些场景下**永远不退出 Phase 3**：一个 task 返回 Pending 但本轮 wake 又把别的 slot 推回 ready。这里直接 `drain ready 队列直到空`，更直观也更对。

### `acquire_slot` 策略

```rust
impl Pool {
    fn acquire(&mut self) -> usize {
        if let Some(idx) = self.free.pop_front() {
            self.in_use += 1;
            return idx;
        }
        // free 为空 → RR 复用最旧
        let idx = self.rr;
        self.rr = (self.rr + 1) % POOL_SIZE;
        self.in_use += 1;
        idx
    }

    fn release(&mut self, idx: usize) {
        self.free.push_back(idx);
        self.in_use -= 1;
    }
}
```

不变量：`POOL_SIZE ≥ 一批最多 IO 挂起数`，所以 `in_use ≤ POOL_SIZE` 恒成立。

### `has_work` 短路判断

```rust
fn has_work(&self) -> bool {
    if !self.task_queue.lock().unwrap().is_empty() { return true; }
    if self.ready.has_any() { return true; }
    if self.pool.in_use > 0 { return true; }
    false
}
```

任何一路有活儿就不 park。

---

## 五、io_uring 桥接（io_ops/*）

### 总览

```
┌─────────────────────────────────────────────────────────────┐
│  Scheduler (src/scheduler.rs)                               │
│    - Phase 4 调用 self.drain_completions()                  │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  IoRegistry (io_ops/registry.rs)                    │    │
│  │    user_data (u64) ↔ IoOpState { slot_id, waker } │    │
│  └─────────────────────────────────────────────────────┘    │
│                          ▲           ▼                      │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  io_ops Future (read.rs / write.rs / fsync.rs)      │    │
│  │    poll: 注册 user_data → 等 CQE → 取 result        │    │
│  └─────────────────────────────────────────────────────┘    │
│                          ▲           │                      │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  monoio::IoUring (raw ring: SQE 提交 / CQE peek)    │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

**关键边界**：monoio 的 IoUring 只用作 SQE/CQE 通道，**不用**其 runtime / spawn。我们的 Waker / 调度全在上一层。

### IoRegistry（io_ops/registry.rs）

**职责**：把 io_uring CQE 的 `user_data` 翻译回我们的 slot + waker。这是 CQE → SlotWaker 的唯一桥梁。

```rust
pub struct IoRegistry {
    inner: Mutex<HashMap<u64, IoOpState>>,
    next_user_data: AtomicU64,            // 单调递增, 永不重用
}

struct IoOpState {
    slot_id: usize,
    waker: Waker,
}

impl IoRegistry {
    fn register(&self, slot_id: usize, waker: Waker) -> u64;
    fn take(&self, user_data: u64) -> Option<IoOpState>;
    fn refresh_waker(&self, user_data: u64, new_waker: Waker); // re-poll 时替换
    fn cancel(&self, user_data: u64);                            // 单个 op
    fn cancel_slot(&self, slot_id: usize);                       // slot 释放时
}
```

**为什么不用 monoio 自己的 Waker**：monoio 的 Reactor 假定你 `monoio::spawn()` 它的 future，由它 own task——这跟我们的 slot 自管模型不兼容。我们必须自己把 CQE 映射回我们的 SlotWaker。

**result 的归属**：IO CQE 的 i32 结果**不**经过 registry，Future 自己用 `peek_cqe_by_user_data(ud)` 拿回。这样省掉了 `SharedResult` (Arc<Mutex<Option<i32>>>) 的间接共享与一次锁；Future 自己的 `user_data` 字段就是查询 key。设计取舍见 §七。

### io_ops 三个 Future 的形态

```rust
pub struct Read<'a> {
    ring: Rc<monoio::IoUring>,
    registry: Rc<IoRegistry>,
    fd: RawFd,
    buf: &'a mut [u8],
    offset: u64,
    user_data: Cell<Option<u64>>,           // 在第一次 poll 时分配, 之后保持
    submitted: bool,
}

impl Future for Read<'_> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // 1. 如果之前已经提交过, 先看 CQE 是否已就绪
        if let Some(ud) = this.user_data.get() {
            if let Some(code) = this.ring.peek_cqe_by_user_data(ud) {
                let _ = this.ring.advance_cqe();          // 推进 CQE ring
                this.registry.cancel(ud);                // 清理注册
                this.user_data.set(None);
                return Poll::Ready(map_result(code));
            }
            // CQE 没到, 但已被 wake → 重新注册 waker (外层已 consume 过)
            this.registry.refresh_waker(ud, cx.waker().clone());
            return Poll::Pending;
        }

        // 2. 首次 poll, 提交 SQE
        let ud = this.registry.register(slot_id, cx.waker().clone());
        this.user_data.set(Some(ud));
        this.ring.push_opcode(
            monoio::raw::opcode::Read::new(this.fd, this.buf.as_mut_ptr(), this.buf.len())
                .offset(this.offset),
            ud,
        );
        this.ring.submit();                              // 触发 ring submit (monoio: 视 API 决定是否手动)
        this.submitted = true;
        Poll::Pending
    }
}
```

**关键技巧**：用 monoio 的 raw opcode builder（`Read::new` / `Write::new` / `Fsync::new`）构造 SQE，通过 monoio 的 `push_opcode` 把 `user_data` 印到 SQE 上。CQE 接收走 ring 的 `peek_cqe_by_user_data(ud)`，waker 路由走 `IoRegistry`。两边各管一段，与 monoio 的 reactor 完全断开。

`Write` / `Fsync` 用相同结构，仅 opcode 类型不同。

### drain_completions

`drain_completions` 只做 waker 唤醒，**不**主动推进 CQE；CQE 留给 Future re-poll 时再去读 + 推进：

```rust
fn drain_completions(&mut self) {
    while let Some(cqe) = self.ring.peek_cqe() {
        let ud = cqe.user_data();
        match self.registry.take(ud) {
            Some(state) => {
                // 正常路径: wake slot; CQE 留在 ring 上, Future re-poll 时 advance + peek result
                state.waker.wake();
            }
            None => {
                // 孤儿 CQE (slot 已被 RR 替换; registry 不再有 entry).
                // 必须 advance 否则阻塞 ring; i32 result 静默丢弃.
                let _ = self.ring.advance_cqe();
            }
        }
    }
}
```

**Future 与 registry 协作**：
- 正常路径：drain_completions `take(ud)` 取出并 wake，Future re-poll `peek_cqe_by_user_data(ud)` 拿到 i32，**显式 `advance_cqe` + `registry.cancel(ud)`**。
- 孤儿路径：drain_completions advance + 丢弃；Future 不会再 poll（CQE 后 future 也已 drop）。
- 两边的清理是幂等的：drain_completions `take` 后 IOOpState 不存在；Future 推进 + cancel 之后再次 future poll 不会重复处理（user_data 已是 None）。

### 槽位回收与取消

**职责边界**：`Pool` 只管 slot 的分配与释放；`IoRegistry` 的取消由 `Scheduler` 协调。

```rust
// Phase 3: 任务完成 (in Scheduler::run)
Poll::Ready(()) => {
    slot.future = None;
    slot.pending_io.clear();           // future 已 done, 无 IO 挂着
    self.pool.release(slot_id);
}

// RR 强制复用: 先取消所有挂着的 IO 注册, 再复用 slot
// 在 Scheduler::run 的 Phase 2 - pool.acquire() 之前 / 之后调用
fn reuse_slot_after_rr(&mut self, slot_id: usize) {
    // 1. 取消该 slot 在 registry 里的所有注册
    for ud in self.pool[slot_id].pending_io.drain() {
        self.registry.cancel_user_data(ud);
    }
    // 2. slot.future 还会在 Phase 2 被覆盖 (旧 future 的 drop 不会影响已提交的 SQE, buffer 已释放)
    // 3. 复位 pending_io 状态
    self.pool[slot_id].future = None;
    // 4. 让出 slot 给 acquire() 的 RR 路径使用
}

impl Pool {
    fn release(&mut self, idx: usize) {
        self.free.push_back(idx);
        self.in_use -= 1;
    }
    // Pool 不接触 registry; Registry 取消只在 Scheduler::reuse_slot_after_rr 中发生
}
```

**v1 不实现 `IORING_OP_ASYNC_CANCEL`**：仅从 registry 移除 entry，未提交的 SQE 让内核自然完成；buffer 仍由 Future 借用 — Future 被 drop 同步发生在 RR 替换那一刻，用户须保证 `io_ops::read/write` 的 buffer 在 await 期间不被释放。`&'a mut [u8]` + async 生命周期已经帮我们强制这一点。

### 内存安全要点

| 风险 | 缓解 |
|---|---|
| SQE 提交后 buffer 被 drop（IO 写入/读取悬空内存） | `io_ops` 借用 `&mut [u8]`，Future 借用的生命周期覆盖 await 点 |
| user_data 冲突（重复或脏值） | `IoRegistry::register` 用 `AtomicU64` 单调递增，从不重用 |
| CQE 回来时 slot 已被复用 | `registry.take(ud)` 返回 None → drain_completions advance 该 CQE，i32 result 静默丢弃；Future 已被 drop，无 Arc 引用泄漏 |
| Future drop 时 SQE 未完成 | RR 复用时清空 pending_io；result cell Arc 释放 |
| Waker clone 后多次 wake | VecDeque push 多次幂等；poll 也是幂等的 |

### 公开 io_ops API

```rust
pub mod io_ops {
    pub async fn read(fd: RawFd, buf: &mut [u8], offset: u64) -> io::Result<usize>;
    pub async fn write(fd: RawFd, buf: &[u8], offset: u64) -> io::Result<usize>;
    pub async fn fsync(fd: RawFd) -> io::Result<()>;
    pub async fn close(fd: RawFd) -> io::Result<()>;
}
```

未来 page 模块典型用法：
```rust
// page/src/wal.rs (未来)
pub async fn flush(&self, chunk: &Chunk) -> Result<()> {
    let fd = self.fd;
    io_ops::write(fd, &chunk.bytes, chunk.offset).await?;
    io_ops::fsync(fd).await?;
    Ok(())
}
```

page 模块只需要 `await`；scheduler / registry / pool 对它不可见，**只有 `io_ops` 露面**。

### monoio 边界使用

| monoio 元素 | 用途 | 不用什么 |
|---|---|---|
| `monoio::IoUring::new(params)` | 创建 ring，参数可控 | `monoio::Runtime` |
| `opcode::Read/Write/Fsync::new()` | 构造 SQE | `monoio::fs::File` |
| `ring.submit()` / `ring.submit_and_wait(n, t)` | 提交 / 等待 | `monoio::spawn()` |
| `ring.peek_cqe*` | 读取 CQE | `monoio::select!` |
| `push_opcode(op, ud)` | 关联 user_data 与提交 | `monoio::task::*` |

明确的边界 → 不污染 monoio 的 runtime，未来可以替换底层为 `io-uring` crate 不影响上层代码。

---

## 六、测试策略

### `tests/lifecycle.rs`
- 创建 Scheduler，spawn 一个返回常量的 future，await JoinHandle 拿到结果
- 验证：`spawn` → `run()` → JoinHandle 拿到值
- 验证：spawn 后立刻 detach（drop JoinHandle）不 panic

### `tests/io_chain.rs`
- 创建临时文件
- spawn 一个 future: `io_ops::write(fd, b"hello", 0).await -> io_ops::fsync(fd).await -> 5`
- 验证：文件内容是 "hello"
- spawn N 个并发 future（不同 offset）写一个文件
- 验证：所有 write 完成后文件内容正确

### `tests/pool_reuse.rs`
- spawn BATCH_SIZE+10 个 fire-and-forget future（每个 yield 一次）
- 验证：跑完所有任务无 panic
- 验证：`pool.in_use` 结尾回到 0
- 验证：RR 替换至少发生过一次（用原子计数器 probe）

### Test 工具

每个 test 跑在独立 OS 线程，调用 `scheduler.run()` 直到 task_queue 空（has_work 恒假时退出，或测试结束时手动停）。

> **停不下来问题**：`run()` 在没有任何任务且没有 IO 时会 `park_timeout(PARK_TIMEOUT)`。测试通过 `Scheduler::stop()` 往 task_queue 推 `InternalMessage::Stop`，主循环 Phase 1 见到它就置 `stop_flag`，本轮结束跳出 `while`。

---

## 七、风险与已知缺口

| 风险 | 优先级 | 缓解 |
|---|---|---|
| `monoio::IoUring::push_opcode` 是否带 `user_data` 字段 | 高 | 实现前先验证；如不带，走 `opcode.set_user_data(sqe, ud)` 路径 |
| Registry `user_data` 用 `AtomicU64` 仍是 64 位 | 低 | 单线程内独占分配，无冲突 |
| CQE 顺序 vs SQE 顺序 | 低 | io_uring 保证；我们靠 user_data 匹配，无关顺序 |
| `&'a mut [u8]` 与 `Pin<&mut Self>` 借用冲突 | 中 | `io_ops::read(buf: &mut [u8])` 在函数体内 `Box::pin`，生命周期逐 frame；返回 `impl Future + 'a`；用户 await 时 borrow 仍持有 |
| RR 复用时未取消 SQE 的 buffer 寿命 | 低 | v1 用户须保证 buffer 在 await 期间不 drop（async 生命周期保证） |

---

## 八、范围声明

### v1 本次交付

- 公开 API: `spawn`, `JoinHandle`, `io_ops::{read, write, fsync, close}`
- Scheduler: 单线程 run loop，Phase 1-4 全实现
- Pool: 1024 slot + RR 复用
- Ready queue + 自实现 Waker + 自实现 oneshot
- IoRegistry: user_data 单调递增分配 + completion 表
- io_uring 三个 op（read/write/fsync）的 Future + close（close 用 monoio 已有 `RingSubmission::close`） 
- 三个 integration test（lifecycle/io_chain/pool_reuse）

### v1 不做（明确列出来）

- 跨 shard MPSC 任务队列（后面分片化时升级）
- 跨 shard 一致性 / 2PC
- LCB-Tree 存储（`crates/page` 另议）
- `IORING_OP_ASYNC_CANCEL` 内联取消（SQE 让内核自然完成，registry 静默丢弃结果）
- panic catching（留 JoinError::Panicked 占位，行为 stub）
- bloom filter、block 格式、WAL chunk 这些 io_ops 之上的语义
- 多 ring / NUMA 亲和

---

## 九、开放问题（实现期间再确认）

1. monoio 版本对齐：`^0.2` 还是其他（待 cargo add 后取实际值）
2. `monoio::IoUring::peek_cqe_by_user_data` 是否存在；如不存在走原始 `peek_cqe` + 过滤
3. `monoio::IoUring::submit_and_wait` 在没有 CQE 可等时是否真的阻塞（不能阻塞的话用 `park_timeout`）
4. `OpcodeBuilder::build()` 返回 sqe 后 monoio 是否自动 submit；不自动的话我们手动 submit

这些问题在第一版实现里一边实现一边验证，最终设计可能微调，但**总体架构与本文件一致**。
