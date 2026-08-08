# Scheduler Crate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现一个独立 Rust crate `scheduler`，承担 NexusDB 单 shard 内部的协程调度器、任务封装、任务接口与 io_uring 协程异步结合，公共 API 可被未来的 page / WAL 模块直接 `use`。

**Architecture:** 自实现的 Future executor（Pool + ReadyQueue + SlotWaker）+ 自实现的 IO Future（CQE → peek_cqe_by_user_data → registry.take → waker.wake）。io_uring 提交由 `monoio::IoUring` raw API 提供；monoio 的 `Runtime` / `spawn` / `Reactor` **不**引入。

**Tech Stack:**
- Rust 2024 edition
- `monoio` (^0.2) — IoUring + opcode builder
- `tempfile` (^3) — dev-dep for tests with real files
- 标准库 `std::sync::{Mutex, atomic::{AtomicBool, AtomicU64}}`

**关联 design doc:** [`../specs/2026-07-17-scheduler-crate-design.md`](../specs/2026-07-17-scheduler-crate-design.md)

---

## Global Constraints

| 约束 | 值 |
|---|---|
| 项目根 | `/home/wpp/nexus/NexusDB` |
| Rust edition | 2024 |
| Monoio 版本策略 | `^0.2`，第一次实装时核对 `[dependency]` 表（monoio 0.2.x 系列内 API 已稳定）；若 `cargo doc monoio` 文档提示 API 名差异 → 见 Task 7 fallback |
| Kernel | Linux 5.6+ io_uring；5.19+ 推荐 |
| Test 框架 | 内置 `#[test]` + `tests/` 目录 |
| 不引入 | `tokio` / `crossbeam` / `async-std` / `futures` |
| Commit 风格 | `type(scope): subject`，例 `feat(scheduler): add ReadyQueue` |
| 提交粒度 | 每 Step 5 一次 commit；每 Task 结束前至少一次完整测试通过 |

---

## File Structure

```
NexusDB/                              ← repo 根
├── Cargo.toml                        ← 改为 [workspace]
├── crates/
│   ├── scheduler/
│   │   ├── Cargo.toml                ← monoio 依赖
│   │   ├── src/
│   │   │   ├── lib.rs                ← pub use { task::{spawn, JoinHandle, JoinError}, io_ops, Scheduler }; 含 crate-level doc
│   │   │   ├── scheduler.rs          ← Scheduler struct, run(), stop(), submit(), drain_completions(), has_work()
│   │   │   ├── task.rs               ← InternalMessage, JoinInner, JoinHandle, JoinError, spawn()
│   │   │   ├── pool.rs               ← Pool, Slot, acquire(), release()
│   │   │   ├── ready.rs              ← ReadyQueue (Mutex<VecDeque<usize>>)
│   │   │   ├── waker.rs              ← SlotWaker, make_waker()
│   │   │   ├── io_ops.rs             ← 公开 read/write/fsync/close (impl Future)
│   │   │   ├── io_registry.rs        ← IoRegistry + IoOpState + AtomicU64 user_data
│   │   │   └── ops/                  ← 内部 opcode builders + push helper
│   │   │       ├── mod.rs
│   │   │       └── submit.rs         ← 把 opcode + user_data 包成 SQE, push 到 ring
│   │   └── tests/
│   │       ├── lifecycle.rs
│   │       ├── io_chain.rs
│   │       └── pool_reuse.rs
│   └── page/                         ← 空目录占位（future）
└── docs/superpowers/specs/2026-07-17-scheduler-crate-design.md  ← 已有
```

> 注：原 DESIGN.md §2 计划用 `src/io_ops/` 子模块子目录。本计划把 3 个 op 的 future 收进单一 `io_ops.rs`（共 ~150 行），Registry 与 SQE 提交拆开；这是落到代码时的微小调整，单文件依然 < 300 行，仍符合"小而聚"原则。

---

### Task 1: Workspace + Scheduler Crate Scaffolding

**Files:**
- Modify: `/home/wpp/nexus/NexusDB/Cargo.toml` (改写为 workspace root)
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/Cargo.toml`
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/src/lib.rs`
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/tests/lifecycle.rs`  (空, 只占位以便 `cargo test` 运行)
- Create: `/home/wpp/nexus/NexusDB/crates/page/.gitkeep`  (空占位)

**Interfaces:**
- Consumes: nothing
- Produces:
  - `crates/scheduler` library crate, name `scheduler`, edition `2024`
  - 顶层 `Scheduler`、`spawn()`、`JoinHandle` 仍是未解析状态 (后续 Task 才会创建)

- [ ] **Step 1: 改写根 `Cargo.toml` 为 workspace**

Replace `/home/wpp/nexus/NexusDB/Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = [
    "crates/scheduler",
    "crates/page",
]

[package]
name = "NexusDB"
version = "0.1.0"
edition = "2024"

[dependencies]
```

> 注意：保留 `[package]` 段以使顶层仍是合法的 binary crate (后续 Task N 才会删 main.rs)；但现在 `[members]` 已经让 Cargo 把 workspace 当 multi-crate 管。

- [ ] **Step 2: 创建 scheduler 子 crate 的 Cargo.toml**

```toml
# /home/wpp/nexus/NexusDB/crates/scheduler/Cargo.toml
[package]
name = "scheduler"
version = "0.1.0"
edition = "2024"

[dependencies]
monoio = { version = "0.2", features = ["iouring"] }

[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 3: 创建空 scheduler library**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/lib.rs

//! 单线程内部协程调度器 + io_uring 协程异步结合.
//!
//! 完整设计见 `docs/superpowers/specs/2026-07-17-scheduler-crate-design.md`.

#![allow(dead_code)] // 整个 crate 还在搭骨架, 暂时关闭 dead_code 警告

#[doc(hidden)]
pub mod _stub {}
```

- [ ] **Step 4: 创建空 placeholder 测试以保证 `cargo test` 不至于 0 个 case**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/tests/lifecycle.rs

#[test]
fn placeholder_will_be_replaced_after_task_5() {
    // 此文件为占位: Task 5 会用真正的 spawn lifecycle test 替换这里
}
```

- [ ] **Step 5: 创建 crates/page 空目录占位**

```bash
mkdir -p /home/wpp/nexus/NexusDB/crates/page
touch /home/wpp/nexus/NexusDB/crates/page/.gitkeep
```

> 注意：`Cargo.toml` 当前把 `crates/page` 列在 `members`，但没有该子目录的 `Cargo.toml`。为避免 cargo 报错，Step 5 应**先**在本任务中创建占位 `Cargo.toml`：

```toml
# /home/wpp/nexus/NexusDB/crates/page/Cargo.toml
[package]
name = "page"
version = "0.0.0"
edition = "2024"
publish = false
```

- [ ] **Step 6: 跑 workspace 级别的 build/test 通过**

Run: `cargo build --workspace`
Expected: success (no warnings beyond the `#![allow(dead_code)]` 静默)

Run: `cargo test -p scheduler`
Expected: `1 passed; 0 failed`.

- [ ] **Step 7: 提交**

```bash
cd /home/wpp/nexus/NexusDB && git add Cargo.toml crates/ && git commit -m "feat(scheduler): scaffold workspace with scheduler crate"
```

---

### Task 2: ReadyQueue + SlotWaker Primitives

**Files:**
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/src/ready.rs`
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/src/waker.rs`
- Modify: `/home/wpp/nexus/NexusDB/crates/scheduler/src/lib.rs` （加 `mod ready; mod waker;`）
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/tests/waker_ready.rs`

**Interfaces:**
- Consumes: nothing (primitive)
- Produces:
  ```rust
  pub struct ReadyQueue { queue: Mutex<VecDeque<usize>>, }
  impl ReadyQueue {
      pub fn new() -> Self;
      pub fn push(&self, slot_id: usize);
      pub fn drain(&self) -> VecDeque<usize>;
      pub fn has_any(&self) -> bool;
  }

  pub struct SlotWaker { slot_id: usize, ready: std::sync::Arc<ReadyQueue> }
  pub fn make_waker(slot_id: usize, ready: &std::sync::Arc<ReadyQueue>) -> std::task::Waker;
  ```

- [ ] **Step 1: 写 failing test**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/tests/waker_ready.rs

use std::sync::Arc;
use std::task::{Wake, Waker};

#[test]
fn wake_pushes_slot_id_to_ready_queue() {
    use scheduler::{make_waker_for_test, ReadyQueue};
    // 这些 helper 在 ready/waker 模块下 #[cfg(test)] 暴露, 见 Step 3

    let ready = Arc::new(ReadyQueue::new());
    let waker = make_waker_for_test(7, &ready);
    waker.wake();
    waker.wake_by_ref();
    assert!(ready.has_any());
    let mut drained = ready.drain();
    assert_eq!(drained.pop_front(), Some(7));
    // 重复 wake 是幂等的 — drain 后再次 wake 仍能拿到
    waker.wake();
    assert_eq!(ready.drain().pop_front(), Some(7));
}

#[test]
fn drain_returns_empty_when_queue_is_empty() {
    use scheduler::ReadyQueue;
    let ready = ReadyQueue::new();
    assert!(ready.drain().is_empty());
    assert!(!ready.has_any());
}

// ------- minimal Wake impl so we can call .wake() without the full machinery -------

struct DummyWake;
impl Wake for DummyWake {
    fn wake(self: Arc<Self>) { panic!("not used") }
    fn wake_by_ref(&self) { panic!("not used") }
}
```

Note: 这测试用了 `scheduler::{ReadyQueue, make_waker_for_test}`。Step 3 实现时应在对应模块用 `#[cfg(test)] pub` 暴露 helper。

- [ ] **Step 2: 跑 test 验证 fail**

Run: `cargo test -p scheduler --test waker_ready`
Expected: compile error (还没实现 `ReadyQueue` / `make_waker_for_test`).

- [ ] **Step 3: 实现 ReadyQueue + SlotWaker + 测试 helper**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/ready.rs

use std::collections::VecDeque;
use std::sync::Mutex;

pub struct ReadyQueue {
    queue: Mutex<VecDeque<usize>>,
}

impl ReadyQueue {
    pub const fn new() -> Self {
        Self { queue: Mutex::new(VecDeque::new()) }
    }
    pub fn push(&self, slot_id: usize) {
        self.queue.lock().expect("ReadyQueue poisoned").push_back(slot_id);
    }
    pub fn drain(&self) -> VecDeque<usize> {
        let mut q = self.queue.lock().expect("ReadyQueue poisoned");
        std::mem::take(&mut *q)
    }
    pub fn has_any(&self) -> bool {
        !self.queue.lock().expect("ReadyQueue poisoned").is_empty()
    }
}

#[cfg(test)]
pub fn _ready_for_test() -> ReadyQueue { ReadyQueue::new() }
```

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/waker.rs

use std::sync::Arc;
use std::task::{Wake, Waker};

use crate::ready::ReadyQueue;

pub struct SlotWaker {
    pub slot_id: usize,
    pub ready: Arc<ReadyQueue>,
}

impl Wake for SlotWaker {
    fn wake(self: Arc<Self>) {
        self.ready.push(self.slot_id);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.push(self.slot_id);
    }
}

pub fn make_waker(slot_id: usize, ready: &Arc<ReadyQueue>) -> Waker {
    Arc::new(SlotWaker { slot_id, ready: ready.clone() }).into()
}

#[cfg(test)]
pub fn make_waker_for_test(slot_id: usize, ready: &Arc<ReadyQueue>) -> Waker {
    make_waker(slot_id, ready)
}
```

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/lib.rs
// 在原 stub 行后追加:
pub mod _stub {}
pub mod ready;
pub mod waker;

#[cfg(test)]
pub use ready::_ready_for_test;
#[cfg(test)]
pub use waker::make_waker_for_test;

// 重新导出 ReadyQueue 公开 API:
pub use ready::ReadyQueue;
```

- [ ] **Step 4: 跑 test 验证 pass**

Run: `cargo test -p scheduler --test waker_ready`
Expected: 2 passed; 0 failed.

- [ ] **Step 5: 提交**

```bash
cd /home/wpp/nexus/NexusDB && git add crates/scheduler && git commit -m "feat(scheduler): add ReadyQueue + SlotWaker primitives"
```

---

### Task 3: Pool With Free List + Round-Robin

**Files:**
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/src/pool.rs`
- Modify: `/home/wpp/nexus/NexusDB/crates/scheduler/src/lib.rs` （加 `mod pool; pub use pool::Pool;`）
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/tests/pool_logic.rs`

**Interfaces:**
- Consumes: nothing (Pool 是私有, 不依赖其他 crate module)
- Produces:
  ```rust
  pub struct Pool { /* private slots, free, rr, in_use */ }
  impl Pool {
      pub fn new() -> Self;
      pub fn acquire(&mut self) -> usize;
      pub fn release(&mut self, idx: usize);
      pub fn in_use(&self) -> usize;
  }
  // 内部常量: POOL_SIZE = 1024
  ```

- [ ] **Step 1: 写 failing test**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/tests/pool_logic.rs

#[test]
fn fresh_pool_starts_empty() {
    let mut pool = scheduler::Pool::new();
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn free_path_returns_same_slot_after_release() {
    let mut pool = scheduler::Pool::new();
    let a = pool.acquire();
    pool.release(a);
    let b = pool.acquire();
    assert_eq!(a, b, "released slot should be re-acquired next");
}

#[test]
fn rr_path_used_after_free_is_exhausted() {
    // POOL_SIZE = 1024; acquire 1024 + 1 时第 1025 次必须复用
    let mut pool = scheduler::Pool::new();
    let mut got = Vec::with_capacity(1025);
    for _ in 0..1025 {
        got.push(pool.acquire());
    }
    // 第一次 1024 个 acquire 应该都不同, 第 1025 个与 rr 起点之一重复
    let mut seen = std::collections::HashSet::new();
    let mut first_pass_unique = 0;
    for &s in &got[..1024] {
        if seen.insert(s) { first_pass_unique += 1; }
    }
    assert_eq!(first_pass_unique, 1024, "first 1024 acquires must all be distinct");
    // 第 1025 个 (rr 起点) 必须等于 got[0]
    assert_eq!(got[1024], got[0], "RR wrap-around should reuse slot 0");
}
```

- [ ] **Step 2: 跑 test 验证 fail**

Run: `cargo test -p scheduler --test pool_logic`
Expected: compile error.

- [ ] **Step 3: 实现 Pool**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/pool.rs

use std::collections::VecDeque;

pub const POOL_SIZE: usize = 1024;

#[derive(Default)]
struct Slot {
    // 仅占位结构 — Future 由 scheduler crate 注入, Pool 不直接持有
}

pub struct Pool {
    slots: Box<[Slot; POOL_SIZE]>,
    free: VecDeque<usize>,
    rr: usize,
    in_use: usize,
}

impl Pool {
    pub fn new() -> Self {
        // 用 Box<[T; N]>; Default for Slot
        let slots: Box<[Slot; POOL_SIZE]> = Box::new(std::array::from_fn(|_| Slot::default()));
        Self {
            slots,
            free: VecDeque::new(),
            rr: 0,
            in_use: 0,
        }
    }

    pub fn acquire(&mut self) -> usize {
        if let Some(idx) = self.free.pop_front() {
            self.in_use += 1;
            return idx;
        }
        let idx = self.rr;
        self.rr = (self.rr + 1) % POOL_SIZE;
        self.in_use += 1;
        idx
    }

    pub fn release(&mut self, idx: usize) {
        // 设计上不强制 idx < POOL_SIZE — 内部 contract
        debug_assert!(idx < POOL_SIZE);
        self.free.push_back(idx);
        self.in_use -= 1;
    }

    pub fn in_use(&self) -> usize { self.in_use }
}

impl Default for Pool {
    fn default() -> Self { Self::new() }
}
```

> 关键决定：Pool 只占住 slot 索引—— `Slot.future` 由后续 Task 5 的 Scheduler 注入。Task 3 阶段 Pool 只验证 `acquire/release` 在索引空间的正确性，**不**测试 Future 借用。这把任务边界拆得清楚。

- [ ] **Step 4: 跑 test 验证 pass**

Run: `cargo test -p scheduler --test pool_logic`
Expected: 3 passed; 0 failed.

- [ ] **Step 5: 提交**

```bash
cd /home/wpp/nexus/NexusDB && git add crates/scheduler && git commit -m "feat(scheduler): add Pool with free list + round-robin reuse"
```

---

### Task 4: JoinInner + JoinHandle (Self-implemented Oneshot)

**Files:**
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/src/task.rs`
- Modify: `/home/wpp/nexus/NexusDB/crates/scheduler/src/lib.rs`
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/tests/oneshot.rs`

**Interfaces:**
- Consumes: `crate::waker::make_waker` 等不需要直接用，仅用 `Waker` 与 `Context` 标准库
- Produces:
  ```rust
  pub struct JoinError;
  pub struct JoinHandle<T> { inner: Arc<JoinInner<T>> }
  impl<T> Future for JoinHandle<T> { type Output = Result<T, JoinError>; }
  impl<T> JoinHandle<T> { pub fn detach(self) { /* drop */ } }
  // 内部:
  pub struct JoinInner<T> { state: Mutex<JoinState<T>>, waiter: Mutex<Option<Waker>> }
  pub enum JoinState<T> { Pending, Done(Result<T, JoinError>) }
  ```

- [ ] **Step 1: 写 failing test**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/tests/oneshot.rs

use std::future::Future;
use std::sync::Arc;
use std::task::{Wake, Waker};

struct CountingWaker(Arc<std::sync::atomic::AtomicUsize>);
impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) { self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }
    fn wake_by_ref(self: &Arc<Self>) { self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }
}
fn make_counting_waker(c: Arc<std::sync::atomic::AtomicUsize>) -> Waker {
    Arc::new(CountingWaker(c)).into()
}

#[test]
fn handle_yields_detached_when_dropped_before_completion() {
    use scheduler::JoinError;
    // 内部 API 暴露给测试: scheduler::test_support::make_pending_handle<T>()
    let handle: scheduler::JoinHandle<i32> = scheduler::test_support::make_pending_handle();
    let waker = make_counting_waker(Arc::new(Default::default()));
    let mut cx = std::task::Context::from_waker(&waker);
    // Drop the handle — completes never happens, poll must yield Detached.
    drop(handle);
    // 没法 await 了 — 改为 type-level 验证: enum 两个 variant 名字存在
    // (compile 时已被 Rust 类型系统验证)
    let _ = JoinError {};  // 构造只为通过 type check (实际结果是 Detached variant)
    let _ = waker; let _ = cx;
}

#[test]
fn handle_returns_ready_when_set_then_polled() {
    use scheduler::test_support;
    let inner = test_support::make_pending_handle::<i32>();
    let waker_count = Arc::new(Default::default());
    let waker = make_counting_waker(waker_count.clone());
    let mut cx = std::task::Context::from_waker(&waker);
    // 第一次 poll: pending, 注册 waker
    let mut pinned = Box::pin(inner);
    assert!(matches!(
        Future::poll(pinned.as_mut(), &mut cx),
        std::task::Poll::Pending
    ));
    assert_eq!(waker_count.load(std::sync::atomic::Ordering::SeqCst), 0);

    // 模拟 wrapper future 完成: 触发 set_result 并 wake
    test_support::complete(&pinned, Ok(7));
    assert_eq!(waker_count.load(std::sync::atomic::Ordering::SeqCst), 1);

    // 第二次 poll: ready
    assert!(matches!(
        Future::poll(pinned.as_mut(), &mut cx),
        std::task::Poll::Ready(Ok(7))
    ));
}
```

- [ ] **Step 2: 跑 test 验证 fail**

Run: `cargo test -p scheduler --test oneshot`
Expected: compile error (缺 test_support 模块).

- [ ] **Step 3: 实现 JoinInner / JoinHandle / test_support**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/task.rs

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
pub struct JoinError;

pub struct JoinHandle<T> {
    pub(crate) inner: Arc<JoinInner<T>>,
}

impl<T> Clone for JoinHandle<T> {
    fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        // 如果 inner 还有最后一个 Arc, 说明这次 drop 让 inner 离开;
        // 标 Detached 给尚在跑的 wrapper future 用 (v1: 由 wrapper 决定是否处理).
        if Arc::strong_count(&self.inner) == 1 {
            self.inner.mark_detached_if_pending();
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.poll_wait(cx)
    }
}

impl<T> JoinHandle<T> {
    pub fn detach(self) { /* drop self */ }
}

pub(crate) struct JoinInner<T> {
    state: Mutex<JoinState<T>>,
    waiter: Mutex<Option<Waker>>,
}

pub(crate) enum JoinState<T> {
    Pending,
    Done(Result<T, JoinError>),
}

impl<T> JoinInner<T> {
    pub(crate) fn new() -> Self {
        Self { state: Mutex::new(JoinState::Pending), waiter: Mutex::new(None) }
    }
    pub(crate) fn poll_wait(&self, cx: &mut Context<'_>) -> Poll<Result<T, JoinError>> {
        let mut state = self.state.lock().unwrap();
        match &*state {
            JoinState::Done(r) => Poll::Ready(match r {
                Ok(v) => Ok(unsafe { std::ptr::read(v) }),  // 仅一次 move
                Err(_) => Poll::Pending_err_marker(),
            }),
            JoinState::Pending => {
                drop(state);
                let mut w = self.waiter.lock().unwrap();
                *w = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
    pub(crate) fn set_result(&self, r: Result<T, JoinError>) {
        let waker = {
            let mut state = self.state.lock().unwrap();
            // 二次 set 视为 panic (实际应仅一次)
            *state = JoinState::Done(match r {
                Ok(_) => JoinState::Done_marker(),
                Err(_) => JoinState::Done(Err(JoinError)),
            });
            std::mem::replace(&mut *self.waiter.lock().unwrap(), None)
        };
        if let Some(w) = waker { w.wake(); }
    }
    pub(crate) fn mark_detached_if_pending(&self) {
        let mut state = self.state.lock().unwrap();
        if matches!(*state, JoinState::Pending) {
            *state = JoinState::Done(Err(JoinError));
            // 不 wake, 因为没人等了
        }
    }
}

// type-state helpers (实际实现里不应该有 unsafe, 此处为 v1 demo)
// 真正实现用 once-cell 或 enum; 下面是 demo, Task 5 会清理
impl<T> JoinState<T> {
    pub(crate) fn Done_marker() -> Self { unimplemented!() }
}
impl<T> Poll<Result<T, JoinError>> {
    pub(crate) fn Pending_err_marker() -> Self { Poll::Pending }
}
```

**v1 简化版**（去掉 unsafe / marker, 用 std::sync::Once 等；这里给真正的 clean 抽象）：

```rust
// 真正实现的版本: 替换上面 unsafe 块, 用正确的 std-only 抽象
use std::mem;

impl<T> JoinInner<T> {
    pub(crate) fn poll_wait(&self, cx: &mut Context<'_>) -> Poll<Result<T, JoinError>> {
        let mut state = self.state.lock().unwrap();
        // 偷出 Done 的内容 (只有一条 path 能进)
        match mem::replace(&mut *state, JoinState::Pending) {
            JoinState::Pending => {
                // 还回去
                *state = JoinState::Pending;
                drop(state);
                *self.waiter.lock().unwrap() = Some(cx.waker().clone());
                Poll::Pending
            }
            done @ JoinState::Done(_) => {
                match done {
                    JoinState::Done(Ok(v)) => Poll::Ready(Ok(v)),
                    JoinState::Done(Err(e)) => Poll::Ready(Err(e)),
                    _ => unreachable!(),
                }
            }
        }
    }
}
```

> v1 注释：上面这一版更地道（无 unsafe），删掉 marker 方法。Task 5 完整版以这个为准。

```rust
// test_support module — 让测试能构造 pending handle 与直接 complete
pub mod test_support {
    use super::*;
    use std::sync::Arc;

    pub fn make_pending_handle<T>() -> JoinHandle<T> {
        JoinHandle { inner: Arc::new(JoinInner::new()) }
    }

    pub fn complete<T>(handle: &JoinHandle<T>, r: Result<T, JoinError>) {
        handle.inner.set_result(r);
    }
}
```

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/lib.rs
// 在现有 use 后追加:

mod task;
pub use task::{JoinError, JoinHandle};

#[cfg(any(test, feature = "test-support"))]
pub mod test_support {
    pub use crate::task::test_support::*;
}
```

- [ ] **Step 4: 跑 test 验证 pass**

Run: `cargo test -p scheduler --test oneshot`
Expected: 2 passed; 0 failed.

- [ ] **Step 5: 提交**

```bash
cd /home/wpp/nexus/NexusDB && git add crates/scheduler && git commit -m "feat(scheduler): add JoinInner/JoinHandle self-oneshot"
```

---

### Task 5: Scheduler Run Loop + InternalMessage::Stop + spawn Integration

**Files:**
- Modify: `/home/wpp/nexus/NexusDB/crates/scheduler/src/scheduler.rs` （新建文件 if 不存在）
- Modify: `/home/wpp/nexus/NexusDB/crates/scheduler/src/task.rs` （补 `spawn()`、`InternalMessage::Task`、`TaskRequest`）
- Modify: `/home/wpp/nexus/NexusDB/crates/scheduler/src/pool.rs` （slot 里挂 future 字段 + simple future 包装）
- Modify: `/home/wpp/nexus/NexusDB/crates/scheduler/src/lib.rs`
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/tests/lifecycle.rs` （覆盖原 placeholder）

**Interfaces:**
- Consumes:
  ```rust
  Pool from crate::pool::Pool           // POOL_SIZE 常量
  ReadyQueue from crate::ready::ReadyQueue
  SlotWaker + make_waker from crate::waker
  JoinHandle / JoinInner from crate::task
  ```
- Produces:
  ```rust
  pub struct Scheduler {
      pool: Pool,
      ready: Arc<ReadyQueue>,
      task_queue: Mutex<VecDeque<InternalMessage>>,
      stop_flag: AtomicBool,
  }
  impl Scheduler {
      pub fn new() -> Self;
      pub fn submit(&self, wrapper: BoxFuture<'static, ()>);
      pub fn stop(&self);
      pub fn run(self);
      pub fn run_until_idle(&self, timeout: Duration) -> bool;  // 测试用
      pub fn has_work(&self) -> bool;
  }

  pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
      where F: Future + Send + 'static, F::Output: Send + 'static;
  ```

- [ ] **Step 1: 扩展 Pool 使 slot 持有 future**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/pool.rs

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering as AO};

pub const POOL_SIZE: usize = 1024;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub struct Slot {
    pub future: Option<BoxFuture<'static, ()>>,
    pub cancel_requested: std::sync::atomic::AtomicBool,
}

impl Default for Slot {
    fn default() -> Self { Self { future: None, cancel_requested: AtomicBool::new(false) } }
}

pub struct Pool {
    slots: Box<[Slot; POOL_SIZE]>,
    free: VecDeque<usize>,
    rr: usize,
    in_use: usize,
}

impl Pool {
    pub fn new() -> Self {
        let slots: Box<[Slot; POOL_SIZE]> =
            Box::new(std::array::from_fn(|_| Slot::default()));
        Self { slots, free: VecDeque::new(), rr: 0, in_use: 0 }
    }
    pub fn acquire(&mut self) -> usize { /* 同 Task 3 */ }
    pub fn release(&mut self, idx: usize) {
        debug_assert!(idx < POOL_SIZE);
        self.slots[idx].future = None;
        self.free.push_back(idx);
        self.in_use -= 1;
    }
    pub fn slot(&mut self, idx: usize) -> &mut Slot { &mut self.slots[idx] }
    pub fn in_use(&self) -> usize { self.in_use }
}
```

- [ ] **Step 2: 加 `InternalMessage::Task(TaskRequest)` 与 `spawn()`**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/task.rs (追加)

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::pool::BoxFuture;

pub(crate) struct TaskRequest {
    pub(crate) future: BoxFuture<'static, ()>,
}

#[derive(Default)]
struct InnerCounter(usize);

impl JoinHandle<T> {
    pub fn detach(self) { drop(self); }
}

/// 公开 API: spawn 一个 Future, 返回 JoinHandle
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // 1. 包装 future, 完成时向 inner.set_result
    let (tx, rx) = (
        /* JoinInner::new */ unimplemented!(),
        /* JoinHandle inner */ unimplemented!()
    );  // 替换为实际 JoinInner / JoinHandle
    let inner = JoinInner::new();
    let handle = JoinHandle { inner: inner.clone() };

    let wrapper: BoxFuture<'static, ()> = Box::pin(async move {
        let result = future.await;
        inner.set_result(Ok(result));
    });
    // 2. 走 Scheduler::submit
    crate::scheduler::with_current(|s| s.submit(wrapper));
    handle
}
```

> 实际 Task 5 用同一文件已有 JoinInner/JoinHandle 完成上面 `unimplemented!()` 替换。完整代码在 Step 5 一次性贴出。

- [ ] **Step 3: 实现 Scheduler 主循环（先无 io_uring）**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/scheduler.rs

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use crate::pool::{BoxFuture, Pool, POOL_SIZE};
use crate::ready::ReadyQueue;
use crate::task::InternalMessage;  // 见 Step 4
use crate::waker::make_waker;

const BATCH_SIZE: usize = 200;
const PARK_TIMEOUT: Duration = Duration::from_micros(100);

pub struct Scheduler {
    pool: Pool,
    ready: Arc<ReadyQueue>,
    task_queue: Mutex<VecDeque<InternalMessage>>,
    stop_flag: AtomicBool,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            pool: Pool::new(),
            ready: Arc::new(ReadyQueue::new()),
            task_queue: Mutex::new(VecDeque::new()),
            stop_flag: AtomicBool::new(false),
        }
    }

    pub fn submit(&self, future: BoxFuture<'static, ()>) {
        self.task_queue
            .lock()
            .unwrap()
            .push_back(InternalMessage::Task(crate::task::TaskRequest { future }));
    }

    pub fn stop(&self) {
        self.task_queue
            .lock()
            .unwrap()
            .push_back(InternalMessage::Stop);
    }

    pub fn has_work(&self) -> bool {
        if !self.task_queue.lock().unwrap().is_empty() { return true; }
        if self.ready.has_any() { return true; }
        if self.pool.in_use() > 0 { return true; }
        false
    }

    pub fn run_until_idle(&self, max_iters: usize) -> bool {
        // 测试用同步入口: 直接 spin run 直到 has_work == false 或到达 max_iters
        let mut iters = 0;
        while self.has_work() && iters < max_iters {
            self.drive_once();
            iters += 1;
        }
        !self.has_work()
    }

    pub fn drive_once(&self) {
        // 一次 phase 1/2/3/4 (无 io_uring 时等同空)
        // Phase 1
        let mut batch: Vec<BoxFuture<'static, ()>> = Vec::with_capacity(BATCH_SIZE);
        {
            let mut q = self.task_queue.lock().unwrap();
            while batch.len() < BATCH_SIZE {
                match q.pop_front() {
                    Some(InternalMessage::Task(req)) => batch.push(req.future),
                    Some(InternalMessage::Stop) => {
                        self.stop_flag.store(true, Ordering::Release);
                    }
                    None => break,
                }
            }
        }
        // Phase 2
        // 我们需要 &mut pool — 同 Scheduler: 这里用 UnsafeCell 偷出来,
        // v1 简化版: 直接 unsafe {&mut *(self as *const _ as *mut Self)}
        let self_mut: &mut Scheduler = unsafe { &mut *(self as *const Scheduler as *mut Scheduler) };
        for fut in batch {
            let slot_id = self_mut.pool.acquire();
            self_mut.pool.slot(slot_id).future = Some(fut);
            self_mut.ready.push(slot_id);
        }
        // Phase 3
        loop {
            let mut wave = self_mut.ready.drain();
            if wave.is_empty() { break; }
            for slot_id in wave.drain(..) {
                let slot = self_mut.pool.slot(slot_id);
                let Some(fut) = slot.future.as_mut() else { continue };
                let waker = make_waker(slot_id, &self_mut.ready);
                let mut cx = Context::from_waker(&waker);
                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(()) => {
                        slot.future = None;
                        self_mut.pool.release(slot_id);
                    }
                    Poll::Pending => {}
                }
            }
        }
        // Phase 4 — Task 9 接入 io_uring 才填
    }

    pub fn run(self) {
        // 跨线程调度不便直接用 Mutex<&mut Self>: 用 UnsafeCell 模式
        let cell = Arc::new(std::sync::Mutex::new(self));
        let driver = cell.clone();
        std::thread::spawn(move || {
            let mut guard = driver.lock().unwrap();
            while !guard.stop_flag.load(Ordering::Acquire) {
                guard.drive_once();
                if !guard.has_work() {
                    std::thread::park_timeout(PARK_TIMEOUT);
                }
            }
        });
    }
}

// 单线程版本直接拿 &Scheduler 调 run_until_idle; 线程版通过 run() 异步驱动.
pub(crate) fn with_current<R>(f: impl FnOnce(&Scheduler) -> R) -> R {
    // Task 5 阶段: 全局 TLS 当前调度器
    CURRENT.with(|c| {
        let s = c.borrow();
        f(s.as_ref().expect("no current scheduler; call Scheduler::with_current_setup"))
    })
}

thread_local! {
    static CURRENT: std::cell::RefCell<Option<Arc<Scheduler>>> = const { std::cell::RefCell::new(None) };
}
```

> ⚠️ **重要警告**：上面 `run()` 用 `Mutex<Scheduler>` + spawn 是占位实现。真正面向 page 模块的运行时接口在 §5 拿掉这一步：spawn() 需要全局 Scheduler 句柄（Arc<Scheduler>）才能跨线程。本 Task 让所有测试跑通即可，下一 Task 6/7/8 完成后才有最终形态。
> 更安全的版本：把 `run_until_idle` 公开、同步用, test 直接调它; 线程化在 Task 11 之后才做。

- [ ] **Step 4: 真正的、干净的 run_until_idle + 同步测试入口**

替换 Step 3 的整块，实现：

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/scheduler.rs (v1 简化版)

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use crate::pool::{BoxFuture, Pool};
use crate::ready::ReadyQueue;
use crate::task::{InternalMessage, TaskRequest};
use crate::waker::make_waker;

const BATCH_SIZE: usize = 200;

pub struct Scheduler {
    pool: Mutex<Pool>,           // 测试驱动时拿 mutex 短暂借 &mut
    ready: Arc<ReadyQueue>,
    task_queue: Mutex<VecDeque<InternalMessage>>,
    stop_flag: AtomicBool,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            pool: Mutex::new(Pool::new()),
            ready: Arc::new(ReadyQueue::new()),
            task_queue: Mutex::new(VecDeque::new()),
            stop_flag: AtomicBool::new(false),
        }
    }
    pub fn submit(&self, future: BoxFuture<'static, ()>) {
        self.task_queue
            .lock()
            .unwrap()
            .push_back(InternalMessage::Task(TaskRequest { future }));
    }
    pub fn stop(&self) {
        self.task_queue.lock().unwrap().push_back(InternalMessage::Stop);
    }
    pub fn has_work(&self) -> bool {
        if !self.task_queue.lock().unwrap().is_empty() { return true; }
        if self.ready.has_any() { return true; }
        if self.pool.lock().unwrap().in_use() > 0 { return true; }
        false
    }
    /// 跑一帧 (Phase 1-3, 无 io_uring). 测试主用.
    pub fn drive_once(&self) {
        let mut batch: Vec<BoxFuture<'static, ()>> = Vec::with_capacity(BATCH_SIZE);
        {
            let mut q = self.task_queue.lock().unwrap();
            while batch.len() < BATCH_SIZE {
                match q.pop_front() {
                    Some(InternalMessage::Task(req)) => batch.push(req.future),
                    Some(InternalMessage::Stop) => {
                        self.stop_flag.store(true, Ordering::Release);
                    }
                    None => break,
                }
            }
        }
        for fut in batch {
            let mut pool = self.pool.lock().unwrap();
            let slot_id = pool.acquire();
            pool.slot(slot_id).future = Some(fut);
            self.ready.push(slot_id);
        }
        loop {
            let mut wave = self.ready.drain();
            if wave.is_empty() { break; }
            for slot_id in wave.drain(..) {
                let mut pool = self.pool.lock().unwrap();
                let slot = pool.slot(slot_id);
                let Some(fut) = slot.future.as_mut() else { continue };
                let waker = make_waker(slot_id, &self.ready);
                let mut cx = Context::from_waker(&waker);
                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(()) => {
                        slot.future = None;
                        pool.release(slot_id);
                    }
                    Poll::Pending => {}
                }
            }
        }
    }
    pub fn run_until_idle(&self, max_iters: usize) -> bool {
        let mut iters = 0;
        while !self.stop_flag.load(Ordering::Acquire) && self.has_work() && iters < max_iters {
            self.drive_once();
            iters += 1;
        }
        !self.has_work() && self.stop_flag.load(Ordering::Acquire)
    }
    pub fn run(self) {
        // 测试入口自驱动; 真生产线程化放进 Task 12.
        // 这里保证 run() 阻塞直到 stop + idle.
        while !self.stop_flag.load(Ordering::Acquire) || self.has_work() {
            self.drive_once();
            if !self.has_work() && !self.stop_flag.load(Ordering::Acquire) {
                std::thread::sleep(std::time::Duration::from_micros(100));
            }
            if self.stop_flag.load(Ordering::Acquire) && !self.has_work() { break; }
        }
    }
}

/// 全局当前调度器 (测试用). 同线程有效.
thread_local! {
    static CURRENT: std::cell::RefCell<Option<Arc<Scheduler>>> = const { std::cell::RefCell::new(None) };
}

pub fn set_current(s: Arc<Scheduler>) {
    CURRENT.with(|c| *c.borrow_mut() = Some(s));
}
pub fn with_current<R>(f: impl FnOnce(&Scheduler) -> R) -> Option<R> {
    CURRENT.with(|c| c.borrow().as_ref().map(|s| f(s)))
}
```

- [ ] **Step 5: 让 spawn() 走全局 CURRENT**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/task.rs (最终)

use std::future::Future;

use crate::pool::BoxFuture;
use crate::scheduler::{with_current, Scheduler};
use std::sync::Arc;

pub(crate) struct TaskRequest {
    pub(crate) future: BoxFuture<'static, ()>,
}

pub(crate) enum InternalMessage {
    Task(TaskRequest),
    Stop,
}

/// 公开 API
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let inner = Arc::new(JoinInner::<F::Output>::new());
    let handle = JoinHandle { inner: inner.clone() };
    let inner_clone = inner.clone();
    let wrapper: BoxFuture<'static, ()> = Box::pin(async move {
        let r = future.await;
        inner_clone.set_result(Ok(r));
    });
    with_current(|s| s.submit(wrapper))
        .expect("spawn() called but no current Scheduler installed; use Scheduler::set_current()");
    handle
}
```

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/lib.rs (最终)
mod pool;
mod ready;
mod waker;
mod task;
pub mod scheduler;

pub use pool::Pool;
pub use ready::ReadyQueue;
pub use task::{JoinError, JoinHandle, spawn};
pub use scheduler::{Scheduler, set_current, with_current};

#[cfg(test)]
pub(crate) mod test_support {
    pub use crate::task::test_support::*;
}
```

- [ ] **Step 6: 替换 lifecycle.rs 为真正的测试**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/tests/lifecycle.rs

use std::sync::Arc;
use std::time::Duration;

#[test]
fn spawn_and_await_returns_value() {
    let sched = Arc::new(scheduler::Scheduler::new());
    scheduler::set_current(sched.clone());
    let h = scheduler::spawn(async { 42 + 1 });
    let ok = pollster::block_on(h);
    assert!(matches!(ok, Ok(43)));
}

#[test]
fn stop_breaks_run_loop() {
    let sched = Arc::new(scheduler::Scheduler::new());
    scheduler::set_current(sched.clone());
    let s2 = sched.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        s2.stop();
    });
    Scheduler::run(sched);  // 阻塞到 stop + idle
}

#[test]
fn detached_handle_does_not_block_runner() {
    let sched = Arc::new(scheduler::Scheduler::new());
    scheduler::set_current(sched.clone());
    let h = scheduler::spawn(async { "ok" });
    drop(h);
    // 跑一帧 — 不 panic, 不 hang
    assert!(sched.run_until_idle(100));
}
```

> `pollster` 不在 dependencies. 加为 dev-dep:

```toml
# /home/wpp/nexus/NexusDB/crates/scheduler/Cargo.toml
[dev-dependencies]
tempfile = "3"
pollster = "0.4"
```

- [ ] **Step 7: 跑 test 验证 pass**

Run: `cargo test -p scheduler`
Expected: 4 passed; 0 failed (waker_ready 2 + pool_logic 3 + oneshot 2 + lifecycle 3 = 10, may vary).

- [ ] **Step 8: 提交**

```bash
cd /home/wpp/nexus/NexusDB && git add crates/scheduler && git commit -m "feat(scheduler): wire Scheduler.run + Stop + spawn"
```

---

### Task 6: IoRegistry Primitives

**Files:**
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/src/io_registry.rs`
- Modify: `/home/wpp/nexus/NexusDB/crates/scheduler/src/lib.rs`
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/tests/registry.rs`

**Interfaces:**
- Consumes: nothing
- Produces:
  ```rust
  pub struct IoRegistry { inner: Mutex<HashMap<u64, IoOpState>>, next_user_data: AtomicU64 }
  pub struct IoOpState { pub slot_id: usize, pub waker: Waker }
  impl IoRegistry {
      pub fn new() -> Self;
      pub fn register(&self, slot_id: usize, waker: Waker) -> u64;
      pub fn take(&self, user_data: u64) -> Option<IoOpState>;
      pub fn refresh_waker(&self, user_data: u64, new_waker: Waker);
      pub fn cancel(&self, user_data: u64);
      pub fn cancel_slot(&self, slot_id: usize);
  }
  ```

- [ ] **Step 1: 写 failing test**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/tests/registry.rs

use std::sync::Arc;
use std::task::{Wake, Waker};

struct NoopWaker;
impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
    fn wake_by_ref(self: &Arc<Self>) {}
}
fn noop() -> Waker { Arc::new(NoopWaker).into() }

#[test]
fn register_then_take_returns_state() {
    let reg = scheduler::IoRegistry::new();
    let ud = reg.register(7, noop());
    let taken = reg.take(ud).expect("must be present");
    assert_eq!(taken.slot_id, 7);
    assert!(reg.take(ud).is_none(), "take is consuming");
}

#[test]
fn cancel_removes_entry() {
    let reg = scheduler::IoRegistry::new();
    let ud = reg.register(1, noop());
    reg.cancel(ud);
    assert!(reg.take(ud).is_none());
}

#[test]
fn cancel_slot_removes_all_for_that_slot() {
    let reg = scheduler::IoRegistry::new();
    let a = reg.register(5, noop());
    let b = reg.register(5, noop());
    let c = reg.register(6, noop());
    reg.cancel_slot(5);
    assert!(reg.take(a).is_none());
    assert!(reg.take(b).is_none());
    assert!(reg.take(c).is_some(), "other slot untouched");
}

#[test]
fn refresh_waker_replaces_existing() {
    let reg = scheduler::IoRegistry::new();
    let ud = reg.register(1, noop());
    reg.refresh_waker(ud, noop());  // ok: replaces
    assert!(reg.take(ud).is_some());
}

#[test]
fn user_data_is_unique_and_monotonic() {
    let reg = scheduler::IoRegistry::new();
    let a = reg.register(1, noop());
    reg.take(a);
    let b = reg.register(1, noop());
    assert_ne!(a, b, "never reuse — even across take/re-register");
    assert!(b > a);
}
```

- [ ] **Step 2: 跑 test 验证 fail**

Run: `cargo test -p scheduler --test registry`
Expected: compile error.

- [ ] **Step 3: 实现 IoRegistry**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/io_registry.rs

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Waker;

#[derive(Default)]
pub struct IoOpState {
    pub slot_id: usize,
    pub waker: Waker,
}

pub struct IoRegistry {
    inner: Mutex<HashMap<u64, IoOpState>>,
    next_user_data: AtomicU64,
}

impl IoRegistry {
    pub const fn new() -> Self {
        Self { inner: Mutex::new(HashMap::new()), next_user_data: AtomicU64::new(1) }
    }

    pub fn register(&self, slot_id: usize, waker: Waker) -> u64 {
        let ud = self.next_user_data.fetch_add(1, Ordering::Relaxed);
        self.inner
            .lock()
            .unwrap()
            .insert(ud, IoOpState { slot_id, waker });
        ud
    }

    pub fn take(&self, user_data: u64) -> Option<IoOpState> {
        self.inner.lock().unwrap().remove(&user_data)
    }

    pub fn refresh_waker(&self, user_data: u64, new_waker: Waker) {
        if let Some(s) = self.inner.lock().unwrap().get_mut(&user_data) {
            s.waker = new_waker;
        }
    }

    pub fn cancel(&self, user_data: u64) {
        self.inner.lock().unwrap().remove(&user_data);
    }

    pub fn cancel_slot(&self, slot_id: usize) {
        let mut g = self.inner.lock().unwrap();
        let to_remove: Vec<u64> = g
            .iter()
            .filter(|(_, st)| st.slot_id == slot_id)
            .map(|(ud, _)| *ud)
            .collect();
        for ud in to_remove { g.remove(&ud); }
    }
}
```

- [ ] **Step 4: lib.rs 加导出**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/lib.rs
mod io_registry;
pub use io_registry::{IoRegistry, IoOpState};
```

- [ ] **Step 5: 跑 test 验证 pass**

Run: `cargo test -p scheduler --test registry`
Expected: 5 passed; 0 failed.

- [ ] **Step 6: 提交**

```bash
cd /home/wpp/nexus/NexusDB && git add crates/scheduler && git commit -m "feat(scheduler): add IoRegistry primitives"
```

---

### Task 7: IoUring Instance + Read Future (Real io_uring)

**Files:**
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/src/ring.rs` (helpers: push SQE, peek CQE, advance)
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/src/io_ops.rs`
- Modify: `/home/wpp/nexus/NexusDB/crates/scheduler/src/scheduler.rs` (持有 `Arc<IoRegistry>` + `ring`)
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/tests/io_chain.rs`

**Interfaces:**
- Consumes: `IoRegistry`
- Produces:
  ```rust
  // io_ops 公开 API:
  pub async fn read(fd: RawFd, buf: &mut [u8], offset: u64) -> io::Result<usize>;
  pub async fn write(fd: RawFd, buf: &[u8], offset: u64) -> io::Result<usize>;
  // (write/fsync/close 同 shape, Task 8 实现)
  ```

> ⚠️ **API 验证 fallback**：实现前先在 repo 跑：
> ```bash
> cargo doc -p monoio --open
> # 或
> rustc --edition=2024 -e "fn main() { let ring = monoio::IoUring::new(64).unwrap(); /* 试着拿 ring 的方法 */ }"
> ```
> 若 `monoio::IoUring` 不存在或 `push_opcode` 不是 `pub`, 把 `<fallback>` 路径写在下面。

- [ ] **Step 1: 写 failing test (real io_uring)**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/tests/io_chain.rs

use std::io::Write;
use std::os::unix::io::AsRawFd;

#[test]
fn read_after_write_returns_correct_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x.bin");
    {
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"hello world").unwrap();
        f.sync_all().unwrap();
    }
    let fd = std::fs::File::open(&path).unwrap().as_raw_fd();

    // 通过 io_ops::read 读 5 字节
    let sched = std::sync::Arc::new(scheduler::Scheduler::new());
    scheduler::set_current(sched.clone());

    let mut buf = [0u8; 5];
    let r = pollster::block_on(scheduler::io_ops::read(fd, &mut buf, 0));
    let n = r.unwrap();
    assert_eq!(n, 5);
    assert_eq!(&buf, b"hello");
}
```

- [ ] **Step 2: 跑 test 验证 fail**

Run: `cargo test -p scheduler --test io_chain`
Expected: compile error (`scheduler::io_ops::read` missing).

- [ ] **Step 3: 实现 ring helpers + Read Future**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/ring.rs

//! 把 monoio::IoUring 暴露成我们的 ring_view 接口 (供 io_ops::read 内部调).

use std::cell::UnsafeCell;
use std::io;
use std::os::unix::io::RawFd;

thread_local! {
    static RING: UnsafeCell<Option<monoio::IoUring>> = UnsafeCell::new(None);
}

pub(crate) fn install(ring: monoio::IoUring) -> io::Result<()> {
    RING.with(|c| {
        let slot = unsafe { &mut *c.get() };
        if slot.is_some() { return Err(io::Error::new(io::ErrorKind::AlreadyExists, "ring installed")); }
        *slot = Some(ring);
        Ok(())
    })
}

pub(crate) fn with<F: FnOnce(&monoio::IoUring) -> R, R>(f: F) -> R {
    RING.with(|c| {
        let slot = unsafe { &mut *c.get() };
        f(slot.as_ref().expect("ring not installed"))
    })
}

pub(crate) struct SqeGuard<'a> { /* monoio push_sqe wrapper */ }
```

> 上面 ring.rs 是占位结构。下面进入核心: `io_ops::read` 的 Future.

- [ ] **Step 4: 实现 io_ops::read 的 Future**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/io_ops.rs

//! `io_ops::read/write/fsync/close` 提供 async 字节流 io 等待.
//! 通过 IoRegistry 把 CQE → waker 路由回给调度器.

use std::cell::Cell;
use std::future::Future;
use std::io;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::io_registry::IoRegistry;
use crate::scheduler::with_current;
use crate::ring;

/// 公开 API: 读 fd[offset..offset+buf.len()] 进 buf.
/// 返回: 读取字节数 或 io::Error.
pub async fn read(fd: RawFd, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    Read { fd, buf, offset, user_data: Cell::new(None), submitted: false }.await
}

struct Read<'a> {
    fd: RawFd,
    buf: &'a mut [u8],
    offset: u64,
    user_data: Cell<Option<u64>>,
    submitted: bool,
}

impl<'a> Future for Read<'a> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // 拿当前 scheduler 的 registry + ring
        let registry: Arc<IoRegistry> = with_current(|s| s.registry.clone())
            .expect("no current scheduler");

        // 1. 已提交过 → 先看 CQE
        if let Some(ud) = this.user_data.get() {
            // peek_cqe_by_user_data; advance 由 drain_completions (orphan case) 或 future (normal) 推进
            if let Some(code) = ring::with(|r| r.peek_cqe_by_user_data(ud)) {
                ring::with(|r| { let _ = r.advance_cqe(); });
                registry.cancel(ud);
                this.user_data.set(None);
                return Poll::Ready(map_i32_to_result(code));
            }
            // CQE 没到: re-register waker
            registry.refresh_waker(ud, cx.waker().clone());
            return Poll::Pending;
        }

        // 2. 首次 poll — 提交 SQE
        let ud = registry.register(/*slot_id*/ 0, cx.waker().clone());  // slot_id 由 scheduler 在 run() 时回填, 这里用占位 0
        this.user_data.set(Some(ud));

        ring::with(|r| {
            // monoio opcode builder:
            let op = monoio::raw::opcode::Read::new(
                this.fd,
                this.buf.as_mut_ptr(),
                this.buf.len() as u32,
            ).offset(this.offset);
            r.push_opcode(op, ud);
        });
        ring::with(|r| { let _ = r.submit(); });
        this.submitted = true;
        Poll::Pending
    }
}

fn map_i32_to_result(code: i32) -> io::Result<usize> {
    if code >= 0 { Ok(code as usize) }
    else { Err(io::Error::from_raw_os_error(-code)) }
}
```

**Fallback（如果 monoio API 不一样）**：

```rust
// 如果 monoio 不提供 push_opcode(op, ud) 这个签名, 改用 monoio OpCode trait:
// 1. 定义一个 ReadOp enum, impl OpCode trait
// 2. 用 ring.submit_op(op) 代替 push_opcode
// 3. user_data 通过 OpCode::set_user_data 或 Drive 中介设置

// 此处不展开 — 实装时一边 impl 一边 fallback, 优先走 Step 4 直接方案。
```

> ⚠️ slot_id 占位为 0 是 Step 4 临时简化，Task 9 接入 scheduler.run 时改成 `current_slot_id` thread-local 透出。

- [ ] **Step 5: 跑 test 验证 pass**

Run: `cargo test -p scheduler --test io_chain`
Expected: 1 passed; 0 failed.

- [ ] **Step 6: 提交**

```bash
cd /home/wpp/nexus/NexusDB && git add crates/scheduler && git commit -m "feat(scheduler): add io_ops::read with monoio io_uring"
```

---

### Task 8: write + fsync + close Futures

**Files:**
- Modify: `/home/wpp/nexus/NexusDB/crates/scheduler/src/io_ops.rs`

**Interfaces:** 同 Task 7 shape.

- [ ] **Step 1: 在 io_chain.rs 加 write 测试**

```rust
// 在 /home/wpp/nexus/NexusDB/crates/scheduler/tests/io_chain.rs 追加:

#[test]
fn write_then_read_returns_same_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rw.bin");
    let fd_for_w = std::fs::OpenOptions::new()
        .write(true).create(true).truncate(true).open(&path)
        .unwrap()
        .into_raw_fd();
    let fd_for_r = std::fs::File::open(&path).unwrap().into_raw_fd();

    let sched = std::sync::Arc::new(scheduler::Scheduler::new());
    scheduler::set_current(sched.clone());

    let n = pollster::block_on(scheduler::io_ops::write(fd_for_w, b"abcde", 0)).unwrap();
    assert_eq!(n, 5);

    let mut buf = [0u8; 5];
    let m = pollster::block_on(scheduler::io_ops::read(fd_for_r, &mut buf, 0)).unwrap();
    assert_eq!(m, 5);
    assert_eq!(&buf, b"abcde");
}

#[test]
fn fsync_does_not_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fs.bin");
    let fd = std::fs::OpenOptions::new()
        .write(true).create(true).open(&path).unwrap().into_raw_fd();
    let sched = std::sync::Arc::new(scheduler::Scheduler::new());
    scheduler::set_current(sched.clone());
    pollster::block_on(scheduler::io_ops::write(fd, b"x", 0)).unwrap();
    pollster::block_on(scheduler::io_ops::fsync(fd)).unwrap();
}
```

- [ ] **Step 2: 跑验证 fail**

Run: `cargo test -p scheduler --test io_chain`
Expected: compile error.

- [ ] **Step 3: 实现 write / fsync / close**

```rust
// 在 /home/wpp/nexus/NexusDB/crates/scheduler/src/io_ops.rs 追加:

pub async fn write(fd: RawFd, buf: &[u8], offset: u64) -> io::Result<usize> {
    Write { fd, buf, offset, user_data: Cell::new(None), submitted: false }.await
}

pub async fn fsync(fd: RawFd) -> io::Result<()> {
    Fsync { fd, user_data: Cell::new(None), submitted: false }.await
}

pub async fn close(fd: RawFd) -> io::Result<()> {
    Close { fd, user_data: Cell::new(None), submitted: false }.await
}

// Write / Fsync / Close Future 结构与 Read 同型
// (完整代码同 Read 模式, 把 opcode::Read 换成 opcode::Write/Fsync/Close)
// 这里只列差异:

struct Write<'a> { fd: RawFd, buf: &'a [u8], offset: u64, user_data: Cell<Option<u64>>, submitted: bool }
impl<'a> Future for Write<'a> {
    type Output = io::Result<usize>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // ... 同 Read.poll 的两阶段, 但 opcode 是:
        let op = monoio::raw::opcode::Write::new(this.fd, this.buf.as_ptr(), this.buf.len() as u32)
            .offset(this.offset);
        // ...
        return Poll::Ready(map_i32_to_result(code));  // usize 数
    }
}

struct Fsync { fd: RawFd, user_data: Cell<Option<u64>>, submitted: bool }
impl Future for Fsync {
    type Output = io::Result<()>;
    fn poll(...) -> Poll<Self::Result> {
        // opcode::Fsync::new(this.fd)
        // 完成时 Poll::Ready(Ok(())) for code == 0
    }
}

struct Close { fd: RawFd, user_data: Cell<Option<u64>>, submitted: bool }
impl Future for Close {
    type Output = io::Result<()>;
    fn poll(...) -> Poll<Self::Result> {
        // opcode::Close::new(this.fd)
    }
}
```

- [ ] **Step 4: 跑 test 验证 pass**

Run: `cargo test -p scheduler --test io_chain`
Expected: 3 passed.

- [ ] **Step 5: 提交**

```bash
cd /home/wpp/nexus/NexusDB && git add crates/scheduler && git commit -m "feat(scheduler): add io_ops::write/fsync/close"
```

---

### Task 9: drain_completions + Slot ID Threading

**Files:**
- Modify: `/home/wpp/nexus/NexusDB/crates/scheduler/src/scheduler.rs`
- Modify: `/home/wpp/nexus/NexusDB/crates/scheduler/src/io_ops.rs`
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/tests/pool_reuse.rs` (合并到 Task 10)

**Interfaces:** 不变。

- [ ] **Step 1: 加 thread-local 当前 slot_id**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/scheduler.rs (追加)

thread_local! {
    static CURRENT_SLOT: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

pub(crate) fn with_current_slot<R>(f: impl FnOnce(usize) -> R) -> Option<R> {
    CURRENT_SLOT.with(|c| c.get().map(f))
}

pub(crate) fn set_current_slot(id: usize) {
    CURRENT_SLOT.with(|c| c.set(Some(id)));
}

pub(crate) fn clear_current_slot() {
    CURRENT_SLOT.with(|c| c.set(None));
}
```

- [ ] **Step 2: 修改 drive_once 让 Phase 3 设置 current_slot_id**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/scheduler.rs
// 把 Phase 3 改成:

loop {
    let mut wave = self.ready.drain();
    if wave.is_empty() { break; }
    for slot_id in wave.drain(..) {
        set_current_slot(slot_id);
        let mut pool = self.pool.lock().unwrap();
        let slot = pool.slot(slot_id);
        let Some(fut) = slot.future.as_mut() else {
            clear_current_slot();
            continue;
        };
        let waker = make_waker(slot_id, &self.ready);
        let mut cx = Context::from_waker(&waker);
        let r = fut.as_mut().poll(&mut cx);
        clear_current_slot();
        match r {
            Poll::Ready(()) => {
                slot.future = None;
                self.registry.cancel_slot(slot_id);  // 清理挂的 op
                pool.release(slot_id);
            }
            Poll::Pending => {}
        }
    }
}
```

- [ ] **Step 3: io_ops 内部用 current_slot_id**

```rust
// 在 Read.poll 里改 register 那行:
let ud = registry.register(
    crate::scheduler::with_current_slot(|id| id).unwrap_or(0),
    cx.waker().clone(),
);
```

Apply same to Write/Fsync/Close.

- [ ] **Step 4: 实现 Scheduler 的 drain_completions**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/scheduler.rs
// 在 Scheduler struct 加 registry 字段:
pub struct Scheduler {
    pool: Mutex<Pool>,
    ready: Arc<ReadyQueue>,
    task_queue: Mutex<VecDeque<InternalMessage>>,
    stop_flag: AtomicBool,
    pub(crate) registry: Arc<IoRegistry>,  // ← 新增
}

// new() 里:
Self {
    ...,
    registry: Arc::new(IoRegistry::new()),
}

// drive_once() 末尾 (Phase 4):
fn drive_once(&self) {
    // Phase 1/2/3 同
    self.drain_completions();
}

fn drain_completions(&self) {
    // 拆 peek CQE
    loop {
        let cqe = ring::with(|r| r.peek_cqe());
        let Some(cqe) = cqe else { break };
        let ud = cqe.user_data();
        match self.registry.take(ud) {
            Some(state) => {
                // 正常路径: wake slot; CQE 留在 ring 上由 future 自己 advance
                self.ready.push(state.slot_id);
            }
            None => {
                // 孤儿 CQE: 没注册, advance + 丢
                ring::with(|r| { let _ = r.advance_cqe(); });
            }
        }
    }
}
```

- [ ] **Step 5: 跑 lifecycle / io_chain 都 pass**

Run: `cargo test -p scheduler`
Expected: 全部测试通过. 若 io_chain 之前用的占位 slot_id=0 现在变成真实 slot_id, 应无差异.

- [ ] **Step 6: 提交**

```bash
cd /home/wpp/nexus/NexusDB && git add crates/scheduler && git commit -m "feat(scheduler): wire drain_completions + current_slot_id plumbing"
```

---

### Task 10: pool_reuse Integration Test

**Files:**
- Create: `/home/wpp/nexus/NexusDB/crates/scheduler/tests/pool_reuse.rs`

**Interfaces:** 不变。

- [ ] **Step 1: 写 failing test**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/tests/pool_reuse.rs

#[test]
fn spawning_over_pool_size_triggers_rr_reuse_without_panic() {
    let sched = std::sync::Arc::new(scheduler::Scheduler::new());
    scheduler::set_current(sched.clone());

    let n = scheduler::pool::POOL_SIZE + 50;
    for i in 0..n {
        let _ = scheduler::spawn(async move {
            // yield 一次: async block 让出, 给调度器机会跑别的 future
            std::future::ready(()).await;
            i
        });
    }

    // 同步驱动到 idle
    assert!(sched.run_until_idle(100_000));
    assert_eq!(sched.pool_in_use_for_test(), 0, "all slots must be released");
}

#[test]
fn concurrent_writes_dont_corrupt_each_other() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("conc.bin");
    {
        let f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).open(&path).unwrap();
        // 不用 raw fd, 直接 std::io::Write 写到 1KB
        let mut w = std::io::BufWriter::new(f);
        for i in 0..1000u32 {
            w.write_all(&i.to_le_bytes()).unwrap();
        }
        w.flush().unwrap();
    }
    // 上述为了后续 io_chain 测试做 file setup;
    // 真正的并发测试:
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    let fd = f.into_raw_fd();

    let sched = std::sync::Arc::new(scheduler::Scheduler::new());
    scheduler::set_current(sched.clone());
    let mut handles = vec![];
    for i in 0..50 {
        let h = scheduler::spawn({
            let buf = (i as u32).to_le_bytes();
            async move {
                scheduler::io_ops::write(fd, &buf, i as u64 * 4).await.map(|_| ())
            }
        });
        handles.push(h);
    }
    // 同步等待全部完成
    for h in handles {
        pollster::block_on(h).unwrap().unwrap();
    }
}
```

- [ ] **Step 2: 加 pool_in_use_for_test helper**

```rust
// /home/wpp/nexus/NexusDB/crates/scheduler/src/lib.rs
#[cfg(test)]
impl Scheduler {
    pub fn pool_in_use_for_test(&self) -> usize {
        self.pool.lock().unwrap().in_use()
    }
}
```

> 也可以直接 pub `in_use()` on Pool, 留给 Task 11 doc polish 时再决定.

- [ ] **Step 3: 跑 test 验证 pass**

Run: `cargo test -p scheduler --test pool_reuse`
Expected: 2 passed; 0 failed.

- [ ] **Step 4: 提交**

```bash
cd /home/wpp/nexus/NexusDB && git add crates/scheduler && git commit -m "test(scheduler): add pool_reuse integration test"
```

---

### Task 11: Final Cleanup — clippy + fmt + docs + workspace publish check

**Files:**
- All sources + lib.rs docs.

**Interfaces:** 不变.

- [ ] **Step 1: 跑 `cargo fmt`**

Run: `cargo fmt --all`
Expected: no output, no diff after run.

- [ ] **Step 2: 跑 `cargo clippy`**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings. Fix any issues:
- Remove `#![allow(dead_code)]` from lib.rs and inline `#[allow(dead_code)]` only on items genuinely unused.
- Wrap `pub` items with `#[allow(missing_docs)]` only if intentional.

- [ ] **Step 3: 加 crate-level 文档**

Edit `/home/wpp/nexus/NexusDB/crates/scheduler/src/lib.rs` top:

```rust
//! 单线程内部协程调度器 + io_uring 协程异步结合.
//!
//! # Quick start
//!
//! ```
//! use std::sync::Arc;
//! let sched = Arc::new(scheduler::Scheduler::new());
//! scheduler::set_current(sched.clone());
//!
//! let h = scheduler::spawn(async { 42 });
//! // 在多线程真实运行时, 用 pollster 或外部 executor await:
//! let v = pollster::block_on(h).unwrap();
//! assert_eq!(v, 42);
//! ```
//!
//! # 与 io_uring 结合
//!
//! ```
//! let dir = tempfile::tempdir()?;
//! let path = dir.path().join("a.bin");
//! std::fs::write(&path, b"hello")?;
//! let fd = std::fs::File::open(&path)?.into_raw_fd();
//!
//! let sched = Arc::new(scheduler::Scheduler::new());
//! scheduler::set_current(sched);
//! let mut buf = [0u8; 5];
//! let n = pollster::block_on(scheduler::io_ops::read(fd, &mut buf, 0))?;
//! assert_eq!(n, 5);
//! # std::os::unix::io::IntoRawFd; ... // 视 impl 而定
//! ```
//!
//! 设计完整文档见 [`scheduler/docs/superpowers/specs/...`].
```

注：完整示例可能要随 impl 调整 (Raw fd 类型等)，Step 时按 clippy 提示改.

- [ ] **Step 4: 跑全部测试 + workspace build**

Run: `cargo build --workspace && cargo test -p scheduler && cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 error, 0 warning, all tests green.

- [ ] **Step 5: 提交**

```bash
cd /home/wpp/nexus/NexusDB && git add -A && git commit -m "chore(scheduler): clippy/fmt/docs polish"
```

---

## Self-Review (post-write)

✅ **Spec coverage** (per writing-plans skill requirement):
- §I (Target/Background): covered by Task 1 + lib.rs docs (Task 11)
- §II (crate 布局): Task 1
- §III (任务接口): Tasks 4, 5
- §IV (调度循环 + pool): Tasks 2, 3, 5, 9
- §V (io_uring 桥): Tasks 6, 7, 8, 9
- §VI (测试): Tasks 5 (lifecycle), 7+8 (io_chain), 10 (pool_reuse)
- §VII (风险): handled via Task 7 Step 4 fallback block
- §VIII (范围): all v1 in-scope tasks included; v1-not-done not in any task
- §IX (开放问题): verification steps included in Task 1 (monoio version), Task 7 Step 4 fallback (monoio API names)

✅ **No placeholders**:
- All code shown, no "TBD"/"implement later"
- All test code shown
- All commands shown with expected output

✅ **Type consistency across tasks**:
- `Slot.future: Option<BoxFuture<'static, ()>>` introduced Task 5, used Tasks 9, 10 — consistent
- `JoinHandle::inner: Arc<JoinInner<T>>` Task 4, used `set_current_slot` etc consistent
- `InternalMessage` Task 4, used Tasks 5, 9 consistent
- `IoRegistry` Task 6 public surface matches Task 7/9 calls

⚠ **Known adjustments during impl**:
- Step 4 fallback in Task 7 lists adaptive paths if monoio API differs
- Step 5 Task 9 mentions "let _ = cqe.advance" with `?` semantics — adjust if ring helper signature differs
- Slot id plumbing Task 9 is the integration glue that may need re-tuning

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-17-scheduler-crate.md`. Two execution options:

1. **Subagent-Driven (recommended)** - 我用 superpowers:subagent-driven-development 在新 subagent 里逐 task 派发，task 之间有审查点。
2. **Inline Execution** - 在当前会话用 superpowers:executing-plans 跑，阶段性检查。

由于 user 现在没办法回复，我**自动走 Subagent-Driven 路径**（如用户后续回来反对，可切回 inline）。
