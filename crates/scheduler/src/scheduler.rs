//! Scheduler: 单线程协程调度器 + io_uring 桥.
//!
//! **设计**:
//! - 调度器 `!Send`, 所有方法 `&mut self`, 强制调度线程唯一.
//! - 跨线程入口仅 `task_queue` 一个 Mutex (真正必要的同步).
//! - 跨线程 stop 信号通过 `StopHandle` (克隆出 `Arc<AtomicBool>`, 直接 store).
//! - 其他内部状态全部 owned, 无 Mutex:
//!   - pool: Pool
//!   - ready: Rc<RefCell<VecDeque>> (让 SlotWaker 持有引用)
//!   - registry: IoRegistry
//!   - ring: IoUring
//!
//! ## 调度循环 (drive_once)
//!
//! ```text
//! Phase 1: drain task_queue (最多 BATCH_SIZE=200)
//! Phase 2: pool.acquire → push to ready
//! Phase 3: drain ready → poll 每个 future → Ready 则 release slot
//! Phase 4: drain CQE → submit_and_wait(1) — 让内核完成至少 1 个 IO
//! ```
//!
//! ## hang 修复
//!
//! 旧版 Phase 4 仅 `ring.completion()` 不阻塞等. 当所有 future 都 Pending 且
//! 尚无 CQE 就绪时, `has_work() == false`, `run_until_idle` 提前退出 → 测试 hang.
//! 新版在 Phase 4: 有 in-flight IO 时调用 `submit_and_wait(1)` (内核必然完成, 不真阻塞),
//! 否则让出 CPU 100us 避免 spin.

use std::cell::Cell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread::ThreadId;
use std::time::Duration;

use crate::io_registry::IoRegistry;
use crate::pool::{BoxFuture, Pool};
use crate::ready::{self, ReadyQueueHandle};
use crate::task::{InternalMessage, TaskRequest};
use crate::waker::make_waker;

const BATCH_SIZE: usize = 200;
const PARK_TIMEOUT: Duration = Duration::from_micros(100);
/// ⭐ G0: 每个调度 wave 至多 poll 的低优先级协程数 (后台任务限额).
const LOW_PRIO_BUDGET: usize = 1;
const IO_URING_ENTRIES: u32 = 1024;
/// Cancel SQE 的 CQE 不属于任何 task；IoRegistry user_data 从 1 开始分配。
pub(crate) const CANCEL_CQE_USER_DATA: u64 = 0;

pub struct Scheduler {
    pool: Pool,
    /// ReadyQueue 由 Rc 持有, SlotWaker 也持有 Rc clone, 共享同一对象.
    ready: ReadyQueueHandle,
    task_queue: Mutex<VecDeque<InternalMessage>>,
    /// 调度线程读取 (驱动循环里), 其他线程也可写 (通过 StopHandle).
    stop_flag: Arc<AtomicBool>,
    pub(crate) registry: IoRegistry,
    pub(crate) ring: io_uring::IoUring,
    /// ⭐ 批量提交: SQ ring 里是否有 push 了但还没 submit 的 SQE.
    /// `submit_sqe!` push 后置 true; 所有 CQ 扫描路径 (poll_cqe / drain) 前
    /// 调 `flush_sq()` 一次 submit 提交全部 — 驱动循环攒批 (少 enter), 同步
    /// 忙等路径 (block_on_io) 也能保证 SQE 不被滞留 (正确性).
    pub(crate) sq_pending: bool,
    /// 首次驱动时绑定；后续从另一线程驱动会立即失败，而非无声破坏 Rc/RefCell 契约。
    driver_thread: Option<ThreadId>,
}

/// 跨线程 stop 句柄 (Send). 克隆后任意线程可调 `stop()` 设置 flag,
/// 调度线程下一帧检测到后退出 run().
#[derive(Clone)]
pub struct StopHandle {
    flag: Arc<AtomicBool>,
}

impl StopHandle {
    pub fn stop(&self) {
        self.flag.store(true, Ordering::Release);
    }
}

impl Scheduler {
    pub fn new() -> Self {
        let ring = io_uring::IoUring::new(IO_URING_ENTRIES).expect("io_uring setup failed");
        Self::from_ring(ring)
    }

    /// 创建带 SQPOLL 的 Scheduler (T18c).
    ///
    /// `sqpoll_ms`: 内核 SQPOLL 线程空闲超时 (ms). 0 = 禁用 SQPOLL.
    /// 当 `sqpoll_ms > 0` 时, 内核线程自旋轮询 SQ 队列, 减少 submit syscall.
    ///
    /// ## 要求
    /// - Linux kernel ≥ 5.11 (非特权用户也支持)
    /// - 多消耗 1 个 CPU 核心 (内核线程)
    pub fn new_with_sqpoll(sqpoll_ms: u32) -> Self {
        let ring = if sqpoll_ms > 0 {
            let mut builder = io_uring::IoUring::builder();
            builder.setup_sqpoll(sqpoll_ms);
            builder
                .build(IO_URING_ENTRIES)
                .expect("io_uring setup with SQPOLL failed")
        } else {
            io_uring::IoUring::new(IO_URING_ENTRIES).expect("io_uring setup failed")
        };
        Self::from_ring(ring)
    }

    fn from_ring(ring: io_uring::IoUring) -> Self {
        let stop_flag = Arc::new(AtomicBool::new(false));
        Self {
            pool: Pool::new(),
            ready: ready::new_handle(),
            task_queue: Mutex::new(VecDeque::new()),
            stop_flag,
            registry: IoRegistry::new(),
            ring,
            sq_pending: false,
            driver_thread: None,
        }
    }

    pub fn submit(&self, future: BoxFuture<'static, ()>) {
        self.submit_with_priority(future, false);
    }

    /// ⭐ G0: 带优先级提交. low_priority=true 的协程在每个 wave 内
    /// 排在普通协程之后, 且每 wave 至多 poll `LOW_PRIO_BUDGET` 个.
    pub fn submit_with_priority(&self, future: BoxFuture<'static, ()>, low_priority: bool) {
        self.task_queue
            .lock()
            .unwrap()
            .push_back(InternalMessage::Task(TaskRequest {
                future,
                low_priority,
            }));
    }

    pub fn stop(&self) {
        self.task_queue
            .lock()
            .unwrap()
            .push_back(InternalMessage::Stop);
    }

    /// 获取 Send 句柄, 可被任意线程 clone 并调 stop().
    pub fn stop_handle(&self) -> StopHandle {
        StopHandle {
            flag: self.stop_flag.clone(),
        }
    }

    /// 获取 &mut IoUring, 供 `FdPool::acquire` 等跨 crate 调用.
    pub fn ring_mut(&mut self) -> &mut io_uring::IoUring {
        &mut self.ring
    }

    pub fn has_work(&self) -> bool {
        if !self.task_queue.lock().unwrap().is_empty() {
            return true;
        }
        if ready::has_any(&self.ready) {
            return true;
        }
        if self.pool.in_use() > 0 {
            return true;
        }
        // 有 in-flight io_uring SQE (还没 CQE) 也要算 work, 否则 driver 会误判空闲退出.
        if !self.registry.is_empty() {
            return true;
        }
        false
    }

    pub fn drive_once(&mut self) {
        // 设置 CURRENT Rc, 让 io_ops 通过 with_current 拿 &mut self.
        // SAFETY: self 在整个 drive_once 调用期间活着, borrow 在函数结束时释放.
        // drive_until_idle 已经在外部 borrow_mut 持有, 此时 CURRENT 已设置.
        self.drive_once_inner();
    }

    fn drive_once_inner(&mut self) {
        self.bind_driver_thread();
        // === Phase 1/2: admission queue → 空闲 slot ===
        // 满载时保留队首 task，绝不能回绕覆盖仍在运行的 future。
        {
            let mut q = self.task_queue.lock().unwrap();
            let mut admitted = 0;
            while admitted < BATCH_SIZE {
                match q.pop_front() {
                    Some(InternalMessage::Task(req)) => {
                        let Some(slot_id) = self.pool.acquire() else {
                            q.push_front(InternalMessage::Task(req));
                            break;
                        };
                        let slot = self.pool.slot(slot_id);
                        slot.future = Some(req.future);
                        slot.low_priority = req.low_priority;
                        ready::push(&self.ready, slot_id);
                        admitted += 1;
                    }
                    Some(InternalMessage::Stop) => {
                        self.stop_flag.store(true, Ordering::Release);
                    }
                    None => break,
                }
            }
        }

        // === Phase 3: 把 ready 里的 future 全部 poll ===
        // 关键: poll 期间释放 self 的 &mut borrow, 让 io_ops.poll 能 borrow_mut.
        // 我们 clone Rc<ReadyQueue> 用于构造 waker, 把 future take 出 slot
        // (用 mem::replace), poll 期间 slot borrow 已释放.
        let ready_rc = Rc::clone(&self.ready);
        // ⭐ G0: 本次 drive 内被限额推迟的低优先级 slot (循环结束后才回 ready,
        // 避免同一次 drive 内 drain→回填→drain 死循环).
        let mut deferred_low: Vec<usize> = Vec::new();
        loop {
            let wave = ready::drain(&self.ready);
            if wave.is_empty() {
                break;
            }
            let run = self.partition_wave(wave, &mut deferred_low);
            for slot_id in run {
                set_current_slot(slot_id);
                // take 出 future (slot borrow 在 take 后立即释放).
                let fut = self.pool.slot(slot_id).future.take();
                let Some(mut fut) = fut else {
                    clear_current_slot();
                    continue;
                };
                let waker = make_waker(slot_id, &ready_rc);
                let mut cx = Context::from_waker(&waker);
                let r = fut.as_mut().poll(&mut cx);
                match r {
                    Poll::Ready(()) => {
                        self.registry.cancel_slot(slot_id);
                        self.pool.release(slot_id);
                        clear_current_slot();
                    }
                    Poll::Pending => {
                        // 把 future 放回 slot
                        self.pool.slot(slot_id).future = Some(fut);
                        clear_current_slot();
                    }
                }
            }
        }
        // 推迟的低优先级 slot 回 ready, 下次 drive 再跑
        for slot_id in deferred_low {
            ready::push(&self.ready, slot_id);
        }

        // === Phase 4: drain CQE + submit_and_wait (修复 hang) ===
        self.drain_completions_and_wait();
    }

    /// ⭐ G0: wave 分区 — 普通协程在前; 低优先级排后且至多取
    /// `LOW_PRIO_BUDGET` 个, 超额的进 `deferred` (caller 负责回 ready).
    /// 无低优先级协程时行为与分区前完全一致 (no-op).
    fn partition_wave(&mut self, wave: VecDeque<usize>, deferred: &mut Vec<usize>) -> Vec<usize> {
        let mut run: Vec<usize> = Vec::with_capacity(wave.len());
        let mut lows: Vec<usize> = Vec::new();
        for slot_id in wave {
            if self.pool.slot(slot_id).low_priority {
                lows.push(slot_id);
            } else {
                run.push(slot_id);
            }
        }
        let mut budget = LOW_PRIO_BUDGET;
        for slot_id in lows {
            if budget > 0 {
                budget -= 1;
                run.push(slot_id);
            } else {
                deferred.push(slot_id);
            }
        }
        run
    }

    /// 生产路径: 阻塞等待内核完成至少 1 个 IO. 调用于 `drive_once`.
    /// 安全的前提: 有 in-flight SQE (registry 非空) 时才调用, 内核必然完成.
    fn drain_completions_and_wait(&mut self) {
        // ⭐ 批量提交: 先提交本轮攒下的 SQE, 否则内核不会执行 → CQE 永不出现.
        self.flush_sq();
        self.drain_completions();
        if !self.registry.is_empty() {
            // submit_and_wait(1) 阻塞直到 1 个 CQE 到达.
            // io-uring 0.6 没有 timeout 参数; 但我们只在 registry 非空时调用,
            // 内核会完成 SQE, 所以不会真正死锁.
            let _ = self.ring.submit_and_wait(1);
            self.drain_completions();
        } else if !ready::has_any(&self.ready) {
            std::thread::sleep(PARK_TIMEOUT);
        }
    }

    /// 非阻塞 poll + submit 版本 (SchedHandle 驱动用). 不挂起线程, 适合测试.
    fn drain_completions_with_submit(&mut self) {
        // ⭐ 批量提交: 把本轮攒下的 SQE 一次性 submit (少 io_uring_enter).
        self.flush_sq();
        self.drain_completions();

        // 检查 SQ ring 是否还有未消费的 SQE.
        // 用 submission() 拿 SQ (调用 .len()), drop 时会 sync head.
        let inflight_sqe = {
            let sq = self.ring.submission();
            sq.len()
        };
        if inflight_sqe > 0 {
            let _ = self.ring.submit_and_wait(1);
            self.drain_completions();
        }
    }

    /// ⭐ 批量提交: 有 pending SQE (push 了没 submit) 时一次性提交.
    /// 由 `submit_sqe!` push 后置位; 所有 CQ 扫描路径 (poll_cqe / drain)
    /// 扫描前调用, 保证 SQE 不被滞留 (block_on_io 同步忙等路径正确性关键).
    pub(crate) fn flush_sq(&mut self) {
        if self.sq_pending {
            let _ = self.ring.submit();
            self.sq_pending = false;
        }
    }

    /// ⭐ 批量提交: 标记有 pending SQE 待提交 (submit_sqe! push 后调用).
    pub(crate) fn mark_sq_pending(&mut self) {
        self.sq_pending = true;
    }

    /// 取消一个已提交但其 future 已被 drop 的 io_uring 请求。
    ///
    /// 仅从 registry 删除不足以释放内核中的 PollAdd/IO；必须同时提交
    /// IORING_OP_ASYNC_CANCEL。取消操作本身使用保留 user_data=0，CQE 到达后会被
    /// drain 路径自然忽略。
    pub(crate) fn cancel_submitted_io(&mut self, target_user_data: u64) {
        self.registry.cancel(target_user_data);
        let entry = io_uring::opcode::AsyncCancel::new(target_user_data)
            .build()
            .user_data(CANCEL_CQE_USER_DATA);
        let mut pushed = false;
        while !pushed {
            let mut sq = self.ring.submission();
            match unsafe { sq.push(&entry) } {
                Ok(()) => pushed = true,
                Err(_) => {
                    sq.sync();
                    drop(sq);
                    let _ = self.ring.submit();
                }
            }
        }
        self.mark_sq_pending();
    }

    fn drain_completions(&mut self) {
        let cq = self.ring.completion();
        let mut to_wake: Vec<usize> = Vec::new();
        for cqe in cq {
            let ud = cqe.user_data();
            let result = cqe.result();
            // mark_completed 不移除, 让 io_ops.poll_cqe 自己取结果 (take_result).
            if self.registry.mark_completed(ud, result) {
                if let Some(st) = self.registry.inner_peek(ud) {
                    to_wake.push(st.slot_id);
                }
            } else if ud != CANCEL_CQE_USER_DATA {
                self.registry.record_unknown_cqe();
            }
        }
        for slot_id in to_wake {
            ready::push(&self.ready, slot_id);
        }
    }

    /// 提取 ready queue 中所有 slot 及其 future (owned). 让外部 poll future 时
    /// 不持有 self borrow, 允许 io_ops 通过 with_current borrow_mut.
    ///
    /// **关键**: ready queue 可能为空 (CQE 已经走另一条路径), 但 pool 里仍有
    /// 挂起的 future. 这里扫描所有 in_use slot, 收集 future 为 Some 的 slot.
    pub fn extract_pending(&mut self) -> Vec<(usize, BoxFuture<'static, ()>)> {
        let mut result = Vec::new();
        let wave: VecDeque<usize> = ready::drain(&self.ready);
        // ⭐ G0: 分区 + 低优先级限额; 超额的直接回 ready (下轮 iter 再取,
        // budget 重置). budget >= 1 保证 wave 非空时 run 非空, 兜底扫描不会
        // 被 deferred 误触发.
        let mut deferred: Vec<usize> = Vec::new();
        let run = self.partition_wave(wave, &mut deferred);
        for slot_id in deferred {
            ready::push(&self.ready, slot_id);
        }
        for slot_id in run {
            if let Some(fut) = self.pool.slot(slot_id).future.take() {
                result.push((slot_id, fut));
            }
        }
        // 兜底: ready 为空时, 扫描 pool 里 future Some 的 slot.
        if result.is_empty() {
            let in_use = self.pool.in_use();
            for slot_id in 0..in_use.max(1024) {
                if let Some(fut) = self.pool.slot(slot_id).future.take() {
                    result.push((slot_id, fut));
                }
            }
        }
        result
    }

    /// 把 task_queue 中的新 task 装入 pool + push 到 ready (供 SchedHandle 三阶段驱动).
    pub fn drain_task_queue_phase(&mut self) {
        {
            let mut q = self.task_queue.lock().unwrap();
            let mut admitted = 0;
            while admitted < BATCH_SIZE {
                match q.pop_front() {
                    Some(InternalMessage::Task(req)) => {
                        let Some(slot_id) = self.pool.acquire() else {
                            q.push_front(InternalMessage::Task(req));
                            break;
                        };
                        let slot = self.pool.slot(slot_id);
                        slot.future = Some(req.future);
                        slot.low_priority = req.low_priority;
                        ready::push(&self.ready, slot_id);
                        admitted += 1;
                    }
                    Some(InternalMessage::Stop) => {
                        self.stop_flag.store(true, Ordering::Release);
                    }
                    None => break,
                }
            }
        }
    }

    pub fn run_until_idle(&mut self, max_iters: usize) -> bool {
        let mut iters = 0;
        while !self.stop_flag.load(Ordering::Acquire) && self.has_work() && iters < max_iters {
            self.drive_once();
            iters += 1;
        }
        !self.has_work()
    }

    /// ⭐ T19: 带 predicate 的 run_until_idle.
    ///
    /// 每 iter 后检查 `predicate()`. true 立即返回 (不跑完 max_iters).
    /// 用法: caller 线程上 run 一个 scheduler 跑 `ReplyFuture` 等 shard 完成
    /// 通过 reply_bus (跨线程 mpmc) 让 caller 的 waker 唤醒, 触发 polling.
    pub fn run_until<F: FnMut() -> bool>(&mut self, predicate: F, max_iters: usize) -> bool {
        let mut pred = predicate;
        let mut iters = 0;
        while !self.stop_flag.load(Ordering::Acquire)
            && !pred()
            && iters < max_iters
            && self.has_work()
        {
            self.drive_once();
            iters += 1;
        }
        pred()
    }

    pub fn run(&mut self) {
        loop {
            self.drive_once();
            if self.stop_flag.load(Ordering::Acquire) && !self.has_work() {
                break;
            }
        }
    }

    fn bind_driver_thread(&mut self) {
        let current = std::thread::current().id();
        match self.driver_thread {
            Some(owner) => assert_eq!(
                owner, current,
                "a Scheduler may only be driven from its original thread"
            ),
            None => self.driver_thread = Some(current),
        }
    }

    /// ⭐ 把 slot  push 进 ready queue (审计新 P1-15: 供 io_ops 的 poll_cqe!
    /// 在 mark **其他** task 的 CQE 后唤醒 owner — 与 drain_completions 的
    /// 入队路径一致).
    pub(crate) fn wake_slot(&mut self, slot_id: usize) {
        crate::ready::push(&self.ready, slot_id);
    }

    /// ⭐ 2026-10-17 (审计新 P1-16): 有界等待指定 ud 的 CQE 到达.
    ///
    /// **用途**: Read/Write future 被 drop (取消) 时, 内核在 cancel 生效前
    /// 可能仍在 DMA 读写调用方的 buffer. 必须等到该 op 的 CQE 到达
    /// (正常结果或 -ECANCELED), DMA 确已结束, buffer 才能安全交还调用方
    /// 复用 — 否则数据竞争 (UB).
    ///
    /// 扫描期间也会 mark/唤醒**其他** ud (与 drain_completions/poll_cqe!
    /// 对齐). 超时 (正常情况 CQE 微秒级到达) 后放弃等待, 尽力而为.
    pub(crate) fn drain_cqe_until(&mut self, ud: u64, timeout: std::time::Duration) {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            self.flush_sq();
            let mut found = false;
            let mut cross_woke: Vec<usize> = Vec::new();
            {
                let mut cq = self.ring.completion();
                cq.sync();
                for cqe in cq {
                    let u = cqe.user_data();
                    let r = cqe.result();
                    if u == ud {
                        found = true;
                        self.registry.mark_completed(u, r);
                    } else if self.registry.mark_completed(u, r)
                        && let Some(st) = self.registry.inner_peek(u)
                    {
                        cross_woke.push(st.slot_id);
                    }
                }
            }
            for slot_id in cross_woke {
                self.wake_slot(slot_id);
            }
            // 已到达 (本轮或此前被其他扫描 mark) → 结束
            if found || self.registry.has_result(ud) {
                return;
            }
            if std::time::Instant::now() >= deadline {
                return;
            }
            std::thread::sleep(std::time::Duration::from_micros(100));
        }
    }

    pub fn io_registry_stats(&self) -> crate::io_registry::IoRegistryStats {
        self.registry.stats()
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

// ---- thread-local: 当前 scheduler (Rc-based, 让 spawn 找得到) ----

thread_local! {
    /// 当前线程的 scheduler Rc clone. 由 SchedHandle::set_current 设置.
    /// io_ops 通过 with_current 拿 Rc, 然后 borrow_mut 拿到 &mut Scheduler.
    static CURRENT: std::cell::RefCell<Option<std::rc::Rc<std::cell::RefCell<Scheduler>>>> =
        const { std::cell::RefCell::new(None) };
}

/// SchedHandle::set_current 用: 设当前 thread 的 CURRENT.
pub(crate) fn set_current_scheduler_via_rc(s: std::rc::Rc<std::cell::RefCell<Scheduler>>) {
    CURRENT.with(|c| *c.borrow_mut() = Some(s));
}

pub(crate) fn clear_current_scheduler() {
    CURRENT.with(|c| *c.borrow_mut() = None);
}

/// io_ops / spawn 用: 通过 thread-local 拿 &mut Scheduler.
/// 闭包返回后 borrow 立即释放, 不会跨 await 持有.
///
/// **关键**: 用 try_borrow_mut 而非 borrow_mut — drive_until_idle 持有 borrow_mut 时,
/// io_ops.poll 触发的 with_current 应该返回 None 或等待; 不应该 panic.
/// 当前选择: try_borrow_mut 失败时 panic (driver 线程上不该有竞争).
pub fn with_current<R>(f: impl FnOnce(&mut Scheduler) -> R) -> Option<R> {
    // TLS 析构阶段 drop future 仍可能尝试清理已提交 IO；此时 scheduler/ring 已在
    // 同一析构链中释放，不能因访问已销毁的 CURRENT 再次 panic/abort。
    CURRENT
        .try_with(|c| {
            let rc = c.borrow();
            let cell = rc.as_ref()?;
            let mut s = cell.try_borrow_mut().ok()?;
            Some(f(&mut s))
        })
        .ok()
        .flatten()
}

// SAFETY: handle 只能以唯一所有权 move 到 driver 线程；运行时会在首次 drive 后绑定
// thread id。它不是 Sync，不能借用共享给多个线程并发驱动。
pub struct SchedHandle(pub std::rc::Rc<std::cell::RefCell<Scheduler>>);
// SAFETY: 见上.
unsafe impl Send for SchedHandle {}

// ⭐ Manual Clone (既支持 Clone 也让我们能 clone Rc 共享 scheduler).
impl Clone for SchedHandle {
    fn clone(&self) -> Self {
        Self(std::rc::Rc::clone(&self.0))
    }
}

impl SchedHandle {
    pub fn new(s: Scheduler) -> Self {
        let rc = std::rc::Rc::new(std::cell::RefCell::new(s));
        Self(rc)
    }
    /// 设当前 scheduler Rc 到 thread-local. 必须在 driver 线程调.
    /// SAFETY: 需保证 self 不会被 drop, 直到 clear.
    pub fn set_current(&self) {
        set_current_scheduler_via_rc(self.0.clone());
    }
    pub fn into_inner(self) -> std::cell::RefCell<Scheduler> {
        std::rc::Rc::try_unwrap(self.0)
            .ok()
            .expect("SchedHandle has multiple owners")
    }

    /// ⭐ 协程 worker 用: 获取 StopHandle, 可被任意线程调 stop() 停止调度.
    pub fn stop_handle(&self) -> StopHandle {
        self.0.borrow().stop_handle()
    }

    /// 跨线程驱动. SchedHandle 是 Send, 可 move 到新线程.
    ///
    /// 每次迭代**临时 borrow**, drive_once 完成后释放 borrow, 让 wrapper poll 时无冲突.
    pub fn drive_until_idle(self, max_iters: usize) -> bool {
        self.0.borrow_mut().bind_driver_thread();
        let mut total_iters = 0usize;
        for _ in 0..max_iters {
            total_iters += 1;
            // Phase A: 临时 borrow_mut, 提取待 poll 的 future + ready Rc, release borrow.
            // **关键**: 必须在 extract_pending 之前调 drain_task_queue_phase, 否则
            // task_queue 里的新任务永远进不了 pool + ready, 调度卡死.
            let (work, ready_rc): (Vec<(usize, BoxFuture<'static, ()>)>, ReadyQueueHandle) = {
                let mut s = self.0.borrow_mut();
                s.drain_task_queue_phase();
                let hw = s.has_work();
                crate::trace!(
                    "iter={total_iters} phase=A has_work={hw} ready_len={} registry={} in_use={}",
                    crate::ready::len(&s.ready),
                    s.registry.len(),
                    s.pool.in_use()
                );
                if !hw {
                    (Vec::new(), Rc::clone(&s.ready))
                } else {
                    let work = s.extract_pending();
                    let rc = Rc::clone(&s.ready);
                    crate::trace!(
                        "iter={total_iters} extract_pending got={} futures",
                        work.len()
                    );
                    (work, rc)
                }
            };
            if work.is_empty() {
                // 即便 work 空, 也可能有 in-flight IO (registry 非空).
                // 再 drain 一次 CQE 让 await 的 future 完成.
                let mut s = self.0.borrow_mut();
                crate::trace!(
                    "iter={total_iters} phase=A work_empty in_use={} registry={}",
                    s.pool.in_use(),
                    s.registry.len()
                );
                if !s.registry.is_empty() {
                    s.drain_completions_with_submit();
                }
                // 再判一次: 如果 has_work 还在 (CQE 唤醒的 slot 进 ready 了), 继续循环.
                if s.has_work() {
                    continue;
                }
                crate::trace!("iter={total_iters} drive complete after {total_iters} iters");
                break;
            }

            // Phase B: poll 每个 future. 这一步**不持有** self.0 borrow_mut,
            // 允许 io_ops 通过 with_current borrow_mut.
            let mut requeue = Vec::new();
            let mut completed = Vec::new();
            crate::trace!("iter={total_iters} phase=B polling {} futures", work.len());
            for (slot_id, mut fut) in work {
                set_current_slot(slot_id);
                let waker = make_waker(slot_id, &ready_rc);
                let mut cx = Context::from_waker(&waker);
                let r = fut.as_mut().poll(&mut cx);
                crate::trace!(
                    "iter={total_iters} phase=B slot={} → {}",
                    slot_id,
                    match r {
                        Poll::Ready(()) => "Ready",
                        Poll::Pending => "Pending",
                    }
                );
                match r {
                    Poll::Ready(()) => {
                        completed.push(slot_id);
                        clear_current_slot();
                    }
                    Poll::Pending => {
                        requeue.push((slot_id, fut));
                        clear_current_slot();
                    }
                }
            }

            // Phase C: 重新 borrow_mut, 提交完成的 slot + 重新挂起 pending 的 future.
            let mut s = self.0.borrow_mut();
            for slot_id in completed {
                s.registry.cancel_slot(slot_id);
                s.pool.release(slot_id);
            }
            for (slot_id, fut) in requeue {
                s.pool.slot(slot_id).future = Some(fut);
            }
            // 同样处理 task_queue 中的新 task + drain CQE (non-blocking).
            s.drain_task_queue_phase();
            s.drain_completions_with_submit(); // non-blocking: ring.submit() + poll
        }
        true
    }
}

// ---- thread-local: 当前正在 poll 的 slot_id (供 io_ops 注册时拿到) ----

thread_local! {
    static CURRENT_SLOT: Cell<Option<usize>> = const { Cell::new(None) };
}

pub(crate) fn set_current_slot(id: usize) {
    CURRENT_SLOT.with(|c| c.set(Some(id)));
}

pub(crate) fn clear_current_slot() {
    CURRENT_SLOT.with(|c| c.set(None));
}

pub(crate) fn with_current_slot<R>(f: impl FnOnce(usize) -> R) -> Option<R> {
    CURRENT_SLOT.with(|c| c.get().map(f))
}

/// 当前协程的 slot_id (= task_id). 用于 `unpark` 唤醒本协程 (park 机制).
/// 必须在调度线程上被 poll 的协程内调用 (即 spawn 出来的 task).
pub fn current_task_id() -> usize {
    with_current_slot(|id| id).expect("current_task_id() called outside scheduler poll context")
}

#[cfg(test)]
mod p1_15_tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Context;
    use std::time::Duration;

    /// ⭐ 审计新 P1-15: `poll_cqe!` 的批量 CQ 扫描会消费**其他** task 的 CQE
    /// (drive_once 先 poll 后 drain), 必须把 owner slot 入 ready — 否则 owner
    /// 永远 park 且 CQE 已被消费, 后续 drain 看到空 CQ (永久死锁).
    ///
    /// 白盒构造: 只 poll B 的 future (A 不被 poll), 让 B 的扫描消费 A 的
    /// CQE, 然后断言 A 的 slot 已入 ready.
    #[test]
    fn cross_task_cqe_mark_enqueues_ready() {
        let mkpipe = || {
            let mut fds = [0i32; 2];
            assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
            (fds[0], fds[1])
        };
        let (r1, w1) = mkpipe();
        let (r2, w2) = mkpipe();

        let a_done = Arc::new(AtomicBool::new(false));
        let a2 = a_done.clone();

        let sched = SchedHandle::new(Scheduler::new());
        sched.set_current();

        // A (slot 0): poll(pipe1) — 提交 SQE 后 park
        crate::spawn_on(&sched, async move {
            let _ = crate::io_ops::poll(r1, libc::POLLIN).await;
            a2.store(true, Ordering::Release);
        });
        sched.clone().drive_until_idle(4096);
        assert!(!a_done.load(Ordering::Acquire), "pipe1 未写, A 不应完成");

        // A 的 io 完成 → CQE 到达 (未被 drain)
        let v: u64 = 1;
        unsafe { libc::write(w1, &v as *const u64 as *mut libc::c_void, 8) };
        std::thread::sleep(Duration::from_millis(100));

        // B (slot 1): poll(pipe2) — 永不完成
        crate::spawn_on(&sched, async move {
            let _ = crate::io_ops::poll(r2, libc::POLLIN).await;
        });

        // 白盒第一步 (持 borrow): B 入 pool+ready, 取出 B 的 future
        let (b_slot, mut b_fut, ready_rc) = {
            let rc = CURRENT.with(|c| c.borrow().clone().expect("current"));
            let mut s = rc.borrow_mut();
            s.drain_task_queue_phase();
            let mut work = s.extract_pending();
            assert_eq!(work.len(), 1, "ready 里应只有 B (A 已 park)");
            let (slot, fut) = work.remove(0);
            (slot, fut, Rc::clone(&s.ready))
        };
        // 白盒第二步 (释放 borrow): 只 poll B — 其 CQ 扫描会消费 A 的 CQE
        let waker = make_waker(b_slot, &ready_rc);
        let mut cx = Context::from_waker(&waker);
        set_current_slot(b_slot);
        // 第一次 poll: 注册 + 提交 SQE (不扫 CQ); 第二次: poll_cqe! 扫 CQ —
        // 此时会消费 A 的 CQE.
        let p1 = b_fut.as_mut().poll(&mut cx);
        let p2 = b_fut.as_mut().poll(&mut cx);
        clear_current_slot();
        assert!(
            p1.is_pending() && p2.is_pending(),
            "pipe2 无数据, B 应 Pending"
        );

        // 白盒第三步: B 放回 slot, 检查 ready (修复后 A 已被 B 的扫描入队)
        let rc2 = CURRENT.with(|c| c.borrow().clone().expect("current"));
        let mut s = rc2.borrow_mut();
        s.pool.slot(b_slot).future = Some(b_fut);
        let ready_len = crate::ready::len(&s.ready);
        drop(s);

        assert!(
            ready_len >= 1,
            "P1-15: B 的扫描消费了 A 的 CQE 但未把 A 入队 ready (修复前 A 永久挂起)"
        );

        // 端到端确认: 继续驱动, A 必须完成
        let t0 = std::time::Instant::now();
        while !a_done.load(Ordering::Acquire) {
            sched.clone().drive_until_idle(4096);
            assert!(
                t0.elapsed() < Duration::from_secs(5),
                "A 未被唤醒 — P1-15 死锁"
            );
        }
        unsafe {
            libc::close(r1);
            libc::close(w1);
            libc::close(r2);
            libc::close(w2);
        }
    }

    /// ⭐ 审计新 P1-16: Read future 在 IO 在途时被 drop, 必须**有界**等待
    /// CQE (内核 DMA 可能仍在写 buffer) 后交还 buffer — 旧实现只发异步
    /// cancel 就返回, 调用方立即可复用 buffer → 数据竞争 (UB).
    #[test]
    fn dropped_inflight_read_waits_bounded_and_drains_registry() {
        let mut fds = [0i32; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let (r, w) = (fds[0], fds[1]);

        let sched = SchedHandle::new(Scheduler::new());
        sched.set_current();

        let mut buf = [0u8; 64];
        let mut fut = Box::pin(crate::io_ops::read(r, &mut buf, 0));
        // 首次 poll: 提交 SQE (pipe 无数据 → 在途)
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(fut.as_mut().poll(&mut cx).is_pending());

        let before = CURRENT
            .with(|c| c.borrow().clone().expect("current"))
            .borrow()
            .io_registry_stats();
        // drop 在途 future: 必须快速返回 (有界等待), 不得挂死
        let t0 = std::time::Instant::now();
        drop(fut);
        let drop_elapsed = t0.elapsed();
        assert!(
            drop_elapsed < Duration::from_secs(2),
            "drop 在途 read future 耗时 {drop_elapsed:?} — 疑似无界等待"
        );

        // registry 应已排空 (CQE 被消费, 无残留 pending entry)
        let after = CURRENT
            .with(|c| c.borrow().clone().expect("current"))
            .borrow()
            .io_registry_stats();
        assert_eq!(after.registered, before.registered, "drop 不应留新注册");
        assert!(
            after.completed > before.completed || after.cancelled > before.cancelled,
            "在途 op 的 CQE 应已被 drain (completed/cancelled 增加)"
        );

        // buffer 交还后可安全复用 (无 sanitizer 报告即通过)
        buf = [0xABu8; 64];
        assert_eq!(buf[0], 0xAB);
        unsafe {
            libc::close(r);
            libc::close(w);
        }
    }
}
