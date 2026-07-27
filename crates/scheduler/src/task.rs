//! Task spawn / JoinHandle.
//!
//! **设计**: `JoinInner` 零 Mutex.
//!
//! ## 为什么不需要 Mutex
//!
//! `JoinInner` 共享在 `wrapper Future` (调度线程 poll) 和 `JoinHandle::poll_wait`
//! (主测试线程 poll) 之间. 但所有访问都在单线程上:
//! - wrapper poll → set_result: 在调度线程
//! - JoinHandle.poll_wait: 用户代码 await, 通常也在调度线程 (除非用户主动
//!   pollster::block_on 跨线程 poll — 这种情况下 waker wake 之后 JoinHandle
//!   poll 也在同一调度线程)
//!
//! 用 `UnsafeCell` 替代 Mutex, 标记同步边界 (调用方必须保证单线程访问).

use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use crate::pool::BoxFuture;

#[derive(Debug, Clone, Copy)]
pub struct JoinError;

pub struct JoinHandle<T> {
    pub(crate) inner: Arc<JoinInner<T>>,
}

impl<T> Clone for JoinHandle<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
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
    pub fn detach(self) {
        // drop self — Drop impl handles cleanup
    }
}

pub(crate) struct JoinInner<T> {
    state: UnsafeCell<JoinState<T>>,
    waiter: UnsafeCell<Option<Waker>>,
}

// SAFETY: JoinInner 自身 !Send, 但 Arc<JoinInner<T>> 通过 Arc<JoinInner<T>>: Send 派生.
// 所有调用方在调度线程上 poll, 跨线程仅通过 spawn -> 调度线程 waker 唤醒 JoinHandle.
// JoinHandle 在不同线程 poll 是非法用例, 这里只支持单线程 JoinHandle poll.
// 如果未来需要多线程 poll, 加回 Mutex 即可.
unsafe impl<T: Send> Send for JoinInner<T> {}

pub(crate) enum JoinState<T> {
    Pending,
    Done(Result<T, JoinError>),
}

impl<T> JoinInner<T> {
    pub(crate) fn new() -> Self {
        Self {
            state: UnsafeCell::new(JoinState::Pending),
            waiter: UnsafeCell::new(None),
        }
    }

    /// SAFETY: 调用方必须保证单线程访问. (driver 线程 + 主线程 `block_on` 时
    /// 调度线程和 block_on 线程通过 channel 同步, 实际只在调度线程访问.)
    pub(crate) fn poll_wait(&self, cx: &mut Context<'_>) -> Poll<Result<T, JoinError>> {
        // SAFETY: 单线程访问语义.
        let state = unsafe { &mut *self.state.get() };
        match state {
            JoinState::Pending => {
                // SAFETY: 同上.
                let waiter = unsafe { &mut *self.waiter.get() };
                *waiter = Some(cx.waker().clone());
                Poll::Pending
            }
            JoinState::Done(Ok(_)) => {
                let v = match std::mem::replace(state, JoinState::Pending) {
                    JoinState::Done(Ok(v)) => v,
                    _ => unreachable!(),
                };
                Poll::Ready(Ok(v))
            }
            JoinState::Done(Err(e)) => Poll::Ready(Err(*e)),
        }
    }

    pub(crate) fn set_result(&self, r: Result<T, JoinError>) {
        // SAFETY: 单线程访问.
        let state = unsafe { &mut *self.state.get() };
        *state = JoinState::Done(r);
        // SAFETY: 同上.
        let waiter = unsafe { &mut *self.waiter.get() };
        if let Some(w) = waiter.take() {
            w.wake();
        }
    }

    pub(crate) fn mark_detached_if_pending(&self) {
        // SAFETY: 单线程访问.
        let state = unsafe { &mut *self.state.get() };
        if matches!(*state, JoinState::Pending) {
            *state = JoinState::Done(Err(JoinError));
        }
        let waiter = unsafe { &mut *self.waiter.get() };
        if let Some(w) = waiter.take() {
            w.wake();
        }
    }
}

pub(crate) struct TaskRequest {
    pub(crate) future: BoxFuture<'static, ()>,
    /// ⭐ G0: 低优先级 (后台任务). 装 pool 时写入 slot.low_priority.
    pub(crate) low_priority: bool,
}

pub(crate) enum InternalMessage {
    Task(TaskRequest),
    Stop,
}

/// 公开 API: spawn 一个 Future, 返回 JoinHandle.
///
/// wrapper Future: 直接包 inner future + set_result, 不跨 await 持有 borrow.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let inner = Arc::new(JoinInner::<F::Output>::new());
    let handle = JoinHandle {
        inner: inner.clone(),
    };
    let wrapper: BoxFuture<'static, ()> = Box::pin(async move {
        let r = future.await;
        inner.set_result(Ok(r));
    });
    crate::scheduler::with_current(|s| s.submit(wrapper))
        .expect("spawn() called but no current Scheduler installed; use SchedHandle::set_current() on the driver thread first");
    handle
}

/// 通过 SchedHandle 显式 spawn (不需要 thread-local).
/// 由测试在主线程调, 但 wrapper 在 driver 线程 poll.
pub fn spawn_on<F>(handle: &crate::scheduler::SchedHandle, future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    spawn_on_with_priority(handle, future, false)
}

/// ⭐ G0: 低优先级 spawn (后台任务: compact/统计/预热).
///
/// 与 `spawn_on` 同构, 但协程在每个调度 wave 内排在普通协程之后,
/// 且每 wave 至多 poll `LOW_PRIO_BUDGET` 个 — 不影响前台请求延迟.
pub fn spawn_on_low<F>(handle: &crate::scheduler::SchedHandle, future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    spawn_on_with_priority(handle, future, true)
}

fn spawn_on_with_priority<F>(
    handle: &crate::scheduler::SchedHandle,
    future: F,
    low_priority: bool,
) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let inner = Arc::new(JoinInner::<F::Output>::new());
    let join = JoinHandle {
        inner: inner.clone(),
    };
    let wrapper: BoxFuture<'static, ()> = Box::pin(async move {
        let r = future.await;
        inner.set_result(Ok(r));
    });
    // 通过 handle.0 直接 borrow_mut 提交.
    handle.0.borrow().submit_with_priority(wrapper, low_priority);
    join
}

pub mod test_support {
    use super::*;

    pub fn make_pending_handle<T>() -> JoinHandle<T> {
        JoinHandle {
            inner: Arc::new(JoinInner::new()),
        }
    }

    pub fn complete<T>(handle: &JoinHandle<T>, r: Result<T, JoinError>) {
        handle.inner.set_result(r);
    }
}
