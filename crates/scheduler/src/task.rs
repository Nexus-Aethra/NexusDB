//! Task spawn / JoinHandle.
//!
//! **设计**: `JoinInner` 零 Mutex — 结果槽的单线程约束由**类型系统**保证,
//! 不靠调用方纪律.
//!
//! ## 单线程不变量 (⭐ 审计 P2-6 修复, 2026-10-19)
//!
//! 共享指针用 `Rc` 而非 `Arc`: `Rc<T>` 本身就 `!Send + !Sync`, 于是
//!
//! - `Rc<JoinInner<T>>`: `!Send` + `!Sync`
//! - `JoinHandle<T>` (只含这一个字段): `!Send` + `!Sync`
//!
//! `set_result` (调度线程 poll wrapper) 与 `poll_wait` (caller poll
//! `JoinHandle`) 因此**不可能**发生在两个线程上: `JoinHandle` 移出创建线程在
//! 编译期即被拒, 连 `&handle` 跨线程也不行 (那需要 `JoinHandle: Sync`)。两个
//! `UnsafeCell` 不存在并发访问, 所以无需 Mutex — 这正是本模块零 Mutex 的根据.
//!
//! 为什么 `Rc` 是sound的: `SchedHandle` 已是 `!Send` (审计 P2-5), 所以
//! `spawn` / `spawn_on` 只可能在 driver 线程上调用, wrapper future 从不跨线程
//! 移动; 调度器自身也是 `!Send` (`Rc<RefCell<Scheduler>>`). 整条链同线程.
//!
//! 旧实现的问题: `unsafe impl<T: Send> Send for JoinInner<T> {}` 的 SAFETY
//! 注释声称 "`Arc<JoinInner<T>>` 通过 `Arc: Send` 派生" — 该结论**是错的**
//! (`Arc<T>: Send` 要求 `T: Send + Sync`, 而 `UnsafeCell` 永不 `Sync`), 所以
//! 那个 unsafe impl 既冗余又用假声明为一个其实由 `!Sync` 偶然保证的不变量背书,
//! 会诱导后来者据此补 `unsafe impl Sync` 或换成 `Sync` 状态实现 — 那才会把
//! 竞争变成真 UB.

use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use crate::pool::BoxFuture;

#[derive(Debug, Clone, Copy)]
pub struct JoinError;

pub struct JoinHandle<T> {
    pub(crate) inner: Rc<JoinInner<T>>,
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
        if Rc::strong_count(&self.inner) == 1 {
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

// 无 `unsafe impl Send/Sync for JoinInner`: 单线程约束是**结构性**的 —
// 共享指针是 `Rc` (`!Send + !Sync`), `UnsafeCell` 也 `!Sync`, 任一条都足以让
// `JoinHandle` 无法跨线程. 详见模块头.

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

    /// 结果未就绪时登记 waker.
    ///
    /// SAFETY: 两个 `UnsafeCell` 的 `&mut` 解引用不构成数据竞争 —
    /// `Rc<JoinInner<T>>` 是 `!Send + !Sync`, 持有它的 `JoinHandle` 无法跨线程
    /// 移动或共享, 故 caller 与 `set_result` 必然同线程 (见模块头).
    pub(crate) fn poll_wait(&self, cx: &mut Context<'_>) -> Poll<Result<T, JoinError>> {
        let state = unsafe { &mut *self.state.get() };
        match state {
            JoinState::Pending => {
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

    /// SAFETY: 同 `poll_wait` — 单线程由 `Rc` (`!Send + !Sync`) 保证.
    pub(crate) fn set_result(&self, r: Result<T, JoinError>) {
        let state = unsafe { &mut *self.state.get() };
        *state = JoinState::Done(r);
        let waiter = unsafe { &mut *self.waiter.get() };
        if let Some(w) = waiter.take() {
            w.wake();
        }
    }

    /// SAFETY: 同 `poll_wait` — 单线程由 `Rc` (`!Send + !Sync`) 保证.
    pub(crate) fn mark_detached_if_pending(&self) {
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
    let inner = Rc::new(JoinInner::<F::Output>::new());
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
    let inner = Rc::new(JoinInner::<F::Output>::new());
    let join = JoinHandle {
        inner: inner.clone(),
    };
    let wrapper: BoxFuture<'static, ()> = Box::pin(async move {
        let r = future.await;
        inner.set_result(Ok(r));
    });
    // 通过 crate 内访问器提交 (字段对外私有).
    handle
        .rc()
        .borrow()
        .submit_with_priority(wrapper, low_priority);
    join
}

pub mod test_support {
    use super::*;

    pub fn make_pending_handle<T>() -> JoinHandle<T> {
        JoinHandle {
            inner: Rc::new(JoinInner::new()),
        }
    }

    pub fn complete<T>(handle: &JoinHandle<T>, r: Result<T, JoinError>) {
        handle.inner.set_result(r);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 结果槽握手: Pending → 登记 waker → set_result → Ready 取值 → 再次 Pending.
    #[test]
    fn join_inner_handoff_roundtrip() {
        let inner = Rc::new(JoinInner::<u32>::new());
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        assert!(
            inner.poll_wait(&mut cx).is_pending(),
            "未 set_result 前应 Pending"
        );
        inner.set_result(Ok(7));
        match inner.poll_wait(&mut cx) {
            Poll::Ready(Ok(v)) => assert_eq!(v, 7),
            other => panic!("set_result 后应 Ready(Ok): {other:?}"),
        }
        // 值被取走后回到 Pending — 同一结果不会被交付两次
        assert!(
            inner.poll_wait(&mut cx).is_pending(),
            "Ready 之后不得重复交付结果"
        );
    }

    /// 未完成的结果被 detach → Ready(Err).
    #[test]
    fn mark_detached_turns_pending_into_error() {
        let inner = Rc::new(JoinInner::<u32>::new());
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(inner.poll_wait(&mut cx).is_pending());

        inner.mark_detached_if_pending();
        assert!(
            matches!(inner.poll_wait(&mut cx), Poll::Ready(Err(_))),
            "detach 后应 Ready(Err)"
        );
        // 已 Done 时再 detach 不覆盖既有结果
        inner.mark_detached_if_pending();
        assert!(matches!(inner.poll_wait(&mut cx), Poll::Ready(Err(_))));
    }

    /// Drop 语义: handle 不是唯一 owner 时 (真实场景里 wrapper future 仍持一份
    /// Rc), drop handle 不得 detach 结果.
    #[test]
    fn dropping_handle_with_other_owners_does_not_detach() {
        let inner = Rc::new(JoinInner::<u32>::new());
        let wrapper_side = inner.clone(); // 模拟 wrapper future 持有的那份
        let h = JoinHandle {
            inner: inner.clone(),
        };
        drop(inner); // 测试自身的引用交出去, 现在强引用 = wrapper_side + h

        drop(h); // h 不是唯一 owner (还有 wrapper_side) → 不应 detach
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(
            wrapper_side.poll_wait(&mut cx).is_pending(),
            "仍有其他强引用时 drop handle 不应 detach"
        );

        wrapper_side.set_result(Ok(9));
        assert!(matches!(
            wrapper_side.poll_wait(&mut cx),
            Poll::Ready(Ok(9))
        ));
    }

    /// Drop 语义: handle 是唯一 owner 时走 detach 分支且不产生 UB/panic.
    /// (detach 的状态迁移由上一个测试直接覆盖; 这里锁住 Drop 挂点本身.)
    #[test]
    fn dropping_sole_owner_handle_runs_detach_branch() {
        let inner = Rc::new(JoinInner::<u32>::new());
        let h = JoinHandle { inner };
        drop(h); // strong_count == 1 → mark_detached_if_pending → Rc 释放
    }

    // ⭐ P2-6 编译期守卫 (维护者探针, 默认注释):
    // 取消下面两行注释后 **必须**编译失败 (E0277: cannot be sent between
    // threads safely). 若它编译通过, 说明 `JoinHandle` 的 `!Send` 不变量已被
    // 破坏 (marker 被误删 / 状态换成 Sync 实现 / 补了 unsafe impl Send),
    // 此时 `set_result` 与 `poll_wait` 可在两个线程并发访问同一 `UnsafeCell`
    // = 数据竞争 (UB), 必须按本文件顶部的设计说明恢复约束, 而不是放行.
    //
    // #[test]
    // fn probe_join_handle_must_not_be_send() {
    //     fn need_send<T: Send>() {}
    //     need_send::<JoinHandle<i32>>();
    // }
}
