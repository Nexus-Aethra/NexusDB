//! `AwaitPredicate` — park 当前 task + 等外部条件或 waker 唤醒.
//!
//! 用于网络层 worker thread:
//! - worker 拿到 reply 后, 想让 task 重新被调度
//! - 调用 `await_waker(predicate).await`, park 当前 task, 等条件满足或外部 wake
//!
//! **设计**: thread-local `WAKE_SIGNAL: AtomicU64` 携带外部 wake 信号 (req_id).
//!   park task 时返回 Pending; 下次 driver 循环运行时检查 `last_wake_id >= parked_id`
//!   或 predicate 满足时返回 Ready(req_id).
//!
//! **简化 (Phase 2.1 首版)**:
//! - predicate 仅检查传入 closure, 不接 timeout
//! - 接受 Option<Waker>: 外部完成时 `unpark_with_waker(slot_id, waker, signal)`

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

/// AwaitPredicate: park 直到 predicate 满足 或 外部 wake.
///
/// **用法**:
/// ```ignore
/// let signal = Rc::new(Cell::new(0u64));
/// AwaitPredicate::new(|| signal.get() == req_id).await; // wait for reply
/// ```
///
/// **唤醒机制**: 当前 task `park_current_coroutine`, 外部事件调 `unpark(slot_id)`
/// (来自现有 park API). AwaitPredicate 二次 poll 时直接 Ready.
pub struct AwaitPredicate {
    /// 用户传入的 predicate
    predicate: Rc<dyn Fn() -> bool>,
    /// 是否已登记过 park (第一次 poll true, 第二次后 false)
    parked: bool,
}

impl AwaitPredicate {
    pub fn new<F: Fn() -> bool + 'static>(predicate: F) -> Self {
        Self {
            predicate: Rc::new(predicate),
            parked: false,
        }
    }
}

impl Future for AwaitPredicate {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // 每次 poll 先检查 predicate (即使 unpark 后, 也重新看 predicate 真不真).
        if (self.predicate)() {
            return Poll::Ready(());
        }
        // 第一次: park 当前 task, 把 waker 注册到全局 slot.
        if !self.parked {
            let slot_id = match crate::scheduler::with_current_slot(|id| id) {
                Some(id) => id,
                None => return Poll::Ready(()), // 调度上下文外, 不挂起
            };
            crate::park::register_parked(slot_id, cx.waker().clone());
            self.parked = true;
        } else {
            // 已 park 过 + 被 unpark: 重新注册最新 waker (caller 可能换了 waker)
            if let Some(slot_id) = crate::scheduler::with_current_slot(|id| id) {
                crate::park::register_parked(slot_id, cx.waker().clone());
            }
        }
        Poll::Pending
    }
}

/// 创建 await predicate future.
pub fn await_predicate<F: Fn() -> bool + 'static>(predicate: F) -> AwaitPredicate {
    AwaitPredicate::new(predicate)
}
