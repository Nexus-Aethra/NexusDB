//! ReplyFuture: 跨线程 spin-poll 回复机制.
//!
//! ## 设计 (消除 futex)
//!
//! 旧版用 Mutex + Waker + thread::park/unpark, 每次 reply 触发 futex syscall.
//!
//! 新版用 AtomicPtr spin-poll:
//! - shard 端: `reply.send(resp)` = AtomicPtr store (纯 atomic, 零 syscall)
//! - client 端: `block_on_v2(fut)` = spin-poll AtomicPtr (纯 atomic load, 零 syscall)
//! - 慢路径: spin 4096 次后 fall back to park_timeout (极少触发)
//!
//! ## 用法
//!
//! ```ignore
//! let (sender, fut) = PendingReply::new();
//! shard.send(ShardRequest::Put { ..., reply: sender });
//! let result = block_on_v2(fut);  // spin-poll, 零 futex
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::thread::Thread;

use crate::request::ShardResponse;

/// 共享回复槽: shard 端 store + unpark, client 端 spin/park.
struct ReplySlot {
    ptr: AtomicPtr<ShardResponse>,
    /// client 线程 handle (shard store 后 unpark 它).
    /// 用 AtomicPtr 存储 (None = 还没注册, Some = 已注册).
    waiter: AtomicPtr<Thread>,
    /// ⭐ async 路径的 task waker (Future::poll 注册, store 时 wake).
    /// 修复: 之前 poll 忽略 cx.waker() 导致 pollster 等 executor 永久 park.
    task_waker: AtomicPtr<Waker>,
}

impl ReplySlot {
    fn new() -> Self {
        Self {
            ptr: AtomicPtr::new(std::ptr::null_mut()),
            waiter: AtomicPtr::new(std::ptr::null_mut()),
            task_waker: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// client 端: 注册当前线程为 waiter (shard 完成后会 unpark).
    fn register_waiter(&self, thread: Thread) {
        let boxed = Box::into_raw(Box::new(thread));
        self.waiter.store(boxed, Ordering::Release);
    }

    /// shard 端: 写入结果并唤醒 client (线程 waiter + task waker 两条路径).
    fn store(&self, resp: ShardResponse) {
        let boxed = Box::into_raw(Box::new(resp));
        self.ptr.store(boxed, Ordering::Release);
        // unpark waiter (if registered)
        let waiter_ptr = self.waiter.swap(std::ptr::null_mut(), Ordering::AcqRel);
        if !waiter_ptr.is_null() {
            let thread = unsafe { *Box::from_raw(waiter_ptr) };
            thread.unpark();
        }
        // wake async task waker (if registered)
        let waker_ptr = self.task_waker.swap(std::ptr::null_mut(), Ordering::AcqRel);
        if !waker_ptr.is_null() {
            let waker = unsafe { *Box::from_raw(waker_ptr) };
            waker.wake();
        }
    }

    /// client 端: 尝试取走结果.
    fn try_take(&self) -> Option<ShardResponse> {
        let ptr = self.ptr.swap(std::ptr::null_mut(), Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { *Box::from_raw(ptr) })
        }
    }
}

impl Drop for ReplySlot {
    fn drop(&mut self) {
        let ptr = *self.ptr.get_mut();
        if !ptr.is_null() {
            unsafe { drop(Box::from_raw(ptr)); }
        }
        let w = *self.waiter.get_mut();
        if !w.is_null() {
            unsafe { drop(Box::from_raw(w)); }
        }
        let tw = *self.task_waker.get_mut();
        if !tw.is_null() {
            unsafe { drop(Box::from_raw(tw)); }
        }
    }
}

unsafe impl Send for ReplySlot {}
unsafe impl Sync for ReplySlot {}

/// ReplySender: shard 端持有, send 即 reply.
#[derive(Clone)]
pub struct ReplySender {
    slot: Arc<ReplySlot>,
}

impl ReplySender {
    /// 把结果发回 caller. 纯 atomic store, 零 syscall.
    pub fn send(self, resp: ShardResponse) -> bool {
        self.slot.store(resp);
        true
    }
}

/// ReplyFuture: caller 持有, spin-poll 等结果.
/// 同时实现 Future trait (供 async API 使用).
pub struct ReplyFuture {
    slot: Arc<ReplySlot>,
}

impl Future for ReplyFuture {
    type Output = ShardResponse;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(resp) = self.slot.try_take() {
            return Poll::Ready(resp);
        }
        // ⭐ 注册 task waker: shard store 后会 wake, executor 才会重新 poll.
        let boxed = Box::into_raw(Box::new(cx.waker().clone()));
        let old = self.slot.task_waker.swap(boxed, Ordering::AcqRel);
        if !old.is_null() {
            unsafe { drop(Box::from_raw(old)); }
        }
        // double-check: send 可能发生在上面 try_take 与注册之间.
        // (waker 残留在槽里无害: 下次 store 会取走 wake, 顶多多 poll 一次)
        match self.slot.try_take() {
            Some(resp) => Poll::Ready(resp),
            None => Poll::Pending,
        }
    }
}

/// PendingReply: caller 创建, 拿到 (ReplySender + ReplyFuture).
pub struct PendingReply;

impl PendingReply {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> (ReplySender, ReplyFuture) {
        let slot = Arc::new(ReplySlot::new());
        let sender = ReplySender { slot: slot.clone() };
        let future = ReplyFuture { slot };
        (sender, future)
    }
}

/// 轻量级 block_on: AtomicPtr + thread::park.
///
/// 比旧版 (Mutex + Waker) 简化: 纯 atomic + 精确 unpark.
/// - 快速路径: spin 128 次检查 (~50-100ns)
/// - 慢路径: 注册线程为 waiter, park 等 shard unpark (单次 futex)
pub fn block_on_v2(fut: ReplyFuture) -> ShardResponse {
    // 立即检查 (cheap load)
    if !fut.slot.ptr.load(Ordering::Acquire).is_null() {
        return fut.slot.try_take().unwrap();
    }

    // 短 spin: 用 load (便宜) 而非 swap (贵)
    for _ in 0..128 {
        std::hint::spin_loop();
        if !fut.slot.ptr.load(Ordering::Acquire).is_null() {
            return fut.slot.try_take().unwrap();
        }
    }

    // 注册 waiter, 然后 park
    fut.slot.register_waiter(std::thread::current());

    // double-check
    if !fut.slot.ptr.load(Ordering::Acquire).is_null() {
        return fut.slot.try_take().unwrap();
    }

    // park 等 shard unpark
    loop {
        std::thread::park();
        if !fut.slot.ptr.load(Ordering::Acquire).is_null() {
            return fut.slot.try_take().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::ShardReply;

    #[test]
    fn immediate_send_then_poll() {
        let (tx, fut) = PendingReply::new();
        tx.send(Ok(ShardReply::PutOk));
        let resp = block_on_v2(fut);
        assert!(matches!(resp, Ok(ShardReply::PutOk)));
    }

    #[test]
    fn send_after_spin() {
        let (tx, fut) = PendingReply::new();
        // 在另一个线程延迟 send
        let handle = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_micros(50));
            tx.send(Ok(ShardReply::GetValue(Some(b"hi".to_vec()))));
        });
        let resp = block_on_v2(fut);
        assert!(matches!(resp, Ok(ShardReply::GetValue(Some(v))) if v == b"hi"));
        handle.join().unwrap();
    }

    #[test]
    fn clone_sender_works() {
        let (tx, fut) = PendingReply::new();
        let tx2 = tx.clone();
        drop(tx);
        tx2.send(Err(crate::request::ShardErrorKind::ChannelClosed));
        let resp = block_on_v2(fut);
        assert!(matches!(resp, Err(crate::request::ShardErrorKind::ChannelClosed)));
    }

    #[test]
    fn future_impl_works() {
        use std::task::Waker;
        let (tx, mut fut) = PendingReply::new();
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        // 先 poll, pending
        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));
        // send 后 poll, ready
        tx.send(Ok(ShardReply::PutOk));
        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Ready(Ok(ShardReply::PutOk))));
    }

    #[test]
    fn block_on_v2_basic() {
        let r = block_on_v2({
            let (tx, fut) = PendingReply::new();
            tx.send(Ok(ShardReply::FlushOk));
            fut
        });
        assert!(matches!(r, Ok(ShardReply::FlushOk)));
    }
}
