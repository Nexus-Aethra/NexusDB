//! ShardInbox: 无锁 MPSC ring buffer + eventfd 通知.
//!
//! 替代 `std::sync::mpsc` channel, 消除 shard 线程的 futex_wait 开销.
//!
//! **设计**:
//! - `crossbeam_queue::ArrayQueue` 提供无锁 MPSC push/pop
//! - Linux eventfd 提供高效的单次唤醒 (1 write syscall, 非 futex)
//! - shard 线程 blocking read eventfd, 醒来后批量 drain ring buffer
//!
//! **容量**: 4096 请求. 超出时 caller spin-yield 重试.

use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_queue::ArrayQueue;

use crate::request::ShardRequest;

/// 默认 ring buffer 容量.
const INBOX_CAPACITY: usize = 4096;

/// ShardInbox: 无锁 ring buffer + eventfd 通知.
///
/// 多个 client 线程 push, 单个 shard 线程 pop.
///
/// **⭐ Phase A 优化**: 用 `pending` atomic 计数器实现批量合并唤醒.
/// 第一个 push 才写 eventfd 通知 shard, 后续 push 搭车.
/// drain 时重置 pending=0, 下一次 push 再次触发通知.
pub struct ShardInbox {
    ring: ArrayQueue<ShardRequest>,
    eventfd: RawFd,
    /// 自上次 drain 以来的 pending push 计数.
    /// 第一个 push (0→1) 触发 eventfd_write, 后续搭车.
    pending: AtomicU64,
}

impl ShardInbox {
    /// 创建新的 ShardInbox. eventfd 初始值 0, 非 semaphore 模式.
    pub fn new() -> Self {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
        assert!(fd >= 0, "eventfd creation failed");
        Self {
            ring: ArrayQueue::new(INBOX_CAPACITY),
            eventfd: fd,
            pending: AtomicU64::new(0),
        }
    }

    /// 带自定义容量创建.
    pub fn with_capacity(cap: usize) -> Self {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
        assert!(fd >= 0, "eventfd creation failed");
        Self {
            ring: ArrayQueue::new(cap),
            eventfd: fd,
            pending: AtomicU64::new(0),
        }
    }

    /// Push 请求到 ring buffer 并通知 shard 线程.
    ///
    /// **⭐ Phase A**: 只有第一个 pending 请求才写 eventfd (后续搭车).
    /// 如果 ring 满, 返回 Err(req) — caller 应 yield + retry.
    pub fn push(&self, req: ShardRequest) -> Result<(), ShardRequest> {
        self.ring.push(req)?;
        // 只有第一个 pending (0→1) 才通知 shard
        if self.pending.fetch_add(1, Ordering::AcqRel) == 0 {
            let val: u64 = 1;
            unsafe {
                libc::write(self.eventfd, &val as *const u64 as *const libc::c_void, 8);
            }
        }
        Ok(())
    }

    /// Push 请求, ring 满时 spin-yield 重试.
    pub fn push_spin(&self, req: ShardRequest) {
        let mut req = req;
        loop {
            match self.push(req) {
                Ok(()) => return,
                Err(r) => {
                    req = r;
                    std::thread::yield_now();
                }
            }
        }
    }

    /// Pop 单个请求 (非阻塞).
    pub fn pop(&self) -> Option<ShardRequest> {
        self.ring.pop()
    }

    /// 批量 drain 所有待处理请求. 返回 Vec (amortize eventfd_read 开销).
    ///
    /// **⭐ 丢唤醒修复 (2026-07-24)**: 先重置 pending 再 pop.
    /// 若先 pop 后重置, producer 在两者之间 push 时 fetch_add 看到旧值 >0
    /// 不写 eventfd, 且该请求未被本轮 pop 到 → 丢唤醒.
    pub fn drain(&self) -> Vec<ShardRequest> {
        // 先重置 pending: store 之后的 push 会看到 0 并重新写 eventfd;
        // store 之前的 push 一定被下面的 pop 循环取到.
        self.pending.store(0, Ordering::Release);
        let mut batch = Vec::with_capacity(64);
        while let Some(req) = self.ring.pop() {
            batch.push(req);
        }
        batch
    }

    /// Blocking wait: 读 eventfd (阻塞直到有新请求).
    /// 返回累积的通知计数 (通常 >= 1).
    pub fn wait(&self) -> u64 {
        let mut val: u64 = 0;
        unsafe {
            libc::read(self.eventfd, &mut val as *mut u64 as *mut libc::c_void, 8);
        }
        val
    }

    /// 当前 ring buffer 中的待处理请求数.
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// ring buffer 是否为空.
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    /// 获取 eventfd (供外部 epoll/io_uring 注册).
    pub fn eventfd(&self) -> RawFd {
        self.eventfd
    }
}

impl Default for ShardInbox {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ShardInbox {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.eventfd);
        }
    }
}

// Safety: ShardInbox 可跨线程共享 (ArrayQueue 是 Send+Sync, eventfd 是 thread-safe)
unsafe impl Send for ShardInbox {}
unsafe impl Sync for ShardInbox {}

/// Arc 包装, 方便多处持有.
pub type SharedInbox = Arc<ShardInbox>;

pub fn new_shared_inbox() -> SharedInbox {
    Arc::new(ShardInbox::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reply::PendingReply;
    use crate::request::ShardRequest;

    #[test]
    fn inbox_push_pop_basic() {
        let inbox = ShardInbox::new();
        let (tx, _fut) = PendingReply::new();
        let req = ShardRequest::Flush { reply: tx };
        assert!(inbox.push(req).is_ok());
        assert_eq!(inbox.len(), 1);
        let popped = inbox.pop();
        assert!(popped.is_some());
        assert!(inbox.is_empty());
    }

    #[test]
    fn inbox_drain_multiple() {
        let inbox = ShardInbox::new();
        for _ in 0..10 {
            let (tx, _fut) = PendingReply::new();
            assert!(inbox.push(ShardRequest::Flush { reply: tx }).is_ok());
        }
        let batch = inbox.drain();
        assert_eq!(batch.len(), 10);
        assert!(inbox.is_empty());
    }

    #[test]
    fn inbox_cross_thread() {
        let inbox = Arc::new(ShardInbox::new());
        let inbox2 = inbox.clone();

        let handle = std::thread::spawn(move || {
            // shard 侧: wait + drain
            inbox2.wait();
            inbox2.drain()
        });

        // client 侧: push
        std::thread::sleep(std::time::Duration::from_millis(10));
        let (tx, _fut) = PendingReply::new();
        assert!(inbox.push(ShardRequest::Flush { reply: tx }).is_ok());

        let batch = handle.join().unwrap();
        assert_eq!(batch.len(), 1);
    }
}
