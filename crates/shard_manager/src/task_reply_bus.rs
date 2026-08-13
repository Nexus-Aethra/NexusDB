//! TaskReplyBus: per-worker 无锁回复队列.
//!
//! Shard 执行完 ShardTask 后, 把 TaskResult push 到对应 worker 的 bus.
//! Worker 的 epoll 循环 drain bus 并发送回客户端.
//!
//! 架构: N 个 worker, 每个 worker 一个 bus. Shard → bus[worker_id] → Worker.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_queue::ArrayQueue;

use crate::request::TaskResult;

/// 单个 worker 的回复队列容量.
const REPLY_BUS_CAPACITY: usize = 8192;

struct QueuedResult {
    result: TaskResult,
    enqueued_at: Option<std::time::Instant>,
}

/// 单个 worker 的回复 bus (shard push, worker drain).
pub struct TaskReplyBus {
    ring: ArrayQueue<QueuedResult>,
    /// eventfd: shard push 后通知 worker 有回复可发.
    #[cfg(target_os = "linux")]
    eventfd: std::os::unix::io::RawFd,
    /// ⭐ 通知合并: 自上次 drain 以来的 pending 计数.
    /// 首条 push (0→1) 才写 eventfd, 后续搭车 —— shard 一轮回 N 条
    /// 从 N 次 write syscall 降到 ~1 次.
    pending: AtomicU64,
}

impl TaskReplyBus {
    pub fn new() -> Self {
        #[cfg(target_os = "linux")]
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        #[cfg(target_os = "linux")]
        assert!(fd >= 0, "eventfd creation failed");
        Self {
            ring: ArrayQueue::new(REPLY_BUS_CAPACITY),
            #[cfg(target_os = "linux")]
            eventfd: fd,
            pending: AtomicU64::new(0),
        }
    }

    /// shard 端: push 一个 result + 合并通知 worker.
    pub fn push(&self, result: TaskResult) {
        let mut queued = QueuedResult {
            result,
            enqueued_at: crate::PROBE.is_enabled().then(std::time::Instant::now),
        };
        // 如果满了 spin retry (不太可能, 8192 容量)
        loop {
            match self.ring.push(queued) {
                Ok(()) => break,
                Err(rejected) => {
                    queued = rejected;
                    std::thread::yield_now();
                }
            }
        }
        // ⭐ 合并通知: 首条才写 eventfd, 后续搭车
        if self.pending.fetch_add(1, Ordering::AcqRel) == 0 {
            #[cfg(target_os = "linux")]
            {
                let val: u64 = 1;
                unsafe {
                    libc::write(self.eventfd, &val as *const u64 as *const libc::c_void, 8);
                }
            }
        }
    }

    /// worker 端: drain 所有待发送的 results.
    ///
    /// ⭐ 防丢唤醒: 先重置 pending 再 pop —— store 之前的 push 必被本轮
    /// pop 到; store 之后的 push 看到 0 会重新写 eventfd.
    pub fn drain(&self) -> Vec<TaskResult> {
        let mut results = Vec::with_capacity(64);
        self.drain_into(&mut results);
        results
    }

    /// Worker 端: drain 到调用方复用的缓冲，避免每次 eventfd 唤醒分配 Vec。
    pub fn drain_into(&self, results: &mut Vec<TaskResult>) {
        // 先 read eventfd (消耗通知计数)
        #[cfg(target_os = "linux")]
        {
            let mut val: u64 = 0;
            unsafe {
                libc::read(self.eventfd, &mut val as *mut u64 as *mut libc::c_void, 8);
            }
        }
        self.pending.store(0, Ordering::Release);
        results.clear();
        while let Some(queued) = self.ring.pop() {
            if let Some(enqueued_at) = queued.enqueued_at {
                crate::PROBE
                    .reply_bus_queue_ns
                    .record(enqueued_at.elapsed().as_nanos() as u64);
            }
            results.push(queued.result);
        }
    }

    /// 非阻塞 drain (不 read eventfd, 用于 poll 模式).
    pub fn try_drain(&self) -> Vec<TaskResult> {
        let mut results = Vec::with_capacity(64);
        while let Some(queued) = self.ring.pop() {
            if let Some(enqueued_at) = queued.enqueued_at {
                crate::PROBE
                    .reply_bus_queue_ns
                    .record(enqueued_at.elapsed().as_nanos() as u64);
            }
            results.push(queued.result);
        }
        results
    }

    /// 获取 eventfd fd (供 epoll 注册).
    #[cfg(target_os = "linux")]
    pub fn eventfd(&self) -> std::os::unix::io::RawFd {
        self.eventfd
    }

    /// 队列中待处理的 result 数.
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }
}

impl Default for TaskReplyBus {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TaskReplyBus {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        unsafe {
            libc::close(self.eventfd);
        }
    }
}

unsafe impl Send for TaskReplyBus {}
unsafe impl Sync for TaskReplyBus {}

/// 共享引用.
pub type SharedTaskReplyBus = Arc<TaskReplyBus>;

/// 所有 worker 的 reply bus 集合.
/// shard 按 task.worker_id 索引选择目标 bus.
pub struct ReplyBusSet {
    buses: Vec<SharedTaskReplyBus>,
}

impl ReplyBusSet {
    pub fn new(worker_count: usize) -> Self {
        let worker_count = worker_count.max(1);
        let buses: Vec<_> = (0..worker_count)
            .map(|_| Arc::new(TaskReplyBus::new()))
            .collect();
        Self { buses }
    }

    /// 获取指定 worker 的 bus.
    pub fn get(&self, worker_id: u32) -> &SharedTaskReplyBus {
        &self.buses[worker_id as usize % self.buses.len()]
    }

    /// 获取指定 worker 的 bus Arc (用于 worker 持有).
    pub fn get_arc(&self, worker_id: u32) -> SharedTaskReplyBus {
        self.buses[worker_id as usize % self.buses.len()].clone()
    }

    /// worker 数量.
    pub fn worker_count(&self) -> usize {
        self.buses.len()
    }
}
