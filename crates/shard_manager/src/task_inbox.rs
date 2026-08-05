//! TaskInbox: 专用于 ShardTask 的无锁 MPSC 队列.
//!
//! 与 ShardInbox (用于 ShardRequest) 并行存在.
//! Worker push ShardTask, Shard drain 并执行.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_queue::ArrayQueue;

use crate::request::ShardTask;

const TASK_INBOX_CAPACITY: usize = 8192;

/// Task 专用 inbox: worker push, shard drain.
pub struct TaskInbox {
    ring: ArrayQueue<ShardTask>,
    eventfd: std::os::unix::io::RawFd,
    pending: AtomicU64,
}

impl TaskInbox {
    pub fn new() -> Self {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
        assert!(fd >= 0, "eventfd creation failed");
        Self {
            ring: ArrayQueue::new(TASK_INBOX_CAPACITY),
            eventfd: fd,
            pending: AtomicU64::new(0),
        }
    }

    /// Worker 端: push 单个 task (batch coalescing: 首次 push 才通知).
    ///
    /// Err = 队满退还 task (caller 自旋重试), 语义上必须把所有权还回去 —
    /// 装箱会波及全链路分配, 且退还是罕见路径, 尺寸 lint 不适用.
    #[allow(clippy::result_large_err)]
    pub fn push(&self, task: ShardTask) -> Result<(), ShardTask> {
        self.ring.push(task)?;
        if self.pending.fetch_add(1, Ordering::AcqRel) == 0 {
            let val: u64 = 1;
            unsafe {
                libc::write(self.eventfd, &val as *const u64 as *const libc::c_void, 8);
            }
        }
        Ok(())
    }

    /// Worker 端: push, 满时 spin retry.
    pub fn push_spin(&self, task: ShardTask) {
        let mut t = task;
        loop {
            match self.push(t) {
                Ok(()) => return,
                Err(rejected) => {
                    t = rejected;
                    std::thread::yield_now();
                }
            }
        }
    }

    /// Worker 端: 按同一 shard 的输入顺序批量投递 task。
    ///
    /// 与逐项 `push_spin` 相比，批量只进行一次 pending 原子累加和至多一次
    /// eventfd 唤醒；每个 task 仍独立入无锁队列，因此队满时的背压与顺序语义不变。
    pub fn push_batch_spin(&self, tasks: Vec<ShardTask>) {
        if tasks.is_empty() {
            return;
        }
        let count = tasks.len() as u64;
        for task in tasks {
            let mut task = task;
            loop {
                match self.ring.push(task) {
                    Ok(()) => break,
                    Err(rejected) => {
                        task = rejected;
                        std::thread::yield_now();
                    }
                }
            }
        }
        if self.pending.fetch_add(count, Ordering::AcqRel) == 0 {
            let val: u64 = 1;
            unsafe {
                libc::write(self.eventfd, &val as *const u64 as *const libc::c_void, 8);
            }
        }
    }

    /// Shard 端: drain 所有 pending tasks.
    ///
    /// **⭐ 丢唤醒修复 (2026-07-24)**: 必须**先重置 pending 再 pop**.
    /// 若先 pop 后重置, producer 在 pop 结束与 store(0) 之间 push 时
    /// fetch_add 看到旧值 >0 不写 eventfd, 而该 task 又没被本轮 pop 到
    /// → shard 睡眠后无人唤醒 (丢唤醒). 先 store(0) 保证:
    /// - store 之前的 push 一定被本轮 pop 到 (Release/Acquire 序)
    /// - store 之后的 push 一定看到 pending=0 并写 eventfd
    pub fn drain(&self) -> Vec<ShardTask> {
        self.pending.store(0, Ordering::Release);
        let mut batch = Vec::with_capacity(128);
        while let Some(task) = self.ring.pop() {
            batch.push(task);
        }
        batch
    }

    /// Shard 端: blocking wait (eventfd read).
    pub fn wait(&self) {
        let mut val: u64 = 0;
        unsafe {
            libc::read(self.eventfd, &mut val as *mut u64 as *mut libc::c_void, 8);
        }
    }

    pub fn len(&self) -> usize {
        self.ring.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    pub fn eventfd(&self) -> std::os::unix::io::RawFd {
        self.eventfd
    }
}

impl Default for TaskInbox {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TaskInbox {
    fn drop(&mut self) {
        unsafe { libc::close(self.eventfd); }
    }
}

unsafe impl Send for TaskInbox {}
unsafe impl Sync for TaskInbox {}

pub type SharedTaskInbox = Arc<TaskInbox>;

pub fn new_shared_task_inbox() -> SharedTaskInbox {
    Arc::new(TaskInbox::new())
}
