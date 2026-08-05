//! TaskInbox: 专用于 ShardTask 的无锁 MPSC 队列.
//!
//! 与 ShardInbox (用于 ShardRequest) 并行存在.
//! Worker push ShardTask, Shard drain 并执行.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_queue::ArrayQueue;

use crate::request::ShardTask;

const TASK_INBOX_CAPACITY: usize = 8192;

struct QueuedTask {
    task: ShardTask,
    enqueued_at: Option<std::time::Instant>,
}

/// Task 专用 inbox: worker push, shard drain.
pub struct TaskInbox {
    ring: ArrayQueue<QueuedTask>,
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
        let queued = QueuedTask {
            task,
            enqueued_at: crate::PROBE.is_enabled().then(std::time::Instant::now),
        };
        if let Err(rejected) = self.ring.push(queued) {
            return Err(rejected.task);
        }
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
            let mut task = QueuedTask {
                task,
                enqueued_at: crate::PROBE.is_enabled().then(std::time::Instant::now),
            };
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
        self.drain_up_to(usize::MAX)
    }

    /// Shard 端: 最多取出 `limit` 个 pending task。
    ///
    /// 有界 drain 用于高负载前台时间片：保留 ring 内剩余任务不会丢失唤醒，
    /// 因为主循环在队列非空时会立即进行下一轮 drain，而不会进入 poll 睡眠。
    pub fn drain_up_to(&self, limit: usize) -> Vec<ShardTask> {
        self.pending.store(0, Ordering::Release);
        let mut batch = Vec::with_capacity(limit.min(128));
        while let Some(queued) = self.ring.pop() {
            if let Some(enqueued_at) = queued.enqueued_at {
                crate::PROBE
                    .task_queue_ns
                    .record(enqueued_at.elapsed().as_nanos() as u64);
            }
            batch.push(queued.task);
            if batch.len() == limit {
                break;
            }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::TaskInbox;
    use crate::request::{BatchOp, ShardTask};

    fn task(req_id: u64) -> ShardTask {
        ShardTask {
            conn_id: 1,
            req_id,
            worker_id: 0,
            group: 0,
            op: BatchOp::Get {
                db: Arc::from("default"),
                table: Arc::from("default"),
                key: req_id.to_be_bytes().to_vec(),
            },
        }
    }

    #[test]
    fn bounded_drain_preserves_fifo_remainder() {
        let inbox = TaskInbox::new();
        for req_id in 0..3 {
            inbox.push(task(req_id)).unwrap();
        }

        let first = inbox.drain_up_to(2);
        assert_eq!(first.iter().map(|task| task.req_id).collect::<Vec<_>>(), [0, 1]);
        assert!(!inbox.is_empty(), "unserved task must remain visible to next turn");

        let second = inbox.drain_up_to(2);
        assert_eq!(second.iter().map(|task| task.req_id).collect::<Vec<_>>(), [2]);
        assert!(inbox.is_empty());
    }
}
