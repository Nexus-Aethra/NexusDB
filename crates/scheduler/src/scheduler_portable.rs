//! 非 Linux target 的可移植单线程 scheduler。
//!
//! 它只提供 task/park/yield 调度，不包含 io_uring、RawFd 或 readiness API。Windows
//! 的网络与文件 I/O 将在各自 reactor/backend 中完成后通过 waker 投递回该调度器。

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread::ThreadId;

use crate::pool::{BoxFuture, Pool};
use crate::ready::{self, ReadyQueueHandle};
use crate::task::{InternalMessage, TaskRequest};
use crate::waker::make_waker;

const BATCH_SIZE: usize = 200;
const LOW_PRIO_BUDGET: usize = 1;

pub struct Scheduler {
    pool: Pool,
    ready: ReadyQueueHandle,
    task_queue: Mutex<VecDeque<InternalMessage>>,
    stop_flag: Arc<AtomicBool>,
    driver_thread: Option<ThreadId>,
}

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
        Self {
            pool: Pool::new(),
            ready: ready::new_handle(),
            task_queue: Mutex::new(VecDeque::new()),
            stop_flag: Arc::new(AtomicBool::new(false)),
            driver_thread: None,
        }
    }

    /// Windows/portable 后端没有 SQPOLL；保留签名以保持 shard 创建契约。
    pub fn new_with_sqpoll(_sqpoll_ms: u32) -> Self {
        Self::new()
    }

    pub fn submit(&self, future: BoxFuture<'static, ()>) {
        self.submit_with_priority(future, false);
    }

    pub fn submit_with_priority(&self, future: BoxFuture<'static, ()>, low_priority: bool) {
        self.task_queue
            .lock()
            .expect("scheduler task queue poisoned")
            .push_back(InternalMessage::Task(TaskRequest {
                future,
                low_priority,
            }));
    }

    pub fn stop_handle(&self) -> StopHandle {
        StopHandle {
            flag: self.stop_flag.clone(),
        }
    }

    fn bind_driver_thread(&mut self) {
        let current = std::thread::current().id();
        match self.driver_thread {
            Some(id) if id != current => panic!("Scheduler driven from a second thread"),
            Some(_) => {}
            None => self.driver_thread = Some(current),
        }
    }

    fn drain_task_queue(&mut self) {
        let mut queue = self
            .task_queue
            .lock()
            .expect("scheduler task queue poisoned");
        for _ in 0..BATCH_SIZE {
            match queue.pop_front() {
                Some(InternalMessage::Task(req)) => {
                    let Some(slot_id) = self.pool.acquire() else {
                        queue.push_front(InternalMessage::Task(req));
                        break;
                    };
                    self.pool.slot(slot_id).future = Some(req.future);
                    self.pool.slot(slot_id).low_priority = req.low_priority;
                    ready::push(&self.ready, slot_id);
                }
                Some(InternalMessage::Stop) => self.stop_flag.store(true, Ordering::Release),
                None => break,
            }
        }
    }

    fn extract_ready(&mut self) -> Vec<(usize, BoxFuture<'static, ()>)> {
        let wave = ready::drain(&self.ready);
        let mut normal = Vec::new();
        let mut low = Vec::new();
        for slot_id in wave {
            if self.pool.slot(slot_id).low_priority {
                low.push(slot_id);
            } else {
                normal.push(slot_id);
            }
        }
        let mut low = low.into_iter();
        normal.extend(low.by_ref().take(LOW_PRIO_BUDGET));
        // `ready::drain` transfers ownership of the entire wakeup wave.  Put
        // unserved low-priority tasks back before polling so a bounded budget
        // cannot silently lose their only readiness notification.
        for slot_id in low {
            ready::push(&self.ready, slot_id);
        }
        normal
            .into_iter()
            .filter_map(|slot_id| self.pool.slot(slot_id).future.take().map(|f| (slot_id, f)))
            .collect()
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<RefCell<Scheduler>>>> = const { RefCell::new(None) };
    static CURRENT_SLOT: Cell<Option<usize>> = const { Cell::new(None) };
}

pub fn with_current<R>(f: impl FnOnce(&mut Scheduler) -> R) -> Option<R> {
    CURRENT
        .try_with(|cell| {
            let scheduler = cell.borrow();
            let scheduler = scheduler.as_ref()?;
            let mut scheduler = scheduler.try_borrow_mut().ok()?;
            Some(f(&mut scheduler))
        })
        .ok()
        .flatten()
}

pub(crate) fn with_current_slot<R>(f: impl FnOnce(usize) -> R) -> Option<R> {
    CURRENT_SLOT.with(|slot| slot.get().map(f))
}

fn set_current_slot(slot_id: usize) {
    CURRENT_SLOT.with(|slot| slot.set(Some(slot_id)));
}

fn clear_current_slot() {
    CURRENT_SLOT.with(|slot| slot.set(None));
}

pub fn current_task_id() -> usize {
    with_current_slot(|id| id).expect("current_task_id called outside scheduler task")
}

pub struct SchedHandle(pub Rc<RefCell<Scheduler>>);
unsafe impl Send for SchedHandle {}

impl Clone for SchedHandle {
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl SchedHandle {
    pub fn new(scheduler: Scheduler) -> Self {
        Self(Rc::new(RefCell::new(scheduler)))
    }

    pub fn set_current(&self) {
        CURRENT.with(|current| *current.borrow_mut() = Some(self.0.clone()));
    }

    pub fn into_inner(self) -> RefCell<Scheduler> {
        Rc::try_unwrap(self.0)
            .ok()
            .expect("SchedHandle has multiple owners")
    }

    pub fn stop_handle(&self) -> StopHandle {
        self.0.borrow().stop_handle()
    }

    /// 有界、非阻塞推进 portable task；外部 reactor 负责等待 I/O 后唤醒 task。
    pub fn drive_until_idle(self, max_iters: usize) -> bool {
        self.0.borrow_mut().bind_driver_thread();
        for _ in 0..max_iters {
            let work = {
                let mut scheduler = self.0.borrow_mut();
                scheduler.drain_task_queue();
                scheduler.extract_ready()
            };
            if work.is_empty() {
                break;
            }
            let mut pending = Vec::new();
            let mut completed = Vec::new();
            for (slot_id, mut future) in work {
                set_current_slot(slot_id);
                let waker = {
                    let scheduler = self.0.borrow();
                    make_waker(slot_id, &scheduler.ready)
                };
                let mut context = Context::from_waker(&waker);
                match future.as_mut().poll(&mut context) {
                    Poll::Ready(()) => completed.push(slot_id),
                    Poll::Pending => pending.push((slot_id, future)),
                }
                clear_current_slot();
            }
            let mut scheduler = self.0.borrow_mut();
            for slot_id in completed {
                scheduler.pool.release(slot_id);
            }
            for (slot_id, future) in pending {
                scheduler.pool.slot(slot_id).future = Some(future);
            }
            if scheduler.stop_flag.load(Ordering::Acquire) {
                break;
            }
        }
        true
    }
}
