use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;

pub const POOL_SIZE: usize = 1024;

/// 单线程 BoxFuture — 不要求 Send, 因为调度器只在自己线程上 poll.
/// 移除 Send 约束后, Future 可以捕获 Rc / &mut 等 !Send 类型.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub struct Slot {
    pub future: Option<BoxFuture<'static, ()>>,
    pub cancel_requested: AtomicBool,
    /// ⭐ G0: 低优先级标记 (后台任务如 compact). wave 内排在普通协程之后
    /// 且每 wave 限额 poll, 不占前台请求名额.
    pub low_priority: bool,
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            future: None,
            cancel_requested: AtomicBool::new(false),
            low_priority: false,
        }
    }
}

pub struct Pool {
    slots: Box<[Slot; POOL_SIZE]>,
    free: VecDeque<usize>,
    rr: usize,
    in_use: usize,
}

impl Pool {
    pub fn new() -> Self {
        let slots: Box<[Slot; POOL_SIZE]> = Box::new(std::array::from_fn(|_| Slot::default()));
        Self {
            slots,
            free: VecDeque::new(),
            rr: 0,
            in_use: 0,
        }
    }

    pub fn acquire(&mut self) -> usize {
        if let Some(idx) = self.free.pop_front() {
            self.in_use += 1;
            return idx;
        }
        let idx = self.rr;
        self.rr = (self.rr + 1) % POOL_SIZE;
        self.in_use += 1;
        idx
    }

    pub fn release(&mut self, idx: usize) {
        debug_assert!(idx < POOL_SIZE);
        self.slots[idx].future = None;
        self.slots[idx].low_priority = false;
        self.free.push_back(idx);
        self.in_use -= 1;
    }

    pub fn slot(&mut self, idx: usize) -> &mut Slot {
        &mut self.slots[idx]
    }

    pub fn in_use(&self) -> usize {
        self.in_use
    }
}

impl Default for Pool {
    fn default() -> Self {
        Self::new()
    }
}
