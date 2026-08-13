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
    in_use: usize,
}

impl Pool {
    pub fn new() -> Self {
        let slots: Box<[Slot; POOL_SIZE]> = Box::new(std::array::from_fn(|_| Slot::default()));
        Self {
            slots,
            // 活跃 slot 绝不可回绕复用；所有 slot 从 free list 分配。
            free: (0..POOL_SIZE).collect(),
            in_use: 0,
        }
    }

    /// 分配一个未被活跃 future 占用的 slot。
    ///
    /// 满载时由上层保留 task 在 admission queue 中等待，不能覆盖活跃 future。
    pub fn acquire(&mut self) -> Option<usize> {
        let idx = self.free.pop_front()?;
        self.in_use += 1;
        Some(idx)
    }

    pub fn release(&mut self, idx: usize) {
        debug_assert!(idx < POOL_SIZE);
        self.slots[idx].future = None;
        self.slots[idx].low_priority = false;
        // 优先复用刚释放的 slot；它已完成 future/IO 清理，且不影响活跃 slot 的所有权。
        self.free.push_front(idx);
        self.in_use -= 1;
    }

    pub fn slot(&mut self, idx: usize) -> &mut Slot {
        &mut self.slots[idx]
    }

    pub fn in_use(&self) -> usize {
        self.in_use
    }

    pub fn available(&self) -> usize {
        self.free.len()
    }
}

impl Default for Pool {
    fn default() -> Self {
        Self::new()
    }
}
