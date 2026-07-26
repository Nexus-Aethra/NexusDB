//! `park_current_coroutine` / `unpark` API.
//!
//! **设计目的**: 让协程显式挂起 (类似 `std::thread::park`), 等待被外部 `unpark` 唤醒.
//! 用于 storage crate 的 chunk_lock wait queue 等场景: 协程申请 chunk owner 已被占用,
//! park 自己, 等 chunk owner 释放时 `unpark` 唤醒.
//!
//! **实现**: 每个调用 `park_current_coroutine()` 的协程持一个 `ParkState` cell.
//! - `Future::poll` 第一次: 存 cx.waker 到 thread-local, return `Poll::Pending`.
//! - `Future::poll` 第二次 (同一 ParkState): 直接 return `Poll::Ready(())`.
//! - `unpark(slot_id)` 移除并调 waker.wake() —— 唤醒后 future 第二次 poll 完成.
//!
//! **约束**:
//! - 必须由调度线程上 poll 的协程调用 (即 spawn 出来的 task)
//! - task_id 复用 pool 的 slot_id (单线程全局唯一)
//! - 每个协程同时只能有一个 parked 状态 (`unpark + 第二次 poll` 周期必须完成)

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

thread_local! {
    /// 所有 parked 协程的 Waker 缓存. key = slot_id (= task_id).
    /// 使用 HashMap 而非 BTreeMap 是因为 task_id 是 usize, HashMap O(1) lookup.
    /// 在 chunk_lock wait queue 是热路径, O(1) 关键.
    static PARKED: RefCell<HashMap<usize, Waker>> = RefCell::new(HashMap::new());
}

/// ⭐ Future state: `Pending` (还没 park 过) → `Parked` (已 park) → `Resolved` (被 unpark)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParkState {
    Pending,
    Parked,
    Resolved,
}

/// `park_current_coroutine().await` 返回的 Future.
///
/// 实现细节: 内部 state 必须是 `Pin`, 但 state 简单用 cell 即可.
///
/// **用法**:
/// ```ignore
/// async fn storage_read(&mut self, vpid: Vpid) -> Page {
///     let task_id = scheduler::current_task_id();
///     if !self.try_acquire_owner(vpid) {
///         scheduler::park_current_coroutine().await;  // park + 等 unpark
///     }
/// }
/// ```
pub struct ParkCurrent {
    state: ParkState,
}

impl ParkCurrent {
    pub(crate) fn new() -> Self {
        Self {
            state: ParkState::Pending,
        }
    }
}

impl Future for ParkCurrent {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.state {
            // 第一次 poll: park 协程, 存 waker, 返回 Pending
            ParkState::Pending => {
                let slot_id = match crate::scheduler::with_current_slot(|id| id) {
                    Some(id) => id,
                    None => {
                        panic!("park_current_coroutine() called outside scheduler poll context");
                    }
                };
                PARKED.with(|p| {
                    p.borrow_mut().insert(slot_id, cx.waker().clone());
                });
                self.state = ParkState::Parked;
                Poll::Pending
            }
            // 第二次 poll: 已经被 unpark 唤醒, 完成
            ParkState::Parked => {
                self.state = ParkState::Resolved;
                Poll::Ready(())
            }
            // 已完成: 再次 poll 时直接 Ready (idempotent, 防止误用)
            ParkState::Resolved => Poll::Ready(()),
        }
    }
}

/// 让当前协程挂起. 等 `unpark(task_id)` 显式唤醒.
///
/// 必须由调度线程上 poll 的协程调用 (即 spawn 出来的 task, 在 `.await` 之后).
pub fn park_current_coroutine() -> ParkCurrent {
    ParkCurrent::new()
}

/// 显式唤醒一个 parked 协程.
///
/// **调用时机**: 协程 park 后, 由"事件源"(chunk_lock owner 释放 / io_uring 完成等) 调.
///
/// **返回**:
/// - `true`  成功唤醒 (找到了 parked 协程, waker 已 wake → 协程下次 poll 看到 Parked 返回 Ready)
/// - `false` 没找到 (slot_id 未 park, 可能已被自动清理)
pub fn unpark(slot_id: usize) -> bool {
    PARKED.with(|p| {
        let mut map = p.borrow_mut();
        if let Some(waker) = map.remove(&slot_id) {
            waker.wake();
            true
        } else {
            false
        }
    })
}

/// 检查某个 task_id 是否还 parked (供测试 + 调试用).
pub fn is_parked(slot_id: usize) -> bool {
    PARKED.with(|p| p.borrow().contains_key(&slot_id))
}

/// 抢在 waker.wake() 之前把 parked 协程从注册表移除, 用于"已确认完成但还需重新唤醒"场景.
pub fn take_parked(slot_id: usize) -> Option<Waker> {
    PARKED.with(|p| p.borrow_mut().remove(&slot_id))
}

/// 储存一个已经构造好的 Waker, 而非依赖 future.poll 自然注册.
pub fn register_parked(slot_id: usize, waker: Waker) {
    PARKED.with(|p| {
        p.borrow_mut().insert(slot_id, waker);
    });
}

/// 清空所有 parked 协程 (主要用于测试 reset).
pub fn clear_all_parked() {
    PARKED.with(|p| p.borrow_mut().clear());
}

/// 当前 parked 协程数量 (测试 + 调试).
pub fn parked_count() -> usize {
    PARKED.with(|p| p.borrow().len())
}

// ⭐ 抑制 dead_code warning (RK 没用到 Rc 但是 import 留以备扩展)
#[allow(dead_code)]
fn _rc_dummy() -> Rc<()> {
    Rc::new(())
}
