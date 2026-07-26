//! 自实现的 Waker, 用于把协程重新放回 ready queue.
//!
//! **设计**: 完全绕开 std::task::Waker 的 `Arc<W: Wake + Send + Sync>` 约束.
//! 我们手写 RawWaker, vtable 内的 clone/wake/wake_by_ref/drop 都用 `Rc` 引用计数.
//!
//! SlotWaker 内部持有 `Rc<RefCell<VecDeque>>` (ReadyQueueHandle), 在调度线程上调用
//! wake() 直接 push slot_id. Rc 不是 Send, 编译器保证跨线程会编译失败.

use std::rc::Rc;
use std::task::{RawWaker, RawWakerVTable, Waker};

use crate::ready::ReadyQueueHandle;

/// 内部数据: 用 Rc 包, 让 vtable 函数拿到 Rc clone.
struct SlotWakerInner {
    slot_id: usize,
    ready: ReadyQueueHandle,
}

/// vtable 4 个函数.
unsafe fn waker_clone(data: *const ()) -> RawWaker {
    let inner_ptr = data as *const SlotWakerInner;
    let rc = unsafe { Rc::from_raw(inner_ptr) };
    let rc_clone = rc.clone();
    std::mem::forget(rc);
    RawWaker::new(Rc::into_raw(rc_clone) as *const (), &VT)
}

unsafe fn waker_wake(data: *const ()) {
    let inner_ptr = data as *const SlotWakerInner;
    let rc = unsafe { Rc::from_raw(inner_ptr) };
    rc.ready.borrow_mut().push_back(rc.slot_id);
}

unsafe fn waker_wake_by_ref(data: *const ()) {
    let inner_ptr = data as *const SlotWakerInner;
    let rc = unsafe { Rc::from_raw(inner_ptr) };
    rc.ready.borrow_mut().push_back(rc.slot_id);
    std::mem::forget(rc);
}

unsafe fn waker_drop(data: *const ()) {
    let inner_ptr = data as *const SlotWakerInner;
    let _ = unsafe { Rc::from_raw(inner_ptr) };
}

static VT: RawWakerVTable =
    RawWakerVTable::new(waker_clone, waker_wake, waker_wake_by_ref, waker_drop);

/// 构造一个 Waker, 内部用 Rc<SlotWakerInner> (单线程引用计数).
pub fn make_waker(slot_id: usize, ready: &ReadyQueueHandle) -> Waker {
    let inner = Rc::new(SlotWakerInner {
        slot_id,
        ready: ready.clone(),
    });
    let raw = Rc::into_raw(inner) as *const ();
    unsafe { Waker::from_raw(RawWaker::new(raw, &VT)) }
}

pub fn make_waker_for_test(slot_id: usize, ready: &ReadyQueueHandle) -> Waker {
    make_waker(slot_id, ready)
}
