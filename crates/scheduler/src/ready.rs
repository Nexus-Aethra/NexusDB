//! Ready queue: 调度线程本地的待 poll 协程列表.
//!
//! **设计**: `Rc<RefCell<VecDeque<usize>>>`, 让 waker 和 scheduler 共享同一份数据.
//! 内部 `VecDeque` 是 owned, 零额外分配. `RefCell` 仅提供内部可变性, **不提供同步** —
//! 所有调用都在调度线程上 (Rc 不是 Send, 编译器强制).

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

pub type ReadyQueueHandle = Rc<RefCell<VecDeque<usize>>>;

pub fn new_handle() -> ReadyQueueHandle {
    Rc::new(RefCell::new(VecDeque::new()))
}

pub fn push(handle: &ReadyQueueHandle, slot_id: usize) {
    handle.borrow_mut().push_back(slot_id);
}

pub fn drain(handle: &ReadyQueueHandle) -> VecDeque<usize> {
    std::mem::take(&mut *handle.borrow_mut())
}

pub fn has_any(handle: &ReadyQueueHandle) -> bool {
    !handle.borrow().is_empty()
}

pub fn len(handle: &ReadyQueueHandle) -> usize {
    handle.borrow().len()
}
