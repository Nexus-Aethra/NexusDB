//! 满载 admission 回归：第 POOL_SIZE+1 个 task 必须等待空闲 slot，不能覆盖活跃 task。

use std::cell::Cell;
use std::rc::Rc;

#[test]
fn queued_tasks_run_after_capacity_is_released() {
    let sched = scheduler::SchedHandle::new(scheduler::Scheduler::new());
    sched.set_current();
    let completed = Rc::new(Cell::new(0usize));

    for _ in 0..(scheduler::POOL_SIZE + 1) {
        let completed = completed.clone();
        scheduler::spawn_on(&sched, async move {
            completed.set(completed.get() + 1);
        });
    }

    assert!(sched.clone().drive_until_idle(8));
    assert_eq!(completed.get(), scheduler::POOL_SIZE + 1);
}
