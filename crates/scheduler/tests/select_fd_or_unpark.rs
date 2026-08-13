//! socket 事件胜出时，组合等待留下的 park waker 必须被清理。

use std::cell::Cell;
use std::rc::Rc;

#[test]
fn fd_winner_does_not_leave_a_parked_task() {
    let sched = scheduler::SchedHandle::new(scheduler::Scheduler::new());
    sched.set_current();
    let fd = unsafe { libc::eventfd(1, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    assert!(fd >= 0);
    let task_id = Rc::new(Cell::new(usize::MAX));
    let task_id_c = task_id.clone();

    scheduler::spawn_on(&sched, async move {
        task_id_c.set(scheduler::current_task_id());
        assert_eq!(scheduler::io_ops::select_fd_or_unpark(fd).await.unwrap(), 1);
    });
    assert!(sched.clone().drive_until_idle(16));
    assert!(!scheduler::is_parked(task_id.get()));

    unsafe { libc::close(fd) };
}
