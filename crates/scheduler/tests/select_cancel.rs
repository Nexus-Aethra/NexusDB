//! `select_read` 的落选 PollAdd 必须被取消，不能随着请求数耗尽 io_uring。

use std::cell::Cell;
use std::rc::Rc;

#[test]
fn repeated_select_does_not_exhaust_pending_poll_capacity() {
    let sched = scheduler::SchedHandle::new(scheduler::Scheduler::new());
    sched.set_current();
    let winner = unsafe { libc::eventfd(1, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    let loser = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    assert!(winner >= 0 && loser >= 0, "eventfd creation failed");

    // 超过 ring entry 数：每轮 winner 立即就绪，loser 的 PollAdd 会在 future drop 时取消。
    for _ in 0..(scheduler::POOL_SIZE + 128) {
        let result = Rc::new(Cell::new(0u8));
        let result_c = result.clone();
        scheduler::spawn_on(&sched, async move {
            result_c.set(scheduler::io_ops::select_read(winner, loser).await.unwrap());
        });
        assert!(sched.clone().drive_until_idle(16));
        assert_eq!(result.get(), 1);
    }

    unsafe {
        libc::close(winner);
        libc::close(loser);
    }
}
