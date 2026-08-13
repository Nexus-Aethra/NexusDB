//! 单 scheduler 只能由一个 driver thread 推进。

#[test]
fn a_second_driver_thread_is_rejected() {
    let sched = scheduler::SchedHandle::new(scheduler::Scheduler::new());
    sched.set_current();
    assert!(sched.clone().drive_until_idle(0));

    let second = sched.clone();
    let result = std::thread::spawn(move || {
        second.set_current();
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            second.drive_until_idle(0)
        }))
    })
    .join()
    .expect("second thread itself must not abort");
    assert!(result.is_err());
}
