use std::time::Duration;

fn fresh_sched() -> scheduler::SchedHandle {
    scheduler::SchedHandle::new(scheduler::Scheduler::new())
}

fn drive_thread(handle: scheduler::SchedHandle, max_iters: usize) -> bool {
    std::thread::spawn(move || {
        handle.set_current();
        handle.drive_until_idle(max_iters)
    })
    .join()
    .unwrap()
}

#[test]
fn spawn_and_await_returns_value() {
    let sched = fresh_sched();
    let h = scheduler::spawn_on(&sched, async { 42 + 1 });
    assert!(drive_thread(sched, 10_000), "scheduler must drain to idle");
    let ok = pollster::block_on(h);
    assert!(matches!(ok, Ok(43)));
}

#[test]
fn detached_handle_does_not_block_runner() {
    let sched = fresh_sched();
    let h = scheduler::spawn_on(&sched, async { "ok" });
    drop(h);
    assert!(drive_thread(sched, 100));
}

#[test]
fn stop_breaks_run_loop() {
    let sched = fresh_sched();
    // 取 stop_handle 在 sched 仍可借用时 (避免后续 set_current 时的 borrow 冲突).
    // SchedHandle 暴露了 stop_handle 间接路径: 拿 Rc clone 后 borrow + get stop_handle.
    let stop = {
        let s = sched.0.borrow();
        s.stop_handle()
    }; // borrow 释放
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        stop.stop();
    });
    std::thread::sleep(Duration::from_millis(30));
    // 现在 sched 没人持有, drive_until_idle 直接在主线程跑 (main 线程就是 driver).
    let s = sched.into_inner();
    let mut s = s.into_inner();
    s.run();
}
