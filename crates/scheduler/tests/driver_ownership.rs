//! 单 scheduler 只能由一个 driver thread 推进.
//!
//! ⭐ 审计 P2 (2026-10-17): 这个前提原来只靠**运行时**检查 (第二个线程
//! `drive_until_idle` 时 panic), 但 `SchedHandle` 同时是 `pub Rc` + `unsafe
//! impl Send` + 手写 `Clone` — 调用方可以 clone 后丢到另一个线程, 在运行时
//! 检查生效**之前**就已经对非原子 `Rc` 引用计数和多线程 `RefCell` 造成数据
//! 竞争 (真 UB). 现在 SchedHandle 不再实现 `Send` 且内层 Rc 私有: 跨线程
//! 移动/克隆在**编译期**即被拒绝, 该用例的场景已无法表达.
//!
//! 保留的用例验证合法模式: 同线程 clone + drive.

#[test]
fn same_thread_clone_and_drive_works() {
    let sched = scheduler::SchedHandle::new(scheduler::Scheduler::new());
    sched.set_current();
    // 同线程 clone (worker 线程内 spawn 协程的既有用法) 仍然允许
    let clone = sched.clone();
    assert!(
        clone.drive_until_idle(0),
        "空 scheduler 也应能 drive 到 idle"
    );
    assert!(sched.drive_until_idle(0), "原 handle 仍可用");
}
