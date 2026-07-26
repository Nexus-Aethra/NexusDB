//! park / unpark API 集成测试.
//!
//! 用真实的 SchedHandle + spawn 跑全流程: spawn 一个协程调 park,
//! 然后外部调 unpark 唤醒它.
//!
//! ⭐ 所有跑 driver 的 thread 都用 `run_with_timeout` 包一层, 避免 hang 时 CI 卡死.
//! hang 视为测试失败.

use scheduler::{SchedHandle, Scheduler, park_current_coroutine, parked_count, spawn_on, unpark};

/// ⭐ 测试超时 (毫秒). hang 超过这个时间视为失败.
const TEST_TIMEOUT_MS: u64 = 2000;

fn fresh_sched() -> SchedHandle {
    SchedHandle::new(Scheduler::new())
}

/// ⭐ 简易 noop Waker (用于不需要真正唤醒的测试场景).
fn make_noop_waker() -> std::task::Waker {
    std::task::Waker::noop().clone()
}

/// ⭐ 在新线程上跑 work, 给定超时. hang 时返回 Err, 测试可观察.
fn run_with_timeout<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
    timeout_ms: u64,
    label: &str,
) -> Result<T, String> {
    use std::sync::mpsc;
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let r = work();
        let _ = tx.send(r);
    });
    match rx.recv_timeout(std::time::Duration::from_millis(timeout_ms)) {
        Ok(v) => Ok(v),
        Err(_) => Err(format!(
            "[{}] timed out after {}ms (likely hang)",
            label, timeout_ms
        )),
    }
}

/// driver 线程内: set_current + drive_until_idle, 由调用方包超时.
fn drive_iteration(sched: &SchedHandle, max_iters: usize) -> bool {
    sched.clone().drive_until_idle(max_iters)
}

/// 主测试: 在 driver 线程里 park + unpark
///
/// ⭐ 设计: 协程完成后通过 channel 通知, 避免在 driver 线程 block_on.
#[test]
fn park_then_unpark_resumes_coroutine() {
    use std::sync::mpsc;
    let result: Result<bool, String> = run_with_timeout(
        move || {
            let sched = fresh_sched();
            sched.set_current();

            let (tx, rx) = mpsc::channel::<()>();

            let _handle = spawn_on(&sched, async move {
                park_current_coroutine().await;
                let _ = tx.send(());
            });

            // 第一轮 driver: 让协程 park
            let _ = drive_iteration(&sched, 100);
            assert!(
                parked_count() >= 1,
                "expected parked > 0, got {}",
                parked_count()
            );

            // unpark 它
            assert!(unpark(0), "should have unparked a parked coroutine");

            // 第二轮 driver: 让 unpark 触发的 ready slot 被 poll, 跑完 channel send
            let _ = drive_iteration(&sched, 1000);

            // 阻塞等待 channel
            rx.recv_timeout(std::time::Duration::from_millis(500))
                .is_ok()
        },
        TEST_TIMEOUT_MS,
        "park_then_unpark_resumes_coroutine",
    );

    match result {
        Ok(true) => {}
        Ok(false) => panic!("coroutine did not finish after unpark"),
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn unpark_returns_false_when_no_parked() {
    let result: Result<bool, String> = run_with_timeout(
        move || {
            let sched = fresh_sched();
            sched.set_current();
            !unpark(999)
        },
        TEST_TIMEOUT_MS,
        "unpark_returns_false_when_no_parked",
    );
    match result {
        Ok(v) => assert!(v, "expected true (unpark of no-parked returns false)"),
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn park_outside_scheduler_context_panics() {
    // 不在 set_current 线程, 直接 poll park_current_coroutine 应 panic
    let result = std::panic::catch_unwind(|| {
        let mut park_fut = park_current_coroutine();
        let waker = make_noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        std::pin::pin!(&mut park_fut);
        let mut pinned = std::pin::pin!(park_fut);
        let _ = std::future::Future::poll(pinned.as_mut(), &mut cx);
    });
    assert!(
        result.is_err(),
        "park without scheduler context should panic"
    );
}

#[test]
fn clear_all_parked_works() {
    let result: Result<(usize, usize), String> = run_with_timeout(
        move || {
            let sched = fresh_sched();
            sched.set_current();
            let initial = parked_count();
            scheduler::clear_all_parked();
            let after_clear = parked_count();
            (initial, after_clear)
        },
        TEST_TIMEOUT_MS,
        "clear_all_parked_works",
    );
    match result {
        Ok((0, 0)) => {}
        Ok(other) => panic!("expected (0, 0), got {:?}", other),
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn register_take_and_is_parked() {
    let result: Result<bool, String> = run_with_timeout(
        move || {
            let sched = fresh_sched();
            sched.set_current();

            let initial = scheduler::parked_count();
            assert_eq!(initial, 0);

            let waker = make_noop_waker();
            scheduler::register_parked(777, waker);
            assert!(scheduler::is_parked(777));
            assert_eq!(scheduler::parked_count(), 1);

            let taken = scheduler::take_parked(777);
            assert!(taken.is_some());
            assert!(!scheduler::is_parked(777));
            assert_eq!(scheduler::parked_count(), 0);
            true
        },
        TEST_TIMEOUT_MS,
        "register_take_and_is_parked",
    );
    match result {
        Ok(true) => {}
        Ok(false) => panic!("test body returned false"),
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn multiple_parked_slots() {
    let result: Result<bool, String> = run_with_timeout(
        move || {
            let sched = fresh_sched();
            sched.set_current();

            for _ in 0..3 {
                spawn_on(&sched, async {
                    park_current_coroutine().await;
                });
            }

            drive_iteration(&sched, 100);
            assert_eq!(parked_count(), 3);

            assert!(unpark(0));
            assert!(unpark(1));
            assert!(unpark(2));
            assert_eq!(parked_count(), 0);

            drive_iteration(&sched, 100);
            true
        },
        TEST_TIMEOUT_MS,
        "multiple_parked_slots",
    );
    match result {
        Ok(true) => {}
        Ok(false) => panic!(""),
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn double_unpark_returns_false_second_time() {
    let result: Result<bool, String> = run_with_timeout(
        move || {
            let sched = fresh_sched();
            sched.set_current();

            let _handle = spawn_on(&sched, async {
                park_current_coroutine().await;
            });
            drive_iteration(&sched, 100);
            assert!(parked_count() >= 1);

            // 第一次 unpark 返回 true
            assert!(unpark(0));
            // 第二次 unpark 返回 false (registry 已空)
            assert!(!unpark(0));
            true
        },
        TEST_TIMEOUT_MS,
        "double_unpark_returns_false_second_time",
    );
    match result {
        Ok(true) => {}
        Ok(false) => panic!(""),
        Err(e) => panic!("{}", e),
    }
}
