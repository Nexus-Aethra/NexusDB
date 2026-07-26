//! `AwaitPredicate` 测试.

use std::sync::mpsc;
use std::time::Duration;

use scheduler::{SchedHandle, Scheduler, await_predicate, parked_count, spawn_on, unpark};

fn fresh() -> SchedHandle {
    SchedHandle::new(Scheduler::new())
}

fn drive(sched: &SchedHandle) {
    sched.clone().drive_until_idle(1000);
}

#[test]
fn await_predicate_immediate_returns_ready() {
    let sched = fresh();
    sched.set_current();
    let hit = spawn_on(&sched, async {
        await_predicate(|| true).await;
    });
    drive(&sched);
    let _ = pollster::block_on(hit);
}

#[test]
fn await_predicate_unpark_resumes() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let sched = fresh();
    sched.set_current();

    // 1. spawn 一个让 predicate 跟随外部 flag 的协程
    let ready = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::channel::<()>();
    let ready_clone = ready.clone();
    let _handle = spawn_on(&sched, async move {
        await_predicate(move || ready_clone.load(Ordering::Acquire)).await;
        let _ = tx.send(());
    });

    // 2. drive → 协程 park (predicate false)
    drive(&sched);
    assert!(parked_count() >= 1, "expected parked > 0, got {}", parked_count());

    // 3. flip flag → 让 predicate 变 true
    ready.store(true, Ordering::Release);

    // 4. 现在直接 unpark → waker.wake() push 到 ready queue → driver re-poll → predicate true → Ready
    assert!(unpark(0), "should have unparked a parked coroutine");
    drive(&sched);

    rx.recv_timeout(Duration::from_millis(500))
        .expect("coroutine should send after predicate true + unpark");
}

#[test]
fn await_predicate_predicate_becomes_true_wakes_via_drive() {
    use std::cell::Cell;
    use std::rc::Rc;
    let sched = fresh();
    sched.set_current();

    let flag = Rc::new(Cell::new(false));
    let check = {
        let flag = flag.clone();
        move || flag.get()
    };

    let (tx, rx) = mpsc::channel::<()>();
    let _handle = spawn_on(&sched, async move {
        await_predicate(check).await;
        let _ = tx.send(());
    });

    // 第一轮 drive 让协程 park (predicate false)
    drive(&sched);
    assert!(parked_count() >= 1);

    // 改 flag → true, 然后 unpark 让 future 再次被 poll
    flag.set(true);
    assert!(unpark(0));

    drive(&sched);

    rx.recv_timeout(Duration::from_millis(500))
        .expect("coroutine should resume after predicate true");
}

#[test]
fn await_predicate_no_unpark_stays_parked() {
    let sched = fresh();
    sched.set_current();

    let (tx, rx) = mpsc::channel::<()>();
    let _handle = spawn_on(&sched, async move {
        await_predicate(|| false).await;
        let _ = tx.send(());
    });

    drive(&sched);
    // drive 多次不会让 predicate 变 true, 协程保持 parked.
    for _ in 0..3 {
        drive(&sched);
    }
    let recv = rx.recv_timeout(Duration::from_millis(50));
    assert!(recv.is_err(), "no unpark should mean no completion");
}
