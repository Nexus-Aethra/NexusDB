//! ⭐ G0: 低优先级协程调度测试.
//!
//! 语义:
//! - `spawn_on_low` 协程在每个调度 wave 内排在普通协程之后
//! - 每 wave 至多 poll `LOW_PRIO_BUDGET` (=1) 个低优先级协程
//! - 无低优先级协程时调度行为与之前完全一致 (分区 no-op)

use std::cell::RefCell;
use std::rc::Rc;

use scheduler::{SchedHandle, Scheduler, spawn_on, spawn_on_low};

/// 普通协程在 wave 内先于低优先级协程执行 (即使 low 先 spawn).
#[test]
fn normal_polls_before_low_in_same_wave() {
    let rt = SchedHandle::new(Scheduler::new());
    rt.set_current();

    let order: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));

    // 先 spawn low, 再 spawn 普通 — 同一批进 pool + ready
    let o1 = order.clone();
    let h_low = spawn_on_low(&rt, async move {
        o1.borrow_mut().push("low");
    });
    let o2 = order.clone();
    let h_a = spawn_on(&rt, async move {
        o2.borrow_mut().push("normal_a");
    });
    let o3 = order.clone();
    let h_b = spawn_on(&rt, async move {
        o3.borrow_mut().push("normal_b");
    });

    assert!(rt.clone().drive_until_idle(1000), "must drain to idle");
    pollster::block_on(h_low).unwrap();
    pollster::block_on(h_a).unwrap();
    pollster::block_on(h_b).unwrap();

    let got = order.borrow().clone();
    assert_eq!(
        got,
        vec!["normal_a", "normal_b", "low"],
        "普通协程必须先于低优先级完成"
    );
}

/// budget=1: 多个低优先级协程跨多个 wave 分批完成, 但最终全部完成.
#[test]
fn low_budget_spreads_across_waves_and_completes() {
    let rt = SchedHandle::new(Scheduler::new());
    rt.set_current();

    let done: Rc<RefCell<usize>> = Rc::new(RefCell::new(0));
    let mut handles = Vec::new();
    for _ in 0..5 {
        let d = done.clone();
        handles.push(spawn_on_low(&rt, async move {
            *d.borrow_mut() += 1;
        }));
    }

    assert!(rt.clone().drive_until_idle(1000), "must drain to idle");
    for h in handles {
        pollster::block_on(h).unwrap();
    }
    assert_eq!(*done.borrow(), 5, "所有低优先级协程最终完成 (无饿死)");
}

/// 混合负载: 大量普通 + 少量 low, 全部完成且 JoinHandle 语义一致.
#[test]
fn mixed_load_all_complete() {
    let rt = SchedHandle::new(Scheduler::new());
    rt.set_current();

    let mut normals = Vec::new();
    let mut lows = Vec::new();
    for i in 0..20u32 {
        normals.push(spawn_on(&rt, async move { i * 2 }));
    }
    for i in 0..3u32 {
        lows.push(spawn_on_low(&rt, async move { i + 100 }));
    }

    assert!(rt.clone().drive_until_idle(2000), "must drain to idle");
    for (i, h) in normals.into_iter().enumerate() {
        assert_eq!(pollster::block_on(h).unwrap(), (i as u32) * 2);
    }
    for (i, h) in lows.into_iter().enumerate() {
        assert_eq!(pollster::block_on(h).unwrap(), (i as u32) + 100);
    }
}

/// 低优先级协程含 await 点 (yield) 时仍能推进完成.
#[test]
fn low_with_yield_progresses() {
    let rt = SchedHandle::new(Scheduler::new());
    rt.set_current();

    let h = spawn_on_low(&rt, async move {
        scheduler::yield_now().await;
        scheduler::yield_now().await;
        42u32
    });
    // 同时有普通协程持续占据 wave
    let mut normals = Vec::new();
    for _ in 0..10 {
        normals.push(spawn_on(&rt, async move {
            scheduler::yield_now().await;
        }));
    }

    assert!(rt.clone().drive_until_idle(2000), "must drain to idle");
    assert_eq!(pollster::block_on(h).unwrap(), 42);
    for h in normals {
        pollster::block_on(h).unwrap();
    }
}
