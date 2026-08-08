//! ⭐ 实验 4: 精确复现 worker 挂起 — conn_eventfd poll + 后台写 + 阻塞驱动.
//!
//! worker 挂起特征: new_conn_loop 的 poll(conn_eventfd) 不被唤醒 (acceptor
//! 写 conn_eventfd 后 CQE 不来). 本实验直接复现该场景.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use scheduler::SchedHandle;

/// 创建 conn_eventfd (同 server.rs), 协程 poll 它, 后台线程写它 (模拟 acceptor),
/// 用 drive_until_idle (阻塞重构版) 驱动, 验证 poll 唤醒.
#[test]
fn conn_eventfd_poll_wakes() {
    run_with_timeout(15000, || {
        let sched = SchedHandle::new(scheduler::Scheduler::new());
        sched.set_current();

        // 同 server.rs 的 conn_eventfd 创建
        let conn_eventfd =
            unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        assert!(conn_eventfd >= 0);

        let woke: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let woke2 = woke.clone();

        // new_conn_loop 协程: poll conn_eventfd (同 worker_coro)
        scheduler::spawn_on(&sched, async move {
            let r = scheduler::io_ops::poll(conn_eventfd, libc::POLLIN).await;
            eprintln!("[exp4] poll(conn_eventfd) returned: {r:?}");
            *woke2.lock().unwrap() = true;
        });

        // 后台线程: 200ms 后写 conn_eventfd (模拟 acceptor notify_worker)
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            let val: u64 = 1;
            unsafe {
                let n = libc::write(
                    conn_eventfd,
                    &val as *const u64 as *const libc::c_void,
                    8,
                );
                eprintln!("[exp4] wrote conn_eventfd: n={n}");
            }
        });

        // 阻塞驱动 (重构版)
        let t0 = Instant::now();
        let mut iters = 0;
        while !*woke.lock().unwrap() && iters < 1_000_000 {
            sched.clone().drive_until_idle(4096);
            iters += 1;
            if iters % 100_000 == 0 {
                eprintln!("[exp4] iter {iters}, elapsed {:?}", t0.elapsed());
            }
        }
        let elapsed = t0.elapsed();
        eprintln!(
            "[exp4] woke={} iters={iters} elapsed={elapsed:?}",
            *woke.lock().unwrap()
        );
        assert!(*woke.lock().unwrap(), "poll(conn_eventfd) 应被唤醒");
        unsafe { libc::close(conn_eventfd) };
    });
}

fn run_with_timeout(ms: u64, f: impl FnOnce() + Send + 'static) {
    let h = std::thread::spawn(f);
    let deadline = Instant::now() + Duration::from_millis(ms);
    while !h.is_finished() {
        if Instant::now() > deadline {
            panic!("test exceeded {}ms", ms);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    h.join().unwrap();
}
