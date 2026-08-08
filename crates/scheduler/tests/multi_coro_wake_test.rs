//! ⭐ 实验 5: 多协程 + 阻塞驱动 — 模拟 worker 完整协程集, 验证 conn_eventfd 唤醒.
//!
//! worker 有 3 个常驻协程:
//!   - reply_dispatch: select_read(reply_eventfd, stop_efd)
//!   - new_conn_loop: poll(conn_eventfd)
//!   - (连接协程: select_read(socket, reply_eventfd) — 连接建立后才有)
//!
//! 本实验复现前 2 个协程, 后台写 conn_eventfd (模拟 acceptor), 验证
//! new_conn_loop 被唤醒.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use scheduler::SchedHandle;

#[test]
fn multi_coro_conn_wake() {
    run_with_timeout(15000, || {
        let sched = SchedHandle::new(scheduler::Scheduler::new());
        sched.set_current();

        // 同 server.rs 的 eventfd
        let reply_eventfd =
            unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        let conn_eventfd =
            unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        let stop_efd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };

        let conn_woke: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let conn_woke2 = conn_woke.clone();

        // 协程 1: reply_dispatch (select_read reply_eventfd + stop_efd)
        scheduler::spawn_on(&sched, async move {
            let r = scheduler::io_ops::select_read(reply_eventfd, stop_efd).await;
            eprintln!("[exp5] reply_dispatch select_read: {r:?}");
        });
        // 协程 2: new_conn_loop (poll conn_eventfd)
        scheduler::spawn_on(&sched, async move {
            let r = scheduler::io_ops::poll(conn_eventfd, libc::POLLIN).await;
            eprintln!("[exp5] new_conn_loop poll(conn_eventfd): {r:?}");
            *conn_woke2.lock().unwrap() = true;
        });

        // 后台: 200ms 后写 conn_eventfd (模拟 acceptor)
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            let v: u64 = 1;
            unsafe {
                libc::write(conn_eventfd, &v as *const u64 as *const libc::c_void, 8);
            }
        });

        let t0 = Instant::now();
        let mut iters = 0;
        while !*conn_woke.lock().unwrap() && iters < 1_000_000 {
            sched.clone().drive_until_idle(4096);
            iters += 1;
            if iters % 100_000 == 0 {
                eprintln!("[exp5] iter {iters}, elapsed {:?}", t0.elapsed());
            }
        }
        eprintln!(
            "[exp5] conn_woke={} iters={iters} elapsed={:?}",
            *conn_woke.lock().unwrap(),
            t0.elapsed()
        );
        assert!(*conn_woke.lock().unwrap(), "new_conn_loop 应被 conn_eventfd 唤醒");
        unsafe {
            libc::close(reply_eventfd);
            libc::close(conn_eventfd);
            libc::close(stop_efd);
        }
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
