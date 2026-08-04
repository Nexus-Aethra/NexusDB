//! ⭐ 实验 3: scheduler 阻塞驱动 (registry 非空时 submit_and_wait) 的唤醒正确性.
//!
//! 背景: 重构 drive 后 worker_coro_e2e 挂起 — worker 提交 poll(conn_eventfd)
//! 等 SQE 后阻塞等 CQE, 但 acceptor 写 conn_eventfd 后 CQE 不来 → 死锁.
//!
//! 本实验复现: scheduler 里 poll eventfd + poll socket 的协程, 后台线程
//! 写 eventfd + socket, 用 drive_until_idle (重构后阻塞 drain) 驱动,
//! 验证协程是否正确被唤醒.

use std::io::Write;
use std::os::fd::AsRawFd;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use scheduler::SchedHandle;

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

/// 场景: 2 个协程 poll (eventfd + socket), 后台线程写两者.
/// 用 drive_until_idle 驱动 (重构后的阻塞 drain), 验证唤醒.
#[test]
fn blocking_drive_wakes_pollers() {
    run_with_timeout(15000, || {
        let sched = SchedHandle::new(scheduler::Scheduler::new());
        sched.set_current();

        let efd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        assert!(efd >= 0);
        let (mut a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        b.set_nonblocking(true).unwrap();

        let efd_wake: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let sock_wake: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let e1 = efd_wake.clone();
        let e2 = sock_wake.clone();

        // 协程 1: poll eventfd
        scheduler::spawn_on(&sched, async move {
            let _ = scheduler::io_ops::poll(efd, libc::POLLIN).await;
            *e1.lock().unwrap() = true;
        });
        // 协程 2: poll socket
        scheduler::spawn_on(&sched, async move {
            let _ = scheduler::io_ops::poll(b.as_raw_fd(), libc::POLLIN).await;
            *e2.lock().unwrap() = true;
        });

        // 后台线程: 200ms 后写 eventfd + socket
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            let v: u64 = 1;
            unsafe {
                libc::write(efd, &v as *const u64 as *const libc::c_void, 8);
            }
            a.write_all(b"x").unwrap();
        });

        // 用 drive_until_idle 驱动 (重构后: registry 非空时 submit_and_wait 阻塞).
        let t0 = Instant::now();
        let mut iters = 0;
        while (!*efd_wake.lock().unwrap() || !*sock_wake.lock().unwrap()) && iters < 1_000_000 {
            sched.clone().drive_until_idle(4096);
            iters += 1;
            if iters % 100_000 == 0 {
                eprintln!("[wake] iter {iters}, elapsed {:?}", t0.elapsed());
            }
        }
        let elapsed = t0.elapsed();
        eprintln!(
            "[wake] eventfd_wake={} sock_wake={} iters={iters} elapsed={elapsed:?}",
            *efd_wake.lock().unwrap(),
            *sock_wake.lock().unwrap()
        );
        assert!(
            *efd_wake.lock().unwrap() && *sock_wake.lock().unwrap(),
            "两个 poll 协程都应被唤醒"
        );
        assert!(
            elapsed >= Duration::from_millis(150),
            "应等待后台写 (至少 150ms), 实际 {elapsed:?}"
        );
        unsafe { libc::close(efd) };
    });
}
