//! ⭐ Phase 0 / T0.1 验证: io_uring 对 socket fd 的可用性.
//!
//! 改造目标: 网络层 worker 连接收发走 scheduler::io_ops (io_uring).
//! 关键前提: io_uring 的 Read/Write opcode 对 socket 需 offset = -1 (当前位置),
//! 否则返回 -EINVAL. 本测试验证 socket 用 u64::MAX 偏移读写正确.
//!
//! 注意: io_uring 实例有内核资源限制, 测试串行, 单 Scheduler 驱动.

use std::os::fd::AsRawFd;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn run_with_timeout(ms: u64, f: impl FnOnce() + Send + 'static) {
    let handle = std::thread::spawn(f);
    let start = Instant::now();
    let deadline = Duration::from_millis(ms);
    while !handle.is_finished() {
        if start.elapsed() > deadline {
            eprintln!("[timeout] test exceeded {}ms, aborting", ms);
            std::process::exit(1);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    handle.join().expect("test thread panicked");
}

fn setup() -> scheduler::SchedHandle {
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

/// 场景 1: 单协程用 io_uring 读 socket → 内存 → io_uring 写回另一 socket.
/// 用 socketpair: 一端写, 另一端读, 验证 io_uring socket 读写往返.
#[test]
fn socket_read_write_roundtrip() {
    run_with_timeout(20000, || {
        let sched = setup();
        let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        b.set_nonblocking(true).unwrap();
        let fd_a = a.as_raw_fd();
        let fd_b = b.as_raw_fd();

        let payload = b"hello io_uring socket";
        let expected_len = payload.len();
        let expected = payload.to_vec();
        let result: Arc<Mutex<Option<Result<(Vec<u8>, usize), String>>>> =
            Arc::new(Mutex::new(None));
        let result2 = result.clone();

        // 协程: 写 fd_a → 关写侧 → 读 fd_b.
        scheduler::spawn_on(&sched, async move {
            let r = async {
                let mut written = 0usize;
                while written < payload.len() {
                    let n = scheduler::io_ops::write(fd_a, &payload[written..], u64::MAX)
                        .await
                        .map_err(|e| e.to_string())?;
                    assert!(n > 0, "write returned 0");
                    written += n;
                }
                drop(a);
                let mut buf = vec![0u8; expected.len()];
                let mut got = 0usize;
                while got < expected.len() {
                    let n = scheduler::io_ops::read(fd_b, &mut buf[got..], u64::MAX)
                        .await
                        .map_err(|e| e.to_string())?;
                    assert!(n > 0, "unexpected EOF/0 on socket read");
                    got += n;
                }
                drop(b);
                Ok((buf, got))
            }
            .await;
            *result2.lock().unwrap() = Some(r);
        });

        let mut iters = 0;
        while result.lock().unwrap().is_none() && iters < 500_000 {
            drive_thread(sched.clone(), 4096);
            iters += 1;
        }
        let (buf, got) = result
            .lock()
            .unwrap()
            .take()
            .expect("task never completed")
            .expect("socket io_uring failed");
        assert_eq!(got, expected_len);
        assert_eq!(buf, b"hello io_uring socket".to_vec(), "socket io_uring roundtrip data mismatch");
    });
}

/// 场景 3: io_uring 读 eventfd — 验证 reply_bus / new_conn 通知也能走 io_uring.
/// worker 协程化后需用 io_uring 监听 eventfd (取代 epoll), 本测试确认其可行.
#[test]
fn io_uring_read_eventfd() {
    run_with_timeout(20000, || {
        let sched = setup();
        let efd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        assert!(efd >= 0, "eventfd failed");

        let result: Arc<Mutex<Option<Result<u64, String>>>> = Arc::new(Mutex::new(None));
        let result2 = result.clone();
        let sched = setup();

        // 协程: io_uring 读 eventfd (读回 8 字节计数).
        scheduler::spawn_on(&sched, async move {
            let r = async {
                let mut buf = [0u8; 8];
                let n = scheduler::io_ops::read(efd, &mut buf, u64::MAX)
                    .await
                    .map_err(|e| e.to_string())?;
                assert_eq!(n, 8, "eventfd read should return 8 bytes");
                Ok(u64::from_ne_bytes(buf))
            }
            .await;
            *result2.lock().unwrap() = Some(r);
        });

        // 先写 eventfd (数据已就绪, io_uring 读将立即完成), 再非阻塞驱动.
        let v: u64 = 1;
        unsafe {
            libc::write(efd, &v as *const u64 as *const libc::c_void, 8);
        }
        let mut iters = 0;
        while result.lock().unwrap().is_none() && iters < 200_000 {
            drive_thread(sched.clone(), 4096);
            iters += 1;
        }
        let got = result
            .lock()
            .unwrap()
            .take()
            .expect("eventfd read never completed")
            .expect("eventfd io_uring failed");
        assert_eq!(got, 1, "eventfd counter mismatch");
        unsafe { libc::close(efd) };
    });
}

/// 场景 4: io_ops::poll 监听 socket 可读 (协程 worker 的事件等待原语).
/// 验证 poll 在数据到达时正确唤醒, 替代 epoll 的等待.
#[test]
fn io_uring_poll_socket_readable() {
    run_with_timeout(20000, || {
        let sched = setup();
        let (mut a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        b.set_nonblocking(true).unwrap();
        let fd_b = b.as_raw_fd();

        let result: Arc<Mutex<Option<Result<u32, String>>>> = Arc::new(Mutex::new(None));
        let result2 = result.clone();

        // 协程: poll fd_b 可读 (POLLIN) → 返回触发 mask.
        scheduler::spawn_on(&sched, async move {
            let r = scheduler::io_ops::poll(fd_b, libc::POLLIN)
                .await
                .map_err(|e| e.to_string());
            *result2.lock().unwrap() = Some(r);
        });

        // 先写数据到 a 使 b 可读, 再驱动.
        std::io::Write::write_all(&mut a, b"x").unwrap();
        let mut iters = 0;
        while result.lock().unwrap().is_none() && iters < 200_000 {
            drive_thread(sched.clone(), 4096);
            iters += 1;
        }
        let got = result
            .lock()
            .unwrap()
            .take()
            .expect("poll never woke")
            .expect("poll failed");
        assert_ne!(got & libc::POLLIN as u32, 0, "should be readable (POLLIN)");
        drop(a);
        drop(b);
    });
}

/// 场景 5: select_read 同时等待 socket 与 eventfd, 验证优先返回就绪者.
#[test]
fn io_uring_select_read_two_fds() {
    run_with_timeout(20000, || {
        let sched = setup();
        // fd1 = socket pair 一端 (b), fd2 = eventfd (无通知).
        let (mut a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        b.set_nonblocking(true).unwrap();
        let fd_sock = b.as_raw_fd();
        let fd_efd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };

        let result: Arc<Mutex<Option<Result<u8, String>>>> = Arc::new(Mutex::new(None));
        let result2 = result.clone();

        scheduler::spawn_on(&sched, async move {
            let r = scheduler::io_ops::select_read(fd_sock, fd_efd)
                .await
                .map_err(|e| e.to_string());
            *result2.lock().unwrap() = Some(r);
        });

        // 写 socket 使 fd_sock 可读 (fd_efd 无通知), 应返回 1.
        std::io::Write::write_all(&mut a, b"y").unwrap();
        let mut iters = 0;
        while result.lock().unwrap().is_none() && iters < 200_000 {
            drive_thread(sched.clone(), 4096);
            iters += 1;
        }
        let got = result
            .lock()
            .unwrap()
            .take()
            .expect("select never woke")
            .expect("select failed");
        assert_eq!(got, 1, "socket readable should win (fd1)");
        drop(a);
        drop(b);
        unsafe { libc::close(fd_efd) };
    });
}

/// 场景 2: 多协程并发 socket 读写, 验证调度公平 + 数据不串.
#[test]
fn socket_concurrent_tasks() {
    run_with_timeout(30000, || {
        let sched = setup();
        let mut results: Vec<Arc<Mutex<Option<Result<Vec<u8>, String>>>>> = Vec::new();
        for i in 0..8u8 {
            let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
            a.set_nonblocking(true).unwrap();
            b.set_nonblocking(true).unwrap();
            let fd_a = a.as_raw_fd();
            let fd_b = b.as_raw_fd();
            let payload = format!("conn-{i}-payload").into_bytes();
            let expect = payload.clone();
            let result: Arc<Mutex<Option<Result<Vec<u8>, String>>>> =
                Arc::new(Mutex::new(None));
            let result2 = result.clone();
            scheduler::spawn_on(&sched, async move {
                let r = async {
                    let mut written = 0;
                    while written < payload.len() {
                        let n =
                            scheduler::io_ops::write(fd_a, &payload[written..], u64::MAX)
                                .await
                                .map_err(|e| e.to_string())?;
                        assert!(n > 0);
                        written += n;
                    }
                    drop(a);
                    let mut buf = vec![0u8; expect.len()];
                    let mut got = 0;
                    while got < expect.len() {
                        let n = scheduler::io_ops::read(fd_b, &mut buf[got..], u64::MAX)
                            .await
                            .map_err(|e| e.to_string())?;
                        assert!(n > 0);
                        got += n;
                    }
                    drop(b);
                    Ok(buf)
                }
                .await;
                *result2.lock().unwrap() = Some(r);
            });
            results.push(result);
        }
        let mut iters = 0;
        while results.iter().any(|r| r.lock().unwrap().is_none()) && iters < 1_000_000 {
            drive_thread(sched.clone(), 4096);
            iters += 1;
        }
        for (i, r) in results.iter().enumerate() {
            let got = r
                .lock()
                .unwrap()
                .take()
                .expect("task never completed")
                .expect("socket io_uring failed");
            assert_eq!(got, format!("conn-{i}-payload").into_bytes(), "mismatch");
        }
    });
}
