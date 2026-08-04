//! ⭐ 回归测试: 批量提交 (攒批 submit) 的正确性 + 修复验证 (2026-08).
//!
//! 背景: 之前把 `submit_sqe!` 从"push 后立即 submit"改成"只 push 攒批"时,
//! 因 shard 的 `block_on_io` (同步忙等, 不经过驱动循环) 只 poll future 而不
//! flush SQ → SQE 滞留 → 内核不执行 → CQE 永不出现 → **shard-0 100% CPU 忙循环**.
//!
//! 修复: `submit_sqe!` 只 push + 置 `sq_pending`; 所有 CQ 扫描路径
//! (`poll_cqe` / 驱动循环 drain) 扫描前统一 `flush_sq()` 一次 submit.
//! 本测试验证两条路径都不挂死:
//!   1. 驱动循环 (`drive_until_idle`) — 攒批后 Phase C 一次 flush.
//!   2. `block_on_io` 式同步忙等 — poll 前 flush (正确性兜底).

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

/// 场景 1: 多个协程同时发读 (攒批), 用 drive_until_idle 驱动.
/// 验证: 攒批的 SQE 在 Phase C 被 flush, 所有读完成, 且无挂死/忙循环.
#[test]
fn batch_submit_driven_by_scheduler() {
    run_with_timeout(15000, || {
        let sched = SchedHandle::new(scheduler::Scheduler::new());
        sched.set_current();

        const N: usize = 16;
        let mut bufs = vec![vec![0u8; 16]; N];
        let mut owners = Vec::new();
        let mut readers = Vec::new();
        for _ in 0..N {
            let (mut a, b) = std::os::unix::net::UnixStream::pair().unwrap();
            a.write_all(b"0123456789abcdef").unwrap();
            owners.push(a);
            readers.push(b);
        }

        let done: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
        for i in 0..N {
            let d = done.clone();
            let fd = readers[i].as_raw_fd();
            let mut buf = std::mem::take(&mut bufs[i]);
            scheduler::spawn_on(&sched, async move {
                let r = scheduler::io_ops::read(fd, &mut buf, 0).await.unwrap();
                assert_eq!(r, 16, "read #{} should read 16 bytes", i);
                assert_eq!(&buf[..16], b"0123456789abcdef", "read #{} data", i);
                *d.lock().unwrap() += 1;
            });
        }

        // 驱动循环. 每轮攒批 → Phase C flush. 所有协程应完成.
        let t0 = Instant::now();
        let mut iters = 0;
        while *done.lock().unwrap() < N && iters < 100_000 {
            sched.clone().drive_until_idle(256);
            iters += 1;
        }
        eprintln!("[sched] completed={} iters={iters} elapsed={:?}", *done.lock().unwrap(), t0.elapsed());
        assert_eq!(*done.lock().unwrap(), N, "所有读协程都应完成 (批量提交 flush)");
    });
}

/// 场景 2: 直接复现原始 bug — 单个协程在 **同步忙等** (block_on_io 式) 下完成 io.
/// 之前: SQE 滞留 → CQE 不来 → 忙等挂死. 修复后: poll 前 flush → 完成.
#[test]
fn batch_submit_block_on_io_path() {
    run_with_timeout(15000, || {
        let sched = SchedHandle::new(scheduler::Scheduler::new());
        sched.set_current();

        let (mut a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        a.write_all(b"sync-path").unwrap();

        // 用 scheduler::io_ops::read 构造 future (内部 submit_sqe 只 push 攒批),
        // 然后 **不经过驱动循环**, 直接同步 poll (模拟 block_on_io).
        // 用 std::cell::Cell 持有 buffer, 让 future 与校验分离借用 (模拟 block_on_io).
        let buf: std::rc::Rc<std::cell::RefCell<[u8; 16]>> =
            std::rc::Rc::new(std::cell::RefCell::new([0u8; 16]));
        {
            let read_fd = b.as_raw_fd();
            let buf_ref = buf.clone();
            let fut = async move {
                let mut b = buf_ref.borrow_mut();
                scheduler::io_ops::read(read_fd, &mut b[..], 0)
                    .await
                    .unwrap()
            };
            let mut fut = std::pin::pin!(fut);
            let waker = std::task::Waker::noop();
            let mut cx = std::task::Context::from_waker(&waker);
            let mut spins = 0u32;
            loop {
                if let std::task::Poll::Ready(r) = fut.as_mut().poll(&mut cx) {
                    assert_eq!(r, 9, "同步路径应读到 9 字节 'sync-path'");
                    break;
                }
                spins += 1;
                if spins > 1_000_000 {
                    panic!("block_on_io 式同步忙等挂死: SQE 未 flush");
                }
                std::thread::yield_now();
            }
        }
        assert_eq!(&buf.borrow()[..9], b"sync-path", "同步路径数据");
    });
}
