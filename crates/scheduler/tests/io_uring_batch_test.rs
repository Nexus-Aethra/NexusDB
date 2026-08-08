//! ⭐ 研究测试: io_uring 批量提交的正确使用方式 (2026-08).
//!
//! 背景: 协程 worker 慢的主因是每个 io 操作一次 `io_uring_enter` (syscall),
//! 而 io_uring 的设计本意是"一次 enter 提交多个 SQE". 本测试用原始 `IoUring`
//! API (不经过 scheduler) 验证:
//!
//! 1. **批量提交正确性**: push 多个 SQE → 一次 `submit()` → 所有操作完成.
//! 2. **`submit()` (want=0) vs `submit_and_wait(N)` 语义**: 提交时机与 CQE 到达.
//! 3. **数据未就绪时 submit()**: CQE 不立即可见, 需后续 enter.
//! 4. **split() 用法**: sq/cq 独立访问 (避免借用冲突).

use std::io::Write;
use std::os::fd::AsRawFd;
use std::time::Duration;

use io_uring::{IoUring, opcode, types};

/// push 2 个 read SQE → 一次 submit() → 2 个 CQE 全部到达.
#[test]
fn batch_push_one_submit_all_complete() {
    run_with_timeout(15000, || {
        let mut ring = IoUring::new(8).expect("io_uring setup");
        let (mut a1, b1) = std::os::unix::net::UnixStream::pair().unwrap();
        let (mut a2, b2) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut buf1 = [0u8; 16];
        let mut buf2 = [0u8; 16];

        let e1 = opcode::Read::new(types::Fd(b1.as_raw_fd()), buf1.as_mut_ptr(), buf1.len() as _)
            .build()
            .user_data(1);
        let e2 = opcode::Read::new(types::Fd(b2.as_raw_fd()), buf2.as_mut_ptr(), buf2.len() as _)
            .build()
            .user_data(2);
        // 分离 sq/cq (split 返回 (Submitter, SubmissionQueue, CompletionQueue))
        let (sub, mut sq, mut cq) = ring.split();
        // ---- push 2 个 SQE (不 submit) ----
        unsafe {
            sq.push(&e1).expect("push1");
            sq.push(&e2).expect("push2");
        }
        // drop(sq) 自动 sync tail
        drop(sq);

        // 对端写入 (触发 read)
        a1.write_all(b"hello-1").unwrap();
        a2.write_all(b"hello-2").unwrap();

        // ---- 一次 submit(): 提交 2 个 SQE (单次 io_uring_enter) ----
        let submitted = sub.submit().expect("submit batch");
        assert_eq!(submitted, 2, "一次 submit 应提交 2 个 SQE");

        // ---- 等 2 个 CQE ----
        let mut got = 0;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while got < 2 && std::time::Instant::now() < deadline {
            cq.sync();
            for cqe in &mut cq {
                got += 1;
                eprintln!("[batch] CQE ud={} res={}", cqe.user_data(), cqe.result());
            }
            if got < 2 {
                let _ = sub.submit_and_wait(1); // 非 sqpoll 需 enter 处理
            }
        }
        assert_eq!(got, 2, "两个 read 都应完成");
        assert_eq!(&buf1[..7], b"hello-1");
        assert_eq!(&buf2[..7], b"hello-2");
    });
}

/// submit() (want=0): 数据已就绪时, CQE 应立即可见 (无需额外 enter).
#[test]
fn submit_then_completion_async() {
    run_with_timeout(15000, || {
        let mut ring = IoUring::new(8).expect("io_uring setup");
        let (mut a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut buf = [0u8; 32];
        let e = opcode::Read::new(types::Fd(b.as_raw_fd()), buf.as_mut_ptr(), buf.len() as _)
            .build()
            .user_data(7);
        let (sub, mut sq, mut cq) = ring.split();
        unsafe { sq.push(&e).expect("push") };
        drop(sq);
        a.write_all(b"ping").unwrap();

        let submitted = sub.submit().expect("submit");
        assert_eq!(submitted, 1);

        // 数据已就绪: CQE 应已可见
        cq.sync();
        let mut n = 0;
        for cqe in &mut cq {
            n += 1;
            assert_eq!(cqe.user_data(), 7);
            assert!(cqe.result() >= 4);
        }
        assert_eq!(n, 1, "数据就绪时 submit() 后 CQE 立即可见");
        assert_eq!(&buf[..4], b"ping");
    });
}

/// 数据**未就绪**时 submit(): CQE 不立即可见; 数据到达后需 enter 才完成.
#[test]
fn poll_not_ready_waits_for_data() {
    run_with_timeout(15000, || {
        let mut ring = IoUring::new(8).expect("io_uring setup");
        let (mut a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        let fd = b.as_raw_fd();

        let e = opcode::PollAdd::new(types::Fd(fd), libc::POLLIN as u32).build().user_data(3);
        let (sub, mut sq, mut cq) = ring.split();
        unsafe { sq.push(&e).expect("push") };
        drop(sq);
        let submitted = sub.submit().expect("submit");
        assert_eq!(submitted, 1);

        // 数据未就绪: CQE 不应可见
        cq.sync();
        assert_eq!(cq.by_ref().count(), 0, "数据未就绪时 poll 不应完成");

        // 写数据 → 非 sqpoll 需 enter 处理完成
        a.write_all(b"data").unwrap();
        let _ = sub.submit_and_wait(1).expect("wait");
        cq.sync();
        let mut n = 0;
        for cqe in &mut cq {
            n += 1;
            assert_eq!(cqe.user_data(), 3);
            assert!(cqe.result() & (libc::POLLIN as i32) != 0, "POLLIN");
        }
        assert_eq!(n, 1, "写数据后 poll 完成");
    });
}

/// push 2 个 read → 一次 submit_and_wait(2): 单次 enter 提交+等 2 个 CQE.
#[test]
fn batch_push_submit_and_wait_all() {
    run_with_timeout(15000, || {
        let mut ring = IoUring::new(8).expect("io_uring setup");
        let (mut a1, b1) = std::os::unix::net::UnixStream::pair().unwrap();
        let (mut a2, b2) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut buf1 = [0u8; 8];
        let mut buf2 = [0u8; 8];
        a1.write_all(b"11111111").unwrap();
        a2.write_all(b"22222222").unwrap();

        let e1 = opcode::Read::new(types::Fd(b1.as_raw_fd()), buf1.as_mut_ptr(), buf1.len() as _)
            .build()
            .user_data(1);
        let e2 = opcode::Read::new(types::Fd(b2.as_raw_fd()), buf2.as_mut_ptr(), buf2.len() as _)
            .build()
            .user_data(2);
        let (sub, mut sq, mut cq) = ring.split();
        unsafe {
            sq.push(&e1).expect("push1");
            sq.push(&e2).expect("push2");
        }
        drop(sq);

        let waited = sub.submit_and_wait(2).expect("submit_and_wait");
        assert_eq!(waited, 2, "应提交 2 个 SQE");

        cq.sync();
        let mut n = 0;
        let mut res_sum = 0i32;
        for cqe in &mut cq {
            n += 1;
            res_sum += cqe.result();
        }
        assert_eq!(n, 2, "2 个 CQE");
        assert_eq!(res_sum, 16, "两读共 16 字节");
        assert_eq!(&buf1, b"11111111");
        assert_eq!(&buf2, b"22222222");
    });
}

fn run_with_timeout(ms: u64, f: impl FnOnce() + Send + 'static) {
    let h = std::thread::spawn(f);
    let deadline = std::time::Instant::now() + Duration::from_millis(ms);
    while !h.is_finished() {
        if std::time::Instant::now() > deadline {
            panic!("test exceeded {}ms", ms);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    h.join().unwrap();
}
