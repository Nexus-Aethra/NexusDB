//! ⭐ 实验 2: 数据未就绪时批量提交 + 阻塞等 CQE 的正确模式 (2026-08).
//!
//! 背景: 之前全局批量 submit 破坏 shard 的根因之一 — 数据未就绪的 io
//! (poll/read 等数据), `submit()` 后 CQE 不可见, 若驱动不阻塞等 CQE 则忙循环.
//!
//! 本实验验证正确的驱动模式:
//!   - 批量 push 多个 SQE (部分数据就绪, 部分未就绪)
//!   - sq.sync() + 一次 submit()
//!   - registry 非空时: submit_and_wait(N) **阻塞**等 CQE (不忙循环)
//!   - 数据到达后, 未就绪的 io 完成

use std::io::Write;
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};

/// 混合场景: 2 个 socket 数据已就绪 + 2 个未就绪, 批量 poll + read.
/// 验证: submit 后已就绪的立即完成; 未就绪的等数据到达 + submit_and_wait 阻塞等.
#[test]
fn mixed_ready_and_pending_batch() {
    run_with_timeout(15000, || {
        let mut ring = IoUring::new(32).expect("setup");
        // 2 就绪 + 2 未就绪
        let (mut ra1, rb1) = std::os::unix::net::UnixStream::pair().unwrap();
        let (mut ra2, rb2) = std::os::unix::net::UnixStream::pair().unwrap();
        let (pa, pb1) = std::os::unix::net::UnixStream::pair().unwrap(); // 未就绪1
        let (qa, qb1) = std::os::unix::net::UnixStream::pair().unwrap(); // 未就绪2
        ra1.write_all(b"AAAA").unwrap();
        ra2.write_all(b"BBBB").unwrap();
        let mut buf1 = [0u8; 8];
        let mut buf2 = [0u8; 8];
        let (mut pa, mut qa) = (pa, qa);

        let (sub, mut sq, mut cq) = ring.split();
        // 批量 push: 2 个 read (就绪) + 2 个 poll (未就绪)
        unsafe {
            sq.push(
                &opcode::Read::new(types::Fd(rb1.as_raw_fd()), buf1.as_mut_ptr(), 8)
                    .build()
                    .user_data(1),
            )
            .unwrap();
            sq.push(
                &opcode::Read::new(types::Fd(rb2.as_raw_fd()), buf2.as_mut_ptr(), 8)
                    .build()
                    .user_data(2),
            )
            .unwrap();
            sq.push(
                &opcode::PollAdd::new(types::Fd(pb1.as_raw_fd()), libc::POLLIN as u32)
                    .build()
                    .user_data(3),
            )
            .unwrap();
            sq.push(
                &opcode::PollAdd::new(types::Fd(qb1.as_raw_fd()), libc::POLLIN as u32)
                    .build()
                    .user_data(4),
            )
            .unwrap();
        }
        sq.sync();
        let n = sub.submit().expect("submit");
        assert_eq!(n, 4, "一次 submit 提交 4 个 SQE");

        // 等 CQE: 已就绪的 read 应立即完成
        let mut got = 0u64;
        let deadline = Instant::now() + Duration::from_secs(5);
        while got < 4 && Instant::now() < deadline {
            let _ = sub.submit_and_wait(1); // 阻塞等至少 1 个 CQE (关键: 不忙循环)
            cq.sync();
            for cqe in &mut cq {
                got += 1;
                eprintln!("[mixed] CQE ud={} res={}", cqe.user_data(), cqe.result());
                // 等前 2 个 read 完成后, 触发未就绪的 poll
                if got == 2 {
                    pa.write_all(b"CC").unwrap();
                    qa.write_all(b"DD").unwrap();
                }
            }
        }
        assert_eq!(got, 4, "4 个 io 都应完成 (含数据到达后完成的 poll)");
        assert_eq!(&buf1[..4], b"AAAA");
        assert_eq!(&buf2[..4], b"BBBB");
    });
}

/// 验证: 全部 io 未就绪时, submit_and_wait 阻塞 (不忙循环, 不挂死), 数据到达后完成.
#[test]
fn all_pending_blocking_wait() {
    run_with_timeout(15000, || {
        let mut ring = IoUring::new(16).expect("setup");
        let (mut a1, b1) = std::os::unix::net::UnixStream::pair().unwrap();
        let (mut a2, b2) = std::os::unix::net::UnixStream::pair().unwrap();

        let (sub, mut sq, mut cq) = ring.split();
        unsafe {
            sq.push(
                &opcode::PollAdd::new(types::Fd(b1.as_raw_fd()), libc::POLLIN as u32)
                    .build()
                    .user_data(1),
            )
            .unwrap();
            sq.push(
                &opcode::PollAdd::new(types::Fd(b2.as_raw_fd()), libc::POLLIN as u32)
                    .build()
                    .user_data(2),
            )
            .unwrap();
        }
        sq.sync();
        assert_eq!(sub.submit().expect("submit"), 2);

        // 全部未就绪: submit_and_wait 应阻塞 (不忙循环). 用后台线程写数据.
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            a1.write_all(b"x").unwrap();
            a2.write_all(b"y").unwrap();
        });

        let t0 = Instant::now();
        let mut got = 0;
        while got < 2 {
            let _ = sub.submit_and_wait(1); // 阻塞等, 应 ~200ms 后完成
            cq.sync();
            for _cqe in &mut cq {
                got += 1;
            }
        }
        let elapsed = t0.elapsed();
        eprintln!("[pending] 全部未就绪: 完成 2 个 poll 用 {:?}", elapsed);
        assert!(
            elapsed >= Duration::from_millis(150),
            "应阻塞等待数据 (至少 150ms), 实际 {elapsed:?}"
        );
        assert_eq!(got, 2);
    });
}

/// 验证: SQ 满时 push 的行为 (io_uring 返回错误, 需 flush).
#[test]
fn sq_full_behavior() {
    run_with_timeout(15000, || {
        // 小 SQ (4 entries) 验证满的行为
        let mut ring = IoUring::new(4).expect("setup");
        let mut sockets = Vec::new();
        for _ in 0..4 {
            let (mut a, b) = std::os::unix::net::UnixStream::pair().unwrap();
            a.write_all(b"data").unwrap();
            sockets.push(b);
        }
        let mut bufs: Vec<Box<[u8]>> = (0..4).map(|_| vec![0u8; 8].into_boxed_slice()).collect();
        let (sub, mut sq, mut cq) = ring.split();

        // push 5 个 (超容量): 第 5 个应失败
        let mut pushes_ok = 0;
        for i in 0..5 {
            let e = opcode::Read::new(
                types::Fd(sockets[i.min(3)].as_raw_fd()),
                bufs[i.min(3)].as_mut_ptr(),
                8,
            )
            .build()
            .user_data(i as u64);
            match unsafe { sq.push(&e) } {
                Ok(()) => pushes_ok += 1,
                Err(e) => {
                    eprintln!("[sqfull] push #{} 失败: {e:?}", i);
                    // 满时需 flush: submit 清 SQ 再继续
                    sq.sync();
                    let _ = sub.submit().expect("flush");
                    break;
                }
            }
        }
        sq.sync();
        eprintln!("[sqfull] 成功 push {} 个 (SQ 容量 4)", pushes_ok);
        assert!(pushes_ok <= 4, "不能 push 超过 SQ 容量");

        // 完成已 push 的
        let mut got = 0;
        let deadline = Instant::now() + Duration::from_secs(3);
        while got < pushes_ok && Instant::now() < deadline {
            let _ = sub.submit_and_wait(1);
            cq.sync();
            for _cqe in &mut cq {
                got += 1;
            }
        }
        eprintln!("[sqfull] 完成 {} 个", got);
        assert_eq!(got, pushes_ok);
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
