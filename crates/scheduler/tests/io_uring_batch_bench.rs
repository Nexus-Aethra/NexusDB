//! ⭐ 实验基准: io_uring 逐个 submit vs 批量提交 (调度器重构方向验证, 2026-08).
//!
//! 背景: 协程 worker 慢的根因是 `submit_sqe!` 逐操作立即 submit → 每个 io 操作
//! 一次 `io_uring_enter` (每请求 ~20 次 syscall, epoll 只需 ~0.15 次).
//!
//! 本实验量化 **批量提交的价值**: 相同读负载下,
//!   - 逐个: 每次 push 1 SQE + submit() (N 次 enter)
//!   - 批量: 每次 push N SQE + 一次 submit() + submit_and_wait(N) (1 次 enter)
//!
//! 结论将决定 scheduler 驱动模型重构方向.

use std::io::Write;
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};

/// N 个 socket 各自可读, 逐个 submit (每次 1 个 SQE + 1 次 enter).
#[test]
fn bench_one_by_one_submit() {
    let (mut ring, readers) = setup_ring_and_readers(64);
    let (sub, mut sq, mut cq) = ring.split();

    let mut bufs: Vec<Box<[u8]>> = (0..64).map(|_| vec![0u8; 16].into_boxed_slice()).collect();
    let mut enter_count = 0usize;

    for i in 0..64 {
        let e = opcode::Read::new(
            types::Fd(readers[i].as_raw_fd()),
            bufs[i].as_mut_ptr(),
            bufs[i].len() as _,
        )
        .build()
        .user_data(i as u64);
        unsafe { sq.push(&e).expect("push") };
        sq.sync();
        let _ = sub.submit().expect("submit");
        enter_count += 1;
        loop {
            let _ = sub.submit_and_wait(1);
            enter_count += 1;
            cq.sync();
            let mut done = false;
            for cqe in &mut cq {
                if cqe.user_data() == i as u64 {
                    done = true;
                }
            }
            if done {
                break;
            }
        }
    }
    eprintln!("[bench] 逐个 submit: 64 reads 用了 {} 次 enter", enter_count);
}

/// N 个 socket 各自可读, 批量提交 (一次 push N + 一次 submit + submit_and_wait(N)).
#[test]
fn bench_batch_submit() {
    let (mut ring, readers) = setup_ring_and_readers(64);
    let (sub, mut sq, mut cq) = ring.split();

    let mut bufs: Vec<Box<[u8]>> = (0..64).map(|_| vec![0u8; 16].into_boxed_slice()).collect();
    let mut enter_count = 0usize;

    for i in 0..64 {
        let e = opcode::Read::new(
            types::Fd(readers[i].as_raw_fd()),
            bufs[i].as_mut_ptr(),
            bufs[i].len() as _,
        )
        .build()
        .user_data(i as u64);
        unsafe { sq.push(&e).expect("push") };
    }
    sq.sync();
    let _ = sub.submit().expect("submit");
    enter_count += 1;
    let mut got = 0;
    while got < 64 {
        let _ = sub.submit_and_wait(1);
        enter_count += 1;
        cq.sync();
        for _cqe in &mut cq {
            got += 1;
        }
    }
    eprintln!("[bench] 批量 submit: 64 reads 用了 {} 次 enter", enter_count);
}

/// 吞吐: 重复 N 次批量读, 测 ops/sec (模拟 drive 批量提交).
#[test]
fn bench_batch_throughput() {
    const ITERS: usize = 1000;
    const BATCH: usize = 32;
    let (mut ring, readers) = setup_ring_and_readers(BATCH);
    let (sub, mut sq, mut cq) = ring.split();

    let mut bufs: Vec<Box<[u8]>> = (0..BATCH).map(|_| vec![0u8; 16].into_boxed_slice()).collect();

    let t0 = Instant::now();
    let mut total_ops = 0usize;
    for _ in 0..ITERS {
        for i in 0..BATCH {
            let e = opcode::Read::new(
                types::Fd(readers[i].as_raw_fd()),
                bufs[i].as_mut_ptr(),
                bufs[i].len() as _,
            )
            .build()
            .user_data(i as u64);
            unsafe { sq.push(&e).expect("push") };
        }
        sq.sync();
        let _ = sub.submit().expect("submit");
        let mut got = 0;
        while got < BATCH {
            let _ = sub.submit_and_wait(1);
            cq.sync();
            for _cqe in &mut cq {
                got += 1;
            }
        }
        total_ops += BATCH;
    }
    let elapsed = t0.elapsed();
    eprintln!(
        "[bench] 批量吞吐: {} ops in {:?} = {:.0} ops/s",
        total_ops,
        elapsed,
        total_ops as f64 / elapsed.as_secs_f64()
    );
}

fn setup_ring_and_readers(n: usize) -> (IoUring, Vec<std::os::unix::net::UnixStream>) {
    let ring = IoUring::new((n * 4).max(128) as u32).expect("io_uring setup");
    let mut readers = Vec::with_capacity(n);
    for _ in 0..n {
        let (mut a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        b.set_nonblocking(true).unwrap();
        a.write_all(b"0123456789abcdef").unwrap();
        readers.push(b);
    }
    (ring, readers)
}
