//! ⭐ 实验 6: io_uring_enter(0, 1, GETEVENTS) 是否阻塞等待 poll SQE 的 CQE.
//!
//! 关键疑问: scheduler 阻塞驱动 (registry 非空时 submit_and_wait(1)) 在
//! 实验 5 返回 0 (不阻塞), 但 io_uring_batch_test 证明 submit_and_wait(1)
//! 能等到 poll CQE. 本实验直接验证.

use std::io::Write;
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};

#[test]
fn submit_and_wait_blocks_for_poll_cqe() {
    let h = std::thread::spawn(|| {
        let mut ring = IoUring::new(16).expect("setup");
        let (mut a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        b.set_nonblocking(true).unwrap();

        let (sub, mut sq, mut cq) = ring.split();
        unsafe {
            sq.push(
                &opcode::PollAdd::new(types::Fd(b.as_raw_fd()), libc::POLLIN as u32)
                    .build()
                    .user_data(1),
            )
            .unwrap();
        }
        sq.sync();
        assert_eq!(sub.submit().expect("submit"), 1);

        // 后台: 300ms 后写 socket
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            a.write_all(b"data").unwrap();
        });

        // submit_and_wait(1): 应阻塞到 300ms 后 poll CQE
        let t0 = Instant::now();
        let n = sub.submit_and_wait(1).expect("submit_and_wait");
        let elapsed = t0.elapsed();
        eprintln!("[exp6] submit_and_wait(1) n={n} elapsed={elapsed:?}");
        cq.sync();
        let mut got = 0;
        for cqe in &mut cq {
            got += 1;
            assert_eq!(cqe.user_data(), 1);
        }
        assert_eq!(got, 1, "poll CQE 应到达");
        assert!(
            elapsed >= Duration::from_millis(250),
            "submit_and_wait(1) 应阻塞等 poll CQE (≥250ms), 实际 {elapsed:?}"
        );
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while !h.is_finished() {
        if Instant::now() > deadline {
            panic!("test exceeded 5s");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    h.join().unwrap();
}
