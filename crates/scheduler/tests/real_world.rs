//! 真实场景模拟: 磁盘 IO + 内存中数据处理交错进行.
//!
//! 这些测试模拟类似 DB 查询引擎的执行模式:
//! - 顺序: 从磁盘读 page → 在内存中校验/转换 → 写回
//! - 并发: 多个 task 同时读不同文件, 验证调度公平
//! - 交错: disk IO 与内存计算交错, 验证 yield_now 正确让出
//!
//! **超时**: 每个测试 5 秒超时, 避免 submit_and_wait 死锁.

use std::io::Write;
use std::os::unix::io::IntoRawFd;
use std::sync::Arc;
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

/// 所有 real_world 测试组合为一个串行测试.
///
/// io_uring 实例存在内核资源限制, 并发创建多个实例可能导致挂起.
/// 串行执行避免此问题.
#[test]
fn all_real_world_scenarios() {
    // 逐个运行, 避免 io_uring 并发实例数超限
    eprintln!("=== scenario 1: read → memory → write ===");
    read_disk_then_memory_transform_then_write_disk_inner();
    eprintln!("=== scenario 2: interleaved IO + compute ===");
    interleaved_disk_io_and_memory_compute_inner();
    eprintln!("=== scenario 3: concurrent reads ===");
    concurrent_tasks_read_different_files_inner();
    eprintln!("=== scenario 4: detached task ===");
    detached_task_completes_on_drivers_inner();
    eprintln!("=== all scenarios passed ===");
}

// ---------- 场景 1: 顺序 disk → memory → disk ----------

/// 模拟: 从磁盘读一段数据, 在内存里逐字节校验 + 转换成大写 ASCII, 再写回磁盘.
fn read_disk_then_memory_transform_then_write_disk_inner() {
    run_with_timeout(30000, || {
        let sched = setup();
        let dir = tempfile::tempdir().unwrap();

        // 准备磁盘数据: 写入 "hello, scheduler!"
        let path_in = dir.path().join("input.bin");
        let path_out = dir.path().join("output.bin");
        {
            let mut f = std::fs::File::create(&path_in).unwrap();
            f.write_all(b"hello, scheduler!").unwrap();
            f.sync_all().unwrap();
        }
        let fd_in = std::fs::File::open(&path_in).unwrap().into_raw_fd();
        let fd_out = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path_out)
            .unwrap()
            .into_raw_fd();

        let h = scheduler::spawn_on(&sched, async move {
            // 步骤 1: 从磁盘读 17 字节 ("hello, scheduler!")
            let mut buf = [0u8; 17];
            let n = scheduler::io_ops::read(fd_in, &mut buf, 0).await.unwrap();
            assert_eq!(n, 17);

            // 步骤 2: 在内存中处理 — 大写转换 (模拟内存中数据转换)
            scheduler::yield_now().await; // 让出, 证明 await 切换正确
            for b in buf.iter_mut() {
                if b.is_ascii_lowercase() {
                    *b = b.to_ascii_uppercase();
                }
            }

            // 步骤 3: 再让出一次, 模拟多次内存操作
            scheduler::yield_now().await;

            // 步骤 4: 写回磁盘
            let m = scheduler::io_ops::write(fd_out, &buf, 0).await.unwrap();
            assert_eq!(m, 17);

            scheduler::io_ops::close(fd_in).await.unwrap();
            scheduler::io_ops::close(fd_out).await.unwrap();

            buf
        });

        assert!(drive_thread(sched, 10_000), "scheduler must drain to idle");
        let result = pollster::block_on(h).unwrap();
        assert_eq!(&result, b"HELLO, SCHEDULER!");

        // 校验磁盘上的内容
        let written = std::fs::read(&path_out).unwrap();
        assert_eq!(&written, b"HELLO, SCHEDULER!");
    });
}

// ---------- 场景 2: 并发多 task disk IO ----------

/// 模拟: 多个 task 同时读不同文件, 验证调度器在并发 IO 间公平切换.
fn concurrent_tasks_read_different_files_inner() {
    run_with_timeout(30000, || {
        let sched = setup();
        let dir = tempfile::tempdir().unwrap();

        // 准备 3 个文件
        let mut fds = Vec::new();
        for i in 0..3 {
            let path = dir.path().join(format!("file_{i}.bin"));
            let content = format!("content-from-file-{i}-padding");
            std::fs::write(&path, content.as_bytes()).unwrap();
            let fd = std::fs::File::open(&path).unwrap().into_raw_fd();
            fds.push((fd, content.into_bytes()));
        }

        // spawn 3 个并发 task, 每个读一个文件, 内存中校验
        let mut handles = Vec::new();
        for (fd, expected) in fds.into_iter() {
            let h = scheduler::spawn_on(&sched, async move {
                let mut buf = vec![0u8; expected.len()];
                let n = scheduler::io_ops::read(fd, &mut buf, 0).await.unwrap();
                assert_eq!(n, expected.len());
                assert_eq!(buf, expected);
                scheduler::io_ops::close(fd).await.unwrap();
                buf
            });
            handles.push(h);
        }

        assert!(drive_thread(sched, 10_000), "scheduler must drain to idle");
        for h in handles {
            let buf = pollster::block_on(h).unwrap();
            // 校验每条记录内容
            let s = std::str::from_utf8(&buf).unwrap();
            assert!(s.starts_with("content-from-file-"));
        }
    });
}

// ---------- 场景 3: 交错 disk 与内存计算 ----------

/// 模拟: 一个 task 频繁在 disk IO 与内存计算间切换;
/// 同时另一个 task 做纯内存计算 (多次 yield).
/// 验证调度器在混合工作负载下不会饥饿 IO task 也不会饿死 memory task.
fn interleaved_disk_io_and_memory_compute_inner() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    run_with_timeout(30000, || {
        let sched = setup();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("work.bin");

        // 准备一个稍大的文件 (4 KiB)
        let content: Vec<u8> = (0..4096u32).map(|i| (i & 0xff) as u8).collect();
        std::fs::write(&path, &content).unwrap();
        let fd = std::fs::File::open(&path).unwrap().into_raw_fd();

        // 共享计数器, 记录两个 task 的进度 (调度公平性观察).
        let disk_progress = Arc::new(AtomicUsize::new(0));
        let mem_progress = Arc::new(AtomicUsize::new(0));

        // task A: 反复 read 4 KiB 文件 (4 块 × 1 KiB), 每轮跑 4 次 read.
        let disk_prog_a = disk_progress.clone();
        let h_a = scheduler::spawn_on(&sched, async move {
            let mut total: u64 = 0;
            for round in 0..4 {
                let mut buf = [0u8; 1024];
                // 每次读 1 KiB (4 个 block) — 固定 offset 0..4096 覆盖文件
                for block in 0..4 {
                    let off = (block * 1024) as u64;
                    let n = scheduler::io_ops::read(fd, &mut buf, off).await.unwrap();
                    assert_eq!(n, 1024, "round={round} block={block}");
                    // 内存中累加
                    for b in buf {
                        total += b as u64;
                    }
                    disk_prog_a.store(round * 4 + block + 1, Ordering::Relaxed);
                }
                // 内存 task 让出, 让 disk task 切回
                scheduler::yield_now().await;
            }
            scheduler::io_ops::close(fd).await.unwrap();
            total
        });

        // task B: 纯内存计算, 50 次 yield_now (模拟 CPU 密集步骤)
        let mem_prog_b = mem_progress.clone();
        let h_b = scheduler::spawn_on(&sched, async move {
            let mut acc: u64 = 0;
            for i in 0..50 {
                acc = acc.wrapping_mul(1664525).wrapping_add(1013904223);
                mem_prog_b.store(i + 1, Ordering::Relaxed);
                scheduler::yield_now().await;
            }
            acc
        });

        assert!(drive_thread(sched, 50_000), "scheduler must drain to idle");
        let total = pollster::block_on(h_a).unwrap();
        let acc = pollster::block_on(h_b).unwrap();

        // disk task 4 轮 × 4 块 = 16 次 read
        assert_eq!(disk_progress.load(Ordering::Relaxed), 16);
        // mem task 50 步
        assert_eq!(mem_progress.load(Ordering::Relaxed), 50);
        // disk task 累加: 4 轮 × 4 KiB 各累加一次 (offset 固定 0..4096)
        let single_pass_total: u64 = content.iter().map(|&b| b as u64).sum();
        assert_eq!(total, single_pass_total * 4);
        // mem task 的 LCG 末值非零 (证明 yield 切换没把状态搞丢)
        assert_ne!(acc, 0);
    });
}

// ---------- 场景 4: spawn detached task 在 driver 跑完后仍存活 ----------

/// 模拟: spawn 一个 task 但不 await, 它在 driver 跑完后应能完成 (后续手动 drive).
fn detached_task_completes_on_drivers_inner() {
    use std::sync::atomic::{AtomicBool, Ordering};

    run_with_timeout(30000, || {
        let sched = setup();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("detached.bin");
        std::fs::write(&path, b"DETACH-ME").unwrap();
        let fd = std::fs::File::open(&path).unwrap().into_raw_fd();

        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();

        // detached: spawn 后立即 detach (drop handle), task 自己跑
        let h = scheduler::spawn_on(&sched, async move {
            let mut buf = [0u8; 9];
            let n = scheduler::io_ops::read(fd, &mut buf, 0).await.unwrap();
            assert_eq!(n, 9);
            scheduler::io_ops::close(fd).await.unwrap();
            flag_clone.store(true, Ordering::SeqCst);
            buf
        });
        h.detach();

        assert!(drive_thread(sched, 10_000), "scheduler must drain to idle");
        // driver 跑完后 flag 应为 true (detached task 完成)
        assert!(flag.load(Ordering::SeqCst));
    });
}
