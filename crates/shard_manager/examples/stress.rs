//! 多 client 多 shard 并发压力测试 + Benchmark.
//!
//! ## 工作负载
//!
//! - 6 shards, 每个 shard 独立 io_uring 线程
//! - N 个 client threads (默认 6) 并发跑多 phase:
//!   - Phase 1: warmup — 每 client put 200 keys
//!   - Phase 2: mixed — 每 client (put 50% / get 30% / delete 20%) × ops
//!   - Phase 3: 每 client put 100 verify keys
//!   - Phase 4: 重读 phase 3 put 的 keys 验证
//!
//! ## 用法
//!
//! ```bash
//! RUST_MIN_STACK=67108864 cargo run --release --example stress -- [ops_per_client]
//! ```

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Instant;

use shard_manager::{BatchOp, BatchResult, ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

const NUM_SHARDS: usize = 6;
const DEFAULT_OPS_PHASE2: usize = 5000;
const VALUE_LEN: usize = 32;
const BATCH_SIZE: usize = 64;

fn main() {
    let args: Vec<String> = env::args().collect();
    let ops_phase2: usize = args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_OPS_PHASE2);
    let num_clients: usize = args
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(
            args.get(3)
                .and_then(|s| s.parse().ok())
                .unwrap_or(NUM_SHARDS),
        );

    println!("=== NexusDB 多 shard 多 client 并发压测 ===");
    println!("shards: {NUM_SHARDS}");
    println!("clients: {num_clients}");
    println!("ops/phase-2 (mixed): {ops_phase2}");
    println!(
        "phase 2 total ops: {}",
        ops_phase2 * num_clients
    );
    println!();

    let tmp = tempfile::tempdir().expect("create tempdir");
    let opts = ShardManagerOptions {
        num_shards: NUM_SHARDS,
        block_root: tmp.path().to_path_buf(),
        create_if_missing: true,
        io_backend: IoBackend::IoUring,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 16,
        reply_bus_count: None,
        wal_mode: Default::default(),
    };

    println!("[setup] opening ShardManager with {NUM_SHARDS} shards...");
    let setup_start = Instant::now();
    let mgr = Arc::new(ShardManager::open(opts).expect("open"));
    mgr.create_db("bench").expect("create db");
    mgr.create_table("bench", "kv").expect("create table");
    println!(
        "[setup] done in {:.3}s",
        setup_start.elapsed().as_secs_f64()
    );
    println!();

    // ---- 阶段 1: warmup put ----
    {
        println!("[phase 1] warmup: {num_clients} clients × 200 put each ...");
        let barrier = Arc::new(Barrier::new(num_clients));
        let total = Arc::new(AtomicU64::new(0));
        let s = Instant::now();
        let handles: Vec<_> = (0..num_clients)
            .map(|tid| {
                let mgr = mgr.clone();
                let barrier = barrier.clone();
                let total = total.clone();
                thread::Builder::new()
                    .name(format!("client-{tid}"))
                    .stack_size(4 * 1024 * 1024)
                    .spawn(move || {
                        barrier.wait();
                        for i in 0..200 {
                            let key = format!("warmup_t{tid}_{i:06}");
                            let v = vec![(i & 0xFF) as u8; VALUE_LEN];
                            if mgr.put("bench", "kv", key.as_bytes(), &v, 0).is_ok() {
                                total.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    })
                    .expect("spawn")
            })
            .collect();
        for h in handles {
            h.join().expect("join");
        }
        println!(
            "[phase 1] done in {:.3}s, ops: {}",
            s.elapsed().as_secs_f64(),
            total.load(Ordering::Relaxed)
        );
        println!();
    }

    // ---- 阶段 2: submit_tasks 直连路径 (统一架构) ----
    println!(
        "[phase 2] mixed workload (submit_tasks): {num_clients} clients × {ops_phase2} ops, 60/25/15 ..."
    );
    let barrier = Arc::new(Barrier::new(num_clients));
    let total_put = Arc::new(AtomicU64::new(0));
    let total_get = Arc::new(AtomicU64::new(0));
    let total_delete = Arc::new(AtomicU64::new(0));
    let total_ops = Arc::new(AtomicU64::new(0));
    let total_errors = Arc::new(AtomicU64::new(0));
    let agg_lats_phase2 = Arc::new(Mutex::new(Vec::<(u8, f64)>::with_capacity(
        ops_phase2 * num_clients,
    )));

    let handles: Vec<_> = (0..num_clients)
        .map(|tid| {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            let total_put = total_put.clone();
            let total_get = total_get.clone();
            let total_delete = total_delete.clone();
            let total_ops = total_ops.clone();
            let total_errors = total_errors.clone();
            let agg_lats = agg_lats_phase2.clone();
            thread::Builder::new()
                .name(format!("client-{tid}"))
                .stack_size(4 * 1024 * 1024)
                .spawn(move || {
                    barrier.wait();
                    let mut rng_state: u64 = 0x12345678_9ABCDEF0u64
                        .wrapping_add((tid as u64).wrapping_mul(0x9E3779B97F4A7C15u64));
                    let mut next = || {
                        rng_state = rng_state
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        rng_state
                    };
                    let mut local = Vec::with_capacity(ops_phase2);
                    let mut batch_buf: Vec<BatchOp> = Vec::with_capacity(BATCH_SIZE);

                    for i in 0..ops_phase2 {
                        let r = (next() >> 32) % 100;
                        let op = if r < 60 { 0 } else if r < 85 { 1 } else { 2 };
                        let key = format!("t{tid}_{i:08}");

                        let batch_op = if op == 0 {
                            let v = vec![((next() >> 32) & 0xFF) as u8; VALUE_LEN];
                            BatchOp::Put {
                                db: std::sync::Arc::from("bench"),
                                table: std::sync::Arc::from("kv"),
                                key: key.into_bytes(),
                                val: v,
                            }
                        } else if op == 1 {
                            BatchOp::Get {
                                db: std::sync::Arc::from("bench"),
                                table: std::sync::Arc::from("kv"),
                                key: key.into_bytes(),
                            }
                        } else {
                            BatchOp::Delete {
                                db: std::sync::Arc::from("bench"),
                                table: std::sync::Arc::from("kv"),
                                key: key.into_bytes(),
                            }
                        };
                        batch_buf.push(batch_op);

                        if batch_buf.len() >= BATCH_SIZE || i == ops_phase2 - 1 {
                            let s = Instant::now();
                            let results = mgr.submit_tasks(&batch_buf, tid as u32);
                            let d = s.elapsed().as_secs_f64();
                            let per_op_d = d / results.len() as f64;

                            for (j, result) in results.iter().enumerate() {
                                let op_type = match &batch_buf[j] {
                                    BatchOp::Put { .. } => 0u8,
                                    BatchOp::Get { .. } => 1u8,
                                    BatchOp::Delete { .. } => 2u8,
                                    // stress 不构造 Multi/RMW op
                                    _ => 3u8,
                                };
                                match result {
                                    BatchResult::PutOk
                                    | BatchResult::GetValue(_)
                                    | BatchResult::DeleteExisted(_)
                                    | BatchResult::Values(_)
                                    | BatchResult::Integer(_)
                                    | BatchResult::TxnApplied(_)
                                    | BatchResult::ReserveOk
                                    | BatchResult::ReserveConflict { .. }
                                    | BatchResult::Catalog(_)
                                    | BatchResult::ProjRows(_)
                                    | BatchResult::Double(_)
                                    | BatchResult::Pairs(_)
                                    | BatchResult::Members(_)
                                    | BatchResult::OptMember(_)
                                    | BatchResult::IntList(_)
                                    | BatchResult::Rows(_)
                                    | BatchResult::MultiPutOk => {
                                        match op_type {
                                            0 => { total_put.fetch_add(1, Ordering::Relaxed); }
                                            1 => { total_get.fetch_add(1, Ordering::Relaxed); }
                                            _ => { total_delete.fetch_add(1, Ordering::Relaxed); }
                                        }
                                        total_ops.fetch_add(1, Ordering::Relaxed);
                                        local.push((op_type, per_op_d));
                                    }
                                    BatchResult::Error(_) => {
                                        total_errors.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                            batch_buf.clear();
                        }
                    }

                    agg_lats.lock().unwrap().extend(local);
                })
                .expect("spawn")
        })
        .collect();

    let phase_start = Instant::now();
    for h in handles {
        h.join().expect("join");
    }
    let phase_elapsed = phase_start.elapsed();
    let tot = total_ops.load(Ordering::Relaxed);
    let p = total_put.load(Ordering::Relaxed);
    let g = total_get.load(Ordering::Relaxed);
    let d = total_delete.load(Ordering::Relaxed);
    let errs = total_errors.load(Ordering::Relaxed);
    let ops_per_sec = tot as f64 / phase_elapsed.as_secs_f64();
    println!(
        "[phase 2] done in {:.3}s, ops={tot}, ops/sec={:.0}, put/get/del = {p}/{g}/{d}, errors={errs}",
        phase_elapsed.as_secs_f64(),
        ops_per_sec
    );
    println!();

    // ---- 阶段 3: setup verify keys ----
    let verify_keys = Arc::new(Mutex::new(Vec::<Vec<u8>>::with_capacity(num_clients * 100)));
    {
        println!("[phase 3] setup verify keys: {num_clients} clients × 100 put each ...");
        let barrier3 = Arc::new(Barrier::new(num_clients));
        let handles: Vec<_> = (0..num_clients)
            .map(|tid| {
                let mgr = mgr.clone();
                let barrier = barrier3.clone();
                let verify_keys = verify_keys.clone();
                thread::Builder::new()
                    .name(format!("verify-client-{tid}"))
                    .stack_size(4 * 1024 * 1024)
                    .spawn(move || {
                        barrier.wait();
                        let mut local = Vec::with_capacity(100);
                        for i in 0..100 {
                            let key = format!("v{tid}_{i:06}");
                            let v = vec![((tid * 100 + i) & 0xFF) as u8; VALUE_LEN];
                            let s = Instant::now();
                            let r = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
                            let _d = s.elapsed().as_secs_f64();
                            if r.is_ok() {
                                local.push(key.into_bytes());
                            }
                        }
                        verify_keys.lock().unwrap().extend(local);
                    })
                    .expect("spawn")
            })
            .collect();
        for h in handles {
            h.join().expect("join");
        }
        // ⭐ Flush all shards: 把 phase 3 的 nowchunks dirty data 落盘到磁盘并插入 chunk_list.
        // 否则 phase 4 verify 依赖 nowchunks, bug 可能在 nowchunks 里.
        if let Err(e) = mgr.flush_all() {
            eprintln!("[phase 3] flush_all failed: {e}");
        }
        println!("[phase 3] setup done");
        println!();
    }

    // ---- 阶段 4: verify ----
    let verify_keys = verify_keys.lock().unwrap().clone();
    println!("[phase 4] verify: 重读 {} put 的 key...", verify_keys.len());
    let check_start = Instant::now();
    let mut verify_errors = 0u64;
    for key in &verify_keys {
        match mgr.get("bench", "kv", key, 0) {
            Ok(Some(_)) => {}
            Ok(None) => {
                verify_errors += 1;
                if verify_errors <= 3 {
                    let k = String::from_utf8_lossy(key);
                    eprintln!("  [verify] key {k} missing");
                }
            }
            Err(e) => {
                verify_errors += 1;
                if verify_errors <= 3 {
                    let k = String::from_utf8_lossy(key);
                    eprintln!("  [verify] error on {k}: {e}");
                }
            }
        }
    }
    println!(
        "[phase 4] done in {:.3}s, verify errors: {verify_errors}/{}",
        check_start.elapsed().as_secs_f64(),
        verify_keys.len()
    );
    println!();

    // ---- 阶段 5: report ----
    let mut read_lats = Vec::new();
    let mut write_lats = Vec::new();
    let mut delete_lats = Vec::new();
    for (op, d) in agg_lats_phase2.lock().unwrap().iter() {
        match op {
            0 => write_lats.push(*d),
            1 => read_lats.push(*d),
            _ => delete_lats.push(*d),
        }
    }
    let write_p50 = percentile(&mut write_lats, 50.0);
    let write_p99 = percentile(&mut write_lats, 99.0);
    let read_p50 = percentile(&mut read_lats, 50.0);
    let read_p99 = percentile(&mut read_lats, 99.0);
    let delete_p50 = percentile(&mut delete_lats, 50.0);
    let delete_p99 = percentile(&mut delete_lats, 99.0);

    println!("=== Benchmark Results ===");
    println!("shards:        {NUM_SHARDS}");
    println!("clients:       {num_clients}");
    println!("phase 2 total: {tot}");
    println!("phase 2 elapsed: {:.3} s", phase_elapsed.as_secs_f64());
    println!("phase 2 ops/sec: {:.0}", ops_per_sec);
    println!(
        "  put:         {} ({:.1}%)",
        p,
        100.0 * p as f64 / tot as f64
    );
    println!(
        "  get:         {} ({:.1}%)",
        g,
        100.0 * g as f64 / tot as f64
    );
    println!(
        "  delete:      {} ({:.1}%)",
        d,
        100.0 * d as f64 / tot as f64
    );
    println!(
        "read latency:   p50 = {:.3} ms, p99 = {:.3} ms",
        read_p50 * 1000.0,
        read_p99 * 1000.0
    );
    println!(
        "write latency:  p50 = {:.3} ms, p99 = {:.3} ms",
        write_p50 * 1000.0,
        write_p99 * 1000.0
    );
    println!(
        "delete latency: p50 = {:.3} ms, p99 = {:.3} ms",
        delete_p50 * 1000.0,
        delete_p99 * 1000.0
    );
    println!("phase 2 errors: {errs}");
    println!("verify errors:  {verify_errors} / {}", verify_keys.len());
    println!();

    if verify_errors == 0 && errs == 0 {
        println!("[PASS] correctness OK");
    } else {
        println!("[FAIL] correctness errors");
        std::process::exit(1);
    }

    println!();
    println!("[teardown] closing ShardManager...");
    Arc::try_unwrap(mgr).ok().expect("Arc unique").close().expect("close");
    println!("[done] data dir: {}", tmp.path().display());
}

fn percentile(samples: &mut [f64], pct: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((samples.len() as f64) * (pct / 100.0)) as usize;
    let idx = idx.min(samples.len() - 1);
    samples[idx]
}