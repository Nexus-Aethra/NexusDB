//! Network-layer stress test: 多 client 通过 TCP 走完整 NetworkServer + ShardManager.
//!
//! **拓扑**: 1 acceptor + N workers (epoll) + M shards (io_uring)
//!
//! **用法**:
//! ```bash
//! RUST_MIN_STACK=67108864 cargo run --release --example network_stress -- [ops] [conns] [shards] [workers]
//! # 默认: 5000 ops × 6 conns, 6 shards, 2 workers
//! ```

use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::Instant;

use network::{NetworkServer, NetworkServerConfig};
use network::protocol::{BinaryProtocol, DecodeOutcome, Protocol, Request, Response};
use shard_manager::{ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

const NUM_SHARDS: usize = 6;
const VALUE_LEN: usize = 32;
const DEFAULT_OPS_PHASE2: usize = 5000;

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn encode_request(req_id: u64, req: &Request) -> Vec<u8> {
    BinaryProtocol::new().encode_request(req_id, req)
}

fn decode_response(buf: &[u8]) -> Result<(u64, Response), Box<dyn std::error::Error>> {
    if buf.len() < 12 {
        return Err("frame too short".into());
    }
    let req_id = u64::from_be_bytes(buf[4..12].try_into().unwrap());
    match BinaryProtocol::new().decode_response(buf)? {
        DecodeOutcome::Complete { value, .. } => Ok((req_id, value)),
        DecodeOutcome::NeedMore => Err("incomplete".into()),
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let ops_phase2: usize = args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_OPS_PHASE2);
    let num_clients: usize = args
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(NUM_SHARDS);
    let num_shards: usize = args
        .get(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(NUM_SHARDS);
    let num_workers: usize = args
        .get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    // pipeline 深度: 每个连接同时 in-flight 的请求数 (1 = ping-pong)
    let pipeline: usize = args
        .get(5)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
        .max(1);
    let _ = env_or::<u64>("RUST_LOG", 0);

    println!("=== NexusDB 网络层 stress: 1 acceptor + {num_workers} workers + {num_shards} shards ===");
    println!("topology: acceptor=1, workers={num_workers}, shards={num_shards}");
    println!("clients (TCP conn): {num_clients}, pipeline depth: {pipeline}");
    println!("ops/phase-2: {ops_phase2}");
    println!("phase 2 total ops: {}", ops_phase2 * num_clients);
    println!();

    let tmp = tempfile::tempdir().expect("create tempdir");
    let opts = ShardManagerOptions {
        num_shards,
        block_root: tmp.path().to_path_buf(),
        create_if_missing: true,
        io_backend: IoBackend::IoUring,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 16,
        reply_bus_count: None,
        wal_mode: Default::default(),
    };

    println!("[setup] opening ShardManager with {num_shards} shards...");
    let setup_start = Instant::now();
    let mgr = Arc::new(ShardManager::open(opts).expect("open"));
    mgr.create_db("bench").expect("create db");
    mgr.create_table("bench", "kv").expect("create table");
    println!("[setup] ShardManager in {:.3}s", setup_start.elapsed().as_secs_f64());

    // 启动 NetworkServer (1 acceptor + num_workers workers)
    println!("[setup] starting NetworkServer with {num_workers} workers...");
    let net_start = Instant::now();
    let server = NetworkServer::start(NetworkServerConfig {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        shard_manager: mgr.clone(),
        worker_count: num_workers,
        default_db: "bench".to_string(),
        default_table: "kv".to_string(),
        inbox_capacity: 128,
        protocol: network::ProtocolKind::Binary,
        limits: network::KvLimits::default(),
        auth_password: None,
        worker_id_base: 0,
        sql_shared: network::new_sql_shared(),
    })
    .expect("NetworkServer::start");
    let addr = server.local_addr();
    println!("[setup] NetworkServer in {:.3}s listening on {addr}", net_start.elapsed().as_secs_f64());
    println!();

    // ========== Phase 1: warmup ==========
    {
        println!("[phase 1] warmup: {num_clients} clients × 200 put each ...");
        let barrier = Arc::new(std::sync::Barrier::new(num_clients));
        let total = Arc::new(AtomicU64::new(0));
        let s = Instant::now();
        let handles: Vec<_> = (0..num_clients)
            .map(|tid| {
                let barrier = barrier.clone();
                let total = total.clone();
                thread::Builder::new()
                    .name(format!("client-{tid}"))
                    .stack_size(4 * 1024 * 1024)
                    .spawn(move || {
                        barrier.wait();
                        let mut stream = TcpStream::connect(addr).expect("connect");
                        stream.set_nodelay(true).ok();
                        let mut buf = [0u8; 4096];
                        for i in 0..200u64 {
                            let key = format!("warmup_t{tid}_{i:06}");
                            let v = vec![(i & 0xFF) as u8; VALUE_LEN];
                            let req = encode_request(0, &Request::Put {
                                key: key.into_bytes(),
                                value: v,
                            });
                            stream.write_all(&req).ok();
                            stream.flush().ok();
                            let n = stream.read(&mut buf).ok().unwrap_or(0);
                            if n > 0
                                && let Ok((_, Response::PutOk)) = decode_response(&buf[..n])
                            {
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

    // ========== Phase 2: mixed workload ==========
    println!(
        "[phase 2] mixed: {num_clients} clients × {ops_phase2} ops, 50/30/20 (put/get/delete) ..."
    );
    let barrier = Arc::new(std::sync::Barrier::new(num_clients));
    let total_put = Arc::new(AtomicU64::new(0));
    let total_get = Arc::new(AtomicU64::new(0));
    let total_delete = Arc::new(AtomicU64::new(0));
    let total_errors = Arc::new(AtomicU64::new(0));
    let phase_start = Arc::new(Mutex::new(Instant::now()));
    let agg_lats = Arc::new(Mutex::new(Vec::<(u8, f64)>::with_capacity(ops_phase2 * num_clients)));
    let handles: Vec<_> = (0..num_clients)
        .map(|tid| {
            let barrier = barrier.clone();
            let total_put = total_put.clone();
            let total_get = total_get.clone();
            let total_delete = total_delete.clone();
            let total_errors = total_errors.clone();
            let phase_start = phase_start.clone();
            let agg_lats = agg_lats.clone();
            thread::Builder::new()
                .name(format!("client-{tid}"))
                .stack_size(4 * 1024 * 1024)
                .spawn(move || {
                    let mut stream = TcpStream::connect(addr).expect("connect");
                    stream.set_nodelay(true).ok();
                    barrier.wait();
                    *phase_start.lock().unwrap() = Instant::now();

                    let mut rng_state: u64 = 0x12345678_9ABCDEF0u64
                        .wrapping_add((tid as u64).wrapping_mul(0x9E3779B97F4A7C15u64));
                    let mut next = || {
                        rng_state = rng_state
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        rng_state
                    };

                    let mut tmp = [0u8; 16384];
                    let mut recv_buf: Vec<u8> = Vec::with_capacity(64 * 1024);
                    let mut local_lats = Vec::with_capacity(ops_phase2);
                    let mut sent_total = 0usize;

                    // pipeline 模式: 一次发 pipeline 个请求, 再收 pipeline 个回复
                    while sent_total < ops_phase2 {
                        let batch_n = pipeline.min(ops_phase2 - sent_total);
                        let mut op_types = Vec::with_capacity(batch_n);
                        let mut out = Vec::with_capacity(batch_n * 64);
                        for _ in 0..batch_n {
                            let i = sent_total;
                            let r = (next() >> 32) % 100;
                            let op = if r < 60 { 0 } else if r < 85 { 1 } else { 2 };
                            let key = format!("t{tid}_{i:08}");
                            let kb = key.as_bytes();
                            let req = match op {
                                0 => {
                                    let v = vec![((next() >> 32) & 0xFF) as u8; VALUE_LEN];
                                    encode_request(i as u64, &Request::Put { key: kb.to_vec(), value: v })
                                }
                                1 => encode_request(i as u64, &Request::Get { key: kb.to_vec() }),
                                _ => encode_request(i as u64, &Request::Delete { key: kb.to_vec() }),
                            };
                            out.extend_from_slice(&req);
                            op_types.push(op as u8);
                            sent_total += 1;
                        }

                        let batch_start = Instant::now();
                        if stream.write_all(&out).is_err() {
                            total_errors.fetch_add(batch_n as u64, Ordering::Relaxed);
                            continue;
                        }

                        // 收 batch_n 个完整回复帧 (帧重组)
                        let mut got = 0usize;
                        let mut failed = false;
                        while got < batch_n {
                            // 先尝试从 recv_buf 解帧
                            let mut progressed = true;
                            while got < batch_n && progressed {
                                progressed = false;
                                if recv_buf.len() >= 4 {
                                    let total_len = u32::from_be_bytes(recv_buf[0..4].try_into().unwrap()) as usize;
                                    if total_len >= 4 && recv_buf.len() >= total_len {
                                        match decode_response(&recv_buf[..total_len]) {
                                            Ok((_rid, resp)) => {
                                                match resp {
                                                    Response::PutOk => { total_put.fetch_add(1, Ordering::Relaxed); }
                                                    Response::Get(_) => { total_get.fetch_add(1, Ordering::Relaxed); }
                                                    Response::DeleteOk => { total_delete.fetch_add(1, Ordering::Relaxed); }
                                                    Response::Error(_) => { total_errors.fetch_add(1, Ordering::Relaxed); }
                                                }
                                                got += 1;
                                                progressed = true;
                                            }
                                            Err(_) => {
                                                total_errors.fetch_add(1, Ordering::Relaxed);
                                                got += 1;
                                                progressed = true;
                                            }
                                        }
                                        recv_buf.drain(..total_len);
                                    }
                                }
                            }
                            if got >= batch_n { break; }
                            // 需要更多字节
                            match stream.read(&mut tmp) {
                                Ok(0) => { failed = true; break; }
                                Ok(n) => recv_buf.extend_from_slice(&tmp[..n]),
                                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                                Err(_) => { failed = true; break; }
                            }
                        }
                        if failed {
                            total_errors.fetch_add((batch_n - got) as u64, Ordering::Relaxed);
                        }
                        // 均摊延迟
                        let per_op = batch_start.elapsed().as_secs_f64() / batch_n as f64;
                        for op in &op_types {
                            local_lats.push((*op, per_op));
                        }
                    }
                    agg_lats.lock().unwrap().extend(local_lats);
                })
                .expect("spawn")
        })
        .collect();
    for h in handles {
        h.join().expect("join");
    }
    let phase_elapsed = phase_start.lock().unwrap().elapsed();
    let p = total_put.load(Ordering::Relaxed);
    let g = total_get.load(Ordering::Relaxed);
    let d = total_delete.load(Ordering::Relaxed);
    let errs = total_errors.load(Ordering::Relaxed);
    let tot = p + g + d;
    let ops_per_sec = tot as f64 / phase_elapsed.as_secs_f64();
    println!(
        "[phase 2] done in {:.3}s, ops={tot}, ops/sec={:.0}, put/get/del = {p}/{g}/{d}, errors={errs}",
        phase_elapsed.as_secs_f64(),
        ops_per_sec
    );
    println!();

    // ========== Phase 3: setup verify keys ==========
    let verify_keys = Arc::new(Mutex::new(Vec::<Vec<u8>>::with_capacity(num_clients * 100)));
    {
        println!("[phase 3] setup: {num_clients} clients × 100 put each ...");
        let barrier3 = Arc::new(std::sync::Barrier::new(num_clients));
        let handles: Vec<_> = (0..num_clients)
            .map(|tid| {
                let barrier = barrier3.clone();
                let verify_keys = verify_keys.clone();
                thread::Builder::new()
                    .name(format!("verify-client-{tid}"))
                    .stack_size(4 * 1024 * 1024)
                    .spawn(move || {
                        let mut stream = TcpStream::connect(addr).expect("connect");
                        stream.set_nodelay(true).ok();
                        barrier.wait();
                        let mut buf = [0u8; 4096];
                        let mut local = Vec::with_capacity(100);
                        for i in 0..100 {
                            let key = format!("v{tid}_{i:06}");
                            let v = vec![((tid * 100 + i) & 0xFF) as u8; VALUE_LEN];
                            let req = encode_request(0, &Request::Put {
                                key: key.clone().into_bytes(),
                                value: v,
                            });
                            stream.write_all(&req).ok();
                            stream.flush().ok();
                            let n = stream.read(&mut buf).unwrap_or(0);
                            if n > 0
                                && let Ok((_, Response::PutOk)) = decode_response(&buf[..n])
                            {
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
        println!("[phase 3] setup done");
        println!();
    }

    // ========== Phase 4: verify ==========
    let verify_keys = verify_keys.lock().unwrap().clone();
    println!("[phase 4] verify: 重读 {} put 的 key...", verify_keys.len());
    let barrier_v = Arc::new(std::sync::Barrier::new(num_clients));
    let verify_errors = Arc::new(AtomicU64::new(0));
    let check_start = Instant::now();
    let handles: Vec<_> = (0..num_clients)
        .map(|tid| {
            let verify_errors = verify_errors.clone();
            let barrier_v = barrier_v.clone();
            let keys = verify_keys.clone();
            thread::Builder::new()
                .name(format!("verify-client-{tid}"))
                .stack_size(4 * 1024 * 1024)
                .spawn(move || {
                    let mut stream = TcpStream::connect(addr).expect("connect");
                    stream.set_nodelay(true).ok();
                    barrier_v.wait();
                    let mut buf = [0u8; 4096];
                    for (i, key) in keys.iter().enumerate() {
                        if i % num_clients != tid {
                            continue;
                        }
                        let req = encode_request(0, &Request::Get { key: key.clone() });
                        stream.write_all(&req).ok();
                        stream.flush().ok();
                        let n = stream.read(&mut buf).unwrap_or(0);
                        if n == 0 {
                            verify_errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        match decode_response(&buf[..n]) {
                            Ok((_, Response::Get(Some(_)))) => {}
                            Ok((_, Response::Get(None))) => {
                                verify_errors.fetch_add(1, Ordering::Relaxed);
                                if verify_errors.load(Ordering::Relaxed) <= 3 {
                                    let k = String::from_utf8_lossy(key);
                                    eprintln!("[verify] conn-tid {tid} key {k} missing");
                                }
                            }
                            Ok(_) => {}
                            Err(e) => {
                                verify_errors.fetch_add(1, Ordering::Relaxed);
                                if verify_errors.load(Ordering::Relaxed) <= 3 {
                                    eprintln!("[verify] conn-tid {tid} err on {:?}: {e}", String::from_utf8_lossy(key));
                                }
                            }
                        }
                    }
                })
                .expect("spawn")
        })
        .collect();
    for h in handles {
        h.join().expect("join");
    }
    let verify_errors = verify_errors.load(Ordering::Relaxed);
    println!(
        "[phase 4] done in {:.3}s, verify errors: {verify_errors}/{}",
        check_start.elapsed().as_secs_f64(),
        verify_keys.len()
    );
    println!();

    println!("=== Benchmark ===");
    println!("topology: acceptor=1, workers={num_workers}, shards={num_shards}, conns={num_clients}");
    println!("phase 2 ops/sec: {:.0}", ops_per_sec);
    println!("phase 2 errors: {errs}");

    // 延迟百分位统计
    {
        let lats = agg_lats.lock().unwrap();
        let mut put_lats: Vec<f64> = lats.iter().filter(|(op, _)| *op == 0).map(|(_, d)| *d).collect();
        let mut get_lats: Vec<f64> = lats.iter().filter(|(op, _)| *op == 1).map(|(_, d)| *d).collect();
        let mut del_lats: Vec<f64> = lats.iter().filter(|(op, _)| *op == 2).map(|(_, d)| *d).collect();
        let pct = |v: &mut Vec<f64>, p: f64| -> f64 {
            if v.is_empty() { return 0.0; }
            v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let idx = ((v.len() as f64) * (p / 100.0)) as usize;
            v[idx.min(v.len() - 1)]
        };
        println!("put latency:    p50={:.3}ms p99={:.3}ms p999={:.3}ms",
            pct(&mut put_lats, 50.0) * 1000.0, pct(&mut put_lats, 99.0) * 1000.0, pct(&mut put_lats, 99.9) * 1000.0);
        println!("get latency:    p50={:.3}ms p99={:.3}ms p999={:.3}ms",
            pct(&mut get_lats, 50.0) * 1000.0, pct(&mut get_lats, 99.0) * 1000.0, pct(&mut get_lats, 99.9) * 1000.0);
        println!("delete latency: p50={:.3}ms p99={:.3}ms p999={:.3}ms",
            pct(&mut del_lats, 50.0) * 1000.0, pct(&mut del_lats, 99.0) * 1000.0, pct(&mut del_lats, 99.9) * 1000.0);
    }

    println!("verify errors: {verify_errors} / {}", verify_keys.len());
    if verify_errors == 0 && errs == 0 {
        println!("[PASS] correctness OK");
    } else {
        println!("[FAIL] errors present");
    }

    server.shutdown().expect("shutdown server");
    if let Ok(mgr) = Arc::try_unwrap(mgr) {
        mgr.close().expect("close mgr");
    }
    println!();
    println!("[teardown] data dir: {}", tmp.path().display());
}
