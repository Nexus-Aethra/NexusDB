//! 复现 stress.rs 的 verify error bug.
//!
//! 关键发现: phase 1 (warmup 200 puts per client × 6 clients) + phase 2 (mixed)
//! 才会触发 missing key. 单独 phase 2 不行.

use shard_manager::{ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

const NUM_SHARDS: usize = 6;
const NUM_CLIENTS: usize = 6;
const PHASE2_OPS_PER_CLIENT: usize = 10_000;
const VALUE_LEN: usize = 32;

fn make_mgr() -> (tempfile::TempDir, std::sync::Arc<shard_manager::ShardManager>) {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions {
        num_shards: NUM_SHARDS,
        block_root: tmp.path().to_path_buf(),
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 16,
        reply_bus_count: None,
        wal_mode: Default::default(),
    };
    let mgr = ShardManager::open(opts).expect("open");
    mgr.create_db("bench").unwrap();
    mgr.create_table("bench", "kv").unwrap();
    (tmp, std::sync::Arc::new(mgr))
}

#[test]
fn just_phase1_then_phase3() {
    // 没有 phase 2, 只有 phase 1 warmup + phase 3
    let (_tmp, mgr) = make_mgr();
    // Phase 1
    {
        use std::sync::{Arc, Barrier};
        let barrier = Arc::new(Barrier::new(NUM_CLIENTS));
        let mut handles = vec![];
        for tid in 0..NUM_CLIENTS {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for i in 0..200 {
                    let key = format!("warmup_t{tid}_{i:06}");
                    let v = vec![(i & 0xFF) as u8; VALUE_LEN];
                    let _ = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
    // Phase 3 verify keys
    let verify_keys = std::sync::Arc::new(std::sync::Mutex::new(
        Vec::<Vec<u8>>::with_capacity(NUM_CLIENTS * 100),
    ));
    {
        use std::sync::{Arc, Barrier};
        let barrier = Arc::new(Barrier::new(NUM_CLIENTS));
        let mut handles = vec![];
        for tid in 0..NUM_CLIENTS {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            let verify_keys = verify_keys.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let mut local = Vec::with_capacity(100);
                for i in 0..100 {
                    let key = format!("v{tid}_{i:06}");
                    let v = vec![((tid * 100 + i) & 0xFF) as u8; VALUE_LEN];
                    let r = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
                    if r.is_ok() {
                        local.push(key.into_bytes());
                    }
                }
                verify_keys.lock().unwrap().extend(local);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
    let keys = verify_keys.lock().unwrap().clone();
    let mut missing = 0u64;
    for key in &keys {
        if matches!(mgr.get("bench", "kv", key, 0), Ok(None)) {
            missing += 1;
            if missing <= 3 {
                let k = String::from_utf8_lossy(key);
                eprintln!("missing: {k}");
            }
        }
    }
    assert_eq!(missing, 0, "phase1+phase3 only, missing {missing}");
}

#[test]
fn just_phase1_then_phase3_sequential() {
    // 单线程做 phase 1, 排除多 client 干扰
    let (_tmp, mgr) = make_mgr();
    for tid in 0..NUM_CLIENTS as u64 {
        for i in 0..200u64 {
            let key = format!("warmup_t{tid}_{i:06}");
            let v = vec![(i & 0xFF) as u8; VALUE_LEN];
            let _ = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
        }
    }
    let mut local = Vec::new();
    for tid in 0..NUM_CLIENTS as u64 {
        for i in 0..100u64 {
            let key = format!("v{tid}_{i:06}");
            let v = vec![((tid * 100 + i) & 0xFF) as u8; VALUE_LEN];
            let r = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
            if r.is_ok() {
                local.push(key.into_bytes());
            }
        }
    }
    let mut missing = 0u64;
    for key in &local {
        if matches!(mgr.get("bench", "kv", key, 0), Ok(None)) {
            missing += 1;
            if missing <= 3 {
                let k = String::from_utf8_lossy(key);
                eprintln!("missing: {k}");
            }
        }
    }
    assert_eq!(missing, 0, "sequential should never miss");
}

#[test]
fn phase1_then_many_writes_then_phase3() {
    // phase 1 warmup + 大量随机 writes 到不同 key + phase 3
    let (_tmp, mgr) = make_mgr();
    // Phase 1 warmup
    for tid in 0..NUM_CLIENTS as u64 {
        for i in 0..200u64 {
            let key = format!("warmup_t{tid}_{i:06}");
            let v = vec![(i & 0xFF) as u8; VALUE_LEN];
            let _ = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
        }
    }
    // 大量写: 模仿 phase 2 写多个 key, 顺序
    for i in 0..10_000 {
        let key = format!("warm_i{i:08}");
        let v = vec![(i & 0xFF) as u8; VALUE_LEN];
        let _ = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
    }
    // Phase 3 verify keys
    let mut local = Vec::new();
    for tid in 0..NUM_CLIENTS as u64 {
        for i in 0..100u64 {
            let key = format!("v{tid}_{i:06}");
            let v = vec![((tid * 100 + i) & 0xFF) as u8; VALUE_LEN];
            let r = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
            if r.is_ok() {
                local.push(key.into_bytes());
            }
        }
    }
    let mut missing = 0u64;
    for key in &local {
        if matches!(mgr.get("bench", "kv", key, 0), Ok(None)) {
            missing += 1;
            if missing <= 3 {
                let k = String::from_utf8_lossy(key);
                eprintln!("missing: {k}");
            }
        }
    }
    assert_eq!(missing, 0, "phase1 + 10k writes + phase3");
}

#[test]
fn phase1_then_phase2_then_phase3() {
    let (_tmp, mgr) = make_mgr();
    // Phase 1 warmup
    {
        use std::sync::{Arc, Barrier};
        let barrier = Arc::new(Barrier::new(NUM_CLIENTS));
        let mut handles = vec![];
        for tid in 0..NUM_CLIENTS {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for i in 0..200 {
                    let key = format!("warmup_t{tid}_{i:06}");
                    let v = vec![(i & 0xFF) as u8; VALUE_LEN];
                    let _ = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
    // Phase 2 mixed
    {
        use std::sync::{Arc, Barrier};
        let barrier = Arc::new(Barrier::new(NUM_CLIENTS));
        let mut handles = vec![];
        for tid in 0..NUM_CLIENTS {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let mut rng: u64 = 0x12345678_9ABCDEF0u64
                    .wrapping_add((tid as u64).wrapping_mul(0x9E3779B97F4A7C15u64));
                let mut next = || {
                    rng = rng
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    rng
                };
                for i in 0..PHASE2_OPS_PER_CLIENT {
                    let r = (next() >> 32) % 100;
                    let key = format!("t{tid}_{i:08}");
                    let kb = key.as_bytes();
                    let _ = if r < 50 {
                        let v = vec![((next() >> 32) & 0xFF) as u8; VALUE_LEN];
                        mgr.put("bench", "kv", kb, &v, 0)
                    } else if r < 80 {
                        let _ = mgr.get("bench", "kv", kb, 0);
                        Ok(())
                    } else {
                        let _ = mgr.delete("bench", "kv", kb, 0);
                        Ok(())
                    };
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
    // Phase 3 verify
    let verify_keys = std::sync::Arc::new(std::sync::Mutex::new(
        Vec::<Vec<u8>>::with_capacity(NUM_CLIENTS * 100),
    ));
    {
        use std::sync::{Arc, Barrier};
        let barrier = Arc::new(Barrier::new(NUM_CLIENTS));
        let mut handles = vec![];
        for tid in 0..NUM_CLIENTS {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            let verify_keys = verify_keys.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let mut local = Vec::with_capacity(100);
                for i in 0..100 {
                    let key = format!("v{tid}_{i:06}");
                    let v = vec![((tid * 100 + i) & 0xFF) as u8; VALUE_LEN];
                    let r = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
                    if r.is_ok() {
                        local.push(key.into_bytes());
                    }
                }
                verify_keys.lock().unwrap().extend(local);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
    let keys = verify_keys.lock().unwrap().clone();
    let mut missing = 0u64;
    for key in &keys {
        if matches!(mgr.get("bench", "kv", key, 0), Ok(None)) {
            missing += 1;
            if missing <= 3 {
                let k = String::from_utf8_lossy(key);
                eprintln!("missing: {k}");
            }
        }
    }
    assert_eq!(missing, 0, "phase1+phase2+phase3, got {missing}");
}

#[test]
fn phase1_sequential_then_phase2_then_phase3() {
    let (_tmp, mgr) = make_mgr();
    // Phase 1 顺序
    for tid in 0..NUM_CLIENTS as u64 {
        for i in 0..200u64 {
            let key = format!("warmup_t{tid}_{i:06}");
            let v = vec![(i & 0xFF) as u8; VALUE_LEN];
            let _ = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
        }
    }
    // Phase 2 顺序
    for tid in 0..NUM_CLIENTS as u64 {
        let mut rng: u64 = 0x12345678_9ABCDEF0u64
            .wrapping_add(tid.wrapping_mul(0x9E3779B97F4A7C15u64));
        let mut next = || {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            rng
        };
        for i in 0..PHASE2_OPS_PER_CLIENT {
            let r = (next() >> 32) % 100;
            let key = format!("t{tid}_{i:08}");
            let kb = key.as_bytes();
            let _ = if r < 50 {
                let v = vec![((next() >> 32) & 0xFF) as u8; VALUE_LEN];
                mgr.put("bench", "kv", kb, &v, 0)
            } else if r < 80 {
                let _ = mgr.get("bench", "kv", kb, 0);
                Ok(())
            } else {
                let _ = mgr.delete("bench", "kv", kb, 0);
                Ok(())
            };
        }
    }
    // Phase 3 verify
    let mut local = Vec::new();
    for tid in 0..NUM_CLIENTS as u64 {
        for i in 0..100u64 {
            let key = format!("v{tid}_{i:06}");
            let v = vec![((tid * 100 + i) & 0xFF) as u8; VALUE_LEN];
            let r = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
            if r.is_ok() {
                local.push(key.into_bytes());
            }
        }
    }
    let mut missing = 0u64;
    for key in &local {
        if matches!(mgr.get("bench", "kv", key, 0), Ok(None)) {
            missing += 1;
            if missing <= 3 {
                let k = String::from_utf8_lossy(key);
                eprintln!("missing: {k}");
            }
        }
    }
    assert_eq!(missing, 0, "sequential phase1+2+3");
}

#[test]
fn phase1_then_concurrent_puts_only_then_phase3() {
    // 并发但只 put, 不 get/delete
    let (_tmp, mgr) = make_mgr();
    // Phase 1
    {
        use std::sync::{Arc, Barrier};
        let barrier = Arc::new(Barrier::new(NUM_CLIENTS));
        let mut handles = vec![];
        for tid in 0..NUM_CLIENTS {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for i in 0..200 {
                    let key = format!("warmup_t{tid}_{i:06}");
                    let v = vec![(i & 0xFF) as u8; VALUE_LEN];
                    let _ = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
    // Phase 2': only puts, concurrent
    {
        use std::sync::{Arc, Barrier};
        let barrier = Arc::new(Barrier::new(NUM_CLIENTS));
        let mut handles = vec![];
        for tid in 0..NUM_CLIENTS {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for i in 0..PHASE2_OPS_PER_CLIENT {
                    let key = format!("t{tid}_{i:08}");
                    let v = vec![((tid + i) & 0xFF) as u8; VALUE_LEN];
                    let _ = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
    // Phase 3
    let verify_keys = std::sync::Arc::new(std::sync::Mutex::new(
        Vec::<Vec<u8>>::with_capacity(NUM_CLIENTS * 100),
    ));
    {
        use std::sync::{Arc, Barrier};
        let barrier = Arc::new(Barrier::new(NUM_CLIENTS));
        let mut handles = vec![];
        for tid in 0..NUM_CLIENTS {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            let verify_keys = verify_keys.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let mut local = Vec::with_capacity(100);
                for i in 0..100 {
                    let key = format!("v{tid}_{i:06}");
                    let v = vec![((tid * 100 + i) & 0xFF) as u8; VALUE_LEN];
                    let r = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
                    if r.is_ok() {
                        local.push(key.into_bytes());
                    }
                }
                verify_keys.lock().unwrap().extend(local);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
    let keys = verify_keys.lock().unwrap().clone();
    let mut missing = 0u64;
    for key in &keys {
        if matches!(mgr.get("bench", "kv", key, 0), Ok(None)) {
            missing += 1;
            if missing <= 3 {
                let k = String::from_utf8_lossy(key);
                eprintln!("missing: {k}");
            }
        }
    }
    assert_eq!(missing, 0, "concurrent puts only");
}

#[test]
fn minimal_two_clients_two_shards() {
    // 最小化: 2 shard, 2 client, 并发 put+get, 然后 phase 3
    use std::sync::{Arc, Barrier};
    let tmp = tempfile::tempdir().unwrap();
    let opts = shard_manager::ShardManagerOptions {
        num_shards: 2,
        block_root: tmp.path().to_path_buf(),
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 16,
        reply_bus_count: None,
        wal_mode: Default::default(),
    };
    let mgr = Arc::new(ShardManager::open(opts).expect("open"));
    mgr.create_db("bench").unwrap();
    mgr.create_table("bench", "kv").unwrap();

    // phase 1
    {
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = vec![];
        for tid in 0..2u64 {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for i in 0..200 {
                    let key = format!("warmup_t{tid}_{i:06}");
                    let v = vec![(i & 0xFF) as u8; VALUE_LEN];
                    let _ = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    let barrier = Arc::new(Barrier::new(2));
    let mut handles = vec![];
    for tid in 0..2u64 {
        let mgr = mgr.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let mut rng: u64 = 0x12345678_9ABCDEF0u64
                .wrapping_add(tid * 0x9E3779B97F4A7C15u64);
            let mut next = || {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                rng
            };
            for i in 0..5_000u64 {
                let r = (next() >> 32) % 100;
                let key = format!("t{tid}_{i:06}");
                let kb = key.as_bytes();
                if r < 50 {
                    let v = vec![((next() >> 32) & 0xFF) as u8; VALUE_LEN];
                    let _ = mgr.put("bench", "kv", kb, &v, 0);
                } else {
                    let _ = mgr.get("bench", "kv", kb, 0);
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Phase 3 verify
    let mut local = Vec::new();
    for tid in 0..2u64 {
        for i in 0..50u64 {
            let key = format!("v{tid}_{i:06}");
            let v = vec![((tid * 100 + i) & 0xFF) as u8; VALUE_LEN];
            let r = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
            if r.is_ok() {
                local.push(key.into_bytes());
            }
        }
    }
    let mut missing = 0u64;
    for key in &local {
        if matches!(mgr.get("bench", "kv", key, 0), Ok(None)) {
            missing += 1;
            if missing <= 3 {
                let k = String::from_utf8_lossy(key);
                eprintln!("missing: {k}");
            }
        }
    }
    assert_eq!(missing, 0, "2 shard 2 client minimal");
}

#[test]
fn single_threaded_phase2_then_phase3() {
    // 1 个 client 做 60K mixed, 然后 phase 3
    let (_tmp, mgr) = make_mgr();
    // Phase 1
    for tid in 0..NUM_CLIENTS as u64 {
        for i in 0..200u64 {
            let key = format!("warmup_t{tid}_{i:06}");
            let v = vec![(i & 0xFF) as u8; VALUE_LEN];
            let _ = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
        }
    }
    // Phase 2: 60K mixed single thread
    let mut rng: u64 = 0x12345678_9ABCDEF0u64;
    let mut next = || {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        rng
    };
    for i in 0..60_000u64 {
        let r = (next() >> 32) % 100;
        let tid = (next() as usize) % NUM_CLIENTS;
        let key = format!("t{tid}_{i:08}");
        let kb = key.as_bytes();
        if r < 50 {
            let v = vec![((next() >> 32) & 0xFF) as u8; VALUE_LEN];
            let _ = mgr.put("bench", "kv", kb, &v, 0);
        } else if r < 80 {
            let _ = mgr.get("bench", "kv", kb, 0);
        } else {
            let _ = mgr.delete("bench", "kv", kb, 0);
        }
    }
    // Phase 3 verify
    let mut local = Vec::new();
    for tid in 0..NUM_CLIENTS as u64 {
        for i in 0..100u64 {
            let key = format!("v{tid}_{i:06}");
            let v = vec![((tid * 100 + i) & 0xFF) as u8; VALUE_LEN];
            let r = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
            if r.is_ok() {
                local.push(key.into_bytes());
            }
        }
    }
    let mut missing = 0u64;
    for key in &local {
        if matches!(mgr.get("bench", "kv", key, 0), Ok(None)) {
            missing += 1;
            if missing <= 3 {
                let k = String::from_utf8_lossy(key);
                eprintln!("missing: {k}");
            }
        }
    }
    assert_eq!(missing, 0, "single-threaded phase2+phase3");
}

#[test]
fn phase1_then_concurrent_puts_then_gets_then_phase3() {
    // put + get (no delete)
    let (_tmp, mgr) = make_mgr();
    // Phase 1
    {
        use std::sync::{Arc, Barrier};
        let barrier = Arc::new(Barrier::new(NUM_CLIENTS));
        let mut handles = vec![];
        for tid in 0..NUM_CLIENTS {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for i in 0..200 {
                    let key = format!("warmup_t{tid}_{i:06}");
                    let v = vec![(i & 0xFF) as u8; VALUE_LEN];
                    let _ = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
    // Phase 2': puts + gets, 50/50
    {
        use std::sync::{Arc, Barrier};
        let barrier = Arc::new(Barrier::new(NUM_CLIENTS));
        let mut handles = vec![];
        for tid in 0..NUM_CLIENTS {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for i in 0..PHASE2_OPS_PER_CLIENT {
                    let key = format!("t{tid}_{i:08}");
                    let kb = key.as_bytes();
                    if i % 2 == 0 {
                        let v = vec![((tid + i) & 0xFF) as u8; VALUE_LEN];
                        let _ = mgr.put("bench", "kv", kb, &v, 0);
                    } else {
                        let _ = mgr.get("bench", "kv", kb, 0);
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
    // Phase 3
    let verify_keys = std::sync::Arc::new(std::sync::Mutex::new(
        Vec::<Vec<u8>>::with_capacity(NUM_CLIENTS * 100),
    ));
    {
        use std::sync::{Arc, Barrier};
        let barrier = Arc::new(Barrier::new(NUM_CLIENTS));
        let mut handles = vec![];
        for tid in 0..NUM_CLIENTS {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            let verify_keys = verify_keys.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let mut local = Vec::with_capacity(100);
                for i in 0..100 {
                    let key = format!("v{tid}_{i:06}");
                    let v = vec![((tid * 100 + i) & 0xFF) as u8; VALUE_LEN];
                    let r = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
                    if r.is_ok() {
                        local.push(key.into_bytes());
                    }
                }
                verify_keys.lock().unwrap().extend(local);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
    let keys = verify_keys.lock().unwrap().clone();
    let mut missing = 0u64;
    for key in &keys {
        if matches!(mgr.get("bench", "kv", key, 0), Ok(None)) {
            missing += 1;
            if missing <= 3 {
                let k = String::from_utf8_lossy(key);
                eprintln!("missing: {k}");
            }
        }
    }
    assert_eq!(missing, 0, "concurrent puts+gets");
}