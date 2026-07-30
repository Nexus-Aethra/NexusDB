//! Minimal repro: 6 shard × 6 client 模拟 stress phase 1+2+3+4.
//!
//! 现象: phase 3 put 后, phase 4 get 拿到的 leaf bytes 不是最新的,
//!       kc 比 phase 3 insert 后的 kc 少很多.
//!
//! 假设根因: storage layer 的 nowchunks.peek_chunk 在并发读写交错时
//!          返回 stale bytes (旧值).

use shard_manager::{ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

#[test]
fn minimal_repro_storage_reads_stale_leaf_bytes() {
    use std::sync::{Arc, Barrier};
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions {
        num_shards: 6,
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

    // Phase 1: 6 client × 200 puts each
    {
        let barrier = Arc::new(Barrier::new(6));
        let mut handles = vec![];
        for tid in 0..6 {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for i in 0..200 {
                    let key = format!("warmup_t{tid}_{i:06}");
                    let v = vec![((tid * 200 + i) & 0xFF) as u8; 32];
                    let _ = mgr.put("bench", "kv", key.as_bytes(), &v, 0);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    // Phase 2: 6 client × 2000 mixed ops (mixed put/get/delete)
    {
        let barrier = Arc::new(Barrier::new(6));
        let mut handles = vec![];
        for tid in 0..6 {
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
                for i in 0..2000 {
                    let r = (next() >> 32) % 100;
                    let key = format!("t{tid}_{i:08}");
                    let kb = key.as_bytes();
                    if r < 50 {
                        let v = vec![((next() >> 32) & 0xFF) as u8; 32];
                        let _ = mgr.put("bench", "kv", kb, &v, 0);
                    } else if r < 80 {
                        let _ = mgr.get("bench", "kv", kb, 0);
                    } else {
                        let _ = mgr.delete("bench", "kv", kb, 0);
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    // Phase 3: 6 client × 100 verify puts
    let verify_keys = Arc::new(std::sync::Mutex::new(Vec::<Vec<u8>>::with_capacity(600)));
    {
        let barrier = Arc::new(Barrier::new(6));
        let mut handles = vec![];
        for tid in 0..6 {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            let verify_keys = verify_keys.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let mut local = Vec::with_capacity(100);
                for i in 0..100 {
                    let key = format!("v{tid}_{i:06}");
                    let v = vec![((tid * 100 + i) & 0xFF) as u8; 32];
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

    // Phase 4: verify
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
    eprintln!("verify keys total: {}, missing: {}", keys.len(), missing);
    assert_eq!(missing, 0, "should have 0 missing, got {missing}");
}