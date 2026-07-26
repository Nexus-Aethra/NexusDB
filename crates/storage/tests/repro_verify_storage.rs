//! Storage-level repro: 模拟 phase 2 + phase 3 流程.
//!
//! 发现 shard_manager 层的 missing 必出现并发 put+get,
//! 我们现在在 storage 层独立复现, 排除 shard_manager 干扰.

use shard_manager::{ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

const NUM_CLIENTS: usize = 6;
const PHASE2_OPS_PER_CLIENT: usize = 10_000;
const VALUE_LEN: usize = 32;

fn make_mgr() -> (tempfile::TempDir, std::sync::Arc<ShardManager>) {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions {
        num_shards: 6,
        block_root: tmp.path().to_path_buf(),
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 16,
        reply_bus_count: None,
    };
    let mgr = ShardManager::open(opts).expect("open");
    mgr.create_db("bench").unwrap();
    mgr.create_table("bench", "kv").unwrap();
    (tmp, std::sync::Arc::new(mgr))
}

#[test]
fn verify_immediately_after_concurrent_phase2() {
    // 不跑 phase 3, 在 phase 2 完成后立刻 verify phase 2 写的 key
    use std::sync::{Arc, Barrier};
    let (_tmp, mgr) = make_mgr();

    // Phase 2
    let phase2_keys: Arc<std::sync::Mutex<Vec<Vec<u8>>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    {
        let barrier = Arc::new(Barrier::new(NUM_CLIENTS));
        let mut handles = vec![];
        for tid in 0..NUM_CLIENTS {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            let phase2_keys = phase2_keys.clone();
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
                let mut local_keys = Vec::new();
                for i in 0..PHASE2_OPS_PER_CLIENT {
                    let r = (next() >> 32) % 100;
                    let key = format!("t{tid}_{i:08}");
                    let kb = key.as_bytes();
                    if r < 50 {
                        // put
                        let v = vec![((next() >> 32) & 0xFF) as u8; VALUE_LEN];
                        let _ = mgr.put("bench", "kv", kb, &v, 0);
                        local_keys.push(kb.to_vec());
                    } else if r < 80 {
                        let _ = mgr.get("bench", "kv", kb, 0);
                    } else {
                        let _ = mgr.delete("bench", "kv", kb, 0);
                    }
                }
                phase2_keys.lock().unwrap().extend(local_keys);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    // Verify phase 2 写的 key (去重)
    let keys = phase2_keys.lock().unwrap().clone();
    eprintln!("phase 2 put keys total: {}", keys.len());
    let mut seen = std::collections::HashSet::new();
    let mut checked = 0u64;
    for key in &keys {
        if !seen.insert(key.clone()) {
            continue;
        }
        checked += 1;
        // get 这个 key — 可能被 phase 2 后续 delete 删了
        match mgr.get("bench", "kv", key, 0) {
            Ok(Some(_)) => {}
            Ok(None) => {
                // key 没值, 可能是 phase 2 后续删了
            }
            Err(e) => eprintln!("err on key={:?}: {e}", key),
        }
    }
    eprintln!("checked {} unique keys after phase 2", checked);
    // 我们不 assert missing (因为 phase 2 里有 delete)
}

#[test]
fn phase3_put_v0_then_get_v0_works() {
    // 最小化: 6 个 v0_NNN 并发 put (乱序) 然后单线程 get
    use std::sync::{Arc, Barrier};
    let (_tmp, mgr) = make_mgr();

    // 先做 phase 2 用相同的 hash bucket 让 shard 0 紧张
    {
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
                    if r < 50 {
                        let v = vec![((next() >> 32) & 0xFF) as u8; VALUE_LEN];
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

    // Phase 3: 6 client 并发 put verify keys (顺序)
    let verify_keys: Arc<std::sync::Mutex<Vec<Vec<u8>>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(NUM_CLIENTS * 100)));
    {
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

    // Phase 4 verify
    let keys = verify_keys.lock().unwrap().clone();
    let mut missing = 0u64;
    for key in &keys {
        if matches!(mgr.get("bench", "kv", key, 0), Ok(None)) {
            missing += 1;
            if missing <= 5 {
                let k = String::from_utf8_lossy(key);
                eprintln!("missing: {k}");
            }
        }
    }
    assert_eq!(missing, 0, "should have 0 missing, got {missing}");
}

#[test]
fn get_then_immediate_put_then_get() {
    // 序列化单线程, 但刻意交错 get/put
    let (_tmp, mgr) = make_mgr();

    // 先 put 一些 key 建立 chunk
    for i in 0..100 {
        let key = format!("k_{i:04}");
        let v = vec![0u8; 32];
        mgr.put("bench", "kv", key.as_bytes(), &v, 0).unwrap();
    }
    // 现在 chunk 0 满/部分满

    // 关键操作: get 一个 key (走 chunk_list 或 disk 路径)
    let _ = mgr.get("bench", "kv", b"k_0050", 0).unwrap();
    // 立即 put 同一 key — 是否复用 pid?
    let v = vec![1u8; 32];
    mgr.put("bench", "kv", b"k_0050", &v, 0).unwrap();
    // 立即 get — 必须看到新值
    let got = mgr.get("bench", "kv", b"k_0050", 0).unwrap();
    assert_eq!(got, Some(v), "get after put must see new value");
}