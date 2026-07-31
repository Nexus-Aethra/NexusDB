//! 单元测试: write_page 后立刻 read, 验证一致性.
//!
//! 直接调用 StorageEngine API, 单线程, 排除 shard_manager 干扰.

use storage::{IoBackend, IoBackendConfig, OpenOptions};

fn make_engine(test_name: &str) -> (tempfile::TempDir, storage::StorageEngine) {
    let tmp = tempfile::tempdir().unwrap();
    let opts = OpenOptions {
        block_dir: Some(tmp.path().to_path_buf()),
        block_root: tmp.path().to_path_buf(),
        db_name: Some(test_name.to_string()),
        shard_id: 0,
        create_if_missing: true,
        chunk_cache_size: 16,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        wal_mode: Default::default(),
    };
    let rt = scheduler::SchedHandle::new(scheduler::Scheduler::new());
    rt.set_current();
    let mut engine = pollster::block_on(storage::StorageEngine::open(opts)).unwrap();
    pollster::block_on(engine.create_db("bench")).unwrap();
    pollster::block_on(engine.create_table("bench", "kv")).unwrap();
    (tmp, engine)
}

#[test]
fn write_then_read_consistency() {
    let (_tmp, mut engine) = make_engine("test1");

    // 写 100 个 keys
    for i in 0..100 {
        let key = format!("k{i:04}");
        let v = vec![((i * 7) & 0xFF) as u8; 32];
        pollster::block_on(engine.table_put("bench", "kv", key.as_bytes(), &v)).unwrap();
    }

    // 立刻读: 应该都拿到 Some (100/100)
    let mut found = 0u64;
    for i in 0..100 {
        let key = format!("k{i:04}");
        let got = pollster::block_on(engine.table_get("bench", "kv", key.as_bytes())).unwrap();
        if got.is_some() {
            found += 1;
        }
    }
    assert_eq!(found, 100, "write 后 read 应该 100% 命中, got {found}");
}

#[test]
fn interleaved_write_read() {
    let (_tmp, mut engine) = make_engine("test2");

    // write + read 交替, 模拟并发交错
    for i in 0..200 {
        let key = format!("k{i:04}");
        let v = vec![((i * 13) & 0xFF) as u8; 32];
        pollster::block_on(engine.table_put("bench", "kv", key.as_bytes(), &v)).unwrap();
        // 立刻 read 同一 key
        let got = pollster::block_on(engine.table_get("bench", "kv", key.as_bytes())).unwrap();
        assert!(got.is_some(), "key {key} 应该被 put 后 read");
    }

    // 再读所有
    let mut found = 0u64;
    for i in 0..200 {
        let key = format!("k{i:04}");
        let got = pollster::block_on(engine.table_get("bench", "kv", key.as_bytes())).unwrap();
        if got.is_some() {
            found += 1;
        }
    }
    assert_eq!(found, 200, "全部 read 应该 100% 命中, got {found}");
}

#[test]
fn batch_submission_with_split() {
    // 测试 batch submit + split 路径
    let (_tmp, mut engine) = make_engine("test3");

    // write 很多 keys, 触发多次 leaf split
    for i in 0..1000 {
        let key = format!("k{i:04}");
        let v = vec![((i * 17) & 0xFF) as u8; 32];
        pollster::block_on(engine.table_put("bench", "kv", key.as_bytes(), &v)).unwrap();
    }

    // 读所有
    let mut found = 0u64;
    for i in 0..1000 {
        let key = format!("k{i:04}");
        let got = pollster::block_on(engine.table_get("bench", "kv", key.as_bytes())).unwrap();
        if got.is_some() {
            found += 1;
        }
    }
    assert_eq!(found, 1000, "split 后 read 应该 100% 命中, got {found}");
}

/// 验证 shard_manager 层 + StorageEngine 在单 thread 写
/// + pollster::block_on (yield-once) 是否会导致 missing
#[test]
fn engine_table_put_get_with_many_splits() {
    let (_tmp, mut engine) = make_engine("test4");
    // 5000 keys, 强制多次 split
    for i in 0..5000 {
        let key = format!("k{i:06}");
        let v = vec![((i * 31) & 0xFF) as u8; 32];
        pollster::block_on(engine.table_put("bench", "kv", key.as_bytes(), &v)).unwrap();
    }
    let mut found = 0u64;
    for i in 0..5000 {
        let key = format!("k{i:06}");
        let got = pollster::block_on(engine.table_get("bench", "kv", key.as_bytes())).unwrap();
        if got.is_some() {
            found += 1;
        }
    }
    assert_eq!(found, 5000, "5000 split 后 read 应该 100% 命中, got {found}");
}