//! ⭐ meta log (实验): shard 异步链路 — 1s 节奏的 drain / merge 真实跑通.
//!
//! 用环境变量把 drain/merge 间隔压到 20ms/50ms, 断言:
//! 1) pending 先被 append 成日志段 (drain)
//! 2) 段随后被合并进 page.mate 并删除 (merge)
//! 3) 关闭后重开数据完整

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use shard_manager::{ShardManager, ShardManagerOptions};
use storage::wal::WalMode;
use storage::{IoBackend, IoBackendConfig};

fn opts(root: PathBuf, create: bool) -> ShardManagerOptions {
    ShardManagerOptions {
        num_shards: 1,
        block_root: root,
        create_if_missing: create,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 4,
        reply_bus_count: None,
        wal_mode: WalMode::Off,
    }
}

fn shard_dir(root: &Path) -> PathBuf {
    // ShardManager: block_root/shard_N, engine: block_root/db/shard_N
    root.join("shard_0").join("default").join("shard_0")
}

fn log_segments(dir: &Path) -> Vec<PathBuf> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<PathBuf> = rd
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|s| s.starts_with("page.metlog."))
        })
        .map(|e| e.path())
        .collect();
    out.sort();
    out
}

fn wait_until(what: &str, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if pred() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timeout waiting for: {what}");
}

#[test]
fn async_meta_log_drains_then_merges() {
    unsafe {
        std::env::set_var("NEXUSDB_META_LOG", "1");
        std::env::set_var("NEXUSDB_META_LOG_DRAIN_MS", "20");
        std::env::set_var("NEXUSDB_META_LOG_MERGE_MS", "50");
    }
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    let dir = shard_dir(&root);
    {
        let mgr = ShardManager::open(opts(root.clone(), true)).expect("open");
        mgr.create_db("app").expect("create db");
        mgr.create_table("app", "kv").expect("create table");
        for i in 0..500u32 {
            let key = format!("k{i:04}");
            mgr.put("app", "kv", key.as_bytes(), b"v", 0).expect("put");
        }

        // 1) drain: 段文件出现 (pending 已 append)
        wait_until("meta log drain (segment appears)", || {
            !log_segments(&dir).is_empty()
        });
        // 2) merge: 段被合并进 mate 并删除
        wait_until("meta log merge (segments deleted)", || {
            log_segments(&dir).is_empty()
        });

        for i in 0..500u32 {
            let key = format!("k{i:04}");
            assert_eq!(
                mgr.get("app", "kv", key.as_bytes(), 0).unwrap().as_deref(),
                Some(b"v".as_ref()),
                "key{i} 读不到"
            );
        }
        mgr.close().expect("close");
    }

    // 3) 重开 (仍启用 log), 数据完整
    let mgr2 = ShardManager::open(opts(root, false)).expect("reopen");
    for i in 0..500u32 {
        let key = format!("k{i:04}");
        assert_eq!(
            mgr2.get("app", "kv", key.as_bytes(), 0).unwrap().as_deref(),
            Some(b"v".as_ref()),
            "key{i} 重开后丢失"
        );
    }
    mgr2.close().expect("close2");

    unsafe {
        std::env::remove_var("NEXUSDB_META_LOG");
        std::env::remove_var("NEXUSDB_META_LOG_DRAIN_MS");
        std::env::remove_var("NEXUSDB_META_LOG_MERGE_MS");
    }
}
