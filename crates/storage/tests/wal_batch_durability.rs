//! P0 回归: 批量写 (`table_put_many` / RESP MSET) 必须进 WAL.
//!
//! 旧实现直接走 `registry::table_put_many`, 没有 `append_put`:
//! strict 档 OK 回复后 kill -9, nowchunks 里的页丢失且 WAL 无记录 → 全丢.
//! 本测试在修复前必败 (`wal_needs_sync` 为 false / 重开后 key 丢失).

mod common;

use std::path::Path;

use common::run_async;
use storage::engine::OpenOptions;
use storage::wal::WalMode;
use storage::{IoBackend, IoBackendConfig, StorageEngine};

fn opts(root: &Path, mode: WalMode) -> OpenOptions {
    OpenOptions {
        block_root: root.to_path_buf(),
        block_dir: None,
        db_name: Some("default".to_string()),
        shard_id: 0,
        create_if_missing: true,
        chunk_cache_size: 4,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        wal_mode: mode,
    }
}

/// 模拟 kill -9: 不 flush 不 close, 直接泄漏 engine.
fn simulate_crash(e: StorageEngine) {
    std::mem::forget(e);
}

fn wal_bytes(root: &Path) -> u64 {
    let Ok(rd) = std::fs::read_dir(root) else {
        return 0;
    };
    rd.flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|s| s.starts_with("shard_") && s.contains(".wal."))
        })
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}

#[test]
fn batch_put_survives_crash_via_wal() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    run_async(async move {
        let mut e = StorageEngine::open(opts(&root, WalMode::Strict))
            .await
            .unwrap();
        e.create_db("app").await.unwrap();
        e.create_table("app", "kv").await.unwrap();
        // 生产路径 DDL 后强制 flush (manager 层行为) — WAL 只覆盖 KV.
        e.flush().await.unwrap();

        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..200u32)
            .map(|i| {
                (
                    format!("k{i:04}").into_bytes(),
                    format!("v{i:04}").into_bytes(),
                )
            })
            .collect();
        e.table_put_many("app", "kv", &pairs).await.unwrap();

        // 1) 批量写必须追加 WAL (修复前: false)
        assert!(
            e.wal_needs_sync(),
            "batch put must append WAL records (P0 regression)"
        );
        // 2) strict 回复前 barrier 必须把记录落盘
        e.wal_barrier().await.unwrap();
        let bytes = wal_bytes(&root);
        assert!(
            bytes > 0,
            "WAL segments must be non-empty after barrier (got {bytes})"
        );

        // 3) 崩溃模拟: nowchunks 里的页全部丢失
        simulate_crash(e);

        // 4) 重开: WAL 重放必须恢复全部 key
        let mut e2 = StorageEngine::open(opts(&root, WalMode::Strict))
            .await
            .unwrap();
        for (k, v) in &pairs {
            let got = e2.table_get("app", "kv", k).await.unwrap();
            assert_eq!(
                got.as_deref(),
                Some(v.as_slice()),
                "key {:?} lost after crash",
                String::from_utf8_lossy(k)
            );
        }
        e2.close().await.unwrap();
    });
}
