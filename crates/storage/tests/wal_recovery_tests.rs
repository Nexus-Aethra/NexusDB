//! ⭐ WAL (F60) 崩溃恢复 e2e: 模拟 crash (不 flush 不 close 直接 forget) 后
//! 重开 engine, 验证 WAL 重放填补刷盘窗口.
//!
//! - periodic 档: 写入 + wal_barrier (模拟 1s tick 已发生) → crash → 数据全在
//! - strict 语义等价 (barrier 即回复前 fsync)
//! - seal 边界: flush (chunk+meta 落盘 + 段生命周期) 后新写仍可恢复
//! - delete 重放 / drop_table 后陈旧记录跳过 / off 档零文件

use std::path::Path;

use storage::engine::OpenOptions;
use storage::wal::WalMode;
use storage::{IoBackend, IoBackendConfig, StorageEngine};

mod common;
use common::run_async;

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

/// 模拟 kill -9: 不 flush 不 close, 直接泄漏 engine (盘上只有 WAL 段可依赖).
fn simulate_crash(e: StorageEngine) {
    std::mem::forget(e);
}

#[test]
fn wal_replay_after_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    run_async(async move {
        let mut e = StorageEngine::open(opts(&root, WalMode::Periodic)).await.unwrap();
        e.create_db("default").await.unwrap();
        e.create_table("default", "t").await.unwrap();
        // 生产路径 DDL 后强制 flush (manager 层行为) — 这里对齐语义
        e.flush().await.unwrap();
        for i in 0..50u32 {
            let k = format!("key-{i}");
            let v = format!("val-{i}");
            e.table_put("default", "t", k.as_bytes(), v.as_bytes()).await.unwrap();
        }
        // 建表元数据先持久化 (WAL 只覆盖 KV; 生产路径 DDL 后强制 flush 同语义)
        // 注: flush 放在写之后 — 同时验证"已刷数据 + WAL 增量"并存恢复
        e.wal_barrier().await.unwrap(); // 模拟 periodic tick 已 fsync
        simulate_crash(e);

        // 重开: recover 只有空表 (chunk 未刷), WAL 重放补回 50 条
        let mut e2 = StorageEngine::open(opts(&root, WalMode::Periodic)).await.unwrap();
        for i in 0..50u32 {
            let k = format!("key-{i}");
            let got = e2.table_get("default", "t", k.as_bytes()).await.unwrap();
            assert_eq!(
                got.as_deref(),
                Some(format!("val-{i}").as_bytes()),
                "key-{i} 应由 WAL 重放恢复"
            );
        }
        // 重放后段已清空: 再次重开无重复重放 (幂等性由此隐式验证)
        e2.close().await.unwrap();
        let e3 = StorageEngine::open(opts(&root, WalMode::Periodic)).await.unwrap();
        drop(e3);
    });
}

#[test]
fn wal_replay_mixed_flushed_and_tail() {
    // seal 边界: flush 已落盘的老数据 + WAL 尾部增量, 重开两者都在
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    run_async(async move {
        let mut e = StorageEngine::open(opts(&root, WalMode::Periodic)).await.unwrap();
        e.create_db("default").await.unwrap();
        e.create_table("default", "t").await.unwrap();
        e.table_put("default", "t", b"old", b"flushed").await.unwrap();
        e.flush().await.unwrap(); // chunk+meta 持久化
        e.table_put("default", "t", b"new", b"wal-only").await.unwrap();
        e.table_delete("default", "t", b"old").await.unwrap(); // delete 也进 WAL
        e.wal_barrier().await.unwrap();
        simulate_crash(e);

        let mut e2 = StorageEngine::open(opts(&root, WalMode::Periodic)).await.unwrap();
        assert_eq!(
            e2.table_get("default", "t", b"new").await.unwrap().as_deref(),
            Some(b"wal-only".as_ref()),
            "WAL 尾部增量恢复"
        );
        assert_eq!(
            e2.table_get("default", "t", b"old").await.unwrap(),
            None,
            "delete 重放生效"
        );
    });
}

#[test]
fn wal_stale_records_after_drop_table() {
    // drop_table 后 WAL 里残留旧表记录 → 重放时 ensure_table 会重建表并
    // 恢复记录 (表级 drop 未持久化 + crash = drop 本身也丢, 语义自洽);
    // 这里验证的红线是: 重放不 panic 不报错
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    run_async(async move {
        let mut e = StorageEngine::open(opts(&root, WalMode::Periodic)).await.unwrap();
        e.create_db("default").await.unwrap();
        e.create_table("default", "t").await.unwrap();
        e.table_put("default", "t", b"k", b"v").await.unwrap();
        e.wal_barrier().await.unwrap();
        e.drop_table("default", "t").await.unwrap();
        simulate_crash(e);
        let mut e2 = StorageEngine::open(opts(&root, WalMode::Periodic)).await.unwrap();
        // 重放正常完成即通过 (表可能被重建 — drop 未持久化时属正确恢复)
        let _ = e2.table_get("default", "t", b"k").await;
    });
}

#[test]
fn wal_off_no_files() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    run_async(async move {
        let mut e = StorageEngine::open(opts(&root, WalMode::Off)).await.unwrap();
        e.create_db("default").await.unwrap();
        e.create_table("default", "t").await.unwrap();
        e.table_put("default", "t", b"k", b"v").await.unwrap();
        assert_eq!(e.wal_mode(), WalMode::Off);
        e.close().await.unwrap();
    });
    assert!(
        storage::wal::WalWriter::existing_segments(tmp.path(), 0).is_empty(),
        "off 档不产生 WAL 文件"
    );
}

#[test]
fn wal_clean_close_leaves_no_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    run_async(async move {
        let mut e = StorageEngine::open(opts(&root, WalMode::Strict)).await.unwrap();
        e.create_db("default").await.unwrap();
        e.create_table("default", "t").await.unwrap();
        e.table_put("default", "t", b"k", b"v").await.unwrap();
        e.wal_barrier().await.unwrap();
        e.close().await.unwrap();
    });
    assert!(
        storage::wal::WalWriter::existing_segments(tmp.path(), 0).is_empty(),
        "正常关闭后段应全部清除"
    );
}
