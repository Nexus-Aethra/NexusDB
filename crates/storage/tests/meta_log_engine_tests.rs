//! ⭐ meta log (实验): engine 级开关 + 写入 / close / 重开可见性.
//!
//! 该文件独立进程运行; 测试内设置 `NEXUSDB_META_LOG=1` 不影响其他测试二进制.

mod common;

use common::run_async;
use storage::engine::OpenOptions;
use storage::{IoBackend, IoBackendConfig, StorageEngine};

fn opts_for(tmp: &tempfile::TempDir) -> OpenOptions {
    OpenOptions {
        block_root: tmp.path().to_path_buf(),
        block_dir: None,
        db_name: Some("default".to_string()),
        shard_id: 0,
        create_if_missing: true,
        chunk_cache_size: 4,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        // WAL off: 只验证 meta log + close flush 路径本身
        wal_mode: storage::wal::WalMode::Off,
    }
}

#[test]
fn engine_roundtrip_with_meta_log() {
    unsafe { std::env::set_var("NEXUSDB_META_LOG", "1") };
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        assert!(e.pager_mut().meta_log_enabled(), "env 开关必须生效");
        e.create_db("db1").await.unwrap();
        e.create_table("db1", "t1").await.unwrap();
        for i in 0..300u32 {
            let k = format!("key{i:04}");
            let v = format!("val{i}");
            e.table_put("db1", "t1", k.as_bytes(), v.as_bytes())
                .await
                .unwrap();
        }
        e.close().await.unwrap();

        // close = flush_dirty_log: 日志 + mate 全量落盘, 段清空
        let dir = tmp.path().join("default").join("shard_0");
        let leftover: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .is_some_and(|s| s.starts_with("page.metlog."))
            })
            .collect();
        assert!(leftover.is_empty(), "close 后无残留日志段");

        // 重开 (仍启用 log): 数据全部可见
        let mut e2 = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        assert!(e2.pager_mut().meta_log_enabled());
        for i in 0..300u32 {
            let k = format!("key{i:04}");
            let got = e2.table_get("db1", "t1", k.as_bytes()).await.unwrap();
            assert_eq!(
                got.as_deref(),
                Some(format!("val{i}").as_bytes()),
                "key{i} 重开后必须可见"
            );
        }
        e2.close().await.unwrap();
    });
    unsafe { std::env::remove_var("NEXUSDB_META_LOG") };
}
