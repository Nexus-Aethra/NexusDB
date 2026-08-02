//! ⭐ 临时诊断: 批量 row_put 10000 行 (有序 pk + 2 索引) 复现 btree bad page.
use storage::row::ColValue;
use storage::schema::{ColType, Column, TableSchema};
use storage::wal::WalMode;
use storage::{IoBackend, IoBackendConfig, OpenOptions, StorageEngine};

mod common;
use common::run_async;

fn demo_schema() -> TableSchema {
    TableSchema::new(
        vec![
            Column { name: "id".into(), ty: ColType::I64, nullable: false },
            Column { name: "name".into(), ty: ColType::Str, nullable: false },
            Column { name: "score".into(), ty: ColType::F64, nullable: true },
        ],
        0,
        &[1, 2],
        &[],
        &[],
    )
    .unwrap()
}

fn opts_for(tmp: &tempfile::TempDir) -> OpenOptions {
    OpenOptions {
        block_root: tmp.path().to_path_buf().clone(),
        block_dir: None,
        db_name: Some("default".to_string()),
        shard_id: 0,
        create_if_missing: true,
        chunk_cache_size: 4,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        wal_mode: WalMode::Off,
    }
}

#[test]
fn stress_10000_rows() {
    for round in 0..5 {
        run_async(async move {
            let tmp = tempfile::tempdir().unwrap();
            let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
            e.create_db("default").await.unwrap();
            e.create_table("default", "t1").await.unwrap();
            let s = demo_schema();
            e.set_schema("default", "t1", &s).await.unwrap();
            // ⭐ 乱序插入模拟分片路由 (7919 与 10000 互质 → 乘法置换 = 0..9999 全排列)
            for k in 0..10000i64 {
                let pk = (k * 7919) % 10000;
                let row = vec![
                    ColValue::I64(pk),
                    ColValue::Bytes(format!("u{}", k % 50).into_bytes()),
                    ColValue::Null,
                ];
                if let Err(err) = e.row_put("default", "t1", &pk.to_be_bytes(), &row).await {
                    panic!("round {round} row {k} (pk={pk}) put failed: {err}");
                }
            }
            let n = e.estimate_row_count("default", "t1");
            assert_eq!(n, Some(10000), "round {round}: count {n:?}");
            let n = e.estimate_row_count("default", "t1");
            assert_eq!(n, Some(10000), "round {round}: count {n:?}");
            // ⭐ 实际扫描校验 (e2e 的 COUNT 走扫描; estimate_row_count 是内存计数)
            let scanned = e.index_scan_keys_local("default", "t1", 0, None, None, 0).await.unwrap();
            assert_eq!(
                scanned.len(),
                10000,
                "round {round}: scanned {} < 10000 (分裂后 key 不可达?)",
                scanned.len()
            );
            e.flush().await.unwrap();
            e.close().await.unwrap();
        });
    }
}
