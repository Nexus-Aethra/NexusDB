//! ⭐ M3-1 (CBO 统计): 每表近似行数估计 — `StorageEngine::estimate_row_count`.
//!
//! 验证 put 新 key +1 / 覆盖不加 / delete -1 / put_many 近似 +N / 多表隔离.
//!
//! 用途: 为 M3-2 连接顺序 (NestedLoop 小表驱动) 与 M3-3 代价模型提供行数基.

use storage::row::ColValue;
use storage::schema::{ColType, Column, TableSchema};
use storage::{IoBackend, IoBackendConfig, OpenOptions, StorageEngine};

mod common;

use common::run_async;

/// (id I64 pk, name Str idx0, score F64 nullable)
fn demo_schema() -> TableSchema {
    TableSchema::new(
        vec![
            Column { name: "id".into(), ty: ColType::I64, nullable: false },
            Column { name: "name".into(), ty: ColType::Str, nullable: false },
            Column { name: "score".into(), ty: ColType::F64, nullable: true },
        ],
        0,
        &[1], // iid 0 = name
        &[],
        &[],
    )
    .unwrap()
}

// =====================================================================
// 测试 helper
// =====================================================================

fn setup() -> (tempfile::TempDir, OpenOptions) {
    let tmp = tempfile::tempdir().unwrap();
    let opts = OpenOptions {
        block_root: tmp.path().to_path_buf().clone(),
        block_dir: None,
        db_name: Some("default".to_string()),
        shard_id: 0,
        create_if_missing: true,
        chunk_cache_size: 4,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        wal_mode: Default::default(),
    };
    (tmp, opts)
}

// =====================================================================
// M3-1: 行数统计
// =====================================================================

/// 新 key 插入 +1; 覆盖不加; delete 存在才 -1.
#[test]
fn row_count_insert_overwrite_delete() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");
        e.create_db("app").await.expect("create db");
        e.create_table("app", "t").await.expect("create table");

        // 无记录 → None (视为未知/小表)
        assert_eq!(e.estimate_row_count("app", "t"), None);

        // 3 个新 key → 3
        e.table_put("app", "t", b"k1", b"v1").await.unwrap();
        e.table_put("app", "t", b"k2", b"v2").await.unwrap();
        e.table_put("app", "t", b"k3", b"v3").await.unwrap();
        assert_eq!(e.estimate_row_count("app", "t"), Some(3));

        // 覆盖 k2 (存在) → 仍 3
        e.table_put("app", "t", b"k2", b"v2x").await.unwrap();
        assert_eq!(e.estimate_row_count("app", "t"), Some(3));

        // 删除 k1 (存在) → 2
        assert!(e.table_delete("app", "t", b"k1").await.unwrap());
        assert_eq!(e.estimate_row_count("app", "t"), Some(2));

        // 删除不存在的 key → 返回 false, 行数不变
        assert!(!e.table_delete("app", "t", b"nope").await.unwrap());
        assert_eq!(e.estimate_row_count("app", "t"), Some(2));
    });
}

/// put_many 近似 +N (覆盖会高估, 近似基数可接受); 多表/多 db 隔离.
#[test]
fn row_count_put_many_and_isolation() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");
        e.create_db("app").await.expect("create db");
        e.create_table("app", "t").await.expect("create table");
        e.create_table("app", "u").await.expect("create table u");

        // 批量 5 新 key → 5
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..5)
            .map(|i| (format!("k{i}").into_bytes(), b"v".to_vec()))
            .collect();
        e.table_put_many("app", "t", &pairs).await.unwrap();
        assert_eq!(e.estimate_row_count("app", "t"), Some(5));

        // 另一个表独立计数
        e.table_put("app", "u", b"a", b"1").await.unwrap();
        assert_eq!(e.estimate_row_count("app", "u"), Some(1));
        assert_eq!(e.estimate_row_count("app", "t"), Some(5));

        // 单 key 追加 → 6
        e.table_put("app", "t", b"k9", b"v9").await.unwrap();
        assert_eq!(e.estimate_row_count("app", "t"), Some(6));
    });
}

/// ⭐ M3-4: 索引列近似 distinct 基数 — 插入不同值 +1, 重复/覆盖不变.
#[test]
fn distinct_counts_via_index_write() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");
        e.create_db("db1").await.expect("create db");
        e.create_table("db1", "t1").await.expect("create table");
        e.set_schema("db1", "t1", &demo_schema()).await.expect("set schema");

        // 无记录 → None (未知)
        assert_eq!(e.estimate_distinct("db1", "t1", 0), None);

        // 6 行, name 3 个不同值 (i % 3) → distinct = 3
        for i in 0..6i64 {
            let row = vec![
                ColValue::I64(i),
                ColValue::Bytes(format!("u{}", i % 3).into_bytes()),
                ColValue::Null,
            ];
            e.row_put("db1", "t1", &i.to_be_bytes(), &row).await.unwrap();
        }
        assert_eq!(e.estimate_distinct("db1", "t1", 0), Some(3));

        // 覆盖写同索引值 (old==new) → 索引行不动, distinct 不变
        let row = vec![
            ColValue::I64(0),
            ColValue::Bytes(b"u0".to_vec()),
            ColValue::Null,
        ];
        e.row_put("db1", "t1", &0i64.to_be_bytes(), &row).await.unwrap();
        assert_eq!(e.estimate_distinct("db1", "t1", 0), Some(3));

        // 新值 → 4
        let row = vec![
            ColValue::I64(6),
            ColValue::Bytes(b"u9".to_vec()),
            ColValue::Null,
        ];
        e.row_put("db1", "t1", &6i64.to_be_bytes(), &row).await.unwrap();
        assert_eq!(e.estimate_distinct("db1", "t1", 0), Some(4));
    });
}
