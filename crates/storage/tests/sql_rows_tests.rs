//! ⭐ Q4: SQL row 表 + 本地二级索引 引擎层测试.

use std::sync::Arc;

use storage::engine::OpenOptions;
use storage::row::ColValue;
use storage::schema::{ColType, Column, TableSchema};
use storage::{IoBackend, IoBackendConfig, StorageEngine};

mod common;

use common::run_async;

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
        wal_mode: Default::default(),
    }
}

/// (id I64 pk, name Str idx0, score F64 idx1, note Str nullable)
fn demo_schema() -> TableSchema {
    TableSchema::new(
        vec![
            Column { name: "id".into(), ty: ColType::I64, nullable: false, default: None },
            Column { name: "name".into(), ty: ColType::Str, nullable: false, default: None },
            Column { name: "score".into(), ty: ColType::F64, nullable: true, default: None },
            Column { name: "note".into(), ty: ColType::Str, nullable: true, default: None },
        ],
        0,
        &[1, 2], // iid 0 = name, iid 1 = score
        &[], &[], &[], &[])
    .unwrap()
}

fn vals(id: i64, name: &str, score: Option<f64>) -> Vec<ColValue> {
    vec![
        ColValue::I64(id),
        ColValue::Bytes(name.as_bytes().to_vec()),
        score.map(ColValue::F64).unwrap_or(ColValue::Null),
        ColValue::Null,
    ]
}

fn pk(id: i64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

async fn setup(e: &mut StorageEngine) -> Arc<TableSchema> {
    e.create_db("db1").await.unwrap();
    e.create_table("db1", "t1").await.unwrap();
    e.set_schema("db1", "t1", &demo_schema()).await.unwrap();
    e.get_schema("db1", "t1").await.unwrap().unwrap()
}

#[test]
fn schema_persist_reopen() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
            let s = setup(&mut e).await;
            assert_eq!(s.indexes.len(), 2);
            e.flush().await.unwrap();
            e.close().await.unwrap();
        }
        // reopen: schema 从 [$] 行 lazy load
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        let s = e.get_schema("db1", "t1").await.unwrap().unwrap();
        assert_eq!(*s, demo_schema());
        // 无 schema 表 → None (纯 KV)
        e.create_table("db1", "kv").await.unwrap();
        assert!(e.get_schema("db1", "kv").await.unwrap().is_none());
    });
}

#[test]
fn row_put_get_delete_with_index_consistency() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        let s = setup(&mut e).await;

        for i in 0..10i64 {
            e.row_put("db1", "t1", &pk(i), &vals(i, &format!("user{}", i % 3), Some(i as f64)))
                .await
                .unwrap();
        }
        // row_get + decode
        let bytes = e.row_get("db1", "t1", &pk(7)).await.unwrap().unwrap();
        let cols = storage::row::decode_row(&s, &bytes).unwrap();
        assert_eq!(cols[0], ColValue::I64(7));
        assert_eq!(cols[1], ColValue::Bytes(b"user1".to_vec()));

        // iid 0 (name) 等值: user1 → id 1,4,7
        let eq = ColValue::Bytes(b"user1".to_vec());
        let hits = e
            .index_scan_local("db1", "t1", 0, Some(&eq), Some(&eq), 0)
            .await
            .unwrap();
        assert_eq!(
            hits.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>(),
            vec![pk(1), pk(4), pk(7)],
            "一对多相邻行 + 回表"
        );

        // update: id7 改名 user2, score 改 NULL → 旧索引行消失、score 行消失
        e.row_put("db1", "t1", &pk(7), &vals(7, "user2", None)).await.unwrap();
        let hits = e
            .index_scan_local("db1", "t1", 0, Some(&eq), Some(&eq), 0)
            .await
            .unwrap();
        assert_eq!(hits.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>(), vec![pk(1), pk(4)]);
        // score 索引 (iid 1) 只剩 9 条 (id7 NULL 不入索引)
        let all_scores = e
            .index_scan_keys_local("db1", "t1", 1, None, None, 0)
            .await
            .unwrap();
        assert_eq!(all_scores.len(), 9);

        // delete: 行 + 全部索引行同步消失
        assert!(e.row_delete("db1", "t1", &pk(4)).await.unwrap());
        assert!(!e.row_delete("db1", "t1", &pk(4)).await.unwrap());
        assert_eq!(e.row_get("db1", "t1", &pk(4)).await.unwrap(), None);
        let all_names = e
            .index_scan_keys_local("db1", "t1", 0, None, None, 0)
            .await
            .unwrap();
        assert_eq!(all_names.len(), 9, "删行后 name 索引 10-1 条");
        // 索引数与行数严格一致校验
        let all_scores = e
            .index_scan_keys_local("db1", "t1", 1, None, None, 0)
            .await
            .unwrap();
        assert_eq!(all_scores.len(), 8, "score: 10 - NULL(id7) - 删除(id4)");
    });
}

#[test]
fn index_range_scan_and_limit() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        setup(&mut e).await;
        for i in 0..20i64 {
            e.row_put("db1", "t1", &pk(i), &vals(i, "n", Some(i as f64 - 10.0)))
                .await
                .unwrap();
        }
        // score ∈ [-2.0, 3.0] 闭区间 → id 8..=13
        let (lo, hi) = (ColValue::F64(-2.0), ColValue::F64(3.0));
        let hits = e
            .index_scan_local("db1", "t1", 1, Some(&lo), Some(&hi), 0)
            .await
            .unwrap();
        assert_eq!(
            hits.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>(),
            (8..=13).map(pk).collect::<Vec<_>>(),
            "闭区间按 score 升序"
        );
        // limit 下推早停
        let hits = e
            .index_scan_local("db1", "t1", 1, Some(&lo), None, 3)
            .await
            .unwrap();
        assert_eq!(hits.len(), 3);
        assert_eq!(hits[0].0, pk(8));
        // 无下界
        let hits = e
            .index_scan_local("db1", "t1", 1, None, Some(&ColValue::F64(-8.0)), 0)
            .await
            .unwrap();
        assert_eq!(hits.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>(), vec![pk(0), pk(1), pk(2)]);
    });
}

#[test]
fn string_index_binary_safe_ordering() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        setup(&mut e).await;
        // 含 0x00 的名字 + 边界值 (字典序: "a" < "a\x00" < "a\x01" < "ab")
        let names: [&[u8]; 4] = [b"a", b"a\x00", b"a\x01", b"ab"];
        for (i, n) in names.iter().enumerate() {
            let v = vec![
                ColValue::I64(i as i64),
                ColValue::Bytes(n.to_vec()),
                ColValue::Null,
                ColValue::Null,
            ];
            e.row_put("db1", "t1", &pk(i as i64), &v).await.unwrap();
        }
        // 全量扫按值序返回, 原值无损还原
        let all = e
            .index_scan_keys_local("db1", "t1", 0, None, None, 0)
            .await
            .unwrap();
        assert_eq!(
            all.iter().map(|(v, _)| v.as_slice()).collect::<Vec<_>>(),
            names.to_vec(),
            "0x00 转义后仍保序且可还原"
        );
        // "a" 等值不误圈 "a\x00"/"ab"
        let eq = ColValue::Bytes(b"a".to_vec());
        let hits = e
            .index_scan_local("db1", "t1", 0, Some(&eq), Some(&eq), 0)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, pk(0));
        // 范围 ["a\x00", "a\x01"] 闭区间
        let (lo, hi) = (
            ColValue::Bytes(b"a\x00".to_vec()),
            ColValue::Bytes(b"a\x01".to_vec()),
        );
        let hits = e
            .index_scan_local("db1", "t1", 0, Some(&lo), Some(&hi), 0)
            .await
            .unwrap();
        assert_eq!(
            hits.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>(),
            vec![pk(1), pk(2)]
        );
    });
}

#[test]
fn row_table_type_isolation() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        setup(&mut e).await;
        // row 表上复合命令 → WRONGTYPE
        let err = e
            .hash_set("db1", "t1", b"k", &[(b"f".to_vec(), vec![0x01, b'v'])])
            .await;
        assert!(err.is_err(), "row 表 HSET 应 WRONGTYPE");
        // 纯 KV 写过的 key 上 row_put → WRONGTYPE (非 TAG_ROW 旧值)
        e.table_put("db1", "t1", b"\x00rawkey", &[0x01, b'v']).await.unwrap();
        let err = e.row_put("db1", "t1", b"\x00rawkey", &vals(1, "n", None)).await;
        assert!(err.is_err(), "非 TAG_ROW 旧值上 row_put 应报错");
        // 无 schema 表上 row op → Schema 错误
        e.create_table("db1", "kv").await.unwrap();
        assert!(e.row_put("db1", "kv", &pk(1), &vals(1, "n", None)).await.is_err());
    });
}

#[test]
fn rows_and_indexes_survive_reopen() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
            setup(&mut e).await;
            for i in 0..300i64 {
                // 300 行跨 leaf (索引行 + row 行 + [$] 行混居一棵树)
                e.row_put("db1", "t1", &pk(i), &vals(i, &format!("u{}", i % 5), Some(i as f64)))
                    .await
                    .unwrap();
            }
            e.flush().await.unwrap();
            e.close().await.unwrap();
        }
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        // 等值: u3 → id 3,8,...,298 共 60 条
        let eq = ColValue::Bytes(b"u3".to_vec());
        let hits = e
            .index_scan_local("db1", "t1", 0, Some(&eq), Some(&eq), 0)
            .await
            .unwrap();
        assert_eq!(hits.len(), 60);
        assert_eq!(hits[0].0, pk(3));
        // 范围 + 回表数据正确
        let (lo, hi) = (ColValue::F64(100.0), ColValue::F64(102.0));
        let hits = e
            .index_scan_local("db1", "t1", 1, Some(&lo), Some(&hi), 0)
            .await
            .unwrap();
        assert_eq!(hits.len(), 3);
        let s = e.get_schema("db1", "t1").await.unwrap().unwrap();
        let cols = storage::row::decode_row(&s, &hits[0].1).unwrap();
        assert_eq!(cols[0], ColValue::I64(100));
    });
}

/// ⭐ Q6: crash 模拟 — flush 后不 close 直接 drop (无 close 簿记),
/// reopen 走 recover 扫 .block 重建; index_scan 与全表逐 pk 点查一致.
#[test]
fn crash_reopen_index_consistent_with_rows()  {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
            setup(&mut e).await;
            for i in 0..50i64 {
                e.row_put("db1", "t1", &pk(i), &vals(i, &format!("u{}", i % 4), Some(i as f64)))
                    .await
                    .unwrap();
            }
            e.flush().await.unwrap();
            // 不 close: 模拟进程被杀 (drop 不做 close 簿记)
            drop(e);
        }
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        // 索引全扫出的 (pk 集合) 与逐 pk 点查一致
        let mut index_pks: Vec<Vec<u8>> = Vec::new();
        for iid in [0u32] {
            for (_, p) in e
                .index_scan_keys_local("db1", "t1", iid, None, None, 0)
                .await
                .unwrap()
            {
                index_pks.push(p);
            }
        }
        index_pks.sort();
        assert_eq!(index_pks.len(), 50, "name 索引条目数 == 行数");
        for p in &index_pks {
            assert!(
                e.row_get("db1", "t1", p).await.unwrap().is_some(),
                "索引指向的行必须存在"
            );
        }
        // 反向: 每行的索引值可查回自身
        let eq = ColValue::Bytes(b"u2".to_vec());
        let hits = e
            .index_scan_local("db1", "t1", 0, Some(&eq), Some(&eq), 0)
            .await
            .unwrap();
        assert_eq!(hits.len(), 12, "u2: id 2,6,...,46 共 12 条 (0..50 步长 4 偏移 2)");
    });
}

/// ⭐ Y1: 布隆剪枝 — 等值 miss 短路 (skip 计数)、命中不误杀、
/// 范围不受影响、reopen 重建后仍生效且无假阴性.
#[test]
fn bloom_prunes_equality_miss_without_false_negative() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
            setup(&mut e).await;
            for i in 0..30i64 {
                e.row_put("db1", "t1", &pk(i), &vals(i, &format!("u{}", i % 4), Some(i as f64)))
                    .await
                    .unwrap();
            }
            // 命中: 全部已插入值可查 (无假阴性), skip 不增长
            let base_skip = e.bloom_skip_count;
            for m in 0..4 {
                let eq = ColValue::Bytes(format!("u{m}").into_bytes());
                let hits = e
                    .index_scan_local("db1", "t1", 0, Some(&eq), Some(&eq), 0)
                    .await
                    .unwrap();
                assert!(!hits.is_empty(), "已插入值 u{m} 不可被误杀");
            }
            assert_eq!(e.bloom_skip_count, base_skip, "命中路径不应触发短路");

            // miss: 未插入值 → bloom 短路 (skip 计数 + 空结果)
            let eq = ColValue::Bytes(b"nope".to_vec());
            let hits = e
                .index_scan_local("db1", "t1", 0, Some(&eq), Some(&eq), 0)
                .await
                .unwrap();
            assert!(hits.is_empty());
            assert_eq!(e.bloom_skip_count, base_skip + 1, "miss 应被 bloom 短路");

            // 范围查询不剪枝 (即便区间空)
            let (lo, hi) = (ColValue::F64(500.0), ColValue::F64(600.0));
            let hits = e
                .index_scan_local("db1", "t1", 1, Some(&lo), Some(&hi), 0)
                .await
                .unwrap();
            assert!(hits.is_empty());
            assert_eq!(e.bloom_skip_count, base_skip + 1, "范围查询不走 bloom");

            e.flush().await.unwrap();
            e.close().await.unwrap();
        }
        // reopen: rebuild 扫 [I] 重建 bloom → 剪枝生效且无假阴性
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        for m in 0..4 {
            let eq = ColValue::Bytes(format!("u{m}").into_bytes());
            let hits = e
                .index_scan_local("db1", "t1", 0, Some(&eq), Some(&eq), 0)
                .await
                .unwrap();
            assert!(!hits.is_empty(), "reopen 后 u{m} 不可被误杀");
        }
        assert_eq!(e.bloom_skip_count, 0);
        let eq = ColValue::Bytes(b"ghost".to_vec());
        let hits = e
            .index_scan_local("db1", "t1", 0, Some(&eq), Some(&eq), 0)
            .await
            .unwrap();
        assert!(hits.is_empty());
        assert_eq!(e.bloom_skip_count, 1, "reopen 后重建的 bloom 应能剪枝");
        // 数值索引等值 miss 同样短路
        let eq = ColValue::F64(999.0);
        let hits = e
            .index_scan_local("db1", "t1", 1, Some(&eq), Some(&eq), 0)
            .await
            .unwrap();
        assert!(hits.is_empty());
        assert_eq!(e.bloom_skip_count, 2);
    });
}

/// ⭐ O3: UNIQUE 约束 — 单 shard 引擎级确定性验证 (e2e hash 不可控的补充).
#[test]
fn unique_index_rejects_duplicates() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        e.create_db("db1").await.unwrap();
        e.create_table("db1", "t1").await.unwrap();
        // email (col 1) 唯一索引
        let schema = TableSchema::new(
            vec![
                Column { name: "id".into(), ty: ColType::I64, nullable: false, default: None },
                Column { name: "email".into(), ty: ColType::Str, nullable: false, default: None },
            ],
            0,
            &[],
            &[1], &[], &[], &[])
        .unwrap();
        e.set_schema("db1", "t1", &schema).await.unwrap();
        let row = |id: i64, em: &str| {
            vec![ColValue::I64(id), ColValue::Bytes(em.as_bytes().to_vec())]
        };
        e.row_put("db1", "t1", &pk(1), &row(1, "a@x")).await.unwrap();
        e.row_put("db1", "t1", &pk(2), &row(2, "b@x")).await.unwrap();
        // 不同 pk 撞已有值 → 拒绝
        let err = e.row_put("db1", "t1", &pk(3), &row(3, "a@x")).await;
        assert!(
            err.as_ref().is_err_and(|e| e.to_string().contains("duplicate key")),
            "重复 unique 值必须拒绝: {err:?}"
        );
        // 同 pk 同值覆盖 → 不误报
        e.row_put("db1", "t1", &pk(1), &row(1, "a@x")).await.unwrap();
        // 同 pk 换成空闲值 → ok; 旧值让位后他行可用
        e.row_put("db1", "t1", &pk(1), &row(1, "c@x")).await.unwrap();
        e.row_put("db1", "t1", &pk(3), &row(3, "a@x")).await.unwrap();
        // 换成已占值 → 拒绝
        let err = e.row_put("db1", "t1", &pk(2), &row(2, "c@x")).await;
        assert!(err.is_err(), "换值撞库应拒绝");
        // 拒绝路径无半写: pk2 仍是旧值
        let bytes = e.row_get("db1", "t1", &pk(2)).await.unwrap().unwrap();
        let cols = storage::row::decode_row(&schema, &bytes).unwrap();
        assert_eq!(cols[1], ColValue::Bytes(b"b@x".to_vec()));
    });
}
