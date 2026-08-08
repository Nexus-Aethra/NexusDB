//! ⭐ Q5: SQL row 表 + 本地二级索引 跨 shard e2e.
//!
//! 验证核心设计: row 按 PK hash 分散多 shard, 索引行与 row co-location;
//! IndexScan 广播 → 各 shard 本地扫 + 本地回表 → 聚合全局有序.

use shard_manager::request::{BatchOp, BatchResult};
use shard_manager::{ShardManager, ShardManagerOptions};
use std::sync::Arc;
use storage::row::ColValue;
use storage::schema::{ColType, Column, TableSchema};

/// (id I64 pk, name Str iid0, score F64 iid1)
fn demo_schema() -> TableSchema {
    TableSchema::new(
        vec![
            Column { name: "id".into(), ty: ColType::I64, nullable: false, default: None },
            Column { name: "name".into(), ty: ColType::Str, nullable: false, default: None },
            Column { name: "score".into(), ty: ColType::F64, nullable: true, default: None },
        ],
        0,
        &[1, 2],
        &[], &[], &[], &[])
    .unwrap()
}

fn vals(id: i64, name: &str, score: Option<f64>) -> Vec<ColValue> {
    vec![
        ColValue::I64(id),
        ColValue::Bytes(name.as_bytes().to_vec()),
        score.map(ColValue::F64).unwrap_or(ColValue::Null),
    ]
}

fn pk(id: i64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

fn row_put_op(db: &Arc<str>, table: &Arc<str>, id: i64, name: &str, score: Option<f64>) -> BatchOp {
    BatchOp::RowPut {
        db: db.clone(),
        table: table.clone(),
        pk: pk(id),
        values: vals(id, name, score),
    }
}

#[test]
fn sql_index_broadcast_scan_multi_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions::new(4, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("open");
    mgr.create_db("default").expect("create db");
    mgr.create_table("default", "t").expect("create table");
    mgr.set_table_schema("default", "t", &demo_schema()).expect("set schema");

    let (db, table): (Arc<str>, Arc<str>) = (Arc::from("default"), Arc::from("t"));

    // 100 行按 PK hash 分散 4 shard
    let ops: Vec<BatchOp> = (0..100)
        .map(|i| row_put_op(&db, &table, i, &format!("u{}", i % 7), Some(i as f64)))
        .collect();
    for r in mgr.batch_ops(&ops) {
        assert_eq!(r, BatchResult::PutOk);
    }

    // RowGet 点查 (单 shard)
    let r = mgr.batch_ops(&[BatchOp::RowGet { db: db.clone(), table: table.clone(), pk: pk(42) }]);
    let BatchResult::GetValue(Some(bytes)) = &r[0] else {
        panic!("RowGet 应命中: {:?}", r[0]);
    };
    let cols = storage::row::decode_row(&demo_schema(), bytes).unwrap();
    assert_eq!(cols[0], ColValue::I64(42));

    // 等值广播: name = u3 → id 3,10,...,94 共 15 条, 全局按 (val, pk) 序
    let eq = ColValue::Bytes(b"u3".to_vec());
    let hits = mgr
        .index_scan("default", "t", 0, Some(eq.clone()), Some(eq.clone()), 0, true)
        .expect("index_scan");
    let ids: Vec<Vec<u8>> = hits.iter().map(|(_, p, _)| p.clone()).collect();
    let expect: Vec<Vec<u8>> = (0..100).filter(|i| i % 7 == 3).map(pk).collect();
    assert_eq!(ids, expect, "跨 shard 聚合后全局有序且完整");
    // 回表数据正确 (row_bytes 可解码且 id 匹配 pk)
    for (_, p, row) in &hits {
        let cols = storage::row::decode_row(&demo_schema(), row).unwrap();
        let ColValue::I64(id) = cols[0] else { panic!() };
        assert_eq!(pk(id), *p);
    }

    // 范围广播: score ∈ [20, 29] → id 20..=29
    let hits = mgr
        .index_scan(
            "default", "t", 1,
            Some(ColValue::F64(20.0)), Some(ColValue::F64(29.0)),
            0, true,
        )
        .expect("range scan");
    assert_eq!(
        hits.iter().map(|(_, p, _)| p.clone()).collect::<Vec<_>>(),
        (20..=29).map(pk).collect::<Vec<_>>(),
        "范围按 score 升序"
    );

    // limit 下推: 全局 top-5 (每 shard 本地 limit + 聚合截断)
    let hits = mgr
        .index_scan("default", "t", 1, Some(ColValue::F64(50.0)), None, 5, false)
        .expect("limit scan");
    assert_eq!(
        hits.iter().map(|(_, p, _)| p.clone()).collect::<Vec<_>>(),
        (50..55).map(pk).collect::<Vec<_>>()
    );
    assert!(hits.iter().all(|(_, _, row)| row.is_empty()), "keys_only 不回表");

    // update: id 3 改名 u0 → u3 等值少一条, 旧值查不到
    let r = mgr.batch_ops(&[row_put_op(&db, &table, 3, "u0", Some(3.0))]);
    assert_eq!(r[0], BatchResult::PutOk);
    let hits = mgr
        .index_scan("default", "t", 0, Some(eq.clone()), Some(eq.clone()), 0, false)
        .expect("scan after update");
    // 原 u3 集合 = {3,10,...,94} 共 14 条, update 走 1 条
    assert_eq!(hits.len(), 13);
    assert!(hits.iter().all(|(_, p, _)| *p != pk(3)));

    // delete: 行 + 索引同步消失
    let r = mgr.batch_ops(&[BatchOp::RowDelete { db: db.clone(), table: table.clone(), pk: pk(10) }]);
    assert_eq!(r[0], BatchResult::DeleteExisted(true));
    let hits = mgr
        .index_scan("default", "t", 0, Some(eq.clone()), Some(eq), 0, false)
        .expect("scan after delete");
    assert_eq!(hits.len(), 12, "u3: 14 - update(id3) - delete(id10)");

    mgr.close().expect("close");
}

#[test]
fn sql_rows_persist_across_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let opts = ShardManagerOptions::new(2, tmp.path().to_path_buf());
        let mgr = ShardManager::open(opts).expect("open");
        mgr.create_db("default").expect("create db");
        mgr.create_table("default", "t").expect("create table");
        mgr.set_table_schema("default", "t", &demo_schema()).expect("set schema");
        let (db, table): (Arc<str>, Arc<str>) = (Arc::from("default"), Arc::from("t"));
        let ops: Vec<BatchOp> = (0..40)
            .map(|i| row_put_op(&db, &table, i, &format!("u{}", i % 3), Some(i as f64)))
            .collect();
        for r in mgr.batch_ops(&ops) {
            assert_eq!(r, BatchResult::PutOk);
        }
        mgr.close().expect("close");
    }
    // reopen: schema ([$] 行) + row + 索引全部恢复
    let opts = ShardManagerOptions::new(2, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("reopen");
    let eq = ColValue::Bytes(b"u1".to_vec());
    let hits = mgr
        .index_scan("default", "t", 0, Some(eq.clone()), Some(eq), 0, true)
        .expect("scan after reopen");
    assert_eq!(
        hits.iter().map(|(_, p, _)| p.clone()).collect::<Vec<_>>(),
        (0..40).filter(|i| i % 3 == 1).map(pk).collect::<Vec<_>>()
    );
    mgr.close().expect("close");
}
