//! ⭐ N4: 数值类型原生存储 e2e (TAG_I64/F64 二进制, 非字符串).
//!
//! 通过 `batch_ops` 直连 API 拿 **raw stored bytes** 断言底层存储形态
//! (GetValue 返回原始 stored, 不经门面渲染) — 证明 INCR/INCRBYFLOAT
//! 写回的是 `[tag][LE bytes]` 而非十进制字符串.

use std::sync::Arc;

use shard_manager::request::{BatchOp, BatchResult};
use shard_manager::value_num::{TAG_F64, TAG_I64, TAG_RAW};
use shard_manager::{ShardManager, ShardManagerOptions};

fn db() -> Arc<str> {
    Arc::from("default")
}
fn tbl() -> Arc<str> {
    Arc::from("kv")
}

fn setup() -> (tempfile::TempDir, ShardManager) {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions::new(2, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("open");
    mgr.create_db("default").expect("create db");
    mgr.create_table("default", "kv").expect("create table");
    (tmp, mgr)
}

fn raw_get(mgr: &ShardManager, key: &[u8]) -> Option<Vec<u8>> {
    let r = mgr.batch_ops(&[BatchOp::Get {
        db: db(),
        table: tbl(),
        key: key.to_vec(),
    }]);
    match &r[0] {
        BatchResult::GetValue(v) => v.clone(),
        other => panic!("unexpected: {other:?}"),
    }
}

/// INCR 后底层 stored = [TAG_I64][8B LE] — 原生二进制, 非 "6" 字符串.
#[test]
fn incr_stores_native_i64_bytes() {
    let (_tmp, mgr) = setup();

    // SET "5" (RAW 文本) → INCR → 结果应升级为 TAG_I64 二进制
    let mut raw5 = vec![TAG_RAW];
    raw5.extend_from_slice(b"5");
    let r = mgr.batch_ops(&[BatchOp::Put {
        db: db(),
        table: tbl(),
        key: b"n".to_vec(),
        val: raw5,
    }]);
    assert_eq!(r[0], BatchResult::PutOk);

    let r = mgr.batch_ops(&[BatchOp::Incr {
        db: db(),
        table: tbl(),
        key: b"n".to_vec(),
        delta: 1,
    }]);
    assert_eq!(r[0], BatchResult::Integer(6));

    // ⭐ 断言底层字节: [0x02][6_i64 LE] = 9B, 不是 "6"
    let stored = raw_get(&mgr, b"n").expect("exists");
    assert_eq!(stored.len(), 9, "8B LE + 1B tag, got {stored:?}");
    assert_eq!(stored[0], TAG_I64);
    assert_eq!(i64::from_le_bytes(stored[1..9].try_into().unwrap()), 6);

    // 二进制上继续 INCR (纯 I64 路径)
    let r = mgr.batch_ops(&[BatchOp::Incr {
        db: db(),
        table: tbl(),
        key: b"n".to_vec(),
        delta: -10,
    }]);
    assert_eq!(r[0], BatchResult::Integer(-4));
    let stored = raw_get(&mgr, b"n").unwrap();
    assert_eq!(i64::from_le_bytes(stored[1..9].try_into().unwrap()), -4);

    mgr.close().expect("close");
}

/// INCRBYFLOAT: RAW 科学计数法文本 / I64 / F64 全路径 → TAG_F64 8B LE.
#[test]
fn incrbyfloat_stores_native_f64_bytes() {
    let (_tmp, mgr) = setup();

    // Redis 文档用例: SET "3.0e3" → INCRBYFLOAT 200 → 3200
    let mut raw = vec![TAG_RAW];
    raw.extend_from_slice(b"3.0e3");
    mgr.batch_ops(&[BatchOp::Put {
        db: db(),
        table: tbl(),
        key: b"f".to_vec(),
        val: raw,
    }]);
    let r = mgr.batch_ops(&[BatchOp::IncrFloat {
        db: db(),
        table: tbl(),
        key: b"f".to_vec(),
        delta: 200.0,
    }]);
    assert_eq!(r[0], BatchResult::Double(3200.0));

    let stored = raw_get(&mgr, b"f").unwrap();
    assert_eq!(stored.len(), 9);
    assert_eq!(stored[0], TAG_F64);
    assert_eq!(f64::from_le_bytes(stored[1..9].try_into().unwrap()), 3200.0);

    // I64 值上 INCRBYFLOAT → 提升 F64
    mgr.batch_ops(&[BatchOp::Incr {
        db: db(),
        table: tbl(),
        key: b"g".to_vec(),
        delta: 7,
    }]);
    let r = mgr.batch_ops(&[BatchOp::IncrFloat {
        db: db(),
        table: tbl(),
        key: b"g".to_vec(),
        delta: 0.5,
    }]);
    assert_eq!(r[0], BatchResult::Double(7.5));
    let stored = raw_get(&mgr, b"g").unwrap();
    assert_eq!(stored[0], TAG_F64);

    mgr.close().expect("close");
}

/// 类型语义: F64 上 INCR 报错 (Redis "not an integer"); INCR 溢出报错;
/// APPEND 到 I64 → 渲染字符串化 + 退回 RAW.
#[test]
fn typed_semantics_incr_errors_and_append_demotion() {
    let (_tmp, mgr) = setup();

    // F64 上 INCR → 报错
    mgr.batch_ops(&[BatchOp::IncrFloat {
        db: db(),
        table: tbl(),
        key: b"pi".to_vec(),
        delta: 3.25,
    }]);
    let r = mgr.batch_ops(&[BatchOp::Incr {
        db: db(),
        table: tbl(),
        key: b"pi".to_vec(),
        delta: 1,
    }]);
    assert!(matches!(&r[0], BatchResult::Error(e) if e.contains("not an integer")));

    // 溢出
    mgr.batch_ops(&[BatchOp::Incr {
        db: db(),
        table: tbl(),
        key: b"max".to_vec(),
        delta: i64::MAX,
    }]);
    let r = mgr.batch_ops(&[BatchOp::Incr {
        db: db(),
        table: tbl(),
        key: b"max".to_vec(),
        delta: 1,
    }]);
    assert!(matches!(&r[0], BatchResult::Error(e) if e.contains("overflow")));

    // APPEND 到 I64: 渲染 "42" 再拼 → RAW "42xx"
    mgr.batch_ops(&[BatchOp::Incr {
        db: db(),
        table: tbl(),
        key: b"a".to_vec(),
        delta: 42,
    }]);
    let r = mgr.batch_ops(&[BatchOp::Append {
        db: db(),
        table: tbl(),
        key: b"a".to_vec(),
        suffix: b"xx".to_vec(),
    }]);
    assert_eq!(r[0], BatchResult::Integer(4)); // "42xx"
    let stored = raw_get(&mgr, b"a").unwrap();
    assert_eq!(stored[0], TAG_RAW);
    assert_eq!(&stored[1..], b"42xx");

    mgr.close().expect("close");
}

/// 重启持久化: 二进制数值 tag 落盘 → reopen → 继续 RMW 正确.
#[test]
fn typed_values_survive_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let opts = ShardManagerOptions::new(2, tmp.path().to_path_buf());
        let mgr = ShardManager::open(opts).expect("open");
        mgr.create_db("default").expect("create db");
        mgr.create_table("default", "kv").expect("create table");
        mgr.batch_ops(&[BatchOp::Incr {
            db: db(),
            table: tbl(),
            key: b"persist".to_vec(),
            delta: 123,
        }]);
        mgr.close().expect("close");
    }
    let opts = ShardManagerOptions::new(2, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("reopen");
    let stored = raw_get(&mgr, b"persist").expect("survives");
    assert_eq!(stored[0], TAG_I64);
    assert_eq!(i64::from_le_bytes(stored[1..9].try_into().unwrap()), 123);
    // reopen 后继续 RMW
    let r = mgr.batch_ops(&[BatchOp::Incr {
        db: db(),
        table: tbl(),
        key: b"persist".to_vec(),
        delta: 1,
    }]);
    assert_eq!(r[0], BatchResult::Integer(124));
    mgr.close().expect("close");
}
