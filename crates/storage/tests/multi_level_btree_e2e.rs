//! T15 多层 BTree 端到端测试.
//!
//! 验证 Table BTree 升级到多层后:
//! - 单 page 兼容 (小数据量, root = leaf, 零 split)
//! - 触发 leaf split (~500 KV)
//! - 触发 internal split (~5000 KV)
//! - 触发 root split (树高 +1, ~10000 KV)
//! - reopen 后多层 tree 数据完整
//! - delete / update 走多层 BTree
//! - TableDirectory 升级后能容纳大量 table (跨多层 BTree)

use storage::{IoBackend, IoBackendConfig};
use storage::OpenOptions;
use storage::StorageEngine;

mod common;

use common::run_async;

// =====================================================================
// 测试 helper
// =====================================================================

fn setup() -> (tempfile::TempDir, OpenOptions) {
    let tmp = tempfile::tempdir().unwrap();
    let opts = OpenOptions {
        block_root: tmp.path().to_path_buf(),
        block_dir: Some(tmp.path().to_path_buf()),
        db_name: None,
        shard_id: 0,
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 8, // 大一些, 减少 flush 干扰
        wal_mode: Default::default(),
    };
    (tmp, opts)
}

// =====================================================================
// 1. Table BTree 兼容: 小数据量, root = leaf, 零 split
// =====================================================================

#[test]
fn t15_table_btree_small_data_single_leaf() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.unwrap();
        e.create_db("db").await.unwrap();
        e.create_table("db", "t").await.unwrap();

        // 写 50 KV, 完全不触发 split
        for i in 0..50u32 {
            let key = format!("k{:04}", i);
            let val = format!("v{:04}", i);
            e.table_put("db", "t", key.as_bytes(), val.as_bytes())
                .await
                .unwrap();
        }
        // 读回
        for i in 0..50u32 {
            let key = format!("k{:04}", i);
            let val = format!("v{:04}", i);
            let got = e.table_get("db", "t", key.as_bytes()).await.unwrap();
            assert_eq!(got, Some(val.into_bytes()), "key {} 读错", i);
        }
    });
}

// =====================================================================
// 2. Table BTree 触发 leaf split: ~500 KV 必触发 1 次 split
// =====================================================================

#[test]
fn t15_table_btree_triggers_leaf_split() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.unwrap();
        e.create_db("db").await.unwrap();
        e.create_table("db", "t").await.unwrap();

        for i in 0..500u32 {
            let key = format!("k{:05}", i);
            let val = format!("v{:05}", i);
            e.table_put("db", "t", key.as_bytes(), val.as_bytes())
                .await
                .unwrap();
        }
        // 读回所有
        for i in 0..500u32 {
            let key = format!("k{:05}", i);
            let val = format!("v{:05}", i);
            let got = e.table_get("db", "t", key.as_bytes()).await.unwrap();
            assert_eq!(got, Some(val.into_bytes()), "key {} 读错", i);
        }
    });
}

// =====================================================================
// 3. Table BTree 触发 internal split: ~5000 KV 必触发 internal + root split
// =====================================================================

#[test]
fn t15_table_btree_triggers_internal_and_root_split() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.unwrap();
        e.create_db("db").await.unwrap();
        e.create_table("db", "t").await.unwrap();

        for i in 0..5000u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            e.table_put("db", "t", key.as_bytes(), val.as_bytes())
                .await
                .unwrap();
        }
        // 抽样读回 (每 50 个抽一个, 减少测试时间)
        for i in (0..5000u32).step_by(50) {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            let got = e.table_get("db", "t", key.as_bytes()).await.unwrap();
            assert_eq!(got, Some(val.into_bytes()), "key {} 读错", i);
        }
        // 边界: 第一个 + 最后一个
        assert_eq!(
            e.table_get("db", "t", b"k000000").await.unwrap(),
            Some(b"v000000".to_vec())
        );
        assert_eq!(
            e.table_get("db", "t", b"k004999").await.unwrap(),
            Some(b"v004999".to_vec())
        );
    });
}

// =====================================================================
// 4. reopen 后多层 tree 数据完整 (持久化 + recover 跨多层)
// =====================================================================

#[test]
fn t15_table_btree_reopen_after_split() {
    run_async(async move {
        let (_tmp, opts) = setup();
        {
            let mut e = StorageEngine::open(opts.clone()).await.unwrap();
            e.create_db("db").await.unwrap();
            e.create_table("db", "t").await.unwrap();
            for i in 0..3000u32 {
                let key = format!("k{:05}", i);
                let val = format!("v{:05}", i);
                e.table_put("db", "t", key.as_bytes(), val.as_bytes())
                    .await
                    .unwrap();
            }
            e.flush().await.unwrap();
            // 抽查: 写入未 reopen 时能否查到
            assert_eq!(
                e.table_get("db", "t", b"k00000").await.unwrap(),
                Some(b"v00000".to_vec())
            );
            assert_eq!(
                e.table_get("db", "t", b"k00700").await.unwrap(),
                Some(b"v00700".to_vec())
            );
        }
        // reopen
        let mut opts2 = opts;
        opts2.create_if_missing = false;
        let mut e2 = StorageEngine::open(opts2).await.unwrap();
        // 抽样验证
        for i in (0..3000u32).step_by(100) {
            let key = format!("k{:05}", i);
            let val = format!("v{:05}", i);
            let got = e2.table_get("db", "t", key.as_bytes()).await.unwrap();
            assert_eq!(got, Some(val.into_bytes()), "reopen 后 key {} 读错", i);
        }
        // 边界
        assert_eq!(
            e2.table_get("db", "t", b"k00000").await.unwrap(),
            Some(b"v00000".to_vec())
        );
        assert_eq!(
            e2.table_get("db", "t", b"k02999").await.unwrap(),
            Some(b"v02999".to_vec())
        );
        drop(_tmp);
    });
}

// =====================================================================
// 5. delete 走多层 BTree
// =====================================================================

#[test]
fn t15_table_btree_delete_across_multi_level() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.unwrap();
        e.create_db("db").await.unwrap();
        e.create_table("db", "t").await.unwrap();
        // 写 2000 条触发 split
        for i in 0..2000u32 {
            let key = format!("k{:05}", i);
            let val = format!("v{:05}", i);
            e.table_put("db", "t", key.as_bytes(), val.as_bytes())
                .await
                .unwrap();
        }
        // 删中间几个
        for &del_idx in &[100u32, 500, 1000, 1500, 1999] {
            let key = format!("k{:05}", del_idx);
            let existed = e.table_delete("db", "t", key.as_bytes()).await.unwrap();
            assert!(existed, "key {} 应存在", del_idx);
        }
        // 验证删除
        for &del_idx in &[100u32, 500, 1000, 1500, 1999] {
            let key = format!("k{:05}", del_idx);
            assert_eq!(
                e.table_get("db", "t", key.as_bytes()).await.unwrap(),
                None,
                "key {} 应被删",
                del_idx
            );
        }
        // 验证邻居仍在
        for &idx in &[99u32, 101, 499, 501, 999, 1001, 1499, 1501, 1998] {
            let key = format!("k{:05}", idx);
            let val = format!("v{:05}", idx);
            let got = e.table_get("db", "t", key.as_bytes()).await.unwrap();
            assert_eq!(got, Some(val.into_bytes()), "邻居 key {} 应仍在", idx);
        }
    });
}

// =====================================================================
// 6. update 走多层 BTree (kv 替换)
// =====================================================================

#[test]
fn t15_table_btree_update_across_multi_level() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.unwrap();
        e.create_db("db").await.unwrap();
        e.create_table("db", "t").await.unwrap();
        for i in 0..2000u32 {
            let key = format!("k{:05}", i);
            let val = format!("v{:05}", i);
            e.table_put("db", "t", key.as_bytes(), val.as_bytes())
                .await
                .unwrap();
        }
        // 更新 100 个 key
        for &upd_idx in &[0u32, 100, 500, 1000, 1500, 1999] {
            let key = format!("k{:05}", upd_idx);
            let new_val = format!("new_v{:05}", upd_idx);
            e.table_put("db", "t", key.as_bytes(), new_val.as_bytes())
                .await
                .unwrap();
        }
        // 验证更新
        for &upd_idx in &[0u32, 100, 500, 1000, 1500, 1999] {
            let key = format!("k{:05}", upd_idx);
            let new_val = format!("new_v{:05}", upd_idx);
            assert_eq!(
                e.table_get("db", "t", key.as_bytes()).await.unwrap(),
                Some(new_val.into_bytes()),
                "key {} 应是 new_val",
                upd_idx
            );
        }
        // 验证未更新的仍为原值
        for &idx in &[1u32, 50, 200, 800, 1234] {
            let key = format!("k{:05}", idx);
            let val = format!("v{:05}", idx);
            assert_eq!(
                e.table_get("db", "t", key.as_bytes()).await.unwrap(),
                Some(val.into_bytes()),
                "未更新的 key {} 应为原 val",
                idx
            );
        }
    });
}

// =====================================================================
// 7. TableDirectory 升级: 大量 table 跨多层
// =====================================================================

#[test]
fn t15_table_directory_many_tables() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.unwrap();
        e.create_db("db").await.unwrap();
        // 创建 300 个 table (>200 单 leaf 容量)
        for i in 0..300u32 {
            let tname = format!("table_{:04}", i);
            e.create_table("db", &tname).await.unwrap();
        }
        // 列出: 应按 name 升序, 全部 300 个
        let tables = e.list_tables("db").unwrap();
        assert_eq!(
            tables.len(),
            300,
            "应列出 300 个 table, 实际 {}",
            tables.len()
        );
        for (idx, name) in tables.iter().enumerate() {
            let expected = format!("table_{:04}", idx);
            assert_eq!(name, &expected, "第 {} 个 table 应为 {}", idx, expected);
        }
        // 删一些 + 再 list
        e.drop_table("db", "table_0050").await.unwrap();
        e.drop_table("db", "table_0150").await.unwrap();
        e.drop_table("db", "table_0250").await.unwrap();
        let tables2 = e.list_tables("db").unwrap();
        assert_eq!(tables2.len(), 297, "删 3 个后应剩 297");
        assert!(!tables2.contains(&"table_0050".to_string()));
        assert!(!tables2.contains(&"table_0150".to_string()));
        assert!(!tables2.contains(&"table_0250".to_string()));
    });
}

// =====================================================================
// 8. 跨多层 BTree reopen 后 TableDirectory 数据完整
// =====================================================================

#[test]
fn t15_table_directory_reopen_after_split() {
    run_async(async move {
        let (_tmp, opts) = setup();
        {
            let mut e = StorageEngine::open(opts.clone()).await.unwrap();
            e.create_db("db").await.unwrap();
            for i in 0..250u32 {
                let tname = format!("table_{:04}", i);
                e.create_table("db", &tname).await.unwrap();
            }
            e.flush().await.unwrap();
        }
        // reopen
        let mut opts2 = opts;
        opts2.create_if_missing = false;
        let mut e2 = StorageEngine::open(opts2).await.unwrap();
        let tables = e2.list_tables("db").unwrap();
        assert_eq!(tables.len(), 250);
        // 能 open_table
        let vpid = e2.open_table("db", "table_0123").await.unwrap();
        assert!(vpid.is_some(), "table_0123 应可查到");
    });
}
