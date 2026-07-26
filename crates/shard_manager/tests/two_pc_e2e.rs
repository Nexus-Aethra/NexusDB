//! 2PC 跨 shard 协调 e2e 测试.
//!
//! ## 测什么
//!
//! 验证 `ShardManager::create_db` / `create_table` 走 2PC 后:
//! 1. **正常路径**: 所有 shard 都生效 (跨 shard 一致性)
//! 2. **回滚路径**: 任一 shard 失败, 全部回滚
//! 3. **持久化**: 关闭重开后状态保持
//!
//! ## 验证手段
//!
//! 由于 ShardManager 没有 list_dbs 公开 API, 我们通过:
//! - `route` API 知道 key 去哪个 shard
//! - 在每个 shard 上 `put` 验证 db 存在 (put 失败表示 db 不存在)
//! - 文件系统验证 (`{block_root}/shard_{N}/` 下目录)

use shard_manager::{ShardManager, ShardManagerOptions};

/// ⭐ 2PC 测试 1: 正常路径, create_db 在所有 shard 上生效.
#[test]
fn two_pc_create_db_visible_on_all_shards() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions::new(4, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("open");

    mgr.create_db("app").expect("create_db app");
    mgr.create_table("app", "users")
        .expect("create_table users");

    // 验证: 路由到任意 shard 都能 put (put 内部会 check db + table 存在)
    for i in 0..20 {
        let key = format!("k_{i}");
        let shard = mgr.route("app", "users", key.as_bytes());
        // put 应 OK (任何 shard 都能找到 "app" db + "users" table)
        mgr.put("app", "users", key.as_bytes(), b"v", 0)
            .unwrap_or_else(|e| panic!("put on shard {shard} (key={key}) failed: {e:?}"));
    }

    mgr.close().expect("close");
}

/// ⭐ 2PC 测试 2: 正常路径, create_table 在所有 shard 上生效.
#[test]
fn two_pc_create_table_visible_on_all_shards() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions::new(4, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("open");

    mgr.create_db("app").expect("create_db");
    mgr.create_table("app", "events").expect("create_table");

    // 任意 shard put/get 都应成功
    for i in 0..20 {
        let key = format!("event_{i}");
        mgr.put("app", "events", key.as_bytes(), b"v", 0).expect("put");
        let v = mgr.get("app", "events", key.as_bytes(), 0).expect("get");
        assert_eq!(v, Some(b"v".to_vec()));
    }

    mgr.close().expect("close");
}

/// ⭐ 2PC 测试 3: 持久化 — create_db 关闭重开后, 跨 shard 状态保持.
#[test]
fn two_pc_create_db_persists_across_reopen() {
    let tmp = tempfile::tempdir().unwrap();

    // 第一次 open, create_db
    {
        let opts = ShardManagerOptions::new(4, tmp.path().to_path_buf());
        let mgr = ShardManager::open(opts).expect("open 1");
        mgr.create_db("app").expect("create_db");
        mgr.create_table("app", "users").expect("create_table");
        mgr.put("app", "users", b"alice", b"v1", 0).expect("put");
        mgr.close().expect("close 1");
    }

    // 第二次 open, 验证 db + table + data 都在
    {
        let opts = ShardManagerOptions::new(4, tmp.path().to_path_buf());
        let mgr = ShardManager::open(opts).expect("open 2");
        let v = mgr.get("app", "users", b"alice", 0).expect("get after reopen");
        assert_eq!(v, Some(b"v1".to_vec()), "data persists after reopen");
        mgr.close().expect("close 2");
    }
}

/// ⭐ 2PC 测试 4: 重复 create_db 失败回滚 — 第二次应 PrepareFailed, 第一次的 db 仍存在.
#[test]
fn two_pc_create_db_duplicate_triggers_abort() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions::new(4, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("open");

    // 第一次 create_db 成功
    mgr.create_db("app")
        .expect("first create_db should succeed");
    mgr.create_table("app", "users").expect("create_table");

    // 第二次 create_db 失败 (storage 层会报 "db already exists")
    let result = mgr.create_db("app");
    assert!(
        result.is_err(),
        "duplicate create_db should fail, got: {result:?}"
    );

    // 验证: db + table 仍存在 (第一次 Prepare 已 Commit, 第二次失败未影响)
    mgr.put("app", "users", b"k", b"v", 0).expect("put after dup");

    mgr.close().expect("close");
}

/// ⭐ 2PC 测试 5: 跨 shard 路由 + 2PC 元数据 — put 在 4 个 shard 间分布, 但 create_table 已
/// 同步到所有 shard, 所以任何 put 都能成功.
#[test]
fn two_pc_metadata_with_cross_shard_routing() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions::new(4, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("open");

    mgr.create_db("db1").expect("create_db 1");
    mgr.create_table("db1", "t1").expect("create_table 1");
    mgr.create_db("db2").expect("create_db 2");
    mgr.create_table("db2", "t2").expect("create_table 2");

    // 跨 shard 分布 key
    let mut hits = [0usize; 4];
    for i in 0..40 {
        let key = format!("k_{i}");
        let s = mgr.route("db1", "t1", key.as_bytes());
        hits[s] += 1;
        mgr.put("db1", "t1", key.as_bytes(), b"v1", 0)
            .expect("put db1");
        mgr.put("db2", "t2", key.as_bytes(), b"v2", 0)
            .expect("put db2");
    }
    // 至少 2 个 shard 分到 key (4 个 shard 上 40 个 key)
    let active = hits.iter().filter(|&&h| h > 0).count();
    assert!(active >= 2, "4 shards should spread, got {hits:?}");

    // 读回验证
    for i in 0..40 {
        let key = format!("k_{i}");
        let v1 = mgr.get("db1", "t1", key.as_bytes(), 0).expect("get 1");
        let v2 = mgr.get("db2", "t2", key.as_bytes(), 0).expect("get 2");
        assert_eq!(v1, Some(b"v1".to_vec()));
        assert_eq!(v2, Some(b"v2".to_vec()));
    }

    mgr.close().expect("close");
}

/// ⭐ 2PC 测试 6: 文件系统验证 — 每个 shard 目录都有独立 storage state
/// (说明每个 shard 独立 open, 元数据已同步).
///
/// **路径结构**: `{block_root}/shard_{N}/{db_name=default}/shard_{N}/{page.mate, *.block}`.
#[test]
fn two_pc_each_shard_has_independent_state() {
    let tmp = tempfile::tempdir().unwrap();
    let block_root = tmp.path().to_path_buf();
    let opts = ShardManagerOptions::new(3, block_root.clone());
    let mgr = ShardManager::open(opts).expect("open");

    mgr.create_db("test").expect("create_db");
    mgr.create_table("test", "tbl").expect("create_table");

    mgr.close().expect("close");

    // 验证: 3 个 shard 目录都存在
    for shard_id in 0..3 {
        // ShardManager 层: block_root/shard_{N}/
        let shard_root = block_root.join(format!("shard_{shard_id}"));
        assert!(
            shard_root.exists(),
            "shard_root {shard_root:?} should exist"
        );

        // Storage 层: shard_root/default/shard_{N}/
        let storage_dir = shard_root.join("default").join(format!("shard_{shard_id}"));
        assert!(
            storage_dir.exists(),
            "storage_dir {storage_dir:?} should exist"
        );
        assert!(
            storage_dir.join("page.mate").exists(),
            "{storage_dir:?}/page.mate should exist"
        );
        assert!(
            storage_dir.join("000001.block").exists(),
            "{storage_dir:?}/000001.block should exist"
        );
    }
}

/// ⭐ 2PC 测试 7: 多 db + 多 table, 全部 create_db 走 2PC 后能正常使用.
#[test]
fn two_pc_multiple_dbs_and_tables() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions::new(2, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("open");

    // 创建多个 db + table
    for db_idx in 0..3 {
        let db_name = format!("db_{db_idx}");
        mgr.create_db(&db_name).expect("create_db");
        for table_idx in 0..2 {
            let table_name = format!("t_{table_idx}");
            mgr.create_table(&db_name, &table_name)
                .expect("create_table");
        }
    }

    // 每个 db 写自己的 key
    for db_idx in 0..3 {
        let db_name = format!("db_{db_idx}");
        for table_idx in 0..2 {
            let table_name = format!("t_{table_idx}");
            let key = format!("key_{db_idx}_{table_idx}");
            let val = format!("val_{db_idx}_{table_idx}");
            mgr.put(&db_name, &table_name, key.as_bytes(), val.as_bytes(), 0)
                .expect("put");
            let got = mgr.get(&db_name, &table_name, key.as_bytes(), 0).expect("get");
            assert_eq!(got, Some(val.into_bytes()));
        }
    }

    mgr.close().expect("close");
}

/// ⭐ 2PC 测试 8: 验证错误路径 — get 不存在的 db / table 报错.
/// 这测的是错误响应透传到 ShardManager.
#[test]
fn two_pc_error_propagation_via_get() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions::new(2, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("open");

    // 1. 不存在的 db → get 应 DbNotFound
    let r = mgr.get("nonexistent", "t", b"k", 0);
    assert!(r.is_err(), "get on non-existent db should err, got {r:?}");

    // 2. 不存在的 table → get 应 TableNotFound
    mgr.create_db("real").expect("create_db");
    let r = mgr.get("real", "nonexistent", b"k", 0);
    assert!(r.is_err(), "get on non-existent table should err");

    mgr.close().expect("close");
}
