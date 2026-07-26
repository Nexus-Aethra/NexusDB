//! T11 DbRegistry 端到端测试 (DESIGN §4.6 + plan §Task 11).
//!
//! 设计要点:
//! - 多 db / 多表: 通过 DbRegistry write-through 缓存管理
//! - MetaPage (db_name → table_dir_root_vpid) + TableDirectory (table_name → table_root_vpid) + table BTree
//! - 端到端 create_db → create_table → put → get → close → reopen → 验证
//! - 验证 db 隔离 (不同 db 的同名 table 不应混淆)
//! - 验证 drop_table / drop_db 持久化
//!
//! 详见 `docs/superpowers/plans/2026-07-18-storage-crate.md` §Task 11.

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
        block_root: tmp.path().to_path_buf().clone(),
        block_dir: Some(tmp.path().to_path_buf()),
        db_name: None,
        shard_id: 0,
        create_if_missing: true,
        chunk_cache_size: 4,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
    };
    (tmp, opts)
}

// =====================================================================
// 1. 单 db 单表基础: create_db → create_table → put/get
// =====================================================================

#[test]
fn registry_create_one_db_then_one_table_then_put_get() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        // 1. 创建一个 db
        e.create_db("analytics").await.expect("create db analytics");
        assert_eq!(e.db_count(), 1);
        assert_eq!(e.list_dbs(), vec!["analytics".to_string()]);

        // 2. 在 db 中创建一张表
        let events_vpid = e
            .create_table("analytics", "events")
            .await
            .expect("create table events");
        assert!(
            events_vpid >= 1,
            "vpid 应 > 0 (MetaPage 占用 0), got {}",
            events_vpid
        );

        // 3. put / get
        e.table_put("analytics", "events", b"page_view", b"view_count=42")
            .await
            .expect("put");
        let v = e
            .table_get("analytics", "events", b"page_view")
            .await
            .expect("get");
        assert_eq!(v, Some(b"view_count=42".to_vec()));

        // 4. 不存在的 key 返回 None
        let missing = e
            .table_get("analytics", "events", b"click")
            .await
            .expect("get missing");
        assert_eq!(missing, None);

        e.close().await.expect("close ok");
    });
}

// =====================================================================
// 2. 多 db 隔离: 同样表名在不同 db 互不影响
// =====================================================================

#[test]
fn registry_put_get_multiple_dbs_isolated() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        e.create_db("db1").await.expect("create db1");
        e.create_db("db2").await.expect("create db2");
        assert_eq!(e.db_count(), 2);

        // db1.users 与 db2.users 是不同 table
        e.create_table("db1", "users")
            .await
            .expect("create db1.users");
        e.create_table("db2", "users")
            .await
            .expect("create db2.users");

        // db1.users
        e.table_put("db1", "users", b"alice", b"db1-alice")
            .await
            .expect("put db1");
        // db2.users
        e.table_put("db2", "users", b"alice", b"db2-alice")
            .await
            .expect("put db2");

        // 验证隔离
        let v1 = e
            .table_get("db1", "users", b"alice")
            .await
            .expect("get db1");
        let v2 = e
            .table_get("db2", "users", b"alice")
            .await
            .expect("get db2");
        assert_eq!(v1, Some(b"db1-alice".to_vec()));
        assert_eq!(v2, Some(b"db2-alice".to_vec()));

        e.close().await.expect("close ok");
    });
}

// =====================================================================
// 3. 跨重启持久化: 全部 catalog 状态 (MetaPage + TableDirectory + table BTree)
// =====================================================================

#[test]
fn registry_two_dbs_persist_across_restart() {
    run_async(async move {
        let (_tmp, opts) = setup();

        // 第一次: 写完整 catalog + 数据
        let (db1_users_vpid, db2_posts_vpid) = {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open 1");
            e.create_db("shop").await.expect("create shop");
            e.create_db("blog").await.expect("create blog");
            e.create_table("shop", "users").await.expect("shop.users");
            let users_vpid = e
                .open_table("shop", "users")
                .await
                .expect("open shop.users")
                .expect("shop.users exists");
            e.create_table("blog", "posts").await.expect("blog.posts");
            let posts_vpid = e
                .open_table("blog", "posts")
                .await
                .expect("open blog.posts")
                .expect("blog.posts exists");
            e.table_put("shop", "users", b"u1", b"alice")
                .await
                .expect("put");
            e.table_put("blog", "posts", b"p1", b"hello")
                .await
                .expect("put");
            e.flush().await.expect("flush");
            e.close().await.expect("close 1");
            (users_vpid, posts_vpid)
        };

        // 第二次: 重启, 验证 catalog + 数据
        let mut e2 = StorageEngine::open(opts).await.expect("open 2");
        assert_eq!(e2.db_count(), 2);
        assert_eq!(e2.list_dbs(), vec!["blog".to_string(), "shop".to_string()]);

        // 验证 table vpid 一致
        let users_vpid2 = e2
            .open_table("shop", "users")
            .await
            .expect("open shop.users")
            .expect("shop.users exists");
        assert_eq!(users_vpid2, db1_users_vpid);
        let posts_vpid2 = e2
            .open_table("blog", "posts")
            .await
            .expect("open blog.posts")
            .expect("blog.posts exists");
        assert_eq!(posts_vpid2, db2_posts_vpid);

        // 验证数据
        assert_eq!(
            e2.table_get("shop", "users", b"u1").await.expect("get u1"),
            Some(b"alice".to_vec())
        );
        assert_eq!(
            e2.table_get("blog", "posts", b"p1").await.expect("get p1"),
            Some(b"hello".to_vec())
        );

        e2.close().await.expect("close 2");
    });
}

// =====================================================================
// 4. drop_table: catalog leaf 持久化反映
// =====================================================================

#[test]
fn registry_drop_table_removes_from_catalog() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        e.create_db("app").await.expect("create db app");
        e.create_table("app", "users").await.expect("create users");
        e.create_table("app", "orders")
            .await
            .expect("create orders");
        e.table_put("app", "users", b"u1", b"alice")
            .await
            .expect("put");
        e.table_put("app", "orders", b"o1", b"o_data")
            .await
            .expect("put");

        // 验证两个表都在
        let tables = e.list_tables("app").expect("list_tables");
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));

        // drop users
        let existed = e.drop_table("app", "users").await.expect("drop users");
        assert!(existed, "drop 应返回 true (users 存在)");

        // 验证 users 不在 catalog
        assert!(
            e.open_table("app", "users")
                .await
                .expect("open users")
                .is_none()
        );
        let tables_after = e.list_tables("app").expect("list_tables after");
        assert_eq!(tables_after, vec!["orders".to_string()]);

        // orders 数据不受影响
        assert_eq!(
            e.table_get("app", "orders", b"o1").await.expect("get o1"),
            Some(b"o_data".to_vec())
        );

        // 重复 drop 返回 false
        let existed2 = e
            .drop_table("app", "users")
            .await
            .expect("drop users again");
        assert!(!existed2, "重复 drop 应返回 false");

        e.close().await.expect("close ok");
    });
}

// =====================================================================
// 5. drop_db: 从 MetaPage 删除整个 db
// =====================================================================

#[test]
fn registry_drop_db_removes_from_meta_page() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        e.create_db("app1").await.expect("create app1");
        e.create_db("app2").await.expect("create app2");
        assert_eq!(e.db_count(), 2);

        e.drop_db("app1").await.expect("drop app1");
        assert_eq!(e.db_count(), 1);
        assert_eq!(e.list_dbs(), vec!["app2".to_string()]);

        // 访问已 drop 的 db 应报错
        let r = e.open_db("app1");
        assert!(r.is_err(), "open 已 drop 的 db 应返回 Err");

        // app2 仍可访问
        let _db2 = e.open_db("app2").expect("app2 仍可访问");

        e.close().await.expect("close ok");
    });
}

// =====================================================================
// 6. 重启后 drop 状态持久化
// =====================================================================

#[test]
fn registry_drop_table_persists_across_restart() {
    run_async(async move {
        let (_tmp, opts) = setup();

        // 第一次: drop 一张表后 close
        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open 1");
            e.create_db("test").await.expect("create test");
            e.create_table("test", "t1").await.expect("create t1");
            e.create_table("test", "t2").await.expect("create t2");
            e.drop_table("test", "t1").await.expect("drop t1");
            e.flush().await.expect("flush");
            e.close().await.expect("close 1");
        }

        // 第二次: 重启, 验证 t1 不在 catalog
        let mut e2 = StorageEngine::open(opts).await.expect("open 2");
        let tables = e2.list_tables("test").expect("list_tables");
        assert_eq!(tables, vec!["t2".to_string()]);
        assert!(
            e2.open_table("test", "t1")
                .await
                .expect("open t1")
                .is_none()
        );

        e2.close().await.expect("close 2");
    });
}

#[test]
fn registry_drop_db_persists_across_restart() {
    run_async(async move {
        let (_tmp, opts) = setup();

        // 第一次: drop 一个 db
        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open 1");
            e.create_db("keep").await.expect("create keep");
            e.create_db("drop_me").await.expect("create drop_me");
            e.drop_db("drop_me").await.expect("drop drop_me");
            e.flush().await.expect("flush");
            e.close().await.expect("close 1");
        }

        // 第二次: 重启, drop_me 应不在
        let mut e2 = StorageEngine::open(opts).await.expect("open 2");
        assert_eq!(e2.db_count(), 1);
        assert_eq!(e2.list_dbs(), vec!["keep".to_string()]);
        assert!(e2.open_db("drop_me").is_err());

        e2.close().await.expect("close 2");
    });
}

// =====================================================================
// 7. 重复 create 错误
// =====================================================================

#[test]
fn registry_create_duplicate_db_errors() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        e.create_db("app").await.expect("create app");
        let r = e.create_db("app");
        assert!(r.await.is_err(), "重复 create db 应返回 Err");
        e.close().await.expect("close ok");
    });
}

#[test]
fn registry_create_duplicate_table_errors() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        e.create_db("app").await.expect("create app");
        e.create_table("app", "users").await.expect("create users");
        let r = e.create_table("app", "users");
        assert!(r.await.is_err(), "重复 create table 应返回 Err");
        e.close().await.expect("close ok");
    });
}

// =====================================================================
// 8. open_db 错误: db 不存在
// =====================================================================

#[test]
fn registry_open_nonexistent_db_errors() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        let r = e.open_db("nonexistent");
        assert!(r.is_err(), "open 不存在的 db 应返回 Err");
        e.close().await.expect("close ok");
    });
}

// =====================================================================
// 9. 多个 db 多 table 后: 大量数据 put/get
// =====================================================================

#[test]
fn registry_many_keys_in_table_persist_across_restart() {
    run_async(async move {
        let (_tmp, opts) = setup();

        let n = 50;
        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open 1");
            e.create_db("data").await.expect("create data");
            e.create_table("data", "kv").await.expect("create kv");
            for i in 0..n {
                let k = format!("key_{:04}", i);
                let v = format!("value_{:04}", i);
                e.table_put("data", "kv", k.as_bytes(), v.as_bytes())
                    .await
                    .expect("put");
            }
            e.flush().await.expect("flush");
            e.close().await.expect("close 1");
        }

        // 验证
        let mut e2 = StorageEngine::open(opts).await.expect("open 2");
        assert_eq!(e2.db_count(), 1);
        for i in 0..n {
            let k = format!("key_{:04}", i);
            let expected = format!("value_{:04}", i);
            let v = e2.table_get("data", "kv", k.as_bytes()).await.expect("get");
            assert_eq!(v, Some(expected.into_bytes()), "key {} mismatch", k);
        }
        e2.close().await.expect("close 2");
    });
}

// =====================================================================
// 10. table_delete
// =====================================================================

#[test]
fn registry_table_delete_removes_key() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        e.create_db("app").await.expect("create app");
        e.create_table("app", "kv").await.expect("create kv");
        e.table_put("app", "kv", b"k1", b"v1")
            .await
            .expect("put k1");
        e.table_put("app", "kv", b"k2", b"v2")
            .await
            .expect("put k2");

        // 删 k1
        let existed = e.table_delete("app", "kv", b"k1").await.expect("delete k1");
        assert!(existed);

        // k1 不再存在
        assert_eq!(e.table_get("app", "kv", b"k1").await.expect("get k1"), None);
        // k2 仍存在
        assert_eq!(
            e.table_get("app", "kv", b"k2").await.expect("get k2"),
            Some(b"v2".to_vec())
        );

        // 删不存在的 key 返回 false
        let existed2 = e
            .table_delete("app", "kv", b"nonexistent")
            .await
            .expect("delete missing");
        assert!(!existed2);

        e.close().await.expect("close ok");
    });
}

// =====================================================================
// 11. 创建 db + table 后, 通过 DbHandle 直接操作
// =====================================================================

#[test]
fn registry_db_handle_direct_api() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        e.create_db("app").await.expect("create app");

        // 用 DbHandle 直接操作: split borrow 用 pager_and_db
        {
            let (pager, db) = e.pager_and_db("app").expect("open app");
            let v1 = db.create_table(pager, "t1").await.expect("t1");
            let v2 = db.create_table(pager, "t2").await.expect("t2");
            assert_ne!(v1, v2);
            assert_eq!(db.table_count(), 2);
            assert_eq!(db.list_tables(), vec!["t1".to_string(), "t2".to_string()]);
            assert_eq!(db.open_table(pager, "t1").await.expect("open t1"), Some(v1));
        }

        e.close().await.expect("close ok");
    });
}

// =====================================================================
// 12. close 隐式 flush 后, 跨重启 put/get 验证
// =====================================================================

#[test]
fn registry_close_flushes_catalog() {
    run_async(async move {
        let (_tmp, opts) = setup();

        // 第一次: 不显式 flush, close
        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open");
            e.create_db("app").await.expect("create");
            e.create_table("app", "t").await.expect("create table");
            e.table_put("app", "t", b"k", b"v").await.expect("put");
            e.close().await.expect("close (隐式 flush)");
        }

        // 重启
        let mut e2 = StorageEngine::open(opts).await.expect("reopen");
        assert_eq!(e2.db_count(), 1);
        assert_eq!(e2.list_dbs(), vec!["app".to_string()]);
        assert_eq!(
            e2.table_get("app", "t", b"k").await.expect("get"),
            Some(b"v".to_vec())
        );
        e2.close().await.expect("close 2");
    });
}
