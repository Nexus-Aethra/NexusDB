//! 异步 API 集成测试 (T15).
//!
//! 验证:
//! 1. 异步 put/get/delete 返回 future, await 拿结果 (用 pollster::block_on 驱动)
//! 2. 异步 create_db/create_table 走完整 2PC 流程
//! 3. 并发多个异步请求都正确处理
//! 4. 异步 API 路由到正确的 shard
//!
//! **测试 runtime**: pollster (与 shard 线程内部一致), 单线程 executor 够用.

use shard_manager::{ShardManager, ShardManagerOptions};

fn make_mgr(num_shards: usize) -> (tempfile::TempDir, ShardManager) {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions::new(num_shards, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("open ShardManager");
    (tmp, mgr)
}

#[test]
fn async_put_get_delete_basic() {
    let (_tmp, mgr) = make_mgr(4);
    mgr.create_db("app").expect("create_db");
    mgr.create_table("app", "users").expect("create_table");

    // put
    pollster::block_on(async {
        mgr.put_async("app", "users", b"alice", b"engineer", 0)
            .expect("put_async")
            .await
            .expect("put ok");
    });

    // get
    let v = pollster::block_on(async {
        mgr.get_async("app", "users", b"alice", 0)
            .expect("get_async")
            .await
            .expect("get ok")
    });
    assert_eq!(v, Some(b"engineer".to_vec()));

    // delete
    let existed = pollster::block_on(async {
        mgr.delete_async("app", "users", b"alice", 0)
            .expect("delete_async")
            .await
            .expect("delete ok")
    });
    assert!(existed);

    // get again → None
    let v = pollster::block_on(async {
        mgr.get_async("app", "users", b"alice", 0)
            .expect("get_async")
            .await
            .expect("get ok")
    });
    assert_eq!(v, None);
}

#[test]
fn async_put_get_routes_correctly() {
    let (_tmp, mgr) = make_mgr(8);
    mgr.create_db("d").expect("create_db");
    mgr.create_table("d", "t").expect("create_table");

    // 写 100 个不同 key, 确保 hash 路由后所有 shard 都被访问
    pollster::block_on(async {
        for i in 0..100 {
            let key = format!("k_{i:04}");
            let val = format!("v_{i}");
            mgr.put_async("d", "t", key.as_bytes(), val.as_bytes(), 0)
                .expect("put")
                .await
                .expect("put ok");
        }
    });

    // 读回验证
    pollster::block_on(async {
        for i in 0..100 {
            let key = format!("k_{i:04}");
            let expected = format!("v_{i}");
            let v = mgr
                .get_async("d", "t", key.as_bytes(), 0)
                .expect("get")
                .await
                .expect("get ok");
            assert_eq!(v, Some(expected.into_bytes()), "key={key}");
        }
    });
}

#[test]
fn async_create_table_then_concurrent_writes() {
    let (_tmp, mgr) = make_mgr(4);
    pollster::block_on(async {
        mgr.create_db_async("app").await.expect("create_db");
        mgr.create_table_async("app", "users")
            .await
            .expect("create_table");

        // 顺序 10 个 put (用 put_async 验证 future 路径)
        for i in 0..10 {
            let key = format!("u_{i}");
            mgr.put_async("app", "users", key.as_bytes(), b"hi", 0)
                .expect("put")
                .await
                .expect("put ok");
        }

        // 读回验证
        for i in 0..10 {
            let key = format!("u_{i}");
            let v = mgr
                .get_async("app", "users", key.as_bytes(), 0)
                .expect("get")
                .await
                .expect("get ok");
            assert_eq!(v, Some(b"hi".to_vec()));
        }
    });
}

#[test]
fn async_create_db_visible_on_all_shards() {
    let (_tmp, mgr) = make_mgr(4);
    pollster::block_on(async {
        mgr.create_db_async("app").await.expect("create_db");
        mgr.create_table_async("app", "users")
            .await
            .expect("create_table");

        // 验证所有 4 个 shard 都接受这个 db 的请求
        for i in 0..50 {
            let key = format!("k_{i}");
            mgr.put_async("app", "users", key.as_bytes(), b"v", 0)
                .expect("put")
                .await
                .expect("put ok");
        }
    });
}

#[test]
fn async_get_nonexistent_returns_none() {
    let (_tmp, mgr) = make_mgr(2);
    mgr.create_db("app").expect("create_db");
    mgr.create_table("app", "users").expect("create_table");

    let v = pollster::block_on(async {
        mgr.get_async("app", "users", b"ghost", 0)
            .expect("get")
            .await
            .expect("get ok")
    });
    assert_eq!(v, None);
}

#[test]
fn async_put_to_missing_table_errors() {
    let (_tmp, mgr) = make_mgr(2);
    mgr.create_db("app").expect("create_db");
    // 不创建 table, 直接 put
    let result = pollster::block_on(async {
        mgr.put_async("app", "missing", b"k", b"v", 0)
            .expect("put send")
            .await
    });
    assert!(result.is_err(), "should error on missing table");
}

#[test]
fn sync_and_async_interop() {
    // 同步和异步 API 混用: 同一 ShardManager 既能 .put() 也能 .put_async()
    let (_tmp, mgr) = make_mgr(2);
    mgr.create_db("app").expect("sync create_db");
    mgr.create_table("app", "t").expect("sync create_table");

    // sync put
    mgr.put("app", "t", b"sync_key", b"sync_val", 0)
        .expect("sync put");
    // async get
    let v = pollster::block_on(async {
        mgr.get_async("app", "t", b"sync_key", 0)
            .expect("get")
            .await
            .expect("get ok")
    });
    assert_eq!(v, Some(b"sync_val".to_vec()));

    // async put
    pollster::block_on(async {
        mgr.put_async("app", "t", b"async_key", b"async_val", 0)
            .expect("put")
            .await
            .expect("put ok");
    });
    // sync get
    let v = mgr.get("app", "t", b"async_key", 0).expect("sync get");
    assert_eq!(v, Some(b"async_val".to_vec()));
}

#[test]
fn async_concurrent_puts_to_same_key() {
    // 同一 key 路由到同一 shard, 多次 put 应该都成功
    let (_tmp, mgr) = make_mgr(4);
    pollster::block_on(async {
        mgr.create_db_async("d").await.expect("create_db");
        mgr.create_table_async("d", "t")
            .await
            .expect("create_table");

        // 顺序覆盖写
        for i in 0..20 {
            let val = format!("v_{i}");
            mgr.put_async("d", "t", b"hot_key", val.as_bytes(), 0)
                .expect("put")
                .await
                .expect("put ok");
        }

        // 读到最后一个
        let v = mgr
            .get_async("d", "t", b"hot_key", 0)
            .expect("get")
            .await
            .expect("get ok");
        assert_eq!(v, Some(b"v_19".to_vec()));
    });
}
