//! ShardManager e2e 测试.
//!
//! 注意: 跨 shard 测试**不能**走 ShardManager 自己 (会破坏 self.close 等),
//! 但基本能开 / put / get / delete / close 即可验证架构.

use shard_manager::{ShardManager, ShardManagerOptions};

#[test]
fn shard_manager_open_close() {
    // ⭐ 大栈测试: storage async fn 内联需要 64MB 栈, 跑在 cargo test 进程里
    // 用 RUST_MIN_STACK 环境变量启动 cargo test
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions::new(2, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("open");
    assert_eq!(mgr.num_shards(), 2);
    mgr.close().expect("close");
}

#[test]
fn shard_manager_put_get_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions::new(2, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("open");

    // ⭐ 测 db 在所有 shard 上创建 (跨 db 同步, 暂时走单 shard 创建)
    mgr.create_db("default").expect("create db default");
    mgr.create_table("default", "users").expect("create table");

    // put
    mgr.put("default", "users", b"alice", b"v1", 0)
        .expect("put alice");
    mgr.put("default", "users", b"bob", b"v2", 0).expect("put bob");

    // get
    let v = mgr.get("default", "users", b"alice", 0).expect("get alice");
    assert_eq!(v, Some(b"v1".to_vec()));
    let v = mgr.get("default", "users", b"bob", 0).expect("get bob");
    assert_eq!(v, Some(b"v2".to_vec()));

    // delete
    let existed = mgr.delete("default", "users", b"alice", 0).expect("delete");
    assert!(existed);
    let v = mgr
        .get("default", "users", b"alice", 0)
        .expect("get after delete");
    assert_eq!(v, None);

    mgr.close().expect("close");
}

#[test]
fn shard_manager_routing_consistency() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions::new(4, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("open");

    mgr.create_db("default").expect("create db");
    mgr.create_table("default", "users").expect("create table");

    // 同一 (db, table, key) 应该路由到同一 shard
    // 我们无法直接断言 shard_id, 但 put + get 应该 OK
    for i in 0..100 {
        let key = format!("key_{i}");
        mgr.put("default", "users", key.as_bytes(), b"v", 0)
            .expect("put");
        let v = mgr.get("default", "users", key.as_bytes(), 0).expect("get");
        assert_eq!(v, Some(b"v".to_vec()), "key {key} 不一致");
    }

    mgr.close().expect("close");
}

#[test]
fn shard_manager_cross_shard_distribution() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions::new(4, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("open");

    mgr.create_db("app").expect("create db app");
    mgr.create_table("app", "events").expect("create table");

    // 写 100 个 key, 应分布在多个 shard
    for i in 0..100 {
        let key = format!("event_{i}");
        mgr.put("app", "events", key.as_bytes(), b"data", 0)
            .expect("put");
    }

    // 读全部回, 应 OK
    for i in 0..100 {
        let key = format!("event_{i}");
        let v = mgr.get("app", "events", key.as_bytes(), 0).expect("get");
        assert_eq!(v, Some(b"data".to_vec()));
    }

    mgr.close().expect("close");
}

#[test]
fn shard_manager_close_timeout_safe() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions::new(2, tmp.path().to_path_buf());
    let mgr = ShardManager::open(opts).expect("open");
    // 立即 close (没操作过)
    mgr.close().expect("immediate close OK");
}

/// ⭐ 退出完整性: close 时必须把 nowchunks + WriteQueue pending + meta 全部落盘.
/// put 大 value 触发 chunk 满 swap (WriteQueue 路径), 不显式 flush 直接 close,
/// reopen 同一 block_root 后所有 key 必须命中.
#[test]
fn close_persists_pending_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let block_root = tmp.path().to_path_buf();

    // 2KB value × 800 keys ≈ 1.6MB, 足以触发 chunk 满 swap (单 chunk 1MB);
    // 注意 value 不能超过半页 (无 overflow page, 否则 split 无法进行)
    let big_val = vec![0xABu8; 2 * 1024];
    {
        let opts = ShardManagerOptions::new(2, block_root.clone());
        let mgr = ShardManager::open(opts).expect("open");
        mgr.create_db("app").expect("create db");
        mgr.create_table("app", "kv").expect("create table");
        for i in 0..800 {
            let key = format!("persist_{i:04}");
            mgr.put("app", "kv", key.as_bytes(), &big_val, 0)
                .expect("put");
        }
        // 不显式 flush_all — 依赖 close 的最终 flush
        mgr.close().expect("close");
    }

    // reopen 同一 block_root, recover 后全部 key 必须命中
    {
        let opts = ShardManagerOptions::new(2, block_root);
        let mgr = ShardManager::open(opts).expect("reopen");
        for i in 0..800 {
            let key = format!("persist_{i:04}");
            let v = mgr
                .get("app", "kv", key.as_bytes(), 0)
                .expect("get after reopen");
            assert_eq!(
                v.as_deref(),
                Some(big_val.as_slice()),
                "key {key} missing/corrupt after close+reopen"
            );
        }
        mgr.close().expect("close 2");
    }
}
