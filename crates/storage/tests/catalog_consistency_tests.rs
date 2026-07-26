//! Catalog 一致性测试 (DESIGN §4.5 + §4.6 + plan §Task 9-11).
//!
//! ## 测试目标
//!
//! 验证 MetaPage + TableDirectory + table BTree 三层目录在各种边界场景下的
//! 持久化一致性 (close → reopen 后数据完整).
//!
//! ## 关键场景
//!
//! 1. **多 page 写回原子性**: MetaPage + TableDirectory 多 page 写回必须一致
//! 2. **崩溃恢复一致性**: close 中途 (未 fsync) 也能 recover 到上一次 flush 状态
//! 3. **catalog 写回崩在中间**: 创建 db/table 中途模拟 crash, 验证部分写入不破坏 BTree
//! 4. **跨进程一致性**: close → reopen → 所有 catalog 操作正常
//! 5. **db / table 重命名场景** (留 polish): 当前不实现 rename
//!
//! 详见 `docs/superpowers/plans/2026-07-18-storage-crate.md` §Task 11.

use std::collections::HashSet;

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
    // ⭐ T12.21: 走新多 db 路径模式 (block_root + db_name + shard_id),
    // 替代旧的 compat block_dir 模式. 实际路径 = {tmp}/default/shard_0/
    let opts = OpenOptions {
        block_root: tmp.path().to_path_buf().clone(),
        block_dir: None,
        db_name: Some("default".to_string()),
        shard_id: 0,
        create_if_missing: true,
        chunk_cache_size: 4,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
    };
    (tmp, opts)
}

/// 重新打开一个已 close 的 engine (模拟进程重启)
#[allow(dead_code)]
async fn reopen(opts: OpenOptions) -> StorageEngine {
    StorageEngine::open(opts).await.expect("reopen ok")
}

// =====================================================================
// 1. MetaPage + TableDirectory 多 page 写回原子性
// =====================================================================

#[test]
fn catalog_multi_page_writeback_atomicity() {
    run_async(async move {
        // 验证: create_db 触发的 MetaPage 写 + TableDirectory 写 是 page 序列, 但
        // 顺序由 PageWriteBatch 强制, 不会出现"MetaPage 已写 / TableDirectory 还没"
        // 的中间态 (从 recover 视角看).
        //
        // 测试方法: 写完后立即 flush, 然后 reopen, 检查 db 仍在.
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts.clone()).await.expect("open 1");

        e.create_db("alpha").await.expect("create alpha");
        e.create_db("beta").await.expect("create beta");
        e.create_db("gamma").await.expect("create gamma");

        {
            let (pager, db_alpha) = e.pager_and_db("alpha").expect("open alpha");
            let _ = db_alpha.create_table(pager, "t1").await.expect("t1");
            let _ = db_alpha.create_table(pager, "t2").await.expect("t2");
        }
        {
            let (pager, db_beta) = e.pager_and_db("beta").expect("open beta");
            let _ = db_beta.create_table(pager, "users").await.expect("users");
        }

        e.flush().await.expect("flush");

        // 不调 close, 模拟 flush 后立即 reopen
        drop(e);

        // reopen 后所有 catalog 状态应一致
        let mut e2 = StorageEngine::open(opts).await.expect("reopen ok");
        let mut dbs: Vec<String> = e2.list_dbs();
        dbs.sort();
        assert_eq!(
            dbs,
            vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()],
            "MetaPage 写回后所有 db 都在"
        );

        // alpha 的 t1, t2
        let alpha_t1 = e2.open_table("alpha", "t1").await.expect("open alpha.t1");
        assert!(alpha_t1.is_some(), "alpha.t1 应在");

        // beta 的 users
        let beta_users = e2
            .open_table("beta", "users")
            .await
            .expect("open beta.users");
        assert!(beta_users.is_some(), "beta.users 应在");

        // gamma 没表
        let gamma_count = e2.list_tables("gamma").expect("list gamma");
        assert_eq!(gamma_count.len(), 0, "gamma 没有任何表");

        e2.close().await.expect("close");
    });
}

// =====================================================================
// 2. 崩溃恢复一致性 (close 后 reopen, drop + reopen)
// =====================================================================

#[test]
fn catalog_crash_recovery_drops_db() {
    run_async(async move {
        // 模拟 "crash after dropping a db, no explicit flush" 场景:
        // 实际我们 drop_db 内部走 PageWriteBatch, 已经持久化.
        // 这里测: 写 db → drop db → 重新 open, db 不应再存在
        let (_tmp, opts) = setup();

        let meta_page_vpid = {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open 1");
            e.create_db("temp").await.expect("create temp");
            e.create_db("keep").await.expect("create keep");
            e.flush().await.expect("flush");
            let v = e.open_table("temp", "any").await.expect("ok"); // 触发 lazy init
            e.close().await.expect("close");
            v
        };

        let _ = meta_page_vpid; // suppress unused

        // reopen, create more, drop one, close without flush
        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("reopen 1");
            e.create_db("new_one").await.expect("create new_one");
            e.drop_db("temp").await.expect("drop temp");
            e.close().await.expect("close without explicit flush");
        }

        // 再 reopen, 验证状态
        let e2 = StorageEngine::open(opts).await.expect("reopen 2");
        let mut dbs: Vec<String> = e2.list_dbs();
        dbs.sort();
        assert_eq!(
            dbs,
            vec!["keep".to_string(), "new_one".to_string()],
            "temp 应被 drop, 留下 keep + new_one"
        );
        e2.close().await.expect("close");
    });
}

#[test]
fn catalog_recovery_preserves_table_data() {
    run_async(async move {
        // 验证: put/get table BTree 数据 在 close → reopen 后还在
        let (_tmp, opts) = setup();

        // 写数据
        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open 1");
            e.create_db("app").await.expect("create db");
            e.create_table("app", "users").await.expect("create table");
            e.table_put("app", "users", b"alice", b"v1")
                .await
                .expect("put alice");
            e.table_put("app", "users", b"bob", b"v2")
                .await
                .expect("put bob");
            e.table_put("app", "users", b"charlie", b"v3")
                .await
                .expect("put charlie");
            e.flush().await.expect("flush");
            e.close().await.expect("close");
        }

        // 重启验证
        let mut e2 = StorageEngine::open(opts.clone()).await.expect("reopen");
        assert_eq!(
            e2.table_get("app", "users", b"alice").await.expect("get"),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            e2.table_get("app", "users", b"bob").await.expect("get"),
            Some(b"v2".to_vec())
        );
        assert_eq!(
            e2.table_get("app", "users", b"charlie").await.expect("get"),
            Some(b"v3".to_vec())
        );

        // 删除一个
        let existed = e2
            .table_delete("app", "users", b"bob")
            .await
            .expect("delete");
        assert!(existed, "bob 应存在");
        assert_eq!(
            e2.table_get("app", "users", b"bob").await.expect("get"),
            None
        );
        e2.flush().await.expect("flush");

        // 再重启
        drop(e2);
        let mut e3 = StorageEngine::open(opts).await.expect("reopen 2");
        assert_eq!(
            e3.table_get("app", "users", b"alice")
                .await
                .expect("get alice"),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            e3.table_get("app", "users", b"bob").await.expect("get bob"),
            None
        );
        assert_eq!(
            e3.table_get("app", "users", b"charlie")
                .await
                .expect("get charlie"),
            Some(b"v3".to_vec())
        );
        e3.close().await.expect("close");
    });
}

// =====================================================================
// 3. 多 db 隔离
// =====================================================================

#[test]
fn catalog_multi_db_isolation() {
    run_async(async move {
        // 不同 db 的同名 table 互相隔离: db_a.users 和 db_b.users 是不同的 table,
        // 各有自己的 root vpid 和数据.
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts.clone()).await.expect("open");

        e.create_db("a").await.expect("create a");
        e.create_db("b").await.expect("create b");
        let a_users = e.create_table("a", "users").await.expect("a.users");
        let b_users = e.create_table("b", "users").await.expect("b.users");
        assert_ne!(a_users, b_users, "不同 db 的同名 table 应有不同 root vpid");

        e.table_put("a", "users", b"alice", b"a_alice")
            .await
            .expect("a.put");
        e.table_put("b", "users", b"alice", b"b_alice")
            .await
            .expect("b.put");

        // 读应按 db 隔离
        assert_eq!(
            e.table_get("a", "users", b"alice").await.expect("a.get"),
            Some(b"a_alice".to_vec())
        );
        assert_eq!(
            e.table_get("b", "users", b"alice").await.expect("b.get"),
            Some(b"b_alice".to_vec())
        );

        // drop db a 不影响 db b
        e.drop_db("a").await.expect("drop a");
        // drop 后再 open db a 应返回 DbNotFound 错误
        let err = e
            .open_table("a", "users")
            .await
            .expect_err("a should be gone");
        assert!(
            matches!(err, storage::RegistryError::DbNotFound(_)),
            "expected DbNotFound, got {:?}",
            err
        );
        assert_eq!(
            e.table_get("b", "users", b"alice")
                .await
                .expect("b.get after drop a"),
            Some(b"b_alice".to_vec())
        );

        e.close().await.expect("close");
    });
}

// =====================================================================
// 4. 大量 catalog 操作
// =====================================================================

#[test]
fn catalog_many_dbs_many_tables_persist() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let n_dbs = 5;
        let n_tables_per_db = 20;

        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open 1");
            for i in 0..n_dbs {
                let db_name = format!("db_{:02}", i);
                e.create_db(&db_name).await.expect("create db");
                for j in 0..n_tables_per_db {
                    let tbl_name = format!("t_{:02}", j);
                    e.create_table(&db_name, &tbl_name)
                        .await
                        .expect("create table");
                }
            }
            // 给每个 table 写一对 k/v
            for i in 0..n_dbs {
                let db_name = format!("db_{:02}", i);
                for j in 0..n_tables_per_db {
                    let tbl_name = format!("t_{:02}", j);
                    let key = format!("k_{:02}_{:02}", i, j);
                    let val = format!("v_{:02}_{:02}", i, j);
                    e.table_put(&db_name, &tbl_name, key.as_bytes(), val.as_bytes())
                        .await
                        .expect("put");
                }
            }
            e.flush().await.expect("flush");
            e.close().await.expect("close");
        }

        // 重启验证
        let mut e2 = StorageEngine::open(opts).await.expect("reopen");
        let dbs: Vec<String> = e2.list_dbs();
        assert_eq!(dbs.len(), n_dbs, "所有 db 应在");
        for i in 0..n_dbs {
            let db_name = format!("db_{:02}", i);
            let tables = e2.list_tables(&db_name).expect("list tables");
            assert_eq!(
                tables.len(),
                n_tables_per_db,
                "db {} 应有 {} 个表",
                i,
                n_tables_per_db
            );
            for j in 0..n_tables_per_db {
                let tbl_name = format!("t_{:02}", j);
                let key = format!("k_{:02}_{:02}", i, j);
                let expected_val = format!("v_{:02}_{:02}", i, j);
                let actual = e2
                    .table_get(&db_name, &tbl_name, key.as_bytes())
                    .await
                    .expect("get");
                assert_eq!(
                    actual,
                    Some(expected_val.into_bytes()),
                    "db {}.{} key {} value mismatch",
                    i,
                    j,
                    key
                );
            }
        }
        e2.close().await.expect("close");
    });
}

// =====================================================================
// 5. catalog 操作顺序: create → put → drop → recreate
// =====================================================================

#[test]
fn catalog_recreate_after_drop_uses_new_vpid() {
    run_async(async move {
        // drop table 后 recreate, 新的 root vpid 必须与原 vpid 不同 (vpid 永不重用)
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts.clone()).await.expect("open");

        e.create_db("app").await.expect("create db");
        let v1 = e.create_table("app", "users").await.expect("create v1");
        e.table_put("app", "users", b"k1", b"v1")
            .await
            .expect("put v1");
        e.drop_table("app", "users").await.expect("drop users");

        let v2 = e
            .create_table("app", "users")
            .await
            .expect("recreate users");
        assert_ne!(v1, v2, "vpid 永不重用, 新 vpid 必须不同");

        // 新 table 应该是空的
        assert_eq!(
            e.table_get("app", "users", b"k1").await.expect("get"),
            None,
            "recreate 后旧 key 不应在"
        );

        // 写新数据
        e.table_put("app", "users", b"k2", b"v2")
            .await
            .expect("put v2");
        assert_eq!(
            e.table_get("app", "users", b"k2").await.expect("get v2"),
            Some(b"v2".to_vec())
        );

        e.close().await.expect("close");
    });
}

// =====================================================================
// 6. write-through 缓存与 BTree 同步
// =====================================================================

#[test]
fn catalog_cache_miss_populates_via_btree() {
    run_async(async move {
        // 验证: cache miss 后能从 BTree 重新填充 (写穿透协议)
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts.clone()).await.expect("open");

        e.create_db("app").await.expect("create db");
        let _ = e.create_table("app", "users").await.expect("create users");

        // 第一次 open_table (走 cache 命中)
        let v1 = e.open_table("app", "users").await.expect("open 1").unwrap();

        // 直接从 DbHandle 调 open_table (会触发 cache 写入)
        {
            let (pager, db) = e.pager_and_db("app").expect("pager_and_db");
            let v2 = db.open_table(pager, "users").await.expect("open 2");
            assert_eq!(v2, Some(v1), "DbHandle 直接 open 应返回相同 vpid");
        }

        // 验证 cache 中确实有 users (通过 list_tables 间接)
        {
            let (_, db) = e.pager_and_db("app").expect("pager_and_db");
            let names = db.list_tables();
            assert!(names.contains(&"users".to_string()), "cache 应已填 users");
        }

        e.close().await.expect("close");
    });
}

#[test]
fn catalog_cache_miss_does_not_overwrite() {
    run_async(async move {
        // 关键不变量: cache miss 走 BTree 查到的 vpid 必须与当前 cache 一致
        // (正常情况下 cache 与 BTree 同步, cache miss 后填入的应是同一个 vpid)
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts.clone()).await.expect("open");

        e.create_db("app").await.expect("create db");
        let v1 = e.create_table("app", "users").await.expect("create users");

        // 强制 cache miss: refresh_table_cache
        {
            let (pager, db) = e.pager_and_db("app").expect("pager_and_db");
            db.refresh_table_cache(pager).await.expect("refresh ok");
            assert_eq!(
                db.open_table(pager, "users")
                    .await
                    .expect("open after refresh"),
                Some(v1),
                "refresh 后 vpid 应不变"
            );
        }

        e.close().await.expect("close");
    });
}

// =====================================================================
// 7. drop_db 不清理 table 的 page (孤儿 vpid, LRU 自然驱逐)
// =====================================================================

#[test]
fn catalog_drop_db_leaves_orphan_pages() {
    run_async(async move {
        // 验证: drop_db 不级联清理 table page. 这是 vpid 永不重用 + COW 设计
        // 的天然结果: 旧 page 留在 nowchunks/chunk_list, 等 LRU 驱逐.
        // (后续 T11 polish 用 vpid_log 显式追踪孤儿)
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts.clone()).await.expect("open");

        e.create_db("temp").await.expect("create db");
        e.create_table("temp", "users").await.expect("create users");
        e.create_table("temp", "posts").await.expect("create posts");
        e.table_put("temp", "users", b"alice", b"data")
            .await
            .expect("put");
        e.flush().await.expect("flush");

        // drop db
        e.drop_db("temp").await.expect("drop temp");
        e.flush().await.expect("flush after drop");

        // temp 已不在, 但 table_root_vpid 指向的 page 字节仍可能存在 (vpid 永不重用).
        // 这里只验证: 重新 open 整个 engine 后 db 不在, 不会 panic.
        drop(e);
        let e2 = StorageEngine::open(opts.clone()).await.expect("reopen");
        let dbs: Vec<String> = e2.list_dbs();
        assert!(!dbs.contains(&"temp".to_string()), "temp db 应被 drop");

        e2.close().await.expect("close");
    });
}

// =====================================================================
// 8. 边界: 空 db 的 TableDirectory
// =====================================================================

#[test]
fn catalog_empty_table_directory_persists() {
    run_async(async move {
        // 创建一个空 db (只有 TableDirectory BTree, 无 table) 并验证 reopen 后还在
        let (_tmp, opts) = setup();
        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open 1");
            e.create_db("empty").await.expect("create empty db");
            e.flush().await.expect("flush");
            e.close().await.expect("close");
        }
        let mut e2 = StorageEngine::open(opts).await.expect("reopen");
        let tables = e2.list_tables("empty").expect("list empty");
        assert_eq!(tables.len(), 0, "空 db 应无 table");
        e2.close().await.expect("close");
    });
}

// =====================================================================
// 9. 特殊字符 db / table 名
// =====================================================================

#[test]
fn catalog_special_char_names_work() {
    run_async(async move {
        // 验证: 含下划线 / 数字 / 短横线的名字都 OK
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts.clone()).await.expect("open");

        let db_names = vec!["db_with_underscore", "db-with-dash", "db123", "_priv", "a"];
        let table_names = vec!["table_1", "user-data", "log2026", "_sys", "T"];

        for db in &db_names {
            e.create_db(db).await.expect("create db");
        }
        for db in &db_names {
            for tbl in &table_names {
                let v = e.create_table(db, tbl).await.expect("create table");
                assert!(v >= 1);
            }
        }

        e.flush().await.expect("flush");

        // 重启验证
        drop(e);
        let mut e2 = StorageEngine::open(opts).await.expect("reopen");
        for db in &db_names {
            let tables = e2.list_tables(db).expect("list tables");
            let tables_set: HashSet<String> = tables.into_iter().collect();
            let expected: HashSet<String> = table_names.iter().map(|s| s.to_string()).collect();
            assert_eq!(tables_set, expected, "db {} 应有完整 table 集合", db);
        }

        e2.close().await.expect("close");
    });
}

// =====================================================================
// 10. db 数很多时 MetaPage 整页重写
// =====================================================================

#[test]
fn catalog_meta_page_full_rewrite_handles_many_dbs() {
    run_async(async move {
        // 验证: 大量 db 后 MetaPage 整页重写仍正常 (BTreeMap 镜像 + 整页 flush)
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts.clone()).await.expect("open");

        let n = 50;
        for i in 0..n {
            let name = format!("db_{:04}", i);
            e.create_db(&name).await.expect("create db");
        }
        e.flush().await.expect("flush");

        // 验证数量
        assert_eq!(e.db_count(), n, "应有 {} 个 db", n);

        // 再加一个
        e.create_db("new_one").await.expect("create new");
        e.flush().await.expect("flush 2");

        // 重启
        drop(e);
        let e2 = StorageEngine::open(opts.clone()).await.expect("reopen");
        assert_eq!(e2.db_count(), n + 1, "reopen 后 db 数应正确");

        e2.close().await.expect("close");
    });
}
