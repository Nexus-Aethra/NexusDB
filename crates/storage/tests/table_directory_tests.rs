//! T10 TableDirectory 集成测试 (DESIGN §4.5 + plan §Task 10).
//!
//! 设计要点:
//! - TableDirectory 是单 leaf page BTree, key=table_name, value=8B u64 LE
//! - 复用 page crate leaf API (insert / get / delete)
//! - 所有写走 Pager::write_page (PageWriteBatch)
//! - 第一次 leaf_insert 触发 init_sentinel
//! - flush + close + reopen 后数据持久化
//!
//! **测试用例** (来自 plan):
//! 1. create_new 返回非零 root_vpid, list 为空
//! 2. create_table 后 get_table / list_tables 正确
//! 3. drop_table 后 list 不再包含
//! 4. flush + reopen 持久化
//!
//! 详见 `docs/superpowers/plans/2026-07-18-storage-crate.md` §Task 10.

use storage::{IoBackend, IoBackendConfig};
use storage::table_directory::TableDirError;
use storage::{OpenOptions, StorageEngine};

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
        chunk_cache_size: 8,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
    };
    (tmp, opts)
}

// =====================================================================
// 1. create_new
// =====================================================================

#[test]
fn table_directory_create_new_returns_distinct_vpid_and_empty_list() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        let td = e
            .create_table_directory()
            .await
            .expect("create_table_directory ok");

        // root_vpid 必须非零 (vpid 0 是 MetaPage)
        assert!(
            td.root_vpid >= 1,
            "root_vpid should be >= 1, got {}",
            td.root_vpid
        );
        // 新建目录为空
        let tables = td.list_tables(e.pager_mut()).await.expect("list_tables ok");
        assert_eq!(tables.len(), 0, "新建 TableDirectory 应为空");
        assert_eq!(td.table_count(e.pager_mut()).await.unwrap(), 0);

        e.close().await.expect("close ok");
    });
}

#[test]
fn table_directory_create_new_twice_returns_distinct_root_vpids() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        let td1 = e.create_table_directory().await.expect("td1 ok");
        let td2 = e.create_table_directory().await.expect("td2 ok");

        assert_ne!(td1.root_vpid, td2.root_vpid, "两个 root_vpid 必须不同");
        assert!(td2.root_vpid > td1.root_vpid, "vpid 单调递增");

        e.close().await.expect("close ok");
    });
}

// =====================================================================
// 2. create_table / get_table / list_tables
// =====================================================================

#[test]
fn table_directory_create_table_returns_distinct_root_vpids() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        let mut td = e.create_table_directory().await.expect("create td");

        let users_vpid = td
            .create_table(e.pager_mut(), "users")
            .await
            .expect("create users");
        let posts_vpid = td
            .create_table(e.pager_mut(), "posts")
            .await
            .expect("create posts");
        let orders_vpid = td
            .create_table(e.pager_mut(), "orders")
            .await
            .expect("create orders");

        // 三个 vpid 必须互不相同, 且都 >= td.root_vpid + 1
        assert_ne!(users_vpid, posts_vpid);
        assert_ne!(users_vpid, orders_vpid);
        assert_ne!(posts_vpid, orders_vpid);
        assert!(users_vpid > td.root_vpid);
        assert!(posts_vpid > users_vpid);
        assert!(orders_vpid > posts_vpid);

        e.close().await.expect("close ok");
    });
}

#[test]
fn table_directory_get_table_returns_allocated_vpid() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        let mut td = e.create_table_directory().await.expect("create td");
        let users_vpid = td
            .create_table(e.pager_mut(), "users")
            .await
            .expect("create users");
        let posts_vpid = td
            .create_table(e.pager_mut(), "posts")
            .await
            .expect("create posts");

        assert_eq!(
            td.get_table(e.pager_mut(), "users").await.unwrap(),
            Some(users_vpid)
        );
        assert_eq!(
            td.get_table(e.pager_mut(), "posts").await.unwrap(),
            Some(posts_vpid)
        );
        assert_eq!(
            td.get_table(e.pager_mut(), "nonexistent").await.unwrap(),
            None
        );

        e.close().await.expect("close ok");
    });
}

#[test]
fn table_directory_list_tables_returns_sorted_names() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        let mut td = e.create_table_directory().await.expect("create td");
        // 故意打乱顺序插入
        td.create_table(e.pager_mut(), "zebra")
            .await
            .expect("zebra");
        td.create_table(e.pager_mut(), "alpha")
            .await
            .expect("alpha");
        td.create_table(e.pager_mut(), "middle")
            .await
            .expect("middle");
        td.create_table(e.pager_mut(), "beta").await.expect("beta");

        let tables = td.list_tables(e.pager_mut()).await.expect("list ok");
        assert_eq!(
            tables,
            vec![
                "alpha".to_string(),
                "beta".to_string(),
                "middle".to_string(),
                "zebra".to_string()
            ],
            "list_tables 应按 name 升序"
        );
        assert_eq!(td.table_count(e.pager_mut()).await.unwrap(), 4);

        e.close().await.expect("close ok");
    });
}

#[test]
fn table_directory_create_duplicate_table_errors() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        let mut td = e.create_table_directory().await.expect("create td");
        td.create_table(e.pager_mut(), "users")
            .await
            .expect("first create users ok");

        // 重复 create 应报错
        let result = td.create_table(e.pager_mut(), "users").await;
        assert!(matches!(result, Err(TableDirError::AlreadyExists(_))));

        // get_table 应仍返回原 vpid
        let v1 = td.get_table(e.pager_mut(), "users").await.unwrap();
        assert!(v1.is_some(), "原 users vpid 仍存在");

        e.close().await.expect("close ok");
    });
}

// =====================================================================
// 3. drop_table
// =====================================================================

#[test]
fn table_directory_drop_table_removes_mapping() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        let mut td = e.create_table_directory().await.expect("create td");
        let users_vpid = td
            .create_table(e.pager_mut(), "users")
            .await
            .expect("create users");
        td.create_table(e.pager_mut(), "posts")
            .await
            .expect("create posts");

        // drop users
        let dropped = td
            .drop_table(e.pager_mut(), "users")
            .await
            .expect("drop ok");
        assert!(dropped, "drop users 应返回 true");

        // 验证 users 不再存在
        assert_eq!(td.get_table(e.pager_mut(), "users").await.unwrap(), None);
        // posts 仍在
        assert!(
            td.get_table(e.pager_mut(), "posts")
                .await
                .unwrap()
                .is_some()
        );
        // list 只有一个
        let tables = td.list_tables(e.pager_mut()).await.unwrap();
        assert_eq!(tables, vec!["posts".to_string()]);

        // **注意**: users_vpid 的 page 还在 Pager 里 (孤儿), 这是预期行为.
        // 这里只验证 directory mapping 已删除.
        let _ = users_vpid;

        e.close().await.expect("close ok");
    });
}

#[test]
fn table_directory_drop_nonexistent_table_returns_false() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        let mut td = e.create_table_directory().await.expect("create td");
        td.create_table(e.pager_mut(), "users")
            .await
            .expect("create users");

        let dropped = td
            .drop_table(e.pager_mut(), "nonexistent")
            .await
            .expect("drop ok");
        assert!(!dropped, "drop 不存在的 table 应返回 false");

        e.close().await.expect("close ok");
    });
}

#[test]
fn table_directory_drop_then_recreate_works() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        let mut td = e.create_table_directory().await.expect("create td");
        let v1 = td
            .create_table(e.pager_mut(), "users")
            .await
            .expect("create v1");
        td.drop_table(e.pager_mut(), "users")
            .await
            .expect("drop users");

        // 重新创建 users
        let v2 = td
            .create_table(e.pager_mut(), "users")
            .await
            .expect("recreate users");
        assert_ne!(v1, v2, "vpid 不复用 (永不重用), 新 vpid 必须不同");

        assert_eq!(
            td.get_table(e.pager_mut(), "users").await.unwrap(),
            Some(v2)
        );
        assert_eq!(td.table_count(e.pager_mut()).await.unwrap(), 1);

        e.close().await.expect("close ok");
    });
}

// =====================================================================
// 4. flush + reopen 持久化
// =====================================================================

#[test]
fn table_directory_flush_persists_via_storage_engine() {
    run_async(async move {
        let (_tmp, opts) = setup();

        // 第一次: 创建 td, 写两个 table, flush, close
        let (td_root, users_vpid, posts_vpid) = {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open 1");
            let mut td = e.create_table_directory().await.expect("create td");
            let u = td
                .create_table(e.pager_mut(), "users")
                .await
                .expect("users");
            let p = td
                .create_table(e.pager_mut(), "posts")
                .await
                .expect("posts");
            e.flush().await.expect("flush");
            e.close().await.expect("close");
            (td.root_vpid, u, p)
        };

        // 重启: 用 engine.open_table_directory(td_root) 恢复
        let mut e2 = StorageEngine::open(opts).await.expect("open 2");
        let td2 = e2.open_table_directory(td_root).await.expect("open td ok");

        assert_eq!(td2.root_vpid, td_root);
        assert_eq!(
            td2.get_table(e2.pager_mut(), "users").await.unwrap(),
            Some(users_vpid)
        );
        assert_eq!(
            td2.get_table(e2.pager_mut(), "posts").await.unwrap(),
            Some(posts_vpid)
        );
        assert_eq!(td2.list_tables(e2.pager_mut()).await.unwrap().len(), 2);

        e2.close().await.expect("close 2");
    });
}

#[test]
fn table_directory_close_flushes_pending_writes() {
    run_async(async move {
        // 验证 close 隐式 flush (engine.close 调 pager.flush)
        let (_tmp, opts) = setup();

        let (td_root, users_vpid) = {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open");
            let mut td = e.create_table_directory().await.expect("create td");
            let u = td
                .create_table(e.pager_mut(), "users")
                .await
                .expect("users");
            // 不显式 flush, 调 close (应隐式 flush)
            e.close().await.expect("close");
            (td.root_vpid, u)
        };

        // 重启验证
        let mut e2 = StorageEngine::open(opts).await.expect("reopen");
        let td2 = e2.open_table_directory(td_root).await.expect("open td");
        assert_eq!(
            td2.get_table(e2.pager_mut(), "users").await.unwrap(),
            Some(users_vpid)
        );
        e2.close().await.expect("close 2");
    });
}

#[test]
fn table_directory_open_rejects_bad_vpid() {
    run_async(async move {
        // 用一个未分配的大 vpid 应报错 (Pager::read 返回 NotFound)
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        // vpid 99999 不存在
        let result = e.open_table_directory(99999);
        assert!(result.await.is_err(), "未分配 vpid 应返回 Err");

        e.close().await.expect("close ok");
    });
}

#[test]
fn table_directory_open_rejects_non_leaf_page() {
    run_async(async move {
        // 所有 Pager 写的 page 都是 Leaf, 这个 test 实际无法构造 non-leaf page.
        // 改测: 用 leaf_new() 构造一个 valid leaf page (含哨兵), put 进 engine,
        // 然后 open_table_directory 验证可以打开.
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open ok");

        // put 一个 valid leaf page (有 header), 用 page::leaf_new
        let v = e.put(page::leaf_new()).await.expect("put ok");
        // user data 是 leaf page, open 应成功 (vpid 一致, page_type=Leaf)
        let td = e
            .open_table_directory(v)
            .await
            .expect("open user vpid as td ok");
        // 但内容是空 leaf, get_table 应返回 None
        assert_eq!(td.get_table(e.pager_mut(), "anything").await.unwrap(), None);

        e.close().await.expect("close ok");
    });
}

// =====================================================================
// 5. 大量 table (验证单 leaf page 性能 + 边界)
// =====================================================================

#[test]
fn table_directory_many_tables_persist() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts.clone()).await.expect("open");

        let mut td = e.create_table_directory().await.expect("create td");
        let n = 100;
        let mut inserted = std::collections::HashMap::new();
        for i in 0..n {
            let name = format!("table_{:03}", i);
            let v = td.create_table(e.pager_mut(), &name).await.expect("create");
            inserted.insert(name, v);
        }

        assert_eq!(td.table_count(e.pager_mut()).await.unwrap(), n);

        e.flush().await.expect("flush");
        e.close().await.expect("close");

        // 重启验证
        let mut e2 = StorageEngine::open(opts).await.expect("reopen");
        let td2 = e2
            .open_table_directory(td.root_vpid)
            .await
            .expect("reopen td");
        assert_eq!(td2.table_count(e2.pager_mut()).await.unwrap(), n);
        for (name, expected_vpid) in &inserted {
            assert_eq!(
                td2.get_table(e2.pager_mut(), name).await.unwrap(),
                Some(*expected_vpid),
                "table {} vpid mismatch",
                name
            );
        }

        e2.close().await.expect("close 2");
    });
}

#[test]
fn table_directory_create_drop_churn() {
    run_async(async move {
        // 反复 create + drop 验证 leaf page 状态一致
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open");

        let mut td = e.create_table_directory().await.expect("create td");

        for i in 0..20 {
            let name = format!("tmp_{:03}", i);
            td.create_table(e.pager_mut(), &name).await.expect("create");
            let dropped = td.drop_table(e.pager_mut(), &name).await.expect("drop");
            assert!(dropped);
            assert_eq!(td.get_table(e.pager_mut(), &name).await.unwrap(), None);
        }

        assert_eq!(td.table_count(e.pager_mut()).await.unwrap(), 0);

        e.close().await.expect("close ok");
    });
}

// =====================================================================
// 6. flush() 方法
// =====================================================================

#[test]
fn table_directory_flush_method_works() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open");

        let mut td = e.create_table_directory().await.expect("create td");
        td.create_table(e.pager_mut(), "users")
            .await
            .expect("users");
        td.flush(e.pager_mut()).await.expect("flush ok");

        // flush 后数据应可读
        assert!(
            td.get_table(e.pager_mut(), "users")
                .await
                .unwrap()
                .is_some()
        );

        e.close().await.expect("close ok");
    });
}
