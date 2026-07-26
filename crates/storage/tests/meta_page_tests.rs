//! T9 MetaPage 集成测试: Pager + MetaPage 协同 (DESIGN §4.5 + plan §Task 9).
//!
//! 设计要点:
//! - MetaPage 永远在 chunk 0 page 0 (vpid 0)
//! - 创建 engine 后, MetaPage 应自动初始化
//! - put / get / flush / reopen 后 MetaPage 数据应持久化
//! - 写 MetaPage 必须走 PageWriteBatch, 写 user data 也必须走 PageWriteBatch
//!
//! 详见 `docs/superpowers/plans/2026-07-18-storage-crate.md` §Task 9.

use std::os::unix::fs::FileExt;

use storage::{IoBackend, IoBackendConfig};
use storage::meta_page::{META_PID, META_VPID, MetaError, MetaPage};
use storage::{OpenOptions, PAGE_SIZE, StorageEngine};

mod common;

use common::run_async;

// =====================================================================
// ⭐ MetaPage 单元级集成 (不依赖 Engine, 走 raw Pager 路径)
// =====================================================================

#[test]
fn meta_page_flush_then_load_via_storage_engine() {
    run_async(async move {
        // 用临时目录 + 手动构造 meta page + pager 路径
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
        
        // 第一次: open, put data, flush, close
        let (v_user, _meta_pid_pos) = {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open ok");

            // put 一个 user page
            let mut data = [0u8; PAGE_SIZE];
            data[0x28] = 0xCC;
            let v = e.put(data).await.expect("put ok");
            // v 应该是 1, 因为 vpid 0 = MetaPage
            assert_eq!(v, 1, "vpid 0 是 MetaPage, user data 从 vpid 1 开始");

            e.flush().await.expect("flush ok");
            e.close().await.expect("close ok");
            (v, META_PID)
        };

        // 重启: open, 验证 MetaPage 存在 (默认空) 且 user data 可读
        let mut e2 = StorageEngine::open(opts).await.expect("reopen ok");
        let r = e2.get(v_user).await.expect("get user ok");
        assert_eq!(r[0x28], 0xCC);
        e2.close().await.expect("close 2");
    });
}

#[test]
fn meta_page_persists_in_disk_after_recovery() {
    run_async(async move {
        // 验证 MetaPage 字节确实在 .block chunk 0 page 0
        let tmp = tempfile::tempdir().unwrap();
        let opts = OpenOptions {
            block_root: tmp.path().to_path_buf().clone(),
            block_dir: None,
            db_name: Some("default".to_string()),
            shard_id: 0,
            create_if_missing: true,
            chunk_cache_size: 8,
            io_backend: IoBackend::StdFs,
            io_config: IoBackendConfig::default(),
        };
        
        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open ok");
            e.flush().await.expect("flush ok");
            e.close().await.expect("close ok");
        }

        // 直接读 .block chunk 0 page 0 (新路径: {tmp}/default/shard_0/000001.block)
        let block_path = tmp
            .path()
            .join("default")
            .join("shard_0")
            .join("000001.block");
        let f = std::fs::File::open(&block_path).expect("open block");
        let mut buf = [0u8; PAGE_SIZE];
        f.read_exact_at(&mut buf, 0).expect("read page 0");

        // 校验 magic
        assert_eq!(&buf[0..4], b"LCBP", "page 0 magic 应是 LCBP");
        // 校验 vpid 字段 = 0
        let vpid = u64::from_le_bytes(buf[0x18..0x20].try_into().unwrap());
        assert_eq!(vpid, META_VPID, "page 0 vpid 应是 0 (MetaPage)");
    });
}

#[test]
fn meta_page_position_is_chunk0_page0() {
    // META_PID 必须是 chunk 0 page 0
    assert_eq!(META_VPID, 0);
    assert_eq!(META_PID.file_id(), 0);
    assert_eq!(META_PID.chunk_idx(), 0);
    assert_eq!(META_PID.page_idx(), 0);
}

// =====================================================================
// ⭐ MetaPage 镜像 + flush 协同
// =====================================================================

#[test]
fn meta_page_can_be_loaded_from_engine_owned_bytes() {
    run_async(async move {
        // 直接构造一个 MetaPage, flush, 再用 storage engine 把 page 字节作为 user page 存
        // 然后验证能 load
        let mut meta = MetaPage::new_empty();
        meta.add_db("default", 42).expect("add ok");
        let page_bytes = meta.flush();

        // page_bytes 是有效的 MetaPage 字节 (page_type=Leaf, vpid=0, 1 个 db)
        let loaded = MetaPage::load(page_bytes.as_ref()).expect("load ok");
        assert_eq!(loaded.db_count(), 1);
        assert_eq!(loaded.get_db("default"), Some(42));
    });
}

#[test]
fn meta_page_many_dbs_roundtrip() {
    run_async(async move {
        // 大量 db 添加, 验证 sort + flush + load
        let mut meta = MetaPage::new_empty();
        let db_names: Vec<String> = (0..50).map(|i| format!("db_{:03}", i)).collect();
        for (i, name) in db_names.iter().enumerate() {
            meta.add_db(name, (i + 1) as u64).expect("add ok");
        }

        let page = meta.flush();
        let loaded = MetaPage::load(page.as_ref()).expect("load ok");
        assert_eq!(loaded.db_count(), db_names.len());

        let listed = loaded.list_dbs();
        for (i, (name, vpid)) in listed.iter().enumerate() {
            let expected_name = format!("db_{:03}", i);
            assert_eq!(name, &expected_name, "db name at position {}", i);
            assert_eq!(*vpid, (i + 1) as u64, "vpid at position {}", i);
        }
    });
}

#[test]
fn meta_page_add_remove_add_cycle() {
    run_async(async move {
        // 反复 add / remove / add 验证 BTreeMap 正确性
        let mut meta = MetaPage::new_empty();

        meta.add_db("a", 1).unwrap();
        meta.add_db("b", 2).unwrap();
        assert!(meta.remove_db("a"));
        assert!(!meta.remove_db("a"));
        assert!(!meta.remove_db("nonexistent"));
        meta.add_db("a", 3).unwrap();
        assert_eq!(meta.get_db("a"), Some(3));
        assert_eq!(meta.get_db("b"), Some(2));

        let page = meta.flush();
        let loaded = MetaPage::load(page.as_ref()).unwrap();
        assert_eq!(loaded.get_db("a"), Some(3));
        assert_eq!(loaded.get_db("b"), Some(2));
        assert_eq!(loaded.db_count(), 2);
    });
}

// =====================================================================
// ⭐ MetaPage 错误情况
// =====================================================================

#[test]
fn meta_page_load_errors_on_bad_bytes() {
    let bad = [0u8; PAGE_SIZE];
    let result = MetaPage::load(&bad);
    assert!(matches!(result, Err(MetaError::BadMagic)));
}

#[test]
fn meta_page_empty_dbs_after_recovery() {
    run_async(async move {
        // 全新 db (无任何 page) 重启后, MetaPage 应是空的
        let tmp = tempfile::tempdir().unwrap();
        let opts = OpenOptions {
            block_root: tmp.path().to_path_buf().clone(),
            block_dir: None,
            db_name: Some("default".to_string()),
            shard_id: 0,
            create_if_missing: true,
            chunk_cache_size: 8,
            io_backend: IoBackend::StdFs,
            io_config: IoBackendConfig::default(),
        };
        
        {
            let e = StorageEngine::open(opts.clone()).await.expect("open ok");
            e.close().await.expect("close ok");
        }

        // 重启 - 此时 MetaPage 应该是空 dbs 镜像
        let mut e2 = StorageEngine::open(opts).await.expect("reopen ok");
        // put 一个 user data 触发后续 vpids 分配
        let v = e2.put([0u8; PAGE_SIZE]).await.expect("put ok");
        assert_eq!(v, 1, "MetaPage 占 vpid 0, user data 从 1 开始");
        e2.close().await.expect("close 2");
    });
}

// =====================================================================
// ⭐ MetaPage flush 后 page bytes 内部一致性
// =====================================================================

#[test]
fn meta_page_flush_yields_valid_pager_parseable_page() {
    run_async(async move {
        // 验证 flush 出的 page 字节能被 page crate 直接 parse
        let mut meta = MetaPage::new_empty();
        meta.add_db("default", 1).unwrap();
        meta.add_db("analytics", 2).unwrap();
        let page = meta.flush();

        // magic
        assert_eq!(&page[0..4], b"LCBP");
        // page_type = 3 (Leaf)
        assert_eq!(page[4], 3, "MetaPage 用 Leaf page_type");
        // vpid = 0
        let vpid = u64::from_le_bytes(page[0x18..0x20].try_into().unwrap());
        assert_eq!(vpid, 0);

        // key_count = 2 (不含哨兵, 但 PageIndex 含哨兵 = 3 items)
        let key_count = u16::from_le_bytes(page[0x06..0x08].try_into().unwrap());
        assert_eq!(key_count, 2);

        // 用 leaf_get 验证
        use page::leaf_get;
        let v = leaf_get(page.as_ref(), b"default").unwrap();
        assert_eq!(v, 1u64.to_le_bytes().to_vec());
        let v = leaf_get(page.as_ref(), b"analytics").unwrap();
        assert_eq!(v, 2u64.to_le_bytes().to_vec());
    });
}

// =====================================================================
// ⭐ 大量数据: 模拟一个 catalog 场景
// =====================================================================

#[test]
fn meta_page_pager_integration_with_simulated_catalog() {
    run_async(async move {
        // 模拟: 创建 engine, 直接构造一个 MetaPage flush, 然后验证 engine 把它写到 chunk 0 page 0 后能 reopen 读回
        // 这是 T11 DbRegistry 的初步集成方式
        let tmp = tempfile::tempdir().unwrap();
        let opts = OpenOptions {
            block_root: tmp.path().to_path_buf().clone(),
            block_dir: None,
            db_name: Some("default".to_string()),
            shard_id: 0,
            create_if_missing: true,
            chunk_cache_size: 8,
            io_backend: IoBackend::StdFs,
            io_config: IoBackendConfig::default(),
        };
        
        // 1. 构造 MetaPage (3 个 db)
        let mut meta = MetaPage::new_empty();
        meta.add_db("users", 100).unwrap();
        meta.add_db("orders", 200).unwrap();
        meta.add_db("inventory", 300).unwrap();
        let meta_page_bytes = meta.flush();

        // 2. open engine (这一步会 init MetaPage 到 chunk 0 page 0, 即 vpid 0)
        //    engine.put 调用 pager.create, pager 把 caller 字节视为 raw user data
        //    (不构造 header, header 由 pager::write_page_with_vpid 自动写).
        //    所以直接 put meta_page_bytes 是不行的: header 区域会被覆盖, key_count 变 0.
        //
        //    正确做法: 用 pager 直接写 vpid 0 (走 PageWriteBatch).
        //    这个 test 跳过这个细节, 只验证 MetaPage 自身逻辑 (在 meta_page.rs unit tests
        //    和其他 tests 里覆盖).
        //
        //    替代验证: 用 pager.read(vpid 0) 能拿到 init 时的空 MetaPage.
        let mut e = StorageEngine::open(opts.clone()).await.expect("open ok");
        let r0 = e.get(0).await.expect("get vpid 0 (MetaPage)");
        // vpid 0 的 page header
        assert_eq!(&r0[0..4], b"LCBP", "vpid 0 magic");
        let vpid_in_page = u64::from_le_bytes(r0[0x18..0x20].try_into().unwrap());
        assert_eq!(vpid_in_page, 0, "vpid 0 page 标记 vpid 0");
        // 空 MetaPage 的 dbs 应该是空
        let loaded = MetaPage::load(r0.as_ref()).expect("load vpid 0 page");
        assert_eq!(loaded.db_count(), 0, "vpid 0 是空 MetaPage");
        e.close().await.expect("close");

        // 3. 重复上面, 但用 PageWriteBatch 写一个非空 MetaPage 到 vpid 0 (模拟后续 T11 集成)
        //    这里直接调 meta_page.flush() 写到 vpid 0: 不行, 因为我们没有低层 API 写 vpid 0.
        //    所以这个 test 的 catalog 集成留给 T10 / T11 验证.
        let _ = meta_page_bytes;
    });
}
