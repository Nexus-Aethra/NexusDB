//! T12.14 DbNameResolver 集成测试: MetaPage + Resolver 持久化 + reopen.
//!
//! ## 验证范围
//!
//! 1. MetaPage 字节布局: 头部 0-40 是 page header, 40-1064 是 resolver 段,
//!    1064+ 是 leaf BTree items.
//! 2. Resolver 段序列化: 64+ 字节 header, 默认含 "default" → DbId 0.
//! 3. 跨 reopen: 创建多个 db, close engine, reopen, 验证 resolver 完整恢复.
//! 4. 多 db 场景: 验证 name → DbId 反向解析 + DbId → name 正向解析.
//! 5. 错误处理: 损坏的 resolver 段能被 load 正确拒绝.

use std::collections::HashSet;
use std::os::unix::fs::FileExt;

use page::PAGE_HEADER_SIZE;
use storage::{IoBackend, IoBackendConfig};
use storage::db_name_resolver::{DbNameResolver, RESOLVER_HEADER_SIZE};
use storage::meta_page::{MetaError, MetaPage};
use storage::{OpenOptions, StorageEngine};

mod common;

use common::run_async;

// =====================================================================
// MetaPage 字节布局
// =====================================================================

#[test]
fn meta_page_layout_has_resolver_segment_at_offset_40() {
    run_async(async move {
        // 验证: MetaPage page bytes 头部布局是 [page_header | resolver | items]
        // page_header = [0..40], resolver = [40..40+1024], items = [40+1024..]
        let meta = MetaPage::new_empty();
        let page = meta.flush();

        // 1. page header 区域 (0..40) 应是合法 page header
        assert_eq!(&page[0..4], b"LCBP", "page magic");
        assert_eq!(page[4], 3, "page_type = Leaf");

        // 2. resolver 段 (40..1064) 应非全零 (有 default entry)
        let resolver_bytes = &page[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + RESOLVER_HEADER_SIZE];
        let count = u32::from_le_bytes(resolver_bytes[0..4].try_into().unwrap());
        assert_eq!(count, 1, "resolver count 应是 1 (含 default)");
        let next_id = u32::from_le_bytes(resolver_bytes[4..8].try_into().unwrap());
        assert_eq!(next_id, 1, "resolver next_id 应是 1 (default 用 0)");

        // 3. item 区从 1064 开始, free_off 字段 (header [0x08..0x0A]) 应 >= 1064
        //    (空 page 时 free_off = 1064 + sentinel_size; 有 db 后再前进)
        let free_off = u16::from_le_bytes(page[0x08..0x0A].try_into().unwrap());
        assert!(
            free_off as usize >= PAGE_HEADER_SIZE + RESOLVER_HEADER_SIZE,
            "free_off={} 应 >= item area 起点={}",
            free_off,
            PAGE_HEADER_SIZE + RESOLVER_HEADER_SIZE
        );
    });
}

#[test]
fn meta_page_resolver_segment_survives_reopen() {
    run_async(async move {
        // 验证: 写多 db → flush → close → reopen, resolver 段完整恢复
        let tmp = tempfile::tempdir().unwrap();
        let opts = OpenOptions {
            block_root: tmp.path().to_path_buf(),
            block_dir: Some(tmp.path().to_path_buf()),
            db_name: None,
            shard_id: 0,
            create_if_missing: true,
            chunk_cache_size: 8,
            io_backend: IoBackend::StdFs,
            io_config: IoBackendConfig::default(),
            wal_mode: Default::default(),
        };

        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open ok");
            e.create_db("analytics").await.expect("create analytics");
            e.create_db("users").await.expect("create users");
            e.create_db("inventory").await.expect("create inventory");
            e.flush().await.expect("flush");
            e.close().await.expect("close");
        }

        // 直接从 .block 读 vpid 0 (MetaPage) 字节, 验证 resolver 段
        let block_path = tmp.path().join("000001.block");
        let f = std::fs::File::open(&block_path).expect("open block");
        let mut page = [0u8; storage::PAGE_SIZE];
        f.read_exact_at(&mut page, 0).expect("read page 0");

        // 解析 resolver 段
        let resolver_bytes: [u8; RESOLVER_HEADER_SIZE] = page
            [PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + RESOLVER_HEADER_SIZE]
            .try_into()
            .unwrap();
        let resolver = DbNameResolver::deserialize(&resolver_bytes).expect("deserialize ok");

        // 应含 4 个 db: default + analytics + users + inventory
        assert_eq!(resolver.count(), 4, "resolver 应含 4 个 db");
        assert_eq!(resolver.resolve("default"), Some(0));
        assert_eq!(resolver.resolve("analytics"), Some(1));
        assert_eq!(resolver.resolve("users"), Some(2));
        assert_eq!(resolver.resolve("inventory"), Some(3));
        assert_eq!(resolver.next_id(), 4);

        // 用 MetaPage::load 验证整体一致性
        let loaded = MetaPage::load(&page).expect("load ok");
        assert_eq!(loaded.db_count(), 3, "应含 3 个 user db");
        assert_eq!(
            loaded.resolver().count(),
            4,
            "resolver 仍含 default + 3 user db"
        );
        assert_eq!(loaded.db_id("default"), Some(0));
        assert_eq!(loaded.db_id("analytics"), Some(1));
        assert_eq!(loaded.db_id("users"), Some(2));
        assert_eq!(loaded.db_id("inventory"), Some(3));
    });
}

// =====================================================================
// 多 db 场景: name ↔ id 双向解析
// =====================================================================

#[test]
fn meta_page_resolver_name_id_round_trip_via_storage_engine() {
    run_async(async move {
        // 验证: engine 创建多个 db 后, reopen, 通过 db_name 仍能查 DbId
        let tmp = tempfile::tempdir().unwrap();
        let opts = OpenOptions {
            block_root: tmp.path().to_path_buf(),
            block_dir: Some(tmp.path().to_path_buf()),
            db_name: None,
            shard_id: 0,
            create_if_missing: true,
            chunk_cache_size: 8,
            io_backend: IoBackend::StdFs,
            io_config: IoBackendConfig::default(),
            wal_mode: Default::default(),
        };

        let names = vec!["default", "analytics", "users", "inventory", "orders"];
        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open ok");
            for n in &names {
                e.create_db(n).await.expect("create");
            }
            e.flush().await.expect("flush");
            e.close().await.expect("close");
        }

        let e2 = StorageEngine::open(opts).await.expect("reopen");
        // 通过 list_dbs + create_table 走兼容 API, 间接验证 resolver 仍正确
        let listed: HashSet<String> = e2.list_dbs().into_iter().collect();
        let expected: HashSet<String> = names.iter().map(|s| s.to_string()).collect();
        assert_eq!(listed, expected, "reopen 后 list_dbs 应一致");
        e2.close().await.expect("close 2");
    });
}

// =====================================================================
// 损坏的 resolver 段
// =====================================================================

#[test]
fn meta_page_load_errors_on_corrupted_resolver_segment() {
    run_async(async move {
        // 构造一个 MetaPage 字节, 把 resolver 段写损坏 (count 越界)
        let mut meta = MetaPage::new_empty();
        meta.add_db("default", 1).unwrap();
        let mut page = meta.flush();

        // 故意把 resolver 段的 count 改成超大值, 超过 name area (1016B) 能容纳的 entry 数.
        // 0-length name 占 1B (len=0). 1016 个 0-length name 用完 1016B, 第 1017 个越界.
        // 设 count=1017 → 第 1017 次迭代 off=8+1016=1024, 触发 `off >= RESOLVER_HEADER_SIZE` 检查
        // 返回 ResolverError::InvalidData.
        let resolver_off = PAGE_HEADER_SIZE;
        page[resolver_off..resolver_off + 4].copy_from_slice(&1017u32.to_le_bytes());

        let result = MetaPage::load(page.as_ref());
        // 期望: 加载失败, 返回 PageDecode 错误 (包 ResolverError::InvalidData).
        match result {
            Err(MetaError::PageDecode(_)) => {}
            other => panic!("count 越界应触发 PageDecode 错误, got {:?}", other.err()),
        }
    });
}

// =====================================================================
// Resolver 与 dbs 一致性
// =====================================================================

#[test]
fn resolver_count_matches_dbs_count_plus_default() {
    run_async(async move {
        // 验证: resolver.count() 始终 >= dbs.count() + 1 (含 default)
        let mut meta = MetaPage::new_empty();
        assert_eq!(meta.resolver().count(), 1, "new_empty resolver 含 default");
        assert_eq!(meta.db_count(), 0);

        meta.add_db("a", 10).unwrap();
        assert_eq!(meta.resolver().count(), 2);
        assert_eq!(meta.db_count(), 1);

        meta.add_db("b", 20).unwrap();
        assert_eq!(meta.resolver().count(), 3);
        assert_eq!(meta.db_count(), 2);

        meta.remove_db("a");
        // remove_db 不从 resolver 删除 (id 永不重用)
        assert_eq!(
            meta.resolver().count(),
            3,
            "remove_db 不影响 resolver count"
        );
        assert_eq!(meta.db_count(), 1);

        let page = meta.flush();
        let loaded = MetaPage::load(page.as_ref()).unwrap();
        assert_eq!(
            loaded.resolver().count(),
            3,
            "round-trip 后 resolver count 保留"
        );
        assert_eq!(loaded.db_count(), 1, "round-trip 后 db count 正确");
    });
}

// =====================================================================
// 错误处理: name 重复 add
// =====================================================================

#[test]
fn add_db_with_existing_name_after_resolver_registration_errors() {
    // 验证: add_db 同步到 resolver, 第二次 add 同一 name 时 AlreadyExists 错误
    let mut meta = MetaPage::new_empty();
    meta.add_db("foo", 1).unwrap();
    let result = meta.add_db("foo", 2);
    assert!(matches!(result, Err(MetaError::AlreadyExists(_))));
    // resolver 不应有重复 id
    assert_eq!(meta.resolver().count(), 2); // default + foo
    assert_eq!(meta.resolver().resolve("foo"), Some(1));
}

#[test]
fn add_db_persists_resolver_to_page_bytes() {
    run_async(async move {
        // 验证: add_db 后 flush, page bytes 的 resolver 段包含新 db
        let mut meta = MetaPage::new_empty();
        meta.add_db("newdb", 42).unwrap();
        let page = meta.flush();

        // 直接从 page bytes 解析 resolver 段
        let resolver_bytes: [u8; RESOLVER_HEADER_SIZE] = page
            [PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + RESOLVER_HEADER_SIZE]
            .try_into()
            .unwrap();
        let resolver = DbNameResolver::deserialize(&resolver_bytes).unwrap();
        assert_eq!(resolver.count(), 2);
        assert_eq!(resolver.resolve("newdb"), Some(1));
        assert_eq!(resolver.resolve("default"), Some(0));
    });
}

// =====================================================================
// Drop engine 不清 resolver 段 (持久化)
// =====================================================================

#[test]
fn resolver_persists_across_multiple_open_close_cycles() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let opts = OpenOptions {
            block_root: tmp.path().to_path_buf(),
            block_dir: Some(tmp.path().to_path_buf()),
            db_name: None,
            shard_id: 0,
            create_if_missing: true,
            chunk_cache_size: 8,
            io_backend: IoBackend::StdFs,
            io_config: IoBackendConfig::default(),
            wal_mode: Default::default(),
        };

        // 第一次: 创建 3 个 db
        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open 1");
            e.create_db("db_a").await.unwrap();
            e.create_db("db_b").await.unwrap();
            e.create_db("db_c").await.unwrap();
            e.flush().await.unwrap();
            e.close().await.unwrap();
        }
        // 第二次: 再加 2 个
        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open 2");
            e.create_db("db_d").await.unwrap();
            e.create_db("db_e").await.unwrap();
            e.flush().await.unwrap();
            e.close().await.unwrap();
        }
        // 第三次: 验证总数
        {
            let e = StorageEngine::open(opts).await.expect("open 3");
            // 应有 5 个 user db + default = 6 个
            let listed: HashSet<String> = e
                .list_dbs()
                .into_iter()
                .chain(std::iter::once("default".to_string()))
                .collect();
            assert_eq!(listed.len(), 6);
            for n in &["db_a", "db_b", "db_c", "db_d", "db_e", "default"] {
                assert!(listed.contains(*n), "{} 应在 db 列表", n);
            }
            e.close().await.unwrap();
        }
    });
}

// =====================================================================
// 单元测试: 解析器本身边界
// =====================================================================

#[test]
fn resolver_default_db_id_always_zero() {
    // 不变量: "default" 永远是 DbId 0 (向后兼容, 单 db 模式)
    let r = DbNameResolver::new();
    assert_eq!(r.resolve("default"), Some(0));
    assert_eq!(r.name(0), Some("default"));
}

#[test]
fn resolver_get_or_create_default_returns_zero() {
    let mut r = DbNameResolver::new();
    assert_eq!(r.get_or_create("default").unwrap(), 0);
}
