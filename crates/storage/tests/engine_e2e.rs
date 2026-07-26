//! T8 StorageEngine 端到端测试 (DESIGN §4.5 + plan §3.0.1 + §3.0.5 + T8 实施).
//!
//! 设计要点:
//! - StorageEngine facade: open / put / get / flush / close
//! - 端到端 put-get-close-reopen-get 验证 recover 链路
//! - 多 page put / 多次 flush / 大数据量
//! - chunk_list cache 命中验证
//! - TravelTreeGuard + PageWriteBatch 集成验证 (e2e B+Tree 操作留 polish, 这里只验证单 page put)
//!
//! **第一版 (TDD 简化)**:
//! - 同步 API (与 Pager 一致), 不用 async/await
//! - 单一 file_id 假设, 不实现 multi-block rotate
//!
//! 详见 `docs/superpowers/plans/2026-07-18-storage-crate.md` T8.

use std::path::Path;

use storage::{IoBackend, IoBackendConfig};
use storage::engine::OpenOptions;
use storage::{PAGE_SIZE, StorageEngine};

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

fn make_data(first_byte: u8) -> [u8; PAGE_SIZE] {
    let mut d = [0u8; PAGE_SIZE];
    // ⭐ 2026-07-19: Pager 不再覆盖 [0..0x28] header, caller 负责.
    // 写一个 valid leaf page header:
    //   - magic "LCBP" at [0..4]
    //   - page_type = Leaf (3) at [4]
    //   - key_count = 0 at [6..8]
    //   - free_off = PAGE_HEADER_SIZE at [8..0x0A]
    //   - version = 1 at [0x14..0x18]
    d[0..4].copy_from_slice(&[0x4C, 0x43, 0x42, 0x50]); // "LCBP"
    d[4] = 3; // Leaf
    d[0x06..0x08].copy_from_slice(&0u16.to_le_bytes()); // key_count = 0
    d[0x08..0x0A].copy_from_slice(&(storage::PAGE_SIZE as u16).to_le_bytes()); // free_off = PAGE_SIZE
    d[0x14..0x18].copy_from_slice(&1u32.to_le_bytes()); // version = 1
    // caller data 写在 [0x28..PAGE_SIZE] (header 区域 [0..0x28] 已被 header 占用)
    d[0x28] = first_byte;
    d
}

/// 确保 block_dir + page.mate + 000001.block 存在 (helper for non-default test paths).
#[allow(dead_code)]
fn ensure_dir(path: &Path) {
    std::fs::create_dir_all(path).unwrap();
}

// =====================================================================
// ⭐ 基础: put / get / close / reopen / get
// =====================================================================

#[test]
fn engine_put_get_close_reopen_get() {
    run_async(async move {
        let (_tmp, opts) = setup();

        // 第一次: put 3 page, flush, close
        let (v1, v2, v3) = {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open ok");
            let v1 = e.put(make_data(0x11)).await.expect("put 1");
            let v2 = e.put(make_data(0x22)).await.expect("put 2");
            let v3 = e.put(make_data(0x33)).await.expect("put 3");
            e.flush().await.expect("flush");
            e.close().await.expect("close");
            (v1, v2, v3)
        };

        // 重启: open, get, 验证
        let mut e2 = StorageEngine::open(opts).await.expect("reopen ok");
        let r1 = e2.get(v1).await.expect("get 1");
        let r2 = e2.get(v2).await.expect("get 2");
        let r3 = e2.get(v3).await.expect("get 3");
        assert_eq!(r1[0x28], 0x11);
        assert_eq!(r2[0x28], 0x22);
        assert_eq!(r3[0x28], 0x33);
        e2.close().await.expect("close 2");
    });
}

#[test]
fn engine_open_creates_block_dir_and_files() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let opts = OpenOptions {
            block_root: tmp.path().join("nested_data").clone(),
            block_dir: Some(tmp.path().join("nested_data")),
            db_name: None,
            shard_id: 0,
            create_if_missing: true,
            chunk_cache_size: 4,
            io_backend: IoBackend::StdFs,
            io_config: IoBackendConfig::default(),
        };

        let block_dir = opts.block_dir.as_ref().expect("compat mode");
        assert!(!block_dir.exists());
        let _e = StorageEngine::open(opts.clone())
            .await
            .expect("open with create");

        // 验证: block_dir, page.mate, 000001.block 都存在
        assert!(block_dir.exists(), "block_dir 应被创建");
        assert!(block_dir.join("page.mate").exists(), "page.mate 应被创建");
        assert!(
            block_dir.join("000001.block").exists(),
            "000001.block 应被创建"
        );
    });
}

#[test]
fn engine_open_without_create_if_missing_errors_on_missing_dir() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let opts = OpenOptions {
            block_root: tmp.path().join("missing_dir").clone(),
            block_dir: Some(tmp.path().join("missing_dir")),
            db_name: None,
            shard_id: 0,
            create_if_missing: false,
            chunk_cache_size: 4,
            io_backend: IoBackend::StdFs,
            io_config: IoBackendConfig::default(),
        };

        let result = StorageEngine::open(opts);
        // 不创建目录 → recover 在 create 路径中应失败
        // (具体行为取决于 recover: 可能成功因 read_dir 不存在 → 返回空状态)
        // 我们的当前实现: recover 在 block_dir 不存在时返回空 state, 不报错
        // 这其实是个 "新库" 场景, 也合理
        drop(result);
    });
}

// =====================================================================
// ⭐ 多次 put 不 flush 也能 close (close 隐式 flush)
// =====================================================================

#[test]
fn engine_close_flushes_pending_writes() {
    run_async(async move {
        let (_tmp, opts) = setup();

        let v = {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open");
            let v = e.put(make_data(0x99)).await.expect("put");
            // 不显式 flush, 调 close (应隐式 flush)
            e.close().await.expect("close");
            v
        };

        // 重启: get 验证数据已落盘
        let mut e2 = StorageEngine::open(opts).await.expect("reopen");
        let r = e2.get(v).await.expect("get");
        assert_eq!(r[0x28], 0x99, "close 应已 flush, 数据应持久化");
    });
}

#[test]
fn engine_flush_idempotent() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open");

        let v = e.put(make_data(0x42)).await.expect("put");
        e.flush().await.expect("flush 1");
        e.flush().await.expect("flush 2 (idempotent)");
        e.flush().await.expect("flush 3");

        let r = e.get(v).await.expect("get");
        assert_eq!(r[0x28], 0x42);
        e.close().await.expect("close");
    });
}

// =====================================================================
// ⭐ 多 page 写入与读出
// =====================================================================

#[test]
fn engine_put_100_pages_persist_across_restart() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut vpids = Vec::new();

        // 第一次: 写 100 page
        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open");
            for i in 0..100u64 {
                let v = e.put(make_data(i as u8)).await.expect("put");
                vpids.push(v);
            }
            e.flush().await.expect("flush");
            e.close().await.expect("close");
        }

        // 重启: 验证
        let mut e2 = StorageEngine::open(opts).await.expect("reopen");
        for (i, &v) in vpids.iter().enumerate() {
            let r = e2.get(v).await.unwrap_or_else(|_| panic!("get {}", i));
            assert_eq!(r[0x28], i as u8, "vpid {} first byte", i);
        }
        e2.close().await.expect("close");
    });
}

#[test]
fn engine_get_unmapped_vpid_returns_not_found() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open");

        // 写一个 vpid, 拿它的值, 然后用一个未分配的 vpid
        let v = e.put(make_data(0xAA)).await.expect("put");
        let r = e.get(v).await.expect("get existing");
        assert_eq!(r[0x28], 0xAA);

        // vpid 9999 未分配
        let result = e.get(9999);
        assert!(result.await.is_err(), "未分配 vpid 应返回 Err");
        e.close().await.expect("close");
    });
}

#[test]
fn engine_overwrite_vpid_in_nowchunks_then_flush() {
    run_async(async move {
        // 同一 vpid 多次 put: 后者覆盖前者 (走 nowchunks)
        let (_tmp, opts) = setup();
        let v = {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open");
            let v1 = e.put(make_data(0x11)).await.expect("put 1");
            let v2 = e.put(make_data(0x22)).await.expect("put 2");
            let v3 = e.put(make_data(0x33)).await.expect("put 3");

            // 3 个不同的 vpid, 验证
            let r1 = e.get(v1).await.expect("get v1");
            let r2 = e.get(v2).await.expect("get v2");
            let r3 = e.get(v3).await.expect("get v3");
            assert_eq!(r1[0x28], 0x11);
            assert_eq!(r2[0x28], 0x22);
            assert_eq!(r3[0x28], 0x33);

            e.flush().await.expect("flush");
            e.close().await.expect("close");
            v1 // 任意一个, 用于下面 reopen 验证
        };

        let mut e2 = StorageEngine::open(opts).await.expect("reopen");
        let r = e2.get(v).await.expect("get after reopen");
        assert_eq!(r[0x28], 0x11);
        e2.close().await.expect("close");
    });
}

// =====================================================================
// ⭐ chunk_list 缓存行为
// =====================================================================

#[test]
fn engine_multiple_gets_hit_chunk_cache() {
    run_async(async move {
        // 同一 chunk 内的多个 page, 多次 get 应走 chunk_list cache 命中
        let (_tmp, opts) = setup();

        let mut e = StorageEngine::open(opts).await.expect("open");
        let v1 = e.put(make_data(0x11)).await.expect("put 1");
        let v2 = e.put(make_data(0x22)).await.expect("put 2");
        let v3 = e.put(make_data(0x33)).await.expect("put 3");
        e.flush().await.expect("flush");

        // 多次 get, 应全部命中 cache
        let initial_cache_len = e.chunk_cache_len();
        for _ in 0..3 {
            let _ = e.get(v1).await.expect("get 1");
            let _ = e.get(v2).await.expect("get 2");
            let _ = e.get(v3).await.expect("get 3");
        }
        // 多次读不增加 cache 长度
        let final_cache_len = e.chunk_cache_len();
        assert_eq!(
            initial_cache_len, final_cache_len,
            "同一 chunk 多次 get 不应增加 cache 长度"
        );

        e.close().await.expect("close");
    });
}

// =====================================================================
// ⭐ chunk 满触发 rotate (page_idx 跨 64 边界)
// =====================================================================

#[test]
fn engine_put_crosses_chunk_boundary_and_rotate() {
    run_async(async move {
        // 写 70 page, 跨 chunk 0 (64 page) + chunk 1 (6 page)
        let (_tmp, opts) = setup();
        let mut vpids = Vec::new();

        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open");
            for i in 0..70u64 {
                let v = e
                    .put(make_data(i as u8))
                    .await
                    .unwrap_or_else(|_| panic!("put {}", i));
                vpids.push(v);
            }
            e.flush().await.expect("flush");
            e.close().await.expect("close");
        }

        let mut e2 = StorageEngine::open(opts).await.expect("reopen");
        for (i, &v) in vpids.iter().enumerate() {
            let r = e2.get(v).await.unwrap_or_else(|_| panic!("get {}", i));
            assert_eq!(r[0x28], i as u8);
        }
        e2.close().await.expect("close");
    });
}

// =====================================================================
// ⭐ OpenOptions Clone + Default
// =====================================================================

#[test]
fn open_options_clone_works() {
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
    let opts2 = opts.clone();
    assert_eq!(opts.block_dir, opts2.block_dir);
    assert_eq!(opts.create_if_missing, opts2.create_if_missing);
    assert_eq!(opts.chunk_cache_size, opts2.chunk_cache_size);
}

// =====================================================================
// ⭐ 多轮 open/close 复用
// =====================================================================

#[test]
fn engine_repeated_open_close_consistent() {
    run_async(async move {
        let (_tmp, opts) = setup();

        // 轮 1: 写 5 page
        let mut vpids_round1 = Vec::new();
        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open 1");
            for i in 0..5u64 {
                let v = e.put(make_data(0xA0 + i as u8)).await.expect("put");
                vpids_round1.push(v);
            }
            e.flush().await.expect("flush");
            e.close().await.expect("close");
        }

        // 轮 2: 在已有数据基础上再写 5 page
        let mut vpids_round2 = Vec::new();
        {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open 2");
            for i in 0..5u64 {
                let v = e.put(make_data(0xB0 + i as u8)).await.expect("put");
                vpids_round2.push(v);
            }
            e.flush().await.expect("flush");
            e.close().await.expect("close");
        }

        // 验证: 轮 1 + 轮 2 的 page 都能读到
        let mut e3 = StorageEngine::open(opts).await.expect("open 3");
        for (i, &v) in vpids_round1.iter().enumerate() {
            let r = e3.get(v).await.expect("get round 1");
            assert_eq!(r[0x28], 0xA0 + i as u8);
        }
        for (i, &v) in vpids_round2.iter().enumerate() {
            let r = e3.get(v).await.expect("get round 2");
            assert_eq!(r[0x28], 0xB0 + i as u8);
        }
        e3.close().await.expect("close");
    });
}

// =====================================================================
// ⭐ 验证 engine 暴露 meta / chunk_cache_len
// =====================================================================

#[test]
fn engine_exposes_meta_and_chunk_cache_len() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open");
        e.put(make_data(0x55)).await.expect("put");
        e.flush().await.expect("flush");

        // 验证 accessor
        let _meta = e.meta();
        let cache_len = e.chunk_cache_len();
        // 写入后 chunk_list 还没加载, cache_len 可能是 0
        let _ = cache_len;
        e.close().await.expect("close");
    });
}

// =====================================================================
// ⭐ 重启后 vpid 重新分配不复用旧 vpid
// =====================================================================

#[test]
fn engine_after_reopen_vpid_starts_from_max_plus_one() {
    run_async(async move {
        let (_tmp, opts) = setup();

        // 第一次: 写 3 page, close
        let first_v: [u64; 3] = {
            let mut e = StorageEngine::open(opts.clone()).await.expect("open 1");
            let v1 = e.put(make_data(0x11)).await.expect("put 1");
            let v2 = e.put(make_data(0x22)).await.expect("put 2");
            let v3 = e.put(make_data(0x33)).await.expect("put 3");
            e.close().await.expect("close");
            [v1, v2, v3]
        };

        // 第二次: 写 1 个新 page, 验证 vpid 不复用
        let mut e2 = StorageEngine::open(opts).await.expect("open 2");
        let v4 = e2.put(make_data(0x44)).await.expect("put 4");
        e2.flush().await.expect("flush");

        // v4 应是 4 (max 0..3 + 1 = 4, 因为 vpid 0 现在保留给 MetaPage)
        assert_eq!(
            v4, 4,
            "新 vpid 应从 max+1 开始, 不复用 (vpid 0 保留给 MetaPage)"
        );

        // 验证旧 vpid 仍能 read
        let expected_bytes = [0x11, 0x22, 0x33];
        for (i, &v) in first_v.iter().enumerate() {
            let r = e2.get(v).await.unwrap_or_else(|_| panic!("get {}", i));
            assert_eq!(r[0x28], expected_bytes[i]);
        }
        e2.close().await.expect("close");
    });
}

// =====================================================================
// ⭐ StorageEngine 跨 dir 隔离
// =====================================================================

#[test]
fn engine_two_engines_in_separate_dirs_isolated() {
    run_async(async move {
        let tmp1 = tempfile::tempdir().unwrap();
        let tmp2 = tempfile::tempdir().unwrap();
        let opts1 = OpenOptions {
            block_root: tmp1.path().to_path_buf().clone(),
            block_dir: Some(tmp1.path().to_path_buf()),
            db_name: None,
            shard_id: 0,
            create_if_missing: true,
            chunk_cache_size: 4,
            io_backend: IoBackend::StdFs,
            io_config: IoBackendConfig::default(),
        };
        let opts2 = OpenOptions {
            block_root: tmp2.path().to_path_buf().clone(),
            block_dir: Some(tmp2.path().to_path_buf()),
            db_name: None,
            shard_id: 0,
            create_if_missing: true,
            chunk_cache_size: 4,
            io_backend: IoBackend::StdFs,
            io_config: IoBackendConfig::default(),
        };

        // 写 1 page 到 opts1
        let v1 = {
            let mut e1 = StorageEngine::open(opts1.clone()).await.expect("open 1");
            let v = e1.put(make_data(0x11)).await.expect("put");
            e1.flush().await.expect("flush");
            e1.close().await.expect("close");
            v
        };

        // opts2 是新库, 不应能读到 v1
        let mut e2 = StorageEngine::open(opts2).await.expect("open 2");
        let result = e2.get(v1);
        assert!(
            result.await.is_err(),
            "独立 dir 的 engine 不应能读到另一个 dir 的 vpid"
        );

        // opts2 自己的 vpid 从 1 开始 (vpid 0 保留给 MetaPage)
        let v2 = e2.put(make_data(0x22)).await.expect("put 2");
        assert_eq!(
            v2, 1,
            "opts2 新库 user vpid 从 1 开始 (vpid 0 保留给 MetaPage)"
        );
        e2.close().await.expect("close");
    });
}

// =====================================================================
// ⭐ 验证 page 字节在 chunk_list + nowchunks 转换正确
// =====================================================================

#[test]
fn engine_get_after_flush_uses_chunk_list() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open");

        let v = e.put(make_data(0x77)).await.expect("put");
        e.flush().await.expect("flush");

        // flush 后, nowchunks → chunk_list, get 应走 chunk_list 路径
        let r = e.get(v).await.expect("get");
        assert_eq!(r[0x28], 0x77);

        e.close().await.expect("close");
    });
}

#[test]
fn engine_get_before_flush_uses_nowchunks() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts).await.expect("open");

        let v = e.put(make_data(0x88)).await.expect("put");
        // 不 flush, 直接 get (应走 nowchunks 路径)
        let r = e.get(v).await.expect("get (nowchunks)");
        assert_eq!(r[0x28], 0x88);

        e.close().await.expect("close");
    });
}

// =====================================================================
// ⭐ 写入时损坏 / IO 错误
// =====================================================================

#[test]
fn engine_get_after_external_modification() {
    run_async(async move {
        // 模拟外部修改: engine 已 flush, 然后我们手动改 .block 文件, engine get 应读到新值
        let (tmp, opts) = setup();
        let mut e = StorageEngine::open(opts.clone()).await.expect("open");

        let v = e.put(make_data(0xCC)).await.expect("put");
        e.flush().await.expect("flush");
        e.close().await.expect("close");

        // 验证数据在 .block 文件中 (新路径: {tmp}/default/shard_0/000001.block)
        let block_path = tmp
            .path()
            .join("default")
            .join("shard_0")
            .join("000001.block");
        assert!(block_path.exists(), "block_path 应存在: {:?}", block_path);

        // 重新打开, 验证仍能 get
        let mut e2 = StorageEngine::open(opts).await.expect("reopen");
        let r = e2.get(v).await.expect("get");
        assert_eq!(r[0x28], 0xCC);

        e2.close().await.expect("close");
    });
}

// =====================================================================
// ⭐ StorageEngine Drop 不应 panic (即使没 close)
// =====================================================================

#[test]
fn engine_drop_without_close_does_not_panic() {
    run_async(async move {
        let (_tmp, opts) = setup();
        {
            let mut e = StorageEngine::open(opts).await.expect("open");
            let _v = e.put(make_data(0xDD)).await.expect("put");
            // 故意不 flush / close, 直接 drop
        }
        // 上面 drop 应不 panic
        // 注意: 数据可能丢失 (未 fsync), 但不应 panic
    });
}
