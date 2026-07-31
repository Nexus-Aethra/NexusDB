//! T7 recover 集成测试 (DESIGN §4.7 + plan §3.0.3 + T7 实施).
//!
//! 设计要点:
//! - recover 启动时调用, 扫描 block_dir 内所有 `.block` 文件.
//! - 流程: 加载 page.mate → MetaCache 初值 (可能 stale) → scan .block (权威) →
//!   meta_cache.write(vpid, pid) 覆盖 mate 同 vpid 条目 (MetaCache union 语义).
//! - 推导 `next_vpid` = max(seen vpid) + 1.
//! - 推导 `next_file_id` = max(seen file_id) + 1.
//! - 推导 `pid_alloc` 状态: 活跃 block 的最后一个 chunk + page_idx.
//! - skip invalid pages (无 magic 或 page_type 越界).
//!
//! 详见 `docs/superpowers/plans/2026-07-18-storage-crate.md` T7 + §3.0.3.

use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;

use page::{PAGE_MAGIC, PageType};
use storage::PAGE_SIZE;
use storage::alloc::VpidAllocator;
use storage::recover::recover;

mod common;

use common::run_async;

// =====================================================================
// 测试 helper: 写一个带正确 header 的 page 到指定 (file_id, page_idx).
// =====================================================================

/// 写一个带 LCBP magic + type + vpid 的 page 到 block 文件.
fn write_test_page(
    block_path: &Path,
    _file_id: u32,
    page_idx: u64,
    vpid: u64,
    page_type: PageType,
) {
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(block_path)
        .expect("open block for write");
    let mut page = [0u8; PAGE_SIZE];
    page[0..4].copy_from_slice(&PAGE_MAGIC);
    page[4] = page_type as u8;
    page[0x18..0x20].copy_from_slice(&vpid.to_le_bytes());
    let version: u32 = 1;
    page[0x14..0x18].copy_from_slice(&version.to_le_bytes());
    f.write_all_at(&page, page_idx * PAGE_SIZE as u64)
        .expect("write page");
    f.sync_all().expect("sync block");
}

/// 创建 10MB block 文件.
fn make_block(path: &Path) {
    let f = std::fs::File::create(path).expect("create block");
    f.set_len(10 * 1024 * 1024).expect("set_len 10MB");
}

/// 创建 1MB page.mate (空).
fn make_empty_mate(dir: &Path) {
    let path = dir.join("page.mate");
    std::fs::File::create(&path)
        .expect("create mate")
        .write_all(&vec![0u8; 1024 * 1024])
        .expect("write empty mate");
}

/// 写 page.mate 中 vpid → pid 映射 (8B PidLocation, LE).
fn write_mate_entry(mate_path: &Path, vpid: u64, pid_bytes: [u8; 8]) {
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(mate_path)
        .expect("open mate");
    let off = vpid * 8;
    f.write_all_at(&pid_bytes, off).expect("write mate entry");
}

// =====================================================================
// ⭐ 基础: recover 找到 .block 内已写 page
// =====================================================================

#[test]
fn recover_finds_vpids_written_in_block_file() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let block_path = tmp.path().join("000001.block");
        make_block(&block_path);

        for i in 0..5u64 {
            write_test_page(&block_path, 0, i, i, PageType::Leaf);
        }
        make_empty_mate(tmp.path());

        let mut state = recover(tmp.path()).expect("recover ok");

        for v in 0..5u64 {
            let pid = state
                .meta
                .read(v)
                .unwrap_or_else(|| panic!("vpid {} recovered", v));
            assert_eq!(pid.file_id(), 0, "vpid {} → file_id 0", v);
            assert_eq!(pid.chunk_idx(), 0, "vpid {} → chunk 0 (page 0..63)", v);
        }
        assert!(
            state.meta.read(5).is_none(),
            "vpid 5 should not be recovered"
        );
        assert!(state.meta.read(99).is_none());

        assert_eq!(state.next_vpid, 5, "max vpid + 1 = 5");
        assert_eq!(state.next_file_id, 1, "max file_id 0 + 1 = 1");
    });
}

#[test]
fn recover_finds_pages_scattered_across_chunks() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let block_path = tmp.path().join("000001.block");
        make_block(&block_path);

        for i in 0..130u64 {
            write_test_page(&block_path, 0, i, i, PageType::Leaf);
        }

        make_empty_mate(tmp.path());
        let mut state = recover(tmp.path()).expect("recover ok");

        for v in 0..130u64 {
            let pid = state.meta.read(v).unwrap_or_else(|| panic!("vpid {}", v));
            let expected_chunk = (v / 64) as u8;
            assert_eq!(pid.chunk_idx(), expected_chunk, "vpid {} chunk", v);
            assert_eq!(pid.file_id(), 0);
        }
        assert_eq!(state.meta.read(0).unwrap().chunk_idx(), 0);
        assert_eq!(state.meta.read(63).unwrap().chunk_idx(), 0);
        assert_eq!(state.meta.read(64).unwrap().chunk_idx(), 1);
        assert_eq!(state.meta.read(127).unwrap().chunk_idx(), 1);
        assert_eq!(state.meta.read(128).unwrap().chunk_idx(), 2);
        assert_eq!(state.meta.read(129).unwrap().chunk_idx(), 2);

        assert_eq!(state.next_vpid, 130);
        let (fid, chunk, next_page) = state.pid_alloc.current();
        assert_eq!(fid, 0);
        assert_eq!(chunk, 2, "最后一个 chunk 是 2 (写满 2 page)");
        assert_eq!(next_page, 2, "next_page_in_chunk = 2");
    });
}

// =====================================================================
// ⭐ 选最大 block file
// =====================================================================

#[test]
fn recover_picks_largest_block_file() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let block1 = tmp.path().join("000001.block");
        let block2 = tmp.path().join("000002.block");
        make_block(&block1);
        make_block(&block2);

        write_test_page(&block1, 0, 0, 0, PageType::Leaf);
        write_test_page(&block2, 1, 0, 99, PageType::Leaf);

        make_empty_mate(tmp.path());
        let mut state = recover(tmp.path()).expect("recover ok");

        assert!(state.meta.read(0).is_some(), "block 1 的 vpid 0 应被恢复");
        assert!(state.meta.read(99).is_some(), "block 2 的 vpid 99 应被恢复");

        assert_eq!(state.next_file_id, 2);
        assert_eq!(state.next_vpid, 100);
    });
}

#[test]
fn recover_no_block_files_empty_state() {
    let tmp = tempfile::tempdir().unwrap();
    make_empty_mate(tmp.path());

    let state = recover(tmp.path()).expect("recover ok");

    assert_eq!(state.next_vpid, 0);
    assert_eq!(state.next_file_id, 0);
    let (fid, chunk, next_page) = state.pid_alloc.current();
    assert_eq!((fid, chunk, next_page), (0, 0, 0));
}

// =====================================================================
// ⭐ G3 主源切换: mate 有记录以 mate 为准, 扫描仅补缺失 vpid
// =====================================================================

#[test]
fn recover_meta_is_primary_scan_fills_missing() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let block_path = tmp.path().join("000001.block");
        make_block(&block_path);

        for i in 0..5u64 {
            write_test_page(&block_path, 0, i, i, PageType::Leaf);
        }

        make_empty_mate(tmp.path());
        let mate_path = tmp.path().join("page.mate");

        // mate 记录 vpid 0..2 指向 chunk 1 (与磁盘扫描位置 chunk 0 不同).
        // ⭐ G3 语义: chunk 可复用后 "pid 大=新" 不成立, mate 是主源 —
        // 这些 vpid 必须保持 mate 的映射, 不被扫描覆盖.
        for v in 0..3u64 {
            let mate_pid = storage::PidLocation {
                file_id: 0,
                chunk_idx: 1,
                page_idx: v as u16,
                flags: storage::PID_ALIVE,
            };
            write_mate_entry(&mate_path, v, mate_pid.to_bytes());
        }

        let mut state = recover(tmp.path()).expect("recover ok");

        // mate 有记录的 vpid: 以 mate 为准 (chunk 1)
        for v in 0..3u64 {
            let pid = state.meta.read(v).unwrap_or_else(|| panic!("vpid {}", v));
            assert_eq!(pid.chunk_idx(), 1, "vpid {} 以 mate 为准 (主源)", v);
        }
        // mate 缺失的 vpid: 扫描补缺 (chunk 0, crash 窗口新写场景)
        for v in 3..5u64 {
            let pid = state.meta.read(v).unwrap_or_else(|| panic!("vpid {}", v));
            assert_eq!(pid.file_id(), 0);
            assert_eq!(pid.chunk_idx(), 0, "vpid {} 由扫描补缺", v);
        }
        assert!(state.meta.read(5).is_none());
    });
}

#[test]
fn recover_handles_empty_mate() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let block_path = tmp.path().join("000001.block");
        make_block(&block_path);

        for i in 0..3u64 {
            write_test_page(&block_path, 0, i, i, PageType::Leaf);
        }
        make_empty_mate(tmp.path());

        let mut state = recover(tmp.path()).expect("recover ok");
        for v in 0..3u64 {
            assert!(state.meta.read(v).is_some());
        }
        assert_eq!(state.next_vpid, 3);
    });
}

#[test]
fn recover_stops_at_empty_page() {
    run_async(async move {
        // 当前实现 (2026-07-21 修复 T15): 遇到 sparse page (无 magic) **跳过继续**,
        // 因为 .block 文件可能 sparse (chunk 末 page 还没分配).
        // 写 page 0, 1, 跳过 2, 写 page 3 — page 2 跳过, vpid 3 仍会被恢复.
        let tmp = tempfile::tempdir().unwrap();
        let block_path = tmp.path().join("000001.block");
        make_block(&block_path);

        write_test_page(&block_path, 0, 0, 0, PageType::Leaf);
        write_test_page(&block_path, 0, 1, 1, PageType::Leaf);
        // page 2 不写 (空)
        write_test_page(&block_path, 0, 3, 3, PageType::Leaf);

        make_empty_mate(tmp.path());
        let mut state = recover(tmp.path()).expect("recover ok");

        assert!(state.meta.read(0).is_some());
        assert!(state.meta.read(1).is_some());
        // ⭐ 修复后: page 2 empty 被跳过, vpid 3 仍被恢复
        assert!(
            state.meta.read(3).is_some(),
            "page 2 empty 跳过继续, vpid 3 应被恢复"
        );

        // next_vpid = 4 (max=3, +1)
        assert_eq!(state.next_vpid, 4);
    });
}

#[test]
fn recover_stops_at_corrupted_page() {
    run_async(async move {
        // 当前实现 (2026-07-21 修复 T15): 遇到 corrupted page (bad page_type) **跳过继续**.
        // 写 page 0 (valid), page 1 (corrupted: bad page_type), page 2 (valid) — 都应被恢复.
        let tmp = tempfile::tempdir().unwrap();
        let block_path = tmp.path().join("000001.block");
        make_block(&block_path);

        write_test_page(&block_path, 0, 0, 0, PageType::Leaf);

        // 手动写 page 1: magic valid, page_type = 99 (corrupted)
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&block_path)
                .unwrap();
            let mut page = [0u8; PAGE_SIZE];
            page[0..4].copy_from_slice(&PAGE_MAGIC);
            page[4] = 99; // invalid page_type
            page[0x18..0x20].copy_from_slice(&1u64.to_le_bytes());
            f.write_all_at(&page, PAGE_SIZE as u64).unwrap();
            f.sync_all().unwrap();
        }

        // 写 page 2 (valid)
        write_test_page(&block_path, 0, 2, 2, PageType::Leaf);

        make_empty_mate(tmp.path());
        let mut state = recover(tmp.path()).expect("recover ok");

        assert!(state.meta.read(0).is_some());
        // ⭐ 修复后: page 1 corrupted 跳过, vpid 2 仍被恢复
        assert!(
            state.meta.read(2).is_some(),
            "page 1 corrupted 跳过继续, vpid 2 应被恢复"
        );
    });
}

// =====================================================================
// ⭐ pid_alloc 恢复
// =====================================================================

#[test]
fn recover_pid_alloc_state_matches_last_written_page() {
    let tmp = tempfile::tempdir().unwrap();
    let block_path = tmp.path().join("000001.block");
    make_block(&block_path);

    for i in 0..70u64 {
        write_test_page(&block_path, 0, i, i, PageType::Leaf);
    }

    make_empty_mate(tmp.path());
    let state = recover(tmp.path()).expect("recover ok");

    let (fid, chunk, next_page) = state.pid_alloc.current();
    assert_eq!(fid, 0);
    assert_eq!(chunk, 1, "最后一个 chunk 是 1 (page 64..69)");
    assert_eq!(next_page, 6, "chunk 1 已写 6 page (64..69)");
}

#[test]
fn recover_vpid_alloc_continues_from_max() {
    let tmp = tempfile::tempdir().unwrap();
    let block_path = tmp.path().join("000001.block");
    make_block(&block_path);

    for i in 0..10u64 {
        write_test_page(&block_path, 0, i, i, PageType::Leaf);
    }

    make_empty_mate(tmp.path());
    let state = recover(tmp.path()).expect("recover ok");

    // 用 next_vpid 重建 VpidAllocator, 验证从 max+1 开始
    let mut alloc = VpidAllocator::new(state.next_vpid);
    let mut meta = state.meta;

    let new_vpid = alloc.alloc(&mut meta);
    assert_eq!(new_vpid, 10, "recover 后 vpid 从 max+1 开始");
    let new_vpid2 = alloc.alloc(&mut meta);
    assert_eq!(new_vpid2, 11);
}

// =====================================================================
// ⭐ recover 后分配器满 chunk 行为
// =====================================================================

#[test]
fn recover_full_chunk_marks_pid_alloc_as_full() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let block_path = tmp.path().join("000001.block");
        make_block(&block_path);

        for i in 0..64u64 {
            write_test_page(&block_path, 0, i, i, PageType::Leaf);
        }
        make_empty_mate(tmp.path());
        let state = recover(tmp.path()).expect("recover ok");

        let (fid, chunk, next_page) = state.pid_alloc.current();
        assert_eq!(
            (fid, chunk, next_page),
            (0, 0, 64),
            "chunk 0 写满 64 page, next_page_in_chunk = 64"
        );

        // PidAllocator::alloc 在 chunk 满时应返回 None
        let mut pid_alloc = state.pid_alloc;
        let mut meta = state.meta;
        let result = pid_alloc.alloc();
        assert!(result.is_none(), "chunk 满时 alloc 返回 None");
        // 仍能继续读 meta
        let _ = meta.read(0);
    });
}

// =====================================================================
// ⭐ Phase B: pid.state 快速路径 (2026-07-26)
// =====================================================================

/// pid.state roundtrip: flush 写入水位 → recover 时优先采用 (取与扫描的较大值).
#[test]
fn pid_state_fast_path_roundtrip() {
    run_async(async move {
        use storage::{OpenOptions, StorageEngine};
        let tmp = tempfile::tempdir().unwrap();
        let opts = OpenOptions {
            block_root: tmp.path().to_path_buf(),
            block_dir: None,
            db_name: Some("default".to_string()),
            shard_id: 0,
            create_if_missing: true,
            chunk_cache_size: 4,
            io_backend: storage::IoBackend::StdFs,
            io_config: storage::IoBackendConfig::default(),
            wal_mode: Default::default(),
        };
        {
            let mut e = StorageEngine::open(opts.clone()).await.unwrap();
            e.create_db("app").await.unwrap();
            e.create_table("app", "kv").await.unwrap();
            for i in 0..50u32 {
                let k = format!("k{i:04}");
                e.table_put("app", "kv", k.as_bytes(), b"v").await.unwrap();
            }
            e.flush().await.unwrap();
        }
        // pid.state 应已写入 (default db 的 shard 目录; app db 各自独立)
        let ps = tmp
            .path()
            .join("default")
            .join("shard_0")
            .join("pid.state");
        assert!(ps.exists(), "pid.state must be persisted on flush: {ps:?}");
        assert_eq!(std::fs::metadata(&ps).unwrap().len(), 8, "8B PidLocation");

        // reopen: 数据完整 (pid.state 与扫描一致, 取 max 不回退)
        let mut opts2 = opts;
        opts2.create_if_missing = false;
        let mut e2 = StorageEngine::open(opts2).await.unwrap();
        for i in 0..50u32 {
            let k = format!("k{i:04}");
            let got = e2.table_get("app", "kv", k.as_bytes()).await.unwrap();
            assert_eq!(got.as_deref(), Some(b"v".as_slice()), "key {k} after reopen");
        }
        // 新写入不得覆盖旧数据 (pid 水位正确前进)
        e2.table_put("app", "kv", b"new_key", b"new_v").await.unwrap();
        assert_eq!(
            e2.table_get("app", "kv", b"k0000").await.unwrap().as_deref(),
            Some(b"v".as_slice())
        );
    });
}
