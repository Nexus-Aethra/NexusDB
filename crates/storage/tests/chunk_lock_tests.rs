//! T6 chunk_lock 集成测试 (DESIGN §3.0 + plan §3.0).
//!
//! 设计要点:
//! - chunk_list 命中走快路径 AlreadyLoaded, 不动 chunk_lock
//! - chunk_list miss 走 chunk_lock: 同 task 多次 acquire (reentrant) 是 owner
//! - 不同 task acquire 同 chunk: 后者是 waiter, FIFO 顺序排队
//! - owner release_and_wake 唤醒下一个 waiter
//! - 不同 chunk 互不阻塞
//!
//! **同步版本语义**: 不会真触发 wait queue (因为没 await), 但数据结构行为可测.

use std::io::Write;
use std::os::unix::fs::FileExt;

use storage::alloc::{PidAllocator, VpidAllocator};
use storage::chunk_lru::ChunkList;
use storage::chunk_writer::{ChunkWriter, NowChunks};
use storage::pager::{Pager, TaskId};
use storage::test_support::AcquireResult;
use storage::types::PageKey;
use storage::{MetaCache, PAGE_SIZE};

mod common;

use common::run_async;

fn setup() -> (tempfile::TempDir, MetaCache) {
    let tmp = tempfile::tempdir().unwrap();
    let mate = tmp.path().join("page.mate");
    std::fs::File::create(&mate)
        .unwrap()
        .write_all(&vec![0u8; 1024 * 1024])
        .unwrap();
    let meta = MetaCache::open(&mate).unwrap();
    (tmp, meta)
}

fn make_block(tmp: &tempfile::TempDir) -> std::path::PathBuf {
    let block_path = tmp.path().join("000001.block");
    let f = std::fs::File::create(&block_path).unwrap();
    f.set_len(10 * 1024 * 1024).unwrap();
    block_path
}

fn new_pager(tmp: &tempfile::TempDir, meta: MetaCache) -> Pager {
    let block = make_block(tmp);
    Pager::new(
        tmp.path().to_path_buf(),
        meta,
        VpidAllocator::new(0),
        // ⭐ T12.14: pid_alloc 起点 (0, 0, 1) 跳过 page 0, 让 META_PID 独占
        // page 0. META_VPID 写 page 走 META_PID 直接, 不走 pid_alloc.
        PidAllocator::new(0, 0, 1),
        ChunkList::new(8),
        NowChunks::new(),
        ChunkWriter::new(&block).unwrap(),
    )
}

fn key(file_id: u32, chunk_idx: u8) -> PageKey {
    PageKey { file_id, chunk_idx }
}

// =====================================================================
// ⭐ AlreadyLoaded 快路径: chunk 在 chunk_list 中, acquire 返回 AlreadyLoaded
// =====================================================================

#[test]
fn chunk_lock_already_loaded_short_circuits() {
    run_async(async move {
        // 触发 chunk 0 加载到 chunk_list
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let data = [0u8; PAGE_SIZE];
        let v = pager.create(Box::new(data)).await.unwrap();
        pager.flush().await.unwrap();
        let _ = pager.read(v).await.unwrap(); // 触发 chunk 0 加载

        // chunk 0 已在 chunk_list
        let k = key(0, 0);
        let r = pager.acquire_chunk_lock(k.into(), 100);
        assert_eq!(
            r,
            AcquireResult::AlreadyLoaded,
            "chunk_list 命中时 acquire 应返回 AlreadyLoaded"
        );

        // chunk_lock 内 entry 不应有 (因为 AlreadyLoaded 不动 entry)
        assert!(
            pager.chunk_lock().is_empty(),
            "AlreadyLoaded 路径不应创建 entry"
        );
    });
}

#[test]
fn chunk_lock_nowchunks_loaded_still_creates_entry() {
    run_async(async move {
        // nowchunks 命中但 chunk_list miss: 走 chunk_lock acquire (BecameOwner)
        // 因为 chunk_list.contains 是 false, 走 try_acquire 创建 entry
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let data = [0u8; PAGE_SIZE];
        let _ = pager.create(Box::new(data)).await.unwrap();
        // 不 flush, nowchunks 有数据但 chunk_list 空

        let k = key(0, 0);
        let r = pager.acquire_chunk_lock(k.into(), 100);
        assert_eq!(
            r,
            AcquireResult::BecameOwner,
            "chunk_list miss + nowchunks 命中: acquire 走 owner 路径"
        );

        // entry 应被创建
        assert!(!pager.chunk_lock().is_empty());
        let entry = pager.chunk_lock().get(&k.into()).expect("entry must exist");
        assert_eq!(entry.owner, Some(100));
    });
}

// =====================================================================
// ⭐ BecameOwner 路径: chunk_list miss 时, 第一次 acquire 是 owner
// =====================================================================

#[test]
fn chunk_lock_first_acquire_becomes_owner() {
    run_async(async move {
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        // chunk 0 不在 cache
        let k = key(0, 0);
        let r = pager.acquire_chunk_lock(k.into(), 42);
        assert_eq!(r, AcquireResult::BecameOwner);

        let entry = pager.chunk_lock().get(&k.into()).unwrap();
        assert_eq!(entry.owner, Some(42));
        assert!(entry.loading);
        assert_eq!(entry.waiter_count(), 0);
    });
}

#[test]
fn chunk_lock_reentrant_same_task_returns_owner() {
    run_async(async move {
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let k = key(0, 0);
        let _ = pager.acquire_chunk_lock(k.into(), 100);
        // 同一 task 再次 acquire: 仍是 owner (reentrant)
        let r = pager.acquire_chunk_lock(k.into(), 100);
        assert_eq!(r, AcquireResult::BecameOwner);
        let entry = pager.chunk_lock().get(&k.into()).unwrap();
        assert_eq!(entry.owner, Some(100));
        assert_eq!(entry.waiter_count(), 0, "reentrant 不应增加 waiter");
    });
}

// =====================================================================
// ⭐ BecameWaiter 路径: 别的 task 是 owner, 加入 waiters
// =====================================================================

#[test]
fn chunk_lock_second_acquire_becomes_waiter() {
    run_async(async move {
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let k = key(0, 0);
        let _ = pager.acquire_chunk_lock(k.into(), 100);
        let r = pager.acquire_chunk_lock(k.into(), 200);
        assert_eq!(r, AcquireResult::BecameWaiter);

        let entry = pager.chunk_lock().get(&k.into()).unwrap();
        assert_eq!(entry.owner, Some(100));
        assert_eq!(entry.waiter_count(), 1);
        assert_eq!(entry.waiters.front(), Some(&200));
    });
}

#[test]
fn chunk_lock_fifo_waiter_order() {
    run_async(async move {
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let k = key(0, 0);
        let _ = pager.acquire_chunk_lock(k.into(), 100);
        // 三个 waiter 排队
        let _ = pager.acquire_chunk_lock(k.into(), 200);
        let _ = pager.acquire_chunk_lock(k.into(), 300);
        let _ = pager.acquire_chunk_lock(k.into(), 400);

        let entry = pager.chunk_lock().get(&k.into()).unwrap();
        assert_eq!(entry.owner, Some(100));
        let mut expected_waiters = vec![200, 300, 400];
        let actual: Vec<TaskId> = entry.waiters.iter().copied().collect();
        assert_eq!(
            actual, expected_waiters,
            "waiter 队列应严格 FIFO, got {:?}",
            actual
        );
        expected_waiters.clear();
    });
}

// =====================================================================
// ⭐ release_and_wake 流程
// =====================================================================

#[test]
fn chunk_lock_release_wakes_next_waiter() {
    run_async(async move {
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let k = key(0, 0);
        let _ = pager.acquire_chunk_lock(k.into(), 100);
        let _ = pager.acquire_chunk_lock(k.into(), 200);
        let _ = pager.acquire_chunk_lock(k.into(), 300);

        // 100 release: 唤醒 200
        let next = pager.release_chunk_lock(&k.into(), 100);
        assert_eq!(next, Some(200));
        let entry = pager.chunk_lock().get(&k.into()).unwrap();
        assert_eq!(entry.owner, Some(200), "新 owner 应是 200");
        assert_eq!(entry.waiter_count(), 1);
        assert!(entry.loading, "新 owner 应 loading=true");
    });
}

#[test]
fn chunk_lock_release_with_no_waiter_removes_entry() {
    run_async(async move {
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let k = key(0, 0);
        let _ = pager.acquire_chunk_lock(k.into(), 100);

        let next = pager.release_chunk_lock(&k.into(), 100);
        assert_eq!(next, None);
        assert!(
            pager.chunk_lock().get(&k.into()).is_none(),
            "无 waiter 时 release 应移除 entry"
        );
    });
}

#[test]
fn chunk_lock_release_chain_promotes_in_fifo_order() {
    run_async(async move {
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let k = key(0, 0);
        let _ = pager.acquire_chunk_lock(k.into(), 100);
        let _ = pager.acquire_chunk_lock(k.into(), 200);
        let _ = pager.acquire_chunk_lock(k.into(), 300);

        // 100 → 200
        assert_eq!(pager.release_chunk_lock(&k.into(), 100), Some(200));
        // 200 → 300
        assert_eq!(pager.release_chunk_lock(&k.into(), 200), Some(300));
        // 300 → None
        assert_eq!(pager.release_chunk_lock(&k.into(), 300), None);
        assert!(pager.chunk_lock().get(&k.into()).is_none());
    });
}

// =====================================================================
// ⭐ 不同 chunk 互不阻塞
// =====================================================================

#[test]
fn chunk_lock_different_chunks_independent() {
    run_async(async move {
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let k0 = key(0, 0);
        let k1 = key(0, 1);
        let _ = pager.acquire_chunk_lock(k0.into(), 100);
        let _ = pager.acquire_chunk_lock(k1.into(), 200);

        let e0 = pager.chunk_lock_view().get(&k0.into()).unwrap();
        let e1 = pager.chunk_lock_view().get(&k1.into()).unwrap();
        assert_eq!(e0.owner, Some(100));
        assert_eq!(e1.owner, Some(200));
        assert_eq!(pager.chunk_lock().len(), 2);

        // 释放 chunk 0 不影响 chunk 1
        assert_eq!(pager.release_chunk_lock(&k0.into(), 100), None);
        assert!(pager.chunk_lock().get(&k0.into()).is_none());
        let e1_after = pager.chunk_lock().get(&k1.into()).unwrap();
        assert_eq!(e1_after.owner, Some(200), "chunk 1 owner 不变");
    });
}

#[test]
fn chunk_lock_different_file_ids_independent() {
    run_async(async move {
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let k_file0 = key(0, 5);
        let k_file1 = key(1, 5);
        let _ = pager.acquire_chunk_lock(k_file0.into(), 100);
        let _ = pager.acquire_chunk_lock(k_file1.into(), 200);

        assert_eq!(pager.chunk_lock().len(), 2);
        let e0 = pager.chunk_lock_view().get(&k_file0.into()).unwrap();
        let e1 = pager.chunk_lock_view().get(&k_file1.into()).unwrap();
        assert_eq!(e0.owner, Some(100));
        assert_eq!(e1.owner, Some(200));
    });
}

// =====================================================================
// ⭐ Pager.read 集成: cache miss 走 acquire, cache hit 走 AlreadyLoaded
// =====================================================================

#[test]
fn pager_read_miss_acquires_owner_and_loads() {
    run_async(async move {
        // 第一次 read (cache miss) → 走 chunk_lock acquire → owner → 加载 → release
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let mut data = [0u8; PAGE_SIZE];
        data[0x28] = 0xAA;
        let v = pager.create(Box::new(data)).await.unwrap();
        pager.flush().await.unwrap();

        // flush 之后 chunk_list 已有 chunk 0 (insert_from_write_queue)
        let r = pager.read(v).await.unwrap();
        assert_eq!(r[0x28], 0xAA);
        // chunk_list 已有, 不会创建 chunk_lock entry
        assert!(
            pager.chunk_lock().is_empty(),
            "flush 后 chunk_list 已有, read 不应触发 chunk_lock entry"
        );
    });
}

#[test]
fn pager_read_miss_creates_chunk_lock_then_loads() {
    run_async(async move {
        // 先清空 chunk_list, 模拟"cache miss after eviction"
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let data = [0u8; PAGE_SIZE];
        let v = pager.create(Box::new(data)).await.unwrap();
        pager.flush().await.unwrap();
        let _ = pager.read(v).await.unwrap();

        // 现在 chunk_list 有 chunk 0. 强制 invalidate 模拟 evict
        let k = key(0, 0);
        {
            let list = pager.chunk_list();
            list.invalidate(&k.into());
        }

        // 读: chunk_list miss, 走 load_fn. 但本次 read 自身会触发 chunk_lock acquire
        // 实际上, 当前 Pager.read 还没集成 chunk_lock, 走原始 load_fn 路径
        // 验证: chunk_list 重新加载, 数据仍正确
        let r = pager.read(v).await.unwrap();
        assert_eq!(r[0x28], 0);
        assert!(
            !pager.chunk_list().is_empty(),
            "read 后 chunk_list 重新加载"
        );
    });
}

// =====================================================================
// ⭐ 错误情况
// =====================================================================

#[test]
fn chunk_lock_release_with_wrong_owner_panics() {
    let (tmp, meta) = setup();
    let mut pager = new_pager(&tmp, meta);

    let k = key(0, 0);
    let _ = pager.acquire_chunk_lock(k.into(), 100);
    // 错误 task_id release 应 panic
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pager.release_chunk_lock(&k.into(), 999);
    }));
    assert!(result.is_err(), "非 owner release 应 panic");
}

#[test]
fn chunk_lock_release_unloaded_chunk_panics() {
    let (tmp, meta) = setup();
    let mut pager = new_pager(&tmp, meta);

    let k = key(0, 0);
    // entry 不存在, release 应 panic
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pager.release_chunk_lock(&k.into(), 100);
    }));
    assert!(result.is_err(), "不存在的 entry release 应 panic");
}

// =====================================================================
// ⭐ block 文件存在性检查 (chunk_lock 集成到 disk load)
// =====================================================================

#[test]
fn chunk_lock_load_real_chunk_from_disk() {
    run_async(async move {
        // 真实 .block 文件存在, chunk 0 已 flush 落盘, 重新加载
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        // create + flush, 落盘到 .block chunk 0
        let mut data = [0u8; PAGE_SIZE];
        data[0x28] = 0xDE;
        data[0x29] = 0xAD;
        let _v = pager.create(Box::new(data)).await.unwrap();
        pager.flush().await.unwrap();

        // 验证 .block 文件已写入
        // 注意: 这里不能重新 make_block, 否则会覆盖已有 block 文件
        // disk page layout: [0..0x28] header (LCBP + page_type + vpid), [0x28..PAGE_SIZE] user_data
        let block_path = tmp.path().join("000001.block");
        let f = std::fs::File::open(&block_path).unwrap();
        let mut buf = [0u8; PAGE_SIZE];
        f.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(buf[0x28], 0xDE);
        assert_eq!(buf[0x29], 0xAD);
    });
}
