//! T3 alloc 测试: VpidAllocator / PidAllocator / FreePageQueue.
//!
//! 设计 (来自 plan §3.3 + 用户敲定):
//! - **单线程 UnsafeCell** (per-shard thread)
//! - VpidAllocator: free list 复用, 但 next_vpid 单调递增 (永不重用)
//! - PidAllocator: chunk 满 (== 64 page) 时返回 None, caller 触发 rotate
//! - FreePageQueue: 单线程 LIFO 栈

use std::io::Write;
use std::path::Path;

use storage::test_support::PidLocation;
use storage::{MetaCache, PID_ALIVE, alloc};

fn make_mate(p: &Path) {
    let mut f = std::fs::File::create(p).unwrap();
    f.write_all(&vec![0u8; 1024 * 1024]).unwrap();
    f.sync_all().unwrap();
}

#[test]
fn vpid_alloc_returns_monotonic_ids() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("page.mate");
    make_mate(&path);
    let mut meta = MetaCache::open(&path).unwrap();
    let mut alloc = alloc::VpidAllocator::new(0);
    assert_eq!(alloc.alloc(&mut meta), 0);
    assert_eq!(alloc.alloc(&mut meta), 1);
    assert_eq!(alloc.alloc(&mut meta), 2);
    assert_eq!(alloc.current(), 3);
}

#[test]
fn vpid_free_reuses_from_free_list_lifo() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("page.mate");
    make_mate(&path);
    let mut meta = MetaCache::open(&path).unwrap();
    let mut alloc = alloc::VpidAllocator::new(0);
    let a = alloc.alloc(&mut meta);
    let b = alloc.alloc(&mut meta);
    let c = alloc.alloc(&mut meta);
    assert_eq!(alloc.free_count(), 0);

    alloc.free(b, &mut meta);
    assert_eq!(alloc.free_count(), 1);

    // LIFO: 重新 alloc 拿到刚才 free 的 b
    let reused = alloc.alloc(&mut meta);
    assert_eq!(reused, b);
    assert_eq!(alloc.free_count(), 0);

    // free list 空后, next 是 c + 1 (继续单调递增)
    assert_eq!(alloc.alloc(&mut meta), c + 1);
    let _ = a;
}

#[test]
fn vpid_free_to_empty_list_is_just_appended() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("page.mate");
    make_mate(&path);
    let mut meta = MetaCache::open(&path).unwrap();
    let mut alloc = alloc::VpidAllocator::new(0);

    // 分配 3 个, 然后全部 free
    let _a = alloc.alloc(&mut meta);
    let _b = alloc.alloc(&mut meta);
    let _c = alloc.alloc(&mut meta);
    assert_eq!(alloc.current(), 3);

    alloc.free(1, &mut meta);
    assert_eq!(alloc.free_count(), 1);

    let _r1 = alloc.alloc(&mut meta);
    assert_eq!(alloc.free_count(), 0);
    // monotonic 保持 next_vpid 不变
    assert_eq!(alloc.current(), 3);
}

#[test]
fn pid_alloc_returns_sequential_page_idx_until_chunk_full() {
    let mut pid_alloc = alloc::PidAllocator::new(0, 0, 0);
    for i in 0..64u16 {
        let pid = pid_alloc.alloc().expect("chunk should not be full");
        assert_eq!(pid.file_id(), 0);
        assert_eq!(pid.chunk_idx(), 0);
        assert_eq!(pid.page_idx(), i);
        assert_eq!(pid.flags(), PID_ALIVE);
    }
    // 第 65 次返回 None (chunk 满, caller 触发 rotate)
    assert!(pid_alloc.alloc().is_none());
}

#[test]
fn pid_alloc_after_rotate_to_new_chunk() {
    let mut pid_alloc = alloc::PidAllocator::new(0, 0, 0);
    for _ in 0..64 {
        pid_alloc.alloc().unwrap();
    }
    assert!(pid_alloc.alloc().is_none());

    pid_alloc.rotate_to(0, 1);
    let pid = pid_alloc.alloc().unwrap();
    assert_eq!(pid.chunk_idx(), 1);
    assert_eq!(pid.page_idx(), 0);

    // 跨 file_id 的 rotate
    pid_alloc.rotate_to(7, 0);
    let pid = pid_alloc.alloc().unwrap();
    assert_eq!(pid.file_id(), 7);
}

#[test]
fn pid_alloc_current_snapshot() {
    let mut pid_alloc = alloc::PidAllocator::new(2, 3, 5);
    let (f, c, p) = pid_alloc.current();
    assert_eq!(f, 2);
    assert_eq!(c, 3);
    assert_eq!(p, 5);

    pid_alloc.alloc().unwrap();
    let (f, c, p) = pid_alloc.current();
    assert_eq!(f, 2);
    assert_eq!(c, 3);
    assert_eq!(p, 6);
}

#[test]
fn free_page_queue_lifo() {
    let mut q = alloc::FreePageQueue::new();
    assert!(q.is_empty());
    assert_eq!(q.len(), 0);

    q.push(5);
    q.push(10);
    q.push(3);
    assert!(!q.is_empty());
    assert_eq!(q.len(), 3);

    assert_eq!(q.pop(), Some(3)); // LIFO
    assert_eq!(q.pop(), Some(10));
    assert_eq!(q.pop(), Some(5));
    assert!(q.is_empty());
    assert!(q.pop().is_none());
}

#[test]
fn free_page_queue_dedup_recording_via_chunk_helper() {
    // 实际使用: chunk 切换时旧 page 进 FreePageQueue, 新 chunk 优先从 queue 取
    let mut q = alloc::FreePageQueue::new();
    q.push(0); // 旧 chunk page 0
    q.push(1);
    q.push(2);

    // 新 chunk 优先复用一个
    assert_eq!(q.pop(), Some(2));
    assert_eq!(q.pop(), Some(1));
    assert_eq!(q.pop(), Some(0));
}

#[test]
fn pid_location_construction_in_alloc_test() {
    // 简单 sanity 测试, PidLocation 字段读取正确
    let pid = PidLocation::from_bytes(&[1, 0, 0, 0, 2, 0, 0, PID_ALIVE]);
    assert_eq!(pid.file_id(), 1);
    assert_eq!(pid.chunk_idx(), 2);
    assert_eq!(pid.page_idx(), 0);
    assert_eq!(pid.flags(), PID_ALIVE);
}

// =====================================================================
// ⭐ 增强稳定性测试: alloc 边界 / 大数 / 跨 chunk
// =====================================================================

#[test]
fn vpid_alloc_high_initial_value() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("page.mate");
    make_mate(&path);
    let mut meta = MetaCache::open(&path).unwrap();
    let mut a = alloc::VpidAllocator::new(1_000_000);
    assert_eq!(a.alloc(&mut meta), 1_000_000);
    assert_eq!(a.alloc(&mut meta), 1_000_001);
    assert_eq!(a.current(), 1_000_002);
}

#[test]
fn vpid_alloc_free_many_then_alloc_recycles() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("page.mate");
    make_mate(&path);
    let mut meta = MetaCache::open(&path).unwrap();
    let mut a = alloc::VpidAllocator::new(0);

    // 分配 100 个, free 后 50 个, alloc 100 次应拿到 free 的 50 个 + 新 50 个
    let mut vpids = Vec::new();
    for _ in 0..100 {
        vpids.push(a.alloc(&mut meta));
    }
    assert_eq!(a.current(), 100);

    // free 中间 50 个 (50..100)
    for vpid in vpids[50..100].iter() {
        a.free(*vpid, &mut meta);
    }
    assert_eq!(a.free_count(), 50);
    assert_eq!(a.current(), 100, "current() shouldn't decrement after free");

    // 再 alloc 100 个: 先 LIFO 复用 50 个 free, 再 next_vpid 100..150
    let mut re_alloced = Vec::new();
    for _ in 0..100 {
        re_alloced.push(a.alloc(&mut meta));
    }
    assert_eq!(a.current(), 150, "next_vpid 自增 50 次");
    assert_eq!(a.free_count(), 0);

    // 验证前 50 个是回收的 (顺序 LIFO: 99, 98, 97, ...)
    for i in 0..50 {
        assert_eq!(re_alloced[i], vpids[99 - i], "LIFO 顺序: index {}", i);
    }
    // 后 50 个是新分配的
    for vpid in re_alloced.iter().take(100).skip(50) {
        assert!(*vpid >= 100, "new vpids are >= 100, got {}", vpid);
    }
}

#[test]
fn pid_alloc_high_file_id() {
    let mut pid_alloc = alloc::PidAllocator::new(u32::MAX, 9, 0);
    let pid = pid_alloc.alloc().unwrap();
    assert_eq!(pid.file_id(), u32::MAX);
    assert_eq!(pid.chunk_idx(), 9);
}

#[test]
fn pid_alloc_after_full_cycle() {
    // 写满 chunk 0, rotate, 再写满, rotate
    let mut pid_alloc = alloc::PidAllocator::new(0, 0, 0);
    for _ in 0..64 {
        pid_alloc.alloc().unwrap();
    }
    assert!(pid_alloc.alloc().is_none());
    pid_alloc.rotate_to(0, 1);
    for _ in 0..64 {
        pid_alloc.alloc().unwrap();
    }
    assert!(pid_alloc.alloc().is_none());
    pid_alloc.rotate_to(5, 7); // 跨 file_id
    let pid = pid_alloc.alloc().unwrap();
    assert_eq!(pid.file_id(), 5);
    assert_eq!(pid.chunk_idx(), 7);
    assert_eq!(pid.page_idx(), 0);
}

#[test]
fn pid_alloc_partial_rotate_mid_chunk() {
    // next_page_in_chunk = 30, alloc 30 次后 rotate 切到下一 chunk
    let mut pid_alloc = alloc::PidAllocator::new(0, 0, 30);
    for i in 30..64 {
        let pid = pid_alloc.alloc().unwrap();
        assert_eq!(pid.page_idx(), i);
    }
    assert!(pid_alloc.alloc().is_none());
    pid_alloc.rotate_to(0, 1);
    let pid = pid_alloc.alloc().unwrap();
    assert_eq!(pid.page_idx(), 0);
}

#[test]
fn free_page_queue_clear_resets() {
    let mut q = alloc::FreePageQueue::new();
    q.push(1);
    q.push(2);
    q.push(3);
    assert_eq!(q.len(), 3);
    q.clear();
    assert!(q.is_empty());
    assert_eq!(q.len(), 0);
    // 仍可继续用
    q.push(42);
    assert_eq!(q.pop(), Some(42));
}

#[test]
fn free_page_queue_large_push_pop() {
    // FreePageQueue 限制 page_idx < 64 (chunk 内), 所以用 mod 64
    let mut q = alloc::FreePageQueue::new();
    for i in 0..1000u32 {
        q.push((i % 64) as u16);
    }
    assert_eq!(q.len(), 1000);
    let mut count = 0;
    while let Some(_v) = q.pop() {
        count += 1;
    }
    assert_eq!(count, 1000);
}

#[test]
fn alloc_chained_operations_dont_panic() {
    // 多次 alloc / free / alloc 不应触发 use-after-free 或 panic
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("page.mate");
    make_mate(&path);
    let mut meta = MetaCache::open(&path).unwrap();

    let mut vpid_alloc = alloc::VpidAllocator::new(0);
    let mut pid_alloc = alloc::PidAllocator::new(0, 0, 0);
    let mut freeq = alloc::FreePageQueue::new();

    for round in 0..10 {
        let _vpid = vpid_alloc.alloc(&mut meta);
        let pid = pid_alloc.alloc().unwrap_or_else(|| {
            // chunk 满, rotate
            pid_alloc.rotate_to(0, ((round / 10) % 10) as u8 + 1);
            pid_alloc.alloc().unwrap()
        });
        assert!(pid.flags() & PID_ALIVE != 0);
        // 不验证 vpid == round, 因为 alloc 后立即 free + 再 alloc 时会复用 free list
        vpid_alloc.free(_vpid, &mut meta);
        freeq.push(pid.page_idx());
    }
    // next_vpid 不自减: 第一轮 alloc 后 free 立即回 free list,
    // 第二轮 alloc 复用 free list (next_vpid 不变). 10 轮结束 next_vpid 应是 1.
    assert_eq!(
        vpid_alloc.current(),
        1,
        "next_vpid 不自减 (alloc→free 复用)"
    );
    assert_eq!(freeq.len(), 10);
}
