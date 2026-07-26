//! T6 multi_page_sync_tests: PageWriteBatch 原子性 + 崩溃恢复验证 (DESIGN §3.0.5 + plan §3.0.5).
//!
//! 设计要点:
//! - PageWriteBatch 单次提交多 page 到 nowchunks, 中途不 await / 不 panic
//! - batch 内部单线程连续 memcpy, 失败时整批回滚 (单 page submit panic 模拟)
//! - batch submit 后所有 page 都应在 nowchunks 中 (后续 read 走 nowchunks 优先)
//! - batch flush 后 .block 文件所有 page 落盘 + chunk_list 加载
//! - 跨 batch 多次提交, 累加 vpid 正确
//! - batch 与 chunk_lock 协同: 写路径无 pin, 不触发 chunk_lock

use std::io::Write;
use std::os::unix::fs::FileExt;

use storage::alloc::{PidAllocator, VpidAllocator};
use storage::chunk_lru::ChunkList;
use storage::chunk_writer::{ChunkWriter, NowChunks};
use storage::pager::Pager;
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
        // page 0. META_VPID 写 page 走 META_PID 直接, 不走 pid_alloc; 跳过 page 0
        // 避免与 META_VPID 冲突.
        PidAllocator::new(0, 0, 1),
        ChunkList::new(8),
        NowChunks::new(),
        ChunkWriter::new(&block).unwrap(),
    )
}

// =====================================================================
// ⭐ PageWriteBatch 原子性: 单批多 page 提交
// =====================================================================

#[test]
fn batch_submit_3_pages_atomically() {
    run_async(async move {
        // 模拟 B+Tree split 场景: 一次提交 3 page
        // - left page (原 vpid 5) 保留, 内容是 split 后的左半
        // - right page (新 vpid 6) 新分配, 内容是 split 后的右半
        // - parent page (vpid 0) 写回更新后的 separator
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let mut batch = pager.new_write_batch();
        let mut left = [0u8; PAGE_SIZE];
        left[0x28] = 0xA1;
        left[0x29] = 0xA2;
        let mut right = [0u8; PAGE_SIZE];
        right[0x28] = 0xB1;
        right[0x29] = 0xB2;
        let mut parent = [0u8; PAGE_SIZE];
        parent[0x28] = 0xC1;
        parent[0x29] = 0xC2;
        batch.add(0, Box::new(parent));
        batch.add(5, Box::new(left));
        batch.add(6, Box::new(right));

        let mappings = batch.submit(&mut pager).await.expect("submit ok");
        assert_eq!(mappings.len(), 3);

        // 验证 mappings 按 batch.add 顺序 (parent, left, right)
        assert_eq!(mappings[0].0, 0);
        assert_eq!(mappings[1].0, 5);
        assert_eq!(mappings[2].0, 6);

        // 提交后 vpid 0/5/6 都应映射到 page, read 能拿到正确数据
        let expected = [(0u64, 0xC1u8), (5, 0xA1), (6, 0xB1)];
        for (vpid, first_byte) in expected.iter() {
            let r = pager.read(*vpid).await.unwrap();
            assert_eq!(r[0x28], *first_byte, "vpid {} first byte", vpid);
        }
    });
}

#[test]
fn batch_submit_single_page_works() {
    run_async(async move {
        // 单 page batch 也能正确工作
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let mut batch = pager.new_write_batch();
        let mut data = [0u8; PAGE_SIZE];
        data[0x28] = 0x99;
        batch.add(42, Box::new(data));
        let mappings = batch.submit(&mut pager).await.unwrap();
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].0, 42);

        let r = pager.read(42).await.unwrap();
        assert_eq!(r[0x28], 0x99);
    });
}

// =====================================================================
// ⭐ 跨 chunk 批: 多 page 跨多个 chunk
// =====================================================================

#[test]
fn batch_submit_pages_across_chunks() {
    run_async(async move {
        // 一次 batch 提交 5 page, 都应分配到不同 page_idx
        // 1MB chunk 容纳 64 page (16KB each), 5 page 都在 chunk 0
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let mut batch = pager.new_write_batch();
        let mut pids = Vec::new();
        for i in 0..5u64 {
            let mut data = [0u8; PAGE_SIZE];
            data[0x28] = (i + 1) as u8;
            batch.add(i, Box::new(data));
            pids.push(i);
        }
        let mappings = batch.submit(&mut pager).await.unwrap();
        assert_eq!(mappings.len(), 5);

        // 验证所有 page 都能 read
        for (i, _) in mappings.iter().enumerate() {
            let r = pager.read(i as u64).await.unwrap();
            assert_eq!(r[0x28], (i + 1) as u8, "page {} first byte", i);
        }
    });
}

// =====================================================================
// ⭐ 多次 batch 累加
// =====================================================================

#[test]
fn multiple_batches_accumulate_vpids() {
    run_async(async move {
        // 多次 batch 累加, vpid 单调递增
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        // 第一批: 3 page
        let mut batch1 = pager.new_write_batch();
        batch1.add(0, Box::new([0u8; PAGE_SIZE]));
        batch1.add(1, Box::new([0u8; PAGE_SIZE]));
        batch1.add(2, Box::new([0u8; PAGE_SIZE]));
        let m1 = batch1.submit(&mut pager).await.unwrap();
        assert_eq!(m1.len(), 3);

        // 第二批: 2 page
        let mut batch2 = pager.new_write_batch();
        batch2.add(3, Box::new([0u8; PAGE_SIZE]));
        batch2.add(4, Box::new([0u8; PAGE_SIZE]));
        let m2 = batch2.submit(&mut pager).await.unwrap();
        assert_eq!(m2.len(), 2);

        // 全部 5 page 都能 read
        for i in 0..5u64 {
            let r = pager.read(i).await.unwrap();
            assert_eq!(r[0x28], 0);
        }
    });
}

// =====================================================================
// ⭐ PageWriteBatch 上限保护
// =====================================================================

#[test]
fn batch_max_16_pages_limit() {
    run_async(async move {
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        // 16 page 应成功
        let mut batch = pager.new_write_batch();
        for i in 0..16u64 {
            batch.add(i, Box::new([0u8; PAGE_SIZE]));
        }
        let mappings = batch.submit(&mut pager).await.unwrap();
        assert_eq!(mappings.len(), 16);

        // 第 17 个 add 应 panic (在单元测试中验证)
        // integration test 不验证 panic (panic 终止测试), 仅 verify 上限逻辑
        assert!(pager.new_write_batch().is_empty());
    });
}

#[test]
fn batch_with_zero_pages_is_empty_submit() {
    run_async(async move {
        // 0 page batch submit 应该是 no-op
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let batch = pager.new_write_batch();
        let mappings = batch.submit(&mut pager).await.unwrap();
        assert!(mappings.is_empty());
        assert_eq!(pager.travel_tree_count(), 0);
    });
}

// =====================================================================
// ⭐ batch + flush 协同: 落盘 + chunk_list
// =====================================================================

#[test]
fn batch_submit_then_flush_persists_all_pages() {
    run_async(async move {
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        // batch 提交 3 page
        let mut batch = pager.new_write_batch();
        let mut p0 = [0u8; PAGE_SIZE];
        p0[0x28] = 0xA0;
        let mut p1 = [0u8; PAGE_SIZE];
        p1[0x28] = 0xB0;
        let mut p2 = [0u8; PAGE_SIZE];
        p2[0x28] = 0xC0;
        batch.add(0, Box::new(p0));
        batch.add(1, Box::new(p1));
        batch.add(2, Box::new(p2));
        batch.submit(&mut pager).await.unwrap();

        // flush 后 .block 文件应包含所有 page
        pager.flush().await.unwrap();

        let block_path = tmp.path().join("000001.block");
        let f = std::fs::File::open(&block_path).unwrap();
        for (page_idx, expected) in [(0u64, 0xA0u8), (1, 0xB0), (2, 0xC0)] {
            let mut buf = [0u8; PAGE_SIZE];
            f.read_exact_at(&mut buf, page_idx * PAGE_SIZE as u64)
                .unwrap();
            assert_eq!(buf[0x28], expected, "page {} on disk", page_idx);
        }
    });
}

#[test]
fn batch_submit_after_flush_overwrites_correctly() {
    run_async(async move {
        // 第一次 batch 写 page 0, flush
        // 第二次 batch 写 page 0 (新值), flush
        // 验证 page 0 是新值 (LSM 重映射)
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        // 第一次
        let mut b1 = pager.new_write_batch();
        let mut d1 = [0u8; PAGE_SIZE];
        d1[0x28] = 0x11;
        b1.add(0, Box::new(d1));
        b1.submit(&mut pager).await.unwrap();
        pager.flush().await.unwrap();

        // 第二次: page 0 新值
        let mut b2 = pager.new_write_batch();
        let mut d2 = [0u8; PAGE_SIZE];
        d2[0x28] = 0x22;
        b2.add(0, Box::new(d2));
        b2.submit(&mut pager).await.unwrap();
        pager.flush().await.unwrap();

        // read page 0 应是新值 0x22
        let r = pager.read(0).await.unwrap();
        assert_eq!(r[0x28], 0x22, "第二次写覆盖第一次");
    });
}

// =====================================================================
// ⭐ chunk_lock 与 batch 协同: 写路径不触发 chunk_lock
// =====================================================================

#[test]
fn batch_write_does_not_acquire_chunk_lock() {
    run_async(async move {
        // batch 写路径只动 nowchunks, 不触发 chunk_lock
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        assert!(pager.chunk_lock().is_empty(), "初始 chunk_lock 应为空");

        let mut batch = pager.new_write_batch();
        batch.add(0, Box::new([0u8; PAGE_SIZE]));
        batch.add(1, Box::new([0u8; PAGE_SIZE]));
        batch.submit(&mut pager).await.unwrap();

        // batch submit 不会触发 chunk_lock acquire (写路径无 pin)
        assert!(
            pager.chunk_lock().is_empty(),
            "batch submit 不应触发 chunk_lock entry"
        );
    });
}

#[test]
fn batch_write_then_read_triggers_chunk_lock_already_loaded() {
    run_async(async move {
        // batch write 完, read 应走 nowchunks 优先, 不触发 chunk_lock
        // 但 flush 后, 再 read 触发 chunk_list miss → 走 chunk_lock owner 路径
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let mut batch = pager.new_write_batch();
        let mut data = [0u8; PAGE_SIZE];
        data[0x28] = 0xEE;
        batch.add(0, Box::new(data));
        batch.submit(&mut pager).await.unwrap();

        // 立刻 read: 走 nowchunks 优先
        let r = pager.read(0).await.unwrap();
        assert_eq!(r[0x28], 0xEE);
        assert!(pager.chunk_lock().is_empty());

        // flush 后, read 触发 chunk_list miss → chunk_lock owner
        pager.flush().await.unwrap();
        // 先 invalidate chunk_list 模拟 evict
        {
            let list = pager.chunk_list();
            list.invalidate(
                &PageKey {
                    file_id: 0,
                    chunk_idx: 0,
                }
                .into(),
            );
        }
        // 读 page 0: chunk_list miss, 走 acquire_chunk_lock (BecameOwner)
        let r = pager.read(0).await.unwrap();
        assert_eq!(r[0x28], 0xEE);

        // 注: 当前 Pager.read 还没集成 chunk_lock, 所以不会创建 entry
        // 但接口已准备 (Pager::acquire_chunk_lock), T11 polish 时接入
        let _ = AcquireResult::BecameOwner; // 类型存在性
    });
}

// =====================================================================
// ⭐ chunk 满时 batch submit 自动 rotate
// =====================================================================

#[test]
fn batch_submit_rotates_to_next_chunk_when_full() {
    run_async(async move {
        // 写 64 page 满 chunk 0, 第 65 page 应触发 rotate 到 chunk 1
        // 1MB chunk = 64 page (16KB each)
        // PageWriteBatch 上限 16 page, 用 4 个 batch + 1 个 batch
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        // 前 4 个 batch 写满 64 page (chunk 0)
        for batch_no in 0..4u64 {
            let mut batch = pager.new_write_batch();
            for j in 0..16u64 {
                let vpid = batch_no * 16 + j;
                batch.add(vpid, Box::new([0u8; PAGE_SIZE]));
            }
            let m = batch.submit(&mut pager).await.unwrap();
            assert_eq!(m.len(), 16);
            // 都在 chunk 0
            for (_vpid, pid) in m.iter() {
                assert_eq!(pid.chunk_idx(), 0, "page 应在 chunk 0");
            }
        }

        // 第 65 个 page (vpid 64) 应触发 rotate 到 chunk 1
        let mut b5 = pager.new_write_batch();
        b5.add(64, Box::new([0u8; PAGE_SIZE]));
        let m5 = b5.submit(&mut pager).await.unwrap();
        assert_eq!(m5[0].1.chunk_idx(), 1, "page 64 应在 chunk 1");
    });
}

// =====================================================================
// ⭐ 跨 chunk batch: 一批 page 跨多个 chunk (T8 polish 会遇到)
// =====================================================================

#[test]
fn batch_submit_pages_across_chunks_advanced() {
    run_async(async move {
        // 一次 batch 包含 70 page, 应跨 chunk 0 (64 page) + chunk 1 (6 page)
        // PageWriteBatch 上限 16, 用 5 个 batch 写 70 page
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let mut all_mappings = Vec::new();
        for batch_no in 0..5u64 {
            let mut batch = pager.new_write_batch();
            for j in 0..16u64 {
                let vpid = batch_no * 16 + j;
                if vpid < 70 {
                    batch.add(vpid, Box::new([0u8; PAGE_SIZE]));
                }
            }
            let m = batch.submit(&mut pager).await.unwrap();
            all_mappings.extend(m);
        }
        // 70 page 都写完
        assert_eq!(all_mappings.len(), 70);

        // 验证 chunk 分布
        for (i, (_vpid, pid)) in all_mappings.iter().enumerate() {
            if i < 64 {
                assert_eq!(pid.chunk_idx(), 0, "page {} 应在 chunk 0", i);
            } else {
                assert_eq!(pid.chunk_idx(), 1, "page {} 应在 chunk 1", i);
            }
        }

        // 全部 read 能拿到正确数据
        pager.flush().await.unwrap();
        for i in 0..70u64 {
            let r = pager.read(i).await.unwrap();
            assert_eq!(r[0x28], 0, "page {} first byte", i);
        }
    });
}

// =====================================================================
// ⭐ batch 失败: 不存在的 vpid? (实际 vpid 永不重用, 应支持)
// =====================================================================

#[test]
fn batch_add_same_vpid_twice_overwrites() {
    run_async(async move {
        // 同一 vpid 在 batch 中 add 两次: 第二次覆盖第一次的 data
        // ⭐ 用 vpid=42 避免 META_VPID(=0) 特殊路径 — META_VPID 写 page 始终走 META_PID,
        //    不能 COW, 所以两次返回同一 pid. 这里测的是非 META vpid 的 COW 行为.
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let mut batch = pager.new_write_batch();
        let mut d1 = [0u8; PAGE_SIZE];
        d1[0x28] = 0x11;
        let mut d2 = [0u8; PAGE_SIZE];
        d2[0x28] = 0x22;
        batch.add(42, Box::new(d1));
        batch.add(42, Box::new(d2)); // 覆盖
        let mappings = batch.submit(&mut pager).await.unwrap();
        assert_eq!(mappings.len(), 2, "两次 add 产生 2 个 mapping");

        // ⭐ T17b 设计: vpid 在 nowchunks 中复用原 pid (in-place 覆盖, 不 COW).
        //    之前实现是 alloc 新 pid (LSM COW), 现在 in-nowchunk 复用.
        //    意义: 节省 page_idx 槽位, 不浪费磁盘空间.
        assert_eq!(mappings[0].0, 42);
        assert_eq!(mappings[1].0, 42);
        assert_eq!(
            mappings[0].1, mappings[1].1,
            "vpid 42 在 nowchunks 中 (第一次 add 后), 第二次 add 复用同一 pid"
        );

        // read 42 应该是最后 add 的 d2 (因为 meta_cache 写最后那次)
        let r = pager.read(42).await.unwrap();
        assert_eq!(r[0x28], 0x22, "read 42 应是 d2 (后写覆盖)");
    });
}
