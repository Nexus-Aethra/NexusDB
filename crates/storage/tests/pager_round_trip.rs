//! T6 Pager 集成测试: read / create / flush / round_trip.
//!
//! 设计 (来自 plan §3.4 + §3.0.5 + DESIGN §4.5):
//! - `read` 走三源查找 (nowchunks > WriteQueue > chunk_list > disk)
//! - `create` 走 PageWriteBatch → nowchunks → flush 落盘
//! - `flush` 同步等 chunk data + meta 落盘
//! - `read_after_create` 走 chunk_list cache (二次读 hit)
//!
//! 第一版 (TDD 简化): Pager 用同步 std::fs IO 读 .block 文件.
//! 后续 T11 polish 接 scheduler::io_ops::read 走 io_uring 异步.

use std::io::Write;
use std::os::unix::fs::FileExt;

use storage::alloc::{PidAllocator, VpidAllocator};
use storage::chunk_lru::ChunkList;
use storage::chunk_writer::{ChunkWriter, NowChunks};
use storage::pager::Pager;
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

#[test]
fn pager_create_then_read_roundtrip_via_nowchunks() {
    run_async(async move {
        let (tmp, meta) = setup();
        let block_path = make_block(&tmp);

        let mut pager = Pager::new(
            tmp.path().to_path_buf(),
            meta,
            VpidAllocator::new(0),
            // ⭐ T12.14: pid_alloc 起点 (0, 0, 1) 跳过 page 0, 让 META_PID 独占
            // page 0. META_VPID 写 page 走 META_PID 直接, 不走 pid_alloc.
            PidAllocator::new(0, 0, 1),
            ChunkList::new(8),
            NowChunks::new(),
            ChunkWriter::new(&block_path).unwrap(),
        );

        // create: 分配 vpid 0, 写 nowchunks (未 flush)
        let data = [0xDEu8; PAGE_SIZE];
        let vpid = pager.create(Box::new(data)).await.expect("create ok");
        assert_eq!(vpid, 0);

        // 立刻 read: 走 nowchunks (peek)
        let read_data = pager.read(vpid).await.expect("read after create ok");
        assert_eq!(read_data[0x28], 0xDE);
        assert_eq!(read_data[PAGE_SIZE - 1], 0xDE);
    });
}

#[test]
fn pager_flush_then_read_roundtrip_via_disk() {
    run_async(async move {
        let (tmp, meta) = setup();
        let block_path = make_block(&tmp);

        let mut pager = Pager::new(
            tmp.path().to_path_buf(),
            meta,
            VpidAllocator::new(0),
            // ⭐ T12.14: pid_alloc 起点 (0, 0, 1) 跳过 page 0, 让 META_PID 独占
            // page 0. META_VPID 写 page 走 META_PID 直接, 不走 pid_alloc.
            PidAllocator::new(0, 0, 1),
            ChunkList::new(8),
            NowChunks::new(),
            ChunkWriter::new(&block_path).unwrap(),
        );

        let mut data = [0u8; PAGE_SIZE];
        data[0x28] = 0xAB;
        data[0x29] = 0xCD;
        let vpid = pager.create(Box::new(data)).await.expect("create ok");
        assert_eq!(vpid, 0);

        // flush 走 sync write
        pager.flush().await.expect("flush ok");

        // flush 后 nowchunks 已 drain, read 应走 disk (chunk_list miss → load_fn 读盘)
        let read_data = pager.read(vpid).await.expect("read after flush ok");
        assert_eq!(read_data[0x28], 0xAB);
        assert_eq!(read_data[0x29], 0xCD);
    });
}

#[test]
fn pager_multiple_pages_flush_in_order() {
    run_async(async move {
        let (tmp, meta) = setup();
        let block_path = make_block(&tmp);

        let mut pager = Pager::new(
            tmp.path().to_path_buf(),
            meta,
            VpidAllocator::new(0),
            // ⭐ T12.14: pid_alloc 起点 (0, 0, 1) 跳过 page 0, 让 META_PID 独占
            // page 0. META_VPID 写 page 走 META_PID 直接, 不走 pid_alloc.
            PidAllocator::new(0, 0, 1),
            ChunkList::new(8),
            NowChunks::new(),
            ChunkWriter::new(&block_path).unwrap(),
        );

        // create 3 pages
        let mut data0 = [0u8; PAGE_SIZE];
        data0[0x28] = 0xA0;
        let mut data1 = [0u8; PAGE_SIZE];
        data1[0x28] = 0xB0;
        let mut data2 = [0u8; PAGE_SIZE];
        data2[0x28] = 0xC0;
        let v0 = pager.create(Box::new(data0)).await.unwrap();
        let v1 = pager.create(Box::new(data1)).await.unwrap();
        let v2 = pager.create(Box::new(data2)).await.unwrap();
        assert_eq!((v0, v1, v2), (0, 1, 2));

        pager.flush().await.expect("flush ok");

        // 验证 .block 文件内容: page 0/1/2 字节正确
        let f = std::fs::File::open(&block_path).unwrap();
        let mut buf = [0u8; PAGE_SIZE];
        f.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(buf[0x28], 0xA0);
        f.read_exact_at(&mut buf, PAGE_SIZE as u64).unwrap();
        assert_eq!(buf[0x28], 0xB0);
        f.read_exact_at(&mut buf, 2 * PAGE_SIZE as u64).unwrap();
        assert_eq!(buf[0x28], 0xC0);
    });
}

#[test]
fn pager_reopen_after_flush_restores_data() {
    run_async(async move {
        let (tmp, meta) = setup();
        let block_path = make_block(&tmp);

        // 第一次 session: create + flush
        {
            let mut pager = Pager::new(
                tmp.path().to_path_buf(),
                meta,
                VpidAllocator::new(0),
                PidAllocator::new(0, 0, 0),
                ChunkList::new(8),
                NowChunks::new(),
                ChunkWriter::new(&block_path).unwrap(),
            );
            let mut data = [0u8; PAGE_SIZE];
            data[0x28] = 0xDE;
            data[0x29] = 0xAD;
            let v = pager.create(Box::new(data)).await.unwrap();
            assert_eq!(v, 0);
            pager.flush().await.unwrap();
        }

        // 第二次 session: reopen, 验证数据持久化
        let meta2 = MetaCache::open(&tmp.path().join("page.mate")).unwrap();
        // 持久化后 vpid 0 已映射, vpid_alloc 起始 1, pid_alloc 起始 chunk 0 page 1
        let mut pager2 = Pager::new(
            tmp.path().to_path_buf(),
            meta2,
            VpidAllocator::new(1),
            PidAllocator::new(0, 0, 1),
            ChunkList::new(8),
            NowChunks::new(),
            ChunkWriter::new(&block_path).unwrap(),
        );
        let read_data = pager2.read(0).await.expect("read after reopen ok");
        assert_eq!(read_data[0x28], 0xDE);
        assert_eq!(read_data[0x29], 0xAD);
    });
}

#[test]
fn pager_chunk_cache_hit_on_second_read() {
    run_async(async move {
        let (tmp, meta) = setup();
        let block_path = make_block(&tmp);

        let mut pager = Pager::new(
            tmp.path().to_path_buf(),
            meta,
            VpidAllocator::new(0),
            // ⭐ T12.14: pid_alloc 起点 (0, 0, 1) 跳过 page 0, 让 META_PID 独占
            // page 0. META_VPID 写 page 走 META_PID 直接, 不走 pid_alloc.
            PidAllocator::new(0, 0, 1),
            ChunkList::new(8),
            NowChunks::new(),
            ChunkWriter::new(&block_path).unwrap(),
        );

        let data = [0x42u8; PAGE_SIZE];
        let v = pager.create(Box::new(data)).await.unwrap();
        pager.flush().await.unwrap();

        // 第一次 read: chunk_list miss → load_fn 读盘 → 插入 chunk_list
        let r1 = pager.read(v).await.unwrap();
        assert_eq!(r1[0x28], 0x42);

        // 第二次 read: chunk_list hit, 零拷贝
        let r2 = pager.read(v).await.unwrap();
        assert_eq!(r2[0x28], 0x42);

        // chunk_list 应至少有 1 个 chunk
        assert!(
            pager.chunk_cache_len() >= 1,
            "chunk should be cached after first read, got {}",
            pager.chunk_cache_len()
        );
    });
}

#[test]
fn pager_read_unmapped_vpid_returns_not_found() {
    run_async(async move {
        let (tmp, meta) = setup();
        let block_path = make_block(&tmp);

        let mut pager = Pager::new(
            tmp.path().to_path_buf(),
            meta,
            VpidAllocator::new(0),
            // ⭐ T12.14: pid_alloc 起点 (0, 0, 1) 跳过 page 0, 让 META_PID 独占
            // page 0. META_VPID 写 page 走 META_PID 直接, 不走 pid_alloc.
            PidAllocator::new(0, 0, 1),
            ChunkList::new(8),
            NowChunks::new(),
            ChunkWriter::new(&block_path).unwrap(),
        );

        // vpid 0 还没分配
        let result = pager.read(0);
        assert!(result.await.is_err(), "unmapped vpid should error");
    });
}

#[test]
fn pager_page_write_batch_submits_atomic() {
    run_async(async move {
        let (tmp, meta) = setup();
        let block_path = make_block(&tmp);

        let mut pager = Pager::new(
            tmp.path().to_path_buf(),
            meta,
            VpidAllocator::new(0),
            // ⭐ T12.14: pid_alloc 起点 (0, 0, 1) 跳过 page 0, 让 META_PID 独占
            // page 0. META_VPID 写 page 走 META_PID 直接, 不走 pid_alloc.
            PidAllocator::new(0, 0, 1),
            ChunkList::new(8),
            NowChunks::new(),
            ChunkWriter::new(&block_path).unwrap(),
        );

        // batch 一次写 3 page 到 vpid 0/1/2
        let mut batch = pager.new_write_batch();
        let mut d0 = [0u8; PAGE_SIZE];
        d0[0x28] = 0x10;
        let mut d1 = [0u8; PAGE_SIZE];
        d1[0x28] = 0x20;
        let mut d2 = [0u8; PAGE_SIZE];
        d2[0x28] = 0x30;
        batch.add(0, Box::new(d0));
        batch.add(1, Box::new(d1));
        batch.add(2, Box::new(d2));
        let mappings = batch.submit(&mut pager).await.expect("batch submit ok");
        assert_eq!(mappings.len(), 3);

        pager.flush().await.unwrap();

        // 全部 page 读回
        for (i, expected) in [(0, 0x10u8), (1, 0x20), (2, 0x30)] {
            let r = pager.read(i as u64).await.unwrap();
            assert_eq!(r[0x28], expected, "page {} first byte", i);
        }
    });
}

#[test]
fn pager_take_page_for_write_cow() {
    run_async(async move {
        let (tmp, meta) = setup();
        let block_path = make_block(&tmp);

        let mut pager = Pager::new(
            tmp.path().to_path_buf(),
            meta,
            VpidAllocator::new(0),
            // ⭐ T12.14: pid_alloc 起点 (0, 0, 1) 跳过 page 0, 让 META_PID 独占
            // page 0. META_VPID 写 page 走 META_PID 直接, 不走 pid_alloc.
            PidAllocator::new(0, 0, 1),
            ChunkList::new(8),
            NowChunks::new(),
            ChunkWriter::new(&block_path).unwrap(),
        );

        // 先 create + flush 让 vpid 0 在 chunk_list 有副本
        let data = [0u8; PAGE_SIZE];
        let v = pager.create(Box::new(data)).await.unwrap();
        pager.flush().await.unwrap();
        // 触发 chunk_list 加载
        let _ = pager.read(v).await.unwrap();

        // take_page_for_write: 拿独立 owned copy
        let mut owned = pager.take_page_for_write(v).await.unwrap();
        owned[0x28] = 0xFF;
        #[allow(unused_assignments)]
        {
            owned[100] = 0xAB;
        }

        // take_page_for_write 返回的是 owned bytes, 写它不影响 chunk_list 中旧值
        // (chunk_list 中 page v 还是旧值, 0)
        let re_read = pager.read(v).await.unwrap();
        assert_eq!(re_read[0x28], 0, "chunk_list 旧值不被修改");
        assert_eq!(re_read[100], 0, "chunk_list 旧值不被修改");
    });
}

#[test]
fn pager_page_key_roundtrip() {
    // PageKey 转换测试
    let pk = PageKey {
        file_id: 0,
        chunk_idx: 0,
    };
    assert_eq!(pk.file_id, 0);
    assert_eq!(pk.chunk_idx, 0);
}
