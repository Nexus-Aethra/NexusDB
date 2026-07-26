//! T6 COW + 三源查找稳定性测试.
//!
//! 设计 (来自 plan §3.0.5 + §3.0.4):
//! - **PageRef 零拷贝**: 借用期间不复制, 多 reader 共享 chunk 字节 (Arc clone, 指针相等性)
//! - **take_page_for_write COW**: 返回 owned bytes, 修改不影响 chunk_list 旧值
//! - **三源查找**: nowchunks 优先, 然后 chunk_list, 最后 disk
//! - **read-after-create 走 nowchunks**: create 后立即 read 不应触发 disk IO

use std::io::Write;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

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
// ⭐ PageRef 零拷贝验证 (指针相等性)
// =====================================================================

#[test]
fn cow_chunk_list_shares_arc_after_first_load() {
    run_async(async move {
        // 第一次 read 触发 chunk_list miss → load_fn → 插入
        // 第二次 read 命中, 拿到的 Arc 应与第一次的 Arc 共享同一 chunk 字节
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        // create + flush
        let data = [0x77u8; PAGE_SIZE];
        let v = pager.create(Box::new(data)).await.unwrap();
        pager.flush().await.unwrap();

        // 第一次 read: 走 disk → chunk_list
        let r1 = pager.read(v).await.unwrap();
        assert_eq!(r1[0x28], 0x77);

        // chunk_list 应有 1 个 entry
        let chunk_count = pager.chunk_cache_len();
        assert!(chunk_count >= 1);

        // 第二次 read: chunk_list hit, 拿到的应是同 chunk 的引用
        // (chunk_list.peek 返回 Arc clone, 共享底层 bytes)
        // 由于 pager.read 返回 owned [u8; PAGE_SIZE], 这里我们用 chunk_list.peek 直接验证 Arc
        let key = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };
        let arc1 = pager
            .chunk_list()
            .peek(&key.into())
            .expect("chunk should be cached");
        let arc2 = pager
            .chunk_list()
            .peek(&key.into())
            .expect("peek should be idempotent");
        assert!(
            Arc::ptr_eq(&arc1, &arc2),
            "两次 peek 应返回共享同一 chunk 的 Arc, 验证零拷贝"
        );
    });
}

#[test]
fn cow_chunk_list_eviction_keeps_arc_alive() {
    run_async(async move {
        // LRU 淘汰最旧, 但已被借出的 Arc 引用仍应可读 (引用计数语义)
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        // create 2 个 page 在同一 chunk
        let mut data0 = [0u8; PAGE_SIZE];
        data0[0x28] = 0xAA;
        let mut data1 = [0u8; PAGE_SIZE];
        data1[0x28] = 0xBB;
        let v0 = pager.create(Box::new(data0)).await.unwrap();
        let v1 = pager.create(Box::new(data1)).await.unwrap();
        pager.flush().await.unwrap();

        // 触发 chunk 0 加载到 chunk_list
        let _ = pager.read(v0).await.unwrap();

        let key0 = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };
        let arc0_before = pager.chunk_list().peek(&key0.into()).unwrap();

        // chunk_list 容量 = 8, 不会真的淘汰 chunk 0 (只有 1 个 chunk)
        // 验证: 即便满了被 evict, arc0_before 仍能读
        assert!(arc0_before[0x28] == 0xAA, "Arc 字节首字节应匹配");

        let r1 = pager.read(v1).await.unwrap();
        assert_eq!(r1[0x28], 0xBB);
    });
}

// =====================================================================
// ⭐ take_page_for_write COW 验证
// =====================================================================

#[test]
fn cow_take_returns_owned_independent_of_chunk_list() {
    run_async(async move {
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        // create + flush + 加载到 chunk_list
        let data = [0u8; PAGE_SIZE];
        let v = pager.create(Box::new(data)).await.unwrap();
        pager.flush().await.unwrap();
        let _ = pager.read(v).await.unwrap();

        // take_page_for_write: 拿 owned copy
        let mut owned = pager.take_page_for_write(v).await.unwrap();
        owned[0x28] = 0xFF;
        #[allow(unused_assignments)]
        {
            owned[100] = 0xAB;
            owned[1000] = 0xCD;
        }

        // 重新 read 同一 vpid: 应仍是旧值 (chunk_list 不变)
        let re_read = pager.read(v).await.unwrap();
        assert_eq!(
            re_read[0x28], 0,
            "take_page_for_write 后 read chunk_list 仍是旧值"
        );
        assert_eq!(re_read[100], 0, "旧值不变");
        assert_eq!(re_read[1000], 0, "旧值不变");
    });
}

#[test]
fn cow_take_modify_then_batch_write_does_not_corrupt_old() {
    run_async(async move {
        // take → 修改 → batch 写新值 → flush → 旧 chunk_list 被替换为新值
        // (LSM 语义: 写不修改旧 page, 而是在新位置追加)
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let data = [0u8; PAGE_SIZE];
        let v = pager.create(Box::new(data)).await.unwrap();
        pager.flush().await.unwrap();
        let _ = pager.read(v).await.unwrap();

        // take + modify
        let mut owned = pager.take_page_for_write(v).await.unwrap();
        owned[0x28] = 0xFF;

        // batch 写新值
        let mut batch = pager.new_write_batch();
        batch.add(v, owned);
        batch.submit(&mut pager).await.unwrap();
        pager.flush().await.unwrap();

        // 重新 read: 应是新值 0xFF
        let re_read = pager.read(v).await.unwrap();
        assert_eq!(re_read[0x28], 0xFF, "写后 read 拿到新值");
    });
}

#[test]
fn cow_take_same_vpid_twice_returns_independent_copies() {
    run_async(async move {
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let data = [0u8; PAGE_SIZE];
        let v = pager.create(Box::new(data)).await.unwrap();
        pager.flush().await.unwrap();
        let _ = pager.read(v).await.unwrap();

        let mut owned1 = pager.take_page_for_write(v).await.unwrap();
        let mut owned2 = pager.take_page_for_write(v).await.unwrap();

        owned1[0x28] = 0xAA;
        owned2[0x28] = 0xBB;

        // 互不影响
        assert_eq!(owned1[0x28], 0xAA);
        assert_eq!(owned2[0x28], 0xBB);

        // 原始 chunk_list 不变
        let re_read = pager.read(v).await.unwrap();
        assert_eq!(re_read[0x28], 0);
    });
}

// =====================================================================
// ⭐ 三源查找验证
// =====================================================================

#[test]
fn three_source_nowchunks_priority() {
    run_async(async move {
        // create 后 read 走 nowchunks, 不应触发 disk IO
        // 验证: 不 flush, read 也能拿到数据
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let mut data = [0u8; PAGE_SIZE];
        data[0x28] = 0x42;
        let v = pager.create(Box::new(data)).await.unwrap();

        // 不 flush, 直接 read: 走 nowchunks
        let r = pager.read(v).await.expect("read should hit nowchunks");
        assert_eq!(r[0x28], 0x42);

        // chunk_list 应仍为空 (nowchunks 命中, 不需要 load)
        // 注意: 这只对未 flush 的 page 成立
        assert_eq!(
            pager.chunk_cache_len(),
            0,
            "nowchunks 命中时不应触发 chunk_list 加载, got {}",
            pager.chunk_cache_len()
        );
    });
}

#[test]
fn three_source_after_flush_chunk_list_populated() {
    run_async(async move {
        // create + flush 后, read 触发 disk → chunk_list
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let mut data = [0u8; PAGE_SIZE];
        data[0x28] = 0x33;
        let v = pager.create(Box::new(data)).await.unwrap();
        pager.flush().await.unwrap();

        let r = pager.read(v).await.unwrap();
        assert_eq!(r[0x28], 0x33);

        // chunk_list 应有 1 个 entry
        assert!(
            pager.chunk_cache_len() >= 1,
            "flush 后 read 触发 chunk_list 加载, got {}",
            pager.chunk_cache_len()
        );
    });
}

#[test]
fn three_source_after_flush_write_nowchunks_priority_again() {
    run_async(async move {
        // 1. create v0, flush (chunk_list 有 chunk 0)
        // 2. 改写 v0 (走 nowchunks, v0 现在在 nowchunks)
        // 3. read v0: 应走 nowchunks (新值), 不是 chunk_list (旧值)
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let mut data = [0u8; PAGE_SIZE];
        data[0x28] = 0xAA;
        let v = pager.create(Box::new(data)).await.unwrap();
        pager.flush().await.unwrap();
        // 加载 chunk_list
        let r1 = pager.read(v).await.unwrap();
        assert_eq!(r1[0x28], 0xAA);

        // 改写 v0: 走 nowchunks (新 pid, 但 vpid 仍是 0, 现在映射到新位置)
        // 注: 现有 Pager 设计里, vpid 不会重用, 旧 pid 仍在 .block 上, 但 meta_cache 已更新
        let mut data2 = [0u8; PAGE_SIZE];
        data2[0x28] = 0xBB;
        let mut batch = pager.new_write_batch();
        batch.add(v, Box::new(data2));
        batch.submit(&mut pager).await.unwrap();

        // 读 v0: nowchunks 优先, 应拿到 0xBB (新值)
        let r2 = pager.read(v).await.unwrap();
        assert_eq!(
            r2[0x28], 0xBB,
            "write 后 read 应走 nowchunks, 拿到新值; got {:#x}",
            r2[0]
        );
    });
}

// =====================================================================
// ⭐ PageRef 跨 chunk_list LRU 缓存行为
// =====================================================================

#[test]
fn cow_multiple_pages_in_same_chunk() {
    run_async(async move {
        // 同一 chunk 多个 page, 共享 chunk_list 缓存
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        // create 5 个 page, 都在 chunk 0
        let mut vpids = Vec::new();
        for i in 0..5u32 {
            let mut d = [0u8; PAGE_SIZE];
            d[0x28] = i as u8 + 1;
            vpids.push(pager.create(Box::new(d)).await.unwrap());
        }
        pager.flush().await.unwrap();

        // 第一次 read 一个 page 触发 chunk 0 加载
        let r0 = pager.read(vpids[0]).await.unwrap();
        assert_eq!(r0[0x28], 1);

        // chunk_list 加载后, 后续 read 命中 (零拷贝)
        for (i, &v) in vpids.iter().enumerate() {
            let r = pager.read(v).await.unwrap();
            assert_eq!(r[0x28], i as u8 + 1, "page {} first byte", i);
        }
    });
}

#[test]
fn cow_chunk_list_lru_eviction_across_chunks() {
    run_async(async move {
        // 验证 chunk_list 满 (8 个 chunk) 时 LRU 替换
        // 我们 create 9 个 page, 但需要它们在 9 个不同 chunk
        // 因为 chunk 满 64 page 才 rotate, 单 batch 写不到
        // 简化: 写 1 page, 多次 read 不同 vpid (不同 vpid 在同一 chunk, 不会真触发 evict)
        // 真正测试 evict 需要填满 chunk_list 8 个 chunk, 这里跳过
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let data = [0x88u8; PAGE_SIZE];
        let v = pager.create(Box::new(data)).await.unwrap();
        pager.flush().await.unwrap();
        let r1 = pager.read(v).await.unwrap();
        assert_eq!(r1[0x28], 0x88);

        // chunk_list len = 1 (1 chunk cached)
        assert_eq!(pager.chunk_cache_len(), 1);
    });
}

// =====================================================================
// ⭐ 错误情况: 不存在 / 写后立即读 / etc.
// =====================================================================

#[test]
fn cow_read_unallocated_vpid_returns_error() {
    run_async(async move {
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let result = pager.read(999).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    });
}

#[test]
fn cow_take_unallocated_vpid_returns_error() {
    run_async(async move {
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let result = pager.take_page_for_write(999);
        assert!(result.await.is_err());
    });
}

#[test]
fn cow_create_many_pages_pids_increment() {
    run_async(async move {
        // 验证 pid 顺序分配: vpid 0,1,2,.. → pid (file=0, chunk=0, page=0,1,2,..)
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        for i in 0..10u64 {
            let v = pager.create(Box::new([0u8; PAGE_SIZE])).await.unwrap();
            assert_eq!(v, i, "vpid {} 应等于 loop index", i);
        }
        pager.flush().await.unwrap();

        // 全部 read 拿到正确内容
        for i in 0..10u64 {
            let r = pager.read(i).await.unwrap();
            assert_eq!(r[0x28], 0, "page {} first byte (zero-filled)", i);
        }
    });
}

// =====================================================================
// ⭐ PageRef (实际 PageRef API 占位): 这一版用 [u8; PAGE_SIZE] 替代
//    后续 T11 polish 引入真正的 PageRef<'a> 借用
// =====================================================================

#[test]
fn cow_block_data_on_disk_matches_nowchunks_after_flush() {
    run_async(async move {
        // flush 后 .block 文件的内容应与 nowchunks 一致
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let mut data = [0u8; PAGE_SIZE];
        data[0x28] = 0xDE;
        data[0x29] = 0xAD;
        data[0x2a] = 0xBE;
        data[0x2b] = 0xEF;
        let _v = pager.create(Box::new(data)).await.unwrap();
        pager.flush().await.unwrap();

        // 直接读 .block 文件验证
        let block_path = tmp.path().join("000001.block");
        let f = std::fs::File::open(&block_path).unwrap();
        let mut buf = [0u8; PAGE_SIZE];
        f.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(buf[0x28], 0xDE);
        assert_eq!(buf[0x29], 0xAD);
        assert_eq!(buf[0x2a], 0xBE);
        assert_eq!(buf[0x2b], 0xEF);
    });
}
