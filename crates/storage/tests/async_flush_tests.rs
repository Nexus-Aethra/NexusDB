//! ⭐ 异步 chunk 落盘状态机测试 (take_flush_batches / complete_flush / 背压).
//!
//! 只测 Pager 状态机语义; 真实协程并发落盘由 shard_manager e2e 覆盖.

use std::io::Write;

use storage::alloc::{PidAllocator, VpidAllocator};
use storage::chunk_lru::ChunkList;
use storage::chunk_writer::{ChunkWriter, NowChunks};
use storage::pager::Pager;
use storage::{MetaCache, PAGE_SIZE};

mod common;

use common::run_async;

fn setup_pager(tmp: &tempfile::TempDir) -> Pager {
    let mate = tmp.path().join("page.mate");
    std::fs::File::create(&mate)
        .unwrap()
        .write_all(&vec![0u8; 1024 * 1024])
        .unwrap();
    let meta = MetaCache::open(&mate).unwrap();
    let block_path = tmp.path().join("000001.block");
    let f = std::fs::File::create(&block_path).unwrap();
    f.set_len(16 * 1024 * 1024).unwrap();
    Pager::new(
        tmp.path().to_path_buf(),
        meta,
        VpidAllocator::new(0),
        PidAllocator::new(0, 0, 1),
        ChunkList::new(32),
        NowChunks::new(),
        ChunkWriter::new(&block_path).unwrap(),
    )
}

/// 持续 create page 直到 flush_backlog 达到目标 (chunk 满自动 swap 入队).
async fn fill_until_backlog(pager: &mut Pager, target: usize, max_creates: usize) {
    for _ in 0..max_creates {
        if pager.flush_backlog() >= target {
            return;
        }
        let page = Box::new([0xABu8; PAGE_SIZE]);
        pager.create(page).await.unwrap();
    }
    panic!(
        "backlog {} never reached target {} within {} creates",
        pager.flush_backlog(),
        target,
        max_creates
    );
}

#[test]
fn take_batches_dedup_and_complete_ok() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut pager = setup_pager(&tmp);

        fill_until_backlog(&mut pager, 1, 300).await;

        // 取批: pending → in-flight (同 file 归入同一批)
        let batches = pager.take_flush_batches();
        assert!(!batches.is_empty(), "expect at least 1 flush batch");
        assert!(pager.has_inflight());
        let key = batches[0].items[0].0;

        // 同 key 去重: 没有新 pending 时再取应为空
        let batches2 = pager.take_flush_batches();
        assert!(batches2.is_empty(), "in-flight key must not be re-issued");

        // 周期刷盘守卫: in-flight 未排空时推迟
        let flushed = pager.maybe_periodic_flush().await.unwrap();
        assert!(!flushed, "periodic flush must defer while in-flight");

        // 完成收割: 移出 in-flight, 字节入 chunk_list
        let before = pager.chunk_cache_len();
        pager.complete_flush(key, Ok(())).unwrap();
        assert!(!pager.has_inflight());
        assert!(pager.chunk_cache_len() > before, "chunk must enter chunk_list");
    });
}

#[test]
fn complete_flush_error_requeues() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut pager = setup_pager(&tmp);

        fill_until_backlog(&mut pager, 1, 300).await;
        let batches = pager.take_flush_batches();
        assert!(!batches.is_empty());
        let key = batches[0].items[0].0;
        let backlog_inflight = pager.flush_backlog();

        // 模拟落盘失败 → 回 pending 重试, meta 不前进
        let err = std::io::Error::other("simulated io failure");
        let r = pager.complete_flush(key, Err(err));
        assert!(r.is_err());
        assert!(!pager.has_inflight());
        assert_eq!(
            pager.flush_backlog(),
            backlog_inflight,
            "failed chunk must be requeued to pending"
        );

        // 重试路径: 再取批能拿到同 key
        let retry = pager.take_flush_batches();
        assert!(
            retry
                .iter()
                .any(|b| b.items.iter().any(|(k, _)| *k == key)),
            "retry batch for same key"
        );
    });
}

#[test]
fn backpressure_degrades_to_sync_write() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut pager = setup_pager(&tmp);

        // 灌满 8 个 in-flight (取批但不完成 → 占住 in-flight 槽)
        // MAX_INFLIGHT_CHUNKS = 8
        for _ in 0..8 {
            let cur = pager.flush_backlog();
            fill_until_backlog(&mut pager, cur + 1, 300).await;
            let _batches = pager.take_flush_batches();
        }
        assert_eq!(pager.flush_backlog(), 8);

        // 再灌满一个 chunk: 超限 swap 应退化同步落盘 (backlog 不增, 直接进 chunk_list)
        let cache_before = pager.chunk_cache_len();
        let mut synced = false;
        for _ in 0..300 {
            let page = Box::new([0xCDu8; PAGE_SIZE]);
            pager.create(page).await.unwrap();
            if pager.chunk_cache_len() > cache_before {
                synced = true;
                break;
            }
            assert!(
                pager.flush_backlog() <= 8,
                "backlog must not grow past MAX_INFLIGHT_CHUNKS"
            );
        }
        assert!(synced, "over-limit swap must sync-write into chunk_list");
        assert_eq!(pager.flush_backlog(), 8, "backlog capped at limit");
    });
}
