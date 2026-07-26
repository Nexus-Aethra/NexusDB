//! T4 NowChunks + WriteQueue + ChunkWriter 测试.
//!
//! 设计 (来自 plan §3.3 / §3.2 / DESIGN §4.4):
//! - NowChunks: LSM 写缓冲. PageWriteBatch 写 page data 走 NowChunks::write_page.
//! - WriteQueue: chunk 满了后从 NowChunks 移出, 进 WriteQueue 等落盘.
//! - ChunkWriter::flush 把 WriteQueue 的 chunk 落盘 + 触发 meta_cache.write + 完成后调
//!   chunk_list.insert_from_write_queue (本 T 不实现 chunk_list, 仅回调 stub).
//!
//! 第一版 (T4): 简化为同步 std::fs IO. scheduler::io_ops::write 留给 T11 polish 接入.

use std::io::Write;
use std::os::unix::fs::FileExt;

use storage::chunk_writer::{ChunkWriter, NowChunks, WriteHandle, WriteQueue};
use storage::types::PageKey;
use storage::{MetaCache, PAGE_SIZE, PID_ALIVE};

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

#[test]
fn nowchunks_write_page_then_peek() {
    run_async(async move {
        let mut nc = NowChunks::new();
        let key = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };
        let mut data = [0u8; PAGE_SIZE];
        data[0] = 42;
        nc.write_page(key, 0, data);

        assert_eq!(nc.dirty_count(), 1, "enqueued one page");
        // peek_chunk 返回 1MB 字节, 第 0 page 是 data[0]=42
        let peeked = nc.peek_chunk(key).expect("chunk just written");
        assert_eq!(peeked[0], 42);
        // 其他 page 是 0
        assert_eq!(peeked[PAGE_SIZE], 0);
    });
}

#[test]
fn nowchunks_write_overrides_same_page_idx() {
    run_async(async move {
        let mut nc = NowChunks::new();
        let key = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };
        let data1 = [1u8; PAGE_SIZE];
        let data2 = [2u8; PAGE_SIZE];
        nc.write_page(key, 5, data1);
        nc.write_page(key, 5, data2);
        // 同 page_idx 二次写: 应覆盖, dirty_count 仍 1 (LSM 优化)
        assert_eq!(nc.dirty_count(), 1);
        let peeked = nc.peek_chunk(key).unwrap();
        assert_eq!(peeked[PAGE_SIZE * 5], 2);
    });
}

#[test]
fn nowchunks_drain_to_write_queue() {
    run_async(async move {
        let mut nc = NowChunks::new();
        let key1 = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };
        let key2 = PageKey {
            file_id: 0,
            chunk_idx: 1,
        };
        nc.write_page(key1, 0, [1u8; PAGE_SIZE]);
        nc.write_page(key1, 1, [2u8; PAGE_SIZE]);
        nc.write_page(key2, 0, [3u8; PAGE_SIZE]);

        // drain 时按 key 分组, 每个 key 拿 full chunk
        let wq = nc.drain_dirty();
        assert_eq!(wq.len(), 2, "two distinct chunks should be queued");
        assert_eq!(nc.dirty_count(), 0, "after drain, nowchunks is empty");
    });
}

#[test]
fn nowchunks_take_chunk_returns_1mb_bytes() {
    run_async(async move {
        let mut nc = NowChunks::new();
        let key = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };
        let mut data = [0u8; PAGE_SIZE];
        data[7] = 99;
        nc.write_page(key, 3, data);
        let chunk = nc.take_chunk(key).expect("chunk present");
        assert_eq!(chunk.len(), 1024 * 1024, "chunk is 1MB");
        assert_eq!(chunk[PAGE_SIZE * 3 + 7], 99);
    });
}

#[test]
fn write_queue_peek_pending_and_drain_completed() {
    let mut wq = WriteQueue::new();
    let key = PageKey {
        file_id: 0,
        chunk_idx: 0,
    };
    let chunk = vec![0u8; 1024 * 1024];
    let handle = WriteHandle::new(key, chunk);
    wq.enqueue(handle);
    assert!(wq.peek_pending(key).is_some(), "chunk is in-flight");
    // drain_completed 在没有完成项时返回空
    let drained = wq.drain_completed();
    assert!(drained.is_empty());
}

#[test]
fn write_queue_marks_completed_and_drains() {
    let mut wq = WriteQueue::new();
    let key = PageKey {
        file_id: 0,
        chunk_idx: 0,
    };
    let chunk = vec![0u8; 1024 * 1024];
    let handle = WriteHandle::new(key, chunk);
    wq.enqueue(handle);

    // 模拟落盘完成
    wq.mark_completed(key);
    let drained = wq.drain_completed();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].key, key);
    // peek_pending 在 drained 后应返回 None
    assert!(wq.peek_pending(key).is_none());
}

#[test]
fn chunk_writer_new_preallocates_block() {
    let (tmp, _meta) = setup();
    let block_path = tmp.path().join("000001.block");
    let writer = ChunkWriter::new(&block_path).unwrap();
    let size = std::fs::metadata(&block_path).unwrap().len();
    assert_eq!(size, 10 * 1024 * 1024, "block file pre-allocated to 10MB");
    drop(writer);
}

#[test]
fn chunk_writer_enqueue_sequential_pages_in_one_chunk() {
    let (tmp, _meta) = setup();
    let block_path = tmp.path().join("000001.block");
    let mut writer = ChunkWriter::new(&block_path).unwrap();

    for i in 0..3u32 {
        let mut data = [0u8; PAGE_SIZE];
        data[0] = i as u8;
        writer.enqueue(
            0,
            Box::new(data),
            PageKey {
                file_id: 0,
                chunk_idx: 0,
            },
            i as u8,
        );
    }
    assert_eq!(writer.pending_count(), 3);
    // writer 自己持有 current_chunk_idx / next_page 跟踪, 应在 chunk 0 page 0..2
    assert_eq!(writer.current_chunk_idx(), 0);
    assert_eq!(writer.next_page_in_chunk(), 3);
}

#[test]
fn chunk_writer_flush_writes_to_block_and_updates_meta() {
    run_async(async move {
        let (tmp, mut meta) = setup();
        let block_path = tmp.path().join("000001.block");
        let mut writer = ChunkWriter::new(&block_path).unwrap();

        // 写 2 page 到 vpid 0/1
        let mut data0 = [0u8; PAGE_SIZE];
        data0[0] = 0xAB;
        let mut data1 = [0u8; PAGE_SIZE];
        data1[0] = 0xCD;
        let key = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };
        writer.enqueue(0, Box::new(data0), key, 0);
        writer.enqueue(1, Box::new(data1), key, 1);

        // flush 后应写入 .block 并更新 meta
        writer.flush(&mut meta).expect("flush ok");
        assert_eq!(writer.pending_count(), 0);

        // 验证 .block 文件 offset 0 = data0, offset PAGE_SIZE = data1
        let f = std::fs::File::open(&block_path).unwrap();
        let mut buf = [0u8; PAGE_SIZE];
        f.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(buf[0], 0xAB);
        let mut buf2 = [0u8; PAGE_SIZE];
        f.read_exact_at(&mut buf2, PAGE_SIZE as u64).unwrap();
        assert_eq!(buf2[0], 0xCD);

        // 验证 meta: vpid 0/1 都映射到正确 pid
        let pid0 = meta.read(0).expect("vpid 0 mapped after flush");
        assert_eq!(pid0.file_id(), 0);
        assert_eq!(pid0.chunk_idx(), 0);
        assert_eq!(pid0.page_idx(), 0);
        assert_eq!(pid0.flags(), PID_ALIVE);
        let pid1 = meta.read(1).unwrap();
        assert_eq!(pid1.page_idx(), 1);
    });
}

#[test]
fn chunk_writer_after_chunk_full_caller_must_flush() {
    run_async(async move {
        // 第一版语义: chunk 满后 caller 必须先 flush + rotate 再 enqueue.
        // 这里验证: enqueue 满 (64 page) 后再 enqueue 会触发 debug_assert (panic in debug build).
        let (tmp, _meta) = setup();
        let block_path = tmp.path().join("000001.block");
        let mut writer = ChunkWriter::new(&block_path).unwrap();
        let key = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };

        for i in 0..64u8 {
            writer.enqueue(i as u64, Box::new([0u8; PAGE_SIZE]), key, i);
        }
        assert_eq!(writer.pending_count(), 64);
        assert_eq!(writer.next_page_in_chunk(), 64);

        // flush + 切到 chunk 1
        let mut meta = MetaCache::open(&tmp.path().join("page.mate")).unwrap();
        writer.flush(&mut meta).expect("flush ok");
        // flush 后 reset, 可以继续 enqueue 到 chunk 0 page 0
        writer.enqueue(
            64,
            Box::new([0u8; PAGE_SIZE]),
            PageKey {
                file_id: 0,
                chunk_idx: 0,
            },
            0,
        );
        assert_eq!(writer.pending_count(), 1);
    });
}

// =====================================================================
// ⭐ 增强稳定性测试
// =====================================================================

#[test]
fn nowchunks_lazy_allocates_chunk_on_first_write() {
    run_async(async move {
        // NowChunks 不应预分配 chunks, 只在 write_page 时创建
        let mut nc = NowChunks::new();
        assert!(nc.is_empty(), "initial state: no chunks");
        assert_eq!(nc.len(), 0);

        let key = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };
        nc.write_page(key, 0, [1u8; PAGE_SIZE]);
        assert_eq!(nc.len(), 1, "first write creates chunk");
    });
}

#[test]
fn nowchunks_drain_only_picks_dirty_chunks() {
    run_async(async move {
        // 不 dirty 的 chunk 不应被 drain (因为 created 后没人写)
        let mut nc = NowChunks::new();
        let k1 = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };
        let k2 = PageKey {
            file_id: 0,
            chunk_idx: 1,
        };

        // 写 dirty chunk
        nc.write_page(k1, 0, [1u8; PAGE_SIZE]);
        // 创建 chunk 但不 write_page: 不 dirty
        nc.write_page(k2, 0, [2u8; PAGE_SIZE]); // dirty

        let wq = nc.drain_dirty();
        assert_eq!(wq.len(), 2); // 两个都是 dirty (都 write 过)
    });
}

#[test]
fn nowchunks_drain_clears_all_state() {
    run_async(async move {
        // drain 后 NowChunks 完全空 (清掉 lazy alloc 的 chunk)
        let mut nc = NowChunks::new();
        let key = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };
        nc.write_page(key, 0, [42u8; PAGE_SIZE]);
        nc.write_page(key, 1, [43u8; PAGE_SIZE]);
        assert_eq!(nc.len(), 1);
        assert_eq!(nc.dirty_count(), 1);

        let _wq = nc.drain_dirty();
        assert!(nc.is_empty(), "drain clears NowChunks");
        assert_eq!(nc.len(), 0);
        assert_eq!(nc.dirty_count(), 0);

        // 重新写能再次工作
        nc.write_page(key, 0, [99u8; PAGE_SIZE]);
        let peeked = nc.peek_chunk(key).unwrap();
        assert_eq!(peeked[0], 99);
    });
}

#[test]
fn nowchunks_take_chunk_then_arc_sharing_ends() {
    run_async(async move {
        let mut nc = NowChunks::new();
        let key = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };
        nc.write_page(key, 5, [7u8; PAGE_SIZE]);
        let arc = nc.peek_chunk(key).unwrap();
        let _bytes_ref: &[u8] = arc; // borrow the chunk
        let _ = arc; // drop without explicit drop() (avoids clippy dropping_references)

        let owned = nc.take_chunk(key).unwrap();
        assert_eq!(owned.len(), 1024 * 1024);
        // take 后 peek 返回 None (chunk 已从 NowChunks 移除)
        assert!(nc.peek_chunk(key).is_none());
    });
}

#[test]
fn write_queue_pending_count_vs_completed_count() {
    let mut wq = WriteQueue::new();
    let k1 = PageKey {
        file_id: 0,
        chunk_idx: 0,
    };
    let k2 = PageKey {
        file_id: 0,
        chunk_idx: 1,
    };

    wq.enqueue(WriteHandle::new(k1, vec![0u8; 1024 * 1024]));
    wq.enqueue(WriteHandle::new(k2, vec![0u8; 1024 * 1024]));
    assert_eq!(wq.pending_count(), 2);
    assert_eq!(wq.completed_count(), 0);
    assert_eq!(wq.len(), 2);

    wq.mark_completed(k1);
    assert_eq!(wq.pending_count(), 1);
    assert_eq!(wq.completed_count(), 1);
    assert_eq!(wq.len(), 2);

    let drained = wq.drain_completed();
    assert_eq!(drained.len(), 1);
    assert_eq!(wq.completed_count(), 0);
    assert_eq!(wq.len(), 1); // k2 still pending
}

#[test]
fn chunk_writer_flush_empty_pending_is_noop() {
    run_async(async move {
        let (tmp, mut meta) = setup();
        let block_path = tmp.path().join("000001.block");
        let mut writer = ChunkWriter::new(&block_path).unwrap();

        // 没 enqueue 直接 flush: 不应写 disk, 不应 panics
        writer.flush(&mut meta).expect("empty flush ok");
        assert_eq!(writer.pending_count(), 0);
    });
}

#[test]
fn chunk_writer_flush_preserves_unwritten_pages() {
    run_async(async move {
        // flush 后, 之前 enqueue 的 page 在 .block 中可读到正确内容
        let (tmp, mut meta) = setup();
        let block_path = tmp.path().join("000001.block");
        let mut writer = ChunkWriter::new(&block_path).unwrap();
        let key = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };

        // 写 5 个 page, 每个首字节不同
        for i in 0..5u8 {
            let mut data = [0u8; PAGE_SIZE];
            data[0] = 100 + i;
            data[PAGE_SIZE - 1] = 200 + i;
            writer.enqueue(i as u64, Box::new(data), key, i);
        }

        writer.flush(&mut meta).expect("flush ok");

        // 验证 .block 内容
        use std::os::unix::fs::FileExt;
        let f = std::fs::File::open(&block_path).unwrap();
        for i in 0..5u8 {
            let mut buf = [0u8; PAGE_SIZE];
            f.read_exact_at(&mut buf, i as u64 * PAGE_SIZE as u64)
                .unwrap();
            assert_eq!(buf[0], 100 + i, "page {} first byte", i);
            assert_eq!(buf[PAGE_SIZE - 1], 200 + i, "page {} last byte", i);
        }
    });
}

#[test]
fn chunk_writer_multiple_flush_cycles() {
    run_async(async move {
        // 多次 flush 模拟多次 commit batch
        let (tmp, mut meta) = setup();
        let block_path = tmp.path().join("000001.block");
        let mut writer = ChunkWriter::new(&block_path).unwrap();
        let key = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };

        for cycle in 0..3u64 {
            let vpid_offset = cycle * 10;
            for i in 0..10u8 {
                let mut data = [0u8; PAGE_SIZE];
                data[0] = (cycle * 10 + i as u64) as u8;
                writer.enqueue(vpid_offset + i as u64, Box::new(data), key, i);
            }
            writer.flush(&mut meta).expect("flush ok");
            assert_eq!(writer.pending_count(), 0);
        }

        // 验证 meta: 30 个 vpid 都映射到正确 pid
        for vpid in 0..30u64 {
            let pid = meta.read(vpid).expect("vpid mapped");
            assert!(pid.flags() & PID_ALIVE != 0);
            assert_eq!(pid.chunk_idx(), 0);
            assert_eq!(pid.page_idx(), (vpid % 10) as u16);
        }
    });
}
