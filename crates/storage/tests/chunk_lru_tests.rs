//! T5 ChunkList 测试: 1MB chunk 只读 LRU 缓存.
//!
//! 设计 (DESIGN §3.1 + §3.0.5 chunk_list 不可修改):
//! - **只读 cache**: chunk 字节永不修改. 多 reader 可 clone Arc 共享, 无 Mutex.
//! - LRU 替换 (front=最新, back=最旧).
//! - 通过 WriteQueue::drain_completed → chunk_list.insert_from_write_queue 注入.
//! - 通过 Pager::read_page (T6) 走 get_or_load 读, miss 时调 load_fn 加载.

use storage::chunk_lru::{ChunkKey, ChunkList};
use storage::chunk_writer::{WriteHandle, WriteQueue};
use storage::types::{DEFAULT_DB_ID, PageKey};

mod common;

use common::run_async;

#[test]
fn chunk_list_insert_and_get() {
    let mut list = ChunkList::new(8);
    let key = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    };
    let mut chunk = vec![0u8; 1024 * 1024];
    chunk[0] = 42;
    list.insert(key, chunk);

    let arc = list.peek(&key).expect("inserted chunk must be peekable");
    assert_eq!(arc[0], 42);
    assert_eq!(arc.len(), 1024 * 1024);
}

#[test]
fn chunk_list_lru_newest_at_front() {
    let mut list = ChunkList::new(8);
    let k1 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    };
    let k2 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 1,
    };
    list.insert(k1, vec![0u8; 1024 * 1024]);
    list.insert(k2, vec![0u8; 1024 * 1024]);

    let order = list.order();
    assert_eq!(order[0], k2, "newest should be at front");
    assert_eq!(order[1], k1);
}

#[test]
fn chunk_list_evicts_oldest_on_overflow() {
    let mut list = ChunkList::new(2);
    let k1 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    };
    let k2 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 1,
    };
    let k3 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 2,
    };
    list.insert(k1, vec![0u8; 1024 * 1024]);
    list.insert(k2, vec![0u8; 1024 * 1024]);
    list.insert(k3, vec![0u8; 1024 * 1024]);

    assert_eq!(list.len(), 2);
    assert!(!list.contains(&k1), "k1 (oldest) should be evicted");
    assert!(list.contains(&k2));
    assert!(list.contains(&k3));
}

#[test]
fn chunk_list_hit_moves_key_to_front() {
    let mut list = ChunkList::new(2);
    let k1 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    };
    let k2 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 1,
    };
    let k3 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 2,
    };
    list.insert(k1, vec![0u8; 1024 * 1024]);
    list.insert(k2, vec![0u8; 1024 * 1024]);
    let _ = list.peek(&k1).expect("k1 should be cached");
    list.insert(k3, vec![0u8; 1024 * 1024]);
    assert!(list.contains(&k1), "k1 promoted to front, still cached");
    assert!(!list.contains(&k2), "k2 now oldest, should be evicted");
    assert!(list.contains(&k3));
}

#[test]
fn chunk_list_invalidate_removes_entry() {
    let mut list = ChunkList::new(2);
    let k1 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    };
    list.insert(k1, vec![0u8; 1024 * 1024]);
    list.invalidate(&k1);
    assert_eq!(list.len(), 0);
    assert!(!list.contains(&k1));
    assert!(list.peek(&k1).is_none());
}

#[test]
fn chunk_list_get_or_load_hit() {
    run_async(async move {
        let mut list = ChunkList::new(8);
        let k = ChunkKey {
            db: DEFAULT_DB_ID,
            file_id: 0,
            chunk_idx: 0,
        };
        list.insert(k, vec![1u8; 1024 * 1024]);

        let loaded_count = std::cell::Cell::new(0u32);
        let arc = list
            .get_or_load(k, || {
                loaded_count.set(loaded_count.get() + 1);
                Ok::<_, std::io::Error>(vec![0u8; 1024 * 1024])
            })
            .unwrap();
        assert_eq!(arc[0], 1, "should return cached");
        assert_eq!(loaded_count.get(), 0, "load_fn should NOT be called on hit");
    });
}

#[test]
fn chunk_list_get_or_load_miss() {
    let mut list = ChunkList::new(8);
    let k = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    };
    let mut data = vec![7u8; 1024 * 1024];
    data[100] = 99;

    let arc = list
        .get_or_load(k, || Ok::<_, std::io::Error>(data.clone()))
        .unwrap();
    assert_eq!(arc[100], 99);
    assert_eq!(list.len(), 1);
    // 再次 hit, load_fn panic 测试不调
    let arc2 = list
        .get_or_load(k, || panic!("load_fn should not be called on second hit"))
        .unwrap();
    assert_eq!(arc2[100], 99);
    assert!(
        std::sync::Arc::ptr_eq(&arc, &arc2),
        "peek/get_or_load must return same Arc"
    );
}

#[test]
fn chunk_list_peek_returns_arc() {
    let mut list = ChunkList::new(4);
    let k = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 1,
        chunk_idx: 3,
    };
    list.insert(k, vec![5u8; 1024 * 1024]);

    let arc = list.peek(&k).unwrap();
    let arc2 = list.peek(&k).unwrap();
    assert!(
        std::sync::Arc::ptr_eq(&arc, &arc2),
        "peek must return clone Arc, sharing bytes"
    );
    assert_eq!(arc[0], 5);
}

#[test]
fn chunk_list_capacity_limit() {
    let mut list = ChunkList::new(3);
    for i in 0..3 {
        list.insert(
            ChunkKey {
                db: DEFAULT_DB_ID,
                file_id: 0,
                chunk_idx: i,
            },
            vec![0u8; 1024 * 1024],
        );
    }
    assert_eq!(list.len(), 3);
    assert_eq!(list.capacity(), 3);

    list.insert(
        ChunkKey {
            db: DEFAULT_DB_ID,
            file_id: 0,
            chunk_idx: 3,
        },
        vec![0u8; 1024 * 1024],
    );
    assert_eq!(list.len(), 3, "eviction must keep size at capacity");
    assert!(!list.contains(&ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0
    }));
}

#[test]
fn chunk_list_insert_from_write_queue_drains_completed() {
    let mut list = ChunkList::new(4);
    let mut wq = WriteQueue::new();
    let k1 = PageKey {
        file_id: 0,
        chunk_idx: 0,
    };
    let k2 = PageKey {
        file_id: 0,
        chunk_idx: 1,
    };
    wq.enqueue(WriteHandle::new(k1, vec![1u8; 1024 * 1024]));
    wq.enqueue(WriteHandle::new(k2, vec![2u8; 1024 * 1024]));
    wq.mark_completed(k1);
    wq.mark_completed(k2);

    let drained = wq.drain_completed();
    for h in drained {
        list.insert_from_write_queue(h.key, h.chunk);
    }
    assert_eq!(list.len(), 2);
    assert!(list.contains(&ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0
    }));
    assert!(list.contains(&ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 1
    }));
}

#[test]
fn chunk_list_order_helper() {
    let mut list = ChunkList::new(3);
    let k1 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    };
    let k2 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 1,
    };
    list.insert(k1, vec![0u8; 1024 * 1024]);
    list.insert(k2, vec![0u8; 1024 * 1024]);
    let order = list.order();
    assert_eq!(order, vec![k2, k1], "newest first");
}

// =====================================================================
// ⭐ 增强稳定性测试: 边界 / 并发 reader / chunk 大小校验
// =====================================================================

#[test]
fn chunk_list_multi_file_id_isolation() {
    // 不同 file_id 的 chunk 在 LRU 里独立
    let mut list = ChunkList::new(8);
    let k_file0_chunk0 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    };
    let k_file1_chunk0 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 1,
        chunk_idx: 0,
    };
    list.insert(k_file0_chunk0, vec![1u8; 1024 * 1024]);
    list.insert(k_file1_chunk0, vec![2u8; 1024 * 1024]);
    assert!(list.contains(&k_file0_chunk0));
    assert!(list.contains(&k_file1_chunk0));
    assert_ne!(
        list.peek(&k_file0_chunk0).unwrap().as_ref()[0],
        list.peek(&k_file1_chunk0).unwrap().as_ref()[0],
        "different files should hold different bytes"
    );
}

#[test]
fn chunk_list_eviction_is_lru_correct_under_repeated_access() {
    // 反复访问同一 key 应阻止它被淘汰
    let mut list = ChunkList::new(2);
    let k1 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    };
    let k2 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 1,
    };
    let k3 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 2,
    };

    list.insert(k1, vec![0u8; 1024 * 1024]);
    list.insert(k2, vec![0u8; 1024 * 1024]);

    // 反复访问 k1 (10 次)
    for _ in 0..10 {
        let _ = list.peek(&k1).expect("k1 should remain");
    }

    // 加 k3: 应淘汰 k2 (现在最旧), k1 保持
    list.insert(k3, vec![0u8; 1024 * 1024]);
    assert!(list.contains(&k1), "k1 promoted to front, must remain");
    assert!(!list.contains(&k2), "k2 now oldest, evicted");
    assert!(list.contains(&k3));
}

#[test]
fn chunk_list_concurrent_arc_readers_share_same_bytes() {
    // 多 reader (clone Arc) 共享同一 chunk, 字节不可变.
    // 测试: Arc::strong_count 正确递增, 内容一致.
    let mut list = ChunkList::new(4);
    let k = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    };
    list.insert(k, vec![7u8; 1024 * 1024]);

    let arc1 = list.peek(&k).unwrap();
    assert_eq!(std::sync::Arc::strong_count(&arc1), 2, "list + arc1");

    let arc2 = list.peek(&k).unwrap();
    assert_eq!(std::sync::Arc::strong_count(&arc1), 3, "list + arc1 + arc2");

    let arc3 = list.peek(&k).unwrap();
    assert_eq!(std::sync::Arc::strong_count(&arc1), 4);

    drop(arc2);
    assert_eq!(std::sync::Arc::strong_count(&arc1), 3);

    drop(arc3);
    assert_eq!(std::sync::Arc::strong_count(&arc1), 2);
}

#[test]
fn chunk_list_insert_then_invalidate_releases_arc() {
    let mut list = ChunkList::new(4);
    let k = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    };
    list.insert(k, vec![0u8; 1024 * 1024]);

    let arc = list.peek(&k).unwrap();
    assert_eq!(std::sync::Arc::strong_count(&arc), 2);

    // invalidate 不应立刻 drop chunk 字节 (因 arc 还持有)
    list.invalidate(&k);
    assert!(!list.contains(&k));
    assert_eq!(std::sync::Arc::strong_count(&arc), 1, "list dropped");

    // arc 仍有效 (drop 后才真释放)
    assert_eq!(arc.len(), 1024 * 1024);
}

#[test]
fn chunk_list_eviction_keeps_pinned_arc_alive() {
    let mut list = ChunkList::new(2);
    let k1 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    };
    list.insert(k1, vec![0u8; 1024 * 1024]);

    let arc_k1 = list.peek(&k1).unwrap();
    assert_eq!(arc_k1.len(), 1024 * 1024);

    // 插满再插触发 evict k1, 但 arc_k1 仍应有效 (Arc 引用计数)
    let k2 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 1,
    };
    list.insert(k2, vec![0u8; 1024 * 1024]);
    let k3 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 2,
    };
    list.insert(k3, vec![0u8; 1024 * 1024]);

    assert!(!list.contains(&k1), "k1 evicted from list");
    // 但 arc_k1 字节仍有效 (Arc 引用计数保证)
    assert_eq!(
        arc_k1[0], 0,
        "evicted chunk's bytes still accessible via Arc"
    );
}

#[test]
fn chunk_list_get_or_load_then_insert_deduplicates() {
    // get_or_load 后再 insert 同一 key: 不应重复, Arc 共享
    let mut list = ChunkList::new(4);
    let k = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    };

    let arc1 = list
        .get_or_load(k, || Ok::<_, std::io::Error>(vec![1u8; 1024 * 1024]))
        .unwrap();

    // insert 同一 key 不同 bytes: 应 hit (move to front) 不重复插入
    list.insert(k, vec![2u8; 1024 * 1024]);

    let arc2 = list.peek(&k).unwrap();
    assert!(
        std::sync::Arc::ptr_eq(&arc1, &arc2),
        "insert after get_or_load should not duplicate (Arc shared)"
    );
    assert_eq!(arc2[0], 1, "first load wins (LSM immutability)");
}

#[test]
fn chunk_list_capacity_one_evicts_on_each_insert() {
    let mut list = ChunkList::new(1);
    let k1 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    };
    let k2 = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 1,
    };

    list.insert(k1, vec![1u8; 1024 * 1024]);
    assert_eq!(list.len(), 1);
    assert!(list.contains(&k1));

    list.insert(k2, vec![2u8; 1024 * 1024]);
    assert_eq!(list.len(), 1, "capacity 1 must evict");
    assert!(!list.contains(&k1));
    assert!(list.contains(&k2));
}

#[test]
fn chunk_list_invalidate_nonexistent_is_noop() {
    let mut list = ChunkList::new(4);
    let k = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 99,
    };
    list.invalidate(&k); // 不应 panic
    assert_eq!(list.len(), 0);
}

#[test]
fn chunk_list_chunk_size_mismatch_panics() {
    let mut list = ChunkList::new(4);
    let k = ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    };
    // 1MB 是硬约束, 给错大小应 debug_assert (panic in debug)
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        list.insert(k, vec![0u8; 100]);
    }));
    assert!(
        result.is_err(),
        "insert with wrong chunk size should panic in debug"
    );
}
