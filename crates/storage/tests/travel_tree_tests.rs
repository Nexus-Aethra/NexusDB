//! T6 TravelTree 集成测试 (DESIGN §3.0.1 + plan §3.0.1).
//!
//! 设计要点:
//! - TravelTree 是 task-private, 用于 B+Tree split 传播时更新栈路径.
//! - record: 向下 travel 时记录 (separator_key, child_vpid).
//! - lookup: 向上回推时用 key 拿最新 vpid.
//! - range_update: split 广播, 把右节点范围内的旧 vpid 替换为新 vpid.
//! - find_all_with_vpid: 调试用, 找所有指向某 vpid 的 key.
//! - TravelTreeGuard RAII: drop 时自动 unregister.

use std::io::Write;

use storage::alloc::{PidAllocator, VpidAllocator};
use storage::chunk_lru::ChunkList;
use storage::chunk_writer::{ChunkWriter, NowChunks};
use storage::pager::{Pager, TravelTree};
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

// =====================================================================
// ⭐ TravelTree 基本操作
// =====================================================================

#[test]
fn travel_tree_basic_record_lookup() {
    let mut t = TravelTree::new();
    t.record(b"a".to_vec(), 10);
    t.record(b"m".to_vec(), 20);
    t.record(b"z".to_vec(), 30);

    assert_eq!(t.lookup(b"a"), Some(10));
    assert_eq!(t.lookup(b"m"), Some(20));
    assert_eq!(t.lookup(b"z"), Some(30));
    assert_eq!(t.lookup(b"missing"), None);
    assert_eq!(t.len(), 3);
    assert!(!t.is_empty());
}

#[test]
fn travel_tree_record_overwrites_existing() {
    // 同一 key 多次 record: 后者覆盖前者
    let mut t = TravelTree::new();
    t.record(b"k".to_vec(), 1);
    t.record(b"k".to_vec(), 2);
    t.record(b"k".to_vec(), 3);
    assert_eq!(t.lookup(b"k"), Some(3));
    assert_eq!(t.len(), 1, "同一 key 多次 record 仍只占 1 个 slot");
}

#[test]
fn travel_tree_default_is_empty() {
    let t = TravelTree::default();
    assert!(t.is_empty());
    assert_eq!(t.len(), 0);
    assert_eq!(t.lookup(b"any"), None);
}

// =====================================================================
// ⭐ split 传播: range_update 模拟
// =====================================================================

#[test]
fn travel_tree_range_update_replaces_old_vpid_in_range() {
    // 模拟 split: right page vpid 30, right_lo = "m", right_hi = "z" (开区间, 不含)
    // 旧 left page vpid 10 持有的 key 在 [m, z) 范围内 → 替换为 right vpid 30
    let mut t = TravelTree::new();
    t.record(b"a".to_vec(), 10);
    t.record(b"m".to_vec(), 10); // 临界: lo, 算在 right 范围
    t.record(b"p".to_vec(), 10);
    t.record(b"y".to_vec(), 10);
    t.record(b"z".to_vec(), 10); // 临界: hi, 不在 [m, z)
    t.record(b"b".to_vec(), 20); // value 不是 10, 不变

    t.range_update(b"m", b"z", 10, 30);

    assert_eq!(t.lookup(b"a"), Some(10), "a 不在 [m, z), 不变");
    assert_eq!(t.lookup(b"m"), Some(30), "m 在 [m, z) 范围, 替换为 30");
    assert_eq!(t.lookup(b"p"), Some(30), "p 在 [m, z) 范围, 替换为 30");
    assert_eq!(t.lookup(b"y"), Some(30), "y 在 [m, z) 范围, 替换为 30");
    assert_eq!(t.lookup(b"z"), Some(10), "z 是 hi, 不在 [m, z), 不变");
    assert_eq!(t.lookup(b"b"), Some(20), "b 的 value 不是 10, 不变");
}

#[test]
fn travel_tree_range_update_empty_range_no_op() {
    let mut t = TravelTree::new();
    t.record(b"a".to_vec(), 5);
    t.record(b"b".to_vec(), 5);
    // 范围 [c, d) 没匹配任何 key
    t.range_update(b"c", b"d", 5, 99);
    assert_eq!(t.lookup(b"a"), Some(5));
    assert_eq!(t.lookup(b"b"), Some(5));
}

#[test]
fn travel_tree_range_update_no_matching_vpid_no_op() {
    let mut t = TravelTree::new();
    t.record(b"a".to_vec(), 5);
    t.record(b"b".to_vec(), 5);
    // 范围内但 vpid 不匹配
    t.range_update(b"a", b"z", 999, 88);
    assert_eq!(t.lookup(b"a"), Some(5));
    assert_eq!(t.lookup(b"b"), Some(5));
}

// =====================================================================
// ⭐ find_all_with_vpid: 调试
// =====================================================================

#[test]
fn travel_tree_find_all_with_vpid() {
    let mut t = TravelTree::new();
    t.record(b"x".to_vec(), 7);
    t.record(b"y".to_vec(), 8);
    t.record(b"z".to_vec(), 7);
    t.record(b"w".to_vec(), 9);

    let mut found = t.find_all_with_vpid(7);
    found.sort();
    let mut expected = vec![b"x".to_vec(), b"z".to_vec()];
    expected.sort();
    assert_eq!(found, expected);

    let found_8 = t.find_all_with_vpid(8);
    assert_eq!(found_8, vec![b"y".to_vec()]);

    let found_none = t.find_all_with_vpid(999);
    assert!(found_none.is_empty());
}

// =====================================================================
// ⭐ split 传播综合场景
// =====================================================================

#[test]
fn travel_tree_split_propagation_scenario() {
    // 模拟真实 B+Tree split 流程:
    // 1. task 向下 travel, 记录 (key, vpid) 到 travel_tree
    // 2. 某个 page 触发 split, 生成 right page (新 vpid)
    // 3. 通过 range_update 把 travel_tree 中指向 left page 的 key 替换为 right vpid
    // 4. task 向上回推时用 lookup 拿最新 vpid

    let mut t = TravelTree::new();

    // 步骤 1: 向下 travel
    t.record(b"k1".to_vec(), 5); // root → child 5
    t.record(b"k2".to_vec(), 8); // 5 → child 8
    t.record(b"k3".to_vec(), 12); // 8 → child 12

    assert_eq!(t.lookup(b"k1"), Some(5));
    assert_eq!(t.lookup(b"k2"), Some(8));
    assert_eq!(t.lookup(b"k3"), Some(12));

    // 步骤 2+3: page 8 触发 split, right page vpid = 88, right_lo = "k2", right_hi = "k3"
    // 假设 travel_tree 中 page 8 的范围是 [k2, k3)
    // 所以 range_update 应只更新 k2 那个条目
    t.range_update(b"k2", b"k3", 8, 88);

    assert_eq!(t.lookup(b"k1"), Some(5), "k1 仍指向 root, 不变");
    assert_eq!(t.lookup(b"k2"), Some(88), "k2 指向新 vpid 88 (split)");
    assert_eq!(t.lookup(b"k3"), Some(12), "k3 不在 [k2, k3), 不变");
}

#[test]
fn travel_tree_multiple_splits_propagate_correctly() {
    // 连续两次 split 传播
    let mut t = TravelTree::new();
    t.record(b"a".to_vec(), 100);
    t.record(b"b".to_vec(), 100);
    t.record(b"c".to_vec(), 100);
    t.record(b"d".to_vec(), 100);

    // 第一次 split: vpid 100 → 200, 范围 [b, c) (c 不含)
    t.range_update(b"b", b"c", 100, 200);
    assert_eq!(t.lookup(b"a"), Some(100));
    assert_eq!(t.lookup(b"b"), Some(200), "b 在 [b, c), 替换为 200");
    assert_eq!(t.lookup(b"c"), Some(100), "c 是 hi, 不替换");
    assert_eq!(t.lookup(b"d"), Some(100));

    // 第二次 split: vpid 200 → 300, 范围 [b, b) (空)
    t.range_update(b"b", b"b", 200, 300);
    assert_eq!(t.lookup(b"a"), Some(100));
    assert_eq!(t.lookup(b"b"), Some(200), "[b, b) 空, 不替换");
    assert_eq!(t.lookup(b"c"), Some(100));
    assert_eq!(t.lookup(b"d"), Some(100));

    // 第三次 split: 范围 [a, b), 应把 a 替换
    t.range_update(b"a", b"b", 100, 400);
    assert_eq!(t.lookup(b"a"), Some(400), "a 在 [a, b), 替换为 400");
    assert_eq!(t.lookup(b"b"), Some(200));
    assert_eq!(t.lookup(b"c"), Some(100));
    assert_eq!(t.lookup(b"d"), Some(100));
}

// =====================================================================
// ⭐ TravelTreeGuard RAII
// =====================================================================

#[test]
fn travel_tree_guard_register_and_unregister() {
    let (tmp, meta) = setup();
    let mut pager = new_pager(&tmp, meta);

    assert!(pager.travel_tree_count() == 0, "初始 travel_trees 应为空");

    // 创建 guard, 验证 register
    {
        let mut guard = pager.travel_tree_guard(42);
        guard.tree().record(b"x".to_vec(), 100);
        assert_eq!(guard.tree().len(), 1);
    }
    // guard 离开作用域, drop 自动 unregister
    assert!(
        !pager.has_travel_tree(42),
        "guard drop 后 travel_trees 应为空"
    );
}

#[test]
fn travel_tree_guard_multiple_tasks_isolated() {
    let (tmp, meta) = setup();
    let mut pager = new_pager(&tmp, meta);

    // 串行创建/使用/销毁 guard, 验证 task-private 隔离
    let g1_len = {
        let mut g = pager.travel_tree_guard(1);
        g.tree().record(b"a".to_vec(), 10);
        let lookup_a = g.tree().lookup(b"a");
        let lookup_b = g.tree().lookup(b"b");
        assert_eq!(lookup_a, Some(10));
        assert_eq!(lookup_b, None, "task 1 看不到 task 2 的 key");
        g.tree().len()
    };
    assert_eq!(g1_len, 1);
    assert!(pager.travel_tree_count() == 0, "task 1 guard drop 后清空");

    let g2_len = {
        let mut g = pager.travel_tree_guard(2);
        g.tree().record(b"b".to_vec(), 20);
        let lookup_b = g.tree().lookup(b"b");
        let lookup_a = g.tree().lookup(b"a");
        assert_eq!(lookup_b, Some(20));
        assert_eq!(lookup_a, None, "task 2 看不到 task 1 的 key");
        g.tree().len()
    };
    assert_eq!(g2_len, 1);
    assert!(pager.travel_tree_count() == 0);
}

#[test]
fn travel_tree_guard_repeated_creation_no_leak() {
    let (tmp, meta) = setup();
    let mut pager = new_pager(&tmp, meta);

    // 多次创建/销毁 guard, 验证不泄漏
    for i in 0..10u64 {
        {
            let mut g = pager.travel_tree_guard(i);
            g.tree().record(b"k".to_vec(), i);
        }
        assert!(
            pager.travel_tree_count() == 0,
            "iter {} drop 后应为空, got {} entries",
            i,
            pager.travel_tree_count()
        );
    }
    assert!(pager.travel_tree_count() == 0);
}

#[test]
fn travel_tree_guard_panic_drops_register() {
    // panic 时 Drop 也会触发, 不会泄漏
    let (tmp, meta) = setup();
    let mut pager = new_pager(&tmp, meta);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = pager.travel_tree_guard(7);
        panic!("oops");
    }));
    assert!(result.is_err());
    // _guard 已被 drop, 不会泄漏
    assert!(
        pager.travel_tree_count() == 0,
        "panic 后 guard 应被 drop, travel_trees 应为空"
    );
}

// =====================================================================
// ⭐ TravelTree + Pager.read 集成: 模拟 task-private 路径
// =====================================================================

#[test]
fn pager_create_page_then_travel_tree_records() {
    run_async(async move {
        // 模拟 B+Tree 操作: task 持 guard, 创建 page, 记录到 travel_tree
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let data = [0u8; PAGE_SIZE];
        let v1 = pager.create(Box::new(data)).await.unwrap();
        let v2 = pager.create(Box::new(data)).await.unwrap();
        let v3 = pager.create(Box::new(data)).await.unwrap();

        // task 持 guard, 记录 travel 路径
        {
            let mut guard = pager.travel_tree_guard(99);
            guard.tree().record(b"a".to_vec(), v1);
            guard.tree().record(b"m".to_vec(), v2);
            guard.tree().record(b"z".to_vec(), v3);
            assert_eq!(guard.tree().len(), 3);
        }
        // guard 离开作用域, travel_trees 清空
        assert!(pager.travel_tree_count() == 0);
    });
}

#[test]
fn pager_read_data_consistent_with_travel_tree_path() {
    run_async(async move {
        // 验证: travel_tree 记录的 vpid 路径, 实际 read 都能拿到正确数据
        let (tmp, meta) = setup();
        let mut pager = new_pager(&tmp, meta);

        let mut data = [0u8; PAGE_SIZE];
        data[0x28] = 0xAA;
        let v = pager.create(Box::new(data)).await.unwrap();

        let recorded_vpid = {
            let mut guard = pager.travel_tree_guard(1);
            guard.tree().record(b"only_key".to_vec(), v);
            guard.tree().lookup(b"only_key").unwrap()
        };
        assert_eq!(recorded_vpid, v);

        // 验证 vpid 真的映射到正确 page (guard 已 drop, 不冲突)
        let r = pager.read(recorded_vpid).await.unwrap();
        assert_eq!(r[0x28], 0xAA);

        assert!(pager.travel_tree_count() == 0);
    });
}
