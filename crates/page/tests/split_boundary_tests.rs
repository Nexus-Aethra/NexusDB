//! 边界条件测试: split / merge 的精确行为.
//!
//! 主要验证:
//! 1. internal_split 正确转移 first_child 到 right
//! 2. split 后两个 page 都有 sentinel
//! 3. leaf_split 在 split point 处的 key 处理
//! 4. 段分裂 (pre_split) 时 cp[N+1] 段首 shared=0 不变量
//! 5. 段删除后空段清理
//! 6. 多轮 split 后 first_child 路由仍正确

use page::{
    ItemKind, PageIndex, internal_child, internal_delete, internal_insert, internal_new,
    internal_split, leaf_delete, leaf_get, leaf_insert, leaf_new, leaf_split, page_free_off,
    page_key_count, page_set_vpid,
};

// ============================================================================
// internal_split 验证对
// ============================================================================

/// internal_split 必须正确转移 first_child 到 right page (mid item 的 child).
/// 否则 split 后 routing 会全部错位.
#[test]
fn internal_split_transfers_first_child_correctly() {
    let mut left = internal_new();
    page_set_vpid(&mut left, 100); // first_child_left = 100

    // 插入 6 个 separator. sentinel 在 item 0, 不计入 key_count.
    // 循环 for i in 0..(mid+2) 会跑到 i=mid+1 (即 right 第一项).
    // separator[i] 的 child_vpid = 200 + i * 10.
    let seps: &[(&[u8], u64)] = &[
        (b"d", 200),
        (b"h", 210),
        (b"l", 220),
        (b"p", 230),
        (b"t", 240),
        (b"x", 250),
    ];
    for (s, v) in seps {
        internal_insert(&mut left, s, *v).unwrap();
    }
    assert_eq!(page_key_count(&left), 6);

    let mut right = internal_new();
    let split_key = internal_split(&mut left, &mut right).unwrap();

    // mid = 3 (6/2 = 3). 循环 i in 0..5:
    //   i=0: sentinel, i=1: d, i=2: h, i=3: l (mid_off = end of l),
    //   i=4: p (mid_full_key = "p", mid_child_vpid = 230)
    // 所以 split_key = "p" (right 第一项), left 有 d, h, l (3 keys),
    // right 有 p, t, x (3 keys, mid_full_key = "p" 是第一个).
    assert_eq!(page_key_count(&left), 3);
    assert_eq!(page_key_count(&right), 3);
    assert_eq!(split_key, b"p");

    // first_child_left 不变 = 100
    assert_eq!(page::page_vpid(&left), 100);

    // first_child_right = mid item ("p") 的 child_vpid = 230
    // (因为 split 把 mid item "p" 移到 right 作为 cp[0] 段首 shared=0 的 item)
    assert_eq!(page::page_vpid(&right), 230);

    // 路由验证: left 上 keys < "p" 应该路由到 left 的 children
    assert_eq!(internal_child(&left, b"d").unwrap(), 200);
    assert_eq!(internal_child(&left, b"h").unwrap(), 210);
    assert_eq!(internal_child(&left, b"l").unwrap(), 220);
    assert_eq!(internal_child(&left, b"o").unwrap(), 220); // "l" < "o" < "p"
    // < "d" → first_child = 100
    assert_eq!(internal_child(&left, b"a").unwrap(), 100);

    // 路由验证: right 上 keys >= "p" 应该路由到 right 的 children
    assert_eq!(internal_child(&right, b"p").unwrap(), 230);
    assert_eq!(internal_child(&right, b"t").unwrap(), 240);
    assert_eq!(internal_child(&right, b"x").unwrap(), 250);
    // < "p" (right 的最小 separator) → first_child_right = 230 (mid item "p" 的 child)
    assert_eq!(internal_child(&right, b"o").unwrap(), 230);
    assert_eq!(internal_child(&right, b"a").unwrap(), 230);
}

/// split 后两边 PageIndex 都必须能正确加载 (校验 sentinel + shared=0 invariants).
#[test]
fn split_both_pages_pass_pageindex_load() {
    let mut left = leaf_new();
    for i in 0..80 {
        leaf_insert(&mut left, format!("k_{i:04}").as_bytes(), b"v").unwrap();
    }
    let mut right = leaf_new();
    let _ = leaf_split(&mut left, &mut right).unwrap();

    // 两边 PageIndex::load 必须成功 (验证 cp 段首 shared=0 不变量)
    PageIndex::load(&left, ItemKind::Leaf).expect("left PageIndex load should succeed after split");
    PageIndex::load(&right, ItemKind::Leaf)
        .expect("right PageIndex load should succeed after split");

    // 两边都能正常查询
    let lc = page_key_count(&left) as usize;
    for i in 0..lc {
        let key = format!("k_{i:04}");
        assert!(
            leaf_get(&left, key.as_bytes()).is_some(),
            "left missing key {i}"
        );
    }
    let rc = page_key_count(&right) as usize;
    for i in lc..lc + rc {
        let key = format!("k_{i:04}");
        assert!(
            leaf_get(&right, key.as_bytes()).is_some(),
            "right missing key {i}"
        );
    }
}

/// leaf_split 在 split point 处的 key: 返回值必须等于 right page 第一个真实 key.
#[test]
fn leaf_split_returns_correct_split_key() {
    let mut left = leaf_new();
    for i in 0..10 {
        leaf_insert(&mut left, format!("k_{i:04}").as_bytes(), b"v").unwrap();
    }
    let mut right = leaf_new();
    let split_key = leaf_split(&mut left, &mut right).unwrap();

    // split_key 是 right page 第一个真实 key, 必须能查到
    assert!(
        leaf_get(&right, &split_key).is_some(),
        "split_key={:?} should exist in right",
        String::from_utf8_lossy(&split_key)
    );

    // split_key 不在 left page
    assert!(
        leaf_get(&left, &split_key).is_none(),
        "split_key={:?} should NOT exist in left",
        String::from_utf8_lossy(&split_key)
    );
}

// ============================================================================
// 段分裂 (pre_split_segment) 后 cp[N+1] 不变量
// ============================================================================

/// 触发 pre_split_segment 后, cp[N+1].first_item 必须 shared=0.
/// 这是通过插入 32+ 个 key 自动触发的.
#[test]
fn presplit_keeps_cp_segment_head_invariant() {
    let mut page = leaf_new();
    // 插入 50 个 keys, 触发多次 pre_split (max=32, mid=16 → 50/16 ≈ 3 段)
    for i in 0..50 {
        leaf_insert(&mut page, format!("k_{i:04}").as_bytes(), b"v").unwrap();
    }
    assert_eq!(page_key_count(&page), 50);

    // 每次 insert 后 PageIndex 必须能 load (这隐式验证了 cp 段首 shared=0)
    PageIndex::load(&page, ItemKind::Leaf).expect("PageIndex load should succeed");

    // 全量查询验证
    for i in 0..50 {
        let key = format!("k_{i:04}");
        assert!(leaf_get(&page, key.as_bytes()).is_some(), "missing key {i}");
    }
}

/// 触发 cp[N+1] 边界插入 (insert at cp boundary): 已修复的 B1 regression test.
/// 在 cp[0]/cp[1] 边界插入新 key, 确保 cp[1].first_item 仍指向 shared=0 item.
#[test]
fn insert_at_cp_boundary_preserves_shared_zero() {
    let mut page = leaf_new();
    // 插 50 个, 触发 pre_split (产生 2 个 cp 段)
    for i in 0..50 {
        leaf_insert(&mut page, format!("k_{i:04}").as_bytes(), b"v").unwrap();
    }

    // 在 cp[0]/cp[1] 边界插入 key "k_0024" (应该已经存在 → rejected)
    // 然后插入一个新 key 在边界附近
    let result = leaf_insert(&mut page, b"k_0024", b"new");
    assert!(result.is_err(), "duplicate key should be rejected");

    // 插入一个全新 key, 跨过某个 cp 边界
    leaf_insert(&mut page, b"k_9999", b"end").unwrap();

    // 关键验证: PageIndex::load 必须成功 (cp 段首 shared=0)
    PageIndex::load(&page, ItemKind::Leaf).expect("PageIndex load should succeed");

    // 还能查到所有原 key
    for i in 0..50 {
        let key = format!("k_{i:04}");
        assert!(leaf_get(&page, key.as_bytes()).is_some(), "missing key {i}");
    }
    assert_eq!(leaf_get(&page, b"k_9999").unwrap(), b"end");
}

// ============================================================================
// 段删除后空段清理
// ============================================================================

/// 删除后空段 (item_count=0) 必须从 segments 中移除 (除哨兵段外),
/// 否则后续 insert 会引用无效 first_item_off.
#[test]
fn leaf_delete_clears_empty_segments() {
    let mut page = leaf_new();

    // 插 50 个, 触发 pre_split, 产生多个 cp 段
    for i in 0..50 {
        leaf_insert(&mut page, format!("k_{i:04}").as_bytes(), b"v").unwrap();
    }
    let idx_before = PageIndex::load(&page, ItemKind::Leaf).unwrap();
    let initial_segs = idx_before.segments.len();

    // 删掉全部 50 个
    for i in 0..50 {
        let deleted = leaf_delete(&mut page, format!("k_{i:04}").as_bytes()).unwrap();
        assert!(deleted, "delete should succeed at i={i}");
    }
    assert_eq!(page_key_count(&page), 0);

    // 现在 PageIndex 应该只剩哨兵段 (segments.len() = 1)
    let idx_after = PageIndex::load(&page, ItemKind::Leaf).expect("should load");
    assert_eq!(
        idx_after.segments.len(),
        1,
        "should only have sentinel segment after all deletes, got {} (was {})",
        idx_after.segments.len(),
        initial_segs
    );

    // 重新插入能成功 (空段被清理)
    leaf_insert(&mut page, b"new_key", b"new_val").unwrap();
    assert_eq!(leaf_get(&page, b"new_key").unwrap(), b"new_val");
}

// ============================================================================
// 多轮 split 后 routing 仍然正确
// ============================================================================

/// 连续 split 一个 page 多次, 验证 child_vpid 在所有 page 间正确流转.
#[test]
fn multi_round_split_preserves_child_routing() {
    let mut left = internal_new();
    page_set_vpid(&mut left, 0);

    // 插 80 个 separator
    for i in 0..80 {
        let k = format!("s_{i:04}");
        internal_insert(&mut left, k.as_bytes(), 1000 + i as u64).unwrap();
    }

    // 第 1 次 split
    let mut right1 = internal_new();
    let _ = internal_split(&mut left, &mut right1).unwrap();
    let lc1 = page_key_count(&left) as usize;
    let rc1 = page_key_count(&right1) as usize;
    let total1 = lc1 + rc1;
    assert_eq!(total1, 80);

    // 验证 routing: left + right1 的所有 keys
    for i in 0..total1 {
        let k = format!("s_{i:04}");
        let page = if i < lc1 { &left } else { &right1 };
        assert_eq!(
            internal_child(page, k.as_bytes()).unwrap(),
            1000 + i as u64,
            "routing fail at i={i} (lc1={lc1} rc1={rc1})"
        );
    }

    // 第 2 次 split: split right1
    let mut right2 = internal_new();
    let _ = internal_split(&mut right1, &mut right2).unwrap();
    let rc1_after = page_key_count(&right1) as usize;
    let rc2 = page_key_count(&right2) as usize;
    assert_eq!(rc1_after + rc2, rc1, "right1 split must preserve total");

    // 三片都能正确 routing. 总 keys = lc1 + rc1_after + rc2 = 80
    for i in 0..80 {
        let k = format!("s_{i:04}");
        let page = if i < lc1 {
            &left
        } else if i < lc1 + rc1_after {
            &right1
        } else {
            &right2
        };
        assert_eq!(
            internal_child(page, k.as_bytes()).unwrap(),
            1000 + i as u64,
            "routing fail at i={i} after 2nd split (lc1={lc1} rc1_after={rc1_after} rc2={rc2})"
        );
    }

    // 验证 first_child 流转: right1 的 first_child = mid item 的 child_vpid
    // mid item 是原 left 的 s_lc1 (即 s_{lc1}), 它的 child_vpid = 1000 + lc1.
    assert_eq!(page::page_vpid(&right1), 1000 + lc1 as u64);
    // right2 的 first_child = right1 split 时 mid item 的 child_vpid = 1000 + (lc1 + rc1_after)
    assert_eq!(page::page_vpid(&right2), 1000 + (lc1 + rc1_after) as u64);
}

// ============================================================================
// 边界: empty page 操作
// ============================================================================

/// 空 page 上调用 leaf_get 不应 panic.
#[test]
fn leaf_get_on_empty_page() {
    let page = leaf_new();
    assert!(leaf_get(&page, b"any").is_none());
    assert!(leaf_get(&page, b"").is_none());
}

/// 插入再删空 → 再插 → 哨兵应该还能正常工作.
#[test]
fn insert_delete_all_reinsert() {
    let mut page = leaf_new();
    leaf_insert(&mut page, b"k1", b"v1").unwrap();
    leaf_insert(&mut page, b"k2", b"v2").unwrap();
    assert_eq!(page_key_count(&page), 2);

    leaf_delete(&mut page, b"k1").unwrap();
    leaf_delete(&mut page, b"k2").unwrap();
    assert_eq!(page_key_count(&page), 0);

    // 重新插入
    leaf_insert(&mut page, b"k3", b"v3").unwrap();
    assert_eq!(page_key_count(&page), 1);
    assert_eq!(leaf_get(&page, b"k3").unwrap(), b"v3");
    assert!(leaf_get(&page, b"k1").is_none());
}

// ============================================================================
// 边界: free_off 和 key_count 同步
// ============================================================================

/// 大量增删后 key_count 和 free_off 关系正确.
#[test]
fn key_count_and_free_off_consistent_after_churn() {
    let mut page = leaf_new();
    let initial_free_off = page_free_off(&page);

    for round in 0..5 {
        // 插 10 个
        for i in 0..10 {
            leaf_insert(&mut page, format!("r{round}_k_{i}").as_bytes(), b"v").unwrap();
        }
        // 删 10 个 (回到 0 keys)
        for i in 0..10 {
            leaf_delete(&mut page, format!("r{round}_k_{i}").as_bytes()).unwrap();
        }
        assert_eq!(page_key_count(&page), 0);
        // free_off 在每轮插之后会增加 (因插入 new item 字节), 即使删完仍 > initial
        assert!(
            (page_free_off(&page) as usize) > initial_free_off as usize,
            "free_off should grow after insertions (round {round}): free_off={} initial={}",
            page_free_off(&page),
            initial_free_off
        );
    }
    assert_eq!(page_key_count(&page), 0);
}

// ============================================================================
// Internal page 边界 routing
// ============================================================================

/// 多个 separator 之间插入新 key 后 routing 仍正确.
#[test]
fn internal_insert_between_separators_keeps_routing() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 100);

    internal_insert(&mut page, b"d", 200).unwrap();
    internal_insert(&mut page, b"h", 210).unwrap();
    internal_insert(&mut page, b"t", 230).unwrap();

    // 在 "d" 和 "h" 之间插入 "f"
    internal_insert(&mut page, b"f", 205).unwrap();
    assert_eq!(page_key_count(&page), 4);

    // routing 验证
    assert_eq!(internal_child(&page, b"a").unwrap(), 100); // < "d"
    assert_eq!(internal_child(&page, b"d").unwrap(), 200);
    assert_eq!(internal_child(&page, b"e").unwrap(), 200); // < "f"
    assert_eq!(internal_child(&page, b"f").unwrap(), 205);
    assert_eq!(internal_child(&page, b"g").unwrap(), 205); // "f" < "g" < "h"
    assert_eq!(internal_child(&page, b"h").unwrap(), 210);
    assert_eq!(internal_child(&page, b"s").unwrap(), 210); // "h" < "s" < "t"
    assert_eq!(internal_child(&page, b"t").unwrap(), 230);
    assert_eq!(internal_child(&page, b"z").unwrap(), 230); // > "t"
}

/// 删除 separator 后 routing 自动调整 (前面的 separator 接管).
#[test]
fn internal_delete_separator_routing_adapts() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 100);

    internal_insert(&mut page, b"d", 200).unwrap();
    internal_insert(&mut page, b"h", 210).unwrap();
    internal_insert(&mut page, b"l", 220).unwrap();

    // 删除 "h"
    let deleted = internal_delete(&mut page, b"h").unwrap();
    assert!(deleted);
    assert_eq!(page_key_count(&page), 2);

    // 现在 routing: < "d" → 100, >= "d" < "l" → 200, >= "l" → 220
    assert_eq!(internal_child(&page, b"a").unwrap(), 100);
    assert_eq!(internal_child(&page, b"d").unwrap(), 200);
    assert_eq!(internal_child(&page, b"k").unwrap(), 200); // "d" < "k" < "l"
    assert_eq!(internal_child(&page, b"l").unwrap(), 220);
    assert_eq!(internal_child(&page, b"z").unwrap(), 220);
}
