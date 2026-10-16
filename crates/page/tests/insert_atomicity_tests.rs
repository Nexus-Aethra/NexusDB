//! 回归测试: 失败的插入不得改变页字节 (2026-10-16, 审计新 P0-1).
//!
//! 背景: `internal_insert` 曾在 `pre_split_segment` 前无空间预检 —
//! 近满页上 mid item 的 full-key 重编码使 `copy_within` 越界 panic, 或使
//! free_off 写穿 checkpoint 区; 且 push_back 失败路径不回滚, 污染页会被
//! btree split 路径 (storage/src/btree.rs) 拆分后持久化 → 静默丢数据.
//! 与 2026-08-02 冷启动丢数据事故 (docs/bug-cold-start-data-loss.md) 同根因.
//!
//! 修复: plan/apply 两阶段 + 精确边界预检 (L1) + 失败快照回滚 (L2) +
//! pre_split_segment 自身硬化 (L3); 并修复 internal_split 对无哨兵页
//! 左半重建 cp[0] 时的 off-by-one (幽灵 item).

use page::{
    CHECKPOINT_AREA_END, ItemKind, PAGE_HEADER_SIZE, PageError, PageIndex, decode_item,
    internal_child, internal_insert, internal_new, internal_split, leaf_get, leaf_insert, leaf_new,
    leaf_update, page_free_off, page_key_count, page_set_vpid, pre_split_segment,
};

/// 从 PAGE_HEADER_SIZE 顺序走 item 链, 返回实际 item 数; 结构损坏返回 None.
fn count_items(page: &[u8]) -> Option<usize> {
    let free = page_free_off(page) as usize;
    let mut off = PAGE_HEADER_SIZE;
    let mut n = 0usize;
    while off < free {
        let (_, len) = decode_item(page, off, ItemKind::Internal).ok()?;
        if len == 0 {
            return None;
        }
        off += len;
        n += 1;
    }
    if off != free {
        return None;
    }
    Some(n)
}

/// 页自洽性: item 链恰好结束于 free_off; 段表 item_count 总和 == 实际 item 数.
fn assert_page_consistent(page: &[u8], ctx: &str) {
    let items = count_items(page).unwrap_or_else(|| panic!("{ctx}: item 链损坏"));
    let idx = PageIndex::load(page, ItemKind::Internal)
        .unwrap_or_else(|e| panic!("{ctx}: PageIndex::load 失败: {e:?}"));
    let sum: usize = idx.segments.iter().map(|s| s.item_count as usize).sum();
    assert_eq!(sum, items, "{ctx}: 段表总数 {sum} != 实际 item 数 {items}");
    let free = page_free_off(page) as usize;
    assert!(free <= CHECKPOINT_AREA_END, "{ctx}: free_off {free} 越界");
    for (i, s) in idx.segments.iter().enumerate() {
        assert!(
            (s.first_item_off as usize) < free,
            "{ctx}: seg[{i}].first_item_off={} >= free_off={free}",
            s.first_item_off
        );
    }
}

// ---------------------------------------------------------------------------
// 1. internal_insert: 失败的插入必须保持页字节不变 (多前缀长度, 覆盖
//    panic / 静默污染 / 干净拒绝三种历史表现)
// ---------------------------------------------------------------------------

#[test]
fn failed_internal_insert_leaves_page_unchanged() {
    for prefix_len in [0usize, 50, 100, 150, 200, 300, 400, 500, 600, 700, 750] {
        let mut page = internal_new();
        page_set_vpid(&mut page, 1);
        let prefix = "a".repeat(prefix_len);
        let mut n = 0usize;
        loop {
            let key = format!("{prefix}{n:08}");
            let before = page; // [u8; PAGE_SIZE] is Copy
            match internal_insert(&mut page, key.as_bytes(), (n + 2) as u64) {
                Ok(()) => {
                    n += 1;
                    assert_page_consistent(&page, &format!("prefix={prefix_len} insert#{n}"));
                    assert!(n < 100_000, "prefix={prefix_len}: insert 不收敛");
                }
                Err(e) => {
                    assert_eq!(
                        page, before,
                        "prefix={prefix_len}: 第 {n} 个 key 失败 ({e:?}) 但页字节被改写"
                    );
                    assert_page_consistent(&page, &format!("prefix={prefix_len} after-fail"));
                    break;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 2. 失败后路由不变: 修复前 600B 前缀场景下已插入 key 的路由从
//    Some(child) 静默变为 None (数据不可达)
// ---------------------------------------------------------------------------

#[test]
fn routing_survives_failed_internal_insert() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 1);
    let prefix = "a".repeat(600);
    let mut n = 0usize;
    loop {
        let key = format!("{prefix}{n:08}");
        match internal_insert(&mut page, key.as_bytes(), (n + 2) as u64) {
            Ok(()) => n += 1,
            Err(_) => break,
        }
        assert!(n < 100_000);
    }
    assert!(n > 100, "应已填入相当数量的 key (实际 {n})");
    // 全部已插入 key 必须仍路由到插入时指定的 child
    for i in 0..n {
        let k = format!("{prefix}{i:08}");
        assert_eq!(
            internal_child(&page, k.as_bytes()).unwrap(),
            (i + 2) as u64,
            "key #{i} 路由丢失/错误"
        );
    }
}

// ---------------------------------------------------------------------------
// 3. L3: pre_split_segment 自身硬化 — 近满页上直接调用必须拒绝且不改字节
// ---------------------------------------------------------------------------

#[test]
fn pre_split_segment_refuses_to_overflow_page() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 1);
    let prefix = "a".repeat(600);
    let mut n = 0usize;
    loop {
        let key = format!("{prefix}{n:08}");
        match internal_insert(&mut page, key.as_bytes(), (n + 2) as u64) {
            Ok(()) => n += 1,
            Err(_) => break,
        }
        assert!(n < 100_000);
    }
    // 填满后必然还有 >=32 item 的段 (最后一次失败插入正是触发 pre_split 的段)
    let mut idx = PageIndex::load(&page, ItemKind::Internal).unwrap();
    let seg_idx = idx
        .segments
        .iter()
        .position(|s| s.item_count >= 32)
        .expect("填满的页应存在 >=32 item 的段");
    let before = page; // [u8; PAGE_SIZE] is Copy
    let r = pre_split_segment(&mut page, &mut idx, seg_idx, ItemKind::Internal);
    assert!(r.is_err(), "近满页上 pre_split 必须报错");
    assert_eq!(page, before, "pre_split 失败也不得改变页字节");
    assert_page_consistent(&page, "pre_split 拒绝后");
}

// ---------------------------------------------------------------------------
// 4. internal_split: 无哨兵页 (上次 split 的 right 页) 再分裂时, 左半
//    cp[0].item_count 不得多数幽灵 item (修复前 199 vs 实际 198)
// ---------------------------------------------------------------------------

#[test]
fn internal_split_no_sentinel_left_has_no_phantom_item() {
    let prefix = "a".repeat(600);

    // 第一层: 有哨兵的 root 页
    let mut page = internal_new();
    page_set_vpid(&mut page, 1);
    let mut n = 0usize;
    loop {
        let key = format!("{prefix}{n:08}");
        match internal_insert(&mut page, key.as_bytes(), (n + 2) as u64) {
            Ok(()) => n += 1,
            Err(_) => break,
        }
        assert!(n < 100_000);
    }
    let mut right = internal_new();
    internal_split(&mut page, &mut right).unwrap();
    assert_page_consistent(&page, "left(有哨兵)");
    assert_page_consistent(&right, "right(无哨兵)");

    // 第二层: 无哨兵的 right 页打满后再分裂 → 左半同样不得有幽灵 item
    let mut m = 0usize;
    loop {
        let key = format!("{prefix}b{m:08}");
        match internal_insert(&mut right, key.as_bytes(), (m + 100) as u64) {
            Ok(()) => m += 1,
            Err(_) => break,
        }
        assert!(m < 100_000);
    }
    let mut right2 = internal_new();
    internal_split(&mut right, &mut right2).unwrap();
    assert_page_consistent(&right, "left2(无哨兵)");
    assert_page_consistent(&right2, "right2(无哨兵)");
    assert_eq!(
        page_key_count(&right) as usize,
        count_items(&right).unwrap()
    );
}

// ---------------------------------------------------------------------------
// 5. leaf_insert 同型回归: est 预检是启发式, 快照回滚兜底
// ---------------------------------------------------------------------------

#[test]
fn failed_leaf_insert_leaves_page_unchanged() {
    for prefix_len in [0usize, 100, 300, 600] {
        let mut page = leaf_new();
        let prefix = "a".repeat(prefix_len);
        let value = vec![b'v'; 64];
        let mut n = 0usize;
        loop {
            let key = format!("{prefix}{n:08}");
            let before = page; // [u8; PAGE_SIZE] is Copy
            match leaf_insert(&mut page, key.as_bytes(), &value) {
                Ok(()) => n += 1,
                Err(e) => {
                    assert_eq!(
                        page, before,
                        "leaf prefix={prefix_len}: 第 {n} 个 key 失败 ({e:?}) 但页字节被改写"
                    );
                    break;
                }
            }
            assert!(n < 100_000, "leaf prefix={prefix_len}: insert 不收敛");
        }
        // 已插入的 key 全部可读
        for i in 0..n {
            let k = format!("{prefix}{i:08}");
            assert_eq!(
                leaf_get(&page, k.as_bytes()),
                Some(value.clone()),
                "leaf prefix={prefix_len}: key #{i} 读回失败"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 6. leaf_update 慢路径: 空间不足必须干净拒绝且不碰页 (审计新 P0-2).
//    修复前: 近满页上 value 增大导致 copy_within 目的端越界 — 越过 PAGE_SIZE
//    panic ("dest is out of bounds") 或静默覆盖 checkpoint 区. 探针实证:
//    152 key 填满页后 value 100B→4000B (合法输入) 即复现.
// ---------------------------------------------------------------------------

#[test]
fn leaf_update_grow_on_full_page_rejects_cleanly() {
    let mut page = leaf_new();
    let v100 = vec![b'v'; 100];
    let mut n = 0usize;
    loop {
        let key = format!("k{n:08}");
        match leaf_insert(&mut page, key.as_bytes(), &v100) {
            Ok(()) => n += 1,
            Err(_) => break,
        }
        assert!(n < 100_000);
    }
    assert!(n > 50, "应已填得相当满 (实际 {n})");

    // 合法输入: value 100B → 4000B (key+value 远在 KvLimits 上限内)
    let key = format!("k{:08}", 10);
    let before = page; // [u8; PAGE_SIZE] is Copy
    let r = leaf_update(&mut page, key.as_bytes(), &vec![b'V'; 4000]);
    assert!(
        matches!(r, Err(PageError::PageFull)),
        "近满页增长 value 应返回 PageFull, 实际 {r:?}"
    );
    assert_eq!(page, before, "失败的 update 不得改变页字节");
    // 全部 key 仍可读且值仍是旧的
    for i in 0..n {
        let k = format!("k{i:08}");
        assert_eq!(
            leaf_get(&page, k.as_bytes()),
            Some(v100.clone()),
            "key #{i} 读回失败"
        );
    }
    assert_page_consistent_leaf(&page, "leaf_update 拒绝后");
}

/// leaf 页自洽性 (free_off 不越界 + PageIndex 可加载).
fn assert_page_consistent_leaf(page: &[u8], ctx: &str) {
    let free = page_free_off(page) as usize;
    assert!(free <= CHECKPOINT_AREA_END, "{ctx}: free_off {free} 越界");
    assert!(
        PageIndex::load(page, ItemKind::Leaf).is_ok(),
        "{ctx}: PageIndex::load 失败"
    );
}

#[test]
fn leaf_update_grow_within_capacity_succeeds() {
    // 防过度拒绝: 放得下的增长仍必须成功
    let mut page = leaf_new();
    let v100 = vec![b'v'; 100];
    for i in 0..50 {
        leaf_insert(&mut page, format!("k{i:08}").as_bytes(), &v100).unwrap();
    }
    let v2000 = vec![b'V'; 2000];
    assert!(leaf_update(&mut page, b"k00000010", &v2000).unwrap());
    assert_eq!(leaf_get(&page, b"k00000010"), Some(v2000));
    // 相邻 key 不受影响
    assert_eq!(leaf_get(&page, b"k00000011"), Some(v100.clone()));
    assert_eq!(leaf_get(&page, b"k00000009"), Some(v100));
}
