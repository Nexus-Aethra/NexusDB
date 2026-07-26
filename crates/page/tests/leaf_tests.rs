//! Leaf Page 集成测试.

use page::{
    Checkpoint, CheckpointHeader, ItemKind, PAGE_HEADER_SIZE, PAGE_SIZE, PageIndex, PageType,
    leaf_delete, leaf_get, leaf_insert, leaf_new, leaf_push_back, leaf_split, page_free_off,
    page_free_space, page_key_count, page_set_free_off, page_set_key_count, page_type,
    write_checkpoint, write_checkpoint_header,
};
use page::{LeafItemPtr, decode_item, encode_leaf_item};

#[test]
fn empty_page_has_no_keys() {
    let page = leaf_new();
    assert_eq!(page_key_count(&page), 0);
    assert_eq!(page_type(&page), PageType::Leaf);
    assert!(leaf_get(&page, b"any").is_none());
    assert!(page_free_space(&page) > 0);
}

#[test]
fn insert_and_get_single_key() {
    let mut page = leaf_new();
    leaf_insert(&mut page, b"hello", b"world").unwrap();
    assert_eq!(page_key_count(&page), 1);

    let v = leaf_get(&page, b"hello").expect("key should exist");
    assert_eq!(v, b"world");
}

#[test]
fn get_nonexistent_returns_none() {
    let mut page = leaf_new();
    leaf_insert(&mut page, b"alpha", b"a").unwrap();
    leaf_insert(&mut page, b"bravo", b"b").unwrap();
    leaf_insert(&mut page, b"charlie", b"c").unwrap();
    assert!(leaf_get(&page, b"delta").is_none());
}

#[test]
fn many_keys_insert_get() {
    let mut page = leaf_new();
    let n = 100;
    for i in 0..n {
        let key = format!("key_{i:04}");
        let val = format!("value_{i}");
        leaf_insert(&mut page, key.as_bytes(), val.as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), n);

    // 全部能查到
    for i in 0..n {
        let key = format!("key_{i:04}");
        let expected = format!("value_{i}");
        let got = leaf_get(&page, key.as_bytes()).unwrap();
        assert_eq!(got, expected.as_bytes());
    }
}

#[test]
fn delete_existing_key() {
    let mut page = leaf_new();
    leaf_insert(&mut page, b"a", b"1").unwrap();
    leaf_insert(&mut page, b"b", b"2").unwrap();
    leaf_insert(&mut page, b"c", b"3").unwrap();

    let deleted = leaf_delete(&mut page, b"b").unwrap();
    assert!(deleted);
    assert_eq!(page_key_count(&page), 2);
    assert!(leaf_get(&page, b"b").is_none());
    assert_eq!(leaf_get(&page, b"a").unwrap(), b"1");
    assert_eq!(leaf_get(&page, b"c").unwrap(), b"3");
}

#[test]
fn delete_nonexistent_returns_false() {
    let mut page = leaf_new();
    leaf_insert(&mut page, b"x", b"1").unwrap();
    let deleted = leaf_delete(&mut page, b"y").unwrap();
    assert!(!deleted);
    assert_eq!(page_key_count(&page), 1);
}

#[test]
fn prefix_compression_works() {
    let mut page = leaf_new();
    // 长公共前缀 keys
    let keys: Vec<Vec<u8>> = vec![
        b"user_profile_alice".to_vec(),
        b"user_profile_bob".to_vec(),
        b"user_profile_carol".to_vec(),
        b"user_profile_dave".to_vec(),
    ];
    for (i, k) in keys.iter().enumerate() {
        leaf_insert(&mut page, k, format!("data{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 4);

    // 全部能查到
    for (i, k) in keys.iter().enumerate() {
        let v = leaf_get(&page, k).unwrap();
        assert_eq!(v, format!("data{i}").as_bytes());
    }
}

#[test]
fn split_divides_items_between_two_pages() {
    let mut left = leaf_new();
    for i in 0..20 {
        let key = format!("k_{i:04}");
        leaf_insert(&mut left, key.as_bytes(), b"v").unwrap();
    }
    assert_eq!(page_key_count(&left), 20);

    let mut right = leaf_new();
    let split_key = leaf_split(&mut left, &mut right).unwrap();

    // 核心不变量: left + right = 20 keys, 且两侧都非空
    let left_count = page_key_count(&left) as usize;
    let right_count = page_key_count(&right) as usize;
    assert_eq!(left_count + right_count, 20);
    assert!(left_count > 0 && right_count > 0);

    // split_key 正确划分: left 的所有 key < split_key, right 的所有 key >= split_key
    // 验证 split_key 在 right 中能找到, 在 left 中找不到
    assert!(leaf_get(&left, &split_key).is_none());
    assert!(leaf_get(&right, &split_key).is_some());

    // 验证所有原始 key 仍然可以在某一侧找到
    for i in 0..20 {
        let key = format!("k_{i:04}");
        let in_left = leaf_get(&left, key.as_bytes()).is_some();
        let in_right = leaf_get(&right, key.as_bytes()).is_some();
        assert!(in_left || in_right, "key {} not found in either page", key);
        assert!(!(in_left && in_right), "key {} found in both pages", key);
    }
}

#[test]
fn page_full_returns_error() {
    let mut page = leaf_new();
    let mut inserted = 0;
    // 试图插到满
    for i in 0..1000 {
        let key = format!("{:010}", i);
        match leaf_insert(
            &mut page,
            key.as_bytes(),
            b"very_long_value_payload_xxxxxxxxxxxxxxxxx",
        ) {
            Ok(()) => inserted += 1,
            Err(page::PageError::PageFull) => break,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
    assert!(inserted > 0, "should insert at least one");
    println!("inserted {inserted} items before page full");
}

// ===== leaf_push_back 测试 =====

/// 工具: 创建一个含哨兵的 page, 写入一批 items (带正确前缀压缩).
/// 返回 (page, sentinel_n).
fn setup_page_with_items(items: &[(&[u8], &[u8])]) -> ([u8; PAGE_SIZE], usize) {
    let mut page = leaf_new();
    let mut off = PAGE_HEADER_SIZE;

    // 哨兵
    let mut buf = [0u8; 4096];
    let sentinel_n = encode_leaf_item(&mut buf, &[], b"", b"").unwrap();
    page[off..off + sentinel_n].copy_from_slice(&buf[..sentinel_n]);
    off += sentinel_n;
    let mut prev_key = b"".to_vec();

    // 真实 items (带前缀压缩: 用 prev_key 编码)
    for (k, v) in items {
        let n = encode_leaf_item(&mut buf, &prev_key, k, v).unwrap();
        page[off..off + n].copy_from_slice(&buf[..n]);
        off += n;
        prev_key = k.to_vec();
    }

    page_set_key_count(&mut page, items.len() as u16);
    page_set_free_off(&mut page, off as u16);

    // cp[0] 指向哨兵 (含哨兵 + 所有 items 作为一个段)
    let hdr = CheckpointHeader {
        checkpoint_count: 1,
        ..Default::default()
    };
    write_checkpoint_header(&mut page, hdr);
    write_checkpoint(
        &mut page,
        0,
        Checkpoint {
            // cp[0].item_count 包含哨兵 + 真实 items
            item_count: (1 + items.len()) as u16,
            first_item_off: PAGE_HEADER_SIZE as u16,
        },
    );

    (page, sentinel_n)
}

/// 验证 page 的 item 编码正确性: 按序 decode, 检查 shared_prefix_len 与 key.
fn verify_page_integrity(page: &[u8]) {
    let free_off = page_free_off(page) as usize;
    let mut off = PAGE_HEADER_SIZE;
    let mut prev_key = Vec::new();
    let mut i = 0;

    // 循环到 free_off (可能含哨兵)
    while off < free_off {
        let (item, n) = decode_item(page, off, ItemKind::Leaf).unwrap();
        let full = item.full_key(&prev_key);

        let actual_shared = std::iter::zip(&prev_key, &full)
            .take_while(|(a, b)| a == b)
            .count();
        assert_eq!(
            item.shared_prefix_len as usize,
            actual_shared,
            "item {i}: key={:?}, expected shared={} got {}",
            String::from_utf8_lossy(&full),
            actual_shared,
            item.shared_prefix_len
        );

        off += n;
        prev_key = full;
        i += 1;
    }

    assert_eq!(off, free_off, "items consume exactly up to free_off");
}

/// 工具: 从 cp 段首构造 LeafItemPtr
fn ptr_from_cp<'a>(page: &'a [u8], idx: &PageIndex, seg_idx: usize) -> LeafItemPtr<'a> {
    LeafItemPtr::new(page, idx.segments[seg_idx].first_item_off as usize).unwrap()
}

/// 工具: 在 page 中顺序遍历找到 key 对应的 LeafItemPtr
fn find_ptr_by_key<'a>(
    page: &'a [u8],
    idx: &PageIndex,
    target_key: &[u8],
) -> Option<LeafItemPtr<'a>> {
    let seg_idx = idx.locate_segment(target_key);
    let mut ptr = ptr_from_cp(page, idx, seg_idx);
    loop {
        if ptr.key() == target_key {
            return Some(ptr);
        }
        ptr = ptr.next().ok()??;
    }
}

#[test]
fn test_push_back_after_sentinel() {
    let (mut page, _) = setup_page_with_items(&[(b"bbb", b"v2"), (b"ccc", b"v3")]);
    let mut idx = PageIndex::load(&page[..], ItemKind::Leaf).unwrap();
    assert_eq!(idx.key_count, 2); // 真实 keys (哨兵不计入 key_count)

    // 提取哨兵 ptr 信息 (owned), 然后释放 borrow
    let (prev_key, insert_off, seg_idx) = {
        let ptr = ptr_from_cp(&page[..], &idx, 0);
        assert_eq!(ptr.key(), b"");
        let off = ptr.byte_offset();
        let s = idx.find_segment_by_offset(off);
        (ptr.key().to_vec(), off + ptr.total_len(), s)
    };

    eprintln!(
        "BEFORE push_back: free_off={} key_count={}",
        page_free_off(&page),
        page_key_count(&page)
    );

    // 在哨兵后插入 "aaa"
    leaf_push_back(
        &mut page, &mut idx, &prev_key, insert_off, b"aaa", b"v1", seg_idx,
    )
    .unwrap();
    assert_eq!(idx.key_count, 3);

    eprintln!(
        "AFTER push_back: free_off={} key_count={}",
        page_free_off(&page),
        page_key_count(&page)
    );

    // 写回 page
    idx.write_back(&mut page).unwrap();

    eprintln!(
        "AFTER write_back: free_off={} key_count={}",
        page_free_off(&page),
        page_key_count(&page)
    );

    // 验证: 全部能查到
    verify_page_integrity(&page);
    let reloaded = PageIndex::load(&page[..], ItemKind::Leaf).unwrap();
    assert_eq!(reloaded.key_count, 3);

    // "aaa" 应该在哨兵之后, "bbb" 之前
    let p = find_ptr_by_key(&page, &reloaded, b"aaa").unwrap();
    assert_eq!(p.value(), b"v1");
}

#[test]
fn test_push_back_middle() {
    let (mut page, _) = setup_page_with_items(&[
        (b"aaa", b"v1"),
        (b"bbb", b"v2"),
        (b"ddd", b"v4"),
        (b"eee", b"v5"),
    ]);
    let mut idx = PageIndex::load(&page[..], ItemKind::Leaf).unwrap();
    assert_eq!(idx.key_count, 4);

    // 提取 "bbb" ptr 信息
    let (prev_key, insert_off, seg_idx) = {
        let ptr = find_ptr_by_key(&page, &idx, b"bbb").unwrap();
        assert_eq!(ptr.key(), b"bbb");
        let off = ptr.byte_offset();
        let s = idx.find_segment_by_offset(off);
        (ptr.key().to_vec(), off + ptr.total_len(), s)
    };

    leaf_push_back(
        &mut page, &mut idx, &prev_key, insert_off, b"ccc", b"v3", seg_idx,
    )
    .unwrap();
    assert_eq!(idx.key_count, 5);

    idx.write_back(&mut page).unwrap();
    verify_page_integrity(&page);

    let reloaded = PageIndex::load(&page[..], ItemKind::Leaf).unwrap();

    // 验证顺序: aaa, bbb, ccc, ddd, eee
    for k in &[b"aaa", b"bbb", b"ccc", b"ddd", b"eee"] {
        let p = find_ptr_by_key(&page, &reloaded, *k).unwrap();
        assert!(!p.value().is_empty(), "key {:?} should have value", k);
    }
}

#[test]
fn test_push_back_at_end() {
    let (mut page, _) = setup_page_with_items(&[(b"aaa", b"v1"), (b"bbb", b"v2")]);
    let mut idx = PageIndex::load(&page[..], ItemKind::Leaf).unwrap();

    // 提取 "bbb" ptr 信息 (最后一个 item, 在其后插入)
    let (prev_key, insert_off, seg_idx) = {
        let ptr = find_ptr_by_key(&page, &idx, b"bbb").unwrap();
        let off = ptr.byte_offset();
        let s = idx.find_segment_by_offset(off);
        (ptr.key().to_vec(), off + ptr.total_len(), s)
    };

    leaf_push_back(
        &mut page, &mut idx, &prev_key, insert_off, b"ccc", b"v3", seg_idx,
    )
    .unwrap();
    assert_eq!(idx.key_count, 3);

    idx.write_back(&mut page).unwrap();
    verify_page_integrity(&page);

    let p = find_ptr_by_key(
        &page,
        &PageIndex::load(&page[..], ItemKind::Leaf).unwrap(),
        b"ccc",
    )
    .unwrap();
    assert_eq!(p.value(), b"v3");
}

#[test]
fn test_push_back_preserves_existing_items() {
    let (mut page, _) =
        setup_page_with_items(&[(b"alpha", b"a"), (b"beta", b"b"), (b"delta", b"d")]);
    let mut idx = PageIndex::load(&page[..], ItemKind::Leaf).unwrap();

    // 提取 "beta" ptr 信息
    let (prev_key, insert_off, seg_idx) = {
        let ptr = find_ptr_by_key(&page, &idx, b"beta").unwrap();
        let off = ptr.byte_offset();
        let s = idx.find_segment_by_offset(off);
        (ptr.key().to_vec(), off + ptr.total_len(), s)
    };

    leaf_push_back(
        &mut page, &mut idx, &prev_key, insert_off, b"gamma", b"g", seg_idx,
    )
    .unwrap();
    idx.write_back(&mut page).unwrap();

    verify_page_integrity(&page);

    // 旧 values 还在
    let reloaded = PageIndex::load(&page[..], ItemKind::Leaf).unwrap();
    assert_eq!(
        find_ptr_by_key(&page, &reloaded, b"alpha").unwrap().value(),
        b"a"
    );
    assert_eq!(
        find_ptr_by_key(&page, &reloaded, b"beta").unwrap().value(),
        b"b"
    );
    assert_eq!(
        find_ptr_by_key(&page, &reloaded, b"delta").unwrap().value(),
        b"d"
    );
}

#[test]
fn test_push_back_with_prefix_compression_impact() {
    // 测试有公共前缀的 keys, push_back 后 k+1 的 shared_prefix_len 正确
    let (mut page, _) = setup_page_with_items(&[
        (b"user_alpha", b"a"),
        (b"user_beta", b"b"),
        (b"user_delta", b"d"),
    ]);
    let mut idx = PageIndex::load(&page[..], ItemKind::Leaf).unwrap();

    // 提取 "user_beta" ptr 信息
    let (prev_key, insert_off, seg_idx) = {
        let ptr = find_ptr_by_key(&page, &idx, b"user_beta").unwrap();
        let off = ptr.byte_offset();
        let s = idx.find_segment_by_offset(off);
        (ptr.key().to_vec(), off + ptr.total_len(), s)
    };

    leaf_push_back(
        &mut page,
        &mut idx,
        &prev_key,
        insert_off,
        b"user_gamma",
        b"g",
        seg_idx,
    )
    .unwrap();
    idx.write_back(&mut page).unwrap();

    verify_page_integrity(&page);
}

/// ⭐ 回归: leaf_update 更新「段首 item」时必须保持 shared=0 自包含不变量.
///
/// Bug (2026-07-26, memtier 发现): target 是段首时段内扫描第一个就命中,
/// prev_ptr 被初始化为 target 自身 → prev_key == key → 重编码 shared=len-1,
/// 破坏段首不变量, 后续 PageIndex::load 报
/// "segment head item must have shared=0, got shared=15".
/// memtier 的长公共前缀 key ("memtier-XXXXXXXX") + 覆盖写必现.
#[test]
fn update_segment_head_keeps_shared_zero() {
    use page::leaf_update;

    let mut page = leaf_new();
    // 长公共前缀 key (模拟 memtier-XXXXXXXX), 插入 60 个触发 pre_split 出多段
    for i in 0..60 {
        let key = format!("memtier-{i:08}");
        leaf_insert(&mut page, key.as_bytes(), b"v_initial_00000000000000000000000").unwrap();
    }
    let idx = PageIndex::load(&page, ItemKind::Leaf).unwrap();
    assert!(idx.segments.len() >= 2, "need multiple segments, got {}", idx.segments.len());

    // 逐个更新每个段的段首 key (不同长度 value 覆盖快/慢两条路径)
    let heads: Vec<Vec<u8>> = idx.segments[1..]
        .iter()
        .map(|s| s.first_full_key.clone())
        .collect();
    for (i, head) in heads.iter().enumerate() {
        // 慢路径 (长度变化)
        let new_val = format!("v_updated_{i}");
        assert!(leaf_update(&mut page, head, new_val.as_bytes()).unwrap());
        // 每次更新后 PageIndex 必须仍能加载 (段首 shared=0 不变量)
        PageIndex::load(&page, ItemKind::Leaf)
            .unwrap_or_else(|e| panic!("PageIndex::load after update {i}: {e}"));
        assert_eq!(
            leaf_get(&page, head).unwrap(),
            new_val.as_bytes(),
            "head {i} value mismatch"
        );
        // 快路径 (同长度覆盖)
        let same_len: Vec<u8> = new_val.bytes().rev().collect();
        assert!(leaf_update(&mut page, head, &same_len).unwrap());
        assert_eq!(leaf_get(&page, head).unwrap(), same_len.as_slice());
    }

    // 全量 key 仍可读 (无邻居损坏)
    for i in 0..60 {
        let key = format!("memtier-{i:08}");
        assert!(leaf_get(&page, key.as_bytes()).is_some(), "key {i} lost");
    }
}
