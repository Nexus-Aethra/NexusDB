//! Internal Page 集成测试.

use page::{
    PageType, internal_child, internal_delete, internal_insert, internal_new, internal_split,
    page_key_count, page_set_vpid, page_type, read_checkpoint_header,
};

#[test]
fn empty_internal_navigates_to_first_child() {
    // 空 internal 不合法, 但至少不应 panic
    let page = internal_new();
    assert_eq!(page_type(&page), PageType::Internal);
    assert_eq!(page_key_count(&page), 0);
}

#[test]
fn single_child_route() {
    let mut page = internal_new();
    // first child vpid = 100
    page_set_vpid(&mut page, 100);
    // separator "m" → child 200
    internal_insert(&mut page, b"m", 200).unwrap();
    assert_eq!(page_key_count(&page), 1);

    // key < "m" → first child = 100
    assert_eq!(internal_child(&page, b"a").unwrap(), 100);
    assert_eq!(internal_child(&page, b"k").unwrap(), 100);
    // key >= "m" → child = 200
    assert_eq!(internal_child(&page, b"m").unwrap(), 200);
    assert_eq!(internal_child(&page, b"z").unwrap(), 200);
}

#[test]
fn multiple_separators_route_correctly() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 10);
    internal_insert(&mut page, b"f", 20).unwrap();
    internal_insert(&mut page, b"m", 30).unwrap();
    internal_insert(&mut page, b"t", 40).unwrap();
    assert_eq!(page_key_count(&page), 3);

    // < "f" → 10
    assert_eq!(internal_child(&page, b"a").unwrap(), 10);
    // < "m" → 20
    assert_eq!(internal_child(&page, b"g").unwrap(), 20);
    // < "t" → 30
    assert_eq!(internal_child(&page, b"n").unwrap(), 30);
    // >= "t" → 40
    assert_eq!(internal_child(&page, b"u").unwrap(), 40);
}

#[test]
fn delete_separator() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 10);
    internal_insert(&mut page, b"f", 20).unwrap();
    internal_insert(&mut page, b"m", 30).unwrap();

    let deleted = internal_delete(&mut page, b"f").unwrap();
    assert!(deleted);
    assert_eq!(page_key_count(&page), 1);

    // 现在只有 "m" 分隔
    assert_eq!(internal_child(&page, b"a").unwrap(), 10);
    assert_eq!(internal_child(&page, b"x").unwrap(), 30);
}

#[test]
fn split_internal_page() {
    let mut left = internal_new();
    page_set_vpid(&mut left, 100);
    let seps: Vec<&[u8]> = vec![b"d", b"h", b"l", b"p", b"t"];
    for (i, s) in seps.iter().enumerate() {
        internal_insert(&mut left, s, (200 + i * 10) as u64).unwrap();
    }
    assert_eq!(page_key_count(&left), 5);

    let mut right = internal_new();
    let split_key = internal_split(&mut left, &mut right).unwrap();

    // mid = 2, left 保留 0..2 ("d", "h"), right 拿 2..5 ("l", "p", "t")
    assert_eq!(page_key_count(&left), 2);
    assert_eq!(page_key_count(&right), 3);
    assert_eq!(split_key, b"l");
}

#[test]
fn prefix_compression_on_long_separators() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 1);
    internal_insert(&mut page, b"user_alice", 10).unwrap();
    internal_insert(&mut page, b"user_bob", 20).unwrap();
    internal_insert(&mut page, b"user_carol", 30).unwrap();
    assert_eq!(page_key_count(&page), 3);

    // 仍然正确路由
    assert_eq!(internal_child(&page, b"user_alice").unwrap(), 10);
    assert_eq!(internal_child(&page, b"user_bob").unwrap(), 20);
    assert_eq!(internal_child(&page, b"user_dave").unwrap(), 30);
}

#[test]
fn checkpoints_added_on_insert() {
    // 插入超过 MAX_PER_CHECKPOINT 个 separator 后, 应当自动补 checkpoint
    let mut page = internal_new();
    page_set_vpid(&mut page, 1);

    // 插入 50 个 (段数由对半拆分决定, 不精确等于 ceil(50/32))
    for i in 0..50 {
        let k = format!("k_{i:04}");
        internal_insert(&mut page, k.as_bytes(), 100 + i as u64).unwrap();
    }
    assert_eq!(page_key_count(&page), 50);

    let (hdr, _) = read_checkpoint_header(&page);
    // 至少 1 段, 且不超过 50 / MIN_PER_CHECKPOINT
    assert!(hdr.checkpoint_count >= 1);
    assert!(hdr.checkpoint_count <= 50 / 8 + 2);

    // 路由仍正确 (验证 checkpoint 二分查找没破坏 routing)
    for i in 0..50 {
        let k = format!("k_{i:04}");
        assert_eq!(internal_child(&page, k.as_bytes()).unwrap(), 100 + i as u64);
    }
}

#[test]
fn routing_uses_checkpoints_when_many_separators() {
    // 100 个 separators (段数由对半拆分决定, 不精确等于 4)
    let mut page = internal_new();
    page_set_vpid(&mut page, 0);
    for i in 0..100 {
        let k = format!("key_{i:04}");
        internal_insert(&mut page, k.as_bytes(), 1000 + i as u64).unwrap();
    }

    // 边界值正确路由
    assert_eq!(internal_child(&page, b"key_0000").unwrap(), 1000);
    assert_eq!(internal_child(&page, b"key_0031").unwrap(), 1000 + 31);
    assert_eq!(internal_child(&page, b"key_0032").unwrap(), 1000 + 32); // 跨第一个 cp
    assert_eq!(internal_child(&page, b"key_0099").unwrap(), 1000 + 99);

    // 跨多个 cp
    assert_eq!(internal_child(&page, b"key_0063").unwrap(), 1000 + 63); // 第二个 cp 内
    assert_eq!(internal_child(&page, b"key_0095").unwrap(), 1000 + 95); // 第三个 cp 内
}
