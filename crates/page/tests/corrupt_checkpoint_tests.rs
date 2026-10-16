//! ⭐ 回归: 损坏的 checkpoint 头不得导致 panic (审计 P2).
//!
//! `checkpoint_count >= 4093` 的损坏页会使 `read_checkpoint` 的
//! `hdr_off-(i+1)*4` usize 下溢 — debug/release 均 panic. 现在
//! `PageIndex::load` 先校验容量上界并返回 Err.

use page::{ItemKind, PAGE_SIZE, PageError, PageIndex, leaf_insert, leaf_new};

#[test]
fn corrupt_checkpoint_count_rejected_not_panicked() {
    // 构造一个 leaf 页, 写入若干 key, 然后把 cp header 的 count 改成超大值
    let mut page = leaf_new();
    for i in 0..10u32 {
        leaf_insert(&mut page, format!("k{i:04}").as_bytes(), b"v").unwrap();
    }
    // cp header 位于 CHECKPOINT_AREA_END-8 (16360); count 字段为前 2 字节
    let hdr_off = page::CHECKPOINT_AREA_END - 8;
    page[hdr_off..hdr_off + 2].copy_from_slice(&4093u16.to_le_bytes());
    // 修复前: usize 下溢 panic; 修复后: 显式 Err
    let r = PageIndex::load(&page, ItemKind::Leaf);
    assert!(
        matches!(r, Err(PageError::InvalidHeader)),
        "超大 checkpoint_count 必须返回 InvalidHeader, 实际 {r:?}"
    );
    // 更大的值 (u16::MAX) 同样不得 panic
    page[hdr_off..hdr_off + 2].copy_from_slice(&u16::MAX.to_le_bytes());
    let r2 = PageIndex::load(&page, ItemKind::Leaf);
    assert!(r2.is_err(), "u16::MAX 的 cp_count 必须返回 Err");
    assert_eq!(page.len(), PAGE_SIZE, "页长度不变");
}

#[test]
fn read_checkpoint_huge_index_no_panic() {
    // 直接对 read_checkpoint 传入超大 i: 不得 panic, 返回空 entry
    let page = [0u8; PAGE_SIZE];
    let cp = page::read_checkpoint(&page, usize::MAX / 8);
    assert_eq!(cp.item_count, 0);
    assert_eq!(cp.first_item_off, 0);
}
