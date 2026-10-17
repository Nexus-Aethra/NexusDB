//! ⭐ 回归: internal_update 不得破坏段首 shared=0 不变量 (审计 P2,
//! 对齐 leaf.rs:932-936 的 2026-07-26 修复 — "修复不对称"遗留).

use page::{
    ItemKind, PageIndex, decode_item, internal_insert, internal_new, internal_update,
    page_free_off, page_set_vpid,
};

#[test]
fn internal_update_segment_head_keeps_shared_zero() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 1);
    // 插入 40 个 separator → pre_split 产生多段 (段 1 的段首是真实 key)
    for i in 0..40u32 {
        internal_insert(&mut page, format!("k{i:04}").as_bytes(), (i + 2) as u64).unwrap();
    }
    let idx = PageIndex::load(&page, ItemKind::Internal).unwrap();
    assert!(idx.segments.len() >= 2, "应已产生多段");
    let head_off = idx.segments[1].first_item_off;
    let head_key = idx.segments[1].first_full_key.clone();

    // 更新段首 key 的 child_vpid
    assert!(internal_update(&mut page, &head_key, 9999).unwrap());

    // 段首 item 必须仍是 shared=0 (修复前被重编码成 shared=key.len())
    let (item, _) = decode_item(&page, head_off as usize, ItemKind::Internal).unwrap();
    assert_eq!(
        item.shared_prefix_len, 0,
        "段首 shared=0 不变量被 internal_update 破坏"
    );
    assert_eq!(item.child_vpid, 9999, "child_vpid 应已更新");

    // 页仍自洽: 可加载且路由正确
    let idx2 = PageIndex::load(&page, ItemKind::Internal).unwrap();
    assert!(idx2.segments.len() >= 2);
    assert!(page_free_off(&page) as usize <= page::CHECKPOINT_AREA_END);
}
