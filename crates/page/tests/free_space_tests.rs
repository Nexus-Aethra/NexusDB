//! `page_free_space` 的 checkpoint 区域算法一致性回归.
//!
//! 背景: 该函数曾在本文件里另写一份 `key_count / 16` 的估算, 而写入方用的是
//! `needed_checkpoint_count` (= ceil(key_count / 32)) —— 两者在页接近满时相差近 2 倍。
//! 现在统一为: 页头 checkpoint_count (精确) 与 ceil 下界取大者。

use page::{
    CHECKPOINT_AREA_END, CHECKPOINT_HEADER_SIZE, CHECKPOINT_SIZE, CheckpointHeader, PAGE_SIZE,
    checkpoint_area_size, leaf_new, needed_checkpoint_count, page_free_off, page_free_space,
    page_key_count, page_set_key_count, read_checkpoint_header, write_checkpoint_header,
};

/// 构造一个 key_count 为 `k` 的页, 并把 checkpoint header 的段数写成 `segs`.
fn page_with(k: u16, segs: u16) -> [u8; PAGE_SIZE] {
    let mut page = leaf_new();
    page_set_key_count(&mut page, k);
    let (hdr, _) = read_checkpoint_header(&page);
    write_checkpoint_header(
        &mut page,
        CheckpointHeader {
            checkpoint_count: segs,
            ..hdr
        },
    );
    page
}

#[test]
fn empty_page_reserves_only_checkpoint_header() {
    let page = leaf_new();
    let free = page_free_space(&page);
    let expect = CHECKPOINT_AREA_END - checkpoint_area_size(0) - page_free_off(&page) as usize;
    assert_eq!(free, expect, "空页不应凭空多算 checkpoint 数组");
    assert!(free > 0);
}

#[test]
fn matches_authoritative_checkpoint_area_when_header_current() {
    // 覆盖跨越 32 边界的 key_count —— 旧实现在这些点偏得最狠.
    for k in [1u16, 15, 16, 31, 32, 33, 47, 48, 63, 64, 100, 200] {
        let segs = needed_checkpoint_count(k as usize) as u16;
        let page = page_with(k, segs);
        let expect = CHECKPOINT_AREA_END
            - CHECKPOINT_HEADER_SIZE
            - segs as usize * CHECKPOINT_SIZE
            - page_free_off(&page) as usize;
        assert_eq!(
            page_free_space(&page),
            expect,
            "key_count={k} 时与 checkpoint_area_size 不一致"
        );
    }
}

#[test]
fn never_overestimates_when_header_lags() {
    // 页头段数落后于实际 (写入路径尚未更新 header): 必须用 ceil 下界兜底,
    // 否则会高估可用空间 → 后续写入越过 checkpoint 区域.
    for k in [32u16, 33, 64, 96, 200] {
        let stale = page_with(k, 0);
        let floor = needed_checkpoint_count(k as usize);
        let free = page_free_space(&stale);
        let conservative =
            CHECKPOINT_AREA_END - checkpoint_area_size(floor) - page_free_off(&stale) as usize;
        assert_eq!(
            free, conservative,
            "key_count={k}: 页头滞后时应退化为 ceil 下界"
        );
    }
}

#[test]
fn monotone_non_increasing_in_key_count() {
    // 页越满, 段数只会不减 → 可用空间不应增加.
    let mut prev = usize::MAX;
    for k in (0u16..=200).step_by(4) {
        let page = page_with(k, needed_checkpoint_count(k as usize) as u16);
        let free = page_free_space(&page);
        assert!(free <= prev, "key_count={k} 时可用空间反而变大了");
        prev = free;
        let _ = page_key_count(&page);
    }
}

/// ⭐ 审计 P3 (2026-10-17): encode_leaf_item / encode_internal_item 的 buf
/// 边界保护 — 旧实现只校验 value 段, header/key/varint 直接下标写, buf 给小了
/// 就是 slice panic (crate 级无保护). 现在统一返回 `PageFull`.
#[test]
fn encode_item_rejects_small_buffer_instead_of_panicking() {
    // leaf: key 段就超 buf (旧实现在 `buf[4..4+key.len()]` 处直接 panic)
    let mut tiny = [0u8; 6];
    assert!(
        page::encode_leaf_item(&mut tiny, b"", b"abcdefgh", b"").is_err(),
        "key 超 buf 必须返回 Err 而不是 panic"
    );
    // value 段超 buf
    let mut tiny = [0u8; 8];
    assert!(
        page::encode_leaf_item(&mut tiny, b"", b"abc", b"value").is_err(),
        "value 超 buf 必须返回 Err"
    );
    let mut ok = [0u8; 16];
    assert!(page::encode_leaf_item(&mut ok, b"", b"abc", b"value").is_ok());

    // internal: key 段超 buf (旧实现同样 panic)
    let mut tiny = [0u8; 6];
    assert!(
        page::encode_internal_item(&mut tiny, b"", b"abcdefgh", 42).is_err(),
        "internal key 超 buf 必须返回 Err 而不是 panic"
    );
    let mut ok = [0u8; 16];
    assert!(page::encode_internal_item(&mut ok, b"", b"abc", 42).is_ok());
}
