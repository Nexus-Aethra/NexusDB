//! Leaf Page 操作: get / insert / delete / split.
//!
//! ## API 概览
//!
//! | 函数 | 用途 |
//! |---|---|
//! | `leaf_new` | 创建空 leaf page |
//! | `leaf_get` | 读 key 对应 value |
//! | `leaf_insert` | 插入 (key, value) |
//! | `leaf_delete` | 删除 key |
//! | `leaf_split` | 满时分裂: 左半保留, 右半填新 page, 返回分隔 key |
//!
//! 所有函数均接受 `[u8; PAGE_SIZE]` 字节缓冲, 不持有任何状态.

use crate::dprintln;

use crate::checkpoint::{
    Checkpoint, CheckpointHeader, MAX_PER_CHECKPOINT, MIN_PER_CHECKPOINT, checkpoint_area_size,
    write_checkpoint, write_checkpoint_header,
};
use crate::error::PageError;
use crate::header::{
    PAGE_FOOTER_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE, PageType, page_check_magic, page_free_off,
    page_init_header, page_key_count, page_set_free_off, page_set_key_count, page_type,
};
use crate::index::{PageIndex, pre_split_segment};
use crate::item::{ItemKind, decode_item, encode_leaf_item};
use crate::ptr::LeafItemPtr;
use crate::varint::decode_varint;

/// 创建空 leaf page.
pub fn leaf_new() -> [u8; PAGE_SIZE] {
    let mut page = [0u8; PAGE_SIZE];
    page_init_header(&mut page, PageType::Leaf);
    write_checkpoint_header(&mut page, CheckpointHeader::default());
    page
}

/// 初始化哨兵 (空 page 第一次插入时调用).
///
/// 写入哨兵 item (shared=0, key_unshared_len=0) 到 PAGE_HEADER_SIZE 处,
/// 设置 cp[0] 指向哨兵. **key_count 不变** (哨兵不计入 key_count).
///
/// 这样:
/// - key_count 始终是真实 key 数 (向后兼容现有测试)
/// - cp[0] 段首是哨兵, 但 PageIndex 维护 item_count 含哨兵
fn init_sentinel(page: &mut [u8]) -> Result<(), PageError> {
    if page_free_off(page) as usize != PAGE_HEADER_SIZE {
        // page 已经有内容, 不重复初始化
        return Ok(());
    }
    let mut buf = [0u8; 4096];
    let n = encode_leaf_item(&mut buf, &[], b"", b"[]")?;
    let off = PAGE_HEADER_SIZE;
    page[off..off + n].copy_from_slice(&buf[..n]);
    page_set_free_off(page, (off + n) as u16);
    // key_count 不变 (哨兵不计入)
    let hdr = CheckpointHeader {
        checkpoint_count: 1,
        ..Default::default()
    };
    write_checkpoint_header(page, hdr);
    write_checkpoint(
        page,
        0,
        Checkpoint {
            item_count: 1, // 哨兵
            first_item_off: off as u16,
        },
    );
    Ok(())
}

/// 读取 leaf page 中 key 对应的 value.
pub fn leaf_get(page: &[u8], key: &[u8]) -> Option<Vec<u8>> {
    leaf_get_with(page, key, |v| v.to_vec())
}

/// ⭐ 借用回调版 leaf_get: 命中时以 `&[u8]` 借用回调, 零 value 拷贝.
///
/// 热路径用 (存在性判定 / 前缀窥视 / 直接编码进回复帧), 避免
/// `leaf_get` 的整值 `to_vec` 物化.
pub fn leaf_get_with<R>(page: &[u8], key: &[u8], f: impl FnOnce(&[u8]) -> R) -> Option<R> {
    if page_type(page) != PageType::Leaf {
        return None;
    }
    if page_check_magic(page).is_err() {
        return None;
    }
    // 禁止查空 key: 哨兵专用
    if key.is_empty() {
        return None;
    }
    if page_key_count(page) == 0 {
        return None;
    }
    // 用 PageIndex 段二分 + 段内 next 定位
    let idx = PageIndex::load(page, ItemKind::Leaf).ok()?;
    dprintln!(
        leaf,
        "[LEAF_GET] key={:?} page_key_count={} segments={}",
        std::str::from_utf8(key).unwrap_or("?"),
        page_key_count(page),
        idx.segments.len()
    );
    for (i, s) in idx.segments.iter().enumerate() {
        dprintln!(
            leaf,
            "[LEAF_GET]   cp[{}] first_off={} count={} first_key={:?}",
            i,
            s.first_item_off,
            s.item_count,
            String::from_utf8_lossy(&s.first_full_key)
        );
    }
    if idx.segments.is_empty() {
        return None;
    }
    let seg_idx = idx.locate_segment(key);
    dprintln!(
        leaf,
        "[LEAF_GET] seg_idx={} key={:?}",
        seg_idx,
        std::str::from_utf8(key).unwrap_or("?")
    );
    let seg = &idx.segments[seg_idx];
    let mut ptr = LeafItemPtr::new(page, seg.first_item_off as usize).ok()?;
    dprintln!(
        leaf,
        "[LEAF_GET] initial ptr off={} key={:?}",
        ptr.byte_offset(),
        std::str::from_utf8(ptr.key()).unwrap_or("?")
    );
    // cp[0] 段首是哨兵, 跳过
    if seg_idx == 0 && ptr.key().is_empty() {
        let next_ptr = ptr.next().ok()??;
        dprintln!(
            leaf,
            "[LEAF_GET] skip sentinel, next off={} key={:?}",
            next_ptr.byte_offset(),
            std::str::from_utf8(next_ptr.key()).unwrap_or("?")
        );
        ptr = next_ptr;
    }
    let mut loop_count = 0;
    loop {
        loop_count += 1;
        if loop_count > 1000 {
            dprintln!(leaf, "[LEAF_GET] LOOP LIMIT EXCEEDED");
            return None;
        }
        let cur_key = ptr.key();
        dprintln!(
            leaf,
            "[LEAF_GET] loop #{} off={} key={:?} (looking for {:?})",
            loop_count,
            ptr.byte_offset(),
            std::str::from_utf8(cur_key).unwrap_or("?"),
            std::str::from_utf8(key).unwrap_or("?")
        );
        if cur_key == key {
            return Some(f(ptr.value()));
        }
        if cur_key > key {
            dprintln!(leaf, "[LEAF_GET] cur_key > key, return None");
            return None;
        }
        match ptr.next().ok()? {
            Some(p) => ptr = p,
            None => {
                dprintln!(leaf, "[LEAF_GET] next is None, return None");
                return None;
            }
        }
    }
}

/// ⭐ Phase R: 从 `start` 开始顺序扫描 leaf 内 `key >= start` 的全部 item.
///
/// 每命中一项以 `(key, value)` 借用回调 (零拷贝); 回调返回
/// `ControlFlow::Break` 立即停止并上传 (供 limit / 前缀越界早停).
/// 返回 `Break` 表示扫描被中断, `Continue` 表示本 leaf 扫尽.
///
/// **按 segment 逐段迭代** (段间有序、段内 `item_count` 界定), 不依赖
/// `next()` 跨段, 与物理布局无关; 跳过哨兵 (seg0 空 key).
pub fn leaf_scan_from<F: FnMut(&[u8], &[u8]) -> core::ops::ControlFlow<()>>(
    page: &[u8],
    start: &[u8],
    f: &mut F,
) -> core::ops::ControlFlow<()> {
    use core::ops::ControlFlow;
    if page_type(page) != PageType::Leaf || page_check_magic(page).is_err() {
        return ControlFlow::Continue(());
    }
    if page_key_count(page) == 0 {
        return ControlFlow::Continue(());
    }
    let Ok(idx) = PageIndex::load(page, ItemKind::Leaf) else {
        return ControlFlow::Continue(());
    };
    if idx.segments.is_empty() {
        return ControlFlow::Continue(());
    }
    // start 非空 → 从其所在段开始 (更早的段全 < start); 空 → 从 seg0.
    let start_seg = if start.is_empty() {
        0
    } else {
        idx.locate_segment(start)
    };
    for seg_idx in start_seg..idx.segments.len() {
        let seg = &idx.segments[seg_idx];
        let Ok(mut ptr) = LeafItemPtr::new(page, seg.first_item_off as usize) else {
            return ControlFlow::Continue(());
        };
        for item_i in 0..seg.item_count {
            let cur_key = ptr.key();
            // seg0 item0 是哨兵 (空 key), 跳过
            let is_sentinel = seg_idx == 0 && item_i == 0 && cur_key.is_empty();
            if !is_sentinel
                && cur_key >= start
                && let ControlFlow::Break(()) = f(cur_key, ptr.value())
            {
                return ControlFlow::Break(());
            }
            // 非末项才推进
            if item_i + 1 < seg.item_count {
                match ptr.next() {
                    Ok(Some(p)) => ptr = p,
                    _ => break,
                }
            }
        }
    }
    ControlFlow::Continue(())
}

/// 在 leaf page 中插入 (key, value).
///
/// 返回 `Ok(())` 表示成功, `Err(PageError::PageFull)` 表示空间不足.
pub fn leaf_insert(page: &mut [u8], key: &[u8], value: &[u8]) -> Result<(), PageError> {
    if page_type(page) != PageType::Leaf {
        return Err(PageError::InvalidPageType {
            expected: PageType::Leaf,
            got: page_type(page),
        });
    }
    // 禁止空 key: 空 key 保留给哨兵 (item 0). 用户不应该插入.
    if key.is_empty() {
        return Err(PageError::ItemDecode(
            "empty key is reserved for sentinel".into(),
        ));
    }

    // 0. 空 page: 先初始化哨兵
    if page_key_count(page) == 0 {
        dprintln!(leaf, "[LEAF_INSERT] init_sentinel");
        init_sentinel(page)?;
    }

    let mut idx = PageIndex::load(page, ItemKind::Leaf)?;

    // 1. 段二分定位 key 应该插入的段
    let seg_idx = idx.locate_segment(key);
    let mut cur_seg_idx = seg_idx;
    dprintln!(
        leaf,
        "[LEAF_INSERT] key={:?} seg_idx={} key_count={} segments={}",
        std::str::from_utf8(key).unwrap_or("?"),
        seg_idx,
        page_key_count(page),
        idx.segments.len()
    );

    // 2. ⭐ 空间预检 + pre_split (2026-08-02 修复):
    //    先估算空间需求, 不足则直接 PageFull (页数据未修改, checkpoint 一致).
    //    充足才执行 pre_split_segment + 插入.
    let need_pre_split = idx.segments[cur_seg_idx].item_count >= MAX_PER_CHECKPOINT;
    {
        // 估算新 item 大小: key + value + 编码开销 (shared/varint/头)
        let est_item = key.len() + value.len() + 20;
        // pre_split 会多一个段 = 多一个 checkpoint entry (4B)
        let cp_growth = if need_pre_split { crate::checkpoint::CHECKPOINT_SIZE } else { 0 };
        let cp_size = crate::checkpoint::checkpoint_area_size(idx.segments.len()) + cp_growth;
        let free_off = page_free_off(page) as usize;
        if free_off + est_item + cp_size + PAGE_FOOTER_SIZE > PAGE_SIZE {
            return Err(PageError::PageFull);
        }
    }
    if need_pre_split {
        pre_split_segment(page, &mut idx, cur_seg_idx, ItemKind::Leaf)?;
        cur_seg_idx = idx.locate_segment(key);
    }

    // 3. 段内顺序扫描找到 prev_ptr (指向 <key 的最后一个 item)
    dprintln!(
        leaf,
        "[LEAF_INSERT] Starting segment scan at cur_seg_idx={}, first_off={}",
        cur_seg_idx,
        idx.segments[cur_seg_idx].first_item_off
    );
    let mut ptr = LeafItemPtr::new(page, idx.segments[cur_seg_idx].first_item_off as usize)?;
    let mut prev_ptr = ptr.clone();
    loop {
        if ptr.key() >= key {
            // ptr 是第一个 >= key 的 item. prev_ptr 是 < key 的最后一个 item.
            break;
        }
        // ptr.key() < key, 推进 prev_ptr 和 ptr
        prev_ptr = ptr.clone();
        let next = ptr
            .next()
            .map_err(|e| PageError::ItemDecode(format!("next: {e}")))?;
        match next {
            None => {
                // 到段末尾, prev_ptr 是段尾 item
                break;
            }
            Some(p) => ptr = p,
        }
    }

    // 4. 检查 key 是否已存在
    //    loop 退出时 ptr 是第一个 >= key 的 item. 若 ptr.key() == key, 已存在.
    if ptr.key() == key {
        dprintln!(leaf, "[LEAF_INSERT] key already exists, returning Err");
        // **重要**: pre_split 可能已经修改了 page, 需要 write_back 保存 pre_split 的变更.
        // 否则 page 的 cp array 与 page bytes 不一致.
        idx.write_back(page)?;
        return Err(PageError::ItemDecode(
            "key exists, overwrite not yet supported".into(),
        ));
    }

    // 5. 调用 leaf_push_back (用 prev_ptr 而非 ptr, 因为 ptr 可能是新 key 的位置)
    let prev_key = prev_ptr.key().to_vec();
    let insert_off = prev_ptr.byte_offset() + prev_ptr.total_len();
    // 定位 prev_ptr 所在的段 (不是 insert_off! 边界情况下 prev_ptr 在 seg[N] 末尾,
    // insert_off == seg[N+1].first_item_off, 新 item 物理上在 seg[N] 末尾).
    let seg_idx_for_push = idx.find_segment_by_offset(prev_ptr.byte_offset());
    // ptr 借用 page, 必须先 drop 再 mut borrow
    drop(ptr);
    drop(prev_ptr);
    leaf_push_back(
        page,
        &mut idx,
        &prev_key,
        insert_off,
        key,
        value,
        seg_idx_for_push,
    )?;

    // 6. 写回 PageIndex (cp array + header.key_count)
    dprintln!(
        leaf,
        "[LEAF_INSERT] before write_back: idx.segments.len={}, segments={:?}",
        idx.segments.len(),
        idx.segments
            .iter()
            .map(|s| (s.item_count, s.first_item_off))
            .collect::<Vec<_>>()
    );
    idx.write_back(page)?;

    Ok(())
}

/// 在 leaf page 中删除 key. 返回 true 表示 key 存在并已删除.
pub fn leaf_delete(page: &mut [u8], key: &[u8]) -> Result<bool, PageError> {
    dprintln!(
        leaf,
        "[LEAF_DELETE] called key={:?} key_count={} free_off={}",
        std::str::from_utf8(key).unwrap_or("?"),
        page_key_count(page),
        page_free_off(page)
    );
    if page_type(page) != PageType::Leaf {
        return Err(PageError::InvalidPageType {
            expected: PageType::Leaf,
            got: page_type(page),
        });
    }
    if key.is_empty() {
        return Err(PageError::ItemDecode(
            "empty key is reserved for sentinel".into(),
        ));
    }
    if page_key_count(page) == 0 {
        return Ok(false);
    }

    let mut idx = PageIndex::load(page, ItemKind::Leaf)?;
    dprintln!(
        leaf,
        "[LEAF_DELETE] PageIndex loaded: key_count={} segments={}",
        idx.key_count,
        idx.segments.len()
    );

    // 1. 段二分 + 段内 next() 找 key 所在位置
    let seg_idx = idx.locate_segment(key);
    let seg = &idx.segments[seg_idx];
    let mut ptr = LeafItemPtr::new(page, seg.first_item_off as usize)?;
    if seg_idx == 0 && ptr.key().is_empty() {
        ptr = match ptr
            .next()
            .map_err(|e| PageError::ItemDecode(format!("next: {e}")))?
        {
            Some(p) => p,
            None => return Ok(false),
        };
    }
    // 顺序扫描 — 找到 target item 位置 + 记录 target 的 full key (用于后面 k+1 重写)
    let mut target_off = ptr.byte_offset();
    let mut target_n = ptr.total_len();
    let mut prev_key = ptr.key().to_vec();
    dprintln!(
        leaf,
        "[LEAF_DELETE] start scan: key={:?} ptr_off={} ptr_n={} prev_key={:?}",
        std::str::from_utf8(key).unwrap_or("?"),
        target_off,
        target_n,
        String::from_utf8_lossy(&prev_key)
    );
    loop {
        if prev_key == key {
            dprintln!(
                leaf,
                "[LEAF_DELETE] matched: target_off={} target_n={} prev_key={:?}",
                target_off,
                target_n,
                String::from_utf8_lossy(&prev_key)
            );
            break;
        }
        let cur_off = target_off + target_n;
        let next_free = page_free_off(page) as usize;
        if cur_off >= next_free {
            dprintln!(
                leaf,
                "[LEAF_DELETE] not found: cur_off={} next_free={}",
                cur_off,
                next_free
            );
            return Ok(false);
        }
        let (next_item, next_n) = decode_item(page, cur_off, ItemKind::Leaf)?;
        prev_key = next_item.full_key(&prev_key);
        target_off = cur_off;
        target_n = next_n;
    }
    // target_full_key = prev_key (循环退出时 prev_key = target 的 full key)
    let target_full_key = prev_key.clone();
    let target_prev_key = if target_off > PAGE_HEADER_SIZE {
        reconstruct_key_before(page, target_off, ItemKind::Leaf)?
    } else {
        Vec::new() // target 是哨兵, 但实际我们禁止空 key 删除, 所以不会到这里
    };

    // 预计算: target_seg_idx, k+1 是否是下一段段首, target 是否是当前段段首
    // (必须在物理删除前算, 因为物理删除后 target_seg_idx 失效)
    let target_seg_idx = idx.find_segment_by_offset(target_off);
    let k1_orig_off = target_off + target_n;
    let k1_is_seg_start = target_seg_idx + 1 < idx.segments.len()
        && idx.segments[target_seg_idx + 1].first_item_off as usize == k1_orig_off;
    // target 若是当前 cp 段段首, 删除后 k+1 变新段首, 必须用 shared=0 编码
    let target_was_seg_start = target_off == idx.segments[target_seg_idx].first_item_off as usize;
    dprintln!(
        leaf,
        "[LEAF_DELETE] pre: target_seg_idx={} k1_orig_off={} k1_is_seg_start={} target_was_seg_start={}",
        target_seg_idx,
        k1_orig_off,
        k1_is_seg_start,
        target_was_seg_start
    );

    // 2. 物理删除
    let free_off = page_free_off(page) as usize;
    page.copy_within(target_off + target_n..free_off, target_off);
    page_set_free_off(page, (free_off - target_n) as u16);
    page_set_key_count(page, page_key_count(page) - 1);

    // 3. 重写下一个 item 的 shared_prefix_len (prev_key 变了).
    //    物理删除后, 原来 k+1 的 prev_key 从 (target item) 变成 (target - 1).
    //    需要用 target-1 的 full key 作为新的 prev_key 重新编码 k+1.
    //
    //    **重要**: 若 k+1 正好是 cp[target_seg_idx+1] 段首, 必须用 shared=0 编码
    //    (因为 cp 段首必须是 shared=0, 否则 PageIndex::load 失败).
    let next_off = target_off;
    let mut k1_delta: isize = 0;
    let mut k1_full_key: Vec<u8> = Vec::new();
    if next_off < page_free_off(page) as usize {
        dprintln!(
            leaf,
            "[LEAF_DELETE] rewrite k+1: target_off={} target_full_key={:?} target_prev_key={:?} next_off={} free_off={}",
            target_off,
            String::from_utf8_lossy(&target_full_key),
            String::from_utf8_lossy(&target_prev_key),
            next_off,
            page_free_off(page)
        );
        // 重新算 target-1 的 full key (作为 k+1 的新 prev_key)
        let new_prev_key = reconstruct_key_before(page, next_off, ItemKind::Leaf)?;
        // 解码 k+1
        let (k1_item, k1_old_n) = decode_item(page, next_off, ItemKind::Leaf)?;
        dprintln!(
            leaf,
            "[LEAF_DELETE] k1 decoded: shared={} unshared_len={} value_len={} old_n={} value={:?}",
            k1_item.shared_prefix_len,
            k1_item.key_unshared_len,
            k1_item.value_len,
            k1_old_n,
            std::str::from_utf8(k1_item.value).unwrap_or("?")
        );
        // k+1 的 full key 用**原 prev** (target 的 full key) 还原, 不是 new_prev_key.
        // 因为 k1_item 编码时 shared_prefix_len 是基于 target (旧 prev) 的.
        k1_full_key = k1_item.full_key(&target_full_key);
        dprintln!(
            leaf,
            "[LEAF_DELETE] k1 full_key={:?} new_prev={:?} k1_is_seg_start={} target_seg_idx={} segments.len={}",
            String::from_utf8_lossy(&k1_full_key),
            String::from_utf8_lossy(&new_prev_key),
            k1_is_seg_start,
            target_seg_idx,
            idx.segments.len()
        );
        if k1_is_seg_start {
            dprintln!(
                leaf,
                "[LEAF_DELETE] k1 next seg first_item_off={} (should match next_off={})",
                idx.segments[target_seg_idx + 1].first_item_off,
                next_off
            );
        }
        // 决定 k+1 编码时的 prev_key:
        //   若 k+1 是某 cp 段段首 (k1_is_seg_start), 或 target 原来是当前段段首
        //   (target_was_seg_start), k+1 变新段首, 必须用 shared=0 (即 prev = "") 编码.
        //   否则用 new_prev_key (target-1 的 full key).
        let k1_becomes_seg_start = k1_is_seg_start || target_was_seg_start;
        let prev_for_k1: Vec<u8> = if k1_becomes_seg_start {
            Vec::new()
        } else {
            new_prev_key.clone()
        };
        dprintln!(
            leaf,
            "[LEAF_DELETE] prev_for_k1={:?} (k1_becomes_seg_start={}, k1_is_seg_start={}, target_was_seg_start={})",
            String::from_utf8_lossy(&prev_for_k1),
            k1_becomes_seg_start,
            k1_is_seg_start,
            target_was_seg_start
        );
        // 重新编码 k+1
        let mut buf = [0u8; 4096];
        let k1_new_n = encode_leaf_item(&mut buf, &prev_for_k1, &k1_full_key, k1_item.value)?;
        dprintln!(
            leaf,
            "[LEAF_DELETE] k1 new_n={} k1_old_n={} k1_is_seg_start={}",
            k1_new_n,
            k1_old_n,
            k1_is_seg_start
        );
        k1_delta = k1_new_n as isize - k1_old_n as isize;
        if k1_delta != 0 {
            let cur_free = page_free_off(page) as usize;
            page.copy_within(
                next_off + k1_old_n..cur_free,
                (next_off as isize + k1_new_n as isize) as usize,
            );
            page_set_free_off(page, (cur_free as isize + k1_delta) as u16);
        }
        page[next_off..next_off + k1_new_n].copy_from_slice(&buf[..k1_new_n]);
    }

    // 4. 更新 PageIndex:
    //    - 删除的 item 所在段 item_count -= 1
    //    - 后续段 first_item_off 加上 delta (delete -target_n, k+1 re-encode +k1_delta)
    dprintln!(
        leaf,
        "[LEAF_DELETE] update idx: target_off={} target_n={} k1_delta={} net_delta={} target_seg_idx={}",
        target_off,
        target_n,
        k1_delta,
        k1_delta - target_n as isize,
        target_seg_idx
    );
    idx.segments[target_seg_idx].item_count -= 1;
    let net_delta = k1_delta - target_n as isize;
    if k1_is_seg_start {
        dprintln!(
            leaf,
            "[LEAF_DELETE] k+1 at next seg start, special-case cp[{}]",
            target_seg_idx + 1
        );
        idx.segments[target_seg_idx + 1].first_item_off =
            (idx.segments[target_seg_idx + 1].first_item_off as isize - target_n as isize) as u16;
        for s in idx.segments.iter_mut().skip(target_seg_idx + 2) {
            s.first_item_off = (s.first_item_off as isize + net_delta) as u16;
        }
    } else if target_was_seg_start {
        dprintln!(
            leaf,
            "[LEAF_DELETE] target was seg start, k+1 becomes new head of cp[{}]",
            target_seg_idx
        );
        // 当前段 first_item_off 不变 (k+1 现在在该位置)
        // 但 first_full_key 需要更新为 k+1 的 full key (如果 k+1 存在)
        if !k1_full_key.is_empty() {
            idx.segments[target_seg_idx].first_full_key = k1_full_key.clone();
        }
        // 后续段 (target_seg_idx+1..) 全部应用 net_delta
        for s in idx.segments.iter_mut().skip(target_seg_idx + 1) {
            s.first_item_off = (s.first_item_off as isize + net_delta) as u16;
        }
    } else {
        dprintln!(
            leaf,
            "[LEAF_DELETE] k+1 not at next seg start, normal net_delta"
        );
        for s in idx.segments.iter_mut().skip(target_seg_idx + 1) {
            s.first_item_off = (s.first_item_off as isize + net_delta) as u16;
        }
    }

    // 4.5 清理空段: item_count 减到 0 的段应当从 segments 中移除.
    //     否则后续操作引用该段 first_item_off 时, 指向错位 (指向物理上其他段内部的 item,
    //     而该 item 的 shared 是相对前段的 prev_key, 不是段首应有的 shared=0).
    //     cp[0] 永远保留 (哨兵段), 即使变空也保留.
    if idx.segments[target_seg_idx].item_count == 0 && target_seg_idx > 0 {
        dprintln!(
            leaf,
            "[LEAF_DELETE] remove empty segment cp[{}]",
            target_seg_idx
        );
        idx.segments.remove(target_seg_idx);
    }

    // 4.6 pre_merge: 若 target_seg_idx 段 item_count < MIN_PER_CHECKPOINT 且有右邻, 合并.
    //     优先尝试 full merge (total <= MAX), 失败则尝试 steal/borrow (total > MAX).
    //     都不满足则保持现状 (left 段偏小, 但不破坏不变量).
    //
    //     注意: 如果上一段清理空段时把 target_seg_idx 移除了, target_seg_idx 已无效,
    //           不能再访问 idx.segments[target_seg_idx]. 改为基于 idx.segments.len() 判断.
    let effective_seg_idx = if target_seg_idx < idx.segments.len() {
        target_seg_idx
    } else {
        // 段已被移除, 段数 -1. 若段数 > 0, pre_merge 应该看新段尾 (即原 seg_idx-1) 的右邻.
        // 但更简单的策略: 若段数 >= 1 且最后一段 item_count < MIN, 让它走 pre_merge 流程.
        idx.segments.len().saturating_sub(1)
    };
    if effective_seg_idx < idx.segments.len()
        && idx.segments[effective_seg_idx].item_count < MIN_PER_CHECKPOINT
        && effective_seg_idx + 1 < idx.segments.len()
    {
        dprintln!(
            leaf,
            "[LEAF_DELETE] pre_merge candidate: seg_idx={} item_count={} has_right_neighbor=true",
            effective_seg_idx,
            idx.segments[effective_seg_idx].item_count
        );
        let merged =
            crate::index::apply_pre_merge(page, &mut idx, effective_seg_idx, ItemKind::Leaf)?;
        dprintln!(leaf, "[LEAF_DELETE] pre_merge result: {}", merged);
        if !merged {
            // total > MAX: 尝试 steal/borrow. 借调后 left 达到 MIN, right 仍 > 0.
            let stolen = crate::index::apply_pre_merge_steal(
                page,
                &mut idx,
                effective_seg_idx,
                ItemKind::Leaf,
            )?;
            dprintln!(leaf, "[LEAF_DELETE] pre_merge_steal result: {}", stolen);
        }
    }

    idx.key_count = page_key_count(page) as usize;
    idx.write_back(page)?;

    Ok(true)
}

/// 重建 page 中 off 之前一个 item 的完整 key.
/// 从 PAGE_HEADER_SIZE 开始顺序解码到 off.
pub(crate) fn reconstruct_key_before(
    page: &[u8],
    off: usize,
    kind: ItemKind,
) -> Result<Vec<u8>, PageError> {
    dprintln!(
        leaf,
        "[RECONSTRUCT] called off={} free_off={}",
        off,
        page_free_off(page)
    );
    let mut prev_key: Vec<u8> = Vec::new();
    let mut cur_off = PAGE_HEADER_SIZE;
    let mut result = Vec::new();
    while cur_off < off {
        let (item, n) = decode_item(page, cur_off, kind)?;
        result = item.full_key(&prev_key);
        prev_key = result.clone();
        cur_off += n;
    }
    Ok(result)
}

pub fn leaf_push_back(
    page: &mut [u8],
    idx: &mut PageIndex,
    prev_key: &[u8],
    insert_off: usize,
    key: &[u8],
    value: &[u8],
    seg_idx: usize,
) -> Result<(), PageError> {
    let free_off_before = page_free_off(page) as usize;

    // Step 1: 编码新 item
    let mut buf_new = [0u8; 4096];
    let new_n = encode_leaf_item(&mut buf_new, prev_key, key, value)?;

    // Step 2: 预解码 k+1 (仍在原位置, 尚未 shift)
    let k1_off_orig = insert_off; // k+1 在原 page 中的位置
    let mut k1_buf = [0u8; 4096];
    let mut k1_new_n = 0usize;
    let mut k1_delta = 0isize;

    if k1_off_orig < free_off_before {
        let (k1_item, k1_old_n) = decode_item(page, k1_off_orig, ItemKind::Leaf)?;
        dprintln!(
            leaf,
            "[LEAF_PUSH_BACK] k+1 at off={} shared={} key_unshared_len={} value_len={} old_n={} prev_key={:?}",
            k1_off_orig,
            k1_item.shared_prefix_len,
            k1_item.key_unshared_len,
            k1_item.value_len,
            k1_old_n,
            String::from_utf8_lossy(prev_key)
        );
        if k1_item.shared_prefix_len > 0 {
            // k+1 的 full key 用旧 prev_key 还原
            let k1_full_key = k1_item.full_key(prev_key);
            dprintln!(
                leaf,
                "[LEAF_PUSH_BACK]   k1_full_key={:?} (re-encoded with new prev={:?})",
                String::from_utf8_lossy(&k1_full_key),
                String::from_utf8_lossy(key)
            );
            k1_new_n = encode_leaf_item(&mut k1_buf, key, &k1_full_key, k1_item.value)?;
            k1_delta = k1_new_n as isize - k1_old_n as isize;
            dprintln!(
                leaf,
                "[LEAF_PUSH_BACK]   k1 new_n={} delta={}",
                k1_new_n,
                k1_delta
            );
        }
    }

    // Step 3: 检查空间
    let total_delta = new_n as isize + k1_delta;
    let new_free = (free_off_before as isize + total_delta) as usize;
    let cp_size = checkpoint_area_size(idx.segments.len());
    if new_free + cp_size + PAGE_FOOTER_SIZE > PAGE_SIZE {
        return Err(PageError::PageFull);
    }

    // 注: seg_idx 由调用方传入 (prev_ptr 所在的段), 不在这里重算.
    //     边界情况下 insert_off == seg[N+1].first_item_off, 新 item 物理上扩展 seg[N].

    // Step 4: 大块后移 new_n
    page.copy_within(insert_off..free_off_before, insert_off + new_n);
    page_set_free_off(page, (free_off_before + new_n) as u16);

    // Step 5: 写入新 item
    page[insert_off..insert_off + new_n].copy_from_slice(&buf_new[..new_n]);

    // Step 6: 重写 k+1 (已移至 insert_off + new_n)
    if k1_delta != 0 {
        let k1_off = insert_off + new_n;
        let k1_old_n = (k1_new_n as isize - k1_delta) as usize;
        let current_free = page_free_off(page) as usize;
        page.copy_within(k1_off + k1_old_n..current_free, k1_off + k1_new_n);
        page_set_free_off(page, (current_free as isize + k1_delta) as u16);
        page[k1_off..k1_off + k1_new_n].copy_from_slice(&k1_buf[..k1_new_n]);
    } else if k1_new_n > 0 {
        // k1_delta == 0, 直接覆盖
        page[insert_off + new_n..insert_off + new_n + k1_new_n]
            .copy_from_slice(&k1_buf[..k1_new_n]);
    }

    // Step 7: 增量更新 PageIndex
    idx.segments[seg_idx].item_count += 1;
    // total_delta 是 isize (k+1 重编码后可能缩短, push_back 中也可能因重编码而缩短).
    // 不能直接 `as u16` (负数会 wrap 到 65535+).
    // 也不能用 wrapping_add, 那会让 first_item_off wrap 出非法地址.
    // 改为 checked_add: 负数 / overflow 都返回 None, 我们直接 panic, 以便定位 bug.
    //
    // **关键洞察**: k+1 在当前段 (seg[N]) 内部, 不在 seg[N+1] 中.
    // 后续所有段 (包括 seg[N+1..]) 都要应用 total_delta = new_n + k1_delta.
    debug_assert!(
        total_delta >= 0,
        "leaf_push_back total_delta must be >= 0, got {}",
        total_delta
    );
    if total_delta < 0 {
        panic!(
            "leaf_push_back: unexpected negative total_delta={} (new_n={} k1_delta={})",
            total_delta, new_n, k1_delta
        );
    }
    let _total_delta_u16: u16 = total_delta
        .try_into()
        .expect("leaf_push_back: total_delta overflows u16");
    for seg in idx.segments.iter_mut().skip(seg_idx + 1) {
        let new_off = (seg.first_item_off as isize)
            .checked_add(total_delta)
            .and_then(|v| u16::try_from(v).ok())
            .unwrap_or_else(|| {
                panic!(
                    "leaf_push_back: first_item_off overflow: old={} delta={}",
                    seg.first_item_off, total_delta
                )
            });
        seg.first_item_off = new_off;
    }
    idx.key_count += 1;

    Ok(())
}

/// 更新 leaf page 中 key 对应的 value. 返回 Ok(true) 表示成功, Ok(false) 表示 key 不存在.
///
/// 与 insert 的区别: 不允许新增 key, 也不允许哨兵占用.
///
/// # 流程
/// 1. 找 ptr 指向 key (locate_segment + 段内 next)
/// 2. 计算 prev_key (= ptr 之前 item 的 full key)
/// 3. 解码旧 item, 计算 old_n 和 value 字节偏移
/// 4. 计算 new_n = encode_leaf_item(prev_key, key, new_value)
/// 5. 若 new_n == old_n: 就地替换 value (含 vint 前缀). 不动 PageIndex.
///    否则: 大块后移 (old_n != new_n), 重写 item 字节, 更新 free_off + PageIndex.
///
/// # 设计要点
/// - `prev_key` 不变 → shared_prefix_len 和 key_unshared 字节都不变
/// - 只替换 vint(value_len) + value bytes
/// - new_n != old_n 时, 后续 items 物理后移/前移, free_off 调整
pub fn leaf_update(page: &mut [u8], key: &[u8], new_value: &[u8]) -> Result<bool, PageError> {
    if page_type(page) != PageType::Leaf {
        return Err(PageError::InvalidPageType {
            expected: PageType::Leaf,
            got: page_type(page),
        });
    }
    if key.is_empty() {
        return Err(PageError::ItemDecode(
            "empty key is reserved for sentinel".into(),
        ));
    }
    if page_key_count(page) == 0 {
        return Ok(false);
    }

    let mut idx = PageIndex::load(page, ItemKind::Leaf)?;
    let seg_idx = idx.locate_segment(key);
    let seg_first_off = idx.segments[seg_idx].first_item_off;
    let mut ptr = LeafItemPtr::new(page, seg_first_off as usize)?;
    // 哨兵跳过: 拿真实 prev_key 用 (否则 prev_ptr.key() 是 "", 而非哨兵的 prev — 但哨兵是 item 0,
    // 它的 prev 是空. 跳过哨兵后, prev_ptr 应指向哨兵自身, key="").
    let mut prev_ptr: Option<LeafItemPtr> = None;
    if seg_idx == 0 && ptr.key().is_empty() {
        // 哨兵本身就是 first 段首, prev 是空. 记录哨兵为 prev_ptr.
        prev_ptr = Some(ptr.clone());
        ptr = match ptr
            .next()
            .map_err(|e| PageError::ItemDecode(format!("next: {e}")))?
        {
            Some(p) => p,
            None => return Ok(false),
        };
    }

    // 顺序扫描找 target item, 同步跟踪 prev_ptr (用于 prev_key).
    // **关键**: 循环进入时 prev_ptr 还没记录 ptr, 必须先把 ptr 作为 prev 记下,
    // 再 `next()` 推进 ptr. 退出时 prev_ptr.key() 是 target 之前那个 item 的 full key.
    if prev_ptr.is_none() {
        prev_ptr = Some(ptr.clone());
    }
    loop {
        if ptr.key() == key {
            break;
        }
        prev_ptr = Some(ptr.clone());
        let cur = match ptr
            .next()
            .map_err(|e| PageError::ItemDecode(format!("next: {e}")))?
        {
            Some(p) => p,
            None => return Ok(false),
        };
        ptr = cur;
    }
    let prev_ptr = prev_ptr.expect("loop must set prev_ptr");

    let old_off = ptr.byte_offset();
    let old_n = ptr.total_len();
    // ⭐ 修复 (2026-07-26): target 恰好是段首 item 时 (seg_idx > 0, 扫描第一个就命中),
    // prev_ptr 初始化为 target 自身 → prev_key == key → 重编码后 shared = len-1,
    // 破坏段首 shared=0 自包含不变量 (memtier 长公共前缀 key 覆盖写必现:
    // "segment head item must have shared=0, got shared=15").
    // 段首的编码语义 prev 必须视为空 (shared=0), 与原 encoding 一致.
    let prev_key = if prev_ptr.byte_offset() == old_off {
        Vec::new()
    } else {
        prev_ptr.key().to_vec()
    };

    // 解码旧 item, 取 value 起始偏移
    let (old_item, _) = decode_item(page, old_off, ItemKind::Leaf)?;
    let key_end = old_off + 4 + old_item.key_unshared_len as usize;
    // 跳过 vint(value_len) 拿到 value 起始
    let (_v_len, vint_size) = decode_varint(&page[key_end..])
        .ok_or_else(|| PageError::ItemDecode("bad varint for value_len".into()))?;
    let _value_off = key_end + vint_size;

    // 计算 new_n (基于 prev_key)
    let mut new_buf = [0u8; 4096];
    let new_n = encode_leaf_item(&mut new_buf, &prev_key, key, new_value)?;

    let free_off_before = page_free_off(page) as usize;

    if new_n == old_n {
        // 快路径: 同字节数, 就地覆盖 [old_off .. old_off + old_n]
        dprintln!(
            leaf,
            "[LEAF_UPDATE] in-place at off={} (same size {})",
            old_off,
            old_n
        );
        page[old_off..old_off + new_n].copy_from_slice(&new_buf[..new_n]);
        // PageIndex 不变 (cp array 和 key_count 都无需更新)
    } else {
        // 慢路径: new_n != old_n, 需要 shift 后续 items
        let delta = new_n as isize - old_n as isize;
        dprintln!(
            leaf,
            "[LEAF_UPDATE] shift at off={} old_n={} new_n={} delta={}",
            old_off,
            old_n,
            new_n,
            delta
        );
        // copy_within 自动处理 dst > src 或 dst < src 两种方向
        page.copy_within(old_off + old_n..free_off_before, old_off + new_n);
        page_set_free_off(page, (free_off_before as isize + delta) as u16);
        // 写新 item
        page[old_off..old_off + new_n].copy_from_slice(&new_buf[..new_n]);
        // k+1 的 prev_key 不变 (== prev_key 仍是 ptr 之前那个 item 的 full key),
        // shared 不变, 不需要重写 k+1 字节 (它的 prev_key 字节布局没变).
        // 但 k+1 在物理位置上平移了 delta 字节 (copy_within 已经处理).
        //
        // PageIndex: 后续段的 first_item_off 需要 += delta.
        let target_seg_idx = idx.find_segment_by_offset(old_off);
        for seg in idx.segments.iter_mut().skip(target_seg_idx + 1) {
            let new_off = (seg.first_item_off as isize + delta) as u16;
            seg.first_item_off = new_off;
        }
        idx.write_back(page)?;
    }

    Ok(true)
}

// ===== 内部 helper =====

// ============================================================================
// 已废弃 (Phase 7 清理)
// ============================================================================
//
// 之前以下函数已删除, 用 PageIndex + LeafItemPtr/InternalItemPtr 替代:
// - locate_item / locate_item_by_index: 用 PageIndex::load + locate_segment + 段内 next()
// - reconstruct_key_at_index: 用 LeafItemPtr.key() / InternalItemPtr.key()
// - read_cp_full_key: 用 PageIndex.segments[i].first_full_key (cached in PageIndex)
//
// 这些函数 O(N) 性能不可接受, PageIndex 提供 O(log segments) + O(段内 item 数) 定位.
