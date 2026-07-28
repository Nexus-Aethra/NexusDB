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

    // 2. pre_split: 如果目标段已满, 先分裂
    if idx.segments[cur_seg_idx].item_count >= MAX_PER_CHECKPOINT {
        dprintln!(
            leaf,
            "[LEAF_INSERT] pre_split triggered: seg_idx={} item_count={} key={:?}",
            cur_seg_idx,
            idx.segments[cur_seg_idx].item_count,
            std::str::from_utf8(key).unwrap_or("?")
        );
        dprintln!(
            leaf,
            "[LEAF_INSERT]   idx.segments.len BEFORE pre_split = {}",
            idx.segments.len()
        );
        let pre_result = pre_split_segment(page, &mut idx, cur_seg_idx, ItemKind::Leaf);
        dprintln!(leaf, "[LEAF_INSERT] pre_split result: {:?}", pre_result);
        dprintln!(
            leaf,
            "[LEAF_INSERT]   idx.segments.len AFTER pre_split = {}",
            idx.segments.len()
        );
        pre_result?;
        dprintln!(
            leaf,
            "[LEAF_INSERT] pre_split done. segments.len={}, segments={:?}",
            idx.segments.len(),
            idx.segments
                .iter()
                .map(|s| (s.item_count, s.first_item_off))
                .collect::<Vec<_>>()
        );
        // 重新定位段
        cur_seg_idx = idx.locate_segment(key);
        dprintln!(
            leaf,
            "[LEAF_INSERT] after pre_split: cur_seg_idx={}",
            cur_seg_idx
        );
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

/// 强制在段内中点创建段边界 (无视 MAX_PER_CHECKPOINT 限制).
/// 复用 `pre_split_segment` 的核心逻辑, 但去掉了 "item_count < MAX" 的早退.
fn force_split_segment_at_mid(
    page: &mut [u8],
    idx: &mut PageIndex,
    seg_idx: usize,
    kind: ItemKind,
) -> Result<(), PageError> {
    use crate::index::Segment;

    let seg = &idx.segments[seg_idx];
    if seg.item_count < 3 {
        return Err(PageError::SplitTooFew(seg.item_count as usize));
    }

    let mid_offset = (seg.item_count / 2) as usize;
    let front_count = mid_offset as u16;
    let back_count = seg.item_count - front_count;

    // 段内遍历到 mid item
    let mut prev_key = seg.first_full_key.clone();
    let mut off = seg.first_item_off as usize;
    for _ in 0..mid_offset {
        let (item, n) = decode_item(page, off, kind)?;
        prev_key = item.full_key(&prev_key);
        off += n;
    }
    let mid_item_off = off;

    // 解码 mid item 并重编码为 shared=0
    let (mid_item, mid_old_n) = decode_item(page, mid_item_off, kind)?;
    let mid_full_key = mid_item.full_key(&prev_key);
    let mut buf = [0u8; 4096];
    let mid_new_n = encode_leaf_item(&mut buf, &[], &mid_full_key, mid_item.value)?;
    let delta_mid = mid_new_n as isize - mid_old_n as isize;

    // 移动后续 items
    if delta_mid != 0 {
        let free_off = page_free_off(page) as usize;
        page.copy_within(
            mid_item_off + mid_old_n..free_off,
            (mid_item_off as isize + mid_new_n as isize) as usize,
        );
        page_set_free_off(page, (free_off as isize + delta_mid) as u16);
    }
    page[mid_item_off..mid_item_off + mid_new_n].copy_from_slice(&buf[..mid_new_n]);

    // 更新 PageIndex
    idx.segments[seg_idx].item_count = front_count;
    let new_seg = Segment {
        first_item_off: mid_item_off as u16,
        item_count: back_count,
        first_full_key: mid_full_key.clone(),
    };
    idx.segments.insert(seg_idx + 1, new_seg);

    // k+1 重写 + 后续段 offset 更新 (与 pre_split_segment 相同逻辑)
    if back_count > 1 {
        let k1_off = mid_item_off + mid_new_n;
        let free_off = page_free_off(page) as usize;
        if k1_off < free_off {
            let (k1_item, k1_old_n) = decode_item(page, k1_off, kind)?;
            let k1_full_key = k1_item.full_key(&mid_full_key);
            let mut k1_buf = [0u8; 4096];
            let k1_new_n = encode_leaf_item(&mut k1_buf, &mid_full_key, &k1_full_key, k1_item.value)?;
            let k1_delta = k1_new_n as isize - k1_old_n as isize;
            if k1_delta != 0 {
                let cur_free = page_free_off(page) as usize;
                page.copy_within(
                    k1_off + k1_old_n..cur_free,
                    (k1_off as isize + k1_new_n as isize) as usize,
                );
                page_set_free_off(page, (cur_free as isize + k1_delta) as u16);
            }
            page[k1_off..k1_off + k1_new_n].copy_from_slice(&k1_buf[..k1_new_n]);
            let total_delta = delta_mid + k1_delta;
            for s in idx.segments.iter_mut().skip(seg_idx + 2) {
                s.first_item_off = (s.first_item_off as isize + total_delta) as u16;
            }
        } else {
            for s in idx.segments.iter_mut().skip(seg_idx + 2) {
                s.first_item_off = (s.first_item_off as isize + delta_mid) as u16;
            }
        }
    } else {
        for s in idx.segments.iter_mut().skip(seg_idx + 2) {
            s.first_item_off = (s.first_item_off as isize + delta_mid) as u16;
        }
    }

    Ok(())
}

/// ⭐ Bulk memcpy leaf split: 基于 checkpoint 段边界的整段拷贝.
///
/// 返回 Some(split_key) 表示成功, None 表示条件不满足 (回退到原始逻辑).
/// 安全性: 任何内部错误都返回 None (不 panic, 不破坏 page).
fn try_bulk_leaf_split(
    left: &mut [u8; PAGE_SIZE],
    right: &mut [u8; PAGE_SIZE],
) -> Option<Vec<u8>> {
    let mut idx = PageIndex::load(left, ItemKind::Leaf).ok()?;

    // 如果只有 1 个段, 强制在段内创建段边界
    if idx.segments.len() < 2 {
        let seg = &idx.segments[0];
        // 需要至少 4 items (哨兵 + 3 真实 keys) 才能分出两侧各至少 1 key
        // (因为 mid = item_count/2 >= 2, front=2 含哨兵+1key, back=剩余)
        if seg.item_count < 4 {
            return None;
        }
        force_split_segment_at_mid(left, &mut idx, 0, ItemKind::Leaf).ok()?;
        idx.write_back(left).ok()?;
    }

    // 现在保证 >= 2 个段
    if idx.segments.len() < 2 {
        return None; // 防御性
    }

    let free = page_free_off(left) as usize;

    // 找字节偏移最接近中点的段边界
    let byte_mid = (PAGE_HEADER_SIZE + free) / 2;
    let mut best_seg = 1usize;
    let mut best_dist = usize::MAX;
    for i in 1..idx.segments.len() {
        let seg_off = idx.segments[i].first_item_off as usize;
        let dist = seg_off.abs_diff(byte_mid);
        if dist < best_dist {
            best_dist = dist;
            best_seg = i;
        }
    }

    let split_off = idx.segments[best_seg].first_item_off as usize;
    let split_key = idx.segments[best_seg].first_full_key.clone();

    // 计算 key counts
    let left_total_items: u16 = idx.segments[..best_seg].iter().map(|s| s.item_count).sum();
    let left_key_count = left_total_items - 1; // 减去哨兵
    let right_key_count: u16 = idx.segments[best_seg..].iter().map(|s| s.item_count).sum();

    // 安全检查
    if left_key_count == 0 || right_key_count == 0 {
        return None;
    }

    // === 初始化 right: header + 哨兵 ===
    page_init_header(right, PageType::Leaf);
    let mut sent_buf = [0u8; 64];
    let sent_n = encode_leaf_item(&mut sent_buf, &[], b"", b"").ok()?;
    right[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + sent_n].copy_from_slice(&sent_buf[..sent_n]);
    let write_start = PAGE_HEADER_SIZE + sent_n;

    // === BULK MEMCPY: 一次 copy 完成分裂 ===
    let src_len = free - split_off;
    right[write_start..write_start + src_len].copy_from_slice(&left[split_off..free]);
    let right_free = write_start + src_len;

    // === 重建 right cp array ===
    write_checkpoint_header(
        right,
        CheckpointHeader {
            checkpoint_count: 1,
            ..Default::default()
        },
    );
    write_checkpoint(
        right,
        0,
        Checkpoint {
            item_count: 1 + right_key_count,
            first_item_off: PAGE_HEADER_SIZE as u16,
        },
    );
    page_set_free_off(right, right_free as u16);
    page_set_key_count(right, right_key_count);
    let right_idx = PageIndex::load(right, ItemKind::Leaf).ok()?;
    right_idx.write_back(right).ok()?;
    crate::header::page_set_vpid(right, 0);

    // === 截断 left ===
    left[split_off..free].fill(0);
    page_set_free_off(left, split_off as u16);
    page_set_key_count(left, left_key_count);

    // 重建 left cp array (与原始 leaf_split 相同的防御性模式)
    write_checkpoint_header(left, CheckpointHeader::default());
    let left_idx = PageIndex::load(left, ItemKind::Leaf).ok()?;
    if left_idx.segments.is_empty() && left_key_count > 0 {
        write_checkpoint(
            left,
            0,
            Checkpoint {
                item_count: left_key_count + 1,
                first_item_off: PAGE_HEADER_SIZE as u16,
            },
        );
        write_checkpoint_header(
            left,
            CheckpointHeader {
                checkpoint_count: 1,
                ..Default::default()
            },
        );
    } else {
        left_idx.write_back(left).ok()?;
    }

    Some(split_key)
}

/// 分裂 leaf page.
///
/// **⭐ 统一算法 (2026-07-25)**: 基于 checkpoint 段边界的整段 bulk memcpy.
///
/// 流程:
/// 1. 加载 PageIndex, 若只有 1 段则先 pre_split_segment 创建边界
/// 2. 找字节偏移最接近中点的段边界
/// 3. 初始化 right (header + 哨兵) + bulk memcpy 整段
/// 4. 重建双方 cp array
/// 5. 截断 left, 返回 split_key
pub fn leaf_split(
    left: &mut [u8; PAGE_SIZE],
    right: &mut [u8; PAGE_SIZE],
) -> Result<Vec<u8>, PageError> {
    if page_type(left) != PageType::Leaf {
        return Err(PageError::InvalidPageType {
            expected: PageType::Leaf,
            got: page_type(left),
        });
    }
    let real_keys = page_key_count(left) as usize;
    if real_keys < 2 {
        return Err(PageError::SplitTooFew(real_keys));
    }

    // 统一路径: bulk memcpy
    // 唯一例外: real_keys=2 (只有 2 个 key, 无法在段边界 split)
    // 此时用最简单的单 item 移动
    match try_bulk_leaf_split(left, right) {
        Some(split_key) => Ok(split_key),
        None => leaf_split_minimal(left, right),
    }
}

/// 最小化 split: 用于 real_keys=2~3 的极端情况 (不足以建立段边界).
/// 按 item 数量中点分割, 逐项重编码后半到 right.
fn leaf_split_minimal(
    left: &mut [u8; PAGE_SIZE],
    right: &mut [u8; PAGE_SIZE],
) -> Result<Vec<u8>, PageError> {
    let real_keys = page_key_count(left) as usize;
    let mid = real_keys / 2; // left 保留 mid 个真实 keys

    // 顺序扫描找到 mid+1 个 items (哨兵 + mid 真实) 的末尾
    let mut off = PAGE_HEADER_SIZE;
    let mut prev_key: Vec<u8> = Vec::new();
    let mut mid_off: usize = 0;
    let mut mid_full_key: Vec<u8> = Vec::new();
    let mut mid_value: Vec<u8> = Vec::new();
    let mut mid_n: usize = 0;
    for i in 0..mid + 2 {
        let (item, n) = decode_item(left, off, ItemKind::Leaf)?;
        let full = item.full_key(&prev_key);
        if i == mid {
            mid_off = off + n;
        }
        if i == mid + 1 {
            mid_full_key = full;
            mid_value = item.value.to_vec();
            mid_n = n;
            break;
        }
        prev_key = full;
        off += n;
    }
    let left_orig_free = page_free_off(left) as usize;
    let mut new_prev_key = mid_full_key.clone();

    // 初始化 right
    page_init_header(right, PageType::Leaf);
    let mut sent_buf = [0u8; 64];
    let sent_n = encode_leaf_item(&mut sent_buf, &[], b"", b"")?;
    right[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + sent_n].copy_from_slice(&sent_buf[..sent_n]);
    let mut write_off = PAGE_HEADER_SIZE + sent_n;
    let mut new_key_count: u16 = 0;

    // mid item 作为 right 第一个真实 item (shared=0)
    {
        let mut buf = [0u8; 4096];
        let n = encode_leaf_item(&mut buf, &[], &mid_full_key, &mid_value)?;
        right[write_off..write_off + n].copy_from_slice(&buf[..n]);
        write_off += n;
        new_key_count += 1;
    }
    // 后续 items
    let mut cur = mid_off + mid_n;
    while cur < left_orig_free {
        let (item, n) = decode_item(left, cur, ItemKind::Leaf)?;
        let full = item.full_key(&new_prev_key);
        let mut buf = [0u8; 4096];
        let enc_n = encode_leaf_item(&mut buf, &new_prev_key, &full, item.value)?;
        right[write_off..write_off + enc_n].copy_from_slice(&buf[..enc_n]);
        write_off += enc_n;
        new_prev_key = full;
        new_key_count += 1;
        cur += n;
    }

    // right cp
    page_set_free_off(right, write_off as u16);
    page_set_key_count(right, new_key_count);
    write_checkpoint_header(right, CheckpointHeader { checkpoint_count: 1, ..Default::default() });
    write_checkpoint(right, 0, Checkpoint { item_count: 1 + new_key_count, first_item_off: PAGE_HEADER_SIZE as u16 });
    let ri = PageIndex::load(right, ItemKind::Leaf)?;
    ri.write_back(right)?;
    crate::header::page_set_vpid(right, 0);

    // 截断 left
    left[mid_off..left_orig_free].fill(0);
    page_set_free_off(left, mid_off as u16);
    page_set_key_count(left, mid as u16);
    write_checkpoint_header(left, CheckpointHeader::default());
    let li = PageIndex::load(left, ItemKind::Leaf)?;
    if li.segments.is_empty() && mid > 0 {
        write_checkpoint(left, 0, Checkpoint { item_count: mid as u16 + 1, first_item_off: PAGE_HEADER_SIZE as u16 });
        write_checkpoint_header(left, CheckpointHeader { checkpoint_count: 1, ..Default::default() });
    } else {
        li.write_back(left)?;
    }

    Ok(mid_full_key)
}


// ===== leaf_push_back: 在 ptr 后插入新 item =====

/// 在 `insert_off` 之后插入 `(key, value)`.
///
/// - `prev_key`: ptr 指向的 item 的完整 key (用于编码新 item 的前缀压缩)
/// - `insert_off`: 插入位置 = ptr.off + ptr.total_len()
/// - `seg_idx`: prev_ptr 所在的段 idx (新 item 物理上扩展该段)
///
/// # 流程 (按计划)
///
/// 1. 编码新 item (prev_key = ptr.key())
/// 2. 预解码 k+1 (在 shift 之前), 计算 k1_delta
/// 3. 检查空间
/// 4. `copy_within(insert_off..free_off, insert_off + new_n)` — 大块后移
/// 5. 写入新 item
/// 6. 重写 k+1 (shared_prefix_len 变了)
/// 7. 增量更新 PageIndex
///
/// # 关键洞察
///
/// 只有紧邻的 k+1 需要重写 shared_prefix_len.
/// k+2 及之后的 prev_key 字节没变, 不需要重写.
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
