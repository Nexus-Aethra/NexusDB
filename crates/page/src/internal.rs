//! Internal Page 操作: child 导航 + 插入 + 删除 + 分裂.

use crate::checkpoint::{
    Checkpoint, CheckpointHeader, MIN_PER_CHECKPOINT, checkpoint_area_size, write_checkpoint,
    write_checkpoint_header,
};
use crate::error::PageError;
use crate::header::{
    PAGE_FOOTER_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE, PageType, page_check_magic, page_free_off,
    page_init_header, page_key_count, page_set_free_off, page_set_key_count, page_set_vpid,
    page_type, page_vpid,
};
use crate::index::PageIndex;
use crate::item::{ItemKind, decode_item, encode_internal_item};
use crate::ptr::InternalItemPtr;

/// 创建空 internal page.
pub fn internal_new() -> [u8; PAGE_SIZE] {
    let mut page = [0u8; PAGE_SIZE];
    page_init_header(&mut page, PageType::Internal);
    write_checkpoint_header(&mut page, CheckpointHeader::default());
    page
}

/// 给定 key, 返回对应 child page 的 vpid.
///
/// 段二分定位段 + 段内顺序扫描, 跳过哨兵.
/// 哨兵的 key="" 永远 <= 任何 key, 但它的 child_vpid=0 应当被忽略.
pub fn internal_child(page: &[u8], key: &[u8]) -> Option<u64> {
    internal_child_with_bounds(page, key).map(|(c, _, _)| c)
}

/// ⭐ 区间版 internal_child: 顺带返回选中 child 的覆盖区间
/// `(child, lower_sep, upper_sep)` — child 覆盖 `[lower_sep, upper_sep)`.
///
/// - `lower_sep = None`: 走 first_child / 页内无更小 sep (下界由父层给)
/// - `upper_sep = None`: 页内无更大 sep (上界由父层给)
///
/// 零额外扫描成本: 现有 "找最大 sep <= key" 的循环本就路过这两个词
/// (chosen 的 sep 与第一个 > key 的 sep). travel 逐层收窄即得 leaf
/// 覆盖区间, 供批量操作判断 "下一个 key 是否同一 leaf" (免回 root).
#[allow(clippy::type_complexity)]
pub fn internal_child_with_bounds(
    page: &[u8],
    key: &[u8],
) -> Option<(u64, Option<Vec<u8>>, Option<Vec<u8>>)> {
    if page_type(page) != PageType::Internal {
        return None;
    }
    if page_check_magic(page).is_err() {
        return None;
    }
    if key.is_empty() {
        return None;
    }
    if page_key_count(page) == 0 {
        return None;
    }

    let first_child = page_vpid(page);
    let idx = PageIndex::load(page, ItemKind::Internal).ok()?;
    if idx.segments.is_empty() {
        return Some((first_child, None, None));
    }
    let seg_idx = idx.locate_segment(key);
    let seg = &idx.segments[seg_idx];
    // 段内顺序扫描
    let mut ptr = InternalItemPtr::new(page, seg.first_item_off as usize).ok()?;
    // cp[0] 段首是哨兵, 跳过
    if seg_idx == 0 && ptr.key().is_empty() {
        ptr = match ptr.next() {
            Ok(Some(p)) => p,
            _ => return Some((first_child, None, None)), // 只有哨兵, 走 first_child
        };
    }
    // 找最大 i 使 full_key(i) <= key. chosen 默认 first_child.
    let (mut chosen, mut lower) = if seg_idx == 0 {
        (first_child, None)
    } else {
        (ptr.child_vpid(), Some(ptr.key().to_vec()))
    };
    // 段内找到 >= key 时停止
    let mut upper: Option<Vec<u8>> = None;
    loop {
        if ptr.key() > key {
            upper = Some(ptr.key().to_vec());
            break;
        }
        chosen = ptr.child_vpid();
        lower = Some(ptr.key().to_vec());
        match ptr.next() {
            Ok(Some(p)) => ptr = p,
            _ => break, // 页尾: 无更大 sep, 上界由父层给
        }
    }
    Some((chosen, lower, upper))
}

/// 在 internal page 中插入 (separator_key, child_vpid).
/// 使用 PageIndex + push_back (与 leaf_insert 流程一致).
pub fn internal_insert(page: &mut [u8], sep_key: &[u8], child_vpid: u64) -> Result<(), PageError> {
    use crate::checkpoint::MAX_PER_CHECKPOINT;
    use crate::index::pre_split_segment;
    use crate::ptr::InternalItemPtr;

    if page_type(page) != PageType::Internal {
        return Err(PageError::InvalidPageType {
            expected: PageType::Internal,
            got: page_type(page),
        });
    }
    if sep_key.is_empty() {
        return Err(PageError::ItemDecode(
            "empty key is reserved for sentinel".into(),
        ));
    }
    if page_key_count(page) == 0 {
        // 0 真实 keys: 调用 init_sentinel 风格的初始化, 但用 internal encoding.
        let mut buf = [0u8; 4096];
        let n = encode_internal_item(&mut buf, &[], &[], 0)?;
        let off = PAGE_HEADER_SIZE;
        page[off..off + n].copy_from_slice(&buf[..n]);
        page_set_free_off(page, (off + n) as u16);
        let hdr = CheckpointHeader {
            checkpoint_count: 1,
            ..Default::default()
        };
        write_checkpoint_header(page, hdr);
        write_checkpoint(
            page,
            0,
            Checkpoint {
                item_count: 1,
                first_item_off: off as u16,
            },
        );
    }

    let mut idx = PageIndex::load(page, ItemKind::Internal)?;

    // 1. 段二分定位
    let seg_idx = idx.locate_segment(sep_key);
    let mut cur_seg_idx = seg_idx;

    // 2. pre_split
    if idx.segments[cur_seg_idx].item_count >= MAX_PER_CHECKPOINT {
        pre_split_segment(page, &mut idx, cur_seg_idx, ItemKind::Internal)?;
        cur_seg_idx = idx.locate_segment(sep_key);
    }

    // 3. 段内顺序扫描找 prev_ptr (与 leaf_insert 同样的设计)
    //    prev_ptr = 段内 <sep_key 的最后一个 item (插入位置在 prev_ptr 之后)
    //    哨兵也作为 prev_ptr 候选 (空 key 是所有真实 key 的 prev)
    let mut ptr = InternalItemPtr::new(page, idx.segments[cur_seg_idx].first_item_off as usize)?;
    let mut prev_ptr = ptr.clone();
    loop {
        if ptr.key() >= sep_key {
            break;
        }
        prev_ptr = ptr.clone();
        let next = ptr
            .next()
            .map_err(|e| PageError::ItemDecode(format!("next: {e}")))?;
        match next {
            None => break,
            Some(p) => ptr = p,
        }
    }

    // 4. 检查重复 (sep_key 已存在)
    //    **重要**: pre_split 可能已经修改了 page, 需要 write_back 保存 pre_split 的变更.
    if ptr.key() == sep_key {
        idx.write_back(page)?;
        return Err(PageError::ItemDecode(
            "key exists, overwrite not yet supported".into(),
        ));
    }
    if let Some(p) = ptr
        .next()
        .map_err(|e| PageError::ItemDecode(format!("next: {e}")))?
        && p.key() == sep_key
    {
        idx.write_back(page)?;
        return Err(PageError::ItemDecode(
            "key exists, overwrite not yet supported".into(),
        ));
    }

    // 5. push_back (用 prev_ptr 而非 ptr, 因为 ptr 可能是新 key 的位置)
    let prev_key = prev_ptr.key().to_vec();
    let insert_off = prev_ptr.byte_offset() + prev_ptr.total_len();
    // 定位 prev_ptr 所在的段 (不是 insert_off! 边界情况下 prev_ptr 在 seg[N] 末尾,
    // insert_off == seg[N+1].first_item_off, 新 item 物理上扩展 seg[N]).
    //
    // **重要**: 必须用 prev_ptr.byte_offset() 而不是 insert_off 调用 find_segment_by_offset,
    // 因为 insert_off 恰好等于 cp[N+1].first_item_off 时, 用 insert_off 会让 find 返回 N+1
    // (因 find 使用 <=), 错误地把新 item 算到 seg[N+1] 里, 导致 cp[N+1] 指向 shared!=0 的 item.
    let seg_idx_for_push = idx.find_segment_by_offset(prev_ptr.byte_offset());
    drop(ptr);
    drop(prev_ptr);
    internal_push_back(
        page,
        &mut idx,
        &prev_key,
        insert_off,
        sep_key,
        child_vpid,
        seg_idx_for_push,
    )?;
    idx.write_back(page)?;
    Ok(())
}

/// 在 internal page 中按 key 删除 separator. 返回 true 表示找到并删除.
pub fn internal_delete(page: &mut [u8], key: &[u8]) -> Result<bool, PageError> {
    use crate::dprintln;

    if page_type(page) != PageType::Internal {
        return Err(PageError::InvalidPageType {
            expected: PageType::Internal,
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

    let mut idx = PageIndex::load(page, ItemKind::Internal)?;
    let seg_idx = idx.locate_segment(key);
    let seg = &idx.segments[seg_idx];
    let mut ptr = InternalItemPtr::new(page, seg.first_item_off as usize)?;
    if seg_idx == 0 && ptr.key().is_empty() {
        ptr = match ptr.next() {
            Ok(Some(p)) => p,
            _ => return Ok(false),
        };
    }

    // 段内顺序扫描找 target
    let mut target_off: Option<usize> = None;
    let mut target_n: usize = 0;
    if ptr.key() == key {
        target_off = Some(ptr.byte_offset());
        target_n = ptr.total_len();
    } else {
        let mut cur = ptr;
        while let Ok(Some(next)) = cur.next() {
            if next.key() == key {
                target_off = Some(next.byte_offset());
                target_n = next.total_len();
                break;
            }
            cur = next;
        }
    }
    let Some(target_off) = target_off else {
        return Ok(false);
    };
    // target_full_key = key (prev_key 历遍结束后, key 是 target 的 full key)
    let target_full_key = key.to_vec();

    // 预计算: target_seg_idx, k+1 是否是下一段段首
    let target_seg_idx = idx.find_segment_by_offset(target_off);
    let k1_orig_off = target_off + target_n;
    let k1_is_seg_start = target_seg_idx + 1 < idx.segments.len()
        && idx.segments[target_seg_idx + 1].first_item_off as usize == k1_orig_off;
    let target_was_seg_start = target_off == idx.segments[target_seg_idx].first_item_off as usize;
    dprintln!(
        internal,
        "[INTERNAL_DELETE] target_off={} target_n={} target_seg_idx={} k1_is_seg_start={} target_was_seg_start={}",
        target_off,
        target_n,
        target_seg_idx,
        k1_is_seg_start,
        target_was_seg_start
    );

    // 物理删除
    let free_off = page_free_off(page) as usize;
    page.copy_within(target_off + target_n..free_off, target_off);
    page_set_free_off(page, (free_off - target_n) as u16);
    page_set_key_count(page, page_key_count(page) - 1);

    // 物理删除后, 原来 k+1 的 prev_key 从 target 变成 target_prev_key.
    // 必须重写 k+1 的 shared_prefix_len, 否则 PageIndex 解析时会得到错位 key.
    //
    // 关键: 若 k+1 变成新 cp 段首 (k1_is_seg_start 或 target_was_seg_start),
    //      必须用 shared=0 编码 (cp 段首必须是 shared=0).
    let next_off = target_off;
    let mut k1_delta: isize = 0;
    let mut k1_full_key: Vec<u8> = Vec::new();
    if next_off < page_free_off(page) as usize {
        // 重新算 target-1 的 full key (作为 k+1 的新 prev_key)
        let new_prev_key = crate::leaf::reconstruct_key_before(page, next_off, ItemKind::Internal)?;
        // 解码 k+1
        let (k1_item, k1_old_n) = decode_item(page, next_off, ItemKind::Internal)?;
        dprintln!(
            internal,
            "[INTERNAL_DELETE] k1 decoded: shared={} unshared_len={} old_n={} child={}",
            k1_item.shared_prefix_len,
            k1_item.key_unshared_len,
            k1_old_n,
            k1_item.child_vpid
        );
        // k+1 的 full key 用**原 prev** (target 的 full key) 还原, 不是 new_prev_key.
        // 因为 k1_item 编码时 shared_prefix_len 是基于 target (旧 prev) 的.
        k1_full_key = k1_item.full_key(&target_full_key);
        dprintln!(
            internal,
            "[INTERNAL_DELETE] k1 full_key={:?} new_prev={:?} k1_is_seg_start={} target_was_seg_start={} target_seg_idx={} segments.len={}",
            String::from_utf8_lossy(&k1_full_key),
            String::from_utf8_lossy(&new_prev_key),
            k1_is_seg_start,
            target_was_seg_start,
            target_seg_idx,
            idx.segments.len()
        );
        // 决定 k+1 编码时的 prev_key:
        //   若 k+1 变新段首, 必须 shared=0.
        //   否则用 new_prev_key (target-1 的 full key).
        let k1_becomes_seg_start = k1_is_seg_start || target_was_seg_start;
        let prev_for_k1: Vec<u8> = if k1_becomes_seg_start {
            Vec::new()
        } else {
            new_prev_key.clone()
        };
        dprintln!(
            internal,
            "[INTERNAL_DELETE] prev_for_k1={:?} (k1_becomes_seg_start={})",
            String::from_utf8_lossy(&prev_for_k1),
            k1_becomes_seg_start
        );
        // 重新编码 k+1
        let mut buf = [0u8; 4096];
        let k1_new_n =
            encode_internal_item(&mut buf, &prev_for_k1, &k1_full_key, k1_item.child_vpid)?;
        dprintln!(
            internal,
            "[INTERNAL_DELETE] k1 new_n={} k1_old_n={} k1_delta={}",
            k1_new_n,
            k1_old_n,
            k1_new_n as isize - k1_old_n as isize
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

    // 更新 PageIndex:
    //    - 删除的 item 所在段 item_count -= 1
    //    - 后续段 first_item_off 加上 net_delta (delete -target_n, k+1 re-encode +k1_delta)
    dprintln!(
        internal,
        "[INTERNAL_DELETE] update idx: target_off={} target_n={} k1_delta={} net_delta={} target_seg_idx={}",
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
            internal,
            "[INTERNAL_DELETE] k+1 at next seg start, special-case cp[{}]",
            target_seg_idx + 1
        );
        idx.segments[target_seg_idx + 1].first_item_off =
            (idx.segments[target_seg_idx + 1].first_item_off as isize - target_n as isize) as u16;
        for s in idx.segments.iter_mut().skip(target_seg_idx + 2) {
            s.first_item_off = (s.first_item_off as isize + net_delta) as u16;
        }
    } else if target_was_seg_start {
        dprintln!(
            internal,
            "[INTERNAL_DELETE] target was seg start, k+1 becomes new head of cp[{}]",
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
            internal,
            "[INTERNAL_DELETE] k+1 not at next seg start, normal net_delta"
        );
        for s in idx.segments.iter_mut().skip(target_seg_idx + 1) {
            s.first_item_off = (s.first_item_off as isize + net_delta) as u16;
        }
    }

    // 清理空段
    if idx.segments[target_seg_idx].item_count == 0 && target_seg_idx > 0 {
        idx.segments.remove(target_seg_idx);
    }

    // pre_merge: 若 target_seg_idx 段 item_count < MIN 且有右邻, 合并或借调.
    //
    // 注意: 如果上一段清理空段时把 target_seg_idx 移除了, target_seg_idx 已无效,
    //       不能再访问 idx.segments[target_seg_idx]. 改为基于 idx.segments.len() 判断.
    let effective_seg_idx = if target_seg_idx < idx.segments.len() {
        target_seg_idx
    } else {
        idx.segments.len().saturating_sub(1)
    };
    if effective_seg_idx < idx.segments.len()
        && idx.segments[effective_seg_idx].item_count < MIN_PER_CHECKPOINT
        && effective_seg_idx + 1 < idx.segments.len()
    {
        dprintln!(
            internal,
            "[INTERNAL_DELETE] pre_merge candidate: seg_idx={} item_count={}",
            effective_seg_idx,
            idx.segments[effective_seg_idx].item_count
        );
        let merged =
            crate::index_merge::apply_pre_merge(page, &mut idx, effective_seg_idx, ItemKind::Internal)?;
        dprintln!(internal, "[INTERNAL_DELETE] pre_merge result: {}", merged);
        if !merged {
            // total > MAX: 尝试 steal/borrow
            let stolen = crate::index_merge::apply_pre_merge_steal(
                page,
                &mut idx,
                effective_seg_idx,
                ItemKind::Internal,
            )?;
            dprintln!(
                internal,
                "[INTERNAL_DELETE] pre_merge_steal result: {}",
                stolen
            );
        }
    }

    idx.key_count = page_key_count(page) as usize;
    idx.write_back(page)?;

    Ok(true)
}

/// 分裂 internal page.
///
/// 流程与 `leaf_split` 一致, 但保留 first_child_left / first_child_right.
/// mid 真实 key 的 child_vpid 移到 right (作为 right 的 first_child).
pub fn internal_split(
    left: &mut [u8; PAGE_SIZE],
    right: &mut [u8; PAGE_SIZE],
) -> Result<Vec<u8>, PageError> {
    use crate::dprintln;
    if page_type(left) != PageType::Internal {
        return Err(PageError::InvalidPageType {
            expected: PageType::Internal,
            got: page_type(left),
        });
    }
    let real_keys = page_key_count(left) as usize;
    if real_keys < 2 {
        return Err(PageError::SplitTooFew(real_keys));
    }
    let mid = real_keys / 2;
    dprintln!(
        internal,
        "[INTERNAL_SPLIT] BEGIN real_keys={} mid={} first_child_left={} free_off={}",
        real_keys,
        mid,
        page_vpid(left),
        page_free_off(left)
    );

    // Step 1: 顺序扫描找 split point (mid_off = left last item 末 = k_0009 末)
    let first_child_left = page_vpid(left);
    let mut off = PAGE_HEADER_SIZE;
    let mut prev_key: Vec<u8> = Vec::new();
    let mut mid_off: usize = 0;
    let mut mid_full_key: Vec<u8> = Vec::new();
    let mut mid_child_vpid: u64 = 0;
    let mut mid_n: usize = 0;
    // **重要**: 检测源 page 是否有 sentinel (i=0 item is sentinel iff shared=0, key="", child=0).
    // 如果有 sentinel, loop 用 i==mid 取左半最后, i==mid+1 取右半第一.
    // 如果没有 (例如 right page from previous split), loop 用 i==mid 取右半第一, i==mid-1 取左半最后.
    let (first_item, _first_n) = decode_item(left, PAGE_HEADER_SIZE, ItemKind::Internal)?;
    let has_sentinel = first_item.shared_prefix_len == 0
        && first_item.key_unshared_len == 0
        && first_item.child_vpid == 0;
    if has_sentinel {
        // Loop: i=0=sentinel, i=1..=mid=mid 真实 keys, i=mid+1=mid+1-th 真实 key (right 第一)
        for i in 0..(mid + 2) {
            let (item, n) = decode_item(left, off, ItemKind::Internal)?;
            let full = item.full_key(&prev_key);
            dprintln!(
                internal,
                "[INTERNAL_SPLIT] i={} off={} n={} key={:?} child={}",
                i,
                off,
                n,
                String::from_utf8_lossy(&full),
                item.child_vpid
            );
            if i == mid {
                // left 最后 (i=mid-th 0-indexed = k_{mid-1}): split point
                mid_off = off + n;
            }
            if i == mid + 1 {
                // right 第一 (i=mid+1-th = k_{mid})
                mid_full_key = full;
                mid_child_vpid = item.child_vpid;
                mid_n = n;
                break;
            }
            prev_key = full;
            off += n;
        }
    } else {
        // 无 sentinel: i=0 = 第一个真实 key (k_0), i=mid-1 = 左半最后, i=mid = 右半第一
        // 循环跑到 i=mid+1, 这样能取到 i=mid 的 mid_n.
        for i in 0..(mid + 2) {
            let (item, n) = decode_item(left, off, ItemKind::Internal)?;
            let full = item.full_key(&prev_key);
            dprintln!(
                internal,
                "[INTERNAL_SPLIT] [no-sentinel] i={} off={} n={} key={:?} child={}",
                i,
                off,
                n,
                String::from_utf8_lossy(&full),
                item.child_vpid
            );
            if i == mid - 1 && mid > 0 {
                // 左半最后 (i=mid-1-th)
                mid_off = off + n;
            }
            if i == mid {
                // 右半第一 (i=mid-th)
                mid_full_key = full;
                mid_child_vpid = item.child_vpid;
                mid_n = n;
                break;
            }
            prev_key = full;
            off += n;
        }
    }
    let mut new_prev_key = mid_full_key.clone();
    let left_orig_free = page_free_off(left) as usize;

    // Step 2: 初始化 right
    page_init_header(right, PageType::Internal);
    // 必须设置 checkpoint_count=1, 否则 PageIndex::load 会读成 0 segments.
    write_checkpoint_header(
        right,
        CheckpointHeader {
            checkpoint_count: 1,
            ..Default::default()
        },
    );
    // 显式设置 cp[0] 指向 mid item
    write_checkpoint(
        right,
        0,
        Checkpoint {
            item_count: 1, // 临时
            first_item_off: PAGE_HEADER_SIZE as u16,
        },
    );
    // right 的 first_child = mid_child_vpid (mid 真实 key 的 child)
    crate::header::page_set_vpid(right, mid_child_vpid);

    // Step 3: 写 right
    //     第一个: mid item (shared=0, full key, child=mid_child_vpid)
    //     后续: 从 left[mid_off + mid_n .. orig_free] 重新 prefix-compress
    let mut write_off = PAGE_HEADER_SIZE;
    let mut new_key_count: u16 = 0;

    {
        let mut buf = [0u8; 4096];
        let n = encode_internal_item(&mut buf, &[], &mid_full_key, mid_child_vpid)?;
        right[write_off..write_off + n].copy_from_slice(&buf[..n]);
        write_off += n;
        new_key_count += 1;
    }

    let mut cur = mid_off + mid_n;
    while cur < left_orig_free {
        let (item, n) = decode_item(left, cur, ItemKind::Internal)?;
        let full = item.full_key(&new_prev_key);
        let mut buf = [0u8; 4096];
        let enc_n = encode_internal_item(&mut buf, &new_prev_key, &full, item.child_vpid)?;
        right[write_off..write_off + enc_n].copy_from_slice(&buf[..enc_n]);
        write_off += enc_n;
        new_prev_key = full;
        new_key_count += 1;
        cur += n;
    }

    // Step 4: 截断 left — 清零 mid_off..orig_free 字节, 更新 free_off + key_count
    left[mid_off..left_orig_free].fill(0);
    page_set_free_off(left, mid_off as u16);
    page_set_key_count(left, mid as u16);
    page_set_vpid(left, first_child_left);

    // Step 5: 写回 PageIndex
    // 重要: 先把 left 的 cp header 重置为 0, 避免 PageIndex::load 读到旧的 cp_count 引用已清零区域.
    write_checkpoint_header(left, CheckpointHeader::default());
    let left_kc = page_key_count(left);
    let left_idx = PageIndex::load(left, ItemKind::Internal)?;
    if left_idx.segments.is_empty() && left_kc > 0 {
        // 防御性: 与 leaf_split 一致, load 返回 0 segments 时手动建 cp[0].
        write_checkpoint(
            left,
            0,
            Checkpoint {
                item_count: left_kc + 1, // +1 含哨兵
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
        left_idx.write_back(left)?;
    }

    // 修正 right 的 cp[0].item_count = 实际 item 数 (覆盖临时值 1)
    write_checkpoint(
        right,
        0,
        Checkpoint {
            item_count: new_key_count,
            first_item_off: PAGE_HEADER_SIZE as u16,
        },
    );
    page_set_free_off(right, write_off as u16);
    page_set_key_count(right, new_key_count);
    let right_idx = PageIndex::load(right, ItemKind::Internal)?;
    right_idx.write_back(right)?;

    Ok(mid_full_key)
}

// ===== internal_push_back: 在 ptr 后插入新 separator =====

/// 在 `insert_off` 之后插入 `(sep_key, child_vpid)`.
///
/// - `prev_key`: ptr 指向的 item 的完整 key
/// - `insert_off`: 插入位置 = ptr.off + ptr.total_len()
/// - `seg_idx`: prev_ptr 所在的段 (由调用方传入, 因为 insert_off 可能恰好等于
///   cp[N+1].first_item_off, 此时不能直接用 insert_off 找段)
///
/// 逻辑与 `leaf_push_back` 相同, 区别:
/// - 使用 `encode_internal_item` (无 value, 有 child_vpid)
pub fn internal_push_back(
    page: &mut [u8],
    idx: &mut PageIndex,
    prev_key: &[u8],
    insert_off: usize,
    sep_key: &[u8],
    child_vpid: u64,
    seg_idx: usize,
) -> Result<(), PageError> {
    let free_off_before = page_free_off(page) as usize;

    // Step 1: 编码新 item
    let mut buf_new = [0u8; 4096];
    let new_n = encode_internal_item(&mut buf_new, prev_key, sep_key, child_vpid)?;

    // Step 2: 预解码 k+1
    let k1_off_orig = insert_off;
    let mut k1_buf = [0u8; 4096];
    let mut k1_new_n = 0usize;
    let mut k1_delta = 0isize;

    if k1_off_orig < free_off_before {
        let (k1_item, k1_old_n) = decode_item(page, k1_off_orig, ItemKind::Internal)?;
        if k1_item.shared_prefix_len > 0 {
            let k1_full_key = k1_item.full_key(prev_key);
            k1_new_n =
                encode_internal_item(&mut k1_buf, sep_key, &k1_full_key, k1_item.child_vpid)?;
            k1_delta = k1_new_n as isize - k1_old_n as isize;
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

    // Step 4: 大块后移
    page.copy_within(insert_off..free_off_before, insert_off + new_n);
    page_set_free_off(page, (free_off_before + new_n) as u16);

    // Step 5: 写入新 item
    page[insert_off..insert_off + new_n].copy_from_slice(&buf_new[..new_n]);

    // Step 6: 重写 k+1
    if k1_delta != 0 {
        let k1_off = insert_off + new_n;
        let k1_old_n = (k1_new_n as isize - k1_delta) as usize;
        let current_free = page_free_off(page) as usize;
        page.copy_within(k1_off + k1_old_n..current_free, k1_off + k1_new_n);
        page_set_free_off(page, (current_free as isize + k1_delta) as u16);
        page[k1_off..k1_off + k1_new_n].copy_from_slice(&k1_buf[..k1_new_n]);
    } else if k1_new_n > 0 {
        page[insert_off + new_n..insert_off + new_n + k1_new_n]
            .copy_from_slice(&k1_buf[..k1_new_n]);
    }

    // Step 7: 增量更新 PageIndex
    idx.segments[seg_idx].item_count += 1;
    // total_delta 是 isize, 不能直接 `as u16` (负数会 wrap 到 65535+).
    debug_assert!(
        total_delta >= 0,
        "internal_push_back total_delta must be >= 0, got {}",
        total_delta
    );
    if total_delta < 0 {
        panic!(
            "internal_push_back: unexpected negative total_delta={} (new_n={} k1_delta={})",
            total_delta, new_n, k1_delta
        );
    }
    let _total_delta_u16: u16 = total_delta
        .try_into()
        .expect("internal_push_back: total_delta overflows u16");
    for seg in idx.segments.iter_mut().skip(seg_idx + 1) {
        let new_off = (seg.first_item_off as isize)
            .checked_add(total_delta)
            .and_then(|v| u16::try_from(v).ok())
            .unwrap_or_else(|| {
                panic!(
                    "internal_push_back: first_item_off overflow: old={} delta={}",
                    seg.first_item_off, total_delta
                )
            });
        seg.first_item_off = new_off;
    }
    idx.key_count += 1;

    Ok(())
}

/// 更新 internal page 中 separator key 对应的 child_vpid. 返回 Ok(true) 表示成功, Ok(false) 表示不存在.
///
/// 与 `leaf_update` 对称: 只替换 child_vpid 字节 (固定 8B), shared + key 都不变.
/// - new_child_vpid == old: 就地 8B 覆盖 (最常见, 完全零额外操作)
/// - new_child_vpid != old 但 prev_key 不变 → shared/key 字节不变, 只换 8B
///
/// # 流程
/// 1. locate_segment + 段内 next 找 target item (复用 internal_delete 的扫描方式)
/// 2. 取 prev_key (= target 之前 item 的 full key)
/// 3. 计算 new_n = encode_internal_item(prev_key, key, new_child_vpid)
/// 4. 若 new_n == old_n (固定 13B for normal keys): 就地 8B 替换 child_vpid. 否则需 shift (极少见,
///    只在 shared + key_unshared 字节数变化时发生, 而 separator key 字节不变时 shared/key 字节数也不变).
pub fn internal_update(
    page: &mut [u8],
    key: &[u8],
    new_child_vpid: u64,
) -> Result<bool, PageError> {
    use crate::dprintln;
    use crate::item::decode_item;

    if page_type(page) != PageType::Internal {
        return Err(PageError::InvalidPageType {
            expected: PageType::Internal,
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

    let mut idx = PageIndex::load(page, ItemKind::Internal)?;
    let seg_idx = idx.locate_segment(key);
    let seg = &idx.segments[seg_idx];
    let mut ptr = InternalItemPtr::new(page, seg.first_item_off as usize)?;
    if seg_idx == 0 && ptr.key().is_empty() {
        ptr = match ptr.next() {
            Ok(Some(p)) => p,
            _ => return Ok(false),
        };
    }

    // 顺序扫描找 target
    let mut prev_ptr = ptr.clone();
    loop {
        if ptr.key() == key {
            break;
        }
        let cur = match ptr.next() {
            Ok(Some(p)) => p,
            _ => return Ok(false),
        };
        prev_ptr = ptr;
        ptr = cur;
    }

    let old_off = ptr.byte_offset();
    let old_n = ptr.total_len();
    let prev_key = prev_ptr.key().to_vec();

    // 计算新 item 字节数. prev_key 不变, key 不变, child_vpid 字节固定 8B,
    // 所以 new_n 通常 == old_n (除非 shared/key_unshared 字节数变化, 极少见).
    let mut new_buf = [0u8; 4096];
    let new_n = encode_internal_item(&mut new_buf, &prev_key, key, new_child_vpid)?;
    let free_off_before = page_free_off(page) as usize;

    if new_n == old_n {
        // 快路径: 直接覆盖整个 item 字节 (主要是 8B child_vpid 不同)
        dprintln!(
            internal,
            "[INTERNAL_UPDATE] in-place at off={} new_vpid={}",
            old_off,
            new_child_vpid
        );
        page[old_off..old_off + new_n].copy_from_slice(&new_buf[..new_n]);
        // PageIndex 不变
    } else {
        // 慢路径: new_n != old_n (key 长度跨越 shared 边界), shift + 重写
        let delta = new_n as isize - old_n as isize;
        dprintln!(
            internal,
            "[INTERNAL_UPDATE] shift at off={} old_n={} new_n={} delta={}",
            old_off,
            old_n,
            new_n,
            delta
        );
        page.copy_within(old_off + old_n..free_off_before, old_off + new_n);
        page_set_free_off(page, (free_off_before as isize + delta) as u16);
        page[old_off..old_off + new_n].copy_from_slice(&new_buf[..new_n]);
        // k+1 的 prev_key 不变, shared 不变, 不需重写
        let target_seg_idx = idx.find_segment_by_offset(old_off);
        for seg in idx.segments.iter_mut().skip(target_seg_idx + 1) {
            let new_off = (seg.first_item_off as isize + delta) as u16;
            seg.first_item_off = new_off;
        }
        idx.write_back(page)?;
    }

    // verify: 重新 decode 应得到新 child_vpid
    let (item, _) = decode_item(page, old_off, ItemKind::Internal)?;
    debug_assert_eq!(
        item.child_vpid, new_child_vpid,
        "internal_update: child_vpid mismatch after copy"
    );
    Ok(true)
}
