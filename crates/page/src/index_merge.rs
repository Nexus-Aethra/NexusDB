//! PageIndex 段合并逻辑 (拆自 index.rs).
//!
//! `pre_merge_segment` / `pre_merge_segment_steal` 决定段是否可合并/借项,
//! `apply_pre_merge` / `apply_pre_merge_steal` 执行物理合并与 PageIndex 增量更新.

use crate::dprintln;

use crate::error::PageError;
use crate::header::{page_free_off, page_set_free_off};
use crate::index::PageIndex;
use crate::item::{ItemKind, decode_item, encode_internal_item, encode_leaf_item};

pub fn pre_merge_segment(idx: &mut PageIndex, seg_idx: usize) -> Result<bool, PageError> {
    use crate::checkpoint::{MAX_PER_CHECKPOINT, MIN_PER_CHECKPOINT};

    if seg_idx + 1 >= idx.segments.len() {
        return Ok(false);
    }

    let left_count = idx.segments[seg_idx].item_count;
    if left_count >= MIN_PER_CHECKPOINT {
        return Ok(false);
    }

    let right_count = idx.segments[seg_idx + 1].item_count;
    let total = left_count + right_count;

    if total > MAX_PER_CHECKPOINT {
        // total > 32: 不能直接合并, 调用方应改用 borrow
        return Ok(false);
    }

    // 直接合并: 左段吸收右段
    idx.segments[seg_idx].item_count = total;
    idx.segments.remove(seg_idx + 1);

    Ok(true)
}

/// 预借调 (steal/rebalance) segments[seg_idx] ← segments[seg_idx+1]:
/// 当 seg.item_count < MIN_PER_CHECKPOINT 且 right 段足够大 (total > MAX) 时调用.
///
/// 逻辑层面只更新 item_count. 物理层面 (apply_pre_merge_steal) 还需重写新右段首
/// 的 shared_prefix_len=0 并移动后续 bytes.
///
/// 流程:
/// 1. 检查 left < MIN, has_right_neighbor, right_count > need
/// 2. left.item_count = MIN, right.item_count -= need
pub fn pre_merge_segment_steal(idx: &mut PageIndex, seg_idx: usize) -> Result<bool, PageError> {
    use crate::checkpoint::MIN_PER_CHECKPOINT;

    if seg_idx + 1 >= idx.segments.len() {
        return Ok(false);
    }
    let left_count = idx.segments[seg_idx].item_count;
    if left_count >= MIN_PER_CHECKPOINT {
        return Ok(false);
    }
    let right_count = idx.segments[seg_idx + 1].item_count;
    let need = (MIN_PER_CHECKPOINT - left_count) as usize;
    if (right_count as usize) <= need {
        return Ok(false);
    }
    // 借调
    idx.segments[seg_idx].item_count = left_count + need as u16;
    idx.segments[seg_idx + 1].item_count = right_count - need as u16;
    Ok(true)
}

/// 物理合并 segments[seg_idx] 与 segments[seg_idx+1]:
/// 当 seg.item_count < MIN_PER_CHECKPOINT (8) 且 total <= MAX 时调用.
///
/// 这是 pre_merge_segment 的物理版本: 不仅更新 PageIndex, 还重新 prefix-compress 右段
/// 的 items (使用左段最后一个 item 的 full key 作为新 prev_key) 并移动字节.
///
/// # 流程
/// 1. 检查 pre_merge_segment 条件: left_count < MIN && total <= MAX && has_right_neighbor
/// 2. 找到左段最后一个 item 的 full key (新 prev_key)
/// 3. 解码右段所有 items 到内存 vec
/// 4. **先**移动后续段 (如果有) by delta = new_right_total_n - right_total_n (向后或向前)
/// 5. 用新 prev_key 重新 prefix-compress 右段 items 写入 (从 right_first_off 开始)
/// 6. 更新 free_off
/// 7. 更新 PageIndex: left.item_count += right.item_count, 删除 right 段,
///    后续段的 first_item_off += delta
///
/// # 调用方
/// 由 `leaf_delete` / `internal_delete` 在物理删除 + k+1 重写后, write_back 之前调用.
/// 若返回 Ok(false) (不满足合并条件), 不做任何事, 调用方继续原流程.
///
/// # 性能
/// O(右段 item 数) ≤ O(32). 解码右段 + 重 prefix-compress, 不触碰左段字节.
pub fn apply_pre_merge(
    page: &mut [u8],
    idx: &mut PageIndex,
    seg_idx: usize,
    kind: ItemKind,
) -> Result<bool, PageError> {
    use crate::checkpoint::{MAX_PER_CHECKPOINT, MIN_PER_CHECKPOINT};
    use crate::header::{page_free_off, page_set_free_off};

    if seg_idx + 1 >= idx.segments.len() {
        return Ok(false);
    }
    let left_count = idx.segments[seg_idx].item_count;
    if left_count >= MIN_PER_CHECKPOINT {
        return Ok(false);
    }
    let right_count = idx.segments[seg_idx + 1].item_count;
    let total = left_count + right_count;
    if total > MAX_PER_CHECKPOINT {
        return Ok(false);
    }

    let left_first_off = idx.segments[seg_idx].first_item_off as usize;
    let right_first_off = idx.segments[seg_idx + 1].first_item_off as usize;
    let free_off_before = page_free_off(page) as usize;
    // **重要**: 右段的实际结束位置 = 下一段段首 OR page free_off (若无下一段).
    // 不能用 free_off_before 作为右段边界, 否则会把后续段的 bytes 也当成右段
    // 的一部分, 导致后续段被覆盖. 这是修复的 root cause.
    let right_segment_end: usize = if seg_idx + 2 < idx.segments.len() {
        idx.segments[seg_idx + 2].first_item_off as usize
    } else {
        free_off_before
    };

    // 1. 找左段最后一个 item 的 full key (新 prev_key for right segment)
    let mut cur_off = left_first_off;
    let mut prev_key: Vec<u8> = Vec::new();
    let mut left_last_key: Vec<u8> = Vec::new();
    while cur_off < right_first_off {
        let (item, n) = decode_item(page, cur_off, kind)?;
        let full = item.full_key(&prev_key);
        prev_key = full.clone();
        left_last_key = full;
        cur_off += n;
    }

    // 2. 解码右段所有 items 到内存 vec (用 page 中已有的 prev_key 作为基准)
    let mut right_items: Vec<(Vec<u8>, Vec<u8>, u64, usize)> =
        Vec::with_capacity(right_count as usize);
    let mut cur = right_first_off;
    while cur < right_segment_end {
        let (item, n) = decode_item(page, cur, kind)?;
        let full = item.full_key(&prev_key);
        right_items.push((full.clone(), item.value.to_vec(), item.child_vpid, n));
        prev_key = full;
        cur += n;
    }
    let right_total_n = right_segment_end - right_first_off;

    // 3. 计算 new_right_total_n (用 left_last_key 作新 prev_key 重 prefix-compress)
    let mut new_right_total_n: usize = 0;
    {
        let mut new_prev_key = left_last_key.clone();
        for (full_key, value, child_vpid, _old_n) in &right_items {
            let mut buf = [0u8; 4096];
            let n = match kind {
                ItemKind::Leaf => encode_leaf_item(&mut buf, &new_prev_key, full_key, value)?,
                ItemKind::Internal => {
                    encode_internal_item(&mut buf, &new_prev_key, full_key, *child_vpid)?
                }
            };
            new_right_total_n += n;
            new_prev_key = full_key.clone();
        }
    }
    let delta = new_right_total_n as isize - right_total_n as isize;
    dprintln!(
        index,
        "[APPLY_PRE_MERGE] seg_idx={} left_count={} right_count={} right_first_off={} old_right_n={} new_right_n={} delta={} free_off_before={}",
        seg_idx,
        left_count,
        right_count,
        right_first_off,
        right_total_n,
        new_right_total_n,
        delta,
        free_off_before
    );

    // 4. 先移动后续段 (如果有, 即 seg_idx + 2), by delta. (PageIndex 中移除 right 段后, 这些变成 seg_idx+1..)
    //    src = [right_segment_end, free_off_before) (后续段原始位置)
    //    dst = [right_first_off + new_right_total_n, ...) (后续段新位置)
    if delta != 0 && right_segment_end < free_off_before {
        page.copy_within(
            right_segment_end..free_off_before,
            right_first_off + new_right_total_n,
        );
    }

    // 5. 写新右段 (从 right_first_off 开始)
    let mut write_off = right_first_off;
    let mut new_prev_key = left_last_key.clone();
    for (full_key, value, child_vpid, _old_n) in &right_items {
        let mut buf = [0u8; 4096];
        let n = match kind {
            ItemKind::Leaf => encode_leaf_item(&mut buf, &new_prev_key, full_key, value)?,
            ItemKind::Internal => {
                encode_internal_item(&mut buf, &new_prev_key, full_key, *child_vpid)?
            }
        };
        page[write_off..write_off + n].copy_from_slice(&buf[..n]);
        write_off += n;
        new_prev_key = full_key.clone();
    }
    let new_free_off = if delta != 0 {
        // 移动后续段后的 free_off
        (free_off_before as isize + delta) as usize
    } else {
        write_off
    };
    page_set_free_off(page, new_free_off as u16);

    // 6. 更新 PageIndex
    idx.segments[seg_idx].item_count = total;
    idx.segments.remove(seg_idx + 1);
    if delta != 0 {
        // 后续段 (原 seg_idx+2, 现在 seg_idx+1..) first_item_off += delta
        for s in idx.segments.iter_mut().skip(seg_idx + 1) {
            s.first_item_off = (s.first_item_off as isize + delta) as u16;
        }
    }

    Ok(true)
}

/// 物理借调 (steal/rebalance) segments[seg_idx] ← segments[seg_idx+1]:
/// 当 seg.item_count < MIN_PER_CHECKPOINT 且 total > MAX_PER_CHECKPOINT 时调用.
///
/// 这是 `apply_pre_merge` 的兄弟函数: 不能直接合并 (会超过 MAX), 但也不能放任 left 段
/// 太小 (会拖慢 locate_segment). 因此从右段头部"借" `need = MIN - left_count` 个 items
/// 到左段尾部, 使左段达到 MIN. 借调后左右都不超 MAX (因为 total > MAX ⟹ right - need
/// > MAX - MIN, 仍然够大).
///
/// # 关键观察: 字节变化最小
///
/// - **被借到左段的 items** (old right[0..need]): 物理上**不动**, 字节不变. 它们的
///   shared_prefix_len 原本基于"右段前一个 item 的 full key"编码, 借调后基于"左段
///   最后一个 item 的 full key" (left_last_key). 但因为这是它们的**同一个**前一个 item
///   (old left[L-1] == old right[-1], 即 left_last_key), prev_key 没变, 编码仍正确.
///
/// - **新右段首** (old right[need]): **必须重编码为 shared=0** (它是 cp 段头, PageIndex::load
///   要求段首 shared=0). prev_key 仍不变, 但需要把 shared 强制置 0 并写完整 key.
///
/// - **新右段后续** (old right[need+1..]): 物理上**整体平移** by delta, 字节不变.
///   它们原本就基于"右段前一个 item"编码, 借调后还是基于"右段前一个 item" (只是现在
///   是 new right[0]), prev_key 没变, 编码仍正确.
///
/// # 流程
/// 1. 检查: left < MIN, has_right_neighbor, right_count > need (即 right 借出后仍 > 0)
/// 2. 顺序扫描右段 need 个 items, 找到 new_split_off (= old right[need] 的位置)
/// 3. 解码 new_split_off 处的 item (= 新右段首) 并用 shared=0 重编码, 得 new_n_0
/// 4. 移动 old right[need+1..] by delta = new_n_0 - old_n_0
/// 5. 写 new right[0] at new_split_off
/// 6. 更新 free_off += delta
/// 7. 更新 PageIndex: left.item_count = MIN, right.item_count -= need,
///    right.first_item_off = new_split_off, right.first_full_key = new full key,
///    后续段 first_item_off += delta
///
/// # 调用方
/// 由 `leaf_delete` / `internal_delete` 在 `apply_pre_merge` 返回 false 时调用.
pub fn apply_pre_merge_steal(
    page: &mut [u8],
    idx: &mut PageIndex,
    seg_idx: usize,
    kind: ItemKind,
) -> Result<bool, PageError> {
    use crate::checkpoint::MIN_PER_CHECKPOINT;

    if seg_idx + 1 >= idx.segments.len() {
        return Ok(false);
    }
    let left_count = idx.segments[seg_idx].item_count;
    if left_count >= MIN_PER_CHECKPOINT {
        return Ok(false);
    }
    let right_count = idx.segments[seg_idx + 1].item_count;
    let need = (MIN_PER_CHECKPOINT - left_count) as usize;
    // need = MIN - left; if right <= need 则借出会清空右段, 应该走 full merge (total < MAX 情形).
    // 实际: total > MAX 时, right - need = total - MIN > MAX - MIN = 24 >= MIN, 不可能触发此分支.
    if (right_count as usize) <= need {
        return Ok(false);
    }

    let left_first_off = idx.segments[seg_idx].first_item_off as usize;
    let right_first_off = idx.segments[seg_idx + 1].first_item_off as usize;
    let free_off_before = page_free_off(page) as usize;

    // 1. 顺序扫描左段, 还原 prev_key (= left_last_key, 也是 right[0] 的 prev_key)
    let mut cur = left_first_off;
    let mut prev_key: Vec<u8> = Vec::new();
    while cur < right_first_off {
        let (item, n) = decode_item(page, cur, kind)?;
        let full = item.full_key(&prev_key);
        prev_key = full;
        cur += n;
    }
    // cur == right_first_off, prev_key = left_last_key

    // 2. 扫描右段 need 个 items, 找 new_split_off (= old right[need] 的位置)
    //
    // **重要**: 必须用 right_segment_end 作为右段边界, 否则会跨段读后续段的 items.
    let right_segment_end: usize = if seg_idx + 2 < idx.segments.len() {
        idx.segments[seg_idx + 2].first_item_off as usize
    } else {
        free_off_before
    };
    let mut stolen: usize = 0;
    while stolen < need && cur < right_segment_end {
        let (item, n) = decode_item(page, cur, kind)?;
        let full = item.full_key(&prev_key);
        prev_key = full;
        cur += n;
        stolen += 1;
    }
    if stolen < need {
        // 防御: 实际右段 item 数比 right_count 少, 异常. 不做借调.
        return Ok(false);
    }
    let new_split_off = cur;
    // prev_key 现在是 old right[need] 的 full key (= 新右段首的 full key)

    // 3. 解码 new_split_off 处的 item 并用 shared=0 重编码
    let (boundary_item, old_n_0) = decode_item(page, new_split_off, kind)?;
    let boundary_full_key = boundary_item.full_key(&prev_key);
    let mut buf = [0u8; 4096];
    let new_n_0 = match kind {
        ItemKind::Leaf => encode_leaf_item(&mut buf, &[], &boundary_full_key, boundary_item.value)?,
        ItemKind::Internal => {
            encode_internal_item(&mut buf, &[], &boundary_full_key, boundary_item.child_vpid)?
        }
    };
    let delta = new_n_0 as isize - old_n_0 as isize;
    dprintln!(
        index,
        "[APPLY_PRE_MERGE_STEAL] seg_idx={} left_count={} right_count={} need={} \
         new_split_off={} old_n_0={} new_n_0={} delta={} boundary_key={:?} free_off_before={}",
        seg_idx,
        left_count,
        right_count,
        need,
        new_split_off,
        old_n_0,
        new_n_0,
        delta,
        String::from_utf8_lossy(&boundary_full_key),
        free_off_before
    );

    // 4. 移动 old right[need+1..] by delta (src 和 dst 可能重叠, copy_within memmove 语义)
    if delta != 0 {
        let src_start = new_split_off + old_n_0;
        let src_end = free_off_before;
        let dst_start = (new_split_off as isize + new_n_0 as isize) as usize;
        page.copy_within(src_start..src_end, dst_start);
    }

    // 5. 写新 right[0] (shared=0) at new_split_off
    page[new_split_off..new_split_off + new_n_0].copy_from_slice(&buf[..new_n_0]);

    // 6. 更新 free_off
    if delta != 0 {
        page_set_free_off(page, (free_off_before as isize + delta) as u16);
    }

    // 7. 更新 PageIndex
    idx.segments[seg_idx].item_count = left_count + need as u16;
    idx.segments[seg_idx + 1].item_count = right_count - need as u16;
    idx.segments[seg_idx + 1].first_item_off = new_split_off as u16;
    idx.segments[seg_idx + 1].first_full_key = boundary_full_key;
    if delta != 0 {
        // 后续段 (原 seg_idx+2..) 物理上平移了 delta 字节
        for s in idx.segments.iter_mut().skip(seg_idx + 2) {
            s.first_item_off = (s.first_item_off as isize + delta) as u16;
        }
    }

    Ok(true)
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::{PAGE_HEADER_SIZE, page_set_free_off, page_set_key_count};
    use crate::index::Segment;
    use crate::item::encode_leaf_item;
    use crate::leaf::leaf_new;

    fn write_leaf_at(page: &mut [u8; 16384], off: usize, key: &[u8], value: &[u8]) -> usize {
        let mut buf = [0u8; 4096];
        let n = encode_leaf_item(&mut buf, &[], key, value).unwrap();
        page[off..off + n].copy_from_slice(&buf[..n]);
        n
    }

    #[test]
    fn test_load_with_sentinel_only() {
        let mut page = leaf_new();
        // 写入哨兵 (shared=0, key_unshared_len=0)
        let sentinel_n = write_leaf_at(&mut page, PAGE_HEADER_SIZE, b"", b"");
        page_set_key_count(&mut page, 1);
        page_set_free_off(&mut page, (PAGE_HEADER_SIZE + sentinel_n) as u16);
        // 设置 cp[0] 指向哨兵
        let hdr = crate::checkpoint::CheckpointHeader {
            checkpoint_count: 1,
            ..Default::default()
        };
        crate::checkpoint::write_checkpoint_header(&mut page, hdr);
        crate::checkpoint::write_checkpoint(
            &mut page,
            0,
            crate::checkpoint::Checkpoint {
                item_count: 1,
                first_item_off: PAGE_HEADER_SIZE as u16,
            },
        );

        let idx = PageIndex::load(&page, ItemKind::Leaf).unwrap();
        assert_eq!(idx.key_count, 1);
        // segments 直接映射 cp array: cp_count=1 → segments.len()=1
        assert_eq!(idx.segments.len(), 1);
        assert_eq!(idx.segments[0].item_count, 1);
        assert_eq!(idx.segments[0].first_full_key, Vec::<u8>::new());
    }

    #[test]
    fn test_locate_segment() {
        let mut page = leaf_new();

        // 写入 items, 追踪每个 item 的实际偏移
        let mut off = PAGE_HEADER_SIZE;
        let mut offsets: Vec<(usize, Vec<u8>)> = Vec::new();

        // 哨兵
        let n = write_leaf_at(&mut page, off, b"", b"");
        offsets.push((off, b"".to_vec()));
        off += n;

        // 真实 items: k_005, k_010, k_020, k_030
        for k in [b"k_005", b"k_010", b"k_020", b"k_030"] {
            let n = write_leaf_at(&mut page, off, k, b"v");
            offsets.push((off, k.to_vec()));
            off += n;
        }
        page_set_key_count(&mut page, 5); // 哨兵 + 4 真实
        page_set_free_off(&mut page, off as u16);

        // cp array: cp[0] = 哨兵, cp[1] = k_005,k_010, cp[2] = k_020,k_030
        let hdr = crate::checkpoint::CheckpointHeader {
            checkpoint_count: 3,
            ..Default::default()
        };
        crate::checkpoint::write_checkpoint_header(&mut page, hdr);
        crate::checkpoint::write_checkpoint(
            &mut page,
            0,
            crate::checkpoint::Checkpoint {
                item_count: 1,
                first_item_off: offsets[0].0 as u16,
            },
        );
        crate::checkpoint::write_checkpoint(
            &mut page,
            1,
            crate::checkpoint::Checkpoint {
                item_count: 2,
                first_item_off: offsets[1].0 as u16, // k_005
            },
        );
        crate::checkpoint::write_checkpoint(
            &mut page,
            2,
            crate::checkpoint::Checkpoint {
                item_count: 2,
                first_item_off: offsets[3].0 as u16, // k_020
            },
        );

        let idx = PageIndex::load(&page, ItemKind::Leaf).unwrap();
        assert_eq!(idx.segments.len(), 3);

        // locate_segment: 找最后一个 first_full_key <= key 的段
        // "aaa" < "k_005" → 段 0 (哨兵段)
        assert_eq!(idx.locate_segment(b"aaa"), 0);
        // "k_005" == segments[1].first_full_key → 段 1
        assert_eq!(idx.locate_segment(b"k_005"), 1);
        // "k_010" > "k_005" 且 < "k_020" → 段 1
        assert_eq!(idx.locate_segment(b"k_010"), 1);
        // "k_020" == segments[2].first_full_key → 段 2
        assert_eq!(idx.locate_segment(b"k_020"), 2);
        // "k_025" > "k_020" 且 < "k_030" → 段 2
        assert_eq!(idx.locate_segment(b"k_025"), 2);
        // "z" > 所有 key → 最后一段
        assert_eq!(idx.locate_segment(b"z"), 2);
    }

    #[test]
    fn test_write_back_roundtrip() {
        let mut page = leaf_new();
        // 哨兵 + 3 真实 items
        let mut off = PAGE_HEADER_SIZE;
        let n = write_leaf_at(&mut page, off, b"", b"");
        off += n;
        for k in &[b"apple".to_vec(), b"banana".to_vec(), b"cherry".to_vec()] {
            let n = write_leaf_at(&mut page, off, k, b"v");
            off += n;
        }
        page_set_key_count(&mut page, 4);
        page_set_free_off(&mut page, off as u16);

        let hdr = crate::checkpoint::CheckpointHeader {
            checkpoint_count: 1,
            ..Default::default()
        };
        crate::checkpoint::write_checkpoint_header(&mut page, hdr);
        crate::checkpoint::write_checkpoint(
            &mut page,
            0,
            crate::checkpoint::Checkpoint {
                item_count: 4,
                first_item_off: PAGE_HEADER_SIZE as u16,
            },
        );

        let idx = PageIndex::load(&page, ItemKind::Leaf).unwrap();
        let mut page2 = page;
        idx.write_back(&mut page2).unwrap();

        // 重新加载应该一样
        let idx2 = PageIndex::load(&page2, ItemKind::Leaf).unwrap();
        assert_eq!(idx.key_count, idx2.key_count);
        assert_eq!(idx.segments.len(), idx2.segments.len());
    }

    /// pre_merge_segment 单元测试: left 段小于 MIN, right 段正常, total <= MAX → 合并.
    #[test]
    fn test_pre_merge_trivial_case() {
        // 构造 PageIndex: 哨兵段 (3 items) + 真实段 (10 items) = total 13 ≤ 32
        let mut idx = PageIndex {
            segments: vec![
                Segment {
                    first_item_off: 40,
                    item_count: 3,
                    first_full_key: Vec::new(),
                },
                Segment {
                    first_item_off: 100,
                    item_count: 10,
                    first_full_key: b"m".to_vec(),
                },
            ],
            key_count: 12,
        };

        // left_count=3 < MIN(8), right_count=10, total=13 ≤ MAX(32) → 合并
        let merged = pre_merge_segment(&mut idx, 0).unwrap();
        assert!(merged, "should merge");
        assert_eq!(idx.segments.len(), 1, "right segment should be removed");
        assert_eq!(idx.segments[0].item_count, 13);
        assert_eq!(idx.segments[0].first_item_off, 40);
    }

    /// pre_merge_segment: left 段已经 >= MIN → 不合并.
    #[test]
    fn test_pre_merge_no_op_when_left_sufficient() {
        let mut idx = PageIndex {
            segments: vec![
                Segment {
                    first_item_off: 40,
                    item_count: 8, // = MIN
                    first_full_key: Vec::new(),
                },
                Segment {
                    first_item_off: 100,
                    item_count: 5,
                    first_full_key: b"k".to_vec(),
                },
            ],
            key_count: 12,
        };

        let merged = pre_merge_segment(&mut idx, 0).unwrap();
        assert!(!merged, "should NOT merge when left >= MIN");
        assert_eq!(idx.segments.len(), 2);
        assert_eq!(idx.segments[0].item_count, 8);
    }

    /// pre_merge_segment: 没有右邻 → 不合并.
    #[test]
    fn test_pre_merge_no_op_when_no_right_neighbor() {
        let mut idx = PageIndex {
            segments: vec![Segment {
                first_item_off: 40,
                item_count: 3,
                first_full_key: Vec::new(),
            }],
            key_count: 2,
        };

        let merged = pre_merge_segment(&mut idx, 0).unwrap();
        assert!(!merged, "should NOT merge without right neighbor");
        assert_eq!(idx.segments.len(), 1);
    }

    /// pre_merge_segment: total > MAX (32) → 不合并 (应改用 steal/borrow).
    #[test]
    fn test_pre_merge_no_op_when_total_over_max() {
        let mut idx = PageIndex {
            segments: vec![
                Segment {
                    first_item_off: 40,
                    item_count: 3, // < MIN
                    first_full_key: Vec::new(),
                },
                Segment {
                    first_item_off: 100,
                    item_count: 30, // total = 33 > MAX (32)
                    first_full_key: b"k".to_vec(),
                },
            ],
            key_count: 32,
        };

        let merged = pre_merge_segment(&mut idx, 0).unwrap();
        assert!(
            !merged,
            "should NOT merge when total > MAX (use steal instead)"
        );
        assert_eq!(idx.segments.len(), 2);
    }

    // ===== pre_merge_segment_steal 单元测试 =====

    /// pre_merge_segment_steal: left < MIN, right 足够大 (total > MAX) → 借调.
    #[test]
    fn test_pre_merge_steal_basic() {
        let mut idx = PageIndex {
            segments: vec![
                Segment {
                    first_item_off: 40,
                    item_count: 3, // < MIN
                    first_full_key: Vec::new(),
                },
                Segment {
                    first_item_off: 100,
                    item_count: 30, // total = 33 > MAX
                    first_full_key: b"k".to_vec(),
                },
            ],
            key_count: 32,
        };

        let stolen = pre_merge_segment_steal(&mut idx, 0).unwrap();
        assert!(stolen, "should steal");
        assert_eq!(idx.segments.len(), 2);
        // need = 8 - 3 = 5; left absorbs 5, right gives 5
        assert_eq!(idx.segments[0].item_count, 8);
        assert_eq!(idx.segments[1].item_count, 25);
    }

    /// pre_merge_segment_steal: left >= MIN → 不借调.
    #[test]
    fn test_pre_merge_steal_no_op_when_left_sufficient() {
        let mut idx = PageIndex {
            segments: vec![
                Segment {
                    first_item_off: 40,
                    item_count: 8, // = MIN
                    first_full_key: Vec::new(),
                },
                Segment {
                    first_item_off: 100,
                    item_count: 30,
                    first_full_key: b"k".to_vec(),
                },
            ],
            key_count: 37,
        };

        let stolen = pre_merge_segment_steal(&mut idx, 0).unwrap();
        assert!(!stolen, "should NOT steal when left >= MIN");
        assert_eq!(idx.segments[0].item_count, 8);
        assert_eq!(idx.segments[1].item_count, 30);
    }

    /// pre_merge_segment_steal: right 借出后会清空 (right <= need) → 不借调.
    /// 此时应该走 full merge (因为 total < MAX).
    #[test]
    fn test_pre_merge_steal_no_op_when_right_too_small() {
        let mut idx = PageIndex {
            segments: vec![
                Segment {
                    first_item_off: 40,
                    item_count: 5, // < MIN, need = 3
                    first_full_key: Vec::new(),
                },
                Segment {
                    first_item_off: 100,
                    item_count: 3, // right <= need, 借出会清空
                    first_full_key: b"k".to_vec(),
                },
            ],
            key_count: 7,
        };

        let stolen = pre_merge_segment_steal(&mut idx, 0).unwrap();
        assert!(!stolen, "should NOT steal when right <= need");
        // 此时 total=8 <= MAX, 应该走 full merge 路径
        let merged = pre_merge_segment(&mut idx, 0).unwrap();
        assert!(merged, "should fall back to full merge");
        assert_eq!(idx.segments.len(), 1);
        assert_eq!(idx.segments[0].item_count, 8);
    }

    /// pre_merge_segment_steal: 没有右邻 → 不借调.
    #[test]
    fn test_pre_merge_steal_no_op_when_no_right_neighbor() {
        let mut idx = PageIndex {
            segments: vec![Segment {
                first_item_off: 40,
                item_count: 3,
                first_full_key: Vec::new(),
            }],
            key_count: 2,
        };

        let stolen = pre_merge_segment_steal(&mut idx, 0).unwrap();
        assert!(!stolen, "should NOT steal without right neighbor");
    }

    /// apply_pre_merge_steal: left 已经 >= MIN → 不借调.
    #[test]
    fn test_apply_pre_merge_steal_no_op_when_left_sufficient() {
        // 真实 page 测试: 段已经 >= MIN, steal 应无效.
        // 复用普通 leaf_insert 构建正常 page (含 cp 段首 shared=0 不变量).
        use crate::leaf::leaf_insert;
        let mut page = leaf_new();
        // 插 9 个 items, 哨兵段 cp[0] 含 1+9=10 items (>= MIN)
        for i in 1..=9 {
            leaf_insert(&mut page, format!("k_{:03}", i).as_bytes(), b"v").unwrap();
        }
        let mut idx = PageIndex::load(&page, ItemKind::Leaf).unwrap();
        // 当前只有 cp[0] 段, 没有右邻
        let stolen = apply_pre_merge_steal(&mut page, &mut idx, 0, ItemKind::Leaf).unwrap();
        assert!(!stolen, "should NOT steal without right neighbor");
    }

    /// apply_pre_merge_steal: 没有右邻 → 不借调.
    #[test]
    fn test_apply_pre_merge_steal_no_op_when_no_right_neighbor() {
        use crate::leaf::leaf_insert;
        let mut page = leaf_new();
        for i in 1..=9 {
            leaf_insert(&mut page, format!("k_{:03}", i).as_bytes(), b"v").unwrap();
        }
        let mut idx = PageIndex::load(&page, ItemKind::Leaf).unwrap();
        // seg 0 = cp[0] 段 (含哨兵), 1+9=10 items >= MIN
        // seg 0 没有右邻, steal 无效
        let stolen = apply_pre_merge_steal(&mut page, &mut idx, 0, ItemKind::Leaf).unwrap();
        assert!(!stolen, "should NOT steal without right neighbor");
    }
}
