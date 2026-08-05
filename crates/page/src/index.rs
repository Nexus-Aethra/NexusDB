//! PageIndex: 内存中的 cp array 镜像, 提供 O(log segments) 二分定位.
//!
//! ## 设计
//!
//! PageIndex 是从 page 字节构建的轻量快照, 持有:
//! - segments: Vec<Segment>, 每段 = { first_item_off, item_count, first_full_key }
//!
//! **关键**: cp[0] 段的第一个 item 是哨兵 (shared=0, key_unshared_len=0).
//! cp[0].item_count 包含哨兵 + 真实 items.
//! cp[0].first_item_off = PAGE_HEADER_SIZE (哨兵位置).
//!
//! segments[0] = cp[0] 段 (包含哨兵 + 真实 items, first_full_key = 空)
//! segments[1..] = cp[1..] 段 (真实 items)

use crate::dprintln;

use crate::error::PageError;
use crate::header::{page_free_off, page_key_count, page_set_free_off};
use crate::item::{ItemKind, decode_item, encode_internal_item, encode_leaf_item};

#[derive(Clone, Debug)]
pub struct Segment {
    /// 段首 item 在 page 中的字节偏移
    pub first_item_off: u16,
    /// 段内 item 数 (含哨兵 if 第一段)
    pub item_count: u16,
    /// 段首 item 的完整 key (cached)
    pub first_full_key: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct PageIndex {
    /// segments[0] = cp[0] (包含哨兵 + 真实 items, first_full_key = 空)
    /// segments[1..] = cp[1..] (真实 items)
    pub segments: Vec<Segment>,
    pub key_count: usize,
}

impl PageIndex {
    /// 从 page 字节加载 PageIndex.
    ///
    /// 假设 page 已经包含哨兵 (key_count >= 1).
    /// cp[0] 段包含哨兵 + 真实 items.
    #[allow(clippy::needless_borrow)] // 显式 `&Vec<u8>` 在 String::from_utf8_lossy 调用更清晰
    pub fn load(page: &[u8], kind: ItemKind) -> Result<Self, PageError> {
        let key_count = page_key_count(page) as usize;
        let (hdr, _) = crate::checkpoint::read_checkpoint_header(page);
        let cp_count = hdr.checkpoint_count as usize;
        dprintln!(
            index,
            "[PAGE_INDEX_LOAD] BEGIN key_count={} cp_count={} free_off={}",
            key_count,
            cp_count,
            page_free_off(page)
        );

        let mut segments = Vec::with_capacity(cp_count);

        for i in 0..cp_count {
            let cp = crate::checkpoint::read_checkpoint(page, i);
            // decode 段首 item (必然 shared=0)
            dprintln!(
                index,
                "[PAGE_INDEX_LOAD] decoding cp[{}] at off={} (free_off={})",
                i,
                cp.first_item_off,
                page_free_off(page)
            );
            let (item, _) = match decode_item(page, cp.first_item_off as usize, kind) {
                Ok(v) => v,
                Err(e) => {
                    dprintln!(
                        index,
                        "[PAGE_INDEX_LOAD] cp[{}] decode FAILED at off={} free_off={}: {}",
                        i,
                        cp.first_item_off,
                        page_free_off(page),
                        e
                    );
                    return Err(e);
                }
            };
            dprintln!(
                index,
                "[PAGE_INDEX_LOAD] cp[{}] first_item_off={} item_count={} shared={} key_unshared_len={} key={:?}",
                i,
                cp.first_item_off,
                cp.item_count,
                item.shared_prefix_len,
                item.key_unshared_len,
                String::from_utf8_lossy(&item.key_unshared)
            );
            if item.shared_prefix_len != 0 {
                // 出错时 dump 段首 item 周围 32B 原始字节, 帮助定位 cp[] 指向非 item 边界的情况
                let raw_off = cp.first_item_off as usize;
                let raw_end = (raw_off + 32).min(page.len());
                dprintln!(
                    index,
                    "[PAGE_INDEX_LOAD] cp[{}] BAD shared={} at off={}, raw bytes[{}..{}]: {:02X?}",
                    i,
                    item.shared_prefix_len,
                    raw_off,
                    raw_off,
                    raw_end,
                    &page[raw_off..raw_end]
                );
                return Err(PageError::ItemDecode(format!(
                    "cp[{}] segment head item must have shared=0, got shared={} at off={} (key={:?})",
                    i,
                    item.shared_prefix_len,
                    cp.first_item_off,
                    String::from_utf8_lossy(&item.key_unshared)
                )));
            }
            // 调试: 遍历该段内所有 item, 验证 keys
            if i < 4 {
                let mut off = cp.first_item_off as usize;
                let mut prev_key: Vec<u8> = Vec::new();
                for j in 0..cp.item_count {
                    let (it, n) = match decode_item(page, off, kind) {
                        Ok(v) => v,
                        Err(e) => {
                            dprintln!(
                                index,
                                "[PAGE_INDEX_LOAD] cp[{}].item[{}] decode FAILED at off={}: {}",
                                i, j, off, e
                            );
                            return Err(e);
                        }
                    };
                    let full = it.full_key(&prev_key);
                    dprintln!(
                        index,
                        "[PAGE_INDEX_LOAD]   cp[{}].item[{}] off={} key={:?} value={:?}",
                        i,
                        j,
                        off,
                        String::from_utf8_lossy(&full),
                        std::str::from_utf8(it.value).unwrap_or("?")
                    );
                    prev_key = full;
                    off += n;
                }
            }

            segments.push(Segment {
                first_item_off: cp.first_item_off,
                item_count: cp.item_count,
                first_full_key: item.key_unshared.to_vec(),
            });
        }

        Ok(Self {
            segments,
            key_count,
        })
    }

    /// 把 PageIndex 写回 page (cp array + header.key_count).
    pub fn write_back(&self, page: &mut [u8]) -> Result<(), PageError> {
        // 写 header.key_count
        crate::header::page_set_key_count(page, self.key_count as u16);

        // 写 cp header
        let cp_count = self.segments.len();
        dprintln!(index, "[WRITE_BACK] writing {} cp segments", cp_count);
        let hdr = crate::checkpoint::CheckpointHeader {
            checkpoint_count: cp_count as u16,
            ..Default::default()
        };
        crate::checkpoint::write_checkpoint_header(page, hdr);

        // 写每个 cp segment
        for (i, seg) in self.segments.iter().enumerate() {
            dprintln!(
                index,
                "[WRITE_BACK]   cp[{}] item_count={} first_off={}",
                i,
                seg.item_count,
                seg.first_item_off
            );
            crate::checkpoint::write_checkpoint(
                page,
                i,
                crate::checkpoint::Checkpoint {
                    item_count: seg.item_count,
                    first_item_off: seg.first_item_off,
                },
            );
        }

        Ok(())
    }

    /// 二分定位 key 应该插入的段. 返回段 idx (0-based).
    ///
    /// 语义: 找最后一个 first_full_key <= key 的段.
    /// 如果 key < segments[0].first_full_key (空), 返回 0.
    /// 如果 key >= 所有段的 first_full_key, 返回最后一个段.
    pub fn locate_segment(&self, key: &[u8]) -> usize {
        // `partition_point` would express this directly, but keeping the
        // search explicit avoids a version-dependent std API in this hot path.
        // `lo` ends one past the last segment whose first key is <= `key`.
        let mut lo = 0usize;
        let mut hi = self.segments.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.segments[mid].first_full_key.as_slice() <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo.saturating_sub(1)
    }

    /// 哨兵段引用 (segments[0])
    pub fn sentinel_segment(&self) -> &Segment {
        &self.segments[0]
    }

    /// 真实 cp 段 (segments[1..])
    pub fn real_segments(&self) -> &[Segment] {
        if self.segments.len() > 1 {
            &self.segments[1..]
        } else {
            &[]
        }
    }

    /// 真实 cp 段数 (不含哨兵段)
    pub fn real_segment_count(&self) -> usize {
        if !self.segments.is_empty() {
            self.segments.len() - 1
        } else {
            0
        }
    }

    /// 根据字节偏移定位所在的段 idx.
    ///
    /// 找最后一个 `first_item_off <= off` 的段.
    /// 确保 off 落在该段的 item 范围内.
    pub fn find_segment_by_offset(&self, off: usize) -> usize {
        let mut best = 0usize;
        for i in 0..self.segments.len() {
            if (self.segments[i].first_item_off as usize) <= off {
                best = i;
            } else {
                break;
            }
        }
        best
    }
}

// ===== pre_split / pre_merge (基于 PageIndex 的段分裂与合并) =====

/// 预分裂 segments[seg_idx]: 当 item_count >= MAX_PER_CHECKPOINT (32) 时,
/// 将段对半分裂为两个段.
///
/// 流程:
/// 1. 找到段的 mid item, 将其重编码为 shared=0 (full key)
/// 2. 前一半保留在原段, 后一半放入新段
/// 3. 增量更新 PageIndex
pub fn pre_split_segment(
    page: &mut [u8],
    idx: &mut PageIndex,
    seg_idx: usize,
    kind: ItemKind,
) -> Result<(), PageError> {
    use crate::checkpoint::MAX_PER_CHECKPOINT;

    let seg = &idx.segments[seg_idx];
    if seg.item_count < MAX_PER_CHECKPOINT {
        return Ok(());
    }

    let mid_offset = (seg.item_count / 2) as usize;
    let front_count = mid_offset as u16;
    let back_count = seg.item_count - front_count;

    dprintln!(
        index,
        "[PRE_SPLIT] seg_idx={} item_count={} mid_offset={} front={} back={}",
        seg_idx,
        seg.item_count,
        mid_offset,
        front_count,
        back_count
    );

    // 段内遍历到 mid item
    let mut prev_key = seg.first_full_key.clone();
    let mut off = seg.first_item_off as usize;

    for _ in 0..mid_offset {
        let (item, n) = decode_item(page, off, kind)?;
        prev_key = item.full_key(&prev_key);
        off += n;
    }
    // off 现在是 mid item 的位置
    let mid_item_off = off;

    // 解码 mid item 并计算完整 key
    let (mid_item, mid_old_n) = decode_item(page, mid_item_off, kind)?;
    let mid_full_key = mid_item.full_key(&prev_key);

    // 重编码 mid item 为 shared=0 (full key)
    let mut buf = [0u8; 4096];
    let mid_new_n = match kind {
        ItemKind::Leaf => encode_leaf_item(&mut buf, &[], &mid_full_key, mid_item.value)?,
        ItemKind::Internal => {
            encode_internal_item(&mut buf, &[], &mid_full_key, mid_item.child_vpid)?
        }
    };
    let delta_mid = mid_new_n as isize - mid_old_n as isize;

    // 如果有 delta, 移动后续 items 并更新 free_off
    if delta_mid != 0 {
        let free_off = page_free_off(page) as usize;
        page.copy_within(
            mid_item_off + mid_old_n..free_off,
            (mid_item_off as isize + mid_new_n as isize) as usize,
        );
        page_set_free_off(page, (free_off as isize + delta_mid) as u16);
    }
    // 写入重编码后的 mid item
    page[mid_item_off..mid_item_off + mid_new_n].copy_from_slice(&buf[..mid_new_n]);

    // 更新 PageIndex: 原段缩小, 插入新段
    idx.segments[seg_idx].item_count = front_count;

    let new_seg = Segment {
        first_item_off: mid_item_off as u16,
        item_count: back_count,
        first_full_key: mid_full_key.clone(),
    };
    dprintln!(
        index,
        "[PRE_SPLIT] inserting new seg at {}: first_off={} item_count={} first_key={:?}. Existing segs: {:?}",
        seg_idx + 1,
        mid_item_off,
        back_count,
        String::from_utf8_lossy(&mid_full_key),
        idx.segments
    );
    idx.segments.insert(seg_idx + 1, new_seg);

    // 关键: 重写 mid 之后的第一个 item (k+1) 为新 mid_full_key 基准.
    //
    // 原因: 旧 k+1 是用 mid (old) 算 shared. mid 现在用 full key 编码 (shared=0),
    // prev_key 字节布局变化可忽略, 因为 k+1 的 shared 是相对 MID 的 (不是 mid-1).
    // 必须用 mid_full_key 还原 k+1 的 full key, 然后用 mid_full_key 重编码.
    //
    // 防御: 如果 back_count == 0, mid 是段尾, 没有 k+1, 跳过.
    if back_count > 0 {
        let k1_off = mid_item_off + mid_new_n;
        let free_off = page_free_off(page) as usize;
        if k1_off < free_off {
            let (k1_item, k1_old_n) = decode_item(page, k1_off, kind)?;
            // k1 的 full key 用**mid_full_key**还原 (不是 prev_key=mid-1),
            // 因为 k+1 的 shared 是相对 mid 算的.
            let k1_full_key = k1_item.full_key(&mid_full_key);
            let mut k1_buf = [0u8; 4096];
            let k1_new_n = match kind {
                ItemKind::Leaf => {
                    encode_leaf_item(&mut k1_buf, &mid_full_key, &k1_full_key, k1_item.value)?
                }
                ItemKind::Internal => encode_internal_item(
                    &mut k1_buf,
                    &mid_full_key,
                    &k1_full_key,
                    k1_item.child_vpid,
                )?,
            };
            let k1_delta = k1_new_n as isize - k1_old_n as isize;
            dprintln!(
                index,
                "[PRE_SPLIT] rewrite k+1 at off={} old_n={} new_n={} delta={}",
                k1_off,
                k1_old_n,
                k1_new_n,
                k1_delta
            );
            if k1_delta != 0 {
                let cur_free = page_free_off(page) as usize;
                page.copy_within(
                    k1_off + k1_old_n..cur_free,
                    (k1_off as isize + k1_new_n as isize) as usize,
                );
                page_set_free_off(page, (cur_free as isize + k1_delta) as u16);
            }
            page[k1_off..k1_off + k1_new_n].copy_from_slice(&k1_buf[..k1_new_n]);

            // k+1 重写后, 后续 items 整体后移 k1_delta 字节.
            // 后续段的 first_item_off 需要累加 mid_delta + k1_delta.
            let total_delta = delta_mid + k1_delta;
            for s in idx.segments.iter_mut().skip(seg_idx + 2) {
                s.first_item_off = (s.first_item_off as isize + total_delta) as u16;
            }
        } else {
            // mid 是段尾, 没有 k+1, 只需应用 mid_delta 给后续段.
            for s in idx.segments.iter_mut().skip(seg_idx + 2) {
                s.first_item_off = (s.first_item_off as isize + delta_mid) as u16;
            }
        }
    } else {
        // back_count == 0, 没有 k+1, 只需应用 mid_delta 给后续段.
        for s in idx.segments.iter_mut().skip(seg_idx + 2) {
            s.first_item_off = (s.first_item_off as isize + delta_mid) as u16;
        }
    }

    Ok(())
}
