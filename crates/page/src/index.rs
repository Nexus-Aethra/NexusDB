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

use crate::checkpoint::{MAX_PER_CHECKPOINT, checkpoint_area_size};
use crate::dprintln;

use crate::error::PageError;
use crate::header::{
    PAGE_FOOTER_SIZE, PAGE_SIZE, page_free_off, page_key_count, page_set_free_off,
};
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
                                i,
                                j,
                                off,
                                e
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
        let mut lo = 0usize;
        let mut hi = self.segments.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.segments[mid].first_item_off as usize <= off {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo.saturating_sub(1)
    }
}

// ===== pre_split (基于 PageIndex 的段分裂) =====

/// pre_split 执行计划: **只读**演算阶段的产出.
///
/// 把"计算"与"改字节"分离 (plan → fits? → apply), 使调用方能在页字节
/// **未被修改之前**判断空间是否足够.
///
/// 2026-10-16 修复 (审计新 P0-1): 旧实现边算边改, 近满页上 mid item 的
/// full-key 重编码可使 `copy_within` 越界 panic, 或使 free_off 写穿
/// checkpoint 区; 且失败路径不回滚, 污染页会被 btree split 路径持久化 —
/// 与 2026-08-02 leaf 冷启动丢数据事故 (docs/bug-cold-start-data-loss.md)
/// 同一根因, 当年只修了 leaf 侧.
#[derive(Debug)]
pub(crate) struct PreSplitPlan {
    /// mid item 在页中的偏移
    pub mid_item_off: usize,
    /// mid item 原编码长度 (shared 压缩态)
    pub mid_old_n: usize,
    /// mid item full-key 重编码后的字节 (shared=0)
    pub mid_buf: Vec<u8>,
    /// mid item 的完整 key (新段 first_full_key / k+1 重编码基准)
    pub mid_full_key: Vec<u8>,
    pub front_count: u16,
    pub back_count: u16,
    /// k+1 (mid 之后第一个 item) 的重写计划; None = mid 是段尾, 无 k+1
    pub k1: Option<K1Plan>,
}

/// k+1 重写计划: shared 基准从"旧 mid 压缩表示"换成 `mid_full_key` 后的重编码.
#[derive(Debug)]
pub(crate) struct K1Plan {
    /// k+1 在**原页** (mid 移位前) 中的偏移 — 解码与取旧长度用
    pub k1_off_orig: usize,
    /// k+1 在 mid 落地后的新偏移 (写入目标)
    pub k1_off: usize,
    pub k1_old_n: usize,
    /// 重编码后的 k+1 字节
    pub k1_buf: Vec<u8>,
    /// k1_buf.len() - k1_old_n (通常为 0: k+1 的 shared 本就相对 mid 的 full key)
    pub delta: isize,
}

impl PreSplitPlan {
    /// 页数据区字节总增量 (两种重编码的净增长, >= 0).
    pub fn total_delta(&self) -> isize {
        let mut d = self.mid_buf.len() as isize - self.mid_old_n as isize;
        if let Some(k1) = &self.k1 {
            d += k1.delta;
        }
        d
    }
}

/// pre_split 后的 free_off (应用前).
pub(crate) fn pre_split_resulting_free_off(page: &[u8], plan: &PreSplitPlan) -> isize {
    page_free_off(page) as isize + plan.total_delta()
}

/// pre_split 落地后页是否仍然合法:
/// `free_off + checkpoint_area_size(段数+1) + footer <= PAGE_SIZE`.
///
/// 调用方必须在**任何字节被修改前**调用本检查; 不满足则放弃 pre_split
/// (返回 `PageFull`), 页保持原样、checkpoint 保持一致.
pub(crate) fn pre_split_fits(page: &[u8], idx: &PageIndex, plan: &PreSplitPlan) -> bool {
    let new_free = pre_split_resulting_free_off(page, plan);
    if new_free < 0 {
        return false;
    }
    let cp_size = checkpoint_area_size(idx.segments.len() + 1);
    new_free as usize + cp_size + PAGE_FOOTER_SIZE <= PAGE_SIZE
}

/// 只读演算 pre_split: 定位 mid item, 计算 full-key 重编码增量与 k+1 重写计划.
/// **不修改页任何字节** — 空间是否足够由调用方用 `pre_split_fits` 判断.
pub(crate) fn plan_pre_split(
    page: &[u8],
    idx: &PageIndex,
    seg_idx: usize,
    kind: ItemKind,
) -> Result<PreSplitPlan, PageError> {
    let seg = &idx.segments[seg_idx];
    if seg.item_count < MAX_PER_CHECKPOINT {
        return Err(PageError::SplitTooFew(seg.item_count as usize));
    }

    let mid_offset = (seg.item_count / 2) as usize;
    let front_count = mid_offset as u16;
    let back_count = seg.item_count - front_count;

    dprintln!(
        index,
        "[PRE_SPLIT] plan: seg_idx={} item_count={} mid_offset={} front={} back={}",
        seg_idx,
        seg.item_count,
        mid_offset,
        front_count,
        back_count
    );

    // 段内遍历到 mid item (只读)
    let mut prev_key = seg.first_full_key.clone();
    let mut off = seg.first_item_off as usize;
    for _ in 0..mid_offset {
        let (item, n) = decode_item(page, off, kind)?;
        prev_key = item.full_key(&prev_key);
        off += n;
    }
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

    // k+1 重写计划 (只读演算).
    //
    // ⚠️ 关键: k+1 必须在**原页位置** `mid_item_off + mid_old_n` 解码 — 此时页尚未
    // 移位, 该处才是 k+1 的真实字节. (mid 落地后 k+1 才位于 mid_item_off + mid_new_n,
    // 那是 apply 阶段的写入目标, 不是 plan 阶段的解码位置.)
    // 存在性判断两种等价: k1_off_orig < free_off_orig ⟺ (mid_item_off + mid_new_n) <
    // (free_off_orig + delta_mid).
    let k1 = if back_count > 0 {
        let k1_off_orig = mid_item_off + mid_old_n;
        let free_off_orig = page_free_off(page) as usize;
        if k1_off_orig < free_off_orig {
            let (k1_item, k1_old_n) = decode_item(page, k1_off_orig, kind)?;
            // k1 的 full key 用 mid_full_key 还原 (k+1 的 shared 是相对 mid 算的)
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
            Some(K1Plan {
                k1_off_orig,
                k1_off: mid_item_off + mid_new_n, // mid 落地后的写入目标
                k1_old_n,
                k1_buf: k1_buf[..k1_new_n].to_vec(),
                delta: k1_new_n as isize - k1_old_n as isize,
            })
        } else {
            None // mid 是段尾, 没有 k+1
        }
    } else {
        None
    };

    Ok(PreSplitPlan {
        mid_item_off,
        mid_old_n,
        mid_buf: buf[..mid_new_n].to_vec(),
        mid_full_key,
        front_count,
        back_count,
        k1,
    })
}

/// 按 plan 落地 pre_split (改字节). 调用方必须先用 `pre_split_fits` 校验空间.
///
/// 字节操作顺序与原实现逐字节一致: mid 移位 → mid 写入 → PageIndex 更新 →
/// k+1 移位 → k+1 写入 → 后续段 offset 修正.
pub(crate) fn apply_pre_split(
    page: &mut [u8],
    idx: &mut PageIndex,
    seg_idx: usize,
    plan: &PreSplitPlan,
) -> Result<(), PageError> {
    let mid_item_off = plan.mid_item_off;
    let mid_old_n = plan.mid_old_n;
    let mid_new_n = plan.mid_buf.len();
    let delta_mid = mid_new_n as isize - mid_old_n as isize;

    // 1. mid item 重编码: 移动后续 items 并更新 free_off
    if delta_mid != 0 {
        let free_off = page_free_off(page) as usize;
        page.copy_within(mid_item_off + mid_old_n..free_off, mid_item_off + mid_new_n);
        page_set_free_off(page, (free_off as isize + delta_mid) as u16);
    }
    // 写入重编码后的 mid item
    page[mid_item_off..mid_item_off + mid_new_n].copy_from_slice(&plan.mid_buf);

    // 2. 更新 PageIndex: 原段缩小, 插入新段
    idx.segments[seg_idx].item_count = plan.front_count;

    let new_seg = Segment {
        first_item_off: mid_item_off as u16,
        item_count: plan.back_count,
        first_full_key: plan.mid_full_key.clone(),
    };
    dprintln!(
        index,
        "[PRE_SPLIT] apply: inserting new seg at {}: first_off={} item_count={} first_key={:?}. Existing segs: {:?}",
        seg_idx + 1,
        mid_item_off,
        plan.back_count,
        String::from_utf8_lossy(&plan.mid_full_key),
        idx.segments
    );
    idx.segments.insert(seg_idx + 1, new_seg);

    // 3. 重写 mid 之后的第一个 item (k+1) 为新 mid_full_key 基准.
    //
    // 原因: 旧 k+1 是用 mid (old) 算 shared. mid 现在用 full key 编码 (shared=0),
    // 必须用 mid_full_key 还原 k+1 的 full key, 然后用 mid_full_key 重编码.
    match &plan.k1 {
        Some(k1) => {
            let k1_new_n = k1.k1_buf.len();
            if k1.delta != 0 {
                let cur_free = page_free_off(page) as usize;
                page.copy_within(k1.k1_off + k1.k1_old_n..cur_free, k1.k1_off + k1_new_n);
                page_set_free_off(page, (cur_free as isize + k1.delta) as u16);
            }
            page[k1.k1_off..k1.k1_off + k1_new_n].copy_from_slice(&k1.k1_buf);

            // k+1 重写后, 后续 items 整体后移 total_delta 字节,
            // 后续段的 first_item_off 需要累加.
            let total_delta = delta_mid + k1.delta;
            for s in idx.segments.iter_mut().skip(seg_idx + 2) {
                s.first_item_off = (s.first_item_off as isize + total_delta) as u16;
            }
        }
        None => {
            // mid 是段尾, 没有 k+1, 只需应用 delta_mid 给后续段.
            for s in idx.segments.iter_mut().skip(seg_idx + 2) {
                s.first_item_off = (s.first_item_off as isize + delta_mid) as u16;
            }
        }
    }

    Ok(())
}

/// 预分裂 segments[seg_idx]: 当 item_count >= MAX_PER_CHECKPOINT (32) 时,
/// 将段对半分裂为两个段.
///
/// 流程:
/// 1. 找到段的 mid item, 将其重编码为 shared=0 (full key)
/// 2. 前一半保留在原段, 后一半放入新段
/// 3. 增量更新 PageIndex
///
/// ⭐ 安全边界 (2026-10-16, L3): 先只读演算 + `pre_split_fits` 校验,
/// **在任何字节被修改前**拒绝放不下的分裂. 旧实现无此检查: 近满页上
/// `copy_within` 越界 panic, 或 free_off 写穿 checkpoint 区 (静默损毁页).
pub fn pre_split_segment(
    page: &mut [u8],
    idx: &mut PageIndex,
    seg_idx: usize,
    kind: ItemKind,
) -> Result<(), PageError> {
    if idx.segments[seg_idx].item_count < MAX_PER_CHECKPOINT {
        return Ok(());
    }

    let plan = plan_pre_split(page, idx, seg_idx, kind)?;
    if !pre_split_fits(page, idx, &plan) {
        // 页尚未被修改: 安全退出, 由调用方决定 split 整页
        return Err(PageError::PageFull);
    }
    apply_pre_split(page, idx, seg_idx, &plan)
}

#[cfg(test)]
mod tests {
    use super::{PageIndex, Segment};

    #[test]
    fn find_segment_by_offset_is_upper_bound_binary_search() {
        let idx = PageIndex {
            segments: vec![
                Segment {
                    first_item_off: 32,
                    item_count: 1,
                    first_full_key: vec![],
                },
                Segment {
                    first_item_off: 96,
                    item_count: 1,
                    first_full_key: b"a".to_vec(),
                },
                Segment {
                    first_item_off: 192,
                    item_count: 1,
                    first_full_key: b"b".to_vec(),
                },
                Segment {
                    first_item_off: 384,
                    item_count: 1,
                    first_full_key: b"c".to_vec(),
                },
            ],
            key_count: 4,
        };
        assert_eq!(idx.find_segment_by_offset(0), 0);
        assert_eq!(idx.find_segment_by_offset(96), 1);
        assert_eq!(idx.find_segment_by_offset(191), 1);
        assert_eq!(idx.find_segment_by_offset(999), 3);
    }
}
