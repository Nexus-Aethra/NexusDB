//! Leaf Page 分裂操作 (拆自 leaf.rs).
//!
//! 独立成模块以解耦 leaf 主操作 (get/insert/delete/push_back/update) 与分裂逻辑.
//! 分裂涉及强制段中点分裂 (`force_split_segment_at_mid`)、批量分裂 (`try_bulk_leaf_split`)
//! 与最简分裂 (`leaf_split_minimal`), 统一由 `leaf_split` 入口驱动.

use crate::checkpoint::{Checkpoint, CheckpointHeader, write_checkpoint, write_checkpoint_header};
use crate::error::PageError;
use crate::header::{
    PAGE_HEADER_SIZE, PAGE_SIZE, PageType, page_free_off, page_init_header, page_key_count,
    page_set_free_off, page_set_key_count, page_type,
};
use crate::index::PageIndex;
use crate::item::{ItemKind, decode_item, encode_leaf_item};

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

    // 计算 key counts (从 PageIndex 段边界直接求和)
    let left_total_items: u16 = idx.segments[..best_seg].iter().map(|s| s.item_count).sum();
    let left_key_count = left_total_items - 1; // 减去哨兵
    let right_key_count: u16 = idx.segments[best_seg..].iter().map(|s| s.item_count).sum();

    // ⭐ DIAG: 校验 PageIndex item_count 与实际 item 数是否一致
    if std::env::var("NLOG_DIAG").is_ok_and(|v| v == "1") {
        let mut actual_count = 0u16;
        let mut off = PAGE_HEADER_SIZE;
        let mut prev_key: Vec<u8> = Vec::new();
        while off < free {
            match decode_item(left, off, ItemKind::Leaf) {
                Ok((item, n)) => {
                    let _full = item.full_key(&prev_key);
                    prev_key = _full;
                    actual_count += 1;
                    off += n;
                }
                Err(_) => break,
            }
        }
        let idx_total: u16 = idx.segments.iter().map(|s| s.item_count).sum();
        if actual_count != idx_total {
            eprintln!(
                "[DIAG-IDX-MISMATCH] actual_items={actual_count} idx_items={idx_total} \
                 key_count={} free={free} segments={} best_seg={best_seg} \
                 left_keys={left_key_count} right_keys={right_key_count}",
                page_key_count(left),
                idx.segments.len()
            );
            // dump 每个 segment 的 item_count
            for (si, seg) in idx.segments.iter().enumerate() {
                eprintln!(
                    "  seg[{si}] item_count={} first_off={} first_key={:?}",
                    seg.item_count, seg.first_item_off,
                    String::from_utf8_lossy(&seg.first_full_key[..seg.first_full_key.len().min(20)])
                );
            }
        }
    }

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
    // ⭐ 修复 (2026-08-02, 冷启动丢 32 行 P0): 分裂点的首 item 可能与左页
    // 末尾 item 存在前缀压缩 (shared > 0). 直接 memcpy 到右页后, 前缀引用
    // 变为哨兵 (empty key) 而非原始前驱 → key 解压错误 → 后续 item 解码
    // 偏移 → 静默丢 key. 修复: 首 item 用 split_key 重编码 (shared=0),
    // 其余 item 保持原始 memcpy (它们的前缀引用首 item, key 不变).
    let (first_item, first_n) = decode_item(left, split_off, ItemKind::Leaf).ok()?;
    // ⭐ DIAG: 检查首 item 是否有前缀压缩 (shared > 0 = 确认 bug 场景)
    if first_item.shared_prefix_len > 0 {
        eprintln!(
            "[DIAG-SPLIT-FIX] split_key={split_key:?} first_item shared={} unshared={} — re-encoding with shared=0",
            first_item.shared_prefix_len, first_item.key_unshared_len
        );
    }
    let mut first_buf = [0u8; PAGE_SIZE];
    let first_encoded_n =
        encode_leaf_item(&mut first_buf, &[], &split_key, first_item.value).ok()?;
    right[write_start..write_start + first_encoded_n]
        .copy_from_slice(&first_buf[..first_encoded_n]);
    // 复制剩余 items (首 item 之后)
    let rest_src = split_off + first_n;
    let rest_len = free - rest_src;
    let rest_dst = write_start + first_encoded_n;
    if rest_len > 0 {
        right[rest_dst..rest_dst + rest_len].copy_from_slice(&left[rest_src..free]);
    }
    let right_free = rest_dst + rest_len;

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
