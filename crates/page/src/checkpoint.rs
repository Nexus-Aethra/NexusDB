//! Checkpoint 索引数组.
//!
//! ## 设计思路
//!
//! checkpoint item 是 **item 区中的普通 item**,只不过段首 item 必须
//! `shared_prefix_len = 0` (存完整 key),这样二分查找段时可以**直接读 cp item
//! 的字节得到完整 key**,无需从头还原.
//!
//! checkpoint index array 放在页面尾部,只存 `[item_count, first_item_off]`
//! 这两个 metadata,加上 cp header 8B.
//!
//! ## 段二分算法
//!
//! 1. 读 cp[i].first_item_off 处的 item 字节,解码得到 shared_prefix_len=0 的 item
//! 2. 用其 full_key 与目标 key 比较
//! 3. 找最后一个 cp[i].full_key <= target_key 的 cp (这是目标 key 可能所在的段)
//!
//! ## 段内查找
//!
//! 段大小 ≤ MAX_PER_CHECKPOINT (32),顺序扫描 O(32) = O(1).

use crate::header::CHECKPOINT_AREA_END;

/// Checkpoint index 固定字节数: item_count + first_item_off.
pub const CHECKPOINT_SIZE: usize = 4;

/// Checkpoint Header: 8 字节.
pub const CHECKPOINT_HEADER_SIZE: usize = 8;

/// 每段 item 数上限. 超过此值强制分裂.
pub const MAX_PER_CHECKPOINT: u16 = 32;

/// 每段 item 数下限. 低于此值考虑合并.
pub const MIN_PER_CHECKPOINT: u16 = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    pub item_count: u16,
    pub first_item_off: u16,
}

#[derive(Clone, Copy, Debug)]
pub struct CheckpointHeader {
    pub checkpoint_count: u16,
    pub min_per_cp: u16,
    pub max_per_cp: u16,
    pub flags: u16,
}

impl Default for CheckpointHeader {
    fn default() -> Self {
        Self {
            checkpoint_count: 0,
            min_per_cp: MIN_PER_CHECKPOINT,
            max_per_cp: MAX_PER_CHECKPOINT,
            flags: 0,
        }
    }
}

/// cp area 占用字节数 (含 8B header).
pub fn checkpoint_area_size(checkpoint_count: usize) -> usize {
    CHECKPOINT_HEADER_SIZE + checkpoint_count * CHECKPOINT_SIZE
}

/// 读 cp header.
pub fn read_checkpoint_header(page: &[u8]) -> (CheckpointHeader, usize) {
    let hdr_off = CHECKPOINT_AREA_END - CHECKPOINT_HEADER_SIZE;
    let hdr = CheckpointHeader {
        checkpoint_count: u16::from_le_bytes(page[hdr_off..hdr_off + 2].try_into().unwrap()),
        min_per_cp: u16::from_le_bytes(page[hdr_off + 2..hdr_off + 4].try_into().unwrap()),
        max_per_cp: u16::from_le_bytes(page[hdr_off + 4..hdr_off + 6].try_into().unwrap()),
        flags: u16::from_le_bytes(page[hdr_off + 6..hdr_off + 8].try_into().unwrap()),
    };
    (hdr, hdr_off)
}

/// 写 cp header.
pub fn write_checkpoint_header(page: &mut [u8], hdr: CheckpointHeader) {
    let hdr_off = CHECKPOINT_AREA_END - CHECKPOINT_HEADER_SIZE;
    page[hdr_off..hdr_off + 2].copy_from_slice(&hdr.checkpoint_count.to_le_bytes());
    page[hdr_off + 2..hdr_off + 4].copy_from_slice(&hdr.min_per_cp.to_le_bytes());
    page[hdr_off + 4..hdr_off + 6].copy_from_slice(&hdr.max_per_cp.to_le_bytes());
    page[hdr_off + 6..hdr_off + 8].copy_from_slice(&hdr.flags.to_le_bytes());
}

/// 读第 i 个 cp index entry.
pub fn read_checkpoint(page: &[u8], i: usize) -> Checkpoint {
    let hdr_off = CHECKPOINT_AREA_END - CHECKPOINT_HEADER_SIZE;
    let data_start = hdr_off - (i + 1) * CHECKPOINT_SIZE;
    Checkpoint {
        item_count: u16::from_le_bytes(page[data_start..data_start + 2].try_into().unwrap()),
        first_item_off: u16::from_le_bytes(
            page[data_start + 2..data_start + 4].try_into().unwrap(),
        ),
    }
}

/// 写第 i 个 cp index entry.
pub fn write_checkpoint(page: &mut [u8], i: usize, cp: Checkpoint) {
    let hdr_off = CHECKPOINT_AREA_END - CHECKPOINT_HEADER_SIZE;
    let data_start = hdr_off - (i + 1) * CHECKPOINT_SIZE;
    page[data_start..data_start + 2].copy_from_slice(&cp.item_count.to_le_bytes());
    page[data_start + 2..data_start + 4].copy_from_slice(&cp.first_item_off.to_le_bytes());
}

/// 需要的 checkpoint 数量.
pub fn needed_checkpoint_count(key_count: usize) -> usize {
    if key_count == 0 {
        0
    } else {
        key_count.div_ceil(MAX_PER_CHECKPOINT as usize)
    }
}

// ============================================================================
// 已废弃 (Phase 7 清理)
// ============================================================================
//
// 之前计划中以下函数已删除, 请勿再引入:
// - reencode_item_at_as_full_key
// - insert_checkpoint_at / remove_checkpoint_at
// - split_checkpoint / try_merge_checkpoint
// - maybe_split_after_insert / maybe_merge_after_delete
// - rebuild_all_items / rebuild_cps
//
// 替代方案 (基于 PageIndex + 哨兵 + prefix-compress):
// - segment 分裂: pre_split_segment (index.rs)
// - segment 合并: pre_merge_segment (index.rs, 暂未完整实现)
// - 全量重建: 已废弃, 性能不可接受 O(N)
// - 单 item 重写 shared_prefix_len: leaf_push_back / internal_push_back 增量处理
// - 单 item 删除后链式重写: leaf_delete 增量处理
