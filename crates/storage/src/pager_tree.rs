//! Pager 的 travel tree (拆自 pager.rs).
//!
//! `TravelTree` + `TravelTreeGuard`: B+Tree split 传播一致性 (DESIGN §3.0.1).
//! RAII guard 自动 register / unregister; split 广播更新逻辑在 T8 实施.

use std::collections::HashMap;

use crate::chunk_lru::ChunkKey;
use crate::pager::{Pager, TaskId};
use crate::types::{CHUNK_SIZE, DEFAULT_DB_ID, PageKey};

pub struct TravelTree {
    map: HashMap<Vec<u8>, u64>,
}

impl TravelTree {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// 记录 (key, vpid).
    pub fn record(&mut self, key: Vec<u8>, vpid: u64) {
        self.map.insert(key, vpid);
    }

    /// 用 key 查 vpid.
    pub fn lookup(&self, key: &[u8]) -> Option<u64> {
        self.map.get(key).copied()
    }

    /// 范围更新: 把 value == old_vpid 且 key 在 [lo, hi) 范围的条目更新为 new_vpid.
    /// 用于 split 传播: right page 的 vpid 替换 left page 的 vpid 在某些 key 上的 mapping.
    ///
    /// **半开区间**: `[lo, hi)`, lo 包含, hi 不包含. 与 B+Tree split 语义一致:
    /// - right_lo = right page 第一个 cp 段首 key (含, 应该被替换)
    /// - right_hi = 下一个 page 的最小 key (不含, 不应被替换)
    pub fn range_update(&mut self, lo: &[u8], hi: &[u8], old_vpid: u64, new_vpid: u64) {
        let updates: Vec<Vec<u8>> = self
            .map
            .iter()
            .filter(|(k, v)| **v == old_vpid && k.as_slice() >= lo && k.as_slice() < hi)
            .map(|(k, _)| k.clone())
            .collect();
        for k in updates {
            self.map.insert(k, new_vpid);
        }
    }

    /// 找所有 value == vpid 的 key.
    pub fn find_all_with_vpid(&self, vpid: u64) -> Vec<Vec<u8>> {
        self.map
            .iter()
            .filter(|(_, v)| **v == vpid)
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl Default for TravelTree {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard: drop 时自动 unregister. 防止手动 unregister 遗漏.
pub struct TravelTreeGuard<'p> {
    task_id: TaskId,
    pager: &'p mut Pager,
}

impl<'p> TravelTreeGuard<'p> {
    /// 创建 guard, 内部自动 register.
    pub fn new(task_id: TaskId, pager: &'p mut Pager) -> Self {
        pager.travel_trees.entry(task_id).or_default();
        Self { task_id, pager }
    }

    /// 拿到 task 的 travel_tree 的可变引用.
    pub fn tree(&mut self) -> &mut TravelTree {
        self.pager
            .travel_trees
            .get_mut(&self.task_id)
            .expect("travel_tree should be registered in guard::new")
    }
}

impl Drop for TravelTreeGuard<'_> {
    fn drop(&mut self) {
        // 自动 unregister
        self.pager.travel_trees.remove(&self.task_id);
    }
}

// =====================================================================
// helper
// =====================================================================

/// chunk_offset: 给定 chunk 在 .block file 内的字节偏移.
///
/// **重要**: 每个 .block 文件是独立的物理文件, offset 总是从 0 开始.
/// file_id 选择哪个 .block 文件, chunk_idx 决定文件内的 1MB 段偏移.
pub(crate) fn chunk_offset(key: PageKey) -> u64 {
    // ⭐ 修复 (2026-07-21): 每个 .block 文件独立 offset 空间.
    // 早期实现把 `file_id * BLOCK_SIZE + chunk_idx * CHUNK_SIZE` 当作全局 offset,
    // 错误: 写入 `000003.block` 时 offset 应该是 chunk 内的偏移 (chunk_idx * CHUNK_SIZE),
    // 不是全局 20MB+. 旧实现导致 sparse file 扩展到错误位置, 后续 page (page 14) 写到了
    // file 末尾, 但 scan 仍然按 0..PAGES_PER_CHUNK 读 page 0 (空) 就以为该 chunk 是空的.
    (key.chunk_idx as u64) * CHUNK_SIZE as u64
}

// 抑制 unused warning
#[allow(dead_code)]
fn _unused_chunk_key() -> ChunkKey {
    ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    }
}
