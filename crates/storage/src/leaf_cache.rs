//! ⭐ LeafCache: 读路径 PageIndex 缓存 (第一步性能优化)
//!
//! ## 动机
//!
//! `btree_lookup` 每次都从 root travel 到 leaf (depth ~3-4 层), 每层一次
//! `pager.read` (四源查找 + 16KB 拷贝). 热点 key 重复 travel 是纯浪费.
//!
//! ## 设计
//!
//! `HashMap<root_vpid, BTreeMap<lower, LeafGuide>>` 分层:
//! - 外层 HashMap 按 root 分组 (O(1) 查找)
//! - 内层 BTreeMap 按 lower 排序, 用 `&[u8]` range 查找 (`..=key` 取 next_back)
//!
//! **零分配 lookup**: `range(..=key)` 用 `&[u8]` bound (Vec<u8>: Borrow<[u8]>),
//! 无需 to_vec, 避免 alloc 开销.
//!
//! ## 安全性 (COW + split)
//!
//! - **vpid 永不重用**: 缓存的 leaf_vpid 不会指向别的 page.
//! - **leaf split 后**: 原 leaf vpid 仍是 Leaf 类型 (存左半), page_type 校验通过.
//!   但 key 可能跑到 right leaf → `LeafGuide.contains` 返回 false → cache miss →
//!   重新 travel + 回填. **不会读到错误数据**.
//! - **split 时主动失效**: `invalidate_root(root_vpid)` 清空该 root 的所有条目.
//! - **delete 不失效**: 当前不实现 merge, leaf 覆盖区间不变.
//! - **update 不失效**: 原地更新, leaf_vpid + 覆盖区间都不变.
//!
//! ## 容量管理
//!
//! 简单策略: 总条目数超过 cap 时清空整个 cache. split 也是清空整个 root.
//! 热点 key 会快速回填.

use std::collections::{BTreeMap, HashMap};

use crate::btree::LeafGuide;

/// 默认容量 (per-shard). 10K 条目 × ~80B ≈ 800KB 内存.
const DEFAULT_CAP: usize = 10_000;

/// Leaf 覆盖区间缓存.
///
/// 按 root 分组, 组内按 lower 排序. lookup 零分配.
pub struct LeafCache {
    /// root_vpid → (inner BTreeMap: lower → LeafGuide)
    roots: HashMap<u64, BTreeMap<Vec<u8>, LeafGuide>>,
    /// 总条目数 (所有 root 加起来)
    len: usize,
    /// 容量上限 (满时清空)
    cap: usize,
    /// 命中次数 (观测/测试用)
    hits: u64,
    /// 未命中次数
    misses: u64,
    /// 失效次数 (split 触发)
    invalidations: u64,
}

impl Default for LeafCache {
    fn default() -> Self {
        Self::new(DEFAULT_CAP)
    }
}

impl LeafCache {
    /// 创建指定容量的 LeafCache.
    pub fn new(cap: usize) -> Self {
        Self {
            roots: HashMap::new(),
            len: 0,
            cap,
            hits: 0,
            misses: 0,
            invalidations: 0,
        }
    }

    /// 查找 key 所属的 leaf vpid.
    ///
    /// **算法**: 在 root_vpid 对应的 inner BTreeMap 里 `range(..=key)` 取
    /// `next_back()` (最大的 lower ≤ key), 校验 `contains(key)`.
    ///
    /// **零分配**: range 用 `&[u8]` bound, 无 to_vec.
    ///
    /// **为什么只检查 next_back**: B+Tree 叶子区间连续不重叠. lower ≤ key 的
    /// 条目里, 最大的 lower 对应的 guide 是唯一可能 contains(key) 的. 其他
    /// lower 更小的 guide, upper 只会更小, 不可能 contains.
    pub fn lookup(&mut self, root_vpid: u64, key: &[u8]) -> Option<u64> {
        use std::ops::Bound;
        let inner = self.roots.get(&root_vpid);
        if let Some(inner) = inner
            && let Some((_, guide)) = inner
                .range::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
                .next_back()
            && guide.contains(key)
        {
            self.hits += 1;
            return Some(guide.leaf_vpid);
        }
        self.misses += 1;
        None
    }

    /// 插入/更新 leaf guide. 用 `guide.lower` 作排序键.
    ///
    /// 如果 lower 为 None (最左 leaf), 用空 Vec 作 key (排序最前).
    pub fn insert(&mut self, root_vpid: u64, guide: LeafGuide) {
        // 容量检查: 满时清空 (简单策略, 热点会快速回填)
        if self.len >= self.cap {
            self.roots.clear();
            self.len = 0;
        }
        let sort_key = guide.lower.clone().unwrap_or_default();
        let inner = self.roots.entry(root_vpid).or_default();
        // BTreeMap insert 替换同 key, 长度只在新增 key 时 +1
        let was_new = inner.insert(sort_key, guide).is_none();
        if was_new {
            self.len += 1;
        }
    }

    /// 失效整个 root 的所有条目.
    ///
    /// **调用时机**: btree_insert 触发 split 时 (leaf split 或 internal split
    /// 都会改变 leaf 覆盖区间). split 是低频操作, 清空整个 root 的 cache 可接受.
    pub fn invalidate_root(&mut self, root_vpid: u64) {
        if let Some(inner) = self.roots.remove(&root_vpid)
            && !inner.is_empty()
        {
            self.len -= inner.len();
            self.invalidations += 1;
        }
    }

    /// 清空所有条目.
    pub fn clear(&mut self) {
        self.roots.clear();
        self.len = 0;
    }

    /// 当前条目数.
    pub fn len(&self) -> usize {
        self.len
    }

    /// 是否为空.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// 统计 (hits, misses, invalidations).
    pub fn stats(&self) -> (u64, u64, u64) {
        (self.hits, self.misses, self.invalidations)
    }

    /// 重置统计.
    pub fn reset_stats(&mut self) {
        self.hits = 0;
        self.misses = 0;
        self.invalidations = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn guide(leaf_vpid: u64, lower: Option<&[u8]>, upper: Option<&[u8]>) -> LeafGuide {
        LeafGuide {
            leaf_vpid,
            lower: lower.map(|b| b.to_vec()),
            upper: upper.map(|b| b.to_vec()),
        }
    }

    #[test]
    fn test_lookup_hit_single_leaf() {
        let mut cache = LeafCache::new(100);
        cache.insert(1, guide(10, None, None));

        assert_eq!(cache.lookup(1, b"abc"), Some(10));
        assert_eq!(cache.lookup(1, b"xyz"), Some(10));
        assert_eq!(cache.lookup(1, b""), Some(10));
        let (hits, misses, _) = cache.stats();
        assert_eq!(hits, 3);
        assert_eq!(misses, 0);
    }

    #[test]
    fn test_lookup_hit_multiple_leaves() {
        let mut cache = LeafCache::new(100);
        cache.insert(1, guide(10, None, Some(b"m")));
        cache.insert(1, guide(20, Some(b"m"), Some(b"z")));
        cache.insert(1, guide(30, Some(b"z"), None));

        assert_eq!(cache.lookup(1, b"apple"), Some(10));
        assert_eq!(cache.lookup(1, b"monkey"), Some(20));
        assert_eq!(cache.lookup(1, b"zebra"), Some(30));
        assert_eq!(cache.lookup(1, b"m"), Some(20)); // [lower, upper)
    }

    #[test]
    fn test_lookup_miss_empty_cache() {
        let mut cache = LeafCache::new(100);
        assert_eq!(cache.lookup(1, b"abc"), None);
        let (hits, misses, _) = cache.stats();
        assert_eq!(hits, 0);
        assert_eq!(misses, 1);
    }

    #[test]
    fn test_lookup_miss_wrong_root() {
        let mut cache = LeafCache::new(100);
        cache.insert(1, guide(10, None, None));
        assert_eq!(cache.lookup(2, b"abc"), None);
    }

    #[test]
    fn test_invalidate_root() {
        let mut cache = LeafCache::new(100);
        cache.insert(1, guide(10, None, Some(b"m")));
        cache.insert(1, guide(20, Some(b"m"), None));
        cache.insert(2, guide(30, None, None));

        cache.invalidate_root(1);
        assert_eq!(cache.lookup(1, b"abc"), None);
        assert_eq!(cache.lookup(2, b"abc"), Some(30));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_invalidate_root_noop_for_unknown_root() {
        let mut cache = LeafCache::new(100);
        cache.insert(1, guide(10, None, None));
        cache.invalidate_root(999);
        assert_eq!(cache.len(), 1);
        let (_, _, invalidations) = cache.stats();
        assert_eq!(invalidations, 0);
    }

    #[test]
    fn test_capacity_clears_all() {
        let mut cache = LeafCache::new(3);
        cache.insert(1, guide(10, None, Some(b"a")));
        cache.insert(1, guide(20, Some(b"a"), Some(b"b")));
        cache.insert(1, guide(30, Some(b"b"), None));
        assert_eq!(cache.len(), 3);
        cache.insert(1, guide(40, Some(b"c"), None));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_split_safety_stale_upper() {
        let mut cache = LeafCache::new(100);
        cache.insert(1, guide(10, None, Some(b"z")));

        assert_eq!(cache.lookup(1, b"x"), Some(10)); // stale 命中

        cache.invalidate_root(1);
        assert_eq!(cache.lookup(1, b"x"), None);
    }

    #[test]
    fn test_multiple_roots_isolated() {
        let mut cache = LeafCache::new(100);
        cache.insert(1, guide(10, None, None));
        cache.insert(2, guide(20, None, None));

        assert_eq!(cache.lookup(1, b"key"), Some(10));
        assert_eq!(cache.lookup(2, b"key"), Some(20));

        cache.invalidate_root(1);
        assert_eq!(cache.lookup(1, b"key"), None);
        assert_eq!(cache.lookup(2, b"key"), Some(20));
    }

    #[test]
    fn test_insert_replaces_same_lower() {
        let mut cache = LeafCache::new(100);
        cache.insert(1, guide(10, None, Some(b"m")));
        assert_eq!(cache.len(), 1);
        // 同 lower (None → 空 Vec), 替换不增计数
        cache.insert(1, guide(15, None, Some(b"n")));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.lookup(1, b"a"), Some(15)); // 新 guide 生效
    }
}
