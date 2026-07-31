//! ⭐ Y1 (布隆剪枝): shard 本地索引布隆过滤器.
//!
//! 每 shard 引擎内每 (db, table, iid) 一个; ⭐ ORM-B1: 位图原子化
//! (`AtomicU64`) — worker 层路由缓存多 worker 共享时无锁并发安全
//! (只增不减语义与原子 fetch_or 天然契合); shard 单线程用法零行为变化.
//!
//! ## 正确性 (永不假阴性)
//! - 喂值点单一 (`row_put` 写索引行处); 删/换值不摘除 → 只累积假阳性 (无害)
//! - 开库随 `rebuild_composite_counts` 扫 `[I]` 前缀重建 → 重启后照样剪枝
//! - 无条目 (异常路径) → caller 不剪枝正常扫
//! - 并发可见性: insert 用 AcqRel, 查询 Acquire — insert 先于任务 channel
//!   send (Release) 发生, 完成回执因果链后的查询必见位

use std::sync::atomic::{AtomicU64, Ordering};

/// 64K bit (8KB) 定长位图; k=2 双哈希.
const BLOOM_BITS: usize = 64 * 1024;
const BLOOM_WORDS: usize = BLOOM_BITS / 64;

/// 单个索引的布隆过滤器 (原子位图, `&self` 即可写).
pub struct IndexBloom {
    bits: Vec<AtomicU64>,
}

impl Default for IndexBloom {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexBloom {
    pub fn new() -> Self {
        Self { bits: (0..BLOOM_WORDS).map(|_| AtomicU64::new(0)).collect() }
    }

    /// FNV-1a 双哈希: h1 正向, h2 反向字节流 (独立性足够, k=2).
    fn hashes(val: &[u8]) -> (usize, usize) {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x100_0000_01b3;
        let mut h1 = OFFSET;
        for &b in val {
            h1 = (h1 ^ b as u64).wrapping_mul(PRIME);
        }
        let mut h2 = OFFSET;
        for &b in val.iter().rev() {
            h2 = (h2 ^ b as u64).wrapping_mul(PRIME).wrapping_add(0x9e37_79b9);
        }
        (h1 as usize % BLOOM_BITS, h2 as usize % BLOOM_BITS)
    }

    pub fn insert(&self, val: &[u8]) {
        let (a, b) = Self::hashes(val);
        self.bits[a / 64].fetch_or(1u64 << (a % 64), Ordering::AcqRel);
        self.bits[b / 64].fetch_or(1u64 << (b % 64), Ordering::AcqRel);
    }

    pub fn may_contain(&self, val: &[u8]) -> bool {
        let (a, b) = Self::hashes(val);
        (self.bits[a / 64].load(Ordering::Acquire) >> (a % 64)) & 1 == 1
            && (self.bits[b / 64].load(Ordering::Acquire) >> (b % 64)) & 1 == 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        // property: 插入过的值必可查 (剪枝正确性红线)
        let b = IndexBloom::new();
        let mut vals = Vec::new();
        let mut x: u64 = 0x1234_5678_9abc_def0;
        for i in 0..5000u32 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let mut v = x.to_be_bytes().to_vec();
            v.extend_from_slice(&i.to_le_bytes());
            b.insert(&v);
            vals.push(v);
        }
        for v in &vals {
            assert!(b.may_contain(v), "假阴性: {v:?}");
        }
    }

    #[test]
    fn rejects_most_absent_values() {
        // 剪枝效果: 未插入值绝大多数被拒 (5000 项 / 64K bit, 假阳性率应很低)
        let b = IndexBloom::new();
        for i in 0..5000u64 {
            b.insert(format!("present-{i}").as_bytes());
        }
        let fp = (0..5000u64)
            .filter(|i| b.may_contain(format!("absent-{i}").as_bytes()))
            .count();
        assert!(fp < 250, "假阳性率过高: {fp}/5000");
    }

    #[test]
    fn empty_bloom_rejects_all() {
        let b = IndexBloom::new();
        assert!(!b.may_contain(b"anything"));
        assert!(!b.may_contain(b""));
    }
}
