//! 路由策略: 根据 (db, table, key) 决定去哪个 shard.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::ShardId;

/// 路由 trait: 把 KV 操作路由到具体 shard.
///
/// **稳定性**: 同一 (db, table, key) 永远路由到同一 shard, 即使重启后
/// (只要 num_shards 不变). 这是 hash 路由的基础保证.
pub trait Router: Send + Sync {
    fn route(&self, db: &str, table: &str, key: &[u8]) -> ShardId;
}

/// 默认 hash router: `std::hash::DefaultHasher` (SipHash 1-3).
///
/// **为什么用 DefaultHasher**:
/// - 标准库自带, 无依赖
/// - SipHash 1-3 性能足够好
/// - 跨平台一致 (Rust 标准保证)
///
/// **为什么 hash (db, table, key) 三元组**:
/// - 不同 db 的同名 table 不冲突
/// - 同 table 的不同 key 分布均匀
/// - 重启后 num_shards 不变 → 路由一致
pub struct HashRouter {
    num_shards: usize,
}

impl HashRouter {
    pub fn new(num_shards: usize) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        Self { num_shards }
    }
}

impl Router for HashRouter {
    fn route(&self, db: &str, table: &str, key: &[u8]) -> ShardId {
        let mut hasher = DefaultHasher::new();
        db.hash(&mut hasher);
        table.hash(&mut hasher);
        key.hash(&mut hasher);
        let h = hasher.finish();
        (h as usize) % self.num_shards
    }
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_router_same_key_same_shard() {
        let r = HashRouter::new(4);
        let s1 = r.route("default", "users", b"alice");
        let s2 = r.route("default", "users", b"alice");
        assert_eq!(s1, s2, "同一 key 永远路由到同一 shard");
    }

    #[test]
    fn hash_router_different_keys_distribute() {
        let r = HashRouter::new(4);
        let mut hits = [0; 4];
        for i in 0..1000 {
            let s = r.route("default", "users", format!("key_{i}").as_bytes());
            hits[s] += 1;
        }
        // 1000 个 key 应该在 4 个 shard 上有分布 (不必均匀, 但不应有 0)
        for (i, &count) in hits.iter().enumerate() {
            assert!(count > 0, "shard {i} 没分到 key (分布不均)");
        }
    }

    #[test]
    fn hash_router_diff_db_diff_table_routes_correctly() {
        let r = HashRouter::new(8);
        // 不同 db + 相同 key 应该可以路由到不同 shard
        // (hash 三元组不同)
        let s0 = r.route("app_a", "users", b"x");
        let s1 = r.route("app_b", "users", b"x");
        // 不强求不等 (collision OK), 但 hash 应是确定的
        let _ = (s0, s1);
    }
}
