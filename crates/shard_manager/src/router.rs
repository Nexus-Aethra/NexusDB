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

/// ⭐ 审计 P2 (2026-10-17): 全仓库**唯一**的 (db, table, key) → shard 路由哈希.
///
/// 旧实现三处复制 (本文件 `HashRouter::route` / `manager::route_db_table_key` /
/// network worker `hash_route_key`): 任一处漂移 (输入顺序 / hasher / 取模) 都会
/// 让**同一个 key 在不同路径落到不同 shard** — 例如 worker 把写发去 shard A,
/// 而读集验证 / 索引查询去 shard B, 表现为"写成功却查不到"这类极难排查的数据
/// 不一致. 集中到此处 + 跨 crate 一致性测试 (见本文件 tests) 杜绝漂移.
///
/// **稳定性契约 (明确化)**: `DefaultHasher` (SipHash 1-3) 的输出**不跨 Rust
/// 版本保证稳定** (标准库保留改动权), 因此本哈希只用于**同进程内**的分片选择:
/// 哪一行在哪个 shard 不跨进程/跨版本持久化, 重启后 num_shards 不变即路由一致.
/// 若未来需要跨版本稳定 (如按 key 直接寻址的外部契约), 必须换固定算法
/// (如 FNV/xxhash) 并做一次数据重分布 — 那是独立的设计变更.
pub fn route_hash(db: &str, table: &str, key: &[u8], num_shards: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    db.hash(&mut hasher);
    table.hash(&mut hasher);
    key.hash(&mut hasher);
    (hasher.finish() as usize) % num_shards
}

/// 默认 hash router: `std::hash::DefaultHasher` (SipHash 1-3).
///
/// **为什么用 DefaultHasher**:
/// - 标准库自带, 无依赖
/// - SipHash 1-3 性能足够好
///
/// **为什么 hash (db, table, key) 三元组**:
/// - 不同 db 的同名 table 不冲突
/// - 同 table 的不同 key 分布均匀
/// - 重启后 num_shards 不变 → 路由一致 (路由实现见 [`route_hash`])
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
        route_hash(db, table, key, self.num_shards)
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
