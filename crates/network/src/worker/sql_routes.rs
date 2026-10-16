//! ⭐ ORM-B2: 进程级共享 SQL 路由缓存 (跨 worker 跨 SQL 门面单例).
//!
//! `SqlSharedRoutes` 持有: 路由 bloom (db,table,iid → per-shard 只增 bloom)、
//! 本进程 CREATE 表集合、DDL epoch、外键反向引用 (FMT_VER 8 级联删除用)、
//! 集群控制面 (建库 2PC). 拆分自 mod.rs (2026-08, 大文件解耦).

use std::collections::HashMap;
use storage::schema::TableSchema;

/// 路由条目: per-shard 只增 bloom 组 (Arc 克隆锁外读写).
pub type RouteBlooms = std::sync::Arc<Vec<storage::index_bloom::IndexBloom>>;
type FkIncomingKey = (std::sync::Arc<str>, String);
type FkIncomingMap = HashMap<FkIncomingKey, Vec<FkIncoming>>;

/// ⭐ ORM-B2: 进程级共享路由缓存 (跨 worker 跨 SQL 门面单例).
/// bloom 本体原子无锁; RwLock 仅保护 map 结构 (读取克隆 Arc 锁外操作,
/// 写仅 DDL 低频). created_here/routes **必须**进程级 — per-worker 会因
/// INSERT 分散到多 worker 产生假阴性漏行.
pub struct SqlSharedRoutes {
    /// (db, table, iid) → per-shard 只增 bloom (仅 created_here 的表).
    pub(crate) routes: std::sync::RwLock<HashMap<(String, String, u32), RouteBlooms>>,
    /// 本进程内 CREATE 的表 (路由缓存启用条件; 语义从"本 worker"平移到"本进程").
    pub(crate) created_here: std::sync::RwLock<std::collections::HashSet<(String, String)>>,
    /// DDL 世代 (DROP 时 +1; worker 每语句比对, 变化即清 per-worker schema 缓存).
    pub(crate) ddl_epoch: std::sync::atomic::AtomicU64,
    /// 观测: 等值查询被候选剪枝的次数 (fanout < num_shards).
    pub(crate) route_pruned: std::sync::atomic::AtomicU64,
    /// 观测: 等值查询零任务短路的次数 (候选空, 直接回空结果).
    pub(crate) route_bypassed: std::sync::atomic::AtomicU64,
    /// ⭐ PG 兼容: 集群控制面 (建库 2PC). worker 启动后由 main 注入.
    cluster_ctl: std::sync::RwLock<Option<std::sync::Arc<shard_manager::ShardManager>>>,
    /// ⭐ PG 兼容 (FMT_VER 8): 外键反向引用 — (db, ref_table) → 引用它的表.
    /// 由 CREATE TABLE 注册 / DROP TABLE 移除; 级联删除按此分发.
    fk_incoming: std::sync::RwLock<FkIncomingMap>,
    /// ⭐ 审计 P2 (2026-10-17): 认证失败限速 (跨门面/跨 worker 共享一把闸).
    /// 放这里是因为 `SqlSharedRoutes` 本身就是"同一数据集群的进程级共享状态"
    /// 容器 (main/测试 每逻辑集群一个, 传给同数据的全部门面).
    pub(crate) auth_throttle: crate::worker::auth_throttle::AuthThrottle,
}

/// ⭐ 审计 P2 (2026-10-17): 读锁 — 毒化不 panic.
///
/// 这些锁保护的都是缓存/注册表 (routes/created_here/fk_incoming/cluster_ctl),
/// 毒化后数据最多停在一次未完成的结构更新上 (下次写入/查询自然修正); 若按
/// `expect` 直接 panic, 一个持锁 panic 会连带杀死**全部** SQL 门面 worker
/// (`SqlSharedRoutes` 是进程级单例), 也让语句级 panic 隔离失去意义.
pub(crate) fn rlock<T>(l: &std::sync::RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// ⭐ 审计 P2 (2026-10-17): 写锁 — 毒化不 panic (见 `rlock`).
pub(crate) fn wlock<T>(l: &std::sync::RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Default for SqlSharedRoutes {
    fn default() -> Self {
        Self {
            routes: std::sync::RwLock::new(HashMap::new()),
            created_here: std::sync::RwLock::new(std::collections::HashSet::new()),
            ddl_epoch: std::sync::atomic::AtomicU64::new(0),
            route_pruned: std::sync::atomic::AtomicU64::new(0),
            route_bypassed: std::sync::atomic::AtomicU64::new(0),
            cluster_ctl: std::sync::RwLock::new(None),
            fk_incoming: std::sync::RwLock::new(HashMap::new()),
            auth_throttle: Default::default(),
        }
    }
}

impl SqlSharedRoutes {
    /// ⭐ PG 兼容: 注入集群控制面 (建库 2PC). 未注入时 CREATE DATABASE 报错.
    pub fn set_cluster_ctl(&self, mgr: std::sync::Arc<shard_manager::ShardManager>) {
        *wlock(&self.cluster_ctl) = Some(mgr);
    }

    /// 取集群控制面 (None = 未注入, 测试/无管理面场景).
    pub fn cluster_ctl(&self) -> Option<std::sync::Arc<shard_manager::ShardManager>> {
        rlock(&self.cluster_ctl).clone()
    }

    /// ⭐ PG 兼容 (FMT_VER 8): CREATE TABLE 注册外键反向引用.
    pub fn register_fks(&self, db: &str, table: &str, schema: &TableSchema) {
        if schema.fks.is_empty() {
            return;
        }
        let mut m = wlock(&self.fk_incoming);
        for fk in &schema.fks {
            m.entry((std::sync::Arc::from(db), fk.ref_table.clone()))
                .or_default()
                .push(FkIncoming {
                    table: table.to_string(),
                    col: schema.columns[fk.col as usize].name.clone(),
                    action: fk.on_delete,
                });
        }
    }

    /// ⭐ PG 兼容 (FMT_VER 8): DROP TABLE 移除外键反向引用.
    pub fn unregister_fks(&self, db: &str, table: &str) {
        let mut m = wlock(&self.fk_incoming);
        m.remove(&(std::sync::Arc::from(db), table.to_string()));
        m.retain(|_, v| {
            v.retain(|i| i.table != table);
            !v.is_empty()
        });
    }

    /// ⭐ PG 兼容 (FMT_VER 8): 反向引用 — 谁引用了 `ref_table` (级联删除分发用).
    pub fn incoming_fks(&self, db: &str, ref_table: &str) -> Vec<FkIncoming> {
        rlock(&self.fk_incoming)
            .get(&(std::sync::Arc::from(db), ref_table.to_string()))
            .cloned()
            .unwrap_or_default()
    }
}

/// ⭐ PG 兼容 (FMT_VER 8): 外键反向引用条目 (表 → 引用它的表/列/动作).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FkIncoming {
    pub table: String,
    pub col: String,
    pub action: storage::schema::FkAction,
}

/// 进程级实例构造 (main/测试 每逻辑集群一个, 传给同数据的全部 SQL 门面).
pub fn new_sql_shared() -> std::sync::Arc<SqlSharedRoutes> {
    std::sync::Arc::new(SqlSharedRoutes::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ⭐ 审计 P2 (2026-10-17): 锁中毒后访问不得 panic.
    ///
    /// 旧实现 `expect("... lock")`: 任意持锁 panic 之后, 该进程级单例的每次
    /// 访问都 panic → 拖死全部 SQL 门面 worker (语句级 panic 隔离被架空).
    #[test]
    fn poisoned_lock_access_does_not_panic() {
        let sh = SqlSharedRoutes::default();
        // 制造毒化: 持写锁时 panic (guard 在 unwind 时 drop → 锁标记为毒化)
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = sh.cluster_ctl.write().unwrap();
            panic!("poison on purpose");
        }));
        assert!(r.is_err(), "内层 panic 应被 catch_unwind 捕获");

        // 毒后读写都不得 panic (恢复 guard 继续用)
        assert!(sh.cluster_ctl().is_none(), "毒后读锁仍可用");
        sh.unregister_fks("db", "t"); // 写锁路径
        assert!(sh.incoming_fks("db", "t").is_empty(), "毒后读写均可用");
    }
}
