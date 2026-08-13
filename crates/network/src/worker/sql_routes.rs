//! ⭐ ORM-B2: 进程级共享 SQL 路由缓存 (跨 worker 跨 SQL 门面单例).
//!
//! `SqlSharedRoutes` 持有: 路由 bloom (db,table,iid → per-shard 只增 bloom)、
//! 本进程 CREATE 表集合、DDL epoch、外键反向引用 (FMT_VER 8 级联删除用)、
//! 集群控制面 (建库 2PC). 拆分自 mod.rs (2026-08, 大文件解耦).

use std::collections::HashMap;
use storage::schema::TableSchema;

/// 路由条目: per-shard 只增 bloom 组 (Arc 克隆锁外读写).
pub type RouteBlooms = std::sync::Arc<Vec<storage::index_bloom::IndexBloom>>;

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
    fk_incoming: std::sync::RwLock<HashMap<(std::sync::Arc<str>, String), Vec<FkIncoming>>>,
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
        }
    }
}

impl SqlSharedRoutes {
    /// ⭐ PG 兼容: 注入集群控制面 (建库 2PC). 未注入时 CREATE DATABASE 报错.
    pub fn set_cluster_ctl(&self, mgr: std::sync::Arc<shard_manager::ShardManager>) {
        *self.cluster_ctl.write().expect("cluster_ctl lock") = Some(mgr);
    }

    /// 取集群控制面 (None = 未注入, 测试/无管理面场景).
    pub fn cluster_ctl(&self) -> Option<std::sync::Arc<shard_manager::ShardManager>> {
        self.cluster_ctl.read().expect("cluster_ctl lock").clone()
    }

    /// ⭐ PG 兼容 (FMT_VER 8): CREATE TABLE 注册外键反向引用.
    pub fn register_fks(&self, db: &str, table: &str, schema: &TableSchema) {
        if schema.fks.is_empty() {
            return;
        }
        let mut m = self.fk_incoming.write().expect("fk_incoming lock");
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
        let mut m = self.fk_incoming.write().expect("fk_incoming lock");
        m.remove(&(std::sync::Arc::from(db), table.to_string()));
        m.retain(|_, v| {
            v.retain(|i| i.table != table);
            !v.is_empty()
        });
    }

    /// ⭐ PG 兼容 (FMT_VER 8): 反向引用 — 谁引用了 `ref_table` (级联删除分发用).
    pub fn incoming_fks(&self, db: &str, ref_table: &str) -> Vec<FkIncoming> {
        self.fk_incoming
            .read()
            .expect("fk_incoming lock")
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
