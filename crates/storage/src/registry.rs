//! T11 DbRegistry: 多 db / 多表的内存缓存 + 写穿透 catalog 管理.
//!
//! ## 设计 (来自 plan §Task 11)
//!
//! - **三层目录**: MetaPage (db_name → table_dir_root_vpid) → TableDirectory
//!   (table_name → table_root_vpid) → Table BTree (用户数据)
//! - **Write-through 缓存**: `HashMap<db, HashMap<table, vpid>>` 与 BTree 同步
//! - **崩溃后自动 rebuild**: open 时从 MetaPage 读所有 db, 遍历每个 db 的
//!   TableDirectory 填 HashMap
//! - **PageWriteBatch 强制**: 所有 catalog 写 (create_db / drop_db / create_table
//!   / drop_table) 走 `Pager::write_page` (内部 PageWriteBatch)
//!
//! ## 单线程使用 + 借用约定
//!
//! 与 `Pager` 契约一致, per-shard thread 单线程使用, 无锁.
//!
//! ## **不持有 raw pointer** ⚠️ 关键 (修复 aliasing UB)
//!
//! 早期版本 DbRegistry 内部持 `pager: *mut Pager`, 然后用 `unsafe { &mut *self.pager }`
//! 解引用. 但 caller (e.g. `StorageEngine::create_db`) 同时借了 `&mut self`, 涵盖
//! 整个 StorageEngine, 等于 aliasing `&mut self.pager` (Pager) 和 `&mut self.pager`
//! (Pager) — 触发 `vec::pop` 内 `hint::assert_unchecked` UB.
//!
//! 修复: 移除 raw pointer, 每个方法接受 `&mut Pager` 作为参数. caller 显式传入,
//! borrow checker 静态保证 aliasing 安全.

use std::collections::HashMap;
use std::io;

use page::Vpid;

use crate::btree::BTreeError;
use crate::meta_page::{META_VPID, MetaError, MetaPage};
use crate::pager::Pager;
use crate::table_directory::{TableDirError, TableDirectory};
use crate::types::DbId;

// =====================================================================
// DbRegistryError
// =====================================================================

/// DbRegistry 操作错误类型. 统一 catalog 相关错误.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("meta page error: {0}")]
    Meta(#[from] MetaError),
    #[error("table directory error: {0}")]
    TableDir(#[from] TableDirError),
    #[error("btree error: {0}")]
    BTree(#[from] BTreeError),
    #[error("db not found: {0}")]
    DbNotFound(String),
    #[error("db already exists: {0}")]
    DbAlreadyExists(String),
    #[error("table not found: {0}.{1}")]
    TableNotFound(String, String),
    #[error("bad value size: expected 8 bytes, got {0}")]
    BadValueSize(usize),
    #[error("table operation error: {0}")]
    TableOp(String),
    #[error("non-utf8 name")]
    NonUtf8Name,
}

impl From<page::PageError> for RegistryError {
    fn from(e: page::PageError) -> Self {
        RegistryError::TableOp(format!("{:?}", e))
    }
}

// =====================================================================
// DbHandle
// =====================================================================

/// 内存中一个 db 的句柄. 包含:
/// - `name`: db 名
/// - `table_dir`: TableDirectory BTree handle (单 leaf page)
/// - `tables`: `table_name → table_root_vpid` 的内存缓存 (write-through 镜像)
#[derive(Debug)]
pub struct DbHandle {
    pub name: String,
    pub table_dir: TableDirectory,
    /// `table_name → table_root_vpid` 缓存镜像. 永远与 table_dir BTree 一致.
    pub(crate) tables: HashMap<String, Vpid>,
}

impl DbHandle {
    /// 在 db 中创建一张新表. 返回新表的 root_vpid.
    ///
    /// **write-through 协议**:
    /// 1. 调 `table_dir.create_table(pager, name)` 写 TableDirectory BTree (走 PageWriteBatch)
    /// 2. 在 HashMap 中插入新 entry
    /// 3. 顺序很重要: BTree 是事实源, 缓存永不超前
    pub async fn create_table(
        &mut self,
        pager: &mut Pager,
        name: &str,
    ) -> Result<Vpid, RegistryError> {
        let vpid = self.table_dir.create_table(pager, name).await?;
        self.tables.insert(name.to_string(), vpid);
        Ok(vpid)
    }

    /// 删除 db 中一张表. 返回 true 表示存在并删除.
    pub async fn drop_table(
        &mut self,
        pager: &mut Pager,
        name: &str,
    ) -> Result<bool, RegistryError> {
        let existed = self.table_dir.drop_table(pager, name).await?;
        if existed {
            self.tables.remove(name);
        }
        Ok(existed)
    }

    /// ⭐ T15: 显式更新某个 table 的 root_vpid. 用于 table BTree root split 时
    /// 同步缓存镜像. caller (e.g. `engine::table_put`) 在 `btree_insert` 返回
    /// `Some(new_root)` 时调用本方法.
    ///
    /// **不变量**: `name` 必须已存在在 `tables` 中 (因为是从 BTree 已有 entry
    /// 触发的 root split). 防御性: 不存在时插入新 entry.
    pub fn update_table_root(&mut self, name: &str, new_root_vpid: Vpid) {
        self.tables.insert(name.to_string(), new_root_vpid);
    }

    /// 暴露 `&mut TableDirectory`, 用于 T15 高级操作 (e.g. `update_table` 持久化
    /// 新 root 到 TableDirectory BTree). 内部使用, caller 负责不违反 borrow 规则.
    pub fn table_dir_mut(&mut self) -> &mut TableDirectory {
        &mut self.table_dir
    }

    /// 查表. 返回 Some(vpid) 表示存在, None 表示不存在.
    pub async fn open_table(
        &mut self,
        pager: &mut Pager,
        name: &str,
    ) -> Result<Option<Vpid>, RegistryError> {
        // 缓存命中: 直接返回 (write-through 永不超前)
        if let Some(&v) = self.tables.get(name) {
            return Ok(Some(v));
        }
        // 缓存 miss: 问 BTree, 顺便填缓存
        let v = self.table_dir.get_table(pager, name).await?;
        if let Some(vpid) = v {
            // write-through: cache miss → BTree → 填 HashMap
            self.tables.insert(name.to_string(), vpid);
            return Ok(Some(vpid));
        }
        Ok(None)
    }

    /// 列出所有 table (按 name 升序). 直接走缓存镜像 (write-through 永不落后).
    pub fn list_tables(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.keys().cloned().collect();
        names.sort();
        names
    }

    /// 当前 table 数 (走缓存).
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// 强制同步缓存与 BTree (用于 cache miss 触发后).
    /// 当前实现: 重新从 BTree 读所有表填缓存.
    pub async fn refresh_table_cache(&mut self, pager: &mut Pager) -> Result<(), RegistryError> {
        let names = self.table_dir.list_tables(pager).await?;
        let mut new_cache = HashMap::new();
        for name in names {
            if let Some(vpid) = self.table_dir.get_table(pager, &name).await? {
                new_cache.insert(name, vpid);
            }
        }
        self.tables = new_cache;
        Ok(())
    }
}

// =====================================================================
// DbRegistry
// =====================================================================

/// 整个 StorageEngine 的 db + table 缓存.
///
/// **生命周期**:
/// - `load`: 从 MetaPage + 各 db TableDirectory 重建 HashMap
/// - `create_db / drop_db`: 调 MetaPage BTree + 各 TableDirectory
/// - `open_db`: 拿 DbHandle
/// - `flush`: 把 MetaPage 落盘 (其他 page 已经在 create/drop 时走 PageWriteBatch)
pub struct DbRegistry {
    /// `db_name → DbHandle` 缓存
    dbs: HashMap<String, DbHandle>,
    /// MetaPage 内存镜像 (BTree 写穿透时同时更新)
    meta: MetaPage,
}

impl DbRegistry {
    /// 从 MetaPage + Pager 构造 (recover 之后调用).
    ///
    /// **流程**:
    /// 1. 读 vpid 0 (MetaPage), parse 出所有 db
    /// 2. 对每个 db, 打开 TableDirectory (用 db 对应的 root_vpid)
    /// 3. 读 TableDirectory BTree, 填 tables 缓存
    /// 4. 返回 DbRegistry { dbs, meta }
    pub async fn load(pager: &mut Pager) -> Result<Self, RegistryError> {
        // 1. 读 MetaPage
        let meta_page_bytes = pager.read(META_VPID).await?;
        let meta = MetaPage::load(&*meta_page_bytes)?;

        // 2. 对每个 db, 打开 TableDirectory
        let mut dbs = HashMap::new();
        for (db_name, table_dir_root) in meta.list_dbs() {
            let table_dir = TableDirectory::open(table_dir_root, pager).await?;
            // 3. 读 TableDirectory BTree, 填 tables 缓存
            let table_names = table_dir.list_tables(pager).await?;
            let mut tables = HashMap::new();
            for t in &table_names {
                if let Some(vpid) = table_dir.get_table(pager, t).await? {
                    tables.insert(t.clone(), vpid);
                }
            }
            dbs.insert(
                db_name.clone(),
                DbHandle {
                    name: db_name,
                    table_dir,
                    tables,
                },
            );
        }

        Ok(Self { dbs, meta })
    }

    /// 创建一个新 db.
    ///
    /// **流程**:
    /// 1. 检查 db 名未占用
    /// 2. 创建新 TableDirectory (分配 vpid, 写空 leaf)
    /// 3. 写 MetaPage: db_name → table_dir_root_vpid
    /// 4. 写 HashMap: 插入新 DbHandle
    /// 5. flush MetaPage (走 PageWriteBatch)
    pub async fn create_db(&mut self, pager: &mut Pager, name: &str) -> Result<(), RegistryError> {
        if self.dbs.contains_key(name) {
            return Err(RegistryError::DbAlreadyExists(name.to_string()));
        }

        // 1. 创建新 TableDirectory
        let table_dir = TableDirectory::create_new(pager).await?;
        let table_dir_root = table_dir.root_vpid;

        // 2. 写 MetaPage 内存镜像
        self.meta.add_db(name, table_dir_root)?;

        // 3. flush MetaPage 落盘 (走 PageWriteBatch)
        let meta_page_bytes = self.meta.flush();
        pager.write_page(META_VPID, meta_page_bytes).await?;

        // 4. 写 HashMap (BTree 已是事实源, 缓存永不超前)
        self.dbs.insert(
            name.to_string(),
            DbHandle {
                name: name.to_string(),
                table_dir,
                tables: HashMap::new(),
            },
        );

        Ok(())
    }

    /// 删除一个 db. 不会清理 db 内 table 的 page (孤儿 vpid), 等 LRU 自然驱逐.
    pub async fn drop_db(&mut self, pager: &mut Pager, name: &str) -> Result<(), RegistryError> {
        let _db = self
            .dbs
            .remove(name)
            .ok_or_else(|| RegistryError::DbNotFound(name.to_string()))?;

        // 1. 写 MetaPage 内存镜像
        self.meta.remove_db(name);

        // 2. flush MetaPage 落盘
        let meta_page_bytes = self.meta.flush();
        pager.write_page(META_VPID, meta_page_bytes).await?;

        Ok(())
    }

    /// 拿 db 句柄. 不存在返回 DbNotFound.
    pub fn open_db(&mut self, name: &str) -> Result<&mut DbHandle, RegistryError> {
        self.dbs
            .get_mut(name)
            .ok_or_else(|| RegistryError::DbNotFound(name.to_string()))
    }

    /// 列出所有 db (按 name 升序).
    pub fn list_dbs(&self) -> Vec<String> {
        let mut names: Vec<String> = self.dbs.keys().cloned().collect();
        names.sort();
        names
    }

    /// db 总数.
    pub fn db_count(&self) -> usize {
        self.dbs.len()
    }

    /// ⭐ T12.16 解析 db name → DbId. 走 MetaPage 的 resolver (权威).
    ///
    /// **为什么不直接用 HashMap 映射**: resolver 是 MetaPage 持久化的, 反映
    /// "on-disk 状态"; HashMap 是 write-through 缓存, 写后立即一致. 这里用
    /// resolver 是为了保证语义清晰: 即使 HashMap 和 resolver 临时不一致 (理论上不会发生),
    /// resolver 是事实源.
    pub fn db_id(&self, name: &str) -> Option<DbId> {
        self.meta.resolver().resolve(name)
    }

    /// ⭐ T12.16 反向解析 DbId → db name. 走 resolver.
    ///
    /// **错误**: id 不在 resolver 中返回 None (这种情况 = bug, caller 持有非法 id).
    pub fn db_name(&self, id: DbId) -> Option<String> {
        self.meta.resolver().name(id).map(|s| s.to_string())
    }

    /// 强制 flush: 把 pager 缓冲写盘.
    ///
    /// **调用场景**: 用户调 `engine.flush()` 时.
    /// MetaPage + 各 TableDirectory BTree 已经在 create_db / create_table 时走
    /// PageWriteBatch, 这里主要是触发 nowchunks → disk 落盘.
    pub async fn flush(&mut self, pager: &mut Pager) -> Result<(), RegistryError> {
        pager.flush().await?;
        Ok(())
    }

    /// 当前 MetaPage 镜像 (测试 / 调试用).
    pub fn meta(&self) -> &MetaPage {
        &self.meta
    }
}

// =====================================================================
// Table BTree 操作 (走 btree router, T15 多层 BTree)
// =====================================================================
//
// Table BTree 升级到多层后, table_put/get/delete 走 `crate::btree::*` 路由.
// 单 leaf page 限制消除, 大数据量自动触发 split + 内部节点创建.

/// 写 (key, value) 到 table BTree. table 必须已创建.
///
/// **实现 (T15)**:
/// - 先 `btree_lookup` 查 key 是否存在
/// - 存在 → `btree_update` (原地替换 value, 不触发 split)
/// - 不存在 → `btree_insert` (可能触发 split + 内部节点创建)
///
/// **⭐ 大 value**: `key.len + value.len > INLINE_LIMIT` 时 value 切成
/// 溢出页, leaf item 只存 13B 描述符. **防泄漏不变量**:
/// - 覆盖写成功 → 释放旧链 (旧值是描述符时)
/// - 新链已写但 leaf 提交失败 → 回滚释放新链
///
/// 两个方向都不留孤儿页.
///
/// **PageWriteBatch**: 走 btree 路由的 PageWriteBatch 一次提交.
///
/// **⭐ T15 返回值**: 返回 `Option<new_root_vpid>`. 如果 table BTree root split
/// (树高 +1), caller (e.g. `DbHandle::table_put` / `engine::table_put`) 必须用
/// 新 root 替换 `tables[table_name]` 的映射. 否则后续 lookup / update 还在用旧
/// root, 路径断裂, 数据找不到.
pub async fn table_put(
    pager: &mut Pager,
    table_root_vpid: Vpid,
    key: &[u8],
    value: &[u8],
) -> Result<Option<Vpid>, RegistryError> {
    use crate::btree::{btree_insert, btree_lookup, btree_update};
    use crate::overflow;

    // 1. 查旧值 (存在性判定 + 旧溢出链释放依据)
    let old_stored = btree_lookup(pager, table_root_vpid, key).await?;

    // 2. 编码 stored value: 大 value → 溢出链 + 13B 描述符
    let descriptor;
    let stored: &[u8] = if overflow::needs_overflow(key.len(), value.len()) {
        descriptor = overflow::write_overflow(pager, value).await?;
        &descriptor
    } else {
        value
    };

    // 3. 提交 leaf item; 失败时回滚新溢出链 (防泄漏: 新链无人引用)
    let commit = if old_stored.is_some() {
        btree_update(pager, table_root_vpid, key, stored)
            .await
            .map(|_| None)
    } else {
        btree_insert(pager, table_root_vpid, key, stored).await
    };
    let new_root = match commit {
        Ok(r) => r,
        Err(e) => {
            if overflow::is_indirect(stored) {
                overflow::free_overflow(pager, stored).await?;
            }
            return Err(e.into());
        }
    };

    // 4. 覆盖写成功: 释放旧溢出链 (防泄漏: 旧链已不被 leaf 引用)
    if let Some(old) = &old_stored
        && overflow::is_indirect(old)
    {
        overflow::free_overflow(pager, old).await?;
    }
    Ok(new_root)
}

/// 读 key 对应的 value. 返回 None 表示 key 不存在.
///
/// **T15**: 走 btree_lookup (travel 跨多层 BTree).
/// **⭐ 大 value**: stored 是 13B 描述符时展开溢出链返回完整 value.
pub async fn table_get(
    pager: &mut Pager,
    table_root_vpid: Vpid,
    key: &[u8],
) -> Result<Option<Vec<u8>>, RegistryError> {
    use crate::btree::btree_lookup;
    use crate::overflow;
    match btree_lookup(pager, table_root_vpid, key).await? {
        Some(stored) if overflow::is_indirect(&stored) => {
            Ok(Some(overflow::read_overflow(pager, &stored).await?))
        }
        other => Ok(other),
    }
}

/// 删除 key. 返回 true 表示 key 存在并删除, false 表示不存在.
///
/// **T15**: 走 btree_delete (travel 跨多层 BTree).
/// **暂不实现 merge**: 删后 leaf 可能空, 留 polish.
/// **⭐ 大 value**: 删除成功且旧值是描述符 → 释放溢出链 (防泄漏).
pub async fn table_delete(
    pager: &mut Pager,
    table_root_vpid: Vpid,
    key: &[u8],
) -> Result<bool, RegistryError> {
    use crate::btree::{btree_delete, btree_lookup};
    use crate::overflow;
    // 先取旧值 (删除后取不到了)
    let old_stored = btree_lookup(pager, table_root_vpid, key).await?;
    let existed = btree_delete(pager, table_root_vpid, key).await?;
    if existed
        && let Some(old) = &old_stored
        && overflow::is_indirect(old)
    {
        overflow::free_overflow(pager, old).await?;
    }
    Ok(existed)
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_error_display_format() {
        let e = RegistryError::DbNotFound("users".to_string());
        assert!(format!("{}", e).contains("users"));
        let e = RegistryError::DbAlreadyExists("foo".to_string());
        assert!(format!("{}", e).contains("foo"));
        let e = RegistryError::TableNotFound("db".to_string(), "tbl".to_string());
        assert!(format!("{}", e).contains("db.tbl"));
    }
}
