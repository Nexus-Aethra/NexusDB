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
    /// ⭐ Phase H: key 已以另一种数据类型存在 (Redis WRONGTYPE 语义).
    /// 消息文本与 Redis 一致, 协议层 encode_error 直接透传.
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
    /// ⭐ Q4 (SQL 索引): schema / row 编解码错误 (类型不符/截断/无 schema).
    #[error("schema error: {0}")]
    Schema(String),
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

    /// ⭐ D1 (分库): 列出所有 db 的 (id, name) — 走 resolver (持久化事实源),
    /// 供上层构建 KV 数字 id ↔ SQL name 双向翻译视图.
    /// 只含**真实已创建**的库 (resolver 内建的 "default" 条目若未 create 则过滤,
    /// 避免 SELECT 选到不存在的库).
    pub fn list_dbs_with_ids(&self) -> Vec<(u32, String)> {
        let mut out: Vec<(u32, String)> = self
            .meta
            .resolver()
            .list()
            .into_iter()
            .filter(|(_, name)| self.dbs.contains_key(*name))
            .map(|(id, name)| (id, name.to_string()))
            .collect();
        out.sort_by_key(|(id, _)| *id);
        out
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
) -> Result<(Option<Vpid>, bool), RegistryError> {
    use crate::btree::{btree_insert_from_leaf, travel_to_leaf_with_path};
    use crate::overflow;
    use page::{leaf_get_with, leaf_update, leaf_update_with};

    // 1. ⭐ 单 travel: 直达 leaf, 同一份 leaf 字节完成 旧值窥视 + 原地更新
    //    (旧实现 lookup + update 各 travel 一次, 且旧值整值物化只为判存在).
    let (leaf_vpid, mut leaf_bytes, mut path) =
        travel_to_leaf_with_path(pager, table_root_vpid, key).await?;

    // The overwhelmingly common RESP overwrite has an inline value.  Combine
    // its old-value inspection and leaf update into one PageIndex load / item
    // scan.  Overflow values retain the conservative path below because their
    // new chain must be allocated asynchronously after inspecting the old one.
    if !overflow::needs_overflow(key.len(), value.len()) {
        let old_desc = leaf_update_with(&mut *leaf_bytes, key, value, |old| {
            if overflow::is_indirect(old) {
                let descriptor: [u8; overflow::DESCRIPTOR_LEN] =
                    old.try_into().expect("13B descriptor");
                Some(descriptor)
            } else {
                None
            }
        })?;
        match old_desc {
            Some(old_desc) => {
                let mut batch = pager.new_write_batch();
                batch.add(leaf_vpid, leaf_bytes);
                if let Err(e) = batch.submit(pager).await {
                    return Err(e.into());
                }
                if let Some(old) = old_desc {
                    overflow::free_overflow(pager, &old).await?;
                }
                return Ok((None, true));
            }
            None => {
                let new_root = btree_insert_from_leaf(
                    pager,
                    table_root_vpid,
                    key,
                    value,
                    leaf_vpid,
                    leaf_bytes,
                    &mut path,
                )
                .await?;
                return Ok((new_root, false));
            }
        }
    }

    // 2. 旧值窥视: 只取存在性 + 溢出描述符 (13B), 不物化 inline 大值
    let old_desc: Option<Option<[u8; overflow::DESCRIPTOR_LEN]>> =
        leaf_get_with(&leaf_bytes[..], key, |v| {
            if overflow::is_indirect(v) {
                Some(v.try_into().expect("13B descriptor"))
            } else {
                None
            }
        });

    // 3. 编码 stored value: 大 value → 溢出链 + 13B 描述符
    let descriptor;
    let stored: &[u8] = if overflow::needs_overflow(key.len(), value.len()) {
        descriptor = overflow::write_overflow(pager, value).await?;
        &descriptor
    } else {
        value
    };

    // 4. 提交 leaf item; 失败时回滚新溢出链 (防泄漏: 新链无人引用)
    let commit: Result<Option<Vpid>, crate::btree::BTreeError> = if old_desc.is_some() {
        // 已存在: 原地 leaf_update + 写回 (无第二次 travel)
        match leaf_update(&mut *leaf_bytes, key, stored) {
            Ok(_) => {
                let mut batch = pager.new_write_batch();
                batch.add(leaf_vpid, leaf_bytes);
                match batch.submit(pager).await {
                    Ok(_) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            Err(e) => {
                crate::page_pool::recycle(leaf_bytes);
                Err(e.into())
            }
        }
    } else {
        // 不存在: insert (可能 split 传播, 低频路径, 复用现有 btree_insert)
        btree_insert_from_leaf(
            pager,
            table_root_vpid,
            key,
            stored,
            leaf_vpid,
            leaf_bytes,
            &mut path,
        )
        .await
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

    // 5. 覆盖写成功: 释放旧溢出链 (防泄漏: 旧链已不被 leaf 引用)
    if let Some(Some(old)) = old_desc {
        overflow::free_overflow(pager, &old).await?;
    }
    // ⭐ M3-1: 返回 (new_root, existed) — existed 供 engine 维护每表近似行数
    // (旧值窥视 old_desc 已存在, 零额外 IO).
    Ok((new_root, old_desc.is_some()))
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

/// ⭐ 零拷贝版 table_get: 命中时以 `&[u8]` 借用回调.
///
/// **注意**: 回调收到的是 leaf page 内的 stored value (可能为 13B 溢出描述符).
/// 调用方需自行判断 `overflow::is_indirect` 并处理溢出链 (如需完整 value).
/// 对于存在性判定 / 长度窥视 / 非 overflow 值的即时处理, 直接用回调零拷贝.
pub async fn table_get_with<R>(
    pager: &mut Pager,
    table_root_vpid: Vpid,
    key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
) -> Result<Option<R>, RegistryError> {
    use crate::btree::btree_lookup_with;
    let result = btree_lookup_with(pager, table_root_vpid, key, f).await?;
    Ok(result)
}

/// ⭐ 存在性判定: 仅检查 key 是否存在, 不物化 value (零 alloc).
///
/// **用途**: SETNX / MSETNX / EXISTS 等只需 Some/None 的场景.
/// 比调用 `table_get` 然后丢弃 value 省一次 `to_vec` 分配.
pub async fn table_exists(
    pager: &mut Pager,
    table_root_vpid: Vpid,
    key: &[u8],
) -> Result<bool, RegistryError> {
    use crate::btree::btree_lookup_with;
    let result = btree_lookup_with(pager, table_root_vpid, key, |_| ()).await?;
    Ok(result.is_some())
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
// ⭐ 批量 API: MGET/MSET (LeafGuide 区间复用, 同 leaf 免重复 travel)
// =====================================================================

/// ⭐ 批量读: `btree_lookup_many` (区间复用) + 溢出描述符逐个展开.
/// 结果按原输入顺序.
pub async fn table_get_many(
    pager: &mut Pager,
    table_root_vpid: Vpid,
    keys: &[&[u8]],
) -> Result<Vec<Option<Vec<u8>>>, RegistryError> {
    use crate::btree::btree_lookup_many;
    use crate::overflow;
    let (mut results, _travels) = btree_lookup_many(pager, table_root_vpid, keys).await?;
    for slot in results.iter_mut() {
        if let Some(stored) = slot
            && overflow::is_indirect(stored)
        {
            *slot = Some(overflow::read_overflow(pager, stored).await?);
        }
    }
    Ok(results)
}

/// ⭐ Phase R: 前缀范围扫描. 以 (physical_key, stored_value) 借用回调
/// 遍历所有以 `prefix` 开头的行; 回调 `Break` 早停 (limit).
///
/// **注**: value 可能是溢出描述符 (大 field value); 本层不展开
/// (回调是同步的, 无法 async 读溢出页), 由上层 (Hash/Set...) 先收集
/// (key, stored) 再在扫描外 async 展开. physical_key 含完整编码前缀,
/// 上层用 `keyspace::split_data` 剥出 suffix.
pub async fn table_scan_prefix<F: FnMut(&[u8], &[u8]) -> core::ops::ControlFlow<()>>(
    pager: &mut Pager,
    table_root_vpid: Vpid,
    prefix: &[u8],
    f: &mut F,
) -> Result<(), RegistryError> {
    crate::btree::btree_scan(pager, table_root_vpid, prefix, f)
        .await
        .map_err(Into::into)
}

/// ⭐ Q4 (SQL 索引): 带起点的前缀扫描 — `start >= prefix`, 用于范围查询
/// 跳过下界之前的行 (上界由回调 Break 早停).
pub async fn table_scan_range<F: FnMut(&[u8], &[u8]) -> core::ops::ControlFlow<()>>(
    pager: &mut Pager,
    table_root_vpid: Vpid,
    start: &[u8],
    prefix: &[u8],
    f: &mut F,
) -> Result<(), RegistryError> {
    crate::btree::btree_scan_from(pager, table_root_vpid, start, prefix, f)
        .await
        .map_err(Into::into)
}

/// ⭐ 批量写: 排序迭代 + LeafGuide 区间复用 — 同 leaf 的多个 key 在
/// 同一份 leaf 字节上连续 update/insert, 一次 batch 提交.
///
/// **防泄漏不变量** (与单 key table_put 一致):
/// - 旧溢出链: 记入 pending 列表, **所在 leaf 提交成功后**才释放
///   (提交前释放 → 盘上 leaf 仍指向已墓碑的链 = 丢数据)
/// - 新溢出链: 所在 leaf 提交失败 → 全部回滚释放
///
/// **guide 失效保守处理**: PageFull / miss → 提交当前累积 leaf, 退化
/// 单 key `table_put` (可能 split, root 变化则后续用新 root), guide 丢弃重建.
///
/// 同 key 批内重复: 稳定排序保持原相对顺序, 后者覆盖前者 (Redis MSET 语义).
pub async fn table_put_many(
    pager: &mut Pager,
    table_root_vpid: Vpid,
    pairs: &[(Vec<u8>, &[u8])],
) -> Result<Option<Vpid>, RegistryError> {
    use crate::btree::{LeafGuide, travel_to_leaf_guided};
    use crate::overflow;
    use page::{PAGE_SIZE, leaf_get_with, leaf_insert, leaf_update, leaf_update_with};

    if pairs.is_empty() {
        return Ok(None);
    }
    let mut order: Vec<usize> = (0..pairs.len()).collect();
    order.sort_by(|&a, &b| pairs[a].0.cmp(&pairs[b].0)); // stable: 同 key 保持原序

    let mut cur_root = table_root_vpid;
    let mut root_changed = false;

    // 当前累积 leaf: (guide, bytes, dirty)
    let mut cur: Option<(LeafGuide, Box<[u8; PAGE_SIZE]>, bool)> = None;
    // 本 leaf 提交成功后待释放的旧链 / 失败时待回滚的新链
    let mut pending_free_old: Vec<[u8; overflow::DESCRIPTOR_LEN]> = Vec::new();
    let mut uncommitted_new: Vec<[u8; overflow::DESCRIPTOR_LEN]> = Vec::new();

    // 提交当前累积 leaf. Ok → 释放旧链; Err → 回滚新链.
    async fn flush_cur(
        pager: &mut Pager,
        cur: &mut Option<(LeafGuide, Box<[u8; PAGE_SIZE]>, bool)>,
        pending_free_old: &mut Vec<[u8; overflow::DESCRIPTOR_LEN]>,
        uncommitted_new: &mut Vec<[u8; overflow::DESCRIPTOR_LEN]>,
    ) -> Result<(), RegistryError> {
        let Some((guide, bytes, dirty)) = cur.take() else {
            return Ok(());
        };
        if !dirty {
            crate::page_pool::recycle(bytes);
            debug_assert!(pending_free_old.is_empty() && uncommitted_new.is_empty());
            return Ok(());
        }
        let mut batch = pager.new_write_batch();
        batch.add(guide.leaf_vpid, bytes);
        match batch.submit(pager).await {
            Ok(_) => {
                uncommitted_new.clear(); // 新链已被持久 leaf 引用
                for old in pending_free_old.drain(..) {
                    overflow::free_overflow(pager, &old).await?;
                }
                Ok(())
            }
            Err(e) => {
                // 回滚: 未提交 leaf 引用的新链全部释放 (防泄漏)
                for d in uncommitted_new.drain(..) {
                    overflow::free_overflow(pager, &d).await?;
                }
                pending_free_old.clear();
                Err(e.into())
            }
        }
    }

    for &i in &order {
        let key = &pairs[i].0;
        let value: &[u8] = pairs[i].1;

        // guide miss → 提交当前 leaf, travel 新 leaf
        let hit = matches!(&cur, Some((g, _, _)) if g.contains(key));
        if !hit {
            flush_cur(pager, &mut cur, &mut pending_free_old, &mut uncommitted_new).await?;
            let (g, b) = travel_to_leaf_guided(pager, cur_root, key).await?;
            cur = Some((g, b, false));
        }

        // 编码 stored (大 value → 溢出链)
        let descriptor;
        let stored: &[u8] = if overflow::needs_overflow(key.len(), value.len()) {
            descriptor = overflow::write_overflow(pager, value).await?;
            &descriptor
        } else {
            value
        };

        let (_, leaf_bytes, dirty) = cur.as_mut().expect("cur filled above");
        // Inline overwrite is the common SET case. Locate once and update the
        // same item, rather than leaf_get_with + leaf_update doing two full
        // PageIndex/segment walks. Indirect values keep the conservative
        // inspect-then-write flow because their new overflow chain is created
        // after inspection.
        let (old_desc, already_updated) = if !overflow::is_indirect(stored) {
            match leaf_update_with(&mut leaf_bytes[..], key, stored, |v| {
                if overflow::is_indirect(v) {
                    Some(v.try_into().expect("13B descriptor"))
                } else {
                    None
                }
            })? {
                Some(old) => (Some(old), true),
                None => (None, false),
            }
        } else {
            (
                leaf_get_with(&leaf_bytes[..], key, |v| {
                    if overflow::is_indirect(v) {
                        Some(v.try_into().expect("13B descriptor"))
                    } else {
                        None
                    }
                }),
                false,
            )
        };

        let apply = if already_updated {
            Ok(())
        } else if old_desc.is_some() {
            leaf_update(&mut leaf_bytes[..], key, stored).map(|_| ())
        } else {
            leaf_insert(&mut leaf_bytes[..], key, stored)
        };
        match apply {
            Ok(()) => {
                *dirty = true;
                if overflow::is_indirect(stored) {
                    uncommitted_new.push(stored.try_into().expect("13B"));
                }
                if let Some(Some(old)) = old_desc {
                    pending_free_old.push(old);
                }
            }
            Err(_page_full) => {
                // 退化: 提交累积 leaf 后走单 key 路径 (split 传播),
                // root 可能变化; guide 已在 flush_cur 丢弃.
                flush_cur(pager, &mut cur, &mut pending_free_old, &mut uncommitted_new)
                    .await?;
                // stored 若为新溢出链, 交给单 key路径? 单 key table_put 会重新
                // write_overflow → 先回滚本次新链, 避免双写泄漏.
                if overflow::is_indirect(stored) {
                    overflow::free_overflow(pager, stored).await?;
                }
                if let (Some(new_root), _) = table_put(pager, cur_root, key, value).await? {
                    cur_root = new_root;
                    root_changed = true;
                }
            }
        }
    }
    flush_cur(pager, &mut cur, &mut pending_free_old, &mut uncommitted_new).await?;

    Ok(if root_changed { Some(cur_root) } else { None })
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
