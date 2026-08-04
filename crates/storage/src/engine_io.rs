// ⭐ 解耦 2026-08: StorageEngine 数据读写物理层 (从 engine.rs 拆出).
// 职责: table_put/get/delete、物理页读写 (put_physical/get_physical)、批量读写.
use super::engine::{StorageEngine, StorageError};
use crate::meta_cache::MetaCache;
use crate::meta_page::{MetaPage, META_PID, META_VPID};
use crate::registry::{DbRegistry, RegistryError};
use crate::types::DbId;
use crate::TableDirectory;
use std::io;

impl StorageEngine {
    /// 写 (key, value) 到指定 table.
    ///
    /// **⭐ T15**: 如果 table BTree 内部发生 root split:
    /// 1. 新的 root_vpid 写回 `DbHandle.tables[table]` (内存缓存)
    /// 2. 通过 `TableDirectory::update_table` 持久化到 TableDirectory BTree
    ///
    /// 否则 reopen 后 DbRegistry::load 读 TableDirectory 拿到旧 root,
    /// 旧 root 只含 split 左半数据, 找不到右半的 key.
    pub async fn table_put(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), RegistryError> {
        // ⭐ Phase K: user key 统一编码为 [S][klen][key]
        // ⭐ U2: SET 覆盖异类旧值 — 若 key 当前是复合类型先 purge (Redis 语义).
        // ⭐ PERF (F49): 表内从未写过复合类型 → 不可能有旧复合行, 跳过探测.
        if self.has_composite(db, table) {
            self.purge_composite_if_any(db, table, key).await?;
        }
        let ek = crate::keyspace::encode_string(key);
        // ⭐ M3-1: 用户行写入 → 行数 +1 (覆盖不加; existed 由 put_physical 返回,
        // 复合类型/索引条目写不走此处不计行数)
        let existed = self.put_physical(db, table, &ek, value).await?;
        if !existed {
            *self
                .row_counts
                .entry((db.to_string(), table.to_string()))
                .or_insert(0) += 1;
        }
        Ok(())
    }

    // =================================================================
    // ⭐ Phase H: 物理 key 层辅助 (复合结构 op 用; String 入口是其薄封装).
    // pkey = 已编码的 BTree 物理 key (keyspace::encode_*).
    // =================================================================

    /// 按物理 key 写入 (含 root split 的 TableDirectory 回写, 与 table_put 同逻辑).
    /// 返回 existed (key 是否已存在), 供**用户行入口** (table_put / row_put 主行) 维护行数.
    pub(crate) async fn put_physical(
        &mut self,
        db: &str,
        table: &str,
        pkey: &[u8],
        value: &[u8],
    ) -> Result<bool, RegistryError> {
        let db_handle = self.registry.open_db(db)?;
        let table_vpid = db_handle
            .open_table(&mut self.pager, table)
            .await?
            .ok_or_else(|| RegistryError::TableNotFound(db.to_string(), table.to_string()))?;

        let (new_root, existed) =
            crate::registry::table_put(&mut self.pager, table_vpid, pkey, value).await?;

        // ⭐ WAL (F60): 成功路径记录结果态 (重放幂等)
        if let Some(w) = self.wal.as_mut() {
            w.append_put(db, table, pkey, value);
        }

        // ⭐ T15: root split 时同步 TableDirectory BTree + 缓存
        if let Some(new_root) = new_root {
            let db_handle = self.registry.open_db(db)?;
            db_handle
                .table_dir_mut()
                .update_table(&mut self.pager, table, new_root)
                .await
                .map_err(RegistryError::from)?;
            let db_handle = self.registry.open_db(db)?;
            db_handle.update_table_root(table, new_root);
        }
        Ok(existed)
    }

    /// 按物理 key 读 (溢出链自动展开).
    pub(crate) async fn get_physical(
        &mut self,
        db: &str,
        table: &str,
        pkey: &[u8],
    ) -> Result<Option<Vec<u8>>, RegistryError> {
        let db_handle = self.registry.open_db(db)?;
        let table_vpid = db_handle
            .open_table(&mut self.pager, table)
            .await?
            .ok_or_else(|| RegistryError::TableNotFound(db.to_string(), table.to_string()))?;
        crate::registry::table_get(&mut self.pager, table_vpid, pkey).await
    }

    /// ⭐ O2: 物理 key 批量读 (LeafGuide 区间复用, 结果按输入序, 溢出展开).
    /// 复合 op 多 field/member 探在从逐条 travel 摊薄为区间复用.
    pub(crate) async fn get_physical_many(
        &mut self,
        db: &str,
        table: &str,
        pkeys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, RegistryError> {
        let db_handle = self.registry.open_db(db)?;
        let table_vpid = db_handle
            .open_table(&mut self.pager, table)
            .await?
            .ok_or_else(|| RegistryError::TableNotFound(db.to_string(), table.to_string()))?;
        crate::registry::table_get_many(&mut self.pager, table_vpid, pkeys).await
    }

    /// ⭐ O2: 物理 key 批量写 (排序 + 同 leaf 一次 batch 提交;
    /// root split 同步 TableDirectory, 与 put_physical 同逻辑).
    pub(crate) async fn put_physical_many(
        &mut self,
        db: &str,
        table: &str,
        pairs: &[(Vec<u8>, &[u8])],
    ) -> Result<(), RegistryError> {
        if pairs.is_empty() {
            return Ok(());
        }
        let db_handle = self.registry.open_db(db)?;
        let table_vpid = db_handle
            .open_table(&mut self.pager, table)
            .await?
            .ok_or_else(|| RegistryError::TableNotFound(db.to_string(), table.to_string()))?;
        let new_root =
            crate::registry::table_put_many(&mut self.pager, table_vpid, pairs).await?;
        // ⭐ WAL (F60): 批量记录 (一次遍历, flush 时共享后续 fsync)
        if let Some(w) = self.wal.as_mut() {
            for (pkey, value) in pairs {
                w.append_put(db, table, pkey, value);
            }
        }
        if let Some(new_root) = new_root {
            let db_handle = self.registry.open_db(db)?;
            db_handle
                .table_dir_mut()
                .update_table(&mut self.pager, table, new_root)
                .await
                .map_err(RegistryError::from)?;
            let db_handle = self.registry.open_db(db)?;
            db_handle.update_table_root(table, new_root);
        }
        Ok(())
    }

    /// 按物理 key 删 (溢出链自动释放). 返回是否存在.
    pub(crate) async fn delete_physical(
        &mut self,
        db: &str,
        table: &str,
        pkey: &[u8],
    ) -> Result<bool, RegistryError> {
        let db_handle = self.registry.open_db(db)?;
        let table_vpid = db_handle
            .open_table(&mut self.pager, table)
            .await?
            .ok_or_else(|| RegistryError::TableNotFound(db.to_string(), table.to_string()))?;
        let existed = crate::registry::table_delete(&mut self.pager, table_vpid, pkey).await?;
        // ⭐ M3-1: 删除成功 → 近似行数 -1 (saturating 防下溢)
        if existed {
            if let Some(c) = self.row_counts.get_mut(&(db.to_string(), table.to_string())) {
                *c = c.saturating_sub(1);
            }
        }
        // ⭐ WAL (F60): 存在才记 (不存在的 delete 重放无意义)
        if existed && let Some(w) = self.wal.as_mut() {
            w.append_del(db, table, pkey);
        }
        Ok(existed)
    }

    /// 读 key 对应 value. 返回 None 表示 key 不存在.
    pub async fn table_get(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, RegistryError> {
        let ek = crate::keyspace::encode_string(key);
        self.get_physical(db, table, &ek).await
    }

    /// ⭐ 批量读 (MGET): LeafGuide 区间复用, 结果按输入顺序.
    pub async fn table_get_many(
        &mut self,
        db: &str,
        table: &str,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, RegistryError> {
        let db_handle = self.registry.open_db(db)?;
        let table_vpid = db_handle
            .open_table(&mut self.pager, table)
            .await?
            .ok_or_else(|| RegistryError::TableNotFound(db.to_string(), table.to_string()))?;
        // ⭐ Phase K: 每个 key 编码为 [S][klen][key] 再交给 registry.
        // 编码后物理序 != 裸 key 序 (klen 前缀), 但 registry 内部按传入的
        // 物理 key 排序走 LeafGuide, 结果按输入索引还原 — 一致性成立.
        let encoded: Vec<Vec<u8>> = keys.iter().map(|k| crate::keyspace::encode_string(k)).collect();
        let refs: Vec<&[u8]> = encoded.iter().map(|v| v.as_slice()).collect();
        crate::registry::table_get_many(&mut self.pager, table_vpid, &refs).await
    }

    /// ⭐ 批量写 (MSET): LeafGuide 区间复用, 同 leaf 一次 batch 提交.
    /// root split 时同步 TableDirectory (与 table_put 同逻辑).
    pub async fn table_put_many(
        &mut self,
        db: &str,
        table: &str,
        pairs: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), RegistryError> {
        let db_handle = self.registry.open_db(db)?;
        let table_vpid = db_handle
            .open_table(&mut self.pager, table)
            .await?
            .ok_or_else(|| RegistryError::TableNotFound(db.to_string(), table.to_string()))?;

        // ⭐ U2: MSET 覆盖异类旧值 — 逐 key purge 复合旧值 (与 SET 一致).
        for (k, _) in pairs {
            self.purge_composite_if_any(db, table, k).await?;
        }
        // ⭐ Phase K: 编码 key (value 借用不动, 避免大 value 拷贝).
        let encoded: Vec<(Vec<u8>, &[u8])> = pairs
            .iter()
            .map(|(k, v)| (crate::keyspace::encode_string(k), v.as_slice()))
            .collect();
        let new_root =
            crate::registry::table_put_many(&mut self.pager, table_vpid, &encoded).await?;

        // ⭐ M3-1: 批量写近似 +N (覆盖会高估; 近似基数对驱动选择足够)
        *self
            .row_counts
            .entry((db.to_string(), table.to_string()))
            .or_insert(0) += pairs.len() as u64;

        if let Some(new_root) = new_root {
            let db_handle = self.registry.open_db(db)?;
            db_handle
                .table_dir_mut()
                .update_table(&mut self.pager, table, new_root)
                .await
                .map_err(RegistryError::from)?;
            let db_handle = self.registry.open_db(db)?;
            db_handle.update_table_root(table, new_root);
        }
        Ok(())
    }

    /// 删 key. 返回 true 表示存在并删除, false 表示不存在.
    pub async fn table_delete(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<bool, RegistryError> {
        let ek = crate::keyspace::encode_string(key);
        self.delete_physical(db, table, &ek).await
    }

    /// ⭐ M3-1: 近似行数估计 (CBO 连接顺序/访问路径用; None = 无记录, 视为未知/小表).
    pub fn estimate_row_count(&self, db: &str, table: &str) -> Option<u64> {
        self.row_counts
            .get(&(db.to_string(), table.to_string()))
            .copied()
    }

    /// ⭐ M3-4: 索引列近似 distinct 基数 (CBO 选择度; None = 无记录, 视为未知).
    pub fn estimate_distinct(&self, db: &str, table: &str, iid: u32) -> Option<u64> {
        self.distinct_counts
            .get(&(db.to_string(), table.to_string(), iid))
            .copied()
    }

    /// ⭐ M3-5: 索引列 (min, max) 有序字节 (值序比较; 范围选择度/直方图基础; None = 无记录).
    pub fn estimate_range(&self, db: &str, table: &str, iid: u32) -> Option<(Vec<u8>, Vec<u8>)> {
        self.range_counts
            .get(&(db.to_string(), table.to_string(), iid))
            .cloned()
    }

    /// ⭐ M3-1b: 持久化 CBO 统计 (row + distinct + range) 到 stats.bin (best-effort).
    pub(crate) fn save_stats(&self) {
        use std::io::Write;
        let Ok(mut f) = std::fs::File::create(&self.stats_path) else { return };
        let _ = f.write_all(b"NXTST1");
        // row_counts
        let _ = f.write_all(&(self.row_counts.len() as u32).to_le_bytes());
        for ((db, table), n) in &self.row_counts {
            let _ = f.write_all(&(db.len() as u16).to_le_bytes());
            let _ = f.write_all(db.as_bytes());
            let _ = f.write_all(&(table.len() as u16).to_le_bytes());
            let _ = f.write_all(table.as_bytes());
            let _ = f.write_all(&n.to_le_bytes());
        }
        // distinct_counts
        let _ = f.write_all(&(self.distinct_counts.len() as u32).to_le_bytes());
        for ((db, table, iid), n) in &self.distinct_counts {
            let _ = f.write_all(&(db.len() as u16).to_le_bytes());
            let _ = f.write_all(db.as_bytes());
            let _ = f.write_all(&(table.len() as u16).to_le_bytes());
            let _ = f.write_all(table.as_bytes());
            let _ = f.write_all(&iid.to_le_bytes());
            let _ = f.write_all(&n.to_le_bytes());
        }
        // range_counts (min/max)
        let _ = f.write_all(&(self.range_counts.len() as u32).to_le_bytes());
        for ((db, table, iid), (lo, hi)) in &self.range_counts {
            let _ = f.write_all(&(db.len() as u16).to_le_bytes());
            let _ = f.write_all(db.as_bytes());
            let _ = f.write_all(&(table.len() as u16).to_le_bytes());
            let _ = f.write_all(table.as_bytes());
            let _ = f.write_all(&iid.to_le_bytes());
            let _ = f.write_all(&(lo.len() as u16).to_le_bytes());
            let _ = f.write_all(lo);
            let _ = f.write_all(&(hi.len() as u16).to_le_bytes());
            let _ = f.write_all(hi);
        }
    }

    /// ⭐ M3-1b: 加载 stats.bin (open 时; 缺失/损坏 → 空统计, 无碍).
    pub(crate) fn load_stats(&mut self) {
        use std::io::Read;
        let Ok(mut f) = std::fs::File::open(&self.stats_path) else { return };
        let mut buf = Vec::new();
        if f.read_to_end(&mut buf).is_err() || buf.len() < 6 || &buf[..6] != b"NXTST1" {
            return;
        }
        let mut p = 6usize;
        // row_counts
        let Some(n) = st_u32(&buf, &mut p) else { return };
        for _ in 0..n {
            let (Some(db), Some(table), Some(c)) =
                (st_str(&buf, &mut p), st_str(&buf, &mut p), st_u64(&buf, &mut p))
            else { return };
            self.row_counts.insert((db, table), c);
        }
        // distinct_counts
        let Some(n) = st_u32(&buf, &mut p) else { return };
        for _ in 0..n {
            let (Some(db), Some(table), Some(iid), Some(c)) = (
                st_str(&buf, &mut p),
                st_str(&buf, &mut p),
                st_u32(&buf, &mut p),
                st_u64(&buf, &mut p),
            ) else { return };
            self.distinct_counts.insert((db, table, iid), c);
        }
        // range_counts
        let Some(n) = st_u32(&buf, &mut p) else { return };
        for _ in 0..n {
            let (Some(db), Some(table), Some(iid), Some(lo), Some(hi)) = (
                st_str(&buf, &mut p),
                st_str(&buf, &mut p),
                st_u32(&buf, &mut p),
                st_bytes(&buf, &mut p),
                st_bytes(&buf, &mut p),
            ) else { return };
            self.range_counts.insert((db, table, iid), (lo, hi));
        }
    }

    /// 暴露内部 DbRegistry (高级用法: 测试 / 调试).
    pub fn registry_mut(&mut self) -> &mut DbRegistry {
        &mut self.registry
    }

    /// 当前 DbRegistry 的不可变访问.
    pub fn registry(&self) -> &DbRegistry {
        &self.registry
    }

    // =================================================================
    // ⭐ T12.16: 多 db 上下文 API (current_db)
    // =================================================================

    /// 当前 db 的 `DbId`. 默认 0 (= "default" db).
    ///
    /// **含义**: 这是"本 engine 当前在哪个 db"的状态标识. 单 db 模式
    /// 始终是 0; 多 db 模式 ShardManager 显式调用 `use_db` / `set_current_db` 切换.
    ///
    /// **不影响已有的 `db: &str` API**: 已有 API 显式传 db 名, 不依赖 current_db.
    /// current_db 是 ShardManager 等高层模块的"默认 db"标记.
    pub fn current_db(&self) -> DbId {
        self.current_db
    }

    /// 当前 db 的名称 (解析 `current_db` 到 db name).
    ///
    /// **错误**: 如果 current_db 在 resolver 中找不到对应 name, 返回 DbNotFound.
    /// 这种情况理论上不应该发生 (current_db 永远从 resolver 拿), 但作为防御性检查.
    pub fn current_db_name(&self) -> Result<String, RegistryError> {
        self.registry
            .db_name(self.current_db)
            .ok_or_else(|| RegistryError::DbNotFound(format!("db_id={}", self.current_db)))
    }

    /// 按 name 切换当前 db. 返回新 current_db 的 `DbId`.
    ///
    /// **调用场景**: ShardManager 收到 "USE dbname" 类命令, 调此方法切到目标 db.
    /// 如果 db 不存在, 返回 DbNotFound.
    ///
    /// **不会触发 IO**: 只是更新内存中的 current_db 字段 + 解析 db name → id.
    /// 不读不写磁盘.
    pub fn use_db(&mut self, name: &str) -> Result<DbId, RegistryError> {
        let id = self
            .registry
            .db_id(name)
            .ok_or_else(|| RegistryError::DbNotFound(name.to_string()))?;
        self.current_db = id;
        Ok(id)
    }

    /// 按 DbId 切换当前 db. **不**验证 id 存在 (因为 resolver 没有反向 in-memory API,
    /// 走 name 解析更安全). 内部主要用于 ShardManager 已通过 use_db 拿到 id 后,
    /// 序列化场景下直接 set.
    ///
    /// **警告**: caller 应保证 `id` 是有效 DbId (从 `use_db` / `create_db` 返回的).
    /// 非法 id 不会立即报错, 但后续 `current_db_name()` 会返回 DbNotFound.
    pub fn set_current_db(&mut self, id: DbId) {
        self.current_db = id;
    }
}

// Drop: 不隐式 flush, 但确保资源释放不 panic

impl Drop for StorageEngine {
    fn drop(&mut self) {
        // 不隐式 flush (因 flush 可能 IO 阻塞, Drop 不应阻塞).
        // 注意: 调用方应负责 close() 或显式 flush().
    }
}

// MetaPage 初始化: 写空 MetaPage 到 chunk 0 page 0 (T9 集成)

// ===== ⭐ M3-1b: stats.bin 解码辅助 =====
fn st_u16(b: &[u8], p: &mut usize) -> Option<u16> {
    if *p + 2 > b.len() {
        return None;
    }
    let v = u16::from_le_bytes([b[*p], b[*p + 1]]);
    *p += 2;
    Some(v)
}
fn st_u32(b: &[u8], p: &mut usize) -> Option<u32> {
    if *p + 4 > b.len() {
        return None;
    }
    let v = u32::from_le_bytes([b[*p], b[*p + 1], b[*p + 2], b[*p + 3]]);
    *p += 4;
    Some(v)
}
fn st_u64(b: &[u8], p: &mut usize) -> Option<u64> {
    if *p + 8 > b.len() {
        return None;
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[*p..*p + 8]);
    *p += 8;
    Some(u64::from_le_bytes(a))
}
fn st_str(b: &[u8], p: &mut usize) -> Option<String> {
    let n = st_u16(b, p)? as usize;
    if *p + n > b.len() {
        return None;
    }
    let s = String::from_utf8_lossy(&b[*p..*p + n]).to_string();
    *p += n;
    Some(s)
}
fn st_bytes(b: &[u8], p: &mut usize) -> Option<Vec<u8>> {
    let n = st_u16(b, p)? as usize;
    if *p + n > b.len() {
        return None;
    }
    let v = b[*p..*p + n].to_vec();
    *p += n;
    Some(v)
}

/// 写一个空的 MetaPage 到 block_dir/000001.block 的 chunk 0 page 0 (offset 0),
/// 并在 MetaCache 中登记 vpid 0 → META_PID.
///
/// **调用场景**: `StorageEngine::open` 时, 如果 recover 发现 vpid 0 未映射 (全新库),
/// 则认为 MetaPage 还没初始化, 主动写一个空 MetaPage 落盘 + 注册映射.
///
/// **不更新 vpid_alloc**: 调用方负责设置 `vpid_alloc` 起点 (本函数不感知 alloc).
pub(crate) fn init_meta_page(block_dir: &std::path::Path, meta: &mut MetaCache) -> io::Result<()> {
    // 1. 构造空 MetaPage 字节
    let meta_page = MetaPage::new_empty();
    let bytes = meta_page.flush();

    // 2. 写盘: block_dir/000001.block, offset 0 (chunk 0 page 0)
    let block_path = block_dir.join("000001.block");
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&block_path)?;
    // use FileExt for write_all_at
    use std::os::unix::fs::FileExt;
    f.write_all_at(&*bytes, 0)?;
    f.sync_all()?;
    drop(f);

    // 3. 在 MetaCache 中登记 vpid 0 → META_PID
    meta.write(META_VPID, META_PID);

    Ok(())
}
