//! ⭐ Phase H: 复合数据结构操作 (Hash; Set/List/ZSet 后续同居此模块).
//!
//! 每个复合结构 = meta 行 (subkind 0, 存 `[kind 1B][count u64 LE]`) +
//! N 条 data 行 (subkind 1, 每 field/member 一行), 全部是普通 BTree 行,
//! 与 COW/GC/溢出页体系正交. 编码见 `keyspace`.
//!
//! ## WRONGTYPE 不变量 (v1)
//! - 复合 op 先查 String 行: 存在 → `RegistryError::WrongType`
//!   (SET 覆盖后旧复合行不可见, 与 Redis 可见语义一致; 孤儿行由 DEL 清理)
//! - String GET 的 **miss 路径**查 hash meta (`table_get_typed`):
//!   命中路径零开销, miss 时 +1 点查
//! - `key_delete_any`: 删 String 行 + 探测复合 meta → 范围删全部行
//!   (顺带清 SET 覆盖产生的孤儿行)
//!
//! ## 已知 gap (v1, 文档记录)
//! - SET 覆盖复合 key 后, 旧复合行占空间直到 DEL (不可见但未回收)
//! - meta count 崩溃窗口 (行已落盘、meta 未落盘) 与溢出页孤儿 gap 同级

use std::ops::ControlFlow;

use crate::engine::StorageEngine;
use crate::keyspace as ks;
use crate::registry::RegistryError;

/// meta 行 value: `[kind 1B][count u64 LE]` (9B 定长).
fn enc_meta_val(kind: u8, count: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(9);
    v.push(kind);
    v.extend_from_slice(&count.to_le_bytes());
    v
}

/// 解 meta 行 count (容错: 长度不足按 0).
fn dec_meta_count(v: &[u8]) -> u64 {
    if v.len() < 9 {
        return 0;
    }
    u64::from_le_bytes(v[1..9].try_into().expect("8B"))
}

impl StorageEngine {
    /// key 当前的数据类型 (kind 字节): 先探 String 行, 否则探统一类型 meta 行
    /// `[#]key` 并读其 value 首字节 (kind). None = key 不存在. **2 次点查**.
    pub(crate) async fn kind_of(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<Option<u8>, RegistryError> {
        // ⭐ O2: 先探 `[#]` meta (复合 op 大概率命中, 命中即返 —
        // meta 与 String 行互斥不变量保证正确), miss 再探 String 行.
        // 已存在复合 key 的类型检查 2 → 1 次点查.
        if let Some(v) = self
            .get_physical(db, table, &ks::encode_type_meta(key))
            .await?
        {
            return Ok(v.first().copied());
        }
        Ok(self
            .get_physical(db, table, &ks::encode_string(key))
            .await?
            .map(|_| ks::KIND_STRING))
    }

    /// 复合 op 的 WRONGTYPE 判据: key 以其他类型存在 → Err(WrongType).
    async fn ensure_kind(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        want: u8,
    ) -> Result<(), RegistryError> {
        // ⭐ Q4 (SQL 索引): 有 schema 的表是 row 表, 复合命令一律 WRONGTYPE.
        // get_schema 镜像缓存后为纯内存查表, 不增热路径点查.
        if self.get_schema(db, table).await?.is_some() {
            return Err(RegistryError::WrongType);
        }
        match self.kind_of(db, table, key).await? {
            Some(k) if k != want => Err(RegistryError::WrongType),
            _ => Ok(()),
        }
    }

    /// hash count (统一类型 meta 行; caller 已 ensure_kind=HASH). None = 不存在.
    async fn hash_meta(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<Option<u64>, RegistryError> {
        Ok(self
            .get_physical(db, table, &ks::encode_type_meta(key))
            .await?
            .map(|v| dec_meta_count(&v)))
    }

    // =================================================================
    // Hash ops
    // =================================================================

    /// HSET 多 field: 返回**新增** field 数 (Redis HSET 语义).
    /// value 已带 `[tag][payload]` (与 String 同约定, 溢出页自动).
    /// ⭐ O2: 探在/写入批量化 (LeafGuide 区间复用, 多 field 摊薄树遍历).
    pub async fn hash_set(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        pairs: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_HASH).await?;
        self.mark_composite(db, table); // ⭐ F49: 复合写入口打标
        let fks: Vec<Vec<u8>> = pairs
            .iter()
            .map(|(f, _)| ks::encode_data(ks::KIND_HASH, key, f))
            .collect();
        let refs: Vec<&[u8]> = fks.iter().map(|k| k.as_slice()).collect();
        let existing = self.get_physical_many(db, table, &refs).await?;
        // 批内同 field 重复: 只有首次未在算新增 (后写覆盖前写)
        let mut seen: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
        let mut added = 0u64;
        for (i, (f, _)) in pairs.iter().enumerate() {
            if existing[i].is_none() && seen.insert(f.as_slice()) {
                added += 1;
            }
        }
        let mut writes: Vec<(Vec<u8>, &[u8])> = fks
            .into_iter()
            .zip(pairs.iter().map(|(_, v)| v.as_slice()))
            .collect();
        let meta_val;
        if added > 0 {
            let count = self.hash_meta(db, table, key).await?.unwrap_or(0);
            meta_val = enc_meta_val(ks::KIND_HASH, count + added);
            writes.push((ks::encode_type_meta(key), &meta_val));
        }
        self.put_physical_many(db, table, &writes).await?;
        Ok(added as i64)
    }

    /// HSETNX: field 不存在才写. 返回 1=写入 0=已存在.
    pub async fn hash_set_nx(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_HASH).await?;
        self.mark_composite(db, table); // ⭐ F49
        let fk = ks::encode_data(ks::KIND_HASH, key, field);
        if self.get_physical(db, table, &fk).await?.is_some() {
            return Ok(0);
        }
        self.put_physical(db, table, &fk, value).await?;
        let mk = ks::encode_type_meta(key);
        let count = self.hash_meta(db, table, key).await?.unwrap_or(0);
        self.put_physical(db, table, &mk, &enc_meta_val(ks::KIND_HASH, count + 1))
            .await?;
        Ok(1)
    }

    /// HGET: 单 field 读 (stored 带 tag, caller 渲染).
    pub async fn hash_get(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        field: &[u8],
    ) -> Result<Option<Vec<u8>>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_HASH).await?;
        self.get_physical(db, table, &ks::encode_data(ks::KIND_HASH, key, field))
            .await
    }

    /// HMGET: 多 field 读, 结果按输入顺序.
    pub async fn hash_get_many(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        fields: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_HASH).await?;
        let mut out = Vec::with_capacity(fields.len());
        for f in fields {
            out.push(
                self.get_physical(db, table, &ks::encode_data(ks::KIND_HASH, key, f))
                    .await?,
            );
        }
        Ok(out)
    }

    /// HDEL: 删多 field, 返回实际删除数; count 归 0 时删 meta.
    pub async fn hash_del(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        fields: &[Vec<u8>],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_HASH).await?;
        let mut deleted = 0u64;
        for f in fields {
            if self
                .delete_physical(db, table, &ks::encode_data(ks::KIND_HASH, key, f))
                .await?
            {
                deleted += 1;
            }
        }
        if deleted > 0 {
            let mk = ks::encode_type_meta(key);
            let count = self.hash_meta(db, table, key).await?.unwrap_or(0);
            let remain = count.saturating_sub(deleted);
            if remain == 0 {
                self.delete_physical(db, table, &mk).await?;
            } else {
                self.put_physical(db, table, &mk, &enc_meta_val(ks::KIND_HASH, remain))
                    .await?;
            }
        }
        Ok(deleted as i64)
    }

    /// HLEN: meta count (无 meta → 0).
    pub async fn hash_len(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_HASH).await?;
        Ok(self.hash_meta(db, table, key).await?.unwrap_or(0) as i64)
    }

    /// HGETALL: 前缀范围扫描收集 (field, stored value), 扫描外展开溢出链.
    pub async fn hash_get_all(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_HASH).await?;
        let root = match self.open_table(db, table).await? {
            Some(r) => r,
            None => {
                return Err(RegistryError::TableNotFound(
                    db.to_string(),
                    table.to_string(),
                ));
            }
        };
        let prefix = ks::data_prefix(ks::KIND_HASH, key);
        // 1. 扫描收集 (回调同步, 不能 async 展开溢出) — 先收 owned
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        crate::registry::table_scan_prefix(self.pager_mut(), root, &prefix, &mut |k, v| {
            if let Some((_, field)) = ks::split_data(k) {
                rows.push((field.to_vec(), v.to_vec()));
            }
            ControlFlow::Continue(())
        })
        .await?;
        // 2. 扫描外展开溢出描述符 (大 field value)
        for (_, v) in rows.iter_mut() {
            if crate::overflow::is_indirect(v) {
                *v = crate::overflow::read_overflow(self.pager_mut(), v).await?;
            }
        }
        Ok(rows)
    }

    // =================================================================
    // 类型感知的通用 key 操作 (String + 全部复合类型)
    // =================================================================

    /// GET 的类型感知版: String 命中路径零开销; miss 时探统一类型 meta
    /// `[#]key` (1 次) → 存在即 WRONGTYPE (覆盖全部 Hash/Set/List/ZSet).
    ///
    /// ⭐ U2: 统一 meta 后, 单次探测即能覆盖全类型 (不再限 hash),
    /// 与之前"只探 hash"同成本但语义完整; GET miss 仍只 1 次额外点查.
    pub async fn table_get_typed(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, RegistryError> {
        let got = self.table_get(db, table, key).await?;
        if got.is_some() {
            return Ok(got);
        }
        // ⭐ PERF (F49): 表内从未写过复合类型 → 不可能 WRONGTYPE, 跳过探测
        // (纯 String 表的 GET miss 恢复零额外点查)
        if self.has_composite(db, table)
            && self
                .get_physical(db, table, &ks::encode_type_meta(key))
                .await?
                .is_some()
        {
            return Err(RegistryError::WrongType);
        }
        Ok(None)
    }

    /// DEL 的类型感知版: 删 String 行 + 逐类型探测 meta → 范围删全部子行
    /// (含 SET 覆盖产生的孤儿行; ZSet 额外清 score 索引行).
    /// 返回 key 是否存在过 (任一类型).
    pub async fn key_delete_any(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<bool, RegistryError> {
        let s_existed = self.table_delete(db, table, key).await?;
        // 统一类型 meta: 1 次探测即知 kind, 命中则 purge 整个复合结构
        // ⭐ F49: 纯 String 表跳过探测 (零额外点查)
        let kind = if self.has_composite(db, table) {
            self.get_physical(db, table, &ks::encode_type_meta(key))
                .await?
                .and_then(|v| v.first().copied())
        } else {
            None
        };
        if let Some(kind) = kind {
            self.purge_key_data(db, table, key, kind).await?;
            return Ok(true);
        }
        Ok(s_existed)
    }

    /// ⭐ U2: 清除 key 的整个复合结构 (全部 data 行 + ZSet score 索引 + 类型 meta).
    /// 用于 DEL 与 SET 覆盖异类旧值. kind 由 caller 从 meta value 首字节得出.
    async fn purge_key_data(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        kind: u8,
    ) -> Result<(), RegistryError> {
        let root = self.open_table(db, table).await?.ok_or_else(|| {
            RegistryError::TableNotFound(db.to_string(), table.to_string())
        })?;
        let mut prefixes = vec![ks::data_prefix(kind, key)];
        if kind == ks::KIND_ZSET {
            prefixes.push(ks::zscore_prefix(key)); // score 索引行
        }
        let mut pkeys: Vec<Vec<u8>> = Vec::new();
        for prefix in &prefixes {
            crate::registry::table_scan_prefix(self.pager_mut(), root, prefix, &mut |k, _v| {
                pkeys.push(k.to_vec());
                ControlFlow::Continue(())
            })
            .await?;
        }
        for pk in &pkeys {
            self.delete_physical(db, table, pk).await?;
        }
        self.delete_physical(db, table, &ks::encode_type_meta(key)).await?;
        Ok(())
    }

    /// ⭐ U2: SET (或其它 String 写入) 前调用 — 若 key 当前是复合类型则
    /// 先 purge (Redis: SET 使 key 变 string, 旧类型丢弃). 非复合时 1 次探测即返.
    pub(crate) async fn purge_composite_if_any(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<(), RegistryError> {
        if let Some(kind) = self
            .get_physical(db, table, &ks::encode_type_meta(key))
            .await?
            .and_then(|v| v.first().copied())
        {
            self.purge_key_data(db, table, key, kind).await?;
        }
        Ok(())
    }

    // =================================================================
    // ⭐ Phase Set: Set ops (member 行空载荷, 存在即成员)
    // =================================================================

    /// SADD: 返回新增成员数.
    pub async fn set_add(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        members: &[Vec<u8>],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_SET).await?;
        self.mark_composite(db, table); // ⭐ F49
        // ⭐ O2: 探在/写入批量化 (LeafGuide 区间复用)
        let mks: Vec<Vec<u8>> = members
            .iter()
            .map(|m| ks::encode_data(ks::KIND_SET, key, m))
            .collect();
        let refs: Vec<&[u8]> = mks.iter().map(|k| k.as_slice()).collect();
        let existing = self.get_physical_many(db, table, &refs).await?;
        let mut seen: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
        let mut writes: Vec<(Vec<u8>, &[u8])> = Vec::new();
        let mut added = 0u64;
        for (i, m) in members.iter().enumerate() {
            if existing[i].is_none() && seen.insert(m.as_slice()) {
                // 1B 占位值 (存在即成员; 避免空 value 边界)
                writes.push((mks[i].clone(), &[1u8]));
                added += 1;
            }
        }
        let meta_val;
        if added > 0 {
            let count = self
                .get_physical(db, table, &ks::encode_type_meta(key))
                .await?
                .map(|v| dec_meta_count(&v))
                .unwrap_or(0);
            meta_val = enc_meta_val(ks::KIND_SET, count + added);
            writes.push((ks::encode_type_meta(key), &meta_val));
        }
        self.put_physical_many(db, table, &writes).await?;
        Ok(added as i64)
    }

    /// SREM: 返回实删数; card 归 0 删 meta.
    pub async fn set_rem(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        members: &[Vec<u8>],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_SET).await?;
        let mut removed = 0u64;
        for m in members {
            if self
                .delete_physical(db, table, &ks::encode_data(ks::KIND_SET, key, m))
                .await?
            {
                removed += 1;
            }
        }
        if removed > 0 {
            let meta_k = ks::encode_type_meta(key);
            let count = self
                .get_physical(db, table, &meta_k)
                .await?
                .map(|v| dec_meta_count(&v))
                .unwrap_or(0);
            let remain = count.saturating_sub(removed);
            if remain == 0 {
                self.delete_physical(db, table, &meta_k).await?;
            } else {
                self.put_physical(db, table, &meta_k, &enc_meta_val(ks::KIND_SET, remain))
                    .await?;
            }
        }
        Ok(removed as i64)
    }

    /// SISMEMBER: 点查.
    pub async fn set_is_member(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        member: &[u8],
    ) -> Result<bool, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_SET).await?;
        Ok(self
            .get_physical(db, table, &ks::encode_data(ks::KIND_SET, key, member))
            .await?
            .is_some())
    }

    /// SCARD: meta count.
    pub async fn set_card(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_SET).await?;
        Ok(self
            .get_physical(db, table, &ks::encode_type_meta(key))
            .await?
            .map(|v| dec_meta_count(&v))
            .unwrap_or(0) as i64)
    }

    /// SPOP/SRANDMEMBER 用: 取任意一个成员 (BTree 序首个, Break 早停免全扫).
    /// Set 无序语义下返回任意成员均合法 (v1 简化: 非随机).
    pub async fn set_pick_one(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_SET).await?;
        let root = self.open_table(db, table).await?.ok_or_else(|| {
            RegistryError::TableNotFound(db.to_string(), table.to_string())
        })?;
        let prefix = ks::data_prefix(ks::KIND_SET, key);
        let mut found: Option<Vec<u8>> = None;
        crate::registry::table_scan_prefix(self.pager_mut(), root, &prefix, &mut |k, _v| {
            if let Some((_, m)) = ks::split_data(k) {
                found = Some(m.to_vec());
            }
            ControlFlow::Break(())
        })
        .await?;
        Ok(found)
    }

    /// SMEMBERS/SSCAN: 前缀扫描全部成员 (BTree 序).
    pub async fn set_members(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_SET).await?;
        let root = self.open_table(db, table).await?.ok_or_else(|| {
            RegistryError::TableNotFound(db.to_string(), table.to_string())
        })?;
        let prefix = ks::data_prefix(ks::KIND_SET, key);
        let mut members: Vec<Vec<u8>> = Vec::new();
        crate::registry::table_scan_prefix(self.pager_mut(), root, &prefix, &mut |k, _v| {
            if let Some((_, m)) = ks::split_data(k) {
                members.push(m.to_vec());
            }
            ControlFlow::Continue(())
        })
        .await?;
        Ok(members)
    }

    // =================================================================
    // ⭐ Phase L: List ops (元素 sub-key = 保序 i64 idx; meta 存 head/tail)
    // meta value: [L][count u64 LE][head i64 LE][tail i64 LE] (25B)
    // =================================================================

    /// 读 list meta (count, head_idx, tail_idx). None = 不存在.
    async fn list_meta(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<Option<(u64, i64, i64)>, RegistryError> {
        Ok(self
            .get_physical(db, table, &ks::encode_type_meta(key))
            .await?
            .map(|v| {
                if v.len() < 25 {
                    (0, 0, 0)
                } else {
                    (
                        u64::from_le_bytes(v[1..9].try_into().expect("8B")),
                        i64::from_le_bytes(v[9..17].try_into().expect("8B")),
                        i64::from_le_bytes(v[17..25].try_into().expect("8B")),
                    )
                }
            }))
    }

    async fn put_list_meta(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        count: u64,
        head: i64,
        tail: i64,
    ) -> Result<(), RegistryError> {
        self.mark_composite(db, table); // ⭐ F49 (List 全部写路径的单点)
        let mut v = Vec::with_capacity(25);
        v.push(ks::KIND_LIST);
        v.extend_from_slice(&count.to_le_bytes());
        v.extend_from_slice(&head.to_le_bytes());
        v.extend_from_slice(&tail.to_le_bytes());
        self.put_physical(db, table, &ks::encode_type_meta(key), &v)
            .await
            .map(|_| ())
    }

    /// LPUSH(left=true)/RPUSH: 逐个 push, 返回新长度.
    /// LPUSH 多值逆序落地 (Redis: LPUSH k a b c → c b a).
    pub async fn list_push(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        values: &[Vec<u8>],
        left: bool,
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        let (mut count, mut head, mut tail) =
            self.list_meta(db, table, key).await?.unwrap_or((0, 0, 0));
        for v in values {
            let idx = if count == 0 {
                0
            } else if left {
                head - 1
            } else {
                tail + 1
            };
            let ek = ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(idx));
            self.put_physical(db, table, &ek, v).await?;
            if count == 0 {
                head = idx;
                tail = idx;
            } else if left {
                head = idx;
            } else {
                tail = idx;
            }
            count += 1;
        }
        self.put_list_meta(db, table, key, count, head, tail).await?;
        Ok(count as i64)
    }

    /// LPOP(left=true)/RPOP: 弹出 count 个, 返回 stored 值 (caller 渲染).
    ///
    /// ⭐ C2: 容忍中段空洞 (LREM/LTRIM 产生) — extreme idx 上 get miss 时
    /// 朝内收缩一格重试 (步数上限 = 历史删除数, 摊还可接受).
    pub async fn list_pop(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        left: bool,
        count: usize,
    ) -> Result<Vec<Vec<u8>>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        let Some((mut cnt, mut head, mut tail)) = self.list_meta(db, table, key).await? else {
            return Ok(vec![]);
        };
        let mut out = Vec::new();
        'pop: for _ in 0..count {
            if cnt == 0 {
                break;
            }
            loop {
                if head > tail {
                    cnt = 0;
                    break 'pop;
                }
                let idx = if left { head } else { tail };
                let ek = ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(idx));
                if let Some(v) = self.get_physical(db, table, &ek).await? {
                    out.push(v);
                    self.delete_physical(db, table, &ek).await?;
                    cnt -= 1;
                    if left {
                        head += 1;
                    } else {
                        tail -= 1;
                    }
                    break;
                }
                // 空洞: 收缩一格再试 (不减 cnt)
                if left {
                    head += 1;
                } else {
                    tail -= 1;
                }
            }
        }
        if cnt == 0 {
            self.delete_physical(db, table, &ks::encode_type_meta(key))
                .await?;
        } else {
            self.put_list_meta(db, table, key, cnt, head, tail).await?;
        }
        Ok(out)
    }

    /// LLEN.
    pub async fn list_len(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        Ok(self.list_meta(db, table, key).await?.map(|(c, _, _)| c).unwrap_or(0) as i64)
    }

    /// LRANGE start end (含负索引, end inclusive). 返回 stored 值 (caller 渲染).
    pub async fn list_range(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        start: i64,
        end: i64,
    ) -> Result<Vec<Vec<u8>>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        let Some((cnt, _, _)) = self.list_meta(db, table, key).await? else {
            return Ok(vec![]);
        };
        let len = cnt as i64;
        if len == 0 {
            return Ok(vec![]);
        }
        let mut s = if start < 0 { len + start } else { start };
        let mut e = if end < 0 { len + end } else { end };
        if s < 0 {
            s = 0;
        }
        if e >= len {
            e = len - 1;
        }
        if s > e {
            return Ok(vec![]);
        }
        // 扫描全部元素 (BTree 序 = 列表左到右), 取 [s..=e]
        let root = self.open_table(db, table).await?.ok_or_else(|| {
            RegistryError::TableNotFound(db.to_string(), table.to_string())
        })?;
        let prefix = ks::data_prefix(ks::KIND_LIST, key);
        let mut pos = 0i64;
        let mut out = Vec::new();
        crate::registry::table_scan_prefix(self.pager_mut(), root, &prefix, &mut |_k, v| {
            if pos >= s && pos <= e {
                out.push(v.to_vec());
            }
            pos += 1;
            if pos > e {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .await?;
        Ok(out)
    }

    /// LINDEX idx (含负索引). 返回 stored 值.
    ///
    /// ⭐ C2: 按扫描序取第 pos 个 (O(n), 符合 Redis) — 不再假设 idx 连续
    /// (LREM/LTRIM/LINSERT 会留空洞).
    pub async fn list_index(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        idx: i64,
    ) -> Result<Option<Vec<u8>>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        let Some((cnt, _, _)) = self.list_meta(db, table, key).await? else {
            return Ok(None);
        };
        let len = cnt as i64;
        let pos = if idx < 0 { len + idx } else { idx };
        if pos < 0 || pos >= len {
            return Ok(None);
        }
        let root = self.open_table(db, table).await?.ok_or_else(|| {
            RegistryError::TableNotFound(db.to_string(), table.to_string())
        })?;
        let prefix = ks::data_prefix(ks::KIND_LIST, key);
        let mut i = 0i64;
        let mut found: Option<Vec<u8>> = None;
        crate::registry::table_scan_prefix(self.pager_mut(), root, &prefix, &mut |_k, v| {
            if i == pos {
                found = Some(v.to_vec());
                return ControlFlow::Break(());
            }
            i += 1;
            ControlFlow::Continue(())
        })
        .await?;
        // 扫描拿到的是裸 stored, 溢出描述符需展开
        if let Some(v) = &mut found
            && crate::overflow::is_indirect(v)
        {
            *v = crate::overflow::read_overflow(self.pager_mut(), v).await?;
        }
        Ok(found)
    }

    /// LSET idx val (含负索引). 返回 false = 越界.
    ///
    /// ⭐ C2: 扫描定位第 pos 个行的物理 idx 后原位覆写 (旧溢出链自动释放).
    pub async fn list_set(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        idx: i64,
        value: &[u8],
    ) -> Result<bool, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        let Some((cnt, _, _)) = self.list_meta(db, table, key).await? else {
            return Ok(false);
        };
        let len = cnt as i64;
        let pos = if idx < 0 { len + idx } else { idx };
        if pos < 0 || pos >= len {
            return Ok(false);
        }
        let root = self.open_table(db, table).await?.ok_or_else(|| {
            RegistryError::TableNotFound(db.to_string(), table.to_string())
        })?;
        let prefix = ks::data_prefix(ks::KIND_LIST, key);
        let mut i = 0i64;
        let mut target: Option<i64> = None;
        crate::registry::table_scan_prefix(self.pager_mut(), root, &prefix, &mut |k, _v| {
            if i == pos {
                if let Some((_, suf)) = ks::split_data(k)
                    && suf.len() == 8
                {
                    target = Some(ks::decode_idx(suf.try_into().expect("8B")));
                }
                return ControlFlow::Break(());
            }
            i += 1;
            ControlFlow::Continue(())
        })
        .await?;
        let Some(actual) = target else {
            return Ok(false);
        };
        self.put_physical(
            db,
            table,
            &ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(actual)),
            value,
        )
        .await?;
        Ok(true)
    }

    // =================================================================
    // ⭐ C2: List 中段操作 (LREM / LTRIM / LPOS / LINSERT)
    // =================================================================

    /// 扫描全部 (物理 idx, 裸 stored) 行, 扫描序 = 列表左到右.
    async fn list_rows(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<Vec<(i64, Vec<u8>)>, RegistryError> {
        let root = self.open_table(db, table).await?.ok_or_else(|| {
            RegistryError::TableNotFound(db.to_string(), table.to_string())
        })?;
        let prefix = ks::data_prefix(ks::KIND_LIST, key);
        let mut rows: Vec<(i64, Vec<u8>)> = Vec::new();
        crate::registry::table_scan_prefix(self.pager_mut(), root, &prefix, &mut |k, v| {
            if let Some((_, suf)) = ks::split_data(k)
                && suf.len() == 8
            {
                rows.push((ks::decode_idx(suf.try_into().expect("8B")), v.to_vec()));
            }
            ControlFlow::Continue(())
        })
        .await?;
        Ok(rows)
    }

    /// stored 与目标值比较 (溢出描述符先展开).
    async fn stored_eq(&mut self, stored: &[u8], want: &[u8]) -> Result<bool, RegistryError> {
        if crate::overflow::is_indirect(stored) {
            let full = crate::overflow::read_overflow(self.pager_mut(), stored).await?;
            Ok(full == want)
        } else {
            Ok(stored == want)
        }
    }

    /// 搜行后更新 meta: 从剩余行重算 count/head/tail (全删 → 删 meta).
    async fn refresh_list_meta(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<(), RegistryError> {
        let rows = self.list_rows(db, table, key).await?;
        if rows.is_empty() {
            self.delete_physical(db, table, &ks::encode_type_meta(key))
                .await?;
        } else {
            let head = rows.first().expect("non-empty").0;
            let tail = rows.last().expect("non-empty").0;
            self.put_list_meta(db, table, key, rows.len() as u64, head, tail)
                .await?;
        }
        Ok(())
    }

    /// LREM count value: 删除匹配行. count>0 从头数 N; <0 从尾; =0 全部.
    /// value 为 stored 布局 ([tag][payload]).
    pub async fn list_rem(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        count: i64,
        value: &[u8],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        if self.list_meta(db, table, key).await?.is_none() {
            return Ok(0);
        }
        let rows = self.list_rows(db, table, key).await?;
        // 匹配行的物理 idx (扫描序)
        let mut matched: Vec<i64> = Vec::new();
        for (idx, stored) in &rows {
            if self.stored_eq(stored, value).await? {
                matched.push(*idx);
            }
        }
        let victims: Vec<i64> = if count > 0 {
            matched.into_iter().take(count as usize).collect()
        } else if count < 0 {
            let n = count.unsigned_abs() as usize;
            let skip = matched.len().saturating_sub(n);
            matched.into_iter().skip(skip).collect()
        } else {
            matched
        };
        for idx in &victims {
            self.delete_physical(db, table, &ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(*idx)))
                .await?;
        }
        if !victims.is_empty() {
            self.refresh_list_meta(db, table, key).await?;
        }
        Ok(victims.len() as i64)
    }

    /// LTRIM start stop (含负索引, 保留 [start, stop]).
    pub async fn list_trim(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<(), RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        if self.list_meta(db, table, key).await?.is_none() {
            return Ok(());
        }
        let rows = self.list_rows(db, table, key).await?;
        let len = rows.len() as i64;
        let mut s = if start < 0 { len + start } else { start };
        let mut e = if stop < 0 { len + stop } else { stop };
        if s < 0 {
            s = 0;
        }
        if e >= len {
            e = len - 1;
        }
        for (pos, (idx, _)) in rows.iter().enumerate() {
            let pos = pos as i64;
            if s > e || pos < s || pos > e {
                self.delete_physical(
                    db,
                    table,
                    &ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(*idx)),
                )
                .await?;
            }
        }
        self.refresh_list_meta(db, table, key).await?;
        Ok(())
    }

    /// LPOS value rank count: 返回匹配位置 (0-based, 扫描序).
    /// rank>0 从第 rank 个匹配起; rank<0 从尾倒数. count=0 → 全部.
    pub async fn list_pos(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        value: &[u8],
        rank: i64,
        count: usize,
    ) -> Result<Vec<i64>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        if self.list_meta(db, table, key).await?.is_none() {
            return Ok(vec![]);
        }
        let rows = self.list_rows(db, table, key).await?;
        let mut matches: Vec<i64> = Vec::new();
        for (pos, (_, stored)) in rows.iter().enumerate() {
            if self.stored_eq(stored, value).await? {
                matches.push(pos as i64);
            }
        }
        if rank < 0 {
            matches.reverse();
        }
        let skip = rank.unsigned_abs().saturating_sub(1) as usize;
        let iter = matches.into_iter().skip(skip);
        Ok(if count == 0 {
            iter.collect()
        } else {
            iter.take(count).collect()
        })
    }

    /// 搬行: 读完整 value (溢出展开) → 写新 idx → 删旧 idx (释放旧溢出链).
    async fn move_list_row(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<(), RegistryError> {
        let from_k = ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(from));
        let Some(full) = self.get_physical(db, table, &from_k).await? else {
            return Ok(()); // 空洞容忍
        };
        self.put_physical(
            db,
            table,
            &ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(to)),
            &full,
        )
        .await?;
        self.delete_physical(db, table, &from_k).await?;
        Ok(())
    }

    /// LINSERT BEFORE|AFTER pivot value. 返回新长度; pivot 不存在 -1; key 不存在 0.
    /// 优先复用相邻 idx 间的空洞; 无稺密时才整体移位较小一侧 (O(n) 搬行).
    pub async fn list_insert(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        before: bool,
        pivot: &[u8],
        value: &[u8],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        if self.list_meta(db, table, key).await?.is_none() {
            return Ok(0);
        }
        let rows = self.list_rows(db, table, key).await?;
        let mut pivot_pos: Option<usize> = None;
        for (pos, (_, stored)) in rows.iter().enumerate() {
            if self.stored_eq(stored, pivot).await? {
                pivot_pos = Some(pos);
                break;
            }
        }
        let Some(p) = pivot_pos else {
            return Ok(-1);
        };
        let ins = if before { p } else { p + 1 }; // 新元素在扫描序中的位置
        let n = rows.len();
        let new_idx: i64 = if ins == 0 {
            rows[0].0 - 1 // 头前插入 (同 LPUSH)
        } else if ins == n {
            rows[n - 1].0 + 1 // 尾后插入 (同 RPUSH)
        } else if rows[ins].0 - rows[ins - 1].0 > 1 {
            rows[ins - 1].0 + 1 // 复用空洞, O(1)
        } else if ins <= n - ins {
            // 左侧更小: [0..ins) 全部下移 1 (升序处理避免碰撞), 腾出 rows[ins-1].0
            for (idx, _) in rows[..ins].iter() {
                self.move_list_row(db, table, key, *idx, idx - 1).await?;
            }
            rows[ins - 1].0
        } else {
            // 右侧更小: [ins..) 全部上移 1 (降序处理), 腾出 rows[ins].0
            for (idx, _) in rows[ins..].iter().rev() {
                self.move_list_row(db, table, key, *idx, idx + 1).await?;
            }
            rows[ins].0
        };
        self.put_physical(
            db,
            table,
            &ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(new_idx)),
            value,
        )
        .await?;
        self.refresh_list_meta(db, table, key).await?;
        Ok((n + 1) as i64)
    }

    // =================================================================
    // ⭐ Phase Z: ZSet 双索引
    //   meta       [Z][0][klen][key] = [kind][count u64]
    //   member→score [Z][1][klen][key][member] = score(f64 LE 8B)  (点查)
    //   score→member [Z][2][klen][key][score8 保序][member] = [1]   (有序扫)
    // =================================================================

    async fn zset_count(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<Option<u64>, RegistryError> {
        Ok(self
            .get_physical(db, table, &ks::encode_type_meta(key))
            .await?
            .map(|v| dec_meta_count(&v)))
    }

    /// 读 member 的 score (点查 index1).
    async fn zset_score_of(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        member: &[u8],
    ) -> Result<Option<f64>, RegistryError> {
        Ok(self
            .get_physical(db, table, &ks::encode_data(ks::KIND_ZSET, key, member))
            .await?
            .and_then(|v| v.get(..8).map(|b| f64::from_le_bytes(b.try_into().expect("8B")))))
    }

    /// ZADD: 返回**新增**成员数 (已存在只更新 score). 双索引一致维护.
    pub async fn zset_add(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        pairs: &[(f64, Vec<u8>)],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_ZSET).await?;
        self.mark_composite(db, table); // ⭐ F49
        let mut count = self.zset_count(db, table, key).await?.unwrap_or(0);
        // ⭐ O2: 旧 score 批量读 (index1) + 写入批量化; 删旧 index2 仍逐条
        // (delete 无批量基建, 记录). 批内同 member 重复: 后写覆盖, 逐项处理
        // 时用本地 last_score 跟踪保证 index2 一致.
        let idx1s: Vec<Vec<u8>> = pairs
            .iter()
            .map(|(_, m)| ks::encode_data(ks::KIND_ZSET, key, m))
            .collect();
        let refs: Vec<&[u8]> = idx1s.iter().map(|k| k.as_slice()).collect();
        let olds = self.get_physical_many(db, table, &refs).await?;
        let dec_score = |v: &[u8]| -> Option<f64> {
            v.try_into().ok().map(f64::from_le_bytes)
        };
        // member → 本批内最新已处理 score (覆盖旧盘上值)
        let mut last: std::collections::HashMap<&[u8], f64> = std::collections::HashMap::new();
        let mut score_bytes: Vec<[u8; 8]> = Vec::with_capacity(pairs.len());
        let mut writes: Vec<(Vec<u8>, &[u8])> = Vec::new();
        let mut added = 0u64;
        // 先算删除集与新增数 (删除必须先于批量写提交, 防同批新旧行乱序)
        for (i, (score, member)) in pairs.iter().enumerate() {
            let prev = last
                .get(member.as_slice())
                .copied()
                .or_else(|| olds[i].as_deref().and_then(dec_score));
            match prev {
                Some(old) if old == *score => {
                    last.insert(member.as_slice(), *score);
                    score_bytes.push(score.to_le_bytes());
                    continue;
                }
                Some(old) => {
                    self.delete_physical(
                        db,
                        table,
                        &ks::encode_zscore(key, ks::encode_f64_ordered(old), member),
                    )
                    .await?;
                }
                None => added += 1,
            }
            last.insert(member.as_slice(), *score);
            score_bytes.push(score.to_le_bytes());
        }
        // 批量写: index1 (member→score LE) + index2 (score→member 占位)
        for (i, (score, member)) in pairs.iter().enumerate() {
            // 批内重复 member: 只写最终 score 的行 (last 判定)
            if last.get(member.as_slice()) != Some(score) {
                continue;
            }
            writes.push((idx1s[i].clone(), &score_bytes[i]));
            writes.push((
                ks::encode_zscore(key, ks::encode_f64_ordered(*score), member),
                &[1u8],
            ));
        }
        let meta_val;
        if added > 0 {
            count += added;
            meta_val = enc_meta_val(ks::KIND_ZSET, count);
            writes.push((ks::encode_type_meta(key), &meta_val));
        }
        self.put_physical_many(db, table, &writes).await?;
        Ok(added as i64)
    }

    /// ZSCORE.
    pub async fn zset_score(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        member: &[u8],
    ) -> Result<Option<f64>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_ZSET).await?;
        self.zset_score_of(db, table, key, member).await
    }

    /// ZINCRBY: 新 score = 旧 (或 0) + delta.
    pub async fn zset_incr(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        delta: f64,
        member: &[u8],
    ) -> Result<f64, RegistryError> {
        let cur = self.zset_score(db, table, key, member).await?.unwrap_or(0.0);
        let new = cur + delta;
        self.zset_add(db, table, key, &[(new, member.to_vec())]).await?;
        Ok(new)
    }

    /// ZREM: 返回实删数; count 归 0 删 meta.
    pub async fn zset_rem(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        members: &[Vec<u8>],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_ZSET).await?;
        let mut removed = 0u64;
        for member in members {
            if let Some(old) = self.zset_score_of(db, table, key, member).await? {
                self.delete_physical(db, table, &ks::encode_data(ks::KIND_ZSET, key, member))
                    .await?;
                self.delete_physical(
                    db,
                    table,
                    &ks::encode_zscore(key, ks::encode_f64_ordered(old), member),
                )
                .await?;
                removed += 1;
            }
        }
        if removed > 0 {
            let count = self.zset_count(db, table, key).await?.unwrap_or(0);
            let remain = count.saturating_sub(removed);
            let mk = ks::encode_type_meta(key);
            if remain == 0 {
                self.delete_physical(db, table, &mk).await?;
            } else {
                self.put_physical(db, table, &mk, &enc_meta_val(ks::KIND_ZSET, remain))
                    .await?;
            }
        }
        Ok(removed as i64)
    }

    /// ZCARD.
    pub async fn zset_card(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_ZSET).await?;
        Ok(self.zset_count(db, table, key).await?.unwrap_or(0) as i64)
    }

    /// 扫描 score 索引, 按序返回全部 (member, score); rev 时反转.
    async fn zset_ordered(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<Vec<(Vec<u8>, f64)>, RegistryError> {
        let root = self.open_table(db, table).await?.ok_or_else(|| {
            RegistryError::TableNotFound(db.to_string(), table.to_string())
        })?;
        let prefix = ks::zscore_prefix(key);
        let mut out: Vec<(Vec<u8>, f64)> = Vec::new();
        crate::registry::table_scan_prefix(self.pager_mut(), root, &prefix, &mut |k, _v| {
            if let Some((_, score8, member)) = ks::split_zscore(k) {
                out.push((member.to_vec(), ks::decode_f64_ordered(score8)));
            }
            ControlFlow::Continue(())
        })
        .await?;
        Ok(out)
    }

    /// ZRANGE / ZREVRANGE (按 rank, 含负索引, end inclusive).
    pub async fn zset_range(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        start: i64,
        end: i64,
        rev: bool,
    ) -> Result<Vec<(Vec<u8>, f64)>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_ZSET).await?;
        let mut all = self.zset_ordered(db, table, key).await?;
        if rev {
            all.reverse();
        }
        let len = all.len() as i64;
        if len == 0 {
            return Ok(vec![]);
        }
        let mut s = if start < 0 { len + start } else { start };
        let mut e = if end < 0 { len + end } else { end };
        if s < 0 {
            s = 0;
        }
        if e >= len {
            e = len - 1;
        }
        if s > e {
            return Ok(vec![]);
        }
        Ok(all[s as usize..=e as usize].to_vec())
    }

    /// ZRANGEBYSCORE min max (含端, 有序). 负无穷/正无穷由 caller 传 f64::INFINITY.
    pub async fn zset_range_by_score(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        min: f64,
        max: f64,
    ) -> Result<Vec<(Vec<u8>, f64)>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_ZSET).await?;
        let all = self.zset_ordered(db, table, key).await?;
        Ok(all.into_iter().filter(|(_, sc)| *sc >= min && *sc <= max).collect())
    }

    /// ZRANK / ZREVRANK: member 的排名 (0-based), None = 不存在.
    pub async fn zset_rank(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        member: &[u8],
        rev: bool,
    ) -> Result<Option<i64>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_ZSET).await?;
        let all = self.zset_ordered(db, table, key).await?;
        let n = all.len();
        for (i, (m, _)) in all.iter().enumerate() {
            if m.as_slice() == member {
                return Ok(Some(if rev { (n - 1 - i) as i64 } else { i as i64 }));
            }
        }
        Ok(None)
    }

    /// ZPOPMIN(rev=false)/ZPOPMAX(rev=true): 弹出 count 个最小/最大 (member, score).
    pub async fn zset_pop(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        rev: bool,
        count: usize,
    ) -> Result<Vec<(Vec<u8>, f64)>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_ZSET).await?;
        let mut all = self.zset_ordered(db, table, key).await?;
        if rev {
            all.reverse();
        }
        let take: Vec<(Vec<u8>, f64)> = all.into_iter().take(count).collect();
        if !take.is_empty() {
            let members: Vec<Vec<u8>> = take.iter().map(|(m, _)| m.clone()).collect();
            self.zset_rem(db, table, key, &members).await?;
        }
        Ok(take)
    }

    /// ZMSCORE: 逐 member 点查 score (缺失 = None), 保持输入序.
    pub async fn zset_mscore(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        members: &[Vec<u8>],
    ) -> Result<Vec<Option<f64>>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_ZSET).await?;
        let mut out = Vec::with_capacity(members.len());
        for m in members {
            out.push(self.zset_score_of(db, table, key, m).await?);
        }
        Ok(out)
    }

    // =================================================================
    // ⭐ C1: Set/Hash 命令空洞补齐 helper
    // =================================================================

    /// SMISMEMBER: 逐 member 判存在, 保持输入序.
    pub async fn set_mismember(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        members: &[Vec<u8>],
    ) -> Result<Vec<bool>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_SET).await?;
        let mut out = Vec::with_capacity(members.len());
        for m in members {
            out.push(
                self.get_physical(db, table, &ks::encode_data(ks::KIND_SET, key, m))
                    .await?
                    .is_some(),
            );
        }
        Ok(out)
    }

    /// 取前 N 个成员 (BTree 序, 不删). count=0 → 全部. (v1: 非随机.)
    async fn set_take_n(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        count: usize,
    ) -> Result<Vec<Vec<u8>>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_SET).await?;
        let root = self.open_table(db, table).await?.ok_or_else(|| {
            RegistryError::TableNotFound(db.to_string(), table.to_string())
        })?;
        let prefix = ks::data_prefix(ks::KIND_SET, key);
        let mut out: Vec<Vec<u8>> = Vec::new();
        crate::registry::table_scan_prefix(self.pager_mut(), root, &prefix, &mut |k, _v| {
            if let Some((_, m)) = ks::split_data(k) {
                out.push(m.to_vec());
            }
            if count != 0 && out.len() >= count {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .await?;
        Ok(out)
    }

    /// SRANDMEMBER key count: 取前 N 成员不删.
    pub async fn set_rand_n(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        count: usize,
    ) -> Result<Vec<Vec<u8>>, RegistryError> {
        self.set_take_n(db, table, key, count).await
    }

    /// SPOP key count: 取前 N 成员并删除.
    pub async fn set_pop_n(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        count: usize,
    ) -> Result<Vec<Vec<u8>>, RegistryError> {
        let members = self.set_take_n(db, table, key, count).await?;
        if !members.is_empty() {
            self.set_rem(db, table, key, &members).await?;
        }
        Ok(members)
    }

    /// HRANDFIELD key count: 取前 N 个 (field, stored value). (v1: 非随机.)
    pub async fn hash_rand(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        count: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RegistryError> {
        let all = self.hash_get_all(db, table, key).await?;
        if count == 0 {
            return Ok(all);
        }
        Ok(all.into_iter().take(count).collect())
    }

    // =================================================================
    // ⭐ U3: recover 计数重建 (修复 crash 中 data 行已落盘但 meta count 未落盘)
    // =================================================================

    /// 遍历全部 db/table 的统一类型 meta 行 `[#]key`, 按实际 data 行数修正
    /// count (List 额外从首尾行重建 head/tail); 数据行已全死则删 meta.
    /// 开库时调一次, 成本与复合 key 总行数同量级.
    pub(crate) async fn rebuild_composite_counts(&mut self) -> Result<(), RegistryError> {
        let dbs = self.list_dbs();
        for db in dbs {
            let tables = self.list_tables(&db)?;
            for table in tables {
                let Some(root) = self.open_table(&db, &table).await? else {
                    continue;
                };
                // ⭐ Y1: 顺路重建 SQL 表的索引 bloom — 探 [$] schema 行,
                // 有则扫全部 [I] 索引行喂 bloom (重启后剪枝照样生效, 无假阴性)
                if self
                    .get_physical(&db, &table, &ks::encode_schema_row())
                    .await?
                    .is_some()
                {
                    let mut entries: Vec<(u32, Vec<u8>)> = Vec::new();
                    crate::registry::table_scan_prefix(
                        self.pager_mut(),
                        root,
                        &[ks::KIND_INDEX],
                        &mut |k, _v| {
                            if k.len() >= 5
                                && let Some(iid_raw) = k.get(1..5)
                                && let Some((ev, _)) = ks::split_index_val(&k[5..])
                            {
                                let iid = u32::from_be_bytes(iid_raw.try_into().expect("4B"));
                                entries.push((iid, ev.to_vec()));
                            }
                            ControlFlow::Continue(())
                        },
                    )
                    .await?;
                    for (iid, ev) in entries {
                        self.bloom_entry(&db, &table, iid).insert(&ev);
                    }
                }
                // 1. 收集本表全部类型 meta 物理 key
                let mut metas: Vec<Vec<u8>> = Vec::new();
                crate::registry::table_scan_prefix(
                    self.pager_mut(),
                    root,
                    &ks::type_meta_scan_prefix(),
                    &mut |k, _v| {
                        metas.push(k.to_vec());
                        ControlFlow::Continue(())
                    },
                )
                .await?;
                // 2. 逐个重算
                if !metas.is_empty() {
                    // ⭐ F49: 开库重建复合类型提示位 (热路径探测跳过的依据)
                    self.mark_composite(&db, &table);
                }
                for mk in metas {
                    let Some(ukey) = ks::split_type_meta(&mk).map(|k| k.to_vec()) else {
                        continue;
                    };
                    let Some(v) = self.get_physical(&db, &table, &mk).await? else {
                        continue;
                    };
                    let Some(&kind) = v.first() else { continue };

                    if kind == ks::KIND_LIST {
                        let (mut cnt, mut mn, mut mx) = (0u64, i64::MAX, i64::MIN);
                        let prefix = ks::data_prefix(ks::KIND_LIST, &ukey);
                        crate::registry::table_scan_prefix(
                            self.pager_mut(),
                            root,
                            &prefix,
                            &mut |k, _v| {
                                if let Some((_, suf)) = ks::split_data(k)
                                    && suf.len() == 8
                                {
                                    let idx = ks::decode_idx(suf.try_into().expect("8B"));
                                    cnt += 1;
                                    mn = mn.min(idx);
                                    mx = mx.max(idx);
                                }
                                ControlFlow::Continue(())
                            },
                        )
                        .await?;
                        if cnt == 0 {
                            self.delete_physical(&db, &table, &mk).await?;
                        } else {
                            self.put_list_meta(&db, &table, &ukey, cnt, mn, mx).await?;
                        }
                    } else {
                        // Hash/Set/ZSet: count = data 行数 (ZSet 只算 member→score 行)
                        let mut cnt = 0u64;
                        let prefix = ks::data_prefix(kind, &ukey);
                        crate::registry::table_scan_prefix(
                            self.pager_mut(),
                            root,
                            &prefix,
                            &mut |_k, _v| {
                                cnt += 1;
                                ControlFlow::Continue(())
                            },
                        )
                        .await?;
                        if cnt == 0 {
                            self.delete_physical(&db, &table, &mk).await?;
                        } else if dec_meta_count(&v) != cnt {
                            self.put_physical(&db, &table, &mk, &enc_meta_val(kind, cnt))
                                .await?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
