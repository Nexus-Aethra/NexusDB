//! T12.14 DbNameResolver: 全局 db name ↔ DbId 双向映射, 持久化到 MetaPage 头部.
//!
//! ## 设计 (来自 plan 2026-07-20 §3.2)
//!
//! - **作用**: 在单 `StorageEngine` / `ShardManager` 内部将 `db_name: &str`
//!   (用户字符串) 映射到 `DbId: u32` (内部 4 字节 ID, Copy + 零分配).
//! - **持久化**: 序列化到 MetaPage 头部固定 64 字节段, 所有 shard 同步
//!   (MetaPage 是 vpid 0, 每 shard 都有完整副本).
//! - **API**: `new()` (含默认 "default" → DbId 0) / `get_or_create` / `resolve` /
//!   `name` / `list` / `count` / `serialize_into(&mut [u8; 64])` / `deserialize(&[u8; 64])`.
//! - **限制**: 64B 头最多容纳 ~7-8 个短 db name (实际单 ShardManager 内 < 16).
//!   超出会返回 `ResolverError::Full` (留 polish 时扩展, 当前 panic-on-overflow 也可).
//!
//! ## 单线程使用
//!
//! 沿用 Pager 契约, per-shard thread 单线程使用, 无锁.
//!
//! ## MetaPage 集成
//!
//! MetaPage 头部 `[0..64)` 字节段是 `DbNameResolver` 序列化区.
//! `[64..)` 字节是 BTree leaf (key=db_name 字符串, value=table_dir_root_vpid).
//! 加载/刷新时分别 parse / serialize 两段, 互不干扰.

use std::collections::HashMap;
use std::io;

use crate::types::{DEFAULT_DB_ID, DEFAULT_DB_NAME, DbId};

// =====================================================================
// 1024 字节头布局
// =====================================================================
//
// ```
// offset      0..4    : u32 LE   count (当前已注册 db 数, 含默认 "default")
// offset      4..8    : u32 LE   next_id (下一个分配的 DbId, 单调递增)
// offset      8..1024 : 1016B name entries 区
// ```
//
// 每个 name entry 编码: `[u8 len][len bytes UTF-8]`.
// 1016B 区最多容纳 ~145 个 6 字符短名 (7B 每 entry) + 1 个 "default" (8B entry).
// 适合单 ShardManager 内 < 200 db 的常见场景. 超出会返回 `ResolverError::Full`.
//
// **容量选择理由**: 1024B = 1 KiB, 占 MetaPage 16 KiB 的 6.25%, item area
// 仍有 16384 - 40 - 1024 - 16 = 15304B (~14900B 可用), 足够放 ~200 个 db_name
// + value. 大于 200 db 留 polish 时再考虑 (可能改为 multi-page resolver).

/// MetaPage 头部 DbNameResolver 序列化段固定大小.
pub const RESOLVER_HEADER_SIZE: usize = 1024;

/// 名字条目区的可用字节数 (1024B 头减去 count + next_id 各 4B).
pub const RESOLVER_NAME_AREA_SIZE: usize = RESOLVER_HEADER_SIZE - 8;

/// 单个 name 字符串的最大字节数.
pub const RESOLVER_MAX_NAME_LEN: usize = 254;

// =====================================================================
// ResolverError
// =====================================================================

/// DbNameResolver 操作错误.
#[derive(Debug, thiserror::Error)]
pub enum ResolverError {
    #[error("resolver name area full, cannot add '{0}' (max ~8 names)")]
    Full(String),
    #[error("resolver name too long: {0} bytes (max {1})")]
    NameTooLong(usize, usize),
    #[error("resolver header too short: got {0}, expected {1}")]
    HeaderTooShort(usize, usize),
    #[error("resolver header has invalid data: {0}")]
    InvalidData(String),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

// =====================================================================
// DbNameResolver
// =====================================================================

/// 全局 db name ↔ DbId 双向映射.
///
/// - `names: Vec<String>` — `id → name` (按 id 顺序, names[0] = id 0 的名字).
/// - `name_to_id: HashMap<String, DbId>` — `name → id` 反向索引.
///
/// **默认构造**: `new()` 自动注册 "default" → DbId 0.
#[derive(Debug, Clone)]
pub struct DbNameResolver {
    /// `id → name`, names[id] 即 id 对应的名字.
    names: Vec<String>,
    /// `name → id` 反向索引, 用于 O(1) 解析.
    name_to_id: HashMap<String, DbId>,
}

impl Default for DbNameResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl DbNameResolver {
    /// 构造一个空 resolver, 自动注册 "default" → DbId 0.
    pub fn new() -> Self {
        let mut s = Self {
            names: Vec::new(),
            name_to_id: HashMap::new(),
        };
        // ⭐ 默认注册 "default" → DbId 0
        s.names.push(DEFAULT_DB_NAME.to_string());
        s.name_to_id
            .insert(DEFAULT_DB_NAME.to_string(), DEFAULT_DB_ID);
        s
    }

    /// 从已注册的 names + next_id 构造 (deserialize 用).
    fn from_parts(names: Vec<String>) -> Self {
        let mut name_to_id = HashMap::with_capacity(names.len());
        for (id, name) in names.iter().enumerate() {
            name_to_id.insert(name.clone(), id as DbId);
        }
        Self { names, name_to_id }
    }

    /// 下一个可分配的 DbId (= 当前 names.len(), 因为 id 永不重用).
    pub fn next_id(&self) -> DbId {
        self.names.len() as DbId
    }

    /// 获取或创建一个 db 的 DbId.
    ///
    /// - 已存在 → 返回现有 DbId
    /// - 不存在 → 分配新 DbId (= next_id()), 注册, 返回新 id
    ///
    /// **错误**: 名字太长 (> 48B) 或 name 区满 (> 8 个 db).
    pub fn get_or_create(&mut self, name: &str) -> Result<DbId, ResolverError> {
        if let Some(&id) = self.name_to_id.get(name) {
            return Ok(id);
        }

        // 校验 name 长度
        let name_bytes = name.as_bytes();
        if name_bytes.len() > RESOLVER_MAX_NAME_LEN {
            return Err(ResolverError::NameTooLong(
                name_bytes.len(),
                RESOLVER_MAX_NAME_LEN,
            ));
        }

        // 校验 name 区是否放得下 (1B len + name_bytes.len())
        let needed = 1 + name_bytes.len();
        let used = self.used_name_area_bytes();
        if used + needed > RESOLVER_NAME_AREA_SIZE {
            return Err(ResolverError::Full(name.to_string()));
        }

        let id = self.next_id();
        self.names.push(name.to_string());
        self.name_to_id.insert(name.to_string(), id);
        Ok(id)
    }

    /// 查 name → id. 不存在返回 None.
    pub fn resolve(&self, name: &str) -> Option<DbId> {
        self.name_to_id.get(name).copied()
    }

    /// 查 id → name. id 越界返回 None.
    pub fn name(&self, id: DbId) -> Option<&str> {
        self.names.get(id as usize).map(|s| s.as_str())
    }

    /// 列出所有 (id, name) 对, 按 id 升序.
    pub fn list(&self) -> Vec<(DbId, &str)> {
        self.names
            .iter()
            .enumerate()
            .map(|(id, name)| (id as DbId, name.as_str()))
            .collect()
    }

    /// 当前已注册 db 数.
    pub fn count(&self) -> usize {
        self.names.len()
    }

    /// 计算已用的 name 区字节数 (1B len + name_bytes 累计).
    fn used_name_area_bytes(&self) -> usize {
        self.names.iter().map(|n| 1 + n.len()).sum()
    }

    // =================================================================
    // 序列化 / 反序列化
    // =================================================================

    /// 序列化为 64 字节.
    ///
    /// **布局**:
    /// - `[0..4]`: u32 LE count
    /// - `[4..8]`: u32 LE next_id (= names.len(), 即 next_id 备用)
    /// - `[8..64]`: 56B name entries 区, 每个 entry = `[u8 len][len B 字符串]`,
    ///   余 0 填充.
    pub fn serialize(&self) -> [u8; RESOLVER_HEADER_SIZE] {
        let mut buf = [0u8; RESOLVER_HEADER_SIZE];
        let count = self.names.len() as u32;
        let next_id = self.next_id();
        buf[0..4].copy_from_slice(&count.to_le_bytes());
        buf[4..8].copy_from_slice(&next_id.to_le_bytes());

        let mut off = 8;
        for name in &self.names {
            let bytes = name.as_bytes();
            // 防御: 调用方应已校验, 但兜底再检查一次
            debug_assert!(bytes.len() <= u8::MAX as usize);
            debug_assert!(off + 1 + bytes.len() <= RESOLVER_HEADER_SIZE);
            buf[off] = bytes.len() as u8;
            off += 1;
            buf[off..off + bytes.len()].copy_from_slice(bytes);
            off += bytes.len();
        }
        // 余 0 填充 (默认)
        buf
    }

    /// 从 64 字节反序列化.
    pub fn deserialize(bytes: &[u8; RESOLVER_HEADER_SIZE]) -> Result<Self, ResolverError> {
        let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let _next_id = u32::from_le_bytes(bytes[4..8].try_into().unwrap());

        let mut names = Vec::with_capacity(count);
        let mut off = 8;
        for _ in 0..count {
            if off >= RESOLVER_HEADER_SIZE {
                return Err(ResolverError::InvalidData(format!(
                    "name entry header at off={off} out of bounds"
                )));
            }
            let name_len = bytes[off] as usize;
            off += 1;
            if off + name_len > RESOLVER_HEADER_SIZE {
                return Err(ResolverError::InvalidData(format!(
                    "name entry body at off={off} len={name_len} out of bounds"
                )));
            }
            let name_bytes = &bytes[off..off + name_len];
            let name = std::str::from_utf8(name_bytes)
                .map_err(|e| ResolverError::InvalidData(format!("non-utf8 name: {e}")))?;
            names.push(name.to_string());
            off += name_len;
        }

        Ok(Self::from_parts(names))
    }
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_registers_default() {
        let r = DbNameResolver::new();
        assert_eq!(r.count(), 1);
        assert_eq!(r.resolve("default"), Some(DEFAULT_DB_ID));
        assert_eq!(r.name(DEFAULT_DB_ID), Some("default"));
        assert_eq!(r.next_id(), 1);
    }

    #[test]
    fn default_impl_matches_new() {
        let r1 = DbNameResolver::default();
        let r2 = DbNameResolver::new();
        assert_eq!(r1.count(), r2.count());
        assert_eq!(r1.list(), r2.list());
    }

    #[test]
    fn get_or_create_returns_existing_id() {
        let mut r = DbNameResolver::new();
        let id1 = r.get_or_create("default").unwrap();
        let id2 = r.get_or_create("default").unwrap();
        assert_eq!(id1, id2);
        assert_eq!(id1, DEFAULT_DB_ID);
        assert_eq!(r.count(), 1);
    }

    #[test]
    fn get_or_create_assigns_new_id_incrementally() {
        let mut r = DbNameResolver::new();
        let id_a = r.get_or_create("analytics").unwrap();
        let id_b = r.get_or_create("users").unwrap();
        assert_eq!(id_a, 1);
        assert_eq!(id_b, 2);
        assert_eq!(r.count(), 3);
        assert_eq!(r.next_id(), 3);
    }

    #[test]
    fn resolve_returns_none_for_unknown() {
        let r = DbNameResolver::new();
        assert_eq!(r.resolve("nonexistent"), None);
    }

    #[test]
    fn name_returns_none_for_out_of_range_id() {
        let r = DbNameResolver::new();
        assert_eq!(r.name(999), None);
    }

    #[test]
    fn list_returns_by_id_order() {
        let mut r = DbNameResolver::new();
        r.get_or_create("z").unwrap();
        r.get_or_create("a").unwrap();
        r.get_or_create("m").unwrap();
        let list = r.list();
        assert_eq!(list, vec![(0, "default"), (1, "z"), (2, "a"), (3, "m"),]);
    }

    #[test]
    fn serialize_empty_default_fits_in_header() {
        let r = DbNameResolver::new();
        let buf = r.serialize();
        assert_eq!(buf.len(), RESOLVER_HEADER_SIZE);
        // count = 1
        assert_eq!(&buf[0..4], &1u32.to_le_bytes());
        // next_id = 1
        assert_eq!(&buf[4..8], &1u32.to_le_bytes());
        // "default" = 7 chars, [7, 'd', 'e', 'f', 'a', 'u', 'l', 't']
        assert_eq!(buf[8], 7);
        assert_eq!(&buf[9..16], b"default");
        // 余 1024-16 = 1008B 0
        for &b in &buf[16..] {
            assert_eq!(b, 0);
        }
    }

    #[test]
    fn round_trip_default() {
        let r = DbNameResolver::new();
        let buf = r.serialize();
        let loaded = DbNameResolver::deserialize(&buf).unwrap();
        assert_eq!(loaded.count(), 1);
        assert_eq!(loaded.resolve("default"), Some(0));
        assert_eq!(loaded.name(0), Some("default"));
    }

    #[test]
    fn round_trip_multiple_names() {
        let mut r = DbNameResolver::new();
        r.get_or_create("analytics").unwrap();
        r.get_or_create("users").unwrap();
        r.get_or_create("db_3").unwrap();

        let buf = r.serialize();
        let loaded = DbNameResolver::deserialize(&buf).unwrap();
        assert_eq!(loaded.count(), 4);
        assert_eq!(loaded.list(), r.list());
        assert_eq!(loaded.resolve("default"), Some(0));
        assert_eq!(loaded.resolve("analytics"), Some(1));
        assert_eq!(loaded.resolve("users"), Some(2));
        assert_eq!(loaded.resolve("db_3"), Some(3));
        assert_eq!(loaded.next_id(), 4);
    }

    #[test]
    fn name_too_long_errors() {
        let mut r = DbNameResolver::new();
        let huge = "a".repeat(RESOLVER_MAX_NAME_LEN + 1);
        let err = r.get_or_create(&huge).unwrap_err();
        assert!(matches!(err, ResolverError::NameTooLong(_, _)));
        // 不应被注册
        assert_eq!(r.resolve(&huge), None);
    }

    #[test]
    fn area_full_errors_after_capacity_exhausted() {
        // 1024B 头: 8B 元数据 + 1016B names 区.
        // "default" 7 字符 = 1+7 = 8B. 余 1008B.
        // 每个 7 字符名 = 1+7 = 8B. 1008 / 8 = 126 个 (1008B), 刚好填满.
        // 第 127 个需要 8B, 1008+8=1016 ≤ 1016, 还能, 但 default(8) + 127*8(1016) = 1024 > 1016
        // 实际: 8 + 125*8 = 1008B, 余 0B, 第 126 个 8B 不够 → Full
        let mut r = DbNameResolver::new();
        for i in 0..125 {
            let name = format!("db7_{:03}", i); // "db7_000"=7 chars
            r.get_or_create(&name).unwrap();
        }
        assert_eq!(r.count(), 126);
        // 第 126 个新增: used = 8 (default) + 125*8 = 1008B, need 8B, 1008+8=1016 ≤ 1016 OK
        // 等等 125 加上 default 是 126 个, 第 127 个即 r.count() == 127
        r.get_or_create("db7_125").unwrap();
        assert_eq!(r.count(), 127);
        // 第 128 个: used = 8 + 126*8 = 1016B, need 8B, 1016+8=1024 > 1016 → Full
        let result = r.get_or_create("overflow");
        match result {
            Ok(_) => panic!("expected Full error, got Ok with count={}", r.count()),
            Err(ResolverError::Full(_)) => {}
            Err(e) => panic!("expected Full, got {:?}", e),
        }
        // 仍应是 127 个
        assert_eq!(r.count(), 127);
    }

    #[test]
    fn deserialize_invalid_count_with_truncated_data_errors() {
        // 真实越界场景: 第 5 个 entry 的 len 字段超出 1024B 头.
        // 前 4 个各占 100B, 第 5 个说还有 100B → 5*101 = 505 < 1024 不行.
        // 用最大单 entry 254B: 5 * 255 = 1275 > 1024.
        // 实际构造: 4 个 250B entry (252B 各), 第 5 个 200B.
        // 4*252 + 1*201 = 1008 + 201 = 1209 > 1024.
        let mut buf = [0u8; RESOLVER_HEADER_SIZE];
        buf[0..4].copy_from_slice(&5u32.to_le_bytes()); // count=5
        // 4 entries of "aaaa...a" (250 chars each)
        for i in 0..4 {
            let off = 8 + i * 251;
            buf[off] = 250;
            for j in 0..250 {
                buf[off + 1 + j] = b'a';
            }
        }
        // 第 5 个 entry 在 8 + 4*251 = 1012 处, 声称 100B → 1012+1+100 = 1113 > 1024
        buf[1012] = 100;
        let err = DbNameResolver::deserialize(&buf).unwrap_err();
        assert!(matches!(err, ResolverError::InvalidData(_)));
    }

    #[test]
    fn deserialize_non_utf8_name_errors() {
        let mut buf = [0u8; RESOLVER_HEADER_SIZE];
        buf[0..4].copy_from_slice(&1u32.to_le_bytes());
        buf[8] = 2;
        buf[9] = 0xFF;
        buf[10] = 0xFE;
        let err = DbNameResolver::deserialize(&buf).unwrap_err();
        assert!(matches!(err, ResolverError::InvalidData(_)));
    }

    #[test]
    fn deserialize_zero_count_returns_empty_with_default() {
        // 故意构造 count=0 (损坏的 header) — 应当 OK (返回仅含默认的空 resolver)
        // 不, deserialize 是从持久化恢复, 不应添加默认. count=0 → 空 resolver, 调用方决定要不要加默认.
        let buf = [0u8; RESOLVER_HEADER_SIZE];
        let r = DbNameResolver::deserialize(&buf).unwrap();
        assert_eq!(r.count(), 0);
        assert_eq!(r.list(), vec![]);
    }

    #[test]
    fn resolver_error_display() {
        let e = ResolverError::Full("foo".to_string());
        assert!(format!("{}", e).contains("foo"));
        let e = ResolverError::NameTooLong(100, 48);
        assert!(format!("{}", e).contains("100"));
    }
}
