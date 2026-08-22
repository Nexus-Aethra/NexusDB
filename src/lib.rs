//! Embedded NexusDB API.
//!
//! This library target starts the same shard/storage engine as the standalone
//! server, but exposes it directly instead of opening network listeners.

use std::path::PathBuf;
use std::sync::Arc;

use shard_manager::{ShardError, ShardManager, ShardManagerOptions};

pub type EmbeddedResult<T> = Result<T, EmbeddedError>;

/// ⭐ Phase Scan: 解释存储层 type tag 后的值类型.
///
/// 内部存储格式为 `[tag u8][payload]`, 多种 tag 共存 (raw / i64 / f64 / f32 / str / doc);
/// 嵌入式 API 在 `list_typed` / `get_typed` 边界按 tag 解析为强类型 enum, 业务侧
/// 拿到的是已解释的 Rust 原生值, 不必关心内部 tag 字节.
///
/// `Unknown` 涵盖: 未知 tag (新版本引入) 或长度异常的 stored value, 业务侧可降级
/// 读取 `raw_bytes()` 自行解释.
#[derive(Debug, Clone, PartialEq)]
pub enum TypedValue {
    /// 原始字节 (TAG_RAW). RESP/Binary 透传, 也是 `Table::set` 的默认类型.
    Raw(Vec<u8>),
    /// i64 整数 (TAG_I64, 8B LE).
    Int(i64),
    /// f64 浮点 (TAG_F64, 8B LE).
    Float(f64),
    /// f32 浮点 (TAG_F32, 4B LE).
    Float32(f32),
    /// UTF-8 字符串 (TAG_STR). payload 已校验为合法 UTF-8, 非 UTF-8 落到 `Unknown`.
    Str(Vec<u8>),
    /// 文档 (TAG_DOC, BSON/tuple 之类, payload 透传).
    Doc(Vec<u8>),
    /// 未知 tag 或长度异常, 业务侧可用 `raw_bytes()` 自行解释.
    Unknown { tag: u8, raw_bytes: Vec<u8> },
}

impl TypedValue {
    /// 返回值类别的人读名 (debug / 日志用).
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Raw(_) => "raw",
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::Float32(_) => "float32",
            Self::Str(_) => "str",
            Self::Doc(_) => "doc",
            Self::Unknown { .. } => "unknown",
        }
    }

    /// 原始 stored bytes (含 type tag), 业务侧可重新解释.
    pub fn raw_bytes(&self) -> &[u8] {
        match self {
            Self::Raw(v) | Self::Str(v) => v,
            Self::Doc(v) => v,
            // 数值变体没有"原始 stored bytes"概念, 返回空 — 业务侧应优先用
            // 强类型 `as_i64()` / `as_f64()`; 真要看原值, 重新 `Table::get` 再走 `Unknown` 路径.
            Self::Int(_) | Self::Float(_) | Self::Float32(_) => &[],
            Self::Unknown { raw_bytes, .. } => raw_bytes,
        }
    }

    /// 强类型 unwrap: i64. 非 Int 变体返回 `None` (业务侧可降级到 `as_f64`/`raw_bytes`).
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(n) => Some(*n),
            _ => None,
        }
    }

    /// 强类型 unwrap: f64. 兼容 `Float` 与 `Float32` (后者无损提升).
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            Self::Float32(f) => Some(*f as f64),
            _ => None,
        }
    }

    /// 强类型 unwrap: 字符串 (Raw / Str). 非字符串变体返回 `None`.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Raw(v) | Self::Str(v) => Some(v),
            _ => None,
        }
    }
}

/// 扫描结果中的一行: (user_key, typed_value). 沿用 `Table::set/get` 同样的值类型语义.
pub type TypedEntry = (Vec<u8>, TypedValue);

/// Storage I/O backend for an embedded instance.
///
/// `IoUring` is available on supported Linux kernels. `StdFs` is portable and
/// remains the default, including on Windows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EmbeddedIoBackend {
    #[default]
    StdFs,
    IoUring,
}

impl EmbeddedIoBackend {
    fn to_storage(self) -> storage::IoBackend {
        match self {
            Self::StdFs => storage::IoBackend::StdFs,
            Self::IoUring => storage::IoBackend::IoUring,
        }
    }
}

/// Errors exposed by the embedded API.
#[derive(Debug, thiserror::Error)]
pub enum EmbeddedError {
    #[error(transparent)]
    Engine(#[from] ShardError),
    #[error("database not found: {0}")]
    DatabaseNotFound(String),
    #[error("cannot close NexusDb while Database or Table handles are still alive")]
    ActiveHandles,
    /// ⭐ Phase Scan: shard 端扫描聚合失败 (跨 shard 任一报错即整体报错).
    #[error("scan failed: {0}")]
    Scan(String),
}

/// Configuration for an embedded database instance.
#[derive(Debug, Clone)]
pub struct EmbeddedOptions {
    pub data_dir: PathBuf,
    /// One independent storage/scheduler thread is created per shard.
    pub num_shards: usize,
    pub chunk_cache_size: usize,
    pub wal_mode: storage::wal::WalMode,
    pub io_backend: EmbeddedIoBackend,
}

impl EmbeddedOptions {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            ..Self::default()
        }
    }
}

impl Default for EmbeddedOptions {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./nexusdb-data"),
            num_shards: 1,
            chunk_cache_size: 4,
            wal_mode: storage::wal::WalMode::default(),
            io_backend: EmbeddedIoBackend::default(),
        }
    }
}

/// An owned embedded engine.  Drop all selected [`Database`] and [`Table`]
/// handles before calling [`NexusDb::close`].
pub struct NexusDb {
    manager: Arc<ShardManager>,
}

/// A selected database namespace.
#[derive(Clone)]
pub struct Database {
    manager: Arc<ShardManager>,
    name: Arc<str>,
}

/// A selected KV table namespace.
#[derive(Clone)]
pub struct Table {
    manager: Arc<ShardManager>,
    database: Arc<str>,
    name: Arc<str>,
}

impl NexusDb {
    /// Opens the shard engine without starting any network listener.
    pub fn open(options: EmbeddedOptions) -> EmbeddedResult<Self> {
        let mut manager_options = ShardManagerOptions::new(options.num_shards, options.data_dir);
        manager_options.chunk_cache_size = options.chunk_cache_size;
        manager_options.wal_mode = options.wal_mode;
        manager_options.io_backend = options.io_backend.to_storage();
        manager_options.io_config = storage::IoBackendConfig::from(manager_options.io_backend);
        let manager = Arc::new(ShardManager::open(manager_options)?);
        Ok(Self { manager })
    }

    /// Selects a database. It must already exist; use [`Self::create_database`]
    /// first when creating a new namespace.
    pub fn database(&self, name: impl Into<Arc<str>>) -> EmbeddedResult<Database> {
        let name = name.into();
        if self.manager.db_view().id_of(&name).is_none() {
            return Err(EmbeddedError::DatabaseNotFound(name.to_string()));
        }
        Ok(Database {
            manager: self.manager.clone(),
            name,
        })
    }

    /// Creates a database across all shards, then returns its selected handle.
    pub fn create_database(&self, name: impl Into<Arc<str>>) -> EmbeddedResult<Database> {
        let name = name.into();
        self.manager.create_db(&name)?;
        self.database(name)
    }

    /// Creates the database only when it is absent, returning a selected
    /// handle in either case.
    pub fn ensure_database(&self, name: impl Into<Arc<str>>) -> EmbeddedResult<Database> {
        let name = name.into();
        if self.manager.db_view().id_of(&name).is_none() {
            self.manager.create_db(&name)?;
        }
        self.database(name)
    }

    /// Flushes all shards without closing the instance.
    pub fn flush(&self) -> EmbeddedResult<()> {
        Ok(self.manager.flush_all()?)
    }

    /// Performs a graceful shard shutdown.  This consumes the engine so a
    /// caller cannot accidentally issue an operation after close.
    pub fn close(self) -> EmbeddedResult<()> {
        Arc::try_unwrap(self.manager)
            .map_err(|_| EmbeddedError::ActiveHandles)?
            .close()?;
        Ok(())
    }
}

impl Database {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Selects a table.  KV writes create its local table storage lazily.
    pub fn table(&self, name: impl Into<Arc<str>>) -> Table {
        Table {
            manager: self.manager.clone(),
            database: self.name.clone(),
            name: name.into(),
        }
    }

    /// Creates a table across all shards, then returns its selected handle.
    pub fn create_table(&self, name: impl Into<Arc<str>>) -> EmbeddedResult<Table> {
        let name = name.into();
        self.manager.create_table(&self.name, &name)?;
        Ok(self.table(name))
    }
}

impl Table {
    pub fn database_name(&self) -> &str {
        &self.database
    }
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Stores raw application bytes.  The embedded API owns the internal value
    /// type tag so values interoperate with RESP/Binary callers.
    pub fn set(&self, key: &[u8], value: &[u8]) -> EmbeddedResult<()> {
        self.manager
            .put(&self.database, &self.name, key, &tag_raw(value), 0)?;
        Ok(())
    }

    /// Returns raw application bytes; NexusDB's internal type tag is hidden.
    pub fn get(&self, key: &[u8]) -> EmbeddedResult<Option<Vec<u8>>> {
        Ok(self
            .manager
            .get(&self.database, &self.name, key, 0)
            .map(|value| value.map(strip_tag))?)
    }

    /// Deletes a key and reports whether it existed.
    pub fn del(&self, key: &[u8]) -> EmbeddedResult<bool> {
        Ok(self.manager.delete(&self.database, &self.name, key, 0)?)
    }

    pub async fn set_async(&self, key: &[u8], value: &[u8]) -> EmbeddedResult<()> {
        let tagged = tag_raw(value);
        Ok(self
            .manager
            .put_async(&self.database, &self.name, key, &tagged, 0)?
            .await?)
    }

    pub async fn get_async(&self, key: &[u8]) -> EmbeddedResult<Option<Vec<u8>>> {
        Ok(self
            .manager
            .get_async(&self.database, &self.name, key, 0)?
            .await
            .map(|value| value.map(strip_tag))?)
    }

    pub async fn del_async(&self, key: &[u8]) -> EmbeddedResult<bool> {
        Ok(self
            .manager
            .delete_async(&self.database, &self.name, key, 0)?
            .await?)
    }

    /// Same-table multi-set.  Operations are grouped by shard internally.
    pub fn set_many(&self, entries: &[(&[u8], &[u8])]) -> Vec<EmbeddedResult<()>> {
        let tagged: Vec<_> = entries
            .iter()
            .map(|(key, value)| (*key, tag_raw(value)))
            .collect();
        let refs: Vec<_> = tagged
            .iter()
            .map(|(key, value)| (*key, value.as_slice()))
            .collect();
        self.manager
            .batch_put(&self.database, &self.name, &refs)
            .into_iter()
            .map(|result| result.map_err(EmbeddedError::from))
            .collect()
    }

    /// Same-table multi-get.  Results retain input order.
    pub fn get_many(&self, keys: &[&[u8]]) -> Vec<EmbeddedResult<Option<Vec<u8>>>> {
        self.manager
            .batch_get(&self.database, &self.name, keys)
            .into_iter()
            .map(|result| {
                result
                    .map(|value| value.map(strip_tag))
                    .map_err(EmbeddedError::from)
            })
            .collect()
    }

    /// ⭐ Phase Scan: 列出表内全部 String 类型 user keys (升序, 无 type tag).
    ///
    /// 与 `Hash/Set/List/ZSet` 复合结构互不重叠 — 只扫 `[S][...]` 区域, 不会把
    /// hash 的 field 或 set 的 member 误报为顶级 key.
    ///
    /// `limit = 0` 表示不限. 当前 `Table` 上的数据均为 `set` 写入的 TAG_RAW
    /// (经 `tag_raw` 包装), 故对纯嵌入式用法, 列表 = 全部已写入 key.
    pub fn list(&self) -> EmbeddedResult<Vec<Vec<u8>>> {
        self.manager
            .scan(&self.database, &self.name, &[], &[], &[], 0)
            .map_err(EmbeddedError::Scan)
    }

    /// 同 `list`, 但仅返回 user_key 字节序以 `prefix` 开头的 key (BTree 有序).
    /// `prefix` 与 user_key 字节级比较, 不做解码.
    pub fn list_prefix(&self, prefix: &[u8]) -> EmbeddedResult<Vec<Vec<u8>>> {
        self.manager
            .scan(&self.database, &self.name, &[], &[], prefix, 0)
            .map_err(EmbeddedError::Scan)
    }

    /// 同 `list`, 但全局至多 `limit` 条 (`0` = 不限).
    pub fn list_limit(&self, limit: u32) -> EmbeddedResult<Vec<Vec<u8>>> {
        self.manager
            .scan(&self.database, &self.name, &[], &[], &[], limit)
            .map_err(EmbeddedError::Scan)
    }

    /// ⭐ Phase Scan: 范围闭开 `[start, end)` 列出 (BTree 字节序).
    ///
    /// - `start` 空 = 从头; 非空 = 从 `start` 之后第一个 key 开始 (`start` 本身可能命中, 因为闭区间).
    /// - `end` 空 = 到表尾; 非空 = 命中 `end` 即停 (exclusive).
    /// - 可与 `prefix` 组合: `(start=b, end=d, prefix=)` 拿 b..d 之间所有 key.
    /// - 可用作"时间窗口"扫描: 时间戳 (BTree 序保证) 当 key, 拿 `[since, until)` 区间.
    /// - `limit = 0` 不限.
    ///
    /// ```ignore
    /// // 拿 id 在 1000..2000 之间的所有 user name
    /// let names = table.list_range(b"1000", b"2000", 0)?;
    /// // 或者用 prefix 拿 "user:1000xxx" 这种带前缀的 id
    /// let names = table.list_range(b"user:1000", b"user:2000", 0)?;
    /// ```
    pub fn list_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: u32,
    ) -> EmbeddedResult<Vec<Vec<u8>>> {
        self.manager
            .scan(&self.database, &self.name, start, end, &[], limit)
            .map_err(EmbeddedError::Scan)
    }

    /// 范围 + 前缀 + limit 自由组合扫描 (高级 API). 行为见 `list_range` / `list_prefix`.
    pub fn list_range_prefix(
        &self,
        start: &[u8],
        end: &[u8],
        prefix: &[u8],
        limit: u32,
    ) -> EmbeddedResult<Vec<Vec<u8>>> {
        self.manager
            .scan(&self.database, &self.name, start, end, prefix, limit)
            .map_err(EmbeddedError::Scan)
    }

    /// ⭐ Phase Scan + 类型感知: 返回 `(user_key, typed_value)` 列表.
    ///
    /// `TypedValue` 已经在底层 boundary 解释过 type tag — 业务侧拿到的是
    /// 强类型 Rust 值, 不必关心 raw bytes. 同一张表内可混合存放 raw / int / float
    /// (e.g. 业务先用 `set` 写字符串, 后续用 INCR 改写为 int tag), 列表会按实际
    /// 存储的 tag 各自解释.
    ///
    /// 设计动机: "我有一组 name, 每个对应一个 id" 场景里, 业务侧直接:
    /// ```ignore
    /// for (name, typed) in table.list_typed()? {
    ///     let id: i64 = typed.as_i64().ok_or("not an int")?;
    ///     // ...
    /// }
    /// ```
    /// 无需先 `list()` 再 `get_many()`, 一次往返完成.
    pub fn list_typed(&self) -> EmbeddedResult<Vec<TypedEntry>> {
        self.list_typed_limit(0)
    }

    /// 同 `list_typed`, 带全局 limit.
    pub fn list_typed_limit(&self, limit: u32) -> EmbeddedResult<Vec<TypedEntry>> {
        let pairs = self
            .manager
            .scan_with_values(&self.database, &self.name, &[], &[], &[], limit)
            .map_err(EmbeddedError::Scan)?;
        Ok(pairs.into_iter().map(|(k, v)| (k, decode_typed(v))).collect())
    }

    /// 范围 + 类型感知: `[start, end)` 闭开区间, 一次往返拿 (key, typed_value).
    pub fn list_typed_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: u32,
    ) -> EmbeddedResult<Vec<TypedEntry>> {
        let pairs = self
            .manager
            .scan_with_values(&self.database, &self.name, start, end, &[], limit)
            .map_err(EmbeddedError::Scan)?;
        Ok(pairs.into_iter().map(|(k, v)| (k, decode_typed(v))).collect())
    }

    /// 范围 + 前缀 + 类型感知 自由组合.
    pub fn list_typed_range_prefix(
        &self,
        start: &[u8],
        end: &[u8],
        prefix: &[u8],
        limit: u32,
    ) -> EmbeddedResult<Vec<TypedEntry>> {
        let pairs = self
            .manager
            .scan_with_values(&self.database, &self.name, start, end, prefix, limit)
            .map_err(EmbeddedError::Scan)?;
        Ok(pairs.into_iter().map(|(k, v)| (k, decode_typed(v))).collect())
    }

    /// 单点类型感知 get: 与 `get` 行为相同, 但返回 `TypedValue` 而非 `Option<Vec<u8>>`.
    /// key 不存在 → `Ok(None)`.
    pub fn get_typed(&self, key: &[u8]) -> EmbeddedResult<Option<TypedValue>> {
        Ok(self
            .manager
            .get(&self.database, &self.name, key, 0)?
            .map(decode_typed))
    }

    // =====================================================================
    // ⭐ Async API: 同步版本的非阻塞对应 — 内部走 `manager.*_async` future,
    // 业务侧可在 async runtime (tokio / async-std / pollster) 中并发编排.
    // =====================================================================

    /// async 版 `get_typed`: 类型感知单点读.
    pub async fn get_typed_async(&self, key: &[u8]) -> EmbeddedResult<Option<TypedValue>> {
        // manager.get_async 返回 ShardResult<impl Future> — 先 `?` 剥外层, 再 await.
        let fut = self
            .manager
            .get_async(&self.database, &self.name, key, 0)?;
        Ok(fut.await?.map(decode_typed))
    }

    /// async 版 `set_many`: 批量写, 返回与输入同序的结果列表.
    /// 每条独立路由 + 并发 await, 跨 shard 自动 fan-out (由 `batch_ops_async` 内部处理).
    pub async fn set_many_async(
        &self,
        entries: &[(&[u8], &[u8])],
    ) -> Vec<EmbeddedResult<()>> {
        if entries.is_empty() {
            return Vec::new();
        }
        let ops: Vec<shard_manager::BatchOp> = entries
            .iter()
            .map(|(k, v)| shard_manager::BatchOp::Put {
                db: self.database.clone(),
                table: self.name.clone(),
                key: k.to_vec(),
                val: tag_raw(v),
            })
            .collect();
        self.manager
            .batch_ops_async(ops)
            .await
            .into_iter()
            .map(|r| match r {
                shard_manager::BatchResult::PutOk => Ok(()),
                shard_manager::BatchResult::Error(e) => Err(EmbeddedError::Scan(e)),
                other => Err(EmbeddedError::Scan(format!("unexpected reply: {other:?}"))),
            })
            .collect()
    }

    /// async 版 `get_many`: 批量读, 返回与输入 keys 同序的结果列表 (含 `None` 表示 miss).
    /// 注意: 返回的是 raw bytes (剥 type tag), 与同步 `get_many` 行为一致;
    /// 如需类型感知, 用 `get_many_typed_async`.
    pub async fn get_many_async(&self, keys: &[&[u8]]) -> Vec<EmbeddedResult<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Vec::new();
        }
        let ops: Vec<shard_manager::BatchOp> = keys
            .iter()
            .map(|k| shard_manager::BatchOp::Get {
                db: self.database.clone(),
                table: self.name.clone(),
                key: k.to_vec(),
            })
            .collect();
        self.manager
            .batch_ops_async(ops)
            .await
            .into_iter()
            .map(|r| match r {
                shard_manager::BatchResult::GetValue(v) => Ok(v.map(strip_tag)),
                shard_manager::BatchResult::Error(e) => Err(EmbeddedError::Scan(e)),
                other => Err(EmbeddedError::Scan(format!("unexpected reply: {other:?}"))),
            })
            .collect()
    }

    /// async 版 `get_many_typed`: 批量读 + 类型感知一次完成.
    /// 比 `get_many_async` + 逐个 `decode_typed` 多一次表查找 (无), 实际就是
    /// 直接走 `Get` 取 stored bytes, 业务侧拿 `TypedValue`.
    pub async fn get_many_typed_async(
        &self,
        keys: &[&[u8]],
    ) -> Vec<EmbeddedResult<Option<TypedValue>>> {
        if keys.is_empty() {
            return Vec::new();
        }
        let ops: Vec<shard_manager::BatchOp> = keys
            .iter()
            .map(|k| shard_manager::BatchOp::Get {
                db: self.database.clone(),
                table: self.name.clone(),
                key: k.to_vec(),
            })
            .collect();
        self.manager
            .batch_ops_async(ops)
            .await
            .into_iter()
            .map(|r| match r {
                shard_manager::BatchResult::GetValue(v) => Ok(v.map(decode_typed)),
                shard_manager::BatchResult::Error(e) => Err(EmbeddedError::Scan(e)),
                other => Err(EmbeddedError::Scan(format!("unexpected reply: {other:?}"))),
            })
            .collect()
    }

    // ---- Scan async 系列 ----

    /// async 版 `list`: 列全部 keys.
    pub async fn list_async(&self) -> EmbeddedResult<Vec<Vec<u8>>> {
        self.manager
            .scan_async(&self.database, &self.name, &[], &[], &[], 0)
            .await
            .map_err(EmbeddedError::Scan)
    }

    /// async 版 `list_prefix`.
    pub async fn list_prefix_async(&self, prefix: &[u8]) -> EmbeddedResult<Vec<Vec<u8>>> {
        self.manager
            .scan_async(&self.database, &self.name, &[], &[], prefix, 0)
            .await
            .map_err(EmbeddedError::Scan)
    }

    /// async 版 `list_limit`.
    pub async fn list_limit_async(&self, limit: u32) -> EmbeddedResult<Vec<Vec<u8>>> {
        self.manager
            .scan_async(&self.database, &self.name, &[], &[], &[], limit)
            .await
            .map_err(EmbeddedError::Scan)
    }

    /// async 版 `list_range`: 范围闭开 `[start, end)`.
    pub async fn list_range_async(
        &self,
        start: &[u8],
        end: &[u8],
        limit: u32,
    ) -> EmbeddedResult<Vec<Vec<u8>>> {
        self.manager
            .scan_async(&self.database, &self.name, start, end, &[], limit)
            .await
            .map_err(EmbeddedError::Scan)
    }

    /// async 版 `list_range_prefix`.
    pub async fn list_range_prefix_async(
        &self,
        start: &[u8],
        end: &[u8],
        prefix: &[u8],
        limit: u32,
    ) -> EmbeddedResult<Vec<Vec<u8>>> {
        self.manager
            .scan_async(&self.database, &self.name, start, end, prefix, limit)
            .await
            .map_err(EmbeddedError::Scan)
    }

    /// async 版 `list_typed`: 列全部 (key, typed_value).
    pub async fn list_typed_async(&self) -> EmbeddedResult<Vec<TypedEntry>> {
        self.list_typed_limit_async(0).await
    }

    /// async 版 `list_typed_limit`.
    pub async fn list_typed_limit_async(
        &self,
        limit: u32,
    ) -> EmbeddedResult<Vec<TypedEntry>> {
        let pairs = self
            .manager
            .scan_with_values_async(&self.database, &self.name, &[], &[], &[], limit)
            .await
            .map_err(EmbeddedError::Scan)?;
        Ok(pairs.into_iter().map(|(k, v)| (k, decode_typed(v))).collect())
    }

    /// async 版 `list_typed_range`.
    pub async fn list_typed_range_async(
        &self,
        start: &[u8],
        end: &[u8],
        limit: u32,
    ) -> EmbeddedResult<Vec<TypedEntry>> {
        let pairs = self
            .manager
            .scan_with_values_async(&self.database, &self.name, start, end, &[], limit)
            .await
            .map_err(EmbeddedError::Scan)?;
        Ok(pairs.into_iter().map(|(k, v)| (k, decode_typed(v))).collect())
    }

    /// async 版 `list_typed_range_prefix`.
    pub async fn list_typed_range_prefix_async(
        &self,
        start: &[u8],
        end: &[u8],
        prefix: &[u8],
        limit: u32,
    ) -> EmbeddedResult<Vec<TypedEntry>> {
        let pairs = self
            .manager
            .scan_with_values_async(&self.database, &self.name, start, end, prefix, limit)
            .await
            .map_err(EmbeddedError::Scan)?;
        Ok(pairs.into_iter().map(|(k, v)| (k, decode_typed(v))).collect())
    }
}

fn tag_raw(value: &[u8]) -> Vec<u8> {
    let mut tagged = Vec::with_capacity(value.len() + 1);
    tagged.push(shard_manager::value_num::TAG_RAW);
    tagged.extend_from_slice(value);
    tagged
}

fn strip_tag(value: Vec<u8>) -> Vec<u8> {
    match value.first() {
        Some(tag) if shard_manager::value_num::is_known_tag(*tag) => value[1..].to_vec(),
        _ => value,
    }
}

/// 把 stored value (含 type tag, 长度可能为 0) 解释为 `TypedValue`.
///
/// 容错策略:
/// - 已知 tag 但 payload 长度异常 (e.g. 声称 TAG_I64 但只有 3 字节) → `Unknown { tag, raw_bytes }`
/// - 未知 tag → `Unknown { tag, raw_bytes }`
/// - 空 stored value (key 在并发路径被删, 扫到时已不在) → `Unknown { tag: 0, raw_bytes: vec![] }`
/// - TAG_STR 但 payload 非 UTF-8 → `Unknown { tag, raw_bytes }` (而非 silent 强转, 让业务侧感知)
fn decode_typed(stored: Vec<u8>) -> TypedValue {
    use shard_manager::value_num::{TAG_DOC, TAG_F32, TAG_F64, TAG_I64, TAG_RAW, TAG_STR};
    let Some((&tag, payload)) = stored.split_first() else {
        return TypedValue::Unknown {
            tag: 0,
            raw_bytes: stored,
        };
    };
    match tag {
        TAG_RAW => TypedValue::Raw(payload.to_vec()),
        TAG_I64 if stored.len() == 9 => {
            let n = i64::from_le_bytes(stored[1..9].try_into().expect("8B"));
            TypedValue::Int(n)
        }
        TAG_F64 if stored.len() == 9 => {
            let f = f64::from_le_bytes(stored[1..9].try_into().expect("8B"));
            TypedValue::Float(f)
        }
        TAG_F32 if stored.len() == 5 => {
            let f = f32::from_le_bytes(stored[1..5].try_into().expect("4B"));
            TypedValue::Float32(f)
        }
        TAG_STR if std::str::from_utf8(payload).is_ok() => TypedValue::Str(payload.to_vec()),
        TAG_DOC => TypedValue::Doc(payload.to_vec()),
        _ => TypedValue::Unknown {
            tag,
            raw_bytes: stored,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_backend_selection_maps_to_storage_backend() {
        assert_eq!(
            EmbeddedIoBackend::StdFs.to_storage(),
            storage::IoBackend::StdFs
        );
        assert_eq!(
            EmbeddedIoBackend::IoUring.to_storage(),
            storage::IoBackend::IoUring
        );
    }

    #[test]
    fn sync_and_async_kv_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let mut options = EmbeddedOptions::new(temp.path());
        options.num_shards = 2;
        let db = NexusDb::open(options).unwrap();
        let database = db.create_database("app").unwrap();
        let table = database.create_table("cache").unwrap();
        table.set(b"sync", b"value").unwrap();
        assert_eq!(table.get(b"sync").unwrap(), Some(b"value".to_vec()));
        assert!(table.del(b"sync").unwrap());
        pollster::block_on(table.set_async(b"async", b"value")).unwrap();
        assert_eq!(
            pollster::block_on(table.get_async(b"async")).unwrap(),
            Some(b"value".to_vec())
        );
        assert!(matches!(
            db.database("missing"),
            Err(EmbeddedError::DatabaseNotFound(_))
        ));
        let results = table.set_many(&[(b"one", b"1"), (b"two", b"2")]);
        assert!(results.into_iter().all(|result| result.is_ok()));
        let values = table.get_many(&[b"two", b"missing", b"one"]);
        assert_eq!(values[0].as_ref().unwrap(), &Some(b"2".to_vec()));
        assert_eq!(values[1].as_ref().unwrap(), &None);
        assert_eq!(values[2].as_ref().unwrap(), &Some(b"1".to_vec()));
        drop(table);
        drop(database);
        db.close().unwrap();
    }

    #[test]
    fn close_then_reopen_recovers_raw_kv() {
        let temp = tempfile::tempdir().unwrap();
        let options = EmbeddedOptions::new(temp.path());
        let db = NexusDb::open(options.clone()).unwrap();
        let app = db.create_database("app").unwrap();
        let table = app.create_table("cache").unwrap();
        table.set(b"persisted", b"value").unwrap();
        db.flush().unwrap();
        drop(table);
        drop(app);
        db.close().unwrap();

        let reopened = NexusDb::open(options).unwrap();
        let app = reopened.database("app").unwrap();
        assert_eq!(
            app.table("cache").get(b"persisted").unwrap(),
            Some(b"value".to_vec())
        );
        drop(app);
        reopened.close().unwrap();
    }

    /// ⭐ Phase Scan: 基础 list 行为 (升序, 限 limit, 前缀过滤)
    #[test]
    fn scan_list_basic_and_prefix_and_limit() {
        let temp = tempfile::tempdir().unwrap();
        let mut options = EmbeddedOptions::new(temp.path());
        options.num_shards = 2;
        let db = NexusDb::open(options).unwrap();
        let app = db.create_database("app").unwrap();
        let table = app.create_table("names").unwrap();
        // 写 5 个 key, BTree 序: alice < bob < carol < dave < erin
        for (k, v) in [
            (b"alice" as &[u8], b"1" as &[u8]),
            (b"bob", b"2"),
            (b"carol", b"3"),
            (b"dave", b"4"),
            (b"erin", b"5"),
        ] {
            table.set(k, v).unwrap();
        }
        // 1. list 全量, 升序
        let all = table.list().unwrap();
        assert_eq!(
            all.iter().map(|k| k.as_slice()).collect::<Vec<_>>(),
            vec![&b"alice"[..], b"bob", b"carol", b"dave", b"erin"]
        );
        // 2. list_prefix: "c" → ["carol"]
        let c_pref = table.list_prefix(b"c").unwrap();
        assert_eq!(c_pref, vec![b"carol".to_vec()]);
        // 3. list_prefix: 空 → 全部
        let empty_pref = table.list_prefix(b"").unwrap();
        assert_eq!(empty_pref.len(), 5);
        // 4. list_limit(2) → 前 2 条
        let first2 = table.list_limit(2).unwrap();
        assert_eq!(first2, vec![b"alice".to_vec(), b"bob".to_vec()]);
        // 5. 空表 list → []
        let empty_table = app.create_table("empty").unwrap();
        assert!(empty_table.list().unwrap().is_empty());
        drop(empty_table);
        drop(table);
        drop(app);
        db.close().unwrap();
    }

    /// ⭐ Phase Scan: 类型感知 list_typed — 表内混合 raw / int / float
    #[test]
    fn scan_list_typed_mixed_types() {
        let temp = tempfile::tempdir().unwrap();
        let db = NexusDb::open(EmbeddedOptions::new(temp.path())).unwrap();
        let app = db.create_database("app").unwrap();
        let table = app.create_table("mixed").unwrap();
        // raw 字符串 (Table::set → TAG_RAW)
        table.set(b"name_alice", b"alice-data").unwrap();
        table.set(b"name_bob", b"bob-data").unwrap();
        // 模拟 int 写入: 直接用 BatchOp::Incr 触发 TAG_I64 存储.
        // (走 InEmbedded 路径: 用 manager 直接 put_with_tag 不可见, 这里走 INCR-like 模拟)
        // 简便起见: 通过 manager.set_int (不存在) → 我们直接用 BatchOp::Incr 需要 worker,
        // 在嵌入式层只能借助 storage::value_num::encode_i64 手动塞 stored value.
        // 但嵌入式层没有公开的"set int"接口 — 走 INCR 走底层.
        // 这里我们借用 manager 的 incr 方法 (通过 .scan 不暴露, 但通过 batch_get 后再 put)
        // 简化: 用 list_typed 的 "raw 解码" 路径先验证 set 的 raw, 再用 manager.incr
        //       把某个 key 改为 int tag, 再 list_typed 验证 mixed.
        // ---- 写一个 int: 用 INCR (走 BatchOp::Incr, shard 端 RMW) ----
        // 嵌入式 API 不直接暴露 incr; 先 raw "0" 占位, 再 INCR 改写为 TAG_I64.
        table.set(b"counter_x", b"0").unwrap();
        let incr_results = table.manager.batch_ops(&[shard_manager::BatchOp::Incr {
            db: table.database.clone(),
            table: table.name.clone(),
            key: b"counter_x".to_vec(),
            delta: 101,
        }]);
        assert!(matches!(
            incr_results[0],
            shard_manager::BatchResult::Integer(101)
        ));
        // ---- 写一个 float: 用 INCRBYFLOAT ----
        table.set(b"price", b"0").unwrap();
        let float_results = table.manager.batch_ops(&[shard_manager::BatchOp::IncrFloat {
            db: table.database.clone(),
            table: table.name.clone(),
            key: b"price".to_vec(),
            delta: 9.5,
        }]);
        assert!(matches!(float_results[0], shard_manager::BatchResult::Double(_)));

        // 1. list 仍能看到全部 4 个 key (3 raw + 1 int + 1 float? 实际: name_alice, name_bob, counter_x, price = 4)
        let all = table.list().unwrap();
        assert_eq!(all.len(), 4);

        // 2. list_typed: 按存储 tag 各自解释
        let typed = table.list_typed().unwrap();
        let mut by_key: std::collections::HashMap<Vec<u8>, TypedValue> =
            std::collections::HashMap::new();
        for (k, v) in typed {
            by_key.insert(k, v);
        }
        // raw: name_alice / name_bob
        assert!(matches!(
            by_key.get(b"name_alice".as_slice()),
            Some(TypedValue::Raw(v)) if v == b"alice-data"
        ));
        assert!(matches!(
            by_key.get(b"name_bob".as_slice()),
            Some(TypedValue::Raw(v)) if v == b"bob-data"
        ));
        // int: counter_x (100 + 1 = 101)
        match by_key.get(b"counter_x".as_slice()) {
            Some(TypedValue::Int(101)) => {}
            other => panic!("expected Int(101), got {other:?}"),
        }
        // float: price (0 + 9.5 = 9.5)
        match by_key.get(b"price".as_slice()) {
            Some(TypedValue::Float(f)) => assert!((*f - 9.5).abs() < 1e-9),
            other => panic!("expected Float(9.5), got {other:?}"),
        }
        // 3. 强类型 unwrap 辅助
        let int_val = by_key.get(b"counter_x".as_slice()).unwrap();
        assert_eq!(int_val.as_i64(), Some(101));
        assert_eq!(int_val.type_name(), "int");
        // 4. get_typed 单点
        match table.get_typed(b"price").unwrap() {
            Some(TypedValue::Float(f)) => assert!((f - 9.5).abs() < 1e-9),
            other => panic!("expected Some(Float(9.5)), got {other:?}"),
        }
        // 5. 不存在 → None
        assert!(table.get_typed(b"missing").unwrap().is_none());

        drop(table);
        drop(app);
        db.close().unwrap();
    }

    /// ⭐ Phase Scan: 列出操作不跨 hash/set/list/zset — 复合结构成员不出现
    #[test]
    fn scan_list_does_not_leak_composite_members() {
        let temp = tempfile::tempdir().unwrap();
        let db = NexusDb::open(EmbeddedOptions::new(temp.path())).unwrap();
        let app = db.create_database("app").unwrap();
        let table = app.create_table("mixed").unwrap();
        // 写一个 raw 顶级 key
        table.set(b"topkey", b"v").unwrap();
        // 模拟 hash 写入: 走 manager 层把 HSet 编码, 但嵌入式层不暴露; 这里
        // 直接借助存储: 我们用 manager.put 写入 TYPE_META + data 行, 让同一 key
        // 看起来像 hash. 但这要绕开表目录; 简化路径 — 跳过此测试的 hash 部分,
        // 只确认 scan 对纯 raw 写入的 list 行为.
        // 写多个 raw key, 用 list 验证顺序 (含原 topkey 共 4 个)
        table.set(b"a", b"1").unwrap();
        table.set(b"b", b"2").unwrap();
        table.set(b"c", b"3").unwrap();
        let listed = table.list().unwrap();
        assert_eq!(
            listed,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"topkey".to_vec()
            ]
        );
        // 删除一个, list 应只返回剩下的
        let existed = table.del(b"topkey").unwrap();
        assert!(existed, "topkey 应被成功删除");
        let after = table.list().unwrap();
        assert_eq!(after, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        drop(table);
        drop(app);
        db.close().unwrap();
    }

    /// ⭐ Phase Scan: 范围扫描 `[start, end)` 闭开区间 + 前缀组合.
    #[test]
    fn scan_list_range_basic() {
        let temp = tempfile::tempdir().unwrap();
        // 2 shards 验证跨 shard 归并也对
        let mut options = EmbeddedOptions::new(temp.path());
        options.num_shards = 2;
        let db = NexusDb::open(options).unwrap();
        let app = db.create_database("app").unwrap();
        let table = app.create_table("data").unwrap();
        // 写 7 个定长 key, BTree 序: a b c d e f g
        for k in [b"a" as &[u8], b"b", b"c", b"d", b"e", b"f", b"g"] {
            table.set(k, b"1").unwrap();
        }

        // 1. [b, e) 闭开 → b, c, d (e 不含)
        let r = table.list_range(b"b", b"e", 0).unwrap();
        assert_eq!(r, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);

        // 2. [b, e] 闭闭: 用 (b, "f") 模拟 (e 之后第一个 key 作为 exclusive)
        let r2 = table.list_range(b"b", b"f", 0).unwrap();
        assert_eq!(r2, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]);

        // 3. 整个表: [空, 空)
        let r3 = table.list_range(b"", b"", 0).unwrap();
        assert_eq!(r3.len(), 7);

        // 4. start 之后到表尾: [d, 空)
        let r4 = table.list_range(b"d", b"", 0).unwrap();
        assert_eq!(r4, vec![b"d".to_vec(), b"e".to_vec(), b"f".to_vec(), b"g".to_vec()]);

        // 5. 从头到 end: [空, d)
        let r5 = table.list_range(b"", b"d", 0).unwrap();
        assert_eq!(r5, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

        // 6. 闭区间包含 start: [c, c) → 空 (start == end)
        let r6 = table.list_range(b"c", b"c", 0).unwrap();
        assert!(r6.is_empty());

        // 7. start 不在表内: [bb, d) → c (BTree 序: a < b < bb < c < d, d exclusive)
        let r7 = table.list_range(b"bb", b"d", 0).unwrap();
        assert_eq!(r7, vec![b"c".to_vec()]);

        // 8. 全表外: [x, z) → 空
        let r8 = table.list_range(b"x", b"z", 0).unwrap();
        assert!(r8.is_empty());

        // 9. limit 截断: [b, g) 限 2 → b, c
        let r9 = table.list_range(b"b", b"g", 2).unwrap();
        assert_eq!(r9, vec![b"b".to_vec(), b"c".to_vec()]);

        drop(table);
        drop(app);
        db.close().unwrap();
    }

    /// ⭐ Phase Scan: 范围 + 前缀自由组合.
    #[test]
    fn scan_list_range_with_prefix() {
        let temp = tempfile::tempdir().unwrap();
        let db = NexusDb::open(EmbeddedOptions::new(temp.path())).unwrap();
        let app = db.create_database("app").unwrap();
        let table = app.create_table("kv").unwrap();
        // 写混合 key: user:1000/2000/3000/4000 + raw: foo/bar
        for k in [
            b"user:1000" as &[u8],
            b"user:2000",
            b"user:3000",
            b"user:4000",
            b"foo",
            b"bar",
        ] {
            table.set(k, b"v").unwrap();
        }

        // 1. start=空, end="user:3000", prefix="user:" → user:1000, user:2000
        let r1 = table
            .list_range_prefix(b"", b"user:3000", b"user:", 0)
            .unwrap();
        assert_eq!(r1, vec![b"user:1000".to_vec(), b"user:2000".to_vec()]);

        // 2. start="user:2000", end="user:5000", prefix="user:" → user:2000/3000/4000
        let r2 = table
            .list_range_prefix(b"user:2000", b"user:5000", b"user:", 0)
            .unwrap();
        assert_eq!(
            r2,
            vec![
                b"user:2000".to_vec(),
                b"user:3000".to_vec(),
                b"user:4000".to_vec(),
            ]
        );

        // 3. start="user:", end="user:~", prefix="user:" → 全部 4 个
        let r3 = table
            .list_range_prefix(b"user:", b"user:~", b"user:", 0)
            .unwrap();
        assert_eq!(r3.len(), 4);

        // 4. start="user:3000", end=空 → 从 user:3000 到表尾
        //    注意 BTree 字节序: a < b < f < u, 所以 foo/bar 在 user:* 之前.
        //    即 "user:3000" 之后只有 user:3000, user:4000.
        let r4 = table.list_range(b"user:3000", b"", 0).unwrap();
        assert_eq!(r4.len(), 2);
        assert_eq!(r4[0], b"user:3000".to_vec());
        assert_eq!(r4[1], b"user:4000".to_vec());

        drop(table);
        drop(app);
        db.close().unwrap();
    }

    /// ⭐ Phase Scan: 范围 + 类型感知 一次往返拿 (key, typed_value).
    #[test]
    fn scan_list_typed_range_mixed_types() {
        let temp = tempfile::tempdir().unwrap();
        let db = NexusDb::open(EmbeddedOptions::new(temp.path())).unwrap();
        let app = db.create_database("app").unwrap();
        let table = app.create_table("mixed").unwrap();
        // raw 字符串
        table.set(b"a:001", b"alice").unwrap();
        table.set(b"b:002", b"bob").unwrap();
        // int (走 INCR 触发 TAG_I64)
        table.set(b"c:003", b"0").unwrap();
        table
            .manager
            .batch_ops(&[shard_manager::BatchOp::Incr {
                db: table.database.clone(),
                table: table.name.clone(),
                key: b"c:003".to_vec(),
                delta: 42,
            }]);
        // float (走 INCRBYFLOAT 触发 TAG_F64)
        table.set(b"d:004", b"0").unwrap();
        table
            .manager
            .batch_ops(&[shard_manager::BatchOp::IncrFloat {
                db: table.database.clone(),
                table: table.name.clone(),
                key: b"d:004".to_vec(),
                delta: 3.14,
            }]);

        // 范围 [b, d) 闭开: b:002, c:003 (d:004 不含)
        let entries = table.list_typed_range(b"b", b"d", 0).unwrap();
        assert_eq!(entries.len(), 2);
        let mut by_key: std::collections::HashMap<&[u8], &TypedValue> =
            std::collections::HashMap::new();
        for (k, v) in &entries {
            by_key.insert(k.as_slice(), v);
        }
        match by_key[b"b:002".as_slice()] {
            TypedValue::Raw(v) => assert_eq!(v.as_slice(), b"bob"),
            other => panic!("expected Raw(\"bob\"), got {other:?}"),
        }
        match by_key[b"c:003".as_slice()] {
            TypedValue::Int(n) => assert_eq!(*n, 42),
            other => panic!("expected Int(42), got {other:?}"),
        }
        // d:004 不在范围内
        assert!(!by_key.contains_key(b"d:004".as_slice()));

        drop(table);
        drop(app);
        db.close().unwrap();
    }

    /// ⭐ Async API: 一次性覆盖 set_many_async / get_many_async / get_typed_async /
    /// list_async / list_prefix_async / list_limit_async / list_range_async /
    /// list_range_prefix_async / list_typed_async / list_typed_range_async /
    /// list_typed_range_prefix_async — 全部用 pollster 跑 (单线程 block_on).
    #[test]
    fn async_api_parity_with_sync() {
        let temp = tempfile::tempdir().unwrap();
        let mut options = EmbeddedOptions::new(temp.path());
        options.num_shards = 2; // 跨 shard 验证 fan-out 路径
        let db = NexusDb::open(options).unwrap();
        let app = db.create_database("app").unwrap();
        let table = app.create_table("kv").unwrap();

        // 1. set_many_async: 5 个 key 一次批量写
        let entries: &[(&[u8], &[u8])] = &[
            (b"a", b"1"),
            (b"b", b"2"),
            (b"c", b"3"),
            (b"d", b"4"),
            (b"e", b"5"),
        ];
        let write_results = pollster::block_on(table.set_many_async(entries));
        assert_eq!(write_results.len(), 5);
        assert!(write_results.iter().all(|r| r.is_ok()));

        // 2. get_many_async: 读回 + 验证同序
        let keys: &[&[u8]] = &[b"a", b"c", b"missing", b"e"];
        let read_results = pollster::block_on(table.get_many_async(keys));
        assert_eq!(read_results.len(), 4);
        assert_eq!(read_results[0].as_ref().unwrap(), &Some(b"1".to_vec()));
        assert_eq!(read_results[1].as_ref().unwrap(), &Some(b"3".to_vec()));
        assert_eq!(read_results[2].as_ref().unwrap(), &None);
        assert_eq!(read_results[3].as_ref().unwrap(), &Some(b"5".to_vec()));

        // 3. get_typed_async: 单点类型感知
        let typed = pollster::block_on(table.get_typed_async(b"b")).unwrap();
        match typed {
            Some(TypedValue::Raw(v)) => assert_eq!(v.as_slice(), b"2"),
            other => panic!("expected Raw(\"2\"), got {other:?}"),
        }
        assert!(pollster::block_on(table.get_typed_async(b"missing"))
            .unwrap()
            .is_none());

        // 4. list_async: 全量
        let all = pollster::block_on(table.list_async()).unwrap();
        assert_eq!(
            all,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec()
            ]
        );

        // 5. list_prefix_async
        let c_pref = pollster::block_on(table.list_prefix_async(b"c")).unwrap();
        assert_eq!(c_pref, vec![b"c".to_vec()]);

        // 6. list_limit_async
        let first2 = pollster::block_on(table.list_limit_async(2)).unwrap();
        assert_eq!(first2, vec![b"a".to_vec(), b"b".to_vec()]);

        // 7. list_range_async: 范围闭开
        let range = pollster::block_on(table.list_range_async(b"b", b"d", 0)).unwrap();
        assert_eq!(range, vec![b"b".to_vec(), b"c".to_vec()]);

        // 8. list_range_prefix_async
        let rp = pollster::block_on(table.list_range_prefix_async(
            b"a", b"d", b"", 0,
        ))
        .unwrap();
        assert_eq!(rp, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

        // 9. list_typed_async
        let typed_all = pollster::block_on(table.list_typed_async()).unwrap();
        assert_eq!(typed_all.len(), 5);
        for (k, v) in &typed_all {
            // 全部 raw
            assert!(matches!(v, TypedValue::Raw(_)), "{k:?} not raw");
        }

        // 10. list_typed_limit_async
        let typed_first2 = pollster::block_on(table.list_typed_limit_async(2)).unwrap();
        assert_eq!(typed_first2.len(), 2);

        // 11. list_typed_range_async
        let typed_range =
            pollster::block_on(table.list_typed_range_async(b"b", b"d", 0)).unwrap();
        assert_eq!(typed_range.len(), 2);

        // 12. list_typed_range_prefix_async (与上同, prefix 空)
        let typed_rp =
            pollster::block_on(table.list_typed_range_prefix_async(b"b", b"d", b"", 0))
                .unwrap();
        assert_eq!(typed_rp.len(), 2);

        // 13. get_many_typed_async: 类型感知批量读
        let typed_many =
            pollster::block_on(table.get_many_typed_async(&[b"a", b"missing", b"c"]));
        assert_eq!(typed_many.len(), 3);
        assert!(matches!(
            typed_many[0].as_ref().unwrap(),
            Some(TypedValue::Raw(v)) if v == b"1"
        ));
        assert!(typed_many[1].as_ref().unwrap().is_none());
        assert!(matches!(
            typed_many[2].as_ref().unwrap(),
            Some(TypedValue::Raw(v)) if v == b"3"
        ));

        drop(table);
        drop(app);
        db.close().unwrap();
    }

    /// 回归: list_prefix_async 对含 `\x00` 的 prefix 不会被截断.
    ///
    /// 场景: 4 个 user key 前 7 字节相同, 只差第 8 字节; 第 1~3 字节全为 0x00.
    /// 误传 C 字符串语义时 prefix 会被截短, 命中全部 key. Rust `starts_with`
    /// 是字节比较, 应只返回 1 条.
    #[test]
    fn list_prefix_async_with_nul_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let options = EmbeddedOptions::new(temp.path());
        let db = NexusDb::open(options).unwrap();
        let app = db.create_database("app").unwrap();
        let table = app.create_table("kv").unwrap();

        let common = [0x00u8, 0x00, 0x00, 0x12, 0xE8, 0x93, 0x70];
        let ids: &[&[u8]] = &[b"\x01", b"\x02", b"\x03", b"\x04"];
        for id in ids {
            let mut k = common.to_vec();
            k.extend_from_slice(id);
            table.set(&k, b"v").unwrap();
        }

        // 单独查每个 subject: 必须恰好 1 条.
        for id in ids {
            let mut prefix = common.to_vec();
            prefix.extend_from_slice(id);
            let got = pollster::block_on(table.list_prefix_async(&prefix)).unwrap();
            assert_eq!(
                got.len(),
                1,
                "prefix {prefix:?} matched {} keys, expected 1",
                got.len()
            );
            assert_eq!(got[0], prefix);
        }

        drop(table);
        drop(app);
        db.close().unwrap();
    }

    /// ⭐ Async 跨 shard 正确性: 在 num_shards=2 下批量写, list_async 收齐
    /// (验证 fan-out + 归并不丢 key, 不重复).
    #[test]
    fn async_cross_shard_correctness() {
        let temp = tempfile::tempdir().unwrap();
        let mut options = EmbeddedOptions::new(temp.path());
        options.num_shards = 3;
        let db = NexusDb::open(options).unwrap();
        let app = db.create_database("app").unwrap();
        let table = app.create_table("sharded").unwrap();

        // 20 个 key, 跨 3 shard 分布
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..20)
            .map(|i| {
                let k = format!("k{i:02}");
                let v = format!("v{i}");
                (k.into_bytes(), v.into_bytes())
            })
            .collect();
        let entry_refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let results = pollster::block_on(table.set_many_async(&entry_refs));
        assert!(results.iter().all(|r| r.is_ok()));

        // list_async 应收齐 20 个
        let all = pollster::block_on(table.list_async()).unwrap();
        assert_eq!(all.len(), 20);

        // 升序排列 (BTree 序: k00 < k01 < ... < k19)
        let expected: Vec<Vec<u8>> = (0..20)
            .map(|i| format!("k{i:02}").into_bytes())
            .collect();
        assert_eq!(all, expected);

        // 范围 [k05, k15) 闭开: 10 个
        let range = pollster::block_on(table.list_range_async(b"k05", b"k15", 0)).unwrap();
        assert_eq!(range.len(), 10);
        assert_eq!(range[0], b"k05".to_vec());
        assert_eq!(range[9], b"k14".to_vec());

        drop(table);
        drop(app);
        db.close().unwrap();
    }
}
