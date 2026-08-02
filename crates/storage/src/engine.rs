//! T17 StorageEngine: 全 async 高层 facade.
//!
//! ## 职责
//!
//! 把 `Pager` + `MetaCache` + `recover` 串起来, 提供对用户友好的:
//! - `open(opts) -> StorageEngine`: 走 recover 重建状态适
//! - `put(data) -> vpid`: 分配新 vpid, 写入 page (走 nowchunks)
//! - `get(vpid) -> data`: 三源查找 (nowchunks > chunk_list > disk)
//! - `flush()`: 显式持久化 (写盘 + fsync + meta flush)
//! - `close()`: 隐式 flush, 然后 drop
//!
//! ## T17 (2026-07-21)
//!
//! - 全部 API 改为 `async fn`, 在 Scheduler 协程上下文中运行.
//! - `OpenOptions.io_backend` 控制 StdFs / IoUring 后端.
//! - Pager 走 `PagerIo` 抽象层, 所有 IO 直接 await, 无 daemon 线程.
//!
//! ## 单线程使用
//!
//! 沿用 `Pager` / `MetaCache` 契约, per-shard thread 单线程使用.
//! 所有 `async fn` 必须在 Scheduler 的 `run()` 循环中调用.

use std::io;
use std::path::PathBuf;

use crate::chunk_lru::ChunkList;
use crate::chunk_writer::{ChunkWriter, NowChunks};
use crate::meta_cache::MetaCache;
use crate::meta_page::{META_PID, META_VPID, MetaPage};
use crate::pager::Pager;
use crate::pager_io::PagerIo;
use crate::recover::{recover, recover_for_shard, shard_dir_path};
use crate::registry::{DbHandle, DbRegistry, RegistryError};
use crate::table_directory::TableDirectory;
use crate::types::{DEFAULT_DB_ID, DEFAULT_DB_NAME, DEFAULT_SHARD_ID, DbId, IoBackend, IoBackendConfig, PAGE_SIZE};

// =====================================================================
// OpenOptions
// =====================================================================

/// `StorageEngine::open` 的参数.
#[derive(Clone, Debug)]
pub struct OpenOptions {
    /// ⭐ T12.12 引入: ShardManager 级共享的根目录. 实际 .block 在
    /// `{block_root}/{db_name}/shard_{shard_id}/`. 单 db 单 shard 测试用 `block_root` +
    /// 默认 db ("default") + 默认 shard (0).
    pub block_root: PathBuf,

    /// **Compat 字段** (T12.12 之前): 直接指定 .block 所在目录. 仍可用, 等价于
    /// `block_root = block_dir, db = "default", shard = 0` 但**不**走子目录拼接.
    /// 如果显式设置 (非 None), 优先用此字段 (旧测试 / 旧 API 路径).
    pub block_dir: Option<PathBuf>,

    /// ⭐ T12.12: shard id (per-shard 独立 io_uring). 默认 0 = 单 shard.
    pub shard_id: u32,

    /// ⭐ T12.17: 要打开的 db name. 默认 `"default"`.
    ///
    /// **多 db 模式**: ShardManager 给每个 db 创建一个 engine, 设不同的 db_name
    /// 即可让 engine 绑定到对应 db 的物理路径. 路径格式:
    /// `{block_root}/{db_name}/shard_{shard_id}/`.
    ///
    /// **单 db 模式**: 留 `None` 或设 `"default"`, 行为与之前完全一致.
    /// 不显式传 db_name 时, 用 `DEFAULT_DB_NAME` (= "default").
    ///
    /// **Compat 模式**: 如果设了 `block_dir`, db_name 被忽略 (compat 路径不拼接).
    pub db_name: Option<String>,

    /// 目录不存在时是否自动创建.
    pub create_if_missing: bool,
    /// chunk_list 缓存大小 (chunk 数, 每个 1MB).
    pub chunk_cache_size: usize,
    /// ⭐ T16: IO 后端选择. 默认 `IoBackend::StdFs` (同步 std::fs).
    /// `IoBackend::IoUring` 时自动启动后台 daemon 线程.
    pub io_backend: IoBackend,
    /// ⭐ T18a: 进阶 IO 后端配置 (FD 池 / 注册缓冲区 / SQPOLL / O_DIRECT).
    /// 默认 `IoBackendConfig::default()` (use_fixed_file=true, 其余关闭).
    pub io_config: IoBackendConfig,
    /// ⭐ WAL (F60): 预写日志档位 (Off / Periodic 默认 / Strict).
    /// 段文件在 `{block_root}/shard_{N}.wal.{seq}` (compat 模式在 block_dir).
    pub wal_mode: crate::wal::WalMode,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            block_root: PathBuf::from("./data"),
            block_dir: None, // 默认走 block_root + 默认 db + 默认 shard
            shard_id: 0,
            db_name: None, // 默认 = "default" (DEFAULT_DB_NAME)
            create_if_missing: true,
            chunk_cache_size: 8,
            io_backend: IoBackend::default(),
            io_config: IoBackendConfig::default(),
            wal_mode: crate::wal::WalMode::default(),
        }
    }
}

// =====================================================================
// StorageError
// =====================================================================

/// StorageEngine 错误类型.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("invalid options: {0}")]
    InvalidOptions(String),
}

// =====================================================================
// StorageEngine
// =====================================================================

/// 单 shard 存储引擎 facade.
///
/// 内部持有 `Pager` (进而持有 MetaCache / ChunkList / Allocator / NowChunks / ChunkWriter)
/// + `DbRegistry` (管理多 db / 多表 catalog).
///
/// `Pager` 已经聚合所有底层组件, 所以 StorageEngine 直接代理到 Pager.
///
/// **⭐ T12.16 多 db 上下文**: `current_db` 字段标记"本 engine 当前的 db" (DbId).
/// 单 db 模式默认 0 (= "default"). 切换 db 用 `use_db(name)` 方法.
/// ShardManager 会为每个 db 创建一个 StorageEngine, 显式设置 `current_db`.
///
/// **⭐ T16**: `_io_runner` 持有 `SchedulerRunner` daemon 线程, 在 IoUring 模式下
/// 保持后台 io_uring 事件循环存活. 用 `_` 前缀表明"不直接使用, 仅防 Drop".
pub struct StorageEngine {
    pager: Pager,
    /// T11: 多 db/多表 catalog 缓存 (write-through 镜像 MetaPage + 各 TableDirectory)
    registry: DbRegistry,
    /// ⭐ T12.16 当前 db 的 `DbId`. 默认 `DEFAULT_DB_ID` (= 0, "default" db).
    /// 用 `use_db(name)` 切换; ShardManager 创建多 engine 时显式 set.
    current_db: DbId,
    /// ⭐ PERF (F49): "表内出现过复合类型" 单调提示 (db → table 集合).
    /// 只增不减 — 用于纯 String 表跳过热路径类型探测 (SET purge / GET-miss
    /// WRONGTYPE); false positive 无害 (仅多一次点查), 开库时由
    /// `rebuild_composite_counts` 从 `[#]` meta 行重建.
    composite_tables: std::collections::HashMap<String, std::collections::HashSet<String>>,
    /// ⭐ Q1 (SQL 索引): table schema 常驻镜像 ((db, table) → schema).
    /// write-through: set_schema 先落 `[$]` 行再更新; lazy load (首次 get miss
    /// 时读 `[$]` 行); 无 schema 行 = 纯 KV 表 (缓存 None 免重复探盘).
    schemas: std::collections::HashMap<(String, String), SchemaSlot>,
    /// ⭐ Y1 (布隆剪枝): 每 (db, table, iid) 一个本地索引 bloom.
    /// 等值 IndexScan 快速拒绝; set_schema 建空 bloom, row_put 喂值,
    /// 开库随 rebuild_composite_counts 扫 [I] 前缀重建 (永不假阴性).
    index_blooms: std::collections::HashMap<(String, String, u32), crate::index_bloom::IndexBloom>,
    /// ⭐ Y1: 剪枝命中计数 (等值扫被 bloom 短路的次数; 测试/观测用).
    pub bloom_skip_count: u64,
    /// ⭐ WAL (F60): per-shard 预写日志 (None = Off 档 / 重放期间).
    wal: Option<crate::wal::WalWriter>,
    /// ⭐ M3-1 (CBO 统计): 每 (db, table) 近似行数 (内存增量维护).
    /// put 新 key +1 (覆盖不加, 由 registry::table_put 返回 existed), delete -1;
    /// put_many 近似 +N (覆盖会高估). 重启后从 0 重算 (持久化 M3-1b 待做).
    row_counts: std::collections::HashMap<(String, String), u64>,
}

/// ⭐ Q1: schema 镜像槽别名 (None = 已确认无 schema 的纯 KV 表).
pub(crate) type SchemaSlot = Option<std::sync::Arc<crate::schema::TableSchema>>;

impl StorageEngine {
    /// ⭐ Q1: schema 镜像读 (外层 None = 未加载过, 内层 None = 确认无 schema).
    pub(crate) fn schema_cache_get(&self, db: &str, table: &str) -> Option<&SchemaSlot> {
        self.schemas.get(&(db.to_string(), table.to_string()))
    }

    /// ⭐ Q1: schema 镜像写 (write-through 的内存侧; caller 先保证 `[$]` 行已落).
    pub(crate) fn schema_cache_put(&mut self, db: &str, table: &str, slot: SchemaSlot) {
        self.schemas.insert((db.to_string(), table.to_string()), slot);
    }
    
    /// ⭐ Y1: 确保 (db, table, iid) 的 bloom 存在并返回可变引用.
    pub(crate) fn bloom_entry(
        &mut self,
        db: &str,
        table: &str,
        iid: u32,
    ) -> &mut crate::index_bloom::IndexBloom {
        self.index_blooms
            .entry((db.to_string(), table.to_string(), iid))
            .or_default()
    }
    
    /// ⭐ Y1: 等值剪枝判定 — Some(false) = bloom 断言不存在 (可短路);
    /// 无条目 → true (不剪枝, 正常扫).
    pub(crate) fn bloom_may_contain(&self, db: &str, table: &str, iid: u32, val: &[u8]) -> bool {
        self.index_blooms
            .get(&(db.to_string(), table.to_string(), iid))
            .is_none_or(|b| b.may_contain(val))
    }
}

impl StorageEngine {
    /// 打开或创建存储引擎.
    ///
    /// 流程 (DESIGN §4.5 + §3.0.3):
    /// 1. 若 `create_if_missing` = true, 创建 `block_dir` + `page.mate` + 第一个 `.block` 文件
    /// 2. 调 `recover(block_dir)` 扫描已有 .block 重建 MetaCache + Allocator 状态
    /// 3. 创建 `ChunkWriter` (持有 .block fd) + `ChunkList` (LRU 缓存) + `NowChunks` (写缓冲)
    /// 4. 拼装 `Pager`
    pub async fn open(opts: OpenOptions) -> Result<Self, StorageError> {
        if opts.chunk_cache_size == 0 {
            return Err(StorageError::InvalidOptions(
                "chunk_cache_size must be > 0".to_string(),
            ));
        }

        // ⭐ T12.12 路径解析: 优先 block_dir (compat), 否则 block_root + db_name + shard_id
        // ⭐ T12.17: db_name 从 opts.db_name 取 (None → DEFAULT_DB_NAME)
        let compat_block_dir: Option<PathBuf> = opts.block_dir.clone();
        let opts_db_name: String = opts
            .db_name
            .clone()
            .unwrap_or_else(|| DEFAULT_DB_NAME.to_string());
        let (block_root, db_name, shard_id, block_dir_for_io) = if let Some(bd) = compat_block_dir {
            // Compat 模式: block_dir 直接是 .block 所在, 走旧 Pager 逻辑
            // opts.db_name 在 compat 模式下被忽略 (路径不拼接)
            (
                bd.clone(),
                DEFAULT_DB_NAME.to_string(),
                DEFAULT_SHARD_ID,
                bd,
            )
        } else {
            // ⭐ T12.17 修复: tuple 第二项必须用 opts_db_name, 而不是 DEFAULT_DB_NAME,
            // 否则 recover_for_shard 扫描错误目录 (路径已经用了 db_name, 但 recover 不知情)
            let shard_dir = shard_dir_path(&opts.block_root, &opts_db_name, opts.shard_id);
            (
                opts.block_root.clone(),
                opts_db_name,
                opts.shard_id,
                shard_dir,
            )
        };

        // 1. 创建目录和占位文件
        if opts.create_if_missing {
            std::fs::create_dir_all(&block_dir_for_io)?;
            let mate = block_dir_for_io.join("page.mate");
            if !mate.exists() {
                let f = std::fs::File::create(&mate)?;
                // MetaCache 需要 10MB mate; 预分配避免后续 read 短
                f.set_len(10 * 1024 * 1024)?;
            }
            // 创建第一个 .block 文件 (file_id 0 → "000001.block")
            let first_block = block_dir_for_io.join("000001.block");
            if !first_block.exists() {
                let f = std::fs::File::create(&first_block)?;
                f.set_len(10 * 1024 * 1024)?;
            }
        }

        // 2. recover (compat / shard 模式自动选择)
        let mut recovered = if opts.block_dir.is_some() {
            recover(&block_dir_for_io)?
        } else {
            recover_for_shard(&block_root, &db_name, shard_id)?
        };

        // 2.5 ⭐ 初始化 MetaPage (T9 集成):
        //     - 如果 vpid 0 未在 MetaCache 中 (全新库), 写空 MetaPage 到 chunk 0 page 0,
        //       并在 MetaCache 中记录 vpid 0 → META_PID
        //     - 调整 vpid_alloc / pid_alloc, 使 vpid 0 / page 0 永远保留给 MetaPage
        if recovered.meta.read(META_VPID).is_none() {
            init_meta_page(&block_dir_for_io, &mut recovered.meta)?;
            // pid_alloc 推进到 page_idx=1 (跳过 MetaPage 占用的 page 0)
            recovered.pid_alloc = crate::alloc::PidAllocator::new(0, 0, 1);
        } else {
            // 已有 MetaPage 映射, 但可能 pid_alloc 没推到 page 1.
            // 安全起见: 至少推到 page 1 (MetaPage 占用 page 0).
            let (_, _, next_page) = recovered.pid_alloc.current();
            if next_page < 1 {
                recovered.pid_alloc = crate::alloc::PidAllocator::new(0, 0, 1);
            }
        }
        // 不论 meta 中是否已有 vpid 0, vpid_alloc 都至少从 1 开始
        if recovered.vpid_alloc.current() < 1 {
            // vpid_alloc 内部 next_vpid 直接跳到 1 (覆盖 vpid 0)
            // 用 VpidAllocator::new(initial) 重建
            recovered.vpid_alloc = crate::alloc::VpidAllocator::new(1);
        }

        // 3. 创建 ChunkWriter (持有 .block fd)
        // 注意: ChunkWriter::new 会重新 open .block, 我们的预创建 set_len 会被保留
        // Pager 实际写 .block 时, 走 key.file_id + 1 命名
        // recover 推断 last_file_id 后, 新写入从 next_file_id 开始
        // 简化: 我们总是用 next_file_id 推断的 block_path
        let active_file_id = recovered.pid_alloc.current().0;
        let block_path = block_dir_for_io.join(format!("{:06}.block", active_file_id + 1));
        // 如果推断的 block 不存在 (e.g., new db), 用第一个
        let block_path = if block_path.exists() {
            block_path
        } else {
            block_dir_for_io.join("000001.block")
        };
        let writer = ChunkWriter::new(&block_path)?;

        // T17+T18a: 创建 IO 后端 (PagerIo), 携带 io_config
        let pager_io = PagerIo::new(opts.io_config);

        // 4. 拼装 Pager
        let pager = if opts.block_dir.is_some() {
            Pager::with_io(
                block_dir_for_io,
                recovered.meta,
                recovered.vpid_alloc,
                recovered.pid_alloc,
                ChunkList::new(opts.chunk_cache_size),
                NowChunks::new(),
                writer,
                pager_io,
            )
        } else {
            Pager::new_for_shard_with_io(
                block_root.clone(),
                db_name,
                shard_id,
                recovered.meta,
                recovered.vpid_alloc,
                recovered.pid_alloc,
                ChunkList::new(opts.chunk_cache_size),
                NowChunks::new(),
                writer,
                pager_io,
            )
        };

        // 5. ⭐ 加载 DbRegistry
        let mut pager_for_registry = pager;
        // ⭐ G1: 从全量平坦 meta 反推重建 chunk/block 活性统计 (GC 基础)
        pager_for_registry.rebuild_liveness();
        let registry = DbRegistry::load(&mut pager_for_registry)
            .await
            .map_err(|e| {
                StorageError::Io(std::io::Error::other(format!("DbRegistry::load: {}", e)))
            })?;

        let mut engine = Self {
            pager: pager_for_registry,
            registry,
            current_db: DEFAULT_DB_ID,
            composite_tables: std::collections::HashMap::new(),
            schemas: std::collections::HashMap::new(),
            index_blooms: std::collections::HashMap::new(),
            bloom_skip_count: 0,
            wal: None, // 重放期间保持 None (append 自动跳过)
            row_counts: std::collections::HashMap::new(),
        };
        // ⭐ U3: 从 data 行重建复合结构计数 (修复 crash 中 meta count 漂移).
        engine.rebuild_composite_counts().await.map_err(|e| {
            StorageError::Io(std::io::Error::other(format!("rebuild_composite_counts: {e}")))
        })?;
        // ⭐ WAL (F60): 重放现存段 (填补上次 crash 的刷盘窗口), 完成后删段并
        // 启用新 WalWriter. 段目录: compat 模式用 block_dir, 否则 block_root 根.
        if opts.wal_mode != crate::wal::WalMode::Off {
            let wal_dir =
                if let Some(bd) = &opts.block_dir { bd.clone() } else { block_root.clone() };
            let segs = crate::wal::WalWriter::existing_segments(&wal_dir, shard_id);
            if !segs.is_empty() {
                engine.replay_wal_segments(&segs).await;
                // 重放产物立即全量落盘 (数据+meta), 之后段可安全删除
                engine.pager.flush().await?;
                crate::wal::WalWriter::purge_replayed(&segs);
            }
            engine.wal = Some(
                crate::wal::WalWriter::open(
                    &wal_dir,
                    shard_id,
                    opts.wal_mode,
                    matches!(opts.io_backend, IoBackend::IoUring),
                )
                .map_err(StorageError::Io)?,
            );
        }
        Ok(engine)
    }

    /// ⭐ WAL (F60): 按段序逐条重放 (幂等: 结果态 put / delete).
    /// db/table 已不存在 (drop 后残留记录) → warn 跳过不 panic.
    async fn replay_wal_segments(&mut self, segs: &[std::path::PathBuf]) {
        let mut applied = 0u64;
        let mut skipped = 0u64;
        for seg in segs {
            let Ok(data) = std::fs::read(seg) else { continue };
            for rec in crate::wal::decode_records(&data) {
                let r = match &rec.value {
                    Some(v) => {
                        // 惰性建表窗口: 表未持久化时重建 (db 必须存在 —
                        // create_db 路径已强制落盘; 不存在 = 已 drop, 跳过)
                        if self.ensure_table(&rec.db, &rec.table).await.is_err() {
                            skipped += 1;
                            continue;
                        }
                        self.put_physical(&rec.db, &rec.table, &rec.pkey, v).await.map(|_| ())
                    }
                    None => self
                        .delete_physical(&rec.db, &rec.table, &rec.pkey)
                        .await
                        .map(|_| ()),
                };
                match r {
                    Ok(()) => applied += 1,
                    Err(_) => skipped += 1, // db/table 已删等陈旧记录, 跳过
                }
            }
        }
        if applied + skipped > 0 {
            eprintln!("[storage] WAL replay: {applied} applied, {skipped} skipped (stale db/table)");
        }
    }

    /// 写一个 page. 分配新 vpid, 走 nowchunks.
    pub async fn put(&mut self, data: [u8; PAGE_SIZE]) -> io::Result<u64> {
        self.pager.create(Box::new(data)).await
    }

    /// 读 page 数据. 三源查找.
    pub async fn get(&mut self, vpid: u64) -> io::Result<Box<[u8; PAGE_SIZE]>> {
        self.pager.read(vpid).await
    }

    /// 显式 flush: nowchunks → .block (pwrite + fsync) → chunk_list → meta flush.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.pager.flush().await
    }

    /// 隐式 flush 后 drop. **不**保证 fsync (调用方应先调 flush).
    pub async fn close(mut self) -> io::Result<()> {
        // ⭐ 退出完整性 (2026-07-26): 顺序必须是 **先 flush 后 drive**.
        // flush 写 nowchunks 最新视图并清除同 key 的 stale pending (防回滚);
        // drive_write_queue 再写剩余 pending (nowchunks 中无新版本的 chunk).
        // 两步内部都保证 "chunk data 全部写完才刷 meta".
        self.pager.flush().await?;
        self.pager.drive_write_queue().await?;
        // ⭐ WAL (F60): 正常关闭 = 全量已落盘, 全部段可删 (重启免重放)
        if let Some(mut w) = self.wal.take() {
            w.purge_all();
        }
        Ok(())
    }

    /// 暴露 meta cache (高级用法: 调试, 直接读写映射).
    pub fn meta(&mut self) -> &mut MetaCache {
        self.pager.meta()
    }

    // =================================================================
    // ⭐ WAL (F60): shard 主循环接线面
    // =================================================================

    /// 当前 WAL 档位 (Off = 未启用).
    pub fn wal_mode(&self) -> crate::wal::WalMode {
        self.wal.as_ref().map_or(crate::wal::WalMode::Off, |w| w.mode())
    }

    /// 有未持久化的 WAL 内容 (strict 档回复前判断是否需 barrier).
    pub fn wal_needs_sync(&self) -> bool {
        self.wal.as_ref().is_some_and(|w| w.needs_sync())
    }

    /// WAL 持久化屏障: buf 落盘 + fsync (strict 档回复前 / 显式 flush 前).
    pub async fn wal_barrier(&mut self) -> io::Result<()> {
        match self.wal.as_mut() {
            Some(w) => w.flush_and_sync().await,
            None => Ok(()),
        }
    }

    /// Periodic 档心跳: 距上次 sync ≥ 1s 且有内容 → 落盘+fsync.
    pub async fn wal_periodic_tick(&mut self) -> io::Result<()> {
        const WAL_SYNC_PERIOD: std::time::Duration = std::time::Duration::from_secs(1);
        match self.wal.as_mut() {
            Some(w)
                if w.mode() == crate::wal::WalMode::Periodic
                    && w.periodic_due(WAL_SYNC_PERIOD) =>
            {
                w.flush_and_sync().await
            }
            _ => Ok(()),
        }
    }

    /// 刷盘快照触发时调用 (同轮内无并发写): seal 当前段.
    pub fn wal_seal(&mut self) {
        if let Some(w) = self.wal.as_mut()
            && let Err(e) = w.seal()
        {
            eprintln!("[storage] WAL seal failed (segment kept): {e}");
        }
    }

    /// meta 全部持久化后调用: 删除 sealed 段 (其记录已由 chunk+meta 覆盖).
    pub fn wal_drop_sealed_if_meta_flushed(&mut self) {
        if self.pager.meta_all_flushed()
            && let Some(w) = self.wal.as_mut()
        {
            w.drop_sealed();
        }
    }

    /// chunk_list 缓存大小 (测试 helper).
    pub fn chunk_cache_len(&mut self) -> usize {
        self.pager.chunk_cache_len()
    }

    /// 暴露内部 Pager (T10/T11 catalog 集成: 用于构造 TableDirectory / DbRegistry).
    ///
    /// **生命周期**: 返回的 `&mut Pager` 与 engine 内部 pager 同一生命周期.
    /// 调用方不能在持此借用期间调 `engine.put/get/flush/close` (会双重 &mut).
    pub fn pager_mut(&mut self) -> &mut Pager {
        &mut self.pager
    }

    /// **辅助**: 同时拿 `&mut Pager` 和 `&mut DbRegistry`, 解决 caller 既要操作
    /// pager 又要拿 DbHandle 的 split borrow 问题.
    ///
    /// **为什么需要**: `&mut self` 借整个 engine, `pager_mut()` 和 `registry_mut()`
    /// 不能同时存在. 此方法用 destructure 借分开字段, 绕过 borrow checker.
    pub fn split_pager_and_registry(&mut self) -> (&mut Pager, &mut DbRegistry) {
        (&mut self.pager, &mut self.registry)
    }

    /// **辅助**: 同时拿 `&mut Pager` 和 `&mut DbHandle<db>`, 解决 split borrow.
    /// db 不存在返回 `RegistryError::DbNotFound`.
    pub fn pager_and_db(
        &mut self,
        db_name: &str,
    ) -> Result<(&mut Pager, &mut DbHandle), RegistryError> {
        let (pager, registry) = self.split_pager_and_registry();
        let db_handle = registry.open_db(db_name)?;
        Ok((pager, db_handle))
    }

    /// 用 engine 内部的 Pager 构造一个新的 TableDirectory BTree.
    ///
    /// **典型用法 (T11 DbRegistry)**:
    /// ```ignore
    /// let mut e = StorageEngine::open(opts).await?;
    /// let td = e.create_table_directory().await?;
    /// let users_vpid = td.create_table(&mut e.pager_mut(), "users").await?;
    /// e.flush().await?;
    /// ```
    ///
    /// **注意**: 分配一个新 vpid 作为 TableDirectory BTree 的 root. 调用方负责
    /// 把这个 vpid 写进 MetaPage (DbRegistry 在 T11 处理).
    pub async fn create_table_directory(
        &mut self,
    ) -> Result<TableDirectory, crate::table_directory::TableDirError> {
        TableDirectory::create_new(&mut self.pager).await
    }

    /// 用已知的 root_vpid 打开 TableDirectory BTree.
    ///
    /// **典型用法 (T11 DbRegistry recover)**: open 时从 MetaPage 读 table_dir_root_vpid,
    /// 然后 `open_table_directory` 拿 TableDirectory handle.
    pub async fn open_table_directory(
        &mut self,
        root_vpid: u64,
    ) -> Result<TableDirectory, crate::table_directory::TableDirError> {
        TableDirectory::open(root_vpid, &mut self.pager).await
    }

    // =================================================================
    // T11 DbRegistry: 多 db / 多表管理 API
    // =================================================================

    /// 创建一个新 db. 自动创建对应的 TableDirectory BTree, 写入 MetaPage.
    pub async fn create_db(&mut self, name: &str) -> Result<(), RegistryError> {
        self.registry.create_db(&mut self.pager, name).await
    }

    /// 删除一个 db. **不**清理 db 内 table 的 page (孤儿 vpid, LRU 自然驱逐).
    pub async fn drop_db(&mut self, name: &str) -> Result<(), RegistryError> {
        self.registry.drop_db(&mut self.pager, name).await
    }

    /// 拿 db 句柄. 不存在返回 `DbNotFound`.
    pub fn open_db(&mut self, name: &str) -> Result<&mut DbHandle, RegistryError> {
        self.registry.open_db(name)
    }

    /// 列出所有 db (按 name 升序).
    pub fn list_dbs(&self) -> Vec<String> {
        self.registry.list_dbs()
    }

    /// ⭐ D1 (分库): 列出所有 db 的 (id, name) — resolver 持久化事实源.
    pub fn list_dbs_with_ids(&self) -> Vec<(u32, String)> {
        self.registry.list_dbs_with_ids()
    }

    /// db 总数.
    pub fn db_count(&self) -> usize {
        self.registry.db_count()
    }

    /// 在指定 db 中创建一张新表. 返回新表的 root_vpid.
    pub async fn create_table(&mut self, db: &str, table: &str) -> Result<u64, RegistryError> {
        let db_handle = self.registry.open_db(db)?;
        db_handle.create_table(&mut self.pager, table).await
    }

    /// 删除指定 db 中的一张表.
    pub async fn drop_table(&mut self, db: &str, table: &str) -> Result<bool, RegistryError> {
        let db_handle = self.registry.open_db(db)?;
        db_handle.drop_table(&mut self.pager, table).await
    }

    /// 查表: 返回 Some(vpid) 表示存在, None 表示不存在.
    pub async fn open_table(
        &mut self,
        db: &str,
        table: &str,
    ) -> Result<Option<u64>, RegistryError> {
        let db_handle = self.registry.open_db(db)?;
        db_handle.open_table(&mut self.pager, table).await
    }

    /// ⭐ PERF (F49): 表内是否出现过复合类型 (单调提示; 见字段注释).
    pub(crate) fn has_composite(&self, db: &str, table: &str) -> bool {
        self.composite_tables
            .get(db)
            .is_some_and(|s| s.contains(table))
    }

    /// ⭐ PERF (F49): 标记表出现过复合类型 (复合 meta 写入口 + 开库重建时调).
    pub(crate) fn mark_composite(&mut self, db: &str, table: &str) {
        let set = self.composite_tables.entry(db.to_string()).or_default();
        if !set.contains(table) {
            set.insert(table.to_string());
        }
    }

    /// ⭐ S1 (DROP TABLE): 清除表的全部 engine 侧派生状态 —
    /// schema 镜像 + 全部 index bloom + 复合提示位.
    /// (物理数据由 drop_table 负责; 此处只清内存缓存, 防已删表幽灵命中)
    pub(crate) fn purge_table_state(&mut self, db: &str, table: &str) {
        self.schemas.remove(&(db.to_string(), table.to_string()));
        self.index_blooms
            .retain(|(d, t, _), _| !(d == db && t == table));
        if let Some(set) = self.composite_tables.get_mut(db) {
            set.remove(table);
        }
    }

    /// ⭐ T1 (分表): 惰性建表 — 表已存在 (registry 缓存命中) 零 IO 返回;
    /// 不存在则本 shard 本地创建 (幂等; shard 间物理隔离, 无需 2PC 协调).
    /// RESP 冒号前缀路由的自动建表入口, 由 shard 数据面在 op 执行前调用.
    pub async fn ensure_table(&mut self, db: &str, table: &str) -> Result<(), RegistryError> {
        if self.open_table(db, table).await?.is_some() {
            return Ok(());
        }
        match self.create_table(db, table).await {
            Ok(_) => Ok(()),
            // shard 单线程无并发窗口, 但保持幂等语义 (重复建表视为成功)
            Err(RegistryError::TableOp(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// 列出指定 db 中所有表名 (按 name 升序).
    ///
    /// **注意**: 需要 `&mut self` 因为内部要查 DbHandle (拿 vpid 缓存).
    pub fn list_tables(&mut self, db: &str) -> Result<Vec<String>, RegistryError> {
        let db_handle = self.registry.open_db(db)?;
        Ok(db_handle.list_tables())
    }

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
        self.put_physical(db, table, &ek, value).await
    }

    // =================================================================
    // ⭐ Phase H: 物理 key 层辅助 (复合结构 op 用; String 入口是其薄封装).
    // pkey = 已编码的 BTree 物理 key (keyspace::encode_*).
    // =================================================================

    /// 按物理 key 写入 (含 root split 的 TableDirectory 回写, 与 table_put 同逻辑).
    pub(crate) async fn put_physical(
        &mut self,
        db: &str,
        table: &str,
        pkey: &[u8],
        value: &[u8],
    ) -> Result<(), RegistryError> {
        let db_handle = self.registry.open_db(db)?;
        let table_vpid = db_handle
            .open_table(&mut self.pager, table)
            .await?
            .ok_or_else(|| RegistryError::TableNotFound(db.to_string(), table.to_string()))?;

        let (new_root, existed) =
            crate::registry::table_put(&mut self.pager, table_vpid, pkey, value).await?;

        // ⭐ M3-1: 新 key → 近似行数 +1 (覆盖不加)
        if !existed {
            *self
                .row_counts
                .entry((db.to_string(), table.to_string()))
                .or_insert(0) += 1;
        }

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
        Ok(())
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

// =====================================================================
// Drop: 不隐式 flush, 但确保资源释放不 panic
// =====================================================================

impl Drop for StorageEngine {
    fn drop(&mut self) {
        // 不隐式 flush (因 flush 可能 IO 阻塞, Drop 不应阻塞).
        // 注意: 调用方应负责 close() 或显式 flush().
    }
}

// =====================================================================
// MetaPage 初始化: 写空 MetaPage 到 chunk 0 page 0 (T9 集成)
// =====================================================================

/// 写一个空的 MetaPage 到 block_dir/000001.block 的 chunk 0 page 0 (offset 0),
/// 并在 MetaCache 中登记 vpid 0 → META_PID.
///
/// **调用场景**: `StorageEngine::open` 时, 如果 recover 发现 vpid 0 未映射 (全新库),
/// 则认为 MetaPage 还没初始化, 主动写一个空 MetaPage 落盘 + 注册映射.
///
/// **不更新 vpid_alloc**: 调用方负责设置 `vpid_alloc` 起点 (本函数不感知 alloc).
fn init_meta_page(block_dir: &std::path::Path, meta: &mut MetaCache) -> io::Result<()> {
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

// =====================================================================
// 单元测试 (集成测试在 tests/engine_e2e.rs)
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// **T17 测试 helper**: 跑 async 测试在一个新的 scheduler 上.
    ///
    /// 模式 (跟 pager_io.rs 单元测试一致):
    /// 1. 新建 SchedHandle
    /// 2. set_current 让 storage engine 的 with_current 能找到 scheduler
    /// 3. spawn_on 提交 future
    /// 4. drive_until_idle 驱动 scheduler 直到没有 ready 任务
    /// 5. pollster::block_on 等 JoinHandle 完成
    fn run_async<F>(f: F) -> F::Output
    where
        F: std::future::Future<Output = ()> + 'static,
    {
        let rt = scheduler::SchedHandle::new(scheduler::Scheduler::new());
        rt.set_current();
        let h = scheduler::spawn_on(&rt, f);
        let _ = rt.drive_until_idle(10_000);
        pollster::block_on(h).unwrap()
    }

    #[test]
    fn open_options_default_values() {
        let opts = OpenOptions::default();
        assert_eq!(opts.block_root, PathBuf::from("./data"));
        assert!(opts.block_dir.is_none());
        assert_eq!(opts.shard_id, 0);
        assert!(opts.create_if_missing);
        assert_eq!(opts.chunk_cache_size, 8);
    }

    #[test]
    fn open_options_zero_cache_size_errors() {
        let opts = OpenOptions {
            block_root: PathBuf::from("/tmp/anywhere"),
            block_dir: None,
            db_name: None,
            shard_id: 0,
            create_if_missing: false,
            chunk_cache_size: 0,
            io_backend: IoBackend::StdFs,
            io_config: IoBackendConfig::default(),
            wal_mode: crate::wal::WalMode::Off,
        };
        let result = StorageEngine::open(opts);
        // 用 pollster 等结果
        let r = pollster::block_on(result);
        assert!(
            r.is_err(),
            "chunk_cache_size = 0 应返回 Err(InvalidOptions)"
        );
        match r {
            Err(StorageError::InvalidOptions(_)) => {}
            _ => panic!("expected InvalidOptions"),
        }
    }

    #[test]
    fn open_options_clone_preserves_fields() {
        let opts = OpenOptions {
            block_root: PathBuf::from("/tmp/parent"),
            block_dir: Some(PathBuf::from("/tmp/foo")),
            db_name: None,
            shard_id: 2,
            create_if_missing: false,
            chunk_cache_size: 16,
            io_backend: IoBackend::StdFs,
            io_config: IoBackendConfig::default(),
            wal_mode: crate::wal::WalMode::Off,
        };
        let cloned = opts.clone();
        assert_eq!(cloned.block_root, opts.block_root);
        assert_eq!(cloned.block_dir, opts.block_dir);
        assert_eq!(cloned.shard_id, opts.shard_id);
        assert_eq!(cloned.create_if_missing, opts.create_if_missing);
        assert_eq!(cloned.chunk_cache_size, opts.chunk_cache_size);
    }

    // =================================================================
    // T12.12 集成测试: block_root + shard_id 路径实际生效
    // =================================================================

    #[test]
    fn open_with_block_root_creates_shard_dir_layout() {
        run_async(async {
            // 全新 root, 默认 db "default", shard 0
            let tmp = tempfile::tempdir().unwrap();
            let block_root = tmp.path().to_path_buf();

            let opts = OpenOptions {
                block_root: block_root.clone(),
                block_dir: None,
                db_name: None, // 走 block_root 路径
                shard_id: 0,
                create_if_missing: true,
                chunk_cache_size: 4,
                io_backend: IoBackend::StdFs,
                io_config: IoBackendConfig::default(),
                wal_mode: crate::wal::WalMode::Off,
            };
            let mut engine = StorageEngine::open(opts)
                .await
                .expect("open with block_root ok");

            // 验证目录 layout: {block_root}/default/shard_0/ 下应有 page.mate + 000001.block
            let shard_dir = block_root.join("default").join("shard_0");
            assert!(shard_dir.exists(), "应创建 shard_dir = {shard_dir:?}");
            assert!(
                shard_dir.join("page.mate").exists(),
                "page.mate 应在 shard_dir"
            );
            assert!(
                shard_dir.join("000001.block").exists(),
                "000001.block 应在 shard_dir"
            );

            // 验证 engine 可用. caller 写带 valid header 的 page: Pager 会
            // 覆盖 [0x18..0x20] 写 vpid, 但 [0x28..PAGE_SIZE] caller 字节保留.
            let mut data = [0u8; PAGE_SIZE];
            // 写一个 valid leaf page header (跟 engine_e2e.rs make_data 一致)
            data[0..4].copy_from_slice(&[0x4C, 0x43, 0x42, 0x50]); // "LCBP"
            data[4] = 3; // Leaf
            data[0x06..0x08].copy_from_slice(&0u16.to_le_bytes()); // key_count = 0
            data[0x08..0x0A].copy_from_slice(&(PAGE_SIZE as u16).to_le_bytes()); // free_off
            data[0x14..0x18].copy_from_slice(&1u32.to_le_bytes()); // version = 1
            data[0x28] = 0xAB; // 业务 marker

            let vpid = engine.put(data).await.expect("put ok");
            let read = engine.get(vpid).await.expect("get ok");
            // 业务 marker 应原样保留
            assert_eq!(read[0x28], 0xAB, "caller 字节 [0x28] 应保留");
            // magic 应保留
            assert_eq!(&read[0..4], &[0x4C, 0x43, 0x42, 0x50], "magic 应保留");
            // vpid 字段应被 Pager 写入 (vpid = 1, MetaPage 占 vpid 0)
            let read_vpid = u64::from_le_bytes(read[0x18..0x20].try_into().unwrap());
            assert_eq!(read_vpid, 1, "Pager 应在 [0x18..0x20] 写 vpid");
        });
    }

    #[test]
    fn open_with_block_dir_compat_still_works() {
        run_async(async {
            // Compat 模式: 显式设 block_dir, 路径就是 block_dir 直接
            let tmp = tempfile::tempdir().unwrap();

            let opts = OpenOptions {
                block_root: tmp.path().to_path_buf(), // 这个值在 compat 模式不重要
                block_dir: Some(tmp.path().to_path_buf()),
                db_name: None,
                shard_id: 0,
                create_if_missing: true,
                chunk_cache_size: 4,
                io_backend: IoBackend::StdFs,
                io_config: IoBackendConfig::default(),
                wal_mode: crate::wal::WalMode::Off,
            };
            let mut engine = StorageEngine::open(opts).await.expect("open compat ok");

            // Compat 模式: 旧 layout, page.mate 直接在 block_dir
            assert!(tmp.path().join("page.mate").exists());
            assert!(tmp.path().join("000001.block").exists());

            // 不应在 compat 模式下创建子目录
            let shard_subdir = tmp.path().join("default").join("shard_0");
            assert!(
                !shard_subdir.exists(),
                "compat 模式不应创建 default/shard_0 子目录"
            );

            let _ = engine
                .put([0u8; PAGE_SIZE])
                .await
                .expect("put ok in compat");
        });
    }

    #[test]
    fn open_different_shard_ids_use_separate_dirs() {
        run_async(async {
            // shard 0 和 shard 1 各自独立目录
            let tmp = tempfile::tempdir().unwrap();
            let block_root = tmp.path().to_path_buf();

            let opts0 = OpenOptions {
                block_root: block_root.clone(),
                block_dir: None,
                db_name: None,
                shard_id: 0,
                create_if_missing: true,
                chunk_cache_size: 4,
                io_backend: IoBackend::StdFs,
                io_config: IoBackendConfig::default(),
                wal_mode: crate::wal::WalMode::Off,
            };
            let _e0 = StorageEngine::open(opts0).await.expect("shard 0 ok");

            let opts1 = OpenOptions {
                block_root: block_root.clone(),
                block_dir: None,
                db_name: None,
                shard_id: 1,
                create_if_missing: true,
                chunk_cache_size: 4,
                io_backend: IoBackend::StdFs,
                io_config: IoBackendConfig::default(),
                wal_mode: crate::wal::WalMode::Off,
            };
            let _e1 = StorageEngine::open(opts1).await.expect("shard 1 ok");

            // 两个 shard 目录都应存在
            let s0 = block_root.join("default").join("shard_0");
            let s1 = block_root.join("default").join("shard_1");
            assert!(s0.exists(), "shard_0 dir 应存在");
            assert!(s1.exists(), "shard_1 dir 应存在");
            assert!(s0.join("page.mate").exists());
            assert!(s1.join("page.mate").exists());
        });
    }

    // =================================================================
    // ⭐ T12.16 测试: current_db / use_db / set_current_db / current_db_name
    // =================================================================

    /// 验证刚 open 的 engine current_db = DEFAULT_DB_ID (= 0), name = "default".
    #[test]
    fn current_db_defaults_to_default() {
        run_async(async {
            let tmp = tempfile::tempdir().unwrap();
            let opts = OpenOptions {
                block_root: tmp.path().to_path_buf(),
                block_dir: None,
                db_name: None,
                shard_id: 0,
                create_if_missing: true,
                chunk_cache_size: 4,
                io_backend: IoBackend::StdFs,
                io_config: IoBackendConfig::default(),
                wal_mode: crate::wal::WalMode::Off,
            };
            let engine = StorageEngine::open(opts).await.expect("open ok");
            assert_eq!(engine.current_db(), DEFAULT_DB_ID);
            assert_eq!(
                engine.current_db_name().expect("current_db_name ok"),
                DEFAULT_DB_NAME
            );
        });
    }

    /// use_db 切到已存在的 db, current_db 和 current_db_name 同步更新.
    #[test]
    fn use_db_switches_current_db() {
        run_async(async {
            let tmp = tempfile::tempdir().unwrap();
            let opts = OpenOptions {
                block_root: tmp.path().to_path_buf(),
                block_dir: None,
                db_name: None,
                shard_id: 0,
                create_if_missing: true,
                chunk_cache_size: 4,
                io_backend: IoBackend::StdFs,
                io_config: IoBackendConfig::default(),
                wal_mode: crate::wal::WalMode::Off,
            };
            let mut engine = StorageEngine::open(opts).await.expect("open ok");

            // create_db 切到 "app"
            engine.create_db("app").await.expect("create app");
            let app_id = engine.use_db("app").expect("use_db app");
            assert_eq!(engine.current_db(), app_id);
            assert_eq!(engine.current_db_name().expect("name ok"), "app");
            assert_ne!(app_id, DEFAULT_DB_ID, "app_id != default 0");

            // 再切回 "default"
            let def_id = engine.use_db("default").expect("use_db default");
            assert_eq!(def_id, DEFAULT_DB_ID);
            assert_eq!(engine.current_db_name().expect("name ok"), "default");
        });
    }

    /// use_db 切到不存在的 db 返回 DbNotFound, current_db 不变.
    #[test]
    fn use_db_nonexistent_errors_and_preserves_current() {
        run_async(async {
            let tmp = tempfile::tempdir().unwrap();
            let opts = OpenOptions {
                block_root: tmp.path().to_path_buf(),
                block_dir: None,
                db_name: None,
                shard_id: 0,
                create_if_missing: true,
                chunk_cache_size: 4,
                io_backend: IoBackend::StdFs,
                io_config: IoBackendConfig::default(),
                wal_mode: crate::wal::WalMode::Off,
            };
            let mut engine = StorageEngine::open(opts).await.expect("open ok");
            let before = engine.current_db();
            let result = engine.use_db("does_not_exist");
            assert!(matches!(result, Err(RegistryError::DbNotFound(_))));
            assert_eq!(engine.current_db(), before, "失败后 current_db 不变");
        });
    }

    /// set_current_db 显式设 id, 不走 name 解析.
    #[test]
    fn set_current_db_explicit_id() {
        run_async(async {
            let tmp = tempfile::tempdir().unwrap();
            let opts = OpenOptions {
                block_root: tmp.path().to_path_buf(),
                block_dir: None,
                db_name: None,
                shard_id: 0,
                create_if_missing: true,
                chunk_cache_size: 4,
                io_backend: IoBackend::StdFs,
                io_config: IoBackendConfig::default(),
                wal_mode: crate::wal::WalMode::Off,
            };
            let mut engine = StorageEngine::open(opts).await.expect("open ok");
            engine.create_db("logs").await.expect("create logs");
            let logs_id = engine.registry().db_id("logs").expect("logs id");

            // 切到 logs_id
            engine.set_current_db(logs_id);
            assert_eq!(engine.current_db(), logs_id);
            assert_eq!(engine.current_db_name().expect("name ok"), "logs");

            // set_current_db 非法 id 不报错, 但 current_db_name 报 DbNotFound
            engine.set_current_db(9999);
            assert_eq!(engine.current_db(), 9999);
            assert!(matches!(
                engine.current_db_name(),
                Err(RegistryError::DbNotFound(_))
            ));
        });
    }

    /// current_db 跨 reopen 持久化: open → use_db("X") → close → reopen → 验证
    /// current_db 重置回 DEFAULT_DB_ID (因为 current_db 是 in-memory, 不持久化).
    #[test]
    fn current_db_resets_to_default_after_reopen() {
        run_async(async {
            let tmp = tempfile::tempdir().unwrap();
            let block_root = tmp.path().to_path_buf();

            // 第一次 open + use_db
            let opts1 = OpenOptions {
                block_root: block_root.clone(),
                block_dir: None,
                db_name: None,
                shard_id: 0,
                create_if_missing: true,
                chunk_cache_size: 4,
                io_backend: IoBackend::StdFs,
                io_config: IoBackendConfig::default(),
                wal_mode: crate::wal::WalMode::Off,
            };
            let mut engine1 = StorageEngine::open(opts1).await.expect("open1 ok");
            engine1
                .create_db("analytics")
                .await
                .expect("create analytics");
            let analytics_id = engine1.use_db("analytics").expect("use_db analytics");
            assert_eq!(engine1.current_db(), analytics_id);

            // close (drop)
            let _ = engine1.close().await;

            // 第二次 open
            let opts2 = OpenOptions {
                block_root,
                block_dir: None,
                db_name: None,
                shard_id: 0,
                create_if_missing: false,
                chunk_cache_size: 4,
                io_backend: IoBackend::StdFs,
                io_config: IoBackendConfig::default(),
                wal_mode: crate::wal::WalMode::Off,
            };
            let engine2 = StorageEngine::open(opts2).await.expect("open2 ok");
            // current_db 重置为 DEFAULT_DB_ID
            assert_eq!(
                engine2.current_db(),
                DEFAULT_DB_ID,
                "reopen 后 current_db 重置"
            );
            // 但 "analytics" db 仍然存在
            assert!(engine2.registry().db_id("analytics").is_some());
        });
    }
}
