//! T6 Pager: read / create / write_batch / flush 派发器.
//!
//! 设计 (DESIGN §4.5 + plan §3.0/§3.0.4/§3.0.5):
//! - **三源查找**: `read` 按 `nowchunks > WriteQueue > chunk_list > disk` 顺序找 page.
//! - **写路径**: `create` / `take_page_for_write` / `new_write_batch().submit` 走 nowchunks
//!   (LSM 写缓冲), flush 触发落盘 + meta_cache flush.
//! - **COW**: `take_page_for_write` 拿独立 owned bytes, 修改不影响 chunk_list 旧值.
//! - **PageWriteBatch**: 一次提交多个 page 到 nowchunks, 返回 vpid→pid mappings.
//!
//! **T16 (2026-07-21)**: IO 走 `PagerIo` 抽象层, 支持 StdFs / IoUring 后端切换.
//! Pager 不再直接调 `std::fs::File::read_exact_at` / `write_all_at`, 改为
//! `self.io.read_page_chunk()` / `write_page_chunk()` / `fsync_block()`.
//!
//! **单线程使用**: per-shard thread, 与 scheduler crate 契约一致.

use std::collections::HashMap;
use std::rc::Rc;
use std::io;
use std::path::PathBuf;

use crate::pager_io::PagerIo;

use crate::alloc::{PidAllocator, VpidAllocator};
use crate::chunk_lock::{AcquireResult, ChunkLockMap};
use crate::chunk_lru::{ChunkKey, ChunkList};
use crate::chunk_writer::{ChunkWriter, NowChunks, WriteQueue};
use crate::meta_cache::MetaCache;
use crate::meta_page::{META_PID, META_VPID};
use crate::page_pool;
use crate::types::{
    CHUNK_SIZE, CHUNKS_PER_BLOCK, DEFAULT_DB_ID, DEFAULT_DB_NAME, DEFAULT_SHARD_ID, PAGE_SIZE,
    PageKey, PidLocation, ShardId,
};

// =====================================================================
// Pager 主体
// =====================================================================

/// 读 / 写 / 创建 page 的派发器.
///
/// 持有 MetaCache / ChunkList / VpidAllocator / PidAllocator / NowChunks / ChunkWriter.
/// 整合后对外提供高层 API.
///
/// **T16**: `io` 字段是 `PagerIo` 抽象层, 支持 StdFs / IoUring 切换.
pub struct Pager {
    /// ⭐ T12.12: 根目录. ShardManager 级共享 (所有 db / shard 都基于此).
    /// 实际 .block 所在目录 = `{block_root}/{current_db}/shard_{shard_id}/`.
    #[allow(dead_code)]
    block_root: PathBuf,
    /// 当前 db 名称 (T12.16 multi-db 切换).
    #[allow(dead_code)]
    db_name: String,
    /// ⭐ T12.12: 当前 shard id (0 = 单 shard 模式).
    #[allow(dead_code)]
    shard_id: ShardId,
    /// ⭐ 内部直接 block_dir: 拼好的实际路径 (= `{block_root}/{db_name}/shard_{N}/`).
    /// 所有 .block / page.mate 读写都用这个.
    block_dir: PathBuf,
    /// vpid → pid 映射缓存
    meta: MetaCache,
    /// 1MB chunk 只读 LRU 缓存
    chunk_list: ChunkList,
    /// ⭐ 协程级 chunk 锁 (DESIGN §3.0). 同步版本下不会真触发 wait queue,
    /// 数据结构完整保留, T11 polish 接 async 时激活.
    chunk_lock: ChunkLockMap,
    /// 虚拟 page ID 分配
    vpid_alloc: VpidAllocator,
    /// 物理 page ID 分配
    pid_alloc: PidAllocator,
    /// LSM 写缓冲
    nowchunks: NowChunks,
    /// ⭐ WriteQueue: nowchunks 和 disk 之间的桥接队列.
    /// 数据流: nowchunks → drain_dirty → WriteQueue pending → 落盘 → WriteQueue completed → chunk_list.
    /// 读路径三源查找: nowchunks → WriteQueue → chunk_list → disk.
    write_queue: WriteQueue,
    /// 落盘编排器 (持有 .block fd 等)
    #[allow(dead_code)]
    writer: ChunkWriter,
    /// TravelTrees: task_id → TravelTree. 用于 B+Tree split 传播 (T8 polish)
    pub(crate) travel_trees: HashMap<TaskId, TravelTree>,
    /// ⭐ T16: IO 后端抽象 (StdFs / IoUring). Rc 共享: 异步落盘协程持有克隆
    /// (shard 单线程模型, Rc 安全).
    io: Rc<PagerIo>,
    /// ⭐ 自动持久化: 写计数器 (周期/计数 flush 用).
    write_count_since_flush: u64,
    /// ⭐ 自动持久化: 上次 flush 时间.
    last_flush_time: std::time::Instant,
    /// ⭐ 异步落盘: 已 spawn 协程、等待完成的 chunk (key → 写盘中的字节).
    /// Rc 与落盘协程共享; 读路径可见; 完成后迁入 chunk_list.
    in_flight: HashMap<PageKey, Rc<Vec<u8>>>,
    /// ⭐ Phase M3: data backlog 排空后置位, 下轮 drive 取 meta window 快照
    /// 异步写盘 (不再在收割路径同步 fsync page.mate).
    meta_flush_due: bool,
}

/// ⭐ 异步落盘作业 (Phase C: 按 file_id 分组批量): 零 Pager 借用,
/// 所需句柄全部 owned/Rc, 由 shard 线程 spawn 成独立协程执行.
/// 同 file 的 N 个 chunk 一个协程内逐个 write + 单次 fsync (长尾对症).
pub struct FlushBatch {
    pub items: Vec<(PageKey, Rc<Vec<u8>>)>,
    pub dir: PathBuf,
    pub io: Rc<PagerIo>,
}

/// ⭐ Phase M3: meta window 异步落盘作业: 零 Pager 借用.
/// dirty window 的 copy 快照, 协程内 write ×N + fsync ×1.
pub struct MetaFlushBatch {
    pub windows: Vec<(u32, Vec<u8>)>,
    pub mate_path: PathBuf,
    pub io: Rc<PagerIo>,
}

/// ⭐ 异步落盘: in-flight + pending 总数上限 (背压阈值).
/// 超出后 chunk 满 swap 退化为同步落盘, 写入降速到磁盘速度.
const MAX_INFLIGHT_CHUNKS: usize = 8;

/// 写 page header 到 disk page 字节.
///
/// **disk page layout (DESIGN §4.2.3, 40B header)**:
/// - [0x00..0x04] magic "LCBP" = [0x4C, 0x43, 0x42, 0x50]
/// - [0x04]       page_type (1=Meta, 2=Internal, 3=Leaf)
/// - [0x14..0x18] version (4B LE, 当前固定 1)
/// - [0x18..0x20] vpid (8B LE)
///
/// **覆盖 caller 字节 [0..0x28]**: caller 字节应假设 [0..0x28] 是 header 区域,
/// 数据写在 [0x28..PAGE_SIZE] = 16344B 范围. caller 标记位建议用 ≥ 0x28 偏移.
fn write_page_header(page: &mut [u8], vpid: u64) {
    debug_assert!(page.len() == PAGE_SIZE);
    page[0..4].copy_from_slice(&[0x4C, 0x43, 0x42, 0x50]); // "LCBP"
    page[4] = 3; // page_type = Leaf (TDD 简化: 暂只写 leaf)
    page[0x14..0x18].copy_from_slice(&1u32.to_le_bytes()); // version
    page[0x18..0x20].copy_from_slice(&vpid.to_le_bytes());
}

/// task ID (per-shard 全局唯一, 由 scheduler slot_id 提供).
pub type TaskId = u64;

// =====================================================================
// Pager 核心方法
// =====================================================================

impl Pager {
    /// 构造 Pager. **不**做 IO, 不读磁盘.
    ///
    /// **Compat API (T12.12 之前)**: 单 db 单 shard 测试用, block_dir 直接是 .block 所在.
    ///
    /// caller 负责:
    /// - `meta` 应已 `MetaCache::open(&mate_path)`
    /// - `pid_alloc` 应已根据 recover 初始化 (T7) 或从 (0, 0, 0) 开始
    /// - `vpid_alloc` 应已根据 recover 初始化 或从 0 开始
    /// - `block_dir` 直接是 .block / page.mate 所在目录
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        block_dir: PathBuf,
        meta: MetaCache,
        vpid_alloc: VpidAllocator,
        pid_alloc: PidAllocator,
        chunk_list: ChunkList,
        nowchunks: NowChunks,
        writer: ChunkWriter,
    ) -> Self {
        Self::with_io(
            block_dir,
            meta,
            vpid_alloc,
            pid_alloc,
            chunk_list,
            nowchunks,
            writer,
            PagerIo::default(),
        )
    }

    /// ⭐ T16: 构造 Pager (兼容 API + 指定 IO 后端). 与 `new` 等价, 加 `io` 参数.
    #[allow(clippy::too_many_arguments)]
    pub fn with_io(
        block_dir: PathBuf,
        meta: MetaCache,
        vpid_alloc: VpidAllocator,
        pid_alloc: PidAllocator,
        chunk_list: ChunkList,
        nowchunks: NowChunks,
        writer: ChunkWriter,
        io: PagerIo,
    ) -> Self {
        Self {
            block_root: block_dir.clone(),
            db_name: DEFAULT_DB_NAME.to_string(),
            shard_id: DEFAULT_SHARD_ID,
            block_dir,
            meta,
            chunk_list,
            chunk_lock: ChunkLockMap::new(),
            vpid_alloc,
            pid_alloc,
            nowchunks,
            write_queue: WriteQueue::new(),
            writer,
            travel_trees: HashMap::new(),
            io: Rc::new(io),
            write_count_since_flush: 0,
            last_flush_time: std::time::Instant::now(),
            in_flight: HashMap::new(),
            meta_flush_due: false,
        }
    }

    /// ⭐ T12.12 构造 Pager (新 API). 走 `{block_root}/{db_name}/shard_{shard_id}/` 路径.
    ///
    /// **参数**:
    /// - `block_root`: ShardManager 级根目录. 实际 .block 在 `{block_root}/{db_name}/shard_{shard_id}/`.
    /// - `db_name`: 当前 db 名 (T12.16 multi-db 切换时变).
    /// - `shard_id`: 当前 shard id (0 = 单 shard).
    /// - `meta`: 已 `MetaCache::open(&mate_path)`.
    /// - `vpid_alloc` / `pid_alloc`: 已根据 recover 初始化 或从 (0, 0, 0) 开始.
    /// - `chunk_list`: LRU 缓存.
    /// - `nowchunks` + `writer`: 已构造.
    #[allow(clippy::too_many_arguments)]
    pub fn new_for_shard(
        block_root: PathBuf,
        db_name: String,
        shard_id: ShardId,
        meta: MetaCache,
        vpid_alloc: VpidAllocator,
        pid_alloc: PidAllocator,
        chunk_list: ChunkList,
        nowchunks: NowChunks,
        writer: ChunkWriter,
    ) -> Self {
        Self::new_for_shard_with_io(
            block_root,
            db_name,
            shard_id,
            meta,
            vpid_alloc,
            pid_alloc,
            chunk_list,
            nowchunks,
            writer,
            PagerIo::default(),
        )
    }

    /// ⭐ T16: 构造 Pager (新 API + 指定 IO 后端).
    #[allow(clippy::too_many_arguments)]
    pub fn new_for_shard_with_io(
        block_root: PathBuf,
        db_name: String,
        shard_id: ShardId,
        meta: MetaCache,
        vpid_alloc: VpidAllocator,
        pid_alloc: PidAllocator,
        chunk_list: ChunkList,
        nowchunks: NowChunks,
        writer: ChunkWriter,
        io: PagerIo,
    ) -> Self {
        let block_dir = crate::recover::shard_dir_path(&block_root, &db_name, shard_id);
        Self {
            block_root,
            db_name,
            shard_id,
            block_dir,
            meta,
            chunk_list,
            chunk_lock: ChunkLockMap::new(),
            vpid_alloc,
            pid_alloc,
            nowchunks,
            write_queue: WriteQueue::new(),
            writer,
            travel_trees: HashMap::new(),
            io: Rc::new(io),
            write_count_since_flush: 0,
            last_flush_time: std::time::Instant::now(),
            in_flight: HashMap::new(),
            meta_flush_due: false,
        }
    }

    /// 读 page. **核心四源查找** (nowchunks + WriteQueue + chunk_list + disk):
    /// 1. peek nowchunks (有 → 立即返回 owned bytes)
    /// 2. WriteQueue peek (pending 或 completed 中的 chunk 对读路径可见)
    /// 3. peek chunk_list (有 → 切片返回)
    /// 4. miss → load_fn 同步读 .block → 插入 chunk_list → 切片返回
    pub async fn read(&mut self, vpid: u64) -> io::Result<Box<[u8; PAGE_SIZE]>> {
        // 1. meta_cache 拿 pid
        let pid = self.meta.read(vpid).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("vpid {} not mapped", vpid))
        })?;

        let key = PageKey {
            file_id: pid.file_id(),
            chunk_idx: pid.chunk_idx(),
        };
        let page_idx = pid.page_idx() as u8;

        // 2. nowchunks 优先 (有最新未 flush 数据)
        if let Some(chunk_bytes) = self.nowchunks.peek_chunk(key) {
            let mut out = page_pool::alloc();
            let off = page_idx as usize * PAGE_SIZE;
            out.copy_from_slice(&chunk_bytes[off..off + PAGE_SIZE]);
            return Ok(out);
        }
        // 3. WriteQueue 检索: pending 或 completed 中的 chunk 对读路径可见
        if let Some(chunk_bytes) = self.write_queue.peek_chunk_pending(key) {
            let mut out = page_pool::alloc();
            let off = page_idx as usize * PAGE_SIZE;
            out.copy_from_slice(&chunk_bytes[off..off + PAGE_SIZE]);
            return Ok(out);
        }
        if let Some(chunk_bytes) = self.write_queue.peek_chunk_completed(key) {
            let mut out = page_pool::alloc();
            let off = page_idx as usize * PAGE_SIZE;
            out.copy_from_slice(&chunk_bytes[off..off + PAGE_SIZE]);
            return Ok(out);
        }
        // 3b. ⭐ 异步落盘: 写盘中的 in-flight 快照对读可见
        if let Some(bytes) = self.in_flight.get(&key) {
            let mut out = page_pool::alloc();
            let off = page_idx as usize * PAGE_SIZE;
            out.copy_from_slice(&bytes[off..off + PAGE_SIZE]);
            return Ok(out);
        }
        // 4. chunk_list 命中 (peek 走 LRU 访问)
        let chunk_arc = if self.chunk_list.contains(&key.into()) {
            // hit: peek 把 key 移到 front
            self.chunk_list
                .peek(&key.into())
                .expect("just checked contains")
        } else {
            // miss: 同步读盘
            let bytes = self.load_chunk_from_disk(key).await?;
            self.chunk_list.insert(key.into(), bytes);
            self.chunk_list.peek(&key.into()).expect("just inserted")
        };

        // 5. 切片: chunk 内 page_idx 偏移
        let mut out = page_pool::alloc();
        let off = page_idx as usize * PAGE_SIZE;
        out.copy_from_slice(&chunk_arc[off..off + PAGE_SIZE]);
        Ok(out)
    }

    /// ⭐ B 级优化: 借用版 read, caller 提供 buffer, 减少 Box 分配.
    ///
    /// `buf` 长度必须是 `PAGE_SIZE`, 否则返回 InvalidInput.
    pub async fn read_into(&mut self, vpid: u64, buf: &mut [u8]) -> io::Result<()> {
        if buf.len() != PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("buf len {} != PAGE_SIZE {}", buf.len(), PAGE_SIZE),
            ));
        }
        let pid = self.meta.read(vpid).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("vpid {} not mapped", vpid))
        })?;

        let key = PageKey {
            file_id: pid.file_id(),
            chunk_idx: pid.chunk_idx(),
        };
        let page_idx = pid.page_idx() as u8;
        let off = page_idx as usize * PAGE_SIZE;

        if let Some(chunk_bytes) = self.nowchunks.peek_chunk(key) {
            buf.copy_from_slice(&chunk_bytes[off..off + PAGE_SIZE]);
            return Ok(());
        }

        // WriteQueue 检索 (pending 或 completed)
        if let Some(chunk_bytes) = self.write_queue.peek_chunk_pending(key) {
            buf.copy_from_slice(&chunk_bytes[off..off + PAGE_SIZE]);
            return Ok(());
        }
        if let Some(chunk_bytes) = self.write_queue.peek_chunk_completed(key) {
            buf.copy_from_slice(&chunk_bytes[off..off + PAGE_SIZE]);
            return Ok(());
        }
        // ⭐ 异步落盘: in-flight 快照对读可见
        if let Some(bytes) = self.in_flight.get(&key) {
            buf.copy_from_slice(&bytes[off..off + PAGE_SIZE]);
            return Ok(());
        }

        let chunk_arc = if self.chunk_list.contains(&key.into()) {
            self.chunk_list
                .peek(&key.into())
                .expect("just checked contains")
        } else {
            let bytes = self.load_chunk_from_disk(key).await?;
            self.chunk_list.insert(key.into(), bytes);
            self.chunk_list.peek(&key.into()).expect("just inserted")
        };

        buf.copy_from_slice(&chunk_arc[off..off + PAGE_SIZE]);
        Ok(())
    }

    /// ⭐ 拿 page 的 owned 副本 (COW). 修改返回的 bytes 不影响 chunk_list 旧值.
    ///
    /// 内部: read 已经返回 Box, 直接转发.
    pub async fn take_page_for_write(&mut self, vpid: u64) -> io::Result<Box<[u8; PAGE_SIZE]>> {
        self.read(vpid).await
    }

    /// 创建新 page. 分配 vpid + 走 nowchunks (单 page 也走 batch).
    pub async fn create(&mut self, data: Box<[u8; PAGE_SIZE]>) -> io::Result<u64> {
        let vpid = self.vpid_alloc.alloc(&mut self.meta);
        let mut batch = self.new_write_batch();
        batch.add(vpid, data);
        let mappings = batch.submit(self).await?;
        debug_assert_eq!(mappings.len(), 1);
        debug_assert_eq!(mappings[0].0, vpid);
        Ok(vpid)
    }

    /// 覆盖已存在的 vpid. 走 nowchunks 分配新 pid (COW 友好, 旧 pid 仍留在
    /// chunk_list 直到 LRU 踢出). meta_cache 写回会标 dirty, flush 时持久化.
    ///
    /// **典型用法**:
    /// - `TableDirectory` 改 leaf page 后写回
    /// - `DbRegistry` 改 MetaPage 后写回
    /// - 用户 B+Tree 内部 page 修改后写回
    ///
    /// **vpid 必须已分配并映射到某个 pid**, 否则会因 `meta.read` 失败
    /// 而在 `PageWriteBatch::submit` 内部 `nowchunks.write_page_with_vpid`
    /// 静默写入到错误位置. caller 应保证 vpid 存在 (recover 后从 MetaPage
    /// 拿到的 root_vpid 都已映射).
    pub async fn write_page(&mut self, vpid: u64, data: Box<[u8; PAGE_SIZE]>) -> io::Result<()> {
        // 验证 vpid 已映射 (防御性)
        if self.meta.read(vpid).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("write_page: vpid {} not mapped", vpid),
            ));
        }
        let mut batch = self.new_write_batch();
        batch.add(vpid, data);
        let mappings = batch.submit(self).await?;
        debug_assert_eq!(mappings.len(), 1);
        debug_assert_eq!(mappings[0].0, vpid);
        Ok(())
    }

    /// 创建一个新的 PageWriteBatch (caller 多次 add 后调 submit 一次).
    pub fn new_write_batch(&self) -> PageWriteBatch {
        PageWriteBatch::new()
    }

    /// ⭐ flush: 把 nowchunks dirty chunks 同步落盘 + 插入 chunk_list + flush meta.
    ///
    /// 流程 (DESIGN §3.0.3):
    /// 1. 遍历 nowchunks 所有 dirty chunks, 对每个:
    ///    a. pwrite chunk bytes 到 .block 对应 chunk offset
    ///    b. fsync .block
    ///    c. chunk_list.insert_from_write_queue (Arc 共享, 零拷贝)
    /// 2. flush meta_cache dirty windows → fsync page.mate
    /// 3. nowchunks 留 dirty 状态 (下次 write_page 仍走 nowchunks, lazy alloc 覆盖)
    pub async fn flush(&mut self) -> io::Result<()> {
        // ⭐ 异步落盘契约: caller (shard 主循环/close) 必须先排空 in-flight
        // 再调 flush, 否则同 key 新旧快照可能并发写同一磁盘 offset.
        debug_assert!(
            self.in_flight.is_empty(),
            "flush() requires in-flight chunks drained first"
        );
        // ⭐ 纯 COW (2026-07-26): 驻留即待刷, take 即移出不回插.
        // flush 后这些 chunk 内 vpid 的后续更新走 COW alloc 新 pid.
        let resident: Vec<PageKey> = self.nowchunks.resident_keys();

        for key in resident {
            // 1. 从 nowchunks take_chunk_box 拿完整 1MB chunk.
            //    **直接写盘, 不需要 merge**: pid_alloc 单调不重用,
            //    chunk 内 page_idx 0..N 是该 chunk 内已 alloc 过的所有 page.
            //    未 alloc 过的 page_idx 位置是 0 (recover 时 magic 检查自动跳过).
            let chunk_box = match self.nowchunks.take_chunk_box(key) {
                Some(b) => b,
                None => continue,
            };

            // 2. 直接 pwrite 1MB chunk 到 .block (1MB write + fsync).
            //    chunk_box 含完整 chunk 视图 (含历史 page + 本次新写 page).
            self.io
                .write_page_chunk(&self.block_dir, key, chunk_box.to_vec())
                .await?;
            // 3. 移入 chunk_list (作为该 chunk 的最新视图, 后续 read 优先命中).
            self.chunk_list
                .insert_from_write_queue(key, chunk_box.to_vec());
            // 4. ⭐ 回滚防护: 移除 write_queue 中同 key 的 stale 快照
            //    (本次写盘版本 ⊇ 旧快照, 不清除会被旧快照回滚覆盖).
            self.write_queue.remove_pending(key);
        }

        // ⭐ 纯 COW 补充 (2026-07-26): swap 进 pending 但尚未被异步协程写盘的
        // chunk 也必须在这里落盘 —— flush() 语义是"全量持久化".
        // (旧版靠 reinsert 后驻留 chunk 全量重写掩盖了 pending 未写的问题;
        //  无 shard 主循环 drive 的纯 Pager 使用者 (测试/嵌入) 依赖此保证.)
        let pending: Vec<PageKey> = self.write_queue.pending_keys();
        for key in pending {
            if let Some(bytes) = self.write_queue.take_pending(key) {
                self.io
                    .write_page_chunk_slice(&self.block_dir, key, &bytes)
                    .await?;
                self.chunk_list.insert_from_write_queue(key, bytes);
            }
        }

        // 5. ⭐ chunk data 全部确认写完之后, 才 flush meta
        // 保证: meta 永远不会指向还没写完的 chunk data
        self.meta.flush_dirty()?;
        self.persist_pid_state();

        self.write_count_since_flush = 0;
        self.last_flush_time = std::time::Instant::now();

        Ok(())
    }

    /// ⭐ 自动持久化: chunk 满时 swap 到 WriteQueue.
    /// ⭐ 纯 COW (2026-07-26): take 即移出, **不再 reinsert** —— swap 后该 chunk
    /// 内 vpid 的后续更新走 COW alloc 新 pid (省掉每次 swap 的 1MB 回插拷贝).
    ///
    /// ⭐ 背压: pending + in_flight 达到 `MAX_INFLIGHT_CHUNKS` 时,
    /// 本次 swap **退化为同步落盘** —— 磁盘到上限时写入自然降速到磁盘速度,
    /// 避免写时队列无限膨胀, 且无等待+收割死锁风险.
    pub async fn swap_full_chunk_to_write_queue(&mut self, key: PageKey) -> io::Result<()> {
        let Some(chunk_box) = self.nowchunks.take_chunk_box(key) else {
            return Ok(());
        };
        let chunk_vec = chunk_box.to_vec();

        if self.write_queue.pending_keys().len() + self.in_flight.len()
            >= MAX_INFLIGHT_CHUNKS
        {
            // 背压: 同步落盘 (不入队). 同 key 若在 in-flight 则仍入队
            // (不能并发写同 offset), 由后续 take_flush_batches 去重处理.
            if self.in_flight.contains_key(&key) {
                self.write_queue
                    .enqueue(crate::chunk_writer::WriteHandle::new(key, chunk_vec));
                return Ok(());
            }
            // ⭐ 探针 (NLOG_PROBE=1): 背压退化同步写耗时. 用 dprintln! 而非
            // 直接引用 shard_manager (避免反向依赖). shard 端 manager.rs 的
            // maybe_periodic_flush block_on_io_ns 也会捕获到这一步的等待.
            let bp_start = std::time::Instant::now();
            self.io
                .write_page_chunk_slice(&self.block_dir, key, &chunk_vec)
                .await?;
            let bp_ns = bp_start.elapsed().as_nanos() as u64;
            // ⭐ 探针: 编译期开关 (DEBUG_PAGE_PROBE), 运行时零开销.
            // 无 shard_manager 反向依赖. shard 端 manager.rs 的
            // maybe_periodic_flush block_on_io_ns 也会捕获到这一步的等待.
            if page::debug::DEBUG_PAGE && page::debug::DEBUG_PAGE_PROBE {
                eprintln!("[pager_probe] backpressure_sync_write_ns={bp_ns} key={:?}", key);
            }
            // 旧快照作废 (本次写盘版本 ⊇ pending 旧快照)
            self.write_queue.remove_pending(key);
            self.chunk_list.insert_from_write_queue(key, chunk_vec);
            return Ok(());
        }

        self.write_queue
            .enqueue(crate::chunk_writer::WriteHandle::new(key, chunk_vec));
        Ok(())
    }

    /// ⭐ 异步落盘: 取出待提交的落盘批 (shard 线程 spawn 成协程执行).
    ///
    /// Phase C: 按 file_id 分组返回 FlushBatch, 每批一个协程
    /// `write_chunks_batch` (write ×N + fsync ×1).
    /// 同 key 去重: 已在 in-flight 的 key 跳过 (新快照留在 pending,
    /// 完成后下轮再提交), 避免两个协程并发写同一磁盘 offset 乱序.
    pub fn take_flush_batches(&mut self) -> Vec<FlushBatch> {
        let pending_keys = self.write_queue.pending_keys();
        // 按 file_id 分组 (保持 key 顺序; 单 shard 通常只有 1 个 file)
        type FileGroup = (u32, Vec<(PageKey, Rc<Vec<u8>>)>);
        let mut batches: Vec<FileGroup> = Vec::new();
        for key in pending_keys {
            if self.in_flight.contains_key(&key) {
                continue; // 同 key 在写, 新快照等下轮
            }
            if let Some(bytes) = self.write_queue.take_pending(key) {
                let rc = Rc::new(bytes);
                self.in_flight.insert(key, rc.clone());
                match batches.iter_mut().find(|(fid, _)| *fid == key.file_id) {
                    Some((_, items)) => items.push((key, rc)),
                    None => batches.push((key.file_id, vec![(key, rc)])),
                }
            }
        }
        batches
            .into_iter()
            .map(|(_, items)| FlushBatch {
                items,
                dir: self.block_dir.clone(),
                io: self.io.clone(),
            })
            .collect()
    }

    /// ⭐ 异步落盘: 协程完成后的收割回调 (shard 主循环调用).
    ///
    /// - 成功: 移出 in-flight, 字节迁入 chunk_list;
    ///   若 in-flight 归零且无 pending → 置位 meta_flush_due (下轮异步刷 meta,
    ///   data→meta 顺序保证; 不再在收割路径同步 fsync page.mate)
    /// - 失败: 重新入 pending 下轮重试, meta 不前进
    pub fn complete_flush(&mut self, key: PageKey, result: io::Result<()>) -> io::Result<()> {
        let Some(rc) = self.in_flight.remove(&key) else {
            return Ok(()); // 防御: 未知 key
        };
        match result {
            Ok(()) => {
                let bytes = Rc::try_unwrap(rc).unwrap_or_else(|rc| (*rc).clone());
                self.chunk_list.insert_from_write_queue(key, bytes);
                // 本批全部确认落盘且无待提交快照 → meta 可安全异步刷
                if self.in_flight.is_empty() && self.write_queue.pending_keys().is_empty() {
                    self.meta_flush_due = true;
                }
            }
            Err(e) => {
                let bytes = Rc::try_unwrap(rc).unwrap_or_else(|rc| (*rc).clone());
                self.write_queue
                    .enqueue(crate::chunk_writer::WriteHandle::new(key, bytes));
                return Err(e);
            }
        }
        Ok(())
    }

    /// 异步落盘: 是否有写盘中的 chunk.
    pub fn has_inflight(&self) -> bool {
        !self.in_flight.is_empty()
    }

    /// 异步落盘: 写盘中 + 待提交的 chunk 总数 (测试/背压观察).
    /// 注意: 不含 meta backlog (背压阈值只看 data chunk).
    pub fn flush_backlog(&self) -> usize {
        self.in_flight.len() + self.write_queue.pending_keys().len()
    }

    /// ⭐ Phase M3: 含 meta 的全部异步 backlog (drain/close 判空用).
    /// data in-flight/pending + meta due/dirty/in-flight 全部排空才算真正空闲.
    pub fn total_async_backlog(&self) -> usize {
        self.flush_backlog()
            + self.meta.in_flight_window_count()
            + (self.meta_flush_due && self.meta.has_unflushed()) as usize
    }

    /// ⭐ Phase M3: 取 meta window 异步刷盘批.
    ///
    /// **保序关键**: 仅在 meta_flush_due 且 data backlog 为空时取快照
    /// (有 in-flight data chunk 时绝不发 meta, data→meta 顺序不变);
    /// 同 window 有在飞快照时由 MetaCache 去重 (防同 offset 乱序).
    pub fn take_meta_flush_batch(&mut self) -> Option<MetaFlushBatch> {
        if !self.meta_flush_due || self.flush_backlog() > 0 {
            return None;
        }
        self.meta_flush_due = false;
        let windows = self.meta.take_dirty_window_snapshots();
        // 有 dirty 因 in-flight 去重被跳过 → 保持 due, 收割后下轮再取
        if self.meta.dirty_count() > 0 {
            self.meta_flush_due = true;
        }
        if windows.is_empty() {
            return None; // 无脏 window (或全在飞), 不发空批
        }
        Some(MetaFlushBatch {
            windows,
            mate_path: self.block_dir.join("page.mate"),
            io: self.io.clone(),
        })
    }

    /// ⭐ Phase M3: 收割 meta window 写盘结果.
    ///
    /// 全部 window 确认后才 persist_pid_state (data→meta→pid.state 顺序闭环);
    /// 失败重标 dirty + 重新置位 due (下轮重试).
    pub fn complete_meta_flush(&mut self, window_idx: u32, result: io::Result<()>) {
        let ok = result.is_ok();
        self.meta.complete_window_flush(window_idx, ok);
        if !ok {
            self.meta_flush_due = true;
            return;
        }
        if !self.meta.has_unflushed() {
            self.persist_pid_state();
        }
    }

    /// ⭐ 自动持久化: 驱动 WriteQueue 落盘.
    /// 遍历 pending chunks, 异步写盘, 完成后移入 chunk_list.
    /// ⭐ 关键: 全部 chunk 写完后才 flush meta (保证一致性).
    pub async fn drive_write_queue(&mut self) -> io::Result<()> {
        // 取出所有 pending keys
        let pending_keys: Vec<PageKey> = self.write_queue.pending_keys();
        if pending_keys.is_empty() {
            return Ok(());
        }

        // 先写完所有 chunk data
        for key in &pending_keys {
            if let Some(chunk_bytes) = self.write_queue.peek_chunk_pending(*key) {
                let bytes = chunk_bytes.to_vec();
                self.io.write_page_chunk(&self.block_dir, *key, bytes).await?;
            }
            self.write_queue.mark_completed(*key);
        }

        // drain completed 入 chunk_list
        let completed = self.write_queue.drain_completed();
        for handle in completed {
            self.chunk_list.insert_from_write_queue(handle.key, handle.chunk);
        }

        // ⭐ 全部 chunk data 确认落盘后, 再 flush meta
        self.meta.flush_dirty()?;
        self.persist_pid_state();

        Ok(())
    }

    /// ⭐ 自动持久化: 周期/计数检查.
    /// 如果 writes >= 256 或 elapsed >= 10s, 执行 dirty flush + meta flush.
    /// 返回 true 表示触发了 flush.
    pub async fn maybe_periodic_flush(&mut self) -> io::Result<bool> {
        const FLUSH_WRITE_THRESHOLD: u64 = 256;
        const FLUSH_PERIOD_SECS: u64 = 10;

        // ⭐ 背压守卫: 落盘队列已满时推迟 (磁盘已饱和, 再入队只会膨胀).
        if self.flush_backlog() >= MAX_INFLIGHT_CHUNKS {
            return Ok(false);
        }

        let should_flush = self.write_count_since_flush >= FLUSH_WRITE_THRESHOLD
            || self.last_flush_time.elapsed().as_secs() >= FLUSH_PERIOD_SECS;

        if !should_flush {
            return Ok(false);
        }

        // ⭐ 纯 COW + 数组化 (2026-07-26): 驻留即待刷.
        // 未满 active chunk **快照入队不 take** (chunk 留在原地继续增量写);
        // 满 chunk 已在满时自动 swap, 这里只剩 active chunk (通常 ≤ 2 个).
        // 重复快照未变化 chunk 的开销 (~1MB/10s) 已记录为设计取舍 (plan A3).
        // meta 由 complete_flush 在 backlog 排空后统一刷 (data→meta 不变).
        let resident: Vec<PageKey> = self.nowchunks.resident_keys();
        for key in resident {
            if let Some(chunk_data) = self.nowchunks.peek_chunk(key) {
                let chunk_vec = chunk_data.to_vec();
                // 旧 pending 快照作废 (本快照 ⊇ 旧快照)
                self.write_queue.remove_pending(key);
                self.write_queue
                    .enqueue(crate::chunk_writer::WriteHandle::new(key, chunk_vec));
            }
        }

        self.write_count_since_flush = 0;
        self.last_flush_time = std::time::Instant::now();
        Ok(true)
    }

    /// 增加写计数 (由 PageWriteBatch::submit 调用).
    pub fn inc_write_count(&mut self, n: u64) {
        self.write_count_since_flush += n;
    }

    /// ⭐ Phase B: 持久化 pid_alloc 水位到 `{block_dir}/pid.state` (8B PidLocation).
    ///
    /// 在每次 meta.flush_dirty() 成功后调用 (data→meta→pid.state 顺序).
    /// pid.state 是纯 hint: recover 与 .block 扫描取较大值, 落后/丢失都安全
    /// (最多浪费少量 pid 槽, COW 语义下无害). 因此**不做 fsync** ——
    /// 收割路径上每次 2-5ms 的 fsync 会直接抬高写延迟 p99 (探针实测),
    /// 交给 page cache 回写即可. 8B 小写用同步 std fs (不值得走 io_uring).
    fn persist_pid_state(&self) {
        use std::io::Write;
        let (file_id, chunk_idx, page_idx) = self.pid_alloc.current();
        let pid = PidLocation {
            file_id,
            chunk_idx,
            page_idx: page_idx as u16,
            flags: crate::types::PID_ALIVE,
        };
        let path = self.block_dir.join("pid.state");
        // 失败只记日志不阻断 (退化为扫描 recover)
        let r = std::fs::File::create(&path).and_then(|mut f| f.write_all(&pid.to_bytes()));
        if let Err(e) = r {
            eprintln!("[pager] persist pid.state failed (fallback to scan recover): {e}");
        }
    }

    /// 内部: 从 .block 文件读 1MB chunk (load_fn 用).
    /// **重要**: disk page layout = [header(32B)][user_data(16352B)],
    /// chunk_list 统一存 user_data 视图 (16KB per page, 无 header).
    /// 读盘后**剥 header**: 对每个 page, 跳过前 32B, 拷贝后 16352B 进 user view.
    /// 但 PAGE_SIZE = 16KB, 而 user_data = 16352B, 这里简化:
    /// **TDD 简化**: disk page 与内存 page 都用 16KB, 落盘时写 header 到 page[0..32]
    /// (覆盖 caller 写的前 32B); 读盘后 caller 拿到的 page 字节 = disk 字节 = 含 header.
    /// caller 应知道 page header 在 page[0..32]. 后续 T8 polish 会改 page layout.
    ///
    /// **⭐ 修复 (2026-07-21)**: 与 `chunk_offset` 保持一致, 每个 .block 文件独立 offset 空间.
    ///
    /// **T16**: 走 `self.io.read_page_chunk()` 支持 StdFs / IoUring.
    async fn load_chunk_from_disk(&self, key: PageKey) -> io::Result<Vec<u8>> {
        self.io.read_page_chunk(&self.block_dir, key).await
    }

    /// chunk_list 缓存大小 (测试 helper).
    pub fn chunk_cache_len(&self) -> usize {
        self.chunk_list.len()
    }

    /// 暴露 meta_cache (测试 / 高级用法)
    pub fn meta(&mut self) -> &mut MetaCache {
        &mut self.meta
    }

    /// 暴露 nowchunks (测试 / 高级用法)
    pub fn nowchunks(&mut self) -> &mut NowChunks {
        &mut self.nowchunks
    }

    /// 暴露 chunk_list (测试 / 高级用法)
    pub fn chunk_list(&mut self) -> &mut ChunkList {
        &mut self.chunk_list
    }

    /// 暴露 pid_alloc (测试 / 高级用法)
    pub fn pid_alloc(&mut self) -> &mut PidAllocator {
        &mut self.pid_alloc
    }

    /// 暴露 vpid_alloc (测试 / 高级用法)
    pub fn vpid_alloc(&mut self) -> &mut VpidAllocator {
        &mut self.vpid_alloc
    }

    /// 暴露 writer (测试 / 高级用法)
    pub fn writer(&mut self) -> &mut ChunkWriter {
        &mut self.writer
    }

    /// ⭐ 申请 chunk_lock (DESIGN §3.0). 同步版本立即返回结果.
    ///
    /// 调用场景: Pager 内部 read 流程 / 测试验证 chunk_lock 行为.
    ///
    /// **逻辑**:
    /// 1. chunk_list.contains(chunk_key) → `AlreadyLoaded` (快路径)
    /// 2. 否则调 `chunk_lock.try_acquire(...)` 拿 owner / waiter 角色
    ///
    /// **同步版本**: 不会真触发 wait queue (因为没有 await), 但接口一致, T11
    /// 接 async 时 caller 只需把 `try_acquire` 替换为 `try_acquire.await` 即可.
    pub fn acquire_chunk_lock(&mut self, chunk_key: ChunkKey, task_id: TaskId) -> AcquireResult {
        let already_loaded = self.chunk_list.contains(&chunk_key);
        self.chunk_lock
            .try_acquire(chunk_key, task_id, already_loaded)
    }

    /// ⭐ Owner 完成 IO 后释放 chunk_lock, 唤醒下一个 waiter.
    ///
    /// **调用场景**:
    /// - 同步版本: Pager 加载 chunk 到 chunk_list 后立即调
    /// - 异步版本 (T11): owner 协程在 `io_ops::read.await` 完成后调
    ///
    /// **返回**: 下一个要唤醒的 waiter task_id (如果有).
    pub fn release_chunk_lock(
        &mut self,
        chunk_key: &ChunkKey,
        current_task: TaskId,
    ) -> Option<TaskId> {
        self.chunk_lock.release_and_wake(chunk_key, current_task)
    }

    /// 暴露 chunk_lock (测试 / 高级用法)
    pub fn chunk_lock(&mut self) -> &mut ChunkLockMap {
        &mut self.chunk_lock
    }

    /// 只读访问 chunk_lock (避免多次 &mut borrow 冲突).
    pub fn chunk_lock_view(&self) -> &ChunkLockMap {
        &self.chunk_lock
    }

    /// 拿 travel_tree 的 RAII guard (caller 用 `guard.tree().record(...)`).
    pub fn travel_tree_guard(&mut self, task_id: TaskId) -> TravelTreeGuard<'_> {
        TravelTreeGuard::new(task_id, self)
    }

    /// 当前 travel_trees 中 task 数 (测试 / 调试).
    pub fn travel_tree_count(&self) -> usize {
        self.travel_trees.len()
    }

    /// 检查指定 task 是否已注册 travel_tree.
    pub fn has_travel_tree(&mut self, task_id: TaskId) -> bool {
        self.travel_trees.contains_key(&task_id)
    }
}

// =====================================================================
// PageWriteBatch: 一次提交多 page 到 nowchunks
// =====================================================================

/// 批量写 page. caller 多次 `add`, 最后 `submit` 一次性把 page 拷到 nowchunks.
///
/// **不变量** (DESIGN §3.0.5):
/// - 单次 submit 在单线程内连续 memcpy, 中途不可 await
/// - 超过 16 page (256KB) 触发 panic
/// - 提交后 caller 拿到 `Vec<(vpid, pid)>` mappings, meta_cache.write 已在内部完成
pub struct PageWriteBatch {
    /// 待提交 (vpid, page_data)
    pages: Vec<(u64, Box<[u8; PAGE_SIZE]>)>,
}

/// PageWriteBatch 上限 16 page (256KB). 超过触发 panic (DESIGN §3.0.5).
const MAX_BATCH_PAGES: usize = 16;

impl PageWriteBatch {
    pub fn new() -> Self {
        Self { pages: Vec::new() }
    }

    /// 加一个 page 进 batch.
    pub fn add(&mut self, vpid: u64, data: Box<[u8; PAGE_SIZE]>) -> &mut Self {
        assert!(
            self.pages.len() < MAX_BATCH_PAGES,
            "PageWriteBatch 超过 16 page 上限 (256KB), 应 caller 分批"
        );
        self.pages.push((vpid, data));
        self
    }

    /// 当前 batch 中的 page 数.
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// ⭐ 提交: 把所有 page 一次性 memcpy 到 nowchunks + 分配 pid + 写回 meta_cache.
    ///
    /// **流程** (单线程连续, 不 await):
    /// 1. 遍历 pages, 调 `pid_alloc.alloc()` 拿 pid
    /// 2. chunk 满 (== 64 page) → rotate_to 下一 chunk (或新 file)
    /// 3. memcpy 到 nowchunks
    /// 4. meta_cache.write(vpid, pid) 标 dirty
    /// 5. 返回 mappings 给 caller (供 debug / 验证)
    ///
    /// **⭐ T12.14 特殊: META_VPID 走 META_PID 直接覆盖 (不 COW)**.
    /// MetaPage 设计上是固定位置 (chunk 0 page 0), 必须始终在 META_PID.
    /// 走通用 pid_alloc 会把 MetaPage COW 到一个新 page, 破坏固定位置约定
    /// (写盘后 disk page 0 仍是旧 MetaPage, 实际 MetaPage 在另一个 page).
    pub async fn submit(self, pager: &mut Pager) -> io::Result<Vec<(u64, PidLocation)>> {
        let mut mappings = Vec::with_capacity(self.pages.len());
        for (vpid, data) in self.pages {
            // 1. 分配 pid
            //    ⭐ META_VPID 例外: 始终用 META_PID (MetaPage 必须在固定位置)
            //
            //    ⭐⭐⭐ 关键设计: **复用 vs COW** (2026-07-26 改为直接问 nowchunks):
            //    - vpid 旧 pid 对应的 chunk **仍在 nowchunks** → **复用原 pid**
            //      原位覆盖同一 page_idx (增量写, 不浪费槽位)
            //    - 否则 (新 vpid / chunk 已 swap/flush 走) → **alloc 新 pid** 走纯 COW
            //
            //    **语义修正**: 旧代码用 meta.is_dirty(vpid) 判定, 但 swap 后 meta 仍
            //    dirty 而 chunk 已不在内存 (旧版靠 reinsert_clean 兜底)。去掉 reinsert
            //    后 dirty 不再等价"chunk 驻留", 必须直接问 nowchunks (meta 是 SoT)。
            let pid = if vpid == META_VPID {
                META_PID
            } else if let Some(old_pid) = pager.meta.read(vpid)
                && pager.nowchunks.peek_chunk(PageKey {
                    file_id: old_pid.file_id(),
                    chunk_idx: old_pid.chunk_idx(),
                }).is_some()
            {
                // 复用原 pid (chunk 还在内存, 原位覆盖同一 page_idx)
                old_pid
            } else {
                // COW: alloc 新 pid
                loop {
                    if let Some(p) = pager.pid_alloc.alloc() {
                        break p;
                    }
                    // chunk 满: rotate
                    let (file_id, chunk_idx, _) = pager.pid_alloc.current();
                    let (next_file, next_chunk) = if chunk_idx as usize + 1 >= CHUNKS_PER_BLOCK {
                        (file_id + 1, 0u8)
                    } else {
                        (file_id, chunk_idx + 1)
                    };
                    pager.pid_alloc.rotate_to(next_file, next_chunk);
                }
            };

            // 2. memcpy 到 nowchunks (user data 完整保留, 不构造 header)
            let key = PageKey {
                file_id: pid.file_id(),
                chunk_idx: pid.chunk_idx(),
            };
            let page_idx = pid.page_idx() as u8;
            // ⭐ 驻留兜底: chunk 不在 nowchunks 时加载完整视图, 避免全 0
            //    ChunkBuf 覆盖历史 page. 触发: reopen 首写 / 固定位置页 (MetaPage)
            //    所在 chunk 被 swap 后再写 / COW rotate 首写新 chunk.
            //
            //    ⭐ 加载源优先级必须与读路径一致 (最新优先):
            //    write_queue pending → completed → in_flight → chunk_list → disk.
            //    否则 swap 后立即重建会从 disk 读到 stale 视图, 而读路径
            //    nowchunks 优先 → 读到坏页 (catalog_many_dbs 回归抓到).
            if pager.nowchunks.peek_chunk(key).is_none() {
                let ck: crate::chunk_lru::ChunkKey = key.into();
                let bytes = if let Some(b) = pager.write_queue.peek_chunk_pending(key) {
                    b.to_vec()
                } else if let Some(b) = pager.write_queue.peek_chunk_completed(key) {
                    b.to_vec()
                } else if let Some(b) = pager.in_flight.get(&key) {
                    (**b).clone()
                } else if pager.chunk_list.contains(&ck) {
                    pager.chunk_list.peek(&ck).unwrap().to_vec()
                } else {
                    match pager.io.read_page_chunk(&pager.block_dir, key).await {
                        Ok(b) => b,
                        Err(_) => vec![0u8; CHUNK_SIZE],
                    }
                };
                pager.nowchunks.load_full_view(key, bytes);
            }
            pager
                .nowchunks
                .write_page_with_vpid(key, page_idx, vpid, *data);
            // 3. 写回 meta_cache (标 dirty, 持久化在 flush 时).
            pager.meta.write(vpid, pid);

            // 4. ⭐ 自动持久化: 检查 chunk 是否已满, 满则 swap 到 WriteQueue
            //    (背压超限时内部退化同步落盘)
            if pager.nowchunks.is_chunk_full(key) {
                pager.swap_full_chunk_to_write_queue(key).await?;
            }

            // 5. 收集 mapping
            mappings.push((vpid, pid));
        }

        // ⭐ 更新写计数
        pager.inc_write_count(mappings.len() as u64);

        Ok(mappings)
    }
}

impl Default for PageWriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

// =====================================================================
// TravelTree + TravelTreeGuard: B+Tree 传播一致性 (DESIGN §3.0.1)
// =====================================================================
//
// 这一版先做最小可用实现: 维护 task_id → TravelTree 的 HashMap,
// RAII guard 自动 register / unregister.
// split 广播更新逻辑在 T8 实施 (需要 page crate split 钩子).

/// task 私有的 travel tree: 记录 (separator_key, child_vpid) 用于 split 传播.
pub struct TravelTree {
    map: HashMap<Vec<u8>, u64>,
}

impl TravelTree {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// 记录 (key, vpid).
    pub fn record(&mut self, key: Vec<u8>, vpid: u64) {
        self.map.insert(key, vpid);
    }

    /// 用 key 查 vpid.
    pub fn lookup(&self, key: &[u8]) -> Option<u64> {
        self.map.get(key).copied()
    }

    /// 范围更新: 把 value == old_vpid 且 key 在 [lo, hi) 范围的条目更新为 new_vpid.
    /// 用于 split 传播: right page 的 vpid 替换 left page 的 vpid 在某些 key 上的 mapping.
    ///
    /// **半开区间**: `[lo, hi)`, lo 包含, hi 不包含. 与 B+Tree split 语义一致:
    /// - right_lo = right page 第一个 cp 段首 key (含, 应该被替换)
    /// - right_hi = 下一个 page 的最小 key (不含, 不应被替换)
    pub fn range_update(&mut self, lo: &[u8], hi: &[u8], old_vpid: u64, new_vpid: u64) {
        let updates: Vec<Vec<u8>> = self
            .map
            .iter()
            .filter(|(k, v)| **v == old_vpid && k.as_slice() >= lo && k.as_slice() < hi)
            .map(|(k, _)| k.clone())
            .collect();
        for k in updates {
            self.map.insert(k, new_vpid);
        }
    }

    /// 找所有 value == vpid 的 key.
    pub fn find_all_with_vpid(&self, vpid: u64) -> Vec<Vec<u8>> {
        self.map
            .iter()
            .filter(|(_, v)| **v == vpid)
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl Default for TravelTree {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard: drop 时自动 unregister. 防止手动 unregister 遗漏.
pub struct TravelTreeGuard<'p> {
    task_id: TaskId,
    pager: &'p mut Pager,
}

impl<'p> TravelTreeGuard<'p> {
    /// 创建 guard, 内部自动 register.
    pub fn new(task_id: TaskId, pager: &'p mut Pager) -> Self {
        pager.travel_trees.entry(task_id).or_default();
        Self { task_id, pager }
    }

    /// 拿到 task 的 travel_tree 的可变引用.
    pub fn tree(&mut self) -> &mut TravelTree {
        self.pager
            .travel_trees
            .get_mut(&self.task_id)
            .expect("travel_tree should be registered in guard::new")
    }
}

impl Drop for TravelTreeGuard<'_> {
    fn drop(&mut self) {
        // 自动 unregister
        self.pager.travel_trees.remove(&self.task_id);
    }
}

// =====================================================================
// helper
// =====================================================================

/// chunk_offset: 给定 chunk 在 .block file 内的字节偏移.
///
/// **重要**: 每个 .block 文件是独立的物理文件, offset 总是从 0 开始.
/// file_id 选择哪个 .block 文件, chunk_idx 决定文件内的 1MB 段偏移.
fn chunk_offset(key: PageKey) -> u64 {
    // ⭐ 修复 (2026-07-21): 每个 .block 文件独立 offset 空间.
    // 早期实现把 `file_id * BLOCK_SIZE + chunk_idx * CHUNK_SIZE` 当作全局 offset,
    // 错误: 写入 `000003.block` 时 offset 应该是 chunk 内的偏移 (chunk_idx * CHUNK_SIZE),
    // 不是全局 20MB+. 旧实现导致 sparse file 扩展到错误位置, 后续 page (page 14) 写到了
    // file 末尾, 但 scan 仍然按 0..PAGES_PER_CHUNK 读 page 0 (空) 就以为该 chunk 是空的.
    (key.chunk_idx as u64) * CHUNK_SIZE as u64
}

// 抑制 unused warning
#[allow(dead_code)]
fn _unused_chunk_key() -> ChunkKey {
    ChunkKey {
        db: DEFAULT_DB_ID,
        file_id: 0,
        chunk_idx: 0,
    }
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_write_batch_max_pages_limit() {
        let mut b = PageWriteBatch::new();
        for i in 0..MAX_BATCH_PAGES {
            b.add(i as u64, Box::new([0u8; PAGE_SIZE]));
        }
        assert_eq!(b.len(), MAX_BATCH_PAGES);

        // 第 17 个应 panic
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut b = PageWriteBatch::new();
            for i in 0..MAX_BATCH_PAGES + 1 {
                b.add(i as u64, Box::new([0u8; PAGE_SIZE]));
            }
        }));
        assert!(result.is_err(), "超过 16 page 应 panic");
    }

    #[test]
    fn travel_tree_record_and_lookup() {
        let mut t = TravelTree::new();
        t.record(b"key1".to_vec(), 100);
        t.record(b"key2".to_vec(), 200);
        assert_eq!(t.lookup(b"key1"), Some(100));
        assert_eq!(t.lookup(b"key2"), Some(200));
        assert_eq!(t.lookup(b"key3"), None);
    }

    #[test]
    fn travel_tree_record_overwrites() {
        let mut t = TravelTree::new();
        t.record(b"k".to_vec(), 1);
        t.record(b"k".to_vec(), 2);
        assert_eq!(t.lookup(b"k"), Some(2));
    }

    #[test]
    fn travel_tree_range_update() {
        let mut t = TravelTree::new();
        // 三条记录, value 都是 5 (old_vpid)
        t.record(b"a".to_vec(), 5);
        t.record(b"m".to_vec(), 5);
        t.record(b"z".to_vec(), 5);
        // 另一条 value 是 9, 不应被 range_update 影响
        t.record(b"b".to_vec(), 9);

        // range_update: [b, n) → 10, 但 b 的 value 是 9, 不应被更新
        t.range_update(b"b", b"n", 5, 10);

        assert_eq!(t.lookup(b"a"), Some(5), "a 不在 [b, n), 不变");
        assert_eq!(t.lookup(b"m"), Some(10), "m 在 [b, n), 应更新为 10");
        assert_eq!(t.lookup(b"z"), Some(5), "z 不在 [b, n), 不变");
        assert_eq!(t.lookup(b"b"), Some(9), "b 的 value 不是 5, 不变");
    }

    #[test]
    fn travel_tree_find_all_with_vpid() {
        let mut t = TravelTree::new();
        t.record(b"x".to_vec(), 7);
        t.record(b"y".to_vec(), 8);
        t.record(b"z".to_vec(), 7);
        let mut found = t.find_all_with_vpid(7);
        found.sort();
        assert_eq!(found, vec![b"x".to_vec(), b"z".to_vec()]);
    }

    #[test]
    fn travel_tree_guard_drops_unregister() {
        let tmp = tempfile::tempdir().unwrap();
        let mate = tmp.path().join("page.mate");
        std::fs::File::create(&mate).unwrap();
        let meta = MetaCache::open(&mate).unwrap();
        let block = tmp.path().join("000001.block");
        std::fs::File::create(&block)
            .unwrap()
            .set_len(10 * 1024 * 1024)
            .unwrap();
        let mut pager = Pager::new(
            tmp.path().to_path_buf(),
            meta,
            VpidAllocator::new(0),
            PidAllocator::new(0, 0, 0),
            ChunkList::new(8),
            NowChunks::new(),
            ChunkWriter::new(&block).unwrap(),
        );

        assert!(pager.travel_trees.is_empty(), "初始 travel_trees 应为空");

        // 创建 guard, 验证在 guard 持有时 travel_trees 有 1 个 entry
        {
            let mut guard = pager.travel_tree_guard(42);
            guard.tree().record(b"x".to_vec(), 100);
            assert_eq!(guard.tree().len(), 1);
        }
        // guard 离开作用域, drop 自动 unregister
        assert!(
            pager.travel_trees.is_empty(),
            "guard drop 后 travel_trees 应为空"
        );

        // 多次创建销毁 guard, 不应泄漏
        for i in 0..5u64 {
            {
                let mut _g = pager.travel_tree_guard(i);
                assert_eq!(_g.tree().len(), 0, "新 guard 的 tree 初始为空");
                _g.tree().record(b"k".to_vec(), i);
                assert_eq!(_g.tree().len(), 1);
            }
            // guard drop 后 travel_trees 应清空
            assert!(
                pager.travel_trees.is_empty(),
                "guard {} drop 后 travel_trees 应为空, got {}",
                i,
                pager.travel_trees.len()
            );
        }
        // 5 个 guard 都 drop
        assert!(
            pager.travel_trees.is_empty(),
            "5 个 guard drop 后 travel_trees 应为空, got {}",
            pager.travel_trees.len()
        );
    }

    #[test]
    fn chunk_offset_per_file() {
        // ⭐ 修复 (2026-07-21): 每个 .block 文件独立 offset 空间.
        // file 0, chunk 0 → offset 0 (file 0 内的 offset)
        assert_eq!(
            chunk_offset(PageKey {
                file_id: 0,
                chunk_idx: 0
            }),
            0
        );
        // file 0, chunk 5 → 5MB (file 0 内的 offset, 不是全局)
        assert_eq!(
            chunk_offset(PageKey {
                file_id: 0,
                chunk_idx: 5
            }),
            5 * CHUNK_SIZE as u64
        );
        // file 1, chunk 0 → 0 (file 1 内的 offset, file_id 决定哪个文件)
        assert_eq!(
            chunk_offset(PageKey {
                file_id: 1,
                chunk_idx: 0
            }),
            0
        );
        // file 1, chunk 3 → 3MB (file 1 内的 offset)
        assert_eq!(
            chunk_offset(PageKey {
                file_id: 1,
                chunk_idx: 3
            }),
            3 * CHUNK_SIZE as u64
        );
    }
}
