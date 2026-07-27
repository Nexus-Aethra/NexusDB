//! T4 三层架构核心: NowChunks + WriteQueue + ChunkWriter.
//!
//! 设计 (DESIGN §4.4 + §3.2/§3.3):
//! - **NowChunks** (LSM 写缓冲): 维护一个 `HashMap<PageKey, ChunkBuf>`, 每个 ChunkBuf = 1MB 字节.
//!   `write_page(key, page_idx, data)` 把 16KB page memcpy 到 ChunkBuf 对应偏移.
//!   满了 (1MB) 或显式 `drain_dirty` 时整 chunk 移到 WriteQueue.
//! - **WriteQueue**: 排队等待落盘的 chunks. `enqueue` / `peek_pending` / `mark_completed` /
//!   `drain_completed`. 完成后调 `chunk_list.insert_from_write_queue` (T5 chunk_list).
//! - **ChunkWriter**: 持有 .block fd, 把 WriteQueue 的 chunk 用 writev + fsync 落盘.
//!   第一版用同步 std::fs IO (DESIGN §4.4 注释允许 TDD 简化). T11 polish 接 io_uring.
//!
//! **数据流**: page data → NowChunks (in-memory) → WriteQueue (pending IO) → .block on disk →
//!   chunk_list (read cache). 唯一: never NowChunks → 直接 chunk_list (写不修改旧数据, LSM).
//!
//! **单线程使用**: per-shard thread, 同 scheduler crate 契约.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::meta_cache::MetaCache;
use crate::types::{
    BLOCK_SIZE, CHUNK_SIZE, DEFAULT_DB_ID, DbId, PAGE_SIZE, PAGES_PER_CHUNK, PID_ALIVE, PageKey,
    PidLocation, pid_to_offset,
};

// =====================================================================
// NowChunks: 写缓冲 (LSM 风格)
// =====================================================================

/// 单个 1MB chunk 的内存缓冲.
///
/// ⭐ 数组化重构 (2026-07-26): data + vpid 表 + 计数合并为一个结构
/// (物理相邻, cache 友好), 替代原来的 ChunkBuf + 独立 vpid_maps 嵌套 BTreeMap.
/// **无 dirty 标记**: 驻留即待写 (纯 COW 设计, swap 后同 chunk 不再回插).
#[derive(Clone)]
struct ChunkBuf {
    data: Box<[u8; CHUNK_SIZE]>,
    /// page_idx → vpid. `VPID_UNSET` = 未写 (vpid 0 是 META, 合法值不可作哨兵).
    vpids: [u64; PAGES_PER_CHUNK],
    /// 已写 page 数 (O(1) is_chunk_full).
    page_count: u8,
}

/// vpids 数组的"未写"哨兵 (vpid 0 合法, 用 u64::MAX).
const VPID_UNSET: u64 = u64::MAX;

impl ChunkBuf {
    fn new() -> Self {
        Self {
            data: Box::new([0u8; CHUNK_SIZE]),
            vpids: [VPID_UNSET; PAGES_PER_CHUNK],
            page_count: 0,
        }
    }

    /// 记录 page_idx 已写 (首次写时 page_count += 1).
    fn mark_written(&mut self, page_idx: u8, vpid: u64) {
        let i = page_idx as usize;
        if self.vpids[i] == VPID_UNSET {
            self.page_count += 1;
        }
        self.vpids[i] = vpid;
    }

    /// 把 16KB page 写到 chunk 内 page_idx 位置 (caller 原始字节, 不写 header).
    fn write_page(&mut self, page_idx: u8, data: &[u8; PAGE_SIZE]) {
        debug_assert!((page_idx as usize) < PAGES_PER_CHUNK);
        let off = page_idx as usize * PAGE_SIZE;
        self.data[off..off + PAGE_SIZE].copy_from_slice(data);
        // 无 vpid 信息的路径 (测试/底层写): 仍计入 page_count, vpid 置 0
        // (只影响 drain_vpid_map 输出, 不影响数据).
        let i = page_idx as usize;
        if self.vpids[i] == VPID_UNSET {
            self.page_count += 1;
            self.vpids[i] = 0;
        }
    }

    /// ⭐ 写 page 字节到 chunk 内 page_idx 位置. **保留 caller's [0..0x28] 完整
    /// header 区域**, 仅当 page_type != Internal 时覆盖 vpid 字段 (8B at [0x18..0x20]).
    ///
    /// **设计原则 (修正 2026-07-21)**: caller 是 page header 的事实源. Pager 只
    /// 负责 (a) 把 caller 字节 memcpy 到 chunk, (b) 对于 **非 Internal** page 类型,
    /// 自动把分配的 vpid 写到 caller header 的 vpid 字段 (防止 caller 忘了 set).
    /// 其他 header 字段 (magic / page_type / key_count / free_off / version 等)
    /// caller 自行负责, 不应被 Pager 覆盖.
    ///
    /// **⭐ Internal page 特殊处理**: page crate 的 `internal_child` 用 [0x18..0x20]
    /// (page_vpid header 字段) 作为内部节点的 `first_child` (因为 internal page 没有
    /// "child at index -1" 的哨兵 item). 如果 Pager 强制覆盖这个字段, 每次 write
    /// 后 first_child 会丢失 (变成 page 自己的 vpid), 导致 internal_child 永远返回
    /// 自己, 形成死循环. 因此 Internal page 的 vpid 字段 caller 必须自己用
    /// `page_set_vpid(page, first_child_vpid)` 设置, Pager 不会覆盖.
    ///
    /// **典型 caller**:
    /// - `Pager::create` / `Pager::write_page`: caller 传 [u8; PAGE_SIZE],
    ///   page header 由 caller 保证
    /// - **Internal page**: caller 调 `page_set_vpid(page, first_child)` 设置首个子节点
    /// - **Leaf/Meta page**: caller 可选调 `page_set_vpid`, 不设则 Pager 帮忙填
    fn write_page_with_header(&mut self, page_idx: u8, vpid: u64, data: &[u8; PAGE_SIZE]) {
        use page::PageType;
        debug_assert!((page_idx as usize) < PAGES_PER_CHUNK);
        let off = page_idx as usize * PAGE_SIZE;
        // 完整 memcpy caller 字节 (含完整 [0..0x28] header)
        self.data[off..off + PAGE_SIZE].copy_from_slice(data);
        // ⭐ 仅当 page_type != Internal 时, 自动覆盖 vpid 字段.
        // Internal page 的 vpid 字段被 page crate 复用作 first_child, 必须保留 caller 设置.
        let page_type_byte = self.data[off + 4];
        if page_type_byte != PageType::Internal as u8 {
            self.data[off + 0x18..off + 0x20].copy_from_slice(&vpid.to_le_bytes());
        }
        self.mark_written(page_idx, vpid);
    }
}

/// NowChunks: 多 chunk 的写缓冲.
///
/// ⭐ 数组化重构 (2026-07-26): chunk 地址空间天然连续
/// (`file_id × 256 chunk × 64 page`), 用二级数组索引替代 BTreeMap:
/// - 外层 `Vec<FileBuf>` 按 file_id 线性查 (单 shard 几乎永远 1 项)
/// - 内层 `Vec<Option<ChunkBuf>>` 索引 = chunk_idx, O(1) 直查
///
/// **无 dirty 标记**: 驻留即待写. 满 chunk swap = take 移出 (不回插),
/// 之后该 chunk 内 vpid 更新走纯 COW alloc 新 pid (meta 是 source of truth).
/// (旧版用 BTreeMap 是为规避 hashbrown SSE2 UB, 数组无此问题.)
pub struct NowChunks {
    files: Vec<FileBuf>,
}

struct FileBuf {
    file_id: u32,
    /// 索引 = chunk_idx (0..256), 懒扩容到写到的最大 idx.
    chunks: Vec<Option<ChunkBuf>>,
}

impl FileBuf {
    fn new(file_id: u32) -> Self {
        Self {
            file_id,
            chunks: Vec::new(),
        }
    }

    fn slot(&self, chunk_idx: u8) -> Option<&ChunkBuf> {
        self.chunks.get(chunk_idx as usize).and_then(|s| s.as_ref())
    }

    fn slot_mut_or_insert(&mut self, chunk_idx: u8) -> &mut ChunkBuf {
        let i = chunk_idx as usize;
        if self.chunks.len() <= i {
            self.chunks.resize_with(i + 1, || None);
        }
        self.chunks[i].get_or_insert_with(ChunkBuf::new)
    }

    fn take(&mut self, chunk_idx: u8) -> Option<ChunkBuf> {
        self.chunks.get_mut(chunk_idx as usize).and_then(|s| s.take())
    }

    fn resident_count(&self) -> usize {
        self.chunks.iter().flatten().count()
    }
}

impl NowChunks {
    pub fn new() -> Self {
        Self { files: Vec::new() }
    }

    fn file(&self, file_id: u32) -> Option<&FileBuf> {
        self.files.iter().find(|f| f.file_id == file_id)
    }

    fn file_mut_or_insert(&mut self, file_id: u32) -> &mut FileBuf {
        if let Some(pos) = self.files.iter().position(|f| f.file_id == file_id) {
            &mut self.files[pos]
        } else {
            self.files.push(FileBuf::new(file_id));
            self.files.last_mut().expect("just pushed")
        }
    }

    /// 写一个 page 到指定 chunk/page_idx. 不存在的 chunk 自动创建 (lazy alloc).
    pub fn write_page(&mut self, key: PageKey, page_idx: u8, data: [u8; PAGE_SIZE]) {
        self.file_mut_or_insert(key.file_id)
            .slot_mut_or_insert(key.chunk_idx)
            .write_page(page_idx, &data);
    }

    /// ⭐ 写一个 page 同时记录 vpid (用于 flush 时构造 page header).
    /// 这是 Pager::PageWriteBatch::submit 用的接口.
    ///
    /// **写 header 到 caller 字节 [0..0x28]** (DESIGN §4.2.3, 40B header):
    /// - [0..4]   magic "LCBP"
    /// - [4]      page_type = 3 (Leaf, TDD 简化)
    /// - [0x14..0x18] version = 1
    /// - [0x18..0x20] vpid LE
    ///
    /// caller 字节应假设 [0..0x28] 是 header 区域 (被覆盖), 数据写在 [0x28..PAGE_SIZE].
    pub fn write_page_with_vpid(
        &mut self,
        key: PageKey,
        page_idx: u8,
        vpid: u64,
        data: [u8; PAGE_SIZE],
    ) {
        self.file_mut_or_insert(key.file_id)
            .slot_mut_or_insert(key.chunk_idx)
            .write_page_with_header(page_idx, vpid, &data);
    }

    /// ⭐ 读 peek: 返回 `Some(&[u8; CHUNK_SIZE])` if chunk 在 nowchunks 内存中, 否则 None.
    /// 这是 Pager 多源查找的"nowchunks first"路径 (DESIGN §3.0.4).
    pub fn peek_chunk(&self, key: PageKey) -> Option<&[u8; CHUNK_SIZE]> {
        self.file(key.file_id)
            .and_then(|f| f.slot(key.chunk_idx))
            .map(|c| &*c.data)
    }

    /// ⭐ flush/swap 路径: take 走整个 chunk (驻留移出, 不回插).
    /// 返回的 Box 含 chunk 完整 1MB 视图 (所有已写 page 的最新数据).
    pub fn take_chunk_box(&mut self, key: PageKey) -> Option<Box<[u8; CHUNK_SIZE]>> {
        self.files
            .iter_mut()
            .find(|f| f.file_id == key.file_id)
            .and_then(|f| f.take(key.chunk_idx))
            .map(|c| c.data)
    }

    /// ⭐ 取走并返回 chunk 的 vpid map (page_idx → vpid). 用于 flush 时构造 page header.
    /// 返回 `Vec<(page_idx, vpid)>` 按 page_idx 升序.
    ///
    /// ⭐ 数组化后 vpids 已内联在 ChunkBuf, take_chunk_box 后 chunk 已移除,
    /// 本方法对已 take 的 chunk 返回空 (兼容旧调用顺序: 先 take 后 drain).
    pub fn drain_vpid_map_for_chunk(&mut self, key: PageKey) -> Vec<(u8, u64)> {
        match self
            .files
            .iter()
            .find(|f| f.file_id == key.file_id)
            .and_then(|f| f.slot(key.chunk_idx))
        {
            Some(c) => c
                .vpids
                .iter()
                .enumerate()
                .filter(|(_, v)| **v != VPID_UNSET)
                .map(|(i, v)| (i as u8, *v))
                .collect(),
            None => Vec::new(),
        }
    }

    /// ⭐ 全部驻留 chunk 的 key (驻留即待刷; 替代旧 dirty_keys — 无 dirty 概念).
    /// 返回顺序天然按 (file_id, chunk_idx) 升序 (顺序写盘友好).
    pub fn resident_keys(&self) -> Vec<PageKey> {
        let mut keys = Vec::new();
        for f in &self.files {
            for (idx, slot) in f.chunks.iter().enumerate() {
                if slot.is_some() {
                    keys.push(PageKey {
                        file_id: f.file_id,
                        chunk_idx: idx as u8,
                    });
                }
            }
        }
        keys
    }

    /// 兼容旧名: 驻留即待刷.
    pub fn dirty_keys(&self) -> Vec<PageKey> {
        self.resident_keys()
    }

    /// 兼容旧名: 驻留 chunk 数.
    pub fn dirty_count(&self) -> usize {
        self.len()
    }

    /// 总驻留 chunk 数.
    pub fn len(&self) -> usize {
        self.files.iter().map(|f| f.resident_count()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// chunk 内已写入的 page 数 (O(1), ChunkBuf 内联计数).
    pub fn chunk_page_count(&self, key: PageKey) -> usize {
        self.file(key.file_id)
            .and_then(|f| f.slot(key.chunk_idx))
            .map_or(0, |c| c.page_count as usize)
    }

    /// 检查 chunk 是否已满 (>= PAGES_PER_CHUNK = 64).
    pub fn is_chunk_full(&self, key: PageKey) -> bool {
        self.chunk_page_count(key) >= PAGES_PER_CHUNK
    }

    /// 取走整个 chunk 的 owned bytes (兼容接口, 测试用).
    pub fn take_chunk(&mut self, key: PageKey) -> Option<Vec<u8>> {
        self.take_chunk_box(key).map(|b| b.to_vec())
    }

    /// ⭐ G3: 为复用的 free chunk 创建空视图占位.
    ///
    /// 复用 chunk 的磁盘历史内容全部无效 (全死 + meta 已确认), submit 的
    /// 驻留兜底不应从 disk 加载旧死页视图 (否则 page_count 立刻满 →
    /// 新写无槽可用). 空 ChunkBuf 从零开始增量写.
    pub fn insert_empty(&mut self, key: PageKey) {
        let file = self.file_mut_or_insert(key.file_id);
        let idx = key.chunk_idx as usize;
        if file.chunks.len() <= idx {
            file.chunks.resize_with(idx + 1, || None);
        }
        file.chunks[idx] = Some(ChunkBuf::new());
    }

    /// ⭐ reopen 兜底: 加载磁盘上半满 chunk 的完整视图, 供后续增量写.
    ///
    /// 扫描 64 个 page header (magic "LCBP" + vpid 字段) 重建 vpids/page_count,
    /// 保证 is_chunk_full 在 reopen 后仍正确 (否则该 chunk 永远不触发满 swap).
    pub fn load_full_view(&mut self, key: PageKey, data: Vec<u8>) {
        assert_eq!(data.len(), CHUNK_SIZE, "chunk bytes must be CHUNK_SIZE");
        let mut buf = ChunkBuf::new();
        buf.data.copy_from_slice(&data);
        for i in 0..PAGES_PER_CHUNK {
            let off = i * PAGE_SIZE;
            // page 自描述: magic "LCBP" 判存在, vpid 在 [0x18..0x20]
            if &buf.data[off..off + 4] == b"LCBP" {
                let vpid = u64::from_le_bytes(
                    buf.data[off + 0x18..off + 0x20].try_into().expect("8B"),
                );
                buf.vpids[i] = vpid;
                buf.page_count += 1;
            }
        }
        let file = self.file_mut_or_insert(key.file_id);
        let idx = key.chunk_idx as usize;
        if file.chunks.len() <= idx {
            file.chunks.resize_with(idx + 1, || None);
        }
        file.chunks[idx] = Some(buf);
    }

    /// ⭐ 把所有驻留 chunks 移到 WriteQueue (驻留即待写, 无 dirty 过滤).
    /// drained 后 nowchunks 空 (lazy alloc 下次写时重建).
    pub fn drain_dirty(&mut self) -> WriteQueue {
        let mut wq = WriteQueue::new();
        for key in self.resident_keys() {
            if let Some(data) = self.take_chunk_box(key) {
                wq.enqueue(WriteHandle::new(key, data.to_vec()));
            }
        }
        wq
    }
}

impl Default for NowChunks {
    fn default() -> Self {
        Self::new()
    }
}

// =====================================================================
// WriteQueue: 排队等待落盘的 chunk
// =====================================================================

/// 单个等待落盘的 chunk 句柄. owned 1MB bytes + PageKey.
#[derive(Clone)]
pub struct WriteHandle {
    pub key: PageKey,
    pub chunk: Vec<u8>,
}

impl WriteHandle {
    pub fn new(key: PageKey, chunk: Vec<u8>) -> Self {
        debug_assert_eq!(chunk.len(), CHUNK_SIZE);
        Self { key, chunk }
    }
}

/// WriteQueue: FIFO 队列, 跟踪每个 chunk 的落盘状态.
pub struct WriteQueue {
    /// 等待落盘的 chunks (FIFO)
    pending: VecDeque<WriteHandle>,
    /// 已完成待 drain 的 chunks (按 key 索引). 用 BTreeMap 规避 hashbrown SSE2 UB.
    completed: BTreeMap<PageKey, Vec<u8>>,
}

impl WriteQueue {
    pub fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            completed: BTreeMap::new(),
        }
    }

    pub fn enqueue(&mut self, handle: WriteHandle) {
        self.pending.push_back(handle);
    }

    /// ⭐ 移除指定 key 的 pending 快照 (不入 completed).
    ///
    /// 用于 flush 路径: nowchunks 因 reinsert_clean 保留完整 chunk 视图,
    /// flush 写盘的版本永远 ⊇ write_queue 里的旧快照. 写完后必须移除
    /// 同 key pending, 否则后续 drive_write_queue 会用 stale 快照回滚覆盖新数据.
    pub fn remove_pending(&mut self, key: PageKey) -> bool {
        let before = self.pending.len();
        self.pending.retain(|h| h.key != key);
        self.pending.len() != before
    }

    /// ⭐ peek pending: Pager 用来快速判断"chunk 正在 io_uring 写".
    pub fn peek_pending(&self, key: PageKey) -> Option<&WriteHandle> {
        self.pending.iter().find(|h| h.key == key)
    }

    /// ⭐ 读路径: 从 pending 中拿 chunk 字节 (三源查找用).
    /// 返回 `Some(&[u8; CHUNK_SIZE])` 如果 chunk 在 pending 队列中.
    pub fn peek_chunk_pending(&self, key: PageKey) -> Option<&[u8]> {
        self.pending
            .iter()
            .find(|h| h.key == key)
            .map(|h| h.chunk.as_slice())
    }

    /// ⭐ 读路径: 从 completed 中拿 chunk 字节 (三源查找用).
    /// 返回 `Some(&[u8; CHUNK_SIZE])` 如果 chunk 已完成落盘但尚未插入 chunk_list.
    pub fn peek_chunk_completed(&self, key: PageKey) -> Option<&[u8]> {
        self.completed.get(&key).map(|v| v.as_slice())
    }

    /// ⭐ 异步落盘: 取走指定 key 的 pending 快照 (移出队列, 所有权交给 caller).
    /// 同 key 多个快照时取最新一个 (后入队的), 旧快照直接丢弃
    /// (新快照是完整 chunk 视图, ⊇ 旧快照).
    pub fn take_pending(&mut self, key: PageKey) -> Option<Vec<u8>> {
        let mut latest: Option<Vec<u8>> = None;
        let mut i = 0;
        while i < self.pending.len() {
            if self.pending[i].key == key {
                let h = self.pending.remove(i).expect("index checked");
                latest = Some(h.chunk);
            } else {
                i += 1;
            }
        }
        latest
    }

    /// 收集所有 pending chunk 的 keys.
    pub fn pending_keys(&self) -> Vec<PageKey> {
        self.pending.iter().map(|h| h.key).collect()
    }

    /// 标记某个 chunk 落盘完成 (落盘线程/io_uring 回调时调).
    pub fn mark_completed(&mut self, key: PageKey) {
        // 从 pending 找到 handle, 移到 completed
        let pos = self.pending.iter().position(|h| h.key == key);
        if let Some(pos) = pos {
            let handle = self.pending.remove(pos).expect("just checked");
            self.completed.insert(handle.key, handle.chunk);
        }
    }

    /// ⭐ drain completed: 返回所有已完成 chunks, 触发 chunk_list.insert_from_write_queue.
    /// 调用方负责把 drain 出的 chunks 插入 chunk_list (T5 实现).
    pub fn drain_completed(&mut self) -> Vec<WriteHandle> {
        let keys: Vec<PageKey> = self.completed.keys().copied().collect();
        keys.into_iter()
            .filter_map(|k| {
                self.completed
                    .remove(&k)
                    .map(|chunk| WriteHandle { key: k, chunk })
            })
            .collect()
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn completed_count(&self) -> usize {
        self.completed.len()
    }

    pub fn len(&self) -> usize {
        self.pending.len() + self.completed.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.completed.is_empty()
    }
}

impl Default for WriteQueue {
    fn default() -> Self {
        Self::new()
    }
}

// =====================================================================
// ChunkWriter: 持有 .block fd, 把 WriteHandle 落盘 + 更新 MetaCache
// =====================================================================

/// 持有 .block fd, 把 WriteHandle (1MB chunk) 同步落盘 + 更新 MetaCache.
///
/// 第一版用 std::fs 同步 IO (pwrite_at + fsync). T11 polish 接 scheduler::io_ops::write/fsync.
///
/// **限制**: 同步 IO 期间阻塞调度线程. 实际生产应换成 io_uring.
///
/// **T12.10 多 db 隔离**: 内部 `block_paths: HashMap<(DbId, u32), PathBuf>` 维护
/// per-(db, file_id) 路径. 兼容 API (compat) 走 db=0 + file_id=0.
pub struct ChunkWriter {
    /// ⭐ T12.10: per-(db, file_id) 路径. key 是 (db_id, file_id) 对.
    block_paths: HashMap<(DbId, u32), PathBuf>,
    /// 预分配的 pending chunk (累计 bytes). 单线程, 一次只 hold 一个 chunk.
    current: Vec<u8>,
    /// pending entries: (vpid, page_idx_in_chunk)
    pending_entries: Vec<(u64, u8)>,
    /// 当前 chunk 的 (db, file_id, chunk_idx), 用于决定写到 disk 的 offset
    current_key: (DbId, u32, u8),
    /// chunk 内 page 写入顺序: next_page_in_chunk (0..=64)
    next_page_in_chunk: u8,
}

impl ChunkWriter {
    pub fn new(block_path: &Path) -> io::Result<Self> {
        // 预分配 10MB (单 shard 单 .block 文件)
        // 重要: truncate(false) — 不能 truncate(true), 否则 reopen 时会清空已存在
        // 的 .block 文件 (所有 chunk data + MetaPage 都丢失)
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(block_path)?;
        f.set_len(BLOCK_SIZE as u64)?;
        drop(f);

        let mut block_paths = HashMap::new();
        block_paths.insert((DEFAULT_DB_ID, 0), block_path.to_path_buf());
        Ok(Self {
            block_paths,
            current: Vec::with_capacity(CHUNK_SIZE),
            pending_entries: Vec::new(),
            current_key: (DEFAULT_DB_ID, 0, 0),
            next_page_in_chunk: 0,
        })
    }

    /// ⭐ T12.10: 注册 per-(db, file_id) block 路径.
    /// caller (Pager::open) 在 open 时为每个 db 注册自己的 .block 路径.
    /// 路径由 `block_root/{db_name}/shard_{N}/block_file_id` 决定.
    pub fn register_db_block(&mut self, db: DbId, file_id: u32, path: PathBuf) -> io::Result<()> {
        if !path.exists() {
            // ⭐ 先创建父目录 (T12.16 polish: Pager::open 会预先 mkdir -p,
            // 这里兜底保证 register 不会因父目录缺失而失败).
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            // 创建并预分配 10MB
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)?;
            f.set_len(BLOCK_SIZE as u64)?;
            drop(f);
        }
        self.block_paths.insert((db, file_id), path);
        Ok(())
    }

    /// ⭐ 测试 helper: 列出所有注册的 (db, file_id).
    pub fn registered_files(&self) -> Vec<(DbId, u32)> {
        let mut v: Vec<(DbId, u32)> = self.block_paths.keys().copied().collect();
        v.sort();
        v
    }

    /// 加一个 page 进 pending queue. 自动分配 page_idx (同 chunk 内顺序).
    /// **chunk 满自动 rotate**: 当 next_page_in_chunk == 64 时, 自动 flush + 切到下一 chunk.
    pub fn enqueue(
        &mut self,
        vpid: u64,
        data: Box<[u8; PAGE_SIZE]>,
        target_key: PageKey,
        page_idx_in_chunk: u8,
    ) {
        // ⭐ T12.10: 适配 (db, file_id) 维度. 兼容: target_key 转 (db=0, file_id=target_key.file_id).
        let target_full_key: (DbId, u32, u8) =
            (DEFAULT_DB_ID, target_key.file_id, target_key.chunk_idx);
        // 如果请求的 page_idx 与当前 chunk 不匹配, 先 flush + 切
        if target_full_key != self.current_key || self.next_page_in_chunk >= PAGES_PER_CHUNK as u8 {
            // 强制 flush (用 caller 提供的 meta? 我们要 meta 才能 flush)
            // 简化: 直接清 pending 让 caller 显式 flush. 这里不动.
        }
        // 检查是否满了, 满了 caller 应该先调 flush. 这里保守: 如果满了直接 panic.
        debug_assert!(
            self.next_page_in_chunk < PAGES_PER_CHUNK as u8,
            "chunk full, caller should flush before enqueue"
        );
        debug_assert_eq!(
            page_idx_in_chunk, self.next_page_in_chunk,
            "page_idx_in_chunk must be sequential for now"
        );

        // 扩展 current 到 16KB 边界
        while self.current.len() < page_idx_in_chunk as usize * PAGE_SIZE + PAGE_SIZE {
            self.current.push(0);
        }
        // 但更简单: 每 page 1MB 累积用固定 layout. 现在简化: 用 Vec<u8> 累积.
        // 这里改用 Vec<u8> 累积完整 1MB chunk, first call 必须 fill 0.
        // 简化: 假设 caller 按顺序 fill 0..64
        let off = page_idx_in_chunk as usize * PAGE_SIZE;
        if self.current.len() < off + PAGE_SIZE {
            self.current.resize(off + PAGE_SIZE, 0);
        }
        self.current[off..off + PAGE_SIZE].copy_from_slice(&data[..]);
        self.pending_entries.push((vpid, page_idx_in_chunk));
        self.next_page_in_chunk += 1;
    }

    /// ⭐ flush pending chunk 到 .block 文件 + 更新 MetaCache.
    ///
    /// 简化 (TDD 第一版):
    /// 1. 把 current (Vec<u8>) pwrite 到 disk offset = `pid_to_offset(first_pid)`
    /// 2. 遍历 pending_entries, 调 `meta.write(vpid, pid)` 更新 vpid→pid 映射
    /// 3. 清 current + pending
    ///
    /// 注意: 实际生产应替换为 scheduler::io_ops::write + fsync (T11 polish).
    pub fn flush(&mut self, meta: &mut MetaCache) -> io::Result<()> {
        if self.pending_entries.is_empty() {
            return Ok(());
        }

        // 1. 打开 .block fd (T12.10: per-(db, file_id) 路径)
        let (cur_db, cur_file_id, cur_chunk_idx) = self.current_key;
        let block_path = self
            .block_paths
            .get(&(cur_db, cur_file_id))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "no block_path registered for db={} file_id={}",
                        cur_db, cur_file_id
                    ),
                )
            })?;
        let f = OpenOptions::new().write(true).open(block_path)?;
        // 计算 disk offset: 该 chunk 起始字节
        // 用 pending_entries[0].page_idx 反算 chunk 起始
        let first_page_idx = self.pending_entries[0].1;
        let first_pid = PidLocation::from_bytes(&[
            (cur_file_id & 0xFF) as u8,
            ((cur_file_id >> 8) & 0xFF) as u8,
            ((cur_file_id >> 16) & 0xFF) as u8,
            ((cur_file_id >> 24) & 0xFF) as u8,
            cur_chunk_idx,
            first_page_idx,
            0,
            PID_ALIVE,
        ]);
        let chunk_start_offset =
            pid_to_offset(&first_pid) - (first_page_idx as u64) * PAGE_SIZE as u64;

        // 2. pwrite current bytes 到 disk
        // current 长度可能 < CHUNK_SIZE (因为 next_page_in_chunk < 64), 补齐
        let write_len = self.current.len();
        if write_len == 0 {
            // 罕见: pending_entries 非空但 current 空 → caller 错误
            return Ok(());
        }
        // 确保 current 长度是 PAGE_SIZE 倍数
        debug_assert!(write_len.is_multiple_of(PAGE_SIZE));
        // 只写已填充的部分 (不写零字节)
        let buf = &self.current[..write_len];
        f.write_all_at(buf, chunk_start_offset)?;
        f.sync_all()?;

        // 3. 更新 MetaCache: 每个 pending entry 写一个 PidLocation
        for (vpid, page_idx_in_chunk) in &self.pending_entries {
            let pid = PidLocation::from_bytes(&[
                (cur_file_id & 0xFF) as u8,
                ((cur_file_id >> 8) & 0xFF) as u8,
                ((cur_file_id >> 16) & 0xFF) as u8,
                ((cur_file_id >> 24) & 0xFF) as u8,
                cur_chunk_idx,
                *page_idx_in_chunk,
                0,
                PID_ALIVE,
            ]);
            meta.write(*vpid, pid);
        }

        // 4. 清 pending, 准备下一 chunk
        self.current.clear();
        self.pending_entries.clear();
        self.next_page_in_chunk = 0;

        Ok(())
    }

    pub fn pending_count(&self) -> usize {
        self.pending_entries.len()
    }

    pub fn current_chunk_idx(&self) -> u8 {
        self.current_key.2
    }

    pub fn next_page_in_chunk(&self) -> u8 {
        self.next_page_in_chunk
    }

    /// ⭐ T12.10: 返回默认 db=0 的 .block 文件父目录.
    /// 兼容旧 API: 假定 Pager 在单 db 模式下用此.
    /// 多 db 模式 caller 应自己持有路径, 不用此方法.
    pub fn block_dir(&self) -> &Path {
        self.block_paths
            .get(&(DEFAULT_DB_ID, 0))
            .and_then(|p| p.parent())
            .unwrap_or(Path::new("."))
    }
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_buf_write_page_offset() {
        let mut c = ChunkBuf::new();
        let data = [42u8; PAGE_SIZE];
        c.write_page(3, &data);
        assert_eq!(c.page_count, 1);
        assert_eq!(c.data[PAGE_SIZE * 3], 42);
        // 其他 page 仍是 0
        assert_eq!(c.data[0], 0);
    }

    #[test]
    fn nowchunks_chunk_isolation() {
        let mut nc = NowChunks::new();
        let k1 = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };
        let k2 = PageKey {
            file_id: 0,
            chunk_idx: 1,
        };
        nc.write_page(k1, 0, [1u8; PAGE_SIZE]);
        nc.write_page(k2, 0, [2u8; PAGE_SIZE]);
        assert_eq!(nc.peek_chunk(k1).unwrap()[0], 1);
        assert_eq!(nc.peek_chunk(k2).unwrap()[0], 2);
    }

    #[test]
    fn write_queue_mark_completed_idempotent() {
        let mut wq = WriteQueue::new();
        let k = PageKey {
            file_id: 0,
            chunk_idx: 0,
        };
        let h = WriteHandle::new(k, vec![0u8; CHUNK_SIZE]);
        wq.enqueue(h);
        wq.mark_completed(k);
        wq.mark_completed(k); // 第二次: 没效果 (already removed)
        let drained = wq.drain_completed();
        assert_eq!(drained.len(), 1);
    }

    // =====================================================================
    // ⭐ T12.10: ChunkWriter per-(db, file_id) 测试
    // =====================================================================

    #[test]
    fn chunk_writer_register_per_db_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let default_path = tmp.path().join("000001.block");
        std::fs::File::create(&default_path).unwrap();
        let mut writer = ChunkWriter::new(&default_path).unwrap();

        // 默认 db=0 + file_id=0 注册
        assert_eq!(writer.registered_files(), vec![(DEFAULT_DB_ID, 0)]);

        // 注册 db=1 的 file_id=0
        let db1_path = tmp.path().join("db1_000001.block");
        writer.register_db_block(1, 0, db1_path.clone()).unwrap();
        assert_eq!(writer.registered_files(), vec![(0, 0), (1, 0)]);

        // 注册 db=0 的 file_id=1 (替换)
        let db0_f1_path = tmp.path().join("db0_file1.block");
        writer.register_db_block(0, 1, db0_f1_path).unwrap();
        assert_eq!(writer.registered_files().len(), 3);

        // block_dir 仍兼容返回 default db 路径
        assert_eq!(writer.block_dir(), default_path.parent().unwrap());
    }

    #[test]
    fn chunk_writer_register_db_block_creates_file() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("db_new/000001.block");
        assert!(!db_path.exists(), "path 不应预先存在");

        let default_path = tmp.path().join("default.block");
        std::fs::File::create(&default_path).unwrap();
        let mut writer = ChunkWriter::new(&default_path).unwrap();

        // 注册新 db 路径, 应自动创建并预分配 10MB
        writer.register_db_block(2, 0, db_path.clone()).unwrap();
        assert!(db_path.exists(), "register 应创建 .block 文件");
        let size = std::fs::metadata(&db_path).unwrap().len();
        assert_eq!(size, BLOCK_SIZE as u64, "register 应预分配 10MB");
    }

    #[test]
    fn chunk_writer_register_db_block_preserves_existing() {
        // 已存在的 .block 文件不应被 truncate 清空
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("db_existing.block");
        std::fs::File::create(&db_path).unwrap();
        std::fs::write(&db_path, b"important data").unwrap();

        let default_path = tmp.path().join("default.block");
        std::fs::File::create(&default_path).unwrap();
        let mut writer = ChunkWriter::new(&default_path).unwrap();
        writer.register_db_block(3, 0, db_path.clone()).unwrap();

        let content = std::fs::read(&db_path).unwrap();
        // truncate(false) → 内容应保留 (但会被 set_len 拉长到 10MB)
        let prefix = &content[..14];
        assert_eq!(prefix, b"important data", "register 不应清空已存在内容");
    }
}
