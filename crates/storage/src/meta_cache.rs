//! MetaCache v3: 全量平坦数组 + dirty window (T-meta-async).
//!
//! 设计 (2026-07-27, 取代 v2 LFU):
//! - **全量缓存**: `Vec<PidLocation>` 按 vpid 直接索引 (meta:data = 1:2048,
//!   1TB 数据 → 512MB/shard-db, 平坦数组无 HashMap 3-4× 膨胀).
//!   open 时整读 page.mate, read 纯内存索引, **不再有 pread miss 路径**.
//! - **dirty window**: 1MB/window = 128K slot, 写按 **8KB 子区** (128 子区/窗)
//!   标脏 (O2, 2026-10-20): 快照/写/确认的粒度从整窗收窄到连续脏子区,
//!   热窗单槽重写的落盘放大从 1MB 降到 ≤8KB.
//! - **异步刷盘支持**: `take_dirty_subrange_snapshots` 取连续脏子区快照
//!   (清 dirty + 标 in-flight 去重), 由 Pager/协程走 io_uring 批量写 + 单次 fsync;
//!   `complete_subrange_flush` 收割 (失败重标 dirty 下轮重试).
//! - `flush_dirty` 保留为**同步全量路径** (flush()/close 契约):
//!   逐脏子区 `write_at` + 单次 `sync_all`. **前提: 无 in-flight 快照**
//!   (caller 先 drain 异步 backlog, 否则旧快照可能乱序覆盖新数据).
//!
//! **vpid 单空间**: 持久化 off = vpid×8 从 v1 起就不区分 db (Pager 已按
//! db 目录物理隔离, 每 db 独立 mate 文件), v3 的 db-aware API 仅保留签名
//! (db 参数忽略), 内存语义与持久化语义一致.
//!
//! **磁盘格式零变化**: 子区只是窗口 1MB 区间内的偏移段 (write_at),
//! 与整窗重写字节级兼容.
//!
//! **多线程契约**: 所有方法 `&mut self`/`&self`, 单线程使用.

use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::file_at::FileAt;
use crate::meta_log;
use crate::types::{DbId, META_PID, PidLocation};

/// dirty window 大小: 1MB.
pub const META_WINDOW_SIZE: usize = 1 << 20;
/// 每 window 的 slot 数: 1MB / 8B = 128K.
pub const SLOTS_PER_WINDOW: usize = META_WINDOW_SIZE / 8;
/// ⭐ O2: 脏区跟踪的子区大小: 8KB = 1024 槽, 128 子区/窗.
pub const SUB_SLOTS: usize = 1024;
/// 子区字节大小 (8KB) — 落盘偏移与写长单位.
pub const SUB_SIZE: usize = SUB_SLOTS * 8;
/// 每 window 的子区数.
pub const SUBRANGES_PER_WINDOW: usize = SLOTS_PER_WINDOW / SUB_SLOTS;
/// ⭐ meta log: 单批序列化上限 — 把 admission 同步 stall 钉在 ~32K 条
/// (≈640KB 编码); pending ≥ 该阈值时不等间隔立即续批 (高写入率下分批追平).
pub const META_LOG_BATCH_ENTRIES: usize = 32_768;

/// 未分配 slot 哨兵 (flags=0 = 未分配, 与磁盘全零 slot 一致).
const ZERO_PID: PidLocation = PidLocation {
    file_id: 0,
    chunk_idx: 0,
    page_idx: 0,
    flags: 0,
};

// ===== O2: 子区脏位图 (128 位 = 2×u64 per window) =====

fn bit_get(bits: &[u64; 2], i: usize) -> bool {
    (bits[i / 64] >> (i % 64)) & 1 == 1
}
fn bit_set(bits: &mut [u64; 2], i: usize) {
    bits[i / 64] |= 1 << (i % 64);
}
fn bit_clear(bits: &mut [u64; 2], i: usize) {
    bits[i / 64] &= !(1 << (i % 64));
}
fn bit_range_set(bits: &mut [u64; 2], start: usize, len: usize) {
    for i in start..start + len {
        bit_set(bits, i);
    }
}
fn bit_range_clear(bits: &mut [u64; 2], start: usize, len: usize) {
    for i in start..start + len {
        bit_clear(bits, i);
    }
}
fn bit_popcount(bits: &[u64; 2]) -> usize {
    (bits[0].count_ones() + bits[1].count_ones()) as usize
}

// ===== meta log: 1D 位图 (pending 去重, 1 bit / vpid) =====

fn bit64_get(bits: &[u64], i: usize) -> bool {
    bits.get(i / 64).is_some_and(|w| (w >> (i % 64)) & 1 == 1)
}
fn bit64_set(bits: &mut [u64], i: usize) {
    if let Some(w) = bits.get_mut(i / 64) {
        *w |= 1 << (i % 64);
    }
}
fn bit64_clear(bits: &mut [u64], i: usize) {
    if let Some(w) = bits.get_mut(i / 64) {
        *w &= !(1 << (i % 64));
    }
}

/// ⭐ O2: 一次子区快照条目 — window 内 [sub_start, sub_start+sub_len) 连续脏子区
/// (单条最大 128 子区 = 整窗, 与整窗语义等价).
#[derive(Debug)]
pub struct MetaWindowItem {
    pub window: u32,
    pub sub_start: u16,
    pub sub_len: u16,
    pub bytes: Vec<u8>,
}

impl MetaWindowItem {
    /// page.mate 内的写入偏移 (byte).
    pub fn offset(&self) -> u64 {
        self.window as u64 * META_WINDOW_SIZE as u64 + self.sub_start as u64 * SUB_SIZE as u64
    }
}

// =====================================================================
// ⭐ meta log (实验): 批次 / 完成回执 / 状态
// =====================================================================

/// 日志写入状态 (Some = log 模式; page.mate 只由合并/同步 flush 写).
pub struct MetaLogState {
    dir: PathBuf,
    /// 下一个新段 seq (单调递增).
    next_seq: u64,
    /// 磁盘上未合并的段 (seq, path), 升序.
    segments: Vec<(u64, PathBuf)>,
    /// 未合并段总字节数 (触发合并阈值).
    unmerged_bytes: u64,
    /// 当前追加段 seq (None = 下次追加开新段).
    cur_seq: Option<u64>,
    /// 当前追加段写偏移.
    cur_off: u64,
    /// 在飞 append 批次数 (0/1).
    in_flight: u32,
    /// 在飞合并 (含 mate 写 + 删段) 标志.
    merge_in_flight: bool,
}

impl MetaLogState {
    fn fresh(dir: PathBuf, next_seq: u64) -> Self {
        Self {
            dir,
            next_seq,
            segments: Vec::new(),
            unmerged_bytes: 0,
            cur_seq: None,
            cur_off: 0,
            in_flight: 0,
            merge_in_flight: false,
        }
    }
}

/// 一批待追加的日志记录 (owned, 交给写盘协程).
pub struct MetaLogBatch {
    pub seq: u64,
    pub path: PathBuf,
    pub offset: u64,
    pub bytes: Vec<u8>,
    /// 去重后的 vpid 列表 (失败重排队用).
    pub entries: Vec<u64>,
    pub new_segment: bool,
    pub prev_seq: Option<u64>,
    pub prev_off: u64,
    pub bytes_len: u64,
}

impl MetaLogBatch {
    pub fn done_info(&self) -> MetaLogDone {
        MetaLogDone {
            seq: self.seq,
            path: self.path.clone(),
            bytes_len: self.bytes_len,
            entries: self.entries.clone(),
            new_segment: self.new_segment,
            prev_seq: self.prev_seq,
            prev_off: self.prev_off,
        }
    }
}

/// append 完成回执.
pub struct MetaLogDone {
    pub seq: u64,
    pub path: PathBuf,
    pub bytes_len: u64,
    pub entries: Vec<u64>,
    pub new_segment: bool,
    pub prev_seq: Option<u64>,
    pub prev_off: u64,
}

/// 合并作业: (可选) 先把 pending 追加进 log, 再把脏子区写进 mate, 成功后删段.
pub struct MetaMergeBatch {
    pub log: Option<MetaLogBatch>,
    pub items: Vec<MetaWindowItem>,
    pub segments: Vec<PathBuf>,
    pub mate_path: PathBuf,
}

impl MetaMergeBatch {
    pub fn done_info(&self) -> MetaMergeDone {
        MetaMergeDone {
            log: self.log.as_ref().map(|b| b.done_info()),
            items: self
                .items
                .iter()
                .map(|it| (it.window, it.sub_start, it.sub_len))
                .collect(),
            segments: self.segments.clone(),
        }
    }
}

/// 合并完成回执.
pub struct MetaMergeDone {
    pub log: Option<MetaLogDone>,
    pub items: Vec<(u32, u16, u16)>,
    pub segments: Vec<PathBuf>,
}

// =====================================================================
// MetaCache v3
// =====================================================================

/// MetaCache v3: 全量平坦数组 + page.mate 持久化.
///
/// **公共 API 与 v2 兼容**: `read(vpid)` / `write(vpid, pid)` / `flush_dirty()` /
/// `contains` / `dirty_count` / `len`; db-aware 变体保留签名 (db 忽略).
///
/// - read 纯内存索引 (越界 / flags==0 → None)
/// - write 懒扩容 + 标对应 1MB window 的 8KB 子区 dirty
/// - 无容量上限: 内存由 vpid 水位天然决定
pub struct MetaCache {
    /// 平坦 slot 数组, 索引 = vpid. 懒扩容 (write 越界时 resize).
    slots: Vec<PidLocation>,
    /// 脏区标记, 索引 = (vpid / SLOTS_PER_WINDOW), 每窗 128 位
    /// (第 i 位 = 子区 i). 与 slots 同步扩容.
    dirty_sub: Vec<[u64; 2]>,
    /// 快照已入队未确认 (去重: 防两个协程乱序写同 offset).
    in_flight_sub: Vec<[u64; 2]>,
    /// page.mate fd (同步 flush 路径用).
    mate_file: File,
    /// ⭐ meta log: 未 append 的 vpid 队列 (值仍从 slots 读, 只存 vpid).
    pending: VecDeque<u64>,
    /// ⭐ meta log: pending 去重位 (1 bit/vpid) — 热 key 在缓冲内只保留一条.
    pending_bits: Vec<u64>,
    /// ⭐ meta log 状态 (None = 原地写模式, 与 v3 行为完全一致).
    log: Option<MetaLogState>,
}

impl MetaCache {
    /// ⭐ open: 整读 page.mate 进平坦数组 (全量缓存).
    ///
    /// 全零 slot 保留在数组里 (read 按 `flags()==0` 返回 None).
    /// 同目录若存在 `page.metlog.*`, 一律先重放 (enable_log=false 时重放后
    /// 立即合并进 mate 并删段 — 功能关闭/只读工具的数据安全路径).
    pub fn open(mate_path: &Path) -> io::Result<Self> {
        Self::open_with_log(mate_path, false)
    }

    /// ⭐ open + meta log 开关.
    pub fn open_with_log(mate_path: &Path, enable_log: bool) -> io::Result<Self> {
        // 重要: truncate(false) — 保留已存在 page.mate 内容
        let mut mate_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(mate_path)?;

        let mut bytes = Vec::new();
        mate_file.read_to_end(&mut bytes)?;
        let slot_count = bytes.len() / 8; // 尾部非 8B 残片忽略 (正常不出现)
        let mut slots = Vec::with_capacity(slot_count);
        for i in 0..slot_count {
            let chunk: &[u8; 8] = bytes[i * 8..i * 8 + 8].try_into().expect("8B slot");
            slots.push(PidLocation::from_bytes(chunk));
        }
        let window_count = slot_count.div_ceil(SLOTS_PER_WINDOW);

        let mut cache = Self {
            slots,
            dirty_sub: vec![[0u64; 2]; window_count],
            in_flight_sub: vec![[0u64; 2]; window_count],
            mate_file,
            pending: VecDeque::new(),
            pending_bits: Vec::new(),
            log: None,
        };

        // ⭐ meta log: 重放未合并尾部到内存 (幂等: 后写覆盖先写).
        let dir = mate_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let info = meta_log::replay_into(&dir, |vpid, pid| {
            let idx = vpid as usize;
            cache.ensure_capacity(idx + 1);
            cache.slots[idx] = pid;
            let sub = (idx % SLOTS_PER_WINDOW) / SUB_SLOTS;
            bit_set(&mut cache.dirty_sub[idx / SLOTS_PER_WINDOW], sub);
        })?;

        if info.segments.is_empty() {
            if enable_log {
                cache.log = Some(MetaLogState::fresh(dir, 1));
            }
        } else if enable_log {
            cache.log = Some(MetaLogState {
                next_seq: info.max_seq + 1,
                segments: info.segments,
                unmerged_bytes: info.total_bytes,
                ..MetaLogState::fresh(dir, 1)
            });
        } else {
            // 功能关闭/只读兼容: 已有日志尾巴 → 立即合并进 mate 再删段,
            // 保持 "mate 是权威" 的旧契约 (否则旧路径永远看不到尾部).
            cache.sync_write_dirty_runs()?;
            let paths: Vec<PathBuf> = info.segments.iter().map(|(_, p)| p.clone()).collect();
            meta_log::delete_segments(&paths);
        }
        Ok(cache)
    }

    /// ⭐ vpid-only API: 读 vpid → PidLocation. 纯内存索引.
    /// 越界 / 未分配 (flags=0) → None.
    pub fn read(&mut self, vpid: u64) -> Option<PidLocation> {
        self.peek(vpid)
    }

    /// 不可变读 (G1: ChunkLiveness::rebuild_from_meta 等遍历场景用).
    /// 墓碑 slot (PID_FREED, 溢出页已释放) 同样返回 None — 页已死.
    pub fn peek(&self, vpid: u64) -> Option<PidLocation> {
        let slot = self.slots.get(vpid as usize)?;
        if slot.flags() == 0 || slot.flags() & crate::types::PID_FREED != 0 {
            None
        } else {
            Some(*slot)
        }
    }

    /// ⭐ 大 value: slot 是否有过记录 (含墓碑). recover 扫描回填判据 —
    /// 有记录 (活或墓碑) 的 vpid 以 meta 为准, 扫描不得覆盖;
    /// 否则已释放溢出页会被磁盘残留 header "复活" (存储泄漏).
    pub fn has_record(&self, vpid: u64) -> bool {
        self.slots
            .get(vpid as usize)
            .is_some_and(|s| s.flags() != 0)
    }

    /// ⭐ 大 value: 释放 vpid — 写墓碑 (保留原位置字节, flags 置 PID_FREED).
    /// 持久化随 dirty window 走; read/peek 此后返回 None.
    /// no-op 条件: 越界 / 从未分配 / 已是墓碑.
    pub fn free_slot(&mut self, vpid: u64) {
        let idx = vpid as usize;
        let Some(slot) = self.slots.get(idx) else {
            return;
        };
        if slot.flags() == 0 || slot.flags() & crate::types::PID_FREED != 0 {
            return;
        }
        let tomb = PidLocation {
            file_id: slot.file_id,
            chunk_idx: slot.chunk_idx,
            page_idx: slot.page_idx,
            flags: crate::types::PID_FREED,
        };
        self.slots[idx] = tomb;
        if self.log.is_some() {
            if !bit64_get(&self.pending_bits, idx) {
                bit64_set(&mut self.pending_bits, idx);
                self.pending.push_back(vpid);
            }
        } else {
            let sub = (idx % SLOTS_PER_WINDOW) / SUB_SLOTS;
            bit_set(&mut self.dirty_sub[idx / SLOTS_PER_WINDOW], sub);
        }
    }

    /// ⭐ G2: 遍历全部已分配 slot (vpid, pid). compact 判活等全扫场景用
    /// (meta 是存活性的 source of truth, 不依赖 page header 自描述).
    pub fn iter_allocated(&self) -> impl Iterator<Item = (u64, PidLocation)> + '_ {
        self.slots
            .iter()
            .enumerate()
            .filter(|(_, s)| s.flags() != 0)
            .map(|(i, s)| (i as u64, *s))
    }

    /// ⭐ vpid-only API: 写 vpid → PidLocation. 懒扩容 + 标脏.
    ///
    /// log 模式: 只进 pending 缓冲 (按 vpid 去重), 由 `take_log_batch` 序列化
    /// 追加; 原地模式: 标 8KB 子区 dirty (v3 行为).
    pub fn write(&mut self, vpid: u64, pid: PidLocation) {
        let idx = vpid as usize;
        self.ensure_capacity(idx + 1);
        self.slots[idx] = pid;
        if self.log.is_some() {
            if !bit64_get(&self.pending_bits, idx) {
                bit64_set(&mut self.pending_bits, idx);
                self.pending.push_back(vpid);
            }
        } else {
            let sub = (idx % SLOTS_PER_WINDOW) / SUB_SLOTS;
            bit_set(&mut self.dirty_sub[idx / SLOTS_PER_WINDOW], sub);
        }
    }

    /// ⭐ db-aware 签名兼容 (db 忽略, vpid 单空间与持久化一致).
    pub fn read_db(&mut self, _db: DbId, vpid: u64) -> Option<PidLocation> {
        self.read(vpid)
    }

    /// ⭐ db-aware 签名兼容 (db 忽略).
    pub fn write_db(&mut self, _db: DbId, vpid: u64, pid: PidLocation) {
        self.write(vpid, pid);
    }

    /// vpid 是否已分配 (flags != 0). v3 语义: "已分配", 不再是"在 cache 中"
    /// (全量缓存下两者等价于 v2 的持久化视角).
    pub fn contains(&self, vpid: u64) -> bool {
        self.slots
            .get(vpid as usize)
            .map(|s| s.flags() != 0)
            .unwrap_or(false)
    }

    /// db-aware 签名兼容 (db 忽略).
    pub fn contains_db(&self, _db: DbId, vpid: u64) -> bool {
        self.contains(vpid)
    }

    /// ⭐ 同步全量刷盘 (flush()/close 契约: 返回即持久).
    ///
    /// 逐脏子区 `write_at` (O2: 8KB 粒度) + 单次 `sync_all`.
    /// **前提**: 无 in-flight 快照 (caller 先 drain 异步 backlog);
    /// 否则协程稍后写入的旧快照会乱序覆盖这里的新数据.
    pub fn flush_dirty(&mut self) -> io::Result<()> {
        if self.log.is_some() {
            return self.flush_dirty_log();
        }
        // ⭐ 审计 P2 (2026-10-17): 前置条件从 debug_assert 升级为**运行时硬错误**.
        // 旧实现只在 debug 下断言: release 里若真有 in-flight 快照 (异步刷盘
        // 协程还握着旧快照), 这里写的"新数据"会被协程稍后写出的旧快照**乱序
        // 覆盖** → page.mate 留下陈旧映射 (指向已释放/已复用的 chunk), 重启后
        // 读到错页. 宁可让 flush 失败 (调用方可见), 也不静默损坏.
        if self.in_flight_sub.iter().any(|b| b[0] | b[1] != 0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "flush_dirty requires no in-flight meta snapshots (drain the async backlog first)",
            ));
        }
        let mut wrote_any = false;
        for (w, sub_start, sub_len) in self.dirty_runs() {
            let bytes = self.window_bytes_range(w, sub_start, sub_len);
            let offset = w as u64 * META_WINDOW_SIZE as u64 + sub_start as u64 * SUB_SIZE as u64;
            self.mate_file.write_at(&bytes, offset)?;
            bit_range_clear(&mut self.dirty_sub[w], sub_start, sub_len);
            wrote_any = true;
        }
        if wrote_any {
            self.mate_file.sync_all()?;
        }
        Ok(())
    }

    // ---------------------------------------------------------------
    // ⭐ 异步刷盘支持 (Phase M3: Pager/manager 接线用)
    // ---------------------------------------------------------------

    /// 取 dirty ∧ ¬in-flight 的连续子区快照: copy 字节 (末窗截断到实际水位),
    /// 清 dirty、标 in-flight (去重: 同子区有快照在飞时跳过, 下轮再取).
    pub fn take_dirty_subrange_snapshots(&mut self) -> Vec<MetaWindowItem> {
        let mut out = Vec::new();
        for (w, sub_start, sub_len) in self.dirty_runs() {
            let bytes = self.window_bytes_range(w, sub_start, sub_len);
            bit_range_clear(&mut self.dirty_sub[w], sub_start, sub_len);
            bit_range_set(&mut self.in_flight_sub[w], sub_start, sub_len);
            out.push(MetaWindowItem {
                window: w as u32,
                sub_start: sub_start as u16,
                sub_len: sub_len as u16,
                bytes,
            });
        }
        out
    }

    /// 收割子区快照写盘结果: Ok 清 in-flight; Err 清 in-flight + 重标
    /// dirty (下轮重试, 内存 slot 是最新值, 重取快照 ⊇ 失败快照).
    pub fn complete_subrange_flush(&mut self, window: u32, sub_start: u16, sub_len: u16, ok: bool) {
        let w = window as usize;
        if w >= self.in_flight_sub.len() {
            return; // 防御: 未知 window
        }
        let (start, len) = (sub_start as usize, sub_len as usize);
        bit_range_clear(&mut self.in_flight_sub[w], start, len);
        if !ok {
            bit_range_set(&mut self.dirty_sub[w], start, len);
        }
    }

    /// 是否还有未确认落盘的 meta (dirty ∪ in-flight 非空).
    ///
    /// log 模式: pending 缓冲 + 在飞 append/merge = 未持久; 已 append 进日志
    /// 但未合并进 mate 的条目**算持久** (崩溃后重放日志可恢复).
    pub fn has_unflushed(&self) -> bool {
        if let Some(log) = &self.log {
            return !self.pending.is_empty() || log.in_flight > 0 || log.merge_in_flight;
        }
        self.dirty_sub
            .iter()
            .chain(self.in_flight_sub.iter())
            .any(|b| b[0] | b[1] != 0)
    }

    /// in-flight 快照子区总数 (Pager backlog 判空用).
    pub fn in_flight_subrange_count(&self) -> usize {
        self.in_flight_sub.iter().map(bit_popcount).sum()
    }

    // ---------------------------------------------------------------
    // ⭐ meta log (实验): pending 缓冲 / append / 合并
    // ---------------------------------------------------------------

    /// 是否运行在 log 模式 (热路径只追加日志, page.mate 由合并更新).
    pub fn log_enabled(&self) -> bool {
        self.log.is_some()
    }

    /// 是否有在飞 append/merge (admission 去重 + flush 前置条件).
    pub fn log_busy(&self) -> bool {
        self.log
            .as_ref()
            .is_some_and(|l| l.in_flight > 0 || l.merge_in_flight)
    }

    /// pending 缓冲中的 vpid 数 (去重后).
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// 未合并日志段总字节数 (合并阈值).
    pub fn log_unmerged_bytes(&self) -> u64 {
        self.log.as_ref().map_or(0, |l| l.unmerged_bytes)
    }

    /// 在飞日志作业数 (append 或 merge, 0/1).
    pub fn log_inflight_count(&self) -> usize {
        self.log
            .as_ref()
            .is_some_and(|l| l.in_flight > 0 || l.merge_in_flight) as usize
    }

    /// 取一批 pending → 日志记录 (按 vpid 去重后的当前值), 分配段偏移.
    ///
    /// 同时给这批 vpid 所在子区标 dirty — 供后续 `take_merge_batch` 生成
    /// mate 快照. 返回 None: 无 pending / 已有在飞作业.
    pub fn take_log_batch(&mut self) -> Option<MetaLogBatch> {
        {
            let log = self.log.as_ref()?;
            if log.in_flight > 0 || log.merge_in_flight || self.pending.is_empty() {
                return None;
            }
        }
        let mut entries: Vec<u64> = Vec::new();
        let mut popped = 0usize;
        while popped < META_LOG_BATCH_ENTRIES {
            let Some(vpid) = self.pending.pop_front() else {
                break;
            };
            popped += 1;
            let idx = vpid as usize;
            if !bit64_get(&self.pending_bits, idx) {
                continue;
            }
            bit64_clear(&mut self.pending_bits, idx);
            if idx < self.slots.len() {
                entries.push(vpid);
            }
        }
        if entries.is_empty() {
            return None;
        }
        let mut bytes = Vec::with_capacity(
            entries.len() * meta_log::RECORD_SIZE + meta_log::HEADER_SIZE as usize,
        );
        for vpid in &entries {
            let idx = *vpid as usize;
            meta_log::encode_record(&mut bytes, *vpid, self.slots[idx]);
            let sub = (idx % SLOTS_PER_WINDOW) / SUB_SLOTS;
            bit_set(&mut self.dirty_sub[idx / SLOTS_PER_WINDOW], sub);
        }
        let log = self.log.as_mut().expect("log checked above");
        let rec_len = bytes.len() as u64;
        let (seq, new_segment, offset) = match log.cur_seq {
            Some(s) if log.cur_off + rec_len <= meta_log::SEGMENT_SIZE => (s, false, log.cur_off),
            _ => (log.next_seq, true, 0),
        };
        let path = meta_log::segment_path(&log.dir, seq);
        let prev_seq = log.cur_seq;
        let prev_off = log.cur_off;
        if new_segment {
            let mut buf = Vec::with_capacity(meta_log::HEADER_SIZE as usize + bytes.len());
            buf.extend_from_slice(&meta_log::header_bytes());
            buf.extend_from_slice(&bytes);
            bytes = buf;
            log.next_seq = seq + 1;
            log.segments.push((seq, path.clone()));
            log.cur_seq = Some(seq);
            log.cur_off = bytes.len() as u64;
            log.unmerged_bytes += bytes.len() as u64;
        } else {
            log.cur_off += rec_len;
            log.unmerged_bytes += rec_len;
        }
        log.in_flight = 1;
        let bytes_len = bytes.len() as u64;
        Some(MetaLogBatch {
            seq,
            path,
            offset,
            bytes,
            entries,
            new_segment,
            prev_seq,
            prev_off,
            bytes_len,
        })
    }

    /// 收割 append 结果. 失败: 回滚段位置/截断半写段 + 条目重排队.
    /// 返回 pending 是否已空 (供 "meta 全部持久" 判定).
    pub fn complete_log_append(&mut self, done: MetaLogDone, ok: bool) -> bool {
        {
            let Some(log) = self.log.as_mut() else {
                return false;
            };
            log.in_flight = 0;
            if !ok {
                log.cur_seq = done.prev_seq;
                log.cur_off = done.prev_off;
                log.unmerged_bytes = log.unmerged_bytes.saturating_sub(done.bytes_len);
                if done.new_segment {
                    log.segments.retain(|(s, _)| *s != done.seq);
                    let _ = std::fs::remove_file(&done.path);
                    // 注意: seq 不回退 — 路径绝不复用, 避免 PagerIo fd 缓存
                    // 持有已删除 inode 的 fd 导致后续写入"写进空洞".
                } else {
                    // 半写尾部必须截断: 否则下次追加后重放会把残留字节当记录.
                    let _ = meta_log::truncate_sync(&done.path, done.prev_off);
                }
            }
        }
        if !ok {
            for vpid in done.entries {
                let idx = vpid as usize;
                if !bit64_get(&self.pending_bits, idx) {
                    bit64_set(&mut self.pending_bits, idx);
                    self.pending.push_back(vpid);
                }
            }
        }
        self.pending.is_empty()
    }

    /// 取合并作业: (可选) 先把 pending 追加进日志, 再快照全部脏子区写 mate,
    /// 成功后删除全部已合并段. 返回 None: 无内容 / 已有在飞作业.
    ///
    /// **不变式**: 快照在调用线程同步完成; 此刻内存值 = "已 append 的日志前缀
    /// + 本批 pending" (本批先 append 后写 mate), 新写入不会进入本批快照.
    pub fn take_merge_batch(&mut self, mate_path: PathBuf) -> Option<MetaMergeBatch> {
        {
            let log = self.log.as_ref()?;
            if log.in_flight > 0 || log.merge_in_flight {
                return None;
            }
            if self.pending.is_empty() && self.dirty_count() == 0 {
                return None;
            }
        }
        let log_batch = self.take_log_batch();
        if self.dirty_count() == 0 {
            // 防御: 没有可合并内容 — 已取的批退回 pending (按失败回滚).
            if let Some(b) = log_batch {
                let done = b.done_info();
                self.complete_log_append(done, false);
            }
            return None;
        }
        let items = self.take_dirty_subrange_snapshots();
        if items.is_empty() {
            if let Some(b) = log_batch {
                let done = b.done_info();
                self.complete_log_append(done, false);
            }
            return None;
        }
        let segments: Vec<PathBuf> = self
            .log
            .as_ref()?
            .segments
            .iter()
            .map(|(_, p)| p.clone())
            .collect();
        if let Some(l) = self.log.as_mut() {
            l.merge_in_flight = true;
        }
        Some(MetaMergeBatch {
            log: log_batch,
            items,
            segments,
            mate_path,
        })
    }

    /// 收割合并结果. 成功 (log+mate 均 ok): 删段 + 重置日志; 失败保留段待重试.
    pub fn complete_merge(&mut self, done: MetaMergeDone, log_ok: bool, mate_ok: bool) {
        if let Some(ld) = done.log {
            self.complete_log_append(ld, log_ok);
        }
        for (w, s, l) in done.items {
            self.complete_subrange_flush(w, s, l, mate_ok);
        }
        if let Some(log) = self.log.as_mut() {
            log.merge_in_flight = false;
            if mate_ok && log_ok {
                meta_log::delete_segments(&done.segments);
                log.segments.clear();
                log.unmerged_bytes = 0;
                log.cur_seq = None;
                log.cur_off = 0;
            }
        }
    }

    /// log 模式的同步全量 flush (flush()/close 契约: 返回即 mate 权威):
    /// pending → 日志 (fsync) → 脏子区 → mate (fsync) → 删段.
    fn flush_dirty_log(&mut self) -> io::Result<()> {
        if self.log_busy() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "flush_dirty requires no in-flight meta log append/merge",
            ));
        }
        if let Some(b) = self.take_log_batch() {
            let r = meta_log::append_sync(&b.path, b.offset, &b.bytes);
            let done = b.done_info();
            let ok = r.is_ok();
            self.complete_log_append(done, ok);
            r?;
        }
        self.sync_write_dirty_runs()?;
        if let Some(log) = self.log.as_mut() {
            let paths: Vec<PathBuf> = log.segments.iter().map(|(_, p)| p.clone()).collect();
            meta_log::delete_segments(&paths);
            log.segments.clear();
            log.unmerged_bytes = 0;
            log.cur_seq = None;
            log.cur_off = 0;
        }
        Ok(())
    }

    /// 同步把全部脏子区写进 page.mate 并 fsync; 成功后才清 dirty 位
    /// (失败保留, 下轮重试; 部分写 + 重放幂等无害).
    fn sync_write_dirty_runs(&mut self) -> io::Result<()> {
        let runs = self.dirty_runs();
        let mut written: Vec<(usize, usize, usize)> = Vec::with_capacity(runs.len());
        for (w, sub_start, sub_len) in runs {
            let bytes = self.window_bytes_range(w, sub_start, sub_len);
            let offset = w as u64 * META_WINDOW_SIZE as u64 + sub_start as u64 * SUB_SIZE as u64;
            self.mate_file.write_at(&bytes, offset)?;
            written.push((w, sub_start, sub_len));
        }
        if !written.is_empty() {
            self.mate_file.sync_all()?;
        }
        for (w, s, l) in written {
            bit_range_clear(&mut self.dirty_sub[w], s, l);
        }
        Ok(())
    }

    // ---------------------------------------------------------------
    // 查询 helpers
    // ---------------------------------------------------------------

    /// slot 数组水位 (= 最大写过的 vpid + 1, 含未分配空洞).
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// 剩余脏子区数 (take 后 > 0 ⟺ 有子区因 in-flight 被延后).
    pub fn dirty_count(&self) -> usize {
        self.dirty_sub.iter().map(bit_popcount).sum()
    }

    /// ⭐ T12.6 stub: 写默认 db 的 MetaPage pid.
    pub fn write_meta_page_default(&mut self) {
        self.write(META_VPID_DEFAULT, META_PID);
    }

    // ---------------------------------------------------------------
    // 内部 helpers
    // ---------------------------------------------------------------

    /// 连续脏子区段扫描: (window, sub_start, sub_len), dirty ∧ ¬in-flight,
    /// 升序 (flush_dirty / take 共用判据).
    fn dirty_runs(&self) -> Vec<(usize, usize, usize)> {
        let mut runs = Vec::new();
        for w in 0..self.dirty_sub.len() {
            let mut i = 0usize;
            while i < SUBRANGES_PER_WINDOW {
                if !bit_get(&self.dirty_sub[w], i) || bit_get(&self.in_flight_sub[w], i) {
                    i += 1;
                    continue;
                }
                let start = i;
                while i < SUBRANGES_PER_WINDOW
                    && bit_get(&self.dirty_sub[w], i)
                    && !bit_get(&self.in_flight_sub[w], i)
                {
                    i += 1;
                }
                runs.push((w, start, i - start));
            }
        }
        runs
    }

    /// 懒扩容 slots 到至少 n 个, 并同步 window 标记数组.
    fn ensure_capacity(&mut self, n: usize) {
        if n > self.slots.len() {
            self.slots.resize(n, ZERO_PID);
        }
        let windows = self.slots.len().div_ceil(SLOTS_PER_WINDOW);
        if windows > self.dirty_sub.len() {
            self.dirty_sub.resize(windows, [0u64; 2]);
            self.in_flight_sub.resize(windows, [0u64; 2]);
        }
        let words = self.slots.len().div_ceil(64);
        if words > self.pending_bits.len() {
            self.pending_bits.resize(words, 0);
        }
    }

    /// 序列化第 w 个 window 内 [sub_start, sub_start+sub_len) 子区的字节
    /// (末窗截断到 slots 实际水位).
    ///
    /// ⭐ O1: LE 目标上整段 memcpy — `PidLocation` 是 `repr(C, packed)` 8B 无
    /// padding, `to_bytes()` = 逐字段 `to_le_bytes` 拼接, 故 LE 内存布局即磁盘
    /// 字节序; 128K 槽的逐槽 append 循环 (~3.3ms/窗) 退化为一次 memcpy.
    /// BE 目标保留标量回退, 交叉验证由
    /// `window_snapshot_is_memcpy_fast_and_byte_exact` 钉死.
    fn window_bytes_range(&self, w: usize, sub_start: usize, sub_len: usize) -> Vec<u8> {
        let start = w * SLOTS_PER_WINDOW + sub_start * SUB_SLOTS;
        let end = (start + sub_len * SUB_SLOTS).min(self.slots.len());
        if end <= start {
            return Vec::new();
        }
        let mut bytes = Vec::with_capacity((end - start) * 8);
        #[cfg(target_endian = "little")]
        {
            let src = &self.slots[start..end];
            // SAFETY: src 是连续排布的 [PidLocation; N], repr(C, packed) 且
            // 大小恰 8B (编译期断言), 无填充字节; 以 u8 视图读 N*8 字节全部
            // 落在该 Vec 分配内, 无对齐/越界/未初始化读.
            bytes.extend_from_slice(unsafe {
                std::slice::from_raw_parts(src.as_ptr().cast::<u8>(), src.len() * 8)
            });
        }
        #[cfg(not(target_endian = "little"))]
        {
            for slot in &self.slots[start..end] {
                bytes.extend_from_slice(&slot.to_bytes());
            }
        }
        bytes
    }
}

/// 默认 vpid 0 (单 db 兼容).
const META_VPID_DEFAULT: u64 = 0;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PID_ALIVE;

    fn pid(b: u8) -> PidLocation {
        PidLocation::from_bytes(&[b, 0, 0, 0, 0, 0, 0, PID_ALIVE])
    }

    #[test]
    fn basic_open_close() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("page.mate");
        let _cache = MetaCache::open(&path).unwrap();
    }

    #[test]
    fn read_unallocated_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("page.mate");
        std::fs::File::create(&path).unwrap();
        let mut cache = MetaCache::open(&path).unwrap();
        assert!(cache.read(0).is_none());
        assert!(cache.read(1000).is_none());
    }

    /// 全零 slot read 返回 None (v3: 数组保留全零槽, flags==0 过滤).
    #[test]
    fn zero_slot_reads_none() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("page.mate");
        use std::io::Write;
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&[0u8; 16]).unwrap();
        f.sync_all().unwrap();
        drop(f);

        let mut cache = MetaCache::open(&path).unwrap();
        assert_eq!(cache.len(), 2, "全零 slot 计入水位");
        assert!(cache.read(0).is_none());
        assert!(cache.read(1).is_none());
        assert!(!cache.contains(0));
    }

    /// take → complete(Ok) 状态机: dirty 清空, in-flight 清空.
    #[test]
    fn snapshot_take_complete_ok() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("page.mate");
        let mut cache = MetaCache::open(&path).unwrap();

        cache.write(3, pid(7));
        assert_eq!(cache.dirty_count(), 1);

        let snaps = cache.take_dirty_subrange_snapshots();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].window, 0);
        assert_eq!(snaps[0].bytes.len(), 4 * 8, "末窗截断到水位 (vpid 0..=3)");
        assert_eq!(cache.dirty_count(), 0, "take 清 dirty");
        assert!(cache.has_unflushed(), "in-flight 计入未确认");

        // in-flight 期间再 take 应为空 (去重)
        assert!(cache.take_dirty_subrange_snapshots().is_empty());

        cache.complete_subrange_flush(0, 0, 1, true);
        assert!(!cache.has_unflushed());
    }

    /// complete(Err) 重标 dirty, 下轮重取.
    #[test]
    fn snapshot_complete_err_requeues() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("page.mate");
        let mut cache = MetaCache::open(&path).unwrap();

        cache.write(0, pid(1));
        let snaps = cache.take_dirty_subrange_snapshots();
        assert_eq!(snaps.len(), 1);

        cache.complete_subrange_flush(0, 0, 1, false);
        assert_eq!(cache.dirty_count(), 1, "失败重标 dirty");
        assert!(!cache.take_dirty_subrange_snapshots().is_empty(), "可重取");
    }

    /// in-flight 期间新 write 重标 dirty, complete 后还能取到新快照.
    #[test]
    fn write_during_in_flight_marks_dirty_again() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("page.mate");
        let mut cache = MetaCache::open(&path).unwrap();

        cache.write(0, pid(1));
        let _ = cache.take_dirty_subrange_snapshots();
        cache.write(1, pid(2)); // 同 window, in-flight 中
        assert_eq!(cache.dirty_count(), 1);
        assert!(
            cache.take_dirty_subrange_snapshots().is_empty(),
            "in-flight 去重"
        );

        cache.complete_subrange_flush(0, 0, 1, true);
        let snaps = cache.take_dirty_subrange_snapshots();
        assert_eq!(snaps.len(), 1, "complete 后可取新快照");
        assert_eq!(snaps[0].bytes.len(), 2 * 8);
    }

    /// 跨 window 边界懒扩容: vpid = SLOTS_PER_WINDOW 落在 window 1.
    #[test]
    fn cross_window_boundary_lazy_grow() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("page.mate");
        let mut cache = MetaCache::open(&path).unwrap();

        cache.write(SLOTS_PER_WINDOW as u64, pid(9));
        assert_eq!(cache.dirty_count(), 1);
        let snaps = cache.take_dirty_subrange_snapshots();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].window, 1, "vpid=128K 落在 window 1");
        assert_eq!(snaps[0].bytes.len(), 8, "window 1 只有 1 个 slot");
    }
}
