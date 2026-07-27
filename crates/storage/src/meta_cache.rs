//! MetaCache v3: 全量平坦数组 + dirty window (T-meta-async).
//!
//! 设计 (2026-07-27, 取代 v2 LFU):
//! - **全量缓存**: `Vec<PidLocation>` 按 vpid 直接索引 (meta:data = 1:2048,
//!   1TB 数据 → 512MB/shard-db, 平坦数组无 HashMap 3-4× 膨胀).
//!   open 时整读 page.mate, read 纯内存索引, **不再有 pread miss 路径**.
//! - **dirty window**: 1MB/window = 128K slot, write 按 window 粒度标脏.
//! - **异步刷盘支持**: `take_dirty_window_snapshots` copy 脏 window 快照
//!   (清 dirty + 标 in-flight 去重), 由 Pager/协程走 io_uring 批量写 + 单次 fsync;
//!   `complete_window_flush` 收割 (失败重标 dirty 下轮重试).
//! - `flush_dirty` 保留为**同步全量路径** (flush()/close 契约):
//!   整 window `write_at` + 单次 `sync_all`. **前提: 无 in-flight 快照**
//!   (caller 先 drain 异步 backlog, 否则旧快照可能乱序覆盖新数据).
//!
//! **vpid 单空间**: 持久化 off = vpid×8 从 v1 起就不区分 db (Pager 已按
//! db 目录物理隔离, 每 db 独立 mate 文件), v3 的 db-aware API 仅保留签名
//! (db 参数忽略), 内存语义与持久化语义一致.
//!
//! **多线程契约**: 所有方法 `&mut self`/`&self`, 单线程使用.

use std::fs::File;
use std::io;
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::types::{DbId, META_PID, PidLocation};

/// dirty window 大小: 1MB.
pub const META_WINDOW_SIZE: usize = 1 << 20;
/// 每 window 的 slot 数: 1MB / 8B = 128K.
pub const SLOTS_PER_WINDOW: usize = META_WINDOW_SIZE / 8;

/// 未分配 slot 哨兵 (flags=0 = 未分配, 与磁盘全零 slot 一致).
const ZERO_PID: PidLocation = PidLocation {
    file_id: 0,
    chunk_idx: 0,
    page_idx: 0,
    flags: 0,
};

// =====================================================================
// MetaCache v3
// =====================================================================

/// MetaCache v3: 全量平坦数组 + page.mate 持久化.
///
/// **公共 API 与 v2 兼容**: `read(vpid)` / `write(vpid, pid)` / `flush_dirty()` /
/// `contains` / `dirty_count` / `len`; db-aware 变体保留签名 (db 忽略).
///
/// - read 纯内存索引 (越界 / flags==0 → None)
/// - write 懒扩容 + 标对应 1MB window dirty
/// - 无容量上限: 内存由 vpid 水位天然决定
pub struct MetaCache {
    /// 平坦 slot 数组, 索引 = vpid. 懒扩容 (write 越界时 resize).
    slots: Vec<PidLocation>,
    /// dirty 标记, 索引 = vpid / SLOTS_PER_WINDOW. 与 slots 同步扩容.
    dirty_windows: Vec<bool>,
    /// 快照已入队未确认 (去重: 防两个协程乱序写同 offset).
    in_flight_windows: Vec<bool>,
    /// page.mate fd (同步 flush 路径用).
    mate_file: File,
}

impl MetaCache {
    /// ⭐ open: 整读 page.mate 进平坦数组 (全量缓存).
    ///
    /// 全零 slot 保留在数组里 (read 按 `flags()==0` 返回 None).
    pub fn open(mate_path: &Path) -> io::Result<Self> {
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

        Ok(Self {
            slots,
            dirty_windows: vec![false; window_count],
            in_flight_windows: vec![false; window_count],
            mate_file,
        })
    }

    /// ⭐ vpid-only API: 读 vpid → PidLocation. 纯内存索引.
    /// 越界 / 未分配 (flags=0) → None.
    pub fn read(&mut self, vpid: u64) -> Option<PidLocation> {
        self.peek(vpid)
    }

    /// 不可变读 (G1: ChunkLiveness::rebuild_from_meta 等遍历场景用).
    pub fn peek(&self, vpid: u64) -> Option<PidLocation> {
        let slot = self.slots.get(vpid as usize)?;
        if slot.flags() == 0 { None } else { Some(*slot) }
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

    /// ⭐ vpid-only API: 写 vpid → PidLocation. 懒扩容 + 标 window dirty.
    pub fn write(&mut self, vpid: u64, pid: PidLocation) {
        let idx = vpid as usize;
        self.ensure_capacity(idx + 1);
        self.slots[idx] = pid;
        self.dirty_windows[idx / SLOTS_PER_WINDOW] = true;
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
    /// 逐 dirty window 整块 `write_at` + 单次 `sync_all`.
    /// **前提**: 无 in-flight 快照 (caller 先 drain 异步 backlog);
    /// 否则协程稍后写入的旧快照会乱序覆盖这里的新数据.
    pub fn flush_dirty(&mut self) -> io::Result<()> {
        debug_assert!(
            !self.in_flight_windows.iter().any(|&b| b),
            "flush_dirty 要求无 in-flight meta 快照 (先 drain 异步 backlog)"
        );
        let mut wrote_any = false;
        for w in 0..self.dirty_windows.len() {
            if !self.dirty_windows[w] {
                continue;
            }
            let bytes = self.window_bytes(w);
            self.mate_file
                .write_at(&bytes, (w * META_WINDOW_SIZE) as u64)?;
            self.dirty_windows[w] = false;
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

    /// 取 dirty ∧ ¬in-flight 的 window 快照: copy 字节 (末窗截断到实际水位),
    /// 清 dirty、标 in-flight (去重: 同 window 有快照在飞时跳过, 下轮再取).
    pub fn take_dirty_window_snapshots(&mut self) -> Vec<(u32, Vec<u8>)> {
        let mut out = Vec::new();
        for w in 0..self.dirty_windows.len() {
            if !self.dirty_windows[w] || self.in_flight_windows[w] {
                continue;
            }
            out.push((w as u32, self.window_bytes(w)));
            self.dirty_windows[w] = false;
            self.in_flight_windows[w] = true;
        }
        out
    }

    /// 收割 window 快照写盘结果: Ok 清 in-flight; Err 清 in-flight + 重标
    /// dirty (下轮重试, 内存 slot 是最新值, 重取快照 ⊇ 失败快照).
    pub fn complete_window_flush(&mut self, window_idx: u32, ok: bool) {
        let w = window_idx as usize;
        if w >= self.in_flight_windows.len() {
            return; // 防御: 未知 window
        }
        self.in_flight_windows[w] = false;
        if !ok {
            self.dirty_windows[w] = true;
        }
    }

    /// 是否还有未确认落盘的 meta (dirty ∪ in-flight 非空).
    pub fn has_unflushed(&self) -> bool {
        self.dirty_windows.iter().any(|&b| b) || self.in_flight_windows.iter().any(|&b| b)
    }

    /// in-flight 快照 window 数 (Pager backlog 判空用).
    pub fn in_flight_window_count(&self) -> usize {
        self.in_flight_windows.iter().filter(|&&b| b).count()
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

    /// dirty window 数 (v3 语义变化: v2 是 dirty entry 数).
    pub fn dirty_count(&self) -> usize {
        self.dirty_windows.iter().filter(|&&b| b).count()
    }

    /// ⭐ T12.6 stub: 写默认 db 的 MetaPage pid.
    pub fn write_meta_page_default(&mut self) {
        self.write(META_VPID_DEFAULT, META_PID);
    }

    // ---------------------------------------------------------------
    // 内部 helpers
    // ---------------------------------------------------------------

    /// 懒扩容 slots 到至少 n 个, 并同步 window 标记数组.
    fn ensure_capacity(&mut self, n: usize) {
        if n > self.slots.len() {
            self.slots.resize(n, ZERO_PID);
        }
        let windows = self.slots.len().div_ceil(SLOTS_PER_WINDOW);
        if windows > self.dirty_windows.len() {
            self.dirty_windows.resize(windows, false);
            self.in_flight_windows.resize(windows, false);
        }
    }

    /// 序列化第 w 个 window 的字节 (末窗截断到 slots 实际水位).
    fn window_bytes(&self, w: usize) -> Vec<u8> {
        let start = w * SLOTS_PER_WINDOW;
        let end = (start + SLOTS_PER_WINDOW).min(self.slots.len());
        let mut bytes = Vec::with_capacity((end - start) * 8);
        for slot in &self.slots[start..end] {
            bytes.extend_from_slice(&slot.to_bytes());
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

        let snaps = cache.take_dirty_window_snapshots();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].0, 0);
        assert_eq!(snaps[0].1.len(), 4 * 8, "末窗截断到水位 (vpid 0..=3)");
        assert_eq!(cache.dirty_count(), 0, "take 清 dirty");
        assert!(cache.has_unflushed(), "in-flight 计入未确认");

        // in-flight 期间再 take 应为空 (去重)
        assert!(cache.take_dirty_window_snapshots().is_empty());

        cache.complete_window_flush(0, true);
        assert!(!cache.has_unflushed());
    }

    /// complete(Err) 重标 dirty, 下轮重取.
    #[test]
    fn snapshot_complete_err_requeues() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("page.mate");
        let mut cache = MetaCache::open(&path).unwrap();

        cache.write(0, pid(1));
        let snaps = cache.take_dirty_window_snapshots();
        assert_eq!(snaps.len(), 1);

        cache.complete_window_flush(0, false);
        assert_eq!(cache.dirty_count(), 1, "失败重标 dirty");
        assert!(!cache.take_dirty_window_snapshots().is_empty(), "可重取");
    }

    /// in-flight 期间新 write 重标 dirty, complete 后还能取到新快照.
    #[test]
    fn write_during_in_flight_marks_dirty_again() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("page.mate");
        let mut cache = MetaCache::open(&path).unwrap();

        cache.write(0, pid(1));
        let _ = cache.take_dirty_window_snapshots();
        cache.write(1, pid(2)); // 同 window, in-flight 中
        assert_eq!(cache.dirty_count(), 1);
        assert!(
            cache.take_dirty_window_snapshots().is_empty(),
            "in-flight 去重"
        );

        cache.complete_window_flush(0, true);
        let snaps = cache.take_dirty_window_snapshots();
        assert_eq!(snaps.len(), 1, "complete 后可取新快照");
        assert_eq!(snaps[0].1.len(), 2 * 8);
    }

    /// 跨 window 边界懒扩容: vpid = SLOTS_PER_WINDOW 落在 window 1.
    #[test]
    fn cross_window_boundary_lazy_grow() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("page.mate");
        let mut cache = MetaCache::open(&path).unwrap();

        cache.write(SLOTS_PER_WINDOW as u64, pid(9));
        assert_eq!(cache.dirty_count(), 1);
        let snaps = cache.take_dirty_window_snapshots();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].0, 1, "vpid=128K 落在 window 1");
        assert_eq!(snaps[0].1.len(), 8, "window 1 只有 1 个 slot");
    }
}
