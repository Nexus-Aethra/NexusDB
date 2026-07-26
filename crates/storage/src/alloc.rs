//! Allocator 模块: VpidAllocator + PidAllocator + FreePageQueue.
//!
//! 设计 (用户敲定 2026-07-18):
//! - **单线程使用** (per-shard thread, 与 scheduler crate 同约束)
//! - 用 `Cell<u64>` / `Cell<u32>` / `Cell<u8>` / 内部 `Vec` 即可, 无需 Atomic / Mutex
//! - 调用方保证不跨线程使用 (违反会触发 UB, 与 `JoinInner::UnsafeCell` 同契约)
//!
//! 设计原则 (来自 plan §3.3):
//! - **Vpid 永不重用**: 一旦分配永不回收. free list 仅复用"曾经分配过"的 vpid, 不让 next_vpid 自减.
//! - **COW 重映射由 meta_cache 完成**, 而不是 vpid 被分配新值.
//! - PidAllocator 在 chunk 满 (== 64) 时返回 None, caller (ChunkWriter) 触发 rotate.
//! - PidAllocator **不**做持久化, 启动时由 recover (T7) 重建.
//!
//! ⚠️ PidAllocator 的 file_id / chunk_idx 状态**会随 rotate 改变**, 但 next_vpid 不变.
//! VpidAllocator 的 free list **不持久化** (运行期内 in-memory, 重启归零由 recover 重建).

use std::collections::HashMap;

use crate::meta_cache::MetaCache;
use crate::types::{DEFAULT_DB_ID, DbId, PAGES_PER_CHUNK, PID_ALIVE, PidLocation};

// =====================================================================
// VpidAllocator (per-db, T12.7)
// =====================================================================

/// 虚拟页 ID 分配器 (per-db).
///
/// **不变量** (per-db 独立):
/// - next_vpid 单调递增 (永不递减), free list 仅复用
/// - 同一 vpid 不会被分配两次 ("永不重用" 旧解释); free 后再次 alloc 可能命中
/// - 启动后 next_vpid 从 `initial` 开始 (recover 时传最后 max_vpid)
///
/// **多 db 隔离** (T12.7): 不同 db 的 vpid 空间独立 — db=0 的 vpid 0..N 与
/// db=1 的 vpid 0..M 互不干扰. 内部 `HashMap<DbId, VpidState>`.
pub struct VpidAllocator {
    /// per-db 状态.
    states: HashMap<DbId, VpidState>,
    /// 全局默认 initial (单 db 兼容).
    default_initial: u64,
}

/// 单 db 的 vpid 状态.
struct VpidState {
    /// 下一个待分配的 vpid.
    next_vpid: u64,
    /// LIFO free list (Vec 即可, 单线程).
    free: Vec<u64>,
}

impl VpidAllocator {
    /// 单 db 兼容: 创建默认 db=0 状态, initial 给定.
    pub fn new(initial: u64) -> Self {
        let mut states = HashMap::new();
        states.insert(
            DEFAULT_DB_ID,
            VpidState {
                next_vpid: initial,
                free: Vec::new(),
            },
        );
        Self {
            states,
            default_initial: initial,
        }
    }

    /// 创建空 allocator (无默认 db).
    pub fn empty() -> Self {
        Self {
            states: HashMap::new(),
            default_initial: 0,
        }
    }

    /// ⭐ alloc / free compat: 等价 db-aware 默认 db=0 版本.
    /// 分配 vpid (默认 db=0).
    pub fn alloc(&mut self, _meta: &mut MetaCache) -> u64 {
        self.alloc_db(DEFAULT_DB_ID)
    }

    /// 回收 vpid (默认 db=0).
    pub fn free(&mut self, vpid: u64, _meta: &mut MetaCache) {
        self.free_db(DEFAULT_DB_ID, vpid);
    }

    /// 已分配最大 vpid + 1 (默认 db).
    pub fn current(&self) -> u64 {
        self.current_db(DEFAULT_DB_ID)
    }

    /// free list 长度 (默认 db).
    pub fn free_count(&self) -> usize {
        self.free_count_db(DEFAULT_DB_ID)
    }

    // ⭐ db-aware API

    /// ⭐ db-aware alloc.
    pub fn alloc_db(&mut self, db: DbId) -> u64 {
        let state = self.states.entry(db).or_insert_with(|| VpidState {
            next_vpid: self.default_initial,
            free: Vec::new(),
        });
        if let Some(v) = state.free.pop() {
            return v;
        }
        let v = state.next_vpid;
        state.next_vpid += 1;
        v
    }

    /// ⭐ db-aware free.
    pub fn free_db(&mut self, db: DbId, vpid: u64) {
        let state = self.states.entry(db).or_insert_with(|| VpidState {
            next_vpid: self.default_initial,
            free: Vec::new(),
        });
        state.free.push(vpid);
    }

    /// ⭐ db-aware current (next_vpid).
    pub fn current_db(&self, db: DbId) -> u64 {
        self.states
            .get(&db)
            .map(|s| s.next_vpid)
            .unwrap_or(self.default_initial)
    }

    /// ⭐ db-aware free_count.
    pub fn free_count_db(&self, db: DbId) -> usize {
        self.states.get(&db).map(|s| s.free.len()).unwrap_or(0)
    }

    /// ⭐ 测试 helper: 该 db 已分配 vpid 数 (含 free list).
    pub fn total_allocated_db(&self, db: DbId) -> u64 {
        self.current_db(db) // next_vpid 即包含已分配的 + free list 头
    }

    /// ⭐ 设置某个 db 的初始 next_vpid (recover 时用).
    pub fn set_initial_db(&mut self, db: DbId, initial: u64) {
        let state = self.states.entry(db).or_insert_with(|| VpidState {
            next_vpid: initial,
            free: Vec::new(),
        });
        if initial > state.next_vpid {
            state.next_vpid = initial;
        }
    }

    /// ⭐ 测试 helper: 列出所有 db.
    pub fn dbs(&self) -> Vec<DbId> {
        let mut v: Vec<DbId> = self.states.keys().copied().collect();
        v.sort();
        v
    }
}

// =====================================================================
// PidAllocator (per-db, T12.8)
// =====================================================================

/// 物理 page ID 分配器 (per-db). chunk 满时返回 None, caller (ChunkWriter) 触发 rotate.
///
/// **设计**:
/// - file_id: 当前 .block 文件 ID
/// - chunk_idx: 当前 block 内 chunk 索引 (0..9)
/// - next_page_in_chunk: 当前 chunk 内已分配 page 数 (0..64, ==64 时满)
///
/// **多 db 隔离** (T12.8): 每个 db 独立 (file_id, chunk_idx, next_page).
/// 不同 db 物理地址空间完全独立 (新 db 不复用旧 db 的 page).
///
/// 启动时由 recover (T7) 从磁盘恢复状态.
pub struct PidAllocator {
    states: HashMap<DbId, PidState>,
}

#[derive(Clone, Copy, Debug)]
struct PidState {
    file_id: u32,
    chunk_idx: u8,
    next_page_in_chunk: u8,
}

impl PidAllocator {
    /// 单 db 兼容: 创建默认 db=0 状态.
    pub fn new(file_id: u32, chunk_idx: u8, next_page: u8) -> Self {
        let mut states = HashMap::new();
        states.insert(
            DEFAULT_DB_ID,
            PidState {
                file_id,
                chunk_idx,
                next_page_in_chunk: next_page,
            },
        );
        Self { states }
    }

    /// 创建空 allocator.
    pub fn empty() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    // ⭐ compat: 默认 db=0

    /// 分配 pid (默认 db=0).
    pub fn alloc(&mut self) -> Option<PidLocation> {
        self.alloc_db(DEFAULT_DB_ID)
    }

    /// 当前状态快照 (默认 db).
    pub fn current(&self) -> (u32, u8, u8) {
        self.current_db(DEFAULT_DB_ID)
    }

    /// 切到新 chunk (默认 db).
    pub fn rotate_to(&mut self, file_id: u32, chunk_idx: u8) {
        self.rotate_to_db(DEFAULT_DB_ID, file_id, chunk_idx);
    }

    // ⭐ db-aware API

    /// ⭐ db-aware alloc.
    pub fn alloc_db(&mut self, db: DbId) -> Option<PidLocation> {
        // ⭐ 用 or_insert_with 而不是 or_insert: 否则会覆盖 `PidAllocator::new()`
        // 已经创建的 db=0 状态, 丢失起点参数.
        let state = self.states.entry(db).or_insert_with(|| PidState {
            file_id: 0,
            chunk_idx: 0,
            next_page_in_chunk: 0,
        });
        let page = state.next_page_in_chunk;
        if page as usize >= PAGES_PER_CHUNK {
            return None;
        }
        state.next_page_in_chunk += 1;
        let pid = PidLocation::from_bytes(&[
            (state.file_id & 0xFF) as u8,
            ((state.file_id >> 8) & 0xFF) as u8,
            ((state.file_id >> 16) & 0xFF) as u8,
            ((state.file_id >> 24) & 0xFF) as u8,
            state.chunk_idx,
            page,
            0,
            PID_ALIVE,
        ]);
        Some(pid)
    }

    /// ⭐ db-aware current.
    pub fn current_db(&self, db: DbId) -> (u32, u8, u8) {
        self.states
            .get(&db)
            .map(|s| (s.file_id, s.chunk_idx, s.next_page_in_chunk))
            .unwrap_or((0, 0, 0))
    }

    /// ⭐ db-aware rotate.
    pub fn rotate_to_db(&mut self, db: DbId, file_id: u32, chunk_idx: u8) {
        // ⭐ 用 or_insert_with 而不是 or_insert, 避免覆盖已有 state.
        let state = self.states.entry(db).or_insert_with(|| PidState {
            file_id: 0,
            chunk_idx: 0,
            next_page_in_chunk: 0,
        });
        state.file_id = file_id;
        state.chunk_idx = chunk_idx;
        state.next_page_in_chunk = 0;
    }

    /// ⭐ 设置某个 db 的初始 (file_id, chunk_idx, next_page) — recover 时用.
    pub fn set_initial_db(&mut self, db: DbId, file_id: u32, chunk_idx: u8, next_page: u8) {
        self.states.insert(
            db,
            PidState {
                file_id,
                chunk_idx,
                next_page_in_chunk: next_page,
            },
        );
    }

    /// ⭐ 测试 helper: 列出所有 db.
    pub fn dbs(&self) -> Vec<DbId> {
        let mut v: Vec<DbId> = self.states.keys().copied().collect();
        v.sort();
        v
    }
}

// =====================================================================
// FreePageQueue (单线程使用, Vec 即可, 无 Mutex)
// =====================================================================

/// 当前 chunk 内空闲 page 索引 LIFO 栈 (per-db, T12.8).
///
/// **设计**:
/// - 当 chunk 内 page 被 COW 替换 (旧 page 不再用), 把旧 page_idx push 进 queue
/// - 新的 PidAllocator 分配时优先调 `pop()` 取, 否则正常 next_page_in_chunk
/// - chunk 切换时清空 (chunk 内的 free page 只在 chunk 内有效)
pub struct FreePageQueue {
    /// per-db 队列.
    states: HashMap<DbId, Vec<u16>>,
}

impl FreePageQueue {
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    /// compat: push 到默认 db=0.
    pub fn push(&mut self, page_idx: u16) {
        self.push_db(DEFAULT_DB_ID, page_idx);
    }

    /// compat: pop 默认 db=0.
    pub fn pop(&mut self) -> Option<u16> {
        self.pop_db(DEFAULT_DB_ID)
    }

    /// compat: 默认 db is_empty.
    pub fn is_empty(&self) -> bool {
        self.is_empty_db(DEFAULT_DB_ID)
    }

    /// compat: 默认 db len.
    pub fn len(&self) -> usize {
        self.len_db(DEFAULT_DB_ID)
    }

    /// compat: 清空 (兼容旧 API, 清所有 db).
    pub fn clear(&mut self) {
        self.clear_all();
    }

    // ⭐ db-aware API

    /// ⭐ db-aware push.
    pub fn push_db(&mut self, db: DbId, page_idx: u16) {
        debug_assert!(
            page_idx < PAGES_PER_CHUNK as u16,
            "page_idx must be < 64 (within chunk)"
        );
        self.states.entry(db).or_default().push(page_idx);
    }

    /// ⭐ db-aware pop.
    pub fn pop_db(&mut self, db: DbId) -> Option<u16> {
        self.states.get_mut(&db).and_then(|v| v.pop())
    }

    /// ⭐ db-aware is_empty.
    pub fn is_empty_db(&self, db: DbId) -> bool {
        self.states.get(&db).map(|v| v.is_empty()).unwrap_or(true)
    }

    /// ⭐ db-aware len.
    pub fn len_db(&self, db: DbId) -> usize {
        self.states.get(&db).map(|v| v.len()).unwrap_or(0)
    }

    /// ⭐ 清空所有 db.
    pub fn clear_all(&mut self) {
        self.states.clear();
    }

    /// ⭐ 清空特定 db.
    pub fn clear_db(&mut self, db: DbId) {
        self.states.remove(&db);
    }
}

impl Default for FreePageQueue {
    fn default() -> Self {
        Self::new()
    }
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vpid_initial_0() {
        let a = VpidAllocator::new(0);
        assert_eq!(a.current(), 0);
        assert_eq!(a.free_count(), 0);
    }

    #[test]
    fn vpid_with_initial_offset() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("page.mate");
        std::fs::File::create(&path).unwrap();
        let mut meta = MetaCache::open(&path).unwrap();
        let mut a = VpidAllocator::new(1000);
        assert_eq!(a.alloc(&mut meta), 1000);
        assert_eq!(a.alloc(&mut meta), 1001);
        assert_eq!(a.current(), 1002);
    }

    // =====================================================================
    // ⭐ T12.7: VpidAllocator per-db 测试
    // =====================================================================

    #[test]
    fn vpid_db_aware_independent_counters() {
        // db=0 和 db=1 的 vpid 空间完全独立
        let mut a = VpidAllocator::new(0);
        assert_eq!(a.alloc_db(0), 0);
        assert_eq!(a.alloc_db(0), 1);
        assert_eq!(a.alloc_db(1), 0, "db=1 从 0 开始, 不受 db=0 影响");
        assert_eq!(a.alloc_db(1), 1);
        assert_eq!(a.alloc_db(0), 2, "db=0 独立继续");
    }

    #[test]
    fn vpid_db_aware_free_list_isolated() {
        let mut a = VpidAllocator::new(0);
        let v0 = a.alloc_db(0);
        let v1 = a.alloc_db(1);
        a.free_db(0, v0);
        // db=0 free list 命中, db=1 free list 空
        assert_eq!(a.alloc_db(0), v0, "db=0 free list 命中");
        assert_eq!(a.alloc_db(1), v1 + 1, "db=1 没 free list, next_vpid 自增");
    }

    #[test]
    fn vpid_db_aware_set_initial_db_recover() {
        // 模拟 recover: 设初始 next_vpid 后 alloc
        let mut a = VpidAllocator::new(0);
        a.set_initial_db(0, 100);
        a.set_initial_db(1, 50);
        assert_eq!(a.alloc_db(0), 100);
        assert_eq!(a.alloc_db(1), 50);
        assert_eq!(a.alloc_db(0), 101);
    }

    #[test]
    fn vpid_compat_api_still_works_db_zero() {
        let mut a = VpidAllocator::new(5);
        assert_eq!(a.alloc(&mut unused_meta()), 5);
        assert_eq!(a.current(), 6);
        a.free(5, &mut unused_meta());
        assert_eq!(a.free_count(), 1);
        assert_eq!(a.alloc(&mut unused_meta()), 5, "free list 命中");
    }

    // =====================================================================
    // ⭐ T12.8: PidAllocator per-db 测试
    // =====================================================================

    #[test]
    fn pid_db_aware_independent_state() {
        let mut p = PidAllocator::new(0, 0, 0);
        // db=0 分配 5 个
        for _ in 0..5 {
            assert!(p.alloc_db(0).is_some());
        }
        assert_eq!(p.current_db(0), (0, 0, 5));

        // db=1 应独立 (file_id=0, chunk_idx=0, page=0)
        let pid_db1_first = p.alloc_db(1).expect("db=1 first alloc");
        assert_eq!(pid_db1_first.file_id(), 0);
        assert_eq!(pid_db1_first.chunk_idx(), 0);
        assert_eq!(pid_db1_first.page_idx(), 0);
        assert_eq!(p.current_db(1), (0, 0, 1), "db=1 独立计数");
    }

    #[test]
    fn pid_db_aware_rotate_independent() {
        let mut p = PidAllocator::new(0, 0, 0);
        p.rotate_to_db(0, 1, 5);
        // db=1 没动
        assert_eq!(p.current_db(0), (1, 5, 0));
        assert_eq!(p.current_db(1), (0, 0, 0));
    }

    #[test]
    fn pid_chunk_full_returns_none_per_db() {
        // 填满 db=0 chunk (64 pages), 第 65 个 alloc 应 None
        let mut p = PidAllocator::new(0, 0, 0);
        for _ in 0..64 {
            assert!(p.alloc_db(0).is_some());
        }
        assert!(p.alloc_db(0).is_none(), "第 65 个 alloc 应返回 None");

        // db=1 独立 chunk, 不受影响
        assert!(p.alloc_db(1).is_some(), "db=1 独立 chunk, 应能 alloc");
    }

    #[test]
    fn free_page_queue_db_aware_isolated() {
        let mut q = FreePageQueue::new();
        q.push_db(0, 10);
        q.push_db(0, 11);
        q.push_db(1, 20);

        // db=0 应 pop 11, 10 (LIFO)
        assert_eq!(q.pop_db(0), Some(11));
        assert_eq!(q.pop_db(0), Some(10));
        assert_eq!(q.pop_db(0), None);

        // db=1 独立, 仍能 pop 20
        assert_eq!(q.pop_db(1), Some(20));
        assert_eq!(q.pop_db(1), None);
    }

    #[test]
    fn free_page_queue_clear_specific_db() {
        let mut q = FreePageQueue::new();
        q.push_db(0, 1);
        q.push_db(1, 2);
        q.clear_db(0);
        assert_eq!(q.pop_db(0), None, "db=0 清空");
        assert_eq!(q.pop_db(1), Some(2), "db=1 保留");
    }

    // =====================================================================
    // ⭐ 测试 helper: 创建未使用 MetaCache (compat API 调用需要 &mut meta)
    // =====================================================================
    fn unused_meta() -> crate::meta_cache::MetaCache {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("unused.mate");
        std::fs::File::create(&path).unwrap();
        MetaCache::open(&path).unwrap()
    }
}
