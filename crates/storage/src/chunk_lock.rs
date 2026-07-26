//! T6 chunk_lock: 协程级 chunk 锁 (DESIGN §3.0 + plan §3.0).
//!
//! **设计目的**:
//! - 同一 chunk 内所有协程严格按入队顺序串行执行, 避免"两个协程轮流操作同一 page"的撕裂读.
//! - 不同 chunk 互不阻塞, 仍可并行.
//!
//! **当前实现: 同步版本 (TDD 简化)**
//! - 同步 Pager 第一版没有真实 IO 阻塞, chunk_lock 不会真触发 wait queue.
//! - 数据结构完整 (`owner` + `waiters` + `loading`), 逻辑可测.
//! - T11 polish 接 io_uring async 时, waiters 改用 `VecDeque<JoinHandle>`, 并在
//!   `release_and_wake` 时 push 到 scheduler ready queue.
//!
//! **关键不变量**:
//! - 同一 chunk 同一时刻最多一个 owner (持有锁, 准备做 IO 或正在做 IO).
//! - waiters 严格 FIFO 顺序, owner 完成时按序唤醒.
//! - chunk 已在 chunk_list 缓存时无需加锁 (peek 命中走快路径).
//!
//! **单线程使用**: per-shard thread, 同 scheduler crate 契约.

use std::collections::{HashMap, VecDeque};

use crate::chunk_lru::ChunkKey;
use crate::pager::TaskId;
#[cfg(test)]
use crate::types::DEFAULT_DB_ID;

// =====================================================================
// AcquireResult: 申请 chunk_lock 的结果
// =====================================================================

/// `acquire_chunk_lock` 的结果.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireResult {
    /// chunk 已在 chunk_list 缓存, 无需加锁, 直接走快路径返回.
    AlreadyLoaded,
    /// 申请成功: 你是这个 chunk 的 owner, 负责做 IO / 装入 chunk_list / release.
    BecameOwner,
    /// 申请失败: 已有其他 task 是 owner, 你被加入 waiters 队列, 等待被唤醒.
    BecameWaiter,
}

// =====================================================================
// ChunkLockEntry: 单个 chunk 的锁状态
// =====================================================================

/// 单个 chunk 的锁状态.
///
/// **字段语义**:
/// - `owner`: 当前持有锁的 task_id (None 表示锁空闲).
/// - `waiters`: 等待队列, owner 完成时按 FIFO 顺序唤醒.
/// - `loading`: 标记这个 chunk 正在被 owner 加载中 (避免重复 IO).
#[derive(Debug, Default)]
pub struct ChunkLockEntry {
    pub owner: Option<TaskId>,
    pub waiters: VecDeque<TaskId>,
    pub loading: bool,
}

impl ChunkLockEntry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 是否有 owner.
    pub fn is_locked(&self) -> bool {
        self.owner.is_some()
    }

    /// waiters 数量.
    pub fn waiter_count(&self) -> usize {
        self.waiters.len()
    }

    /// 当前 owner 是不是这个 task.
    pub fn is_owner(&self, task_id: TaskId) -> bool {
        self.owner == Some(task_id)
    }
}

// =====================================================================
// ChunkLockMap: 全局 chunk → ChunkLockEntry
// =====================================================================

/// 全局 chunk_lock 表. Pager 持有一个, 协调所有协程对 chunk 的访问.
#[derive(Debug, Default)]
pub struct ChunkLockMap {
    map: HashMap<ChunkKey, ChunkLockEntry>,
}

impl ChunkLockMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// 当前 entry 数 (调试用).
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// 拿到某个 chunk 的 entry (供测试 / 调试).
    pub fn get(&self, key: &ChunkKey) -> Option<&ChunkLockEntry> {
        self.map.get(key)
    }

    /// 申请 chunk_lock. 同步版本立即返回结果 (不 await).
    ///
    /// **逻辑** (DESIGN §3.0):
    /// 1. entry 不存在 → 创建新 entry, 自己为 owner, `loading=true`, return `BecameOwner`
    /// 2. entry 存在, owner 为 None → 抢占, return `BecameOwner`
    /// 3. entry 存在, owner 为自己 → return `BecameOwner` (reentrant, 同一 task 多次 acquire)
    /// 4. entry 存在, owner 为其他 → 加入 waiters 队列, return `BecameWaiter`
    /// 5. 外部标记 `already_loaded=true` (chunk 在 chunk_list 中) → 不动 entry, return `AlreadyLoaded`
    pub fn try_acquire(
        &mut self,
        key: ChunkKey,
        task_id: TaskId,
        already_loaded: bool,
    ) -> AcquireResult {
        if already_loaded {
            // 已在 cache: 不动 entry (有可能是 stale owner, 但 cache hit 走快路径)
            return AcquireResult::AlreadyLoaded;
        }
        match self.map.get_mut(&key) {
            None => {
                // 新 chunk: 创建 entry, 自己为 owner
                let mut entry = ChunkLockEntry::new();
                entry.owner = Some(task_id);
                entry.loading = true;
                self.map.insert(key, entry);
                AcquireResult::BecameOwner
            }
            Some(entry) => {
                if entry.owner == Some(task_id) {
                    // reentrant: 同一 task 再次 acquire
                    entry.loading = true;
                    AcquireResult::BecameOwner
                } else {
                    // 别人是 owner: 加入 waiters 队列
                    entry.waiters.push_back(task_id);
                    AcquireResult::BecameWaiter
                }
            }
        }
    }

    /// ⭐ Owner 完成 IO 后调用: 释放 owner + 唤醒下一个 waiter.
    ///
    /// **返回**: 下一个要唤醒的 waiter task_id (如果有), caller 负责
    /// 在 T11 时把它 push 到 scheduler ready queue.
    ///
    /// **不变量**:
    /// - caller 必须是当前 owner (assert)
    /// - 调用后 entry 仍存在, 但 `owner` 字段可能已切换到新 waiter
    /// - 如果 waiters 为空, entry 完全移除 (避免 map 内存泄漏)
    pub fn release_and_wake(&mut self, key: &ChunkKey, current_task: TaskId) -> Option<TaskId> {
        let entry = self
            .map
            .get_mut(key)
            .expect("release_and_wake: entry must exist");
        assert_eq!(
            entry.owner,
            Some(current_task),
            "release_and_wake: task_id must be current owner"
        );
        entry.loading = false;
        // 唤醒下一个 waiter (FIFO 顺序)
        if let Some(next) = entry.waiters.pop_front() {
            entry.owner = Some(next);
            entry.loading = true;
            Some(next)
        } else {
            // 没 waiter: 移除 entry
            self.map.remove(key);
            None
        }
    }

    /// 强制释放锁 (不唤醒 waiter). 用于 panic 恢复 / 测试清理.
    pub fn force_release(&mut self, key: &ChunkKey) {
        self.map.remove(key);
    }

    /// 清空所有 entry (测试 reset).
    pub fn clear(&mut self) {
        self.map.clear();
    }
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn k(c: u8) -> ChunkKey {
        ChunkKey {
            db: DEFAULT_DB_ID,
            file_id: 0,
            chunk_idx: c,
        }
    }

    #[test]
    fn try_acquire_new_chunk_becomes_owner() {
        let mut m = ChunkLockMap::new();
        let r = m.try_acquire(k(0), 100, false);
        assert_eq!(r, AcquireResult::BecameOwner);
        let entry = m.get(&k(0)).unwrap();
        assert_eq!(entry.owner, Some(100));
        assert!(entry.loading);
        assert_eq!(entry.waiter_count(), 0);
    }

    #[test]
    fn try_acquire_already_loaded_no_entry_change() {
        let mut m = ChunkLockMap::new();
        // 即使 entry 不存在, already_loaded=true 直接返回 AlreadyLoaded
        let r = m.try_acquire(k(0), 100, true);
        assert_eq!(r, AcquireResult::AlreadyLoaded);
        assert!(m.is_empty(), "AlreadyLoaded 不应创建 entry");
    }

    #[test]
    fn try_acquire_existing_owner_becomes_waiter() {
        let mut m = ChunkLockMap::new();
        m.try_acquire(k(0), 100, false);
        let r = m.try_acquire(k(0), 200, false);
        assert_eq!(r, AcquireResult::BecameWaiter);
        let entry = m.get(&k(0)).unwrap();
        assert_eq!(entry.owner, Some(100));
        assert_eq!(entry.waiter_count(), 1);
        assert_eq!(entry.waiters.front(), Some(&200));
    }

    #[test]
    fn try_acquire_reentrant_same_task_returns_owner() {
        let mut m = ChunkLockMap::new();
        m.try_acquire(k(0), 100, false);
        let r = m.try_acquire(k(0), 100, false);
        assert_eq!(
            r,
            AcquireResult::BecameOwner,
            "reentrant 同一 task 应仍是 owner"
        );
    }

    #[test]
    fn release_and_wake_promotes_next_waiter() {
        let mut m = ChunkLockMap::new();
        m.try_acquire(k(0), 100, false);
        m.try_acquire(k(0), 200, false);
        m.try_acquire(k(0), 300, false);
        // 三个 task 排队: 100 owner, 200 / 300 waiters
        let entry = m.get(&k(0)).unwrap();
        assert_eq!(entry.owner, Some(100));
        assert_eq!(entry.waiter_count(), 2);

        // 100 release: 唤醒 200
        let next = m.release_and_wake(&k(0), 100);
        assert_eq!(next, Some(200));
        let entry = m.get(&k(0)).unwrap();
        assert_eq!(entry.owner, Some(200), "新 owner 应是 200");
        assert_eq!(entry.waiter_count(), 1, "waiter 队列剩 1 个 (300)");
        assert!(entry.loading);
    }

    #[test]
    fn release_and_wake_no_waiter_removes_entry() {
        let mut m = ChunkLockMap::new();
        m.try_acquire(k(0), 100, false);
        let next = m.release_and_wake(&k(0), 100);
        assert_eq!(next, None);
        assert!(m.get(&k(0)).is_none(), "无 waiter 应移除 entry");
    }

    #[test]
    fn release_and_wake_asserts_owner_mismatch() {
        let mut m = ChunkLockMap::new();
        m.try_acquire(k(0), 100, false);
        // 错误 task_id release 应 panic
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            m.release_and_wake(&k(0), 999);
        }));
        assert!(result.is_err(), "非 owner release 应 panic");
    }

    #[test]
    fn fifo_order_preserved() {
        let mut m = ChunkLockMap::new();
        m.try_acquire(k(0), 100, false);
        // 加入 waiters 200, 300, 400
        m.try_acquire(k(0), 200, false);
        m.try_acquire(k(0), 300, false);
        m.try_acquire(k(0), 400, false);

        // 100 release → 200
        assert_eq!(m.release_and_wake(&k(0), 100), Some(200));
        // 200 release → 300
        assert_eq!(m.release_and_wake(&k(0), 200), Some(300));
        // 300 release → 400
        assert_eq!(m.release_and_wake(&k(0), 300), Some(400));
        // 400 release → None
        assert_eq!(m.release_and_wake(&k(0), 400), None);
        assert!(m.get(&k(0)).is_none());
    }

    #[test]
    fn force_release_clears_entry() {
        let mut m = ChunkLockMap::new();
        m.try_acquire(k(0), 100, false);
        m.try_acquire(k(0), 200, false);
        m.force_release(&k(0));
        assert!(m.get(&k(0)).is_none(), "force_release 后 entry 消失");
    }

    #[test]
    fn different_chunks_independent() {
        let mut m = ChunkLockMap::new();
        m.try_acquire(k(0), 100, false);
        m.try_acquire(k(1), 200, false);
        // 两个 chunk 独立, 互不影响
        let e0 = m.get(&k(0)).unwrap();
        let e1 = m.get(&k(1)).unwrap();
        assert_eq!(e0.owner, Some(100));
        assert_eq!(e1.owner, Some(200));
        assert_eq!(m.len(), 2);
    }
}
