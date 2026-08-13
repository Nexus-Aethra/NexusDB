//! IoRegistry: 把 io_uring CQE 的 `user_data` 翻译回 slot_id + waker.
//!
//! **设计**: 单线程 owned `HashMap`, 不需要 Mutex. 调度器 `&mut self` 调用.
//! `next_user_data` 仍是 `AtomicU64` (虽然也是单线程用, 但保持原子语义以备未来扩展).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Waker;

/// 一个挂起的 IO 操作: 哪个 slot 在等, 谁来唤醒.
/// `result` 字段: Some 表示 CQE 已到达, Future poll 时读这个; None 表示还没到.
pub struct IoOpState {
    pub slot_id: usize,
    pub waker: Waker,
    pub result: Option<i32>,
}

/// 单 scheduler 生命周期内的 IO registry 观测快照。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IoRegistryStats {
    pub registered: u64,
    pub completed: u64,
    pub cancelled: u64,
    pub unknown_cqe: u64,
}

pub struct IoRegistry {
    inner: HashMap<u64, IoOpState>,
    next_user_data: AtomicU64,
    stats: IoRegistryStats,
}

impl IoRegistry {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
            next_user_data: AtomicU64::new(1),
            stats: IoRegistryStats::default(),
        }
    }

    /// 注册一个 slot + waker, 返回单调递增的 user_data (永不重用).
    pub fn register(&mut self, slot_id: usize, waker: Waker) -> u64 {
        let ud = self.next_user_data.fetch_add(1, Ordering::Relaxed);
        self.inner.insert(
            ud,
            IoOpState {
                slot_id,
                waker,
                result: None,
            },
        );
        self.stats.registered += 1;
        ud
    }

    /// 取出并移除. Future re-poll 时不再能找到.
    pub fn take(&mut self, user_data: u64) -> Option<IoOpState> {
        self.inner.remove(&user_data)
    }

    /// 标记某 ud 的 CQE 已到, 存结果. 不移除, 让 io_ops.poll 自己 take.
    /// 返回是否找到对应的 pending registry entry。
    pub fn mark_completed(&mut self, user_data: u64, result: i32) -> bool {
        if let Some(s) = self.inner.get_mut(&user_data) {
            if s.result.is_none() {
                self.stats.completed += 1;
            }
            s.result = Some(result);
            true
        } else {
            false
        }
    }

    /// Future poll 时取结果 (不唤醒).
    ///
    /// ⭐ 修复 (2026-07-24): 只在结果已到达时才 remove entry.
    /// 之前 `remove().and_then()` 在 CQE 未到时也删 entry, 导致后续
    /// mark_completed 找不到注册项, 结果永久丢失, future 永远 Pending
    /// (主动轮询型 driver 如 drive_until_idle 下必现的 heisenbug).
    pub fn take_result(&mut self, user_data: u64) -> Option<i32> {
        let has_result = self
            .inner
            .get(&user_data)
            .is_some_and(|st| st.result.is_some());
        if !has_result {
            return None;
        }
        self.inner.remove(&user_data).and_then(|st| st.result)
    }

    /// drain_completions 时用: 看 slot_id 但不取走 (已经 mark_completed 了).
    pub fn inner_peek(&self, user_data: u64) -> Option<&IoOpState> {
        self.inner.get(&user_data)
    }

    /// re-poll 时替换 waker (Context 换了新 waker).
    pub fn refresh_waker(&mut self, user_data: u64, new_waker: Waker) {
        if let Some(s) = self.inner.get_mut(&user_data) {
            s.waker = new_waker;
        }
    }

    /// 取消单个 ud (Future 完成或 slot drop).
    pub fn cancel(&mut self, user_data: u64) -> bool {
        let removed = self.inner.remove(&user_data).is_some();
        if removed {
            self.stats.cancelled += 1;
        }
        removed
    }

    /// 取消某 slot 的所有注册 (RR 强制复用时).
    pub fn cancel_slot(&mut self, slot_id: usize) {
        let before = self.inner.len();
        self.inner.retain(|_, st| st.slot_id != slot_id);
        self.stats.cancelled += (before - self.inner.len()) as u64;
    }

    /// 当前 in-flight 操作数 (调试 / has_work 用).
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// 记录不属于任何 pending task 的 CQE，例如 AsyncCancel 自身的完成事件。
    pub fn record_unknown_cqe(&mut self) {
        self.stats.unknown_cqe += 1;
    }

    pub fn stats(&self) -> IoRegistryStats {
        self.stats
    }
}

impl Default for IoRegistry {
    fn default() -> Self {
        Self::new()
    }
}
