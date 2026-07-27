//! 单线程内部协程调度器 + io_uring 协程异步结合.
//!
//! 完整设计见 `docs/superpowers/specs/2026-07-17-scheduler-crate-design.md`.
//!
//! ## Feature flags
//! - `scheduler-trace`: 启用内部 trace 日志. 默认关闭, 零开销.

#![allow(dead_code)]

mod await_predicate;
mod park;
mod pool;
mod ready;
mod scheduler;
mod task;
mod trace;
mod waker;

pub mod fd_pool;
pub mod io_ops;
mod io_registry;
mod yield_now;

pub use fd_pool::{FdPool, FdPoolError, MAX_FD_PER_SHARD};

pub use io_registry::{IoOpState, IoRegistry};
pub use park::{
    clear_all_parked, is_parked, park_current_coroutine, parked_count, register_parked,
    take_parked, unpark,
};
pub use pool::Pool;
pub use ready::{ReadyQueueHandle, new_handle as new_ready_queue};
pub use scheduler::{SchedHandle, Scheduler, with_current};
pub use task::{JoinError, JoinHandle, spawn, spawn_on, spawn_on_low};
pub use yield_now::yield_now;

// 测试帮助模块
pub mod test_support {
    pub use crate::task::test_support::*;
}

pub use waker::make_waker_for_test;
pub use await_predicate::{await_predicate, AwaitPredicate};
