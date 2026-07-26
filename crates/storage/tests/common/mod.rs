//! T17 公共测试 helper: 跑 async 测试在 scheduler 上.
//!
//! **为什么需要**: Storage engine 从 T17 开始全 async, 但 `#[test]` 是 sync 的.
//! 模式: 用 `scheduler::SchedHandle + spawn_on + drive_until_idle + pollster::block_on`
//! 在测试线程上跑协程, 直到 future 完成.
//!
//! **⭐ 栈大小 (T17)**: Storage 内联 async fn 返回 [u8; 16KB] page buffer,
//! 多个 inline 后 poll frame 含多个 16KB 数组, 极易爆 8MB 默认线程栈.
//! 跑测试前请设置 `RUST_MIN_STACK=67108864` (或 64MB+),
//! 否则测试 thread 栈不够会 stack overflow.
//!
//! **用法**:
//! ```ignore
//! use crate::common::run_async;
//!
//! #[test]
//! fn my_test() {
//!     run_async(async {
//!         let mut e = StorageEngine::open(opts).await.unwrap();
//!         e.put(data).await.unwrap();
//!     });
//! }
//! ```

/// 跑 async 测试在一个新的 scheduler 上.
///
/// **前提**: 测试进程须以 `RUST_MIN_STACK=67108864` (64MB) 启动,
/// 否则 storage 内联 async fn 状态机 + 多个 [u8; 16KB] 局部变量会爆栈.
///
/// 模式:
/// 1. 新建 SchedHandle
/// 2. set_current 让 storage engine 的 with_current 能找到 scheduler
/// 3. spawn_on 提交 future
/// 4. drive_until_idle 驱动 scheduler 直到没有 ready 任务
/// 5. pollster::block_on 等 JoinHandle 完成
#[allow(dead_code)] // 不是所有测试文件都用 run_async
pub fn run_async<F>(f: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    let rt = scheduler::SchedHandle::new(scheduler::Scheduler::new());
    rt.set_current();
    let h = scheduler::spawn_on(&rt, f);
    let _ = rt.drive_until_idle(10_000);
    pollster::block_on(h).unwrap();
}

/// 跑 async 测试返回结果 (用于 `assert_eq!` 直接比较).
#[allow(dead_code)] // 不是所有测试文件都用 run_async_ret
pub fn run_async_ret<F, R>(f: F) -> R
where
    F: std::future::Future<Output = R> + 'static,
    R: 'static,
{
    let rt = scheduler::SchedHandle::new(scheduler::Scheduler::new());
    rt.set_current();
    let h = scheduler::spawn_on(&rt, f);
    let _ = rt.drive_until_idle(10_000);
    pollster::block_on(h).unwrap()
}
