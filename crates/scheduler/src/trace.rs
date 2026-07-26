//! 调试用 trace 日志, 通过 `scheduler-trace` feature 启用.
//!
//! 默认零开销 (`trace!` 宏在 feature 关闭时展开为空).
//! 开启时打印到 stderr, 用 `[trace]` 前缀, 方便 grep 过滤.
//!
//! 用法:
//! ```ignore
//! use crate::trace::trace;
//! trace!("drive_once phase A start");
//! ```

/// feature 关闭时, 整个 trace! 调用被宏替换为空, 零运行时开销.
///
/// Rust 宏展开 `trace!("x")` 为 `()`, `trace!("x {}", y)` 为 `()`.
/// `cfg(feature = "scheduler-trace")` 为 false 时, 宏体完全不进编译.
#[macro_export]
macro_rules! trace {
    ($($arg:tt)*) => {
        #[cfg(feature = "scheduler-trace")]
        {
            eprintln!("[trace] {}", format_args!($($arg)*));
        }
        #[cfg(not(feature = "scheduler-trace"))]
        {
            // 静默吞掉所有参数, 但保留格式检查 (format_args 编译期展开).
            let _ = format_args!($($arg)*);
        }
    };
}
