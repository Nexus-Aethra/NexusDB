//! 调试追踪集中管理 (避免各处加 eprintln 又回头删).
//!
//! ## 使用方法
//!
//! 1. 调试时翻 `DEBUG_PAGE = true` 全开, 或单独翻某个细分子开关.
//! 2. 提交前确保 `DEBUG_PAGE = false` (CI 默认零输出).
//!
//! 各模块统一用 `dprintln!` 宏, 第一个参数是 tag, 表示输出归属哪个子系统:
//! ```ignore
//! dprintln!(leaf,     "key={:?}", key);
//! dprintln!(index,    "cp_count={}", cp_count);
//! dprintln!(internal, "split_key={:?}", sk);
//! ```
//!
//! 这样:
//! - 加调试时: 只加一行 `dprintln!(xxx, "fmt", args)` 即可, 不用复制 macro_rules
//! - 删调试时: `DEBUG_PAGE = false` 一行搞定, 不需要删散落的 eprintln
//! - 子模块单独控制: 调试时只关心 leaf 翻 `DEBUG_PAGE_LEAF = true` 即可
//!
//! ## 关闭后的零开销保证
//!
//! `DEBUG_PAGE = false` 时, 编译期 `if false { ... }` 会被优化掉,
//! `dprintln!` 调用在 release binary 中**完全消失** (类似 `debug_assert!`).

/// 总开关. 设为 `true` 启用 page crate 的所有调试 `eprintln!` 输出.
/// **默认 `false` (CI / 提交前保持)**, 调试时翻成 `true`.
pub const DEBUG_PAGE: bool = false;

/// 细分子开关. 即使 `DEBUG_PAGE = true`, 这里为 `false` 的 tag 也不会输出.
/// 默认全部 `false` (避免意外开启 dprintln!).
pub const DEBUG_PAGE_LEAF: bool = false;
pub const DEBUG_PAGE_INDEX: bool = false;
pub const DEBUG_PAGE_INTERNAL: bool = false;
pub const DEBUG_PAGE_PTR: bool = false;
pub const DEBUG_PAGE_HEADER: bool = false;
pub const DEBUG_PAGE_CHECKPOINT: bool = false;
pub const DEBUG_PAGE_ITEM: bool = false;
/// ⭐ 探针 category: 背压退化同步写耗时. 调试时配合 DEBUG_PAGE=true 启用.
pub const DEBUG_PAGE_PROBE: bool = false;

/// 查询: 给定 tag 是否启用.
///
/// `dprintln!` 内部用 `if DEBUG_PAGE && tag_enabled("leaf")` 短路求值,
/// 当 `DEBUG_PAGE = false` 时整个 `if` 分支在 codegen 被消除,
/// `eprintln!` 不进入 binary (与 `debug_assert!` 等价).
pub fn tag_enabled(tag: &'static str) -> bool {
    match tag {
        "leaf" => DEBUG_PAGE_LEAF,
        "index" => DEBUG_PAGE_INDEX,
        "internal" => DEBUG_PAGE_INTERNAL,
        "ptr" => DEBUG_PAGE_PTR,
        "header" => DEBUG_PAGE_HEADER,
        "checkpoint" => DEBUG_PAGE_CHECKPOINT,
        "item" => DEBUG_PAGE_ITEM,
        "pager_probe" => DEBUG_PAGE_PROBE,
        _ => false,
    }
}

/// 统一调试输出宏: `dprintln!(<tag>, "<fmt>", args...)`.
///
/// - 第一个参数 `tag` 是 ident (子系统名: leaf/index/internal/...).
/// - 第二个参数必须是 string literal (`concat!` 需要).
/// - 之后是任意 `expr` 格式参数.
///
/// 输出格式: `[tag] <fmt>`, 写入 stderr. `DEBUG_PAGE = false` 或该 tag
/// 子开关为 `false` 时整条调用编译期消除, 零运行时开销.
#[macro_export]
macro_rules! dprintln {
    ($tag:ident, $fmt:literal $(, $arg:expr)* $(,)?) => {{
        // 编译期把 [tag] 前缀与 fmt 拼接, 避免 eprintln! 拿 string literal 当参数.
        // 用 let-binding 把 tag 提前 stringify, 避免 macro 嵌套 stringify 引发解析问题.
        let __dprintln_tag: &'static str = stringify!($tag);
        if $crate::debug::DEBUG_PAGE && $crate::debug::tag_enabled(__dprintln_tag) {
            eprint!("[{}] ", __dprintln_tag);
            eprintln!($fmt $(, $arg)*);
        }
    }};
}
