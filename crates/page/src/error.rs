//! Page 操作错误类型.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PageError {
    #[error("page is full, cannot insert item")]
    PageFull,

    #[error("invalid page header: magic mismatch")]
    InvalidHeader,

    #[error("checksum mismatch: page corrupted")]
    ChecksumMismatch,

    #[error("item decode error: {0}")]
    ItemDecode(String),

    /// 调用方违反了"该偏移必须是段首/哨兵 (shared_prefix_len == 0)"的前置条件.
    /// 用硬错误而非 `debug_assert`: 违反时 cur_key 只能由 key_unshared 拼出,
    /// 会**静默产出错误 key** —— debug 下 panic 尚可被看见, release 下无声错读更糟.
    #[error("item at offset is not a segment head: shared_prefix_len={0} (expected 0)")]
    NotSegmentHead(u16),

    #[error("invalid page type: expected {expected:?}, got {got:?}")]
    InvalidPageType {
        expected: crate::header::PageType,
        got: crate::header::PageType,
    },

    #[error("page too small: need {need} bytes, got {got}")]
    PageTooSmall { need: usize, got: usize },

    #[error("split failed: page has only {0} items, cannot split")]
    SplitTooFew(usize),

    #[error("key not found")]
    KeyNotFound,
}
