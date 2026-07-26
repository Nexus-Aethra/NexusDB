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
