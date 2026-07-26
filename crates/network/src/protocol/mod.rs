//! Protocol trait + Request/Response types.
//!
//! 纯字节 ↔ KV 转换, 不接触 shard / scheduler / IO.

pub mod binary;
pub mod resp;

use thiserror::Error;

pub use self::binary::BinaryProtocol;
pub use self::resp::{RespCommand, RespCodec};

/// Protocol codec error.
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("frame too large: {size} > max {max}")]
    FrameTooLarge { size: usize, max: usize },
    #[error("incomplete frame: need more bytes")]
    Incomplete,
    #[error("invalid opcode: {0}")]
    InvalidOpcode(u8),
    #[error("invalid length: {0}")]
    InvalidLength(String),
    #[error("internal codec error: {0}")]
    Internal(String),
}

/// 解码结果: 完整帧 or 还需更多字节.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeOutcome<T> {
    Complete { consumed: usize, value: T },
    NeedMore,
}

/// 上层 KV 请求 (跟 ShardManager API 解耦).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Put { key: Vec<u8>, value: Vec<u8> },
    Get { key: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// 上层 KV 响应.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    PutOk,
    Get(Option<Vec<u8>>),
    DeleteOk,
    Error(String),
}

/// KV 长度限制 (所有协议门面共用的校验层).
///
/// 超限请求在 worker parse 后、进 shard 前被拦截, 直接返回协议级 error.
///
/// 上限依据: page crate 编码路径用 `[0u8; 4096]` 栈缓冲,
/// 单条 item (key + value + tag + varint 开销) 硬上限 4096B.
/// 默认 key 1024 + value 3000 + 1B type tag + 编码开销 < 4096, 任意组合安全.
#[derive(Debug, Clone, Copy)]
pub struct KvLimits {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
}

impl Default for KvLimits {
    fn default() -> Self {
        Self {
            max_key_bytes: 1024,
            max_value_bytes: 3000,
        }
    }
}

/// 校验请求的 key/value 长度. 超限返回人类可读错误消息 (直接回给 client).
pub fn validate_request(req: &Request, limits: &KvLimits) -> Result<(), String> {
    let (key, value): (&[u8], &[u8]) = match req {
        Request::Put { key, value } => (key, value),
        Request::Get { key } | Request::Delete { key } => (key, &[]),
    };
    if key.is_empty() {
        return Err("key must not be empty".to_string());
    }
    if key.len() > limits.max_key_bytes {
        return Err(format!(
            "key too long: {} > max {}",
            key.len(),
            limits.max_key_bytes
        ));
    }
    if value.len() > limits.max_value_bytes {
        return Err(format!(
            "value too long: {} > max {}",
            value.len(),
            limits.max_value_bytes
        ));
    }
    Ok(())
}

/// Protocol codec trait.
pub trait Protocol: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn decode_request(&self, buf: &[u8]) -> Result<DecodeOutcome<Request>, Self::Error>;
    fn encode_request(&self, req_id: u64, req: &Request) -> Vec<u8>;

    fn decode_response(&self, buf: &[u8]) -> Result<DecodeOutcome<Response>, Self::Error>;
    fn encode_response(&self, req_id: u64, resp: &Response) -> Vec<u8>;

    fn max_frame_size(&self) -> usize {
        16 * 1024 * 1024
    }
}