//! Protocol trait + Request/Response types.
//!
//! 纯字节 ↔ KV 转换, 不接触 shard / scheduler / IO.

pub mod binary;
pub mod crypto;
#[cfg(target_os = "linux")]
pub mod http;
#[cfg(target_os = "linux")]
pub mod mysql;
#[cfg(target_os = "linux")]
pub mod pg;
pub mod resp;
pub mod resp_cmd;
#[cfg(target_os = "linux")]
pub mod sql;

use thiserror::Error;

pub use self::binary::BinaryProtocol;
pub use self::resp::{RespCodec, RespCommand, SetAlgOp};

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
/// ⭐ 大 value: 超过存储层 inline 阈值 (~4000B) 的 value 由存储层自动
/// 切溢出页 (13B 描述符入 leaf item), page crate 4096B 编码缓冲只约束
/// inline 路径. value 上限 1MB (溢出单层间接); key 维持 1024B
/// (参与比较/分裂/internal 路由, 不走溢出).
#[derive(Debug, Clone, Copy)]
pub struct KvLimits {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
}

impl Default for KvLimits {
    fn default() -> Self {
        Self {
            max_key_bytes: 1024,
            max_value_bytes: 1024 * 1024,
        }
    }
}

/// 校验请求的 key/value 长度. 超限返回人类可读错误消息 (直接回给 client).
///
/// ⭐ `Request::Put.value` 是 `[type_tag][payload]` 布局 (decode 时预置),
/// 校验按业务 payload 长度扣除 1B tag.
pub fn validate_request(req: &Request, limits: &KvLimits) -> Result<(), String> {
    let (key, value_len): (&[u8], usize) = match req {
        Request::Put { key, value } => (key, value.len().saturating_sub(1)),
        Request::Get { key } | Request::Delete { key } => (key, 0),
    };
    validate_kv(key, value_len, limits)
}

/// ⭐ 借用版校验 (热路径: 免为校验构造 Request / clone key).
/// `value_len` 是**业务 payload 长度** (不含 value type tag 字节).
pub fn validate_kv(key: &[u8], value_len: usize, limits: &KvLimits) -> Result<(), String> {
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
    if value_len > limits.max_value_bytes {
        return Err(format!(
            "value too long: {} > max {}",
            value_len, limits.max_value_bytes
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
