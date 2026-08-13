//! 二进制 codec.
//!
//! Wire format:
//! ```text
//! | total_len: u32 BE | req_id: u64 BE | op: u8 | key_len: u16 BE | val_len: u32 BE | key | val |
//! ```
//!
//! total_len 包含自身 4 字节, 即 `4 + 14 + key_len + val_len`.

use super::{DecodeOutcome, Protocol, ProtocolError, Request, Response};

const HEADER_LEN: usize = 4 + 8 + 1 + 2 + 4; // 19 bytes

pub(crate) const OP_PUT: u8 = 1;
pub(crate) const OP_GET: u8 = 2;
pub(crate) const OP_DELETE: u8 = 3;

pub(crate) const RESP_PUT_OK: u8 = 0x10;
pub(crate) const RESP_GET: u8 = 0x11;
pub(crate) const RESP_DELETE_OK: u8 = 0x12;
pub(crate) const RESP_ERROR: u8 = 0xFF;

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default)]
pub struct BinaryProtocol;

impl BinaryProtocol {
    pub fn new() -> Self {
        Self
    }
}

impl Protocol for BinaryProtocol {
    type Error = ProtocolError;

    fn decode_request(&self, buf: &[u8]) -> Result<DecodeOutcome<Request>, Self::Error> {
        if buf.len() < HEADER_LEN {
            return Ok(DecodeOutcome::NeedMore);
        }
        let total_len = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
        if total_len < HEADER_LEN {
            return Err(ProtocolError::InvalidLength(format!(
                "total_len {total_len} < header {HEADER_LEN}"
            )));
        }
        if total_len > self.max_frame_size() {
            return Err(ProtocolError::FrameTooLarge {
                size: total_len,
                max: self.max_frame_size(),
            });
        }
        if buf.len() < total_len {
            return Ok(DecodeOutcome::NeedMore);
        }
        let req_id = u64::from_be_bytes(buf[4..12].try_into().unwrap());
        let op = buf[12];
        let key_len = u16::from_be_bytes(buf[13..15].try_into().unwrap()) as usize;
        let val_len = u32::from_be_bytes(buf[15..19].try_into().unwrap()) as usize;

        let expected = HEADER_LEN + key_len + val_len;
        if expected != total_len {
            return Err(ProtocolError::InvalidLength(format!(
                "header says {expected} bytes, total_len says {total_len}"
            )));
        }

        let key = buf[HEADER_LEN..HEADER_LEN + key_len].to_vec();
        // ⭐ 热路径优化: Put 的 value 物化时直接预置 1B type tag
        // (`Request::Put.value` 统一 `[TAG_RAW][payload]` 布局,
        // worker 层零二次拷贝; Get/Delete 无 value 不受影响).
        let req = match op {
            OP_PUT => {
                let payload = &buf[HEADER_LEN + key_len..HEADER_LEN + key_len + val_len];
                let mut value = Vec::with_capacity(1 + payload.len());
                value.push(crate::value_codec::TAG_RAW);
                value.extend_from_slice(payload);
                Request::Put { key, value }
            }
            OP_GET => Request::Get { key },
            OP_DELETE => Request::Delete { key },
            other => return Err(ProtocolError::InvalidOpcode(other)),
        };
        let _ = req_id; // req_id 在 frame header 保留, 调用方需要时从 frame 头部单独拿 (TODO: 增强 trait)
        Ok(DecodeOutcome::Complete {
            consumed: total_len,
            value: req,
        })
    }

    fn encode_request(&self, req_id: u64, req: &Request) -> Vec<u8> {
        let (op, key, value) = match req {
            Request::Put { key, value } => (OP_PUT, key.as_slice(), value.as_slice()),
            Request::Get { key } => (OP_GET, key.as_slice(), &[][..]),
            Request::Delete { key } => (OP_DELETE, key.as_slice(), &[][..]),
        };
        let key_len = key.len();
        let val_len = value.len();
        let total_len = HEADER_LEN + key_len + val_len;
        let mut out = Vec::with_capacity(total_len);
        out.extend_from_slice(&(total_len as u32).to_be_bytes());
        out.extend_from_slice(&req_id.to_be_bytes());
        out.push(op);
        out.extend_from_slice(&(key_len as u16).to_be_bytes());
        out.extend_from_slice(&(val_len as u32).to_be_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(value);
        out
    }

    fn decode_response(&self, buf: &[u8]) -> Result<DecodeOutcome<Response>, Self::Error> {
        if buf.len() < HEADER_LEN {
            return Ok(DecodeOutcome::NeedMore);
        }
        let total_len = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
        if total_len > self.max_frame_size() {
            return Err(ProtocolError::FrameTooLarge {
                size: total_len,
                max: self.max_frame_size(),
            });
        }
        if buf.len() < total_len {
            return Ok(DecodeOutcome::NeedMore);
        }
        let op = buf[12];
        let key_len = u16::from_be_bytes(buf[13..15].try_into().unwrap()) as usize;
        let val_len = u32::from_be_bytes(buf[15..19].try_into().unwrap()) as usize;

        let expected = HEADER_LEN + key_len + val_len;
        if expected != total_len {
            return Err(ProtocolError::InvalidLength(format!(
                "header says {expected}, total_len says {total_len}"
            )));
        }

        let resp = match op {
            RESP_PUT_OK => Response::PutOk,
            RESP_DELETE_OK => Response::DeleteOk,
            RESP_GET => {
                let val = buf[HEADER_LEN + key_len..HEADER_LEN + key_len + val_len].to_vec();
                Response::Get(if val.is_empty() { None } else { Some(val) })
            }
            RESP_ERROR => {
                let msg = buf[HEADER_LEN + key_len..HEADER_LEN + key_len + val_len].to_vec();
                let s = String::from_utf8_lossy(&msg).into_owned();
                Response::Error(s)
            }
            other => return Err(ProtocolError::InvalidOpcode(other)),
        };
        Ok(DecodeOutcome::Complete {
            consumed: total_len,
            value: resp,
        })
    }

    fn encode_response(&self, req_id: u64, resp: &Response) -> Vec<u8> {
        let (op, key, value) = match resp {
            Response::PutOk => (RESP_PUT_OK, [].as_slice(), [].as_slice()),
            Response::DeleteOk => (RESP_DELETE_OK, [].as_slice(), [].as_slice()),
            Response::Get(None) => (RESP_GET, [].as_slice(), [].as_slice()),
            Response::Get(Some(v)) => (RESP_GET, [].as_slice(), v.as_slice()),
            Response::Error(msg) => (RESP_ERROR, [].as_slice(), msg.as_bytes()),
        };
        let key_len = key.len();
        let val_len = value.len();
        let total_len = HEADER_LEN + key_len + val_len;
        let mut out = Vec::with_capacity(total_len);
        out.extend_from_slice(&(total_len as u32).to_be_bytes());
        out.extend_from_slice(&req_id.to_be_bytes());
        out.push(op);
        out.extend_from_slice(&(key_len as u16).to_be_bytes());
        out.extend_from_slice(&(val_len as u32).to_be_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(value);
        out
    }

    fn max_frame_size(&self) -> usize {
        MAX_FRAME_BYTES
    }
}
