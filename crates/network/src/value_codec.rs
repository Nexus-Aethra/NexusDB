//! Value 类型标签编码层.
//!
//! 多协议数据互联的统一 value 编码约定: 存储格式 = `[type_tag u8][payload]`.
//! 网络门面 (Binary/RESP) 的原始写入打 `TAG_RAW`; ⭐ 数值 RMW (INCR/
//! INCRBYFLOAT) 产出**原生二进制数值** (`TAG_I64`/`TAG_F64`, LE bytes),
//! RESP 读回经 `render` 按 tag 渲染为字符串.
//!
//! **tag 常量与数值编解码的唯一定义源在 `shard_manager::value_num`**
//! (shard 端 RMW 也要用, 而 network 依赖 shard_manager) — 这里 re-export.
//!
//! **作用域**: 仅网络门面写入的数据; 直连 API (submit_tasks) 不经过此层.

pub use shard_manager::value_num::{
    NumValue, TAG_DOC, TAG_F32, TAG_F64, TAG_I64, TAG_RAW, TAG_STR, encode_f32, encode_f64,
    encode_i64, is_known_tag, parse_num, render,
};

/// 编码: 在 payload 前附加 1 字节 type tag.
pub fn encode_value(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + payload.len());
    out.push(tag);
    out.extend_from_slice(payload);
    out
}

/// 解码: 拆出 (tag, payload).
///
/// 容错: 空 value 或首字节不是已知 tag 时, 按 (TAG_RAW, 原样) 返回 —
/// 兼容早期未打 tag 的存量数据 / 直连 API 写入的数据.
///
/// ⭐ 注意: 这是 **Binary 门面**的剥 tag 路径 (返回原生 payload bytes,
/// 数值 key 是 LE 二进制); RESP 门面应使用 `render` (字符串渲染).
pub fn decode_value(stored: &[u8]) -> (u8, &[u8]) {
    match stored.first() {
        Some(&tag) if is_known_tag(tag) => (tag, &stored[1..]),
        _ => (TAG_RAW, stored),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_raw() {
        let payload = b"hello world";
        let stored = encode_value(TAG_RAW, payload);
        assert_eq!(stored.len(), payload.len() + 1);
        let (tag, out) = decode_value(&stored);
        assert_eq!(tag, TAG_RAW);
        assert_eq!(out, payload);
    }

    #[test]
    fn roundtrip_empty_payload() {
        let stored = encode_value(TAG_RAW, b"");
        assert_eq!(stored, vec![TAG_RAW]);
        let (tag, out) = decode_value(&stored);
        assert_eq!(tag, TAG_RAW);
        assert!(out.is_empty());
    }

    #[test]
    fn empty_stored_tolerated() {
        let (tag, out) = decode_value(b"");
        assert_eq!(tag, TAG_RAW);
        assert!(out.is_empty());
    }

    #[test]
    fn unknown_tag_treated_as_raw() {
        // 首字节 0xAB 不是已知 tag → 整体按 raw 返回 (不剥字节)
        let stored = vec![0xABu8, 1, 2, 3];
        let (tag, out) = decode_value(&stored);
        assert_eq!(tag, TAG_RAW);
        assert_eq!(out, &stored[..]);
    }

    #[test]
    fn reserved_tags_recognized() {
        for t in [TAG_I64, TAG_F64, TAG_STR, TAG_DOC] {
            let stored = encode_value(t, b"x");
            let (tag, out) = decode_value(&stored);
            assert_eq!(tag, t);
            assert_eq!(out, b"x");
        }
    }
}
