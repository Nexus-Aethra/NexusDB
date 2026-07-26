//! Value 类型标签编码层.
//!
//! 多协议数据互联的统一 value 编码约定: 存储格式 = `[type_tag u8][payload]`.
//! 本阶段所有网络门面 (Binary/RESP) 写入统一打 `TAG_RAW`, 其余 tag 预留给
//! Phase 3 (SQL/Mongo 门面的类型化编码), 现在预留 1 字节避免存量数据迁移.
//!
//! **作用域**: 仅网络门面写入的数据; 直连 API (submit_tasks) 不经过此层.

/// 原始字节 (Redis/Binary 直通).
pub const TAG_RAW: u8 = 0x01;
/// 预留: i64 (保序编码).
pub const TAG_I64: u8 = 0x02;
/// 预留: f64 (保序编码).
pub const TAG_F64: u8 = 0x03;
/// 预留: UTF-8 字符串.
pub const TAG_STR: u8 = 0x04;
/// 预留: 文档 (Mongo BSON 子集 / tuple).
pub const TAG_DOC: u8 = 0x05;

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
pub fn decode_value(stored: &[u8]) -> (u8, &[u8]) {
    match stored.first() {
        Some(&tag) if is_known_tag(tag) => (tag, &stored[1..]),
        _ => (TAG_RAW, stored),
    }
}

/// 首字节是否为已知 type tag.
pub fn is_known_tag(tag: u8) -> bool {
    matches!(tag, TAG_RAW | TAG_I64 | TAG_F64 | TAG_STR | TAG_DOC)
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
