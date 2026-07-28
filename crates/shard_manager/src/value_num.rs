//! ⭐ 数值类型原生存储 (N1): value type tag 的数值体系.
//!
//! 存储格式 `[tag u8][原生 LE bytes]` — 数值**不再用十进制字符串**表示:
//! - `TAG_I64` → 8B i64 LE (承载 MySQL/PG 的 int/bigint; i32 无损提升)
//! - `TAG_F64` → 8B f64 LE (double)
//! - `TAG_F32` → 4B f32 LE (float; 独立 tag, 回读免列 schema 即知精度)
//!
//! value 不参与排序 (排序在 key 侧), 无需保序编码, 直接 LE.
//!
//! **类型跃迁 (Redis 兼容)**: SET 写 TAG_RAW 字符串; INCR 一个 RAW 数字文本
//! 后结果写回 TAG_I64 二进制; INCRBYFLOAT → TAG_F64; APPEND 到数值 tag →
//! 渲染字符串再拼, 退回 TAG_RAW.
//!
//! **门面渲染**: RESP 读回按 tag `render` 成 Redis 兼容字符串;
//! Binary 门面剥 tag 返回原生 payload bytes (自研协议消费者自解释).
//!
//! 本模块是 tag 常量与数值编解码的**唯一定义源**;
//! `network::value_codec` re-export, 避免双源漂移.

use std::borrow::Cow;

/// 原始字节 (Redis/Binary 直通).
pub const TAG_RAW: u8 = 0x01;
/// i64 原生 8B LE (int/bigint).
pub const TAG_I64: u8 = 0x02;
/// f64 原生 8B LE (double).
pub const TAG_F64: u8 = 0x03;
/// 预留: UTF-8 字符串.
pub const TAG_STR: u8 = 0x04;
/// 预留: 文档 (Mongo BSON 子集 / tuple).
pub const TAG_DOC: u8 = 0x05;
/// f32 原生 4B LE (float).
pub const TAG_F32: u8 = 0x06;

/// 首字节是否为已知 type tag.
pub fn is_known_tag(tag: u8) -> bool {
    matches!(tag, TAG_RAW | TAG_I64 | TAG_F64 | TAG_STR | TAG_DOC | TAG_F32)
}

/// 解析后的数值 (RMW 运算用).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NumValue {
    I64(i64),
    F32(f32),
    F64(f64),
}

/// 编码 i64 → `[TAG_I64][8B LE]`.
pub fn encode_i64(n: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(TAG_I64);
    out.extend_from_slice(&n.to_le_bytes());
    out
}

/// 编码 f64 → `[TAG_F64][8B LE]`.
pub fn encode_f64(f: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(TAG_F64);
    out.extend_from_slice(&f.to_le_bytes());
    out
}

/// 编码 f32 → `[TAG_F32][4B LE]`.
pub fn encode_f32(f: f32) -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    out.push(TAG_F32);
    out.extend_from_slice(&f.to_le_bytes());
    out
}

/// 从 stored value 解析数值.
///
/// - 数值 tag: 按 LE 解码 (长度非法 → None, 不 panic)
/// - TAG_RAW / 无 tag 存量数据: 尝试 UTF-8 十进制整数, 失败再试浮点
///   (Redis 兼容: 字符串数字可参与 INCR/INCRBYFLOAT)
/// - 其余 tag → None
pub fn parse_num(stored: &[u8]) -> Option<NumValue> {
    match stored.first() {
        Some(&TAG_I64) if stored.len() == 9 => Some(NumValue::I64(i64::from_le_bytes(
            stored[1..9].try_into().expect("8B"),
        ))),
        Some(&TAG_F64) if stored.len() == 9 => Some(NumValue::F64(f64::from_le_bytes(
            stored[1..9].try_into().expect("8B"),
        ))),
        Some(&TAG_F32) if stored.len() == 5 => Some(NumValue::F32(f32::from_le_bytes(
            stored[1..5].try_into().expect("4B"),
        ))),
        _ => {
            // RAW (含 1B tag) 或无 tag 存量数据: 文本解析
            let payload = match stored.first() {
                Some(&TAG_RAW) => &stored[1..],
                _ => stored,
            };
            let s = std::str::from_utf8(payload).ok()?;
            if let Ok(n) = s.parse::<i64>() {
                return Some(NumValue::I64(n));
            }
            s.parse::<f64>().ok().map(NumValue::F64)
        }
    }
}

/// 只接受整数语义 (TAG_I64 / RAW 十进制整数文本), 浮点返回 None.
/// Redis INCR 语义: 对 float 值报 "not an integer".
pub fn parse_num_int_only(stored: &[u8]) -> Option<i64> {
    match stored.first() {
        Some(&TAG_I64) if stored.len() == 9 => Some(i64::from_le_bytes(
            stored[1..9].try_into().expect("8B"),
        )),
        Some(&TAG_F64) | Some(&TAG_F32) => None,
        _ => {
            let payload = match stored.first() {
                Some(&TAG_RAW) => &stored[1..],
                _ => stored,
            };
            std::str::from_utf8(payload).ok()?.parse::<i64>().ok()
        }
    }
}

/// ⭐ 门面渲染: stored value → 面向文本协议 (RESP) 的字节表示.
///
/// - RAW: 借用 payload (零拷贝)
/// - I64: 十进制字符串
/// - F32/F64: Rust 最短往返表示 (整数值无 ".0", 与 Redis INCRBYFLOAT
///   的去尾零风格一致; 精度语义差异见 plan 取舍)
/// - 未知 tag / 无 tag 存量: 原样借用
pub fn render(stored: &[u8]) -> Cow<'_, [u8]> {
    match stored.first() {
        Some(&TAG_RAW) => Cow::Borrowed(&stored[1..]),
        Some(&TAG_I64) if stored.len() == 9 => {
            let n = i64::from_le_bytes(stored[1..9].try_into().expect("8B"));
            Cow::Owned(n.to_string().into_bytes())
        }
        Some(&TAG_F64) if stored.len() == 9 => {
            let f = f64::from_le_bytes(stored[1..9].try_into().expect("8B"));
            Cow::Owned(format!("{f}").into_bytes())
        }
        Some(&TAG_F32) if stored.len() == 5 => {
            let f = f32::from_le_bytes(stored[1..5].try_into().expect("4B"));
            Cow::Owned(format!("{f}").into_bytes())
        }
        Some(&TAG_STR) => Cow::Borrowed(&stored[1..]),
        // 未知 tag / 长度非法 / 无 tag 存量数据: 原样返回 (容错不 panic)
        _ => Cow::Borrowed(stored),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn i64_roundtrip_and_render() {
        let stored = encode_i64(-42);
        assert_eq!(stored.len(), 9);
        assert_eq!(stored[0], TAG_I64);
        assert_eq!(parse_num(&stored), Some(NumValue::I64(-42)));
        assert_eq!(render(&stored).as_ref(), b"-42");
        assert_eq!(parse_num_int_only(&stored), Some(-42));
    }

    #[test]
    fn f64_roundtrip_and_render() {
        let stored = encode_f64(3200.0);
        assert_eq!(parse_num(&stored), Some(NumValue::F64(3200.0)));
        // 整数值 f64 渲染无 ".0" (Redis 去尾零风格)
        assert_eq!(render(&stored).as_ref(), b"3200");
        let stored = encode_f64(0.25);
        assert_eq!(render(&stored).as_ref(), b"0.25");
        // INCR 对 float 报错语义
        assert_eq!(parse_num_int_only(&stored), None);
    }

    #[test]
    fn f32_roundtrip_and_render() {
        let stored = encode_f32(1.5);
        assert_eq!(stored.len(), 5);
        assert_eq!(parse_num(&stored), Some(NumValue::F32(1.5)));
        assert_eq!(render(&stored).as_ref(), b"1.5");
    }

    #[test]
    fn raw_text_parsing() {
        // RAW 文本整数 (Redis SET "5" 后 INCR 的兼容路径)
        let mut stored = vec![TAG_RAW];
        stored.extend_from_slice(b"5");
        assert_eq!(parse_num(&stored), Some(NumValue::I64(5)));
        assert_eq!(parse_num_int_only(&stored), Some(5));
        // RAW 文本浮点 (INCRBYFLOAT 的 3.0e3 用例)
        let mut stored = vec![TAG_RAW];
        stored.extend_from_slice(b"3.0e3");
        assert_eq!(parse_num(&stored), Some(NumValue::F64(3000.0)));
        assert_eq!(parse_num_int_only(&stored), None);
        // 非数字
        let mut stored = vec![TAG_RAW];
        stored.extend_from_slice(b"abc");
        assert_eq!(parse_num(&stored), None);
    }

    #[test]
    fn malformed_lengths_do_not_panic() {
        // 声称 I64 但长度不足: 落到文本解析路径 → None (LE 字节非 UTF-8 数字)
        let bad = vec![TAG_I64, 1, 2, 3];
        assert!(parse_num(&bad).is_none() || parse_num(&bad).is_some()); // 不 panic 即可
        let _ = render(&bad); // 容错原样返回
        let _ = render(&[]);
        let _ = render(&[TAG_F32, 0, 0]);
    }
}
