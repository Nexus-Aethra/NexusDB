//! 变长整数 (varint) 编解码.
//!
//! 格式: 每个字节低 7 位存数据, 最高位为 continuation bit (1 = 后续字节).
//! 最多 5 字节编码 u32 (28 位数据).

/// varint 编码所需的最大字节数.
pub const VARINT_MAX_BYTES: usize = 5;

/// 把 u32 编码为 varint, 返回写入字节数.
pub fn encode_varint(buf: &mut [u8], value: u32) -> usize {
    let mut v = value;
    let mut i = 0;
    while v >= 0x80 {
        buf[i] = (v as u8 & 0x7F) | 0x80;
        v >>= 7;
        i += 1;
    }
    buf[i] = v as u8;
    i + 1
}

/// 从 varint 解码 u32, 返回 (value, 读取字节数).
///
/// ⭐ 审计 P3 (2026-10-17): 第 5 字节只容纳 4 个数据位 (前 4 字节已用 28 位).
/// 旧实现无条件 `(b & 0x7F) << 28`: 非规范编码 (第 5 字节高位非零, 即值超出
/// u32) 在 debug 下移位溢出 panic, release 下**静默截断**成错误的值. 现在
/// 显式拒绝 → `None` (调用方按损坏数据处理).
pub fn decode_varint(buf: &[u8]) -> Option<(u32, usize)> {
    let mut v: u32 = 0;
    let mut shift = 0u32;
    for (i, &b) in buf.iter().enumerate().take(VARINT_MAX_BYTES) {
        let payload = b & 0x7F;
        if i == VARINT_MAX_BYTES - 1 && payload & 0x70 != 0 {
            // 第 5 字节高位非零 = 值超过 u32 可表示范围 (非规范编码)
            return None;
        }
        v |= (payload as u32) << shift;
        if b & 0x80 == 0 {
            return Some((v, i + 1));
        }
        shift += 7;
    }
    None
}

/// varint 编码 u32 所需的字节数.
pub fn varint_len(value: u32) -> usize {
    if value < 0x80 {
        1
    } else if value < 0x4000 {
        2
    } else if value < 0x200000 {
        3
    } else if value < 0x10000000 {
        4
    } else {
        5
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ⭐ 审计 P3: 第 5 字节非规范编码必须报错, 不得静默截断.
    #[test]
    fn non_canonical_fifth_byte_rejected() {
        // 5 字节且第 5 字节高位非零 (bit4..6) → 超出 u32, 必须 None
        for &last in &[0x10u8, 0x20, 0x40, 0x70, 0x7F] {
            let buf = [0x80u8, 0x80, 0x80, 0x80, last];
            assert!(
                decode_varint(&buf).is_none(),
                "第 5 字节 {last:#x} 必须被拒绝"
            );
        }
        // 第 5 字节只有低 4 位 → 合法 (28 位值)
        let buf = [0x80u8, 0x80, 0x80, 0x80, 0x0F];
        let (v, n) = decode_varint(&buf).expect("合法 5 字节");
        assert_eq!(n, 5);
        assert_eq!(v, 0x0F << 28);
        // 6 字节 (第 5 字节仍带 continuation) → None
        let buf = [0x80u8, 0x80, 0x80, 0x80, 0x8F];
        assert!(decode_varint(&buf).is_none(), "超过 5 字节必须拒绝");
    }

    #[test]
    fn round_trip_small() {
        let mut buf = [0u8; 5];
        for v in [0u32, 1, 127, 128, 255, 16383, 16384, u32::MAX] {
            let n = encode_varint(&mut buf, v);
            assert_eq!(n, varint_len(v));
            let (decoded, n2) = decode_varint(&buf[..n]).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(n, n2);
        }
    }
}
