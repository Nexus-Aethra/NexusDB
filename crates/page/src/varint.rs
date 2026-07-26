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
pub fn decode_varint(buf: &[u8]) -> Option<(u32, usize)> {
    let mut v: u32 = 0;
    let mut shift = 0u32;
    for (i, &b) in buf.iter().enumerate().take(VARINT_MAX_BYTES) {
        v |= ((b & 0x7F) as u32) << shift;
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
