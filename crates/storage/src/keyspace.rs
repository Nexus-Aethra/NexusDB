//! ⭐ Phase K: 统一 key 命名空间编码.
//!
//! 所有进入 table BTree 的 user key 都编码为
//! `[kind u8][subkind u8?][varint klen][user_key][suffix?]`:
//! - **长度前缀**消除 key/field 拼接的分隔符歧义 (二进制安全, 可含 `\x00`)
//! - **kind 类型字节**隔离 String / Hash / List / Set / ZSet 命名空间, 无跨类型冲突
//! - 一个集合的所有子行共享精确前缀 `[kind][sub][klen][key]`, 在有序 BTree 里
//!   天然连续, 可被前缀范围扫描精确圈选 (`user:1` 不误扫 `user:10`, 因 klen 位先分叉)
//!
//! String 只有单行 `[S][klen][key]` (无 subkind); 复合结构有 meta 行 (subkind 0)
//! + data 行 (subkind 1), ZSet 额外有 score 索引行 (subkind 2).
//!
//! **编码只在存储边界**: 协议层 / 跨 shard 路由仍用裸 (db,table,key), 仅
//! `engine::table_*` 与复合结构 op 往 BTree 读写那层做 encode/decode.

/// 类型字节 (BTree 物理 key 首字节, 隔离命名空间).
pub const KIND_STRING: u8 = b'S';
pub const KIND_HASH: u8 = b'H';
pub const KIND_LIST: u8 = b'L';
pub const KIND_SET: u8 = b'T';
pub const KIND_ZSET: u8 = b'Z';

/// subkind (复合结构第二字节): data 行统一用 SUB_DATA.
pub const SUB_DATA: u8 = 1;
/// ZSet 专属: score→member 有序索引行 (排在 member→score 之后).
pub const SUB_ZSCORE: u8 = 2;

// =====================================================================
// varint (klen 专用; user key <= 1024 → 1~2 字节). page::varint 是私有
// mod 无法复用, 这里内置一个最小实现.
// =====================================================================

fn push_varint(buf: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        buf.push((v as u8 & 0x7F) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

fn read_varint(buf: &[u8]) -> Option<(u32, usize)> {
    let mut v = 0u32;
    let mut shift = 0u32;
    for (i, &b) in buf.iter().enumerate().take(5) {
        v |= ((b & 0x7F) as u32) << shift;
        if b & 0x80 == 0 {
            return Some((v, i + 1));
        }
        shift += 7;
    }
    None
}

// =====================================================================
// String
// =====================================================================

/// String 单值 key: `[S][klen][key]`.
pub fn encode_string(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 2 + key.len());
    out.push(KIND_STRING);
    push_varint(&mut out, key.len() as u32);
    out.extend_from_slice(key);
    out
}

// =====================================================================
// 复合结构通用: meta 行 / data 行 / data 前缀
// =====================================================================

/// ⭐ U1: 统一 per-key 类型 meta 行的物理 key 首字节 (与 kind 无关).
/// 与 S/H/L/T/Z 均不冲突; 保证一个 key 至多一行类型 meta.
pub const TYPE_META: u8 = b'#';

/// ⭐ U1: 统一类型 meta 行: `[#][klen][key]` (每 key 唯一, 与 kind 无关).
/// value = `[kind_byte][count u64 LE]` (List 额外 `[head i64][tail i64]`).
/// 类型检查只需 1 次探测即知 key 是哪种复合类型.
pub fn encode_type_meta(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 2 + key.len());
    out.push(TYPE_META);
    push_varint(&mut out, key.len() as u32);
    out.extend_from_slice(key);
    out
}

/// 全部类型 meta 行的扫描前缀 (recover 计数重建用; 仅 `[#]` 行).
pub fn type_meta_scan_prefix() -> Vec<u8> {
    vec![TYPE_META]
}

/// 解析 `[#][klen][key]` → user key.
pub fn split_type_meta(encoded: &[u8]) -> Option<&[u8]> {
    if encoded.first() != Some(&TYPE_META) {
        return None;
    }
    let rest = &encoded[1..];
    let (klen, n) = read_varint(rest)?;
    let klen = klen as usize;
    let body = &rest[n..];
    if body.len() < klen {
        return None;
    }
    Some(&body[..klen])
}

/// 复合结构 data 行前缀: `[kind][1][klen][key]` (范围扫描的精确前缀).
pub fn data_prefix(kind: u8, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + 2 + key.len());
    out.push(kind);
    out.push(SUB_DATA);
    push_varint(&mut out, key.len() as u32);
    out.extend_from_slice(key);
    out
}

/// 复合结构 data 行: `[kind][1][klen][key][suffix]`
/// (suffix = hash field / set member / list idx / zset member).
pub fn encode_data(kind: u8, key: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut out = data_prefix(kind, key);
    out.extend_from_slice(suffix);
    out
}

/// 解析 data 行 (含 meta 行), 返回 (user_key, suffix).
/// meta 行的 suffix 为空. 用于范围扫描输出剥前缀 (HKEYS/SMEMBERS...).
pub fn split_data(encoded: &[u8]) -> Option<(&[u8], &[u8])> {
    if encoded.len() < 2 {
        return None;
    }
    let rest = &encoded[2..];
    let (klen, n) = read_varint(rest)?;
    let klen = klen as usize;
    let body = &rest[n..];
    if body.len() < klen {
        return None;
    }
    Some((&body[..klen], &body[klen..]))
}

// =====================================================================
// ZSet score 索引: [Z][2][klen][key][score 8B 保序][member]
// =====================================================================

/// ZSet score 索引行: `[Z][2][klen][key][score8][member]`.
pub fn encode_zscore(key: &[u8], score: [u8; 8], member: &[u8]) -> Vec<u8> {
    let mut out = zscore_prefix(key);
    out.extend_from_slice(&score);
    out.extend_from_slice(member);
    out
}

/// ZSet score 索引前缀 `[Z][2][klen][key]` (整个 zset 的 score 有序扫描).
pub fn zscore_prefix(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + 2 + key.len() + 8);
    out.push(KIND_ZSET);
    out.push(SUB_ZSCORE);
    push_varint(&mut out, key.len() as u32);
    out.extend_from_slice(key);
    out
}

/// 解析 score 索引行, 返回 (user_key, score8, member).
pub fn split_zscore(encoded: &[u8]) -> Option<(&[u8], [u8; 8], &[u8])> {
    if encoded.len() < 2 {
        return None;
    }
    let rest = &encoded[2..];
    let (klen, n) = read_varint(rest)?;
    let klen = klen as usize;
    let body = &rest[n..];
    if body.len() < klen + 8 {
        return None;
    }
    let key = &body[..klen];
    let score: [u8; 8] = body[klen..klen + 8].try_into().ok()?;
    let member = &body[klen + 8..];
    Some((key, score, member))
}

// =====================================================================
// 保序数值编码 (字典序 == 数值序)
// =====================================================================

/// f64 → 8B big-endian 保序编码 (ZSet score).
/// 正数翻符号位、负数全翻 → 字节字典序等于数值序. NaN 由 caller 拦截.
pub fn encode_f64_ordered(f: f64) -> [u8; 8] {
    let bits = f.to_bits();
    let mask = if bits & 0x8000_0000_0000_0000 != 0 {
        0xFFFF_FFFF_FFFF_FFFF // 负数: 全翻
    } else {
        0x8000_0000_0000_0000 // 正数/0: 翻符号位
    };
    (bits ^ mask).to_be_bytes()
}

/// 逆变换: 8B 保序编码 → f64.
pub fn decode_f64_ordered(b: [u8; 8]) -> f64 {
    let ordered = u64::from_be_bytes(b);
    let mask = if ordered & 0x8000_0000_0000_0000 != 0 {
        0x8000_0000_0000_0000 // 原正数
    } else {
        0xFFFF_FFFF_FFFF_FFFF // 原负数
    };
    f64::from_bits(ordered ^ mask)
}

/// i64 → 8B big-endian 保序编码 (List 索引; 负 head 排在正 tail 前).
pub fn encode_idx(i: i64) -> [u8; 8] {
    ((i as u64) ^ 0x8000_0000_0000_0000).to_be_bytes()
}

/// 逆变换: 8B → i64.
pub fn decode_idx(b: [u8; 8]) -> i64 {
    (u64::from_be_bytes(b) ^ 0x8000_0000_0000_0000) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_roundtrip_and_kind_isolation() {
        let s = encode_string(b"user:1");
        assert_eq!(s[0], KIND_STRING);
        // 同名 user key 的 String 与统一类型 meta 首字节不同 → 不冲突
        let h = encode_type_meta(b"user:1");
        assert_ne!(s[0], h[0]);
        assert_eq!(split_type_meta(&h), Some(&b"user:1"[..]));
    }

    #[test]
    fn data_split_binary_safe() {
        // key/field 含 \x00 也无歧义 (长度前缀而非分隔符)
        let e1 = encode_data(KIND_HASH, b"a", b"b\x00c");
        let e2 = encode_data(KIND_HASH, b"a\x00b", b"c");
        assert_ne!(e1, e2);
        assert_eq!(split_data(&e1), Some((&b"a"[..], &b"b\x00c"[..])));
        assert_eq!(split_data(&e2), Some((&b"a\x00b"[..], &b"c"[..])));
    }

    #[test]
    fn prefix_containment_no_overreach() {
        // user:1 的 data 前缀不能是 user:10 的 data 行前缀
        let p1 = data_prefix(KIND_HASH, b"user:1");
        let row10 = encode_data(KIND_HASH, b"user:10", b"f");
        assert!(!row10.starts_with(&p1), "user:1 前缀误扫到 user:10");
        let row1 = encode_data(KIND_HASH, b"user:1", b"f");
        assert!(row1.starts_with(&p1));
    }

    #[test]
    fn f64_ordered_monotonic() {
        let vals = [
            f64::NEG_INFINITY,
            -1e300,
            -1.5,
            -0.0,
            0.0,
            1.5,
            1e300,
            f64::INFINITY,
        ];
        let mut prev: Option<[u8; 8]> = None;
        for &v in &vals {
            let e = encode_f64_ordered(v);
            if let Some(p) = prev {
                assert!(p <= e, "score 编码非单调: {v}");
            }
            prev = Some(e);
            // roundtrip (跳过 -0.0/0.0 位模式差异)
            if v != 0.0 {
                assert_eq!(decode_f64_ordered(e), v);
            }
        }
    }

    #[test]
    fn idx_ordered_monotonic() {
        let vals = [i64::MIN, -1000, -1, 0, 1, 1000, i64::MAX];
        let mut prev: Option<[u8; 8]> = None;
        for &v in &vals {
            let e = encode_idx(v);
            if let Some(p) = prev {
                assert!(p < e);
            }
            prev = Some(e);
            assert_eq!(decode_idx(e), v);
        }
    }

    #[test]
    fn zscore_split_roundtrip() {
        let score = encode_f64_ordered(3.5);
        let e = encode_zscore(b"z", score, b"m1");
        assert_eq!(split_zscore(&e), Some((&b"z"[..], score, &b"m1"[..])));
        assert!(e.starts_with(&zscore_prefix(b"z")));
    }
}
