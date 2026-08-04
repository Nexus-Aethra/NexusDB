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

/// ⭐ S2: `[S][varint klen][key]` 物理 key → 逻辑 key (全表扫提取 pk 用).
pub fn split_string(pkey: &[u8]) -> Option<&[u8]> {
    if pkey.first() != Some(&KIND_STRING) {
        return None;
    }
    let (klen, n) = read_varint(&pkey[1..])?;
    let start = 1 + n;
    pkey.get(start..start + klen as usize)
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

/// ⭐ Q1 (SQL 索引): 表级 schema 保留行首字节 (与 S/H/L/T/Z/#/I 均不冲突).
/// 每表至多一行, 无 user key 段 — 整个物理 key 就是 `[$]` 单字节.
pub const SCHEMA_ROW: u8 = b'$';

/// schema 行物理 key: `[$]` (表级单行).
pub fn encode_schema_row() -> Vec<u8> {
    vec![SCHEMA_ROW]
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

/// ⭐ F81: i128 → 16B 符号翻转 BE 保序编码 (Decimal 定标整数; memcmp 保序).
pub fn encode_i128_ordered(i: i128) -> [u8; 16] {
    ((i as u128) ^ (1u128 << 127)).to_be_bytes()
}

/// 逆变换: 16B → i128.
pub fn decode_i128_ordered(b: [u8; 16]) -> i128 {
    (u128::from_be_bytes(b) ^ (1u128 << 127)) as i128
}

// =====================================================================
// ⭐ Q3 (SQL 索引): 本地二级索引行 [I][iid u32 BE][memcmp 保序值][PK] → 空值
// =====================================================================
//
// 值段必须 memcmp 保序 (支持范围扫), 因此**不用长度前缀**:
// - 数值列: 1B 型别字节 + 8B 保序编码 (encode_idx / encode_f64_ordered)
// - 字符串/字节列: 1B 型别字节 + 转义编码 (0x00 → 0x00 0xFF) + 终结符 0x00 0x00
//   转义保证任何内嵌 0x00 都排在终结符之后不歧义, 且保持原字节字典序.
// 型别字节保证异型值不混序 (同一 iid 实际只会有单一类型, 防御性设计).

/// 索引行首字节 (与 S/H/L/T/Z/#/$ 均不冲突).
pub const KIND_INDEX: u8 = b'I';

/// ⭐ F65: 全局 UNIQUE 占坑行首字节 (与 S/H/L/T/Z/#/$/I 均不冲突).
/// 行在 email-shard 上 (按 unique 值路由), key = `[U][iid u32 BE][enc_val]`,
/// value = `[state 1B][txn_id u64 LE][pk...]` (state: 1=PENDING / 2=COMMITTED).
pub const KIND_UNIQUE: u8 = b'U';

/// 占坑行 key: `[U][iid u32 BE][enc_val]` (enc_val 同索引值编码).
pub fn unique_slot_key(iid: u32, enc_val: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + enc_val.len());
    out.push(KIND_UNIQUE);
    out.extend_from_slice(&iid.to_be_bytes());
    out.extend_from_slice(enc_val);
    out
}

/// 索引值段的型别字节 (参与排序: 数值 < 字节串).
pub const IVAL_NUM: u8 = 0x01;
pub const IVAL_BYTES: u8 = 0x02;
/// ⭐ F81: Decimal 索引值型别字节 (17B: 型别 + 16B i128 符号翻转 BE 保序).
pub const IVAL_DECIMAL: u8 = 0x03;
/// ⭐ PG 兼容 (FMT_VER 7): 复合索引值型别字节
/// (`[0x04][nseg][u16 len][enc]...` 长度前缀拼接, 防分界歧义).
pub const IVAL_COMPOSITE: u8 = 0x04;

/// 整个 iid 的扫描前缀: `[I][iid u32 BE]`.
pub fn index_prefix(iid: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    out.push(KIND_INDEX);
    out.extend_from_slice(&iid.to_be_bytes());
    out
}

/// 等值扫描前缀: `[I][iid][enc_val]` (enc_val 来自 encode_index_num/bytes).
pub fn index_value_prefix(iid: u32, enc_val: &[u8]) -> Vec<u8> {
    let mut out = index_prefix(iid);
    out.extend_from_slice(enc_val);
    out
}

/// 完整索引行: `[I][iid][enc_val][pk]` (value 为空; 一对多 = 相邻多行).
pub fn encode_index_entry(iid: u32, enc_val: &[u8], pk: &[u8]) -> Vec<u8> {
    let mut out = index_value_prefix(iid, enc_val);
    out.extend_from_slice(pk);
    out
}

/// 数值列索引值: `[IVAL_NUM][8B 保序]` (i64 用 encode_idx, f64 用 encode_f64_ordered).
pub fn encode_index_num(ordered8: [u8; 8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(IVAL_NUM);
    out.extend_from_slice(&ordered8);
    out
}

/// ⭐ F81: Decimal 列索引值: `[IVAL_DECIMAL][16B i128 保序]` (17B 定长).
pub fn encode_index_decimal(ordered16: [u8; 16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(17);
    out.push(IVAL_DECIMAL);
    out.extend_from_slice(&ordered16);
    out
}

/// 字节串列索引值: `[IVAL_BYTES][转义体][0x00 0x00]`.
/// 转义 `0x00 → 0x00 0xFF` 保持字典序且与终结符 `0x00 0x00` 无歧义.
pub fn encode_index_bytes(val: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(3 + val.len());
    out.push(IVAL_BYTES);
    for &b in val {
        out.push(b);
        if b == 0x00 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
    out
}

/// 解析索引行 `[I][iid][enc_val][pk]` → (iid, val_bytes 原值, pk).
/// 值段自定界: 数值定长 9B; 字节串扫描终结符 (转义还原).
pub fn split_index_entry(encoded: &[u8]) -> Option<(u32, Vec<u8>, &[u8])> {
    if encoded.len() < 6 || encoded[0] != KIND_INDEX {
        return None;
    }
    let iid = u32::from_be_bytes(encoded[1..5].try_into().ok()?);
    let (enc_val, pk) = split_index_val(&encoded[5..])?;
    let val = decode_index_val(enc_val)?;
    Some((iid, val, pk))
}

/// 切分值段: `[enc_val 原样(含型别字节)][pk]` → (enc_val, pk).
/// 用于范围扫描时直接以编码形态与界比较 (memcmp 保序, 免解码).
pub fn split_index_val(rest: &[u8]) -> Option<(&[u8], &[u8])> {
    match *rest.first()? {
        IVAL_NUM => {
            if rest.len() < 9 {
                return None;
            }
            Some((&rest[..9], &rest[9..]))
        }
        IVAL_DECIMAL => {
            if rest.len() < 17 {
                return None;
            }
            Some((&rest[..17], &rest[17..]))
        }
        IVAL_BYTES => {
            let body = &rest[1..];
            let mut i = 0usize;
            loop {
                if *body.get(i)? != 0x00 {
                    i += 1;
                    continue;
                }
                match *body.get(i + 1)? {
                    0xFF => i += 2,
                    0x00 => return Some((&rest[..1 + i + 2], &body[i + 2..])),
                    _ => return None,
                }
            }
        }
        // ⭐ PG 兼容 (FMT_VER 7): 复合索引值 `[IVAL_COMPOSITE][nseg][u16 len][enc]...`
        IVAL_COMPOSITE => {
            let nseg = *rest.get(1)? as usize;
            let mut pos = 2usize;
            for _ in 0..nseg {
                if pos + 2 > rest.len() {
                    return None;
                }
                let seg = u16::from_le_bytes(rest[pos..pos + 2].try_into().ok()?) as usize;
                pos += 2;
                if pos + seg > rest.len() {
                    return None;
                }
                pos += seg;
            }
            Some((&rest[..pos], &rest[pos..]))
        }
        _ => None,
    }
}

/// 解码值段 (enc_val 含型别字节) → 原值字节 (数值 = 8B 保序编码, 字节串 = 还原体).
pub fn decode_index_val(enc_val: &[u8]) -> Option<Vec<u8>> {
    match *enc_val.first()? {
        IVAL_NUM => (enc_val.len() == 9).then(|| enc_val[1..9].to_vec()),
        IVAL_DECIMAL => (enc_val.len() == 17).then(|| enc_val[1..17].to_vec()),
        IVAL_BYTES => {
            let body = enc_val.get(1..enc_val.len().checked_sub(2)?)?;
            let mut val = Vec::with_capacity(body.len());
            let mut i = 0usize;
            while i < body.len() {
                let b = body[i];
                val.push(b);
                i += if b == 0x00 { 2 } else { 1 }; // 跳过转义 0xFF
            }
            Some(val)
        }
        _ => None,
    }
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

    // =============== ⭐ Q3: 索引行编码 ===============

    #[test]
    fn index_entry_roundtrip() {
        // 数值
        let ev = encode_index_num(encode_idx(-7));
        let e = encode_index_entry(3, &ev, b"pk1");
        let (iid, val, pk) = split_index_entry(&e).unwrap();
        assert_eq!(iid, 3);
        assert_eq!(decode_idx(val.try_into().unwrap()), -7);
        assert_eq!(pk, b"pk1");
        assert!(e.starts_with(&index_value_prefix(3, &ev)));
        assert!(e.starts_with(&index_prefix(3)));
        // 字节串 (含内嵌 0x00, pk 也含 0x00)
        let ev = encode_index_bytes(b"a\x00b");
        let e = encode_index_entry(9, &ev, b"p\x00k");
        let (iid, val, pk) = split_index_entry(&e).unwrap();
        assert_eq!((iid, val.as_slice(), pk), (9, &b"a\x00b"[..], &b"p\x00k"[..]));
    }

    #[test]
    fn index_bytes_memcmp_order_matches_value_order() {
        // 编码后 memcmp 序 == 原值字典序 (含 0x00 边界情形)
        let vals: Vec<&[u8]> = vec![
            b"", b"\x00", b"\x00\x00", b"\x00\x01", b"a", b"a\x00", b"a\x00x", b"a\x01",
            b"ab", b"b",
        ];
        for w in vals.windows(2) {
            let (lo, hi) = (encode_index_bytes(w[0]), encode_index_bytes(w[1]));
            assert!(lo < hi, "编码破坏序: {:?} vs {:?}", w[0], w[1]);
        }
    }

    #[test]
    fn index_value_prefix_no_cross_value_match() {
        // 等值前缀不误圈其它值的行 ("a" 的前缀不匹配 "a\x00"/"ab" 的行)
        let pa = index_value_prefix(1, &encode_index_bytes(b"a"));
        for other in [&b"a\x00"[..], b"ab", b"a\x01"] {
            let row = encode_index_entry(1, &encode_index_bytes(other), b"pk");
            assert!(!row.starts_with(&pa), "值 {other:?} 被 a 的等值前缀误圈");
        }
        let row_a = encode_index_entry(1, &encode_index_bytes(b"a"), b"pk");
        assert!(row_a.starts_with(&pa));
        // 不同 iid 隔离
        assert!(!row_a.starts_with(&index_prefix(2)));
    }

    #[test]
    fn schema_row_key_isolated() {
        let k = encode_schema_row();
        assert_eq!(k, vec![SCHEMA_ROW]);
        // 与其它 kind 首字节全不冲突
        for b in [KIND_STRING, KIND_HASH, KIND_LIST, KIND_SET, KIND_ZSET, TYPE_META, KIND_INDEX] {
            assert_ne!(k[0], b);
        }
    }
}
