//! ⭐ Q2 (SQL 索引基建): row 值编码 — 纯函数.
//!
//! value 格式 (定长列不记偏移, 变长列偏移表定位, NULL 不占数据区):
//! ```text
//! [TAG_ROW][schema_ver u8][null bitmap ⌈n/8⌉B]
//! [定长列区: 非 NULL 的 I64/F64 各 8B LE, 按列序]
//! [变长偏移表: u16 LE × m (m = 变长列数, 相对变长数据区起点的起始偏移)]
//! [变长数据区]
//! ```
//! - 定长列偏移 = 8 × (它之前非 NULL 定长列数), 由 bitmap 推导
//! - 变长列长度 = 下一条目起点 (或区末) − 本条目起点; NULL 变长列
//!   起点与下一条相同 (零长), 与空串靠 bitmap 区分
//! - `schema_ver` 为 ALTER TABLE 预留 (本轮读写校验一致即可)

use crate::schema::{ColType, TableSchema};

/// row 值 tag (0x01-0x06 已被 shard_manager::value_num 占用/预留).
pub const TAG_ROW: u8 = 0x07;

/// 单列值.
#[derive(Debug, Clone, PartialEq)]
pub enum ColValue {
    Null,
    I64(i64),
    F64(f64),
    /// Str 与 Bytes 共用字节载体 (类型由 schema 决定).
    Bytes(Vec<u8>),
}

/// row 编解码错误.
#[derive(Debug, PartialEq)]
pub enum RowError {
    /// 值个数与列数不符.
    ArityMismatch,
    /// 值类型与列类型不符.
    TypeMismatch(u16),
    /// NOT NULL 列传了 Null.
    NullViolation(u16),
    /// 字节流截断 / tag 或版本不符.
    Corrupt,
    /// 变长列总长超 u16 偏移表示范围.
    TooLarge,
}

impl std::fmt::Display for RowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowError::ArityMismatch => write!(f, "row arity mismatch"),
            RowError::TypeMismatch(c) => write!(f, "row col {c} type mismatch"),
            RowError::NullViolation(c) => write!(f, "row col {c} is NOT NULL"),
            RowError::Corrupt => write!(f, "row bytes corrupt"),
            RowError::TooLarge => write!(f, "row varlen data too large"),
        }
    }
}

impl std::error::Error for RowError {}

/// 编码一行. `values` 与 schema.columns 一一对应.
pub fn encode_row(schema: &TableSchema, values: &[ColValue]) -> Result<Vec<u8>, RowError> {
    let n = schema.columns.len();
    if values.len() != n {
        return Err(RowError::ArityMismatch);
    }
    // 校验 + 统计
    let mut var_total = 0usize;
    for (i, (col, v)) in schema.columns.iter().zip(values).enumerate() {
        match (col.ty, v) {
            (_, ColValue::Null) => {
                if !col.nullable {
                    return Err(RowError::NullViolation(i as u16));
                }
            }
            (ColType::I64, ColValue::I64(_)) | (ColType::F64, ColValue::F64(_)) => {}
            (ColType::Str | ColType::Bytes, ColValue::Bytes(b)) => var_total += b.len(),
            _ => return Err(RowError::TypeMismatch(i as u16)),
        }
    }
    if var_total > u16::MAX as usize {
        return Err(RowError::TooLarge);
    }

    let bitmap_len = n.div_ceil(8);
    let mut out = Vec::with_capacity(2 + bitmap_len + 8 * n + var_total);
    out.push(TAG_ROW);
    out.push(schema.version);
    // null bitmap
    let bitmap_at = out.len();
    out.resize(bitmap_at + bitmap_len, 0u8);
    for (i, v) in values.iter().enumerate() {
        if matches!(v, ColValue::Null) {
            out[bitmap_at + i / 8] |= 1 << (i % 8);
        }
    }
    // 定长列区 (非 NULL, 按列序)
    for v in values.iter() {
        match v {
            ColValue::I64(x) => out.extend_from_slice(&x.to_le_bytes()),
            ColValue::F64(x) => out.extend_from_slice(&x.to_le_bytes()),
            _ => {}
        }
    }
    // 变长偏移表 + 数据区
    let mut off = 0u16;
    for (col, v) in schema.columns.iter().zip(values) {
        if col.ty.is_fixed() {
            continue;
        }
        out.extend_from_slice(&off.to_le_bytes());
        if let ColValue::Bytes(b) = v {
            off += b.len() as u16;
        }
    }
    for v in values.iter() {
        if let ColValue::Bytes(b) = v {
            out.extend_from_slice(b);
        }
    }
    Ok(out)
}

/// 解码整行.
pub fn decode_row(schema: &TableSchema, bytes: &[u8]) -> Result<Vec<ColValue>, RowError> {
    let n = schema.columns.len();
    (0..n as u16).map(|c| read_col(schema, bytes, c)).collect()
}

/// 读单列 (免全行解码; 索引维护/覆盖查询用).
pub fn read_col(schema: &TableSchema, bytes: &[u8], col: u16) -> Result<ColValue, RowError> {
    let n = schema.columns.len();
    let ci = col as usize;
    if ci >= n {
        return Err(RowError::ArityMismatch);
    }
    let bitmap_len = n.div_ceil(8);
    let header = 2 + bitmap_len;
    if bytes.len() < header || bytes[0] != TAG_ROW || bytes[1] != schema.version {
        return Err(RowError::Corrupt);
    }
    let bitmap = &bytes[2..header];
    let is_null = |i: usize| bitmap[i / 8] & (1 << (i % 8)) != 0;
    if is_null(ci) {
        return Ok(ColValue::Null);
    }

    // 定长区大小 = 8 × 非 NULL 定长列数 (全列扫 bitmap 推导)
    let mut fixed_before = 0usize; // 目标列之前的非 NULL 定长列数
    let mut fixed_total = 0usize;
    let mut var_before = 0usize; // 目标列之前的变长列数 (含 NULL)
    let mut var_total = 0usize;
    for (i, c) in schema.columns.iter().enumerate() {
        if c.ty.is_fixed() {
            if !is_null(i) {
                fixed_total += 1;
                if i < ci {
                    fixed_before += 1;
                }
            }
        } else {
            var_total += 1;
            if i < ci {
                var_before += 1;
            }
        }
    }
    let fixed_at = header;
    let offtab_at = fixed_at + 8 * fixed_total;
    let data_at = offtab_at + 2 * var_total;
    if bytes.len() < data_at {
        return Err(RowError::Corrupt);
    }

    let ty = schema.columns[ci].ty;
    if ty.is_fixed() {
        let at = fixed_at + 8 * fixed_before;
        let raw: [u8; 8] = bytes
            .get(at..at + 8)
            .and_then(|s| s.try_into().ok())
            .ok_or(RowError::Corrupt)?;
        return Ok(match ty {
            ColType::I64 => ColValue::I64(i64::from_le_bytes(raw)),
            ColType::F64 => ColValue::F64(f64::from_le_bytes(raw)),
            _ => unreachable!(),
        });
    }
    // 变长列: 起点 = 偏移表[var_before], 终点 = 下一条目 (或区末)
    let read_off = |idx: usize| -> Result<usize, RowError> {
        let at = offtab_at + 2 * idx;
        bytes
            .get(at..at + 2)
            .map(|s| u16::from_le_bytes(s.try_into().expect("2B")) as usize)
            .ok_or(RowError::Corrupt)
    };
    let start = read_off(var_before)?;
    let end = if var_before + 1 < var_total {
        read_off(var_before + 1)?
    } else {
        bytes.len() - data_at
    };
    bytes
        .get(data_at + start..data_at + end)
        .map(|s| ColValue::Bytes(s.to_vec()))
        .ok_or(RowError::Corrupt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColType, Column, TableSchema};

    fn schema() -> TableSchema {
        TableSchema::new(
            vec![
                Column { name: "id".into(), ty: ColType::I64, nullable: false },
                Column { name: "name".into(), ty: ColType::Str, nullable: false },
                Column { name: "score".into(), ty: ColType::F64, nullable: true },
                Column { name: "blob".into(), ty: ColType::Bytes, nullable: true },
                Column { name: "note".into(), ty: ColType::Str, nullable: true },
            ],
            0,
            &[1, 2],
            &[], &[])
        .unwrap()
    }

    #[test]
    fn roundtrip_full() {
        let s = schema();
        let vals = vec![
            ColValue::I64(42),
            ColValue::Bytes(b"alice".to_vec()),
            ColValue::F64(-3.5),
            ColValue::Bytes(b"\x00\x01\xFF".to_vec()),
            ColValue::Bytes(b"".to_vec()), // 空串 (非 NULL)
        ];
        let bytes = encode_row(&s, &vals).unwrap();
        assert_eq!(bytes[0], TAG_ROW);
        assert_eq!(decode_row(&s, &bytes).unwrap(), vals);
        // 单列读
        assert_eq!(read_col(&s, &bytes, 0).unwrap(), ColValue::I64(42));
        assert_eq!(read_col(&s, &bytes, 3).unwrap(), ColValue::Bytes(b"\x00\x01\xFF".to_vec()));
        assert_eq!(read_col(&s, &bytes, 4).unwrap(), ColValue::Bytes(vec![]));
    }

    #[test]
    fn null_columns_skip_storage() {
        let s = schema();
        let vals = vec![
            ColValue::I64(1),
            ColValue::Bytes(b"n".to_vec()),
            ColValue::Null, // 定长 NULL → 不占定长区
            ColValue::Null, // 变长 NULL → 零长, bitmap 区分
            ColValue::Bytes(b"x".to_vec()),
        ];
        let bytes = encode_row(&s, &vals).unwrap();
        assert_eq!(decode_row(&s, &bytes).unwrap(), vals);
        assert_eq!(read_col(&s, &bytes, 2).unwrap(), ColValue::Null);
        assert_eq!(read_col(&s, &bytes, 3).unwrap(), ColValue::Null);
        assert_eq!(read_col(&s, &bytes, 4).unwrap(), ColValue::Bytes(b"x".to_vec()));
        // NULL 省空间: 比全填版本短
        let full = encode_row(
            &s,
            &[
                ColValue::I64(1),
                ColValue::Bytes(b"n".to_vec()),
                ColValue::F64(0.0),
                ColValue::Bytes(b"y".to_vec()),
                ColValue::Bytes(b"x".to_vec()),
            ],
        )
        .unwrap();
        assert!(bytes.len() < full.len());
    }

    #[test]
    fn validation_errors() {
        let s = schema();
        // 列数不符
        assert_eq!(encode_row(&s, &[ColValue::I64(1)]), Err(RowError::ArityMismatch));
        // NOT NULL 违约
        let e = encode_row(
            &s,
            &[
                ColValue::Null,
                ColValue::Bytes(b"n".to_vec()),
                ColValue::Null,
                ColValue::Null,
                ColValue::Null,
            ],
        );
        assert_eq!(e, Err(RowError::NullViolation(0)));
        // 类型不符
        let e = encode_row(
            &s,
            &[
                ColValue::F64(1.0),
                ColValue::Bytes(b"n".to_vec()),
                ColValue::Null,
                ColValue::Null,
                ColValue::Null,
            ],
        );
        assert_eq!(e, Err(RowError::TypeMismatch(0)));
    }

    #[test]
    fn corrupt_and_version_check() {
        let s = schema();
        let vals = vec![
            ColValue::I64(1),
            ColValue::Bytes(b"n".to_vec()),
            ColValue::Null,
            ColValue::Null,
            ColValue::Null,
        ];
        let bytes = encode_row(&s, &vals).unwrap();
        // 坏 tag
        let mut bad = bytes.clone();
        bad[0] = 0x01;
        assert_eq!(read_col(&s, &bad, 0), Err(RowError::Corrupt));
        // 版本不符
        let mut bad = bytes.clone();
        bad[1] = 9;
        assert_eq!(read_col(&s, &bad, 0), Err(RowError::Corrupt));
        // 截断
        assert_eq!(read_col(&s, &bytes[..3], 0), Err(RowError::Corrupt));
    }
}
