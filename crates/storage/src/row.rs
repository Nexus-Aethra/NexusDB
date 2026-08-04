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
    /// ⭐ F81: 定点小数 (i128 定标整数 + scale). 变长区 16B i128 LE 承载;
    /// scale 随值携带 (读取时由 schema 列 scale 恢复) → 渲染/比较自描述.
    Decimal(i128, u8),
}

/// ⭐ PG 兼容 (FMT_VER 6): ColDefault::Lit 需要 Eq (schema 比较/测试).
/// f64 语义上 NaN 破坏 Eq 定律, 但仅作 trait bound 满足 (无 HashMap key 用途).
impl Eq for ColValue {}

/// ⭐ PG 兼容 (UPDATE SET 表达式): 更新集 — 值 或 表达式 (对旧行求值).
/// 由 worker 把 SQL 表达式翻译成此结构; `row_update` 读旧行后对旧行值求值
/// (引擎单线程天然原子 = CAS 读改写语义).
#[derive(Debug, Clone, PartialEq)]
pub enum SetVal {
    /// 直接赋值.
    Val(ColValue),
    /// 表达式 (旧行上下文求值).
    Expr(RowExpr),
}

/// ⭐ PG 兼容 (UPDATE SET): 行更新表达式树 (v1: 数值算术 + 一元 NOT + 列引用 + 字面量).
#[derive(Debug, Clone, PartialEq)]
pub enum RowExpr {
    /// 字面量.
    Lit(ColValue),
    /// 列引用 (同表当前旧行, 按列号).
    Col(u16),
    /// 一元 NOT (布尔取反: 0→1, 非0→0, NULL→NULL).
    Not(Box<RowExpr>),
    /// 二元算术 (+ - * / %; 数值). Div 除零 → Null; 全整且非 Div → I64, 否则 F64.
    Bin {
        op: RowArith,
        l: Box<RowExpr>,
        r: Box<RowExpr>,
    },
}

/// 二元算术操作符.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowArith {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

/// ⭐ PG 兼容: 对旧行值求值行更新表达式. 未知/类型不符 → Null (与 SQL 语义对齐).
pub fn eval_row_expr(e: &RowExpr, values: &[ColValue]) -> ColValue {
    match e {
        RowExpr::Lit(v) => v.clone(),
        RowExpr::Col(i) => values.get(*i as usize).cloned().unwrap_or(ColValue::Null),
        RowExpr::Not(inner) => match eval_row_expr(inner, values) {
            ColValue::I64(x) => ColValue::I64(if x == 0 { 1 } else { 0 }),
            ColValue::Null => ColValue::Null,
            _ => ColValue::Null,
        },
        RowExpr::Bin { op, l, r } => {
            let (lv, rv) = (eval_row_expr(l, values), eval_row_expr(r, values));
            // 提数: (值, 是否整型); 非数值/NULL → None
            let num = |v: &ColValue| -> Option<(i64, f64, bool)> {
                match v {
                    ColValue::I64(x) => Some((*x, *x as f64, true)),
                    ColValue::F64(x) => Some((0, *x, false)),
                    _ => None,
                }
            };
            let (Some((li, lf, li_is)), Some((ri, rf, ri_is))) = (num(&lv), num(&rv)) else {
                return ColValue::Null;
            };
            let both_int = li_is && ri_is && *op != RowArith::Div;
            if both_int {
                let out = match op {
                    RowArith::Add => li.checked_add(ri),
                    RowArith::Sub => li.checked_sub(ri),
                    RowArith::Mul => li.checked_mul(ri),
                    RowArith::Div => unreachable!(),
                    RowArith::Rem => li.checked_rem(ri),
                };
                return out.map(ColValue::I64).unwrap_or(ColValue::Null);
            }
            let out = match op {
                RowArith::Add => lf + rf,
                RowArith::Sub => lf - rf,
                RowArith::Mul => lf * rf,
                RowArith::Div => {
                    if rf == 0.0 {
                        return ColValue::Null;
                    }
                    lf / rf
                }
                RowArith::Rem => {
                    if rf == 0.0 {
                        return ColValue::Null;
                    }
                    lf % rf
                }
            };
            ColValue::F64(out)
        }
    }
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
            // ⭐ F80: Bool/Date/Time/Timestamp 以 i64 承载 (定长 8B)
            (ColType::Bool | ColType::Date | ColType::Time | ColType::Timestamp, ColValue::I64(_)) => {}
            (ColType::Str | ColType::Bytes | ColType::Json | ColType::Uuid, ColValue::Bytes(b)) => {
                var_total += b.len()
            }
            // ⭐ F81: Decimal 走变长区, 定宽 16B (i128 LE)
            (ColType::Decimal { .. }, ColValue::Decimal(_, _)) => var_total += 16,
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
        match v {
            ColValue::Bytes(b) => off += b.len() as u16,
            ColValue::Decimal(_, _) => off += 16, // ⭐ F81
            _ => {}
        }
    }
    for v in values.iter() {
        match v {
            ColValue::Bytes(b) => out.extend_from_slice(b),
            ColValue::Decimal(x, _) => out.extend_from_slice(&x.to_le_bytes()), // ⭐ F81: 16B i128
            _ => {}
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
    let ci = col as usize;
    if ci >= schema.columns.len() {
        return Err(RowError::ArityMismatch);
    }
    // ⭐ F79: 多版本解码 — 行首部 version 字节决定写入时的列数 n_old.
    // ADD COLUMN 只追加 → 前 n_old 列布局与当前 schema 前 n_old 列一致;
    // 请求列 >= n_old 说明该行写入后才加列 → 补 NULL.
    if bytes.len() < 2 || bytes[0] != TAG_ROW {
        return Err(RowError::Corrupt);
    }
    let rv = bytes[1];
    if rv < 1 || rv > schema.version {
        return Err(RowError::Corrupt);
    }
    let n = schema.col_count_at(rv);
    if ci >= n {
        return Ok(ColValue::Null); // 该行写入时尚无此列
    }
    let bitmap_len = n.div_ceil(8);
    let header = 2 + bitmap_len;
    if bytes.len() < header {
        return Err(RowError::Corrupt);
    }
    let bitmap = &bytes[2..header];
    let is_null = |i: usize| bitmap[i / 8] & (1 << (i % 8)) != 0;
    if is_null(ci) {
        return Ok(ColValue::Null);
    }

    // 定长区大小 = 8 × 非 NULL 定长列数 (按 n_old 列扫 bitmap 推导)
    let mut fixed_before = 0usize; // 目标列之前的非 NULL 定长列数
    let mut fixed_total = 0usize;
    let mut var_before = 0usize; // 目标列之前的变长列数 (含 NULL)
    let mut var_total = 0usize;
    for (i, c) in schema.columns[..n].iter().enumerate() {
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
            // ⭐ F80: Bool/Date/Time/Timestamp 以 i64 承载
            ColType::Bool | ColType::Date | ColType::Time | ColType::Timestamp => {
                ColValue::I64(i64::from_le_bytes(raw))
            }
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
    let slice = bytes.get(data_at + start..data_at + end).ok_or(RowError::Corrupt)?;
    // ⭐ F81: Decimal 变长承载 16B i128; scale 由 schema 列恢复 (自描述值)
    if let ColType::Decimal { scale, .. } = ty {
        let raw: [u8; 16] = slice.try_into().map_err(|_| RowError::Corrupt)?;
        return Ok(ColValue::Decimal(i128::from_le_bytes(raw), scale));
    }
    Ok(ColValue::Bytes(slice.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColType, Column, TableSchema};

    fn schema() -> TableSchema {
        TableSchema::new(
            vec![
                Column { name: "id".into(), ty: ColType::I64, nullable: false, default: None },
                Column { name: "name".into(), ty: ColType::Str, nullable: false, default: None },
                Column { name: "score".into(), ty: ColType::F64, nullable: true, default: None },
                Column { name: "blob".into(), ty: ColType::Bytes, nullable: true, default: None },
                Column { name: "note".into(), ty: ColType::Str, nullable: true, default: None },
            ],
            0,
            &[1, 2],
            &[], &[], &[], &[])
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
        // 版本不符 (超出当前 schema.version)
        let mut bad = bytes.clone();
        bad[1] = 9;
        assert_eq!(read_col(&s, &bad, 0), Err(RowError::Corrupt));
        // 截断
        assert_eq!(read_col(&s, &bytes[..3], 0), Err(RowError::Corrupt));
    }

    /// ⭐ F79: ADD COLUMN 多版本解码 — 旧版本编码的行, 用新 (+列) schema 解码 → 新列读 NULL.
    #[test]
    fn add_column_old_rows_read_null() {
        use crate::schema::Column;
        // 旧 schema: 3 列 (id I64 pk, name Str, age I64)
        let old = TableSchema::new(
            vec![
                Column { name: "id".into(), ty: ColType::I64, nullable: false, default: None },
                Column { name: "name".into(), ty: ColType::Str, nullable: false, default: None },
                Column { name: "age".into(), ty: ColType::I64, nullable: true, default: None },
            ],
            0, &[], &[], &[], &[], &[],
        )
        .unwrap();
        assert_eq!(old.version, 1);
        let old_vals = vec![ColValue::I64(7), ColValue::Bytes(b"bob".to_vec()), ColValue::I64(30)];
        let old_bytes = encode_row(&old, &old_vals).unwrap();

        // ADD COLUMN email Str (nullable) → version 2, 4 列
        let new = old
            .with_added_column(Column { name: "email".into(), ty: ColType::Str, nullable: true, default: None })
            .unwrap();
        assert_eq!(new.version, 2);
        assert_eq!(new.version_ncols, vec![3, 4]);

        // 旧行 (version 1) 用新 schema 解码: 前 3 列正确, 第 4 列 (email) 补 NULL
        let decoded = decode_row(&new, &old_bytes).unwrap();
        assert_eq!(
            decoded,
            vec![
                ColValue::I64(7),
                ColValue::Bytes(b"bob".to_vec()),
                ColValue::I64(30),
                ColValue::Null,
            ]
        );
        assert_eq!(read_col(&new, &old_bytes, 3).unwrap(), ColValue::Null);
        assert_eq!(read_col(&new, &old_bytes, 1).unwrap(), ColValue::Bytes(b"bob".to_vec()));

        // 新行 (version 2, 带 email) 编解码完整
        let new_vals = vec![
            ColValue::I64(8),
            ColValue::Bytes(b"cara".to_vec()),
            ColValue::Null,
            ColValue::Bytes(b"c@x".to_vec()),
        ];
        let new_bytes = encode_row(&new, &new_vals).unwrap();
        assert_eq!(decode_row(&new, &new_bytes).unwrap(), new_vals);

        // schema FMT_VER3 roundtrip 保 version_ncols
        let re = TableSchema::decode(&new.encode()).unwrap();
        assert_eq!(re.version_ncols, vec![3, 4]);
        assert_eq!(re, new);
    }

    /// ⭐ F80: 新类型 (Bool/Date/Time/Timestamp 以 i64 承载; Json/Uuid 以 Bytes 承载)
    /// row 编解码 + 单列读取 roundtrip.
    #[test]
    fn f80_new_types_roundtrip() {
        let s = TableSchema::new(
            vec![
                Column { name: "id".into(), ty: ColType::I64, nullable: false, default: None },
                Column { name: "active".into(), ty: ColType::Bool, nullable: false, default: None },
                Column { name: "d".into(), ty: ColType::Date, nullable: true, default: None },
                Column { name: "t".into(), ty: ColType::Time, nullable: true, default: None },
                Column { name: "ts".into(), ty: ColType::Timestamp, nullable: true, default: None },
                Column { name: "meta".into(), ty: ColType::Json, nullable: true, default: None },
                Column { name: "uid".into(), ty: ColType::Uuid, nullable: true, default: None },
            ],
            0,
            &[],
            &[],
            &[], &[], &[],
        )
        .unwrap();
        let vals = vec![
            ColValue::I64(1),
            ColValue::I64(1),                                // bool true
            ColValue::I64(19_737 * 86_400_000_000),          // 某天 (micros)
            ColValue::I64(37_800_000_000),                   // 10:30:00
            ColValue::I64(19_737 * 86_400_000_000 + 37_800_000_000),
            ColValue::Bytes(br#"{"a":1}"#.to_vec()),
            ColValue::Bytes(vec![0u8; 16]),
        ];
        let bytes = encode_row(&s, &vals).unwrap();
        assert_eq!(decode_row(&s, &bytes).unwrap(), vals);
        // 定长承载列单读
        assert_eq!(read_col(&s, &bytes, 1).unwrap(), ColValue::I64(1));
        assert_eq!(read_col(&s, &bytes, 3).unwrap(), ColValue::I64(37_800_000_000));
        // 变长承载列单读
        assert_eq!(read_col(&s, &bytes, 5).unwrap(), ColValue::Bytes(br#"{"a":1}"#.to_vec()));
        assert_eq!(read_col(&s, &bytes, 6).unwrap(), ColValue::Bytes(vec![0u8; 16]));
        // NULL 时间列
        let vals2 = vec![
            ColValue::I64(2),
            ColValue::I64(0),
            ColValue::Null,
            ColValue::Null,
            ColValue::Null,
            ColValue::Null,
            ColValue::Null,
        ];
        let b2 = encode_row(&s, &vals2).unwrap();
        assert_eq!(decode_row(&s, &b2).unwrap(), vals2);
    }

    /// ⭐ F81: DECIMAL 变长 16B i128 承载 row 编解码 + schema FMT_VER4 roundtrip.
    #[test]
    fn f81_decimal_roundtrip() {
        let s = TableSchema::new(
            vec![
                Column { name: "id".into(), ty: ColType::I64, nullable: false, default: None },
                Column {
                    name: "amt".into(),
                    ty: ColType::Decimal { precision: 18, scale: 2 },
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "note".into(),
                    ty: ColType::Str,
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "big".into(),
                    ty: ColType::Decimal { precision: 38, scale: 4 },
                    nullable: true,
                    default: None,
                },
            ],
            0,
            &[],
            &[],
            &[], &[], &[],
        )
        .unwrap();
        let vals = vec![
            ColValue::I64(1),
            ColValue::Decimal(12345, 2),                    // 123.45
            ColValue::Bytes(b"x".to_vec()),
            ColValue::Decimal(-98765432100000i128, 4),      // 负大数
        ];
        let bytes = encode_row(&s, &vals).unwrap();
        assert_eq!(decode_row(&s, &bytes).unwrap(), vals);
        assert_eq!(read_col(&s, &bytes, 1).unwrap(), ColValue::Decimal(12345, 2));
        assert_eq!(read_col(&s, &bytes, 3).unwrap(), ColValue::Decimal(-98765432100000i128, 4));
        // NULL decimal
        let vals2 = vec![
            ColValue::I64(2),
            ColValue::Decimal(0, 2),
            ColValue::Null,
            ColValue::Null,
        ];
        let b2 = encode_row(&s, &vals2).unwrap();
        assert_eq!(decode_row(&s, &b2).unwrap(), vals2);
        // FMT_VER4: schema 编解码保 precision/scale
        let re = TableSchema::decode(&s.encode()).unwrap();
        assert_eq!(re, s);
        assert_eq!(re.columns[1].ty, ColType::Decimal { precision: 18, scale: 2 });
        assert_eq!(re.columns[3].ty, ColType::Decimal { precision: 38, scale: 4 });
    }
}
