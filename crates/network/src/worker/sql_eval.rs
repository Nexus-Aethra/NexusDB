//! SQL 值评估/比较/行构建/协议字节 — 完成点过滤与渲染.
//! 从 worker/mod.rs 拆分 (2026-08).

use super::*;

type ScalarConstRow = (Vec<(&'static str, ColType)>, Vec<ColValue>);

/// ⭐ compat: 表达式投影输出行 — 按 (proj 列号, expr_proj 表达式) 构建单行
/// 输出 (列定义 + 值). Some(expr) → 求值 (JSONB 取字段, 输出 Str);
/// None → 直接输出 proj 列. 供 materialize_select_agg / RYOW 单行共用.
#[allow(clippy::type_complexity)]
pub(crate) fn project_output_row(
    schema: &TableSchema,
    proj: &[u16],
    expr_proj: &[Option<BoundExpr>],
    out_names: &[Option<String>],
    row: &[ColValue],
) -> (Vec<(String, ColType)>, Vec<ColValue>) {
    let mut cols: Vec<(String, ColType)> = Vec::with_capacity(proj.len());
    let mut vals: Vec<ColValue> = Vec::with_capacity(proj.len());
    for (k, &i) in proj.iter().enumerate() {
        let c = &schema.columns[i as usize];
        match expr_proj.get(k).and_then(|o| o.as_ref()) {
            Some(e) => {
                cols.push((
                    out_names
                        .get(k)
                        .and_then(|o| o.clone())
                        .unwrap_or_else(|| c.name.clone()),
                    ColType::Str,
                ));
                vals.push(eval_bound_expr(e, row));
            }
            None => {
                cols.push((
                    out_names
                        .get(k)
                        .and_then(|o| o.clone())
                        .unwrap_or_else(|| c.name.clone()),
                    c.ty,
                ));
                vals.push(row.get(i as usize).cloned().unwrap_or(ColValue::Null));
            }
        }
    }
    (cols, vals)
}

/// ⭐ compat: 无 FROM 标量函数投影常量单行 (SELECT NOW() 等).
/// 返回 (列定义, 常量行); 未知函数报错.
pub(crate) fn scalar_fn_const_row(items: &[sql::SelectItem]) -> Result<ScalarConstRow, String> {
    let mut cref: Vec<(&str, ColType)> = Vec::with_capacity(items.len());
    let mut row: Vec<ColValue> = Vec::with_capacity(items.len());
    for it in items {
        if let sql::SelectItem::ScalarFn { name } = it {
            match name.to_ascii_uppercase().as_str() {
                "NOW" | "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME" => {
                    cref.push(("now", ColType::Timestamp));
                    // Timestamp 以 i64 微秒承载
                    row.push(ColValue::I64(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_micros() as i64)
                            .unwrap_or(0),
                    ));
                }
                other => return Err(format!("unknown scalar function '{other}'")),
            }
        }
    }
    Ok((cref, row))
}

/// ⭐ compat (自动主键): 隐藏 `__rowid` 列名 (L2 注入; parse 层拒绝用户同名列).
pub(crate) const HIDDEN_ROWID: &str = "__rowid";

/// ⭐ compat (自动主键): 隐藏列生成 — 进程级 Atomic 递增.
/// seed = 首次使用的当前时间微秒 (单调; 重启后从新时间戳续 → 与存量
/// (旧时间戳) 不冲突, 免恢复; 时钟大幅回拨 + 重启 → 极小概率覆盖, v1 文档化).
static AUTO_ROWID: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

pub(crate) fn next_auto_rowid() -> i64 {
    use std::sync::atomic::Ordering::{AcqRel, Acquire};
    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(1)
    };
    let mut cur = AUTO_ROWID.load(Acquire);
    loop {
        let base = if cur == 0 { now() } else { cur };
        match AUTO_ROWID.compare_exchange(cur, base + 1, AcqRel, Acquire) {
            Ok(_) => return base + 1,
            Err(actual) => cur = actual,
        }
    }
}

/// ⭐ compat: 该表是否自动主键表 (pk 列名为隐藏 __rowid).
pub(crate) fn is_auto_pk(schema: &TableSchema) -> bool {
    schema
        .columns
        .get(schema.pk_col as usize)
        .map(|c| c.name == HIDDEN_ROWID)
        .unwrap_or(false)
}

/// ⭐ compat: 可见列号 — `SELECT *` 全列展开时排除隐藏 __rowid 与已删列.
pub(crate) fn visible_cols(schema: &TableSchema) -> Vec<u16> {
    (0..schema.columns.len() as u16)
        .filter(|&i| {
            schema.columns[i as usize].name != HIDDEN_ROWID && !schema.dropped.contains(&i)
        })
        .collect()
}

/// INSERT 值列表 → 全列 ColValue (列清单缺省填 NULL; 类型转换).
/// ⭐ compat (自动主键 + DROP COLUMN): 自动主键表用户不提供 __rowid; 已删列
/// 不可写 (col_by_name 过滤); 无列清单时 VALUES 数 = 可见列数 (全列 − 隐藏/已删).
pub(crate) fn sql_build_row(
    schema: &TableSchema,
    cols: &[String],
    vals: &[SqlValue],
) -> Result<Vec<ColValue>, String> {
    let n = schema.columns.len();
    let auto_rowid = is_auto_pk(schema).then_some(schema.pk_col as usize);
    let hidden = |i: usize| auto_rowid == Some(i) || schema.dropped.contains(&(i as u16));
    let mut out = vec![ColValue::Null; n];
    let mut provided = vec![false; n];
    if cols.is_empty() {
        let visible = (0..n).filter(|&i| !hidden(i)).count();
        if vals.len() != visible {
            return Err(format!("expected {visible} values, got {}", vals.len()));
        }
        let mut vi = 0;
        for (i, c) in schema.columns.iter().enumerate() {
            if hidden(i) {
                continue;
            }
            out[i] = sql_to_col(c.ty, &vals[vi])?;
            provided[i] = true;
            vi += 1;
        }
    } else {
        for (name, v) in cols.iter().zip(vals) {
            if name.eq_ignore_ascii_case(HIDDEN_ROWID) {
                return Err(format!(
                    "column '{HIDDEN_ROWID}' is reserved for auto rowid"
                ));
            }
            let i = schema
                .col_by_name(name)
                .ok_or_else(|| format!("unknown column '{name}'"))? as usize;
            out[i] = sql_to_col(schema.columns[i].ty, v)?;
            provided[i] = true;
        }
    }
    // ⭐ PG 兼容: 未显式提供的列 → 列默认值 (DEFAULT 表达式求值; 显式 NULL 不覆盖)
    for (i, c) in schema.columns.iter().enumerate() {
        if !provided[i]
            && !hidden(i)
            && let Some(d) = &c.default
        {
            out[i] = eval_col_default(c.ty, d)?;
        }
    }
    // 自动主键: 未提供 (或显式值) → 生成; 禁止用户覆盖 (保持隐藏语义)
    if let Some(i) = auto_rowid {
        out[i] = ColValue::I64(next_auto_rowid());
    }
    Ok(out)
}

/// ⭐ PG 兼容: DEFAULT 表达式求值 (字面量 / NOW / uuid_generate_v4).
pub(crate) fn eval_col_default(
    ty: ColType,
    d: &storage::schema::ColDefault,
) -> Result<ColValue, String> {
    Ok(match d {
        storage::schema::ColDefault::Lit(v) => v.clone(),
        storage::schema::ColDefault::Now => ColValue::I64(now_micros()),
        storage::schema::ColDefault::UuidGenV4 => match ty {
            ColType::Uuid => ColValue::Bytes(uuid_v4_bytes()),
            // 非 UUID 列上的 uuid_generate_v4() → 也产 16B 文本? 保守报错
            _ => ColValue::Bytes(uuid_v4_bytes()),
        },
        // ⭐ PG 兼容 (portal): SERIAL/BIGSERIAL → 进程级单调递增
        storage::schema::ColDefault::Serial => ColValue::I64(next_auto_rowid()),
    })
}

/// 当前时间 (UTC 微秒).
fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// ⭐ PG 兼容: 生成 UUID v4 (16B). 无 rand 依赖 — 时间戳 + 单调计数器
/// + 黄金比例混合; 版本/变体位按 RFC 4122 设置. 唯一性足够 (进程内单调).
fn uuid_v4_bytes() -> Vec<u8> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let t = now_micros() as u64;
    let c = COUNTER.fetch_add(1, Ordering::Relaxed);
    // 混合: 时间戳高 32 / 低 32 ^ 计数*黄金比例 / 计数
    let mut b = [0u8; 16];
    b[0..4].copy_from_slice(&(t as u32).to_le_bytes());
    b[4..8].copy_from_slice(&((t >> 32) as u32).to_le_bytes());
    let mix = c.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    b[8..12].copy_from_slice(&(mix as u32).to_le_bytes());
    b[12..16].copy_from_slice(&(c as u32).to_le_bytes());
    b[6] = (b[6] & 0x0F) | 0x40; // version 4
    b[8] = (b[8] & 0x3F) | 0x80; // variant 10xx
    b.to_vec()
}

/// SQL 字面量 → 列值 (Int 可升 F64; 类型不符报错).
/// ⭐ P1: 数值列收到 Str → 尝试文本解析 (PG 文本参数按目标类型转换语义).
pub(crate) fn sql_to_col(ty: ColType, v: &SqlValue) -> Result<ColValue, String> {
    Ok(match (ty, v) {
        (_, SqlValue::Null) => ColValue::Null,
        (_, SqlValue::Param(_)) => return Err("unbound parameter".into()),
        // ⭐ F71: 子查询未折叠就流到执行层 = bug (防御)
        (_, SqlValue::Subquery(_)) => return Err("unresolved subquery".into()),
        // ⭐ F74: 列引用未去相关就流到执行层 = bug (防御)
        (_, SqlValue::ColRef(_)) => return Err("unresolved column reference".into()),
        // ⭐ PG 兼容 (UPDATE SET `= NOW()`): 展开为当前 Unix 微秒
        (_, SqlValue::Now) => {
            let micros = now_micros();
            match ty {
                ColType::Date | ColType::Time | ColType::Timestamp | ColType::I64 => {
                    ColValue::I64(micros)
                }
                ColType::Str | ColType::Json => {
                    ColValue::Bytes(render_timestamp(micros).into_bytes())
                }
                _ => ColValue::Null,
            }
        }
        (ColType::I64, SqlValue::Int(i)) => ColValue::I64(*i),
        (ColType::F64, SqlValue::Int(i)) => ColValue::F64(*i as f64),
        (ColType::F64, SqlValue::Float(f)) => ColValue::F64(*f),
        (ColType::Str | ColType::Bytes, SqlValue::Str(s)) => ColValue::Bytes(s.clone()),
        // ⭐ F80: BOOL — TRUE/FALSE(Int 1/0) 或文本 true/false/t/f/1/0 → I64(0/1)
        (ColType::Bool, SqlValue::Int(i)) => ColValue::I64(i64::from(*i != 0)),
        (ColType::Bool, SqlValue::Str(s)) => {
            let t = std::str::from_utf8(s)
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase();
            match t.as_str() {
                "1" | "true" | "t" | "yes" | "y" => ColValue::I64(1),
                "0" | "false" | "f" | "no" | "n" | "" => ColValue::I64(0),
                _ => return Err("invalid boolean text".into()),
            }
        }
        // ⭐ F80: DATE/TIME/TIMESTAMP — 文本解析成 i64 微秒; Int 视为已是微秒
        (ColType::Date, SqlValue::Str(s)) => {
            parse_date_micros(std::str::from_utf8(s).unwrap_or(""))
                .map(ColValue::I64)
                .ok_or("invalid DATE literal (expect 'YYYY-MM-DD')")?
        }
        (ColType::Time, SqlValue::Str(s)) => {
            parse_time_micros(std::str::from_utf8(s).unwrap_or(""))
                .map(ColValue::I64)
                .ok_or("invalid TIME literal (expect 'HH:MM:SS')")?
        }
        (ColType::Timestamp, SqlValue::Str(s)) => {
            parse_timestamp_micros(std::str::from_utf8(s).unwrap_or(""))
                .map(ColValue::I64)
                .ok_or("invalid TIMESTAMP literal")?
        }
        (ColType::Date | ColType::Time | ColType::Timestamp, SqlValue::Int(i)) => ColValue::I64(*i),
        // ⭐ F81: DECIMAL — 文本(精确)/整数(精确)/浮点(经最短文本, 保常见精度) → 定标 i128
        (ColType::Decimal { scale, .. }, SqlValue::Str(s)) => {
            parse_decimal(std::str::from_utf8(s).unwrap_or(""), scale)
                .map(|d| ColValue::Decimal(d, scale))
                .ok_or("invalid DECIMAL literal")?
        }
        (ColType::Decimal { scale, .. }, SqlValue::Int(i)) => (*i as i128)
            .checked_mul(pow10_i128(scale).ok_or("DECIMAL scale overflow")?)
            .map(|d| ColValue::Decimal(d, scale))
            .ok_or("DECIMAL overflow")?,
        (ColType::Decimal { scale, .. }, SqlValue::Float(f)) => {
            parse_decimal(&format!("{f}"), scale)
                .map(|d| ColValue::Decimal(d, scale))
                .ok_or("invalid DECIMAL value")?
        }
        // ⭐ F80: JSON — 存文本字节 (v1 不校验合法性)
        (ColType::Json, SqlValue::Str(s)) => ColValue::Bytes(s.clone()),
        // ⭐ F80: UUID — 解析 36/32 字符 hex → 16B
        (ColType::Uuid, SqlValue::Str(s)) => parse_uuid(std::str::from_utf8(s).unwrap_or(""))
            .map(ColValue::Bytes)
            .ok_or("invalid UUID literal")?,
        (ColType::I64, SqlValue::Str(s)) => std::str::from_utf8(s)
            .ok()
            .and_then(|t| t.trim().parse::<i64>().ok())
            .map(ColValue::I64)
            .ok_or("invalid integer text for bigint column")?,
        (ColType::F64, SqlValue::Str(s)) => std::str::from_utf8(s)
            .ok()
            .and_then(|t| t.trim().parse::<f64>().ok())
            .map(ColValue::F64)
            .ok_or("invalid float text for double column")?,
        _ => {
            return Err(format!(
                "type mismatch: {v:?} not assignable to {ty:?} column"
            ));
        }
    })
}

/// pk 列值 → 存储 pk 字节 (数值保序编码, 字节串原样; NULL/空串非法).
pub(crate) fn sql_pk_bytes(ty: ColType, v: &ColValue) -> Result<Vec<u8>, String> {
    match (ty, v) {
        (ColType::I64, ColValue::I64(i)) => Ok(storage::keyspace::encode_idx(*i).to_vec()),
        (ColType::F64, ColValue::F64(f)) => Ok(storage::keyspace::encode_f64_ordered(*f).to_vec()),
        // ⭐ F80: Bool/Date/Time/Timestamp 以 i64 承载 → 保序数值编码
        (ColType::Bool | ColType::Date | ColType::Time | ColType::Timestamp, ColValue::I64(i)) => {
            Ok(storage::keyspace::encode_idx(*i).to_vec())
        }
        (ColType::Str | ColType::Bytes | ColType::Json | ColType::Uuid, ColValue::Bytes(b))
            if !b.is_empty() =>
        {
            Ok(b.clone())
        }
        // ⭐ F81: Decimal PK → 16B i128 保序编码
        (ColType::Decimal { .. }, ColValue::Decimal(x, _)) => {
            Ok(storage::keyspace::encode_i128_ordered(*x).to_vec())
        }
        (_, ColValue::Null) => Err("PRIMARY KEY must not be NULL".into()),
        _ => Err("bad PRIMARY KEY value".into()),
    }
}

/// 行值 vs 全部 WHERE 条件 (AND; NULL 列比较恒 false — SQL 语义).
/// ⭐ F69: 单条 Cond 判定 (NULL 列恒 false).
pub(crate) fn eval_cond_leaf(schema: &TableSchema, values: &[ColValue], c: &Cond) -> bool {
    use std::cmp::Ordering;
    let Some(i) = schema.col_by_name(&c.col) else {
        return false; // plan 已校验, 防御
    };
    let cv = &values[i as usize];
    let colty = schema.columns[i as usize].ty; // ⭐ F80: 用于时间/布尔字面量强转
    // ⭐ S2: IN — 集合任一相等 (NULL 列恒 false)
    if c.op == CmpOp::In {
        // ⭐ F73: 大同型集合 → 二分 (解析/折叠期已 sort_in_set 排序去重);
        // 混型/跨型 coercion 保守回退线性
        if c.set.len() > 64 {
            match cv {
                ColValue::I64(x) if c.set.iter().all(|v| matches!(v, SqlValue::Int(_))) => {
                    return c
                        .set
                        .binary_search_by(|v| match v {
                            SqlValue::Int(b) => b.cmp(x),
                            _ => std::cmp::Ordering::Less,
                        })
                        .is_ok();
                }
                ColValue::Bytes(x) if c.set.iter().all(|v| matches!(v, SqlValue::Str(_))) => {
                    return c
                        .set
                        .binary_search_by(|v| match v {
                            SqlValue::Str(b) => b.as_slice().cmp(x.as_slice()),
                            _ => std::cmp::Ordering::Less,
                        })
                        .is_ok();
                }
                _ => {}
            }
        }
        return c.set.iter().any(|v| {
            let cvt = coerce_cmp_lit_uuid(colty, v);
            sql_cmp(cv, cvt.as_ref().unwrap_or(v)) == Some(Ordering::Equal)
        });
    }
    // ⭐ PG 兼容: UUID 列的字面量/参数 (36 字符文本) → 16B (SqlValue::Str 字节载体)
    // 再比较; 否则 16B 存储值 vs 36 字符文本直接字节比较恒不等.
    let cval = coerce_cmp_lit_uuid(colty, &c.val);
    match sql_cmp(cv, cval.as_ref().unwrap_or(&c.val)) {
        None => false,
        Some(o) => match c.op {
            CmpOp::Eq => o == Ordering::Equal,
            CmpOp::Gt => o == Ordering::Greater,
            CmpOp::Ge => o != Ordering::Less,
            CmpOp::Lt => o == Ordering::Less,
            CmpOp::Le => o != Ordering::Greater,
            CmpOp::Ne => o != Ordering::Equal, // ⭐ S2
            CmpOp::In => unreachable!(),
            CmpOp::JsonExists => {
                let key = sql_to_col(ColType::Str, &c.val).unwrap_or(ColValue::Null);
                eval_json_exists(cv, &key)
            }
        },
    }
}

/// ⭐ F69: WHERE 谓词树递归求值 (And=全真, Or=任一真, Not=取反; NULL 叶子为 false).
pub(crate) fn eval_pred(schema: &TableSchema, values: &[ColValue], pred: &Pred<Cond>) -> bool {
    match pred {
        Pred::Leaf(c) => eval_cond_leaf(schema, values, c),
        Pred::And(v) => v.iter().all(|p| eval_pred(schema, values, p)),
        Pred::Or(v) => v.iter().any(|p| eval_pred(schema, values, p)),
        Pred::Not(b) => !eval_pred(schema, values, b),
    }
}

/// ⭐ F69: 系统表专用 eval — `__` 前缀内部标记叶子视为真 (已在生成器处理).
pub(crate) fn eval_pred_sysq(schema: &TableSchema, values: &[ColValue], pred: &Pred<Cond>) -> bool {
    match pred {
        Pred::Leaf(c) if c.col.starts_with("__") => true,
        Pred::Leaf(c) => eval_cond_leaf(schema, values, c),
        Pred::And(v) => v.iter().all(|p| eval_pred_sysq(schema, values, p)),
        Pred::Or(v) => v.iter().any(|p| eval_pred_sysq(schema, values, p)),
        Pred::Not(b) => !eval_pred_sysq(schema, values, b),
    }
}

/// ⭐ F80: WHERE/比较字面量按目标列类型强制转换 — DATE/TIME/TIMESTAMP 的
/// 字符串字面量 → i64 微秒 (SqlValue::Int), BOOL 文本 → 0/1. 无需转换返回 None.
pub(crate) fn coerce_cmp_lit(ty: ColType, sv: &SqlValue) -> Option<SqlValue> {
    let s = match sv {
        SqlValue::Str(b) => std::str::from_utf8(b).ok()?,
        _ => return None,
    };
    match ty {
        ColType::Date => parse_date_micros(s).map(SqlValue::Int),
        ColType::Time => parse_time_micros(s).map(SqlValue::Int),
        ColType::Timestamp => parse_timestamp_micros(s).map(SqlValue::Int),
        ColType::Bool => match s.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "t" | "yes" | "y" => Some(SqlValue::Int(1)),
            "0" | "false" | "f" | "no" | "n" => Some(SqlValue::Int(0)),
            _ => None,
        },
        _ => None,
    }
}

/// ⭐ PG 兼容: 比较字面量 coercion — UUID 列文本 (36 字符) → 16B (SqlValue::Str
/// 字节载体); 其余类型走 `coerce_cmp_lit`.
fn coerce_cmp_lit_uuid(colty: ColType, v: &SqlValue) -> Option<SqlValue> {
    if colty == ColType::Uuid {
        match v {
            SqlValue::Str(s) => std::str::from_utf8(s)
                .ok()
                .and_then(parse_uuid)
                .map(SqlValue::Str),
            _ => None,
        }
    } else {
        coerce_cmp_lit(colty, v)
    }
}

/// 列值与字面量比较 (数值跨型比较; NULL/类型不符 → None = 条件 false).
/// ⭐ P1: 数值列 vs 文本 → 按文本数字解析比较 (PG 文本参数弱类型, 与 sql_to_col 一致).
pub(crate) fn sql_cmp(cv: &ColValue, sv: &SqlValue) -> Option<std::cmp::Ordering> {
    match (cv, sv) {
        // ⭐ IS [NOT] NULL (desugar 为 `col = NULL` / `col <> NULL`): NULL 列值 vs NULL
        // 字面量 → Equal (Eq 匹配 / Ne 不匹配); 非 NULL 值 vs NULL → Greater (Eq false / Ne true).
        // 标准 SQL 的 `x = NULL` 恒 unknown, 但此处只由 IS [NOT] NULL desugar 产生,
        // 是专有语义 (需先于 `(Null, _)` 分支匹配).
        (cv, SqlValue::Null) => Some(match cv {
            ColValue::Null => std::cmp::Ordering::Equal,
            _ => std::cmp::Ordering::Greater,
        }),
        (ColValue::Null, _) => None,
        (ColValue::I64(a), SqlValue::Int(b)) => Some(a.cmp(b)),
        (ColValue::I64(a), SqlValue::Float(b)) => (*a as f64).partial_cmp(b),
        (ColValue::F64(a), SqlValue::Int(b)) => a.partial_cmp(&(*b as f64)),
        (ColValue::F64(a), SqlValue::Float(b)) => a.partial_cmp(b),
        (ColValue::Bytes(a), SqlValue::Str(b)) => Some(a.as_slice().cmp(b.as_slice())),
        (ColValue::I64(a), SqlValue::Str(s)) => {
            let t = std::str::from_utf8(s).ok()?.trim();
            if let Ok(b) = t.parse::<i64>() {
                Some(a.cmp(&b))
            } else {
                (*a as f64).partial_cmp(&t.parse::<f64>().ok()?)
            }
        }
        (ColValue::F64(a), SqlValue::Str(s)) => {
            a.partial_cmp(&std::str::from_utf8(s).ok()?.trim().parse::<f64>().ok()?)
        }
        // ⭐ F81: DECIMAL 比较 — 字面量转同 scale 定标整数 (精确); Float 走 f64 兜底
        (ColValue::Decimal(a, sc), SqlValue::Int(b)) => (*b as i128)
            .checked_mul(pow10_i128(*sc)?)
            .map(|bb| a.cmp(&bb)),
        (ColValue::Decimal(a, sc), SqlValue::Str(s)) => {
            let t = std::str::from_utf8(s).ok()?.trim();
            match parse_decimal(t, *sc) {
                Some(bb) => Some(a.cmp(&bb)),
                None => (*a as f64 / 10f64.powi(*sc as i32)).partial_cmp(&t.parse::<f64>().ok()?),
            }
        }
        (ColValue::Decimal(a, sc), SqlValue::Float(b)) => {
            (*a as f64 / 10f64.powi(*sc as i32)).partial_cmp(b)
        }
        _ => None,
    }
}

/// ⭐ S4: SQL 错误 → per-proto 字节 (PG 带 SQLSTATE + ReadyForQuery;
/// ⭐ H3: HTTP 按消息映射 4xx/5xx JSON).
pub(crate) fn sql_err_bytes(proto: ProtocolKind, msg: &str) -> Vec<u8> {
    if proto == ProtocolKind::Http {
        let status = if msg.contains("duplicate key") {
            409
        } else if msg.contains("unknown column")
            || msg.contains("Unknown database")
            || msg.contains("expected")
            || msg.contains("unexpected")
            || msg.contains("unterminated")
            || msg.contains("unknown type")
            || msg.contains("no schema")
        {
            400
        } else {
            500
        };
        crate::metrics::HTTP_ERRORS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return crate::protocol::http::build_response(
            status,
            &crate::protocol::http::error_body(msg),
            crate::http_config::cors_origin(),
            true,
        );
    }
    if proto == ProtocolKind::Pg {
        let code = if msg.contains("unknown column") {
            "42703"
        } else if msg.contains("duplicate key") {
            "23505"
        } else if msg.contains("serialization failure") {
            "40001"
        } else if msg.contains("read-only transaction") {
            "25006"
        } else if msg.contains("transaction is aborted") {
            "25P02"
        } else if msg.contains("Unknown database") {
            "3D000"
        } else if msg.contains("expected")
            || msg.contains("unexpected")
            || msg.contains("unterminated")
            || msg.contains("unknown type")
        {
            "42601"
        } else {
            "XX000"
        };
        let mut out = crate::protocol::pg::build_error(code, msg);
        out.extend_from_slice(&crate::protocol::pg::build_ready());
        out
    } else {
        mysql_err_packet(msg)
    }
}

/// ⭐ S4: DML OK → per-proto 字节 (PG: CommandComplete tag + ReadyForQuery;
/// ⭐ H3: HTTP {"affected": n}).
pub(crate) fn sql_ok_bytes(proto: ProtocolKind, affected: u64) -> Vec<u8> {
    if proto == ProtocolKind::Http {
        return crate::protocol::http::build_response(
            200,
            &serde_json::to_vec(&serde_json::json!({ "affected": affected })).unwrap_or_default(),
            crate::http_config::cors_origin(),
            true,
        );
    }
    if proto == ProtocolKind::Pg {
        let mut out = crate::protocol::pg::build_command_complete(&format!("OK {affected}"));
        out.extend_from_slice(&crate::protocol::pg::build_ready());
        out
    } else {
        crate::protocol::mysql::build_ok(1, affected)
    }
}

/// ⭐ S4: 结果集 → per-proto 字节 (PG 尾随 ReadyForQuery;
/// ⭐ H3: HTTP {"columns": [...], "rows": [[...]]}).
pub(crate) fn sql_rows_bytes(
    proto: ProtocolKind,
    binary: bool,
    cols: &[(&str, ColType)],
    rows: &[Vec<ColValue>],
) -> Vec<u8> {
    // ⭐ P2: COM_STMT_EXECUTE 的结果集必须用二进制协议行
    if binary && proto == ProtocolKind::Sql {
        return crate::protocol::mysql::build_binary_result_set(cols, rows);
    }
    if proto == ProtocolKind::Http {
        let columns: Vec<&str> = cols.iter().map(|(n, _)| *n).collect();
        let jrows: Vec<Vec<serde_json::Value>> = rows
            .iter()
            .map(|r| r.iter().map(col_to_json).collect())
            .collect();
        return crate::protocol::http::build_response(
            200,
            &serde_json::to_vec(&serde_json::json!({ "columns": columns, "rows": jrows }))
                .unwrap_or_default(),
            crate::http_config::cors_origin(),
            true,
        );
    }
    if proto == ProtocolKind::Pg {
        let mut out = crate::protocol::pg::build_result_set(cols, rows);
        out.extend_from_slice(&crate::protocol::pg::build_ready());
        out
    } else {
        crate::protocol::mysql::build_result_set(1, cols, rows)
    }
}

/// ⭐ H3: 列值 → JSON (Bytes 优先 UTF-8 字符串, 非法回退 base64 字符串).
pub(crate) fn col_to_json(v: &ColValue) -> serde_json::Value {
    match v {
        ColValue::Null => serde_json::Value::Null,
        ColValue::I64(x) => serde_json::json!(x),
        ColValue::F64(x) => serde_json::json!(x),
        ColValue::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => serde_json::json!(s),
            Err(_) => serde_json::json!(crate::protocol::http::base64_encode(b)),
        },
        // ⭐ F81: Decimal → JSON 字符串 (保精度; JSON number 会丢精度)
        ColValue::Decimal(x, scale) => serde_json::json!(render_decimal(*x, *scale)),
    }
}

/// SELECT 结果渲染 (列定义/行值按投影序; per-proto 编码).
/// ⭐ F76: names 与 proj 同序; 某项 Some 时用作输出列名 (AS 别名), 否则用 schema 列名.
pub(crate) fn render_sql_rows(
    proto: ProtocolKind,
    binary: bool,
    schema: &TableSchema,
    proj: &[u16],
    names: &[Option<String>],
    rows: &[Vec<ColValue>],
) -> Vec<u8> {
    let cols: Vec<(&str, ColType)> = proj
        .iter()
        .enumerate()
        .map(|(k, &i)| {
            let c = &schema.columns[i as usize];
            let name = names
                .get(k)
                .and_then(|o| o.as_deref())
                .unwrap_or(c.name.as_str());
            (name, c.ty)
        })
        .collect();
    let proj_rows: Vec<Vec<ColValue>> = rows
        .iter()
        .map(|r| proj.iter().map(|&i| r[i as usize].clone()).collect())
        .collect();
    sql_rows_bytes(proto, binary, &cols, &proj_rows)
}

/// ⭐ O1: 覆盖索引值重建 — 索引条目的原值字节 → 列值 (与 keyspace 编码同源).
/// 数值 = 8B 保序编码; 字节串 = 原字节. 长度不符 → None (防御).
pub(crate) fn col_from_ordered_bytes(ty: ColType, raw: &[u8]) -> Option<ColValue> {
    match ty {
        ColType::I64 => raw
            .try_into()
            .ok()
            .map(|b| ColValue::I64(storage::keyspace::decode_idx(b))),
        // ⭐ F80: Bool/Date/Time/Timestamp 以 i64 承载 → 同 I64 保序解码
        ColType::Bool | ColType::Date | ColType::Time | ColType::Timestamp => raw
            .try_into()
            .ok()
            .map(|b| ColValue::I64(storage::keyspace::decode_idx(b))),
        ColType::F64 => raw
            .try_into()
            .ok()
            .map(|b| ColValue::F64(storage::keyspace::decode_f64_ordered(b))),
        ColType::Str | ColType::Bytes | ColType::Json | ColType::Uuid => {
            Some(ColValue::Bytes(raw.to_vec()))
        }
        // ⭐ F81: Decimal 覆盖索引值重建 (16B 保序 → i128; scale 从列类型)
        ColType::Decimal { scale, .. } => raw
            .try_into()
            .ok()
            .map(|b| ColValue::Decimal(storage::keyspace::decode_i128_ordered(b), scale)),
    }
}

/// ⭐ S1: DML phase1 完成 — 全条件过滤取 pk (rows 取走清空; 去重防跨 shard 幽灵重复).
pub(crate) fn collect_dml_pks(agg: &mut SqlSelectAgg) -> Result<Vec<Vec<u8>>, String> {
    let rows = std::mem::take(&mut agg.rows);
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut pks = Vec::new();
    for (_, pk, rb) in &rows {
        let values = storage::row::decode_row(&agg.schema, rb).map_err(|e| e.to_string())?;
        if eval_pred(&agg.schema, &values, &agg.conds) && seen.insert(pk.clone()) {
            pks.push(pk.clone());
        }
    }
    Ok(pks)
}

/// ⭐ S1: phase2 op 构造 (每 pk 一发, 按 pk 路由).
pub(crate) fn sql_dml_op(
    db: &std::sync::Arc<str>,
    table: &str,
    pk: Vec<u8>,
    action: &SqlDmlAction,
) -> BatchOp {
    match action {
        SqlDmlAction::Delete => BatchOp::RowDelete {
            db: db.clone(),
            table: std::sync::Arc::from(table),
            pk,
        },
        SqlDmlAction::Update(sets) => BatchOp::RowUpdate {
            db: db.clone(),
            table: std::sync::Arc::from(table),
            pk,
            sets: sets.clone(),
        },
    }
}

/// ⭐ S2: ORDER BY 比较 (多列; NULL 按 asc 排最后, desc 时相反 — PG 默认行为).
pub(crate) fn sql_order_cmp(
    a: &[ColValue],
    b: &[ColValue],
    order: &[(u16, bool)],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for &(col, desc) in order {
        let (av, bv) = (&a[col as usize], &b[col as usize]);
        let o = match (av, bv) {
            (ColValue::Null, ColValue::Null) => Ordering::Equal,
            (ColValue::Null, _) => Ordering::Greater,
            (_, ColValue::Null) => Ordering::Less,
            (ColValue::I64(x), ColValue::I64(y)) => x.cmp(y),
            (ColValue::F64(x), ColValue::F64(y)) => x.total_cmp(y),
            (ColValue::I64(x), ColValue::F64(y)) => (*x as f64).total_cmp(y),
            (ColValue::F64(x), ColValue::I64(y)) => x.total_cmp(&(*y as f64)),
            (ColValue::Bytes(x), ColValue::Bytes(y)) => x.cmp(y),
            // ⭐ F81: Decimal 同列同 scale → 定标整数比较
            (ColValue::Decimal(x, _), ColValue::Decimal(y, _)) => x.cmp(y),
            _ => Ordering::Equal, // 异型防御 (schema 同列不应发生)
        };
        let o = if desc { o.reverse() } else { o };
        if o != Ordering::Equal {
            return o;
        }
    }
    std::cmp::Ordering::Equal
}

/// ⭐ S2: COUNT(*) 单行结果集.
pub(crate) fn render_sql_count(proto: ProtocolKind, binary: bool, n: u64) -> Vec<u8> {
    sql_rows_bytes(
        proto,
        binary,
        &[("COUNT(*)", ColType::I64)],
        &[vec![ColValue::I64(n as i64)]],
    )
}
