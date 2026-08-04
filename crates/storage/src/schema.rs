//! ⭐ Q1 (SQL 索引基建): table schema 定义 + 二进制序列化.
//!
//! schema 是 SQL 表的元数据: 列定义 (类型/可空) + 主键列 + 二级索引定义.
//! 持久化为表内保留物理行 `[$]` (见 `keyspace::encode_schema_row`),
//! 并在 `StorageEngine` 常驻内存镜像 (write-through, lazy load).
//! 无 schema 行的表 = 纯 KV 表, 走原路径零回归.

/// 列数据类型.
///
/// I64/F64 为定长 8B (row 编码不记偏移, 索引值编码复用保序数值编码);
/// Str/Bytes 为变长 (row 记偏移, 索引值用转义终结符编码).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColType {
    I64,
    F64,
    Str,
    Bytes,
    Bool,
    Date,
    Time,
    Timestamp,
    Json,
    Uuid,
    /// ⭐ F81: 定点小数. precision=总位数(仅回显), scale=小数位.
    /// 值以 i128 承载 (变长区 16B LE); scale 随类型 → 转换/渲染/比较全程可得.
    Decimal { precision: u8, scale: u8 },
}

impl ColType {
    fn to_byte(self) -> u8 {
        match self {
            ColType::I64 => 1,
            ColType::F64 => 2,
            ColType::Str => 3,
            ColType::Bytes => 4,
            ColType::Bool => 5,
            ColType::Date => 6,
            ColType::Time => 7,
            ColType::Timestamp => 8,
            ColType::Json => 9,
            ColType::Uuid => 10,
            ColType::Decimal { .. } => 11,
        }
    }

    fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(ColType::I64),
            2 => Some(ColType::F64),
            3 => Some(ColType::Str),
            4 => Some(ColType::Bytes),
            5 => Some(ColType::Bool),
            6 => Some(ColType::Date),
            7 => Some(ColType::Time),
            8 => Some(ColType::Timestamp),
            9 => Some(ColType::Json),
            10 => Some(ColType::Uuid),
            // 11 (Decimal) 需读取额外 precision/scale 字节, 由 decode 特判处理.
            _ => None,
        }
    }

    /// 定长列 (row 编码不记偏移, 8B 槽). ⭐ F80: Bool/Date/Time/Timestamp 以 i64 承载也定长.
    /// ⭐ F81: Decimal 走变长区 (16B i128), 非定长.
    pub fn is_fixed(self) -> bool {
        matches!(
            self,
            ColType::I64
                | ColType::F64
                | ColType::Bool
                | ColType::Date
                | ColType::Time
                | ColType::Timestamp
        )
    }
}

/// 列定义.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: ColType,
    pub nullable: bool,
    /// ⭐ PG 兼容: 列默认值 (DEFAULT 表达式; v1: 字面量 / NOW / uuid_generate_v4).
    pub default: Option<ColDefault>,
}

use crate::row::ColValue;

/// ⭐ PG 兼容 (FMT_VER 6): 列 DEFAULT 表达式.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColDefault {
    /// 字面量 — 已按列类型编码 (parser 构造时用列类型转换).
    Lit(ColValue),
    /// NOW() / CURRENT_TIMESTAMP — 当前时间戳 (i64 微秒).
    Now,
    /// uuid_generate_v4() — 随机 UUID (16B).
    UuidGenV4,
    /// ⭐ PG 兼容 (portal): SERIAL/BIGSERIAL — 进程级单调递增 I64.
    Serial,
}

/// ⭐ PG 兼容 (FMT_VER 8): 外键 ON DELETE 动作.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FkAction {
    /// 无动作 (默认; 仅语法兼容, v1 不做 RESTRICT 检查).
    NoAction,
    /// ON DELETE CASCADE — 删除父行时级联删引用行.
    Cascade,
    /// ON DELETE SET NULL — 删除父行时引用列置 NULL (要求列可空).
    SetNull,
}

/// ⭐ PG 兼容 (FMT_VER 8): 外键定义 — 本表 `col` 引用 `ref_table(ref_col)`.
/// v1: 单列外键; ref_table/ref_col 按名存储 (跨表引用, 位置在引用表 schema).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FkDef {
    pub col: u16,
    pub ref_table: String,
    pub ref_col: String,
    pub on_delete: FkAction,
}

/// ColDefault::Lit 编码 (值标签 + payload; 仅 DEFAULT 常用类型).
fn encode_colvalue(v: &ColValue, out: &mut Vec<u8>) {
    match v {
        ColValue::Null => out.push(0),
        ColValue::I64(x) => {
            out.push(1);
            out.extend_from_slice(&x.to_le_bytes());
        }
        ColValue::F64(x) => {
            out.push(2);
            out.extend_from_slice(&x.to_le_bytes());
        }
        ColValue::Bytes(b) => {
            out.push(3);
            out.extend_from_slice(&(b.len() as u16).to_le_bytes());
            out.extend_from_slice(b);
        }
        ColValue::Decimal(x, sc) => {
            out.push(4);
            out.push(*sc);
            out.extend_from_slice(&x.to_le_bytes());
        }
    }
}

fn decode_colvalue(r: &mut Reader) -> Result<ColValue, SchemaError> {
    Ok(match r.u8()? {
        0 => ColValue::Null,
        1 => ColValue::I64(r.i64()?),
        2 => ColValue::F64(r.f64()?),
        3 => {
            let len = r.u16()? as usize;
            ColValue::Bytes(r.bytes(len)?.to_vec())
        }
        4 => {
            let sc = r.u8()?;
            ColValue::Decimal(r.i128()?, sc)
        }
        _ => return Err(SchemaError::BadFormat),
    })
}

/// 二级索引定义 (iid 表内唯一, 由 schema 内 next_iid 单调分配).
/// ⭐ O3: `unique` = 唯一索引 (写入强制唯一 + 等值查询可早停).
/// ⭐ F65: `global` = 跨 shard 全局唯一 (email-shard 占坑; 仅 unique 时有意义).
/// ⭐ PG 兼容 (FMT_VER 7): `cols` = 复合索引列集 (单列 = [col]).
/// `col` 保留 = cols[0] (兼容既有单列引用/诊断).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    pub iid: u32,
    pub col: u16,
    /// ⭐ PG 兼容: 索引列集 (复合唯一索引 >1 列; 单列 = vec![col]).
    pub cols: Vec<u16>,
    pub unique: bool,
    pub global: bool,
}

/// 表 schema.
///
/// `version` 是 schema 演进版本 (row 编码首部携带, 为将来 ALTER TABLE 预留);
/// `pk_col` 指向主键列 (PK 即存储 key, 唯一性由用户层保证).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub version: u8,
    pub columns: Vec<Column>,
    pub pk_col: u16,
    pub indexes: Vec<IndexDef>,
    /// 下一个可分配的 iid (只增; 删索引不回收 iid, 防旧索引行误匹配).
    pub next_iid: u32,
    /// ⭐ F79 (ALTER): 各版本的列数 (index=version-1 → 该版本写入时的列数).
    /// ADD COLUMN 只追加列 → 旧行 (版本较小) 按其版本列数解码, 超出列补 NULL.
    pub version_ncols: Vec<u16>,
    /// ⭐ compat (DROP COLUMN): 已删除列的列号集合 (被删列仍占 columns 位置,
    /// 行布局/版本机制不变 → 存量行零重写; 新行该列写 NULL). 索引 on 该列一并移除.
    pub dropped: Vec<u16>,
    /// ⭐ PG 兼容 (FMT_VER 8): 外键列表 (本表引用其他表; 反向查询在 worker 聚合).
    pub fks: Vec<FkDef>,
}

/// 序列化格式版本 (与 schema.version 无关, 是编码布局版本).
/// ⭐ F65: 1→2 索引项加 1B global 标志; decode 兼容 v1 (global=false).
/// ⭐ F79: 2→3 尾部加 version_ncols; decode 兼容 v1/v2 (version_ncols=[当前列数]).
/// ⭐ F81: 3→4 Decimal 列在类型字节后追加 precision+scale; decode 兼容 v1-3 (无 Decimal 列).
/// ⭐ compat: 4→5 尾部加 dropped 列号列表; decode 兼容 v1-4 (dropped=[]).
/// ⭐ PG 兼容: 5→6 每列追加 default 段; decode 兼容 v1-5 (default=None).
/// ⭐ PG 兼容: 6→7 索引项追加 cols 列集; decode 兼容 v1-6 (cols=[col]).
/// ⭐ PG 兼容: 7→8 表尾追加 fks 外键列表; decode 兼容 v1-7 (fks=[]).
const FMT_VER: u8 = 8;

/// schema 反序列化错误.
#[derive(Debug, PartialEq, Eq)]
pub enum SchemaError {
    /// 字节流截断或字段越界.
    Truncated,
    /// 未知的编码格式版本 / 列类型字节.
    BadFormat,
    /// 列/索引引用越界 (pk_col / IndexDef.col 超出列数).
    BadRef,
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::Truncated => write!(f, "schema bytes truncated"),
            SchemaError::BadFormat => write!(f, "schema bad format"),
            SchemaError::BadRef => write!(f, "schema column ref out of range"),
        }
    }
}

impl std::error::Error for SchemaError {}

impl TableSchema {
    /// 新建 schema (校验引用合法性). indexes 的 iid 由本函数重新分配;
    /// `unique_cols` 建唯一索引 (⭐ O3); `global_unique_cols` 建全局唯一索引
    /// (⭐ F65, 隐含 unique); 与 index_cols 并集去重后按序分配 iid.
    pub fn new(
        columns: Vec<Column>,
        pk_col: u16,
        index_cols: &[u16],
        unique_cols: &[u16],
        global_unique_cols: &[u16],
        composite_unique_cols: &[Vec<u16>],
        fks: &[FkDef],
    ) -> Result<Self, SchemaError> {
        if pk_col as usize >= columns.len() {
            return Err(SchemaError::BadRef);
        }
        let mut next_iid = 0u32;
        let mut indexes: Vec<IndexDef> = Vec::with_capacity(
            index_cols.len() + unique_cols.len() + global_unique_cols.len() + composite_unique_cols.len(),
        );
        let mut seen_cols = std::collections::HashSet::new();
        // (列集, unique, global); 单列索引逐列登记, 复合唯一整组登记
        let mut push_index = |indexes: &mut Vec<IndexDef>, cols: Vec<u16>, unique: bool, global: bool| -> Result<(), SchemaError> {
            let col = cols[0];
            if col as usize >= columns.len() {
                return Err(SchemaError::BadRef);
            }
            if col == pk_col && global {
                // pk 已天然全局唯一 (存储 key), 无需占坑
                return Err(SchemaError::BadRef);
            }
            if cols.iter().any(|&c| c as usize >= columns.len()) {
                return Err(SchemaError::BadRef);
            }
            // 去重: 单列索引按列判重; 复合整组判重 (与既有单列键不同 → 保留)
            let dup = if cols.len() == 1 {
                seen_cols.contains(&col)
            } else {
                indexes.iter().any(|i| i.cols == cols)
            };
            if dup {
                return Ok(());
            }
            indexes.push(IndexDef { iid: next_iid, col, cols, unique, global });
            next_iid += 1;
            seen_cols.insert(col);
            Ok(())
        };
        for &col in index_cols {
            push_index(&mut indexes, vec![col], false, false)?;
        }
        for &col in unique_cols {
            push_index(&mut indexes, vec![col], true, false)?;
        }
        for &col in global_unique_cols {
            push_index(&mut indexes, vec![col], true, true)?;
        }
        for group in composite_unique_cols {
            if group.len() < 2 {
                return Err(SchemaError::BadRef);
            }
            push_index(&mut indexes, group.clone(), true, false)?;
        }
        Ok(Self {
            version: 1,
            version_ncols: vec![columns.len() as u16],
            columns,
            pk_col,
            indexes,
            next_iid,
            dropped: Vec::new(),
            fks: fks.to_vec(),
        })
    }

    /// ⭐ F79 (ALTER): 追加一列产新 schema (version+1, 记录新版本列数).
    /// 仅追加到末尾; pk_col/indexes 不变 (不移位). version 超 255 报错.
    pub fn with_added_column(&self, col: Column) -> Result<Self, SchemaError> {
        let new_ver = self.version.checked_add(1).ok_or(SchemaError::BadFormat)?;
        let mut columns = self.columns.clone();
        columns.push(col);
        let mut version_ncols = self.version_ncols.clone();
        version_ncols.push(columns.len() as u16);
        Ok(Self {
            version: new_ver,
            columns,
            pk_col: self.pk_col,
            indexes: self.indexes.clone(),
            next_iid: self.next_iid,
            version_ncols,
            dropped: self.dropped.clone(),
            fks: self.fks.clone(),
        })
    }

    /// ⭐ compat (DROP COLUMN): 标记删除一列 — 该列保留在 columns (行布局不变,
    /// 存量行零重写), 从可见列/索引移除; version 不变 (列数未变). 重复删/越界报错.
    pub fn with_dropped_column(&self, col_idx: u16) -> Result<Self, SchemaError> {
        if col_idx as usize >= self.columns.len() || self.dropped.contains(&col_idx) {
            return Err(SchemaError::BadRef);
        }
        let mut dropped = self.dropped.clone();
        dropped.push(col_idx);
        Ok(Self {
            version: self.version,
            version_ncols: self.version_ncols.clone(),
            columns: self.columns.clone(),
            pk_col: self.pk_col,
            indexes: self.indexes.iter().filter(|i| i.col != col_idx).cloned().collect(),
            next_iid: self.next_iid,
            dropped,
            fks: self.fks.clone(),
        })
    }

    /// ⭐ F79: 某行版本写入时的列数 (行首部 version 字节 → 列数). 未知版本回退当前列数.
    pub fn col_count_at(&self, ver: u8) -> usize {
        if ver >= 1 && (ver as usize) <= self.version_ncols.len() {
            self.version_ncols[ver as usize - 1] as usize
        } else {
            self.columns.len()
        }
    }

    /// 按列名查列下标.
    /// ⭐ compat (DROP COLUMN): 列名 → 列号 (跳过已删列 → None, 显式引用报 unknown).
    pub fn col_by_name(&self, name: &str) -> Option<u16> {
        self.columns
            .iter()
            .position(|c| c.name == name)
            .map(|i| i as u16)
            .filter(|&i| !self.dropped.contains(&i))
    }

    /// 序列化:
    /// `[FMT_VER][version][pk_col u16][n_cols u16]
    ///  {[name_len u8][name][ty u8][nullable u8]}×n
    ///  [n_idx u16] {[iid u32][col u16]}×m [next_iid u32]` (整数 LE).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.push(FMT_VER);
        out.push(self.version);
        out.extend_from_slice(&self.pk_col.to_le_bytes());
        out.extend_from_slice(&(self.columns.len() as u16).to_le_bytes());
        for c in &self.columns {
            debug_assert!(c.name.len() <= u8::MAX as usize, "列名过长");
            out.push(c.name.len() as u8);
            out.extend_from_slice(c.name.as_bytes());
            out.push(c.ty.to_byte());
            // ⭐ F81 (FMT_VER 4): Decimal 列在类型字节后追加 precision + scale
            if let ColType::Decimal { precision, scale } = c.ty {
                out.push(precision);
                out.push(scale);
            }
            out.push(c.nullable as u8);
            // ⭐ PG 兼容 (FMT_VER 6): 列默认值 (0=无; 1=Lit; 2=Now; 3=UuidGenV4)
            match &c.default {
                None => out.push(0),
                Some(ColDefault::Lit(v)) => {
                    out.push(1);
                    encode_colvalue(v, &mut out);
                }
                Some(ColDefault::Now) => out.push(2),
                Some(ColDefault::UuidGenV4) => out.push(3),
                Some(ColDefault::Serial) => out.push(4),
            }
        }
        out.extend_from_slice(&(self.indexes.len() as u16).to_le_bytes());
        for i in &self.indexes {
            out.extend_from_slice(&i.iid.to_le_bytes());
            out.extend_from_slice(&i.col.to_le_bytes());
            // ⭐ PG 兼容 (FMT_VER 7): 索引列集 (单列 = len1)
            out.extend_from_slice(&(i.cols.len() as u16).to_le_bytes());
            for &c in &i.cols {
                out.extend_from_slice(&c.to_le_bytes());
            }
            out.push(i.unique as u8); // ⭐ O3
            out.push(i.global as u8); // ⭐ F65 (FMT_VER 2)
        }
        out.extend_from_slice(&self.next_iid.to_le_bytes());
        // ⭐ F79 (FMT_VER 3): version_ncols 尾部
        out.push(self.version_ncols.len() as u8);
        for &nc in &self.version_ncols {
            out.extend_from_slice(&nc.to_le_bytes());
        }
        // ⭐ compat (FMT_VER 5): dropped 列号列表
        out.push(self.dropped.len() as u8);
        for &d in &self.dropped {
            out.extend_from_slice(&d.to_le_bytes());
        }
        // ⭐ PG 兼容 (FMT_VER 8): fks 外键列表
        out.extend_from_slice(&(self.fks.len() as u16).to_le_bytes());
        for fk in &self.fks {
            out.extend_from_slice(&fk.col.to_le_bytes());
            let rt = fk.ref_table.as_bytes();
            out.extend_from_slice(&(rt.len() as u8).to_le_bytes());
            out.extend_from_slice(rt);
            let rc = fk.ref_col.as_bytes();
            out.extend_from_slice(&(rc.len() as u8).to_le_bytes());
            out.extend_from_slice(rc);
            out.push(fk.on_delete as u8);
        }
        out
    }

    /// 反序列化 (逆 `encode`).
    pub fn decode(buf: &[u8]) -> Result<Self, SchemaError> {
        let mut r = Reader { buf, pos: 0 };
        let fmt = r.u8()?;
        if fmt != FMT_VER
            && fmt != 1
            && fmt != 2
            && fmt != 3
            && fmt != 4
            && fmt != 5
            && fmt != 6
            && fmt != 7
        {
            return Err(SchemaError::BadFormat);
        }
        let version = r.u8()?;
        let pk_col = r.u16()?;
        let n_cols = r.u16()? as usize;
        let mut columns = Vec::with_capacity(n_cols);
        for _ in 0..n_cols {
            let nlen = r.u8()? as usize;
            let name = std::str::from_utf8(r.bytes(nlen)?)
                .map_err(|_| SchemaError::BadFormat)?
                .to_string();
            let tb = r.u8()?;
            // ⭐ F81: Decimal 类型字节后跟 precision + scale
            let ty = if tb == 11 {
                let precision = r.u8()?;
                let scale = r.u8()?;
                ColType::Decimal { precision, scale }
            } else {
                ColType::from_byte(tb).ok_or(SchemaError::BadFormat)?
            };
            let nullable = r.u8()? != 0;
            // ⭐ PG 兼容 (FMT_VER 6): 列默认值; v1-5 无此段 → None
            let default = if fmt >= 6 {
                match r.u8()? {
                    0 => None,
                    1 => Some(ColDefault::Lit(decode_colvalue(&mut r)?)),
                    2 => Some(ColDefault::Now),
                    3 => Some(ColDefault::UuidGenV4),
                    4 => Some(ColDefault::Serial),
                    _ => return Err(SchemaError::BadFormat),
                }
            } else {
                None
            };
            columns.push(Column { name, ty, nullable, default });
        }
        let n_idx = r.u16()? as usize;
        let mut indexes = Vec::with_capacity(n_idx);
        for _ in 0..n_idx {
            let iid = r.u32()?;
            let col = r.u16()?;
            // ⭐ PG 兼容 (FMT_VER 7): 索引列集; v1-6 无此段 → 单列 [col]
            let cols = if fmt >= 7 {
                let n = r.u16()? as usize;
                if n == 0 {
                    return Err(SchemaError::BadFormat);
                }
                let mut cs = Vec::with_capacity(n);
                for _ in 0..n {
                    cs.push(r.u16()?);
                }
                cs
            } else {
                vec![col]
            };
            if col as usize >= columns.len() || cols.iter().any(|&c| c as usize >= columns.len()) {
                return Err(SchemaError::BadRef);
            }
            let unique = r.u8()? != 0; // ⭐ O3
            let global = if fmt >= 2 { r.u8()? != 0 } else { false }; // ⭐ F65 (v1 无此字段)
            indexes.push(IndexDef { iid, col, cols, unique, global });
        }
        let next_iid = r.u32()?;
        if pk_col as usize >= columns.len() {
            return Err(SchemaError::BadRef);
        }
        // ⭐ F79 (FMT_VER 3): version_ncols; v1/v2 无此段 → 单版本=当前列数
        let version_ncols = if fmt >= 3 {
            let nv = r.u8()? as usize;
            let mut v = Vec::with_capacity(nv);
            for _ in 0..nv {
                v.push(r.u16()?);
            }
            if v.is_empty() { vec![columns.len() as u16] } else { v }
        } else {
            vec![columns.len() as u16]
        };
        // ⭐ compat (FMT_VER 5): dropped 列号列表; v1-4 无此段 → 空
        let dropped = if fmt >= 5 {
            let nd = r.u8()? as usize;
            let mut d = Vec::with_capacity(nd);
            for _ in 0..nd {
                d.push(r.u16()?);
            }
            d
        } else {
            Vec::new()
        };
        // ⭐ PG 兼容 (FMT_VER 8): fks 外键列表; v1-7 无此段 → 空
        let fks = if fmt >= 8 {
            let nf = r.u16()? as usize;
            let mut fs = Vec::with_capacity(nf);
            for _ in 0..nf {
                let col = r.u16()?;
                let rt_len = r.u8()? as usize;
                let ref_table = String::from_utf8_lossy(r.bytes(rt_len)?).into_owned();
                let rc_len = r.u8()? as usize;
                let ref_col = String::from_utf8_lossy(r.bytes(rc_len)?).into_owned();
                let action = match r.u8()? {
                    1 => FkAction::Cascade,
                    2 => FkAction::SetNull,
                    _ => FkAction::NoAction,
                };
                fs.push(FkDef { col, ref_table, ref_col, on_delete: action });
            }
            fs
        } else {
            Vec::new()
        };
        Ok(Self {
            version,
            columns,
            pk_col,
            indexes,
            next_iid,
            version_ncols,
            dropped,
            fks,
        })
    }
}

/// 最小顺序读取器 (schema 解码专用).
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn bytes(&mut self, n: usize) -> Result<&'a [u8], SchemaError> {
        let end = self.pos.checked_add(n).ok_or(SchemaError::Truncated)?;
        if end > self.buf.len() {
            return Err(SchemaError::Truncated);
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8, SchemaError> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, SchemaError> {
        Ok(u16::from_le_bytes(self.bytes(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, SchemaError> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64, SchemaError> {
        Ok(i64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    fn f64(&mut self) -> Result<f64, SchemaError> {
        Ok(f64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    fn i128(&mut self) -> Result<i128, SchemaError> {
        Ok(i128::from_le_bytes(self.bytes(16)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn demo_schema() -> TableSchema {
        TableSchema::new(
            vec![
                Column { name: "id".into(), ty: ColType::I64, nullable: false, default: None },
                Column { name: "name".into(), ty: ColType::Str, nullable: false, default: None },
                Column { name: "score".into(), ty: ColType::F64, nullable: true, default: None },
                Column { name: "blob".into(), ty: ColType::Bytes, nullable: true, default: None },
            ],
            0,
            &[1, 2], // name / score 建索引
            &[3],    // blob 唯一索引 (⭐ O3 roundtrip)
            &[], &[], &[],
        )
        .unwrap()
    }

    #[test]
    fn roundtrip() {
        let s = demo_schema();
        let bytes = s.encode();
        let d = TableSchema::decode(&bytes).unwrap();
        assert_eq!(s, d);
        assert_eq!(d.indexes.len(), 3);
        assert_eq!(d.indexes[0], IndexDef { iid: 0, col: 1, cols: vec![1], unique: false, global: false });
        assert_eq!(d.indexes[1], IndexDef { iid: 1, col: 2, cols: vec![2], unique: false, global: false });
        // ⭐ O3: unique 位随序列化 roundtrip
        assert_eq!(d.indexes[2], IndexDef { iid: 2, col: 3, cols: vec![3], unique: true, global: false });
        assert_eq!(d.next_iid, 3);
    }

    #[test]
    fn col_lookup_and_refs() {
        let s = demo_schema();
        assert_eq!(s.col_by_name("score"), Some(2));
        assert_eq!(s.col_by_name("nope"), None);
        // 越界引用被拒
        assert_eq!(
            TableSchema::new(
                vec![Column { name: "a".into(), ty: ColType::I64, nullable: false, default: None }],
                1,
                &[],
                &[], &[], &[], &[]),
            Err(SchemaError::BadRef)
        );
        assert_eq!(
            TableSchema::new(
                vec![Column { name: "a".into(), ty: ColType::I64, nullable: false, default: None }],
                0,
                &[3],
                &[], &[], &[], &[]),
            Err(SchemaError::BadRef)
        );
    }

    #[test]
    fn decode_rejects_garbage() {
        assert_eq!(TableSchema::decode(&[]), Err(SchemaError::Truncated));
        assert_eq!(TableSchema::decode(&[99, 1, 0, 0, 0, 0]), Err(SchemaError::BadFormat));
        // 截断的列区
        let mut bytes = demo_schema().encode();
        bytes.truncate(bytes.len() / 2);
        assert!(TableSchema::decode(&bytes).is_err());
    }
}
