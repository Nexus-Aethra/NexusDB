//! ⭐ Q4 (SQL 索引基建): row 表操作 + 本地二级索引维护 / 扫描.
//!
//! ## 设计不变量
//! - **row 行复用 String 命名空间** `[S][klen][pk]` → `[TAG_ROW]...`
//!   (主键点查 = 既有 table_get 热路径; 溢出页/COW/GC 全部正交复用)
//! - **索引行与 row 同 shard** (本地二级索引): 本模块只在单 shard 引擎内
//!   维护 `[I][iid][保序值][PK]` → 空值; 路由由上层按 PK 决定, 索引行
//!   永不独立路由 — 写入原子性 = 单 shard 批内一致
//! - **NULL 列不入索引** (无索引行; IS NULL 查询走全表扫, 文档记录)
//! - crash 窗口 (row 已落、索引行未落) 与复合结构 meta count 同级 gap
//!
//! ## 与既有类型体系互斥
//! 有 schema 的表视为 row 表: 复合命令 (HSET...) 在 `ensure_kind` 处
//! 报 WRONGTYPE (见 collections.rs); row op 反向要求 schema 存在.

use std::ops::ControlFlow;
use std::sync::Arc;

use crate::engine::StorageEngine;
use crate::keyspace as ks;
use crate::registry::RegistryError;
use crate::row::{self, ColValue};
use crate::schema::{ColType, TableSchema};

/// schema/row 错误统一挂到 RegistryError::Schema.
fn se(e: impl std::fmt::Display) -> RegistryError {
    RegistryError::Schema(e.to_string())
}

/// 索引扫描条目: (索引原值, pk, row_bytes; 覆盖索引时 row 为空).
pub type IndexEntry = (Vec<u8>, Vec<u8>, Vec<u8>);

/// ⭐ F67 (JOIN): 下推谓词算子 (与 network CmpOp 一一对应; 定于 storage 避分层反向).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    In,
}

/// ⭐ F67 (JOIN): 下推到 shard 本地执行的谓词 `列号 op 值` (AND 连接).
/// In 时 val 忽略、用 set; 其余用 val、set 空. 列号由 worker 按各表 schema 解析.
#[derive(Debug, Clone)]
pub struct ScanPred {
    pub col: u16,
    pub op: PredOp,
    pub val: ColValue,
    pub set: Vec<ColValue>,
}

/// ⭐ F68 (JOIN): 索引驱动 gather 提示 — 先走索引范围扫缩候选, 再残余 preds 精确.
/// lo/hi 为闭区间界 (None = 无界); worker 只作过度近似到界, 正确性由 preds 保证.
#[derive(Debug, Clone)]
pub struct IndexHint {
    pub iid: u32,
    pub lo: Option<ColValue>,
    pub hi: Option<ColValue>,
    /// ⭐ PG 兼容 (范围查): 扫主键 B+Tree 区间 (非二级索引). 主键列范围谓词
    /// (BETWEEN/>=/<=) 用此走主键索引, 避免全表扫.
    pub pk: bool,
}

/// ⭐ F70 (JOIN): 键集合索引点查提示 — probe 侧只回 join 键 ∈ keys 的行.
/// 前序已 gather 表的 ON 等值键值集合; shard 端对每键等值索引点查 (bloom 短路).
#[derive(Debug, Clone)]
pub struct KeySetHint {
    pub iid: u32,
    pub keys: Vec<ColValue>,
}

/// ⭐ F67 (JOIN): ColValue 跨型比较 (语义与 worker sql_cmp 一致: 数值跨 I64/F64,
/// Bytes 字典序, 数值列对 Bytes 按解析; NULL → None 不匹配). a=列值, b=谓词值.
fn cmp_colval(a: &ColValue, b: &ColValue) -> Option<std::cmp::Ordering> {
    use ColValue::*;
    match (a, b) {
        (Null, _) | (_, Null) => None,
        (I64(x), I64(y)) => Some(x.cmp(y)),
        (I64(x), F64(y)) => (*x as f64).partial_cmp(y),
        (F64(x), I64(y)) => x.partial_cmp(&(*y as f64)),
        (F64(x), F64(y)) => x.partial_cmp(y),
        (Bytes(x), Bytes(y)) => Some(x.as_slice().cmp(y.as_slice())),
        (I64(x), Bytes(s)) => {
            let t = std::str::from_utf8(s).ok()?.trim();
            if let Ok(y) = t.parse::<i64>() {
                Some(x.cmp(&y))
            } else {
                (*x as f64).partial_cmp(&t.parse::<f64>().ok()?)
            }
        }
        (F64(x), Bytes(s)) => {
            x.partial_cmp(&std::str::from_utf8(s).ok()?.trim().parse::<f64>().ok()?)
        }
        _ => None,
    }
}

/// ⭐ F67 (JOIN): 单行过谓词集 (AND; NULL 列恒 false, 与 sql_eval_conds 同义).
fn row_pass_preds(cols: &[ColValue], preds: &[ScanPred]) -> bool {
    use std::cmp::Ordering;
    for p in preds {
        let Some(cv) = cols.get(p.col as usize) else {
            return false;
        };
        if p.op == PredOp::In {
            if !p.set.iter().any(|v| cmp_colval(cv, v) == Some(Ordering::Equal)) {
                return false;
            }
            continue;
        }
        let pass = match cmp_colval(cv, &p.val) {
            None => false,
            Some(o) => match p.op {
                PredOp::Eq => o == Ordering::Equal,
                PredOp::Ne => o != Ordering::Equal,
                PredOp::Gt => o == Ordering::Greater,
                PredOp::Ge => o != Ordering::Less,
                PredOp::Lt => o == Ordering::Less,
                PredOp::Le => o != Ordering::Greater,
                PredOp::In => unreachable!(),
            },
        };
        if !pass {
            return false;
        }
    }
    true
}

/// 单索引的 (iid, 编码值 | None-NULL) 快照 (update/delete 时对比新旧).
type IndexValSnapshot = Vec<(u32, Option<Vec<u8>>)>;

/// 按列类型编码索引值段 (含型别字节). NULL → None (不入索引).
/// 类型不符在 encode_row 已拦, 此处防御性返回 None.
/// (pub: worker 路由缓存与引擎共用同一编码, 保证 bloom 键一致)
pub fn index_val_bytes(ty: ColType, v: &ColValue) -> Option<Vec<u8>> {
    match (ty, v) {
        (_, ColValue::Null) => None,
        (ColType::I64, ColValue::I64(x)) => Some(ks::encode_index_num(ks::encode_idx(*x))),
        (ColType::F64, ColValue::F64(x)) => {
            Some(ks::encode_index_num(ks::encode_f64_ordered(*x)))
        }
        // ⭐ F80: Bool/Date/Time/Timestamp 以 i64 承载 → 复用保序数值索引
        (
            ColType::Bool | ColType::Date | ColType::Time | ColType::Timestamp,
            ColValue::I64(x),
        ) => Some(ks::encode_index_num(ks::encode_idx(*x))),
        (ColType::Str | ColType::Bytes | ColType::Json | ColType::Uuid, ColValue::Bytes(b)) => {
            Some(ks::encode_index_bytes(b))
        }
        // ⭐ F81: Decimal → 17B 保序索引值
        (ColType::Decimal { .. }, ColValue::Decimal(x, _)) => {
            Some(ks::encode_index_decimal(ks::encode_i128_ordered(*x)))
        }
        _ => None,
    }
}

/// ⭐ PG 兼容 (FMT_VER 7): 索引 key 编码 — 多列索引拼接各列编码
/// (长度前缀防碰撞: `[u16 len][enc]...`). 单列索引保持 `index_val_bytes`
/// 原编码 (兼容存量索引行/bloom 键). 任一列为 NULL → None (不入索引).
pub fn index_vals_bytes(
    schema: &TableSchema,
    idx: &crate::schema::IndexDef,
    values: &[ColValue],
) -> Option<Vec<u8>> {
    if idx.cols.len() == 1 {
        let c = idx.cols[0] as usize;
        return index_val_bytes(schema.columns[c].ty, &values[c]);
    }
    // ⭐ 复合 key: `[IVAL_COMPOSITE][nseg][u16 len][enc]...` — 型别字节让
    // split_index_val 正确切分 (长度前缀防碰撞, 型别字节防误判单值).
    let mut out = Vec::with_capacity(16);
    out.push(crate::keyspace::IVAL_COMPOSITE);
    out.push(idx.cols.len() as u8);
    for &c in &idx.cols {
        let enc = index_val_bytes(schema.columns[c as usize].ty, &values[c as usize])?;
        out.extend_from_slice(&(enc.len() as u16).to_le_bytes());
        out.extend_from_slice(&enc);
    }
    Some(out)
}

impl StorageEngine {
    // =================================================================
    // ⭐ Q1: schema 持久化 + 常驻镜像
    // =================================================================

    /// 设置表 schema: 先落 `[$]` 行再更新内存镜像 (write-through).
    /// 幂等可重复调用 (SetSchema 广播重试安全).
    pub async fn set_schema(
        &mut self,
        db: &str,
        table: &str,
        schema: &TableSchema,
    ) -> Result<(), RegistryError> {
        self.put_physical(db, table, &ks::encode_schema_row(), &schema.encode())
            .await?;
        // ⭐ Y1: 为各索引建空 bloom (建表时刻无数据, 空 bloom 即完备)
        for idx in &schema.indexes {
            self.bloom_entry(db, table, idx.iid);
        }
        self.schema_cache_put(db, table, Some(Arc::new(schema.clone())));
        Ok(())
    }

    /// 读表 schema: 镜像命中零 IO; miss 时 lazy load `[$]` 行,
    /// 无行 = 纯 KV 表 (缓存 None 免重复探盘).
    pub async fn get_schema(
        &mut self,
        db: &str,
        table: &str,
    ) -> Result<Option<Arc<TableSchema>>, RegistryError> {
        if let Some(slot) = self.schema_cache_get(db, table) {
            return Ok(slot.clone());
        }
        let slot = match self.get_physical(db, table, &ks::encode_schema_row()).await? {
            Some(bytes) => Some(Arc::new(TableSchema::decode(&bytes).map_err(se)?)),
            None => None,
        };
        self.schema_cache_put(db, table, slot.clone());
        Ok(slot)
    }

    // =================================================================
    // ⭐ Q4: row 写路径 (row 行 + 索引行同 shard 批内维护)
    // =================================================================

    /// ⭐ 事务 v1 (F61): UNIQUE 探测 (row_put 写前 / TxnApply 预检共用).
    #[allow(clippy::type_complexity)]
    async fn check_unique(
        &mut self,
        db: &str,
        table: &str,
        schema: &crate::schema::TableSchema,
        pk: &[u8],
        values: &[ColValue],
        old_ivals: &Option<Vec<(u32, Option<Vec<u8>>)>>,
    ) -> Result<(), RegistryError> {
        for idx in schema.indexes.iter().filter(|i| i.unique) {
            let Some(nv) = index_vals_bytes(schema, idx, &values) else {
                continue;
            };
            // 值未变 (同 pk 覆盖) 不必探测
            let unchanged = old_ivals
                .as_ref()
                .and_then(|m| m.iter().find(|(iid, _)| *iid == idx.iid))
                .is_some_and(|(_, ov)| ov.as_deref() == Some(nv.as_slice()));
            if unchanged {
                continue;
            }
            let root = self.open_table(db, table).await?.ok_or_else(|| {
                RegistryError::TableNotFound(db.to_string(), table.to_string())
            })?;
            let prefix = ks::index_value_prefix(idx.iid, &nv);
            let mut dup = false;
            crate::registry::table_scan_prefix(self.pager_mut(), root, &prefix, &mut |k, _v| {
                if let Some((_, other_pk)) = ks::split_index_val(&k[5..])
                    && other_pk != pk
                {
                    dup = true;
                    return ControlFlow::Break(());
                }
                ControlFlow::Continue(())
            })
            .await?;
            if dup {
                // ⭐ PG 兼容: 复合唯一 → 列组名
                let colnames = if idx.cols.len() == 1 {
                    schema.columns[idx.cols[0] as usize].name.clone()
                } else {
                    idx.cols
                        .iter()
                        .map(|&c| schema.columns[c as usize].name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                return Err(se(format!("duplicate key on unique columns ({colnames})")));
            }
        }
        Ok(())
    }

    /// ⭐ 事务 v1 (F61): row_put 预检 (不写) — TxnApply 先验后写用.
    /// 检查: schema 存在 / 编码合法 / 旧行类型 / UNIQUE 冲突.
    pub async fn row_put_check(
        &mut self,
        db: &str,
        table: &str,
        pk: &[u8],
        values: &[ColValue],
    ) -> Result<(), RegistryError> {
        let schema = self
            .get_schema(db, table)
            .await?
            .ok_or_else(|| se(format!("table {db}.{table} has no schema")))?;
        let _ = row::encode_row(&schema, values).map_err(se)?;
        let rk = ks::encode_string(pk);
        let old = self.get_physical(db, table, &rk).await?;
        let old_ivals = match &old {
            Some(bytes) if bytes.first() == Some(&row::TAG_ROW) => {
                Some(index_vals_of(&schema, bytes)?)
            }
            Some(_) => return Err(RegistryError::WrongType),
            None => None,
        };
        self.check_unique(db, table, &schema, pk, values, &old_ivals).await
    }

    /// 插入/覆盖一行. `values` 与 schema.columns 一一对应.
    /// 覆盖时先按旧 row 算旧索引值, 变化的索引行删旧写新.
    pub async fn row_put(
        &mut self,
        db: &str,
        table: &str,
        pk: &[u8],
        values: &[ColValue],
    ) -> Result<(), RegistryError> {
        let schema = self
            .get_schema(db, table)
            .await?
            .ok_or_else(|| se(format!("table {db}.{table} has no schema")))?;
        let new_bytes = row::encode_row(&schema, values).map_err(se)?;
        let rk = ks::encode_string(pk);

        // 旧 row (若有) → 旧索引值; 旧行不是 TAG_ROW (纯 KV 写入过) → WRONGTYPE
        let old = self.get_physical(db, table, &rk).await?;
        let old_ivals = match &old {
            Some(bytes) if bytes.first() == Some(&row::TAG_ROW) => {
                Some(index_vals_of(&schema, bytes)?)
            }
            Some(_) => return Err(RegistryError::WrongType),
            None => None,
        };

        // ⭐ O3: UNIQUE 约束 — 写任何行之前拒绝 (防半写状态).
        // 探测仅本 shard: 不同 pk hash 到不同 shard 时漏检 (跨 shard 唯一性
        // gap, v1 文档记录; 真全局唯一需广播探测/全局索引).
        self.check_unique(db, table, &schema, pk, values, &old_ivals).await?;

        // row 行
        // ⭐ M3-1: SQL 用户行 → 行数 +1 (覆盖不加; 索引条目写不计行数)
        let existed = self.put_physical(db, table, &rk, &new_bytes).await?;
        if !existed {
            *self
                .row_counts
                .entry((db.to_string(), table.to_string()))
                .or_insert(0) += 1;
        }

        // 索引行: 逐 IndexDef 对比新旧值, 只动变化的
        for idx in schema.indexes.clone() {
            let new_iv = index_vals_bytes(&schema, &idx, &values);
            let old_iv = old_ivals
                .as_ref()
                .and_then(|m| m.iter().find(|(iid, _)| *iid == idx.iid))
                .and_then(|(_, v)| v.clone());
            if old_iv == new_iv {
                continue; // 值未变 (含双 NULL): 索引行不动
            }
            if let Some(ov) = old_iv {
                self.delete_physical(db, table, &ks::encode_index_entry(idx.iid, &ov, pk))
                    .await?;
            }
            if let Some(nv) = new_iv {
                self.put_physical(db, table, &ks::encode_index_entry(idx.iid, &nv, pk), &[])
                    .await?;
                // ⭐ Y1: 喂 bloom (只增; 删/换值不摘除 → 只累积假阳性)
                let (db_s, tbl_s) = (db.to_string(), table.to_string());
                let b = self.bloom_entry(db, table, idx.iid);
                // ⭐ M3-4: 近似 distinct — bloom miss = 确定新值 → +1 (假阳性低估, 近似)
                let is_new = !b.may_contain(&nv);
                b.insert(&nv);
                if is_new {
                    *self
                        .distinct_counts
                        .entry((db_s.clone(), tbl_s.clone(), idx.iid))
                        .or_insert(0) += 1;
                }
                // ⭐ M3-5: 索引列 min/max (有序字节比较 = 值序; 覆盖写同值不破坏)
                let e = self
                    .range_counts
                    .entry((db_s, tbl_s, idx.iid))
                    .or_insert_with(|| (nv.to_vec(), nv.to_vec()));
                if nv.as_slice() < e.0.as_slice() {
                    e.0 = nv.to_vec();
                }
                if nv.as_slice() > e.1.as_slice() {
                    e.1 = nv.to_vec();
                }
            }
        }
        // ⭐ DIAG: 全操作回查 — 索引更新后确认行仍可达 (NLOG_DIAG=1)
        if crate::chunk_writer::diag_enabled() {
            match self.get_physical(db, table, &rk).await {
                Ok(Some(_)) => {}
                Ok(None) => eprintln!(
                    "[DIAG-ROWPUT-LOST] pk={pk:?} lost after row_put (index updates done)! db={db} table={table}"
                ),
                Err(e) => eprintln!(
                    "[DIAG-ROWPUT-ERR] pk={pk:?} verify error: {e} db={db} table={table}"
                ),
            }
            // ⭐ DIAG canary: per-shard 追踪最近 64 个 pk, 每次插入后全部回查
            {
                thread_local! {
                    static RECENT_PKS: std::cell::RefCell<std::collections::VecDeque<Vec<u8>>> =
                        const { std::cell::RefCell::new(std::collections::VecDeque::new()) };
                    static SHARD_N: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
                }
                let n = SHARD_N.with(|c| { let v = c.get(); c.set(v + 1); v });
                // 每 200 次插入做一次全量回查 (避免太慢)
                if n > 0 && n % 500 == 0 {
                    let pks: Vec<Vec<u8>> = RECENT_PKS.with(|r| r.borrow().iter().cloned().collect());
                    let mut lost = 0;
                    for prev_pk in &pks {
                        let rk = ks::encode_string(prev_pk);
                        match self.get_physical(db, table, &rk).await {
                            Ok(Some(_)) => {}
                            Ok(None) => {
                                lost += 1;
                                if lost <= 3 {
                                    eprintln!(
                                        "[DIAG-CANARY-LOST] pk={prev_pk:?} lost at shard-insert #{n}! db={db} table={table}"
                                    );
                                }
                            }
                            Err(_) => {}
                        }
                    }
                    if lost > 0 {
                        eprintln!("[DIAG-CANARY-SUMMARY] {lost}/{} keys lost at shard-insert #{n} db={db} table={table}", pks.len());
                    }
                }
                RECENT_PKS.with(|r| {
                    let mut r = r.borrow_mut();
                    r.push_back(pk.to_vec());
                    if r.len() > 4000 { r.pop_front(); }
                });
            }
        }
        Ok(())
    }

    /// 读一行 (溢出自动展开). None = 不存在; 非 TAG_ROW → WRONGTYPE.
    pub async fn row_get(
        &mut self,
        db: &str,
        table: &str,
        pk: &[u8],
    ) -> Result<Option<Vec<u8>>, RegistryError> {
        match self.get_physical(db, table, &ks::encode_string(pk)).await? {
            Some(bytes) if bytes.first() == Some(&row::TAG_ROW) => Ok(Some(bytes)),
            Some(_) => Err(RegistryError::WrongType),
            None => Ok(None),
        }
    }

    // =================================================================
    // ⭐ F65: 全局 UNIQUE 占坑原语 (在 email-shard 上; 单线程原子 check-and-reserve)
    // 占坑行: key `[U][iid][enc_val]` → value `[state][txn_id u64 LE][pk]`
    // =================================================================

    /// 占坑结果 (worker 编排依据).
    /// db/table 不存在时先 ensure_table (占坑行与 row 同表名空间).
    async fn unique_slot_get(
        &mut self,
        db: &str,
        table: &str,
        iid: u32,
        enc_val: &[u8],
    ) -> Result<Option<(u8, u64, Vec<u8>)>, RegistryError> {
        let key = ks::unique_slot_key(iid, enc_val);
        match self.get_physical(db, table, &key).await? {
            Some(v) if v.len() >= 9 => {
                let state = v[0];
                let txn = u64::from_le_bytes(v[1..9].try_into().unwrap());
                Ok(Some((state, txn, v[9..].to_vec())))
            }
            _ => Ok(None),
        }
    }

    fn unique_slot_val(state: u8, txn_id: u64, pk: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(9 + pk.len());
        v.push(state);
        v.extend_from_slice(&txn_id.to_le_bytes());
        v.extend_from_slice(pk);
        v
    }

    /// ⭐ F65: check-and-reserve (单线程原子). 返回:
    /// - `Ok(None)`: 占坑成功 (写入 PENDING) 或幂等重入 (同 pk 已 COMMITTED)
    /// - `Ok(Some((state, holder_txn, holder_pk)))`: 冲突, 现有坑信息 (worker 决定校对/拒)
    pub async fn unique_reserve(
        &mut self,
        db: &str,
        table: &str,
        iid: u32,
        enc_val: &[u8],
        pk: &[u8],
        txn_id: u64,
    ) -> Result<Option<(u8, u64, Vec<u8>)>, RegistryError> {
        self.ensure_table(db, table).await?;
        match self.unique_slot_get(db, table, iid, enc_val).await? {
            None => {
                let key = ks::unique_slot_key(iid, enc_val);
                let val = Self::unique_slot_val(1, txn_id, pk); // PENDING
                self.put_physical(db, table, &key, &val).await?;
                Ok(None)
            }
            // 同 pk 已 COMMITTED → 幂等
            Some((2, _, holder)) if holder == pk => Ok(None),
            // 其他情形 (COMMITTED 异 pk / PENDING) → 冲突, 交 worker 处理
            Some(slot) => Ok(Some(slot)),
        }
    }

    /// ⭐ F65: 强制抢占 (worker 回查行确认 stale 后) — 覆写为本 txn PENDING.
    pub async fn unique_steal(
        &mut self,
        db: &str,
        table: &str,
        iid: u32,
        enc_val: &[u8],
        pk: &[u8],
        txn_id: u64,
    ) -> Result<(), RegistryError> {
        self.ensure_table(db, table).await?;
        let key = ks::unique_slot_key(iid, enc_val);
        let val = Self::unique_slot_val(1, txn_id, pk);
        self.put_physical(db, table, &key, &val).await.map(|_| ())
    }

    /// ⭐ F65: PENDING→COMMITTED (写行成功后; 仅 txn+pk 匹配才转, 防误转).
    pub async fn unique_confirm(
        &mut self,
        db: &str,
        table: &str,
        iid: u32,
        enc_val: &[u8],
        pk: &[u8],
        txn_id: u64,
    ) -> Result<(), RegistryError> {
        if let Some((_, txn, holder)) = self.unique_slot_get(db, table, iid, enc_val).await?
            && txn == txn_id
            && holder == pk
        {
            let key = ks::unique_slot_key(iid, enc_val);
            let val = Self::unique_slot_val(2, txn_id, pk); // COMMITTED
            self.put_physical(db, table, &key, &val).await?;
        }
        Ok(())
    }

    /// ⭐ F65: 删坑 (abort 回滚 / DELETE 清坑). txn_id!=0 时仅匹配才删 (防误删);
    /// txn_id==0 无条件删 (DELETE 行时清坑, 不关心是谁的 txn).
    pub async fn unique_release(
        &mut self,
        db: &str,
        table: &str,
        iid: u32,
        enc_val: &[u8],
        txn_id: u64,
    ) -> Result<(), RegistryError> {
        let del = match self.unique_slot_get(db, table, iid, enc_val).await? {
            Some((_, txn, _)) => txn_id == 0 || txn == txn_id,
            None => false,
        };
        if del {
            let key = ks::unique_slot_key(iid, enc_val);
            self.delete_physical(db, table, &key).await?;
        }
        Ok(())
    }

    /// 删一行 (含全部索引行). 返回是否存在.
    pub async fn row_delete(
        &mut self,
        db: &str,
        table: &str,
        pk: &[u8],
    ) -> Result<bool, RegistryError> {
        let schema = self
            .get_schema(db, table)
            .await?
            .ok_or_else(|| se(format!("table {db}.{table} has no schema")))?;
        let rk = ks::encode_string(pk);
        let Some(old) = self.get_physical(db, table, &rk).await? else {
            return Ok(false);
        };
        if old.first() == Some(&row::TAG_ROW) {
            for (iid, iv) in index_vals_of(&schema, &old)? {
                if let Some(iv) = iv {
                    self.delete_physical(db, table, &ks::encode_index_entry(iid, &iv, pk))
                        .await?;
                }
            }
        }
        self.delete_physical(db, table, &rk).await
    }

    /// ⭐ S1: 部分列更新 — shard 端读-改-写 (引擎单线程天然原子).
    /// 不存在 → Ok(false) 不更新; 复用 row_put (UNIQUE 校验/索引跟随全继承).
    /// sets 列号越界/改 pk 由 worker 规划层拦截, 此处防御性校验.
    pub async fn row_update(
        &mut self,
        db: &str,
        table: &str,
        pk: &[u8],
        sets: &[(u16, row::SetVal)],
    ) -> Result<bool, RegistryError> {
        let schema = self
            .get_schema(db, table)
            .await?
            .ok_or_else(|| se(format!("table {db}.{table} has no schema")))?;
        let Some(old) = self.row_get(db, table, pk).await? else {
            return Ok(false);
        };
        let mut values = row::decode_row(&schema, &old).map_err(se)?;
        for (col, sv) in sets {
            let i = *col as usize;
            if i >= values.len() || i == schema.pk_col as usize {
                return Err(se(format!("bad update column {col}")));
            }
            // ⭐ PG 兼容: 表达式对旧行求值 (行内引用用已更新列的前序 set 结果 —
            // PG 语义: 同一 SET 列表按书写序, 后续引用可见前序更新).
            values[i] = match sv {
                row::SetVal::Val(v) => v.clone(),
                row::SetVal::Expr(e) => row::eval_row_expr(e, &values),
            };
        }
        self.row_put(db, table, pk, &values).await?;
        Ok(true)
    }

    /// ⭐ S1: SQL DROP TABLE — 物理删表 + 清 engine 侧派生状态
    /// (schema 镜像 / index bloom / 复合提示位). 返回表是否存在过.
    pub async fn drop_table_sql(&mut self, db: &str, table: &str) -> Result<bool, RegistryError> {
        let existed = self.drop_table(db, table).await?;
        self.purge_table_state(db, table);
        Ok(existed)
    }

    /// ⭐ S2: 全表扫 — 扫 `[S]` 前缀收 TAG_ROW 行 (跳过混入的纯 KV 行),
    /// 返回 (空 val, pk, row_bytes) 与 IndexEntry 同构; limit 0 = 不限.
    /// 溢出行经 row_get 二次展开 (扫描值是溢出描述符时).
    pub async fn table_scan_rows_local(
        &mut self,
        db: &str,
        table: &str,
        limit: usize,
        out: &mut Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
    ) -> Result<(), RegistryError> {
        let Some(root) = self.open_table(db, table).await? else {
            return Ok(());
        };
        let mut hits: Vec<Vec<u8>> = Vec::new(); // pk 列表 (值经回读统一展开溢出)
        crate::registry::table_scan_prefix(
            self.pager_mut(),
            root,
            &[ks::KIND_STRING],
            &mut |k, _v| {
                if let Some(pk) = ks::split_string(k) {
                    hits.push(pk.to_vec());
                    if limit > 0 && hits.len() >= limit {
                        return ControlFlow::Break(());
                    }
                }
                ControlFlow::Continue(())
            },
        )
        .await?;
        // 批量回读 (LeafGuide 复用 + 溢出展开); 非 TAG_ROW 值 (纯 KV) 跳过
        let refs: Vec<&[u8]> = hits.iter().map(|p| p.as_slice()).collect();
        let rows = self.table_get_many(db, table, &refs).await?;
        for (pk, row) in hits.iter().zip(rows) {
            if let Some(rb) = row
                && rb.first() == Some(&row::TAG_ROW)
            {
                out.push((Vec::new(), pk.clone(), rb));
            }
        }
        Ok(())
    }

    /// ⭐ PG 兼容 (范围查): 主键 B+Tree 区间扫描 — 扫 `[KIND_STRING]` 段,
    /// 起点 = `encode_string(pk_enc(lo))` (无 lo 从段头), 上界 = pk 编码
    /// > pk_enc(hi) 即 Break (含界). 返回 (空 val, pk, row_bytes) 与
    /// IndexEntry 同构; limit 0 = 不限. 主键列范围谓词避免全表扫.
    pub async fn pk_scan_local(
        &mut self,
        db: &str,
        table: &str,
        lo: Option<&ColValue>,
        hi: Option<&ColValue>,
        limit: usize,
        out: &mut Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
    ) -> Result<(), RegistryError> {
        let Some(schema) = self.get_schema(db, table).await? else {
            return Ok(());
        };
        let pk_ty = schema.columns[schema.pk_col as usize].ty;
        // 主键保序编码界 — 与 worker `sql_pk_bytes` 同源 (数值 encode_idx/
        // encode_f64_ordered/encode_i128_ordered, 字节串原样; 复合主键 v1 用
        // 首列编码近似, 残余 preds 精确). ⭐ 不能用 index_val_bytes (带类型字节).
        let enc_bound = |b: Option<&ColValue>| -> Result<Option<Vec<u8>>, RegistryError> {
            let Some(v) = b else { return Ok(None) };
            let enc = match (pk_ty, v) {
                (ColType::I64, ColValue::I64(i)) => crate::keyspace::encode_idx(*i).to_vec(),
                (ColType::F64, ColValue::F64(f)) => crate::keyspace::encode_f64_ordered(*f).to_vec(),
                (ColType::Bool | ColType::Date | ColType::Time | ColType::Timestamp, ColValue::I64(i)) => {
                    crate::keyspace::encode_idx(*i).to_vec()
                }
                (ColType::Str | ColType::Bytes | ColType::Json | ColType::Uuid, ColValue::Bytes(b)) => {
                    if b.is_empty() {
                        return Err(se("bad pk bound (empty bytes)"));
                    }
                    b.clone()
                }
                (ColType::Decimal { .. }, ColValue::Decimal(x, _)) => {
                    crate::keyspace::encode_i128_ordered(*x).to_vec()
                }
                _ => return Err(se("bad pk bound type")),
            };
            Ok(Some(crate::keyspace::encode_string(&enc)))
        };
        let lo_enc = enc_bound(lo)?;
        let hi_enc = enc_bound(hi)?;
        let root = self.open_table(db, table).await?.ok_or_else(|| {
            RegistryError::TableNotFound(db.to_string(), table.to_string())
        })?;
        // 起点: 有 lo 用 encode_string(pk_enc(lo)) 精确定位; 无 lo 从 [S] 段头.
        let start = lo_enc.clone().unwrap_or_else(|| vec![ks::KIND_STRING]);
        // 段前缀: [S] 保证扫描不越入其他 kind (Hash/List/...)
        let seg = vec![ks::KIND_STRING];
        let mut n = 0usize;
        crate::registry::table_scan_range(
            self.pager_mut(),
            root,
            &start,
            &seg,
            &mut |k, _v| {
                // 上界: 扫描越过 [KIND_STRING] 段 (非字符串 key) 或 pk 编码 > hi
                if k.first() != Some(&ks::KIND_STRING) {
                    return ControlFlow::Break(());
                }
                if let Some(h) = &hi_enc {
                    // k = [S][klen][pk_enc], 与 [S][klen'][pk_enc(hi)] 整体比较
                    if k > h.as_slice() {
                        return ControlFlow::Break(());
                    }
                }
                if let Some(pk) = ks::split_string(k) {
                    out.push((Vec::new(), pk.to_vec(), Vec::new()));
                    n += 1;
                    if limit > 0 && n >= limit {
                        return ControlFlow::Break(());
                    }
                }
                ControlFlow::Continue(())
            },
        )
        .await?;
        // 回读行 (批量, 溢出展开)
        let refs: Vec<&[u8]> = out.iter().map(|(_, pk, _)| pk.as_slice()).collect();
        let rows = self.table_get_many(db, table, &refs).await?;
        for ((_, _, rb), row) in out.iter_mut().zip(rows) {
            if let Some(r) = row
                && r.first() == Some(&row::TAG_ROW)
            {
                *rb = r;
            } else {
                *rb = Vec::new(); // 非 TAG_ROW → 过滤
            }
        }
        Ok(())
    }

    /// ⭐ F67/F68/F70 (JOIN): 带谓词+投影下推的本地扫. 行来源优先级:
    /// key_set (多键索引点查) > hint (单区间范围扫) > 全表扫; 三者都 decode 行
    /// → preds AND 过滤 (NULL 恒 false) → 按 proj 取列 → 收集. limit 0 = 不限.
    #[allow(clippy::too_many_arguments)]
    pub async fn table_scan_filtered_local(
        &mut self,
        db: &str,
        table: &str,
        preds: &[ScanPred],
        proj: &[u16],
        hint: Option<&IndexHint>,
        key_set: Option<&KeySetHint>,
        limit: usize,
        out: &mut Vec<Vec<ColValue>>,
    ) -> Result<(), RegistryError> {
        let Some(schema) = self.get_schema(db, table).await? else {
            return Ok(());
        };
        // 行来源: key_set 多键点查 > hint 范围扫 > 全表扫
        let raw_rows: Vec<Vec<u8>> = if let Some(ks) = key_set {
            self.index_multi_point_local(db, table, ks.iid, &ks.keys).await?
        } else if let Some(h) = hint {
            if h.pk {
                // ⭐ PG 兼容: 主键区间扫描
                let mut pairs: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::new();
                self.pk_scan_local(db, table, h.lo.as_ref(), h.hi.as_ref(), 0, &mut pairs)
                    .await?;
                pairs.into_iter().map(|(_v, _pk, rb)| rb).collect()
            } else {
                let pairs = self
                    .index_scan_local(db, table, h.iid, h.lo.as_ref(), h.hi.as_ref(), 0)
                    .await?;
                pairs.into_iter().map(|(_pk, rb)| rb).collect()
            }
        } else {
            let scan_limit = if preds.is_empty() { limit } else { 0 };
            let mut raw: Vec<IndexEntry> = Vec::new();
            self.table_scan_rows_local(db, table, scan_limit, &mut raw).await?;
            raw.into_iter().map(|(_v, _pk, rb)| rb).collect()
        };
        for rb in raw_rows {
            let cols = match row::decode_row(&schema, &rb) {
                Ok(c) => c,
                Err(_) => continue, // 坏行跳过 (防御)
            };
            if !row_pass_preds(&cols, preds) {
                continue;
            }
            let projected: Vec<ColValue> =
                proj.iter().map(|&i| cols.get(i as usize).cloned().unwrap_or(ColValue::Null)).collect();
            out.push(projected);
            if limit > 0 && out.len() >= limit {
                break;
            }
        }
        Ok(())
    }

    /// ⭐ 修复 (2026-08): DML phase1 范围扫 — 与 table_scan_filtered_local 同走
    /// 索引/主键范围, 但返回完整 `(索引原值, pk, row_bytes)` 三元组, 供 worker
    /// collect_dml_pks 提取 pk 执行 phase2. (ScanFiltered 只回投影列值, 无 pk.)
    pub async fn table_scan_filtered_rows_local(
        &mut self,
        db: &str,
        table: &str,
        hint: Option<&IndexHint>,
        limit: usize,
        out: &mut Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
    ) -> Result<(), RegistryError> {
        let Some(_schema) = self.get_schema(db, table).await? else {
            return Ok(());
        };
        if let Some(h) = hint {
            if h.pk {
                // 主键区间扫描 → (v, pk, rb)
                self.pk_scan_local(db, table, h.lo.as_ref(), h.hi.as_ref(), limit, out).await?;
            } else {
                // 二级索引范围 → (pk, rb), 转成 (v=pk, pk, rb) 满足三元组
                let pairs = self
                    .index_scan_local(db, table, h.iid, h.lo.as_ref(), h.hi.as_ref(), limit)
                    .await?;
                for (pk, rb) in pairs {
                    out.push((pk.clone(), pk, rb));
                }
            }
        }
        Ok(())
    }

    /// ⭐ F70 (JOIN): 多键等值索引点查 — 对每键索引点查收 pk (bloom 短路免 travel),
    /// 去重后一次 table_get_many 批量回表. 返回命中行 row_bytes.
    pub async fn index_multi_point_local(
        &mut self,
        db: &str,
        table: &str,
        iid: u32,
        keys: &[ColValue],
    ) -> Result<Vec<Vec<u8>>, RegistryError> {
        let mut pk_seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        let mut pks: Vec<Vec<u8>> = Vec::new();
        for k in keys {
            let hit = self.index_scan_pks_local(db, table, iid, Some(k), Some(k), 0).await?;
            for pk in hit {
                if pk_seen.insert(pk.clone()) {
                    pks.push(pk);
                }
            }
        }
        let refs: Vec<&[u8]> = pks.iter().map(|p| p.as_slice()).collect();
        let rows = self.table_get_many(db, table, &refs).await?;
        Ok(rows.into_iter().flatten().collect())
    }

    // =================================================================
    // ⭐ Q4: 本地索引扫描 (+ 本地回表; 禁止两跳的 shard 内闭环)
    // =================================================================

    /// 本地索引范围扫描 + 回表: 返回 `(pk, row_bytes)` 按索引值升序.
    /// `lo`/`hi` 为闭区间界 (None = 无界); `limit` 0 = 不限.
    /// 等值查询即 `lo == hi`. NULL 界非法 (NULL 不入索引).
    /// ⭐ PERF: 回表走 `table_get_many` (排序 + LeafGuide 区间复用,
    /// 同 leaf 的多个 pk 只 travel 一次), 不再逐 pk 独立树遍历.
    pub async fn index_scan_local(
        &mut self,
        db: &str,
        table: &str,
        iid: u32,
        lo: Option<&ColValue>,
        hi: Option<&ColValue>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RegistryError> {
        let pks = self
            .index_scan_pks_local(db, table, iid, lo, hi, limit)
            .await?;
        // 本地批量回表 (pk 与索引行同 shard — co-location 保证)
        let refs: Vec<&[u8]> = pks.iter().map(|p| p.as_slice()).collect();
        let rows = self.table_get_many(db, table, &refs).await?;
        Ok(pks
            .into_iter()
            .zip(rows)
            // 索引行存在但 row 缺失 = crash 窗口残留, 跳过 (文档记录)
            .filter_map(|(pk, row)| row.map(|r| (pk, r)))
            .collect())
    }

    /// 覆盖索引路径: 只返回 `(原值字节, pk)` 免回表.
    pub async fn index_scan_keys_local(
        &mut self,
        db: &str,
        table: &str,
        iid: u32,
        lo: Option<&ColValue>,
        hi: Option<&ColValue>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RegistryError> {
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        self.index_scan_raw(db, table, iid, lo, hi, limit, &mut |k| {
            if let Some((_, val, pk)) = ks::split_index_entry(k) {
                out.push((val, pk.to_vec()));
            }
        })
        .await?;
        Ok(out)
    }

    /// ⭐ Q5: 广播聚合用 — 返回 `(原值字节, pk, row_bytes)` 三元组
    /// (`with_rows = false` 时 row 为空, 覆盖索引免回表).
    /// 原值字节保序 (数值 = 8B 保序编码, 字节串 = 原字节), 跨 shard 归并
    /// 直接按 (val, pk) 排序即全局索引序.
    #[allow(clippy::too_many_arguments)]
    pub async fn index_scan_entries_local(
        &mut self,
        db: &str,
        table: &str,
        iid: u32,
        lo: Option<&ColValue>,
        hi: Option<&ColValue>,
        limit: usize,
        with_rows: bool,
    ) -> Result<Vec<IndexEntry>, RegistryError> {
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        self.index_scan_raw(db, table, iid, lo, hi, limit, &mut |k| {
            if let Some((_, val, pk)) = ks::split_index_entry(k) {
                entries.push((val, pk.to_vec()));
            }
        })
        .await?;
        if !with_rows {
            return Ok(entries.into_iter().map(|(v, p)| (v, p, Vec::new())).collect());
        }
        // ⭐ PERF: 批量回表 (LeafGuide 区间复用) — 广播查询大结果集的主要成本
        let refs: Vec<&[u8]> = entries.iter().map(|(_, p)| p.as_slice()).collect();
        let rows = self.table_get_many(db, table, &refs).await?;
        Ok(entries
            .into_iter()
            .zip(rows)
            // crash 窗口残留索引行 (row 缺失) 跳过
            .filter_map(|((val, pk), row)| row.map(|r| (val, pk, r)))
            .collect())
    }

    /// 收集命中的 PK 列表 (升序).
    async fn index_scan_pks_local(
        &mut self,
        db: &str,
        table: &str,
        iid: u32,
        lo: Option<&ColValue>,
        hi: Option<&ColValue>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, RegistryError> {
        let mut pks: Vec<Vec<u8>> = Vec::new();
        self.index_scan_raw(db, table, iid, lo, hi, limit, &mut |k| {
            if let Some((_, pk)) = ks::split_index_val(&k[5..]) {
                pks.push(pk.to_vec());
            }
        })
        .await?;
        Ok(pks)
    }

    /// 扫描骨架: 起点 = `[I][iid][enc(lo)]` (无 lo 从 iid 前缀头),
    /// 上界 = enc_val 段 memcmp > enc(hi) 即 Break (含界).
    #[allow(clippy::too_many_arguments)]
    async fn index_scan_raw(
        &mut self,
        db: &str,
        table: &str,
        iid: u32,
        lo: Option<&ColValue>,
        hi: Option<&ColValue>,
        limit: usize,
        on_entry: &mut dyn FnMut(&[u8]),
    ) -> Result<(), RegistryError> {
        let schema = self
            .get_schema(db, table)
            .await?
            .ok_or_else(|| se(format!("table {db}.{table} has no schema")))?;
        let idx = schema
            .indexes
            .iter()
            .find(|i| i.iid == iid)
            .cloned()
            .ok_or_else(|| se(format!("index {iid} not found")))?;
        let ty = schema.columns[idx.col as usize].ty;
        let enc_bound = |b: Option<&ColValue>| -> Result<Option<Vec<u8>>, RegistryError> {
            b.map(|v| index_val_bytes(ty, v).ok_or_else(|| se("bad index bound type")))
                .transpose()
        };
        let lo_enc = enc_bound(lo)?;
        let hi_enc = enc_bound(hi)?;

        // ⭐ Y1: 等值扫 (lo == hi) 先查本地 bloom — miss 断言不存在,
        // 免 BTree travel 直接回空 (无假阴性: 见 index_bloom 模块头).
        if let (Some(l), Some(h)) = (&lo_enc, &hi_enc)
            && l == h
            && !self.bloom_may_contain(db, table, iid, l)
        {
            self.bloom_skip_count += 1;
            return Ok(());
        }

        let root = self.open_table(db, table).await?.ok_or_else(|| {
            RegistryError::TableNotFound(db.to_string(), table.to_string())
        })?;
        let prefix = ks::index_prefix(iid);
        let start = match &lo_enc {
            Some(l) => ks::index_value_prefix(iid, l),
            None => prefix.clone(),
        };
        let mut n = 0usize;
        crate::registry::table_scan_range(self.pager_mut(), root, &start, &prefix, &mut |k, _v| {
            // 上界: enc_val 段与 hi_enc 比较 (memcmp 保序; 等值含界)
            if let Some(hi) = &hi_enc {
                match ks::split_index_val(&k[5..]) {
                    Some((ev, _)) if ev <= hi.as_slice() => {}
                    _ => return ControlFlow::Break(()),
                }
            }
            on_entry(k);
            n += 1;
            if limit > 0 && n >= limit {
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        })
        .await
    }
}

/// 从存量 row 字节算全部索引值: `[(iid, Some(enc_val) | None-NULL)]`.
fn index_vals_of(
    schema: &TableSchema,
    row_bytes: &[u8],
) -> Result<IndexValSnapshot, RegistryError> {
    schema
        .indexes
        .iter()
        .map(|idx| {
            // ⭐ PG 兼容 (FMT_VER 7): 复合索引 → 拼接各列编码 (长度前缀)
            if idx.cols.len() == 1 {
                let c = idx.cols[0];
                let v = row::read_col(schema, row_bytes, c).map_err(se)?;
                return Ok((idx.iid, index_val_bytes(schema.columns[c as usize].ty, &v)));
            }
            let mut out = Vec::with_capacity(16);
            out.push(crate::keyspace::IVAL_COMPOSITE);
            out.push(idx.cols.len() as u8);
            let mut any_null = false;
            for &c in &idx.cols {
                let v = row::read_col(schema, row_bytes, c).map_err(se)?;
                let Some(e) = index_val_bytes(schema.columns[c as usize].ty, &v) else {
                    any_null = true;
                    break;
                };
                out.extend_from_slice(&(e.len() as u16).to_le_bytes());
                out.extend_from_slice(&e);
            }
            Ok((idx.iid, if any_null { None } else { Some(out) }))
        })
        .collect()
}
