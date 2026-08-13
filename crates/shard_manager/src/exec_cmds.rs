// ⭐ 解耦 2026-08: shard 命令执行 (从 manager.rs 拆出).
// 职责: 各存储命令的 RMW/扫描/事务应用 (exec_incr/exec_append/exec_txn_apply/
// exec_task_op 等), 操作 StorageEngine, 与 ShardManager 状态解耦.
use crate::manager::block_on_io;
use std::ops::ControlFlow;
use storage::StorageEngine;

pub(crate) fn exec_incr(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    delta: i64,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    use crate::value_num;
    let old = match block_on_io(e.table_get(db, table, key)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    let cur: i64 = match &old {
        None => 0,
        Some(stored) => match value_num::parse_num_int_only(stored) {
            Some(n) => n,
            None => {
                return BatchResult::Error("value is not an integer or out of range".to_string());
            }
        },
    };
    let Some(newv) = cur.checked_add(delta) else {
        return BatchResult::Error("increment or decrement would overflow".to_string());
    };
    // ⭐ 原生二进制写回 (TAG_I64 + 8B LE), 非十进制字符串
    let stored = value_num::encode_i64(newv);
    match block_on_io(e.table_put(db, table, key, &stored)) {
        Ok(_) => BatchResult::Integer(newv),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ String RMW: INCRBYFLOAT (N2). 按 tag 分派, 结果写回 TAG_F64 8B LE.
///
/// Redis 语义: 整数值/文本数字均可参与; 结果 NaN/inf 拒绝.
pub(crate) fn exec_incr_float(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    delta: f64,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    use crate::value_num::{self, NumValue};
    let old = match block_on_io(e.table_get(db, table, key)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    let cur: f64 = match &old {
        None => 0.0,
        Some(stored) => match value_num::parse_num(stored) {
            Some(NumValue::I64(n)) => n as f64,
            Some(NumValue::F64(f)) => f,
            Some(NumValue::F32(f)) => f as f64, // f32 → f64 无损提升
            None => return BatchResult::Error("value is not a valid float".to_string()),
        },
    };
    let newv = cur + delta;
    if !newv.is_finite() {
        return BatchResult::Error("increment would produce NaN or Infinity".to_string());
    }
    // ⭐ 原生二进制写回 (TAG_F64 + 8B LE)
    let stored = value_num::encode_f64(newv);
    match block_on_io(e.table_put(db, table, key, &stored)) {
        Ok(_) => BatchResult::Double(newv),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ String RMW: APPEND. 返回追加后 payload 长度.
///
/// ⭐ N2: 数值 tag 值先 `render` 字符串化再拼接, 结果退回 TAG_RAW
/// (Redis 语义: append 后是普通 string).
pub(crate) fn exec_append(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    suffix: &[u8],
) -> crate::request::BatchResult {
    use crate::request::{BatchResult, VALUE_TAG_RAW};
    use crate::value_num;
    let old = match block_on_io(e.table_get(db, table, key)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    let mut stored = match old {
        Some(s) if s.first() == Some(&VALUE_TAG_RAW) => s,
        Some(s) => {
            // 数值 tag / 无 tag 存量: 渲染字符串化后归一为 RAW
            let rendered = value_num::render(&s);
            let mut t = Vec::with_capacity(1 + rendered.len());
            t.push(VALUE_TAG_RAW);
            t.extend_from_slice(&rendered);
            t
        }
        None => vec![VALUE_TAG_RAW],
    };
    stored.extend_from_slice(suffix);
    let new_len = (stored.len() - 1) as i64;
    match block_on_io(e.table_put(db, table, key, &stored)) {
        Ok(_) => BatchResult::Integer(new_len),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ SETNX: 不存在才写. 返回 1=写入, 0=已存在.
/// 使用 table_exists (零 value 物化) 代替 table_get.
pub(crate) fn exec_setnx(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    val: &[u8],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match block_on_io(e.table_exists(db, table, key)) {
        Ok(true) => BatchResult::Integer(0),
        Ok(false) => match block_on_io(e.table_put(db, table, key, val)) {
            Ok(_) => BatchResult::Integer(1),
            Err(err) => BatchResult::Error(err.to_string()),
        },
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ GETDEL: 返回旧值 + 删除 (table_delete 内部释放溢出链).
pub(crate) fn exec_getdel(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    let old = match block_on_io(e.table_get(db, table, key)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    if old.is_some()
        && let Err(err) = block_on_io(e.table_delete(db, table, key))
    {
        return BatchResult::Error(err.to_string());
    }
    BatchResult::GetValue(old)
}

/// ⭐ GETSET: 写新值 + 返回旧值 (val 已带 tag).
pub(crate) fn exec_getset(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    val: &[u8],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    let old = match block_on_io(e.table_get(db, table, key)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    match block_on_io(e.table_put(db, table, key, val)) {
        Ok(_) => BatchResult::GetValue(old),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Phase B: SETBIT — shard 端 RMW (零扩展到 offset/8+1), 返回旧 bit.
pub(crate) fn exec_setbit(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    offset: u64,
    bit: bool,
) -> crate::request::BatchResult {
    use crate::request::{BatchResult, VALUE_TAG_RAW};
    use crate::value_num;
    let old = match block_on_io(e.table_get(db, table, key)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    let mut base: Vec<u8> = match &old {
        Some(s) => value_num::render(s).into_owned(),
        None => Vec::new(),
    };
    let byte = (offset / 8) as usize;
    let mask = 1u8 << (7 - (offset % 8) as u8);
    if base.len() <= byte {
        base.resize(byte + 1, 0u8); // 零扩展
    }
    let old_bit = (base[byte] & mask) != 0;
    if bit {
        base[byte] |= mask;
    } else {
        base[byte] &= !mask;
    }
    let mut stored = Vec::with_capacity(1 + base.len());
    stored.push(VALUE_TAG_RAW);
    stored.extend_from_slice(&base);
    match block_on_io(e.table_put(db, table, key, &stored)) {
        Ok(_) => BatchResult::Integer(i64::from(old_bit)),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ X2: 数据面 schema 分发 — decode 校验后落 `[$]` 行 + 常驻镜像 (幂等).
/// 表由 exec 前置的惰性建表保证存在.
pub(crate) fn exec_set_schema(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    bytes: &[u8],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match storage::schema::TableSchema::decode(bytes) {
        Ok(schema) => match block_on_io(e.set_schema(db, table, &schema)) {
            Ok(()) => BatchResult::PutOk,
            Err(err) => BatchResult::Error(err.to_string()),
        },
        Err(err) => BatchResult::Error(format!("bad schema bytes: {err}")),
    }
}

/// ⭐ X2: 读表 schema 字节 (worker 缓存 miss 拉取; None = 纯 KV 表).
pub(crate) fn exec_get_schema(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match block_on_io(e.get_schema(db, table)) {
        Ok(s) => BatchResult::GetValue(s.map(|s| s.encode())),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Q5: RowPut — shard 端引擎内部维护 row 行 + 索引行 (同 shard 原子).
pub(crate) fn exec_row_put(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    pk: &[u8],
    values: &[storage::row::ColValue],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match block_on_io(e.row_put(db, table, pk, values)) {
        Ok(()) => BatchResult::PutOk,
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Phase Scan: 列出表内 String 类型 user keys (BTree 有序, 跨 leaf 游标).
///
/// 行为契约:
/// - 只走 `[S][varint klen][user_key]` 前缀 (KEYSPACE::KIND_STRING), 不跨
///   H/L/T/Z 复合结构 (与 HKEYS/SMEMBERS/LRANGE/ZRANGE 互不重叠).
/// - `prefix` 为空 = 全表 string key; 非空 = user_key 字节序以此开头 (BTree
///   物理 key 含 `[S][klen]` 公共前缀, 同长度 user_key 共享, 跨长度通过
///   `split_string` 解析后再回退到 bytewise 前缀比较).
/// - `with_values = true` 时按 user_key 逐个 `table_get_typed` 拉 stored value
///   (含 type tag; 未知 key 返回 `None`, 与 HSet/HGetAll 的 (None) 区分).
/// - `limit > 0` 时在 callback 内 `Break` 早停.
/// ⭐ Phase Scan: 列出表内 String 类型 user keys (BTree 有序, 跨 leaf 游标).
///
/// 行为契约:
/// - 只走 `[S][varint klen][user_key]` 前缀 (KEYSPACE::KIND_STRING), 不跨
///   H/L/T/Z 复合结构 (与 HKEYS/SMEMBERS/LRANGE/ZRANGE 互不重叠).
/// - 范围闭开 `[start, end)` (BTree 字节序): start 空 = 从头; end 空 = 到尾.
/// - `prefix` 为空 = 不做前缀过滤; 非空 = user_key 必须以此前缀开头
///   (BTree 物理 key 共享 `[S][klen]` 前缀, 跨长度由 `split_string` 剥出后回退
///   到 user_key 字节序 starts_with 检查).
/// - `with_values = true` 时按 user_key 逐个 `table_get` 拉 stored value
///   (含 type tag; 未知 key 返回 `None`, 与 HSet/HGetAll 的 (None) 区分).
/// - `limit > 0` 时在 callback 内 `Break` 早停.
///
/// **实现权衡**: 范围 / 前缀 / limit 都在 callback 内做 post-filter — BTree
/// 不知道 user 提供的 `start` 对应的 varint klen 是什么, 物理 start 难拼; 用
/// callback 过滤换来正确性. 几十到几百条规模下 cost 忽略; 大规模时可在
/// `exec_scan_keys` 顶部按 `start` 长度构造物理 start, 跳到对应 leaf 起点.
pub(crate) fn exec_scan_keys(
    e: &mut StorageEngine,
    db: &str,
    table: &str,
    start: &[u8],
    end: &[u8],
    prefix: &[u8],
    limit: u32,
    with_values: bool,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    // 1. open_table 找不到 → 空集 (与 table_get_typed 一致: 表不存在视为无 key).
    let root = match block_on_io(e.open_table(db, table)) {
        Ok(Some(v)) => v,
        Ok(None) => {
            return if with_values {
                BatchResult::KeysWithValues(Vec::new())
            } else {
                BatchResult::Keys(Vec::new())
            };
        }
        Err(err) => return BatchResult::Error(err.to_string()),
    };

    // 2. 物理扫描前缀 = `[KIND_STRING]`. 跨 leaf 游标由 btree_scan 处理.
    //    范围 / 前缀 / limit 都在 callback 里做 post-filter (BTree 不知道 user
    //    start 的 varint klen 是什么, 物理 start 难拼; 用 callback 过滤换来
    //    正确性 — 几十到几百条规模下 cost 忽略).
    let scan_prefix = [storage::keyspace::KIND_STRING];
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let scan_result = block_on_io(storage::registry::table_scan_prefix(
        e.pager_mut(),
        root,
        &scan_prefix,
        &mut |pkey, _stored| {
            // 物理 = [S][varint klen][user_key], 剥前缀拿 user_key.
            let Some(uk) = storage::keyspace::split_string(pkey) else {
                return ControlFlow::Continue(());
            };
            // 范围闭开 [start, end):
            // - start 空: 不做下界过滤; 非空: uk < start → 跳过.
            // - end 空: 不做上界; 非空: uk >= end → 停 (BTree 后面可能还有更小的,
            //   但本表 BTree 序保证 uk 之后只会 >= 当前 uk, 故安全 break).
            if !start.is_empty() && uk < start {
                return ControlFlow::Continue(());
            }
            if !end.is_empty() && uk >= end {
                return ControlFlow::Break(());
            }
            // 前缀过滤 (post-filter).
            if !prefix.is_empty() && !uk.starts_with(prefix) {
                return ControlFlow::Continue(());
            }
            if limit > 0 && keys.len() >= limit as usize {
                return ControlFlow::Break(());
            }
            keys.push(uk.to_vec());
            ControlFlow::Continue(())
        },
    ));
    if let Err(err) = scan_result {
        return BatchResult::Error(err.to_string());
    }

    if !with_values {
        return BatchResult::Keys(keys);
    }

    // 3. 逐 key 拉 stored value (含 type tag, 长度可能 0, 调用方按 tag 解释).
    let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(keys.len());
    for k in &keys {
        match block_on_io(e.table_get(db, table, k)) {
            Ok(Some(v)) => out.push((k.clone(), v)),
            Ok(None) => out.push((k.clone(), Vec::new())), // 与"扫到但被并发删"区分
            Err(err) => return BatchResult::Error(err.to_string()),
        }
    }
    BatchResult::KeysWithValues(out)
}

/// ⭐ Q5: IndexScan — shard 内闭环 "本地索引扫 → 本地回表" (禁止两跳).
#[allow(clippy::too_many_arguments)]
pub(crate) fn exec_index_scan(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    iid: u32,
    lo: Option<&storage::row::ColValue>,
    hi: Option<&storage::row::ColValue>,
    limit: u32,
    with_rows: bool,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match block_on_io(e.index_scan_entries_local(db, table, iid, lo, hi, limit as usize, with_rows))
    {
        Ok(rows) => BatchResult::Rows(rows),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ 事务 v1 (F61): COMMIT 原子批 — 先验后写.
///
/// 预检 (零部分应用红线):
/// 1. 全部 op 的表 ensure_table (惰性建表)
/// 2. RowPut 逐个 row_put_check (schema/编码/类型/UNIQUE)
/// 3. 批内自冲突: 不同 pk 写同一 unique 值 (互相看不见盘上探测) → 拒
///
/// 应用: 逐 op 执行 (shard 单线程 = 批内零并发穿插); 预检后仅剩 IO 级
/// 失败 (灾难态, 回复标注 partially applied). 完成后无条件 wal_barrier
/// (事务语义: 回复到达 ⇒ 已持久; wal_mode=off 时退化, 文档化).
pub(crate) fn exec_txn_apply(
    e: &mut StorageEngine,
    ops: Vec<crate::request::BatchOp>,
    read_set: Vec<crate::request::ReadCheck>,
) -> crate::request::BatchResult {
    use crate::request::{BatchOp, BatchResult};
    // --- ⭐ v2 (F62): OCC 读集验证 (SERIALIZABLE) — 重读比对指纹,
    // 变了整批拒 (shard 单线程: 验证+应用之间零并发窗口) ---
    for rc in &read_set {
        // 表已删/不存在 → 当作行不存在 (读时若存在则必冲突)
        let cur = block_on_io(e.row_get(&rc.db, &rc.table, &rc.pk)).unwrap_or_default();
        let cur_fp = cur.as_deref().map(storage::wal::crc32);
        if cur_fp != rc.fp {
            return BatchResult::Error(
                "serialization failure: concurrent update detected (retry transaction)".into(),
            );
        }
    }
    // --- 预检 ---
    let mut batch_uniques: std::collections::HashMap<(String, u32, Vec<u8>), Vec<u8>> =
        std::collections::HashMap::new();
    for op in &ops {
        let (db, table, _) = op.locator();
        if let Err(err) = block_on_io(e.ensure_table(db, table)) {
            return BatchResult::Error(err.to_string());
        }
        if let BatchOp::RowPut {
            db,
            table,
            pk,
            values,
        } = op
        {
            if let Err(err) = block_on_io(e.row_put_check(db, table, pk, values)) {
                return BatchResult::Error(err.to_string());
            }
            // 批内自冲突 (盘上探测看不见未应用的同批写)
            if let Ok(Some(schema)) = block_on_io(e.get_schema(db, table)) {
                for idx in schema.indexes.iter().filter(|i| i.unique) {
                    if let Some(nv) = storage::sql_rows::index_vals_bytes(&schema, idx, values) {
                        let key = (format!("{db}\u{0}{table}"), idx.iid, nv);
                        if let Some(prev) = batch_uniques.insert(key, pk.clone())
                            && &prev != pk
                        {
                            return BatchResult::Error(format!(
                                "duplicate key on unique column '{}' (within transaction)",
                                schema.columns[idx.col as usize].name
                            ));
                        }
                    }
                }
            }
        }
    }
    // --- 应用 ---
    let n = ops.len() as u64;
    for op in ops {
        if let BatchResult::Error(err) = exec_task_op(e, op) {
            return BatchResult::Error(format!("txn partially applied (IO-level): {err}"));
        }
    }
    // --- 事务持久化屏障 (独立于 wal_mode strict/periodic) ---
    if let Err(err) = block_on_io(e.wal_barrier()) {
        return BatchResult::Error(format!("txn applied but WAL sync failed: {err}"));
    }
    BatchResult::TxnApplied(n)
}

/// ⭐ 事务 v1 (F61): 单 op 执行 — ShardTask 热路径与 TxnApply 原子批共用.
/// 从 shard_thread_main 的 ShardTask 臂原样提取, 行为零变化.
pub(crate) fn exec_task_op(
    e: &mut StorageEngine,
    op: crate::request::BatchOp,
) -> crate::request::BatchResult {
    match op {
        crate::request::BatchOp::Put {
            ref db,
            ref table,
            ref key,
            ref val,
        } => match block_on_io(e.table_put(db, table, key, val)) {
            Ok(_) => crate::request::BatchResult::PutOk,
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::Get {
            ref db,
            ref table,
            ref key,
        } => {
            // ⭐ Phase H: 类型感知 (hash key → WRONGTYPE)
            match block_on_io(e.table_get_typed(db, table, key)) {
                Ok(v) => crate::request::BatchResult::GetValue(v),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::Delete {
            ref db,
            ref table,
            ref key,
        } => {
            // ⭐ Phase H: 类型感知 (顺带清 hash 全部行/孤儿行)
            match block_on_io(e.key_delete_any(db, table, key)) {
                Ok(b) => crate::request::BatchResult::DeleteExisted(b),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        // ⭐ MGET/MSET 分片: shard 内 LeafGuide 区间复用批量执行
        crate::request::BatchOp::MultiGet {
            ref db,
            ref table,
            ref keys,
        } => {
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
            match block_on_io(e.table_get_many(db, table, &refs)) {
                Ok(vs) => crate::request::BatchResult::Values(vs),
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::MultiPut {
            ref db,
            ref table,
            ref pairs,
        } => match block_on_io(e.table_put_many(db, table, pairs)) {
            Ok(_) => crate::request::BatchResult::MultiPutOk,
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::MultiPutNx {
            ref db,
            ref table,
            ref pairs,
        } => exec_multiputnx(e, db, table, pairs),
        // ⭐ String RMW (shard 单线程内天然原子)
        crate::request::BatchOp::Incr {
            ref db,
            ref table,
            ref key,
            delta,
        } => exec_incr(e, db, table, key, delta),
        crate::request::BatchOp::IncrFloat {
            ref db,
            ref table,
            ref key,
            delta,
        } => exec_incr_float(e, db, table, key, delta),
        crate::request::BatchOp::Append {
            ref db,
            ref table,
            ref key,
            ref suffix,
        } => exec_append(e, db, table, key, suffix),
        crate::request::BatchOp::SetNx {
            ref db,
            ref table,
            ref key,
            ref val,
        } => exec_setnx(e, db, table, key, val),
        crate::request::BatchOp::GetDel {
            ref db,
            ref table,
            ref key,
        } => exec_getdel(e, db, table, key),
        crate::request::BatchOp::GetSet {
            ref db,
            ref table,
            ref key,
            ref val,
        } => exec_getset(e, db, table, key, val),
        crate::request::BatchOp::SetRange {
            ref db,
            ref table,
            ref key,
            offset,
            ref data,
        } => exec_setrange(e, db, table, key, offset, data),
        // ⭐ M3-2 (CBO): 表近似行数 (内存增量统计; 未统计=0)
        crate::request::BatchOp::EstimateRowCount { ref db, ref table } => {
            crate::request::BatchResult::RowCount(e.estimate_row_count(db, table).unwrap_or(0))
        }
        // ⭐ M3-4 (CBO): 索引列 distinct (worker 已算好 iid; 未统计=0)
        crate::request::BatchOp::EstimateDistinct {
            ref db,
            ref table,
            ref iids,
        } => crate::request::BatchResult::DistinctCounts(
            iids.iter()
                .map(|iid| e.estimate_distinct(db, table, *iid).unwrap_or(0))
                .collect(),
        ),
        // ⭐ M3-5 (CBO): 索引列 (min, max) 有序字节 (未统计 = (None, None))
        crate::request::BatchOp::EstimateRanges {
            ref db,
            ref table,
            ref iids,
        } => crate::request::BatchResult::RangeBounds(
            iids.iter()
                .map(|iid| {
                    e.estimate_range(db, table, *iid)
                        .map(|(lo, hi)| (Some(lo), Some(hi)))
                        .unwrap_or((None, None))
                })
                .collect(),
        ),
        // ⭐ Phase Scan: 列出 String user keys (跨 shard 由 ShardManager::scan 归并)
        crate::request::BatchOp::ScanKeys {
            ref db,
            ref table,
            ref start,
            ref end,
            ref prefix,
            limit,
            with_values,
        } => exec_scan_keys(e, db, table, start, end, prefix, limit, with_values),
        // ⭐ Phase H: Hash ops (单 key 单 shard, 无需聚合)
        crate::request::BatchOp::HSet {
            ref db,
            ref table,
            ref key,
            ref pairs,
        } => match block_on_io(e.hash_set(db, table, key, pairs)) {
            Ok(n) => crate::request::BatchResult::Integer(n),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::HSetNx {
            ref db,
            ref table,
            ref key,
            ref field,
            ref val,
        } => match block_on_io(e.hash_set_nx(db, table, key, field, val)) {
            Ok(n) => crate::request::BatchResult::Integer(n),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::HGet {
            ref db,
            ref table,
            ref key,
            ref field,
        } => match block_on_io(e.hash_get(db, table, key, field)) {
            Ok(v) => crate::request::BatchResult::GetValue(v),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::HMGet {
            ref db,
            ref table,
            ref key,
            ref fields,
        } => match block_on_io(e.hash_get_many(db, table, key, fields)) {
            Ok(vs) => crate::request::BatchResult::Values(vs),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::HDel {
            ref db,
            ref table,
            ref key,
            ref fields,
        } => match block_on_io(e.hash_del(db, table, key, fields)) {
            Ok(n) => crate::request::BatchResult::Integer(n),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::HLen {
            ref db,
            ref table,
            ref key,
        } => match block_on_io(e.hash_len(db, table, key)) {
            Ok(n) => crate::request::BatchResult::Integer(n),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::HGetAll {
            ref db,
            ref table,
            ref key,
        } => match block_on_io(e.hash_get_all(db, table, key)) {
            Ok(ps) => crate::request::BatchResult::Pairs(ps),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::HIncrBy {
            ref db,
            ref table,
            ref key,
            ref field,
            delta,
        } => exec_hincrby(e, db, table, key, field, delta),
        crate::request::BatchOp::HIncrByFloat {
            ref db,
            ref table,
            ref key,
            ref field,
            delta,
        } => exec_hincrbyfloat(e, db, table, key, field, delta),
        // ⭐ Phase Set: Set ops
        crate::request::BatchOp::SAdd {
            ref db,
            ref table,
            ref key,
            ref members,
        } => match block_on_io(e.set_add(db, table, key, members)) {
            Ok(n) => crate::request::BatchResult::Integer(n),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::SRem {
            ref db,
            ref table,
            ref key,
            ref members,
        } => match block_on_io(e.set_rem(db, table, key, members)) {
            Ok(n) => crate::request::BatchResult::Integer(n),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::SIsMember {
            ref db,
            ref table,
            ref key,
            ref member,
        } => match block_on_io(e.set_is_member(db, table, key, member)) {
            Ok(b) => crate::request::BatchResult::Integer(i64::from(b)),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::SCard {
            ref db,
            ref table,
            ref key,
        } => match block_on_io(e.set_card(db, table, key)) {
            Ok(n) => crate::request::BatchResult::Integer(n),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::SMembers {
            ref db,
            ref table,
            ref key,
        } => match block_on_io(e.set_members(db, table, key)) {
            Ok(ms) => crate::request::BatchResult::Members(ms),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::SPop {
            ref db,
            ref table,
            ref key,
        } => exec_spop(e, db, table, key),
        crate::request::BatchOp::SRandMember {
            ref db,
            ref table,
            ref key,
        } => match block_on_io(e.set_pick_one(db, table, key)) {
            Ok(m) => crate::request::BatchResult::Members(m.into_iter().collect()),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        // ⭐ Phase L: List ops
        crate::request::BatchOp::LPush {
            ref db,
            ref table,
            ref key,
            ref values,
            left,
        } => match block_on_io(e.list_push(db, table, key, values, left)) {
            Ok(n) => crate::request::BatchResult::Integer(n),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::LPop {
            ref db,
            ref table,
            ref key,
            left,
            count,
        } => exec_lpop(e, db, table, key, left, count as usize),
        crate::request::BatchOp::LLen {
            ref db,
            ref table,
            ref key,
        } => match block_on_io(e.list_len(db, table, key)) {
            Ok(n) => crate::request::BatchResult::Integer(n),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::LRange {
            ref db,
            ref table,
            ref key,
            start,
            end,
        } => exec_lrange(e, db, table, key, start, end),
        crate::request::BatchOp::LIndex {
            ref db,
            ref table,
            ref key,
            idx,
        } => match block_on_io(e.list_index(db, table, key, idx)) {
            Ok(v) => crate::request::BatchResult::GetValue(v),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::LSet {
            ref db,
            ref table,
            ref key,
            idx,
            ref val,
        } => exec_lset(e, db, table, key, idx, val),
        // ⭐ Phase Z: ZSet ops
        crate::request::BatchOp::ZAdd {
            ref db,
            ref table,
            ref key,
            ref pairs,
        } => match block_on_io(e.zset_add(db, table, key, pairs)) {
            Ok(n) => crate::request::BatchResult::Integer(n),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::ZRem {
            ref db,
            ref table,
            ref key,
            ref members,
        } => match block_on_io(e.zset_rem(db, table, key, members)) {
            Ok(n) => crate::request::BatchResult::Integer(n),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::ZScore {
            ref db,
            ref table,
            ref key,
            ref member,
        } => match block_on_io(e.zset_score(db, table, key, member)) {
            Ok(s) => crate::request::BatchResult::OptMember(s.map(fmt_score)),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::ZCard {
            ref db,
            ref table,
            ref key,
        } => match block_on_io(e.zset_card(db, table, key)) {
            Ok(n) => crate::request::BatchResult::Integer(n),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::ZIncrBy {
            ref db,
            ref table,
            ref key,
            delta,
            ref member,
        } => match block_on_io(e.zset_incr(db, table, key, delta, member)) {
            Ok(s) => crate::request::BatchResult::Double(s),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::ZRange {
            ref db,
            ref table,
            ref key,
            start,
            end,
            rev,
            withscores,
        } => match block_on_io(e.zset_range(db, table, key, start, end, rev)) {
            Ok(rows) => crate::request::BatchResult::Members(zrows_to_members(rows, withscores)),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::ZRangeByScore {
            ref db,
            ref table,
            ref key,
            min,
            max,
            withscores,
        } => match block_on_io(e.zset_range_by_score(db, table, key, min, max)) {
            Ok(rows) => crate::request::BatchResult::Members(zrows_to_members(rows, withscores)),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::ZRank {
            ref db,
            ref table,
            ref key,
            ref member,
            rev,
        } => match block_on_io(e.zset_rank(db, table, key, member, rev)) {
            Ok(Some(r)) => crate::request::BatchResult::Integer(r),
            Ok(None) => crate::request::BatchResult::OptMember(None),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::ZCount {
            ref db,
            ref table,
            ref key,
            min,
            max,
        } => match block_on_io(e.zset_range_by_score(db, table, key, min, max)) {
            Ok(rows) => crate::request::BatchResult::Integer(rows.len() as i64),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::ZMScore {
            ref db,
            ref table,
            ref key,
            ref members,
        } => match block_on_io(e.zset_mscore(db, table, key, members)) {
            Ok(scores) => crate::request::BatchResult::Values(
                scores.into_iter().map(|s| s.map(fmt_score)).collect(),
            ),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::ZPop {
            ref db,
            ref table,
            ref key,
            rev,
            count,
        } => match block_on_io(e.zset_pop(db, table, key, rev, count as usize)) {
            Ok(rows) => crate::request::BatchResult::Members(zrows_to_members(rows, true)),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::SMisMember {
            ref db,
            ref table,
            ref key,
            ref members,
        } => match block_on_io(e.set_mismember(db, table, key, members)) {
            Ok(bs) => crate::request::BatchResult::IntList(bs.into_iter().map(i64::from).collect()),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::SPopN {
            ref db,
            ref table,
            ref key,
            count,
        } => match block_on_io(e.set_pop_n(db, table, key, count as usize)) {
            Ok(ms) => crate::request::BatchResult::Members(ms),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::SRandCount {
            ref db,
            ref table,
            ref key,
            count,
        } => match block_on_io(e.set_rand_n(db, table, key, count as usize)) {
            Ok(ms) => crate::request::BatchResult::Members(ms),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::HRandField {
            ref db,
            ref table,
            ref key,
            count,
            ..
        } => match block_on_io(e.hash_rand(db, table, key, count as usize)) {
            Ok(ps) => crate::request::BatchResult::Pairs(ps),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::LRem {
            ref db,
            ref table,
            ref key,
            count,
            ref val,
        } => match block_on_io(e.list_rem(db, table, key, count, val)) {
            Ok(n) => crate::request::BatchResult::Integer(n),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::LTrim {
            ref db,
            ref table,
            ref key,
            start,
            stop,
        } => match block_on_io(e.list_trim(db, table, key, start, stop)) {
            Ok(()) => crate::request::BatchResult::Integer(1),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::LPos {
            ref db,
            ref table,
            ref key,
            ref val,
            rank,
            count,
        } => exec_lpos(e, db, table, key, val, rank, count),
        crate::request::BatchOp::LInsert {
            ref db,
            ref table,
            ref key,
            before,
            ref pivot,
            ref val,
        } => match block_on_io(e.list_insert(db, table, key, before, pivot, val)) {
            Ok(n) => crate::request::BatchResult::Integer(n),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::SetBit {
            ref db,
            ref table,
            ref key,
            offset,
            bit,
        } => exec_setbit(e, db, table, key, offset, bit),
        // ---- ⭐ Q5: SQL row 表 ----
        crate::request::BatchOp::RowPut {
            ref db,
            ref table,
            ref pk,
            ref values,
        } => exec_row_put(e, db, table, pk, values),
        crate::request::BatchOp::RowGet {
            ref db,
            ref table,
            ref pk,
        } => match block_on_io(e.row_get(db, table, pk)) {
            Ok(v) => crate::request::BatchResult::GetValue(v),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::RowDelete {
            ref db,
            ref table,
            ref pk,
        } => match block_on_io(e.row_delete(db, table, pk)) {
            Ok(existed) => crate::request::BatchResult::DeleteExisted(existed),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::RowUpdate {
            ref db,
            ref table,
            ref pk,
            ref sets,
        } => match block_on_io(e.row_update(db, table, pk, sets)) {
            Ok(updated) => crate::request::BatchResult::DeleteExisted(updated),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::RowUnset {
            ref db,
            ref table,
            ref pk,
            ref cols,
        } => match block_on_io(e.row_unset(db, table, pk, cols)) {
            Ok(changed) => crate::request::BatchResult::Integer(changed),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::RowSetNx {
            ref db,
            ref table,
            ref pk,
            col,
            ref val,
        } => match block_on_io(e.row_set_nx(db, table, pk, col, val.clone())) {
            Ok(set) => crate::request::BatchResult::Integer(i64::from(set)),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::RowPatchUpsert {
            ref db,
            ref table,
            ref pk,
            ref sets,
            ref insert_values,
        } => match block_on_io(e.row_patch_upsert(db, table, pk, sets, insert_values)) {
            Ok(added) => crate::request::BatchResult::Integer(added),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::RowIncr {
            ref db,
            ref table,
            ref pk,
            col,
            delta,
        } => match block_on_io(e.row_incr(db, table, pk, col, delta)) {
            Ok(storage::row::ColValue::I64(v)) => crate::request::BatchResult::Integer(v),
            Ok(storage::row::ColValue::F64(v)) => crate::request::BatchResult::Double(v),
            Ok(_) => crate::request::BatchResult::Error(
                "row increment returned non-numeric value".into(),
            ),
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::DropTableOp { ref db, ref table } => {
            match block_on_io(e.drop_table_sql(db, table)) {
                Ok(_) => crate::request::BatchResult::PutOk,
                Err(err) => crate::request::BatchResult::Error(err.to_string()),
            }
        }
        crate::request::BatchOp::TableScan {
            ref db,
            ref table,
            limit,
        } => exec_table_scan(e, db, table, limit),
        crate::request::BatchOp::ScanFiltered {
            ref db,
            ref table,
            ref preds,
            ref proj,
            ref index_hint,
            ref key_set_hint,
            limit,
        } => exec_scan_filtered(
            e,
            db,
            table,
            preds,
            proj,
            index_hint.as_ref(),
            key_set_hint.as_ref(),
            limit,
        ),
        crate::request::BatchOp::ScanFilteredRows {
            ref db,
            ref table,
            ref index_hint,
            limit,
        } => exec_scan_filtered_rows(e, db, table, index_hint.as_ref(), limit),
        crate::request::BatchOp::IndexScan {
            ref db,
            ref table,
            iid,
            ref lo,
            ref hi,
            limit,
            with_rows,
        } => exec_index_scan(
            e,
            db,
            table,
            iid,
            lo.as_ref(),
            hi.as_ref(),
            limit,
            with_rows,
        ),
        crate::request::BatchOp::SetSchemaOp {
            ref db,
            ref table,
            ref bytes,
        } => exec_set_schema(e, db, table, bytes),
        crate::request::BatchOp::GetSchemaOp { ref db, ref table } => exec_get_schema(e, db, table),
        // ⭐ 事务 v1 (F61): COMMIT 原子批 — 先验后写 + 逐 op 应用.
        // shard 单线程 = 批内零并发穿插; 预检失败整批拒绝 (零部分应用);
        // wal_barrier 由 caller (ShardTask 臂) 在回复前统一执行.
        crate::request::BatchOp::TxnApply { ops, read_set } => exec_txn_apply(e, ops, read_set),
        // ⭐ F65: 全局 UNIQUE 占坑原语 (email-shard 单线程原子)
        crate::request::BatchOp::ReserveUnique {
            db,
            table,
            iid,
            enc_val,
            pk,
            txn_id,
        } => match block_on_io(e.unique_reserve(&db, &table, iid, &enc_val, &pk, txn_id)) {
            Ok(None) => crate::request::BatchResult::ReserveOk,
            Ok(Some((state, holder_txn, holder_pk))) => {
                crate::request::BatchResult::ReserveConflict {
                    state,
                    holder_txn,
                    holder_pk,
                }
            }
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::StealUnique {
            db,
            table,
            iid,
            enc_val,
            pk,
            txn_id,
        } => match block_on_io(e.unique_steal(&db, &table, iid, &enc_val, &pk, txn_id)) {
            Ok(()) => crate::request::BatchResult::ReserveOk,
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::ConfirmUnique {
            db,
            table,
            iid,
            enc_val,
            pk,
            txn_id,
        } => match block_on_io(e.unique_confirm(&db, &table, iid, &enc_val, &pk, txn_id)) {
            Ok(()) => crate::request::BatchResult::PutOk,
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        crate::request::BatchOp::ReleaseUnique {
            db,
            table,
            iid,
            enc_val,
            txn_id,
        } => match block_on_io(e.unique_release(&db, &table, iid, &enc_val, txn_id)) {
            Ok(()) => crate::request::BatchResult::PutOk,
            Err(err) => crate::request::BatchResult::Error(err.to_string()),
        },
        // ⭐ F66: catalog 快照 — 列当前 db 全表 + schema (任意单 shard).
        crate::request::BatchOp::CatalogDump { db } => {
            let tables = match e.list_tables(&db) {
                Ok(t) => t,
                Err(err) => return crate::request::BatchResult::Error(err.to_string()),
            };
            let mut out = Vec::with_capacity(tables.len());
            for t in tables {
                match block_on_io(e.get_schema(&db, &t)) {
                    Ok(Some(sc)) => out.push((t, sc.encode())),
                    Ok(None) => {} // 无 schema 的纯 KV 表不入 catalog
                    Err(err) => return crate::request::BatchResult::Error(err.to_string()),
                }
            }
            crate::request::BatchResult::Catalog(out)
        }
    }
}

/// ⭐ S2: 全表扫 (广播 op; `[S]` 前缀收 TAG_ROW 行).
pub(crate) fn exec_table_scan(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    limit: u32,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    let mut out = Vec::new();
    match block_on_io(e.table_scan_rows_local(db, table, limit as usize, &mut out)) {
        Ok(()) => BatchResult::Rows(out),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ F67 (JOIN): 带谓词+投影下推的全表扫 → ProjRows.
#[allow(clippy::too_many_arguments)]
pub(crate) fn exec_scan_filtered(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    preds: &[crate::request::ScanPred],
    proj: &[u16],
    index_hint: Option<&storage::sql_rows::IndexHint>,
    key_set_hint: Option<&storage::sql_rows::KeySetHint>,
    limit: u32,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    let mut out = Vec::new();
    match block_on_io(e.table_scan_filtered_local(
        db,
        table,
        preds,
        proj,
        index_hint,
        key_set_hint,
        limit as usize,
        &mut out,
    )) {
        Ok(()) => BatchResult::ProjRows(out),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ 修复 (2026-08): DML phase1 范围扫 → 返回完整 Rows (含 pk/row_bytes).
#[allow(clippy::too_many_arguments)]
pub(crate) fn exec_scan_filtered_rows(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    index_hint: Option<&storage::sql_rows::IndexHint>,
    limit: u32,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    let mut out: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::new();
    match block_on_io(e.table_scan_filtered_rows_local(
        db,
        table,
        index_hint,
        limit as usize,
        &mut out,
    )) {
        Ok(()) => BatchResult::Rows(out),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ SETRANGE: 从 offset 覆盖写 data (零扩展), 结果归一为 TAG_RAW,
/// 返回新长度. data 空 → 不写, 返回当前长度 (Redis 语义).
pub(crate) fn exec_setrange(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    offset: u32,
    data: &[u8],
) -> crate::request::BatchResult {
    use crate::request::{BatchResult, VALUE_TAG_RAW};
    use crate::value_num;
    let old = match block_on_io(e.table_get(db, table, key)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    let mut base: Vec<u8> = match &old {
        Some(s) => value_num::render(s).into_owned(),
        None => Vec::new(),
    };
    if data.is_empty() {
        return BatchResult::Integer(base.len() as i64);
    }
    let offset = offset as usize;
    let end = offset + data.len();
    if base.len() < end {
        base.resize(end, 0u8); // 零扩展
    }
    base[offset..end].copy_from_slice(data);
    let new_len = base.len() as i64;
    let mut stored = Vec::with_capacity(1 + base.len());
    stored.push(VALUE_TAG_RAW);
    stored.extend_from_slice(&base);
    match block_on_io(e.table_put(db, table, key, &stored)) {
        Ok(_) => BatchResult::Integer(new_len),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ MSETNX 分片: 本组全部 key 不存在才写. 返回 1=写入, 0=有 key 已存在.
/// (跨 shard 非原子: 别组可能已写 — 已记为 gap.)
/// 使用 table_exists (零 value 物化) 代替 table_get.
pub(crate) fn exec_multiputnx(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    pairs: &[(Vec<u8>, Vec<u8>)],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    for (k, _) in pairs {
        match block_on_io(e.table_exists(db, table, k)) {
            Ok(true) => return BatchResult::Integer(0),
            Ok(false) => {}
            Err(err) => return BatchResult::Error(err.to_string()),
        }
    }
    match block_on_io(e.table_put_many(db, table, pairs)) {
        Ok(_) => BatchResult::Integer(1),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Phase H: HINCRBY — field 整数 RMW, 结果写回 TAG_I64.
pub(crate) fn exec_hincrby(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    field: &[u8],
    delta: i64,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    use crate::value_num;
    let old = match block_on_io(e.hash_get(db, table, key, field)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    let cur: i64 = match &old {
        None => 0,
        Some(stored) => match value_num::parse_num_int_only(stored) {
            Some(n) => n,
            None => return BatchResult::Error("hash value is not an integer".into()),
        },
    };
    let Some(new) = cur.checked_add(delta) else {
        return BatchResult::Error("increment or decrement would overflow".into());
    };
    let pair = vec![(field.to_vec(), value_num::encode_i64(new))];
    match block_on_io(e.hash_set(db, table, key, &pair)) {
        Ok(_) => BatchResult::Integer(new),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Phase H: HINCRBYFLOAT — field 浮点 RMW, 结果写回 TAG_F64.
pub(crate) fn exec_hincrbyfloat(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    field: &[u8],
    delta: f64,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    use crate::value_num::{self, NumValue};
    let old = match block_on_io(e.hash_get(db, table, key, field)) {
        Ok(v) => v,
        Err(err) => return BatchResult::Error(err.to_string()),
    };
    let cur: f64 = match &old {
        None => 0.0,
        Some(stored) => match value_num::parse_num(stored) {
            Some(NumValue::I64(n)) => n as f64,
            Some(NumValue::F32(f)) => f as f64,
            Some(NumValue::F64(f)) => f,
            None => return BatchResult::Error("hash value is not a float".into()),
        },
    };
    let new = cur + delta;
    if !new.is_finite() {
        return BatchResult::Error("increment would produce NaN or Infinity".into());
    }
    let pair = vec![(field.to_vec(), value_num::encode_f64(new))];
    match block_on_io(e.hash_set(db, table, key, &pair)) {
        Ok(_) => BatchResult::Double(new),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Phase Set: SPOP — 取任意成员 + 删除 (shard 内原子).
pub(crate) fn exec_spop(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match block_on_io(e.set_pick_one(db, table, key)) {
        Ok(Some(m)) => match block_on_io(e.set_rem(db, table, key, std::slice::from_ref(&m))) {
            Ok(_) => BatchResult::Members(vec![m]),
            Err(err) => BatchResult::Error(err.to_string()),
        },
        Ok(None) => BatchResult::Members(vec![]),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ C2: LPOS — count 缺省回首位 (Integer / nil), 否则 IntList (count=0 全部).
pub(crate) fn exec_lpos(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    val: &[u8],
    rank: i64,
    count: Option<u32>,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match count {
        None => match block_on_io(e.list_pos(db, table, key, val, rank, 1)) {
            Ok(ps) => match ps.first() {
                Some(&p) => BatchResult::Integer(p),
                None => BatchResult::OptMember(None),
            },
            Err(err) => BatchResult::Error(err.to_string()),
        },
        Some(c) => match block_on_io(e.list_pos(db, table, key, val, rank, c as usize)) {
            Ok(ps) => BatchResult::IntList(ps),
            Err(err) => BatchResult::Error(err.to_string()),
        },
    }
}

/// ⭐ Phase L: LPOP/RPOP — 弹出后 render 剥 tag (与 Set 统一为就绪字节).
pub(crate) fn exec_lpop(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    left: bool,
    count: usize,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    use crate::value_num;
    match block_on_io(e.list_pop(db, table, key, left, count)) {
        Ok(vs) => BatchResult::Members(
            vs.iter()
                .map(|v| value_num::render(v).into_owned())
                .collect(),
        ),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Phase L: LRANGE — 区间元素 render 剥 tag.
pub(crate) fn exec_lrange(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    start: i64,
    end: i64,
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    use crate::value_num;
    match block_on_io(e.list_range(db, table, key, start, end)) {
        Ok(vs) => BatchResult::Members(
            vs.iter()
                .map(|v| value_num::render(v).into_owned())
                .collect(),
        ),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Phase L: LSET — 越界回 Redis 错误, 否则 Integer(1) (worker 转 +OK).
pub(crate) fn exec_lset(
    e: &mut storage::StorageEngine,
    db: &str,
    table: &str,
    key: &[u8],
    idx: i64,
    val: &[u8],
) -> crate::request::BatchResult {
    use crate::request::BatchResult;
    match block_on_io(e.list_set(db, table, key, idx, val)) {
        Ok(true) => BatchResult::Integer(1),
        Ok(false) => BatchResult::Error("index out of range".into()),
        Err(err) => BatchResult::Error(err.to_string()),
    }
}

/// ⭐ Phase Z: score 渲染为 Redis 风格字符串 (3.0→"3", 3.5→"3.5").
pub(crate) fn fmt_score(s: f64) -> Vec<u8> {
    format!("{s}").into_bytes()
}

/// ⭐ Phase Z: (member, score) 列表 → Members (withscores 时 member/score 交替).
pub(crate) fn zrows_to_members(rows: Vec<(Vec<u8>, f64)>, withscores: bool) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(if withscores {
        rows.len() * 2
    } else {
        rows.len()
    });
    for (m, sc) in rows {
        out.push(m);
        if withscores {
            out.push(fmt_score(sc));
        }
    }
    out
}
