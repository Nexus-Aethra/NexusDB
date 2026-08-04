//! ⭐ 解耦 2026-08: RESP 命令分发 + 跨 shard 回包处理 (拆自 mod.rs).
//! dispatch_resp_command (RESP 命令 → 本地/KV/SQL 分派) +
//! handle_resp_shard_result (shard 回包 → 聚合/渲染/级联).

use super::*;

/// 分发单条 RESP 命令: 本地命令直接回 (占 seq 进重排缓冲), KV 命令进 shard.
#[allow(clippy::too_many_arguments)]
pub(crate) fn push_task(
    conn: &mut ConnState,
    conn_id: u64,
    req_id: u64,
    worker_id: u32,
    mut op: BatchOp,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    // ⭐ T2 (分表): 单 key op 统一在此按 "table:key" 冒号前缀选表 (单点重写;
    // 无前缀保持构造时的 default 表). Multi op 由 dispatch 预分组时已解析.
    if let Some((tbl, key)) = op.table_key_mut()
        && let Some(pos) = split_table_key(key)
    {
        let prefix = key[..pos].to_vec();
        *tbl = conn.table_arc(&prefix);
        key.drain(..=pos);
    }
    let shard_id = hash_route_op(&op, num_shards);
    shard_inboxes[shard_id].push_spin(ShardTask {
        conn_id,
        req_id,
        worker_id,
        group: 0,
        op,
    });
}

/// ⭐ MGET/MSET: 定向 push 到指定 shard, 带组号 (聚合回填用).
pub(crate) fn push_task_grouped(
    conn_id: u64,
    req_id: u64,
    worker_id: u32,
    group: u32,
    shard_id: usize,
    op: BatchOp,
    shard_inboxes: &[SharedTaskInbox],
) {
    shard_inboxes[shard_id].push_spin(ShardTask {
        conn_id,
        req_id,
        worker_id,
        group,
        op,
    });
}

/// key 级路由 (与 hash_route_op 同 hash 逻辑, 分组场景用).
pub(crate) fn hash_route_key(db: &str, table: &str, key: &[u8], num_shards: usize) -> usize {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    db.hash(&mut h);
    table.hash(&mut h);
    key.hash(&mut h);
    (h.finish() as usize) % num_shards
}

/// ⭐ GETRANGE 切片 (Redis 语义): 负索引从尾算, end inclusive, 越界 clamp.
pub(crate) fn getrange_slice(data: &[u8], start: i64, end: i64) -> &[u8] {
    let len = data.len() as i64;
    if len == 0 {
        return &[];
    }
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if end < 0 { len + end } else { end };
    if s < 0 {
        s = 0;
    }
    if e < 0 {
        e = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e {
        return &[];
    }
    &data[s as usize..=e as usize]
}
