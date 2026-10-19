//! ⭐ 审计 P3-3: 协议分派公共层 — epoll worker 与协程 worker 共用.
//!
//! `match conn.proto` 的**输入分派**与**回复分派**曾在 `worker_epoll.rs` 与
//! `worker_coro.rs` 各写一份: 新增协议/改 `process_*_input` 参数时改一处漏
//! 一处, 两条事件循环行为静默漂移 (审计 P3-3). 现收敛到本模块单份实现.
//!
//! 两处调用方的差异 (epoll 克隆 per-conn 配置避免 `&conn`/`&mut conn` 双重
//! 借用; 协程持有配置) 由各自构造 `ConnDispatch` 消化, 分派逻辑不再有第二份.
//!
//! 关闭语义统一为"处理后一律 `resp_should_close()`": Binary 旧 epoll 路径
//! 不查该标志, 但 Binary 仅在 TLS 写 30s 卡死/空闲回收时才置
//! `close_after_flush`, 此时关闭正是预期结果 (且 `resp_should_close` 要求
//! 回复全部排空, 不会丢回包) — 与协程路径原行为一致.

use std::sync::Arc;

use shard_manager::{BatchResult, SharedTaskInbox};

use super::resp_shard::handle_resp_shard_result;
use super::{
    ConnState, KvLimits, ProtocolKind, batch_result_to_response, process_binary_input,
    process_http_input, process_pg_input, process_resp_input, process_sql_input,
};

/// 协议分派上下文: 两个 worker 实现按各自生命周期提供同一组资源.
pub(crate) struct ConnDispatch<'a> {
    pub conn_id: u64,
    pub worker_id: u32,
    pub db: &'a Arc<str>,
    pub table: &'a Arc<str>,
    pub limits: &'a KvLimits,
    /// SQL/PG 门面的 http_token/auth_password (MySQL native / PG cleartext).
    pub auth_pw: &'a Option<String>,
    pub tls: &'a Option<Arc<rustls::ServerConfig>>,
    pub db_view: &'a Arc<shard_manager::DbDirView>,
    pub shard_inboxes: &'a [SharedTaskInbox],
    pub num_shards: usize,
}

/// 输入分派: 按 `conn.proto` 把已读入 `conn.read_buf` 的字节交给对应
/// 协议处理器. 返回 true 表示处理完后连接应关闭.
pub(crate) fn dispatch_conn_input(conn: &mut ConnState, d: &ConnDispatch<'_>) -> bool {
    match conn.proto {
        ProtocolKind::Binary => process_binary_input(
            conn,
            d.conn_id,
            d.worker_id,
            d.db,
            d.table,
            d.limits,
            d.shard_inboxes,
            d.num_shards,
        ),
        ProtocolKind::Resp => process_resp_input(
            conn,
            d.conn_id,
            d.worker_id,
            d.db_view,
            d.table,
            d.limits,
            d.auth_pw,
            d.shard_inboxes,
            d.num_shards,
        ),
        ProtocolKind::Sql => process_sql_input(
            conn,
            d.conn_id,
            d.worker_id,
            d.auth_pw,
            d.db,
            d.db_view,
            d.shard_inboxes,
            d.num_shards,
            d.tls,
            d.limits,
        ),
        ProtocolKind::Pg => process_pg_input(
            conn,
            d.conn_id,
            d.worker_id,
            d.auth_pw,
            d.db,
            d.db_view,
            d.shard_inboxes,
            d.num_shards,
            d.tls,
            d.limits,
        ),
        ProtocolKind::Http => process_http_input(
            conn,
            d.conn_id,
            d.worker_id,
            d.auth_pw,
            d.db,
            d.db_view,
            d.limits,
            d.num_shards,
            d.shard_inboxes,
            d.num_shards,
        ),
    }
    conn.resp_should_close()
}

/// 回复分派: shard 回包按协议渲染. Binary 直发 (帧内 req_id, 乱序语义);
/// 其余走 `handle_resp_shard_result` (seq 重排 + 聚合). 返回 true 表示应关闭.
#[allow(clippy::too_many_arguments)]
pub(crate) fn dispatch_conn_reply(
    conn: &mut ConnState,
    conn_id: u64,
    req_id: u64,
    group: u32,
    result: &BatchResult,
    worker_id: u32,
    default_db: &Arc<str>,
    db_view: &Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) -> bool {
    if conn.proto == ProtocolKind::Binary {
        // Binary 门面: 直发 batch 结果 (不聚合, 每 op 一回包).
        let resp = batch_result_to_response(result);
        conn.send_binary_response(req_id, &resp);
    } else {
        // SQL/HTTP/RESP/PG conn 复用 seq 重排 + 聚合钩子.
        handle_resp_shard_result(
            conn,
            conn_id,
            req_id,
            group,
            result,
            worker_id,
            default_db,
            db_view,
            shard_inboxes,
            num_shards,
        );
    }
    conn.resp_should_close()
}
