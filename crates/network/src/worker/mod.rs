//! Worker 线程池: epoll 事件循环驱动, 双协议门面 (Binary / RESP2).
//!
//! **架构**:
//! - 每个 worker 1 个线程, 1 个 epoll 事件循环
//! - epoll 监听: 所有 conn 的 readable + reply_bus eventfd
//! - conn readable: recv → parse → 校验 (KvLimits/AUTH) → route → push task_inbox[shard_id]
//! - reply eventfd: drain reply_bus → 按 conn_id 找连接 → encode → send
//!
//! **协议差异**:
//! - Binary: 帧内带 req_id, 回复乱序直发
//! - RESP: 无 req_id, per-conn 分配递增 seq 作为 req_id, 回复经重排缓冲严格 FIFO;
//!   本地命令 (PING/AUTH/超限 error) 也占 seq 进同一缓冲, 保证 pipeline 顺序
//!
//! **value 类型标签**: Put 时统一 `encode_value(TAG_RAW, ..)`, Get 回复时剥 tag.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::unix::io::{FromRawFd, RawFd};
use std::thread;

use crossbeam_channel::Receiver;
use shard_manager::{BatchOp, BatchResult, SharedTaskInbox, SharedTaskReplyBus, ShardTask};

use crate::acceptor::NewConn;
use crate::protocol::sql::{
    self, CmpOp, Cond, JoinCond, JoinItem, JoinKind, Pred, QualCol, SqlStmt, SqlValue,
};
use crate::protocol::{
    BinaryProtocol, DecodeOutcome, KvLimits, Protocol, Request, RespCodec, RespCommand, Response,
    SetAlgOp, validate_kv, validate_request,
};
use crate::value_codec::{decode_value, render};
use storage::row::ColValue;
use storage::schema::{ColType, TableSchema};

/// ⭐ 拆分 (2026-08): SQL 纯工具函数 (不依赖 ConnState 状态).
mod sql_util;
use sql_util::*;
/// ⭐ 拆分 (2026-08): SQL 聚合执行 (GROUP BY/聚合函数/HAVING).
mod sql_agg;
/// ⭐ PG 兼容 (FMT_VER 8): 外键级联删除编排.
mod sql_cascade;
pub(crate) use sql_cascade::{cascade_job_done, cascade_kickoff, is_cascade_seq, CascadeJob};
/// ⭐ PG 兼容 (引用完整性, FMT_VER 8): 外键 INSERT 存在性预检.
mod sql_fk;
pub(crate) use sql_fk::{all_parents_cached, sql_fk_on_reply, sql_fk_start};
/// ⭐ ORM-B2 (解耦 2026-08): 进程级共享路由缓存 (SqlSharedRoutes/FkIncoming).
mod sql_routes;
pub use sql_routes::{new_sql_shared, FkIncoming, SqlSharedRoutes};
/// ⭐ 解耦 2026-08: RESP 跨 shard 聚合状态结构体.
mod resp_agg;
pub(crate) use resp_agg::{
    BitCtx, DelAgg, ExistsAgg, GeoCtx, GetKind, MembersKind, MGetAgg, MSetAgg, MSetNxAgg,
    PairsKind, ScoredMembers, SetAlgAgg, StoreFinishAgg, ZStoreAgg,
};
/// ⭐ 解耦 2026-08: SQL 规划/执行状态结构体 (事务/聚合/JOIN/规划/schema 缓存).
mod sql_state;
/// ⭐ 解耦 2026-08: RESP 命令分发 + 跨 shard 回包处理.
mod resp_dispatch;
pub(crate) use resp_dispatch::{getrange_slice, hash_route_key, push_task, push_task_grouped};
/// ⭐ 解耦 2026-08: RESP 命令分发主函数 (拆自 resp_dispatch).
mod resp_dispatch_cmd;
pub(crate) use resp_dispatch_cmd::dispatch_resp_command;
/// ⭐ 解耦 2026-08: shard 回包处理主函数 (拆自 resp_dispatch).
mod resp_shard;
pub(crate) use resp_shard::handle_resp_shard_result;
/// ⭐ 解耦 2026-08: 协议 wire 入口 (HTTP 处理).
mod protocol_io;
pub(crate) use protocol_io::{
    handle_http_request, mysql_err_packet, process_http_input, process_pg_input,
    process_sql_input,
};
pub(crate) use sql_state::{
    DerivedCtx, MatResult, MultiStmt, PendingSql, SharedSqlCache, SqlDmlAgg, SqlFkIns,
    SqlPlan, SqlRowCtx, SqlTxnAgg, SqlUniqueIns, SqlWorkerCache, SubqCtx, TxnState,
    UniquePhase, EST_SKIP_STATS_ROWS, SUBQ_IN_MAX, TXN_MAX_BYTES, TXN_MAX_OPS,
};
/// ⭐ 拆分 (2026-08): SQL 语句分派/规划/执行核心.
mod sql_dispatch;
/// ⭐ 解耦 2026-08: DML 执行 (从 sql_dispatch 拆出).
mod sql_dml;
/// ⭐ 解耦 2026-08: JOIN 编排 (从 sql_dispatch 拆出).
mod sql_join;
/// ⭐ 解耦 2026-08: 全局 UNIQUE 约束 INSERT 编排 (从 sql_dispatch 拆出).
mod sql_unique;
/// ⭐ 解耦 2026-08: 系统表查询虚拟表合成 (从 sql_dispatch 拆出).
mod sql_sysquery;
/// ⭐ 拆分 (2026-08): SQL 值评估/比较/行构建/协议字节.
mod sql_eval;
pub(crate) use sql_agg::{
    bind_scalar_expr, cmp_colvalue, eval_bound_expr, eval_json_exists, materialize_select_agg,
    render_select_agg, sql_run_agg_select, AggSpec, BoundExpr,
};
pub(crate) use sql_dispatch::{sql_dispatch_stmt, sql_plan_select};
pub(crate) use sql_dml::sql_run_dml;
pub(crate) use sql_join::{sql_join_drive, sql_join_kickoff};
pub(crate) use sql_sysquery::{
    coltype_sql_name, sbytes, sysq_exists, sysq_finish, sysq_render_catalog, sysq_render_dblist,
    SysQuerySpec,
};
pub(crate) use sql_unique::{
    push_unique_op, row_global_unique_encs, schema_global_unique, sql_unique_drive,
    sql_unique_ins_start,
};
pub(crate) use sql_eval::{
    col_from_ordered_bytes, collect_dml_pks, eval_pred, eval_pred_sysq, is_auto_pk,
    project_output_row, render_sql_count, render_sql_rows, scalar_fn_const_row, sql_build_row,
    sql_cmp, sql_dml_op, sql_err_bytes, sql_ok_bytes, sql_order_cmp, sql_pk_bytes,
    sql_rows_bytes, sql_to_col, visible_cols, HIDDEN_ROWID,
};
/// ⭐ 拆分 (2026-08): SQL 值编码/解码工具 (日期/时间/UUID/Decimal).
mod sql_encode;
use sql_encode::*;
/// 跨模块 (protocol/mysql.rs, protocol/pg.rs) 继续通过 `crate::worker::xxx` 引用.
pub(crate) use sql_encode::{
    datetime_parts, render_date, render_decimal, render_time, render_timestamp, render_uuid,
    time_parts,
};

/// ⭐ 解耦 2026-08: SQL derived table (子查询) 物化与渲染 (拆自 mod.rs).
mod worker_derived;
pub(crate) use worker_derived::{
    derived_capture_rowctx, derived_render, finish_derived, finish_derived_join, sql_subq_advance,
    sql_subq_start,
};
/// ⭐ 解耦 2026-08: ConnState 核心方法 (拆自 mod.rs). impl 方法无需 re-export.
mod worker_conn;

/// 特殊 epoll token: reply bus eventfd.
const REPLY_TOKEN: u64 = u64::MAX;
/// 特殊 epoll token: new conn inbox eventfd (如果有).
const NEW_CONN_TOKEN: u64 = u64::MAX - 1;

/// 连接使用的协议门面.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolKind {
    Binary,
    Resp,
    /// ⭐ Z2: MySQL wire SQL 门面 (5434).
    Sql,
    /// ⭐ S4: PostgreSQL wire SQL 门面 (5435) — 与 Sql 共内核, 仅 framing 分流.
    Pg,
    /// ⭐ H1: HTTP/1.1 REST 门面 (6778) — KV/SQL JSON + CORS + 可观测性.
    Http,
}

pub struct WorkerConfig {
    pub worker_id: u32,
    pub inbox: Receiver<NewConn>,
    /// 新连接通知 eventfd (acceptor send 后写它, worker epoll 精确唤醒).
    pub conn_eventfd: RawFd,
    pub shard_inboxes: Vec<SharedTaskInbox>,
    pub reply_bus: SharedTaskReplyBus,
    pub default_db: String,
    pub default_table: String,
    /// 本 worker 所有连接使用的协议.
    pub protocol: ProtocolKind,
    /// KV 长度限制 (超限不进 shard, 直接回协议 error).
    pub limits: KvLimits,
    /// RESP AUTH 密码 (None = 不启用认证).
    pub auth_password: Option<String>,
    /// ⭐ D3 (分库): SELECT n → db name 翻译视图 (ShardManager resolver 镜像).
    pub db_view: std::sync::Arc<shard_manager::DbDirView>,
    /// ⭐ ORM-B2: 进程级共享路由缓存 (同数据集群的全部 SQL 门面共用一个).
    pub sql_shared: std::sync::Arc<SqlSharedRoutes>,
    /// ⭐ F83: TLS 配置 (None = 明文; Some = SQL 门面 STARTTLS 可升级).
    pub tls_config: Option<std::sync::Arc<rustls::ServerConfig>>,
}

pub struct WorkerPool {
    handles: Vec<thread::JoinHandle<()>>,
}

impl WorkerPool {
    pub fn start(configs: Vec<WorkerConfig>) -> std::io::Result<Self> {
        let mut handles = Vec::with_capacity(configs.len());
        for cfg in configs {
            let wid = cfg.worker_id;
            let join = thread::Builder::new()
                .name(format!("network-worker-{wid}"))
                .stack_size(4 * 1024 * 1024)
                .spawn(move || worker_main_epoll(cfg))
                .map_err(|e| std::io::Error::other(format!("spawn: {e}")))?;
            handles.push(join);
        }
        Ok(Self { handles })
    }

    pub fn join(self) -> std::io::Result<()> {
        for h in self.handles {
            h.join().map_err(|_| std::io::Error::other("worker panicked"))?;
        }
        Ok(())
    }
}

// =====================================================================
// ⭐ X3 (SQL 落地): worker 端规划器的聚合/上下文状态
// =====================================================================

/// CREATE TABLE: SetSchemaOp 广播聚合 (N shard 各一份 PutOk).
struct SqlDdlAgg {
    remaining: usize,
    error: Option<String>,
    /// 成功后填 worker schema 缓存的 key/值.
    key: (String, String),
    schema: std::sync::Arc<TableSchema>,
    /// ⭐ F79: ALTER (非 CREATE) — 完成时递增 ddl_epoch 使其他 worker 旧 schema 缓存失效.
    alter: bool,
}

/// SELECT 索引路径: IndexScan 广播聚合.
/// 完成时: (val, pk) 排序 → decode row → 全条件残余过滤 → LIMIT → 渲染.
struct SqlSelectAgg {
    remaining: usize,
    error: Option<String>,
    rows: Vec<storage::sql_rows::IndexEntry>,
    schema: std::sync::Arc<TableSchema>,
    conds: Pred<Cond>,
    limit: Option<u32>,
    /// ⭐ O1: 投影列号 (渲染只出这些列, 顺序 = 投影序).
    proj: Vec<u16>,
    /// ⭐ O1: 覆盖索引 — Some((索引列号, pk 列号)) 时条目免回表,
    /// 行值从 (val, pk) 重建 (仅这两列有值; 覆盖判定保证过滤/投影不越界).
    cover: Option<(u16, u16)>,
    /// ⭐ O3: 唯一索引等值查询 — 首个非空 Rows 即回复 (早停).
    unique_early: bool,
    /// ⭐ O3: 已回复 (早停后续收迟到回包只减计数丢结果, 防重复 complete).
    done: bool,
    /// ⭐ S1: DML 两阶段 — Some 时本聚合是 DELETE/UPDATE 的 phase1
    /// (收行过滤取 pk, 完成后发 phase2 而非渲染结果集).
    dml: Option<SqlDmlAction>,
    /// ⭐ G2 (F63): 广义聚合计划 (Some = GROUP BY/聚合函数路径).
    agg_spec: Option<AggSpec>,
    /// ⭐ S1: phase2 发送目标 (db, table).
    dml_target: Option<(std::sync::Arc<str>, String)>,
    /// ⭐ S2: ORDER BY (列号, desc); 非空时 shard limit 不下推 (需全量排序).
    order: Vec<(u16, bool)>,
    /// ⭐ M2b (2026-08): 排序消排 — ORDER BY 单列 ASC 且 == 索引列 →
    /// 索引序即排序序, worker 端免 sql_order_cmp 全量排序; 配合 top-k 下推.
    sorted: bool,
    /// ⭐ S2: OFFSET (排序后跳过).
    offset: u32,
    /// ⭐ S2: COUNT(*) — 输出单行计数 (免投影; limit/offset 不影响计数).
    count: bool,
    /// ⭐ F76: 投影输出列名 (与 proj 同序; None = 用 schema 列名, 空 vec = 全 None).
    out_names: Vec<Option<String>>,
    /// ⭐ compat: 表达式投影 — 与 proj 同长; Some(bound) = 该输出列为表达式求值
    /// (JSONB 取字段; base 列号已含于 proj), None = 直接输出 proj 列.
    expr_proj: Vec<Option<BoundExpr>>,
    /// ⭐ P0-2: 投影下推 — 非空 = shard 只回这些列 (行内列序 = 下标).
    /// 仅简单 SELECT (无 ORDER/聚合/COUNT/DML/覆盖索引) 的 FullScan 启用.
    down_proj: Vec<u16>,
    /// ⭐ P0-2: 投影下推收行 (与 down_proj 同序; 仅 down_proj 非空时使用).
    plain_rows: Vec<Vec<ColValue>>,
}

/// ⭐ S1: 两阶段 DML 的动作 (phase2 每 pk 一发).
/// ⭐ PG 兼容: Update 的 sets 支持值或表达式 (SetVal), 表达式由 shard 端
/// `row_update` 读旧行后求值 (原子读改写).
#[derive(Clone)]
enum SqlDmlAction {
    Delete,
    Update(Vec<(u16, storage::row::SetVal)>),
}

/// ⭐ F67/F68 (JOIN): N 表左深 hash join 状态机阶段.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinPhase {
    /// 拉取 tables[idx] 的 schema (单 shard GetSchemaOp).
    FetchSchema(usize),
    /// 广播 gather tables[idx] (ScanFiltered).
    Gather(usize),
    /// ⭐ M3-2: 收集表行数 (双表 Inner 驱动选择; ctx.est_phase 0=tables[0], 1=tables[1]).
    EstimateRows,
}

/// ⭐ F68 (JOIN): 参与 JOIN 的单表运行态.
struct JoinTable {
    table: std::sync::Arc<str>,
    alias: String,
    schema: Option<std::sync::Arc<TableSchema>>,
    /// 下推投影列号 (升序; 与 rows 每行位置一一对应).
    proj: Vec<u16>,
    rows: Vec<Vec<ColValue>>,
    /// ⭐ F75: 派生表预填 (rows 已物化, schema 合成) — kickoff/gather 跳过其 fetch/scan.
    prefilled: bool,
}

/// ⭐ F67/F68 (JOIN): N 表左深 hash join 上下文 (worker 完成点执行).
/// 顺序: 逐表补 schema → 逐表 Gather (广播 ScanFiltered) → 左深迭代 hash join 渲染.
/// tables[0] = from; tables[i+1] = joins[i].table.
struct SqlJoinCtx {
    db: std::sync::Arc<str>,
    tables: Vec<JoinTable>,
    joins: Vec<sql::JoinClause>,
    items: Vec<JoinItem>,
    conds: Pred<JoinCond>,
    order: Vec<(QualCol, bool)>,
    limit: Option<u32>,
    offset: Option<u32>,
    phase: JoinPhase,
    remaining: usize,
    /// ⭐ M3-2 (连接顺序): 双表 Inner 驱动交换 — 先 Gather 小表 (全量), 大表 key_set 点查.
    /// 输出列序仍按 tables 顺序 (col_offset 固定), 仅广播/gather 顺序交换.
    swapped: bool,
    /// ⭐ M3-2: gather 顺序 (表下标; 默认 [0,1,...], swapped 双表 [1,0]).
    gather_order: Vec<usize>,
    /// ⭐ M3-2/M3-4/M3-5 + 方案 A: EstimateRows 收集批次 — 0=两表行数 (合并一轮,
    /// group 0/1 区分表), 1=两表 distinct (合并一轮), 2=两表 ranges (合并一轮).
    /// 行数批收齐后两表均 ≤ EST_SKIP_STATS_ROWS → 跳过 1/2 直接决策.
    est_phase: u8,
    est_rows: [u64; 2],
    /// ⭐ M3-4: 每表索引列 distinct 基数 (ti → iid → distinct; 仅双表 Inner
    /// EstimateRows 路径收集; 无数据 = 空 map, index_hint 退化为取第一个 Eq).
    join_distinct: Vec<std::collections::HashMap<u32, u64>>,
    /// ⭐ M3-5: 每表索引列 (min, max) 有序字节 (ti → iid → (min,max); 范围候选占比).
    join_ranges: Vec<std::collections::HashMap<u32, (Vec<u8>, Vec<u8>)>>,
}

/// ⭐ F67 (JOIN): 单侧 gather 行数上限 (止 worker OOM; 超限报错).
const JOIN_MAX_ROWS: usize = 262_144;

/// ⭐ F70 (JOIN): 键集合下推上限 (超阈退回全表扫; 海量点查劣于全扫).
const JOIN_KEYSET_MAX: usize = 1024;

// SQL 规划/执行状态结构体已拆至 sql_state.rs (解耦 2026-08), 见上方 re-export.

/// 单个连接状态.
pub(crate) struct ConnState {
    pub(crate) fd: RawFd,
    pub(crate) stream: TcpStream,
    /// ⭐ F83: TLS 会话 (None = 明文; Some = 已 STARTTLS 升级, recv/send 走 rustls).
    pub(crate) tls: Option<Box<rustls::ServerConnection>>,
    pub(crate) read_buf: Vec<u8>,
    pub(crate) proto: ProtocolKind,
    /// RESP: 是否已通过 AUTH (无密码配置时恒 true).
    pub(crate) authenticated: bool,
    /// RESP: 下一条命令分配的 seq (作为 ShardTask.req_id).
    pub(crate) next_seq: u64,
    /// RESP: 下一个应发送的 seq (FIFO 重排游标).
    pub(crate) next_to_send: u64,
    /// RESP: 已就绪但前面还有洞的回复字节.
    pub(crate) pending: BTreeMap<u64, Vec<u8>>,
    /// RESP: DEL 多 key 聚合 (seq → 状态).
    pub(crate) del_agg: HashMap<u64, DelAgg>,
    /// RESP: MGET 聚合 (seq → 状态).
    pub(crate) mget_agg: HashMap<u64, MGetAgg>,
    /// RESP: MSET 聚合 (seq → 状态).
    pub(crate) mset_agg: HashMap<u64, MSetAgg>,
    /// RESP: EXISTS 聚合 (seq → 状态).
    pub(crate) exists_agg: HashMap<u64, ExistsAgg>,
    /// RESP: STRLEN/TYPE 的 Get 语义转换 (seq → kind).
    pub(crate) get_kind: HashMap<u64, GetKind>,
    /// RESP: GETRANGE 的 (start, end) 参数 (seq → 参数; Get 后切片).
    pub(crate) getrange_ctx: HashMap<u64, (i64, i64)>,
    /// RESP: MSETNX 聚合 (seq → 状态).
    pub(crate) msetnx_agg: HashMap<u64, MSetNxAgg>,
    /// RESP: Pairs 结果渲染形态 (HGETALL/HKEYS/HVALS/HSCAN).
    pub(crate) pairs_kind: HashMap<u64, PairsKind>,
    /// RESP: HMSET 的 Integer 结果改回 +OK.
    pub(crate) hmset_ok: std::collections::HashSet<u64>,
    /// RESP: Members 结果渲染形态 (SMEMBERS/SSCAN/SPOP...).
    pub(crate) members_kind: HashMap<u64, MembersKind>,
    /// RESP: SINTER/SUNION/SDIFF 聚合 (seq → 状态).
    pub(crate) setalg_agg: HashMap<u64, SetAlgAgg>,
    /// ⭐ C1: ZMSCORE 的 Values 按裸 bulk 渲染 (score 串已成形, 不走 render tag).
    pub(crate) values_raw: std::collections::HashSet<u64>,
    /// ⭐ C3: *STORE 第二阶段聚合 (seq → 状态).
    pub(crate) store_agg: HashMap<u64, StoreFinishAgg>,
    /// ⭐ C3: ZINTERSTORE/ZUNIONSTORE 源聚合 (seq → 状态).
    pub(crate) zstore_agg: HashMap<u64, ZStoreAgg>,
    /// ⭐ Phase G: Geo 渲染上下文 (seq → 状态).
    pub(crate) geo_ctx: HashMap<u64, GeoCtx>,
    /// ⭐ Phase B: Bitmap 读渲染上下文 (seq → 状态).
    pub(crate) bit_ctx: HashMap<u64, BitCtx>,
    /// ⭐ D3 (分库): 当前连接选中的 db (SELECT n 翻译后的 name; 断连重置).
    pub(crate) current_db: std::sync::Arc<str>,
    /// ⭐ T2 (分表): 表名前缀 → Arc<str> 缓存 (免热路径每 op 一次 String 分配).
    pub(crate) table_cache: HashMap<Vec<u8>, std::sync::Arc<str>>,
    /// ⭐ X3 (SQL): worker 级共享缓存 (schema + 索引路由; 同 worker 全 conn 共享).
    pub(crate) sql_cache: SharedSqlCache,
    /// ⭐ ORM-B2: 进程级共享路由缓存 (跨 worker/门面).
    pub(crate) sql_shared: std::sync::Arc<SqlSharedRoutes>,
    /// ⭐ X3: CREATE TABLE 的 SetSchemaOp 广播聚合 (seq → 状态).
    pub(crate) sql_ddl_agg: HashMap<u64, SqlDdlAgg>,
    /// ⭐ S1: DML 计数聚合 (多行 INSERT / DELETE·UPDATE phase2 / DROP 广播).
    pub(crate) sql_dml_agg: HashMap<u64, SqlDmlAgg>,
    /// ⭐ PG 兼容 (FMT_VER 8): FK 级联 — 根/子 DELETE 的 phase1 收的
    /// (表, 被删 pk) 列表 (Fire::Dml 存; DmlAgg 完成时触发/推进级联).
    pub(crate) cascade_pending: HashMap<u64, (std::sync::Arc<str>, String, Vec<Vec<u8>>)>,
    /// ⭐ PG 兼容: 级联子任务 (伪高位 seq → 完成时推进, 不回包).
    pub(crate) cascade_jobs: HashMap<u64, CascadeJob>,
    /// ⭐ PG 兼容: 级联根状态 (主 DELETE seq → 计数/失败/防环).
    pub(crate) cascade_roots: HashMap<u64, crate::worker::sql_cascade::CascadeRoot>,
    /// ⭐ PG 兼容: 级联伪 seq 计数器.
    pub(crate) cascade_seq_ctr: u64,
    /// ⭐ PG 兼容 (引用完整性, FMT_VER 8): 外键 INSERT 存在性预检 (seq → 状态).
    pub(crate) sql_fk_ins: HashMap<u64, SqlFkIns>,
    /// ⭐ PG 兼容 (multi-statement): 原 seq → 多语句顺序执行状态.
    pub(crate) multi_stmt: HashMap<u64, MultiStmt>,
    /// ⭐ PG 兼容 (multi-statement): 子 seq → 原 seq (DDL/DML 完成时回映射推进).
    multi_sub_seq: HashMap<u64, u64>,
    /// ⭐ 巨型 INSERT 防死锁 (2026-08): worker reply bus + 路由上下文 — 批量 push
    /// 超过 inbox/reply_bus 容量时, 在 push 循环内先 drain reply_bus 处理回包,
    /// 打破 worker(等 inbox)↔shard(等 reply_bus) 循环等待.
    reply_bus: SharedTaskReplyBus,
    reply_db_view: std::sync::Arc<shard_manager::DbDirView>,
    reply_worker_id: u32,
    reply_num_shards: usize,
    reply_default_db: std::sync::Arc<str>,
    reply_shard_inboxes: Vec<SharedTaskInbox>,
    /// ⭐ 事务 v1 (F61): 当前事务缓冲 (None = autocommit).
    txn: Option<TxnState>,
    /// ⭐ 事务 v1 (F61): PG 语义 — 事务内语句出错后拒后续 (25P02),
    /// COMMIT/ROLLBACK 清位. MySQL 语义不置位 (语句失败事务继续).
    txn_failed: bool,
    /// ⭐ 事务 v1 (F61): COMMIT 聚合 (seq → 多 shard TxnApply 计数).
    sql_txn_agg: HashMap<u64, SqlTxnAgg>,
    /// ⭐ F65: 全局 UNIQUE INSERT 编排 (seq → 状态机).
    sql_unique_ins: HashMap<u64, SqlUniqueIns>,
    /// ⭐ F66: 系统表查询挂起 (seq → spec; CatalogDump 回来后合成).
    sql_sysq: HashMap<u64, SysQuerySpec>,
    /// ⭐ F67 (JOIN): 两表 hash join 顺序状态机 (seq → ctx).
    sql_join: HashMap<u64, SqlJoinCtx>,
    /// ⭐ F71 (子查询): WHERE 子查询编排 (seq → ctx).
    sql_subq: HashMap<u64, SubqCtx>,
    /// ⭐ F72 (派生表): FROM 派生表编排 (seq → ctx).
    sql_derived: HashMap<u64, DerivedCtx>,
    /// ⭐ v2 (F62): 连接级默认隔离级别/读写属性 (SET SESSION TRANSACTION).
    default_iso: sql::TxnIso,
    default_ro: bool,
    /// ⭐ X3: SELECT 索引路径广播聚合 (seq → 状态).
    sql_select_agg: HashMap<u64, SqlSelectAgg>,
    /// ⭐ X3: SELECT pk 点查渲染上下文 (seq → 状态).
    sql_row_ctx: HashMap<u64, SqlRowCtx>,
    /// ⭐ X3: schema miss 挂起的语句 (seq → 语句; GetSchemaOp 回来后续跑).
    sql_pending: HashMap<u64, PendingSql>,
    /// ⭐ Z2 (MySQL wire): Sql conn 的握手/登录状态 (非 Sql conn 为 None).
    mysql: Option<MysqlState>,
    /// ⭐ S4: PG wire 状态 (0 = 等 startup, 1 = 等 password/SASL, 2 = 已认证).
    pg_phase: u8,
    /// ⭐ F82: PG SCRAM-SHA-256 会话状态 (仅 SCRAM 认证期非 None).
    pg_scram: Option<crate::protocol::pg::ScramState>,
    /// ⭐ H2: HTTP KV 请求渲染簿记 (seq → 请求上下文).
    http_ctx: HashMap<u64, HttpReqCtx>,
    /// ⭐ P2: MySQL 预处理语句注册表 (stmt_id → 模板).
    mysql_stmts: HashMap<u32, MyPrepared>,
    next_stmt_id: u32,
    /// ⭐ P2: COM_STMT_EXECUTE 的 seq → 结果集需用二进制协议编码.
    mysql_binary: std::collections::HashSet<u64>,
    /// ⭐ P3: PG 命名预处理语句 (Parse 注册; "" = unnamed, 每次覆盖).
    pg_stmts: HashMap<String, PgPrepared>,
    /// ⭐ P3 (portal): Parse 时 schema miss 挂起的 prepared (name → 待解析).
    pg_pending_prepares: HashMap<String, PgPendingPrepare>,
    /// ⭐ P3 (portal): 当前扩展批次在等 schema (Sync 到达时暂不 flush, 等续跑).
    pg_waiting_schema: bool,
    /// ⭐ P3 (portal): 挂起批次的 GetSchemaOp 关联 seq (回包续跑用).
    pg_waiting_schema_seq: u64,
    /// ⭐ P3: 扩展查询批次累积 (Parse..Sync 之间).
    pg_batch: PgBatch,
    /// ⭐ P3: seq → 扩展协议响应前缀 (resp_complete 单点拼接).
    pg_ext: HashMap<u64, Vec<u8>>,
    /// RESP: QUIT/协议错误后, 待 pending 清空即关连接.
    close_after_flush: bool,
}

/// ⭐ Z2: MySQL 连接登录状态机.
struct MysqlState {
    salt: [u8; 20],
    /// 0 = 等 HandshakeResponse41; 1 = 等 AuthSwitch 响应; 2 = 已认证.
    phase: u8,
    /// ⭐ S5: 登录报文带的 database (AuthSwitch 二段认证后再切库).
    pending_db: Option<String>,
}

/// ⭐ P2: MySQL 预处理语句 (conn 级注册表项).
struct MyPrepared {
    stmt: SqlStmt,
    params: u16,
    /// COM_STMT_EXECUTE new_params_bound=0 时复用的类型缓存.
    types: Option<Vec<(u8, u8)>>,
}

/// ⭐ P3: PG 命名预处理语句.
struct PgPrepared {
    stmt: SqlStmt,
    params: u16,
    /// Parse 声明的参数 OID (二进制格式参数解码依据; 0 = 未声明).
    oids: Vec<u32>,
}

/// ⭐ P3 (portal): Parse 时目标表 schema 未入 worker 缓存 → 挂起的 prepared.
/// 待 GetSchemaOp 拉回 schema 后推断参数 OID, 再插入 pg_stmts 并续跑批次.
struct PgPendingPrepare {
    name: String,
    stmt: SqlStmt,
    params: u16,
    oids: Vec<u32>,
}

/// ⭐ P3: PG 扩展查询批次状态 (Parse..Sync 累积; Sync 时消费重置).
#[derive(Default)]
struct PgBatch {
    /// 累积的即时响应帧 (ParseComplete/BindComplete/CloseComplete/ParamDesc...).
    prefix: Vec<u8>,
    /// Bind 完成的待执行语句.
    bound: Option<SqlStmt>,
    /// Execute 收到 (无 Execute 的批次 Sync 时只回前缀).
    has_execute: bool,
    /// 批内错误 → skip-to-Sync (协议标准行为).
    error: Option<String>,
}

/// ⭐ H2: HTTP KV 请求上下文 (回包渲染 JSON 用).
struct HttpReqCtx {
    op: HttpKvOp,
    keep_alive: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum HttpKvOp {
    Get,
    Put,
    Delete,
}

/// ⭐ T2 (分表): key 首个 `:` 前的前缀若为合法表名 (`[A-Za-z0-9_.-]{1,64}`)
/// 返回其字节长; 否则 None → 整个 key 落 default 表
/// (防二进制 key 撞 `:` 字节产生垃圾表 + 无界建表).
fn split_table_key(raw: &[u8]) -> Option<usize> {
    let pos = raw.iter().position(|&b| b == b':')?;
    if pos == 0 || pos > 64 {
        return None;
    }
    raw[..pos]
        .iter()
        .all(|&b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
        .then_some(pos)
}

/// epoll 事件循环主函数.
fn worker_main_epoll(cfg: WorkerConfig) {
    let epoll_fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    assert!(epoll_fd >= 0, "epoll_create1 failed");

    let mut conn_map: HashMap<u64, ConnState> = HashMap::new();
    let mut next_conn_id: u64 = 0;

    // 注册 reply_bus eventfd + 新连接通知 eventfd
    epoll_add(epoll_fd, cfg.reply_bus.eventfd(), REPLY_TOKEN);
    epoll_add(epoll_fd, cfg.conn_eventfd, NEW_CONN_TOKEN);

    let shard_inboxes = cfg.shard_inboxes;
    let reply_bus = cfg.reply_bus;
    let worker_id = cfg.worker_id;
    // ⭐ 热路径优化: db/table 一次性转 Arc<str>, 每 op 仅引用计数 clone
    let db: std::sync::Arc<str> = std::sync::Arc::from(cfg.default_db.as_str());
    let table: std::sync::Arc<str> = std::sync::Arc::from(cfg.default_table.as_str());
    // ⭐ W1: SQL worker 级共享缓存 (schema + 索引路由; 单线程 Rc<RefCell>)
    let sql_cache: SharedSqlCache = std::rc::Rc::new(std::cell::RefCell::new(
        SqlWorkerCache::default(),
    ));
    // ⭐ ORM-B2: 进程级共享路由缓存 (server 注入, 跨 worker/门面)
    let sql_shared = cfg.sql_shared;
    // ⭐ D3 (分库): SELECT n → db name 翻译视图
    let db_view = cfg.db_view;
    let inbox = cfg.inbox;
    let conn_eventfd = cfg.conn_eventfd;
    let proto_kind = cfg.protocol;
    let limits = cfg.limits;
    let auth_password = cfg.auth_password;
    let auth_required = auth_password.is_some();
    let tls_config = cfg.tls_config; // ⭐ F83: None = 明文门面
    let num_shards = shard_inboxes.len();

    let mut events = vec![
        libc::epoll_event { events: 0, u64: 0 };
        256
    ];

    loop {
        // 检查新连接 (非阻塞; eventfd 另有精确唤醒, 这里是兑底)
        // ⭐ 退出条件: acceptor 侧 sender 已 drop (shutdown) 且无存活连接
        let mut inbox_disconnected = false;
        loop {
            match inbox.try_recv() {
                Ok(new_conn) => {
                    let id = next_conn_id;
                    next_conn_id += 1;
                    let mut state = ConnState::new(new_conn.fd, proto_kind, auth_required, db.clone(), sql_cache.clone(), sql_shared.clone(), reply_bus.clone(), db_view.clone(), worker_id, num_shards, shard_inboxes.clone());
                    // ⭐ Z2 (MySQL wire): Sql conn 建立即主动发 HandshakeV10
                    if proto_kind == ProtocolKind::Sql {
                        let salt = mysql_gen_salt(id, worker_id);
                        state.send_bytes(&crate::protocol::mysql::build_handshake_v10_caps(
                            &salt, id as u32, tls_config.is_some(),
                        ));
                        state.mysql = Some(MysqlState { salt, phase: 0, pending_db: None });
                    }
                    epoll_add(epoll_fd, state.fd, id);
                    conn_map.insert(id, state);
                    nlog::debug!("worker", "worker-{worker_id} conn {id} from {}", new_conn.peer);
                }
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    inbox_disconnected = true;
                    break;
                }
            }
        }
        if inbox_disconnected {
            // acceptor 侧 sender 已全部 drop = server 正在 shutdown.
            // 强制退出: 剩余连接随 conn_map drop 一并 close (TcpStream drop).
            break;
        }

        // 所有事件源 (conn readable / reply_bus / 新连接) 都有 eventfd/fd 精确唤醒,
        // 100ms timeout 仅兑底.
        let n = unsafe {
            libc::epoll_wait(epoll_fd, events.as_mut_ptr(), events.len() as i32, 100)
        };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            break;
        }

        for ev in events.iter().take(n as usize) {
            let token = ev.u64;

            if token == REPLY_TOKEN {
                // shard 有回复: drain bus, 按协议分发
                let results = reply_bus.drain();
                for r in results {
                    let mut close_conn = false;
                    if let Some(conn) = conn_map.get_mut(&r.conn_id) {
                        match conn.proto {
                            ProtocolKind::Binary => {
                                let resp = batch_result_to_response(&r.result);
                                conn.send_binary_response(r.req_id, &resp);
                            }
                            ProtocolKind::Resp
                            | ProtocolKind::Sql
                            | ProtocolKind::Pg
                            | ProtocolKind::Http => {
                                // ⭐ Y2/S4/H1: SQL/HTTP conn 复用 seq 重排 + 聚合钩子
                                handle_resp_shard_result(
                                    conn,
                                    r.conn_id,
                                    r.req_id,
                                    r.group,
                                    &r.result,
                                    worker_id,
                                    &db,
                                    &db_view,
                                    &shard_inboxes,
                                    num_shards,
                                );
                                close_conn = conn.resp_should_close();
                            }
                        }
                    }
                    if close_conn {
                        remove_conn(epoll_fd, &mut conn_map, r.conn_id, worker_id);
                    }
                }
            } else if token == NEW_CONN_TOKEN {
                // 新连接通知: 消耗 eventfd 计数 (nonblocking), 连接在循环顶部 try_recv 接收
                let mut v: u64 = 0;
                unsafe {
                    libc::read(conn_eventfd, &mut v as *mut u64 as *mut libc::c_void, 8);
                }
                while let Ok(new_conn) = inbox.try_recv() {
                    let id = next_conn_id;
                    next_conn_id += 1;
                    let mut state = ConnState::new(new_conn.fd, proto_kind, auth_required, db.clone(), sql_cache.clone(), sql_shared.clone(), reply_bus.clone(), db_view.clone(), worker_id, num_shards, shard_inboxes.clone());
                    // ⭐ Z2 (MySQL wire): Sql conn 建立即主动发 HandshakeV10
                    if proto_kind == ProtocolKind::Sql {
                        let salt = mysql_gen_salt(id, worker_id);
                        state.send_bytes(&crate::protocol::mysql::build_handshake_v10_caps(
                            &salt, id as u32, tls_config.is_some(),
                        ));
                        state.mysql = Some(MysqlState { salt, phase: 0, pending_db: None });
                    }
                    epoll_add(epoll_fd, state.fd, id);
                    conn_map.insert(id, state);
                    nlog::debug!("worker", "worker-{worker_id} conn {id} from {}", new_conn.peer);
                }
            } else {
                // conn 可读: recv + parse + 校验 + route + push
                let conn_id = token;
                let mut should_remove = false;
                if let Some(conn) = conn_map.get_mut(&conn_id) {
                    match conn.recv() {
                        Ok(true) => match conn.proto {
                            ProtocolKind::Binary => {
                                process_binary_input(
                                    conn, conn_id, worker_id, &db, &table, &limits,
                                    &shard_inboxes, num_shards,
                                );
                            }
                            ProtocolKind::Resp => {
                                process_resp_input(
                                    conn, conn_id, worker_id, &db_view, &table, &limits,
                                    &auth_password, &shard_inboxes, num_shards,
                                );
                                should_remove = conn.resp_should_close();
                            }
                            ProtocolKind::Sql => {
                                // ⭐ Z2: MySQL wire 帧循环
                                process_sql_input(
                                    conn, conn_id, worker_id, &auth_password, &db, &db_view,
                                    &shard_inboxes, num_shards, &tls_config,
                                );
                                should_remove = conn.resp_should_close();
                            }
                            ProtocolKind::Pg => {
                                // ⭐ S4: PostgreSQL wire 帧循环
                                process_pg_input(
                                    conn, conn_id, worker_id, &auth_password, &db, &db_view,
                                    &shard_inboxes, num_shards, &tls_config,
                                );
                                should_remove = conn.resp_should_close();
                            }
                            ProtocolKind::Http => {
                                // ⭐ H1: HTTP/1.1 REST 帧循环 (token = auth_password)
                                process_http_input(
                                    conn, conn_id, worker_id, &auth_password, &db, &db_view,
                                    &limits, num_shards, &shard_inboxes, num_shards,
                                );
                                should_remove = conn.resp_should_close();
                            }
                        },
                        Ok(false) => should_remove = true, // EOF
                        Err(_) => should_remove = true,
                    }
                }
                if should_remove {
                    remove_conn(epoll_fd, &mut conn_map, conn_id, worker_id);
                }
            }
        }
    }

    unsafe {
        libc::close(conn_eventfd);
        libc::close(epoll_fd);
    }
}

fn remove_conn(
    epoll_fd: RawFd,
    conn_map: &mut HashMap<u64, ConnState>,
    conn_id: u64,
    worker_id: u32,
) {
    if let Some(conn) = conn_map.remove(&conn_id) {
        epoll_del(epoll_fd, conn.fd);
        nlog::debug!("worker", "worker-{worker_id} conn {conn_id} closed");
    }
}

// ===== Binary 协议输入处理 =====

#[allow(clippy::too_many_arguments)]
fn process_binary_input(
    conn: &mut ConnState,
    conn_id: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    table: &std::sync::Arc<str>,
    limits: &KvLimits,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    let proto = BinaryProtocol::new();
    // ⭐ 热路径优化: 游标推进, 循环末一次 drain — 消 pipeline 下
    // 每帧 memmove 尾部字节的 O(n²).
    let mut cursor = 0usize;
    loop {
        match proto.decode_request(&conn.read_buf[cursor..]) {
            Ok(DecodeOutcome::Complete { consumed, value }) => {
                let req_id = peek_req_id(&conn.read_buf[cursor..cursor + consumed]);
                cursor += consumed;
                // ⭐ 长度校验: 超限不进 shard, 直接回 error 帧
                if let Err(msg) = validate_request(&value, limits) {
                    conn.send_binary_response(req_id, &Response::Error(msg));
                    continue;
                }
                let op = request_to_batch_op(value, db, table);
                let shard_id = hash_route_op(&op, num_shards);
                shard_inboxes[shard_id].push_spin(ShardTask {
                    conn_id,
                    req_id,
                    worker_id,
                    group: 0,
                    op,
                });
            }
            Ok(DecodeOutcome::NeedMore) => break,
            Err(_) => {
                if cursor < conn.read_buf.len() {
                    cursor += 1; // 重同步: 跳过 1 字节
                } else {
                    break;
                }
            }
        }
    }
    if cursor > 0 {
        conn.read_buf.drain(..cursor);
    }
}

// ===== RESP 协议输入处理 =====

#[allow(clippy::too_many_arguments)]
fn process_resp_input(
    conn: &mut ConnState,
    conn_id: u64,
    worker_id: u32,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    table: &std::sync::Arc<str>,
    limits: &KvLimits,
    auth_password: &Option<String>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    let codec = RespCodec::new();
    // ⭐ 热路径优化: 游标推进, 循环末一次 drain (pipeline 下免每命令 memmove)
    let mut cursor = 0usize;
    loop {
        if conn.close_after_flush {
            // QUIT/协议错误后不再解析后续输入
            conn.read_buf.clear();
            cursor = 0;
            break;
        }
        match codec.decode_command(&conn.read_buf[cursor..]) {
            Ok(DecodeOutcome::Complete { consumed, value }) => {
                cursor += consumed;
                // ⭐ D3 (分库): 每命令取当前连接 db (SELECT 可在 pipeline 中切换)
                let cur_db = conn.current_db.clone();
                dispatch_resp_command(
                    conn, conn_id, worker_id, &cur_db, table, limits, auth_password,
                    db_view, shard_inboxes, num_shards, value,
                );
            }
            Ok(DecodeOutcome::NeedMore) => break,
            Err(msg) => {
                // RESP 流错位无法重新同步: 回 error 后关连接
                let seq = conn.next_seq;
                conn.next_seq += 1;
                let bytes = codec.encode_error(&msg);
                conn.resp_complete(seq, bytes);
                conn.close_after_flush = true;
                conn.read_buf.clear();
                cursor = 0;
                break;
            }
        }
    }
    if cursor > 0 {
        conn.read_buf.drain(..cursor);
    }
}


/// ⭐ Phase H: Pairs 结果渲染 (HGETALL/HKEYS/HVALS/HSCAN 共用).
fn encode_pairs(codec: &RespCodec, ps: &[(Vec<u8>, Vec<u8>)], kind: PairsKind) -> Vec<u8> {
    match kind {
        PairsKind::All => {
            let mut out = format!("*{}\r\n", ps.len() * 2).into_bytes();
            for (f, v) in ps {
                out.extend_from_slice(&codec.encode_bulk(f));
                out.extend_from_slice(&codec.encode_bulk(&render(v)));
            }
            out
        }
        PairsKind::Keys => {
            let mut out = format!("*{}\r\n", ps.len()).into_bytes();
            for (f, _) in ps {
                out.extend_from_slice(&codec.encode_bulk(f));
            }
            out
        }
        PairsKind::Vals => {
            let mut out = format!("*{}\r\n", ps.len()).into_bytes();
            for (_, v) in ps {
                out.extend_from_slice(&codec.encode_bulk(&render(v)));
            }
            out
        }
        PairsKind::Scan => {
            // HSCAN v1: 单次全量返回, cursor 恒为 "0"
            let mut out = b"*2\r\n".to_vec();
            out.extend_from_slice(&codec.encode_bulk(b"0"));
            out.extend_from_slice(&encode_pairs(codec, ps, PairsKind::All));
            out
        }
        // ⭐ C1: HRANDFIELD 无 count — 首 field 单 bulk / nil
        PairsKind::OneKey => match ps.first() {
            Some((f, _)) => codec.encode_bulk(f),
            None => codec.encode_nil(),
        },
    }
}

// ===== 辅助函数 =====

fn epoll_add(epoll_fd: RawFd, fd: RawFd, token: u64) {
    // 水平触发 (默认): 比边缘触发更稳健, 不会丢事件
    let mut event = libc::epoll_event {
        events: libc::EPOLLIN as u32,
        u64: token,
    };
    unsafe {
        libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, fd, &mut event);
    }
}

fn epoll_del(epoll_fd: RawFd, fd: RawFd) {
    unsafe {
        libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut());
    }
}

fn peek_req_id(frame: &[u8]) -> u64 {
    if frame.len() < 12 {
        return 0;
    }
    u64::from_be_bytes(frame[4..12].try_into().unwrap())
}

/// Request → BatchOp. ⭐ `Request::Put.value` 已是 `[tag][payload]` 布局
/// (decode 时预置), 直接 move — 零二次拷贝.
fn request_to_batch_op(req: Request, db: &std::sync::Arc<str>, table: &std::sync::Arc<str>) -> BatchOp {
    match req {
        Request::Put { key, value } => BatchOp::Put {
            db: db.clone(),
            table: table.clone(),
            key,
            val: value,
        },
        Request::Get { key } => BatchOp::Get {
            db: db.clone(),
            table: table.clone(),
            key,
        },
        Request::Delete { key } => BatchOp::Delete {
            db: db.clone(),
            table: table.clone(),
            key,
        },
    }
}

/// BatchResult → Binary Response. ⭐ Get 命中时剥 value type tag.
/// (注: payload.to_vec 是 Response::Get(Option<Vec>) 结构所需;
/// Binary 非 benchmark 主路径, 借用化需改 Protocol trait, 收益不值 — 记录保留.)
fn batch_result_to_response(result: &BatchResult) -> Response {
    match result {
        BatchResult::PutOk => Response::PutOk,
        BatchResult::TxnApplied(_) => Response::PutOk, // 事务批不走 Binary 门面
        BatchResult::ReserveOk | BatchResult::ReserveConflict { .. } => Response::PutOk, // 占坑不走 Binary
        BatchResult::Catalog(_) => Response::PutOk, // catalog 不走 Binary
        BatchResult::ProjRows(_) => Response::PutOk, // JOIN 不走 Binary
        BatchResult::RowCount(_) => Response::PutOk, // M3-2 行数估计不走 Binary
        BatchResult::DistinctCounts(_) => Response::PutOk, // M3-4 distinct 不走 Binary
        BatchResult::RangeBounds(_) => Response::PutOk, // M3-5 min/max 不走 Binary
        BatchResult::GetValue(None) => Response::Get(None),
        BatchResult::GetValue(Some(stored)) => {
            let (_tag, payload) = decode_value(stored);
            Response::Get(Some(payload.to_vec()))
        }
        BatchResult::DeleteExisted(_) => Response::DeleteOk,
        // Multi/RMW/Hash op 是 RESP 专属 (Binary 门面不会产生)
        BatchResult::Values(_)
        | BatchResult::MultiPutOk
        | BatchResult::Integer(_)
        | BatchResult::Double(_)
        | BatchResult::Pairs(_)
        | BatchResult::Members(_)
        | BatchResult::OptMember(_)
        | BatchResult::IntList(_)
        | BatchResult::Rows(_) => {
            Response::Error("multi ops unsupported on binary protocol".into())
        }
        BatchResult::Error(e) => Response::Error(e.clone()),
    }
}

pub(crate) fn hash_route_op(op: &BatchOp, num_shards: usize) -> usize {
    // ⭐ T1: 单源提取 (Multi op 已由 dispatch 预分组定向 push, 不经此路径;
    // locator 对 Multi 取首 key, 与预分组路由一致, 兜底亦安全)
    let (db, table, key) = op.locator();
    hash_route_key(db, table, key, num_shards)
}

/// ⭐ Phase G: score 串 (fmt_score 输出) → 52-bit geohash.
fn geo_bits(b: &[u8]) -> Option<u64> {
    std::str::from_utf8(b)
        .ok()?
        .parse::<f64>()
        .ok()
        .filter(|f| *f >= 0.0 && *f < (1u64 << 52) as f64)
        .map(|f| f as u64)
}

/// ⭐ Phase G: Geo 命令渲染 (GEOPOS/GEODIST/GEOSEARCH).
fn render_geo(codec: &RespCodec, ctx: GeoCtx, result: &BatchResult) -> Vec<u8> {
    use crate::geo_bridge as geo;
    if let BatchResult::Error(e) = result {
        return codec.encode_error(e);
    }
    match ctx {
        // GEOPOS: 每 member → [lon, lat] 或 nil array
        GeoCtx::Pos => {
            let BatchResult::Values(vs) = result else {
            return codec.encode_error("unexpected result");
            };
            let mut out = format!("*{}\r\n", vs.len()).into_bytes();
            for v in vs {
                match v.as_deref().and_then(geo_bits) {
                    Some(bits) => {
                        let (lon, lat) = geo::decode(bits);
                        out.extend_from_slice(b"*2\r\n");
                        out.extend_from_slice(&codec.encode_bulk(format!("{lon:.17}").as_bytes()));
                        out.extend_from_slice(&codec.encode_bulk(format!("{lat:.17}").as_bytes()));
                    }
                    None => out.extend_from_slice(b"*-1\r\n"),
                }
            }
            out
        }
        // GEODIST: 两点都在才有距离
        GeoCtx::Dist { factor } => {
            let BatchResult::Values(vs) = result else {
                return codec.encode_error("unexpected result");
            };
            let b1 = vs.first().and_then(|v| v.as_deref()).and_then(geo_bits);
            let b2 = vs.get(1).and_then(|v| v.as_deref()).and_then(geo_bits);
            match (b1, b2) {
                (Some(b1), Some(b2)) => {
                    let (lon1, lat1) = geo::decode(b1);
                    let (lon2, lat2) = geo::decode(b2);
                    let d = geo::haversine_m(lon1, lat1, lon2, lat2) / factor;
                    codec.encode_bulk(format!("{d:.4}").as_bytes())
                }
                _ => codec.encode_nil(),
            }
        }
        // GEOSEARCH: 解码全量 (member, score) → 距离过滤 + 排序 + COUNT
        GeoCtx::Search { lon, lat, radius_m, asc, count, withcoord, withdist } => {
            let BatchResult::Members(ms) = result else {
                return codec.encode_error("unexpected result");
            };
            let mut hits: Vec<(&[u8], f64, f64, f64)> = Vec::new(); // (member, dist, lon, lat)
            let mut i = 0;
            while i + 1 < ms.len() {
                if let Some(bits) = geo_bits(&ms[i + 1]) {
                    let (mlon, mlat) = geo::decode(bits);
                    let d = geo::haversine_m(lon, lat, mlon, mlat);
                    if d <= radius_m {
                        hits.push((&ms[i], d, mlon, mlat));
                    }
                }
                i += 2;
            }
            hits.sort_by(|a, b| a.1.partial_cmp(&b.1).expect("dist 非 NaN"));
            if !asc {
                hits.reverse();
            }
            if count > 0 {
                hits.truncate(count);
            }
            let mut out = format!("*{}\r\n", hits.len()).into_bytes();
            for (m, d, mlon, mlat) in hits {
                if !withcoord && !withdist {
                    out.extend_from_slice(&codec.encode_bulk(m));
                    continue;
                }
                // 嵌套数组: [member, (dist), ([lon, lat])] (Redis 顺序)
                let items = 1 + usize::from(withdist) + usize::from(withcoord);
                out.extend_from_slice(format!("*{items}\r\n").as_bytes());
                out.extend_from_slice(&codec.encode_bulk(m));
                if withdist {
                    out.extend_from_slice(&codec.encode_bulk(format!("{d:.4}").as_bytes()));
                }
                if withcoord {
                    out.extend_from_slice(b"*2\r\n");
                    out.extend_from_slice(&codec.encode_bulk(format!("{mlon:.17}").as_bytes()));
                    out.extend_from_slice(&codec.encode_bulk(format!("{mlat:.17}").as_bytes()));
                }
            }
            out
        }
    }
}

/// ⭐ Phase B: BYTE 区间裁剪 (Redis 负索引语义, 与 getrange_slice 同).
fn bit_byte_range(len: usize, start: i64, end: i64) -> Option<(usize, usize)> {
    let len = len as i64;
    if len == 0 {
        return None;
    }
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if end < 0 { len + end } else { end };
    if s < 0 {
        s = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e {
        return None;
    }
    Some((s as usize, e as usize))
}

/// ⭐ Phase B: Bitmap 读命令渲染 (GETBIT/BITCOUNT/BITPOS).
fn render_bit(codec: &RespCodec, ctx: BitCtx, result: &BatchResult) -> Vec<u8> {
    if let BatchResult::Error(e) = result {
        return codec.encode_error(e);
    }
    let data: &[u8] = match result {
        BatchResult::GetValue(Some(stored)) => &render(stored),
        BatchResult::GetValue(None) => &[],
        _ => return codec.encode_error("unexpected result"),
    };
    match ctx {
        BitCtx::GetBit { offset } => {
            let byte = (offset / 8) as usize;
            let bit = if byte < data.len() {
                (data[byte] >> (7 - (offset % 8) as u8)) & 1
            } else {
                0
            };
            codec.encode_integer(bit as i64)
        }
        BitCtx::Count { start, end } => {
            let n = match bit_byte_range(data.len(), start, end) {
                Some((s, e)) => data[s..=e].iter().map(|b| b.count_ones() as i64).sum(),
                None => 0,
            };
            codec.encode_integer(n)
        }
        BitCtx::Pos { bit, start, end } => {
            // 不存在 key: 找 1 → -1; 找 0 → 0 (Redis 语义)
            if data.is_empty() {
                return codec.encode_integer(if bit { -1 } else { 0 });
            }
            let range = bit_byte_range(data.len(), start, end.unwrap_or(-1));
            let pos = match range {
                None => -1,
                Some((s, e)) => {
                    let mut found = -1i64;
                    for (i, &b) in data[s..=e].iter().enumerate() {
                        let probe = if bit { b } else { !b };
                        if probe != 0 {
                            found = ((s + i) * 8 + probe.leading_zeros() as usize) as i64;
                            break;
                        }
                    }
                    // 全 1 找 0 且未显式给 end → 返回字符串右侧第一个越界位 (Redis)
                    if found == -1 && !bit && end.is_none() {
                        found = (data.len() * 8) as i64;
                    }
                    found
                }
            };
            codec.encode_integer(pos)
        }
    }
}

// =====================================================================
// ⭐ X3 (SQL 落地): 规划 / 执行 / 过滤 / 渲染
// =====================================================================


/// ⭐ W2/事务 v1: RowPut 喂进程级路由 bloom (value → shard).
/// 事务缓冲时也喂 — rollback 后只多假阳性 (只增语义无害);
/// commit 时不重复喂.
pub(crate) fn feed_route_bloom(
    conn: &ConnState,
    db: &str,
    table: &str,
    schema: &TableSchema,
    op: &BatchOp,
    sid: usize,
) {
    let sh = &conn.sql_shared;
    let ckey = (db.to_string(), table.to_string());
    if !sh.created_here.read().unwrap().contains(&ckey) {
        return;
    }
    let BatchOp::RowPut { values, .. } = op else { return };
    for idx in schema.indexes.iter() {
        if let Some(enc) = storage::sql_rows::index_vals_bytes(&schema, idx, &values) {
            let entry = sh
                .routes
                .read()
                .unwrap()
                .get(&(ckey.0.clone(), ckey.1.clone(), idx.iid))
                .cloned();
            if let Some(blooms) = entry {
                blooms[sid].insert(&enc);
            }
        }
    }
}

/// ⭐ 事务 RYOW (F63 正确性): 重放事务内同一 pk 的全部缓冲 op,
/// 得出该 pk 的未提交最终态. 返回:
/// - `Resolved(Some(values))`: 纯内存可定 (首 op 是 RowPut 或 Delete)
/// - `Resolved(None)`: 最终被删
/// - `NeedBase(sets)`: 首 op 是基于已提交行的 UPDATE, 需读盘基行再叠加 sets
enum RyowState {
    Resolved(Option<Vec<ColValue>>),
    NeedBase(Vec<(u16, storage::row::SetVal)>),
}

fn resolve_ryow(txn: &TxnState, tkey: &(String, String, Vec<u8>)) -> Option<RyowState> {
    // 收集该 pk 的全部缓冲 op (保序)
    let ops: Vec<&BatchOp> = txn
        .ops
        .iter()
        .filter(|op| {
            let (d, t, k) = op.locator();
            d == tkey.0 && t == tkey.1 && k == tkey.2.as_slice()
        })
        .collect();
    if ops.is_empty() {
        return None;
    }
    let mut cur: Option<Vec<ColValue>> = None; // 纯内存态
    let mut pending_sets: Vec<(u16, storage::row::SetVal)> = Vec::new(); // 基于盘行的叠加
    let mut based_on_disk = false;
    for op in ops {
        match op {
            BatchOp::RowPut { values, .. } => {
                cur = Some(values.clone());
                pending_sets.clear();
                based_on_disk = false;
            }
            BatchOp::RowDelete { .. } => {
                // 删后态确定 — 后续同 pk op 只可能是再 INSERT (RowPut 覆盖),
                // 由循环继续处理; 此处仅重置
                cur = None;
                pending_sets.clear();
                based_on_disk = false;
            }
            BatchOp::RowUpdate { sets, .. } => {
                // ⭐ PG 兼容: SET 值或表达式; 事务缓冲 v1 仅支持值 (表达式需
                // 旧行求值, 事务内退化为不支持 — 由 worker 在非事务路径处理).
                if let Some(v) = cur.as_mut() {
                    for (ci, sv) in sets {
                        let Some(slot) = v.get_mut(*ci as usize) else { continue };
                        if let storage::row::SetVal::Val(cv) = sv {
                            *slot = cv.clone();
                        }
                        // Expr 在事务纯内存态无法求值 → 保持旧值 (v1 边界)
                    }
                } else {
                    // 基于已提交盘行: 累积 sets (后写覆盖前写); 表达式保留待 shard
                    based_on_disk = true;
                    for (ci, sv) in sets {
                        if let Some(e) = pending_sets.iter_mut().find(|(c, _)| c == ci) {
                            e.1 = sv.clone();
                        } else {
                            pending_sets.push((*ci, sv.clone()));
                        }
                    }
                }
            }
            _ => {}
        }
    }
    if based_on_disk {
        Some(RyowState::NeedBase(pending_sets))
    } else {
        Some(RyowState::Resolved(cur))
    }
}

/// ⭐ v2 (F63 正确性): SERIALIZABLE 事务内的 pk 点查 → 读集记录坐标
/// (RC/非事务回 None 零开销).
fn sql_read_key(
    conn: &ConnState,
    db: &std::sync::Arc<str>,
    table: &str,
    pk: &[u8],
) -> Option<(String, String, Vec<u8>)> {
    conn.txn
        .as_ref()
        .filter(|t| t.iso == sql::TxnIso::Serializable)
        .map(|_| (db.to_string(), table.to_string(), pk.to_vec()))
}

/// ⭐ 事务 v1 (F61): 写 op 进 write_set (上限护栏; 超限自动回滚).
fn txn_buffer_op(conn: &mut ConnState, op: BatchOp) -> Result<(), String> {
    let (d, t, k) = op.locator();
    let key = (d.to_string(), t.to_string(), k.to_vec());
    let sz = 128 + key.2.len(); // 粗估 (values 不逐列量)
    let txn = conn.txn.as_mut().expect("txn_buffer_op 仅事务内调用");
    if txn.ops.len() >= TXN_MAX_OPS || txn.bytes + sz > TXN_MAX_BYTES {
        conn.txn = None;
        conn.txn_failed = false;
        return Err("transaction too large (rolled back)".into());
    }
    txn.ops.push(op);
    txn.index.insert(key, txn.ops.len() - 1);
    txn.bytes += sz;
    Ok(())
}

// =====================================================================
// ⭐ F71 (子查询): 非关联 WHERE 子查询编排
// =====================================================================















/// ⭐ G2 (F63): 广义聚合 SELECT — 列名解析/类型校验/计划构建后广播
/// (索引可用时 IndexScan, 否则 TableScan; PkGet 也降级广播 — 聚合需全量行,
/// 单行情形低频可接受).
#[allow(clippy::too_many_arguments)]
fn mysql_gen_salt(conn_id: u64, worker_id: u32) -> [u8; 20] {
    let _ = (conn_id, worker_id); // ⭐ F82: 改用 CSPRNG, 不再依赖 conn/worker 派生
    // ⭐ F82: CSPRNG (/dev/urandom) 生成 salt, 映射到可打印区间 0x21..=0x7D (兼容旧客户端).
    let rnd = crate::protocol::crypto::rand_bytes(20);
    let mut salt = [0u8; 20];
    for (b, r) in salt.iter_mut().zip(rnd) {
        *b = 0x21 + (r % 93);
    }
    salt
}

