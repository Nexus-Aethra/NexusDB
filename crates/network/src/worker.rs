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

/// DEL 多 key 的聚合状态 (RESP :N 回复需等全部 Delete 完成).
struct DelAgg {
    remaining: usize,
    count: i64,
}

/// ⭐ MGET 跨 shard 聚合: 每 shard 一组, Values 按组内索引表回填原始槽.
struct MGetAgg {
    remaining: usize,
    /// 原始请求顺序的结果槽 (None = miss 或未回).
    slots: Vec<Option<Vec<u8>>>,
    /// group 号 → 该组 keys 的原始索引 (与 MultiGet keys 同序).
    groups: Vec<Vec<usize>>,
    /// 任一组失败: 记首个错误 (仍等全部组回齐再回复).
    error: Option<String>,
}

/// ⭐ MSET 跨 shard 聚合: 全部组 MultiPutOk → +OK.
struct MSetAgg {
    remaining: usize,
    error: Option<String>,
}

/// ⭐ EXISTS 多 key 聚合 (DEL 同构: 计数存在数).
struct ExistsAgg {
    remaining: usize,
    count: i64,
}

/// ⭐ MSETNX 跨 shard 聚合: 全部分片 MultiPutNx 返回 1 → :1, 否则 :0.
/// (跨 shard 非原子: 部分分片可能已写 — 已记为 gap.)
struct MSetNxAgg {
    remaining: usize,
    all_set: bool,
}

/// ⭐ 单 op Get 的回复语义转换 (STRLEN/TYPE/HEXISTS 复用 Get/HGet 任务).
#[derive(Clone, Copy)]
enum GetKind {
    Strlen,
    TypeOf,
    /// ⭐ Phase H: HEXISTS — GetValue(Some)→:1, None→:0
    HExists,
}

/// ⭐ Phase H: Pairs 结果渲染形态 (HGETALL/HKEYS/HVALS/HSCAN 复用同一 op).
#[derive(Clone, Copy)]
enum PairsKind {
    All,
    Keys,
    Vals,
    Scan,
    /// ⭐ C1: HRANDFIELD 无 count — 首 field 单 bulk / nil.
    OneKey,
}

/// ⭐ Phase Set: Members 结果渲染形态.
#[derive(Clone, Copy)]
enum MembersKind {
    /// SMEMBERS → *N
    List,
    /// SSCAN → ["0", *N]
    Scan,
    /// SPOP/SRANDMEMBER → bulk / nil (0/1 项)
    One,
}

/// ⭐ Phase Set: SINTER/SUNION/SDIFF 跨 shard 聚合 — 每 key 一个 SMembers
/// (group = key 序号), 全部回齐后 worker 端求交/并/差 (首 key 为基).
struct SetAlgAgg {
    remaining: usize,
    op: SetAlgOp,
    sets: Vec<Option<Vec<Vec<u8>>>>,
    error: Option<String>,
    /// ⭐ C1: SINTERCARD — 只回交集势 (Integer) 而非成员数组.
    card_only: bool,
    /// ⭐ C1: SINTERCARD LIMIT (0 = 无限制).
    limit: usize,
    /// ⭐ C3: *STORE — 结果写入 dst (先 DEL 再 SAdd), 回 :card.
    store_dst: Option<Vec<u8>>,
    /// ⭐ D3 (分库): 命令发起时的 (db, table) — 二阶段任务用, 防 pipeline 中
    /// SELECT 切库后错库.
    db: std::sync::Arc<str>,
    table: std::sync::Arc<str>,
}

/// ⭐ C3: *STORE 第二阶段 (Delete dst + SAdd/ZAdd dst) 完成聚合.
/// 跨 shard 非原子 (源读与目标写分离) — 与 SINTER/MSETNX 同级 gap.
struct StoreFinishAgg {
    remaining: usize,
    card: i64,
    error: Option<String>,
}

/// ⭐ C3: ZINTERSTORE/ZUNIONSTORE 源聚合 — 每源 key 一个 ZRange(withscores),
/// 回齐后 SUM 聚合写 dst (无 weights/AGGREGATE, 计划内 defer).
type ScoredMembers = Vec<(Vec<u8>, f64)>;
struct ZStoreAgg {
    remaining: usize,
    inter: bool,
    sets: Vec<Option<ScoredMembers>>,
    error: Option<String>,
    dst: Vec<u8>,
    /// ⭐ D3 (分库): 命令发起时的 (db, table) — 二阶段任务用.
    db: std::sync::Arc<str>,
    table: std::sync::Arc<str>,
}

/// ⭐ Phase G: Geo 命令的渲染上下文 (复用 ZMScore/ZRange 结果 + geohash 解码).
enum GeoCtx {
    /// GEOPOS → *N 个 [lon, lat] / nil
    Pos,
    /// GEODIST → bulk 距离 / nil
    Dist { factor: f64 },
    /// GEOSEARCH → 距离过滤 + 排序 + 可选 WITHCOORD/WITHDIST
    Search {
        lon: f64,
        lat: f64,
        radius_m: f64,
        asc: bool,
        count: usize,
        withcoord: bool,
        withdist: bool,
    },
}

/// ⭐ Phase B: Bitmap 读命令的渲染上下文 (Get 结果 + worker 位运算).
enum BitCtx {
    /// GETBIT offset → :0|:1
    GetBit { offset: u64 },
    /// BITCOUNT [start end] (BYTE, 含负索引) → :popcount
    Count { start: i64, end: i64 },
    /// BITPOS bit [start [end]] → :pos / :-1
    Pos { bit: bool, start: i64, end: Option<i64> },
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
    /// ⭐ S2: OFFSET (排序后跳过).
    offset: u32,
    /// ⭐ S2: COUNT(*) — 输出单行计数 (免投影; limit/offset 不影响计数).
    count: bool,
    /// ⭐ F76: 投影输出列名 (与 proj 同序; None = 用 schema 列名, 空 vec = 全 None).
    out_names: Vec<Option<String>>,
}

/// ⭐ S1: 两阶段 DML 的动作 (phase2 每 pk 一发).
#[derive(Clone)]
enum SqlDmlAction {
    Delete,
    Update(Vec<(u16, ColValue)>),
}

/// ⭐ F67/F68 (JOIN): N 表左深 hash join 状态机阶段.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinPhase {
    /// 拉取 tables[idx] 的 schema (单 shard GetSchemaOp).
    FetchSchema(usize),
    /// 广播 gather tables[idx] (ScanFiltered).
    Gather(usize),
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
}

/// ⭐ F67 (JOIN): 单侧 gather 行数上限 (止 worker OOM; 超限报错).
const JOIN_MAX_ROWS: usize = 262_144;

/// ⭐ F70 (JOIN): 键集合下推上限 (超阈退回全表扫; 海量点查劣于全扫).
const JOIN_KEYSET_MAX: usize = 1024;

/// ⭐ 事务 v1 (F61): conn 层事务缓冲 — BEGIN..COMMIT 间写语句截流,
/// shard/调度器零事务状态 (时间维度: 交互式间隙不占 shard;
/// 空间维度: 跨 shard 编排本就在 worker). COMMIT 时按 shard 分组为
/// TxnApply 原子批. 断连/drop 自然丢弃 = 隐式回滚.
struct TxnState {
    /// 保序 write_set (只 append; 同 key 多写按序重放语义正确).
    ops: Vec<BatchOp>,
    /// (db, table, pk) → 最新 op 下标 (RYOW pk 点查).
    index: HashMap<(String, String, Vec<u8>), usize>,
    /// 粗估字节 (上限护栏).
    bytes: usize,
    /// ⭐ v2 (F62): 隔离级别 (Serializable = OCC 读集验证).
    iso: sql::TxnIso,
    /// ⭐ v2 (F62): 只读事务 (写语句拒 25006).
    read_only: bool,
    /// ⭐ v2 (F62): OCC 读集 — 首读指纹为准 (不覆盖); ROLLBACK TO 后
    /// 保留 (保守更严格, 正确性无损).
    read_set: HashMap<(String, String, Vec<u8>), Option<u32>>,
    /// ⭐ v2 (F62): savepoint 栈 (name, ops 水位).
    savepoints: Vec<(String, usize)>,
}

impl TxnState {
    fn new(iso: sql::TxnIso, read_only: bool) -> Self {
        Self {
            ops: Vec::new(),
            index: HashMap::new(),
            bytes: 0,
            iso,
            read_only,
            read_set: HashMap::new(),
            savepoints: Vec::new(),
        }
    }
}

/// 事务缓冲上限 (超限报错并自动回滚 — 巨型事务非 v1 目标).
const TXN_MAX_OPS: usize = 8192;
const TXN_MAX_BYTES: usize = 8 * 1024 * 1024;

/// ⭐ 事务 v1 (F61): COMMIT 的 TxnApply 多 shard 计数聚合.
struct SqlTxnAgg {
    remaining: usize,
    applied: u64,
    error: Option<String>,
}

/// ⭐ F65: 全局 UNIQUE INSERT 编排状态机 (autocommit 单行).
/// 顺序推进: 逐列 Reserve → (committed 冲突时) Verify → 写行 → 逐列 Confirm.
/// 至多一个在途 shard op, 每个 reply 推进一步 (契合 worker 事件驱动).
struct SqlUniqueIns {
    db: std::sync::Arc<str>,
    table: String,
    schema: std::sync::Arc<TableSchema>,
    pk: Vec<u8>,
    values: Vec<ColValue>,
    /// 待处理的全局唯一列: (iid, enc_val).
    guc: Vec<(u32, Vec<u8>)>,
    txn_id: u64,
    phase: UniquePhase,
    /// 当前处理到 guc 的下标 (reserve/confirm 阶段逐个推进).
    idx: usize,
    /// 已成功 reserve/steal 的列数 (回滚时 release guc[0..reserved]).
    reserved: usize,
}

#[derive(PartialEq)]
enum UniquePhase {
    Reserve,
    Verify,
    Write,
    Confirm,
}

/// ⭐ S1: DML 计数聚合 (INSERT 多行 / DELETE/UPDATE phase2 / DROP 广播).
/// 完成 → OK affected=n; DeleteExisted(true) 与 PutOk 各计 1.
struct SqlDmlAgg {
    remaining: usize,
    affected: u64,
    error: Option<String>,
    /// DROP TABLE: 完成时清 worker 缓存 (schemas/routes/created_here),
    /// 且 affected 渲染为 0 (广播 PutOk 不是行数).
    drop_key: Option<(String, String)>,
}

/// SELECT pk 点查: RowGet 结果的过滤/渲染上下文.
struct SqlRowCtx {
    schema: std::sync::Arc<TableSchema>,
    conds: Pred<Cond>,
    /// ⭐ O1: 投影列号.
    proj: Vec<u16>,
    /// ⭐ S2: COUNT(*) — 回单行 0/1.
    count: bool,
    /// ⭐ v2 (F62): OCC 读集记录坐标 (SERIALIZABLE 事务内的 pk 点查).
    read_key: Option<(String, String, Vec<u8>)>,
    /// ⭐ RYOW (F63): 事务内 UPDATE 基于已提交盘行时, 读盘后叠加的 sets.
    ryow_overlay: Vec<(u16, ColValue)>,
    /// ⭐ F76: 投影输出列名 (与 proj 同序; None = 用 schema 列名, 空 vec = 全 None).
    out_names: Vec<Option<String>>,
}

/// schema 缓存 miss 时挂起的语句 (GetSchemaOp 结果到达后续跑).
struct PendingSql {
    stmt: SqlStmt,
    db: std::sync::Arc<str>,
    table: String,
}

/// ⭐ F71 (子查询): 非关联 WHERE 子查询编排 — 顺序跑内层→折叠→重跑外层.
/// inners 按 DFS 左右序; 每个内层跑完 materialize 行集存 results; 全部完→fold→重跑.
struct SubqCtx {
    outer: SqlStmt,
    db: std::sync::Arc<str>,
    inners: Vec<SqlStmt>,
    results: Vec<Vec<Vec<ColValue>>>, // 每内层的行集 (投影后)
    cur: usize,
}

/// ⭐ F71: 子查询内层捕获行上限 (防 OOM; 超限报错). EXISTS 只需存在性 /
/// IN 去重后的精确上限在 fold_one_subq 按叶子类型判定; 此值仅作捕获阶段 OOM 护栏.
const SUBQ_IN_MAX: usize = 65_536;

/// ⭐ F72 (派生表): FROM `(SELECT ...) alias` 编排 — 内层物化完成后的去向.
/// ⭐ F75: Standalone = 单独派生表 (worker 内存执行外层);
/// JoinFrom = 派生表作 JOIN 首表 (物化行预填 tables[0] 后转 JOIN 状态机).
enum DerivedCtx {
    Standalone {
        alias: String,
        items: Vec<sql::SelectItem>,
        conds: Pred<Cond>,
        order: Vec<(String, bool)>,
        limit: Option<u32>,
        offset: Option<u32>,
    },
    JoinFrom {
        db: std::sync::Arc<str>,
        /// 去掉 from_inner 的 SelectJoin (from.table = 别名).
        join_stmt: SqlStmt,
    },
}

/// ⭐ F71: materialize 返回 — (输出列定义, 行集).
type MatResult = Result<(Vec<(String, ColType)>, Vec<Vec<ColValue>>), String>;

/// SELECT 访问路径 (worker 过滤器选择).
enum SqlPlan {
    /// WHERE pk = v → 单 shard 点查.
    PkGet { pk: Vec<u8> },
    /// 索引列命中 → 广播/候选分派 IndexScan (界下推; eq_enc = 等值编码,
    /// Some 时可查 worker 路由缓存做候选剪枝).
    Index {
        iid: u32,
        lo: Option<ColValue>,
        hi: Option<ColValue>,
        limit_push: bool,
        eq_enc: Option<Vec<u8>>,
    },
    /// ⭐ S2: 无可用索引 → 广播全表扫 + 全条件残余过滤.
    FullScan,
}

/// ⭐ W1 (索引路由缓存): SQL worker 级共享缓存 — schema + 索引路由.
///
/// **正确性红线** (双层剪枝, 永不假阴性):
/// - routes 只对 `created_here` 的表存在 (CREATE 时刻零数据, 空 bloom 即完备);
///   重启后旧表 / GetSchemaOp 拉来的表无 entry → 回退广播 (shard 本地 bloom 兜底)
/// - bloom 只增不减 (UPDATE 换值/DELETE 不摘除 → 假阳性多播, 无害);
///   **禁止**换成精确 map + LRU (驱逐后重新累积 = "存在但不完整" → 假阴性漏行)
/// - 单 SQL worker 前提: 所有 INSERT 必经此线程 (放开多 worker 前必须改共享结构)
#[derive(Default)]
struct SqlWorkerCache {
    /// (db, table) → schema (CREATE 或 GetSchemaOp 填充).
    /// ⭐ ORM-B2: per-worker 零锁; 失效靠进程级 DDL epoch (陈旧即整体清空).
    schemas: HashMap<(String, String), std::sync::Arc<TableSchema>>,
    /// 本 worker 已同步到的 DDL epoch (与 SqlSharedRoutes::ddl_epoch 比对).
    local_epoch: u64,
}

type SharedSqlCache = std::rc::Rc<std::cell::RefCell<SqlWorkerCache>>;

/// 路由条目: per-shard 只增 bloom 组 (Arc 克隆锁外读写).
type RouteBlooms = std::sync::Arc<Vec<storage::index_bloom::IndexBloom>>;

/// ⭐ ORM-B2: 进程级共享路由缓存 (跨 worker 跨 SQL 门面单例).
/// bloom 本体原子无锁; RwLock 仅保护 map 结构 (读取克隆 Arc 锁外操作,
/// 写仅 DDL 低频). created_here/routes **必须**进程级 — per-worker 会因
/// INSERT 分散到多 worker 产生假阴性漏行.
pub struct SqlSharedRoutes {
    /// (db, table, iid) → per-shard 只增 bloom (仅 created_here 的表).
    routes: std::sync::RwLock<HashMap<(String, String, u32), RouteBlooms>>,
    /// 本进程内 CREATE 的表 (路由缓存启用条件; 语义从"本 worker"平移到"本进程").
    created_here: std::sync::RwLock<std::collections::HashSet<(String, String)>>,
    /// DDL 世代 (DROP 时 +1; worker 每语句比对, 变化即清 per-worker schema 缓存).
    ddl_epoch: std::sync::atomic::AtomicU64,
    /// 观测: 等值查询被候选剪枝的次数 (fanout < num_shards).
    route_pruned: std::sync::atomic::AtomicU64,
    /// 观测: 等值查询零任务短路的次数 (候选空, 直接回空结果).
    route_bypassed: std::sync::atomic::AtomicU64,
}

impl Default for SqlSharedRoutes {
    fn default() -> Self {
        Self {
            routes: std::sync::RwLock::new(HashMap::new()),
            created_here: std::sync::RwLock::new(std::collections::HashSet::new()),
            ddl_epoch: std::sync::atomic::AtomicU64::new(0),
            route_pruned: std::sync::atomic::AtomicU64::new(0),
            route_bypassed: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

/// 进程级实例构造 (main/测试 每逻辑集群一个, 传给同数据的全部 SQL 门面).
pub fn new_sql_shared() -> std::sync::Arc<SqlSharedRoutes> {
    std::sync::Arc::new(SqlSharedRoutes::default())
}

/// 单个连接状态.
struct ConnState {
    fd: RawFd,
    stream: TcpStream,
    /// ⭐ F83: TLS 会话 (None = 明文; Some = 已 STARTTLS 升级, recv/send 走 rustls).
    tls: Option<Box<rustls::ServerConnection>>,
    read_buf: Vec<u8>,
    proto: ProtocolKind,
    /// RESP: 是否已通过 AUTH (无密码配置时恒 true).
    authenticated: bool,
    /// RESP: 下一条命令分配的 seq (作为 ShardTask.req_id).
    next_seq: u64,
    /// RESP: 下一个应发送的 seq (FIFO 重排游标).
    next_to_send: u64,
    /// RESP: 已就绪但前面还有洞的回复字节.
    pending: BTreeMap<u64, Vec<u8>>,
    /// RESP: DEL 多 key 聚合 (seq → 状态).
    del_agg: HashMap<u64, DelAgg>,
    /// RESP: MGET 聚合 (seq → 状态).
    mget_agg: HashMap<u64, MGetAgg>,
    /// RESP: MSET 聚合 (seq → 状态).
    mset_agg: HashMap<u64, MSetAgg>,
    /// RESP: EXISTS 聚合 (seq → 状态).
    exists_agg: HashMap<u64, ExistsAgg>,
    /// RESP: STRLEN/TYPE 的 Get 语义转换 (seq → kind).
    get_kind: HashMap<u64, GetKind>,
    /// RESP: GETRANGE 的 (start, end) 参数 (seq → 参数; Get 后切片).
    getrange_ctx: HashMap<u64, (i64, i64)>,
    /// RESP: MSETNX 聚合 (seq → 状态).
    msetnx_agg: HashMap<u64, MSetNxAgg>,
    /// RESP: Pairs 结果渲染形态 (HGETALL/HKEYS/HVALS/HSCAN).
    pairs_kind: HashMap<u64, PairsKind>,
    /// RESP: HMSET 的 Integer 结果改回 +OK.
    hmset_ok: std::collections::HashSet<u64>,
    /// RESP: Members 结果渲染形态 (SMEMBERS/SSCAN/SPOP...).
    members_kind: HashMap<u64, MembersKind>,
    /// RESP: SINTER/SUNION/SDIFF 聚合 (seq → 状态).
    setalg_agg: HashMap<u64, SetAlgAgg>,
    /// ⭐ C1: ZMSCORE 的 Values 按裸 bulk 渲染 (score 串已成形, 不走 render tag).
    values_raw: std::collections::HashSet<u64>,
    /// ⭐ C3: *STORE 第二阶段聚合 (seq → 状态).
    store_agg: HashMap<u64, StoreFinishAgg>,
    /// ⭐ C3: ZINTERSTORE/ZUNIONSTORE 源聚合 (seq → 状态).
    zstore_agg: HashMap<u64, ZStoreAgg>,
    /// ⭐ Phase G: Geo 渲染上下文 (seq → 状态).
    geo_ctx: HashMap<u64, GeoCtx>,
    /// ⭐ Phase B: Bitmap 读渲染上下文 (seq → 状态).
    bit_ctx: HashMap<u64, BitCtx>,
    /// ⭐ D3 (分库): 当前连接选中的 db (SELECT n 翻译后的 name; 断连重置).
    current_db: std::sync::Arc<str>,
    /// ⭐ T2 (分表): 表名前缀 → Arc<str> 缓存 (免热路径每 op 一次 String 分配).
    table_cache: HashMap<Vec<u8>, std::sync::Arc<str>>,
    /// ⭐ X3 (SQL): worker 级共享缓存 (schema + 索引路由; 同 worker 全 conn 共享).
    sql_cache: SharedSqlCache,
    /// ⭐ ORM-B2: 进程级共享路由缓存 (跨 worker/门面).
    sql_shared: std::sync::Arc<SqlSharedRoutes>,
    /// ⭐ X3: CREATE TABLE 的 SetSchemaOp 广播聚合 (seq → 状态).
    sql_ddl_agg: HashMap<u64, SqlDdlAgg>,
    /// ⭐ S1: DML 计数聚合 (多行 INSERT / DELETE·UPDATE phase2 / DROP 广播).
    sql_dml_agg: HashMap<u64, SqlDmlAgg>,
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

impl ConnState {
    fn new(
        fd: RawFd,
        proto: ProtocolKind,
        auth_required: bool,
        default_db: std::sync::Arc<str>,
        sql_cache: SharedSqlCache,
        sql_shared: std::sync::Arc<SqlSharedRoutes>,
    ) -> Self {
        let stream = unsafe { TcpStream::from_raw_fd(fd) };
        stream.set_nonblocking(true).ok();
        // ⭐ 关闭 Nagle: 小回复立即发送, 避免与 delayed-ACK 交互导致 40ms 延迟
        stream.set_nodelay(true).ok();
        Self {
            fd,
            stream,
            tls: None,
            read_buf: Vec::with_capacity(4096),
            proto,
            authenticated: !auth_required,
            next_seq: 0,
            next_to_send: 0,
            pending: BTreeMap::new(),
            del_agg: HashMap::new(),
            mget_agg: HashMap::new(),
            mset_agg: HashMap::new(),
            exists_agg: HashMap::new(),
            get_kind: HashMap::new(),
            getrange_ctx: HashMap::new(),
            msetnx_agg: HashMap::new(),
            pairs_kind: HashMap::new(),
            hmset_ok: std::collections::HashSet::new(),
            members_kind: HashMap::new(),
            setalg_agg: HashMap::new(),
            values_raw: std::collections::HashSet::new(),
            store_agg: HashMap::new(),
            zstore_agg: HashMap::new(),
            geo_ctx: HashMap::new(),
            bit_ctx: HashMap::new(),
            current_db: default_db,
            table_cache: HashMap::new(),
            sql_cache,
            sql_shared,
            sql_ddl_agg: HashMap::new(),
            sql_dml_agg: HashMap::new(),
            txn: None,
            txn_failed: false,
            sql_txn_agg: HashMap::new(),
            sql_unique_ins: HashMap::new(),
            sql_sysq: HashMap::new(),
            sql_join: HashMap::new(),
            sql_subq: HashMap::new(),
            sql_derived: HashMap::new(),
            default_iso: sql::TxnIso::default(),
            default_ro: false,
            sql_select_agg: HashMap::new(),
            sql_row_ctx: HashMap::new(),
            sql_pending: HashMap::new(),
            mysql: None,
            pg_phase: 0,
            pg_scram: None,
            http_ctx: HashMap::new(),
            mysql_stmts: HashMap::new(),
            next_stmt_id: 1,
            mysql_binary: std::collections::HashSet::new(),
            pg_stmts: HashMap::new(),
            pg_batch: PgBatch::default(),
            pg_ext: HashMap::new(),
            close_after_flush: false,
        }
    }

    /// 从连接 recv 数据, 追加到 read_buf.
    /// 返回 Ok(true) = 有数据, Ok(false) = 连接关闭, Err = 错误.
    fn recv(&mut self) -> std::io::Result<bool> {
        // ⭐ F83: TLS 路径 — 读密文喂 rustls → 冲刷握手待写 → 读明文入 read_buf.
        if let Some(tls) = self.tls.as_mut() {
            let mut eof = false;
            loop {
                match tls.read_tls(&mut self.stream) {
                    Ok(0) => {
                        eof = true;
                        break;
                    }
                    Ok(_) => {
                        if let Err(e) = tls.process_new_packets() {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e));
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            }
            // 冲刷握手/告警等待写字节 (spin, 同明文 send 语义)
            while tls.wants_write() {
                match tls.write_tls(&mut self.stream) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::yield_now();
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                }
            }
            // 读明文
            let before = self.read_buf.len();
            let mut tmp = [0u8; 4096];
            loop {
                match tls.reader().read(&mut tmp) {
                    Ok(0) => break,
                    Ok(n) => self.read_buf.extend_from_slice(&tmp[..n]),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => break,
                }
            }
            let got_plain = self.read_buf.len() > before;
            // EOF 且本轮无新明文 → 连接关闭
            return Ok(!(eof && !got_plain));
        }
        let mut tmp = [0u8; 4096];
        loop {
            match self.stream.read(&mut tmp) {
                Ok(0) => return Ok(false), // EOF
                Ok(n) => {
                    self.read_buf.extend_from_slice(&tmp[..n]);
                    if n < tmp.len() {
                        return Ok(true); // 读完了本次可用数据
                    }
                    // 可能还有更多, 继续 read
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(true),
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// ⭐ F83: 就地把明文连接升级为 TLS (STARTTLS). 握手在后续 recv 泵中完成.
    fn start_tls(&mut self, config: std::sync::Arc<rustls::ServerConfig>) -> bool {
        match rustls::ServerConnection::new(config) {
            Ok(c) => {
                self.tls = Some(Box::new(c));
                true
            }
            Err(_) => false,
        }
    }

    /// 发送原始字节. non-blocking socket 遇 WouldBlock 时 spin retry
    /// (回复帧小, 正常情况下 send buffer 不会满太久).
    fn send_bytes(&mut self, bytes: &[u8]) {
        // ⭐ F83: TLS 路径 — 明文写入 rustls writer, 再泵密文到 socket.
        if let Some(tls) = self.tls.as_mut() {
            if tls.writer().write_all(bytes).is_err() {
                return;
            }
            while tls.wants_write() {
                match tls.write_tls(&mut self.stream) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::yield_now();
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                }
            }
            return;
        }
        let mut written = 0usize;
        while written < bytes.len() {
            match self.stream.write(&bytes[written..]) {
                Ok(0) => break, // 对端关闭
                Ok(n) => written += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::yield_now();
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
    }

    /// Binary: 直发回复 (req_id 乱序语义, 无重排).
    fn send_binary_response(&mut self, req_id: u64, resp: &Response) {
        let bytes = BinaryProtocol::new().encode_response(req_id, resp);
        self.send_bytes(&bytes);
    }

    /// RESP: 回复字节进重排缓冲, 然后把从 next_to_send 起的连续段发出.
    fn resp_complete(&mut self, seq: u64, bytes: Vec<u8>) {
        // ⭐ P3: PG 扩展查询批次 — 响应前拼 [ParseComplete][BindComplete]... 前缀
        // (单点侵入; 非 Pg conn 恒空查零开销)
        let mut bytes = match self.pg_ext.remove(&seq) {
            Some(mut prefix) => {
                prefix.extend_from_slice(&bytes);
                prefix
            }
            None => bytes,
        };
        // ⭐ 事务 v1 (F61): 协议级事务状态单点注入 (免渲染函数签名扩散)
        match self.proto {
            ProtocolKind::Pg => {
                // 事务内遇 ErrorResponse → 置 failed (后续语句 25P02 拦截)
                if self.txn.is_some() && !self.txn_failed && pg_frames_contain_error(&bytes) {
                    self.txn_failed = true;
                }
                // 尾部 ReadyForQuery 状态字节: I idle / T in-txn / E failed
                let n = bytes.len();
                if n >= 6 && bytes[n - 6] == b'Z' && bytes[n - 5..n - 1] == [0, 0, 0, 5] {
                    bytes[n - 1] = if self.txn_failed {
                        b'E'
                    } else if self.txn.is_some() {
                        b'T'
                    } else {
                        b'I'
                    };
                }
            }
            ProtocolKind::Sql if self.txn.is_some() => {
                // 纯 OK 包 (单包且 payload 首字节 0x00) → status |= IN_TRANS
                let n = bytes.len();
                if n >= 11
                    && bytes[4] == 0x00
                    && u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]) as usize + 4 == n
                {
                    bytes[n - 4] |= 0x01; // SERVER_STATUS_IN_TRANS
                }
            }
            _ => {}
        }
        self.pending.insert(seq, bytes);
        self.resp_flush_ready();
    }

    fn resp_flush_ready(&mut self) {
        let mut out: Vec<u8> = Vec::new();
        while let Some(bytes) = self.pending.remove(&self.next_to_send) {
            out.extend_from_slice(&bytes);
            self.next_to_send += 1;
        }
        if !out.is_empty() {
            self.send_bytes(&out);
        }
    }

    /// RESP: 是否可以关闭 (QUIT/协议错误 且回复已全部发出).
    fn resp_should_close(&self) -> bool {
        self.close_after_flush && self.pending.is_empty() && self.next_seq == self.next_to_send
    }

    /// ⭐ T2 (分表): 表名前缀 → Arc<str> (缓存复用, 免热路径 String 分配).
    fn table_arc(&mut self, prefix: &[u8]) -> std::sync::Arc<str> {
        if let Some(t) = self.table_cache.get(prefix) {
            return t.clone();
        }
        let t: std::sync::Arc<str> =
            std::sync::Arc::from(std::str::from_utf8(prefix).expect("前缀已校验 ASCII"));
        self.table_cache.insert(prefix.to_vec(), t.clone());
        t
    }

    /// ⭐ T2 (分表): 就地解析 "table:key" — 命中合法前缀则剥离前缀并返回表;
    /// 否则 None (整个 key 落 default 表).
    fn resolve_table(&mut self, key: &mut Vec<u8>) -> Option<std::sync::Arc<str>> {
        let pos = split_table_key(key)?;
        let tbl = self.table_arc(&key[..pos]);
        key.drain(..=pos);
        Some(tbl)
    }
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
                    let mut state = ConnState::new(new_conn.fd, proto_kind, auth_required, db.clone(), sql_cache.clone(), sql_shared.clone());
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
                    let mut state = ConnState::new(new_conn.fd, proto_kind, auth_required, db.clone(), sql_cache.clone(), sql_shared.clone());
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

/// 分发单条 RESP 命令: 本地命令直接回 (占 seq 进重排缓冲), KV 命令进 shard.
#[allow(clippy::too_many_arguments)]
fn dispatch_resp_command(
    conn: &mut ConnState,
    conn_id: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    table: &std::sync::Arc<str>,
    limits: &KvLimits,
    auth_password: &Option<String>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    cmd: RespCommand,
) {
    // ⭐ H4: 命令计数 (relaxed 单次原子加, 热路径零锁)
    crate::metrics::KV_OPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let codec = RespCodec::new();
    let seq = conn.next_seq;
    conn.next_seq += 1;

    // AUTH 门禁: 未认证时只放行 AUTH/HELLO/QUIT
    if !conn.authenticated
        && !matches!(
            cmd,
            RespCommand::Auth { .. } | RespCommand::Hello(_) | RespCommand::Quit
        )
    {
        conn.resp_complete(seq, codec.encode_error("NOAUTH Authentication required."));
        return;
    }

    match cmd {
        RespCommand::Set { key, value } => {
            // ⭐ value 已是 [TAG_RAW][payload] 布局 (decode 时预置),
            // 校验扣 1B tag; 直构 BatchOp 免 Request 中转/二次拷贝.
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::Put {
                db: db.clone(),
                table: table.clone(),
                key,
                val: value,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::Get { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::Get {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::Del { keys } => {
            // 逐 key 校验 (借用版, 免 clone); 任一超限整条命令拒绝 (不部分执行)
            for key in &keys {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // 多 key 拆多个 Delete task 共用同一 seq, 聚合计数后回 :N
            conn.del_agg.insert(
                seq,
                DelAgg {
                    remaining: keys.len(),
                    count: 0,
                },
            );
            for key in keys {
                let op = BatchOp::Delete {
                    db: db.clone(),
                    table: table.clone(),
                    key,
                };
                push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
            }
        }
        RespCommand::MGet { keys } => {
            for key in &keys {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // ⭐ 按 (shard, 表) 分组: 每组一个 MultiGet (shard 内区间复用),
            // group 号回传后按索引表回填原始槽. ⭐ T2: 每 key 独立冒号选表.
            let n = keys.len();
            type MGroup = ((usize, std::sync::Arc<str>), Vec<Vec<u8>>, Vec<usize>);
            let mut by_shard: Vec<MGroup> = Vec::new();
            for (i, mut key) in keys.into_iter().enumerate() {
                let tbl = conn.resolve_table(&mut key).unwrap_or_else(|| table.clone());
                let sid = hash_route_key(db.as_ref(), tbl.as_ref(), &key, num_shards);
                match by_shard
                    .iter_mut()
                    .find(|(g, _, _)| g.0 == sid && g.1 == tbl)
                {
                    Some((_, ks, idxs)) => {
                        ks.push(key);
                        idxs.push(i);
                    }
                    None => by_shard.push(((sid, tbl), vec![key], vec![i])),
                }
            }
            let groups: Vec<Vec<usize>> = by_shard.iter().map(|(_, _, idxs)| idxs.clone()).collect();
            conn.mget_agg.insert(
                seq,
                MGetAgg {
                    remaining: by_shard.len(),
                    slots: vec![None; n],
                    groups,
                    error: None,
                },
            );
            for (gidx, ((sid, tbl), ks, _)) in by_shard.into_iter().enumerate() {
                let op = BatchOp::MultiGet {
                    db: db.clone(),
                    table: tbl,
                    keys: ks,
                };
                push_task_grouped(conn_id, seq, worker_id, gidx as u32, sid, op, shard_inboxes);
            }
        }
        RespCommand::MSet { pairs } => {
            // value 已带 1B tag, 校验扣除
            for (key, value) in &pairs {
                if let Err(msg) = validate_kv(key, value.len().saturating_sub(1), limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // ⭐ T2: 每 key 独立冒号选表 → 按 (shard, 表) 分组
            type ShardPairs = ((usize, std::sync::Arc<str>), Vec<(Vec<u8>, Vec<u8>)>);
            let mut by_shard: Vec<ShardPairs> = Vec::new();
            for (mut key, value) in pairs {
                let tbl = conn.resolve_table(&mut key).unwrap_or_else(|| table.clone());
                let sid = hash_route_key(db.as_ref(), tbl.as_ref(), &key, num_shards);
                match by_shard.iter_mut().find(|(g, _)| g.0 == sid && g.1 == tbl) {
                    Some((_, ps)) => ps.push((key, value)),
                    None => by_shard.push(((sid, tbl), vec![(key, value)])),
                }
            }
            conn.mset_agg.insert(
                seq,
                MSetAgg {
                    remaining: by_shard.len(),
                    error: None,
                },
            );
            for (gidx, ((sid, tbl), ps)) in by_shard.into_iter().enumerate() {
                let op = BatchOp::MultiPut {
                    db: db.clone(),
                    table: tbl,
                    pairs: ps,
                };
                push_task_grouped(conn_id, seq, worker_id, gidx as u32, sid, op, shard_inboxes);
            }
        }
        RespCommand::Ping(msg) => {
            let bytes = match msg {
                None => codec.encode_simple("PONG"),
                Some(m) => codec.encode_bulk(&m),
            };
            conn.resp_complete(seq, bytes);
        }
        RespCommand::Incr { key, delta } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::Incr {
                db: db.clone(),
                table: table.clone(),
                key,
                delta,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::IncrFloat { key, delta } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::IncrFloat {
                db: db.clone(),
                table: table.clone(),
                key,
                delta,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::Append { key, suffix } => {
            // suffix 不带 tag (RMW 端拼接); 校验按追加段长度上限保守拦截
            if let Err(msg) = validate_kv(&key, suffix.len(), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::Append {
                db: db.clone(),
                table: table.clone(),
                key,
                suffix,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SetNx { key, value } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SetNx {
                db: db.clone(),
                table: table.clone(),
                key,
                val: value,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::Exists { keys } => {
            for key in &keys {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // N 个 Get 共用 seq, 聚合计数 (Redis EXISTS: 重复 key 重复计)
            conn.exists_agg.insert(
                seq,
                ExistsAgg {
                    remaining: keys.len(),
                    count: 0,
                },
            );
            for key in keys {
                let op = BatchOp::Get {
                    db: db.clone(),
                    table: table.clone(),
                    key,
                };
                push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
            }
        }
        RespCommand::Strlen { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.get_kind.insert(seq, GetKind::Strlen);
            let op = BatchOp::Get {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::TypeOf { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.get_kind.insert(seq, GetKind::TypeOf);
            let op = BatchOp::Get {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GetDel { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::GetDel {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GetSet { key, value } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::GetSet {
                db: db.clone(),
                table: table.clone(),
                key,
                val: value,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SetRange { key, offset, data } => {
            // 新长度 = offset + data.len(), 保守校验不超 value 上限
            if let Err(msg) = validate_kv(&key, offset as usize + data.len(), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SetRange {
                db: db.clone(),
                table: table.clone(),
                key,
                offset,
                data,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GetRange { key, start, end } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // 复用 Get; 结果到达时按 (start,end) 切片 (getrange_ctx)
            conn.getrange_ctx.insert(seq, (start, end));
            let op = BatchOp::Get {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::MSetNx { pairs } => {
            for (key, value) in &pairs {
                if let Err(msg) = validate_kv(key, value.len().saturating_sub(1), limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // 按 (shard, 表) 分组, 每组一个 MultiPutNx; 全部写入 → :1, 否则 :0
            // ⭐ T2: 每 key 独立冒号选表
            type NxPairs = ((usize, std::sync::Arc<str>), Vec<(Vec<u8>, Vec<u8>)>);
            let mut by_shard: Vec<NxPairs> = Vec::new();
            for (mut key, value) in pairs {
                let tbl = conn.resolve_table(&mut key).unwrap_or_else(|| table.clone());
                let sid = hash_route_key(db.as_ref(), tbl.as_ref(), &key, num_shards);
                match by_shard.iter_mut().find(|(g, _)| g.0 == sid && g.1 == tbl) {
                    Some((_, ps)) => ps.push((key, value)),
                    None => by_shard.push(((sid, tbl), vec![(key, value)])),
                }
            }
            conn.msetnx_agg.insert(
                seq,
                MSetNxAgg {
                    remaining: by_shard.len(),
                    all_set: true,
                },
            );
            for (gidx, ((sid, tbl), ps)) in by_shard.into_iter().enumerate() {
                let op = BatchOp::MultiPutNx {
                    db: db.clone(),
                    table: tbl,
                    pairs: ps,
                };
                push_task_grouped(conn_id, seq, worker_id, gidx as u32, sid, op, shard_inboxes);
            }
        }
        // ---- ⭐ Phase H: Hash (单 key 单 shard, 直推 push_task) ----
        RespCommand::HSet { key, pairs, reply_ok } => {
            for (f, v) in &pairs {
                if let Err(msg) = validate_kv(&key, 0, limits)
                    .and_then(|_| validate_kv(f, v.len().saturating_sub(1), limits))
                {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            if reply_ok {
                conn.hmset_ok.insert(seq); // HMSET 回 +OK (Integer 转换)
            }
            let op = BatchOp::HSet { db: db.clone(), table: table.clone(), key, pairs };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HSetNx { key, field, value } => {
            if let Err(msg) = validate_kv(&key, 0, limits)
                .and_then(|_| validate_kv(&field, value.len().saturating_sub(1), limits))
            {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::HSetNx {
                db: db.clone(),
                table: table.clone(),
                key,
                field,
                val: value,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HGet { key, field } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::HGet { db: db.clone(), table: table.clone(), key, field };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HMGet { key, fields } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::HMGet { db: db.clone(), table: table.clone(), key, fields };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HDel { key, fields } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::HDel { db: db.clone(), table: table.clone(), key, fields };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HExists { key, field } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.get_kind.insert(seq, GetKind::HExists);
            let op = BatchOp::HGet { db: db.clone(), table: table.clone(), key, field };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HLen { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::HLen { db: db.clone(), table: table.clone(), key };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HGetAll { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.pairs_kind.insert(seq, PairsKind::All);
            let op = BatchOp::HGetAll { db: db.clone(), table: table.clone(), key };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HKeys { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.pairs_kind.insert(seq, PairsKind::Keys);
            let op = BatchOp::HGetAll { db: db.clone(), table: table.clone(), key };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HVals { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.pairs_kind.insert(seq, PairsKind::Vals);
            let op = BatchOp::HGetAll { db: db.clone(), table: table.clone(), key };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HScan { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.pairs_kind.insert(seq, PairsKind::Scan);
            let op = BatchOp::HGetAll { db: db.clone(), table: table.clone(), key };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HIncrBy { key, field, delta } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::HIncrBy {
                db: db.clone(),
                table: table.clone(),
                key,
                field,
                delta,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HIncrByFloat { key, field, delta } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::HIncrByFloat {
                db: db.clone(),
                table: table.clone(),
                key,
                field,
                delta,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ Phase Set: Set (单 key 直推; 代数类跨 shard 聚合) ----
        RespCommand::SAdd { key, members } => {
            for m in &members {
                if let Err(msg) =
                    validate_kv(&key, 0, limits).and_then(|_| validate_kv(m, 0, limits))
                {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            let op = BatchOp::SAdd { db: db.clone(), table: table.clone(), key, members };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SRem { key, members } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SRem { db: db.clone(), table: table.clone(), key, members };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SIsMember { key, member } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SIsMember { db: db.clone(), table: table.clone(), key, member };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SCard { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SCard { db: db.clone(), table: table.clone(), key };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SMembers { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::List);
            let op = BatchOp::SMembers { db: db.clone(), table: table.clone(), key };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SScan { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::Scan);
            let op = BatchOp::SMembers { db: db.clone(), table: table.clone(), key };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SPop { key, count } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // count 缺省 → 单 bulk (One); 显式 count → 数组 (List)
            match count {
                None => {
                    conn.members_kind.insert(seq, MembersKind::One);
                    let op = BatchOp::SPop { db: db.clone(), table: table.clone(), key };
                    push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
                Some(c) => {
                    conn.members_kind.insert(seq, MembersKind::List);
                    let op = BatchOp::SPopN { db: db.clone(), table: table.clone(), key, count: c };
                    push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
            }
        }
        RespCommand::SRandMember { key, count } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            match count {
                None => {
                    conn.members_kind.insert(seq, MembersKind::One);
                    let op = BatchOp::SRandMember { db: db.clone(), table: table.clone(), key };
                    push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
                Some(c) => {
                    conn.members_kind.insert(seq, MembersKind::List);
                    let op = BatchOp::SRandCount { db: db.clone(), table: table.clone(), key, count: c };
                    push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
            }
        }
        RespCommand::SMisMember { key, members } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SMisMember { db: db.clone(), table: table.clone(), key, members };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SInterCard { keys, limit } => {
            for key in &keys {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // 复用 SetAlg 聚合 (Inter), 完成点回 :card 而非数组
            let n = keys.len();
            conn.setalg_agg.insert(
                seq,
                SetAlgAgg {
                    remaining: n,
                    op: SetAlgOp::Inter,
                    sets: vec![None; n],
                    error: None,
                    card_only: true,
                    limit,
                    store_dst: None,
                    db: db.clone(),
                    table: table.clone(),
                },
            );
            for (i, mut key) in keys.into_iter().enumerate() {
                // ⭐ T2: 源 key 逐个冒号选表 (天然支持跨表代数)
                let tbl = conn.resolve_table(&mut key).unwrap_or_else(|| table.clone());
                let sid = hash_route_key(db.as_ref(), tbl.as_ref(), &key, num_shards);
                let smem = BatchOp::SMembers { db: db.clone(), table: tbl, key };
                push_task_grouped(conn_id, seq, worker_id, i as u32, sid, smem, shard_inboxes);
            }
        }
        RespCommand::SetAlg { op, keys } => {
            for key in &keys {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // 每 key 一个 SMembers (group = key 序号), 全部回齐后求交/并/差
            let n = keys.len();
            conn.setalg_agg.insert(
                seq,
                SetAlgAgg {
                    remaining: n,
                    op,
                    sets: vec![None; n],
                    error: None,
                    card_only: false,
                    limit: 0,
                    store_dst: None,
                    db: db.clone(),
                    table: table.clone(),
                },
            );
            for (i, mut key) in keys.into_iter().enumerate() {
                // ⭐ T2: 源 key 逐个冒号选表 (天然支持跨表代数)
                let tbl = conn.resolve_table(&mut key).unwrap_or_else(|| table.clone());
                let sid = hash_route_key(db.as_ref(), tbl.as_ref(), &key, num_shards);
                let smem = BatchOp::SMembers { db: db.clone(), table: tbl, key };
                push_task_grouped(conn_id, seq, worker_id, i as u32, sid, smem, shard_inboxes);
            }
        }
        // ---- ⭐ C3: *STORE (源读聚合 + dst 写; 跨 shard 非原子, 记 gap) ----
        RespCommand::SetAlgStore { op, dst, keys } => {
            for key in keys.iter().chain(std::iter::once(&dst)) {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            let n = keys.len();
            // ⭐ T2: dst 冒号选表 (二阶段任务写入 dst 的表)
            let mut dst = dst;
            let dst_tbl = conn.resolve_table(&mut dst).unwrap_or_else(|| table.clone());
            conn.setalg_agg.insert(
                seq,
                SetAlgAgg {
                    remaining: n,
                    op,
                    sets: vec![None; n],
                    error: None,
                    card_only: false,
                    limit: 0,
                    store_dst: Some(dst),
                    db: db.clone(),
                    table: dst_tbl,
                },
            );
            for (i, mut key) in keys.into_iter().enumerate() {
                // ⭐ T2: 源 key 逐个冒号选表 (天然支持跨表代数)
                let tbl = conn.resolve_table(&mut key).unwrap_or_else(|| table.clone());
                let sid = hash_route_key(db.as_ref(), tbl.as_ref(), &key, num_shards);
                let smem = BatchOp::SMembers { db: db.clone(), table: tbl, key };
                push_task_grouped(conn_id, seq, worker_id, i as u32, sid, smem, shard_inboxes);
            }
        }
        RespCommand::ZSetStore { inter, dst, keys } => {
            for key in keys.iter().chain(std::iter::once(&dst)) {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            let n = keys.len();
            // ⭐ T2: dst 冒号选表 (二阶段任务写入 dst 的表)
            let mut dst = dst;
            let dst_tbl = conn.resolve_table(&mut dst).unwrap_or_else(|| table.clone());
            conn.zstore_agg.insert(
                seq,
                ZStoreAgg {
                    remaining: n,
                    inter,
                    sets: vec![None; n],
                    error: None,
                    dst,
                    db: db.clone(),
                    table: dst_tbl,
                },
            );
            // 每源 key 取全量 (member, score) — 复用 ZRange withscores 交替串
            for (i, mut key) in keys.into_iter().enumerate() {
                // ⭐ T2: 源 key 逐个冒号选表
                let tbl = conn.resolve_table(&mut key).unwrap_or_else(|| table.clone());
                let sid = hash_route_key(db.as_ref(), tbl.as_ref(), &key, num_shards);
                let zr = BatchOp::ZRange {
                    db: db.clone(),
                    table: tbl,
                    key,
                    start: 0,
                    end: -1,
                    rev: false,
                    withscores: true,
                };
                push_task_grouped(conn_id, seq, worker_id, i as u32, sid, zr, shard_inboxes);
            }
        }
        // ---- ⭐ Phase L: List (单 key 直推) ----
        RespCommand::LPush { key, values, left } => {
            for v in &values {
                if let Err(msg) =
                    validate_kv(&key, v.len().saturating_sub(1), limits)
                {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            let op = BatchOp::LPush { db: db.clone(), table: table.clone(), key, values, left };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LPop { key, left, count } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // count 缺省 → 单 bulk (One); 显式 count → 数组 (List)
            conn.members_kind.insert(
                seq,
                if count.is_none() { MembersKind::One } else { MembersKind::List },
            );
            let op = BatchOp::LPop {
                db: db.clone(),
                table: table.clone(),
                key,
                left,
                count: count.unwrap_or(1),
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LLen { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::LLen { db: db.clone(), table: table.clone(), key };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LRange { key, start, end } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::List);
            let op = BatchOp::LRange { db: db.clone(), table: table.clone(), key, start, end };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LIndex { key, idx } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::LIndex { db: db.clone(), table: table.clone(), key, idx };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LSet { key, idx, value } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.hmset_ok.insert(seq); // Integer(1) → +OK
            let op = BatchOp::LSet { db: db.clone(), table: table.clone(), key, idx, val: value };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ C2: List 中段操作 ----
        RespCommand::LRem { key, count, value } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::LRem { db: db.clone(), table: table.clone(), key, count, val: value };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LTrim { key, start, stop } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.hmset_ok.insert(seq); // Integer(1) → +OK
            let op = BatchOp::LTrim { db: db.clone(), table: table.clone(), key, start, stop };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LPos { key, value, rank, count } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::LPos { db: db.clone(), table: table.clone(), key, val: value, rank, count };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LInsert { key, before, pivot, value } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::LInsert {
                db: db.clone(),
                table: table.clone(),
                key,
                before,
                pivot,
                val: value,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ Phase Z: ZSet (单 key 直推) ----
        RespCommand::ZAdd { key, pairs } => {
            for (_, m) in &pairs {
                if let Err(msg) = validate_kv(&key, 0, limits).and_then(|_| validate_kv(m, 0, limits)) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            let op = BatchOp::ZAdd { db: db.clone(), table: table.clone(), key, pairs };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZRem { key, members } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZRem { db: db.clone(), table: table.clone(), key, members };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZScore { key, member } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZScore { db: db.clone(), table: table.clone(), key, member };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZCard { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZCard { db: db.clone(), table: table.clone(), key };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZIncrBy { key, delta, member } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZIncrBy { db: db.clone(), table: table.clone(), key, delta, member };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZRange { key, start, end, rev, withscores } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::List);
            let op = BatchOp::ZRange { db: db.clone(), table: table.clone(), key, start, end, rev, withscores };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZRangeByScore { key, min, max, withscores } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::List);
            let op = BatchOp::ZRangeByScore { db: db.clone(), table: table.clone(), key, min, max, withscores };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZRank { key, member, rev } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZRank { db: db.clone(), table: table.clone(), key, member, rev };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ C1: ZSet/Hash 命令空洞 ----
        RespCommand::ZCount { key, min, max } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZCount { db: db.clone(), table: table.clone(), key, min, max };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZMScore { key, members } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // Values 已是成形 score 串, 按裸 bulk 渲染 (不走 render tag)
            conn.values_raw.insert(seq);
            let op = BatchOp::ZMScore { db: db.clone(), table: table.clone(), key, members };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZPop { key, rev, count } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::List);
            let op = BatchOp::ZPop { db: db.clone(), table: table.clone(), key, rev, count };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HStrlen { key, field } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // 复用 HGet + Strlen 语义转换 (miss → :0)
            conn.get_kind.insert(seq, GetKind::Strlen);
            let op = BatchOp::HGet { db: db.clone(), table: table.clone(), key, field };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HRandField { key, count, withvalues } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let kind = match (count, withvalues) {
                (None, _) => PairsKind::OneKey,
                (Some(_), true) => PairsKind::All,
                (Some(_), false) => PairsKind::Keys,
            };
            conn.pairs_kind.insert(seq, kind);
            let op = BatchOp::HRandField {
                db: db.clone(),
                table: table.clone(),
                key,
                count: count.unwrap_or(1),
                withvalues,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ Phase G: Geo (复用 ZSet 链路 + 渲染钩子) ----
        RespCommand::GeoPos { key, members } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.geo_ctx.insert(seq, GeoCtx::Pos);
            let op = BatchOp::ZMScore { db: db.clone(), table: table.clone(), key, members };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GeoDist { key, m1, m2, factor } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.geo_ctx.insert(seq, GeoCtx::Dist { factor });
            let op = BatchOp::ZMScore {
                db: db.clone(),
                table: table.clone(),
                key,
                members: vec![m1, m2],
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GeoSearch { key, lon, lat, radius_m, asc, count, withcoord, withdist } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.geo_ctx.insert(
                seq,
                GeoCtx::Search { lon, lat, radius_m, asc, count, withcoord, withdist },
            );
            // 全量 (member, score) — worker 端 geohash 解码 + 距离过滤
            let op = BatchOp::ZRange {
                db: db.clone(),
                table: table.clone(),
                key,
                start: 0,
                end: -1,
                rev: false,
                withscores: true,
            };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ Phase B: Bitmap (String 字节) ----
        RespCommand::SetBit { key, offset, bit } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // 位偏移上限: 落地字节 ≤ max_value_bytes (溢出页上限内)
            if (offset / 8) as usize + 1 > limits.max_value_bytes {
                conn.resp_complete(
                    seq,
                    codec.encode_error("bit offset is not an integer or out of range"),
                );
                return;
            }
            let op = BatchOp::SetBit { db: db.clone(), table: table.clone(), key, offset, bit };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GetBit { key, offset } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.bit_ctx.insert(seq, BitCtx::GetBit { offset });
            let op = BatchOp::Get { db: db.clone(), table: table.clone(), key };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::BitCount { key, start, end } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.bit_ctx.insert(seq, BitCtx::Count { start, end });
            let op = BatchOp::Get { db: db.clone(), table: table.clone(), key };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::BitPos { key, bit, start, end } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.bit_ctx.insert(seq, BitCtx::Pos { bit, start, end });
            let op = BatchOp::Get { db: db.clone(), table: table.clone(), key };
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::InvalidInt(_) => {
            conn.resp_complete(
                seq,
                codec.encode_error("value is not an integer or out of range"),
            );
        }
        RespCommand::InvalidFloat(_) => {
            conn.resp_complete(seq, codec.encode_error("value is not a valid float"));
        }
        RespCommand::Echo(m) => {
            conn.resp_complete(seq, codec.encode_bulk(&m));
        }
        RespCommand::Auth { user, pass } => {
            let bytes = match auth_password {
                None => codec.encode_error("ERR Client sent AUTH, but no password is set."),
                Some(expected) => {
                    let user_ok = match &user {
                        None => true,
                        Some(u) => u.as_slice() == b"default",
                    };
                    if user_ok && pass.as_slice() == expected.as_bytes() {
                        conn.authenticated = true;
                        codec.encode_ok()
                    } else {
                        codec.encode_error(
                            "WRONGPASS invalid username-password pair or user is disabled.",
                        )
                    }
                }
            };
            conn.resp_complete(seq, bytes);
        }
        RespCommand::Quit => {
            conn.resp_complete(seq, codec.encode_ok());
            conn.close_after_flush = true;
        }
        RespCommand::Command => {
            conn.resp_complete(seq, codec.encode_empty_array());
        }
        RespCommand::Hello(proto) => {
            let is_v2 = match &proto {
                None => true,
                Some(p) => p.as_slice() == b"2",
            };
            let bytes = if is_v2 {
                // 最小 HELLO 回复: 扁平 key-value 数组 (RESP2 无 map 类型)
                let mut out = Vec::new();
                out.extend_from_slice(b"*6\r\n");
                out.extend_from_slice(&codec.encode_bulk(b"server"));
                out.extend_from_slice(&codec.encode_bulk(b"nexusdb"));
                out.extend_from_slice(&codec.encode_bulk(b"version"));
                out.extend_from_slice(&codec.encode_bulk(b"0.1.0"));
                out.extend_from_slice(&codec.encode_bulk(b"proto"));
                out.extend_from_slice(&codec.encode_integer(2));
                out
            } else {
                codec.encode_error(
                    "NOPROTO unsupported protocol version",
                )
            };
            conn.resp_complete(seq, bytes);
        }
        RespCommand::Select { idx } => {
            // ⭐ D3 (分库): idx 经 DbDirView 翻译为 db name, per-connection 生效.
            // (分表维度走 key 冒号前缀, 与 SELECT 正交)
            let bytes = match u32::try_from(idx).ok().and_then(|id| db_view.name_of(id)) {
                Some(name) => {
                    conn.current_db = name;
                    codec.encode_ok()
                }
                None => codec.encode_error("DB index is out of range"),
            };
            conn.resp_complete(seq, bytes);
        }
        RespCommand::Unknown(name) => {
            conn.resp_complete(seq, codec.encode_error(&format!("unknown command '{name}'")));
        }
        RespCommand::WrongArity(name) => {
            conn.resp_complete(
                seq,
                codec.encode_error(&format!("wrong number of arguments for '{name}' command")),
            );
        }
    }
}

fn push_task(
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
fn push_task_grouped(
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
fn hash_route_key(db: &str, table: &str, key: &[u8], num_shards: usize) -> usize {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    db.hash(&mut h);
    table.hash(&mut h);
    key.hash(&mut h);
    (h.finish() as usize) % num_shards
}

/// ⭐ GETRANGE 切片 (Redis 语义): 负索引从尾算, end inclusive, 越界 clamp.
fn getrange_slice(data: &[u8], start: i64, end: i64) -> &[u8] {
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

/// RESP: 处理 shard 回来的单条结果 (含 DEL/MGET/MSET 聚合).
/// ⭐ C3: *STORE 二阶段任务的 (db, table) 存于 agg (发起时快照), 无需入参.
#[allow(clippy::too_many_arguments)]
fn handle_resp_shard_result(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    group: u32,
    result: &BatchResult,
    worker_id: u32,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    let codec = RespCodec::new();
    // ⭐ H2: HTTP KV 回包渲染 (seq 簿记, 与 SQL 钩子互斥)
    if let Some(ctx) = conn.http_ctx.remove(&seq) {
        use crate::protocol::http as h;
        use shard_manager::value_num as vn;
        let cors = crate::http_config::cors_origin();
        let bytes = match (ctx.op, result) {
            (HttpKvOp::Get, BatchResult::GetValue(Some(stored))) => {
                let (tag, payload) = crate::value_codec::decode_value(stored);
                let val = match tag {
                    vn::TAG_I64 if payload.len() == 8 => {
                        serde_json::json!(i64::from_le_bytes(payload.try_into().unwrap()))
                    }
                    vn::TAG_F64 if payload.len() == 8 => {
                        serde_json::json!(f64::from_le_bytes(payload.try_into().unwrap()))
                    }
                    _ => match std::str::from_utf8(payload) {
                        Ok(s) => serde_json::json!(s),
                        Err(_) => serde_json::json!({
                            "b64": h::base64_encode(payload),
                            "encoding": "base64",
                        }),
                    },
                };
                let body = serde_json::to_vec(&serde_json::json!({ "value": val }))
                    .unwrap_or_default();
                h::build_response(200, &body, cors, ctx.keep_alive)
            }
            (HttpKvOp::Get, BatchResult::GetValue(None)) => {
                h::build_response(404, &h::error_body("not found"), cors, ctx.keep_alive)
            }
            (HttpKvOp::Put, BatchResult::PutOk) => {
                h::build_response(200, br#"{"ok":true}"#, cors, ctx.keep_alive)
            }
            (HttpKvOp::Delete, BatchResult::DeleteExisted(b)) => {
                let body = serde_json::to_vec(&serde_json::json!({ "deleted": b }))
                    .unwrap_or_default();
                h::build_response(200, &body, cors, ctx.keep_alive)
            }
            (_, BatchResult::Error(e)) => {
                crate::metrics::HTTP_ERRORS
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                h::build_response(500, &h::error_body(e), cors, ctx.keep_alive)
            }
            _ => h::build_response(
                500,
                &h::error_body("unexpected reply"),
                cors,
                ctx.keep_alive,
            ),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ F65: 全局 UNIQUE 占坑状态机推进 (优先于其他聚合器)
    if sql_unique_drive(conn, conn_id, seq, worker_id, result, shard_inboxes, num_shards) {
        return;
    }
    // ⭐ F66: 系统表 CatalogDump 回调 → 合成虚拟表
    if let Some(spec) = conn.sql_sysq.remove(&seq) {
        let bin = conn.mysql_binary.remove(&seq);
        let bytes = match result {
            BatchResult::Catalog(entries) => {
                // decode schema 字节 (跳过坏的)
                let decoded: Vec<(String, TableSchema)> = entries
                    .iter()
                    .filter_map(|(t, b)| {
                        TableSchema::decode(b).ok().map(|s| (t.clone(), s))
                    })
                    .collect();
                sysq_render_catalog(conn.proto, bin, &spec, &conn.current_db.clone(), &decoded)
            }
            BatchResult::Error(e) => sql_err_bytes(conn.proto, e),
            _ => sql_err_bytes(conn.proto, "unexpected catalog reply"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ F67 (JOIN): 两表 hash join 状态机推进 (schema 拉取 / 两轮 gather / 完成点)
    if sql_join_drive(conn, conn_id, seq, worker_id, result, shard_inboxes, num_shards) {
        return;
    }
    // ⭐ X3: SQL 钩子 — schema 拉取续跑 (挂起语句在 schema 到达后继续规划)
    if let Some(pending) = conn.sql_pending.remove(&seq) {
        match result {
            BatchResult::GetValue(Some(bytes)) => match TableSchema::decode(bytes) {
                Ok(s) => {
                    let schema = std::sync::Arc::new(s);
                    // ⭐ W1: 存量表 (GetSchemaOp 拉取) 只填 schema, 不建路由
                    // (启用路由需 CREATE 时刻零数据的完备性前提)
                    conn.sql_cache
                        .borrow_mut()
                        .schemas
                        .insert((pending.db.to_string(), pending.table), schema.clone());
                    sql_run_dml(
                        conn, conn_id, seq, worker_id, &pending.db, shard_inboxes, num_shards,
                        schema, pending.stmt,
                    );
                }
                Err(e) => {
                    conn.resp_complete(seq, sql_err_bytes(conn.proto, &format!("bad schema: {e}")));
                }
            },
            BatchResult::GetValue(None) => {
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, &format!(
                        "table '{}' has no schema (not a SQL table)",
                        pending.table
                    )),
                );
            }
            BatchResult::Error(e) => conn.resp_complete(seq, sql_err_bytes(conn.proto, e)),
            _ => conn.resp_complete(seq, sql_err_bytes(conn.proto, "unexpected schema reply")),
        }
        return;
    }
    // ⭐ X3: SELECT pk 点查 — decode + 全条件过滤 → 0/1 行 (⭐ S2: COUNT → 计数)
    if let Some(ctx) = conn.sql_row_ctx.remove(&seq) {
        let bin = conn.mysql_binary.remove(&seq);
        // ⭐ v2 (F62): SERIALIZABLE 读集记录 — 首读指纹为准 (entry 不覆盖);
        // RYOW 命中 write_set 的读不经此路径 (读自己的写无需验证)
        if let Some(key) = ctx.read_key.clone()
            && let Some(txn) = conn.txn.as_mut()
        {
            let fp = match &result {
                BatchResult::GetValue(Some(row)) => Some(storage::wal::crc32(row)),
                _ => None,
            };
            txn.read_set.entry(key).or_insert(fp);
        }
        let bytes = match result {
            BatchResult::GetValue(Some(row)) => {
                match storage::row::decode_row(&ctx.schema, row) {
                    Ok(mut values) => {
                        // ⭐ RYOW (F63): 事务内 UPDATE 基于此盘行 → 叠加未提交 sets
                        for (ci, cv) in &ctx.ryow_overlay {
                            if let Some(slot) = values.get_mut(*ci as usize) {
                                *slot = cv.clone();
                            }
                        }
                        let hit = eval_pred(&ctx.schema, &values, &ctx.conds);
                        // ⭐ F71: 内层子查询 → 捕获 0/1 行 (投影/计数) 而非渲染
                        if conn.sql_subq.contains_key(&seq) {
                            let captured: Vec<Vec<ColValue>> = if !hit {
                                vec![]
                            } else if ctx.count {
                                vec![vec![ColValue::I64(1)]]
                            } else {
                                vec![ctx.proj.iter().map(|&i| values[i as usize].clone()).collect()]
                            };
                            sql_subq_advance(
                                conn, conn_id, seq, worker_id, default_db, db_view,
                                shard_inboxes, num_shards, captured,
                            );
                            return;
                        }
                        // ⭐ F72: 派生表内层 (pk 点查形态) → 物化后 worker 内存执行外层
                        if conn.sql_derived.contains_key(&seq) {
                            let (cols, captured) = derived_capture_rowctx(&ctx, hit, &values);
                            finish_derived(
                                conn, conn_id, seq, worker_id, bin, shard_inboxes, num_shards,
                                cols, captured,
                            );
                            return;
                        }
                        if hit {
                            if ctx.count {
                                render_sql_count(conn.proto, bin, 1)
                            } else {
                                render_sql_rows(conn.proto, bin, &ctx.schema, &ctx.proj, &ctx.out_names, &[values])
                            }
                        } else if ctx.count {
                            render_sql_count(conn.proto, bin, 0)
                        } else {
                            render_sql_rows(conn.proto, bin, &ctx.schema, &ctx.proj, &ctx.out_names, &[])
                        }
                    }
                    Err(e) => sql_err_bytes(conn.proto, &e.to_string()),
                }
            }
            BatchResult::GetValue(None) if conn.sql_subq.contains_key(&seq) => {
                // ⭐ F71: 内层子查询空结果
                let captured: Vec<Vec<ColValue>> =
                    if ctx.count { vec![vec![ColValue::I64(0)]] } else { vec![] };
                sql_subq_advance(
                    conn, conn_id, seq, worker_id, default_db, db_view, shard_inboxes,
                    num_shards, captured,
                );
                return;
            }
            BatchResult::GetValue(None) if conn.sql_derived.contains_key(&seq) => {
                // ⭐ F72: 派生表内层空结果
                let (cols, captured) = derived_capture_rowctx(&ctx, false, &[]);
                finish_derived(
                    conn, conn_id, seq, worker_id, bin, shard_inboxes, num_shards, cols, captured,
                );
                return;
            }
            BatchResult::GetValue(None) if ctx.count => render_sql_count(conn.proto, bin, 0),
            BatchResult::GetValue(None) => render_sql_rows(conn.proto, bin, &ctx.schema, &ctx.proj, &ctx.out_names, &[]),
            BatchResult::Error(e) => sql_err_bytes(conn.proto, e),
            _ => sql_err_bytes(conn.proto, "unexpected reply"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ X3: SELECT 索引路径广播聚合 (⭐ O3: unique 等值可早停; ⭐ S1: DML phase1)
    if conn.sql_select_agg.contains_key(&seq) {
        let proto = conn.proto;
        let bin = conn.mysql_binary.contains(&seq); // ⭐ P2 (借用前 peek)
        // ⭐ F71: 此 agg 属内层子查询 → 完成时 materialize 行集而非渲染
        let is_subq_inner = conn.sql_subq.contains_key(&seq);
        // ⭐ F72: 此 agg 属派生表内层 → 完成时物化 (列定义+行集) 交 finish_derived
        let is_derived = conn.sql_derived.contains_key(&seq);
        enum Fire {
            No,
            Reply(Vec<u8>),
            Dml { pks: Vec<Vec<u8>>, action: SqlDmlAction, target: (std::sync::Arc<str>, String) },
            SubqInner(Vec<Vec<ColValue>>),
            DerivedDone(MatResult),
        }
        let (fire, drained) = {
            let agg = conn.sql_select_agg.get_mut(&seq).expect("just checked");
            if !agg.done {
                match result {
                    BatchResult::Rows(rows) => agg.rows.extend(rows.iter().cloned()),
                    BatchResult::Error(e) => agg.error = Some(e.clone()),
                    _ => agg.error = Some("unexpected reply".into()),
                }
            }
            agg.remaining -= 1;
            // 回复时机: 全部回齐, 或 unique 等值首个非空/出错即早停 (DML 禁早停)
            let should_fire = !agg.done
                && (agg.remaining == 0
                    || (agg.unique_early && (!agg.rows.is_empty() || agg.error.is_some())));
            let fire = if should_fire {
                agg.done = true;
                match agg.dml.take() {
                    // ⭐ S1: DML phase1 完成 — 过滤取 pk (出错则直接回错)
                    Some(action) if agg.error.is_none() => match collect_dml_pks(agg) {
                        Ok(pks) => Fire::Dml {
                            pks,
                            action,
                            target: agg.dml_target.take().expect("dml 必带 target"),
                        },
                        Err(e) => Fire::Reply(sql_err_bytes(proto, &e)),
                    },
                    Some(_) => Fire::Reply(sql_err_bytes(
                        proto,
                        agg.error.as_deref().unwrap_or("error"),
                    )),
                    // ⭐ F71: 内层子查询 → materialize 行集捕获; 否则正常渲染
                    None if is_subq_inner => match materialize_select_agg(agg) {
                        Ok((_cols, rows)) => Fire::SubqInner(rows),
                        Err(e) => Fire::Reply(sql_err_bytes(proto, &e)),
                    },
                    // ⭐ F72: 派生表内层 → 物化 (含错误; 清理在 fire 处)
                    None if is_derived => Fire::DerivedDone(materialize_select_agg(agg)),
                    None => Fire::Reply(render_select_agg(proto, bin, agg)),
                }
            } else {
                Fire::No
            };
            (fire, agg.remaining == 0)
        };
        // agg 保留至全部回包收齐 (迟到回包只减计数丢结果, 防重复 complete)
        if drained {
            conn.sql_select_agg.remove(&seq);
            conn.mysql_binary.remove(&seq);
        }
        match fire {
            Fire::No => {}
            Fire::Reply(bytes) => conn.resp_complete(seq, bytes),
            // ⭐ F71: 内层子查询完成 → 存行集并推进编排 (行数上限护栏)
            Fire::SubqInner(rows) => {
                // ⭐ F73: 捕获阶段 OOM 护栏 (精确语义在 fold_one_subq 按叶子类型);
                // IN >SUBQ_IN_MAX / scalar >1 由 fold 报错, EXISTS 无上限但捕获封顶
                if rows.len() > SUBQ_IN_MAX {
                    conn.sql_subq.remove(&seq);
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(
                            proto,
                            "subquery result too large; rewrite as JOIN",
                        ),
                    );
                    return;
                }
                sql_subq_advance(
                    conn, conn_id, seq, worker_id, default_db, db_view, shard_inboxes,
                    num_shards, rows,
                );
            }
            // ⭐ F72: 派生表内层完成 → worker 内存执行外层 (错误时清理 ctx)
            Fire::DerivedDone(res) => match res {
                Ok((cols, rows)) => finish_derived(
                    conn, conn_id, seq, worker_id, bin, shard_inboxes, num_shards, cols, rows,
                ),
                Err(e) => {
                    conn.sql_derived.remove(&seq);
                    conn.resp_complete(seq, sql_err_bytes(proto, &e));
                }
            },
            Fire::Dml { pks, action, target } => {
                // ⭐ 事务 v1 (F61): 两阶段 DML 的 phase2 在事务中截流
                // (phase1 读的是已提交态 — v1 文档化语义)
                if conn.txn.is_some() {
                    let n = pks.len() as u64;
                    for pk in pks {
                        let op = sql_dml_op(&target.0, &target.1, pk, &action);
                        if let Err(e) = txn_buffer_op(conn, op) {
                            conn.resp_complete(seq, sql_err_bytes(proto, &e));
                            return;
                        }
                    }
                    conn.resp_complete(seq, sql_ok_bytes(proto, n));
                    return;
                }
                // phase2: 逐 pk 按路由下发 (DML 禁早停保证此刻 phase1 已 drained,
                // 同 seq 注册 dml_agg 无双聚合并存)
                debug_assert!(drained, "DML phase1 必须全量回齐后才 fire");
                if pks.is_empty() {
                    conn.resp_complete(seq, sql_ok_bytes(proto, 0));
                } else {
                    conn.sql_dml_agg.insert(
                        seq,
                        SqlDmlAgg {
                            remaining: pks.len(),
                            affected: 0,
                            error: None,
                            drop_key: None,
                        },
                    );
                    for pk in pks {
                        let op = sql_dml_op(&target.0, &target.1, pk, &action);
                        let sid = hash_route_op(&op, num_shards);
                        push_task_grouped(
                            conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes,
                        );
                    }
                }
            }
        }
        return;
    }
    // ⭐ 事务 v1 (F61): COMMIT 的 TxnApply 多 shard 聚合 — 全 OK 回 commit ok
    // (此刻各 shard 已 wal_barrier, 回复到达 ⇒ 已持久);
    // 任一失败回错 (跨 shard 已应用分片不回滚 — v1 gap 文档化)
    if let Some(agg) = conn.sql_txn_agg.get_mut(&seq) {
        match result {
            BatchResult::TxnApplied(n) => agg.applied += n,
            BatchResult::Error(e) => agg.error = Some(e.clone()),
            _ => agg.error = Some("unexpected reply".into()),
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.sql_txn_agg.remove(&seq).expect("just checked");
            conn.mysql_binary.remove(&seq);
            let bytes = match agg.error {
                Some(e) => sql_err_bytes(conn.proto, &format!("commit failed: {e}")),
                None => sql_ok_bytes(conn.proto, agg.applied),
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // ⭐ S1: DML 计数聚合 (INSERT 多行 / DELETE·UPDATE phase2 / DROP 广播)
    if let Some(agg) = conn.sql_dml_agg.get_mut(&seq) {
        match result {
            BatchResult::PutOk => agg.affected += 1,
            BatchResult::DeleteExisted(true) => agg.affected += 1,
            BatchResult::DeleteExisted(false) => {}
            BatchResult::Error(e) => agg.error = Some(e.clone()),
            _ => agg.error = Some("unexpected reply".into()),
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.sql_dml_agg.remove(&seq).expect("just checked");
            conn.mysql_binary.remove(&seq);
            let bytes = match agg.error {
                Some(e) => sql_err_bytes(conn.proto, &e),
                None => {
                    let affected = if let Some(key) = agg.drop_key {
                        // DROP 完成: 本 worker schema 缓存 + 进程级路由/注册清理,
                        // DDL epoch +1 (其它 worker 靠 epoch 失效重拉)
                        conn.sql_cache.borrow_mut().schemas.remove(&key);
                        let sh = &conn.sql_shared;
                        sh.created_here.write().unwrap().remove(&key);
                        sh.routes
                            .write()
                            .unwrap()
                            .retain(|(d, t, _), _| !(d == &key.0 && t == &key.1));
                        sh.ddl_epoch.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        0
                    } else {
                        agg.affected
                    };
                    sql_ok_bytes(conn.proto, affected)
                }
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // ⭐ X3: CREATE TABLE 广播聚合 (全 shard PutOk → 填缓存 + OK)
    if let Some(agg) = conn.sql_ddl_agg.get_mut(&seq) {
        match result {
            BatchResult::PutOk => {}
            BatchResult::Error(e) => agg.error = Some(e.clone()),
            _ => agg.error = Some("unexpected reply".into()),
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.sql_ddl_agg.remove(&seq).expect("just checked");
            conn.mysql_binary.remove(&seq);
            let bytes = match agg.error {
                Some(e) => sql_err_bytes(conn.proto, &e),
                None => {
                    // ⭐ W1/W2 → ORM-B2: CREATE 成功 → schema (本 worker) +
                    // created_here + 空路由 bloom (进程级共享 — 建表时刻零数据,
                    // 空 bloom 即完备; 跨 worker/门面 INSERT 都喂同一实例)
                    {
                        let sh = &conn.sql_shared;
                        let mut routes = sh.routes.write().unwrap();
                        for idx in &agg.schema.indexes {
                            routes
                                .entry((agg.key.0.clone(), agg.key.1.clone(), idx.iid))
                                .or_insert_with(|| {
                                    std::sync::Arc::new(
                                        (0..num_shards)
                                            .map(|_| storage::index_bloom::IndexBloom::new())
                                            .collect(),
                                    )
                                });
                        }
                        drop(routes);
                        sh.created_here.write().unwrap().insert(agg.key.clone());
                    }
                    conn.sql_cache.borrow_mut().schemas.insert(agg.key, agg.schema);
                    // ⭐ F79: ALTER 递增 ddl_epoch — 其他 worker 下次 dispatch 重拉新 schema,
                    // 避免用旧列数解码新写的行 (同 DROP 先例)
                    if agg.alter {
                        conn.sql_shared
                            .ddl_epoch
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    sql_ok_bytes(conn.proto, 0)
                }
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // ⭐ Y2: SQL conn 的裸结果兜底 (Sql/Pg 共用)
    if matches!(conn.proto, ProtocolKind::Sql | ProtocolKind::Pg) {
        let bytes = match result {
            BatchResult::PutOk => sql_ok_bytes(conn.proto, 1),
            BatchResult::Error(e) => sql_err_bytes(conn.proto, e),
            _ => sql_err_bytes(conn.proto, "unexpected reply"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ Phase G: Geo 渲染钩子 (复用 ZMScore/ZRange 结果, 优先拦截)
    if let Some(ctx) = conn.geo_ctx.remove(&seq) {
        let bytes = render_geo(&codec, ctx, result);
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ Phase B: Bitmap 读渲染钩子 (Get 结果 + 位运算)
    if let Some(ctx) = conn.bit_ctx.remove(&seq) {
        let bytes = render_bit(&codec, ctx, result);
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ MGET 聚合: Values 按组索引表回填原始槽, 全组回齐拼 *N 数组
    if let Some(agg) = conn.mget_agg.get_mut(&seq) {
        match result {
            BatchResult::Values(vs) => {
                if let Some(idxs) = agg.groups.get(group as usize) {
                    for (v, &orig) in vs.iter().zip(idxs.iter()) {
                        agg.slots[orig] = v.clone();
                    }
                }
            }
            BatchResult::Error(e) if agg.error.is_none() => {
                agg.error = Some(e.clone());
            }
            _ => {}
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.mget_agg.remove(&seq).expect("just checked");
            let bytes = if let Some(e) = agg.error {
                codec.encode_error(&e)
            } else {
                let mut out = format!("*{}\r\n", agg.slots.len()).into_bytes();
                for slot in &agg.slots {
                    match slot {
                        Some(stored) => {
                            // ⭐ N3: 按 tag 渲染 (数值二进制 → 字符串)
                            out.extend_from_slice(&codec.encode_bulk(&render(stored)));
                        }
                        None => out.extend_from_slice(b"$-1\r\n"),
                    }
                }
                out
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // ⭐ MSET 聚合: 全组 MultiPutOk → +OK
    if let Some(agg) = conn.mset_agg.get_mut(&seq) {
        if let BatchResult::Error(e) = result
            && agg.error.is_none()
        {
            agg.error = Some(e.clone());
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.mset_agg.remove(&seq).expect("just checked");
            let bytes = match agg.error {
                Some(e) => codec.encode_error(&e),
                None => codec.encode_ok(),
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // ⭐ EXISTS 聚合: GetValue(Some) 计数, 全部回齐回 :n
    if let Some(agg) = conn.exists_agg.get_mut(&seq) {
        if let BatchResult::GetValue(Some(_)) = result {
            agg.count += 1;
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let count = agg.count;
            conn.exists_agg.remove(&seq);
            conn.resp_complete(seq, codec.encode_integer(count));
        }
        return;
    }
    // ⭐ STRLEN/TYPE/HEXISTS: Get 结果语义转换
    if let Some(kind) = conn.get_kind.remove(&seq) {
        let bytes = match (kind, result) {
            (GetKind::Strlen, BatchResult::GetValue(None)) => codec.encode_integer(0),
            (GetKind::Strlen, BatchResult::GetValue(Some(stored))) => {
                // ⭐ N3: 数值 tag 按渲染后字符串计长 (Redis 语义)
                codec.encode_integer(render(stored).len() as i64)
            }
            (GetKind::TypeOf, BatchResult::GetValue(None)) => codec.encode_simple("none"),
            (GetKind::TypeOf, BatchResult::GetValue(Some(_))) => codec.encode_simple("string"),
            // ⭐ Phase H: HEXISTS — HGet 结果转 0/1
            (GetKind::HExists, BatchResult::GetValue(None)) => codec.encode_integer(0),
            (GetKind::HExists, BatchResult::GetValue(Some(_))) => codec.encode_integer(1),
            (_, BatchResult::Error(e)) => codec.encode_error(e),
            _ => codec.encode_error("unexpected result"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ Phase H: HMSET — Integer 结果转 +OK
    if conn.hmset_ok.remove(&seq) {
        let bytes = match result {
            BatchResult::Integer(_) => codec.encode_ok(),
            BatchResult::Error(e) => codec.encode_error(e),
            _ => codec.encode_error("unexpected result"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ GETRANGE: Get 结果渲染后按 (start,end) 切片 (支持负索引)
    if let Some((start, end)) = conn.getrange_ctx.remove(&seq) {
        let bytes = match result {
            BatchResult::GetValue(None) => codec.encode_bulk(b""),
            BatchResult::GetValue(Some(stored)) => {
                let s = render(stored);
                codec.encode_bulk(getrange_slice(s.as_ref(), start, end))
            }
            BatchResult::Error(e) => codec.encode_error(e),
            _ => codec.encode_error("unexpected result"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ MSETNX 聚合: 全组 Integer(1) → :1, 任一非 1 → :0
    if let Some(agg) = conn.msetnx_agg.get_mut(&seq) {
        if !matches!(result, BatchResult::Integer(1)) {
            agg.all_set = false;
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let all = agg.all_set;
            conn.msetnx_agg.remove(&seq);
            conn.resp_complete(seq, codec.encode_integer(i64::from(all)));
        }
        return;
    }
    // ⭐ Phase Set: SINTER/SUNION/SDIFF 聚合 — 全部 key 的成员回齐后求代数
    if let Some(agg) = conn.setalg_agg.get_mut(&seq) {
        match result {
            BatchResult::Members(ms) => {
                if let Some(slot) = agg.sets.get_mut(group as usize) {
                    *slot = Some(ms.clone());
                }
            }
            BatchResult::Error(e) if agg.error.is_none() => {
                agg.error = Some(e.clone());
            }
            _ => {}
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.setalg_agg.remove(&seq).expect("just checked");
            if let Some(e) = agg.error {
                conn.resp_complete(seq, codec.encode_error(&e));
                return;
            }
            use std::collections::HashSet;
            let (card_only, limit) = (agg.card_only, agg.limit);
            let store_dst = agg.store_dst;
            // ⭐ D3: 二阶段任务用命令发起时的 (db, table), 不受后续 SELECT 影响
            let (agg_db, agg_table) = (agg.db.clone(), agg.table.clone());
            let mut sets: Vec<Vec<Vec<u8>>> =
                agg.sets.into_iter().map(|s| s.unwrap_or_default()).collect();
            let first = if sets.is_empty() { Vec::new() } else { sets.remove(0) };
            let out: Vec<Vec<u8>> = match agg.op {
                SetAlgOp::Inter => {
                    let others: Vec<HashSet<&[u8]>> = sets
                        .iter()
                        .map(|s| s.iter().map(|m| m.as_slice()).collect())
                        .collect();
                    first
                        .into_iter()
                        .filter(|m| others.iter().all(|o| o.contains(m.as_slice())))
                        .collect()
                }
                SetAlgOp::Diff => {
                    let others: Vec<HashSet<&[u8]>> = sets
                        .iter()
                        .map(|s| s.iter().map(|m| m.as_slice()).collect())
                        .collect();
                    first
                        .into_iter()
                        .filter(|m| !others.iter().any(|o| o.contains(m.as_slice())))
                        .collect()
                }
                SetAlgOp::Union => {
                    let mut seen: HashSet<Vec<u8>> = HashSet::new();
                    let mut out = Vec::new();
                    for m in first.into_iter().chain(sets.into_iter().flatten()) {
                        if seen.insert(m.clone()) {
                            out.push(m);
                        }
                    }
                    out
                }
            };
            // ⭐ C3: *STORE — 结果写 dst (同 shard FIFO: 先 Delete 再 SAdd), 完成后回 :card
            if let Some(dst) = store_dst {
                let card = out.len() as i64;
                let sid = hash_route_key(agg_db.as_ref(), agg_table.as_ref(), &dst, num_shards);
                let mut remaining = 1usize;
                let del = BatchOp::Delete { db: agg_db.clone(), table: agg_table.clone(), key: dst.clone() };
                push_task_grouped(conn_id, seq, worker_id, 0, sid, del, shard_inboxes);
                if !out.is_empty() {
                    remaining += 1;
                    let sadd = BatchOp::SAdd { db: agg_db, table: agg_table, key: dst, members: out };
                    push_task_grouped(conn_id, seq, worker_id, 1, sid, sadd, shard_inboxes);
                }
                conn.store_agg.insert(seq, StoreFinishAgg { remaining, card, error: None });
                return;
            }
            // ⭐ C1: SINTERCARD — 只回势 (LIMIT 截断); 否则回成员数组
            let bytes = if card_only {
                let card = if limit > 0 { out.len().min(limit) } else { out.len() };
                codec.encode_integer(card as i64)
            } else {
                let mut buf = format!("*{}\r\n", out.len()).into_bytes();
                for m in &out {
                    buf.extend_from_slice(&codec.encode_bulk(m));
                }
                buf
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // ⭐ C3: ZINTERSTORE/ZUNIONSTORE 源聚合 — ZRange(withscores) 交替串还原 (member, score)
    if let Some(agg) = conn.zstore_agg.get_mut(&seq) {
        match result {
            BatchResult::Members(ms) => {
                let mut rows = Vec::with_capacity(ms.len() / 2);
                let mut i = 0;
                while i + 1 < ms.len() {
                    let score = std::str::from_utf8(&ms[i + 1])
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    rows.push((ms[i].clone(), score));
                    i += 2;
                }
                if let Some(slot) = agg.sets.get_mut(group as usize) {
                    *slot = Some(rows);
                }
            }
            BatchResult::Error(e) if agg.error.is_none() => {
                agg.error = Some(e.clone());
            }
            _ => {}
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.zstore_agg.remove(&seq).expect("just checked");
            if let Some(e) = agg.error {
                conn.resp_complete(seq, codec.encode_error(&e));
                return;
            }
            // SUM 聚合 (首现序保序; inter 要求出现在全部源)
            let inter = agg.inter;
            let n_sets = agg.sets.len();
            // ⭐ D3: 二阶段任务用命令发起时的 (db, table)
            let (agg_db, agg_table) = (agg.db.clone(), agg.table.clone());
            let mut acc: Vec<(Vec<u8>, f64, usize)> = Vec::new();
            let mut pos: HashMap<Vec<u8>, usize> = HashMap::new();
            for set in agg.sets.into_iter().map(|s| s.unwrap_or_default()) {
                for (m, sc) in set {
                    match pos.get(&m) {
                        Some(&i) => {
                            acc[i].1 += sc;
                            acc[i].2 += 1;
                        }
                        None => {
                            pos.insert(m.clone(), acc.len());
                            acc.push((m, sc, 1));
                        }
                    }
                }
            }
            let pairs: Vec<(f64, Vec<u8>)> = acc
                .into_iter()
                .filter(|(_, _, cnt)| !inter || *cnt == n_sets)
                .map(|(m, sc, _)| (sc, m))
                .collect();
            let card = pairs.len() as i64;
            let dst = agg.dst;
            let sid = hash_route_key(agg_db.as_ref(), agg_table.as_ref(), &dst, num_shards);
            let mut remaining = 1usize;
            let del = BatchOp::Delete { db: agg_db.clone(), table: agg_table.clone(), key: dst.clone() };
            push_task_grouped(conn_id, seq, worker_id, 0, sid, del, shard_inboxes);
            if !pairs.is_empty() {
                remaining += 1;
                let zadd = BatchOp::ZAdd { db: agg_db, table: agg_table, key: dst, pairs };
                push_task_grouped(conn_id, seq, worker_id, 1, sid, zadd, shard_inboxes);
            }
            conn.store_agg.insert(seq, StoreFinishAgg { remaining, card, error: None });
        }
        return;
    }
    // ⭐ C3: *STORE 第二阶段 (Delete + SAdd/ZAdd) 全部完成 → 回 :card
    if let Some(agg) = conn.store_agg.get_mut(&seq) {
        if let BatchResult::Error(e) = result
            && agg.error.is_none()
        {
            agg.error = Some(e.clone());
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.store_agg.remove(&seq).expect("just checked");
            let bytes = match agg.error {
                Some(e) => codec.encode_error(&e),
                None => codec.encode_integer(agg.card),
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // DEL 聚合路径
    if let Some(agg) = conn.del_agg.get_mut(&seq) {
        match result {
            BatchResult::DeleteExisted(existed) => {
                if *existed {
                    agg.count += 1;
                }
            }
            BatchResult::Error(_) => {
                // 单 key 失败按未删除计 (Redis DEL 语义: 返回实际删除数)
            }
            _ => {}
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let count = agg.count;
            conn.del_agg.remove(&seq);
            conn.resp_complete(seq, codec.encode_integer(count));
        }
        return;
    }

    let bytes = match result {
        // ⭐ 事务 v1: TxnApplied 只出现在 SQL 门面 (上方 sql_txn_agg 已拦截)
        BatchResult::TxnApplied(_) => codec.encode_error("unexpected txn reply"),
        // ⭐ F65: 占坑结果只出现在 SQL 门面 (sql_unique_drive 已拦截)
        BatchResult::ReserveOk | BatchResult::ReserveConflict { .. } => {
            codec.encode_error("unexpected unique reply")
        }
        BatchResult::Catalog(_) => codec.encode_error("unexpected catalog reply"),
        BatchResult::ProjRows(_) => codec.encode_error("unexpected join reply"),
        BatchResult::PutOk | BatchResult::MultiPutOk => codec.encode_ok(),
        BatchResult::GetValue(None) => codec.encode_nil(),
        BatchResult::GetValue(Some(stored)) => {
            // ⭐ N3: 按 tag 渲染 (RAW 借用零拷贝; 数值二进制 → 字符串)
            codec.encode_bulk(&render(stored))
        }
        BatchResult::DeleteExisted(existed) => codec.encode_integer(*existed as i64),
        BatchResult::Integer(n) => codec.encode_integer(*n),
        // INCRBYFLOAT: Redis 语义回 bulk string (非 integer)
        BatchResult::Double(f) => codec.encode_bulk(format!("{f}").as_bytes()),
        // ⭐ Phase H: HMGET 单 op 直回 Values → *N 数组 (逐项渲染;
        // ⭐ C1: ZMSCORE 的 Values 已成形, 裸 bulk 直出)
        BatchResult::Values(vs) => {
            let raw = conn.values_raw.remove(&seq);
            let mut out = format!("*{}\r\n", vs.len()).into_bytes();
            for v in vs {
                match v {
                    Some(stored) => {
                        if raw {
                            out.extend_from_slice(&codec.encode_bulk(stored));
                        } else {
                            out.extend_from_slice(&codec.encode_bulk(&render(stored)));
                        }
                    }
                    None => out.extend_from_slice(b"$-1\r\n"),
                }
            }
            out
        }
        // ⭐ Phase H: HGETALL/HKEYS/HVALS/HSCAN 按 pairs_kind 渲染
        BatchResult::Pairs(ps) => {
            let kind = conn.pairs_kind.remove(&seq).unwrap_or(PairsKind::All);
            encode_pairs(&codec, ps, kind)
        }
        // ⭐ Phase Set: SMEMBERS/SSCAN/SPOP/SRANDMEMBER 按 members_kind 渲染
        BatchResult::Members(ms) => {
            let kind = conn.members_kind.remove(&seq).unwrap_or(MembersKind::List);
            match kind {
                MembersKind::List => {
                    let mut out = format!("*{}\r\n", ms.len()).into_bytes();
                    for m in ms {
                        out.extend_from_slice(&codec.encode_bulk(m));
                    }
                    out
                }
                MembersKind::Scan => {
                    let mut out = b"*2\r\n".to_vec();
                    out.extend_from_slice(&codec.encode_bulk(b"0"));
                    out.extend_from_slice(&format!("*{}\r\n", ms.len()).into_bytes());
                    for m in ms {
                        out.extend_from_slice(&codec.encode_bulk(m));
                    }
                    out
                }
                MembersKind::One => match ms.first() {
                    Some(m) => codec.encode_bulk(m),
                    None => codec.encode_nil(),
                },
            }
        }
        // ⭐ Phase Z: ZSCORE/ZRANK 可选成员 (Some→bulk, None→nil)
        BatchResult::OptMember(m) => match m {
            Some(b) => codec.encode_bulk(b),
            None => codec.encode_nil(),
        },
        // ⭐ C1: SMISMEMBER → *N 个 :0/:1
        BatchResult::IntList(ns) => {
            let mut out = format!("*{}\r\n", ns.len()).into_bytes();
            for n in ns {
                out.extend_from_slice(&codec.encode_integer(*n));
            }
            out
        }
        // ⭐ Q5: Rows 是 SQL 门面专属 (RESP 命令不产生; 防御性兜底)
        BatchResult::Rows(_) => codec.encode_error("row results unsupported on RESP"),
        BatchResult::Error(e) => codec.encode_error(e),
    };
    conn.resp_complete(seq, bytes);
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

fn hash_route_op(op: &BatchOp, num_shards: usize) -> usize {
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

/// 语句分派: CREATE 广播 schema; INSERT/SELECT 需 schema (缓存 miss 先拉).
/// ⭐ 事务 v1 (F61): PG 帧流中是否含 ErrorResponse ('E' 帧) —
/// resp_complete 单点检测, 事务内出错置 failed (25P02 语义).
fn pg_frames_contain_error(bytes: &[u8]) -> bool {
    let mut pos = 0usize;
    while pos + 5 <= bytes.len() {
        let ty = bytes[pos];
        let len = u32::from_be_bytes([
            bytes[pos + 1],
            bytes[pos + 2],
            bytes[pos + 3],
            bytes[pos + 4],
        ]) as usize;
        if ty == b'E' {
            return true;
        }
        pos += 1 + len.max(4); // len 含自身 4B
    }
    false
}

/// ⭐ W2/事务 v1: RowPut 喂进程级路由 bloom (value → shard).
/// 事务缓冲时也喂 — rollback 后只多假阳性 (只增语义无害);
/// commit 时不重复喂.
fn feed_route_bloom(
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
        let ty = schema.columns[idx.col as usize].ty;
        if let Some(enc) = storage::sql_rows::index_val_bytes(ty, &values[idx.col as usize]) {
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
    NeedBase(Vec<(u16, ColValue)>),
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
    let mut pending_sets: Vec<(u16, ColValue)> = Vec::new(); // 基于盘行的叠加
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
                if let Some(v) = cur.as_mut() {
                    for (ci, cv) in sets {
                        if let Some(slot) = v.get_mut(*ci as usize) {
                            *slot = cv.clone();
                        }
                    }
                } else {
                    // 基于已提交盘行: 累积 sets (后写覆盖前写)
                    based_on_disk = true;
                    for (ci, cv) in sets {
                        if let Some(e) = pending_sets.iter_mut().find(|(c, _)| c == ci) {
                            e.1 = cv.clone();
                        } else {
                            pending_sets.push((*ci, cv.clone()));
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

/// ColValue → SqlValue (子查询结果折叠回字面量).
fn colval_to_sqlval(cv: &ColValue) -> SqlValue {
    match cv {
        ColValue::Null => SqlValue::Null,
        ColValue::I64(i) => SqlValue::Int(*i),
        ColValue::F64(f) => SqlValue::Float(*f),
        ColValue::Bytes(b) => SqlValue::Str(b.clone()),
        // ⭐ F81: Decimal 折叠回字面量用定点文本 (保精度; 目标列再按 scale 解析)
        ColValue::Decimal(x, scale) => SqlValue::Str(render_decimal(*x, *scale).into_bytes()),
    }
}

/// stmt 的 WHERE conds (Select/Delete/Update) 只读引用.
fn stmt_where_conds(stmt: &SqlStmt) -> Option<&Pred<Cond>> {
    match stmt {
        SqlStmt::Select { conds, .. }
        | SqlStmt::Delete { conds, .. }
        | SqlStmt::Update { conds, .. } => Some(conds),
        _ => None,
    }
}

/// 重建 stmt 替换 conds (折叠后重跑外层用).
fn stmt_replace_conds(stmt: SqlStmt, new: Pred<Cond>) -> SqlStmt {
    match stmt {
        SqlStmt::Select { table, items, limit, order, offset, group_by, having, .. } => {
            SqlStmt::Select { table, items, conds: new, limit, order, offset, group_by, having }
        }
        SqlStmt::Delete { table, .. } => SqlStmt::Delete { table, conds: new },
        SqlStmt::Update { table, sets, .. } => SqlStmt::Update { table, sets, conds: new },
        other => other,
    }
}

/// DFS 左右序收集 WHERE 中的子查询内层 stmt (与 fold 同序).
fn collect_pred_subq(pred: &Pred<Cond>, out: &mut Vec<SqlStmt>) {
    match pred {
        Pred::Leaf(c) => {
            if let SqlValue::Subquery(s) = &c.val {
                out.push((**s).clone());
            }
        }
        Pred::And(v) | Pred::Or(v) => v.iter().for_each(|p| collect_pred_subq(p, out)),
        Pred::Not(b) => collect_pred_subq(b, out),
    }
}

fn true_pred() -> Pred<Cond> {
    Pred::And(vec![])
}
fn false_pred() -> Pred<Cond> {
    Pred::Not(Box::new(Pred::And(vec![])))
}

/// ⭐ F74: 该子查询 stmt 的 WHERE 是否含相关列 (ColRef) — 判定关联性.
fn subquery_has_colref(inner: &SqlStmt) -> bool {
    stmt_where_conds(inner).is_some_and(|p| p.leaves().iter().any(|c| matches!(c.val, SqlValue::ColRef(_))))
}

/// ⭐ F74: 相关等值两侧分类 → (外层列名, 内层列名). 一侧外层一侧内层, 否则 Err.
fn classify_corr(
    outer_table: &str,
    inner_table: &str,
    a: &QualCol,
    b: &QualCol,
) -> Result<(String, String), String> {
    let is_outer = |q: &QualCol| {
        q.qualifier.as_deref().is_some_and(|x| x.eq_ignore_ascii_case(outer_table))
    };
    let is_inner = |q: &QualCol| match &q.qualifier {
        Some(x) => x.eq_ignore_ascii_case(inner_table),
        None => true, // 无限定 → 默认内层
    };
    if is_outer(a) && !is_outer(b) && is_inner(b) {
        Ok((a.col.clone(), b.col.clone()))
    } else if is_outer(b) && !is_outer(a) && is_inner(a) {
        Ok((b.col.clone(), a.col.clone()))
    } else {
        Err("correlated equality must reference one outer and one inner column (v1)".into())
    }
}

/// ⭐ F74: 单个关联 EXISTS 内层 → 非关联 IN 叶 (`外层列 IN (SELECT 内层列 FROM .. WHERE 剩余)`).
fn decorrelate_exists(outer_table: &str, inner: &SqlStmt) -> Result<Pred<Cond>, String> {
    let SqlStmt::Select { table: inner_table, conds, .. } = inner else {
        return Err("correlated EXISTS inner must be a simple SELECT (v1)".into());
    };
    let Some(conjuncts) = conds.as_conjuncts() else {
        return Err("correlated EXISTS supports only AND conditions (v1)".into());
    };
    let mut corr: Option<(String, String)> = None;
    let mut remaining: Vec<Cond> = Vec::new();
    for c in conjuncts {
        if let SqlValue::ColRef(rhs) = &c.val {
            if c.op != CmpOp::Eq {
                return Err("correlated condition must be equality (v1)".into());
            }
            if corr.is_some() {
                return Err("correlated EXISTS supports only a single equality (v1)".into());
            }
            let pair = classify_corr(
                outer_table,
                inner_table,
                &QualCol::parse(&c.col),
                &QualCol::parse(rhs),
            )?;
            corr = Some(pair);
        } else {
            remaining.push(c.clone());
        }
    }
    let Some((outer_col, inner_col)) = corr else {
        return Err("correlated EXISTS: no correlation equality found (v1)".into());
    };
    let new_conds = if remaining.is_empty() {
        Pred::And(vec![])
    } else {
        Pred::And(remaining.into_iter().map(Pred::Leaf).collect())
    };
    let new_inner = SqlStmt::Select {
        table: inner_table.clone(),
        items: vec![sql::SelectItem::Col { name: inner_col, alias: None }],
        conds: new_conds,
        limit: None,
        order: vec![],
        offset: None,
        group_by: vec![],
        having: Pred::And(vec![]),
    };
    Ok(Pred::Leaf(Cond {
        col: outer_col,
        op: CmpOp::In,
        val: SqlValue::Subquery(Box::new(new_inner)),
        set: vec![],
    }))
}

/// ⭐ F74: 单叶去相关. 关联 EXISTS → IN; 非关联原样; 其余含相关形态 → 拒.
fn decorrelate_leaf(outer_table: &str, c: &Cond) -> Result<Pred<Cond>, String> {
    if c.col == sql::EXISTS_SENTINEL_COL
        && let SqlValue::Subquery(inner) = &c.val
    {
        if subquery_has_colref(inner) {
            return decorrelate_exists(outer_table, inner);
        }
        return Ok(Pred::Leaf(c.clone())); // 非关联 EXISTS (F71 处理)
    }
    if matches!(c.val, SqlValue::ColRef(_)) {
        return Err("correlated subquery not supported (v1, only single-equality EXISTS)".into());
    }
    if let SqlValue::Subquery(inner) = &c.val
        && subquery_has_colref(inner)
    {
        return Err("correlated subquery not supported (v1, only single-equality EXISTS)".into());
    }
    Ok(Pred::Leaf(c.clone()))
}

/// ⭐ F74: 递归去相关整个谓词树 (NOT EXISTS 包在 Pred::Not 内, 改写叶后自然成 NOT IN).
fn decorrelate_pred(outer_table: &str, pred: &Pred<Cond>) -> Result<Pred<Cond>, String> {
    match pred {
        Pred::Leaf(c) => decorrelate_leaf(outer_table, c),
        Pred::And(v) => Ok(Pred::And(
            v.iter().map(|p| decorrelate_pred(outer_table, p)).collect::<Result<_, _>>()?,
        )),
        Pred::Or(v) => Ok(Pred::Or(
            v.iter().map(|p| decorrelate_pred(outer_table, p)).collect::<Result<_, _>>()?,
        )),
        Pred::Not(b) => Ok(Pred::Not(Box::new(decorrelate_pred(outer_table, b)?))),
    }
}

/// ⭐ F74: 去相关整个 stmt 的 WHERE (仅 Select/Delete/Update). 无相关时返回原 stmt.
fn decorrelate_stmt(stmt: &SqlStmt) -> Result<SqlStmt, String> {
    let table = match stmt {
        SqlStmt::Select { table, .. }
        | SqlStmt::Delete { table, .. }
        | SqlStmt::Update { table, .. } => table.clone(),
        _ => return Ok(stmt.clone()),
    };
    let conds = stmt_where_conds(stmt).expect("has where");
    let new = decorrelate_pred(&table, conds)?;
    Ok(stmt_replace_conds(stmt.clone(), new))
}

/// 单个子查询叶子折叠. rows = 内层投影行集.
fn fold_one_subq(c: &Cond, rows: &[Vec<ColValue>]) -> Result<Pred<Cond>, String> {
    // EXISTS: 哨兵空列名 → 非空真/空假
    if c.col == sql::EXISTS_SENTINEL_COL {
        return Ok(if rows.is_empty() { false_pred() } else { true_pred() });
    }
    // IN 子查询: 各行首列 → set (跳 NULL); 空集 → 恒假
    if c.op == CmpOp::In {
        let mut set: Vec<SqlValue> = rows
            .iter()
            .filter_map(|r| r.first())
            .map(colval_to_sqlval)
            .filter(|v| *v != SqlValue::Null)
            .collect();
        if set.is_empty() {
            return Ok(false_pred());
        }
        // ⭐ F73: 排序去重 → 大集合求值二分化; 去重后 > SUBQ_IN_MAX 才报错
        sql::sort_in_set(&mut set);
        if set.len() > SUBQ_IN_MAX {
            return Err(format!(
                "IN subquery returns too many rows ({} > {SUBQ_IN_MAX})",
                set.len()
            ));
        }
        return Ok(Pred::Leaf(Cond { col: c.col.clone(), op: CmpOp::In, val: SqlValue::Null, set }));
    }
    // 标量子查询: 0 行→假, 1 行→常量, >1→错
    match rows.len() {
        0 => Ok(false_pred()),
        1 => {
            let sv = rows[0].first().map(colval_to_sqlval).unwrap_or(SqlValue::Null);
            if sv == SqlValue::Null {
                return Ok(false_pred());
            }
            Ok(Pred::Leaf(Cond { col: c.col.clone(), op: c.op, val: sv, set: vec![] }))
        }
        _ => Err("subquery returns more than one row".into()),
    }
}

/// 按 DFS 序消费 results, 子查询叶子 → Cond/恒真恒假子树.
fn fold_pred_subq(
    pred: &Pred<Cond>,
    it: &mut std::slice::Iter<Vec<Vec<ColValue>>>,
) -> Result<Pred<Cond>, String> {
    match pred {
        Pred::Leaf(c) => {
            if matches!(c.val, SqlValue::Subquery(_)) {
                let rows = it.next().ok_or("subquery result missing")?;
                fold_one_subq(c, rows)
            } else {
                Ok(Pred::Leaf(c.clone()))
            }
        }
        Pred::And(v) => {
            Ok(Pred::And(v.iter().map(|p| fold_pred_subq(p, it)).collect::<Result<_, _>>()?))
        }
        Pred::Or(v) => {
            Ok(Pred::Or(v.iter().map(|p| fold_pred_subq(p, it)).collect::<Result<_, _>>()?))
        }
        Pred::Not(b) => Ok(Pred::Not(Box::new(fold_pred_subq(b, it)?))),
    }
}

/// ⭐ F71: 启动子查询编排. 无子查询返回 false (caller 走常规); 否则跑首个内层返回 true.
#[allow(clippy::too_many_arguments)]
fn sql_subq_start(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    stmt: &SqlStmt,
) -> bool {
    // ⭐ F74: 先去相关 (单等值关联 EXISTS/NOT EXISTS → 非关联 IN/NOT IN);
    // 不可去相关形态 → 报错 (已消费, 返回 true)
    let decorr;
    let stmt: &SqlStmt = match decorrelate_stmt(stmt) {
        Ok(s) => {
            decorr = s;
            &decorr
        }
        Err(e) => {
            conn.resp_complete(seq, sql_err_bytes(conn.proto, &e));
            return true;
        }
    };
    let mut inners: Vec<SqlStmt> = Vec::new();
    if let Some(p) = stmt_where_conds(stmt) {
        collect_pred_subq(p, &mut inners);
    }
    if inners.is_empty() {
        return false;
    }
    // v1: 内层仅单表 SELECT (非 JOIN, 非嵌套) — 否则会绕过 SqlSelectAgg 拦截
    for inn in &inners {
        if !matches!(inn, SqlStmt::Select { .. }) {
            conn.resp_complete(
                seq,
                sql_err_bytes(conn.proto, "subquery inner must be a simple SELECT (v1)"),
            );
            return true;
        }
        if let Some(p) = stmt_where_conds(inn) {
            let mut nested = Vec::new();
            collect_pred_subq(p, &mut nested);
            if !nested.is_empty() {
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, "nested subquery not supported (v1)"),
                );
                return true;
            }
        }
    }
    let first = inners[0].clone();
    conn.sql_subq.insert(
        seq,
        SubqCtx { outer: stmt.clone(), db: db.clone(), inners, results: Vec::new(), cur: 0 },
    );
    sql_dispatch_stmt(
        conn, conn_id, seq, worker_id, db, default_db, db_view, shard_inboxes, num_shards, first,
    );
    true
}

/// ⭐ F71: 内层完成→存行集→跑下一内层或折叠重跑外层.
#[allow(clippy::too_many_arguments)]
fn sql_subq_advance(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    captured: Vec<Vec<ColValue>>,
) {
    let (next, db) = {
        let ctx = conn.sql_subq.get_mut(&seq).expect("subq ctx");
        ctx.results.push(captured);
        ctx.cur += 1;
        let next = ctx.inners.get(ctx.cur).cloned();
        (next, ctx.db.clone())
    };
    if let Some(inner) = next {
        sql_dispatch_stmt(
            conn, conn_id, seq, worker_id, &db, default_db, db_view, shard_inboxes, num_shards, inner,
        );
        return;
    }
    // 全部内层完 → 折叠 → 重跑外层
    let ctx = conn.sql_subq.remove(&seq).expect("subq ctx");
    let folded = {
        let conds = stmt_where_conds(&ctx.outer).expect("outer has where");
        let mut it = ctx.results.iter();
        fold_pred_subq(conds, &mut it)
    };
    match folded {
        Ok(fp) => {
            let outer = stmt_replace_conds(ctx.outer, fp);
            sql_dispatch_stmt(
                conn, conn_id, seq, worker_id, &db, default_db, db_view, shard_inboxes, num_shards,
                outer,
            );
        }
        Err(e) => conn.resp_complete(seq, sql_err_bytes(conn.proto, &e)),
    }
}

/// ⭐ F72: 派生表内层走 pk 点查 (SqlRowCtx) 完成时的物化 —
/// 从 ctx 合成列定义 (COUNT → 单列; 否则投影列) + 0/1 行行集.
fn derived_capture_rowctx(
    ctx: &SqlRowCtx,
    hit: bool,
    values: &[ColValue],
) -> (Vec<(String, ColType)>, Vec<Vec<ColValue>>) {
    if ctx.count {
        let n = i64::from(hit);
        return (
            vec![("COUNT(*)".to_string(), ColType::I64)],
            vec![vec![ColValue::I64(n)]],
        );
    }
    let cols: Vec<(String, ColType)> = ctx
        .proj
        .iter()
        .map(|&i| {
            let c = &ctx.schema.columns[i as usize];
            (c.name.clone(), c.ty)
        })
        .collect();
    let rows = if hit {
        vec![ctx.proj.iter().map(|&i| values[i as usize].clone()).collect()]
    } else {
        vec![]
    };
    (cols, rows)
}

/// ⭐ F72: 派生表内层物化完成 → 外层在 worker 内存执行并回包.
#[allow(clippy::too_many_arguments)]
fn finish_derived(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    binary: bool,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    cols: Vec<(String, ColType)>,
    rows: Vec<Vec<ColValue>>,
) {
    let ctx = conn.sql_derived.remove(&seq).expect("derived ctx");
    match ctx {
        // ⭐ F72: 单独派生表 → worker 内存执行外层并回包
        DerivedCtx::Standalone { alias, items, conds, order, limit, offset } => {
            let bytes = derived_render(
                conn.proto, binary, &alias, &items, &conds, &order, limit, offset, &cols, rows,
            );
            conn.resp_complete(seq, bytes);
        }
        // ⭐ F75: 派生表作 JOIN 首表 → 预填 tables[0] 后转 JOIN 状态机
        DerivedCtx::JoinFrom { db, join_stmt } => {
            finish_derived_join(
                conn, conn_id, seq, worker_id, shard_inboxes, num_shards, db, join_stmt, cols, rows,
            );
        }
    }
}

/// ⭐ F75: 派生表物化完成 → 建 SqlJoinCtx (tables[0] 预填) → sql_join_kickoff.
#[allow(clippy::too_many_arguments)]
fn finish_derived_join(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    db: std::sync::Arc<str>,
    join_stmt: SqlStmt,
    cols: Vec<(String, ColType)>,
    rows: Vec<Vec<ColValue>>,
) {
    if rows.len() > JOIN_MAX_ROWS {
        conn.resp_complete(seq, sql_err_bytes(conn.proto, "derived table too large (limit 262144 rows)"));
        return;
    }
    let SqlStmt::SelectJoin { from, joins, items, conds, order, limit, offset, .. } = join_stmt else {
        conn.resp_complete(seq, sql_err_bytes(conn.proto, "internal: derived join expects SelectJoin"));
        return;
    };
    // 合成派生表 schema (内层真实列类型); proj = 全列 identity (行已定宽)
    let synth = std::sync::Arc::new(TableSchema {
        version: 1,
        columns: cols
            .iter()
            .map(|(n, t)| storage::schema::Column { name: n.clone(), ty: *t, nullable: true })
            .collect(),
        pk_col: 0,
        indexes: Vec::new(),
        next_iid: 0,
        version_ncols: Vec::new(),
    });
    let ncols = cols.len() as u16;
    let mut tables: Vec<JoinTable> = Vec::with_capacity(joins.len() + 1);
    tables.push(JoinTable {
        table: std::sync::Arc::from(from.table.as_str()),
        alias: from.alias.clone(),
        schema: Some(synth),
        proj: (0..ncols).collect(),
        rows,
        prefilled: true,
    });
    for j in &joins {
        let schema = conn
            .sql_cache
            .borrow()
            .schemas
            .get(&(db.to_string(), j.table.table.clone()))
            .cloned();
        tables.push(JoinTable {
            table: std::sync::Arc::from(j.table.table.as_str()),
            alias: j.table.alias.clone(),
            schema,
            proj: Vec::new(),
            rows: Vec::new(),
            prefilled: false,
        });
    }
    let ctx = SqlJoinCtx {
        db,
        tables,
        joins,
        items,
        conds,
        order,
        limit,
        offset,
        phase: JoinPhase::Gather(0),
        remaining: 0,
    };
    conn.sql_join.insert(seq, ctx);
    sql_join_kickoff(conn, conn_id, seq, worker_id, shard_inboxes, num_shards);
}

/// ⭐ F72: 外层内存管线 — 列名解析 (剥 alias 前缀) → eval_pred 过滤 →
/// ORDER → OFFSET/LIMIT → 投影 (COUNT(*) 特判) → 渲染 (sysq_finish 同款先例,
/// 但保留内层真实列类型).
#[allow(clippy::too_many_arguments)]
fn derived_render(
    proto: ProtocolKind,
    binary: bool,
    alias: &str,
    items: &[sql::SelectItem],
    conds_in: &Pred<Cond>,
    order: &[(String, bool)],
    limit: Option<u32>,
    offset: Option<u32>,
    cols: &[(String, ColType)],
    mut rows: Vec<Vec<ColValue>>,
) -> Vec<u8> {
    if rows.len() > JOIN_MAX_ROWS {
        return sql_err_bytes(proto, "derived table too large (limit 262144 rows)");
    }
    // 列名解析: `t.x` / 裸 `x` — qualifier 仅接受 alias
    let resolve = |name: &str| -> Result<usize, String> {
        let qc = QualCol::parse(name);
        if let Some(q) = &qc.qualifier
            && !q.eq_ignore_ascii_case(alias)
        {
            return Err(format!("unknown table '{q}'"));
        }
        cols.iter()
            .position(|(n, _)| n.eq_ignore_ascii_case(&qc.col))
            .ok_or_else(|| format!("unknown column '{}'", qc.col))
    };
    // 合成 schema (内层真实列类型) 供 eval_pred; 叶子列名先剥前缀重写
    let schema = TableSchema {
        version: 1,
        columns: cols
            .iter()
            .map(|(n, t)| storage::schema::Column { name: n.clone(), ty: *t, nullable: true })
            .collect(),
        pk_col: 0,
        indexes: Vec::new(),
        next_iid: 0,
        version_ncols: Vec::new(),
    };
    let conds = match conds_in.try_map(&|c: &Cond| {
        let idx = resolve(&c.col)?;
        Ok::<_, String>(Cond {
            col: schema.columns[idx].name.clone(),
            op: c.op,
            val: c.val.clone(),
            set: c.set.clone(),
        })
    }) {
        Ok(p) => p,
        Err(e) => return sql_err_bytes(proto, &e),
    };
    rows.retain(|r| eval_pred(&schema, r, &conds));
    // ORDER BY (逆序叠加稳定排序 = 多键优先级)
    for (name, desc) in order.iter().rev() {
        match resolve(name) {
            Ok(ci) => rows.sort_by(|a, b| {
                let o = cmp_colvalue(&a[ci], &b[ci]);
                if *desc { o.reverse() } else { o }
            }),
            Err(e) => return sql_err_bytes(proto, &e),
        }
    }
    // OFFSET / LIMIT
    let start = (offset.unwrap_or(0) as usize).min(rows.len());
    let end = match limit {
        Some(l) => (start + l as usize).min(rows.len()),
        None => rows.len(),
    };
    let rows = &rows[start..end];
    // COUNT(*) 特判 (parse 已保证含 Agg 时必为孤 COUNT(*))
    if items.iter().any(|i| matches!(i, sql::SelectItem::Agg { .. })) {
        let cref = [("COUNT(*)", ColType::I64)];
        return sql_rows_bytes(proto, binary, &cref, &[vec![ColValue::I64(rows.len() as i64)]]);
    }
    // 投影: items 空 = 全列
    if items.is_empty() {
        let cref: Vec<(&str, ColType)> = cols.iter().map(|(n, t)| (n.as_str(), *t)).collect();
        return sql_rows_bytes(proto, binary, &cref, rows);
    }
    let mut idxs: Vec<usize> = Vec::with_capacity(items.len());
    for it in items {
        match it {
            sql::SelectItem::Col { name: c, .. } => match resolve(c) {
                Ok(i) => idxs.push(i),
                Err(e) => return sql_err_bytes(proto, &e),
            },
            sql::SelectItem::Agg { .. } => unreachable!("孤 COUNT(*) 已在上方特判"),
        }
    }
    let cref: Vec<(&str, ColType)> = idxs.iter().map(|&i| (cols[i].0.as_str(), cols[i].1)).collect();
    let proj: Vec<Vec<ColValue>> =
        rows.iter().map(|r| idxs.iter().map(|&i| r[i].clone()).collect()).collect();
    sql_rows_bytes(proto, binary, &cref, &proj)
}

#[allow(clippy::too_many_arguments)]
fn sql_dispatch_stmt(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    stmt: SqlStmt,
) {
    crate::metrics::SQL_QUERIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // ⭐ ORM-B2: DDL epoch 检查 — DROP/重建后本 worker 陈旧 schema 缓存整体
    // 失效 (一次 relaxed load 热路径; DDL 低频, 全量清空重拉可接受)
    {
        let ep = conn.sql_shared.ddl_epoch.load(std::sync::atomic::Ordering::Acquire);
        let mut cache = conn.sql_cache.borrow_mut();
        if cache.local_epoch != ep {
            cache.schemas.clear();
            cache.local_epoch = ep;
        }
    }
    match stmt {
        // ⭐ 事务 v1/v2: BEGIN / COMMIT / ROLLBACK / SAVEPOINT (conn 层状态机)
        SqlStmt::Begin { iso, read_only } => {
            if conn.txn.is_some() {
                // PG 行为: 警告+忽略 (不重置已有缓冲)
            } else {
                conn.txn = Some(TxnState::new(
                    iso.unwrap_or(conn.default_iso),
                    read_only.unwrap_or(conn.default_ro),
                ));
                conn.txn_failed = false;
            }
            conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
        }
        // ⭐ v2 (F62): SET [SESSION] TRANSACTION — session 改连接默认,
        // 否则改当前事务 (非事务中也落连接默认 — MySQL "下一个事务" 近似)
        SqlStmt::SetTransaction { iso, read_only, session } => {
            if !session && let Some(txn) = conn.txn.as_mut() {
                if let Some(i) = iso {
                    txn.iso = i;
                }
                if let Some(ro) = read_only {
                    txn.read_only = ro;
                }
            } else {
                if let Some(i) = iso {
                    conn.default_iso = i;
                }
                if let Some(ro) = read_only {
                    conn.default_ro = ro;
                }
            }
            conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
        }
        SqlStmt::Rollback => {
            conn.txn = None;
            conn.txn_failed = false;
            conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
        }
        // ⭐ v2 (F62): ROLLBACK TO — E 态下允许 (SQLAlchemy/psycopg 靠它恢复
        // aborted 子事务), 成功后清 failed 位
        SqlStmt::RollbackTo { name } => {
            let Some(txn) = conn.txn.as_mut() else {
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, &format!("savepoint \"{name}\" does not exist")),
                );
                return;
            };
            let Some(pos) = txn.savepoints.iter().rposition(|(n, _)| n == &name) else {
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, &format!("savepoint \"{name}\" does not exist")),
                );
                return;
            };
            let watermark = txn.savepoints[pos].1;
            txn.ops.truncate(watermark);
            txn.savepoints.truncate(pos + 1); // 保留自身 (PG 语义可重复回滚)
            // index 重建 (截断后下标失效)
            txn.index.clear();
            let entries: Vec<_> = txn
                .ops
                .iter()
                .enumerate()
                .map(|(i, op)| {
                    let (d, t, k) = op.locator();
                    ((d.to_string(), t.to_string(), k.to_vec()), i)
                })
                .collect();
            txn.index.extend(entries);
            conn.txn_failed = false;
            conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
        }
        SqlStmt::Commit => {
            let failed = conn.txn_failed;
            conn.txn_failed = false;
            match conn.txn.take() {
                // failed 事务的 COMMIT = 回滚 (PG 语义); 无事务/空事务 no-op
                None => conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0)),
                Some(_) if failed => conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0)),
                Some(txn) if txn.ops.is_empty() => {
                    // 纯读事务: 序列化点可取 BEGIN 时刻, 无需验证直接成功
                    conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
                }
                Some(txn) => {
                    // 按 shard 分组 → 每 shard 一个 TxnApply 原子批;
                    // ⭐ v2: read_set 同样按 pk 路由分组 (验证与写同批原子)
                    let mut groups: HashMap<usize, Vec<BatchOp>> = HashMap::new();
                    for op in txn.ops {
                        let sid = hash_route_op(&op, num_shards);
                        groups.entry(sid).or_default().push(op);
                    }
                    let mut checks: HashMap<usize, Vec<shard_manager::request::ReadCheck>> =
                        HashMap::new();
                    for ((d, t, pk), fp) in txn.read_set {
                        let sid = hash_route_key(&d, &t, &pk, num_shards);
                        checks.entry(sid).or_default().push(
                            shard_manager::request::ReadCheck { db: d, table: t, pk, fp },
                        );
                    }
                    // 并集 shard: 有写或有验证项都发 (纯验证批 ops 空)
                    let mut sids: Vec<usize> = groups.keys().chain(checks.keys()).copied().collect();
                    sids.sort_unstable();
                    sids.dedup();
                    conn.sql_txn_agg.insert(
                        seq,
                        SqlTxnAgg { remaining: sids.len(), applied: 0, error: None },
                    );
                    for (gidx, sid) in sids.into_iter().enumerate() {
                        push_task_grouped(
                            conn_id,
                            seq,
                            worker_id,
                            gidx as u32,
                            sid,
                            BatchOp::TxnApply {
                                ops: groups.remove(&sid).unwrap_or_default(),
                                read_set: checks.remove(&sid).unwrap_or_default(),
                            },
                            shard_inboxes,
                        );
                    }
                }
            }
        }
        // ⭐ 事务 v1 (F61): failed 事务拒后续 (PG 25P02 语义; MySQL 门面
        // 不置位故此臂仅 PG 命中; ROLLBACK TO 已在上方放行)
        _ if conn.txn_failed => {
            conn.resp_complete(
                seq,
                sql_err_bytes(
                    conn.proto,
                    "current transaction is aborted, commands ignored until end of transaction block",
                ),
            );
        }
        // ⭐ v2 (F62): SAVEPOINT / RELEASE (E 态被上方拦截 — PG 语义)
        SqlStmt::Savepoint { name } => match conn.txn.as_mut() {
            Some(txn) => {
                let watermark = txn.ops.len();
                txn.savepoints.push((name, watermark));
                conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
            }
            None => conn.resp_complete(
                seq,
                sql_err_bytes(conn.proto, "SAVEPOINT can only be used in transaction blocks"),
            ),
        },
        SqlStmt::Release { name } => match conn.txn.as_mut() {
            Some(txn) => match txn.savepoints.iter().rposition(|(n, _)| n == &name) {
                Some(pos) => {
                    txn.savepoints.remove(pos);
                    conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
                }
                None => conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, &format!("savepoint \"{name}\" does not exist")),
                ),
            },
            None => conn.resp_complete(
                seq,
                sql_err_bytes(conn.proto, "RELEASE can only be used in transaction blocks"),
            ),
        },
        // ⭐ v2 (F62): READ ONLY 事务拒写 (25006)
        SqlStmt::Insert { .. } | SqlStmt::Update { .. } | SqlStmt::Delete { .. }
            if conn.txn.as_ref().is_some_and(|t| t.read_only) =>
        {
            conn.resp_complete(
                seq,
                sql_err_bytes(
                    conn.proto,
                    "cannot execute write statement in a read-only transaction",
                ),
            );
        }
        // ⭐ 事务 v1 (F61): DDL 在事务中拒绝 (避免与 2PC 交叉)
        SqlStmt::CreateTable { .. } | SqlStmt::DropTable { .. } | SqlStmt::AlterTable { .. }
            if conn.txn.is_some() =>
        {
            conn.resp_complete(
                seq,
                sql_err_bytes(conn.proto, "DDL is not allowed inside a transaction"),
            );
        }
        // ⭐ S3: 工具命令 (worker 本地, 零任务)
        SqlStmt::SetStub => {
            conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
        }
        SqlStmt::VersionStub => {
            let ver: &[u8] = if conn.proto == ProtocolKind::Pg {
                b"PostgreSQL 16.0 (NexusDB)"
            } else {
                b"8.0.35-NexusDB"
            };
            let bin = conn.mysql_binary.remove(&seq);
            conn.resp_complete(
                seq,
                sql_rows_bytes(
                    conn.proto,
                    bin,
                    &[("version()", ColType::Str)],
                    &[vec![ColValue::Bytes(ver.to_vec())]],
                ),
            );
        }
        // ⭐ S5: SELECT DATABASE() — 当前库名单行
        SqlStmt::DatabaseStub => {
            let bin = conn.mysql_binary.remove(&seq);
            conn.resp_complete(
                seq,
                sql_rows_bytes(
                    conn.proto,
                    bin,
                    &[("DATABASE()", ColType::Str)],
                    &[vec![ColValue::Bytes(db.as_bytes().to_vec())]],
                ),
            );
        }
        // ⭐ F66: `SELECT @@var` 系统变量 — 回合理值单行 (SQLAlchemy 初始化)
        SqlStmt::SystemVarStub { vars } => {
            let bin = conn.mysql_binary.remove(&seq);
            let vals: Vec<(String, String)> = vars
                .iter()
                .map(|v| {
                    let key = v.rsplit('.').next().unwrap_or(v).to_ascii_lowercase();
                    let val = match key.as_str() {
                        "transaction_isolation" | "tx_isolation" => "READ-COMMITTED",
                        "version" => "8.0.0-nexusdb",
                        "version_comment" => "NexusDB",
                        "sql_mode" => "",
                        "lower_case_table_names" => "0",
                        "autocommit" => "1",
                        "max_allowed_packet" => "16777216",
                        "character_set_client" | "character_set_connection"
                        | "character_set_results" => "utf8mb4",
                        _ => "",
                    };
                    (format!("@@{v}"), val.to_string())
                })
                .collect();
            let cols: Vec<(&str, ColType)> =
                vals.iter().map(|(n, _)| (n.as_str(), ColType::Str)).collect();
            let row: Vec<ColValue> =
                vals.iter().map(|(_, val)| ColValue::Bytes(val.as_bytes().to_vec())).collect();
            conn.resp_complete(seq, sql_rows_bytes(conn.proto, bin, &cols, &[row]));
        }
        // ⭐ F66: 系统表查询 (information_schema / pg_catalog 虚拟表)
        SqlStmt::SystemQuery { catalog, table, cols, conds, order, limit, offset } => {
            let spec = SysQuerySpec { catalog, table, cols, conds, order, limit, offset };
            // 纯 db 列表的虚拟表 (schemata / pg_namespace) → 零任务直接合成;
            // 需表/列元数据的 → 发 CatalogDump 挂起
            if spec.needs_catalog() {
                conn.sql_sysq.insert(seq, spec);
                let op = BatchOp::CatalogDump { db: db.clone() };
                let sid = hash_route_key(db, "", &[], num_shards);
                push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
            } else {
                let dbs: Vec<String> =
                    db_view.all_names().iter().map(|s| s.to_string()).collect();
                // default 库隐式不入 resolver — 补入
                let mut dbs = dbs;
                if !dbs.iter().any(|d| d.as_str() == default_db.as_ref()) {
                    dbs.push(default_db.to_string());
                }
                let bin = conn.mysql_binary.remove(&seq);
                let bytes = sysq_render_dblist(conn.proto, bin, &spec, &dbs);
                conn.resp_complete(seq, bytes);
            }
        }
        SqlStmt::Use { db: name } => {
            // 校验存在 (default 库隐式不入 resolver, 特判)
            if name.as_str() == default_db.as_ref() || db_view.id_of(&name).is_some() {
                conn.current_db = std::sync::Arc::from(name.as_str());
                conn.resp_complete(seq, sql_ok_bytes(conn.proto, 0));
            } else {
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, &format!("Unknown database '{name}'")),
                );
            }
        }
        SqlStmt::CreateTable { table, schema } => {
            let bytes = schema.encode();
            let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
            conn.sql_ddl_agg.insert(
                seq,
                SqlDdlAgg {
                    remaining: num_shards,
                    error: None,
                    key: (db.to_string(), table),
                    schema: std::sync::Arc::new(schema),
                    alter: false,
                },
            );
            // 数据面广播 (worker 不持控制面); shard 端惰性建表 + set_schema 幂等
            for sid in 0..num_shards {
                let op = BatchOp::SetSchemaOp {
                    db: db.clone(),
                    table: table_arc.clone(),
                    bytes: bytes.clone(),
                };
                push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
            }
        }
        SqlStmt::Insert { ref table, .. }
        | SqlStmt::Select { ref table, .. }
        | SqlStmt::Delete { ref table, .. }
        | SqlStmt::Update { ref table, .. }
        | SqlStmt::AlterTable { ref table, .. }
        | SqlStmt::Describe { ref table } => {
            // ⭐ F71: WHERE 子查询 — 先顺序跑内层折叠, 完后重跑外层 (仅 Select/Delete/Update)
            if matches!(
                stmt,
                SqlStmt::Select { .. } | SqlStmt::Delete { .. } | SqlStmt::Update { .. }
            ) && sql_subq_start(
                conn, conn_id, seq, worker_id, db, default_db, db_view, shard_inboxes,
                num_shards, &stmt,
            ) {
                return;
            }
            let key = (db.to_string(), table.clone());
            // ⭐ W1: worker 级共享缓存 (borrow 局部化: 取 Arc 即还)
            let cached = conn.sql_cache.borrow().schemas.get(&key).cloned();
            if let Some(schema) = cached {
                sql_run_dml(conn, conn_id, seq, worker_id, db, shard_inboxes, num_shards, schema, stmt);
            } else {
                // schema miss: 挂起语句, 先拉 schema (GetSchemaOp 定向单 shard)
                let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
                let table_name = table.clone();
                conn.sql_pending.insert(seq, PendingSql { stmt, db: db.clone(), table: table_name });
                let op = BatchOp::GetSchemaOp { db: db.clone(), table: table_arc };
                push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
            }
        }
        // ⭐ F67 (JOIN): 两表 hash join — 建 ctx → 补 schema/gather 顺序启动
        SqlStmt::SelectJoin { from, from_inner, joins, items, conds, order, limit, offset } => {
            // ⭐ F75: 首表为派生表 → 先物化内层 (同 seq 完成点拦截), 完后 finish_derived 建 JOIN
            if let Some(inner) = from_inner {
                if !matches!(*inner, SqlStmt::Select { .. }) {
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(conn.proto, "derived-table inner must be a simple SELECT (v1)"),
                    );
                    return;
                }
                if let Some(p) = stmt_where_conds(&inner) {
                    let mut nested = Vec::new();
                    collect_pred_subq(p, &mut nested);
                    if !nested.is_empty() {
                        conn.resp_complete(
                            seq,
                            sql_err_bytes(conn.proto, "subquery inside derived table not supported (v1)"),
                        );
                        return;
                    }
                }
                let join_stmt = SqlStmt::SelectJoin {
                    from, from_inner: None, joins, items, conds, order, limit, offset,
                };
                conn.sql_derived.insert(seq, DerivedCtx::JoinFrom { db: db.clone(), join_stmt });
                sql_dispatch_stmt(
                    conn, conn_id, seq, worker_id, db, default_db, db_view, shard_inboxes,
                    num_shards, *inner,
                );
                return;
            }
            // 构建 tables 列表 (from + 各 join.table); schema 命中缓存则填
            let mut tables: Vec<JoinTable> = Vec::with_capacity(joins.len() + 1);
            for tr in std::iter::once(&from).chain(joins.iter().map(|j| &j.table)) {
                let schema = conn
                    .sql_cache
                    .borrow()
                    .schemas
                    .get(&(db.to_string(), tr.table.clone()))
                    .cloned();
                tables.push(JoinTable {
                    table: std::sync::Arc::from(tr.table.as_str()),
                    alias: tr.alias.clone(),
                    schema,
                    proj: Vec::new(),
                    rows: Vec::new(),
                    prefilled: false,
                });
            }
            let ctx = SqlJoinCtx {
                db: db.clone(),
                tables,
                joins,
                items,
                conds,
                order,
                limit,
                offset,
                phase: JoinPhase::Gather(0),
                remaining: 0,
            };
            conn.sql_join.insert(seq, ctx);
            sql_join_kickoff(conn, conn_id, seq, worker_id, shard_inboxes, num_shards);
        }
        // ⭐ F72: FROM 派生表 — 内层先物化 (同 seq 完成点拦截), 完后 finish_derived
        // 在 worker 内存执行外层 (过滤/投影/排序/截断; 不下推 shard)
        SqlStmt::SelectDerived { inner, alias, items, conds, order, limit, offset } => {
            // v1: 内层仅单表 SELECT (非 JOIN/系统表) — 否则绕过完成点拦截
            if !matches!(*inner, SqlStmt::Select { .. }) {
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, "derived-table inner must be a simple SELECT (v1)"),
                );
                return;
            }
            // v1: 内层不得再带 WHERE 子查询 (双层编排留后)
            if let Some(p) = stmt_where_conds(&inner) {
                let mut nested = Vec::new();
                collect_pred_subq(p, &mut nested);
                if !nested.is_empty() {
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(conn.proto, "subquery inside derived table not supported (v1)"),
                    );
                    return;
                }
            }
            conn.sql_derived.insert(seq, DerivedCtx::Standalone { alias, items, conds, order, limit, offset });
            sql_dispatch_stmt(
                conn, conn_id, seq, worker_id, db, default_db, db_view, shard_inboxes,
                num_shards, *inner,
            );
        }
        // ⭐ S1: DROP TABLE — 无需 schema, 数据面广播删表
        SqlStmt::DropTable { table } => {
            conn.sql_dml_agg.insert(
                seq,
                SqlDmlAgg {
                    remaining: num_shards,
                    affected: 0,
                    error: None,
                    drop_key: Some((db.to_string(), table.clone())),
                },
            );
            let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
            for sid in 0..num_shards {
                let op = BatchOp::DropTableOp { db: db.clone(), table: table_arc.clone() };
                push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
            }
        }
    }
}

/// ⭐ F68 (JOIN): 限定列 → (table_index, col_idx). 未知限定符/列/歧义 → Err.
fn sql_join_resolve(ctx: &SqlJoinCtx, qc: &QualCol) -> Result<(usize, u16), String> {
    match &qc.qualifier {
        Some(q) => {
            let ti = ctx
                .tables
                .iter()
                .position(|t| t.alias.eq_ignore_ascii_case(q))
                .ok_or_else(|| format!("unknown table qualifier '{q}'"))?;
            let sc = ctx.tables[ti].schema.as_ref().expect("schema ready");
            sc.col_by_name(&qc.col)
                .map(|i| (ti, i))
                .ok_or_else(|| format!("unknown column '{}.{}'", q, qc.col))
        }
        None => {
            let mut found: Option<(usize, u16)> = None;
            for (ti, t) in ctx.tables.iter().enumerate() {
                let sc = t.schema.as_ref().expect("schema ready");
                if let Some(i) = sc.col_by_name(&qc.col) {
                    if found.is_some() {
                        return Err(format!("ambiguous column '{}' (qualify it)", qc.col));
                    }
                    found = Some((ti, i));
                }
            }
            found.ok_or_else(|| format!("unknown column '{}'", qc.col))
        }
    }
}

/// ⭐ F68 (JOIN): ON 操作数解析 (未限定名优先前序表, 支持 USING 糖糖).
/// rt = 本次新表下标; 限定名 → 常规解析; 未限定 → tables[0..rt] 取最后一个, 否则 rt.
fn sql_join_resolve_on(ctx: &SqlJoinCtx, qc: &QualCol, rt: usize) -> Result<(usize, u16), String> {
    if qc.qualifier.is_some() {
        return sql_join_resolve(ctx, qc);
    }
    let mut found: Option<(usize, u16)> = None;
    for ti in 0..rt {
        let sc = ctx.tables[ti].schema.as_ref().expect("schema ready");
        if let Some(i) = sc.col_by_name(&qc.col) {
            found = Some((ti, i));
        }
    }
    if found.is_none() {
        let sc = ctx.tables[rt].schema.as_ref().expect("schema ready");
        if let Some(i) = sc.col_by_name(&qc.col) {
            found = Some((rt, i));
        }
    }
    found.ok_or_else(|| format!("unknown column '{}'", qc.col))
}

/// ⭐ F68 (JOIN): 规划 — 校验所有限定名 + 算每表下推投影列 (含 items/on/where/order 引用).
/// 返回每表 proj (items 空 `*` → 各表全列). 同时校验每个 ON 等值恰好引用本次新表.
fn sql_join_plan(ctx: &SqlJoinCtx) -> Result<Vec<Vec<u16>>, String> {
    let n = ctx.tables.len();
    let mut sets: Vec<std::collections::BTreeSet<u16>> = vec![Default::default(); n];
    // ON 键/残余 (并校验 Eq 引用新表)
    for (ji, jc) in ctx.joins.iter().enumerate() {
        let rt = ji + 1;
        for on in &jc.on {
            match on {
                sql::OnPred::Eq(l, r) => {
                    let (lt, li) = sql_join_resolve_on(ctx, l, rt)?;
                    let (rtt, ri) = sql_join_resolve_on(ctx, r, rt)?;
                    let one_new = (lt == rt) ^ (rtt == rt);
                    if !one_new {
                        return Err("JOIN ON equality must reference the joined table".into());
                    }
                    sets[lt].insert(li);
                    sets[rtt].insert(ri);
                }
                sql::OnPred::Cmp { left, right, .. } => {
                    let (lt, li) = sql_join_resolve_on(ctx, left, rt)?;
                    let (rt2, ri) = sql_join_resolve_on(ctx, right, rt)?;
                    sets[lt].insert(li);
                    sets[rt2].insert(ri);
                }
            }
        }
    }
    // 投影项
    if ctx.items.is_empty() {
        for (ti, t) in ctx.tables.iter().enumerate() {
            let sc = t.schema.as_ref().expect("schema ready");
            for i in 0..sc.columns.len() as u16 {
                sets[ti].insert(i);
            }
        }
    } else {
        for it in &ctx.items {
            let JoinItem::Col(qc) = it;
            let (ti, i) = sql_join_resolve(ctx, qc)?;
            sets[ti].insert(i);
        }
    }
    // WHERE / ORDER 引用列
    for c in ctx.conds.leaves() {
        let (ti, i) = sql_join_resolve(ctx, &c.col)?;
        sets[ti].insert(i);
    }
    for (qc, _) in &ctx.order {
        let (ti, i) = sql_join_resolve(ctx, qc)?;
        sets[ti].insert(i);
    }
    Ok(sets.into_iter().map(|s| s.into_iter().collect()).collect())
}

/// ⭐ F68 (JOIN): 启动/推进 — 补第一个缺失 schema, 否则规划并从表 0 开始 gather.
fn sql_join_kickoff(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    let need = {
        let c = conn.sql_join.get(&seq).expect("join ctx");
        c.tables.iter().position(|t| t.schema.is_none())
    };
    if let Some(idx) = need {
        let (db, table) = {
            let c = conn.sql_join.get_mut(&seq).unwrap();
            c.phase = JoinPhase::FetchSchema(idx);
            c.remaining = 1;
            (c.db.clone(), c.tables[idx].table.clone())
        };
        let sid = hash_route_key(&db, &table, &[], num_shards);
        let op = BatchOp::GetSchemaOp { db, table };
        push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
        return;
    }
    // schema 全就绪 → 规划
    let plan = sql_join_plan(conn.sql_join.get(&seq).expect("join ctx"));
    match plan {
        Err(e) => {
            conn.sql_join.remove(&seq);
            conn.mysql_binary.remove(&seq);
            conn.resp_complete(seq, sql_err_bytes(conn.proto, &e));
        }
        Ok(projs) => {
            let start = {
                let c = conn.sql_join.get_mut(&seq).unwrap();
                for (t, p) in c.tables.iter_mut().zip(projs) {
                    if t.prefilled {
                        // ⭐ F75: 预填表行已定宽 (全列) → proj 强制 identity, 不清空 rows
                        let ncols = t.schema.as_ref().unwrap().columns.len() as u16;
                        t.proj = (0..ncols).collect();
                    } else {
                        t.proj = p;
                    }
                }
                // ⭐ F75: 从第一个非预填表开始 gather (预填表 0 跳过)
                c.tables.iter().position(|t| !t.prefilled)
            };
            match start {
                Some(idx) => {
                    {
                        let c = conn.sql_join.get_mut(&seq).unwrap();
                        c.phase = JoinPhase::Gather(idx);
                        c.remaining = num_shards;
                        c.tables[idx].rows.clear();
                    }
                    sql_join_broadcast(conn, conn_id, seq, worker_id, shard_inboxes, num_shards, idx);
                }
                // 全部预填 (理论不可达: joins 非空) → 直接 finish
                None => sql_join_finish(conn, seq),
            }
        }
    }
}

/// ⭐ F68 (JOIN): 广播 tables[idx] 的 ScanFiltered (下推该表 WHERE 谓词).
/// 下推仅优化; finish 总会再残余过滤全 WHERE, 故对任何表下推均安全 (含外连接可空侧).
fn sql_join_broadcast(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    idx: usize,
) {
    let (db, table, preds, proj) = {
        let c = conn.sql_join.get(&seq).expect("join ctx");
        let t = &c.tables[idx];
        let schema = t.schema.as_ref().unwrap();
        let mut preds: Vec<shard_manager::ScanPred> = Vec::new();
        // ⭐ F69: 仅纯 AND 合取时下推 (含 OR/NOT → 空 preds 全扫, finish 递归残余保正确)
        for cond in c.conds.as_conjuncts().unwrap_or_default() {
            let Ok((ti, cidx)) = sql_join_resolve(c, &cond.col) else { continue };
            if ti != idx {
                continue;
            }
            let ty = schema.columns[cidx as usize].ty;
            let op = match cond.op {
                CmpOp::Eq => shard_manager::PredOp::Eq,
                CmpOp::Ne => shard_manager::PredOp::Ne,
                CmpOp::Gt => shard_manager::PredOp::Gt,
                CmpOp::Ge => shard_manager::PredOp::Ge,
                CmpOp::Lt => shard_manager::PredOp::Lt,
                CmpOp::Le => shard_manager::PredOp::Le,
                CmpOp::In => shard_manager::PredOp::In,
            };
            if cond.op == CmpOp::In {
                let set: Vec<ColValue> =
                    cond.set.iter().filter_map(|v| sql_to_col(ty, v).ok()).collect();
                if set.len() == cond.set.len() {
                    preds.push(shard_manager::ScanPred { col: cidx, op, val: ColValue::Null, set });
                }
            } else if let Ok(val) = sql_to_col(ty, &cond.val) {
                preds.push(shard_manager::ScanPred { col: cidx, op, val, set: Vec::new() });
            }
        }
        (c.db.clone(), t.table.clone(), preds, t.proj.clone())
    };
    // ⭐ F68: 索引驱动提示 — 该表任一可索引列的 Eq/范围谓词 → 范围扫 (Eq 优先)
    // ⭐ F70: key_set_hint 优先 (前序表 join 键集合 → 索引点查); 命中时不再用 index_hint
    let key_set_hint = sql_join_keyset_hint(conn.sql_join.get(&seq).expect("join ctx"), idx);
    let index_hint = if key_set_hint.is_some() {
        None
    } else {
        let c = conn.sql_join.get(&seq).expect("join ctx");
        let t = &c.tables[idx];
        let schema = t.schema.as_ref().unwrap();
        sql_join_index_hint(c, idx, schema)
    };
    for sid in 0..num_shards {
        let op = BatchOp::ScanFiltered {
            db: db.clone(),
            table: table.clone(),
            preds: preds.clone(),
            proj: proj.clone(),
            index_hint: index_hint.clone(),
            key_set_hint: key_set_hint.clone(),
            limit: 0,
        };
        push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
    }
}

/// ⭐ F70 (JOIN): 键集合下推决策 — idx>=1 且满足安全条件时, 从前序表抽取
/// ON 等值键值集合下推为索引点查. 启用条件:
/// - joins[idx-1].kind ∈ {Inner, Left} (RIGHT/FULL/CROSS 禁用: 语义不能丢未匹配行)
/// - 息含单个 OnPred::Eq (多列组合键 v1 跳过)
/// - Eq 一侧属 idx 表且该列有普通二级索引, 另一侧属前序表 ti<idx
/// - 前序键集合去重后 <= JOIN_KEYSET_MAX (超阈退回全表扫)
fn sql_join_keyset_hint(ctx: &SqlJoinCtx, idx: usize) -> Option<shard_manager::KeySetHint> {
    if idx == 0 {
        return None;
    }
    let jc = &ctx.joins[idx - 1];
    if !matches!(jc.kind, JoinKind::Inner | JoinKind::Left) {
        return None;
    }
    // 息含单个 Eq
    let eqs: Vec<&sql::OnPred> =
        jc.on.iter().filter(|o| matches!(o, sql::OnPred::Eq(..))).collect();
    if eqs.len() != 1 {
        return None;
    }
    let sql::OnPred::Eq(l, r) = eqs[0] else { return None };
    // resolve 两侧 → (表下标, 列号)
    let (lt, li) = sql_join_resolve_on(ctx, l, idx).ok()?;
    let (rt, ri) = sql_join_resolve_on(ctx, r, idx).ok()?;
    // 分辨新表侧 (idx) 与前序表侧 (ti<idx)
    let (new_col, prev_ti, prev_col) = if lt == idx && rt < idx {
        (li, rt, ri)
    } else if rt == idx && lt < idx {
        (ri, lt, li)
    } else {
        return None;
    };
    // 新表 join 列需有普通二级索引
    let schema = ctx.tables[idx].schema.as_ref()?;
    let iid = schema.indexes.iter().find(|i| i.col == new_col).map(|i| i.iid)?;
    // 前序表 prev_col 在其 proj 中的位置
    let prev_tab = &ctx.tables[prev_ti];
    let pos = prev_tab.proj.iter().position(|&c| c == prev_col)?;
    // 抽取去重键值 (跳 NULL); 超阈 → 退回
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut keys: Vec<ColValue> = Vec::new();
    for row in &prev_tab.rows {
        let cv = &row[pos];
        let Some(kb) = join_key(cv) else { continue }; // NULL 不入键集
        if seen.insert(kb) {
            keys.push(cv.clone());
            if keys.len() > JOIN_KEYSET_MAX {
                return None; // 超阈退回全表扫
            }
        }
    }
    Some(shard_manager::KeySetHint { iid, keys })
}

/// ⭐ F68 (JOIN): 为 tables[idx] 选一个可索引谓词产索引提示 (Eq 优先, 否则范围).
/// lo/hi 为过度近似闭界 (Gt/Lt 也用含界, 由残余 preds 精确); 无可用 → None.
fn sql_join_index_hint(
    ctx: &SqlJoinCtx,
    idx: usize,
    schema: &TableSchema,
) -> Option<shard_manager::IndexHint> {
    // 列号 → iid (仅取非全局普通二级索引即可)
    let iid_of = |col: u16| schema.indexes.iter().find(|i| i.col == col).map(|i| i.iid);
    let mut best: Option<shard_manager::IndexHint> = None;
    for cond in ctx.conds.as_conjuncts().unwrap_or_default() {
        let Ok((ti, cidx)) = sql_join_resolve(ctx, &cond.col) else { continue };
        if ti != idx {
            continue;
        }
        let Some(iid) = iid_of(cidx) else { continue };
        let ty = schema.columns[cidx as usize].ty;
        let Ok(v) = sql_to_col(ty, &cond.val) else { continue };
        match cond.op {
            CmpOp::Eq => {
                // Eq 最优: 直接定界返回
                return Some(shard_manager::IndexHint {
                    iid,
                    lo: Some(v.clone()),
                    hi: Some(v),
                });
            }
            CmpOp::Gt | CmpOp::Ge if best.is_none() => {
                best = Some(shard_manager::IndexHint { iid, lo: Some(v), hi: None });
            }
            CmpOp::Lt | CmpOp::Le if best.is_none() => {
                best = Some(shard_manager::IndexHint { iid, lo: None, hi: Some(v) });
            }
            _ => {}
        }
    }
    best
}

/// ⭐ F67 (JOIN): handle_resp 认领 — 按 phase 推进. 返回 true = 已处理此 seq.
fn sql_join_drive(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    result: &BatchResult,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) -> bool {
    if !conn.sql_join.contains_key(&seq) {
        return false;
    }
    // 错误: 直接终止
    if let BatchResult::Error(e) = result {
        let msg = e.clone();
        conn.sql_join.remove(&seq);
        conn.mysql_binary.remove(&seq);
        conn.resp_complete(seq, sql_err_bytes(conn.proto, &msg));
        return true;
    }
    let phase = conn.sql_join.get(&seq).unwrap().phase;
    match phase {
        JoinPhase::FetchSchema(idx) => {
            let bytes = match result {
                BatchResult::GetValue(Some(b)) => b.clone(),
                BatchResult::GetValue(None) => {
                    conn.sql_join.remove(&seq);
                    conn.mysql_binary.remove(&seq);
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(conn.proto, "table has no schema (not a SQL table)"),
                    );
                    return true;
                }
                _ => {
                    conn.sql_join.remove(&seq);
                    conn.mysql_binary.remove(&seq);
                    conn.resp_complete(seq, sql_err_bytes(conn.proto, "unexpected schema reply"));
                    return true;
                }
            };
            match TableSchema::decode(&bytes) {
                Ok(s) => {
                    let schema = std::sync::Arc::new(s);
                    let (db, table) = {
                        let c = conn.sql_join.get_mut(&seq).unwrap();
                        c.tables[idx].schema = Some(schema.clone());
                        (c.db.clone(), c.tables[idx].table.clone())
                    };
                    conn.sql_cache
                        .borrow_mut()
                        .schemas
                        .insert((db.to_string(), table.to_string()), schema);
                    // 继续补下一个或进 gather
                    sql_join_kickoff(conn, conn_id, seq, worker_id, shard_inboxes, num_shards);
                }
                Err(e) => {
                    conn.sql_join.remove(&seq);
                    conn.mysql_binary.remove(&seq);
                    conn.resp_complete(seq, sql_err_bytes(conn.proto, &format!("bad schema: {e}")));
                }
            }
            true
        }
        JoinPhase::Gather(idx) => {
            let rows = match result {
                BatchResult::ProjRows(r) => r.clone(),
                _ => Vec::new(),
            };
            let (done, overflow) = {
                let c = conn.sql_join.get_mut(&seq).unwrap();
                c.tables[idx].rows.extend(rows);
                c.remaining = c.remaining.saturating_sub(1);
                let of = c.tables[idx].rows.len() > JOIN_MAX_ROWS;
                (c.remaining == 0, of)
            };
            if overflow {
                conn.sql_join.remove(&seq);
                conn.mysql_binary.remove(&seq);
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, "JOIN input too large (row cap exceeded)"),
                );
                return true;
            }
            if done {
                let ntables = conn.sql_join.get(&seq).unwrap().tables.len();
                if idx + 1 < ntables {
                    {
                        let c = conn.sql_join.get_mut(&seq).unwrap();
                        c.phase = JoinPhase::Gather(idx + 1);
                        c.remaining = num_shards;
                        c.tables[idx + 1].rows.clear();
                    }
                    sql_join_broadcast(conn, conn_id, seq, worker_id, shard_inboxes, num_shards, idx + 1);
                } else {
                    sql_join_finish(conn, seq);
                }
            }
            true
        }
    }
}

/// ⭐ F68 (JOIN): 各表 gather 完成 → 左深迭代 hash join (右建表、左探测) +
/// 各 kind (Inner/Left/Right/Full/Cross) + ON 残余 + 残余 WHERE + 输出列 + ORDER/OFFSET/LIMIT.
fn sql_join_finish(conn: &mut ConnState, seq: u64) {
    let ctx = conn.sql_join.remove(&seq).expect("join ctx");
    let bin = conn.mysql_binary.remove(&seq);
    let n = ctx.tables.len();
    // 宽行列偏移: col_offset[t] = 表 t 列在宽行的起始; 表宽 = proj.len()
    let mut col_offset = vec![0usize; n + 1];
    for t in 0..n {
        col_offset[t + 1] = col_offset[t] + ctx.tables[t].proj.len();
    }
    let pos_in = |t: usize, cidx: u16| -> usize {
        ctx.tables[t].proj.iter().position(|&c| c == cidx).unwrap()
    };
    let wide_pos = |t: usize, cidx: u16| -> usize { col_offset[t] + pos_in(t, cidx) };

    // acc = 表 0 行 (宽度 = col_offset[1]); 逐 join 折叠
    let mut acc: Vec<Vec<ColValue>> = ctx.tables[0].rows.clone();
    for (ji, jc) in ctx.joins.iter().enumerate() {
        let rt = ji + 1;
        let acc_w = col_offset[rt];
        let right_pw = ctx.tables[rt].proj.len();
        // ON 等值键: (acc 宽位, right proj 位); ON 非等值残余: Cmp
        let mut eq_keys: Vec<(usize, usize)> = Vec::new();
        for on in &jc.on {
            if let sql::OnPred::Eq(l, r) = on {
                let (lt, li) = sql_join_resolve_on(&ctx, l, rt).unwrap();
                let (_rtt, ri) = sql_join_resolve_on(&ctx, r, rt).unwrap();
                if lt == rt {
                    // l 属新表, r 属 acc
                    eq_keys.push((wide_pos(_rtt, ri), pos_in(rt, li)));
                } else {
                    // l 属 acc, r 属新表
                    eq_keys.push((wide_pos(lt, li), pos_in(rt, ri)));
                }
            }
        }
        // 右表建 hash: 组合键 → 右行下标
        let right_rows = &ctx.tables[rt].rows;
        let mut hash: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        if !eq_keys.is_empty() {
            for (ri, row) in right_rows.iter().enumerate() {
                if let Some(k) = join_key_multi(row, eq_keys.iter().map(|&(_, rp)| rp)) {
                    hash.entry(k).or_default().push(ri);
                }
            }
        }
        // ON 残余 Cmp 判定 (acc_row + right_row)
        let on_cmp_pass = |acc_row: &[ColValue], right_row: &[ColValue]| -> bool {
            for on in &jc.on {
                if let sql::OnPred::Cmp { left, op, right } = on {
                    let (lt, li) = sql_join_resolve_on(&ctx, left, rt).unwrap();
                    let (rtt, ri) = sql_join_resolve_on(&ctx, right, rt).unwrap();
                    let lv = if lt == rt { &right_row[pos_in(rt, li)] } else { &acc_row[wide_pos(lt, li)] };
                    let rv = if rtt == rt { &right_row[pos_in(rt, ri)] } else { &acc_row[wide_pos(rtt, ri)] };
                    if !join_cmp_cols(lv, *op, rv) {
                        return false;
                    }
                }
            }
            true
        };
        let extend = |acc_row: &[ColValue], right_row: Option<&Vec<ColValue>>| -> Vec<ColValue> {
            let mut w = Vec::with_capacity(acc_w + right_pw);
            w.extend_from_slice(acc_row);
            match right_row {
                Some(r) => w.extend_from_slice(r),
                None => w.extend(std::iter::repeat_n(ColValue::Null, right_pw)),
            }
            w
        };
        let mut new_acc: Vec<Vec<ColValue>> = Vec::new();
        let mut matched_right = vec![false; right_rows.len()];
        for acc_row in &acc {
            if jc.kind == JoinKind::Cross {
                for right_row in right_rows.iter() {
                    new_acc.push(extend(acc_row, Some(right_row)));
                }
                continue;
            }
            let key = join_key_multi(acc_row, eq_keys.iter().map(|&(ap, _)| ap));
            let mut any = false;
            if let Some(k) = key
                && let Some(cands) = hash.get(&k)
            {
                for &ri in cands {
                    if on_cmp_pass(acc_row, &right_rows[ri]) {
                        new_acc.push(extend(acc_row, Some(&right_rows[ri])));
                        matched_right[ri] = true;
                        any = true;
                    }
                }
            }
            if !any
                && matches!(jc.kind, JoinKind::Left | JoinKind::Full)
            {
                new_acc.push(extend(acc_row, None));
            }
        }
        // RIGHT/FULL: 未匹配右行 → NULL acc 前缀 + 右行
        if matches!(jc.kind, JoinKind::Right | JoinKind::Full) {
            for (ri, m) in matched_right.iter().enumerate() {
                if !*m {
                    let mut w = vec![ColValue::Null; acc_w];
                    w.extend_from_slice(&right_rows[ri]);
                    new_acc.push(w);
                }
            }
        }
        if new_acc.len() > JOIN_MAX_ROWS {
            conn.resp_complete(
                seq,
                sql_err_bytes(conn.proto, "JOIN result too large (row cap exceeded)"),
            );
            return;
        }
        acc = new_acc;
    }

    // 残余 WHERE (全 conds 递归; null 扩展位由 NULL→false 天然过滤, 保外连接标准语义)
    acc.retain(|row| eval_join_pred(&ctx, row, &wide_pos, &ctx.conds));
    // ORDER BY (倒序逐键稳定排序)
    for (qc, desc) in ctx.order.iter().rev() {
        if let Ok((t, idx)) = sql_join_resolve(&ctx, qc) {
            let wp = wide_pos(t, idx);
            acc.sort_by(|a, b| {
                let o = cmp_colvalue(&a[wp], &b[wp]);
                if *desc { o.reverse() } else { o }
            });
        }
    }
    // OFFSET / LIMIT
    let start = (ctx.offset.unwrap_or(0) as usize).min(acc.len());
    let end = match ctx.limit {
        Some(l) => (start + l as usize).min(acc.len()),
        None => acc.len(),
    };
    let out_rows = &acc[start..end];
    // 输出列计划: (列头, wide_pos)
    let mut out_plan: Vec<(String, usize)> = Vec::new();
    if ctx.items.is_empty() {
        for (t, jt) in ctx.tables.iter().enumerate() {
            let sc = jt.schema.as_ref().unwrap();
            for (i, col) in sc.columns.iter().enumerate() {
                out_plan.push((format!("{}.{}", jt.alias, col.name), wide_pos(t, i as u16)));
            }
        }
    } else {
        for it in &ctx.items {
            let JoinItem::Col(qc) = it;
            let (t, idx) = sql_join_resolve(&ctx, qc).unwrap();
            let label = match &qc.qualifier {
                Some(q) => format!("{}.{}", q, qc.col),
                None => qc.col.clone(),
            };
            out_plan.push((label, wide_pos(t, idx)));
        }
    }
    // 列类型: 由 wide_pos 反查所属表/列 (out_plan 已存 wide_pos; 再算 ty)
    // 直接从 out_plan 重算: 找 (t,localpos) s.t. col_offset[t] <= wp < col_offset[t+1]
    let ty_of = |wp: usize| -> ColType {
        let t = (0..n).rev().find(|&t| col_offset[t] <= wp).unwrap();
        let local = wp - col_offset[t];
        let cidx = ctx.tables[t].proj[local];
        ctx.tables[t].schema.as_ref().unwrap().columns[cidx as usize].ty
    };
    let cols: Vec<(&str, ColType)> =
        out_plan.iter().map(|(label, wp)| (label.as_str(), ty_of(*wp))).collect();
    let rows: Vec<Vec<ColValue>> = out_rows
        .iter()
        .map(|row| out_plan.iter().map(|(_, wp)| row[*wp].clone()).collect())
        .collect();
    conn.resp_complete(seq, sql_rows_bytes(conn.proto, bin, &cols, &rows));
}

/// ⭐ F68 (JOIN): 组合键 — 按给定位置序拼接各列 join_key; 任一 NULL → None (不匹配).
fn join_key_multi(
    row: &[ColValue],
    positions: impl Iterator<Item = usize>,
) -> Option<Vec<u8>> {
    let mut key = Vec::new();
    for p in positions {
        let part = join_key(&row[p])?;
        key.extend_from_slice(&(part.len() as u32).to_le_bytes());
        key.extend_from_slice(&part);
    }
    Some(key)
}

/// ⭐ F67 (JOIN): join key 规范化字节 (类型 tag + 值; NULL → None 不匹配).
fn join_key(cv: &ColValue) -> Option<Vec<u8>> {
    match cv {
        ColValue::Null => None,
        ColValue::I64(i) => {
            let mut k = Vec::with_capacity(9);
            k.push(0);
            k.extend_from_slice(&i.to_le_bytes());
            Some(k)
        }
        ColValue::F64(f) => {
            let mut k = Vec::with_capacity(9);
            k.push(1);
            k.extend_from_slice(&f.to_bits().to_le_bytes());
            Some(k)
        }
        ColValue::Bytes(b) => {
            let mut k = Vec::with_capacity(1 + b.len());
            k.push(2);
            k.extend_from_slice(b);
            Some(k)
        }
        // ⭐ F81: Decimal join key (tag 3 + 16B i128 LE)
        ColValue::Decimal(x, _) => {
            let mut k = Vec::with_capacity(17);
            k.push(3);
            k.extend_from_slice(&x.to_le_bytes());
            Some(k)
        }
    }
}

/// ⭐ F67 (JOIN): 单条 WHERE 残余判定 (NULL 列恒 false, 与 sql_eval_conds 同义).
fn join_cond_pass(cv: &ColValue, cond: &JoinCond) -> bool {
    use std::cmp::Ordering;
    if cond.op == CmpOp::In {
        return cond.set.iter().any(|v| sql_cmp(cv, v) == Some(Ordering::Equal));
    }
    match sql_cmp(cv, &cond.val) {
        None => false,
        Some(o) => match cond.op {
            CmpOp::Eq => o == Ordering::Equal,
            CmpOp::Ne => o != Ordering::Equal,
            CmpOp::Gt => o == Ordering::Greater,
            CmpOp::Ge => o != Ordering::Less,
            CmpOp::Lt => o == Ordering::Less,
            CmpOp::Le => o != Ordering::Greater,
            CmpOp::In => unreachable!(),
        },
    }
}

/// ⭐ F69: JOIN WHERE 谓词树递归求值 (叶子 resolve 限定列 → 宽行取值判定).
fn eval_join_pred(
    ctx: &SqlJoinCtx,
    row: &[ColValue],
    wide_pos: &impl Fn(usize, u16) -> usize,
    pred: &Pred<JoinCond>,
) -> bool {
    match pred {
        Pred::Leaf(cond) => match sql_join_resolve(ctx, &cond.col) {
            Ok((t, idx)) => join_cond_pass(&row[wide_pos(t, idx)], cond),
            Err(_) => false,
        },
        Pred::And(v) => v.iter().all(|p| eval_join_pred(ctx, row, wide_pos, p)),
        Pred::Or(v) => v.iter().any(|p| eval_join_pred(ctx, row, wide_pos, p)),
        Pred::Not(b) => !eval_join_pred(ctx, row, wide_pos, b),
    }
}

/// ⭐ F68 (JOIN): col-col 比较 (ON 非等值残余用; 任一 NULL → false).
fn join_cmp_cols(a: &ColValue, op: CmpOp, b: &ColValue) -> bool {
    use std::cmp::Ordering;
    let ord = match (a, b) {
        (ColValue::Null, _) | (_, ColValue::Null) => return false,
        (ColValue::I64(x), ColValue::I64(y)) => x.cmp(y),
        (ColValue::F64(x), ColValue::F64(y)) => match x.partial_cmp(y) {
            Some(o) => o,
            None => return false,
        },
        (ColValue::I64(x), ColValue::F64(y)) => match (*x as f64).partial_cmp(y) {
            Some(o) => o,
            None => return false,
        },
        (ColValue::F64(x), ColValue::I64(y)) => match x.partial_cmp(&(*y as f64)) {
            Some(o) => o,
            None => return false,
        },
        (ColValue::Bytes(x), ColValue::Bytes(y)) => x.as_slice().cmp(y.as_slice()),
        _ => return false,
    };
    match op {
        CmpOp::Eq => ord == Ordering::Equal,
        CmpOp::Ne => ord != Ordering::Equal,
        CmpOp::Gt => ord == Ordering::Greater,
        CmpOp::Ge => ord != Ordering::Less,
        CmpOp::Lt => ord == Ordering::Less,
        CmpOp::Le => ord != Ordering::Greater,
        CmpOp::In => false,
    }
}

/// ⭐ F65: 提取 schema 的全局唯一列 (iid, col); 空 = 无全局唯一.
fn schema_global_unique(schema: &TableSchema) -> Vec<(u32, u16)> {
    schema
        .indexes
        .iter()
        .filter(|i| i.unique && i.global)
        .map(|i| (i.iid, i.col))
        .collect()
}

/// ⭐ F65: 从 row values 算出各全局唯一列的 (iid, enc_val); NULL 列跳过 (不占坑).
fn row_global_unique_encs(
    schema: &TableSchema,
    values: &[ColValue],
) -> Vec<(u32, Vec<u8>)> {
    schema_global_unique(schema)
        .into_iter()
        .filter_map(|(iid, col)| {
            let ty = schema.columns[col as usize].ty;
            storage::sql_rows::index_val_bytes(ty, &values[col as usize]).map(|enc| (iid, enc))
        })
        .collect()
}

/// ⭐ F65: 向 email-shard 发一个占坑 op (按 enc_val 路由).
#[allow(clippy::too_many_arguments)]
fn push_unique_op(
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    table: &str,
    op: BatchOp,
    enc_val: &[u8],
    num_shards: usize,
    shard_inboxes: &[SharedTaskInbox],
) {
    let sid = hash_route_key(db, table, enc_val, num_shards);
    push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
}

/// ⭐ F65: 启动 autocommit 单行 INSERT 的占坑编排 (已知含全局唯一列).
/// 发第一个 ReserveUnique, 后续由 sql_unique_drive 推进.
#[allow(clippy::too_many_arguments)]
fn sql_unique_ins_start(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    schema: std::sync::Arc<TableSchema>,
    table: String,
    pk: Vec<u8>,
    values: Vec<ColValue>,
) {
    let guc = row_global_unique_encs(&schema, &values);
    // guc 不可能为空 (caller 已判 has_global_unique); 但 NULL 值会使其空 —
    // 全局唯一列隐含 NOT NULL, 实际不会空; 防御性处理: 空则直写行
    if guc.is_empty() {
        let op = BatchOp::RowPut {
            db: db.clone(),
            table: std::sync::Arc::from(table.as_str()),
            pk,
            values,
        };
        let sid = hash_route_op(&op, num_shards);
        push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
        conn.sql_dml_agg.insert(
            seq,
            SqlDmlAgg { remaining: 1, affected: 0, error: None, drop_key: None },
        );
        return;
    }
    let txn_id = seq.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1; // 非 0 的伪唯 txn 标记
    let first = guc[0].clone();
    let st = SqlUniqueIns {
        db: db.clone(),
        table,
        schema,
        pk: pk.clone(),
        values,
        guc,
        txn_id,
        phase: UniquePhase::Reserve,
        idx: 0,
        reserved: 0,
    };
    let tbl = st.table.clone();
    conn.sql_unique_ins.insert(seq, st);
    let op = BatchOp::ReserveUnique {
        db: db.clone(),
        table: std::sync::Arc::from(tbl.as_str()),
        iid: first.0,
        enc_val: first.1.clone(),
        pk,
        txn_id,
    };
    push_unique_op(conn_id, seq, worker_id, db, &tbl, op, &first.1, num_shards, shard_inboxes);
}

/// ⭐ F65: 占坑状态机推进 (在 handle_resp_shard_result 内命中 seq 时调).
/// 返回 true = 已处理此 reply (不再走后续聚合器).
#[allow(clippy::too_many_arguments)]
fn sql_unique_drive(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    result: &BatchResult,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) -> bool {
    if !conn.sql_unique_ins.contains_key(&seq) {
        return false;
    }
    let proto = conn.proto;
    let mut st = conn.sql_unique_ins.remove(&seq).expect("just checked");

    // 回滚 helper: release 已占的 guc[0..reserved], 然后回错
    let rollback_and_err = |conn: &mut ConnState, st: &SqlUniqueIns, msg: String| {
        for (iid, enc) in st.guc.iter().take(st.reserved) {
            let op = BatchOp::ReleaseUnique {
                db: st.db.clone(),
                table: std::sync::Arc::from(st.table.as_str()),
                iid: *iid,
                enc_val: enc.clone(),
                txn_id: st.txn_id,
            };
            // fire-and-forget release (seq=0 不等回复; 用专用低优先无聚合)
            let sid = hash_route_key(&st.db, &st.table, enc, num_shards);
            push_task_grouped(conn_id, 0, worker_id, sid as u32, sid, op, shard_inboxes);
        }
        let bin = conn.mysql_binary.remove(&seq);
        let _ = bin;
        conn.resp_complete(seq, sql_err_bytes(proto, &msg));
    };

    match st.phase {
        UniquePhase::Reserve => match result {
            BatchResult::ReserveOk => {
                st.reserved += 1;
                st.idx += 1;
                if st.idx < st.guc.len() {
                    // 发下一列 reserve
                    let (iid, enc) = st.guc[st.idx].clone();
                    let pk = st.pk.clone();
                    let (db, tbl, txn) = (st.db.clone(), st.table.clone(), st.txn_id);
                    conn.sql_unique_ins.insert(seq, st);
                    let op = BatchOp::ReserveUnique {
                        db: db.clone(),
                        table: std::sync::Arc::from(tbl.as_str()),
                        iid,
                        enc_val: enc.clone(),
                        pk,
                        txn_id: txn,
                    };
                    push_unique_op(conn_id, seq, worker_id, &db, &tbl, op, &enc, num_shards, shard_inboxes);
                } else {
                    // 全部 reserve 完→写行
                    st.phase = UniquePhase::Write;
                    let op = BatchOp::RowPut {
                        db: st.db.clone(),
                        table: std::sync::Arc::from(st.table.as_str()),
                        pk: st.pk.clone(),
                        values: st.values.clone(),
                    };
                    let sid = hash_route_op(&op, num_shards);
                    conn.sql_unique_ins.insert(seq, st);
                    push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
                }
            }
            BatchResult::ReserveConflict { state, holder_pk, .. } => {
                if *state == 2 {
                    // COMMITTED 冲突 → Verify: 回查持有者行是否真存在
                    st.phase = UniquePhase::Verify;
                    let hp = holder_pk.clone();
                    let op = BatchOp::RowGet {
                        db: st.db.clone(),
                        table: std::sync::Arc::from(st.table.as_str()),
                        pk: hp,
                    };
                    let sid = hash_route_op(&op, num_shards);
                    conn.sql_unique_ins.insert(seq, st);
                    push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
                } else {
                    // PENDING 冲突 (在飞) → 拒 (客户端重试)
                    rollback_and_err(conn, &st, "duplicate key on global unique column".into());
                }
            }
            BatchResult::Error(e) => rollback_and_err(conn, &st, e.clone()),
            _ => rollback_and_err(conn, &st, "unexpected reserve reply".into()),
        },
        UniquePhase::Verify => {
            // 回查结果: 持有者行存在且含本 enc_val → 真冲突; 否则 stale → 抢占
            let cur = &st.guc[st.idx];
            let holder_has = matches!(result, BatchResult::GetValue(Some(row))
                if row_has_index_val(&st.schema, row, cur.0, &cur.1));
            if holder_has {
                rollback_and_err(conn, &st, "duplicate key on global unique column".into());
            } else {
                // stale 坑 → 抢占, 继续当前列
                st.phase = UniquePhase::Reserve;
                let (iid, enc) = st.guc[st.idx].clone();
                let pk = st.pk.clone();
                let (db, tbl, txn) = (st.db.clone(), st.table.clone(), st.txn_id);
                conn.sql_unique_ins.insert(seq, st);
                let op = BatchOp::StealUnique {
                    db: db.clone(),
                    table: std::sync::Arc::from(tbl.as_str()),
                    iid,
                    enc_val: enc.clone(),
                    pk,
                    txn_id: txn,
                };
                push_unique_op(conn_id, seq, worker_id, &db, &tbl, op, &enc, num_shards, shard_inboxes);
            }
        }
        UniquePhase::Write => match result {
            BatchResult::PutOk => {
                // 写行成功 → 逐列 confirm
                st.phase = UniquePhase::Confirm;
                st.idx = 0;
                let (iid, enc) = st.guc[0].clone();
                let pk = st.pk.clone();
                let (db, tbl, txn) = (st.db.clone(), st.table.clone(), st.txn_id);
                conn.sql_unique_ins.insert(seq, st);
                let op = BatchOp::ConfirmUnique {
                    db: db.clone(),
                    table: std::sync::Arc::from(tbl.as_str()),
                    iid,
                    enc_val: enc.clone(),
                    pk,
                    txn_id: txn,
                };
                push_unique_op(conn_id, seq, worker_id, &db, &tbl, op, &enc, num_shards, shard_inboxes);
            }
            BatchResult::Error(e) => rollback_and_err(conn, &st, e.clone()),
            _ => rollback_and_err(conn, &st, "unexpected rowput reply".into()),
        },
        UniquePhase::Confirm => {
            // confirm ack (PutOk); 逐列推进, 全部完 → 回 OK
            st.idx += 1;
            if st.idx < st.guc.len() {
                let (iid, enc) = st.guc[st.idx].clone();
                let pk = st.pk.clone();
                let (db, tbl, txn) = (st.db.clone(), st.table.clone(), st.txn_id);
                conn.sql_unique_ins.insert(seq, st);
                let op = BatchOp::ConfirmUnique {
                    db: db.clone(),
                    table: std::sync::Arc::from(tbl.as_str()),
                    iid,
                    enc_val: enc.clone(),
                    pk,
                    txn_id: txn,
                };
                push_unique_op(conn_id, seq, worker_id, &db, &tbl, op, &enc, num_shards, shard_inboxes);
            } else {
                let bin = conn.mysql_binary.remove(&seq);
                let _ = bin;
                conn.resp_complete(seq, sql_ok_bytes(proto, 1));
            }
        }
    }
    true
}

/// ⭐ F65: 判断 row 字节的指定 iid 列值是否等于 enc_val (Verify 用).
fn row_has_index_val(schema: &TableSchema, row: &[u8], iid: u32, enc_val: &[u8]) -> bool {
    let Ok(values) = storage::row::decode_row(schema, row) else {
        return false;
    };
    schema.indexes.iter().find(|i| i.iid == iid).is_some_and(|idx| {
        let ty = schema.columns[idx.col as usize].ty;
        storage::sql_rows::index_val_bytes(ty, &values[idx.col as usize])
            .is_some_and(|e| e == enc_val)
    })
}

/// ⭐ F66: 系统表查询规格 (解析产物, worker 合成虚拟表用).
struct SysQuerySpec {
    catalog: String,
    table: String,
    cols: Vec<String>,
    conds: Pred<Cond>,
    order: Vec<(String, bool)>,
    limit: Option<u32>,
    offset: Option<u32>,
}

impl SysQuerySpec {
    /// 需要表/列元数据 (发 CatalogDump); 否则仅 db 列表.
    fn needs_catalog(&self) -> bool {
        !matches!(
            (self.catalog.as_str(), self.table.as_str()),
            ("information_schema", "schemata")
                | ("pg_catalog", "pg_namespace")
                | ("__show__", "databases")
                | ("__show__", "__empty__")
        )
    }
}

/// ⭐ F66: ColType → information_schema.columns 的 data_type 字符串.
fn coltype_sql_name(ty: ColType) -> &'static str {
    match ty {
        ColType::I64 => "bigint",
        ColType::F64 => "double",
        ColType::Str => "text",
        ColType::Bytes => "blob",
        ColType::Bool => "boolean",
        ColType::Date => "date",
        ColType::Time => "time",
        ColType::Timestamp => "timestamp",
        ColType::Json => "json",
        ColType::Uuid => "uuid",
        ColType::Decimal { .. } => "decimal",
    }
}

/// ⭐ F66: 用合成列名+行跑完成点 (过滤/投影/排序/截断) → 三门面渲染.
/// 虚拟列均为 Str; 行值用 ColValue::Bytes (NULL 用 ColValue::Null).
fn sysq_finish(
    proto: ProtocolKind,
    binary: bool,
    spec: &SysQuerySpec,
    all_cols: &[&str],
    mut rows: Vec<Vec<ColValue>>,
) -> Vec<u8> {
    // 合成 schema (全 Str) 用于 WHERE 过滤 + 投影 + 排序列定位
    let schema = TableSchema {
        version: 1,
        columns: all_cols
            .iter()
            .map(|n| storage::schema::Column {
                name: n.to_string(),
                ty: ColType::Str,
                nullable: true,
            })
            .collect(),
        pk_col: 0,
        indexes: Vec::new(),
        next_iid: 0,
        version_ncols: Vec::new(),
    };
    // WHERE 残余过滤 (递归 eval; `__` 前缀的内部标记叶子如 __table__ 视为真,
    // 已在生成器里处理; 未知真实列的条件 → 不匹配则滤掉)
    rows.retain(|r| eval_pred_sysq(&schema, r, &spec.conds));
    // ORDER BY (按输出列字典序; 未知列忽略)
    for (name, desc) in spec.order.iter().rev() {
        if let Some(ci) = all_cols.iter().position(|c| c.eq_ignore_ascii_case(name)) {
            rows.sort_by(|a, b| {
                let o = cmp_colvalue(&a[ci], &b[ci]);
                if *desc { o.reverse() } else { o }
            });
        }
    }
    // OFFSET / LIMIT
    let start = (spec.offset.unwrap_or(0) as usize).min(rows.len());
    let end = match spec.limit {
        Some(l) => (start + l as usize).min(rows.len()),
        None => rows.len(),
    };
    let rows = &rows[start..end];
    // 投影: cols 空 = 全列; 否则按名选 (未知列 → 全 NULL 列)
    if spec.cols.is_empty() {
        let cols: Vec<(&str, ColType)> = all_cols.iter().map(|c| (*c, ColType::Str)).collect();
        sql_rows_bytes(proto, binary, &cols, rows)
    } else {
        let idxs: Vec<Option<usize>> = spec
            .cols
            .iter()
            .map(|c| all_cols.iter().position(|a| a.eq_ignore_ascii_case(c)))
            .collect();
        let cols: Vec<(&str, ColType)> =
            spec.cols.iter().map(|c| (c.as_str(), ColType::Str)).collect();
        let proj: Vec<Vec<ColValue>> = rows
            .iter()
            .map(|r| {
                idxs.iter()
                    .map(|oi| oi.and_then(|i| r.get(i).cloned()).unwrap_or(ColValue::Null))
                    .collect()
            })
            .collect();
        sql_rows_bytes(proto, binary, &cols, &proj)
    }
}

fn sbytes(s: &str) -> ColValue {
    ColValue::Bytes(s.as_bytes().to_vec())
}

/// ⭐ F66: db 列表类虚拟表 (schemata / pg_namespace) — 零任务合成.
fn sysq_render_dblist(
    proto: ProtocolKind,
    binary: bool,
    spec: &SysQuerySpec,
    dbs: &[String],
) -> Vec<u8> {
    let (all_cols, rows): (Vec<&str>, Vec<Vec<ColValue>>) =
        match (spec.catalog.as_str(), spec.table.as_str()) {
            ("information_schema", "schemata") => (
                vec!["catalog_name", "schema_name", "default_character_set_name"],
                dbs.iter()
                    .map(|d| vec![sbytes("def"), sbytes(d), sbytes("utf8mb4")])
                    .collect(),
            ),
            ("pg_catalog", "pg_namespace") => (
                vec!["nspname", "oid"],
                dbs.iter()
                    .enumerate()
                    .map(|(i, d)| vec![sbytes(d), sbytes(&(i as u32 + 1).to_string())])
                    .collect(),
            ),
            // ⭐ F66: SHOW DATABASES — 单列 "Database"
            ("__show__", "databases") => (
                vec!["Database"],
                dbs.iter().map(|d| vec![sbytes(d)]).collect(),
            ),
            // ⭐ F66: 其他 SHOW stub → 空
            ("__show__", "__empty__") => (vec![""], vec![]),
            _ => (vec![], vec![]),
        };
    sysq_finish(proto, binary, spec, &all_cols, rows)
}

/// ⭐ F66: 需 catalog 快照的虚拟表合成 (tables/columns/key_column_usage/pg_*).
/// `entries` = CatalogDump 回的 (table_name, TableSchema).
fn sysq_render_catalog(
    proto: ProtocolKind,
    binary: bool,
    spec: &SysQuerySpec,
    db: &str,
    entries: &[(String, TableSchema)],
) -> Vec<u8> {
    let key = (spec.catalog.as_str(), spec.table.as_str());
    // ⭐ F66: SHOW TABLES 动态列名 (函数级存活, 避免每次查询泄漏)
    let tables_in = format!("Tables_in_{db}");
    let (all_cols, rows): (Vec<&str>, Vec<Vec<ColValue>>) = match key {
        // ⭐ F66: SHOW [FULL] TABLES — 列名 Tables_in_<db> [+ Table_type]
        ("__show__", "tables") | ("__show__", "full_tables") => {
            let full = spec.table == "full_tables";
            let mut rows = Vec::new();
            for (t, _) in entries {
                if full {
                    rows.push(vec![sbytes(t), sbytes("BASE TABLE")]);
                } else {
                    rows.push(vec![sbytes(t)]);
                }
            }
            if full {
                (vec![tables_in.as_str(), "Table_type"], rows)
            } else {
                (vec![tables_in.as_str()], rows)
            }
        }
        // ⭐ F66: SHOW [FULL] COLUMNS FROM t — Field/Type/Null/Key/Default/Extra
        ("__show__", "columns") | ("__show__", "full_columns") => {
            let full = spec.table == "full_columns";
            // 从 __table__ cond 取目标表名
            let target = spec
                .conds
                .leaves()
                .into_iter()
                .find(|c| c.col == "__table__")
                .and_then(|c| match &c.val {
                    crate::protocol::sql::SqlValue::Str(b) => {
                        Some(String::from_utf8_lossy(b).to_string())
                    }
                    _ => None,
                });
            let mut rows = Vec::new();
            for (t, sc) in entries {
                if let Some(tt) = &target
                    && !t.eq_ignore_ascii_case(tt)
                {
                    continue;
                }
                for (i, c) in sc.columns.iter().enumerate() {
                    let key = if i as u16 == sc.pk_col {
                        "PRI"
                    } else if let Some(idx) = sc.indexes.iter().find(|x| x.col == i as u16) {
                        if idx.unique { "UNI" } else { "MUL" }
                    } else {
                        ""
                    };
                    let mut row = vec![
                        sbytes(&c.name),
                        sbytes(coltype_sql_name(c.ty)),
                        sbytes(if c.nullable { "YES" } else { "NO" }),
                        sbytes(key),
                        ColValue::Null, // Default
                        sbytes(""),     // Extra
                    ];
                    if full {
                        row.push(ColValue::Null); // Collation
                        row.push(sbytes("select,insert,update,references")); // Privileges
                        row.push(sbytes("")); // Comment
                    }
                    rows.push(row);
                }
            }
            if full {
                (
                    vec![
                        "Field", "Type", "Null", "Key", "Default", "Extra", "Collation",
                        "Privileges", "Comment",
                    ],
                    rows,
                )
            } else {
                (vec!["Field", "Type", "Null", "Key", "Default", "Extra"], rows)
            }
        }
        // ⭐ F66: SHOW CREATE TABLE t — 重建 MySQL DDL (SQLAlchemy 从此解析列)
        ("__show__", "create_table") => {
            let target = spec
                .conds
                .leaves()
                .into_iter()
                .find(|c| c.col == "__table__")
                .and_then(|c| match &c.val {
                    crate::protocol::sql::SqlValue::Str(b) => {
                        Some(String::from_utf8_lossy(b).to_string())
                    }
                    _ => None,
                })
                .unwrap_or_default();
            let mut rows = Vec::new();
            if let Some((t, sc)) = entries.iter().find(|(t, _)| t.eq_ignore_ascii_case(&target)) {
                let mut lines: Vec<String> = Vec::new();
                for (i, c) in sc.columns.iter().enumerate() {
                    let ty: std::borrow::Cow<str> = match c.ty {
                        ColType::I64 => "int".into(),
                        ColType::F64 => "double".into(),
                        ColType::Str => "text".into(),
                        ColType::Bytes => "blob".into(),
                        ColType::Bool => "tinyint(1)".into(),
                        ColType::Date => "date".into(),
                        ColType::Time => "time".into(),
                        ColType::Timestamp => "timestamp".into(),
                        ColType::Json => "json".into(),
                        ColType::Uuid => "char(36)".into(),
                        ColType::Decimal { precision, scale } => {
                            format!("decimal({precision},{scale})").into()
                        }
                    };
                    let nullness = if i as u16 == sc.pk_col || !c.nullable {
                        " NOT NULL".to_string()
                    } else {
                        " DEFAULT NULL".to_string()
                    };
                    lines.push(format!("  `{}` {}{}", c.name, ty, nullness));
                }
                let pkc = &sc.columns[sc.pk_col as usize].name;
                lines.push(format!("  PRIMARY KEY (`{pkc}`)"));
                for idx in &sc.indexes {
                    let cn = &sc.columns[idx.col as usize].name;
                    if idx.unique {
                        lines.push(format!("  UNIQUE KEY `{cn}` (`{cn}`)"));
                    } else {
                        lines.push(format!("  KEY `{cn}` (`{cn}`)"));
                    }
                }
                let ddl = format!(
                    "CREATE TABLE `{}` (\n{}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
                    t,
                    lines.join(",\n")
                );
                rows.push(vec![sbytes(t), sbytes(&ddl)]);
            }
            (vec!["Table", "Create Table"], rows)
        }
        ("information_schema", "tables") => (
            vec!["table_catalog", "table_schema", "table_name", "table_type"],
            entries
                .iter()
                .map(|(t, _)| {
                    vec![sbytes("def"), sbytes(db), sbytes(t), sbytes("BASE TABLE")]
                })
                .collect(),
        ),
        ("information_schema", "columns") => {
            let cols = vec![
                "table_catalog",
                "table_schema",
                "table_name",
                "column_name",
                "ordinal_position",
                "is_nullable",
                "data_type",
                "column_default",
            ];
            let mut rows = Vec::new();
            for (t, sc) in entries {
                for (i, c) in sc.columns.iter().enumerate() {
                    rows.push(vec![
                        sbytes("def"),
                        sbytes(db),
                        sbytes(t),
                        sbytes(&c.name),
                        sbytes(&(i + 1).to_string()),
                        sbytes(if c.nullable { "YES" } else { "NO" }),
                        sbytes(coltype_sql_name(c.ty)),
                        ColValue::Null,
                    ]);
                }
            }
            (cols, rows)
        }
        ("information_schema", "key_column_usage") => {
            let cols = vec![
                "table_schema",
                "table_name",
                "column_name",
                "constraint_name",
                "ordinal_position",
            ];
            let mut rows = Vec::new();
            for (t, sc) in entries {
                // pk
                let pkc = &sc.columns[sc.pk_col as usize].name;
                rows.push(vec![
                    sbytes(db),
                    sbytes(t),
                    sbytes(pkc),
                    sbytes("PRIMARY"),
                    sbytes("1"),
                ]);
                // unique 索引
                for idx in sc.indexes.iter().filter(|i| i.unique) {
                    let cn = &sc.columns[idx.col as usize].name;
                    rows.push(vec![
                        sbytes(db),
                        sbytes(t),
                        sbytes(cn),
                        sbytes(&format!("uniq_{cn}")),
                        sbytes("1"),
                    ]);
                }
            }
            (cols, rows)
        }
        ("pg_catalog", "pg_class") => (
            vec!["relname", "relkind", "oid"],
            entries
                .iter()
                .enumerate()
                .map(|(i, (t, _))| {
                    vec![sbytes(t), sbytes("r"), sbytes(&(i as u32 + 1).to_string())]
                })
                .collect(),
        ),
        ("pg_catalog", "pg_attribute") => {
            let cols = vec!["attrelid", "attname", "attnum", "attnotnull"];
            let mut rows = Vec::new();
            for (ri, (_, sc)) in entries.iter().enumerate() {
                for (i, c) in sc.columns.iter().enumerate() {
                    rows.push(vec![
                        sbytes(&(ri as u32 + 1).to_string()),
                        sbytes(&c.name),
                        sbytes(&(i + 1).to_string()),
                        sbytes(if c.nullable { "f" } else { "t" }),
                    ]);
                }
            }
            (cols, rows)
        }
        // 未知系统表 → 空结果 (工具探测容错)
        _ => (vec!["unknown"], vec![]),
    };
    sysq_finish(proto, binary, spec, &all_cols, rows)
}

/// ⭐ F76: 剥列名的表名限定前缀 (`表.列`/`别名.列` → `列`); 仅当前缀匹配时.
fn strip_col_qual(col: &mut String, table: &str) {
    if let Some((q, c)) = col.split_once('.')
        && q.eq_ignore_ascii_case(table)
    {
        *col = c.to_string();
    }
}

fn strip_pred_qual(pred: &mut Pred<Cond>, table: &str) {
    match pred {
        Pred::Leaf(c) => strip_col_qual(&mut c.col, table),
        Pred::And(v) | Pred::Or(v) => v.iter_mut().for_each(|p| strip_pred_qual(p, table)),
        Pred::Not(b) => strip_pred_qual(b, table),
    }
}

/// ⭐ F76: 单表 Select/Delete/Update 内所有列引用剥表名限定符 (JOIN 走 QualCol 不经此).
fn strip_qual_in_stmt(stmt: &mut SqlStmt) {
    match stmt {
        SqlStmt::Select { table, items, conds, order, group_by, having, .. } => {
            let t = table.clone();
            for it in items.iter_mut() {
                match it {
                    sql::SelectItem::Col { name, .. } => strip_col_qual(name, &t),
                    // ⭐ F78: 聚合参可为表达式 — 递归剥内部列引用的表限定前缀
                    sql::SelectItem::Agg { arg: Some(e), .. } => {
                        e.for_each_col_mut(&mut |c| strip_col_qual(c, &t));
                    }
                    sql::SelectItem::Agg { .. } => {}
                }
            }
            strip_pred_qual(conds, &t);
            strip_pred_qual(having, &t);
            for (n, _) in order.iter_mut() {
                strip_col_qual(n, &t);
            }
            for g in group_by.iter_mut() {
                strip_col_qual(g, &t);
            }
        }
        SqlStmt::Delete { table, conds } => {
            let t = table.clone();
            strip_pred_qual(conds, &t);
        }
        SqlStmt::Update { table, sets, conds } => {
            let t = table.clone();
            for (c, _) in sets.iter_mut() {
                strip_col_qual(c, &t);
            }
            strip_pred_qual(conds, &t);
        }
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn sql_run_dml(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    schema: std::sync::Arc<TableSchema>,
    stmt: SqlStmt,
) {
    // ⭐ F76: 单表限定列 `表.列` → 剥为 `列` (ORM 单表查询也带表名限定符)
    let mut stmt = stmt;
    strip_qual_in_stmt(&mut stmt);
    match stmt {
        SqlStmt::Insert { table, cols, rows } => {
            // ⭐ S1: 多行 VALUES — 逐行 RowPut, DmlAgg 计数 (批内非原子, 文档记录)
            let mut ops: Vec<BatchOp> = Vec::with_capacity(rows.len());
            for vals in &rows {
                let values = match sql_build_row(&schema, &cols, vals) {
                    Ok(v) => v,
                    Err(e) => {
                        conn.resp_complete(seq, sql_err_bytes(conn.proto, &e));
                        return;
                    }
                };
                let pk = match sql_pk_bytes(
                    schema.columns[schema.pk_col as usize].ty,
                    &values[schema.pk_col as usize],
                ) {
                    Ok(p) => p,
                    Err(e) => {
                        conn.resp_complete(seq, sql_err_bytes(conn.proto, &e));
                        return;
                    }
                };
                ops.push(BatchOp::RowPut {
                    db: db.clone(),
                    table: std::sync::Arc::from(table.as_str()),
                    pk,
                    values,
                });
            }
            // ⭐ 事务 v1 (F61): 事务中 INSERT 截流进 write_set (喂 bloom
            // 照旧 — rollback 只多假阳性), 立即回 OK, commit 时原子应用
            if conn.txn.is_some() {
                // ⭐ F65 v1 边界: 全局唯一表不支持事务内写 (占坑需在 commit 编排,
                // 未实现; 拒绝而非静默破坏全局唯一性)
                if schema.indexes.iter().any(|i| i.unique && i.global) {
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(
                            conn.proto,
                            "INSERT into GLOBAL UNIQUE table inside a transaction not supported (v1); use autocommit",
                        ),
                    );
                    return;
                }
                let n = ops.len() as u64;
                for op in ops {
                    let sid = hash_route_op(&op, num_shards);
                    feed_route_bloom(conn, db, &table, &schema, &op, sid);
                    if let Err(e) = txn_buffer_op(conn, op) {
                        conn.resp_complete(seq, sql_err_bytes(conn.proto, &e));
                        return;
                    }
                }
                conn.resp_complete(seq, sql_ok_bytes(conn.proto, n));
                return;
            }
            conn.sql_dml_agg.insert(
                seq,
                SqlDmlAgg {
                    remaining: ops.len(),
                    affected: 0,
                    error: None,
                    drop_key: None,
                },
            );
            // ⭐ F65: 含全局唯一列且单行 autocommit → 走占坑编排
            let has_gu = schema.indexes.iter().any(|i| i.unique && i.global);
            if has_gu {
                if ops.len() != 1 {
                    conn.sql_dml_agg.remove(&seq);
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(
                            conn.proto,
                            "multi-row INSERT into GLOBAL UNIQUE table not supported (v1)",
                        ),
                    );
                    return;
                }
                conn.sql_dml_agg.remove(&seq); // 占坑编排自己管回复
                let BatchOp::RowPut { pk, values, .. } = ops.into_iter().next().unwrap() else {
                    unreachable!()
                };
                // 喂 bloom (与普通路径一致)
                let probe = BatchOp::RowPut {
                    db: db.clone(),
                    table: std::sync::Arc::from(table.as_str()),
                    pk: pk.clone(),
                    values: values.clone(),
                };
                let sid = hash_route_op(&probe, num_shards);
                feed_route_bloom(conn, db, &table, &schema, &probe, sid);
                sql_unique_ins_start(
                    conn, conn_id, seq, worker_id, db, shard_inboxes, num_shards, schema, table,
                    pk, values,
                );
                return;
            }
            for op in ops {
                // ⭐ W2 → ORM-B2: created_here 的表 → 喂进程级路由缓存
                // (value → 所在 shard; bloom 原子只增, 多 worker/门面并发安全)
                let sid = hash_route_op(&op, num_shards);
                feed_route_bloom(conn, db, &table, &schema, &op, sid);
                push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
            }
        }
        // ⭐ S1: DELETE / UPDATE — pk 等值单发, 其余两阶段 (SELECT 内部路径收 pk)
        SqlStmt::Delete { .. } | SqlStmt::Update { .. } => {
            let (table, conds, action) = match stmt {
                SqlStmt::Delete { table, conds } => (table, conds, SqlDmlAction::Delete),
                SqlStmt::Update { table, conds, sets } => {
                    // 校验 + 转换 sets → (列号, ColValue)
                    let mut out: Vec<(u16, ColValue)> = Vec::with_capacity(sets.len());
                    for (name, v) in &sets {
                        let Some(i) = schema.col_by_name(name) else {
                            conn.resp_complete(
                                seq,
                                sql_err_bytes(conn.proto, &format!("unknown column '{name}'")),
                            );
                            return;
                        };
                        if i == schema.pk_col {
                            conn.resp_complete(
                                seq,
                                sql_err_bytes(conn.proto, "cannot UPDATE PRIMARY KEY column"),
                            );
                            return;
                        }
                        // ⭐ F65 v1 边界: 不支持 UPDATE 全局唯一列 (需輁坑; 未实现)
                        if schema.indexes.iter().any(|idx| idx.col == i && idx.unique && idx.global)
                        {
                            conn.resp_complete(
                                seq,
                                sql_err_bytes(
                                    conn.proto,
                                    "UPDATE of GLOBAL UNIQUE column not supported (v1); DELETE + INSERT instead",
                                ),
                            );
                            return;
                        }
                        let cv = match sql_to_col(schema.columns[i as usize].ty, v) {
                            Ok(c) => c,
                            Err(e) => {
                                conn.resp_complete(seq, sql_err_bytes(conn.proto, &e));
                                return;
                            }
                        };
                        if cv == ColValue::Null && !schema.columns[i as usize].nullable {
                            conn.resp_complete(
                                seq,
                                sql_err_bytes(conn.proto, &format!("column '{name}' is NOT NULL")),
                            );
                            return;
                        }
                        out.push((i, cv));
                    }
                    (table, conds, SqlDmlAction::Update(out))
                }
                _ => unreachable!(),
            };
            match sql_plan_select(&schema, &conds) {
                Err(e) => conn.resp_complete(seq, sql_err_bytes(conn.proto, &e)),
                Ok(SqlPlan::PkGet { pk }) => {
                    // ⭐ 事务 v1 (F61): pk 等值 UPDATE/DELETE 截流进 write_set
                    // (affected 乐观估 1, 真实效果 commit 时定 — 文档化)
                    if conn.txn.is_some() {
                        let op = sql_dml_op(db, &table, pk, &action);
                        match txn_buffer_op(conn, op) {
                            Ok(()) => conn.resp_complete(seq, sql_ok_bytes(conn.proto, 1)),
                            Err(e) => conn.resp_complete(seq, sql_err_bytes(conn.proto, &e)),
                        }
                        return;
                    }
                    // pk 等值 → 单 shard 原子, 直发 phase2
                    conn.sql_dml_agg.insert(
                        seq,
                        SqlDmlAgg { remaining: 1, affected: 0, error: None, drop_key: None },
                    );
                    let op = sql_dml_op(db, &table, pk, &action);
                    push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
                Ok(SqlPlan::Index { iid, lo, hi, .. }) => {
                    // 两阶段 phase1: 复用 SELECT 广播路径收全行 (残余过滤需行值),
                    // 完成点取 pk 发 phase2. limit 不下推 (DML 无 LIMIT).
                    conn.sql_select_agg.insert(
                        seq,
                        SqlSelectAgg {
                            remaining: num_shards,
                            error: None,
                            rows: Vec::new(),
                            schema: schema.clone(),
                            conds,
                            limit: None,
                            proj: Vec::new(),
                            cover: None,
                            unique_early: false, // DML 禁早停 (防同 seq 双 agg 并存)
                            done: false,
                            dml: Some(action),
                            dml_target: Some((db.clone(), table.clone())),
                            order: Vec::new(),
                            offset: 0,
                            count: false,
                            agg_spec: None,
                            out_names: Vec::new(),
                        },
                    );
                    let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
                    for sid in 0..num_shards {
                        let op = BatchOp::IndexScan {
                            db: db.clone(),
                            table: table_arc.clone(),
                            iid,
                            lo: lo.clone(),
                            hi: hi.clone(),
                            limit: 0,
                            with_rows: true,
                        };
                        push_task_grouped(
                            conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes,
                        );
                    }
                }
                // ⭐ S2: 无可用索引的 DML (含无 WHERE 全删/全改) → 全表扫 phase1
                Ok(SqlPlan::FullScan) => {
                    conn.sql_select_agg.insert(
                        seq,
                        SqlSelectAgg {
                            remaining: num_shards,
                            error: None,
                            rows: Vec::new(),
                            schema: schema.clone(),
                            conds,
                            limit: None,
                            proj: Vec::new(),
                            cover: None,
                            unique_early: false,
                            done: false,
                            dml: Some(action),
                            dml_target: Some((db.clone(), table.clone())),
                            order: Vec::new(),
                            offset: 0,
                            count: false,
                            agg_spec: None,
                            out_names: Vec::new(),
                        },
                    );
                    let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
                    for sid in 0..num_shards {
                        let op = BatchOp::TableScan {
                            db: db.clone(),
                            table: table_arc.clone(),
                            limit: 0,
                        };
                        push_task_grouped(
                            conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes,
                        );
                    }
                }
            }
        }
        SqlStmt::Select { table, items, conds, limit, order, offset, group_by, having } => {
            // ⭐ G1/G2 (F63): 投影分型 — 纯列 / COUNT(*) 特例 (旧路径) /
            // 广义聚合 (分桶完成点)
            let has_agg = items.iter().any(|i| matches!(i, sql::SelectItem::Agg { .. }));
            let count = has_agg
                && items.len() == 1
                && group_by.is_empty()
                && having.is_true()
                && order.is_empty()
                && matches!(
                    items[0],
                    sql::SelectItem::Agg { func: sql::AggFn::Count, arg: None, .. }
                );
            if (has_agg || !group_by.is_empty()) && !count {
                sql_run_agg_select(
                    conn, conn_id, seq, worker_id, db, shard_inboxes, num_shards, schema,
                    table, items, conds, group_by, having, order, limit, offset,
                );
                return;
            }
            let cols: Vec<String> = items
                .iter()
                .filter_map(|i| match i {
                    sql::SelectItem::Col { name, .. } => Some(name.clone()),
                    sql::SelectItem::Agg { .. } => None, // 仅 COUNT(*) 特例可达
                })
                .collect();
            // ⭐ F76: 输出列名 (alias 优先) — 与 proj 同序; 空 items (SELECT *) → 全 None
            let out_names: Vec<Option<String>> = items
                .iter()
                .filter_map(|i| match i {
                    sql::SelectItem::Col { alias, .. } => Some(alias.clone()),
                    sql::SelectItem::Agg { .. } => None,
                })
                .collect();
            // ⭐ O1: 投影列名 → 列号 (空/COUNT = 全列)
            let proj: Vec<u16> = if cols.is_empty() {
                (0..schema.columns.len() as u16).collect()
            } else {
                let mut p = Vec::with_capacity(cols.len());
                for c in &cols {
                    match schema.col_by_name(c) {
                        Some(i) => p.push(i),
                        None => {
                            conn.resp_complete(
                                seq,
                                sql_err_bytes(conn.proto, &format!("unknown column '{c}'")),
                            );
                            return;
                        }
                    }
                }
                p
            };
            // ⭐ S2: ORDER BY 列名 → 列号
            let mut order_cols: Vec<(u16, bool)> = Vec::with_capacity(order.len());
            for (name, desc) in &order {
                match schema.col_by_name(name) {
                    Some(i) => order_cols.push((i, *desc)),
                    None => {
                        conn.resp_complete(
                            seq,
                            sql_err_bytes(conn.proto, &format!("unknown column '{name}'")),
                        );
                        return;
                    }
                }
            }
            let offset = offset.unwrap_or(0);
            match sql_plan_select(&schema, &conds) {
            Err(e) => conn.resp_complete(seq, sql_err_bytes(conn.proto, &e)),
            Ok(SqlPlan::PkGet { pk }) => {
                // ⭐ 事务 v1 (F61): RYOW — pk 点查命中本事务 write_set 时
                // 直接回缓冲内容 (INSERT 见新行 / DELETE 见空; UPDATE 直通
                // 读已提交版本 — v1 文档化)
                if let Some(txn) = conn.txn.as_ref() {
                    let tkey = (db.to_string(), table.clone(), pk.clone());
                    match resolve_ryow(txn, &tkey) {
                        Some(RyowState::Resolved(state)) => {
                            let bin = conn.mysql_binary.remove(&seq);
                            let bytes = match state {
                                Some(values) if eval_pred(&schema, &values, &conds) => {
                                    if count {
                                        render_sql_count(conn.proto, bin, 1)
                                    } else {
                                        render_sql_rows(
                                            conn.proto,
                                            bin,
                                            &schema,
                                            &proj,
                                            &out_names,
                                            std::slice::from_ref(&values),
                                        )
                                    }
                                }
                                _ if count => render_sql_count(conn.proto, bin, 0),
                                _ => render_sql_rows(conn.proto, bin, &schema, &proj, &out_names, &[]),
                            };
                            conn.resp_complete(seq, bytes);
                            return;
                        }
                        Some(RyowState::NeedBase(overlay)) => {
                            let read_key = sql_read_key(conn, db, &table, &pk);
                            conn.sql_row_ctx.insert(
                                seq,
                                SqlRowCtx {
                                    schema,
                                    conds,
                                    proj,
                                    count,
                                    read_key,
                                    ryow_overlay: overlay,
                                    out_names: out_names.clone(),
                                },
                            );
                            let op = BatchOp::RowGet {
                                db: db.clone(),
                                table: std::sync::Arc::from(table.as_str()),
                                pk,
                            };
                            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                            return;
                        }
                        None => {}
                    }
                }
                let read_key = sql_read_key(conn, db, &table, &pk);
                conn.sql_row_ctx.insert(
                    seq,
                    SqlRowCtx { schema, conds, proj, count, read_key, ryow_overlay: Vec::new(), out_names },
                );
                let op = BatchOp::RowGet {
                    db: db.clone(),
                    table: std::sync::Arc::from(table.as_str()),
                    pk,
                };
                push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
            }
            // ⭐ S2: 全表扫 — 广播 TableScan + 全条件残余过滤
            Ok(SqlPlan::FullScan) => {
                // limit 下推仅当无条件且无排序 (下推额含 offset)
                let shard_limit = if conds.is_true() && order_cols.is_empty() && !count {
                    limit.map(|l| l + offset).unwrap_or(0)
                } else {
                    0
                };
                conn.sql_select_agg.insert(
                    seq,
                    SqlSelectAgg {
                        remaining: num_shards,
                        error: None,
                        rows: Vec::new(),
                        schema,
                        conds,
                        limit,
                        proj,
                        cover: None,
                        unique_early: false,
                        done: false,
                        dml: None,
                        dml_target: None,
                        order: order_cols,
                        offset,
                        count,
                        agg_spec: None,
                        out_names,
                    },
                );
                let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
                for sid in 0..num_shards {
                    let op = BatchOp::TableScan {
                        db: db.clone(),
                        table: table_arc.clone(),
                        limit: shard_limit,
                    };
                    push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
                }
            }
            Ok(SqlPlan::Index { iid, lo, hi, limit_push, eq_enc }) => {
                // limit 下推: 仅当条件可被闭界完全表达且无排序
                // (否则残余过滤/全量排序会漏行; 下推额含 offset)
                let shard_limit = if limit_push && order_cols.is_empty() && !count {
                    limit.map(|l| l + offset).unwrap_or(0)
                } else {
                    0
                };
                // ⭐ O1: 覆盖判定 — 投影∪条件∪排序列 ⊆ {索引列, pk 列} → 免回表
                let idx_col = schema
                    .indexes
                    .iter()
                    .find(|i| i.iid == iid)
                    .map(|i| i.col)
                    .expect("plan 产出的 iid 必在 schema");
                let pk_col = schema.pk_col;
                let in_cover = |c: u16| c == idx_col || c == pk_col;
                let cover = (count || proj.iter().all(|&c| in_cover(c)))
                    && order_cols.iter().all(|&(c, _)| in_cover(c))
                    && conds
                        .leaves()
                        .iter()
                        .all(|c| schema.col_by_name(&c.col).is_some_and(in_cover));
                // ⭐ W2 → ORM-B2: 等值查询 + created_here 表 → 进程级路由缓存
                // 候选剪枝 (Arc 克隆锁外读 bloom; 无 entry / 范围查询 → 广播)
                let candidates: Vec<usize> = {
                    use std::sync::atomic::Ordering::Relaxed;
                    let sh = &conn.sql_shared;
                    let entry = eq_enc.as_ref().and_then(|_| {
                        sh.routes
                            .read()
                            .unwrap()
                            .get(&(db.to_string(), table.clone(), iid))
                            .cloned()
                    });
                    match (eq_enc.as_ref(), entry) {
                        (Some(enc), Some(blooms)) => {
                            let c: Vec<usize> = (0..num_shards)
                                .filter(|&s| blooms[s].may_contain(enc))
                                .collect();
                            if c.is_empty() {
                                sh.route_bypassed.fetch_add(1, Relaxed);
                            } else if c.len() < num_shards {
                                sh.route_pruned.fetch_add(1, Relaxed);
                            }
                            c
                        }
                        _ => (0..num_shards).collect(),
                    }
                };
                if candidates.is_empty() {
                    // 零任务短路: 值从未插入过 (bloom 无假阴性保证)
                    let bin = conn.mysql_binary.remove(&seq);
                    let bytes = if count {
                        render_sql_count(conn.proto, bin, 0)
                    } else {
                        render_sql_rows(conn.proto, bin, &schema, &proj, &out_names, &[])
                    };
                    conn.resp_complete(seq, bytes);
                    return;
                }
                conn.sql_select_agg.insert(
                    seq,
                    SqlSelectAgg {
                        remaining: candidates.len(),
                        error: None,
                        rows: Vec::new(),
                        schema: schema.clone(),
                        conds,
                        limit,
                        proj,
                        cover: cover.then_some((idx_col, pk_col)),
                        // ⭐ O3: unique 索引等值 → 首个非空回包即回复
                        // (⭐ S2: 排序/offset/count 与单行早停正交, 保持启用)
                        unique_early: eq_enc.is_some()
                            && schema
                                .indexes
                                .iter()
                                .any(|i| i.iid == iid && i.unique),
                        done: false,
                        dml: None,
                        dml_target: None,
                        order: order_cols,
                        offset,
                        count,
                        agg_spec: None,
                        out_names,
                    },
                );
                let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
                for sid in candidates {
                    let op = BatchOp::IndexScan {
                        db: db.clone(),
                        table: table_arc.clone(),
                        iid,
                        lo: lo.clone(),
                        hi: hi.clone(),
                        limit: shard_limit,
                        with_rows: !cover, // ⭐ O1: 覆盖 → shard 免回表
                    };
                    push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
                }
            }
        }
        }
        SqlStmt::CreateTable { .. } => unreachable!("CREATE 在 sql_dispatch_stmt 处理"),
        SqlStmt::DropTable { .. } => unreachable!("DROP 在 sql_dispatch_stmt 处理"),
        // ⭐ F79: ALTER TABLE ADD COLUMN — 基于旧 schema (参数) 合成新 schema 并广播 SetSchemaOp
        SqlStmt::AlterTable { table, add } => {
            if schema.col_by_name(&add.name).is_some() {
                conn.resp_complete(
                    seq,
                    sql_err_bytes(conn.proto, &format!("duplicate column name '{}'", add.name)),
                );
                return;
            }
            let new_schema = match schema.with_added_column(add) {
                Ok(s) => s,
                Err(_) => {
                    conn.resp_complete(
                        seq,
                        sql_err_bytes(conn.proto, "too many ALTER TABLE versions (v1 limit)"),
                    );
                    return;
                }
            };
            let bytes = new_schema.encode();
            let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
            conn.sql_ddl_agg.insert(
                seq,
                SqlDdlAgg {
                    remaining: num_shards,
                    error: None,
                    key: (db.to_string(), table),
                    schema: std::sync::Arc::new(new_schema),
                    alter: true,
                },
            );
            for sid in 0..num_shards {
                let op = BatchOp::SetSchemaOp {
                    db: db.clone(),
                    table: table_arc.clone(),
                    bytes: bytes.clone(),
                };
                push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
            }
        }
        SqlStmt::SelectDerived { .. } => unreachable!("派生表在 sql_dispatch_stmt 处理"),
        SqlStmt::Begin { .. }
        | SqlStmt::Commit
        | SqlStmt::Rollback
        | SqlStmt::SetTransaction { .. }
        | SqlStmt::Savepoint { .. }
        | SqlStmt::RollbackTo { .. }
        | SqlStmt::Release { .. } => {
            unreachable!("事务语句在 sql_dispatch_stmt 处理")
        }
        SqlStmt::Use { .. }
        | SqlStmt::SetStub
        | SqlStmt::VersionStub
        | SqlStmt::DatabaseStub
        | SqlStmt::SystemQuery { .. }
        | SqlStmt::SystemVarStub { .. }
        | SqlStmt::SelectJoin { .. } => {
            unreachable!("工具命令在 sql_dispatch_stmt 处理")
        }
        // ⭐ S3: DESCRIBE — schema 本地渲染 (Field/Type/Null/Key)
        SqlStmt::Describe { .. } => {
            let mut rows: Vec<Vec<ColValue>> = Vec::new();
            for (i, col) in schema.columns.iter().enumerate() {
                let ty = coltype_sql_name(col.ty);
                let key = if i as u16 == schema.pk_col {
                    "PRI"
                } else if let Some(idx) = schema.indexes.iter().find(|x| x.col == i as u16) {
                    if idx.unique { "UNI" } else { "MUL" }
                } else {
                    ""
                };
                rows.push(vec![
                    ColValue::Bytes(col.name.as_bytes().to_vec()),
                    ColValue::Bytes(ty.as_bytes().to_vec()),
                    ColValue::Bytes(if col.nullable { b"YES".to_vec() } else { b"NO".to_vec() }),
                    ColValue::Bytes(key.as_bytes().to_vec()),
                ]);
            }
            let cols: [(&str, ColType); 4] = [
                ("Field", ColType::Str),
                ("Type", ColType::Str),
                ("Null", ColType::Str),
                ("Key", ColType::Str),
            ];
            let bin = conn.mysql_binary.remove(&seq);
            conn.resp_complete(seq, sql_rows_bytes(conn.proto, bin, &cols, &rows));
        }
    }
}

/// SELECT 访问路径选择 (worker 过滤器核心):
/// 1. pk 等值 → PkGet; 2. 首个命中条件的索引 → Index (界下推);
/// 3. 无可用索引 → 报错 (v1 不做全表扫).
fn sql_plan_select(schema: &TableSchema, pred: &Pred<Cond>) -> Result<SqlPlan, String> {
    // 先校验所有叶子列名 (不论结构)
    for c in pred.leaves() {
        if schema.col_by_name(&c.col).is_none() {
            return Err(format!("unknown column '{}'", c.col));
        }
    }
    // ⭐ F69: 含 OR/NOT → 无单一区间, 回退全表扫 (正确性由完成点 eval_pred 兼底)
    let Some(conds) = pred.as_conjuncts() else {
        return Ok(SqlPlan::FullScan);
    };
    // 1. pk 等值点查
    let pk_col = &schema.columns[schema.pk_col as usize];
    if let Some(c) = conds.iter().find(|c| c.op == CmpOp::Eq && c.col == pk_col.name) {
        let cv = sql_to_col(pk_col.ty, &c.val)?;
        return Ok(SqlPlan::PkGet { pk: sql_pk_bytes(pk_col.ty, &cv)? });
    }
    // 2. 首个有条件命中的索引 (界下推; 开界值多包含由残余过滤兜底)
    for idx in &schema.indexes {
        let col = &schema.columns[idx.col as usize];
        let mut lo: Option<ColValue> = None;
        let mut hi: Option<ColValue> = None;
        let mut hit = false;
        for c in conds.iter().filter(|c| c.col == col.name) {
            let cv_of = |v: &SqlValue| sql_to_col(col.ty, v);
            match c.op {
                CmpOp::Eq => {
                    hit = true;
                    let cv = cv_of(&c.val)?;
                    lo = Some(cv.clone());
                    hi = Some(cv);
                }
                CmpOp::Gt | CmpOp::Ge => {
                    hit = true;
                    if lo.is_none() {
                        lo = Some(cv_of(&c.val)?);
                    }
                }
                CmpOp::Lt | CmpOp::Le => {
                    hit = true;
                    if hi.is_none() {
                        hi = Some(cv_of(&c.val)?);
                    }
                }
                // ⭐ S2: IN → [min, max] 闭界超集 (保序编码字节比较取极值),
                // 残余过滤精确; Ne 无剪枝价值, 不算命中
                CmpOp::In => {
                    hit = true;
                    if lo.is_none() && hi.is_none() {
                        let mut min: Option<ColValue> = None;
                        let mut max: Option<ColValue> = None;
                        for v in &c.set {
                            let cv = cv_of(v)?;
                            let enc = storage::sql_rows::index_val_bytes(col.ty, &cv)
                                .ok_or("bad IN value")?;
                            let replace_min = min
                                .as_ref()
                                .and_then(|m| storage::sql_rows::index_val_bytes(col.ty, m))
                                .is_none_or(|me| enc < me);
                            if replace_min {
                                min = Some(cv.clone());
                            }
                            let replace_max = max
                                .as_ref()
                                .and_then(|m| storage::sql_rows::index_val_bytes(col.ty, m))
                                .is_none_or(|me| enc > me);
                            if replace_max {
                                max = Some(cv);
                            }
                        }
                        lo = min;
                        hi = max;
                    }
                }
                CmpOp::Ne => {}
            }
        }
        if hit {
            // limit 可下推 ⟺ 全部条件都在本索引列且均为闭界算子
            // (Eq/Ge/Le 的闭界下推与过滤语义一致, 不会截掉本应命中的行)
            let limit_push = conds
                .iter()
                .all(|c| c.col == col.name && matches!(c.op, CmpOp::Eq | CmpOp::Ge | CmpOp::Le));
            // ⭐ W2: 等值 (lo == hi) 时算路由缓存键 (与引擎索引值编码同源)
            let eq_enc = match (&lo, &hi) {
                (Some(l), Some(h)) if l == h => {
                    storage::sql_rows::index_val_bytes(col.ty, l)
                }
                _ => None,
            };
            return Ok(SqlPlan::Index { iid: idx.iid, lo, hi, limit_push, eq_enc });
        }
    }
    // ⭐ S2: 无可用索引 → 全表扫 + 残余过滤 (v1 的报错路径退役)
    Ok(SqlPlan::FullScan)
}

/// INSERT 值列表 → 全列 ColValue (列清单缺省填 NULL; 类型转换).
fn sql_build_row(
    schema: &TableSchema,
    cols: &[String],
    vals: &[SqlValue],
) -> Result<Vec<ColValue>, String> {
    let n = schema.columns.len();
    let mut out = vec![ColValue::Null; n];
    if cols.is_empty() {
        if vals.len() != n {
            return Err(format!("expected {n} values, got {}", vals.len()));
        }
        for (i, v) in vals.iter().enumerate() {
            out[i] = sql_to_col(schema.columns[i].ty, v)?;
        }
    } else {
        for (name, v) in cols.iter().zip(vals) {
            let i = schema
                .col_by_name(name)
                .ok_or_else(|| format!("unknown column '{name}'"))? as usize;
            out[i] = sql_to_col(schema.columns[i].ty, v)?;
        }
    }
    Ok(out)
}

/// ⭐ F80: 民用日期 (y,m,d) → 距 1970-01-01 的天数 (Howard Hinnant days_from_civil).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe - 719468
}

/// ⭐ F80: 逆变换 天数 → (y,m,d).
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// ⭐ F80: 解析 'YYYY-MM-DD' → 距 epoch 微秒 (00:00:00).
fn parse_date_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    let mut it = s.splitn(3, '-');
    let y = it.next()?.parse::<i64>().ok()?;
    let m = it.next()?.parse::<i64>().ok()?;
    let d = it.next()?.parse::<i64>().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some(days_from_civil(y, m, d) * MICROS_PER_DAY)
}

/// ⭐ F80: 解析 'HH:MM:SS[.ffffff]' → 距零点微秒.
fn parse_time_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    let (hms, frac) = match s.split_once('.') {
        Some((a, b)) => (a, b),
        None => (s, ""),
    };
    let mut it = hms.splitn(3, ':');
    let h = it.next()?.parse::<i64>().ok()?;
    let mi = it.next()?.parse::<i64>().ok()?;
    let se = it.next().unwrap_or("0").parse::<i64>().ok()?;
    let mut micros = ((h * 60 + mi) * 60 + se) * 1_000_000;
    if !frac.is_empty() {
        let mut f = frac.to_string();
        f.truncate(6);
        while f.len() < 6 {
            f.push('0');
        }
        micros += f.parse::<i64>().ok()?;
    }
    Some(micros)
}

/// ⭐ F80: 解析 'YYYY-MM-DD[ T]HH:MM:SS[.ffffff]' → 距 epoch 微秒.
fn parse_timestamp_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date, time) = if let Some((d, t)) = s.split_once('T') {
        (d, Some(t))
    } else if let Some((d, t)) = s.split_once(' ') {
        (d, Some(t))
    } else {
        (s, None)
    };
    let base = parse_date_micros(date)?;
    match time {
        Some(t) if !t.trim().is_empty() => Some(base + parse_time_micros(t)?),
        _ => Some(base),
    }
}

/// ⭐ F80: 渲染 (供三门面): 微秒 → 'YYYY-MM-DD'.
pub(crate) fn render_date(micros: i64) -> String {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// ⭐ F80: 微秒 → 'HH:MM:SS' (截去小数; 距零点).
pub(crate) fn render_time(micros: i64) -> String {
    let mut secs = micros.rem_euclid(MICROS_PER_DAY) / 1_000_000;
    let h = secs / 3600;
    secs %= 3600;
    format!("{:02}:{:02}:{:02}", h, secs / 60, secs % 60)
}

/// ⭐ F80: 微秒 → 'YYYY-MM-DD HH:MM:SS'.
pub(crate) fn render_timestamp(micros: i64) -> String {
    format!("{} {}", render_date(micros), render_time(micros))
}

/// ⭐ F80: 微秒 → (年, 月, 日, 时, 分, 秒, 微秒) — MySQL 二进制协议 DATE/DATETIME 编码用.
pub(crate) fn datetime_parts(micros: i64) -> (u16, u8, u8, u8, u8, u8, u32) {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    let tod = micros.rem_euclid(MICROS_PER_DAY);
    let micro = (tod % 1_000_000) as u32;
    let secs = tod / 1_000_000;
    let hh = (secs / 3600) as u8;
    let mm = ((secs % 3600) / 60) as u8;
    let ss = (secs % 60) as u8;
    (y as u16, m as u8, d as u8, hh, mm, ss, micro)
}

/// ⭐ F80: 距零点微秒 → (时, 分, 秒, 微秒) — MySQL 二进制 TIME 编码用.
pub(crate) fn time_parts(micros: i64) -> (u8, u8, u8, u32) {
    let tod = micros.rem_euclid(MICROS_PER_DAY);
    let micro = (tod % 1_000_000) as u32;
    let secs = tod / 1_000_000;
    ((secs / 3600) as u8, ((secs % 3600) / 60) as u8, (secs % 60) as u8, micro)
}

/// ⭐ F80: 16B → 36 字符带连字符 UUID.
pub(crate) fn render_uuid(b: &[u8]) -> String {
    if b.len() != 16 {
        return String::from_utf8_lossy(b).into_owned();
    }
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

/// ⭐ F80: 解析 UUID 文本 (带/不带连字符) → 16B; 失败返回 None.
fn parse_uuid(s: &str) -> Option<Vec<u8>> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    (0..16).map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()).collect()
}

/// ⭐ F81: 10^scale (i128; scale<=38 → <i128::MAX). None=溢出.
fn pow10_i128(scale: u8) -> Option<i128> {
    10i128.checked_pow(scale as u32)
}

/// ⭐ F81: 十进制文本 → 定标 i128 (按 scale; 超出小数位截断, 不四舍五入). 非法/溢出→None.
fn parse_decimal(s: &str, scale: u8) -> Option<i128> {
    let s = s.trim();
    let (neg, s) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match s.split_once('.') {
        Some((a, b)) => (a, b),
        None => (s, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|c| c.is_ascii_digit()) || !frac_part.bytes().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let sc = scale as usize;
    let mut frac = frac_part.to_string();
    if frac.len() > sc {
        frac.truncate(sc);
    }
    while frac.len() < sc {
        frac.push('0');
    }
    let int_val: i128 = if int_part.is_empty() { 0 } else { int_part.parse().ok()? };
    let frac_val: i128 = if sc == 0 || frac.is_empty() { 0 } else { frac.parse().ok()? };
    let scaled = int_val.checked_mul(pow10_i128(scale)?)?.checked_add(frac_val)?;
    Some(if neg { -scaled } else { scaled })
}

/// ⭐ F81: 定标 i128 + scale → 十进制文本 "123.45".
pub(crate) fn render_decimal(v: i128, scale: u8) -> String {
    if scale == 0 {
        return v.to_string();
    }
    let neg = v < 0;
    let av = v.unsigned_abs();
    let p = 10u128.pow(scale as u32);
    format!("{}{}.{:0width$}", if neg { "-" } else { "" }, av / p, av % p, width = scale as usize)
}

/// SQL 字面量 → 列值 (Int 可升 F64; 类型不符报错).
/// ⭐ P1: 数值列收到 Str → 尝试文本解析 (PG 文本参数按目标类型转换语义).
fn sql_to_col(ty: ColType, v: &SqlValue) -> Result<ColValue, String> {
    Ok(match (ty, v) {
        (_, SqlValue::Null) => ColValue::Null,
        (_, SqlValue::Param(_)) => return Err("unbound parameter".into()),
        // ⭐ F71: 子查询未折叠就流到执行层 = bug (防御)
        (_, SqlValue::Subquery(_)) => return Err("unresolved subquery".into()),
        // ⭐ F74: 列引用未去相关就流到执行层 = bug (防御)
        (_, SqlValue::ColRef(_)) => return Err("unresolved column reference".into()),
        (ColType::I64, SqlValue::Int(i)) => ColValue::I64(*i),
        (ColType::F64, SqlValue::Int(i)) => ColValue::F64(*i as f64),
        (ColType::F64, SqlValue::Float(f)) => ColValue::F64(*f),
        (ColType::Str | ColType::Bytes, SqlValue::Str(s)) => ColValue::Bytes(s.clone()),
        // ⭐ F80: BOOL — TRUE/FALSE(Int 1/0) 或文本 true/false/t/f/1/0 → I64(0/1)
        (ColType::Bool, SqlValue::Int(i)) => ColValue::I64(i64::from(*i != 0)),
        (ColType::Bool, SqlValue::Str(s)) => {
            let t = std::str::from_utf8(s).unwrap_or("").trim().to_ascii_lowercase();
            match t.as_str() {
                "1" | "true" | "t" | "yes" | "y" => ColValue::I64(1),
                "0" | "false" | "f" | "no" | "n" | "" => ColValue::I64(0),
                _ => return Err("invalid boolean text".into()),
            }
        }
        // ⭐ F80: DATE/TIME/TIMESTAMP — 文本解析成 i64 微秒; Int 视为已是微秒
        (ColType::Date, SqlValue::Str(s)) => parse_date_micros(std::str::from_utf8(s).unwrap_or(""))
            .map(ColValue::I64)
            .ok_or("invalid DATE literal (expect 'YYYY-MM-DD')")?,
        (ColType::Time, SqlValue::Str(s)) => parse_time_micros(std::str::from_utf8(s).unwrap_or(""))
            .map(ColValue::I64)
            .ok_or("invalid TIME literal (expect 'HH:MM:SS')")?,
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
        _ => return Err(format!("type mismatch: {v:?} not assignable to {ty:?} column")),
    })
}

/// pk 列值 → 存储 pk 字节 (数值保序编码, 字节串原样; NULL/空串非法).
fn sql_pk_bytes(ty: ColType, v: &ColValue) -> Result<Vec<u8>, String> {
    match (ty, v) {
        (ColType::I64, ColValue::I64(i)) => Ok(storage::keyspace::encode_idx(*i).to_vec()),
        (ColType::F64, ColValue::F64(f)) => Ok(storage::keyspace::encode_f64_ordered(*f).to_vec()),
        // ⭐ F80: Bool/Date/Time/Timestamp 以 i64 承载 → 保序数值编码
        (
            ColType::Bool | ColType::Date | ColType::Time | ColType::Timestamp,
            ColValue::I64(i),
        ) => Ok(storage::keyspace::encode_idx(*i).to_vec()),
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
fn eval_cond_leaf(schema: &TableSchema, values: &[ColValue], c: &Cond) -> bool {
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
            let cvt = coerce_cmp_lit(colty, v);
            sql_cmp(cv, cvt.as_ref().unwrap_or(v)) == Some(Ordering::Equal)
        });
    }
    let cval = coerce_cmp_lit(colty, &c.val);
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
        },
    }
}

/// ⭐ F69: WHERE 谓词树递归求值 (And=全真, Or=任一真, Not=取反; NULL 叶子为 false).
fn eval_pred(schema: &TableSchema, values: &[ColValue], pred: &Pred<Cond>) -> bool {
    match pred {
        Pred::Leaf(c) => eval_cond_leaf(schema, values, c),
        Pred::And(v) => v.iter().all(|p| eval_pred(schema, values, p)),
        Pred::Or(v) => v.iter().any(|p| eval_pred(schema, values, p)),
        Pred::Not(b) => !eval_pred(schema, values, b),
    }
}

/// ⭐ F69: 系统表专用 eval — `__` 前缀内部标记叶子视为真 (已在生成器处理).
fn eval_pred_sysq(schema: &TableSchema, values: &[ColValue], pred: &Pred<Cond>) -> bool {
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
fn coerce_cmp_lit(ty: ColType, sv: &SqlValue) -> Option<SqlValue> {
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

/// 列值与字面量比较 (数值跨型比较; NULL/类型不符 → None = 条件 false).
/// ⭐ P1: 数值列 vs 文本 → 按文本数字解析比较 (PG 文本参数弱类型, 与 sql_to_col 一致).
fn sql_cmp(cv: &ColValue, sv: &SqlValue) -> Option<std::cmp::Ordering> {
    match (cv, sv) {
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
        (ColValue::Decimal(a, sc), SqlValue::Int(b)) => {
            (*b as i128).checked_mul(pow10_i128(*sc)?).map(|bb| a.cmp(&bb))
        }
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
fn sql_err_bytes(proto: ProtocolKind, msg: &str) -> Vec<u8> {
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
fn sql_ok_bytes(proto: ProtocolKind, affected: u64) -> Vec<u8> {
    if proto == ProtocolKind::Http {
        return crate::protocol::http::build_response(
            200,
            &serde_json::to_vec(&serde_json::json!({ "affected": affected }))
                .unwrap_or_default(),
            crate::http_config::cors_origin(),
            true,
        );
    }
    if proto == ProtocolKind::Pg {
        let mut out =
            crate::protocol::pg::build_command_complete(&format!("OK {affected}"));
        out.extend_from_slice(&crate::protocol::pg::build_ready());
        out
    } else {
        crate::protocol::mysql::build_ok(1, affected)
    }
}

/// ⭐ S4: 结果集 → per-proto 字节 (PG 尾随 ReadyForQuery;
/// ⭐ H3: HTTP {"columns": [...], "rows": [[...]]}).
fn sql_rows_bytes(
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
fn col_to_json(v: &ColValue) -> serde_json::Value {
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
fn render_sql_rows(
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
            let name = names.get(k).and_then(|o| o.as_deref()).unwrap_or(c.name.as_str());
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
fn col_from_ordered_bytes(ty: ColType, raw: &[u8]) -> Option<ColValue> {
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
fn collect_dml_pks(agg: &mut SqlSelectAgg) -> Result<Vec<Vec<u8>>, String> {
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
fn sql_dml_op(
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
fn sql_order_cmp(a: &[ColValue], b: &[ColValue], order: &[(u16, bool)]) -> std::cmp::Ordering {
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
fn render_sql_count(proto: ProtocolKind, binary: bool, n: u64) -> Vec<u8> {
    sql_rows_bytes(
        proto,
        binary,
        &[("COUNT(*)", ColType::I64)],
        &[vec![ColValue::I64(n as i64)]],
    )
}

/// ⭐ G2 (F63): 广义聚合 SELECT — 列名解析/类型校验/计划构建后广播
/// (索引可用时 IndexScan, 否则 TableScan; PkGet 也降级广播 — 聚合需全量行,
/// 单行情形低频可接受).
#[allow(clippy::too_many_arguments)]
fn sql_run_agg_select(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    schema: std::sync::Arc<TableSchema>,
    table: String,
    items: Vec<sql::SelectItem>,
    conds: Pred<Cond>,
    group_by: Vec<String>,
    having: Pred<Cond>,
    order: Vec<(String, bool)>,
    limit: Option<u32>,
    offset: Option<u32>,
) {
    let fail = |conn: &mut ConnState, msg: String| {
        conn.resp_complete(seq, sql_err_bytes(conn.proto, &msg));
    };
    // 输出项解析 (列号 + label + 输出类型)
    let mut spec_items: Vec<AggItem> = Vec::with_capacity(items.len());
    for it in &items {
        match it {
            sql::SelectItem::Col { name: c, alias } => {
                let Some(i) = schema.col_by_name(c) else {
                    return fail(conn, format!("unknown column '{c}'"));
                };
                spec_items.push(AggItem {
                    label: alias.clone().unwrap_or_else(|| c.clone()),
                    kind: AggItemKind::Col(i),
                    out_ty: schema.columns[i as usize].ty,
                });
            }
            sql::SelectItem::Agg { func, arg, distinct, alias } => {
                // ⭐ F78: 绑定表达式 (裸列退化) → (BoundExpr, 推导类型); COUNT(*) arg=None
                let bound: Option<(BoundExpr, ColType)> = match arg {
                    Some(e) => match bind_scalar_expr(&schema, e) {
                        Ok(bt) => Some(bt),
                        Err(msg) => return fail(conn, msg),
                    },
                    None => None,
                };
                // ⭐ F77: DISTINCT 仅 COUNT(DISTINCT col) (解析已拦; 双保险)
                if *distinct
                    && (*func != sql::AggFn::Count
                        || arg.as_ref().and_then(|e| e.as_col()).is_none())
                {
                    return fail(conn, "DISTINCT is only supported in COUNT(col) (v1)".into());
                }
                let src_ty = bound.as_ref().map(|(_, t)| *t);
                // SUM/AVG 仅数值 (⭐ F81: 含 DECIMAL)
                if matches!(func, sql::AggFn::Sum | sql::AggFn::Avg)
                    && !matches!(
                        src_ty,
                        Some(ColType::I64) | Some(ColType::F64) | Some(ColType::Decimal { .. })
                    )
                {
                    return fail(
                        conn,
                        format!("{} requires a numeric argument", func.label(None)),
                    );
                }
                let out_ty = match func {
                    sql::AggFn::Count => ColType::I64,
                    sql::AggFn::Sum => src_ty.unwrap_or(ColType::I64),
                    sql::AggFn::Avg => ColType::F64,
                    sql::AggFn::Min | sql::AggFn::Max => src_ty.unwrap_or(ColType::Bytes),
                };
                let inner = match arg {
                    None => "*".to_string(),
                    Some(e) => e.render(),
                };
                let default_label = if *distinct {
                    format!("COUNT(DISTINCT {inner})")
                } else {
                    format!("{}({inner})", func.label(None).trim_end_matches("(*)"))
                };
                spec_items.push(AggItem {
                    label: alias.clone().unwrap_or(default_label),
                    kind: AggItemKind::Agg {
                        func: *func,
                        arg: bound.map(|(b, _)| b),
                        distinct: *distinct,
                    },
                    out_ty,
                });
            }
        }
    }
    // 组键列号
    let mut group_idx: Vec<u16> = Vec::with_capacity(group_by.len());
    for g in &group_by {
        match schema.col_by_name(g) {
            Some(i) => group_idx.push(i),
            None => return fail(conn, format!("unknown column '{g}'")),
        }
    }
    // 输出列定位 helper: label 大小写归一匹配
    let find_out = |name: &str| -> Option<usize> {
        spec_items.iter().position(|it| it.label.eq_ignore_ascii_case(name))
    };
    // HAVING 谓词树 → (输出下标, op, val) 叶子树
    let having_out = match having.try_map(&|h: &Cond| -> Result<(usize, sql::CmpOp, sql::SqlValue), String> {
        find_out(&h.col)
            .map(|idx| (idx, h.op, h.val.clone()))
            .ok_or_else(|| format!("HAVING column '{}' must appear in the select list", h.col))
    }) {
        Ok(p) => p,
        Err(e) => return fail(conn, e),
    };
    // ORDER BY → (输出下标, desc)
    let mut order_out = Vec::with_capacity(order.len());
    for (name, desc) in &order {
        let Some(idx) = find_out(name) else {
            return fail(
                conn,
                format!("ORDER BY column '{name}' must appear in the select list"),
            );
        };
        order_out.push((idx, *desc));
    }
    let spec = AggSpec { items: spec_items, group_idx, having: having_out, order: order_out };
    // 广播: 索引计划可用则 IndexScan (界下推), 否则 TableScan (含 PkGet 降级)
    let plan = sql_plan_select(&schema, &conds);
    conn.sql_select_agg.insert(
        seq,
        SqlSelectAgg {
            remaining: num_shards,
            error: None,
            rows: Vec::new(),
            schema: schema.clone(),
            conds,
            limit,
            proj: Vec::new(),
            cover: None,
            unique_early: false, // 聚合需全量, 禁早停
            done: false,
            dml: None,
            dml_target: None,
            order: Vec::new(), // 排序在 agg_spec.order (输出列域)
            offset: offset.unwrap_or(0),
            count: false,
            agg_spec: Some(spec),
            out_names: Vec::new(),
        },
    );
    let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
    for sid in 0..num_shards {
        let op = match &plan {
            Ok(SqlPlan::Index { iid, lo, hi, .. }) => BatchOp::IndexScan {
                db: db.clone(),
                table: table_arc.clone(),
                iid: *iid,
                lo: lo.clone(),
                hi: hi.clone(),
                limit: 0,
                with_rows: true,
            },
            _ => BatchOp::TableScan { db: db.clone(), table: table_arc.clone(), limit: 0 },
        };
        push_task_grouped(conn_id, seq, worker_id, sid as u32, sid, op, shard_inboxes);
    }
}

/// ⭐ G2 (F63): 聚合计划 — dispatch 时列名已解析为列号/输出下标.
struct AggSpec {
    /// 输出列序 (label 供列头与 HAVING/ORDER 匹配).
    items: Vec<AggItem>,
    /// 组键列号 (空 = 全表单桶).
    group_idx: Vec<u16>,
    /// HAVING: (输出列下标, 算子, 右值).
    having: Pred<(usize, sql::CmpOp, sql::SqlValue)>,
    /// ORDER BY: (输出列下标, desc).
    order: Vec<(usize, bool)>,
}

struct AggItem {
    label: String,
    kind: AggItemKind,
    out_ty: ColType,
}

enum AggItemKind {
    /// 组键列直出 (必 ∈ group_by, 解析层已校验).
    Col(u16),
    /// ⭐ F78: arg = 已绑定列号的表达式 (None = COUNT(*)).
    Agg { func: sql::AggFn, arg: Option<BoundExpr>, distinct: bool },
}

/// ⭐ F78: 已绑定 (列名→列号) 的聚合内标量表达式.
enum BoundExpr {
    Col(u16),
    Lit(ColValue),
    Bin { op: sql::ArithOp, l: Box<BoundExpr>, r: Box<BoundExpr> },
}

/// ⭐ F78: 逐行求值 — 任一操作数 NULL/非数值 → NULL; Div 除零 → NULL;
/// 全整型且非 Div → I64 (溢出→NULL); 否则 F64.
fn eval_bound_expr(e: &BoundExpr, row: &[ColValue]) -> ColValue {
    match e {
        BoundExpr::Col(i) => row.get(*i as usize).cloned().unwrap_or(ColValue::Null),
        BoundExpr::Lit(v) => v.clone(),
        BoundExpr::Bin { op, l, r } => {
            let lv = eval_bound_expr(l, row);
            let rv = eval_bound_expr(r, row);
            // 提数: (值, 是否整型); 非数值/NULL → None
            let num = |v: &ColValue| -> Option<(f64, bool)> {
                match v {
                    ColValue::I64(x) => Some((*x as f64, true)),
                    ColValue::F64(x) => Some((*x, false)),
                    _ => None,
                }
            };
            let (Some((lf, li)), Some((rf, ri))) = (num(&lv), num(&rv)) else {
                return ColValue::Null;
            };
            let both_int = li && ri && *op != sql::ArithOp::Div;
            if both_int {
                let (a, b) = (lf as i64, rf as i64);
                let out = match op {
                    sql::ArithOp::Add => a.checked_add(b),
                    sql::ArithOp::Sub => a.checked_sub(b),
                    sql::ArithOp::Mul => a.checked_mul(b),
                    sql::ArithOp::Div => unreachable!(),
                };
                out.map(ColValue::I64).unwrap_or(ColValue::Null)
            } else {
                let out = match op {
                    sql::ArithOp::Add => lf + rf,
                    sql::ArithOp::Sub => lf - rf,
                    sql::ArithOp::Mul => lf * rf,
                    sql::ArithOp::Div => {
                        if rf == 0.0 {
                            return ColValue::Null;
                        }
                        lf / rf
                    }
                };
                ColValue::F64(out)
            }
        }
    }
}

/// ⭐ F78: 将解析期 ScalarExpr 绑定列号 + 推导输出类型 (未知列报错).
fn bind_scalar_expr(
    schema: &TableSchema,
    e: &sql::ScalarExpr,
) -> Result<(BoundExpr, ColType), String> {
    match e {
        sql::ScalarExpr::Col(name) => {
            let i = schema.col_by_name(name).ok_or_else(|| format!("unknown column '{name}'"))?;
            Ok((BoundExpr::Col(i), schema.columns[i as usize].ty))
        }
        sql::ScalarExpr::Lit(v) => {
            let (cv, ty) = match v {
                SqlValue::Int(x) => (ColValue::I64(*x), ColType::I64),
                SqlValue::Float(x) => (ColValue::F64(*x), ColType::F64),
                SqlValue::Str(b) => (ColValue::Bytes(b.clone()), ColType::Str),
                _ => return Err("unsupported literal in aggregate expression".into()),
            };
            Ok((BoundExpr::Lit(cv), ty))
        }
        sql::ScalarExpr::Bin { op, l, r } => {
            let (lb, lt) = bind_scalar_expr(schema, l)?;
            let (rb, rt) = bind_scalar_expr(schema, r)?;
            // 输出类型: Div → F64; 任一 F64 → F64; 否则 I64
            let out_ty = if *op == sql::ArithOp::Div || lt == ColType::F64 || rt == ColType::F64 {
                ColType::F64
            } else {
                ColType::I64
            };
            Ok((BoundExpr::Bin { op: *op, l: Box::new(lb), r: Box::new(rb) }, out_ty))
        }
    }
}

/// ⭐ G2 (F63): 聚合累加器 (NULL 忽略, COUNT(*) 除外; SUM 整列溢出报错).
enum Accum {
    CountStar(u64),
    CountCol(u64),
    /// ⭐ F77: COUNT(DISTINCT col) — 去重集 (类型标记编码, 不计 NULL).
    CountDistinct(std::collections::HashSet<Vec<u8>>),
    SumI { acc: i64, seen: bool },
    SumF { acc: f64, seen: bool },
    /// ⭐ F81: SUM(DECIMAL) → i128 定标累加, 输出同 scale Decimal.
    SumDec { acc: i128, scale: u8, seen: bool },
    Avg { sum: f64, n: u64 },
    Min(Option<ColValue>),
    Max(Option<ColValue>),
}

impl Accum {
    fn new(func: sql::AggFn, is_star: bool, col_ty: Option<ColType>, distinct: bool) -> Self {
        match func {
            // ⭐ F77: COUNT(DISTINCT col) → 去重集
            sql::AggFn::Count if distinct => Accum::CountDistinct(std::collections::HashSet::new()),
            sql::AggFn::Count if is_star => Accum::CountStar(0),
            sql::AggFn::Count => Accum::CountCol(0),
            sql::AggFn::Sum => match col_ty {
                Some(ColType::F64) => Accum::SumF { acc: 0.0, seen: false },
                Some(ColType::Decimal { scale, .. }) => {
                    Accum::SumDec { acc: 0, scale, seen: false }
                }
                _ => Accum::SumI { acc: 0, seen: false },
            },
            sql::AggFn::Avg => Accum::Avg { sum: 0.0, n: 0 },
            sql::AggFn::Min => Accum::Min(None),
            sql::AggFn::Max => Accum::Max(None),
        }
    }

    fn feed(&mut self, v: &ColValue) -> Result<(), String> {
        match self {
            Accum::CountStar(n) => *n += 1,
            Accum::CountCol(n) => {
                if !matches!(v, ColValue::Null) {
                    *n += 1;
                }
            }
            // ⭐ F77: COUNT(DISTINCT) — 非 NULL 值按类型标记编码入集
            Accum::CountDistinct(set) => {
                if !matches!(v, ColValue::Null) {
                    set.insert(encode_col_key(v));
                }
            }
            Accum::SumI { acc, seen } => match v {
                ColValue::I64(x) => {
                    *acc = acc.checked_add(*x).ok_or("SUM overflow (BIGINT)")?;
                    *seen = true;
                }
                ColValue::Null => {}
                _ => return Err("SUM requires a numeric column".into()),
            },
            Accum::SumF { acc, seen } => match v {
                ColValue::F64(x) => {
                    *acc += x;
                    *seen = true;
                }
                ColValue::I64(x) => {
                    *acc += *x as f64;
                    *seen = true;
                }
                ColValue::Null => {}
                _ => return Err("SUM requires a numeric column".into()),
            },
            // ⭐ F81: SUM(DECIMAL) 定标 i128 累加 (同 scale)
            Accum::SumDec { acc, seen, .. } => match v {
                ColValue::Decimal(x, _) => {
                    *acc = acc.checked_add(*x).ok_or("SUM overflow (DECIMAL)")?;
                    *seen = true;
                }
                ColValue::Null => {}
                _ => return Err("SUM requires a numeric column".into()),
            },
            Accum::Avg { sum, n } => match v {
                ColValue::F64(x) => {
                    *sum += x;
                    *n += 1;
                }
                ColValue::I64(x) => {
                    *sum += *x as f64;
                    *n += 1;
                }
                // ⭐ F81: AVG(DECIMAL) → f64 (v1; 精度回退)
                ColValue::Decimal(x, sc) => {
                    *sum += *x as f64 / 10f64.powi(*sc as i32);
                    *n += 1;
                }
                ColValue::Null => {}
                _ => return Err("AVG requires a numeric column".into()),
            },
            Accum::Min(cur) => {
                if !matches!(v, ColValue::Null)
                    && cur.as_ref().is_none_or(|c| cmp_colvalue(v, c).is_lt())
                {
                    *cur = Some(v.clone());
                }
            }
            Accum::Max(cur) => {
                if !matches!(v, ColValue::Null)
                    && cur.as_ref().is_none_or(|c| cmp_colvalue(v, c).is_gt())
                {
                    *cur = Some(v.clone());
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> ColValue {
        match self {
            Accum::CountStar(n) | Accum::CountCol(n) => ColValue::I64(n as i64),
            // ⭐ F77: COUNT(DISTINCT) → 去重集基数
            Accum::CountDistinct(set) => ColValue::I64(set.len() as i64),
            // SUM 空集 → NULL (SQL 语义)
            Accum::SumI { seen: false, .. } | Accum::SumF { seen: false, .. } => ColValue::Null,
            Accum::SumI { acc, .. } => ColValue::I64(acc),
            Accum::SumF { acc, .. } => ColValue::F64(acc),
            // ⭐ F81: SUM(DECIMAL) 空集→NULL; 否则同 scale Decimal
            Accum::SumDec { seen: false, .. } => ColValue::Null,
            Accum::SumDec { acc, scale, .. } => ColValue::Decimal(acc, scale),
            Accum::Avg { n: 0, .. } => ColValue::Null,
            Accum::Avg { sum, n } => ColValue::F64(sum / n as f64),
            Accum::Min(v) | Accum::Max(v) => v.unwrap_or(ColValue::Null),
        }
    }
}

/// 同型 ColValue 全序比较 (Null 最小; 异型按枚举序 — 同列值不会异型).
fn cmp_colvalue(a: &ColValue, b: &ColValue) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (ColValue::Null, ColValue::Null) => Equal,
        (ColValue::Null, _) => Less,
        (_, ColValue::Null) => Greater,
        (ColValue::I64(x), ColValue::I64(y)) => x.cmp(y),
        (ColValue::F64(x), ColValue::F64(y)) => x.partial_cmp(y).unwrap_or(Equal),
        (ColValue::I64(x), ColValue::F64(y)) => (*x as f64).partial_cmp(y).unwrap_or(Equal),
        (ColValue::F64(x), ColValue::I64(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Equal),
        (ColValue::Bytes(x), ColValue::Bytes(y)) => x.cmp(y),
        // ⭐ F81: 同列 Decimal 同 scale → 定标整数直接比较
        (ColValue::Decimal(x, _), ColValue::Decimal(y, _)) => x.cmp(y),
        (ColValue::I64(_) | ColValue::F64(_), ColValue::Bytes(_)) => Less,
        (ColValue::Bytes(_), ColValue::I64(_) | ColValue::F64(_)) => Greater,
        // Decimal 与异型 (同列不会发生): 稳定兜底
        (ColValue::Decimal(_, _), _) => Greater,
        (_, ColValue::Decimal(_, _)) => Less,
    }
}

/// 分桶数上限 (防内存失控).
const AGG_MAX_GROUPS: usize = 64 * 1024;

/// ⭐ G2 (F63): 分桶聚合完成点 — 已过滤行 → 分桶 → 累加 → HAVING →
/// ORDER → OFFSET/LIMIT → 合成结果集 (sql_rows_bytes 三门面统一).
/// ⭐ F69: HAVING 谓词树递归求值 (输出列下标域; NULL 不满足任何比较).
fn eval_having_pred(
    row: &[ColValue],
    pred: &Pred<(usize, sql::CmpOp, sql::SqlValue)>,
) -> bool {
    match pred {
        Pred::Leaf((idx, op, val)) => {
            let rhs = match val {
                sql::SqlValue::Int(x) => ColValue::I64(*x),
                sql::SqlValue::Float(x) => ColValue::F64(*x),
                sql::SqlValue::Str(s) => ColValue::Bytes(s.clone()),
                _ => return false,
            };
            if matches!(row[*idx], ColValue::Null) {
                return false; // NULL 不满足任何比较 (SQL 语义)
            }
            let ord = cmp_colvalue(&row[*idx], &rhs);
            match op {
                sql::CmpOp::Eq => ord.is_eq(),
                sql::CmpOp::Ne => ord.is_ne(),
                sql::CmpOp::Gt => ord.is_gt(),
                sql::CmpOp::Ge => ord.is_ge(),
                sql::CmpOp::Lt => ord.is_lt(),
                sql::CmpOp::Le => ord.is_le(),
                sql::CmpOp::In => false, // HAVING 不支持 IN
            }
        }
        Pred::And(v) => v.iter().all(|p| eval_having_pred(row, p)),
        Pred::Or(v) => v.iter().any(|p| eval_having_pred(row, p)),
        Pred::Not(b) => !eval_having_pred(row, b),
    }
}

/// ⭐ F77: 列值自包含类型标记编码 (只求相等性 + 确定序) —
/// GROUP BY 组键与 COUNT(DISTINCT) 去重集同源, 保证一致. 0=Null/1=I64/2=F64/3=Bytes.
fn encode_col_key_into(key: &mut Vec<u8>, v: &ColValue) {
    match v {
        ColValue::Null => key.push(0u8),
        ColValue::I64(x) => {
            key.push(1u8);
            key.extend_from_slice(&((*x as u64) ^ (1u64 << 63)).to_be_bytes());
        }
        ColValue::F64(x) => {
            key.push(2u8);
            key.extend_from_slice(&x.to_bits().to_be_bytes());
        }
        ColValue::Bytes(b) => {
            key.push(3u8);
            key.extend_from_slice(&(b.len() as u32).to_be_bytes());
            key.extend_from_slice(b);
        }
        // ⭐ F81: Decimal (tag 4 + 16B i128 LE); 同列同 scale → 定标整数唯一
        ColValue::Decimal(x, _) => {
            key.push(4u8);
            key.extend_from_slice(&x.to_le_bytes());
        }
    }
}

fn encode_col_key(v: &ColValue) -> Vec<u8> {
    let mut k = Vec::new();
    encode_col_key_into(&mut k, v);
    k
}

fn materialize_agg_groups(
    spec: &AggSpec,
    rows: Vec<Vec<ColValue>>,
    offset: u32,
    limit: Option<u32>,
) -> MatResult {
    // 分桶: 组键 = 各列保序编码级联 (NULL 归一组, 0x00 标记); BTreeMap =
    // 无 ORDER BY 时输出按组键序 (确定性)
    let mut buckets: std::collections::BTreeMap<Vec<u8>, (Vec<ColValue>, Vec<Accum>)> =
        std::collections::BTreeMap::new();
    let new_accums = |first_row: &[ColValue]| -> Vec<Accum> {
        let _ = first_row;
        spec.items
            .iter()
            .map(|it| match &it.kind {
                AggItemKind::Col(_) => Accum::CountStar(0), // 占位不用 (代表值直出)
                AggItemKind::Agg { func, arg, distinct } => {
                    // ⭐ F81: 直接传 out_ty (含 Decimal{scale}), 让 Accum 选 SumDec/SumF/SumI
                    Accum::new(*func, arg.is_none(), Some(it.out_ty), *distinct)
                }
            })
            .collect()
    };
    // 无 group_by = 全表单桶 (空表也输出一行 — PG 语义)
    if spec.group_idx.is_empty() {
        buckets.insert(Vec::new(), (Vec::new(), new_accums(&[])));
    }
    for values in &rows {
        let mut key = Vec::new();
        for &gi in &spec.group_idx {
            // 自包含类型标记编码 (只求相等性 + 确定序; 代表值另存)
            encode_col_key_into(&mut key, &values[gi as usize]);
        }
        if !buckets.contains_key(&key) && buckets.len() >= AGG_MAX_GROUPS {
            return Err("too many groups (limit 65536)".into());
        }
        let entry = buckets
            .entry(key)
            .or_insert_with(|| (values.clone(), new_accums(values)));
        for (it, acc) in spec.items.iter().zip(entry.1.iter_mut()) {
            if let AggItemKind::Agg { arg, .. } = &it.kind {
                // ⭐ F78: arg=Some → 逐行求值 (裸列/字面量/算术); None(COUNT(*)) → 常量 1
                match arg {
                    Some(e) => {
                        let v = eval_bound_expr(e, values);
                        acc.feed(&v)?;
                    }
                    None => acc.feed(&ColValue::I64(1))?,
                }
            }
        }
    }
    // 桶 → 输出行 (materialize)
    let mut out: Vec<Vec<ColValue>> = Vec::with_capacity(buckets.len());
    for (_, (rep, accums)) in buckets {
        let mut row = Vec::with_capacity(spec.items.len());
        let mut accums = accums.into_iter();
        for it in &spec.items {
            match &it.kind {
                AggItemKind::Col(ci) => {
                    accums.next(); // 跳过占位累加器
                    row.push(rep.get(*ci as usize).cloned().unwrap_or(ColValue::Null));
                }
                AggItemKind::Agg { .. } => {
                    row.push(accums.next().expect("accum 与 items 同长").finish());
                }
            }
        }
        out.push(row);
    }
    // HAVING (输出列比较; ⭐ F69 递归 AND/OR/NOT)
    out.retain(|row| eval_having_pred(row, &spec.having));
    // ORDER BY 输出列
    if !spec.order.is_empty() {
        out.sort_by(|a, b| {
            for (idx, desc) in &spec.order {
                let ord = cmp_colvalue(&a[*idx], &b[*idx]);
                if !ord.is_eq() {
                    return if *desc { ord.reverse() } else { ord };
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    // OFFSET / LIMIT
    let start = (offset as usize).min(out.len());
    let end = match limit {
        Some(l) => (start + l as usize).min(out.len()),
        None => out.len(),
    };
    // 合成结果集 (render_sql_count 同源路径, 三门面统一)
    let cols: Vec<(String, ColType)> =
        spec.items.iter().map(|it| (it.label.clone(), it.out_ty)).collect();
    Ok((cols, out[start..end].to_vec()))
}

/// SELECT 聚合完成渲染: (val, pk) 排序 → 覆盖重建或 decode → 残余过滤
/// → ⭐ S2: ORDER BY → OFFSET → LIMIT → 投影/COUNT 结果集.
/// (⭐ O3: 早停时提前调用, agg.rows 取走清空)
fn render_select_agg(proto: ProtocolKind, binary: bool, agg: &mut SqlSelectAgg) -> Vec<u8> {
    match materialize_select_agg(agg) {
        Ok((cols, rows)) => {
            let cref: Vec<(&str, ColType)> = cols.iter().map(|(n, t)| (n.as_str(), *t)).collect();
            sql_rows_bytes(proto, binary, &cref, &rows)
        }
        Err(e) => sql_err_bytes(proto, &e),
    }
}

/// ⭐ F71: SELECT 完成点物化 (不渲染) — 返回最终投影列定义 + 行集.
/// 供子查询捕获 (materialize) 与正常渲染 (render_select_agg) 共用.
fn materialize_select_agg(
    agg: &mut SqlSelectAgg,
) -> MatResult {
    if let Some(e) = agg.error.take() {
        return Err(e);
    }
    // 全局序: (索引值, pk); 残余过滤全条件 (下推界是超集, 过滤幂等)
    let mut rows = std::mem::take(&mut agg.rows);
    rows.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    let early_cut: Option<usize> = if agg.count || !agg.order.is_empty() || agg.agg_spec.is_some()
    {
        None
    } else {
        agg.limit.map(|l| (l + agg.offset) as usize)
    };
    let mut out_rows: Vec<Vec<ColValue>> = Vec::new();
    for (val, pk, rb) in &rows {
        let decoded = if let Some((idx_col, pk_col)) = agg.cover {
            let n = agg.schema.columns.len();
            let iv = col_from_ordered_bytes(agg.schema.columns[idx_col as usize].ty, val);
            let pv = col_from_ordered_bytes(agg.schema.columns[pk_col as usize].ty, pk);
            match (iv, pv) {
                (Some(iv), Some(pv)) => {
                    let mut values = vec![ColValue::Null; n];
                    values[idx_col as usize] = iv;
                    values[pk_col as usize] = pv;
                    Ok(values)
                }
                _ => Err("bad covered index entry".to_string()),
            }
        } else {
            storage::row::decode_row(&agg.schema, rb).map_err(|e| e.to_string())
        };
        let values = decoded?;
        if eval_pred(&agg.schema, &values, &agg.conds) {
            out_rows.push(values);
            if let Some(cut) = early_cut
                && out_rows.len() >= cut
            {
                break;
            }
        }
    }
    // ⭐ G2: 广义聚合
    if let Some(spec) = agg.agg_spec.take() {
        return materialize_agg_groups(&spec, out_rows, agg.offset, agg.limit);
    }
    // ⭐ S2: COUNT(*)
    if agg.count {
        return Ok((
            vec![("COUNT(*)".to_string(), ColType::I64)],
            vec![vec![ColValue::I64(out_rows.len() as i64)]],
        ));
    }
    if !agg.order.is_empty() {
        out_rows.sort_by(|a, b| sql_order_cmp(a, b, &agg.order));
    }
    let start = (agg.offset as usize).min(out_rows.len());
    let end = match agg.limit {
        Some(l) => (start + l as usize).min(out_rows.len()),
        None => out_rows.len(),
    };
    // 投影到输出列 (与 render_sql_rows 同义); ⭐ F76: out_names 有则用作列名 (AS 别名)
    let cols: Vec<(String, ColType)> = agg
        .proj
        .iter()
        .enumerate()
        .map(|(k, &i)| {
            let c = &agg.schema.columns[i as usize];
            let name = agg
                .out_names
                .get(k)
                .and_then(|o| o.clone())
                .unwrap_or_else(|| c.name.clone());
            (name, c.ty)
        })
        .collect();
    let proj_rows: Vec<Vec<ColValue>> = out_rows[start..end]
        .iter()
        .map(|r| agg.proj.iter().map(|&i| r[i as usize].clone()).collect())
        .collect();
    Ok((cols, proj_rows))
}

// =====================================================================
// ⭐ Z2 (MySQL wire 门面): 帧循环 — 握手/登录状态机 + COM_QUERY
// =====================================================================

/// 伪随机 salt (可打印区间 0x21..0x7E, 兼容各客户端对 NUL 敏感的解析).
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

/// MySQL 帧输入循环: 登录阶段就地回包; 认证后 COM_QUERY 走既有
/// SQL 规划器 (占 conn.next_seq 进重排缓冲, 响应包 seq 恒从 1 起 —
/// COM_QUERY 请求包 seq 恒为 0).
#[allow(clippy::too_many_arguments)]
fn process_sql_input(
    conn: &mut ConnState,
    conn_id: u64,
    worker_id: u32,
    sql_password: &Option<String>,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    tls_config: &Option<std::sync::Arc<rustls::ServerConfig>>,
) {
    use crate::protocol::mysql as my;
    let mut cursor = 0usize;
    while let Some((pkt_seq, n, payload)) = my::read_packet(&conn.read_buf[cursor..]) {
        cursor += n;
        if conn.close_after_flush {
            break;
        }
        let Some(st) = conn.mysql.as_ref() else {
            break; // 防御: 非 mysql 状态的 Sql conn 不存在
        };
        let (phase, salt) = (st.phase, st.salt);
        let pwd = sql_password.as_deref().unwrap_or("");
        // ⭐ F83: phase 0 且 conn 未升级 TLS 时, 短包 + CLIENT_SSL → SSLRequest, 升级后等加密的真响应
        if phase == 0 && conn.tls.is_none() && payload.len() >= 4 {
            let caps = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
            if caps & my::CLIENT_SSL != 0 && payload.len() <= 36 {
                if let Some(cfg) = tls_config {
                    if !conn.start_tls(cfg.clone()) {
                        conn.close_after_flush = true;
                    }
                    // 消费该 SSLRequest 包, 退出循环等 ClientHello + 加密 HandshakeResponse41
                    break;
                }
                // 未配置 TLS 却收到 SSLRequest → 拒
                conn.send_bytes(&my::build_err(pkt_seq.wrapping_add(1), 1043, "TLS not supported"));
                conn.close_after_flush = true;
                break;
            }
        }
        match phase {
            // ---- 等 HandshakeResponse41 ----
            0 => match my::parse_handshake_response(&payload) {
                Ok(login) => {
                    // ⭐ S5: 登录带 database → 认证通过后切库 (不存在 1049 断连)
                    let want_db = login.database.clone().filter(|d| !d.is_empty());
                    let db_ok = |name: &str| {
                        name == default_db.as_ref() || db_view.id_of(name).is_some()
                    };
                    if let Some(d) = &want_db
                        && !db_ok(d)
                    {
                        conn.send_bytes(&my::build_err(
                            pkt_seq.wrapping_add(1),
                            1049,
                            &format!("Unknown database '{d}'"),
                        ));
                        conn.close_after_flush = true;
                        continue;
                    }
                    let native = login
                        .plugin
                        .as_deref()
                        .is_none_or(|p| p == "mysql_native_password");
                    let is_caching = login.plugin.as_deref() == Some("caching_sha2_password");
                    // ⭐ F82: caching_sha2 fast-auth — 服务端知明文口令直接验证 (免 RSA/TLS).
                    //   成功 → fast_auth_success(0x01 0x03)+OK; 失败/其他 → 走 AuthSwitch 兜底.
                    if is_caching && my::caching_sha2_password_ok(&salt, &login.auth_resp, pwd) {
                        conn.send_bytes(&my::build_fast_auth_success(pkt_seq.wrapping_add(1)));
                        conn.send_bytes(&my::build_ok(pkt_seq.wrapping_add(2), 0));
                        if let Some(d) = want_db {
                            conn.current_db = std::sync::Arc::from(d.as_str());
                        }
                        if let Some(st) = conn.mysql.as_mut() {
                            st.phase = 2;
                        }
                    } else if !native || (login.auth_resp.is_empty() && !pwd.is_empty()) {
                        // 客户端默认 caching_sha2 (8.x) 或未带凭据 → 切换插件重试
                        conn.send_bytes(&my::build_auth_switch(pkt_seq.wrapping_add(1), &salt));
                        if let Some(st) = conn.mysql.as_mut() {
                            st.phase = 1;
                            st.pending_db = want_db;
                        }
                    } else if my::native_password_ok(&salt, &login.auth_resp, pwd) {
                        conn.send_bytes(&my::build_ok(pkt_seq.wrapping_add(1), 0));
                        if let Some(d) = want_db {
                            conn.current_db = std::sync::Arc::from(d.as_str());
                        }
                        if let Some(st) = conn.mysql.as_mut() {
                            st.phase = 2;
                        }
                    } else {
                        conn.send_bytes(&my::build_err(
                            pkt_seq.wrapping_add(1),
                            1045,
                            "Access denied",
                        ));
                        conn.close_after_flush = true;
                    }
                }
                Err(e) => {
                    conn.send_bytes(&my::build_err(pkt_seq.wrapping_add(1), 1043, &e));
                    conn.close_after_flush = true;
                }
            },
            // ---- 等 AuthSwitch 响应 (payload = 裸 native token) ----
            1 => {
                if my::native_password_ok(&salt, &payload, pwd) {
                    conn.send_bytes(&my::build_ok(pkt_seq.wrapping_add(1), 0));
                    // ⭐ S5: 二段认证通过 → 应用登录时的 database
                    let pending = conn.mysql.as_mut().and_then(|st| st.pending_db.take());
                    if let Some(d) = pending {
                        conn.current_db = std::sync::Arc::from(d.as_str());
                    }
                    if let Some(st) = conn.mysql.as_mut() {
                        st.phase = 2;
                    }
                } else {
                    conn.send_bytes(&my::build_err(
                        pkt_seq.wrapping_add(1),
                        1045,
                        "Access denied",
                    ));
                    conn.close_after_flush = true;
                }
            }
            // ---- 已认证: 命令阶段 ----
            _ => match payload.first() {
                Some(&my::COM_QUERY) => {
                    let seq = conn.next_seq;
                    conn.next_seq += 1;
                    let cur_db = conn.current_db.clone();
                    match sql::parse(&payload[1..]) {
                        Err(e) => conn.resp_complete(seq, sql_err_bytes(conn.proto, &e)),
                        Ok(stmt) => sql_dispatch_stmt(
                            conn, conn_id, seq, worker_id, &cur_db, default_db, db_view,
                            shard_inboxes, num_shards, stmt,
                        ),
                    }
                }
                Some(&my::COM_PING) => {
                    // 占 seq 保序 (与在途 COM_QUERY 的 FIFO 一致)
                    let seq = conn.next_seq;
                    conn.next_seq += 1;
                    conn.resp_complete(seq, my::build_ok(pkt_seq.wrapping_add(1), 0));
                }
                // ⭐ S5: COM_INIT_DB (mysql cli 的 `USE x`) — 真切库
                Some(&my::COM_INIT_DB) => {
                    let seq = conn.next_seq;
                    conn.next_seq += 1;
                    let name = String::from_utf8_lossy(&payload[1..]).into_owned();
                    let ok = name == default_db.as_ref() || db_view.id_of(&name).is_some();
                    if ok {
                        conn.current_db = std::sync::Arc::from(name.as_str());
                        conn.resp_complete(seq, my::build_ok(pkt_seq.wrapping_add(1), 0));
                    } else {
                        conn.resp_complete(
                            seq,
                            my::build_err(
                                pkt_seq.wrapping_add(1),
                                1049,
                                &format!("Unknown database '{name}'"),
                            ),
                        );
                    }
                }
                // ⭐ P2: 预处理语句族
                Some(&my::COM_STMT_PREPARE) => {
                    let seq = conn.next_seq;
                    conn.next_seq += 1;
                    match sql::parse_prepared(&payload[1..]) {
                        Ok((stmt, params)) => {
                            let id = conn.next_stmt_id;
                            conn.next_stmt_id += 1;
                            conn.mysql_stmts.insert(id, MyPrepared { stmt, params, types: None });
                            conn.resp_complete(seq, my::build_stmt_prepare_ok(id, params));
                        }
                        Err(e) => conn.resp_complete(seq, mysql_err_packet(&e)),
                    }
                }
                Some(&my::COM_STMT_EXECUTE) => {
                    let seq = conn.next_seq;
                    conn.next_seq += 1;
                    // stmt_id 先探 (解参需要 params 数)
                    let stmt_id = if payload.len() >= 5 {
                        u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]])
                    } else {
                        0
                    };
                    let Some(prep) = conn.mysql_stmts.get_mut(&stmt_id) else {
                        conn.resp_complete(seq, my::build_err(1, 1243, "unknown statement id"));
                        continue;
                    };
                    // ⭐ ORM-C: 解参后直接对模板绑定 (bind_params 单次深拷贝),
                    // 省掉此前绕借用的 prep.stmt.clone() 整次拷贝
                    let bound = my::parse_stmt_execute(&payload, prep.params, &mut prep.types)
                        .and_then(|(_, vals)| sql::bind_params(&prep.stmt, &vals));
                    match bound {
                        Ok(stmt) => {
                            // SELECT 类结果需二进制结果集 (渲染点按标记分流)
                            conn.mysql_binary.insert(seq);
                            let cur_db = conn.current_db.clone();
                            sql_dispatch_stmt(
                                conn, conn_id, seq, worker_id, &cur_db, default_db, db_view,
                                shard_inboxes, num_shards, stmt,
                            );
                        }
                        Err(e) => conn.resp_complete(seq, mysql_err_packet(&e)),
                    }
                }
                Some(&my::COM_STMT_CLOSE) => {
                    // 无响应命令 (不占 seq)
                    if payload.len() >= 5 {
                        let id =
                            u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
                        conn.mysql_stmts.remove(&id);
                    }
                }
                Some(&my::COM_STMT_RESET) => {
                    let seq = conn.next_seq;
                    conn.next_seq += 1;
                    conn.resp_complete(seq, my::build_ok(1, 0));
                }
                Some(&my::COM_QUIT) => {
                    conn.close_after_flush = true;
                }
                _ => {
                    let seq = conn.next_seq;
                    conn.next_seq += 1;
                    conn.resp_complete(seq, my::build_err(1, 1047, "unsupported command"));
                }
            },
        }
    }
    if cursor > 0 {
        conn.read_buf.drain(..cursor);
    }
}

/// ⭐ S4: PostgreSQL wire 帧循环 — startup (SSLRequest 拒绝/参数解析) →
/// cleartext 认证 → simple Query. 每语句回复自带 ReadyForQuery (sql_*_bytes).
#[allow(clippy::too_many_arguments)]
fn process_pg_input(
    conn: &mut ConnState,
    conn_id: u64,
    worker_id: u32,
    sql_password: &Option<String>,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    tls_config: &Option<std::sync::Arc<rustls::ServerConfig>>,
) {
    use crate::protocol::pg;
    let pwd = sql_password.as_deref().unwrap_or("");
    let mut cursor = 0usize;
    loop {
        if conn.close_after_flush {
            break;
        }
        match conn.pg_phase {
            // ---- 等 StartupMessage (无 type 帧) ----
            0 => {
                let Some((n, payload)) = pg::read_startup_frame(&conn.read_buf[cursor..])
                else {
                    break;
                };
                cursor += n;
                if payload.len() == 4 {
                    let code = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                    match code {
                        // ⭐ F83: SSLRequest — 配置了 TLS → 回 'S' 并升级; 否则回 'N' 明文回落
                        pg::SSL_REQUEST_CODE => {
                            if let Some(cfg) = tls_config {
                                conn.send_bytes(b"S"); // 明文 'S' (升级前最后一个明文字节)
                                if !conn.start_tls(cfg.clone()) {
                                    conn.close_after_flush = true;
                                }
                                // 后续 StartupMessage 走 TLS, 回到 epoll 等 ClientHello
                                break;
                            }
                            conn.send_bytes(b"N");
                            continue;
                        }
                        pg::GSSENC_REQUEST_CODE => {
                            conn.send_bytes(b"N");
                            continue;
                        }
                        pg::CANCEL_REQUEST_CODE => {
                            conn.close_after_flush = true;
                            break;
                        }
                        _ => {}
                    }
                }
                match pg::parse_startup(payload) {
                    Ok((_user, database)) => {
                        // database 参数 → 切库 (不存在直接拒绝断连)
                        if let Some(dbn) = database
                            && !dbn.is_empty()
                            && dbn != default_db.as_ref()
                        {
                            if db_view.id_of(&dbn).is_some() {
                                conn.current_db = std::sync::Arc::from(dbn.as_str());
                            } else {
                                conn.send_bytes(&pg::build_error(
                                    "3D000",
                                    &format!("database \"{dbn}\" does not exist"),
                                ));
                                conn.close_after_flush = true;
                                break;
                            }
                        }
                        if pwd.is_empty() {
                            conn.send_bytes(&pg::build_auth_ok_bundle(conn_id as u32));
                            conn.pg_phase = 2;
                        } else {
                            // ⭐ F82: 宣告 SCRAM-SHA-256 (取代明文口令), 进 SASL 交换
                            conn.send_bytes(&pg::build_auth_sasl());
                            conn.pg_scram = None;
                            conn.pg_phase = 1;
                        }
                    }
                    Err(e) => {
                        conn.send_bytes(&pg::build_error("08P01", &e));
                        conn.close_after_flush = true;
                    }
                }
            }
            // ---- SASL 交换 (SCRAM-SHA-256): 首条 SASLInitialResponse, 次条 SASLResponse ----
            1 => {
                let Some((n, ty, payload)) = pg::read_frame(&conn.read_buf[cursor..]) else {
                    break;
                };
                cursor += n;
                if ty != b'p' {
                    conn.send_bytes(&pg::build_error("28P01", "expected SASL message"));
                    conn.close_after_flush = true;
                    continue;
                }
                if conn.pg_scram.is_none() {
                    // 首条: SASLInitialResponse (mechanism + client-first)
                    let Some((mech, client_first)) = pg::parse_sasl_initial(payload) else {
                        conn.send_bytes(&pg::build_error("28P01", "malformed SASL initial response"));
                        conn.close_after_flush = true;
                        continue;
                    };
                    if mech != "SCRAM-SHA-256" {
                        conn.send_bytes(&pg::build_error("28P01", "unsupported SASL mechanism"));
                        conn.close_after_flush = true;
                        continue;
                    }
                    match pg::scram_server_first(&client_first) {
                        Some((state, server_first)) => {
                            conn.send_bytes(&pg::build_auth_sasl_continue(&server_first));
                            conn.pg_scram = Some(state);
                        }
                        None => {
                            conn.send_bytes(&pg::build_error("28P01", "malformed SCRAM client-first"));
                            conn.close_after_flush = true;
                        }
                    }
                } else {
                    // 次条: SASLResponse (client-final) → 验证 proof
                    let state = conn.pg_scram.take().expect("scram state present");
                    match pg::scram_verify_final(&state, payload, pwd) {
                        Some(server_final) => {
                            conn.send_bytes(&pg::build_auth_sasl_final(&server_final));
                            conn.send_bytes(&pg::build_auth_ok_bundle(conn_id as u32));
                            conn.pg_phase = 2;
                        }
                        None => {
                            conn.send_bytes(&pg::build_error(
                                "28P01",
                                "password authentication failed",
                            ));
                            conn.close_after_flush = true;
                        }
                    }
                }
            }
            // ---- 已认证: simple Query ----
            _ => {
                let Some((n, ty, payload)) = pg::read_frame(&conn.read_buf[cursor..]) else {
                    break;
                };
                cursor += n;
                match ty {
                    b'Q' => {
                        // 语句预处理: NUL 截断 + trim + 剥尾分号 (多语句报错)
                        let end = payload.iter().position(|&b| b == 0).unwrap_or(payload.len());
                        let text = String::from_utf8_lossy(&payload[..end]);
                        let trimmed = text.trim().trim_end_matches(';').trim();
                        let seq = conn.next_seq;
                        conn.next_seq += 1;
                        if trimmed.is_empty() {
                            // EmptyQueryResponse + ReadyForQuery
                            let mut out = Vec::new();
                            out.push(b'I');
                            out.extend_from_slice(&4u32.to_be_bytes());
                            out.extend_from_slice(&pg::build_ready());
                            conn.resp_complete(seq, out);
                        } else if trimmed.contains(';') {
                            conn.resp_complete(
                                seq,
                                sql_err_bytes(
                                    ProtocolKind::Pg,
                                    "multi-statement query is unsupported",
                                ),
                            );
                        } else {
                            let cur_db = conn.current_db.clone();
                            match sql::parse(trimmed.as_bytes()) {
                                Err(e) => conn
                                    .resp_complete(seq, sql_err_bytes(ProtocolKind::Pg, &e)),
                                Ok(stmt) => sql_dispatch_stmt(
                                    conn, conn_id, seq, worker_id, &cur_db, default_db,
                                    db_view, shard_inboxes, num_shards, stmt,
                                ),
                            }
                        }
                    }
                    b'X' => {
                        conn.close_after_flush = true;
                    }
                    // ---- ⭐ P3: 扩展查询协议 (Parse..Sync 批次) ----
                    b'P' => match pg::parse_parse(payload) {
                        Ok((name, query, oids)) => match sql::parse_prepared(&query) {
                            Ok((stmt, params)) => {
                                conn.pg_stmts.insert(name, PgPrepared { stmt, params, oids });
                                let pc = pg::build_parse_complete();
                                conn.pg_batch.prefix.extend_from_slice(&pc);
                            }
                            Err(e) => {
                                if conn.pg_batch.error.is_none() {
                                    conn.pg_batch.error = Some(e);
                                }
                            }
                        },
                        Err(e) => {
                            if conn.pg_batch.error.is_none() {
                                conn.pg_batch.error = Some(e);
                            }
                        }
                    },
                    b'B' => {
                        if conn.pg_batch.error.is_some() {
                            continue; // skip-to-Sync
                        }
                        let r = pg::parse_bind(payload).and_then(|bind| {
                            let prep = conn
                                .pg_stmts
                                .get(&bind.statement)
                                .ok_or_else(|| format!("unknown statement '{}'", bind.statement))?;
                            if bind.binary_results {
                                return Err("binary result format is unsupported".into());
                            }
                            if bind.params.len() != prep.params as usize {
                                return Err(format!(
                                    "expected {} parameters, got {}",
                                    prep.params,
                                    bind.params.len()
                                ));
                            }
                            let mut vals = Vec::with_capacity(bind.params.len());
                            for (i, raw) in bind.params.iter().enumerate() {
                                let oid = prep.oids.get(i).copied().unwrap_or(0);
                                vals.push(pg::decode_param(
                                    raw.as_deref(),
                                    bind.formats.get(i).copied().unwrap_or(0),
                                    oid,
                                )?);
                            }
                            sql::bind_params(&prep.stmt, &vals)
                        });
                        match r {
                            Ok(stmt) => {
                                conn.pg_batch.bound = Some(stmt);
                                let bc = pg::build_bind_complete();
                                conn.pg_batch.prefix.extend_from_slice(&bc);
                            }
                            Err(e) => conn.pg_batch.error = Some(e),
                        }
                    }
                    b'D' => {
                        if conn.pg_batch.error.is_some() {
                            continue;
                        }
                        // Describe(statement) → ParameterDescription + NoData
                        // (列描述延迟到结果流 RowDescription — pgx/node-postgres
                        //  的 Describe(portal) 流由结果自带 T 满足).
                        if let Ok((b'S', name)) = pg::parse_target(payload)
                            && let Some(prep) = conn.pg_stmts.get(&name)
                        {
                            let pd = pg::build_param_description(&prep.oids, prep.params);
                            conn.pg_batch.prefix.extend_from_slice(&pd);
                            let nd = pg::build_no_data();
                            conn.pg_batch.prefix.extend_from_slice(&nd);
                        }
                    }
                    b'E' => {
                        conn.pg_batch.has_execute = true;
                    }
                    b'C' => {
                        // Close (语句/portal) → CloseComplete
                        if let Ok((b'S', name)) = pg::parse_target(payload) {
                            conn.pg_stmts.remove(&name);
                        }
                        let cc = pg::build_close_complete();
                        conn.pg_batch.prefix.extend_from_slice(&cc);
                    }
                    b'H' => {
                        // Flush: v1 以 Sync 为响应边界 (asyncpg 依赖 Flush 的
                        // 路径记录为 gap)
                    }
                    b'S' => {
                        let batch = std::mem::take(&mut conn.pg_batch);
                        let seq = conn.next_seq;
                        conn.next_seq += 1;
                        if let Some(e) = batch.error {
                            let mut out = batch.prefix;
                            out.extend_from_slice(&pg::build_error("42601", &e));
                            out.extend_from_slice(&pg::build_ready());
                            conn.resp_complete(seq, out);
                        } else if batch.has_execute && let Some(bound) = batch.bound {
                            // 结果主体 (T+D+C+Z / C+Z) 由既有渲染产出,
                            // 前缀在 resp_complete 单点拼接
                            conn.pg_ext.insert(seq, batch.prefix);
                            let cur_db = conn.current_db.clone();
                            sql_dispatch_stmt(
                                conn,
                                conn_id,
                                seq,
                                worker_id,
                                &cur_db,
                                default_db,
                                db_view,
                                shard_inboxes,
                                num_shards,
                                bound,
                            );
                        } else {
                            let mut out = batch.prefix;
                            out.extend_from_slice(&pg::build_ready());
                            conn.resp_complete(seq, out);
                        }
                    }
                    // Parse/Bind/... 扩展协议未支持; 其它消息容错回错误不断连
                    _ => {
                        let seq = conn.next_seq;
                        conn.next_seq += 1;
                        conn.resp_complete(
                            seq,
                            sql_err_bytes(
                                ProtocolKind::Pg,
                                "extended query protocol is unsupported",
                            ),
                        );
                    }
                }
            }
        }
    }
    if cursor > 0 {
        conn.read_buf.drain(..cursor);
    }
}

/// ⭐ H1: HTTP/1.1 REST 帧循环 — preflight / Bearer 鉴权 / 路由分发.
/// keep-alive pipeline 复用 seq 重排 (每请求一 seq); Connection: close →
/// close_after_flush (pending 回复出完再关).
#[allow(clippy::too_many_arguments)]
fn process_http_input(
    conn: &mut ConnState,
    conn_id: u64,
    worker_id: u32,
    http_token: &Option<String>,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    limits: &KvLimits,
    num_shards_total: usize,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    use crate::protocol::http as h;
    let cors = crate::http_config::cors_origin();
    let mut cursor = 0usize;
    loop {
        if conn.close_after_flush {
            break;
        }
        match h::parse_request(&conn.read_buf[cursor..]) {
            Ok(None) => break,
            Err((code, msg)) => {
                crate::metrics::HTTP_ERRORS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let seq = conn.next_seq;
                conn.next_seq += 1;
                conn.resp_complete(seq, h::build_response(code, &h::error_body(msg), cors, false));
                conn.close_after_flush = true;
                break;
            }
            Ok(Some((n, req))) => {
                cursor += n;
                crate::metrics::HTTP_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if !req.keep_alive {
                    conn.close_after_flush = true;
                }
                handle_http_request(
                    conn, conn_id, worker_id, req, http_token, default_db, db_view, limits,
                    num_shards_total, shard_inboxes, num_shards,
                );
            }
        }
    }
    if cursor > 0 {
        conn.read_buf.drain(..cursor);
    }
}

/// 单请求路由分发 (worker 本地端点就地渲染, KV/SQL 走 shard 任务).
#[allow(clippy::too_many_arguments)]
fn handle_http_request(
    conn: &mut ConnState,
    conn_id: u64,
    worker_id: u32,
    req: crate::protocol::http::HttpRequest,
    http_token: &Option<String>,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    limits: &KvLimits,
    num_shards_total: usize,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    use crate::protocol::http as h;
    use std::sync::atomic::Ordering::Relaxed;
    let cors = crate::http_config::cors_origin();
    let seq = conn.next_seq;
    conn.next_seq += 1;
    let ka = req.keep_alive;
    let fail = |conn: &mut ConnState, seq, code: u16, msg: &str| {
        crate::metrics::HTTP_ERRORS.fetch_add(1, Relaxed);
        conn.resp_complete(seq, h::build_response(code, &h::error_body(msg), cors, ka));
    };
    // OPTIONS preflight (免鉴权)
    if req.method == "OPTIONS" {
        conn.resp_complete(seq, h::build_preflight(cors, ka));
        return;
    }
    // Bearer 鉴权 (白名单: /metrics /v1/status — 监控接入惯例)
    if let Some(token) = http_token
        && req.path != "/metrics"
        && req.path != "/v1/status"
    {
        let ok = req
            .authorization
            .as_deref()
            .and_then(|a| a.strip_prefix("Bearer "))
            .is_some_and(|t| t == token);
        if !ok {
            fail(conn, seq, 401, "unauthorized");
            return;
        }
    }
    // db 选择: query 参数 (KV) / body 字段 (SQL); 缺省 = default_db
    let resolve_db = |name: Option<&str>| -> Result<std::sync::Arc<str>, String> {
        match name.filter(|s| !s.is_empty()) {
            None => Ok(conn_default(default_db)),
            Some(d) if d == default_db.as_ref() || db_view.id_of(d).is_some() => {
                Ok(std::sync::Arc::from(d))
            }
            Some(d) => Err(format!("Unknown database '{d}'")),
        }
    };
    match (req.method.as_str(), req.path.as_str()) {
        // ---- ⭐ H4: 可观测性端点 (worker 本地零任务) ----
        ("GET", "/metrics") => {
            let m = format!(
                "# TYPE nexusdb_http_requests_total counter\n\
                 nexusdb_http_requests_total {}\n\
                 # TYPE nexusdb_http_errors_total counter\n\
                 nexusdb_http_errors_total {}\n\
                 # TYPE nexusdb_sql_queries_total counter\n\
                 nexusdb_sql_queries_total {}\n\
                 # TYPE nexusdb_kv_ops_total counter\n\
                 nexusdb_kv_ops_total {}\n\
                 # TYPE nexusdb_uptime_seconds gauge\n\
                 nexusdb_uptime_seconds {}\n",
                crate::metrics::HTTP_REQUESTS.load(Relaxed),
                crate::metrics::HTTP_ERRORS.load(Relaxed),
                crate::metrics::SQL_QUERIES.load(Relaxed),
                crate::metrics::KV_OPS.load(Relaxed),
                crate::metrics::uptime_seconds(),
            );
            conn.resp_complete(seq, h::build_text_response(200, m.as_bytes(), ka));
        }
        ("GET", "/v1/status") => {
            let body = serde_json::json!({
                "version": env!("CARGO_PKG_VERSION"),
                "uptime_seconds": crate::metrics::uptime_seconds(),
                "num_shards": num_shards_total,
                "protocols": {"binary": 5433, "resp": 6379, "mysql": 5434, "pg": 5435, "http": 6778},
            });
            conn.resp_complete(
                seq,
                h::build_response(200, &serde_json::to_vec(&body).unwrap_or_default(), cors, ka),
            );
        }
        ("GET", "/v1/debug/sql-cache") => {
            let body = {
                use std::sync::atomic::Ordering::Relaxed;
                let sh = &conn.sql_shared;
                serde_json::json!({
                    "worker_schemas": conn.sql_cache.borrow().schemas.len(),
                    "routes": sh.routes.read().unwrap().len(),
                    "created_here": sh.created_here.read().unwrap().len(),
                    "ddl_epoch": sh.ddl_epoch.load(Relaxed),
                    "route_pruned": sh.route_pruned.load(Relaxed),
                    "route_bypassed": sh.route_bypassed.load(Relaxed),
                })
            };
            conn.resp_complete(
                seq,
                h::build_response(200, &serde_json::to_vec(&body).unwrap_or_default(), cors, ka),
            );
        }
        // ---- ⭐ H3: SQL ----
        ("POST", "/v1/sql") => {
            let parsed: Result<serde_json::Value, _> = serde_json::from_slice(&req.body);
            let Ok(body) = parsed else {
                fail(conn, seq, 400, "body must be JSON");
                return;
            };
            let Some(query) = body.get("query").and_then(|q| q.as_str()) else {
                fail(conn, seq, 400, "missing 'query' field");
                return;
            };
            let db = match resolve_db(body.get("db").and_then(|d| d.as_str())) {
                Ok(d) => d,
                Err(e) => {
                    fail(conn, seq, 400, &e);
                    return;
                }
            };
            match sql::parse(query.as_bytes()) {
                Err(e) => conn.resp_complete(seq, sql_err_bytes(ProtocolKind::Http, &e)),
                Ok(stmt) => sql_dispatch_stmt(
                    conn, conn_id, seq, worker_id, &db, default_db, db_view, shard_inboxes,
                    num_shards, stmt,
                ),
            }
        }
        // ---- ⭐ H2: KV ----
        (m, p) if p.starts_with("/v1/kv/") => {
            let rest = &p["/v1/kv/".len()..];
            let Some((table_raw, key_raw)) = rest.split_once('/') else {
                fail(conn, seq, 404, "expected /v1/kv/{table}/{key}");
                return;
            };
            let table = String::from_utf8_lossy(&h::percent_decode(table_raw)).into_owned();
            let key = h::percent_decode(key_raw);
            if table.is_empty() || key.is_empty() {
                fail(conn, seq, 400, "empty table or key");
                return;
            }
            if key.len() > limits.max_key_bytes {
                fail(conn, seq, 400, "key too long");
                return;
            }
            let db = match resolve_db(h::query_param(&req.query, "db")) {
                Ok(d) => d,
                Err(e) => {
                    fail(conn, seq, 400, &e);
                    return;
                }
            };
            let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
            crate::metrics::KV_OPS.fetch_add(1, Relaxed);
            let (op, kv) = match m {
                "GET" => (
                    BatchOp::Get { db, table: table_arc, key },
                    HttpKvOp::Get,
                ),
                "DELETE" => (
                    BatchOp::Delete { db, table: table_arc, key },
                    HttpKvOp::Delete,
                ),
                "PUT" | "POST" => {
                    // body: {"value": <string|number>} — tag 与 RESP 同源
                    let Ok(body) = serde_json::from_slice::<serde_json::Value>(&req.body) else {
                        fail(conn, seq, 400, "body must be JSON");
                        return;
                    };
                    let stored = match body.get("value") {
                        Some(serde_json::Value::String(s)) => crate::value_codec::encode_value(
                            shard_manager::request::VALUE_TAG_RAW,
                            s.as_bytes(),
                        ),
                        Some(v) if v.is_i64() => {
                            shard_manager::value_num::encode_i64(v.as_i64().unwrap())
                        }
                        Some(v) if v.is_f64() => {
                            shard_manager::value_num::encode_f64(v.as_f64().unwrap())
                        }
                        _ => {
                            fail(conn, seq, 400, "missing 'value' (string or number)");
                            return;
                        }
                    };
                    if stored.len().saturating_sub(1) > limits.max_value_bytes {
                        fail(conn, seq, 400, "value too long");
                        return;
                    }
                    (
                        BatchOp::Put { db, table: table_arc, key, val: stored },
                        HttpKvOp::Put,
                    )
                }
                _ => {
                    fail(conn, seq, 405, "method not allowed");
                    return;
                }
            };
            conn.http_ctx.insert(seq, HttpReqCtx { op: kv, keep_alive: ka });
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        _ => fail(conn, seq, 404, "not found"),
    }
}

/// db 缺省 helper (borrow checker 拆分用).
fn conn_default(default_db: &std::sync::Arc<str>) -> std::sync::Arc<str> {
    default_db.clone()
}

/// SQL 错误 → MySQL ERR 包 (seq 1; 错误码按消息粗分类).
fn mysql_err_packet(msg: &str) -> Vec<u8> {
    let code = if msg.contains("unknown column") {
        1054
    } else if msg.contains("duplicate key") {
        1062 // ER_DUP_ENTRY — UNIQUE 冲突 (ORM 据此识别 IntegrityError)
    } else if msg.contains("serialization failure") {
        1213 // MySQL deadlock/serialization 惯用重试码
    } else if msg.contains("read-only transaction") {
        1792
    } else if msg.contains("Unknown database") {
        1049
    } else if msg.contains("has no schema") || msg.contains("doesn't exist") {
        1146 // ER_NO_SUCH_TABLE — ORM has_table 据此判表不存在后发 CREATE
    } else if msg.contains("expected") || msg.contains("unexpected") || msg.contains("unterminated")
    {
        1064
    } else if msg.contains("Access denied") {
        1045
    } else {
        1105
    };
    crate::protocol::mysql::build_err(1, code, msg)
}
