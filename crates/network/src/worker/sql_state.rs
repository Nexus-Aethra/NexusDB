//! ⭐ X3 (解耦 2026-08): SQL 规划/执行状态结构体 (拆自 mod.rs).
//! 事务缓冲、DML/DDL 聚合、JOIN/子查询/派生表编排、SELECT 规划、schema 缓存.

use std::collections::HashMap;

use crate::protocol::sql::{self, Cond, Pred, SqlStmt};
use storage::row::ColValue;
use storage::schema::{ColType, TableSchema};
use shard_manager::BatchOp;
use super::{SysQuerySpec, MyPrepared, MysqlState};

/// ⭐ 方案 A (调优): EstimateRows 小表阈值 — 双表 Inner JOIN 两表行数均 ≤ 此值
/// → 跳过 distinct/ranges 统计收集, 直接按行数决策驱动表 (小表 JOIN 固定只 1 轮
/// 行数广播; 索引选择收益在极小表上可忽略). 调优面: 调大 → 更多表跳过统计 (省
/// 广播轮次); 调小 → 更多表走统计 (索引选择更准).
pub const EST_SKIP_STATS_ROWS: u64 = 1024;

/// ⭐ 事务 v1 (F61): conn 层事务缓冲 — BEGIN..COMMIT 间写语句截流,
/// shard/调度器零事务状态 (时间维度: 交互式间隙不占 shard;
/// 空间维度: 跨 shard 编排本就在 worker). COMMIT 时按 shard 分组为
/// TxnApply 原子批. 断连/drop 自然丢弃 = 隐式回滚.
pub struct TxnState {
    /// 保序 write_set (只 append; 同 key 多写按序重放语义正确).
    pub ops: Vec<BatchOp>,
    /// (db, table, pk) → 最新 op 下标 (RYOW pk 点查).
    pub index: HashMap<(String, String, Vec<u8>), usize>,
    /// 粗估字节 (上限护栏).
    pub bytes: usize,
    /// ⭐ v2 (F62): 隔离级别 (Serializable = OCC 读集验证).
    pub iso: sql::TxnIso,
    /// ⭐ v2 (F62): 只读事务 (写语句拒 25006).
    pub read_only: bool,
    /// ⭐ v2 (F62): OCC 读集 — 首读指纹为准 (不覆盖); ROLLBACK TO 后
    /// 保留 (保守更严格, 正确性无损).
    pub read_set: HashMap<(String, String, Vec<u8>), Option<u32>>,
    /// ⭐ v2 (F62): savepoint 栈 (name, ops 水位).
    pub savepoints: Vec<(String, usize)>,
}

impl TxnState {
    pub fn new(iso: sql::TxnIso, read_only: bool) -> Self {
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
pub const TXN_MAX_OPS: usize = 8192;
pub const TXN_MAX_BYTES: usize = 8 * 1024 * 1024;

/// ⭐ 事务 v1 (F61): COMMIT 的 TxnApply 多 shard 计数聚合.
pub struct SqlTxnAgg {
    pub remaining: usize,
    pub applied: u64,
    pub error: Option<String>,
}

/// ⭐ F65: 全局 UNIQUE INSERT 编排状态机 (autocommit 单行).
/// 顺序推进: 逐列 Reserve → (committed 冲突时) Verify → 写行 → 逐列 Confirm.
/// 至多一个在途 shard op, 每个 reply 推进一步 (契合 worker 事件驱动).
pub struct SqlUniqueIns {
    pub db: std::sync::Arc<str>,
    pub table: String,
    pub schema: std::sync::Arc<TableSchema>,
    pub pk: Vec<u8>,
    pub values: Vec<ColValue>,
    /// 待处理的全局唯一列: (iid, enc_val).
    pub guc: Vec<(u32, Vec<u8>)>,
    pub txn_id: u64,
    pub phase: UniquePhase,
    /// 当前处理到 guc 的下标 (reserve/confirm 阶段逐个推进).
    pub idx: usize,
    /// 已成功 reserve/steal 的列数 (回滚时 release guc[0..reserved]).
    pub reserved: usize,
}

#[derive(PartialEq)]
pub enum UniquePhase {
    Reserve,
    Verify,
    Write,
    Confirm,
}

/// ⭐ S1: DML 计数聚合 (INSERT 多行 / DELETE/UPDATE phase2 / DROP 广播).
/// 完成 → OK affected=n; DeleteExisted(true) 与 PutOk 各计 1.
pub struct SqlDmlAgg {
    pub remaining: usize,
    pub affected: u64,
    pub error: Option<String>,
    /// DROP TABLE: 完成时清 worker 缓存 (schemas/routes/created_here),
    /// 且 affected 渲染为 0 (广播 PutOk 不是行数).
    pub drop_key: Option<(String, String)>,
}

/// ⭐ PG 兼容 (引用完整性, FMT_VER 8): 外键 INSERT 存在性预检状态.
/// 流程: 收集全部父表引用 → 发 RowGet 存在性检查 (real seq) → 全存在则
/// 注册 sql_dml_agg 发原 RowPut, 任一缺失拒. 以 real seq 为 key.
#[derive(Debug)]
pub struct SqlFkIns {
    /// 剩余待回的存在性检查数.
    pub remaining: usize,
    /// 已确认存在的引用数.
    pub ok: usize,
    /// 缺失的引用 (父表, 编码) — 非空即拒.
    pub missing: Vec<(String, Vec<u8>)>,
    /// 失败错误.
    pub error: Option<String>,
    /// 原 INSERT 的 RowPut op (全通过后发).
    pub ops: Vec<BatchOp>,
    /// 本表 schema (bloom 喂路由用).
    pub schema: std::sync::Arc<TableSchema>,
    /// 原 db (bloom 喂路由用).
    pub db: std::sync::Arc<str>,
}

/// ⭐ PG 兼容 (multi-statement, 2026-08): PG simple Query 多语句顺序执行状态.
/// story-loom 迁移把整文件作为一条 multi-statement Exec → 需顺序执行每条
/// DDL (纯 DDL 走 sql_ddl_agg, 每条完成推进下一条), 全部完成回原 seq.
#[derive(Debug)]
pub struct MultiStmt {
    /// 剩余待执行的语句 (SQL 文本).
    pub stmts: std::collections::VecDeque<String>,
    /// 首个语句子 seq (用于建 multi_sub_seq 映射基线).
    pub base_sub_seq: u64,
    /// 首个子 seq 已 dispatch 标记 (续跑从 base+idx).
    pub dispatched: usize,
    /// 是否遇错 (错则中断后续, 回错误).
    pub error: Option<String>,
    /// 当前语句类型 (1=DDL 走 ddl_agg, 2=DML 走 dml_agg, 0=同步/其他).
    pub cur_kind: u8,
    /// 发起连接的 id (同步语句推进 multi_step 用).
    pub conn_id: u64,
    /// ⭐ PG 兼容: 每条语句的 CommandComplete 累积 (multi-statement 需逐条
    /// 响应, 否则 pgx 等不足 N 个 CommandComplete 而挂起).
    pub cmd_bytes: Vec<u8>,
}

/// ⭐ X3: SELECT pk 点查渲染上下文 (seq → 状态).
pub struct SqlRowCtx {
    pub schema: std::sync::Arc<TableSchema>,
    pub conds: Pred<Cond>,
    /// ⭐ O1: 投影列号.
    pub proj: Vec<u16>,
    /// ⭐ S2: COUNT(*) — 回单行 0/1.
    pub count: bool,
    /// ⭐ v2 (F62): OCC 读集记录坐标 (SERIALIZABLE 事务内的 pk 点查).
    pub read_key: Option<(String, String, Vec<u8>)>,
    /// ⭐ RYOW (F63): 事务内 UPDATE 基于已提交盘行时, 读盘后叠加的 sets
    /// (值或表达式).
    pub ryow_overlay: Vec<(u16, storage::row::SetVal)>,
    /// ⭐ F76: 投影输出列名 (与 proj 同序; None = 用 schema 列名, 空 vec = 全 None).
    pub out_names: Vec<Option<String>>,
    pub row: Option<Vec<ColValue>>,
    pub error: Option<String>,
}

/// schema 缓存 miss 时挂起的语句 (GetSchemaOp 结果到达后续跑).
pub struct PendingSql {
    pub stmt: SqlStmt,
    pub db: std::sync::Arc<str>,
    pub table: String,
}

/// ⭐ F71 (子查询): 非关联 WHERE 子查询编排 — 顺序跑内层→折叠→重跑外层.
/// inners 按 DFS 左右序; 每个内层跑完 materialize 行集存 results; 全部完→fold→重跑.
pub struct SubqCtx {
    pub outer: SqlStmt,
    pub db: std::sync::Arc<str>,
    pub inners: Vec<SqlStmt>,
    pub results: Vec<Vec<Vec<ColValue>>>, // 每内层的行集 (投影后)
    pub cur: usize,
}

/// ⭐ F71: 子查询内层捕获行上限 (防 OOM; 超限报错). EXISTS 只需存在性 /
/// IN 去重后的精确上限在 fold_one_subq 按叶子类型判定; 此值仅作捕获阶段 OOM 护栏.
pub const SUBQ_IN_MAX: usize = 65_536;

/// ⭐ F72 (派生表): FROM `(SELECT ...) alias` 编排 — 内层物化完成后的去向.
/// ⭐ F75: Standalone = 单独派生表 (worker 内存执行外层);
/// JoinFrom = 派生表作 JOIN 首表 (物化行预填 tables[0] 后转 JOIN 状态机).
pub enum DerivedCtx {
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

/// SELECT 访问路径 (worker 过滤器选择).
#[derive(Debug)]
pub enum SqlPlan {
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
        /// ⭐ PG 兼容 (范围查): 扫主键 B+Tree 区间 (主键列范围谓词, 非二级索引).
        pk: bool,
    },
    /// ⭐ S2: 无可用索引 → 广播全表扫 + 全条件残余过滤.
    FullScan,
    /// ⭐ M2 (2026-08): OR → 索引并集 — 多个同索引列等值/范围分支,
    /// 分别 IndexScan 后 worker 合并去重 (避免全表扫).
    IndexUnion {
        /// (索引列名, 分支界)
        branches: Vec<(u16, Option<ColValue>, Option<ColValue>)>,
    },
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
pub struct SqlWorkerCache {
    /// (db, table) → schema (CREATE 或 GetSchemaOp 填充).
    /// ⭐ ORM-B2: per-worker 零锁; 失效靠进程级 DDL epoch (陈旧即整体清空).
    pub schemas: HashMap<(String, String), std::sync::Arc<TableSchema>>,
    /// 本 worker 已同步到的 DDL epoch (与 SqlSharedRoutes::ddl_epoch 比对).
    pub local_epoch: u64,
}

/// per-worker 共享 schema 缓存 (Rc 单线程; ConnState 持有).
pub type SharedSqlCache = std::rc::Rc<std::cell::RefCell<SqlWorkerCache>>;

/// ⭐ F71: materialize 返回 — (输出列定义, 行集).
pub type MatResult = Result<(Vec<(String, ColType)>, Vec<Vec<ColValue>>), String>;
