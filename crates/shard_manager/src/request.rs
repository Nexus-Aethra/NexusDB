//! ShardRequest / ShardResponse enum: 跨线程消息.
//!
//! ## 2PC 协议 (T14)
//!
//! 跨 shard 元数据操作 (create_db / create_table) 走两阶段提交:
//!
//! ```text
//! Coordinator                              Shard N
//!    │─── Prepare{op, txn_id} ──────────────→│
//!    │←────────── Ack/Err ───────────────────│
//!    │  (所有 shard ack → Commit)
//!    │  (任一 shard err → Abort)
//!    │─── Commit{op, txn_id} ───────────────→│
//!    │←────────── CommitOk ──────────────────│
//!    │  或
//!    │─── Abort{op, txn_id} ────────────────→│
//!    │←────────── AbortOk ───────────────────│
//! ```
//!
//! **Prepare 语义**: 尝试执行操作 (create_db / create_table).
//! 成功后操作已落盘, 但标记为 "pending" (可见性由 Commit 控制).
//! **Commit 语义**: 将 pending 操作标记为已提交 (可见).
//! **Abort 语义**: 回滚 Prepare 阶段的操作 (drop_db / drop_table).
//!
//! **MVP 简化**: Prepare 直接执行操作 (不区分 pending/committed),
//! Commit 是 no-op, Abort 执行 reverse op. 这仍然提供 all-or-nothing 保证:
//! - 全部 Prepare 成功 → Commit (no-op, 操作已生效)
//! - 任一 Prepare 失败 → Abort 所有已成功的 shard (回滚)
//!
//! ## T15 async API
//!
//! `reply: ReplySender` 替代原来的 `SyncSender<ShardResponse>`.
//! 调用方拿到 `PendingReply::new() -> (ReplySender, ReplyFuture)`,
//! 把 `ReplySender` 塞进 `ShardRequest` 发给 shard, 持有 `ReplyFuture` 等.
//! shard 端 `reply.send(...)` 会自动 wake 调用方.
//!
//! ## T19 async network stack
//!
//! 每个 Put/Get/Delete 加 `req_id: u64` 字段 (默认 0 表示"网络层未启用,
//! 走 channel reply"). 当 `req_id > 0` 且 `ShardManager` 启用了 `reply_bus`
//! 时, shard 端会**同时**写入 reply_bus (用于 worker 异步路由), 原有
//! `reply.send(...)` 仍然调用, 但**不阻塞**任何线程.

use crate::reply::ReplySender;

/// Shard ID = `[0, num_shards)`.
pub type ShardId = usize;

/// 全局唯一事务 ID (单调递增).
pub type TxnId = u64;

/// 单条请求: ShardManager 主线程 → shard 线程.
///
/// **T15 更新**: reply 是 `ReplySender` (waker-based), 替代 `SyncSender` 阻塞 reply.
///   这样 ShardManager 异步 API 不会阻塞调用线程 (适合 Tokio/Axum 集成).
pub enum ShardRequest {
    /// 插入 / 更新 KV.
    Put {
        db: String,
        table: String,
        key: Vec<u8>,
        val: Vec<u8>,
        /// 网络层 req_id (默认 0 表示旧行为: 仅 channel reply).
        /// 当 `> 0` 且 `ShardManager::enable_reply_bus` 被调用过,
        /// shard 完成后会同时 push 一份到 reply_bus.
        req_id: u64,
        reply: ReplySender,
    },
    /// 点查.
    Get {
        db: String,
        table: String,
        key: Vec<u8>,
        req_id: u64,
        reply: ReplySender,
    },
    /// 删除.
    Delete {
        db: String,
        table: String,
        key: Vec<u8>,
        req_id: u64,
        reply: ReplySender,
    },
    /// 在本 shard 创建表 (单 shard 操作, 不跨 shard).
    CreateTable {
        db: String,
        table: String,
        reply: ReplySender,
    },
    /// 在本 shard 创建 db (MVP: 单 shard, 未来 T14 改 2PC 跨 shard).
    CreateDb { db: String, reply: ReplySender },
    // =================================================================
    // ⭐ T14: 2PC 协议消息
    // =================================================================
    /// 2PC Prepare: 准备创建 db (尝试执行, 失败可回滚).
    PrepareCreateDb {
        db: String,
        txn_id: TxnId,
        reply: ReplySender,
    },
    /// 2PC Commit: 确认创建 db (no-op, Prepare 已生效).
    CommitCreateDb {
        db: String,
        txn_id: TxnId,
        reply: ReplySender,
    },
    /// 2PC Abort: 回滚创建 db (drop_db).
    AbortCreateDb {
        db: String,
        txn_id: TxnId,
        reply: ReplySender,
    },

    /// 2PC Prepare: 准备创建表.
    PrepareCreateTable {
        db: String,
        table: String,
        txn_id: TxnId,
        reply: ReplySender,
    },
    /// 2PC Commit: 确认创建表 (no-op, Prepare 已生效).
    CommitCreateTable {
        db: String,
        table: String,
        txn_id: TxnId,
        reply: ReplySender,
    },
    /// 2PC Abort: 回滚创建表 (drop_table).
    AbortCreateTable {
        db: String,
        table: String,
        txn_id: TxnId,
        reply: ReplySender,
    },

    /// 关闭 shard (Shutting down 流程).
    Shutdown { reply: ReplySender },
    /// ⭐ Flush: 把所有 dirty nowchunks 落盘并插入 chunk_list.
    /// 后置: 所有写入数据 durability = disk, chunk_list 命中.
    Flush { reply: ReplySender },
    /// ⭐ 批量操作: 多个 ops 一次性提交, 一次性回复.
    Batch {
        ops: Vec<BatchOp>,
        req_id: u64,
        reply: ReplySender,
    },
}

/// 单个 batch 操作 (不带 reply, batch 整体回复).
///
/// ⭐ 热路径优化: db/table 用 `Arc<str>` — worker 每 op 仅引用计数,
/// 不再每请求两次 String 堆分配 (275K ops/s 下省 550K allocs/s).
#[derive(Debug, Clone)]
pub enum BatchOp {
    Put { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, val: Vec<u8> },
    Get { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8> },
    Delete { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8> },
    /// ⭐ MGET (单 shard 分片): worker 已按 key 路由分好组,
    /// shard 内走 LeafGuide 区间复用批量读.
    MultiGet { db: std::sync::Arc<str>, table: std::sync::Arc<str>, keys: Vec<Vec<u8>> },
    /// ⭐ MSET (单 shard 分片): shard 内批量写 (同 leaf 一次提交).
    MultiPut { db: std::sync::Arc<str>, table: std::sync::Arc<str>, pairs: Vec<(Vec<u8>, Vec<u8>)> },
    /// ⭐ MSETNX (单 shard 分片): 本组全部 key 不存在才写, 返回是否写入
    /// (Integer 1/0). 跨 shard 非原子 (已记为 gap).
    MultiPutNx { db: std::sync::Arc<str>, table: std::sync::Arc<str>, pairs: Vec<(Vec<u8>, Vec<u8>)> },
    /// ⭐ INCR/DECR/INCRBY: shard 端 RMW (单线程 shard 内天然原子).
    /// stored value 按 `[type_tag][payload]` 约定 (tag 见 VALUE_TAG_RAW).
    /// ⭐ 数值原生存储: 结果写回 TAG_I64 8B LE 二进制 (非十进制字符串).
    Incr { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, delta: i64 },
    /// ⭐ INCRBYFLOAT: shard 端 RMW, 结果写回 TAG_F64 8B LE 二进制.
    IncrFloat { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, delta: f64 },
    /// ⭐ APPEND: shard 端 RMW, 返回追加后长度.
    Append { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, suffix: Vec<u8> },
    /// ⭐ SETNX: 不存在才写 (val 已带 tag). 返回是否写入.
    SetNx { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, val: Vec<u8> },
    /// ⭐ GETDEL: 读旧值 + 删除, 返回旧值 (GetValue). 旧值是溢出链则释放.
    GetDel { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8> },
    /// ⭐ GETSET: 写新值 (val 已带 tag) + 返回旧值 (GetValue).
    GetSet { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, val: Vec<u8> },
    /// ⭐ SETRANGE: 从 offset 覆盖写 data (零扩展), 结果存 TAG_RAW,
    /// 返回新长度 (Integer).
    SetRange { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, offset: u32, data: Vec<u8> },
    // ---- ⭐ Phase H: Hash (全部单 key 路由, 一个 hash 的所有 field 同 shard) ----
    /// HSET 多 field: 返回新增 field 数 (Integer). value 带 tag.
    HSet { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, pairs: Vec<(Vec<u8>, Vec<u8>)> },
    /// HSETNX: field 不存在才写, 返回 1/0 (Integer).
    HSetNx { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, field: Vec<u8>, val: Vec<u8> },
    /// HGET: 返回 field 值 (GetValue).
    HGet { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, field: Vec<u8> },
    /// HMGET: 多 field 读, 按输入序 (Values).
    HMGet { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, fields: Vec<Vec<u8>> },
    /// HDEL: 删多 field, 返回实删数 (Integer).
    HDel { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, fields: Vec<Vec<u8>> },
    /// HLEN: field 数 (Integer).
    HLen { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8> },
    /// HGETALL: 全部 (field, value) 对 (Pairs); HKEYS/HVALS/HSCAN 复用.
    HGetAll { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8> },
    /// HINCRBY: field 整数 RMW (结果 TAG_I64), 返回新值 (Integer).
    HIncrBy { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, field: Vec<u8>, delta: i64 },
    /// HINCRBYFLOAT: field 浮点 RMW (结果 TAG_F64), 返回新值 (Double).
    HIncrByFloat { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, field: Vec<u8>, delta: f64 },
    // ---- ⭐ Phase Set: Set (单 key 路由; 代数类跨 key 由 worker 分组聚合) ----
    /// SADD: 返回新增成员数 (Integer).
    SAdd { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, members: Vec<Vec<u8>> },
    /// SREM: 返回实删数 (Integer).
    SRem { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, members: Vec<Vec<u8>> },
    /// SISMEMBER: 返回 1/0 (Integer).
    SIsMember { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, member: Vec<u8> },
    /// SCARD: 成员数 (Integer).
    SCard { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8> },
    /// SMEMBERS/SSCAN/代数类取成员: 返回全部成员 (Members).
    SMembers { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8> },
    /// SPOP: 随机弹出一个成员 (Members 0/1 项).
    SPop { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8> },
    /// SRANDMEMBER: 随机返回一个成员不删 (Members 0/1 项).
    SRandMember { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8> },
    // ---- ⭐ Phase L: List (全部单 key 路由) ----
    /// LPUSH(left=true)/RPUSH: 返回新长度 (Integer). val 带 tag.
    LPush { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, values: Vec<Vec<u8>>, left: bool },
    /// LPOP(left=true)/RPOP: 弹出 count 个 (Members; count=1 时 worker 回单 bulk).
    LPop { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, left: bool, count: u32 },
    /// LLEN: 长度 (Integer).
    LLen { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8> },
    /// LRANGE start end: 区间元素 (Members).
    LRange { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, start: i64, end: i64 },
    /// LINDEX: 单元素 (GetValue).
    LIndex { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, idx: i64 },
    /// LSET idx val: 越界 → Error (Integer 1=ok; worker 回 +OK / 错误).
    LSet { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, idx: i64, val: Vec<u8> },
    // ---- ⭐ Phase Z: ZSet (全部单 key 路由) ----
    /// ZADD: 返回新增成员数 (Integer).
    ZAdd { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, pairs: Vec<(f64, Vec<u8>)> },
    /// ZREM: 返回实删数 (Integer).
    ZRem { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, members: Vec<Vec<u8>> },
    /// ZSCORE: bulk 渲染 score / nil (Members 0/1 项, 已渲染字符串).
    ZScore { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, member: Vec<u8> },
    /// ZCARD: 成员数 (Integer).
    ZCard { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8> },
    /// ZINCRBY: 新 score (Double).
    ZIncrBy { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, delta: f64, member: Vec<u8> },
    /// ZRANGE/ZREVRANGE (按 rank). withscores 时 member/score 交替 (Members).
    ZRange { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, start: i64, end: i64, rev: bool, withscores: bool },
    /// ZRANGEBYSCORE (含端). withscores 同上 (Members).
    ZRangeByScore { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, min: f64, max: f64, withscores: bool },
    /// ZRANK/ZREVRANK: 排名 (Integer) / nil.
    ZRank { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, member: Vec<u8>, rev: bool },
    // ---- ⭐ C1: ZSet/Set/Hash 命令空洞 ----
    /// ZCOUNT key min max: 闭区间内成员数 (Integer).
    ZCount { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, min: f64, max: f64 },
    /// ZMSCORE key m...: 逐 member score (Values, 已渲染字符串/nil).
    ZMScore { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, members: Vec<Vec<u8>> },
    /// ZPOPMIN(rev=false)/ZPOPMAX(rev=true) key count: 弹出 (Members, member/score 交替).
    ZPop { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, rev: bool, count: u32 },
    /// SMISMEMBER key m...: 逐 member 0/1 (IntList).
    SMisMember { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, members: Vec<Vec<u8>> },
    /// SPOP key count: 弹出 N 成员 (Members).
    SPopN { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, count: u32 },
    /// SRANDMEMBER key count: 取 N 成员不删 (Members).
    SRandCount { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, count: u32 },
    /// HRANDFIELD key count [WITHVALUES]: 取 N field (Pairs; worker 按 withvalues 渲染).
    HRandField { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, count: u32, withvalues: bool },
    // ---- ⭐ C2: List 中段操作 ----
    /// LREM key count value: 删匹配行, 返回实删数 (Integer). val 带 tag.
    LRem { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, count: i64, val: Vec<u8> },
    /// LTRIM key start stop: 保留区间 (Integer 1 → worker 回 +OK).
    LTrim { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, start: i64, stop: i64 },
    /// LPOS key value [RANK r] [COUNT n]: count 缺省回首位 (Integer/nil), 否则 IntList.
    LPos { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, val: Vec<u8>, rank: i64, count: Option<u32> },
    /// LINSERT key BEFORE|AFTER pivot value: 新长度 / -1 pivot 不存在 / 0 key 不存在.
    LInsert { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, before: bool, pivot: Vec<u8>, val: Vec<u8> },
    // ---- ⭐ Phase B: Bitmap (String 字节 RMW) ----
    /// SETBIT key offset 0|1: shard 端 RMW (零扩展), 返回旧 bit (Integer).
    SetBit { db: std::sync::Arc<str>, table: std::sync::Arc<str>, key: Vec<u8>, offset: u64, bit: bool },
}

/// ⭐ value type tag: 与 network::value_codec 单源共享 (定义在 value_num).
/// 协议门面写入的 stored value = `[tag][payload]`. shard 端 RMW 需剥/加 tag.
pub use crate::value_num::TAG_RAW as VALUE_TAG_RAW;

/// 单个 batch 操作的结果.
///
/// (无 Eq: `Double(f64)` 浮点无全序等值; 测试用 PartialEq 断言足够.)
#[derive(Debug, Clone, PartialEq)]
pub enum BatchResult {
    PutOk,
    GetValue(Option<Vec<u8>>),
    DeleteExisted(bool),
    /// ⭐ MultiGet 结果 (与请求 keys 同序).
    Values(Vec<Option<Vec<u8>>>),
    /// ⭐ MultiPut 全部成功.
    MultiPutOk,
    /// ⭐ 整数结果 (INCR 新值 / APPEND 新长度 / SETNX 0|1).
    Integer(i64),
    /// ⭐ 浮点结果 (INCRBYFLOAT 新值; worker 渲染为 bulk string).
    Double(f64),
    /// ⭐ Phase H: HGETALL 结果 — (field, stored value) 对.
    Pairs(Vec<(Vec<u8>, Vec<u8>)>),
    /// ⭐ Phase Set: 成员列表 (裸字节, 不经 tag 渲染).
    Members(Vec<Vec<u8>>),
    /// ⭐ Phase Z: nil 可表达的可选成员 (ZSCORE None=nil, ZRANK None=nil).
    OptMember(Option<Vec<u8>>),
    /// ⭐ C1: 整数列表 (SMISMEMBER → *N 个 :0/:1).
    IntList(Vec<i64>),
    Error(String),
}

// =====================================================================
// ⭐ 独立服务架构: ShardTask / TaskResult (Phase 1)
// =====================================================================

/// 网络层 → shard 的任务单元.
/// 由 worker 解析协议后构造, push 到 shard task queue.
#[derive(Debug)]
pub struct ShardTask {
    /// 来源连接 ID (用于回复路由).
    pub conn_id: u64,
    /// 请求 ID (支持 pipeline/多路复用).
    pub req_id: u64,
    /// worker ID (用于确定 reply 回哪个 worker 的 bus).
    pub worker_id: u32,
    /// ⭐ 组号 (MGET/MSET 跨 shard 分组聚合用; 单 op 填 0).
    /// shard 原样回传, worker 据此把 Values 回填到原始索引槽.
    pub group: u32,
    /// 具体操作.
    pub op: BatchOp,
}

/// shard 执行完成后的结果, 写入 TaskReplyBus.
#[derive(Debug, Clone)]
pub struct TaskResult {
    /// 来源连接 ID.
    pub conn_id: u64,
    /// 请求 ID.
    pub req_id: u64,
    /// ⭐ 组号 (ShardTask::group 原样回传).
    pub group: u32,
    /// 执行结果.
    pub result: BatchResult,
}

/// shard 处理完发回的结果.
///
/// `ShardResponse` = `Result<ShardReply, ShardErrorKind>`.
pub type ShardResponse = Result<ShardReply, ShardErrorKind>;

/// 成功的回复: 不同操作返回不同类型, 用 enum 统一.
#[derive(Debug, Clone, PartialEq)]
pub enum ShardReply {
    /// Put 完成 (无返回值).
    PutOk,
    /// Get 返回值.
    GetValue(Option<Vec<u8>>),
    /// Delete 返回是否存在.
    DeleteExisted(bool),
    /// CreateTable 返回 table root vpid.
    CreateTableOk(u64),
    /// CreateDb 完成.
    CreateDbOk,
    /// 2PC Prepare 阶段成功 (操作已准备, 等待 Commit/Abort).
    PrepareOk,
    /// 2PC Commit 阶段成功.
    CommitOk,
    /// 2PC Abort 阶段成功.
    AbortOk,
    /// Shutdown 完成.
    ShutdownOk,
    /// Flush 完成 (所有 dirty data 已落盘).
    FlushOk,
    /// Batch 结果: 与 ops 一一对应.
    BatchResults(Vec<BatchResult>),
}

/// 错误类型. 暂时简化, 后面按 storage 错误细分.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardErrorKind {
    /// db 不存在.
    DbNotFound,
    /// table 不存在.
    TableNotFound,
    /// Storage 内部错误.
    StorageError(String),
    /// channel 关闭 (sender drop / receiver drop).
    ChannelClosed,
    /// Shard 线程 join 时 panic.
    JoinPanic,
}

impl ShardErrorKind {
    pub fn from_storage_display(err: &dyn std::fmt::Display) -> Self {
        let s = format!("{err}");
        if s.contains("DbNotFound") {
            ShardErrorKind::DbNotFound
        } else if s.contains("TableNotFound") {
            ShardErrorKind::TableNotFound
        } else {
            ShardErrorKind::StorageError(s)
        }
    }
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_error_kind_from_storage() {
        let err = std::io::Error::other("DbNotFound");
        let kind = ShardErrorKind::from_storage_display(&err);
        assert!(matches!(kind, ShardErrorKind::DbNotFound));
    }
}
