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
    /// ⭐ D2 (分库): 读本 shard 的 (DbId, name) 全表 — DbDirView 初始化/刷新用.
    ListDbsWithIds { reply: ReplySender },
    /// ⭐ Q5 (SQL 索引): 设置表 schema (序列化字节) — ShardManager 顺序
    /// 广播全 shard (控制面低频, 幂等; 本轮不走 2PC). 回 PutOk.
    SetSchema {
        db: String,
        table: String,
        bytes: Vec<u8>,
        reply: ReplySender,
    },
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

/// ⭐ F67 (JOIN): 下推谓词类型定义在 storage (避分层反向), 此处 re-export.
pub use storage::sql_rows::{IndexHint, KeySetHint, PredOp, ScanPred};

/// 单个 batch 操作 (不带 reply, batch 整体回复).
///
/// ⭐ 热路径优化: db/table 用 `Arc<str>` — worker 每 op 仅引用计数,
/// 不再每请求两次 String 堆分配 (275K ops/s 下省 550K allocs/s).
#[derive(Debug, Clone)]
pub enum BatchOp {
    Put {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        val: Vec<u8>,
    },
    Get {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
    },
    Delete {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
    },
    /// ⭐ MGET (单 shard 分片): worker 已按 key 路由分好组,
    /// shard 内走 LeafGuide 区间复用批量读.
    MultiGet {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        keys: Vec<Vec<u8>>,
    },
    /// ⭐ MSET (单 shard 分片): shard 内批量写 (同 leaf 一次提交).
    MultiPut {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// ⭐ MSETNX (单 shard 分片): 本组全部 key 不存在才写, 返回是否写入
    /// (Integer 1/0). 跨 shard 非原子 (已记为 gap).
    MultiPutNx {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// ⭐ INCR/DECR/INCRBY: shard 端 RMW (单线程 shard 内天然原子).
    /// stored value 按 `[type_tag][payload]` 约定 (tag 见 VALUE_TAG_RAW).
    /// ⭐ 数值原生存储: 结果写回 TAG_I64 8B LE 二进制 (非十进制字符串).
    Incr {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        delta: i64,
    },
    /// ⭐ INCRBYFLOAT: shard 端 RMW, 结果写回 TAG_F64 8B LE 二进制.
    IncrFloat {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        delta: f64,
    },
    /// ⭐ APPEND: shard 端 RMW, 返回追加后长度.
    Append {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        suffix: Vec<u8>,
    },
    /// ⭐ SETNX: 不存在才写 (val 已带 tag). 返回是否写入.
    SetNx {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        val: Vec<u8>,
    },
    /// ⭐ GETDEL: 读旧值 + 删除, 返回旧值 (GetValue). 旧值是溢出链则释放.
    GetDel {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
    },
    /// ⭐ GETSET: 写新值 (val 已带 tag) + 返回旧值 (GetValue).
    GetSet {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        val: Vec<u8>,
    },
    /// ⭐ SETRANGE: 从 offset 覆盖写 data (零扩展), 结果存 TAG_RAW,
    /// 返回新长度 (Integer).
    SetRange {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        offset: u32,
        data: Vec<u8>,
    },
    /// ⭐ M3-2 (CBO): 估算表近似行数 (内存增量统计, 未统计=0). 返回 RowCount.
    EstimateRowCount {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
    },
    /// ⭐ M3-4 (CBO): 索引列近似 distinct 基数 (worker 已算好 iid 列表; 免 shard 查 schema).
    /// 返回 DistinctCounts (与 iids 同序).
    EstimateDistinct {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        iids: Vec<u32>,
    },
    /// ⭐ M3-5 (CBO): 索引列 (min, max) 有序字节 (范围选择度; 未统计 = (None, None)).
    /// 返回 RangeBounds (与 iids 同序).
    EstimateRanges {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        iids: Vec<u32>,
    },
    // ---- ⭐ Phase H: Hash (全部单 key 路由, 一个 hash 的所有 field 同 shard) ----
    /// HSET 多 field: 返回新增 field 数 (Integer). value 带 tag.
    HSet {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// HSETNX: field 不存在才写, 返回 1/0 (Integer).
    HSetNx {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        field: Vec<u8>,
        val: Vec<u8>,
    },
    /// HGET: 返回 field 值 (GetValue).
    HGet {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        field: Vec<u8>,
    },
    /// HMGET: 多 field 读, 按输入序 (Values).
    HMGet {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        fields: Vec<Vec<u8>>,
    },
    /// HDEL: 删多 field, 返回实删数 (Integer).
    HDel {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        fields: Vec<Vec<u8>>,
    },
    /// HLEN: field 数 (Integer).
    HLen {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
    },
    /// HGETALL: 全部 (field, value) 对 (Pairs); HKEYS/HVALS/HSCAN 复用.
    HGetAll {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
    },
    /// HINCRBY: field 整数 RMW (结果 TAG_I64), 返回新值 (Integer).
    HIncrBy {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        field: Vec<u8>,
        delta: i64,
    },
    /// HINCRBYFLOAT: field 浮点 RMW (结果 TAG_F64), 返回新值 (Double).
    HIncrByFloat {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        field: Vec<u8>,
        delta: f64,
    },
    // ---- ⭐ Phase Set: Set (单 key 路由; 代数类跨 key 由 worker 分组聚合) ----
    /// SADD: 返回新增成员数 (Integer).
    SAdd {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    /// SREM: 返回实删数 (Integer).
    SRem {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    /// SISMEMBER: 返回 1/0 (Integer).
    SIsMember {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        member: Vec<u8>,
    },
    /// SCARD: 成员数 (Integer).
    SCard {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
    },
    /// SMEMBERS/SSCAN/代数类取成员: 返回全部成员 (Members).
    SMembers {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
    },
    /// SPOP: 随机弹出一个成员 (Members 0/1 项).
    SPop {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
    },
    /// SRANDMEMBER: 随机返回一个成员不删 (Members 0/1 项).
    SRandMember {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
    },
    // ---- ⭐ Phase L: List (全部单 key 路由) ----
    /// LPUSH(left=true)/RPUSH: 返回新长度 (Integer). val 带 tag.
    LPush {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
        left: bool,
    },
    /// LPOP(left=true)/RPOP: 弹出 count 个 (Members; count=1 时 worker 回单 bulk).
    LPop {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        left: bool,
        count: u32,
    },
    /// LLEN: 长度 (Integer).
    LLen {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
    },
    /// LRANGE start end: 区间元素 (Members).
    LRange {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        start: i64,
        end: i64,
    },
    /// LINDEX: 单元素 (GetValue).
    LIndex {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        idx: i64,
    },
    /// LSET idx val: 越界 → Error (Integer 1=ok; worker 回 +OK / 错误).
    LSet {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        idx: i64,
        val: Vec<u8>,
    },
    // ---- ⭐ Phase Z: ZSet (全部单 key 路由) ----
    /// ZADD: 返回新增成员数 (Integer).
    ZAdd {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        pairs: Vec<(f64, Vec<u8>)>,
    },
    /// ZREM: 返回实删数 (Integer).
    ZRem {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    /// ZSCORE: bulk 渲染 score / nil (Members 0/1 项, 已渲染字符串).
    ZScore {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        member: Vec<u8>,
    },
    /// ZCARD: 成员数 (Integer).
    ZCard {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
    },
    /// ZINCRBY: 新 score (Double).
    ZIncrBy {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        delta: f64,
        member: Vec<u8>,
    },
    /// ZRANGE/ZREVRANGE (按 rank). withscores 时 member/score 交替 (Members).
    ZRange {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        start: i64,
        end: i64,
        rev: bool,
        withscores: bool,
    },
    /// ZRANGEBYSCORE (含端). withscores 同上 (Members).
    ZRangeByScore {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        min: f64,
        max: f64,
        withscores: bool,
    },
    /// ZRANK/ZREVRANK: 排名 (Integer) / nil.
    ZRank {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        member: Vec<u8>,
        rev: bool,
    },
    // ---- ⭐ C1: ZSet/Set/Hash 命令空洞 ----
    /// ZCOUNT key min max: 闭区间内成员数 (Integer).
    ZCount {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        min: f64,
        max: f64,
    },
    /// ZMSCORE key m...: 逐 member score (Values, 已渲染字符串/nil).
    ZMScore {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    /// ZPOPMIN(rev=false)/ZPOPMAX(rev=true) key count: 弹出 (Members, member/score 交替).
    ZPop {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        rev: bool,
        count: u32,
    },
    /// SMISMEMBER key m...: 逐 member 0/1 (IntList).
    SMisMember {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    /// SPOP key count: 弹出 N 成员 (Members).
    SPopN {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        count: u32,
    },
    /// SRANDMEMBER key count: 取 N 成员不删 (Members).
    SRandCount {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        count: u32,
    },
    /// HRANDFIELD key count [WITHVALUES]: 取 N field (Pairs; worker 按 withvalues 渲染).
    HRandField {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        count: u32,
        withvalues: bool,
    },
    // ---- ⭐ C2: List 中段操作 ----
    /// LREM key count value: 删匹配行, 返回实删数 (Integer). val 带 tag.
    LRem {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        count: i64,
        val: Vec<u8>,
    },
    /// LTRIM key start stop: 保留区间 (Integer 1 → worker 回 +OK).
    LTrim {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        start: i64,
        stop: i64,
    },
    /// LPOS key value [RANK r] [COUNT n]: count 缺省回首位 (Integer/nil), 否则 IntList.
    LPos {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        val: Vec<u8>,
        rank: i64,
        count: Option<u32>,
    },
    /// LINSERT key BEFORE|AFTER pivot value: 新长度 / -1 pivot 不存在 / 0 key 不存在.
    LInsert {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        before: bool,
        pivot: Vec<u8>,
        val: Vec<u8>,
    },
    // ---- ⭐ Phase B: Bitmap (String 字节 RMW) ----
    /// SETBIT key offset 0|1: shard 端 RMW (零扩展), 返回旧 bit (Integer).
    SetBit {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        key: Vec<u8>,
        offset: u64,
        bit: bool,
    },
    // ---- ⭐ Q5: SQL row 表 (本地二级索引; 见 storage::sql_rows) ----
    /// 插入/覆盖一行: 按 PK 路由, shard 端引擎内部维护索引行 (同 shard 原子).
    RowPut {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        pk: Vec<u8>,
        values: Vec<storage::row::ColValue>,
    },
    /// 主键点查: 回 GetValue (TAG_ROW 字节).
    RowGet {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        pk: Vec<u8>,
    },
    /// 删一行 (含全部索引行): 回 DeleteExisted.
    RowDelete {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        pk: Vec<u8>,
    },
    /// ⭐ S1: 部分列更新 — shard 端读-改-写 (单 shard 原子, UNIQUE/索引跟随).
    /// sets = (列号, 值或表达式); 回 DeleteExisted (true = 行存在且已更新).
    RowUpdate {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        pk: Vec<u8>,
        sets: Vec<(u16, storage::row::SetVal)>,
    },
    /// RESP SQL adapter: 原子将可空字段设为 NULL，回 Integer (实际清空字段数).
    RowUnset {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        pk: Vec<u8>,
        cols: Vec<u16>,
    },
    /// RESP SQL adapter: 仅当列为 NULL 时写入，回 Integer (1=写入，0=已有字段).
    RowSetNx {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        pk: Vec<u8>,
        col: u16,
        val: storage::row::ColValue,
    },
    /// RESP SQL adapter: 原子 patch；缺行时以完整默认值行 UPSERT，回新增字段数.
    RowPatchUpsert {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        pk: Vec<u8>,
        sets: Vec<(u16, storage::row::SetVal)>,
        insert_values: Vec<storage::row::ColValue>,
    },
    /// SQL 行内原子数值更新，回 Integer/Double 新值。
    RowIncr {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        pk: Vec<u8>,
        col: u16,
        delta: storage::sql_rows::RowIncrDelta,
    },
    /// ⭐ S1: 广播 op — 数据面删表 (物理数据 + schema/bloom 派生状态). 回 PutOk.
    DropTableOp {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
    },
    /// ⭐ S2: 广播 op — 全表扫 (`[S]` 前缀收 TAG_ROW 行, 跳过纯 KV 行).
    /// 回 Rows ((空 val, pk, row)); limit 每 shard 本地生效 (0 = 不限).
    TableScan {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        limit: u32,
    },
    /// ⭐ F67 (JOIN): 广播 op — 带谓词+投影下推的全表扫. shard 本地
    /// decode 行 → preds AND 过滤 (NULL 恒 false) → 按 proj 取列 → 回 ProjRows.
    /// 与 TableScan 同路广播; proj 空 = 不取列 (仅计数形态, 本版不用).
    ScanFiltered {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        preds: Vec<ScanPred>,
        proj: Vec<u16>,
        /// ⭐ F68: 索引驱动提示 (Some 时先走索引范围扫缩候选).
        index_hint: Option<storage::sql_rows::IndexHint>,
        /// ⭐ F70 (JOIN): 键集合点查提示 (Some 时只回 join 键 ∈ keys 的行; 优先于 index_hint).
        key_set_hint: Option<storage::sql_rows::KeySetHint>,
        limit: u32,
    },
    /// ⭐ 修复 (2026-08): DML phase1 范围扫 — 与 ScanFiltered 同走索引/主键范围,
    /// 但返回完整 `Rows` (索引原值, pk, row_bytes) 供 collect_dml_pks 提取 pk 执行 phase2.
    ScanFilteredRows {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        index_hint: Option<storage::sql_rows::IndexHint>,
        limit: u32,
    },
    /// ⭐ 事务 v1 (F61): COMMIT 原子批 — worker 把 conn 层 write_set 按 shard
    /// 分组后每 shard 一个; shard 单线程保证批内无并发穿插 (先验后写 +
    /// wal_barrier 后回复). 组内 op 同 shard (worker 路由保证).
    /// ⭐ v2 (F62): read_set = OCC backward validation 输入 (SERIALIZABLE 档
    /// 事务内读过的行指纹, 预检阶段重读比对, 变了回 40001).
    TxnApply {
        ops: Vec<BatchOp>,
        read_set: Vec<ReadCheck>,
    },
    /// ⭐ F65: 全局 UNIQUE 占坑 — 路由键 = enc_val (落 email-shard).
    /// 回 ReserveOk / ReserveConflict{state, holder_txn, holder_pk}.
    ReserveUnique {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        iid: u32,
        enc_val: Vec<u8>,
        pk: Vec<u8>,
        txn_id: u64,
    },
    /// ⭐ F65: 强制抢占 (worker 回查确认 stale 后).
    StealUnique {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        iid: u32,
        enc_val: Vec<u8>,
        pk: Vec<u8>,
        txn_id: u64,
    },
    /// ⭐ F65: PENDING→COMMITTED (写行成功后).
    ConfirmUnique {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        iid: u32,
        enc_val: Vec<u8>,
        pk: Vec<u8>,
        txn_id: u64,
    },
    /// ⭐ F65: 删坑 (abort / DELETE 清坑; txn_id=0 无条件删).
    ReleaseUnique {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        iid: u32,
        enc_val: Vec<u8>,
        txn_id: u64,
    },
    /// ⭐ F66: catalog 快照 — 列一个 db 的全部表名 + schema 字节 (任意单 shard,
    /// schema 每 shard 全副本). 回 Catalog(Vec<(table, schema_bytes)>).
    CatalogDump { db: std::sync::Arc<str> },
    /// ⭐ 广播 op: caller 对**每个 shard** 各发一份 (不走 hash 路由),
    /// shard 内闭环 "本地索引扫 → 本地回表", 回 Rows; 界为闭区间,
    /// limit 每 shard 本地生效 (0 = 不限), 聚合方归并后截断.
    IndexScan {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        iid: u32,
        lo: Option<storage::row::ColValue>,
        hi: Option<storage::row::ColValue>,
        limit: u32,
        with_rows: bool,
    },
    /// ⭐ X2 (SQL 落地): 数据面 schema 分发 — worker 逐 shard 广播
    /// (shard 端 ensure_table + set_schema, 幂等; worker 不持控制面). 回 PutOk.
    SetSchemaOp {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
        bytes: Vec<u8>,
    },
    /// ⭐ X2: 读表 schema 字节 (worker 缓存 miss 时定向 shard 0). 回 GetValue.
    GetSchemaOp {
        db: std::sync::Arc<str>,
        table: std::sync::Arc<str>,
    },
}

impl BatchOp {
    /// ⭐ T1: (db, table, 路由 key) 单源提取 — 路由 hash 与 shard 端惰性建表共用.
    /// Multi op 按第一个 key (worker 已预分组, 批内同 shard 同表).
    pub fn locator(&self) -> (&str, &str, &[u8]) {
        use BatchOp::*;
        match self {
            Put { db, table, key, .. }
            | Get { db, table, key }
            | Delete { db, table, key }
            | Incr { db, table, key, .. }
            | IncrFloat { db, table, key, .. }
            | Append { db, table, key, .. }
            | SetNx { db, table, key, .. }
            | GetDel { db, table, key }
            | GetSet { db, table, key, .. }
            | SetRange { db, table, key, .. }
            | HSet { db, table, key, .. }
            | HSetNx { db, table, key, .. }
            | HGet { db, table, key, .. }
            | HMGet { db, table, key, .. }
            | HDel { db, table, key, .. }
            | HLen { db, table, key }
            | HGetAll { db, table, key }
            | HIncrBy { db, table, key, .. }
            | HIncrByFloat { db, table, key, .. }
            | SAdd { db, table, key, .. }
            | SRem { db, table, key, .. }
            | SIsMember { db, table, key, .. }
            | SCard { db, table, key }
            | SMembers { db, table, key }
            | SPop { db, table, key }
            | SRandMember { db, table, key }
            | LPush { db, table, key, .. }
            | LPop { db, table, key, .. }
            | LLen { db, table, key }
            | LRange { db, table, key, .. }
            | LIndex { db, table, key, .. }
            | LSet { db, table, key, .. }
            | ZAdd { db, table, key, .. }
            | ZRem { db, table, key, .. }
            | ZScore { db, table, key, .. }
            | ZCard { db, table, key }
            | ZIncrBy { db, table, key, .. }
            | ZRange { db, table, key, .. }
            | ZRangeByScore { db, table, key, .. }
            | ZRank { db, table, key, .. }
            | ZCount { db, table, key, .. }
            | ZMScore { db, table, key, .. }
            | ZPop { db, table, key, .. }
            | SMisMember { db, table, key, .. }
            | SPopN { db, table, key, .. }
            | SRandCount { db, table, key, .. }
            | HRandField { db, table, key, .. }
            | LRem { db, table, key, .. }
            | LTrim { db, table, key, .. }
            | LPos { db, table, key, .. }
            | LInsert { db, table, key, .. }
            | SetBit { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
            // ⭐ Q5: row op 以 PK 为路由 key (索引行与 row co-location 的根基);
            // IndexScan 是广播 op 不走 locator 路由, 兜底返回空 key.
            RowPut { db, table, pk, .. }
            | RowGet { db, table, pk }
            | RowDelete { db, table, pk }
            | RowUpdate { db, table, pk, .. }
            | RowUnset { db, table, pk, .. }
            | RowSetNx { db, table, pk, .. }
            | RowPatchUpsert { db, table, pk, .. }
            | RowIncr { db, table, pk, .. } => (db.as_ref(), table.as_ref(), pk.as_slice()),
            IndexScan { db, table, .. }
            | DropTableOp { db, table }
            | TableScan { db, table, .. } => (db.as_ref(), table.as_ref(), &[]),
            // ⭐ F67 (JOIN): 广播 op, 不走 locator 路由 (兵底空 key)
            ScanFiltered { db, table, .. } => (db.as_ref(), table.as_ref(), &[]),
            // ⭐ DML phase1 范围扫 (2026-08): 广播 op, 不走 locator 路由
            ScanFilteredRows { db, table, .. } => (db.as_ref(), table.as_ref(), &[]),
            // ⭐ M3-2: 行数估计广播 op, 不走路由 (空 key)
            EstimateRowCount { db, table } => (db.as_ref(), table.as_ref(), &[]),
            // ⭐ M3-4: distinct 估计广播 op, 不走路由
            EstimateDistinct { db, table, .. } => (db.as_ref(), table.as_ref(), &[]),
            // ⭐ M3-5: min/max 估计广播 op, 不走路由
            EstimateRanges { db, table, .. } => (db.as_ref(), table.as_ref(), &[]),
            // ⭐ 事务批: 取第一个 op 的 locator (组内同 shard, 仅兼容用;
            // ensure_table 在 shard 端逐 op 处理)
            TxnApply { ops, .. } => ops.first().map(|o| o.locator()).unwrap_or(("", "", &[])),
            // ⭐ F65: 占坑 op 按 enc_val 路由到 email-shard (与 pk 路由独立)
            ReserveUnique {
                db, table, enc_val, ..
            }
            | StealUnique {
                db, table, enc_val, ..
            }
            | ConfirmUnique {
                db, table, enc_val, ..
            }
            | ReleaseUnique {
                db, table, enc_val, ..
            } => (db.as_ref(), table.as_ref(), enc_val.as_slice()),
            // ⭐ F66: catalog dump 任意单 shard (空 key 路由)
            CatalogDump { db } => (db.as_ref(), "", &[]),
            SetSchemaOp { db, table, .. } | GetSchemaOp { db, table } => {
                (db.as_ref(), table.as_ref(), &[])
            }
            MultiGet { db, table, keys } => (
                db.as_ref(),
                table.as_ref(),
                keys.first().map(|k| k.as_slice()).unwrap_or(&[]),
            ),
            MultiPut { db, table, pairs } | MultiPutNx { db, table, pairs } => (
                db.as_ref(),
                table.as_ref(),
                pairs.first().map(|p| p.0.as_slice()).unwrap_or(&[]),
            ),
        }
    }

    /// ⭐ T2 (分表): 单 key op 的 (table, key) 可变访问 — worker 冒号前缀
    /// 选表在 push 前就地重写. Multi op 返回 None (dispatch 已按 key 预分组).
    pub fn table_key_mut(&mut self) -> Option<(&mut std::sync::Arc<str>, &mut Vec<u8>)> {
        use BatchOp::*;
        match self {
            Put { table, key, .. }
            | Get { table, key, .. }
            | Delete { table, key, .. }
            | Incr { table, key, .. }
            | IncrFloat { table, key, .. }
            | Append { table, key, .. }
            | SetNx { table, key, .. }
            | GetDel { table, key, .. }
            | GetSet { table, key, .. }
            | SetRange { table, key, .. }
            | HSet { table, key, .. }
            | HSetNx { table, key, .. }
            | HGet { table, key, .. }
            | HMGet { table, key, .. }
            | HDel { table, key, .. }
            | HLen { table, key, .. }
            | HGetAll { table, key, .. }
            | HIncrBy { table, key, .. }
            | HIncrByFloat { table, key, .. }
            | SAdd { table, key, .. }
            | SRem { table, key, .. }
            | SIsMember { table, key, .. }
            | SCard { table, key, .. }
            | SMembers { table, key, .. }
            | SPop { table, key, .. }
            | SRandMember { table, key, .. }
            | LPush { table, key, .. }
            | LPop { table, key, .. }
            | LLen { table, key, .. }
            | LRange { table, key, .. }
            | LIndex { table, key, .. }
            | LSet { table, key, .. }
            | ZAdd { table, key, .. }
            | ZRem { table, key, .. }
            | ZScore { table, key, .. }
            | ZCard { table, key, .. }
            | ZIncrBy { table, key, .. }
            | ZRange { table, key, .. }
            | ZRangeByScore { table, key, .. }
            | ZRank { table, key, .. }
            | ZCount { table, key, .. }
            | ZMScore { table, key, .. }
            | ZPop { table, key, .. }
            | SMisMember { table, key, .. }
            | SPopN { table, key, .. }
            | SRandCount { table, key, .. }
            | HRandField { table, key, .. }
            | LRem { table, key, .. }
            | LTrim { table, key, .. }
            | LPos { table, key, .. }
            | LInsert { table, key, .. }
            | SetBit { table, key, .. } => Some((table, key)),
            MultiGet { .. } | MultiPut { .. } | MultiPutNx { .. } => None,
            // ⭐ Q5: SQL row op 的 pk 是二进制主键, 不参与 RESP 冒号选表
            RowPut { .. }
            | RowGet { .. }
            | RowDelete { .. }
            | RowUpdate { .. }
            | RowUnset { .. }
            | RowSetNx { .. }
            | RowPatchUpsert { .. }
            | RowIncr { .. }
            | IndexScan { .. }
            | DropTableOp { .. }
            | TableScan { .. } => None,
            ScanFiltered { .. } => None,
            ScanFilteredRows { .. } => None,
            // ⭐ M3-2: 行数估计无 key (不参与 RESP 冒号选表)
            EstimateRowCount { .. } => None,
            // ⭐ M3-4: distinct 估计无 key
            EstimateDistinct { .. } => None,
            // ⭐ M3-5: min/max 估计无 key
            EstimateRanges { .. } => None,
            // ⭐ X2: schema op 无 key
            SetSchemaOp { .. } | GetSchemaOp { .. } => None,
            TxnApply { .. } => None,
            ReserveUnique { .. }
            | StealUnique { .. }
            | ConfirmUnique { .. }
            | ReleaseUnique { .. } => None,
            CatalogDump { .. } => None,
        }
    }
}

/// ⭐ value type tag: 与 network::value_codec 单源共享 (定义在 value_num).
/// 协议门面写入的 stored value = `[tag][payload]`. shard 端 RMW 需剥/加 tag.
pub use crate::value_num::TAG_RAW as VALUE_TAG_RAW;

/// ⭐ v2 (F62): OCC 读集验证项 — SERIALIZABLE 事务内读过的 (db, table, pk)
/// 及当时行字节的 crc32 指纹 (None = 读时不存在). commit 时 shard 端重读
/// 比对 — 变了即 serialization failure (40001/1213), 整批拒.
#[derive(Debug, Clone)]
pub struct ReadCheck {
    pub db: String,
    pub table: String,
    pub pk: Vec<u8>,
    pub fp: Option<u32>,
}

/// ⭐ M3-5 (CBO): 索引列 (min, max) 字节边界 (None = 该列无值, 不参与直方图).
pub type RangeBound = (Option<Vec<u8>>, Option<Vec<u8>>);

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
    /// ⭐ Q5: 索引扫描结果 `(索引原值, pk, row_bytes)` — 单 shard 内按
    /// (val, pk) 升序; 跨 shard 归并直接按同键排序即全局序.
    /// `with_rows = false` 时 row_bytes 为空 (覆盖索引).
    Rows(Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>),
    /// ⭐ 事务 v1 (F61): TxnApply 完成 (应用的 op 数).
    TxnApplied(u64),
    /// ⭐ F65: 占坑成功 (写入 PENDING 或幂等重入).
    ReserveOk,
    /// ⭐ F65: 占坑冲突 — 现有坑 (state 1=PENDING/2=COMMITTED, 持有者 txn/pk).
    ReserveConflict {
        state: u8,
        holder_txn: u64,
        holder_pk: Vec<u8>,
    },
    /// ⭐ F66: catalog 快照 — (table_name, schema_bytes) 列表.
    Catalog(Vec<(String, Vec<u8>)>),
    /// ⭐ F67 (JOIN): ScanFiltered 结果 — 只含投影列值的行 (省带宽, worker 免 decode).
    ProjRows(Vec<Vec<storage::row::ColValue>>),
    /// ⭐ M3-2 (CBO): 表近似行数估计 (EstimateRowCount 响应).
    RowCount(u64),
    /// ⭐ M3-4 (CBO): 索引列 distinct 计数 (EstimateDistinct 响应, 与 cols 同序).
    DistinctCounts(Vec<u64>),
    /// ⭐ M3-5 (CBO): 索引列 (min, max) 有序字节 (EstimateRanges 响应, 与 iids 同序).
    RangeBounds(Vec<RangeBound>),
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
    /// ⭐ D2 (分库): (DbId, name) 全表.
    DbList(Vec<(u32, String)>),
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
