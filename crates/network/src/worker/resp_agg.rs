//! ⭐ RESP 协议跨 shard 聚合状态结构体 (拆自 mod.rs 2026-08, 大文件解耦).
//! 均为纯数据定义, 由 worker 的 dispatch_resp_command / handle_resp 使用.

use crate::protocol::resp::SetAlgOp;

/// DEL 多 key 的聚合状态 (RESP :N 回复需等全部 Delete 完成).
pub struct DelAgg {
    pub remaining: usize,
    pub count: i64,
}

/// ⭐ MGET 跨 shard 聚合: 每 shard 一组, Values 按组内索引表回填原始槽.
pub struct MGetAgg {
    pub remaining: usize,
    /// 原始请求顺序的结果槽 (None = miss 或未回).
    pub slots: Vec<Option<Vec<u8>>>,
    /// group 号 → 该组 keys 的原始索引 (与 MultiGet keys 同序).
    pub groups: Vec<Vec<usize>>,
    /// 任一组失败: 记首个错误 (仍等全部组回齐再回复).
    pub error: Option<String>,
}

/// ⭐ MSET 跨 shard 聚合: 全部组 MultiPutOk → +OK.
pub struct MSetAgg {
    pub remaining: usize,
    pub error: Option<String>,
}

/// ⭐ EXISTS 多 key 聚合 (DEL 同构: 计数存在数).
pub struct ExistsAgg {
    pub remaining: usize,
    pub count: i64,
}

/// ⭐ MSETNX 跨 shard 聚合: 全部分片 MultiPutNx 返回 1 → :1, 否则 :0.
/// (跨 shard 非原子: 部分分片可能已写 — 已记为 gap.)
pub struct MSetNxAgg {
    pub remaining: usize,
    pub all_set: bool,
}

/// ⭐ 单 op Get 的回复语义转换 (STRLEN/TYPE/HEXISTS 复用 Get/HGet 任务).
#[derive(Clone, Copy)]
pub enum GetKind {
    Strlen,
    TypeOf,
    /// ⭐ Phase H: HEXISTS — GetValue(Some)→:1, None→:0
    HExists,
}

/// ⭐ Phase H: Pairs 结果渲染形态 (HGETALL/HKEYS/HVALS/HSCAN 复用同一 op).
#[derive(Clone, Copy)]
pub enum PairsKind {
    All,
    Keys,
    Vals,
    Scan,
    /// ⭐ C1: HRANDFIELD 无 count — 首 field 单 bulk / nil.
    OneKey,
}

/// ⭐ Phase Set: Members 结果渲染形态.
#[derive(Clone, Copy)]
pub enum MembersKind {
    /// SMEMBERS → *N
    List,
    /// SSCAN → ["0", *N]
    Scan,
    /// SPOP/SRANDMEMBER → bulk / nil (0/1 项)
    One,
}

/// ⭐ Phase Set: SINTER/SUNION/SDIFF 跨 shard 聚合 — 每 key 一个 SMembers
/// (group = key 序号), 全部回齐后 worker 端求交/并/差 (首 key 为基).
pub struct SetAlgAgg {
    pub remaining: usize,
    pub op: SetAlgOp,
    pub sets: Vec<Option<Vec<Vec<u8>>>>,
    pub error: Option<String>,
    /// ⭐ C1: SINTERCARD — 只回交集势 (Integer) 而非成员数组.
    pub card_only: bool,
    /// ⭐ C1: SINTERCARD LIMIT (0 = 无限制).
    pub limit: usize,
    /// ⭐ C3: *STORE — 结果写入 dst (先 DEL 再 SAdd), 回 :card.
    pub store_dst: Option<Vec<u8>>,
    /// ⭐ D3 (分库): 命令发起时的 (db, table) — 二阶段任务用, 防 pipeline 中
    /// SELECT 切库后错库.
    pub db: std::sync::Arc<str>,
    pub table: std::sync::Arc<str>,
}

/// ⭐ C3: *STORE 第二阶段 (Delete dst + SAdd/ZAdd dst) 完成聚合.
/// 跨 shard 非原子 (源读与目标写分离) — 与 SINTER/MSETNX 同级 gap.
pub struct StoreFinishAgg {
    pub remaining: usize,
    pub card: i64,
    pub error: Option<String>,
}

/// ⭐ C3: ZINTERSTORE/ZUNIONSTORE 源聚合 — 每源 key 一个 ZRange(withscores),
/// 回齐后 SUM 聚合写 dst (无 weights/AGGREGATE, 计划内 defer).
pub type ScoredMembers = Vec<(Vec<u8>, f64)>;
pub struct ZStoreAgg {
    pub remaining: usize,
    pub inter: bool,
    pub sets: Vec<Option<ScoredMembers>>,
    pub error: Option<String>,
    pub dst: Vec<u8>,
    /// ⭐ D3 (分库): 命令发起时的 (db, table) — 二阶段任务用.
    pub db: std::sync::Arc<str>,
    pub table: std::sync::Arc<str>,
}

/// ⭐ Phase G: Geo 命令的渲染上下文 (复用 ZMScore/ZRange 结果 + geohash 解码).
pub enum GeoCtx {
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
pub enum BitCtx {
    /// GETBIT offset → :0|:1
    GetBit { offset: u64 },
    /// BITCOUNT [start end] (BYTE, 含负索引) → :popcount
    Count { start: i64, end: i64 },
    /// BITPOS bit [start [end]] → :pos / :-1
    Pos { bit: bool, start: i64, end: Option<i64> },
}
