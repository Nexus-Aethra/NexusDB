//! RESP2 协议门面 (Redis 兼容).
//!
//! 解析 `*N\r\n$len\r\narg\r\n...` (array of bulk strings) 增量帧,
//! 输出 `RespCommand`; 编码器输出 RESP2 回复 (`+OK` / `$n` / `:n` / `-ERR`).
//!
//! **范围**: RESP2 only. `HELLO 3` 回 `-NOPROTO` 让客户端回退 RESP2.
//! **认证**: AUTH 命令在 worker 本地处理 (per-conn 状态), 不进 shard.

use super::DecodeOutcome;

/// RESP 命令 (worker 侧分发单元).
#[derive(Debug, Clone, PartialEq)] // 无 Eq: IncrFloat 含 f64
pub enum RespCommand {
    /// SET key value → BatchOp::Put
    Set { key: Vec<u8>, value: Vec<u8> },
    /// GET key → BatchOp::Get
    Get { key: Vec<u8> },
    /// DEL key [key ...] → 多个 BatchOp::Delete, 回复 :N
    Del { keys: Vec<Vec<u8>> },
    /// ⭐ MGET key [key ...] → 按 shard 分组 MultiGet, 聚合回 *N 数组
    MGet { keys: Vec<Vec<u8>> },
    /// ⭐ MSET key value [key value ...] → 按 shard 分组 MultiPut, 回 +OK
    /// (value 已预置 type tag, 与 Set 一致)
    MSet { pairs: Vec<(Vec<u8>, Vec<u8>)> },
    /// ⭐ INCR/DECR/INCRBY/DECRBY → BatchOp::Incr (shard 端 RMW), 回 :n
    Incr { key: Vec<u8>, delta: i64 },
    /// ⭐ INCRBYFLOAT → BatchOp::IncrFloat, 回 bulk string (Redis 语义)
    IncrFloat { key: Vec<u8>, delta: f64 },
    /// ⭐ APPEND key value → BatchOp::Append (suffix 不带 tag), 回 :len
    Append { key: Vec<u8>, suffix: Vec<u8> },
    /// ⭐ SETNX key value → BatchOp::SetNx (value 带 tag), 回 :0|:1
    SetNx { key: Vec<u8>, value: Vec<u8> },
    /// ⭐ EXISTS key [key ...] → N 个 Get 聚合计数, 回 :n
    Exists { keys: Vec<Vec<u8>> },
    /// ⭐ STRLEN key → Get 转长度, 回 :len (miss → :0)
    Strlen { key: Vec<u8> },
    /// ⭐ TYPE key → Get 转类型, 回 +string / +none
    TypeOf { key: Vec<u8> },
    /// ⭐ GETRANGE key start end → Get 后切片 (支持负索引), 回 bulk
    GetRange { key: Vec<u8>, start: i64, end: i64 },
    /// ⭐ SETRANGE key offset value → BatchOp::SetRange, 回 :len
    SetRange { key: Vec<u8>, offset: u32, data: Vec<u8> },
    /// ⭐ GETDEL key → BatchOp::GetDel, 回旧值 bulk / nil
    GetDel { key: Vec<u8> },
    /// ⭐ GETSET key value → BatchOp::GetSet (value 带 tag), 回旧值 bulk / nil
    GetSet { key: Vec<u8>, value: Vec<u8> },
    /// ⭐ MSETNX key value [key value ...] → 全不存在才批量写, 回 :0|:1
    MSetNx { pairs: Vec<(Vec<u8>, Vec<u8>)> },
    // ---- ⭐ Phase H: Hash ----
    /// HSET/HMSET key f v [f v ...] → BatchOp::HSet; HSET 回 :新增数, HMSET 回 +OK
    HSet { key: Vec<u8>, pairs: Vec<(Vec<u8>, Vec<u8>)>, reply_ok: bool },
    /// HSETNX key field value → 回 :0|:1
    HSetNx { key: Vec<u8>, field: Vec<u8>, value: Vec<u8> },
    /// HGET key field → 回 bulk / nil
    HGet { key: Vec<u8>, field: Vec<u8> },
    /// HMGET key f [f ...] → 回 *N 数组
    HMGet { key: Vec<u8>, fields: Vec<Vec<u8>> },
    /// HDEL key f [f ...] → 回 :实删数
    HDel { key: Vec<u8>, fields: Vec<Vec<u8>> },
    /// HEXISTS key field → 回 :0|:1 (HGet 语义转换)
    HExists { key: Vec<u8>, field: Vec<u8> },
    /// HLEN key → 回 :n
    HLen { key: Vec<u8> },
    /// HGETALL key → 回 *2N (field,value 交替)
    HGetAll { key: Vec<u8> },
    /// HKEYS key → 回 *N fields
    HKeys { key: Vec<u8> },
    /// HVALS key → 回 *N values
    HVals { key: Vec<u8> },
    /// HSCAN key cursor […] → v1 单次全量, 回 ["0", *2N]
    HScan { key: Vec<u8> },
    /// HINCRBY key field n → 回 :新值
    HIncrBy { key: Vec<u8>, field: Vec<u8>, delta: i64 },
    /// HINCRBYFLOAT key field f → 回 bulk 新值
    HIncrByFloat { key: Vec<u8>, field: Vec<u8>, delta: f64 },
    // ---- ⭐ Phase Set: Set ----
    /// SADD key m [m ...] → 回 :新增数
    SAdd { key: Vec<u8>, members: Vec<Vec<u8>> },
    /// SREM key m [m ...] → 回 :实删数
    SRem { key: Vec<u8>, members: Vec<Vec<u8>> },
    /// SISMEMBER key m → 回 :0|:1
    SIsMember { key: Vec<u8>, member: Vec<u8> },
    /// SCARD key → 回 :n
    SCard { key: Vec<u8> },
    /// SMEMBERS key → 回 *N
    SMembers { key: Vec<u8> },
    /// SSCAN key cursor […] → v1 单次全量, 回 ["0", *N]
    SScan { key: Vec<u8> },
    /// SPOP key [count] → count 缺省回单 bulk/nil, 否则回 *N
    SPop { key: Vec<u8>, count: Option<u32> },
    /// SRANDMEMBER key [count] → count 缺省回单 bulk/nil, 否则回 *N
    SRandMember { key: Vec<u8>, count: Option<u32> },
    /// SMISMEMBER key m... → 回 *N 个 :0/:1
    SMisMember { key: Vec<u8>, members: Vec<Vec<u8>> },
    /// SINTERCARD numkeys key... [LIMIT n] → 回 :交集势 (worker 聚合)
    SInterCard { keys: Vec<Vec<u8>>, limit: usize },
    /// SINTER/SUNION/SDIFF key [key ...] → 跨 shard 取成员 + worker 端代数
    SetAlg { op: SetAlgOp, keys: Vec<Vec<u8>> },
    /// ⭐ C3: SINTERSTORE/SUNIONSTORE/SDIFFSTORE dst key... → :card (非原子)
    SetAlgStore { op: SetAlgOp, dst: Vec<u8>, keys: Vec<Vec<u8>> },
    /// ⭐ C3: ZINTERSTORE/ZUNIONSTORE dst numkeys key... (无 weights, SUM) → :card
    ZSetStore { inter: bool, dst: Vec<u8>, keys: Vec<Vec<u8>> },
    // ---- ⭐ Phase L: List ----
    /// LPUSH/RPUSH key v [v ...] → 回 :新长度
    LPush { key: Vec<u8>, values: Vec<Vec<u8>>, left: bool },
    /// LPOP/RPOP key [count] → count 缺省回单 bulk/nil, 否则回 *N
    LPop { key: Vec<u8>, left: bool, count: Option<u32> },
    /// LLEN key → :n
    LLen { key: Vec<u8> },
    /// LRANGE key start end → *N
    LRange { key: Vec<u8>, start: i64, end: i64 },
    /// LINDEX key idx → bulk / nil
    LIndex { key: Vec<u8>, idx: i64 },
    /// LSET key idx val → +OK / -ERR index out of range
    LSet { key: Vec<u8>, idx: i64, value: Vec<u8> },
    // ---- ⭐ C2: List 中段操作 ----
    /// LREM key count element → :实删数
    LRem { key: Vec<u8>, count: i64, value: Vec<u8> },
    /// LTRIM key start stop → +OK
    LTrim { key: Vec<u8>, start: i64, stop: i64 },
    /// LPOS key element [RANK r] [COUNT n] → :idx / nil / *N
    LPos { key: Vec<u8>, value: Vec<u8>, rank: i64, count: Option<u32> },
    /// LINSERT key BEFORE|AFTER pivot element → :新长度 / :-1 / :0
    LInsert { key: Vec<u8>, before: bool, pivot: Vec<u8>, value: Vec<u8> },
    // ---- ⭐ Phase Z: ZSet ----
    /// ZADD key score member [score member ...] → :新增数
    ZAdd { key: Vec<u8>, pairs: Vec<(f64, Vec<u8>)> },
    /// ZREM key m [m ...] → :实删数
    ZRem { key: Vec<u8>, members: Vec<Vec<u8>> },
    /// ZSCORE key member → bulk / nil
    ZScore { key: Vec<u8>, member: Vec<u8> },
    /// ZCARD key → :n
    ZCard { key: Vec<u8> },
    /// ZINCRBY key delta member → bulk 新 score
    ZIncrBy { key: Vec<u8>, delta: f64, member: Vec<u8> },
    /// ZRANGE/ZREVRANGE key start end [WITHSCORES] → *N
    ZRange { key: Vec<u8>, start: i64, end: i64, rev: bool, withscores: bool },
    /// ZRANGEBYSCORE key min max [WITHSCORES] → *N
    ZRangeByScore { key: Vec<u8>, min: f64, max: f64, withscores: bool },
    /// ZRANK/ZREVRANK key member → :rank / nil
    ZRank { key: Vec<u8>, member: Vec<u8>, rev: bool },
    /// ZCOUNT key min max → :闭区间成员数
    ZCount { key: Vec<u8>, min: f64, max: f64 },
    /// ZMSCORE key m... → *N 个 bulk score / nil
    ZMScore { key: Vec<u8>, members: Vec<Vec<u8>> },
    /// ZPOPMIN(rev=false)/ZPOPMAX(rev=true) key [count] → *2N member/score
    ZPop { key: Vec<u8>, rev: bool, count: u32 },
    // ---- ⭐ C1: Hash 补齐 ----
    /// HSTRLEN key field → :len (HGet 转长度)
    HStrlen { key: Vec<u8>, field: Vec<u8> },
    /// HRANDFIELD key [count [WITHVALUES]] → 无 count 单 bulk; 有 count *N / *2N
    HRandField { key: Vec<u8>, count: Option<u32>, withvalues: bool },
    // ---- ⭐ Phase G: Geo (复用 ZSet: score = 52-bit geohash) ----
    /// GEOPOS key m... → *N 个 [lon, lat] / nil (复用 ZMScore + 渲染钩子)
    GeoPos { key: Vec<u8>, members: Vec<Vec<u8>> },
    /// GEODIST key m1 m2 [unit] → bulk 距离 / nil (复用 ZMScore)
    GeoDist { key: Vec<u8>, m1: Vec<u8>, m2: Vec<u8>, factor: f64 },
    /// GEOSEARCH key FROMLONLAT lon lat BYRADIUS r unit [...] (复用 ZRange 全扫)
    GeoSearch {
        key: Vec<u8>,
        lon: f64,
        lat: f64,
        radius_m: f64,
        asc: bool,
        count: usize,
        withcoord: bool,
        withdist: bool,
    },
    // ---- ⭐ Phase B: Bitmap (String 字节) ----
    /// SETBIT key offset 0|1 → :旧bit (shard RMW)
    SetBit { key: Vec<u8>, offset: u64, bit: bool },
    /// GETBIT key offset → :0|:1 (Get + worker 取位)
    GetBit { key: Vec<u8>, offset: u64 },
    /// BITCOUNT key [start end] (BYTE 语义) → :n
    BitCount { key: Vec<u8>, start: i64, end: i64 },
    /// BITPOS key bit [start [end]] (BYTE 语义) → :pos / :-1
    BitPos { key: Vec<u8>, bit: bool, start: i64, end: Option<i64> },
    /// PING [msg] → 本地回 +PONG / $msg
    Ping(Option<Vec<u8>>),
    /// ECHO msg → 本地回 $msg
    Echo(Vec<u8>),
    /// AUTH [user] pass → 本地认证
    Auth {
        user: Option<Vec<u8>>,
        pass: Vec<u8>,
    },
    /// QUIT → 回 +OK 后关连接
    Quit,
    /// COMMAND [...] → 本地最小回复 (空数组)
    Command,
    /// HELLO [proto] → proto=2/缺省回最小 map; proto=3 回 -NOPROTO
    Hello(Option<Vec<u8>>),
    /// SELECT idx → 本地回 +OK (单 db 语义, 忽略)
    Select,
    /// 未知命令 → -ERR unknown command
    Unknown(String),
    /// 命令参数个数错误 → -ERR wrong number of arguments
    WrongArity(String),
    /// ⭐ 整数参数非法 (INCRBY/DECRBY) → -ERR value is not an integer
    InvalidInt(String),
    /// ⭐ 浮点参数非法 (INCRBYFLOAT) → -ERR value is not a valid float
    InvalidFloat(String),
}

/// ⭐ Phase Set: 集合代数操作类型.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetAlgOp {
    Inter,
    Union,
    Diff,
}

/// ⭐ Phase Z: 解析 ZRANGEBYSCORE 的 min/max (支持 -inf/+inf/inf).
fn parse_score_bound(b: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(b).ok()?;
    match s.to_ascii_lowercase().as_str() {
        "-inf" => Some(f64::NEG_INFINITY),
        "+inf" | "inf" => Some(f64::INFINITY),
        // 开区间 "(x" v1 不支持, 当作非法
        _ => s.parse::<f64>().ok().filter(|f| !f.is_nan()),
    }
}

/// RESP2 编解码器 (无状态).
#[derive(Debug, Clone, Copy, Default)]
pub struct RespCodec;

/// 单帧最大字节数 (防恶意超大 bulk len 撑爆内存).
const MAX_RESP_FRAME: usize = 16 * 1024 * 1024;
/// 数组最大元素数.
const MAX_RESP_ARGS: usize = 1024;

impl RespCodec {
    pub fn new() -> Self {
        Self
    }

    /// 增量解析一条命令. 返回 (consumed, command) 或 NeedMore.
    ///
    /// 协议错误 (非法前缀/超限) 返回 Err(错误消息) — caller 应回 -ERR 并断开连接
    /// (RESP 流一旦错位无法重新同步).
    pub fn decode_command(
        &self,
        buf: &[u8],
    ) -> Result<DecodeOutcome<RespCommand>, String> {
        if buf.is_empty() {
            return Ok(DecodeOutcome::NeedMore);
        }
        // 只支持 array-of-bulk-strings 形式 (redis-cli / 所有正规客户端都用这个);
        // inline command 不支持.
        if buf[0] != b'*' {
            return Err(format!(
                "protocol error: expected '*', got {:?}",
                buf[0] as char
            ));
        }
        let (argc, mut pos) = match parse_int_line(buf, 1)? {
            Some(v) => v,
            None => return Ok(DecodeOutcome::NeedMore),
        };
        if argc <= 0 || argc as usize > MAX_RESP_ARGS {
            return Err(format!("protocol error: invalid multibulk length {argc}"));
        }

        let mut args: Vec<(usize, usize)> = Vec::with_capacity(argc as usize);
        for _ in 0..argc {
            if pos >= buf.len() {
                return Ok(DecodeOutcome::NeedMore);
            }
            if buf[pos] != b'$' {
                return Err(format!(
                    "protocol error: expected '$', got {:?}",
                    buf[pos] as char
                ));
            }
            let (blen, data_start) = match parse_int_line(buf, pos + 1)? {
                Some(v) => v,
                None => return Ok(DecodeOutcome::NeedMore),
            };
            if blen < 0 || blen as usize > MAX_RESP_FRAME {
                return Err(format!("protocol error: invalid bulk length {blen}"));
            }
            let blen = blen as usize;
            // data + 尾部 \r\n
            if buf.len() < data_start + blen + 2 {
                return Ok(DecodeOutcome::NeedMore);
            }
            if &buf[data_start + blen..data_start + blen + 2] != b"\r\n" {
                return Err("protocol error: bulk string missing CRLF".to_string());
            }
            // ⭐ 热路径优化: 只记 span, 由 args_to_command 按需物化
            // (SET 的 value 物化时直接预置 type tag, 免 worker 二次全值拷贝)
            args.push((data_start, blen));
            pos = data_start + blen + 2;
        }

        Ok(DecodeOutcome::Complete {
            consumed: pos,
            value: args_to_command(buf, &args),
        })
    }

    // ===== 回复编码 =====

    pub fn encode_ok(&self) -> Vec<u8> {
        b"+OK\r\n".to_vec()
    }

    pub fn encode_simple(&self, s: &str) -> Vec<u8> {
        format!("+{s}\r\n").into_bytes()
    }

    pub fn encode_error(&self, msg: &str) -> Vec<u8> {
        // 消息里不允许 CR/LF (会破坏帧边界)
        let clean: String = msg
            .chars()
            .map(|c| if c == '\r' || c == '\n' { ' ' } else { c })
            .collect();
        // 已带错误码前缀 (如 WRONGPASS/NOAUTH/NOPROTO/WRONGTYPE) 的消息不再加 ERR
        if clean.starts_with("NOAUTH")
            || clean.starts_with("WRONGPASS")
            || clean.starts_with("NOPROTO")
            || clean.starts_with("WRONGTYPE")
            || clean.starts_with("ERR")
        {
            format!("-{clean}\r\n").into_bytes()
        } else {
            format!("-ERR {clean}\r\n").into_bytes()
        }
    }

    pub fn encode_bulk(&self, data: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(data.len() + 16);
        out.extend_from_slice(format!("${}\r\n", data.len()).as_bytes());
        out.extend_from_slice(data);
        out.extend_from_slice(b"\r\n");
        out
    }

    pub fn encode_nil(&self) -> Vec<u8> {
        b"$-1\r\n".to_vec()
    }

    pub fn encode_integer(&self, n: i64) -> Vec<u8> {
        format!(":{n}\r\n").into_bytes()
    }

    /// COMMAND 的最小回复: 空数组 (足以让 redis-cli 不报错).
    pub fn encode_empty_array(&self) -> Vec<u8> {
        b"*0\r\n".to_vec()
    }
}

/// ⭐ 把字节切片解析为十进制 i64 (命令参数用, 如 GETRANGE start/end).
fn parse_i64(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse::<i64>().ok()
}

/// ⭐ Phase G: 解析有限 f64 (经纬度/半径).
fn parse_f64(bytes: &[u8]) -> Option<f64> {
    std::str::from_utf8(bytes)
        .ok()?
        .parse::<f64>()
        .ok()
        .filter(|f| f.is_finite())
}

/// 从 `buf[start..]` 解析 `<digits>\r\n`, 返回 (值, \r\n 之后的位置).
/// 数据不足返回 Ok(None); 非法数字返回 Err.
fn parse_int_line(buf: &[u8], start: usize) -> Result<Option<(i64, usize)>, String> {
    let mut i = start;
    let mut val: i64 = 0;
    let mut neg = false;
    let mut digits = 0usize;
    if i < buf.len() && buf[i] == b'-' {
        neg = true;
        i += 1;
    }
    while i < buf.len() {
        match buf[i] {
            b'0'..=b'9' => {
                val = val
                    .checked_mul(10)
                    .and_then(|v| v.checked_add((buf[i] - b'0') as i64))
                    .ok_or_else(|| "protocol error: integer overflow".to_string())?;
                digits += 1;
                i += 1;
            }
            b'\r' => {
                if digits == 0 {
                    return Err("protocol error: empty integer".to_string());
                }
                if i + 1 >= buf.len() {
                    return Ok(None); // 还差 \n
                }
                if buf[i + 1] != b'\n' {
                    return Err("protocol error: expected LF after CR".to_string());
                }
                return Ok(Some((if neg { -val } else { val }, i + 2)));
            }
            other => {
                return Err(format!(
                    "protocol error: invalid digit {:?}",
                    other as char
                ));
            }
        }
        // 防超长数字行 (正常 len 不会超过 8 位数字)
        if digits > 10 {
            return Err("protocol error: integer line too long".to_string());
        }
    }
    Ok(None)
}

/// args (span 形式) → RespCommand (命令名大小写不敏感).
///
/// ⭐ 热路径优化: 每个参数只在此处按需物化一次;
/// **SET 的 value 物化时直接预置 1B type tag** (`[TAG_RAW][payload]`),
/// worker 层零二次拷贝 (`RespCommand::Set.value` 即存储层 stored 布局).
fn args_to_command(buf: &[u8], args: &[(usize, usize)]) -> RespCommand {
    let arg = |i: usize| -> &[u8] {
        let (off, len) = args[i];
        &buf[off..off + len]
    };
    let owned = |i: usize| -> Vec<u8> { arg(i).to_vec() };
    let name = String::from_utf8_lossy(arg(0)).to_ascii_uppercase();
    let arity = args.len();
    match name.as_str() {
        "SET" => {
            if arity < 3 {
                return RespCommand::WrongArity("set".into());
            }
            // 忽略 SET 的扩展参数 (EX/PX/NX/XX...) — 后续版本支持
            let payload = arg(2);
            let mut value = Vec::with_capacity(1 + payload.len());
            value.push(crate::value_codec::TAG_RAW);
            value.extend_from_slice(payload);
            RespCommand::Set {
                key: owned(1),
                value,
            }
        }
        "GET" => {
            if arity != 2 {
                return RespCommand::WrongArity("get".into());
            }
            RespCommand::Get { key: owned(1) }
        }
        "DEL" => {
            if arity < 2 {
                return RespCommand::WrongArity("del".into());
            }
            RespCommand::Del {
                keys: (1..arity).map(owned).collect(),
            }
        }
        "MGET" => {
            if arity < 2 {
                return RespCommand::WrongArity("mget".into());
            }
            RespCommand::MGet {
                keys: (1..arity).map(owned).collect(),
            }
        }
        "MSET" => {
            if arity < 3 || !(arity - 1).is_multiple_of(2) {
                return RespCommand::WrongArity("mset".into());
            }
            let pairs = (1..arity)
                .step_by(2)
                .map(|i| {
                    let payload = arg(i + 1);
                    let mut value = Vec::with_capacity(1 + payload.len());
                    value.push(crate::value_codec::TAG_RAW);
                    value.extend_from_slice(payload);
                    (owned(i), value)
                })
                .collect();
            RespCommand::MSet { pairs }
        }
        "PING" => match arity {
            1 => RespCommand::Ping(None),
            2 => RespCommand::Ping(Some(owned(1))),
            _ => RespCommand::WrongArity("ping".into()),
        },
        "INCR" | "DECR" => {
            if arity != 2 {
                return RespCommand::WrongArity(name.to_ascii_lowercase());
            }
            RespCommand::Incr {
                key: owned(1),
                delta: if name == "INCR" { 1 } else { -1 },
            }
        }
        "INCRBY" | "DECRBY" => {
            if arity != 3 {
                return RespCommand::WrongArity(name.to_ascii_lowercase());
            }
            let Some(n) = std::str::from_utf8(arg(2))
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            else {
                // 非法数字参数 → -ERR value is not an integer
                return RespCommand::InvalidInt(name.to_ascii_lowercase());
            };
            RespCommand::Incr {
                key: owned(1),
                delta: if name == "INCRBY" { n } else { n.wrapping_neg() },
            }
        }
        "INCRBYFLOAT" => {
            if arity != 3 {
                return RespCommand::WrongArity("incrbyfloat".into());
            }
            let Some(f) = std::str::from_utf8(arg(2))
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .filter(|f| f.is_finite())
            else {
                return RespCommand::InvalidFloat("incrbyfloat".into());
            };
            RespCommand::IncrFloat {
                key: owned(1),
                delta: f,
            }
        }
        "APPEND" => {
            if arity != 3 {
                return RespCommand::WrongArity("append".into());
            }
            RespCommand::Append {
                key: owned(1),
                suffix: owned(2),
            }
        }
        "SETNX" => {
            if arity != 3 {
                return RespCommand::WrongArity("setnx".into());
            }
            let payload = arg(2);
            let mut value = Vec::with_capacity(1 + payload.len());
            value.push(crate::value_codec::TAG_RAW);
            value.extend_from_slice(payload);
            RespCommand::SetNx {
                key: owned(1),
                value,
            }
        }
        "EXISTS" => {
            if arity < 2 {
                return RespCommand::WrongArity("exists".into());
            }
            RespCommand::Exists {
                keys: (1..arity).map(owned).collect(),
            }
        }
        "STRLEN" => {
            if arity != 2 {
                return RespCommand::WrongArity("strlen".into());
            }
            RespCommand::Strlen { key: owned(1) }
        }
        "TYPE" => {
            if arity != 2 {
                return RespCommand::WrongArity("type".into());
            }
            RespCommand::TypeOf { key: owned(1) }
        }
        "GETRANGE" | "SUBSTR" => {
            if arity != 4 {
                return RespCommand::WrongArity(name.to_ascii_lowercase());
            }
            let (Some(start), Some(end)) = (parse_i64(arg(2)), parse_i64(arg(3))) else {
                return RespCommand::InvalidInt(name.to_ascii_lowercase());
            };
            RespCommand::GetRange { key: owned(1), start, end }
        }
        "SETRANGE" => {
            if arity != 4 {
                return RespCommand::WrongArity("setrange".into());
            }
            let Some(offset) = parse_i64(arg(2)).filter(|&o| o >= 0).map(|o| o as u32) else {
                return RespCommand::InvalidInt("setrange".into());
            };
            RespCommand::SetRange { key: owned(1), offset, data: owned(3) }
        }
        "GETDEL" => {
            if arity != 2 {
                return RespCommand::WrongArity("getdel".into());
            }
            RespCommand::GetDel { key: owned(1) }
        }
        "GETSET" => {
            if arity != 3 {
                return RespCommand::WrongArity("getset".into());
            }
            let payload = arg(2);
            let mut value = Vec::with_capacity(1 + payload.len());
            value.push(crate::value_codec::TAG_RAW);
            value.extend_from_slice(payload);
            RespCommand::GetSet { key: owned(1), value }
        }
        "MSETNX" => {
            if arity < 3 || !(arity - 1).is_multiple_of(2) {
                return RespCommand::WrongArity("msetnx".into());
            }
            let pairs = (1..arity)
                .step_by(2)
                .map(|i| {
                    let payload = arg(i + 1);
                    let mut value = Vec::with_capacity(1 + payload.len());
                    value.push(crate::value_codec::TAG_RAW);
                    value.extend_from_slice(payload);
                    (owned(i), value)
                })
                .collect();
            RespCommand::MSetNx { pairs }
        }
        "HSET" | "HMSET" => {
            if arity < 4 || !(arity - 2).is_multiple_of(2) {
                return RespCommand::WrongArity(name.to_ascii_lowercase());
            }
            let pairs = (2..arity)
                .step_by(2)
                .map(|i| {
                    let payload = arg(i + 1);
                    let mut value = Vec::with_capacity(1 + payload.len());
                    value.push(crate::value_codec::TAG_RAW);
                    value.extend_from_slice(payload);
                    (owned(i), value)
                })
                .collect();
            RespCommand::HSet {
                key: owned(1),
                pairs,
                reply_ok: name == "HMSET",
            }
        }
        "HSETNX" => {
            if arity != 4 {
                return RespCommand::WrongArity("hsetnx".into());
            }
            let payload = arg(3);
            let mut value = Vec::with_capacity(1 + payload.len());
            value.push(crate::value_codec::TAG_RAW);
            value.extend_from_slice(payload);
            RespCommand::HSetNx {
                key: owned(1),
                field: owned(2),
                value,
            }
        }
        "HGET" => {
            if arity != 3 {
                return RespCommand::WrongArity("hget".into());
            }
            RespCommand::HGet { key: owned(1), field: owned(2) }
        }
        "HMGET" => {
            if arity < 3 {
                return RespCommand::WrongArity("hmget".into());
            }
            RespCommand::HMGet {
                key: owned(1),
                fields: (2..arity).map(owned).collect(),
            }
        }
        "HDEL" => {
            if arity < 3 {
                return RespCommand::WrongArity("hdel".into());
            }
            RespCommand::HDel {
                key: owned(1),
                fields: (2..arity).map(owned).collect(),
            }
        }
        "HEXISTS" => {
            if arity != 3 {
                return RespCommand::WrongArity("hexists".into());
            }
            RespCommand::HExists { key: owned(1), field: owned(2) }
        }
        "HLEN" => {
            if arity != 2 {
                return RespCommand::WrongArity("hlen".into());
            }
            RespCommand::HLen { key: owned(1) }
        }
        "HGETALL" => {
            if arity != 2 {
                return RespCommand::WrongArity("hgetall".into());
            }
            RespCommand::HGetAll { key: owned(1) }
        }
        "HKEYS" => {
            if arity != 2 {
                return RespCommand::WrongArity("hkeys".into());
            }
            RespCommand::HKeys { key: owned(1) }
        }
        "HVALS" => {
            if arity != 2 {
                return RespCommand::WrongArity("hvals".into());
            }
            RespCommand::HVals { key: owned(1) }
        }
        "HSCAN" => {
            // v1: 单次全量, cursor/MATCH/COUNT 参数接受但忽略
            if arity < 3 {
                return RespCommand::WrongArity("hscan".into());
            }
            RespCommand::HScan { key: owned(1) }
        }
        "HINCRBY" => {
            if arity != 4 {
                return RespCommand::WrongArity("hincrby".into());
            }
            let Some(n) = parse_i64(arg(3)) else {
                return RespCommand::InvalidInt("hincrby".into());
            };
            RespCommand::HIncrBy {
                key: owned(1),
                field: owned(2),
                delta: n,
            }
        }
        "HINCRBYFLOAT" => {
            if arity != 4 {
                return RespCommand::WrongArity("hincrbyfloat".into());
            }
            let Some(f) = std::str::from_utf8(arg(3))
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .filter(|f| f.is_finite())
            else {
                return RespCommand::InvalidFloat("hincrbyfloat".into());
            };
            RespCommand::HIncrByFloat {
                key: owned(1),
                field: owned(2),
                delta: f,
            }
        }
        "SADD" => {
            if arity < 3 {
                return RespCommand::WrongArity("sadd".into());
            }
            RespCommand::SAdd {
                key: owned(1),
                members: (2..arity).map(owned).collect(),
            }
        }
        "SREM" => {
            if arity < 3 {
                return RespCommand::WrongArity("srem".into());
            }
            RespCommand::SRem {
                key: owned(1),
                members: (2..arity).map(owned).collect(),
            }
        }
        "SISMEMBER" => {
            if arity != 3 {
                return RespCommand::WrongArity("sismember".into());
            }
            RespCommand::SIsMember { key: owned(1), member: owned(2) }
        }
        "SCARD" => {
            if arity != 2 {
                return RespCommand::WrongArity("scard".into());
            }
            RespCommand::SCard { key: owned(1) }
        }
        "SMEMBERS" => {
            if arity != 2 {
                return RespCommand::WrongArity("smembers".into());
            }
            RespCommand::SMembers { key: owned(1) }
        }
        "SSCAN" => {
            if arity < 3 {
                return RespCommand::WrongArity("sscan".into());
            }
            RespCommand::SScan { key: owned(1) }
        }
        "SPOP" => {
            if arity != 2 && arity != 3 {
                return RespCommand::WrongArity("spop".into());
            }
            let count = if arity == 3 {
                match parse_i64(arg(2)).filter(|&c| c >= 0) {
                    Some(c) => Some(c as u32),
                    None => return RespCommand::InvalidInt("spop".into()),
                }
            } else {
                None
            };
            RespCommand::SPop { key: owned(1), count }
        }
        "SRANDMEMBER" => {
            if arity != 2 && arity != 3 {
                return RespCommand::WrongArity("srandmember".into());
            }
            let count = if arity == 3 {
                match parse_i64(arg(2)) {
                    // 负 count (Redis 允许重复) v1 按绝对值去重处理
                    Some(c) => Some(c.unsigned_abs() as u32),
                    None => return RespCommand::InvalidInt("srandmember".into()),
                }
            } else {
                None
            };
            RespCommand::SRandMember { key: owned(1), count }
        }
        "SMISMEMBER" => {
            if arity < 3 {
                return RespCommand::WrongArity("smismember".into());
            }
            RespCommand::SMisMember {
                key: owned(1),
                members: (2..arity).map(owned).collect(),
            }
        }
        "SINTERCARD" => {
            // SINTERCARD numkeys key... [LIMIT n]
            if arity < 3 {
                return RespCommand::WrongArity("sintercard".into());
            }
            let Some(numkeys) = parse_i64(arg(1)).filter(|&n| n > 0).map(|n| n as usize) else {
                return RespCommand::InvalidInt("sintercard".into());
            };
            if arity < 2 + numkeys {
                return RespCommand::WrongArity("sintercard".into());
            }
            let keys: Vec<Vec<u8>> = (2..2 + numkeys).map(owned).collect();
            // 可选 LIMIT n
            let mut limit = 0usize;
            if arity >= 2 + numkeys + 2 && arg(2 + numkeys).eq_ignore_ascii_case(b"LIMIT") {
                let Some(l) = parse_i64(arg(2 + numkeys + 1)).filter(|&l| l >= 0).map(|l| l as usize)
                else {
                    return RespCommand::InvalidInt("sintercard".into());
                };
                limit = l;
            }
            RespCommand::SInterCard { keys, limit }
        }
        "SINTER" | "SUNION" | "SDIFF" => {
            if arity < 2 {
                return RespCommand::WrongArity(name.to_ascii_lowercase());
            }
            let op = match name.as_str() {
                "SINTER" => SetAlgOp::Inter,
                "SUNION" => SetAlgOp::Union,
                _ => SetAlgOp::Diff,
            };
            RespCommand::SetAlg {
                op,
                keys: (1..arity).map(owned).collect(),
            }
        }
        "SINTERSTORE" | "SUNIONSTORE" | "SDIFFSTORE" => {
            if arity < 3 {
                return RespCommand::WrongArity(name.to_ascii_lowercase());
            }
            let op = match name.as_str() {
                "SINTERSTORE" => SetAlgOp::Inter,
                "SUNIONSTORE" => SetAlgOp::Union,
                _ => SetAlgOp::Diff,
            };
            RespCommand::SetAlgStore {
                op,
                dst: owned(1),
                keys: (2..arity).map(owned).collect(),
            }
        }
        "ZINTERSTORE" | "ZUNIONSTORE" => {
            // ZINTERSTORE dst numkeys key... (WEIGHTS/AGGREGATE 本轮不支持)
            if arity < 4 {
                return RespCommand::WrongArity(name.to_ascii_lowercase());
            }
            let Some(numkeys) = parse_i64(arg(2)).filter(|&n| n > 0).map(|n| n as usize) else {
                return RespCommand::InvalidInt(name.to_ascii_lowercase());
            };
            if arity != 3 + numkeys {
                // 含 WEIGHTS/AGGREGATE 或 numkeys 不匹配 → 本轮拒绝
                return RespCommand::WrongArity(name.to_ascii_lowercase());
            }
            RespCommand::ZSetStore {
                inter: name == "ZINTERSTORE",
                dst: owned(1),
                keys: (3..arity).map(owned).collect(),
            }
        }
        "LPUSH" | "RPUSH" => {
            if arity < 3 {
                return RespCommand::WrongArity(name.to_ascii_lowercase());
            }
            let values = (2..arity)
                .map(|i| {
                    let payload = arg(i);
                    let mut v = Vec::with_capacity(1 + payload.len());
                    v.push(crate::value_codec::TAG_RAW);
                    v.extend_from_slice(payload);
                    v
                })
                .collect();
            RespCommand::LPush {
                key: owned(1),
                values,
                left: name == "LPUSH",
            }
        }
        "LPOP" | "RPOP" => {
            if arity != 2 && arity != 3 {
                return RespCommand::WrongArity(name.to_ascii_lowercase());
            }
            let count = if arity == 3 {
                match parse_i64(arg(2)).filter(|&c| c >= 0) {
                    Some(c) => Some(c as u32),
                    None => return RespCommand::InvalidInt(name.to_ascii_lowercase()),
                }
            } else {
                None
            };
            RespCommand::LPop {
                key: owned(1),
                left: name == "LPOP",
                count,
            }
        }
        "LLEN" => {
            if arity != 2 {
                return RespCommand::WrongArity("llen".into());
            }
            RespCommand::LLen { key: owned(1) }
        }
        "LRANGE" => {
            if arity != 4 {
                return RespCommand::WrongArity("lrange".into());
            }
            let (Some(start), Some(end)) = (parse_i64(arg(2)), parse_i64(arg(3))) else {
                return RespCommand::InvalidInt("lrange".into());
            };
            RespCommand::LRange { key: owned(1), start, end }
        }
        "LINDEX" => {
            if arity != 3 {
                return RespCommand::WrongArity("lindex".into());
            }
            let Some(idx) = parse_i64(arg(2)) else {
                return RespCommand::InvalidInt("lindex".into());
            };
            RespCommand::LIndex { key: owned(1), idx }
        }
        "LSET" => {
            if arity != 4 {
                return RespCommand::WrongArity("lset".into());
            }
            let Some(idx) = parse_i64(arg(2)) else {
                return RespCommand::InvalidInt("lset".into());
            };
            let payload = arg(3);
            let mut value = Vec::with_capacity(1 + payload.len());
            value.push(crate::value_codec::TAG_RAW);
            value.extend_from_slice(payload);
            RespCommand::LSet { key: owned(1), idx, value }
        }
        "LREM" => {
            if arity != 4 {
                return RespCommand::WrongArity("lrem".into());
            }
            let Some(count) = parse_i64(arg(2)) else {
                return RespCommand::InvalidInt("lrem".into());
            };
            let payload = arg(3);
            let mut value = Vec::with_capacity(1 + payload.len());
            value.push(crate::value_codec::TAG_RAW);
            value.extend_from_slice(payload);
            RespCommand::LRem { key: owned(1), count, value }
        }
        "LTRIM" => {
            if arity != 4 {
                return RespCommand::WrongArity("ltrim".into());
            }
            let (Some(start), Some(stop)) = (parse_i64(arg(2)), parse_i64(arg(3))) else {
                return RespCommand::InvalidInt("ltrim".into());
            };
            RespCommand::LTrim { key: owned(1), start, stop }
        }
        "LPOS" => {
            // LPOS key element [RANK r] [COUNT n]
            if arity < 3 {
                return RespCommand::WrongArity("lpos".into());
            }
            let payload = arg(2);
            let mut value = Vec::with_capacity(1 + payload.len());
            value.push(crate::value_codec::TAG_RAW);
            value.extend_from_slice(payload);
            let mut rank = 1i64;
            let mut count: Option<u32> = None;
            let mut i = 3;
            while i + 1 < arity {
                if arg(i).eq_ignore_ascii_case(b"RANK") {
                    let Some(r) = parse_i64(arg(i + 1)).filter(|&r| r != 0) else {
                        return RespCommand::InvalidInt("lpos".into());
                    };
                    rank = r;
                } else if arg(i).eq_ignore_ascii_case(b"COUNT") {
                    let Some(c) = parse_i64(arg(i + 1)).filter(|&c| c >= 0) else {
                        return RespCommand::InvalidInt("lpos".into());
                    };
                    count = Some(c as u32);
                } else {
                    return RespCommand::WrongArity("lpos".into());
                }
                i += 2;
            }
            RespCommand::LPos { key: owned(1), value, rank, count }
        }
        "LINSERT" => {
            if arity != 5 {
                return RespCommand::WrongArity("linsert".into());
            }
            let before = if arg(2).eq_ignore_ascii_case(b"BEFORE") {
                true
            } else if arg(2).eq_ignore_ascii_case(b"AFTER") {
                false
            } else {
                return RespCommand::WrongArity("linsert".into());
            };
            let mut pivot = Vec::with_capacity(1 + arg(3).len());
            pivot.push(crate::value_codec::TAG_RAW);
            pivot.extend_from_slice(arg(3));
            let mut value = Vec::with_capacity(1 + arg(4).len());
            value.push(crate::value_codec::TAG_RAW);
            value.extend_from_slice(arg(4));
            RespCommand::LInsert { key: owned(1), before, pivot, value }
        }
        "ZADD" => {
            if arity < 4 || !(arity - 2).is_multiple_of(2) {
                return RespCommand::WrongArity("zadd".into());
            }
            let mut pairs = Vec::new();
            let mut i = 2;
            while i < arity {
                let Some(score) = std::str::from_utf8(arg(i))
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                    .filter(|f| !f.is_nan())
                else {
                    return RespCommand::InvalidFloat("zadd".into());
                };
                pairs.push((score, owned(i + 1)));
                i += 2;
            }
            RespCommand::ZAdd { key: owned(1), pairs }
        }
        "ZREM" => {
            if arity < 3 {
                return RespCommand::WrongArity("zrem".into());
            }
            RespCommand::ZRem {
                key: owned(1),
                members: (2..arity).map(owned).collect(),
            }
        }
        "ZSCORE" => {
            if arity != 3 {
                return RespCommand::WrongArity("zscore".into());
            }
            RespCommand::ZScore { key: owned(1), member: owned(2) }
        }
        "ZCARD" => {
            if arity != 2 {
                return RespCommand::WrongArity("zcard".into());
            }
            RespCommand::ZCard { key: owned(1) }
        }
        "ZINCRBY" => {
            if arity != 4 {
                return RespCommand::WrongArity("zincrby".into());
            }
            let Some(delta) = std::str::from_utf8(arg(2))
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .filter(|f| f.is_finite())
            else {
                return RespCommand::InvalidFloat("zincrby".into());
            };
            RespCommand::ZIncrBy { key: owned(1), delta, member: owned(3) }
        }
        "ZRANGE" | "ZREVRANGE" => {
            if arity != 4 && arity != 5 {
                return RespCommand::WrongArity(name.to_ascii_lowercase());
            }
            let (Some(start), Some(end)) = (parse_i64(arg(2)), parse_i64(arg(3))) else {
                return RespCommand::InvalidInt(name.to_ascii_lowercase());
            };
            let withscores = arity == 5
                && arg(4).eq_ignore_ascii_case(b"WITHSCORES");
            RespCommand::ZRange {
                key: owned(1),
                start,
                end,
                rev: name == "ZREVRANGE",
                withscores,
            }
        }
        "ZRANGEBYSCORE" => {
            if arity != 4 && arity != 5 {
                return RespCommand::WrongArity("zrangebyscore".into());
            }
            let (Some(min), Some(max)) = (parse_score_bound(arg(2)), parse_score_bound(arg(3)))
            else {
                return RespCommand::InvalidFloat("zrangebyscore".into());
            };
            let withscores = arity == 5 && arg(4).eq_ignore_ascii_case(b"WITHSCORES");
            RespCommand::ZRangeByScore { key: owned(1), min, max, withscores }
        }
        "ZRANK" | "ZREVRANK" => {
            if arity != 3 {
                return RespCommand::WrongArity(name.to_ascii_lowercase());
            }
            RespCommand::ZRank {
                key: owned(1),
                member: owned(2),
                rev: name == "ZREVRANK",
            }
        }
        "GEOADD" => {
            // GEOADD key lon lat member ... (NX/XX/CH 本轮不支持)
            if arity < 5 || !(arity - 2).is_multiple_of(3) {
                return RespCommand::WrongArity("geoadd".into());
            }
            let mut pairs = Vec::new();
            let mut i = 2;
            while i < arity {
                let (Some(lon), Some(lat)) = (parse_f64(arg(i)), parse_f64(arg(i + 1))) else {
                    return RespCommand::InvalidFloat("geoadd".into());
                };
                let Some(bits) = crate::geo_bridge::encode(lon, lat) else {
                    return RespCommand::InvalidFloat("geoadd".into()); // 超经纬度范围
                };
                pairs.push((bits as f64, owned(i + 2)));
                i += 3;
            }
            // ⭐ 直接复用 ZAdd 全链路 (score = geohash), 回 :新增数 (Redis 同)
            RespCommand::ZAdd { key: owned(1), pairs }
        }
        "GEOPOS" => {
            if arity < 3 {
                return RespCommand::WrongArity("geopos".into());
            }
            RespCommand::GeoPos {
                key: owned(1),
                members: (2..arity).map(owned).collect(),
            }
        }
        "GEODIST" => {
            if arity != 4 && arity != 5 {
                return RespCommand::WrongArity("geodist".into());
            }
            let factor = if arity == 5 {
                match crate::geo_bridge::unit_factor(arg(4)) {
                    Some(f) => f,
                    None => return RespCommand::InvalidFloat("geodist".into()),
                }
            } else {
                1.0
            };
            RespCommand::GeoDist {
                key: owned(1),
                m1: owned(2),
                m2: owned(3),
                factor,
            }
        }
        "GEOSEARCH" => {
            // GEOSEARCH key FROMLONLAT lon lat BYRADIUS r unit
            //           [ASC|DESC] [COUNT n] [WITHCOORD] [WITHDIST]
            // (FROMMEMBER / BYBOX 本轮不支持)
            if arity < 8 {
                return RespCommand::WrongArity("geosearch".into());
            }
            if !arg(2).eq_ignore_ascii_case(b"FROMLONLAT")
                || !arg(5).eq_ignore_ascii_case(b"BYRADIUS")
            {
                return RespCommand::WrongArity("geosearch".into());
            }
            let (Some(lon), Some(lat), Some(r)) =
                (parse_f64(arg(3)), parse_f64(arg(4)), parse_f64(arg(6)))
            else {
                return RespCommand::InvalidFloat("geosearch".into());
            };
            let Some(factor) = crate::geo_bridge::unit_factor(arg(7)) else {
                return RespCommand::InvalidFloat("geosearch".into());
            };
            let (mut asc, mut count, mut withcoord, mut withdist) = (true, 0usize, false, false);
            let mut i = 8;
            while i < arity {
                let a = arg(i);
                if a.eq_ignore_ascii_case(b"ASC") {
                    asc = true;
                } else if a.eq_ignore_ascii_case(b"DESC") {
                    asc = false;
                } else if a.eq_ignore_ascii_case(b"WITHCOORD") {
                    withcoord = true;
                } else if a.eq_ignore_ascii_case(b"WITHDIST") {
                    withdist = true;
                } else if a.eq_ignore_ascii_case(b"COUNT") && i + 1 < arity {
                    let Some(c) = parse_i64(arg(i + 1)).filter(|&c| c > 0) else {
                        return RespCommand::InvalidInt("geosearch".into());
                    };
                    count = c as usize;
                    i += 1;
                } else {
                    return RespCommand::WrongArity("geosearch".into());
                }
                i += 1;
            }
            RespCommand::GeoSearch {
                key: owned(1),
                lon,
                lat,
                radius_m: r * factor,
                asc,
                count,
                withcoord,
                withdist,
            }
        }
        "ZCOUNT" => {
            if arity != 4 {
                return RespCommand::WrongArity("zcount".into());
            }
            let (Some(min), Some(max)) = (parse_score_bound(arg(2)), parse_score_bound(arg(3)))
            else {
                return RespCommand::InvalidFloat("zcount".into());
            };
            RespCommand::ZCount { key: owned(1), min, max }
        }
        "ZMSCORE" => {
            if arity < 3 {
                return RespCommand::WrongArity("zmscore".into());
            }
            RespCommand::ZMScore {
                key: owned(1),
                members: (2..arity).map(owned).collect(),
            }
        }
        "ZPOPMIN" | "ZPOPMAX" => {
            if arity != 2 && arity != 3 {
                return RespCommand::WrongArity(name.to_ascii_lowercase());
            }
            let count = if arity == 3 {
                match parse_i64(arg(2)).filter(|&c| c >= 0) {
                    Some(c) => c as u32,
                    None => return RespCommand::InvalidInt(name.to_ascii_lowercase()),
                }
            } else {
                1
            };
            RespCommand::ZPop {
                key: owned(1),
                rev: name == "ZPOPMAX",
                count,
            }
        }
        "HSTRLEN" => {
            if arity != 3 {
                return RespCommand::WrongArity("hstrlen".into());
            }
            RespCommand::HStrlen { key: owned(1), field: owned(2) }
        }
        "SETBIT" => {
            if arity != 4 {
                return RespCommand::WrongArity("setbit".into());
            }
            let Some(offset) = parse_i64(arg(2)).filter(|&o| o >= 0).map(|o| o as u64) else {
                return RespCommand::InvalidInt("setbit".into());
            };
            let bit = match arg(3) {
                b"0" => false,
                b"1" => true,
                _ => return RespCommand::InvalidInt("setbit".into()),
            };
            RespCommand::SetBit { key: owned(1), offset, bit }
        }
        "GETBIT" => {
            if arity != 3 {
                return RespCommand::WrongArity("getbit".into());
            }
            let Some(offset) = parse_i64(arg(2)).filter(|&o| o >= 0).map(|o| o as u64) else {
                return RespCommand::InvalidInt("getbit".into());
            };
            RespCommand::GetBit { key: owned(1), offset }
        }
        "BITCOUNT" => {
            // BITCOUNT key [start end [BYTE]] (BIT 粒度本轮不支持)
            if arity != 2 && arity != 4 && arity != 5 {
                return RespCommand::WrongArity("bitcount".into());
            }
            if arity == 5 && !arg(4).eq_ignore_ascii_case(b"BYTE") {
                return RespCommand::WrongArity("bitcount".into());
            }
            let (start, end) = if arity >= 4 {
                match (parse_i64(arg(2)), parse_i64(arg(3))) {
                    (Some(s), Some(e)) => (s, e),
                    _ => return RespCommand::InvalidInt("bitcount".into()),
                }
            } else {
                (0, -1)
            };
            RespCommand::BitCount { key: owned(1), start, end }
        }
        "BITPOS" => {
            // BITPOS key bit [start [end [BYTE]]]
            if !(3..=6).contains(&arity) {
                return RespCommand::WrongArity("bitpos".into());
            }
            if arity == 6 && !arg(5).eq_ignore_ascii_case(b"BYTE") {
                return RespCommand::WrongArity("bitpos".into());
            }
            let bit = match arg(2) {
                b"0" => false,
                b"1" => true,
                _ => return RespCommand::InvalidInt("bitpos".into()),
            };
            let start = if arity >= 4 {
                match parse_i64(arg(3)) {
                    Some(s) => s,
                    None => return RespCommand::InvalidInt("bitpos".into()),
                }
            } else {
                0
            };
            let end = if arity >= 5 {
                match parse_i64(arg(4)) {
                    Some(e) => Some(e),
                    None => return RespCommand::InvalidInt("bitpos".into()),
                }
            } else {
                None
            };
            RespCommand::BitPos { key: owned(1), bit, start, end }
        }
        "HRANDFIELD" => {
            if !(2..=4).contains(&arity) {
                return RespCommand::WrongArity("hrandfield".into());
            }
            // 无 count → 单 bulk; 有 count → 数组 (负值 v1 按绝对值去重)
            let count = if arity >= 3 {
                match parse_i64(arg(2)) {
                    Some(c) => Some(c.unsigned_abs() as u32),
                    None => return RespCommand::InvalidInt("hrandfield".into()),
                }
            } else {
                None
            };
            let withvalues = arity == 4 && arg(3).eq_ignore_ascii_case(b"WITHVALUES");
            RespCommand::HRandField { key: owned(1), count, withvalues }
        }
        "ECHO" => {
            if arity != 2 {
                return RespCommand::WrongArity("echo".into());
            }
            RespCommand::Echo(owned(1))
        }
        "AUTH" => match arity {
            2 => RespCommand::Auth {
                user: None,
                pass: owned(1),
            },
            3 => RespCommand::Auth {
                user: Some(owned(1)),
                pass: owned(2),
            },
            _ => RespCommand::WrongArity("auth".into()),
        },
        "QUIT" => RespCommand::Quit,
        "COMMAND" => RespCommand::Command,
        "HELLO" => RespCommand::Hello(if arity >= 2 { Some(owned(1)) } else { None }),
        "SELECT" => RespCommand::Select,
        other => RespCommand::Unknown(other.to_ascii_lowercase()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_full(buf: &[u8]) -> (usize, RespCommand) {
        match RespCodec::new().decode_command(buf).unwrap() {
            DecodeOutcome::Complete { consumed, value } => (consumed, value),
            DecodeOutcome::NeedMore => panic!("expected complete frame"),
        }
    }

    #[test]
    fn parse_set_get_del() {
        let (n, cmd) = decode_full(b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$5\r\nhello\r\n");
        assert_eq!(n, 32);
        // ⭐ SET 的 value 在 decode 时预置 1B type tag ([TAG_RAW][payload])
        let mut tagged = vec![crate::value_codec::TAG_RAW];
        tagged.extend_from_slice(b"hello");
        assert_eq!(
            cmd,
            RespCommand::Set {
                key: b"k1".to_vec(),
                value: tagged
            }
        );

        let (_, cmd) = decode_full(b"*2\r\n$3\r\nget\r\n$2\r\nk1\r\n");
        assert_eq!(cmd, RespCommand::Get { key: b"k1".to_vec() });

        let (_, cmd) = decode_full(b"*3\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\nb\r\n");
        assert_eq!(
            cmd,
            RespCommand::Del {
                keys: vec![b"a".to_vec(), b"b".to_vec()]
            }
        );
    }

    #[test]
    fn parse_partial_frames_need_more() {
        let full = b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$5\r\nhello\r\n";
        let codec = RespCodec::new();
        for cut in 1..full.len() {
            match codec.decode_command(&full[..cut]) {
                Ok(DecodeOutcome::NeedMore) => {}
                other => panic!("cut={cut}: expected NeedMore, got {other:?}"),
            }
        }
    }

    #[test]
    fn parse_pipeline_multiple_commands() {
        let buf = b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n";
        let codec = RespCodec::new();
        let (n1, c1) = match codec.decode_command(buf).unwrap() {
            DecodeOutcome::Complete { consumed, value } => (consumed, value),
            _ => panic!(),
        };
        assert_eq!(c1, RespCommand::Ping(None));
        let (_, c2) = match codec.decode_command(&buf[n1..]).unwrap() {
            DecodeOutcome::Complete { consumed, value } => (consumed, value),
            _ => panic!(),
        };
        assert_eq!(c2, RespCommand::Echo(b"hi".to_vec()));
    }

    #[test]
    fn parse_auth_forms() {
        let (_, cmd) = decode_full(b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n");
        assert_eq!(
            cmd,
            RespCommand::Auth {
                user: None,
                pass: b"secret".to_vec()
            }
        );
        let (_, cmd) = decode_full(b"*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$6\r\nsecret\r\n");
        assert_eq!(
            cmd,
            RespCommand::Auth {
                user: Some(b"default".to_vec()),
                pass: b"secret".to_vec()
            }
        );
    }

    #[test]
    fn inline_command_rejected() {
        let err = RespCodec::new().decode_command(b"PING\r\n").unwrap_err();
        assert!(err.contains("expected '*'"), "{err}");
    }

    #[test]
    fn invalid_bulk_len_rejected() {
        let err = RespCodec::new()
            .decode_command(b"*1\r\n$-5\r\n")
            .unwrap_err();
        assert!(err.contains("invalid bulk length"), "{err}");
    }

    #[test]
    fn unknown_and_arity() {
        let (_, cmd) = decode_full(b"*1\r\n$5\r\nFLUSH\r\n");
        assert_eq!(cmd, RespCommand::Unknown("flush".into()));
        let (_, cmd) = decode_full(b"*2\r\n$3\r\nSET\r\n$1\r\nk\r\n");
        assert_eq!(cmd, RespCommand::WrongArity("set".into()));
    }

    #[test]
    fn encode_replies() {
        let c = RespCodec::new();
        assert_eq!(c.encode_ok(), b"+OK\r\n");
        assert_eq!(c.encode_simple("PONG"), b"+PONG\r\n");
        assert_eq!(c.encode_bulk(b"abc"), b"$3\r\nabc\r\n");
        assert_eq!(c.encode_nil(), b"$-1\r\n");
        assert_eq!(c.encode_integer(2), b":2\r\n");
        assert_eq!(c.encode_error("boom"), b"-ERR boom\r\n");
        assert_eq!(
            c.encode_error("NOAUTH Authentication required."),
            b"-NOAUTH Authentication required.\r\n"
        );
        assert_eq!(c.encode_error("bad\r\nmsg"), b"-ERR bad  msg\r\n");
    }
}
