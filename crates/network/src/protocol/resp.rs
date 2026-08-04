//! RESP2 协议门面 (Redis 兼容).
//!
//! 解析 `*N\r\n$len\r\narg\r\n...` (array of bulk strings) 增量帧,
//! 输出 `RespCommand`; 编码器输出 RESP2 回复 (`+OK` / `$n` / `:n` / `-ERR`).
//!
//! **范围**: RESP2 only. `HELLO 3` 回 `-NOPROTO` 让客户端回退 RESP2.
//! **认证**: AUTH 命令在 worker 本地处理 (per-conn 状态), 不进 shard.

use super::DecodeOutcome;
use super::resp_cmd::args_to_command;

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
    /// SELECT idx → per-connection 切库 (idx 经 DbDirView 翻译成 db name;
    /// 越界回 -ERR DB index is out of range)
    Select { idx: i64 },
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
pub(crate) fn parse_score_bound(b: &[u8]) -> Option<f64> {
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
pub(crate) fn parse_i64(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse::<i64>().ok()
}

/// ⭐ Phase G: 解析有限 f64 (经纬度/半径).
pub(crate) fn parse_f64(bytes: &[u8]) -> Option<f64> {
    std::str::from_utf8(bytes)
        .ok()?
        .parse::<f64>()
        .ok()
        .filter(|f| f.is_finite())
}

/// 从 `buf[start..]` 解析 `<digits>\r\n`, 返回 (值, \r\n 之后的位置).
/// 数据不足返回 Ok(None); 非法数字返回 Err.
pub(crate) fn parse_int_line(buf: &[u8], start: usize) -> Result<Option<(i64, usize)>, String> {
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
