// ⭐ 解耦 2026-08: RESP 命令参数解析 (从 resp.rs 拆出).
// 职责: RESP2 数组参数 → RespCommand (args_to_command + 命令族辅助).
use super::resp::{RespCommand, SetAlgOp, parse_f64, parse_i64, parse_score_bound};

pub(crate) fn args_to_command(buf: &[u8], args: &[(usize, usize)]) -> RespCommand {
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
                delta: if name == "INCRBY" {
                    n
                } else {
                    n.wrapping_neg()
                },
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
            RespCommand::GetRange {
                key: owned(1),
                start,
                end,
            }
        }
        "SETRANGE" => {
            if arity != 4 {
                return RespCommand::WrongArity("setrange".into());
            }
            let Some(offset) = parse_i64(arg(2)).filter(|&o| o >= 0).map(|o| o as u32) else {
                return RespCommand::InvalidInt("setrange".into());
            };
            RespCommand::SetRange {
                key: owned(1),
                offset,
                data: owned(3),
            }
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
            RespCommand::GetSet {
                key: owned(1),
                value,
            }
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
            RespCommand::HGet {
                key: owned(1),
                field: owned(2),
            }
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
        "HQUERY" => {
            // Strict grammar: HQUERY table WHERE c op v [AND c op v ...] FIELDS f... LIMIT n
            if arity < 8 || !arg(2).eq_ignore_ascii_case(b"WHERE") {
                return RespCommand::WrongArity("hquery".into());
            }
            let mut i = 3;
            let mut terms = Vec::new();
            loop {
                if i + 2 >= arity {
                    return RespCommand::WrongArity("hquery".into());
                }
                let op = arg(i + 1);
                if !matches!(op, b"=" | b">" | b">=" | b"<" | b"<=") {
                    return RespCommand::Unknown("HQUERY only supports =, >, >=, <, <=".into());
                }
                terms.push((owned(i), owned(i + 1), owned(i + 2)));
                i += 3;
                if i >= arity {
                    return RespCommand::WrongArity("hquery".into());
                }
                if arg(i).eq_ignore_ascii_case(b"AND") {
                    i += 1;
                    continue;
                }
                break;
            }
            if !arg(i).eq_ignore_ascii_case(b"FIELDS") {
                return RespCommand::WrongArity("hquery".into());
            }
            i += 1;
            let fields_start = i;
            while i < arity && !arg(i).eq_ignore_ascii_case(b"LIMIT") {
                i += 1;
            }
            if i == fields_start || i + 1 != arity - 1 || !arg(i).eq_ignore_ascii_case(b"LIMIT") {
                return RespCommand::WrongArity("hquery".into());
            }
            let Some(limit) = std::str::from_utf8(arg(i + 1))
                .ok()
                .and_then(|s| s.parse::<u32>().ok())
                .filter(|n| *n > 0 && *n <= 10_000)
            else {
                return RespCommand::InvalidInt("hquery limit".into());
            };
            RespCommand::HQuery {
                table: owned(1),
                terms,
                fields: (fields_start..i).map(owned).collect(),
                limit,
            }
        }
        "HEXISTS" => {
            if arity != 3 {
                return RespCommand::WrongArity("hexists".into());
            }
            RespCommand::HExists {
                key: owned(1),
                field: owned(2),
            }
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
            RespCommand::SIsMember {
                key: owned(1),
                member: owned(2),
            }
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
            RespCommand::SPop {
                key: owned(1),
                count,
            }
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
            RespCommand::SRandMember {
                key: owned(1),
                count,
            }
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
                let Some(l) = parse_i64(arg(2 + numkeys + 1))
                    .filter(|&l| l >= 0)
                    .map(|l| l as usize)
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
            RespCommand::LRange {
                key: owned(1),
                start,
                end,
            }
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
            RespCommand::LSet {
                key: owned(1),
                idx,
                value,
            }
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
            RespCommand::LRem {
                key: owned(1),
                count,
                value,
            }
        }
        "LTRIM" => {
            if arity != 4 {
                return RespCommand::WrongArity("ltrim".into());
            }
            let (Some(start), Some(stop)) = (parse_i64(arg(2)), parse_i64(arg(3))) else {
                return RespCommand::InvalidInt("ltrim".into());
            };
            RespCommand::LTrim {
                key: owned(1),
                start,
                stop,
            }
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
            RespCommand::LPos {
                key: owned(1),
                value,
                rank,
                count,
            }
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
            RespCommand::LInsert {
                key: owned(1),
                before,
                pivot,
                value,
            }
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
            RespCommand::ZAdd {
                key: owned(1),
                pairs,
            }
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
            RespCommand::ZScore {
                key: owned(1),
                member: owned(2),
            }
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
            RespCommand::ZIncrBy {
                key: owned(1),
                delta,
                member: owned(3),
            }
        }
        "ZRANGE" | "ZREVRANGE" => {
            if arity != 4 && arity != 5 {
                return RespCommand::WrongArity(name.to_ascii_lowercase());
            }
            let (Some(start), Some(end)) = (parse_i64(arg(2)), parse_i64(arg(3))) else {
                return RespCommand::InvalidInt(name.to_ascii_lowercase());
            };
            let withscores = arity == 5 && arg(4).eq_ignore_ascii_case(b"WITHSCORES");
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
            RespCommand::ZRangeByScore {
                key: owned(1),
                min,
                max,
                withscores,
            }
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
            RespCommand::ZAdd {
                key: owned(1),
                pairs,
            }
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
            RespCommand::ZCount {
                key: owned(1),
                min,
                max,
            }
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
            RespCommand::HStrlen {
                key: owned(1),
                field: owned(2),
            }
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
            RespCommand::SetBit {
                key: owned(1),
                offset,
                bit,
            }
        }
        "GETBIT" => {
            if arity != 3 {
                return RespCommand::WrongArity("getbit".into());
            }
            let Some(offset) = parse_i64(arg(2)).filter(|&o| o >= 0).map(|o| o as u64) else {
                return RespCommand::InvalidInt("getbit".into());
            };
            RespCommand::GetBit {
                key: owned(1),
                offset,
            }
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
            RespCommand::BitCount {
                key: owned(1),
                start,
                end,
            }
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
            RespCommand::BitPos {
                key: owned(1),
                bit,
                start,
                end,
            }
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
            RespCommand::HRandField {
                key: owned(1),
                count,
                withvalues,
            }
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
        "SELECT" => {
            // ⭐ Y2: SQL 已迁独立端口, SELECT 回归纯 Redis 选库语义
            if arity != 2 {
                return RespCommand::WrongArity("select".into());
            }
            let Some(idx) = parse_i64(arg(1)) else {
                return RespCommand::InvalidInt("select".into());
            };
            RespCommand::Select { idx }
        }
        other => RespCommand::Unknown(other.to_ascii_lowercase()),
    }
}
