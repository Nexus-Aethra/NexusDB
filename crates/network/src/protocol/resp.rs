//! RESP2 协议门面 (Redis 兼容).
//!
//! 解析 `*N\r\n$len\r\narg\r\n...` (array of bulk strings) 增量帧,
//! 输出 `RespCommand`; 编码器输出 RESP2 回复 (`+OK` / `$n` / `:n` / `-ERR`).
//!
//! **范围**: RESP2 only. `HELLO 3` 回 `-NOPROTO` 让客户端回退 RESP2.
//! **认证**: AUTH 命令在 worker 本地处理 (per-conn 状态), 不进 shard.

use super::DecodeOutcome;

/// RESP 命令 (worker 侧分发单元).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespCommand {
    /// SET key value → BatchOp::Put
    Set { key: Vec<u8>, value: Vec<u8> },
    /// GET key → BatchOp::Get
    Get { key: Vec<u8> },
    /// DEL key [key ...] → 多个 BatchOp::Delete, 回复 :N
    Del { keys: Vec<Vec<u8>> },
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

        let mut args: Vec<Vec<u8>> = Vec::with_capacity(argc as usize);
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
            args.push(buf[data_start..data_start + blen].to_vec());
            pos = data_start + blen + 2;
        }

        Ok(DecodeOutcome::Complete {
            consumed: pos,
            value: args_to_command(args),
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
        // 已带错误码前缀 (如 WRONGPASS/NOAUTH/NOPROTO) 的消息不再加 ERR
        if clean.starts_with("NOAUTH")
            || clean.starts_with("WRONGPASS")
            || clean.starts_with("NOPROTO")
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

/// args → RespCommand (命令名大小写不敏感).
fn args_to_command(mut args: Vec<Vec<u8>>) -> RespCommand {
    let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    let arity = args.len();
    match name.as_str() {
        "SET" => {
            if arity < 3 {
                return RespCommand::WrongArity("set".into());
            }
            // 忽略 SET 的扩展参数 (EX/PX/NX/XX...) — 后续版本支持
            let value = args.swap_remove(2);
            let key = args.swap_remove(1);
            RespCommand::Set { key, value }
        }
        "GET" => {
            if arity != 2 {
                return RespCommand::WrongArity("get".into());
            }
            RespCommand::Get {
                key: args.swap_remove(1),
            }
        }
        "DEL" => {
            if arity < 2 {
                return RespCommand::WrongArity("del".into());
            }
            RespCommand::Del {
                keys: args.drain(1..).collect(),
            }
        }
        "PING" => match arity {
            1 => RespCommand::Ping(None),
            2 => RespCommand::Ping(Some(args.swap_remove(1))),
            _ => RespCommand::WrongArity("ping".into()),
        },
        "ECHO" => {
            if arity != 2 {
                return RespCommand::WrongArity("echo".into());
            }
            RespCommand::Echo(args.swap_remove(1))
        }
        "AUTH" => match arity {
            2 => RespCommand::Auth {
                user: None,
                pass: args.swap_remove(1),
            },
            3 => {
                let pass = args.swap_remove(2);
                let user = args.swap_remove(1);
                RespCommand::Auth {
                    user: Some(user),
                    pass,
                }
            }
            _ => RespCommand::WrongArity("auth".into()),
        },
        "QUIT" => RespCommand::Quit,
        "COMMAND" => RespCommand::Command,
        "HELLO" => RespCommand::Hello(if arity >= 2 {
            Some(args.swap_remove(1))
        } else {
            None
        }),
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
        assert_eq!(
            cmd,
            RespCommand::Set {
                key: b"k1".to_vec(),
                value: b"hello".to_vec()
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
