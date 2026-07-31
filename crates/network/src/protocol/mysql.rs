//! ⭐ Z1 (MySQL wire 门面): 协议纯函数 — 帧/握手/登录/结果集编码.
//!
//! 子集 (v1, 以 mysql cli 兼容为验收标准):
//! - 帧: `[len u24 LE][seq u8][payload]`
//! - HandshakeV10 (20B salt, PROTOCOL_41 | SECURE_CONNECTION | PLUGIN_AUTH,
//!   charset utf8mb4, **不设 DEPRECATE_EOF** — 老式 EOF 结果集最广兼容)
//! - `mysql_native_password`: `SHA1(pwd) XOR SHA1(salt ‖ SHA1(SHA1(pwd)))`
//!   + AuthSwitchRequest 兜底 (8.x 客户端默认 caching_sha2 时切换)
//! - 文本协议结果集: 列数 + 列定义×M + EOF + 行 (lenenc, NULL=0xFB) + EOF

use storage::row::ColValue;
use storage::schema::ColType;

// ===== capability flags (仅用到的) =====
pub const CLIENT_LONG_PASSWORD: u32 = 0x1;
pub const CLIENT_CONNECT_WITH_DB: u32 = 0x8;
pub const CLIENT_PROTOCOL_41: u32 = 0x200;
pub const CLIENT_SECURE_CONNECTION: u32 = 0x8000;
pub const CLIENT_PLUGIN_AUTH: u32 = 0x8_0000;
pub const CLIENT_PLUGIN_AUTH_LENENC_DATA: u32 = 0x20_0000;

/// 服务端声明的 capability.
pub const SERVER_CAPS: u32 = CLIENT_LONG_PASSWORD
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_PROTOCOL_41
    | CLIENT_SECURE_CONNECTION
    | CLIENT_PLUGIN_AUTH;

const CHARSET_UTF8MB4: u8 = 45; // utf8mb4_general_ci
const STATUS_AUTOCOMMIT: u16 = 0x0002;
const AUTH_PLUGIN: &[u8] = b"mysql_native_password";

// =====================================================================
// SHA-1 (RFC 3174; 零依赖手写, 仅 auth 用 — 非安全敏感新协议场景)
// =====================================================================

pub fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [0x6745_2301, 0xEFCD_AB89, 0x98BA_DCFE, 0x1032_5476, 0xC3D2_E1F0];
    let ml = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&ml.to_be_bytes());

    let mut w = [0u32; 80];
    for chunk in msg.chunks_exact(64) {
        for (i, word) in w.iter_mut().take(16).enumerate() {
            *word = u32::from_be_bytes(chunk[i * 4..i * 4 + 4].try_into().expect("4B"));
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h[0], h[1], h[2], h[3], h[4]);
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let tmp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = tmp;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }
    let mut out = [0u8; 20];
    for (i, x) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&x.to_be_bytes());
    }
    out
}

/// `mysql_native_password` 校验.
/// 空密码 ⇔ 空 auth_resp; 非空: resp == SHA1(pwd) XOR SHA1(salt ‖ SHA1(SHA1(pwd))).
pub fn native_password_ok(salt: &[u8; 20], auth_resp: &[u8], password: &str) -> bool {
    if password.is_empty() {
        return auth_resp.is_empty();
    }
    if auth_resp.len() != 20 {
        return false;
    }
    let stage1 = sha1(password.as_bytes());
    let stage2 = sha1(&stage1);
    let mut buf = Vec::with_capacity(40);
    buf.extend_from_slice(salt);
    buf.extend_from_slice(&stage2);
    let scramble = sha1(&buf);
    // resp XOR scramble == stage1
    let mut derived = [0u8; 20];
    for i in 0..20 {
        derived[i] = auth_resp[i] ^ scramble[i];
    }
    derived == stage1
}

/// 客户端侧 token 生成 (e2e 测试用).
pub fn native_password_token(salt: &[u8; 20], password: &str) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let stage1 = sha1(password.as_bytes());
    let stage2 = sha1(&stage1);
    let mut buf = Vec::with_capacity(40);
    buf.extend_from_slice(salt);
    buf.extend_from_slice(&stage2);
    let scramble = sha1(&buf);
    (0..20).map(|i| stage1[i] ^ scramble[i]).collect()
}

/// ⭐ F82: `caching_sha2_password` fast-auth 校验.
/// scramble = SHA256(pw) XOR SHA256(SHA256(SHA256(pw)) ‖ nonce); nonce = 20B salt.
/// 服务端知道明文口令即可直接验证 (免 RSA/TLS full-auth). 空口令 ⇔ 空响应.
pub fn caching_sha2_password_ok(salt: &[u8; 20], auth_resp: &[u8], password: &str) -> bool {
    use crate::protocol::crypto::sha256;
    if password.is_empty() {
        return auth_resp.is_empty();
    }
    if auth_resp.len() != 32 {
        return false;
    }
    let s1 = sha256(password.as_bytes());
    let s2 = sha256(&s1);
    let mut buf = Vec::with_capacity(52);
    buf.extend_from_slice(&s2);
    buf.extend_from_slice(salt);
    let inner = sha256(&buf);
    // auth_resp XOR inner == s1  ⇔  校验通过
    (0..32).all(|i| auth_resp[i] ^ inner[i] == s1[i])
}

/// ⭐ F82: caching_sha2 客户端侧 fast-auth token (e2e 测试用).
pub fn caching_sha2_token(salt: &[u8; 20], password: &str) -> Vec<u8> {
    use crate::protocol::crypto::sha256;
    if password.is_empty() {
        return Vec::new();
    }
    let s1 = sha256(password.as_bytes());
    let s2 = sha256(&s1);
    let mut buf = Vec::with_capacity(52);
    buf.extend_from_slice(&s2);
    buf.extend_from_slice(salt);
    let inner = sha256(&buf);
    (0..32).map(|i| s1[i] ^ inner[i]).collect()
}

/// ⭐ F82: caching_sha2 fast_auth_success 包 (0x01 0x03); 其后再发 OK.
pub fn build_fast_auth_success(seq: u8) -> Vec<u8> {
    write_packet(seq, &[0x01, 0x03])
}

// =====================================================================
// 帧
// =====================================================================

/// 解一帧: Some((seq, payload_range_end_offset, payload)) / None = 半包.
/// 返回 (seq, 整帧长度, payload owned).
pub fn read_packet(buf: &[u8]) -> Option<(u8, usize, Vec<u8>)> {
    if buf.len() < 4 {
        return None;
    }
    let len = u32::from_le_bytes([buf[0], buf[1], buf[2], 0]) as usize;
    if buf.len() < 4 + len {
        return None;
    }
    Some((buf[3], 4 + len, buf[4..4 + len].to_vec()))
}

/// 封帧 (v1 不处理 16MB 分片 — 结果集单包上限内, 记录).
pub fn write_packet(seq: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes()[..3]);
    out.push(seq);
    out.extend_from_slice(payload);
    out
}

// ===== lenenc =====

pub fn lenenc_int(out: &mut Vec<u8>, v: u64) {
    match v {
        0..=250 => out.push(v as u8),
        251..=0xFFFF => {
            out.push(0xFC);
            out.extend_from_slice(&(v as u16).to_le_bytes());
        }
        0x1_0000..=0xFF_FFFF => {
            out.push(0xFD);
            out.extend_from_slice(&(v as u32).to_le_bytes()[..3]);
        }
        _ => {
            out.push(0xFE);
            out.extend_from_slice(&v.to_le_bytes());
        }
    }
}

pub fn lenenc_bytes(out: &mut Vec<u8>, b: &[u8]) {
    lenenc_int(out, b.len() as u64);
    out.extend_from_slice(b);
}

// =====================================================================
// 握手 / 登录
// =====================================================================

/// HandshakeV10 (连接建立后服务端首包, seq=0).
pub fn build_handshake_v10(salt: &[u8; 20], thread_id: u32) -> Vec<u8> {
    build_handshake_v10_caps(salt, thread_id, false)
}

/// ⭐ F83: CLIENT_SSL capability bit.
pub const CLIENT_SSL: u32 = 0x0000_0800;

/// HandshakeV10, 可选宣告 CLIENT_SSL (tls=true 时客户端可发 SSLRequest 升级).
pub fn build_handshake_v10_caps(salt: &[u8; 20], thread_id: u32, tls: bool) -> Vec<u8> {
    let caps = if tls { SERVER_CAPS | CLIENT_SSL } else { SERVER_CAPS };
    let mut p = Vec::with_capacity(96);
    p.push(10); // protocol version
    p.extend_from_slice(b"8.0.0-NexusDB\0");
    p.extend_from_slice(&thread_id.to_le_bytes());
    p.extend_from_slice(&salt[..8]); // auth-plugin-data part 1
    p.push(0); // filler
    p.extend_from_slice(&(caps as u16).to_le_bytes()); // caps lower
    p.push(CHARSET_UTF8MB4);
    p.extend_from_slice(&STATUS_AUTOCOMMIT.to_le_bytes());
    p.extend_from_slice(&((caps >> 16) as u16).to_le_bytes()); // caps upper
    p.push(21); // auth plugin data total len (20 + NUL)
    p.extend_from_slice(&[0u8; 10]); // reserved
    p.extend_from_slice(&salt[8..20]); // part 2 (12B)
    p.push(0); // part 2 NUL (凑 13B)
    p.extend_from_slice(AUTH_PLUGIN);
    p.push(0);
    write_packet(0, &p)
}

/// 解析后的登录请求.
#[derive(Debug, PartialEq)]
pub struct LoginRequest {
    pub username: String,
    pub auth_resp: Vec<u8>,
    pub database: Option<String>,
    /// 客户端声明的 auth 插件 (None = 未带 PLUGIN_AUTH).
    pub plugin: Option<String>,
}

fn read_nul_str(buf: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    let start = *pos;
    let nul = buf[start..].iter().position(|&b| b == 0)?;
    *pos = start + nul + 1;
    Some(buf[start..start + nul].to_vec())
}

/// 解析 HandshakeResponse41 (按客户端 capability 逐段).
pub fn parse_handshake_response(payload: &[u8]) -> Result<LoginRequest, String> {
    if payload.len() < 32 {
        return Err("handshake response too short".into());
    }
    let flags = u32::from_le_bytes(payload[0..4].try_into().expect("4B"));
    if flags & CLIENT_PROTOCOL_41 == 0 {
        return Err("client does not speak protocol 41".into());
    }
    let mut pos = 4 + 4 + 1 + 23; // flags + max_packet + charset + filler
    let username = read_nul_str(payload, &mut pos).ok_or("bad username")?;
    let auth_resp: Vec<u8> = if flags & CLIENT_PLUGIN_AUTH_LENENC_DATA != 0 {
        // lenenc bytes (客户端 auth 数据 < 251B, 单字节长度即够)
        let n = *payload.get(pos).ok_or("bad auth len")? as usize;
        pos += 1;
        if n >= 251 {
            return Err("unsupported lenenc auth length".into());
        }
        payload.get(pos..pos + n).ok_or("bad auth data")?.to_vec()
    } else if flags & CLIENT_SECURE_CONNECTION != 0 {
        let n = *payload.get(pos).ok_or("bad auth len")? as usize;
        pos += 1;
        payload.get(pos..pos + n).ok_or("bad auth data")?.to_vec()
    } else {
        read_nul_str(payload, &mut pos).ok_or("bad auth data")?
    };
    // lenenc/secure 分支只推进了长度字节, 数据段在此统一推进
    if flags & (CLIENT_PLUGIN_AUTH_LENENC_DATA | CLIENT_SECURE_CONNECTION) != 0 {
        pos += auth_resp.len();
    }
    let database = if flags & CLIENT_CONNECT_WITH_DB != 0 && pos < payload.len() {
        read_nul_str(payload, &mut pos)
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .filter(|s| !s.is_empty())
    } else {
        None
    };
    let plugin = if flags & CLIENT_PLUGIN_AUTH != 0 && pos < payload.len() {
        read_nul_str(payload, &mut pos).map(|b| String::from_utf8_lossy(&b).into_owned())
    } else {
        None
    };
    Ok(LoginRequest {
        username: String::from_utf8_lossy(&username).into_owned(),
        auth_resp,
        database,
        plugin,
    })
}

/// AuthSwitchRequest: 客户端默认 caching_sha2 时切到 native (0xFE + 插件名 + salt).
pub fn build_auth_switch(seq: u8, salt: &[u8; 20]) -> Vec<u8> {
    let mut p = Vec::with_capacity(44);
    p.push(0xFE);
    p.extend_from_slice(AUTH_PLUGIN);
    p.push(0);
    p.extend_from_slice(salt);
    p.push(0);
    write_packet(seq, &p)
}

// =====================================================================
// OK / ERR / EOF / 结果集
// =====================================================================

pub fn build_ok(seq: u8, affected: u64) -> Vec<u8> {
    let mut p = Vec::with_capacity(8);
    p.push(0x00);
    lenenc_int(&mut p, affected);
    lenenc_int(&mut p, 0); // last_insert_id
    p.extend_from_slice(&STATUS_AUTOCOMMIT.to_le_bytes());
    p.extend_from_slice(&0u16.to_le_bytes()); // warnings
    write_packet(seq, &p)
}

pub fn build_err(seq: u8, code: u16, msg: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(16 + msg.len());
    p.push(0xFF);
    p.extend_from_slice(&code.to_le_bytes());
    p.push(b'#');
    // ⭐ F65: 按 error code 映射 SQLSTATE (ORM 据此分类异常:
    // 23000 → IntegrityError; 40001 → 锁/序列化重试; 42S22 → 列不存在等).
    let sqlstate: &[u8; 5] = match code {
        1062 => b"23000", // ER_DUP_ENTRY → 完整性约束
        1052 | 1054 => b"42S22",
        1049 => b"42000",
        1064 => b"42000",
        1045 => b"28000",
        1213 => b"40001", // 死锁/序列化失败 → 可重试
        1792 => b"25006", // 只读事务
        _ => b"HY000",
    };
    p.extend_from_slice(sqlstate);
    p.extend_from_slice(msg.as_bytes());
    write_packet(seq, &p)
}

pub fn build_eof(seq: u8) -> Vec<u8> {
    let mut p = Vec::with_capacity(5);
    p.push(0xFE);
    p.extend_from_slice(&0u16.to_le_bytes()); // warnings
    p.extend_from_slice(&STATUS_AUTOCOMMIT.to_le_bytes());
    write_packet(seq, &p)
}

fn mysql_type(ty: ColType) -> u8 {
    match ty {
        ColType::I64 => 8,     // LONGLONG
        ColType::F64 => 5,     // DOUBLE
        ColType::Str => 253,   // VAR_STRING
        ColType::Bytes => 252, // BLOB
        ColType::Bool => 1,    // TINY (tinyint(1))
        ColType::Date => 10,   // DATE
        ColType::Time => 11,   // TIME
        ColType::Timestamp => 12, // DATETIME
        ColType::Json => 245,  // JSON
        ColType::Uuid => 253,  // VAR_STRING (char(36))
        ColType::Decimal { .. } => 246, // NEWDECIMAL
    }
}

/// 文本协议结果集 (老式 EOF 格式):
/// 列数包 + 列定义×M + EOF + 行×N + EOF; seq 从 `seq_start` 连续递增.
pub fn build_result_set(
    seq_start: u8,
    cols: &[(&str, ColType)],
    rows: &[Vec<ColValue>],
) -> Vec<u8> {
    let mut out = Vec::new();
    let mut seq = seq_start;
    let push = |out: &mut Vec<u8>, seq: &mut u8, payload: &[u8]| {
        out.extend_from_slice(&write_packet(*seq, payload));
        *seq = seq.wrapping_add(1);
    };
    // 列数
    let mut p = Vec::new();
    lenenc_int(&mut p, cols.len() as u64);
    push(&mut out, &mut seq, &p);
    // 列定义 41
    for (name, ty) in cols {
        let mut p = Vec::with_capacity(48);
        lenenc_bytes(&mut p, b"def");
        lenenc_bytes(&mut p, b""); // schema
        lenenc_bytes(&mut p, b""); // table
        lenenc_bytes(&mut p, b""); // org_table
        lenenc_bytes(&mut p, name.as_bytes());
        lenenc_bytes(&mut p, name.as_bytes()); // org_name
        p.push(0x0C); // fixed fields len
        p.extend_from_slice(&(CHARSET_UTF8MB4 as u16).to_le_bytes());
        p.extend_from_slice(&1024u32.to_le_bytes()); // column_length
        p.push(mysql_type(*ty));
        p.extend_from_slice(&0u16.to_le_bytes()); // flags
        p.push(0); // decimals
        p.extend_from_slice(&0u16.to_le_bytes()); // filler
        push(&mut out, &mut seq, &p);
    }
    push(&mut out, &mut seq, &build_eof_payload());
    // 文本行
    for r in rows {
        let mut p = Vec::new();
        for (ci, v) in r.iter().enumerate() {
            let ty = cols.get(ci).map(|(_, t)| *t).unwrap_or(ColType::Str);
            match v {
                ColValue::Null => p.push(0xFB),
                // ⭐ F80: 按列 ColType 渲染 (Bool→'1'/'0', Date/Time/Timestamp→格式化)
                ColValue::I64(x) => {
                    let s = match ty {
                        ColType::Bool => (if *x != 0 { "1" } else { "0" }).to_string(),
                        ColType::Date => crate::worker::render_date(*x),
                        ColType::Time => crate::worker::render_time(*x),
                        ColType::Timestamp => crate::worker::render_timestamp(*x),
                        _ => x.to_string(),
                    };
                    lenenc_bytes(&mut p, s.as_bytes());
                }
                ColValue::F64(f) => lenenc_bytes(&mut p, format!("{f}").as_bytes()),
                ColValue::Bytes(b) => match ty {
                    ColType::Uuid => lenenc_bytes(&mut p, crate::worker::render_uuid(b).as_bytes()),
                    _ => lenenc_bytes(&mut p, b),
                },
                // ⭐ F81: Decimal → 定点文本 (NEWDECIMAL 文本协议)
                ColValue::Decimal(x, scale) => {
                    lenenc_bytes(&mut p, crate::worker::render_decimal(*x, *scale).as_bytes())
                }
            }
        }
        push(&mut out, &mut seq, &p);
    }
    push(&mut out, &mut seq, &build_eof_payload());
    out
}

fn build_eof_payload() -> Vec<u8> {
    let mut p = Vec::with_capacity(5);
    p.push(0xFE);
    p.extend_from_slice(&0u16.to_le_bytes());
    p.extend_from_slice(&STATUS_AUTOCOMMIT.to_le_bytes());
    p
}

// ===== 命令字节 =====
pub const COM_QUIT: u8 = 0x01;
pub const COM_INIT_DB: u8 = 0x02;
pub const COM_QUERY: u8 = 0x03;
pub const COM_PING: u8 = 0x0E;
// ⭐ P2: 预处理语句命令族
pub const COM_STMT_PREPARE: u8 = 0x16;
pub const COM_STMT_EXECUTE: u8 = 0x17;
pub const COM_STMT_CLOSE: u8 = 0x19;
pub const COM_STMT_RESET: u8 = 0x1A;

// =====================================================================
// ⭐ P2: COM_STMT_* — prepare 响应 / execute 二进制参数 / 二进制结果集
// =====================================================================

/// PREPARE_OK: [00][stmt_id][num_columns=0][num_params][filler][warnings]
/// + 参数定义包 × n + EOF. num_columns=0 = 列定义延迟到 execute 结果集自描述
///   (免 prepare 期 schema 依赖; mysql2/Go driver 兼容).
pub fn build_stmt_prepare_ok(stmt_id: u32, num_params: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    let mut seq = 1u8;
    let mut p = Vec::with_capacity(12);
    p.push(0x00);
    p.extend_from_slice(&stmt_id.to_le_bytes());
    p.extend_from_slice(&0u16.to_le_bytes()); // num_columns = 0
    p.extend_from_slice(&num_params.to_le_bytes());
    p.push(0x00); // filler
    p.extend_from_slice(&0u16.to_le_bytes()); // warnings
    out.extend_from_slice(&write_packet(seq, &p));
    seq = seq.wrapping_add(1);
    if num_params > 0 {
        // 参数定义 (占位名 "?", 类型 VAR_STRING — 客户端不依赖此处类型)
        for _ in 0..num_params {
            let mut c = Vec::with_capacity(32);
            lenenc_bytes(&mut c, b"def");
            lenenc_bytes(&mut c, b"");
            lenenc_bytes(&mut c, b"");
            lenenc_bytes(&mut c, b"");
            lenenc_bytes(&mut c, b"?");
            lenenc_bytes(&mut c, b"?");
            c.push(0x0C);
            c.extend_from_slice(&(CHARSET_UTF8MB4 as u16).to_le_bytes());
            c.extend_from_slice(&0u32.to_le_bytes());
            c.push(253); // VAR_STRING
            c.extend_from_slice(&0u16.to_le_bytes());
            c.push(0);
            c.extend_from_slice(&0u16.to_le_bytes());
            out.extend_from_slice(&write_packet(seq, &c));
            seq = seq.wrapping_add(1);
        }
        out.extend_from_slice(&write_packet(seq, &build_eof_payload()));
    }
    out
}

fn read_lenenc(buf: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    let first = *buf.get(*pos)?;
    *pos += 1;
    let len = match first {
        0..=0xFA => first as usize,
        0xFC => {
            let l = u16::from_le_bytes([*buf.get(*pos)?, *buf.get(*pos + 1)?]) as usize;
            *pos += 2;
            l
        }
        0xFD => {
            let l = u32::from_le_bytes([
                *buf.get(*pos)?,
                *buf.get(*pos + 1)?,
                *buf.get(*pos + 2)?,
                0,
            ]) as usize;
            *pos += 3;
            l
        }
        _ => return None, // 0xFB NULL / 0xFE 8B 长度不在参数场景
    };
    let out = buf.get(*pos..*pos + len)?.to_vec();
    *pos += len;
    Some(out)
}

/// COM_STMT_EXECUTE 参数解码 (payload 含 cmd 字节).
/// `cached_types`: new_params_bound=0 时复用上次类型 (协议允许省略).
/// 返回 (stmt_id, 参数值). 不支持的类型报错.
pub fn parse_stmt_execute(
    payload: &[u8],
    num_params: u16,
    cached_types: &mut Option<Vec<(u8, u8)>>,
) -> Result<(u32, Vec<crate::protocol::sql::SqlValue>), String> {
    use crate::protocol::sql::SqlValue;
    if payload.len() < 10 {
        return Err("short STMT_EXECUTE".into());
    }
    let stmt_id = u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
    // flags u8 + iteration u32 跳过
    let mut pos = 10usize;
    let n = num_params as usize;
    if n == 0 {
        return Ok((stmt_id, Vec::new()));
    }
    let bitmap_len = n.div_ceil(8);
    let bitmap = payload
        .get(pos..pos + bitmap_len)
        .ok_or("short NULL bitmap")?
        .to_vec();
    pos += bitmap_len;
    let new_bound = *payload.get(pos).ok_or("missing new_params_bound")?;
    pos += 1;
    let types: Vec<(u8, u8)> = if new_bound == 1 {
        let mut t = Vec::with_capacity(n);
        for _ in 0..n {
            let ty = *payload.get(pos).ok_or("short types")?;
            let flag = *payload.get(pos + 1).ok_or("short types")?;
            t.push((ty, flag));
            pos += 2;
        }
        *cached_types = Some(t.clone());
        t
    } else {
        cached_types.clone().ok_or("no cached parameter types")?
    };
    let mut vals = Vec::with_capacity(n);
    for (i, &(ty, _flag)) in types.iter().enumerate() {
        if bitmap[i / 8] & (1 << (i % 8)) != 0 {
            vals.push(SqlValue::Null);
            continue;
        }
        let take = |pos: &mut usize, k: usize| -> Result<&[u8], String> {
            let s = payload.get(*pos..*pos + k).ok_or("short param value")?;
            *pos += k;
            Ok(s)
        };
        let v = match ty {
            0x01 => SqlValue::Int(take(&mut pos, 1)?[0] as i8 as i64), // TINY
            0x02 | 0x0D => {
                // SHORT / YEAR
                let b = take(&mut pos, 2)?;
                SqlValue::Int(i16::from_le_bytes([b[0], b[1]]) as i64)
            }
            0x03 | 0x09 => {
                // LONG / INT24
                let b = take(&mut pos, 4)?;
                SqlValue::Int(i32::from_le_bytes([b[0], b[1], b[2], b[3]]) as i64)
            }
            0x08 => {
                // LONGLONG
                let b = take(&mut pos, 8)?;
                SqlValue::Int(i64::from_le_bytes(b.try_into().unwrap()))
            }
            0x04 => {
                // FLOAT → f64 提升
                let b = take(&mut pos, 4)?;
                SqlValue::Float(f32::from_le_bytes(b.try_into().unwrap()) as f64)
            }
            0x05 => {
                // DOUBLE
                let b = take(&mut pos, 8)?;
                SqlValue::Float(f64::from_le_bytes(b.try_into().unwrap()))
            }
            0x06 => SqlValue::Null, // NULL 类型 (bitmap 外冗余)
            // DECIMAL/NEWDECIMAL/VARCHAR/BLOB 族/VAR_STRING/STRING: lenenc 字节
            0x00 | 0xF6 | 0x0F | 0xF9..=0xFC | 0xFD | 0xFE => {
                let b = read_lenenc(payload, &mut pos).ok_or("bad lenenc param")?;
                SqlValue::Str(b)
            }
            other => return Err(format!("unsupported parameter type 0x{other:02x}")),
        };
        vals.push(v);
    }
    Ok((stmt_id, vals))
}

/// ⭐ P2: 二进制协议结果集 (COM_STMT_EXECUTE 响应).
/// binary row = [0x00 头][NULL bitmap (n+2+7)/8, 位偏移 +2][各列二进制值].
pub fn build_binary_result_set(
    cols: &[(&str, ColType)],
    rows: &[Vec<ColValue>],
) -> Vec<u8> {
    let mut out = Vec::new();
    let mut seq = 1u8;
    let push = |out: &mut Vec<u8>, seq: &mut u8, payload: &[u8]| {
        out.extend_from_slice(&write_packet(*seq, payload));
        *seq = seq.wrapping_add(1);
    };
    let mut p = Vec::new();
    lenenc_int(&mut p, cols.len() as u64);
    push(&mut out, &mut seq, &p);
    for (name, ty) in cols {
        let mut c = Vec::with_capacity(48);
        lenenc_bytes(&mut c, b"def");
        lenenc_bytes(&mut c, b"");
        lenenc_bytes(&mut c, b"");
        lenenc_bytes(&mut c, b"");
        lenenc_bytes(&mut c, name.as_bytes());
        lenenc_bytes(&mut c, name.as_bytes());
        c.push(0x0C);
        c.extend_from_slice(&(CHARSET_UTF8MB4 as u16).to_le_bytes());
        c.extend_from_slice(&1024u32.to_le_bytes());
        c.push(mysql_type(*ty));
        c.extend_from_slice(&0u16.to_le_bytes());
        c.push(0);
        c.extend_from_slice(&0u16.to_le_bytes());
        push(&mut out, &mut seq, &c);
    }
    push(&mut out, &mut seq, &build_eof_payload());
    let bitmap_len = (cols.len() + 7 + 2) / 8;
    for r in rows {
        let mut p = vec![0x00u8];
        let bitmap_at = p.len();
        p.extend(std::iter::repeat_n(0u8, bitmap_len));
        for (i, v) in r.iter().enumerate() {
            let ty = cols.get(i).map(|(_, t)| *t).unwrap_or(ColType::Str);
            match v {
                ColValue::Null => {
                    let bit = i + 2;
                    p[bitmap_at + bit / 8] |= 1 << (bit % 8);
                }
                // ⭐ F80: 按列 ColType 走 MySQL 二进制编码 (DATE/TIME/DATETIME 特殊格式)
                ColValue::I64(x) => match ty {
                    ColType::Bool => p.push(if *x != 0 { 1 } else { 0 }), // TINY 1B
                    ColType::Date => encode_bin_date(&mut p, *x),
                    ColType::Time => encode_bin_time(&mut p, *x),
                    ColType::Timestamp => encode_bin_datetime(&mut p, *x),
                    _ => p.extend_from_slice(&x.to_le_bytes()),
                },
                ColValue::F64(x) => p.extend_from_slice(&x.to_le_bytes()),
                ColValue::Bytes(b) => match ty {
                    ColType::Uuid => lenenc_bytes(&mut p, crate::worker::render_uuid(b).as_bytes()),
                    _ => lenenc_bytes(&mut p, b),
                },
                // ⭐ F81: NEWDECIMAL 二进制 = lenenc 定点文本
                ColValue::Decimal(x, scale) => {
                    lenenc_bytes(&mut p, crate::worker::render_decimal(*x, *scale).as_bytes())
                }
            }
        }
        push(&mut out, &mut seq, &p);
    }
    push(&mut out, &mut seq, &build_eof_payload());
    out
}

/// ⭐ F80: MySQL 二进制 DATE 编码 (length + year u16 + month + day). 微秒截去.
fn encode_bin_date(p: &mut Vec<u8>, micros: i64) {
    let (y, m, d, _, _, _, _) = crate::worker::datetime_parts(micros);
    p.push(4);
    p.extend_from_slice(&y.to_le_bytes());
    p.push(m);
    p.push(d);
}

/// ⭐ F80: MySQL 二进制 DATETIME 编码 (length 7 无微秒 / 11 含微秒).
fn encode_bin_datetime(p: &mut Vec<u8>, micros: i64) {
    let (y, mo, d, h, mi, s, us) = crate::worker::datetime_parts(micros);
    if us == 0 {
        p.push(7);
        p.extend_from_slice(&y.to_le_bytes());
        p.extend_from_slice(&[mo, d, h, mi, s]);
    } else {
        p.push(11);
        p.extend_from_slice(&y.to_le_bytes());
        p.extend_from_slice(&[mo, d, h, mi, s]);
        p.extend_from_slice(&us.to_le_bytes());
    }
}

/// ⭐ F80: MySQL 二进制 TIME 编码 (length + is_neg + days u32 + h + m + s [+ micro u32]).
fn encode_bin_time(p: &mut Vec<u8>, micros: i64) {
    let (h, mi, s, us) = crate::worker::time_parts(micros);
    if us == 0 {
        p.push(8);
        p.push(0); // is_negative
        p.extend_from_slice(&0u32.to_le_bytes()); // days
        p.extend_from_slice(&[h, mi, s]);
    } else {
        p.push(12);
        p.push(0);
        p.extend_from_slice(&0u32.to_le_bytes());
        p.extend_from_slice(&[h, mi, s]);
        p.extend_from_slice(&us.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_rfc_vectors() {
        let hex = |b: [u8; 20]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
        assert_eq!(hex(sha1(b"abc")), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(hex(sha1(b"")), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(
            hex(sha1(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
        );
    }

    #[test]
    fn native_password_roundtrip() {
        let salt = [7u8; 20];
        let tok = native_password_token(&salt, "s3cret");
        assert!(native_password_ok(&salt, &tok, "s3cret"));
        assert!(!native_password_ok(&salt, &tok, "wrong"));
        assert!(!native_password_ok(&salt, &[0u8; 20], "s3cret"));
        // 空密码 ⇔ 空 resp
        assert!(native_password_ok(&salt, &[], ""));
        assert!(!native_password_ok(&salt, &tok, ""));
        assert!(!native_password_ok(&salt, &[], "s3cret"));
    }

    #[test]
    fn packet_framing_half_and_sticky() {
        let a = write_packet(0, b"hello");
        let b = write_packet(1, b"world!");
        let mut buf = a.clone();
        buf.extend_from_slice(&b);
        // 半包
        assert!(read_packet(&buf[..3]).is_none());
        assert!(read_packet(&buf[..a.len() - 1]).is_none());
        // 粘包逐帧
        let (seq, n, p) = read_packet(&buf).unwrap();
        assert_eq!((seq, p.as_slice()), (0, &b"hello"[..]));
        let (seq2, _, p2) = read_packet(&buf[n..]).unwrap();
        assert_eq!((seq2, p2.as_slice()), (1, &b"world!"[..]));
    }

    #[test]
    fn handshake_roundtrip() {
        let salt = [9u8; 20];
        let hs = build_handshake_v10(&salt, 42);
        let (seq, _, p) = read_packet(&hs).unwrap();
        assert_eq!(seq, 0);
        assert_eq!(p[0], 10);
        // 构造一个 HandshakeResponse41 再解析
        let mut resp = Vec::new();
        let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_CONNECT_WITH_DB;
        resp.extend_from_slice(&flags.to_le_bytes());
        resp.extend_from_slice(&0x0100_0000u32.to_le_bytes());
        resp.push(45);
        resp.extend_from_slice(&[0u8; 23]);
        resp.extend_from_slice(b"root\0");
        let tok = native_password_token(&salt, "pw");
        resp.push(tok.len() as u8);
        resp.extend_from_slice(&tok);
        resp.extend_from_slice(b"appdb\0");
        resp.extend_from_slice(b"mysql_native_password\0");
        let login = parse_handshake_response(&resp).unwrap();
        assert_eq!(login.username, "root");
        assert_eq!(login.database.as_deref(), Some("appdb"));
        assert_eq!(login.plugin.as_deref(), Some("mysql_native_password"));
        assert!(native_password_ok(&salt, &login.auth_resp, "pw"));
    }

    #[test]
    fn lenenc_boundaries() {
        let cases: [(u64, usize); 5] = [(250, 1), (251, 3), (0xFFFF, 3), (0x1_0000, 4), (0x100_0000, 9)];
        for (v, want_len) in cases {
            let mut b = Vec::new();
            lenenc_int(&mut b, v);
            assert_eq!(b.len(), want_len, "lenenc({v})");
        }
    }

    #[test]
    fn result_set_structure() {
        let rows = vec![
            vec![ColValue::I64(1), ColValue::Bytes(b"a".to_vec()), ColValue::Null],
            vec![ColValue::I64(2), ColValue::Bytes(b"b".to_vec()), ColValue::F64(1.5)],
        ];
        let rs = build_result_set(1, &[("id", ColType::I64), ("n", ColType::Str), ("s", ColType::F64)], &rows);
        // 逐帧: 列数(1) + 列定义(3) + EOF + 行(2) + EOF = 8 帧, seq 1..=8
        let mut pos = 0;
        let mut seqs = Vec::new();
        let mut frames = Vec::new();
        while pos < rs.len() {
            let (seq, n, p) = read_packet(&rs[pos..]).unwrap();
            seqs.push(seq);
            frames.push(p);
            pos += n;
        }
        assert_eq!(seqs, (1u8..=8).collect::<Vec<_>>());
        assert_eq!(frames[0], vec![3u8]); // 列数
        assert_eq!(frames[4][0], 0xFE); // 首 EOF
        assert_eq!(frames[7][0], 0xFE); // 尾 EOF
        // 行 1: "1", "a", NULL
        assert_eq!(frames[5], vec![1, b'1', 1, b'a', 0xFB]);
    }

    #[test]
    fn ok_err_eof_shape() {
        let (_, _, ok) = read_packet(&build_ok(1, 1)).unwrap();
        assert_eq!(ok[0], 0x00);
        let (_, _, err) = read_packet(&build_err(1, 1064, "syntax")).unwrap();
        assert_eq!(err[0], 0xFF);
        assert_eq!(u16::from_le_bytes([err[1], err[2]]), 1064);
        assert_eq!(&err[9..], b"syntax");
        let (_, _, eof) = read_packet(&build_eof(3)).unwrap();
        assert_eq!(eof[0], 0xFE);
        // auth switch
        let (_, _, sw) = read_packet(&build_auth_switch(2, &[1u8; 20])).unwrap();
        assert_eq!(sw[0], 0xFE);
        assert!(sw[1..].starts_with(b"mysql_native_password\0"));
    }
}
