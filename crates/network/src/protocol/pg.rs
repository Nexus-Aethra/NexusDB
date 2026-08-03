//! ⭐ S4: PostgreSQL wire protocol v3 门面 (最小子集, 零依赖).
//!
//! 支持: SSLRequest 拒绝 ('N') / StartupMessage (user, database) /
//! cleartext password 认证 / simple Query ('Q') / Terminate ('X').
//! 结果集: RowDescription('T') + DataRow('D') 文本格式 + CommandComplete('C').
//! 错误: ErrorResponse('E', S/C/M 字段) — 命令阶段错误不断连 (跟 ReadyForQuery).
//!
//! 明确不做 (文档记录): 扩展查询协议 (Parse/Bind/Execute)、COPY、TLS、
//! SCRAM/md5 (仅 cleartext — 内网/开发用, 生产需 TLS 前提)、NOTIFY、游标.

use storage::row::ColValue;
use storage::schema::ColType;

/// SSLRequest magic (len=8 的特殊 startup).
pub const SSL_REQUEST_CODE: u32 = 80877103;
/// GSSENCRequest magic (同样拒绝).
pub const GSSENC_REQUEST_CODE: u32 = 80877104;
/// CancelRequest magic (忽略断连).
pub const CANCEL_REQUEST_CODE: u32 = 80877102;
/// 协议版本 3.0.
pub const PROTOCOL_V3: u32 = 196608;

// ---- 类型 OID (RowDescription 用) ----
pub const OID_INT8: u32 = 20;
pub const OID_FLOAT8: u32 = 701;
pub const OID_TEXT: u32 = 25;
pub const OID_BYTEA: u32 = 17;
// ⭐ F80
pub const OID_BOOL: u32 = 16;
pub const OID_DATE: u32 = 1082;
pub const OID_TIME: u32 = 1083;
pub const OID_TIMESTAMP: u32 = 1114;
pub const OID_JSON: u32 = 114;
pub const OID_UUID: u32 = 2950;
pub const OID_NUMERIC: u32 = 1700;

/// startup 阶段帧: `[len u32 BE 含自身][payload]` (无 type 字节).
/// 返回 (消耗字节, payload).
pub fn read_startup_frame(buf: &[u8]) -> Option<(usize, &[u8])> {
    if buf.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if !(4..=1 << 20).contains(&len) || buf.len() < len {
        return None;
    }
    Some((len, &buf[4..len]))
}

/// 命令阶段帧: `[type u8][len u32 BE 含 len 不含 type][payload]`.
/// 返回 (消耗字节, type, payload).
pub fn read_frame(buf: &[u8]) -> Option<(usize, u8, &[u8])> {
    if buf.len() < 5 {
        return None;
    }
    let ty = buf[0];
    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    if !(4..=1 << 26).contains(&len) || buf.len() < 1 + len {
        return None;
    }
    Some((1 + len, ty, &buf[5..1 + len]))
}

fn frame(ty: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(ty);
    out.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// StartupMessage 参数解析 → (user, database).
pub fn parse_startup(payload: &[u8]) -> Result<(String, Option<String>), String> {
    if payload.len() < 4 {
        return Err("short startup".into());
    }
    let ver = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    if ver != PROTOCOL_V3 {
        return Err(format!("unsupported protocol {ver}"));
    }
    let mut user = String::new();
    let mut database = None;
    let mut it = payload[4..].split(|&b| b == 0);
    while let (Some(k), Some(v)) = (it.next(), it.next()) {
        if k.is_empty() {
            break;
        }
        match k {
            b"user" => user = String::from_utf8_lossy(v).into_owned(),
            b"database" => database = Some(String::from_utf8_lossy(v).into_owned()),
            _ => {} // client_encoding / application_name 等忽略
        }
    }
    Ok((user, database))
}

/// PasswordMessage payload → 密码 (尾 NUL 剥除).
pub fn parse_password(payload: &[u8]) -> String {
    let end = payload.iter().position(|&b| b == 0).unwrap_or(payload.len());
    String::from_utf8_lossy(&payload[..end]).into_owned()
}

/// AuthenticationCleartextPassword.
pub fn build_auth_cleartext() -> Vec<u8> {
    frame(b'R', &3u32.to_be_bytes())
}

// ===== ⭐ F82: SCRAM-SHA-256 (RFC 5802 / 7677) =====

/// AuthenticationSASL (code 10): 宣告支持的机制列表 (仅 SCRAM-SHA-256).
pub fn build_auth_sasl() -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&10u32.to_be_bytes());
    p.extend_from_slice(b"SCRAM-SHA-256");
    p.push(0); // 机制名以 NUL 分隔
    p.push(0); // 列表终止空串
    frame(b'R', &p)
}

/// AuthenticationSASLContinue (code 11): server-first-message.
pub fn build_auth_sasl_continue(server_first: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&11u32.to_be_bytes());
    p.extend_from_slice(server_first);
    frame(b'R', &p)
}

/// AuthenticationSASLFinal (code 12): "v=base64(ServerSignature)".
pub fn build_auth_sasl_final(server_final: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&12u32.to_be_bytes());
    p.extend_from_slice(server_final);
    frame(b'R', &p)
}

/// 解析 SASLInitialResponse payload (客户端 'p' 帧首条):
/// `[mechanism NUL][client-first len u32 BE][client-first-message]` → (mechanism, client_first).
pub fn parse_sasl_initial(payload: &[u8]) -> Option<(String, Vec<u8>)> {
    let nul = payload.iter().position(|&b| b == 0)?;
    let mech = String::from_utf8_lossy(&payload[..nul]).into_owned();
    let rest = &payload[nul + 1..];
    if rest.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
    let body = rest.get(4..4 + len)?;
    Some((mech, body.to_vec()))
}

/// SCRAM 服务端会话状态 (跨两条客户端消息).
#[derive(Debug, Clone)]
pub struct ScramState {
    pub client_first_bare: Vec<u8>, // "n=user,r=cnonce"
    pub server_first: Vec<u8>,      // "r=nonce,s=salt,i=iter"
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub nonce: Vec<u8>, // 完整 r= (client+server)
}

/// SCRAM 步骤 1: 处理 client-first-message → (state, server-first-message).
/// client_first 形如 `n,,n=user,r=cnonce` (gs2 头 `n,,` = 无 channel binding).
pub fn scram_server_first(client_first: &[u8]) -> Option<(ScramState, Vec<u8>)> {
    let s = std::str::from_utf8(client_first).ok()?;
    // 剥 gs2 头: 支持 "n,,"/"y,," (无 CB); "p=" (要求 CB) v1 不支持
    let bare = s
        .strip_prefix("n,,")
        .or_else(|| s.strip_prefix("y,,"))?;
    // 取 client nonce (r=...)
    let cnonce = bare.split(',').find_map(|kv| kv.strip_prefix("r="))?;
    let snonce = String::from_utf8(crate::protocol::crypto::rand_printable(18)).ok()?;
    let full_nonce = format!("{cnonce}{snonce}");
    let salt = crate::protocol::crypto::rand_bytes(16);
    let iterations = 4096u32;
    let server_first = format!(
        "r={full_nonce},s={},i={iterations}",
        crate::protocol::crypto::base64_encode(&salt)
    );
    let state = ScramState {
        client_first_bare: bare.as_bytes().to_vec(),
        server_first: server_first.clone().into_bytes(),
        salt,
        iterations,
        nonce: full_nonce.into_bytes(),
    };
    Some((state, server_first.into_bytes()))
}

/// SCRAM 步骤 2: 验证 client-final-message, 返回 server-final ("v=...") 或 None (认证失败).
/// client_final 形如 `c=biws,r=fullnonce,p=base64(proof)`.
pub fn scram_verify_final(state: &ScramState, client_final: &[u8], password: &str) -> Option<Vec<u8>> {
    use crate::protocol::crypto::{base64_decode, base64_encode, hmac_sha256, pbkdf2_sha256_32, sha256};
    let s = std::str::from_utf8(client_final).ok()?;
    // 校验 nonce 一致
    let recv_nonce = s.split(',').find_map(|kv| kv.strip_prefix("r="))?;
    if recv_nonce.as_bytes() != state.nonce.as_slice() {
        return None;
    }
    let proof_b64 = s.split(',').find_map(|kv| kv.strip_prefix("p="))?;
    let proof = base64_decode(proof_b64.as_bytes())?;
    if proof.len() != 32 {
        return None;
    }
    // client-final-without-proof = "c=biws,r=nonce"
    let cfwp = {
        let end = s.rfind(",p=")?;
        &s[..end]
    };
    // AuthMessage = client-first-bare + "," + server-first + "," + client-final-without-proof
    let mut auth_msg = state.client_first_bare.clone();
    auth_msg.push(b',');
    auth_msg.extend_from_slice(&state.server_first);
    auth_msg.push(b',');
    auth_msg.extend_from_slice(cfwp.as_bytes());

    let salted = pbkdf2_sha256_32(password.as_bytes(), &state.salt, state.iterations);
    let client_key = hmac_sha256(&salted, b"Client Key");
    let stored_key = sha256(&client_key);
    let client_sig = hmac_sha256(&stored_key, &auth_msg);
    // 恢复 ClientKey = proof XOR ClientSignature, 校验 SHA256 == StoredKey
    let mut recovered = [0u8; 32];
    for i in 0..32 {
        recovered[i] = proof[i] ^ client_sig[i];
    }
    if sha256(&recovered) != stored_key {
        return None; // 密码错误
    }
    let server_key = hmac_sha256(&salted, b"Server Key");
    let server_sig = hmac_sha256(&server_key, &auth_msg);
    Some(format!("v={}", base64_encode(&server_sig)).into_bytes())
}

/// AuthenticationOk + ParameterStatus × n + BackendKeyData + ReadyForQuery.
pub fn build_auth_ok_bundle(backend_pid: u32) -> Vec<u8> {
    let mut out = frame(b'R', &0u32.to_be_bytes());
    for (k, v) in [
        ("server_version", "16.0 (NexusDB)"),
        ("client_encoding", "UTF8"),
        ("server_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
    ] {
        let mut p = Vec::with_capacity(k.len() + v.len() + 2);
        p.extend_from_slice(k.as_bytes());
        p.push(0);
        p.extend_from_slice(v.as_bytes());
        p.push(0);
        out.extend_from_slice(&frame(b'S', &p));
    }
    let mut key = Vec::with_capacity(8);
    key.extend_from_slice(&backend_pid.to_be_bytes());
    key.extend_from_slice(&0x6e78_6462u32.to_be_bytes()); // "nxdb"
    out.extend_from_slice(&frame(b'K', &key));
    out.extend_from_slice(&build_ready());
    out
}

/// ReadyForQuery (idle).
pub fn build_ready() -> Vec<u8> {
    frame(b'Z', b"I")
}

/// ErrorResponse (S=severity, C=SQLSTATE, M=message).
pub fn build_error(code: &str, msg: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(msg.len() + 24);
    for (tag, val) in [(b'S', "ERROR"), (b'V', "ERROR"), (b'C', code), (b'M', msg)] {
        p.push(tag);
        p.extend_from_slice(val.as_bytes());
        p.push(0);
    }
    p.push(0);
    frame(b'E', &p)
}

/// CommandComplete.
pub fn build_command_complete(tag: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(tag.len() + 1);
    p.extend_from_slice(tag.as_bytes());
    p.push(0);
    frame(b'C', &p)
}

/// ⭐ PG 兼容 (multi-statement): 多语句顺序执行完成后, 回 CommandComplete +
/// ReadyForQuery (PG 协议要求每条 simple query 以 ReadyForQuery 收尾).
pub fn build_command_complete_multi() -> Vec<u8> {
    let mut out = build_command_complete("SELECT 1");
    out.extend_from_slice(&build_ready());
    out
}

fn type_oid(ty: ColType) -> u32 {
    match ty {
        ColType::I64 => OID_INT8,
        ColType::F64 => OID_FLOAT8,
        ColType::Str => OID_TEXT,
        ColType::Bytes => OID_BYTEA,
        ColType::Bool => OID_BOOL,
        ColType::Date => OID_DATE,
        ColType::Time => OID_TIME,
        ColType::Timestamp => OID_TIMESTAMP,
        ColType::Json => OID_JSON,
        ColType::Uuid => OID_UUID,
        ColType::Decimal { .. } => OID_NUMERIC,
    }
}

/// 值 → 文本格式单元 (None = SQL NULL). ⭐ F80: 按列 ColType 渲染
/// (Bool→'t'/'f', Date/Time/Timestamp→格式化文本, Uuid→36 字符 hex).
fn text_cell(ty: ColType, v: &ColValue) -> Option<Vec<u8>> {
    match v {
        ColValue::Null => None,
        ColValue::I64(x) => Some(
            match ty {
                ColType::Bool => (if *x != 0 { "t" } else { "f" }).to_string(),
                ColType::Date => crate::worker::render_date(*x),
                ColType::Time => crate::worker::render_time(*x),
                ColType::Timestamp => crate::worker::render_timestamp(*x),
                _ => x.to_string(),
            }
            .into_bytes(),
        ),
        ColValue::F64(x) => Some(format_f64(*x).into_bytes()),
        ColValue::Bytes(b) => Some(match ty {
            ColType::Uuid => crate::worker::render_uuid(b).into_bytes(),
            _ => b.clone(),
        }),
        // ⭐ F81: Decimal → 定点文本 "123.45"
        ColValue::Decimal(x, scale) => Some(crate::worker::render_decimal(*x, *scale).into_bytes()),
    }
}

/// f64 文本 (与 mysql 门面同规则: 整值省小数点).
fn format_f64(f: f64) -> String {
    if f == f.trunc() && f.abs() < 1e15 {
        format!("{}", f as i64)
    } else {
        format!("{f}")
    }
}

/// 结果集: RowDescription + DataRow × n + CommandComplete("SELECT n").
pub fn build_result_set(cols: &[(&str, ColType)], rows: &[Vec<ColValue>]) -> Vec<u8> {
    let mut out = Vec::new();
    // RowDescription
    let mut p = Vec::new();
    p.extend_from_slice(&(cols.len() as u16).to_be_bytes());
    for (name, ty) in cols {
        p.extend_from_slice(name.as_bytes());
        p.push(0);
        p.extend_from_slice(&0u32.to_be_bytes()); // table oid
        p.extend_from_slice(&0u16.to_be_bytes()); // attr num
        p.extend_from_slice(&type_oid(*ty).to_be_bytes());
        p.extend_from_slice(&(-1i16).to_be_bytes()); // typlen (variable)
        p.extend_from_slice(&(-1i32).to_be_bytes()); // typmod
        p.extend_from_slice(&0u16.to_be_bytes()); // format = text
    }
    out.extend_from_slice(&frame(b'T', &p));
    // DataRow
    for r in rows {
        let mut p = Vec::new();
        p.extend_from_slice(&(r.len() as u16).to_be_bytes());
        for (ci, v) in r.iter().enumerate() {
            let ty = cols.get(ci).map(|(_, t)| *t).unwrap_or(ColType::Str);
            match text_cell(ty, v) {
                None => p.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(cell) => {
                    p.extend_from_slice(&(cell.len() as u32).to_be_bytes());
                    p.extend_from_slice(&cell);
                }
            }
        }
        out.extend_from_slice(&frame(b'D', &p));
    }
    out.extend_from_slice(&build_command_complete(&format!("SELECT {}", rows.len())));
    out
}

// =====================================================================
// ⭐ P3: 扩展查询协议 (Parse/Bind/Describe/Execute/Close/Sync)
// =====================================================================

fn read_cstr<'a>(buf: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    let start = *pos;
    let end = buf[start..].iter().position(|&b| b == 0)? + start;
    *pos = end + 1;
    Some(&buf[start..end])
}

/// Parse ('P'): (语句名, 查询文本, 参数 OID 列表).
pub fn parse_parse(payload: &[u8]) -> Result<(String, Vec<u8>, Vec<u32>), String> {
    let mut pos = 0;
    let name = read_cstr(payload, &mut pos).ok_or("bad Parse: name")?;
    let query = read_cstr(payload, &mut pos).ok_or("bad Parse: query")?;
    let n = u16::from_be_bytes(
        payload.get(pos..pos + 2).ok_or("bad Parse: oid count")?.try_into().unwrap(),
    ) as usize;
    pos += 2;
    let mut oids = Vec::with_capacity(n);
    for _ in 0..n {
        let b = payload.get(pos..pos + 4).ok_or("bad Parse: oid")?;
        oids.push(u32::from_be_bytes(b.try_into().unwrap()));
        pos += 4;
    }
    Ok((
        String::from_utf8_lossy(name).into_owned(),
        query.to_vec(),
        oids,
    ))
}

/// Bind ('B') 解析结果: 参数原始值 (None = NULL) + 各自格式码 (0 文本/1 二进制).
pub struct BindMsg {
    pub statement: String,
    pub params: Vec<Option<Vec<u8>>>,
    pub formats: Vec<u16>,
    /// 任一结果列请求二进制格式 (v1 不支持 → caller 报错).
    pub binary_results: bool,
}

pub fn parse_bind(payload: &[u8]) -> Result<BindMsg, String> {
    let mut pos = 0;
    let _portal = read_cstr(payload, &mut pos).ok_or("bad Bind: portal")?;
    let statement = read_cstr(payload, &mut pos).ok_or("bad Bind: statement")?;
    let u16_at = |pos: &mut usize| -> Result<u16, String> {
        let b = payload.get(*pos..*pos + 2).ok_or("bad Bind: short")?;
        *pos += 2;
        Ok(u16::from_be_bytes(b.try_into().unwrap()))
    };
    let fmt_n = u16_at(&mut pos)? as usize;
    let mut fmts = Vec::with_capacity(fmt_n);
    for _ in 0..fmt_n {
        fmts.push(u16_at(&mut pos)?);
    }
    let param_n = u16_at(&mut pos)? as usize;
    let mut params = Vec::with_capacity(param_n);
    for _ in 0..param_n {
        let b = payload.get(pos..pos + 4).ok_or("bad Bind: param len")?;
        let len = i32::from_be_bytes(b.try_into().unwrap());
        pos += 4;
        if len < 0 {
            params.push(None);
        } else {
            let l = len as usize;
            params.push(Some(
                payload.get(pos..pos + l).ok_or("bad Bind: param value")?.to_vec(),
            ));
            pos += l;
        }
    }
    // 格式码语义: 0 个 = 全文本; 1 个 = 应用到全部; n 个 = 逐一
    let formats: Vec<u16> = match fmts.len() {
        0 => vec![0; param_n],
        1 => vec![fmts[0]; param_n],
        _ => fmts,
    };
    let rf_n = u16_at(&mut pos)? as usize;
    let mut binary_results = false;
    for _ in 0..rf_n {
        if u16_at(&mut pos)? == 1 {
            binary_results = true;
        }
    }
    Ok(BindMsg {
        statement: String::from_utf8_lossy(statement).into_owned(),
        params,
        formats,
        binary_results,
    })
}

/// Describe/Close ('D'/'C'): (类型 'S' | 'P', 名字).
pub fn parse_target(payload: &[u8]) -> Result<(u8, String), String> {
    let ty = *payload.first().ok_or("bad Describe/Close")?;
    let mut pos = 1;
    let name = read_cstr(payload, &mut pos).ok_or("bad Describe/Close: name")?;
    Ok((ty, String::from_utf8_lossy(name).into_owned()))
}

pub fn build_parse_complete() -> Vec<u8> {
    frame(b'1', &[])
}
pub fn build_bind_complete() -> Vec<u8> {
    frame(b'2', &[])
}
pub fn build_close_complete() -> Vec<u8> {
    frame(b'3', &[])
}
pub fn build_no_data() -> Vec<u8> {
    frame(b'n', &[])
}

/// ParameterDescription ('t'): 参数 OID (未声明按 text=25 报告).
pub fn build_param_description(oids: &[u32], count: u16) -> Vec<u8> {
    let mut p = Vec::with_capacity(2 + count as usize * 4);
    p.extend_from_slice(&count.to_be_bytes());
    for i in 0..count as usize {
        let oid = oids.get(i).copied().filter(|&o| o != 0).unwrap_or(OID_TEXT);
        p.extend_from_slice(&oid.to_be_bytes());
    }
    frame(b't', &p)
}

/// 参数解码: 文本 → Str (弱类型, 目标列转换); 二进制按 OID.
pub fn decode_param(
    raw: Option<&[u8]>,
    format: u16,
    oid: u32,
) -> Result<crate::protocol::sql::SqlValue, String> {
    use crate::protocol::sql::SqlValue;
    let Some(b) = raw else {
        return Ok(SqlValue::Null);
    };
    if format == 0 {
        return Ok(SqlValue::Str(b.to_vec()));
    }
    // 二进制格式: 依 Parse 声明的 OID
    match oid {
        20 => b
            .try_into()
            .map(|a| SqlValue::Int(i64::from_be_bytes(a)))
            .map_err(|_| "bad int8 param".into()),
        23 => b
            .try_into()
            .map(|a| SqlValue::Int(i32::from_be_bytes(a) as i64))
            .map_err(|_| "bad int4 param".into()),
        21 => b
            .try_into()
            .map(|a| SqlValue::Int(i16::from_be_bytes(a) as i64))
            .map_err(|_| "bad int2 param".into()),
        701 => b
            .try_into()
            .map(|a| SqlValue::Float(f64::from_be_bytes(a)))
            .map_err(|_| "bad float8 param".into()),
        700 => b
            .try_into()
            .map(|a| SqlValue::Float(f32::from_be_bytes(a) as f64))
            .map_err(|_| "bad float4 param".into()),
        16 => Ok(SqlValue::Int((b.first() == Some(&1)) as i64)),
        25 | 1043 | 17 => Ok(SqlValue::Str(b.to_vec())),
        0 => Err("binary parameter requires declared type OID".into()),
        other => Err(format!("unsupported binary parameter OID {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip() {
        let f = frame(b'Q', b"SELECT 1\0");
        let (n, ty, payload) = read_frame(&f).unwrap();
        assert_eq!((n, ty), (f.len(), b'Q'));
        assert_eq!(payload, b"SELECT 1\0");
        assert!(read_frame(&f[..4]).is_none(), "不完整帧");
    }

    #[test]
    fn startup_parse() {
        let mut p = PROTOCOL_V3.to_be_bytes().to_vec();
        p.extend_from_slice(b"user\0alice\0database\0app\0\0");
        let (user, db) = parse_startup(&p).unwrap();
        assert_eq!(user, "alice");
        assert_eq!(db.as_deref(), Some("app"));
        // 无 database 参数
        let mut p = PROTOCOL_V3.to_be_bytes().to_vec();
        p.extend_from_slice(b"user\0bob\0\0");
        let (user, db) = parse_startup(&p).unwrap();
        assert_eq!(user, "bob");
        assert!(db.is_none());
        // 错误版本
        assert!(parse_startup(&1234u32.to_be_bytes()).is_err());
    }

    #[test]
    fn result_set_structure() {
        let out = build_result_set(
            &[("id", ColType::I64), ("name", ColType::Str)],
            &[
                vec![ColValue::I64(7), ColValue::Bytes(b"x".to_vec())],
                vec![ColValue::I64(8), ColValue::Null],
            ],
        );
        // T / D / D / C 四帧
        let (n1, t1, _) = read_frame(&out).unwrap();
        assert_eq!(t1, b'T');
        let (n2, t2, _) = read_frame(&out[n1..]).unwrap();
        assert_eq!(t2, b'D');
        let (n3, t3, d2) = read_frame(&out[n1 + n2..]).unwrap();
        assert_eq!(t3, b'D');
        // NULL 单元 = -1 长度
        assert_eq!(&d2[d2.len() - 4..], (-1i32).to_be_bytes().as_slice());
        let (_, t4, c) = read_frame(&out[n1 + n2 + n3..]).unwrap();
        assert_eq!(t4, b'C');
        assert!(c.starts_with(b"SELECT 2"));
    }

    #[test]
    fn error_and_ready() {
        let e = build_error("42601", "syntax error");
        let (_, ty, p) = read_frame(&e).unwrap();
        assert_eq!(ty, b'E');
        assert!(p.windows(6).any(|w| w == b"C42601"));
        let ready = build_ready();
        let (_, ty, p) = read_frame(&ready).unwrap();
        assert_eq!((ty, p), (b'Z', b"I".as_slice()));
    }
}
