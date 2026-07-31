//! ⭐ H1: HTTP/1.1 REST 门面 (零依赖手写, 与 MySQL/PG wire 同风格).
//!
//! 支持: 增量解析 (不完整返回 None 续读) / keep-alive / Content-Length body /
//! CORS (含 OPTIONS preflight) / Bearer 鉴权在 worker 层.
//! 明确不做 (文档记录): chunked encoding / TLS / HTTP2 / 压缩.
//!
//! 上限: 头部 16KB (431), body 1MB (413) — 超限由 caller 拒绝.

/// 头部区上限 (request line + headers).
pub const MAX_HEAD_BYTES: usize = 16 * 1024;
/// body 上限.
pub const MAX_BODY_BYTES: usize = 1024 * 1024;

/// 一个已解析的 HTTP 请求 (借用检查后拷出, 简化生命周期).
#[derive(Debug)]
pub struct HttpRequest {
    pub method: String,
    /// path 不含 query, 已 percent-decode 的原样段 (解码由 caller 按段做).
    pub path: String,
    /// query 原文 (?后, 不含 #).
    pub query: String,
    pub content_length: usize,
    pub keep_alive: bool,
    /// Authorization 头原文 (Bearer 校验在 caller).
    pub authorization: Option<String>,
    /// Origin 头 (CORS 回显用).
    pub origin: Option<String>,
    pub body: Vec<u8>,
}

/// 解析错误 → (状态码, 消息).
pub type HttpParseError = (u16, &'static str);

/// 增量解析: 完整请求 → Some((消耗字节, 请求)); 数据不足 → None.
/// 头部超限/畸形 → Err (caller 回错误响应并断连).
pub fn parse_request(buf: &[u8]) -> Result<Option<(usize, HttpRequest)>, HttpParseError> {
    // 找头部终结 \r\n\r\n
    let head_end = match find_head_end(buf) {
        Some(e) => e,
        None => {
            if buf.len() > MAX_HEAD_BYTES {
                return Err((431, "request header too large"));
            }
            return Ok(None);
        }
    };
    if head_end > MAX_HEAD_BYTES {
        return Err((431, "request header too large"));
    }
    let head = &buf[..head_end];
    let mut lines = head.split(|&b| b == b'\n').map(|l| l.strip_suffix(b"\r").unwrap_or(l));
    let request_line = lines.next().ok_or((400, "empty request"))?;
    let mut parts = request_line.split(|&b| b == b' ').filter(|s| !s.is_empty());
    let method = std::str::from_utf8(parts.next().ok_or((400, "bad request line"))?)
        .map_err(|_| (400, "bad request line"))?
        .to_ascii_uppercase();
    let target = std::str::from_utf8(parts.next().ok_or((400, "bad request line"))?)
        .map_err(|_| (400, "bad target"))?;
    let version = parts.next().ok_or((400, "bad request line"))?;
    if !version.starts_with(b"HTTP/1.") {
        return Err((505, "http version not supported"));
    }
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.split('#').next().unwrap_or("").to_string()),
        None => (target.to_string(), String::new()),
    };

    let mut content_length = 0usize;
    // HTTP/1.1 默认 keep-alive; Connection: close 显式关闭
    let mut keep_alive = true;
    let mut authorization = None;
    let mut origin = None;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some(colon) = line.iter().position(|&b| b == b':') else {
            return Err((400, "bad header"));
        };
        let name = std::str::from_utf8(&line[..colon]).map_err(|_| (400, "bad header"))?;
        let value = std::str::from_utf8(&line[colon + 1..])
            .map_err(|_| (400, "bad header"))?
            .trim();
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.parse().map_err(|_| (400, "bad content-length"))?;
        } else if name.eq_ignore_ascii_case("connection") {
            if value.eq_ignore_ascii_case("close") {
                keep_alive = false;
            }
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            // chunked 不支持 (v1)
            return Err((501, "transfer-encoding not supported"));
        } else if name.eq_ignore_ascii_case("authorization") {
            authorization = Some(value.to_string());
        } else if name.eq_ignore_ascii_case("origin") {
            origin = Some(value.to_string());
        }
    }
    if content_length > MAX_BODY_BYTES {
        return Err((413, "body too large"));
    }
    let total = head_end + 4 + content_length;
    if buf.len() < total {
        return Ok(None); // body 未到齐
    }
    let body = buf[head_end + 4..total].to_vec();
    Ok(Some((
        total,
        HttpRequest {
            method,
            path,
            query,
            content_length,
            keep_alive,
            authorization,
            origin,
            body,
        },
    )))
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// query 串取参 (`db=x&y=z` → 首个匹配值, 未 percent-decode 的简单键值).
pub fn query_param<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v)
}

/// URL path 段 percent-decode (%XX + '+' 不转义 — path 语义).
pub fn percent_decode(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len()
            && let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2]))
        {
            out.push((h << 4) | l);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    out
}

fn hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        413 => "Payload Too Large",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        505 => "HTTP Version Not Supported",
        _ => "Unknown",
    }
}

/// 统一响应出口: JSON body (空 = 无 body, 如 204) + CORS 头 + keep-alive.
/// `cors_origin`: 配置值非空时回显 (`*` 原样).
pub fn build_response(
    status: u16,
    json_body: &[u8],
    cors_origin: Option<&str>,
    keep_alive: bool,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(json_body.len() + 160);
    out.extend_from_slice(format!("HTTP/1.1 {} {}\r\n", status, status_text(status)).as_bytes());
    out.extend_from_slice(b"Server: NexusDB\r\n");
    if !json_body.is_empty() {
        out.extend_from_slice(b"Content-Type: application/json\r\n");
    }
    out.extend_from_slice(format!("Content-Length: {}\r\n", json_body.len()).as_bytes());
    if let Some(origin) = cors_origin {
        out.extend_from_slice(
            format!("Access-Control-Allow-Origin: {origin}\r\n").as_bytes(),
        );
    }
    out.extend_from_slice(if keep_alive {
        b"Connection: keep-alive\r\n".as_slice()
    } else {
        b"Connection: close\r\n".as_slice()
    });
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(json_body);
    out
}

/// OPTIONS preflight 响应 (204 + 完整 CORS 头; cors 未配置时仍回 204 无 CORS 头).
pub fn build_preflight(cors_origin: Option<&str>, keep_alive: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(220);
    out.extend_from_slice(b"HTTP/1.1 204 No Content\r\nServer: NexusDB\r\nContent-Length: 0\r\n");
    if let Some(origin) = cors_origin {
        out.extend_from_slice(
            format!(
                "Access-Control-Allow-Origin: {origin}\r\n\
                 Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n\
                 Access-Control-Allow-Headers: Content-Type, Authorization\r\n\
                 Access-Control-Max-Age: 86400\r\n"
            )
            .as_bytes(),
        );
    }
    out.extend_from_slice(if keep_alive {
        b"Connection: keep-alive\r\n".as_slice()
    } else {
        b"Connection: close\r\n".as_slice()
    });
    out.extend_from_slice(b"\r\n");
    out
}

/// 错误 JSON body.
pub fn error_body(msg: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({ "error": msg })).unwrap_or_default()
}

/// text/plain 响应 (Prometheus /metrics 用).
pub fn build_text_response(status: u16, body: &[u8], keep_alive: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 128);
    out.extend_from_slice(format!("HTTP/1.1 {} {}\r\n", status, status_text(status)).as_bytes());
    out.extend_from_slice(b"Server: NexusDB\r\nContent-Type: text/plain; version=0.0.4\r\n");
    out.extend_from_slice(format!("Content-Length: {}\r\n", body.len()).as_bytes());
    out.extend_from_slice(if keep_alive {
        b"Connection: keep-alive\r\n".as_slice()
    } else {
        b"Connection: close\r\n".as_slice()
    });
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(body);
    out
}

/// 标准 base64 编码 (非法 UTF-8 value 的 JSON 兜底; 零依赖).
pub fn base64_encode(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { T[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { T[n as usize & 63] as char } else { '=' });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incremental_parse() {
        let full = b"GET /v1/kv/t/k?db=app HTTP/1.1\r\nHost: x\r\nOrigin: http://a.b\r\n\r\n";
        // 不完整 → None
        for cut in [0, 5, full.len() - 1] {
            assert!(parse_request(&full[..cut]).unwrap().is_none(), "cut={cut}");
        }
        let (n, req) = parse_request(full).unwrap().unwrap();
        assert_eq!(n, full.len());
        assert_eq!((req.method.as_str(), req.path.as_str()), ("GET", "/v1/kv/t/k"));
        assert_eq!(query_param(&req.query, "db"), Some("app"));
        assert_eq!(req.origin.as_deref(), Some("http://a.b"));
        assert!(req.keep_alive, "HTTP/1.1 默认 keep-alive");
    }

    #[test]
    fn body_and_pipeline() {
        let r1 = b"POST /v1/sql HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello";
        let mut buf = r1.to_vec();
        buf.extend_from_slice(b"GET /v1/status HTTP/1.1\r\nConnection: close\r\n\r\n");
        // body 未到齐 → None
        assert!(parse_request(&r1[..r1.len() - 1]).unwrap().is_none());
        let (n1, req1) = parse_request(&buf).unwrap().unwrap();
        assert_eq!(req1.body, b"hello");
        let (_, req2) = parse_request(&buf[n1..]).unwrap().unwrap();
        assert_eq!(req2.path, "/v1/status");
        assert!(!req2.keep_alive);
    }

    #[test]
    fn limits_and_errors() {
        // 头超限
        let mut huge = b"GET / HTTP/1.1\r\nX: ".to_vec();
        huge.extend(std::iter::repeat_n(b'a', MAX_HEAD_BYTES + 1));
        assert_eq!(parse_request(&huge).unwrap_err().0, 431);
        // body 超限
        let big = format!("POST / HTTP/1.1\r\nContent-Length: {}\r\n\r\n", MAX_BODY_BYTES + 1);
        assert_eq!(parse_request(big.as_bytes()).unwrap_err().0, 413);
        // chunked 拒绝
        let ch = b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n";
        assert_eq!(parse_request(ch).unwrap_err().0, 501);
        // 畸形
        assert_eq!(parse_request(b"BAD\r\n\r\n").unwrap_err().0, 400);
        assert_eq!(parse_request(b"GET / HTTP/2.0\r\n\r\n").unwrap_err().0, 505);
    }

    #[test]
    fn responses() {
        let r = build_response(200, br#"{"ok":1}"#, Some("*"), true);
        let s = String::from_utf8_lossy(&r);
        assert!(s.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(s.contains("Access-Control-Allow-Origin: *\r\n"));
        assert!(s.contains("Content-Length: 8\r\n"));
        assert!(s.ends_with(r#"{"ok":1}"#));
        let p = build_preflight(Some("http://a.b"), true);
        let s = String::from_utf8_lossy(&p);
        assert!(s.starts_with("HTTP/1.1 204"));
        assert!(s.contains("Access-Control-Allow-Methods:"));
        assert!(s.contains("Access-Control-Allow-Origin: http://a.b\r\n"));
        // CORS 未配置 → 无 CORS 头
        let r = build_response(404, b"{}", None, false);
        assert!(!String::from_utf8_lossy(&r).contains("Access-Control"));
    }

    #[test]
    fn decode_helpers() {
        assert_eq!(percent_decode("a%2Fb%20c"), b"a/b c");
        assert_eq!(percent_decode("no-escape"), b"no-escape");
        assert_eq!(percent_decode("%zz"), b"%zz", "非法转义原样");
    }
}
