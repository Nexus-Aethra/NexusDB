//! ⭐ H5: HTTP REST 门面 e2e — 手写最小 HTTP/1.1 客户端.
//! 覆盖: KV roundtrip (字符串/数值/数值 tag 渲染) / SQL 全流程 / CORS preflight /
//! Bearer 鉴权 / keep-alive 多请求 / /metrics 与 /v1/status / 错误码映射.
//!
//! 注: CORS origin 是进程级 OnceLock (单 HTTP server 语义), 本文件统一 set "*".

use network::{KvLimits, NetworkServer, NetworkServerConfig, ProtocolKind};
use shard_manager::{ShardManager, ShardManagerOptions};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use storage::{IoBackend, IoBackendConfig};

fn start_http_server(token: Option<&str>) -> (NetworkServer, Arc<ShardManager>) {
    network::http_config::set_cors_origin(Some("*".to_string())); // 幂等, 首次生效
    network::metrics::init_start_time();
    let tmp = tempfile::tempdir().expect("tempdir");
    let opts = ShardManagerOptions {
        num_shards: 3,
        block_root: tmp.path().to_path_buf(),
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 4,
        reply_bus_count: None,
        wal_mode: Default::default(),
    };
    let mgr = Arc::new(ShardManager::open(opts).expect("open mgr"));
    mgr.create_db("app").expect("create db");
    mgr.create_table("app", "kv").expect("create table");
    std::mem::forget(tmp);

    let cfg = NetworkServerConfig {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        shard_manager: mgr.clone(),
        worker_count: 1,
        default_db: "app".to_string(),
        default_table: "kv".to_string(),
        inbox_capacity: 64,
        protocol: ProtocolKind::Http,
        limits: KvLimits::default(),
        auth_password: token.map(|s| s.to_string()),
        worker_id_base: 0,
        sql_shared: network::new_sql_shared(),
        tls_config: None,
    };
    let server = NetworkServer::start(cfg).expect("start server");
    (server, mgr)
}

// ===== 最小 HTTP 客户端 =====

struct HttpConn {
    stream: TcpStream,
    buf: Vec<u8>,
}

#[derive(Debug)]
struct Resp {
    status: u16,
    headers: String,
    body: Vec<u8>,
}

impl Resp {
    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with(&name.to_ascii_lowercase()))
            .and_then(|l| l.split_once(':'))
            .map(|(_, v)| v.trim())
    }
}

impl HttpConn {
    fn connect(server: &NetworkServer) -> Self {
        let stream = TcpStream::connect(server.local_addr()).unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        Self { stream, buf: Vec::new() }
    }

    fn request(&mut self, method: &str, path: &str, body: Option<&str>, extra: &str) -> Resp {
        let b = body.unwrap_or("");
        let req = format!(
            "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n{extra}\r\n{b}",
            b.len()
        );
        self.stream.write_all(req.as_bytes()).unwrap();
        // 读一个完整响应 (头 + Content-Length body)
        loop {
            if let Some(head_end) = self.buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&self.buf[..head_end]).into_owned();
                let cl: usize = headers
                    .lines()
                    .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
                    .and_then(|l| l.split(':').nth(1))
                    .and_then(|v| v.trim().parse().ok())
                    .unwrap_or(0);
                if self.buf.len() >= head_end + 4 + cl {
                    let status: u16 = headers
                        .lines()
                        .next()
                        .and_then(|l| l.split(' ').nth(1))
                        .and_then(|s| s.parse().ok())
                        .unwrap();
                    let body = self.buf[head_end + 4..head_end + 4 + cl].to_vec();
                    self.buf.drain(..head_end + 4 + cl);
                    return Resp { status, headers, body };
                }
            }
            let mut tmp = [0u8; 65536];
            let n = self.stream.read(&mut tmp).expect("read");
            assert!(n > 0, "peer closed");
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    fn sql(&mut self, query: &str) -> Resp {
        let body = serde_json::json!({ "query": query }).to_string();
        self.request("POST", "/v1/sql", Some(&body), "")
    }
}

/// KV 三操作 + tag 感知渲染 + SQL 全流程 + 观测端点 (keep-alive 单连接全跑).
#[test]
fn http_full_flow() {
    let (server, mgr) = start_http_server(None);
    let mut c = HttpConn::connect(&server);

    // ---- KV ----
    let r = c.request("PUT", "/v1/kv/user/alice", Some(r#"{"value":"hello"}"#), "");
    assert_eq!(r.status, 200);
    assert_eq!(r.header("Access-Control-Allow-Origin"), Some("*"), "CORS 头");
    let r = c.request("GET", "/v1/kv/user/alice", None, "");
    assert_eq!((r.status, r.json()["value"].as_str()), (200, Some("hello")));
    // 数值 value → 数值 tag → JSON number 回读
    c.request("PUT", "/v1/kv/user/cnt", Some(r#"{"value":42}"#), "");
    let r = c.request("GET", "/v1/kv/user/cnt", None, "");
    assert_eq!(r.json()["value"].as_i64(), Some(42));
    c.request("PUT", "/v1/kv/user/pi", Some(r#"{"value":3.25}"#), "");
    assert_eq!(
        c.request("GET", "/v1/kv/user/pi", None, "").json()["value"].as_f64(),
        Some(3.25)
    );
    // percent-encode key
    c.request("PUT", "/v1/kv/user/a%2Fb%20c", Some(r#"{"value":"x"}"#), "");
    assert_eq!(
        c.request("GET", "/v1/kv/user/a%2Fb%20c", None, "").json()["value"].as_str(),
        Some("x")
    );
    // 404 / DELETE
    assert_eq!(c.request("GET", "/v1/kv/user/ghost", None, "").status, 404);
    let r = c.request("DELETE", "/v1/kv/user/alice", None, "");
    assert_eq!(r.json()["deleted"].as_bool(), Some(true));
    assert_eq!(c.request("GET", "/v1/kv/user/alice", None, "").status, 404);
    assert_eq!(
        c.request("DELETE", "/v1/kv/user/alice", None, "").json()["deleted"].as_bool(),
        Some(false)
    );
    // 错误面
    assert_eq!(c.request("PUT", "/v1/kv/user/x", Some("not-json"), "").status, 400);
    assert_eq!(c.request("PATCH", "/v1/kv/user/x", None, "").status, 405);
    assert_eq!(c.request("GET", "/v1/nope", None, "").status, 404);
    assert_eq!(c.request("GET", "/v1/kv/user/k?db=nodb", None, "").status, 400);

    // ---- SQL ----
    assert_eq!(
        c.sql("CREATE TABLE ht (id INT PRIMARY KEY, name TEXT NOT NULL, INDEX(name))").status,
        200
    );
    let r = c.sql("INSERT INTO ht VALUES (1,'a'), (2,'b'), (3,'a')");
    assert_eq!((r.status, r.json()["affected"].as_u64()), (200, Some(3)));
    let r = c.sql("SELECT id, name FROM ht WHERE name = 'a' ORDER BY id DESC");
    assert_eq!(r.json()["columns"], serde_json::json!(["id", "name"]));
    assert_eq!(r.json()["rows"], serde_json::json!([[3, "a"], [1, "a"]]));
    let r = c.sql("SELECT COUNT(*) FROM ht");
    assert_eq!(r.json()["rows"][0][0].as_i64(), Some(3));
    let r = c.sql("UPDATE ht SET name = 'z' WHERE id = 2");
    assert_eq!(r.json()["affected"].as_u64(), Some(1));
    // 错误映射: 语法 400 / 未知列 400 / duplicate 409
    assert_eq!(c.sql("SELEKT 1").status, 400);
    assert_eq!(c.sql("SELECT nope FROM ht WHERE id = 1").status, 400);
    assert_eq!(c.request("POST", "/v1/sql", Some(r#"{"q":"x"}"#), "").status, 400);

    // ---- 观测 ----
    let r = c.request("GET", "/v1/status", None, "");
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["num_shards"].as_u64(), Some(3));
    let r = c.request("GET", "/metrics", None, "");
    assert_eq!(r.status, 200);
    let text = String::from_utf8_lossy(&r.body).into_owned();
    assert!(text.contains("nexusdb_http_requests_total"), "{text}");
    assert!(text.contains("nexusdb_sql_queries_total"));
    // ⭐ 方案 A (调优): EstimateRows 开销观测指标
    assert!(text.contains("nexusdb_sql_join_est_rounds"), "{text}");
    assert!(text.contains("nexusdb_sql_join_est_skipped"), "{text}");
    assert!(r.header("Content-Type").unwrap().starts_with("text/plain"));
    let r = c.request("GET", "/v1/debug/sql-cache", None, "");
    assert!(r.json()["worker_schemas"].as_u64().unwrap() >= 1);

    // ---- CORS preflight ----
    let r = c.request("OPTIONS", "/v1/sql", None, "Origin: http://app.example\r\n");
    assert_eq!(r.status, 204);
    assert_eq!(r.header("Access-Control-Allow-Origin"), Some("*"));
    assert!(r.header("Access-Control-Allow-Methods").unwrap().contains("DELETE"));

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

/// Bearer 鉴权: 401 拒绝 / 放行 / 白名单端点免鉴权.
#[test]
fn http_bearer_auth() {
    let (server, mgr) = start_http_server(Some("tok123"));
    let mut c = HttpConn::connect(&server);

    assert_eq!(c.request("GET", "/v1/kv/t/k", None, "").status, 401);
    assert_eq!(
        c.request("GET", "/v1/kv/t/k", None, "Authorization: Bearer wrong\r\n").status,
        401
    );
    assert_eq!(
        c.request("GET", "/v1/kv/t/k", None, "Authorization: Bearer tok123\r\n").status,
        404,
        "鉴权过 → 正常 404 (key 不存在)"
    );
    // 白名单免鉴权
    assert_eq!(c.request("GET", "/metrics", None, "").status, 200);
    assert_eq!(c.request("GET", "/v1/status", None, "").status, 200);
    // debug 端点需要鉴权
    assert_eq!(c.request("GET", "/v1/debug/sql-cache", None, "").status, 401);

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

/// Connection: close 语义 + 畸形请求断连.
#[test]
fn http_connection_semantics() {
    let (server, mgr) = start_http_server(None);
    // Connection: close → 响应后服务器关连接
    let mut c = HttpConn::connect(&server);
    let r = c.request("GET", "/v1/status", None, "Connection: close\r\n");
    assert_eq!(r.status, 200);
    let mut tmp = [0u8; 16];
    let n = c.stream.read(&mut tmp).unwrap_or(0);
    assert_eq!(n, 0, "服务器应关闭连接");
    // 畸形请求 → 400 + 断连
    let mut c = HttpConn::connect(&server);
    c.stream.write_all(b"GARBAGE REQUEST\r\n\r\n").unwrap();
    let mut all = Vec::new();
    let _ = c.stream.read_to_end(&mut all);
    let s = String::from_utf8_lossy(&all);
    assert!(s.starts_with("HTTP/1.1 400"), "{s}");

    server.shutdown().unwrap();
    drop(mgr);
}
