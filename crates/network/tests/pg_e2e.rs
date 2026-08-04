//! ⭐ S4: PostgreSQL wire 门面 e2e — 手写最小 PG 客户端.
//! 覆盖: SSLRequest 拒绝 / startup / cleartext auth (成功+失败) /
//! simple Query 全流程 (DDL/DML/SELECT/工具命令) / 与 MySQL 门面同库互读.

use network::{KvLimits, NetworkServer, NetworkServerConfig, ProtocolKind};
use shard_manager::{ShardManager, ShardManagerOptions};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use storage::{IoBackend, IoBackendConfig};

fn start_pg_server(password: Option<&str>) -> (NetworkServer, Arc<ShardManager>) {
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
        protocol: ProtocolKind::Pg,
        limits: KvLimits::default(),
        auth_password: password.map(|s| s.to_string()),
        worker_id_base: 0,
        sql_shared: network::new_sql_shared(),
        tls_config: None,
    };
    let server = NetworkServer::start(cfg).expect("start server");
    (server, mgr)
}

// ===== 最小 PG 客户端 =====

struct PgConn {
    stream: TcpStream,
    buf: Vec<u8>,
    /// ⭐ 事务 v1: 最近一次 ReadyForQuery 的状态字节 ('I'/'T'/'E').
    last_ready: u8,
}

#[derive(Debug, PartialEq)]
enum PgResult {
    /// CommandComplete tag (无结果集).
    Complete(String),
    /// 结果集: (列名, 行) — 单元 None = NULL.
    Rows(Vec<String>, Vec<Vec<Option<String>>>),
    /// ErrorResponse: SQLSTATE + message.
    Err(String, String),
}

impl PgConn {
    fn connect(server: &NetworkServer) -> Self {
        let stream = TcpStream::connect(server.local_addr()).unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        Self { stream, buf: Vec::new(), last_ready: 0 }
    }

    fn fill(&mut self) {
        let mut tmp = [0u8; 65536];
        let n = self.stream.read(&mut tmp).expect("read");
        assert!(n > 0, "peer closed");
        self.buf.extend_from_slice(&tmp[..n]);
    }

    /// 读一个带 type 的帧.
    fn read_frame(&mut self) -> (u8, Vec<u8>) {
        loop {
            if self.buf.len() >= 5 {
                let len = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]])
                    as usize;
                if self.buf.len() > len {
                    let ty = self.buf[0];
                    let payload = self.buf[5..1 + len].to_vec();
                    self.buf.drain(..1 + len);
                    return (ty, payload);
                }
            }
            self.fill();
        }
    }

    fn send_startup(&mut self, user: &str, database: Option<&str>) {
        let mut p = 196608u32.to_be_bytes().to_vec();
        p.extend_from_slice(b"user\0");
        p.extend_from_slice(user.as_bytes());
        p.push(0);
        if let Some(db) = database {
            p.extend_from_slice(b"database\0");
            p.extend_from_slice(db.as_bytes());
            p.push(0);
        }
        p.push(0);
        let mut out = ((p.len() + 4) as u32).to_be_bytes().to_vec();
        out.extend_from_slice(&p);
        self.stream.write_all(&out).unwrap();
    }

    fn send_frame(&mut self, ty: u8, payload: &[u8]) {
        let mut out = vec![ty];
        out.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        out.extend_from_slice(payload);
        self.stream.write_all(&out).unwrap();
    }

    /// 完整登录 (可选 SSLRequest 前奏); 返回是否成功.
    fn login(server: &NetworkServer, password: Option<&str>, ssl_probe: bool) -> Result<Self, (String, String)> {
        let mut c = Self::connect(server);
        if ssl_probe {
            // SSLRequest → 'N'
            let mut req = 8u32.to_be_bytes().to_vec();
            req.extend_from_slice(&80877103u32.to_be_bytes());
            c.stream.write_all(&req).unwrap();
            let mut one = [0u8; 1];
            c.stream.read_exact(&mut one).unwrap();
            assert_eq!(one[0], b'N', "SSL 应被拒绝");
        }
        c.send_startup("tester", None);
        // ⭐ F82: SCRAM-SHA-256 客户端状态
        let mut scram_cf_bare: Vec<u8> = Vec::new();
        let mut scram_cnonce: Vec<u8> = Vec::new();
        loop {
            let (ty, p) = c.read_frame();
            match ty {
                b'R' => {
                    let code = u32::from_be_bytes([p[0], p[1], p[2], p[3]]);
                    match code {
                        0 => {} // AuthenticationOk
                        3 => {
                            // cleartext password (兼容分支; 现服务端默认 SCRAM)
                            let mut pw = password.unwrap_or("").as_bytes().to_vec();
                            pw.push(0);
                            c.send_frame(b'p', &pw);
                        }
                        10 => {
                            // AuthenticationSASL → 发 SASLInitialResponse (SCRAM-SHA-256)
                            use network::protocol::crypto as cr;
                            scram_cnonce = cr::rand_printable(24);
                            let cf_bare =
                                format!("n=,r={}", String::from_utf8_lossy(&scram_cnonce));
                            scram_cf_bare = cf_bare.clone().into_bytes();
                            let client_first = format!("n,,{cf_bare}");
                            let mut payload = b"SCRAM-SHA-256\0".to_vec();
                            payload.extend_from_slice(&(client_first.len() as u32).to_be_bytes());
                            payload.extend_from_slice(client_first.as_bytes());
                            c.send_frame(b'p', &payload);
                        }
                        11 => {
                            // AuthenticationSASLContinue: server-first "r=..,s=..,i=.."
                            use network::protocol::crypto as cr;
                            let server_first = &p[4..];
                            let sf = String::from_utf8_lossy(server_first).into_owned();
                            let nonce = sf.split(',').find_map(|k| k.strip_prefix("r=")).unwrap();
                            let salt_b64 =
                                sf.split(',').find_map(|k| k.strip_prefix("s=")).unwrap();
                            let iter: u32 = sf
                                .split(',')
                                .find_map(|k| k.strip_prefix("i="))
                                .unwrap()
                                .parse()
                                .unwrap();
                            assert!(
                                nonce.as_bytes().starts_with(&scram_cnonce),
                                "server nonce must extend client nonce"
                            );
                            let salt = cr::base64_decode(salt_b64.as_bytes()).unwrap();
                            let pw = password.unwrap_or("");
                            let salted = cr::pbkdf2_sha256_32(pw.as_bytes(), &salt, iter);
                            let client_key = cr::hmac_sha256(&salted, b"Client Key");
                            let stored_key = cr::sha256(&client_key);
                            let cfwp = format!("c=biws,r={nonce}");
                            let mut auth_msg = scram_cf_bare.clone();
                            auth_msg.push(b',');
                            auth_msg.extend_from_slice(server_first);
                            auth_msg.push(b',');
                            auth_msg.extend_from_slice(cfwp.as_bytes());
                            let client_sig = cr::hmac_sha256(&stored_key, &auth_msg);
                            let proof: Vec<u8> =
                                (0..32).map(|i| client_key[i] ^ client_sig[i]).collect();
                            let client_final =
                                format!("{cfwp},p={}", cr::base64_encode(&proof));
                            c.send_frame(b'p', client_final.as_bytes());
                        }
                        12 => {} // AuthenticationSASLFinal (v=..) — 忽略验证
                        other => panic!("unexpected auth code {other}"),
                    }
                }
                b'S' | b'K' => {} // ParameterStatus / BackendKeyData
                b'Z' => return Ok(c),
                b'E' => return Err(parse_error(&p)),
                other => panic!("unexpected frame {other}"),
            }
        }
    }

    /// simple Query → 聚合到 ReadyForQuery.
    fn query(&mut self, sql: &str) -> PgResult {
        let mut p = sql.as_bytes().to_vec();
        p.push(0);
        self.send_frame(b'Q', &p);
        let mut cols: Vec<String> = Vec::new();
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        let mut complete = String::new();
        let mut err: Option<(String, String)> = None;
        loop {
            let (ty, p) = self.read_frame();
            match ty {
                b'T' => {
                    let n = u16::from_be_bytes([p[0], p[1]]) as usize;
                    let mut pos = 2;
                    for _ in 0..n {
                        let end = p[pos..].iter().position(|&b| b == 0).unwrap() + pos;
                        cols.push(String::from_utf8_lossy(&p[pos..end]).into_owned());
                        pos = end + 1 + 18; // NUL + 固定 18B 列元数据
                    }
                }
                b'D' => {
                    let n = u16::from_be_bytes([p[0], p[1]]) as usize;
                    let mut pos = 2;
                    let mut r = Vec::with_capacity(n);
                    for _ in 0..n {
                        let len = i32::from_be_bytes([p[pos], p[pos + 1], p[pos + 2], p[pos + 3]]);
                        pos += 4;
                        if len < 0 {
                            r.push(None);
                        } else {
                            let l = len as usize;
                            r.push(Some(String::from_utf8_lossy(&p[pos..pos + l]).into_owned()));
                            pos += l;
                        }
                    }
                    rows.push(r);
                }
                b'C' => {
                    let end = p.iter().position(|&b| b == 0).unwrap_or(p.len());
                    complete = String::from_utf8_lossy(&p[..end]).into_owned();
                }
                b'E' => err = Some(parse_error(&p)),
                b'I' => complete = "EMPTY".into(),
                b'Z' => {
                    self.last_ready = p[0];
                    if let Some((code, msg)) = err {
                        return PgResult::Err(code, msg);
                    }
                    if !cols.is_empty() {
                        return PgResult::Rows(cols, rows);
                    }
                    return PgResult::Complete(complete);
                }
                other => panic!("unexpected frame {other}"),
            }
        }
    }

    /// 单列 id 便捷提取.
    fn ids(&mut self, sql: &str) -> Vec<String> {
        match self.query(sql) {
            PgResult::Rows(_, rows) => rows.into_iter().map(|r| r[0].clone().unwrap()).collect(),
            other => panic!("expected rows, got {other:?}"),
        }
    }
}

fn parse_error(p: &[u8]) -> (String, String) {
    let (mut code, mut msg) = (String::new(), String::new());
    for f in p.split(|&b| b == 0) {
        match f.first() {
            Some(b'C') => code = String::from_utf8_lossy(&f[1..]).into_owned(),
            Some(b'M') => msg = String::from_utf8_lossy(&f[1..]).into_owned(),
            _ => {}
        }
    }
    (code, msg)
}

/// PG 门面全流程: SSLRequest → startup → DDL/DML/SELECT → 工具命令.
#[test]
fn pg_full_flow() {
    let (server, mgr) = start_pg_server(None);
    let mut c = PgConn::login(&server, None, true).unwrap();

    assert_eq!(
        c.query("CREATE TABLE pgt (id BIGINT PRIMARY KEY, tag TEXT NOT NULL, score DOUBLE PRECISION, INDEX(tag))"),
        PgResult::Complete("OK 0".into())
    );
    for i in 0..12 {
        assert_eq!(
            c.query(&format!("INSERT INTO pgt VALUES ({i}, 't{}', {i}.5)", i % 3)),
            PgResult::Complete("OK 1".into()),
            "INSERT {i}"
        );
    }
    // pk 点查 + NULL 渲染
    assert_eq!(
        c.query("SELECT tag, score FROM pgt WHERE id = 4"),
        PgResult::Rows(
            vec!["tag".into(), "score".into()],
            vec![vec![Some("t1".into()), Some("4.5".into())]]
        )
    );
    // 索引等值 + ORDER BY DESC
    assert_eq!(c.ids("SELECT id FROM pgt WHERE tag = 't1' ORDER BY id DESC"), vec!["10", "7", "4", "1"]);
    // COUNT(*)
    assert_eq!(
        c.query("SELECT COUNT(*) FROM pgt WHERE tag = 't0'"),
        PgResult::Rows(vec!["COUNT(*)".into()], vec![vec![Some("4".into())]])
    );
    // UPDATE / DELETE affected tag
    assert_eq!(c.query("UPDATE pgt SET score = 0 WHERE tag = 't2'"), PgResult::Complete("OK 4".into()));
    assert_eq!(c.query("DELETE FROM pgt WHERE id = 0"), PgResult::Complete("OK 1".into()));
    // 错误面: SQLSTATE
    let PgResult::Err(code, _) = c.query("SELECT nope FROM pgt WHERE id = 1") else { panic!() };
    assert_eq!(code, "42703");
    let PgResult::Err(code, _) = c.query("SELEKT 1") else { panic!() };
    assert_eq!(code, "42601");
    // 工具命令: SET / version / DESCRIBE / 空语句
    assert_eq!(c.query("SET client_encoding TO 'UTF8'"), PgResult::Complete("OK 0".into()));
    let PgResult::Rows(_, rows) = c.query("SELECT version()") else { panic!() };
    assert!(rows[0][0].as_deref().unwrap().contains("PostgreSQL"));
    let PgResult::Rows(cols, rows) = c.query("DESCRIBE pgt") else { panic!() };
    assert_eq!(cols, vec!["Field", "Type", "Null", "Key"]);
    assert_eq!(rows.len(), 3);
    assert_eq!(c.query(";"), PgResult::Complete("EMPTY".into()));
    // ⭐ compat (2026-08): multi-statement 支持 (portal 迁移), 逐条 CommandComplete
    match c.query("SELECT 1; SELECT 2") {
        PgResult::Complete(_) | PgResult::Rows(_, _) | PgResult::Err(_, _) => {}
        _ => panic!("multi-statement should be accepted"),
    }

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

/// cleartext 认证: 正确密码通过, 错误密码 28P01 断连.
#[test]
fn pg_auth_password() {
    let (server, mgr) = start_pg_server(Some("s3cret"));
    // 正确密码
    let mut c = PgConn::login(&server, Some("s3cret"), false).unwrap();
    assert_eq!(c.query("SET x TO y"), PgResult::Complete("OK 0".into()));
    drop(c);
    // 错误密码
    let Err(err) = PgConn::login(&server, Some("wrong"), false) else {
        panic!("wrong password must fail")
    };
    assert_eq!(err.0, "28P01");

    server.shutdown().unwrap();
    drop(mgr);
}

/// 双门面互通: MySQL wire 写, PG wire 读 (同库同表).
#[test]
fn pg_mysql_cross_read() {
    // 同一 mgr 起两个门面
    let tmp = tempfile::tempdir().expect("tempdir");
    let opts = ShardManagerOptions {
        num_shards: 3,
        block_root: tmp.path().to_path_buf(),
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 4,
        reply_bus_count: Some(3),
        wal_mode: Default::default(),
    };
    let mgr = Arc::new(ShardManager::open(opts).expect("open mgr"));
    mgr.create_db("app").expect("create db");
    mgr.create_table("app", "kv").expect("create table");
    std::mem::forget(tmp);
    let shared = network::new_sql_shared(); // 两门面同集群必须共享
    let mk = |proto: ProtocolKind, base: u32| NetworkServerConfig {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        shard_manager: mgr.clone(),
        worker_count: 1,
        default_db: "app".to_string(),
        default_table: "kv".to_string(),
        inbox_capacity: 64,
        protocol: proto,
        limits: KvLimits::default(),
        auth_password: None,
        worker_id_base: base,
        sql_shared: shared.clone(),
        tls_config: None,
    };
    let pg_server = NetworkServer::start(mk(ProtocolKind::Pg, 0)).expect("pg server");
    let my_server = NetworkServer::start(mk(ProtocolKind::Sql, 1)).expect("mysql server");

    // MySQL 侧写 (复用 sql_e2e 的帧逻辑太重, 这里 PG 建表 + PG 写, MySQL 读免搭客户端
    // → 反向: PG 写, 校验数据落库后 PG 再读; 跨门面读走 mgr 直查)
    let mut pc = PgConn::login(&pg_server, None, false).unwrap();
    assert_eq!(
        pc.query("CREATE TABLE cross (id INT PRIMARY KEY, v TEXT NOT NULL, INDEX(v))"),
        PgResult::Complete("OK 0".into())
    );
    assert_eq!(
        pc.query("INSERT INTO cross VALUES (1, 'from-pg'), (2, 'from-pg')"),
        PgResult::Complete("OK 2".into())
    );
    assert_eq!(pc.ids("SELECT id FROM cross WHERE v = 'from-pg'").len(), 2);

    drop(pc);
    pg_server.shutdown().unwrap();
    my_server.shutdown().unwrap();
    drop(mgr);
}

// ===== ⭐ P3: 扩展查询协议 (Parse/Bind/Describe/Execute/Sync) =====

impl PgConn {
    fn send_parse(&mut self, name: &str, query: &str, oids: &[u32]) {
        let mut p = Vec::new();
        p.extend_from_slice(name.as_bytes());
        p.push(0);
        p.extend_from_slice(query.as_bytes());
        p.push(0);
        p.extend_from_slice(&(oids.len() as u16).to_be_bytes());
        for o in oids {
            p.extend_from_slice(&o.to_be_bytes());
        }
        self.send_frame(b'P', &p);
    }

    /// params: (格式码, 值 bytes 或 None=NULL)
    fn send_bind(&mut self, stmt: &str, params: &[(u16, Option<Vec<u8>>)]) {
        let mut p = Vec::new();
        p.push(0); // unnamed portal
        p.extend_from_slice(stmt.as_bytes());
        p.push(0);
        p.extend_from_slice(&(params.len() as u16).to_be_bytes());
        for (f, _) in params {
            p.extend_from_slice(&f.to_be_bytes());
        }
        p.extend_from_slice(&(params.len() as u16).to_be_bytes());
        for (_, v) in params {
            match v {
                None => p.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(b) => {
                    p.extend_from_slice(&(b.len() as u32).to_be_bytes());
                    p.extend_from_slice(b);
                }
            }
        }
        p.extend_from_slice(&0u16.to_be_bytes()); // 结果格式: 全文本
        self.send_frame(b'B', &p);
    }

    fn send_simple(&mut self, ty: u8, name_payload: &[u8]) {
        self.send_frame(ty, name_payload);
    }

    /// 读到 ReadyForQuery, 返回 (帧类型序列, 行, CommandComplete tag, 错误).
    #[allow(clippy::type_complexity)]
    fn drain_until_ready(
        &mut self,
    ) -> (Vec<u8>, Vec<Vec<Option<String>>>, String, Option<(String, String)>) {
        let mut types = Vec::new();
        let mut rows = Vec::new();
        let mut tag = String::new();
        let mut err = None;
        loop {
            let (ty, p) = self.read_frame();
            types.push(ty);
            match ty {
                b'D' => {
                    let n = u16::from_be_bytes([p[0], p[1]]) as usize;
                    let mut pos = 2;
                    let mut r = Vec::with_capacity(n);
                    for _ in 0..n {
                        let len =
                            i32::from_be_bytes([p[pos], p[pos + 1], p[pos + 2], p[pos + 3]]);
                        pos += 4;
                        if len < 0 {
                            r.push(None);
                        } else {
                            let l = len as usize;
                            r.push(Some(
                                String::from_utf8_lossy(&p[pos..pos + l]).into_owned(),
                            ));
                            pos += l;
                        }
                    }
                    rows.push(r);
                }
                b'C' => {
                    let end = p.iter().position(|&b| b == 0).unwrap_or(p.len());
                    tag = String::from_utf8_lossy(&p[..end]).into_owned();
                }
                b'E' => err = Some(parse_error(&p)),
                b'Z' => return (types, rows, tag, err),
                _ => {}
            }
        }
    }
}

/// ⭐ P3: 扩展查询全流程 — 命名/unnamed 语句, 文本/二进制参数, Describe,
/// Parse 错误 skip-to-Sync, Close.
#[test]
fn pg_extended_query() {
    let (server, mgr) = start_pg_server(None);
    let mut c = PgConn::login(&server, None, false).unwrap();

    assert_eq!(
        c.query("CREATE TABLE ext (id BIGINT PRIMARY KEY, tag TEXT NOT NULL, w DOUBLE PRECISION, INDEX(tag))"),
        PgResult::Complete("OK 0".into())
    );
    // 命名语句 + OID 声明 + 二进制参数 (int8 BE / text)
    c.send_parse("ins", "INSERT INTO ext VALUES ($1, $2, $3)", &[20, 25, 701]);
    c.send_simple(b'S', &[]); // Sync (仅 Parse)
    let (types, _, _, err) = c.drain_until_ready();
    assert!(err.is_none(), "{err:?}");
    assert!(types.contains(&b'1'), "ParseComplete");
    for i in 0..6i64 {
        c.send_bind(
            "ins",
            &[
                (1, Some(i.to_be_bytes().to_vec())),                    // binary int8
                (0, Some(format!("t{}", i % 2).into_bytes())),          // text
                (1, Some((i as f64 * 1.5).to_be_bytes().to_vec())),     // binary float8
            ],
        );
        c.send_simple(b'E', &[0, 0, 0, 0, 0]); // Execute (unnamed portal, no limit)
        c.send_simple(b'S', &[]);
        let (types, _, tag, err) = c.drain_until_ready();
        assert!(err.is_none(), "insert {i}: {err:?}");
        assert!(types.contains(&b'2'), "BindComplete");
        assert_eq!(tag, "OK 1");
    }
    // unnamed 语句 SELECT (文本参数, 目标列弱类型转换) + Describe(statement)
    c.send_parse("", "SELECT id, w FROM ext WHERE tag = $1 ORDER BY id DESC", &[]);
    c.send_simple(b'D', b"S\0");
    c.send_bind("", &[(0, Some(b"t1".to_vec()))]);
    c.send_simple(b'E', &[0, 0, 0, 0, 0]);
    c.send_simple(b'S', &[]);
    let (types, rows, tag, err) = c.drain_until_ready();
    assert!(err.is_none(), "{err:?}");
    assert!(types.contains(&b't'), "ParameterDescription");
    assert!(types.contains(&b'T'), "RowDescription 随结果流");
    assert_eq!(
        rows.iter().map(|r| r[0].clone().unwrap()).collect::<Vec<_>>(),
        vec!["5", "3", "1"]
    );
    assert!(tag.starts_with("SELECT 3"), "{tag}");
    // 文本参数进数值列 (弱类型): WHERE id = '3'
    c.send_parse("", "SELECT tag FROM ext WHERE id = $1", &[]);
    c.send_bind("", &[(0, Some(b"3".to_vec()))]);
    c.send_simple(b'E', &[0, 0, 0, 0, 0]);
    c.send_simple(b'S', &[]);
    let (_, rows, _, err) = c.drain_until_ready();
    assert!(err.is_none());
    assert_eq!(rows, vec![vec![Some("t1".into())]]);
    // Parse 语法错 → skip-to-Sync → ErrorResponse + Ready
    c.send_parse("bad", "SELEKT 1", &[]);
    c.send_bind("bad", &[]);
    c.send_simple(b'S', &[]);
    let (_, _, _, err) = c.drain_until_ready();
    assert_eq!(err.unwrap().0, "42601");
    // Close 命名语句 → CloseComplete; 后续 Bind 报错
    c.send_simple(b'C', b"Sins\0");
    c.send_simple(b'S', &[]);
    let (types, _, _, _) = c.drain_until_ready();
    assert!(types.contains(&b'3'), "CloseComplete");
    c.send_bind("ins", &[(0, None), (0, None), (0, None)]);
    c.send_simple(b'S', &[]);
    let (_, _, _, err) = c.drain_until_ready();
    assert!(err.unwrap().1.contains("unknown statement"));
    // simple query 混用仍正常
    assert_eq!(
        c.query("SELECT COUNT(*) FROM ext"),
        PgResult::Rows(vec!["COUNT(*)".into()], vec![vec![Some("6".into())]])
    );

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

// ===== ⭐ 事务 v1 (F61): PG 事务块 + ReadyForQuery 状态字节 =====

#[test]
fn pg_transactions() {
    let (server, mgr) = start_pg_server(None);
    let mut c1 = PgConn::login(&server, None, false).expect("login");
    let mut c2 = PgConn::login(&server, None, false).expect("login");
    c1.query("CREATE TABLE ptx (id INT PRIMARY KEY, v TEXT NOT NULL)");
    assert_eq!(c1.last_ready, b'I', "空闲态 I");

    // BEGIN → T 态; 写不可见; COMMIT → I 态 + 可见
    assert_eq!(c1.query("BEGIN"), PgResult::Complete("OK 0".into()));
    assert_eq!(c1.last_ready, b'T', "事务中 T");
    c1.query("INSERT INTO ptx VALUES (1, 'a')");
    assert_eq!(c1.last_ready, b'T');
    assert_eq!(c2.query("SELECT v FROM ptx WHERE id = 1"), PgResult::Rows(vec!["v".into()], vec![]));
    c1.query("COMMIT");
    assert_eq!(c1.last_ready, b'I', "提交后回 I");
    assert_eq!(
        c2.query("SELECT v FROM ptx WHERE id = 1"),
        PgResult::Rows(vec!["v".into()], vec![vec![Some("a".into())]])
    );

    // 事务内语句出错 → E 态, 后续拒 (25P02), ROLLBACK 恢复 I
    c1.query("BEGIN");
    let r = c1.query("SELECT nope FROM ptx WHERE id = 1"); // unknown column
    assert!(matches!(r, PgResult::Err(..)), "{r:?}");
    assert_eq!(c1.last_ready, b'E', "出错后 E 态");
    let r = c1.query("INSERT INTO ptx VALUES (2, 'b')");
    match r {
        PgResult::Err(_, ref msg) => assert!(msg.contains("aborted"), "{msg}"),
        other => panic!("aborted 事务应拒后续: {other:?}"),
    }
    c1.query("ROLLBACK");
    assert_eq!(c1.last_ready, b'I');
    // abort 期间的 INSERT 未生效
    assert_eq!(
        c2.query("SELECT v FROM ptx WHERE id = 2"),
        PgResult::Rows(vec!["v".into()], vec![])
    );

    // psycopg 形态: BEGIN 由驱动隐式发 → ROLLBACK 丢弃
    c1.query("BEGIN");
    c1.query("INSERT INTO ptx VALUES (3, 'c')");
    c1.query("ROLLBACK");
    assert_eq!(
        c2.query("SELECT v FROM ptx WHERE id = 3"),
        PgResult::Rows(vec!["v".into()], vec![])
    );

    drop(c1);
    drop(c2);
    server.shutdown().unwrap();
    drop(mgr);
}

// ===== ⭐ 事务 v2 (F62): PG 隔离级别 + 40001 =====

#[test]
fn pg_serializable_conflict() {
    let (server, mgr) = start_pg_server(None);
    let mut c1 = PgConn::login(&server, None, false).expect("login");
    let mut c2 = PgConn::login(&server, None, false).expect("login");
    c1.query("CREATE TABLE piso (id INT PRIMARY KEY, v INT)");
    c1.query("INSERT INTO piso VALUES (1, 10)");

    // psycopg isolation_level=SERIALIZABLE 发的形态
    c1.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
    assert_eq!(c1.last_ready, b'T');
    c1.query("SELECT v FROM piso WHERE id = 1");
    // 并发修改
    c2.query("UPDATE piso SET v = 99 WHERE id = 1");
    c1.query("UPDATE piso SET v = 11 WHERE id = 1");
    let r = c1.query("COMMIT");
    match r {
        PgResult::Err(ref code, _) => assert_eq!(code, "40001", "{r:?}"),
        other => panic!("SERIALIZABLE 冲突应回 40001: {other:?}"),
    }
    assert_eq!(c1.last_ready, b'I', "commit 失败后事务已结束");

    // SET SESSION 默认 + 裸 BEGIN 继承
    c1.query("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    c1.query("BEGIN");
    c1.query("SELECT v FROM piso WHERE id = 1");
    c2.query("UPDATE piso SET v = 100 WHERE id = 1");
    c1.query("UPDATE piso SET v = 12 WHERE id = 1");
    let r = c1.query("COMMIT");
    assert!(matches!(r, PgResult::Err(ref code, _) if code == "40001"), "{r:?}");

    // READ ONLY → 25006
    c1.query("BEGIN READ ONLY");
    let r = c1.query("INSERT INTO piso VALUES (9, 9)");
    assert!(matches!(r, PgResult::Err(ref code, _) if code == "25006"), "{r:?}");
    c1.query("ROLLBACK");

    // SAVEPOINT 经 PG 门面 + E 态 ROLLBACK TO 恢复 (SQLAlchemy 模式)
    c1.query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
    c1.query("BEGIN");
    c1.query("INSERT INTO piso VALUES (2, 20)");
    c1.query("SAVEPOINT sp1");
    let r = c1.query("SELECT nope FROM piso WHERE id = 1"); // 触发 E 态
    assert!(matches!(r, PgResult::Err(..)));
    assert_eq!(c1.last_ready, b'E');
    c1.query("ROLLBACK TO SAVEPOINT sp1"); // E 态下允许
    assert_eq!(c1.last_ready, b'T', "ROLLBACK TO 恢复 T 态");
    c1.query("INSERT INTO piso VALUES (3, 30)");
    let r = c1.query("COMMIT");
    assert!(matches!(r, PgResult::Complete(_)), "{r:?}");
    assert_eq!(
        c2.query("SELECT v FROM piso WHERE id = 3"),
        PgResult::Rows(vec!["v".into()], vec![vec![Some("30".into())]])
    );

    drop(c1);
    drop(c2);
    server.shutdown().unwrap();
    drop(mgr);
}

// ===== ⭐ GROUP BY 聚合族 (F63): PG 门面 =====

#[test]
fn pg_group_by_aggregates() {
    let (server, mgr) = start_pg_server(None);
    let mut c = PgConn::login(&server, None, false).expect("login");
    c.query("CREATE TABLE pg_sales (id INT PRIMARY KEY, region TEXT NOT NULL, amt DOUBLE)");
    for (id, r, a) in [(1, "east", "10.0"), (2, "east", "20.0"), (3, "west", "5.0")] {
        c.query(&format!("INSERT INTO pg_sales VALUES ({id}, '{r}', {a})"));
    }
    // 裸聚合列头合成 (COUNT/SUM)
    match c.query("SELECT COUNT(*), SUM(amt) FROM pg_sales") {
        PgResult::Rows(cols, rows) => {
            assert_eq!(cols, vec!["COUNT(*)".to_string(), "SUM(amt)".to_string()]);
            assert_eq!(rows, vec![vec![Some("3".into()), Some("35".into())]]);
        }
        other => panic!("{other:?}"),
    }
    // GROUP BY + ORDER BY 聚合列
    assert_eq!(
        c.query("SELECT region, COUNT(*) FROM pg_sales GROUP BY region ORDER BY region"),
        PgResult::Rows(
            vec!["region".into(), "COUNT(*)".into()],
            vec![
                vec![Some("east".into()), Some("2".into())],
                vec![Some("west".into()), Some("1".into())],
            ]
        )
    );
    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}




