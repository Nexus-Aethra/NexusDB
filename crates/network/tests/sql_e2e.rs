//! ⭐ Z2/Z3: SQL 门面 (MySQL wire protocol) e2e.
//!
//! 手写最小 MySQL 客户端: 握手 → mysql_native_password 登录 → COM_QUERY
//! → OK/ERR/文本结果集解析. 覆盖免密/密码/密码错/AuthSwitch 流程.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use network::protocol::mysql as my;
use network::{KvLimits, NetworkServer, NetworkServerConfig, ProtocolKind};
use shard_manager::{ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

fn start_sql_server(password: Option<&str>) -> (NetworkServer, Arc<ShardManager>) {
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
        protocol: ProtocolKind::Sql,
        limits: KvLimits::default(),
        auth_password: password.map(|s| s.to_string()),
        worker_id_base: 0,
        sql_shared: network::new_sql_shared(),
    };
    let server = NetworkServer::start(cfg).expect("start server");
    (server, mgr)
}

// ===== 最小 MySQL 客户端 =====

struct MyConn {
    stream: TcpStream,
    buf: Vec<u8>,
}

/// 查询结果.
#[derive(Debug, PartialEq)]
enum QueryResult {
    Ok { affected: u64 },
    Err { code: u16, msg: String },
    /// 文本结果集: 每行每列 Option<String> (None = NULL).
    Rows(Vec<Vec<Option<String>>>),
}

impl MyConn {
    fn connect(server: &NetworkServer) -> Self {
        Self::connect_addr(server.local_addr())
    }

    /// ⭐ Phase A: addr 版构造 (bench 多线程用).
    fn connect_addr(addr: std::net::SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).expect("connect");
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        stream.set_nodelay(true).unwrap();
        Self { stream, buf: Vec::new() }
    }

    /// ⭐ Phase A: 已连接后登录 (免 &NetworkServer).
    fn login(&mut self, password: &str) {
        let salt = self.read_handshake();
        let token = my::native_password_token(&salt, password);
        let (_, resp) = self.login_native("root", &token);
        assert_eq!(resp[0], 0x00, "login should succeed");
    }

    fn read_frame(&mut self) -> (u8, Vec<u8>) {
        loop {
            if let Some((seq, n, payload)) = my::read_packet(&self.buf) {
                self.buf.drain(..n);
                return (seq, payload);
            }
            let mut tmp = [0u8; 4096];
            let got = self.stream.read(&mut tmp).expect("read");
            assert!(got > 0, "connection closed");
            self.buf.extend_from_slice(&tmp[..got]);
        }
    }

    /// 读握手 → 提取 salt.
    fn read_handshake(&mut self) -> [u8; 20] {
        let (seq, p) = self.read_frame();
        assert_eq!(seq, 0);
        assert_eq!(p[0], 10, "protocol version");
        let mut pos = 1;
        while p[pos] != 0 {
            pos += 1; // server version
        }
        pos += 1 + 4; // NUL + thread_id
        let mut salt = [0u8; 20];
        salt[..8].copy_from_slice(&p[pos..pos + 8]);
        pos += 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10; // salt1+filler+caps_lo+charset+status+caps_hi+len+reserved
        salt[8..20].copy_from_slice(&p[pos..pos + 12]);
        salt
    }

    /// 发 HandshakeResponse41 (native plugin). 返回服务端响应帧.
    fn login_native(&mut self, user: &str, token: &[u8]) -> (u8, Vec<u8>) {
        let flags = my::CLIENT_PROTOCOL_41 | my::CLIENT_SECURE_CONNECTION | my::CLIENT_PLUGIN_AUTH;
        let mut p = Vec::new();
        p.extend_from_slice(&flags.to_le_bytes());
        p.extend_from_slice(&0x0100_0000u32.to_le_bytes());
        p.push(45);
        p.extend_from_slice(&[0u8; 23]);
        p.extend_from_slice(user.as_bytes());
        p.push(0);
        p.push(token.len() as u8);
        p.extend_from_slice(token);
        p.extend_from_slice(b"mysql_native_password\0");
        self.stream.write_all(&my::write_packet(1, &p)).unwrap();
        self.read_frame()
    }

    /// 完整登录 (握手 + native 登录), 断言成功.
    fn handshake_login(server: &NetworkServer, password: &str) -> Self {
        let mut c = Self::connect(server);
        let salt = c.read_handshake();
        let token = my::native_password_token(&salt, password);
        let (_, resp) = c.login_native("root", &token);
        assert_eq!(resp[0], 0x00, "login should succeed: {resp:?}");
        c
    }

    fn query(&mut self, stmt: &str) -> QueryResult {
        let mut p = vec![my::COM_QUERY];
        p.extend_from_slice(stmt.as_bytes());
        self.stream.write_all(&my::write_packet(0, &p)).unwrap();
        let (_, first) = self.read_frame();
        match first[0] {
            0x00 => QueryResult::Ok { affected: first[1] as u64 }, // affected < 251
            0xFF => QueryResult::Err {
                code: u16::from_le_bytes([first[1], first[2]]),
                msg: String::from_utf8_lossy(&first[9..]).into_owned(),
            },
            n => {
                let ncols = n as usize; // 列数 < 251
                for _ in 0..ncols {
                    self.read_frame(); // 列定义
                }
                let (_, eof) = self.read_frame();
                assert_eq!(eof[0], 0xFE, "expect EOF after columns");
                let mut rows = Vec::new();
                loop {
                    let (_, rp) = self.read_frame();
                    if rp[0] == 0xFE && rp.len() < 9 {
                        break; // 尾 EOF
                    }
                    let mut row = Vec::with_capacity(ncols);
                    let mut pos = 0usize;
                    for _ in 0..ncols {
                        if rp[pos] == 0xFB {
                            row.push(None);
                            pos += 1;
                        } else {
                            let len = rp[pos] as usize; // 测试值 < 251
                            pos += 1;
                            row.push(Some(
                                String::from_utf8_lossy(&rp[pos..pos + len]).into_owned(),
                            ));
                            pos += len;
                        }
                    }
                    rows.push(row);
                }
                QueryResult::Rows(rows)
            }
        }
    }

    fn ids(&mut self, stmt: &str) -> Vec<String> {
        match self.query(stmt) {
            QueryResult::Rows(rows) => {
                rows.iter().map(|r| r[0].clone().expect("id 非 NULL")).collect()
            }
            other => panic!("expect rows, got {other:?}"),
        }
    }
}

// ===== 测试 =====

#[test]
fn mysql_auth_password_and_failures() {
    let (server, mgr) = start_sql_server(Some("s3cret"));

    // 正确密码
    let mut c = MyConn::handshake_login(&server, "s3cret");
    assert_eq!(c.query("SELECT 1").err_code(), Some(1064), "SELECT 1 非法语法 (需 FROM)");

    // 密码错 → 1045
    let mut bad = MyConn::connect(&server);
    let salt = bad.read_handshake();
    let token = my::native_password_token(&salt, "wrong");
    let (_, resp) = bad.login_native("root", &token);
    assert_eq!(resp[0], 0xFF);
    assert_eq!(u16::from_le_bytes([resp[1], resp[2]]), 1045);

    // caching_sha2 客户端 → AuthSwitch 兜底
    let mut sw = MyConn::connect(&server);
    let salt = sw.read_handshake();
    let flags = my::CLIENT_PROTOCOL_41 | my::CLIENT_SECURE_CONNECTION | my::CLIENT_PLUGIN_AUTH;
    let mut p = Vec::new();
    p.extend_from_slice(&flags.to_le_bytes());
    p.extend_from_slice(&0x0100_0000u32.to_le_bytes());
    p.push(45);
    p.extend_from_slice(&[0u8; 23]);
    p.extend_from_slice(b"root\0");
    p.push(32); // caching_sha2 风格 32B 响应 (内容无效)
    p.extend_from_slice(&[0xAA; 32]);
    p.extend_from_slice(b"caching_sha2_password\0");
    sw.stream.write_all(&my::write_packet(1, &p)).unwrap();
    let (_, switch) = sw.read_frame();
    assert_eq!(switch[0], 0xFE, "应发 AuthSwitchRequest");
    assert!(switch[1..].starts_with(b"mysql_native_password\0"));
    // 用切换包里的新 salt 重新计算 token
    let mut new_salt = [0u8; 20];
    new_salt.copy_from_slice(&switch[1 + 22..1 + 22 + 20]);
    assert_eq!(new_salt, salt, "AuthSwitch 复用同一 salt");
    let token = my::native_password_token(&salt, "s3cret");
    sw.stream.write_all(&my::write_packet(3, &token)).unwrap();
    let (_, ok) = sw.read_frame();
    assert_eq!(ok[0], 0x00, "AuthSwitch 后登录成功");

    drop(c);
    drop(sw);
    server.shutdown().unwrap();
    drop(mgr);
}

impl QueryResult {
    fn err_code(&self) -> Option<u16> {
        match self {
            QueryResult::Err { code, .. } => Some(*code),
            _ => None,
        }
    }
}

#[test]
fn mysql_sql_full_flow() {
    let (server, mgr) = start_sql_server(None); // 免密

    let mut c = MyConn::handshake_login(&server, "");

    // CREATE (幂等重放)
    let create = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, score DOUBLE, INDEX(name), INDEX(score))";
    assert_eq!(c.query(create), QueryResult::Ok { affected: 0 });
    assert_eq!(c.query(create), QueryResult::Ok { affected: 0 });

    for i in 0..20 {
        assert_eq!(
            c.query(&format!(
                "INSERT INTO users (id, name, score) VALUES ({i}, 'u{}', {i}.0)",
                i % 3
            )),
            QueryResult::Ok { affected: 1 },
            "INSERT {i}"
        );
    }
    // 含空格 + 转义引号字面量
    assert_eq!(
        c.query("INSERT INTO users (id, name) VALUES (100, 'bob smith''s')"),
        QueryResult::Ok { affected: 1 }
    );

    // pk 点查 (NULL 渲染 0xFB)
    assert_eq!(
        c.query("SELECT * FROM users WHERE id = 100"),
        QueryResult::Rows(vec![vec![
            Some("100".into()),
            Some("bob smith's".into()),
            None,
        ]])
    );
    assert_eq!(c.query("SELECT * FROM users WHERE id = 999"), QueryResult::Rows(vec![]));

    // 索引等值 (广播聚合全局有序)
    assert_eq!(
        c.ids("SELECT * FROM users WHERE name = 'u2'"),
        vec!["2", "5", "8", "11", "14", "17"]
    );
    // 范围/开区间/AND 残余/LIMIT
    assert_eq!(c.ids("SELECT * FROM users WHERE score >= 5 AND score <= 8").len(), 4);
    assert_eq!(c.ids("SELECT * FROM users WHERE score > 5 AND score < 8"), vec!["6", "7"]);
    assert_eq!(c.ids("SELECT * FROM users WHERE name = 'u2' AND score > 10").len(), 3);
    assert_eq!(c.ids("SELECT * FROM users WHERE name = 'u0' LIMIT 2"), vec!["0", "3"]);

    // ⭐ Y1: bloom 拒绝未插入值
    assert_eq!(c.query("SELECT * FROM users WHERE name = 'ghost'"), QueryResult::Rows(vec![]));

    // ⭐ O1: 投影列 (顺序 = 投影序, 与 schema 序无关)
    assert_eq!(
        c.query("SELECT score, id FROM users WHERE id = 7"),
        QueryResult::Rows(vec![vec![Some("7".into()), Some("7".into())]])
    );
    // ⭐ O1: 覆盖索引 — 投影/条件全在 (name, id) → 免回表, 值从索引条目重建
    let r = c.query("SELECT name, id FROM users WHERE name = 'u2'");
    let QueryResult::Rows(rows) = &r else { panic!("{r:?}") };
    assert_eq!(rows.len(), 6);
    assert!(rows.iter().all(|x| x[0].as_deref() == Some("u2")));
    assert_eq!(
        rows.iter().map(|x| x[1].clone().unwrap()).collect::<Vec<_>>(),
        vec!["2", "5", "8", "11", "14", "17"],
        "覆盖路径 pk 重建 + 全局有序"
    );
    // 覆盖 + 数值索引 (F64 保序解码重建)
    let r = c.query("SELECT score FROM users WHERE score >= 5 AND score <= 7");
    let QueryResult::Rows(rows) = &r else { panic!("{r:?}") };
    assert_eq!(
        rows.iter().map(|x| x[0].clone().unwrap()).collect::<Vec<_>>(),
        vec!["5", "6", "7"],
        "F64 覆盖重建"
    );
    // 非覆盖投影 (投影列不在索引) — 回表后投影
    let r = c.query("SELECT score FROM users WHERE name = 'u2' LIMIT 2");
    let QueryResult::Rows(rows) = &r else { panic!("{r:?}") };
    assert_eq!(rows.iter().map(|x| x[0].clone().unwrap()).collect::<Vec<_>>(), vec!["2", "5"]);
    // 投影未知列
    assert_eq!(c.query("SELECT nope FROM users WHERE id = 1").err_code(), Some(1054));

    // UPDATE 覆盖换索引
    assert_eq!(
        c.query("INSERT INTO users (id, name, score) VALUES (5, 'u9', 5.0)"),
        QueryResult::Ok { affected: 1 }
    );
    assert_eq!(c.ids("SELECT * FROM users WHERE name = 'u9'"), vec!["5"]);
    assert_eq!(c.ids("SELECT * FROM users WHERE name = 'u2'").len(), 5);

    // 错误面 (MySQL 错误码)
    assert_eq!(c.query("SELECT * FORM users").err_code(), Some(1064));
    assert_eq!(c.query("SELECT * FROM users WHERE nope = 1").err_code(), Some(1054));
    // ⭐ S2: 无 WHERE 从报错变为全表扫 (行为升级; 0..20 + 上方 INSERT id=100)
    assert_eq!(c.ids("SELECT * FROM users").len(), 21);
    assert_eq!(c.query("SELECT * FROM plainkv WHERE id = 1").err_code(), Some(1105));
    assert_eq!(c.query("INSERT INTO users (id, name) VALUES ('x', 'y')").err_code(), Some(1105));

    // schema miss 续跑 (新连接)
    let mut c2 = MyConn::handshake_login(&server, "");
    assert_eq!(
        c2.query("SELECT * FROM users WHERE id = 3"),
        QueryResult::Rows(vec![vec![Some("3".into()), Some("u0".into()), Some("3".into())]])
    );
    // ⭐ W3: 跨连接等值查询 — INSERT 全在 conn A, conn B 经 worker 级共享
    // 路由缓存候选分派, 结果必须完整 (共享 cache 可见性 + 无假阴性)
    assert_eq!(c2.ids("SELECT * FROM users WHERE name = 'u1'").len(), 7);
    assert_eq!(c2.query("SELECT * FROM users WHERE name = 'ghost2'"), QueryResult::Rows(vec![]));

    // COM_PING
    c2.stream.write_all(&my::write_packet(0, &[my::COM_PING])).unwrap();
    let (_, pong) = c2.read_frame();
    assert_eq!(pong[0], 0x00);

    drop(c);
    drop(c2);
    server.shutdown().unwrap();
    drop(mgr);
}

#[test]
fn mysql_unauthed_command_rejected() {
    let (server, mgr) = start_sql_server(Some("pw"));
    let mut c = MyConn::connect(&server);
    let _salt = c.read_handshake();
    // 未登录直接发 COM_QUERY → 登录状态机按 HandshakeResponse 解析失败, 拒绝并断连
    let mut p = vec![my::COM_QUERY];
    p.extend_from_slice(b"SELECT * FROM t WHERE id = 1");
    c.stream.write_all(&my::write_packet(1, &p)).unwrap();
    let (_, resp) = c.read_frame();
    assert_eq!(resp[0], 0xFF, "未认证命令应被拒绝");
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ O3: UNIQUE 索引 — 约束强制 + 等值早停正确性.
#[test]
fn mysql_unique_index() {
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");

    assert_eq!(
        c.query("CREATE TABLE accts (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT NOT NULL, INDEX(name))"),
        QueryResult::Ok { affected: 0 }
    );
    for i in 0..12 {
        assert_eq!(
            c.query(&format!(
                "INSERT INTO accts VALUES ({i}, 'u{i}@x.com', 'n{}')",
                i % 3
            )),
            QueryResult::Ok { affected: 1 },
            "INSERT {i}"
        );
    }

    // 重复 email (不同 pk): 唯一性探测仅本 shard — 同 shard 必拒,
    // 跨 shard 漏检 (文档 gap). 两种结果都合法, 漏检路径恢复唯一性后继续.
    let r = c.query("INSERT INTO accts VALUES (100, 'u3@x.com', 'nx')");
    match r {
        QueryResult::Err { code, msg } => {
            assert_eq!(code, 1105);
            assert!(msg.contains("duplicate key"), "{msg}");
        }
        QueryResult::Ok { .. } => {
            // 跨 shard 漏检: u3@x.com 暂时双行 — 改回不冲突值恢复唯一
            assert_eq!(
                c.query("INSERT INTO accts VALUES (100, 'u100@x.com', 'nx')"),
                QueryResult::Ok { affected: 1 }
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
    // 确定性断言: 同 pk 覆盖**自身** email 不误报 (unchanged 跳过);
    // 同 shard 撞库的确定性拒绝由引擎测试覆盖 (e2e hash 不可控)
    assert_eq!(
        c.query("INSERT INTO accts VALUES (5, 'u5@x.com', 'renamed')"),
        QueryResult::Ok { affected: 1 },
        "同 pk 同 email 覆盖不应误报 duplicate"
    );

    // 等值早停 (unique): 结果正确且完整
    assert_eq!(
        c.query("SELECT id FROM accts WHERE email = 'u7@x.com'"),
        QueryResult::Rows(vec![vec![Some("7".into())]])
    );
    assert_eq!(
        c.query("SELECT * FROM accts WHERE email = 'zz@x.com'"),
        QueryResult::Rows(vec![]),
        "miss: 早停不触发 (空结果), 等全量回齐"
    );
    // 早停后连接继续可用 (迟到回包被丢弃不产生错乱)
    for i in 0..12 {
        let r = c.query(&format!("SELECT id FROM accts WHERE email = 'u{i}@x.com'"));
        assert_eq!(
            r,
            QueryResult::Rows(vec![vec![Some(i.to_string())]]),
            "早停连发 {i}"
        );
    }
    // 普通索引不受影响
    let r = c.query("SELECT * FROM accts WHERE name = 'n1'");
    let QueryResult::Rows(rows) = &r else { panic!("{r:?}") };
    assert_eq!(rows.len(), 4);

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ S1: DML 补全 — DELETE / UPDATE SET / 多行 INSERT / DROP TABLE.
#[test]
fn mysql_dml_full() {
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");

    assert_eq!(
        c.query("CREATE TABLE items (id INT PRIMARY KEY, cat TEXT NOT NULL, qty INT, INDEX(cat))"),
        QueryResult::Ok { affected: 0 }
    );
    // 多行 INSERT: affected = 行数
    assert_eq!(
        c.query("INSERT INTO items VALUES (1,'a',10), (2,'b',20), (3,'a',30), (4,'c',40), (5,'a',50)"),
        QueryResult::Ok { affected: 5 }
    );
    // UPDATE pk 等值 (单 shard 原子)
    assert_eq!(
        c.query("UPDATE items SET qty = 21 WHERE id = 2"),
        QueryResult::Ok { affected: 1 }
    );
    assert_eq!(
        c.query("SELECT qty FROM items WHERE id = 2"),
        QueryResult::Rows(vec![vec![Some("21".into())]])
    );
    // UPDATE pk miss → affected 0
    assert_eq!(
        c.query("UPDATE items SET qty = 0 WHERE id = 99"),
        QueryResult::Ok { affected: 0 }
    );
    // UPDATE 索引条件 (两阶段): cat='a' 的 3 行 qty 置 7
    assert_eq!(
        c.query("UPDATE items SET qty = 7 WHERE cat = 'a'"),
        QueryResult::Ok { affected: 3 }
    );
    let r = c.query("SELECT qty, id FROM items WHERE cat = 'a'");
    let QueryResult::Rows(rows) = &r else { panic!("{r:?}") };
    assert!(rows.iter().all(|x| x[0].as_deref() == Some("7")), "{rows:?}");
    // UPDATE 改索引列: 索引行跟随 (b → a)
    assert_eq!(
        c.query("UPDATE items SET cat = 'a' WHERE id = 2"),
        QueryResult::Ok { affected: 1 }
    );
    assert_eq!(c.ids("SELECT * FROM items WHERE cat = 'a'").len(), 4);
    assert_eq!(c.query("SELECT * FROM items WHERE cat = 'b'"), QueryResult::Rows(vec![]));
    // UPDATE 禁改 pk / 未知列
    assert_eq!(c.query("UPDATE items SET id = 9 WHERE id = 1").err_code(), Some(1105));
    assert_eq!(c.query("UPDATE items SET nope = 9 WHERE id = 1").err_code(), Some(1054));

    // DELETE pk 等值
    assert_eq!(c.query("DELETE FROM items WHERE id = 4"), QueryResult::Ok { affected: 1 });
    assert_eq!(c.query("DELETE FROM items WHERE id = 4"), QueryResult::Ok { affected: 0 });
    // DELETE 索引条件 (两阶段): cat='a' 剩 4 行全删
    assert_eq!(
        c.query("DELETE FROM items WHERE cat = 'a'"),
        QueryResult::Ok { affected: 4 }
    );
    assert_eq!(c.query("SELECT * FROM items WHERE cat = 'a'"), QueryResult::Rows(vec![]));
    // 剩 id=5? 不 — 5 是 cat 'a' 已删; 全表剩 0 行 (1,2,3,5 删 + 4 删)
    assert_eq!(c.query("SELECT * FROM items WHERE id = 5"), QueryResult::Rows(vec![]));

    // DROP TABLE: 后续查询报 no schema; 重建同名表干净
    assert_eq!(
        c.query("INSERT INTO items VALUES (7,'z',1)"),
        QueryResult::Ok { affected: 1 }
    );
    assert_eq!(c.query("DROP TABLE items"), QueryResult::Ok { affected: 0 });
    assert_eq!(c.query("SELECT * FROM items WHERE id = 7").err_code(), Some(1105));
    assert_eq!(
        c.query("CREATE TABLE items (id INT PRIMARY KEY, cat TEXT NOT NULL, INDEX(cat))"),
        QueryResult::Ok { affected: 0 }
    );
    assert_eq!(
        c.query("SELECT * FROM items WHERE cat = 'z'"),
        QueryResult::Rows(vec![]),
        "DROP 后重建: 旧数据/旧索引/路由缓存全清"
    );

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ S2: SELECT 扩展 — 全表扫 / ORDER BY / OFFSET / COUNT / IN / BETWEEN / != / LIKE.
#[test]
fn mysql_select_extended() {
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");

    assert_eq!(
        c.query("CREATE TABLE nums (id INT PRIMARY KEY, grp TEXT NOT NULL, val DOUBLE, note TEXT, INDEX(grp), INDEX(val))"),
        QueryResult::Ok { affected: 0 }
    );
    for i in 0..20 {
        assert_eq!(
            c.query(&format!(
                "INSERT INTO nums VALUES ({i}, 'g{}', {i}.0, 'n{}')",
                i % 4,
                i % 5
            )),
            QueryResult::Ok { affected: 1 }
        );
    }

    // 全表扫: note 无索引 (之前会报错, 现在 fallback)
    assert_eq!(c.ids("SELECT id FROM nums WHERE note = 'n2'").len(), 4);
    // 无 WHERE 全表扫 + LIMIT 下推
    assert_eq!(c.ids("SELECT id FROM nums").len(), 20);
    assert_eq!(c.ids("SELECT id FROM nums LIMIT 5").len(), 5);
    // COUNT(*)
    assert_eq!(
        c.query("SELECT COUNT(*) FROM nums"),
        QueryResult::Rows(vec![vec![Some("20".into())]])
    );
    assert_eq!(
        c.query("SELECT COUNT(*) FROM nums WHERE grp = 'g1'"),
        QueryResult::Rows(vec![vec![Some("5".into())]])
    );
    assert_eq!(
        c.query("SELECT COUNT(*) FROM nums WHERE id = 3"),
        QueryResult::Rows(vec![vec![Some("1".into())]])
    );
    assert_eq!(
        c.query("SELECT COUNT(*) FROM nums WHERE id = 99"),
        QueryResult::Rows(vec![vec![Some("0".into())]])
    );
    // ORDER BY DESC + LIMIT
    assert_eq!(
        c.ids("SELECT id FROM nums ORDER BY val DESC LIMIT 3"),
        vec!["19", "18", "17"]
    );
    // ORDER BY ASC + OFFSET
    assert_eq!(
        c.ids("SELECT id FROM nums ORDER BY val ASC LIMIT 3 OFFSET 5"),
        vec!["5", "6", "7"]
    );
    // 多列排序: grp asc, val desc
    assert_eq!(
        c.ids("SELECT id FROM nums ORDER BY grp ASC, val DESC LIMIT 2"),
        vec!["16", "12"],
        "g0 组内 val 降序"
    );
    // IN (索引列, [min,max] 界 + 残余精确)
    assert_eq!(c.ids("SELECT id FROM nums WHERE grp IN ('g1', 'g3')").len(), 10);
    // BETWEEN (desugar 成闭界)
    assert_eq!(c.ids("SELECT id FROM nums WHERE val BETWEEN 5 AND 8").len(), 4);
    // != (残余)
    assert_eq!(c.ids("SELECT id FROM nums WHERE grp = 'g1' AND val != 5").len(), 4);
    // LIKE 前缀 (索引列 → 范围; 无索引列 → 全表扫)
    assert_eq!(c.ids("SELECT id FROM nums WHERE grp LIKE 'g%'").len(), 20);
    assert_eq!(c.ids("SELECT id FROM nums WHERE note LIKE 'n1%'").len(), 4);
    assert_eq!(c.query("SELECT * FROM nums WHERE note LIKE '%x'").err_code(), Some(1105));

    // ⭐ S1+S2 组合: 无 WHERE 的 UPDATE/DELETE (全表扫 phase1)
    assert_eq!(
        c.query("UPDATE nums SET note = 'flat' WHERE val >= 15"),
        QueryResult::Ok { affected: 5 }
    );
    assert_eq!(
        c.query("SELECT COUNT(*) FROM nums WHERE note = 'flat'"),
        QueryResult::Rows(vec![vec![Some("5".into())]])
    );
    assert_eq!(c.query("DELETE FROM nums"), QueryResult::Ok { affected: 20 });
    assert_eq!(
        c.query("SELECT COUNT(*) FROM nums"),
        QueryResult::Rows(vec![vec![Some("0".into())]])
    );

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ S3: 方言别名 + USE / DESCRIBE / SET / version() stub.
#[test]
fn mysql_dialect_and_tools() {
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");

    // PG 风格类型: DOUBLE PRECISION / VARCHAR(n) / BYTEA / BOOLEAN
    assert_eq!(
        c.query("CREATE TABLE dlg (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL UNIQUE, w DOUBLE PRECISION, ok BOOLEAN, bin BYTEA)"),
        QueryResult::Ok { affected: 0 }
    );
    assert_eq!(
        c.query("INSERT INTO dlg VALUES (1, 'a', 1.5, 1, 'zz')"),
        QueryResult::Ok { affected: 1 }
    );
    // DESCRIBE: Field/Type/Null/Key
    let r = c.query("DESCRIBE dlg");
    let QueryResult::Rows(rows) = &r else { panic!("{r:?}") };
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0][0].as_deref(), Some("id"));
    assert_eq!(rows[0][3].as_deref(), Some("PRI"));
    assert_eq!(rows[1][3].as_deref(), Some("UNI"));
    assert_eq!(rows[1][2].as_deref(), Some("NO"), "UNIQUE 隐含 NOT NULL");
    assert_eq!(rows[3][1].as_deref(), Some("bigint"), "BOOLEAN → I64");

    // SET / SELECT version() stub
    assert_eq!(c.query("SET NAMES utf8mb4"), QueryResult::Ok { affected: 0 });
    let r = c.query("SELECT version()");
    let QueryResult::Rows(rows) = &r else { panic!("{r:?}") };
    assert!(rows[0][0].as_deref().unwrap().contains("NexusDB"));

    // USE: 未知库报 1049; 默认库 (app) 可切
    assert_eq!(c.query("USE nodb").err_code(), Some(1049));
    assert_eq!(c.query("USE app"), QueryResult::Ok { affected: 0 });
    assert_eq!(
        c.query("SELECT name FROM dlg WHERE id = 1"),
        QueryResult::Rows(vec![vec![Some("a".into())]]),
        "USE app 后原表仍可见 (同库)"
    );

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

// ===== ⭐ P2: COM_STMT_* (预处理语句, 二进制协议) =====

/// execute 参数 (客户端侧).
enum MyParam {
    I64(i64),
    F64(f64),
    Str(Vec<u8>),
    Null,
}

impl MyConn {
    /// COM_STMT_PREPARE → (stmt_id, num_params).
    fn stmt_prepare(&mut self, sql: &str) -> Result<(u32, u16), u16> {
        let mut p = vec![0x16u8];
        p.extend_from_slice(sql.as_bytes());
        self.stream.write_all(&my::write_packet(0, &p)).unwrap();
        let (_, f) = self.read_frame();
        if f[0] == 0xFF {
            return Err(u16::from_le_bytes([f[1], f[2]]));
        }
        assert_eq!(f[0], 0x00);
        let stmt_id = u32::from_le_bytes([f[1], f[2], f[3], f[4]]);
        let num_cols = u16::from_le_bytes([f[5], f[6]]);
        let num_params = u16::from_le_bytes([f[7], f[8]]);
        assert_eq!(num_cols, 0, "prepare 报告 num_columns=0");
        // 参数定义包 × n + EOF
        if num_params > 0 {
            for _ in 0..num_params {
                self.read_frame();
            }
            let (_, eof) = self.read_frame();
            assert_eq!(eof[0], 0xFE);
        }
        Ok((stmt_id, num_params))
    }

    /// COM_STMT_EXECUTE → 二进制结果集解析 / OK affected / Err.
    fn stmt_execute(&mut self, stmt_id: u32, params: &[MyParam]) -> QueryResult {
        let mut p = vec![0x17u8];
        p.extend_from_slice(&stmt_id.to_le_bytes());
        p.push(0); // flags
        p.extend_from_slice(&1u32.to_le_bytes()); // iteration
        if !params.is_empty() {
            let mut bitmap = vec![0u8; params.len().div_ceil(8)];
            for (i, v) in params.iter().enumerate() {
                if matches!(v, MyParam::Null) {
                    bitmap[i / 8] |= 1 << (i % 8);
                }
            }
            p.extend_from_slice(&bitmap);
            p.push(1); // new_params_bound
            for v in params {
                let ty: u8 = match v {
                    MyParam::I64(_) => 0x08,
                    MyParam::F64(_) => 0x05,
                    MyParam::Str(_) => 0xFD,
                    MyParam::Null => 0x06,
                };
                p.push(ty);
                p.push(0);
            }
            for v in params {
                match v {
                    MyParam::I64(x) => p.extend_from_slice(&x.to_le_bytes()),
                    MyParam::F64(x) => p.extend_from_slice(&x.to_le_bytes()),
                    MyParam::Str(s) => {
                        assert!(s.len() < 251);
                        p.push(s.len() as u8);
                        p.extend_from_slice(s);
                    }
                    MyParam::Null => {}
                }
            }
        }
        self.stream.write_all(&my::write_packet(0, &p)).unwrap();
        // 响应: OK / ERR / 二进制结果集
        let (_, f) = self.read_frame();
        match f[0] {
            0x00 => QueryResult::Ok { affected: f[1] as u64 }, // lenenc 1B 快路径
            0xFF => QueryResult::Err {
                code: u16::from_le_bytes([f[1], f[2]]),
                msg: String::from_utf8_lossy(&f[9..]).into_owned(),
            },
            n => {
                let ncols = n as usize;
                // 列定义: 取每列 type 字节 (倒数第 5? 固定布局末尾: type,flags2,dec1,filler2)
                let mut types = Vec::with_capacity(ncols);
                for _ in 0..ncols {
                    let (_, c) = self.read_frame();
                    types.push(c[c.len() - 6]); // [type][flags u16][dec][filler u16]
                }
                let (_, eof) = self.read_frame();
                assert_eq!(eof[0], 0xFE);
                let mut rows = Vec::new();
                loop {
                    let (_, r) = self.read_frame();
                    if r[0] == 0xFE && r.len() < 9 {
                        break;
                    }
                    assert_eq!(r[0], 0x00, "binary row header");
                    let bl = (ncols + 7 + 2) / 8;
                    let bitmap = &r[1..1 + bl];
                    let mut pos = 1 + bl;
                    let mut row = Vec::with_capacity(ncols);
                    for (i, &ty) in types.iter().enumerate() {
                        let bit = i + 2;
                        if bitmap[bit / 8] & (1 << (bit % 8)) != 0 {
                            row.push(None);
                            continue;
                        }
                        match ty {
                            8 => {
                                let v = i64::from_le_bytes(r[pos..pos + 8].try_into().unwrap());
                                pos += 8;
                                row.push(Some(v.to_string()));
                            }
                            5 => {
                                let v = f64::from_le_bytes(r[pos..pos + 8].try_into().unwrap());
                                pos += 8;
                                row.push(Some(format!("{v}")));
                            }
                            _ => {
                                let l = r[pos] as usize; // 短串 lenenc 快路径
                                pos += 1;
                                row.push(Some(
                                    String::from_utf8_lossy(&r[pos..pos + l]).into_owned(),
                                ));
                                pos += l;
                            }
                        }
                    }
                    rows.push(row);
                }
                QueryResult::Rows(rows)
            }
        }
    }

    fn stmt_close(&mut self, stmt_id: u32) {
        let mut p = vec![0x19u8];
        p.extend_from_slice(&stmt_id.to_le_bytes());
        self.stream.write_all(&my::write_packet(0, &p)).unwrap();
        // 无响应
    }
}

/// ⭐ P2: prepare/execute 全流程 — 各类型参数/NULL/二进制结果集/复用/关闭.
#[test]
fn mysql_prepared_statements() {
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");

    assert_eq!(
        c.query("CREATE TABLE ps (id INT PRIMARY KEY, name TEXT NOT NULL, score DOUBLE, INDEX(name))"),
        QueryResult::Ok { affected: 0 }
    );
    // prepare INSERT (3 参数)
    let (ins, n) = c.stmt_prepare("INSERT INTO ps VALUES (?, ?, ?)").unwrap();
    assert_eq!(n, 3);
    // execute × N 复用 (含 NULL / 数值 / 字符串)
    for i in 0..10i64 {
        let r = c.stmt_execute(
            ins,
            &[
                MyParam::I64(i),
                MyParam::Str(format!("n{}", i % 3).into_bytes()),
                if i == 5 { MyParam::Null } else { MyParam::F64(i as f64 + 0.5) },
            ],
        );
        assert_eq!(r, QueryResult::Ok { affected: 1 }, "insert {i}");
    }
    // prepare SELECT (索引等值参数)
    let (sel, n) = c.stmt_prepare("SELECT id, name, score FROM ps WHERE name = ? ORDER BY id").unwrap();
    assert_eq!(n, 1);
    let r = c.stmt_execute(sel, &[MyParam::Str(b"n1".to_vec())]);
    let QueryResult::Rows(rows) = &r else { panic!("{r:?}") };
    assert_eq!(rows.len(), 3, "n1 → id 1,4,7");
    assert_eq!(rows[0][0].as_deref(), Some("1"));
    // pk 点查 + NULL 二进制渲染
    let (pk, _) = c.stmt_prepare("SELECT score FROM ps WHERE id = ?").unwrap();
    let r = c.stmt_execute(pk, &[MyParam::I64(5)]);
    assert_eq!(r, QueryResult::Rows(vec![vec![None]]), "NULL → bitmap 位");
    let r = c.stmt_execute(pk, &[MyParam::I64(3)]);
    assert_eq!(r, QueryResult::Rows(vec![vec![Some("3.5".into())]]));
    // prepared UPDATE / DELETE (affected)
    let (upd, _) = c.stmt_prepare("UPDATE ps SET score = ? WHERE name = ?").unwrap();
    let r = c.stmt_execute(upd, &[MyParam::F64(9.0), MyParam::Str(b"n2".to_vec())]);
    assert_eq!(r, QueryResult::Ok { affected: 3 });
    // 文本协议交叉验证 (同连接混用 COM_QUERY)
    assert_eq!(
        c.query("SELECT COUNT(*) FROM ps WHERE score = 9"),
        QueryResult::Rows(vec![vec![Some("3".into())]]),
        "n2 组 3 行被 UPDATE"
    );
    // 错误面: 未知 stmt_id / prepare 语法错
    let r = c.stmt_execute(9999, &[]);
    assert!(matches!(r, QueryResult::Err { code: 1243, .. }), "{r:?}");
    assert!(c.stmt_prepare("SELEKT ?").is_err());
    // CLOSE 后 execute 报错
    c.stmt_close(sel);
    let r = c.stmt_execute(sel, &[MyParam::Str(b"n1".to_vec())]);
    assert!(matches!(r, QueryResult::Err { code: 1243, .. }));

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

// ===== ⭐ ORM 性能专项 Phase A: 归因基准 (release + --ignored 跑) =====

/// text vs prepared 服务端净差 (同环境同客户端, 剥离 Python 驱动开销).
/// `cargo test --release -p network --test sql_e2e -- --ignored bench_ --nocapture`
#[test]
#[ignore]
fn bench_text_vs_prepared() {
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");
    c.query("CREATE TABLE bp (id INT PRIMARY KEY, name TEXT NOT NULL, INDEX(name))");
    for i in 0..100 {
        c.query(&format!("INSERT INTO bp VALUES ({i}, 'n{}')", i % 10));
    }
    let n = 20000u32;
    // text
    let t0 = std::time::Instant::now();
    for i in 0..n {
        let r = c.query(&format!("SELECT name FROM bp WHERE id = {}", i % 100));
        assert!(matches!(r, QueryResult::Rows(_)));
    }
    let text = t0.elapsed();
    // prepared
    let (sid, _) = c.stmt_prepare("SELECT name FROM bp WHERE id = ?").unwrap();
    let t0 = std::time::Instant::now();
    for i in 0..n {
        let r = c.stmt_execute(sid, &[MyParam::I64((i % 100) as i64)]);
        assert!(matches!(r, QueryResult::Rows(_)));
    }
    let prep = t0.elapsed();
    println!(
        "text:     {:>7.0} qps ({:.1}us/op)",
        n as f64 / text.as_secs_f64(),
        text.as_micros() as f64 / n as f64
    );
    println!(
        "prepared: {:>7.0} qps ({:.1}us/op)  ratio {:.2}x",
        n as f64 / prep.as_secs_f64(),
        prep.as_micros() as f64 / n as f64,
        text.as_secs_f64() / prep.as_secs_f64()
    );
    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

/// 并发饱和曲线: 1/2/4/8/16 连接 pk 点查 (单 worker 上限标定).
#[test]
#[ignore]
fn bench_concurrency_curve() {
    bench_concurrency_impl(1);
}

/// ⭐ ORM-B3: 4 worker 并发曲线 (对照单 worker).
#[test]
#[ignore]
fn bench_concurrency_curve_4w() {
    bench_concurrency_impl(4);
}

fn bench_concurrency_impl(workers: usize) {
    let (server, mgr) = start_sql_server_n(workers);
    let mut c = MyConn::handshake_login(&server, "");
    c.query("CREATE TABLE bc (id INT PRIMARY KEY, name TEXT NOT NULL)");
    for i in 0..100 {
        c.query(&format!("INSERT INTO bc VALUES ({i}, 'v')"));
    }
    drop(c);
    println!("--- {workers} worker(s) ---");
    for conns in [1usize, 2, 4, 8, 16] {
        let per = 40000 / conns;
        let t0 = std::time::Instant::now();
        let handles: Vec<_> = (0..conns)
            .map(|_| {
                let addr = server.local_addr();
                std::thread::spawn(move || {
                    let mut c = MyConn::connect_addr(addr);
                    c.login("");
                    for i in 0..per {
                        c.query(&format!("SELECT name FROM bc WHERE id = {}", i % 100));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let el = t0.elapsed();
        println!(
            "{conns:>2} conns: {:>7.0} qps",
            (per * conns) as f64 / el.as_secs_f64()
        );
    }
    server.shutdown().unwrap();
    drop(mgr);
}

// ===== ⭐ ORM-B3: 多 worker 正确性 (进程级路由缓存共享) =====

fn start_sql_server_n(workers: usize) -> (NetworkServer, Arc<ShardManager>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let opts = ShardManagerOptions {
        num_shards: 3,
        block_root: tmp.path().to_path_buf(),
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 4,
        reply_bus_count: Some(workers.max(3)),
        wal_mode: Default::default(),
    };
    let mgr = Arc::new(ShardManager::open(opts).expect("open mgr"));
    mgr.create_db("app").expect("create db");
    mgr.create_table("app", "kv").expect("create table");
    std::mem::forget(tmp);
    let cfg = NetworkServerConfig {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        shard_manager: mgr.clone(),
        worker_count: workers,
        default_db: "app".to_string(),
        default_table: "kv".to_string(),
        inbox_capacity: 64,
        protocol: ProtocolKind::Sql,
        limits: KvLimits::default(),
        auth_password: None,
        worker_id_base: 0,
        sql_shared: network::new_sql_shared(),
    };
    (NetworkServer::start(cfg).expect("start server"), mgr)
}

/// 2 worker: 连接轮询落不同 worker — CREATE(A) → INSERT 分散(A/B) →
/// 等值 SELECT 两侧完整 (进程级 bloom 无假阴性) + DROP/重建 epoch 失效.
#[test]
fn multi_worker_route_cache_consistency() {
    let (server, mgr) = start_sql_server_n(2);
    // c1/c2 轮询分配到 worker 0/1
    let mut c1 = MyConn::handshake_login(&server, "");
    let mut c2 = MyConn::handshake_login(&server, "");

    assert_eq!(
        c1.query("CREATE TABLE mw (id INT PRIMARY KEY, tag TEXT NOT NULL, INDEX(tag))"),
        QueryResult::Ok { affected: 0 }
    );
    // INSERT 分散两 worker (奇偶交替)
    for i in 0..30 {
        let c = if i % 2 == 0 { &mut c1 } else { &mut c2 };
        assert_eq!(
            c.query(&format!("INSERT INTO mw VALUES ({i}, 't{}')", i % 5)),
            QueryResult::Ok { affected: 1 },
            "insert {i}"
        );
    }
    // 等值完整性: 两 worker 各查全部 tag (若 bloom per-worker 必假阴性漏行)
    for tag in 0..5 {
        let q = format!("SELECT id FROM mw WHERE tag = 't{tag}'");
        assert_eq!(c1.ids(&q).len(), 6, "worker0 tag {tag}");
        assert_eq!(c2.ids(&q).len(), 6, "worker1 tag {tag}");
    }
    // miss 零任务短路两侧生效
    assert_eq!(c1.query("SELECT * FROM mw WHERE tag = 'ghost'"), QueryResult::Rows(vec![]));
    assert_eq!(c2.query("SELECT * FROM mw WHERE tag = 'ghost'"), QueryResult::Rows(vec![]));

    // DROP(经 c2/worker1) + 重建换 schema → c1(worker0) 的陈旧 schema 被
    // epoch 失效, 用新 schema 正确解码
    assert_eq!(c2.query("DROP TABLE mw"), QueryResult::Ok { affected: 0 });
    assert_eq!(
        c2.query("CREATE TABLE mw (id INT PRIMARY KEY, tag TEXT NOT NULL, extra DOUBLE, INDEX(tag))"),
        QueryResult::Ok { affected: 0 }
    );
    assert_eq!(
        c2.query("INSERT INTO mw VALUES (1, 'x', 9.5)"),
        QueryResult::Ok { affected: 1 }
    );
    // c1 若还用旧 2 列 schema 会解码错/列数错 — epoch 应已失效重拉
    assert_eq!(
        c1.query("SELECT extra FROM mw WHERE id = 1"),
        QueryResult::Rows(vec![vec![Some("9.5".into())]]),
        "worker0 epoch 失效后用新 schema"
    );
    assert_eq!(c1.ids("SELECT id FROM mw WHERE tag = 'x'"), vec!["1"]);

    drop(c1);
    drop(c2);
    server.shutdown().unwrap();
    drop(mgr);
}

// ===== ⭐ 事务 v1 (F61): BEGIN/COMMIT/ROLLBACK =====

/// 事务全流程: 可见性 / RYOW / ROLLBACK / DDL 拒绝 / unique 冲突零部分应用.
#[test]
fn mysql_transactions() {
    let (server, mgr) = start_sql_server(None);
    let mut c1 = MyConn::handshake_login(&server, "");
    let mut c2 = MyConn::handshake_login(&server, "");
    c1.query("CREATE TABLE tx (id INT PRIMARY KEY, name TEXT NOT NULL, mail TEXT UNIQUE)");

    // --- COMMIT 前另一连接不可见, RYOW 自见 ---
    assert_eq!(c1.query("BEGIN"), QueryResult::Ok { affected: 0 });
    for i in 0..5 {
        assert_eq!(
            c1.query(&format!("INSERT INTO tx VALUES ({i}, 'n{i}', 'm{i}@x')")),
            QueryResult::Ok { affected: 1 }
        );
    }
    // 另一连接: 不可见
    assert_eq!(c2.query("SELECT * FROM tx WHERE id = 3"), QueryResult::Rows(vec![]));
    // RYOW: 本连接 pk 点查见未提交行
    assert_eq!(c1.ids("SELECT id FROM tx WHERE id = 3"), vec!["3"]);
    // DDL 在事务中拒绝
    assert!(matches!(
        c1.query("CREATE TABLE t2 (id INT PRIMARY KEY)"),
        QueryResult::Err { .. }
    ));
    // COMMIT → 双方可见
    assert_eq!(c1.query("COMMIT"), QueryResult::Ok { affected: 5 });
    assert_eq!(c2.ids("SELECT id FROM tx WHERE id = 3"), vec!["3"]);
    assert_eq!(c2.ids("SELECT id FROM tx ORDER BY id").len(), 5);

    // --- ROLLBACK 丢弃 (含 UPDATE/DELETE pk 混合) ---
    c1.query("BEGIN");
    c1.query("INSERT INTO tx VALUES (100, 'ghost', 'g@x')");
    c1.query("UPDATE tx SET name = 'changed' WHERE id = 0");
    c1.query("DELETE FROM tx WHERE id = 1");
    assert_eq!(c1.query("ROLLBACK"), QueryResult::Ok { affected: 0 });
    assert_eq!(c2.query("SELECT * FROM tx WHERE id = 100"), QueryResult::Rows(vec![]));
    assert_eq!(
        c2.query("SELECT name FROM tx WHERE id = 0"),
        QueryResult::Rows(vec![vec![Some("n0".into())]])
    );
    assert_eq!(c2.ids("SELECT id FROM tx WHERE id = 1"), vec!["1"]);

    // --- 事务中 UPDATE/DELETE 提交生效 ---
    c1.query("BEGIN");
    c1.query("UPDATE tx SET name = 'upd' WHERE id = 2");
    c1.query("DELETE FROM tx WHERE id = 4");
    assert_eq!(c1.query("COMMIT"), QueryResult::Ok { affected: 2 });
    assert_eq!(
        c2.query("SELECT name FROM tx WHERE id = 2"),
        QueryResult::Rows(vec![vec![Some("upd".into())]])
    );
    assert_eq!(c2.query("SELECT * FROM tx WHERE id = 4"), QueryResult::Rows(vec![]));

    // --- unique 冲突: 同 shard 时 commit 报错且零部分应用; 不同 shard 时
    // 漏检 (O3 既有跨 shard 唯一性 gap, 非事务引入 — 稳定验证见
    // mysql_txn_unique_single_shard) ---
    c1.query("BEGIN");
    c1.query("INSERT INTO tx VALUES (200, 'a', 'dup@x')");
    c1.query("INSERT INTO tx VALUES (201, 'b', 'm0@x')"); // 与 id=0 的 mail 冲突
    if let QueryResult::Err { ref msg, .. } = c1.query("COMMIT") {
        assert!(msg.contains("duplicate"), "{msg}");
        assert_eq!(c2.query("SELECT * FROM tx WHERE id = 201"), QueryResult::Rows(vec![]));
    }

    // --- 批内自冲突 (两行同 unique 值) ---
    c1.query("BEGIN");
    c1.query("INSERT INTO tx VALUES (300, 'x', 'same@x')");
    c1.query("INSERT INTO tx VALUES (301, 'y', 'same@x')");
    let r = c1.query("COMMIT");
    // 同 shard 时预检拒; 跨 shard 时盘上探测各自过 (v1 gap) — 至少不 panic
    if matches!(r, QueryResult::Err { .. }) {
        assert_eq!(c2.query("SELECT * FROM tx WHERE id = 300"), QueryResult::Rows(vec![]));
    }

    // --- 重复 BEGIN 忽略 + 空事务 COMMIT ---
    c1.query("BEGIN");
    assert_eq!(c1.query("BEGIN"), QueryResult::Ok { affected: 0 });
    assert_eq!(c1.query("COMMIT"), QueryResult::Ok { affected: 0 });

    // --- 断连隐式回滚 ---
    c1.query("BEGIN");
    c1.query("INSERT INTO tx VALUES (400, 'drop', 'd@x')");
    drop(c1);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(c2.query("SELECT * FROM tx WHERE id = 400"), QueryResult::Rows(vec![]));

    drop(c2);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ 事务 v1: 单 shard 集群下 unique 冲突必检出 + 零部分应用 (先验后写).
#[test]
fn mysql_txn_unique_single_shard() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let opts = ShardManagerOptions {
        num_shards: 1, // 单 shard: unique 探测无跨 shard 盲区
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
    let cfg = NetworkServerConfig {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        shard_manager: mgr.clone(),
        worker_count: 1,
        default_db: "app".to_string(),
        default_table: "kv".to_string(),
        inbox_capacity: 64,
        protocol: ProtocolKind::Sql,
        limits: KvLimits::default(),
        auth_password: None,
        worker_id_base: 0,
        sql_shared: network::new_sql_shared(),
    };
    let server = NetworkServer::start(cfg).expect("start server");
    let mut c = MyConn::handshake_login(&server, "");
    c.query("CREATE TABLE u1 (id INT PRIMARY KEY, mail TEXT UNIQUE)");
    assert_eq!(c.query("INSERT INTO u1 VALUES (1, 'a@x')"), QueryResult::Ok { affected: 1 });

    // 盘上冲突: 预检拒 + 零部分应用 (2 号行也不落)
    c.query("BEGIN");
    c.query("INSERT INTO u1 VALUES (2, 'clean@x')");
    c.query("INSERT INTO u1 VALUES (3, 'a@x')"); // 与 id=1 冲突
    let r = c.query("COMMIT");
    assert!(matches!(r, QueryResult::Err { ref msg, .. } if msg.contains("duplicate")), "{r:?}");
    assert_eq!(c.query("SELECT * FROM u1 WHERE id = 2"), QueryResult::Rows(vec![]), "零部分应用");
    assert_eq!(c.query("SELECT * FROM u1 WHERE id = 3"), QueryResult::Rows(vec![]));

    // 批内自冲突: 预检拒
    c.query("BEGIN");
    c.query("INSERT INTO u1 VALUES (4, 'same@x')");
    c.query("INSERT INTO u1 VALUES (5, 'same@x')");
    let r = c.query("COMMIT");
    assert!(
        matches!(r, QueryResult::Err { ref msg, .. } if msg.contains("within transaction")),
        "{r:?}"
    );
    assert_eq!(c.query("SELECT * FROM u1 WHERE id = 4"), QueryResult::Rows(vec![]));

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

// ===== ⭐ 事务 v2 (F62): 隔离级别 / OCC 冲突 / READ ONLY / SAVEPOINT =====

/// 隔离级别语法 + SERIALIZABLE OCC 冲突检测 + RC 对照.
#[test]
fn mysql_isolation_levels() {
    let (server, mgr) = start_sql_server(None);
    let mut c1 = MyConn::handshake_login(&server, "");
    let mut c2 = MyConn::handshake_login(&server, "");
    c1.query("CREATE TABLE iso (id INT PRIMARY KEY, v INT)");
    c1.query("INSERT INTO iso VALUES (1, 10)");
    c1.query("INSERT INTO iso VALUES (2, 20)");

    // --- 语法全解析 (四级 + SET 变体) ---
    for s in [
        "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE",
    ] {
        assert_eq!(c1.query(s), QueryResult::Ok { affected: 0 }, "{s}");
    }
    // 复位默认
    c1.query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");

    // --- SERIALIZABLE: 不可重复读防护 (读过的行被并发改 → commit 拒) ---
    c1.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
    assert_eq!(
        c1.query("SELECT v FROM iso WHERE id = 1"),
        QueryResult::Rows(vec![vec![Some("10".into())]])
    );
    // 另一连接并发改 id=1 并提交
    assert_eq!(
        c2.query("UPDATE iso SET v = 99 WHERE id = 1"),
        QueryResult::Ok { affected: 1 }
    );
    // 本事务写 (基于已读的过期值) → commit 必须 1213
    c1.query("UPDATE iso SET v = 11 WHERE id = 1");
    let r = c1.query("COMMIT");
    assert!(
        matches!(r, QueryResult::Err { code: 1213, .. }),
        "SERIALIZABLE 冲突应回 1213: {r:?}"
    );
    // 冲突未应用: c2 的 99 保留
    assert_eq!(
        c2.query("SELECT v FROM iso WHERE id = 1"),
        QueryResult::Rows(vec![vec![Some("99".into())]])
    );
    // 重试成功 (ORM 标准路径)
    c1.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
    c1.query("SELECT v FROM iso WHERE id = 1");
    c1.query("UPDATE iso SET v = 11 WHERE id = 1");
    assert_eq!(c1.query("COMMIT"), QueryResult::Ok { affected: 1 });

    // --- RC 对照: 同场景 last-writer-wins 成功 ---
    c1.query("BEGIN"); // 默认 RC
    c1.query("SELECT v FROM iso WHERE id = 2");
    c2.query("UPDATE iso SET v = 200 WHERE id = 2");
    c1.query("UPDATE iso SET v = 21 WHERE id = 2");
    assert_eq!(c1.query("COMMIT"), QueryResult::Ok { affected: 1 }, "RC 无读集验证");

    // --- SER 读到的行未变 → commit 成功 (无假阳性) ---
    c1.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
    c1.query("SELECT v FROM iso WHERE id = 1");
    c1.query("UPDATE iso SET v = 12 WHERE id = 1");
    assert_eq!(c1.query("COMMIT"), QueryResult::Ok { affected: 1 });

    // --- 纯读 SER 事务: 无写不验证, 直接成功 ---
    c1.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
    c1.query("SELECT v FROM iso WHERE id = 1");
    c2.query("UPDATE iso SET v = 1000 WHERE id = 1");
    assert_eq!(c1.query("COMMIT"), QueryResult::Ok { affected: 0 });

    // --- READ ONLY: 写拒 1792 ---
    c1.query("BEGIN READ ONLY");
    let r = c1.query("INSERT INTO iso VALUES (3, 30)");
    assert!(matches!(r, QueryResult::Err { code: 1792, .. }), "{r:?}");
    assert_eq!(
        c1.query("SELECT v FROM iso WHERE id = 2"),
        QueryResult::Rows(vec![vec![Some("21".into())]]),
        "READ ONLY 可读"
    );
    c1.query("COMMIT");

    drop(c1);
    drop(c2);
    server.shutdown().unwrap();
    drop(mgr);
}

/// SAVEPOINT: 嵌套部分回滚 / RELEASE / 非事务报错.
#[test]
fn mysql_savepoints() {
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");
    c.query("CREATE TABLE sp (id INT PRIMARY KEY, v TEXT NOT NULL)");

    // 非事务中 SAVEPOINT 报错
    assert!(matches!(c.query("SAVEPOINT s1"), QueryResult::Err { .. }));

    c.query("BEGIN");
    c.query("INSERT INTO sp VALUES (1, 'keep')");
    c.query("SAVEPOINT s1");
    c.query("INSERT INTO sp VALUES (2, 'drop-me')");
    c.query("INSERT INTO sp VALUES (3, 'drop-me-too')");
    // 回滚到 s1: 2/3 丢弃, 1 保留
    assert_eq!(c.query("ROLLBACK TO SAVEPOINT s1"), QueryResult::Ok { affected: 0 });
    // RYOW 验证: 2 不可见, 1 可见
    assert_eq!(c.query("SELECT * FROM sp WHERE id = 2"), QueryResult::Rows(vec![]));
    assert_eq!(c.ids("SELECT id FROM sp WHERE id = 1"), vec!["1"]);
    // 继续写 + 再次回滚到同一 savepoint (PG 允许)
    c.query("INSERT INTO sp VALUES (4, 'also-drop')");
    c.query("ROLLBACK TO s1");
    // RELEASE 后名字失效
    assert_eq!(c.query("RELEASE SAVEPOINT s1"), QueryResult::Ok { affected: 0 });
    assert!(matches!(c.query("ROLLBACK TO s1"), QueryResult::Err { .. }));
    c.query("INSERT INTO sp VALUES (5, 'final')");
    assert_eq!(c.query("COMMIT"), QueryResult::Ok { affected: 2 }); // 1 + 5

    assert_eq!(c.ids("SELECT id FROM sp ORDER BY id"), vec!["1", "5"]);

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}
