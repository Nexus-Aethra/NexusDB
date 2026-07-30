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
            assert_eq!(code, 1062, "duplicate key 应映射 ER_DUP_ENTRY 1062");
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

// ===== ⭐ GROUP BY 聚合族 (F63) =====

/// 裸聚合 / GROUP BY / HAVING / ORDER BY 聚合列 / NULL 语义.
#[test]
fn mysql_group_by_aggregates() {
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");
    c.query("CREATE TABLE sales (id INT PRIMARY KEY, region TEXT NOT NULL, amt DOUBLE, qty INT, INDEX(region))");
    // region: east ×3 (amt 10/20/30, qty 1/2/NULL), west ×2 (amt 5/NULL, qty 4/5), north ×1
    let rows = [
        (1, "'east'", "10.0", "1"),
        (2, "'east'", "20.0", "2"),
        (3, "'east'", "30.0", "NULL"),
        (4, "'west'", "5.0", "4"),
        (5, "'west'", "NULL", "5"),
        (6, "'north'", "7.5", "6"),
    ];
    for (id, r, a, q) in rows {
        assert_eq!(
            c.query(&format!("INSERT INTO sales VALUES ({id}, {r}, {a}, {q})")),
            QueryResult::Ok { affected: 1 }
        );
    }

    // --- 裸聚合 (全表单桶) ---
    assert_eq!(
        c.query("SELECT COUNT(*), SUM(amt), MIN(amt), MAX(amt) FROM sales"),
        QueryResult::Rows(vec![vec![
            Some("6".into()),
            Some("72.5".into()),
            Some("5".into()),
            Some("30".into()),
        ]])
    );
    // COUNT(col) 忽略 NULL
    assert_eq!(
        c.query("SELECT COUNT(qty) FROM sales"),
        QueryResult::Rows(vec![vec![Some("5".into())]])
    );
    // AVG 输出 F64 (72.5 / 5 非 NULL amt = 14.5)
    assert_eq!(
        c.query("SELECT AVG(amt) FROM sales"),
        QueryResult::Rows(vec![vec![Some("14.5".into())]])
    );
    // 带 WHERE (索引路径) 的裸聚合
    assert_eq!(
        c.query("SELECT SUM(amt) FROM sales WHERE region = 'east'"),
        QueryResult::Rows(vec![vec![Some("60".into())]])
    );

    // --- 空表/空结果单行语义 (COUNT=0 其余 NULL) ---
    assert_eq!(
        c.query("SELECT COUNT(*), SUM(amt) FROM sales WHERE region = 'ghost'"),
        QueryResult::Rows(vec![vec![Some("0".into()), None]])
    );

    // --- GROUP BY (ORDER BY region 明确定序) ---
    assert_eq!(
        c.query("SELECT region, COUNT(*), SUM(amt) FROM sales GROUP BY region ORDER BY region"),
        QueryResult::Rows(vec![
            vec![Some("east".into()), Some("3".into()), Some("60".into())],
            vec![Some("north".into()), Some("1".into()), Some("7.5".into())],
            vec![Some("west".into()), Some("2".into()), Some("5".into())],
        ])
    );
    // ORDER BY 聚合列 DESC + LIMIT
    assert_eq!(
        c.query("SELECT region, SUM(amt) FROM sales GROUP BY region ORDER BY SUM(amt) DESC LIMIT 2"),
        QueryResult::Rows(vec![
            vec![Some("east".into()), Some("60".into())],
            vec![Some("north".into()), Some("7.5".into())],
        ])
    );
    // HAVING 过滤桶 (+ ORDER BY 定序)
    assert_eq!(
        c.query("SELECT region, COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) >= 2 ORDER BY region"),
        QueryResult::Rows(vec![
            vec![Some("east".into()), Some("3".into())],
            vec![Some("west".into()), Some("2".into())],
        ])
    );

    // --- 校验类错误 ---
    // 非聚合项不在 GROUP BY
    assert!(matches!(
        c.query("SELECT amt, COUNT(*) FROM sales GROUP BY region"),
        QueryResult::Err { .. }
    ));
    // SUM 非数值列
    assert!(matches!(c.query("SELECT SUM(region) FROM sales"), QueryResult::Err { .. }));
    // 旧 COUNT(*) 路径不回归
    assert_eq!(
        c.query("SELECT COUNT(*) FROM sales WHERE region = 'east'"),
        QueryResult::Rows(vec![vec![Some("3".into())]])
    );

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ F63 正确性修复: 事务内 UPDATE 的 RYOW (读自己的未提交改动).
/// 端到端正确性检验发现: 之前 UPDATE 缓冲后 pk 点查直通读盘, 读不到自己的改动.
#[test]
fn mysql_txn_ryow_update() {
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");
    c.query("CREATE TABLE ry (id INT PRIMARY KEY, bal INT, note TEXT)");
    c.query("INSERT INTO ry VALUES (1, 100, 'init')");
    c.query("INSERT INTO ry VALUES (2, 200, 'init')");

    c.query("BEGIN");
    // 基于已提交盘行的 UPDATE → 事务内点查须见新值 (overlay)
    c.query("UPDATE ry SET bal = 700 WHERE id = 1");
    assert_eq!(
        c.query("SELECT bal FROM ry WHERE id = 1"),
        QueryResult::Rows(vec![vec![Some("700".into())]]),
        "RYOW: UPDATE 后须见自己的改动"
    );
    // 多次 UPDATE 叠加 (后写覆盖前写, 不同列各自生效)
    c.query("UPDATE ry SET bal = 800 WHERE id = 1");
    c.query("UPDATE ry SET note = 'changed' WHERE id = 1");
    assert_eq!(
        c.query("SELECT bal, note FROM ry WHERE id = 1"),
        QueryResult::Rows(vec![vec![Some("800".into()), Some("changed".into())]]),
        "RYOW: 多次 UPDATE 叠加"
    );
    // 另一连接不可见 (未提交)
    let mut c2 = MyConn::handshake_login(&server, "");
    assert_eq!(
        c2.query("SELECT bal FROM ry WHERE id = 1"),
        QueryResult::Rows(vec![vec![Some("100".into())]]),
        "另一连接读已提交态"
    );
    // INSERT 后再 UPDATE (纯内存链) → 见最终态
    c.query("INSERT INTO ry VALUES (3, 5, 'new')");
    c.query("UPDATE ry SET bal = 50 WHERE id = 3");
    assert_eq!(
        c.query("SELECT bal, note FROM ry WHERE id = 3"),
        QueryResult::Rows(vec![vec![Some("50".into()), Some("new".into())]]),
        "RYOW: INSERT→UPDATE 链"
    );
    // UPDATE 后 DELETE → 见空
    c.query("UPDATE ry SET bal = 999 WHERE id = 2");
    c.query("DELETE FROM ry WHERE id = 2");
    assert_eq!(c.query("SELECT bal FROM ry WHERE id = 2"), QueryResult::Rows(vec![]), "RYOW: 删后见空");

    c.query("COMMIT");
    // 提交后另一连接见最终态
    assert_eq!(
        c2.query("SELECT bal, note FROM ry WHERE id = 1"),
        QueryResult::Rows(vec![vec![Some("800".into()), Some("changed".into())]])
    );
    assert_eq!(c2.query("SELECT bal FROM ry WHERE id = 2"), QueryResult::Rows(vec![]));
    assert_eq!(
        c2.query("SELECT bal FROM ry WHERE id = 3"),
        QueryResult::Rows(vec![vec![Some("50".into())]])
    );

    drop(c);
    drop(c2);
    server.shutdown().unwrap();
    drop(mgr);
}

// ===== ⭐ 全局 UNIQUE 约束 (F65) =====

/// 跨 shard 全局唯一: 不同 pk 同值必拒 (旗舰); 幂等重插; 删后重插 (懒校对自愈);
/// 事务内 / UPDATE 全局唯一列 → v1 边界拒绝.
#[test]
fn mysql_global_unique() {
    // 多 shard (默认 6) 才是 gap 场景 — start_sql_server 用默认配置
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");
    c.query("CREATE TABLE gu (id INT PRIMARY KEY, email TEXT GLOBAL UNIQUE, name TEXT)");
    assert_eq!(
        c.query("INSERT INTO gu VALUES (1, 'a@x', 'alice')"),
        QueryResult::Ok { affected: 1 }
    );

    // 旗舰: 不同 pk (落不同 shard) 同 email → 必拒 1062
    let r = c.query("INSERT INTO gu VALUES (2, 'a@x', 'bob')");
    assert!(
        matches!(r, QueryResult::Err { code: 1062, .. }),
        "跨 shard 同 email 应拒 1062: {r:?}"
    );
    // 遍历多个 pk, 全部拒 (确保不是碰巧同 shard)
    for id in 3..12 {
        let r = c.query(&format!("INSERT INTO gu VALUES ({id}, 'a@x', 'x')"));
        assert!(matches!(r, QueryResult::Err { code: 1062, .. }), "id={id}: {r:?}");
    }
    // 不同 email 各自成功
    for id in 20..30 {
        assert_eq!(
            c.query(&format!("INSERT INTO gu VALUES ({id}, 'e{id}@x', 'n')")),
            QueryResult::Ok { affected: 1 },
            "id={id}"
        );
    }

    // 幂等重插同 (pk, email)
    assert_eq!(
        c.query("INSERT INTO gu VALUES (1, 'a@x', 'alice2')"),
        QueryResult::Ok { affected: 1 },
        "同 pk 同 email 幂等"
    );

    // 删后重插同 email (懒校对: 旧 COMMITTED 坑回查行已删 → 抢占)
    assert_eq!(c.query("DELETE FROM gu WHERE id = 1"), QueryResult::Ok { affected: 1 });
    assert_eq!(
        c.query("INSERT INTO gu VALUES (99, 'a@x', 'new-owner')"),
        QueryResult::Ok { affected: 1 },
        "删后同 email 应可被新 pk 占用 (懒校对自愈)"
    );
    // 现在 99 持有 a@x, 再插又该拒
    let r = c.query("INSERT INTO gu VALUES (100, 'a@x', 'z')");
    assert!(matches!(r, QueryResult::Err { code: 1062, .. }), "{r:?}");

    // v1 边界: 事务内写全局唯一表 → 拒
    c.query("BEGIN");
    let r = c.query("INSERT INTO gu VALUES (200, 'txn@x', 't')");
    assert!(matches!(r, QueryResult::Err { .. }), "事务内全局唯一写应拒: {r:?}");
    c.query("ROLLBACK");
    // v1 边界: UPDATE 全局唯一列 → 拒
    let r = c.query("UPDATE gu SET email = 'moved@x' WHERE id = 99");
    assert!(matches!(r, QueryResult::Err { .. }), "UPDATE 全局唯一列应拒: {r:?}");
    // 但 UPDATE 非全局唯一列 OK
    assert_eq!(
        c.query("UPDATE gu SET name = 'renamed' WHERE id = 99"),
        QueryResult::Ok { affected: 1 }
    );

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

/// 普通 UNIQUE (非 global) 行为不变: 单 shard 拒, 跨 shard best-effort (回归保护).
#[test]
fn mysql_plain_unique_unchanged() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let opts = ShardManagerOptions {
        num_shards: 1, // 单 shard: 普通 UNIQUE 必拒
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
    c.query("CREATE TABLE p (id INT PRIMARY KEY, email TEXT UNIQUE)");
    c.query("INSERT INTO p VALUES (1, 'a@x')");
    let r = c.query("INSERT INTO p VALUES (2, 'a@x')");
    assert!(matches!(r, QueryResult::Err { code: 1062, .. }), "普通 UNIQUE 单 shard 拒: {r:?}");
    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

// ===== ⭐ information_schema 系统表 (F66) =====

/// 系统表虚拟化: tables/columns/key_column_usage/schemata + 投影/过滤/大小写/未知回空.
#[test]
fn mysql_information_schema() {
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");
    c.query("CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT, age INT)");
    c.query("CREATE TABLE orders (id INT PRIMARY KEY, amt DOUBLE)");

    // information_schema.tables (default 库名是 app? start_sql_server 用 "app")
    // 用 mysql_sql_full_flow 同款: 默认库名从 server 配置; 这里不带 table_schema 过滤取全部
    let ids = c.ids("SELECT table_name FROM information_schema.tables ORDER BY table_name");
    assert!(ids.contains(&"orders".to_string()) && ids.contains(&"users".to_string()), "{ids:?}");

    // columns: 列名 + 类型 + nullable (UNIQUE 隐含 NOT NULL)
    let r = c.query("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'users' ORDER BY ordinal_position");
    assert_eq!(
        r,
        QueryResult::Rows(vec![
            vec![Some("id".into()), Some("bigint".into()), Some("NO".into())],
            vec![Some("email".into()), Some("text".into()), Some("NO".into())],
            vec![Some("name".into()), Some("text".into()), Some("YES".into())],
            vec![Some("age".into()), Some("bigint".into()), Some("YES".into())],
        ]),
        "columns 元数据"
    );

    // key_column_usage: pk + unique
    let r = c.query("SELECT column_name, constraint_name FROM information_schema.key_column_usage WHERE table_name = 'users' ORDER BY column_name");
    assert_eq!(
        r,
        QueryResult::Rows(vec![
            vec![Some("email".into()), Some("uniq_email".into())],
            vec![Some("id".into()), Some("PRIMARY".into())],
        ]),
        "key_column_usage"
    );

    // schemata: 列出 db (至少含默认库)
    let schemas = c.ids("SELECT schema_name FROM information_schema.schemata");
    assert!(!schemas.is_empty(), "schemata 非空");

    // 大小写不敏感 catalog/表名
    let ids2 = c.ids("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ORDER BY table_name");
    assert_eq!(ids, ids2, "大小写不敏感");

    // WHERE 过滤精确到表
    let r = c.query("SELECT table_name FROM information_schema.columns WHERE table_name = 'orders' AND column_name = 'amt'");
    assert_eq!(r, QueryResult::Rows(vec![vec![Some("orders".into())]]));

    // 未知系统表 → 空结果 (不报错)
    assert_eq!(c.query("SELECT * FROM information_schema.routines"), QueryResult::Rows(vec![]));

    // 普通表查询不受影响
    c.query("INSERT INTO orders VALUES (1, 9.9)");
    assert_eq!(c.ids("SELECT id FROM orders"), vec!["1"]);

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

/// SHOW TABLES / SHOW COLUMNS / SHOW CREATE TABLE / SHOW DATABASES + 反引号标识符.
#[test]
fn mysql_show_commands() {
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");
    c.query("CREATE TABLE products (id INT PRIMARY KEY, sku TEXT UNIQUE, price DOUBLE)");
    c.query("CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)");

    // SHOW TABLES (单列)
    let mut t = c.ids("SHOW TABLES");
    t.sort();
    assert_eq!(t, vec!["customers".to_string(), "products".to_string()]);

    // SHOW FULL TABLES (反引号库名; 忽略库名走 current_db)
    let r = c.query("SHOW FULL TABLES FROM `default`");
    if let QueryResult::Rows(rows) = &r {
        assert_eq!(rows.len(), 2, "两表");
        assert!(rows.iter().all(|row| row[1] == Some("BASE TABLE".into())), "{rows:?}");
    } else {
        panic!("expected rows: {r:?}");
    }

    // SHOW COLUMNS FROM t: Field/Type/Null/Key
    let r = c.query("SHOW COLUMNS FROM products");
    assert_eq!(
        r,
        QueryResult::Rows(vec![
            vec![Some("id".into()), Some("bigint".into()), Some("NO".into()), Some("PRI".into()), None, Some("".into())],
            vec![Some("sku".into()), Some("text".into()), Some("NO".into()), Some("UNI".into()), None, Some("".into())],
            vec![Some("price".into()), Some("double".into()), Some("YES".into()), Some("".into()), None, Some("".into())],
        ]),
        "SHOW COLUMNS"
    );

    // SHOW CREATE TABLE: 两列 Table / Create Table, DDL 含列与键
    let r = c.query("SHOW CREATE TABLE products");
    if let QueryResult::Rows(rows) = &r {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("products".into()));
        let ddl = rows[0][1].clone().unwrap();
        assert!(ddl.contains("CREATE TABLE `products`"), "{ddl}");
        assert!(ddl.contains("`id` int NOT NULL"), "{ddl}");
        assert!(ddl.contains("PRIMARY KEY (`id`)"), "{ddl}");
        assert!(ddl.contains("UNIQUE KEY `sku`"), "{ddl}");
    } else {
        panic!("expected rows: {r:?}");
    }

    // SHOW DATABASES (单列 Database)
    let dbs = c.ids("SHOW DATABASES");
    assert!(!dbs.is_empty(), "至少默认库");

    // 未知 SHOW → 空 (不报错)
    assert_eq!(c.query("SHOW STATUS"), QueryResult::Rows(vec![]));

    // SELECT @@var 系统变量 stub
    let r = c.query("SELECT @@transaction_isolation");
    assert_eq!(r, QueryResult::Rows(vec![vec![Some("READ-COMMITTED".into())]]));

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

// ===== ⭐ 两表 hash JOIN (F67) =====

/// INNER/LEFT 等值 JOIN + 投影/谓词下推 + 重名列限定 + ORDER/LIMIT + 单表零回归.
#[test]
fn mysql_join_two_tables() {
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");
    c.query("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)");
    c.query("CREATE TABLE orders (id INT PRIMARY KEY, uid INT, amt DOUBLE)");
    for i in 1..=4 {
        c.query(&format!("INSERT INTO users VALUES ({i}, 'u{i}', {})", 20 + i));
    }
    c.query("INSERT INTO orders VALUES (1, 1, 9.9)");
    c.query("INSERT INTO orders VALUES (2, 1, 5.0)");
    c.query("INSERT INTO orders VALUES (3, 3, 7.7)");

    // INNER: 正确配对, ORDER BY 右表列
    let r = c.query("SELECT u.name, o.amt FROM users u JOIN orders o ON u.id = o.uid ORDER BY o.amt");
    assert_eq!(
        r,
        QueryResult::Rows(vec![
            vec![Some("u1".into()), Some("5".into())],
            vec![Some("u3".into()), Some("7.7".into())],
            vec![Some("u1".into()), Some("9.9".into())],
        ]),
        "INNER JOIN"
    );

    // LEFT: 无订单用户补 NULL 右列 (完整 ORDER BY 保确定顺序: u1 有两单)
    let r = c.query("SELECT u.name, o.amt FROM users u LEFT JOIN orders o ON u.id = o.uid ORDER BY u.id, o.amt");
    assert_eq!(
        r,
        QueryResult::Rows(vec![
            vec![Some("u1".into()), Some("5".into())],
            vec![Some("u1".into()), Some("9.9".into())],
            vec![Some("u2".into()), None],
            vec![Some("u3".into()), Some("7.7".into())],
            vec![Some("u4".into()), None],
        ]),
        "LEFT JOIN 补 NULL"
    );

    // 谓词下推 (u.age > 22 → 仅 u3/u4; 有订单的只有 u3)
    let r = c.query("SELECT u.name, o.amt FROM users u JOIN orders o ON u.id = o.uid WHERE u.age > 22");
    assert_eq!(r, QueryResult::Rows(vec![vec![Some("u3".into()), Some("7.7".into())]]), "WHERE 下推");

    // 右表谓词 (INNER 下推)
    let r = c.query("SELECT u.name FROM users u JOIN orders o ON u.id = o.uid WHERE o.amt > 8.0");
    assert_eq!(r, QueryResult::Rows(vec![vec![Some("u1".into())]]), "右表 WHERE");

    // SELECT * → 限定列头展开左右全列
    let r = c.query("SELECT * FROM users u JOIN orders o ON u.id = o.uid ORDER BY o.id LIMIT 1");
    assert_eq!(
        r,
        QueryResult::Rows(vec![vec![
            Some("1".into()), Some("u1".into()), Some("21".into()),
            Some("1".into()), Some("1".into()), Some("9.9".into()),
        ]]),
        "SELECT * 展开"
    );

    // 反向 ON (o.uid = u.id) 等价
    let r = c.query("SELECT u.name, o.amt FROM users u JOIN orders o ON o.uid = u.id ORDER BY o.amt LIMIT 1");
    assert_eq!(r, QueryResult::Rows(vec![vec![Some("u1".into()), Some("5".into())]]), "反向 ON");

    // 单表 SELECT 零回归
    assert_eq!(c.ids("SELECT id FROM users WHERE id = 2"), vec!["2"]);

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

// ===== ⭐ JOIN 族 Phase 1 (F68): 多表/RIGHT/FULL/CROSS/USING/多 ON/索引 =====

/// 三表左深 + RIGHT/FULL/CROSS/USING + 多条件 ON + 索引驱动 gather.
#[test]
fn mysql_join_family() {
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");
    c.query("CREATE TABLE u (id INT PRIMARY KEY, name TEXT, age INT, INDEX(age))");
    c.query("CREATE TABLE o (id INT PRIMARY KEY, uid INT, pid INT, amt DOUBLE)");
    c.query("CREATE TABLE p (id INT PRIMARY KEY, pname TEXT)");
    for i in 1..=3 {
        c.query(&format!("INSERT INTO u VALUES ({i}, 'u{i}', {})", 20 + i));
    }
    c.query("INSERT INTO p VALUES (10, 'apple')");
    c.query("INSERT INTO p VALUES (20, 'banana')");
    c.query("INSERT INTO o VALUES (1, 1, 10, 9.9)");
    c.query("INSERT INTO o VALUES (2, 1, 20, 5.0)");
    c.query("INSERT INTO o VALUES (3, 3, 10, 7.7)");
    c.query("INSERT INTO o VALUES (4, 9, 20, 1.0)"); // uid=9 无对应 user

    // 三表左深
    let r = c.query("SELECT u.name, o.amt, p.pname FROM u JOIN o ON u.id = o.uid JOIN p ON o.pid = p.id ORDER BY o.amt");
    assert_eq!(
        r,
        QueryResult::Rows(vec![
            vec![Some("u1".into()), Some("5".into()), Some("banana".into())],
            vec![Some("u3".into()), Some("7.7".into()), Some("apple".into())],
            vec![Some("u1".into()), Some("9.9".into()), Some("apple".into())],
        ]),
        "3-table left-deep"
    );

    // RIGHT: uid=9 未匹配 → 左列 NULL
    let r = c.query("SELECT u.name, o.id FROM u RIGHT JOIN o ON u.id = o.uid ORDER BY o.id");
    assert_eq!(
        r,
        QueryResult::Rows(vec![
            vec![Some("u1".into()), Some("1".into())],
            vec![Some("u1".into()), Some("2".into())],
            vec![Some("u3".into()), Some("3".into())],
            vec![None, Some("4".into())],
        ]),
        "RIGHT JOIN"
    );

    // FULL: u2 无订单 + o4 无用户 双补 NULL
    let r = c.query("SELECT u.id, o.id FROM u FULL JOIN o ON u.id = o.uid ORDER BY u.id, o.id");
    assert_eq!(
        r,
        QueryResult::Rows(vec![
            vec![None, Some("4".into())],
            vec![Some("1".into()), Some("1".into())],
            vec![Some("1".into()), Some("2".into())],
            vec![Some("2".into()), None],
            vec![Some("3".into()), Some("3".into())],
        ]),
        "FULL JOIN"
    );

    // CROSS: 3 * 2 = 6 行
    if let QueryResult::Rows(rows) = c.query("SELECT u.id, p.id FROM u CROSS JOIN p") {
        assert_eq!(rows.len(), 6, "CROSS cardinality");
    } else {
        panic!("cross");
    }

    // 多条件 ON (等值 + 非等值残余): amt > age 无一满足
    assert_eq!(
        c.query("SELECT o.id FROM o JOIN u ON o.uid = u.id AND o.amt > u.age"),
        QueryResult::Rows(vec![]),
        "multi-cond ON residual"
    );

    // 索引驱动 gather (age > 21): 结果正确 (u3 有订单 o3)
    let r = c.query("SELECT u.id, o.amt FROM u JOIN o ON u.id = o.uid WHERE u.age > 21 ORDER BY o.amt");
    assert_eq!(r, QueryResult::Rows(vec![vec![Some("3".into()), Some("7.7".into())]]), "index-driven gather");

    // USING (id): 合成等值连接
    c.query("CREATE TABLE a (id INT PRIMARY KEY, x INT)");
    c.query("CREATE TABLE b (id INT PRIMARY KEY, y INT)");
    c.query("INSERT INTO a VALUES (1, 100)");
    c.query("INSERT INTO a VALUES (2, 300)");
    c.query("INSERT INTO b VALUES (1, 200)");
    assert_eq!(
        c.query("SELECT a.x, b.y FROM a JOIN b USING (id)"),
        QueryResult::Rows(vec![vec![Some("100".into()), Some("200".into())]]),
        "USING"
    );

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

// ===== ⭐ OR/NOT/括号 谓词树 (F69) =====

/// WHERE 支持 OR/NOT/括号嵌套 + 索引回退 + DELETE/UPDATE/JOIN 带 OR.
#[test]
fn mysql_or_predicates() {
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");
    c.query("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, INDEX(a))");
    for (i, (a, b)) in [(1, 10), (2, 20), (3, 30), (1, 99)].iter().enumerate() {
        c.query(&format!("INSERT INTO t VALUES ({}, {a}, {b})", i + 1));
    }

    // OR (索引列 a 上 OR → 全表扫回退, 结果正确)
    assert_eq!(c.ids("SELECT id FROM t WHERE a = 1 OR b = 30 ORDER BY id"), vec!["1", "3", "4"]);
    // 混合 (a=1 OR a=2) AND b>15
    assert_eq!(c.ids("SELECT id FROM t WHERE (a = 1 OR a = 2) AND b > 15 ORDER BY id"), vec!["2", "4"]);
    // NOT (括号)
    assert_eq!(c.ids("SELECT id FROM t WHERE NOT (a = 1) ORDER BY id"), vec!["2", "3"]);
    // 纯 AND 仍走索引 (零回归)
    assert_eq!(c.ids("SELECT id FROM t WHERE a = 2"), vec!["2"]);
    // 嵌套括号
    assert_eq!(
        c.ids("SELECT id FROM t WHERE (a = 3) OR (b = 10 AND a = 1) ORDER BY id"),
        vec!["1", "3"]
    );

    // DELETE 带 OR
    c.query("DELETE FROM t WHERE a = 1 OR b = 30");
    assert_eq!(c.ids("SELECT id FROM t ORDER BY id"), vec!["2"]);

    // UPDATE 带 OR
    c.query("INSERT INTO t VALUES (5, 5, 5)");
    c.query("UPDATE t SET b = 100 WHERE a = 2 OR a = 5");
    assert_eq!(c.ids("SELECT id FROM t WHERE b = 100 ORDER BY id"), vec!["2", "5"]);

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

/// JOIN WHERE 带 OR (下推回退, worker 残余递归) + HAVING 带 OR.
#[test]
fn mysql_or_join_having() {
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");
    c.query("CREATE TABLE u (id INT PRIMARY KEY, name TEXT, age INT)");
    c.query("CREATE TABLE o (id INT PRIMARY KEY, uid INT, amt INT)");
    for i in 1..=3 {
        c.query(&format!("INSERT INTO u VALUES ({i}, 'u{i}', {})", 20 + i));
    }
    c.query("INSERT INTO o VALUES (1, 1, 5)");
    c.query("INSERT INTO o VALUES (2, 2, 50)");
    c.query("INSERT INTO o VALUES (3, 3, 7)");

    // JOIN WHERE 带 OR (u.age>22 OR o.amt>10)
    let r = c.query("SELECT u.name, o.amt FROM u JOIN o ON u.id = o.uid WHERE u.age > 22 OR o.amt > 10 ORDER BY o.amt");
    assert_eq!(
        r,
        QueryResult::Rows(vec![
            vec![Some("u3".into()), Some("7".into())],  // age 23 > 22
            vec![Some("u2".into()), Some("50".into())], // amt 50 > 10
        ]),
        "JOIN WHERE OR"
    );

    // HAVING 带 OR: 分组求和后 OR 过滤
    c.query("CREATE TABLE s (id INT PRIMARY KEY, g INT, v INT)");
    for (i, (g, v)) in [(1, 1), (2, 5), (1, 100), (3, 3)].iter().enumerate() {
        c.query(&format!("INSERT INTO s VALUES ({}, {g}, {v})", i + 1));
    }
    // g=1 → sum 101, g=2 → 5, g=3 → 3; HAVING sum>50 OR g=3
    let r = c.query("SELECT g, SUM(v) FROM s GROUP BY g HAVING SUM(v) > 50 OR g = 3 ORDER BY g");
    assert_eq!(
        r,
        QueryResult::Rows(vec![
            vec![Some("1".into()), Some("101".into())],
            vec![Some("3".into()), Some("3".into())],
        ]),
        "HAVING OR"
    );

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}
