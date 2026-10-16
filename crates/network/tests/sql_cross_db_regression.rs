//! 回归测试 (2026-10-16, 审计新 P0-3): 跨库表限定名不得静默按当前库解析.
//!
//! 修复前: 解析器 (`strip_db_qual`) 静默剥离 `db.tbl` 的库名, 全部按当前库
//! 解析 — `USE app; SELECT * FROM other.users` 返回 **app.users** 的数据
//! (跨库数据暴露), INSERT/DELETE 则写入/删除当前库同名表 (跨库污染).
//!
//! 修复后: 同库限定名剥离放行; 跨库限定名显式报错
//! (MySQL 1235 / PG 0A000; 库不存在 → 1049 / 3D000).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use network::protocol::mysql as my;
use network::{KvLimits, NetworkServer, NetworkServerConfig, ProtocolKind};
use shard_manager::{ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

fn start_sql_server() -> (NetworkServer, Arc<ShardManager>) {
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
    mgr.create_db("app").expect("create db app");
    mgr.create_db("other").expect("create db other");
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
        tls_config: None,
        shared_workers: None,
    };
    let server = NetworkServer::start(cfg).expect("start server");
    (server, mgr)
}

#[derive(Debug, PartialEq)]
enum QueryResult {
    Ok { affected: u64 },
    Err { code: u16, msg: String },
    Rows(Vec<Vec<Option<String>>>),
}

struct MyConn {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl MyConn {
    fn connect(server: &NetworkServer) -> Self {
        let stream = TcpStream::connect(server.local_addr()).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        stream.set_nodelay(true).unwrap();
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    fn login(&mut self) {
        let salt = self.read_handshake();
        let token = my::native_password_token(&salt, "");
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

    fn read_handshake(&mut self) -> [u8; 20] {
        let (seq, p) = self.read_frame();
        assert_eq!(seq, 0);
        assert_eq!(p[0], 10, "protocol version");
        let mut pos = 1;
        while p[pos] != 0 {
            pos += 1;
        }
        pos += 1 + 4;
        let mut salt = [0u8; 20];
        salt[..8].copy_from_slice(&p[pos..pos + 8]);
        pos += 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10;
        salt[8..20].copy_from_slice(&p[pos..pos + 12]);
        salt
    }

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

    fn query(&mut self, stmt: &str) -> QueryResult {
        let mut p = vec![my::COM_QUERY];
        p.extend_from_slice(stmt.as_bytes());
        self.stream.write_all(&my::write_packet(0, &p)).unwrap();
        let (_, first) = self.read_frame();
        match first[0] {
            0x00 => QueryResult::Ok {
                affected: first[1] as u64,
            },
            0xFF => QueryResult::Err {
                code: u16::from_le_bytes([first[1], first[2]]),
                msg: String::from_utf8_lossy(&first[9..]).into_owned(),
            },
            n => {
                let ncols = n as usize;
                for _ in 0..ncols {
                    self.read_frame();
                }
                let (_, eof) = self.read_frame();
                assert_eq!(eof[0], 0xFE, "expect EOF after columns");
                let mut rows = Vec::new();
                loop {
                    let (_, rp) = self.read_frame();
                    if rp[0] == 0xFE && rp.len() < 9 {
                        break;
                    }
                    let mut row = Vec::with_capacity(ncols);
                    let mut pos = 0usize;
                    for _ in 0..ncols {
                        if rp[pos] == 0xFB {
                            row.push(None);
                            pos += 1;
                        } else {
                            let len = rp[pos] as usize;
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

    fn rows1(&mut self, stmt: &str) -> Vec<Option<String>> {
        match self.query(stmt) {
            QueryResult::Rows(r) => {
                assert_eq!(r.len(), 1, "{stmt}: 期望 1 行, 实际 {r:?}");
                r.into_iter().next().unwrap()
            }
            other => panic!("{stmt}: 期望结果集, 实际 {other:?}"),
        }
    }

    fn expect_err(&mut self, stmt: &str, code: u16) {
        match self.query(stmt) {
            QueryResult::Err { code: c, msg } => {
                assert_eq!(c, code, "{stmt}: 错误码 {c} != {code} (msg={msg})");
            }
            other => panic!("{stmt}: 期望 Err({code}), 实际 {other:?}"),
        }
    }
}

const CREATE: &str = "CREATE TABLE users (id INT PRIMARY KEY, v TEXT NOT NULL)";

#[test]
fn cross_db_qualified_name_errors_instead_of_silent_wrong_data() {
    let (server, _mgr) = start_sql_server();
    let mut c = MyConn::connect(&server);
    c.login();

    // app.users = app_row
    assert_eq!(c.query(CREATE), QueryResult::Ok { affected: 0 });
    assert_eq!(
        c.query("INSERT INTO users VALUES (1, 'app_row')"),
        QueryResult::Ok { affected: 1 }
    );

    // other.users = other_row (同名表, 不同库)
    assert_eq!(c.query("USE other"), QueryResult::Ok { affected: 0 });
    assert_eq!(c.query(CREATE), QueryResult::Ok { affected: 0 });
    assert_eq!(
        c.query("INSERT INTO users VALUES (1, 'other_row')"),
        QueryResult::Ok { affected: 1 }
    );

    // 回到 app
    assert_eq!(c.query("USE app"), QueryResult::Ok { affected: 0 });

    // 1) 无限定名: 当前库 (不变行为)
    assert_eq!(c.rows1("SELECT v FROM users"), vec![Some("app_row".into())]);

    // 2) 跨库读: 必须显式报错 (修复前静默返回 app_row)
    c.expect_err("SELECT v FROM other.users", 1235);
    c.expect_err("SELECT * FROM other.users", 1235);

    // 3) 同库限定名: 剥离放行 (现有偶发正确行为保持工作)
    assert_eq!(
        c.rows1("SELECT v FROM app.users"),
        vec![Some("app_row".into())]
    );

    // 4) 跨库写: 必须报错 (修复前会写进 app.users!)
    c.expect_err("INSERT INTO other.users VALUES (2, 'evil')", 1235);
    c.expect_err("UPDATE other.users SET v = 'evil'", 1235);
    c.expect_err("DELETE FROM other.users", 1235);
    c.expect_err("DROP TABLE other.users", 1235);

    // 5) 不存在的库: Unknown database (1049)
    c.expect_err("SELECT v FROM nosuchdb.users", 1049);

    // 6) 当前库未被污染: 仍只有 app_row 一行
    assert_eq!(c.rows1("SELECT v FROM users"), vec![Some("app_row".into())]);

    // 7) SHOW COLUMNS ... FROM db: 同样报错 (修复前 `let _ = ident` 静默丢弃)
    c.expect_err("SHOW COLUMNS FROM users FROM other", 1235);
    // 同库显式库名: 放行
    let cols = c.query("SHOW COLUMNS FROM users FROM app");
    assert!(
        matches!(cols, QueryResult::Rows(_)),
        "SHOW COLUMNS FROM app 应放行: {cols:?}"
    );
}
