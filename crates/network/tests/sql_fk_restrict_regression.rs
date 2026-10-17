//! 回归测试 (审计 P1-4): 父表 DELETE 前置 RESTRICT 预检.
//!
//! 修复前: 外键检查仅覆盖 INSERT (sql_fk.rs); 父表 DELETE 时非 CASCADE 引用
//! (默认 RESTRICT/NO ACTION) 无任何检查 — 子行被静默留下, 引用完整性破坏
//! 且无告警. 本文件独立二进制 (进程级 incoming_fks/catalog 状态不与其他
//! 测试文件互相干扰).
//!
//! 覆盖:
//! 1. 父行被子行引用 → DELETE 拒 (MySQL 1451 / PG 23503), 父子行均不动;
//! 2. 无引用 → DELETE 放行;
//! 3. ON DELETE CASCADE 不受影响 (级联仍生效);
//! 4. 子行跨 shard 分布 → 广播探针仍能拦截;
//! 5. 重启 (新 sql_shared, 模拟新进程) 后 incoming_fks 为空 → catalog 补注
//!    后仍拦截 (否则静默跳过);
//! 6. 多行 DELETE 部分被引用 → 整语句拒绝 (RESTRICT 即时语义).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use network::protocol::mysql as my;
use network::{KvLimits, NetworkServer, NetworkServerConfig, ProtocolKind};
use shard_manager::{ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

fn start_sql_server(
    block_root: std::path::PathBuf,
    create: bool,
) -> (NetworkServer, Arc<ShardManager>) {
    let opts = ShardManagerOptions {
        num_shards: 3,
        block_root,
        create_if_missing: create,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 4,
        reply_bus_count: None,
        wal_mode: Default::default(),
    };
    let mgr = Arc::new(ShardManager::open(opts).expect("open mgr"));
    if create {
        mgr.create_db("app").expect("create db");
        mgr.create_table("app", "kv").expect("create table");
    }

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

// ===== 最小 MySQL 客户端 (与 sql_e2e.rs 同款, 复制以保持本文件独立) =====

struct MyConn {
    stream: TcpStream,
    buf: Vec<u8>,
}

#[derive(Debug, PartialEq)]
enum QueryResult {
    Ok { affected: u64 },
    Err { code: u16, msg: String },
    Rows(Vec<Vec<Option<String>>>),
}

impl MyConn {
    fn connect(server: &NetworkServer) -> Self {
        Self::connect_addr(server.local_addr())
    }

    fn connect_addr(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        stream.set_nodelay(true).unwrap();
        Self {
            stream,
            buf: Vec::new(),
        }
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

    fn ids(&mut self, stmt: &str) -> Vec<String> {
        match self.query(stmt) {
            QueryResult::Rows(rows) => rows
                .iter()
                .map(|r| r[0].clone().expect("id 非 NULL"))
                .collect(),
            other => panic!("expect rows, got {other:?}"),
        }
    }
}

/// 断言是 FK 违规错误 (含消息内容).
fn assert_fk_violation(r: &QueryResult, ctx: &str) {
    match r {
        QueryResult::Err { code, msg } => {
            assert!(
                msg.contains("foreign key violation"),
                "{ctx}: 期望 FK 违规错误, 实际: {msg}"
            );
            assert_eq!(
                *code, 1451,
                "{ctx}: MySQL 应映射 ER_ROW_IS_REFERENCED_2 (1451)"
            );
        }
        other => panic!("{ctx}: 期望 FK 违规错误, 实际: {other:?}"),
    }
}

fn setup_fk_tables(c: &mut MyConn, child_ddl_extra: &str) {
    c.query("CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)");
    c.query(&format!(
        "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id){child_ddl_extra})"
    ));
}

#[test]
fn delete_parent_rejected_when_referenced() {
    let tmp = tempfile::tempdir().unwrap();
    let (server, mgr) = start_sql_server(tmp.path().to_path_buf(), true);
    let mut c = MyConn::handshake_login(&server, "");
    setup_fk_tables(&mut c, "");
    c.query("INSERT INTO parent VALUES (1, 'p1')");
    c.query("INSERT INTO child VALUES (1, 1)");

    // 父行被子行引用 → 必须拒绝 (修复前: 静默删父行留孤儿)
    assert_fk_violation(
        &c.query("DELETE FROM parent WHERE id = 1"),
        "delete referenced parent",
    );

    // 父子行都不得被动过
    assert_eq!(c.ids("SELECT id FROM parent"), vec!["1"], "父行应仍在");
    assert_eq!(c.ids("SELECT id FROM child"), vec!["1"], "子行应仍在");

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

#[test]
fn delete_parent_ok_when_unreferenced() {
    let tmp = tempfile::tempdir().unwrap();
    let (server, mgr) = start_sql_server(tmp.path().to_path_buf(), true);
    let mut c = MyConn::handshake_login(&server, "");
    setup_fk_tables(&mut c, "");
    c.query("INSERT INTO parent VALUES (1, 'p1')");
    c.query("INSERT INTO child VALUES (1, 1)");
    // 先删子行 (无引用) → 父行可删
    assert_eq!(
        c.query("DELETE FROM child WHERE id = 1"),
        QueryResult::Ok { affected: 1 }
    );
    assert_eq!(
        c.query("DELETE FROM parent WHERE id = 1"),
        QueryResult::Ok { affected: 1 },
        "无引用时 DELETE 父行必须放行"
    );
    // 删不存在的行: 无引用可能 → OK(0)
    assert_eq!(
        c.query("DELETE FROM parent WHERE id = 999"),
        QueryResult::Ok { affected: 0 }
    );

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

#[test]
fn cascade_delete_still_works() {
    let tmp = tempfile::tempdir().unwrap();
    let (server, mgr) = start_sql_server(tmp.path().to_path_buf(), true);
    let mut c = MyConn::handshake_login(&server, "");
    setup_fk_tables(&mut c, " ON DELETE CASCADE");
    c.query("INSERT INTO parent VALUES (1, 'p1')");
    c.query("INSERT INTO child VALUES (1, 1)");
    c.query("INSERT INTO child VALUES (2, 1)");

    // CASCADE 语义不受 RESTRICT 预检影响
    assert_eq!(
        c.query("DELETE FROM parent WHERE id = 1"),
        QueryResult::Ok { affected: 1 }
    );
    assert_eq!(
        c.ids("SELECT id FROM child"),
        Vec::<String>::new(),
        "级联应删光"
    );

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

#[test]
fn cross_shard_references_blocked() {
    let tmp = tempfile::tempdir().unwrap();
    let (server, mgr) = start_sql_server(tmp.path().to_path_buf(), true);
    let mut c = MyConn::handshake_login(&server, "");
    setup_fk_tables(&mut c, "");
    c.query("INSERT INTO parent VALUES (1, 'p1')");
    // 多个子行 (pk 各异 → hash 分散到 3 个 shard), 全部引用父行 1
    for i in 1..=12 {
        c.query(&format!("INSERT INTO child VALUES ({i}, 1)"));
    }
    assert_fk_violation(
        &c.query("DELETE FROM parent WHERE id = 1"),
        "cross-shard referenced parent",
    );
    assert_eq!(c.ids("SELECT id FROM child").len(), 12, "子行应全在");

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

#[test]
fn multi_row_delete_partial_reference_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let (server, mgr) = start_sql_server(tmp.path().to_path_buf(), true);
    let mut c = MyConn::handshake_login(&server, "");
    setup_fk_tables(&mut c, "");
    for i in 1..=3 {
        c.query(&format!("INSERT INTO parent VALUES ({i}, 'p{i}')"));
    }
    c.query("INSERT INTO child VALUES (1, 1)");
    c.query("INSERT INTO child VALUES (2, 3)"); // 引用 parent 3

    // IN (1,2,3): 1 和 3 被引用 → 整语句拒绝 (RESTRICT 即时语义, 不做部分删除)
    assert_fk_violation(
        &c.query("DELETE FROM parent WHERE id IN (1, 2, 3)"),
        "partial reference in multi-row delete",
    );
    let ids = c.ids("SELECT id FROM parent ORDER BY id");
    assert_eq!(ids, vec!["1", "2", "3"], "三行父行都应仍在");

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

/// 重启场景: 新 sql_shared (模拟新进程 — incoming_fks 进程级注册表为空).
/// worker 必须经 catalog 补注发现引用关系后仍拦截, 不得静默放行.
#[test]
fn restart_registers_incoming_fks() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    {
        let (server, mgr) = start_sql_server(root.clone(), true);
        let mut c = MyConn::handshake_login(&server, "");
        setup_fk_tables(&mut c, "");
        c.query("INSERT INTO parent VALUES (1, 'p1')");
        c.query("INSERT INTO child VALUES (1, 1)");
        drop(c);
        server.shutdown().unwrap();
        drop(mgr);
    }
    // 重开 (数据持久化; sql_shared 全新 — 模拟进程重启)
    {
        let (server, mgr) = start_sql_server(root.clone(), false);
        let mut c = MyConn::handshake_login(&server, "");
        assert_fk_violation(
            &c.query("DELETE FROM parent WHERE id = 1"),
            "restart: referenced parent",
        );
        assert_eq!(c.ids("SELECT id FROM parent"), vec!["1"], "父行应仍在");
        assert_eq!(c.ids("SELECT id FROM child"), vec!["1"], "子行应仍在");

        drop(c);
        server.shutdown().unwrap();
        drop(mgr);
    }
}
