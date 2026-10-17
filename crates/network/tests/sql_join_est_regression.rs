//! 回归测试 (JOIN estimate 阈值, 方案 A 调优): 独立二进制运行.
//!
//! 为什么独立成文件: 本用例断言**进程级** metrics 计数器
//! (`SQL_JOIN_EST_ROUNDS` / `SQL_JOIN_EST_SKIPPED`) 的增量 — 同一进程内任何
//! 走 estimation 路径的 JOIN 都会递增它们. 曾与 sql_e2e.rs 其余 JOIN 用例同
//! 二进制并行运行, 计数器被污染导致断言失败 (实测: r2-r1 期望 3 实际更多).
//! 独立进程后计数器无噪声, 可与其他测试文件并行.
//!
//! 覆盖: 小表 JOIN (双表 ≤ EST_SKIP_STATS_ROWS → 跳过 distinct/ranges 统计,
//! 固定 1 轮广播); 大表 JOIN (> 阈值 → 行数+distinct+ranges 3 轮); 无索引
//! 大表 (候选索引列为空 → 统计批次自动跳过, 仍 1 轮). 三条路径结果正确性
//! 均断言.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
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

/// 查询结果.
#[derive(Debug, PartialEq)]
enum QueryResult {
    Ok {
        affected: u64,
    },
    Err {
        code: u16,
        msg: String,
    },
    /// 文本结果集: 每行每列 Option<String> (None = NULL).
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
            0x00 => QueryResult::Ok {
                affected: first[1] as u64,
            }, // affected < 251
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
}

/// ⭐ 方案 A (调优): EstimateRows 开销收敛 — 小表 JOIN (两表行数 ≤ EST_SKIP_STATS_ROWS)
/// 跳过 distinct/ranges 统计收集, 固定只 1 轮行数广播 (SQL_JOIN_EST_ROUNDS 增量=1,
/// SQL_JOIN_EST_SKIPPED 增量=1); 大表 JOIN 走完整统计 (行数+distinct+ranges=3 轮,
/// 不触发跳过). 结果正确性两条路径都保持.
#[test]
fn mysql_join_est_threshold() {
    use std::sync::atomic::Ordering::Relaxed;
    let (server, mgr) = start_sql_server(None);
    let mut c = MyConn::handshake_login(&server, "");
    // ---- 小表场景: 两表 ≤ 阈值 → 跳过统计, 只 1 轮 ----
    c.query("CREATE TABLE s (id INT PRIMARY KEY, a INT, b INT)");
    c.query("CREATE TABLE u (id INT PRIMARY KEY, name TEXT)");
    c.query("CREATE INDEX sa ON s(a)");
    c.query("CREATE INDEX sb ON s(b)");
    for i in 0..100 {
        c.query(&format!("INSERT INTO s VALUES ({i}, {}, {i})", i % 2));
    }
    for (id, name) in [("0", "zero"), ("1", "one")] {
        c.query(&format!("INSERT INTO u VALUES ({id}, '{name}')"));
    }
    let r0 = network::metrics::SQL_JOIN_EST_ROUNDS.load(Relaxed);
    let k0 = network::metrics::SQL_JOIN_EST_SKIPPED.load(Relaxed);
    assert_eq!(
        c.query("SELECT u.name, s.id FROM u JOIN s ON u.id = s.id WHERE s.a = 0 AND s.b = 0"),
        QueryResult::Rows(vec![vec![Some("zero".into()), Some("0".into())]]),
        "小表 JOIN 结果正确 (跳过统计 + key_set/index_hint 兜底)"
    );
    let r1 = network::metrics::SQL_JOIN_EST_ROUNDS.load(Relaxed);
    let k1 = network::metrics::SQL_JOIN_EST_SKIPPED.load(Relaxed);
    assert_eq!(r1 - r0, 1, "小表 JOIN 应只 1 轮 (行数合并, 统计跳过)");
    assert_eq!(k1 - k0, 1, "小表 JOIN 应触发阈值跳过");

    // ---- 大表场景: 一行 > 阈值 → 走完整统计 (行数+distinct+ranges) ----
    // 注: 独立 CREATE INDEX 在 worker 被吞 (v1), 须用建表内联 INDEX(col).
    c.query("CREATE TABLE big (id INT PRIMARY KEY, a INT, b INT, INDEX(a), INDEX(b))");
    // 多行 VALUES 批量插入 (2000 行 > 阈值 1024)
    let mut sql = String::from("INSERT INTO big VALUES ");
    for i in 0..2000 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i}, {}, {i})", i % 50));
    }
    c.query(&sql);
    assert_eq!(
        c.query(
            "SELECT u.name, big.id FROM u JOIN big ON u.id = big.id WHERE big.a = 0 AND big.b = 0"
        ),
        QueryResult::Rows(vec![vec![Some("zero".into()), Some("0".into())]]),
        "大表 JOIN 走统计后结果仍正确"
    );
    let r2 = network::metrics::SQL_JOIN_EST_ROUNDS.load(Relaxed);
    let k2 = network::metrics::SQL_JOIN_EST_SKIPPED.load(Relaxed);
    assert_eq!(r2 - r1, 3, "大表 JOIN 应 3 轮 (行数+distinct+ranges)");
    assert_eq!(k2 - k1, 0, "大表 JOIN 不应触发阈值跳过");

    // ---- 无索引大表: 行数 > 阈值但无候选索引列 → distinct/ranges 自动跳过, 仍 1 轮 ----
    c.query("CREATE TABLE big2 (id INT PRIMARY KEY, c INT)");
    let mut sql = String::from("INSERT INTO big2 VALUES ");
    for i in 0..1500 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i}, {i})"));
    }
    c.query(&sql);
    assert_eq!(
        c.query("SELECT u.name, big2.id FROM u JOIN big2 ON u.id = big2.id WHERE big2.c = 0"),
        QueryResult::Rows(vec![vec![Some("zero".into()), Some("0".into())]]),
        "无索引大表 JOIN 结果正确 (全扫 Gather)"
    );
    let r3 = network::metrics::SQL_JOIN_EST_ROUNDS.load(Relaxed);
    assert_eq!(r3 - r2, 1, "无索引大表应 1 轮 (候选空, 统计批次自动跳过)");

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}
