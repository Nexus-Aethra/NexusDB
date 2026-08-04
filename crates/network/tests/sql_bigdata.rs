//! ⭐ 大数据量 SQL 端到端正确性测试.
//!
//! 通过 MySQL wire 协议向 NexusDB 灌入数万行确定性数据, 校验:
//! COUNT/WHERE/GROUP BY/聚合(SUM/AVG/MIN/MAX)/ORDER BY/LIMIT OFFSET/JOIN/
//! UPDATE/DELETE, 以及写入后重连的数据一致性.
//!
//! 数据确定性生成 (id 1..=N): name=`u{i}`, age=i%100, score=(i%1000).0
//! 使所有聚合/过滤/排序结果可精确计算.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use network::protocol::mysql as my;
use network::{KvLimits, NetworkServer, NetworkServerConfig, ProtocolKind};
use shard_manager::{ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

// ===== 与 sql_e2e 相同的最小 MySQL 客户端 (测试辅助, 各 e2e 自持) =====

const N: i64 = 20_000;

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
        stream.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
        stream.set_nodelay(true).unwrap();
        Self { stream, buf: Vec::new() }
    }

    fn read_frame(&mut self) -> (u8, Vec<u8>) {
        loop {
            if let Some((seq, n, payload)) = my::read_packet(&self.buf) {
                self.buf.drain(..n);
                return (seq, payload);
            }
            let mut tmp = [0u8; 8192];
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

    fn handshake_login(server: &NetworkServer) -> Self {
        let mut c = Self::connect(server);
        let salt = c.read_handshake();
        let token = my::native_password_token(&salt, "");
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
            0x00 => {
                // affected rows 为 length-encoded integer (支持 >= 251 的大影响行数)
                let affected = read_lenenc(&first, 1).0;
                QueryResult::Ok { affected }
            }
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

    fn err_code(&self, r: &QueryResult) -> Option<u16> {
        match r {
            QueryResult::Err { code, .. } => Some(*code),
            _ => None,
        }
    }
}

// ===== 大数据测试 =====

/// 解析 MySQL length-encoded integer (起始位置 pos). 返回 (值, 结束位置).
fn read_lenenc(p: &[u8], pos: usize) -> (u64, usize) {
    let b = p[pos];
    match b {
        0..=0xFB => (b as u64, pos + 1),
        0xFC => {
            let v = u16::from_le_bytes([p[pos + 1], p[pos + 2]]) as u64;
            (v, pos + 3)
        }
        0xFD => {
            let v = u32::from_le_bytes([p[pos + 1], p[pos + 2], p[pos + 3], 0]) as u64;
            (v, pos + 4)
        }
        0xFE => {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&p[pos + 1..pos + 9]);
            (u64::from_le_bytes(arr), pos + 9)
        }
        _ => panic!("invalid lenenc marker {b:#x}"),
    }
}

#[test]
fn bigdata_count_and_filter() {
    let (server, mgr) = start_sql_server();
    let mut c = MyConn::handshake_login(&server);
    c.query("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT, score DOUBLE)");

    // 分批多行 INSERT: 每批 200 行 (affected=200 < 251), 共 N=20000 行.
    let mut inserted = 0i64;
    for start in (1..=N).step_by(200) {
        let end = (start + 199).min(N);
        let mut vals = String::new();
        for i in start..=end {
            if !vals.is_empty() {
                vals.push_str(", ");
            }
            vals.push_str(&format!("({i}, 'u{i}', {}, {}.0)", i % 100, i % 1000));
        }
        let r = c.query(&format!("INSERT INTO users VALUES {vals}"));
        let expect = (end - start + 1) as u64;
        assert_eq!(r, QueryResult::Ok { affected: expect }, "batch INSERT");
        inserted += end - start + 1;
    }
    assert_eq!(inserted, N, "全部写入");

    // COUNT(*)
    match c.query("SELECT COUNT(*) FROM users") {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0].as_deref(), Some(N.to_string().as_str()), "COUNT(*) 全表");
        }
        other => panic!("expect rows, got {other:?}"),
    }

    // WHERE 过滤: age=i%100, 故 age=50 对应 i≡50 (mod 100) → N/100 = 200 行
    match c.query("SELECT COUNT(*) FROM users WHERE age = 50") {
        QueryResult::Rows(rows) => {
            assert_eq!(rows[0][0].as_deref(), Some("200"), "age=50 计数");
        }
        other => panic!("{other:?}"),
    }
    // score>=500: score=i%1000 → 500..=999 占 500/1000 → N/2 = 10000 行
    match c.query("SELECT COUNT(*) FROM users WHERE score >= 500") {
        QueryResult::Rows(rows) => {
            assert_eq!(rows[0][0].as_deref(), Some("10000"), "score>=500 计数");
        }
        other => panic!("{other:?}"),
    }
    // 等值主键
    match c.query("SELECT name, age, score FROM users WHERE id = 12345") {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows,
                vec![vec![
                    Some("u12345".into()),
                    Some((12345 % 100).to_string()),
                    Some((12345 % 1000).to_string()),
                ]],
                "主键等值"
            );
        }
        other => panic!("{other:?}"),
    }

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

#[test]
fn bigdata_aggregate_groupby() {
    let (server, mgr) = start_sql_server();
    let mut c = MyConn::handshake_login(&server);
    c.query("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT, score DOUBLE)");
    for start in (1..=N).step_by(200) {
        let end = (start + 199).min(N);
        let mut vals = String::new();
        for i in start..=end {
            if !vals.is_empty() {
                vals.push_str(", ");
            }
            vals.push_str(&format!("({i}, 'u{i}', {}, {}.0)", i % 100, i % 1000));
        }
        c.query(&format!("INSERT INTO users VALUES {vals}"));
    }

    // SUM(id) = N(N+1)/2
    match c.query("SELECT SUM(id) FROM users") {
        QueryResult::Rows(rows) => {
            let expect = (N * (N + 1)) / 2;
            assert_eq!(rows[0][0].as_deref(), Some(expect.to_string().as_str()), "SUM(id)");
        }
        other => panic!("{other:?}"),
    }
    // MIN/MAX(id)
    match c.query("SELECT MIN(id), MAX(id) FROM users") {
        QueryResult::Rows(rows) => {
            assert_eq!(rows[0][0].as_deref(), Some("1"), "MIN(id)");
            assert_eq!(rows[0][1].as_deref(), Some(N.to_string().as_str()), "MAX(id)");
        }
        other => panic!("{other:?}"),
    }
    // AVG(id) = (N+1)/2 = 10000.5 (注意 DOUBLE 渲染格式; 用 SUM/COUNT 推导)
    match c.query("SELECT AVG(id) FROM users") {
        QueryResult::Rows(rows) => {
            let s = rows[0][0].as_deref().expect("avg");
            let v: f64 = s.parse().expect("parse avg");
            assert!(
                (v - (N + 1) as f64 / 2.0).abs() < 1e-6,
                "AVG(id)={s} 应≈10000.5"
            );
        }
        other => panic!("{other:?}"),
    }

    // GROUP BY age 组数 = 100
    match c.query("SELECT COUNT(DISTINCT age) FROM users") {
        QueryResult::Rows(rows) => {
            assert_eq!(rows[0][0].as_deref(), Some("100"), "age 组数");
        }
        other => panic!("{other:?}"),
    }
    // GROUP BY age + 每组合计数 (每组 N/100=200)
    match c.query("SELECT age, COUNT(*) AS cnt FROM users GROUP BY age ORDER BY age LIMIT 5") {
        QueryResult::Rows(rows) => {
            let expect: Vec<Vec<Option<String>>> = (0..5)
                .map(|a| vec![Some(a.to_string()), Some("200".to_string())])
                .collect();
            assert_eq!(rows, expect, "GROUP BY age LIMIT 5 每组合计");
        }
        other => panic!("{other:?}"),
    }
    // GROUP BY score 按范围聚合 + HAVING 等价检查 (score 0..999 每组 N/1000=20)
    match c.query("SELECT score, COUNT(*) FROM users GROUP BY score ORDER BY score LIMIT 3") {
        QueryResult::Rows(rows) => {
            let expect: Vec<Vec<Option<String>>> = (0..3)
                .map(|s| vec![Some(s.to_string()), Some("20".to_string())])
                .collect();
            assert_eq!(rows, expect, "GROUP BY score 每组 20");
        }
        other => panic!("{other:?}"),
    }

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

#[test]
fn bigdata_order_paging() {
    let (server, mgr) = start_sql_server();
    let mut c = MyConn::handshake_login(&server);
    c.query("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT, score DOUBLE)");
    for start in (1..=N).step_by(200) {
        let end = (start + 199).min(N);
        let mut vals = String::new();
        for i in start..=end {
            if !vals.is_empty() {
                vals.push_str(", ");
            }
            vals.push_str(&format!("({i}, 'u{i}', {}, {}.0)", i % 100, i % 1000));
        }
        c.query(&format!("INSERT INTO users VALUES {vals}"));
    }

    // ORDER BY id 前 10
    match c.query("SELECT id FROM users ORDER BY id LIMIT 10") {
        QueryResult::Rows(rows) => {
            let expect: Vec<Vec<Option<String>>> =
                (1..=10).map(|i| vec![Some(i.to_string())]).collect();
            assert_eq!(rows, expect, "ORDER BY id LIMIT 10");
        }
        other => panic!("{other:?}"),
    }
    // 倒序
    match c.query(&format!("SELECT id FROM users ORDER BY id DESC LIMIT 5")) {
        QueryResult::Rows(rows) => {
            let expect: Vec<Vec<Option<String>>> = (0..5)
                .map(|i| vec![Some((N - i).to_string())])
                .collect();
            assert_eq!(rows, expect, "ORDER BY id DESC");
        }
        other => panic!("{other:?}"),
    }
    // OFFSET 分页
    match c.query("SELECT id FROM users ORDER BY id LIMIT 5 OFFSET 100") {
        QueryResult::Rows(rows) => {
            let expect: Vec<Vec<Option<String>>> = (101..=105)
                .map(|i| vec![Some(i.to_string())])
                .collect();
            assert_eq!(rows, expect, "OFFSET 分页");
        }
        other => panic!("{other:?}"),
    }
    // 非主键排序: age 升序 (age=i%100, 稳定排序按 id) 前 5
    match c.query("SELECT id, age FROM users ORDER BY age, id LIMIT 5") {
        QueryResult::Rows(rows) => {
            // age=0 → i%100==0 → i=100,200,... 最小 id=100
            let expect: Vec<Vec<Option<String>>> = (1..=5)
                .map(|k| {
                    let i = 100 * k;
                    vec![Some(i.to_string()), Some("0".to_string())]
                })
                .collect();
            assert_eq!(rows, expect, "ORDER BY age,id");
        }
        other => panic!("{other:?}"),
    }

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

#[test]
fn bigdata_join() {
    let (server, mgr) = start_sql_server();
    let mut c = MyConn::handshake_login(&server);
    c.query("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT, score DOUBLE)");
    c.query("CREATE TABLE orders (id INT PRIMARY KEY, uid INT, amt DOUBLE)");

    // users: id 1..=N
    for start in (1..=N).step_by(200) {
        let end = (start + 199).min(N);
        let mut vals = String::new();
        for i in start..=end {
            if !vals.is_empty() {
                vals.push_str(", ");
            }
            vals.push_str(&format!("({i}, 'u{i}', {}, {}.0)", i % 100, i % 1000));
        }
        c.query(&format!("INSERT INTO users VALUES {vals}"));
    }
    // orders: id 1..=N/2 (10000 行), uid = i (与 users.id 一一对应前 10000), amt = (i%100).0
    for start in (1..=N / 2).step_by(200) {
        let end = (start + 199).min(N / 2);
        let mut vals = String::new();
        for i in start..=end {
            if !vals.is_empty() {
                vals.push_str(", ");
            }
            vals.push_str(&format!("({i}, {i}, {}.0)", i % 100));
        }
        c.query(&format!("INSERT INTO orders VALUES {vals}"));
    }

    // 注: 解析器明确不支持 JOIN 内聚合函数 (见 parser_select.rs:
    // "aggregate functions are not supported in JOIN queries"), 故 JOIN 校验走行集/单行.
    // JOIN 单行: uid=i 与 users.id=i 一一对应
    match c.query("SELECT u.name, o.amt FROM users u JOIN orders o ON u.id = o.uid WHERE u.id = 7") {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows,
                vec![vec![Some("u7".into()), Some("7".to_string())]],
                "JOIN 单行"
            );
        }
        other => panic!("{other:?}"),
    }
    // JOIN 多行: 前 3 个订单对应 u1..u3
    match c.query("SELECT u.name, o.amt FROM users u JOIN orders o ON u.id = o.uid ORDER BY o.id LIMIT 3") {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows,
                vec![
                    vec![Some("u1".into()), Some("1".to_string())],
                    vec![Some("u2".into()), Some("2".to_string())],
                    vec![Some("u3".into()), Some("3".to_string())],
                ],
                "JOIN 前 3 行"
            );
        }
        other => panic!("{other:?}"),
    }
    // JOIN 过滤下推: uid<=3 的订单 → 关联到 u1..u3
    match c.query("SELECT u.name FROM users u JOIN orders o ON u.id = o.uid WHERE o.uid <= 3 ORDER BY o.uid LIMIT 3") {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows,
                vec![
                    vec![Some("u1".into())],
                    vec![Some("u2".into())],
                    vec![Some("u3".into())],
                ],
                "JOIN WHERE 下推"
            );
        }
        other => panic!("{other:?}"),
    }
    // LEFT JOIN: 无订单用户 (id > N/2=10000) 右列为 NULL
    match c.query(&format!(
        "SELECT u.id, o.amt FROM users u LEFT JOIN orders o ON u.id = o.uid WHERE u.id = {}",
        N / 2 + 5
    )) {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows,
                vec![vec![Some((N / 2 + 5).to_string()), None]],
                "LEFT JOIN 补 NULL"
            );
        }
        other => panic!("{other:?}"),
    }
    // LEFT JOIN: 有订单用户右列非 NULL
    match c.query("SELECT u.id, o.amt FROM users u LEFT JOIN orders o ON u.id = o.uid WHERE u.id = 42") {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows,
                vec![vec![Some("42".into()), Some("42".to_string())]],
                "LEFT JOIN 有订单"
            );
        }
        other => panic!("{other:?}"),
    }

    drop(c);
    server.shutdown().unwrap();
    drop(mgr);
}

#[test]
fn bigdata_update_delete_consistency() {
    let (server, mgr) = start_sql_server();
    let mut c = MyConn::handshake_login(&server);
    c.query("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT, score DOUBLE)");
    for start in (1..=N).step_by(200) {
        let end = (start + 199).min(N);
        let mut vals = String::new();
        for i in start..=end {
            if !vals.is_empty() {
                vals.push_str(", ");
            }
            vals.push_str(&format!("({i}, 'u{i}', {}, {}.0)", i % 100, i % 1000));
        }
        c.query(&format!("INSERT INTO users VALUES {vals}"));
    }

    // UPDATE 大数据: 把 age 全部 +1 (影响 N 行)
    match c.query("UPDATE users SET age = age + 1") {
        QueryResult::Ok { affected } => {
            assert_eq!(affected, N as u64, "UPDATE 影响行数");
        }
        other => panic!("{other:?}"),
    }
    // 校验: age=51 的计数 = 原 age=50 的计数 = 200
    match c.query("SELECT COUNT(*) FROM users WHERE age = 51") {
        QueryResult::Rows(rows) => {
            assert_eq!(rows[0][0].as_deref(), Some("200"), "UPDATE 后 age=51");
        }
        other => panic!("{other:?}"),
    }
    // 条件 UPDATE: 前一半 id<=N/2 的 score 置 0
    match c.query(&format!("UPDATE users SET score = 0 WHERE id <= {}", N / 2)) {
        QueryResult::Ok { affected } => {
            assert_eq!(affected, (N / 2) as u64, "条件 UPDATE 影响行数");
        }
        other => panic!("{other:?}"),
    }
    // score=0 计数 = N/2 (UPDATE 置 0) + 10 (原本 id>N/2 且 i%1000==0 的行: 11000..20000 步长 1000)
    match c.query("SELECT COUNT(*) FROM users WHERE score = 0") {
        QueryResult::Rows(rows) => {
            assert_eq!(rows[0][0].as_deref(), Some((N / 2 + 10).to_string().as_str()), "score=0");
        }
        other => panic!("{other:?}"),
    }

    // DELETE 大数据: 删除后一半 id>N/2 (10000 行), 剩前一半
    match c.query(&format!("DELETE FROM users WHERE id > {}", N / 2)) {
        QueryResult::Ok { affected } => {
            assert_eq!(affected, (N / 2) as u64, "DELETE 影响行数");
        }
        other => panic!("{other:?}"),
    }
    match c.query("SELECT COUNT(*) FROM users") {
        QueryResult::Rows(rows) => {
            assert_eq!(rows[0][0].as_deref(), Some((N / 2).to_string().as_str()), "删半后计数");
        }
        other => panic!("{other:?}"),
    }
    // 删除后剩前一半 (score 均已置 0)
    match c.query("SELECT COUNT(*) FROM users WHERE score = 0") {
        QueryResult::Rows(rows) => {
            assert_eq!(rows[0][0].as_deref(), Some((N / 2).to_string().as_str()), "剩余 score 全 0");
        }
        other => panic!("{other:?}"),
    }

    // 重连验证数据持久 (数据已落盘, 新连接应看到一致状态)
    drop(c);
    let mut c2 = MyConn::handshake_login(&server);
    match c2.query("SELECT COUNT(*) FROM users") {
        QueryResult::Rows(rows) => {
            assert_eq!(rows[0][0].as_deref(), Some((N / 2).to_string().as_str()), "重连后计数");
        }
        other => panic!("{other:?}"),
    }
    match c2.query("SELECT COUNT(*) FROM users WHERE score = 0") {
        QueryResult::Rows(rows) => {
            assert_eq!(rows[0][0].as_deref(), Some((N / 2).to_string().as_str()), "重连后剩余全 score=0");
        }
        other => panic!("{other:?}"),
    }
    // 重连后最大 id 应为 N/2 (后一半已删)
    match c2.query("SELECT MAX(id) FROM users") {
        QueryResult::Rows(rows) => {
            assert_eq!(rows[0][0].as_deref(), Some((N / 2).to_string().as_str()), "重连后 MAX(id)");
        }
        other => panic!("{other:?}"),
    }

    drop(c2);
    server.shutdown().unwrap();
    drop(mgr);
}
