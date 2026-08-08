//! ⭐ Phase 1b 验证: 协程 worker 端到端 (握手 + 简单查询).
//! 需 `NEXUS_CORO_WORKER=1` 环境变量启用协程 worker.
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use network::protocol::mysql as my;
use network::{KvLimits, NetworkServer, NetworkServerConfig, ProtocolKind};
use shard_manager::{ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

fn start() -> (NetworkServer, Arc<ShardManager>) {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions {
        num_shards: 1,
        block_root: tmp.path().to_path_buf(),
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 4,
        reply_bus_count: None,
        wal_mode: Default::default(),
    };
    let mgr = Arc::new(ShardManager::open(opts).unwrap());
    mgr.create_db("app").unwrap();
    mgr.create_table("app", "kv").unwrap();
    std::mem::forget(tmp);
    let cfg = NetworkServerConfig {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        shard_manager: mgr.clone(),
        worker_count: 1,
        default_db: "app".into(),
        default_table: "kv".into(),
        inbox_capacity: 64,
        protocol: ProtocolKind::Sql,
        limits: KvLimits::default(),
        auth_password: None,
        worker_id_base: 0,
        sql_shared: network::new_sql_shared(),
        tls_config: None,
        shared_workers: None,
    };
    (NetworkServer::start(cfg).unwrap(), mgr)
}

struct Conn {
    stream: TcpStream,
    buf: Vec<u8>,
}
impl Conn {
    fn new(s: &NetworkServer) -> Self {
        let stream = TcpStream::connect(s.local_addr()).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        Self { stream, buf: Vec::new() }
    }
    fn frame(&mut self) -> Vec<u8> {
        loop {
            if let Some((_, n, p)) = my::read_packet(&self.buf) {
                self.buf.drain(..n);
                return p;
            }
            let mut t = [0u8; 4096];
            let n = self.stream.read(&mut t).unwrap();
            assert!(n > 0, "connection closed");
            self.buf.extend_from_slice(&t[..n]);
        }
    }
    fn login(&mut self, s: &NetworkServer) {
        let p = self.frame();
        let mut pos = 1;
        while p[pos] != 0 { pos += 1; }
        pos += 1 + 4;
        let mut salt = [0u8; 20];
        salt[..8].copy_from_slice(&p[pos..pos + 8]);
        pos += 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10;
        salt[8..].copy_from_slice(&p[pos..pos + 12]);
        let tok = my::native_password_token(&salt, "");
        let mut r = vec![
            my::CLIENT_PROTOCOL_41 as u32 | my::CLIENT_SECURE_CONNECTION as u32 | my::CLIENT_PLUGIN_AUTH as u32,
        ]
        .into_iter()
        .flat_map(|x| x.to_le_bytes())
        .collect::<Vec<_>>();
        r.extend_from_slice(&0x0100_0000u32.to_le_bytes());
        r.push(45);
        r.extend_from_slice(&[0u8; 23]);
        r.extend_from_slice(b"root\0");
        r.push(tok.len() as u8);
        r.extend_from_slice(&tok);
        r.extend_from_slice(b"mysql_native_password\0");
        self.stream.write_all(&my::write_packet(1, &r)).unwrap();
        let resp = self.frame();
        assert_eq!(resp[0], 0, "login failed: {resp:?}");
    }
    fn q(&mut self, sql: &str) -> Vec<u8> {
        let mut p = vec![my::COM_QUERY];
        p.extend_from_slice(sql.as_bytes());
        self.stream.write_all(&my::write_packet(0, &p)).unwrap();
        let f = self.frame();
        if f[0] == 0x00 {
            return f; // OK 包
        }
        // 结果集: 首字节 = lenenc 列数; 读 列定义*ncols + EOF + 行 + EOF, 消费完.
        let ncols = f[0] as usize;
        for _ in 0..ncols {
            self.frame();
        }
        self.frame(); // EOF after columns
        loop {
            let rp = self.frame();
            if rp[0] == 0xFE && rp.len() < 9 {
                break; // EOF after rows
            }
        }
        f
    }
}

#[test]
fn coro_worker_e2e_sql_query() {
    let (s, mgr) = start();
    let mut c = Conn::new(&s);
    c.login(&s);
    let r = c.q("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)");
    assert_eq!(r[0], 0, "CREATE TABLE ok");
    let r = c.q("INSERT INTO users VALUES (1, 'alice')");
    assert_eq!(r[0], 0, "INSERT ok");
    let r = c.q("SELECT COUNT(*) FROM users");
    assert_eq!(r[0], 1, "SELECT COUNT returns 1 column: {r:?}");
    drop(c);
    s.shutdown().unwrap();
    drop(mgr);
}
