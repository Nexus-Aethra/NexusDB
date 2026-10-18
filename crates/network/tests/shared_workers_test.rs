//! ⭐ Phase 3 / T3.1: 全局共享 worker 池验证.
//!
//! 多个协议 server (RESP + SQL) 共享同一个 `SharedWorkerPool`:
//! - 线程数 = 池大小 (2), 不随 server 数膨胀 (各协议仍各建池则为 4)
//! - RESP 门面与 SQL 门面在同一批 worker 上正常服务 (协议 per-conn 分发)
//!
//! 运行: 支持 epoll + 协程 worker (NEXUS_CORO_WORKER=1).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use network::protocol::mysql as my;
use network::{KvLimits, NetworkServer, NetworkServerConfig, ProtocolKind, SharedWorkerPool};
use shard_manager::{ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

fn open_mgr() -> Arc<ShardManager> {
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
    mgr
}

fn base_cfg(mgr: &Arc<ShardManager>, protocol: ProtocolKind) -> NetworkServerConfig {
    base_cfg_auth(mgr, protocol, None)
}

fn base_cfg_auth(
    mgr: &Arc<ShardManager>,
    protocol: ProtocolKind,
    auth_password: Option<String>,
) -> NetworkServerConfig {
    NetworkServerConfig {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        shard_manager: mgr.clone(),
        worker_count: 2,
        default_db: "app".to_string(),
        default_table: "kv".to_string(),
        inbox_capacity: 64,
        protocol,
        limits: KvLimits::default(),
        auth_password,
        worker_id_base: 0,
        sql_shared: network::new_sql_shared(),
        tls_config: None,
        shared_workers: None,
    }
}

// ===== RESP 客户端 (简化) =====
fn resp_cmd(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// auth: None = 免认证; Some(pw) = 先 AUTH 再操作.
fn resp_set_get(addr: SocketAddr, auth: Option<&str>) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut buf = [0u8; 64];
    if let Some(pw) = auth {
        // 无 AUTH 先操作应被拒
        s.write_all(&resp_cmd(&[b"SET", b"k0", b"x"])).unwrap();
        let n = s.read(&mut buf).unwrap();
        assert_eq!(
            &buf[..n],
            b"-NOAUTH Authentication required.\r\n",
            "未认证应拒"
        );
        // AUTH
        s.write_all(&resp_cmd(&[b"AUTH", pw.as_bytes()])).unwrap();
        let n = s.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"+OK\r\n", "AUTH should OK");
    }
    s.write_all(&resp_cmd(&[b"SET", b"k1", b"v1"])).unwrap();
    let n = s.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"+OK\r\n", "SET should OK");
    s.write_all(&resp_cmd(&[b"GET", b"k1"])).unwrap();
    let n = s.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"$2\r\nv1\r\n", "GET should return v1");
}

// ===== MySQL 客户端 (简化: 握手 + 一条查询) =====
fn sql_ping(addr: SocketAddr) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // 读握手
    let mut buf = [0u8; 4096];
    let mut got = 0;
    loop {
        if got > 4 {
            let plen = (buf[0] as usize) | ((buf[1] as usize) << 8) | ((buf[2] as usize) << 16);
            if got >= plen + 4 {
                break;
            }
        }
        let n = s.read(&mut buf[got..]).unwrap();
        assert!(n > 0);
        got += n;
    }
    // 发握手响应 (空密码, root)
    let p = &buf[..got];
    let mut pos = 1;
    while p[pos] != 0 {
        pos += 1;
    }
    pos += 1 + 4;
    let mut salt = [0u8; 20];
    salt[..8].copy_from_slice(&p[pos..pos + 8]);
    pos += 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10;
    salt[8..].copy_from_slice(&p[pos..pos + 12]);
    let token = my::native_password_token(&salt, "");
    let flags = my::CLIENT_PROTOCOL_41 | my::CLIENT_SECURE_CONNECTION | my::CLIENT_PLUGIN_AUTH;
    let mut resp = Vec::new();
    resp.extend_from_slice(&flags.to_le_bytes());
    resp.extend_from_slice(&0x0100_0000u32.to_le_bytes());
    resp.push(45);
    resp.extend_from_slice(&[0u8; 23]);
    resp.extend_from_slice(b"root\0");
    resp.push(token.len() as u8);
    resp.extend_from_slice(&token);
    resp.extend_from_slice(b"mysql_native_password\0");
    let pkt = my::write_packet(1, &resp);
    s.write_all(&pkt).unwrap();
    // 读登录响应 (OK 包): 读够一个包, 解析 payload 检查首字节 0x00.
    got = 0;
    loop {
        if got > 4 {
            let plen = (buf[0] as usize) | ((buf[1] as usize) << 8) | ((buf[2] as usize) << 16);
            if got >= plen + 4 {
                let seq = buf[3];
                assert_eq!(seq, 2, "login resp seq should be 2, got {seq}");
                assert_eq!(
                    buf[4],
                    0x00,
                    "login should succeed, payload={:?}",
                    &buf[4..4 + plen]
                );
                return;
            }
        }
        let n = s.read(&mut buf[got..]).unwrap();
        assert!(n > 0);
        got += n;
    }
}

/// 进程内 network-worker 线程数 (epoll: network-worker-*; coro: network-worker-coro-*).
fn worker_thread_count() -> usize {
    let mut count = 0;
    if let Ok(entries) = std::fs::read_dir("/proc/self/task") {
        for e in entries.flatten() {
            let tid = e.file_name().to_string_lossy().into_owned();
            let name_path = format!("/proc/self/task/{tid}/comm");
            if let Ok(name) = std::fs::read_to_string(name_path) {
                let n = name.trim();
                if n.starts_with("network-worker") {
                    count += 1;
                }
            }
        }
    }
    count
}

/// 等 worker 线程名可见再返回计数. `thread::spawn` 返回时新线程可能还没被
/// 调度 (pthread_setname_np 未执行, comm 仍是进程名) — CI 高负载下实测
/// 直接数会数到 0 (2026-10-17 main 推送与 PR 各挂一次). 有界轮询到期望值
/// 或超时; 断言语义不变 (池膨胀成 4 仍会以 actual=4 失败).
fn wait_worker_count(expected: usize) -> usize {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let n = worker_thread_count();
        if n == expected || std::time::Instant::now() >= deadline {
            return n;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

// ===== 并行安全 =====

/// 三个用例都断言进程级 worker 线程数, 并行执行会互相串数 (实测: 并行下
/// `assert_eq!(workers, 2)` 看到 4 而 panic; 且 panic unwind 不走 shutdown,
/// 泄漏的 acceptor 持着 channel sender → `SharedWorkerPool::drop` 的 join
/// 永久死锁, 整个测试二进制挂死).
/// 文件级互斥把三个用例串行化; 与其他测试文件天然隔离 (各自独立进程).
static SHARED_POOL_TESTS: Mutex<()> = Mutex::new(());

fn pool_test_lock() -> MutexGuard<'static, ()> {
    // 忽略 poison: 前一个用例 panic 不应连带本用例
    SHARED_POOL_TESTS.lock().unwrap_or_else(|e| e.into_inner())
}

/// panic 安全的收尾: 顺序 = servers shutdown → pool drop → mgr drop.
/// 该顺序即 `SharedWorkerPool::drop` 的契约 — 所有 server 停用 (sender 全部
/// 释放) 之后才能 join workers, 否则 worker 的 inbox 永不断开, join 永久阻塞.
struct PoolTestEnv {
    servers: Vec<NetworkServer>,
    pool: Option<Arc<SharedWorkerPool>>,
    mgr: Option<Arc<ShardManager>>,
}

impl PoolTestEnv {
    fn new(
        servers: Vec<NetworkServer>,
        pool: Arc<SharedWorkerPool>,
        mgr: Arc<ShardManager>,
    ) -> Self {
        Self {
            servers,
            pool: Some(pool),
            mgr: Some(mgr),
        }
    }
}

impl Drop for PoolTestEnv {
    fn drop(&mut self) {
        for s in self.servers.drain(..) {
            let _ = s.shutdown();
        }
        // 最后引用归零: SharedWorkerPool::drop → sender 释放 → join workers
        drop(self.pool.take());
        // ShardManager::drop → flush + 停 shard 线程 (RAII 兜底)
        drop(self.mgr.take());
    }
}

#[test]
fn shared_worker_pool_resp_and_sql() {
    let _lock = pool_test_lock();
    let mgr = open_mgr();
    let shared =
        SharedWorkerPool::new(&base_cfg(&mgr, ProtocolKind::Resp), 0).expect("shared pool");

    // 两个 server 共享同一池 (不同协议)
    let mut cfg_a = base_cfg(&mgr, ProtocolKind::Resp);
    cfg_a.shared_workers = Some(shared.clone());
    let mut cfg_b = base_cfg(&mgr, ProtocolKind::Sql);
    cfg_b.shared_workers = Some(shared.clone());
    let server_a = NetworkServer::start(cfg_a).expect("start RESP");
    let server_b = NetworkServer::start(cfg_b).expect("start SQL");
    let env = PoolTestEnv::new(vec![server_a, server_b], shared, mgr);

    // 线程数 = 池大小 (2), 而非 2 server × 2 = 4
    let workers = wait_worker_count(2);
    assert_eq!(workers, 2, "共享池应只有 2 个 worker 线程, 实际 {workers}");

    // 功能: RESP + SQL 在同一批 worker 上正常
    resp_set_get(env.servers[0].local_addr(), None);
    sql_ping(env.servers[1].local_addr());
    // env drop: shutdown servers → drop pool (join workers) → drop mgr
}

/// T3.3: 共享池中不同 server 用不同 auth_password — worker 按连接取 per-conn 配置.
/// RESP server 需 AUTH (secret), SQL server 免认证. 同一批 worker 正确区分.
#[test]
fn shared_worker_pool_per_conn_auth() {
    let _lock = pool_test_lock();
    let mgr = open_mgr();
    let shared =
        SharedWorkerPool::new(&base_cfg(&mgr, ProtocolKind::Resp), 0).expect("shared pool");

    let mut cfg_a = base_cfg_auth(&mgr, ProtocolKind::Resp, Some("secret".to_string()));
    cfg_a.shared_workers = Some(shared.clone());
    let mut cfg_b = base_cfg(&mgr, ProtocolKind::Sql);
    cfg_b.shared_workers = Some(shared.clone());
    let server_a = NetworkServer::start(cfg_a).expect("start RESP (auth)");
    let server_b = NetworkServer::start(cfg_b).expect("start SQL (no auth)");
    let env = PoolTestEnv::new(vec![server_a, server_b], shared, mgr);

    // 同一批 worker (2 个) 处理两个协议, 且 RESP 需 AUTH
    let workers = wait_worker_count(2);
    assert_eq!(workers, 2, "共享池应只有 2 个 worker, 实际 {workers}");
    resp_set_get(env.servers[0].local_addr(), Some("secret"));
    sql_ping(env.servers[1].local_addr());
    // env drop: panic 安全收尾
}

/// T3.4: 多协议混合压力 — 同一共享池同时处理 RESP + SQL 多连接并发, 无串扰.
#[test]
fn shared_worker_pool_mixed_load() {
    let _lock = pool_test_lock();
    let mgr = open_mgr();
    let shared =
        SharedWorkerPool::new(&base_cfg(&mgr, ProtocolKind::Resp), 0).expect("shared pool");

    let mut cfg_a = base_cfg(&mgr, ProtocolKind::Resp);
    cfg_a.shared_workers = Some(shared.clone());
    let mut cfg_b = base_cfg(&mgr, ProtocolKind::Sql);
    cfg_b.shared_workers = Some(shared.clone());
    let server_a = NetworkServer::start(cfg_a).expect("start RESP");
    let server_b = NetworkServer::start(cfg_b).expect("start SQL");
    let env = PoolTestEnv::new(vec![server_a, server_b], shared, mgr);

    // 并发: 8 RESP 连接 + 8 SQL 连接, 每连接多次往返.
    let mut handles = Vec::new();
    for i in 0..8 {
        let addr_a = env.servers[0].local_addr();
        let addr_b = env.servers[1].local_addr();
        handles.push(std::thread::spawn(move || {
            // RESP: 每连接独立 key, SET+GET 往返多次
            for j in 0..10 {
                let mut s = TcpStream::connect(addr_a).unwrap();
                s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                let key = format!("k{i}_{j}");
                let val = format!("v{i}_{j}");
                s.write_all(&resp_cmd(&[b"SET", key.as_bytes(), val.as_bytes()]))
                    .unwrap();
                let mut buf = [0u8; 64];
                let n = s.read(&mut buf).unwrap();
                assert_eq!(&buf[..n], b"+OK\r\n");
                s.write_all(&resp_cmd(&[b"GET", key.as_bytes()])).unwrap();
                let n = s.read(&mut buf).unwrap();
                assert_eq!(
                    &buf[..n],
                    format!("${}\r\n{}\r\n", val.len(), val).as_bytes()
                );
            }
        }));
        handles.push(std::thread::spawn(move || {
            // SQL: 多次握手 (每次新连接)
            for _ in 0..10 {
                sql_ping(addr_b);
            }
        }));
    }
    for h in handles {
        h.join().expect("load thread panicked");
    }

    // env drop: panic 安全收尾
}
