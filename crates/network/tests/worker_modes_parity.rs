//! ⭐ 审计 P3-3: epoll worker 与协程 worker 的协议分派 parity.
//!
//! 协议分派 `match conn.proto` 曾在 `worker_epoll.rs` 与 `worker_coro.rs`
//! 各写一份 (输入 + 回复两条 match) — 新增协议/改参数时改一处漏一处,
//! 两路径行为静默漂移 (审计 P3-3). 现已收敛到 `worker::dispatch` 公共层.
//!
//! 本测试把同一组多协议客户端行为在两种 worker 模式下各跑一遍并逐项断言
//! 一致: 公共层把某协议接错线 (如 Sql 臂接成 Pg 处理器) 时, 只有对应
//! 模式失败 — 立即暴露, 不靠人眼对比两份 match.
//!
//! 覆盖 5 个协议门面的本地路径 (不依赖 shard 数据面):
//! - RESP: SET/GET 往返 (含 shard 路由 + 回复重排)
//! - MySQL: 握手 + `SELECT 1`
//! - PG: SSLRequest → 'N' 明文回落
//! - HTTP: `GET /v1/status` → 200
//! - Binary: PING 帧 → PONG 帧
//!
//! 运行: 默认 epoll worker; `NEXUS_CORO_WORKER=1` 切协程 worker (env 是
//! 进程级的, 本文件用互斥锁串行化并在用完即删 — 不同测试文件是独立进程).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use network::protocol::mysql as my;
use network::{KvLimits, NetworkServer, NetworkServerConfig, ProtocolKind};
use shard_manager::{ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

static MODE_GUARD: Mutex<()> = Mutex::new(());

fn mode_lock() -> MutexGuard<'static, ()> {
    // 忽略 poison: 前一个模式 panic 不应连带本模式
    MODE_GUARD.lock().unwrap_or_else(|e| e.into_inner())
}

fn open_mgr() -> Arc<ShardManager> {
    let tmp = tempfile::tempdir().expect("tempdir");
    let opts = ShardManagerOptions {
        num_shards: 3,
        block_root: tmp.path().to_path_buf(),
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 4,
        // 5 个 server 各自独立 worker 池: worker_id 必须隔开, 否则共享
        // reply_bus slot, 回复会被别的 server 的 worker 吃掉 (随机丢回复).
        reply_bus_count: Some(8),
        wal_mode: Default::default(),
    };
    let mgr = Arc::new(ShardManager::open(opts).expect("open mgr"));
    mgr.create_db("app").expect("create db");
    mgr.create_table("app", "kv").expect("create table");
    std::mem::forget(tmp);
    mgr
}

fn cfg(
    mgr: &Arc<ShardManager>,
    protocol: ProtocolKind,
    worker_id_base: u32,
) -> NetworkServerConfig {
    NetworkServerConfig {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        shard_manager: mgr.clone(),
        worker_count: 1,
        default_db: "app".to_string(),
        default_table: "kv".to_string(),
        inbox_capacity: 64,
        protocol,
        limits: KvLimits::default(),
        auth_password: None,
        worker_id_base,
        sql_shared: network::new_sql_shared(),
        tls_config: None,
        shared_workers: None,
    }
}

/// 一次多协议冒烟的观测结果 (两模式逐字段对比).
#[derive(Debug, PartialEq, Eq)]
struct Smoke {
    resp_set_get: bool,
    mysql_select1_cols: u8,
    pg_ssl_reply: u8,
    http_ok: bool,
    binary_ping_pong: bool,
}

fn resp_cmd(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn read_some(s: &mut TcpStream) -> Vec<u8> {
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        match s.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                if !buf.is_empty() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    buf
}

fn resp_set_get(addr: SocketAddr) -> bool {
    let mut s = TcpStream::connect(addr).unwrap();
    s.write_all(&resp_cmd(&[b"SET", b"k1", b"v1"])).unwrap();
    if read_some(&mut s) != b"+OK\r\n" {
        return false;
    }
    s.write_all(&resp_cmd(&[b"GET", b"k1"])).unwrap();
    read_some(&mut s) == b"$2\r\nv1\r\n"
}

/// MySQL: 握手 + COM_QUERY "SELECT 1". 返回结果集列数 (ERR 包则 0xff).
fn mysql_select1(addr: SocketAddr) -> u8 {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    // 读 HandshakeV10, 解析 salt
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
        assert!(n > 0, "mysql handshake closed");
        got += n;
    }
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
    s.write_all(&my::write_packet(1, &resp)).unwrap();
    // 登录 OK
    got = 0;
    loop {
        if got > 4 {
            let plen = (buf[0] as usize) | ((buf[1] as usize) << 8) | ((buf[2] as usize) << 16);
            if got >= plen + 4 {
                assert_eq!(buf[4], 0x00, "mysql login should succeed");
                break;
            }
        }
        let n = s.read(&mut buf[got..]).unwrap();
        assert!(n > 0, "mysql login closed");
        got += n;
    }
    // COM_QUERY "SELECT 1" → 结果集: 首包 = 列数 (1)
    let mut q = vec![0x03];
    q.extend_from_slice(b"SELECT 1");
    s.write_all(&my::write_packet(0, &q)).unwrap();
    let reply = read_some(&mut s);
    if reply.len() < 5 {
        return 0xff;
    }
    reply[4] // 列数 (ERR 包则是 0xff)
}

fn pg_ssl_request(addr: SocketAddr) -> u8 {
    let mut s = TcpStream::connect(addr).unwrap();
    let mut req = Vec::new();
    req.extend_from_slice(&8u32.to_be_bytes());
    req.extend_from_slice(&network::protocol::pg::SSL_REQUEST_CODE.to_be_bytes());
    s.write_all(&req).unwrap();
    let reply = read_some(&mut s);
    reply.first().copied().unwrap_or(0)
}

fn http_status(addr: SocketAddr) -> bool {
    let mut s = TcpStream::connect(addr).unwrap();
    s.write_all(b"GET /v1/status HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .unwrap();
    let reply = read_some(&mut s);
    reply.starts_with(b"HTTP/1.1 200")
}

/// Binary 协议: PUT/GET 往返 (经 shard 路由 + 回复直发, 无重排).
fn binary_put_get(addr: SocketAddr) -> bool {
    use network::protocol::{BinaryProtocol, DecodeOutcome, Protocol, Request};
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let put = BinaryProtocol::new().encode_request(
        1,
        &Request::Put {
            key: b"bk".to_vec(),
            value: b"bv".to_vec(),
        },
    );
    s.write_all(&put).unwrap();
    let _put_reply = read_some(&mut s);
    let get = BinaryProtocol::new().encode_request(
        2,
        &Request::Get {
            key: b"bk".to_vec(),
        },
    );
    s.write_all(&get).unwrap();
    let reply = read_some(&mut s);
    match BinaryProtocol::new().decode_response(&reply) {
        Ok(DecodeOutcome::Complete { value, .. }) => {
            matches!(value, network::Response::Get(Some(v)) if v == b"bv")
        }
        _ => false,
    }
}

fn run_smoke(mgr: &Arc<ShardManager>, coro: bool) -> Smoke {
    if coro {
        // SAFETY: 进程级 env, 由 MODE_GUARD 串行化; 仅本测试二进制读写.
        unsafe { std::env::set_var("NEXUS_CORO_WORKER", "1") };
    }
    let servers: Vec<NetworkServer> = [
        cfg(mgr, ProtocolKind::Resp, 0),
        cfg(mgr, ProtocolKind::Sql, 1),
        cfg(mgr, ProtocolKind::Pg, 2),
        cfg(mgr, ProtocolKind::Http, 3),
        cfg(mgr, ProtocolKind::Binary, 4),
    ]
    .into_iter()
    .map(|c| NetworkServer::start(c).expect("start server"))
    .collect();
    let addrs: Vec<SocketAddr> = servers.iter().map(|s| s.local_addr()).collect();

    let out = Smoke {
        resp_set_get: resp_set_get(addrs[0]),
        mysql_select1_cols: mysql_select1(addrs[1]),
        pg_ssl_reply: pg_ssl_request(addrs[2]),
        http_ok: http_status(addrs[3]),
        binary_ping_pong: binary_put_get(addrs[4]),
    };

    for s in servers {
        let _ = s.shutdown();
    }
    if coro {
        // SAFETY: 同上, 用完即删, 不波及其他测试.
        unsafe { std::env::remove_var("NEXUS_CORO_WORKER") };
    }
    out
}

#[test]
fn dispatch_parity_between_epoll_and_coro_worker() {
    let _lock = mode_lock();
    let mgr = open_mgr();
    let epoll = run_smoke(&mgr, false);
    let coro = run_smoke(&mgr, true);
    assert_eq!(
        epoll, coro,
        "两种 worker 模式的协议分派行为不一致 (公共层接错线?)"
    );
    // 健全性: 不能两边一起坏 (parity 相等但全坏也意味着公共层接错)
    assert!(epoll.resp_set_get, "RESP SET/GET 应成功");
    assert_eq!(epoll.mysql_select1_cols, 1, "SELECT 1 应返回 1 列结果集");
    assert_eq!(epoll.pg_ssl_reply, b'N', "PG SSLRequest 应回落明文 'N'");
    assert!(epoll.http_ok, "HTTP /v1/status 应 200");
    assert!(epoll.binary_ping_pong, "Binary PING 应回 PONG");
}
