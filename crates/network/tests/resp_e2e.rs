//! RESP (Redis 兼容) 门面 e2e 测试.
//!
//! 原生 TcpStream 手写 RESP2 帧, 走完整链路:
//! client → acceptor → worker (epoll) → shard → reply_bus → worker → client.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use network::{KvLimits, NetworkServer, NetworkServerConfig, ProtocolKind};
use shard_manager::{ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

// ===== helpers =====

fn start_server(auth_password: Option<&str>) -> (NetworkServer, Arc<ShardManager>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let opts = ShardManagerOptions {
        num_shards: 3,
        block_root: tmp.path().to_path_buf(),
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 4,
        reply_bus_count: None,
    };
    let mgr = Arc::new(ShardManager::open(opts).expect("open mgr"));
    mgr.create_db("app").expect("create db");
    mgr.create_table("app", "kv").expect("create table");
    // tempdir 生命周期: 测试进程内泄漏, 进程退出自动清理
    std::mem::forget(tmp);

    let cfg = NetworkServerConfig {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        shard_manager: mgr.clone(),
        worker_count: 2,
        default_db: "app".to_string(),
        default_table: "kv".to_string(),
        inbox_capacity: 64,
        protocol: ProtocolKind::Resp,
        limits: KvLimits::default(),
        auth_password: auth_password.map(|s| s.to_string()),
        worker_id_base: 0,
    };
    let server = NetworkServer::start(cfg).expect("start server");
    (server, mgr)
}

fn connect(server: &NetworkServer) -> TcpStream {
    let stream = TcpStream::connect(server.local_addr()).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
}

/// 编码 RESP array-of-bulk-strings 命令.
fn cmd(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// 读取直到 buf 中包含 n 条完整 RESP 回复, 返回按顺序切分的回复.
fn read_replies(stream: &mut TcpStream, n: usize) -> Vec<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let replies = split_replies(&buf);
        if replies.len() >= n {
            return replies.into_iter().take(n).collect();
        }
        let got = stream.read(&mut tmp).expect("read");
        assert!(got > 0, "connection closed while waiting for replies");
        buf.extend_from_slice(&tmp[..got]);
    }
}

/// 按 RESP 帧边界切分回复 (支持 + - : $ *).
fn split_replies(buf: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < buf.len() {
        match reply_len(&buf[pos..]) {
            Some(n) => {
                out.push(buf[pos..pos + n].to_vec());
                pos += n;
            }
            None => break,
        }
    }
    out
}

/// 单条 RESP 回复的字节长度 (不完整返回 None).
fn reply_len(buf: &[u8]) -> Option<usize> {
    if buf.is_empty() {
        return None;
    }
    let line_end = find_crlf(buf)?;
    match buf[0] {
        b'+' | b'-' | b':' => Some(line_end + 2),
        b'$' => {
            let n: i64 = std::str::from_utf8(&buf[1..line_end]).ok()?.parse().ok()?;
            if n < 0 {
                Some(line_end + 2) // $-1
            } else {
                let total = line_end + 2 + n as usize + 2;
                (buf.len() >= total).then_some(total)
            }
        }
        b'*' => {
            let n: i64 = std::str::from_utf8(&buf[1..line_end]).ok()?.parse().ok()?;
            let mut pos = line_end + 2;
            for _ in 0..n.max(0) {
                let inner = reply_len(&buf[pos..])?;
                pos += inner;
            }
            Some(pos)
        }
        _ => None,
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

// ===== tests =====

#[test]
fn resp_set_get_del_ping_roundtrip() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    s.write_all(&cmd(&[b"PING"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+PONG\r\n");

    s.write_all(&cmd(&[b"SET", b"k1", b"v_hello"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");

    s.write_all(&cmd(&[b"GET", b"k1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$7\r\nv_hello\r\n");

    // GET 不存在 → nil
    s.write_all(&cmd(&[b"GET", b"nope"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");

    // DEL 两个 key (一个存在一个不存在) → :1
    s.write_all(&cmd(&[b"DEL", b"k1", b"nope"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");

    // 删除后 GET → nil
    s.write_all(&cmd(&[b"GET", b"k1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

#[test]
fn resp_auth_flow() {
    let (server, mgr) = start_server(Some("s3cret"));
    let mut s = connect(&server);

    // 未认证发 SET → NOAUTH
    s.write_all(&cmd(&[b"SET", b"k", b"v"])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(r[0].starts_with(b"-NOAUTH"), "{:?}", String::from_utf8_lossy(&r[0]));

    // 错密码 → WRONGPASS
    s.write_all(&cmd(&[b"AUTH", b"wrong"])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(r[0].starts_with(b"-WRONGPASS"), "{:?}", String::from_utf8_lossy(&r[0]));

    // 对密码 → +OK, 之后 SET/GET 正常
    s.write_all(&cmd(&[b"AUTH", b"s3cret"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");

    s.write_all(&cmd(&[b"SET", b"k", b"v"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"GET", b"k"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\nv\r\n");

    // AUTH user 形式 (user=default)
    s.write_all(&cmd(&[b"AUTH", b"default", b"s3cret"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

#[test]
fn resp_auth_without_password_configured() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    s.write_all(&cmd(&[b"AUTH", b"whatever"])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(
        r[0].starts_with(b"-ERR Client sent AUTH"),
        "{:?}",
        String::from_utf8_lossy(&r[0])
    );

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

#[test]
fn resp_pipeline_replies_in_order() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // 一次写入 3 条命令: SET a 1; PING; GET a — 回复必须严格按序
    let mut batch = Vec::new();
    batch.extend_from_slice(&cmd(&[b"SET", b"a", b"1"]));
    batch.extend_from_slice(&cmd(&[b"PING"]));
    batch.extend_from_slice(&cmd(&[b"GET", b"a"]));
    s.write_all(&batch).unwrap();

    let replies = read_replies(&mut s, 3);
    assert_eq!(replies[0], b"+OK\r\n");
    assert_eq!(replies[1], b"+PONG\r\n");
    assert_eq!(replies[2], b"$1\r\n1\r\n");

    // 大 pipeline: 32 组 SET+GET 交错, 校验回复顺序与值
    let mut batch = Vec::new();
    for i in 0..32 {
        let k = format!("pk{i:02}");
        let v = format!("pv{i:02}");
        batch.extend_from_slice(&cmd(&[b"SET", k.as_bytes(), v.as_bytes()]));
        batch.extend_from_slice(&cmd(&[b"GET", k.as_bytes()]));
    }
    s.write_all(&batch).unwrap();
    let replies = read_replies(&mut s, 64);
    for i in 0..32 {
        assert_eq!(replies[i * 2], b"+OK\r\n", "SET reply {i}");
        let expect = format!("$4\r\npv{i:02}\r\n").into_bytes();
        assert_eq!(replies[i * 2 + 1], expect, "GET reply {i}");
    }

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

#[test]
fn resp_kv_limits_rejected() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // 5KB value 超默认 3000 上限 → -ERR value too long (不进 shard)
    let big = vec![b'x'; 5 * 1024];
    s.write_all(&cmd(&[b"SET", b"bigv", &big])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(
        r[0].starts_with(b"-ERR value too long"),
        "{:?}",
        String::from_utf8_lossy(&r[0])
    );

    // 2KB key 超默认 1024 上限
    let bigk = vec![b'k'; 2 * 1024];
    s.write_all(&cmd(&[b"GET", &bigk])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(
        r[0].starts_with(b"-ERR key too long"),
        "{:?}",
        String::from_utf8_lossy(&r[0])
    );

    // 边界: 恰好 3000 字节 value 应通过
    let ok_val = vec![b'y'; 3000];
    s.write_all(&cmd(&[b"SET", b"okv", &ok_val])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"GET", b"okv"])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(r[0].starts_with(b"$3000\r\n"));

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

#[test]
fn resp_misc_commands() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    s.write_all(&cmd(&[b"ECHO", b"hi"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$2\r\nhi\r\n");

    s.write_all(&cmd(&[b"SELECT", b"0"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");

    s.write_all(&cmd(&[b"COMMAND"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*0\r\n");

    // HELLO 3 → NOPROTO (只支持 RESP2)
    s.write_all(&cmd(&[b"HELLO", b"3"])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(r[0].starts_with(b"-NOPROTO"), "{:?}", String::from_utf8_lossy(&r[0]));

    // 未知命令
    s.write_all(&cmd(&[b"FLUSHALL"])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(r[0].starts_with(b"-ERR unknown command"), "{:?}", String::from_utf8_lossy(&r[0]));

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}
