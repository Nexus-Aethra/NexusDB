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
        wal_mode: Default::default(),
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
        sql_shared: network::new_sql_shared(),
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

    // ⭐ 大 value 支持后默认上限 1MB: 1MB+1 value → -ERR value too long (不进 shard)
    let big = vec![b'x'; 1024 * 1024 + 1];
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

    // ⭐ D (分库): SELECT 不再忽略 — e2e 环境 "app" 是 resolver id 1
    // ("default" 占 0 但未创建 → out of range)
    s.write_all(&cmd(&[b"SELECT", b"1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"SELECT", b"0"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-ERR DB index is out of range"));

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

/// ⭐ MSET/MGET: 跨 shard 分组聚合 (3 shard 下 8 key 必跨), 顺序与 nil 语义.
#[test]
fn resp_mset_mget_cross_shard() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // MSET 8 对 (3 shard 下必然跨 shard 分组)
    let mut args: Vec<Vec<u8>> = vec![b"MSET".to_vec()];
    for i in 0..8u32 {
        args.push(format!("mk{i}").into_bytes());
        args.push(format!("mv{i}").into_bytes());
    }
    let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
    s.write_all(&cmd(&refs)).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");

    // MGET: 乱序 + 混入 miss key, 断言按请求顺序返回
    s.write_all(&cmd(&[b"MGET", b"mk7", b"nope", b"mk0", b"mk3"])).unwrap();
    let r = read_replies(&mut s, 1);
    assert_eq!(
        r[0],
        b"*4\r\n$3\r\nmv7\r\n$-1\r\n$3\r\nmv0\r\n$3\r\nmv3\r\n".to_vec(),
        "got {:?}",
        String::from_utf8_lossy(&r[0])
    );

    // MSET 同 key 重复: 后者覆盖 (Redis 语义)
    s.write_all(&cmd(&[b"MSET", b"dup", b"first", b"dup", b"second"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"GET", b"dup"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$6\r\nsecond\r\n");

    // MSET 奇数参数 → WrongArity
    s.write_all(&cmd(&[b"MSET", b"k", b"v", b"orphan"])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(r[0].starts_with(b"-ERR wrong number"), "{:?}", String::from_utf8_lossy(&r[0]));

    // 与 pipeline 的单 op 混排 (seq 重排缓冲正确性)
    let mut buf = Vec::new();
    buf.extend_from_slice(&cmd(&[b"SET", b"pipek", b"pv"]));
    buf.extend_from_slice(&cmd(&[b"MGET", b"mk1", b"pipek"]));
    buf.extend_from_slice(&cmd(&[b"GET", b"mk2"]));
    s.write_all(&buf).unwrap();
    let r = read_replies(&mut s, 3);
    assert_eq!(r[0], b"+OK\r\n");
    assert_eq!(r[1], b"*2\r\n$3\r\nmv1\r\n$2\r\npv\r\n".to_vec());
    assert_eq!(r[2], b"$3\r\nmv2\r\n");

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ String 命令补全: INCR/DECR/INCRBY/APPEND/SETNX/EXISTS/STRLEN/TYPE.
#[test]
fn resp_string_commands() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // INCR: miss → 1, 再 INCR → 2, DECR → 1, INCRBY 10 → 11, DECRBY 5 → 6
    s.write_all(&cmd(&[b"INCR", b"cnt"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"INCR", b"cnt"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");
    s.write_all(&cmd(&[b"DECR", b"cnt"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"INCRBY", b"cnt", b"10"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":11\r\n");
    s.write_all(&cmd(&[b"DECRBY", b"cnt", b"5"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":6\r\n");
    // GET 读回字符串形式
    s.write_all(&cmd(&[b"GET", b"cnt"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\n6\r\n");
    // 非数字 value → error
    s.write_all(&cmd(&[b"SET", b"strk", b"abc"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"INCR", b"strk"])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(r[0].starts_with(b"-ERR value is not an integer"), "{:?}", String::from_utf8_lossy(&r[0]));
    // INCRBY 非法数字参数 → error
    s.write_all(&cmd(&[b"INCRBY", b"cnt", b"xyz"])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(r[0].starts_with(b"-ERR value is not an integer"), "{:?}", String::from_utf8_lossy(&r[0]));

    // APPEND: miss → 创建, 返回新长度
    s.write_all(&cmd(&[b"APPEND", b"app", b"Hello"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":5\r\n");
    s.write_all(&cmd(&[b"APPEND", b"app", b" World"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":11\r\n");
    s.write_all(&cmd(&[b"GET", b"app"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$11\r\nHello World\r\n");

    // SETNX: 首次 1, 再次 0 (不覆盖)
    s.write_all(&cmd(&[b"SETNX", b"nx", b"v1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"SETNX", b"nx", b"v2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");
    s.write_all(&cmd(&[b"GET", b"nx"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$2\r\nv1\r\n");

    // EXISTS: 多 key + 重复 key 重复计 (Redis 语义)
    s.write_all(&cmd(&[b"EXISTS", b"nx", b"missing", b"nx", b"cnt"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");

    // STRLEN: 命中/未命中
    s.write_all(&cmd(&[b"STRLEN", b"app"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":11\r\n");
    s.write_all(&cmd(&[b"STRLEN", b"missing"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");

    // TYPE: string / none
    s.write_all(&cmd(&[b"TYPE", b"app"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+string\r\n");
    s.write_all(&cmd(&[b"TYPE", b"missing"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+none\r\n");

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ 数值原生存储 + 门面渲染: INCR 后底层是 8B 二进制, RESP GET 渲染字符串.
#[test]
fn resp_typed_num_render() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // INCR → 底层 TAG_I64 二进制; GET 渲染 "1"
    s.write_all(&cmd(&[b"INCR", b"tn"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"GET", b"tn"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\n1\r\n");
    // STRLEN = 渲染后长度 (1), 不是底层 8B
    s.write_all(&cmd(&[b"STRLEN", b"tn"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");

    // INCRBYFLOAT: Redis 文档用例 SET 3.0e3 → INCRBYFLOAT 200 → "3200"
    s.write_all(&cmd(&[b"SET", b"tf", b"3.0e3"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"INCRBYFLOAT", b"tf", b"200"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$4\r\n3200\r\n");
    s.write_all(&cmd(&[b"GET", b"tf"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$4\r\n3200\r\n");
    // 小数渲染
    s.write_all(&cmd(&[b"INCRBYFLOAT", b"tf", b"0.25"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$7\r\n3200.25\r\n");

    // F64 上 INCR → not an integer
    s.write_all(&cmd(&[b"INCR", b"tf"])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(r[0].starts_with(b"-ERR value is not an integer"), "{:?}", String::from_utf8_lossy(&r[0]));

    // INCRBYFLOAT 非法参数
    s.write_all(&cmd(&[b"INCRBYFLOAT", b"tf", b"abc"])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(r[0].starts_with(b"-ERR value is not a valid float"), "{:?}", String::from_utf8_lossy(&r[0]));

    // MGET 混合渲染: RAW + I64 + F64
    s.write_all(&cmd(&[b"SET", b"traw", b"hello"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"MGET", b"traw", b"tn", b"tf"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*3\r\n$5\r\nhello\r\n$1\r\n1\r\n$7\r\n3200.25\r\n".to_vec()
    );

    // APPEND 到 I64: 渲染字符串化再拼, 类型退回 RAW
    s.write_all(&cmd(&[b"APPEND", b"tn", b"x"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n"); // "1x"
    s.write_all(&cmd(&[b"GET", b"tn"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$2\r\n1x\r\n");

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ Phase S: String 范围/杂项命令 (GETRANGE/SETRANGE/GETDEL/GETSET/MSETNX).
#[test]
fn resp_string_range_and_misc() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // GETSET: 旧值 nil (新 key), 返回 nil 并写入
    s.write_all(&cmd(&[b"GETSET", b"gs", b"v1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");
    s.write_all(&cmd(&[b"GETSET", b"gs", b"v2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$2\r\nv1\r\n");
    s.write_all(&cmd(&[b"GET", b"gs"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$2\r\nv2\r\n");

    // GETDEL: 返回旧值并删除
    s.write_all(&cmd(&[b"GETDEL", b"gs"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$2\r\nv2\r\n");
    s.write_all(&cmd(&[b"GET", b"gs"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");
    // GETDEL miss → nil
    s.write_all(&cmd(&[b"GETDEL", b"nope"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");

    // GETRANGE: Redis 文档用例 "This is a string"
    s.write_all(&cmd(&[b"SET", b"gr", b"This is a string"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"GETRANGE", b"gr", b"0", b"3"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$4\r\nThis\r\n");
    s.write_all(&cmd(&[b"GETRANGE", b"gr", b"-3", b"-1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$3\r\ning\r\n");
    s.write_all(&cmd(&[b"GETRANGE", b"gr", b"0", b"-1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$16\r\nThis is a string\r\n");
    // start > end → 空
    s.write_all(&cmd(&[b"GETRANGE", b"gr", b"10", b"2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$0\r\n\r\n");

    // SETRANGE: 覆盖写 + 返回新长度; Redis 文档用例
    s.write_all(&cmd(&[b"SET", b"sr", b"Hello World"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"SETRANGE", b"sr", b"6", b"Redis"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":11\r\n");
    s.write_all(&cmd(&[b"GET", b"sr"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$11\r\nHello Redis\r\n");
    // SETRANGE 零扩展 (新 key, offset 5)
    s.write_all(&cmd(&[b"SETRANGE", b"sr2", b"5", b"abc"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":8\r\n");
    s.write_all(&cmd(&[b"GET", b"sr2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$8\r\n\x00\x00\x00\x00\x00abc\r\n");

    // MSETNX: 全不存在 → :1; 任一存在 → :0 (且不覆盖已存在的)
    s.write_all(&cmd(&[b"MSETNX", b"nx1", b"a", b"nx2", b"b"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"MSETNX", b"nx2", b"x", b"nx3", b"c"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");
    s.write_all(&cmd(&[b"GET", b"nx1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\na\r\n");

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ Phase H: Hash 全命令面 (HSET/HGET/HMGET/HDEL/HGETALL/HINCRBY/WRONGTYPE...).
#[test]
fn resp_hash_commands() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // HSET 多 field → 新增数; 重复 HSET 更新 → :0
    s.write_all(&cmd(&[b"HSET", b"h1", b"f1", b"v1", b"f2", b"v2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");
    s.write_all(&cmd(&[b"HSET", b"h1", b"f1", b"v1x"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");

    // HGET / miss / HEXISTS / HLEN
    s.write_all(&cmd(&[b"HGET", b"h1", b"f1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$3\r\nv1x\r\n");
    s.write_all(&cmd(&[b"HGET", b"h1", b"nope"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");
    s.write_all(&cmd(&[b"HEXISTS", b"h1", b"f2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"HEXISTS", b"h1", b"nope"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");
    s.write_all(&cmd(&[b"HLEN", b"h1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");

    // HMGET 按输入序 (含 miss)
    s.write_all(&cmd(&[b"HMGET", b"h1", b"f2", b"nope", b"f1"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*3\r\n$2\r\nv2\r\n$-1\r\n$3\r\nv1x\r\n"
    );

    // HGETALL (field 按 BTree 字典序) / HKEYS / HVALS
    s.write_all(&cmd(&[b"HGETALL", b"h1"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*4\r\n$2\r\nf1\r\n$3\r\nv1x\r\n$2\r\nf2\r\n$2\r\nv2\r\n"
    );
    s.write_all(&cmd(&[b"HKEYS", b"h1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*2\r\n$2\r\nf1\r\n$2\r\nf2\r\n");
    s.write_all(&cmd(&[b"HVALS", b"h1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*2\r\n$3\r\nv1x\r\n$2\r\nv2\r\n");

    // HSCAN v1: 单次全量, cursor "0"
    s.write_all(&cmd(&[b"HSCAN", b"h1", b"0"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*2\r\n$1\r\n0\r\n*4\r\n$2\r\nf1\r\n$3\r\nv1x\r\n$2\r\nf2\r\n$2\r\nv2\r\n"
    );

    // HSETNX: 已存在 :0, 新 field :1
    s.write_all(&cmd(&[b"HSETNX", b"h1", b"f1", b"zzz"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");
    s.write_all(&cmd(&[b"HSETNX", b"h1", b"f3", b"v3"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");

    // HINCRBY / HINCRBYFLOAT (原生二进制数值, 渲染回字符串)
    s.write_all(&cmd(&[b"HINCRBY", b"h1", b"cnt", b"5"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":5\r\n");
    s.write_all(&cmd(&[b"HINCRBY", b"h1", b"cnt", b"-2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");
    s.write_all(&cmd(&[b"HGET", b"h1", b"cnt"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\n3\r\n");
    s.write_all(&cmd(&[b"HINCRBYFLOAT", b"h1", b"fl", b"1.5"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$3\r\n1.5\r\n");

    // HMSET → +OK
    s.write_all(&cmd(&[b"HMSET", b"h2", b"a", b"1", b"b", b"2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");

    // HDEL 多 field (含 miss) → 实删数; count 归 0 后 HLEN=0
    s.write_all(&cmd(&[b"HDEL", b"h2", b"a", b"nope", b"b"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");
    s.write_all(&cmd(&[b"HLEN", b"h2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");

    // WRONGTYPE 双向: String key 上 HSET; Hash key 上 GET
    s.write_all(&cmd(&[b"SET", b"str1", b"v"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"HSET", b"str1", b"f", b"v"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-WRONGTYPE"));
    s.write_all(&cmd(&[b"GET", b"h1"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-WRONGTYPE"));

    // DEL 整 hash → :1; 之后 HGETALL 空、HLEN 0
    s.write_all(&cmd(&[b"DEL", b"h1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"HGETALL", b"h1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*0\r\n");
    s.write_all(&cmd(&[b"HLEN", b"h1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ Phase Set: Set 命令面 (SADD/SREM/SISMEMBER/SMEMBERS/SCARD/SPOP + 代数).
#[test]
fn resp_set_commands() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // SADD 去重: 新增 2, 重复 0
    s.write_all(&cmd(&[b"SADD", b"s1", b"a", b"b"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");
    s.write_all(&cmd(&[b"SADD", b"s1", b"a", b"c"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");

    // SISMEMBER / SCARD / SMEMBERS (BTree 序)
    s.write_all(&cmd(&[b"SISMEMBER", b"s1", b"b"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"SISMEMBER", b"s1", b"zz"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");
    s.write_all(&cmd(&[b"SCARD", b"s1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");
    s.write_all(&cmd(&[b"SMEMBERS", b"s1"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
    );

    // SSCAN v1
    s.write_all(&cmd(&[b"SSCAN", b"s1", b"0"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*2\r\n$1\r\n0\r\n*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
    );

    // SREM (含 miss)
    s.write_all(&cmd(&[b"SREM", b"s1", b"a", b"zz"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"SCARD", b"s1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");

    // SPOP: 弹出后 card-1; SRANDMEMBER 不删
    s.write_all(&cmd(&[b"SRANDMEMBER", b"s1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\nb\r\n"); // BTree 序首个
    s.write_all(&cmd(&[b"SPOP", b"s1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\nb\r\n");
    s.write_all(&cmd(&[b"SCARD", b"s1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    // 空集 SPOP → nil
    s.write_all(&cmd(&[b"SPOP", b"empty"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");

    // 代数: SINTER / SUNION / SDIFF (跨 shard 聚合)
    s.write_all(&cmd(&[b"SADD", b"x1", b"a", b"b", b"c"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");
    s.write_all(&cmd(&[b"SADD", b"x2", b"b", b"c", b"d"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");
    s.write_all(&cmd(&[b"SINTER", b"x1", b"x2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n");
    s.write_all(&cmd(&[b"SDIFF", b"x1", b"x2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*1\r\n$1\r\na\r\n");
    s.write_all(&cmd(&[b"SUNION", b"x1", b"x2"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*4\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n"
    );

    // WRONGTYPE: String key 上 SADD; Set key 与 Hash 互斥
    s.write_all(&cmd(&[b"SET", b"strk", b"v"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"SADD", b"strk", b"m"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-WRONGTYPE"));
    s.write_all(&cmd(&[b"HSET", b"s1", b"f", b"v"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-WRONGTYPE"));

    // DEL 整 set → :1, 之后空
    s.write_all(&cmd(&[b"DEL", b"s1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"SCARD", b"s1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ Phase L: List 命令面 (LPUSH/RPUSH/LPOP/RPOP/LRANGE/LINDEX/LSET/LLEN).
#[test]
fn resp_list_commands() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // RPUSH a b c → [a,b,c]; LPUSH x y → [y,x,a,b,c]
    s.write_all(&cmd(&[b"RPUSH", b"l1", b"a", b"b", b"c"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");
    s.write_all(&cmd(&[b"LPUSH", b"l1", b"x", b"y"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":5\r\n"); // [y,x,a,b,c]

    // LLEN / LRANGE 全量 / 负索引
    s.write_all(&cmd(&[b"LLEN", b"l1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":5\r\n");
    s.write_all(&cmd(&[b"LRANGE", b"l1", b"0", b"-1"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*5\r\n$1\r\ny\r\n$1\r\nx\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
    );
    s.write_all(&cmd(&[b"LRANGE", b"l1", b"1", b"3"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*3\r\n$1\r\nx\r\n$1\r\na\r\n$1\r\nb\r\n"
    );

    // LINDEX 正/负/越界
    s.write_all(&cmd(&[b"LINDEX", b"l1", b"0"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\ny\r\n");
    s.write_all(&cmd(&[b"LINDEX", b"l1", b"-1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\nc\r\n");
    s.write_all(&cmd(&[b"LINDEX", b"l1", b"99"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");

    // LSET + 越界报错
    s.write_all(&cmd(&[b"LSET", b"l1", b"0", b"Y"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"LINDEX", b"l1", b"0"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\nY\r\n");
    s.write_all(&cmd(&[b"LSET", b"l1", b"99", b"z"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-ERR"));

    // LPOP / RPOP 单个
    s.write_all(&cmd(&[b"LPOP", b"l1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\nY\r\n");
    s.write_all(&cmd(&[b"RPOP", b"l1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\nc\r\n");
    s.write_all(&cmd(&[b"LLEN", b"l1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n"); // [x,a,b]

    // LPOP count → 数组
    s.write_all(&cmd(&[b"LPOP", b"l1", b"2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*2\r\n$1\r\nx\r\n$1\r\na\r\n");

    // 空 list LPOP → nil
    s.write_all(&cmd(&[b"RPOP", b"l1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\nb\r\n");
    s.write_all(&cmd(&[b"LPOP", b"l1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");
    s.write_all(&cmd(&[b"LLEN", b"l1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");

    // WRONGTYPE: String key 上 LPUSH
    s.write_all(&cmd(&[b"SET", b"lk", b"v"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"LPUSH", b"lk", b"m"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-WRONGTYPE"));

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ Phase Z: ZSet 命令面 (ZADD/ZSCORE/ZRANGE/ZRANGEBYSCORE/ZRANK/ZINCRBY...).
#[test]
fn resp_zset_commands() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // ZADD 3 成员 (乱序 score); 更新已存在成员 score → 新增 0
    s.write_all(&cmd(&[b"ZADD", b"z1", b"3", b"c", b"1", b"a", b"2", b"b"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");
    s.write_all(&cmd(&[b"ZADD", b"z1", b"5", b"a"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");

    // ZCARD / ZSCORE (整数 score 渲染无 .0)
    s.write_all(&cmd(&[b"ZCARD", b"z1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");
    s.write_all(&cmd(&[b"ZSCORE", b"z1", b"b"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\n2\r\n");
    s.write_all(&cmd(&[b"ZSCORE", b"z1", b"nope"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");

    // 现在 score: b=2, c=3, a=5 → 按 score 升序 [b,c,a]
    s.write_all(&cmd(&[b"ZRANGE", b"z1", b"0", b"-1"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\na\r\n"
    );
    // WITHSCORES
    s.write_all(&cmd(&[b"ZRANGE", b"z1", b"0", b"1", b"WITHSCORES"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*4\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n"
    );
    // ZREVRANGE
    s.write_all(&cmd(&[b"ZREVRANGE", b"z1", b"0", b"-1"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*3\r\n$1\r\na\r\n$1\r\nc\r\n$1\r\nb\r\n"
    );

    // ZRANGEBYSCORE
    s.write_all(&cmd(&[b"ZRANGEBYSCORE", b"z1", b"2", b"3"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n");
    s.write_all(&cmd(&[b"ZRANGEBYSCORE", b"z1", b"-inf", b"+inf"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\na\r\n"
    );

    // ZRANK / ZREVRANK (b 排名 0, a 排名 2)
    s.write_all(&cmd(&[b"ZRANK", b"z1", b"b"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");
    s.write_all(&cmd(&[b"ZRANK", b"z1", b"a"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");
    s.write_all(&cmd(&[b"ZREVRANK", b"z1", b"a"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");
    s.write_all(&cmd(&[b"ZRANK", b"z1", b"nope"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");

    // ZINCRBY: b 2 → 4.5
    s.write_all(&cmd(&[b"ZINCRBY", b"z1", b"2.5", b"b"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$3\r\n4.5\r\n");
    s.write_all(&cmd(&[b"ZSCORE", b"z1", b"b"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$3\r\n4.5\r\n");

    // ZREM
    s.write_all(&cmd(&[b"ZREM", b"z1", b"c", b"nope"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"ZCARD", b"z1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");

    // WRONGTYPE: String key 上 ZADD
    s.write_all(&cmd(&[b"SET", b"zstr", b"v"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"ZADD", b"zstr", b"1", b"m"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-WRONGTYPE"));

    // DEL 整 zset (清 member 索引 + score 索引 + meta)
    s.write_all(&cmd(&[b"DEL", b"z1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"ZCARD", b"z1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");
    s.write_all(&cmd(&[b"ZRANGE", b"z1", b"0", b"-1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*0\r\n");

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ C1: 命令空洞补齐 e2e (ZCOUNT/ZMSCORE/ZPOP + SMISMEMBER/SINTERCARD/SPOP count + HSTRLEN/HRANDFIELD).
#[test]
fn resp_c1_command_holes() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // ---- ZSet ----
    s.write_all(&cmd(&[b"ZADD", b"cz", b"1", b"a", b"2", b"b", b"3", b"c"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");
    s.write_all(&cmd(&[b"ZCOUNT", b"cz", b"2", b"+inf"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");
    s.write_all(&cmd(&[b"ZMSCORE", b"cz", b"a", b"nope", b"c"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n3\r\n");
    // ZPOPMIN 弹最小 (member+score)
    s.write_all(&cmd(&[b"ZPOPMIN", b"cz"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*2\r\n$1\r\na\r\n$1\r\n1\r\n");
    // ZPOPMAX 2 个
    s.write_all(&cmd(&[b"ZPOPMAX", b"cz", b"2"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*4\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nb\r\n$1\r\n2\r\n"
    );
    s.write_all(&cmd(&[b"ZCARD", b"cz"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");

    // ---- Set ----
    s.write_all(&cmd(&[b"SADD", b"cs1", b"a", b"b", b"c"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");
    s.write_all(&cmd(&[b"SMISMEMBER", b"cs1", b"a", b"x", b"c"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*3\r\n:1\r\n:0\r\n:1\r\n");
    s.write_all(&cmd(&[b"SADD", b"cs2", b"b", b"c", b"d"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");
    s.write_all(&cmd(&[b"SINTERCARD", b"2", b"cs1", b"cs2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");
    s.write_all(&cmd(&[b"SINTERCARD", b"2", b"cs1", b"cs2", b"LIMIT", b"1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    // SRANDMEMBER count (不删) + SPOP count (删)
    s.write_all(&cmd(&[b"SRANDMEMBER", b"cs1", b"2"])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(r[0].starts_with(b"*2\r\n"), "SRANDMEMBER 2 应回 2 项: {:?}", r[0]);
    s.write_all(&cmd(&[b"SCARD", b"cs1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");
    s.write_all(&cmd(&[b"SPOP", b"cs1", b"2"])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(r[0].starts_with(b"*2\r\n"), "SPOP 2 应回 2 项: {:?}", r[0]);
    s.write_all(&cmd(&[b"SCARD", b"cs1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");

    // ---- Hash ----
    s.write_all(&cmd(&[b"HSET", b"ch", b"f1", b"hello", b"f2", b"world!"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");
    s.write_all(&cmd(&[b"HSTRLEN", b"ch", b"f2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":6\r\n");
    s.write_all(&cmd(&[b"HSTRLEN", b"ch", b"nope"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");
    // HRANDFIELD 无 count → 单 bulk; count 2 → *2; WITHVALUES → *4
    s.write_all(&cmd(&[b"HRANDFIELD", b"ch"])).unwrap();
    let r = read_replies(&mut s, 1);
    assert!(r[0].starts_with(b"$2\r\nf"), "单 field bulk: {:?}", r[0]);
    s.write_all(&cmd(&[b"HRANDFIELD", b"ch", b"2"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"*2\r\n"));
    s.write_all(&cmd(&[b"HRANDFIELD", b"ch", b"2", b"WITHVALUES"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"*4\r\n"));
    // 不存在 key: HRANDFIELD → nil / *0
    s.write_all(&cmd(&[b"HRANDFIELD", b"nokey"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ C2: List 中段操作 e2e (LREM/LTRIM/LPOS/LINSERT).
#[test]
fn resp_c2_list_mid_ops() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // [a, b, a, c, a]
    s.write_all(&cmd(&[b"RPUSH", b"ml", b"a", b"b", b"a", b"c", b"a"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":5\r\n");

    // LPOS
    s.write_all(&cmd(&[b"LPOS", b"ml", b"a"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");
    s.write_all(&cmd(&[b"LPOS", b"ml", b"a", b"COUNT", b"0"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*3\r\n:0\r\n:2\r\n:4\r\n");
    s.write_all(&cmd(&[b"LPOS", b"ml", b"a", b"RANK", b"-1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":4\r\n");
    s.write_all(&cmd(&[b"LPOS", b"ml", b"nope"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");

    // LREM 从尾删 1 个 a → [a, b, a, c]
    s.write_all(&cmd(&[b"LREM", b"ml", b"-1", b"a"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"LRANGE", b"ml", b"0", b"-1"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*4\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\na\r\n$1\r\nc\r\n"
    );

    // LINSERT AFTER b → [a, b, x, a, c]
    s.write_all(&cmd(&[b"LINSERT", b"ml", b"AFTER", b"b", b"x"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":5\r\n");
    s.write_all(&cmd(&[b"LINSERT", b"ml", b"BEFORE", b"nope", b"y"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":-1\r\n");

    // LTRIM 1..=3 → [b, x, a]
    s.write_all(&cmd(&[b"LTRIM", b"ml", b"1", b"3"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"LRANGE", b"ml", b"0", b"-1"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*3\r\n$1\r\nb\r\n$1\r\nx\r\n$1\r\na\r\n"
    );
    // 空洞后 LINDEX/LPOP 正确
    s.write_all(&cmd(&[b"LINDEX", b"ml", b"-1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\na\r\n");
    s.write_all(&cmd(&[b"LPOP", b"ml"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\nb\r\n");
    s.write_all(&cmd(&[b"LLEN", b"ml"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ C3: *STORE 变体 e2e (SINTERSTORE/SUNIONSTORE/SDIFFSTORE/ZINTERSTORE/ZUNIONSTORE).
#[test]
fn resp_c3_store_variants() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    s.write_all(&cmd(&[b"SADD", b"st1", b"a", b"b", b"c"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");
    s.write_all(&cmd(&[b"SADD", b"st2", b"b", b"c", b"d"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");

    // SINTERSTORE → {b, c}
    s.write_all(&cmd(&[b"SINTERSTORE", b"sti", b"st1", b"st2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");
    s.write_all(&cmd(&[b"SMEMBERS", b"sti"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n");

    // SUNIONSTORE 覆盖旧 dst → {a,b,c,d}
    s.write_all(&cmd(&[b"SUNIONSTORE", b"sti", b"st1", b"st2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":4\r\n");
    s.write_all(&cmd(&[b"SCARD", b"sti"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":4\r\n");

    // SDIFFSTORE → {a}
    s.write_all(&cmd(&[b"SDIFFSTORE", b"std", b"st1", b"st2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"SMEMBERS", b"std"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*1\r\n$1\r\na\r\n");

    // 空结果 → dst 被清空
    s.write_all(&cmd(&[b"SDIFFSTORE", b"sti", b"st1", b"st1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");
    s.write_all(&cmd(&[b"EXISTS", b"sti"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");

    // ---- ZSet STORE ----
    s.write_all(&cmd(&[b"ZADD", b"zt1", b"1", b"a", b"2", b"b"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");
    s.write_all(&cmd(&[b"ZADD", b"zt2", b"10", b"b", b"20", b"c"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");

    // ZINTERSTORE → {b: 2+10=12}
    s.write_all(&cmd(&[b"ZINTERSTORE", b"zti", b"2", b"zt1", b"zt2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"ZSCORE", b"zti", b"b"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$2\r\n12\r\n");

    // ZUNIONSTORE → {a:1, b:12, c:20} 按 score 排序
    s.write_all(&cmd(&[b"ZUNIONSTORE", b"ztu", b"2", b"zt1", b"zt2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");
    s.write_all(&cmd(&[b"ZRANGE", b"ztu", b"0", b"-1", b"WITHSCORES"])).unwrap();
    assert_eq!(
        read_replies(&mut s, 1)[0],
        b"*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$2\r\n12\r\n$1\r\nc\r\n$2\r\n20\r\n"
    );

    // WEIGHTS 拒绝 (本轮不支持)
    s.write_all(&cmd(&[b"ZUNIONSTORE", b"ztw", b"2", b"zt1", b"zt2", b"WEIGHTS", b"2", b"3"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-ERR"));

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ Phase G: Geo e2e (GEOADD/GEOPOS/GEODIST/GEOSEARCH, 复用 ZSet).
#[test]
fn resp_g_geo_commands() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // 北京 / 上海 / 广州
    s.write_all(&cmd(&[
        b"GEOADD", b"geo", b"116.397128", b"39.916527", b"beijing",
        b"121.4737", b"31.2304", b"shanghai", b"113.2644", b"23.1291", b"guangzhou",
    ]))
    .unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");

    // TYPE = zset (Geo 即 ZSet)
    s.write_all(&cmd(&[b"ZCARD", b"geo"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":3\r\n");

    // GEOPOS: 坐标 roundtrip 误差 < 0.001 度
    s.write_all(&cmd(&[b"GEOPOS", b"geo", b"beijing", b"nowhere"])).unwrap();
    let r = String::from_utf8(read_replies(&mut s, 1)[0].clone()).unwrap();
    assert!(r.starts_with("*2\r\n*2\r\n"), "首项坐标对: {r}");
    assert!(r.contains("116.39"), "lon 近似: {r}");
    assert!(r.contains("39.91"), "lat 近似: {r}");
    assert!(r.ends_with("*-1\r\n"), "缺失成员 → nil array: {r}");

    // GEODIST 北京↔上海 ≈ 1067km (±1%)
    s.write_all(&cmd(&[b"GEODIST", b"geo", b"beijing", b"shanghai", b"km"])).unwrap();
    let r = String::from_utf8(read_replies(&mut s, 1)[0].clone()).unwrap();
    let d: f64 = r.split("\r\n").nth(1).unwrap().parse().unwrap();
    assert!((d - 1067.0).abs() < 15.0, "北京-上海 {d}km");
    s.write_all(&cmd(&[b"GEODIST", b"geo", b"beijing", b"nowhere"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");

    // GEOSEARCH 上海为心 400km → 只有 shanghai
    s.write_all(&cmd(&[
        b"GEOSEARCH", b"geo", b"FROMLONLAT", b"121.5", b"31.2", b"BYRADIUS", b"400", b"km",
    ]))
    .unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*1\r\n$8\r\nshanghai\r\n");

    // 1500km → 三城全中, ASC 距离序: shanghai, beijing, guangzhou; COUNT 2 截断
    s.write_all(&cmd(&[
        b"GEOSEARCH", b"geo", b"FROMLONLAT", b"121.5", b"31.2", b"BYRADIUS", b"1500", b"km",
        b"ASC", b"COUNT", b"2", b"WITHDIST",
    ]))
    .unwrap();
    let r = String::from_utf8(read_replies(&mut s, 1)[0].clone()).unwrap();
    assert!(r.starts_with("*2\r\n"), "COUNT 2: {r}");
    assert!(r.contains("shanghai"), "{r}");
    assert!(r.contains("beijing"), "{r}");
    assert!(!r.contains("guangzhou"), "COUNT 截断: {r}");

    // WRONGTYPE: string key 上 GEOPOS
    s.write_all(&cmd(&[b"SET", b"gstr", b"v"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"GEOPOS", b"gstr", b"m"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-WRONGTYPE"));

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ Phase B: Bitmap e2e (SETBIT/GETBIT/BITCOUNT/BITPOS).
#[test]
fn resp_b_bitmap_commands() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // SETBIT 置位 7 → "\x01"; 旧 bit 0
    s.write_all(&cmd(&[b"SETBIT", b"bm", b"7", b"1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");
    s.write_all(&cmd(&[b"GETBIT", b"bm", b"7"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"GETBIT", b"bm", b"6"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");
    // 重置位 7 → 旧 bit 1
    s.write_all(&cmd(&[b"SETBIT", b"bm", b"7", b"0"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");

    // 零扩展: 置位 100 → 长度 13 字节
    s.write_all(&cmd(&[b"SETBIT", b"bm", b"100", b"1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");
    s.write_all(&cmd(&[b"STRLEN", b"bm"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":13\r\n");

    // BITCOUNT: "foobar" → 26; 区间 [1,1] → 6
    s.write_all(&cmd(&[b"SET", b"bstr", b"foobar"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"BITCOUNT", b"bstr"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":26\r\n");
    s.write_all(&cmd(&[b"BITCOUNT", b"bstr", b"1", b"1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":6\r\n");
    s.write_all(&cmd(&[b"BITCOUNT", b"nobm"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");

    // BITPOS: "foobar" 首个 1 在 bit 1 ('f' = 0x66)
    s.write_all(&cmd(&[b"BITPOS", b"bstr", b"1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    // "\xff" 找 0 无 end → 越界位 8
    s.write_all(&cmd(&[b"SET", b"ff", b"\xff"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"BITPOS", b"ff", b"0"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":8\r\n");
    // 不存在 key: 找 1 → -1; 找 0 → 0
    s.write_all(&cmd(&[b"BITPOS", b"nobm", b"1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":-1\r\n");
    s.write_all(&cmd(&[b"BITPOS", b"nobm", b"0"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":0\r\n");

    // 越界 offset 拒绝 (max_value_bytes 默认 1MB)
    s.write_all(&cmd(&[b"SETBIT", b"bm", b"99999999999", b"1"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-ERR"));

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ T2 (分表): key 冒号前缀选表 e2e — "table:key" 路由 + shard 惰性建表.
#[test]
fn resp_t_table_routing() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // 三个表 (user / order / default) 同名 stripped key 互不干扰
    s.write_all(&cmd(&[b"SET", b"user:1", b"a"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"SET", b"order:1", b"b"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"SET", b"1", b"c"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"GET", b"user:1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\na\r\n");
    s.write_all(&cmd(&[b"GET", b"order:1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\nb\r\n");
    s.write_all(&cmd(&[b"GET", b"1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\nc\r\n");

    // DEL 只删所在表
    s.write_all(&cmd(&[b"DEL", b"user:1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"GET", b"order:1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\nb\r\n");

    // 只拆第一个冒号: user:1000:profile → 表 user, key "1000:profile"
    s.write_all(&cmd(&[b"SET", b"user:1000:profile", b"p"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"GET", b"user:1000:profile"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$1\r\np\r\n");

    // 复合结构带前缀
    s.write_all(&cmd(&[b"HSET", b"user:1000", b"f", b"v"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"HGETALL", b"user:1000"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*2\r\n$1\r\nf\r\n$1\r\nv\r\n");
    s.write_all(&cmd(&[b"LPUSH", b"q:jobs", b"j1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"LLEN", b"q:jobs"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");

    // 跨表 MGET (混合前缀 + 无前缀, 按输入序回填)
    s.write_all(&cmd(&[b"MGET", b"order:1", b"1", b"user:1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*3\r\n$1\r\nb\r\n$1\r\nc\r\n$-1\r\n");
    // 跨表 MSET
    s.write_all(&cmd(&[b"MSET", b"m1:k", b"x", b"m2:k", b"y", b"k", b"z"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"MGET", b"m1:k", b"m2:k", b"k"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*3\r\n$1\r\nx\r\n$1\r\ny\r\n$1\r\nz\r\n");

    // *STORE 带前缀 dst + 跨表源
    s.write_all(&cmd(&[b"SADD", b"sa:s", b"a", b"b"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");
    s.write_all(&cmd(&[b"SADD", b"sb:s", b"b", b"c"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":2\r\n");
    s.write_all(&cmd(&[b"SINTERSTORE", b"out:r", b"sa:s", b"sb:s"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b":1\r\n");
    s.write_all(&cmd(&[b"SMEMBERS", b"out:r"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"*1\r\n$1\r\nb\r\n");

    // 边界: 非法前缀 → 整 key 落 default 表 (与写入自洽)
    s.write_all(&cmd(&[b"SET", b":x", b"e1"])).unwrap(); // 空前缀
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"GET", b":x"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$2\r\ne1\r\n");
    s.write_all(&cmd(&[b"GET", b"x"])).unwrap(); // 不是 "x" (未剥前缀)
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");
    s.write_all(&cmd(&[b"SET", b"\xff\x01:bin", b"e2"])).unwrap(); // 二进制前缀
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"GET", b"\xff\x01:bin"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$2\r\ne2\r\n");
    let long = [b"t".repeat(65), b":k".to_vec()].concat(); // 65B 前缀超长
    s.write_all(&cmd(&[b"SET", &long, b"e3"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"GET", &long])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$2\r\ne3\r\n");

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}

/// ⭐ D (分库): SELECT n 经 DbNameResolver id 翻译切库 e2e.
#[test]
fn resp_d_select_db() {
    let (server, mgr) = start_server(None);
    // harness 已建 "app" (resolver id 1; "default" 占 0 但未创建被视图过滤).
    // 再建 "app2" → id 2 (create_db 后 DbDirView 自动刷新); 表靠惰性建表.
    mgr.create_db("app2").expect("create app2");
    let mut s = connect(&server);

    // 默认连接在 "app": 写 k
    s.write_all(&cmd(&[b"SET", b"k", b"in-app"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");

    // SELECT 2 → app2: k 不可见 (库隔离); 写入走惰性建表
    s.write_all(&cmd(&[b"SELECT", b"2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"GET", b"k"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");
    s.write_all(&cmd(&[b"SET", b"k", b"in-app2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    // SELECT 与表前缀正交
    s.write_all(&cmd(&[b"SET", b"user:k", b"u2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");

    // 切回 app (id 1): 各自数据独立
    s.write_all(&cmd(&[b"SELECT", b"1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"GET", b"k"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$6\r\nin-app\r\n");
    s.write_all(&cmd(&[b"GET", b"user:k"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$-1\r\n");
    s.write_all(&cmd(&[b"SELECT", b"2"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");
    s.write_all(&cmd(&[b"GET", b"k"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$7\r\nin-app2\r\n");
    s.write_all(&cmd(&[b"GET", b"user:k"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"$2\r\nu2\r\n");

    // 越界 / 非整数
    s.write_all(&cmd(&[b"SELECT", b"99"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-ERR DB index is out of range"));
    s.write_all(&cmd(&[b"SELECT", b"abc"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-ERR"));
    // "default" 未真实创建 → id 0 不可选
    s.write_all(&cmd(&[b"SELECT", b"0"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-ERR DB index is out of range"));

    // 断连重置: 新连接回默认 db (app)
    drop(s);
    let mut s2 = connect(&server);
    s2.write_all(&cmd(&[b"GET", b"k"])).unwrap();
    assert_eq!(read_replies(&mut s2, 1)[0], b"$6\r\nin-app\r\n");

    drop(s2);
    server.shutdown().unwrap();
    drop(mgr);
}

// ===== ⭐ Y2: SQL 已迁独立端口 — RESP 回归纯 Redis 语义断言 =====

/// SQL 剥离后 RESP 行为回归: CREATE/INSERT 是未知命令, SELECT 严格选库.
#[test]
fn resp_sql_removed_pure_redis_semantics() {
    let (server, mgr) = start_server(None);
    let mut s = connect(&server);

    // CREATE / INSERT → unknown command
    s.write_all(&cmd(&[b"CREATE", b"TABLE", b"t", b"(a", b"INT", b"PRIMARY", b"KEY)"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-ERR unknown command 'create'"));
    s.write_all(&cmd(&[b"INSERT", b"INTO", b"t", b"VALUES", b"(1)"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-ERR unknown command 'insert'"));

    // SELECT 恢复严格 arity=2 整数
    s.write_all(&cmd(&[b"SELECT", b"*", b"FROM", b"t"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-ERR wrong number of arguments"));
    s.write_all(&cmd(&[b"SELECT", b"abc"])).unwrap();
    assert!(read_replies(&mut s, 1)[0].starts_with(b"-ERR"));
    // 选库语义不变 (e2e 环境 "app" 是 resolver id 1)
    s.write_all(&cmd(&[b"SELECT", b"1"])).unwrap();
    assert_eq!(read_replies(&mut s, 1)[0], b"+OK\r\n");

    drop(s);
    server.shutdown().unwrap();
    drop(mgr);
}
