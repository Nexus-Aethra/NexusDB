//! End-to-end test: 启动 NetworkServer + 真实 ShardManager + 用 std::net::TcpStream 客户端.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use network::{NetworkServer, NetworkServerConfig, Request};
use shard_manager::{ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

fn encode_request(req_id: u64, req: &Request) -> Vec<u8> {
    use network::protocol::{BinaryProtocol, Protocol};
    BinaryProtocol::new().encode_request(req_id, req)
}

fn decode_response(buf: &[u8]) -> Result<(u64, network::Response), network::ProtocolError> {
    use network::protocol::{BinaryProtocol, DecodeOutcome, Protocol};
    // 偷懒: 我们只用 enc/dec 通过 BinaryProtocol trait 接口, 客户端只拿 resp.
    // 这里把整个 decoding 流程都跑一遍, 但只关心 response.
    // 因为 trait 不返回 req_id, 我们重新 parse 头部.
    if buf.len() < 19 {
        return Err(network::ProtocolError::Incomplete);
    }
    let _req_id = u64::from_be_bytes(buf[4..12].try_into().unwrap());
    match BinaryProtocol::new().decode_response(buf)? {
        DecodeOutcome::Complete { consumed: _, value } => {
            // 反推 req_id:
            let _req_id2 = u64::from_be_bytes(buf[4..12].try_into().unwrap());
            Ok((_req_id2, value))
        }
        DecodeOutcome::NeedMore => Err(network::ProtocolError::Incomplete),
    }
}

#[test]
fn put_get_roundtrip() {
    let _ = std::env::var("RUST_LOG");

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

    let cfg = NetworkServerConfig {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        shard_manager: mgr.clone(),
        worker_count: 2,
        default_db: "app".to_string(),
        default_table: "kv".to_string(),
        inbox_capacity: 64,
        protocol: network::ProtocolKind::Binary,
        limits: network::KvLimits::default(),
        auth_password: None,
        worker_id_base: 0,
        sql_shared: network::new_sql_shared(),
        tls_config: None,
    };
    let server = NetworkServer::start(cfg).expect("start server");
    let addr = server.local_addr();

    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_nodelay(true).expect("nodelay");

    // PUT
    let req = encode_request(42, &Request::Put {
        key: b"hello".to_vec(),
        value: b"world".to_vec(),
    });
    stream.write_all(&req).expect("write put");
    stream.flush().expect("flush put");

    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).expect("read put reply");
    let (id, resp) = decode_response(&buf[..n]).expect("decode put");
    assert_eq!(id, 42, "req_id match");
    assert!(matches!(resp, network::Response::PutOk));

    // GET
    let req = encode_request(43, &Request::Get {
        key: b"hello".to_vec(),
    });
    stream.write_all(&req).expect("write get");
    stream.flush().expect("flush get");
    let n = stream.read(&mut buf).expect("read get reply");
    let (id, resp) = decode_response(&buf[..n]).expect("decode get");
    assert_eq!(id, 43);
    match resp {
        network::Response::Get(Some(v)) => assert_eq!(v, b"world"),
        other => panic!("expected Get(Some(world)), got {other:?}"),
    }

    // GET miss
    let req = encode_request(44, &Request::Get {
        key: b"missing".to_vec(),
    });
    stream.write_all(&req).expect("write get miss");
    stream.flush().expect("flush get miss");
    let n = stream.read(&mut buf).expect("read miss reply");
    let (_id, resp) = decode_response(&buf[..n]).expect("decode miss");
    assert!(matches!(resp, network::Response::Get(None)));

    // DELETE
    let req = encode_request(45, &Request::Delete {
        key: b"hello".to_vec(),
    });
    stream.write_all(&req).expect("write del");
    stream.flush().expect("flush del");
    let n = stream.read(&mut buf).expect("read del reply");
    assert!(matches!(
        decode_response(&buf[..n]).expect("decode del").1,
        network::Response::DeleteOk
    ));

    drop(stream);
    server.shutdown().expect("shutdown");
    if let Ok(mgr) = Arc::try_unwrap(mgr) {
        mgr.close().expect("close mgr");
    }
}

#[test]
fn multi_request_single_connection() {
    let _ = std::env::var("RUST_LOG");

    let tmp = tempfile::tempdir().expect("tempdir");
    let opts = ShardManagerOptions {
        num_shards: 2,
        block_root: tmp.path().to_path_buf(),
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 4,
        reply_bus_count: None,
        wal_mode: Default::default(),
    };
    let mgr = Arc::new(ShardManager::open(opts).expect("open mgr"));
    mgr.create_db("d").expect("create db");
    mgr.create_table("d", "t").expect("create table");

    let cfg = NetworkServerConfig {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        shard_manager: mgr.clone(),
        worker_count: 1,
        default_db: "d".to_string(),
        default_table: "t".to_string(),
        inbox_capacity: 64,
        protocol: network::ProtocolKind::Binary,
        limits: network::KvLimits::default(),
        auth_password: None,
        worker_id_base: 0,
        sql_shared: network::new_sql_shared(),
        tls_config: None,
    };
    let server = NetworkServer::start(cfg).expect("start");
    let addr = server.local_addr();

    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

    let mut buf = [0u8; 4096];
    // 3 个 Put 在一条连接上, server 应该按 frame 解析分别回 3 个 reply.
    for i in 0..3u64 {
        let key = format!("k{i}");
        let req = encode_request(
            100 + i,
            &Request::Put {
                key: key.into_bytes(),
                value: vec![i as u8; 8],
            },
        );
        stream.write_all(&req).expect("write");
        let n = stream.read(&mut buf).expect("read");
        let (id, resp) = decode_response(&buf[..n]).expect("decode");
        assert_eq!(id, 100 + i);
        assert!(matches!(resp, network::Response::PutOk));
    }

    server.shutdown().expect("shutdown");
    if let Ok(mgr) = Arc::try_unwrap(mgr) {
        mgr.close().expect("close mgr");
    }
}

#[test]
fn multi_connection_concurrent() {
    let _ = std::env::var("RUST_LOG");

    let tmp = tempfile::tempdir().expect("tempdir");
    let opts = ShardManagerOptions {
        num_shards: 4,
        block_root: tmp.path().to_path_buf(),
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 4,
        reply_bus_count: None,
        wal_mode: Default::default(),
    };
    let mgr = Arc::new(ShardManager::open(opts).expect("open mgr"));
    mgr.create_db("d").expect("create db");
    mgr.create_table("d", "t").expect("create table");

    let cfg = NetworkServerConfig {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        shard_manager: mgr.clone(),
        worker_count: 2,
        default_db: "d".to_string(),
        default_table: "t".to_string(),
        inbox_capacity: 64,
        protocol: network::ProtocolKind::Binary,
        limits: network::KvLimits::default(),
        auth_password: None,
        worker_id_base: 0,
        sql_shared: network::new_sql_shared(),
        tls_config: None,
    };
    let server = NetworkServer::start(cfg).expect("start");
    let addr = server.local_addr();

    // 8 个并发客户端, 每个 put 10 个 key, 完后 read 自校验.
    use std::thread;
    let mut joins = Vec::new();
    for tid in 0..8 {
        joins.push(thread::spawn(move || {
            let mut stream = TcpStream::connect(addr).expect("connect");
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut buf = [0u8; 4096];
            for i in 0..10 {
                let key = format!("t{tid}_k{i}");
                let value = vec![(tid * 100 + i) as u8; 8];
                let req = encode_request(
                    (tid * 1000 + i) as u64,
                    &Request::Put {
                        key: key.clone().into_bytes(),
                        value: value.clone(),
                    },
                );
                stream.write_all(&req).expect("write put");
                let n = stream.read(&mut buf).expect("read put reply");
                let (_id, resp) = decode_response(&buf[..n]).expect("decode");
                assert!(matches!(resp, network::Response::PutOk), "tid={tid} i={i}");

                // GET 自校验
                let req = encode_request(
                    (tid * 1000 + 100 + i) as u64,
                    &Request::Get {
                        key: key.clone().into_bytes(),
                    },
                );
                stream.write_all(&req).expect("write get");
                let n = stream.read(&mut buf).expect("read get reply");
                let (_id, resp) = decode_response(&buf[..n]).expect("decode get");
                match resp {
                    network::Response::Get(Some(v)) => assert_eq!(v, value, "tid={tid} i={i}"),
                    other => panic!("expected Get(Some(value)), got {other:?} tid={tid} i={i}"),
                }
            }
        }));
    }
    for j in joins {
        j.join().expect("client thread panicked");
    }

    server.shutdown().expect("shutdown");
    if let Ok(mgr) = Arc::try_unwrap(mgr) {
        mgr.close().expect("close mgr");
    }
}
