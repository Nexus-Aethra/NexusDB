//! 验证 ShardManager 在 enable_reply_bus 后, 完成 Put/Get/Delete 会
//! 同时 push 一份到 reply_bus. 网络层可以消费这些 reply 异步路由.
//!
//! Phase 1.4 / Task 1.3 集成测试.

use std::sync::Arc;

use shard_manager::{ReplySink, ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

use network::protocol::Response;
use network::reply_bus::{reply_bus, ReplyEnvelope};

/// 测试用 ReplySink: 收集所有 push_reply 调用.
#[derive(Default)]
struct CollectingSink {
    received: std::sync::Mutex<Vec<(u64, u32, shard_manager::ShardResponse)>>,
}

impl ReplySink for CollectingSink {
    fn push_reply(&self, req_id: u64, shard_id: u32, resp: shard_manager::ShardResponse) {
        self.received.lock().unwrap().push((req_id, shard_id, resp));
    }
}

fn make_mgr(num_shards: usize) -> (tempfile::TempDir, ShardManager) {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions {
        num_shards,
        block_root: tmp.path().to_path_buf(),
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 4,
        reply_bus_count: None,
    };
    let mgr = ShardManager::open(opts).expect("open");
    (tmp, mgr)
}

#[test]
fn reply_bus_receives_put() {
    let (_tmp, mgr) = make_mgr(2);
    mgr.create_db("d").unwrap();
    mgr.create_table("d", "t").unwrap();

    let sink = Arc::new(CollectingSink::default());
    mgr.enable_reply_bus(sink.clone());

    mgr.put("d", "t", b"k", b"v", 42).unwrap();
    // 也发一个 req_id = 0 (不应 push)
    mgr.put("d", "t", b"k2", b"v", 0).unwrap();

    let received = sink.received.lock().unwrap();
    assert_eq!(received.len(), 1, "只有 req_id > 0 才 push");
    let (req_id, _shard_id, resp) = &received[0];
    assert_eq!(*req_id, 42);
    assert!(matches!(resp, Ok(shard_manager::ShardReply::PutOk)));
}

#[test]
fn reply_bus_receives_get_and_delete() {
    let (_tmp, mgr) = make_mgr(2);
    mgr.create_db("d").unwrap();
    mgr.create_table("d", "t").unwrap();

    let sink = Arc::new(CollectingSink::default());
    mgr.enable_reply_bus(sink.clone());

    mgr.put("d", "t", b"key1", b"hello", 0).unwrap();
    mgr.get("d", "t", b"key1", 100).unwrap();
    mgr.delete("d", "t", b"key1", 200).unwrap();

    let received = sink.received.lock().unwrap();
    assert_eq!(received.len(), 2);
    assert_eq!(received[0].0, 100);
    assert_eq!(received[1].0, 200);
    assert!(matches!(received[0].2, Ok(shard_manager::ShardReply::GetValue(Some(_)))));
    assert!(matches!(received[1].2, Ok(shard_manager::ShardReply::DeleteExisted(true))));
}

#[test]
fn reply_bus_default_not_active() {
    let (_tmp, mgr) = make_mgr(2);
    mgr.create_db("d").unwrap();
    mgr.create_table("d", "t").unwrap();

    // 不 enable_reply_bus, 即使 req_id > 0 也不应 push
    let sink = Arc::new(CollectingSink::default());
    // 注意: 不调 enable_reply_bus
    let _ = &sink; // 仅供 ref

    mgr.put("d", "t", b"k", b"v", 999).unwrap();
    // 检查全局 sink 应为 None
    assert!(mgr._peek_reply_sink().is_none());
}

#[test]
fn reply_bus_concurrent_many_puts() {
    // 100 个并发 put (sync), 所有 req_id > 0 都应被 sink 收到.
    let (_tmp, mgr) = make_mgr(4);
    mgr.create_db("d").unwrap();
    mgr.create_table("d", "t").unwrap();

    let sink = Arc::new(CollectingSink::default());
    mgr.enable_reply_bus(sink.clone());

    let total = 100u64;
    let mgr = Arc::new(mgr);
    let mut handles = vec![];
    for tid in 1..=4u64 {
        let mgr = mgr.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..(total / 4) {
                let key = format!("k_{}_{}", tid, i);
                let req_id = tid * 1000 + i;
                mgr.put("d", "t", key.as_bytes(), b"v", req_id).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let received = sink.received.lock().unwrap();
    assert_eq!(received.len() as u64, total);
    // 所有 req_id 都收到且唯一
    let mut ids: Vec<u64> = received.iter().map(|(r, _, _)| *r).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len() as u64, total);
}

#[test]
fn reply_bus_sender_integration() {
    // 真正的 network::ReplyBusSender 作为 ReplySink, 验证 crossbeam
    // 端到端: ShardManager → bus → consumer.
    let (_tmp, mgr) = make_mgr(2);
    mgr.create_db("d").unwrap();
    mgr.create_table("d", "t").unwrap();

    let (tx, rx) = reply_bus();
    mgr.enable_reply_bus(Arc::new(tx));

    mgr.put("d", "t", b"k1", b"v1", 11).unwrap();
    mgr.get("d", "t", b"k1", 22).unwrap();
    mgr.delete("d", "t", b"k1", 33).unwrap();

    // 用 drain 拿到全部
    let envs: Vec<ReplyEnvelope> = rx.drain();
    assert_eq!(envs.len(), 3);
    assert_eq!(envs[0].req_id, 11);
    assert!(matches!(envs[0].response, Response::PutOk));
    assert_eq!(envs[1].req_id, 22);
    assert!(matches!(envs[1].response, Response::Get(Some(_))));
    assert_eq!(envs[2].req_id, 33);
    assert!(matches!(envs[2].response, Response::DeleteOk));
}