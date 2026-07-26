//! ReplyBus mpmc 测试: 4 producer + 4 consumer, 1000 条 message.

use std::sync::Arc;
use std::thread;

use network::protocol::Response;
use network::reply_bus::{reply_bus, ReplyEnvelope};

#[test]
fn mpmc_roundtrip_1000_msgs() {
    let (tx, rx) = reply_bus();
    let total_msgs = 1000u64;
    let producers = 4;
    let consumers = 4;

    let received = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let received_clone = received.clone();

    // 4 个 producer
    let mut producer_handles = vec![];
    for p in 0..producers {
        let tx = tx.clone();
        producer_handles.push(thread::spawn(move || {
            for i in 0..total_msgs / producers {
                let env = ReplyEnvelope {
                    req_id: p * 1000 + i,
                    shard_id: p as u32,
                    response: Response::PutOk,
                };
                tx.push(env);
            }
        }));
    }
    drop(tx); // 关掉 producer side

    // 4 个 consumer
    let mut consumer_handles = vec![];
    for _ in 0..consumers {
        let rx = rx.clone();
        let received = received_clone.clone();
        consumer_handles.push(thread::spawn(move || {
            while let Some(_env) = rx.pop() {
                received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }));
    }

    for h in producer_handles {
        h.join().unwrap();
    }
    // 最后一个 producer drop 后所有 Receiver 都拿到 None, consumer 自然退出
    drop(rx);
    for h in consumer_handles {
        h.join().unwrap();
    }

    assert_eq!(received.load(std::sync::atomic::Ordering::Relaxed), total_msgs);
}

#[test]
fn try_pop_returns_none_when_empty() {
    let (tx, rx) = reply_bus();
    assert!(rx.try_pop().is_none());
    tx.push(ReplyEnvelope {
        req_id: 1,
        shard_id: 0,
        response: Response::PutOk,
    });
    assert!(rx.try_pop().is_some());
    assert!(rx.try_pop().is_none());
}