//! ReplyBus: ShardManager → Worker 的 reply 通道.
//!
//! Phase 1 stub: 简单包装 crossbeam unbounded channel.

use crossbeam_channel::{unbounded, Receiver, Sender};

use crate::protocol::Response;
use shard_manager::{ReplySink, ShardResponse};

#[derive(Debug, Clone)]
pub struct ReplyEnvelope {
    pub req_id: u64,
    pub shard_id: u32,
    pub response: Response,
}

/// 把 ShardManager 的内部 `ShardResponse` 转成 network 层 `Response`.
/// 只关心 Put/Get/Delete 这三种 op; 其他 reply (CreateTable/2PC/Shutdown)
/// 转成 Error 字符串, worker 收到会走错误路径 (实际不会发生, 因为 worker
/// 只发 KV op).
fn shard_resp_to_response(resp: ShardResponse) -> Response {
    use shard_manager::ShardReply;
    match resp {
        Ok(ShardReply::PutOk) => Response::PutOk,
        Ok(ShardReply::GetValue(v)) => Response::Get(v),
        Ok(ShardReply::DeleteExisted(_)) => Response::DeleteOk,
        // 其他 op 不会被网络层触发
        Ok(other) => Response::Error(format!("unexpected reply: {other:?}")),
        Err(kind) => Response::Error(format!("{kind:?}")),
    }
}

#[derive(Debug, Clone)]
pub struct ReplyBusSender {
    inner: Sender<ReplyEnvelope>,
}

#[derive(Debug, Clone)]
pub struct ReplyBusReceiver {
    inner: Receiver<ReplyEnvelope>,
}

pub fn reply_bus() -> (ReplyBusSender, ReplyBusReceiver) {
    let (tx, rx) = unbounded();
    (ReplyBusSender { inner: tx }, ReplyBusReceiver { inner: rx })
}

impl ReplyBusSender {
    pub fn push(&self, env: ReplyEnvelope) {
        // 失败忽略 (consumer 已关闭, 业务侧已经不需要 reply)
        let _ = self.inner.send(env);
    }
}

impl ReplyBusReceiver {
    pub fn pop(&self) -> Option<ReplyEnvelope> {
        self.inner.recv().ok()
    }

    pub fn try_pop(&self) -> Option<ReplyEnvelope> {
        self.inner.try_recv().ok()
    }

    /// 收集所有 pending reply (非阻塞).
    pub fn drain(&self) -> Vec<ReplyEnvelope> {
        let mut out = Vec::new();
        while let Ok(env) = self.inner.try_recv() {
            out.push(env);
        }
        out
    }
}

impl ReplySink for ReplyBusSender {
    fn push_reply(&self, req_id: u64, shard_id: u32, resp: ShardResponse) {
        self.push(ReplyEnvelope {
            req_id,
            shard_id,
            response: shard_resp_to_response(resp),
        });
    }
}