//! # `network` crate
//!
//! NexusDB 的网络层. 基于已存在的 per-shard-thread 协程调度 (`scheduler`),
//! 接入 TCP listener, 把外部 client 流量路由到对应的 shard.
//!
//! ## 分层
//!
//! - `protocol` — Presentation Layer: 字节 ↔ KV 转换 (无 shard 知识)
//! - `kv_to_shard` — Application Layer: KV ↔ ShardManager API 转换
//! - `acceptor` — 1 个 acceptor 线程, accept 新 conn + LB
//! - `worker` — N 个 worker 线程, 每个 own 1 个 Scheduler
//! - `reply_bus` — ShardManager → Worker 的 reply 通道 (crossbeam mpmc)
//!
//! ## 范围
//!
//! 自家二进制协议 + RESP2 (Redis 兼容, 含 AUTH) + KV 路由. TLS 不在范围.

pub mod acceptor;
pub mod kv_to_shard;
pub mod protocol;
pub mod reply_bus;
pub mod server;
pub mod value_codec;
pub mod worker;

pub use acceptor::{Acceptor, AcceptorConfig, LbStrategy, NewConn};
pub use kv_to_shard::dispatch_request;
pub use protocol::{
    BinaryProtocol, DecodeOutcome, KvLimits, Protocol, ProtocolError, Request, RespCodec,
    RespCommand, Response, validate_request,
};
pub use reply_bus::{ReplyBusReceiver, ReplyBusSender, ReplyEnvelope};
pub use server::{NetworkServer, NetworkServerConfig, ProtocolKind};
pub use worker::{WorkerConfig, WorkerPool};