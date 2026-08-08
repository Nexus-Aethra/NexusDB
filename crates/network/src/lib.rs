//! # `network` crate
//!
//! NexusDB 的网络层. 基于已存在的 per-shard-thread 协程调度 (`scheduler`),
//! 接入 TCP listener, 把外部 client 流量路由到对应的 shard.
//!
//! ## 分层
//!
//! - `protocol` — Presentation Layer: 字节 ↔ KV 转换 (无 shard 知识)
//! - `kv_to_shard` — Application Layer: KV ↔ ShardManager API 转换
//! - `acceptor` — 每协议一个 acceptor 线程, accept 新 conn + 按端口打标协议 + LB
//! - `worker` — 全局共享 worker 池 (线程数 = 用户配置, 不随协议数膨胀);
//!   每 worker 一个 Scheduler (协程 worker) 或 epoll 事件循环 (epoll worker),
//!   按连接上下文 (NewConn.protocol + per-conn 配置) 处理对应协议
//! - `reply_bus` — ShardManager → Worker 的 reply 通道 (crossbeam mpmc)
//!
//! ## 范围
//!
//! 五种协议门面 (自家二进制 + RESP2 + MySQL wire + PostgreSQL wire + HTTP REST)
//! 共享同一批 worker 与存储内核. TLS 支持 SQL/PG 门面 (STARTTLS).

pub mod acceptor;
pub mod kv_to_shard;
pub mod protocol;
pub mod reply_bus;
pub mod server;
pub mod tls;
pub mod value_codec;
pub mod worker;
pub use worker::{SqlSharedRoutes, new_sql_shared};

/// ⭐ H4: 进程级指标 (relaxed 原子, 热路径零锁; /metrics 导出).
pub mod metrics {
    use std::sync::OnceLock;
    use std::sync::atomic::AtomicU64;

    pub static HTTP_REQUESTS: AtomicU64 = AtomicU64::new(0);
    pub static HTTP_ERRORS: AtomicU64 = AtomicU64::new(0);
    /// SQL 语句计数 (三门面共入口: sql_dispatch_stmt).
    pub static SQL_QUERIES: AtomicU64 = AtomicU64::new(0);
    /// RESP 命令计数.
    pub static KV_OPS: AtomicU64 = AtomicU64::new(0);
    /// ⭐ 方案 A (调优): JOIN EstimateRows 统计广播轮数 (合并行数后每批 +1).
    pub static SQL_JOIN_EST_ROUNDS: AtomicU64 = AtomicU64::new(0);
    /// ⭐ 方案 A (调优): 小表阈值跳过统计收集次数 (行数批收齐直接决策).
    pub static SQL_JOIN_EST_SKIPPED: AtomicU64 = AtomicU64::new(0);
    /// 进程启动 unix 秒 (uptime 计算).
    pub static START_UNIX: OnceLock<u64> = OnceLock::new();

    pub fn init_start_time() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let _ = START_UNIX.set(now);
    }

    pub fn uptime_seconds() -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        now.saturating_sub(*START_UNIX.get().unwrap_or(&now))
    }
}

/// ⭐ H1: HTTP 门面 CORS origin 全局配置 (进程单 HTTP server; 启动时 set 一次).
/// 空/未设 = 不发 CORS 头.
pub mod http_config {
    use std::sync::OnceLock;

    static CORS_ORIGIN: OnceLock<Option<String>> = OnceLock::new();

    /// main/测试 在启动 HTTP server 前调用 (幂等, 首次生效).
    pub fn set_cors_origin(origin: Option<String>) {
        let _ = CORS_ORIGIN.set(origin.filter(|s| !s.is_empty()));
    }

    pub fn cors_origin() -> Option<&'static str> {
        CORS_ORIGIN.get().and_then(|o| o.as_deref())
    }
}

/// ⭐ Phase G: geohash 纯函数桥 (协议层编/解码 + worker 渲染用).
pub mod geo_bridge {
    pub use storage::geo::{decode, encode, haversine_m, unit_factor};
}

pub use acceptor::{Acceptor, AcceptorConfig, LbStrategy, NewConn};
pub use kv_to_shard::dispatch_request;
pub use protocol::{
    BinaryProtocol, DecodeOutcome, KvLimits, Protocol, ProtocolError, Request, RespCodec,
    RespCommand, Response, validate_request,
};
pub use reply_bus::{ReplyBusReceiver, ReplyBusSender, ReplyEnvelope};
pub use server::{NetworkServer, NetworkServerConfig, ProtocolKind, SharedWorkerPool};
pub use worker::{WorkerConfig, WorkerPool};