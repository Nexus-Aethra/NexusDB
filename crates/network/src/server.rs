//! NetworkServer: 顶层组装 acceptor + worker pool + ShardManager.
//!
//! **Phase 4.1 实现**:
//! - bind TcpListener 到指定 addr
//! - 创建 N 个 worker inbox (crossbeam bounded)
//! - 启动 N 个 worker threads
//! - 启动 acceptor 线程
//!
//! **简化**: 不接 reply_bus (Phase 6+); 默认 RoundRobin LB; worker 用同步 ShardManager API.

use std::net::{SocketAddr, TcpListener};
use std::os::fd::{IntoRawFd, RawFd};
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{bounded, Sender};
use shard_manager::ShardManager;

use crate::acceptor::{AcceptorConfig, NewConn};
use crate::protocol::KvLimits;
use crate::worker::{WorkerConfig, WorkerPool};

pub use crate::worker::ProtocolKind;

pub struct NetworkServerConfig {
    pub listen_addr: SocketAddr,
    pub shard_manager: Arc<ShardManager>,
    pub worker_count: usize,
    pub default_db: String,
    pub default_table: String,
    /// 内部 worker inbox 容量. 默认 1024.
    pub inbox_capacity: usize,
    /// 本 server 所有连接的协议门面.
    pub protocol: ProtocolKind,
    /// KV 长度限制.
    pub limits: KvLimits,
    /// RESP AUTH 密码 (None = 不启用认证; Binary 协议忽略).
    pub auth_password: Option<String>,
    /// worker_id 起点 (多协议 server 并存时隔开 reply_bus 空间).
    pub worker_id_base: u32,
    /// ⭐ ORM-B2: 进程级共享路由缓存 — 同一数据集群 (同 ShardManager) 的
    /// 全部 SQL 门面必须传同一个实例 (跨门面 INSERT/SELECT 一致性).
    pub sql_shared: std::sync::Arc<crate::worker::SqlSharedRoutes>,
    /// ⭐ F83: TLS 配置 (None = 明文; Some = SQL 门面支持 STARTTLS 升级).
    pub tls_config: Option<Arc<rustls::ServerConfig>>,
}

pub struct NetworkServer {
    local_addr: SocketAddr,
    worker_pool: WorkerPool,
    acceptor_handle: Option<thread::JoinHandle<()>>,
    worker_inboxes: Vec<Sender<NewConn>>,
    /// ⭐ 协程 worker shutdown: per-worker 新连接通知 eventfd, shutdown 时写唤醒 worker.
    worker_wakeups: Vec<RawFd>,
    acceptor_stop: Option<Arc<std::sync::atomic::AtomicBool>>,
}

impl NetworkServer {
    pub fn start(config: NetworkServerConfig) -> std::io::Result<Self> {
        let listen_addr = config.listen_addr;
        let worker_count = config.worker_count.max(1);
        let inbox_capacity = config.inbox_capacity.max(64);

        // 1. 创建 N 个 worker inbox
        let mut worker_inboxes: Vec<Sender<NewConn>> = Vec::with_capacity(worker_count);
        let mut worker_inbox_recv: Vec<_> = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let (tx, rx) = bounded(inbox_capacity);
            worker_inboxes.push(tx);
            worker_inbox_recv.push(rx);
        }

        // 2. 提前创建 listener (暴露 local_addr 给 caller)
        let listener = TcpListener::bind(listen_addr)?;
        let local_addr = listener.local_addr()?;

        // 3. 启动 worker threads
        //    ⭐ 方向 1: 每 worker 一个新连接通知 eventfd (nonblocking, 供 epoll 注册).
        //    fd 所有权归 worker (退出时 close); acceptor 只写不 close.
        let num_shards = config.shard_manager.num_shards();
        let mut worker_wakeups: Vec<RawFd> = Vec::with_capacity(worker_count);
        let mut worker_configs = Vec::with_capacity(worker_count);
        for (i, rx) in worker_inbox_recv.into_iter().enumerate() {
            let conn_eventfd =
                unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
            assert!(conn_eventfd >= 0, "eventfd creation failed");
            worker_wakeups.push(conn_eventfd);
            // 收集所有 shard 的 task inbox
            let shard_inboxes: Vec<_> = (0..num_shards)
                .map(|s| config.shard_manager.task_inbox(s).clone())
                .collect();
            let worker_id = config.worker_id_base + i as u32;
            worker_configs.push(WorkerConfig {
                worker_id,
                inbox: rx,
                conn_eventfd,
                shard_inboxes,
                reply_bus: config.shard_manager.reply_bus_set.get_arc(worker_id),
                default_db: config.default_db.clone(),
                default_table: config.default_table.clone(),
                protocol: config.protocol,
                limits: config.limits,
                auth_password: config.auth_password.clone(),
                // ⭐ D3 (分库): SELECT n → db name 翻译视图 (Arc 共享只读)
                db_view: config.shard_manager.db_view(),
                sql_shared: config.sql_shared.clone(),
                tls_config: config.tls_config.clone(),
            });
        }
        let worker_pool = WorkerPool::start(worker_configs)?;

        // 4. 启动 acceptor 线程. Acceptor 不用 listener 而是用 bind 信息,所以我们把 listener 转入 acceptor.
        //    acceptor 内部 bind 会冲突,所以 listener 作为参数传入.
        //    这里做一个变通: 我们直接 spawn 一个 thread, accept 后用 worker_inboxes[idx] send.
        // ⭐ 协程 worker shutdown: 保留 worker_wakeups 副本, shutdown 时写它们唤醒
        // worker 的 new_conn_loop (检测 inbox 断开). acceptor 用 clone.
        let worker_wakeups_server = worker_wakeups.clone();
        let acceptor_cfg = AcceptorConfig {
            listen_addr: local_addr,
            worker_queues: worker_inboxes.clone(),
            worker_wakeups,
            lb_strategy: crate::acceptor::LbStrategy::RoundRobin,
            protocol: config.protocol,
        };
        let acceptor_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let acceptor_stop_clone = acceptor_stop.clone();
        let acceptor_handle = thread::Builder::new()
            .name("network-acceptor".to_string())
            .spawn(move || {
                if let Err(e) =
                    acceptor_run_with_listener(listener, acceptor_cfg, acceptor_stop_clone)
                {
                    nlog::error!("acceptor", "exited with error: {e}");
                }
            })
            .map_err(|e| std::io::Error::other(format!("spawn acceptor: {e}")))?;

        Ok(Self {
            local_addr,
            worker_pool,
            acceptor_handle: Some(acceptor_handle),
            worker_inboxes,
            worker_wakeups: worker_wakeups_server,
            acceptor_stop: Some(acceptor_stop),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn shutdown(mut self) -> std::io::Result<()> {
        // 1. Signal acceptor stop first so it breaks out of accept() loop.
        if let Some(sig) = self.acceptor_stop.take() {
            sig.store(true, std::sync::atomic::Ordering::Release);
        }
        // 2. Drop worker senders so workers' inbox.recv() returns Err.
        self.worker_inboxes.clear();
        // 3. Join acceptor (it will exit on next iter after stop_signal is set, dropping
        //    its sender clones). 必须等在 acceptor 退出后再唤醒 worker, 否则
        //    worker 的 new_conn_loop 检测不到 inbox 断开 (acceptor 的 sender 还活着).
        if let Some(h) = self.acceptor_handle.take() {
            let _ = h.join();
        }
        // 4. ⭐ 协程 worker: 写 wakeup eventfd 唤醒 worker 的 new_conn_loop,
        //    使其检测到 inbox 断开并停止调度 (epoll worker 靠 inbox.recv 断开, 无需此步).
        for &fd in &self.worker_wakeups {
            let val: u64 = 1;
            unsafe {
                libc::write(fd, &val as *const u64 as *const libc::c_void, 8);
            }
        }
        // 5. Join workers.
        self.worker_pool.join()?;
        Ok(())
    }
}

/// acceptor 但用传入的 listener (而不是自己 bind).
fn acceptor_run_with_listener(
    listener: TcpListener,
    config: AcceptorConfig,
    stop_signal: Arc<std::sync::atomic::AtomicBool>,
) -> std::io::Result<()> {
    use std::os::fd::FromRawFd;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    let worker_count = config.worker_queues.len();
    if worker_count == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "no worker queues",
        ));
    }
    let next_worker = AtomicUsize::new(0);

    // 用 set_nonblocking + poll 周期性检查 stop_signal:
    // 100ms 内有新 conn 就 accept, 否则检查是否被 stop.
    listener.set_nonblocking(true)?;

    loop {
        // Check stop signal first.
        if stop_signal.load(std::sync::atomic::Ordering::Acquire) {
            nlog::info!("acceptor", "stop signaled");
            return Ok(());
        }

        let (stream, peer) = match listener.accept() {
            Ok(p) => p,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // 100ms 后再 check stop.
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            Err(e) => {
                if stop_signal.load(std::sync::atomic::Ordering::Acquire) {
                    return Ok(());
                }
                nlog::error!("acceptor", "accept error: {e}");
                return Err(e);
            }
        };
        let fd: RawFd = stream.into_raw_fd();

        let idx = match config.lb_strategy {
            crate::acceptor::LbStrategy::RoundRobin => {
                next_worker.fetch_add(1, Ordering::Relaxed) % worker_count
            }
            crate::acceptor::LbStrategy::Random => {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut h = DefaultHasher::new();
                peer.hash(&mut h);
                (h.finish() as usize) % worker_count
            }
            crate::acceptor::LbStrategy::Sticky => {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut h = DefaultHasher::new();
                peer.ip().hash(&mut h);
                (h.finish() as usize) % worker_count
            }
        };

        let new_conn = NewConn { fd, peer, protocol: config.protocol };
        if config.worker_queues[idx].send(new_conn).is_err() {
            let _ = unsafe { std::net::TcpStream::from_raw_fd(fd) };
            nlog::warn!("acceptor", "worker {idx} inbox closed; shutdown");
            return Ok(());
        }
        // ⭐ 方向 1: 通知 worker 有新连接 (精确唤醒 epoll)
        crate::acceptor::notify_worker(&config.worker_wakeups, idx);
    }
}
