//! Acceptor 线程: TCP listener + LB 分发 fd 到 worker.
//!
//! **简化版**: acceptor 线程无 scheduler. pure blocking accept + send(fd) 到 worker inbox.
//! Worker 用 `from_raw_fd` 重建 TcpStream (在 worker 自有 scheduler + io_uring 上跑 read/write).

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::io::{FromRawFd, IntoRawFd, RawFd};
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_channel::Sender;

/// 待处理的新连接 (含 fd + peer addr + 协议门面 + per-conn server 配置).
///
/// ⭐ 解耦 (Phase 3): 协议与 per-conn 配置随连接传递 (acceptor 投递时打标),
/// 使共享 worker 池能按连接使用正确的配置 (default_db/limits/auth/tls),
/// 多协议 server 共享同一批 worker 的前提.
#[derive(Debug)]
pub struct NewConn {
    pub fd: RawFd,
    pub peer: SocketAddr,
    pub protocol: crate::worker::ProtocolKind,
    /// per-conn 默认 db (server 级).
    pub default_db: std::sync::Arc<str>,
    /// per-conn 默认表 (server 级, RESP/Binary 用).
    pub default_table: std::sync::Arc<str>,
    /// per-conn 协议长度限制 (server 级).
    pub limits: crate::protocol::KvLimits,
    /// per-conn 认证密码 (server 级; None = 免认证).
    pub auth_password: Option<String>,
    /// per-conn TLS 配置 (server 级; None = 明文).
    pub tls_config: Option<std::sync::Arc<rustls::ServerConfig>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LbStrategy {
    RoundRobin,
    Random,
    Sticky,
}

#[derive(Debug)]
pub struct AcceptorConfig {
    pub listen_addr: SocketAddr,
    pub worker_queues: Vec<Sender<NewConn>>,
    /// ⭐ 方向 1: per-worker 新连接通知 eventfd. send 后写对应 fd
    /// 精确唤醒 worker 的 epoll (空 = 不通知, 兼容旧行为).
    pub worker_wakeups: Vec<RawFd>,
    pub lb_strategy: LbStrategy,
    /// ⭐ 解耦 (Phase 3): 本 acceptor 服务的协议门面 (端口即协议), 投递连接时打标.
    pub protocol: crate::worker::ProtocolKind,
    /// per-conn 默认 db (server 级, 投递连接时打标).
    pub default_db: std::sync::Arc<str>,
    /// per-conn 默认表 (server 级).
    pub default_table: std::sync::Arc<str>,
    /// per-conn 协议长度限制 (server 级).
    pub limits: crate::protocol::KvLimits,
    /// per-conn 认证密码 (server 级).
    pub auth_password: Option<String>,
    /// per-conn TLS 配置 (server 级).
    pub tls_config: Option<std::sync::Arc<rustls::ServerConfig>>,
}

pub struct Acceptor;

impl Acceptor {
    /// 阻塞 accept loop. 直到 listener 出错或所有 worker inbox 都断开.
    pub fn run(config: AcceptorConfig) -> std::io::Result<()> {
        let listener = TcpListener::bind(config.listen_addr)?;
        let worker_count = config.worker_queues.len();
        if worker_count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "AcceptorConfig::worker_queues is empty",
            ));
        }
        let next_worker = AtomicUsize::new(0);

        listener.set_nonblocking(false)?;
        loop {
            let (stream, peer) = match listener.accept() {
                Ok(pair) => pair,
                Err(e) => {
                    nlog::error!("acceptor", "accept error: {e}");
                    return Err(e);
                }
            };
            // 转 raw fd 后 ownership 进 NewConn; worker 拿 fd 重建 TcpStream.
            let fd = stream.into_raw_fd();

            let idx = match config.lb_strategy {
                LbStrategy::RoundRobin => {
                    next_worker.fetch_add(1, Ordering::Relaxed) % worker_count
                }
                LbStrategy::Random => {
                    use std::collections::hash_map::DefaultHasher;
                    use std::hash::{Hash, Hasher};
                    let mut h = DefaultHasher::new();
                    peer.hash(&mut h);
                    (h.finish() as usize) % worker_count
                }
                LbStrategy::Sticky => {
                    use std::collections::hash_map::DefaultHasher;
                    use std::hash::{Hash, Hasher};
                    let mut h = DefaultHasher::new();
                    peer.ip().hash(&mut h);
                    (h.finish() as usize) % worker_count
                }
            };

            let new_conn = NewConn {
                fd,
                peer,
                protocol: config.protocol,
                default_db: config.default_db.clone(),
                default_table: config.default_table.clone(),
                limits: config.limits,
                auth_password: config.auth_password.clone(),
                tls_config: config.tls_config.clone(),
            };
            if config.worker_queues[idx].send(new_conn).is_err() {
                // 关闭 fd (没人会重建这个连接)
                // SAFETY: fd 是合法 owned fd, drop 时 close.
                let _ = unsafe { TcpStream::from_raw_fd(fd) };
                nlog::warn!(
                    "acceptor",
                    "worker {idx} inbox closed; dropping conn from {peer} (shutdown?)"
                );
                return Ok(()); // graceful exit
            }
            // ⭐ 通知 worker 有新连接
            notify_worker(&config.worker_wakeups, idx);
        }
    }
}

/// send 新连接后写 worker 的 wakeup eventfd (如果配置了).
pub(crate) fn notify_worker(wakeups: &[RawFd], idx: usize) {
    if let Some(&fd) = wakeups.get(idx) {
        let val: u64 = 1;
        unsafe {
            libc::write(fd, &val as *const u64 as *const libc::c_void, 8);
        }
    }
}

/// 测试 helper: 把 raw fd 包回 TcpStream.
///
/// # Safety
///
/// `fd` 必须是合法的、已连接的 socket fd, 且调用者拥有其所有权.
/// 调用后原 fd 不再可独立使用, 其生命周期移交给返回的 `TcpStream`.
pub unsafe fn fd_to_tcp_stream(fd: RawFd) -> TcpStream {
    unsafe { TcpStream::from_raw_fd(fd) }
}

/// 测试 helper: 取 listener local addr.
pub fn local_addr_of(listener: &TcpListener) -> std::io::Result<SocketAddr> {
    listener.local_addr()
}
