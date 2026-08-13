//! Windows/portable network runtime.
//!
//! This deliberately small P1 implementation uses blocking `std::net` sockets
//! and one thread per accepted connection.  It keeps the Linux epoll/io_uring
//! fast path untouched while making the native Windows build useful for the
//! Binary protocol and the common RESP commands (PING/GET/SET/DEL).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use shard_manager::ShardManager;

use crate::kv_to_shard::dispatch_request;
use crate::protocol::{
    BinaryProtocol, DecodeOutcome, KvLimits, Protocol, Request, RespCodec, RespCommand, Response,
    validate_request,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolKind {
    Binary,
    Resp,
    Sql,
    Pg,
    Http,
}

pub struct SqlSharedRoutes {
    cluster_ctl: RwLock<Option<Arc<ShardManager>>>,
}
impl Default for SqlSharedRoutes {
    fn default() -> Self {
        Self {
            cluster_ctl: RwLock::new(None),
        }
    }
}
impl SqlSharedRoutes {
    pub fn set_cluster_ctl(&self, mgr: Arc<ShardManager>) {
        *self.cluster_ctl.write().expect("cluster_ctl lock") = Some(mgr);
    }
}
pub fn new_sql_shared() -> Arc<SqlSharedRoutes> {
    Arc::new(SqlSharedRoutes::default())
}

pub struct NetworkServerConfig {
    pub listen_addr: SocketAddr,
    pub shard_manager: Arc<ShardManager>,
    pub worker_count: usize,
    pub default_db: String,
    pub default_table: String,
    pub inbox_capacity: usize,
    pub protocol: ProtocolKind,
    pub limits: KvLimits,
    pub auth_password: Option<String>,
    pub worker_id_base: u32,
    pub sql_shared: Arc<SqlSharedRoutes>,
    pub tls_config: Option<Arc<crate::tls::ServerConfig>>,
    pub shared_workers: Option<Arc<SharedWorkerPool>>,
}

pub struct SharedWorkerPool;
impl SharedWorkerPool {
    pub fn new(base: &NetworkServerConfig, _base_worker_id: u32) -> std::io::Result<Arc<Self>> {
        if !matches!(base.protocol, ProtocolKind::Binary | ProtocolKind::Resp) {
            return Err(std::io::Error::other(
                "portable runtime supports Binary and RESP only",
            ));
        }
        Ok(Arc::new(Self))
    }
}

pub struct NetworkServer {
    local_addr: SocketAddr,
    stop: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
}
impl NetworkServer {
    pub fn start(config: NetworkServerConfig) -> std::io::Result<Self> {
        if !matches!(config.protocol, ProtocolKind::Binary | ProtocolKind::Resp) {
            return Err(std::io::Error::other(
                "portable runtime supports Binary and RESP only",
            ));
        }
        if config.tls_config.is_some() {
            return Err(std::io::Error::other(
                "TLS is not available in the portable runtime",
            ));
        }
        let listener = TcpListener::bind(config.listen_addr)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let join = thread::Builder::new()
            .name("network-portable-acceptor".into())
            .spawn(move || {
                while !stop2.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let mgr = config.shard_manager.clone();
                            let db = config.default_db.clone();
                            let table = config.default_table.clone();
                            let protocol = config.protocol;
                            let limits = config.limits;
                            let password = config.auth_password.clone();
                            let _ = thread::Builder::new()
                                .name("network-portable-conn".into())
                                .spawn(move || {
                                    serve_conn(stream, mgr, db, table, protocol, limits, password)
                                });
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10))
                        }
                        Err(e) => {
                            nlog::warn!("network", "portable accept error: {e}");
                            break;
                        }
                    }
                }
            })
            .map_err(|e| std::io::Error::other(format!("spawn portable acceptor: {e}")))?;
        Ok(Self {
            local_addr,
            stop,
            join: Some(join),
        })
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
    pub fn shutdown(mut self) -> std::io::Result<()> {
        self.stop.store(true, Ordering::Release);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        Ok(())
    }
}

fn serve_conn(
    mut stream: TcpStream,
    mgr: Arc<ShardManager>,
    db: String,
    table: String,
    protocol: ProtocolKind,
    limits: KvLimits,
    password: Option<String>,
) {
    let mut buf = Vec::with_capacity(8192);
    let mut scratch = [0u8; 8192];
    let mut authed = password.is_none();
    loop {
        match stream.read(&mut scratch) {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&scratch[..n]),
        }
        loop {
            let out = match protocol {
                ProtocolKind::Binary => match BinaryProtocol.decode_request(&buf) {
                    Ok(DecodeOutcome::NeedMore) => break,
                    Ok(DecodeOutcome::Complete { consumed, value }) => {
                        buf.drain(..consumed);
                        match validate_request(&value, &limits) {
                            Ok(()) => BinaryProtocol.encode_response(
                                0,
                                &binary_client_response(dispatch_request(&mgr, &db, &table, value)),
                            ),
                            Err(e) => BinaryProtocol.encode_response(0, &Response::Error(e)),
                        }
                    }
                    Err(_) => return,
                },
                ProtocolKind::Resp => match RespCodec::new().decode_command(&buf) {
                    Ok(DecodeOutcome::NeedMore) => break,
                    Ok(DecodeOutcome::Complete { consumed, value }) => {
                        buf.drain(..consumed);
                        dispatch_resp(
                            &mgr,
                            &db,
                            &table,
                            &limits,
                            password.as_deref(),
                            &mut authed,
                            value,
                        )
                    }
                    Err(e) => {
                        let _ = stream.write_all(&RespCodec::new().encode_error(&e));
                        return;
                    }
                },
                _ => return,
            };
            if stream.write_all(&out).is_err() {
                return;
            }
        }
    }
}

fn dispatch_resp(
    mgr: &ShardManager,
    db: &str,
    table: &str,
    limits: &KvLimits,
    password: Option<&str>,
    authed: &mut bool,
    cmd: RespCommand,
) -> Vec<u8> {
    let c = RespCodec::new();
    match cmd {
        RespCommand::Auth { pass: supplied, .. } => {
            *authed = password.is_none_or(|p| p.as_bytes() == supplied.as_slice());
            if *authed {
                c.encode_ok()
            } else {
                c.encode_error("WRONGPASS invalid username-password pair")
            }
        }
        _ if !*authed => c.encode_error("NOAUTH Authentication required."),
        RespCommand::Ping(v) => v.map_or_else(|| c.encode_simple("PONG"), |v| c.encode_bulk(&v)),
        RespCommand::Command => c.encode_empty_array(),
        RespCommand::Set { key, value } => reply_resp(
            c,
            dispatch_limited(mgr, db, table, limits, Request::Put { key, value }),
        ),
        RespCommand::Get { key } => reply_resp(
            c,
            dispatch_limited(mgr, db, table, limits, Request::Get { key }),
        ),
        RespCommand::Del { keys } => {
            let mut n = 0;
            for key in keys {
                if matches!(
                    dispatch_limited(mgr, db, table, limits, Request::Delete { key }),
                    Response::DeleteOk
                ) {
                    n += 1;
                }
            }
            c.encode_integer(n)
        }
        other => c.encode_error(&format!("portable runtime does not yet support {other:?}")),
    }
}
fn dispatch_limited(
    mgr: &ShardManager,
    db: &str,
    table: &str,
    limits: &KvLimits,
    req: Request,
) -> Response {
    validate_request(&req, limits)
        .map_or_else(Response::Error, |_| dispatch_request(mgr, db, table, req))
}
fn reply_resp(c: RespCodec, response: Response) -> Vec<u8> {
    match response {
        Response::PutOk | Response::DeleteOk => c.encode_ok(),
        Response::Get(Some(v)) => c.encode_bulk(shard_manager::value_num::render(&v).as_ref()),
        Response::Get(None) => c.encode_nil(),
        Response::Error(e) => c.encode_error(&e),
    }
}

/// Binary clients exchange raw payload bytes; the storage type tag is internal.
fn binary_client_response(response: Response) -> Response {
    match response {
        Response::Get(Some(stored)) => {
            Response::Get(Some(crate::value_codec::decode_value(&stored).1.to_vec()))
        }
        other => other,
    }
}
