//! Windows runtime for the network layer.
//!
//! M2 (revised after IOCP/AcceptEx dead-loop on Windows): pure std blocking
//! path.  One acceptor thread calls `TcpListener::incoming()`; each accepted
//! `TcpStream` is dispatched to a fresh `std::thread` that runs RESP / Binary
//! protocol dispatch synchronously.  Concurrency comes from one thread per
//! connection; per-thread blocking IO keeps the code path simple and
//! debuggable.  M3+ will revisit IOCP (or RIO) for higher fan-out.
//!
//! Only `ProtocolKind::Binary` and `ProtocolKind::Resp` are accepted in M2;
//! Sql / Pg / Http land in M4-M6.

#![cfg(target_os = "windows")]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use shard_manager::ShardManager;

use crate::kv_to_shard::dispatch_request;
use crate::protocol::{
    BinaryProtocol, DecodeOutcome, KvLimits, Protocol, Request, RespCodec, RespCommand, Response,
    validate_request,
};

const RECV_BUF_CAP: usize = 8 * 1024;

/// Protocol a listener accepts.  M2 routes Binary + Resp; M3+ will route the
/// rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolKind {
    Binary,
    Resp,
    Sql,
    Pg,
    Http,
}

pub struct NetworkServerConfig {
    pub listen_addr: SocketAddr,
    pub protocol: ProtocolKind,
    pub worker_count: usize,
    pub limits: KvLimits,
    pub shard_manager: Arc<ShardManager>,
    pub default_db: String,
    pub default_table: String,
    pub auth_password: Option<String>,
}

struct ConnShared {
    mgr: Arc<ShardManager>,
    default_db: String,
    default_table: String,
    auth_password: Option<String>,
    limits: KvLimits,
}

pub struct NetworkServer {
    local_addr: SocketAddr,
    stop: Arc<AtomicBool>,
    /// Held so the OS socket is only closed when we explicitly shut down;
    /// the acceptor thread observes `stop` and exits.
    acceptor_handle: Option<thread::JoinHandle<()>>,
    /// Cloned socket handles let shutdown unblock connection threads that are
    /// waiting in a blocking read.
    connection_streams: Arc<Mutex<HashMap<u64, TcpStream>>>,
    /// Every spawned connection is joined during shutdown.  This makes the
    /// manager lifetime explicit instead of letting live connections retain it.
    connection_handles: Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
}

impl NetworkServer {
    pub fn start(config: NetworkServerConfig) -> std::io::Result<Self> {
        if !matches!(config.protocol, ProtocolKind::Binary | ProtocolKind::Resp) {
            return Err(std::io::Error::other(
                "M2: runtime_iocp supports Binary and Resp only (Sql/Pg/Http land in M4-M6)",
            ));
        }

        let listener = TcpListener::bind(config.listen_addr)?;
        let local_addr = listener.local_addr()?;

        let shared = Arc::new(ConnShared {
            mgr: config.shard_manager,
            default_db: config.default_db,
            default_table: config.default_table,
            auth_password: config.auth_password,
            limits: config.limits,
        });

        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();
        let shared_thread = shared.clone();
        let protocol = config.protocol;
        let connection_streams = Arc::new(Mutex::new(HashMap::new()));
        let connection_handles = Arc::new(Mutex::new(Vec::new()));
        let next_connection_id = Arc::new(AtomicU64::new(1));
        let streams_thread = connection_streams.clone();
        let handles_thread = connection_handles.clone();
        let ids_thread = next_connection_id.clone();
        let acceptor_handle = thread::Builder::new()
            .name(format!("runtime-acceptor-{protocol:?}"))
            .spawn(move || {
                acceptor_main(
                    listener,
                    stop_thread,
                    shared_thread,
                    protocol,
                    streams_thread,
                    handles_thread,
                    ids_thread,
                )
            })
            .map_err(|e| std::io::Error::other(format!("spawn acceptor: {e}")))?;

        Ok(Self {
            local_addr,
            stop,
            acceptor_handle: Some(acceptor_handle),
            connection_streams,
            connection_handles,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn shutdown(mut self) -> std::io::Result<()> {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.acceptor_handle.take() {
            let _ = h.join();
        }
        // The acceptor has stopped, so its handle list is complete.  Force
        // every blocking `read` to return before joining connection threads.
        let streams = std::mem::take(
            &mut *self
                .connection_streams
                .lock()
                .expect("connection streams lock"),
        );
        for (_, stream) in streams {
            let _ = stream.shutdown(Shutdown::Both);
        }
        for handle in self
            .connection_handles
            .lock()
            .expect("connection handles lock")
            .drain(..)
        {
            let _ = handle.join();
        }
        Ok(())
    }
}

fn acceptor_main(
    listener: TcpListener,
    stop: Arc<AtomicBool>,
    shared: Arc<ConnShared>,
    protocol: ProtocolKind,
    connection_streams: Arc<Mutex<HashMap<u64, TcpStream>>>,
    connection_handles: Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
    next_connection_id: Arc<AtomicU64>,
) {
    // Make accept() non-blocking so we can poll `stop` and break out
    // promptly on shutdown.  Per-conn threads are the heavy lifters; this
    // thread is just a dispatcher.
    if let Err(e) = listener.set_nonblocking(true) {
        eprintln!("[runtime] set_nonblocking failed: {e}");
    }
    let local = listener.local_addr().ok();
    while !stop.load(Ordering::Acquire) {
        reap_finished_connections(&connection_handles);
        match listener.accept() {
            Ok((stream, _addr)) => {
                let shutdown_stream = match stream.try_clone() {
                    Ok(stream) => stream,
                    Err(e) => {
                        eprintln!("[runtime] clone connection for shutdown failed: {e}");
                        continue;
                    }
                };
                let shared_thread = shared.clone();
                let proto_thread = protocol;
                let name = format!("runtime-conn-{proto_thread:?}");
                let connection_id = next_connection_id.fetch_add(1, Ordering::Relaxed);
                connection_streams
                    .lock()
                    .expect("connection streams lock")
                    .insert(connection_id, shutdown_stream);
                let streams_thread = connection_streams.clone();
                match thread::Builder::new().name(name).spawn(move || {
                    if let Err(e) = run_conn(stream, shared_thread, proto_thread) {
                        eprintln!("[runtime] conn ended: {e}");
                    }
                    streams_thread
                        .lock()
                        .expect("connection streams lock")
                        .remove(&connection_id);
                }) {
                    Ok(handle) => {
                        connection_handles
                            .lock()
                            .expect("connection handles lock")
                            .push(handle);
                    }
                    Err(e) => {
                        connection_streams
                            .lock()
                            .expect("connection streams lock")
                            .remove(&connection_id);
                        eprintln!("[runtime] spawn connection failed: {e}");
                    }
                }
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(20));
                }
                std::io::ErrorKind::Interrupted => continue,
                _ => {
                    eprintln!("[runtime] accept error: {e}");
                    thread::sleep(Duration::from_millis(50));
                }
            },
        }
    }
    if let Some(addr) = local {
        eprintln!("[runtime] acceptor for {addr} exiting");
    }
}

/// A completed thread must be joined to release its OS handle.  M2 uses one
/// blocking thread per connection, so reclaim them in the acceptor instead of
/// retaining one join handle for every historical client connection.
fn reap_finished_connections(handles: &Mutex<Vec<thread::JoinHandle<()>>>) {
    let mut handles = handles.lock().expect("connection handles lock");
    let mut live = Vec::with_capacity(handles.len());
    for handle in handles.drain(..) {
        if handle.is_finished() {
            let _ = handle.join();
        } else {
            live.push(handle);
        }
    }
    *handles = live;
}

fn run_conn(
    mut stream: TcpStream,
    shared: Arc<ConnShared>,
    protocol: ProtocolKind,
) -> std::io::Result<()> {
    let mut buf = vec![0u8; RECV_BUF_CAP];
    let mut read_buf: Vec<u8> = Vec::with_capacity(RECV_BUF_CAP * 2);
    // If no AUTH is required, every conn is auto-authed; this matches the
    // Linux worker's `authenticated = password.is_none()`.
    let mut authed = shared.auth_password.is_none();

    loop {
        let n = match stream.read(&mut buf) {
            Ok(0) => return Ok(()), // graceful EOF
            Ok(n) => n,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(e);
            }
        };
        read_buf.extend_from_slice(&buf[..n]);

        // Drain as many complete frames as we can from the read buffer.
        loop {
            match protocol {
                ProtocolKind::Resp => {
                    let outcome = RespCodec::new().decode_command(&read_buf);
                    match outcome {
                        Ok(DecodeOutcome::NeedMore) => break,
                        Ok(DecodeOutcome::Complete { consumed, value }) => {
                            read_buf.drain(..consumed);
                            let response = dispatch_resp(&mut authed, &shared, value);
                            stream.write_all(&response)?;
                        }
                        Err(e) => {
                            let err = format!("-ERR {e}\r\n").into_bytes();
                            stream.write_all(&err)?;
                            read_buf.clear();
                            break;
                        }
                    }
                }
                ProtocolKind::Binary => {
                    let outcome = BinaryProtocol::new().decode_request(&read_buf);
                    match outcome {
                        Ok(DecodeOutcome::NeedMore) => break,
                        Ok(DecodeOutcome::Complete { consumed, value }) => {
                            read_buf.drain(..consumed);
                            let response = match validate_request(&value, &shared.limits) {
                                Ok(()) => binary_client_response(dispatch_request(
                                    &shared.mgr,
                                    &shared.default_db,
                                    &shared.default_table,
                                    value,
                                )),
                                Err(e) => Response::Error(e),
                            };
                            let bytes = BinaryProtocol::new().encode_response(0, &response);
                            stream.write_all(&bytes)?;
                        }
                        Err(_) => {
                            // Bad frame — drop it and keep the conn alive.
                            read_buf.clear();
                            break;
                        }
                    }
                }
                ProtocolKind::Sql | ProtocolKind::Pg | ProtocolKind::Http => {
                    // M4-M6: not yet supported on Windows.
                    let err = b"-ERR not yet supported on Windows runtime\r\n".to_vec();
                    stream.write_all(&err)?;
                    read_buf.clear();
                    break;
                }
            }
        }
    }
}

fn dispatch_resp(authed: &mut bool, shared: &ConnShared, cmd: RespCommand) -> Vec<u8> {
    let c = RespCodec::new();
    match cmd {
        RespCommand::Auth { pass: supplied, .. } => {
            *authed = shared
                .auth_password
                .as_ref()
                .is_none_or(|p| p.as_bytes() == supplied.as_slice());
            if *authed {
                c.encode_ok()
            } else {
                c.encode_error("WRONGPASS invalid username-password pair")
            }
        }
        _ if !*authed => c.encode_error("NOAUTH Authentication required."),
        RespCommand::Ping(v) => v.map_or_else(|| c.encode_simple("PONG"), |v| c.encode_bulk(&v)),
        RespCommand::Command => c.encode_empty_array(),
        RespCommand::Set { key, value } => {
            reply_resp(&c, dispatch_limited(shared, Request::Put { key, value }))
        }
        RespCommand::Get { key } => reply_resp(&c, dispatch_limited(shared, Request::Get { key })),
        RespCommand::Del { keys } => {
            let mut n = 0;
            for key in keys {
                if matches!(
                    dispatch_limited(shared, Request::Delete { key }),
                    Response::DeleteOk
                ) {
                    n += 1;
                }
            }
            c.encode_integer(n)
        }
        other => c.encode_error(&format!("runtime_iocp does not yet support {other:?}")),
    }
}

fn dispatch_limited(shared: &ConnShared, req: Request) -> Response {
    validate_request(&req, &shared.limits).map_or_else(Response::Error, |_| {
        dispatch_request(&shared.mgr, &shared.default_db, &shared.default_table, req)
    })
}

fn reply_resp(c: &RespCodec, response: Response) -> Vec<u8> {
    match response {
        Response::PutOk | Response::DeleteOk => c.encode_ok(),
        Response::Get(Some(v)) => c.encode_bulk(shard_manager::value_num::render(&v).as_ref()),
        Response::Get(None) => c.encode_nil(),
        Response::Error(e) => c.encode_error(&e),
    }
}

fn binary_client_response(response: Response) -> Response {
    match response {
        Response::Get(Some(stored)) => {
            Response::Get(Some(crate::value_codec::decode_value(&stored).1.to_vec()))
        }
        other => other,
    }
}
