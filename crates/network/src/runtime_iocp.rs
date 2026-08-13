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

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
        let acceptor_handle = thread::Builder::new()
            .name(format!("runtime-acceptor-{protocol:?}"))
            .spawn(move || acceptor_main(listener, stop_thread, shared_thread, protocol))
            .map_err(|e| std::io::Error::other(format!("spawn acceptor: {e}")))?;

        Ok(Self {
            local_addr,
            stop,
            acceptor_handle: Some(acceptor_handle),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn shutdown(mut self) -> std::io::Result<()> {
        self.stop.store(true, Ordering::Release);
        // Closing the listener requires owning the TcpListener, which lives
        // in the acceptor thread.  We can't drop it from here; instead the
        // shutdown path is:
        //   1. main thread sets stop = true
        //   2. main thread drops the NetworkServer, which is currently
        //      impossible because we just consumed `self` by-value.
        //   3. acceptor thread loops with non-blocking accept and checks
        //      stop each iteration.  After observing stop, it returns and
        //      drops the listener, which then closes the OS socket.
        //
        // We need the accept() to time out so the loop can re-check `stop`.
        // So we set the listener to non-blocking and accept with a short
        // poll interval.  To make that work, we set non-blocking BEFORE
        // handing the listener to the thread.
        // (Done in acceptor_main via `set_nonblocking`.)
        if let Some(h) = self.acceptor_handle.take() {
            let _ = h.join();
        }
        Ok(())
    }
}

fn acceptor_main(
    listener: TcpListener,
    stop: Arc<AtomicBool>,
    shared: Arc<ConnShared>,
    protocol: ProtocolKind,
) {
    // Make accept() non-blocking so we can poll `stop` and break out
    // promptly on shutdown.  Per-conn threads are the heavy lifters; this
    // thread is just a dispatcher.
    if let Err(e) = listener.set_nonblocking(true) {
        eprintln!("[runtime] set_nonblocking failed: {e}");
    }
    let local = listener.local_addr().ok();
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let shared_thread = shared.clone();
                let proto_thread = protocol;
                let name = format!("runtime-conn-{proto_thread:?}");
                let _ = thread::Builder::new()
                    .name(name)
                    .spawn(move || {
                        if let Err(e) = run_conn(stream, shared_thread, proto_thread) {
                            eprintln!("[runtime] conn ended: {e}");
                        }
                    });
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
        RespCommand::Set { key, value } => reply_resp(
            &c,
            dispatch_limited(shared, Request::Put { key, value }),
        ),
        RespCommand::Get { key } => reply_resp(
            &c,
            dispatch_limited(shared, Request::Get { key }),
        ),
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
    validate_request(&req, &shared.limits)
        .map_or_else(Response::Error, |_| {
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
