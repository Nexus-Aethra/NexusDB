//! Windows IOCP runtime for the network layer.
//!
//! M1 skeleton: bind + listen (std::net) + IOCP worker pool + per-conn echo
//! (no protocol parsing yet — that lands in M2-M6).  See
//! `docs/plans/2026-08-13-windows-iocp.md` for the full design.

#![cfg(target_os = "windows")]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
use windows_sys::Win32::Networking::WinSock::{
    closesocket, WSARecv, WSASend, INVALID_SOCKET, SOCKADDR_STORAGE, SOCKET, WSAEWOULDBLOCK,
    WSA_IO_PENDING,
};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatus, PostQueuedCompletionStatus, OVERLAPPED,
};

use crate::protocol::KvLimits;

/// Sentinel completion key for shutdown.
const SHUTDOWN_KEY: usize = 0xFFFF_FFFF;

/// Protocol a listener accepts.  Only `Binary` and `Resp` are actually wired in
/// M1; M2+ will route the rest.
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
}

/// Newtype around `HANDLE` so it is `Send + Sync`.  The IOCP handle is safe to
/// share across worker threads: any operation on it goes through Win32 APIs
/// that themselves are thread-safe (GetQueuedCompletionStatus / PostQueued...).
/// This is `Copy` so we can pass it into each thread without wrapping in
/// `Arc<Mutex<>>` and adding a lock on the hot path.
#[derive(Clone, Copy)]
struct IocpHandle(HANDLE);
unsafe impl Send for IocpHandle {}
unsafe impl Sync for IocpHandle {}

pub struct NetworkServer {
    local_addr: SocketAddr,
    stop: Arc<AtomicBool>,
    iocp: IocpHandle,
    worker_handles: Vec<thread::JoinHandle<()>>,
    acceptor_handle: Option<thread::JoinHandle<()>>,
}

impl NetworkServer {
    pub fn start(config: NetworkServerConfig) -> std::io::Result<Self> {
        // Bind via std so the platform-correct sockaddr path is used.
        let listener = std::net::TcpListener::bind(config.listen_addr)?;
        let local_addr = listener.local_addr()?;
        // WSAStartup not strictly required on modern Windows but call it once
        // per process to be safe; idempotent via a Once.
        wsa_startup()?;

        // Create the IOCP with concurrency = worker_count.
        let iocp_raw: HANDLE = unsafe {
            CreateIoCompletionPort(
                std::ptr::null_mut::<std::ffi::c_void>(),
                std::ptr::null_mut(),
                0,
                config.worker_count as u32,
            )
        };
        if iocp_raw.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        let iocp = IocpHandle(iocp_raw);

        let stop = Arc::new(AtomicBool::new(false));
        let mut worker_handles = Vec::with_capacity(config.worker_count);
        for worker_id in 0..config.worker_count {
            let iocp_thread = iocp;
            let stop_thread = stop.clone();
            let handle = thread::Builder::new()
                .name(format!("iocp-worker-{worker_id}"))
                .stack_size(4 * 1024 * 1024)
                .spawn(move || worker_main(iocp_thread, stop_thread))
                .map_err(|e| std::io::Error::other(format!("spawn worker: {e}")))?;
            worker_handles.push(handle);
        }

        // Acceptor takes the raw SOCKET so we don't have to clone the
        // std listener (which is owned).  It also closes the listener when
        // it exits.
        let raw_listener: SOCKET = {
            use std::os::windows::io::AsRawSocket;
            listener.as_raw_socket() as SOCKET
        };
        let acceptor_handle = {
            let iocp_thread = iocp;
            let stop_thread = stop.clone();
            thread::Builder::new()
                .name("iocp-acceptor".to_string())
                .spawn(move || acceptor_main(raw_listener, iocp_thread, stop_thread))
                .map_err(|e| std::io::Error::other(format!("spawn acceptor: {e}")))?
        };
        // listener is dropped here; the raw SOCKET in the acceptor thread
        // remains valid because `as_raw_socket` is a borrow, not a move.
        drop(listener);

        Ok(Self {
            local_addr,
            stop,
            iocp,
            worker_handles,
            acceptor_handle: Some(acceptor_handle),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn shutdown(mut self) -> std::io::Result<()> {
        self.stop.store(true, Ordering::Release);

        for _ in 0..self.worker_handles.len() {
            unsafe { PostQueuedCompletionStatus(self.iocp.0, 0, SHUTDOWN_KEY, std::ptr::null_mut()) };
        }
        for h in self.worker_handles.drain(..) {
            let _ = h.join();
        }
        if let Some(h) = self.acceptor_handle.take() {
            let _ = h.join();
        }
        unsafe { CloseHandle(self.iocp.0) };
        Ok(())
    }
}

// ===========================================================================
// Acceptor: blocking accept, then post the first WSARecv on the new socket.
// ===========================================================================

fn acceptor_main(raw_listener: SOCKET, iocp: IocpHandle, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Acquire) {
        let mut storage: SOCKADDR_STORAGE = unsafe { std::mem::zeroed() };
        let mut len: i32 = std::mem::size_of::<SOCKADDR_STORAGE>() as i32;
        let accepted = unsafe {
            windows_sys::Win32::Networking::WinSock::accept(
                raw_listener,
                &mut storage as *mut _ as *mut _,
                &mut len,
            )
        };
        if accepted == INVALID_SOCKET {
            if stop.load(Ordering::Acquire) {
                break;
            }
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(WSAEWOULDBLOCK) {
                thread::sleep(Duration::from_millis(20));
                continue;
            }
            // Transient error: back off briefly and keep going.
            thread::sleep(Duration::from_millis(50));
            continue;
        }

        // Register the socket to the IOCP.  Completion key is the raw socket
        // value cast to usize so the worker can recover it.
        let key = accepted as usize;
        let sock_handle = accepted as isize as *mut std::ffi::c_void;
        let reg: HANDLE = unsafe {
            CreateIoCompletionPort(sock_handle, iocp.0, key, 0)
        };
        if reg.is_null() {
            unsafe { closesocket(accepted) };
            continue;
        }

        // Allocate a fresh OverlappedData for the first WSARecv.
        let data = OverlappedData::new_recv(accepted, 8 * 1024);
        unsafe { post_wsa_recv(accepted, data) };
    }

    // Acceptor exiting: close the listener so no new connections arrive.
    unsafe { closesocket(raw_listener) };
}

// ===========================================================================
// Worker: GQCS -> handle completion.
// ===========================================================================

fn worker_main(iocp: IocpHandle, stop: Arc<AtomicBool>) {
    loop {
        let mut bytes_transferred: u32 = 0;
        let mut completion_key: usize = 0;
        let mut overlapped: *mut OVERLAPPED = std::ptr::null_mut();

        let ok = unsafe {
            GetQueuedCompletionStatus(
                iocp.0,
                &mut bytes_transferred,
                &mut completion_key,
                &mut overlapped,
                100, // 100ms timeout -> periodically check stop
            )
        };

        if completion_key == SHUTDOWN_KEY {
            break;
        }
        if ok == 0 {
            // Timeout or error.  Null overlapped == pure timeout.
            if overlapped.is_null() {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                continue;
            }
        }
        if overlapped.is_null() {
            continue;
        }

        let mut data = unsafe { Box::from_raw(overlapped as *mut OverlappedData) };
        let socket = data.socket;
        let buf = std::mem::take(&mut data.buf);
        // Drop the original OverlappedData now; we pass a fresh one on repost.
        drop(data);

        if bytes_transferred == 0 {
            // Graceful close.
            unsafe { closesocket(socket) };
            continue;
        }

        // Echo: write the received bytes back.  Allocate a fresh send data.
        let send_data = OverlappedData::new_send(socket, &buf[..bytes_transferred as usize]);
        unsafe { post_wsa_send(socket, send_data) };

        // Repost the next WSARecv so the client can keep talking.
        let recv_data = OverlappedData::new_recv(socket, 8 * 1024);
        unsafe { post_wsa_recv(socket, recv_data) };
    }
}

// ===========================================================================
// Per-IO bookkeeping.  OVERLAPPED is the first field (required by Win32).
// ===========================================================================

struct OverlappedData {
    overlapped: OVERLAPPED,
    socket: SOCKET,
    /// Buffer for the IO.  We hand the kernel a raw pointer into this Vec;
    /// ownership transfers to the completion callback.
    buf: Vec<u8>,
}

impl OverlappedData {
    fn new_recv(socket: SOCKET, capacity: usize) -> *mut Self {
        Box::into_raw(Box::new(Self {
            overlapped: unsafe { std::mem::zeroed() },
            socket,
            buf: vec![0u8; capacity],
        }))
    }
    fn new_send(socket: SOCKET, payload: &[u8]) -> *mut Self {
        Box::into_raw(Box::new(Self {
            overlapped: unsafe { std::mem::zeroed() },
            socket,
            buf: payload.to_vec(),
        }))
    }
}

// ===========================================================================
// Post helpers
// ===========================================================================

/// # Safety
/// `data` must be a valid pointer to a freshly-allocated `OverlappedData`
/// and the buffer inside must remain live until the IO completes (we
/// transfer ownership to the Box dropped in the worker).
unsafe fn post_wsa_recv(socket: SOCKET, data: *mut OverlappedData) {
    unsafe {
        let mut bytes_recv: u32 = 0;
        let mut flags: u32 = 0;
        let data_ref = &mut *data;
        let mut buf = windows_sys::Win32::Networking::WinSock::WSABUF {
            len: data_ref.buf.len() as u32,
            buf: data_ref.buf.as_mut_ptr(),
        };
        let rc = WSARecv(
            socket,
            &mut buf,
            1,
            &mut bytes_recv,
            &mut flags,
            &mut data_ref.overlapped,
            None,
        );
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() != Some(WSA_IO_PENDING) {
                // Anything else: real error, drop the slot.
                closesocket(socket);
                drop(Box::from_raw(data));
            }
        }
    }
}

/// # Safety
/// Same as `post_wsa_recv`.
unsafe fn post_wsa_send(socket: SOCKET, data: *mut OverlappedData) {
    unsafe {
        let mut bytes_sent: u32 = 0;
        let data_ref = &mut *data;
        let mut buf = windows_sys::Win32::Networking::WinSock::WSABUF {
            len: data_ref.buf.len() as u32,
            buf: data_ref.buf.as_mut_ptr(),
        };
        let rc = WSASend(
            socket,
            &mut buf,
            1,
            &mut bytes_sent,
            0,
            &mut data_ref.overlapped,
            None,
        );
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() != Some(WSA_IO_PENDING) {
                closesocket(socket);
                drop(Box::from_raw(data));
            }
        }
    }
}

// ===========================================================================
// WSAStartup helper
// ===========================================================================

fn wsa_startup() -> std::io::Result<()> {
    use std::sync::Once;
    static START: Once = Once::new();
    START.call_once(|| {
        unsafe {
            let mut data = std::mem::zeroed();
            let _ = windows_sys::Win32::Networking::WinSock::WSAStartup(
                0x0202,
                &mut data,
            );
        }
    });
    Ok(())
}
