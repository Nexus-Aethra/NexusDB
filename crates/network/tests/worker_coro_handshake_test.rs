//! ⭐ Phase 1 / T1.2: 协程 worker 最小闭环 — 用 io_uring 完成 MySQL 门面握手.
//!
//! 验证: 一个基于 scheduler 协程 + io_uring socket 收发的"worker", 能否服务
//! 真实 MySQL 客户端 (本测试用 sql_e2e 的 MyConn) 完成完整握手.
//!
//! 链路: 协程 worker io_uring 发 HandshakeV10 → io_uring 收 HandshakeResponse41
//! → 校验 native_password → io_uring 发 OK. 不依赖 shard (纯协议层).
//!
//! 这是协程 worker 可落地的第一个可验证里程碑; SQL 查询等 shard 交互留 Phase 2.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use network::protocol::mysql as my;

// ===== 与 sql_e2e 相同的最小 MySQL 客户端 (测试辅助) =====

#[derive(Debug, PartialEq)]
enum QueryResult {
    Ok { affected: u64 },
    Err { code: u16, msg: String },
    Rows(Vec<Vec<Option<String>>>),
}

struct MyConn {
    stream: TcpStream,
    buf: Vec<u8>,
}
impl MyConn {
    fn connect(addr: std::net::SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).expect("connect");
        stream.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        stream.set_nodelay(true).unwrap();
        Self { stream, buf: Vec::new() }
    }
    fn read_frame(&mut self) -> (u8, Vec<u8>) {
        loop {
            if let Some((seq, n, payload)) = my::read_packet(&self.buf) {
                self.buf.drain(..n);
                return (seq, payload);
            }
            let mut tmp = [0u8; 8192];
            let got = self.stream.read(&mut tmp).expect("read");
            assert!(got > 0, "connection closed");
            self.buf.extend_from_slice(&tmp[..got]);
        }
    }
    fn read_handshake(&mut self) -> [u8; 20] {
        let (seq, p) = self.read_frame();
        assert_eq!(seq, 0);
        assert_eq!(p[0], 10, "protocol version");
        let mut pos = 1;
        while p[pos] != 0 { pos += 1; }
        pos += 1 + 4;
        let mut salt = [0u8; 20];
        salt[..8].copy_from_slice(&p[pos..pos + 8]);
        pos += 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10;
        salt[8..20].copy_from_slice(&p[pos..pos + 12]);
        salt
    }
    fn login_native(&mut self, user: &str, token: &[u8]) -> (u8, Vec<u8>) {
        let flags = my::CLIENT_PROTOCOL_41 | my::CLIENT_SECURE_CONNECTION | my::CLIENT_PLUGIN_AUTH;
        let mut p = Vec::new();
        p.extend_from_slice(&flags.to_le_bytes());
        p.extend_from_slice(&0x0100_0000u32.to_le_bytes());
        p.push(45);
        p.extend_from_slice(&[0u8; 23]);
        p.extend_from_slice(user.as_bytes());
        p.push(0);
        p.push(token.len() as u8);
        p.extend_from_slice(token);
        p.extend_from_slice(b"mysql_native_password\0");
        self.stream.write_all(&my::write_packet(1, &p)).unwrap();
        self.read_frame()
    }
}

// ===== 协程 worker: 只做握手的最小实现 =====

/// 协程 worker 处理一个连接: 发 Handshake → 收握手响应 → 校验 auth → 发 OK.
/// 用 scheduler::io_ops (io_uring) 做 socket 收发.
fn coro_worker_handshake(fd: i32) {
    let sched = scheduler::SchedHandle::new(scheduler::Scheduler::new());
    sched.set_current();

    let salt: [u8; 20] = [
        b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'a', b'b', b'c', b'd', b'e',
        b'f', b'g', b'h', b'i', b'j',
    ];
    let done: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let done2 = done.clone();

    scheduler::spawn_on(&sched, async move {
        // 1. 发 HandshakeV10
        let greeting = my::build_handshake_v10_caps(&salt, 1, false);
        let mut w = 0usize;
        while w < greeting.len() {
            w += scheduler::io_ops::write(fd, &greeting[w..], u64::MAX).await.unwrap();
        }

        // 2. 收 HandshakeResponse41
        let mut buf = vec![0u8; 2048];
        let mut got = 0usize;
        // 循环读直到拿到完整包 (长度在包头 3 字节).
        let mut total = 0usize;
        loop {
            let n = scheduler::io_ops::read(fd, &mut buf[total..], u64::MAX).await.unwrap();
            if n == 0 { break; }
            total += n;
            if let Some((_, _n, payload)) = my::read_packet(&buf[..total]) {
                // 校验 auth: 空密码 + native_password
                if let Ok(login) = my::parse_handshake_response(&payload) {
                    let ok = my::native_password_ok(&salt, &login.auth_resp, "");
                    if ok {
                        let okpkt = my::build_ok(2, 0);
                        let mut wo = 0usize;
                        while wo < okpkt.len() {
                            wo += scheduler::io_ops::write(fd, &okpkt[wo..], u64::MAX).await.unwrap();
                        }
                        got = 1;
                    }
                }
                break;
            }
        }
        let _ = got;
        *done2.lock().unwrap() = true;
    });

    // 驱动直到完成.
    let mut iters = 0;
    while !*done.lock().unwrap() && iters < 5_000_000 {
        sched.clone().drive_until_idle(4096);
        iters += 1;
    }
    unsafe { libc::close(fd) };
}

#[test]
fn coro_worker_does_mysql_handshake() {
    // listener
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();

    // spawn 协程 worker 线程: accept 一个连接然后跑握手.
    let worker = std::thread::spawn(move || {
        // 等连接 (poll)
        let mut cfd: Option<i32> = None;
        for _ in 0..1000 {
            match listener.accept() {
                Ok((stream, _)) => {
                    let fd = stream.as_raw_fd();
                    stream.set_nonblocking(true).ok();
                    // 让 fd 不被 drop (用 ManuallyDrop 保活)
                    let _ = std::mem::ManuallyDrop::new(stream);
                    cfd = Some(fd);
                    break;
                }
                Err(_) => std::thread::sleep(Duration::from_millis(5)),
            }
        }
        coro_worker_handshake(cfd.expect("no client connected"));
    });

    // 客户端连上并做握手
    let mut c = MyConn::connect(addr);
    let salt = c.read_handshake();
    let token = my::native_password_token(&salt, "");
    let (_, resp) = c.login_native("root", &token);
    assert_eq!(resp[0], 0x00, "handshake should succeed: {resp:?}");
    drop(c);
    worker.join().unwrap();
}
