//! ⭐ 协程 worker (2026-08): 每 worker 一个自研 Scheduler, 每连接一个协程.
//!
//! **架构**: 与 epoll worker (`worker_main_epoll`) 等价的事件驱动模型, 但:
//! - 事件源用 io_uring (而非 epoll): 连接协程 `select_read(socket, per_conn_reply_eventfd)`
//!   同时等待"连接可读"与"本连接 shard 回包", 替代 epoll 的 conn readable + REPLY_TOKEN.
//! - **多连接 reply 路由**: `reply_dispatch` 协程统一读 reply_bus, 按 `conn_id` 把回包
//!   投递到每连接的私有队列并写其 per-conn eventfd 精确唤醒对应连接协程 —
//!   避免多个连接协程共享 reply_bus 的 drain 竞争 (会抢走/丢弃他人回包).
//! - 每连接一个协程, 协程持有自己的 `ConnState` (Rc<RefCell> 保留, 单线程 executor).
//! - 协议处理仍为同步 (`process_*_input` 纯解析→push→返回, 回包走事件驱动),
//!   因此只需把"读 socket"与"等 reply"替换为 io_uring await.
//!
//! **范围 (Phase 1b/1c)**: 支持全部 5 种协议门面 (SQL/RESP/PG/HTTP/Binary).
//! SQL 连接建立即发 HandshakeV10; RESP/PG 由客户端先发言; 回包按协议分发
//! (SQL/PG/HTTP 走 handle_resp_shard_result 聚合, RESP 走 process_resp_input,
//! Binary 直发 batch 结果).
//!
//! **可回退**: 与 `worker_main_epoll` 并存, 通过 server.rs 的 env 开关选择.

use std::collections::{HashMap, VecDeque};

use super::*;
use crate::protocol::mysql as my;
use scheduler::io_ops as sio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// 每连接的 reply 队列 + 精确唤醒 eventfd.
struct ConnReply {
    queue: VecDeque<shard_manager::TaskResult>,
    eventfd: std::os::unix::io::RawFd,
}

type ConnRegistry = Arc<Mutex<HashMap<u64, ConnReply>>>;

/// 协程 worker 入口. 每 worker 1 个 Scheduler, 每连接 1 个协程.
pub(crate) fn worker_main_coro(cfg: WorkerConfig) {
    let shard_inboxes = cfg.shard_inboxes;
    let reply_bus = cfg.reply_bus;
    let worker_id = cfg.worker_id;
    let db: std::sync::Arc<str> = std::sync::Arc::from(cfg.default_db.as_str());
    let table: std::sync::Arc<str> = std::sync::Arc::from(cfg.default_table.as_str());
    let limits = cfg.limits;
    let sql_cache: SharedSqlCache =
        std::rc::Rc::new(std::cell::RefCell::new(SqlWorkerCache::default()));
    let sql_shared = cfg.sql_shared;
    let db_view = cfg.db_view;
    let inbox = cfg.inbox;
    let conn_eventfd = cfg.conn_eventfd;
    let proto_kind = cfg.protocol;
    let auth_password = cfg.auth_password;
    let auth_required = auth_password.is_some();
    let tls_config = cfg.tls_config;
    let num_shards = shard_inboxes.len();

    let sched = scheduler::SchedHandle::new(scheduler::Scheduler::new());
    sched.set_current();
    let reply_eventfd = reply_bus.eventfd();
    let stop_handle = sched.stop_handle();

    // ---- shutdown 状态 ----
    let stop: Arc<std::sync::atomic::AtomicBool> =
        Arc::new(std::sync::atomic::AtomicBool::new(false));
    let active: Arc<std::sync::atomic::AtomicUsize> =
        Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // ---- per-conn reply 注册表 (reply_dispatch 写, 连接协程读) ----
    let registry: ConnRegistry = Arc::new(Mutex::new(HashMap::new()));
    // ⭐ shutdown: 停止 reply_dispatch 协程的信号 eventfd (主循环退出时写, 让协程 break).
    let stop_efd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    assert!(stop_efd >= 0, "stop_efd failed");

    // ============ reply_dispatch 协程: 读 reply_bus → 按 conn_id 路由 ============
    let reg_r = registry.clone();
    let reply_bus_r = reply_bus.clone();
    let sched_r = sched.clone();
    let stop_efd_r = stop_efd;
    scheduler::spawn_on(&sched_r, async move {
        loop {
            // 等 reply_bus 有新回包 或 stop 信号.
            let w = match sio::select_read(reply_eventfd, stop_efd_r).await {
                Ok(w) => w,
                Err(_) => break,
            };
            if w == 2 {
                break; // stop 信号 → 退出 reply_dispatch
            }
            // 消耗 reply eventfd 计数
            let mut v: u64 = 0;
            unsafe {
                libc::read(reply_eventfd, &mut v as *mut u64 as *mut libc::c_void, 8);
            }
            // drain 所有回包, 按 conn_id 路由到 per-conn 队列 + 写 per-conn eventfd
            let results = reply_bus_r.drain();
            if !results.is_empty() {
                let mut reg = reg_r.lock().unwrap();
                for r in results {
                    if let Some(entry) = reg.get_mut(&r.conn_id) {
                        entry.queue.push_back(r);
                        // 精确唤醒该连接协程 (level-triggered eventfd)
                        let val: u64 = 1;
                        unsafe {
                            libc::write(
                                entry.eventfd,
                                &val as *const u64 as *const libc::c_void,
                                8,
                            );
                        }
                    }
                    // conn 已移除 (已关闭) → 回包丢弃 (client 已断开)
                }
            }
        }
        // 结束: 不 close stop_efd (所有权归 worker_main_coro, 收尾时统一 close).
    });

    // ============ new_conn_loop 协程: 收新连接 → 建 per-conn 队列 + spawn 连接协程 ============
    let sched2 = sched.clone();
    let inbox2 = inbox.clone();
    let shard_inboxes2 = shard_inboxes.clone();
    let reply_bus2 = reply_bus.clone();
    let db2 = db.clone();
    let table2 = table.clone();
    let limits2 = limits;
    let sql_cache2 = sql_cache.clone();
    let sql_shared2 = sql_shared.clone();
    let db_view2 = db_view.clone();
    let auth_required2 = auth_required;
    let tls_config2 = tls_config.clone();
    let proto2 = proto_kind;
    let sched3 = sched.clone();
    let worker_id2 = worker_id;
    let stop2 = stop.clone();
    let active2 = active.clone();
    let registry2 = registry.clone();

    scheduler::spawn_on(&sched2, async move {
        let mut next_conn_id: u64 = 0;
        'outer: loop {
            // 等 conn_eventfd 可读 (acceptor 投递新连接). shutdown 时 server 会写它唤醒.
            if sio::poll(conn_eventfd, libc::POLLIN).await.is_err() {
                break;
            }
            let mut v: u64 = 0;
            unsafe {
                libc::read(conn_eventfd, &mut v as *mut u64 as *mut libc::c_void, 8);
            }
            // 收新连接; 检测 acceptor 断开 (sender drop → Disconnected) → stop
            let mut new_conns: Vec<NewConn> = Vec::new();
            loop {
                match inbox2.try_recv() {
                    Ok(nc) => new_conns.push(nc),
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        stop_handle.stop();
                        stop2.store(true, std::sync::atomic::Ordering::Release);
                        break 'outer;
                    }
                    Err(_) => break,
                }
            }
            for nc in new_conns {
                let id = next_conn_id;
                next_conn_id += 1;
                let mut state = ConnState::new(
                    nc.fd,
                    proto2,
                    auth_required2,
                    db2.clone(),
                    sql_cache2.clone(),
                    sql_shared2.clone(),
                    reply_bus2.clone(),
                    db_view2.clone(),
                    worker_id2,
                    num_shards,
                    shard_inboxes2.clone(),
                );
                // ⭐ Z2 (MySQL wire): Sql conn 建立即主动发 HandshakeV10
                if proto2 == ProtocolKind::Sql {
                    let salt = mysql_gen_salt(id, worker_id2);
                    state.mysql = Some(MysqlState { salt, phase: 0, pending_db: None });
                    let greeting = my::build_handshake_v10_caps(&salt, 1, false);
                    state.send_bytes(&greeting);
                }
                // 建 per-conn reply 队列 + eventfd
                let pcefd =
                    unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
                assert!(pcefd >= 0, "per-conn eventfd failed");
                registry2.lock().unwrap().insert(
                    id,
                    ConnReply {
                        queue: VecDeque::new(),
                        eventfd: pcefd,
                    },
                );
                // spawn 连接协程
                active2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let active_c = active2.clone();
                let reg_c = registry2.clone();
                let sched_conn = sched3.clone();
                let shard_inboxes_c = shard_inboxes2.clone();
                let db_view_c = db_view2.clone();
                let db_c = db2.clone();
                let table_c = table2.clone();
                let limits_c = limits2;
                let sql_password_c = auth_password.clone();
                let tls_c = tls_config2.clone();
                scheduler::spawn_on(&sched_conn, async move {
                    conn_coro(
                        state,
                        id,
                        worker_id2,
                        nc.fd,
                        pcefd,
                        reg_c,
                        db_c,
                        table_c,
                        limits_c,
                        db_view_c,
                        sql_password_c,
                        shard_inboxes_c,
                        num_shards,
                        tls_c,
                    )
                    .await;
                    active_c.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                });
            }
        }
    });

    // ---- 驱动: 阻塞式 (io_uring 等待连接/reply/socket 事件) ----
    // shutdown: new_conn_loop 检测到 acceptor 断开 (stop=true) 且所有连接协程结束 (active==0) 时退出.
    loop {
        // ⭐ 限制 drive_until_idle 迭代: 连接空闲时有常驻 poll 协程 (has_work 恒 true),
        // 若 max_iters 过大 drive 会忙循环到上限 (1e6 次 ≈ 14s). 用小上限 + sleep 让出,
        // 避免卡死; 事件不因 sleep 丢失 (CQE 在 io_uring queue, 下轮 drive 处理).
        sched.clone().drive_until_idle(2048);
        if stop.load(std::sync::atomic::Ordering::Acquire)
            && active.load(std::sync::atomic::Ordering::Acquire) == 0
        {
            break;
        }
        std::thread::sleep(Duration::from_micros(50));
    }

    // ---- 收尾 ----
    // 不在此 drive (worker 已停止调度). reply_dispatch 的 stop_efd 通知仅用于让其在
    // scheduler drop 前自然退出. 关闭 per-conn eventfd (残留).
    for (_, entry) in registry.lock().unwrap().drain() {
        unsafe {
            libc::close(entry.eventfd);
        }
    }
    // 关闭 stop_efd (reply_dispatch 协程结束时也会 close, 但 worker 也 move 了一份用于写).
    unsafe {
        libc::close(stop_efd);
    }
}

/// 单连接协程: 循环等待 socket 可读 或 本连接 reply 到达, 处理之.
///
/// socket 可读 → `recv_async` (io_uring) 读入 read_buf → 同步协议处理 (push shard).
/// reply 到达 (per-conn eventfd) → 消耗 eventfd → drain 自己队列 → `handle_resp_shard_result`.
#[allow(clippy::too_many_arguments)]
async fn conn_coro(
    mut conn: ConnState,
    conn_id: u64,
    worker_id: u32,
    fd: std::os::unix::io::RawFd,
    reply_eventfd: std::os::unix::io::RawFd,
    registry: ConnRegistry,
    db: std::sync::Arc<str>,
    table: std::sync::Arc<str>,
    limits: KvLimits,
    db_view: std::sync::Arc<shard_manager::DbDirView>,
    sql_password: Option<String>,
    shard_inboxes: Vec<SharedTaskInbox>,
    num_shards: usize,
    tls_config: Option<std::sync::Arc<rustls::ServerConfig>>,
) {
    loop {
        // 同时等 socket 可读 (1) 或 本连接 reply 到达 (2)
        let which = match sio::select_read(fd, reply_eventfd).await {
            Ok(w) => w,
            Err(_) => break, // fd 关闭/错误 → 结束
        };

        if which == 1 {
            // socket 可读: 读数据 → 协议处理
            match conn.recv_async().await {
                Ok(true) => {
                    let should_close = match conn.proto {
                        ProtocolKind::Sql => {
                            process_sql_input(
                                &mut conn,
                                conn_id,
                                worker_id,
                                &sql_password,
                                &db,
                                &db_view,
                                &shard_inboxes,
                                num_shards,
                                &tls_config,
                            );
                            conn.resp_should_close()
                        }
                        ProtocolKind::Pg => {
                            process_pg_input(
                                &mut conn,
                                conn_id,
                                worker_id,
                                &sql_password,
                                &db,
                                &db_view,
                                &shard_inboxes,
                                num_shards,
                                &tls_config,
                            );
                            conn.resp_should_close()
                        }
                        ProtocolKind::Resp => {
                            process_resp_input(
                                &mut conn,
                                conn_id,
                                worker_id,
                                &db_view,
                                &table,
                                &limits,
                                &sql_password,
                                &shard_inboxes,
                                num_shards,
                            );
                            conn.resp_should_close()
                        }
                        ProtocolKind::Http => {
                            process_http_input(
                                &mut conn,
                                conn_id,
                                worker_id,
                                &sql_password,
                                &db,
                                &db_view,
                                &limits,
                                num_shards,
                                &shard_inboxes,
                                num_shards,
                            );
                            conn.resp_should_close()
                        }
                        ProtocolKind::Binary => {
                            process_binary_input(
                                &mut conn,
                                conn_id,
                                worker_id,
                                &db,
                                &table,
                                &limits,
                                &shard_inboxes,
                                num_shards,
                            );
                            conn.resp_should_close()
                        }
                    };
                    if should_close {
                        break;
                    }
                }
                Ok(false) => break, // EOF
                Err(_) => break,
            }
        } else {
            // 本连接 reply 到达: 消耗 eventfd + drain 自己队列处理
            let mut v: u64 = 0;
            unsafe {
                libc::read(reply_eventfd, &mut v as *mut u64 as *mut libc::c_void, 8);
            }
            let mut close = false;
            loop {
                let r = {
                    let mut reg = registry.lock().unwrap();
                    reg.get_mut(&conn_id).and_then(|e| e.queue.pop_front())
                };
                let Some(r) = r else { break };
                if conn.proto == ProtocolKind::Binary {
                    // Binary 门面: 直发 batch 结果 (不聚合, 每 op 一回包).
                    let resp = batch_result_to_response(&r.result);
                    conn.send_binary_response(r.req_id, &resp);
                } else {
                    handle_resp_shard_result(
                        &mut conn,
                        conn_id,
                        r.req_id,
                        r.group,
                        &r.result,
                        worker_id,
                        &db,
                        &db_view,
                        &shard_inboxes,
                        num_shards,
                    );
                }
                close = close || conn.resp_should_close();
            }
            if close {
                break;
            }
        }
    }

    // 连接协程结束: 移除 registry 条目 + 关闭 per-conn eventfd.
    if let Some(entry) = registry.lock().unwrap().remove(&conn_id) {
        unsafe {
            libc::close(entry.eventfd);
        }
    }
    // ConnState drop 会关闭其持有的 TcpStream (fd 所有权已转给 ConnState), 不在此手动 close.
}
