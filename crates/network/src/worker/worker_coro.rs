//! ⭐ 协程 worker (2026-08): 每 worker 一个自研 Scheduler, 每连接一个协程.
//!
//! **架构**: 与 epoll worker (`worker_main_epoll`) 等价的事件驱动模型, 但:
//! - 事件源用 io_uring (而非 epoll): 连接协程 `select_read(socket, reply_eventfd)`
//!   同时等待"连接可读"与"shard 回包", 替代 epoll 的 conn readable + REPLY_TOKEN.
//! - 每连接一个协程, 协程持有自己的 `ConnState` (Rc<RefCell> 保留, 单线程 executor).
//! - 协议处理仍为同步 (`process_*_input` 纯解析→push→返回, 回包走事件驱动),
//!   因此只需把"读 socket"与"等 reply"替换为 io_uring await.
//!
//! **当前范围 (Phase 1b)**: 先支持 SQL 门面 (握手 + COM_QUERY), 验证协程 worker
//! 端到端 (含 shard 回包). 其他协议后续扩展 (架构相同).
//!
//! **可回退**: 与 `worker_main_epoll` 并存, 通过 server.rs 的 env 开关选择.

use super::*;
use crate::protocol::mysql as my;
use scheduler::io_ops as sio;
use std::sync::Arc;

/// 协程 worker 入口. 每 worker 1 个 Scheduler, 每连接 1 个协程.
pub(crate) fn worker_main_coro(cfg: WorkerConfig) {
    let shard_inboxes = cfg.shard_inboxes;
    let reply_bus = cfg.reply_bus;
    let worker_id = cfg.worker_id;
    let db: std::sync::Arc<str> = std::sync::Arc::from(cfg.default_db.as_str());
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
    // ⭐ shutdown: new_conn_loop 检测到 acceptor 断开时 stop scheduler.
    let stop_handle = sched.stop_handle();

    // ---- shutdown 状态: new_conn_loop 检测 acceptor 断开 → stop, 主循环退出 ----
    let stop: Arc<std::sync::atomic::AtomicBool> = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let active: Arc<std::sync::atomic::AtomicUsize> = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // ---- new_conn_loop: 监听 conn_eventfd, 收新连接并 spawn 连接协程 ----
    let sched2 = sched.clone();
    let inbox2 = inbox.clone();
    let shard_inboxes2 = shard_inboxes.clone();
    let reply_bus2 = reply_bus.clone();
    let db2 = db.clone();
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

    scheduler::spawn_on(&sched2, async move {
        let mut next_conn_id: u64 = 0;
        'outer: loop {
            // 等 conn_eventfd 可读 (acceptor 投递新连接). shutdown 时 server 会写它唤醒.
            match sio::poll(conn_eventfd, libc::POLLIN).await {
                Ok(_) => {}
                Err(_) => break,
            }
            // 消耗 eventfd 计数
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
                // spawn 连接协程
                active2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let active_c = active2.clone();
                let sched_conn = sched3.clone();
                let reply_efd = reply_eventfd;
                let reply_bus_c = reply_bus2.clone();
                let shard_inboxes_c = shard_inboxes2.clone();
                let db_view_c = db_view2.clone();
                let db_c = db2.clone();
                let sql_password_c = auth_password.clone();
                let tls_c = tls_config2.clone();
                scheduler::spawn_on(&sched_conn, async move {
                    conn_coro(
                        state,
                        id,
                        worker_id2,
                        nc.fd,
                        reply_efd,
                        reply_bus_c,
                        db_c,
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
    // new_conn_loop 常驻 poll conn_eventfd (registry 非空), 保证 drive 一直有 io 等待.
    // shutdown: new_conn_loop 检测到 acceptor 断开 (stop=true) 且所有连接协程结束 (active==0) 时退出.
    loop {
        sched.clone().drive_until_idle(1_000_000);
        if stop.load(std::sync::atomic::Ordering::Acquire)
            && active.load(std::sync::atomic::Ordering::Acquire) == 0
        {
            break;
        }
    }
}

/// 单连接协程: 循环等待 socket 可读 或 shard 回包, 处理之.
///
/// socket 可读 → `recv_async` (io_uring) 读入 read_buf → 同步协议处理 (push shard).
/// reply 可读 → drain reply_bus → 按 conn_id 匹配回包 → `handle_resp_shard_result` (同步).
#[allow(clippy::too_many_arguments)]
async fn conn_coro(
    mut conn: ConnState,
    conn_id: u64,
    worker_id: u32,
    fd: std::os::unix::io::RawFd,
    reply_eventfd: std::os::unix::io::RawFd,
    reply_bus: shard_manager::SharedTaskReplyBus,
    db: std::sync::Arc<str>,
    db_view: std::sync::Arc<shard_manager::DbDirView>,
    sql_password: Option<String>,
    shard_inboxes: Vec<SharedTaskInbox>,
    num_shards: usize,
    tls_config: Option<std::sync::Arc<rustls::ServerConfig>>,
) {
    loop {
        // 同时等 socket 可读 (1) 或 reply 到达 (2)
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
                        // ⭐ 协程 worker 当前先支持 SQL; 其他协议后续扩展 (架构相同).
                        _ => conn.resp_should_close(),
                    };
                    if should_close {
                        break;
                    }
                }
                Ok(false) => break, // EOF
                Err(_) => break,
            }
        } else {
            // reply 到达: drain reply_bus, 处理本 conn 的回包
            let results = reply_bus.drain();
            let mut close = false;
            for r in results {
                if r.conn_id != conn_id {
                    // 非本 conn 回包: 不应发生 (per-worker reply_bus, 多连接时可能串)
                    // 协程 worker 每个连接协程共享 reply_bus; 非本 conn 的回包需转发.
                    // ⭐ 当前简化: 多连接共享 reply_bus 时, 每个协程 drain 会抢回包.
                    // 这里只处理本 conn; 非本 conn 的回包本轮跳过 (由持有该 conn 的协程
                    // 下次 drain 处理). 由于 select_read 用共享 reply_eventfd, 可能轮询丢.
                    // ⭐ 限制 (Phase 1b): 单连接验证为主; 多连接并发需 per-conn reply 队列,
                    // 见 plan T3.
                    continue;
                }
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
                close = close || conn.resp_should_close();
            }
            if close {
                break;
            }
        }
    }
    // 连接协程结束: ConnState drop 会关闭其持有的 TcpStream (fd 所有权已转给 ConnState),
    // 不在此手动 close (避免 IO Safety violation 双重关闭).
}
