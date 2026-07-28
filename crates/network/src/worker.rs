//! Worker 线程池: epoll 事件循环驱动, 双协议门面 (Binary / RESP2).
//!
//! **架构**:
//! - 每个 worker 1 个线程, 1 个 epoll 事件循环
//! - epoll 监听: 所有 conn 的 readable + reply_bus eventfd
//! - conn readable: recv → parse → 校验 (KvLimits/AUTH) → route → push task_inbox[shard_id]
//! - reply eventfd: drain reply_bus → 按 conn_id 找连接 → encode → send
//!
//! **协议差异**:
//! - Binary: 帧内带 req_id, 回复乱序直发
//! - RESP: 无 req_id, per-conn 分配递增 seq 作为 req_id, 回复经重排缓冲严格 FIFO;
//!   本地命令 (PING/AUTH/超限 error) 也占 seq 进同一缓冲, 保证 pipeline 顺序
//!
//! **value 类型标签**: Put 时统一 `encode_value(TAG_RAW, ..)`, Get 回复时剥 tag.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::unix::io::{FromRawFd, RawFd};
use std::thread;

use crossbeam_channel::Receiver;
use shard_manager::{BatchOp, BatchResult, SharedTaskInbox, SharedTaskReplyBus, ShardTask};

use crate::acceptor::NewConn;
use crate::protocol::{
    BinaryProtocol, DecodeOutcome, KvLimits, Protocol, Request, RespCodec, RespCommand, Response,
    SetAlgOp, validate_kv, validate_request,
};
use crate::value_codec::{decode_value, render};

/// 特殊 epoll token: reply bus eventfd.
const REPLY_TOKEN: u64 = u64::MAX;
/// 特殊 epoll token: new conn inbox eventfd (如果有).
const NEW_CONN_TOKEN: u64 = u64::MAX - 1;

/// 连接使用的协议门面.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolKind {
    Binary,
    Resp,
}

pub struct WorkerConfig {
    pub worker_id: u32,
    pub inbox: Receiver<NewConn>,
    /// 新连接通知 eventfd (acceptor send 后写它, worker epoll 精确唤醒).
    pub conn_eventfd: RawFd,
    pub shard_inboxes: Vec<SharedTaskInbox>,
    pub reply_bus: SharedTaskReplyBus,
    pub default_db: String,
    pub default_table: String,
    /// 本 worker 所有连接使用的协议.
    pub protocol: ProtocolKind,
    /// KV 长度限制 (超限不进 shard, 直接回协议 error).
    pub limits: KvLimits,
    /// RESP AUTH 密码 (None = 不启用认证).
    pub auth_password: Option<String>,
}

pub struct WorkerPool {
    handles: Vec<thread::JoinHandle<()>>,
}

impl WorkerPool {
    pub fn start(configs: Vec<WorkerConfig>) -> std::io::Result<Self> {
        let mut handles = Vec::with_capacity(configs.len());
        for cfg in configs {
            let wid = cfg.worker_id;
            let join = thread::Builder::new()
                .name(format!("network-worker-{wid}"))
                .stack_size(4 * 1024 * 1024)
                .spawn(move || worker_main_epoll(cfg))
                .map_err(|e| std::io::Error::other(format!("spawn: {e}")))?;
            handles.push(join);
        }
        Ok(Self { handles })
    }

    pub fn join(self) -> std::io::Result<()> {
        for h in self.handles {
            h.join().map_err(|_| std::io::Error::other("worker panicked"))?;
        }
        Ok(())
    }
}

/// DEL 多 key 的聚合状态 (RESP :N 回复需等全部 Delete 完成).
struct DelAgg {
    remaining: usize,
    count: i64,
}

/// ⭐ MGET 跨 shard 聚合: 每 shard 一组, Values 按组内索引表回填原始槽.
struct MGetAgg {
    remaining: usize,
    /// 原始请求顺序的结果槽 (None = miss 或未回).
    slots: Vec<Option<Vec<u8>>>,
    /// group 号 → 该组 keys 的原始索引 (与 MultiGet keys 同序).
    groups: Vec<Vec<usize>>,
    /// 任一组失败: 记首个错误 (仍等全部组回齐再回复).
    error: Option<String>,
}

/// ⭐ MSET 跨 shard 聚合: 全部组 MultiPutOk → +OK.
struct MSetAgg {
    remaining: usize,
    error: Option<String>,
}

/// ⭐ EXISTS 多 key 聚合 (DEL 同构: 计数存在数).
struct ExistsAgg {
    remaining: usize,
    count: i64,
}

/// ⭐ MSETNX 跨 shard 聚合: 全部分片 MultiPutNx 返回 1 → :1, 否则 :0.
/// (跨 shard 非原子: 部分分片可能已写 — 已记为 gap.)
struct MSetNxAgg {
    remaining: usize,
    all_set: bool,
}

/// ⭐ 单 op Get 的回复语义转换 (STRLEN/TYPE/HEXISTS 复用 Get/HGet 任务).
#[derive(Clone, Copy)]
enum GetKind {
    Strlen,
    TypeOf,
    /// ⭐ Phase H: HEXISTS — GetValue(Some)→:1, None→:0
    HExists,
}

/// ⭐ Phase H: Pairs 结果渲染形态 (HGETALL/HKEYS/HVALS/HSCAN 复用同一 op).
#[derive(Clone, Copy)]
enum PairsKind {
    All,
    Keys,
    Vals,
    Scan,
    /// ⭐ C1: HRANDFIELD 无 count — 首 field 单 bulk / nil.
    OneKey,
}

/// ⭐ Phase Set: Members 结果渲染形态.
#[derive(Clone, Copy)]
enum MembersKind {
    /// SMEMBERS → *N
    List,
    /// SSCAN → ["0", *N]
    Scan,
    /// SPOP/SRANDMEMBER → bulk / nil (0/1 项)
    One,
}

/// ⭐ Phase Set: SINTER/SUNION/SDIFF 跨 shard 聚合 — 每 key 一个 SMembers
/// (group = key 序号), 全部回齐后 worker 端求交/并/差 (首 key 为基).
struct SetAlgAgg {
    remaining: usize,
    op: SetAlgOp,
    sets: Vec<Option<Vec<Vec<u8>>>>,
    error: Option<String>,
    /// ⭐ C1: SINTERCARD — 只回交集势 (Integer) 而非成员数组.
    card_only: bool,
    /// ⭐ C1: SINTERCARD LIMIT (0 = 无限制).
    limit: usize,
    /// ⭐ C3: *STORE — 结果写入 dst (先 DEL 再 SAdd), 回 :card.
    store_dst: Option<Vec<u8>>,
}

/// ⭐ C3: *STORE 第二阶段 (Delete dst + SAdd/ZAdd dst) 完成聚合.
/// 跨 shard 非原子 (源读与目标写分离) — 与 SINTER/MSETNX 同级 gap.
struct StoreFinishAgg {
    remaining: usize,
    card: i64,
    error: Option<String>,
}

/// ⭐ C3: ZINTERSTORE/ZUNIONSTORE 源聚合 — 每源 key 一个 ZRange(withscores),
/// 回齐后 SUM 聚合写 dst (无 weights/AGGREGATE, 计划内 defer).
type ScoredMembers = Vec<(Vec<u8>, f64)>;
struct ZStoreAgg {
    remaining: usize,
    inter: bool,
    sets: Vec<Option<ScoredMembers>>,
    error: Option<String>,
    dst: Vec<u8>,
}

/// ⭐ Phase G: Geo 命令的渲染上下文 (复用 ZMScore/ZRange 结果 + geohash 解码).
enum GeoCtx {
    /// GEOPOS → *N 个 [lon, lat] / nil
    Pos,
    /// GEODIST → bulk 距离 / nil
    Dist { factor: f64 },
    /// GEOSEARCH → 距离过滤 + 排序 + 可选 WITHCOORD/WITHDIST
    Search {
        lon: f64,
        lat: f64,
        radius_m: f64,
        asc: bool,
        count: usize,
        withcoord: bool,
        withdist: bool,
    },
}

/// ⭐ Phase B: Bitmap 读命令的渲染上下文 (Get 结果 + worker 位运算).
enum BitCtx {
    /// GETBIT offset → :0|:1
    GetBit { offset: u64 },
    /// BITCOUNT [start end] (BYTE, 含负索引) → :popcount
    Count { start: i64, end: i64 },
    /// BITPOS bit [start [end]] → :pos / :-1
    Pos { bit: bool, start: i64, end: Option<i64> },
}

/// 单个连接状态.
struct ConnState {
    fd: RawFd,
    stream: TcpStream,
    read_buf: Vec<u8>,
    proto: ProtocolKind,
    /// RESP: 是否已通过 AUTH (无密码配置时恒 true).
    authenticated: bool,
    /// RESP: 下一条命令分配的 seq (作为 ShardTask.req_id).
    next_seq: u64,
    /// RESP: 下一个应发送的 seq (FIFO 重排游标).
    next_to_send: u64,
    /// RESP: 已就绪但前面还有洞的回复字节.
    pending: BTreeMap<u64, Vec<u8>>,
    /// RESP: DEL 多 key 聚合 (seq → 状态).
    del_agg: HashMap<u64, DelAgg>,
    /// RESP: MGET 聚合 (seq → 状态).
    mget_agg: HashMap<u64, MGetAgg>,
    /// RESP: MSET 聚合 (seq → 状态).
    mset_agg: HashMap<u64, MSetAgg>,
    /// RESP: EXISTS 聚合 (seq → 状态).
    exists_agg: HashMap<u64, ExistsAgg>,
    /// RESP: STRLEN/TYPE 的 Get 语义转换 (seq → kind).
    get_kind: HashMap<u64, GetKind>,
    /// RESP: GETRANGE 的 (start, end) 参数 (seq → 参数; Get 后切片).
    getrange_ctx: HashMap<u64, (i64, i64)>,
    /// RESP: MSETNX 聚合 (seq → 状态).
    msetnx_agg: HashMap<u64, MSetNxAgg>,
    /// RESP: Pairs 结果渲染形态 (HGETALL/HKEYS/HVALS/HSCAN).
    pairs_kind: HashMap<u64, PairsKind>,
    /// RESP: HMSET 的 Integer 结果改回 +OK.
    hmset_ok: std::collections::HashSet<u64>,
    /// RESP: Members 结果渲染形态 (SMEMBERS/SSCAN/SPOP...).
    members_kind: HashMap<u64, MembersKind>,
    /// RESP: SINTER/SUNION/SDIFF 聚合 (seq → 状态).
    setalg_agg: HashMap<u64, SetAlgAgg>,
    /// ⭐ C1: ZMSCORE 的 Values 按裸 bulk 渲染 (score 串已成形, 不走 render tag).
    values_raw: std::collections::HashSet<u64>,
    /// ⭐ C3: *STORE 第二阶段聚合 (seq → 状态).
    store_agg: HashMap<u64, StoreFinishAgg>,
    /// ⭐ C3: ZINTERSTORE/ZUNIONSTORE 源聚合 (seq → 状态).
    zstore_agg: HashMap<u64, ZStoreAgg>,
    /// ⭐ Phase G: Geo 渲染上下文 (seq → 状态).
    geo_ctx: HashMap<u64, GeoCtx>,
    /// ⭐ Phase B: Bitmap 读渲染上下文 (seq → 状态).
    bit_ctx: HashMap<u64, BitCtx>,
    /// RESP: QUIT/协议错误后, 待 pending 清空即关连接.
    close_after_flush: bool,
}

impl ConnState {
    fn new(fd: RawFd, proto: ProtocolKind, auth_required: bool) -> Self {
        let stream = unsafe { TcpStream::from_raw_fd(fd) };
        stream.set_nonblocking(true).ok();
        // ⭐ 关闭 Nagle: 小回复立即发送, 避免与 delayed-ACK 交互导致 40ms 延迟
        stream.set_nodelay(true).ok();
        Self {
            fd,
            stream,
            read_buf: Vec::with_capacity(4096),
            proto,
            authenticated: !auth_required,
            next_seq: 0,
            next_to_send: 0,
            pending: BTreeMap::new(),
            del_agg: HashMap::new(),
            mget_agg: HashMap::new(),
            mset_agg: HashMap::new(),
            exists_agg: HashMap::new(),
            get_kind: HashMap::new(),
            getrange_ctx: HashMap::new(),
            msetnx_agg: HashMap::new(),
            pairs_kind: HashMap::new(),
            hmset_ok: std::collections::HashSet::new(),
            members_kind: HashMap::new(),
            setalg_agg: HashMap::new(),
            values_raw: std::collections::HashSet::new(),
            store_agg: HashMap::new(),
            zstore_agg: HashMap::new(),
            geo_ctx: HashMap::new(),
            bit_ctx: HashMap::new(),
            close_after_flush: false,
        }
    }

    /// 从连接 recv 数据, 追加到 read_buf.
    /// 返回 Ok(true) = 有数据, Ok(false) = 连接关闭, Err = 错误.
    fn recv(&mut self) -> std::io::Result<bool> {
        let mut tmp = [0u8; 4096];
        loop {
            match self.stream.read(&mut tmp) {
                Ok(0) => return Ok(false), // EOF
                Ok(n) => {
                    self.read_buf.extend_from_slice(&tmp[..n]);
                    if n < tmp.len() {
                        return Ok(true); // 读完了本次可用数据
                    }
                    // 可能还有更多, 继续 read
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(true),
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// 发送原始字节. non-blocking socket 遇 WouldBlock 时 spin retry
    /// (回复帧小, 正常情况下 send buffer 不会满太久).
    fn send_bytes(&mut self, bytes: &[u8]) {
        let mut written = 0usize;
        while written < bytes.len() {
            match self.stream.write(&bytes[written..]) {
                Ok(0) => break, // 对端关闭
                Ok(n) => written += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::yield_now();
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
    }

    /// Binary: 直发回复 (req_id 乱序语义, 无重排).
    fn send_binary_response(&mut self, req_id: u64, resp: &Response) {
        let bytes = BinaryProtocol::new().encode_response(req_id, resp);
        self.send_bytes(&bytes);
    }

    /// RESP: 回复字节进重排缓冲, 然后把从 next_to_send 起的连续段发出.
    fn resp_complete(&mut self, seq: u64, bytes: Vec<u8>) {
        self.pending.insert(seq, bytes);
        self.resp_flush_ready();
    }

    fn resp_flush_ready(&mut self) {
        let mut out: Vec<u8> = Vec::new();
        while let Some(bytes) = self.pending.remove(&self.next_to_send) {
            out.extend_from_slice(&bytes);
            self.next_to_send += 1;
        }
        if !out.is_empty() {
            self.send_bytes(&out);
        }
    }

    /// RESP: 是否可以关闭 (QUIT/协议错误 且回复已全部发出).
    fn resp_should_close(&self) -> bool {
        self.close_after_flush && self.pending.is_empty() && self.next_seq == self.next_to_send
    }
}

/// epoll 事件循环主函数.
fn worker_main_epoll(cfg: WorkerConfig) {
    let epoll_fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    assert!(epoll_fd >= 0, "epoll_create1 failed");

    let mut conn_map: HashMap<u64, ConnState> = HashMap::new();
    let mut next_conn_id: u64 = 0;

    // 注册 reply_bus eventfd + 新连接通知 eventfd
    epoll_add(epoll_fd, cfg.reply_bus.eventfd(), REPLY_TOKEN);
    epoll_add(epoll_fd, cfg.conn_eventfd, NEW_CONN_TOKEN);

    let shard_inboxes = cfg.shard_inboxes;
    let reply_bus = cfg.reply_bus;
    let worker_id = cfg.worker_id;
    // ⭐ 热路径优化: db/table 一次性转 Arc<str>, 每 op 仅引用计数 clone
    let db: std::sync::Arc<str> = std::sync::Arc::from(cfg.default_db.as_str());
    let table: std::sync::Arc<str> = std::sync::Arc::from(cfg.default_table.as_str());
    let inbox = cfg.inbox;
    let conn_eventfd = cfg.conn_eventfd;
    let proto_kind = cfg.protocol;
    let limits = cfg.limits;
    let auth_password = cfg.auth_password;
    let auth_required = auth_password.is_some();
    let num_shards = shard_inboxes.len();

    let mut events = vec![
        libc::epoll_event { events: 0, u64: 0 };
        256
    ];

    loop {
        // 检查新连接 (非阻塞; eventfd 另有精确唤醒, 这里是兑底)
        // ⭐ 退出条件: acceptor 侧 sender 已 drop (shutdown) 且无存活连接
        let mut inbox_disconnected = false;
        loop {
            match inbox.try_recv() {
                Ok(new_conn) => {
                    let id = next_conn_id;
                    next_conn_id += 1;
                    let state = ConnState::new(new_conn.fd, proto_kind, auth_required);
                    epoll_add(epoll_fd, state.fd, id);
                    conn_map.insert(id, state);
                    nlog::debug!("worker", "worker-{worker_id} conn {id} from {}", new_conn.peer);
                }
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    inbox_disconnected = true;
                    break;
                }
            }
        }
        if inbox_disconnected {
            // acceptor 侧 sender 已全部 drop = server 正在 shutdown.
            // 强制退出: 剩余连接随 conn_map drop 一并 close (TcpStream drop).
            break;
        }

        // 所有事件源 (conn readable / reply_bus / 新连接) 都有 eventfd/fd 精确唤醒,
        // 100ms timeout 仅兑底.
        let n = unsafe {
            libc::epoll_wait(epoll_fd, events.as_mut_ptr(), events.len() as i32, 100)
        };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            break;
        }

        for ev in events.iter().take(n as usize) {
            let token = ev.u64;

            if token == REPLY_TOKEN {
                // shard 有回复: drain bus, 按协议分发
                let results = reply_bus.drain();
                for r in results {
                    let mut close_conn = false;
                    if let Some(conn) = conn_map.get_mut(&r.conn_id) {
                        match conn.proto {
                            ProtocolKind::Binary => {
                                let resp = batch_result_to_response(&r.result);
                                conn.send_binary_response(r.req_id, &resp);
                            }
                            ProtocolKind::Resp => {
                                handle_resp_shard_result(
                                    conn,
                                    r.conn_id,
                                    r.req_id,
                                    r.group,
                                    &r.result,
                                    &db,
                                    &table,
                                    worker_id,
                                    &shard_inboxes,
                                    num_shards,
                                );
                                close_conn = conn.resp_should_close();
                            }
                        }
                    }
                    if close_conn {
                        remove_conn(epoll_fd, &mut conn_map, r.conn_id, worker_id);
                    }
                }
            } else if token == NEW_CONN_TOKEN {
                // 新连接通知: 消耗 eventfd 计数 (nonblocking), 连接在循环顶部 try_recv 接收
                let mut v: u64 = 0;
                unsafe {
                    libc::read(conn_eventfd, &mut v as *mut u64 as *mut libc::c_void, 8);
                }
                while let Ok(new_conn) = inbox.try_recv() {
                    let id = next_conn_id;
                    next_conn_id += 1;
                    let state = ConnState::new(new_conn.fd, proto_kind, auth_required);
                    epoll_add(epoll_fd, state.fd, id);
                    conn_map.insert(id, state);
                    nlog::debug!("worker", "worker-{worker_id} conn {id} from {}", new_conn.peer);
                }
            } else {
                // conn 可读: recv + parse + 校验 + route + push
                let conn_id = token;
                let mut should_remove = false;
                if let Some(conn) = conn_map.get_mut(&conn_id) {
                    match conn.recv() {
                        Ok(true) => match conn.proto {
                            ProtocolKind::Binary => {
                                process_binary_input(
                                    conn, conn_id, worker_id, &db, &table, &limits,
                                    &shard_inboxes, num_shards,
                                );
                            }
                            ProtocolKind::Resp => {
                                process_resp_input(
                                    conn, conn_id, worker_id, &db, &table, &limits,
                                    &auth_password, &shard_inboxes, num_shards,
                                );
                                should_remove = conn.resp_should_close();
                            }
                        },
                        Ok(false) => should_remove = true, // EOF
                        Err(_) => should_remove = true,
                    }
                }
                if should_remove {
                    remove_conn(epoll_fd, &mut conn_map, conn_id, worker_id);
                }
            }
        }
    }

    unsafe {
        libc::close(conn_eventfd);
        libc::close(epoll_fd);
    }
}

fn remove_conn(
    epoll_fd: RawFd,
    conn_map: &mut HashMap<u64, ConnState>,
    conn_id: u64,
    worker_id: u32,
) {
    if let Some(conn) = conn_map.remove(&conn_id) {
        epoll_del(epoll_fd, conn.fd);
        nlog::debug!("worker", "worker-{worker_id} conn {conn_id} closed");
    }
}

// ===== Binary 协议输入处理 =====

#[allow(clippy::too_many_arguments)]
fn process_binary_input(
    conn: &mut ConnState,
    conn_id: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    table: &std::sync::Arc<str>,
    limits: &KvLimits,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    let proto = BinaryProtocol::new();
    // ⭐ 热路径优化: 游标推进, 循环末一次 drain — 消 pipeline 下
    // 每帧 memmove 尾部字节的 O(n²).
    let mut cursor = 0usize;
    loop {
        match proto.decode_request(&conn.read_buf[cursor..]) {
            Ok(DecodeOutcome::Complete { consumed, value }) => {
                let req_id = peek_req_id(&conn.read_buf[cursor..cursor + consumed]);
                cursor += consumed;
                // ⭐ 长度校验: 超限不进 shard, 直接回 error 帧
                if let Err(msg) = validate_request(&value, limits) {
                    conn.send_binary_response(req_id, &Response::Error(msg));
                    continue;
                }
                let op = request_to_batch_op(value, db, table);
                let shard_id = hash_route_op(&op, num_shards);
                shard_inboxes[shard_id].push_spin(ShardTask {
                    conn_id,
                    req_id,
                    worker_id,
                    group: 0,
                    op,
                });
            }
            Ok(DecodeOutcome::NeedMore) => break,
            Err(_) => {
                if cursor < conn.read_buf.len() {
                    cursor += 1; // 重同步: 跳过 1 字节
                } else {
                    break;
                }
            }
        }
    }
    if cursor > 0 {
        conn.read_buf.drain(..cursor);
    }
}

// ===== RESP 协议输入处理 =====

#[allow(clippy::too_many_arguments)]
fn process_resp_input(
    conn: &mut ConnState,
    conn_id: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    table: &std::sync::Arc<str>,
    limits: &KvLimits,
    auth_password: &Option<String>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    let codec = RespCodec::new();
    // ⭐ 热路径优化: 游标推进, 循环末一次 drain (pipeline 下免每命令 memmove)
    let mut cursor = 0usize;
    loop {
        if conn.close_after_flush {
            // QUIT/协议错误后不再解析后续输入
            conn.read_buf.clear();
            cursor = 0;
            break;
        }
        match codec.decode_command(&conn.read_buf[cursor..]) {
            Ok(DecodeOutcome::Complete { consumed, value }) => {
                cursor += consumed;
                dispatch_resp_command(
                    conn, conn_id, worker_id, db, table, limits, auth_password,
                    shard_inboxes, num_shards, value,
                );
            }
            Ok(DecodeOutcome::NeedMore) => break,
            Err(msg) => {
                // RESP 流错位无法重新同步: 回 error 后关连接
                let seq = conn.next_seq;
                conn.next_seq += 1;
                let bytes = codec.encode_error(&msg);
                conn.resp_complete(seq, bytes);
                conn.close_after_flush = true;
                conn.read_buf.clear();
                cursor = 0;
                break;
            }
        }
    }
    if cursor > 0 {
        conn.read_buf.drain(..cursor);
    }
}

/// 分发单条 RESP 命令: 本地命令直接回 (占 seq 进重排缓冲), KV 命令进 shard.
#[allow(clippy::too_many_arguments)]
fn dispatch_resp_command(
    conn: &mut ConnState,
    conn_id: u64,
    worker_id: u32,
    db: &std::sync::Arc<str>,
    table: &std::sync::Arc<str>,
    limits: &KvLimits,
    auth_password: &Option<String>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    cmd: RespCommand,
) {
    let codec = RespCodec::new();
    let seq = conn.next_seq;
    conn.next_seq += 1;

    // AUTH 门禁: 未认证时只放行 AUTH/HELLO/QUIT
    if !conn.authenticated
        && !matches!(
            cmd,
            RespCommand::Auth { .. } | RespCommand::Hello(_) | RespCommand::Quit
        )
    {
        conn.resp_complete(seq, codec.encode_error("NOAUTH Authentication required."));
        return;
    }

    match cmd {
        RespCommand::Set { key, value } => {
            // ⭐ value 已是 [TAG_RAW][payload] 布局 (decode 时预置),
            // 校验扣 1B tag; 直构 BatchOp 免 Request 中转/二次拷贝.
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::Put {
                db: db.clone(),
                table: table.clone(),
                key,
                val: value,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::Get { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::Get {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::Del { keys } => {
            // 逐 key 校验 (借用版, 免 clone); 任一超限整条命令拒绝 (不部分执行)
            for key in &keys {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // 多 key 拆多个 Delete task 共用同一 seq, 聚合计数后回 :N
            conn.del_agg.insert(
                seq,
                DelAgg {
                    remaining: keys.len(),
                    count: 0,
                },
            );
            for key in keys {
                let op = BatchOp::Delete {
                    db: db.clone(),
                    table: table.clone(),
                    key,
                };
                push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
            }
        }
        RespCommand::MGet { keys } => {
            for key in &keys {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // ⭐ 按 shard 分组: 每 shard 一个 MultiGet (shard 内区间复用),
            // group 号回传后按索引表回填原始槽
            let n = keys.len();
            let mut by_shard: Vec<(usize, Vec<Vec<u8>>, Vec<usize>)> = Vec::new();
            for (i, key) in keys.into_iter().enumerate() {
                let sid = hash_route_key(db.as_ref(), table.as_ref(), &key, num_shards);
                match by_shard.iter_mut().find(|(s, _, _)| *s == sid) {
                    Some((_, ks, idxs)) => {
                        ks.push(key);
                        idxs.push(i);
                    }
                    None => by_shard.push((sid, vec![key], vec![i])),
                }
            }
            let groups: Vec<Vec<usize>> = by_shard.iter().map(|(_, _, idxs)| idxs.clone()).collect();
            conn.mget_agg.insert(
                seq,
                MGetAgg {
                    remaining: by_shard.len(),
                    slots: vec![None; n],
                    groups,
                    error: None,
                },
            );
            for (gidx, (sid, ks, _)) in by_shard.into_iter().enumerate() {
                let op = BatchOp::MultiGet {
                    db: db.clone(),
                    table: table.clone(),
                    keys: ks,
                };
                push_task_grouped(conn_id, seq, worker_id, gidx as u32, sid, op, shard_inboxes);
            }
        }
        RespCommand::MSet { pairs } => {
            // value 已带 1B tag, 校验扣除
            for (key, value) in &pairs {
                if let Err(msg) = validate_kv(key, value.len().saturating_sub(1), limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            type ShardPairs = (usize, Vec<(Vec<u8>, Vec<u8>)>);
            let mut by_shard: Vec<ShardPairs> = Vec::new();
            for (key, value) in pairs {
                let sid = hash_route_key(db.as_ref(), table.as_ref(), &key, num_shards);
                match by_shard.iter_mut().find(|(s, _)| *s == sid) {
                    Some((_, ps)) => ps.push((key, value)),
                    None => by_shard.push((sid, vec![(key, value)])),
                }
            }
            conn.mset_agg.insert(
                seq,
                MSetAgg {
                    remaining: by_shard.len(),
                    error: None,
                },
            );
            for (gidx, (sid, ps)) in by_shard.into_iter().enumerate() {
                let op = BatchOp::MultiPut {
                    db: db.clone(),
                    table: table.clone(),
                    pairs: ps,
                };
                push_task_grouped(conn_id, seq, worker_id, gidx as u32, sid, op, shard_inboxes);
            }
        }
        RespCommand::Ping(msg) => {
            let bytes = match msg {
                None => codec.encode_simple("PONG"),
                Some(m) => codec.encode_bulk(&m),
            };
            conn.resp_complete(seq, bytes);
        }
        RespCommand::Incr { key, delta } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::Incr {
                db: db.clone(),
                table: table.clone(),
                key,
                delta,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::IncrFloat { key, delta } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::IncrFloat {
                db: db.clone(),
                table: table.clone(),
                key,
                delta,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::Append { key, suffix } => {
            // suffix 不带 tag (RMW 端拼接); 校验按追加段长度上限保守拦截
            if let Err(msg) = validate_kv(&key, suffix.len(), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::Append {
                db: db.clone(),
                table: table.clone(),
                key,
                suffix,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SetNx { key, value } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SetNx {
                db: db.clone(),
                table: table.clone(),
                key,
                val: value,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::Exists { keys } => {
            for key in &keys {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // N 个 Get 共用 seq, 聚合计数 (Redis EXISTS: 重复 key 重复计)
            conn.exists_agg.insert(
                seq,
                ExistsAgg {
                    remaining: keys.len(),
                    count: 0,
                },
            );
            for key in keys {
                let op = BatchOp::Get {
                    db: db.clone(),
                    table: table.clone(),
                    key,
                };
                push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
            }
        }
        RespCommand::Strlen { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.get_kind.insert(seq, GetKind::Strlen);
            let op = BatchOp::Get {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::TypeOf { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.get_kind.insert(seq, GetKind::TypeOf);
            let op = BatchOp::Get {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GetDel { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::GetDel {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GetSet { key, value } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::GetSet {
                db: db.clone(),
                table: table.clone(),
                key,
                val: value,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SetRange { key, offset, data } => {
            // 新长度 = offset + data.len(), 保守校验不超 value 上限
            if let Err(msg) = validate_kv(&key, offset as usize + data.len(), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SetRange {
                db: db.clone(),
                table: table.clone(),
                key,
                offset,
                data,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GetRange { key, start, end } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // 复用 Get; 结果到达时按 (start,end) 切片 (getrange_ctx)
            conn.getrange_ctx.insert(seq, (start, end));
            let op = BatchOp::Get {
                db: db.clone(),
                table: table.clone(),
                key,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::MSetNx { pairs } => {
            for (key, value) in &pairs {
                if let Err(msg) = validate_kv(key, value.len().saturating_sub(1), limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // 按 shard 分组, 每 shard 一个 MultiPutNx; 全部写入 → :1, 否则 :0
            type ShardPairs = (usize, Vec<(Vec<u8>, Vec<u8>)>);
            let mut by_shard: Vec<ShardPairs> = Vec::new();
            for (key, value) in pairs {
                let sid = hash_route_key(db.as_ref(), table.as_ref(), &key, num_shards);
                match by_shard.iter_mut().find(|(s, _)| *s == sid) {
                    Some((_, ps)) => ps.push((key, value)),
                    None => by_shard.push((sid, vec![(key, value)])),
                }
            }
            conn.msetnx_agg.insert(
                seq,
                MSetNxAgg {
                    remaining: by_shard.len(),
                    all_set: true,
                },
            );
            for (gidx, (sid, ps)) in by_shard.into_iter().enumerate() {
                let op = BatchOp::MultiPutNx {
                    db: db.clone(),
                    table: table.clone(),
                    pairs: ps,
                };
                push_task_grouped(conn_id, seq, worker_id, gidx as u32, sid, op, shard_inboxes);
            }
        }
        // ---- ⭐ Phase H: Hash (单 key 单 shard, 直推 push_task) ----
        RespCommand::HSet { key, pairs, reply_ok } => {
            for (f, v) in &pairs {
                if let Err(msg) = validate_kv(&key, 0, limits)
                    .and_then(|_| validate_kv(f, v.len().saturating_sub(1), limits))
                {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            if reply_ok {
                conn.hmset_ok.insert(seq); // HMSET 回 +OK (Integer 转换)
            }
            let op = BatchOp::HSet { db: db.clone(), table: table.clone(), key, pairs };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HSetNx { key, field, value } => {
            if let Err(msg) = validate_kv(&key, 0, limits)
                .and_then(|_| validate_kv(&field, value.len().saturating_sub(1), limits))
            {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::HSetNx {
                db: db.clone(),
                table: table.clone(),
                key,
                field,
                val: value,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HGet { key, field } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::HGet { db: db.clone(), table: table.clone(), key, field };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HMGet { key, fields } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::HMGet { db: db.clone(), table: table.clone(), key, fields };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HDel { key, fields } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::HDel { db: db.clone(), table: table.clone(), key, fields };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HExists { key, field } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.get_kind.insert(seq, GetKind::HExists);
            let op = BatchOp::HGet { db: db.clone(), table: table.clone(), key, field };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HLen { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::HLen { db: db.clone(), table: table.clone(), key };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HGetAll { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.pairs_kind.insert(seq, PairsKind::All);
            let op = BatchOp::HGetAll { db: db.clone(), table: table.clone(), key };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HKeys { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.pairs_kind.insert(seq, PairsKind::Keys);
            let op = BatchOp::HGetAll { db: db.clone(), table: table.clone(), key };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HVals { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.pairs_kind.insert(seq, PairsKind::Vals);
            let op = BatchOp::HGetAll { db: db.clone(), table: table.clone(), key };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HScan { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.pairs_kind.insert(seq, PairsKind::Scan);
            let op = BatchOp::HGetAll { db: db.clone(), table: table.clone(), key };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HIncrBy { key, field, delta } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::HIncrBy {
                db: db.clone(),
                table: table.clone(),
                key,
                field,
                delta,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HIncrByFloat { key, field, delta } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::HIncrByFloat {
                db: db.clone(),
                table: table.clone(),
                key,
                field,
                delta,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ Phase Set: Set (单 key 直推; 代数类跨 shard 聚合) ----
        RespCommand::SAdd { key, members } => {
            for m in &members {
                if let Err(msg) =
                    validate_kv(&key, 0, limits).and_then(|_| validate_kv(m, 0, limits))
                {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            let op = BatchOp::SAdd { db: db.clone(), table: table.clone(), key, members };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SRem { key, members } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SRem { db: db.clone(), table: table.clone(), key, members };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SIsMember { key, member } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SIsMember { db: db.clone(), table: table.clone(), key, member };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SCard { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SCard { db: db.clone(), table: table.clone(), key };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SMembers { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::List);
            let op = BatchOp::SMembers { db: db.clone(), table: table.clone(), key };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SScan { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::Scan);
            let op = BatchOp::SMembers { db: db.clone(), table: table.clone(), key };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SPop { key, count } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // count 缺省 → 单 bulk (One); 显式 count → 数组 (List)
            match count {
                None => {
                    conn.members_kind.insert(seq, MembersKind::One);
                    let op = BatchOp::SPop { db: db.clone(), table: table.clone(), key };
                    push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
                Some(c) => {
                    conn.members_kind.insert(seq, MembersKind::List);
                    let op = BatchOp::SPopN { db: db.clone(), table: table.clone(), key, count: c };
                    push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
            }
        }
        RespCommand::SRandMember { key, count } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            match count {
                None => {
                    conn.members_kind.insert(seq, MembersKind::One);
                    let op = BatchOp::SRandMember { db: db.clone(), table: table.clone(), key };
                    push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
                Some(c) => {
                    conn.members_kind.insert(seq, MembersKind::List);
                    let op = BatchOp::SRandCount { db: db.clone(), table: table.clone(), key, count: c };
                    push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
                }
            }
        }
        RespCommand::SMisMember { key, members } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::SMisMember { db: db.clone(), table: table.clone(), key, members };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::SInterCard { keys, limit } => {
            for key in &keys {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // 复用 SetAlg 聚合 (Inter), 完成点回 :card 而非数组
            let n = keys.len();
            conn.setalg_agg.insert(
                seq,
                SetAlgAgg {
                    remaining: n,
                    op: SetAlgOp::Inter,
                    sets: vec![None; n],
                    error: None,
                    card_only: true,
                    limit,
                    store_dst: None,
                },
            );
            for (i, key) in keys.into_iter().enumerate() {
                let sid = hash_route_key(db.as_ref(), table.as_ref(), &key, num_shards);
                let smem = BatchOp::SMembers { db: db.clone(), table: table.clone(), key };
                push_task_grouped(conn_id, seq, worker_id, i as u32, sid, smem, shard_inboxes);
            }
        }
        RespCommand::SetAlg { op, keys } => {
            for key in &keys {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            // 每 key 一个 SMembers (group = key 序号), 全部回齐后求交/并/差
            let n = keys.len();
            conn.setalg_agg.insert(
                seq,
                SetAlgAgg {
                    remaining: n,
                    op,
                    sets: vec![None; n],
                    error: None,
                    card_only: false,
                    limit: 0,
                    store_dst: None,
                },
            );
            for (i, key) in keys.into_iter().enumerate() {
                let sid = hash_route_key(db.as_ref(), table.as_ref(), &key, num_shards);
                let smem = BatchOp::SMembers { db: db.clone(), table: table.clone(), key };
                push_task_grouped(conn_id, seq, worker_id, i as u32, sid, smem, shard_inboxes);
            }
        }
        // ---- ⭐ C3: *STORE (源读聚合 + dst 写; 跨 shard 非原子, 记 gap) ----
        RespCommand::SetAlgStore { op, dst, keys } => {
            for key in keys.iter().chain(std::iter::once(&dst)) {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            let n = keys.len();
            conn.setalg_agg.insert(
                seq,
                SetAlgAgg {
                    remaining: n,
                    op,
                    sets: vec![None; n],
                    error: None,
                    card_only: false,
                    limit: 0,
                    store_dst: Some(dst),
                },
            );
            for (i, key) in keys.into_iter().enumerate() {
                let sid = hash_route_key(db.as_ref(), table.as_ref(), &key, num_shards);
                let smem = BatchOp::SMembers { db: db.clone(), table: table.clone(), key };
                push_task_grouped(conn_id, seq, worker_id, i as u32, sid, smem, shard_inboxes);
            }
        }
        RespCommand::ZSetStore { inter, dst, keys } => {
            for key in keys.iter().chain(std::iter::once(&dst)) {
                if let Err(msg) = validate_kv(key, 0, limits) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            let n = keys.len();
            conn.zstore_agg.insert(
                seq,
                ZStoreAgg {
                    remaining: n,
                    inter,
                    sets: vec![None; n],
                    error: None,
                    dst,
                },
            );
            // 每源 key 取全量 (member, score) — 复用 ZRange withscores 交替串
            for (i, key) in keys.into_iter().enumerate() {
                let sid = hash_route_key(db.as_ref(), table.as_ref(), &key, num_shards);
                let zr = BatchOp::ZRange {
                    db: db.clone(),
                    table: table.clone(),
                    key,
                    start: 0,
                    end: -1,
                    rev: false,
                    withscores: true,
                };
                push_task_grouped(conn_id, seq, worker_id, i as u32, sid, zr, shard_inboxes);
            }
        }
        // ---- ⭐ Phase L: List (单 key 直推) ----
        RespCommand::LPush { key, values, left } => {
            for v in &values {
                if let Err(msg) =
                    validate_kv(&key, v.len().saturating_sub(1), limits)
                {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            let op = BatchOp::LPush { db: db.clone(), table: table.clone(), key, values, left };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LPop { key, left, count } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // count 缺省 → 单 bulk (One); 显式 count → 数组 (List)
            conn.members_kind.insert(
                seq,
                if count.is_none() { MembersKind::One } else { MembersKind::List },
            );
            let op = BatchOp::LPop {
                db: db.clone(),
                table: table.clone(),
                key,
                left,
                count: count.unwrap_or(1),
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LLen { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::LLen { db: db.clone(), table: table.clone(), key };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LRange { key, start, end } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::List);
            let op = BatchOp::LRange { db: db.clone(), table: table.clone(), key, start, end };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LIndex { key, idx } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::LIndex { db: db.clone(), table: table.clone(), key, idx };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LSet { key, idx, value } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.hmset_ok.insert(seq); // Integer(1) → +OK
            let op = BatchOp::LSet { db: db.clone(), table: table.clone(), key, idx, val: value };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ C2: List 中段操作 ----
        RespCommand::LRem { key, count, value } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::LRem { db: db.clone(), table: table.clone(), key, count, val: value };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LTrim { key, start, stop } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.hmset_ok.insert(seq); // Integer(1) → +OK
            let op = BatchOp::LTrim { db: db.clone(), table: table.clone(), key, start, stop };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LPos { key, value, rank, count } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::LPos { db: db.clone(), table: table.clone(), key, val: value, rank, count };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::LInsert { key, before, pivot, value } => {
            if let Err(msg) = validate_kv(&key, value.len().saturating_sub(1), limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::LInsert {
                db: db.clone(),
                table: table.clone(),
                key,
                before,
                pivot,
                val: value,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ Phase Z: ZSet (单 key 直推) ----
        RespCommand::ZAdd { key, pairs } => {
            for (_, m) in &pairs {
                if let Err(msg) = validate_kv(&key, 0, limits).and_then(|_| validate_kv(m, 0, limits)) {
                    conn.resp_complete(seq, codec.encode_error(&msg));
                    return;
                }
            }
            let op = BatchOp::ZAdd { db: db.clone(), table: table.clone(), key, pairs };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZRem { key, members } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZRem { db: db.clone(), table: table.clone(), key, members };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZScore { key, member } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZScore { db: db.clone(), table: table.clone(), key, member };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZCard { key } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZCard { db: db.clone(), table: table.clone(), key };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZIncrBy { key, delta, member } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZIncrBy { db: db.clone(), table: table.clone(), key, delta, member };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZRange { key, start, end, rev, withscores } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::List);
            let op = BatchOp::ZRange { db: db.clone(), table: table.clone(), key, start, end, rev, withscores };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZRangeByScore { key, min, max, withscores } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::List);
            let op = BatchOp::ZRangeByScore { db: db.clone(), table: table.clone(), key, min, max, withscores };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZRank { key, member, rev } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZRank { db: db.clone(), table: table.clone(), key, member, rev };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ C1: ZSet/Hash 命令空洞 ----
        RespCommand::ZCount { key, min, max } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let op = BatchOp::ZCount { db: db.clone(), table: table.clone(), key, min, max };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZMScore { key, members } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // Values 已是成形 score 串, 按裸 bulk 渲染 (不走 render tag)
            conn.values_raw.insert(seq);
            let op = BatchOp::ZMScore { db: db.clone(), table: table.clone(), key, members };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::ZPop { key, rev, count } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.members_kind.insert(seq, MembersKind::List);
            let op = BatchOp::ZPop { db: db.clone(), table: table.clone(), key, rev, count };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HStrlen { key, field } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // 复用 HGet + Strlen 语义转换 (miss → :0)
            conn.get_kind.insert(seq, GetKind::Strlen);
            let op = BatchOp::HGet { db: db.clone(), table: table.clone(), key, field };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::HRandField { key, count, withvalues } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            let kind = match (count, withvalues) {
                (None, _) => PairsKind::OneKey,
                (Some(_), true) => PairsKind::All,
                (Some(_), false) => PairsKind::Keys,
            };
            conn.pairs_kind.insert(seq, kind);
            let op = BatchOp::HRandField {
                db: db.clone(),
                table: table.clone(),
                key,
                count: count.unwrap_or(1),
                withvalues,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ Phase G: Geo (复用 ZSet 链路 + 渲染钩子) ----
        RespCommand::GeoPos { key, members } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.geo_ctx.insert(seq, GeoCtx::Pos);
            let op = BatchOp::ZMScore { db: db.clone(), table: table.clone(), key, members };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GeoDist { key, m1, m2, factor } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.geo_ctx.insert(seq, GeoCtx::Dist { factor });
            let op = BatchOp::ZMScore {
                db: db.clone(),
                table: table.clone(),
                key,
                members: vec![m1, m2],
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GeoSearch { key, lon, lat, radius_m, asc, count, withcoord, withdist } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.geo_ctx.insert(
                seq,
                GeoCtx::Search { lon, lat, radius_m, asc, count, withcoord, withdist },
            );
            // 全量 (member, score) — worker 端 geohash 解码 + 距离过滤
            let op = BatchOp::ZRange {
                db: db.clone(),
                table: table.clone(),
                key,
                start: 0,
                end: -1,
                rev: false,
                withscores: true,
            };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        // ---- ⭐ Phase B: Bitmap (String 字节) ----
        RespCommand::SetBit { key, offset, bit } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            // 位偏移上限: 落地字节 ≤ max_value_bytes (溢出页上限内)
            if (offset / 8) as usize + 1 > limits.max_value_bytes {
                conn.resp_complete(
                    seq,
                    codec.encode_error("bit offset is not an integer or out of range"),
                );
                return;
            }
            let op = BatchOp::SetBit { db: db.clone(), table: table.clone(), key, offset, bit };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::GetBit { key, offset } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.bit_ctx.insert(seq, BitCtx::GetBit { offset });
            let op = BatchOp::Get { db: db.clone(), table: table.clone(), key };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::BitCount { key, start, end } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.bit_ctx.insert(seq, BitCtx::Count { start, end });
            let op = BatchOp::Get { db: db.clone(), table: table.clone(), key };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::BitPos { key, bit, start, end } => {
            if let Err(msg) = validate_kv(&key, 0, limits) {
                conn.resp_complete(seq, codec.encode_error(&msg));
                return;
            }
            conn.bit_ctx.insert(seq, BitCtx::Pos { bit, start, end });
            let op = BatchOp::Get { db: db.clone(), table: table.clone(), key };
            push_task(conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        RespCommand::InvalidInt(_) => {
            conn.resp_complete(
                seq,
                codec.encode_error("value is not an integer or out of range"),
            );
        }
        RespCommand::InvalidFloat(_) => {
            conn.resp_complete(seq, codec.encode_error("value is not a valid float"));
        }
        RespCommand::Echo(m) => {
            conn.resp_complete(seq, codec.encode_bulk(&m));
        }
        RespCommand::Auth { user, pass } => {
            let bytes = match auth_password {
                None => codec.encode_error("ERR Client sent AUTH, but no password is set."),
                Some(expected) => {
                    let user_ok = match &user {
                        None => true,
                        Some(u) => u.as_slice() == b"default",
                    };
                    if user_ok && pass.as_slice() == expected.as_bytes() {
                        conn.authenticated = true;
                        codec.encode_ok()
                    } else {
                        codec.encode_error(
                            "WRONGPASS invalid username-password pair or user is disabled.",
                        )
                    }
                }
            };
            conn.resp_complete(seq, bytes);
        }
        RespCommand::Quit => {
            conn.resp_complete(seq, codec.encode_ok());
            conn.close_after_flush = true;
        }
        RespCommand::Command => {
            conn.resp_complete(seq, codec.encode_empty_array());
        }
        RespCommand::Hello(proto) => {
            let is_v2 = match &proto {
                None => true,
                Some(p) => p.as_slice() == b"2",
            };
            let bytes = if is_v2 {
                // 最小 HELLO 回复: 扁平 key-value 数组 (RESP2 无 map 类型)
                let mut out = Vec::new();
                out.extend_from_slice(b"*6\r\n");
                out.extend_from_slice(&codec.encode_bulk(b"server"));
                out.extend_from_slice(&codec.encode_bulk(b"nexusdb"));
                out.extend_from_slice(&codec.encode_bulk(b"version"));
                out.extend_from_slice(&codec.encode_bulk(b"0.1.0"));
                out.extend_from_slice(&codec.encode_bulk(b"proto"));
                out.extend_from_slice(&codec.encode_integer(2));
                out
            } else {
                codec.encode_error(
                    "NOPROTO unsupported protocol version",
                )
            };
            conn.resp_complete(seq, bytes);
        }
        RespCommand::Select => {
            // 单 db 语义: 接受并忽略
            conn.resp_complete(seq, codec.encode_ok());
        }
        RespCommand::Unknown(name) => {
            conn.resp_complete(seq, codec.encode_error(&format!("unknown command '{name}'")));
        }
        RespCommand::WrongArity(name) => {
            conn.resp_complete(
                seq,
                codec.encode_error(&format!("wrong number of arguments for '{name}' command")),
            );
        }
    }
}

fn push_task(
    conn_id: u64,
    req_id: u64,
    worker_id: u32,
    op: BatchOp,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    let shard_id = hash_route_op(&op, num_shards);
    shard_inboxes[shard_id].push_spin(ShardTask {
        conn_id,
        req_id,
        worker_id,
        group: 0,
        op,
    });
}

/// ⭐ MGET/MSET: 定向 push 到指定 shard, 带组号 (聚合回填用).
fn push_task_grouped(
    conn_id: u64,
    req_id: u64,
    worker_id: u32,
    group: u32,
    shard_id: usize,
    op: BatchOp,
    shard_inboxes: &[SharedTaskInbox],
) {
    shard_inboxes[shard_id].push_spin(ShardTask {
        conn_id,
        req_id,
        worker_id,
        group,
        op,
    });
}

/// key 级路由 (与 hash_route_op 同 hash 逻辑, 分组场景用).
fn hash_route_key(db: &str, table: &str, key: &[u8], num_shards: usize) -> usize {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    db.hash(&mut h);
    table.hash(&mut h);
    key.hash(&mut h);
    (h.finish() as usize) % num_shards
}

/// ⭐ GETRANGE 切片 (Redis 语义): 负索引从尾算, end inclusive, 越界 clamp.
fn getrange_slice(data: &[u8], start: i64, end: i64) -> &[u8] {
    let len = data.len() as i64;
    if len == 0 {
        return &[];
    }
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if end < 0 { len + end } else { end };
    if s < 0 {
        s = 0;
    }
    if e < 0 {
        e = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e {
        return &[];
    }
    &data[s as usize..=e as usize]
}

/// RESP: 处理 shard 回来的单条结果 (含 DEL/MGET/MSET 聚合).
/// ⭐ C3: *STORE 需在聚合完成点发第二阶段任务 → 携带路由上下文.
#[allow(clippy::too_many_arguments)]
fn handle_resp_shard_result(
    conn: &mut ConnState,
    conn_id: u64,
    seq: u64,
    group: u32,
    result: &BatchResult,
    db: &std::sync::Arc<str>,
    table: &std::sync::Arc<str>,
    worker_id: u32,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    let codec = RespCodec::new();
    // ⭐ Phase G: Geo 渲染钩子 (复用 ZMScore/ZRange 结果, 优先拦截)
    if let Some(ctx) = conn.geo_ctx.remove(&seq) {
        let bytes = render_geo(&codec, ctx, result);
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ Phase B: Bitmap 读渲染钩子 (Get 结果 + 位运算)
    if let Some(ctx) = conn.bit_ctx.remove(&seq) {
        let bytes = render_bit(&codec, ctx, result);
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ MGET 聚合: Values 按组索引表回填原始槽, 全组回齐拼 *N 数组
    if let Some(agg) = conn.mget_agg.get_mut(&seq) {
        match result {
            BatchResult::Values(vs) => {
                if let Some(idxs) = agg.groups.get(group as usize) {
                    for (v, &orig) in vs.iter().zip(idxs.iter()) {
                        agg.slots[orig] = v.clone();
                    }
                }
            }
            BatchResult::Error(e) if agg.error.is_none() => {
                agg.error = Some(e.clone());
            }
            _ => {}
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.mget_agg.remove(&seq).expect("just checked");
            let bytes = if let Some(e) = agg.error {
                codec.encode_error(&e)
            } else {
                let mut out = format!("*{}\r\n", agg.slots.len()).into_bytes();
                for slot in &agg.slots {
                    match slot {
                        Some(stored) => {
                            // ⭐ N3: 按 tag 渲染 (数值二进制 → 字符串)
                            out.extend_from_slice(&codec.encode_bulk(&render(stored)));
                        }
                        None => out.extend_from_slice(b"$-1\r\n"),
                    }
                }
                out
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // ⭐ MSET 聚合: 全组 MultiPutOk → +OK
    if let Some(agg) = conn.mset_agg.get_mut(&seq) {
        if let BatchResult::Error(e) = result
            && agg.error.is_none()
        {
            agg.error = Some(e.clone());
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.mset_agg.remove(&seq).expect("just checked");
            let bytes = match agg.error {
                Some(e) => codec.encode_error(&e),
                None => codec.encode_ok(),
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // ⭐ EXISTS 聚合: GetValue(Some) 计数, 全部回齐回 :n
    if let Some(agg) = conn.exists_agg.get_mut(&seq) {
        if let BatchResult::GetValue(Some(_)) = result {
            agg.count += 1;
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let count = agg.count;
            conn.exists_agg.remove(&seq);
            conn.resp_complete(seq, codec.encode_integer(count));
        }
        return;
    }
    // ⭐ STRLEN/TYPE/HEXISTS: Get 结果语义转换
    if let Some(kind) = conn.get_kind.remove(&seq) {
        let bytes = match (kind, result) {
            (GetKind::Strlen, BatchResult::GetValue(None)) => codec.encode_integer(0),
            (GetKind::Strlen, BatchResult::GetValue(Some(stored))) => {
                // ⭐ N3: 数值 tag 按渲染后字符串计长 (Redis 语义)
                codec.encode_integer(render(stored).len() as i64)
            }
            (GetKind::TypeOf, BatchResult::GetValue(None)) => codec.encode_simple("none"),
            (GetKind::TypeOf, BatchResult::GetValue(Some(_))) => codec.encode_simple("string"),
            // ⭐ Phase H: HEXISTS — HGet 结果转 0/1
            (GetKind::HExists, BatchResult::GetValue(None)) => codec.encode_integer(0),
            (GetKind::HExists, BatchResult::GetValue(Some(_))) => codec.encode_integer(1),
            (_, BatchResult::Error(e)) => codec.encode_error(e),
            _ => codec.encode_error("unexpected result"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ Phase H: HMSET — Integer 结果转 +OK
    if conn.hmset_ok.remove(&seq) {
        let bytes = match result {
            BatchResult::Integer(_) => codec.encode_ok(),
            BatchResult::Error(e) => codec.encode_error(e),
            _ => codec.encode_error("unexpected result"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ GETRANGE: Get 结果渲染后按 (start,end) 切片 (支持负索引)
    if let Some((start, end)) = conn.getrange_ctx.remove(&seq) {
        let bytes = match result {
            BatchResult::GetValue(None) => codec.encode_bulk(b""),
            BatchResult::GetValue(Some(stored)) => {
                let s = render(stored);
                codec.encode_bulk(getrange_slice(s.as_ref(), start, end))
            }
            BatchResult::Error(e) => codec.encode_error(e),
            _ => codec.encode_error("unexpected result"),
        };
        conn.resp_complete(seq, bytes);
        return;
    }
    // ⭐ MSETNX 聚合: 全组 Integer(1) → :1, 任一非 1 → :0
    if let Some(agg) = conn.msetnx_agg.get_mut(&seq) {
        if !matches!(result, BatchResult::Integer(1)) {
            agg.all_set = false;
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let all = agg.all_set;
            conn.msetnx_agg.remove(&seq);
            conn.resp_complete(seq, codec.encode_integer(i64::from(all)));
        }
        return;
    }
    // ⭐ Phase Set: SINTER/SUNION/SDIFF 聚合 — 全部 key 的成员回齐后求代数
    if let Some(agg) = conn.setalg_agg.get_mut(&seq) {
        match result {
            BatchResult::Members(ms) => {
                if let Some(slot) = agg.sets.get_mut(group as usize) {
                    *slot = Some(ms.clone());
                }
            }
            BatchResult::Error(e) if agg.error.is_none() => {
                agg.error = Some(e.clone());
            }
            _ => {}
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.setalg_agg.remove(&seq).expect("just checked");
            if let Some(e) = agg.error {
                conn.resp_complete(seq, codec.encode_error(&e));
                return;
            }
            use std::collections::HashSet;
            let (card_only, limit) = (agg.card_only, agg.limit);
            let store_dst = agg.store_dst;
            let mut sets: Vec<Vec<Vec<u8>>> =
                agg.sets.into_iter().map(|s| s.unwrap_or_default()).collect();
            let first = if sets.is_empty() { Vec::new() } else { sets.remove(0) };
            let out: Vec<Vec<u8>> = match agg.op {
                SetAlgOp::Inter => {
                    let others: Vec<HashSet<&[u8]>> = sets
                        .iter()
                        .map(|s| s.iter().map(|m| m.as_slice()).collect())
                        .collect();
                    first
                        .into_iter()
                        .filter(|m| others.iter().all(|o| o.contains(m.as_slice())))
                        .collect()
                }
                SetAlgOp::Diff => {
                    let others: Vec<HashSet<&[u8]>> = sets
                        .iter()
                        .map(|s| s.iter().map(|m| m.as_slice()).collect())
                        .collect();
                    first
                        .into_iter()
                        .filter(|m| !others.iter().any(|o| o.contains(m.as_slice())))
                        .collect()
                }
                SetAlgOp::Union => {
                    let mut seen: HashSet<Vec<u8>> = HashSet::new();
                    let mut out = Vec::new();
                    for m in first.into_iter().chain(sets.into_iter().flatten()) {
                        if seen.insert(m.clone()) {
                            out.push(m);
                        }
                    }
                    out
                }
            };
            // ⭐ C3: *STORE — 结果写 dst (同 shard FIFO: 先 Delete 再 SAdd), 完成后回 :card
            if let Some(dst) = store_dst {
                let card = out.len() as i64;
                let sid = hash_route_key(db.as_ref(), table.as_ref(), &dst, num_shards);
                let mut remaining = 1usize;
                let del = BatchOp::Delete { db: db.clone(), table: table.clone(), key: dst.clone() };
                push_task_grouped(conn_id, seq, worker_id, 0, sid, del, shard_inboxes);
                if !out.is_empty() {
                    remaining += 1;
                    let sadd = BatchOp::SAdd { db: db.clone(), table: table.clone(), key: dst, members: out };
                    push_task_grouped(conn_id, seq, worker_id, 1, sid, sadd, shard_inboxes);
                }
                conn.store_agg.insert(seq, StoreFinishAgg { remaining, card, error: None });
                return;
            }
            // ⭐ C1: SINTERCARD — 只回势 (LIMIT 截断); 否则回成员数组
            let bytes = if card_only {
                let card = if limit > 0 { out.len().min(limit) } else { out.len() };
                codec.encode_integer(card as i64)
            } else {
                let mut buf = format!("*{}\r\n", out.len()).into_bytes();
                for m in &out {
                    buf.extend_from_slice(&codec.encode_bulk(m));
                }
                buf
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // ⭐ C3: ZINTERSTORE/ZUNIONSTORE 源聚合 — ZRange(withscores) 交替串还原 (member, score)
    if let Some(agg) = conn.zstore_agg.get_mut(&seq) {
        match result {
            BatchResult::Members(ms) => {
                let mut rows = Vec::with_capacity(ms.len() / 2);
                let mut i = 0;
                while i + 1 < ms.len() {
                    let score = std::str::from_utf8(&ms[i + 1])
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    rows.push((ms[i].clone(), score));
                    i += 2;
                }
                if let Some(slot) = agg.sets.get_mut(group as usize) {
                    *slot = Some(rows);
                }
            }
            BatchResult::Error(e) if agg.error.is_none() => {
                agg.error = Some(e.clone());
            }
            _ => {}
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.zstore_agg.remove(&seq).expect("just checked");
            if let Some(e) = agg.error {
                conn.resp_complete(seq, codec.encode_error(&e));
                return;
            }
            // SUM 聚合 (首现序保序; inter 要求出现在全部源)
            let inter = agg.inter;
            let n_sets = agg.sets.len();
            let mut acc: Vec<(Vec<u8>, f64, usize)> = Vec::new();
            let mut pos: HashMap<Vec<u8>, usize> = HashMap::new();
            for set in agg.sets.into_iter().map(|s| s.unwrap_or_default()) {
                for (m, sc) in set {
                    match pos.get(&m) {
                        Some(&i) => {
                            acc[i].1 += sc;
                            acc[i].2 += 1;
                        }
                        None => {
                            pos.insert(m.clone(), acc.len());
                            acc.push((m, sc, 1));
                        }
                    }
                }
            }
            let pairs: Vec<(f64, Vec<u8>)> = acc
                .into_iter()
                .filter(|(_, _, cnt)| !inter || *cnt == n_sets)
                .map(|(m, sc, _)| (sc, m))
                .collect();
            let card = pairs.len() as i64;
            let dst = agg.dst;
            let sid = hash_route_key(db.as_ref(), table.as_ref(), &dst, num_shards);
            let mut remaining = 1usize;
            let del = BatchOp::Delete { db: db.clone(), table: table.clone(), key: dst.clone() };
            push_task_grouped(conn_id, seq, worker_id, 0, sid, del, shard_inboxes);
            if !pairs.is_empty() {
                remaining += 1;
                let zadd = BatchOp::ZAdd { db: db.clone(), table: table.clone(), key: dst, pairs };
                push_task_grouped(conn_id, seq, worker_id, 1, sid, zadd, shard_inboxes);
            }
            conn.store_agg.insert(seq, StoreFinishAgg { remaining, card, error: None });
        }
        return;
    }
    // ⭐ C3: *STORE 第二阶段 (Delete + SAdd/ZAdd) 全部完成 → 回 :card
    if let Some(agg) = conn.store_agg.get_mut(&seq) {
        if let BatchResult::Error(e) = result
            && agg.error.is_none()
        {
            agg.error = Some(e.clone());
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let agg = conn.store_agg.remove(&seq).expect("just checked");
            let bytes = match agg.error {
                Some(e) => codec.encode_error(&e),
                None => codec.encode_integer(agg.card),
            };
            conn.resp_complete(seq, bytes);
        }
        return;
    }
    // DEL 聚合路径
    if let Some(agg) = conn.del_agg.get_mut(&seq) {
        match result {
            BatchResult::DeleteExisted(existed) => {
                if *existed {
                    agg.count += 1;
                }
            }
            BatchResult::Error(_) => {
                // 单 key 失败按未删除计 (Redis DEL 语义: 返回实际删除数)
            }
            _ => {}
        }
        agg.remaining -= 1;
        if agg.remaining == 0 {
            let count = agg.count;
            conn.del_agg.remove(&seq);
            conn.resp_complete(seq, codec.encode_integer(count));
        }
        return;
    }

    let bytes = match result {
        BatchResult::PutOk | BatchResult::MultiPutOk => codec.encode_ok(),
        BatchResult::GetValue(None) => codec.encode_nil(),
        BatchResult::GetValue(Some(stored)) => {
            // ⭐ N3: 按 tag 渲染 (RAW 借用零拷贝; 数值二进制 → 字符串)
            codec.encode_bulk(&render(stored))
        }
        BatchResult::DeleteExisted(existed) => codec.encode_integer(*existed as i64),
        BatchResult::Integer(n) => codec.encode_integer(*n),
        // INCRBYFLOAT: Redis 语义回 bulk string (非 integer)
        BatchResult::Double(f) => codec.encode_bulk(format!("{f}").as_bytes()),
        // ⭐ Phase H: HMGET 单 op 直回 Values → *N 数组 (逐项渲染;
        // ⭐ C1: ZMSCORE 的 Values 已成形, 裸 bulk 直出)
        BatchResult::Values(vs) => {
            let raw = conn.values_raw.remove(&seq);
            let mut out = format!("*{}\r\n", vs.len()).into_bytes();
            for v in vs {
                match v {
                    Some(stored) => {
                        if raw {
                            out.extend_from_slice(&codec.encode_bulk(stored));
                        } else {
                            out.extend_from_slice(&codec.encode_bulk(&render(stored)));
                        }
                    }
                    None => out.extend_from_slice(b"$-1\r\n"),
                }
            }
            out
        }
        // ⭐ Phase H: HGETALL/HKEYS/HVALS/HSCAN 按 pairs_kind 渲染
        BatchResult::Pairs(ps) => {
            let kind = conn.pairs_kind.remove(&seq).unwrap_or(PairsKind::All);
            encode_pairs(&codec, ps, kind)
        }
        // ⭐ Phase Set: SMEMBERS/SSCAN/SPOP/SRANDMEMBER 按 members_kind 渲染
        BatchResult::Members(ms) => {
            let kind = conn.members_kind.remove(&seq).unwrap_or(MembersKind::List);
            match kind {
                MembersKind::List => {
                    let mut out = format!("*{}\r\n", ms.len()).into_bytes();
                    for m in ms {
                        out.extend_from_slice(&codec.encode_bulk(m));
                    }
                    out
                }
                MembersKind::Scan => {
                    let mut out = b"*2\r\n".to_vec();
                    out.extend_from_slice(&codec.encode_bulk(b"0"));
                    out.extend_from_slice(&format!("*{}\r\n", ms.len()).into_bytes());
                    for m in ms {
                        out.extend_from_slice(&codec.encode_bulk(m));
                    }
                    out
                }
                MembersKind::One => match ms.first() {
                    Some(m) => codec.encode_bulk(m),
                    None => codec.encode_nil(),
                },
            }
        }
        // ⭐ Phase Z: ZSCORE/ZRANK 可选成员 (Some→bulk, None→nil)
        BatchResult::OptMember(m) => match m {
            Some(b) => codec.encode_bulk(b),
            None => codec.encode_nil(),
        },
        // ⭐ C1: SMISMEMBER → *N 个 :0/:1
        BatchResult::IntList(ns) => {
            let mut out = format!("*{}\r\n", ns.len()).into_bytes();
            for n in ns {
                out.extend_from_slice(&codec.encode_integer(*n));
            }
            out
        }
        BatchResult::Error(e) => codec.encode_error(e),
    };
    conn.resp_complete(seq, bytes);
}

/// ⭐ Phase H: Pairs 结果渲染 (HGETALL/HKEYS/HVALS/HSCAN 共用).
fn encode_pairs(codec: &RespCodec, ps: &[(Vec<u8>, Vec<u8>)], kind: PairsKind) -> Vec<u8> {
    match kind {
        PairsKind::All => {
            let mut out = format!("*{}\r\n", ps.len() * 2).into_bytes();
            for (f, v) in ps {
                out.extend_from_slice(&codec.encode_bulk(f));
                out.extend_from_slice(&codec.encode_bulk(&render(v)));
            }
            out
        }
        PairsKind::Keys => {
            let mut out = format!("*{}\r\n", ps.len()).into_bytes();
            for (f, _) in ps {
                out.extend_from_slice(&codec.encode_bulk(f));
            }
            out
        }
        PairsKind::Vals => {
            let mut out = format!("*{}\r\n", ps.len()).into_bytes();
            for (_, v) in ps {
                out.extend_from_slice(&codec.encode_bulk(&render(v)));
            }
            out
        }
        PairsKind::Scan => {
            // HSCAN v1: 单次全量返回, cursor 恒为 "0"
            let mut out = b"*2\r\n".to_vec();
            out.extend_from_slice(&codec.encode_bulk(b"0"));
            out.extend_from_slice(&encode_pairs(codec, ps, PairsKind::All));
            out
        }
        // ⭐ C1: HRANDFIELD 无 count — 首 field 单 bulk / nil
        PairsKind::OneKey => match ps.first() {
            Some((f, _)) => codec.encode_bulk(f),
            None => codec.encode_nil(),
        },
    }
}

// ===== 辅助函数 =====

fn epoll_add(epoll_fd: RawFd, fd: RawFd, token: u64) {
    // 水平触发 (默认): 比边缘触发更稳健, 不会丢事件
    let mut event = libc::epoll_event {
        events: libc::EPOLLIN as u32,
        u64: token,
    };
    unsafe {
        libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, fd, &mut event);
    }
}

fn epoll_del(epoll_fd: RawFd, fd: RawFd) {
    unsafe {
        libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut());
    }
}

fn peek_req_id(frame: &[u8]) -> u64 {
    if frame.len() < 12 {
        return 0;
    }
    u64::from_be_bytes(frame[4..12].try_into().unwrap())
}

/// Request → BatchOp. ⭐ `Request::Put.value` 已是 `[tag][payload]` 布局
/// (decode 时预置), 直接 move — 零二次拷贝.
fn request_to_batch_op(req: Request, db: &std::sync::Arc<str>, table: &std::sync::Arc<str>) -> BatchOp {
    match req {
        Request::Put { key, value } => BatchOp::Put {
            db: db.clone(),
            table: table.clone(),
            key,
            val: value,
        },
        Request::Get { key } => BatchOp::Get {
            db: db.clone(),
            table: table.clone(),
            key,
        },
        Request::Delete { key } => BatchOp::Delete {
            db: db.clone(),
            table: table.clone(),
            key,
        },
    }
}

/// BatchResult → Binary Response. ⭐ Get 命中时剥 value type tag.
/// (注: payload.to_vec 是 Response::Get(Option<Vec>) 结构所需;
/// Binary 非 benchmark 主路径, 借用化需改 Protocol trait, 收益不值 — 记录保留.)
fn batch_result_to_response(result: &BatchResult) -> Response {
    match result {
        BatchResult::PutOk => Response::PutOk,
        BatchResult::GetValue(None) => Response::Get(None),
        BatchResult::GetValue(Some(stored)) => {
            let (_tag, payload) = decode_value(stored);
            Response::Get(Some(payload.to_vec()))
        }
        BatchResult::DeleteExisted(_) => Response::DeleteOk,
        // Multi/RMW/Hash op 是 RESP 专属 (Binary 门面不会产生)
        BatchResult::Values(_)
        | BatchResult::MultiPutOk
        | BatchResult::Integer(_)
        | BatchResult::Double(_)
        | BatchResult::Pairs(_)
        | BatchResult::Members(_)
        | BatchResult::OptMember(_)
        | BatchResult::IntList(_) => {
            Response::Error("multi ops unsupported on binary protocol".into())
        }
        BatchResult::Error(e) => Response::Error(e.clone()),
    }
}

fn hash_route_op(op: &BatchOp, num_shards: usize) -> usize {
    let (db, table, key) = match op {
        BatchOp::Put { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::Get { db, table, key } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::Delete { db, table, key } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::Incr { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::IncrFloat { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::Append { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::SetNx { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::GetDel { db, table, key } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::GetSet { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::SetRange { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        // ⭐ Phase H: Hash 单 key op, 按 user key 路由
        BatchOp::HSet { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::HSetNx { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::HGet { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::HMGet { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::HDel { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::HLen { db, table, key } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::HGetAll { db, table, key } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::HIncrBy { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::HIncrByFloat { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        // ⭐ Phase Set: Set 单 key op
        BatchOp::SAdd { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::SRem { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::SIsMember { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::SCard { db, table, key } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::SMembers { db, table, key } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::SPop { db, table, key } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::SRandMember { db, table, key } => (db.as_ref(), table.as_ref(), key.as_slice()),
        // ⭐ Phase L: List 单 key op
        BatchOp::LPush { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::LPop { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::LLen { db, table, key } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::LRange { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::LIndex { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::LSet { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        // ⭐ Phase Z: ZSet 单 key op
        BatchOp::ZAdd { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::ZRem { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::ZScore { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::ZCard { db, table, key } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::ZIncrBy { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::ZRange { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::ZRangeByScore { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::ZRank { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::ZCount { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::ZMScore { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::ZPop { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::SMisMember { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::SPopN { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::SRandCount { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::HRandField { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::LRem { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::LTrim { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::LPos { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::LInsert { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        BatchOp::SetBit { db, table, key, .. } => (db.as_ref(), table.as_ref(), key.as_slice()),
        // Multi op 不经此路径 (dispatch 已按 key 分组定向 push)
        BatchOp::MultiGet { .. } | BatchOp::MultiPut { .. } | BatchOp::MultiPutNx { .. } => {
            unreachable!("Multi ops are pre-routed by dispatch")
        }
    };
    hash_route_key(db, table, key, num_shards)
}

/// ⭐ Phase G: score 串 (fmt_score 输出) → 52-bit geohash.
fn geo_bits(b: &[u8]) -> Option<u64> {
    std::str::from_utf8(b)
        .ok()?
        .parse::<f64>()
        .ok()
        .filter(|f| *f >= 0.0 && *f < (1u64 << 52) as f64)
        .map(|f| f as u64)
}

/// ⭐ Phase G: Geo 命令渲染 (GEOPOS/GEODIST/GEOSEARCH).
fn render_geo(codec: &RespCodec, ctx: GeoCtx, result: &BatchResult) -> Vec<u8> {
    use crate::geo_bridge as geo;
    if let BatchResult::Error(e) = result {
        return codec.encode_error(e);
    }
    match ctx {
        // GEOPOS: 每 member → [lon, lat] 或 nil array
        GeoCtx::Pos => {
            let BatchResult::Values(vs) = result else {
            return codec.encode_error("unexpected result");
            };
            let mut out = format!("*{}\r\n", vs.len()).into_bytes();
            for v in vs {
                match v.as_deref().and_then(geo_bits) {
                    Some(bits) => {
                        let (lon, lat) = geo::decode(bits);
                        out.extend_from_slice(b"*2\r\n");
                        out.extend_from_slice(&codec.encode_bulk(format!("{lon:.17}").as_bytes()));
                        out.extend_from_slice(&codec.encode_bulk(format!("{lat:.17}").as_bytes()));
                    }
                    None => out.extend_from_slice(b"*-1\r\n"),
                }
            }
            out
        }
        // GEODIST: 两点都在才有距离
        GeoCtx::Dist { factor } => {
            let BatchResult::Values(vs) = result else {
                return codec.encode_error("unexpected result");
            };
            let b1 = vs.first().and_then(|v| v.as_deref()).and_then(geo_bits);
            let b2 = vs.get(1).and_then(|v| v.as_deref()).and_then(geo_bits);
            match (b1, b2) {
                (Some(b1), Some(b2)) => {
                    let (lon1, lat1) = geo::decode(b1);
                    let (lon2, lat2) = geo::decode(b2);
                    let d = geo::haversine_m(lon1, lat1, lon2, lat2) / factor;
                    codec.encode_bulk(format!("{d:.4}").as_bytes())
                }
                _ => codec.encode_nil(),
            }
        }
        // GEOSEARCH: 解码全量 (member, score) → 距离过滤 + 排序 + COUNT
        GeoCtx::Search { lon, lat, radius_m, asc, count, withcoord, withdist } => {
            let BatchResult::Members(ms) = result else {
                return codec.encode_error("unexpected result");
            };
            let mut hits: Vec<(&[u8], f64, f64, f64)> = Vec::new(); // (member, dist, lon, lat)
            let mut i = 0;
            while i + 1 < ms.len() {
                if let Some(bits) = geo_bits(&ms[i + 1]) {
                    let (mlon, mlat) = geo::decode(bits);
                    let d = geo::haversine_m(lon, lat, mlon, mlat);
                    if d <= radius_m {
                        hits.push((&ms[i], d, mlon, mlat));
                    }
                }
                i += 2;
            }
            hits.sort_by(|a, b| a.1.partial_cmp(&b.1).expect("dist 非 NaN"));
            if !asc {
                hits.reverse();
            }
            if count > 0 {
                hits.truncate(count);
            }
            let mut out = format!("*{}\r\n", hits.len()).into_bytes();
            for (m, d, mlon, mlat) in hits {
                if !withcoord && !withdist {
                    out.extend_from_slice(&codec.encode_bulk(m));
                    continue;
                }
                // 嵌套数组: [member, (dist), ([lon, lat])] (Redis 顺序)
                let items = 1 + usize::from(withdist) + usize::from(withcoord);
                out.extend_from_slice(format!("*{items}\r\n").as_bytes());
                out.extend_from_slice(&codec.encode_bulk(m));
                if withdist {
                    out.extend_from_slice(&codec.encode_bulk(format!("{d:.4}").as_bytes()));
                }
                if withcoord {
                    out.extend_from_slice(b"*2\r\n");
                    out.extend_from_slice(&codec.encode_bulk(format!("{mlon:.17}").as_bytes()));
                    out.extend_from_slice(&codec.encode_bulk(format!("{mlat:.17}").as_bytes()));
                }
            }
            out
        }
    }
}

/// ⭐ Phase B: BYTE 区间裁剪 (Redis 负索引语义, 与 getrange_slice 同).
fn bit_byte_range(len: usize, start: i64, end: i64) -> Option<(usize, usize)> {
    let len = len as i64;
    if len == 0 {
        return None;
    }
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if end < 0 { len + end } else { end };
    if s < 0 {
        s = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e {
        return None;
    }
    Some((s as usize, e as usize))
}

/// ⭐ Phase B: Bitmap 读命令渲染 (GETBIT/BITCOUNT/BITPOS).
fn render_bit(codec: &RespCodec, ctx: BitCtx, result: &BatchResult) -> Vec<u8> {
    if let BatchResult::Error(e) = result {
        return codec.encode_error(e);
    }
    let data: &[u8] = match result {
        BatchResult::GetValue(Some(stored)) => &render(stored),
        BatchResult::GetValue(None) => &[],
        _ => return codec.encode_error("unexpected result"),
    };
    match ctx {
        BitCtx::GetBit { offset } => {
            let byte = (offset / 8) as usize;
            let bit = if byte < data.len() {
                (data[byte] >> (7 - (offset % 8) as u8)) & 1
            } else {
                0
            };
            codec.encode_integer(bit as i64)
        }
        BitCtx::Count { start, end } => {
            let n = match bit_byte_range(data.len(), start, end) {
                Some((s, e)) => data[s..=e].iter().map(|b| b.count_ones() as i64).sum(),
                None => 0,
            };
            codec.encode_integer(n)
        }
        BitCtx::Pos { bit, start, end } => {
            // 不存在 key: 找 1 → -1; 找 0 → 0 (Redis 语义)
            if data.is_empty() {
                return codec.encode_integer(if bit { -1 } else { 0 });
            }
            let range = bit_byte_range(data.len(), start, end.unwrap_or(-1));
            let pos = match range {
                None => -1,
                Some((s, e)) => {
                    let mut found = -1i64;
                    for (i, &b) in data[s..=e].iter().enumerate() {
                        let probe = if bit { b } else { !b };
                        if probe != 0 {
                            found = ((s + i) * 8 + probe.leading_zeros() as usize) as i64;
                            break;
                        }
                    }
                    // 全 1 找 0 且未显式给 end → 返回字符串右侧第一个越界位 (Redis)
                    if found == -1 && !bit && end.is_none() {
                        found = (data.len() * 8) as i64;
                    }
                    found
                }
            };
            codec.encode_integer(pos)
        }
    }
}
