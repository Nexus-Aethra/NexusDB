//! ConnState 核心方法 (拆自 mod.rs).
//!
//! 连接状态管理: 构造/收发/TLS/响应序列化, multi-statement 编排,
//! PG 扩展协议 schema 恢复, 分表路由辅助.

use super::*;
use std::os::fd::AsRawFd;

impl ConnState {
    pub(crate) fn new(
        fd: RawFd,
        proto: ProtocolKind,
        auth_required: bool,
        default_db: std::sync::Arc<str>,
        sql_cache: SharedSqlCache,
        sql_shared: std::sync::Arc<SqlSharedRoutes>,
        reply_bus: SharedTaskReplyBus,
        db_view: std::sync::Arc<shard_manager::DbDirView>,
        worker_id: u32,
        num_shards: usize,
        shard_inboxes: Vec<SharedTaskInbox>,
    ) -> Self {
        let stream = unsafe { TcpStream::from_raw_fd(fd) };
        stream.set_nonblocking(true).ok();
        // ⭐ 关闭 Nagle: 小回复立即发送, 避免与 delayed-ACK 交互导致 40ms 延迟
        stream.set_nodelay(true).ok();
        Self {
            fd,
            stream,
            tls: None,
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
            current_db: default_db.clone(),
            table_cache: HashMap::new(),
            sql_cache,
            sql_shared,
            sql_ddl_agg: HashMap::new(),
            sql_dml_agg: HashMap::new(),
            cascade_pending: HashMap::new(),
            cascade_jobs: HashMap::new(),
            cascade_roots: HashMap::new(),
            cascade_seq_ctr: 0,
            sql_fk_ins: HashMap::new(),
            multi_stmt: HashMap::new(),
            multi_sub_seq: HashMap::new(),
            reply_bus,
            reply_db_view: db_view,
            reply_worker_id: worker_id,
            reply_num_shards: num_shards,
            reply_default_db: default_db,
            reply_shard_inboxes: shard_inboxes,
            txn: None,
            txn_failed: false,
            sql_txn_agg: HashMap::new(),
            sql_unique_ins: HashMap::new(),
            sql_sysq: HashMap::new(),
            sql_join: HashMap::new(),
            sql_subq: HashMap::new(),
            sql_derived: HashMap::new(),
            default_iso: sql::TxnIso::default(),
            default_ro: false,
            sql_select_agg: HashMap::new(),
            sql_row_ctx: HashMap::new(),
            sql_pending: HashMap::new(),
            mysql: None,
            pg_phase: 0,
            pg_scram: None,
            http_ctx: HashMap::new(),
            mysql_stmts: HashMap::new(),
            next_stmt_id: 1,
            mysql_binary: std::collections::HashSet::new(),
            pg_stmts: HashMap::new(),
            pg_pending_prepares: HashMap::new(),
            pg_waiting_schema: false,
            pg_waiting_schema_seq: 0,
            pg_batch: PgBatch::default(),
            pg_ext: HashMap::new(),
            close_after_flush: false,
        }
    }

    /// 从连接 recv 数据, 追加到 read_buf. (epoll worker 用, 同步)
    /// 返回 Ok(true) = 有数据, Ok(false) = 连接关闭, Err = 错误.
    /// TLS 路径读密文喂 rustls → 冲刷握手 → 读明文入 read_buf; 明文路径直接读.
    pub(crate) fn recv(&mut self) -> std::io::Result<bool> {
        // ⭐ F83: TLS 路径
        if let Some(tls) = self.tls.as_mut() {
            let mut eof = false;
            loop {
                match tls.read_tls(&mut self.stream) {
                    Ok(0) => {
                        eof = true;
                        break;
                    }
                    Ok(_) => {
                        if let Err(e) = tls.process_new_packets() {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e));
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            }
            while tls.wants_write() {
                match tls.write_tls(&mut self.stream) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::yield_now();
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                }
            }
            let before = self.read_buf.len();
            let mut tmp = [0u8; 4096];
            loop {
                match tls.reader().read(&mut tmp) {
                    Ok(0) => break,
                    Ok(n) => self.read_buf.extend_from_slice(&tmp[..n]),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => break,
                }
            }
            let got_plain = self.read_buf.len() > before;
            return Ok(!(eof && !got_plain));
        }
        // 明文路径
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

    /// 协程 worker 用: 从连接 recv 数据 (io_uring async), 追加到 read_buf.
    /// 返回 Ok(true) = 有数据, Ok(false) = 连接关闭, Err = 错误.
    /// ⭐ 协程化 (2026-08): 用 scheduler::io_ops::read 替代同步 read + WouldBlock 自旋.
    /// 仅在协程上下文 (Scheduler 线程) 中调用. 暂只支持非 TLS 明文路径
    /// (测试未启用 TLS, TLS 协程化独立后续).
    pub(crate) async fn recv_async(&mut self) -> std::io::Result<bool> {
        let mut tmp = [0u8; 4096];
        loop {
            let n = match scheduler::io_ops::read(self.stream.as_raw_fd(), &mut tmp, u64::MAX).await
            {
                Ok(n) => n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            if n == 0 {
                return Ok(false); // EOF
            }
            self.read_buf.extend_from_slice(&tmp[..n]);
            if n < tmp.len() {
                return Ok(true); // 读完了本次可用数据
            }
            // 可能还有更多, 继续 read
        }
    }

    /// ⭐ F83: 就地把明文连接升级为 TLS (STARTTLS). 握手在后续 recv 泵中完成.
    pub(crate) fn start_tls(&mut self, config: std::sync::Arc<rustls::ServerConfig>) -> bool {
        match rustls::ServerConnection::new(config) {
            Ok(c) => {
                self.tls = Some(Box::new(c));
                true
            }
            Err(_) => false,
        }
    }

    /// 发送原始字节. non-blocking socket 遇 WouldBlock 时 spin retry
    /// (回复帧小, 正常情况下 send buffer 不会满太久).
    pub(crate) fn send_bytes(&mut self, bytes: &[u8]) {
        // ⭐ F83: TLS 路径 — 明文写入 rustls writer, 再泵密文到 socket.
        if let Some(tls) = self.tls.as_mut() {
            if tls.writer().write_all(bytes).is_err() {
                return;
            }
            while tls.wants_write() {
                match tls.write_tls(&mut self.stream) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::yield_now();
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                }
            }
            return;
        }
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
    pub(crate) fn send_binary_response(&mut self, req_id: u64, resp: &Response) {
        let bytes = BinaryProtocol::new().encode_response(req_id, resp);
        self.send_bytes(&bytes);
    }

    /// RESP: 回复字节进重排缓冲, 然后把从 next_to_send 起的连续段发出.
    pub(crate) fn resp_complete(&mut self, seq: u64, bytes: Vec<u8>) {
        // ⭐ PG 兼容 (FMT_VER 8): 级联伪 seq 的回包不发给客户端 (完成/推进
        // 由 DmlAgg/Fire 拦截点经 cascade_job_done 处理; 此处兜底防泄漏).
        if is_cascade_seq(seq) {
            return;
        }
        // ⭐ PG 兼容 (multi-statement): 多语句子 seq 的回包由 multi_step 推进,
        // 不直接发给客户端 (兜底防泄漏). 同步语句 (DdlStub 等) 直接 resp_complete
        // 到这里 → 此处推进下一条.
        if self.multi_sub_seq.contains_key(&seq) {
            let orig = self.multi_sub_seq.get(&seq).cloned().unwrap_or(seq);
            let conn_id = self
                .multi_stmt
                .get(&orig)
                .map(|m| m.conn_id)
                .unwrap_or(0);
            let worker_id = self.reply_worker_id;
            let num_shards = self.reply_num_shards;
            let default_db = self.reply_default_db.clone();
            let db_view = self.reply_db_view.clone();
            let shard_inboxes = self.reply_shard_inboxes.clone();
            self.multi_step(
                seq, conn_id, worker_id, &default_db, &db_view, &shard_inboxes, num_shards,
            );
            return;
        }
        // ⭐ P3: PG 扩展查询批次 — 响应前拼 [ParseComplete][BindComplete]... 前缀
        // (单点侵入; 非 Pg conn 恒空查零开销)
        let mut bytes = match self.pg_ext.remove(&seq) {
            Some(mut prefix) => {
                prefix.extend_from_slice(&bytes);
                prefix
            }
            None => bytes,
        };
        // ⭐ 事务 v1 (F61): 协议级事务状态单点注入 (免渲染函数签名扩散)
        match self.proto {
            ProtocolKind::Pg => {
                // 事务内遇 ErrorResponse → 置 failed (后续语句 25P02 拦截)
                if self.txn.is_some() && !self.txn_failed && pg_frames_contain_error(&bytes) {
                    self.txn_failed = true;
                }
                // 尾部 ReadyForQuery 状态字节: I idle / T in-txn / E failed
                let n = bytes.len();
                if n >= 6 && bytes[n - 6] == b'Z' && bytes[n - 5..n - 1] == [0, 0, 0, 5] {
                    bytes[n - 1] = if self.txn_failed {
                        b'E'
                    } else if self.txn.is_some() {
                        b'T'
                    } else {
                        b'I'
                    };
                }
            }
            ProtocolKind::Sql if self.txn.is_some() => {
                // 纯 OK 包 (单包且 payload 首字节 0x00) → status |= IN_TRANS
                let n = bytes.len();
                if n >= 11
                    && bytes[4] == 0x00
                    && u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]) as usize + 4 == n
                {
                    bytes[n - 4] |= 0x01; // SERVER_STATUS_IN_TRANS
                }
            }
            _ => {}
        }
        self.pending.insert(seq, bytes);
        self.resp_flush_ready();
    }

    pub(crate) fn resp_flush_ready(&mut self) {
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
    pub(crate) fn resp_should_close(&self) -> bool {
        self.close_after_flush && self.pending.is_empty() && self.next_seq == self.next_to_send
    }

    /// ⭐ PG 兼容 (multi-statement): 顺序执行一条语句. 解析后 dispatch,
    /// 记录类型 (DDL/DML/同步) 供完成推进. 子 seq = base + dispatched.
    pub(crate) fn dispatch_multi_one(
        &mut self,
        conn_id: u64,
        worker_id: u32,
        sub_seq: u64,
        text: &str,
        default_db: &std::sync::Arc<str>,
        db_view: &std::sync::Arc<shard_manager::DbDirView>,
        shard_inboxes: &[SharedTaskInbox],
        num_shards: usize,
    ) {
        let cur_db = self.current_db.clone();
        match sql::parse(text.as_bytes()) {
            Err(e) => {
                // 解析失败 → 整条多语句报错
                if let Some(orig) = self.multi_sub_seq.get(&sub_seq).cloned() {
                    self.multi_sub_seq.remove(&sub_seq);
                    if let Some(m) = self.multi_stmt.get_mut(&orig) {
                        m.error = Some(e);
                        m.stmts.clear();
                    }
                    self.multi_finish(orig);
                }
            }
            Ok(stmt) => {
                // 记录类型
                let kind = match &stmt {
                    SqlStmt::CreateTable { .. } | SqlStmt::AlterTable { .. } => 1u8, // DDL
                    SqlStmt::DropTable { .. } => 1u8, // DDL (DROP 走 dml_agg? 见下)
                    _ => 0u8, // 同步/其他 (SELECT/SET/USE 等同步回包)
                };
                if let Some(orig) = self.multi_sub_seq.get(&sub_seq).cloned() {
                    if let Some(m) = self.multi_stmt.get_mut(&orig) {
                        m.cur_kind = kind;
                    }
                }
                sql_dispatch_stmt(
                    self, conn_id, sub_seq, worker_id, &cur_db, default_db, db_view,
                    shard_inboxes, num_shards, stmt,
                );
            }
        }
    }

    /// ⭐ PG 兼容 (multi-statement): 完成处理 — 推进下一条或全部完成回原 seq.
    pub(crate) fn multi_step(
        &mut self,
        sub_seq: u64,
        conn_id: u64,
        worker_id: u32,
        default_db: &std::sync::Arc<str>,
        db_view: &std::sync::Arc<shard_manager::DbDirView>,
        shard_inboxes: &[SharedTaskInbox],
        num_shards: usize,
    ) {
        let Some(orig) = self.multi_sub_seq.get(&sub_seq).cloned() else { return };
        let mut done = false;
        let mut next: Option<String> = None;
        let mut error: Option<String> = None;
        {
            // ⭐ 防御: 同 sub_seq 可能被 DDL agg 完成 + resp_complete 守卫双触发,
            // multi 状态可能已移除 → 安全返回 (防 worker panic / 连接关闭)
            let Some(m) = self.multi_stmt.get_mut(&orig) else { return };
            // ⭐ PG 兼容: 每条语句回一个 CommandComplete (multi-statement 需逐条
            // 响应, 否则 pgx 等不足 N 个 CommandComplete 而挂起)
            m.cmd_bytes.extend_from_slice(&crate::protocol::pg::build_command_complete("SELECT 1"));
            m.dispatched += 1;
            if m.error.is_some() {
                error = m.error.clone();
                m.stmts.clear();
            }
            if let Some(nxt) = m.stmts.pop_front() {
                next = Some(nxt);
            } else {
                done = true;
            }
        }
        if let Some(e) = error {
            self.multi_sub_seq.remove(&sub_seq);
            self.multi_finish(orig);
            return;
        }
        if let Some(nxt) = next {
            // 续跑下一条: 新子 seq = orig? 不, 用 base + dispatched
            let m = self.multi_stmt.get_mut(&orig).unwrap();
            let next_sub_seq = m.base_sub_seq + m.dispatched as u64;
            self.multi_sub_seq.insert(next_sub_seq, orig);
            let text = nxt;
            self.dispatch_multi_one(
                conn_id, worker_id, next_sub_seq, &text, default_db, db_view,
                shard_inboxes, num_shards,
            );
        } else if done {
            self.multi_sub_seq.remove(&sub_seq);
            self.multi_finish(orig);
        }
    }

    /// ⭐ PG 兼容 (multi-statement): 全部完成 → 用原 seq 回逐条 CommandComplete
    /// + ReadyForQuery (PG 协议要求每条语句一个 CommandComplete).
    pub(crate) fn multi_finish(&mut self, orig: u64) {
        let Some(m) = self.multi_stmt.remove(&orig) else { return };
        // ⭐ 修复 (2026-08): multi 子语句占用了客户端 seq 区间 [base, base+N),
        // 但回包只发 orig 一个 seq. resp_complete(orig) 后 next_to_send 停在
        // orig+1(=base), 而 base..base+N-1 的子 seq 无单独 pending 包 → 顺序
        // 推进的 resp_flush_ready 永久等空洞, 导致 multi 完成后同一连接的后续
        // 任何请求 (如 portal 迁移的 INSERT INTO schema_migrations) 全部挂起.
        // 解决: 完成后把 next_to_send / next_seq 直接推进到 span_end(=base+N),
        // 跳过空洞子 seq, 使后续请求恢复可派发.
        let span_end = m.base_sub_seq + m.dispatched as u64;
        if let Some(e) = m.error {
            self.resp_complete(orig, sql_err_bytes(ProtocolKind::Pg, &e));
            if self.next_to_send < span_end {
                self.next_to_send = span_end;
            }
            if self.next_seq < span_end {
                self.next_seq = span_end;
            }
            return;
        }
        let mut out = m.cmd_bytes;
        out.extend_from_slice(&crate::protocol::pg::build_ready());
        self.resp_complete(orig, out);
        if self.next_to_send < span_end {
            self.next_to_send = span_end;
        }
        if self.next_seq < span_end {
            self.next_seq = span_end;
        }
    }

    /// ⭐ P3 (portal): 清理挂起批次的残留状态 (清空 pg_batch, 复位等待标志).
    pub(crate) fn clear_pg_waiting_schema(&mut self) {
        self.pg_waiting_schema = false;
        self.pg_waiting_schema_seq = 0;
        self.pg_pending_prepares.clear();
        std::mem::take(&mut self.pg_batch);
    }

    /// ⭐ P3 (portal): 续跑挂起的 PG Parse — GetSchemaOp 回包到达后, 用 schema
    /// 推断参数 OID, 插入 pg_stmts, 回 ParseComplete+ParameterDescription+NoData
    /// +ReadyForQuery (pgx 的 Prepare 是独立往返 Parse+Describe+Sync, 此时回包即可).
    pub(crate) fn resume_pg_pending_parse(&mut self, schema: std::sync::Arc<storage::schema::TableSchema>) {
        // 填入 worker schema 缓存 (供 infer_param_oids 重推)
        let prepares = std::mem::take(&mut self.pg_pending_prepares);
        for (name, p) in prepares {
            // 从 p.stmt 提取目标表名填 schema 缓存 (Insert/Select/SystemQuery/SelectJoin)
            let table = match &p.stmt {
                crate::protocol::sql::SqlStmt::Insert { table, .. }
                | crate::protocol::sql::SqlStmt::Select { table, .. }
                | crate::protocol::sql::SqlStmt::SystemQuery { table, .. } => Some(table.clone()),
                crate::protocol::sql::SqlStmt::SelectJoin { from, .. } => {
                    Some(from.table.clone())
                }
                _ => None,
            };
            if let Some(table) = table {
                let key = (self.current_db.as_ref().to_string(), table);
                self.sql_cache.borrow_mut().schemas.insert(key, schema.clone());
            }
            // 重推参数 OID (schema 已缓存)
            let (inferred, _) = crate::worker::protocol_io::infer_param_oids(self, &p.stmt, p.params);
            let mut oids = p.oids;
            for (i, o) in inferred.iter().enumerate() {
                if i < oids.len() && oids[i] == 0 {
                    oids[i] = *o;
                }
            }
            // 回 ParseComplete + ParameterDescription + NoData + ReadyForQuery
            // (先用 &oids 构造响应, 再 move 进 pg_stmts)
            // ⭐ seq 用 next_to_send: 挂起批次经 GetSchemaOp 续跑, 期间未产生普通
            // pending 包, next_to_send 停在挂起前; 用 next_seq 会无法 flush (等
            // next_to_send 追上来而卡死). 直接用 next_to_send 保证立即发出.
            let mut out = Vec::with_capacity(64);
            out.extend_from_slice(&crate::protocol::pg::build_parse_complete());
            out.extend_from_slice(&crate::protocol::pg::build_param_description(&oids, p.params));
            out.extend_from_slice(&crate::protocol::pg::build_no_data());
            out.extend_from_slice(&crate::protocol::pg::build_ready());
            // ⭐ 用 next_to_send 作为 seq: resp_complete 内部 resp_flush_ready 从
            // next_to_send 起 flush 挂起的包. 不能手动 +1 (会跳过导致不 flush).
            let seq = self.next_to_send;
            if self.next_seq <= seq {
                self.next_seq = seq + 1;
            }
            self.resp_complete(seq, out);
            self.pg_stmts.insert(name, PgPrepared { stmt: p.stmt, params: p.params, oids });
        }
        // 复位等待标志并清空残留批次 (挂起时未 take 的 pg_batch)
        self.pg_waiting_schema = false;
        self.pg_waiting_schema_seq = 0;
        std::mem::take(&mut self.pg_batch);
    }

    /// ⭐ 巨型 INSERT 防死锁 (2026-08): 批量 push 超过 inbox/reply_bus 容量时,
    /// 在 push 循环内 drain reply_bus 并处理回包, 释放 reply_bus 空间让 shard
    /// 继续消费 inbox — 打破 worker(等 inbox)↔shard(等 reply_bus) 循环等待.
    pub(crate) fn drain_replies(&mut self, conn_id: u64) {
        let results = self.reply_bus.drain();
        if results.is_empty() {
            return;
        }
        for r in results {
            if r.conn_id != conn_id {
                continue; // 只处理本连接的回包 (其余等事件循环)
            }
            let worker_id = self.reply_worker_id;
            let num_shards = self.reply_num_shards;
            let default_db = self.reply_default_db.clone();
            let db_view = self.reply_db_view.clone();
            let shard_inboxes = self.reply_shard_inboxes.clone();
            handle_resp_shard_result(
                self,
                r.conn_id,
                r.req_id,
                r.group,
                &r.result,
                worker_id,
                &default_db,
                &db_view,
                &shard_inboxes,
                num_shards,
            );
        }
    }

    /// ⭐ T2 (分表): 表名前缀 → Arc<str> (缓存复用, 免热路径 String 分配).
    pub(crate) fn table_arc(&mut self, prefix: &[u8]) -> std::sync::Arc<str> {
        if let Some(t) = self.table_cache.get(prefix) {
            return t.clone();
        }
        let t: std::sync::Arc<str> =
            std::sync::Arc::from(std::str::from_utf8(prefix).expect("前缀已校验 ASCII"));
        self.table_cache.insert(prefix.to_vec(), t.clone());
        t
    }

    /// ⭐ T2 (分表): 就地解析 "table:key" — 命中合法前缀则剥离前缀并返回表;
    /// 否则 None (整个 key 落 default 表).
    pub(crate) fn resolve_table(&mut self, key: &mut Vec<u8>) -> Option<std::sync::Arc<str>> {
        let pos = split_table_key(key)?;
        let tbl = self.table_arc(&key[..pos]);
        key.drain(..=pos);
        Some(tbl)
    }
}

