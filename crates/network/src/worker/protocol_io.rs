//! ⭐ 解耦 2026-08: 协议 wire 入口 (HTTP; PG/SQL 后续拆分). 拆自 mod.rs.
//! process_http_input / handle_http_request — HTTP 请求 → KV/SQL 分发.

use super::*;
use crate::protocol::http as h;
use std::sync::atomic::Ordering::Relaxed;

pub(crate) fn process_http_input(
    conn: &mut ConnState,
    conn_id: u64,
    worker_id: u32,
    http_token: &Option<String>,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    limits: &KvLimits,
    num_shards_total: usize,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    use crate::protocol::http as h;
    let cors = crate::http_config::cors_origin();
    let mut cursor = 0usize;
    loop {
        if conn.close_after_flush {
            break;
        }
        match h::parse_request(&conn.read_buf[cursor..]) {
            Ok(None) => break,
            Err((code, msg)) => {
                crate::metrics::HTTP_ERRORS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let seq = conn.next_seq;
                conn.next_seq += 1;
                conn.resp_complete(seq, h::build_response(code, &h::error_body(msg), cors, false));
                conn.close_after_flush = true;
                break;
            }
            Ok(Some((n, req))) => {
                cursor += n;
                crate::metrics::HTTP_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if !req.keep_alive {
                    conn.close_after_flush = true;
                }
                handle_http_request(
                    conn, conn_id, worker_id, req, http_token, default_db, db_view, limits,
                    num_shards_total, shard_inboxes, num_shards,
                );
            }
        }
    }
    if cursor > 0 {
        conn.read_buf.drain(..cursor);
    }
}

/// 单请求路由分发 (worker 本地端点就地渲染, KV/SQL 走 shard 任务).
#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_http_request(
    conn: &mut ConnState,
    conn_id: u64,
    worker_id: u32,
    req: crate::protocol::http::HttpRequest,
    http_token: &Option<String>,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    limits: &KvLimits,
    num_shards_total: usize,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
) {
    use crate::protocol::http as h;
    use std::sync::atomic::Ordering::Relaxed;
    let cors = crate::http_config::cors_origin();
    let seq = conn.next_seq;
    conn.next_seq += 1;
    let ka = req.keep_alive;
    let fail = |conn: &mut ConnState, seq, code: u16, msg: &str| {
        crate::metrics::HTTP_ERRORS.fetch_add(1, Relaxed);
        conn.resp_complete(seq, h::build_response(code, &h::error_body(msg), cors, ka));
    };
    // OPTIONS preflight (免鉴权)
    if req.method == "OPTIONS" {
        conn.resp_complete(seq, h::build_preflight(cors, ka));
        return;
    }
    // Bearer 鉴权 (白名单: /metrics /v1/status — 监控接入惯例)
    if let Some(token) = http_token
        && req.path != "/metrics"
        && req.path != "/v1/status"
    {
        let ok = req
            .authorization
            .as_deref()
            .and_then(|a| a.strip_prefix("Bearer "))
            .is_some_and(|t| t == token);
        if !ok {
            fail(conn, seq, 401, "unauthorized");
            return;
        }
    }
    // db 选择: query 参数 (KV) / body 字段 (SQL); 缺省 = default_db
    let resolve_db = |name: Option<&str>| -> Result<std::sync::Arc<str>, String> {
        match name.filter(|s| !s.is_empty()) {
            None => Ok(conn_default(default_db)),
            Some(d) if d == default_db.as_ref() || db_view.id_of(d).is_some() => {
                Ok(std::sync::Arc::from(d))
            }
            Some(d) => Err(format!("Unknown database '{d}'")),
        }
    };
    match (req.method.as_str(), req.path.as_str()) {
        // ---- ⭐ H4: 可观测性端点 (worker 本地零任务) ----
        ("GET", "/metrics") => {
            let m = format!(
                "# TYPE nexusdb_http_requests_total counter\n\
                 nexusdb_http_requests_total {}\n\
                 # TYPE nexusdb_http_errors_total counter\n\
                 nexusdb_http_errors_total {}\n\
                 # TYPE nexusdb_sql_queries_total counter\n\
                 nexusdb_sql_queries_total {}\n\
                 # TYPE nexusdb_kv_ops_total counter\n\
                 nexusdb_kv_ops_total {}\n\
                 # TYPE nexusdb_sql_join_est_rounds counter\n\
                 nexusdb_sql_join_est_rounds {}\n\
                 # TYPE nexusdb_sql_join_est_skipped counter\n\
                 nexusdb_sql_join_est_skipped {}\n\
                 # TYPE nexusdb_uptime_seconds gauge\n\
                 nexusdb_uptime_seconds {}\n",
                crate::metrics::HTTP_REQUESTS.load(Relaxed),
                crate::metrics::HTTP_ERRORS.load(Relaxed),
                crate::metrics::SQL_QUERIES.load(Relaxed),
                crate::metrics::KV_OPS.load(Relaxed),
                crate::metrics::SQL_JOIN_EST_ROUNDS.load(Relaxed),
                crate::metrics::SQL_JOIN_EST_SKIPPED.load(Relaxed),
                crate::metrics::uptime_seconds(),
            );
            conn.resp_complete(seq, h::build_text_response(200, m.as_bytes(), ka));
        }
        ("GET", "/v1/status") => {
            let body = serde_json::json!({
                "version": env!("CARGO_PKG_VERSION"),
                "uptime_seconds": crate::metrics::uptime_seconds(),
                "num_shards": num_shards_total,
                "protocols": {"binary": 5433, "resp": 6379, "mysql": 5434, "pg": 5435, "http": 6778},
            });
            conn.resp_complete(
                seq,
                h::build_response(200, &serde_json::to_vec(&body).unwrap_or_default(), cors, ka),
            );
        }
        ("GET", "/v1/debug/sql-cache") => {
            let body = {
                use std::sync::atomic::Ordering::Relaxed;
                let sh = &conn.sql_shared;
                serde_json::json!({
                    "worker_schemas": conn.sql_cache.borrow().schemas.len(),
                    "routes": sh.routes.read().unwrap().len(),
                    "created_here": sh.created_here.read().unwrap().len(),
                    "ddl_epoch": sh.ddl_epoch.load(Relaxed),
                    "route_pruned": sh.route_pruned.load(Relaxed),
                    "route_bypassed": sh.route_bypassed.load(Relaxed),
                })
            };
            conn.resp_complete(
                seq,
                h::build_response(200, &serde_json::to_vec(&body).unwrap_or_default(), cors, ka),
            );
        }
        // ---- ⭐ H3: SQL ----
        ("POST", "/v1/sql") => {
            let parsed: Result<serde_json::Value, _> = serde_json::from_slice(&req.body);
            let Ok(body) = parsed else {
                fail(conn, seq, 400, "body must be JSON");
                return;
            };
            let Some(query) = body.get("query").and_then(|q| q.as_str()) else {
                fail(conn, seq, 400, "missing 'query' field");
                return;
            };
            let db = match resolve_db(body.get("db").and_then(|d| d.as_str())) {
                Ok(d) => d,
                Err(e) => {
                    fail(conn, seq, 400, &e);
                    return;
                }
            };
            match sql::parse(query.as_bytes()) {
                Err(e) => conn.resp_complete(seq, sql_err_bytes(ProtocolKind::Http, &e)),
                Ok(stmt) => sql_dispatch_stmt(
                    conn, conn_id, seq, worker_id, &db, default_db, db_view, shard_inboxes,
                    num_shards, stmt,
                ),
            }
        }
        // ---- ⭐ H2: KV ----
        (m, p) if p.starts_with("/v1/kv/") => {
            let rest = &p["/v1/kv/".len()..];
            let Some((table_raw, key_raw)) = rest.split_once('/') else {
                fail(conn, seq, 404, "expected /v1/kv/{table}/{key}");
                return;
            };
            let table = String::from_utf8_lossy(&h::percent_decode(table_raw)).into_owned();
            let key = h::percent_decode(key_raw);
            if table.is_empty() || key.is_empty() {
                fail(conn, seq, 400, "empty table or key");
                return;
            }
            if key.len() > limits.max_key_bytes {
                fail(conn, seq, 400, "key too long");
                return;
            }
            let db = match resolve_db(h::query_param(&req.query, "db")) {
                Ok(d) => d,
                Err(e) => {
                    fail(conn, seq, 400, &e);
                    return;
                }
            };
            let table_arc: std::sync::Arc<str> = std::sync::Arc::from(table.as_str());
            crate::metrics::KV_OPS.fetch_add(1, Relaxed);
            let (op, kv) = match m {
                "GET" => (
                    BatchOp::Get { db, table: table_arc, key },
                    HttpKvOp::Get,
                ),
                "DELETE" => (
                    BatchOp::Delete { db, table: table_arc, key },
                    HttpKvOp::Delete,
                ),
                "PUT" | "POST" => {
                    // body: {"value": <string|number>} — tag 与 RESP 同源
                    let Ok(body) = serde_json::from_slice::<serde_json::Value>(&req.body) else {
                        fail(conn, seq, 400, "body must be JSON");
                        return;
                    };
                    let stored = match body.get("value") {
                        Some(serde_json::Value::String(s)) => crate::value_codec::encode_value(
                            shard_manager::request::VALUE_TAG_RAW,
                            s.as_bytes(),
                        ),
                        Some(v) if v.is_i64() => {
                            shard_manager::value_num::encode_i64(v.as_i64().unwrap())
                        }
                        Some(v) if v.is_f64() => {
                            shard_manager::value_num::encode_f64(v.as_f64().unwrap())
                        }
                        _ => {
                            fail(conn, seq, 400, "missing 'value' (string or number)");
                            return;
                        }
                    };
                    if stored.len().saturating_sub(1) > limits.max_value_bytes {
                        fail(conn, seq, 400, "value too long");
                        return;
                    }
                    (
                        BatchOp::Put { db, table: table_arc, key, val: stored },
                        HttpKvOp::Put,
                    )
                }
                _ => {
                    fail(conn, seq, 405, "method not allowed");
                    return;
                }
            };
            conn.http_ctx.insert(seq, HttpReqCtx { op: kv, keep_alive: ka });
            push_task(conn, conn_id, seq, worker_id, op, shard_inboxes, num_shards);
        }
        _ => fail(conn, seq, 404, "not found"),
    }
}

/// db 缺省 helper (borrow checker 拆分用).
pub(crate) fn conn_default(default_db: &std::sync::Arc<str>) -> std::sync::Arc<str> {
    default_db.clone()
}

/// SQL 错误 → MySQL ERR 包 (seq 1; 错误码按消息粗分类).
pub(crate) fn mysql_err_packet(msg: &str) -> Vec<u8> {
    let code = if msg.contains("unknown column") {
        1054
    } else if msg.contains("duplicate key") {
        1062 // ER_DUP_ENTRY — UNIQUE 冲突 (ORM 据此识别 IntegrityError)
    } else if msg.contains("serialization failure") {
        1213 // MySQL deadlock/serialization 惯用重试码
    } else if msg.contains("read-only transaction") {
        1792
    } else if msg.contains("Unknown database") {
        1049
    } else if msg.contains("has no schema") || msg.contains("doesn't exist") {
        1146 // ER_NO_SUCH_TABLE — ORM has_table 据此判表不存在后发 CREATE
    } else if msg.contains("expected") || msg.contains("unexpected") || msg.contains("unterminated")
    {
        1064
    } else if msg.contains("Access denied") {
        1045
    } else {
        1105
    };
    crate::protocol::mysql::build_err(1, code, msg)
}

pub(crate) fn process_sql_input(
    conn: &mut ConnState,
    conn_id: u64,
    worker_id: u32,
    sql_password: &Option<String>,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    tls_config: &Option<std::sync::Arc<rustls::ServerConfig>>,
) {
    use crate::protocol::mysql as my;
    let mut cursor = 0usize;
    while let Some((pkt_seq, n, payload)) = my::read_packet(&conn.read_buf[cursor..]) {
        cursor += n;
        if conn.close_after_flush {
            break;
        }
        let Some(st) = conn.mysql.as_ref() else {
            break; // 防御: 非 mysql 状态的 Sql conn 不存在
        };
        let (phase, salt) = (st.phase, st.salt);
        let pwd = sql_password.as_deref().unwrap_or("");
        // ⭐ F83: phase 0 且 conn 未升级 TLS 时, 短包 + CLIENT_SSL → SSLRequest, 升级后等加密的真响应
        if phase == 0 && conn.tls.is_none() && payload.len() >= 4 {
            let caps = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
            if caps & my::CLIENT_SSL != 0 && payload.len() <= 36 {
                if let Some(cfg) = tls_config {
                    if !conn.start_tls(cfg.clone()) {
                        conn.close_after_flush = true;
                    }
                    // 消费该 SSLRequest 包, 退出循环等 ClientHello + 加密 HandshakeResponse41
                    break;
                }
                // 未配置 TLS 却收到 SSLRequest → 拒
                conn.send_bytes(&my::build_err(pkt_seq.wrapping_add(1), 1043, "TLS not supported"));
                conn.close_after_flush = true;
                break;
            }
        }
        match phase {
            // ---- 等 HandshakeResponse41 ----
            0 => match my::parse_handshake_response(&payload) {
                Ok(login) => {
                    // ⭐ S5: 登录带 database → 认证通过后切库 (不存在 1049 断连)
                    let want_db = login.database.clone().filter(|d| !d.is_empty());
                    let db_ok = |name: &str| {
                        name == default_db.as_ref() || db_view.id_of(name).is_some()
                    };
                    if let Some(d) = &want_db
                        && !db_ok(d)
                    {
                        conn.send_bytes(&my::build_err(
                            pkt_seq.wrapping_add(1),
                            1049,
                            &format!("Unknown database '{d}'"),
                        ));
                        conn.close_after_flush = true;
                        continue;
                    }
                    let native = login
                        .plugin
                        .as_deref()
                        .is_none_or(|p| p == "mysql_native_password");
                    let is_caching = login.plugin.as_deref() == Some("caching_sha2_password");
                    // ⭐ F82: caching_sha2 fast-auth — 服务端知明文口令直接验证 (免 RSA/TLS).
                    //   成功 → fast_auth_success(0x01 0x03)+OK; 失败/其他 → 走 AuthSwitch 兜底.
                    if is_caching && my::caching_sha2_password_ok(&salt, &login.auth_resp, pwd) {
                        conn.send_bytes(&my::build_fast_auth_success(pkt_seq.wrapping_add(1)));
                        conn.send_bytes(&my::build_ok(pkt_seq.wrapping_add(2), 0));
                        if let Some(d) = want_db {
                            conn.current_db = std::sync::Arc::from(d.as_str());
                        }
                        if let Some(st) = conn.mysql.as_mut() {
                            st.phase = 2;
                        }
                    } else if !native || (login.auth_resp.is_empty() && !pwd.is_empty()) {
                        // 客户端默认 caching_sha2 (8.x) 或未带凭据 → 切换插件重试
                        conn.send_bytes(&my::build_auth_switch(pkt_seq.wrapping_add(1), &salt));
                        if let Some(st) = conn.mysql.as_mut() {
                            st.phase = 1;
                            st.pending_db = want_db;
                        }
                    } else if my::native_password_ok(&salt, &login.auth_resp, pwd) {
                        conn.send_bytes(&my::build_ok(pkt_seq.wrapping_add(1), 0));
                        if let Some(d) = want_db {
                            conn.current_db = std::sync::Arc::from(d.as_str());
                        }
                        if let Some(st) = conn.mysql.as_mut() {
                            st.phase = 2;
                        }
                    } else {
                        conn.send_bytes(&my::build_err(
                            pkt_seq.wrapping_add(1),
                            1045,
                            "Access denied",
                        ));
                        conn.close_after_flush = true;
                    }
                }
                Err(e) => {
                    conn.send_bytes(&my::build_err(pkt_seq.wrapping_add(1), 1043, &e));
                    conn.close_after_flush = true;
                }
            },
            // ---- 等 AuthSwitch 响应 (payload = 裸 native token) ----
            1 => {
                if my::native_password_ok(&salt, &payload, pwd) {
                    conn.send_bytes(&my::build_ok(pkt_seq.wrapping_add(1), 0));
                    // ⭐ S5: 二段认证通过 → 应用登录时的 database
                    let pending = conn.mysql.as_mut().and_then(|st| st.pending_db.take());
                    if let Some(d) = pending {
                        conn.current_db = std::sync::Arc::from(d.as_str());
                    }
                    if let Some(st) = conn.mysql.as_mut() {
                        st.phase = 2;
                    }
                } else {
                    conn.send_bytes(&my::build_err(
                        pkt_seq.wrapping_add(1),
                        1045,
                        "Access denied",
                    ));
                    conn.close_after_flush = true;
                }
            }
            // ---- 已认证: 命令阶段 ----
            _ => match payload.first() {
                Some(&my::COM_QUERY) => {
                    let seq = conn.next_seq;
                    conn.next_seq += 1;
                    let cur_db = conn.current_db.clone();
                    match sql::parse(&payload[1..]) {
                        Err(e) => conn.resp_complete(seq, sql_err_bytes(conn.proto, &e)),
                        Ok(stmt) => sql_dispatch_stmt(
                            conn, conn_id, seq, worker_id, &cur_db, default_db, db_view,
                            shard_inboxes, num_shards, stmt,
                        ),
                    }
                }
                Some(&my::COM_PING) => {
                    // 占 seq 保序 (与在途 COM_QUERY 的 FIFO 一致)
                    let seq = conn.next_seq;
                    conn.next_seq += 1;
                    conn.resp_complete(seq, my::build_ok(pkt_seq.wrapping_add(1), 0));
                }
                // ⭐ S5: COM_INIT_DB (mysql cli 的 `USE x`) — 真切库
                Some(&my::COM_INIT_DB) => {
                    let seq = conn.next_seq;
                    conn.next_seq += 1;
                    let name = String::from_utf8_lossy(&payload[1..]).into_owned();
                    let ok = name == default_db.as_ref() || db_view.id_of(&name).is_some();
                    if ok {
                        conn.current_db = std::sync::Arc::from(name.as_str());
                        conn.resp_complete(seq, my::build_ok(pkt_seq.wrapping_add(1), 0));
                    } else {
                        conn.resp_complete(
                            seq,
                            my::build_err(
                                pkt_seq.wrapping_add(1),
                                1049,
                                &format!("Unknown database '{name}'"),
                            ),
                        );
                    }
                }
                // ⭐ P2: 预处理语句族
                Some(&my::COM_STMT_PREPARE) => {
                    let seq = conn.next_seq;
                    conn.next_seq += 1;
                    match sql::parse_prepared(&payload[1..]) {
                        Ok((stmt, params)) => {
                            let id = conn.next_stmt_id;
                            conn.next_stmt_id += 1;
                            conn.mysql_stmts.insert(id, MyPrepared { stmt, params, types: None });
                            conn.resp_complete(seq, my::build_stmt_prepare_ok(id, params));
                        }
                        Err(e) => conn.resp_complete(seq, mysql_err_packet(&e)),
                    }
                }
                Some(&my::COM_STMT_EXECUTE) => {
                    let seq = conn.next_seq;
                    conn.next_seq += 1;
                    // stmt_id 先探 (解参需要 params 数)
                    let stmt_id = if payload.len() >= 5 {
                        u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]])
                    } else {
                        0
                    };
                    let Some(prep) = conn.mysql_stmts.get_mut(&stmt_id) else {
                        conn.resp_complete(seq, my::build_err(1, 1243, "unknown statement id"));
                        continue;
                    };
                    // ⭐ ORM-C: 解参后直接对模板绑定 (bind_params 单次深拷贝),
                    // 省掉此前绕借用的 prep.stmt.clone() 整次拷贝
                    let bound = my::parse_stmt_execute(&payload, prep.params, &mut prep.types)
                        .and_then(|(_, vals)| sql::bind_params(&prep.stmt, &vals));
                    match bound {
                        Ok(stmt) => {
                            // SELECT 类结果需二进制结果集 (渲染点按标记分流)
                            conn.mysql_binary.insert(seq);
                            let cur_db = conn.current_db.clone();
                            sql_dispatch_stmt(
                                conn, conn_id, seq, worker_id, &cur_db, default_db, db_view,
                                shard_inboxes, num_shards, stmt,
                            );
                        }
                        Err(e) => conn.resp_complete(seq, mysql_err_packet(&e)),
                    }
                }
                Some(&my::COM_STMT_CLOSE) => {
                    // 无响应命令 (不占 seq)
                    if payload.len() >= 5 {
                        let id =
                            u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
                        conn.mysql_stmts.remove(&id);
                    }
                }
                Some(&my::COM_STMT_RESET) => {
                    let seq = conn.next_seq;
                    conn.next_seq += 1;
                    conn.resp_complete(seq, my::build_ok(1, 0));
                }
                Some(&my::COM_QUIT) => {
                    conn.close_after_flush = true;
                }
                _ => {
                    let seq = conn.next_seq;
                    conn.next_seq += 1;
                    conn.resp_complete(seq, my::build_err(1, 1047, "unsupported command"));
                }
            },
        }
    }
    if cursor > 0 {
        conn.read_buf.drain(..cursor);
    }
}

/// ⭐ S4: PostgreSQL wire 帧循环 — startup (SSLRequest 拒绝/参数解析) →
/// cleartext 认证 → simple Query. 每语句回复自带 ReadyForQuery (sql_*_bytes).
#[allow(clippy::too_many_arguments)]
pub(crate) fn process_pg_input(
    conn: &mut ConnState,
    conn_id: u64,
    worker_id: u32,
    sql_password: &Option<String>,
    default_db: &std::sync::Arc<str>,
    db_view: &std::sync::Arc<shard_manager::DbDirView>,
    shard_inboxes: &[SharedTaskInbox],
    num_shards: usize,
    tls_config: &Option<std::sync::Arc<rustls::ServerConfig>>,
) {
    use crate::protocol::pg;
    let pwd = sql_password.as_deref().unwrap_or("");
    let mut cursor = 0usize;
    loop {
        if conn.close_after_flush {
            break;
        }
        match conn.pg_phase {
            // ---- 等 StartupMessage (无 type 帧) ----
            0 => {
                let Some((n, payload)) = pg::read_startup_frame(&conn.read_buf[cursor..])
                else {
                    break;
                };
                cursor += n;
                if payload.len() == 4 {
                    let code = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                    match code {
                        // ⭐ F83: SSLRequest — 配置了 TLS → 回 'S' 并升级; 否则回 'N' 明文回落
                        pg::SSL_REQUEST_CODE => {
                            if let Some(cfg) = tls_config {
                                conn.send_bytes(b"S"); // 明文 'S' (升级前最后一个明文字节)
                                if !conn.start_tls(cfg.clone()) {
                                    conn.close_after_flush = true;
                                }
                                // 后续 StartupMessage 走 TLS, 回到 epoll 等 ClientHello
                                break;
                            }
                            conn.send_bytes(b"N");
                            continue;
                        }
                        pg::GSSENC_REQUEST_CODE => {
                            conn.send_bytes(b"N");
                            continue;
                        }
                        pg::CANCEL_REQUEST_CODE => {
                            conn.close_after_flush = true;
                            break;
                        }
                        _ => {}
                    }
                }
                match pg::parse_startup(payload) {
                    Ok((_user, database)) => {
                        // database 参数 → 切库 (不存在直接拒绝断连)
                        // ⭐ PG 兼容: "postgres" admin 库别名 → 映射 default 库 (migrator 探测用)
                        if let Some(dbn) = database
                            && !dbn.is_empty()
                            && dbn != default_db.as_ref()
                        {
                            let resolved = if dbn == "postgres" {
                                default_db.clone()
                            } else {
                                std::sync::Arc::from(dbn.as_str())
                            };
                            if &*resolved == default_db.as_ref() || db_view.id_of(&resolved).is_some() {
                                conn.current_db = resolved;
                            } else {
                                conn.send_bytes(&pg::build_error(
                                    "3D000",
                                    &format!("database \"{dbn}\" does not exist"),
                                ));
                                conn.close_after_flush = true;
                                break;
                            }
                        }
                        if pwd.is_empty() {
                            conn.send_bytes(&pg::build_auth_ok_bundle(conn_id as u32));
                            conn.pg_phase = 2;
                        } else {
                            // ⭐ F82: 宣告 SCRAM-SHA-256 (取代明文口令), 进 SASL 交换
                            conn.send_bytes(&pg::build_auth_sasl());
                            conn.pg_scram = None;
                            conn.pg_phase = 1;
                        }
                    }
                    Err(e) => {
                        conn.send_bytes(&pg::build_error("08P01", &e));
                        conn.close_after_flush = true;
                    }
                }
            }
            // ---- SASL 交换 (SCRAM-SHA-256): 首条 SASLInitialResponse, 次条 SASLResponse ----
            1 => {
                let Some((n, ty, payload)) = pg::read_frame(&conn.read_buf[cursor..]) else {
                    break;
                };
                cursor += n;
                if ty != b'p' {
                    conn.send_bytes(&pg::build_error("28P01", "expected SASL message"));
                    conn.close_after_flush = true;
                    continue;
                }
                if conn.pg_scram.is_none() {
                    // 首条: SASLInitialResponse (mechanism + client-first)
                    let Some((mech, client_first)) = pg::parse_sasl_initial(payload) else {
                        conn.send_bytes(&pg::build_error("28P01", "malformed SASL initial response"));
                        conn.close_after_flush = true;
                        continue;
                    };
                    if mech != "SCRAM-SHA-256" {
                        conn.send_bytes(&pg::build_error("28P01", "unsupported SASL mechanism"));
                        conn.close_after_flush = true;
                        continue;
                    }
                    match pg::scram_server_first(&client_first) {
                        Some((state, server_first)) => {
                            conn.send_bytes(&pg::build_auth_sasl_continue(&server_first));
                            conn.pg_scram = Some(state);
                        }
                        None => {
                            conn.send_bytes(&pg::build_error("28P01", "malformed SCRAM client-first"));
                            conn.close_after_flush = true;
                        }
                    }
                } else {
                    // 次条: SASLResponse (client-final) → 验证 proof
                    let state = conn.pg_scram.take().expect("scram state present");
                    match pg::scram_verify_final(&state, payload, pwd) {
                        Some(server_final) => {
                            conn.send_bytes(&pg::build_auth_sasl_final(&server_final));
                            conn.send_bytes(&pg::build_auth_ok_bundle(conn_id as u32));
                            conn.pg_phase = 2;
                        }
                        None => {
                            conn.send_bytes(&pg::build_error(
                                "28P01",
                                "password authentication failed",
                            ));
                            conn.close_after_flush = true;
                        }
                    }
                }
            }
            // ---- 已认证: simple Query ----
            _ => {
                let Some((n, ty, payload)) = pg::read_frame(&conn.read_buf[cursor..]) else {
                    break;
                };
                cursor += n;
                match ty {
                    b'Q' => {
                        // 语句预处理: NUL 截断 + trim + 剥尾分号
                        let end = payload.iter().position(|&b| b == 0).unwrap_or(payload.len());
                        let text = String::from_utf8_lossy(&payload[..end]);
                        // ⭐ compat: 支持 multi-statement (分号分割, 字符串/注释感知)
                        let parts = sql::split_sql_statements(text.trim());
                        let seq = conn.next_seq;
                        conn.next_seq += 1;
                        if parts.is_empty() {
                            // EmptyQueryResponse + ReadyForQuery
                            let mut out = Vec::new();
                            out.push(b'I');
                            out.extend_from_slice(&4u32.to_be_bytes());
                            out.extend_from_slice(&pg::build_ready());
                            conn.resp_complete(seq, out);
                        } else if parts.len() == 1 {
                            let cur_db = conn.current_db.clone();
                            match sql::parse(parts[0].as_bytes()) {
                                Err(e) => conn
                                    .resp_complete(seq, sql_err_bytes(ProtocolKind::Pg, &e)),
                                Ok(stmt) => sql_dispatch_stmt(
                                    conn, conn_id, seq, worker_id, &cur_db, default_db,
                                    db_view, shard_inboxes, num_shards, stmt,
                                ),
                            }
                        } else if parts.len() > 1 {
                            // ⭐ PG 兼容 (multi-statement): 顺序执行每条 (story-loom
                            // 迁移整文件 Exec). DDL 走 ddl_agg 完成后推进下一条;
                            // 全部完成回原 seq. 仅支持 DDL/同步语句 (DML 异步广播
                            // 需聚合, 见 dispatch_multi_stmt).
                            let base_sub_seq = conn.next_seq;
                            let mut stmts: std::collections::VecDeque<String> =
                                parts.into_iter().collect();
                            let first = stmts.pop_front().unwrap_or_default();
                            conn.multi_stmt.insert(
                                seq,
                                MultiStmt {
                                    stmts,
                                    base_sub_seq,
                                    dispatched: 0,
                                    error: None,
                                    cur_kind: 0,
                                    conn_id,
                                },
                            );
                            // 首条也占一个子 seq (base_sub_seq), 从 base+1 开始续
                            conn.next_seq = base_sub_seq + 1;
                            conn.multi_sub_seq.insert(base_sub_seq, seq);
                            conn.dispatch_multi_one(
                                conn_id, worker_id, base_sub_seq, &first, default_db,
                                db_view, shard_inboxes, num_shards,
                            );
                        }
                    }
                    b'X' => {
                        conn.close_after_flush = true;
                    }
                    // ---- ⭐ P3: 扩展查询协议 (Parse..Sync 批次) ----
                    b'P' => match pg::parse_parse(payload) {
                        Ok((name, query, oids)) => match sql::parse_prepared(&query) {
                            Ok((stmt, params)) => {
                                conn.pg_stmts.insert(name, PgPrepared { stmt, params, oids });
                                let pc = pg::build_parse_complete();
                                conn.pg_batch.prefix.extend_from_slice(&pc);
                            }
                            Err(e) => {
                                if conn.pg_batch.error.is_none() {
                                    conn.pg_batch.error = Some(e);
                                }
                            }
                        },
                        Err(e) => {
                            if conn.pg_batch.error.is_none() {
                                conn.pg_batch.error = Some(e);
                            }
                        }
                    },
                    b'B' => {
                        if conn.pg_batch.error.is_some() {
                            continue; // skip-to-Sync
                        }
                        let r = pg::parse_bind(payload).and_then(|bind| {
                            let prep = conn
                                .pg_stmts
                                .get(&bind.statement)
                                .ok_or_else(|| format!("unknown statement '{}'", bind.statement))?;
                            if bind.binary_results {
                                return Err("binary result format is unsupported".into());
                            }
                            if bind.params.len() != prep.params as usize {
                                return Err(format!(
                                    "expected {} parameters, got {}",
                                    prep.params,
                                    bind.params.len()
                                ));
                            }
                            let mut vals = Vec::with_capacity(bind.params.len());
                            for (i, raw) in bind.params.iter().enumerate() {
                                let oid = prep.oids.get(i).copied().unwrap_or(0);
                                vals.push(pg::decode_param(
                                    raw.as_deref(),
                                    bind.formats.get(i).copied().unwrap_or(0),
                                    oid,
                                )?);
                            }
                            sql::bind_params(&prep.stmt, &vals)
                        });
                        match r {
                            Ok(stmt) => {
                                conn.pg_batch.bound = Some(stmt);
                                let bc = pg::build_bind_complete();
                                conn.pg_batch.prefix.extend_from_slice(&bc);
                            }
                            Err(e) => conn.pg_batch.error = Some(e),
                        }
                    }
                    b'D' => {
                        if conn.pg_batch.error.is_some() {
                            continue;
                        }
                        // Describe(statement) → ParameterDescription + NoData
                        // (列描述延迟到结果流 RowDescription — pgx/node-postgres
                        //  的 Describe(portal) 流由结果自带 T 满足).
                        if let Ok((b'S', name)) = pg::parse_target(payload)
                            && let Some(prep) = conn.pg_stmts.get(&name)
                        {
                            let pd = pg::build_param_description(&prep.oids, prep.params);
                            conn.pg_batch.prefix.extend_from_slice(&pd);
                            let nd = pg::build_no_data();
                            conn.pg_batch.prefix.extend_from_slice(&nd);
                        }
                    }
                    b'E' => {
                        conn.pg_batch.has_execute = true;
                    }
                    b'C' => {
                        // Close (语句/portal) → CloseComplete
                        if let Ok((b'S', name)) = pg::parse_target(payload) {
                            conn.pg_stmts.remove(&name);
                        }
                        let cc = pg::build_close_complete();
                        conn.pg_batch.prefix.extend_from_slice(&cc);
                    }
                    b'H' => {
                        // Flush: v1 以 Sync 为响应边界 (asyncpg 依赖 Flush 的
                        // 路径记录为 gap)
                    }
                    b'S' => {
                        let batch = std::mem::take(&mut conn.pg_batch);
                        let seq = conn.next_seq;
                        conn.next_seq += 1;
                        if let Some(e) = batch.error {
                            let mut out = batch.prefix;
                            out.extend_from_slice(&pg::build_error("42601", &e));
                            out.extend_from_slice(&pg::build_ready());
                            conn.resp_complete(seq, out);
                        } else if batch.has_execute && let Some(bound) = batch.bound {
                            // 结果主体 (T+D+C+Z / C+Z) 由既有渲染产出,
                            // 前缀在 resp_complete 单点拼接
                            conn.pg_ext.insert(seq, batch.prefix);
                            let cur_db = conn.current_db.clone();
                            sql_dispatch_stmt(
                                conn,
                                conn_id,
                                seq,
                                worker_id,
                                &cur_db,
                                default_db,
                                db_view,
                                shard_inboxes,
                                num_shards,
                                bound,
                            );
                        } else {
                            let mut out = batch.prefix;
                            out.extend_from_slice(&pg::build_ready());
                            conn.resp_complete(seq, out);
                        }
                    }
                    // Parse/Bind/... 扩展协议未支持; 其它消息容错回错误不断连
                    _ => {
                        let seq = conn.next_seq;
                        conn.next_seq += 1;
                        conn.resp_complete(
                            seq,
                            sql_err_bytes(
                                ProtocolKind::Pg,
                                "extended query protocol is unsupported",
                            ),
                        );
                    }
                }
            }
        }
    }
    if cursor > 0 {
        conn.read_buf.drain(..cursor);
    }
}

