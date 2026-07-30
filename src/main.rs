//! NexusDB 服务器入口.
//!
//! 用法: `nexusdb [--config <path>] [--version]`
//!
//! 流程: 读配置 → 初始化日志 → 拉起 ShardManager + NetworkServer → 信号优雅退出.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use network::{KvLimits, NetworkServer, NetworkServerConfig, ProtocolKind};
use shard_manager::{ShardManager, ShardManagerOptions};

/// 信号标志 (SIGINT/SIGTERM → true).
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, Ordering::Release);
}

fn main() {
    let mut config_path = PathBuf::from("./nexusdb.toml");
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => {
                config_path = PathBuf::from(args.next().unwrap_or_else(|| {
                    eprintln!("--config requires a path");
                    std::process::exit(2);
                }));
            }
            "--version" | "-V" => {
                println!("NexusDB {}", env!("CARGO_PKG_VERSION"));
                return;
            }
            other => {
                eprintln!("unknown argument: {other}");
                eprintln!("usage: nexusdb [--config <path>] [--version]");
                std::process::exit(2);
            }
        }
    }

    // 1. 加载配置 (文件不存在 → 默认值)
    let (cfg, from_file) = match config::NexusConfig::load_or_default(&config_path) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[nexusdb] config error: {e}");
            std::process::exit(1);
        }
    };

    // 2. 初始化日志 (io_uring 累积批量后端)
    let level = nlog::Level::parse(&cfg.log.level).expect("validated level");
    let settings = nlog::LogSettings {
        level,
        dir: if cfg.log.dir.is_empty() {
            None
        } else {
            Some(PathBuf::from(&cfg.log.dir))
        },
        buffer_bytes: cfg.log.buffer_kb * 1024,
        flush_interval: Duration::from_millis(cfg.log.flush_interval_ms),
        stderr: cfg.log.stderr,
    };
    if let Err(e) = nlog::init(&settings) {
        eprintln!("[nexusdb] log init failed: {e}");
        std::process::exit(1);
    }
    if from_file {
        nlog::info!("main", "config loaded from {}", config_path.display());
    } else {
        nlog::warn!(
            "main",
            "config file {} not found, using defaults",
            config_path.display()
        );
    }

    // 3. 注册信号处理 (SIGINT/SIGTERM → 优雅退出)
    unsafe {
        let handler = on_signal as extern "C" fn(libc::c_int) as usize;
        libc::signal(libc::SIGINT, handler as libc::sighandler_t);
        libc::signal(libc::SIGTERM, handler as libc::sighandler_t);
    }

    // 4. 启动 ShardManager
    let io_backend = cfg.storage.io_backend().expect("validated backend");
    // 三协议 server 并存时 worker_id 空间不重叠, reply_bus 总数 = 三者 worker 之和
    let resp_enabled = !cfg.server.redis_addr.is_empty();
    let binary_workers = cfg.server.worker_count;
    let resp_workers = if resp_enabled { cfg.server.worker_count } else { 0 };
    // ⭐ ORM-B3: SQL/PG 门面 worker 数可配 (路由缓存进程级共享后正确性成立)
    let sqlwc = cfg.server.sql_worker_count.max(1);
    let sql_workers = if cfg.server.sql_addr.is_empty() { 0 } else { sqlwc };
    let pg_workers = if cfg.server.pg_addr.is_empty() { 0 } else { sqlwc };
    // ⭐ H1: HTTP REST 门面 1 worker
    let http_workers = if cfg.server.http_addr.is_empty() { 0 } else { 1 };
    let opts = ShardManagerOptions {
        num_shards: cfg.storage.num_shards,
        block_root: cfg.storage.block_root.clone(),
        create_if_missing: cfg.storage.create_if_missing,
        io_backend,
        io_config: storage::IoBackendConfig {
            backend: io_backend,
            ..Default::default()
        },
        chunk_cache_size: cfg.storage.chunk_cache_size,
        reply_bus_count: Some(
            (binary_workers + resp_workers + sql_workers + pg_workers + http_workers)
                .max(cfg.storage.num_shards),
        ),
        // ⭐ WAL (F60): off | periodic (默认) | strict (validate 已挡非法值)
        wal_mode: storage::wal::WalMode::parse(&cfg.storage.wal_mode)
            .unwrap_or_default(),
    };
    let mgr = match ShardManager::open(opts) {
        Ok(m) => Arc::new(m),
        Err(e) => {
            nlog::error!("main", "shard manager open failed: {e}");
            nlog::shutdown();
            std::process::exit(1);
        }
    };

    // 5. 幂等确保默认 db/table 存在 (已存在的错误容忍)
    ensure_catalog(&mgr, &cfg.storage.default_db, &cfg.storage.default_table);
    // ⭐ D3 (分库): 预建 db1..dbN (id 1..N) + 各自的 default_table,
    // 供 RESP `SELECT n` 直接使用 (幂等; 建库 2PC 仅启动时一次)
    for n in 1..=cfg.storage.precreate_dbs {
        ensure_catalog(&mgr, &format!("db{n}"), &cfg.storage.default_table);
    }

    // 6. 启动网络层 (Binary + 可选 RESP)
    let limits = KvLimits {
        max_key_bytes: cfg.server.max_key_bytes,
        max_value_bytes: cfg.server.max_value_bytes,
    };
    // ⭐ ORM-B2: 进程级共享 SQL 路由缓存 (五门面同集群共用一个)
    let sql_shared = network::new_sql_shared();
    let listen_addr = cfg.server.listen_addr.parse().expect("validated addr");
    let server = match NetworkServer::start(NetworkServerConfig {
        listen_addr,
        shard_manager: mgr.clone(),
        worker_count: binary_workers,
        default_db: cfg.storage.default_db.clone(),
        default_table: cfg.storage.default_table.clone(),
        inbox_capacity: 1024,
        protocol: ProtocolKind::Binary,
        limits,
        auth_password: None,
        worker_id_base: 0,
            sql_shared: sql_shared.clone(),
    }) {
        Ok(s) => s,
        Err(e) => {
            nlog::error!("main", "network server start failed: {e}");
            nlog::shutdown();
            std::process::exit(1);
        }
    };

    // 6b. RESP (Redis 兼容) 门面
    let resp_server = if resp_enabled {
        let redis_addr = cfg.server.redis_addr.parse().expect("validated redis addr");
        let auth_password = if cfg.server.redis_password.is_empty() {
            None
        } else {
            Some(cfg.server.redis_password.clone())
        };
        match NetworkServer::start(NetworkServerConfig {
            listen_addr: redis_addr,
            shard_manager: mgr.clone(),
            worker_count: resp_workers,
            default_db: cfg.storage.default_db.clone(),
            default_table: cfg.storage.default_table.clone(),
            inbox_capacity: 1024,
            protocol: ProtocolKind::Resp,
            limits,
            auth_password,
            worker_id_base: binary_workers as u32,
            sql_shared: sql_shared.clone(),
        }) {
            Ok(s) => {
                nlog::info!("main", "RESP (Redis) listening on {}", s.local_addr());
                Some(s)
            }
            Err(e) => {
                nlog::error!("main", "RESP server start failed: {e}");
                nlog::shutdown();
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    // 6c. ⭐ Z2: SQL 门面 (MySQL wire protocol, mysql cli 直连)
    let sql_enabled = !cfg.server.sql_addr.is_empty();
    let sql_server = if sql_enabled {
        let sql_addr = cfg.server.sql_addr.parse().expect("validated sql addr");
        // 空密码 = 免密登录
        let sql_password = if cfg.server.sql_password.is_empty() {
            None
        } else {
            Some(cfg.server.sql_password.clone())
        };
        match NetworkServer::start(NetworkServerConfig {
            listen_addr: sql_addr,
            shard_manager: mgr.clone(),
            worker_count: sqlwc,
            default_db: cfg.storage.default_db.clone(),
            default_table: cfg.storage.default_table.clone(),
            inbox_capacity: 1024,
            protocol: ProtocolKind::Sql,
            limits,
            auth_password: sql_password,
            worker_id_base: (binary_workers + resp_workers) as u32,
            sql_shared: sql_shared.clone(),
        }) {
            Ok(s) => {
                nlog::info!("main", "SQL (MySQL wire) listening on {}", s.local_addr());
                Some(s)
            }
            Err(e) => {
                nlog::error!("main", "SQL server start failed: {e}");
                nlog::shutdown();
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    // 6d. ⭐ S4: PostgreSQL wire 门面 (psql 直连; 密码复用 sql_password)
    let pg_enabled = !cfg.server.pg_addr.is_empty();
    let pg_server = if pg_enabled {
        let pg_addr = cfg.server.pg_addr.parse().expect("validated pg addr");
        let pg_password = if cfg.server.sql_password.is_empty() {
            None
        } else {
            Some(cfg.server.sql_password.clone())
        };
        match NetworkServer::start(NetworkServerConfig {
            listen_addr: pg_addr,
            shard_manager: mgr.clone(),
            worker_count: sqlwc,
            default_db: cfg.storage.default_db.clone(),
            default_table: cfg.storage.default_table.clone(),
            inbox_capacity: 1024,
            protocol: ProtocolKind::Pg,
            limits,
            auth_password: pg_password,
            worker_id_base: (binary_workers + resp_workers + sql_workers) as u32,
            sql_shared: sql_shared.clone(),
        }) {
            Ok(s) => {
                nlog::info!("main", "SQL (PostgreSQL wire) listening on {}", s.local_addr());
                Some(s)
            }
            Err(e) => {
                nlog::error!("main", "PG server start failed: {e}");
                nlog::shutdown();
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    // 6e. ⭐ H1: HTTP REST 门面 (JSON + CORS + /metrics)
    network::metrics::init_start_time();
    let http_enabled = !cfg.server.http_addr.is_empty();
    let http_server = if http_enabled {
        let http_addr = cfg.server.http_addr.parse().expect("validated http addr");
        network::http_config::set_cors_origin(Some(cfg.server.http_cors_origin.clone()));
        let http_token = if cfg.server.http_token.is_empty() {
            None
        } else {
            Some(cfg.server.http_token.clone())
        };
        match NetworkServer::start(NetworkServerConfig {
            listen_addr: http_addr,
            shard_manager: mgr.clone(),
            worker_count: 1,
            default_db: cfg.storage.default_db.clone(),
            default_table: cfg.storage.default_table.clone(),
            inbox_capacity: 1024,
            protocol: ProtocolKind::Http,
            limits,
            auth_password: http_token, // = Bearer token
            worker_id_base: (binary_workers + resp_workers + sql_workers + pg_workers) as u32,
            sql_shared: sql_shared.clone(),
        }) {
            Ok(s) => {
                nlog::info!("main", "REST (HTTP) listening on {}", s.local_addr());
                Some(s)
            }
            Err(e) => {
                nlog::error!("main", "HTTP server start failed: {e}");
                nlog::shutdown();
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    nlog::info!(
        "main",
        "NexusDB listening on {} | shards={} workers={} io={:?} data={}",
        server.local_addr(),
        cfg.storage.num_shards,
        cfg.server.worker_count,
        io_backend,
        cfg.storage.block_root.display()
    );

    // 7. 等信号
    while !SHUTDOWN.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(100));
    }
    nlog::info!("main", "shutdown signal received, stopping...");

    // 8. 优雅退出: network (五协议) → shards → log
    if let Some(hs) = http_server
        && let Err(e) = hs.shutdown()
    {
        nlog::warn!("main", "HTTP server shutdown error: {e}");
    }
    if let Some(ps) = pg_server
        && let Err(e) = ps.shutdown()
    {
        nlog::warn!("main", "PG server shutdown error: {e}");
    }
    if let Some(ss) = sql_server
        && let Err(e) = ss.shutdown()
    {
        nlog::warn!("main", "SQL server shutdown error: {e}");
    }
    if let Some(rs) = resp_server
        && let Err(e) = rs.shutdown()
    {
        nlog::warn!("main", "RESP server shutdown error: {e}");
    }
    if let Err(e) = server.shutdown() {
        nlog::warn!("main", "network shutdown error: {e}");
    }
    match Arc::try_unwrap(mgr) {
        Ok(m) => {
            if let Err(e) = m.close() {
                nlog::warn!("main", "shard manager close error: {e}");
            }
        }
        Err(_) => nlog::warn!("main", "shard manager still referenced, skip close"),
    }
    // ⭐ 探针 dump: NLOG_PROBE=1 时各阶段耗时直方图输出到 stderr.
    // 客户端可直接 `2>probe.log` 捕获, 用于长尾定位.
    // (放在 nlog shutdown 之前, 避免被日志 flush 干扰; 进程退出前自动 flush.)
    let dump = shard_manager::PROBE.dump_all();
    if dump.trim() != "(probes disabled, set NLOG_PROBE=1 to enable)" {
        eprintln!("{dump}");
    }
    nlog::info!("main", "NexusDB stopped");

    nlog::shutdown();
}

/// 启动时确保默认 db/table 存在. "already exists" 类错误视为成功 (幂等).
fn ensure_catalog(mgr: &ShardManager, db: &str, table: &str) {
    match mgr.create_db(db) {
        Ok(()) => nlog::info!("main", "created db {db:?}"),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("already exists") {
                nlog::debug!("main", "db {db:?} already exists");
            } else {
                nlog::warn!("main", "create_db({db:?}) failed: {msg}");
            }
        }
    }
    match mgr.create_table(db, table) {
        Ok(_) => nlog::info!("main", "created table {db:?}.{table:?}"),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("already exists") {
                nlog::debug!("main", "table {db:?}.{table:?} already exists");
            } else {
                nlog::warn!("main", "create_table({db:?}.{table:?}) failed: {msg}");
            }
        }
    }
}
