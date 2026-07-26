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
    // 双协议 server 并存时 worker_id 空间不重叠, reply_bus 总数 = 两者 worker 之和
    let resp_enabled = !cfg.server.redis_addr.is_empty();
    let binary_workers = cfg.server.worker_count;
    let resp_workers = if resp_enabled { cfg.server.worker_count } else { 0 };
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
            (binary_workers + resp_workers).max(cfg.storage.num_shards),
        ),
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

    // 6. 启动网络层 (Binary + 可选 RESP)
    let limits = KvLimits {
        max_key_bytes: cfg.server.max_key_bytes,
        max_value_bytes: cfg.server.max_value_bytes,
    };
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

    // 8. 优雅退出: network (双协议) → shards → log
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
