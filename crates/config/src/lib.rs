//! NexusDB 配置模块: TOML 解析 + 默认值 + 校验.
//!
//! 所有 section / 字段均可省略, 缺省时用 `Default` 值.
//! `NexusConfig::load_or_default` 在文件不存在时返回默认配置
//! (caller 据 `from_file` 决定是否打 warn).

use std::io;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// 顶层配置.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct NexusConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub log: LogConfig,
}

/// ⭐ S4: pg_addr 缺省 (旧配置文件无此字段时兼容).
fn default_pg_addr() -> String {
    "0.0.0.0:5435".to_string()
}

/// ⭐ H1: http_addr 缺省 (用户拍板 6778, 避开 8080).
fn default_http_addr() -> String {
    "0.0.0.0:6778".to_string()
}

/// ⭐ ORM-B3: SQL 门面 worker 数缺省.
fn default_sql_worker_count() -> usize {
    1
}

/// `[server]` 网络服务配置.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// TCP 监听地址.
    pub listen_addr: String,
    /// epoll worker 线程数.
    pub worker_count: usize,
    /// RESP (Redis 兼容) 监听地址. 空字符串 = 禁用 RESP 门面.
    pub redis_addr: String,
    /// RESP AUTH 密码. 空字符串 = 不启用认证.
    pub redis_password: String,
    /// ⭐ Y2: SQL 门面监听地址 (MySQL wire protocol).
    /// 空字符串 = 禁用 SQL 门面.
    pub sql_addr: String,
    /// ⭐ S4: PostgreSQL wire 门面监听地址 (psql 直连).
    /// 空字符串 = 禁用; 默认 5435 (5432 留给系统 PG, 可改).
    #[serde(default = "default_pg_addr")]
    pub pg_addr: String,
    /// ⭐ Z2: SQL 门面登录密码 (mysql_native_password).
    /// 空字符串 = 免密 (任意用户名放行).
    pub sql_password: String,
    /// ⭐ ORM-B3: MySQL/PG 门面 worker 数 (默认 1; ORM 连接池并发场景可调
    /// 2-8 — 路由缓存已进程级共享, 多 worker 正确性成立).
    #[serde(default = "default_sql_worker_count")]
    pub sql_worker_count: usize,
    /// ⭐ H1: HTTP REST 门面监听地址. 空 = 禁用; 默认 6778 (避开 8080 撞车区).
    #[serde(default = "default_http_addr")]
    pub http_addr: String,
    /// ⭐ H1: CORS Access-Control-Allow-Origin (`*` 或具体 origin; 空 = 不发 CORS 头).
    #[serde(default)]
    pub http_cors_origin: String,
    /// ⭐ H1: REST Bearer token (空 = 免鉴权; /metrics 与 /v1/status 恒免).
    #[serde(default)]
    pub http_token: String,
    /// ⭐ F83: SQL 门面 TLS 证书 PEM 路径 (空 = 不启用 TLS, 明文). 两门面共用.
    #[serde(default)]
    pub tls_cert: String,
    /// ⭐ F83: SQL 门面 TLS 私钥 PEM 路径 (PKCS8/PKCS1/SEC1; 空 = 不启用).
    #[serde(default)]
    pub tls_key: String,
    /// key 长度上限 (字节). 超限请求在协议层拦截.
    pub max_key_bytes: usize,
    /// value 长度上限 (字节). ⭐ 大 value: 超过 inline 阈值 (~4000B) 的
    /// value 由存储层自动切溢出页 (单层间接), 上限 1MB.
    pub max_value_bytes: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:5433".to_string(),
            worker_count: 2,
            redis_addr: "0.0.0.0:6379".to_string(),
            redis_password: String::new(),
            sql_addr: "0.0.0.0:5434".to_string(),
            pg_addr: default_pg_addr(),
            sql_password: String::new(),
            sql_worker_count: 1,
            http_addr: default_http_addr(),
            http_cors_origin: String::new(),
            http_token: String::new(),
            tls_cert: String::new(),
            tls_key: String::new(),
            max_key_bytes: 1024,
            max_value_bytes: 1024 * 1024,
        }
    }
}

/// `[storage]` 存储引擎配置.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// 数据根目录. 每 shard 独立 `{block_root}/shard_{N}/`.
    pub block_root: PathBuf,
    /// shard 数.
    pub num_shards: usize,
    /// IO 后端: "stdfs" | "io_uring".
    pub io_backend: String,
    /// 每 shard 的 chunk LRU 容量.
    pub chunk_cache_size: usize,
    /// 目录不存在时自动创建.
    pub create_if_missing: bool,
    /// 启动时确保存在的默认 db.
    pub default_db: String,
    /// 启动时确保存在的默认 table.
    pub default_table: String,
    /// ⭐ D3 (分库): 启动时预建 `db1..dbN` (id 1..N), 供 RESP `SELECT n` 直用.
    /// 0 = 不预建 (只有 default db, id 0). 建库走 2PC, 仅启动时一次.
    pub precreate_dbs: usize,
    /// ⭐ WAL (F60): 预写日志档位 — "off" | "periodic" (默认, 每秒 fsync,
    /// 丢失窗口 ~1s) | "strict" (回复前 fsync + 组提交, crash 零丢失).
    #[serde(default = "default_wal_mode")]
    pub wal_mode: String,
}

/// ⭐ WAL (F60): 档位缺省.
fn default_wal_mode() -> String {
    "periodic".to_string()
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            block_root: PathBuf::from("./data"),
            num_shards: 6,
            io_backend: "io_uring".to_string(),
            chunk_cache_size: 16,
            create_if_missing: true,
            default_db: "default".to_string(),
            default_table: "default".to_string(),
            wal_mode: default_wal_mode(),
            precreate_dbs: 0,
        }
    }
}

/// `[log]` 日志配置.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    /// 级别: error|warn|info|debug|trace.
    pub level: String,
    /// 日志目录. 空字符串 = 仅 stderr, 不写文件.
    pub dir: String,
    /// 累积量阈值 (KB), 达到即触发落盘.
    pub buffer_kb: usize,
    /// 时间阈值 (ms), 距上次落盘超时且缓冲非空即触发.
    pub flush_interval_ms: u64,
    /// Error/Warn 是否直通 stderr.
    pub stderr: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            dir: "./logs".to_string(),
            buffer_kb: 64,
            flush_interval_ms: 500,
            stderr: true,
        }
    }
}

impl NexusConfig {
    /// 从 TOML 文件加载. 文件不存在 → (默认配置, from_file=false);
    /// 文件存在但解析/校验失败 → Err.
    pub fn load_or_default(path: &Path) -> io::Result<(Self, bool)> {
        if !path.exists() {
            return Ok((Self::default(), false));
        }
        let text = std::fs::read_to_string(path)?;
        let cfg: NexusConfig = toml::from_str(&text)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("parse {}: {e}", path.display())))?;
        cfg.validate()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok((cfg, true))
    }

    /// 校验字段合法性 (io_backend / level / 数值范围).
    pub fn validate(&self) -> Result<(), String> {
        self.storage.io_backend()?;
        match self.log.level.to_ascii_lowercase().as_str() {
            "error" | "warn" | "info" | "debug" | "trace" => {}
            other => return Err(format!("log.level invalid: {other:?} (expect error|warn|info|debug|trace)")),
        }
        if !matches!(self.storage.wal_mode.to_ascii_lowercase().as_str(), "off" | "periodic" | "strict" | "") {
            return Err(format!("storage.wal_mode invalid: {} (off|periodic|strict)", self.storage.wal_mode));
        }
        if self.storage.num_shards == 0 {
            return Err("storage.num_shards must be >= 1".to_string());
        }
        if self.server.worker_count == 0 {
            return Err("server.worker_count must be >= 1".to_string());
        }
        self.server
            .listen_addr
            .parse::<std::net::SocketAddr>()
            .map_err(|e| format!("server.listen_addr invalid: {e}"))?;
        if !self.server.redis_addr.is_empty() {
            self.server
                .redis_addr
                .parse::<std::net::SocketAddr>()
                .map_err(|e| format!("server.redis_addr invalid: {e}"))?;
        }
        if !self.server.sql_addr.is_empty() {
            self.server
                .sql_addr
                .parse::<std::net::SocketAddr>()
                .map_err(|e| format!("server.sql_addr invalid: {e}"))?;
        }
        if !self.server.pg_addr.is_empty() {
            self.server
                .pg_addr
                .parse::<std::net::SocketAddr>()
                .map_err(|e| format!("server.pg_addr invalid: {e}"))?;
        }
        if !self.server.http_addr.is_empty() {
            self.server
                .http_addr
                .parse::<std::net::SocketAddr>()
                .map_err(|e| format!("server.http_addr invalid: {e}"))?;
        }
        if self.server.max_key_bytes == 0 || self.server.max_value_bytes == 0 {
            return Err("server.max_key_bytes / max_value_bytes must be >= 1".to_string());
        }
        // key 参与 page item 比较/分裂/internal 路由, 维持 inline 上限
        if self.server.max_key_bytes > 1024 {
            return Err("server.max_key_bytes must be <= 1024 (page item routing limit)".to_string());
        }
        // ⭐ 大 value: 超 inline 阈值走溢出页 (单层间接), 上限 1MB
        if self.server.max_value_bytes > 1024 * 1024 {
            return Err(
                "server.max_value_bytes must be <= 1048576 (overflow single-level limit)"
                    .to_string(),
            );
        }
        Ok(())
    }
}

impl StorageConfig {
    /// io_backend 字符串 → storage::IoBackend.
    pub fn io_backend(&self) -> Result<storage::IoBackend, String> {
        match self.io_backend.to_ascii_lowercase().as_str() {
            "stdfs" => Ok(storage::IoBackend::StdFs),
            "io_uring" | "iouring" if platform::storage_backend_supported(&self.io_backend) => {
                Ok(storage::IoBackend::IoUring)
            }
            "io_uring" | "iouring" => Err(format!(
                "storage.io_backend=io_uring is unsupported on {:?}; use stdfs or select that target's native backend",
                platform::CURRENT.target
            )),
            other => Err(format!("storage.io_backend invalid: {other:?} (expect stdfs|io_uring)")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_toml_parses() {
        let text = r#"
[server]
listen_addr = "127.0.0.1:9999"
worker_count = 4

[storage]
block_root = "/tmp/nx"
num_shards = 8
io_backend = "stdfs"
chunk_cache_size = 32
create_if_missing = false
default_db = "app"
default_table = "kv"

[log]
level = "debug"
dir = ""
buffer_kb = 128
flush_interval_ms = 100
stderr = false
"#;
        let cfg: NexusConfig = toml::from_str(text).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.server.listen_addr, "127.0.0.1:9999");
        assert_eq!(cfg.server.worker_count, 4);
        assert_eq!(cfg.storage.num_shards, 8);
        assert!(matches!(cfg.storage.io_backend().unwrap(), storage::IoBackend::StdFs));
        assert!(!cfg.storage.create_if_missing);
        assert_eq!(cfg.log.level, "debug");
        assert_eq!(cfg.log.buffer_kb, 128);
        assert!(!cfg.log.stderr);
    }

    #[test]
    fn partial_toml_falls_back_to_defaults() {
        let text = r#"
[storage]
num_shards = 2
"#;
        let cfg: NexusConfig = toml::from_str(text).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.storage.num_shards, 2);
        // 其余字段用默认值
        assert_eq!(cfg.server.listen_addr, "0.0.0.0:5433");
        assert_eq!(cfg.storage.io_backend, "io_uring");
        assert_eq!(cfg.log.level, "info");
    }

    #[test]
    fn invalid_io_backend_rejected() {
        let text = r#"
[storage]
io_backend = "epoll"
"#;
        let cfg: NexusConfig = toml::from_str(text).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn invalid_level_rejected() {
        let text = r#"
[log]
level = "verbose"
"#;
        let cfg: NexusConfig = toml::from_str(text).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn missing_file_returns_default() {
        let (cfg, from_file) =
            NexusConfig::load_or_default(Path::new("/nonexistent/nexusdb.toml")).unwrap();
        assert!(!from_file);
        assert_eq!(cfg.storage.num_shards, 6);
    }

    #[test]
    fn load_from_real_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("n.toml");
        std::fs::write(&path, "[server]\nlisten_addr = \"127.0.0.1:0\"\n").unwrap();
        let (cfg, from_file) = NexusConfig::load_or_default(&path).unwrap();
        assert!(from_file);
        assert_eq!(cfg.server.listen_addr, "127.0.0.1:0");
    }
}
