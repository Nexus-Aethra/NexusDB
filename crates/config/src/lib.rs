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
    /// key 长度上限 (字节). 超限请求在协议层拦截.
    pub max_key_bytes: usize,
    /// value 长度上限 (字节). 受 page 编码缓冲限制:
    /// max_key_bytes + max_value_bytes 必须 <= 4060 (4096 栈缓冲 - 编码开销).
    pub max_value_bytes: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:5433".to_string(),
            worker_count: 2,
            redis_addr: "0.0.0.0:6379".to_string(),
            redis_password: String::new(),
            max_key_bytes: 1024,
            max_value_bytes: 3000,
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
        if self.server.max_key_bytes == 0 || self.server.max_value_bytes == 0 {
            return Err("server.max_key_bytes / max_value_bytes must be >= 1".to_string());
        }
        // page crate 编码路径 4096B 栈缓冲: key+value+tag+varint 开销必须装得下
        if self.server.max_key_bytes + self.server.max_value_bytes > 4060 {
            return Err(
                "server.max_key_bytes + max_value_bytes must be <= 4060 (page item encode limit)"
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
            "io_uring" | "iouring" => Ok(storage::IoBackend::IoUring),
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
