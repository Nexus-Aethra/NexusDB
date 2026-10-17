//! ⭐ Drop RAII 回归: `drop(mgr)` (忘记显式 close) 必须停掉 shard 线程.
//!
//! 背景: ShardManager 无 Drop 时, 只 `drop(mgr)` 的测试会泄漏 num_shards 个
//! shard 线程 — 它们 idle 循环 10ms poll 醒来驱动持久化检查, 持续烧 CPU 直到
//! 进程退出. 并行测试套件里多个泄漏叠加会拖垮机器并诱发时序性 hang.
//!
//! 本文件是独立测试二进制 (独立进程), /proc 线程计数不受其他测试干扰;
//! 全部断言在同一 #[test] 内顺序执行, 不受并行度影响.

#![cfg(target_os = "linux")]

use std::time::{Duration, Instant};

use shard_manager::{ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

fn count_shard_threads() -> usize {
    let mut n = 0;
    if let Ok(entries) = std::fs::read_dir("/proc/self/task") {
        for e in entries.flatten() {
            let tid = e.file_name().to_string_lossy().into_owned();
            if let Ok(name) = std::fs::read_to_string(format!("/proc/self/task/{tid}/comm"))
                && name.trim().starts_with("shard-")
            {
                n += 1;
            }
        }
    }
    n
}

fn opts(root: std::path::PathBuf, num_shards: usize) -> ShardManagerOptions {
    ShardManagerOptions {
        num_shards,
        block_root: root,
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 4,
        reply_bus_count: None,
        wal_mode: Default::default(),
    }
}

fn wait_shard_threads_gone(before: usize) {
    // join 在 Drop 内同步完成, drop 返回即线程已退出; 轮询仅作兜底.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let now = count_shard_threads();
        assert!(
            now >= before,
            "shard 线程数不应低于基线 (now={now}, before={before})"
        );
        if now == before {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "drop 后 shard 线程未退出 (泄漏 {now} > 基线 {before}) — Drop RAII 失效"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn drop_mgr_joins_shard_threads_and_close_is_idempotent() {
    let before = count_shard_threads();

    // 1. 只 drop (不显式 close): Drop RAII 必须停线程 + join.
    {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = ShardManager::open(opts(tmp.path().to_path_buf(), 3)).expect("open");
        mgr.create_db("app").expect("create db");
        mgr.create_table("app", "kv").expect("create table");
        mgr.put("app", "kv", b"k", b"v", 0).expect("put");
        assert_eq!(
            count_shard_threads(),
            before + 3,
            "open 后应有 3 个 shard 线程"
        );
        drop(mgr);
        wait_shard_threads_gone(before);
    }

    // 2. 显式 shutdown() 后再 drop: 幂等 no-op, 不得重复 join / panic.
    {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = ShardManager::open(opts(tmp.path().to_path_buf(), 2)).expect("open");
        mgr.create_db("app").expect("create db");
        mgr.shutdown().expect("shutdown");
        drop(mgr); // Drop 此时应为 no-op (shutdown_started 已置位)
        wait_shard_threads_gone(before);
    }
}
