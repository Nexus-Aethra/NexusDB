//! ⭐ 审计新 P1-18 回归: shutdown(&self) 必须在仍有其他 Arc 引用时也完成
//! final flush — 旧退出路径 `Arc::try_unwrap(mgr)` 因 cluster_ctl 等持有
//! 克隆必失败, close 被跳过, shard 被硬杀, nowchunks 脏数据每次关机都丢.
//!
//! wal_mode=Off 隔离 nowchunks: 无 WAL 兜底, 数据唯一来源就是 shutdown 的
//! flush — 修复前此测试必失败 (reopen 后 key 丢失).

use std::sync::Arc;

use shard_manager::{ShardManager, ShardManagerOptions};
use storage::wal::WalMode;
use storage::{IoBackend, IoBackendConfig};

fn opts(root: std::path::PathBuf, create: bool) -> ShardManagerOptions {
    ShardManagerOptions {
        num_shards: 1,
        block_root: root,
        create_if_missing: create,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 4,
        reply_bus_count: None,
        wal_mode: WalMode::Off,
    }
}

#[test]
fn shutdown_with_held_arc_flushes_data() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    {
        let mgr = Arc::new(ShardManager::open(opts(root.clone(), true)).expect("open"));
        mgr.create_db("app").expect("create db");
        mgr.create_table("app", "kv").expect("create table");
        for i in 0..100u32 {
            let key = format!("k{i:04}");
            mgr.put("app", "kv", key.as_bytes(), b"v", 0).expect("put");
        }
        // 模拟 main.rs 退出时的真实持有关系 (sql_shared.cluster_ctl 等):
        // 旧路径 Arc::try_unwrap 在此必失败 → close 被跳过 → 数据丢失.
        let held = mgr.clone();
        // 新路径: 经 &self 关闭 — flush + Shutdown, 不要求独占所有权.
        mgr.shutdown()
            .expect("shutdown must flush with outstanding Arc refs");
        drop(held);
        // main.rs 同款收尾: 能独占时 join 线程 (flush 已完成, join 仅清理).
        match Arc::try_unwrap(mgr) {
            Ok(m) => m.close().expect("close"),
            Err(_) => panic!("held 已 drop, 应能独占"),
        }
    }
    // reopen: wal=off, 数据唯一来源是 shutdown 的 flush
    let mgr2 = ShardManager::open(opts(root, false)).expect("reopen");
    for i in 0..100u32 {
        let key = format!("k{i:04}");
        assert_eq!(
            mgr2.get("app", "kv", key.as_bytes(), 0).unwrap().as_deref(),
            Some(b"v".as_ref()),
            "key #{i} 在 shutdown 后丢失 (P1-18)"
        );
    }
    mgr2.close().expect("close2");
}
