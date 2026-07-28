//! ⭐ Phase R: 范围扫描游标测试 (btree_scan 跨 leaf + 前缀精确 + Break 早停).
//!
//! 用 engine 插入定长 String key (编码 `[S][klen][key]`), 按 `[S][klen]` 前缀
//! 扫描: 等长 key 共享该前缀且在 BTree 中连续 → 精确圈选; 不同长度的 decoy
//! key (不同 klen 字节) 天然排除. 400 key 跨多 leaf, 验证跨 leaf 游标推进.

use std::ops::ControlFlow;

use storage::engine::OpenOptions;
use storage::{IoBackend, IoBackendConfig, StorageEngine};

mod common;

use common::run_async;

fn opts_for(tmp: &tempfile::TempDir) -> OpenOptions {
    OpenOptions {
        block_root: tmp.path().to_path_buf(),
        block_dir: None,
        db_name: Some("default".to_string()),
        shard_id: 0,
        create_if_missing: true,
        chunk_cache_size: 4,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
    }
}

#[test]
fn btree_scan_cross_leaf_and_prefix_exact() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut engine = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        engine.create_db("db1").await.unwrap();
        engine.create_table("db1", "t1").await.unwrap();

        // 400 个定长 6 字节 key "kNNNNN" (跨多 leaf)
        for i in 0..400u32 {
            let k = format!("k{i:05}");
            let v = format!("v{i}");
            engine
                .table_put("db1", "t1", k.as_bytes(), v.as_bytes())
                .await
                .unwrap();
        }
        // decoy: 不同长度 key (不同 klen 字节, 不应被 [S][6] 前缀命中)
        engine.table_put("db1", "t1", b"xx", b"d1").await.unwrap(); // len 2
        engine
            .table_put("db1", "t1", b"loooooooong", b"d2")
            .await
            .unwrap(); // len 11

        let root = engine.open_table("db1", "t1").await.unwrap().unwrap();

        // [S][varint klen=6] — 6 字节 key 的公共前缀 (varint(6) = 单字节 0x06)
        let prefix = [storage::keyspace::KIND_STRING, 6u8];

        // 全扫: 精确 400 个, 顺序递增
        let mut collected: Vec<Vec<u8>> = Vec::new();
        storage::registry::table_scan_prefix(engine.pager_mut(), root, &prefix, &mut |k, _v| {
            collected.push(k.to_vec());
            ControlFlow::Continue(())
        })
        .await
        .unwrap();
        assert_eq!(collected.len(), 400, "应精确扫到 400 个 6 字节 key (decoy 排除)");
        // 物理 key 严格递增 (扫描有序保证)
        for w in collected.windows(2) {
            assert!(w[0] < w[1], "扫描结果必须有序递增");
        }
        // 每个都以 [S][6] 开头且解出的 user key 是 6 字节
        for k in &collected {
            assert!(k.starts_with(&prefix));
            assert_eq!(k.len(), 2 + 6, "physical = [S][klen][6B key]");
        }

        // Break 早停: 只收前 50 个
        let mut n = 0usize;
        storage::registry::table_scan_prefix(engine.pager_mut(), root, &prefix, &mut |_k, _v| {
            n += 1;
            if n >= 50 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .await
        .unwrap();
        assert_eq!(n, 50, "Break 应在第 50 项立即停止");
    });
}

#[test]
fn btree_scan_empty_prefix_range() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut engine = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        engine.create_db("db1").await.unwrap();
        engine.create_table("db1", "t1").await.unwrap();
        engine.table_put("db1", "t1", b"aaa", b"1").await.unwrap();

        let root = engine.open_table("db1", "t1").await.unwrap().unwrap();
        // 不存在的前缀 (不同 kind 字节) → 零命中
        let mut hits = 0usize;
        storage::registry::table_scan_prefix(engine.pager_mut(), root, b"ZZZ", &mut |_k, _v| {
            hits += 1;
            ControlFlow::Continue(())
        })
        .await
        .unwrap();
        assert_eq!(hits, 0);
    });
}
