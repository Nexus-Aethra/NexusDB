//! 回归测试 (2026-10-16, 审计新 P0-1): 长共享前缀 key 把 internal 页推到近满时,
//! `internal_insert` 的 `pre_split_segment` 曾无空间预检 ——
//! - `copy_within` 越界 panic (合法插入打崩进程), 或
//! - free_off 写穿 checkpoint 区, 且 push_back 失败路径不回滚 →
//!   污染页被 btree split 路径 (btree.rs:668-696) 拆分后**持久化** → 静默丢数据.
//!
//! 与 2026-08-02 冷启动丢数据事故 (docs/bug-cold-start-data-loss.md) 同根因:
//! 当年只修了 leaf_insert, internal_insert 漏修.
//!
//! 触发构造: 600B 共享前缀 key (internal 页中段首 item 重编码为 full key 时
//! 单次膨胀 ~600B) + 2KB value (加快 leaf 分裂, 用较少 key 数把 internal 页推到近满).

use storage::{IoBackend, IoBackendConfig, OpenOptions, StorageEngine};

mod common;

use common::run_async;

fn setup() -> (tempfile::TempDir, OpenOptions) {
    let tmp = tempfile::tempdir().unwrap();
    let opts = OpenOptions {
        block_root: tmp.path().to_path_buf(),
        block_dir: Some(tmp.path().to_path_buf()),
        db_name: None,
        shard_id: 0,
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 8, // 大一些, 减少 flush 干扰 (与 multi_level_btree_e2e 一致)
        wal_mode: Default::default(),
    };
    (tmp, opts)
}

const N: u32 = 8000;

/// 600B 共享前缀 + 8 位序号 (升序) — 相邻 key 共享 600B.
fn key(i: u32) -> Vec<u8> {
    let mut k = vec![b'a'; 600];
    k.extend_from_slice(format!("{i:08}").as_bytes());
    k
}

/// 2KB value, 头部带序号便于识别错值.
fn val(i: u32) -> Vec<u8> {
    let mut v = vec![b'v'; 2048];
    v[..8].copy_from_slice(format!("{i:08}").as_bytes());
    v
}

#[test]
fn internal_split_long_shared_prefix_no_data_loss() {
    run_async(async move {
        let (_tmp, opts) = setup();
        let mut e = StorageEngine::open(opts.clone()).await.unwrap();
        e.create_db("db").await.unwrap();
        e.create_table("db", "t").await.unwrap();

        for i in 0..N {
            e.table_put("db", "t", &key(i), &val(i)).await.unwrap();
        }
        // 全量读回: 修复前 internal 页污染会在此表现为读失败/读错/panic
        for i in 0..N {
            let got = e.table_get("db", "t", &key(i)).await.unwrap();
            assert_eq!(got, Some(val(i)), "key #{i} 读回失败/不一致");
        }
        e.flush().await.unwrap();
        drop(e);

        // reopen 后全量读回: 污染页一旦落盘, 重启后依然丢
        let mut opts2 = opts;
        opts2.create_if_missing = false;
        let mut e2 = StorageEngine::open(opts2).await.unwrap();
        for i in 0..N {
            let got = e2.table_get("db", "t", &key(i)).await.unwrap();
            assert_eq!(got, Some(val(i)), "reopen 后 key #{i} 读回失败/不一致");
        }
        drop(_tmp);
    });
}
