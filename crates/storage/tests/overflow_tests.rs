//! ⭐ 大 value 溢出页 e2e 测试 (write/read/free/reopen 防泄漏链路).
//!
//! 覆盖:
//! - inline/overflow 阈值边界 roundtrip
//! - 1MB value 写读一致
//! - **防泄漏**: 覆盖写 N 次后活页数不增长 (旧链逐次释放);
//!   delete 后活页数回落
//! - **防复活**: 释放墓碑持久化, reopen 后 recover 扫描不回填死页
//!   (磁盘残留的旧溢出页 header 不得复活)

use storage::engine::OpenOptions;
use storage::{IoBackend, IoBackendConfig, PID_ALIVE, StorageEngine};

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
        wal_mode: Default::default(),
    }
}

async fn setup_table(engine: &mut StorageEngine) {
    engine.create_db("db1").await.unwrap();
    engine.create_table("db1", "t1").await.unwrap();
}

/// value 生成: 可校验的伪随机字节 (seed 决定内容).
fn make_value(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

/// meta 中活页 slot 数 (排除墓碑) — 防泄漏断言的观测量.
fn alive_slots(engine: &mut StorageEngine) -> usize {
    engine
        .pager_mut()
        .meta_debug_iter()
        .iter()
        .filter(|(_, p)| p.flags() & PID_ALIVE != 0)
        .count()
}

// =====================================================================
// roundtrip
// =====================================================================

#[test]
fn threshold_boundary_roundtrip() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut engine = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        setup_table(&mut engine).await;

        // 阈值下 (inline) 与阈值上 (overflow) 各一, key 3B
        let small = make_value(storage::INLINE_LIMIT - 3, 0x11);
        let large = make_value(storage::INLINE_LIMIT - 2, 0x22);
        engine.table_put("db1", "t1", b"sml", &small).await.unwrap();
        engine.table_put("db1", "t1", b"lrg", &large).await.unwrap();

        assert_eq!(
            engine.table_get("db1", "t1", b"sml").await.unwrap().as_deref(),
            Some(&small[..])
        );
        assert_eq!(
            engine.table_get("db1", "t1", b"lrg").await.unwrap().as_deref(),
            Some(&large[..])
        );
    });
}

#[test]
fn one_megabyte_roundtrip() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut engine = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        setup_table(&mut engine).await;

        let v = make_value(storage::MAX_OVERFLOW_VALUE, 0x5A); // 1MB
        engine.table_put("db1", "t1", b"big", &v).await.unwrap();
        let got = engine.table_get("db1", "t1", b"big").await.unwrap().unwrap();
        assert_eq!(got.len(), v.len());
        assert_eq!(got, v, "1MB roundtrip 逐字节一致");
    });
}

// =====================================================================
// ⭐ 防泄漏: 覆盖写 / 删除释放旧链
// =====================================================================

#[test]
fn overwrite_releases_old_chain_no_leak() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut engine = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        setup_table(&mut engine).await;

        let v0 = make_value(200 * 1024, 0); // 200KB → ~13 溢出页 + index
        engine.table_put("db1", "t1", b"k", &v0).await.unwrap();
        let baseline = alive_slots(&mut engine);

        // 同尺寸覆盖写 5 次: 每次新链落地 + 旧链释放, 活页数必须不增长
        for seed in 1..=5u8 {
            let v = make_value(200 * 1024, seed);
            engine.table_put("db1", "t1", b"k", &v).await.unwrap();
            assert_eq!(
                alive_slots(&mut engine),
                baseline,
                "第 {seed} 次覆盖写后活页数增长 = 存储泄漏"
            );
            // 读回最新值
            let got = engine.table_get("db1", "t1", b"k").await.unwrap().unwrap();
            assert_eq!(got, v);
        }
    });
}

#[test]
fn delete_releases_chain() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut engine = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        setup_table(&mut engine).await;

        let before_put = alive_slots(&mut engine);
        let v = make_value(100 * 1024, 0x33);
        engine.table_put("db1", "t1", b"gone", &v).await.unwrap();
        assert!(alive_slots(&mut engine) > before_put, "溢出链已计活");

        let existed = engine.table_delete("db1", "t1", b"gone").await.unwrap();
        assert!(existed);
        // 溢出链全部释放 (leaf item 物理删除不减 leaf 页, 只看溢出页差值:
        // put 增加的 = 溢出链 + 可能的 leaf COW, delete 后溢出链应全回收)
        let after_delete = alive_slots(&mut engine);
        assert!(
            after_delete <= before_put + 1, // +1 容忍 leaf 页自身
            "delete 后活页数 {after_delete} 未回落 (put 前 {before_put}) = 泄漏"
        );
        assert!(engine.table_get("db1", "t1", b"gone").await.unwrap().is_none());
    });
}

// =====================================================================
// ⭐ 防复活: 墓碑持久化, reopen 后 recover 不回填死页
// =====================================================================

#[test]
fn freed_pages_not_resurrected_after_reopen() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let opts = opts_for(&tmp);
        {
            let mut engine = StorageEngine::open(opts.clone()).await.unwrap();
            setup_table(&mut engine).await;

            let v0 = make_value(150 * 1024, 0xA0);
            engine.table_put("db1", "t1", b"k", &v0).await.unwrap();
            // 覆盖写: 旧链释放成墓碑, 但旧页字节仍残留磁盘
            let v1 = make_value(150 * 1024, 0xB0);
            engine.table_put("db1", "t1", b"k", &v1).await.unwrap();
            engine.close().await.unwrap();
        }

        // reopen: recover 扫描会看到磁盘上旧链的 LCBP header —
        // 墓碑 (has_record) 必须阻止回填, 活页数不得回升
        let mut engine = StorageEngine::open(opts.clone()).await.unwrap();
        let after_reopen = alive_slots(&mut engine);
        let got = engine.table_get("db1", "t1", b"k").await.unwrap().unwrap();
        assert_eq!(got, make_value(150 * 1024, 0xB0), "最新值完整");

        // 再覆盖写一轮 + reopen, 活页数应与上轮持平 (无累积泄漏)
        let v2 = make_value(150 * 1024, 0xC0);
        engine.table_put("db1", "t1", b"k", &v2).await.unwrap();
        engine.close().await.unwrap();

        let mut engine = StorageEngine::open(opts).await.unwrap();
        let after_second = alive_slots(&mut engine);
        assert!(
            after_second <= after_reopen,
            "reopen 后活页数 {after_second} > 上轮 {after_reopen} = 死页被复活/泄漏"
        );
        let got = engine.table_get("db1", "t1", b"k").await.unwrap().unwrap();
        assert_eq!(got, make_value(150 * 1024, 0xC0));
    });
}

// =====================================================================
// reopen 数据持久性 (溢出链 + descriptor 一起落盘)
// =====================================================================

#[test]
fn overflow_survives_reopen() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let opts = opts_for(&tmp);
        let v = make_value(500 * 1024, 0x77); // 500KB, 跨多 chunk
        {
            let mut engine = StorageEngine::open(opts.clone()).await.unwrap();
            setup_table(&mut engine).await;
            engine.table_put("db1", "t1", b"persist", &v).await.unwrap();
            engine.close().await.unwrap();
        }
        let mut engine = StorageEngine::open(opts).await.unwrap();
        let got = engine
            .table_get("db1", "t1", b"persist")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, v, "溢出 value reopen 后逐字节一致");
    });
}
