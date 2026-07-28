//! ⭐ 批量 API 测试: LeafGuide 区间复用 (lookup_many / put_many).
//!
//! 覆盖:
//! - lookup_many 乱序输入 + 原顺序返回 + travel 次数远小于 key 数 (区间复用生效)
//! - put_many 混合 insert/update + 同 key 重复 (后者覆盖) + split 退化路径
//! - 大 value 混入批 (溢出链防泄漏与单 key 语义一致)

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

async fn setup(engine: &mut StorageEngine) {
    engine.create_db("db1").await.unwrap();
    engine.create_table("db1", "t1").await.unwrap();
}

#[test]
fn get_many_orders_and_reuses_leaf() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut engine = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        setup(&mut engine).await;

        // 500 个顺序 key (跨多 leaf: 单 leaf 容量有限)
        for i in 0..500u32 {
            let k = format!("key{:05}", i);
            let v = format!("val{i}");
            engine.table_put("db1", "t1", k.as_bytes(), v.as_bytes()).await.unwrap();
        }

        // 乱序请求 (含 miss key), 断言原顺序返回
        let ks: Vec<Vec<u8>> = vec![
            b"key00499".to_vec(),
            b"key00000".to_vec(),
            b"missing!".to_vec(),
            b"key00250".to_vec(),
            b"key00001".to_vec(),
        ];
        let refs: Vec<&[u8]> = ks.iter().map(|k| k.as_slice()).collect();
        let got = engine.table_get_many("db1", "t1", &refs).await.unwrap();
        assert_eq!(got[0].as_deref(), Some(b"val499".as_slice()));
        assert_eq!(got[1].as_deref(), Some(b"val0".as_slice()));
        assert_eq!(got[2], None);
        assert_eq!(got[3].as_deref(), Some(b"val250".as_slice()));
        assert_eq!(got[4].as_deref(), Some(b"val1".as_slice()));

        // travel 计数: 500 个顺序 key 的批量读, travel 次数应远小于 key 数.
        // ⭐ Phase K: 直连 btree 需用编码后的物理 key ([S][klen][key]);
        // 等长 key 编码后仍连续, 区间复用不受影响.
        let root = engine.open_table("db1", "t1").await.unwrap().unwrap();
        let all: Vec<Vec<u8>> = (0..500u32)
            .map(|i| storage::keyspace::encode_string(format!("key{:05}", i).as_bytes()))
            .collect();
        let all_refs: Vec<&[u8]> = all.iter().map(|k| k.as_slice()).collect();
        let (results, travels) =
            storage::btree::btree_lookup_many(engine.pager_mut(), root, &all_refs)
                .await
                .unwrap();
        assert!(results.iter().all(|r| r.is_some()), "500 key 全命中");
        assert!(
            travels * 4 < 500,
            "区间复用应使 travel 次数远小于 key 数, 实测 {travels}/500"
        );
    });
}

#[test]
fn put_many_insert_update_and_duplicate_key() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut engine = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        setup(&mut engine).await;

        // 预置一半 key (update 路径), 另一半留给 insert 路径
        for i in (0..200u32).step_by(2) {
            let k = format!("pm{:04}", i);
            engine.table_put("db1", "t1", k.as_bytes(), b"old").await.unwrap();
        }

        // 批量写 200 个 (交错 insert/update) + 末尾同 key 重复 (后者覆盖)
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..200u32)
            .map(|i| (format!("pm{:04}", i).into_bytes(), format!("new{i}").into_bytes()))
            .collect();
        pairs.push((b"pm0000".to_vec(), b"final".to_vec())); // 重复: 覆盖 new0
        engine.table_put_many("db1", "t1", &pairs).await.unwrap();

        for i in 0..200u32 {
            let k = format!("pm{:04}", i);
            let got = engine.table_get("db1", "t1", k.as_bytes()).await.unwrap().unwrap();
            let expect = if i == 0 {
                b"final".to_vec()
            } else {
                format!("new{i}").into_bytes()
            };
            assert_eq!(got, expect, "key {k}");
        }
    });
}

/// 批量写触发 leaf split (PageFull 退化路径) + reopen 持久性.
#[test]
fn put_many_split_fallback_and_reopen() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let opts = opts_for(&tmp);
        {
            let mut engine = StorageEngine::open(opts.clone()).await.unwrap();
            setup(&mut engine).await;

            // 一次性批量写 2000 个 3KB 级 value → 必然多次 PageFull/split
            let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..2000u32)
                .map(|i| {
                    (
                        format!("big{:05}", i).into_bytes(),
                        vec![(i % 251) as u8 + 1; 800],
                    )
                })
                .collect();
            engine.table_put_many("db1", "t1", &pairs).await.unwrap();

            for i in (0..2000u32).step_by(97) {
                let k = format!("big{:05}", i);
                let got = engine.table_get("db1", "t1", k.as_bytes()).await.unwrap().unwrap();
                assert_eq!(got, vec![(i % 251) as u8 + 1; 800], "key {k}");
            }
            engine.close().await.unwrap();
        }
        // reopen 持久性
        let mut engine = StorageEngine::open(opts).await.unwrap();
        for i in (0..2000u32).step_by(211) {
            let k = format!("big{:05}", i);
            let got = engine.table_get("db1", "t1", k.as_bytes()).await.unwrap().unwrap();
            assert_eq!(got, vec![(i % 251) as u8 + 1; 800], "reopen key {k}");
        }
    });
}

/// 大 value (溢出链) 混入批: 写读一致 + 覆盖写无泄漏.
#[test]
fn put_many_with_overflow_values_no_leak() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut engine = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        setup(&mut engine).await;

        let big1 = vec![0xA1u8; 100 * 1024];
        let big2 = vec![0xB2u8; 60 * 1024];
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"small1".to_vec(), b"v1".to_vec()),
            (b"huge01".to_vec(), big1.clone()),
            (b"small2".to_vec(), b"v2".to_vec()),
            (b"huge02".to_vec(), big2.clone()),
        ];
        engine.table_put_many("db1", "t1", &pairs).await.unwrap();
        assert_eq!(
            engine.table_get("db1", "t1", b"huge01").await.unwrap().unwrap(),
            big1
        );
        assert_eq!(
            engine.table_get("db1", "t1", b"huge02").await.unwrap().unwrap(),
            big2
        );

        // 覆盖写同尺寸批 3 轮: 活页数不增长 (旧链批后释放)
        let alive = |e: &mut StorageEngine| {
            e.pager_mut()
                .meta_debug_iter()
                .iter()
                .filter(|(_, p)| p.flags() & storage::PID_ALIVE != 0)
                .count()
        };
        let baseline = alive(&mut engine);
        for round in 0..3u8 {
            let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
                (b"huge01".to_vec(), vec![round; 100 * 1024]),
                (b"huge02".to_vec(), vec![round.wrapping_add(1); 60 * 1024]),
            ];
            engine.table_put_many("db1", "t1", &pairs).await.unwrap();
            assert_eq!(alive(&mut engine), baseline, "第 {round} 轮覆盖写泄漏");
        }
        assert_eq!(
            engine.table_get("db1", "t1", b"huge01").await.unwrap().unwrap(),
            vec![2u8; 100 * 1024]
        );
    });
}
