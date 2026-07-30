//! ⭐ Phase Z: ZSet 引擎层测试 (双索引一致 + score 排序 + reopen 持久化).

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
        wal_mode: Default::default(),
    }
}

#[test]
fn zset_dual_index_and_score_update() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        e.create_db("db1").await.unwrap();
        e.create_table("db1", "t1").await.unwrap();

        let pairs = vec![
            (3.0, b"c".to_vec()),
            (1.0, b"a".to_vec()),
            (2.0, b"b".to_vec()),
        ];
        assert_eq!(e.zset_add("db1", "t1", b"z", &pairs).await.unwrap(), 3);
        assert_eq!(e.zset_card("db1", "t1", b"z").await.unwrap(), 3);
        assert_eq!(e.zset_score("db1", "t1", b"z", b"b").await.unwrap(), Some(2.0));

        // 按 score 升序
        let r = e.zset_range("db1", "t1", b"z", 0, -1, false).await.unwrap();
        let members: Vec<Vec<u8>> = r.iter().map(|(m, _)| m.clone()).collect();
        assert_eq!(members, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

        // 更新 a 的 score → 10, 双索引一致: a 移到末尾, card 不变
        assert_eq!(e.zset_add("db1", "t1", b"z", &[(10.0, b"a".to_vec())]).await.unwrap(), 0);
        assert_eq!(e.zset_card("db1", "t1", b"z").await.unwrap(), 3);
        let r = e.zset_range("db1", "t1", b"z", 0, -1, false).await.unwrap();
        let members: Vec<Vec<u8>> = r.iter().map(|(m, _)| m.clone()).collect();
        assert_eq!(members, vec![b"b".to_vec(), b"c".to_vec(), b"a".to_vec()]);
        assert_eq!(e.zset_score("db1", "t1", b"z", b"a").await.unwrap(), Some(10.0));

        // ZRANGEBYSCORE
        let r = e.zset_range_by_score("db1", "t1", b"z", 2.0, 3.0).await.unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].0, b"b");
        assert_eq!(r[1].0, b"c");

        // ZRANK
        assert_eq!(e.zset_rank("db1", "t1", b"z", b"b", false).await.unwrap(), Some(0));
        assert_eq!(e.zset_rank("db1", "t1", b"z", b"a", false).await.unwrap(), Some(2));

        // ZREM: 删 b, 旧 score 索引行也应被清 (再 range 不含 b)
        assert_eq!(e.zset_rem("db1", "t1", b"z", &[b"b".to_vec()]).await.unwrap(), 1);
        let r = e.zset_range("db1", "t1", b"z", 0, -1, false).await.unwrap();
        let members: Vec<Vec<u8>> = r.iter().map(|(m, _)| m.clone()).collect();
        assert_eq!(members, vec![b"c".to_vec(), b"a".to_vec()]);

        // ⭐ C1: ZMSCORE (保持输入序, 缺失 None)
        let q: Vec<Vec<u8>> = [b"a", b"x", b"c"].iter().map(|m| m.to_vec()).collect();
        assert_eq!(
            e.zset_mscore("db1", "t1", b"z", &q).await.unwrap(),
            vec![Some(10.0), None, Some(3.0)]
        );

        // ⭐ C1: ZPOPMIN 弹最小 c(3); ZPOPMAX 弹最大 a(10); 双索引同步收缩
        let popped = e.zset_pop("db1", "t1", b"z", false, 1).await.unwrap();
        assert_eq!(popped, vec![(b"c".to_vec(), 3.0)]);
        let popped = e.zset_pop("db1", "t1", b"z", true, 5).await.unwrap();
        assert_eq!(popped, vec![(b"a".to_vec(), 10.0)]);
        assert_eq!(e.zset_card("db1", "t1", b"z").await.unwrap(), 0);
    });
}

#[test]
fn zset_large_cross_leaf_and_reopen() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
            e.create_db("db1").await.unwrap();
            e.create_table("db1", "t1").await.unwrap();
            // 1000 成员, score = i (升序); 双索引各 1000 行 → 跨多 leaf
            let pairs: Vec<(f64, Vec<u8>)> = (0..1000u32)
                .map(|i| (i as f64, format!("m{i:05}").into_bytes()))
                .collect();
            assert_eq!(e.zset_add("db1", "t1", b"big", &pairs).await.unwrap(), 1000);
            e.close().await.unwrap();
        }
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        assert_eq!(e.zset_card("db1", "t1", b"big").await.unwrap(), 1000);
        let all = e.zset_range("db1", "t1", b"big", 0, -1, false).await.unwrap();
        assert_eq!(all.len(), 1000, "跨 leaf score 扫描收齐");
        // 有序: score 递增
        for w in all.windows(2) {
            assert!(w[0].1 <= w[1].1);
        }
        assert_eq!(all[0].0, b"m00000");
        assert_eq!(all[999].0, b"m00999");
        assert_eq!(e.zset_score("db1", "t1", b"big", b"m00500").await.unwrap(), Some(500.0));
    });
}
