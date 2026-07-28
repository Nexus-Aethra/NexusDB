//! ⭐ C1: Set 引擎层测试 (SMISMEMBER / SPOP count / SRANDMEMBER count + reopen).

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
fn set_mismember_and_pop_n() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        e.create_db("db1").await.unwrap();
        e.create_table("db1", "t1").await.unwrap();

        let members: Vec<Vec<u8>> = [b"a", b"b", b"c", b"d"].iter().map(|m| m.to_vec()).collect();
        assert_eq!(e.set_add("db1", "t1", b"s", &members).await.unwrap(), 4);

        // SMISMEMBER 保持输入序
        let q: Vec<Vec<u8>> = [b"a", b"x", b"c"].iter().map(|m| m.to_vec()).collect();
        assert_eq!(
            e.set_mismember("db1", "t1", b"s", &q).await.unwrap(),
            vec![true, false, true]
        );
        // 不存在 key → 全 false
        assert_eq!(
            e.set_mismember("db1", "t1", b"none", &q).await.unwrap(),
            vec![false, false, false]
        );

        // SRANDMEMBER count 不删
        assert_eq!(e.set_rand_n("db1", "t1", b"s", 2).await.unwrap().len(), 2);
        assert_eq!(e.set_card("db1", "t1", b"s").await.unwrap(), 4);

        // SPOP count 删除且 card 递减; count 超量 → 全弹
        let popped = e.set_pop_n("db1", "t1", b"s", 3).await.unwrap();
        assert_eq!(popped.len(), 3);
        assert_eq!(e.set_card("db1", "t1", b"s").await.unwrap(), 1);
        let rest = e.set_pop_n("db1", "t1", b"s", 10).await.unwrap();
        assert_eq!(rest.len(), 1);
        assert_eq!(e.set_card("db1", "t1", b"s").await.unwrap(), 0);

        // reopen: 空 set 的 meta 已清 (U3 重建不复活)
        e.set_add("db1", "t1", b"s2", &members).await.unwrap();
        e.flush().await.unwrap();
        e.close().await.unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        assert_eq!(e.set_card("db1", "t1", b"s").await.unwrap(), 0);
        assert_eq!(e.set_card("db1", "t1", b"s2").await.unwrap(), 4);
    });
}
