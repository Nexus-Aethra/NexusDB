//! ⭐ Phase H: Hash 引擎层测试 (复合 key 行 + meta count + reopen 持久化 + 大 hash).

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

/// value 统一 [TAG_RAW=0x01][payload] (与协议门面同约定).
fn tagged(payload: &[u8]) -> Vec<u8> {
    let mut v = vec![0x01u8];
    v.extend_from_slice(payload);
    v
}

#[test]
fn hash_reopen_persistence() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
            e.create_db("db1").await.unwrap();
            e.create_table("db1", "t1").await.unwrap();
            let pairs = vec![
                (b"name".to_vec(), tagged(b"alice")),
                (b"age".to_vec(), tagged(b"30")),
            ];
            assert_eq!(e.hash_set("db1", "t1", b"user:1", &pairs).await.unwrap(), 2);
            assert_eq!(e.hash_len("db1", "t1", b"user:1").await.unwrap(), 2);
            e.close().await.unwrap();
        }
        // reopen: meta count + field 行完整恢复
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        assert_eq!(e.hash_len("db1", "t1", b"user:1").await.unwrap(), 2);
        assert_eq!(
            e.hash_get("db1", "t1", b"user:1", b"name").await.unwrap(),
            Some(tagged(b"alice"))
        );
        let all = e.hash_get_all("db1", "t1", b"user:1").await.unwrap();
        assert_eq!(all.len(), 2);
        // BTree 字典序: age < name
        assert_eq!(all[0].0, b"age");
        assert_eq!(all[1].0, b"name");
    });
}

/// 大 hash: 2000 field 跨多 leaf, HGETALL 完整且有序; HDEL 后 count 精确.
#[test]
fn hash_large_cross_leaf() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        e.create_db("db1").await.unwrap();
        e.create_table("db1", "t1").await.unwrap();

        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..2000u32)
            .map(|i| (format!("field{i:05}").into_bytes(), tagged(format!("v{i}").as_bytes())))
            .collect();
        assert_eq!(e.hash_set("db1", "t1", b"big", &pairs).await.unwrap(), 2000);
        assert_eq!(e.hash_len("db1", "t1", b"big").await.unwrap(), 2000);

        let all = e.hash_get_all("db1", "t1", b"big").await.unwrap();
        assert_eq!(all.len(), 2000, "跨 leaf 扫描必须收齐全部 field");
        for w in all.windows(2) {
            assert!(w[0].0 < w[1].0, "field 有序");
        }
        assert_eq!(all[0].0, b"field00000");
        assert_eq!(all[1999].0, b"field01999");

        // 删一半
        let dels: Vec<Vec<u8>> = (0..1000u32)
            .map(|i| format!("field{i:05}").into_bytes())
            .collect();
        assert_eq!(e.hash_del("db1", "t1", b"big", &dels).await.unwrap(), 1000);
        assert_eq!(e.hash_len("db1", "t1", b"big").await.unwrap(), 1000);
        let rest = e.hash_get_all("db1", "t1", b"big").await.unwrap();
        assert_eq!(rest.len(), 1000);
        assert_eq!(rest[0].0, b"field01000");
    });
}

/// WRONGTYPE + key_delete_any 清理.
#[test]
fn hash_wrongtype_and_delete_any() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        e.create_db("db1").await.unwrap();
        e.create_table("db1", "t1").await.unwrap();

        // String key 上 hash op → WrongType
        e.table_put("db1", "t1", b"sk", &tagged(b"v")).await.unwrap();
        let err = e
            .hash_set("db1", "t1", b"sk", &[(b"f".to_vec(), tagged(b"v"))])
            .await
            .unwrap_err();
        assert!(err.to_string().starts_with("WRONGTYPE"));

        // Hash key 上 String GET (typed) → WrongType
        e.hash_set("db1", "t1", b"hk", &[(b"f".to_vec(), tagged(b"v"))])
            .await
            .unwrap();
        let err = e.table_get_typed("db1", "t1", b"hk").await.unwrap_err();
        assert!(err.to_string().starts_with("WRONGTYPE"));

        // key_delete_any: 删整 hash (meta + field 行), 再删返回 false
        assert!(e.key_delete_any("db1", "t1", b"hk").await.unwrap());
        assert_eq!(e.hash_len("db1", "t1", b"hk").await.unwrap(), 0);
        assert_eq!(e.hash_get_all("db1", "t1", b"hk").await.unwrap().len(), 0);
        assert!(!e.key_delete_any("db1", "t1", b"hk").await.unwrap());
        // 删除后 String GET 恢复 nil (不再 WRONGTYPE)
        assert_eq!(e.table_get_typed("db1", "t1", b"hk").await.unwrap(), None);
    });
}

/// ⭐ U2: SET 覆盖复合 key → 旧复合行全清 (无孤儿行), key 变 string.
#[test]
fn set_over_composite_purges_old_rows() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        e.create_db("db1").await.unwrap();
        e.create_table("db1", "t1").await.unwrap();

        // 建一个 hash, 再 SET 同 key → 应清光 hash 行
        let pairs = vec![
            (b"f1".to_vec(), tagged(b"v1")),
            (b"f2".to_vec(), tagged(b"v2")),
        ];
        e.hash_set("db1", "t1", b"k", &pairs).await.unwrap();
        e.table_put("db1", "t1", b"k", &tagged(b"strval")).await.unwrap();

        // key 现在是 string
        assert_eq!(
            e.table_get_typed("db1", "t1", b"k").await.unwrap(),
            Some(tagged(b"strval"))
        );
        // hash 行全清: 对 hash key 的 HLEN 应 WRONGTYPE (key 已是 string)
        let hash_err = e.hash_len("db1", "t1", b"k").await;
        assert!(hash_err.is_err(), "SET 后对 hash key 的 HLEN 应 WRONGTYPE");

        // DEL 后彻底干净, 再建 hash 正常 (无孤儿行干扰 count)
        assert!(e.key_delete_any("db1", "t1", b"k").await.unwrap());
        assert_eq!(e.table_get_typed("db1", "t1", b"k").await.unwrap(), None);
        e.hash_set("db1", "t1", b"k", &[(b"nf".to_vec(), tagged(b"nv"))]).await.unwrap();
        assert_eq!(e.hash_len("db1", "t1", b"k").await.unwrap(), 1);
        // 全量 HGETALL 只有新 field (旧 f1/f2 无残留)
        let all = e.hash_get_all("db1", "t1", b"k").await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].0, b"nf");
    });
}

/// ⭐ U2: GET 对 Set/List/ZSet key 完整 WRONGTYPE (不再只 hash).
#[test]
fn get_on_any_composite_is_wrongtype() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        e.create_db("db1").await.unwrap();
        e.create_table("db1", "t1").await.unwrap();

        e.set_add("db1", "t1", b"sk", &[b"m".to_vec()]).await.unwrap();
        e.list_push("db1", "t1", b"lk", &[tagged(b"x")], false).await.unwrap();
        e.zset_add("db1", "t1", b"zk", &[(1.0, b"m".to_vec())]).await.unwrap();

        for k in [&b"sk"[..], &b"lk"[..], &b"zk"[..]] {
            assert!(
                e.table_get_typed("db1", "t1", k).await.is_err(),
                "GET on composite key {k:?} 应 WRONGTYPE"
            );
        }
        // 反向: 各类型 op 在 String key 上 WRONGTYPE
        e.table_put("db1", "t1", b"str", &tagged(b"v")).await.unwrap();
        assert!(e.set_add("db1", "t1", b"str", &[b"m".to_vec()]).await.is_err());
        assert!(e.list_push("db1", "t1", b"str", &[tagged(b"x")], false).await.is_err());
        assert!(e.zset_add("db1", "t1", b"str", &[(1.0, b"m".to_vec())]).await.is_err());
        // 异类复合互斥: set key 上 zadd
        assert!(e.zset_add("db1", "t1", b"sk", &[(1.0, b"m".to_vec())]).await.is_err());
    });
}
