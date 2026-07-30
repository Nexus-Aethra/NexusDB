//! ⭐ Phase L: List 引擎层测试 (head/tail meta + 跨 leaf LRANGE + reopen 持久化).

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

fn tagged(p: &[u8]) -> Vec<u8> {
    let mut v = vec![0x01u8];
    v.extend_from_slice(p);
    v
}

#[test]
fn list_push_pop_order() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        e.create_db("db1").await.unwrap();
        e.create_table("db1", "t1").await.unwrap();

        // RPUSH a b c → [a,b,c]
        let vals: Vec<Vec<u8>> = [b"a", b"b", b"c"].iter().map(|m| tagged(*m)).collect();
        assert_eq!(e.list_push("db1", "t1", b"l", &vals, false).await.unwrap(), 3);
        // LPUSH x → [x,a,b,c]
        assert_eq!(
            e.list_push("db1", "t1", b"l", &[tagged(b"x")], true).await.unwrap(),
            4
        );
        let all = e.list_range("db1", "t1", b"l", 0, -1).await.unwrap();
        let got: Vec<Vec<u8>> = all;
        assert_eq!(got, vec![tagged(b"x"), tagged(b"a"), tagged(b"b"), tagged(b"c")]);

        // LPOP → x; RPOP → c
        assert_eq!(e.list_pop("db1", "t1", b"l", true, 1).await.unwrap(), vec![tagged(b"x")]);
        assert_eq!(e.list_pop("db1", "t1", b"l", false, 1).await.unwrap(), vec![tagged(b"c")]);
        assert_eq!(e.list_len("db1", "t1", b"l").await.unwrap(), 2);

        // LINDEX / LSET
        assert_eq!(e.list_index("db1", "t1", b"l", 0).await.unwrap(), Some(tagged(b"a")));
        assert_eq!(e.list_index("db1", "t1", b"l", -1).await.unwrap(), Some(tagged(b"b")));
        assert!(e.list_set("db1", "t1", b"l", 0, &tagged(b"A")).await.unwrap());
        assert_eq!(e.list_index("db1", "t1", b"l", 0).await.unwrap(), Some(tagged(b"A")));
        assert!(!e.list_set("db1", "t1", b"l", 99, &tagged(b"z")).await.unwrap());
    });
}

#[test]
fn list_large_cross_leaf_and_reopen() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
            e.create_db("db1").await.unwrap();
            e.create_table("db1", "t1").await.unwrap();
            let vals: Vec<Vec<u8>> =
                (0..1500u32).map(|i| tagged(format!("e{i}").as_bytes())).collect();
            assert_eq!(e.list_push("db1", "t1", b"big", &vals, false).await.unwrap(), 1500);
            e.close().await.unwrap();
        }
        // reopen
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        assert_eq!(e.list_len("db1", "t1", b"big").await.unwrap(), 1500);
        let all = e.list_range("db1", "t1", b"big", 0, -1).await.unwrap();
        assert_eq!(all.len(), 1500, "跨 leaf LRANGE 必须收齐");
        assert_eq!(all[0], tagged(b"e0"));
        assert_eq!(all[1499], tagged(b"e1499"));
    });
}

/// ⭐ C2: 中段操作 — LREM/LTRIM/LPOS/LINSERT + 空洞后 LINDEX/LPOP 仍正确.
#[test]
fn list_mid_operations() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        e.create_db("db1").await.unwrap();
        e.create_table("db1", "t1").await.unwrap();

        // 构造 [a, b, a, c, a, d]
        let vals: Vec<Vec<u8>> = [b"a", b"b", b"a", b"c", b"a", b"d"]
            .iter()
            .map(|v| tagged(*v))
            .collect();
        e.list_push("db1", "t1", b"l", &vals, false).await.unwrap();

        // LPOS a → [0, 2, 4]; RANK 2 → [2, 4]; RANK -1 → [4, 2, 0]
        assert_eq!(
            e.list_pos("db1", "t1", b"l", &tagged(b"a"), 1, 0).await.unwrap(),
            vec![0, 2, 4]
        );
        assert_eq!(
            e.list_pos("db1", "t1", b"l", &tagged(b"a"), 2, 0).await.unwrap(),
            vec![2, 4]
        );
        assert_eq!(
            e.list_pos("db1", "t1", b"l", &tagged(b"a"), -1, 1).await.unwrap(),
            vec![4]
        );

        // LREM count=2 从头删 2 个 a → [b, c, a, d]
        assert_eq!(e.list_rem("db1", "t1", b"l", 2, &tagged(b"a")).await.unwrap(), 2);
        assert_eq!(e.list_len("db1", "t1", b"l").await.unwrap(), 4);
        // 空洞后 LINDEX 按扫描序仍正确
        assert_eq!(
            e.list_index("db1", "t1", b"l", 0).await.unwrap(),
            Some(tagged(b"b"))
        );
        assert_eq!(
            e.list_index("db1", "t1", b"l", 2).await.unwrap(),
            Some(tagged(b"a"))
        );
        assert_eq!(
            e.list_index("db1", "t1", b"l", -1).await.unwrap(),
            Some(tagged(b"d"))
        );

        // LINSERT BEFORE c → [b, x, c, a, d] (中段, 走空洞复用或搬行)
        assert_eq!(
            e.list_insert("db1", "t1", b"l", true, &tagged(b"c"), &tagged(b"x"))
                .await
                .unwrap(),
            5
        );
        let r = e.list_range("db1", "t1", b"l", 0, -1).await.unwrap();
        assert_eq!(
            r,
            vec![tagged(b"b"), tagged(b"x"), tagged(b"c"), tagged(b"a"), tagged(b"d")]
        );
        // pivot 不存在 → -1
        assert_eq!(
            e.list_insert("db1", "t1", b"l", false, &tagged(b"zz"), &tagged(b"y"))
                .await
                .unwrap(),
            -1
        );

        // LTRIM 1..=3 → [x, c, a]
        e.list_trim("db1", "t1", b"l", 1, 3).await.unwrap();
        let r = e.list_range("db1", "t1", b"l", 0, -1).await.unwrap();
        assert_eq!(r, vec![tagged(b"x"), tagged(b"c"), tagged(b"a")]);

        // 空洞后 LPOP/RPOP 仍正确 (extreme idx 上有洞)
        assert_eq!(
            e.list_pop("db1", "t1", b"l", true, 1).await.unwrap(),
            vec![tagged(b"x")]
        );
        assert_eq!(
            e.list_pop("db1", "t1", b"l", false, 1).await.unwrap(),
            vec![tagged(b"a")]
        );
        assert_eq!(e.list_len("db1", "t1", b"l").await.unwrap(), 1);

        // LTRIM 空区间 → 全删 + meta 清
        e.list_trim("db1", "t1", b"l", 5, 3).await.unwrap();
        assert_eq!(e.list_len("db1", "t1", b"l").await.unwrap(), 0);

        // reopen 后中段操作结果持久化
        let vals2: Vec<Vec<u8>> = [b"p", b"q", b"r"].iter().map(|v| tagged(*v)).collect();
        e.list_push("db1", "t1", b"l2", &vals2, false).await.unwrap();
        e.list_rem("db1", "t1", b"l2", 0, &tagged(b"q")).await.unwrap();
        e.flush().await.unwrap();
        e.close().await.unwrap();
        let mut e = StorageEngine::open(opts_for(&tmp)).await.unwrap();
        let r = e.list_range("db1", "t1", b"l2", 0, -1).await.unwrap();
        assert_eq!(r, vec![tagged(b"p"), tagged(b"r")]);
        assert_eq!(e.list_len("db1", "t1", b"l2").await.unwrap(), 2);
    });
}
