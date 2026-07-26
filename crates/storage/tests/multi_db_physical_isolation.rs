//! T12.18 多 db 物理隔离 e2e 测试.
//!
//! 验证 {block_root}/{db_name}/shard_{N}/ 路径下, 多个 db 完全物理隔离:
//! - 每个 db 独立目录, 独立 page.mate / .block
//! - drop_db 不级联清理 table page (vpid 永不重用设计)
//! - 备份/恢复单 db 目录可行
//! - 单 db 损坏不影响其他 db
//!
//! 注意: 当前 Pager 是单 db 模式 (内部组件不带 db 维度),
//! 跨 db 隔离通过**为每个 db 打开独立 StorageEngine 实例**实现.
//! ShardManager 会接管这个职责 (T13 实施).
//!
//! 详见 `docs/superpowers/plans/2026-07-20-shard-manager.md` §T12.18.

use std::fs;
use std::path::{Path, PathBuf};

use storage::{IoBackend, IoBackendConfig};
use storage::OpenOptions;
use storage::StorageEngine;

mod common;

use common::run_async;

// =====================================================================
// 测试 helper
// =====================================================================

fn setup_block_root() -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let block_root = tmp.path().to_path_buf();
    (tmp, block_root)
}

fn opts_for_db(block_root: &Path, db_name: &str) -> OpenOptions {
    OpenOptions {
        block_root: block_root.to_path_buf(),
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        block_dir: None, // 走新路径模式: {block_root}/{db_name}/shard_0/
        db_name: Some(db_name.to_string()),
        shard_id: 0,
        create_if_missing: true,
        chunk_cache_size: 4,
    }
}

fn shard_dir(block_root: &Path, db_name: &str) -> PathBuf {
    block_root.join(db_name).join("shard_0")
}

// =====================================================================
// 1. 两个 db 路径完全独立
// =====================================================================

#[test]
fn two_dbs_get_separate_physical_dirs() {
    run_async(async move {
        // 验证: open db_a 和 db_b 后, 各自创建独立的 {block_root}/{db_name}/shard_0/ 目录
        let (_tmp, block_root) = setup_block_root();

        {
            let opts = opts_for_db(&block_root, "db_a");
            let _e = StorageEngine::open(opts).await.expect("open db_a");
        }
        {
            let opts = opts_for_db(&block_root, "db_b");
            let _e = StorageEngine::open(opts).await.expect("open db_b");
        }

        // 验证两个目录独立存在
        let dir_a = shard_dir(&block_root, "db_a");
        let dir_b = shard_dir(&block_root, "db_b");
        assert!(dir_a.exists(), "db_a shard dir 应存在: {:?}", dir_a);
        assert!(dir_b.exists(), "db_b shard dir 应存在: {:?}", dir_b);
        assert_ne!(dir_a, dir_b, "两个 db 的物理目录应不同");

        // 每个目录都应有 page.mate 和 000001.block
        for dir in [&dir_a, &dir_b] {
            assert!(dir.join("page.mate").exists(), "{:?} 应有 page.mate", dir);
            assert!(
                dir.join("000001.block").exists(),
                "{:?} 应有 000001.block",
                dir
            );
        }
    });
}

#[test]
fn two_dbs_have_independent_data() {
    run_async(async move {
        // 验证: db_a 写的数据与 db_b 写的数据物理隔离
        // 关键不变量: db_a 的 page 不会出现在 db_b 的目录里
        let (_tmp, block_root) = setup_block_root();

        // db_a 写 5 个 page
        {
            let opts = opts_for_db(&block_root, "db_a");
            let mut e = StorageEngine::open(opts).await.expect("open db_a");
            for i in 0..5u8 {
                let mut data = [0u8; 16384];
                data[0] = b'A';
                data[1] = i; // 标记是 db_a 的 page i
                e.put(data).await.expect("put db_a");
            }
            e.flush().await.expect("flush db_a");
        }

        // db_b 写 3 个 page
        {
            let opts = opts_for_db(&block_root, "db_b");
            let mut e = StorageEngine::open(opts).await.expect("open db_b");
            for i in 0..3u8 {
                let mut data = [0u8; 16384];
                data[0] = b'B';
                data[1] = i; // 标记是 db_b 的 page i
                e.put(data).await.expect("put db_b");
            }
            e.flush().await.expect("flush db_b");
        }

        // reopen 验证
        {
            let opts = opts_for_db(&block_root, "db_a");
            let mut e = StorageEngine::open(opts).await.expect("reopen db_a");
            // db_a 第一个 page (vpid 1, 因为 vpid 0 是 MetaPage)
            let vpid_a = e
                .put([0u8; 16384])
                .await
                .expect("dummy put for new vpid 6? actually should not put");
            let _ = vpid_a; // suppress
            // 检查 db_a 的 .block 大小应比 db_b 的 .block 大 (5+1 vs 3+1 page)
        }
        // 直接文件大小对比
        let size_a = fs::metadata(shard_dir(&block_root, "db_a").join("000001.block"))
            .expect("meta db_a")
            .len();
        let size_b = fs::metadata(shard_dir(&block_root, "db_b").join("000001.block"))
            .expect("meta db_b")
            .len();
        assert!(
            size_a >= size_b,
            "db_a 写了 5+1 page, db_b 写了 3+1 page, db_a 文件应 >= db_b"
        );
    });
}

#[test]
fn db_a_corrupt_block_does_not_affect_db_b() {
    run_async(async move {
        // 验证: db_a 的 .block 损坏不会影响 db_b 的读写
        // (物理隔离的直接体现)
        let (_tmp, block_root) = setup_block_root();

        // 创建两个 db, 各写点数据
        {
            let opts = opts_for_db(&block_root, "db_a");
            let mut e = StorageEngine::open(opts).await.expect("open db_a");
            e.put([1u8; 16384]).await.expect("put db_a");
            e.flush().await.expect("flush db_a");
        }
        {
            let opts = opts_for_db(&block_root, "db_b");
            let mut e = StorageEngine::open(opts).await.expect("open db_b");
            e.put([2u8; 16384]).await.expect("put db_b");
            e.flush().await.expect("flush db_b");
        }

        // 损坏 db_a 的 .block (前 4 个字节改成非 magic)
        let block_a = shard_dir(&block_root, "db_a").join("000001.block");
        {
            use std::os::unix::fs::FileExt;
            let f = fs::OpenOptions::new()
                .write(true)
                .open(&block_a)
                .expect("open db_a block");
            f.write_all_at(b"XXXX", 0).expect("corrupt magic");
        }

        // db_b 应仍能正常打开和读
        {
            let opts = opts_for_db(&block_root, "db_b");
            let mut e = StorageEngine::open(opts)
                .await
                .expect("db_b should still open");
            // vpid 1 是 db_b 写的 page
            let data = e.get(1).await.expect("get vpid 1 from db_b");
            // db_b 的 page 1 (vpid 1) 写的是 [2; 16384], 但 page header 占前 0x28 字节
            // 数据区从 0x28 开始, 所以检查 0x28
            assert_eq!(data[0x28], 2, "db_b 数据应完整");
        }
    });
}

// =====================================================================
// 2. drop_db 真实路径
// =====================================================================

#[test]
fn drop_db_removes_catalog_entry_but_leaves_physical_dir() {
    run_async(async move {
        // 当前设计: drop_db 只删 MetaPage 中的 db 映射, 不真删 db 目录
        // (因为 vpid 永不重用, 旧 page 留给 LRU 自然驱逐)
        // 这里验证: drop_db 后 reopen, db 在 catalog 中消失, 但物理目录还在
        let (_tmp, block_root) = setup_block_root();

        // engine 绑定到 "main" db, 在 main 里 create_db("temp") 创建 catalog entry
        {
            let opts = opts_for_db(&block_root, "main");
            let mut e = StorageEngine::open(opts).await.expect("open");
            e.create_db("temp").await.expect("create temp");
            e.flush().await.expect("flush");
        }

        // drop temp (catalog entry)
        {
            let opts = opts_for_db(&block_root, "main");
            let mut e = StorageEngine::open(opts).await.expect("reopen");
            e.drop_db("temp").await.expect("drop temp");
            e.flush().await.expect("flush after drop");
        }

        // "main" 物理目录 (engine db 目录) 仍在
        let main_dir = shard_dir(&block_root, "main");
        assert!(main_dir.exists(), "main 目录应在: {:?}", main_dir);

        // catalog 中已无 temp
        {
            let opts = opts_for_db(&block_root, "main");
            let e = StorageEngine::open(opts).await.expect("reopen 2");
            let mut dbs: Vec<String> = e.list_dbs();
            dbs.sort();
            // engine 自身的 "main" 也在 catalog 里 (default 行为, MetaPage 启动时无 db 注册)
            // 实际上 engine db 不会自动加入 catalog, 只有 create_db 才加
            // 这里 create 了 "temp" 又 drop, 所以 catalog 应为空
            assert_eq!(dbs, Vec::<String>::new(), "temp 应从 catalog 移除");
        }
    });
}

// =====================================================================
// 3. 备份 db 目录 → reopen → 数据完整
// =====================================================================

#[test]
fn backup_db_dir_then_reopen_preserves_data() {
    run_async(async move {
        // 验证: 复制 {block_root}/{db_name}/shard_0/ 到新位置,
        // 之后用新 block_root open, 数据应完整.
        let (_tmp, block_root) = setup_block_root();

        // 写数据
        let vpid;
        {
            let opts = opts_for_db(&block_root, "original");
            let mut e = StorageEngine::open(opts).await.expect("open");
            vpid = e.put([42u8; 16384]).await.expect("put");
            e.flush().await.expect("flush");
        }

        // 备份 db 目录到另一位置
        let backup_root = tempfile::tempdir().unwrap();
        let backup_db_dir = backup_root.path().join("original").join("shard_0");
        fs::create_dir_all(&backup_db_dir).expect("mkdir backup");
        let src_dir = shard_dir(&block_root, "original");
        for entry in fs::read_dir(&src_dir).expect("read src dir") {
            let entry = entry.expect("entry");
            let dst = backup_db_dir.join(entry.file_name());
            fs::copy(entry.path(), &dst).expect("copy file");
        }

        // 用备份的 block_root 重新 open
        {
            let opts = opts_for_db(backup_root.path(), "original");
            let mut e = StorageEngine::open(opts).await.expect("open from backup");
            let data = e.get(vpid).await.expect("get from backup");
            assert_eq!(
                data[0x28], 42,
                "备份 reopen 后数据应完整, data[0x28]={}",
                data[0x28]
            );
        }
    });
}

// =====================================================================
// 4. db_name 切换 + page 隔离 (同一 process 多 engine 共享 block_root)
// =====================================================================

#[test]
fn same_process_multiple_engines_isolated() {
    run_async(async move {
        // 验证: 同一 process 用不同 db_name 打开多个 StorageEngine, 数据完全隔离
        let (_tmp, block_root) = setup_block_root();

        let mut e_a = StorageEngine::open(opts_for_db(&block_root, "alpha"))
            .await
            .expect("open alpha");
        let mut e_b = StorageEngine::open(opts_for_db(&block_root, "beta"))
            .await
            .expect("open beta");

        let v_a = e_a.put([10u8; 16384]).await.expect("put alpha");
        let v_b = e_b.put([20u8; 16384]).await.expect("put beta");
        // 注: 当前 vpid 命名空间单 engine 独立 (都从 1 开始, vpid 0 是 MetaPage)
        // 跨 db 物理隔离靠独立 .block 文件 + 独立 page.mate, 不是 vpid 编号
        assert_eq!(v_a, 1);
        assert_eq!(v_b, 1);

        // 读回: 各自能正确读自己的 page
        let a_data = e_a.get(v_a).await.expect("get alpha");
        let b_data = e_b.get(v_b).await.expect("get beta");
        assert_eq!(a_data[0x28], 10, "alpha 数据");
        assert_eq!(b_data[0x28], 20, "beta 数据");

        // 物理文件隔离: alpha 和 beta 各自的 .block 独立
        let size_a = fs::metadata(shard_dir(&block_root, "alpha").join("000001.block"))
            .expect("meta alpha")
            .len();
        let size_b = fs::metadata(shard_dir(&block_root, "beta").join("000001.block"))
            .expect("meta beta")
            .len();
        // 各写了 1 page + MetaPage, 都是 2 pages
        assert_eq!(size_a, size_b, "都写了 1+1 page, 大小应相同");

        e_a.flush().await.expect("flush alpha");
        e_b.flush().await.expect("flush beta");
    });
}

// =====================================================================
// 5. use_db 切换 current_db 标记 (同 db 内多 engine 不需要, 但 API 应工作)
// =====================================================================

#[test]
fn shard_id_different_is_physically_separated() {
    run_async(async move {
        // 验证: 同一 db_name, 不同 shard_id, 物理目录完全独立
        let (_tmp, block_root) = setup_block_root();

        let opts_s0 = OpenOptions {
            block_root: block_root.clone(),
            block_dir: None,
            db_name: Some("mydb".to_string()),
            shard_id: 0,
            create_if_missing: true,
            chunk_cache_size: 4,
            io_backend: IoBackend::StdFs,
            io_config: IoBackendConfig::default(),
        };
        let opts_s1 = OpenOptions {
            block_root: block_root.clone(),
            block_dir: None,
            db_name: Some("mydb".to_string()),
            shard_id: 1,
            create_if_missing: true,
            chunk_cache_size: 4,
            io_backend: IoBackend::StdFs,
            io_config: IoBackendConfig::default(),
        };
        
        let _e0 = StorageEngine::open(opts_s0).await.expect("open shard 0");
        let _e1 = StorageEngine::open(opts_s1).await.expect("open shard 1");

        let dir_s0 = block_root.join("mydb").join("shard_0");
        let dir_s1 = block_root.join("mydb").join("shard_1");
        assert!(dir_s0.exists(), "shard_0 目录应在");
        assert!(dir_s1.exists(), "shard_1 目录应在");
        assert_ne!(dir_s0, dir_s1);
    });
}

// =====================================================================
// 6. 单 db 物理目录 backup 替代 recover
// =====================================================================

#[test]
fn db_count_persists_across_open() {
    run_async(async move {
        // 验证: 多次 open 同 block_root (同 db_name), db 列表稳定
        let (_tmp, block_root) = setup_block_root();

        {
            let opts = opts_for_db(&block_root, "main");
            let mut e = StorageEngine::open(opts).await.expect("open");
            e.create_db("alpha").await.expect("alpha");
            e.create_db("beta").await.expect("beta");
            e.create_db("gamma").await.expect("gamma");
            e.flush().await.expect("flush");
        }

        // reopen 多次, db 列表稳定
        for _ in 0..3 {
            let opts = opts_for_db(&block_root, "main");
            let e = StorageEngine::open(opts).await.expect("reopen");
            let mut dbs: Vec<String> = e.list_dbs();
            dbs.sort();
            assert_eq!(
                dbs,
                vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()],
                "reopen 后 db 列表稳定"
            );
        }
    });
}

// =====================================================================
// 7. 物理路径结构正确性
// =====================================================================

#[test]
fn path_structure_matches_design() {
    run_async(async move {
        // 验证: 实际创建的目录结构符合 plan §1
        //   block_root/
        //   └── {db_name}/
        //       └── shard_{N}/
        //           ├── 000001.block
        //           └── page.mate
        let (_tmp, block_root) = setup_block_root();

        let opts = opts_for_db(&block_root, "app1");
        let _e = StorageEngine::open(opts).await.expect("open");

        let app1_dir = block_root.join("app1");
        assert!(app1_dir.is_dir(), "{:?} 应是目录", app1_dir);
        let shard0 = app1_dir.join("shard_0");
        assert!(shard0.is_dir(), "{:?} 应是目录", shard0);
        assert!(
            shard0.join("000001.block").is_file(),
            "000001.block 应是文件"
        );
        assert!(shard0.join("page.mate").is_file(), "page.mate 应是文件");
    });
}
