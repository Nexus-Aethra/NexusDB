//! 回归测试 (审计 P2-新): scanner 多 shard 遍历.
//!
//! 修复前: 14 个命令全部 `layout.shards.first()` — 只有 shard_0 对 scanner
//! 可见. 默认 6 shard 的目录只救 1/6 数据, 且无任何提示 (静默降级).
//! fix-mate 更糟: 静默修错 shard 的 page.mate.
//!
//! 本文件用真实引擎生成多 shard 数据目录 (ShardManager 落盘 + close flush),
//! 跑 scanner 二进制 (CARGO_BIN_EXE) 做端到端断言.
//!
//! 注: 引擎 page.mate 里 file_id 是 0 基, 而 .block 文件名是 1 基
//! (`{file_id+1:06}.block`), 故 mate 寻址的命令传 `--block-file-id-override 1`
//! (该 flag 的既有用途); export-force 直读 block 文件, 不受影响.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::Command;

use shard_manager::{ShardManager, ShardManagerOptions};
use storage::{IoBackend, IoBackendConfig};

const NUM_KEYS: u32 = 30;

struct Fixture {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    /// (key, 所在 shard) — 由引擎 route 计算, 用于挑非 shard_0 的键.
    key_shard: Vec<(String, usize)>,
}

fn setup(num_shards: usize) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let opts = ShardManagerOptions {
        num_shards,
        block_root: tmp.path().to_path_buf(),
        create_if_missing: true,
        io_backend: IoBackend::StdFs,
        io_config: IoBackendConfig::default(),
        chunk_cache_size: 4,
        reply_bus_count: None,
        wal_mode: Default::default(),
    };
    let mgr = ShardManager::open(opts).expect("open mgr");
    mgr.create_db("app").expect("create db");
    mgr.create_table("app", "kv").expect("create table");
    let mut key_shard = Vec::new();
    for i in 0..NUM_KEYS {
        let key = format!("key{i:03}");
        let val = format!("val{i:03}");
        mgr.put("app", "kv", key.as_bytes(), val.as_bytes(), 0)
            .expect("put");
        key_shard.push((key.clone(), mgr.route("app", "kv", key.as_bytes())));
    }
    // close 会 flush 全部 shard (dirty data 落盘) — scanner 离线读的前提.
    mgr.close().expect("close");
    let root = tmp.path().to_path_buf();
    Fixture {
        _tmp: tmp,
        root,
        key_shard,
    }
}

/// 跑 scanner. `mate_bridge` = 是否传 --block-file-id-override 1.
fn scan(fx: &Fixture, mate_bridge: bool, args: &[&str]) -> (String, i32) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_nexusdb-scanner"));
    cmd.arg("--dir").arg(&fx.root);
    if mate_bridge {
        cmd.arg("--block-file-id-override").arg("1");
    }
    cmd.args(args);
    let out = cmd.output().expect("run scanner");
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        out.status.code().unwrap_or(-1),
    )
}

fn json_lines(stdout: &str) -> Vec<serde_json::Value> {
    stdout
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .collect()
}

/// 物理键 = keyspace 编码 `[kind=S][varint(klen)][logical_key]`.
fn phys_key(key: &str) -> String {
    format!("53{:02x}{}", key.len(), hex::encode(key))
}

#[test]
fn export_force_rescues_all_shards() {
    let fx = setup(3);
    let (stdout, code) = scan(&fx, false, &["export-force", "-f", "kv"]);
    assert_eq!(code, 0, "export-force 应成功");
    let keys: BTreeSet<String> = stdout
        .lines()
        .filter(|l| !l.starts_with("[EXPORT-FORCE]"))
        .filter_map(|l| l.split('\t').nth(2).map(|s| s.to_string()))
        .collect();
    // 30 个键必须全部在 (修复前只剩 shard_0 的那十几个).
    // 注: 暴力扫也会解出 catalog 页 (meta/table_dir) 的条目, 故只断言超集.
    assert!(
        keys.len() >= NUM_KEYS as usize,
        "export-force 应救出全部 shard 的键 (got {})",
        keys.len()
    );
    for (key, _) in &fx.key_shard {
        let logical = hex::encode(key);
        assert!(keys.contains(&logical), "键 {key} 缺失 (静默丢数据)");
    }
}

#[test]
fn export_tree_walks_all_shards() {
    let fx = setup(3);
    let (stdout, code) = scan(&fx, true, &["export", "2"]);
    assert_eq!(code, 0, "export 应成功");
    let keys: BTreeSet<String> = stdout
        .lines()
        .filter(|l| !l.contains("[BAD-PAGE-SKIPPED]"))
        .filter_map(|l| l.split('\t').next().map(|s| s.to_string()))
        .collect();
    assert_eq!(
        keys.len(),
        NUM_KEYS as usize,
        "export -tree 应导出全部 shard 的行 (修复前只有 shard_0)"
    );
}

#[test]
fn dbs_lists_tables_from_every_shard() {
    let fx = setup(3);
    let (stdout, code) = scan(&fx, true, &["dbs", "--json"]);
    assert_eq!(code, 0);
    let rows = json_lines(&stdout);
    let shards: BTreeSet<u64> = rows
        .iter()
        .filter_map(|v| v.get("shard").and_then(|s| s.as_u64()))
        .collect();
    assert_eq!(
        shards,
        BTreeSet::from([0, 1, 2]),
        "dbs 应列出 3 个 shard 的表"
    );
    for r in &rows {
        assert_eq!(r["table_name"], "kv");
        assert_eq!(r["root_vpid"], 2);
    }
}

#[test]
fn verify_aggregates_all_shards() {
    let fx = setup(3);
    let (stdout, code) = scan(&fx, true, &["verify", "2", "--json"]);
    assert_eq!(code, 0);
    let summary = json_lines(&stdout)
        .into_iter()
        .find(|v| v["event"] == "verify_summary")
        .expect("应有 verify_summary");
    assert_eq!(summary["ok"], 3, "3 个 shard 各一棵树都应验到");
    assert_eq!(summary["bad"], 0);
}

#[test]
fn lookup_finds_key_in_nonzero_shard() {
    let fx = setup(3);
    // 挑一个不在 shard_0 的键 — 修复前只搜 shard_0, 必然 not found
    let (key, shard) = fx
        .key_shard
        .iter()
        .find(|(_, s)| *s != 0)
        .expect("30 个键应至少有一个落在非 shard_0");
    let (stdout, code) = scan(&fx, true, &["lookup", "--key", &phys_key(key), "2"]);
    assert_eq!(
        code, 0,
        "shard {shard} 的键 {key} 应被找到 (修复前只搜 shard_0)"
    );
    let pk = phys_key(key);
    assert!(
        stdout.contains(&pk) && stdout.contains(&hex::encode(format!("val{}", &key[3..]))),
        "命中行应含键与值: {stdout}"
    );
}

#[test]
fn header_reports_every_shard_page() {
    let fx = setup(3);
    let (stdout, code) = scan(&fx, true, &["header", "2", "--json"]);
    assert_eq!(code, 0);
    let shards: BTreeSet<u64> = json_lines(&stdout)
        .iter()
        .filter_map(|v| v.get("shard").and_then(|s| s.as_u64()))
        .collect();
    assert_eq!(shards, BTreeSet::from([0, 1, 2]), "header 应逐 shard 解码");
    // 三个 shard 的 key_count 之和 = 全部键数
    let total: u64 = json_lines(&stdout)
        .iter()
        .map(|v| v["key_count"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(total, NUM_KEYS as u64, "各 shard 页键数之和应等于总键数");
}

#[test]
fn map_dumps_every_shard_mate() {
    let fx = setup(3);
    let (stdout, code) = scan(&fx, true, &["map", "--from-mate-only", "--json"]);
    assert_eq!(code, 0);
    let shards: BTreeSet<u64> = json_lines(&stdout)
        .iter()
        .filter_map(|v| v.get("shard").and_then(|s| s.as_u64()))
        .collect();
    assert_eq!(
        shards,
        BTreeSet::from([0, 1, 2]),
        "map 应导出全部 shard 的 mate"
    );
}

#[test]
fn fix_mate_refuses_to_guess_shard() {
    let fx = setup(3);
    // 多 shard 无 --shard: 必须显式报错 (修复前静默修 shard_0 的 mate)
    let (_, code) = scan(&fx, false, &["fix-mate", "--vpid", "5", "--page-idx", "5"]);
    assert_ne!(code, 0, "多 shard 目录下 fix-mate 必须要求 --shard");
    // 指定 shard 后正常 dry-run
    let (stdout, code) = scan(
        &fx,
        false,
        &["fix-mate", "--shard", "1", "--vpid", "5", "--page-idx", "5"],
    );
    assert_eq!(code, 0);
    assert!(stdout.contains("[DRY-RUN]"), "dry-run 应有提示: {stdout}");
}

#[test]
fn single_shard_dir_still_works() {
    // 向后兼容: 单 shard 目录 (文档化的 v1 形态) 行为不变
    let fx = setup(1);
    let (stdout, code) = scan(&fx, false, &["export-force", "-f", "kv"]);
    assert_eq!(code, 0);
    let keys: BTreeSet<String> = stdout
        .lines()
        .filter(|l| !l.starts_with("[EXPORT-FORCE]"))
        .filter_map(|l| l.split('\t').nth(2).map(|s| s.to_string()))
        .collect();
    assert!(keys.len() >= NUM_KEYS as usize);
    for (key, _) in &fx.key_shard {
        assert!(keys.contains(&hex::encode(key)), "键 {key} 缺失");
    }

    // 单 shard 下 fix-mate 无需 --shard (旧行为保留)
    let (_, code) = scan(&fx, false, &["fix-mate", "--vpid", "5", "--page-idx", "5"]);
    assert_eq!(code, 0, "单 shard 目录 fix-mate 应无需 --shard");
}
