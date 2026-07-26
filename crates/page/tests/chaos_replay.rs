//! 用同样的 RNG seed 重放 chaos 5000, 但每次操作后用 PageIndex::load 校验.
//! 若 PageIndex::load 失败, dump 整页并 panic.
//! 也跑全量 truth 校验.

use page::dump::dump_leaf_page_to_stderr;
use page::{
    ItemKind, PageIndex, leaf_delete, leaf_get, leaf_insert, leaf_new, page_free_off,
    page_key_count, read_checkpoint_header,
};
use std::collections::HashMap;

fn next_rand(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

const CHAOS_SEED: u64 = 0x1234_5678_DEAD_BEEF;
const MAX_OPS: usize = 1000;

/// 关键路径 1: 头部插入(反复插不同 key)
#[test]
fn chaos_minimal_head_inserts() {
    let mut page = leaf_new();
    let mut truth: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut rng = CHAOS_SEED;
    let mut last_ok: i64 = -1;
    for op_idx in 0..MAX_OPS {
        let key = format!("k_{:04}", next_rand(&mut rng) % 200).into_bytes();
        let key_for_err = key.clone();
        if let Ok(()) = leaf_insert(&mut page, &key, b"v") {
            truth.insert(key, b"v".to_vec());
        }
        if let Err(e) = PageIndex::load(&page, ItemKind::Leaf) {
            eprintln!(
                "PageIndex corrupted at op {} (last ok {}): {}",
                op_idx, last_ok, e
            );
            eprintln!(
                "  last insert key={:?}",
                String::from_utf8_lossy(&key_for_err)
            );
            eprintln!(
                "  state: key_count={} free_off={} truth_size={}",
                page_key_count(&page),
                page_free_off(&page),
                truth.len()
            );
            dump_leaf_page_to_stderr(&page);
            panic!("PageIndex corrupted at op {}: {}", op_idx, e);
        }
        last_ok = op_idx as i64;
    }
    // 全量验证
    for (k, v) in &truth {
        assert_eq!(leaf_get(&page, k).as_ref(), Some(v));
    }
}

/// 关键路径 2: 在中间 split 边界插入
#[test]
fn chaos_minimal_split_boundary() {
    let mut page = leaf_new();
    let mut rng = CHAOS_SEED;
    // 先插 80 个, 让 cp_count >= 3
    for i in 0..80 {
        let key = format!("k_{:04}", i).into_bytes();
        leaf_insert(&mut page, &key, b"v").unwrap();
    }
    let mut truth: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for i in 0..80 {
        let key = format!("k_{:04}", i).into_bytes();
        truth.insert(key, b"v".to_vec());
    }
    eprintln!(
        "after 80 inserts: cp_count={:?}",
        read_checkpoint_header(&page).0.checkpoint_count
    );

    // 再插 200 个, 触发多次 split + boundary 插入
    for _ in 0..200 {
        let key = format!("k_{:04}", next_rand(&mut rng) % 200).into_bytes();
        if let Ok(()) = leaf_insert(&mut page, &key, b"v") {
            truth.insert(key, b"v".to_vec());
        }
        if let Err(e) = PageIndex::load(&page, ItemKind::Leaf) {
            eprintln!("PageIndex corrupted after split_boundary: {e}");
            dump_leaf_page_to_stderr(&page);
            panic!();
        }
    }
    for (k, v) in &truth {
        assert_eq!(leaf_get(&page, k).as_ref(), Some(v));
    }
}

/// 关键路径 3: split 边界附近的 delete + insert 混合
#[test]
fn chaos_minimal_delete_split() {
    let mut page = leaf_new();
    let mut truth: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut rng = CHAOS_SEED;

    // 插 100 个, 触发多次 split
    for i in 0..100 {
        let key = format!("k_{:04}", i).into_bytes();
        leaf_insert(&mut page, &key, b"v").unwrap();
        truth.insert(key, b"v".to_vec());
    }

    // 删除 30 个 (跨 split 边界)
    let mut to_del: Vec<Vec<u8>> = Vec::new();
    for i in [
        10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 5, 15, 25, 35, 45, 55, 65, 75, 85, 32, 33, 34, 35,
        36, 37, 38, 39, 40, 41, 42,
    ] {
        let key = format!("k_{:04}", i).into_bytes();
        if truth.remove(&key).is_some() {
            to_del.push(key);
        }
    }
    for k in &to_del {
        let deleted = leaf_delete(&mut page, k).unwrap();
        assert!(deleted, "delete should find key");
    }
    if let Err(e) = PageIndex::load(&page, ItemKind::Leaf) {
        eprintln!("PageIndex corrupted after deletes: {e}");
        dump_leaf_page_to_stderr(&page);
        panic!();
    }

    // 再插 50 个
    for _ in 0..50 {
        let key = format!("k_{:04}", next_rand(&mut rng) % 200).into_bytes();
        if let Ok(()) = leaf_insert(&mut page, &key, b"v") {
            truth.insert(key, b"v".to_vec());
        }
        if let Err(e) = PageIndex::load(&page, ItemKind::Leaf) {
            eprintln!("PageIndex corrupted after mixed: {e}");
            dump_leaf_page_to_stderr(&page);
            panic!();
        }
    }

    // 验证
    for (k, v) in &truth {
        assert_eq!(leaf_get(&page, k).as_ref(), Some(v));
    }
}
