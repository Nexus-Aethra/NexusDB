//! 测试 reduce 复杂用例以找到 shared_prefix_len != 0 的 bug 来源.

use page::dump::dump_leaf_page_to_stderr;
use page::{
    ItemKind, PageIndex, leaf_delete, leaf_get, leaf_insert, leaf_new, page_free_off,
    page_key_count,
};
use std::collections::HashMap;

fn next_rand(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// 跑 chaos test 但每次操作后 print 关键状态. 如果 panic, 我们能定位是哪个 op 导致的.
#[test]
fn leaf_chaos_with_state_dump() {
    let mut page = leaf_new();
    let mut truth: HashMap<String, String> = HashMap::new();
    let mut rng_state: u64 = 0x1234_5678_DEAD_BEEF;

    let max_ops = 5000;
    for op_idx in 0..max_ops {
        let op = next_rand(&mut rng_state) % 3;
        let key = format!("k_{:05}", next_rand(&mut rng_state) % 200);
        let val = format!("v{op_idx}");

        let op_name = match op {
            0 => "INSERT",
            1 => "DELETE",
            _ => "GET",
        };
        eprintln!(">>> op {}: {} key={}", op_idx, op_name, key);

        match op {
            0 => {
                if let Ok(()) = leaf_insert(&mut page, key.as_bytes(), val.as_bytes()) {
                    truth.insert(key.clone(), val);
                }
            }
            1 => {
                let existed = truth.remove(&key).is_some();
                let deleted = leaf_delete(&mut page, key.as_bytes()).unwrap();
                assert_eq!(deleted, existed, "delete mismatch at op {op_idx} key={key}");
            }
            _ => {
                let got = leaf_get(&page, key.as_bytes());
                let expected = truth.get(&key).map(|v| v.as_bytes().to_vec());
                assert_eq!(got, expected, "get mismatch at op {op_idx} key={key}");
            }
        }

        // 每 50 次操作 dump 一次 page 状态
        if op_idx % 50 == 0 {
            eprintln!(
                "=== op {op_idx}: key_count={} free_off={} truth_size={} ===",
                page_key_count(&page),
                page_free_off(&page),
                truth.len()
            );
        }
    }
    eprintln!("completed {max_ops} ops without panic");
}

/// 用相同 RNG seed 重放 chaos, 但每步后用 PageIndex::load 校验 page 完整性.
/// 如果 PageIndex::load 失败, 调用 dump_leaf_page_to_stderr 把整页打印出来,
/// 然后 panic. 这样我们能看到 panic 触发时的完整 page 结构.
#[test]
fn leaf_chaos_replay_with_dump_on_failure() {
    let mut page = leaf_new();
    let mut truth: HashMap<String, String> = HashMap::new();
    let mut rng_state: u64 = 0x1234_5678_DEAD_BEEF;

    let max_ops = 5000;
    let mut last_ok: i64 = -1;
    for op_idx in 0..max_ops {
        let op = next_rand(&mut rng_state) % 3;
        let key = format!("k_{:05}", next_rand(&mut rng_state) % 200);
        let val = format!("v{op_idx}");

        match op {
            0 => {
                let _ = leaf_insert(&mut page, key.as_bytes(), val.as_bytes());
                truth.insert(key.clone(), val);
            }
            1 => {
                truth.remove(&key);
                let _ = leaf_delete(&mut page, key.as_bytes());
            }
            _ => {
                let _ = leaf_get(&page, key.as_bytes());
            }
        }

        // 每步后用 PageIndex::load 校验 page 完整性
        match PageIndex::load(&page, ItemKind::Leaf) {
            Ok(_) => {
                last_ok = op_idx as i64;
            }
            Err(e) => {
                eprintln!(
                    "!!! PageIndex load FAILED at op {op_idx} (last ok {}): {}",
                    last_ok, e
                );
                eprintln!(
                    "    last op: {} key={}",
                    match op {
                        0 => "INSERT",
                        1 => "DELETE",
                        _ => "GET",
                    },
                    key
                );
                eprintln!(
                    "    key_count={} free_off={} truth_size={}",
                    page_key_count(&page),
                    page_free_off(&page),
                    truth.len()
                );
                eprintln!(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                // 写到 /tmp 避免 panic 输出把 dump 切碎
                let dump_path = format!("/tmp/page_dump_op_{}.txt", op_idx);
                let dump_text = page::dump::dump_leaf_page(&page);
                if let Err(write_err) = std::fs::write(&dump_path, &dump_text) {
                    eprintln!("failed to write dump file: {write_err}");
                    dump_leaf_page_to_stderr(&page);
                } else {
                    eprintln!("full page dump written to {}", dump_path);
                }
                eprintln!("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
                panic!("PageIndex corrupted at op {op_idx}: {e}");
            }
        }
    }
    eprintln!("completed {max_ops} ops without corruption");
}

/// 还原失败时的具体路径: insert 几个 key, 然后 delete 中间 key, 然后 insert 更小 key
#[test]
fn leaf_delete_head_then_insert_smaller() {
    let mut page = leaf_new();

    // 插 a, b, c
    leaf_insert(&mut page, b"a", b"1").unwrap();
    leaf_insert(&mut page, b"b", b"2").unwrap();
    leaf_insert(&mut page, b"c", b"3").unwrap();
    eprintln!("after insert a,b,c: key_count={}", page_key_count(&page));

    // 删 a (段首)
    leaf_delete(&mut page, b"a").unwrap();
    eprintln!("after delete a: key_count={}", page_key_count(&page));

    // 插 a (再次回到段首)
    leaf_insert(&mut page, b"a", b"11").unwrap();
    eprintln!("after reinsert a: key_count={}", page_key_count(&page));

    // 验证
    assert_eq!(leaf_get(&page, b"a").unwrap(), b"11");
    assert_eq!(leaf_get(&page, b"b").unwrap(), b"2");
    assert_eq!(leaf_get(&page, b"c").unwrap(), b"3");
}
