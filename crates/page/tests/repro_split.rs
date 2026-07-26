//! 复现 leaf_random_chaos_5000_ops 中段首 shared=0 失效问题.
//! 用确定性的操作序列, 每步校验 PageIndex::load 是否成功.

use page::{ItemKind, PageIndex, leaf_delete, leaf_get, leaf_insert, leaf_new};
use std::collections::HashMap;

fn next_rand(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// 用相同 RNG seed 重放 chaos, 每步后用 PageIndex::load 校验 page 完整性.
/// 如果 PageIndex::load 失败就 panic, 报告最后一次成功的 op_idx.
#[test]
fn replay_with_integrity_check() {
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
        if PageIndex::load(&page, ItemKind::Leaf).is_err() {
            eprintln!(
                "!!! PageIndex load FAILED at op {op_idx} (last ok {})",
                last_ok
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
            panic!("PageIndex corrupted at op {op_idx}");
        }
        last_ok = op_idx as i64;
    }
    eprintln!("completed {max_ops} ops without corruption");
}
