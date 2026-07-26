//! 重放 chaos 跑到 op 363, 复现 cp[N] 段首 shared 非 0 的 bug.

use page::dump::dump_leaf_page_to_stderr;
use page::{ItemKind, PageIndex, leaf_insert, leaf_new, page_free_off, page_key_count};

fn next_rand(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn dump_idx_state(label: &str, page: &[u8]) {
    eprintln!("==== {} ====", label);
    let idx = PageIndex::load(page, ItemKind::Leaf);
    match idx {
        Ok(idx) => {
            eprintln!(
                "  PageIndex OK: key_count={} segments={}",
                idx.key_count,
                idx.segments.len()
            );
            for (i, seg) in idx.segments.iter().enumerate() {
                eprintln!(
                    "    seg[{}] item_count={} first_item_off={} first_full_key={:?}",
                    i,
                    seg.item_count,
                    seg.first_item_off,
                    String::from_utf8_lossy(&seg.first_full_key)
                );
            }
        }
        Err(e) => {
            eprintln!("  PageIndex CORRUPTED: {}", e);
            dump_leaf_page_to_stderr(page);
        }
    }
}

#[test]
fn chaos_replay_363() {
    let mut page = leaf_new();
    let mut rng: u64 = 0x1234_5678_DEAD_BEEF;

    // 跑到 op_idx=360 全部成功
    for op_idx in 0..361 {
        let key = format!("k_{:04}", next_rand(&mut rng) % 200).into_bytes();
        let _ = leaf_insert(&mut page, &key, b"v");
        if let Err(e) = PageIndex::load(&page, ItemKind::Leaf) {
            eprintln!("PageIndex corrupted early at op {}: {}", op_idx, e);
            dump_leaf_page_to_stderr(&page);
            panic!();
        }
    }
    dump_idx_state("PageIndex after op 360", &page);

    // 跑剩余的 4 步 (op 361, 362, 363) - 跟踪每一步的 PageIndex 状态
    for op_idx in 361..365 {
        let key = format!("k_{:04}", next_rand(&mut rng) % 200).into_bytes();
        eprintln!(
            "\n==== op {} insert key={:?} ====",
            op_idx,
            String::from_utf8_lossy(&key)
        );
        let _ = leaf_insert(&mut page, &key, b"v");
        eprintln!(
            "  after insert: key_count={} free_off={}",
            page_key_count(&page),
            page_free_off(&page)
        );
        dump_idx_state(&format!("after op {}", op_idx), &page);
        // 显式校验
        if PageIndex::load(&page, ItemKind::Leaf).is_err() {
            panic!("op {} corrupted", op_idx);
        }
    }
}
