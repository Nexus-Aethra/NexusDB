//! 快速定位 cp[1] first_off=0x00D8 的问题.

use page::{ItemKind, PageIndex, leaf_insert, leaf_new};

fn next_rand(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[test]
fn debug_cp1_offset() {
    let mut page = leaf_new();
    let mut rng: u64 = 0x1234_5678_DEAD_BEEF;

    for op_idx in 0..53 {
        let key = format!("k_{:04}", next_rand(&mut rng) % 200).into_bytes();
        let _ = leaf_insert(&mut page, &key, b"v");

        // 每步后检查 PageIndex
        match PageIndex::load(&page, ItemKind::Leaf) {
            Ok(idx) => {
                eprintln!(
                    "op {} ok: segments={} key_count={}",
                    op_idx,
                    idx.segments.len(),
                    idx.key_count
                );
                for (i, seg) in idx.segments.iter().enumerate() {
                    eprintln!(
                        "  seg[{}] item_count={} first_off={}",
                        i, seg.item_count, seg.first_item_off
                    );
                }
            }
            Err(e) => {
                eprintln!("op {} FAILED: {}", op_idx, e);
                break;
            }
        }
    }
}
