//! 仅用于调试 internal_random_chaos_3000_ops 在 op 102 的 crash.

use page::dump::dump_internal_page_to_stderr;
use page::{ItemKind, PageIndex, internal_child, internal_insert, internal_new, page_set_vpid};

fn next_rand(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[test]
fn internal_debug_102() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 0);
    let mut rng: u64 = 0xDEAD_BEEF_CAFE_F00D;

    for op_idx in 0..103 {
        let op = next_rand(&mut rng) % 2;
        let key = format!("s_{:04}", next_rand(&mut rng) % 100);
        let vpid = 1000 + (op_idx as u64);

        match op {
            0 => {
                let _ = internal_insert(&mut page, key.as_bytes(), vpid);
            }
            _ => {
                let _ = internal_child(&page, key.as_bytes());
            }
        }

        if let Err(e) = PageIndex::load(&page, ItemKind::Internal) {
            eprintln!("PageIndex corrupted at op {}: {}", op_idx, e);
            dump_internal_page_to_stderr(&page);
            panic!("op {} corrupted", op_idx);
        }
    }
    eprintln!("completed 103 ops without corruption");
}
