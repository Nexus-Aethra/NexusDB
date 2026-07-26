use page::{
    ItemKind, PageIndex, leaf_get, leaf_insert, leaf_new, leaf_split, page_free_off, page_key_count,
};

#[test]
fn debug_split_get() {
    let mut left = leaf_new();
    for i in 0..20 {
        let key = format!("k_{:04}", i);
        leaf_insert(&mut left, key.as_bytes(), b"v").unwrap();
    }
    let mut right = leaf_new();
    let split_key = leaf_split(&mut left, &mut right).unwrap();
    eprintln!("split_key={:?}", String::from_utf8_lossy(&split_key));
    eprintln!(
        "left kc={} right kc={}",
        page_key_count(&left),
        page_key_count(&right)
    );
    eprintln!(
        "left fo={} right fo={}",
        page_free_off(&left),
        page_free_off(&right)
    );

    let right_idx = PageIndex::load(&right, ItemKind::Leaf).unwrap();
    eprintln!(
        "right idx: key_count={} segments.len()={}",
        right_idx.key_count,
        right_idx.segments.len()
    );
    for (i, s) in right_idx.segments.iter().enumerate() {
        eprintln!(
            "  [{}] first_off={} count={} first_key={:?}",
            i,
            s.first_item_off,
            s.item_count,
            String::from_utf8_lossy(&s.first_full_key)
        );
    }
    eprintln!(
        "leaf_get(right, k_0010) = {:?}",
        leaf_get(&right, b"k_0010")
    );
    eprintln!(
        "leaf_get(right, k_0011) = {:?}",
        leaf_get(&right, b"k_0011")
    );
}
