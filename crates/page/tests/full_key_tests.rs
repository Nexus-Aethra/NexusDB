//! ⭐ 回归: full_key 对损坏 item (shared_prefix_len 越界) 必须显式报错,
//! 不得静默截断产出错 key (审计 P2, C3 同族).

use page::{ItemKind, decode_item};

#[test]
fn full_key_rejects_shared_overflow() {
    // 构造一个 leaf item: shared=10, key_unshared="x", 但 prev_key 只有 3 字节
    // → shared(10) > prev_key.len()(3) → 必须 Err 而非静默截断成 "abc"+"x".
    let mut buf = Vec::new();
    buf.extend_from_slice(&10u16.to_le_bytes()); // shared_prefix_len = 10
    buf.extend_from_slice(&1u16.to_le_bytes()); // key_unshared_len = 1
    buf.push(b'x');
    // value_len varint (vint 小值 = 单字节)
    buf.push(3);
    buf.extend_from_slice(b"val");

    let (item, _n) = decode_item(&buf, 0, ItemKind::Leaf).unwrap();
    let prev_key = b"abc"; // 只有 3 字节 < shared=10
    let r = item.full_key(prev_key);
    assert!(
        r.is_err(),
        "shared_prefix_len 超过 prev_key 长度必须报错, 实际 {:?}",
        r.as_ref().map(|k| k.clone())
    );

    // 合法场景不受影响: shared=2, prev_key=b"abcdef"
    let mut buf2 = Vec::new();
    buf2.extend_from_slice(&2u16.to_le_bytes());
    buf2.extend_from_slice(&1u16.to_le_bytes());
    buf2.push(b'x');
    buf2.push(3);
    buf2.extend_from_slice(b"val");
    let (item2, _) = decode_item(&buf2, 0, ItemKind::Leaf).unwrap();
    assert_eq!(item2.full_key(b"abcdef").unwrap(), b"abx".to_vec());
}
