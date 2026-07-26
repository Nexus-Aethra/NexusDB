//! 演示 dump 工具的样例测试 — 用来肉眼检查 dump 输出格式.

use page::dump::{dump_internal_page, dump_leaf_page};
use page::{internal_insert, internal_new, leaf_delete, leaf_insert, leaf_new};

#[test]
fn dump_leaf_demo() {
    let mut page = leaf_new();
    leaf_insert(&mut page, b"alpha", b"1").unwrap();
    leaf_insert(&mut page, b"beta", b"22").unwrap();
    leaf_insert(&mut page, b"gamma", b"333").unwrap();
    leaf_delete(&mut page, b"beta").unwrap();
    leaf_insert(&mut page, b"zeta", b"zzz").unwrap();

    let out = dump_leaf_page(&page);
    eprintln!("\n{out}");
    // 简单 sanity
    assert!(out.contains("Leaf"));
    assert!(out.contains("alpha"));
    assert!(out.contains("gamma"));
    assert!(out.contains("zeta"));
    // beta 已被删除, 不应出现在 items 中
    assert!(!out.contains("\"beta\""));
}

#[test]
fn dump_internal_demo() {
    let mut page = internal_new();
    internal_insert(&mut page, b"k_010", 0x100).unwrap();
    internal_insert(&mut page, b"k_020", 0x200).unwrap();
    internal_insert(&mut page, b"k_030", 0x300).unwrap();
    internal_insert(&mut page, b"k_040", 0x400).unwrap();

    let out = dump_internal_page(&page);
    eprintln!("\n{out}");
    assert!(out.contains("Internal"));
    assert!(out.contains("k_010"));
    assert!(out.contains("k_040"));
    assert!(out.contains("child_vpid"));
}
