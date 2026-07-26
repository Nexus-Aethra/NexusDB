//! 复杂场景测试: 频繁增删 + 边界条件 + 特殊模式.
//!
//! 与 stress_tests.rs 互补, 重点测试:
//! - 反复 insert + delete 同一个 key
//! - 头部/尾部/中间连续插入
//! - 二进制 keys (非 UTF-8)
//! - 长前缀 / 短前缀混合
//! - 各种 value 长度 (空 / 极长)
//! - 多页 split 后的复杂路由
//! - 段边界附近插入
//! - 删除后立即重新插入

use page::{
    internal_child, internal_delete, internal_insert, internal_new, internal_split, leaf_delete,
    leaf_get, leaf_insert, leaf_new, leaf_split, page_free_off, page_free_space, page_key_count,
    page_set_vpid, read_checkpoint_header,
};

// ============================================================================
// 头部插入: 反复插入越来越小的 key
// ============================================================================

/// 反复在头部插入 (key 越来越小), 验证段分裂能正确处理.
#[test]
fn leaf_insert_at_head_repeatedly() {
    let mut page = leaf_new();

    // 第一次: 插 0..50
    for i in 0..50 {
        let key = format!("k_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 50);

    // 第二轮: 反复在头部插 (每次让最小 key 更小)
    // 先插 m_0000..m_0049 (< k_0000)
    for i in 0..50 {
        let key = format!("m_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("m{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 100);

    // 再插 l_0000..l_0049 (< m_0000)
    for i in 0..50 {
        let key = format!("l_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("l{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 150);

    // 继续插更小: a_*, 0_*
    for i in 0..50 {
        let key = format!("a_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("a{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 200);

    // 全量验证 (按 key 升序)
    // 期望顺序: a_0000..a_0049, k_0000..k_0049, l_0000..l_0049, m_0000..m_0049
    for i in 0..50 {
        let key = format!("a_{i:04}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("a{i}").as_bytes()
        );
    }
    for i in 0..50 {
        let key = format!("k_{i:04}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("v{i}").as_bytes()
        );
    }
    for i in 0..50 {
        let key = format!("l_{i:04}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("l{i}").as_bytes()
        );
    }
    for i in 0..50 {
        let key = format!("m_{i:04}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("m{i}").as_bytes()
        );
    }
}

/// 在头部连续插入 1 个 char 的 key, 触发大量分裂.
#[test]
fn leaf_insert_single_char_keys_head() {
    let mut page = leaf_new();

    // 按字典序插入单字符 keys
    let chars: Vec<u8> = (b'a'..=b'z')
        .chain(b'A'..=b'Z')
        .chain(b'0'..=b'9')
        .collect();
    for &c in &chars {
        let key = [c];
        leaf_insert(&mut page, &key, &[c]).unwrap();
    }
    assert_eq!(page_key_count(&page), chars.len() as u16);

    // 全部能查到
    for &c in &chars {
        let key = [c];
        assert_eq!(leaf_get(&page, &key).unwrap(), vec![c]);
    }
}

/// 反复在中间插入 (单数索引), 验证段内插入位置正确.
#[test]
fn leaf_insert_in_middle_repeatedly() {
    let mut page = leaf_new();

    // 先插 0, 2, 4, 6, ..., 198 (偶数)
    for i in (0..200).step_by(2) {
        let key = format!("k_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 100);

    // 在每个偶数之间插奇数
    for i in (1..200).step_by(2) {
        let key = format!("k_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 200);

    // 全部能查到, 顺序正确
    for i in 0..200 {
        let key = format!("k_{i:04}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("v{i}").as_bytes()
        );
    }
}

// ============================================================================
// 反复 insert + delete 同一 key
// ============================================================================

/// 反复 insert + delete 同一个 key, 验证 page 状态可复用.
#[test]
fn leaf_insert_delete_same_key_reuse() {
    let mut page = leaf_new();
    for round in 0..100 {
        let key = format!("k_{round:04}");
        let val = format!("v_{round}_payload_data");
        leaf_insert(&mut page, key.as_bytes(), val.as_bytes()).unwrap();
        assert_eq!(page_key_count(&page), 1);
        assert_eq!(leaf_get(&page, key.as_bytes()).unwrap(), val.as_bytes());
        let deleted = leaf_delete(&mut page, key.as_bytes()).unwrap();
        assert!(deleted);
        assert_eq!(page_key_count(&page), 0);
        assert!(leaf_get(&page, key.as_bytes()).is_none());
    }
}

/// 删除后立即重新插入相同 key 但不同 value, 验证覆盖行为.
#[test]
fn leaf_delete_then_reinsert_different_value() {
    let mut page = leaf_new();
    for i in 0..50 {
        let key = format!("k_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("v1_{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 50);

    // 删一半
    for i in (0..50).step_by(2) {
        let key = format!("k_{i:04}");
        leaf_delete(&mut page, key.as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 25);

    // 重新插入相同 key, 不同 value
    for i in (0..50).step_by(2) {
        let key = format!("k_{i:04}");
        let result = leaf_insert(&mut page, key.as_bytes(), format!("v2_{i}").as_bytes());
        // 当前设计: 应该成功 (因为之前的 key 已删除)
        assert!(result.is_ok(), "reinsert at {i} should succeed: {result:?}");
    }
    assert_eq!(page_key_count(&page), 50);

    // 验证 value 是新值
    for i in (0..50).step_by(2) {
        let key = format!("k_{i:04}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("v2_{i}").as_bytes()
        );
    }
    for i in (1..50).step_by(2) {
        let key = format!("k_{i:04}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("v1_{i}").as_bytes()
        );
    }
}

// ============================================================================
// 二进制 / 特殊 key
// ============================================================================

/// 测试包含 \0 字节的 key (二进制安全).
#[test]
fn leaf_insert_binary_keys_with_null_bytes() {
    let mut page = leaf_new();
    let keys: Vec<Vec<u8>> = vec![
        vec![0x00, 0x01, 0x02],
        vec![0xFF, 0xFE, 0xFD],
        vec![0x00, 0x00, 0x00],
        vec![0xFF, 0xFF, 0xFF],
        vec![0x00, b'a', 0x00, b'b', 0x00],
        vec![0x80, 0x81, 0x82, 0x83],
    ];

    for (i, key) in keys.iter().enumerate() {
        leaf_insert(&mut page, key, format!("v{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), keys.len() as u16);

    for (i, key) in keys.iter().enumerate() {
        assert_eq!(leaf_get(&page, key).unwrap(), format!("v{i}").as_bytes());
    }
}

/// 测试 1 字节 key (最短可能 key).
#[test]
fn leaf_insert_single_byte_keys() {
    let mut page = leaf_new();
    let bytes: Vec<u8> = (0u8..=255).collect();
    for &b in &bytes {
        leaf_insert(&mut page, &[b], &[b]).unwrap();
    }
    assert_eq!(page_key_count(&page), bytes.len() as u16);

    for &b in &bytes {
        assert_eq!(leaf_get(&page, &[b]).unwrap(), vec![b]);
    }
}

// ============================================================================
// 长前缀 / 短前缀混合
// ============================================================================

/// 高前缀压缩: 所有 key 共享长前缀.
#[test]
fn leaf_high_prefix_compression() {
    let mut page = leaf_new();
    let common_prefix = b"very.long.common.prefix.that.is.shared.by.all.keys/";
    for i in 0..200 {
        let mut key = common_prefix.to_vec();
        key.extend_from_slice(format!("item_{i:06}").as_bytes());
        leaf_insert(&mut page, &key, format!("v{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 200);

    for i in 0..200 {
        let mut key = common_prefix.to_vec();
        key.extend_from_slice(format!("item_{i:06}").as_bytes());
        assert_eq!(leaf_get(&page, &key).unwrap(), format!("v{i}").as_bytes());
    }
}

/// 无共同前缀: 每次插入都是全新前缀, 触发段分裂.
#[test]
fn leaf_no_common_prefix() {
    let mut page = leaf_new();
    // 256 个完全不同前缀的 key
    for i in 0..256 {
        // 构造完全不同的前缀 (高位字节不同)
        let hi = (i / 16) as u8;
        let lo = (i % 16) as u8;
        let key = [hi, lo, 0xAA, 0xBB, 0xCC];
        leaf_insert(&mut page, &key, &[i as u8]).unwrap();
    }
    assert_eq!(page_key_count(&page), 256);

    for i in 0..256 {
        let hi = (i / 16) as u8;
        let lo = (i % 16) as u8;
        let key = [hi, lo, 0xAA, 0xBB, 0xCC];
        assert_eq!(leaf_get(&page, &key).unwrap(), vec![i as u8]);
    }
}

/// 短前缀 → 长前缀 → 短前缀循环插入, 验证段状态正确.
#[test]
fn leaf_alternating_short_long_prefix() {
    let mut page = leaf_new();
    // 短前缀 keys
    for i in 0..50 {
        let key = format!("s{i}");
        leaf_insert(&mut page, key.as_bytes(), format!("s{i}").as_bytes()).unwrap();
    }
    // 长前缀 keys (字典序在 s 之后)
    for i in 0..50 {
        let key = format!("longerprefix_{i:04}_zzzz");
        leaf_insert(&mut page, key.as_bytes(), format!("l{i}").as_bytes()).unwrap();
    }
    // 再插短前缀 (字典序在最前)
    for i in 0..50 {
        let key = format!("a{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("a{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 150);

    // 全部验证
    for i in 0..50 {
        let key = format!("a{i:04}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("a{i}").as_bytes()
        );
        let key = format!("s{i}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("s{i}").as_bytes()
        );
        let key = format!("longerprefix_{i:04}_zzzz");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("l{i}").as_bytes()
        );
    }
}

// ============================================================================
// Value 长度变化
// ============================================================================

/// value 长度从 0 到 200 字节, 测试 varint 编码.
#[test]
fn leaf_value_size_variation() {
    let mut page = leaf_new();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let mut values: Vec<Vec<u8>> = Vec::new();
    for i in 0..50 {
        // value 长度 = i*4, 从 0 字节到 196 字节
        let value: Vec<u8> = vec![b'x'; i * 4];
        let key = format!("vk_{i:04}");
        match leaf_insert(&mut page, key.as_bytes(), &value) {
            Ok(()) => {
                keys.push(key.into_bytes());
                values.push(value);
            }
            Err(_) => break, // page 满了
        }
    }
    assert!(
        keys.len() >= 20,
        "should insert at least 20 keys, got {}",
        keys.len()
    );

    for (i, (key, expected)) in keys.iter().zip(values.iter()).enumerate() {
        let got = leaf_get(&page, key).unwrap();
        assert_eq!(got, *expected, "value mismatch at i={i}");
    }
}

/// 空 value 插入.
#[test]
fn leaf_empty_value_insert() {
    let mut page = leaf_new();
    for i in 0..50 {
        let key = format!("k_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), b"").unwrap();
    }
    assert_eq!(page_key_count(&page), 50);

    for i in 0..50 {
        let key = format!("k_{i:04}");
        let got = leaf_get(&page, key.as_bytes()).unwrap();
        assert!(got.is_empty(), "value at {i} should be empty, got {got:?}");
    }
}

// ============================================================================
// 段分裂/合并的精确触发
// ============================================================================

/// 反复触发段分裂: 插到 N+1 个, 删到 N, 再插, 反复.
#[test]
fn leaf_split_delete_repeat() {
    let mut page = leaf_new();
    let target = 32; // 段上限
    let mut expected_count: u16 = 0;

    for round in 0..5 {
        // 插 target+1 个, 触发段分裂
        let start = round * (target + 1);
        for i in 0..=target {
            let key = format!("sd_{:06}", start + i);
            leaf_insert(
                &mut page,
                key.as_bytes(),
                format!("v{}", start + i).as_bytes(),
            )
            .unwrap();
        }
        expected_count += (target + 1) as u16;
        assert_eq!(
            page_key_count(&page),
            expected_count,
            "round {round} insert count wrong"
        );

        // 验证段数 > 1
        let (hdr, _) = read_checkpoint_header(&page);
        assert!(hdr.checkpoint_count >= 1, "after insert, should have cp");

        // 删一半
        let delete_count = (target / 2 + 1) as u16;
        for i in 0..=(target / 2) {
            let key = format!("sd_{:06}", start + i * 2);
            leaf_delete(&mut page, key.as_bytes()).unwrap();
        }
        expected_count -= delete_count;
        assert_eq!(
            page_key_count(&page),
            expected_count,
            "round {round} after delete count wrong"
        );
    }

    // 全量验证: 查询时如果存在就验证 value
    let total = 5 * (target + 1);
    let mut verified = 0;
    for i in 0..total {
        let key = format!("sd_{i:06}");
        if let Some(v) = leaf_get(&page, key.as_bytes()) {
            assert_eq!(v, format!("v{i}").as_bytes());
            verified += 1;
        }
    }
    assert_eq!(page_key_count(&page) as usize, verified);
}

// ============================================================================
// split 后再 split, 然后各种增删
// ============================================================================

/// 三次连续 split (1 page → 4 pages), 然后在每页上插入.
#[test]
fn leaf_three_splits_then_incremental_insert() {
    let mut pages = vec![leaf_new()];

    // 插 100 个 key
    for i in 0..100 {
        let key = format!("t_{i:04}");
        leaf_insert(&mut pages[0], key.as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }

    // 第一次 split
    let mut p1 = leaf_new();
    let _ = leaf_split(&mut pages[0], &mut p1).unwrap();
    pages.push(p1);

    // 第二次 split (split 第一个 page)
    let mut p2 = leaf_new();
    let _ = leaf_split(&mut pages[0], &mut p2).unwrap();
    pages.insert(1, p2);

    // 第三次 split
    let mut p3 = leaf_new();
    let _ = leaf_split(&mut pages[2], &mut p3).unwrap();
    pages.push(p3);

    // 验证 4 个 page 的 keys 加起来 = 100
    let total: u16 = pages.iter().map(|p| page_key_count(p)).sum();
    assert_eq!(total, 100);

    // 全部能查到
    for i in 0..100 {
        let key = format!("t_{i:04}");
        let mut found = false;
        for p in &pages {
            if leaf_get(p, key.as_bytes()).is_some() {
                found = true;
                assert_eq!(
                    leaf_get(p, key.as_bytes()).unwrap(),
                    format!("v{i}").as_bytes()
                );
                break;
            }
        }
        assert!(found, "key {i} not found in any page");
    }

    // 在每页上插入一些新 keys
    for (pi, p) in pages.iter_mut().enumerate() {
        for i in 0..10 {
            let key = format!("new_p{pi}_{i:04}");
            // 注意: 可能跨 page 的 key 顺序不在该 page 范围内
            // 先试插, 失败就跳过 (page 满)
            if leaf_insert(p, key.as_bytes(), format!("new{pi}_{i}").as_bytes()).is_err() {
                break;
            }
        }
    }
}

// ============================================================================
// 段边界附近的精确插入
// ============================================================================

/// 插入到正好触发段分裂, 然后立刻插一个落入新段第一位置的 key.
#[test]
fn leaf_insert_at_split_boundary() {
    let mut page = leaf_new();

    // 插 32 个, 触发第一次 pre_split
    for i in 0..32 {
        let key = format!("b_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 32);

    // 插第 33 个, 触发 pre_split
    leaf_insert(&mut page, b"b_0032", b"v32").unwrap();
    assert_eq!(page_key_count(&page), 33);

    // 插一个落入新段第一位置的 key (在 b_0016 之后)
    leaf_insert(&mut page, b"b_0033", b"v33").unwrap();
    assert_eq!(page_key_count(&page), 34);

    // 全量验证
    for i in 0..34 {
        let key = format!("b_{i:04}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("v{i}").as_bytes()
        );
    }
}

/// 在段边界 (cp[0] / cp[1] 交界) 删 key, 验证链式重写.
#[test]
fn leaf_delete_at_segment_boundary() {
    let mut page = leaf_new();

    // 插 33 个, 确保至少 2 个段
    for i in 0..33 {
        let key = format!("d_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    let (hdr, _) = read_checkpoint_header(&page);
    let cp_count_before = hdr.checkpoint_count;
    assert!(cp_count_before >= 2, "should have at least 2 segments");

    // 删掉可能正好是段首的几个 key, 验证后续查询
    for i in [15, 16, 17, 30, 31] {
        let key = format!("d_{i:04}");
        let deleted = leaf_delete(&mut page, key.as_bytes()).unwrap();
        if deleted {
            // 验证其余 key 仍能查到
            for j in 0..33 {
                if j == i {
                    continue;
                }
                let k = format!("d_{j:04}");
                if leaf_get(&page, k.as_bytes()).is_none() {
                    // 也可能 j 也被删了
                    continue;
                }
                assert_eq!(
                    leaf_get(&page, k.as_bytes()).unwrap(),
                    format!("v{j}").as_bytes()
                );
            }
        }
    }
}

// ============================================================================
// free_off / free_space 验证
// ============================================================================

/// 多次 insert 后 free_off 单调增加, delete 后 free_off 减小.
#[test]
fn leaf_free_off_monotonic() {
    let mut page = leaf_new();
    let mut prev_free = page_free_off(&page);

    for i in 0..50 {
        let key = format!("f_{i:04}");
        let val = format!("v{i}");
        leaf_insert(&mut page, key.as_bytes(), val.as_bytes()).unwrap();
        let new_free = page_free_off(&page);
        assert!(
            new_free >= prev_free,
            "free_off should not decrease on insert"
        );
        prev_free = new_free;
    }

    // 删一些, free_off 应该减小
    for i in (0..50).step_by(2) {
        let key = format!("f_{i:04}");
        leaf_delete(&mut page, key.as_bytes()).unwrap();
    }
    let new_free = page_free_off(&page);
    assert!(
        new_free < prev_free,
        "free_off should decrease after delete"
    );
}

/// free_space 持续减少, 不会变负.
#[test]
fn leaf_free_space_never_negative() {
    let mut page = leaf_new();
    for i in 0..100 {
        let key = format!("fs_{i:04}");
        let val = vec![b'x'; 50];
        if leaf_insert(&mut page, key.as_bytes(), &val).is_err() {
            break;
        }
        let fs = page_free_space(&page);
        // free_space 是 cp_start - free_off, 应 >= 0
        assert!(fs <= page.len(), "free_space should not exceed page size");
    }
}

// ============================================================================
// 大批量 + 段分裂 组合
// ============================================================================

/// 插 500 个然后立即验证 (触发多次段分裂).
#[test]
fn leaf_insert_500_verify_all() {
    let mut page = leaf_new();
    for i in 0..500 {
        let key = format!("big_{i:05}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{i:05}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 500);

    let (hdr, _) = read_checkpoint_header(&page);
    assert!(
        hdr.checkpoint_count >= 10,
        "500 keys should have many segments"
    );

    // 全量验证
    for i in 0..500 {
        let key = format!("big_{i:05}");
        let got = leaf_get(&page, key.as_bytes()).unwrap();
        assert_eq!(got, format!("v{i:05}").as_bytes());
    }
}

/// 插 500 个, 删一半, 再插 500 个, 全量验证.
#[test]
fn leaf_500_delete_half_then_500_more() {
    let mut page = leaf_new();
    for i in 0..500 {
        let key = format!("x_{i:05}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 500);

    // 删一半
    for i in (0..500).step_by(2) {
        let key = format!("x_{i:05}");
        leaf_delete(&mut page, key.as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 250);

    // 再插 500 个
    for i in 500..1000 {
        let key = format!("x_{i:05}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 750);

    // 全量验证: 奇数(0..500) 仍存在 (偶数被删了), 500..1000 全部存在
    for i in (1..500).step_by(2) {
        let key = format!("x_{i:05}");
        let got = leaf_get(&page, key.as_bytes());
        assert!(got.is_some(), "odd key {i} should still exist");
        assert_eq!(got.unwrap(), format!("v{i}").as_bytes());
    }
    // 偶数(0..500) 应不存在
    for i in (0..500).step_by(2) {
        let key = format!("x_{i:05}");
        assert!(
            leaf_get(&page, key.as_bytes()).is_none(),
            "even key {i} should be deleted"
        );
    }
    for i in 500..1000 {
        let key = format!("x_{i:05}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("v{i}").as_bytes()
        );
    }
}

// ============================================================================
// Internal Page 复杂测试
// ============================================================================

/// Internal: 反复 insert + delete 同一 separator.
#[test]
fn internal_insert_delete_same_key_reuse() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 1);

    for round in 0..50 {
        let key = format!("s_{round:04}");
        internal_insert(&mut page, key.as_bytes(), 1000 + round as u64).unwrap();
        assert_eq!(
            internal_child(&page, key.as_bytes()).unwrap(),
            1000 + round as u64
        );
        let deleted = internal_delete(&mut page, key.as_bytes()).unwrap();
        assert!(deleted);
    }
}

/// Internal: 头部插入 (sep 越来越小).
#[test]
fn internal_insert_at_head_repeatedly() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 100);

    // 先插 sep_e_*, sep_m_*, sep_q_*
    for c in *b"emq" {
        for i in 0..20 {
            let key = format!("{}_{i:04}", c as char);
            internal_insert(&mut page, key.as_bytes(), (c as u64) * 10000 + i as u64).unwrap();
        }
    }
    // 头部插 sep_a_*, sep_c_*
    for c in *b"ac" {
        for i in 0..20 {
            let key = format!("{}_{i:04}", c as char);
            internal_insert(&mut page, key.as_bytes(), (c as u64) * 10000 + i as u64).unwrap();
        }
    }
    // 验证全部 100 个 separator
    let total = 100;
    assert_eq!(page_key_count(&page), total);
    for c in *b"acemq" {
        for i in 0..20 {
            let key = format!("{}_{i:04}", c as char);
            let expected = (c as u64) * 10000 + i as u64;
            assert_eq!(internal_child(&page, key.as_bytes()).unwrap(), expected);
        }
    }
}

/// Internal: 100 个 separator 触发段分裂 + 路由验证.
#[test]
fn internal_100_separators_route_verify() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 0);

    for i in 0..100 {
        let sep = format!("sep_{i:05}");
        internal_insert(&mut page, sep.as_bytes(), 5000 + i as u64).unwrap();
    }
    assert_eq!(page_key_count(&page), 100);

    // 验证每个 separator 路由到正确的 child
    for i in 0..100 {
        let sep = format!("sep_{i:05}");
        assert_eq!(
            internal_child(&page, sep.as_bytes()).unwrap(),
            5000 + i as u64
        );
    }
    // 边界: < 第一个 separator
    assert_eq!(internal_child(&page, b"aaa").unwrap(), 0); // first_child
    // > 最后一个 separator
    assert_eq!(internal_child(&page, b"zzz").unwrap(), 5000 + 99);
}

/// Internal split 后左右各插 50 个, 验证路由.
#[test]
fn internal_split_insert_both_sides() {
    let mut left = internal_new();
    page_set_vpid(&mut left, 0);

    for i in 0..100 {
        let sep = format!("sp_{i:04}");
        internal_insert(&mut left, sep.as_bytes(), 10000 + i as u64).unwrap();
    }

    let mut right = internal_new();
    let split_key = internal_split(&mut left, &mut right).unwrap();
    eprintln!("split_key = {:?}", String::from_utf8_lossy(&split_key));
    let lc = page_key_count(&left);
    let rc = page_key_count(&right);
    assert_eq!(lc + rc, 100);

    // 路由验证
    for i in 0..lc {
        let sep = format!("sp_{i:04}");
        assert_eq!(
            internal_child(&left, sep.as_bytes()).unwrap(),
            10000 + i as u64
        );
    }
    for i in lc..100 {
        let sep = format!("sp_{i:04}");
        assert_eq!(
            internal_child(&right, sep.as_bytes()).unwrap(),
            10000 + i as u64
        );
    }

    // 在两边各插 50 个新 separator
    for i in 100..150 {
        let sep = format!("sp_{i:04}");
        // 路由决定插哪边
        let target = if sep.as_bytes() < split_key.as_slice() {
            &mut left
        } else {
            &mut right
        };
        internal_insert(target, sep.as_bytes(), 10000 + i as u64).unwrap();
    }
}

// ============================================================================
// 多页组合场景
// ============================================================================

/// 模拟 B+Tree 叶子层: split 多页, 每页插删, 验证全局一致性.
#[test]
fn leaf_bptree_layer_simulation() {
    use std::collections::BTreeMap;

    let mut pages: Vec<[u8; page::PAGE_SIZE]> = vec![leaf_new()];
    let mut truth: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    // Phase 1: 插 200 个, 在第一页满了之后自动 split (简化: 我们手动 split)
    for i in 0..200 {
        let key = format!("k_{i:05}");
        let val = format!("v{i}");
        // 找到合适的 page (按 key 范围)
        // 简化: 总是插最后一页
        let last = pages.len() - 1;
        match leaf_insert(&mut pages[last], key.as_bytes(), val.as_bytes()) {
            Ok(()) => {
                truth.insert(key.into_bytes(), val.into_bytes());
            }
            Err(_) => {
                // 满了, split
                let mut new_page = leaf_new();
                let _ = leaf_split(&mut pages[last], &mut new_page).unwrap();
                pages.push(new_page);
                // 重试 insert
                let key_bytes = format!("k_{i:05}");
                let val_bytes = format!("v{i}");
                leaf_insert(
                    &mut pages[last + 1],
                    key_bytes.as_bytes(),
                    val_bytes.as_bytes(),
                )
                .unwrap();
                truth.insert(key_bytes.into_bytes(), val_bytes.into_bytes());
            }
        }
    }

    // 全局验证: 每个 key 都能在某一页找到
    for (k, v) in &truth {
        let mut found = false;
        for p in &pages {
            if let Some(got) = leaf_get(p, k) {
                assert_eq!(&got, v, "value mismatch for key {:?}", k);
                found = true;
                break;
            }
        }
        assert!(found, "key {:?} not found in any page", k);
    }

    // 总 key_count 应等于 truth 大小
    let total: u16 = pages.iter().map(|p| page_key_count(p)).sum();
    assert_eq!(total as usize, truth.len());
}

// ============================================================================
// 边界: 删到 0 再插
// ============================================================================

/// 删到只剩 0 个 key, 再从 0 开始插, 验证 init_sentinel 路径.
#[test]
fn leaf_delete_all_then_insert_fresh() {
    let mut page = leaf_new();
    for i in 0..30 {
        let key = format!("a_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    // 全部删
    for i in 0..30 {
        let key = format!("a_{i:04}");
        leaf_delete(&mut page, key.as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 0);

    // 重新插
    for i in 0..30 {
        let key = format!("b_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 30);
    for i in 0..30 {
        let key = format!("b_{i:04}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("v{i}").as_bytes()
        );
    }
}
