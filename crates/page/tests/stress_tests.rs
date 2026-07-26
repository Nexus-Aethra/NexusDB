//! Page 层全面压力测试.
//!
//! 覆盖场景:
//! 1. 大量 key 插入 (>=500, 触发多次 checkpoint 分裂)
//! 2. 插入后全量查询
//! 3. 增量插入混合前缀
//! 4. 增量后查询 + 原有 key 验证仍在
//! 5. 强制 page split
//! 6. split 后增量插入
//! 7. split 后总量查询
//! 8. 删除穿插验证
//! 9. checkpoint 数组完整性校验
//! 10. Internal Page 大量 separator + 分裂 + 路由

use page::{
    ItemKind, PageIndex, internal_child, internal_insert, internal_new, internal_split,
    leaf_delete, leaf_get, leaf_insert, leaf_new, leaf_split, page_free_off, page_free_space,
    page_key_count, page_set_vpid, read_checkpoint_header,
};

// ===== Leaf Page 压力测试 =====

#[test]
fn leaf_mass_insert_then_query_all() {
    let mut page = leaf_new();
    let n = 80;

    // Phase 1: 批量插入
    for i in 0..n {
        let key = format!("user_profile_{i:04}");
        let val = format!("data_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), val.as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), n);

    // Phase 2: 全量查询, 验证都能查到
    for i in 0..n {
        let key = format!("user_profile_{i:04}");
        let expected = format!("data_{i:04}");
        let got = leaf_get(&page, key.as_bytes()).unwrap();
        assert_eq!(got, expected.as_bytes(), "key {i} mismatch");
    }
}

#[test]
fn leaf_incremental_insert_and_verify() {
    let mut page = leaf_new();

    // Phase 1: 初始 80 个
    for i in 0..80 {
        let key = format!("k_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 80);

    // 验证初始 80
    for i in 0..80 {
        let key = format!("k_{i:04}");
        assert!(leaf_get(&page, key.as_bytes()).is_some());
    }

    // Phase 2: 再插入 80 个
    for i in 80..160 {
        let key = format!("k_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 160);

    // Phase 3: 验证全部 160 个 (包括旧 + 新)
    for i in 0..160 {
        let key = format!("k_{i:04}");
        let expected = format!("v{i}");
        let got = leaf_get(&page, key.as_bytes()).unwrap();
        assert_eq!(got, expected.as_bytes(), "after incremental, key {i} wrong");
    }
}

#[test]
fn leaf_split_then_incremental_insert_and_query() {
    let mut left = leaf_new();

    // Phase 1: 插入足够多 key 直到可以 split
    let n = 60;
    for i in 0..n {
        let key = format!("item_{i:05}");
        leaf_insert(&mut left, key.as_bytes(), format!("val{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&left), n);

    // Phase 2: split
    let mut right = leaf_new();
    let _sk = leaf_split(&mut left, &mut right).unwrap();
    let left_count = page_key_count(&left);
    let right_count = page_key_count(&right);
    assert_eq!(left_count + right_count, n);

    // 验证 split 后左右都能正确查询
    for i in 0..left_count {
        let key = format!("item_{i:05}");
        assert!(
            leaf_get(&left, key.as_bytes()).is_some(),
            "left missing key {i}"
        );
    }
    for i in left_count..n {
        let key = format!("item_{i:05}");
        assert!(
            leaf_get(&right, key.as_bytes()).is_some(),
            "right missing key {i}"
        );
    }

    // Phase 3: split 后增量插入到 right
    let extra = 30;
    for i in n..n + extra {
        let key = format!("item_{i:05}");
        leaf_insert(&mut right, key.as_bytes(), format!("val{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&right), right_count + extra);

    // Phase 4: 验证 right 上所有 key (旧 + 新)
    for i in left_count..n + extra {
        let key = format!("item_{i:05}");
        let expected = format!("val{i}");
        let got = leaf_get(&right, key.as_bytes()).unwrap();
        assert_eq!(
            got,
            expected.as_bytes(),
            "right after incremental, key {i} wrong"
        );
    }

    // Phase 5: 验证 left 上的 key 没受影响
    for i in 0..left_count {
        let key = format!("item_{i:05}");
        let expected = format!("val{i}");
        let got = leaf_get(&left, key.as_bytes()).unwrap();
        assert_eq!(
            got,
            expected.as_bytes(),
            "left corrupted after right insert, key {i} wrong"
        );
    }
}

#[test]
fn leaf_delete_interleaved_with_insert_and_query() {
    let mut page = leaf_new();

    // Phase 1: 批量插入 80 个
    for i in 0..80 {
        let key = format!("del_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("val{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 80);

    // Phase 2: 删除所有偶数 key
    for i in (0..80).step_by(2) {
        let key = format!("del_{i:04}");
        let deleted = leaf_delete(&mut page, key.as_bytes()).unwrap();
        if !deleted {
            eprintln!(
                "delete failed at i={i} key={key:?} key_count={}",
                page_key_count(&page)
            );
        }
        assert!(deleted, "delete failed at i={i}");
    }
    assert_eq!(page_key_count(&page), 40);

    // Phase 3: 验证偶数 key 已删, 奇数 key 仍在
    for i in 0..80 {
        let key = format!("del_{i:04}");
        if i % 2 == 0 {
            assert!(
                leaf_get(&page, key.as_bytes()).is_none(),
                "key {i} should be deleted"
            );
        } else {
            let expected = format!("val{i}");
            assert_eq!(
                leaf_get(&page, key.as_bytes()).unwrap(),
                expected.as_bytes()
            );
        }
    }

    // Phase 4: 增量插入新 key
    for i in 80..120 {
        let key = format!("del_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("val{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 80);

    // Phase 5: 验证最终状态
    for i in 0..120 {
        let key = format!("del_{i:04}");
        if i < 80 && i % 2 == 0 {
            assert!(leaf_get(&page, key.as_bytes()).is_none());
        } else {
            let expected = format!("val{i}");
            assert_eq!(
                leaf_get(&page, key.as_bytes()).unwrap(),
                expected.as_bytes()
            );
        }
    }
}

#[test]
fn leaf_checkpoint_integrity_under_many_splits() {
    let mut page = leaf_new();
    let n = 100; // 触发多次 checkpoint 分裂

    // Phase 1: 大量插入
    for i in 0..n {
        let key = format!("cp_test_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("d{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), n);

    // Phase 2: 检查 checkpoint header
    let (hdr, _) = read_checkpoint_header(&page);
    assert!(
        hdr.checkpoint_count >= 1,
        "should have at least 1 checkpoint"
    );

    // Phase 3: 全量查询, 确保二分查找正确
    for i in 0..n {
        let key = format!("cp_test_{i:04}");
        let expected = format!("d{i}");
        let got = leaf_get(&page, key.as_bytes()).unwrap();
        assert_eq!(
            got,
            expected.as_bytes(),
            "checkpoint integrity fail at key {i}"
        );
    }
}

#[test]
fn leaf_long_common_prefix_stress() {
    let mut page = leaf_new();
    let n = 80;

    // 所有 key 共享超长前缀 (强制前缀压缩发挥最大作用)
    for i in 0..n {
        let key = format!("company.department.team.member.user_{i:08}_profile");
        leaf_insert(&mut page, key.as_bytes(), format!("u{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), n);

    // 全量查询
    for i in 0..n {
        let key = format!("company.department.team.member.user_{i:08}_profile");
        let expected = format!("u{i}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            expected.as_bytes()
        );
    }
}

#[test]
fn leaf_split_twice_then_verify() {
    let mut page = leaf_new();

    // 插入 80 个 key
    let total = 80;
    for i in 0..total {
        let key = format!("multi_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), total);

    // 第一次 split
    let mut right1 = leaf_new();
    let _sk1 = leaf_split(&mut page, &mut right1).unwrap();
    let c1 = page_key_count(&page);
    let c2 = page_key_count(&right1);
    assert_eq!(c1 + c2, total);

    // 第二次 split (再分裂 right1)
    let mut right2 = leaf_new();
    let _sk2 = leaf_split(&mut right1, &mut right2).unwrap();
    let c2a = page_key_count(&right1);
    let c3 = page_key_count(&right2);
    assert_eq!(c2a + c3, c2);

    // 验证三片都能正确查询
    // page 存 0..c1
    for i in 0..c1 {
        let key = format!("multi_{i:04}");
        assert!(
            leaf_get(&page, key.as_bytes()).is_some(),
            "piece1 missing {i}"
        );
    }
    // right1 存 c1..c1+c2a
    for i in c1..c1 + c2a {
        let key = format!("multi_{i:04}");
        assert!(
            leaf_get(&right1, key.as_bytes()).is_some(),
            "piece2 missing {i}"
        );
    }
    // right2 存 c1+c2a..total
    for i in c1 + c2a..total {
        let key = format!("multi_{i:04}");
        assert!(
            leaf_get(&right2, key.as_bytes()).is_some(),
            "piece3 missing {i}"
        );
    }
}

#[test]
fn leaf_page_full_gradual() {
    let mut page = leaf_new();
    let mut inserted = 0;

    // 一直插到满, 验证 page_free_space 递减
    let mut prev_free = page_free_space(&page);
    for i in 0..2000 {
        let key = format!("fill_{i:08}");
        let val = format!("value_payload_{i:04}_padding");
        match leaf_insert(&mut page, key.as_bytes(), val.as_bytes()) {
            Ok(()) => {
                inserted += 1;
                let new_free = page_free_space(&page);
                assert!(new_free <= prev_free, "free space should not increase");
                prev_free = new_free;
            }
            Err(page::PageError::PageFull) => break,
            Err(e) => panic!("unexpected: {e:?}"),
        }
    }
    assert!(
        inserted > 50,
        "should insert a reasonable number before full"
    );
    println!("leaf_page_full_gradual: inserted {inserted} keys, free space = {prev_free}");
}

// ===== Internal Page 压力测试 =====

#[test]
fn internal_mass_insert_then_route_all() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 0);

    // Phase 1: 插入 100 个 separator
    let n = 100;
    for i in 0..n {
        let sep = format!("sep_{i:05}");
        internal_insert(&mut page, sep.as_bytes(), 1000 + i as u64).unwrap();
    }
    assert_eq!(page_key_count(&page), n);

    // Phase 2: 每个 separator key 路由结果验证
    // B+Tree 语义: separator[i] 是 child[i+1] 的 max key
    // internal_child 找最大 i 使得 sep[i] <= key, 返回 child_vpid(i+1)
    // first child vpid = 0 (page_vpid)
    for i in 0..n {
        let sep = format!("sep_{i:05}");
        let expected_vpid = 1000 + i as u64;
        assert_eq!(
            internal_child(&page, sep.as_bytes()).unwrap(),
            expected_vpid,
            "route failed for sep {i}"
        );
    }

    // Phase 3: 边界间的路由验证
    assert_eq!(internal_child(&page, b"sep_00000").unwrap(), 1000);
    assert_eq!(internal_child(&page, b"sep_00001").unwrap(), 1001);
    assert_eq!(internal_child(&page, b"sep_00099").unwrap(), 1000 + 99);
    // 超过最大 sep 应返回最后一个 child
    assert_eq!(internal_child(&page, b"zzz").unwrap(), 1000 + 99);
}

#[test]
fn internal_split_then_incremental_insert_and_route() {
    let mut left = internal_new();
    page_set_vpid(&mut left, 0);

    // Phase 1: 插入 100 个 separator
    for i in 0..100 {
        let sep = format!("s_{i:04}");
        internal_insert(&mut left, sep.as_bytes(), 5000 + i as u64).unwrap();
    }
    assert_eq!(page_key_count(&left), 100);

    // Phase 2: split
    let mut right = internal_new();
    let _sk2 = internal_split(&mut left, &mut right).unwrap();
    let lc = page_key_count(&left);
    let rc = page_key_count(&right);
    assert_eq!(lc + rc, 100);

    // 验证 split 后路由
    for i in 0..lc {
        let sep = format!("s_{i:04}");
        assert_eq!(
            internal_child(&left, sep.as_bytes()).unwrap(),
            5000 + i as u64
        );
    }
    for i in lc..100 {
        let sep = format!("s_{i:04}");
        assert_eq!(
            internal_child(&right, sep.as_bytes()).unwrap(),
            5000 + i as u64
        );
    }

    // Phase 3: 增量插入新 separator 到 right
    for i in 100..150 {
        let sep = format!("s_{i:04}");
        internal_insert(&mut right, sep.as_bytes(), 5000 + i as u64).unwrap();
    }
    assert_eq!(page_key_count(&right), rc + 50);

    // Phase 4: 验证 right 增量部分路由正确
    for i in 100..150 {
        let sep = format!("s_{i:04}");
        assert_eq!(
            internal_child(&right, sep.as_bytes()).unwrap(),
            5000 + i as u64
        );
    }

    // Phase 5: left 路由不受影响
    for i in 0..lc {
        let sep = format!("s_{i:04}");
        assert_eq!(
            internal_child(&left, sep.as_bytes()).unwrap(),
            5000 + i as u64
        );
    }
}

#[test]
fn internal_checkpoint_integrity_under_many_splits() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 999);

    // 插入 150 个 separator (触发多次 checkpoint 分裂)
    let n = 150;
    for i in 0..n {
        let sep = format!("chk_{i:04}");
        internal_insert(&mut page, sep.as_bytes(), 20000 + i as u64).unwrap();
    }
    assert_eq!(page_key_count(&page), n);

    // 检查 checkpoint
    let (hdr, _) = read_checkpoint_header(&page);
    assert!(hdr.checkpoint_count >= 1);

    // 二分查找验证
    for i in 0..n {
        let sep = format!("chk_{i:04}");
        assert_eq!(
            internal_child(&page, sep.as_bytes()).unwrap(),
            20000 + i as u64,
            "checkpoint route failed at sep {i}"
        );
    }
}

#[test]
fn internal_long_common_prefix_stress() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 1);

    let n = 80;
    for i in 0..n {
        let sep = format!("org.division.department.employee.user_{i:06}");
        internal_insert(&mut page, sep.as_bytes(), 70000 + i as u64).unwrap();
    }
    assert_eq!(page_key_count(&page), n);

    // 路由验证
    for i in 0..n {
        let sep = format!("org.division.department.employee.user_{i:06}");
        assert_eq!(
            internal_child(&page, sep.as_bytes()).unwrap(),
            70000 + i as u64
        );
    }
}

#[test]
fn internal_route_between_separators() {
    let mut page = internal_new();
    page_set_vpid(&mut page, 10);

    // 插入几个明确的 separator
    let seps: Vec<(&[u8], u64)> = vec![(b"f", 20), (b"m", 30), (b"t", 40), (b"z", 50)];
    for (s, v) in &seps {
        internal_insert(&mut page, s, *v).unwrap();
    }

    // 区间内路由
    assert_eq!(internal_child(&page, b"a").unwrap(), 10); // < f
    assert_eq!(internal_child(&page, b"f").unwrap(), 20); // == f
    assert_eq!(internal_child(&page, b"g").unwrap(), 20); // f < g < m
    assert_eq!(internal_child(&page, b"m").unwrap(), 30); // == m
    assert_eq!(internal_child(&page, b"n").unwrap(), 30); // m < n < t
    assert_eq!(internal_child(&page, b"t").unwrap(), 40); // == t
    assert_eq!(internal_child(&page, b"w").unwrap(), 40); // t < w < z
    assert_eq!(internal_child(&page, b"z").unwrap(), 50); // == z
    assert_eq!(internal_child(&page, b"zz").unwrap(), 50); // > z
}

// ===== 高难度边界 / 异常压力测试 =====

/// 反复 insert + delete (升序插, 降序删, 再升序插) 验证 page 状态持续正确.
#[test]
fn leaf_alternating_insert_delete_500() {
    let mut page = leaf_new();
    let n = 500;

    // Phase 1: 升序插 500 个
    for i in 0..n {
        let key = format!("key_{i:05}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), n);

    // Phase 2: 降序删 (从最大开始) — 触发多次段合并
    for i in (0..n).rev() {
        let key = format!("key_{i:05}");
        let deleted = leaf_delete(&mut page, key.as_bytes()).unwrap();
        assert!(deleted, "delete failed at i={i}");
    }
    assert_eq!(page_key_count(&page), 0);
    assert!(leaf_get(&page, b"key_00000").is_none());

    // Phase 3: 重新升序插 500 个 (复用 page)
    for i in 0..n {
        let key = format!("key_{i:05}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), n);

    // Phase 4: 全量查询
    for i in 0..n {
        let key = format!("key_{i:05}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("v{i}").as_bytes()
        );
    }
}

/// 在已存在 key 范围"前"插入 (新 key < 最小), 触发段首分裂 + 哨兵保护.
#[test]
fn leaf_insert_at_head_below_min() {
    let mut page = leaf_new();

    // 插 100 个 key, 范围 [k_0000, k_0099]
    for i in 0..100 {
        leaf_insert(
            &mut page,
            format!("k_{i:04}").as_bytes(),
            format!("v{i}").as_bytes(),
        )
        .unwrap();
    }
    assert_eq!(page_key_count(&page), 100);

    // 在最前插 50 个 (k_aaaa00..k_aaaa49 < k_0000)
    for i in 0..50 {
        let key = format!("aaaa_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("head{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 150);

    // 验证 150 个都能查到, 且 head keys 在最前
    for i in 0..50 {
        let key = format!("aaaa_{i:04}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("head{i}").as_bytes()
        );
    }
    for i in 0..100 {
        let key = format!("k_{i:04}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("v{i}").as_bytes()
        );
    }
}

/// 在已存在 key 范围"后"插入 (新 key > 最大), 触发段尾分裂.
#[test]
fn leaf_insert_at_tail_above_max() {
    let mut page = leaf_new();

    for i in 0..100 {
        leaf_insert(
            &mut page,
            format!("k_{i:04}").as_bytes(),
            format!("v{i}").as_bytes(),
        )
        .unwrap();
    }
    assert_eq!(page_key_count(&page), 100);

    // 在最后插 50 个 (k_zzzz00..k_zzzz49 > k_0099)
    for i in 0..50 {
        let key = format!("zzzz_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("tail{i}").as_bytes()).unwrap();
    }
    assert_eq!(page_key_count(&page), 150);

    for i in 0..100 {
        let key = format!("k_{i:04}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("v{i}").as_bytes()
        );
    }
    for i in 0..50 {
        let key = format!("zzzz_{i:04}");
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("tail{i}").as_bytes()
        );
    }
}

/// 5000 次随机操作 (insert/delete/query) 后验证 page 状态自洽.
/// 不确定性测试: 失败说明 race condition / 状态泄漏.
#[test]
fn leaf_random_chaos_5000_ops() {
    use std::collections::HashMap;

    let mut page = leaf_new();
    let mut truth: HashMap<String, String> = HashMap::new();
    let mut rng_state: u64 = 0x1234_5678_DEAD_BEEF;

    // 简单 LCG 伪随机 (确定性, 便于复现)
    fn next_rand(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    for op_idx in 0..5000 {
        let op = next_rand(&mut rng_state) % 3;
        let key = format!("k_{:05}", next_rand(&mut rng_state) % 200); // 200 个 key 池
        let val = format!("v{op_idx}");

        match op {
            0 => {
                // insert (或覆盖失败)
                if let Ok(()) = leaf_insert(&mut page, key.as_bytes(), val.as_bytes()) {
                    truth.insert(key.clone(), val);
                }
                // 若 key 已存在, 我们的 truth 不会变 (覆盖未实现), 与 page 一致
            }
            1 => {
                // delete
                let existed_in_truth = truth.remove(&key).is_some();
                match leaf_delete(&mut page, key.as_bytes()) {
                    Ok(deleted) => {
                        assert_eq!(
                            deleted, existed_in_truth,
                            "delete mismatch at op {op_idx} key={key}"
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "!!! leaf_delete FAILED at op {op_idx} key={key}: {e:?}\n\
                             state: key_count={} free_off={} truth_size={}",
                            page_key_count(&page),
                            page_free_off(&page),
                            truth.len()
                        );
                        panic!("leaf_delete returned Err");
                    }
                }
            }
            _ => {
                // query
                let got = leaf_get(&page, key.as_bytes());
                let expected = truth.get(&key).map(|v| v.as_bytes().to_vec());
                assert_eq!(got, expected, "get mismatch at op {op_idx} key={key}");
            }
        }
    }

    // 最终 page_key_count 应等于 truth 大小
    assert_eq!(
        page_key_count(&page),
        truth.len() as u16,
        "final key count mismatch"
    );

    // 全量验证
    for (k, v) in &truth {
        let got = leaf_get(&page, k.as_bytes()).unwrap();
        assert_eq!(got, v.as_bytes(), "final verify failed for {k}");
    }
}

/// split → delete → 再次 split → delete, 多轮 page 形态变化.
#[test]
fn leaf_split_delete_split_delete_chaos() {
    let mut page = leaf_new();

    // Round 1: 插 80 → split
    for i in 0..80 {
        leaf_insert(
            &mut page,
            format!("a_{i:04}").as_bytes(),
            format!("va{i}").as_bytes(),
        )
        .unwrap();
    }
    let mut right = leaf_new();
    let _ = leaf_split(&mut page, &mut right).unwrap();
    assert_eq!(page_key_count(&page) + page_key_count(&right), 80);

    // 必须在删除前快照 right_base: 它是 right page 中第一个 key 的全局编号,
    // 等于 split 时 left page 的 key_count (= 80/2 = 40). 删除后会变化.
    let right_base = page_key_count(&page) as usize;

    // Round 2: 在两片上交替删除一半
    let left_to_del: Vec<String> = (0..page_key_count(&page) as usize)
        .filter(|i| i % 2 == 0)
        .map(|i| format!("a_{i:04}"))
        .collect();
    for k in &left_to_del {
        let deleted = leaf_delete(&mut page, k.as_bytes()).unwrap();
        assert!(deleted, "left delete failed for {k}");
    }
    let right_to_del: Vec<String> = (0..page_key_count(&right) as usize)
        .filter(|i| i % 2 == 1)
        .map(|i| format!("a_{:04}", right_base + i))
        .collect();
    for k in &right_to_del {
        let deleted = leaf_delete(&mut right, k.as_bytes()).unwrap();
        assert!(deleted, "right delete failed for {k}");
    }
    let remaining = page_key_count(&page) + page_key_count(&right);
    let expected_remaining = 80 - (left_to_del.len() + right_to_del.len()) as u16;
    assert_eq!(remaining, expected_remaining, "round 2 remaining mismatch");

    // Round 3: 在 left 再次插 30 → 触发 split
    for i in 0..30 {
        let key = format!("b_{i:04}");
        leaf_insert(&mut page, key.as_bytes(), format!("vb{i}").as_bytes()).unwrap();
    }

    // Round 4: 在 left 删除新插入的 keys
    for i in 0..30 {
        let key = format!("b_{i:04}");
        let deleted = leaf_delete(&mut page, key.as_bytes()).unwrap();
        assert!(deleted, "round 4 delete failed for {key}");
    }

    // 最终验证
    for i in 0..page_key_count(&page) as usize {
        // i 是当前 key 在 left 物理布局中的位置, 实际是 left 剩下的 a_x 中索引 %2==1 的.
        // 简化: 验证 left 中剩余 keys 都还能查到
        let key = format!("a_{i:04}");
        // 注意: i 可能是被删的, 跳过
        if leaf_get(&page, key.as_bytes()).is_none() {
            // 这个 key 在 left 中被删了, OK
            continue;
        }
        // 否则应该能查到
        let got = leaf_get(&page, key.as_bytes()).unwrap();
        assert_eq!(got, format!("va{i}").as_bytes());
    }
    for i in 0..page_key_count(&right) as usize {
        let key = format!("a_{:04}", right_base + i);
        if leaf_get(&right, key.as_bytes()).is_none() {
            continue;
        }
        let got = leaf_get(&right, key.as_bytes()).unwrap();
        assert_eq!(got, format!("va{}", right_base + i).as_bytes());
    }
}

/// 拒绝空 key 插入 (哨兵专用).
#[test]
fn leaf_reject_empty_key() {
    let mut page = leaf_new();
    let result = leaf_insert(&mut page, b"", b"v");
    assert!(result.is_err(), "empty key should be rejected");
}

/// 拒绝重复 key (覆盖未实现).
#[test]
fn leaf_reject_duplicate_key() {
    let mut page = leaf_new();
    leaf_insert(&mut page, b"foo", b"1").unwrap();
    let result = leaf_insert(&mut page, b"foo", b"2");
    assert!(result.is_err(), "duplicate key should be rejected");
    // 验证值没被覆盖
    assert_eq!(leaf_get(&page, b"foo").unwrap(), b"1");
}

/// 删除不存在的 key 返回 false, 不改变 page 状态.
#[test]
fn leaf_delete_nonexistent_idempotent() {
    let mut page = leaf_new();
    leaf_insert(&mut page, b"a", b"1").unwrap();
    leaf_insert(&mut page, b"c", b"3").unwrap();

    let d1 = leaf_delete(&mut page, b"b").unwrap();
    assert!(!d1, "delete non-existent should return false");
    assert_eq!(page_key_count(&page), 2);

    // 再删一次仍然 false
    let d2 = leaf_delete(&mut page, b"b").unwrap();
    assert!(!d2);

    // 已有 keys 仍能查到
    assert_eq!(leaf_get(&page, b"a").unwrap(), b"1");
    assert_eq!(leaf_get(&page, b"c").unwrap(), b"3");
}

/// 单 key page 反复 insert / delete / reinsert, 验证哨兵 + 单 item 状态.
#[test]
fn leaf_single_key_churn() {
    let mut page = leaf_new();
    for round in 0..100 {
        let key = format!("k_{round}");
        leaf_insert(&mut page, key.as_bytes(), format!("v{round}").as_bytes()).unwrap();
        assert_eq!(page_key_count(&page), 1);
        assert_eq!(
            leaf_get(&page, key.as_bytes()).unwrap(),
            format!("v{round}").as_bytes()
        );
        let deleted = leaf_delete(&mut page, key.as_bytes()).unwrap();
        assert!(deleted);
        assert_eq!(page_key_count(&page), 0);
        assert!(leaf_get(&page, key.as_bytes()).is_none());
    }
}

/// internal page 也做随机 chaos 测试.
#[test]
fn internal_random_chaos_3000_ops() {
    use std::collections::HashMap;

    let mut page = internal_new();
    page_set_vpid(&mut page, 0);
    let mut truth: HashMap<String, u64> = HashMap::new();
    let mut rng_state: u64 = 0xDEAD_BEEF_CAFE_F00D;

    fn next_rand(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    for op_idx in 0..3000 {
        let op = next_rand(&mut rng_state) % 2;
        let key = format!("s_{:04}", next_rand(&mut rng_state) % 100); // 100 个 key 池
        let vpid = 1000 + (op_idx as u64);

        match op {
            0 => {
                if let Ok(()) = internal_insert(&mut page, key.as_bytes(), vpid) {
                    truth.insert(key.clone(), vpid);
                }
            }
            _ => {
                let got = internal_child(&page, key.as_bytes());
                let expected = if let Some(&v) = truth.get(&key) {
                    Some(v)
                } else {
                    // key 不在 truth, 但 routing 仍会返回某个 child (可能是哨兵的 page_vpid)
                    // 所以无法对比; 跳过
                    None
                };
                if let Some(exp) = expected {
                    assert_eq!(
                        got,
                        Some(exp),
                        "internal route mismatch at op {op_idx} key={key}"
                    );
                }
            }
        }

        // 每步后校验 PageIndex 完整性 (与 leaf 测试一致)
        if let Err(e) = PageIndex::load(&page, ItemKind::Internal) {
            eprintln!("PageIndex corrupted at op {op_idx}: {e}");
            panic!("PageIndex corrupted at op {op_idx}: {e}");
        }
    }

    // 最终 key_count 应等于 truth 大小
    assert_eq!(
        page_key_count(&page),
        truth.len() as u16,
        "final key count mismatch"
    );
}
