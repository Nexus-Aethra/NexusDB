//! 真实 key 分布下的前缀压缩链复现测试.
//!
//! 背景: `shard_manager` stress verify 报告 1~3/600 keys missing, 每次不同.
//! 已定位到 `put`/`get` 路由到同一 leaf_vpid, `leaf_insert` Ok 但 `leaf_get` None,
//! 收敛到 page crate 的前缀压缩 (prefix-compress) 链在真实 key 分布下被破坏.
//!
//! 现有 `leaf_random_chaos_5000_ops` 用统一前缀 `k_` + 200 key 小池, 覆盖不到
//! 真实 stress 的三类差异前缀:
//!   - warmup: `warmup_t{tid}_{i:06}`
//!   - mixed:  `t{tid}_{i:08}`
//!   - verify: `v{tid}_{i:06}`
//!
//! 本测试在 page 层用一个"有序 leaf 目录"模型 (镜像 storage `btree_insert` 的
//! split 语义), 用固定 seed 的伪随机打乱插入顺序, 每次插入后验证**所有已插入
//! key** 仍可 `leaf_get` 读到. 一旦失败, 打印最小复现信息.

use page::{PAGE_SIZE, leaf_get, leaf_insert, leaf_new, leaf_split, page_key_count};

/// 确定性 xorshift PRNG (与现有 chaos 测试一致).
fn next_rand(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// 生成一个真实分布的 key. 三类前缀混合, tid 0..6, index 有一定范围.
fn gen_key(state: &mut u64) -> String {
    let kind = next_rand(state) % 3;
    let tid = next_rand(state) % 6;
    match kind {
        0 => {
            let i = next_rand(state) % 200;
            format!("warmup_t{tid}_{i:06}")
        }
        1 => {
            let i = next_rand(state) % 10000;
            format!("t{tid}_{i:08}")
        }
        _ => {
            let i = next_rand(state) % 200;
            format!("v{tid}_{i:06}")
        }
    }
}

/// 有序 leaf 目录: 镜像 storage `btree_insert` 的 leaf-level split 语义.
///
/// - `leaves[i]` 覆盖 key 范围 `[split_keys[i-1], split_keys[i])` (split_keys[i]=下一片首 key).
/// - 用 `split_keys` (leaves[1..] 各自的首 key) 做路由: 找最后一个 split_key <= key 的片.
struct LeafDir {
    leaves: Vec<Box<[u8; PAGE_SIZE]>>,
    /// split_keys[i] 是 leaves[i+1] 的第一个 key. len == leaves.len() - 1.
    split_keys: Vec<Vec<u8>>,
}

impl LeafDir {
    fn new() -> Self {
        Self {
            leaves: vec![Box::new(leaf_new())],
            split_keys: Vec::new(),
        }
    }

    /// 路由: 返回 key 应落入的 leaf idx. 找最后一个 split_key <= key 的片.
    fn route(&self, key: &[u8]) -> usize {
        let mut idx = 0usize;
        for (i, sk) in self.split_keys.iter().enumerate() {
            if sk.as_slice() <= key {
                idx = i + 1;
            } else {
                break;
            }
        }
        idx
    }

    /// 插入 (key, value). PageFull 时 split 并把 key 放入正确的半片.
    /// 镜像 `btree_insert` (修复后): split_key = right 首 key,
    /// key > split_key 进 right, key <= split_key 进 left.
    fn insert(&mut self, key: &[u8], value: &[u8]) {
        let idx = self.route(key);
        match leaf_insert(&mut self.leaves[idx][..], key, value) {
            Ok(()) => {}
            Err(page::PageError::PageFull) => {
                let mut right = Box::new(leaf_new());
                let split_key = leaf_split(&mut self.leaves[idx], &mut right).unwrap();
                // 条件路由: key > split_key 进 right, 否则进 left
                if key > split_key.as_slice() {
                    leaf_insert(&mut right[..], key, value).unwrap();
                } else {
                    leaf_insert(&mut self.leaves[idx][..], key, value).unwrap();
                }
                // 插入新片: leaves[idx+1] = right, split_keys[idx] = split_key
                self.leaves.insert(idx + 1, right);
                self.split_keys.insert(idx, split_key);
            }
            Err(e) => panic!("leaf_insert 意外错误: {e:?}"),
        }
    }

    /// 查询 key.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let idx = self.route(key);
        leaf_get(&self.leaves[idx][..], key)
    }

    fn total_keys(&self) -> usize {
        self.leaves.iter().map(|p| page_key_count(&p[..]) as usize).sum()
    }
}

/// 单个 seed 跑一轮: 随机插入真实分布 key, 每步验证全部已插入 key 可读.
fn run_one_seed(seed: u64, ops: usize) {
    use std::collections::HashMap;

    let mut dir = LeafDir::new();
    let mut truth: HashMap<String, String> = HashMap::new();
    let mut state = seed;

    for op_idx in 0..ops {
        let key = gen_key(&mut state);
        let val = format!("val_{op_idx}");

        // key 已存在则跳过 (覆盖未实现, 与 truth 保持一致).
        if truth.contains_key(&key) {
            // 已存在的 key 必须仍可读.
            let got = dir.get(key.as_bytes());
            assert_eq!(
                got.as_deref(),
                truth.get(&key).map(|v| v.as_bytes()),
                "seed={seed} op={op_idx}: 已存在 key {key} 读取不一致",
            );
            continue;
        }

        dir.insert(key.as_bytes(), val.as_bytes());
        truth.insert(key.clone(), val);

        // 插入后立即验证刚插入的 key 可读.
        if let Some(got) = dir.get(key.as_bytes()) {
            let expected = truth.get(&key).unwrap().as_bytes();
            assert_eq!(
                got, expected,
                "seed={seed} op={op_idx}: 刚插入 key {key} 值错",
            );
        } else {
            panic!(
                "MINIMAL REPRO -> seed={seed} op={op_idx} key={key}: \
                 leaf_insert Ok 但 leaf_get None (前缀压缩链损坏). \
                 total_keys={} truth_size={}",
                dir.total_keys(),
                truth.len(),
            );
        }
    }

    // 全量验证.
    for (k, v) in &truth {
        let got = dir.get(k.as_bytes());
        assert_eq!(
            got.as_deref(),
            Some(v.as_bytes()),
            "seed={seed}: 全量验证 key {k} 丢失/值错",
        );
    }
}

#[test]
fn prefix_chaos_realkeys_single_seed() {
    run_one_seed(0x1234_5678_DEAD_BEEF, 20_000);
}

#[test]
fn prefix_chaos_realkeys_multi_seed() {
    // 多 seed 覆盖不同的交错插入顺序 / split 边界.
    let seeds = [
        0x0000_0000_0000_0001,
        0xDEAD_BEEF_CAFE_BABE,
        0x1111_2222_3333_4444,
        0xFFFF_FFFF_0000_0000,
        0xA5A5_5A5A_A5A5_5A5A,
        0x0123_4567_89AB_CDEF,
        0x9E37_79B9_7F4A_7C15,
        0xB504_F333_F9DE_6484,
    ];
    for &seed in &seeds {
        run_one_seed(seed, 8_000);
    }
}
