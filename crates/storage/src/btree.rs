//! T15 多层 BTree 路由: travel + insert + split propagation.
//!
//! ## 设计 (来自 plan `2026-07-20-multi-level-btree.md`)
//!
//! 升级 Table BTree 和 TableDirectory 从单 leaf page 到完整多层 B+Tree.
//! MetaPage 保持单 leaf page (DESIGN 文档明说 db < 100 够用).
//!
//! ## 路由流程
//!
//! 1. `travel_to_leaf(root_vpid, key)`: 从 root 一路 travel 到 leaf, 沿途记录
//!    `TravelPath` (栈式 `Vec<TravelStep>`). 每条 step 记录走到下一层时用的
//!    `sep_key` 和选中的 `child_vpid`. 供 split 传播时按 key 重定位.
//! 2. `btree_insert(root_vpid, key, value)`:
//!    a. travel 到 leaf
//!    b. 调 `leaf_insert` 尝试插入
//!    c. 成功 → 写回 leaf (PageWriteBatch), 返回 None (无 root split)
//!    d. `PageFull` → 调 `leaf_split` 切两半 → 创建 right vpid (`Pager::create`)
//!    e. 把 (left, right) + parent update 一次性 add 到 PageWriteBatch
//!    f. parent 满 → 继续 split, 沿 TravelPath 向上传播
//!    g. root 满 → 切 root + 创建新 root (返回 Some(new_root_vpid))
//!
//! ## PageWriteBatch 原子性 (DESIGN §3.0.5)
//!
//! 整个 split 流程 (left + right + parent update [+ grandparent + new_root])
//! 必须在**同一个** `PageWriteBatch` 内 add 完, 然后一次 submit. 防止 split
//! 半完成状态 (e.g., left split 了但 parent 没 update, 路径断裂).
//!
//! ## Travel Key Path 协议 (DESIGN §3.0.1)
//!
//! split 传播时: 用 `sep_key` 找到原 child_vpid 所在 internal entry, update
//! 其 child_vpid 为 new_right_vpid. 避免 vpid-based stack 失效 (split 后原
//! vpid 仍指向 left, 但 parent 该指向 right).
//!
//! ## 单线程假设
//!
//! 与 Pager 契约一致: BTree 路由假设 caller 串行使用 Pager (per-shard single-threaded).
//! 并发 split 由 caller 端的 TravelTree 解决 (本文件暂不直接用 TravelTree).

use std::io;

use page::{
    PageType, Vpid, internal_child, internal_insert, internal_new, internal_split, leaf_get,
    leaf_insert, leaf_new, leaf_split, page_type,
};

use crate::pager::Pager;

// =====================================================================
// 错误类型
// =====================================================================

/// BTree 路由错误.
#[derive(Debug, thiserror::Error)]
pub enum BTreeError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("page error: {0}")]
    Page(#[from] page::PageError),
    #[error("bad page type at vpid {vpid}: expected Leaf/Internal, got {page_type:?}")]
    BadPageType { vpid: Vpid, page_type: PageType },
    #[error("internal_child returned None at vpid {0}")]
    InternalChildNone(Vpid),
    #[error("root vpid {0} not found")]
    RootNotFound(Vpid),
    #[error("page full: split propagation exhausted (deep tree)")]
    SplitExhausted,
}

// =====================================================================
// TravelPath: travel 路径栈
// =====================================================================

/// travel 时穿过的一层 (从 root → ... → leaf parent 顺序).
///
/// - `internal_vpid`: 该层 internal page 的 vpid
/// - `sep_key`: 走到下一层时用的 key (用于 split 传播时重定位)
/// - `child_vpid`: 选中的下一层 child vpid
///
/// **不变量**: 最后一条 step 的 `child_vpid` 是 leaf vpid. leaf parent 是
/// 最后一条 step 的 `internal_vpid`.
#[derive(Clone, Debug)]
pub struct TravelStep {
    pub internal_vpid: Vpid,
    pub sep_key: Vec<u8>,
    pub child_vpid: Vpid,
}

/// 完整 travel 路径.
#[derive(Default, Debug)]
pub struct TravelPath {
    stack: Vec<TravelStep>,
}

impl TravelPath {
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }

    pub fn push(&mut self, step: TravelStep) {
        self.stack.push(step);
    }

    /// 路径深度 (经过的 internal 层数).
    pub fn depth(&self) -> usize {
        self.stack.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stack.is_empty()
    }

    /// leaf 的直接 parent. 如果树高 == 1 (root = leaf), 返回 None.
    pub fn leaf_parent(&self) -> Option<&TravelStep> {
        self.stack.last()
    }

    /// leaf parent 的 vpid (split 时要 update 的 internal).
    pub fn leaf_parent_vpid(&self) -> Option<Vpid> {
        self.stack.last().map(|s| s.internal_vpid)
    }

    /// 沿路径向上的迭代器 (从 leaf parent → ... → root).
    pub fn iter_up(&self) -> impl Iterator<Item = &TravelStep> {
        self.stack.iter().rev()
    }

    /// 把路径里所有 internal 重新记录 (split 后 vpid 流转时, 上层可能用新 vpid).
    /// 简化版: 本文件暂不用 (caller 自己知道 split 后哪些 vpid 变了).
    #[allow(dead_code)]
    pub fn replace(&mut self, old_vpid: Vpid, new_vpid: Vpid) {
        for step in &mut self.stack {
            if step.internal_vpid == old_vpid {
                step.internal_vpid = new_vpid;
            }
            if step.child_vpid == old_vpid {
                step.child_vpid = new_vpid;
            }
        }
    }
}

// =====================================================================
// travel_to_leaf
// =====================================================================

/// 沿 key 从 root_vpid 一路 travel 到 leaf. 返回 (leaf_vpid, travel_path).
///
/// **算法**:
/// - current = root_vpid
/// - loop:
///   - 读 current page
///   - Leaf → 返回 (current, path)
///   - Internal → 调 `internal_child(page, key)` 选 child → push step → current = child
pub async fn travel_to_leaf(
    pager: &mut Pager,
    root_vpid: Vpid,
    key: &[u8],
) -> Result<(Vpid, TravelPath), BTreeError> {
    let mut path = TravelPath::new();
    let mut current_vpid = root_vpid;
    let mut depth = 0usize;
    const MAX_DEPTH: usize = 16; // 防御性: 16 层 ≈ 16^32 entries, 远超实际

    loop {
        if depth > MAX_DEPTH {
            return Err(BTreeError::SplitExhausted);
        }
        let page = pager.read(current_vpid).await?;
        let pt = page_type(&page[..]);
        match pt {
            PageType::Leaf => {
                crate::page_pool::recycle(page);
                return Ok((current_vpid, path));
            }
            PageType::Internal => {
                let child_vpid = internal_child(&page[..], key)
                    .ok_or(BTreeError::InternalChildNone(current_vpid))?;
                path.push(TravelStep {
                    internal_vpid: current_vpid,
                    sep_key: key.to_vec(),
                    child_vpid,
                });
                // ⭐ 热路径优化: 中间页用完归还页池
                crate::page_pool::recycle(page);
                current_vpid = child_vpid;
                depth += 1;
            }
            other => {
                // ⭐ DIAG: 坏页诊断 — dump 页头 + meta 映射 (NLOG_DIAG=1)
                if std::env::var("NLOG_DIAG").is_ok_and(|v| v == "1") {
                    let hdr_vpid = u64::from_le_bytes(
                        page[0x18..0x20].try_into().unwrap_or_default(),
                    );
                    let pid_info = pager.meta_debug_iter()
                        .into_iter()
                        .find(|(v, _)| *v == current_vpid)
                        .map(|(_, p)| format!(
                            "file={} chunk={} page_idx={}",
                            p.file_id(), p.chunk_idx(), p.page_idx()
                        ))
                        .unwrap_or_else(|| "UNMAPPED".into());
                    eprintln!(
                        "[DIAG-BADPAGE] vpid={current_vpid} root={root_vpid} depth={depth} \
                         expected=Leaf/Internal got={other:?} \
                         hdr_vpid={hdr_vpid} hdr_magic={:02X?} \
                         meta_pid=[{pid_info}] key={key:?}",
                        &page[0..4]
                    );
                }
                return Err(BTreeError::BadPageType {
                    vpid: current_vpid,
                    page_type: other,
                });
            }
        }
    }
}

/// ⭐ 只读/原地更新版 travel: 无 TravelPath (免每层 sep_key 拷贝),
/// 且直接返回 leaf 字节 (省 caller 的第二次 pager.read = 16KB copy).
///
/// lookup / update / delete 用 (它们丢弃 path); insert 的 split 传播
/// 仍走 `travel_to_leaf`.
pub async fn travel_to_leaf_ro(
    pager: &mut Pager,
    root_vpid: Vpid,
    key: &[u8],
) -> Result<(Vpid, Box<[u8; page::PAGE_SIZE]>), BTreeError> {
    let (guide, bytes) = travel_to_leaf_guided(pager, root_vpid, key).await?;
    Ok((guide.leaf_vpid, bytes))
}

/// ⭐ leaf 覆盖区间指南 (批量操作的 travel 复用凭据).
///
/// travel 每层用 `internal_child_with_bounds` 收窄 running bounds,
/// 最终 `[lower, upper)` 即该 leaf 的 key 覆盖区间 (None 边不设限).
/// 批内排序后的下一个 key `contains` 命中 → 同一 leaf, 免回 root travel.
#[derive(Debug, Clone)]
pub struct LeafGuide {
    pub leaf_vpid: Vpid,
    pub lower: Option<Vec<u8>>,
    pub upper: Option<Vec<u8>>,
}

impl LeafGuide {
    /// key 是否落在本 leaf 覆盖区间 `[lower, upper)` 内.
    pub fn contains(&self, key: &[u8]) -> bool {
        if let Some(lo) = &self.lower
            && key < lo.as_slice()
        {
            return false;
        }
        if let Some(hi) = &self.upper
            && key >= hi.as_slice()
        {
            return false;
        }
        true
    }
}

/// ⭐ 区间版 travel: 返回 (LeafGuide, leaf 字节).
///
/// 与 `travel_to_leaf_ro` 同骨架, 每层收窄 running bounds
/// (B+ 嵌套性质: 子层区间 ⊆ 父层区间; 边界槽 sep=None 时保留父层 bound).
pub async fn travel_to_leaf_guided(
    pager: &mut Pager,
    root_vpid: Vpid,
    key: &[u8],
) -> Result<(LeafGuide, Box<[u8; page::PAGE_SIZE]>), BTreeError> {
    use page::internal_child_with_bounds;

    let mut current_vpid = root_vpid;
    let mut depth = 0usize;
    let mut lower: Option<Vec<u8>> = None;
    let mut upper: Option<Vec<u8>> = None;
    const MAX_DEPTH: usize = 16;

    loop {
        if depth > MAX_DEPTH {
            return Err(BTreeError::SplitExhausted);
        }
        let page = pager.read(current_vpid).await?;
        match page_type(&page[..]) {
            PageType::Leaf => {
                return Ok((
                    LeafGuide {
                        leaf_vpid: current_vpid,
                        lower,
                        upper,
                    },
                    page,
                ));
            }
            PageType::Internal => {
                let (child_vpid, lo, hi) = internal_child_with_bounds(&page[..], key)
                    .ok_or(BTreeError::InternalChildNone(current_vpid))?;
                // 收窄 running bounds (Some 才收窄; 嵌套性质下子层 bound 更紧)
                if lo.is_some() {
                    lower = lo;
                }
                if hi.is_some() {
                    upper = hi;
                }
                crate::page_pool::recycle(page);
                current_vpid = child_vpid;
                depth += 1;
            }
            other => {
                // ⭐ DIAG: 坏页诊断 (guided 版)
                if std::env::var("NLOG_DIAG").is_ok_and(|v| v == "1") {
                    let hdr_vpid = u64::from_le_bytes(
                        page[0x18..0x20].try_into().unwrap_or_default(),
                    );
                    let pid_info = pager.meta_debug_iter()
                        .into_iter()
                        .find(|(v, _)| *v == current_vpid)
                        .map(|(_, p)| format!(
                            "file={} chunk={} page_idx={}",
                            p.file_id(), p.chunk_idx(), p.page_idx()
                        ))
                        .unwrap_or_else(|| "UNMAPPED".into());
                    eprintln!(
                        "[DIAG-BADPAGE] vpid={current_vpid} root={root_vpid} depth={depth} \
                         expected=Leaf/Internal got={other:?} \
                         hdr_vpid={hdr_vpid} hdr_magic={:02X?} \
                         meta_pid=[{pid_info}] key={key:?}",
                        &page[0..4]
                    );
                }
                return Err(BTreeError::BadPageType {
                    vpid: current_vpid,
                    page_type: other,
                });
            }
        }
    }
}

/// ⭐ 批量 lookup: 排序迭代 + LeafGuide 区间复用 (同 leaf 的 key 免重复 travel).
///
/// 结果按**原输入顺序**返回. 返回值附带 travel 次数 (观测/测试用).
pub async fn btree_lookup_many(
    pager: &mut Pager,
    root_vpid: Vpid,
    keys: &[&[u8]],
) -> Result<(Vec<Option<Vec<u8>>>, usize), BTreeError> {
    let mut results: Vec<Option<Vec<u8>>> = vec![None; keys.len()];
    if keys.is_empty() {
        return Ok((results, 0));
    }
    // 排序索引 (原顺序回填)
    let mut order: Vec<usize> = (0..keys.len()).collect();
    order.sort_by(|&a, &b| keys[a].cmp(keys[b]));

    let mut travels = 0usize;
    let mut cur: Option<(LeafGuide, Box<[u8; page::PAGE_SIZE]>)> = None;
    for &i in &order {
        let key = keys[i];
        let hit = matches!(&cur, Some((g, _)) if g.contains(key));
        if !hit {
            if let Some((_, old)) = cur.take() {
                crate::page_pool::recycle(old);
            }
            cur = Some(travel_to_leaf_guided(pager, root_vpid, key).await?);
            travels += 1;
        }
        let (_, leaf_bytes) = cur.as_ref().expect("just filled");
        results[i] = leaf_get(&leaf_bytes[..], key);
    }
    if let Some((_, old)) = cur.take() {
        crate::page_pool::recycle(old);
    }
    Ok((results, travels))
}

/// ⭐ Phase R: 前缀范围扫描. 顺序遍历所有 key 以 `prefix` 开头的行,
/// 每命中一项以 (physical_key, value) 借用回调; 回调 `Break` 早停
/// (limit / 上层提前结束).
///
/// **跨 leaf**: `travel_to_leaf_guided(start)` → `leaf_scan_from` 扫本 leaf →
/// 本 leaf 扫尽后 `next = guide.upper` (下一 leaf 下界); `upper==None` 或
/// `upper` 不再以 `prefix` 开头 → 后续 leaf 全越界, 停. 有序 BTree 中
/// 首个不带 prefix 的 key 即全局下界, 回调内 `Break` 实现早停.
pub async fn btree_scan<F: FnMut(&[u8], &[u8]) -> core::ops::ControlFlow<()>>(
    pager: &mut Pager,
    root_vpid: Vpid,
    prefix: &[u8],
    f: &mut F,
) -> Result<(), BTreeError> {
    btree_scan_from(pager, root_vpid, prefix, prefix, f).await
}

/// ⭐ Q4 (SQL 索引): 从 `start` (>= prefix) 开始的前缀扫描 — 范围查询
/// (`WHERE idx >= lo`) 跳过下界之前的行, 不必从前缀头扫起.
/// `btree_scan` = `btree_scan_from(start = prefix)`. 上界由回调 Break 实现.
pub async fn btree_scan_from<F: FnMut(&[u8], &[u8]) -> core::ops::ControlFlow<()>>(
    pager: &mut Pager,
    root_vpid: Vpid,
    start: &[u8],
    prefix: &[u8],
    f: &mut F,
) -> Result<(), BTreeError> {
    use core::ops::ControlFlow;
    let mut start: Vec<u8> = start.to_vec();
    let mut leaf_count = 0u32;
    let mut total_keys = 0u64;
    loop {
        let (guide, leaf_bytes) = travel_to_leaf_guided(pager, root_vpid, &start).await?;
        let kc = page::page_key_count(&leaf_bytes[..]);
        // 本 leaf 扫 key >= start; 首遇不带 prefix 的 key → Break (全局下界)
        let mut scanned = 0u64;
        let flow = page::leaf_scan_from(&leaf_bytes[..], &start, &mut |k: &[u8], v: &[u8]| {
            if !k.starts_with(prefix) {
                return ControlFlow::Break(());
            }
            scanned += 1;
            f(k, v)
        });
        leaf_count += 1;
        total_keys += scanned;
        let upper = guide.upper.clone();
        // ⭐ DIAG: 扫描路径追踪 (NLOG_DIAG=1)
        if crate::chunk_writer::diag_enabled() {
            eprintln!(
                "[DIAG-SCAN] leaf#{leaf_count} vpid={} key_count={kc} scanned={scanned} \
                 upper={upper:?} start={start:?}",
                guide.leaf_vpid
            );
        }
        crate::page_pool::recycle(leaf_bytes);
        // 回调 Break (limit) 或 前缀越界 → 全局结束
        if matches!(flow, ControlFlow::Break(())) {
            return Ok(());
        }
        // 本 leaf 扫尽, 去下一 leaf; upper==None (最右 leaf) 或 upper 越前缀 → 停
        match upper {
            Some(next) if next.starts_with(prefix) => start = next,
            _ => return Ok(()),
        }
    }
}

// =====================================================================
// btree_insert: 单 key 插入, 自动 split 传播
// =====================================================================

/// 插入 (key, value) 到 root_vpid 指向的 BTree.
///
/// 返回:
/// - `Ok(None)`: 插入成功, root 没变
/// - `Ok(Some(new_root_vpid))`: root split 了, 树高 +1, caller 应更新 root_vpid 引用
/// - `Err(_)`: 错误
///
/// **整体流程** (单 PageWriteBatch 提交):
/// 1. travel 到 leaf
/// 2. 调 leaf_insert 尝试插入
/// 3. 成功 → add leaf 到 batch, submit, return None
/// 4. PageFull → 走 propagate_split_up
pub async fn btree_insert(
    pager: &mut Pager,
    root_vpid: Vpid,
    key: &[u8],
    value: &[u8],
) -> Result<Option<Vpid>, BTreeError> {
    // 1. travel 到 leaf
    let (leaf_vpid, mut path) = travel_to_leaf(pager, root_vpid, key).await?;

    // 2. 读 leaf 字节, 尝试 insert
    let mut leaf_bytes = pager.read(leaf_vpid).await?;
    let insert_result = leaf_insert(&mut *leaf_bytes, key, value);

    let mut batch = pager.new_write_batch();
    match insert_result {
        Ok(()) => {
            // 3. 成功: 写回 leaf
            batch.add(leaf_vpid, leaf_bytes);
            batch.submit(pager).await?;
            Ok(None)
        }
        Err(page::PageError::PageFull) => {
            // 4. PageFull: 走 split 传播
            let orig_key_count = page::page_key_count(&leaf_bytes[..]);
            let mut right_bytes = leaf_new();
            let split_key = leaf_split(&mut leaf_bytes, &mut right_bytes)?;
            // 4a. 把触发的 key 插入到正确的 half:
            //     - key > split_key → 进 right (split_key 是 right 首 key)
            //     - key <= split_key → 进 left (split 后 left 有空间)
            //
            //     ⭐ 修复 (2026-07-25): 之前假设触发 key 一定 > split_key,
            //     对非顺序插入不成立 (e.g. "v..." 夹在 "t..." 和 "warmup..." 之间,
            //     split_key 可能是 "warmup...", 此时 key < split_key).
            if key > split_key.as_slice() {
                page::leaf_insert(&mut right_bytes, key, value)?;
            } else {
                page::leaf_insert(&mut *leaf_bytes, key, value)?;
            }
            // ⭐ DIAG: split 后 key 守恒校验
            if crate::chunk_writer::diag_enabled() {
                let left_count = page::page_key_count(&leaf_bytes[..]);
                let right_count = page::page_key_count(&right_bytes[..]);
                let total_after = left_count as u32 + right_count as u32;
                let expected = orig_key_count as u32 + 1; // +1 for the triggering key
                if total_after != expected {
                    eprintln!(
                        "[DIAG-SPLIT-LEAK] key count mismatch! before={orig_key_count} \
                         after={total_after} (left={left_count} right={right_count}) \
                         expected={expected} split_key={split_key:?}"
                    );
                }
            }

            // 4b. 创建 right vpid (含触发的 key)
            let right_vpid = pager.create(Box::new(right_bytes)).await?;

            // 4c. add left 写回 (right 已通过 create 持久化, 不能再 add 同一 vpid)
            batch.add(leaf_vpid, leaf_bytes);

            // 4d. propagate split up: 处理 parent insert (split_key, right_vpid)
            //     old_root_vpid 用来 root split 时作为新 root 的 first_child
            let new_root = propagate_split_up(
                pager, &mut path, root_vpid, &split_key, right_vpid, &mut batch,
            )
            .await?;
            batch.submit(pager).await?;
            // ⭐ DIAG: split 后回查验证 — 确认 split_key 从根可达
            if crate::chunk_writer::diag_enabled() {
                let check_root = new_root.unwrap_or(root_vpid);
                match btree_lookup(pager, check_root, &split_key).await {
                    Ok(Some(_)) => {} // OK
                    Ok(None) => {
                        eprintln!(
                            "[DIAG-SPLIT-LOST] split_key={split_key:?} NOT reachable after split! \
                             root={check_root} leaf={leaf_vpid} right={right_vpid}"
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "[DIAG-SPLIT-ERR] split_key={split_key:?} lookup error: {e} \
                             root={check_root} leaf={leaf_vpid} right={right_vpid}"
                        );
                    }
                }
            }
            Ok(new_root)
        }
        Err(e) => Err(BTreeError::Page(e)),
    }
}

// =====================================================================
// propagate_split_up: 沿 travel_path 向上传播 split
// =====================================================================

/// 把 (split_key, new_right_vpid) 沿 path 向上传播.
///
/// **逻辑**:
/// - 从 path 顶部 (leaf parent) 开始
/// - 读 parent page → 尝试 `internal_insert(split_key, new_right_vpid)`
/// - 成功 → 写回 parent (add 到 batch), 返回 `Ok(None)`
/// - `PageFull` → `internal_split` → 创建 new_right_vpid (via `pager.create`)
///   - 新的 split_key = `internal_split` 返回的 key
///   - 继续向上一层
/// - path 走完 (全 split) → 创建新 root (root split, 通过 `pager.create`)
///
/// **所有 left page 写回累积在 caller 传入的 `batch` 中**, caller 一次 submit.
/// right page 通过 `pager.create` 持久化, 不走 batch. 原子性靠 caller submit 触发.
///
/// **前置条件**: 调用前 batch 已 add 了原始 leaf 的 left 写回 (split 后的 left).
///
/// **参数**:
/// - `old_root_vpid`: 原始 root, 供 root split 时用作 first_child
async fn propagate_split_up(
    pager: &mut Pager,
    path: &mut TravelPath,
    old_root_vpid: Vpid,
    split_key: &[u8],
    new_right_vpid: Vpid,
    batch: &mut crate::pager::PageWriteBatch,
) -> Result<Option<Vpid>, BTreeError> {
    let mut current_split_key = split_key.to_vec();
    let mut current_right_vpid = new_right_vpid;

    // 从 leaf parent 开始向上
    while let Some(step) = path.stack.pop() {
        let parent_vpid = step.internal_vpid;
        let mut parent_bytes = pager.read(parent_vpid).await?;
        // 尝试 insert (current_split_key, current_right_vpid) 到 parent
        match internal_insert(&mut *parent_bytes, &current_split_key, current_right_vpid) {
            Ok(()) => {
                // 成功: 写回 parent, 返回
                batch.add(parent_vpid, parent_bytes);
                return Ok(None);
            }
            Err(page::PageError::PageFull) => {
                // parent 也满: 继续 split
                let mut parent_right = internal_new();
                let new_split_key = internal_split(&mut parent_bytes, &mut parent_right)?;
                // ⭐ 关键修复: split 后, (current_split_key, current_right_vpid) 需插入到
                // 正确的 half. current_split_key 来自子层 split (key 介于子层原页 max 和
                // 子层 right 第一个之间). 父层 split 把 mid item 移到 parent_right (作为 first),
                // 因此:
                //   - 如果 current_split_key >= new_split_key: 进 parent_right (子层 right
                //     的 key 范围 >= parent_split 的 mid, 所以 parent_right 是 right 的 right)
                //   - 如果 current_split_key < new_split_key: 进 parent (left), 因为子层 right
                //     的 key 范围 < parent_split 的 mid, 与 left half 重叠.
                //
                // 之前代码无条件 insert 到 parent_right, 在 current_split_key < new_split_key
                // 时会错位 (lookup 路由到 parent_right 找不到).
                if current_split_key.as_slice() >= new_split_key.as_slice() {
                    page::internal_insert(
                        &mut parent_right,
                        &current_split_key,
                        current_right_vpid,
                    )?;
                } else {
                    page::internal_insert(
                        &mut *parent_bytes,
                        &current_split_key,
                        current_right_vpid,
                    )?;
                }

                let new_parent_right_vpid = pager.create(Box::new(parent_right)).await?;

                // 写回 parent (left, 已 add 触发的 current_split_key)
                batch.add(parent_vpid, parent_bytes);

                // 准备向上传播
                current_split_key = new_split_key;
                current_right_vpid = new_parent_right_vpid;
                // 继续 while loop, 处理 path 的上一层
            }
            Err(e) => {
                return Err(BTreeError::Page(e));
            }
        }
    }

    // path 走完了: 说明 root 也 split 了, 需要新 root
    // - 如果 path 原本非空: current_right_vpid 是"新分裂出的 right page",
    //   old_root_vpid 是最上层 internal 的 left (也已经被 batch 写回)
    // - 如果 path 原本就空: root = leaf 情况, old_root_vpid = 原始 leaf (= root)
    //
    // 两种情况都走 create_new_root(old_root_vpid, current_split_key, current_right_vpid)
    let new_root =
        create_new_root(pager, old_root_vpid, current_split_key, current_right_vpid).await?;
    Ok(Some(new_root))
}

/// 创建新 root (root split 时的兜底).
///
/// 新 root 是 internal page, first_child = old_root_vpid (即 batch 中已 add 的
/// left 写回的那个 page), 然后 insert (sep_key, right_child_vpid).
///
/// 流程:
/// 1. 调 `internal_new()` 创建空 internal page
/// 2. 设置 first_child = old_root_vpid (通过 page_vpid header field)
/// 3. 调 `internal_insert(root, sep_key, right_child_vpid)`
/// 4. 通过 `Pager::create` 分配新 vpid 并持久化
///
/// **不走 batch**: `Pager::create` 已持久化, caller submit 时不再重复 add.
async fn create_new_root(
    pager: &mut Pager,
    old_root_vpid: Vpid,
    sep_key: Vec<u8>,
    right_child_vpid: Vpid,
) -> Result<Vpid, BTreeError> {
    use page::page_set_vpid;

    let mut new_root_bytes = internal_new();
    // first_child 通过 page header vpid 字段记录 (page crate 设计)
    page_set_vpid(&mut new_root_bytes, old_root_vpid);
    // 插入 (sep_key, right_child_vpid)
    internal_insert(&mut new_root_bytes, &sep_key, right_child_vpid)?;
    // 分配新 vpid 并持久化
    let new_root_vpid = pager.create(Box::new(new_root_bytes)).await?;
    Ok(new_root_vpid)
}

// =====================================================================
// btree_lookup: 走 travel 找 key
// =====================================================================

/// 在 root_vpid 指向的 BTree 中查找 key. 返回 Some(value) 或 None.
pub async fn btree_lookup(
    pager: &mut Pager,
    root_vpid: Vpid,
    key: &[u8],
) -> Result<Option<Vec<u8>>, BTreeError> {
    let (_leaf_vpid, leaf_bytes) = travel_to_leaf_ro(pager, root_vpid, key).await?;
    let out = leaf_get(&leaf_bytes[..], key);
    crate::page_pool::recycle(leaf_bytes);
    Ok(out)
}

// =====================================================================
// btree_delete: 走 travel 删 key (暂不实现 merge)
// =====================================================================

/// 删除 key. 返回 true 表示存在并删除.
///
/// **简化**: 暂不实现 merge / underflow 处理. 删后 leaf 可能空, 留 polish.
pub async fn btree_delete(
    pager: &mut Pager,
    root_vpid: Vpid,
    key: &[u8],
) -> Result<bool, BTreeError> {
    use page::leaf_delete;
    let (leaf_vpid, mut leaf_bytes) = travel_to_leaf_ro(pager, root_vpid, key).await?;
    let existed = leaf_delete(&mut *leaf_bytes, key)?;
    if existed {
        let mut batch = pager.new_write_batch();
        batch.add(leaf_vpid, leaf_bytes);
        batch.submit(pager).await?; // submit 内 memcpy 后归还页池
    } else {
        crate::page_pool::recycle(leaf_bytes);
    }
    Ok(existed)
}

// =====================================================================
// btree_update: 走 travel 找 leaf 然后 leaf_update (不触发 split)
// =====================================================================

/// 更新 key 对应的 value. 返回 Ok(true) 表示成功, Ok(false) 表示 key 不存在.
///
/// **不触发 split**: value 长度变化可能让 leaf 变满, 但 leaf_update 设计为
/// 原地替换 (new_n == old_n 时), 一般不会触发 split. 如果新 value 比旧 value
/// 大很多导致空间不够, 会返回 `PageFull` 错误, caller 应改用 delete + insert.
///
/// **简化版**: 走 travel → leaf_update → PageWriteBatch 写回. 不走 split 传播.
pub async fn btree_update(
    pager: &mut Pager,
    root_vpid: Vpid,
    key: &[u8],
    new_value: &[u8],
) -> Result<bool, BTreeError> {
    use page::leaf_update;
    let (leaf_vpid, mut leaf_bytes) = travel_to_leaf_ro(pager, root_vpid, key).await?;
    let updated = leaf_update(&mut *leaf_bytes, key, new_value)?;
    if updated {
        let mut batch = pager.new_write_batch();
        batch.add(leaf_vpid, leaf_bytes);
        batch.submit(pager).await?; // submit 内 memcpy 后归还页池
    } else {
        crate::page_pool::recycle(leaf_bytes);
    }
    Ok(updated)
}

// =====================================================================
// btree_count: 走 travel 数 leaf 个数 (调试用, 仅单层准确)
// =====================================================================

/// 数 leaf page 中 key 的总数. 走单 leaf 计数 (多 level 时需遍历所有 leaf,
/// 本实现暂只走 root = leaf 的情况, 多层时只能粗略估计).
#[allow(dead_code)]
pub async fn btree_count(pager: &mut Pager, root_vpid: Vpid) -> Result<u64, BTreeError> {
    let (leaf_vpid, _path) = travel_to_leaf(pager, root_vpid, b"").await?;
    let leaf_bytes = pager.read(leaf_vpid).await?;
    Ok(page::page_key_count(&leaf_bytes[..]) as u64)
}

// =====================================================================
// btree_scan_leaves: 收集所有 leaf vpid (供 list_tables / 全量 dump 用)
// =====================================================================

/// 走 BTree 收集所有 leaf vpid. 单层直接返回 [root_vpid]; 多层走 internal page
/// 收集所有 first_child / separator 指向的 child vpid (递归) 直到 leaf.
///
/// **实现**:
/// - 读 root page
/// - Leaf → 返回 [root_vpid]
/// - Internal → first_child 进 list + 遍历所有 separator 的 child_vpid 进 list,
///   然后对每个 child 递归.
pub async fn btree_collect_leaves(
    pager: &mut Pager,
    root_vpid: Vpid,
) -> Result<Vec<Vpid>, BTreeError> {
    let mut acc = Vec::new();
    let mut stack = vec![root_vpid];

    while let Some(vpid) = stack.pop() {
        if acc.contains(&vpid) {
            continue;
        }
        let page = pager.read(vpid).await?;
        match page_type(&page[..]) {
            PageType::Leaf => {
                acc.push(vpid);
            }
            PageType::Internal => {
                // first_child 是 page header vpid 字段
                let first_child = page::page_vpid(&page[..]);
                stack.push(first_child);
                // 遍历所有 separator 收集 child vpid
                let idx = page::PageIndex::load(&page[..], page::ItemKind::Internal)
                    .map_err(BTreeError::Page)?;
                // 段内顺序扫描 + 段间跳转
                let mut seg_idx = 0usize;
                while seg_idx < idx.segments.len() {
                    let seg = &idx.segments[seg_idx];
                    let mut ptr =
                        page::InternalItemPtr::new(&page[..], seg.first_item_off as usize)
                            .map_err(BTreeError::Page)?;
                    if seg_idx == 0 && ptr.key().is_empty() {
                        // 跳哨兵
                        if let Some(next) = ptr.next().map_err(BTreeError::Page)? {
                            ptr = next;
                        } else {
                            seg_idx += 1;
                            continue;
                        }
                    }
                    loop {
                        let child = ptr.child_vpid();
                        stack.push(child);
                        match ptr.next().map_err(BTreeError::Page)? {
                            Some(next) => ptr = next,
                            None => break,
                        }
                    }
                    seg_idx += 1;
                }
            }
            other => {
                return Err(BTreeError::BadPageType {
                    vpid,
                    page_type: other,
                });
            }
        }
    }

    Ok(acc)
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc::{PidAllocator, VpidAllocator};
    use crate::chunk_lru::ChunkList;
    use crate::chunk_writer::{ChunkWriter, NowChunks};
    use crate::meta_cache::MetaCache;
    use crate::pager::Pager;

    /// 构造一个测试 Pager (单 page BTree 场景, root = leaf).
    fn test_pager(tmp: &tempfile::TempDir) -> (Pager, Vpid) {
        let block_dir = tmp.path().to_path_buf();
        // 创建 page.mate 占位
        let mate = block_dir.join("page.mate");
        let f = std::fs::File::create(&mate).unwrap();
        f.set_len(10 * 1024 * 1024).unwrap();
        // 创建 .block 占位
        let block = block_dir.join("000001.block");
        let f = std::fs::File::create(&block).unwrap();
        f.set_len(10 * 1024 * 1024).unwrap();

        let mut meta = MetaCache::open(&mate).unwrap();
        // 给 MetaPage 占 vpid 0
        let meta_vpid = 0u64;
        let meta_pid = crate::types::META_PID;
        meta.write(meta_vpid, meta_pid);
        let mut p = Pager::new(
            block_dir,
            meta,
            VpidAllocator::new(1),      // user data 从 1 开始
            PidAllocator::new(0, 0, 1), // 跳过 page 0 (MetaPage)
            ChunkList::new(4),
            NowChunks::new(),
            ChunkWriter::new(&block).unwrap(),
        );
        // 创建 root (leaf)
        let root = pollster::block_on(p.create(Box::new(leaf_new()))).unwrap();
        (p, root)
    }

    #[test]
    fn travel_to_leaf_returns_root_when_root_is_leaf() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut p, root) = test_pager(&tmp);
        let (leaf, path) = pollster::block_on(travel_to_leaf(&mut p, root, b"anykey")).unwrap();
        assert_eq!(leaf, root, "root = leaf 时 leaf vpid = root vpid");
        assert!(path.is_empty(), "root = leaf 时 path 应为空");
    }

    #[test]
    fn btree_insert_into_single_leaf_succeeds() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut p, root) = test_pager(&tmp);
        // 插入少量 KV, 不应触发 split
        for i in 0..10 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{:04}", i);
            pollster::block_on(btree_insert(&mut p, root, key.as_bytes(), val.as_bytes())).unwrap();
        }
        // 读回
        for i in 0..10 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{:04}", i);
            let got = pollster::block_on(btree_lookup(&mut p, root, key.as_bytes())).unwrap();
            assert_eq!(got, Some(val.into_bytes()));
        }
    }

    #[test]
    fn btree_insert_triggers_leaf_split() {
        // 写 ~500 KV, 触发 leaf split
        let tmp = tempfile::tempdir().unwrap();
        let (mut p, mut root) = test_pager(&tmp);
        for i in 0..500u32 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{:04}", i);
            if let Some(new_root) =
                pollster::block_on(btree_insert(&mut p, root, key.as_bytes(), val.as_bytes()))
                    .unwrap()
            {
                root = new_root;
            }
        }
        // 读回
        for i in 0..500u32 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{:04}", i);
            let got = pollster::block_on(btree_lookup(&mut p, root, key.as_bytes())).unwrap();
            assert_eq!(got, Some(val.into_bytes()), "key {} 读错", i);
        }
    }

    #[test]
    fn btree_insert_triggers_internal_split() {
        // 写 ~5000 KV, 触发 internal split (多层 tree)
        let tmp = tempfile::tempdir().unwrap();
        let (mut p, mut root) = test_pager(&tmp);
        for i in 0..5000u32 {
            let key = format!("key_{:05}", i);
            let val = format!("val_{:05}", i);
            if let Some(new_root) =
                pollster::block_on(btree_insert(&mut p, root, key.as_bytes(), val.as_bytes()))
                    .unwrap()
            {
                root = new_root;
            }
        }
        // 抽样读回
        for i in (0..5000u32).step_by(100) {
            let key = format!("key_{:05}", i);
            let val = format!("val_{:05}", i);
            let got = pollster::block_on(btree_lookup(&mut p, root, key.as_bytes())).unwrap();
            assert_eq!(got, Some(val.into_bytes()), "key {} 读错", i);
        }
        // 边界: 第一个 + 最后一个
        let first = pollster::block_on(btree_lookup(&mut p, root, b"key_00000")).unwrap();
        assert_eq!(first, Some(b"val_00000".to_vec()));
        let last = pollster::block_on(btree_lookup(&mut p, root, b"key_04999")).unwrap();
        assert_eq!(last, Some(b"val_04999".to_vec()));
    }

    #[test]
    fn btree_insert_root_split_increases_height() {
        // 写大量 KV, 触发 root split
        let tmp = tempfile::tempdir().unwrap();
        let (mut p, mut root) = test_pager(&tmp);
        let mut split_count = 0u32;
        for i in 0..10000u32 {
            let key = format!("k{:05}", i);
            let val = format!("v{:05}", i);
            if let Some(new_root) =
                pollster::block_on(btree_insert(&mut p, root, key.as_bytes(), val.as_bytes()))
                    .unwrap()
            {
                root = new_root;
                split_count += 1;
            }
        }
        // 10000 KV 至少触发 1 次 root split
        assert!(
            split_count >= 1,
            "root split 至少 1 次, 实际 {}",
            split_count
        );
        // 读回所有
        for i in (0..10000u32).step_by(500) {
            let key = format!("k{:05}", i);
            let val = format!("v{:05}", i);
            let got = pollster::block_on(btree_lookup(&mut p, root, key.as_bytes())).unwrap();
            assert_eq!(got, Some(val.into_bytes()), "key {} 读错", i);
        }
    }

    #[test]
    fn btree_delete_simple() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut p, mut root) = test_pager(&tmp);
        for i in 0..100u32 {
            let key = format!("k{:04}", i);
            let val = format!("v{:04}", i);
            if let Some(new_root) =
                pollster::block_on(btree_insert(&mut p, root, key.as_bytes(), val.as_bytes()))
                    .unwrap()
            {
                root = new_root;
            }
        }
        // 删一个
        let existed = pollster::block_on(btree_delete(&mut p, root, b"k0050")).unwrap();
        assert!(existed);
        assert_eq!(
            pollster::block_on(btree_lookup(&mut p, root, b"k0050")).unwrap(),
            None
        );
        // 邻居仍在
        assert_eq!(
            pollster::block_on(btree_lookup(&mut p, root, b"k0049")).unwrap(),
            Some(b"v0049".to_vec())
        );
        assert_eq!(
            pollster::block_on(btree_lookup(&mut p, root, b"k0051")).unwrap(),
            Some(b"v0051".to_vec())
        );
    }

    #[test]
    fn btree_after_split_lookup_picks_correct_leaf() {
        // 重点: split 后 lookup 仍能正确路由 (不依赖原 leaf vpid)
        let tmp = tempfile::tempdir().unwrap();
        let (mut p, mut root) = test_pager(&tmp);
        // 插入很少, 触发 1 次 split
        for i in 0..200u32 {
            let key = format!("k{:05}", i);
            let val = format!("v{:05}", i);
            if let Some(new_root) =
                pollster::block_on(btree_insert(&mut p, root, key.as_bytes(), val.as_bytes()))
                    .unwrap()
            {
                root = new_root;
            }
        }
        eprintln!("[TEST_DEBUG] after 200 inserts, root={}", root);
        // 验证所有 key
        for i in 0..200u32 {
            let key = format!("k{:05}", i);
            let val = format!("v{:05}", i);
            let got = pollster::block_on(btree_lookup(&mut p, root, key.as_bytes())).unwrap();
            assert_eq!(
                got,
                Some(val.into_bytes()),
                "split 后 key {} lookup 失败",
                i
            );
        }
    }

    #[test]
    fn btree_after_split_lookup_picks_correct_leaf_full() {
        // 重点: split 后 lookup 仍能正确路由 (不依赖原 leaf vpid)
        let tmp = tempfile::tempdir().unwrap();
        let (mut p, mut root) = test_pager(&tmp);
        // 插入很多, 触发多次 split
        for i in 0..2000u32 {
            let key = format!("k{:05}", i);
            let val = format!("v{:05}", i);
            if let Some(new_root) =
                pollster::block_on(btree_insert(&mut p, root, key.as_bytes(), val.as_bytes()))
                    .unwrap()
            {
                root = new_root;
            }
        }
        // 读所有, 确保 split 后的 key 也能正确找到
        for i in 0..2000u32 {
            let key = format!("k{:05}", i);
            let val = format!("v{:05}", i);
            let got = pollster::block_on(btree_lookup(&mut p, root, key.as_bytes())).unwrap();
            assert_eq!(
                got,
                Some(val.into_bytes()),
                "split 后 key {} lookup 失败",
                i
            );
        }
    }
}
