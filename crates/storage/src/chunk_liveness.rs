//! ⭐ G1: chunk/block 活性统计 (GC 基础).
//!
//! 设计 (2026-07-27, GC plan G1):
//! - **纯内存**: 每 chunk 1B 活页计数 (0..=PAGES_PER_CHUNK) 平坦数组 +
//!   按 file_id 聚合的 block 活跃 chunk 计数. 不持久化.
//! - **重启反推**: `rebuild_from_meta` 遍历全量平坦 meta (v3), 按 pid 聚合
//!   重建 live count / free_chunks — 与持久化 system table 相比零一致性负担.
//! - **延迟释放**: compact 提交后 src chunk 先进 `pending_free`, meta window
//!   确认落盘 (`complete_meta_flush` 全确认点) 才 `promote` 到 `free_chunks`
//!   可复用 — 封死 "meta 落后窗口内 chunk 被复用" 的 recover 窗口.
//! - 维护点在写路径 (COW/delete/compact); 读 page 不影响活性.

use std::collections::{HashMap, HashSet};

use crate::meta_cache::MetaCache;
use crate::types::{CHUNKS_PER_BLOCK, PAGES_PER_CHUNK, PageKey, PidLocation};

/// chunk 全局索引 = file_id * CHUNKS_PER_BLOCK + chunk_idx.
fn chunk_index(file_id: u32, chunk_idx: u8) -> usize {
    file_id as usize * CHUNKS_PER_BLOCK + chunk_idx as usize
}

/// chunk/block 活性统计 + free list (纯内存, 重启从 meta 反推).
#[derive(Debug, Default)]
pub struct ChunkLiveness {
    /// 索引 = file_id * CHUNKS_PER_BLOCK + chunk_idx, 值 = 活页数. 懒扩容.
    live: Vec<u8>,
    /// 按 file_id 聚合: 活跃 (live > 0) chunk 数.
    block_active: Vec<u16>,
    /// 活跃 chunk 总数 (live > 0), O(1) 查询 (GC 债务判据).
    live_chunk_count: usize,
    /// per-chunk vpid 槽索引: 槽 → vpid (EMPTY_VPID = 空).
    /// 只对"有活页的 chunk"建表 (live 归零即移除), 避免稀疏 file_id 下
    /// 512B/chunk 的稠密数组膨胀. 供 compact 判活布局点查, 不再全扫 meta.
    vpid_slots: HashMap<usize, [u64; PAGES_PER_CHUNK]>,
    /// 全死且 meta 已确认落盘的 chunk (可复用).
    free_chunks: Vec<PageKey>,
    /// free_chunks 的成员索引 (避免 O(k²) Vec::contains).
    free_set: HashSet<PageKey>,
    /// 已提交 compact、等 meta 落盘确认的 chunk (延迟释放暂存).
    pending_free: Vec<PageKey>,
    /// pending_free 的成员索引 (去重 O(1)).
    pending_set: HashSet<PageKey>,
}

impl ChunkLiveness {
    /// vpid_slots 空槽哨兵.
    pub const EMPTY_VPID: u64 = u64::MAX;

    pub fn new() -> Self {
        Self::default()
    }

    /// 写路径: 新 pid 分配 (page 存活 +1), 并把 vpid 写入槽索引.
    pub fn on_page_alloc(&mut self, pid: PidLocation, vpid: u64) {
        let idx = chunk_index(pid.file_id(), pid.chunk_idx());
        self.ensure_capacity(idx + 1);
        let slot = pid.page_idx() as usize;
        if slot < PAGES_PER_CHUNK {
            let slots = self
                .vpid_slots
                .entry(idx)
                .or_insert([Self::EMPTY_VPID; PAGES_PER_CHUNK]);
            slots[slot] = vpid;
        }
        if self.live[idx] == 0 {
            self.block_active[pid.file_id() as usize] += 1;
            self.live_chunk_count += 1;
        }
        debug_assert!((self.live[idx] as usize) < PAGES_PER_CHUNK);
        self.live[idx] = self.live[idx].saturating_add(1);
    }

    /// 写路径: 旧 pid 死亡 (COW 覆盖 / delete). 减到 0 **不**直接进 free
    /// (延迟释放由 compact 提交流程走 pending_free; 自然死光的 chunk 由
    /// promote_dead_chunks 周期收割).
    pub fn on_page_dead(&mut self, pid: PidLocation) {
        let idx = chunk_index(pid.file_id(), pid.chunk_idx());
        if idx >= self.live.len() || self.live[idx] == 0 {
            debug_assert!(false, "on_page_dead on untracked chunk {:?}", pid);
            return; // 防御: 统计缺失时宁可少回收
        }
        let slot = pid.page_idx() as usize;
        if let Some(slots) = self.vpid_slots.get_mut(&idx)
            && slot < PAGES_PER_CHUNK
        {
            slots[slot] = Self::EMPTY_VPID;
        }
        self.live[idx] -= 1;
        if self.live[idx] == 0 {
            let f = pid.file_id() as usize;
            self.block_active[f] = self.block_active[f].saturating_sub(1);
            self.live_chunk_count = self.live_chunk_count.saturating_sub(1);
            self.vpid_slots.remove(&idx); // 无活页 → 索引不再需要
        }
    }

    /// ⭐ 重启反推: 遍历全量平坦 meta, 按 pid 聚合重建 live count.
    /// free_chunks 不在这里填 (由 caller 结合 pid 水位判断哪些 chunk 曾分配).
    pub fn rebuild_from_meta(&mut self, meta: &MetaCache) {
        self.live.clear();
        self.block_active.clear();
        self.live_chunk_count = 0;
        self.vpid_slots.clear();
        self.free_chunks.clear();
        self.free_set.clear();
        self.pending_free.clear();
        self.pending_set.clear();
        for vpid in 0..meta.len() as u64 {
            if let Some(pid) = meta.peek(vpid) {
                self.on_page_alloc(pid, vpid);
            }
        }
    }

    /// ⭐ victim 选择: 全扫挑 live 最小的两个 chunk (live > 0),
    /// 满足 live_a + live_b <= PAGES_PER_CHUNK 且不被 exclude 排除.
    /// 返回 (dst, src): dst = live 较大者 (死槽少, 被填充), src = 较小者 (被搬空).
    pub fn pick_compact_victims(
        &self,
        threshold: u8,
        exclude: &dyn Fn(PageKey) -> bool,
    ) -> Option<(PageKey, PageKey)> {
        let mut best: Option<(usize, u8)> = None; // (idx, live)
        let mut second: Option<(usize, u8)> = None;
        for (idx, &l) in self.live.iter().enumerate() {
            if l == 0 || l >= threshold {
                continue;
            }
            let key = PageKey {
                file_id: (idx / CHUNKS_PER_BLOCK) as u32,
                chunk_idx: (idx % CHUNKS_PER_BLOCK) as u8,
            };
            if exclude(key) {
                continue;
            }
            match best {
                None => best = Some((idx, l)),
                Some((_, bl)) if l < bl => {
                    second = best;
                    best = Some((idx, l));
                }
                _ => match second {
                    None => second = Some((idx, l)),
                    Some((_, sl)) if l < sl => second = Some((idx, l)),
                    _ => {}
                },
            }
        }
        let (a_idx, a_live) = best?;
        let (b_idx, b_live) = second?;
        if (a_live as usize) + (b_live as usize) > PAGES_PER_CHUNK {
            return None;
        }
        let key_of = |idx: usize| PageKey {
            file_id: (idx / CHUNKS_PER_BLOCK) as u32,
            chunk_idx: (idx % CHUNKS_PER_BLOCK) as u8,
        };
        // dst = live 较大者 (b), src = live 较小者 (a): 搬运字节最少
        Some((key_of(b_idx), key_of(a_idx)))
    }

    /// compact 提交: src chunk 进延迟释放暂存 (等 meta 确认).
    pub fn stage_pending_free(&mut self, key: PageKey) {
        if self.pending_set.insert(key) {
            self.pending_free.push(key);
        }
    }

    /// ⭐ B-drain: 全扫 block_active 选活跃 chunk 数最小的非空 block
    /// (“最小堆”语义, 惰性全扫实现 — 平坦 u16 数组扫描微秒级,
    /// 仅在 chunk compact 完成时调用, 零堆维护成本).
    /// 条件: 0 < active <= threshold (全空 block 走既有 unlink 路径).
    pub fn pick_block_drain_candidate(
        &self,
        threshold: u16,
        exclude_file: &dyn Fn(u32) -> bool,
    ) -> Option<u32> {
        let mut best: Option<(u32, u16)> = None;
        for (f, &active) in self.block_active.iter().enumerate() {
            if active == 0 || active > threshold || exclude_file(f as u32) {
                continue;
            }
            match best {
                None => best = Some((f as u32, active)),
                Some((_, ba)) if active < ba => best = Some((f as u32, active)),
                _ => {}
            }
        }
        best.map(|(f, _)| f)
    }

    /// ⭐ B-drain: 目标 block 内选一个活 chunk 作 src (live 最小优先,
    /// 搬运字节最少). 无可选 (全被排除/全死) → None.
    pub fn pick_src_in_block(
        &self,
        file_id: u32,
        exclude: &dyn Fn(PageKey) -> bool,
    ) -> Option<PageKey> {
        let start = file_id as usize * CHUNKS_PER_BLOCK;
        let end = (start + CHUNKS_PER_BLOCK).min(self.live.len());
        let mut best: Option<(PageKey, u8)> = None;
        for idx in start..end {
            let l = self.live[idx];
            if l == 0 {
                continue;
            }
            let key = PageKey {
                file_id,
                chunk_idx: (idx - start) as u8,
            };
            if exclude(key) {
                continue;
            }
            match best {
                None => best = Some((key, l)),
                Some((_, bl)) if l < bl => best = Some((key, l)),
                _ => {}
            }
        }
        best.map(|(k, _)| k)
    }

    /// ⭐ B-drain: 选能容纳 need 个活页的 dst chunk (死槽充足且自身有活页,
    /// live 最小优先 — 同时推进 dst 自身的密实度). 无宿主 → None
    /// (caller 兑底开 bump 新 chunk).
    pub fn pick_dst_for(&self, need: usize, exclude: &dyn Fn(PageKey) -> bool) -> Option<PageKey> {
        let mut best: Option<(PageKey, u8)> = None;
        for (idx, &l) in self.live.iter().enumerate() {
            if l == 0 || (l as usize) + need > PAGES_PER_CHUNK {
                continue; // 全死/未分配不作宿主 (死槽容量不足也跳过)
            }
            let key = PageKey {
                file_id: (idx / CHUNKS_PER_BLOCK) as u32,
                chunk_idx: (idx % CHUNKS_PER_BLOCK) as u8,
            };
            if exclude(key) {
                continue;
            }
            match best {
                None => best = Some((key, l)),
                Some((_, bl)) if l < bl => best = Some((key, l)),
                _ => {}
            }
        }
        best.map(|(k, _)| k)
    }

    /// ⭐ G4: 收割自然死光的 chunk (live==0 但从未经 compact 释放).
    ///
    /// COW/delete 路径可能把 chunk 减到全死而不经 compact; 这些 chunk
    /// 不收割就永远不进 free list. 返回候选 (caller 负责 exclude 驻留/
    /// active 后 stage). 已在 pending/free 的跳过.
    pub fn collect_dead_chunks(&self, exclude: &dyn Fn(PageKey) -> bool) -> Vec<PageKey> {
        let mut out = Vec::new();
        for (idx, &l) in self.live.iter().enumerate() {
            if l != 0 {
                continue;
            }
            let key = PageKey {
                file_id: (idx / CHUNKS_PER_BLOCK) as u32,
                chunk_idx: (idx % CHUNKS_PER_BLOCK) as u8,
            };
            if exclude(key) || self.pending_set.contains(&key) || self.free_set.contains(&key) {
                continue;
            }
            out.push(key);
        }
        out
    }

    /// ⭐ meta window 全部确认落盘后调用: pending → free (可复用).
    pub fn promote_pending_free(&mut self) {
        for key in self.pending_free.drain(..) {
            self.pending_set.remove(&key);
            if self.free_set.insert(key) {
                self.free_chunks.push(key);
            }
        }
    }

    /// ⭐ meta log: 只 promote 前 n 个 pending_free.
    ///
    /// 语义: 这 n 个 chunk 的释放所依赖的映射已被"本批日志 append"覆盖
    /// (调用方保证), 因此可以立即复用; 其余留在 pending 等下一批.
    pub fn promote_pending_free_prefix(&mut self, n: usize) {
        let n = n.min(self.pending_free.len());
        for key in self.pending_free.drain(..n) {
            self.pending_set.remove(&key);
            if self.free_set.insert(key) {
                self.free_chunks.push(key);
            }
        }
    }

    /// 延迟释放队列长度 (诊断/回收 tick 用).
    pub fn pending_free_len(&self) -> usize {
        self.pending_free.len()
    }

    /// 取一个可复用 chunk (G3: pid_alloc rotate 时优先用).
    pub fn pop_free_chunk(&mut self) -> Option<PageKey> {
        let key = self.free_chunks.pop()?;
        self.free_set.remove(&key);
        Some(key)
    }

    /// G4: 移除并返回属于 file_id 的全部 free chunk (block unlink 前调用).
    pub fn drain_free_chunks_of_file(&mut self, file_id: u32) -> Vec<PageKey> {
        let (of_file, rest): (Vec<_>, Vec<_>) = self
            .free_chunks
            .drain(..)
            .partition(|k| k.file_id == file_id);
        for k in &of_file {
            self.free_set.remove(k);
        }
        self.free_chunks = rest;
        of_file
    }

    /// G4: free_chunks 中出现过的 file 集合 (block 回收候选).
    pub fn free_files(&self) -> Vec<u32> {
        let mut files: Vec<u32> = self.free_chunks.iter().map(|k| k.file_id).collect();
        files.sort_unstable();
        files.dedup();
        files
    }

    /// block 内已无活跃 chunk (可回收判定的必要条件).
    pub fn block_fully_free(&self, file_id: u32) -> bool {
        self.block_active
            .get(file_id as usize)
            .map(|&n| n == 0)
            .unwrap_or(false)
    }

    /// G4: block unlink 后清位 (该 file 的 live 槽清零).
    pub fn forget_block(&mut self, file_id: u32) {
        let start = file_id as usize * CHUNKS_PER_BLOCK;
        for idx in start..(start + CHUNKS_PER_BLOCK).min(self.live.len()) {
            self.live[idx] = 0;
            self.vpid_slots.remove(&idx);
        }
        if let Some(a) = self.block_active.get_mut(file_id as usize) {
            *a = 0;
        }
    }

    /// 指定 chunk 的 vpid 槽索引 (槽 → vpid; EMPTY_VPID = 空). 越界 → None.
    pub fn vpid_slots(&self, key: PageKey) -> Option<&[u64; PAGES_PER_CHUNK]> {
        self.vpid_slots
            .get(&chunk_index(key.file_id, key.chunk_idx))
    }

    /// 活跃 chunk 数 (live > 0), O(1).
    pub fn live_chunks(&self) -> usize {
        self.live_chunk_count
    }

    /// 曾分配过的 chunk 数 (水位; 含 free/pending/部分空).
    pub fn allocated_chunks(&self) -> usize {
        self.live.len()
    }

    /// 查询: 指定 chunk 的活页数 (越界 = 0).
    pub fn live_pages(&self, key: PageKey) -> u8 {
        self.live
            .get(chunk_index(key.file_id, key.chunk_idx))
            .copied()
            .unwrap_or(0)
    }

    /// 查询: free chunk 数 (测试/观测).
    pub fn free_count(&self) -> usize {
        self.free_chunks.len()
    }

    /// 查询: pending 数 (drain 判空用).
    pub fn pending_free_count(&self) -> usize {
        self.pending_free.len()
    }

    fn ensure_capacity(&mut self, n: usize) {
        if n > self.live.len() {
            self.live.resize(n, 0);
        }
        let blocks = self.live.len().div_ceil(CHUNKS_PER_BLOCK);
        if blocks > self.block_active.len() {
            self.block_active.resize(blocks, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PID_ALIVE;

    fn pid(file_id: u32, chunk_idx: u8, page_idx: u16) -> PidLocation {
        PidLocation {
            file_id,
            chunk_idx,
            page_idx,
            flags: PID_ALIVE,
        }
    }

    #[test]
    fn alloc_dead_roundtrip() {
        let mut lv = ChunkLiveness::new();
        lv.on_page_alloc(pid(0, 0, 0), 1);
        lv.on_page_alloc(pid(0, 0, 1), 1);
        lv.on_page_alloc(pid(0, 1, 0), 1);
        assert_eq!(
            lv.live_pages(PageKey {
                file_id: 0,
                chunk_idx: 0
            }),
            2
        );
        assert_eq!(
            lv.live_pages(PageKey {
                file_id: 0,
                chunk_idx: 1
            }),
            1
        );
        assert!(!lv.block_fully_free(0));
        assert_eq!(lv.live_chunks(), 2, "chunk (0,0)/(0,1) 活跃");
        assert_eq!(lv.allocated_chunks(), 2);

        lv.on_page_dead(pid(0, 0, 0));
        lv.on_page_dead(pid(0, 0, 1));
        lv.on_page_dead(pid(0, 1, 0));
        assert_eq!(
            lv.live_pages(PageKey {
                file_id: 0,
                chunk_idx: 0
            }),
            0
        );
        assert!(lv.block_fully_free(0), "全部死光后 block 无活跃 chunk");
        assert_eq!(lv.live_chunks(), 0);
    }

    /// per-chunk vpid 槽索引: alloc 写入、dead 清空、forget_block 全清.
    #[test]
    fn vpid_slots_track_alloc_and_dead() {
        let mut lv = ChunkLiveness::new();
        let key = PageKey {
            file_id: 0,
            chunk_idx: 1,
        };
        lv.on_page_alloc(pid(0, 1, 3), 42);
        assert_eq!(lv.vpid_slots(key).expect("chunk indexed")[3], 42);
        assert_eq!(lv.vpid_slots(key).unwrap()[0], ChunkLiveness::EMPTY_VPID);
        lv.on_page_dead(pid(0, 1, 3));
        assert!(lv.vpid_slots(key).is_none(), "活页归零移除索引");
        lv.on_page_alloc(pid(0, 1, 0), 7);
        assert_eq!(lv.vpid_slots(key).unwrap()[0], 7);
        lv.forget_block(0);
        assert!(lv.vpid_slots(key).is_none(), "forget_block 移除索引");
    }

    #[test]
    fn pick_victims_smallest_two_with_cap() {
        let mut lv = ChunkLiveness::new();
        // chunk (0,0): 2 活页; (0,1): 5 活页; (0,2): 40 活页 (超阈值)
        for i in 0..2 {
            lv.on_page_alloc(pid(0, 0, i), 1);
        }
        for i in 0..5 {
            lv.on_page_alloc(pid(0, 1, i), 1);
        }
        for i in 0..40 {
            lv.on_page_alloc(pid(0, 2, i), 1);
        }
        let (dst, src) = lv.pick_compact_victims(32, &|_| false).expect("victims");
        // src = live 最小 (0,0), dst = 次小 (0,1)
        assert_eq!(
            src,
            PageKey {
                file_id: 0,
                chunk_idx: 0
            }
        );
        assert_eq!(
            dst,
            PageKey {
                file_id: 0,
                chunk_idx: 1
            }
        );
    }

    #[test]
    fn pick_victims_respects_exclude_and_sum_cap() {
        let mut lv = ChunkLiveness::new();
        for i in 0..30 {
            lv.on_page_alloc(pid(0, 0, i), 1);
        }
        for i in 0..31 {
            lv.on_page_alloc(pid(0, 1, i), 1);
        }
        // 30 + 31 <= 64: 可 compact
        assert!(lv.pick_compact_victims(32, &|_| false).is_some());
        // 排除其一 → 只剩一个候选 → None
        assert!(
            lv.pick_compact_victims(32, &|k| k.chunk_idx == 0).is_none(),
            "单候选不触发"
        );

        // 再加活页让 sum 超 64 → None
        let mut lv2 = ChunkLiveness::new();
        for i in 0..33 {
            lv2.on_page_alloc(pid(0, 0, i), 1);
        }
        for i in 0..33 {
            lv2.on_page_alloc(pid(0, 1, i), 1);
        }
        assert!(
            lv2.pick_compact_victims(64, &|_| false).is_none(),
            "33+33 > 64 不可合并"
        );
    }

    #[test]
    fn block_drain_pickers() {
        let mut lv = ChunkLiveness::new();
        // file0: 2 个活 chunk; file1: 1 个活 chunk (更少 → 候选)
        for i in 0..10 {
            lv.on_page_alloc(pid(0, 0, i), 1);
        }
        for i in 0..5 {
            lv.on_page_alloc(pid(0, 3, i), 1);
        }
        for i in 0..40 {
            lv.on_page_alloc(pid(1, 2, i), 1);
        }

        // 候选 = file1 (active=1 < file0 的 2)
        assert_eq!(lv.pick_block_drain_candidate(3, &|_| false), Some(1));
        // 排除 file1 → file0
        assert_eq!(lv.pick_block_drain_candidate(3, &|f| f == 1), Some(0));
        // 阈值 1 时 file0 (active=2) 不入选
        assert_eq!(lv.pick_block_drain_candidate(1, &|f| f == 1), None);

        // block 内 src: file0 选 live 最小的 chunk3 (5 < 10)
        assert_eq!(
            lv.pick_src_in_block(0, &|_| false),
            Some(PageKey {
                file_id: 0,
                chunk_idx: 3
            })
        );
        // dst 容量: need=40 → 需 live <= 24 的宿主 → chunk(0,3) live=5 最小
        assert_eq!(
            lv.pick_dst_for(40, &|_| false),
            Some(PageKey {
                file_id: 0,
                chunk_idx: 3
            })
        );
        // need=60 → 无宿主 (5+60 / 10+60 / 40+60 均 > 64)
        assert_eq!(lv.pick_dst_for(60, &|_| false), None);
    }

    #[test]
    fn pending_free_promote_flow() {
        let mut lv = ChunkLiveness::new();
        let key = PageKey {
            file_id: 0,
            chunk_idx: 3,
        };
        lv.stage_pending_free(key);
        lv.stage_pending_free(key); // 幂等
        assert_eq!(lv.pending_free_count(), 1);
        assert_eq!(lv.free_count(), 0, "未确认前不可复用");

        lv.promote_pending_free();
        assert_eq!(lv.pending_free_count(), 0);
        assert_eq!(lv.free_count(), 1);
        assert_eq!(lv.pop_free_chunk(), Some(key));
    }

    #[test]
    fn drain_free_of_file_and_forget_block() {
        let mut lv = ChunkLiveness::new();
        lv.on_page_alloc(pid(1, 0, 0), 1);
        lv.stage_pending_free(PageKey {
            file_id: 0,
            chunk_idx: 0,
        });
        lv.stage_pending_free(PageKey {
            file_id: 0,
            chunk_idx: 1,
        });
        lv.stage_pending_free(PageKey {
            file_id: 1,
            chunk_idx: 2,
        });
        lv.promote_pending_free();

        let of0 = lv.drain_free_chunks_of_file(0);
        assert_eq!(of0.len(), 2);
        assert_eq!(lv.free_count(), 1, "file 1 的 free 保留");

        lv.on_page_dead(pid(1, 0, 0));
        assert!(lv.block_fully_free(1));
        lv.forget_block(1);
        assert_eq!(
            lv.live_pages(PageKey {
                file_id: 1,
                chunk_idx: 0
            }),
            0
        );
    }

    /// 成员索引与 Vec 保持一致: 去重/前缀 promote/pop/drain 后不漂移.
    #[test]
    fn membership_sets_stay_consistent() {
        let mut lv = ChunkLiveness::new();
        let a = PageKey {
            file_id: 0,
            chunk_idx: 3,
        };
        let b = PageKey {
            file_id: 0,
            chunk_idx: 4,
        };
        // 让两个 chunk 出现在 live 数组里 (collect_dead_chunks 会遍历)
        lv.on_page_alloc(pid(0, 3, 0), 1);
        lv.on_page_dead(pid(0, 3, 0));
        lv.on_page_alloc(pid(0, 4, 0), 1);
        lv.on_page_dead(pid(0, 4, 0));

        lv.stage_pending_free(a);
        lv.stage_pending_free(a);
        lv.stage_pending_free(b);
        assert_eq!(lv.pending_free_len(), 2, "去重");
        assert!(
            !lv.collect_dead_chunks(&|_| false).contains(&a),
            "pending 成员被排除"
        );

        lv.promote_pending_free_prefix(1);
        assert_eq!(lv.pending_free_len(), 1);
        assert_eq!(lv.free_count(), 1);

        lv.promote_pending_free();
        assert_eq!(lv.free_count(), 2);
        assert!(
            !lv.collect_dead_chunks(&|_| false).contains(&a),
            "free 成员被排除"
        );

        let popped = lv.pop_free_chunk().expect("free chunk");
        assert_eq!(lv.free_count(), 1);
        assert!(
            lv.collect_dead_chunks(&|_| false).contains(&popped),
            "pop 后成员索引已清除"
        );

        let drained = lv.drain_free_chunks_of_file(0);
        assert_eq!(drained.len(), 1);
        assert_eq!(lv.free_count(), 0);
        assert!(
            lv.collect_dead_chunks(&|_| false).contains(&a),
            "drain 后成员索引已清除"
        );
    }
}
