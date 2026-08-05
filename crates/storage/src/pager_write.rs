//! Pager 批量写批 (拆自 pager.rs).
//!
//! `PageWriteBatch`: 一次提交多 page 到 nowchunks, 返回 vpid→pid mappings.
//! `submit` 内完成 pid 分配 (复用/COW)、chunk 驻留兜底、meta_cache 写回与自动换盘.

use std::io;

use crate::meta_page::{META_PID, META_VPID};
use crate::page_pool;
use crate::pager::{next_bump_chunk, Pager};
use crate::types::{CHUNK_SIZE, PAGE_SIZE, PageKey, PidLocation};

pub struct PageWriteBatch {
    /// 待提交 (vpid, page_data)
    pages: Vec<(u64, Box<[u8; PAGE_SIZE]>)>,
}

/// PageWriteBatch 上限 16 page (256KB). 超过触发 panic (DESIGN §3.0.5).
pub const MAX_BATCH_PAGES: usize = 16;

impl PageWriteBatch {
    pub fn new() -> Self {
        Self { pages: Vec::new() }
    }

    /// 加一个 page 进 batch.
    pub fn add(&mut self, vpid: u64, data: Box<[u8; PAGE_SIZE]>) -> &mut Self {
        assert!(
            self.pages.len() < MAX_BATCH_PAGES,
            "PageWriteBatch 超过 16 page 上限 (256KB), 应 caller 分批"
        );
        self.pages.push((vpid, data));
        self
    }

    /// 当前 batch 中的 page 数.
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// ⭐ 提交: 把所有 page 一次性 memcpy 到 nowchunks + 分配 pid + 写回 meta_cache.
    ///
    /// **流程** (单线程连续, 不 await):
    /// 1. 遍历 pages, 调 `pid_alloc.alloc()` 拿 pid
    /// 2. chunk 满 (== 64 page) → rotate_to 下一 chunk (或新 file)
    /// 3. memcpy 到 nowchunks
    /// 4. meta_cache.write(vpid, pid) 标 dirty
    /// 5. 返回 mappings 给 caller (供 debug / 验证)
    ///
    /// **⭐ T12.14 特殊: META_VPID 走 META_PID 直接覆盖 (不 COW)**.
    /// MetaPage 设计上是固定位置 (chunk 0 page 0), 必须始终在 META_PID.
    /// 走通用 pid_alloc 会把 MetaPage COW 到一个新 page, 破坏固定位置约定
    /// (写盘后 disk page 0 仍是旧 MetaPage, 实际 MetaPage 在另一个 page).
    pub async fn submit(self, pager: &mut Pager) -> io::Result<Vec<(u64, PidLocation)>> {
        let mut mappings = Vec::with_capacity(self.pages.len());
        for (vpid, data) in self.pages {
            // 1. 分配 pid
            //    ⭐ META_VPID 例外: 始终用 META_PID (MetaPage 必须在固定位置)
            //
            //    ⭐⭐⭐ 关键设计: **复用 vs COW** (2026-07-26 改为直接问 nowchunks):
            //    - vpid 旧 pid 对应的 chunk **仍在 nowchunks** → **复用原 pid**
            //      原位覆盖同一 page_idx (增量写, 不浪费槽位)
            //    - 否则 (新 vpid / chunk 已 swap/flush 走) → **alloc 新 pid** 走纯 COW
            //
            //    **语义修正**: 旧代码用 meta.is_dirty(vpid) 判定, 但 swap 后 meta 仍
            //    dirty 而 chunk 已不在内存 (旧版靠 reinsert_clean 兜底)。去掉 reinsert
            //    后 dirty 不再等价"chunk 驻留", 必须直接问 nowchunks (meta 是 SoT)。
            let pid = if vpid == META_VPID {
                // ⭐ G1 liveness: MetaPage 首写计一次活 (固定位置覆盖不重复计)
                if pager.meta.read(META_VPID).is_none() {
                    pager.liveness.on_page_alloc(META_PID);
                }
                META_PID
            } else if let Some(old_pid) = pager.meta.read(vpid)
                && pager.nowchunks.peek_chunk(PageKey {
                    file_id: old_pid.file_id(),
                    chunk_idx: old_pid.chunk_idx(),
                }).is_some()
            {
                // 复用原 pid (chunk 还在内存, 原位覆盖同一 page_idx)
                old_pid
            } else {
                // COW: alloc 新 pid
                let old_pid_opt = pager.meta.read(vpid);
                let new_pid = loop {
                    if let Some(p) = pager.pid_alloc.alloc() {
                        break p;
                    }
                    // chunk 满: rotate. ⭐ G3: 优先复用 free chunk (compact
                    // 释放 + meta 已确认), 无 free 才 bump 高水位.
                    // 复用 chunk 预先插入空视图 (磁盘历史内容全死,
                    // 驻留兜底不得加载旧死页).
                    let (next_file, next_chunk) =
                        if let Some(free) = pager.liveness.pop_free_chunk() {
                            pager.nowchunks.insert_empty(free);
                            pager.on_reused_chunk = true;
                            (free.file_id, free.chunk_idx)
                        } else {
                            let t = pager.pid_bump_next;
                            pager.pid_bump_next = next_bump_chunk(t.0, t.1);
                            pager.on_reused_chunk = false;
                            t
                        };
                    pager.pid_alloc.rotate_to(next_file, next_chunk);
                };
                // ⭐ G1 liveness: COW 路径 — 旧页死 (若存在), 新页活.
                // 复用分支 pid 不变不计; 读路径不影响活性.
                if let Some(old) = old_pid_opt {
                    pager.liveness.on_page_dead(old);
                }
                pager.liveness.on_page_alloc(new_pid);
                new_pid
            };

            // 2. memcpy 到 nowchunks (user data 完整保留, 不构造 header)
            let key = PageKey {
                file_id: pid.file_id(),
                chunk_idx: pid.chunk_idx(),
            };
            let page_idx = pid.page_idx() as u8;
            // ⭐ 驻留兜底: chunk 不在 nowchunks 时加载完整视图, 避免全 0
            //    ChunkBuf 覆盖历史 page. 触发: reopen 首写 / 固定位置页 (MetaPage)
            //    所在 chunk 被 swap 后再写 / COW rotate 首写新 chunk.
            //
            //    ⭐ 加载源优先级必须与读路径一致 (最新优先):
            //    write_queue pending → completed → in_flight → chunk_list → disk.
            //    否则 swap 后立即重建会从 disk 读到 stale 视图, 而读路径
            //    nowchunks 优先 → 读到坏页 (catalog_many_dbs 回归抓到).
            if pager.nowchunks.peek_chunk(key).is_none() {
                let ck: crate::chunk_lru::ChunkKey = key.into();
                let bytes = if let Some(b) = pager.write_queue.peek_chunk_pending(key) {
                    b.to_vec()
                } else if let Some(b) = pager.write_queue.peek_chunk_completed(key) {
                    b.to_vec()
                } else if let Some(b) = pager.in_flight.get(&key) {
                    (**b).clone()
                } else if pager.chunk_list.contains(&ck) {
                    pager.chunk_list.peek(&ck).unwrap().to_vec()
                } else {
                    match pager.io.read_page_chunk(&pager.block_dir, key).await {
                        Ok(b) => b,
                        Err(_) => vec![0u8; CHUNK_SIZE],
                    }
                };
                pager.nowchunks.load_full_view(key, bytes);
            }
            pager
                .nowchunks
                .write_page_with_vpid(key, page_idx, vpid, &data);
            // ⭐ 热路径优化: page 字节已 memcpy 进 nowchunks, Box 归还页池
            // (闭合 page_pool 循环: read alloc → submit 消费 → recycle).
            page_pool::recycle(data);
            // 3. 写回 meta_cache (标 dirty, 持久化在 flush 时).
            pager.meta.write(vpid, pid);

            // 4. ⭐ 自动持久化: 检查 chunk 是否已满, 满则 swap 到 WriteQueue
            //    (背压超限时内部退化同步落盘)
            if pager.nowchunks.is_chunk_full(key) {
                pager.swap_full_chunk_to_write_queue(key).await?;
            }

            // 5. 收集 mapping
            mappings.push((vpid, pid));
        }

        // ⭐ 更新写计数
        pager.inc_write_count(mappings.len() as u64);

        Ok(mappings)
    }
}

impl Default for PageWriteBatch {
    fn default() -> Self {
        Self::new()
    }
}
