//! Pager 异步落盘 + compact 后台方法 (拆自 pager.rs).
//!
//! flush 组: 收割 WriteQueue / meta window 异步落盘作业 (`FlushBatch`/`MetaFlushBatch`),
//! compact 组: chunk 垃圾回收 (`start_compact` → `analyze_compact_read` → `complete_compact`),
//! 以及 block 排空 (`request_block_drain`).

use std::io;
use std::rc::Rc;

use crate::pager::{
    count_valid_pages, gc_debug, next_bump_chunk, parse_page_vpid, CompactReadJob,
    CompactWriteJob, FlushBatch, MetaFlushBatch, Pager, BLOCK_DRAIN_ACTIVE_THRESHOLD,
    COMPACT_LIVE_THRESHOLD, COMPACT_MIN_INTERVAL_MS,
};
use crate::types::{CHUNK_SIZE, PAGES_PER_CHUNK, PAGE_SIZE, PidLocation, PageKey};

impl Pager {
    pub fn take_flush_batches(&mut self) -> Vec<FlushBatch> {
        let pending_keys = self.write_queue.pending_keys();
        // 按 file_id 分组 (保持 key 顺序; 单 shard 通常只有 1 个 file)
        type FileGroup = (u32, Vec<(PageKey, Rc<Vec<u8>>)>);
        let mut batches: Vec<FileGroup> = Vec::new();
        for key in pending_keys {
            if self.in_flight.contains_key(&key) {
                continue; // 同 key 在写, 新快照等下轮
            }
            if let Some(bytes) = self.write_queue.take_pending(key) {
                let rc = Rc::new(bytes);
                self.in_flight.insert(key, rc.clone());
                match batches.iter_mut().find(|(fid, _)| *fid == key.file_id) {
                    Some((_, items)) => items.push((key, rc)),
                    None => batches.push((key.file_id, vec![(key, rc)])),
                }
            }
        }
        batches
            .into_iter()
            .map(|(_, items)| FlushBatch {
                items,
                dir: self.block_dir.clone(),
                io: self.io.clone(),
            })
            .collect()
    }

    /// ⭐ 异步落盘: 协程完成后的收割回调 (shard 主循环调用).
    ///
    /// - 成功: 移出 in-flight, 字节迁入 chunk_list;
    ///   若 in-flight 归零且无 pending → 置位 meta_flush_due (下轮异步刷 meta,
    ///   data→meta 顺序保证; 不再在收割路径同步 fsync page.mate)
    /// - 失败: 重新入 pending 下轮重试, meta 不前进
    pub fn complete_flush(&mut self, key: PageKey, result: io::Result<()>) -> io::Result<()> {
        let Some(rc) = self.in_flight.remove(&key) else {
            return Ok(()); // 防御: 未知 key
        };
        match result {
            Ok(()) => {
                let bytes = Rc::try_unwrap(rc).unwrap_or_else(|rc| (*rc).clone());
                // ⭐ 修复 (2026-08-02): 若同 key 有更新版本在 pending 或 in_flight 中,
                // 说明本次落盘的快照已过时. 不插入 chunk_list, 避免旧快照遮蔽
                // 读路径 (nowchunks miss → pending miss → chunk_list 命中旧数据).
                // 场景: periodic_flush 快照 A(1页) 先写盘完成, 但 swap 的 B(64页)
                // 已被 take_flush_batches 取走到 in_flight; 此时 pending 为空但
                // in_flight 有 B, 必须也跳过 A 的插入.
                if self.write_queue.peek_chunk_pending(key).is_some()
                    || self.in_flight.contains_key(&key)
                {
                    // 过时快照: 丢弃 (不写 chunk_list)
                } else {
                    // ⭐ 修复 (2026-08-02, bad page type 根因):
                    // 插入前比较有效页数——仅当新数据的有效页数 >= chunk_list
                    // 中已有版本时才替换. 防止 maybe_periodic_flush 产生的
                    // 中间快照 (4/14/59 页) 覆盖 swap 的完整快照 (64 页).
                    let new_valid = count_valid_pages(&bytes);
                    let should_insert = if let Some(existing) = self.chunk_list.peek(&key.into()) {
                        let old_valid = count_valid_pages(&existing);
                        new_valid >= old_valid
                    } else {
                        true
                    };
                    if should_insert {
                        self.chunk_list.insert_from_write_queue(key, bytes);
                    }
                }
                // 本批全部确认落盘且无待提交快照 → meta 可安全异步刷
                if self.in_flight.is_empty() && self.write_queue.pending_keys().is_empty() {
                    self.meta_flush_due = true;
                }
            }
            Err(e) => {
                let bytes = Rc::try_unwrap(rc).unwrap_or_else(|rc| (*rc).clone());
                self.write_queue
                    .enqueue(crate::chunk_writer::WriteHandle::new(key, bytes));
                return Err(e);
            }
        }
        Ok(())
    }

    /// 异步落盘: 是否有写盘中的 chunk.
    pub fn has_inflight(&self) -> bool {
        !self.in_flight.is_empty()
    }

    /// 异步落盘: 写盘中 + 待提交的 chunk 总数 (测试/背压观察).
    /// 注意: 不含 meta backlog (背压阈值只看 data chunk).
    pub fn flush_backlog(&self) -> usize {
        self.in_flight.len() + self.write_queue.pending_keys().len()
    }

    /// ⭐ Phase M3: 含 meta 的全部异步 backlog (drain/close 判空用).
    /// data in-flight/pending + meta due/dirty/in-flight + compact 在飞
    /// 全部排空才算真正空闲.
    pub fn total_async_backlog(&self) -> usize {
        self.flush_backlog()
            + self.meta.in_flight_window_count()
            + (self.meta_flush_due && self.meta.has_unflushed()) as usize
            + self.compact_inflight as usize
    }

    /// ⭐ Phase M3: 取 meta window 异步刷盘批.
    ///
    /// **保序关键**: 仅在 meta_flush_due 且 data backlog 为空时取快照
    /// (有 in-flight data chunk 时绝不发 meta, data→meta 顺序不变);
    /// 同 window 有在飞快照时由 MetaCache 去重 (防同 offset 乱序).
    pub fn take_meta_flush_batch(&mut self) -> Option<MetaFlushBatch> {
        if !self.meta_flush_due || self.flush_backlog() > 0 {
            return None;
        }
        self.meta_flush_due = false;
        let windows = self.meta.take_dirty_window_snapshots();
        // 有 dirty 因 in-flight 去重被跳过 → 保持 due, 收割后下轮再取
        if self.meta.dirty_count() > 0 {
            self.meta_flush_due = true;
        }
        if windows.is_empty() {
            // ⭐ G2/G4: 无脏 window 且无在飞 → meta 已是持久状态
            // (同步 flush 路径已刷过): 确认点语义同样成立,
            // 延迟释放可直接 promote + 回收全空 block.
            if !self.meta.has_unflushed() {
                self.liveness.promote_pending_free();
                self.maybe_drop_free_blocks();
            }
            return None; // 不发空批
        }
        Some(MetaFlushBatch {
            windows,
            mate_path: self.block_dir.join("page.mate"),
            io: self.io.clone(),
        })
    }

    /// ⭐ WAL (F60): meta 无未刷窗口 (= 上轮刷盘已全部持久化,
    /// sealed WAL 段可安全删除).
    pub fn meta_all_flushed(&self) -> bool {
        !self.meta.has_unflushed()
    }

    /// ⭐ Phase M3: 收割 meta window 写盘结果.
    ///
    /// 全部 window 确认后才 persist_pid_state (data→meta→pid.state 顺序闭环);
    /// 失败重标 dirty + 重新置位 due (下轮重试).
    pub fn complete_meta_flush(&mut self, window_idx: u32, result: io::Result<()>) {
        let ok = result.is_ok();
        self.meta.complete_window_flush(window_idx, ok);
        if !ok {
            self.meta_flush_due = true;
            return;
        }
        if !self.meta.has_unflushed() {
            self.persist_pid_state();
            // ⭐ G2: meta 已全部持久 → compact 延迟释放的 src chunk 可复用
            self.liveness.promote_pending_free();
            // ⭐ G4: 全空 block 回收 (unlink, 低频)
            self.maybe_drop_free_blocks();
        }
    }

    /// ⭐ G4: 回收全空 block 文件.
    ///
    /// 候选 = free_chunks 中出现过的 file 且 block_active == 0 (无任何活页,
    /// 隐含无 nowchunks/pending/in_flight 项 — 驻留页在 alloc 时已计活);
    /// 排除当前写入 file 与 bump 预定 file. 顺序: 清 free list → 逐出 fd
    /// (fd_cache + FdPool 固定槽) → unlink → 清活性位.
    /// unlink 失败仅 warn (下轮重试); META block (file 0) 因 MetaPage 永活天然不会全空.
    fn maybe_drop_free_blocks(&mut self) {
        let (cur_file, _, _) = self.pid_alloc.current();
        for file_id in self.liveness.free_files() {
            if file_id == cur_file || file_id == self.pid_bump_next.0 {
                continue;
            }
            if !self.liveness.block_fully_free(file_id) {
                continue;
            }
            let path = self.io.block_path(&self.block_dir, file_id);
            if !path.exists() {
                // 文件不存在 (从未落盘过): 只清内存状态
                self.liveness.drain_free_chunks_of_file(file_id);
                self.liveness.forget_block(file_id);
                continue;
            }
            // 不再复用该 file 的 chunk → 逐出 fd → unlink
            self.liveness.drain_free_chunks_of_file(file_id);
            self.io.evict_path(&path);
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    self.liveness.forget_block(file_id);
                }
                Err(e) => {
                    eprintln!("[pager] drop block {} failed (retry next round): {e}", path.display());
                }
            }
        }
    }

    // =================================================================
    // ⭐ G2: chunk compact — 死槽填充 + CAS 提交 (两阶段协程)
    // =================================================================

    /// ⭐ G2 阶段 1 (同步): 选 victim, 构造读作业 (协程读 src+dst chunk 字节).
    ///
    /// 触发条件由 caller 保证 (data backlog == 0); 同时至多 1 个 compact 在飞.
    /// victim 排除: 当前 active chunk / nowchunks 驻留 / write_queue pending /
    /// data in-flight / MetaPage 固定 chunk (file 0, chunk 0).
    pub fn start_compact(&mut self) -> Option<CompactReadJob> {
        if self.compact_inflight {
            return None;
        }
        // ⭐ G5: 节流 — 后台回收对实时性不敏感, 限频避免与前台抢 IO 带宽
        if self.last_compact_time.elapsed().as_millis() < COMPACT_MIN_INTERVAL_MS as u128 {
            return None;
        }
        self.last_compact_time = std::time::Instant::now();
        let (cur_file, cur_chunk, _) = self.pid_alloc.current();
        let mut excluded: std::collections::HashSet<PageKey> =
            self.write_queue.pending_keys().into_iter().collect();
        excluded.extend(self.in_flight.keys().copied());
        excluded.extend(self.nowchunks.resident_keys());
        excluded.insert(PageKey { file_id: cur_file, chunk_idx: cur_chunk });
        excluded.insert(PageKey { file_id: 0, chunk_idx: 0 }); // META 固定位置

        // ⭐ G4: 顺带收割自然死光的 chunk (COW 减到全死不经 compact 的).
        // 排除未分配区: tuple >= bump 水位的 chunk 从未分配过 (live 数组懒
        // 扩容的 0 项), 误收割会与未来 bump 分配重叠.
        let bump = self.pid_bump_next;
        let dead: Vec<PageKey> = self.liveness.collect_dead_chunks(&|k| {
            excluded.contains(&k) || (k.file_id, k.chunk_idx) >= bump
        });
        if !dead.is_empty() {
            for key in dead {
                self.chunk_list.invalidate(&key.into());
                self.liveness.stage_pending_free(key);
            }
            self.meta_flush_due = true; // 推动 meta 确认 → promote
        }

        // ⭐ B-drain: 排空模式优先 — 目标 block 内逐 chunk 迁出
        // (无视 live 阈值: 为腾空整个 block, 中等活度 chunk 也搬).
        // 每轮只迁一个 chunk (状态机分片), 不长占运行时.
        if let Some(f) = self.drain_block_target {
            if self.liveness.block_fully_free(f) {
                // 目标达成: 全死 block 由确认点的 maybe_drop_free_blocks 回收
                self.drain_block_target = None;
            } else if let Some(src) = self
                .liveness
                .pick_src_in_block(f, &|k| excluded.contains(&k))
            {
                let need = self.liveness.live_pages(src) as usize;
                // dst: 优先全局死槽宿主 (排除目标 block 自身); 无宿主兑底
                // 开 bump 新 chunk (fresh: 磁盘无内容, 64 槽全可用)
                let (dst, dst_fresh) = match self.liveness.pick_dst_for(need, &|k| {
                    excluded.contains(&k) || k.file_id == f
                }) {
                    Some(d) => (d, false),
                    None => {
                        let t = self.pid_bump_next;
                        self.pid_bump_next = next_bump_chunk(t.0, t.1);
                        (PageKey { file_id: t.0, chunk_idx: t.1 }, true)
                    }
                };
                self.compact_inflight = true;
                return Some(CompactReadJob {
                    dst,
                    src,
                    dst_fresh,
                    dir: self.block_dir.clone(),
                    io: self.io.clone(),
                });
            }
            // src 全被排除 (驻留/在飞): 保留 target 下轮再试, 本轮走普通 pick
        }

        let (dst, src) = self
            .liveness
            .pick_compact_victims(COMPACT_LIVE_THRESHOLD, &|k| excluded.contains(&k))?;
        self.compact_inflight = true;
        Some(CompactReadJob {
            dst,
            src,
            dst_fresh: false,
            dir: self.block_dir.clone(),
            io: self.io.clone(),
        })
    }

    /// ⭐ G2 阶段 2 (同步, 收割读完成后): meta 判活, 组装写作业.
    ///
    /// **判活 = header vpid 候选 + meta 点查确认** (O(128) 点查, 零 meta
    /// 全扫 — 全扫在 1TB/512MB meta 规模下每次 compact 扫数十 ms 不可接受):
    /// - src 活页 = header vpid 在 meta 中仍指向该槽
    /// - dst 死槽 = 非活槽 (含无 magic 的从未写槽)
    /// - 无 magic 页 = 未写 (与 recover 扫描同约定)
    /// - src 已全死 → 无需搬运, 直接入延迟释放
    /// - 读失败 / 死槽不足 → 放弃本轮 (无副作用)
    ///
    /// 页字节原样搬运 (header 内只有 vpid 无 pid, 位置变更不需改写).
    pub fn analyze_compact_read(
        &mut self,
        dst: PageKey,
        src: PageKey,
        dst_fresh: bool,
        read_result: io::Result<(Vec<u8>, Vec<u8>)>, // (dst_bytes, src_bytes)
    ) -> Option<CompactWriteJob> {
        let Ok((dst_bytes, src_bytes)) = read_result else {
            self.compact_inflight = false;
            return None; // 读失败: 无副作用, 下轮重试
        };
        if src_bytes.len() != CHUNK_SIZE || dst_bytes.len() != CHUNK_SIZE {
            self.compact_inflight = false;
            return None;
        }
        // ⭐ 判活以 **meta 全扫为 SoT** (修复数据丢失 bug, 2026-07-24):
        // 旧实现用 page header vpid 自描述 (parse_page_vpid + meta 点查),
        // 但 **Internal 页的 header vpid 字段是 first_child** (page crate
        // 约定, 见 chunk_writer::write_page_with_header) —— header 候选法
        // 会把 Internal 页误判为死页:
        // - 作为 src: 不搬运 → chunk 释放复用后 Internal 页物理销毁
        // - 作为 dst: 活 Internal 页被当死槽直接覆盖
        // 两者都导致子树路由断, travel 落到错误 leaf → GET 返回 nil (静默丢数据).
        // meta 平坦数组全扫微秒~毫秒级, compact 低频 (空闲段 + 10ms 节流) 可接受.
        let mut src_live: Vec<(u64, PidLocation, usize)> = Vec::new();
        let mut dst_alive = [false; PAGES_PER_CHUNK];
        for (vpid, pid) in self.meta.iter_allocated() {
            if pid.flags() & crate::types::PID_ALIVE == 0 {
                continue; // 墓碑 (已释放溢出页) 不算活
            }
            let slot = pid.page_idx() as usize;
            if slot >= PAGES_PER_CHUNK {
                continue;
            }
            if pid.file_id() == src.file_id && pid.chunk_idx() == src.chunk_idx {
                src_live.push((vpid, pid, slot));
            } else if pid.file_id() == dst.file_id && pid.chunk_idx() == dst.chunk_idx {
                dst_alive[slot] = true;
            }
        }
        // ⭐ 排查日志 (NLOG_GC_DEBUG=1): header 候选法与 meta SoT 的差异 —
        // 差异槽即旧逻辑会误判/误伤的页 (典型: Internal 页)
        if gc_debug() {
            for (vpid, _, slot) in &src_live {
                let header_says_alive = parse_page_vpid(&src_bytes, *slot)
                    .is_some_and(|hv| hv == *vpid);
                if !header_says_alive {
                    eprintln!(
                        "[GC_DEBUG] src {:?} slot {} vpid {} alive-in-meta but header disagrees (page_type={}) — old logic would DROP it",
                        src, slot, vpid, src_bytes[slot * PAGE_SIZE + 4]
                    );
                }
            }
        }
        if src_live.is_empty() {
            // src 已全死 (并发 COW 减到位): 直接延迟释放, 无需写盘
            self.compact_inflight = false;
            self.chunk_list.invalidate(&src.into());
            self.liveness.stage_pending_free(src);
            self.meta_flush_due = true; // 推动 meta 确认 → promote
            return None;
        }
        // dst 死槽 = meta 无活页指向的槽 (SoT 同上; fresh dst 天然全死槽)
        let mut dead_slots: Vec<u8> = Vec::new();
        for (i, alive) in dst_alive.iter().enumerate() {
            if !alive {
                dead_slots.push(i as u8);
            }
        }
        if dead_slots.len() < src_live.len() {
            // liveness 阈值下不该发生; 防御性放弃
            self.compact_inflight = false;
            return None;
        }
        let mut moves: Vec<(u64, PidLocation, u8)> = Vec::new();
        let mut items: Vec<(u8, Vec<u8>)> = Vec::new();
        for ((vpid, src_pid, src_slot), dst_slot) in src_live.into_iter().zip(dead_slots) {
            let off = src_slot * PAGE_SIZE;
            items.push((dst_slot, src_bytes[off..off + PAGE_SIZE].to_vec()));
            moves.push((vpid, src_pid, dst_slot));
        }
        Some(CompactWriteJob {
            dst,
            src,
            dst_fresh,
            items,
            moves,
            dir: self.block_dir.clone(),
            io: self.io.clone(),
        })
    }

    /// ⭐ G2 阶段 3 (同步, 收割写完成后): CAS 提交.
    ///
    /// 逐 vpid 校验 `meta 仍指向 src_pid` 才改写 (防回滚 IO 期间的并发 COW 写);
    /// miss 跳过 (页已死, live 已由 COW 路径递减). 提交后 src 入延迟释放,
    /// meta window 确认后才可复用 (data→meta 顺序同构).
    pub fn complete_compact(
        &mut self,
        dst: PageKey,
        src: PageKey,
        moves: Vec<(u64, PidLocation, u8)>,
        result: io::Result<()>,
    ) {
        self.compact_inflight = false;
        if result.is_err() {
            return; // A 死槽半写无害 (meta 未指向), 下轮重试
        }
        let mut migrated = 0usize;
        for (vpid, src_pid, dst_slot) in moves {
            // CAS: IO 期间被用户 COW 覆盖的页跳过
            if self.meta.read(vpid) != Some(src_pid) {
                continue;
            }
            let dst_pid = PidLocation {
                file_id: dst.file_id,
                chunk_idx: dst.chunk_idx,
                page_idx: dst_slot as u16,
                flags: src_pid.flags(), // 保留原 flags (不改变页状态语义)
            };
            self.meta.write(vpid, dst_pid);
            self.liveness.on_page_dead(src_pid);
            self.liveness.on_page_alloc(dst_pid);
            migrated += 1;
        }
        // 旧缓存副本失效: dst 缺迁入页, src 即将释放
        self.chunk_list.invalidate(&dst.into());
        self.chunk_list.invalidate(&src.into());
        if self.liveness.live_pages(src) == 0 {
            self.liveness.stage_pending_free(src);
        }
        if migrated > 0 || self.meta.dirty_count() > 0 {
            self.meta_flush_due = true;
        }
        // ⭐ B-drain: chunk compact 完成后验收 — 无在进行目标时,
        // 全扫 block_active (“最小堆”惰性实现) 选活跃度最低的半空 block
        // 进入排空模式 (排除 active/bump/META file).
        if self.drain_block_target.is_none() {
            let (cur_file, _, _) = self.pid_alloc.current();
            let bump_file = self.pid_bump_next.0;
            self.drain_block_target = self.liveness.pick_block_drain_candidate(
                BLOCK_DRAIN_ACTIVE_THRESHOLD,
                &|file| file == cur_file || file == bump_file || file == 0,
            );
        }
    }

    /// G2: 是否有 compact 在飞 (drain/观测用).
    pub fn compact_inflight(&self) -> bool {
        self.compact_inflight
    }

    /// G5: 测试 helper — 重置节流窗口 (让下一次 start_compact 立即可触发).
    pub fn reset_compact_throttle(&mut self) {
        self.last_compact_time = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_millis(COMPACT_MIN_INTERVAL_MS))
            .unwrap_or_else(std::time::Instant::now);
    }

    /// ⭐ B-drain: 显式请求排空指定 block (测试/未来管理命令用;
    /// 自动路径由 complete_compact 尾部的候选选择触发).
    pub fn request_block_drain(&mut self, file_id: u32) {
        self.drain_block_target = Some(file_id);
    }

    /// B-drain: 当前排空目标 (观测用).
    pub fn drain_block_target(&self) -> Option<u32> {
        self.drain_block_target
    }

    /// ⭐ 自动持久化: 驱动 WriteQueue 落盘.
    /// 遍历 pending chunks, 异步写盘, 完成后移入 chunk_list.
    /// ⭐ 关键: 全部 chunk 写完后才 flush meta (保证一致性).
    pub async fn drive_write_queue(&mut self) -> io::Result<()> {
        // 取出所有 pending keys
        let pending_keys: Vec<PageKey> = self.write_queue.pending_keys();
        if pending_keys.is_empty() {
            return Ok(());
        }

        // 先写完所有 chunk data
        for key in &pending_keys {
            if let Some(chunk_bytes) = self.write_queue.peek_chunk_pending(*key) {
                let bytes = chunk_bytes.to_vec();
                self.io.write_page_chunk(&self.block_dir, *key, bytes).await?;
            }
            self.write_queue.mark_completed(*key);
        }

        // drain completed 入 chunk_list
        let completed = self.write_queue.drain_completed();
        for handle in completed {
            self.chunk_list.insert_from_write_queue(handle.key, handle.chunk);
        }

        // ⭐ 全部 chunk data 确认落盘后, 再 flush meta
        self.meta.flush_dirty()?;
        self.persist_pid_state();

        Ok(())
    }

    /// ⭐ 自动持久化: 周期/计数检查.
    /// 如果 writes >= 256 或 elapsed >= 10s, 触发 WAL seal.
    /// 返回 true 表示触发了 flush.
    ///
    /// ⭐ 修复 (2026-08-02, bad page type 根因): 不再将驻留 chunk 快照
    /// 入 write_queue. 原设计意图是周期刷盘保证持久性, 但:
    /// - 读路径优先命中 nowchunks, 快照对读毫无价值
    /// - 快照经 flush 管道后插入 chunk_list, 会与 swap 的完整数据
    ///   产生时序竞态 → 旧快照遮蔽新数据 → 全零页 → bad page type
    /// - 持久性由 WAL (periodic/strict) + swap (满 chunk) + 显式 flush
    ///   (shutdown) 三重保证, 无需 periodic 快照
    pub async fn maybe_periodic_flush(&mut self) -> io::Result<bool> {
        const FLUSH_WRITE_THRESHOLD: u64 = 256;
        const FLUSH_PERIOD_SECS: u64 = 10;

        let should_flush = self.write_count_since_flush >= FLUSH_WRITE_THRESHOLD
            || self.last_flush_time.elapsed().as_secs() >= FLUSH_PERIOD_SECS;

        if !should_flush {
            return Ok(false);
        }

        // 仅重置计数器 + 触发 WAL seal, 不再快照驻留 chunk
        self.write_count_since_flush = 0;
        self.last_flush_time = std::time::Instant::now();
        Ok(true)
    }

}
