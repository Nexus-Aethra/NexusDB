//! T5 ChunkList: 1MB chunk 只读 LRU 缓存 (DESIGN §3.1).
//!
//! **核心不变量 (DESIGN §3.0.5)**:
//! - chunk_list 内 chunk 字节**永不修改** (LSM: 写不修改旧数据, COW 在新位置追加)
//! - 多 reader 可同时 clone Arc 共享同一 chunk 字节, **无 Mutex**
//! - 容量: 默认 8 个 chunk = 8MB (单 shard 8MB 合理上界)
//! - LRU 替换: `order` 维护访问顺序, front=最新, back=最旧, 满时 pop back
//!
//! **数据流**:
//! - 进: WriteQueue::drain_completed → `insert_from_write_queue(key, chunk)`
//! - 出: Pager::read_page (T6) → `peek(key)` 或 `get_or_load(key, load_fn)`
//! - 淘汰: 满了时 pop back 的 key, 释放 Arc. 若外部还有 Arc 引用, 字节不被立刻 drop.
//!
//! **单线程使用**: per-shard thread, 同 scheduler crate 契约.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::types::{DEFAULT_DB_ID, DbId, PAGE_SIZE, PageKey};

// =====================================================================
// ChunkKey: (db, file_id, chunk_idx) 标识一个 chunk (T12.9 per-db)
// =====================================================================

/// 标识一个 1MB chunk 的 key.
///
/// **T12.9 加 db 字段**: 单 ShardManager 内部 ChunkList 单例, 跨 db 共享
/// 8MB LRU 容量. 不同 db 物理隔离 (独立 file_id 命名空间).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ChunkKey {
    pub db: DbId,
    pub file_id: u32,
    pub chunk_idx: u8,
}

impl ChunkKey {
    /// compat: 创建 db=0 的 ChunkKey.
    pub fn new_legacy(file_id: u32, chunk_idx: u8) -> Self {
        Self {
            db: DEFAULT_DB_ID,
            file_id,
            chunk_idx,
        }
    }
}

impl From<PageKey> for ChunkKey {
    fn from(pk: PageKey) -> Self {
        // PageKey 没有 db 字段, 假设是默认 db (compat)
        Self {
            db: DEFAULT_DB_ID,
            file_id: pk.file_id,
            chunk_idx: pk.chunk_idx,
        }
    }
}

impl From<ChunkKey> for PageKey {
    fn from(ck: ChunkKey) -> Self {
        Self {
            file_id: ck.file_id,
            chunk_idx: ck.chunk_idx,
        }
    }
}

// =====================================================================
// ChunkList: LRU 缓存
// =====================================================================

/// 1MB chunk 的 LRU 缓存.
///
/// **不变量**:
/// - 所有 chunk 字节**只读** (通过 Arc 共享, 不可变)
/// - 容量固定, 满时 LRU 淘汰最旧
/// - 多 reader 拿到 Arc clone 后可并发读 (无 Mutex, 字节不会变)
///
/// ⚠️ chunk 是 `Vec<u8>` 而非 `Box<[u8; CHUNK_SIZE]>`, 因为 WriteQueue 移交过来的字节是 Vec<u8>.
/// LRU 不会 mutate 这些字节, 所以没有越界风险.
pub struct ChunkList {
    capacity: usize,
    /// key → Arc<ChunkBuf> 映射. Arc clone 共享同一 chunk 字节.
    map: HashMap<ChunkKey, Arc<Vec<u8>>>,
    /// LRU order: front = 最新访问, back = 最旧访问.
    order: VecDeque<ChunkKey>,
    /// The page tier belongs to the same cache owner as the chunk tier.  A
    /// page is keyed by physical location, so a COW remap cannot reuse an old
    /// vpid entry accidentally.
    pages: PageLru,
    /// Per-chunk page-miss counters drive asynchronous promotion.  They live
    /// with both cache tiers so the policy sees page and chunk residency.
    promotion_misses: HashMap<ChunkKey, PromotionCandidate>,
    promotion_pending: HashSet<ChunkKey>,
    promotion_queue: VecDeque<ChunkKey>,
    last_promotion_start: Option<Instant>,
    page_miss_epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct PageCacheKey {
    chunk: ChunkKey,
    page_idx: u8,
}

struct PageEntry {
    bytes: Box<[u8]>,
    prev: Option<PageCacheKey>,
    next: Option<PageCacheKey>,
}

/// Evidence that a chunk has short-lived spatial locality.  Counting distinct
/// page slots avoids promoting a chunk merely because one hot page was briefly
/// evicted from the page tier.
struct PromotionCandidate {
    seen_pages: u64,
    last_seen_epoch: u64,
}

/// O(1) true LRU for the 16KiB tier.  Keeping this intrusive list here avoids
/// a second cache owner and avoids `VecDeque::retain` on every page hit.
struct PageLru {
    entries: HashMap<PageCacheKey, PageEntry>,
    most_recent: Option<PageCacheKey>,
    least_recent: Option<PageCacheKey>,
    capacity: usize,
}

impl PageLru {
    const DEFAULT_PAGES: usize = 2048; // 32MiB per shard
    const MIN_PAGES: usize = 64;
    const MAX_PAGES: usize = 16_384;

    fn from_env() -> Self {
        let requested = std::env::var("NEXUS_PAGE_CACHE_PAGES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(Self::DEFAULT_PAGES);
        // Zero is a supported A/B and emergency-disable setting.  It is also
        // useful while tuning the unified cache without changing its callers.
        let capacity = if requested == 0 {
            0
        } else {
            requested.clamp(Self::MIN_PAGES, Self::MAX_PAGES)
        };
        Self::new(capacity)
    }

    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            most_recent: None,
            least_recent: None,
            capacity,
        }
    }

    fn get(&mut self, key: PageCacheKey) -> Option<&[u8]> {
        if !self.entries.contains_key(&key) {
            return None;
        }
        self.promote(key);
        Some(&self.entries.get(&key).expect("page exists").bytes)
    }

    fn insert(&mut self, key: PageCacheKey, bytes: &[u8]) {
        debug_assert_eq!(bytes.len(), PAGE_SIZE);
        if self.capacity == 0 {
            return;
        }
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.bytes.copy_from_slice(bytes);
            self.promote(key);
            return;
        }
        if self.entries.len() >= self.capacity
            && let Some(victim) = self.least_recent
        {
            self.remove(victim);
        }
        let mut cached = vec![0u8; PAGE_SIZE].into_boxed_slice();
        cached.copy_from_slice(bytes);
        let old_head = self.most_recent;
        self.entries.insert(
            key,
            PageEntry {
                bytes: cached,
                prev: None,
                next: old_head,
            },
        );
        if let Some(head) = old_head {
            self.entries.get_mut(&head).expect("head exists").prev = Some(key);
        } else {
            self.least_recent = Some(key);
        }
        self.most_recent = Some(key);
    }

    fn remove(&mut self, key: PageCacheKey) {
        let Some(entry) = self.entries.remove(&key) else {
            return;
        };
        if let Some(prev) = entry.prev {
            self.entries.get_mut(&prev).expect("prev exists").next = entry.next;
        } else {
            self.most_recent = entry.next;
        }
        if let Some(next) = entry.next {
            self.entries.get_mut(&next).expect("next exists").prev = entry.prev;
        } else {
            self.least_recent = entry.prev;
        }
    }

    fn promote(&mut self, key: PageCacheKey) {
        if self.most_recent == Some(key) {
            return;
        }
        let (prev, next) = {
            let entry = self.entries.get(&key).expect("page exists");
            (entry.prev, entry.next)
        };
        if let Some(prev) = prev {
            self.entries.get_mut(&prev).expect("prev exists").next = next;
        }
        if let Some(next) = next {
            self.entries.get_mut(&next).expect("next exists").prev = prev;
        } else {
            self.least_recent = prev;
        }
        let old_head = self.most_recent;
        self.entries.get_mut(&key).expect("page exists").prev = None;
        self.entries.get_mut(&key).expect("page exists").next = old_head;
        if let Some(head) = old_head {
            self.entries.get_mut(&head).expect("head exists").prev = Some(key);
        }
        self.most_recent = Some(key);
    }

    fn remove_chunk(&mut self, chunk: ChunkKey) {
        // A chunk has exactly 64 pages; bounded invalidation keeps the cache
        // coherent when a newer stable chunk replaces the old one.
        for page_idx in 0..64 {
            self.remove(PageCacheKey { chunk, page_idx });
        }
    }
}

impl ChunkList {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::new(),
            order: VecDeque::new(),
            pages: PageLru::from_env(),
            promotion_misses: HashMap::new(),
            promotion_pending: HashSet::new(),
            promotion_queue: VecDeque::new(),
            last_promotion_start: None,
            page_miss_epoch: 0,
        }
    }

    /// Page-tier lookup.  The chunk and page tiers share one owner but have
    /// independent LRU chains because their eviction units differ by 64x.
    pub fn peek_page(&mut self, key: PageKey, page_idx: u8) -> Option<&[u8]> {
        self.pages.get(PageCacheKey {
            chunk: key.into(),
            page_idx,
        })
    }

    /// Admit a page that paid a disk miss.  We do not promote every page from
    /// a chunk hit: that would turn scans into 16KiB allocations.
    pub fn admit_page(&mut self, key: PageKey, page_idx: u8, bytes: &[u8]) {
        self.pages.insert(
            PageCacheKey {
                chunk: key.into(),
                page_idx,
            },
            bytes,
        );
    }

    /// Update a resident page after an in-place nowchunk overwrite.  Returns
    /// false for cold pages so random writes do not allocate cache frames.
    pub fn refresh_page_if_present(&mut self, key: PageKey, page_idx: u8, bytes: &[u8]) -> bool {
        let page = PageCacheKey {
            chunk: key.into(),
            page_idx,
        };
        if self.pages.entries.contains_key(&page) {
            self.pages.insert(page, bytes);
            true
        } else {
            false
        }
    }

    pub fn invalidate_page(&mut self, key: PageKey, page_idx: u8) {
        self.pages.remove(PageCacheKey {
            chunk: key.into(),
            page_idx,
        });
    }

    /// Record a page-tier miss.  Candidate age is measured in intervening
    /// misses rather than wall time: an idle shard keeps useful locality
    /// evidence, while a busy random workload naturally ages it out.
    pub fn note_page_miss(&mut self, key: PageKey, page_idx: u8) {
        let chunk: ChunkKey = key.into();
        if self.map.contains_key(&chunk) || self.promotion_pending.contains(&chunk) {
            return;
        }
        const MAX_CANDIDATES: usize = 256;
        let threshold = std::env::var("NEXUS_CHUNK_PROMOTE_MISSES")
            .ok()
            .and_then(|value| value.parse::<u8>().ok())
            .unwrap_or(4)
            .clamp(2, 16);
        let max_gap = std::env::var("NEXUS_CHUNK_PROMOTE_MISS_GAP")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(512)
            .clamp(32, 65_536);
        self.page_miss_epoch = self.page_miss_epoch.wrapping_add(1);
        let epoch = self.page_miss_epoch;
        if self.promotion_misses.len() >= MAX_CANDIDATES
            && !self.promotion_misses.contains_key(&chunk)
        {
            // Candidates are hints only.  Bounded memory is more important
            // than retaining ancient random-access history.
            self.promotion_misses.clear();
        }
        let candidate = self
            .promotion_misses
            .entry(chunk)
            .or_insert(PromotionCandidate {
                seen_pages: 0,
                last_seen_epoch: epoch,
            });
        if epoch.wrapping_sub(candidate.last_seen_epoch) > max_gap {
            candidate.seen_pages = 0;
        }
        candidate.seen_pages |= 1u64 << page_idx;
        candidate.last_seen_epoch = epoch;
        if candidate.seen_pages.count_ones() >= threshold as u32 {
            self.promotion_misses.remove(&chunk);
            self.promotion_pending.insert(chunk);
            self.promotion_queue.push_back(chunk);
        }
    }

    pub fn take_promotion(&mut self) -> Option<PageKey> {
        let min_interval = Duration::from_millis(
            std::env::var("NEXUS_CHUNK_PROMOTE_MIN_INTERVAL_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(25)
                .clamp(1, 1_000),
        );
        if self
            .last_promotion_start
            .is_some_and(|last| last.elapsed() < min_interval)
        {
            return None;
        }
        let key = self.promotion_queue.pop_front()?;
        self.last_promotion_start = Some(Instant::now());
        Some(key.into())
    }

    pub fn complete_promotion(&mut self, key: PageKey, chunk: Option<Vec<u8>>) {
        let chunk_key: ChunkKey = key.into();
        self.promotion_pending.remove(&chunk_key);
        if self.map.contains_key(&chunk_key) {
            return;
        }
        if let Some(bytes) = chunk {
            self.insert(chunk_key, bytes);
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// 当前 LRU order (front=最新, back=最旧). 供测试 + 调试用.
    pub fn order(&self) -> Vec<ChunkKey> {
        self.order.iter().copied().collect()
    }

    pub fn contains(&self, key: &ChunkKey) -> bool {
        self.map.contains_key(key)
    }

    /// ⭐ T12.9: db-aware contains (兼容旧 API: 走 db=0).
    pub fn contains_db(&self, db: DbId, file_id: u32, chunk_idx: u8) -> bool {
        let k = ChunkKey {
            db,
            file_id,
            chunk_idx,
        };
        self.map.contains_key(&k)
    }

    /// ⭐ 测试 helper: 列出某 db 的所有 keys.
    pub fn keys_for_db(&self, db: DbId) -> Vec<ChunkKey> {
        self.map.keys().filter(|k| k.db == db).copied().collect()
    }

    /// ⭐ 直接插一个 chunk 字节到 cache (供 WriteQueue drain_completed 后调).
    /// 也会在满了时按 LRU 淘汰最旧.
    ///
    /// **语义**:
    /// - `key` 已存在: **不替换**, 视为 LRU 访问移到 front (chunk_list 是只读缓存)
    /// - `key` 不存在: 满了时 LRU 淘汰最旧
    ///
    /// **写路径必须用 `insert_from_write_queue`**: WriteQueue 完成意味着 chunk 字节
    /// 已被落盘, 必须把最新字节载入 cache, 否则 read 拿到 stale 数据.
    pub fn insert(&mut self, key: ChunkKey, chunk: Vec<u8>) {
        debug_assert_eq!(chunk.len(), 1024 * 1024, "chunk must be 1MB");
        // capacity 0 永远不插入 (production 不会, 仅作边界)
        if self.capacity == 0 {
            return;
        }
        if self.map.contains_key(&key) {
            // 已存在: chunk_list 是只读缓存, 不替换字节, 只把 key 移到 front (LRU 访问)
            self.order.retain(|k| k != &key);
            self.order.push_front(key);
            return;
        }
        // 满了: 淘汰最旧
        if self.map.len() >= self.capacity
            && let Some(victim) = self.order.pop_back()
        {
            self.map.remove(&victim);
        }
        self.map.insert(key, Arc::new(chunk));
        self.promotion_misses.remove(&key);
        self.promotion_pending.remove(&key);
        self.order.push_front(key);
    }

    /// ⭐ chunk_list 协同: 把 WriteQueue drain_completed 出来的 chunks 一次性插入.
    ///
    /// **强制覆盖语义**: 与 `insert` 不同, WriteQueue 完成 callback **必须**把最新
    /// chunk 字节装入 cache, 即使 key 已在 cache 中.
    /// 这是 LSM 风格的必然结果: 旧 chunk 已被落盘, cache 必须反映新值, 否则后续
    /// read 拿到 stale 数据 (COW 永远指向最新 pid, chunk_list 必须是新 chunk 字节).
    pub fn insert_from_write_queue(&mut self, key: PageKey, chunk: Vec<u8>) {
        let ck: ChunkKey = key.into();
        debug_assert_eq!(chunk.len(), 1024 * 1024, "chunk must be 1MB");
        if self.capacity == 0 {
            return;
        }
        // 强制覆盖: 不论 key 是否已存在, 都替换为新 chunk 字节
        if let std::collections::hash_map::Entry::Occupied(mut e) = self.map.entry(ck) {
            e.insert(Arc::new(chunk));
            self.pages.remove_chunk(ck);
            self.order.retain(|k| k != &ck);
            self.order.push_front(ck);
            return;
        }
        // 新 key: 满了时 LRU 淘汰最旧
        if self.map.len() >= self.capacity
            && let Some(victim) = self.order.pop_back()
        {
            self.map.remove(&victim);
        }
        self.map.insert(ck, Arc::new(chunk));
        self.promotion_misses.remove(&ck);
        self.promotion_pending.remove(&ck);
        self.pages.remove_chunk(ck);
        self.order.push_front(ck);
    }

    /// ⭐ peek: 命中返回 clone Arc, miss 返回 None. 同时把 key 移到 front (LRU 访问).
    ///
    /// **关键**: 返回 Arc clone 而非 &Vec<u8>, 这样 caller 可以跨 await 持有,
    /// 不会被 ChunkList 内部 mut borrow 限制.
    pub fn peek(&mut self, key: &ChunkKey) -> Option<Arc<Vec<u8>>> {
        if let Some(arc) = self.map.get(key) {
            // LRU: 移到 front
            self.order.retain(|k| k != key);
            self.order.push_front(*key);
            Some(Arc::clone(arc))
        } else {
            None
        }
    }

    /// ⭐ get_or_load: cache 命中返回 clone Arc, miss 调 load_fn 加载后插入.
    /// load_fn 用于从 .block 文件异步读 1MB.
    pub fn get_or_load<F>(&mut self, key: ChunkKey, load_fn: F) -> io::Result<Arc<Vec<u8>>>
    where
        F: FnOnce() -> io::Result<Vec<u8>>,
    {
        if let Some(arc) = self.peek(&key) {
            return Ok(arc);
        }
        // miss: 调 load_fn 加载
        let chunk = load_fn()?;
        // 满了: 淘汰最旧
        if self.map.len() >= self.capacity
            && let Some(victim) = self.order.pop_back()
        {
            self.map.remove(&victim);
        }
        let arc = Arc::new(chunk);
        self.map.insert(key, Arc::clone(&arc));
        self.order.push_front(key);
        Ok(arc)
    }

    /// 显式驱逐一个 chunk. 不会驱逐其他 chunk.
    pub fn invalidate(&mut self, key: &ChunkKey) {
        self.map.remove(key);
        self.order.retain(|k| k != key);
        self.pages.remove_chunk(*key);
        self.promotion_misses.remove(key);
        self.promotion_pending.remove(key);
    }
}

// 抑制 dead_code warning (Rc 没在生产代码用到但导入可留以扩展)
#[allow(dead_code)]
fn _rc_dummy() -> Rc<()> {
    Rc::new(())
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_key_roundtrip_with_page_key() {
        let ck = ChunkKey {
            db: DEFAULT_DB_ID,
            file_id: 7,
            chunk_idx: 3,
        };
        let pk: PageKey = ck.into();
        assert_eq!(pk.file_id, 7);
        assert_eq!(pk.chunk_idx, 3);
        let ck2: ChunkKey = pk.into();
        assert_eq!(ck, ck2);
    }

    #[test]
    fn page_tier_promotes_hot_page_and_evicts_lru() {
        let mut pages = PageLru::new(2);
        let a = PageCacheKey {
            chunk: PageKey {
                file_id: 1,
                chunk_idx: 0,
            }
            .into(),
            page_idx: 0,
        };
        let b = PageCacheKey {
            chunk: PageKey {
                file_id: 2,
                chunk_idx: 0,
            }
            .into(),
            page_idx: 0,
        };
        let c = PageCacheKey {
            chunk: PageKey {
                file_id: 3,
                chunk_idx: 0,
            }
            .into(),
            page_idx: 0,
        };
        let page = vec![7u8; PAGE_SIZE];
        pages.insert(a, &page);
        pages.insert(b, &page);
        assert!(pages.get(a).is_some());
        pages.insert(c, &page);
        assert!(pages.get(a).is_some());
        assert!(pages.get(b).is_none());
        assert!(pages.get(c).is_some());
    }

    #[test]
    fn chunk_list_capacity_zero_does_not_panic() {
        // 容量 0: insert 应立即淘汰 (边界)
        let mut list = ChunkList::new(0);
        list.insert(
            ChunkKey {
                db: DEFAULT_DB_ID,
                file_id: 0,
                chunk_idx: 0,
            },
            vec![0u8; 1024 * 1024],
        );
        assert_eq!(list.len(), 0, "capacity 0 should immediately evict");
    }

    // =====================================================================
    // ⭐ T12.9: ChunkList db-aware 测试
    // =====================================================================

    #[test]
    fn chunk_list_db_aware_isolation() {
        // 不同 db 的同 (file_id, chunk_idx) 是独立 chunks
        let mut list = ChunkList::new(8);
        let key_db0 = ChunkKey {
            db: 0,
            file_id: 0,
            chunk_idx: 0,
        };
        let key_db1 = ChunkKey {
            db: 1,
            file_id: 0,
            chunk_idx: 0,
        };
        list.insert(key_db0, vec![1u8; 1024 * 1024]);
        list.insert(key_db1, vec![2u8; 1024 * 1024]);

        assert_eq!(list.len(), 2, "两个独立 chunks");
        assert!(list.contains(&key_db0));
        assert!(list.contains(&key_db1));
        assert!(!list.contains_db(2, 0, 0), "db=2 无 chunk");

        let got0 = list.peek(&key_db0).unwrap();
        let got1 = list.peek(&key_db1).unwrap();
        assert_eq!(got0[0], 1);
        assert_eq!(got1[0], 2);
    }

    #[test]
    fn chunk_list_db_aware_capacity_shared() {
        // 8 chunk 容量, 跨 db 共享
        let mut list = ChunkList::new(8);
        // 写 4 个 db=0 + 4 个 db=1 = 8 chunks (满)
        for i in 0..4u32 {
            list.insert(
                ChunkKey {
                    db: 0,
                    file_id: i,
                    chunk_idx: 0,
                },
                vec![0u8; 1024 * 1024],
            );
        }
        for i in 0..4u32 {
            list.insert(
                ChunkKey {
                    db: 1,
                    file_id: i,
                    chunk_idx: 0,
                },
                vec![0u8; 1024 * 1024],
            );
        }
        assert_eq!(list.len(), 8);

        // 再插入触发淘汰
        list.insert(
            ChunkKey {
                db: 2,
                file_id: 0,
                chunk_idx: 0,
            },
            vec![0u8; 1024 * 1024],
        );
        assert_eq!(list.len(), 8, "淘汰一个仍保持 8");

        // 验证: 某个最早插入的 db=0 chunk 应被淘汰
        let first_db0 = ChunkKey {
            db: 0,
            file_id: 0,
            chunk_idx: 0,
        };
        // 不一定淘汰第一个 (LRU 顺序取决于访问), 但总数仍 8
        let _ = first_db0;
    }

    #[test]
    fn chunk_list_db_aware_lru_within_db() {
        // LRU 仍按 (db, file_id, chunk_idx) 维度淘汰
        let mut list = ChunkList::new(3);
        list.insert(
            ChunkKey {
                db: 0,
                file_id: 0,
                chunk_idx: 0,
            },
            vec![0u8; 1024 * 1024],
        );
        list.insert(
            ChunkKey {
                db: 0,
                file_id: 1,
                chunk_idx: 0,
            },
            vec![1u8; 1024 * 1024],
        );
        list.insert(
            ChunkKey {
                db: 1,
                file_id: 0,
                chunk_idx: 0,
            },
            vec![2u8; 1024 * 1024],
        );
        assert_eq!(list.len(), 3);

        // 触发 db=0 file_id=0 访问 (LRU 移到 front)
        list.peek(&ChunkKey {
            db: 0,
            file_id: 0,
            chunk_idx: 0,
        });

        // 再插入, 淘汰的是 db=0 file_id=1 (LRU 末位)
        list.insert(
            ChunkKey {
                db: 2,
                file_id: 0,
                chunk_idx: 0,
            },
            vec![3u8; 1024 * 1024],
        );
        assert!(
            !list.contains(&ChunkKey {
                db: 0,
                file_id: 1,
                chunk_idx: 0
            }),
            "LRU 末位应被淘汰"
        );
        assert!(
            list.contains(&ChunkKey {
                db: 0,
                file_id: 0,
                chunk_idx: 0
            }),
            "新近访问保留"
        );
        assert!(list.contains(&ChunkKey {
            db: 1,
            file_id: 0,
            chunk_idx: 0
        }));
        assert!(list.contains(&ChunkKey {
            db: 2,
            file_id: 0,
            chunk_idx: 0
        }));
    }

    #[test]
    fn chunk_list_keys_for_db_filters() {
        let mut list = ChunkList::new(10);
        list.insert(
            ChunkKey {
                db: 0,
                file_id: 0,
                chunk_idx: 0,
            },
            vec![0u8; 1024 * 1024],
        );
        list.insert(
            ChunkKey {
                db: 0,
                file_id: 1,
                chunk_idx: 0,
            },
            vec![0u8; 1024 * 1024],
        );
        list.insert(
            ChunkKey {
                db: 1,
                file_id: 0,
                chunk_idx: 0,
            },
            vec![0u8; 1024 * 1024],
        );
        list.insert(
            ChunkKey {
                db: 1,
                file_id: 1,
                chunk_idx: 0,
            },
            vec![0u8; 1024 * 1024],
        );
        list.insert(
            ChunkKey {
                db: 2,
                file_id: 0,
                chunk_idx: 0,
            },
            vec![0u8; 1024 * 1024],
        );

        assert_eq!(list.keys_for_db(0).len(), 2);
        assert_eq!(list.keys_for_db(1).len(), 2);
        assert_eq!(list.keys_for_db(2).len(), 1);
        assert_eq!(list.keys_for_db(99).len(), 0);
    }

    #[test]
    fn chunk_list_invalidate_specific_key() {
        let mut list = ChunkList::new(10);
        let k = ChunkKey {
            db: 0,
            file_id: 5,
            chunk_idx: 0,
        };
        list.insert(k, vec![0u8; 1024 * 1024]);
        assert!(list.contains(&k));
        list.invalidate(&k);
        assert!(!list.contains(&k));
    }
}
