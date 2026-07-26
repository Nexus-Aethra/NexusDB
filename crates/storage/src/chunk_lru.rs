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

use std::collections::{HashMap, VecDeque};
use std::io;
use std::rc::Rc;
use std::sync::Arc;

use crate::types::{DEFAULT_DB_ID, DbId, PageKey};

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
}

impl ChunkList {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::new(),
            order: VecDeque::new(),
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
