//! Storage 层: LCB-Tree 的物理持久化层.
//!
//! 完整设计见 `DESIGN.md` §4.3 - §4.7, 以及
//! `docs/superpowers/plans/2026-07-18-storage-crate.md`.
//!
//! ## 模块分层
//!
//! | 模块 | 职责 |
//! |---|---|
//! | `types`         | 重新导出 `PidLocation` + 核心常量 (CHUNK_SIZE, BLOCK_SIZE 等) |
//! | `meta_cache`    | vpid→pid 映射的全量平坦数组 + 1MB dirty window 异步刷盘 |
//! | `alloc`         | VpidAllocator / PidAllocator / FreePageQueue |
//! | `chunk_writer`  | `NowChunks` + `WriteQueue` + `ChunkWriter` 三层架构 |
//! | `chunk_lru`     | `ChunkList`: 1MB chunk 的 in-memory 只读缓存 (LRU 替换) |
//! | `pager`         | 读/写/创建 page 的派发器 (cache 命中走 cache, miss 走 io_uring) |
//! | `pager_io`      | **🆕 T11** Pager IO 后端抽象: StdFs / IoUring 切换 |
//! | `recover`       | 启动时扫描最后 block + MetaCache union 重建 |
//! | `engine`        | `StorageEngine` facade: open / put / get / delete / range |
//! | `meta_page`     | chunk 0 page 0, db_name → table_dir_root_vpid (T9) |
//! | `table_directory` | 每 db 一棵 BTree, table_name → table_root_vpid (T10) |
//! | `registry`      | DbRegistry write-through cache (T11) |
//! | `btree`         | **🆕 T15** 多层 BTree 路由: travel + split 传播 (Table BTree / TableDirectory) |
//! | `db_name_resolver` | **🆕 T12.14** db name ↔ DbId 双向映射 (持久化到 MetaPage) |

#![allow(dead_code)] // crate 还在搭骨架, 暂时关闭 dead_code 警告

pub mod alloc;
pub mod btree;
pub mod chunk_liveness;
pub mod chunk_lock;
pub mod chunk_lru;
pub mod chunk_writer;
pub mod collections;
pub mod collections_list;
pub mod db_name_resolver;
pub mod engine;
pub mod engine_io;
pub mod file_at;
pub mod keyspace;
pub mod geo;
pub mod index_bloom;
pub mod leaf_cache;
pub mod meta_cache;
pub mod meta_page;
pub mod overflow;
pub mod page_pool;
pub mod pager;
pub mod pager_backend;
pub mod pager_io;
pub mod pager_tree;
pub mod pager_write;
pub mod recover;
pub mod registry;
pub mod row;
pub mod schema;
pub mod sql_rows;
pub mod table_directory;
pub mod types;
pub mod wal;

// ⭐ Scheduler 多线程契约 (storage 实施必须严格遵守):
// 1. 每个 shard 线程自己 NEW 一个 Scheduler (独立 io_uring), 永久 run() loop
// 2. spawn / drive / JoinHandle::poll 全在同一线程
// 3. 跨 shard 通信用 mpsc channel (不用 JoinHandle 跨线程)
// 4. 违反任一条 → JoinInner::UnsafeCell 跨线程 race → 永久 hang
// 详细见: docs/superpowers/plans/2026-07-18-storage-crate.md 顶部 "Scheduler 多线程使用契约" 段.

pub use alloc::{FreePageQueue, PidAllocator, VpidAllocator};
pub use db_name_resolver::{DbNameResolver, RESOLVER_HEADER_SIZE, ResolverError};
pub use engine::{OpenOptions, StorageEngine, StorageError};
pub use meta_cache::{META_WINDOW_SIZE, MetaCache, SLOTS_PER_WINDOW};
pub use meta_page::{MetaError, MetaPage};
pub use overflow::{INLINE_LIMIT, MAX_OVERFLOW_VALUE, needs_overflow};
pub use registry::RegistryError;
pub use table_directory::{TableDirError, TableDirectory};
pub use types::{
    BLOCK_SIZE, CHUNK_SIZE, CHUNKS_PER_BLOCK, DbId, INDEX_COUNT, INDEX_SIZE, IoBackend,
    IoBackendConfig, MATE_CACHE_SIZE, META_PID, META_VPID, PAGE_SIZE, PAGES_PER_CHUNK,
    PID_ALIVE, PID_DIRTY, PID_IN_TXN, PageKey, PidLocation, SLOTS_PER_INDEX, VpidLogEntry,
    offset_to_pid, pid_to_chunk_offset, pid_to_offset,
};

/// 🆕 test_support: 让 integration test 访问 crate 内部状态.
///
/// 实际生产代码不应 `pub use crate::*`, 但 tests 需要.
pub mod test_support {
    pub use crate::alloc::{FreePageQueue, PidAllocator, VpidAllocator};
    pub use crate::chunk_lock::{AcquireResult, ChunkLockEntry, ChunkLockMap};
    pub use crate::chunk_lru::{ChunkKey, ChunkList};
    pub use crate::chunk_writer::{ChunkWriter, NowChunks, WriteHandle, WriteQueue};
    pub use crate::db_name_resolver::{DbNameResolver, RESOLVER_HEADER_SIZE, ResolverError};
    pub use crate::engine::{OpenOptions, StorageEngine};
    pub use crate::meta_cache::MetaCache;
    pub use crate::meta_page::{MetaError, MetaPage};
    pub use crate::pager::{PageWriteBatch, Pager, TaskId, TravelTree, TravelTreeGuard};
    pub use crate::recover::{RecoveredState, recover};
    pub use crate::table_directory::{TableDirError, TableDirectory};
    pub use crate::types::{META_PID, META_VPID, PageKey, PidLocation};
}
