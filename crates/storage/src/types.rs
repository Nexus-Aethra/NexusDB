//! 核心类型与常量 (来自 DESIGN §4.3.1, §4.3.10, §4.3.11).
//!
//! ⭐ PidLocation 必须 8 字节固定 — MetaCache 一项 8B slot (1MB index = 128K slot).

use page::PidLocation as PagePidLocation;

// =====================================================================
// ⭐ T12.1 DbId + 复合 key 类型 (来自 plan 2026-07-20 §1)
// =====================================================================

/// db 命名空间标识.
///
/// **T12 引入**: 单 ShardManager 内部跨 db 共享所有组件 (ChunkList / NowChunks
/// / VpidAllocator / MetaCache 等), 用 `DbId` 而不是 `String` 区分 db.
///
/// **好处**: 4 字节, Copy trait, 零分配. 优于 String (24B + heap alloc + hash).
///
/// **外部 API**: ShardManager / StorageEngine 公共 API 仍接受 `&str` db_name,
/// 入口处经 `DbNameResolver::get_or_create` 一次转换. 内部所有组件用 u32.
pub type DbId = u32;

/// 默认 db id (单 db 模式 / 测试用).
pub const DEFAULT_DB_ID: DbId = 0;

/// 默认 db 名称 (向后兼容, 单 db 测试用).
pub const DEFAULT_DB_NAME: &str = "default";

/// ⭐ T12.12 ShardId: 单 ShardManager 内部多 shard 标识.
///
/// **T12 引入**: 取代之前"无 shard 概念"的状态. 每个 shard 持一个独立
/// `StorageEngine` (独立 io_uring + 独立 ChunkList 8MB). shard 0 = 默认 / 单机测试.
pub type ShardId = u32;

/// 默认 shard id (单 shard 模式 / 测试用).
pub const DEFAULT_SHARD_ID: ShardId = 0;

/// MetaCache key: `(db_id, vpid)` 对, 16 字节对齐 (避免 hashbrown SSE2 UB).
///
/// `#[repr(C, align(16))]` 强制 16 字节对齐绕过 rustc 1.97 debug 模式
/// `ptr::copy_nonoverlapping` 运行时对齐检查 panic.
///
/// **大小**: 16B (DbId 4B + u64 8B + 4B padding).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(C, align(16))]
pub struct MetaKey {
    pub db: DbId,
    pub vpid: u64,
}

impl MetaKey {
    pub const fn new(db: DbId, vpid: u64) -> Self {
        Self { db, vpid }
    }
}

// =====================================================================
// ⭐ T12.2 IoBackend 抽象 (来自 plan §4)
// =====================================================================

/// StorageEngine IO 后端选择.
///
/// **T12 引入**: 让同 shard 同一 backend, 通过 `OpenOptions.io` 选.
///
/// - `StdFs`: 同步 std::fs IO (测试 / 调试 / 单线程环境)
/// - `IoUring`: scheduler::io_ops 异步 IO (生产, per-shard single-threaded +
///   io_uring, 由 T16 启用)
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum IoBackend {
    #[default]
    StdFs,
    IoUring,
}

/// ⭐ 进阶 IO 后端配置 (T18a+).
///
/// 控制各优化层级的开关.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IoBackendConfig {
    /// 基础后端类型.
    pub backend: IoBackend,
    /// T18a: 启用 `IOSQE_FIXED_FILE` (FD 池 + 省 open/close).
    /// 默认 true (开启). 仅在 IoUring 后端生效.
    pub use_fixed_file: bool,
    /// T18b: 启用 Registered Buffers (`ReadFixed`/`WriteFixed`).
    pub use_fixed_buffer: bool,
    /// T18c: SQPOLL 空闲超时 (ms). 0 = 禁用.
    pub sqpoll_ms: u32,
    /// T18d: 启用 O_DIRECT (绕开 page cache).
    pub o_direct: bool,
}

impl Default for IoBackendConfig {
    fn default() -> Self {
        Self {
            backend: IoBackend::default(),
            use_fixed_file: true,
            use_fixed_buffer: false,
            sqpoll_ms: 0,
            o_direct: false,
        }
    }
}

impl From<IoBackend> for IoBackendConfig {
    fn from(backend: IoBackend) -> Self {
        Self {
            backend,
            ..Default::default()
        }
    }
}

/// Page 固定大小, 来自 page crate (16 KiB).
pub use page::PAGE_SIZE;

/// Block / Chunk / Mate 容量 (来自 DESIGN §4.3.1).
pub const CHUNK_SIZE: usize = 1024 * 1024; // 1 MiB
pub const BLOCK_SIZE: usize = 10 * CHUNK_SIZE; // 10 MiB
pub const CHUNKS_PER_BLOCK: usize = 10;
pub const PAGES_PER_CHUNK: usize = CHUNK_SIZE / PAGE_SIZE; // 64

/// MetaCache 容量 (来自 DESIGN §4.3.4).
pub const MATE_CACHE_SIZE: usize = 10 * 1024 * 1024; // 10 MiB
pub const INDEX_SIZE: usize = 1024 * 1024; // 1 MiB
pub const INDEX_COUNT: usize = 10;
pub const SLOTS_PER_INDEX: usize = INDEX_SIZE / 8; // 128K

/// 重新导出 PidLocation (来自 page crate, 已 #[repr(C, packed)] 8B).
pub type PidLocation = PagePidLocation;

/// ⭐ PidLocation flags 位. storage crate 自己定义 (避免 page crate 暴露不必要的常量).
pub const PID_ALIVE: u8 = 0b0000_0001;
pub const PID_IN_TXN: u8 = 0b0000_0010;
pub const PID_DIRTY: u8 = 0b0000_0100; // 已被 write 但未 fsync (mark in nowchunks)

/// VpidLogEntry: chunk 末尾的 vpid 变更日志条目 (来自 DESIGN §4.3.10).
/// 16B 固定: vpid[8] + PidLocation[8] = 16B.
///
/// **⚠️** 字段顺序必须与 page.mate 一致, recover 时按这个结构 parse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct VpidLogEntry {
    pub vpid: u64,
    pub pid: PidLocation,
}

const _: [(); 16] = [(); std::mem::size_of::<VpidLogEntry>()];

/// pid → 字节偏移的 O(1) 算术 (来自 DESIGN §4.3.11).
///
/// ⭐ 重要约束:
/// - file_id 4B, chunk_idx 1B, page_idx 2B 全部 LE 写入
/// - 向下取整到 chunk 边界: chunk_byte_off = (file_id * BLOCK_SIZE) + (chunk_idx * CHUNK_SIZE)
/// - page_byte_off = chunk_byte_off + (page_idx * PAGE_SIZE)
/// - pid.page_idx 是 chunk 内 page 索引 (0..63)
pub fn pid_to_offset(pid: &PidLocation) -> u64 {
    let block_off = (pid.file_id() as u64) * BLOCK_SIZE as u64;
    let chunk_off = (pid.chunk_idx() as u64) * CHUNK_SIZE as u64;
    let page_off = (pid.page_idx() as u64) * PAGE_SIZE as u64;
    block_off + chunk_off + page_off
}

/// chunk 边界向下取整 (找到该 pid 所在 chunk 的起始 byte 偏移).
///
/// 用于 io_uring read chunk 时: 给一个具体 page 的 pid, 计算其所在 1MB chunk 的起始 offset.
pub fn pid_to_chunk_offset(pid: &PidLocation) -> u64 {
    let block_off = (pid.file_id() as u64) * BLOCK_SIZE as u64;
    let chunk_off = (pid.chunk_idx() as u64) * CHUNK_SIZE as u64;
    block_off + chunk_off
}

/// 反向: 给 file_id + chunk_idx + page_idx → PidLocation (默认 ALIVE flag).
pub fn offset_to_pid(file_id: u32, chunk_idx: u8, page_idx: u16) -> PidLocation {
    PidLocation::from_bytes(&[
        (file_id & 0xFF) as u8,
        ((file_id >> 8) & 0xFF) as u8,
        ((file_id >> 16) & 0xFF) as u8,
        ((file_id >> 24) & 0xFF) as u8,
        chunk_idx,
        (page_idx & 0xFF) as u8,
        ((page_idx >> 8) & 0xFF) as u8,
        PID_ALIVE,
    ])
}

/// PidLocation 的字节序工具, 集中在此方便 T11 polish 改 layout.
pub mod pid_bytes {
    pub fn file_id_le(file_id: u32) -> [u8; 4] {
        file_id.to_le_bytes()
    }
    pub fn page_idx_le(page_idx: u16) -> [u8; 2] {
        page_idx.to_le_bytes()
    }
}

/// ⭐ PageKey: 在 NowChunks / chunk_list / chunk_lock 中标识"一个 1MB chunk".
///
/// 仅含 file_id + chunk_idx (不含 page_idx), 因为整 chunk 是缓存 / 写入单位.
///
/// **对齐故意为 16 字节**: hashbrown 0.17 的 SSE2 优化在 rustc debug 模式
/// 下触发 `unsafe_precondition` 运行时检查, 要求 key 16 字节对齐. 普通
/// `#[derive]` 的 PageKey 是 4 字节对齐, 触发 `ptr::copy_nonoverlapping`
/// 对齐检查 UB panic. `#[repr(C, align(16))]` 强制对齐 16 字节绕过.
/// 性能: align(16) 只影响内存布局, 大小仍是 8 字节, 几乎无影响.
///
/// **Ord**: NowChunks 当前用 `BTreeMap<PageKey, ChunkBuf>` 替代 HashMap
/// 规避 hashbrown SSE2 alignment UB (rustc 1.97 debug 模式 panic, release
/// 模式 SIGSEGV). BTreeMap 需要 `Ord + PartialOrd`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(C, align(16))]
pub struct PageKey {
    pub file_id: u32,
    pub chunk_idx: u8,
}

/// ⭐ MetaPage 固定位置: chunk 0 page 0 (T9 catalog 入口).
///
/// 整个 catalog 树的根, db_name → table_dir_root_vpid 的 BTree.
///
/// 必须在任何用户数据 page 之前初始化 (vpid 0 / chunk 0 page 0).
pub const META_VPID: u64 = 0;

/// MetaPage 对应的 PidLocation (chunk 0 page 0, 默认 ALIVE flag).
pub const META_PID: PidLocation = PidLocation {
    file_id: 0,
    chunk_idx: 0,
    page_idx: 0,
    flags: PID_ALIVE,
};

// =====================================================================
// ⭐ PidLocation 8B packed layout 单元测试 (与 page crate pid_location_tests 配套)
// =====================================================================
#[cfg(test)]
mod types_tests {
    use crate::types::*;

    #[test]
    fn pid_location_is_8_bytes_packed_via_storage() {
        // ⭐ storage 层重新导出 PidLocation, 必须仍是 packed 8B
        assert_eq!(std::mem::size_of::<PidLocation>(), 8);
        assert_eq!(std::mem::align_of::<PidLocation>(), 1);
        assert_eq!(SLOTS_PER_INDEX, 131072, "1MB / 8B = 128K = 131072 slot");
    }

    #[test]
    fn pid_to_offset_chunk_aligned() {
        let pid = PidLocation {
            file_id: 0,
            chunk_idx: 5,
            page_idx: 3,
            flags: 0,
        };
        let off = pid_to_offset(&pid);
        // 5 chunks + 3 pages
        assert_eq!(off, 5 * CHUNK_SIZE as u64 + 3 * PAGE_SIZE as u64);
    }

    #[test]
    fn pid_to_offset_block_aware() {
        // file 0 vs file 1 差 BLOCK_SIZE
        let p0 = PidLocation {
            file_id: 0,
            chunk_idx: 0,
            page_idx: 0,
            flags: 0,
        };
        let p1 = PidLocation {
            file_id: 1,
            chunk_idx: 0,
            page_idx: 0,
            flags: 0,
        };
        assert_eq!(pid_to_offset(&p1) - pid_to_offset(&p0), BLOCK_SIZE as u64);
    }

    #[test]
    fn pid_to_chunk_offset_strips_page_idx() {
        let p0 = PidLocation {
            file_id: 0,
            chunk_idx: 5,
            page_idx: 0,
            flags: 0,
        };
        let p63 = PidLocation {
            file_id: 0,
            chunk_idx: 5,
            page_idx: 63,
            flags: 0,
        };
        assert_eq!(pid_to_chunk_offset(&p0), pid_to_chunk_offset(&p63));
        assert_eq!(pid_to_chunk_offset(&p0), 5 * CHUNK_SIZE as u64);
    }

    #[test]
    fn offset_to_pid_round_trip() {
        let original = PidLocation {
            file_id: 42,
            chunk_idx: 7,
            page_idx: 11,
            flags: 0,
        };
        let bytes = original.to_bytes();
        let restored = PidLocation::from_bytes(&bytes);
        assert_eq!(original, restored);
        // 通过 offset_to_pid 重建应等价
        let rebuilt = offset_to_pid(42, 7, 11);
        assert_eq!(rebuilt.file_id(), 42);
        assert_eq!(rebuilt.chunk_idx(), 7);
        assert_eq!(rebuilt.page_idx(), 11);
        assert_eq!(rebuilt.flags(), PID_ALIVE);
    }

    #[test]
    fn vpid_log_entry_is_16_bytes() {
        assert_eq!(std::mem::size_of::<VpidLogEntry>(), 16);
    }

    #[test]
    fn chunk_math_consistency() {
        assert_eq!(CHUNK_SIZE % PAGE_SIZE, 0);
        assert_eq!(PAGES_PER_CHUNK, 64);
        assert_eq!(BLOCK_SIZE % CHUNK_SIZE, 0);
        assert_eq!(CHUNKS_PER_BLOCK, 10);
        assert_eq!(MATE_CACHE_SIZE, INDEX_SIZE * INDEX_COUNT);
        assert_eq!(SLOTS_PER_INDEX * 8, INDEX_SIZE);
    }

    #[test]
    fn storage_pid_location_8b_math() {
        // 关键不变量: storage crate 也依赖 MetaCache 8B slot 数学
        // 1MB / 8B = 128K slot per index
        // 总 slot = 128K * 10 = 1.28M vpid (1.28M 个 vpid 能被 MetaCache 全缓存)
        let total = SLOTS_PER_INDEX as u64 * INDEX_COUNT as u64;
        assert_eq!(total, 1310720, "128K × 10 = 1.28M vpid per MetaCache");
    }

    // =====================================================================
    // T12.1 DbId + MetaKey 单元测试
    // =====================================================================

    #[test]
    fn db_id_is_u32_alias() {
        // DbId 是 u32 type alias, 4 字节
        assert_eq!(std::mem::size_of::<DbId>(), 4);
        assert_eq!(std::mem::size_of::<DbId>(), std::mem::size_of::<u32>());
        let _id: DbId = 42;
    }

    #[test]
    fn meta_key_size_and_alignment() {
        // MetaKey 必须 16 字节对齐 (避免 hashbrown SSE2 UB)
        assert_eq!(std::mem::align_of::<MetaKey>(), 16);
        // 大小: DbId(4) + u64(8) + 4B padding = 16B
        let key = MetaKey::new(0, 0);
        assert_eq!(std::mem::size_of::<MetaKey>(), 16);
        // 字段可访问
        assert_eq!(key.db, 0);
        assert_eq!(key.vpid, 0);
    }

    #[test]
    fn meta_key_ordering_consistent() {
        // Ord 实现 (BTreeMap 用)
        let a = MetaKey::new(0, 5);
        let b = MetaKey::new(0, 10);
        let c = MetaKey::new(1, 0);
        assert!(a < b, "同 db 不同 vpid 按 vpid 排序");
        assert!(b < c, "db 0 < db 1 即使 vpid 大");
    }

    #[test]
    fn default_db_id_and_name() {
        assert_eq!(DEFAULT_DB_ID, 0);
        assert_eq!(DEFAULT_DB_NAME, "default");
    }

    // =====================================================================
    // T12.2 IoBackend 单元测试
    // =====================================================================

    #[test]
    fn io_backend_default_is_std_fs() {
        // T16 之前唯一可用 backend, 默认值是 StdFs
        assert_eq!(IoBackend::default(), IoBackend::StdFs);
    }

    #[test]
    fn io_backend_traits_derived() {
        // 必须 Copy + Clone + Eq + Hash 才能在 HashMap key 用
        let b = IoBackend::StdFs;
        let copy = b;
        assert_eq!(b, copy);
        // Copy trait 足以传递, 不需 clone (clippy: redundant_clone)
        // Hash 测试
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        IoBackend::StdFs.hash(&mut h1);
        IoBackend::StdFs.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish(), "同 backend 同 hash");
        let mut h3 = DefaultHasher::new();
        IoBackend::IoUring.hash(&mut h3);
        assert_ne!(h1.finish(), h3.finish(), "不同 backend 不同 hash");
    }
}
