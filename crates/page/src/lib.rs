//! Page 层: LCB-Tree 的逻辑页面操作接口.
//!
//! **设计原则**: 函数式 API, 不持有内部状态.
//!
//! 每个公开函数 `fn op(page: &[u8]) -> ...` 或 `fn op(page: &mut [u8], ...)`,
//! 接受一个固定 16 KiB 字节切片, 返回结果或修改 page.
//!
//! 所有权清晰:
//! - 读操作: `&[u8]` (只读 page)
//! - 写操作: `&mut [u8]` (原地修改 page, 由调用方持有 buffer)
//!
//! ## Page 布局
//!
//! ```text
//! 0x0000 ┌──────────────────────────┐
//!        │ Page Header (40 bytes)   │
//! 0x0028 ├──────────────────────────┤
//!        │ Item Area (向高地址长)   │
//!        │ ...                      │
//! 0x3F00 ├──────────────────────────┤
//!        │ Checkpoint Array (向低长)│
//!        │ ...                      │
//! 0x3FE0 ├──────────────────────────┤
//!        │ Footer (32 bytes)        │
//! 0x4000 └──────────────────────────┘
//! ```
//!
//! ## Item 编码 (前缀压缩)
//!
//! 每个 item = `[hdr(4B)][key_unique(N)][vint_value_len][value(M)]`,
//! hdr 中 `shared_prefix_len` 表示与上一个 item 共享前缀字节数.
//!
//! ## Checkpoint 数组
//!
//! 页尾 checkpoint 数组每 16-32 个 item 一个采样点,
//! 支持 O(log n) 二分定位 item.
//!
//! ## 公开 API 分组
//!
//! | 组 | 函数 |
//! |---|---|
//! | Header meta | `page_type`, `page_vpid`, `page_set_vpid`, `page_key_count`, `page_free_space`, `page_version` |
//! | Leaf CRUD   | `leaf_get`, `leaf_insert`, `leaf_delete` |
//! | Internal 导航 | `internal_child`, `internal_insert`, `internal_delete` |
//! | 分裂       | `leaf_split`, `internal_split` |
//! | 校验       | `page_validate_checksum`, `page_set_checksum` |

#![allow(dead_code)]

mod checkpoint;
pub mod debug;
pub mod dump;
mod error;
mod header;
mod index;
mod index_merge;
mod internal;
mod item;
mod leaf;
mod leaf_split;
mod ptr;
mod varint;

pub use checkpoint::{
    Checkpoint, CheckpointHeader, MAX_PER_CHECKPOINT, MIN_PER_CHECKPOINT, checkpoint_area_size,
    needed_checkpoint_count, read_checkpoint, read_checkpoint_header, write_checkpoint,
    write_checkpoint_header,
};
pub use error::PageError;
pub use header::{
    ITEM_AREA_SIZE, PAGE_FOOTER_SIZE, PAGE_HEADER_SIZE, PAGE_MAGIC, PAGE_SIZE, PageHeader,
    PageType, page_check_magic, page_flags, page_free_off, page_free_space, page_init_header,
    page_key_count, page_set_flags, page_set_free_off, page_set_key_count, page_set_version,
    page_set_vpid, page_type, page_version, page_vpid,
};
pub use index::{PageIndex, Segment, pre_split_segment};
pub use index_merge::{apply_pre_merge, apply_pre_merge_steal, pre_merge_segment};
pub use internal::{
    internal_child, internal_child_with_bounds, internal_delete, internal_insert, internal_new, internal_push_back,
    internal_split, internal_update,
};
pub use item::{Item, ItemKind, decode_item, encode_internal_item, encode_leaf_item};
pub use leaf::{
    leaf_delete, leaf_get, leaf_get_with, leaf_insert, leaf_new, leaf_push_back, leaf_scan_from, leaf_update,
};
pub use leaf_split::leaf_split;
pub use ptr::{InternalItemPtr, ItemPtr, LeafItemPtr};

/// 虚拟页 ID (B+Tree 内部命名空间).
pub type Vpid = u64;

/// 单条 vpid → pid 映射占位符.
///
/// ⭐ `#[repr(C, packed)]` 保证 `file_id[4] + chunk_idx[1] + page_idx[2] + flags[1]` = 8 字节,
/// 不会因 Rust 默认 4 字节对齐 padding 撑成 12B.
/// 这是 storage crate MetaCache 一项 8B 槽的前提 (1MB index = 128K slot).
/// 注意: packed 字段不能通过引用 (&) 调用方法, 需用 `read_unaligned` 或显式 `u32::from_le_bytes`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C, packed)]
pub struct PidLocation {
    pub file_id: u32,  // 4B
    pub chunk_idx: u8, // 1B
    pub page_idx: u16, // 2B
    pub flags: u8,     // 1B  (= 8B total, no padding)
}

// Compile-time 断言 PidLocation 确实是 8 字节.
const _: [(); 8] = [(); std::mem::size_of::<PidLocation>()];

impl PidLocation {
    /// 从文件读到的 8 字节创建 PidLocation.
    /// 安全: 接受 [u8; 8] 显式按字段拷贝, 避开 packed 引用的限制.
    pub fn from_bytes(bytes: &[u8; 8]) -> Self {
        PidLocation {
            file_id: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            chunk_idx: bytes[4],
            page_idx: u16::from_le_bytes([bytes[5], bytes[6]]),
            flags: bytes[7],
        }
    }

    /// 序列化为 8 字节 (写入 .block / page.mate 时用).
    pub fn to_bytes(&self) -> [u8; 8] {
        let file_id_bytes = self.file_id.to_le_bytes();
        let page_idx_bytes = self.page_idx.to_le_bytes();
        [
            file_id_bytes[0],
            file_id_bytes[1],
            file_id_bytes[2],
            file_id_bytes[3],
            self.chunk_idx,
            page_idx_bytes[0],
            page_idx_bytes[1],
            self.flags,
        ]
    }

    /// ⭐ 安全读取 `file_id` (packed struct 不能直接 `&self.file_id` 调用方法).
    pub fn file_id(&self) -> u32 {
        u32::from_le_bytes(self.file_id.to_le_bytes())
    }
    pub fn chunk_idx(&self) -> u8 {
        self.chunk_idx
    }
    pub fn page_idx(&self) -> u16 {
        u16::from_le_bytes(self.page_idx.to_le_bytes())
    }
    pub fn flags(&self) -> u8 {
        self.flags
    }

    pub fn with_flags(mut self, flags: u8) -> Self {
        self.flags = flags;
        self
    }
}

// =====================================================================
// ⭐ PidLocation packed 8B 测试 (storage MetaCache 8B 槽前提条件)
// =====================================================================
#[cfg(test)]
mod pid_location_tests {
    use super::*;

    /// 编译时 + 运行时双重断言: size = 8B, align = 1 (packed)
    #[test]
    fn pid_location_is_8_bytes_packed() {
        assert_eq!(
            std::mem::size_of::<PidLocation>(),
            8,
            "PidLocation must be 8 bytes for MetaCache slot layout"
        );
        assert_eq!(
            std::mem::align_of::<PidLocation>(),
            1,
            "PidLocation must be packed (align 1) to avoid padding"
        );
    }

    /// 字段读写 roundtrip (packed struct 用 helper 访问)
    #[test]
    fn pid_location_field_roundtrip() {
        let pid = PidLocation {
            file_id: 0xDEAD_BEEF,
            chunk_idx: 7,
            page_idx: 1234,
            flags: 0xFF,
        };
        // 安全读取 helper (避开 packed 字段引用)
        assert_eq!(pid.file_id(), 0xDEAD_BEEF);
        assert_eq!(pid.chunk_idx(), 7);
        assert_eq!(pid.page_idx(), 1234);
        assert_eq!(pid.flags(), 0xFF);
    }

    /// bytes → PidLocation → bytes roundtrip
    #[test]
    fn pid_location_bytes_roundtrip() {
        let original = PidLocation {
            file_id: 0x1234_5678,
            chunk_idx: 0xAB,
            page_idx: 0xCDEF,
            flags: 0x55,
        };
        let bytes = original.to_bytes();
        assert_eq!(bytes.len(), 8);
        let restored = PidLocation::from_bytes(&bytes);
        assert_eq!(original, restored);
    }

    /// ⭐ 大量随机 case 测试 bytes roundtrip 不丢字段顺序
    #[test]
    fn pid_location_bytes_roundtrip_random() {
        // 用 hash 确定性 seed, 测试可重现
        let mut state: u64 = 0xCAFE_BABE_DEAD_BEEF;
        for _ in 0..1000 {
            // 简单 LCG
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let file_id = (state as u32) ^ 0xFFFF_FFFF;
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let chunk_idx = (state as u8) ^ 0xFF;
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let page_idx = ((state >> 16) as u16) ^ 0xFFFF;
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let flags = (state as u8) ^ 0xFF;

            let pid = PidLocation {
                file_id,
                chunk_idx,
                page_idx,
                flags,
            };
            let bytes = pid.to_bytes();
            let restored = PidLocation::from_bytes(&bytes);
            assert_eq!(pid, restored, "roundtrip mismatch for {:?}", pid);
        }
    }

    /// 已知小端字节序 (这是 storage MetaCache 读取依赖的)
    #[test]
    fn pid_location_little_endian_layout() {
        let pid = PidLocation {
            file_id: 0x0403_0201,
            chunk_idx: 0x05,
            page_idx: 0x0706,
            flags: 0x08,
        };
        let bytes = pid.to_bytes();
        // 已知结构: file_id LE (4B) + chunk_idx (1B) + page_idx LE (2B) + flags (1B)
        assert_eq!(bytes, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    /// with_flags 不动其他字段 (用 helper 访问, 避开 packed 引用)
    #[test]
    fn pid_location_with_flags_only_changes_flags() {
        let original = PidLocation {
            file_id: 100,
            chunk_idx: 3,
            page_idx: 42,
            flags: 0,
        };
        let modified = original.with_flags(0xAA);
        assert_eq!(modified.file_id(), 100);
        assert_eq!(modified.chunk_idx(), 3);
        assert_eq!(modified.page_idx(), 42);
        assert_eq!(modified.flags(), 0xAA);
        // 原值未被修改
        assert_eq!(original.flags(), 0);
    }

    /// Copy + Clone: 与原始相等
    #[test]
    fn pid_location_clone_equals_original() {
        let original = PidLocation {
            file_id: 999,
            chunk_idx: 5,
            page_idx: 100,
            flags: 0x42,
        };
        // PidLocation 实现了 Copy, 但 Clone trait 也存在 (派生).
        // 验证 Copy 和 Clone 行为一致 (都产生相等的副本).
        let copy_clone = original; // Copy 隐式
        assert_eq!(original, copy_clone);
        // 类型系统保证 Clone::clone 行为与 Copy 一致 (因为 PidLocation: Copy + Clone).
        // 我们用 `Copy` 路径覆盖就够了, 避免触发 clippy::clone_on_copy 警告.
    }

    /// ⭐ packed 数组连续布局 (验证 MetaCache 1MB / 8B = 128K slot 数学)
    #[test]
    fn pid_location_array_math_matches_meta_cache_slot() {
        // 1MB = 1024 * 1024 = 1048576 字节
        // 一项 8B → 128K = 131072 slot per index
        const MB: usize = 1024 * 1024;
        const SLOT_SIZE: usize = std::mem::size_of::<PidLocation>();
        assert_eq!(SLOT_SIZE, 8);
        assert_eq!(MB / SLOT_SIZE, 131072, "1MB / 8B = 128K = 131072 slot");
    }

    /// ⭐ packed Vec 字节数 = N × 8 (无 padding)
    #[test]
    fn pid_location_vec_no_padding() {
        let v: Vec<PidLocation> = (0..10)
            .map(|i| PidLocation {
                file_id: i as u32,
                chunk_idx: 0,
                page_idx: 0,
                flags: 0,
            })
            .collect();
        let len_bytes = v.len() * std::mem::size_of::<PidLocation>();
        // Vec 自带 length 字段 (usize), 这里只看元素总字节
        let ptr_size = std::mem::size_of::<PidLocation>();
        assert_eq!(len_bytes, 10 * 8);
        assert_eq!(v.capacity() * ptr_size, std::mem::size_of_val(&v[..]));
    }
}
