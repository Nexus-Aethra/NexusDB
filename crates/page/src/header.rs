//! Page 头部常量与 PageType 枚举.
//!
//! ## Page Header 布局 (40 bytes, 0x0000..0x0028)
//!
//! ```text
//! 0x00 magic[4]        "LCBP" (大端 0x4C434250)
//! 0x04 page_type[1]    PageType 枚举
//! 0x05 flags[1]        bit0 = dirty, bit1 = in_txn
//! 0x06 key_count[2]    LE
//! 0x08 free_off[2]     LE, item 区当前末尾
//! 0x0A prefix_overlap[2] 累计前缀节省字节数 (调试用)
//! 0x0C checksum[8]     xxhash64 (header 之外的全部字节)
//! 0x14 version[4]      COW 版本号
//! 0x18 vpid[8]         所属 vpid
//! 0x20 chunk_log_off[2] vpid 变更日志在 chunk 内的偏移
//! 0x22 reserved[6]
//! ```

use crate::error::PageError;

/// Page 固定大小: 16 KiB (与 DESIGN 4.3.1 一致).
pub const PAGE_SIZE: usize = 16 * 1024;

/// Page Header 大小.
pub const PAGE_HEADER_SIZE: usize = 40;

/// Page Footer 大小 (16B: magic[4] + version[4] + checksum[8]).
pub const PAGE_FOOTER_SIZE: usize = 16;

/// Item Area 最大大小 = PAGE_SIZE - HEADER - FOOTER.
pub const ITEM_AREA_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE - PAGE_FOOTER_SIZE;

/// Checkpoint Array 起始位置 (从 ITEM_AREA 末端往前).
pub const CHECKPOINT_AREA_END: usize = PAGE_SIZE - PAGE_FOOTER_SIZE;

/// Page magic: "LCBP" = [0x4C, 0x43, 0x42, 0x50].
pub const PAGE_MAGIC: [u8; 4] = [0x4C, 0x43, 0x42, 0x50];

/// Page 类型枚举 (1 byte).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    /// 描述 B+Tree 元信息 (root vpid, max_vpid 等).
    Meta = 1,
    /// B+Tree 内部节点, 存 [separator_key, child_vpid].
    Internal = 2,
    /// B+Tree 叶子节点, 存 [key, value].
    Leaf = 3,
}

impl PageType {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(PageType::Meta),
            2 => Some(PageType::Internal),
            3 => Some(PageType::Leaf),
            _ => None,
        }
    }
}

/// Page Header POD 结构 (40 bytes), 用于结构化读写.
///
/// **Note**: 真正的 page 是 `[u8; PAGE_SIZE]`, 此结构只在读写 header 时
/// 通过 transmute 借用, **不允许 long-lived 存储**.
#[repr(C, packed)]
#[derive(Clone, Copy, Debug)]
pub struct PageHeader {
    pub magic: [u8; 4],      // 0x00
    pub page_type: u8,       // 0x04
    pub flags: u8,           // 0x05
    pub key_count: u16,      // 0x06
    pub free_off: u16,       // 0x08
    pub prefix_overlap: u16, // 0x0A
    pub checksum: u64,       // 0x0C
    pub version: u32,        // 0x14
    pub vpid: u64,           // 0x18
    pub chunk_log_off: u16,  // 0x20
    pub reserved: [u8; 6],   // 0x22
}

// Compile-time 断言 PageHeader 确实是 40 字节.
const _: [(); 40] = [(); std::mem::size_of::<PageHeader>()];

impl PageHeader {
    /// 从 page buffer 借用 header. 仅在 PAGE_SIZE 缓冲上调用.
    pub fn from_page(page: &[u8; PAGE_SIZE]) -> &Self {
        unsafe { &*(page.as_ptr() as *const PageHeader) }
    }

    pub fn from_page_mut(page: &mut [u8; PAGE_SIZE]) -> &mut Self {
        unsafe { &mut *(page.as_mut_ptr() as *mut PageHeader) }
    }
}

/// 读取 page 类型.
pub fn page_type(page: &[u8]) -> PageType {
    PageType::from_byte(page[4]).unwrap_or(PageType::Meta)
}

/// 读取 page 关联的 vpid.
pub fn page_vpid(page: &[u8]) -> crate::Vpid {
    u64::from_le_bytes(page[0x18..0x20].try_into().unwrap())
}

/// 设置 page 关联的 vpid.
pub fn page_set_vpid(page: &mut [u8], vpid: crate::Vpid) {
    page[0x18..0x20].copy_from_slice(&vpid.to_le_bytes());
}

/// 读取 item 数量.
pub fn page_key_count(page: &[u8]) -> u16 {
    u16::from_le_bytes(page[0x06..0x08].try_into().unwrap())
}

/// 设置 item 数量.
pub fn page_set_key_count(page: &mut [u8], n: u16) {
    page[0x06..0x08].copy_from_slice(&n.to_le_bytes());
}

/// 读取 item 区的当前末尾偏移 (相对 page 起始).
pub fn page_free_off(page: &[u8]) -> u16 {
    u16::from_le_bytes(page[0x08..0x0A].try_into().unwrap())
}

pub fn page_set_free_off(page: &mut [u8], off: u16) {
    page[0x08..0x0A].copy_from_slice(&off.to_le_bytes());
}

/// 读取 page version (COW 版本号).
pub fn page_version(page: &[u8]) -> u32 {
    u32::from_le_bytes(page[0x14..0x18].try_into().unwrap())
}

pub fn page_set_version(page: &mut [u8], v: u32) {
    page[0x14..0x18].copy_from_slice(&v.to_le_bytes());
}

/// 读取 flags 字节.
pub fn page_flags(page: &[u8]) -> u8 {
    page[5]
}

pub fn page_set_flags(page: &mut [u8], flags: u8) {
    page[5] = flags;
}

/// 计算 page 剩余可用空间 (字节).
///
/// = ITEM_AREA_END - ITEM_AREA_START - checkpoint_array_size
pub fn page_free_space(page: &[u8]) -> usize {
    let key_count = page_key_count(page) as usize;
    let free_off = page_free_off(page) as usize;

    // checkpoint header (8) + key_count / 16 个 checkpoint (每 14B), 至少 1 个
    let cp_count = (key_count / 16).max(1);
    let cp_size = 8 + cp_count * crate::checkpoint::CHECKPOINT_SIZE;

    // checkpoint array 起始 = PAGE_SIZE - PAGE_FOOTER_SIZE - cp_size
    let cp_start = CHECKPOINT_AREA_END - cp_size;

    cp_start.saturating_sub(free_off)
}

/// 校验 page magic.
pub fn page_check_magic(page: &[u8]) -> Result<(), PageError> {
    if page[0..4] == PAGE_MAGIC {
        Ok(())
    } else {
        Err(PageError::InvalidHeader)
    }
}

/// 把 page header 初始化 (清零 + 写 magic + type).
pub fn page_init_header(page: &mut [u8; PAGE_SIZE], pt: PageType) {
    page.fill(0);
    page[0..4].copy_from_slice(&PAGE_MAGIC);
    page[4] = pt as u8;
    page_set_free_off(page, PAGE_HEADER_SIZE as u16);
    page_set_key_count(page, 0);
}
