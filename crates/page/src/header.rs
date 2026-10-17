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
    /// ⭐ 大 value 溢出数据页: header + 原始 payload 切片 (storage 层管理).
    Overflow = 4,
    /// ⭐ 大 value 溢出索引页: header + count + vpid 数组 (storage 层管理).
    OverflowIndex = 5,
}

impl PageType {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(PageType::Meta),
            2 => Some(PageType::Internal),
            3 => Some(PageType::Leaf),
            4 => Some(PageType::Overflow),
            5 => Some(PageType::OverflowIndex),
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

/// ⭐ 审计 P2 (2026-10-17): 页校验和字段偏移 (header 内 8B).
const CHECKSUM_OFF: usize = 0x0C;
const CHECKSUM_LEN: usize = 8;

/// 计算页校验和: 覆盖除 checksum 字段本身 (`0x0C..0x14`) 外的全部页字节.
///
/// **非加密用途** (检测磁盘损坏/撕裂写), 8 字节一块 multiply-xor + 末轮混淆 —
/// 对随机位翻转的检出率接近 1-2^-64, 对恶意构造无抵抗能力 (不需要).
/// 不引第三方哈希依赖 (xxhash 等): 页校验和的成本必须在亚微秒级, 且算法要
/// 能在本 crate 内自证正确 (单测锁定).
pub fn page_checksum(page: &[u8]) -> u64 {
    const M: u64 = 0x0000_0100_0000_01b3; // FNV-1a 64 素数
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut i = 0usize;
    while i < page.len() {
        if i == CHECKSUM_OFF {
            // 跳过 checksum 字段本身 (否则自指)
            i = CHECKSUM_OFF + CHECKSUM_LEN;
            continue;
        }
        let end = (i + 8).min(page.len());
        // 块跨越 checksum 字段时只吃到字段边界 (i < CHECKSUM_OFF 才可能跨越)
        let take = if i < CHECKSUM_OFF && end > CHECKSUM_OFF {
            CHECKSUM_OFF - i
        } else {
            end - i
        };
        let mut buf = [0u8; 8];
        buf[..take].copy_from_slice(&page[i..i + take]);
        let k = u64::from_le_bytes(buf);
        h = (h ^ k).wrapping_mul(M);
        i += take;
    }
    // 末轮混淆 (avalanching)
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h
}

/// 写入页校验和 (在所有页字节定稿后调用).
pub fn page_set_checksum(page: &mut [u8]) {
    let c = page_checksum(page);
    page[CHECKSUM_OFF..CHECKSUM_OFF + CHECKSUM_LEN].copy_from_slice(&c.to_le_bytes());
}

/// 校验页校验和. 返回 `PageError::ChecksumMismatch` 表示页已损坏.
///
/// **兼容规则**: 字段为 0 视为"旧版写入的页" (本特性上线前落盘的页没有校验和),
/// 跳过校验 — 否则存量数据会被误判为损坏. 这类页在下次被写入时自动补上校验和.
pub fn page_validate_checksum(page: &[u8]) -> Result<(), PageError> {
    let stored = u64::from_le_bytes(
        page[CHECKSUM_OFF..CHECKSUM_OFF + CHECKSUM_LEN]
            .try_into()
            .expect("8B checksum"),
    );
    if stored == 0 {
        return Ok(());
    }
    if stored != page_checksum(page) {
        return Err(PageError::ChecksumMismatch);
    }
    Ok(())
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

    // ⚠️ 段数取两个来源的**较大值**:
    //   - 页头 checkpoint_count: 写入方记录的精确段数 (权威)
    //   - needed_checkpoint_count(key_count) = ceil(k/32): 每段 ≤32 故这是段数下界
    // 取大值保证: 页头尚未随扩段更新时不会**高估**可用空间。
    // 旧实现在此处另写了一份 `key_count / 16` 的估算, 与权威算法差最多 2 倍
    // (页越满偏得越多), 且 `.max(1)` 在空页上凭空多算一个 checkpoint。
    let (hdr, _) = crate::checkpoint::read_checkpoint_header(page);
    let seg_count =
        (hdr.checkpoint_count as usize).max(crate::checkpoint::needed_checkpoint_count(key_count));
    let cp_size = crate::checkpoint::checkpoint_area_size(seg_count);

    // checkpoint array 起始 = CHECKPOINT_AREA_END - cp_size
    let cp_start = CHECKPOINT_AREA_END.saturating_sub(cp_size);

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

#[cfg(test)]
mod tests {
    use super::*;

    /// ⭐ 审计 P2 (2026-10-17): 校验和 round-trip + 单位翻转检出 + 旧版页兼容.
    #[test]
    fn page_checksum_detects_corruption_and_tolerates_legacy() {
        let mut page = [0u8; PAGE_SIZE];
        page_init_header(&mut page, PageType::Leaf);
        page[0x100..0x10B].copy_from_slice(b"hello world");
        // 未写校验和 (=0) → 视为旧版页, 放行
        assert!(
            page_validate_checksum(&page).is_ok(),
            "旧版页 (校验和=0) 必须放行"
        );

        page_set_checksum(&mut page);
        assert!(
            page_validate_checksum(&page).is_ok(),
            "刚写入的校验和必须通过"
        );

        // 任意单位置翻转 → 必须检出
        for &off in &[0usize, 4, 0x0A, 0x14, 0x18, 0x100, PAGE_SIZE - 1] {
            let mut corrupt = page;
            corrupt[off] ^= 0x01;
            assert!(
                matches!(
                    page_validate_checksum(&corrupt),
                    Err(PageError::ChecksumMismatch)
                ),
                "offset {off} 的位翻转必须被检出"
            );
        }

        // 校验和字段自身被破坏同样检出 (存的值 != 重算值)
        let mut bad_sum = page;
        bad_sum[0x0C] ^= 0xFF;
        assert!(
            page_validate_checksum(&bad_sum).is_err(),
            "校验和字段损坏必须检出"
        );

        // 全零页 (未初始化) 不得误报
        let zero = [0u8; PAGE_SIZE];
        assert!(page_validate_checksum(&zero).is_ok(), "全零页 = 旧版页");
    }

    /// 校验和必须覆盖**除自身外**的全部字节 (改 vpid / key_count / item 区都算).
    #[test]
    fn page_checksum_covers_whole_page() {
        let mut a = [0u8; PAGE_SIZE];
        page_init_header(&mut a, PageType::Leaf);
        page_set_checksum(&mut a);
        let base = page_checksum(&a);

        let mut b = a;
        page_set_vpid(&mut b, 12345);
        assert_ne!(page_checksum(&b), base, "vpid 变更必须改变校验和");

        let mut c = a;
        page_set_key_count(&mut c, 7);
        assert_ne!(page_checksum(&c), base, "key_count 变更必须改变校验和");

        let mut d = a;
        d[PAGE_SIZE - 1] = 0xAB;
        assert_ne!(page_checksum(&d), base, "item 区字节变更必须改变校验和");
    }
}
