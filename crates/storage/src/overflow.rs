//! ⭐ 大 value 溢出页存储 (~1MB).
//!
//! 设计 (2026-07-24, 大 value plan):
//! - **inline/indirect 二态**: 小 value 原样存 leaf item (现状零变化);
//!   大 value 切成 16KB 溢出页存独立 vpid, leaf item 只存 13B 间接描述符.
//! - **间接标记免冲突**: 网络门面写入的 value 首字节是 value_codec tag
//!   (0x01-0x05), `0x00` 永不出现 → 用 0x00 作描述符标记, 存量数据零迁移.
//! - **标准页头**: 溢出页带完整 LCBP header (magic + type + vpid@0x18) —
//!   recover 扫描 / compact 判活 / B-drain 搬运零改动兼容.
//! - **防泄漏 (修改/删除)**: 覆盖写与删除必须 `free_overflow` 旧链 —
//!   逐 vpid 活性递减 + meta 墓碑 (PID_FREED), 空间由 chunk/block GC 收回;
//!   墓碑保证 recover 扫描不会用磁盘残留 header 复活死页.
//!
//! ## 格式
//!
//! ```text
//! leaf item value:
//!   inline:   [原始字节]                                  (首字节 != 0x00)
//!   indirect: [0x00][head_vpid u64 LE][total_len u32 LE]  (13B 描述符)
//!
//! OverflowIndex 页 (head_vpid):
//!   [0..0x28]   标准页头 (page_type = 5)
//!   [0x28..2A]  count u16 LE (数据页数)
//!   [0x2A.. ]   count × vpid u64 LE
//!
//! Overflow 数据页:
//!   [0..0x28]   标准页头 (page_type = 4)
//!   [0x28.. ]   payload 切片 (末页截断)
//! ```

use std::io;

use page::{PAGE_HEADER_SIZE, PAGE_SIZE, PageType};

use crate::pager::Pager;

/// 间接描述符标记字节 (leaf item value 首字节).
/// 网络门面 value 一律带 value_codec tag (0x01+), 0x00 永不冲突.
pub const INDIRECT_MARKER: u8 = 0x00;

/// 间接描述符长度: marker(1) + head_vpid(8) + total_len(4).
pub const DESCRIPTOR_LEN: usize = 13;

/// 每个溢出数据页的净载荷 (16KB - 40B header).
pub const OVERFLOW_PAYLOAD_PER_PAGE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

/// 溢出 value 上限: 1 MiB + 64B 余量 (协议层 payload 上限 1MiB, 网络门面
/// 会附加 value_codec type tag 等封装字节; 单层间接 ~65 数据页, index 页
/// 容量 2000+ 富余).
pub const MAX_OVERFLOW_VALUE: usize = (1 << 20) + 64;

/// inline 阈值: key.len + value.len 超过则走溢出.
/// 与 page item 编码 4096 栈缓冲对齐 (key+value+tag+varint 开销 <= 4060),
/// 留安全余量; 描述符 13B 恒可 inline.
pub const INLINE_LIMIT: usize = 4000;

/// 是否需要走溢出路径.
pub fn needs_overflow(key_len: usize, value_len: usize) -> bool {
    key_len + value_len > INLINE_LIMIT
}

/// stored value 是否为间接描述符.
pub fn is_indirect(stored: &[u8]) -> bool {
    stored.len() == DESCRIPTOR_LEN && stored[0] == INDIRECT_MARKER
}

/// 编码间接描述符.
pub fn encode_descriptor(head_vpid: u64, total_len: u32) -> [u8; DESCRIPTOR_LEN] {
    let mut d = [0u8; DESCRIPTOR_LEN];
    d[0] = INDIRECT_MARKER;
    d[1..9].copy_from_slice(&head_vpid.to_le_bytes());
    d[9..13].copy_from_slice(&total_len.to_le_bytes());
    d
}

/// 解码间接描述符 → (head_vpid, total_len). 非描述符返回 None.
pub fn decode_descriptor(stored: &[u8]) -> Option<(u64, u32)> {
    if !is_indirect(stored) {
        return None;
    }
    let head_vpid = u64::from_le_bytes(stored[1..9].try_into().expect("8B"));
    let total_len = u32::from_le_bytes(stored[9..13].try_into().expect("4B"));
    Some((head_vpid, total_len))
}

/// 构造带标准页头的空页 (vpid 由 PageWriteBatch::submit 写入 0x18).
fn blank_page(page_type: PageType) -> Box<[u8; PAGE_SIZE]> {
    let mut p = Box::new([0u8; PAGE_SIZE]);
    p[0..4].copy_from_slice(b"LCBP");
    p[4] = page_type as u8;
    p[0x14..0x18].copy_from_slice(&1u32.to_le_bytes()); // version
    p
}

/// ⭐ 写大 value: 切片 → 数据页 → index 页, 返回 13B 描述符.
///
/// 顺序: 数据页先落 (逐页 `pager.create`, 每页一个 batch — 1MB 数据本就
/// 分页写, 无跨页原子性需求), index 页最后; leaf item 由 caller 随后提交.
/// crash 半途 → 孤儿页 (leaf 未引用), 见 plan 取舍记录.
pub async fn write_overflow(pager: &mut Pager, value: &[u8]) -> io::Result<[u8; DESCRIPTOR_LEN]> {
    debug_assert!(value.len() <= MAX_OVERFLOW_VALUE, "caller 应先校验上限");
    if value.len() > MAX_OVERFLOW_VALUE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("value too large: {} > {}", value.len(), MAX_OVERFLOW_VALUE),
        ));
    }

    // 1. 数据页
    let mut data_vpids: Vec<u64> = Vec::with_capacity(value.len().div_ceil(OVERFLOW_PAYLOAD_PER_PAGE));
    for slice in value.chunks(OVERFLOW_PAYLOAD_PER_PAGE) {
        let mut p = blank_page(PageType::Overflow);
        p[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + slice.len()].copy_from_slice(slice);
        data_vpids.push(pager.create(p).await?);
    }

    // 2. index 页: count + vpid 数组
    let mut idx_page = blank_page(PageType::OverflowIndex);
    idx_page[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 2]
        .copy_from_slice(&(data_vpids.len() as u16).to_le_bytes());
    for (i, v) in data_vpids.iter().enumerate() {
        let off = PAGE_HEADER_SIZE + 2 + i * 8;
        idx_page[off..off + 8].copy_from_slice(&v.to_le_bytes());
    }
    let head_vpid = pager.create(idx_page).await?;

    Ok(encode_descriptor(head_vpid, value.len() as u32))
}

/// 解析 index 页里的数据页 vpid 列表.
fn parse_index_page(idx_page: &[u8; PAGE_SIZE]) -> io::Result<Vec<u64>> {
    if page::page_type(&idx_page[..]) != PageType::OverflowIndex {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "overflow head page is not OverflowIndex",
        ));
    }
    let count = u16::from_le_bytes(
        idx_page[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 2]
            .try_into()
            .expect("2B"),
    ) as usize;
    let mut vpids = Vec::with_capacity(count);
    for i in 0..count {
        let off = PAGE_HEADER_SIZE + 2 + i * 8;
        vpids.push(u64::from_le_bytes(
            idx_page[off..off + 8].try_into().expect("8B"),
        ));
    }
    Ok(vpids)
}

/// ⭐ 读大 value: 描述符 → index 页 → 逐数据页拼装.
pub async fn read_overflow(pager: &mut Pager, stored: &[u8]) -> io::Result<Vec<u8>> {
    let Some((head_vpid, total_len)) = decode_descriptor(stored) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not an overflow descriptor",
        ));
    };
    let idx_page = pager.read(head_vpid).await?;
    let data_vpids = parse_index_page(&idx_page)?;

    let mut out = Vec::with_capacity(total_len as usize);
    let mut remaining = total_len as usize;
    for v in data_vpids {
        let p = pager.read(v).await?;
        let take = remaining.min(OVERFLOW_PAYLOAD_PER_PAGE);
        out.extend_from_slice(&p[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + take]);
        remaining -= take;
    }
    if remaining != 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("overflow chain short: {remaining} bytes missing"),
        ));
    }
    Ok(out)
}

/// ⭐ 释放大 value 溢出链 (覆盖写/删除时**必调**, 防存储泄漏).
///
/// 对每个数据页 + index 页自身: `Pager::free_overflow_vpid`
/// (活性递减 + meta 墓碑 + 推动 meta 持久化). 幂等: 重复释放 no-op.
/// index 页读失败 (已损坏/已释放) → 仅释放 index vpid 自身, 不阻断.
pub async fn free_overflow(pager: &mut Pager, stored: &[u8]) -> io::Result<()> {
    let Some((head_vpid, _)) = decode_descriptor(stored) else {
        return Ok(()); // inline value: 无链可释放
    };
    if let Ok(idx_page) = pager.read(head_vpid).await
        && let Ok(data_vpids) = parse_index_page(&idx_page)
    {
        for v in data_vpids {
            pager.free_overflow_vpid(v);
        }
    }
    pager.free_overflow_vpid(head_vpid);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_roundtrip() {
        let d = encode_descriptor(0xDEAD_BEEF_1234, 1 << 20);
        assert!(is_indirect(&d));
        assert_eq!(decode_descriptor(&d), Some((0xDEAD_BEEF_1234, 1 << 20)));
    }

    #[test]
    fn inline_values_not_indirect() {
        // 首字节非 0x00
        assert!(!is_indirect(b"\x01hello"));
        // 长度不等于 13
        assert!(!is_indirect(&[0u8; 12]));
        assert!(!is_indirect(&[0u8; 14]));
        assert!(decode_descriptor(b"\x01raw-value").is_none());
    }

    #[test]
    fn threshold_boundary() {
        assert!(!needs_overflow(1024, INLINE_LIMIT - 1024));
        assert!(needs_overflow(1024, INLINE_LIMIT - 1024 + 1));
        assert!(!needs_overflow(0, INLINE_LIMIT));
        assert!(needs_overflow(0, INLINE_LIMIT + 1));
    }

    #[test]
    fn payload_math() {
        // 1MB → 65 页 (64 满页 + 1 截断页), index 容量富余
        let pages = MAX_OVERFLOW_VALUE.div_ceil(OVERFLOW_PAYLOAD_PER_PAGE);
        assert_eq!(pages, 65);
        let index_capacity = (PAGE_SIZE - PAGE_HEADER_SIZE - 2) / 8;
        assert!(pages <= index_capacity);
    }
}
