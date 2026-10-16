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
    let mut p = crate::page_pool::alloc_zeroed();
    p[0..4].copy_from_slice(b"LCBP");
    p[4] = page_type as u8;
    p[0x14..0x18].copy_from_slice(&1u32.to_le_bytes()); // version
    p
}

/// ⭐ 写大 value: 切片 → 数据页 → index 页, 返回 13B 描述符.
///
/// 顺序: 数据页先落 (逐页 `pager.create`, 每页一个 batch — 1MB 数据本就
/// 分页写, 无跨页原子性需求), index 页最后; leaf item 由 caller 随后提交.
///
/// ⚠️ 中途失败 (磁盘满 / IO 错误) 会留下**孤儿页**: 已分配但永远不会被 leaf 引用,
/// 白占空间且让 liveness 计数偏高. 故这里做失败回滚 —— 每成功一页就记下 vpid,
/// 任一步出错先把已建页全部释放再返回错误.
/// (进程级 crash 的孤儿页由 GC 的死页回收兜底, 见 plan 取舍记录.)
pub async fn write_overflow(pager: &mut Pager, value: &[u8]) -> io::Result<[u8; DESCRIPTOR_LEN]> {
    if value.len() > MAX_OVERFLOW_VALUE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("value too large: {} > {}", value.len(), MAX_OVERFLOW_VALUE),
        ));
    }

    // 1. 数据页
    let mut data_vpids: Vec<u64> =
        Vec::with_capacity(value.len().div_ceil(OVERFLOW_PAYLOAD_PER_PAGE));
    for slice in value.chunks(OVERFLOW_PAYLOAD_PER_PAGE) {
        let mut p = blank_page(PageType::Overflow);
        p[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + slice.len()].copy_from_slice(slice);
        match pager.create(p).await {
            Ok(v) => data_vpids.push(v),
            Err(e) => {
                // 回滚: 已建的数据页此刻还没有任何引用, 释放是安全的.
                for v in &data_vpids {
                    pager.free_overflow_vpid(*v);
                }
                return Err(e);
            }
        }
    }

    // 2. index 页: count + vpid 数组
    // MAX_OVERFLOW_VALUE 把 count 限在 ~65; 这里再硬校验一次, 防止将来调大上限
    // 常量后无声越界写页 (段长超页容量 → index 页自身损坏).
    if data_vpids.len() > MAX_INDEX_ENTRIES {
        for v in &data_vpids {
            pager.free_overflow_vpid(*v);
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "overflow chain too long: {} pages > index capacity {MAX_INDEX_ENTRIES}",
                data_vpids.len()
            ),
        ));
    }
    let mut idx_page = blank_page(PageType::OverflowIndex);
    idx_page[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 2]
        .copy_from_slice(&(data_vpids.len() as u16).to_le_bytes());
    for (i, v) in data_vpids.iter().enumerate() {
        let off = PAGE_HEADER_SIZE + 2 + i * 8;
        idx_page[off..off + 8].copy_from_slice(&v.to_le_bytes());
    }
    let head_vpid = match pager.create(idx_page).await {
        Ok(v) => v,
        Err(e) => {
            for v in &data_vpids {
                pager.free_overflow_vpid(*v);
            }
            return Err(e);
        }
    };

    Ok(encode_descriptor(head_vpid, value.len() as u32))
}

/// index 页内可容纳的最大 vpid 条数: count 字段 2B + 每项 8B, 页尾无保留区.
pub const MAX_INDEX_ENTRIES: usize = (PAGE_SIZE - PAGE_HEADER_SIZE - 2) / 8;

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
    // ⚠️ 磁盘数据不可信: 损坏页的 count 可能是任意 u16 (最大 65535), 而页只能装
    // 2042 项。旧实现直接按 count 切片取 8 字节 → 越界 panic (slice index out of
    // range), 把"数据损坏"变成"进程崩溃". 必须先验容量再返回 InvalidData.
    if count > MAX_INDEX_ENTRIES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "overflow index page corrupt: count={count} exceeds capacity {MAX_INDEX_ENTRIES}"
            ),
        ));
    }
    let mut vpids = Vec::with_capacity(count);
    for i in 0..count {
        let off = PAGE_HEADER_SIZE + 2 + i * 8;
        // 再逐项 checked 一次: 即使 count 合法也不假设布局永不变.
        let bytes: [u8; 8] = idx_page
            .get(off..off + 8)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("overflow index page truncated at entry {i}"),
                )
            })?;
        vpids.push(u64::from_le_bytes(bytes));
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
    // ⚠️ total_len 同样来自磁盘描述符 (u32, 上限 4GiB). 不校验就 with_capacity
    // 会按损坏值预分配巨量内存 (可致 OOM abort, 连错误都来不及返回).
    if total_len as usize > MAX_OVERFLOW_VALUE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "overflow descriptor corrupt: total_len={total_len} exceeds max {MAX_OVERFLOW_VALUE}"
            ),
        ));
    }
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
    fn corrupt_index_count_is_invalid_data_not_panic() {
        // count 字段写满 u16 —— 远超页容量. 修复前这里会按 count 切片取 8B,
        // 直接 panic (slice index out of range): 数据损坏升级成进程崩溃.
        let mut page = blank_page(PageType::OverflowIndex);
        page[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 2].copy_from_slice(&u16::MAX.to_le_bytes());
        let err = parse_index_page(&page).expect_err("corrupt count 应返回 Err");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("exceeds capacity"));
    }

    #[test]
    fn boundary_index_count_accepted() {
        // 恰好装满页容量: 合法, 不应被误拒.
        let mut page = blank_page(PageType::OverflowIndex);
        page[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 2]
            .copy_from_slice(&(MAX_INDEX_ENTRIES as u16).to_le_bytes());
        let v = parse_index_page(&page).expect("边界 count 应被接受");
        assert_eq!(v.len(), MAX_INDEX_ENTRIES);
        // 再多一个就应拒绝
        page[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 2]
            .copy_from_slice(&((MAX_INDEX_ENTRIES + 1) as u16).to_le_bytes());
        assert_eq!(
            parse_index_page(&page).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn wrong_page_type_rejected() {
        let page = blank_page(PageType::Overflow);
        assert_eq!(
            parse_index_page(&page).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
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
