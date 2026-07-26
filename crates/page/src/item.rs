//! Item 编码与解码 (前缀压缩).
//!
//! ## Item 字节布局
//!
//! ```text
//! ┌─ ItemHeader (4B) ─────────────────┐
//! │ shared_prefix_len: u16            │  与上一个 item 的 key 共享前缀
//! │ key_unshared_len:  u16            │  本 item key 的不重合部分长度
//! ├───────────────────────────────────┤
//! │ key_unshared_bytes: [u8; N]       │  N = key_unshared_len
//! │ vint(value_len)                   │  变长整数编码 value 长度
//! │ value_bytes:        [u8; M]       │  M = value_len
//! │ child_vpid:         u64           │  仅 InternalPage, 8B
//! └───────────────────────────────────┘
//! ```
//!
//! ## Item 类型
//!
//! | Kind | value? | child vpid? | 用于 |
//! |---|---|---|---|
//! | `LeafItem`     | ✅ | ❌ | Leaf Page |
//! | `InternalItem` | ❌ | ✅ | Internal Page |
//!
//! **说明**: InternalItem 的 value 字段被复用存 "separator key 的剩余部分".
//! 实际上我们的设计中 separator key 全部编码在 key_unshared_bytes 里,
//! child_vpid 直接放在末尾. value_len 在 InternalItem 里被忽略 (应为 0).

use crate::error::PageError;
use crate::varint::{VARINT_MAX_BYTES, decode_varint, encode_varint, varint_len};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemKind {
    Leaf,
    Internal,
}

/// 解码后的 item (视图, 借用 page buffer).
#[derive(Debug, Clone, Copy)]
pub struct Item<'a> {
    pub shared_prefix_len: u16,
    pub key_unshared_len: u16,
    pub key_unshared: &'a [u8],
    pub value_len: u32,
    pub value: &'a [u8],
    pub child_vpid: u64,
    pub total_len: u16, // item 在 page 中的总字节数
}

impl<'a> Item<'a> {
    /// 仅对 LeafItem: 获取完整 key (用前缀 + 不重合部分).
    /// `prev_key` = 上一 item 的完整 key.
    pub fn full_key(&self, prev_key: &[u8]) -> Vec<u8> {
        let take = (self.shared_prefix_len as usize).min(prev_key.len());
        let mut key = Vec::with_capacity(take + self.key_unshared_len as usize);
        key.extend_from_slice(&prev_key[..take]);
        key.extend_from_slice(self.key_unshared);
        key
    }
}

/// 解码一个 item. 返回 (item, item 在 page 中的字节数).
///
/// **Item 字节布局**:
/// - hdr (4B): shared_prefix_len + key_unshared_len
/// - key_unique (N bytes)
/// - LeafItem: vint(value_len) + value (M bytes)
/// - InternalItem: child_vpid (8 bytes, no value_len prefix)
pub fn decode_item(
    page: &[u8],
    off: usize,
    kind: ItemKind,
) -> Result<(Item<'_>, usize), PageError> {
    if off + 4 > page.len() {
        return Err(PageError::ItemDecode(
            "page too small for item header".into(),
        ));
    }
    let shared_prefix_len = u16::from_le_bytes(page[off..off + 2].try_into().unwrap());
    let key_unshared_len = u16::from_le_bytes(page[off + 2..off + 4].try_into().unwrap());

    let key_off = off + 4;
    let key_end = key_off + key_unshared_len as usize;
    if key_end > page.len() {
        return Err(PageError::ItemDecode("page too small for key".into()));
    }
    let key_unshared = &page[key_off..key_end];

    let (value_len, value, child_vpid, total_len) = match kind {
        ItemKind::Leaf => {
            // vint(value_len) + value bytes
            let (v_len, vint_size) = decode_varint(&page[key_end..])
                .ok_or_else(|| PageError::ItemDecode("bad varint for value_len".into()))?;
            let value_off = key_end + vint_size;
            let value_end = value_off + v_len as usize;
            if value_end > page.len() {
                return Err(PageError::ItemDecode("page too small for value".into()));
            }
            let value = &page[value_off..value_end];
            let total = 4 + key_unshared_len as usize + vint_size + v_len as usize;
            (v_len, value, 0u64, total)
        }
        ItemKind::Internal => {
            // child_vpid (8B) 直接跟随 key_unique
            if key_end + 8 > page.len() {
                return Err(PageError::ItemDecode(
                    "page too small for child_vpid".into(),
                ));
            }
            let child = u64::from_le_bytes(page[key_end..key_end + 8].try_into().unwrap());
            let total = 4 + key_unshared_len as usize + 8;
            (0u32, &[][..], child, total)
        }
    };

    Ok((
        Item {
            shared_prefix_len,
            key_unshared_len,
            key_unshared,
            value_len,
            value,
            child_vpid,
            total_len: total_len as u16,
        },
        total_len,
    ))
}

/// 编码一个 leaf item 到 buf, 返回写入字节数.
///
/// `prev_key` = 上一 item 的完整 key (用于计算 shared_prefix_len).
/// `key`     = 本 item 的完整 key.
pub fn encode_leaf_item(
    buf: &mut [u8],
    prev_key: &[u8],
    key: &[u8],
    value: &[u8],
) -> Result<usize, PageError> {
    // 1. 计算共享前缀
    let shared = common_prefix_len(prev_key, key);
    if shared > u16::MAX as usize {
        return Err(PageError::ItemDecode("key too long for u16 prefix".into()));
    }
    let key_unique = &key[shared..];

    // 2. 编码 header
    buf[0..2].copy_from_slice(&(shared as u16).to_le_bytes());
    buf[2..4].copy_from_slice(&(key_unique.len() as u16).to_le_bytes());

    // 3. key_unique
    let mut pos = 4;
    buf[pos..pos + key_unique.len()].copy_from_slice(key_unique);
    pos += key_unique.len();

    // 4. varint(value_len)
    if value.len() > u32::MAX as usize {
        return Err(PageError::ItemDecode("value too long for u32".into()));
    }
    pos += encode_varint(&mut buf[pos..], value.len() as u32);

    // 5. value
    if pos + value.len() > buf.len() {
        return Err(PageError::PageFull);
    }
    buf[pos..pos + value.len()].copy_from_slice(value);
    pos += value.len();

    Ok(pos)
}

/// 编码一个 internal item 到 buf. child_vpid 在末尾 8 字节.
pub fn encode_internal_item(
    buf: &mut [u8],
    prev_key: &[u8],
    key: &[u8],
    child_vpid: u64,
) -> Result<usize, PageError> {
    let shared = common_prefix_len(prev_key, key);
    if shared > u16::MAX as usize {
        return Err(PageError::ItemDecode("key too long for u16 prefix".into()));
    }
    let key_unique = &key[shared..];

    buf[0..2].copy_from_slice(&(shared as u16).to_le_bytes());
    buf[2..4].copy_from_slice(&(key_unique.len() as u16).to_le_bytes());

    let mut pos = 4;
    buf[pos..pos + key_unique.len()].copy_from_slice(key_unique);
    pos += key_unique.len();

    // InternalItem 没有 value, 直接 child_vpid (8B)
    if pos + 8 > buf.len() {
        return Err(PageError::PageFull);
    }
    buf[pos..pos + 8].copy_from_slice(&child_vpid.to_le_bytes());
    pos += 8;

    Ok(pos)
}

/// 计算 leaf item 编码所需字节数 (不含写入).
pub fn leaf_item_encoded_size(key_unique_len: usize, value_len: usize, vint_extra: usize) -> usize {
    4 + key_unique_len + vint_extra + value_len
}

/// leaf item 在编码后总字节数 (含 varint).
pub fn leaf_item_size(key_unique_len: usize, value_len: usize) -> usize {
    4 + key_unique_len + varint_len(value_len as u32) + value_len
}

/// internal item 编码字节数.
pub fn internal_item_size(key_unique_len: usize) -> usize {
    4 + key_unique_len + 8
}

/// 计算两个 key 的公共前缀长度.
pub fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let mut n = 0;
    while n < a.len() && n < b.len() && a[n] == b[n] {
        n += 1;
    }
    n
}

/// varint(vint_len) 最大字节数 — 避免循环依赖.
pub fn vint_size(value_len: usize) -> usize {
    if value_len < VARINT_MAX_BYTES * 16 {
        1 // 实际多数场景
    } else {
        varint_len(value_len as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_prefix_basic() {
        assert_eq!(common_prefix_len(b"hello", b"help"), 3);
        assert_eq!(common_prefix_len(b"abc", b"abd"), 2);
        assert_eq!(common_prefix_len(b"abc", b"xyz"), 0);
        assert_eq!(common_prefix_len(b"", b"abc"), 0);
        assert_eq!(common_prefix_len(b"abc", b"abc"), 3);
    }

    #[test]
    fn encode_decode_round_trip_leaf() {
        let mut buf = [0u8; 256];
        let prev = b"hello world";
        let key = b"hello there";
        let value = b"some payload data";
        let n = encode_leaf_item(&mut buf, prev, key, value).unwrap();

        let (item, total) = decode_item(&buf, 0, ItemKind::Leaf).unwrap();
        assert_eq!(total, n);
        assert_eq!(item.shared_prefix_len, 6); // "hello "
        assert_eq!(item.key_unshared_len, 5); // "there"
        assert_eq!(item.key_unshared, b"there");
        assert_eq!(item.value, value);
        assert_eq!(item.full_key(prev), key.to_vec());
    }

    #[test]
    fn encode_decode_round_trip_internal() {
        let mut buf = [0u8; 256];
        let prev = b"alpha";
        let key = b"beta";
        let child = 0xDEADBEEF_u64;
        let n = encode_internal_item(&mut buf, prev, key, child).unwrap();

        let (item, total) = decode_item(&buf, 0, ItemKind::Internal).unwrap();
        assert_eq!(total, n);
        assert_eq!(item.child_vpid, child);
        assert_eq!(item.full_key(prev), key.to_vec());
    }
}
