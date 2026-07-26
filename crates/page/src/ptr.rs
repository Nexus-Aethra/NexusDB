//! ItemPtr 游标: 封装 prefix-compress 解码逻辑.
//!
//! ## 设计
//!
//! ItemPtr 是只读游标, 持有:
//! - `page`: 字节缓冲引用
//! - `off`: 当前 item 的字节偏移
//! - `cur_key`: 当前 item 的完整 key (cached)
//! - `cur_n`: 当前 item 字节长度
//!
//! 通过 `next()` 在段内顺序遍历, 自动维护 prev_key 拼接.
//!
//! 两种实现: LeafItemPtr / InternalItemPtr, 区别仅在 value vs child_vpid.

use crate::error::PageError;
use crate::item::{ItemKind, decode_item};

/// ItemPtr 是只读游标, 封装 prefix-compress 解码逻辑.
///
/// 持有 page 引用, 不持有 mut, 不能修改 page.
/// next() 顺序前进, 自动维护 prev_key 拼接.
#[derive(Clone, Debug)]
pub struct ItemPtr<'a> {
    pub page: &'a [u8],
    pub off: usize,
    pub cur_key: Vec<u8>,
    pub cur_n: usize,
}

impl<'a> ItemPtr<'a> {
    /// 从指定字节偏移构造 ptr. 直接 decode 该处 item.
    pub fn new(page: &'a [u8], off: usize) -> Result<Self, PageError> {
        let (item, n) = decode_item(page, off, ItemKind::Leaf)?;
        // 不知道 prev_key (调用方需要), 所以 cur_key 只能用 key_unshared.
        // 但通常我们是从 cp 段首 (shared=0) 或从 next() 来的, 都有 prev_key.
        // 这里 new() 直接从 key_unshared 推 full key — **仅适用于 shared=0 的 item**.
        debug_assert_eq!(
            item.shared_prefix_len, 0,
            "ItemPtr::new called on item with shared_prefix_len={}, must be shared=0 (cp segment head or sentinel)",
            item.shared_prefix_len
        );
        Ok(Self {
            page,
            off,
            cur_key: item.key_unshared.to_vec(),
            cur_n: n,
        })
    }

    /// 当前 item 的完整 key (cached)
    pub fn key(&self) -> &[u8] {
        &self.cur_key
    }

    /// 当前 item 字节长度
    pub fn total_len(&self) -> usize {
        self.cur_n
    }

    /// 当前 item 字节偏移
    pub fn byte_offset(&self) -> usize {
        self.off
    }

    /// 顺序前进到下一个 item. 内部用 cached cur_key 拼接下一个 key.
    /// 返回 None 表示已到 item 区末尾.
    pub fn next(&self, kind: ItemKind) -> Result<Option<ItemPtr<'a>>, PageError> {
        let next_off = self.off + self.cur_n;
        let free_off = crate::header::page_free_off(self.page) as usize;
        if next_off >= free_off {
            return Ok(None);
        }
        let (next_item, next_n) = decode_item(self.page, next_off, kind)?;
        // 用 self.cur_key 作为 prev_key
        let full = next_item.full_key(&self.cur_key);
        Ok(Some(ItemPtr {
            page: self.page,
            off: next_off,
            cur_key: full,
            cur_n: next_n,
        }))
    }
}

/// LeafItemPtr: 与 ItemPtr 相同, 但提供 value() 访问.
#[derive(Clone, Debug)]
pub struct LeafItemPtr<'a> {
    inner: ItemPtr<'a>,
    pub value_len: u32,
    /// 指向 page 中 value 字节的指针 (用于零拷贝访问)
    pub value_ptr: *const u8,
}

// SAFETY: LeafItemPtr 的生命周期与 page 绑定
unsafe impl<'a> Send for LeafItemPtr<'a> {}
unsafe impl<'a> Sync for LeafItemPtr<'a> {}

impl<'a> LeafItemPtr<'a> {
    /// 从指定字节偏移构造 ptr. decode 后缓存 value 指针.
    pub fn new(page: &'a [u8], off: usize) -> Result<Self, PageError> {
        let (item, n) = decode_item(page, off, ItemKind::Leaf)?;
        debug_assert_eq!(
            item.shared_prefix_len, 0,
            "LeafItemPtr::new called on item with shared_prefix_len={}, must be shared=0",
            item.shared_prefix_len
        );
        let value_ptr = item.value.as_ptr();
        Ok(Self {
            value_len: item.value_len,
            value_ptr,
            inner: ItemPtr {
                page,
                off,
                cur_key: item.key_unshared.to_vec(),
                cur_n: n,
            },
        })
    }

    /// 从 cp[i] 段首构造 ptr. 验证 shared=0.
    /// cp_off 由 page_index 给出.
    pub fn create_from_cp(page: &'a [u8], cp_first_item_off: u16) -> Result<Self, PageError> {
        let ptr = Self::new(page, cp_first_item_off as usize)?;
        // Self::new 已经 debug_assert 了 shared=0
        Ok(ptr)
    }

    pub fn key(&self) -> &[u8] {
        self.inner.key()
    }

    pub fn total_len(&self) -> usize {
        self.inner.total_len()
    }

    pub fn byte_offset(&self) -> usize {
        self.inner.byte_offset()
    }

    /// 返回 value 字节 (零拷贝, 引用 page)
    pub fn value(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.value_ptr, self.value_len as usize) }
    }

    /// 顺序前进到下一个 item. 内部用 cached cur_key 拼接下一个 key.
    pub fn next(&self) -> Result<Option<LeafItemPtr<'a>>, PageError> {
        let next_inner = self.inner.next(ItemKind::Leaf)?;
        match next_inner {
            None => Ok(None),
            Some(inner) => {
                let (item, _) = decode_item(inner.page, inner.off, ItemKind::Leaf)?;
                Ok(Some(LeafItemPtr {
                    value_len: item.value_len,
                    value_ptr: item.value.as_ptr(),
                    inner,
                }))
            }
        }
    }
}

/// InternalItemPtr: 提供 child_vpid() 访问.
#[derive(Clone, Debug)]
pub struct InternalItemPtr<'a> {
    inner: ItemPtr<'a>,
    pub child_vpid: u64,
}

impl<'a> InternalItemPtr<'a> {
    pub fn new(page: &'a [u8], off: usize) -> Result<Self, PageError> {
        let (item, n) = decode_item(page, off, ItemKind::Internal)?;
        debug_assert_eq!(
            item.shared_prefix_len, 0,
            "InternalItemPtr::new called on item with shared_prefix_len={}, must be shared=0",
            item.shared_prefix_len
        );
        Ok(Self {
            child_vpid: item.child_vpid,
            inner: ItemPtr {
                page,
                off,
                cur_key: item.key_unshared.to_vec(),
                cur_n: n,
            },
        })
    }

    pub fn create_from_cp(page: &'a [u8], cp_first_item_off: u16) -> Result<Self, PageError> {
        Self::new(page, cp_first_item_off as usize)
    }

    pub fn key(&self) -> &[u8] {
        self.inner.key()
    }

    pub fn total_len(&self) -> usize {
        self.inner.total_len()
    }

    pub fn byte_offset(&self) -> usize {
        self.inner.byte_offset()
    }

    pub fn child_vpid(&self) -> u64 {
        self.child_vpid
    }

    pub fn next(&self) -> Result<Option<InternalItemPtr<'a>>, PageError> {
        let next_inner = self.inner.next(ItemKind::Internal)?;
        match next_inner {
            None => Ok(None),
            Some(inner) => {
                let (item, _) = decode_item(inner.page, inner.off, ItemKind::Internal)?;
                Ok(Some(InternalItemPtr {
                    child_vpid: item.child_vpid,
                    inner,
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::{PAGE_HEADER_SIZE, PAGE_SIZE};
    use crate::item::encode_leaf_item;
    use crate::leaf::leaf_new;

    /// 工具: 写入一个 leaf item 到 page off 处 (shared=0).
    /// 返回写入字节数.
    fn write_leaf_item_at(
        page: &mut [u8; PAGE_SIZE],
        off: usize,
        key: &[u8],
        value: &[u8],
    ) -> usize {
        let mut buf = [0u8; 4096];
        let n = encode_leaf_item(&mut buf, &[], key, value).unwrap();
        page[off..off + n].copy_from_slice(&buf[..n]);
        n
    }

    #[test]
    fn test_leaf_item_ptr_basic() {
        let mut page = leaf_new();
        let n = write_leaf_item_at(&mut page, PAGE_HEADER_SIZE, b"hello", b"world");
        let ptr = LeafItemPtr::new(&page, PAGE_HEADER_SIZE).unwrap();
        assert_eq!(ptr.key(), b"hello");
        assert_eq!(ptr.value(), b"world");
        assert_eq!(ptr.total_len(), n);
        assert_eq!(ptr.byte_offset(), PAGE_HEADER_SIZE);
    }

    #[test]
    fn test_leaf_item_ptr_next_chain() {
        let mut page = leaf_new();
        let mut off = PAGE_HEADER_SIZE;
        let keys: &[&[u8]] = &[b"aaa", b"aab", b"aac", b"bbb"];
        let mut offs = Vec::new();
        for k in keys {
            let n = write_leaf_item_at(&mut page, off, k, b"v");
            offs.push((off, n));
            off += n;
        }
        // 改 free_off 让 next() 能找到
        crate::header::page_set_free_off(&mut page, off as u16);

        let mut ptr = Some(LeafItemPtr::new(&page, offs[0].0).unwrap());
        for (i, k) in keys.iter().enumerate() {
            let p = ptr.as_ref().unwrap();
            assert_eq!(p.key(), *k);
            ptr = p.next().unwrap();
            if i < keys.len() - 1 {
                assert!(ptr.is_some(), "next() should yield Some at i={}", i);
            } else {
                assert!(ptr.is_none(), "next() should yield None at last i");
            }
        }
    }
}
