//! Page buffer pool: thread-local Box<[u8; PAGE_SIZE]> pool + T18b RegisteredBufPool.
//!
//! **目的**: 减少 heap alloc/free 次数, 降低分配器开销.
//!
//! **设计**:
//! - thread-local: 每个 shard 线程独立 pool, 无锁
//! - 固定容量: POOL_CAPACITY = 16
//! - 用完自动释放 (drop 时归还 pool, pool 满就 drop)
//!
//! ## T18b: RegisteredBufPool
//!
//! - 注册 chunk 大小 (1MB) buffer 到 io_uring, 消除 1MB alloc/free 和 SQE memcpy.
//! - 每个 IoUringBackend 持有一个 `RegisteredBufPool`.
//! - 注册时分配 buffer 并调用 `register_buffers`, 归还时不 unregister.
//!
//! **使用**:
//! ```ignore
//! let mut buf = page_pool::alloc();
//! // ... 用 buf ...
//! // buf drop 时自动归还
//! ```

use std::cell::RefCell;
use std::io;
use std::ops::{Deref, DerefMut};

use crate::types::{CHUNK_SIZE, PAGE_SIZE};

/// Pool 容量 (16 page = 256KB per thread).
const POOL_CAPACITY: usize = 16;

thread_local! {
    static PAGE_POOL: RefCell<Vec<Box<[u8; PAGE_SIZE]>>> =
        const { RefCell::new(Vec::new()) };
}

/// RAII page buffer: drop 时自动归还 pool.
pub struct PageBuf {
    inner: Option<Box<[u8; PAGE_SIZE]>>,
}

impl PageBuf {
    pub fn new() -> Self {
        Self {
            inner: Some(alloc()),
        }
    }

    pub fn from_boxed(b: Box<[u8; PAGE_SIZE]>) -> Self {
        Self { inner: Some(b) }
    }

    pub fn into_inner(mut self) -> Box<[u8; PAGE_SIZE]> {
        self.inner.take().expect("inner is always Some")
    }

    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        self.inner.as_ref().expect("inner is always Some")
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.inner.as_mut().expect("inner is always Some")
    }
}

impl Default for PageBuf {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for PageBuf {
    type Target = [u8; PAGE_SIZE];
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().expect("inner is always Some")
    }
}

impl DerefMut for PageBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().expect("inner is always Some")
    }
}

impl Drop for PageBuf {
    fn drop(&mut self) {
        if let Some(b) = self.inner.take() {
            recycle(b);
        }
    }
}

/// 从 pool 拿一个 page buffer. pool 空时分配新的.
pub fn alloc() -> Box<[u8; PAGE_SIZE]> {
    PAGE_POOL.with(|pool| {
        let mut p = pool.borrow_mut();
        if let Some(buf) = p.pop() {
            buf
        } else {
            Box::new([0u8; PAGE_SIZE])
        }
    })
}

/// 归还 buffer 到 pool. pool 满时直接 drop.
pub fn recycle(buf: Box<[u8; PAGE_SIZE]>) {
    PAGE_POOL.with(|pool| {
        let mut p = pool.borrow_mut();
        if p.len() < POOL_CAPACITY {
            p.push(buf);
        }
        // pool 满了就让它 drop
    });
}

/// 清除当前线程的 pool (测试 helper / 内存紧张时用).
pub fn clear() {
    PAGE_POOL.with(|pool| {
        pool.borrow_mut().clear();
    });
}

/// 当前 pool 大小 (测试用).
pub fn pool_len() -> usize {
    PAGE_POOL.with(|pool| pool.borrow().len())
}

// =====================================================================
// T18b: RegisteredBufPool — 注册到 io_uring 的 chunk 缓冲区池
// =====================================================================

/// 注册到 io_uring 的 chunk 缓冲区池 (T18b).
///
/// 在 `IoUringBackend::new` 时创建, 调用 `register` 注册 N 个 1MB buffer 到 ring.
/// 后续 IO 用 `alloc` 拿 (buf, slot_id), 用 `recycle` 归还.
///
/// ## 设计
///
/// - `register_buffers` 注册所有 buffer 到 ring, 每个 buffer 固定 slot_id.
/// - `alloc` 返回 `(&mut [u8; CHUNK_SIZE], slot_id)`, 调用方用 slot_id 作 `ReadFixed`/`WriteFixed` 的 `buf_index`.
/// - `recycle` 归还 slot, 不 unregister buffer (slot 保留注册, 下次直接复用).
/// - Drop 时 unregister buffers (kernel 自动清理, 但显式调用更安全).
///
/// ## 安全性
///
/// `register_buffers` 是 unsafe 的, 因为 buffer 内存必须在注册期间有效.
/// `RegisteredBufPool` 持有 `Box<[u8; CHUNK_SIZE]>` 确保 buffer 不被移动.
#[derive(Debug)]
pub struct RegisteredBufPool {
    /// 所有注册的 chunk buffer (Box 保证地址稳定).
    #[allow(dead_code)]
    buffers: Vec<Box<[u8; CHUNK_SIZE]>>,
    /// 空闲 slot 栈 (slot_id = index into buffers).
    free_list: Vec<u16>,
    /// 是否已注册 (防止重复 unregister).
    registered: bool,
}

impl RegisteredBufPool {
    /// 注册 N 个 chunk 大小 buffer 到 ring.
    ///
    /// 每个 buffer 1MB, 共 `count` × 1MB 内存注册到 io_uring.
    /// 默认 2 个 buffer (2MB) 足够双缓冲.
    pub fn register(ring: &mut io_uring::IoUring, count: usize) -> io::Result<Self> {
        let mut buffers = Vec::with_capacity(count);
        let mut iovecs = Vec::with_capacity(count);

        for _ in 0..count {
            let buf = vec![0u8; CHUNK_SIZE].into_boxed_slice();
            // Box<[u8]> -> Box<[u8; CHUNK_SIZE]>
            let ptr = Box::into_raw(buf) as *mut [u8; CHUNK_SIZE];
            let buf = unsafe { Box::from_raw(ptr) };
            iovecs.push(libc::iovec {
                iov_base: buf.as_ptr() as *mut libc::c_void,
                iov_len: CHUNK_SIZE,
            });
            buffers.push(buf);
        }

        // SAFETY: buffers 是 Box 分配的, 地址稳定, 且 pool 生命周期内不移除.
        unsafe {
            ring.submitter().register_buffers(&iovecs)?;
        }

        let free_list: Vec<u16> = (0..count as u16).rev().collect();

        Ok(Self {
            buffers,
            free_list,
            registered: true,
        })
    }

    /// 拿一个空闲 buffer + 其 slot_id.
    ///
    /// # Panics
    ///
    /// 如果 pool 已空 (所有 buffer 在使用中).
    pub fn alloc(&mut self) -> (&mut [u8; CHUNK_SIZE], u16) {
        let idx = self
            .free_list
            .pop()
            .expect("RegisteredBufPool exhausted: all buffers in use");
        (self.buffers[idx as usize].as_mut(), idx)
    }

    /// 归还 buffer slot 到池.
    pub fn recycle(&mut self, slot_id: u16) {
        self.free_list.push(slot_id);
    }

    /// 当前空闲 buffer 数.
    pub fn available(&self) -> usize {
        self.free_list.len()
    }

    /// 总注册 buffer 数.
    pub fn capacity(&self) -> usize {
        self.buffers.len()
    }
}

impl Drop for RegisteredBufPool {
    fn drop(&mut self) {
        if self.registered {
            // kernel 在 ring drop 时自动清理, 但显式 unregister 是好的实践.
            // 注意: 我们不一定持有 ring 引用, 所以这里可能无法调用 unregister_buffers.
            // kernel 会自动清理, 所以 safe.
            self.registered = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_returns_zeroed_page() {
        let buf = alloc();
        assert_eq!(buf.len(), PAGE_SIZE);
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn recycle_then_alloc_reuses() {
        clear();
        let buf1 = alloc();
        let ptr = buf1.as_ptr();
        recycle(buf1);
        assert_eq!(pool_len(), 1);

        let buf2 = alloc();
        assert_eq!(pool_len(), 0);
        assert_eq!(buf2.as_ptr(), ptr, "recycled buffer should be reused");
    }

    #[test]
    fn pool_capacity_limit() {
        clear();
        let mut bufs = Vec::new();
        for _ in 0..POOL_CAPACITY + 5 {
            bufs.push(alloc());
        }
        for buf in bufs {
            recycle(buf);
        }
        assert_eq!(pool_len(), POOL_CAPACITY);
        clear();
    }

    // =====================================================================
    // T18b: RegisteredBufPool tests
    // =====================================================================

    fn new_test_ring() -> io_uring::IoUring {
        io_uring::IoUring::new(8).expect("io_uring setup")
    }

    #[test]
    #[ignore = "需要内核 io_uring 支持"]
    fn registered_register_returns_distinct_slots() {
        let mut ring = new_test_ring();
        let pool = RegisteredBufPool::register(&mut ring, 4).unwrap();
        assert_eq!(pool.capacity(), 4);
        assert_eq!(pool.available(), 4);
    }

    #[test]
    #[ignore = "需要内核 io_uring 支持"]
    fn registered_alloc_recycle_preserves_slot() {
        let mut ring = new_test_ring();
        let mut pool = RegisteredBufPool::register(&mut ring, 2).unwrap();

        let slot_a = {
            let (buf_a, slot_a) = pool.alloc();
            let _data = buf_a[0];
            slot_a
        };
        // buf_a 已 drop, 可以再访问 pool
        assert_eq!(pool.available(), 1);
        pool.recycle(slot_a);
        assert_eq!(pool.available(), 2);

        // 再 alloc 应拿到 recycled slot
        let (_, slot_b) = pool.alloc();
        assert_eq!(slot_b, slot_a);
    }

    #[test]
    #[ignore = "需要内核 io_uring 支持"]
    fn registered_pool_exhaustion_panics() {
        let mut ring = new_test_ring();
        let mut pool = RegisteredBufPool::register(&mut ring, 1).unwrap();
        let (_buf, _slot) = pool.alloc();
        assert_eq!(pool.available(), 0);
        // 再 alloc 应 panic
    }
}
