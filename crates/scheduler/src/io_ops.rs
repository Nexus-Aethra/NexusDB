//! 公开 IO async Future: read / write / fsync / close + fixed file 变体.
//!
//! **设计**: 通过 `crate::scheduler::with_current(|s| ...)` 拿 &mut Scheduler,
//! 访问 registry 和 ring. 闭包返回 poll 结果, 不持有 borrow 跨 await.
//!
//! 闭包模式避免直接 `borrow_mut()` 双重借用问题.
//!
//! ## Fixed file 变体 (T18a)
//!
//! `read_fixed` / `write_fixed` / `fsync_fixed` 使用 `io_uring::types::Fixed(slot)`,
//! opcode 构建器自动设置 `IOSQE_FIXED_FILE` 标志.
//! 调用方需先通过 `FdPool::acquire` 获取 slot.

use std::cell::Cell;
use std::future::Future;
use std::io;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::scheduler::with_current;
use crate::scheduler::with_current_slot;

/// 公共逻辑: 已提交过 → 扫 CQE
/// 返回 `Some(code)` 表示有结果; `None` 表示还没到.
macro_rules! poll_cqe {
    ($ud:expr, $cx:expr, $cancel_code:expr) => {{
        with_current(|s| {
            let mut cq = s.ring.completion();
            cq.sync();
            // 也扫 CQ, 把任何还没 mark 的 CQE 都 mark 进 registry (drain_completions 可能还没跑).
            // 然后用 take_result 拿结果.
            while let Some(cqe) = cq.next() {
                let ud = cqe.user_data();
                let result = cqe.result();
                crate::trace!("io_ops CQE ud={} result={}", ud, result);
                s.registry.mark_completed(ud, result);
            }
            drop(cq);
            if let Some(r) = s.registry.take_result($ud) {
                crate::trace!("io_ops take_result(ud={}) → {:?}", $ud, r);
                Some(r)
            } else {
                crate::trace!("io_ops take_result(ud={}) → None, refresh waker", $ud);
                s.registry.refresh_waker($ud, $cx.waker().clone());
                None
            }
        })
        .expect("no current scheduler")
        .map($cancel_code)
    }};
}

/// 公共逻辑: 首次 poll → 注册 + push SQE + submit.
macro_rules! submit_sqe {
    ($entry:expr, $slot_id:expr, $cx:expr) => {{
        let ud = with_current(|s| {
            let ud = s.registry.register($slot_id, $cx.waker().clone());
            let mut sq = s.ring.submission();
            unsafe {
                let _ = sq.push(&$entry.user_data(ud));
            }
            drop(sq);
            s.ring.submit().expect("io_uring submit");
            crate::trace!("io_ops submit_sqe ud={} slot={}", ud, $slot_id);
            ud
        })
        .expect("no current scheduler");
        ud
    }};
}

/// 公开 API: 读 `fd[offset..offset+buf.len()]` 进 `buf`.
pub async fn read(fd: RawFd, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    Read {
        fd,
        buf,
        offset,
        user_data: Cell::new(None),
    }
    .await
}

/// 公开 API: 写 `buf` 到 `fd[offset..]`.
pub async fn write(fd: RawFd, buf: &[u8], offset: u64) -> io::Result<usize> {
    Write {
        fd,
        buf,
        offset,
        user_data: Cell::new(None),
    }
    .await
}

/// 公开 API: fsync(fd).
pub async fn fsync(fd: RawFd) -> io::Result<()> {
    Fsync {
        fd,
        user_data: Cell::new(None),
    }
    .await
}

/// 公开 API: close(fd).
pub async fn close(fd: RawFd) -> io::Result<()> {
    Close {
        fd,
        user_data: Cell::new(None),
    }
    .await
}

// ---------- Fixed file 变体 (T18a) ----------

/// 使用 `IOSQE_FIXED_FILE` 读 `slot[offset..offset+buf.len()]` 进 `buf`.
///
/// `slot` 来自 `FdPool::acquire`. 调用方需确保 slot 已注册到当前 ring.
#[allow(dead_code)]
pub async fn read_fixed(slot: u16, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    ReadFixed {
        slot,
        buf,
        offset,
        user_data: Cell::new(None),
    }
    .await
}

/// 使用 `IOSQE_FIXED_FILE` 写 `buf` 到 `slot[offset..]`.
#[allow(dead_code)]
pub async fn write_fixed(slot: u16, buf: &[u8], offset: u64) -> io::Result<usize> {
    WriteFixed {
        slot,
        buf,
        offset,
        user_data: Cell::new(None),
    }
    .await
}

/// 使用 `IOSQE_FIXED_FILE` fsync(slot).
#[allow(dead_code)]
pub async fn fsync_fixed(slot: u16) -> io::Result<()> {
    FsyncFixed {
        slot,
        user_data: Cell::new(None),
    }
    .await
}

// ---------- Fixed file + Fixed buffer 变体 (T18b) ----------

/// 使用 `ReadFixed` opcode: 固定文件 slot + 固定缓冲区 slot.
///
/// `file_slot`: FdPool slot (from `FdPool::acquire`).
/// `buf_slot`: RegisteredBufPool slot (from `RegisteredBufPool::alloc`).
/// `buf_ptr`: 缓冲区指针 (必须与 buf_slot 指向同一内存).
/// `len`: 读取长度.
/// `offset`: 文件偏移.
#[allow(dead_code)]
pub async fn read_fixed_buf(
    file_slot: u16,
    buf_slot: u16,
    buf_ptr: *mut u8,
    len: u32,
    offset: u64,
) -> io::Result<usize> {
    ReadFixedBuf {
        file_slot,
        buf_slot,
        buf_ptr,
        len,
        offset,
        user_data: Cell::new(None),
    }
    .await
}

/// 使用 `WriteFixed` opcode: 固定文件 slot + 固定缓冲区 slot.
#[allow(dead_code)]
pub async fn write_fixed_buf(
    file_slot: u16,
    buf_slot: u16,
    buf_ptr: *const u8,
    len: u32,
    offset: u64,
) -> io::Result<usize> {
    WriteFixedBuf {
        file_slot,
        buf_slot,
        buf_ptr,
        len,
        offset,
        user_data: Cell::new(None),
    }
    .await
}

// ---------- Read ----------

struct Read<'a> {
    fd: RawFd,
    buf: &'a mut [u8],
    offset: u64,
    user_data: Cell<Option<u64>>,
}

impl<'a> Future for Read<'a> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(ud) = this.user_data.get() {
            if let Some(code) = poll_cqe!(ud, cx, map_result_usize) {
                this.user_data.set(None);
                return Poll::Ready(code);
            }
            return Poll::Pending;
        }

        let slot_id = with_current_slot(|id| id).unwrap_or(0);
        let entry = io_uring::opcode::Read::new(
            io_uring::types::Fd(this.fd),
            this.buf.as_mut_ptr(),
            this.buf.len() as u32,
        )
        .offset(this.offset)
        .build();
        let ud = submit_sqe!(entry, slot_id, cx);
        this.user_data.set(Some(ud));
        Poll::Pending
    }
}

// ---------- Write ----------

struct Write<'a> {
    fd: RawFd,
    buf: &'a [u8],
    offset: u64,
    user_data: Cell<Option<u64>>,
}

impl<'a> Future for Write<'a> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(ud) = this.user_data.get() {
            if let Some(code) = poll_cqe!(ud, cx, map_result_usize) {
                this.user_data.set(None);
                return Poll::Ready(code);
            }
            return Poll::Pending;
        }

        let slot_id = with_current_slot(|id| id).unwrap_or(0);
        let entry = io_uring::opcode::Write::new(
            io_uring::types::Fd(this.fd),
            this.buf.as_ptr(),
            this.buf.len() as u32,
        )
        .offset(this.offset)
        .build();
        let ud = submit_sqe!(entry, slot_id, cx);
        this.user_data.set(Some(ud));
        Poll::Pending
    }
}

// ---------- Fsync ----------

struct Fsync {
    fd: RawFd,
    user_data: Cell<Option<u64>>,
}

impl Future for Fsync {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(ud) = this.user_data.get() {
            if let Some(code) = poll_cqe!(ud, cx, map_result_unit) {
                this.user_data.set(None);
                return Poll::Ready(code);
            }
            return Poll::Pending;
        }

        let slot_id = with_current_slot(|id| id).unwrap_or(0);
        let entry = io_uring::opcode::Fsync::new(io_uring::types::Fd(this.fd)).build();
        let ud = submit_sqe!(entry, slot_id, cx);
        this.user_data.set(Some(ud));
        Poll::Pending
    }
}

// ---------- Close ----------

struct Close {
    fd: RawFd,
    user_data: Cell<Option<u64>>,
}

impl Future for Close {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(ud) = this.user_data.get() {
            if let Some(code) = poll_cqe!(ud, cx, map_result_unit) {
                this.user_data.set(None);
                return Poll::Ready(code);
            }
            return Poll::Pending;
        }

        let slot_id = with_current_slot(|id| id).unwrap_or(0);
        let entry = io_uring::opcode::Close::new(io_uring::types::Fd(this.fd)).build();
        let ud = submit_sqe!(entry, slot_id, cx);
        this.user_data.set(Some(ud));
        Poll::Pending
    }
}

// ---------- ReadFixed (T18a) ----------

struct ReadFixed<'a> {
    slot: u16,
    buf: &'a mut [u8],
    offset: u64,
    user_data: Cell<Option<u64>>,
}

impl<'a> Future for ReadFixed<'a> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(ud) = this.user_data.get() {
            if let Some(code) = poll_cqe!(ud, cx, map_result_usize) {
                this.user_data.set(None);
                return Poll::Ready(code);
            }
            return Poll::Pending;
        }

        let slot_id = with_current_slot(|id| id).unwrap_or(0);
        let entry = io_uring::opcode::Read::new(
            io_uring::types::Fixed(this.slot as u32),
            this.buf.as_mut_ptr(),
            this.buf.len() as u32,
        )
        .offset(this.offset)
        .build();
        let ud = submit_sqe!(entry, slot_id, cx);
        this.user_data.set(Some(ud));
        Poll::Pending
    }
}

// ---------- WriteFixed (T18a) ----------

struct WriteFixed<'a> {
    slot: u16,
    buf: &'a [u8],
    offset: u64,
    user_data: Cell<Option<u64>>,
}

impl<'a> Future for WriteFixed<'a> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(ud) = this.user_data.get() {
            if let Some(code) = poll_cqe!(ud, cx, map_result_usize) {
                this.user_data.set(None);
                return Poll::Ready(code);
            }
            return Poll::Pending;
        }

        let slot_id = with_current_slot(|id| id).unwrap_or(0);
        let entry = io_uring::opcode::Write::new(
            io_uring::types::Fixed(this.slot as u32),
            this.buf.as_ptr(),
            this.buf.len() as u32,
        )
        .offset(this.offset)
        .build();
        let ud = submit_sqe!(entry, slot_id, cx);
        this.user_data.set(Some(ud));
        Poll::Pending
    }
}

// ---------- FsyncFixed (T18a) ----------

struct FsyncFixed {
    slot: u16,
    user_data: Cell<Option<u64>>,
}

impl Future for FsyncFixed {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(ud) = this.user_data.get() {
            if let Some(code) = poll_cqe!(ud, cx, map_result_unit) {
                this.user_data.set(None);
                return Poll::Ready(code);
            }
            return Poll::Pending;
        }

        let slot_id = with_current_slot(|id| id).unwrap_or(0);
        let entry = io_uring::opcode::Fsync::new(io_uring::types::Fixed(this.slot as u32)).build();
        let ud = submit_sqe!(entry, slot_id, cx);
        this.user_data.set(Some(ud));
        Poll::Pending
    }
}

// ---------- ReadFixedBuf (T18b) ----------

struct ReadFixedBuf {
    file_slot: u16,
    buf_slot: u16,
    buf_ptr: *mut u8,
    len: u32,
    offset: u64,
    user_data: Cell<Option<u64>>,
}

impl Future for ReadFixedBuf {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(ud) = this.user_data.get() {
            if let Some(code) = poll_cqe!(ud, cx, map_result_usize) {
                this.user_data.set(None);
                return Poll::Ready(code);
            }
            return Poll::Pending;
        }

        let slot_id = with_current_slot(|id| id).unwrap_or(0);
        let entry = io_uring::opcode::ReadFixed::new(
            io_uring::types::Fixed(this.file_slot as u32),
            this.buf_ptr,
            this.len,
            this.buf_slot,
        )
        .offset(this.offset)
        .build();
        let ud = submit_sqe!(entry, slot_id, cx);
        this.user_data.set(Some(ud));
        Poll::Pending
    }
}

// ---------- WriteFixedBuf (T18b) ----------

struct WriteFixedBuf {
    file_slot: u16,
    buf_slot: u16,
    buf_ptr: *const u8,
    len: u32,
    offset: u64,
    user_data: Cell<Option<u64>>,
}

impl Future for WriteFixedBuf {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(ud) = this.user_data.get() {
            if let Some(code) = poll_cqe!(ud, cx, map_result_usize) {
                this.user_data.set(None);
                return Poll::Ready(code);
            }
            return Poll::Pending;
        }

        let slot_id = with_current_slot(|id| id).unwrap_or(0);
        let entry = io_uring::opcode::WriteFixed::new(
            io_uring::types::Fixed(this.file_slot as u32),
            this.buf_ptr,
            this.len,
            this.buf_slot,
        )
        .offset(this.offset)
        .build();
        let ud = submit_sqe!(entry, slot_id, cx);
        this.user_data.set(Some(ud));
        Poll::Pending
    }
}

// ---------- helpers ----------

fn map_result_usize(code: i32) -> io::Result<usize> {
    if code >= 0 {
        Ok(code as usize)
    } else {
        Err(io::Error::from_raw_os_error(-code))
    }
}

fn map_result_unit(code: i32) -> io::Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(-code))
    }
}
