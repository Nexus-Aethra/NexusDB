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
            // ⭐ 批量提交: 扫描 CQ 前先提交攒下的 SQE, 否则 CQE 永不出现
            // (block_on_io 同步忙等路径不经过驱动循环, 靠这里保证正确性).
            s.flush_sq();
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

/// 公共逻辑: 首次 poll → 注册 + push SQE.
///
/// ⭐ 批量提交 (2026-08): push 后**不立即 submit**, 只置 `sq_pending` 标志.
/// 驱动循环在每轮 Phase C / CQ 扫描前统一 `flush_sq()` 一次 submit 提交全部 —
/// 把 N 次 io_uring_enter 合并为 1 次 (协程 worker 每请求 ~20 次 syscall 的
/// 主瓶颈). 正确性由两条路径兜底:
///   - 驱动循环: `drain_completions_*` 开头 flush_sq.
///   - 同步忙等 (block_on_io 不经过驱动循环): `poll_cqe` 扫描 CQ 前 flush_sq.
///
/// SQ 满时 push 失败 → 立即 sync + submit 腾空间重试.
macro_rules! submit_sqe {
    ($entry:expr, $slot_id:expr, $cx:expr) => {{
        let ud = with_current(|s| {
            let ud = s.registry.register($slot_id, $cx.waker().clone());
            let e = $entry.user_data(ud);
            let mut pushed = false;
            while !pushed {
                let mut sq = s.ring.submission();
                match unsafe { sq.push(&e) } {
                    Ok(()) => {
                        drop(sq);
                        pushed = true;
                    }
                    Err(_) => {
                        // SQ 满: sync tail + submit 腾空间后重试 (io_uring 0.6:
                        // push 失败不会更新 tail, 需显式 sync).
                        sq.sync();
                        drop(sq);
                        s.ring.submit().expect("io_uring submit (sq full flush)");
                    }
                }
            }
            s.mark_sq_pending();
            crate::trace!("io_ops submit_sqe(batch) ud={} slot={}", ud, $slot_id);
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

// ---------- PollFd (协程 worker: 监听 socket / eventfd 可读) ----------

struct PollFd {
    fd: RawFd,
    events: libc::c_short,
    user_data: Cell<Option<u64>>,
}

impl PollFd {
    fn new(fd: RawFd, events: libc::c_short) -> Self {
        Self {
            fd,
            events,
            user_data: Cell::new(None),
        }
    }
}

/// 把 io_uring CQE result (mask) 转成 io::Result<u32>.
fn map_result_poll(r: i32) -> io::Result<u32> {
    if r < 0 {
        Err(io::Error::from_raw_os_error(-r))
    } else {
        Ok(r as u32)
    }
}

impl Future for PollFd {
    type Output = io::Result<u32>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(ud) = this.user_data.get() {
            if let Some(code) = poll_cqe!(ud, cx, map_result_poll) {
                this.user_data.set(None);
                return Poll::Ready(code);
            }
            return Poll::Pending;
        }

        let slot_id = with_current_slot(|id| id).unwrap_or(0);
        // IORING_OP_POLL_ADD: 注册对 fd 的事件监听, 返回触发的事件 mask.
        let entry = io_uring::opcode::PollAdd::new(
            io_uring::types::Fd(this.fd),
            this.events as u32,
        )
        .build();
        let ud = submit_sqe!(entry, slot_id, cx);
        this.user_data.set(Some(ud));
        Poll::Pending
    }
}

/// 公开 API: 等待 fd 上指定的 poll 事件 (如 libc::POLLIN).
/// ⭐ 协程 worker 用: 监听 socket 可读 / reply eventfd 可读, 替代 epoll 的等待.
/// 返回触发的事件 mask. 若 fd 已关闭则返回 error.
pub async fn poll(fd: RawFd, events: libc::c_short) -> io::Result<u32> {
    PollFd {
        fd,
        events,
        user_data: Cell::new(None),
    }
    .await
}

/// ⭐ 组合等待 (协程 worker 多连接优化): socket 可读 (io_uring) 或 被 `unpark` 唤醒.
///
/// 返回: `1` = socket 可读 (POLLIN), `2` = 被 unpark (本协程的 reply 队列有新数据).
///
/// 实现: 同时驱动 `PollFd` (io_uring poll socket) 与 `ParkCurrent` (park 注册
/// waker). 两者任一就绪即返回 — 免 per-conn eventfd 的 syscall (多连接场景瓶颈).
pub async fn select_fd_or_unpark(fd: RawFd) -> io::Result<u8> {
    struct FdOrUnpark {
        fd_poll: PollFd,
        park: crate::park::ParkCurrent,
    }
    impl Future for FdOrUnpark {
        type Output = io::Result<u8>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            // 1. socket 可读?
            match Pin::new(&mut this.fd_poll).poll(cx) {
                Poll::Ready(Ok(mask)) if mask & libc::POLLIN as u32 != 0 => {
                    return Poll::Ready(Ok(1));
                }
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {}
            }
            // 2. 被 unpark? (park 第二次 poll → Ready)
            if let Poll::Ready(()) = Pin::new(&mut this.park).poll(cx) {
                return Poll::Ready(Ok(2));
            }
            Poll::Pending
        }
    }
    FdOrUnpark {
        fd_poll: PollFd::new(fd, libc::POLLIN),
        park: crate::park::ParkCurrent::new(),
    }
    .await
}

// ---------- SelectRead (协程 worker: 同时等待两个 fd 可读) ----------

/// 组合 future: 同时监听 fd1 / fd2 的可读 (POLLIN), 返回哪个先就绪 (1 or 2).
///
/// 内部常驻两个 PollFd (不重建): 每次 poll 重新驱动两者 — 已完成的自动重新 submit,
/// pending 的保持注册. 任一触发时, 其余 PollFd 的残留 CQE 会被 registry 忽略
/// (cancel_slot), 下次 select 重新注册, 无 SQE 累积.
pub struct SelectRead {
    f1: Pin<Box<PollFd>>,
    f2: Pin<Box<PollFd>>,
    done1: bool,
    done2: bool,
}

impl SelectRead {
    pub(crate) fn new(fd1: RawFd, fd2: RawFd) -> Self {
        Self {
            f1: Box::pin(PollFd { fd: fd1, events: libc::POLLIN, user_data: Cell::new(None) }),
            f2: Box::pin(PollFd { fd: fd2, events: libc::POLLIN, user_data: Cell::new(None) }),
            done1: false,
            done2: false,
        }
    }
}

impl Future for SelectRead {
    type Output = io::Result<u8>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // 驱动 fd1
        if !this.done1 {
            match this.f1.as_mut().poll(cx) {
                Poll::Ready(Ok(mask)) if mask & libc::POLLIN as u32 != 0 => {
                    this.done1 = true;
                    return Poll::Ready(Ok(1));
                }
                Poll::Ready(Ok(_)) => {} // POLLIN 未置位 (如 HUP), 继续等
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {}
            }
        }
        // 驱动 fd2
        if !this.done2 {
            match this.f2.as_mut().poll(cx) {
                Poll::Ready(Ok(mask)) if mask & libc::POLLIN as u32 != 0 => {
                    this.done2 = true;
                    return Poll::Ready(Ok(2));
                }
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {}
            }
        }
        Poll::Pending
    }
}

/// 公开 API: 同时等待 fd1 / fd2 可读, 返回哪个先就绪 (1 or 2).
/// ⭐ 协程 worker 用: 连接协程同时监听 socket (fd1) 与 reply eventfd (fd2).
pub async fn select_read(fd1: RawFd, fd2: RawFd) -> io::Result<u8> {
    SelectRead::new(fd1, fd2).await
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
