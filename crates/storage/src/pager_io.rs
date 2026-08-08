//! T17 Pager IO 后端抽象: 纯 async 版本, 全部走协程调度.
//!
//! **设计** (2026-07-21):
//! - `PagerIoBackend` trait 的 3 个方法全部为 `async fn`, 直接 await.
//! - `StdFsBackend` 用同步 std::fs 包 async (单 poll 完成).
//! - `IoUringBackend` 直接调 `scheduler::io_ops::read/write/fsync` (原生协程调度).
//! - 无 daemon 线程, 无 mpsc 通道, 全部在同一 Scheduler 线程上运行.
//! - Pager 持有 `PagerIo` 枚举, 通过 `read_page_chunk` / `write_page_chunk` / `fsync_block`
//!   调用, 切换 backend 不需要改 Pager 主体.
//!
//! **关键约束**: 所有 `PagerIo` 方法必须从 Scheduler 线程调用 (协程上下文).
//! `io_ops::read/write/fsync` 通过 `with_current` 访问 scheduler 的 io_uring.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::FileExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use crate::page_pool::RegisteredBufPool;
use crate::types::{CHUNK_SIZE, IoBackend, IoBackendConfig, PAGE_SIZE, PageKey};

/// ⭐ Pager 异步 IO 抽象 (T17).
///
/// 三方法对应 Pager 的 3 个 IO 点:
/// 1. `read_chunk(path, off)` — 1MB 读
/// 2. `write_chunk(path, off, data)` — 1MB 写 + fsync
/// 3. `fsync(path)` — 单文件 fsync
///
/// 全部 async fn, 在 Scheduler 协程上下文中调用.
///
/// **T17 注**: 内部使用, trait 不会跨 crate 边界公开, 不需要 `Send` bound.
#[allow(async_fn_in_trait)]
pub trait PagerIoBackend: std::fmt::Debug {
    /// 读 1MB chunk 从 `path[off..off+CHUNK_SIZE]`. 返回填充的 vec.
    async fn read_chunk(&self, path: &Path, off: u64) -> io::Result<Vec<u8>>;

    /// Read a single 16KiB page.  Point reads use this to avoid allocating and
    /// copying a whole 1MiB chunk on a clean-cache miss.
    async fn read_page(&self, path: &Path, off: u64) -> io::Result<Vec<u8>>;

    /// 写 1MB chunk 到 `path[off..off+data.len()]`.
    async fn write_chunk(&self, path: &Path, off: u64, data: &[u8]) -> io::Result<()>;

    /// fsync `path`.
    async fn fsync(&self, path: &Path) -> io::Result<()>;

    /// 后端类型标签, 调试用.
    fn name(&self) -> &'static str;
}

// =====================================================================
// StdFs 实现 (同步 IO 包 async, 单 poll 完成)
// =====================================================================

/// 用 `std::fs::File` + `FileExt::read_exact_at` / `write_all_at` / `sync_all` 同步 IO.
#[derive(Debug, Default, Clone, Copy)]
pub struct StdFsBackend;

impl PagerIoBackend for StdFsBackend {
    async fn read_chunk(&self, path: &Path, off: u64) -> io::Result<Vec<u8>> {
        let f = File::open(path)?;
        let mut buf = vec![0u8; CHUNK_SIZE];
        f.read_exact_at(&mut buf, off)?;
        Ok(buf)
    }

    async fn read_page(&self, path: &Path, off: u64) -> io::Result<Vec<u8>> {
        let f = File::open(path)?;
        let mut buf = vec![0u8; PAGE_SIZE];
        f.read_exact_at(&mut buf, off)?;
        Ok(buf)
    }

    async fn write_chunk(&self, path: &Path, off: u64, data: &[u8]) -> io::Result<()> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        f.write_all_at(data, off)?;
        f.sync_all()?;
        Ok(())
    }

    async fn fsync(&self, path: &Path) -> io::Result<()> {
        let f = File::open(path)?;
        f.sync_all()
    }

    fn name(&self) -> &'static str {
        "StdFs"
    }
}

impl StdFsBackend {
    /// ⭐ Phase C: 同文件 N 个 chunk 批量写 + 单次 fsync.
    async fn write_chunks_file_batch(&self, path: &Path, items: &[(u64, &[u8])]) -> io::Result<()> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        for (off, data) in items {
            f.write_all_at(data, *off)?;
        }
        f.sync_all()?;
        Ok(())
    }
}

// =====================================================================
// IoUring 实现 (T17, 直接 await scheduler::io_ops)
// =====================================================================

/// IoUring 后端: 直接调 `scheduler::io_ops::read/write/fsync`.
///
/// 无 daemon 线程, 无 mpsc 通道. 所有操作在 Scheduler 协程上下文中执行.
/// `io_ops::read` 会提交 SQE 到 io_uring, 然后 park 当前协程.
/// CQE 到达后 scheduler unpark 协程, 继续执行.
///
/// **T18a**: 持有 `FdPool` 实现 `IOSQE_FIXED_FILE` 零拷贝.
/// 每个 Pager 创建自己的 `IoUringBackend` (含独立 FdPool).
/// 第一次访问 path 时懒分配 slot, 之后永久复用.
///
/// **T18b**: 持有 `RegisteredBufPool` 实现固定缓冲区 IO.
/// 懒注册: 第一次 IO 时注册 2 个 1MB buffer 到 ring.
#[derive(Debug)]
pub struct IoUringBackend {
    /// Per-Pager FD 池 (T18a). RefCell 提供内部可变性, 因为 `PagerIoBackend` 方法取 `&self`.
    fd_pool: RefCell<scheduler::FdPool>,
    /// T18a: 是否启用 `IOSQE_FIXED_FILE` 优化.
    use_fixed_file: bool,
    /// T18b: 注册缓冲区池 (懒注册, 首次 IO 时初始化).
    buf_pool: RefCell<Option<RegisteredBufPool>>,
    /// T18b: 是否启用 `ReadFixed`/`WriteFixed` 固定缓冲区优化.
    use_fixed_buffer: bool,
    /// T18d: 是否启用 O_DIRECT (绕开 page cache).
    o_direct: bool,
    /// ⭐ Phase C: fd cache (path → File). 消除每 chunk 一次 open+close.
    fd_cache: RefCell<HashMap<PathBuf, File>>,
}

impl IoUringBackend {
    pub fn new(config: IoBackendConfig) -> Self {
        Self {
            fd_pool: RefCell::new(scheduler::FdPool::new()),
            use_fixed_file: config.use_fixed_file,
            buf_pool: RefCell::new(None),
            use_fixed_buffer: config.use_fixed_buffer,
            o_direct: config.o_direct,
            fd_cache: RefCell::new(HashMap::new()),
        }
    }

    /// 分配 O_DIRECT 兼容的 1MB buffer (512B 对齐).
    fn alloc_direct_buffer() -> Vec<u8> {
        use std::alloc::{Layout, alloc};
        let layout = Layout::from_size_align(CHUNK_SIZE, 512).expect("CHUNK_SIZE % 512 == 0");
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        unsafe { Vec::from_raw_parts(ptr, CHUNK_SIZE, CHUNK_SIZE) }
    }

    /// 分配 1MB buffer, o_direct 时对齐到 512B.
    fn alloc_chunk_buffer(&self) -> Vec<u8> {
        if self.o_direct {
            Self::alloc_direct_buffer()
        } else {
            vec![0u8; CHUNK_SIZE]
        }
    }

    /// 打开文件, o_direct 时添加 O_DIRECT flag.
    fn open_file(&self, path: &Path, write: bool) -> io::Result<std::fs::File> {
        let mut opts = std::fs::OpenOptions::new();
        opts.read(true);
        if write {
            opts.write(true).create(true).truncate(false);
        }
        if self.o_direct {
            opts.custom_flags(libc::O_DIRECT);
        }
        opts.open(path)
    }

    /// ⭐ Phase C: fd cache — 同路径复用 fd, 消除每 chunk 一次 open+close
    /// (~70-250μs 阻塞 syscall, 不走 io_uring). File 由 cache 持有,
    /// backend drop 时统一 close. 单 shard 文件数极少 (通常 1-2 个).
    fn cached_raw_fd(&self, path: &Path, write: bool) -> io::Result<RawFd> {
        if let Some(f) = self.fd_cache.borrow().get(path) {
            return Ok(f.as_raw_fd());
        }
        let f = self.open_file(path, write)?;
        let fd = f.as_raw_fd();
        self.fd_cache.borrow_mut().insert(path.to_path_buf(), f);
        Ok(fd)
    }

    /// ⭐ Phase M2: plain fd cache (不加 O_DIRECT). page.mate window 长度
    /// 非 512B 对齐 (末窗截断), O_DIRECT 写会 EINVAL, 必须走普通 fd.
    fn cached_plain_raw_fd(&self, path: &Path) -> io::Result<RawFd> {
        if let Some(f) = self.fd_cache.borrow().get(path) {
            return Ok(f.as_raw_fd());
        }
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let fd = f.as_raw_fd();
        self.fd_cache.borrow_mut().insert(path.to_path_buf(), f);
        Ok(fd)
    }

    /// 获取文件 slot (通用方法, 处理 o_direct).
    fn acquire_file_slot(&self, path: &Path) -> Option<u16> {
        scheduler::with_current(|s| {
            self.fd_pool
                .borrow_mut()
                .acquire_with_flags(s.ring_mut(), path, self.o_direct)
        })
        .and_then(|r| r.ok())
    }

    /// 懒注册缓冲区池 (在 IO 协程上下文中调用).
    fn ensure_buf_pool(&self) {
        if !self.use_fixed_buffer {
            return;
        }
        let mut pool = self.buf_pool.borrow_mut();
        if pool.is_some() {
            return;
        }
        // 在 scheduler 上下文中注册
        scheduler::with_current(|s| {
            let registered = RegisteredBufPool::register(s.ring_mut(), 2)
                .expect("Failed to register buffer pool");
            *pool = Some(registered);
        });
    }
}

impl Default for IoUringBackend {
    fn default() -> Self {
        Self::new(IoBackendConfig::default())
    }
}

impl PagerIoBackend for IoUringBackend {
    async fn read_chunk(&self, path: &Path, off: u64) -> io::Result<Vec<u8>> {
        // 尝试 fixed file + fixed buffer 路径 (T18b)
        if self.use_fixed_file && self.use_fixed_buffer {
            self.ensure_buf_pool();

            if let Some(file_slot) = self.acquire_file_slot(path) {
                // 限定 RefCell borrow 范围, 不跨 await
                let alloc_result = {
                    let mut bp = self.buf_pool.borrow_mut();
                    bp.as_mut().and_then(|pool| {
                        if pool.available() > 0 {
                            let (buf, buf_slot) = pool.alloc();
                            Some((buf.as_mut_ptr(), buf_slot, buf.len()))
                        } else {
                            None
                        }
                    })
                };

                if let Some((buf_ptr, buf_slot, _len)) = alloc_result {
                    let n = scheduler::io_ops::read_fixed_buf(
                        file_slot,
                        buf_slot,
                        buf_ptr,
                        CHUNK_SIZE as u32,
                        off,
                    )
                    .await?;

                    // 归还 buffer 后, 再从 buf_ptr 拷贝数据
                    let result = {
                        let mut bp = self.buf_pool.borrow_mut();
                        if let Some(pool) = bp.as_mut() {
                            let data = unsafe { std::slice::from_raw_parts(buf_ptr, n as usize) };
                            let vec = data.to_vec();
                            pool.recycle(buf_slot);
                            vec
                        } else {
                            vec![0u8; n as usize]
                        }
                    };

                    if n != CHUNK_SIZE {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!("read_fixed_buf short: {} of {}", n, CHUNK_SIZE),
                        ));
                    }
                    return Ok(result);
                }
            }
        }

        // 尝试 fixed file 路径 (T18a)
        let mut buf = self.alloc_chunk_buffer();
        if self.use_fixed_file
            && let Some(slot) = self.acquire_file_slot(path)
        {
            let n = scheduler::io_ops::read_fixed(slot, &mut buf, off).await?;
            if n != CHUNK_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("io_uring read_fixed short: {} of {}", n, CHUNK_SIZE),
                ));
            }
            return Ok(buf);
        }

        // fallback: 普通 fd (O_DIRECT 时会自动添加)
        let f = self.open_file(path, false)?;
        let fd = f.as_raw_fd();
        let n = scheduler::io_ops::read(fd, &mut buf, off).await?;
        drop(f);
        if n != CHUNK_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("io_uring read short: {} of {}", n, CHUNK_SIZE),
            ));
        }

        Ok(buf)
    }

    async fn read_page(&self, path: &Path, off: u64) -> io::Result<Vec<u8>> {
        // The registered buffers are 1MiB and O_DIRECT requires stricter
        // alignment.  Keep those configurations on the established chunk
        // path; ordinary io_uring point reads use a compact 16KiB buffer.
        if self.o_direct {
            let chunk_off = off / CHUNK_SIZE as u64 * CHUNK_SIZE as u64;
            let page_off = (off - chunk_off) as usize;
            let chunk = self.read_chunk(path, chunk_off).await?;
            return Ok(chunk[page_off..page_off + PAGE_SIZE].to_vec());
        }
        let f = self.open_file(path, false)?;
        let mut buf = vec![0u8; PAGE_SIZE];
        let n = scheduler::io_ops::read(f.as_raw_fd(), &mut buf, off).await?;
        if n != PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("io_uring page read short: {} of {}", n, PAGE_SIZE),
            ));
        }
        Ok(buf)
    }

    async fn write_chunk(&self, path: &Path, off: u64, data: &[u8]) -> io::Result<()> {
        // 尝试 fixed file + fixed buffer 路径 (T18b)
        if self.use_fixed_file && self.use_fixed_buffer {
            self.ensure_buf_pool();

            if let Some(file_slot) = self.acquire_file_slot(path) {
                // 限定 RefCell borrow 范围 (alloc + copy), 不跨 await
                let alloc_result = {
                    let mut bp = self.buf_pool.borrow_mut();
                    bp.as_mut().and_then(|pool| {
                        if pool.available() > 0 {
                            let (buf, buf_slot) = pool.alloc();
                            buf[..data.len()].copy_from_slice(data);
                            Some((buf.as_ptr(), buf_slot, data.len()))
                        } else {
                            None
                        }
                    })
                };

                if let Some((buf_ptr, buf_slot, write_len)) = alloc_result {
                    let n = scheduler::io_ops::write_fixed_buf(
                        file_slot,
                        buf_slot,
                        buf_ptr,
                        write_len as u32,
                        off,
                    )
                    .await?;
                    scheduler::io_ops::fsync_fixed(file_slot).await?;

                    let mut bp = self.buf_pool.borrow_mut();
                    if let Some(pool) = bp.as_mut() {
                        pool.recycle(buf_slot);
                    }

                    if n != data.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            format!("write_fixed_buf short: {} of {}", n, data.len()),
                        ));
                    }
                    return Ok(());
                }
            }
        }

        // 尝试 fixed file 路径 (T18a)
        if self.use_fixed_file
            && let Some(slot) = self.acquire_file_slot(path)
        {
            // O_DIRECT 需要 buffer 512B 对齐, 对齐后拷贝
            if self.o_direct {
                let mut aligned = Self::alloc_direct_buffer();
                aligned[..data.len()].copy_from_slice(data);
                let n = scheduler::io_ops::write_fixed(slot, &aligned[..data.len()], off).await?;
                scheduler::io_ops::fsync_fixed(slot).await?;
                if n != data.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        format!(
                            "io_uring write_fixed+odirect short: {} of {}",
                            n,
                            data.len()
                        ),
                    ));
                }
            } else {
                let n = scheduler::io_ops::write_fixed(slot, data, off).await?;
                scheduler::io_ops::fsync_fixed(slot).await?;
                if n != data.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        format!("io_uring write_fixed short: {} of {}", n, data.len()),
                    ));
                }
            }
            return Ok(());
        }

        // fallback: 普通 fd (O_DIRECT 时会自动添加)
        let f = self.open_file(path, true)?;
        let fd = f.as_raw_fd();
        if self.o_direct {
            let mut aligned = Self::alloc_direct_buffer();
            aligned[..data.len()].copy_from_slice(data);
            let n = scheduler::io_ops::write(fd, &aligned[..data.len()], off).await?;
            scheduler::io_ops::fsync(fd).await?;
            drop(f);
            if n != data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!("io_uring write+odirect short: {} of {}", n, data.len()),
                ));
            }
        } else {
            let n = scheduler::io_ops::write(fd, data, off).await?;
            scheduler::io_ops::fsync(fd).await?;
            drop(f);
            if n != data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!("io_uring write short: {} of {}", n, data.len()),
                ));
            }
        }

        Ok(())
    }

    async fn fsync(&self, path: &Path) -> io::Result<()> {
        // 尝试 fixed file 路径 (T18a)
        if self.use_fixed_file
            && let Some(slot) = self.acquire_file_slot(path)
        {
            return scheduler::io_ops::fsync_fixed(slot).await;
        }

        // fallback: 普通 fd (O_DIRECT 时会自动添加)
        let f = self.open_file(path, false)?;
        let fd = f.as_raw_fd();
        scheduler::io_ops::fsync(fd).await?;
        drop(f);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "IoUring"
    }
}

impl IoUringBackend {
    /// ⭐ Phase C: 同文件 N 个 chunk 批量写 + 单次 fsync (长尾对症).
    ///
    /// - fixed file 路径: `write_fixed ×N + fsync_fixed ×1`
    /// - fallback: fd cache (免每 chunk open/close) + `io_ops::write ×N + fsync ×1`
    /// - o_direct: 每个写入拷贝到 512B 对齐 buffer, fsync 仍只一次
    ///
    /// 不走 fixed buffer 路径 (池仅 2 个 1MB buffer, 批量下无法覆盖;
    /// 收益主体在 fsync 合并, 非拷贝消除).
    async fn write_chunks_file_batch(&self, path: &Path, items: &[(u64, &[u8])]) -> io::Result<()> {
        // fixed file 路径 (T18a)
        if self.use_fixed_file
            && let Some(slot) = self.acquire_file_slot(path)
        {
            for (off, data) in items {
                let n = if self.o_direct {
                    let mut aligned = Self::alloc_direct_buffer();
                    aligned[..data.len()].copy_from_slice(data);
                    scheduler::io_ops::write_fixed(slot, &aligned[..data.len()], *off).await?
                } else {
                    scheduler::io_ops::write_fixed(slot, data, *off).await?
                };
                if n != data.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        format!("io_uring batch write_fixed short: {} of {}", n, data.len()),
                    ));
                }
            }
            return scheduler::io_ops::fsync_fixed(slot).await;
        }

        // fallback: fd cache + 普通 fd
        let fd = self.cached_raw_fd(path, true)?;
        for (off, data) in items {
            let n = if self.o_direct {
                let mut aligned = Self::alloc_direct_buffer();
                aligned[..data.len()].copy_from_slice(data);
                scheduler::io_ops::write(fd, &aligned[..data.len()], *off).await?
            } else {
                scheduler::io_ops::write(fd, data, *off).await?
            };
            if n != data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!("io_uring batch write short: {} of {}", n, data.len()),
                ));
            }
        }
        scheduler::io_ops::fsync(fd).await
    }

    /// ⭐ Phase M2: page.mate window 批量写 + 单次 fsync.
    ///
    /// 不走 fixed-file / O_DIRECT / fixed-buffer 路径: mate 末窗长度任意
    /// (非 512B 对齐), 且频率低 (backlog 排空才一批) — plain fd cache +
    /// `io_ops::write ×N + fsync ×1` 已够.
    async fn write_plain_file_batch(&self, path: &Path, items: &[(u64, &[u8])]) -> io::Result<()> {
        let fd = self.cached_plain_raw_fd(path)?;
        for (off, data) in items {
            let n = scheduler::io_ops::write(fd, data, *off).await?;
            if n != data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!("io_uring mate batch write short: {} of {}", n, data.len()),
                ));
            }
        }
        scheduler::io_ops::fsync(fd).await
    }
}

// =====================================================================
// Backend 工厂: 根据 IoBackend enum 选 backend
// =====================================================================

/// ⭐ Backend 枚举, Pager 持有, 调度时 match dispatch.
/// (每 Pager 仅一个实例, 不在热路径上拷贝 — 变体尺寸差异无影响.)
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum PagerIo {
    StdFs(StdFsBackend),
    IoUring(IoUringBackend),
}

impl PagerIo {
    /// 工厂: 根据 `IoBackendConfig` 选 backend.
    pub fn new(config: IoBackendConfig) -> Self {
        match config.backend {
            IoBackend::StdFs => PagerIo::StdFs(StdFsBackend),
            IoBackend::IoUring => PagerIo::IoUring(IoUringBackend::new(config)),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            PagerIo::StdFs(b) => b.name(),
            PagerIo::IoUring(b) => b.name(),
        }
    }

    pub fn is_uring(&self) -> bool {
        matches!(self, PagerIo::IoUring(_))
    }

    /// 路径 helper: caller 传 PageKey, 我们解析 .block path.
    pub fn block_path(&self, block_dir: &Path, file_id: u32) -> PathBuf {
        block_dir.join(format!("{:06}.block", file_id + 1))
    }

    /// 读 chunk by PageKey (async).
    pub async fn read_page_chunk(&self, block_dir: &Path, key: PageKey) -> io::Result<Vec<u8>> {
        let path = self.block_path(block_dir, key.file_id);
        let off = (key.chunk_idx as u64) * CHUNK_SIZE as u64;
        match self {
            PagerIo::StdFs(b) => b.read_chunk(&path, off).await,
            PagerIo::IoUring(b) => b.read_chunk(&path, off).await,
        }
    }

    /// Read one physical page by chunk key and page index.
    pub async fn read_page(
        &self,
        block_dir: &Path,
        key: PageKey,
        page_idx: u8,
    ) -> io::Result<Vec<u8>> {
        let path = self.block_path(block_dir, key.file_id);
        let off = key.chunk_idx as u64 * CHUNK_SIZE as u64 + page_idx as u64 * PAGE_SIZE as u64;
        match self {
            PagerIo::StdFs(b) => b.read_page(&path, off).await,
            PagerIo::IoUring(b) => b.read_page(&path, off).await,
        }
    }

    /// 写 chunk by PageKey (async). write_chunk 内部已 fsync.
    pub async fn write_page_chunk(
        &self,
        block_dir: &Path,
        key: PageKey,
        data: Vec<u8>,
    ) -> io::Result<()> {
        self.write_page_chunk_slice(block_dir, key, &data).await
    }

    /// ⭐ 异步落盘: slice 版写 chunk (caller 保持字节所有权, 供 Rc 共享场景).
    pub async fn write_page_chunk_slice(
        &self,
        block_dir: &Path,
        key: PageKey,
        data: &[u8],
    ) -> io::Result<()> {
        let path = self.block_path(block_dir, key.file_id);
        let off = (key.chunk_idx as u64) * CHUNK_SIZE as u64;
        match self {
            PagerIo::StdFs(b) => b.write_chunk(&path, off, data).await,
            PagerIo::IoUring(b) => b.write_chunk(&path, off, data).await,
        }
    }

    pub async fn fsync_block(&self, block_dir: &Path, file_id: u32) -> io::Result<()> {
        let path = self.block_path(block_dir, file_id);
        match self {
            PagerIo::StdFs(b) => b.fsync(&path).await,
            PagerIo::IoUring(b) => b.fsync(&path).await,
        }
    }

    /// ⭐ Phase C: 批量写 chunk — 同 file 的 N 个 chunk 逐个 write + 单次 fsync.
    ///
    /// items 可跨 file (按 file_id 分组, 每组一次 fsync); 单 shard 通常只有 1 个 file.
    /// fsync 次数: N → distinct-file 数.
    pub async fn write_chunks_batch(
        &self,
        block_dir: &Path,
        items: &[(PageKey, &[u8])],
    ) -> io::Result<()> {
        // 按 file_id 分组, 保持提交顺序
        let mut file_ids: Vec<u32> = Vec::new();
        for (key, _) in items {
            if !file_ids.contains(&key.file_id) {
                file_ids.push(key.file_id);
            }
        }
        for file_id in file_ids {
            let path = self.block_path(block_dir, file_id);
            let group: Vec<(u64, &[u8])> = items
                .iter()
                .filter(|(k, _)| k.file_id == file_id)
                .map(|(k, d)| ((k.chunk_idx as u64) * CHUNK_SIZE as u64, *d))
                .collect();
            match self {
                PagerIo::StdFs(b) => b.write_chunks_file_batch(&path, &group).await?,
                PagerIo::IoUring(b) => b.write_chunks_file_batch(&path, &group).await?,
            }
        }
        Ok(())
    }

    /// ⭐ Phase M2: page.mate dirty window 批量写 — N 个 window 逐个 write +
    /// 单次 fsync. off = window_idx × 1MB, 末窗长度可小于 1MB (截断到水位).
    pub async fn write_mate_windows(
        &self,
        mate_path: &Path,
        items: &[(u32, &[u8])],
    ) -> io::Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let group: Vec<(u64, &[u8])> = items
            .iter()
            .map(|(w, d)| ((*w as u64) * crate::meta_cache::META_WINDOW_SIZE as u64, *d))
            .collect();
        match self {
            PagerIo::StdFs(b) => b.write_chunks_file_batch(mate_path, &group).await,
            // mate 末窗非 512B 对齐, 走 plain fd 批路径 (不受 o_direct 影响)
            PagerIo::IoUring(b) => b.write_plain_file_batch(mate_path, &group).await,
        }
    }

    /// ⭐ G2: 同 chunk 的 N 个 page (16KB 粒度) 批量写 + 单次 fsync.
    ///
    /// compact 死槽填充专用: 只写 dst chunk 的死页槽位, 活页不动 —
    /// crash 半写死槽无害 (meta 未指向). off = chunk×1MB + page×16KB.
    pub async fn write_pages_batch(
        &self,
        block_dir: &Path,
        key: PageKey,
        items: &[(u8, &[u8])], // (page_idx, 16KB page 字节)
    ) -> io::Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let path = self.block_path(block_dir, key.file_id);
        let base = (key.chunk_idx as u64) * CHUNK_SIZE as u64;
        let group: Vec<(u64, &[u8])> = items
            .iter()
            .map(|(p, d)| (base + (*p as u64) * crate::types::PAGE_SIZE as u64, *d))
            .collect();
        match self {
            PagerIo::StdFs(b) => b.write_chunks_file_batch(&path, &group).await,
            PagerIo::IoUring(b) => b.write_chunks_file_batch(&path, &group).await,
        }
    }

    /// ⭐ G4: 逐出 path 的全部 fd 缓存 (block 文件 unlink 前必调,
    /// 否则 fd 泄漏且后续写打到已删除的 inode 上).
    ///
    /// - fd_cache: 移除 entry (File drop 即 close)
    /// - FdPool fixed-file 槽: 表项置 -1 + close (需 scheduler 上下文;
    ///   无上下文时仅清 fd_cache, 槽由 close_all 兑底)
    pub fn evict_path(&self, path: &Path) {
        match self {
            PagerIo::StdFs(_) => {}
            PagerIo::IoUring(b) => {
                b.fd_cache.borrow_mut().remove(path);
                let _ = scheduler::with_current(|s| {
                    // 拆借: ring 与 fd_pool 均在 scheduler/backend 内部
                    b.fd_pool.borrow_mut().release_path(s.ring_mut(), path);
                });
            }
        }
    }
}

impl Default for PagerIo {
    fn default() -> Self {
        PagerIo::StdFs(StdFsBackend)
    }
}

// =====================================================================
// Unit tests
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// StdFs backend 端到端: 写 1MB chunk, 读回, 校验内容.
    #[test]
    fn stdfs_backend_roundtrip() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("000001.block");
        let backend = StdFsBackend;

        let rt = scheduler::SchedHandle::new(scheduler::Scheduler::new());
        rt.set_current();

        // 写一段 1MB 字节
        let mut data = vec![0u8; CHUNK_SIZE];
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = (i % 251) as u8;
        }

        let h = scheduler::spawn_on(&rt, async move {
            backend.write_chunk(&path, 0, &data).await.unwrap();
            // 读回
            let read = backend.read_chunk(&path, 0).await.unwrap();
            assert_eq!(read, data, "readback should match write");

            // offset 写
            let half = vec![0xABu8; CHUNK_SIZE];
            backend
                .write_chunk(&path, CHUNK_SIZE as u64, &half)
                .await
                .unwrap();
            let read2 = backend.read_chunk(&path, CHUNK_SIZE as u64).await.unwrap();
            assert_eq!(read2, half, "second chunk readback should match");

            // fsync 不报错即可
            backend.fsync(&path).await.unwrap();
        });
        assert!(
            rt.clone().drive_until_idle(10_000),
            "scheduler must drain to idle"
        );
        pollster::block_on(h).unwrap();
    }

    /// ⭐ IoUring backend 端到端: 在同一 Scheduler 上跑 io_uring async.
    #[test]
    #[ignore = "需要内核 io_uring 支持; 容器/沙箱可能 hang. 用 --ignored 显式跑"]
    fn iouring_backend_roundtrip() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("000001.block");

        let rt = scheduler::SchedHandle::new(scheduler::Scheduler::new());
        rt.set_current();

        let mut data = vec![0u8; CHUNK_SIZE];
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = (i % 251) as u8;
        }

        let backend = IoUringBackend::default();
        let h = scheduler::spawn_on(&rt, async move {
            backend.write_chunk(&path, 0, &data).await.unwrap();
            // 读回
            let read = backend.read_chunk(&path, 0).await.unwrap();
            assert_eq!(read, data, "io_uring readback should match write");

            // offset 写
            let half = vec![0xCDu8; CHUNK_SIZE / 2];
            backend
                .write_chunk(&path, CHUNK_SIZE as u64, &half)
                .await
                .unwrap();
            let read2 = backend.read_chunk(&path, CHUNK_SIZE as u64).await.unwrap();
            assert_eq!(read2, half, "io_uring second chunk should match");
        });
        assert!(
            rt.clone().drive_until_idle(10_000),
            "scheduler must drain to idle"
        );
        pollster::block_on(h).unwrap();
    }

    /// ⭐ T18a: IoUring backend 使用 `IOSQE_FIXED_FILE` 的端到端测试.
    ///
    /// 验证 `use_fixed_file=true` 时, FdPool + read_fixed/write_fixed/fsync_fixed
    /// 正常工作. 与 `iouring_backend_roundtrip` 的区别: 显式构造 IoUringBackend
    /// 使用 `IoBackendConfig { use_fixed_file: true, .. }`.
    #[test]
    #[ignore = "需要内核 io_uring 支持; 容器/沙箱可能 hang. 用 --ignored 显式跑"]
    fn iouring_backend_fixed_file_roundtrip() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("000001.block");

        let rt = scheduler::SchedHandle::new(scheduler::Scheduler::new());
        rt.set_current();

        let mut data = vec![0u8; CHUNK_SIZE];
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = (i % 251) as u8;
        }

        let config = IoBackendConfig {
            backend: IoBackend::IoUring,
            use_fixed_file: true,
            ..Default::default()
        };
        let backend = IoUringBackend::new(config);
        let h = scheduler::spawn_on(&rt, async move {
            // write + fsync fixed
            backend.write_chunk(&path, 0, &data).await.unwrap();
            // read fixed
            let read = backend.read_chunk(&path, 0).await.unwrap();
            assert_eq!(read, data, "fixed file readback should match write");

            // offset 写
            let half = vec![0xABu8; CHUNK_SIZE / 2];
            backend
                .write_chunk(&path, CHUNK_SIZE as u64, &half)
                .await
                .unwrap();
            let read2 = backend.read_chunk(&path, CHUNK_SIZE as u64).await.unwrap();
            assert_eq!(read2, half, "fixed file second chunk should match");
        });
        assert!(
            rt.clone().drive_until_idle(10_000),
            "scheduler must drain to idle"
        );
        pollster::block_on(h).unwrap();
    }

    /// ⭐ T18b: IoUring backend 使用固定缓冲区 (ReadFixed/WriteFixed) 的端到端测试.
    ///
    /// 验证 `use_fixed_file=true, use_fixed_buffer=true` 时,
    /// RegisteredBufPool + read_fixed_buf/write_fixed_buf 正常工作.
    #[test]
    #[ignore = "需要内核 io_uring 支持; 容器/沙箱可能 hang. 用 --ignored 显式跑"]
    fn iouring_backend_fixed_buf_roundtrip() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("000001.block");

        let rt = scheduler::SchedHandle::new(scheduler::Scheduler::new());
        rt.set_current();

        let mut data = vec![0u8; CHUNK_SIZE];
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = (i % 251) as u8;
        }

        let config = IoBackendConfig {
            backend: IoBackend::IoUring,
            use_fixed_file: true,
            use_fixed_buffer: true,
            ..Default::default()
        };
        let backend = IoUringBackend::new(config);
        let h = scheduler::spawn_on(&rt, async move {
            // write + fsync with fixed buffer
            backend.write_chunk(&path, 0, &data).await.unwrap();
            // read with fixed buffer
            let read = backend.read_chunk(&path, 0).await.unwrap();
            assert_eq!(read, data, "fixed buf readback should match write");

            // offset 写
            let half = vec![0xBCu8; CHUNK_SIZE / 2];
            backend
                .write_chunk(&path, CHUNK_SIZE as u64, &half)
                .await
                .unwrap();
            let read2 = backend.read_chunk(&path, CHUNK_SIZE as u64).await.unwrap();
            assert_eq!(read2, half, "fixed buf second chunk should match");
        });
        assert!(
            rt.clone().drive_until_idle(10_000),
            "scheduler must drain to idle"
        );
        pollster::block_on(h).unwrap();
    }

    /// PagerIo::new 工厂.
    #[test]
    fn pagerio_factory() {
        let p = PagerIo::new(IoBackendConfig::from(IoBackend::IoUring));
        assert!(p.is_uring());
        assert_eq!(p.name(), "IoUring");

        let p2 = PagerIo::new(IoBackendConfig::from(IoBackend::StdFs));
        assert!(!p2.is_uring());
        assert_eq!(p2.name(), "StdFs");
    }

    /// block_path 格式.
    #[test]
    fn block_path_format() {
        let p = PagerIo::StdFs(StdFsBackend);
        let dir = Path::new("/tmp/foo");
        assert_eq!(p.block_path(dir, 0), PathBuf::from("/tmp/foo/000001.block"));
        assert_eq!(p.block_path(dir, 2), PathBuf::from("/tmp/foo/000003.block"));
    }

    /// ⭐ T18d: IoUring backend 使用 O_DIRECT 的端到端测试.
    #[test]
    #[ignore = "需要内核 io_uring 支持; 容器/沙箱可能 hang. 用 --ignored 显式跑"]
    fn iouring_backend_o_direct_roundtrip() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("000001.block");

        let rt = scheduler::SchedHandle::new(scheduler::Scheduler::new());
        rt.set_current();

        let mut data = vec![0u8; CHUNK_SIZE];
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = (i % 251) as u8;
        }

        let config = IoBackendConfig {
            backend: IoBackend::IoUring,
            use_fixed_file: true,
            use_fixed_buffer: false,
            o_direct: true,
            ..Default::default()
        };
        let backend = IoUringBackend::new(config);
        let h = scheduler::spawn_on(&rt, async move {
            // write + fsync with O_DIRECT
            backend.write_chunk(&path, 0, &data).await.unwrap();
            // read with O_DIRECT
            let read = backend.read_chunk(&path, 0).await.unwrap();
            assert_eq!(read, data, "O_DIRECT readback should match write");

            // offset 写
            let half = vec![0xDEu8; CHUNK_SIZE / 2];
            backend
                .write_chunk(&path, CHUNK_SIZE as u64, &half)
                .await
                .unwrap();
            let read2 = backend.read_chunk(&path, CHUNK_SIZE as u64).await.unwrap();
            assert_eq!(read2, half, "O_DIRECT second chunk should match");
        });
        assert!(
            rt.clone().drive_until_idle(10_000),
            "scheduler must drain to idle"
        );
        pollster::block_on(h).unwrap();
    }
}
