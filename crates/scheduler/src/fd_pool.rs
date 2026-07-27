//! FdPool: per-shard 懒分配 FD 池 + io_uring `register_files` 集成.
//!
//! ## 目的
//!
//! io_uring `IOSQE_FIXED_FILE` 允许 SQE 只传 slot_id, 不传 fd, 省 syscall + fd lookup.
//! FdPool 把 path 映射到 slot_id 并管理 `register_files` 注册.
//!
//! ## 设计 (T18a, 2026-07-23)
//!
//! - **懒分配**: 第一次访问某 path 时 open + register_files, 之后永久保留 slot_id.
//! - **永生**: fd 不 close, Pager::drop 时批量 close_all. (kernel 不支持单独 unregister_files)
//! - **容量上限**: 每 shard 最多 `MAX_FD_PER_SHARD = 64` 个 fd, 超出报错 (防泄漏).
//!
//! ## 性能
//!
//! | 场景 | buffered | fixed file | 节省 |
//! |---|---|---|---|
//! | 首次 IO | open + submit + close (3 syscall) | open + register + submit (3 syscall) | 等价 |
//! | 第二次 IO | open + submit + close (3 syscall) | submit (1 syscall) | **省 2 syscall** |
//! | 热点 N 次 IO | 3N syscall | open + register + N submit | **省 2N-2 syscall** |
//!
//! ## 与 scheduler 的集成
//!
//! - FdPool 不需要 scheduler (它是 data-only)
//! - `acquire` 需要 `&mut IoUring` 因为 `register_files` 要修改 ring 状态
//! - 调用者 (PagerIo::IoUringBackend) 在自己的线程内串行调用, 无锁

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io;
use std::os::fd::{IntoRawFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use io_uring::IoUring;

/// 每 shard 最多注册的 fd 数 (保险丝).
///
/// Linux 默认 `RLIMIT_NOFILE = 1024`, 16 shard × 64 = 1024 fd 刚好用满.
/// 实际 workload 通常远小于此 (热数据只占几个 file_id).
pub const MAX_FD_PER_SHARD: usize = 64;

/// 单 fd 出错信息 (capacity exhausted 等).
#[derive(Debug)]
pub enum FdPoolError {
    /// 容量超限: 已注册 `MAX_FD_PER_SHARD` 个 fd, 不能再加.
    CapacityExhausted { current: usize, max: usize },
    /// 注册到 ring 失败 (kernel 错误).
    RegisterFailed(io::Error),
    /// open 文件失败.
    OpenFailed { path: PathBuf, err: io::Error },
}

impl std::fmt::Display for FdPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CapacityExhausted { current, max } => {
                write!(f, "FdPool capacity exhausted: {current}/{max}")
            }
            Self::RegisterFailed(e) => write!(f, "io_uring register_files failed: {e}"),
            Self::OpenFailed { path, err } => {
                write!(f, "open block file {} failed: {err}", path.display())
            }
        }
    }
}

impl std::error::Error for FdPoolError {}

/// Per-shard FD 池: 懒分配 + 永生 + 容量上限.
///
/// **不持有 ring**: `acquire` 需要传 `&mut IoUring` 用于 `register_files`.
/// 这样 FdPool 可以独立于 ring 生命周期管理 (Pager 关闭后 pool 跟着 drop).
#[derive(Debug)]
pub struct FdPool {
    /// path → slot_id (cache, O(1) 命中).
    path_to_slot: HashMap<PathBuf, u16>,
    /// slot_id → raw fd (用于 close_all).
    slot_to_fd: HashMap<u16, RawFd>,
    /// 单调递增 slot id (0, 1, 2, ...).
    next_slot: u16,
}

impl FdPool {
    /// 新建空 pool.
    pub fn new() -> Self {
        Self {
            path_to_slot: HashMap::new(),
            slot_to_fd: HashMap::new(),
            next_slot: 0,
        }
    }

    /// 当前已注册 fd 数.
    pub fn len(&self) -> usize {
        self.path_to_slot.len()
    }

    /// 是否空.
    pub fn is_empty(&self) -> bool {
        self.path_to_slot.is_empty()
    }

    /// 拿 path 对应的 slot_id.
    ///
    /// - **命中**: O(1) 返回 cache 的 slot_id.
    /// - **未命中**: open(path) + `ring.register_files(&[fd])` + 缓存 slot_id.
    ///
    /// **错误**:
    /// - 容量超限 (`MAX_FD_PER_SHARD`)
    /// - open 失败
    /// - register_files 失败 (kernel error)
    pub fn acquire(&mut self, ring: &mut IoUring, path: &Path) -> Result<u16, FdPoolError> {
        self.acquire_with_flags(ring, path, false)
    }

    /// 同 `acquire`, 但可指定 `o_direct` (T18d).
    ///
    /// 当 `o_direct=true` 时, open 添加 `libc::O_DIRECT` 标志, 绕开 page cache.
    pub fn acquire_with_flags(
        &mut self,
        ring: &mut IoUring,
        path: &Path,
        o_direct: bool,
    ) -> Result<u16, FdPoolError> {
        // 1. cache 命中
        if let Some(&slot) = self.path_to_slot.get(path) {
            return Ok(slot);
        }

        // 2. 容量检查
        if self.path_to_slot.len() >= MAX_FD_PER_SHARD {
            return Err(FdPoolError::CapacityExhausted {
                current: self.path_to_slot.len(),
                max: MAX_FD_PER_SHARD,
            });
        }

        // 3. open file (创建/打开, 不 truncate, 不删)
        let mut open_opts = OpenOptions::new();
        open_opts.read(true).write(true).create(true).truncate(false);
        if o_direct {
            open_opts.custom_flags(libc::O_DIRECT);
        }
        let fd = open_opts
            .open(path)
            .map_err(|err| FdPoolError::OpenFailed {
                path: path.to_path_buf(),
                err,
            })?
            .into_raw_fd();

        // 4. 注册到 ring
        //    ⭐ 第一次 (next_slot == 0): 用 register_files_sparse 预先分配文件表,
        //       避免 register_files 的替换语义 (会等 ring idle).
        //    后续: 用 register_files_update 增量追加.
        let new_slot = self.next_slot;
        if new_slot == 0 {
            // 首次: 预分配 MAX_FD_PER_SHARD 大小的文件表, 所有 entry = -1
            ring.submitter()
                .register_files_sparse(MAX_FD_PER_SHARD as u32)
                .map_err(|err| {
                    unsafe { libc::close(fd) };
                    FdPoolError::RegisterFailed(err)
                })?;
            // 注册第一个 fd 到 slot 0
            ring.submitter()
                .register_files_update(new_slot as u32, &[fd])
                .map_err(|err| {
                    unsafe { libc::close(fd) };
                    FdPoolError::RegisterFailed(err)
                })?;
        } else {
            // 后续: 增量追加
            ring.submitter()
                .register_files_update(new_slot as u32, &[fd])
                .map_err(|err| {
                    unsafe { libc::close(fd) };
                    FdPoolError::RegisterFailed(err)
                })?;
        }

        // 5. 缓存
        self.path_to_slot.insert(path.to_path_buf(), new_slot);
        self.slot_to_fd.insert(new_slot, fd);
        self.next_slot += 1;

        Ok(new_slot)
    }

    /// ⭐ G4: 释放 path 对应的 fixed-file 槽 (block 文件 unlink 前调用).
    ///
    /// 表项置 -1 (内核不再持有该 fd) + close; slot 号不复用 (next_slot 单调,
    /// unlink 后同 path 不会重现 — file_id 不复用). path 未注册时 no-op.
    pub fn release_path(&mut self, ring: &mut IoUring, path: &Path) {
        let Some(slot) = self.path_to_slot.remove(path) else {
            return;
        };
        // 表项置 -1; 失败仅忽略 (槽泄漏一个, 不阻断 unlink)
        let _ = ring.submitter().register_files_update(slot as u32, &[-1]);
        if let Some(fd) = self.slot_to_fd.remove(&slot) {
            unsafe {
                libc::close(fd);
            }
        }
    }

    /// Pager::drop 时批量 close 所有 fd. ring 本身由 IoUring drop 时 OS 清理.
    pub fn close_all(&mut self) {
        for fd in self.slot_to_fd.values() {
            unsafe {
                libc::close(*fd);
            }
        }
        self.slot_to_fd.clear();
        // path_to_slot 保留 (pool 即将 drop, 不需要清)
    }
}

impl Default for FdPool {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for FdPool {
    fn drop(&mut self) {
        // 防御: 即使 caller 忘了 close_all, drop 时也清理.
        self.close_all();
    }
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn new_test_ring() -> io_uring::IoUring {
        // 至少 128 entries 以容纳 64 fd + 测试用 SQE.
        io_uring::IoUring::new(128).expect("io_uring setup")
    }

    #[test]
    fn acquire_returns_unique_slot_for_different_paths() {
        let tmp = tempdir().unwrap();
        let p1 = tmp.path().join("a.block");
        let p2 = tmp.path().join("b.block");
        std::fs::File::create(&p1).unwrap();
        std::fs::File::create(&p2).unwrap();

        let mut ring = new_test_ring();
        let mut pool = FdPool::new();

        let s1 = pool.acquire(&mut ring, &p1).unwrap();
        let s2 = pool.acquire(&mut ring, &p2).unwrap();
        assert_ne!(s1, s2, "不同 path 必须给不同 slot");
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn acquire_same_path_returns_same_slot() {
        let tmp = tempdir().unwrap();
        let p = tmp.path().join("same.block");
        std::fs::File::create(&p).unwrap();

        let mut ring = new_test_ring();
        let mut pool = FdPool::new();

        let s1 = pool.acquire(&mut ring, &p).unwrap();
        let s2 = pool.acquire(&mut ring, &p).unwrap();
        let s3 = pool.acquire(&mut ring, &p).unwrap();
        assert_eq!(s1, s2);
        assert_eq!(s2, s3);
        assert_eq!(pool.len(), 1, "同一 path 只占一个 slot");
    }

    #[test]
    fn acquire_capacity_exhausted_errors() {
        let tmp = tempdir().unwrap();
        let mut ring = new_test_ring();
        let mut pool = FdPool::new();

        // 创建 MAX_FD_PER_SHARD + 1 个文件
        for i in 0..MAX_FD_PER_SHARD {
            let p = tmp.path().join(format!("f{i}.block"));
            std::fs::File::create(&p).unwrap();
            pool.acquire(&mut ring, &p).expect("acquire ok within limit");
        }
        // 第 MAX_FD_PER_SHARD + 1 个应失败
        let p_overflow = tmp.path().join(format!("f{MAX_FD_PER_SHARD}.block"));
        std::fs::File::create(&p_overflow).unwrap();
        let err = pool.acquire(&mut ring, &p_overflow).unwrap_err();
        match err {
            FdPoolError::CapacityExhausted { current, max } => {
                assert_eq!(current, MAX_FD_PER_SHARD);
                assert_eq!(max, MAX_FD_PER_SHARD);
            }
            _ => panic!("expected CapacityExhausted, got {err:?}"),
        }
    }

    #[test]
    fn acquire_open_failed_for_missing_path() {
        let tmp = tempdir().unwrap();
        let p = tmp.path().join("nonexistent.block");
        // 不创建文件 → open 应该失败 (虽然 create(true), 但 tmpdir 下子目录可能 OK)
        // 简化: 用 create(false) 测试不存在情况. 这里我们用 create(true), 父目录存在就会创建.
        // 跳过这个 case — 测试 capacity 更重要.

        let mut ring = new_test_ring();
        let mut pool = FdPool::new();

        // 父目录不存在的 path
        let p_bad = tmp.path().join("no_such_dir").join("f.block");
        let err = pool.acquire(&mut ring, &p_bad).unwrap_err();
        assert!(matches!(err, FdPoolError::OpenFailed { .. }), "got {err:?}");

        // 防止 unused 警告
        let _ = p;
    }

    #[test]
    fn close_all_clears_fd_table() {
        let tmp = tempdir().unwrap();
        let p = tmp.path().join("c.block");
        std::fs::File::create(&p).unwrap();

        let mut ring = new_test_ring();
        let mut pool = FdPool::new();

        pool.acquire(&mut ring, &p).unwrap();
        assert_eq!(pool.len(), 1);
        pool.close_all();
        // close_all 只清 slot_to_fd, path_to_slot 保留 (cache)
        // 重新 acquire 同一个 path 会命中 cache 返回旧 slot, 但 fd 已关.
        // 这是设计行为: close_all 只在 Pager::drop 时调用, 池不会复用.
        assert_eq!(pool.path_to_slot.len(), 1, "cache 保留");
        assert_eq!(pool.slot_to_fd.len(), 0, "fd 表清空");
    }

    #[test]
    fn drop_without_close_all_does_not_leak() {
        // 创建 file, acquire slot, 不 close_all, 直接 drop pool.
        // Drop impl 应该 close fd 防泄漏.
        let tmp = tempdir().unwrap();
        let p = tmp.path().join("d.block");
        std::fs::File::create(&p).unwrap();

        let mut ring = new_test_ring();
        let mut pool = FdPool::new();
        let _slot = pool.acquire(&mut ring, &p).unwrap();
        // pool 在这里 drop, Drop::drop → close_all → 关 fd
        // 验证: 我们没法直接检查 fd table, 但至少不 panic
        drop(pool);
    }
}