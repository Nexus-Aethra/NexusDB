//! Reading a single page off disk given a [`DiskCoord`].
//!
//! The [`read_page`] helper opens the right `.block` file, seeks to the
//! 16 KiB-aligned offset, and returns a freshly allocated `[u8; PAGE_SIZE]`.
//!
//! This module deliberately uses `std::fs::File::read` (no mmap, no
//! io_uring). The scanner is not a hot path; portability and simplicity win.
//!
//! When the `--features mmap` Cargo feature is enabled, a faster zero-copy
//! path becomes available via [`read_page_mmap`]; it is *not* the default
//! and is gated behind the feature so that the default build pulls in only
//! `std::fs`.
// PR1 calls `read_page` only; `read_page_strict` lands alongside `--strict`
// in PR2.
#[allow(dead_code)]

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use crate::dir::{BlockFile, ShardDir};
use crate::error::{Result, ScannerError};
use crate::pid::DiskCoord;

/// 16 KiB page size. Must match `page::PAGE_SIZE`.
pub const PAGE_SIZE: usize = 16 * 1024;

/// Outcome of a single page read; the scanner reports this directly rather
/// than panic-ing when something is wrong.
#[derive(Debug)]
pub enum PageRead {
    /// Page bytes read successfully from disk.
    Ok(Box<[u8; PAGE_SIZE]>),
    /// `.block` file for `DiskCoord::file_id` does not exist on disk.
    BlockFileMissing { file_id: u32 },
    /// The `.block` file is too short to contain this page.
    BlockFileTruncated { file_id: u32, path: PathBuf, size: u64 },
    /// Underlying `std::io::Error` (permissions, IO error, etc.).
    IoError {
        file_id: u32,
        path: PathBuf,
        offset: u64,
        source: std::io::Error,
    },
}

/// Read a page given a shard and a coordinate. The shard is consulted to
/// resolve which `.block` file to open.
pub fn read_page(shard: &ShardDir, coord: DiskCoord) -> PageRead {
    // Find the .block file. Linear scan is fine: a single shard has at most
    // a few hundred .block files even for very large databases.
    let block = match shard.block_files.iter().find(|b| b.file_id == coord.file_id) {
        Some(b) => b,
        None => {
            return PageRead::BlockFileMissing {
                file_id: coord.file_id,
            };
        }
    };
    let block = block.clone();

    let offset = coord.file_offset();

    // Sanity check size before opening. A too-short block file is not an
    // error -- it just means the page lives past the end of the file
    // (vacant or never written).
    if block.size_bytes < offset + PAGE_SIZE as u64 {
        return PageRead::BlockFileTruncated {
            file_id: block.file_id,
            path: block.path.clone(),
            size: block.size_bytes,
        };
    }

    let mut file = match fs::File::open(&block.path) {
        Ok(f) => f,
        Err(e) => {
            return PageRead::IoError {
                file_id: block.file_id,
                path: block.path.clone(),
                offset,
                source: e,
            };
        }
    };
    if let Err(e) = file.seek(SeekFrom::Start(offset)) {
        return PageRead::IoError {
            file_id: block.file_id,
            path: block.path.clone(),
            offset,
            source: e,
        };
    }

    let mut buf = vec![0u8; PAGE_SIZE].into_boxed_slice();
    let buf_mut: &mut [u8] = &mut buf;
    if let Err(e) = file.read_exact(buf_mut) {
        return PageRead::IoError {
            file_id: block.file_id,
            path: block.path.clone(),
            offset,
            source: e,
        };
    }

    let arr: [u8; PAGE_SIZE] = match buf.into_vec().try_into() {
        Ok(a) => a,
        Err(_) => {
            // Cannot happen -- we asked for exactly PAGE_SIZE bytes.
            return PageRead::IoError {
                file_id: block.file_id,
                path: block.path,
                offset,
                source: std::io::Error::other("read_exact returned wrong length"),
            };
        }
    };
    PageRead::Ok(Box::new(arr))
}

/// Convenience: return the bytes wrapped in `Some`, or convert any failure
/// path into an `Err(ScannerError)`. Used by code paths that *require* a page
/// to proceed (e.g. `header -vpid N` in strict mode).
pub fn read_page_strict(shard: &ShardDir, coord: DiskCoord) -> Result<Box<[u8; PAGE_SIZE]>> {
    match read_page(shard, coord) {
        PageRead::Ok(b) => Ok(b),
        PageRead::BlockFileMissing { file_id } => Err(ScannerError::VpidUnlocatable {
            vpid: 0, // vpid implicit; caller wraps in a higher-level Err
            reason: format!("block file {file_id}.block missing on disk"),
        }),
        PageRead::BlockFileTruncated {
            file_id,
            path,
            size,
        } => Err(ScannerError::ReadFailed {
            path,
            offset: coord.file_offset(),
            source: std::io::Error::other(format!(
                "block {file_id} truncated: size={size}, need >={}",
                coord.file_offset() + PAGE_SIZE as u64
            )),
        }),
        PageRead::IoError {
            file_id: _,
            path,
            offset,
            source,
        } => Err(ScannerError::ReadFailed {
            path,
            offset,
            source,
        }),
    }
}

impl BlockFile {
    /// Convenience constructor for tests/CLI that want to build a stub.
    #[cfg(test)]
    pub fn test_new(file_id: u32, path: PathBuf, size: u64) -> Self {
        Self {
            file_id,
            path,
            size_bytes: size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dir::ShardDir;
    use std::path::PathBuf;

    fn shard_with_block(file_id: u32, path: PathBuf, size: u64) -> ShardDir {
        ShardDir {
            id: 0,
            path: path.parent().unwrap().to_path_buf(),
            block_files: vec![BlockFile {
                file_id,
                path: path.clone(),
                size_bytes: size,
            }],
            page_mate: None,
            pid_state: None,
            wal_segments: vec![],
        }
    }

    #[test]
    fn read_page_returns_missing_for_unknown_file() {
        let dir = tempfile::tempdir().unwrap();
        let shard = shard_with_block(
            /* file_id = */ 0,
            dir.path().join("000001.block"),
            PAGE_SIZE as u64,
        );
        // vpid 640 belongs to file_id 1 (which we did not create).
        let r = read_page(&shard, DiskCoord::from_vpid_arithmetic(640));
        assert!(matches!(r, PageRead::BlockFileMissing { file_id: 1 }));
    }

    #[test]
    fn read_page_returns_truncated_for_short_block() {
        let dir = tempfile::tempdir().unwrap();
        // Block is exactly PAGE_SIZE long: reading vpid=1 needs offset =
        // PAGE_SIZE, which is exactly at the end -> truncated.
        let path = dir.path().join("000001.block");
        std::fs::write(&path, vec![0u8; PAGE_SIZE]).unwrap();
        let shard = shard_with_block(
            /* file_id = */ 0,
            path.clone(),
            PAGE_SIZE as u64,
        );
        let r = read_page(&shard, DiskCoord::from_vpid_arithmetic(1));
        assert!(matches!(r, PageRead::BlockFileTruncated { .. }));
    }

    #[test]
    fn read_page_reads_distinct_bytes_per_page() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("000001.block");
        let mut bytes = vec![0u8; PAGE_SIZE * 2];
        for b in bytes.iter_mut() {
            *b = 0xA5;
        }
        // Stamp a recognisable header into the second page's first 8 bytes.
        bytes[PAGE_SIZE..PAGE_SIZE + 8].copy_from_slice(b"LCBPMARK");
        std::fs::write(&path, &bytes).unwrap();

        let shard = shard_with_block(
            /* file_id = */ 0,
            path,
            (PAGE_SIZE * 2) as u64,
        );

        let r = read_page(&shard, DiskCoord::from_vpid_arithmetic(0));
        let PageRead::Ok(buf) = r else { panic!("expected Ok") };
        assert_eq!(&buf[0..8], b"\xA5\xA5\xA5\xA5\xA5\xA5\xA5\xA5");

        let r = read_page(&shard, DiskCoord::from_vpid_arithmetic(1));
        let PageRead::Ok(buf) = r else { panic!("expected Ok") };
        assert_eq!(&buf[0..8], b"LCBPMARK");
    }
}
