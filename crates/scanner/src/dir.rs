//! Directory layout helpers.
//!
//! A NexusDB data directory contains:
//! - `<block_dir>/<shard_id>.wal.<seq:06>` WAL segments (zero or more).
//! - `<block_dir>/<shard_id>/<file_id:06>.block` chunk files.
//! - `<block_dir>/<shard_id>/page.mate`        vpid -> pid index (~1 MiB).
//! - `<block_dir>/<shard_id>/pid.state`        last persisted allocator hint.
//! - `<block_dir>/<shard_id>/stats.bin`        optional CBO statistics.
//!
//! Compatibility layouts are similar (older versions may omit the per-shard
//! subdirectory and place files at the root of `block_dir`); this module
//! recognises both.
//!
//! All file reads here use `std::fs`; nothing in this module depends on the
//! platform. Names are matched structurally (prefix/suffix), never assuming
//! a particular casing or separator.
// PR1 only reads top-level structure; richer fields are surfaced for
// `map`, `wal`, and related commands arriving in PR2+.
#[allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{Result, ScannerError};

/// Identified data directory; describes the on-disk shape we found.
#[derive(Debug, Clone)]
pub struct DataDir {
    /// Path the user gave us (or the resolved canonical path).
    pub path: PathBuf,
    /// Shard directories we located. Typically just one (`shard_0`).
    pub shards: Vec<ShardDir>,
    /// WAL segment files at the top level (`block_dir/shard_<N>.wal.<seq>`).
    /// (Older compat shapes keep WAL segments here rather than per-shard.)
    pub top_level_wal: Vec<WalSegment>,
}

/// One shard's worth of artifacts.
#[derive(Debug, Clone)]
pub struct ShardDir {
    /// Shard numeric id (parsed from `shard_<N>`).
    pub id: u32,
    /// Path to the shard subdirectory.
    pub path: PathBuf,
    /// `<file_id:06>.block` files found inside the shard.
    pub block_files: Vec<BlockFile>,
    /// `page.mate` index, if present.
    pub page_mate: Option<PathBuf>,
    /// `pid.state` hint, if present.
    pub pid_state: Option<PathBuf>,
    /// WAL segments observed inside the shard (rare; usually top-level).
    pub wal_segments: Vec<WalSegment>,
}

/// A single `<file_id:06>.block` chunk store file.
#[derive(Debug, Clone)]
pub struct BlockFile {
    pub file_id: u32,
    pub path: PathBuf,
    pub size_bytes: u64,
}

/// A single WAL segment `<shard_id>.wal.<seq:06>` or `<shard_id>/...wal.<seq>`.
#[derive(Debug, Clone)]
pub struct WalSegment {
    pub shard_id: u32,
    pub seq: u32,
    pub path: PathBuf,
    pub size_bytes: u64,
}

/// Inspect a user-supplied data directory and return its layout.
pub fn inspect(path: &Path) -> Result<DataDir> {
    let meta = fs::metadata(path).map_err(|e| ScannerError::DirNotAccessible {
        path: path.to_path_buf(),
        source: e,
    })?;
    if !meta.is_dir() {
        return Err(ScannerError::DirNotAccessible {
            path: path.to_path_buf(),
            source: io_error_other("not a directory"),
        });
    }

    let mut shards = Vec::new();
    let mut top_level_wal = Vec::new();

    // Top-level entries: collect .block / shard_*/ wal.
    let entries = fs::read_dir(path).map_err(|e| ScannerError::DirNotAccessible {
        path: path.to_path_buf(),
        source: e,
    })?;
    for entry in entries.flatten() {
        let p = entry.path();
        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue, // ignore unreadable entries
        };

        if ft.is_dir() {
            if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                if let Some(id) = parse_shard_dir_name(name) {
                    let shard = scan_shard_dir(id, &p)?;
                    shards.push(shard);
                }
            }
            continue;
        }

        if !ft.is_file() {
            continue;
        }

        if let Some(wal) = parse_wal_name(p.file_name().and_then(|n| n.to_str()).unwrap_or("")) {
            let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
            top_level_wal.push(WalSegment {
                shard_id: wal.0,
                seq: wal.1,
                path: p,
                size_bytes: size,
            });
        }
    }

    // Sort for deterministic output.
    shards.sort_by_key(|s| s.id);
    top_level_wal.sort_by_key(|w| (w.shard_id, w.seq));

    // Reject directories that look completely empty -- the user almost
    // certainly pointed us at the wrong path.
    if shards.is_empty() && top_level_wal.is_empty() {
        return Err(ScannerError::EmptyDirectory {
            path: path.to_path_buf(),
        });
    }

    Ok(DataDir {
        path: path.to_path_buf(),
        shards,
        top_level_wal,
    })
}

/// Try to interpret a single file as a shard directory (`shard_<id>`).
fn parse_shard_dir_name(name: &str) -> Option<u32> {
    let rest = name.strip_prefix("shard_")?;
    rest.parse::<u32>().ok()
}

/// Try to interpret a file name as `<shard>.wal.<seq:06>`.
fn parse_wal_name(name: &str) -> Option<(u32, u32)> {
    // Accept `shard_<N>.wal.<seq>` (compat: extra underscores allowed).
    let mut parts = name.split('.');
    let head = parts.next()?;
    let middle = parts.next()?;
    let seq = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    if middle != "wal" {
        return None;
    }
    let shard_id: u32 = head.strip_prefix("shard_")?.parse().ok()?;
    let seq: u32 = seq.parse().ok()?;
    Some((shard_id, seq))
}

/// Scan one `shard_<N>` directory.
fn scan_shard_dir(id: u32, path: &Path) -> Result<ShardDir> {
    let mut block_files = Vec::new();
    let mut page_mate = None;
    let mut pid_state = None;
    let mut wal_segments = Vec::new();

    for entry in fs::read_dir(path)
        .map_err(|e| ScannerError::DirNotAccessible {
            path: path.to_path_buf(),
            source: e,
        })?
        .flatten()
    {
        let p = entry.path();
        let name = match p.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };

        let meta = match entry.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        if !meta.is_file() {
            continue;
        }

        if name == "page.mate" {
            page_mate = Some(p);
            continue;
        }
        if name == "pid.state" {
            pid_state = Some(p);
            continue;
        }
        if let Some(block) = parse_block_name(name) {
            block_files.push(BlockFile {
                file_id: block,
                path: p,
                size_bytes: meta.len(),
            });
            continue;
        }
        if let Some(wal) = parse_wal_name(name) {
            // Sometimes shards contain WAL too (compat).
            wal_segments.push(WalSegment {
                shard_id: wal.0,
                seq: wal.1,
                path: p,
                size_bytes: meta.len(),
            });
        }
    }

    block_files.sort_by_key(|b| b.file_id);
    wal_segments.sort_by_key(|w| w.seq);

    Ok(ShardDir {
        id,
        path: path.to_path_buf(),
        block_files,
        page_mate,
        pid_state,
        wal_segments,
    })
}

/// Try to interpret a file name as `<file_id:06>.block`.
fn parse_block_name(name: &str) -> Option<u32> {
    let rest = name.strip_suffix(".block")?;
    if rest.len() != 6 {
        return None;
    }
    rest.parse::<u32>().ok()
}

/// Build a synthetic `io::Error` for the "not a directory" path.
fn io_error_other(msg: &str) -> std::io::Error {
    std::io::Error::other(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_shard_dir_name_accepts_canonical() {
        assert_eq!(parse_shard_dir_name("shard_0"), Some(0));
        assert_eq!(parse_shard_dir_name("shard_3"), Some(3));
        assert_eq!(parse_shard_dir_name("shard_42"), Some(42));
    }

    #[test]
    fn parse_shard_dir_name_rejects_others() {
        assert_eq!(parse_shard_dir_name("data"), None);
        assert_eq!(parse_shard_dir_name("shard_"), None);
        assert_eq!(parse_shard_dir_name("shard_abc"), None);
        assert_eq!(parse_shard_dir_name("ShArD_0"), None); // case sensitive
    }

    #[test]
    fn parse_wal_name_handles_canonical() {
        let (s, n) = parse_wal_name("shard_0.wal.000180").unwrap();
        assert_eq!(s, 0);
        assert_eq!(n, 180);
    }

    #[test]
    fn parse_wal_name_rejects_others() {
        assert!(parse_wal_name("000180.wal").is_none());
        assert!(parse_wal_name("shard_0.000180").is_none());
        assert!(parse_wal_name("shard_0.wal.000180.extra").is_none());
        assert!(parse_wal_name("shard_a.wal.000180").is_none());
    }

    #[test]
    fn parse_block_name_handles_six_digit_id() {
        assert_eq!(parse_block_name("000001.block"), Some(1));
        assert_eq!(parse_block_name("640000.block"), Some(640_000));
        assert_eq!(parse_block_name("00001.block"), None); // too short
        assert_eq!(parse_block_name("0000001.block"), None); // too long
    }

    #[test]
    fn inspect_rejects_nonexistent_directory() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope");
        let err = inspect(&missing).unwrap_err();
        assert!(matches!(err, ScannerError::DirNotAccessible { .. }));
    }

    #[test]
    fn inspect_rejects_empty_directory() {
        let dir = tempfile::tempdir().unwrap();
        let err = inspect(dir.path()).unwrap_err();
        assert!(matches!(err, ScannerError::EmptyDirectory { .. }));
    }

    #[test]
    fn inspect_finds_top_level_wal_only() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("shard_0.wal.000001"), b"").unwrap();
        let layout = inspect(dir.path()).unwrap();
        assert!(layout.shards.is_empty());
        assert_eq!(layout.top_level_wal.len(), 1);
        assert_eq!(layout.top_level_wal[0].seq, 1);
    }

    #[test]
    fn inspect_finds_shard_dir_with_block_file() {
        let dir = tempfile::tempdir().unwrap();
        let shard = dir.path().join("shard_0");
        std::fs::create_dir(&shard).unwrap();
        std::fs::write(shard.join("000001.block"), b"x").unwrap();
        std::fs::write(shard.join("page.mate"), b"y").unwrap();
        std::fs::write(shard.join("pid.state"), b"z").unwrap();
        let layout = inspect(dir.path()).unwrap();
        assert_eq!(layout.shards.len(), 1);
        assert_eq!(layout.shards[0].id, 0);
        assert_eq!(layout.shards[0].block_files.len(), 1);
        assert_eq!(layout.shards[0].block_files[0].file_id, 1);
        assert!(layout.shards[0].page_mate.is_some());
        assert!(layout.shards[0].pid_state.is_some());
    }
}
