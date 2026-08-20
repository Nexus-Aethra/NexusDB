//! Directory layout discovery.
//!
//! A NexusDB data directory on disk can take several shapes. This module
//! recognises all of them and produces a single canonical [`DataDir`]
//! regardless of which shape the user has given us.
//!
//! # Canonical shapes
//!
//! We work with two concepts:
//!
//! - `block_root` — the directory the user passed to `--dir`. WAL segment
//!   files (`shard_<N>.wal.<seq>`) live here, one level deep.
//! - `block_dir` — the directory that holds a single shard's `.block`
//!   files plus `page.mate` and `pid.state`. This may be `block_root`
//!   itself, or a child of it.
//!
//! The concrete shapes observed in the wild:
//!
//! ```text
//! -- L1 (PR1 default) --
//! <block_root>/
//!   shard_<N>/
//!     000001.block
//!     page.mate
//!     pid.state
//!   shard_<N>.wal.<seq>
//!
//! -- L2 (db-name layout, what the engine writes by default) --
//! <block_root>/
//!   <db_name>/
//!     shard_<N>/
//!       000001.block
//!       page.mate
//!       pid.state
//!   shard_<N>.wal.<seq>
//!
//! -- L3 (extra shard wrapper, what we found in the corrupt backup) --
//! <block_root>/
//!   shard_<N>/                        <-- extra wrapper
//!     <db_name>/
//!       shard_<N>/
//!         000001.block
//!         page.mate
//!         pid.state
//!   shard_<N>.wal.<seq>
//!
//! -- L4 (legacy compat: no per-shard subdir) --
//! <block_root>/
//!   000001.block
//!   page.mate
//!   pid.state
//!   shard_<N>.wal.<seq>
//! ```
//!
//! # Discovery algorithm
//!
//! 1. Collect every top-level child of `block_root` that is a directory or
//!    a `.wal.<seq>` file.
//! 2. For each candidate child that is a directory, probe the following
//!    patterns (in order):
//!
//!    - `child/000001.block + child/page.mate`               (L4)
//!    - `child/shard_<N>/000001.block + page.mate`           (L1)
//!    - `child/<db_name>/shard_<N>/000001.block`             (L2 / L3)
//!      The discriminant is the immediate child's basename: if it is
//!      itself `shard_<M>`, the shape is L3; otherwise L2.
//!
//!    Each match contributes one `ShardDir` to the output.
//!
//! 3. Top-level `shard_<N>.wal.<seq>` files are gathered regardless of
//!    whether a matching `ShardDir` was found (the engine may have
//!    pre-rotated WAL segments for shards that did not yet have any
//!    checkpointed blocks).
//!
//! Discovery is **single-pass and depth-bounded** at three levels under
//! `block_root`. Deeper nesting is rejected as ambiguous.
//!
//! # Cross-platform
//!
//! All disk traversal uses `std::fs`. Path comparisons go through
//! `Path::file_name()`; we never assume a separator. No `mmap`, no
//! platform syscalls.
//!
//! See [`layout`] for the probe implementations.

mod layout;

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{Result, ScannerError};

pub use layout::LayoutTag;
use layout::{parse_wal_name, probe_nested_shard, scan_shard_dir};

/// Identified data directory; describes the on-disk shape we found.
#[derive(Debug, Clone)]
pub struct DataDir {
    /// Path the user gave us (or the resolved canonical path).
    pub path: PathBuf,
    /// Shard directories we located. Typically just one (`shard_0`).
    pub shards: Vec<ShardDir>,
    /// WAL segment files at the top level (`block_root/shard_<N>.wal.<seq>`).
    /// (Older compat shapes keep WAL segments here rather than per-shard.)
    pub top_level_wal: Vec<WalSegment>,
    /// Layout shape matched. `"L1"`, `"L2"`, `"L3"`, `"L4"`, or a `+`-joined
    /// combination when a single directory mixes shapes.
    pub layout_tag: String,
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

    let entries = match fs::read_dir(path) {
        Ok(it) => it.flatten().collect::<Vec<_>>(),
        Err(e) => {
            return Err(ScannerError::DirNotAccessible {
                path: path.to_path_buf(),
                source: e,
            });
        }
    };

    let mut shards: Vec<ShardDir> = Vec::new();
    let mut top_level_wal: Vec<WalSegment> = Vec::new();
    let mut layout_tags: Vec<String> = Vec::new();

    for entry in entries {
        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        let p = entry.path();
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");

        if ft.is_file() {
            if let Some((sid, seq)) = parse_wal_name(name) {
                let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                top_level_wal.push(WalSegment {
                    shard_id: sid,
                    seq,
                    path: p,
                    size_bytes: size,
                });
            }
            continue;
        }

        if !ft.is_dir() {
            continue;
        }

        // L1: <root>/shard_<N>/  -- a top-level child whose basename is
        // `shard_<N>` AND whose contents look like a block_dir (i.e. it
        // has its own `.block` files or `page.mate`).
        //
        // We do NOT tag with L1 by basename alone: a `shard_<N>/` could
        // also be the outer wrapper of an L3 layout. The `scan_shard_dir`
        // call distinguishes the two.
        if let Some(sid) = layout::parse_shard_dir_name(name) {
            if let Some(sd) = scan_shard_dir(sid, &p)? {
                shards.push(sd);
                layout_tags.push(LayoutTag::L1.as_str().into());
                continue;
            }
            // `shard_<N>/` exists but has no immediate block files.
            // Fall through and let the nested probe try to find a deeper
            // block_dir; that may produce L3.
            if let Some((sd, tag)) = probe_nested_shard(&p)? {
                shards.push(sd);
                layout_tags.push(tag.as_str().into());
                continue;
            }
            // Truly empty shard_<N>/; record it as a stub for visibility.
            shards.push(ShardDir {
                id: sid,
                path: p.clone(),
                block_files: Vec::new(),
                page_mate: None,
                pid_state: None,
                wal_segments: Vec::new(),
            });
            layout_tags.push(LayoutTag::L1.as_str().into());
            continue;
        }

        // L4 (sub-dir flavour): <root>/<dir>/000001.block + page.mate
        // The directory itself is the block_dir and is named anything
        // other than `shard_<N>`.
        if let Some(sd) = scan_shard_dir(0, &p)? {
            shards.push(sd);
            layout_tags.push(LayoutTag::L4.as_str().into());
            continue;
        }

        // L2 only: <root>/<db_name>/shard_<N>/ -- nested but with a
        // non-`shard_<N>` outer. (L3 is handled by the fallback branch
        // above when the outer happens to also be `shard_<N>`.)
        if let Some((sd, _tag)) = layout::probe_one_level_pub(&p)? {
            shards.push(sd);
            // We have to override L3 here -- if the L3 fallback above
            // already claimed this entry, we will not reach this branch.
            // Since we are in the "outer is not shard_<N>" case, the
            // answer is always L2.
            layout_tags.push(LayoutTag::L2.as_str().into());
            continue;
        }
    }

    // L4 root-anchored probe: the directory itself is the block_dir. We try
    // this after enumerating children so that a directory which *also*
    // contains a `shard_<N>/` subdir with deeper nesting (L3) is not
    // mistaken for L4. The only case L4-root wins is when no recognisable
    // children were found above.
    if shards.is_empty() && top_level_wal.is_empty() {
        if let Some(sd) = scan_shard_dir(0, path)? {
            shards.push(sd);
            layout_tags.push(LayoutTag::L4.as_str().into());
        }
    }

    shards.sort_by_key(|s| s.id);
    top_level_wal.sort_by_key(|w| (w.shard_id, w.seq));

    let layout_tag = if layout_tags.is_empty() {
        "unknown".into()
    } else {
        layout_tags.sort();
        layout_tags.dedup();
        layout_tags.join("+")
    };

    if shards.is_empty() && top_level_wal.is_empty() {
        return Err(ScannerError::EmptyDirectory {
            path: path.to_path_buf(),
        });
    }

    Ok(DataDir {
        path: path.to_path_buf(),
        shards,
        top_level_wal,
        layout_tag,
    })
}

/// Build a synthetic `io::Error` for the "not a directory" path.
fn io_error_other(msg: &str) -> std::io::Error {
    std::io::Error::other(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Layout L1: a top-level `shard_<N>` dir.
    #[test]
    fn inspect_l1_finds_shard_dir_with_block_file() {
        let dir = tempfile::tempdir().unwrap();
        let shard = dir.path().join("shard_0");
        std::fs::create_dir(&shard).unwrap();
        std::fs::write(shard.join("000001.block"), b"x").unwrap();
        std::fs::write(shard.join("page.mate"), b"y").unwrap();
        std::fs::write(shard.join("pid.state"), b"z").unwrap();
        let layout = inspect(dir.path()).unwrap();
        assert_eq!(layout.layout_tag, "L1");
        assert_eq!(layout.shards.len(), 1);
        assert_eq!(layout.shards[0].id, 0);
        assert_eq!(layout.shards[0].block_files.len(), 1);
        assert!(layout.shards[0].page_mate.is_some());
    }

    /// Layout L2: db-name wrapper, e.g. `<root>/<db>/shard_<N>/block files`.
    #[test]
    fn inspect_l2_with_db_wrapper() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("study");
        let shard = db.join("shard_0");
        std::fs::create_dir_all(&shard).unwrap();
        std::fs::write(shard.join("000001.block"), b"x").unwrap();
        std::fs::write(shard.join("page.mate"), b"y").unwrap();
        let layout = inspect(dir.path()).unwrap();
        assert_eq!(layout.layout_tag, "L2");
        assert_eq!(layout.shards.len(), 1);
        assert_eq!(layout.shards[0].id, 0);
        assert_eq!(layout.shards[0].path, shard);
    }

    /// Layout L3: extra `shard_<M>` wrapper around an L2 path.
    #[test]
    fn inspect_l3_with_extra_shard_wrapper() {
        let dir = tempfile::tempdir().unwrap();
        let wrapper = dir.path().join("shard_0");
        let db = wrapper.join("study");
        let shard = db.join("shard_0");
        std::fs::create_dir_all(&shard).unwrap();
        std::fs::write(shard.join("000001.block"), b"x").unwrap();
        std::fs::write(shard.join("page.mate"), b"y").unwrap();
        let layout = inspect(dir.path()).unwrap();
        assert_eq!(layout.layout_tag, "L3");
        assert_eq!(layout.shards.len(), 1);
        assert_eq!(layout.shards[0].id, 0);
        assert_eq!(layout.shards[0].path, shard);
    }

    /// Layout L4: legacy compat, no per-shard subdirectory.
    #[test]
    fn inspect_l4_compat_terminal_layout() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("000001.block"), b"x").unwrap();
        std::fs::write(dir.path().join("page.mate"), b"y").unwrap();
        let layout = inspect(dir.path()).unwrap();
        assert_eq!(layout.layout_tag, "L4");
        assert_eq!(layout.shards.len(), 1);
        // Compat shards have id=0 (no shard_<N> directory present).
        assert_eq!(layout.shards[0].id, 0);
    }

    /// Top-level WAL files must be picked up in any layout.
    #[test]
    fn inspect_finds_top_level_wal() {
        let dir = tempfile::tempdir().unwrap();
        let shard = dir.path().join("shard_0");
        std::fs::create_dir(&shard).unwrap();
        std::fs::write(shard.join("000001.block"), b"x").unwrap();
        std::fs::write(shard.join("page.mate"), b"y").unwrap();
        std::fs::write(dir.path().join("shard_0.wal.000001"), b"").unwrap();
        std::fs::write(dir.path().join("shard_0.wal.000180"), b"").unwrap();
        let layout = inspect(dir.path()).unwrap();
        assert_eq!(layout.top_level_wal.len(), 2);
        assert_eq!(layout.top_level_wal[0].seq, 1);
        assert_eq!(layout.top_level_wal[1].seq, 180);
    }

    /// Multi-shard layout: discover shard_0 and shard_1 in the same root.
    #[test]
    fn inspect_finds_multiple_shards() {
        let dir = tempfile::tempdir().unwrap();
        for sid in [0u32, 1] {
            let shard = dir.path().join(format!("shard_{sid}"));
            std::fs::create_dir(&shard).unwrap();
            std::fs::write(shard.join("000001.block"), b"x").unwrap();
            std::fs::write(shard.join("page.mate"), b"y").unwrap();
        }
        let layout = inspect(dir.path()).unwrap();
        assert_eq!(layout.shards.len(), 2);
        assert_eq!(layout.shards[0].id, 0);
        assert_eq!(layout.shards[1].id, 1);
    }

    /// Layout with only WAL files and no shard dir must still report the
    /// WAL; `inspect` rejects only when BOTH shards and WAL are absent.
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

    /// Mixed layout: one branch is L1, another is L4 (compat side-by-side).
    /// We tag with both shapes joined by `+`.
    #[test]
    fn inspect_records_mixed_tags() {
        let dir = tempfile::tempdir().unwrap();
        // L1 sibling
        let shard = dir.path().join("shard_0");
        std::fs::create_dir(&shard).unwrap();
        std::fs::write(shard.join("000001.block"), b"x").unwrap();
        std::fs::write(shard.join("page.mate"), b"y").unwrap();
        // L4 sibling under a fake "compat" name
        let compat = dir.path().join("compat");
        std::fs::create_dir(&compat).unwrap();
        std::fs::write(compat.join("000001.block"), b"x").unwrap();
        std::fs::write(compat.join("page.mate"), b"y").unwrap();

        let layout = inspect(dir.path()).unwrap();
        assert_eq!(layout.shards.len(), 2);
        assert_eq!(layout.layout_tag, "L1+L4");
    }
}
