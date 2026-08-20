//! Layout-tagged discovery probes.
//!
//! Each probe returns an `Option<ShardDir>` plus a `LayoutTag` when it
//! recognises its target pattern. The probes are independent and short;
//! the dispatch in `super::inspect()` chains them in order. Keeping them
//! in this module (rather than inlining in `mod.rs`) keeps each pattern
//! readable and lets new layouts be added without touching the dispatcher.
//!
//! Cross-platform invariant: every probe uses only `std::fs` and never
//! assumes a path separator. See module-level docs in `super`.

use std::fs;
use std::path::Path;

use crate::error::{Result, ScannerError};

use super::{BlockFile, ShardDir, WalSegment};

/// A short name for the recognised layout shape.
///
/// Variants are stored as `&'static str` so JSON output stays trivially
/// reproducible across platforms; enum variants are reserved for code
/// that needs to switch on shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutTag {
    /// `<root>/shard_<N>/000001.block`  -- engine default.
    L1,
    /// `<root>/<db_name>/shard_<N>/000001.block`  -- incident-report shape.
    L2,
    /// `<root>/shard_<M>/<db_name>/shard_<N>/000001.block`  -- the
    /// shape we observed in the user's corrupt backup.
    L3,
    /// `<root>/000001.block`  -- legacy compat (no per-shard subdir).
    L4,
}

impl LayoutTag {
    pub fn as_str(self) -> &'static str {
        match self {
            LayoutTag::L1 => "L1",
            LayoutTag::L2 => "L2",
            LayoutTag::L3 => "L3",
            LayoutTag::L4 => "L4",
        }
    }
}

/// Try to interpret a single file name as a shard directory (`shard_<id>`).
pub fn parse_shard_dir_name(name: &str) -> Option<u32> {
    let rest = name.strip_prefix("shard_")?;
    rest.parse::<u32>().ok()
}

/// Try to interpret a file name as `<shard>.wal.<seq:06>`.
pub fn parse_wal_name(name: &str) -> Option<(u32, u32)> {
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

/// Try to interpret a file name as `<file_id:06>.block`.
pub fn parse_block_name(name: &str) -> Option<u32> {
    let rest = name.strip_suffix(".block")?;
    if rest.len() != 6 {
        return None;
    }
    rest.parse::<u32>().ok()
}

/// Scan one directory for a `000001.block` + `page.mate` (with optional
/// `pid.state` and inner WAL segments). Returns `Ok(None)` when the
/// directory does not look like a block_dir; `Ok(Some(_))` otherwise.
pub fn scan_shard_dir(id: u32, path: &Path) -> Result<Option<ShardDir>> {
    let mut block_files = Vec::new();
    let mut page_mate = None;
    let mut pid_state = None;
    let mut wal_segments = Vec::new();
    let mut has_block = false;
    let mut has_mate = false;

    let entries = match fs::read_dir(path) {
        Ok(it) => it.flatten().collect::<Vec<_>>(),
        Err(e) => {
            return Err(ScannerError::DirNotAccessible {
                path: path.to_path_buf(),
                source: e,
            });
        }
    };

    for entry in entries {
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
            has_mate = true;
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
            has_block = true;
            continue;
        }
        if let Some(wal) = parse_wal_name(name) {
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

    if !has_block && !has_mate {
        return Ok(None);
    }

    Ok(Some(ShardDir {
        id,
        path: path.to_path_buf(),
        block_files,
        page_mate,
        pid_state,
        wal_segments,
    }))
}

/// Probe one level deeper for `<dir>/shard_<N>/000001.block`.
///
/// Differentiates L2 vs L3 by the name of the *outer* subdir: if it is
/// itself `shard_<M>`, then the shape is L3; otherwise it is L2.
///
/// We do not classify by depth alone because filenames can be arbitrary
/// across OSes and the engine does not, today, guarantee any particular
/// naming for the db-name wrapper.
pub fn probe_nested_shard(path: &Path) -> Result<Option<(ShardDir, LayoutTag)>> {
    // The caller already knows this directory exists; we differentiate
    // whether `path`'s own basename is a `shard_<M>` wrapper (L3) before
    // we descend.
    let self_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let self_is_shard_wrapper = parse_shard_dir_name(self_name).is_some();

    let depth = match path.read_dir() {
        Ok(it) => it.flatten().collect::<Vec<_>>(),
        Err(e) => {
            return Err(ScannerError::DirNotAccessible {
                path: path.to_path_buf(),
                source: e,
            });
        }
    };

    for entry in depth {
        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if !ft.is_dir() {
            continue;
        }
        let sub = entry.path();
        let sub_name = sub.file_name().and_then(|n| n.to_str()).unwrap_or("");

        // L3 probe: <dir>/shard_<M>/<db_name>/shard_<N>/
        // The *outer* basename is itself a shard_<M> wrapper.
        if let Some(_sid_outer) = parse_shard_dir_name(sub_name) {
            // Drill one more level. If we find a directory inside whose
            // name is `shard_<N>` *and* it is itself a block_dir, that's
            // the L3 inner.
            if let Some((sd, tag)) = probe_one_level(&sub)? {
                // Always tag as L3 -- the outer wrapper is the giveaway.
                let _ = tag;
                return Ok(Some((sd, LayoutTag::L3)));
            }
            // Else fall through and treat this as a plain L2 candidate
            // (the outer shard_<M> wrapper happens to also be a valid
            // shard directory on its own).
            if let Some(sd) = scan_shard_dir(_sid_outer, &sub)? {
                return Ok(Some((sd, LayoutTag::L2)));
            }
        }

        // L2 probe (catch-all): walk into one level and look for a
        // `shard_<N>` directory whose contents look like a block_dir.
        if let Some((sd, _tag)) = probe_one_level(&sub)? {
            // If `path` itself was a shard_<M> wrapper, this discovery is
            // L3 by definition; otherwise L2.
            let tag = if self_is_shard_wrapper {
                LayoutTag::L3
            } else {
                LayoutTag::L2
            };
            return Ok(Some((sd, tag)));
        }
    }

    Ok(None)
}

/// Drill one level into `outer` looking for a `shard_<N>/...` block_dir.
/// Returns the discovered block_dir plus an *unspecified* layout tag;
/// the caller decides whether to label this L2 or L3 based on context.
///
/// Re-exported with the `_pub` suffix so the dispatcher in
/// `super::inspect()` can drill into a known-non-`shard_<N>` directory
/// without going through the full `probe_nested_shard` heuristics.
pub fn probe_one_level_pub(outer: &Path) -> Result<Option<(ShardDir, LayoutTag)>> {
    probe_one_level(outer)
}

fn probe_one_level(outer: &Path) -> Result<Option<(ShardDir, LayoutTag)>> {
    let inner = match outer.read_dir() {
        Ok(it) => it.flatten().collect::<Vec<_>>(),
        Err(_) => return Ok(None),
    };
    for entry in inner {
        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if !ft.is_dir() {
            continue;
        }
        let p = entry.path();
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if let Some(sid) = parse_shard_dir_name(name) {
            if let Some(sd) = scan_shard_dir(sid, &p)? {
                // Inner-tag is set to L2 here because L3 is decided by the
                // outer wrapper (see `probe_nested_shard`).
                return Ok(Some((sd, LayoutTag::L2)));
            }
        }
    }
    Ok(None)
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
        assert_eq!(parse_shard_dir_name("ShArD_0"), None);
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
        assert_eq!(parse_block_name("00001.block"), None);
        assert_eq!(parse_block_name("0000001.block"), None);
    }

    #[test]
    fn layout_tag_as_str_is_stable() {
        // Layout tag strings are part of the public CLI/JSON contract.
        // Changing them is a breaking change.
        assert_eq!(LayoutTag::L1.as_str(), "L1");
        assert_eq!(LayoutTag::L2.as_str(), "L2");
        assert_eq!(LayoutTag::L3.as_str(), "L3");
        assert_eq!(LayoutTag::L4.as_str(), "L4");
    }
}
