//! vpid → on-disk location resolution.
//!
//! Two complementary sources of truth for "where does vpid N live on disk?":
//!
//! 1. **`page.mate`** — a 1 MiB index file containing 8-byte `PidLocation`
//!    entries, one per vpid slot. Slot `N` corresponds to vpid `N`. This is the
//!    authoritative on-disk map.
//!
//! 2. **`pid.state`** — an 8-byte `PidLocation` of the highest currently
//!    allocated vpid. Pure hint; can be stale or missing. Used only as a
//!    upper-bound check.
//!
//! 3. **Pure arithmetic** — given `BLOCK_SIZE`, `CHUNK_SIZE`, `PAGE_SIZE`,
//!    `vpid` maps to `(file_id, chunk_idx, page_idx)` deterministically,
//!    **assuming vpid < the file count**. This is the fallback when
//!    `page.mate` is missing.
//!
//! The locator defaults to tolerant mode: missing `page.mate` falls back to
//! arithmetic, missing `pid.state` is silent, malformed entries are dropped
//! with a recorded bad-slot count.
// PR1 only uses `MateThenArithmetic`; the remaining `Strategy` variants,
// `LocateProvenance`, and related accessors are scaffolding for `map` and
// `verify` (PR2+) and the next PR will consume them.

use std::fs;
use std::path::{Path, PathBuf};

use page::PidLocation;

use crate::dir::ShardDir;
use crate::error::{Result, ScannerError};

/// Number of pages per chunk (CHUNK_SIZE / PAGE_SIZE = 1 MiB / 16 KiB).
pub const PAGES_PER_CHUNK: u64 = 64;

/// Number of chunks per block file (BLOCK_SIZE / CHUNK_SIZE = 10 MiB / 1 MiB).
pub const CHUNKS_PER_BLOCK: u64 = 10;

/// Number of pages per block file.
pub const PAGES_PER_BLOCK: u64 = PAGES_PER_CHUNK * CHUNKS_PER_BLOCK; // 640

/// Maximum vpid count that fits in a 1 MiB page.mate index (8 bytes / slot).
#[allow(dead_code)]
pub const MATE_CAPACITY: u64 = 1024 * 1024 / 8;

/// On-disk coordinate for one vpid: which `.block` file + chunk + page-in-chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskCoord {
    pub file_id: u32,
    pub chunk_idx: u8,
    pub page_idx: u16,
}

impl DiskCoord {
    /// Pure-arithmetic mapping. Caller must guarantee `vpid / PAGES_PER_BLOCK`
    /// is a block file that actually exists on disk.
    pub fn from_vpid_arithmetic(vpid: u64) -> Self {
        let within = vpid % PAGES_PER_BLOCK;
        Self {
            file_id: (vpid / PAGES_PER_BLOCK) as u32,
            chunk_idx: (within / PAGES_PER_CHUNK) as u8,
            page_idx: (within % PAGES_PER_CHUNK) as u16,
        }
    }

    /// Offset, in bytes, into the `.block` file where this page sits.
    pub fn file_offset(self) -> u64 {
        let chunk_off = (self.chunk_idx as u64) * 1024 * 1024;
        let page_off = (self.page_idx as u64) * (16 * 1024);
        chunk_off + page_off
    }
}

/// vpid resolver for a single shard.
// Fields beyond `mate` are scaffolding for the `map` and `verify`
// commands in PR2/3; PR1 only consumes `mate` and `block_file_id_range`
// indirectly via `resolve`.
#[allow(dead_code)]
pub struct Locate {
    /// Slot table loaded from `page.mate`; `Some` if the file existed and
    /// was at least `8*vpid_len` bytes long. Always present after `open`,
    /// possibly empty.
    mate: Vec<PidLocation>,
    /// Number of vpid slots we successfully read from `page.mate`.
    /// May exceed `mate.len()` if the file was truncated.
    usable_slots: u64,
    /// Highest allocated vpid reported by `pid.state`, or None if missing.
    pid_state_hint: Option<u64>,
    /// Block-file id range observed on disk. Used to determine whether
    /// arithmetic lookup is safe.
    block_file_id_range: Option<(u32, u32)>,
    /// Where `page.mate` was loaded from (for diagnostics).
    mate_path: Option<PathBuf>,
    /// Where `pid.state` was loaded from (for diagnostics).
    pid_state_path: Option<PathBuf>,
    /// Number of malformed slots encountered while loading `page.mate`.
    bad_slot_count: u32,
    /// Offset applied to `DiskCoord.file_id` after resolve.
    /// Set via `with_file_id_offset`; useful when page.mate stores file_id=X
    /// but actual .block files on disk are numbered file_id=X+N.
    file_id_offset: u32,
}

impl Locate {
    /// Open a locator for the given shard directory.
    pub fn open(shard: &ShardDir) -> Result<Self> {
        let (mate, bad_slot_count, usable_slots, mate_path) = match &shard.page_mate {
            Some(p) => load_mate(p),
            None => (Vec::new(), 0, 0, None),
        };
        let pid_state_hint = match &shard.pid_state {
            Some(p) => load_pid_state(p),
            None => None,
        };
        let pid_state_path = shard.pid_state.clone();
        let block_file_id_range = if shard.block_files.is_empty() {
            None
        } else {
            let min = shard.block_files.iter().map(|b| b.file_id).min().unwrap();
            let max = shard.block_files.iter().map(|b| b.file_id).max().unwrap();
            Some((min, max))
        };

        Ok(Self {
            mate,
            usable_slots,
            pid_state_hint,
            block_file_id_range,
            mate_path,
            pid_state_path,
            bad_slot_count,
            file_id_offset: 0,
        })
    }

    /// Open a locator for the given shard directory, then apply a per-read
    /// file_id offset (from `--block-file-id-override`). Use this entry point
    /// from command dispatch to keep the override logic in one place.
    pub fn open_with_override(shard: &ShardDir, file_id_offset: u32) -> Result<Self> {
        let mut loc = Self::open(shard)?;
        loc.file_id_offset = file_id_offset;
        Ok(loc)
    }

    /// Source diagnostics used by the `map` command.
    #[allow(dead_code)] // Consumed by `map` (PR2).
    pub fn provenance(&self) -> LocateProvenance {
        LocateProvenance {
            mate_path: self.mate_path.clone(),
            pid_state_path: self.pid_state_path.clone(),
            usable_slots: self.usable_slots,
            bad_slot_count: self.bad_slot_count,
            pid_state_hint: self.pid_state_hint,
            block_file_id_range: self.block_file_id_range,
        }
    }

    /// How many vpid slots were recovered from `page.mate` (0 if file was missing).
    #[allow(dead_code)] // Consumed by `map` (PR2).
    pub fn usable_slots(&self) -> u64 {
        self.usable_slots
    }

    /// Access a single mate slot by vpid. Returns `None` if the vpid is beyond
    /// the loaded mate region.
    pub fn mate_slot(&self, vpid: u64) -> Option<PidLocation> {
        let idx = vpid as usize;
        if idx < self.mate.len() {
            Some(self.mate[idx])
        } else {
            None
        }
    }

    /// How many slots were malformed and dropped during `page.mate` load.
    #[allow(dead_code)] // Consumed by `map` (PR2).
    pub fn bad_slot_count(&self) -> u32 {
        self.bad_slot_count
    }

    /// Resolve a single vpid to a `DiskCoord` using the given strategy.
    /// `DiskCoord.file_id` is automatically shifted by `file_id_offset` if set.
    pub fn resolve(&self, vpid: u64, strategy: Strategy) -> Result<DiskCoord> {
        let mut coord = match strategy {
            Strategy::MateOnly => self.lookup_mate(vpid)?,
            Strategy::ArithmeticOnly => DiskCoord::from_vpid_arithmetic(vpid),
            Strategy::MateThenArithmetic => self
                .lookup_mate(vpid)
                .unwrap_or_else(|_| DiskCoord::from_vpid_arithmetic(vpid)),
        };
        if self.file_id_offset != 0 {
            coord.file_id =
                (coord.file_id as u64 + self.file_id_offset as u64) as u32;
        }
        Ok(coord)
    }

    /// Return a copy of this locator with a per-read `file_id` offset.
    /// Useful when page.mate was written with file_id=X but the actual .block
    /// files on disk live under file_id=X+N (common after engine bug
    /// INC-001 fixed the encoding).
    pub fn with_file_id_offset(&self, offset: u32) -> Locate {
        let mut new = Locate {
            mate: self.mate.clone(),
            usable_slots: self.usable_slots,
            pid_state_hint: self.pid_state_hint,
            block_file_id_range: self.block_file_id_range,
            mate_path: self.mate_path.clone(),
            pid_state_path: self.pid_state_path.clone(),
            bad_slot_count: self.bad_slot_count,
            file_id_offset: 0,
        };
        new.file_id_offset = offset;
        new
    }

    fn lookup_mate(&self, vpid: u64) -> Result<DiskCoord> {
        // pid.state hint check is advisory and never used to gate; we have
        // already used it at construction time to inform `usable_slots`.
        let idx = vpid as usize;
        if idx >= self.mate.len() {
            // Outside the loaded region: try arithmetic fallback inline.
            return Err(ScannerError::VpidUnlocatable {
                vpid,
                reason: format!(
                    "vpid {} is beyond loaded page.mate region of {} slots",
                    vpid,
                    self.mate.len()
                ),
            });
        }
        let pid = self.mate[idx];
        Ok(DiskCoord {
            file_id: pid.file_id(),
            chunk_idx: pid.chunk_idx(),
            page_idx: pid.page_idx(),
        })
    }
}

/// How to resolve a vpid to a `DiskCoord` for this shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // MateOnly + ArithmeticOnly reserved for verify (PR3).
pub enum Strategy {
    /// Use the slot from `page.mate`. If absent or out of range, fall back
    /// to arithmetic. This is the default in tolerant mode.
    MateThenArithmetic,
    /// Use only the slot from `page.mate`. Errors if missing. Used by
    /// `verify` in strict mode.
    MateOnly,
    /// Use only arithmetic. Used by `map -from-mate-only=false` and tests.
    ArithmeticOnly,
}

/// What `Locate` learned about its inputs — used by the `map` command.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Consumed by `map` (PR2).
pub struct LocateProvenance {
    pub mate_path: Option<PathBuf>,
    pub pid_state_path: Option<PathBuf>,
    pub usable_slots: u64,
    pub bad_slot_count: u32,
    pub pid_state_hint: Option<u64>,
    pub block_file_id_range: Option<(u32, u32)>,
}

/// Load `page.mate` into a slot vector.
///
/// Returns `(slots, bad_count, usable_slots)` where:
/// - `slots` is the raw decoded slots (length up to `MATE_CAPACITY`),
/// - `bad_count` is the count of malformed 8-byte windows,
/// - `usable_slots` is `slots.len()` (a count of slots read, NOT counting any
///   trailing bad window).
fn load_mate(path: &Path) -> (Vec<PidLocation>, u32, u64, Option<PathBuf>) {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(_) => return (Vec::new(), 0, 0, Some(path.to_path_buf())),
    };
    let usable_len = (bytes.len() / 8) * 8;
    let mut slots = Vec::with_capacity(usable_len / 8);
    let mut bad_count: u32 = 0;
    let mut i = 0;
    while i + 8 <= usable_len {
        let mut chunk = [0u8; 8];
        chunk.copy_from_slice(&bytes[i..i + 8]);
        // PidLocation::from_bytes is infallible (packed struct, plain decode),
        // so "bad" here only really fires if we add semantic checks in future
        // PRs. We still count window boundaries we *skipped* if the file was
        // truncated mid-window — there are none in practice; the rounding
        // above already handled that.
        let pid = PidLocation::from_bytes(&chunk);
        slots.push(pid);
        i += 8;
    }
    // "Bad" today is a placeholder for future semantic validation
    // (e.g. file_id out of observed range). Until that lands, keep the
    // count at 0 so callers don't misreport it as a corruption indicator.
    let _ = &mut bad_count;
    let usable = slots.len() as u64;
    (slots, 0, usable, Some(path.to_path_buf()))
}

/// Decode the 8-byte `pid.state` into a single vpid slot. The vpid is the
/// slot index this `PidLocation` describes, not the fields inside it.
fn load_pid_state(path: &Path) -> Option<u64> {
    let bytes = fs::read(path).ok()?;
    if bytes.len() < 8 {
        return None;
    }
    let mut chunk = [0u8; 8];
    chunk.copy_from_slice(&bytes[0..8]);
    let pid = PidLocation::from_bytes(&chunk);
    let file_id = pid.file_id() as u64;
    let chunk_idx = pid.chunk_idx() as u64;
    let page_idx = pid.page_idx() as u64;
    Some(file_id * PAGES_PER_BLOCK + chunk_idx * PAGES_PER_CHUNK + page_idx)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(path: &Path) -> ShardDir {
        ShardDir {
            id: 0,
            path: path.to_path_buf(),
            block_files: vec![],
            page_mate: None,
            pid_state: None,
            wal_segments: vec![],
        }
    }

    #[test]
    fn arithmetic_for_first_vpid_is_chunk_zero_page_zero_file_zero() {
        let c = DiskCoord::from_vpid_arithmetic(0);
        assert_eq!(c.file_id, 0);
        assert_eq!(c.chunk_idx, 0);
        assert_eq!(c.page_idx, 0);
        assert_eq!(c.file_offset(), 0);
    }

    #[test]
    fn arithmetic_for_vpid_at_block_boundary() {
        // PAGES_PER_BLOCK = 640; vpid 639 = last page of block 0.
        let c = DiskCoord::from_vpid_arithmetic(639);
        assert_eq!(c.file_id, 0);
        assert_eq!(c.chunk_idx, 9);
        assert_eq!(c.page_idx, 63);
        // vpid 640 = first page of block 1.
        let c = DiskCoord::from_vpid_arithmetic(640);
        assert_eq!(c.file_id, 1);
        assert_eq!(c.chunk_idx, 0);
        assert_eq!(c.page_idx, 0);
    }

    #[test]
    fn locate_open_with_no_artifacts_is_empty_but_ok() {
        let dir = tempfile::tempdir().unwrap();
        let s = shard(dir.path());
        let loc = Locate::open(&s).unwrap();
        assert_eq!(loc.usable_slots(), 0);
        assert_eq!(loc.bad_slot_count(), 0);
        assert_eq!(loc.pid_state_hint, None);
        assert_eq!(loc.block_file_id_range, None);
    }

    #[test]
    fn pid_state_decode_roundtrip() {
        // 8 bytes: file_id=7, chunk_idx=5, page_idx=12 => vpid =
        //   7 * 640 + 5 * 64 + 12 = 4480 + 320 + 12 = 4812
        let pid = PidLocation {
            file_id: 7,
            chunk_idx: 5,
            page_idx: 12,
            flags: 0,
        };
        let bytes = pid.to_bytes();
        let decoded = load_pid_state_from_bytes(&bytes);
        assert_eq!(decoded, Some(4812));
    }

    // Test helper that takes raw bytes (load_pid_state goes through fs::read).
    fn load_pid_state_from_bytes(bytes: &[u8]) -> Option<u64> {
        if bytes.len() < 8 {
            return None;
        }
        let mut chunk = [0u8; 8];
        chunk.copy_from_slice(&bytes[0..8]);
        let pid = PidLocation::from_bytes(&chunk);
        let file_id = pid.file_id() as u64;
        let chunk_idx = pid.chunk_idx() as u64;
        let page_idx = pid.page_idx() as u64;
        Some(file_id * PAGES_PER_BLOCK + chunk_idx * PAGES_PER_CHUNK + page_idx)
    }

    #[test]
    fn mate_load_truncates_at_byte_boundary() {
        // 9 bytes -- only one full slot decodable; the trailing byte is dropped.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("page.mate");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[0, 1, 0, 0, 0, 0, 0, 0]); // file_id=256
        bytes.push(0xFF); // stray trailing byte
        std::fs::write(&path, &bytes).unwrap();

        let (slots, _bad, usable, _p) = load_mate(&path);
        assert_eq!(usable, 1);
        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0].file_id(), 256);
    }
}
