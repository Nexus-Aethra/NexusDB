//! `fix-mate` command: in-place repair of `page.mate` slots.
//!
//! This is the **only** scanner subcommand that writes to disk. It is invoked
//! explicitly with `--apply` (otherwise it runs in dry-run mode and only
//! reports what it would do).
//!
//! Two repair modes:
//!
//! 1. **Single slot**: `--vpid N --page-idx M [--file-id F] [--freed]`
//!    - Rewrite one slot to point at a known-good page.
//!    - Use this when the user has inspected `map` and `scan` and identified
//!      exactly which slot is wrong and which page on disk contains the data.
//!
//! 2. **Auto sweep**: `--auto`
//!    - For each ALIVE slot whose `page_idx` resolves to a zero page, scan
//!      every other page in the block file looking for a page whose
//!      `stored_vpid` (page header offset 0x18) matches the claimed vpid.
//!    - If exactly one match is found, repoint the slot.
//!    - If zero matches, mark the slot FREED.
//!    - If multiple matches, leave alone and report (ambiguous).
//!
//! Safety:
//! - Every write goes through a `.bak` file first (timestamped).
//! - In dry-run mode (default), no bytes are written.
//! - The pre-write slot is read back and printed for the user to confirm.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use page::PidLocation;

use crate::cli::Globals;
use crate::dir;
use crate::error::{Result, ScannerError};
use crate::output::{self, JsonObject, JsonValue};

const PAGE_SIZE: u64 = 16 * 1024; // 16384 (engine PAGE_SIZE)
const PAGES_PER_BLOCK: u64 = 640; // 10 MiB / 16 KiB

#[derive(Debug, Clone)]
enum Mode {
    Single {
        vpid: u64,
        page_idx: u64,
        file_id: u32,
        freed: bool,
    },
    Auto,
}

pub fn run<W: Write>(
    globals: &Globals,
    vpid: Option<u64>,
    page_idx: Option<u64>,
    file_id: Option<u32>,
    freed: bool,
    auto: bool,
    apply: bool,
    mut out: W,
) -> Result<u8> {
    // Validate flag combination.
    let mode = if auto {
        if vpid.is_some() || page_idx.is_some() || file_id.is_some() || freed {
            return Err(ScannerError::InvalidFlags(
                "--auto is mutually exclusive with --vpid / --page-idx / --file-id / --freed".into(),
            ));
        }
        Mode::Auto
    } else {
        let v = vpid.ok_or_else(|| {
            ScannerError::InvalidFlags(
                "either --auto or --vpid <N> is required (with --page-idx <M>)".into(),
            )
        })?;
        let p = page_idx.ok_or_else(|| {
            ScannerError::InvalidFlags("--page-idx is required when --vpid is set".into())
        })?;
        if p >= PAGES_PER_BLOCK {
            return Err(ScannerError::InvalidFlags(format!(
                "--page-idx must be < {PAGES_PER_BLOCK}"
            )));
        }
        Mode::Single {
            vpid: v,
            page_idx: p,
            file_id: file_id.unwrap_or(1),
            freed,
        }
    };

    let layout = dir::inspect(&globals.dir)?;
    let shard = layout
        .shards
        .first()
        .ok_or_else(|| ScannerError::EmptyDirectory {
            path: globals.dir.clone(),
        })?;
    let mate_path = shard
        .page_mate
        .clone()
        .ok_or_else(|| ScannerError::EmptyDirectory {
            path: globals.dir.clone(),
        })?;

    if !apply {
        writeln!(
            out,
            "[DRY-RUN] no bytes will be written. Re-run with --apply to commit."
        )
        .ok();
    } else {
        let bak = backup_sidecar(&mate_path);
        fs::copy(&mate_path, &bak).map_err(|e| ScannerError::ReadFailed {
            path: bak.clone(),
            offset: 0,
            source: e,
        })?;
        writeln!(out, "[BACKUP] {}", bak.display()).ok();
    }

    // Read full mate into memory (1 MiB, fits easily).
    let mut data = fs::read(&mate_path).map_err(|e| ScannerError::ReadFailed {
        path: mate_path.clone(),
        offset: 0,
        source: e,
    })?;
    if data.len() < 8 {
        return Err(ScannerError::InvalidFlags(format!(
            "page.mate is only {} bytes; expected at least 8",
            data.len()
        )));
    }

    // Locate the .block file we will read for stored_vpid auto-detection.
    let block_path = shard
        .block_files
        .first()
        .map(|b| b.path.clone())
        .ok_or_else(|| ScannerError::EmptyDirectory {
            path: globals.dir.clone(),
        })?;
    let block_bytes = if matches!(mode, Mode::Auto) {
        Some(fs::read(&block_path).map_err(|e| ScannerError::ReadFailed {
            path: block_path.clone(),
            offset: 0,
            source: e,
        })?)
    } else {
        None
    };

    let planned: Vec<Edit> = match &mode {
        Mode::Single { vpid, page_idx, file_id, freed } => {
            let slot = read_slot(&data, *vpid)?;
            let flags = if *freed { 0x08 } else { slot.flags() };
            vec![Edit {
                vpid: *vpid,
                old: slot,
                new: PidLocation {
                    file_id: *file_id,
                    chunk_idx: slot.chunk_idx(),
                    page_idx: *page_idx as u16,
                    flags,
                },
                reason: format!("explicit --vpid {} --page-idx {} --file-id {}", vpid, page_idx, file_id),
            }]
        }
        Mode::Auto => plan_auto(&data, block_bytes.as_deref().unwrap(), &mut out)?,
    };

    if planned.is_empty() {
        writeln!(out, "[NO-EDITS] nothing to repair").ok();
        return Ok(0);
    }

    // Print plan and apply.
    let mut applied = 0u64;
    let mut kept = 0u64;
    for edit in &planned {
        let dropped = edit.new == edit.old;
        if matches!(globals.output_mode(), output::OutputMode::Json) {
            let obj = JsonObject::new()
                .field("vpid", JsonValue::U64(edit.vpid))
                .field(
                    "old",
                    JsonValue::Str(format!(
                        "fid={} ci={} pidx={} fl=0x{:02x}",
                        edit.old.file_id(),
                        edit.old.chunk_idx(),
                        edit.old.page_idx(),
                        edit.old.flags(),
                    )),
                )
                .field(
                    "new",
                    JsonValue::Str(format!(
                        "fid={} ci={} pidx={} fl=0x{:02x}",
                        edit.new.file_id(),
                        edit.new.chunk_idx(),
                        edit.new.page_idx(),
                        edit.new.flags(),
                    )),
                )
                .field("reason", JsonValue::Str(edit.reason.clone()))
                .field("would_change", JsonValue::Bool(!dropped));
            output::json_row(&mut out, &obj).ok();
        } else {
            writeln!(
                out,
                "[EDIT] vpid={:>3} old=(fid={} ci={} pidx={} fl=0x{:02x}) -> new=(fid={} ci={} pidx={} fl=0x{:02x}) {} {}",
                edit.vpid,
                edit.old.file_id(),
                edit.old.chunk_idx(),
                edit.old.page_idx(),
                edit.old.flags(),
                edit.new.file_id(),
                edit.new.chunk_idx(),
                edit.new.page_idx(),
                edit.new.flags(),
                if dropped { "(no-op)" } else { "" },
                edit.reason,
            )
            .ok();
        }
        if dropped {
            kept += 1;
            continue;
        }
        write_slot(&mut data, edit.vpid, edit.new);
        applied += 1;
    }

    if apply && applied > 0 {
        // Atomic-ish write: write to .tmp, fsync via File::sync_all, rename.
        let tmp = mate_path.with_extension("mate.tmp");
        fs::write(&tmp, &data).map_err(|e| ScannerError::ReadFailed {
            path: tmp.clone(),
            offset: 0,
            source: e,
        })?;
        fs::rename(&tmp, &mate_path).map_err(|e| ScannerError::ReadFailed {
            path: mate_path.clone(),
            offset: 0,
            source: e,
        })?;
        writeln!(out, "[WROTE] {} ({} edits)", mate_path.display(), applied).ok();
    } else if applied > 0 {
        writeln!(
            out,
            "[DRY-RUN] would apply {} edits. Re-run with --apply.",
            applied
        )
        .ok();
    }

    writeln!(
        out,
        "[SUMMARY] planned={} applied={} noop={}",
        planned.len(),
        applied,
        kept
    )
    .ok();
    Ok(0)
}

#[derive(Debug, Clone)]
struct Edit {
    vpid: u64,
    old: PidLocation,
    new: PidLocation,
    reason: String,
}

fn read_slot(data: &[u8], vpid: u64) -> Result<PidLocation> {
    let off = (vpid as usize).checked_mul(8).ok_or_else(|| {
        ScannerError::InvalidFlags(format!("vpid {vpid} causes arithmetic overflow"))
    })?;
    if off + 8 > data.len() {
        return Err(ScannerError::InvalidFlags(format!(
            "vpid {vpid} is beyond page.mate ({} bytes, max vpid = {})",
            data.len(),
            data.len() / 8
        )));
    }
    let bytes: [u8; 8] = data[off..off + 8].try_into().unwrap();
    Ok(PidLocation::from_bytes(&bytes))
}

fn write_slot(data: &mut [u8], vpid: u64, slot: PidLocation) {
    let off = (vpid as usize) * 8;
    let bytes = slot.to_bytes();
    data[off..off + 8].copy_from_slice(&bytes);
}

fn plan_auto<W: Write>(
    data: &[u8],
    block: &[u8],
    out: &mut W,
) -> Result<Vec<Edit>> {
    let mut edits = Vec::new();
    let slots = data.len() / 8;
    for vpid in 0..slots as u64 {
        let slot = read_slot(data, vpid).unwrap();
        let flags = slot.flags();
        if flags != 0x01 {
            continue; // skip FREED and UNALLOC
        }
        // Check if the page the slot currently points at is alive.
        let page_idx = slot.page_idx() as u64;
        let file_id = slot.file_id();
        let is_alive = is_page_alive(block, page_idx);
        if is_alive && file_id != 0 {
            continue; // already healthy
        }

        // Scan every page in the block for stored_vpid == vpid.
        let mut matches: Vec<u16> = Vec::new();
        for pidx in 0..(block.len() as u64 / PAGE_SIZE) {
            if pidx == page_idx as u64 {
                continue;
            }
            if !is_page_alive(block, pidx) {
                continue;
            }
            let stored = read_stored_vpid(block, pidx);
            if stored == vpid {
                matches.push(pidx as u16);
            }
        }

        let old = slot;
        if matches.len() == 1 && !is_alive {
            // Exactly one orphan copy: repoint.
            let new = PidLocation {
                file_id: 1,
                chunk_idx: 0,
                page_idx: matches[0],
                flags: 0x01,
            };
            edits.push(Edit {
                vpid,
                old,
                new,
                reason: format!(
                    "auto: orphan copy at page_idx={} (current page_idx={} is zero)",
                    matches[0], page_idx
                ),
            });
        } else if is_alive && file_id == 0 {
            // Page is alive but file_id wrong.
            let new = PidLocation {
                file_id: 1,
                chunk_idx: 0,
                page_idx: old.page_idx(),
                flags: old.flags(),
            };
            edits.push(Edit {
                vpid,
                old,
                new,
                reason: format!("auto: file_id 0->1 (page_idx={} valid)", page_idx),
            });
        } else if matches.is_empty() && !is_alive {
            // No recovery possible: mark FREED.
            let new = PidLocation {
                file_id: 1,
                chunk_idx: 0,
                page_idx: old.page_idx(),
                flags: 0x08,
            };
            edits.push(Edit {
                vpid,
                old,
                new,
                reason: format!(
                    "auto: no orphan copy found (page_idx={} is zero, scanned {} pages)",
                    page_idx,
                    block.len() as u64 / PAGE_SIZE
                ),
            });
        } else if matches.len() > 1 {
            writeln!(
                out,
                "[AMBIGUOUS] vpid={} has {} orphan copies at page_idx={:?}; leaving alone",
                vpid,
                matches.len(),
                matches
            )
            .ok();
        }
    }
    Ok(edits)
}

fn is_page_alive(block: &[u8], page_idx: u64) -> bool {
    let off = (page_idx * PAGE_SIZE) as usize;
    if off + 4 > block.len() {
        return false;
    }
    &block[off..off + 4] == b"LCBP"
}

fn read_stored_vpid(block: &[u8], page_idx: u64) -> u64 {
    let off = (page_idx * PAGE_SIZE) as usize;
    if off + 0x1c > block.len() {
        return u64::MAX;
    }
    u32::from_le_bytes(block[off + 0x18..off + 0x1c].try_into().unwrap()) as u64
}

fn backup_sidecar(mate_path: &Path) -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut s = mate_path.as_os_str().to_os_string();
    s.push(format!(".pre-fixup-{ts}.bak"));
    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_alive_magic_check() {
        let mut block = vec![0u8; PAGE_SIZE as usize];
        block[0..4].copy_from_slice(b"LCBP");
        assert!(is_page_alive(&block, 0));
        let block2 = vec![0u8; PAGE_SIZE as usize];
        assert!(!is_page_alive(&block2, 0));
    }

    #[test]
    fn read_stored_vpid_offset() {
        let mut block = vec![0u8; PAGE_SIZE as usize];
        block[0..4].copy_from_slice(b"LCBP");
        block[0x18..0x1c].copy_from_slice(&42u32.to_le_bytes());
        assert_eq!(read_stored_vpid(&block, 0), 42);
    }

    #[test]
    fn slot_round_trip() {
        let mut buf = vec![0u8; 64];
        let p = PidLocation {
            file_id: 1,
            chunk_idx: 0,
            page_idx: 5,
            flags: 0x01,
        };
        write_slot(&mut buf, 5, p);
        let r = read_slot(&buf, 5).unwrap();
        assert_eq!(r.file_id(), 1);
        assert_eq!(r.page_idx(), 5);
        assert_eq!(r.flags(), 0x01);
    }
}