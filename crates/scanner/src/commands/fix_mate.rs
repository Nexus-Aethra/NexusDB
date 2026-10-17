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
const PAGES_PER_CHUNK: u64 = 64; // 1 MiB / 16 KiB

/// ⭐ 2026-10-17 (审计新 P1-20): .block **文件内页序号** (0..640, 扫描用)
/// → pid 的 (chunk_idx, page_idx). fix_mate 的存活扫描以文件内页序号寻址
/// (偏移 = pidx × 16KiB), 写回 pid 时必须换算 — 旧实现硬编码 chunk_idx=0,
/// chunk>=1 的孤儿页被修成"chunk 0 的不存在页号", **修复工具本身写坏元数据**.
fn split_page_idx(file_page_idx: u64) -> (u8, u16) {
    (
        (file_page_idx / PAGES_PER_CHUNK) as u8,
        (file_page_idx % PAGES_PER_CHUNK) as u16,
    )
}

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

// flag 组合参数 (vpid/page_idx/--fix/--dry-run...) 与业务无关, 收成 struct 反而多一层映射.
#[allow(clippy::too_many_arguments)]
pub fn run<W: Write>(
    globals: &Globals,
    vpid: Option<u64>,
    page_idx: Option<u64>,
    file_id: Option<u32>,
    freed: bool,
    auto: bool,
    apply: bool,
    shard_sel: Option<u32>,
    mut out: W,
) -> Result<u8> {
    // Validate flag combination.
    let mode = if auto {
        if vpid.is_some() || page_idx.is_some() || file_id.is_some() || freed {
            return Err(ScannerError::InvalidFlags(
                "--auto is mutually exclusive with --vpid / --page-idx / --file-id / --freed"
                    .into(),
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
    // ⭐ 审计 P2-新: page.mate 是 per-shard 的 — 旧实现固定 shards.first()
    // (= shard_0), 多 shard 目录下会静默修错 shard 的 mate. 现在: 单 shard
    // 目录沿用旧行为; 多 shard 必须 --shard 显式指定 (修错数据的代价 >> 多
    // 打一个参数).
    let shard = match shard_sel {
        Some(id) => layout.shards.iter().find(|s| s.id == id).ok_or_else(|| {
            ScannerError::InvalidFlags(format!(
                "shard {} not found; available: {:?}",
                id,
                layout.shards.iter().map(|s| s.id).collect::<Vec<_>>()
            ))
        })?,
        None => {
            if layout.shards.len() > 1 {
                return Err(ScannerError::InvalidFlags(format!(
                    "data dir has {} shards; page.mate is per-shard — specify --shard <id> (available: {:?})",
                    layout.shards.len(),
                    layout.shards.iter().map(|s| s.id).collect::<Vec<_>>()
                )));
            }
            layout
                .shards
                .first()
                .ok_or_else(|| ScannerError::EmptyDirectory {
                    path: globals.dir.clone(),
                })?
        }
    };
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
    // ⭐ 审计新 P1-20: 记住它的真实 file_id (auto 修复写回 pid 用, 不再硬编码 1).
    let block_file = shard
        .block_files
        .first()
        .ok_or_else(|| ScannerError::EmptyDirectory {
            path: globals.dir.clone(),
        })?;
    let block_path = block_file.path.clone();
    let block_file_id = block_file.file_id;
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
        Mode::Single {
            vpid,
            page_idx,
            file_id,
            freed,
        } => {
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
                reason: format!(
                    "explicit --vpid {} --page-idx {} --file-id {}",
                    vpid, page_idx, file_id
                ),
            }]
        }
        Mode::Auto => plan_auto(
            &data,
            block_bytes.as_deref().unwrap(),
            block_file_id,
            &mut out,
        )?,
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
    block_file_id: u32,
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
            // ⭐ 审计新 P1-20: matches[0] 是文件内页序号 — 换算成 (chunk,page)
            // 并用**真实** block file_id (非硬编码 1).
            let (new_chunk, new_page) = split_page_idx(matches[0] as u64);
            let new = PidLocation {
                file_id: block_file_id,
                chunk_idx: new_chunk,
                page_idx: new_page,
                flags: 0x01,
            };
            edits.push(Edit {
                vpid,
                old,
                new,
                reason: format!(
                    "auto: orphan copy at file_page_idx={} → (file {}, chunk {}, page {}); current slot points at dead page_idx={}",
                    matches[0], block_file_id, new_chunk, new_page, page_idx
                ),
            });
        } else if is_alive && file_id == 0 {
            // Page is alive but file_id wrong. 同样换算 chunk/page (page_idx
            // 也是文件内序号) 并填真实 file_id.
            let (new_chunk, new_page) = split_page_idx(page_idx);
            let new = PidLocation {
                file_id: block_file_id,
                chunk_idx: new_chunk,
                page_idx: new_page,
                flags: old.flags(),
            };
            edits.push(Edit {
                vpid,
                old,
                new,
                reason: format!(
                    "auto: file_id 0->{} (file_page_idx={} → chunk {} page {})",
                    block_file_id, page_idx, new_chunk, new_page
                ),
            });
        } else if matches.is_empty() && !is_alive {
            // No recovery possible: mark FREED. 保留原坐标 (freed slot 的
            // 位置无语义, 但不应顺手改成硬编码的 (1,0)).
            let new = PidLocation {
                file_id: old.file_id(),
                chunk_idx: old.chunk_idx(),
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

    /// ⭐ 审计新 P1-20: auto 修复必须把文件内页序号换算成 (chunk, page),
    /// 并使用真实 block file_id. 旧实现硬编码 file_id=1/chunk_idx=0 —
    /// chunk>=1 的孤儿页被修成"chunk 0 的不存在页号", 修复工具本身写坏
    /// 元数据; 非默认布局 (file_id≠1) 同样被写错.
    #[test]
    fn auto_repair_converts_file_page_idx_and_uses_real_file_id() {
        // block: 128 页 (2 chunk). page 3 = 死 (全零); page 71 (chunk 1,
        // page 7) = 活页且 stored_vpid = 5.
        let mut block = vec![0u8; PAGE_SIZE as usize * 128];
        let orphan_off = 71 * PAGE_SIZE as usize;
        block[orphan_off..orphan_off + 4].copy_from_slice(b"LCBP");
        block[orphan_off + 0x18..orphan_off + 0x1c].copy_from_slice(&5u32.to_le_bytes());

        // mate: vpid 5 的 slot 指向已死的 page 3 (file 1, chunk 0).
        let mut data = vec![0u8; 8 * 8];
        write_slot(
            &mut data,
            5,
            PidLocation {
                file_id: 1,
                chunk_idx: 0,
                page_idx: 3,
                flags: 0x01,
            },
        );

        let mut out = Vec::new();
        let edits = plan_auto(&data, &block, /* block_file_id */ 7, &mut out).unwrap();
        let e = edits
            .iter()
            .find(|e| e.vpid == 5)
            .expect("应有 vpid 5 的修复");
        assert_eq!(
            (e.new.file_id(), e.new.chunk_idx(), e.new.page_idx()),
            (7, 1, 7),
            "孤儿页在 chunk 1 page 7, file_id=7 — 旧实现会错写成 (1, 0, 71)"
        );
        assert_eq!(e.new.flags(), 0x01);
    }

    /// 找不到孤儿副本 → 标记 FREED, 且不得顺手把坐标改成硬编码 (1,0).
    #[test]
    fn auto_repair_marks_freed_without_rewriting_coordinates() {
        let block = vec![0u8; PAGE_SIZE as usize]; // 全零: 没有活页
        let mut data = vec![0u8; 8 * 16]; // 16 slots (vpid 9 需要)
        write_slot(
            &mut data,
            9,
            PidLocation {
                file_id: 3,
                chunk_idx: 2,
                page_idx: 40,
                flags: 0x01,
            },
        );
        let mut out = Vec::new();
        let edits = plan_auto(&data, &block, 3, &mut out).unwrap();
        let e = edits.iter().find(|e| e.vpid == 9).unwrap();
        assert_eq!(e.new.flags(), 0x08, "应标记 FREED");
        assert_eq!(
            (e.new.file_id(), e.new.chunk_idx(), e.new.page_idx()),
            (3, 2, 40),
            "FREED 应保留原坐标, 不得改写成硬编码值"
        );
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
