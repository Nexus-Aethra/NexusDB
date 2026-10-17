//! `header` command: read one page by vpid and print its header (40B) and
//! footer (16B) only — no item-area decoding.
//!
//! `--neighbors` also reads the page's immediate neighbours (vpid-1, vpid+1,
//! and the first/last page in the same chunk) to help diagnose whether a
//! bad page is isolated or part of a wider corruption.

use std::io::Write;

use crate::cli::Globals;
use crate::dir;
use crate::error::Result;
use crate::output::{
    format_vpid, human_header, human_row, json_row, ColumnName, HumanRow, JsonObject,
};
use crate::page_io;
use crate::pid::Locate;

pub fn run<W: Write>(globals: &Globals, vpid: u64, neighbors: bool, mut out: W) -> Result<u8> {
    let layout = dir::inspect(&globals.dir)?;

    // ⭐ 审计 P2-新: 遍历全部 shard — vpid 空间 per-shard, 同一 vpid 在各
    // shard 是不同页; 逐 shard 解码输出 (带 shard 列), 任一 shard 有即有用.
    // 全部 shard 都读不到才报 bad (与旧单 shard 行为一致的外推).
    let mut any_ok = false;
    for shard in &layout.shards {
        let locate = Locate::open_with_override(shard, globals.block_file_id_override)?;
        let coord = locate.resolve(vpid, crate::pid::Strategy::MateThenArithmetic)?;
        let read = page_io::read_page(shard, coord);

        match read {
            page_io::PageRead::Ok(buf) => {
                any_ok = true;
                emit_row(&mut out, globals, shard.id, vpid, &buf);
                // ⭐ 审计 P2 (2026-10-17): strict 模式 — 坏页即非零退出 (CI 语义)
                let r = crate::page_decode::PageReport::decode(&buf, vpid);
                if let Some(b) = &r.bad {
                    globals.strict_gate(format!("vpid {vpid}: {:?}: {}", b.kind, b.detail))?;
                }
            }
            other => {
                emit_bad(&mut out, globals, shard.id, vpid, &other);
                globals.strict_gate(format!("vpid {vpid}: unreadable page"))?;
            }
        }

        if neighbors {
            let neighbor_vpids = compute_neighbor_vpids(vpid);
            for nvpid in &neighbor_vpids {
                let ncoord = match locate.resolve(*nvpid, crate::pid::Strategy::MateThenArithmetic)
                {
                    Ok(c) => c,
                    Err(_) => continue,
                };
                let nread = page_io::read_page(shard, ncoord);
                match nread {
                    page_io::PageRead::Ok(buf) => {
                        emit_neighbor_row(&mut out, globals, shard.id, *nvpid, &buf);
                    }
                    other => emit_bad(&mut out, globals, shard.id, *nvpid, &other),
                }
            }
        }
    }

    if !any_ok && layout.shards.is_empty() {
        return Err(crate::error::ScannerError::EmptyDirectory {
            path: globals.dir.clone(),
        });
    }

    Ok(0)
}

/// Compute the set of neighbour vpids to read for context.
fn compute_neighbor_vpids(vpid: u64) -> Vec<u64> {
    let mut out = Vec::new();
    // Same chunk boundaries: PAGES_PER_CHUNK = 64, so chunk_start = (vpid/64)*64
    let chunk_start = (vpid / 64) * 64;
    let chunk_end = chunk_start + 64;
    // Add the immediate neighbours
    if vpid > chunk_start {
        out.push(vpid - 1);
    }
    if vpid + 1 < chunk_end {
        out.push(vpid + 1);
    }
    // Also add the first and last page of the chunk (if different from neighbours)
    if chunk_start != vpid && chunk_start != vpid - 1 {
        out.push(chunk_start);
    }
    if chunk_end - 1 != vpid && chunk_end - 1 != vpid + 1 {
        out.push(chunk_end - 1);
    }
    out.sort();
    out.dedup();
    out
}

fn emit_row<W: Write>(out: &mut W, globals: &Globals, shard_id: u32, vpid: u64, buf: &[u8; 16384]) {
    let r = crate::page_decode::PageReport::decode(buf, vpid);
    let vpid_str = format_vpid(vpid, globals.hex_vpid);

    match globals.output_mode() {
        crate::output::OutputMode::Human => {
            let columns: &[ColumnName] = &[
                "shard",
                "vpid",
                "page_type",
                "header_vpid",
                "key_count",
                "free_off",
                "version",
                "flags",
                "magic_ok",
                "vpid_match",
                "bad",
            ];
            human_header(out, columns).ok();
            let bad_marker = match r.bad {
                Some(b) => format!("[BAD-PAGE] {:?}", b.kind),
                None => "-".to_string(),
            };
            let row = HumanRow::new()
                .field(shard_id.to_string())
                .field(vpid_str)
                .field(format!("{:?}", r.page_type))
                .field(format!("{}", r.vpid))
                .field(format!("{}", r.key_count))
                .field(format!("{}", r.free_off))
                .field(format!("{}", r.version))
                .field(format!("0x{:02x}", r.flags))
                .field(format!("{}", r.magic_ok))
                .field(format!("{}", r.vpid_matches))
                .field(bad_marker);
            human_row(out, &row).ok();
        }
        crate::output::OutputMode::Json => {
            let obj = JsonObject::new()
                .field("shard", shard_id as u64)
                .field("vpid", vpid)
                .field("page_type", format!("{:?}", r.page_type))
                .field("page_type_raw", r.page_type_raw as u64)
                .field("header_vpid", r.vpid)
                .field("key_count", r.key_count as u64)
                .field("free_off", r.free_off as u64)
                .field("version", r.version as u64)
                .field("flags", r.flags as u64)
                .field("magic_ok", r.magic_ok)
                .field("vpid_match", r.vpid_matches)
                .field("free_off_in_range", r.free_off_in_range)
                .field(
                    "bad_kind",
                    r.bad
                        .as_ref()
                        .map(|b| format!("{:?}", b.kind))
                        .unwrap_or_else(|| "-".to_string()),
                );
            json_row(out, &obj).ok();
        }
    }
}

fn emit_neighbor_row<W: Write>(
    out: &mut W,
    globals: &Globals,
    shard_id: u32,
    vpid: u64,
    buf: &[u8; 16384],
) {
    let r = crate::page_decode::PageReport::decode(buf, vpid);
    let vpid_str = format_vpid(vpid, globals.hex_vpid);

    match globals.output_mode() {
        crate::output::OutputMode::Human => {
            let bad_marker = match r.bad {
                Some(b) => format!("[BAD-PAGE] {:?}", b.kind),
                None => "-".to_string(),
            };
            let row = HumanRow::new()
                .field(shard_id.to_string())
                .field(format!("neighbor vpid={}", vpid_str))
                .field(format!("{:?}", r.page_type))
                .field(format!("{}", r.vpid))
                .field(format!("{}", r.key_count))
                .field(format!("{}", r.free_off))
                .field(format!("{}", r.version))
                .field(format!("0x{:02x}", r.flags))
                .field(format!("{}", r.magic_ok))
                .field(format!("{}", r.vpid_matches))
                .field(bad_marker);
            human_row(out, &row).ok();
        }
        crate::output::OutputMode::Json => {
            let obj = JsonObject::new()
                .field("event", "neighbor")
                .field("shard", shard_id as u64)
                .field("vpid", vpid)
                .field("page_type", format!("{:?}", r.page_type))
                .field("magic_ok", r.magic_ok)
                .field("vpid_match", r.vpid_matches)
                .field("free_off_in_range", r.free_off_in_range)
                .field(
                    "bad_kind",
                    r.bad
                        .as_ref()
                        .map(|b| format!("{:?}", b.kind))
                        .unwrap_or_else(|| "-".to_string()),
                );
            json_row(out, &obj).ok();
        }
    }
}

fn emit_bad<W: Write>(
    out: &mut W,
    globals: &Globals,
    shard_id: u32,
    vpid: u64,
    read: &page_io::PageRead,
) {
    let vpid_str = format_vpid(vpid, globals.hex_vpid);
    let reason = match read {
        page_io::PageRead::BlockFileMissing { file_id } => {
            format!("block file {file_id}.block missing")
        }
        page_io::PageRead::BlockFileTruncated { file_id, size, .. } => {
            format!("block {file_id} truncated (size={size})")
        }
        page_io::PageRead::IoError { source, .. } => format!("io error: {source}"),
        page_io::PageRead::Ok(_) => return,
    };
    match globals.output_mode() {
        crate::output::OutputMode::Human => {
            let row = HumanRow::new()
                .field(format!("shard={shard_id}"))
                .field(format!("[BAD-PAGE] vpid={}", vpid_str))
                .field(reason);
            human_row(out, &row).ok();
        }
        crate::output::OutputMode::Json => {
            let obj = JsonObject::new()
                .field("shard", shard_id as u64)
                .field("bad", true)
                .field("kind", "page_read")
                .field("vpid", vpid)
                .field("reason", reason);
            json_row(out, &obj).ok();
        }
    }
}
