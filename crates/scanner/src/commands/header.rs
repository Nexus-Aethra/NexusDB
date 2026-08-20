//! `header` command: read one page by vpid and print its header (40B) and
//! footer (16B) only — no item-area decoding.

use std::io::Write;

use crate::cli::Globals;
use crate::dir;
use crate::error::Result;
use crate::output::{
    ColumnName, HumanRow, JsonObject, format_vpid, human_header, human_row, json_row,
};
use crate::page_io;
use crate::pid::Locate;

pub fn run<W: Write>(globals: &Globals, vpid: u64, mut out: W) -> Result<u8> {
    let layout = dir::inspect(&globals.dir)?;
    let shard = layout
        .shards
        .first()
        .ok_or_else(|| crate::error::ScannerError::EmptyDirectory {
            path: globals.dir.clone(),
        })?;
    let locate = Locate::open(shard)?;

    let coord = locate.resolve(vpid, crate::pid::Strategy::MateThenArithmetic)?;
    let read = page_io::read_page(shard, coord);

    match read {
        page_io::PageRead::Ok(buf) => {
            emit_row(&mut out, globals, vpid, &buf);
        }
        other => emit_bad(&mut out, globals, vpid, &other),
    }
    Ok(0)
}

fn emit_row<W: Write>(out: &mut W, globals: &Globals, vpid: u64, buf: &[u8; 16384]) {
    let r = crate::page_decode::PageReport::decode(buf, vpid);
    let vpid_str = format_vpid(vpid, globals.hex_vpid);

    match globals.output_mode() {
        crate::output::OutputMode::Human => {
            let columns: &[ColumnName] = &[
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

fn emit_bad<W: Write>(out: &mut W, globals: &Globals, vpid: u64, read: &page_io::PageRead) {
    let vpid_str = format_vpid(vpid, globals.hex_vpid);
    let reason = match read {
        page_io::PageRead::BlockFileMissing { file_id } => {
            format!("block file {file_id}.block missing")
        }
        page_io::PageRead::BlockFileTruncated {
            file_id,
            size,
            ..
        } => {
            format!("block {file_id} truncated (size={size})")
        }
        page_io::PageRead::IoError { source, .. } => format!("io error: {source}"),
        page_io::PageRead::Ok(_) => return,
    };
    match globals.output_mode() {
        crate::output::OutputMode::Human => {
            let row = HumanRow::new()
                .field(format!("[BAD-PAGE] vpid={}", vpid_str))
                .field(reason);
            human_row(out, &row).ok();
        }
        crate::output::OutputMode::Json => {
            let obj = JsonObject::new()
                .field("bad", true)
                .field("kind", "page_read")
                .field("vpid", vpid)
                .field("reason", reason);
            json_row(out, &obj).ok();
        }
    }
}
