//! `range` command: per-page range scan over leaf or meta page items.
//!
//! Reads a single page by vpid and decodes every item whose key falls within
//! the optional `[start, end]` range (inclusive). Outputs key-value pairs in
//! page order.
//!
//! This is a single-page operation — it does not span pages. For tree-wide
//! range scans, use `export`.

use std::io::Write;

use page::{ItemKind, decode_item, page_free_off, page_type};

use crate::cli::Globals;
use crate::dir;
use crate::error::Result;
use crate::output::{self, HumanRow, JsonObject, JsonValue};
use crate::page_io;
use crate::pid::{Locate, Strategy};

pub fn run<W: Write>(
    globals: &Globals,
    vpid: u64,
    start_hex: Option<String>,
    end_hex: Option<String>,
    mut out: W,
) -> Result<u8> {
    let layout = dir::inspect(&globals.dir)?;
    let shard = layout
        .shards
        .first()
        .ok_or_else(|| crate::error::ScannerError::EmptyDirectory {
            path: globals.dir.clone(),
        })?;
    let locate = Locate::open(shard)?;

    let start_key: Option<Vec<u8>> = match start_hex {
        Some(s) if s.is_empty() => None,
        Some(s) => match hex::decode(&s) {
            Ok(k) => Some(k),
            Err(e) => {
                writeln!(&mut out, "range: invalid start hex key: {e}").ok();
                return Ok(1);
            }
        },
        None => None,
    };

    let end_key: Option<Vec<u8>> = match end_hex {
        Some(s) if s.is_empty() => None,
        Some(s) => match hex::decode(&s) {
            Ok(k) => Some(k),
            Err(e) => {
                writeln!(&mut out, "range: invalid end hex key: {e}").ok();
                return Ok(1);
            }
        },
        None => None,
    };

    let coord = locate
        .resolve(vpid, Strategy::MateThenArithmetic)
        .map_err(|e| crate::error::ScannerError::VpidUnlocatable {
            vpid,
            reason: e.to_string(),
        })?;

    let buf = match page_io::read_page(shard, coord) {
        page_io::PageRead::Ok(b) => b,
        other => {
            writeln!(
                &mut out,
                "[BAD-PAGE] vpid={}: cannot read: {:?}",
                vpid, other
            )
            .ok();
            return Ok(1);
        }
    };

    let page_bytes: &[u8] = &*buf;
    let pt = page_type(page_bytes);

    if pt != page::PageType::Leaf && pt != page::PageType::Meta {
        writeln!(
            &mut out,
            "range: vpid={} has page type {:?}, expected Leaf or Meta",
            vpid, pt
        )
        .ok();
        return Ok(1);
    }

    let free_off = page_free_off(page_bytes) as usize;
    let mut off = page::PAGE_HEADER_SIZE;
    let mut prev_key: Vec<u8> = Vec::new();
    let mut item_idx: u16 = 0;

    while off < free_off {
        match decode_item(page_bytes, off, ItemKind::Leaf) {
            Ok((item, n)) => {
                let full = item.full_key(&prev_key);
                if item_idx > 0 {
                    // Check if key is within range
                    let in_range = match (&start_key, &end_key) {
                        (Some(start), Some(end)) => {
                            full.as_slice() >= start.as_slice()
                                && full.as_slice() <= end.as_slice()
                        }
                        (Some(start), None) => full.as_slice() >= start.as_slice(),
                        (None, Some(end)) => full.as_slice() <= end.as_slice(),
                        (None, None) => true,
                    };

                    if in_range {
                        let key_hex = hex::encode(&full);
                        let val_hex = hex::encode(item.value);

                        match globals.output_mode() {
                            output::OutputMode::Human => {
                                let row = HumanRow::new().field(&key_hex).field(&val_hex);
                                output::human_row(&mut out, &row)?;
                            }
                            output::OutputMode::Json => {
                                let obj = JsonObject::new()
                                    .field("key", JsonValue::Str(key_hex))
                                    .field("value", JsonValue::Str(val_hex))
                                    .field("vpid", JsonValue::U64(vpid))
                                    .field("item_idx", JsonValue::U64(item_idx as u64));
                                output::json_row(&mut out, &obj)?;
                            }
                        }
                    }
                }
                prev_key = full;
                item_idx += 1;
                off += n;
            }
            Err(_) => {
                writeln!(
                    &mut out,
                    "[BAD-PAGE] vpid={}: item decode error at offset {}",
                    vpid, off
                )
                .ok();
                break;
            }
        }
    }

    Ok(0)
}