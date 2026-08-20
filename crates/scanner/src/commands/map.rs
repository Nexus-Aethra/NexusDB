//! `map` command: dump the vpid → (file_id, chunk_idx, page_idx) mapping.
//!
//! Reads page.mate and pid.state via the existing `Locate` infrastructure and
//! displays the resolved mapping for every vpid slot. Optionally falls back to
//! arithmetic lookup for slots beyond the loaded mate region.

use std::io::Write;

use crate::cli::Globals;
use crate::dir;
use crate::error::Result;
use crate::output::{self, HumanRow, JsonObject, JsonValue};
use crate::pid::{DiskCoord, Locate, MATE_CAPACITY, PAGES_PER_BLOCK, Strategy};

pub fn run<W: Write>(globals: &Globals, from_mate_only: bool, mut out: W) -> Result<u8> {
    let layout = dir::inspect(&globals.dir)?;
    let shard = layout
        .shards
        .first()
        .ok_or_else(|| crate::error::ScannerError::EmptyDirectory {
            path: globals.dir.clone(),
        })?;
    let locate = Locate::open(shard)?;
    let provenance = locate.provenance();

    let strategy = if from_mate_only {
        Strategy::MateOnly
    } else {
        Strategy::MateThenArithmetic
    };

    // Determine how many vpid slots to display
    let max_vpid = if from_mate_only {
        provenance.usable_slots
    } else {
        // Show up to mate capacity, or the arithmetic-sensible range
        let mate_max = MATE_CAPACITY;
        let block_max = provenance.block_file_id_range.map_or(0, |(_, max_id)| {
            (max_id as u64 + 1) * PAGES_PER_BLOCK
        });
        mate_max.max(block_max)
    };

    let mut row_count: u64 = 0;

    for vpid in 0..max_vpid {
        let (coord, source) = match locate.resolve(vpid, strategy) {
            Ok(c) => (c, "mate"),
            Err(_) => {
                if from_mate_only {
                    continue;
                }
                match locate.resolve(vpid, Strategy::ArithmeticOnly) {
                    Ok(c) => (c, "arithmetic"),
                    Err(_) => continue,
                }
            }
        };

        // Get flags from mate slot if available
        let flags_str = if vpid < provenance.usable_slots {
            match locate.mate_slot(vpid) {
                Some(pid) => format!("0x{:02x}", pid.flags()),
                None => "--".into(),
            }
        } else {
            "--".into()
        };

        if from_mate_only && flags_str == "0x00" {
            continue;
        }

        emit_map_entry(&mut out, globals, vpid, coord, &flags_str, source)?;
        row_count += 1;
    }

    // Summary
    if matches!(globals.output_mode(), output::OutputMode::Human) {
        writeln!(
            &mut out,
            "[MAP-SUMMARY] {} entries, {} usable mate slots, {} bad slots",
            row_count, provenance.usable_slots, provenance.bad_slot_count
        )
        .ok();
    }

    Ok(0)
}

fn emit_map_entry<W: Write>(
    out: &mut W,
    globals: &Globals,
    vpid: u64,
    coord: DiskCoord,
    flags: &str,
    source: &str,
) -> std::io::Result<()> {
    let vpid_str = crate::output::format_vpid(vpid, globals.hex_vpid);

    match globals.output_mode() {
        output::OutputMode::Human => {
            let row = HumanRow::new()
                .field(vpid_str)
                .field(coord.file_id.to_string())
                .field(coord.chunk_idx.to_string())
                .field(coord.page_idx.to_string())
                .field(flags)
                .field(source);
            output::human_row(out, &row)
        }
        output::OutputMode::Json => {
            let obj = JsonObject::new()
                .field("vpid", JsonValue::U64(vpid))
                .field("file_id", JsonValue::U64(coord.file_id as u64))
                .field("chunk_idx", JsonValue::U64(coord.chunk_idx as u64))
                .field("page_idx", JsonValue::U64(coord.page_idx as u64))
                .field("flags", JsonValue::Str(flags.to_string()))
                .field("source", JsonValue::Str(source.into()));
            output::json_row(out, &obj)
        }
    }
}