//! `wal list` command: list WAL segment files in the data directory.
//!
//! Walks the inspected directory and lists every WAL segment with its shard id,
//! sequence number, and size. No bytes are read from the segments themselves.

use std::io::Write;

use crate::cli::Globals;
use crate::dir;
use crate::error::Result;
use crate::output::{self, HumanRow, JsonObject, JsonValue};

pub fn run<W: Write>(globals: &Globals, mut out: W) -> Result<u8> {
    let layout = dir::inspect(&globals.dir)?;

    // Collect all WAL segments: top-level + per-shard
    let mut segments: Vec<WalEntry> = Vec::new();

    // Top-level WAL segments
    for seg in &layout.top_level_wal {
        segments.push(WalEntry {
            shard_id: seg.shard_id,
            seq: seg.seq,
            size_bytes: seg.size_bytes,
            path: seg.path.display().to_string(),
        });
    }

    // Per-shard WAL segments
    for shard in &layout.shards {
        for seg in &shard.wal_segments {
            segments.push(WalEntry {
                shard_id: seg.shard_id,
                seq: seg.seq,
                size_bytes: seg.size_bytes,
                path: seg.path.display().to_string(),
            });
        }
    }

    segments.sort_by_key(|e| (e.shard_id, e.seq));

    if segments.is_empty() {
        writeln!(&mut out, "no WAL segments found").ok();
        return Ok(0);
    }

    match globals.output_mode() {
        output::OutputMode::Human => {
            let columns: &[output::ColumnName] = &["shard_id", "seq", "size_bytes", "path"];
            output::human_header(&mut out, columns)?;
            for seg in &segments {
                let row = HumanRow::new()
                    .field(seg.shard_id.to_string())
                    .field(seg.seq.to_string())
                    .field(seg.size_bytes.to_string())
                    .field(&seg.path);
                output::human_row(&mut out, &row)?;
            }
        }
        output::OutputMode::Json => {
            for seg in &segments {
                let obj = JsonObject::new()
                    .field("shard_id", JsonValue::U64(seg.shard_id as u64))
                    .field("seq", JsonValue::U64(seg.seq as u64))
                    .field("size_bytes", JsonValue::U64(seg.size_bytes))
                    .field("path", JsonValue::Str(seg.path.clone()));
                output::json_row(&mut out, &obj)?;
            }
        }
    }

    Ok(0)
}

struct WalEntry {
    shard_id: u32,
    seq: u32,
    size_bytes: u64,
    path: String,
}