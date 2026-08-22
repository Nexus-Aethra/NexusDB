//! `wal dump` command: decode one or more WAL segment files byte-by-byte.
//!
//! Reads each segment file, decodes every record, and prints the decoded fields
//! (op, db, table, key_hex, val_hex, crc_ok). Torn writes are reported as a
//! `[TORN-TAIL]` marker at the end of the segment.

use std::io::Write;
use std::path::Path;

use crate::cli::Globals;
use crate::dir;
use crate::error::Result;
use crate::output::{self, HumanRow, JsonObject, JsonValue};
use crate::wal;

pub fn run<W: Write>(
    globals: &Globals,
    seq: u64,
    to_seq: Option<u64>,
    limit: Option<u64>,
    mut out: W,
) -> Result<u8> {
    let layout = dir::inspect(&globals.dir)?;

    // Collect all WAL segments from top-level and shards
    let mut segments = Vec::new();
    for seg in &layout.top_level_wal {
        segments.push(seg);
    }
    for shard in &layout.shards {
        for seg in &shard.wal_segments {
            segments.push(seg);
        }
    }
    segments.sort_by_key(|s| (s.shard_id, s.seq));

    // Filter by seq range
    let end = to_seq.unwrap_or(seq);
    let segments: Vec<_> = segments
        .into_iter()
        .filter(|s| s.seq as u64 >= seq && s.seq as u64 <= end)
        .collect();

    if segments.is_empty() {
        writeln!(&mut out, "no WAL segments found in range {seq}..{end}").ok();
        return Ok(0);
    }

    let max_frames = limit.unwrap_or(u64::MAX);
    let mut frame_count: u64 = 0;
    let mut total_bad: u64 = 0;

    for seg in &segments {
        let path = &seg.path;
        let seg_data = match std::fs::read(path) {
            Ok(d) => d,
            Err(e) => {
                writeln!(&mut out, "[WAL-READ-ERROR] {}: {e}", path.display()).ok();
                total_bad += 1;
                continue;
            }
        };

        let mut pos = 0usize;
        loop {
            if frame_count >= max_frames {
                break;
            }
            if pos + 8 > seg_data.len() {
                if pos < seg_data.len() {
                    // Trailing bytes that don't form a complete header
                    emit_torn_tail(&mut out, globals, seg.shard_id, seg.seq, path, pos)?;
                    total_bad += 1;
                }
                break;
            }

            let payload_len =
                u32::from_le_bytes(seg_data[pos..pos + 4].try_into().unwrap()) as usize;
            let crc_stored =
                u32::from_le_bytes(seg_data[pos + 4..pos + 8].try_into().unwrap());
            let Some(payload) = seg_data.get(pos + 8..pos + 8 + payload_len) else {
                emit_torn_tail(&mut out, globals, seg.shard_id, seg.seq, path, pos)?;
                total_bad += 1;
                break;
            };
            let crc_actual = wal::crc32(payload);
            let crc_ok = crc_actual == crc_stored;

            if !crc_ok {
                emit_torn_tail(&mut out, globals, seg.shard_id, seg.seq, path, pos)?;
                total_bad += 1;
                break;
            }

            if let Some(rec) = wal::decode_payload(payload) {
                emit_record(
                    &mut out,
                    globals,
                    seg.shard_id,
                    seg.seq,
                    &rec,
                    crc_ok,
                    path,
                )?;
                frame_count += 1;
            } else {
                emit_torn_tail(&mut out, globals, seg.shard_id, seg.seq, path, pos)?;
                total_bad += 1;
                break;
            }

            pos += 8 + payload_len;
        }
    }

    if total_bad > 0 && matches!(globals.output_mode(), output::OutputMode::Human) {
        writeln!(&mut out, "[WAL-SUMMARY] {} frames, {} bad/torn", frame_count, total_bad).ok();
    }

    Ok(0)
}

fn emit_record<W: Write>(
    out: &mut W,
    globals: &Globals,
    shard_id: u32,
    seq: u32,
    rec: &wal::WalRecord,
    crc_ok: bool,
    _path: &Path,
) -> std::io::Result<()> {
    let key_hex = hex::encode(&rec.pkey);
    let val_hex = match &rec.value {
        Some(v) => hex::encode(v),
        None => String::new(),
    };
    let op_str = if rec.value.is_some() { "PUT" } else { "DEL" };

    match globals.output_mode() {
        output::OutputMode::Human => {
            let row = HumanRow::new()
                .field(shard_id.to_string())
                .field(seq.to_string())
                .field(op_str)
                .field(&rec.db)
                .field(&rec.table)
                .field(&key_hex)
                .field(&val_hex)
                .field(if crc_ok { "ok" } else { "BAD" });
            output::human_row(out, &row)
        }
        output::OutputMode::Json => {
            let obj = JsonObject::new()
                .field("shard_id", JsonValue::U64(shard_id as u64))
                .field("seq", JsonValue::U64(seq as u64))
                .field("op", JsonValue::Str(op_str.into()))
                .field("db", JsonValue::Str(rec.db.clone()))
                .field("table", JsonValue::Str(rec.table.clone()))
                .field("key", JsonValue::Str(key_hex))
                .field("value", JsonValue::Str(val_hex))
                .field("crc_ok", JsonValue::Bool(crc_ok));
            output::json_row(out, &obj)
        }
    }
}

fn emit_torn_tail<W: Write>(
    out: &mut W,
    globals: &Globals,
    shard_id: u32,
    seq: u32,
    _path: &Path,
    byte_offset: usize,
) -> std::io::Result<()> {
    let msg = format!("[TORN-TAIL] shard={shard_id} seq={seq} offset={byte_offset}");
    match globals.output_mode() {
        output::OutputMode::Human => {
            let row = HumanRow::new().field(&msg);
            output::human_row(out, &row)
        }
        output::OutputMode::Json => {
            let obj = JsonObject::new()
                .field("shard_id", JsonValue::U64(shard_id as u64))
                .field("seq", JsonValue::U64(seq as u64))
                .field("torn_tail", JsonValue::Bool(true))
                .field("offset", JsonValue::U64(byte_offset as u64));
            output::json_row(out, &obj)
        }
    }
}