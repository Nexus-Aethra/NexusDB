//! `rescue-json`: extract all KV pairs into their original JSON form.
//!
//! Runs the same brute-force scan as `export-force`, but instead of emitting
//! a hex stream it writes one `<table>.json` per table to `--out-dir`. Each
//! value's inner JSON (after stripping the leading type byte) is written
//! back as a JSON object.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::cli::Globals;
use crate::dir;
use crate::error::{Result, ScannerError};

use super::export_force::{scan_all_blocks, ParsedKey};

pub fn run(globals: &Globals, out_dir: &Path, ndjson: bool) -> Result<u8> {
    fs::create_dir_all(out_dir).map_err(|e| ScannerError::IoError {
        path: out_dir.to_path_buf(),
        source: e,
    })?;

    let layout = dir::inspect(&globals.dir)?;
    let shard = layout.shards.first().ok_or_else(|| ScannerError::EmptyDirectory {
        path: globals.dir.clone(),
    })?;

    let (merged, stats) = scan_all_blocks(shard);

    // group by table name (parsed from logical_key)
    let mut by_table: BTreeMap<String, Vec<(String, Vec<u8>)>> = BTreeMap::new();
    let mut skipped = 0u64;

    for (raw_key, value) in &merged {
        let parsed = match ParsedKey::from_physical(raw_key) {
            Some(p) => p,
            None => {
                skipped += 1;
                continue;
            }
        };
        let tbl = if parsed.table_name.is_empty() {
            String::from_utf8_lossy(&parsed.logical_key).to_string()
        } else {
            parsed.table_name.clone()
        };
        by_table
            .entry(tbl)
            .or_default()
            .push((parsed.kind_name, value.clone()));
    }

    let mut written_files = 0u64;
    for (table, rows) in by_table {
        let ext = if ndjson { "ndjson" } else { "json" };
        let path: PathBuf = out_dir.join(format!("{}.{}", table, ext));

        let mut f = fs::File::create(&path).map_err(|e| ScannerError::IoError {
            path: path.clone(),
            source: e,
        })?;

        // Decode each value: strip the leading 01 type-byte, then parse the
        // remainder as JSON. Tolerate binary values (they go in a separate
        // file).
        let mut decoded: Vec<serde_json::Value> = Vec::with_capacity(rows.len());
        let mut undecoded_kinds: Vec<(String, String)> = Vec::new();
        for (kind, value) in &rows {
            // The convention: first byte is a type/version marker (commonly
            // 0x01 for JSON text). For values that aren't JSON at all (e.g.
            // raw bytes for compound types like HASH/LIST/SET), we record
            // them as binary attachments.
            let inner = if !value.is_empty() && value[0] == 0x01 {
                &value[1..]
            } else {
                value.as_slice()
            };

            // Try JSON first
            match serde_json::from_slice::<serde_json::Value>(inner) {
                Ok(mut v) => {
                    if let serde_json::Value::Object(ref mut obj) = v {
                        obj.entry("_kind".to_string())
                            .or_insert(serde_json::Value::String(kind.clone()));
                    }
                    decoded.push(v);
                }
                Err(e) => {
                    // Not JSON — record as raw bytes (hex) so we don't lose data
                    let hex_repr = hex::encode(value);
                    undecoded_kinds.push((kind.clone(), hex_repr));
                }
            }
        }

        if ndjson {
            for v in &decoded {
                writeln!(f, "{}", serde_json::to_string(v).unwrap_or_default())
                    .map_err(|e| ScannerError::IoError {
                        path: path.clone(),
                        source: e,
                    })?;
            }
            for (kind, hex_repr) in &undecoded_kinds {
                let obj = serde_json::json!({
                    "_kind": kind,
                    "_raw_hex": hex_repr,
                    "_note": "not JSON; stored as raw hex",
                });
                writeln!(f, "{}", obj).map_err(|e| ScannerError::IoError {
                    path: path.clone(),
                    source: e,
                })?;
            }
        } else {
            // If there are no binary rows, simplify to a bare JSON array
            if undecoded_kinds.is_empty() {
                let s = serde_json::to_string_pretty(&decoded).unwrap_or_else(|_| "[]".to_string());
                f.write_all(s.as_bytes()).map_err(|e| ScannerError::IoError {
                    path: path.clone(),
                    source: e,
                })?;
            } else {
                let output = serde_json::json!({
                    "table": table,
                    "json_rows": decoded,
                    "binary_rows": undecoded_kinds.iter().map(|(k, h)| {
                        serde_json::json!({"kind": k, "raw_hex": h})
                    }).collect::<Vec<_>>(),
                });
                let s = serde_json::to_string_pretty(&output).unwrap_or_else(|_| "[]".to_string());
                f.write_all(s.as_bytes()).map_err(|e| ScannerError::IoError {
                    path: path.clone(),
                    source: e,
                })?;
            }
            f.write_all(b"\n").map_err(|e| ScannerError::IoError {
                path: path.clone(),
                source: e,
            })?;
        }

        writeln!(
            std::io::stderr(),
            "wrote {} ({} json, {} binary) -> {}",
            table,
            decoded.len(),
            undecoded_kinds.len(),
            path.display()
        )
        .ok();
        written_files += 1;
    }

    writeln!(
        std::io::stderr(),
        "[RESCUE-JSON] scanned={} leaf={} decoded={} dedup={} files_written={}",
        stats.scanned,
        stats.leaf,
        stats.decoded,
        stats.decoded - merged.len() as u64,
        written_files
    )
    .ok();

    Ok(0)
}