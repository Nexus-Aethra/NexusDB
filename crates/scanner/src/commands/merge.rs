//! `merge` command: export a tree with optional WAL replay.
//!
//! This is the full rescue pipeline:
//!
//! 1. Run key-order traversal (`walk_tree_in_order`) to collect all key-value
//!    pairs from the page pool into a `BTreeMap`.
//! 2. If `--include-wal`, read all WAL segments and replay records on top of
//!    the map (PUT = upsert, DELETE = remove).
//! 3. Output the merged stream in kv or json format.
//!
//! Bad pages during traversal are skipped with a summary note at the end.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Write;

use crate::cli::Globals;
use crate::dir;
use crate::error::Result;
use crate::output::OutputMode;
use crate::pid::Locate;
use crate::tree;
use crate::wal;

pub fn run<W: Write>(
    globals: &Globals,
    tree_root: u64,
    format: MergeFormat,
    include_wal: bool,
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

    // Phase 1: collect all key-value pairs from the page pool
    let merged: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let merged_ref = RefCell::new(merged);
    let ok_ref = RefCell::new(0u64);

    let summary = tree::walk_tree_in_order(shard, &locate, tree_root, |key, value| {
        let mut m = merged_ref.borrow_mut();
        m.insert(key.to_vec(), value.to_vec());
        *ok_ref.borrow_mut() += 1;
    });

    let page_pool_bad = (summary.bad + summary.unread) as u64;
    let page_pool_ok = ok_ref.into_inner();
    let mut merged = merged_ref.into_inner();

    // Phase 2: optionally replay WAL
    let mut wal_puts: u64 = 0;
    let mut wal_dels: u64 = 0;
    let mut wal_bad: u64 = 0;

    if include_wal {
        // Collect all WAL segments (top-level + per-shard)
        let mut segments = Vec::new();
        for seg in &layout.top_level_wal {
            segments.push(seg);
        }
        for seg in &shard.wal_segments {
            segments.push(seg);
        }
        segments.sort_by_key(|s| (s.shard_id, s.seq));

        for seg in &segments {
            match wal::read_wal_file(&seg.path) {
                Ok(recs) => {
                    for rec in &recs {
                        match rec.value {
                            Some(ref val) => {
                                merged.insert(rec.pkey.clone(), val.clone());
                                wal_puts += 1;
                            }
                            None => {
                                merged.remove(&rec.pkey);
                                wal_dels += 1;
                            }
                        }
                    }
                }
                Err(e) => {
                    if matches!(globals.output_mode(), OutputMode::Human) {
                        writeln!(
                            &mut out,
                            "[WAL-READ-ERROR] {}: {e}",
                            seg.path.display()
                        )
                        .ok();
                    }
                    wal_bad += 1;
                }
            }
        }
    }

    // Phase 3: output merged stream
    let mut row_count: u64 = 0;
    for (key, value) in &merged {
        match format {
            MergeFormat::Kv => {
                let k_hex = hex::encode(key);
                let v_hex = hex::encode(value);
                writeln!(&mut out, "{}\t{}", k_hex, v_hex).ok();
            }
            MergeFormat::Json => {
                let k_hex = hex::encode(key);
                let v_hex = hex::encode(value);
                writeln!(
                    &mut out,
                    r#"{{"key":"{}","value":"{}"}}"#,
                    k_hex, v_hex
                )
                .ok();
            }
        }
        row_count += 1;
    }

    // Summary
    if matches!(globals.output_mode(), OutputMode::Human) {
        writeln!(
            &mut out,
            "[MERGE-SUMMARY] page_pool: {} ok, {} bad; wal: {} puts, {} dels, {} bad; total rows: {}",
            page_pool_ok, page_pool_bad, wal_puts, wal_dels, wal_bad, row_count
        )
        .ok();
    }

    if row_count == 0 && page_pool_bad > 0 {
        writeln!(&mut out, "merge: zero rows emitted (all pages were bad or unreadable)").ok();
        Ok(1)
    } else {
        Ok(0)
    }
}

/// Output format for the merge stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeFormat {
    /// `hex_key \t hex_value \n`
    Kv,
    /// `{"key":"hex...","value":"hex..."} \n`
    Json,
}

impl MergeFormat {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "kv" | "KV" => Some(MergeFormat::Kv),
            "json" | "JSON" => Some(MergeFormat::Json),
            _ => None,
        }
    }
}