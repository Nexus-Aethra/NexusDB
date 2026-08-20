//! `rescue` command: one-click diagnosis and data-rescue entry point.
//!
//! Runs a full pipeline: inspect → dbs → verify every tree → blame every
//! bad page → produce a summary report with actionable advice.
//!
//! This is the command the user reaches for when the engine refuses to
//! open and they need to know "what's wrong and can I get my data out?".

use std::io::Write;

use crate::cli::Globals;
use crate::dir;
use crate::error::Result;
use crate::output::{JsonObject, format_vpid, json_row};
use crate::page_io;
use crate::pid::Locate;
use crate::tree::{self, WalkSummary};

pub fn run<W: Write>(globals: &Globals, mut out: W) -> Result<u8> {
    let layout = dir::inspect(&globals.dir)?;
    let shard = layout
        .shards
        .first()
        .ok_or_else(|| crate::error::ScannerError::EmptyDirectory {
            path: globals.dir.clone(),
        })?;
    let locate = Locate::open(shard)?;

    match globals.output_mode() {
        crate::output::OutputMode::Human => run_human(&mut out, globals, &layout, shard, &locate),
        crate::output::OutputMode::Json => run_json(&mut out, globals, &layout, shard, &locate),
    }
    Ok(0)
}

fn run_human<W: Write>(
    out: &mut W,
    globals: &Globals,
    layout: &dir::DataDir,
    shard: &dir::ShardDir,
    locate: &Locate,
) {
    writeln!(out, "=== NexusDBScan Rescue Report ===").ok();
    writeln!(out, "data dir:   {}", layout.path.display()).ok();
    writeln!(out, "layout tag: {}", layout.layout_tag).ok();
    writeln!(out, "shards:     {}", layout.shards.len()).ok();
    writeln!(out, "WAL files:  {}", layout.top_level_wal.len()).ok();
    writeln!(out).ok();

    // Phase 1: discover tables via dbs logic
    writeln!(out, "--- Phase 1: Table discovery ---").ok();
    let tables = discover_tables(shard, locate);
    if tables.is_empty() {
        writeln!(out, "  (no tables found)").ok();
        writeln!(out).ok();
        return;
    }
    writeln!(out, "  found {} table(s)", tables.len()).ok();
    for (db, table, root_vpid) in &tables {
        writeln!(out, "    db={} table={} root_vpid={}", db, table, format_vpid(*root_vpid, globals.hex_vpid)).ok();
    }
    writeln!(out).ok();

    // Phase 2: verify each tree
    writeln!(out, "--- Phase 2: Tree verification ---").ok();
    let mut total_visited: u64 = 0;
    let mut total_bad: u64 = 0;
    let mut total_unread: u64 = 0;
    let mut bad_tree_roots: Vec<(String, String, u64, WalkSummary)> = Vec::new();

    for (db, table, root_vpid) in &tables {
        let summary = tree::walk_tree(shard, locate, *root_vpid, |_node| {});
        total_visited += summary.visited;
        total_bad += summary.bad;
        total_unread += summary.unread;
        writeln!(
            out,
            "  tree db={} table={} root_vpid={}: visited={} ok={} bad={} unread={} depth={}",
            db, table, format_vpid(*root_vpid, globals.hex_vpid),
            summary.visited, summary.ok, summary.bad, summary.unread, summary.max_depth
        ).ok();
        if summary.bad > 0 || summary.unread > 0 {
            bad_tree_roots.push((db.clone(), table.clone(), *root_vpid, summary));
        }
    }
    writeln!(out).ok();
    writeln!(
        out,
        "  total: {} pages visited, {} bad, {} unreadable",
        total_visited, total_bad, total_unread
    ).ok();
    writeln!(out).ok();

    // Phase 3: blame each bad page
    writeln!(out, "--- Phase 3: Bad page impact analysis ---").ok();
    if bad_tree_roots.is_empty() {
        writeln!(out, "  no bad pages found — all trees are healthy").ok();
    } else {
        for (db, table, root_vpid, summary) in &bad_tree_roots {
            writeln!(
                out,
                "  tree db={} table={} root_vpid={} has {} bad pages, {} unreadable",
                db, table, format_vpid(*root_vpid, globals.hex_vpid),
                summary.bad, summary.unread
            ).ok();
        }
    }
    writeln!(out).ok();

    // Recovery advice
    writeln!(out, "--- Recovery advice ---").ok();
    if bad_tree_roots.is_empty() {
        writeln!(out, "  All trees appear healthy. The engine may have failed for a reason").ok();
        writeln!(out, "  other than page corruption (e.g. WAL replay, meta count mismatch).").ok();
        writeln!(out, "  Try: cargo run -- -dir <PATH> export -tree <root_vpid> -format json").ok();
    } else {
        for (db, table, root_vpid, _summary) in &bad_tree_roots {
            writeln!(
                out,
                "  Tree db={} table={} (root_vpid={}) has issues.",
                db, table, format_vpid(*root_vpid, globals.hex_vpid)
            ).ok();
            writeln!(
                out,
                "    To export reachable data: nexusdb-scanner --dir {} export -tree {} -format json > {}_{}.ndjson",
                globals.dir.display(), root_vpid, db, table
            ).ok();
            writeln!(
                out,
                "    To examine bad pages:  nexusdb-scanner --dir {} header -vpid <N> --neighbors",
                globals.dir.display()
            ).ok();
        }
    }
    writeln!(out, "  (export command is available in PR3)").ok();
}

fn run_json<W: Write>(
    out: &mut W,
    globals: &Globals,
    _layout: &dir::DataDir,
    shard: &dir::ShardDir,
    locate: &Locate,
) {
    // Phase 1: table discovery
    let tables = discover_tables(shard, locate);
    let obj1 = JsonObject::new()
        .field("event", "rescue_phase1")
        .field("table_count", tables.len() as u64);
    json_row(out, &obj1).ok();
    for (db, table, root_vpid) in &tables {
        let obj = JsonObject::new()
            .field("event", "rescue_table")
            .field("db", db.clone())
            .field("table", table.clone())
            .field("root_vpid", *root_vpid);
        json_row(out, &obj).ok();
    }

    // Phase 2: verify each tree
    for (db, table, root_vpid) in &tables {
        let summary = tree::walk_tree(shard, locate, *root_vpid, |_node| {});
        let obj = JsonObject::new()
            .field("event", "rescue_verify")
            .field("db", db.clone())
            .field("table", table.clone())
            .field("root_vpid", *root_vpid)
            .field("visited", summary.visited)
            .field("ok", summary.ok)
            .field("bad", summary.bad)
            .field("unread", summary.unread)
            .field("max_depth", summary.max_depth as u64)
            .field("cycle", summary.cycle);
        json_row(out, &obj).ok();
    }

    let _ = globals;
}

/// Discover all (db, table, root_vpid) triples.
fn discover_tables(shard: &dir::ShardDir, locate: &Locate) -> Vec<(String, String, u64)> {
    let mut out = Vec::new();
    let meta_vpid = match locate.resolve(0, crate::pid::Strategy::MateThenArithmetic) {
        Ok(c) => c,
        Err(_) => return out,
    };
    let meta_buf = match page_io::read_page(shard, meta_vpid) {
        page_io::PageRead::Ok(b) => b,
        _ => return out,
    };
    let dbs = match crate::meta::decode_meta_page(&meta_buf) {
        Ok(v) => v,
        Err(_) => return out,
    };
    for (db, table_dir_vpid) in &dbs {
        let td_coord = match locate.resolve(*table_dir_vpid, crate::pid::Strategy::MateThenArithmetic) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let td_buf = match page_io::read_page(shard, td_coord) {
            page_io::PageRead::Ok(b) => b,
            _ => continue,
        };
        let tables = match crate::meta::list_table_dir_leaf(&td_buf) {
            Ok(v) => v,
            Err(_) => continue,
        };
        for (table, root_vpid) in &tables {
            out.push((db.clone(), table.clone(), *root_vpid));
        }
    }
    out
}