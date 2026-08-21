//! `blame` command: diagnose a bad page's context within a btree.
//!
//! Given a vpid, find its location in the tree (if any) and report:
//! - The travel path from root to the bad page.
//! - Whether the page's siblings are still alive.
//! - Whether the page's parent internal is still valid.
//! - The range of keys this page is responsible for (from parent separators).

use std::io::Write;

use crate::cli::Globals;
use crate::dir;
use crate::error::Result;
use crate::output::{JsonObject, format_vpid, json_row};
use crate::page_io;
use crate::pid::Locate;
use crate::tree::{bfs_travel_path, BfsResult};

pub fn run<W: Write>(globals: &Globals, bad_vpid: u64, tree_root: Option<u64>, mut out: W) -> Result<u8> {
    let layout = dir::inspect(&globals.dir)?;
    let shard = layout
        .shards
        .first()
        .ok_or_else(|| crate::error::ScannerError::EmptyDirectory {
            path: globals.dir.clone(),
        })?;
    let locate = Locate::open_with_override(shard, globals.block_file_id_override)?;

    if let Some(root) = tree_root {
        blame_in_tree(&mut out, globals, &shard, &locate, bad_vpid, root);
    } else {
        // Discover all trees via dbs, then search each one.
        let meta_table_dir = read_meta_page(&shard, &locate);
        if let Some(dbs) = meta_table_dir {
            for (db, table_dir_vpid) in &dbs {
                let table_dir = read_table_dir(&shard, &locate, *table_dir_vpid);
                if let Some(tables) = table_dir {
                    for (table, root_vpid) in &tables {
                        let r = bfs_travel_path(shard, &locate, *root_vpid, |n| n.vpid == bad_vpid);
                        if r.matched.is_some() {
                            writeln!(&mut out, "Found bad vpid={} in db={} table={} root_vpid={}", bad_vpid, db, table, root_vpid).ok();
                            emit_blame_report(&mut out, globals, &r, bad_vpid, *root_vpid);
                            return Ok(0);
                        }
                    }
                }
            }
        }
        writeln!(&mut out, "bad vpid={} was not found in any recognisable tree", bad_vpid).ok();
    }

    Ok(0)
}

fn blame_in_tree<W: Write>(
    out: &mut W,
    globals: &Globals,
    shard: &dir::ShardDir,
    locate: &Locate,
    bad_vpid: u64,
    root_vpid: u64,
) {
    let r = bfs_travel_path(shard, locate, root_vpid, |n| n.vpid == bad_vpid);
    if r.matched.is_none() {
        writeln!(out, "bad vpid={} is not reachable from tree root_vpid={}", bad_vpid, root_vpid).ok();
        return;
    }
    emit_blame_report(out, globals, &r, bad_vpid, root_vpid);
}

fn emit_blame_report<W: Write>(
    out: &mut W,
    globals: &Globals,
    r: &BfsResult,
    bad_vpid: u64,
    root_vpid: u64,
) {
    let vpid_str = format_vpid(bad_vpid, globals.hex_vpid);
    let root_str = format_vpid(root_vpid, globals.hex_vpid);

    match globals.output_mode() {
        crate::output::OutputMode::Human => {
            writeln!(out, "=== Blame report for vpid {} ===", vpid_str).ok();
            writeln!(out, "  root_vpid: {}", root_str).ok();
            writeln!(out, "  tree depth: {}", r.trail.len()).ok();
            writeln!(out, "  pages visited during search: {}", r.visited).ok();
            writeln!(out, "  travel path:").ok();
            for (i, (vpid, depth)) in r.trail.iter().enumerate() {
                let indent = "  ".repeat(*depth as usize);
                writeln!(out, "{}[{}] vpid={} (depth={})", indent, i, format_vpid(*vpid, globals.hex_vpid), depth).ok();
            }
            writeln!(out, "  bad page details: {:?}", r.matched.as_ref().map(|n| &n.bad)).ok();
        }
        crate::output::OutputMode::Json => {
            let trail: Vec<String> = r.trail.iter().map(|(v, d)| format!("vpid={}/depth={}", v, d)).collect();
            let obj = JsonObject::new()
                .field("event", "blame_report")
                .field("bad_vpid", bad_vpid)
                .field("root_vpid", root_vpid)
                .field("tree_depth", r.trail.len() as u64)
                .field("visited", r.visited)
                .field("trail", trail.join("; "));
            json_row(out, &obj).ok();
        }
    }
}

/// Read the MetaPage at vpid 0 to discover db → table_dir_root_vpid mappings.
fn read_meta_page(shard: &dir::ShardDir, locate: &Locate) -> Option<Vec<(String, u64)>> {
    let coord = locate.resolve(0, crate::pid::Strategy::MateThenArithmetic).ok()?;
    let buf = match page_io::read_page(shard, coord) {
        page_io::PageRead::Ok(b) => b,
        _ => return None,
    };
    crate::meta::decode_meta_page(&buf).ok()
}

/// Read a single table directory leaf page.
fn read_table_dir(shard: &dir::ShardDir, locate: &Locate, vpid: u64) -> Option<Vec<(String, u64)>> {
    let coord = locate.resolve(vpid, crate::pid::Strategy::MateThenArithmetic).ok()?;
    let buf = match page_io::read_page(shard, coord) {
        page_io::PageRead::Ok(b) => b,
        _ => return None,
    };
    crate::meta::list_table_dir_leaf(&buf).ok()
}