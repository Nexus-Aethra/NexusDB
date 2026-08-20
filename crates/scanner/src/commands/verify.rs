//! `verify` command: walk every btree reachable from a given root vpid
//! and report per-page status.
//!
//! This is the read-only half of the full scan. PR3 will add a repair
//! mode (off by default) that turns `[BAD-PAGE]` records into on-disk
//! fix-ups; PR2 ships only the diagnostic surface.

use std::io::Write;

use crate::cli::Globals;
use crate::dir;
use crate::error::Result;
use crate::output::{ColumnName, HumanRow, JsonObject, format_vpid, human_header, human_row, json_row};
use crate::pid::Locate;
use crate::tree::{self, TreeNode, WalkSummary, bad_kind_name};

pub fn run<W: Write>(globals: &Globals, tree_root: u64, mut out: W) -> Result<u8> {
    let layout = dir::inspect(&globals.dir)?;
    let shard = layout
        .shards
        .first()
        .ok_or_else(|| crate::error::ScannerError::EmptyDirectory {
            path: globals.dir.clone(),
        })?;
    let locate = Locate::open(shard)?;

    let summary: WalkSummary = match globals.output_mode() {
        crate::output::OutputMode::Human => {
            let columns: &[ColumnName] = &[
                "vpid",
                "page_type",
                "magic_ok",
                "vpid_match",
                "free_off_ok",
                "bad_kind",
                "detail",
            ];
            human_header(&mut out, columns)?;
            let summary = tree::walk_tree(shard, &locate, tree_root, |node| {
                emit_human_row(&mut out, globals, node);
            });
            emit_summary_human(&mut out, &summary);
            summary
        }
        crate::output::OutputMode::Json => {
            let summary = tree::walk_tree(shard, &locate, tree_root, |node| {
                emit_json_row(&mut out, globals, node);
            });
            emit_summary_json(&mut out, &summary);
            summary
        }
    };

    Ok(0)
}

fn emit_human_row<W: Write>(out: &mut W, globals: &Globals, node: &TreeNode) {
    let page_type_str = node
        .page_type
        .map(|t| format!("{t:?}"))
        .unwrap_or_else(|| "?".into());
    let (bad_kind, detail, magic_ok, vpid_match, free_off_ok) = match &node.bad {
        Some(b) => (
            bad_kind_name(b.kind).to_string(),
            b.reason.clone(),
            // Structural checks only really apply when the page read.
            // When unreadable, the values below are reported as false.
            false,
            false,
            false,
        ),
        None => ("-".to_string(), "-".to_string(), true, true, true),
    };

    let row = HumanRow::new()
        .field(format_vpid(node.vpid, globals.hex_vpid))
        .field(page_type_str)
        .field(format!("{magic_ok}"))
        .field(format!("{vpid_match}"))
        .field(format!("{free_off_ok}"))
        .field(bad_kind)
        .field(detail);
    human_row(out, &row).ok();
}

fn emit_json_row<W: Write>(out: &mut W, globals: &Globals, node: &TreeNode) {
    let page_type_str = node
        .page_type
        .map(|t| format!("{t:?}"))
        .unwrap_or_else(|| "?".into());
    let (bad_kind, reason) = match &node.bad {
        Some(b) => (bad_kind_name(b.kind).to_string(), b.reason.clone()),
        None => ("-".to_string(), String::new()),
    };
    let obj = JsonObject::new()
        .field("vpid", node.vpid)
        .field("page_type", page_type_str)
        .field("bad_kind", bad_kind)
        .field("reason", reason);
    json_row(out, &obj).ok();
    let _ = globals;
}

fn emit_summary_human<W: Write>(out: &mut W, summary: &WalkSummary) {
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "verify summary: visited={} ok={} bad={} unread={} max_depth={} cycle={}",
        summary.visited,
        summary.ok,
        summary.bad,
        summary.unread,
        summary.max_depth,
        summary.cycle,
    );
}

fn emit_summary_json<W: Write>(out: &mut W, summary: &WalkSummary) {
    let obj = JsonObject::new()
        .field("event", "verify_summary")
        .field("visited", summary.visited)
        .field("ok", summary.ok)
        .field("bad", summary.bad)
        .field("unread", summary.unread)
        .field("max_depth", summary.max_depth as u64)
        .field("cycle", summary.cycle);
    json_row(out, &obj).ok();
}
