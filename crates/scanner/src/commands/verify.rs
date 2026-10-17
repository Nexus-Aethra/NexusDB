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
use crate::output::{
    format_vpid, human_header, human_row, json_row, ColumnName, HumanRow, JsonObject,
};
use crate::pid::Locate;
use crate::tree::{self, bad_kind_name, TreeNode, WalkSummary};

pub fn run<W: Write>(globals: &Globals, tree_root: u64, mut out: W) -> Result<u8> {
    let layout = dir::inspect(&globals.dir)?;

    // ⭐ 审计 P2-新: 遍历全部 shard — vpid 空间 per-shard, 同一 root 在各
    // shard 各一棵树; 汇总计数, 行输出带 shard 列.
    let mut total = tree::WalkSummary {
        visited: 0,
        ok: 0,
        bad: 0,
        unread: 0,
        max_depth: 0,
        cycle: false,
        strict_hit: None,
    };

    match globals.output_mode() {
        crate::output::OutputMode::Human => {
            let columns: &[ColumnName] = &[
                "shard",
                "vpid",
                "page_type",
                "magic_ok",
                "vpid_match",
                "free_off_ok",
                "bad_kind",
                "detail",
            ];
            human_header(&mut out, columns)?;
        }
        crate::output::OutputMode::Json => {}
    }

    for shard in &layout.shards {
        let locate = Locate::open_with_override(shard, globals.block_file_id_override)?;
        let summary = match globals.output_mode() {
            crate::output::OutputMode::Human => {
                tree::walk_tree(shard, &locate, tree_root, globals.is_strict(), |node| {
                    emit_human_row(&mut out, globals, shard.id, node);
                })
            }
            crate::output::OutputMode::Json => {
                tree::walk_tree(shard, &locate, tree_root, globals.is_strict(), |node| {
                    emit_json_row(&mut out, globals, shard.id, node);
                })
            }
        };
        total.visited += summary.visited;
        total.ok += summary.ok;
        total.bad += summary.bad;
        total.unread += summary.unread;
        total.max_depth = total.max_depth.max(summary.max_depth);
        total.cycle |= summary.cycle;
        if total.strict_hit.is_none() {
            total.strict_hit = summary.strict_hit;
        }
    }

    match globals.output_mode() {
        crate::output::OutputMode::Human => emit_summary_human(&mut out, &total),
        crate::output::OutputMode::Json => emit_summary_json(&mut out, &total),
    }

    // ⭐ 审计 P2 (2026-10-17): strict 模式撞到坏页 → 非零退出 (CI 语义)
    if let Some(hit) = &total.strict_hit {
        return Err(crate::error::ScannerError::BadPage(hit.clone()));
    }

    Ok(0)
}

fn emit_human_row<W: Write>(out: &mut W, globals: &Globals, shard_id: u32, node: &TreeNode) {
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
        .field(shard_id.to_string())
        .field(format_vpid(node.vpid, globals.hex_vpid))
        .field(page_type_str)
        .field(format!("{magic_ok}"))
        .field(format!("{vpid_match}"))
        .field(format!("{free_off_ok}"))
        .field(bad_kind)
        .field(detail);
    human_row(out, &row).ok();
}

fn emit_json_row<W: Write>(out: &mut W, globals: &Globals, shard_id: u32, node: &TreeNode) {
    let page_type_str = node
        .page_type
        .map(|t| format!("{t:?}"))
        .unwrap_or_else(|| "?".into());
    let (bad_kind, reason) = match &node.bad {
        Some(b) => (bad_kind_name(b.kind).to_string(), b.reason.clone()),
        None => ("-".to_string(), String::new()),
    };
    let obj = JsonObject::new()
        .field("shard", shard_id as u64)
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
        summary.visited, summary.ok, summary.bad, summary.unread, summary.max_depth, summary.cycle,
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
