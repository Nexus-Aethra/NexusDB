//! `export` command: dump an entire btree as a key-value stream.
//!
//! This is the **data-rescue path** — the primary reason the scanner exists.
//! Walks the tree in key order and writes every reachable entry to stdout.
//!
//! ⭐ 审计 P2-新: 遍历全部 shard. vpid 空间是 per-shard 的 — 同一 root_vpid
//! 在每个 shard 各是一棵不同的树, 合起来才是一张表的全量数据 (引擎按 key
//! hash 路由). 输出流不加 shard 列 (key 跨 shard 不相交, 保持既有可重导
//! 入格式); 坏页统计按 shard 汇总.
//!
//! Bad pages are skipped with a `[BAD-PAGE-SKIPPED]` note; the stream
//! continues. The exit code is 0 if any row was emitted.
//!
//! Output formats:
//! - `kv` (default): `physkey_hex \t value_hex \\n` per row.
//! - `json`: line-delimited JSON `{"key":"hex...","value":"hex..."}`.

use std::cell::RefCell;
use std::io::Write;

use crate::cli::Globals;
use crate::dir;
use crate::error::Result;
use crate::output::{human_row, HumanRow};
use crate::pid::Locate;
use crate::tree;

pub fn run<W: Write>(
    globals: &Globals,
    tree_root: u64,
    format: ExportFormat,
    skip_bad: bool,
    mut out: W,
) -> Result<u8> {
    let layout = dir::inspect(&globals.dir)?;

    let out_rc = RefCell::new(&mut out);
    let row_count = RefCell::new(0u64);
    let mut total_bad: u64 = 0;
    let mut total_unread: u64 = 0;
    let mut strict_hit: Option<String> = None;

    for shard in &layout.shards {
        let locate = Locate::open_with_override(shard, globals.block_file_id_override)?;
        let summary = tree::walk_tree_in_order(
            shard,
            &locate,
            tree_root,
            globals.is_strict(),
            |key, value| {
                let mut out_b = out_rc.borrow_mut();
                let mut rc = row_count.borrow_mut();
                match format {
                    ExportFormat::Kv => {
                        let k_hex = hex::encode(key);
                        let v_hex = hex::encode(value);
                        let _ = writeln!(*out_b, "{}\t{}", k_hex, v_hex);
                        *rc += 1;
                    }
                    ExportFormat::Json => {
                        let k_hex = hex::encode(key);
                        let v_hex = hex::encode(value);
                        let _ = writeln!(*out_b, r#"{{"key":"{}","value":"{}"}}"#, k_hex, v_hex);
                        *rc += 1;
                    }
                }
            },
        );
        total_bad += summary.bad;
        total_unread += summary.unread;
        if strict_hit.is_none() {
            strict_hit = summary.strict_hit;
        }
    }

    let _ = skip_bad;
    let rc = row_count.into_inner();
    // ⭐ 审计 P2 (2026-10-17): strict 模式撞到坏页 → 非零退出 (CI 语义)
    if let Some(hit) = &strict_hit {
        return Err(crate::error::ScannerError::BadPage(hit.clone()));
    }
    let skipped = total_bad + total_unread;

    if skipped > 0 {
        let row = HumanRow::new().field(format!(
            "[BAD-PAGE-SKIPPED] {} bad pages, {} unreadable",
            total_bad, total_unread
        ));
        human_row(&mut out, &row).ok();
    }

    if rc == 0 && skipped > 0 {
        writeln!(
            &mut out,
            "export: zero rows emitted (all pages were bad or unreadable)"
        )
        .ok();
        Ok(1)
    } else {
        Ok(0)
    }
}

/// Output format for the export stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    /// `hex_key \t hex_value \\n`
    Kv,
    /// `{"key":"hex...","value":"hex..."} \\n`
    Json,
}

impl ExportFormat {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "kv" | "KV" => Some(ExportFormat::Kv),
            "json" | "JSON" => Some(ExportFormat::Json),
            _ => None,
        }
    }
}
