//! `lookup` command: find a specific key inside a btree and report its full item.
//!
//! Walks the btree from root to leaf by comparing keys against internal page
//! separators, then scans the leaf page for the exact key. Output includes the
//! decoded value and the page vpid + item index where it was found.

use std::io::Write;

use page::{decode_item, page_free_off, page_type, page_vpid, ItemKind};

use crate::cli::Globals;
use crate::dir;
use crate::error::Result;
use crate::output::{self, HumanRow, JsonObject, JsonValue};
use crate::page_io;
use crate::pid::{Locate, Strategy};

pub fn run<W: Write>(globals: &Globals, tree_root: u64, key_hex: &str, mut out: W) -> Result<u8> {
    let layout = dir::inspect(&globals.dir)?;

    let target_key = match hex::decode(key_hex) {
        Ok(k) => k,
        Err(e) => {
            writeln!(&mut out, "lookup: invalid hex key: {e}").ok();
            return Ok(1);
        }
    };

    // ⭐ 审计 P2-新: 遍历全部 shard — key 按 hash 路由只可能落在其中一个
    // shard 的树里, 但 scanner 离线无从知道是哪个; 全部搜过以命中为准
    // (正常 0 或 1 个命中; 多命中全部如实报告, 不静默取第一个).
    let mut found = 0u32;
    let mut last_err: Option<String> = None;
    for shard in &layout.shards {
        let locate = Locate::open_with_override(shard, globals.block_file_id_override)?;
        match lookup_key(shard, &locate, tree_root, &target_key, &mut out, globals) {
            Ok(()) => found += 1,
            Err(msg) => last_err = Some(msg),
        }
    }
    if found > 0 {
        Ok(0)
    } else {
        match last_err {
            Some(msg) => {
                writeln!(&mut out, "{msg}").ok();
            }
            None => {
                writeln!(
                    &mut out,
                    "lookup: key {} not found in any shard",
                    hex::encode(target_key)
                )
                .ok();
            }
        }
        Ok(1)
    }
}

fn lookup_key<W: Write>(
    shard: &dir::ShardDir,
    locate: &Locate,
    root_vpid: u64,
    target_key: &[u8],
    out: &mut W,
    globals: &Globals,
) -> std::result::Result<(), String> {
    let mut current_vpid = root_vpid;

    loop {
        let coord = locate
            .resolve(current_vpid, Strategy::MateThenArithmetic)
            .map_err(|e| format!("[BAD-PAGE] vpid={}: {e}", current_vpid))?;

        let buf = match page_io::read_page(shard, coord) {
            page_io::PageRead::Ok(b) => b,
            other => {
                return Err(format!(
                    "[BAD-PAGE] vpid={}: cannot read: {:?}",
                    current_vpid, other
                ));
            }
        };

        let page_bytes: &[u8] = &*buf;
        let pt = page_type(page_bytes);

        match pt {
            page::PageType::Internal => {
                let free_off = page_free_off(page_bytes) as usize;
                let mut off = page::PAGE_HEADER_SIZE;
                let mut prev_key: Vec<u8> = Vec::new();
                // ⭐ 2026-10-17 (审计新 P1-19): first_child 在**页头 vpid
                // 字段**, 哨兵 item 的 child_vpid 恒为 0 (不可用). 旧实现从
                // 哨兵取 first child (恒 0) → target < 首个 separator 时
                // 误报 "dead end / not found" (左子树整个查不到).
                let mut child_vpid = page_vpid(page_bytes);

                while off < free_off {
                    match decode_item(page_bytes, off, ItemKind::Internal) {
                        Ok((item, n)) => {
                            let full = item
                                .full_key(&prev_key)
                                .map_err(|e| format!("corrupt item: {e}"))?;
                            if off == page::PAGE_HEADER_SIZE {
                                // Sentinel: child_vpid unused; first child
                                // comes from the page header (see above).
                            }
                            // If separator <= target_key, the child after
                            // this separator is the right subtree
                            if full.as_slice() <= target_key {
                                child_vpid = item.child_vpid;
                            } else {
                                break;
                            }
                            prev_key = full;
                            off += n;
                        }
                        Err(_) => {
                            return Err(format!(
                                "[BAD-PAGE] vpid={}: item decode error at offset {}",
                                current_vpid, off
                            ));
                        }
                    }
                }

                if child_vpid == 0 || child_vpid == current_vpid {
                    return Err(format!(
                        "lookup: key {} not found (dead end at internal vpid={})",
                        hex::encode(target_key),
                        current_vpid
                    ));
                }
                current_vpid = child_vpid;
            }
            page::PageType::Leaf => {
                let free_off = page_free_off(page_bytes) as usize;
                let mut off = page::PAGE_HEADER_SIZE;
                let mut prev_key: Vec<u8> = Vec::new();
                let mut item_idx: u16 = 0;

                while off < free_off {
                    match decode_item(page_bytes, off, ItemKind::Leaf) {
                        Ok((item, n)) => {
                            let full = item
                                .full_key(&prev_key)
                                .map_err(|e| format!("corrupt item: {e}"))?;
                            if item_idx > 0 && full == target_key {
                                // Found it!
                                let key_hex = hex::encode(&full);
                                let val_hex = hex::encode(item.value);
                                match globals.output_mode() {
                                    output::OutputMode::Human => {
                                        let row = HumanRow::new()
                                            .field(shard.id.to_string())
                                            .field(current_vpid.to_string())
                                            .field(item_idx.to_string())
                                            .field(&key_hex)
                                            .field(&val_hex);
                                        output::human_row(out, &row).ok();
                                    }
                                    output::OutputMode::Json => {
                                        let obj = JsonObject::new()
                                            .field("shard", JsonValue::U64(shard.id as u64))
                                            .field("vpid", JsonValue::U64(current_vpid))
                                            .field("item_idx", JsonValue::U64(item_idx as u64))
                                            .field("key", JsonValue::Str(key_hex))
                                            .field("value", JsonValue::Str(val_hex));
                                        output::json_row(out, &obj).ok();
                                    }
                                }
                                return Ok(());
                            }
                            prev_key = full;
                            item_idx += 1;
                            off += n;
                        }
                        Err(_) => break,
                    }
                }
                return Err(format!(
                    "lookup: key {} not found (reached leaf vpid={})",
                    hex::encode(target_key),
                    current_vpid
                ));
            }
            other => {
                return Err(format!(
                    "[BAD-PAGE] vpid={}: unexpected page type {:?}",
                    current_vpid, other
                ));
            }
        }
    }
}
