//! `export-force`: brute-force key-value scan of every page in every block.
//!
//! Walks pages in `(file_id, page_idx)` ascending order and decodes every
//! Leaf page's items. Internal/Meta/Overflow pages are skipped.
//! Deduplication uses `BTreeMap<Vec<u8>, Vec<u8>>` — later writes overwrite
//! earlier ones, matching the engine's append-only model.
//!
//! Item decode tries two bases: offset 40 (standard) and offset 104 (MetaPage
//! with resolver). If the primary fails, the fallback is tried.

use std::collections::BTreeMap;
use std::io::Write;

use page::{decode_item, ItemKind, PageType, PAGE_HEADER_SIZE, PAGE_SIZE};

use crate::cli::Globals;
use crate::dir;
use crate::error::Result;
use crate::output::{JsonObject, JsonValue};

#[derive(Debug, Clone, Copy, PartialEq)]
enum PageKind {
    Leaf,
    Other,
    BadMagic,
}

struct ScanStats {
    scanned: u64,
    bad: u64,
    leaf: u64,
    decoded: u64,
}

pub fn run<W: Write>(
    globals: &Globals,
    format: ExportForceFormat,
    mut out: W,
) -> Result<u8> {
    let layout = dir::inspect(&globals.dir)?;
    let shard = layout
        .shards
        .first()
        .ok_or_else(|| crate::error::ScannerError::EmptyDirectory {
            path: globals.dir.clone(),
        })?;

    let (merged, stats) = scan_all_blocks(shard);
    let row_count = merged.len() as u64;

    let mut emitted = 0u64;
    for (key, value) in &merged {
        emitted += 1;
        if globals.limit > 0 && emitted > globals.limit as u64 {
            break;
        }
        emit_row(&mut out, globals, &format, key, value)?;
    }

    if matches!(globals.output_mode(), crate::output::OutputMode::Human) {
        writeln!(
            out,
            "[EXPORT-FORCE] scanned_pages={} bad_pages={} leaf_pages={} items_decoded={} items_deduped={} rows_emitted={}",
            stats.scanned, stats.bad, stats.leaf, stats.decoded, stats.decoded - row_count, emitted
        )
        .ok();
    }

    if emitted == 0 {
        writeln!(out, "export-force: zero rows — all pages were bad or unreadable").ok();
        Ok(1)
    } else {
        Ok(0)
    }
}

fn scan_all_blocks(shard: &dir::ShardDir) -> (BTreeMap<Vec<u8>, Vec<u8>>, ScanStats) {
    let mut map = BTreeMap::new();
    let mut stats = ScanStats {
        scanned: 0,
        bad: 0,
        leaf: 0,
        decoded: 0,
    };

    let mut blocks: Vec<&dir::BlockFile> = shard.block_files.iter().collect();
    blocks.sort_by_key(|b| b.file_id);

    for block in blocks {
        let bytes = match std::fs::read(&block.path) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let total_pages = bytes.len() as u64 / PAGE_SIZE as u64;
        for page_idx in 0..total_pages {
            let offset = (page_idx * PAGE_SIZE as u64) as usize;
            if offset + PAGE_SIZE > bytes.len() {
                break;
            }
            let page = &bytes[offset..offset + PAGE_SIZE];
            stats.scanned += 1;

            match classify_page(page) {
                PageKind::Leaf => {
                    stats.leaf += 1;
                    let n = decode_leaf_items(page, &mut map);
                    stats.decoded += n;
                }
                PageKind::BadMagic => {
                    stats.bad += 1;
                }
                _ => {}
            }
        }
    }

    (map, stats)
}

fn classify_page(page: &[u8]) -> PageKind {
    if page.len() < 5 {
        return PageKind::BadMagic;
    }
    if &page[0..4] != b"LCBP" {
        return PageKind::BadMagic;
    }
    match PageType::from_byte(page[4]) {
        Some(PageType::Leaf) => PageKind::Leaf,
        None => PageKind::BadMagic,
        _ => PageKind::Other,
    }
}

fn decode_leaf_items(page: &[u8], map: &mut BTreeMap<Vec<u8>, Vec<u8>>) -> u64 {
    let mut total = 0u64;
    let free_off = if page.len() >= 10 {
        u16::from_le_bytes(page[8..10].try_into().unwrap()) as usize
    } else {
        return 0;
    };

    for &base_offset in &[PAGE_HEADER_SIZE, 104usize] {
        if base_offset > free_off {
            continue;
        }
        let mut off = base_offset;
        let mut prev_key: Vec<u8> = Vec::new();
        let mut decoded_any = false;

        while off < free_off && off < page.len() {
            match decode_item(page, off, ItemKind::Leaf) {
                Ok((item, n)) => {
                    let full = item.full_key(&prev_key);
                    if !full.is_empty() {
                        map.insert(full.clone(), item.value.to_vec());
                        total += 1;
                    }
                    prev_key = full;
                    off += n;
                    decoded_any = true;
                }
                Err(_) => {
                    if decoded_any {
                        break;
                    }
                    break;
                }
            }
        }

        if decoded_any {
            break;
        }
    }

    total
}

#[derive(Debug, Clone, Copy)]
pub enum ExportForceFormat {
    Kv,
    Json,
}

impl ExportForceFormat {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "kv" | "KV" => Some(ExportForceFormat::Kv),
            "json" | "JSON" => Some(ExportForceFormat::Json),
            _ => None,
        }
    }
}

fn emit_row<W: Write>(
    out: &mut W,
    globals: &Globals,
    format: &ExportForceFormat,
    key: &[u8],
    value: &[u8],
) -> std::io::Result<()> {
    let k_hex = hex::encode(key);
    let v_hex = hex::encode(value);
    match globals.output_mode() {
        crate::output::OutputMode::Json => {
            let obj = JsonObject::new()
                .field("key", JsonValue::Str(k_hex))
                .field("value", JsonValue::Str(v_hex));
            crate::output::json_row(out, &obj)
        }
        crate::output::OutputMode::Human => {
            match format {
                ExportForceFormat::Kv => writeln!(out, "{}\t{}", k_hex, v_hex),
                ExportForceFormat::Json => {
                    let obj = JsonObject::new()
                        .field("key", JsonValue::Str(k_hex))
                        .field("value", JsonValue::Str(v_hex));
                    crate::output::json_row(out, &obj)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_leaf_page() {
        let mut p = vec![0u8; PAGE_SIZE];
        p[0..4].copy_from_slice(b"LCBP");
        p[4] = PageType::Leaf as u8;
        assert_eq!(classify_page(&p), PageKind::Leaf);
    }

    #[test]
    fn classify_meta_page() {
        let mut p = vec![0u8; PAGE_SIZE];
        p[0..4].copy_from_slice(b"LCBP");
        p[4] = PageType::Meta as u8;
        assert_eq!(classify_page(&p), PageKind::Other);
    }

    #[test]
    fn classify_bad_magic() {
        let p = vec![0u8; PAGE_SIZE];
        assert_eq!(classify_page(&p), PageKind::BadMagic);
    }

    #[test]
    fn classify_short_page() {
        let p = vec![0u8; 3];
        assert_eq!(classify_page(&p), PageKind::BadMagic);
    }
}