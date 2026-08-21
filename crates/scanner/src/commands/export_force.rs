//! `export-force`: brute-force key-value scan of every page in every block.
//!
//! Walks pages in `(file_id, page_idx)` ascending order and decodes every
//! Leaf page's items. Internal/Meta/Overflow pages are skipped.
//! Deduplication uses `BTreeMap<Vec<u8>, Vec<u8>>` — later writes overwrite
//! earlier ones, matching the engine's append-only model.
//!
//! Item decode tries two bases: offset 40 (standard) and offset 104 (MetaPage
//! with resolver). If the primary fails, the fallback is tried.
//!
//! **Physical key decode**: keyspace uses `[kind: 1B][varint(klen): 1-5B][logical_key]`.
//! `export-force` decodes this so output is structured and reimportable.
//!
//! Output formats:
//! - **kv**: `kind \t table_name \t logical_key_hex \t value_hex` (per row)
//! - **jsonl**: line-delimited JSON with `{kind, table, logical_key, value, pkey_hex}`
//!
//! For reinsertion: pipe into a script that reads each row and calls
//! `table.set(logical_key_bytes, value_bytes)` on a fresh database.

use std::collections::BTreeMap;
use std::io::Write;

use page::{decode_item, ItemKind, PageType, PAGE_HEADER_SIZE, PAGE_SIZE};

use crate::cli::Globals;
use crate::dir;
use crate::error::Result;
use crate::output::{JsonObject, JsonValue};

// Keyspace kind bytes — mirrors `crates/storage/src/keyspace.rs`.
const KIND_STRING: u8 = b'S';
const KIND_HASH: u8 = b'H';
const KIND_LIST: u8 = b'L';
const KIND_SET: u8 = b'T';
const KIND_ZSET: u8 = b'Z';
const KIND_TYPE_META: u8 = b'#';
const KIND_SCHEMA: u8 = b'$';

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

/// Decoded physical key: kind + logical_key_bytes + parsed table name (if
/// logical_key contains a '/' separator).
#[derive(Debug, Clone)]
struct ParsedKey {
    kind_byte: u8,
    kind_name: String,
    logical_key: Vec<u8>,
    table_name: String, // empty if no '/' found
    logical_key_after_table: Vec<u8>,
}

impl ParsedKey {
    /// Decode a physical key: `[kind: 1B][varint(klen): 1-5B][logical_key_body]`.
    /// Then parse logical_key_body to extract table_name and within-table key.
    fn from_physical(pkey: &[u8]) -> Option<Self> {
        if pkey.is_empty() {
            return None;
        }
        let kind = pkey[0];
        let kind_name = kind_name(kind);
        let rest = &pkey[1..];
        let (klen, n) = read_varint_u32(rest)?;
        let body_start = n;
        let body_end = body_start + klen as usize;
        if body_end > rest.len() {
            return None;
        }
        let logical_key = rest[body_start..body_end].to_vec();

        // Try to extract table name: first '/' in logical_key separates
        // table_name from within-table key.
        let (table_name, logical_key_after) = if let Some(slash) = logical_key.iter().position(|b| *b == b'/') {
            let tbl = String::from_utf8_lossy(&logical_key[..slash]).to_string();
            (tbl, logical_key[slash + 1..].to_vec())
        } else {
            (String::new(), logical_key.clone())
        };

        Some(ParsedKey {
            kind_byte: kind,
            kind_name,
            logical_key,
            table_name,
            logical_key_after_table: logical_key_after,
        })
    }
}

fn kind_name(kind: u8) -> String {
    match kind {
        KIND_STRING => "S".to_string(),
        KIND_HASH => "H".to_string(),
        KIND_LIST => "L".to_string(),
        KIND_SET => "T".to_string(),
        KIND_ZSET => "Z".to_string(),
        KIND_TYPE_META => "#".to_string(),
        KIND_SCHEMA => "$".to_string(),
        other => format!("0x{:02x}", other),
    }
}

/// Decode a varint (u32) from `buf`. Returns (value, bytes_consumed).
fn read_varint_u32(buf: &[u8]) -> Option<(u32, usize)> {
    let mut result: u32 = 0;
    let mut shift = 0;
    let mut i = 0;
    loop {
        if i >= buf.len() {
            return None;
        }
        let b = buf[i];
        result |= ((b & 0x7F) as u32) << shift;
        i += 1;
        if (b & 0x80) == 0 {
            return Some((result, i));
        }
        shift += 7;
        if shift >= 35 {
            return None; // overflow
        }
    }
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
    for (raw_key, value) in &merged {
        emitted += 1;
        if globals.limit > 0 && emitted > globals.limit as u64 {
            break;
        }
        let pkey = ParsedKey::from_physical(raw_key);
        emit_row(&mut out, globals, &format, raw_key, &pkey, value)?;
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
    Jsonl,
}

impl ExportForceFormat {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "kv" | "KV" => Some(ExportForceFormat::Kv),
            "jsonl" | "JSONL" | "json" | "JSON" => Some(ExportForceFormat::Jsonl),
            _ => None,
        }
    }
}

fn emit_row<W: Write>(
    out: &mut W,
    globals: &Globals,
    format: &ExportForceFormat,
    raw_key: &[u8],
    parsed: &Option<ParsedKey>,
    value: &[u8],
) -> std::io::Result<()> {
    let pkey_hex = hex::encode(raw_key);
    match globals.output_mode() {
        crate::output::OutputMode::Json | crate::output::OutputMode::Human => {
            match format {
                ExportForceFormat::Jsonl => {
                    let obj = JsonObject::new()
                        .field("kind", JsonValue::Str(match parsed {
                            Some(p) => p.kind_name.clone(),
                            None => "?".to_string(),
                        }))
                        .field("table", JsonValue::Str(match parsed {
                            Some(p) => p.table_name.clone(),
                            None => "".to_string(),
                        }))
                        .field("logical_key", JsonValue::Str(match parsed {
                            Some(p) => hex::encode(&p.logical_key),
                            None => hex::encode(raw_key),
                        }))
                        .field(
                            "logical_key_after_table",
                            JsonValue::Str(match parsed {
                                Some(p) => hex::encode(&p.logical_key_after_table),
                                None => "".to_string(),
                            }),
                        )
                        .field("value", JsonValue::Str(hex::encode(value)))
                        .field("pkey_hex", JsonValue::Str(pkey_hex));
                    crate::output::json_row(out, &obj)
                }
                ExportForceFormat::Kv => {
                    match parsed {
                        Some(p) => {
                            writeln!(
                                out,
                                "{}\t{}\t{}\t{}",
                                p.kind_name,
                                p.table_name,
                                hex::encode(&p.logical_key_after_table),
                                hex::encode(value),
                            )
                        }
                        None => writeln!(out, "?\t\t{}\t{}", pkey_hex, hex::encode(value)),
                    }
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
    fn read_varint_u32_basic() {
        assert_eq!(read_varint_u32(&[0x07u8]), Some((7, 1)));
        assert_eq!(read_varint_u32(&[0x80u8, 0x01u8]), Some((128, 2)));
        assert_eq!(read_varint_u32(&[0xFFu8, 0xFFu8, 0x7Fu8]), Some((2097151, 3)));
    }

    #[test]
    fn parsed_key_from_physical_string() {
        // [S][klen=12][chapters/123]
        let mut pkey = Vec::new();
        pkey.push(b'S');
        pkey.push(12u8); // varint(12) = single byte 0x0C
        pkey.extend_from_slice(b"chapters/123");
        let pk = ParsedKey::from_physical(&pkey).unwrap();
        assert_eq!(pk.kind_byte, b'S');
        assert_eq!(pk.kind_name, "S");
        assert_eq!(pk.table_name, "chapters");
        assert_eq!(pk.logical_key_after_table, b"123");
    }

    #[test]
    fn parsed_key_from_physical_no_table() {
        // [S][klen=5][hello]
        let mut pkey = Vec::new();
        pkey.push(b'S');
        pkey.push(5u8);
        pkey.extend_from_slice(b"hello");
        let pk = ParsedKey::from_physical(&pkey).unwrap();
        assert_eq!(pk.kind_name, "S");
        assert_eq!(pk.table_name, "");
        assert_eq!(pk.logical_key_after_table, b"hello");
    }

    #[test]
    fn parsed_key_hash_kind() {
        let mut pkey = Vec::new();
        pkey.push(b'H');
        pkey.push(13u8); // "users/123:age" = 13 bytes
        pkey.extend_from_slice(b"users/123:age");
        let pk = ParsedKey::from_physical(&pkey).unwrap();
        assert_eq!(pk.kind_name, "H");
        assert_eq!(pk.table_name, "users");
        assert_eq!(pk.logical_key_after_table, b"123:age");
    }
}