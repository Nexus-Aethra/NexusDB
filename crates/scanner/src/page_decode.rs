//! Decode a raw page buffer into a structured [`PageReport`].
//!
//! This module is **tolerant by design**: every fallible decode path inside
//! the upstream `page` crate becomes a populated `bad` field on the report.
//! Callers always get back a `PageReport`; they never receive an `Err`.
//!
//! The reported view is intentionally limited in PR1:
//! - For `Leaf` and `Internal` pages we re-use `page::dump::*` to produce a
//!   human-readable text dump. Full structured decoding arrives in PR2.
//! - For `Meta` (vpid 0 convention) we surface the db-name map.
//! - For `Overflow` / `OverflowIndex` we emit a tiny schema.
//! - Bad pages get an explicit `bad_page` field with diagnostic context.
// `prefix_overlap`, `dbs`, `DumpEmpty`, `header_line`, and `try_decode_item`
// are scaffolding for richer decoders (OverflowIndex, MetaPage details) and
// the `range`/`lookup` commands arriving in PR3+.
#[allow(dead_code)]

use page::PageType;
use page::dump;
use page::{Item, ItemKind, decode_item};

use crate::page_io::PAGE_SIZE;

/// All fields that any command might want from a page.
#[derive(Debug, Clone)]
pub struct PageReport {
    /// Resolved page type (best-effort; `Meta` is a placeholder for unknown
    /// raw values).
    pub page_type: PageType,
    /// Raw `page_type` byte (so we can report `0xFF`-style garbage).
    pub page_type_raw: u8,
    /// vpid inside the page header.
    pub vpid: u64,
    /// `key_count` field as reported by the header.
    pub key_count: u16,
    /// `free_off` field as reported by the header.
    pub free_off: u16,
    /// `version` field as reported by the header.
    pub version: u32,
    /// 0x0A prefix-overlap byte (only useful for the page decoder).
    pub prefix_overlap: u16,
    /// `flags` byte (bit0 = dirty, bit1 = in-txn).
    pub flags: u8,
    /// Whether the page passed magic + sanity checks.
    pub magic_ok: bool,
    /// Whether the vpid in the header matches what we asked for.
    pub vpid_matches: bool,
    /// Whether `free_off` is in plausible range.
    pub free_off_in_range: bool,
    /// For `Leaf` / `Internal` only: text dump produced by
    /// `page::dump::*`. `None` if dump itself errored.
    pub dump: Option<String>,
    /// For `Meta` only: db_name -> root_vpid pairs parsed from the
    /// directory's table directory leaf (not vpid 0 -- that's reserved
    /// for the engine's own MetaPage). `Meta` decoding for vpid 0 is a
    /// future PR; for now we report it as a Leaf-style dump.
    pub dbs: Vec<(String, u64)>,
    /// Set if the page is unusable. `tolerant` consumers should treat this
    /// as a flag, not a propagation.
    pub bad: Option<BadPage>,
}

/// Specific reasons a page is unrecoverable for downstream decoding.
#[derive(Debug, Clone)]
pub struct BadPage {
    pub kind: BadKind,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BadKind {
    /// First four bytes were not "LCBP".
    BadMagic,
    /// vpid we looked up does not match the vpid recorded in the page header.
    /// Often a sign the page.mate slot was wrong or the page was recycled.
    VpidMismatch,
    /// `free_off` is < PAGE_HEADER_SIZE or > PAGE_SIZE - FOOTER_SIZE.
    FreeOffOutOfRange,
    /// Page bytes themselves looked fine but `dump_*` returned no text; we
    /// treat this as "no items" rather than corrupt, so it is *not* a `bad`
    /// by itself. Reserved here for future use.
    DumpEmpty,
    /// Page could not be read at all (block file missing, IO error). Used
    /// by callers that attach `BadPage` from `page_io::PageRead` rather
    /// than from byte-level decoding.
    Unreadable,
}

impl PageReport {
    /// Decode a raw 16 KiB page. `expected_vpid` is the vpid the locator
    /// handed us; mismatches are recorded but not propagated.
    pub fn decode(bytes: &[u8; PAGE_SIZE], expected_vpid: u64) -> Self {
        // 1. magic
        let magic_ok = bytes[0..4] == page::PAGE_MAGIC;

        // 2. raw type + parsed PageType
        let page_type_raw = bytes[4];
        let page_type = PageType::from_byte(page_type_raw).unwrap_or(PageType::Meta);

        // 3. vpid in header
        let vpid = page::page_vpid(bytes);

        // 4. vpid match
        let vpid_matches = magic_ok && vpid == expected_vpid;

        // 5. header fields
        let key_count = page::page_key_count(bytes);
        let free_off = page::page_free_off(bytes);
        let version = page::page_version(bytes);
        let prefix_overlap =
            u16::from_le_bytes(bytes[0x0A..0x0C].try_into().expect("header overlap slice"));
        let flags = page::page_flags(bytes);

        // 6. free_off sanity (between header end and before footer)
        let free_off_in_range = free_off >= page::PAGE_HEADER_SIZE as u16
            && free_off <= (PAGE_SIZE - page::PAGE_FOOTER_SIZE) as u16;

        // 7. classify bad-ness
        let bad = if !magic_ok {
            Some(BadPage {
                kind: BadKind::BadMagic,
                detail: format!(
                    "expected {:?}, got {:02X?}",
                    page::PAGE_MAGIC,
                    &bytes[0..4]
                ),
            })
        } else if !vpid_matches {
            Some(BadPage {
                kind: BadKind::VpidMismatch,
                detail: format!(
                    "expected vpid {}, header says {}",
                    expected_vpid, vpid
                ),
            })
        } else if !free_off_in_range {
            Some(BadPage {
                kind: BadKind::FreeOffOutOfRange,
                detail: format!("free_off={}", free_off),
            })
        } else {
            None
        };

        // 8. dispatch dump
        let dump = if bad.is_some() {
            None
        } else {
            match page_type {
                PageType::Leaf => Some(dump::dump_leaf_page(bytes)),
                PageType::Internal => Some(dump::dump_internal_page(bytes)),
                // PR1: Meta / Overflow variants reuse Leaf's printer as a
                // best-effort default; proper decoders arrive in PR2.
                PageType::Meta | PageType::Overflow | PageType::OverflowIndex => {
                    Some(dump::dump_leaf_page(bytes))
                }
            }
        };

        Self {
            page_type,
            page_type_raw,
            vpid,
            key_count,
            free_off,
            version,
            prefix_overlap,
            flags,
            magic_ok,
            vpid_matches,
            free_off_in_range,
            dump,
            dbs: Vec::new(), // populated separately by `decode_meta_directory`
            bad,
        }
    }

    /// Render a one-line summary suitable for the `header` command.
    pub fn header_line(&self) -> String {
        let bad_marker = if self.bad.is_some() { "BAD " } else { "" };
        format!(
            "{bad_marker}{:?} vpid={} key_count={} free_off={} version={} flags=0x{:02x} magic={} vpid_match={}",
            self.page_type,
            self.vpid,
            self.key_count,
            self.free_off,
            self.version,
            self.flags,
            self.magic_ok,
            self.vpid_matches,
        )
    }
}

/// Tolerant wrapper around `page::decode_item`: returns the decoded `(item,
/// item_byte_len)` pair on success. Reserved for callers that want to
/// enumerate individual items without invoking the full `dump_*` text path.
pub fn try_decode_item<'a>(
    page: &'a [u8; PAGE_SIZE],
    item_offset: usize,
    kind: ItemKind,
) -> Option<(Item<'a>, usize)> {
    decode_item(page, item_offset, kind).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a synthetic page that has magic + vpid set; everything else
    /// left as initialised by `leaf_new`. Avoids depending on the engine's
    /// private initialiser.
    fn synth_leaf(vpid: u64) -> [u8; PAGE_SIZE] {
        let mut page = page::leaf_new();
        page::page_set_vpid(&mut page, vpid);
        page
    }

    #[test]
    fn decode_zeroed_page_reports_bad_magic() {
        let buf = [0u8; PAGE_SIZE];
        let r = PageReport::decode(&buf, 0);
        assert!(!r.magic_ok);
        assert!(matches!(r.bad, Some(BadPage { kind: BadKind::BadMagic, .. })));
    }

    #[test]
    fn decode_fresh_leaf_page_passes_all_checks() {
        let page = synth_leaf(7);
        let r = PageReport::decode(&page, 7);
        assert!(r.magic_ok);
        assert!(r.vpid_matches);
        assert!(r.free_off_in_range);
        assert!(r.bad.is_none());
        assert_eq!(r.vpid, 7);
        assert!(r.dump.is_some());
    }

    #[test]
    fn decode_vpid_mismatch_is_recorded_as_bad() {
        let page = synth_leaf(7);
        let r = PageReport::decode(&page, /* expected */ 9);
        assert!(r.magic_ok);
        assert!(!r.vpid_matches);
        assert!(matches!(
            r.bad,
            Some(BadPage { kind: BadKind::VpidMismatch, .. })
        ));
    }
}
