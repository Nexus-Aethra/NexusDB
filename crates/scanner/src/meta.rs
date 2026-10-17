//! Decoders that emulate what `storage::meta_page::MetaPage::load` and
//! `storage::table_directory::TableDirectory::list_tables` do, but using only
//! the page bytes we read off disk — never invoking a live Pager.
//!
//! These decoders are **partial**: they trust that the page is well-formed
//! and refuse rather than repair. The scanner's `tolerant` mode encapsulates
//! them so that callers receive a `Bad` row on failure.

use page::{ItemKind, PAGE_HEADER_SIZE};

use crate::page_io::PAGE_SIZE;

/// MetaPage 的 resolver 段宽度 (引擎 `DbNameResolver::RESOLVER_HEADER_SIZE`).
/// item 区从 PAGE_HEADER_SIZE + 1024 开始; scanner 旧实现从 40 起跳, 把
/// resolver 段当成 item 解 → "shared_prefix_len exceeds prev_key" (修复前
/// dbs/export/lookup/verify 在现代引擎目录上全部不可用).
const RESOLVER_HEADER_SIZE: usize = 1024;
const META_ITEM_AREA_START: usize = PAGE_HEADER_SIZE + RESOLVER_HEADER_SIZE;

/// Decode the `MetaPage` at vpid 0.
pub fn decode_meta_page(page: &[u8; PAGE_SIZE]) -> Result<Vec<(String, u64)>, MetaError> {
    walk_meta_items(page, META_ITEM_AREA_START).map_err(|e| MetaError { reason: e })
}

/// Open a single leaf page as a table directory and list every (table_name,
/// root_vpid) it contains.
pub fn list_table_dir_leaf(page: &[u8; PAGE_SIZE]) -> Result<Vec<(String, u64)>, TableDirError> {
    // table_dir 页无 resolver 段, item 区从 PAGE_HEADER_SIZE 开始.
    walk_meta_items(page, PAGE_HEADER_SIZE).map_err(|e| TableDirError { reason: e })
}

/// Walk a meta-like leaf (sentinel at item 0, then `(utf8 name, 8B vpid)` pairs).
/// `item_start` = item 区起始偏移 (MetaPage 104 / table_dir 40).
fn walk_meta_items(
    page: &[u8; PAGE_SIZE],
    item_start: usize,
) -> Result<Vec<(String, u64)>, String> {
    let key_count = page::page_key_count(page) as usize;
    let mut out = Vec::new();
    let mut prev_key: Vec<u8> = Vec::new();
    let mut off = item_start;

    for i in 0..key_count + 1 {
        let (item, n) = page::decode_item(page, off, ItemKind::Leaf)
            .map_err(|e| format!("decode_item[{i}]: {e:?}"))?;
        let full_key = item
            .full_key(&prev_key)
            .map_err(|e| format!("corrupt item: {e}"))?;
        off += n;

        if i == 0 {
            if !full_key.is_empty() {
                return Err("expected empty sentinel at item 0".into());
            }
        } else {
            if item.value.len() != 8 {
                return Err(format!(
                    "value at item {i} should be 8 bytes, got {}",
                    item.value.len()
                ));
            }
            let vpid = u64::from_le_bytes(item.value[..8].try_into().expect("8B"));
            let name = String::from_utf8(full_key.clone())
                .map_err(|_| format!("non-utf8 name at item {i}: {full_key:?}"))?;
            out.push((name, vpid));
        }
        prev_key = full_key;
    }

    Ok(out)
}

#[derive(Debug, Clone)]
pub struct MetaError {
    pub reason: String,
}

impl std::fmt::Display for MetaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for MetaError {}

#[derive(Debug, Clone)]
pub struct TableDirError {
    pub reason: String,
}

impl std::fmt::Display for TableDirError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for TableDirError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a synthetic leaf page with one sentinel + N (key, value) items.
    fn synth_leaf_with(items: &[(&[u8], &[u8])]) -> [u8; PAGE_SIZE] {
        let mut page = page::leaf_new();
        for (k, v) in items {
            page::leaf_insert(&mut page, k, v).expect("leaf_insert in synth_leaf_with");
        }
        page
    }

    #[test]
    fn list_table_dir_roundtrips_three_tables() {
        let page = synth_leaf_with(&[
            (b"alpha", &7u64.to_le_bytes()),
            (b"bravo", &42u64.to_le_bytes()),
            (b"charlie", &[0xFFu8; 8]),
        ]);
        let got = list_table_dir_leaf(&page).unwrap();
        assert_eq!(
            got,
            vec![
                ("alpha".to_string(), 7),
                ("bravo".to_string(), 42),
                ("charlie".to_string(), 0xFFFFFFFFFFFFFFFF),
            ]
        );
    }

    #[test]
    fn empty_table_dir_returns_empty_vec() {
        let page = synth_leaf_with(&[]);
        let got = list_table_dir_leaf(&page).unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn malformed_page_is_an_error_not_a_panic() {
        // page with garbage item area
        let mut page = [0u8; PAGE_SIZE];
        page[..4].copy_from_slice(b"LCBP");
        page[4] = 3; // Leaf
        page::page_set_vpid(&mut page, 0);
        page::page_set_key_count(&mut page, 5); // claim 5 items but bytes are 0
        let r = list_table_dir_leaf(&page);
        assert!(r.is_err());
    }

    /// 合成带 resolver 段的 MetaPage (item 区从 104 开始, 结构对齐引擎
    /// `meta_page::leaf_new_with_vpid` + `init_meta_sentinel`).
    fn synth_meta_page(items: &[(&[u8], &[u8])]) -> [u8; PAGE_SIZE] {
        use page::{
            encode_leaf_item, page_init_header, page_set_free_off, write_checkpoint,
            write_checkpoint_header, Checkpoint, CheckpointHeader,
        };
        let mut page = [0u8; PAGE_SIZE];
        page_init_header(&mut page, page::PageType::Leaf);
        // resolver 段 [40..104]: 内容对解码无意义, 填非零模拟真实布局
        for b in &mut page[PAGE_HEADER_SIZE..META_ITEM_AREA_START] {
            *b = 0xAB;
        }
        // 哨兵写在 item 区起点 (复制引擎 init_meta_sentinel)
        let mut buf = [0u8; 4096];
        let n = encode_leaf_item(&mut buf, &[], b"", b"[]").expect("encode sentinel");
        page[META_ITEM_AREA_START..META_ITEM_AREA_START + n].copy_from_slice(&buf[..n]);
        page_set_free_off(&mut page, (META_ITEM_AREA_START + n) as u16);
        write_checkpoint_header(
            &mut page,
            CheckpointHeader {
                checkpoint_count: 1,
                ..Default::default()
            },
        );
        write_checkpoint(
            &mut page,
            0,
            Checkpoint {
                item_count: 1,
                first_item_off: META_ITEM_AREA_START as u16,
            },
        );
        for (k, v) in items {
            page::leaf_insert(&mut page, k, v).expect("leaf_insert in synth_meta_page");
        }
        page
    }

    /// ⭐ 审计 P2-新: MetaPage item 区从 104 起 (resolver 段占 [40..104]).
    /// 旧实现从 40 起跳 → 把 resolver 字节当 item 解, "shared_prefix_len
    /// exceeds prev_key" — 现代引擎目录上 dbs/export/lookup 全部不可用.
    #[test]
    fn decode_meta_page_skips_resolver_segment() {
        let page = synth_meta_page(&[
            (b"app", &9u64.to_le_bytes()),
            (b"study", &12u64.to_le_bytes()),
        ]);
        let got = decode_meta_page(&page).unwrap();
        assert_eq!(got, vec![("app".to_string(), 9), ("study".to_string(), 12)]);
    }

    /// resolver 段在、item 区为空 → 空 vec (非错误).
    #[test]
    fn decode_meta_page_empty_dbs() {
        let page = synth_meta_page(&[]);
        let got = decode_meta_page(&page).unwrap();
        assert!(got.is_empty());
    }
}
