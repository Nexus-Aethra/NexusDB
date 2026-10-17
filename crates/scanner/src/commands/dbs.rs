//! `dbs` command: list every (db_name, table_name, table_root_vpid) triple
//! discoverable from the directory layout.

use std::io::Write;

use crate::cli::Globals;
use crate::dir;
use crate::error::Result;
use crate::meta::list_table_dir_leaf;
use crate::output::{
    format_vpid, human_header, human_row, human_summary, json_row, ColumnName, HumanRow, JsonObject,
};
use crate::page_decode::PageReport;
use crate::page_io;
use crate::pid::Locate;

/// (shard, 该 shard 的 (db, table, root_vpid) 列表).
type ShardTables<'a> = Vec<(&'a dir::ShardDir, Vec<(String, String, u64)>)>;

pub fn run<W: Write>(globals: &Globals, mut out: W) -> Result<u8> {
    let layout = dir::inspect(&globals.dir)?;

    if layout.shards.is_empty() {
        // No shard dir; only top-level WAL files. Still a valid layout
        // (compat: some older configs keep WAL here). Tell the user.
        writeln!(
            out,
            "(no shard directories; only top-level WAL files found)"
        )?;
        return Ok(0);
    }

    // ⭐ 审计 P2-新: 遍历全部 shard (旧实现只取 shards.first(), 多 shard
    // 目录下其余 shard 的表/数据对 scanner 完全不可见 — 救援只救 1/N).
    // 每个 shard 有独立 catalog 与 vpid 空间, 同一 (db, table) 在每个
    // shard 各有一行 (root_vpid 各异或相同, 由 shard 列区分).
    // (shard, tables) 成对收集 — emit 阶段复用同一 Locate (mate 10MB,
    // 逐行重开等于每行读一遍全表).
    let mut per_shard: ShardTables = Vec::new();
    for shard in &layout.shards {
        let locate = Locate::open_with_override(shard, globals.block_file_id_override)?;

        // 1. Read MetaPage at vpid 0 -- yields (db_name -> table_dir_root_vpid).
        let meta_table_dir = match locate.resolve(0, crate::pid::Strategy::MateThenArithmetic) {
            Ok(c) => match page_io::read_page(shard, c) {
                page_io::PageRead::Ok(buf) => Some(buf),
                _ => None,
            },
            Err(_) => None,
        };

        let dbs: Vec<(String, u64)> = match meta_table_dir {
            Some(buf) => match crate::meta::decode_meta_page(&buf) {
                Ok(v) => v,
                Err(e) => {
                    write_bad_meta_page(&mut out, globals, shard.id, 0, &e.to_string())?;
                    Vec::new()
                }
            },
            None => {
                write_bad_meta_page(&mut out, globals, shard.id, 0, "could not read vpid 0")?;
                Vec::new()
            }
        };

        // 2. For each db, read its table_dir root and list tables.
        let mut tables: Vec<(String, String, u64)> = Vec::new(); // (db, table, root_vpid)
        for (db, table_dir_vpid) in &dbs {
            let coord =
                match locate.resolve(*table_dir_vpid, crate::pid::Strategy::MateThenArithmetic) {
                    Ok(c) => c,
                    Err(e) => {
                        write_bad_table_dir(
                            &mut out,
                            globals,
                            shard.id,
                            db,
                            *table_dir_vpid,
                            &e.to_string(),
                        )?;
                        continue;
                    }
                };
            let buf = match page_io::read_page(shard, coord) {
                page_io::PageRead::Ok(b) => b,
                other => {
                    write_bad_table_dir_page(
                        &mut out,
                        globals,
                        shard.id,
                        db,
                        *table_dir_vpid,
                        &other,
                    )?;
                    continue;
                }
            };
            match list_table_dir_leaf(&buf) {
                Ok(t) => {
                    for (table, root_vpid) in t {
                        tables.push((db.clone(), table, root_vpid));
                    }
                }
                Err(e) => {
                    write_bad_table_dir(
                        &mut out,
                        globals,
                        shard.id,
                        db,
                        *table_dir_vpid,
                        &e.to_string(),
                    )?;
                }
            }
        }
        per_shard.push((shard, tables));
    }

    let total: usize = per_shard.iter().map(|(_, t)| t.len()).sum();

    // 3. Emit
    let summary = format!(
        "scanned {} shard(s); found {} table(s)",
        layout.shards.len(),
        total
    );

    match globals.output_mode() {
        crate::output::OutputMode::Human => {
            human_summary(&mut out, &summary)?;
            let columns: &[ColumnName] =
                &["shard", "db_name", "table_name", "root_vpid", "root_type"];
            human_header(&mut out, columns)?;
            for (shard, tables) in &per_shard {
                let locate = Locate::open_with_override(shard, globals.block_file_id_override)?;
                for (db, table, root_vpid) in tables {
                    let coord =
                        locate.resolve(*root_vpid, crate::pid::Strategy::MateThenArithmetic)?;
                    let report = match page_io::read_page(shard, coord) {
                        page_io::PageRead::Ok(buf) => Some(PageReport::decode(&buf, *root_vpid)),
                        _ => None,
                    };
                    let root_type = report
                        .as_ref()
                        .map(|r| format!("{:?}", r.page_type))
                        .unwrap_or_else(|| "?".to_string());

                    let row = HumanRow::new()
                        .field(shard.id.to_string())
                        .field(db.clone())
                        .field(table.clone())
                        .field(format_vpid(*root_vpid, globals.hex_vpid))
                        .field(root_type);
                    human_row(&mut out, &row)?;
                }
            }
        }
        crate::output::OutputMode::Json => {
            for (shard, tables) in &per_shard {
                let locate = Locate::open_with_override(shard, globals.block_file_id_override)?;
                for (db, table, root_vpid) in tables {
                    let coord =
                        locate.resolve(*root_vpid, crate::pid::Strategy::MateThenArithmetic)?;
                    let report = match page_io::read_page(shard, coord) {
                        page_io::PageRead::Ok(buf) => Some(PageReport::decode(&buf, *root_vpid)),
                        _ => None,
                    };
                    let obj = JsonObject::new()
                        .field("shard", shard.id as u64)
                        .field("db_name", db.clone())
                        .field("table_name", table.clone())
                        .field("root_vpid", *root_vpid)
                        .field(
                            "root_type",
                            report
                                .as_ref()
                                .map(|r| format!("{:?}", r.page_type))
                                .unwrap_or_else(|| "?".to_string()),
                        );
                    json_row(&mut out, &obj)?;
                }
            }
        }
    }

    Ok(0)
}

fn write_bad_meta_page<W: Write>(
    out: &mut W,
    globals: &Globals,
    shard_id: u32,
    vpid: u64,
    reason: &str,
) -> Result<()> {
    match globals.output_mode() {
        crate::output::OutputMode::Human => {
            let row = HumanRow::new()
                .field(format!("shard={shard_id}"))
                .field(format!(
                    "[BAD-PAGE] vpid={}",
                    format_vpid(vpid, globals.hex_vpid)
                ))
                .field(reason);
            human_row(out, &row)?;
        }
        crate::output::OutputMode::Json => {
            let obj = JsonObject::new()
                .field("shard", shard_id as u64)
                .field("bad", true)
                .field("kind", "meta_page")
                .field("vpid", vpid)
                .field("reason", reason);
            json_row(out, &obj)?;
        }
    }
    Ok(())
}

fn write_bad_table_dir<W: Write>(
    out: &mut W,
    globals: &Globals,
    shard_id: u32,
    db: &str,
    vpid: u64,
    reason: &str,
) -> Result<()> {
    match globals.output_mode() {
        crate::output::OutputMode::Human => {
            let row = HumanRow::new()
                .field(format!("shard={shard_id}"))
                .field(format!(
                    "[BAD-PAGE] db={} vpid={}",
                    db,
                    format_vpid(vpid, globals.hex_vpid)
                ))
                .field(reason);
            human_row(out, &row)?;
        }
        crate::output::OutputMode::Json => {
            let obj = JsonObject::new()
                .field("shard", shard_id as u64)
                .field("bad", true)
                .field("kind", "table_dir")
                .field("db_name", db)
                .field("vpid", vpid)
                .field("reason", reason);
            json_row(out, &obj)?;
        }
    }
    Ok(())
}

fn write_bad_table_dir_page<W: Write>(
    out: &mut W,
    globals: &Globals,
    shard_id: u32,
    db: &str,
    vpid: u64,
    page_io: &page_io::PageRead,
) -> Result<()> {
    let reason = match page_io {
        page_io::PageRead::BlockFileMissing { file_id } => {
            format!("block file {file_id}.block missing")
        }
        page_io::PageRead::BlockFileTruncated { file_id, size, .. } => {
            format!("block {file_id} truncated (size={size})")
        }
        page_io::PageRead::IoError { source, .. } => format!("io error: {source}"),
        page_io::PageRead::Ok(_) => "page unreadable (tolerated)".to_string(),
    };
    write_bad_table_dir(out, globals, shard_id, db, vpid, &reason)
}
