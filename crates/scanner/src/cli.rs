//! CLI: clap-based argument parsing for `nexusdb-scanner`.
//!
//! Three layers of args:
//! 1. **Global flags** that apply to *all* subcommands (e.g. `--dir`,
//!    `--tolerant`, `--json`, `--hex-vpid`).
//! 2. **Subcommand** name (required positional-like via clap derive).
//! 3. **Per-subcommand** args.
//!
//! `clap` does all of this for us. We split into `Args` (globals) and
//! `Command` (enum) so dispatch stays trivial in `main.rs`.
// `is_strict`, `name`, and `parse` are scaffolding: `is_strict` will be
// used by `--strict` failure paths in PR2+, `name` is reserved for
// per-command help rendering, and `parse` is a future PR-friendly entry
// point alongside the current `clap::Parser` derive.
#[allow(dead_code)]

use clap::{Args, Parser, Subcommand};

use crate::output::OutputMode;

/// Top-level CLI: `nexusdb-scanner [...globals] <COMMAND> [args]`.
///
/// We use the `command` attribute on Parser so subcommands appear in help.
#[derive(Debug, Parser)]
#[command(
    name = "nexusdb-scanner",
    version,
    about = "Read-only offline scanner / data-rescue tool for NexusDB.",
    long_about = "Reads NexusDB data directories without booting the engine.\n\
                  Designed for the case where the engine refuses to open the directory\n\
                  and the user still needs to inspect or rescue the data."
)]
pub struct Top {
    #[command(flatten)]
    pub globals: Globals,

    #[command(subcommand)]
    pub command: Command,
}

/// Global flags shared by every subcommand.
///
/// `dir` is implemented as a non-`global` flatten member: clap forbids
/// combining `global = true` with required (clap issue tracked in DEBUG
/// asserts). flatten propagates the option to every subcommand anyway, so
/// the user-visible behaviour is identical to "global + required".
#[derive(Debug, Args, Clone)]
pub struct Globals {
    /// Data directory to scan.
    #[arg(long)]
    pub dir: std::path::PathBuf,

    /// Tolerant mode (default): never exit non-zero on bad pages.
    #[arg(long, global = true, default_value_t = true)]
    pub tolerant: bool,

    /// Strict mode: abort on the first bad page. CI use only.
    #[arg(long, global = true, conflicts_with = "tolerant")]
    pub strict: bool,

    /// Emit machine-readable JSON.
    #[arg(long, global = true)]
    pub json: bool,

    /// Parse and render vpid values as hex.
    #[arg(long, global = true)]
    pub hex_vpid: bool,

    /// Cap output rows (0 = unlimited).
    #[arg(long, global = true, default_value_t = 0u32)]
    pub limit: u32,

    /// Disable ANSI color (also auto-honoured when `NO_COLOR` is set).
    #[arg(long, global = true)]
    pub no_color: bool,
}

impl Globals {
    pub fn output_mode(&self) -> OutputMode {
        if self.json {
            OutputMode::Json
        } else {
            OutputMode::Human
        }
    }

    pub fn is_strict(&self) -> bool {
        self.strict
    }
}

/// Every subcommand the scanner supports in PR1 + PR2.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// List tables (the `dbs` command from the design doc).
    Dbs,

    /// Decode the 40-byte header of a single page (the `header` command).
    Header {
        /// vpid to inspect.
        #[arg(value_parser = clap::value_parser!(u64).range(0..))]
        vpid: u64,

        /// Also read and report on the neighbouring vpids (N-1, N+1, and
        /// the first/last page of the same chunk). Useful for diagnosing
        /// whether a bad page is isolated or part of a wider corruption.
        #[arg(long)]
        neighbors: bool,
    },

    /// Full decode of a single page (the `vpid` command).
    Vpid {
        /// vpid to inspect.
        #[arg(value_parser = clap::value_parser!(u64).range(0..))]
        vpid: u64,

        /// Skip item-area decoding; only print header + footer.
        #[arg(long)]
        raw: bool,
    },

    /// Walk every page reachable from a btree root and report per-page status.
    Verify {
        /// Root vpid of the btree to walk.
        #[arg(value_parser = clap::value_parser!(u64).range(0..))]
        root: u64,
    },

    /// Diagnose a bad page's context within a tree.
    Blame {
        /// vpid of the suspected bad page.
        #[arg(value_parser = clap::value_parser!(u64).range(0..))]
        vpid: u64,

        /// Optional: if provided, search only within this tree.
        #[arg(short, long, value_parser = clap::value_parser!(u64).range(0..))]
        tree: Option<u64>,
    },

    /// One-click rescue: run dbs + verify every tree + blame every bad page,
    /// producing a unified diagnosis report.
    Rescue,

    /// Export every reachable (key, value) pair from a btree as a stream.
    Export {
        /// Root vpid of the btree to export.
        #[arg(value_parser = clap::value_parser!(u64).range(0..))]
        root: u64,

        /// Output format: "kv" (hex_key \t hex_value) or "json".
        #[arg(short, long, default_value = "kv")]
        format: String,

        /// Skip bad pages (default: true).
        #[arg(long, default_value_t = true)]
        skip_bad: bool,
    },

    /// List WAL segment files in the data directory.
    WalList,

    /// Decode WAL segment files byte-by-byte.
    WalDump {
        /// Sequence number of the segment to dump.
        #[arg(short, long, value_parser = clap::value_parser!(u64).range(0..))]
        seq: u64,

        /// Optional end sequence (inclusive).
        #[arg(short = 't', long)]
        to: Option<u64>,

        /// Max frames to decode.
        #[arg(short, long)]
        limit: Option<u64>,
    },

    /// Full rescue pipeline: export tree with optional WAL replay.
    Merge {
        /// Root vpid of the btree to export.
        #[arg(value_parser = clap::value_parser!(u64).range(0..))]
        root: u64,

        /// Output format: "kv" or "json".
        #[arg(short, long, default_value = "kv")]
        format: String,

        /// Include WAL replay on top of the page-pool export.
        #[arg(long)]
        include_wal: bool,
    },

    /// Dump the vpid → (file_id, chunk_idx, page_idx) mapping.
    Map {
        /// Only use page.mate entries; skip arithmetic fallback.
        #[arg(long)]
        from_mate_only: bool,
    },

    /// Find a specific key inside a btree and report its value.
    Lookup {
        /// Root vpid of the btree to search.
        #[arg(value_parser = clap::value_parser!(u64).range(0..))]
        tree: u64,

        /// Hex-encoded key to find.
        #[arg(short, long)]
        key: String,
    },

    /// Per-page range scan: list items whose keys fall within [start, end].
    Range {
        /// vpid of the page to scan.
        #[arg(value_parser = clap::value_parser!(u64).range(0..))]
        vpid: u64,

        /// Inclusive start key (hex). Empty = page start.
        #[arg(short, long)]
        start: Option<String>,

        /// Inclusive end key (hex). Empty = page end.
        #[arg(short = 'e', long)]
        end: Option<String>,
    },
}

impl Command {
    pub fn name(&self) -> &'static str {
        match self {
            Command::Dbs { .. } => "dbs",
            Command::Header { .. } => "header",
            Command::Vpid { .. } => "vpid",
            Command::Verify { .. } => "verify",
            Command::Blame { .. } => "blame",
            Command::Rescue { .. } => "rescue",
            Command::Export { .. } => "export",
            Command::WalList { .. } => "wal-list",
            Command::WalDump { .. } => "wal-dump",
            Command::Merge { .. } => "merge",
            Command::Map { .. } => "map",
            Command::Lookup { .. } => "lookup",
            Command::Range { .. } => "range",
        }
    }
}

/// Parse argv into a `Top`. Exits the process on parse error.
pub fn parse() -> Top {
    Top::parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_dbs_command() {
        let t = Top::try_parse_from(["nexusdb-scanner", "--dir", "E:/study", "dbs"]).unwrap();
        assert_eq!(t.globals.dir.to_string_lossy(), "E:/study");
        assert!(t.globals.tolerant);
        assert!(!t.globals.strict);
        assert_eq!(t.command.name(), "dbs");
    }

    #[test]
    fn parses_header_command_with_vpid() {
        let t = Top::try_parse_from([
            "nexusdb-scanner",
            "--dir",
            "E:/study",
            "header",
            "5",
        ])
        .unwrap();
        assert_eq!(t.command.name(), "header");
        match t.command {
            Command::Header { vpid, neighbors } => {
                assert_eq!(vpid, 5);
                assert!(!neighbors);
            }
            _ => panic!("wrong command"),
        }
    }

    #[test]
    fn parses_vpid_command_with_raw_flag() {
        let t = Top::try_parse_from([
            "nexusdb-scanner",
            "--dir",
            "E:/study",
            "vpid",
            "5",
            "--raw",
        ])
        .unwrap();
        match t.command {
            Command::Vpid { vpid, raw } => {
                assert_eq!(vpid, 5);
                assert!(raw);
            }
            _ => panic!("wrong command"),
        }
    }

    #[test]
    fn strict_and_tolerant_cannot_coexist() {
        let r = Top::try_parse_from([
            "nexusdb-scanner",
            "--dir",
            "E:/study",
            "--strict",
            "--tolerant",
            "dbs",
        ]);
        assert!(r.is_err(), "--strict should conflict with --tolerant");
    }

    #[test]
    fn json_flag_sets_output_mode() {
        let t = Top::try_parse_from(["nexusdb-scanner", "--dir", "E:/study", "--json", "dbs"]).unwrap();
        assert_eq!(t.globals.output_mode(), OutputMode::Json);
    }
}
