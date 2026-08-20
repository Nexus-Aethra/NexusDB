//! NexusDBScanner — read-only offline scanner / repair tool.
//!
//! See `crates/scanner/docs/DESIGN.md` for the architecture, CLI surface, and
//! the cross-platform invariants this crate commits to.
//!
//! Dead-code suppression is enabled crate-wide for PR1: most of the
//! PR2/3/5 surface (`map`, `walk`, `range`, `lookup`, `verify`, `export`,
//! `wal ...`, `merge`) is pre-declared so this PR's diff stays small.
//! Strip the attribute after PR5 lands.
#![allow(dead_code)]
//!
//! Invariants enforced at this layer:
//! - Never writes to disk. `--write` would have to be opt-in and is not yet
//!   wired.
//! - Never opens the storage engine. Page parsing is done through the
//!   pure-functional `page` crate on a `&[u8]` slice per page.
//! - Never uses async runtimes, OS-specific syscalls, or platform-conditional
//!   code paths. Behaviour is identical on Windows, Linux, and macOS.

mod cli;
mod commands;
mod dir;
mod error;
mod meta;
mod output;
mod page_decode;
mod page_io;
mod pid;

use std::io::{self, Write};
use std::process::ExitCode;

use clap::Parser;

use crate::cli::{Command, Top};
use crate::error::Result;

fn main() -> ExitCode {
    // `NO_COLOR=1` is honoured automatically by clap for help formatting;
    // we also accept it on the *output* side for our own colour decisions.
    let top = Top::parse();
    let globals = &top.globals;
    let stdout = io::stdout();
    let mut out = stdout.lock();

    let cmd_result: Result<u8> = match top.command {
        Command::Dbs => commands::dbs::run(globals, &mut out),
        Command::Header { vpid } => commands::header::run(globals, vpid, &mut out),
        Command::Vpid { vpid, raw } => commands::vpid::run(globals, vpid, raw, &mut out),
    };

    match cmd_result {
        Ok(exit) => {
            if exit == 0 {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(exit)
            }
        }
        Err(e) => {
            // Errors here are *catastrophic* (couldn't even read the
            // directory). Tolerant per-page errors are handled inside each
            // command and never reach `main`.
            let _ = writeln!(io::stderr(), "nexusdb-scanner: fatal: {e}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod smoke_tests {
    use super::*;

    #[test]
    fn cli_parses_minimal_invocation() {
        // Just confirm parse() succeeds on a valid argv.
        let r = Top::try_parse_from(["nexusdb-scanner", "--dir", "E:/study", "dbs"]);
        assert!(r.is_ok(), "minimal dbs invocation should parse");
    }
}
