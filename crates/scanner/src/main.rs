//! NexusDBScanner — read-only offline scanner / repair tool.
//!
//! See `crates/scanner/docs/DESIGN.md` for the architecture, CLI surface, and
//! the cross-platform invariants this crate commits to. This file is a thin
//! skeleton — the actual command dispatch arrives in the first content PR.
//!
//! Invariants enforced at this layer:
//! - Never writes to disk. `--write` would have to be opt-in and is not yet
//!   wired.
//! - Never opens the storage engine. Page parsing is done through the
//!   pure-functional `page` crate on a `&[u8]` slice per page.
//! - Never uses async runtimes, OS-specific syscalls, or platform-conditional
//!   code paths. Behaviour is identical on Windows, Linux, and macOS.

fn main() {
    // Placeholder. Real command parsing + dispatch lands in PR1.
    eprintln!(
        "nexusdb-scanner skeleton\n\
         no commands implemented yet — see crates/scanner/docs/DESIGN.md"
    );
    std::process::exit(2);
}
