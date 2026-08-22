# Contributing to nexusdb-scanner

This crate ships under stricter rules than the rest of the workspace
because its whole job is to *remain useful while the database is broken*.
Sensible code that degrades the worst-case behaviour is rejected here.

---

## 1. Hard rules (do not relax without updating `docs/DESIGN.md`)

### 1.1 No new OS-conditional dependencies

Adding a `[target.'cfg(target_os = "...")'.dependencies]` entry to
`crates/scanner/Cargo.toml` requires:

1. A *justified* reason (which OS-specific capability and what failure mode
   forces it),
2. A justification that the dependency-free alternative (`std::fs`,
   `path-clean`, `hex`, manual byte parsing) cannot meet the requirement,
3. A signoff in the PR description.

**Default answer: no.**

### 1.2 No `unsafe`

The scanner owns no `unsafe` blocks. We do call into the `page` crate,
which itself uses `unsafe` to `transmute` into `PageHeader`; that is fine
because it is upstream and it is *the reason* the scan logic is
portable. New `unsafe` inside `crates/scanner/src/**` is rejected.

### 1.3 No new direct `thiserror`-free `String` errors

Every fallible function returns `Result<T, ScannerError>`. Ad-hoc `Result<T,
String>` exists only in tests. New variants in `error.rs` are always
welcome if they help the user's mental model.

### 1.4 No new async

Scanner code is sync. Pulling in an async runtime even behind a feature
flag is rejected.

### 1.5 Bad pages are first-class, not exceptions

A corrupt page is a *row in the output*, not a panic and not an early
`Err` bubble. If you find yourself writing `page_bytes.map_err(...)` and
then propagating, you almost certainly belong in `--strict` mode or need a
new helper in `page_decode.rs` that always succeeds and emits a structured
diagnostic.

### 1.6 Output formats must be deterministic

Given the same input bytes and the same flags, output bytes must be
identical across Windows / Linux / macOS. This means:

- No timestamps in default output (`mtime` only appears when the user asks
  for `--with-mtime`),
- No path separators (`/` vs `\`) — paths go through `Display`,
- No reordering between releases — once a JSON field is published it stays
  in the record, even if deprecated,
- Locale-independent number formatting — integers and hex only.

---

## 2. Dependency policy

**Approved:**

| Crate | Reason |
|---|---|
| `page` (workspace) | pure-functional page parser |
| `clap` | CLI; no OS bindings |
| `thiserror` | error ergonomics |
| `serde` / `serde_json` | JSON output |
| `hex` | hex encoding |
| `memmap2` (feature-gated) | large-file reads; platform-neutral wrapper itself |

**Currently banned (would need a written exception):**

- `chrono` / `time` / `nix` / `windows-sys` / `*_sys` in general,
- `tokio` / `async-std` / `smol` / any async runtime,
- any `rayon` / `crossbeam` — single-threaded is the model,
- anything wrapping `winapi` / `CoreFoundation` / `objc`.

**Transitives allowed but pinned to the lowest sane version** — `Cargo.lock`
is committed and `cargo update` on `nexusdb-scanner` alone is a code review
event.

---

## 3. Test matrix

We support three target platforms. Each PR must keep them green:

| Target triple | Tier | How it's tested |
|---|---|---|
| `x86_64-pc-windows-msvc` | tier 1 | GitHub Actions windows runner, smoke + integration |
| `x86_64-unknown-linux-gnu` | tier 1 | GitHub Actions ubuntu runner, smoke + integration |
| `aarch64-apple-darwin` | tier 2 | best-effort local checks; pre-merge CI is informational |

Smoke test (run on every PR, all three platforms):

```bash
cargo build -p nexusdb-scanner
cargo test  -p nexusdb-scanner
```

Integration test (`crates/scanner/tests/integration.rs`) generates a tiny
NexusDB directory in a tempdir via the public test API, then runs every
implemented command against that directory. Output is checked against
golden snapshots located at `crates/scanner/tests/golden/`. To refresh
golden snapshots:

```bash
SCANNER_UPDATE_GOLDEN=1 cargo test -p nexusdb-scanner
```

This regenerates `tests/golden/<command>.txt`. Review the diff. Snapshots
must be checked in on every PR that changes output.

---

## 4. PR review checklist (for reviewers)

When reviewing a PR against this crate, check the following in order:

1. **Cross-platform?** `git grep -nE 'cfg\(target_os|windows-sys|nix\b|libc\.' crates/scanner/src crates/scanner/Cargo.toml`
   should return nothing new in this PR.
2. **Read-only?** `git grep -nE 'std::fs::write|std::fs::rename|remove_file|write_all|copy\b' crates/scanner/src` should return
   nothing (the only writes today are to *stdout*, never the input dir).
3. **Tolerant by default?** every new path that reads a page has a
   `[BAD-PAGE]` branch and does not propagate `io::Error` past a page
   boundary unless the directory itself is unreadable.
4. **Deterministic output?** new commands do not include timestamps, OS
   paths, or `format!` with locale-affected specifiers.
5. **No new external deps** unless justified in §2.

---

## 5. Versioning and releases

- The scanner follows the workspace `Cargo.lock`.
- It is `publish = false`; never `cargo publish`.
- Backward compatibility: commands keep working even after we discover
  better layouts. New arguments are additive; removing a command requires
  a deprecation cycle of at least one minor release.
- Output formats (`--json` schemas, `kv` layout) are part of the contract;
  breaking them is a major version bump.

---

## 6. Where to look when you are stuck

| Symptom | First file to read |
|---|---|
| page parsing fails in unexpected ways | `crates/page/src/header.rs`, `leaf.rs`, `internal.rs` |
| btree traversal is wrong | `crates/page/src/internal.rs` (`internal_child`) and the `dump.rs` examples |
| WAL layout confusion | `crates/storage/src/wal.rs` (read, do not import) |
| vpid ↔ disk mapping broken | `crates/storage/src/meta_page.rs`, `pid_state`, `page.mate` |
| A test fails only on Windows | usually path-display or end-of-line; check `output.rs` |
