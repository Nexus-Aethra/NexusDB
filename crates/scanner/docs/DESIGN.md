# NexusDBScanner — Design

> Status: **PR2 in progress**. PR1 (dbs/header/vpid) and PR2.1-2.2 (layout auto-discovery, tree traversal, verify) are committed. PR2.3-2.6 (header --neighbors, blame, rescue, integration test) are pending.

This document is the contract. Implementation PRs (PR1..PR5) are expected to
honour every "must" clause below. Any deviation needs a written rationale
recorded in `docs/dev/CONTRIBUTING.md`.

---

## 1. Purpose

`nexusdb-scanner` is a **read-only offline** tool for inspecting and rescuing
data from a NexusDB data directory *when the embedded/server engine can no longer
open it*.

The motivating incident: a 21 MB single-process directory written by a kill-9
crash later fails to open because `rebuild_composite_counts` blows up on a
`vpid 5` whose page type is `Meta` instead of `Leaf/Internal`. The engine
exits; the user has no way to even read their own data out.

This tool exists to make that situation *recoverable*: open the on-disk
artifacts (block files, WAL segments, `page.mate`/`pid.state`) directly,
without ever invoking the storage engine, and expose them through a CLI aimed
at diagnosis and, ultimately, data export.

It is not a UI. It is not a hot-path tool. It is the **last-resort data
access channel**.

---

## 2. Hard invariants (do not break)

These rules are non-negotiable. They are the reason this crate exists as a
standalone binary rather than another bin in the engine workspace.

### 2.1 Read-only

The default operating mode **never writes to disk**. Not even to update
`page.mate` / `pid.state` / WAL pointers / mtime.

If a future repair mode needs to write (e.g. truncate a corrupt WAL tail),
that mode must:

1. require `--i-know-what-i-am-doing` on the command line,
2. require an explicit `--out-dir <PATH>` distinct from the input `--dir`,
3. refuse to run when the source directory has any `.lock` or `.flock`
   sentinel left behind by a live engine.

Until then: **there is no write path**.

### 2.2 Tolerant by default

`--tolerant` is the default. In tolerant mode the scanner must:

- never call `panic!` on bad pages,
- never `exit(1)` because of a corrupt page header,
- emit a clearly-tagged bad-page record (`[BAD-PAGE]` in human mode, an object
  with `"bad": true` in JSON mode) and continue,
- accumulate errors and only return non-zero exit code at the very end,
  *after* all requested output has been produced.

`--strict` is opt-in for tests and CI.

### 2.3 Cross-platform

The binary must produce identical output on Windows, Linux, and macOS for the
same input bytes. Specifically:

- No `*-sys` / `*-api` / `winapi` / `nix` crates anywhere in the dependency
  graph except transitive deps we cannot replace (none at the moment).
- No `[target.'cfg(target_os = ...)'.dependencies]` sections in
  `crates/scanner/Cargo.toml`. If a platform-conditional becomes necessary,
  gate it behind a runtime feature flag instead.
- File paths are `PathBuf` and output via `Display`, never `to_str()`.
- Numeric conversion is explicitly `le`/`be`; never `to_native`.
- No timezone-dependent formatting. Time-like fields (WAL `mtime`, frame
  sequence numbers) are emitted as raw integers; if a date string is ever
  added it must be RFC3339 with explicit `+00:00`.
- Text output is UTF-8, no BOM. No ANSI escapes unless `--color` is passed.
- No environment-variable probe that differs by OS (`HOME` vs `USERPROFILE`,
  `\` vs `/`). Path handling goes through `Path`/`PathBuf`.
- `NO_COLOR=1` is honoured by default.

### 2.4 Engine-free

The scanner must not link or call:

- `ShardManager` / `ShardManagerOptions`
- any WAL writer or replay routine from `crate::wal`
- `Pager` / any chunk allocator
- the scheduler

Page parsing goes exclusively through the pure-functional `page` crate:
input is a `&[u8; PAGE_SIZE]` (or any-length slice clipped from disk) and
output is a parsed value or error. This means:

- new block files are never created,
- a stuck `WalWriter` is irrelevant — we just read bytes off disk,
- there is no `async` runtime; the whole tool is sync,
- WAL inspection is byte-level, never through any engine module.

### 2.5 No performance tuning

The scanner is a diagnostic tool. Optimisation work that increases code
surface area is rejected. Specifically:

- no `unsafe` blocks; the one exception is `transmute` into `PageHeader`
  inside the `page` crate, which we do not write — we only call into it,
- no SIMD intrinsics for key comparison,
- no custom allocators,
- no `rayon` parallelism in the first three PRs (sequential reads of a 21 MB
  directory complete in milliseconds),
- `cargo build --release` is fine; `-O3` and PGO are explicitly out of scope.

---

## 3. CLI surface

```
nexusdb-scanner [--dir <PATH>] [--tolerant|--strict] [--json] [--limit N]
                [--hex-vpid] [--color|--no-color]
                <COMMAND> [args...]
```

### 3.1 Global flags

| Flag | Default | Meaning |
|---|---|---|
| `--dir <PATH>` | required for most commands | the NexusDB data directory (`block_dir`) |
| `--tolerant` | **on** | never exit non-zero on bad pages |
| `--strict` | off | abort on first bad page (CI only) |
| `--json` | off | machine-readable output |
| `--limit <N>` | 0 = unlimited | cap output rows |
| `--hex-vpid` | off | vpid values in input/output are hex |
| `--color`/`--no-color` | honour `NO_COLOR` | ANSI escapes |
| `-h`, `--help` | — | per-command help |

### 3.2 Commands

Each command is independent; a single invocation runs exactly one command.

Legend: `[x]` = implemented, `[ ]` = pending design doc, `[~]` = partially implemented.

#### `dbs` [x] (PR1)

Enumerate all tables in the directory's MetaPage. For each table, report the
root_vpid. Reads `page.mate` if available, falls back to scanning `.block`
file headers if `page.mate` is unreadable.

```
nexusdb-scanner --dir <PATH> dbs
```

Output (human mode):

```
db_name    table_name        root_vpid    root_type    items
study      subjects          128          Leaf         5
study      chapters          192          Internal     18
study      knowledge_points  256          Leaf         15
...
```

JSON mode emits an array of records, one per table.

#### `vpid` [x] (PR1)

Read a single page by vpid, print a full decode. Does **not** require a
tree handle. Designed for "look at vpid 5 and tell me what went wrong".

```
nexusdb-scanner --dir <PATH> vpid -vpid <N>
                 [-raw]                # header only, skip item decoding
                 [-items-limit N]
```

`vpid 0` is conventionally the MetaPage in this codebase but the command
does not enforce that; it reads whatever vpid the user specifies and
classifies the page by its `page_type` byte at offset 4.

The decode format reuses `page::dump::dump_leaf_page` /
`dump_internal_page` for `Leaf`/`Internal`; for `Meta` it calls
`MetaPage::load` style parsing directly; for `Overflow`/`OverflowIndex` it
emits a small fixed schema.

If the page is bad (`magic` mismatch, `checksum` mismatch, `page_type` byte
not in `{1..5}`, `vpid` mismatch, `free_off` out of bounds), it prints
`[BAD-PAGE]` with diagnostic fields and exits tolerant mode handling.

#### `range` [ ]

In-page ordered range scan over a single leaf or meta page.

```
nexusdb-scanner --dir <PATH> range -vpid <N>
                 -start <hex>           # empty = page start
                 -end   <hex>           # empty = page end
                 [-kind <S|H|L|T|Z|I>]
                 [-items-limit N]
```

`range` operates on **the contents of one page** only. It does not span pages.
If the user wants a tree-wide range, they use `walk`.

The reason `range` stays per-page: it is the canonical tool for diagnosing
"is this key inside this page, and is the binary search correct?". Anything
that crosses a page boundary inherently traverses the btree, which is `walk`.

#### `walk` [~] (PR2.2 tree.rs provides the BFS traversal engine; CLI command not yet wired)

Tree-wide ordered scan starting from a root vpid.

```
nexusdb-scanner --dir <PATH> walk -tree <root_vpid>
                 [-start <hex>] [-end <hex>] [-items-limit N]
```

Reads as many leaf pages as the range covers, traversing `internal_child`
boundaries. Bad pages along the path are skipped and recorded; the next
internal pointer is read from the bad page's `page_vpid` field directly to
attempt to descend (tolerant only — strict mode halts).

This is the workhorse for "give me everything in this tree".

#### `lookup` [ ] (formerly `keydetail` in the original draft)

Find a specific physical key inside a tree and report its full item.

```
nexusdb-scanner --dir <PATH> lookup -tree <root_vpid> -key <hex>
```

Walks the btree until the leaf containing `key` is reached, then decodes the
item. Output includes:

- reconstructed physical key (after prefix expansion),
- the value bytes (or `child_vpid` if the leaf is an internal pointer stash
  — that only happens on `OverflowIndex`; flagged explicitly),
- the page vpid and item index where it was found,
- a `[BAD-PAGE]` marker if the lookup could not complete.

#### `header` [x] (PR1; `--neighbors` flag arrives in PR2.3)

Lightweight diagnostic — read page header only, skip item decode.

```
nexusdb-scanner --dir <PATH> header -vpid <N>
```

Useful when the full `vpid` command fails deep in item parsing and you still
want the 40-byte header + footer decoded. Identical output to the first block
of `vpid` but guaranteed cheap.

#### `map` [ ]

Dump the vpid → (file_id, chunk_idx, page_idx) map from `page.mate` /
`pid.state`. If both files are missing, scans `.block` files for `LCBP`
magic at fixed strides and synthesises a best-effort map.

```
nexusdb-scanner --dir <PATH> map [-from-mate-only]
```

This is the scanner's internal index for "given a vpid, where on disk does
this page live". It is exposed as a command because the user often wants to
spot-check the map against reality.

#### `verify` [x] (PR2.2)

Walk an entire btree, validating every page along the way.

```
nexusdb-scanner --dir <PATH> verify -tree <root_vpid>
                 [-stop-on-bad]            # default = continue on bad
```

For each page, checks:

- magic (`LCBP`),
- `page_type` byte ∈ `{1..5}`,
- `page_vpid == expected_vpid`,
- `checksum` (xxhash64 of `header[40..]` region),
- `key_count` vs actual items decoded,
- for `Internal`: separator key monotonicity vs sibling's first key,
- for `Leaf`: emitted value bytes do not overlap the checkpoint array.

Outputs a per-page status table. Default mode is `--tolerant`.

#### `export` [~] (PR2.5 rescue command planned; export as standalone command deferred to PR3)

Dump an entire tree to a stream on stdout. **This is the data-rescue path.**

```
nexusdb-scanner --dir <PATH> export -tree <root_vpid>
                 [-format kv|json]
                 [-batch 1024]            # rows per write, default 1024
```

- `kv` (default): each row is `physkey_hex \t value_hex`. Keys are printed
  with hex encoding by default because physical keys may contain NUL bytes
  and human inspection is impossible otherwise. Use `--raw-keys` to emit
  raw bytes (only safe when piped, not for terminals).
- `json`: line-delimited JSON records `{"key": "hex...", "value": "hex...",
  "vpid": N, "item_idx": K}`. Two keys sharing a row are emitted in btree
  order.

Bad pages are skipped with a `[BAD-PAGE-SKIPPED @ vpid=N]` note. The exit
code is 0 if any row was emitted, 1 if zero rows were emitted *and* the
tree appeared non-empty (we cannot distinguish a legitimately empty tree from
a wholly-corrupt tree; that ambiguity is logged).

#### `wal list` [ ]

```
nexusdb-scanner --dir <PATH> wal list
```

Walks the directory, lists every `*.wal.<seq>` segment file with size and
mtime as raw integers. No bytes are read yet.

#### `wal dump` [ ]

Decode one or a range of WAL segments byte-by-byte.

```
nexusdb-scanner --dir <PATH> wal dump -seq <N> [-to <M>] [-limit F]
```

For each segment, parses the segment header, then iterates frames. For each
frame: prints `(op_type, encoded_key_len, encoded_key_hex,
encoded_value_len, encoded_value_hex, checksum_ok, frame_crc_ok)`. There is
no replay — frames are inspected, not applied.

#### `merge`

The full rescue pipeline. **Not implemented in the first three PRs.**

```
nexusdb-scanner --dir <PATH> merge -tree <root_vpid> -out <PATH>
                [-include-wal]
```

Runs `export` against the current page pool and, optionally, replays unapplied
WAL segments on top. The output file is a deterministic, sorted
(jsonl / nsv) stream suitable for piping into a fresh NexusDB instance via
any ingest tool we provide later.

---

## 4. Output formats

### 4.1 Human (default)

- Tab-separated columns, one record per line.
- Header row present.
- Bad-page records are prepended with `[BAD-PAGE]` so they sort to the top
  in `grep`/`less`.
- vpid is decimal by default; `--hex-vpid` switches.

### 4.2 JSON (`--json`)

- Top-level is always an object `{"command": "...", "results": [...]}` or a
  NDJSON stream for `export`.
- Bad pages are first-class records with `"bad": true`, never exceptions.
- Encoding is `serde_json` with explicit field names matching the human
  mode headers.

### 4.3 KV stream (`export -format kv`)

- `physkey_hex \t value_hex \n` per row.
- No BOM, no trailing newline guarantee — pipes decide.
- Bad-page rows are `[BAD-PAGE] vpid=<N> reason="..."` and are emitted at
  the offset where the bad page would have appeared, so the row order is
  preserved.

---

## 5. Layered architecture

```
crates/scanner/src/
├── main.rs              // clap dispatch
├── cli.rs               // argv parsing, flag compatibility
├── dir.rs               // locate block_dir, list .block and .wal.* files
├── pid.rs               // page.mate + pid.state → vpid→(file,chunk,page)
├── page_io.rs           // given a (file,chunk,page), read a [u8;PAGE_SIZE]
├── page_decode.rs       // thin wrappers around `page` crate
├── tree.rs              // walk / lookup / verify
├── export.rs            // export + merge
├── wal.rs               // wal list + dump
├── output.rs            // human / json / kv formatters
└── error.rs             // ScannerError (thiserror)
```

Dependencies flow strictly downward:

```
main → cli → { dir, pid, page_io, tree, wal, export } → page_io
                  ↓                                ↓
                pid                            page (cr dep)
```

No back-edges. No command knows about another command.

---

## 6. Sequence: from a corrupt directory to a JSON dump

```
$ nexusdb-scanner --dir E:\study header -vpid 5
vpid=5  expected_type=Leaf|Internal  actual_type=Meta
         magic_ok=true   checksum_ok=true   bytes_valid=true
#  → confirms the regression: page type does not match traversal expectation.

$ nexusdb-scanner --dir E:\study dbs
db_name=study  table=subjects        root_vpid=128
db_name=study  table=chapters        root_vpid=192
...

$ nexusdb-scanner --dir E:\study walk -tree 192 -items-limit 5
vpid   page_type   items   first_key(last 8B hex)   last_key(last 8B hex)
128    Internal    18      ...                      ...
192    Leaf        9       ...

$ nexusdb-scanner --dir E:\study export -tree 192 -format json > rescued.ndjson
$ wc -l rescued.ndjson
```

That is the entire user journey. Everything else is diagnostics in aid of
figuring out *why* the engine refused to open it.

---

## 7. Out of scope (today)

- Anything that *modifies* data on disk. Repair-PR (later) will add a
  write mode behind the gate described in §2.1.
- A TUI. The CLI is the interface. A TUI is several orders of magnitude
  more code and offers no diagnostic advantage over `less`.
- Online inspection (connecting to a running engine). The scanner takes the
  database *offline* by definition; if the engine is running, the user is
  expected to stop it first.
- Performance work. Single-digit-millisecond responses on the 21 MB case
  study are the target; we explicitly do not need to hit those targets.
- Per-shard routing. The 21 MB case study lives in `default\shard_0`. The
  scanner assumes a single shard for v1; multi-shard is a future PR if and
  when someone files an incident requiring it.

---

## 8. Current progress

| PR | Scope | Date | Status |
|---|---|---|---|
| PR1 | Design docs + skeleton + `dbs`/`header`/`vpid` commands | 2026-08-21 | committed (`bc3018e`, `ba06683`) |
| PR2.1 | dir module split + layout auto-discovery (L1/L2/L3/L4) | 2026-08-21 | committed (`77950b2`) |
| PR2.2 | `tree.rs` (BFS traversal) + `verify` command | 2026-08-21 | committed (`4f1a7fd`) |
| PR2.3 | `header --neighbors` | pending | reading same-chunk neighbour pages |
| PR2.4 | `commands/blame.rs` | pending | bad page → tree path → impact range |
| PR2.5 | `commands/rescue.rs` | pending | one-shot diagnosis pipeline |
| PR2.6 | Integration test | pending | synthetic corrupt directory → rescue report |
| PR3 | `export` + `merge` + `verify` repair mode | deferred | data rescue pipeline |
| PR4 | `wal list`/`dump` + `merge --include-wal` | deferred | WAL replay + export |
| PR5 | `map` + `lookup` + `range` | deferred | fine-grained tree navigation |

## 9. Open questions deferred to a later PR

- Should `verify` follow `internal_child` pointers or recompute them from
  a clean reload of `page.mate`? Currently intent: follow pointers, record
  mismatches.
- Should `export` produce user_key (after `keyspace` decode) or the raw
  physical key (with `[S][klen]…` prefix)? Decision deferred until we have
  feedback from the first use on a real corrupt directory — propose
  physical-key default with `--logical-keys` opt-in.
- WAL frame format specifics (op codes, encoding details) need confirmation
  by reading `crates/storage/src/wal.rs` end-to-end in PR5; this doc asserts
  the schema based on the segment file naming only.
