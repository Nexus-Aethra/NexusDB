# nexusdb-scanner

Read-only offline scanner / data-rescue tool for NexusDB.

This binary exists for one reason: when the engine refuses to open a data
directory, *something* should still be able to read the on-disk artifacts.
That something is this tool.

> See [`docs/DESIGN.md`](docs/DESIGN.md) for the architecture, invariants, and
> CLI reference. This README is the user manual.

---

## When to use this

Use it when **the engine cannot open the database but you still have the
files**. Typical triggers:

- engine init crashes mid-recovery with `rebuild_composite_counts: btree
  error: bad page type at vpid N`,
- the directory contains segments that did not yet make it to a `.block`
  file (i.e. WAL contains live writes that have not been checkpointed),
- you want to audit the physical layout (vpids, page types, checksum
  coverage) without booting the engine.

Do not use it as a normal admin tool. The server (`nexus-study-server`) and
the embedded API carry every operation you need under normal conditions.

---

## Building

```bash
# Default (no optional features)
cargo build -p nexusdb-scanner

# With memmap-backed page reader (faster for very large directories; opt-in)
cargo build -p nexusdb-scanner --features mmap
```

The crate is `edition = "2021"` with `rust-version = "1.75"`. The binary
names `nexusdb-scanner`.

### Cross-platform guarantees

The same binary, byte-for-byte identical output, on:

- Windows + NTFS,
- Linux + ext4 (and any POSIX filesystem),
- macOS + APFS.

No OS-specific dependencies are pulled in. There are no
`[target.'cfg(target_os = ...)'.dependencies]` in `Cargo.toml`. If a future
feature needs platform-conditional code, it must go behind a runtime
feature flag, not a `cfg`.

---

## Quick start

```bash
# 1. See what tables live in the directory
nexusdb-scanner --dir /var/data/nexusdb dbs

# 2. Look at one suspect vpid
nexusdb-scanner --dir /var/data/nexusdb header -vpid 5
nexusdb-scanner --dir /var/data/nexusdb vpid   -vpid 5

# 3. Verify an entire tree
nexusdb-scanner --dir /var/data/nexusdb verify -tree 192

# 4. Export the data — this is the rescue path
nexusdb-scanner --dir /var/data/nexusdb export -tree 192 -format json > rescue.ndjson
```

---

## Real-world runbook

The 21 MB single-shard case study the scanner was originally designed for
presents as follows:

```text
[ERROR][shard] shard-0 engine init failed:
  "rebuild_composite_counts: btree error: bad page type at vpid 5:
   expected Leaf/Internal, got Meta"
```

Steps to recover:

```bash
# Confirm what we are dealing with — should report page_type=Meta on vpid 5
nexusdb-scanner --dir E:\study header -vpid 5

# List every table so we know which root_vpid to walk
nexusdb-scanner --dir E:\study dbs

# (pick a root_vpid — say 192 for "chapters")
# Walk it; we are hunting for bad pages
nexusdb-scanner --dir E:\study walk -tree 192 -items-limit 5

# Run a structural verification; bad pages are reported inline
nexusdb-scanner --dir E:\study verify -tree 192

# Export whatever is reachable
nexusdb-scanner --dir E:\study export -tree 192 -format json > rescued.ndjson
wc -l rescued.ndjson
```

If the directory also has WAL segments with live but uncheckpointed writes
(those `shard_0.wal.NNNNNN` siblings of the `.block` files):

```bash
nexusdb-scanner --dir E:\study wal list
nexusdb-scanner --dir E:\study wal dump -seq 180 -limit 50
# (full WAL replay into a clean database is on the roadmap under `merge`)
```

The scanner never modifies the directory. It is safe to keep running against
the same directory across multiple sessions.

---

## Flags in one place

| Flag | Effect |
|---|---|
| `--dir <PATH>` | required data directory |
| `--tolerant` | default; never exit non-zero on bad pages |
| `--strict` | abort on first bad page (CI use) |
| `--json` | machine-readable JSON output |
| `--limit N` | cap emitted rows |
| `--hex-vpid` | vpid values are hex in input and output |
| `--no-color` | disable ANSI escapes |
| `-h`, `--help` | per-command help |

---

## Status

This is the **design + skeleton PR** (`tool/scanner`). The skeleton binary
exists and `cargo check` succeeds; commands are not yet implemented. See
[`docs/DESIGN.md`](docs/DESIGN.md) for the rollout plan across PR1..PR5.

PR1 implements `dbs` and `header` (the minimum to confirm any directory is
readable). Subsequent PRs add `vpid`, `walk`, `lookup`, `range`, `verify`,
`export`, then WAL handling.
