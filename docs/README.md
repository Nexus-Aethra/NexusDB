# NexusDB documentation

## Start here

- [README](../README.md) / [Chinese README](../README-CN.md): installation,
  protocols, configuration, and benchmark snapshots.
- [GUIDE](./GUIDE.md) / [Chinese guide](./GUIDE-CN.md): feature-oriented usage.
- [Embedded KV API](./EMBEDDED-KV.md): use NexusDB directly from a Rust
  process without starting network listeners.
- [DESIGN](./DESIGN.md): architecture and persistent-data invariants.
- [AGENTS](./AGENTS.md): current development handoff, verified status, and
  operational gotchas.
- [EXPERIMENTAL](./EXPERIMENTAL.md): registry of experimental features,
  default-on rollback switches, tuning knobs, and diagnostics.
- [CHANGELOG](./CHANGELOG.md): chronological fixes and validation evidence.

## Active plans and audits

- [Windows portability plan](./plans/2026-08-13-windows-portability.md) — P1 MVP
  (std::net blocking on Windows) shipped; P2 IOCP / RIO deferred.
- [Windows IOCP runtime design](./plans/2026-08-13-windows-iocp.md) — full
  M1–M7 design; M1/M2 written and reverted to P1 due to `AcceptEx` /
  `OVERLAPPED` quirks documented inline.
- [Storage refactoring index (2026-11)](./plans/2026-11-06-README.md) — entry
  point for the write-amplification work; links the framework and leaf-item
  pointer designs and tracks the open decisions.
- [Storage write-amplification framework (phased)](./plans/2026-11-06-storage-write-amplification.md)
  — Phase 1: keep COW + vpid→pid mapping, add a bounded memtable to batch random
  writes so each affected leaf page is COW'd once per batch instead of once per
  write. GC and crash recovery unchanged by design.
- [Leaf item pointer design (Phase 1-B)](./plans/2026-11-06-leaf-item-pointer.md)
  — leaf items store `key → (vpid, offset, len)` pointers instead of inline values;
  values move to a packed data arena. Keeps COW, the `vpid→pid` mapping and
  page-granular GC untouched.
- [RESP write-path performance plan](./plans/2026-08-05-resp-write-path-performance.md)
- [RESP ↔ SQL schema adapter plan](./plans/2026-08-09-resp-sql-schema-adapter.md)
- [Coroutine scheduler hardening plan](./plans/2026-08-13-coroutine-scheduler-hardening.md)
- [SQL optimizer plan](./sql-optimizer-plan.md)
- [PostgreSQL dialect and Loom integration plan](./pg-dialect-gap-loom.md)
- [Scheduler audit](./scheduler-audit.md)
- [Code audit report (2026-10-16)](./CODE-AUDIT-REPORT-2026-10-16.md) — 93 audit
  items, all remediated as of 2026-10-19 (§5.6 "remaining: 0"); the older
  `CODE-AUDIT-REPORT.md` is the pre-remediation snapshot kept for history.

## Incident reports

- [Cold-start batch INSERT data loss](./bug-cold-start-data-loss.md)
- [B-tree split routing investigation](./bug-report-btree-split-routing.md)

## Historical material

Completed implementation plans, the earlier coroutine-worker status report,
and the original scheduler design are retained under [archive](./archive/) for
traceability. They are not the source of current behavior; use AGENTS.md and
the active plans above for new work.
