# NexusDB documentation

## Start here

- [README](../README.md) / [Chinese README](../README-CN.md): installation,
  protocols, configuration, and benchmark snapshots.
- [GUIDE](./GUIDE.md) / [Chinese guide](./GUIDE-CN.md): feature-oriented usage.
- [DESIGN](../DESIGN.md): architecture and persistent-data invariants.
- [AGENTS](../AGENTS.md): current development handoff, verified status, and
  operational gotchas.
- [CHANGELOG](../CHANGELOG.md): chronological fixes and validation evidence.

## Active plans and audits

- [RESP write-path performance plan](./plans/2026-08-05-resp-write-path-performance.md)
- [SQL optimizer plan](./sql-optimizer-plan.md)
- [PostgreSQL dialect and Loom integration plan](./pg-dialect-gap-loom.md)
- [Scheduler audit](./scheduler-audit.md)

## Incident reports

- [Cold-start batch INSERT data loss](./bug-cold-start-data-loss.md)
- [B-tree split routing investigation](./bug-report-btree-split-routing.md)

## Historical material

Completed implementation plans, the earlier coroutine-worker status report,
and the original scheduler design are retained under [archive](./archive/) for
traceability. They are not the source of current behavior; use AGENTS.md and
the active plans above for new work.
