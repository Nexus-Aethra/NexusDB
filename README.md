# `NexusDB`

> **语言 / Language**: **English** | [简体中文](./README-CN.md)

> A single-node database for write-heavy / low-latency workloads — Share-Nothing + per-core thread + io_uring + a hand-written coroutine scheduler, with unified multi-protocol access (Redis-compatible ✅, PG/MySQL wire ✅, Mongo on the roadmap).

[![Linux](https://img.shields.io/badge/OS-Linux-blue)]() [![Rust](https://img.shields.io/badge/Rust-2024-orange)]() [![Tests](https://img.shields.io/badge/tests-862%20passed-success)]() [![Clippy](https://img.shields.io/badge/clippy-0%20warnings-success)]() [![License](https://img.shields.io/badge/license-MIT-lightgrey)]()

> Architecture: [DESIGN.md](./DESIGN.md); handoff / progress: [AGENTS.md](./AGENTS.md); fix history: [CHANGELOG.md](./CHANGELOG.md).
>
> 📖 **User getting-started guide (features + per-driver access + SQL/type/security examples + performance): [docs/GUIDE.md](./docs/GUIDE.md)**

---

**[Core Features](#core-features) · [Quick Start](#quick-start) · [Performance](#performance) · [Architecture](#architecture) · [GC & Space Reclamation](#gc--space-reclamation) · [Large-Value Overflow Pages](#large-value-overflow-pages) · [Supported Protocols](#supported-protocols) · [Configuration](#configuration) · [Dev Commands](#dev-commands) · [Troubleshooting](#troubleshooting)**

---

## Core Features

**Protocol layer**
- **Five protocol listeners**: RESP2 (6379) + **REST (HTTP/1.1 JSON + CORS + Bearer) 6778** + dual SQL facades (MySQL wire 5434 / PostgreSQL wire 5435, shared kernel) + Binary 5433 (internal). All enforce `KvLimits` length checks at the protocol layer, with TCP_NODELAY.
- **Observability**: `/metrics` (Prometheus) + `/v1/status` + `/v1/debug/*` (process-level atomic counters, lock-free on the hot path).
- **SQL capabilities**: CREATE / ALTER TABLE ADD COLUMN / multi-row INSERT / SELECT (projection · column alias `AS` · ORDER BY · aggregates COUNT/SUM/AVG/MIN/MAX including expressions like `SUM(a+b)` · `COUNT(DISTINCT)` · `SELECT DISTINCT` · GROUP BY · HAVING · IN · BETWEEN · LIKE · WHERE with OR/NOT/parenthesized nesting · full-table scan · single-table qualified columns `table.col` · MySQL `LIMIT offset,count` · `db.table` qualified names) / UPDATE / DELETE / DROP / USE / DESCRIBE. Local secondary indexes + two-layer bloom pruning + covering-index / UNIQUE early stop + GLOBAL UNIQUE (cross-shard global uniqueness). SQLAlchemy ORM basic CRUD / JOIN / pagination / reflection / migration (ADD COLUMN) verified against real drivers.
- **Subqueries (non-correlated WHERE + FROM derived tables + single-equality correlated EXISTS)**: `IN/NOT IN (SELECT..)` / `scalar x op (SELECT..)` / `[NOT] EXISTS (SELECT..)` — inner query runs first → folded into literals / always-true/false → outer query uses existing index paths (large IN cap 65536 + binary search over sorted homogeneous sets); DELETE/UPDATE supported too. `SELECT .. FROM (SELECT ..) t [WHERE/ORDER/LIMIT]` and can participate in JOIN (inner materialized & prefilled, incl. aggregate/GROUP BY, bare COUNT(*)); single-equality correlated `EXISTS/NOT EXISTS` auto-decorrelated to IN; MySQL + PG drivers behave consistently (correlated IN/scalar, JOIN-side derived tables are follow-ups).
- **JOIN (multi-table equi-join)**: N-table left-deep `INNER|LEFT|RIGHT|FULL|CROSS JOIN ... ON/USING` — hash join at the worker completion point (shards only scan locally, zero cross-thread), multi-condition ON + non-equi residual, predicate/projection pushdown + index-driven gather + key-set index point lookup on the probe side (join keys of preceding tables pushed down, ~6x speedup), table aliases / qualified columns / ORDER / LIMIT; MySQL + PG drivers consistent.
- **System tables / introspection (GUI/ORM reflection)**: information_schema (tables/columns/key_column_usage/schemata) + pg_catalog flat single-table + SHOW [FULL] TABLES/COLUMNS/CREATE TABLE/DATABASES + backtick identifiers + `SELECT @@var` stub; SQLAlchemy `inspect()` reflection verified (psql `\d`'s pg_catalog JOIN is a follow-up).
- **Unified record encoding + value type tag**: network facades append a tag on write, transparent to the storage layer — new protocols need zero storage changes.
- **Multi-db / multi-table physical isolation**: `{block_root}/{db_name}/shard_{N}/` directories; db switching goes through MetaPage vpid 0 index.

**IO & scheduling**
- **io_uring persistence**: StdFs backend as fallback; the production path submits SQEs directly via `scheduler::io_ops`.
- **T18 zero-copy**: `IOSQE_FIXED_FILE` + `RegisteredBufPool` dual optimization, removing hot-path memcpy on SQE / FdPool.
- **Hand-written coroutine scheduling**: `crates/scheduler` single-thread park/unpark + priority partitioning (`spawn_on_low` yields the foreground wave to GC/drain).
- **Async flush + bounded backpressure**: `FlushBatch`/`MetaFlushBatch` grouped by file, write ×N + fsync ×1; `MAX_INFLIGHT_CHUNKS=8` degrades to sync when exceeded; the main loop never blocks on fsync.

**Storage engine**
- **Array-based NowChunks + pure COW**: no dirty flag, resident means pending-write; after a full chunk swaps, in-chunk vpids allocate a new pid.
- **Fully flat meta cache**: `meta : data = 1 : 2048` (1 TB data ≈ 512 MB meta/shard-db), full read on open + 1 MB dirty window async flush.
- **data → meta → pid.state flush ordering invariant**: the meta window is only enqueued after the data backlog drains; pid.state is only written after the meta window is confirmed.

**Space & reclamation**
- **GC compact (chunk dead-slot fill + active block drain)**: in-place fill, no new chunk; half-empty blocks are drained round by round and unlinked.
- **PID_FREED tombstone + resurrection prevention**: large-value overwrite frees the old chain and writes a tombstone; recover does not refill dead pages.
- **Large-value overflow pages (≤ 1 MB)**: values over the inline threshold (~4 KB) are automatically split into 16 KB overflow pages + a 13 B descriptor, 0-copy into existing GC.

**Quality**
- **700+ unit/integration tests (currently 862), `cargo clippy --all-targets` 0 warnings.**

---

## Quick Start

```bash
git clone <repo-url> && cd NexusDB
cargo build --release --workspace        # first build ~2min
cp nexusdb.toml /tmp/nexus.toml          # adjust listen_addr / block_root as needed
./target/release/NexusDB --config /tmp/nexus.toml
```

Verify from another terminal (Redis-compatible):

```bash
redis-cli -p 6379 PING                          # PONG
redis-cli -p 6379 SET hello world
redis-cli -p 6379 GET hello                     # "world"
redis-cli -p 6379 MGET hello nosuchkey          # 1) "world"  2) (nil)

# Large value auto-overflow pages (>4KB)
redis-cli -p 6379 -x SET bigkey < some_100k_blob
redis-cli -p 6379 GET bigkey | head -c 102400 | md5sum   # byte-identical
```

Binary protocol port (5433) test cases: [`crates/network/tests/end_to_end.rs`](./crates/network/tests/end_to_end.rs).

5-minute self-check:

```bash
cargo test --workspace --no-fail-fast    # ~30s, expect 0 failed
```

> Docker packaging is available (see [Dockerfile](./Dockerfile) + [docker-compose.yml](./docker-compose.yml)); details in [docs/GUIDE.md](./docs/GUIDE.md#docker-deployment).

---

## Performance

**Test hardware**: Linux 7.0 / AMD Ryzen AI 9 H 365 / 32 GB RAM / io_uring + NVMe SSD / local loopback. Numbers vary by hardware/kernel/IO backend; the tables are a snapshot on this machine and should not be the sole basis for procurement.

### Table 1 — Small-value write-heavy baseline

`memtier_benchmark --ratio=1:1 --pipeline=16 --threads=4 --clients=8 --data-size=64 --test-time=30`

| Metric | Current (String command set + hot-path opt) | Before hot-path opt (same machine A/B) | Before GC |
|---|---|---|---|
| Throughput | **240-310K ops/s** (run-to-run variance) | 201K | 198K |
| p50 | **1.8-2.0ms** | 2.46ms | 2.5ms |
| p99 | **3.4-4.7ms** | 5.34ms | 5.4ms |
| MSET (10 keys, redis-benchmark -P 16) | **107-132K cmd/s ≈ 1.1-1.3M key/s** | - | - |

### Table 1b — Multi-scenario snapshot (2026-07-28, after five data structures; 4 threads × 25 conns pipeline=16)

| Scenario | Throughput | Notes |
|---|---|---|
| GET (warmed) | **395K ops/s** | 3-source read path |
| SET pipeline=1 | 174K, **p50 0.51ms** | single-request latency view |
| Mixed 1:9 (read-heavy) | 315K | |
| Mixed 1:1 | 249K | no String hot-path regression after adding all structures |
| HSET/SADD/ZADD/LPUSH (million keys spread) | 44-97K | composite ops traverse the BTree multiple times (type check + meta maintenance); optimization direction identified |

### Table 2 — Hot-path latency distribution (`NLOG_PROBE=1`, dump on SIGTERM)

| Stage | 1-10μs bucket share | 2-5ms bucket share | Note |
|---|---|---|---|
| `flush_coroutine_total_ns` | dominant | ≈ 0% | fsync fully moved off the main loop |
| `drive_async_flush_round_ns` | dominant | ≈ 0% | shard event loop never blocks |
| `drive_until_idle_ns` | dominant | ≈ 0% | low coroutine scheduling overhead |
| `block_on_io_ns` | dominant | ≈ 0% | sync IO path not triggered |
| `backpressure_sync_write_ns` | dominant | ≈ 0% | backpressure not degraded (in_flight_peak < 8) |

### Table 3 — Large value (memtier `--data-size=65536 --pipeline=4 --threads=2 --clients=4 --test-time=20`)

| Metric | Value |
|---|---|
| Throughput | **31K ops/s (~2 GB/s write bandwidth)** |
| p50 | **0.74ms** |
| p99 | **5.15ms** |
| Single-key cap | 1 MB (64+1 = 65 overflow pages) |

### Table 4 — Crash recovery & GC space observations

| Scenario | Result |
|---|---|
| Periodic flush then `kill -9` → reopen | data intact; pid.state fast path skips header scan |
| 20 × overwrite 512 KB → reopen | live page count matches previous round (no resurrection + no leak) |
| Half-empty block → drain | block file unlinked, data dir stable |
| 1 MB × 20 SET → reopen | data does not diverge, du ≈ 17 MB / 6 blocks |

### Tuning guide

| Parameter | Recommendation |
|---|---|
| `io_backend` | `io_uring` (+30-50% vs `stdfs` on NVMe); use `stdfs` in containers/sandboxes without support |
| `chunk_cache_size` | default 16 (16 MB hot cache); usually **no need to increase after GC** — dead pages are unlinked, the hot set shrinks naturally |
| `num_shards` | ≤ physical CPU cores; more than cores causes cross-core scheduling jitter |
| `max_key_bytes` | keep at 1024 B (keys participate in internal-page routing) |
| `max_value_bytes` | default 1 MB; values ≤ 4 KB inline automatically, otherwise overflow-page chain |

**Method**: run a few rounds with `NLOG_PROBE=1` → inspect `flush_coroutine_total_ns` / `drive_async_flush_round_ns` histograms to locate the longest stage → tweak params → re-run memtier and compare. Always run `cargo test --workspace` before/after tuning to ensure no regression.

---

## Architecture

**Share-Nothing = each shard is one OS thread exclusively owning all data structures + one io_uring instance + one Scheduler instance.** Cross-shard communication is only via mpsc / Inbox / TaskReplyBus — lock-free, race-free.

```text
  Binary 5433 (TCP) ─┐
                     ├── NetworkServer ── KvLimits check ── shard_manager::Router
  RESP  6379 (TCP) ─┘                                                  │
                                                                         ▼
                  shard_n thread (per-core, single-thread event loop)
                                                                         │
                +--------+-----------+-------------------+----------------+
                ▼        ▼           ▼                   ▼                ▼
            LCB-Tree   NowChunks   WriteQueue         ChunkList       MetaCache
             (page)   (active)    (in-flight 8)      (LRU 1MB×16)    (flat Vec + dirty window)
                │        │           │                   │                │
                +--------+-----+-----+-------------------+----------------+
                                  ▼
                          pager_io (io_uring / StdFs)
                                  ▼
                          .block + page.mate (per-db-per-shard)

          ┌──────┐   ┌──────┐
          │ GC   │   │      │   ChunkLiveness (in-memory live-page counts)
          │coro  │◄─►│ bg   │   Low-priority scheduling (spawn_on_low)
          └──────┘   └──────┘   Compact / Drain / tombstone reclaim
```

Key design points:

- **chunk = 1 MB = 64 pages × 16 KB**; NowChunks index = `chunk_idx` lazily grown, no dirty flag — resident means pending-write.
- **meta = `Vec<PidLocation>`, index = vpid**; open reads page.mate fully into memory (no pread miss); flush uses async coroutines over 1 MB windows.
- **Async flush never blocks the main loop**: `complete_flush` only sets `due=true`; fsync runs in a coroutine; the reaping path is ≈ 1.5μs.
- **Ordering invariant**: data chunk write confirmed → meta window write → pid.state, a three-stage closed loop; any stage can be retried on failure.
- **GC & large values**: see the two sections below.

Full breakdown: [DESIGN.md](./DESIGN.md) (10 sections).

### Platform dependencies

- **OS**: Linux (glibc / musl); the io_uring backend requires kernel ≥ 5.6 (≥ 5.15 recommended).
- **Stack size**: `RUST_MIN_STACK=8388608` (8 MB); IO-heavy calls recurse deeply on the default stack.
- **Disk**: NVMe SSD gives fsync ≤ 100μs; SATA SSD ≈ 1ms.
- **io_uring capability**: `cat /proc/sys/kernel/io_uring_disabled` should be `0`.

---

## GC & Space Reclamation

### Liveness counting (in-memory, rebuilt on restart)

vpids are **never reclaimed**, but the underlying chunk/block physical space is reclaimed by GC:
- `ChunkLiveness::live[]`: 1 B live-page count per chunk (0..64), `Vec<u16> block_active[]` aggregates to file.
- **Write-path advancement**: COW alloc / delete / compact increment/decrement; a chunk hitting zero live pages → `pending_free`.
- **Restart rebuild**: `rebuild_from_meta` walks the full flat meta to rebuild (a benefit of the vpid array, done in tens of ms).

### chunk compact (dead-slot fill)

```text
victim B → dead slot in A → fill A's empty 16KB pages → fsync
                                       ↓
                  meta CAS (vpid == B.pid ? → A.pid)  → meta fsync  → promote(B)
```

- **In-place**: no new chunk; A's live pages are untouched (rewriting the whole 1 MB would damage its own live pages, so it's not done).
- **CAS commit**: prevents concurrent COW writes from rolling back moved pages.
- **Deferred free**: B enters `pending_free`, only moving to `free_chunks` after the meta window is confirmed (avoiding reuse before the old location is read).
- **Low-priority coroutine**: runs at the tail of the wave via `spawn_on_low`, capped at 1 poll per wave, not affecting the foreground.

### block drain (actively empty half-empty blocks)

- After chunk compact, **scan candidates**: find `0 < block_active ≤ 3` half-empty blocks (fully empty ones take the unlink path).
- **Sharded state machine**: `drain_block_target` records the target, migrating one chunk per round (reusing the chunk-compact three-stage pipeline).
- **Fresh bump dst**: when no dead slots are available, open a brand-new chunk as host, writing the full 1 MB back at once (normal dst still uses batched dead-slot writes).
- **Done = all dead**: the meta confirmation point triggers `maybe_drop_free_blocks`, evicting the fd_cache + FdPool fixed slot → unlink.

### recover source switch

- **page.mate is primary**: the vpid→pid mapping treats meta as the source of truth.
- **Scan only fills gaps**: .block scanning only refills vpids missing from meta (meta tombstones / already-recorded vpids are left alone — otherwise stale on-disk page headers would **resurrect** dead pages).
- **pid.state fast path**: the 8 B `PidLocation` persisted at last flush; take the larger of it and the scan, lagging is safe.

Implementation: [`crates/storage/src/chunk_liveness.rs`](./crates/storage/src/chunk_liveness.rs) · [`crates/scheduler/src/scheduler.rs:spawn_on_low`](./crates/scheduler/src/scheduler.rs) · [`crates/storage/src/pager.rs:start_compact`](./crates/storage/src/pager.rs)

---

## Large-Value Overflow Pages

### Data format

```text
leaf item value:
  inline:   [raw bytes]                                 (first byte != 0x00)
  indirect: [0x00][head_vpid u64 LE][total_len u32 LE]  (13B descriptor)

OverflowIndex page (head_vpid):
  [0..0x28]   standard page header (page_type = 5)
  [0x28..0x2A] count u16 LE
  [0x2A.. ]   count × vpid u64 LE

Overflow data page:
  [0..0x28]   standard page header (page_type = 4)
  [0x28.. ]   payload slice (last page truncated)
```

### Design points

| Item | Decision | Rationale |
|---|---|---|
| Indirect marker | **0x00** (first byte) | value_codec tags start at 0x01+, never collide; zero migration for existing data |
| Threshold | `key_len + value_len > 4000` | aligned with the 4096 page-item buffer |
| Single-level indirect | 1 index page + 64 data pages ≈ 1 MB | single addressing has inline-buf headroom; multi-level streaming reserved |
| Standard page header | overflow pages carry a full LCBP header | zero-change compatibility for recover/compact (identified by vpid+page_type) |

### Leak-prevention invariants (core of the modify path)

- **Overwrite succeeds → free old chain**: if the old value is a descriptor, `free_overflow` frees each page via `Pager::free_overflow_vpid` (liveness decremented → chunk/block GC reclaims).
- **New chain written but leaf commit fails → roll back and free new chain**: the error path leaves no orphans.
- **PID_FREED tombstone**: freeing **is not zeroing the slot** but writing a `PID_FREED` tombstone persisted with the dirty window.
- **recover does not refill tombstones**: `has_record` replaces `peek`, so stale on-disk page headers don't resurrect dead pages.

Implementation: [`crates/storage/src/overflow.rs`](./crates/storage/src/overflow.rs) · [`crates/storage/src/pager.rs:free_overflow_vpid`](./crates/storage/src/pager.rs) · [`crates/storage/src/meta_cache.rs:free_slot / has_record`](./crates/storage/src/meta_cache.rs)

---

## Supported Protocols

| Protocol | Port | Status | Notes |
|---|---|---|---|
| RESP2 (Redis-compatible) | 6379 | ✅ Complete | **Five data structures + Geo + Bitmap** full command surface, see table below; large-value overflow pages automatic |
| **HTTP REST** | **6778** | ✅ | **KV + SQL JSON API** + CORS + Bearer auth + `/metrics` (Prometheus); for AI tools / web frontends / monitoring, see REST section |
| **MySQL (wire)** | **5434** | ✅ SQL subset | **`mysql` CLI direct connect** + `mysql_native_password` / `caching_sha2_password` fast-auth login + **TLS (opt-in)**; syntax in the SQL facade section below |
| **PostgreSQL (wire)** | **5435** | ✅ SQL subset | **`psql` direct connect** + **SCRAM-SHA-256 auth** + **TLS (opt-in, SSLRequest→'S')**; **shares the kernel** with MySQL facade, same-db read/write |
| Binary (custom) | 5433 | ⚠️ Internal | internal protocol (test/bench tools); use REST/RESP/SQL for external access, disabled by default in future versions |
| MongoDB (BSON) | - | 🚧 Roadmap | see [DESIGN.md §10](./DESIGN.md) |

### RESP command surface (2026-07-28)

| Structure | Commands |
|---|---|
| String | SET/GET/DEL/EXISTS/STRLEN/TYPE, MGET/MSET/MSETNX (cross-shard grouped aggregation + leaf range reuse), INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT/APPEND/SETNX (shard-side atomic RMW), GETRANGE/SETRANGE/GETDEL/GETSET |
| Hash | HSET/HMSET/HSETNX/HGET/HMGET/HDEL/HEXISTS/HLEN/HGETALL/HKEYS/HVALS/HSCAN/HINCRBY/HINCRBYFLOAT/HSTRLEN/HRANDFIELD |
| Set | SADD/SREM/SISMEMBER/SMISMEMBER/SCARD/SMEMBERS/SSCAN/SPOP/SRANDMEMBER (with count), SINTER/SUNION/SDIFF/SINTERCARD/SINTERSTORE/SUNIONSTORE/SDIFFSTORE |
| List | LPUSH/RPUSH/LPOP/RPOP (with count)/LLEN/LRANGE/LINDEX/LSET, LREM/LTRIM/LPOS/LINSERT (mid-list ops) |
| ZSet | ZADD/ZREM/ZSCORE/ZMSCORE/ZCARD/ZCOUNT/ZINCRBY, ZRANGE/ZREVRANGE/ZRANGEBYSCORE/ZRANK/ZREVRANK (dual index), ZPOPMIN/ZPOPMAX, ZINTERSTORE/ZUNIONSTORE (SUM, no weights) |
| Geo | GEOADD/GEOPOS/GEODIST/GEOSEARCH (FROMLONLAT+BYRADIUS; 52-bit geohash reusing ZSet dual index) |
| Bitmap | SETBIT/GETBIT/BITCOUNT/BITPOS (BYTE granularity; reuses String bytes) |
| Connection | PING/ECHO/AUTH/QUIT/HELLO/**SELECT** (pick db)/COMMAND (pipeline FIFO reordering, TCP_NODELAY) |

> Not supported (recorded): TTL family (EXPIRE/SET EX·PX·NX·XX), cross-key atomics (BITOP/SMOVE/LMOVE/BLPOP), ZSTORE WEIGHTS/AGGREGATE, Stream, HyperLogLog; MSETNX / Set algebra / *STORE are non-atomic across shards.

### Sharding by db & table (2026-07-29)

The engine is natively multi-db and multi-table; the RESP-side mapping conventions:

- **Pick db — `SELECT n`**: dbs persist a numeric id (`DbNameResolver`, name↔id bidirectional, 2PC-synced replica across shards). KV clients use the numeric id, SQL facades use the name, translated uniformly at the protocol layer. Per-connection state, reset on disconnect; out of range returns `-ERR DB index is out of range`. **Does not auto-create dbs** — set `precreate_dbs = N` to pre-create `db1..dbN` at startup.
- **Pick table — key colon prefix**: `SET user:1000 x` → table `user`, key `1000`; no colon → default table. Only splits the **first** colon (`user:1000:profile` → table `user`, key `1000:profile`); table names limited to `[A-Za-z0-9_.-]{1,64}`, illegal prefixes (empty/binary/too long) fall into the default table. The protocol is stateless, so cross-table MGET/MSET/SINTERSTORE are valid.
- **Lazy table creation**: a table is created automatically by its owning shard's data plane on first write/read (locally idempotent, no 2PC), fully crash-recoverable. Note: `list_tables` may be inconsistent across shard views (inherent to lazy creation).

### SQL facades (MySQL wire 5434 + PostgreSQL wire 5435, 2026-07-30)

**Dual facades share the kernel**: the same parser / planner / aggregation state machine, only framing and result-set encoding diverge by protocol; both read/write the same data, MySQL-write / psql-read verified consistent. Separate ports (MySQL server speaks first vs PG client speaks first; a shared port would need sniffing, not worth it).

**Prepared statements (ORM integration)**: `?` (MySQL) / `$n` (PG) placeholders, AST template binding (zero injection surface); MySQL COM_STMT_* (binary params/result set) + PG extended query protocol (Parse/Bind/Describe/Execute/Sync). Verified drivers: mysql-connector-python (prepared=True), psycopg3; Go database/sql, JDBC, mysql2, node-postgres, pgx take the same protocol path. asyncpg (Flush timing dependency) not yet guaranteed.

**Transactions (F61 v1 + F62 v2)**: `BEGIN / START TRANSACTION / COMMIT / ROLLBACK` on both protocols. Conn-layer write_set buffering; on COMMIT, applied atomically in shard-grouped batches (validate-then-write + reply only after WAL fsync — **COMMIT return means durable**, independent of wal_mode).

| Isolation level | Implementation | Semantics |
|---|---|---|
| READ UNCOMMITTED / READ COMMITTED | RC (default) | read committed + RYOW for this txn's pk point reads |
| REPEATABLE READ / SERIALIZABLE | row-level OCC validation | pk point reads within the txn record a fingerprint, re-read & compare at commit — concurrent modification → **40001/1213** for the ORM to retry; prevents dirty read / non-repeatable read / lost update / row-level write skew, **does not prevent phantoms** (scan reads not added to the read set) |

Syntax: `SET [SESSION] TRANSACTION ISOLATION LEVEL ... [READ ONLY|READ WRITE]`, `BEGIN ISOLATION LEVEL ... [READ ONLY]`, `SAVEPOINT / ROLLBACK TO / RELEASE` (SQLAlchemy nested-transaction mode; ROLLBACK TO can recover in PG's E state). PG transaction block state (I/T/E + 25P02) and MySQL IN_TRANS flag are complete — psycopg3 default transaction mode / `isolation_level=SERIALIZABLE` (typed SerializationFailure capture) and mysql-connector both verified. v1/v2 boundary: single-shard txns strictly atomic, cross-shard best-effort; ROLLBACK/disconnect discards at zero cost; DDL rejected inside a txn.

```bash
mysql -h127.0.0.1 -P5434 -uroot -ps3cret --default-auth=mysql_native_password
psql "host=127.0.0.1 port=5435 user=root dbname=default password=s3cret"
```

**Syntax subset**:

```sql
CREATE TABLE users (id BIGINT PRIMARY KEY, email VARCHAR(64) UNIQUE,
                    name TEXT NOT NULL, score DOUBLE PRECISION, INDEX(name), INDEX(score));
INSERT INTO users VALUES (1,'a@x','alice',95.5), (2,'b@x','bob',80.0);  -- multi-row
SELECT * FROM users WHERE id = 1;                          -- pk point lookup (single shard)
SELECT name, id FROM users WHERE name = 'alice';           -- covering index, no table lookup
SELECT id FROM users WHERE score BETWEEN 80 AND 99 AND name != 'bob'
  ORDER BY score DESC, id LIMIT 10 OFFSET 5;
SELECT COUNT(*) FROM users WHERE name IN ('alice', 'bob');
SELECT * FROM users WHERE email LIKE 'a%';                 -- prefix LIKE → range
SELECT * FROM users WHERE note = 'x';                      -- no index → full scan + filter
UPDATE users SET score = 99.0, name = 'al' WHERE id = 1;   -- partial column update
DELETE FROM users WHERE score < 60;                        -- index/full-scan conditional delete
DROP TABLE users;  USE db1;  DESCRIBE users;               -- utility commands
```

Types: `INT/BIGINT/SMALLINT → I64`, `BOOLEAN/BOOL → Bool`, `DOUBLE [PRECISION]/FLOAT/REAL → F64`, `DECIMAL/NUMERIC(p,s) → fixed-point i128 (exact money, precision ≤ 38)`, `TEXT/VARCHAR(n)/CHAR(n) → Str`, `BLOB/BYTEA → Bytes`, `DATE/TIME/TIMESTAMP/DATETIME → i64 microseconds (naive UTC)`, `JSON/JSONB → text Bytes`, `UUID → 16B`. Time/boolean/UUID/DECIMAL render natively per column type on both facades (psycopg3 → date/datetime/bool/UUID/Decimal; mysql-connector text + prepared both native datetime/Decimal, no precision loss).

**Execution model (local secondary index + two-layer pruning)**:

- Index rows and data rows **coexist on the same shard** (routed by pk hash) — writes are single-shard atomic, no cross-shard coordination.
- `WHERE pk =` → single shard; indexed column → broadcast "local index scan + local table lookup" one-hop closed loop; no index → full scan + residual filter.
- **Two-layer bloom pruning** (equality): worker route-cache zero-task short circuit + shard-local bloom O(1) rejection, insert-only construction with no false negatives.
- Covering index (projection ∪ conditions ∪ sort ⊆ index columns + pk) skips table lookup; UNIQUE equality stops early on first hit.
- DELETE/UPDATE: pk equality single-shard atomic; index/scan conditions two-phase (collect pks then dispatch per-pk, non-atomic, gap recorded).

**SQL baseline** (5000-row × 2-index table, 4 conns, full MySQL wire path):

| Scenario | Throughput | p50 |
|---|---|---|
| pk point lookup | ~57K qps | 55µs |
| UNIQUE equality (early stop) | ~37K qps | 60µs |
| equality miss (zero-task short circuit) | ~62K qps | 35µs |
| equality 100 rows (covering index) | ~4-5.7K qps | 0.7ms |
| ORDER BY + LIMIT (indexed 100 rows) | ~2.9K qps | 1.2ms |
| DELETE / INSERT (pk) | ~11K qps | 0.11ms |
| full scan 5K rows (+filter/COUNT) | ~70 qps | 55ms |

> SQL gaps (recorded): JOIN/OR/subqueries delivered (F67-F75, see capability table above; remaining: correlated IN/scalar, multi-correlated EXISTS, JOIN-side derived tables); no client-cert mutual auth / no SCRAM channel binding (TLS transport encryption delivered: PG SCRAM-SHA-256 + MySQL caching_sha2 auth, rustls STARTTLS opt-in; single `sql_password`, no per-user permission system); aggregates lack GROUP_CONCAT/window functions (GROUP BY currently collects all rows, shard-side partial aggregation pushdown is a follow-up); LIKE prefix mode only; plain **UNIQUE is cross-shard best-effort** (probe only on the local shard) — use `GLOBAL UNIQUE` for global uniqueness (email-shard placeholder; writing/UPDATE-ing that column inside a txn is a v1 boundary); non-transactional DELETE/UPDATE two-phase and multi-row INSERT are non-atomic (single-shard atomic inside a txn); cross-shard txns best-effort; ORDER BY has no top-k (full sort); schema broadcast is non-atomic (idempotent retry).

### REST facade (HTTP/1.1, 6778, 2026-07-30)

Zero-dependency hand-written HTTP/1.1 (keep-alive, no chunked/TLS/HTTP2), for AI tools / web frontends / monitoring; reads/writes the same data as the other facades.

```bash
# KV (value: string or number; numbers use the native binary tag, interop with RESP)
curl -X PUT localhost:6778/v1/kv/user/alice -d '{"value":"hello"}'
curl localhost:6778/v1/kv/user/alice          # {"value":"hello"}  (404 = not found)
curl -X DELETE localhost:6778/v1/kv/user/alice # {"deleted":true}
# SQL (full syntax subset, same as MySQL/PG facades)
curl -X POST localhost:6778/v1/sql -d '{"query":"SELECT * FROM rt WHERE id = 1", "db":"optional"}'
# → {"columns":["id","v"],"rows":[[1,"x"]]} | {"affected":n} | 4xx/5xx {"error":"..."}
# Observability (no auth)
curl localhost:6778/metrics       # Prometheus text (requests/errors/sql/kv/uptime)
curl localhost:6778/v1/status     # {"version","uptime_seconds","num_shards","protocols"}
curl localhost:6778/v1/debug/sql-cache  # worker route-cache stats (auth required)
```

- **CORS**: `http_cors_origin = "*"` or a specific origin → responses carry Allow-Origin, OPTIONS preflight returns 204 with full headers; empty = none sent.
- **Auth**: non-empty `http_token` → requires `Authorization: Bearer <token>` except for `/metrics` `/v1/status` (401 otherwise).
- **Error mapping**: syntax/unknown column/unknown db → 400, duplicate key → 409, engine error → 500; KV not found → 404.
- Baseline (4 keep-alive conns): KV GET/PUT ~10.4K rps, SQL pk point lookup ~11.3K rps (p50 0.25ms; management/integration surface, use RESP for high throughput).
- gaps: no chunked (Content-Length only) / no streaming large result sets (SELECT is full JSON) / single worker.

Design philosophy: **unified record encoding + value type tag** is reserved (`TAG_RAW/TAG_I64/TAG_F64/TAG_STR/TAG_DOC`); adding a new protocol needs **no storage-layer change**, just a parser + adapter under `crates/network/src/protocol/<x>/`.

---

## Configuration

Full field comments in [`nexusdb.toml`](./nexusdb.toml). Key sections:

```toml
[server]
listen_addr = "0.0.0.0:5433"     # Binary protocol
redis_addr = "0.0.0.0:6379"      # RESP (empty string = disable)
sql_addr = "0.0.0.0:5434"        # SQL facade MySQL wire (empty = disable)
sql_worker_count = 1             # MySQL/PG facade worker count (2-8 for ORM pools; 16 conns 4 workers ≈ 2.5x)
pg_addr = "0.0.0.0:5435"         # SQL facade PostgreSQL wire (empty = disable)
http_addr = "0.0.0.0:6778"       # REST facade (empty = disable)
http_cors_origin = ""            # CORS Allow-Origin ("*"/specific origin, empty = none)
http_token = ""                  # REST Bearer token (empty = no auth)
sql_password = ""                # SQL login password, shared by both facades (empty = no auth; non-empty → PG uses SCRAM-SHA-256)
redis_password = ""              # AUTH password
tls_cert = ""                    # ⭐ SQL/PG facade TLS cert PEM path (empty = plaintext; both must be non-empty to enable)
tls_key = ""                     # ⭐ TLS private key PEM path (PKCS8/PKCS1/SEC1)
max_key_bytes = 1024             # key cap (protocol-layer check)
max_value_bytes = 1048576        # value cap (>4KB auto overflow pages)

[storage]
block_root = "./data"
num_shards = 6                   # cross-shard only hash(key), lock-free
io_backend = "io_uring"          # "stdfs" | "io_uring"
chunk_cache_size = 16            # ChunkList LRU capacity
create_if_missing = true
default_db = "default"           # startup default db
default_table = "default"        # startup default table
precreate_dbs = 0                # pre-create db1..dbN for SELECT n (0 = default db only)

[log]
level = "info"                   # error|warn|info|debug|trace
dir = "./logs"                   # empty = stderr only
buffer_kb = 64
flush_interval_ms = 500
stderr = true
```

`max_value_bytes` was raised from 3 KB to **1 MB** by default (1 MiB + 64 B headroom for tag bytes); `max_key_bytes` stays at 1024 B (keys participate in internal-page routing, no overflow).

---

## Crate Responsibilities

| crate | Responsibility | Status |
|---|---|---|
| [`crates/scheduler`](./crates/scheduler) | single-thread coroutine scheduler + io_uring bridge (`SchedHandle`/`drive_until_idle`/`io_ops`/`FdPool`/`spawn_on_low`) | ✅ |
| [`crates/page`](./crates/page) | LCB-Tree pages (leaf/insert/split/delete/checkpoint/prefix compression/`ItemKind` encoding) | ✅ |
| [`crates/storage`](./crates/storage) | physical persistence layer: `Pager`/`MetaCache` v3 fully flat + dirty window/`NowChunks` array-based/`ChunkList` LRU/`ChunkLiveness` + GC/`overflow` large values/`recover` primary source + tombstone resurrection prevention | ✅ |
| [`crates/network`](./crates/network) | multi-protocol facades (`acceptor` + `epoll worker` + RESP2 + MySQL/PG/HTTP + Binary + `KvLimits` + `value_codec` + `tls`) | ✅ |
| [`crates/shard_manager`](./crates/shard_manager) | multi-shard controller (`ShardManager`/`Router`/`Inbox`/`TaskReplyBus`) + `latency_probe` + stress bench | ✅ |
| [`crates/config`](./crates/config) | TOML config loading | ✅ |
| root `src/main.rs` | server entry: `nexusdb --config nexusdb.toml`, graceful shutdown on signal | ✅ |

Per-crate implementation details: [`docs/plans/`](./docs/plans/) (plan index in each file header).

---

## Dev Commands

```bash
# Full regression (700+ tests, ~30s)
cargo test --workspace --no-fail-fast

# clippy (0-warning hard constraint)
cargo clippy --workspace --all-targets

# release build (required before perf testing)
cargo build --release

# start (production)
RUST_MIN_STACK=8388608 ./target/release/NexusDB --config nexusdb.toml

# start + probe (perf tuning, histogram dumped to stderr on SIGTERM)
NLOG_PROBE=1 ./target/release/NexusDB --config nexusdb.toml

# single-crate test (fast dev iteration)
cargo test -p storage --lib
cargo test -p shard_manager --lib

# single test case (debugging)
cargo test -p storage --test recover_tests -- --exact some_test_name

# large-value e2e (RESP)
redis-cli -p 6379 -x SET bigkey < /dev/urandom   # 1024B..1MB auto overflow
redis-cli -p 6379 GET bigkey                     # byte-identical
```

Debugging tips and gotchas: [AGENTS.md](./AGENTS.md).

---

## Troubleshooting

| Symptom | Likely cause / action |
|---|---|
| startup `permission denied` / `disk full` | `block_root` path permission / disk space; check [nexusdb.toml](./nexusdb.toml) `[storage].block_root` |
| startup hangs at io_uring init | container/sandbox without io_uring support; set `io_backend = "stdfs"` to work around |
| `RST_STREAM` tail-latency spikes | network-layer TCP_NODELAY caveats; see [AGENTS.md](./AGENTS.md) |
| p99 spikes to ms level | usually disk fsync queuing; switch to NVMe / use `NLOG_PROBE=1` for a probe comparison |
| large-value GET returns `ERR ... value too long` | payload exceeds `max_value_bytes` (default 1 MB); check [nexusdb.toml](./nexusdb.toml) or the client→server path |
| p99 jumps from 3 ms to 6 ms | usually in-flight 8 cap degrading to sync writes; lower `[storage].num_shards` or upgrade SSD |
| data not found | multi-db switching: confirm the db name used on SET (`SELECT dbname`); the default db is always valid |

### Known gaps (recorded in DESIGN/AGENTS)

- **vpid reclamation**: vpids are not reclaimed; under heavy-delete workloads `Vec<PidLocation>` grows to the max vpid.
- **per-db per-mate**: currently all dbs share one mate file (off = `vpid*8`); can be split for multi-db + large-vpid scenarios.
- **PG / MySQL / MongoDB protocols**: DESIGN §10 roadmap; unified record encoding and value tag are ready (MySQL/PG delivered).
- **Range scan / cursor**: dependency for List/ZSet/Stream, planned next.
- **Transaction / MVCC**: the single-thread Pager is naturally serial with no concurrency, not urgent now.

### Debug probes

Start with `NLOG_PROBE=1` → on SIGTERM, a 16-bucket histogram is dumped to stderr:

- `flush_coroutine_total_ns` — total time of a single flush coroutine (write + fsync)
- `drive_async_flush_round_ns` / `drive_until_idle_ns` — shard event-loop stages
- `block_on_io_ns` / `poll_wait_ns` — sync wait / poll wakeup
- `backpressure_sync_write_ns` — backpressure-degraded sync write (≈ 0 means not triggered)
- `in_flight_peak` — async flush depth peak

---

## Documentation Index

| Reader | Doc |
|---|---|
| Evaluation / day one | this README (+ [docs/GUIDE.md](./docs/GUIDE.md) usage guide) |
| Architecture | [DESIGN.md](./DESIGN.md) (10 sections) |
| Development handoff (progress / gotchas / TODO) | [AGENTS.md](./AGENTS.md) |
| Fix history (F1-F…) | [CHANGELOG.md](./CHANGELOG.md) |
| Per-crate phased plans | [`docs/plans/`](./docs/plans/), [`docs/specs/`](./docs/specs/) |
| Bug root-cause investigation example | [`docs/bug-report-btree-split-routing.md`](./docs/bug-report-btree-split-routing.md) |

---

## License

NexusDB source is under [LICENSE](./LICENSE) (see repo root).

Acknowledgments: the protocol layer draws on [monoio](https://github.com/bytedance/monoio) / `tokio`'s io_uring experimental branch; performance baselines reference [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark).
