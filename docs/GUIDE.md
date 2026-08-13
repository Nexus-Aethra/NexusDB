# NexusDB Feature Overview & Usage Guide

> **语言 / Language**: **English** | [简体中文](./GUIDE-CN.md)

> A user-facing getting-started guide. For design & implementation details see [`DESIGN.md`](../DESIGN.md); for fix/evolution history see [`CHANGELOG.md`](../CHANGELOG.md).

## Table of Contents
1. [What It Is](#1-what-it-is)
2. [Quick Start](#2-quick-start)
3. [Multi-Protocol Access](#3-multi-protocol-access)
4. [SQL Capabilities & Examples](#4-sql-capabilities--examples)
5. [Data Types](#5-data-types)
6. [Security: Auth & TLS](#6-security-auth--tls)
7. [Configuration Reference](#7-configuration-reference)
8. [Measured Performance](#8-measured-performance)
9. [Capability Boundaries](#9-capability-boundaries)

---

## 1. What It Is

NexusDB is an **embedded single-node, write-heavy / low-latency / high-concurrency KV + SQL database engine** written in Rust 2024. Core characteristics:

- **Architecture**: Share-Nothing + per-core threads + io_uring async I/O + a hand-written coroutine scheduler (no tokio dependency).
- **Storage**: COW append-only + LCB-Tree pages (prefix compression) + multi-db/multi-table physical isolation + crash recovery.
- **Multi-protocol facades (one kernel, five ways in)**:
  - **RESP2** (Redis-compatible): five data structures + Geo + Bitmap
  - **MySQL wire**: `mysql` CLI / driver direct connect
  - **PostgreSQL wire**: `psql` / psycopg direct connect
  - **HTTP REST**: KV + SQL JSON API + Prometheus `/metrics`
  - **Binary** (internal / benchmarking)
- **SQL subset**: DDL/DML/SELECT (JOIN, subqueries, aggregates, GROUP BY/HAVING, DISTINCT, expression aggregates), transactions (OCC isolation levels + SAVEPOINT), local secondary indexes + bloom pruning + GLOBAL UNIQUE.
- **Security**: MySQL `caching_sha2_password` / PostgreSQL `SCRAM-SHA-256` auth + **rustls TLS** (opt-in).

> The MySQL and PostgreSQL facades **share the same kernel and the same data** — you can read/write the same database across both.

---

## 2. Quick Start

### Build
```bash
# debug build
cargo build
# release build (for perf testing / deployment)
cargo build --release
```

> Note: the storage layer's async frames are large; running/testing needs a bigger thread stack: `RUST_MIN_STACK=67108864`.

### Minimal config `nexusdb.toml`
```toml
[server]
sql_addr = "127.0.0.1:5434"      # MySQL wire
pg_addr  = "127.0.0.1:5435"      # PostgreSQL wire
redis_addr = "127.0.0.1:6379"    # RESP (Redis)
http_addr  = "127.0.0.1:6778"    # HTTP REST
sql_password = ""                # empty = no auth

[storage]
block_root = "./data"
default_db = "default"
default_table = "default"
```

### Start
```bash
RUST_MIN_STACK=67108864 ./target/release/NexusDB --config nexusdb.toml
```
On startup the log prints each facade's listen address. Leaving any facade's `addr` empty disables that facade.

### Docker Deployment
The repo ships a [`Dockerfile`](../Dockerfile) (multi-stage: Rust builder → debian-slim) + [`docker-compose.yml`](../docker-compose.yml) + a container default config [`deploy/nexusdb.docker.toml`](../deploy/nexusdb.docker.toml).
```bash
# build image
docker build -t nexusdb:latest .

# run directly (persist to a named volume nexusdb-data)
docker run -d --name nexusdb \
  -p 6379:6379 -p 5434:5434 -p 5435:5435 -p 6778:6778 \
  -v nexusdb-data:/data nexusdb:latest

# or one-shot with compose
docker compose up -d
```
- Data persists at the container `/data` (`VOLUME`); a built-in `HEALTHCHECK` probes HTTP `/v1/status`.
- Override config: `-v /path/your.toml:/etc/nexusdb/nexusdb.toml`.
- **io_uring note**: default `io_backend=io_uring` needs a recent kernel; if the host seccomp blocks it and I/O errors appear, set `io_backend="stdfs"` (mount a custom config) or `docker run --security-opt seccomp=unconfined`.
- Enable TLS: mount a cert directory and set `tls_cert`/`tls_key` in the config.

### Windows Deployment (Beta, 2026-08-13)

Native Windows support is in beta on the `feat/resp-sql-schema-adapter`
branch.  Plan + gotchas:
[`docs/plans/2026-08-13-windows-portability.md`](./plans/2026-08-13-windows-portability.md) +
[`docs/plans/2026-08-13-windows-iocp.md`](./plans/2026-08-13-windows-iocp.md).

```bash
# 1) toolchain
rustup default stable-x86_64-pc-windows-msvc

# 2) build
git clone https://github.com/Nexus-Aethra/NexusDB.git
cd NexusDB
cargo build --release --workspace
```

Minimal `nexusdb-test.toml` (auto-corrects `io_backend` to `stdfs`):

```toml
[server]
listen_addr = "127.0.0.1:5433"     # Binary
redis_addr  = "127.0.0.1:6380"     # RESP (6380 to avoid clashing with the
                                  # SYSTEM-account redis-server on 6379 that
                                  # ships with the Redis.Redis winget package)
worker_count = 1
sql_addr = ""                    # SQL/PG/HTTP are Linux-only paths
pg_addr   = ""
http_addr = ""

[storage]
block_root = "./data-test"
num_shards = 2
io_backend = "stdfs"
create_if_missing = true
default_db = "default"
default_table = "default"
precreate_dbs = 1
```

Run + smoke:

```bash
./target/release/NexusDB.exe --config nexusdb-test.toml
# in another shell (assuming Redis.Redis installed under Program Files):
& "C:\Program Files\Redis\redis-cli.exe" -p 6380 PING             # PONG
& "C:\Program Files\Redis\redis-cli.exe" -p 6380 SET user:1 alice # OK
& "C:\Program Files\Redis\redis-cli.exe" -p 6380 GET user:1       # alice
& "C:\Program Files\Redis\redis-cli.exe" -p 6380 DEL user:1       # 1
```

What works on Windows: Binary (5433) + RESP (6380), `PING`/`AUTH`/`SET`/`GET`/`DEL`/`MGET`/`MSET`/etc., WAL persistence + crash recovery, Ctrl-C graceful shutdown.

What does not: MySQL / PostgreSQL / HTTP REST / TLS facades, IOCP/RIO perf path, memtier benchmarks, the 860-test matrix. `INCR`/`HSET`/`LPUSH`/`SADD`/`ZADD`/`DBSIZE`/`INFO`/`CLIENT LIST` are not yet wired in the dispatch tree (same gap on Linux's `portable.rs`; protocol-layer work tracked separately).

Gotchas:

- **No `io_uring`**: `io_backend` is ignored on Windows; missing-config path silently downgrades to `stdfs`. Pin `io_backend = "stdfs"` to be explicit.
- **Port 6379 occupied by the OS-bundled `redis-server`**: `Redis.Redis` winget package installs `redis-server.exe` running as SYSTEM on 6379; you cannot stop it without admin. Use 6380 (or any free port) for testing. Production: pick your own port.
- **Listener `set_nonblocking(true)` propagates to child sockets**: per-connection read must treat `WSAEWOULDBLOCK` (10035) and `WSAETIMEDOUT` as transient back-pressure (retry + short sleep). Returning `Err` on those errors breaks the conn for the client (`"An existing connection was forcibly closed"`).
- **`#[repr(C)]` on `OverlappedData`**: if you ever re-enable the IOCP path, the `OVERLAPPED` field MUST be the first field and the struct MUST be `#[repr(C)]`. Rust's default `repr(Rust)` will reorder fields and GQCS will hand you the wrong dispatch state.
- **`windows-sys = "0.61"`**: `ACCEPTEX` does not exist; the type is `LPFN_ACCEPTEX` (an `Option<unsafe extern "system" fn(...)>`). `setsockopt`'s 4th argument is `PSTR` (`*const u8`), not `*const c_void`.

---

## 3. Multi-Protocol Access

### 3.1 RESP (Redis-compatible, default 6379)
```bash
redis-cli -p 6379
> SET user:1 alice
OK
> GET user:1
"alice"
> HSET h f1 v1 f2 v2
> LPUSH mylist a b c
> ZADD z 1 one 2 two
> GEOADD geo 13.361 38.115 "palermo"
> SETBIT bm 7 1
```
Supports the String/Hash/List/Set/ZSet five structures + Geo + Bitmap command surface.

### 3.2 MySQL wire (default 5434)
```bash
mysql -h127.0.0.1 -P5434 -uroot
```
```python
# mysql-connector-python
import mysql.connector
c = mysql.connector.connect(host="127.0.0.1", port=5434, user="root",
                            password="", database="default")
cur = c.cursor()
cur.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
cur.execute("INSERT INTO t VALUES (1, 'alice')")
cur.execute("SELECT * FROM t"); print(cur.fetchall())
```

### 3.3 PostgreSQL wire (default 5435)
```bash
psql -h 127.0.0.1 -p 5435 -U root -d default
```
```python
# psycopg 3
import psycopg
c = psycopg.connect("host=127.0.0.1 port=5435 user=root dbname=default", autocommit=True)
with c.cursor() as cur:
    cur.execute("SELECT id, name FROM t")
    print(cur.fetchall())
```

### 3.4 SQLAlchemy ORM (MySQL or PostgreSQL dialect)
```python
from sqlalchemy import create_engine, text
# MySQL dialect
eng = create_engine("mysql+mysqlconnector://root:@127.0.0.1:5434/default")
# or PostgreSQL dialect
# eng = create_engine("postgresql+psycopg://root:@127.0.0.1:5435/default")
with eng.begin() as conn:
    conn.execute(text("CREATE TABLE u (id INT PRIMARY KEY, age INT)"))
    conn.execute(text("INSERT INTO u VALUES (1, 30)"))
    print(conn.execute(text("SELECT * FROM u")).fetchall())
```
Basic CRUD / JOIN / pagination / reflection / migration (ADD COLUMN) work against real drivers.

### 3.5 HTTP REST (default 6778)
```bash
# KV
curl -X PUT  http://127.0.0.1:6778/v1/kv/user:1 -d 'alice'
curl http://127.0.0.1:6778/v1/kv/user:1
# SQL (JSON)
curl -X POST http://127.0.0.1:6778/v1/sql -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT * FROM t"}'
# monitoring
curl http://127.0.0.1:6778/metrics       # Prometheus metrics
curl http://127.0.0.1:6778/v1/status
```

---

## 4. SQL Capabilities & Examples

### DDL
```sql
CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  email TEXT,
  age INT,
  INDEX(age),                 -- local secondary index
  UNIQUE(email)               -- unique index (this shard); use GLOBAL UNIQUE for cross-shard
);
ALTER TABLE users ADD COLUMN created DATE;   -- append a nullable column (zero data rewrite)
DESCRIBE users;
SHOW CREATE TABLE users;
DROP TABLE users;
```

### DML
```sql
INSERT INTO users (id, name, age) VALUES (1, 'alice', 30), (2, 'bob', 25);
UPDATE users SET age = 31 WHERE id = 1;
DELETE FROM users WHERE age < 18;
```

### SELECT (projection / filter / sort / pagination / alias)
```sql
SELECT id, name AS n FROM users WHERE age >= 18 AND name LIKE 'a%'
  ORDER BY age DESC LIMIT 10 OFFSET 5;          -- or MySQL LIMIT 5,10
SELECT * FROM db1.users;                         -- db.table qualified name
```

### Aggregates / GROUP BY / HAVING / DISTINCT
```sql
SELECT age, COUNT(*), SUM(score), AVG(score) FROM users GROUP BY age HAVING COUNT(*) > 1;
SELECT COUNT(DISTINCT age) FROM users;
SELECT DISTINCT age FROM users;
SELECT SUM(price * qty) FROM orders;             -- expression aggregate
```

### JOIN / subqueries
```sql
SELECT u.name, o.amt FROM users u JOIN orders o ON u.id = o.uid;   -- INNER/LEFT/RIGHT/FULL/CROSS/USING
SELECT * FROM users WHERE id IN (SELECT uid FROM orders);          -- non-correlated IN
SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.uid = u.id);  -- single-equality correlated EXISTS
SELECT * FROM (SELECT uid, SUM(amt) s FROM orders GROUP BY uid) t WHERE t.s > 100;  -- FROM derived table
```

### Transactions
```sql
BEGIN;                                  -- or BEGIN ISOLATION LEVEL SERIALIZABLE
UPDATE users SET age = 40 WHERE id = 1;
SAVEPOINT sp1;
DELETE FROM users WHERE id = 2;
ROLLBACK TO sp1;
COMMIT;
```
OCC isolation levels + SAVEPOINT; single-shard strictly atomic, cross-shard best-effort; DDL rejected inside a transaction.

### System tables
```sql
SELECT * FROM information_schema.tables;
SELECT * FROM information_schema.columns;
SHOW TABLES; SHOW DATABASES;
```

---

## 5. Data Types

| SQL type | Storage | Notes |
|---|---|---|
| `INT/BIGINT/SMALLINT` | i64 | integer |
| `BOOLEAN/BOOL` | i64 (0/1) | boolean; renders `1/0` on MySQL, `t/f` on PG |
| `DOUBLE/FLOAT/REAL` | f64 | floating point |
| `DECIMAL/NUMERIC(p,s)` | i128 fixed-point | **exact money** (precision ≤ 38); SUM exact; drivers return native `Decimal` |
| `TEXT/VARCHAR(n)/CHAR(n)` | variable bytes | string |
| `BLOB/BYTES/BYTEA` | variable bytes | binary |
| `DATE/TIME/TIMESTAMP/DATETIME` | i64 microseconds | time (naive UTC); drivers return native `date`/`datetime` |
| `JSON/JSONB` | text bytes | semi-structured (single row < 64KB) |
| `UUID` | 16B | drivers return native `UUID` |

**Type example:**
```sql
CREATE TABLE account (
  id INT PRIMARY KEY,
  active BOOLEAN,
  balance DECIMAL(18,2),        -- exact money
  created DATE,
  updated TIMESTAMP,
  profile JSON,
  token UUID
);
INSERT INTO account VALUES
  (1, TRUE, '1234.56', DATE '2024-06-01',
   TIMESTAMP '2024-06-01 09:30:00', '{"vip":true}',
   '550e8400-e29b-41d4-a716-446655440000');

SELECT SUM(balance) FROM account;               -- exact (no precision loss)
SELECT id FROM account WHERE created > DATE '2024-01-01' ORDER BY created;
SELECT id FROM account WHERE active = TRUE;
```
> psycopg3 maps the above columns directly to Python `bool` / `Decimal` / `date` / `datetime` / `dict` / `UUID`; mysql-connector (including the prepared binary protocol) returns native `datetime` / `Decimal`.

---

## 6. Security: Auth & TLS

### 6.1 Authentication (password)
Setting `sql_password` enables login auth (shared by the MySQL and PostgreSQL facades):
```toml
[server]
sql_password = "s3cret"
```
- **PostgreSQL facade**: a non-empty password uses **SCRAM-SHA-256** (fully eliminates cleartext passwords).
- **MySQL facade**: supports `caching_sha2_password` fast-auth and `mysql_native_password` (challenge-response, automatic fallback).
- Empty password = no auth (any username allowed); wrong password rejected (MySQL 1045 / PG 28P01).

```python
# psycopg uses SCRAM automatically
psycopg.connect("host=127.0.0.1 port=5435 user=root password=s3cret dbname=default")
# mysql-connector negotiates caching_sha2 / native automatically
mysql.connector.connect(host="127.0.0.1", port=5434, user="root", password="s3cret", database="default")
```

### 6.2 TLS transport encryption (opt-in)
Setting cert + key paths enables TLS (**both must be non-empty**; unset = plaintext, zero overhead):
```toml
[server]
tls_cert = "/etc/nexusdb/cert.pem"   # certificate chain PEM
tls_key  = "/etc/nexusdb/key.pem"    # private key PEM (PKCS8 / PKCS1 / SEC1)
```
Generate a self-signed cert (for testing):
```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout key.pem -out cert.pem -days 3650 \
  -subj "/CN=localhost" -addext "subjectAltName=IP:127.0.0.1,DNS:localhost"
```
Connect (STARTTLS-style: SQL facades upgrade within the handshake, same port):
```python
# PostgreSQL: sslmode=require (encrypt, don't verify the self-signed cert)
psycopg.connect("host=127.0.0.1 port=5435 user=root password=s3cret dbname=default sslmode=require")
# MySQL: enable SSL
mysql.connector.connect(host="127.0.0.1", port=5434, user="root", password="s3cret",
                        database="default", ssl_disabled=False, ssl_verify_cert=False)
```
- Under the hood: rustls 0.23 (ring backend), TLS 1.2/1.3.
- Connections without TLS can still connect in plaintext (opt-in, backward compatible).
- v1 boundary: no client-cert mutual auth; no SCRAM channel binding; single `sql_password` (no per-user account system yet).

---

## 7. Configuration Reference

```toml
[server]
listen_addr = "0.0.0.0:5433"     # Binary internal protocol (bench/test)
redis_addr  = "0.0.0.0:6379"     # RESP; empty = disable
sql_addr    = "0.0.0.0:5434"     # MySQL wire; empty = disable
pg_addr     = "0.0.0.0:5435"     # PostgreSQL wire; empty = disable
http_addr   = "0.0.0.0:6778"     # HTTP REST; empty = disable
sql_password = ""                # SQL login password (empty = no auth; non-empty → PG uses SCRAM)
redis_password = ""              # RESP AUTH password
http_cors_origin = ""            # CORS Allow-Origin ("*"/specific origin)
http_token = ""                  # REST Bearer token (empty = no auth)
tls_cert = ""                    # TLS cert PEM (both must be non-empty to enable TLS)
tls_key  = ""                    # TLS private key PEM
sql_worker_count = 1             # SQL/PG facade worker count (2-8 for concurrent pools)
max_key_bytes = 1024             # key cap
max_value_bytes = 1048576        # value cap (>4KB auto overflow pages)

[storage]
block_root = "./data"            # data directory
default_db = "default"
default_table = "default"
```

---

## 8. Measured Performance

> Environment: release build, loopback, single machine. Numbers are ballpark, not a strict benchmark.

### RESP (Redis facade, 50 concurrency)
| Op | No pipeline | pipeline=16 |
|---|---|---|
| SET | ~119K qps (p50 0.31ms) | ~327K qps |
| GET | ~156K qps (p50 0.16ms) | ~498K qps |

### SQL point ops (single connection, includes driver overhead)
| Mode | point SELECT | point INSERT |
|---|---|---|
| plaintext | ~26K qps (p50 0.036ms) | ~14K qps |
| TLS | ~18K qps (p50 0.051ms) | ~11K qps |

**On TLS overhead**: the table above is a **worst case** — single connection, serial, tiny payloads. Each op takes only tens of microseconds, so the crypto share is amplified and qps appears to drop ~25-30%, but the absolute latency only increases by ~15µs (0.036→0.051ms). In real workloads:
- handshake cost is one-time (amortized to near zero with pooled long-lived connections);
- steady-state symmetric encryption is GB/s-class with AES-NI, so throughput-bound workloads typically see single-digit-percent TLS impact;
- **no TLS = zero overhead** (identical to the plaintext path).

---

## 9. Capability Boundaries

Delivered but with v1 limitations — keep in mind when evaluating/using:

- **Permissions**: single `sql_password`, no per-user user/role/permission system yet.
- **TLS**: opt-in; no client-cert mutual auth, no SCRAM channel binding.
- **Transactions**: single-shard strictly atomic, cross-shard best-effort; DDL rejected inside a transaction.
- **SQL**: LIKE prefix mode only; aggregates lack GROUP_CONCAT/window functions; remaining subquery gaps (correlated scalar, multi-correlated EXISTS, JOIN-side derived tables); ORDER BY full sort (no top-k).
- **Constraints**: plain UNIQUE is best-effort on the local shard; use explicit `GLOBAL UNIQUE` for cross-shard uniqueness.
- **Backup / HA**: no built-in backup/PITR, replication/high-availability yet.
- **JSON**: text storage, single row < 64KB, no JSON path index.
- **Time**: unified naive UTC, no timezone conversion.

> Full gap list: the "SQL gap" section of [`README.md`](../README.md) and [`CHANGELOG.md`](../CHANGELOG.md).
