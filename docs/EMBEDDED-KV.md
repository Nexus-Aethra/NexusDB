# Embedded KV API

`nexusdb` can be used as a Rust library without starting any TCP listener. The
embedded API opens the same shard manager, storage format, and WAL as the
standalone server; it is suitable for applications that want an in-process KV
store and do not need RESP, SQL, or HTTP access.

## Add the dependency

During local development, depend on the repository directly:

```toml
[dependencies]
nexusdb = { path = "../NexusDB" }
```

The public entry points are `NexusDb`, `EmbeddedOptions`, `EmbeddedIoBackend`,
`Database`, `Table`, `TypedValue`, `TypedEntry`, `EmbeddedError`, and
`EmbeddedResult`.

## Open, select, and use a table

Create databases and persistent tables explicitly. `ensure_database` is
idempotent. `create_table` creates the table on every shard, so call it when
provisioning a new table; after reopening, select that existing table with
`Database::table`.

```rust
use nexusdb::{EmbeddedOptions, NexusDb};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = NexusDb::open(EmbeddedOptions::new("./app-data"))?;

    // Provision once. Reuse `database("app")?` and `table("cache")` later.
    let app = db.ensure_database("app")?;
    let cache = app.create_table("cache")?;

    cache.set(b"user:42", b"Ada")?;
    assert_eq!(cache.get(b"user:42")?, Some(b"Ada".to_vec()));
    assert!(cache.del(b"user:42")?);

    // Database and Table own shard-manager handles. Release them before close.
    drop(cache);
    drop(app);
    db.close()?;
    Ok(())
}
```

`set`, `get`, and `del` accept arbitrary byte slices. `get` returns
`Result<Option<Vec<u8>>, EmbeddedError>` and `del` reports whether the key
existed. The API owns NexusDB's internal value tag, so callers always receive
the original application bytes.

## Async and batched calls

The async methods are runtime-agnostic futures: use them from Tokio, async-std,
or any other executor. They are `set_async`, `get_async`, `del_async`, plus
batched variants `set_many_async`, `get_many_async`, and the type-aware
`get_many_typed_async`.

```rust
async fn update(cache: &nexusdb::Table) -> nexusdb::EmbeddedResult<()> {
    cache.set_async(b"state", b"ready").await?;
    assert_eq!(cache.get_async(b"state").await?, Some(b"ready".to_vec()));
    Ok(())
}
```

For one table, `set_many` groups writes by shard internally, and `get_many`
preserves input order. Both return one result per input item, allowing callers
to identify individual failures.

```rust
let writes = cache.set_many(&[(b"a", b"1"), (b"b", b"2")]);
assert!(writes.into_iter().all(|result| result.is_ok()));

let values = cache.get_many(&[b"b", b"missing", b"a"]);
assert_eq!(values[0].as_ref().unwrap(), &Some(b"2".to_vec()));
assert_eq!(values[1].as_ref().unwrap(), &None);
```

## Listing and range scans

The embedded API exposes BTree-ordered scans at the storage boundary through
`Table`. Scans only touch the String keyspace — they do not return Hash fields,
Set members, List indices, or ZSet members, so a single `Table` may safely mix
plain keys with composite structures created via other interfaces (currently
RESP). All scan methods return keys (or `Vec<TypedEntry>` for the typed
variants) in BTree byte order.

| Method | What it returns |
|---|---|
| `list()` | All keys, ascending |
| `list_prefix(p)` | Keys with user-key byte prefix `p` |
| `list_limit(n)` | First `n` keys (`n = 0` means unlimited) |
| `list_range(start, end, limit)` | Closed-open `[start, end)` in BTree order |
| `list_range_prefix(start, end, prefix, limit)` | Range + prefix filter |
| `list_typed()`, `list_typed_limit(n)`, `list_typed_range(...)`, etc. | Same shape but returns `(key, TypedValue)` pairs |
| `get_typed(key)` | Type-aware single-point read |

Every method has an `*_async` counterpart (`list_async`, `list_range_async`,
`list_typed_range_async`, …). The async variants are runtime-agnostic futures
built on `std::future::Future`.

```rust
use nexusdb::{EmbeddedOptions, NexusDb, TypedValue};

let db = NexusDb::open(EmbeddedOptions::new("./app-data"))?;
let app = db.ensure_database("users")?;
let users = app.create_table("name_to_id")?;

// "List a group of names and look up each id" — one round-trip
users.set_many(&[(b"alice", b"1001"), (b"bob", b"1002"), (b"carol", b"1003")])?;
for (name, typed) in users.list_typed()? {
    let id = typed.as_bytes().unwrap();
    println!("{name:?} -> {id:?}");
}

// Range scan over the BTree-ordered key space
let in_window = users.list_range(b"bob", b"dave", 0)?;
assert_eq!(in_window, vec![b"bob".to_vec(), b"carol".to_vec()]);

// Pagination
let page = users.list_range(b"a", b"z", 2)?;
```

### Range semantics

- **`start` empty**: scan from the first key
- **`end` empty**: scan to the last key
- **`start == end`**: empty range (closed-open means the end is exclusive)
- **`start` does not exist**: BTree order still applies — e.g. `[bb, d)` over
  `[a, b, c, d, e]` returns `[c]`
- **Cross-shard correctness**: shards report their local slice in BTree order,
  the manager concatenates and sorts the global result, and a `HashSet` removes
  any duplicate that a routing change might surface
- **Composite structures are not enumerated**: only the `[S][klen][user_key]`
  prefix is walked, so the same `Table` can host a hash alongside plain keys
  without the scan returning hash fields

## Type-aware reads

Internally NexusDB stores every value as `[tag u8][payload]`, with the tag
distinguishing `TAG_RAW` (0x01), `TAG_I64` (0x02), `TAG_F64` (0x03),
`TAG_STR` (0x04), `TAG_DOC` (0x05), and `TAG_F32` (0x06). The single-key
`get` and the bulk `get_many` strip the tag for backwards compatibility.
The type-aware variants return the tag interpretation as a strongly-typed
`TypedValue`:

```rust
pub enum TypedValue {
    Raw(Vec<u8>),    // TAG_RAW
    Int(i64),        // TAG_I64
    Float(f64),      // TAG_F64
    Float32(f32),    // TAG_F32
    Str(Vec<u8>),    // TAG_STR (UTF-8 validated)
    Doc(Vec<u8>),    // TAG_DOC (opaque payload)
    Unknown { tag: u8, raw_bytes: Vec<u8> },  // unknown tag or length error
}

impl TypedValue {
    pub fn as_i64(&self) -> Option<i64>;       // strong-typed unwrap
    pub fn as_f64(&self) -> Option<f64>;       // Float + Float32 both
    pub fn as_bytes(&self) -> Option<&[u8]>;    // Raw + Str
    pub fn type_name(&self) -> &'static str;   // "raw" / "int" / ...
    pub fn raw_bytes(&self) -> &[u8];          // stored bytes incl. tag
}
```

A table may hold mixed types in practice (e.g. write `set("counter", b"0")`
and then promote it with `INCR` through the network layer — but for an
embedded-only workflow, `set` always writes `TAG_RAW`). `list_typed*` returns
the actual stored interpretation per key.

## Lifecycle and durability

- Call `flush()` to synchronously flush all shards without closing the engine.
- Call `close(self)` for a graceful shutdown and WAL finalization. It returns
  `EmbeddedError::ActiveHandles` if any `Database` or `Table` clone remains.
- Reopen the same `data_dir` with the same shard count to recover persisted
  data. Treat the data directory as exclusive to one embedded process.
- `EmbeddedOptions` defaults to one shard, chunk-cache size four, and the
  storage crate's default WAL mode. Set `num_shards`, `chunk_cache_size`, and
  `wal_mode` before calling `open` when the application needs different
  durability or parallelism trade-offs.

## I/O backend

`EmbeddedOptions::io_backend` selects the persistence backend. It defaults to
`EmbeddedIoBackend::StdFs`, the portable choice. On a supported Linux host,
select `IoUring` to use NexusDB's asynchronous io_uring path:

```rust
use nexusdb::{EmbeddedIoBackend, EmbeddedOptions, NexusDb};

let mut options = EmbeddedOptions::new("./app-data");
options.num_shards = 4;
options.chunk_cache_size = 32;
options.io_backend = EmbeddedIoBackend::IoUring;
let db = NexusDb::open(options)?;
```

`IoUring` requires an available Linux io_uring runtime. Use `StdFs` for
Windows, unsupported kernels, restricted containers, and predictable portable
deployment.

## Windows

The embedded library currently compiles for `x86_64-pc-windows-msvc` and uses
the portable storage path (`StdFs` rather than Linux `io_uring`). Verify it in
a Windows toolchain with:

```powershell
cargo check --target x86_64-pc-windows-msvc --lib
```

This is a compile/link compatibility check. The embedded API has not yet had a
native Windows read/write smoke test, so production Windows adoption should
include an application-level persistence test.
