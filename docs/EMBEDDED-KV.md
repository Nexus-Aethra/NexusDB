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

The public entry points are `NexusDb`, `EmbeddedOptions`, `Database`, `Table`,
`EmbeddedError`, and `EmbeddedResult`.

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
or any other executor. They are `set_async`, `get_async`, and `del_async`.

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
