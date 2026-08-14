//! Async scan example: 同 embedded_scan 但全部走 async API.
//!
//! 用 `pollster::block_on` 单线程跑 (本示例), 真业务里换成 tokio / async-std
//! runtime 即可, 内部用的是 std `impl Future`, 不绑定 runtime.

use nexusdb::{EmbeddedOptions, NexusDb, TypedValue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempfile::tempdir()?;
    let db = NexusDb::open(EmbeddedOptions::new(tmp.path()))?;
    let app = db.ensure_database("users")?;
    let users = app.create_table("name_to_id")?;

    // 1. 批量写 (async)
    let entries: &[(&[u8], &[u8])] = &[
        (b"alice", b"1001"),
        (b"bob", b"1002"),
        (b"carol", b"1003"),
        (b"dave", b"1004"),
    ];
    let results = pollster::block_on(users.set_many_async(entries));
    assert!(results.iter().all(|r| r.is_ok()));

    // 2. 批量读 (async)
    let probe_keys: &[&[u8]] = &[b"alice", b"bob", b"missing"];
    let read_back = pollster::block_on(users.get_many_async(probe_keys));
    for (k, v) in probe_keys.iter().zip(read_back.iter()) {
        println!("{k:?} -> {:?}", v);
    }

    // 3. 类型感知批量读
    let typed_keys: &[&[u8]] = &[b"alice", b"carol"];
    let typed_many = pollster::block_on(users.get_many_typed_async(typed_keys));
    for (k, v) in typed_keys.iter().zip(typed_many.iter()) {
        match v {
            Ok(Some(TypedValue::Raw(bytes))) => {
                println!("{k:?} (raw) = {}", std::str::from_utf8(bytes)?)
            }
            other => println!("{k:?} = {other:?}"),
        }
    }

    // 4. 列全部 (async)
    let all = pollster::block_on(users.list_async())?;
    println!("all keys: {:?}", all);

    // 5. 范围闭开 (async)
    let range = pollster::block_on(users.list_range_async(b"bob", b"dave", 0))?;
    println!("[bob, dave): {:?}", range);

    // 6. 类型感知范围扫描 (async)
    let typed_range =
        pollster::block_on(users.list_typed_range_async(b"alice", b"dave", 0))?;
    for (k, v) in &typed_range {
        println!("range {k:?} -> {v:?}");
    }

    drop(users);
    drop(app);
    db.close()?;
    Ok(())
}
