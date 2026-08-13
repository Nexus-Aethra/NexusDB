//! Embedded scan example: name → id mapping with type-aware values.
//!
//! Demonstrates `Table::list_typed` for the "list a group of names and look up
//! each id" pattern — single round-trip, returns strongly-typed values. Also
//! shows range scanning `[start, end)` for windowed queries (e.g. "names
//! starting with id 1000..2000").

use nexusdb::{EmbeddedOptions, NexusDb, TypedValue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempfile::tempdir()?;
    let db = NexusDb::open(EmbeddedOptions::new(tmp.path()))?;
    let app = db.create_database("users")?;
    let users = app.create_table("name_to_id")?;

    // 1. Write some raw mappings.
    users.set(b"alice", b"1001")?;
    users.set(b"bob", b"1002")?;
    users.set(b"carol", b"1003")?;

    // 2. list() — pure keys, no value.
    let keys = users.list()?;
    println!("all keys: {:?}", keys);
    assert_eq!(keys, vec![b"alice".to_vec(), b"bob".to_vec(), b"carol".to_vec()]);

    // 3. list_prefix() — keys starting with a prefix.
    let b_pref = users.list_prefix(b"b")?;
    println!("'b' prefix: {:?}", b_pref);
    assert_eq!(b_pref, vec![b"bob".to_vec()]);

    // 4. list_typed() — strongly-typed values, single round-trip.
    for (name, typed) in users.list_typed()? {
        let id_str = match typed {
            TypedValue::Raw(v) => std::str::from_utf8(&v)?.to_owned(),
            other => panic!("unexpected type: {other:?}"),
        };
        println!("{name:?} -> {id_str}");
    }

    // 5. list_range() — closed-open [start, end) over a BTree-ordered key space.
    //    Useful for "all keys in this window" (e.g. by id, by timestamp).
    let range = users.list_range(b"bob", b"dave", 0)?;
    println!("[bob, dave): {:?}", range);
    assert_eq!(range, vec![b"bob".to_vec(), b"carol".to_vec()]);

    // 6. Range + limit — paginated scan.
    let first2 = users.list_range(b"a", b"z", 2)?;
    println!("[a, z) first 2: {:?}", first2);
    assert_eq!(first2, vec![b"alice".to_vec(), b"bob".to_vec()]);

    drop(users);
    drop(app);
    db.close()?;
    Ok(())
}
