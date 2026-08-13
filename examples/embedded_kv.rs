use nexusdb::{EmbeddedOptions, NexusDb};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = NexusDb::open(EmbeddedOptions::new("./embedded-data"))?;
    let app = db.ensure_database("app")?;
    let cache = app.create_table("cache")?;

    cache.set(b"greeting", b"hello")?;
    assert_eq!(cache.get(b"greeting")?, Some(b"hello".to_vec()));
    assert!(cache.del(b"greeting")?);

    drop(cache);
    drop(app);
    db.close()?;
    Ok(())
}
