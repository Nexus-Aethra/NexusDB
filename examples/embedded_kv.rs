use nexusdb::{EmbeddedIoBackend, EmbeddedOptions, NexusDb};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut options = EmbeddedOptions::new("./embedded-data");
    // Portable default; use IoUring on a supported Linux host if desired.
    options.io_backend = EmbeddedIoBackend::StdFs;
    let db = NexusDb::open(options)?;
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
