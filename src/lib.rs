//! Embedded NexusDB API.
//!
//! This library target starts the same shard/storage engine as the standalone
//! server, but exposes it directly instead of opening network listeners.

use std::path::PathBuf;
use std::sync::Arc;

use shard_manager::{ShardError, ShardManager, ShardManagerOptions};

pub type EmbeddedResult<T> = Result<T, EmbeddedError>;

/// Storage I/O backend for an embedded instance.
///
/// `IoUring` is available on supported Linux kernels. `StdFs` is portable and
/// remains the default, including on Windows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EmbeddedIoBackend {
    #[default]
    StdFs,
    IoUring,
}

impl EmbeddedIoBackend {
    fn to_storage(self) -> storage::IoBackend {
        match self {
            Self::StdFs => storage::IoBackend::StdFs,
            Self::IoUring => storage::IoBackend::IoUring,
        }
    }
}

/// Errors exposed by the embedded API.
#[derive(Debug, thiserror::Error)]
pub enum EmbeddedError {
    #[error(transparent)]
    Engine(#[from] ShardError),
    #[error("database not found: {0}")]
    DatabaseNotFound(String),
    #[error("cannot close NexusDb while Database or Table handles are still alive")]
    ActiveHandles,
}

/// Configuration for an embedded database instance.
#[derive(Debug, Clone)]
pub struct EmbeddedOptions {
    pub data_dir: PathBuf,
    /// One independent storage/scheduler thread is created per shard.
    pub num_shards: usize,
    pub chunk_cache_size: usize,
    pub wal_mode: storage::wal::WalMode,
    pub io_backend: EmbeddedIoBackend,
}

impl EmbeddedOptions {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            ..Self::default()
        }
    }
}

impl Default for EmbeddedOptions {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./nexusdb-data"),
            num_shards: 1,
            chunk_cache_size: 4,
            wal_mode: storage::wal::WalMode::default(),
            io_backend: EmbeddedIoBackend::default(),
        }
    }
}

/// An owned embedded engine.  Drop all selected [`Database`] and [`Table`]
/// handles before calling [`NexusDb::close`].
pub struct NexusDb {
    manager: Arc<ShardManager>,
}

/// A selected database namespace.
#[derive(Clone)]
pub struct Database {
    manager: Arc<ShardManager>,
    name: Arc<str>,
}

/// A selected KV table namespace.
#[derive(Clone)]
pub struct Table {
    manager: Arc<ShardManager>,
    database: Arc<str>,
    name: Arc<str>,
}

impl NexusDb {
    /// Opens the shard engine without starting any network listener.
    pub fn open(options: EmbeddedOptions) -> EmbeddedResult<Self> {
        let mut manager_options = ShardManagerOptions::new(options.num_shards, options.data_dir);
        manager_options.chunk_cache_size = options.chunk_cache_size;
        manager_options.wal_mode = options.wal_mode;
        manager_options.io_backend = options.io_backend.to_storage();
        manager_options.io_config = storage::IoBackendConfig::from(manager_options.io_backend);
        let manager = Arc::new(ShardManager::open(manager_options)?);
        Ok(Self { manager })
    }

    /// Selects a database. It must already exist; use [`Self::create_database`]
    /// first when creating a new namespace.
    pub fn database(&self, name: impl Into<Arc<str>>) -> EmbeddedResult<Database> {
        let name = name.into();
        if self.manager.db_view().id_of(&name).is_none() {
            return Err(EmbeddedError::DatabaseNotFound(name.to_string()));
        }
        Ok(Database {
            manager: self.manager.clone(),
            name,
        })
    }

    /// Creates a database across all shards, then returns its selected handle.
    pub fn create_database(&self, name: impl Into<Arc<str>>) -> EmbeddedResult<Database> {
        let name = name.into();
        self.manager.create_db(&name)?;
        self.database(name)
    }

    /// Creates the database only when it is absent, returning a selected
    /// handle in either case.
    pub fn ensure_database(&self, name: impl Into<Arc<str>>) -> EmbeddedResult<Database> {
        let name = name.into();
        if self.manager.db_view().id_of(&name).is_none() {
            self.manager.create_db(&name)?;
        }
        self.database(name)
    }

    /// Flushes all shards without closing the instance.
    pub fn flush(&self) -> EmbeddedResult<()> {
        Ok(self.manager.flush_all()?)
    }

    /// Performs a graceful shard shutdown.  This consumes the engine so a
    /// caller cannot accidentally issue an operation after close.
    pub fn close(self) -> EmbeddedResult<()> {
        Arc::try_unwrap(self.manager)
            .map_err(|_| EmbeddedError::ActiveHandles)?
            .close()?;
        Ok(())
    }
}

impl Database {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Selects a table.  KV writes create its local table storage lazily.
    pub fn table(&self, name: impl Into<Arc<str>>) -> Table {
        Table {
            manager: self.manager.clone(),
            database: self.name.clone(),
            name: name.into(),
        }
    }

    /// Creates a table across all shards, then returns its selected handle.
    pub fn create_table(&self, name: impl Into<Arc<str>>) -> EmbeddedResult<Table> {
        let name = name.into();
        self.manager.create_table(&self.name, &name)?;
        Ok(self.table(name))
    }
}

impl Table {
    pub fn database_name(&self) -> &str {
        &self.database
    }
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Stores raw application bytes.  The embedded API owns the internal value
    /// type tag so values interoperate with RESP/Binary callers.
    pub fn set(&self, key: &[u8], value: &[u8]) -> EmbeddedResult<()> {
        self.manager
            .put(&self.database, &self.name, key, &tag_raw(value), 0)?;
        Ok(())
    }

    /// Returns raw application bytes; NexusDB's internal type tag is hidden.
    pub fn get(&self, key: &[u8]) -> EmbeddedResult<Option<Vec<u8>>> {
        Ok(self
            .manager
            .get(&self.database, &self.name, key, 0)
            .map(|value| value.map(strip_tag))?)
    }

    /// Deletes a key and reports whether it existed.
    pub fn del(&self, key: &[u8]) -> EmbeddedResult<bool> {
        Ok(self.manager.delete(&self.database, &self.name, key, 0)?)
    }

    pub async fn set_async(&self, key: &[u8], value: &[u8]) -> EmbeddedResult<()> {
        let tagged = tag_raw(value);
        Ok(self
            .manager
            .put_async(&self.database, &self.name, key, &tagged, 0)?
            .await?)
    }

    pub async fn get_async(&self, key: &[u8]) -> EmbeddedResult<Option<Vec<u8>>> {
        Ok(self
            .manager
            .get_async(&self.database, &self.name, key, 0)?
            .await
            .map(|value| value.map(strip_tag))?)
    }

    pub async fn del_async(&self, key: &[u8]) -> EmbeddedResult<bool> {
        Ok(self
            .manager
            .delete_async(&self.database, &self.name, key, 0)?
            .await?)
    }

    /// Same-table multi-set.  Operations are grouped by shard internally.
    pub fn set_many(&self, entries: &[(&[u8], &[u8])]) -> Vec<EmbeddedResult<()>> {
        let tagged: Vec<_> = entries
            .iter()
            .map(|(key, value)| (*key, tag_raw(value)))
            .collect();
        let refs: Vec<_> = tagged
            .iter()
            .map(|(key, value)| (*key, value.as_slice()))
            .collect();
        self.manager
            .batch_put(&self.database, &self.name, &refs)
            .into_iter()
            .map(|result| result.map_err(EmbeddedError::from))
            .collect()
    }

    /// Same-table multi-get.  Results retain input order.
    pub fn get_many(&self, keys: &[&[u8]]) -> Vec<EmbeddedResult<Option<Vec<u8>>>> {
        self.manager
            .batch_get(&self.database, &self.name, keys)
            .into_iter()
            .map(|result| {
                result
                    .map(|value| value.map(strip_tag))
                    .map_err(EmbeddedError::from)
            })
            .collect()
    }
}

fn tag_raw(value: &[u8]) -> Vec<u8> {
    let mut tagged = Vec::with_capacity(value.len() + 1);
    tagged.push(shard_manager::value_num::TAG_RAW);
    tagged.extend_from_slice(value);
    tagged
}

fn strip_tag(value: Vec<u8>) -> Vec<u8> {
    match value.first() {
        Some(tag) if shard_manager::value_num::is_known_tag(*tag) => value[1..].to_vec(),
        _ => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_backend_selection_maps_to_storage_backend() {
        assert_eq!(
            EmbeddedIoBackend::StdFs.to_storage(),
            storage::IoBackend::StdFs
        );
        assert_eq!(
            EmbeddedIoBackend::IoUring.to_storage(),
            storage::IoBackend::IoUring
        );
    }

    #[test]
    fn sync_and_async_kv_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let mut options = EmbeddedOptions::new(temp.path());
        options.num_shards = 2;
        let db = NexusDb::open(options).unwrap();
        let database = db.create_database("app").unwrap();
        let table = database.create_table("cache").unwrap();
        table.set(b"sync", b"value").unwrap();
        assert_eq!(table.get(b"sync").unwrap(), Some(b"value".to_vec()));
        assert!(table.del(b"sync").unwrap());
        pollster::block_on(table.set_async(b"async", b"value")).unwrap();
        assert_eq!(
            pollster::block_on(table.get_async(b"async")).unwrap(),
            Some(b"value".to_vec())
        );
        assert!(matches!(
            db.database("missing"),
            Err(EmbeddedError::DatabaseNotFound(_))
        ));
        let results = table.set_many(&[(b"one", b"1"), (b"two", b"2")]);
        assert!(results.into_iter().all(|result| result.is_ok()));
        let values = table.get_many(&[b"two", b"missing", b"one"]);
        assert_eq!(values[0].as_ref().unwrap(), &Some(b"2".to_vec()));
        assert_eq!(values[1].as_ref().unwrap(), &None);
        assert_eq!(values[2].as_ref().unwrap(), &Some(b"1".to_vec()));
        drop(table);
        drop(database);
        db.close().unwrap();
    }

    #[test]
    fn close_then_reopen_recovers_raw_kv() {
        let temp = tempfile::tempdir().unwrap();
        let options = EmbeddedOptions::new(temp.path());
        let db = NexusDb::open(options.clone()).unwrap();
        let app = db.create_database("app").unwrap();
        let table = app.create_table("cache").unwrap();
        table.set(b"persisted", b"value").unwrap();
        db.flush().unwrap();
        drop(table);
        drop(app);
        db.close().unwrap();

        let reopened = NexusDb::open(options).unwrap();
        let app = reopened.database("app").unwrap();
        assert_eq!(
            app.table("cache").get(b"persisted").unwrap(),
            Some(b"value".to_vec())
        );
        drop(app);
        reopened.close().unwrap();
    }
}
