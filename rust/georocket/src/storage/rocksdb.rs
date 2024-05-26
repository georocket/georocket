use std::sync::Arc;

use anyhow::Result;
use rocksdb::{DBCompressionType, Options, DB};
use tokio::task::{self, JoinHandle};
use ulid::Ulid;

use super::Store;

/// An implementation of the [`Store`] trait backed by RocksDB
pub struct RocksDBStore {
    db: Arc<DB>,
    last_join_handle: Option<JoinHandle<Result<()>>>,
}

impl RocksDBStore {
    /// Creates a new RocksDB store at the given location
    pub fn new(path: &str) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(DBCompressionType::Lz4);

        Ok(Self {
            db: Arc::new(DB::open(&opts, path)?),
            last_join_handle: None,
        })
    }
}

impl Store for RocksDBStore {
    async fn add(&mut self, id: Ulid, chunk: Vec<u8>) -> anyhow::Result<()> {
        self.commit().await?;

        let db = Arc::clone(&self.db);
        self.last_join_handle = Some(task::spawn_blocking(move || {
            // important! use `to_be_bytes()` to maintain sort order!
            db.put(id.0.to_be_bytes(), chunk)?;
            Ok(())
        }));

        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        if let Some(h) = self.last_join_handle.take() {
            h.await?
        } else {
            Ok(())
        }
    }
}
