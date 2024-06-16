use anyhow::Result;
use rocksdb::{DBCompressionType, Options, DB};
use ulid::Ulid;

use super::Store;

/// An implementation of the [`Store`] trait backed by RocksDB
pub struct RocksDBStore {
    db: DB,
}

impl RocksDBStore {
    /// Creates a new RocksDB store at the given location
    pub fn new(path: &str) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(DBCompressionType::Lz4);

        Ok(Self {
            db: DB::open(&opts, path)?,
        })
    }
}

impl Store for RocksDBStore {
    fn add(&mut self, id: Ulid, chunk: Vec<u8>) -> anyhow::Result<()> {
        // important! use `to_be_bytes()` to maintain sort order!
        self.db.put(id.0.to_be_bytes(), chunk)?;
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        // nothing to do here
        Ok(())
    }

    fn get(&self, id: Ulid) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(id.0.to_be_bytes())?)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::Store;

    use super::RocksDBStore;

    use tempdir::TempDir;
    use ulid::Ulid;

    #[test]
    fn add_and_get() {
        let dir = TempDir::new("georocket_rocksdb").unwrap();

        let mut store = RocksDBStore::new(dir.path().to_str().unwrap()).unwrap();

        let id = Ulid::new();
        let chunk = b"hello world".to_vec();

        store.add(id, chunk.clone()).unwrap();
        store.commit().unwrap();

        let new_chunk = store.get(id).unwrap();

        assert_eq!(new_chunk, Some(chunk));
    }
}
