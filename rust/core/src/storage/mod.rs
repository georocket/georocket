pub mod chunk_meta;
pub mod rocksdb;

use anyhow::Result;
use ulid::Ulid;

/// A store for chunks
pub trait Store {
    /// Add a chunk with given ID to the store. Depending on the actual
    /// implementation, this operation might be asynchronous. Call [`commit`]
    /// to wait for all operations to finish.
    fn add(&mut self, id: Ulid, chunk: Vec<u8>) -> Result<()>;

    /// Call this method after adding one or more chunks via [`add`]
    fn commit(&mut self) -> Result<()>;

    /// Retrieve a chunk by ID from the store
    fn get(&self, id: Ulid) -> Result<Option<Vec<u8>>>;
}
