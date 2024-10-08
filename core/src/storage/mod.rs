pub mod rocksdb;

use anyhow::Result;
use ulid::Ulid;

/// A store for chunks
pub trait Store {
    /// Add a chunk with given ID to the store. Depending on the actual
    /// implementation, this operation might be asynchronous. Call
    /// [`commit`](Self::commit) to wait for all operations to finish.
    fn add(&mut self, id: Ulid, chunk: Vec<u8>) -> Result<()>;

    /// Delete the chunk with the given ID from the store. Depending on the
    /// actual implementation, this operation might be asynchronous. Call
    /// [`commit`](Self::commit) to wait for all operations to finish.
    fn delete(&mut self, id: Ulid) -> Result<()>;

    /// Call this method after adding one or more chunks via [`add`](Self::add)
    fn commit(&mut self) -> Result<()>;

    /// Retrieve a chunk by ID from the store
    fn get(&self, id: Ulid) -> Result<Option<Vec<u8>>>;
}
