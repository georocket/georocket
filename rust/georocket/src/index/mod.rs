use anyhow::Result;
use std::collections::HashMap;
use ulid::Ulid;

pub use self::value::Value;

pub mod gml;
pub mod tantivy;
pub mod value;

/// Indexes chunks
pub trait Indexer<E> {
    /// Will be called on every stream `event` in the chunk
    fn on_event(&mut self, event: &E) -> Result<()>;

    /// Will be called when the whole chunk has been passed to the indexer.
    /// Returns the results that should be added to the index or an empty
    /// map if nothing should be added.
    /// TODO keys should be from an enum so we have a strict schema
    fn make_result(&mut self) -> HashMap<String, Value>;
}

/// An index store information about chunks in a GeoRocket store and can be
/// used to perform queries.
pub trait Index {
    /// Add the results of indexing a chunk with the given `id` to the index
    async fn add(&self, id: Ulid, indexer_result: HashMap<String, Value>) -> Result<()>;

    /// Persists changes
    async fn commit(&mut self) -> Result<()>;
}
