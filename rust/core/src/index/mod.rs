use anyhow::Result;
use std::collections::HashMap;
use ulid::Ulid;

use crate::query::Query;

pub use self::value::Value;

pub mod gml;
pub mod tantivy;
pub mod value;

/// A value created by an [`Indexer`]
#[derive(Debug, PartialEq)]
pub enum IndexedValue {
    GenericAttributes(HashMap<String, Value>),
}

/// Indexes chunks
pub trait Indexer<E> {
    /// Will be called on every stream `event` in the chunk
    fn on_event(&mut self, event: &E) -> Result<()>;

    /// Will be called when the whole chunk has been passed to the indexer.
    /// Returns the results that should be added to the index or an empty
    /// map if nothing should be added.
    fn make_result(&mut self) -> Vec<IndexedValue>;
}

/// An index store information about chunks in a GeoRocket store and can be
/// used to perform queries.
pub trait Index {
    /// Add the results of indexing a chunk with the given `id` to the index
    async fn add(&self, id: Ulid, indexer_result: Vec<IndexedValue>) -> Result<()>;

    /// Persists changes
    async fn commit(&mut self) -> Result<()>;

    /// Queries the index and returns a list of chunk IDs matching the query
    async fn search(&self, query: Query) -> Result<Vec<Ulid>>;
}
