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
    fn on_event(&mut self, event: E) -> Result<()>;
}

/// An index store information about chunks in a GeoRocket store and can be
/// used to perform queries.
pub trait Index {
    /// Add the results of indexing a chunk with the given `id` to the index
    fn add(&self, id: Ulid, indexer_result: Vec<IndexedValue>) -> Result<()>;

    /// Persists changes
    fn commit(&mut self) -> Result<()>;

    /// Queries the index and returns a list of chunk IDs matching the query
    fn search(&self, query: Query) -> Result<Vec<Ulid>>;
}
