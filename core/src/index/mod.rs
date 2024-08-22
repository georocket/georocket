use anyhow::Result;
use chunk_meta::ChunkMeta;
use geo::Rect;
use ulid::Ulid;

use crate::query::Query;

pub use self::value::Value;

pub mod chunk_meta;
pub mod gml;
pub mod h3_term_index;
pub mod tantivy;
pub mod value;

/// A value created by an [`Indexer`]
#[derive(Debug, PartialEq)]
pub enum IndexedValue {
    GenericAttributes(Vec<(String, Value)>),
    BoundingBox(Rect),
}

/// Indexes chunks
pub trait Indexer<E> {
    /// Will be called on every stream `event` in the chunk
    fn on_event(&mut self, event: E) -> Result<()>;
}

/// An index stores information about chunks in a GeoRocket store and can be
/// used to perform queries.
pub trait Index {
    /// Adds the results of indexing a chunk to the index
    fn add(&self, meta: ChunkMeta, indexer_result: Vec<IndexedValue>) -> Result<()>;

    /// Deletes the chunk with the given `id` from the index
    fn delete(&self, id: Ulid) -> Result<()>;

    /// Persists changes
    fn commit(&mut self) -> Result<()>;

    /// Queries the index and returns an iterator over the metadata objects of
    /// all chunks matching the given query
    fn search(&self, query: Query) -> Result<impl Iterator<Item = Result<ChunkMeta>>>;
}
