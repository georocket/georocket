use anyhow::Result;
use std::collections::HashMap;

pub use self::value::Value;

pub mod gml;
pub mod value;

/// Indexes chunks
pub trait Indexer<E> {
    /// Will be called on every stream `event` in the chunk
    fn on_event(&mut self, event: &E) -> Result<()>;

    /// Will be called when the whole chunk has been passed to the indexer.
    /// Returns the results that should be added to the index or an empty
    /// map if nothing should be added.
    fn make_result(&mut self) -> HashMap<String, Value>;
}
