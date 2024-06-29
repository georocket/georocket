use anyhow::Result;

use crate::index::chunk_meta::ChunkMeta;

pub mod xml;

/// Merges chunks
pub trait Merger<E> {
    /// Merge a chunk with the given metadata object
    fn merge(&mut self, chunk: &[u8], meta: ChunkMeta) -> Result<(), E>;

    /// Finish merging
    fn finish(&mut self) -> Result<()>;
}
