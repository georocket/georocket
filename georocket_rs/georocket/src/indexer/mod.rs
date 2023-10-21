//! The indexer takes [`Chunk`s](crate::types::Chunk) from a channel and processes them into
//! [`Index`es](crate::types::Index).
//! A [`MainIndexer`] receives chunks from a [`Splitter`](crate::input::splitter::Splitter) and
//! spins up concrete indexers for type of index that needs to be created.
//! These are based on the [`InnerChunk`](crate::types::InnerChunk) type. of the received chunk.

use crate::types::IndexElement::BoundingBoxIndex;
use crate::types::{Chunk, GeoJsonChunk, Index, InnerChunk};
use async_channel::Receiver;
use tokio::sync::mpsc::Sender;

mod geo_json_indexer;
use geo_json_indexer::GeoJsonIndexer;

///
pub struct MainIndexer {
    chunk_rec: Receiver<Chunk>,
    index_send: Sender<Index>,
}

impl MainIndexer {
    /// Create a new `MainIndexer` with the provided channels.
    pub fn new(chunk_rec: Receiver<Chunk>, index_send: Sender<Index>) -> Self {
        Self {
            chunk_rec,
            index_send,
        }
    }
    /// Run the `MainIndexer` until the channel is closed.
    pub async fn run(mut self) {
        while let Ok(chunk) = self.chunk_rec.recv().await {
            self.process_chunk(chunk);
        }
    }
    /// Process a single chunk into an index.
    fn process_chunk(&mut self, chunk: Chunk) -> Index {
        let Chunk { id, inner } = chunk;
        let index_elements = match inner {
            InnerChunk::GeoJson(chunk) => GeoJsonIndexer::new(chunk).generate_index(),
        };
        Index { id, index_elements }
    }
}
