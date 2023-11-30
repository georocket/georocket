//! The indexer takes [`Chunk`s](crate::types::Chunk) from a channel and processes them into
//! [`Index`es](crate::types::Index).
//! A [`MainIndexer`] receives chunks from a [`Splitter`](crate::input::splitter::Splitter) and
//! spins up concrete indexers for type of index that needs to be created.
//! These are based on the [`InnerChunk`](crate::types::InnerChunk) type. of the received chunk.

use crate::types::{Chunk, Index, InnerChunk};
use anyhow::Context;
use async_channel::Receiver;
use tokio::sync::mpsc;
use tracing::{event, instrument, Level};

mod geo_json_indexer;
use geo_json_indexer::GeoJsonIndexer;

#[derive(Debug)]
pub struct MainIndexer {
    chunk_rec: Receiver<Chunk>,
    index_send: mpsc::Sender<Index>,
}

impl MainIndexer {
    /// Create a new `MainIndexer` with the provided channels.
    pub fn new(chunk_rec: Receiver<Chunk>, index_send: mpsc::Sender<Index>) -> Self {
        Self {
            chunk_rec,
            index_send,
        }
    }

    /// Create a new `MainIndexer` along with the a c receiver channel for the generated `Index`es.
    /// `capacity` specifies the capacity of the created channel.
    pub fn new_with_index_receiver(
        capacity: usize,
        chunk_rec: Receiver<Chunk>,
    ) -> (Self, mpsc::Receiver<Index>) {
        let (index_send, index_rec) = mpsc::channel(capacity);
        (Self::new(chunk_rec, index_send), index_rec)
    }
    /// Run the `MainIndexer` until the channel is closed.
    /// Returns the amount of `Chunk`s that have been processed.
    pub async fn run(mut self) -> anyhow::Result<usize> {
        let mut count = 0;
        while let Ok(chunk) = self.chunk_rec.recv().await {
            let index = self.process_chunk(chunk);
            self.index_send
                .send(index)
                .await
                .with_context(|| {
                    format!("Failed to send index. {} indexes processed so far.", count)
                })
                .with_context(|| format!("{} chunks indexed", count))?;
            count += 1;
        }
        Ok(count)
    }
    /// Process a single chunk into an index.
    /// Will log the error
    #[instrument(levl = "debug")]
    fn process_chunk(&mut self, chunk: Chunk) -> Index {
        let Chunk { id, inner } = chunk;
        let index_elements = match inner {
            InnerChunk::GeoJson(chunk) => GeoJsonIndexer::new(chunk).generate_index(),
        }
        .into_iter()
        .filter_map(|index| {
            if let Err(error) = &index {
                event!(
                    Level::DEBUG,
                    %error,
                    internal_id = id,
                    "error creating index for chunk",
                );
            }
            index.ok()
        })
        .collect();
        Index { id, index_elements }
    }
}
