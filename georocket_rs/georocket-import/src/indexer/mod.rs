//! The indexer takes [`Chunk`s](crate::importer::types::Chunk) from a channel and processes them into
//! [`Index`es](crate::importer::types::Index).
//! A [`MainIndexer`] receives chunks from a [`Splitter`](crate::splitter::splitter::Splitter) and
//! spins up concrete indexers for type of index that needs to be created.
//! These are based on the [`InnerChunk`](crate::importer::types::InnerChunk) type. of the received chunk.

use anyhow::Context;
use async_channel::Receiver;
use tokio::sync::mpsc;
use tracing::{event, instrument, Level};

mod geo_json_indexer;
use crate::types::{Chunk, Index, InnerChunk};
use geo_json_indexer::GeoJsonIndexer;

#[derive(Debug)]
pub struct IndexerChannels {
    chunk_rec: Receiver<Chunk>,
    index_send: mpsc::Sender<Index>,
}

impl IndexerChannels {
    pub fn new(chunk_rec: Receiver<Chunk>, index_send: mpsc::Sender<Index>) -> Self {
        Self {
            chunk_rec,
            index_send,
        }
    }
}

#[derive(Debug)]
pub struct MainIndexer {
    channels: IndexerChannels,
}

impl MainIndexer {
    /// Create a new `MainIndexer` with the provided channels.
    pub fn new(indexer_channels: IndexerChannels) -> Self {
        Self {
            channels: indexer_channels,
        }
    }

    // /// Create a new `MainIndexer` along with the a c receiver channel for the generated `Index`es.
    // /// `capacity` specifies the capacity of the created channel.
    // pub fn new_with_index_receiver(
    //     capacity: usize,
    //     chunk_rec: Receiver<Chunk>,
    // ) -> (Self, mpsc::Receiver<Index>) {
    //     let (index_send, index_rec) = mpsc::channel(capacity);
    //     (Self::new(chunk_rec, index_send), index_rec)
    // }

    /// Run the `MainIndexer` until the channel is closed.
    /// Returns the amount of `Chunk`s that have been processed.
    pub async fn run(self) -> anyhow::Result<usize> {
        let chunk_rec = self.channels.chunk_rec;
        let index_send = self.channels.index_send;
        let mut count = 0;
        while let Ok(chunk) = chunk_rec.recv().await {
            let index = Self::process_chunk(chunk);
            index_send
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
    #[instrument(level = "debug")]
    fn process_chunk(chunk: Chunk) -> Index {
        let Chunk { id, inner } = chunk;
        let index_elements = match inner {
            InnerChunk::GeoJson(chunk) => GeoJsonIndexer::new(chunk).generate_index(),
            InnerChunk::XML(chunk) => unimplemented!("xml indexer is not yet implemented"),
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
