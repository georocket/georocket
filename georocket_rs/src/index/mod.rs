// Imports
// Error handling:
use anyhow::anyhow;
use thiserror::Error;

// Asynchronous tools:
use async_channel;
use tokio::sync::mpsc;
use tokio::task::JoinError;

// Crate imports:
use crate::chunk::{Chunk, GeoMetaDataCollection, Source};

/// Specialized [`Result`] type indexing errors.
pub type Result<T> = std::result::Result<T, Error>;

/// `Error` type for indexing errors.
#[derive(Error, Debug)]
pub enum Error {
    #[error("unknown index error")]
    Other(#[from] anyhow::Error),
    #[error("failed to join index task")]
    JoinError(#[from] JoinError),
    #[error("error sending or receiving from channel")]
    ChannelError(anyhow::Error),
}

pub struct IndexManager<D, H> {
    chunk_receiver: async_channel::Receiver<Chunk<D, H>>,
    metadata_sender: mpsc::Sender<Result<GeoMetaDataCollection>>,
}

impl<D, H> IndexManager<D, H> {
    /// Construct a new `Indexmanager`, which takes chunks out of the `chunk_receiver` and puts
    /// the calculated metadata in `metadata_sender`.
    fn new(
        chunk_receiver: async_channel::Receiver<Chunk<D, H>>,
        metadata_sender: mpsc::Sender<Result<GeoMetaDataCollection>>,
    ) -> Self {
        Self {
            chunk_receiver,
            metadata_sender,
        }
    }

    /// Construct a new `Indexer` along with the required channels.
    /// Channels are created with a capacity as specified by `feature_channel_capacity` and
    /// `processed_feature_channel_capacity`
    fn new_with_channels(
        feature_channel_capacity: usize,
        processed_feature_channel_capacity: usize,
    ) -> (
        Self,
        async_channel::Sender<Chunk<D, H>>,
        mpsc::Receiver<Result<GeoMetaDataCollection>>,
    ) {
        let (chunk_sender, chuck_receiver) = async_channel::bounded(feature_channel_capacity);
        let (metadata_sender, metadata_receiver) =
            mpsc::channel(processed_feature_channel_capacity);
        (
            Self::new(chuck_receiver, metadata_sender),
            chunk_sender,
            metadata_receiver,
        )
    }

    /// Consumes the `Indexer` and begins processing Chunks
    /// Returns once the channel supplying the chunks has been closed and the final chunk has
    /// been processed.
    ///
    /// The amount of spawned tasks can be configured with the `task_count` parameter.
    async fn run(self, task_count: usize) -> Vec<Result<()>> {
        // contains handles to all indexing tasks that are spawned
        let mut indexing_tasks = tokio::task::JoinSet::new();
        // spawn index tasks
        for _ in 0..task_count {
            let chunks = self.chunk_receiver.clone();
            let metadata = self.metadata_sender.clone();
            indexing_tasks.spawn(Self::index_task(chunks, metadata));
        }
        //
        let mut indexer_results: Vec<Result<()>> = Vec::new();
        while let Some(index_task_join) = indexing_tasks.join_next().await {
            // Nested `Result<Result<()>, JoinError>`.
            // Unwrap the inner result if it is Ok, or turn the contained
            // `JoinError` into an `index::Error` and wrap it in `Result::Err`
            let res = index_task_join.unwrap_or_else(|e| Err(e.into()));
            indexer_results.push(res);
        }
        indexer_results
    }

    /// Indexes `Chunk`s received from `chunk_receiver` and returns the result in `metadata_sender
    /// Returns once the `chunk_receiver` chanel has been closed.
    ///
    /// # Errors
    ///
    /// Returns an error if the associated read half of `metadata_sender` has been closed prematurely.
    async fn index_task(
        chunk_receiver: async_channel::Receiver<Chunk>,
        metadata_sender: mpsc::Sender<Result<GeoMetaDataCollection>>,
    ) -> Result<()> {
        while let Ok(chunk) = chunk_receiver.recv().await {
            let index = Self::index_chunk(chunk);
            metadata_sender
                .send(index)
                .await
                .map_err(|e| Error::ChannelError(anyhow!(e)))?;
        }
        Ok(())
    }

    /// Indexes the `chunk` and returns the generated meta data
    ///
    /// # Errors
    ///
    /// Returns an error, if the indexing fails
    fn index_chunk(chunk: Chunk) -> Result<GeoMetaDataCollection> {
        let index = match chunk.source() {
            Source::GeoJSON => Self::index_geo_json(chunk)?,
        };
        Ok(index)
    }

    /// Interprets `chunk` as GeoJSON data and generates metadata
    fn index_geo_json(chunk: Chunk) -> Result<GeoMetaDataCollection> {
        Ok(GeoMetaDataCollection::new(chunk.id()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::Chunk;
    use crate::chunk::Source::GeoJSON;

    const FEATURES: [&str; 3] = [
        "{ \"type\": \"Feature\", \"geometry\": { \"type\": \"Point\", \"coordinates\": [102.0, 0.5] }, \"properties\": { \"prop0\": \"value0\" } }",
        "{ \"type\": \"Feature\", \"geometry\": { \"type\": \"LineString\", \"coordinates\": [ [102.0, 0.0], [103.0, 1.0], [104.0, 0.0], [105.0, 1.0] ] }, \"properties\": { \"prop0\": \"value0\", \"prop1\": 0.0 } }",
        "{ \"type\": \"Feature\", \"geometry\": { \"type\": \"Polygon\", \"coordinates\": [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] ] } }",
    ];

    #[tokio::test]
    async fn test_feature_processor() {
        const INDEX_TASK_COUNT: usize = 4;
        // generate some chunks out of the test features
        let chunks: Vec<Chunk> = FEATURES
            .iter()
            .enumerate()
            .map(|(i, feature)| Chunk::new(Some(i), GeoJSON, Vec::from(feature.as_bytes())))
            .collect();

        let (indexer, chunk_sender, mut metadata_receiver) =
            IndexManager::new_with_channels(64, 64);

        // run the `FeatureProcessor`
        let indexer_handle = tokio::spawn(indexer.run(INDEX_TASK_COUNT));

        // send all the chunks to the indexer
        for chunk in chunks.iter().cloned() {
            chunk_sender.send(chunk).await.unwrap();
        }

        // close the send end to signal that no more features will be sent though the channel
        chunk_sender.close();

        let mut metadata_vec = Vec::new();
        while let Some(metadata) = metadata_receiver.recv().await {
            metadata_vec.push(metadata);
        }
        assert_eq!(metadata_vec.len(), FEATURES.len());

        // # we are expecting the same number of Results, as indexer tasks we started
        assert_eq!(indexer_handle.await.unwrap().len(), INDEX_TASK_COUNT);
    }
}
