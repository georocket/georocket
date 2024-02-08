use crate::indexer::MainIndexer;
use crate::input::{Splitter, SplitterChannels};
use crate::output::Store;
use crate::types::{Index, RawChunk};
use futures::future::BoxFuture;
use std::future::Future;
use std::pin::{pin, Pin};
use tokio::sync::mpsc;

pub struct GeoDataImporter {
    splitter: Box<dyn Splitter + Send>,
    store: Box<dyn Store + Send>,
    indexer: MainIndexer,
}

impl GeoDataImporter {
    pub async fn from_constructors<SplitC, StoreC>(
        splitter_constructor: SplitC,
        store_constructor: StoreC,
    ) -> anyhow::Result<Self>
    where
        SplitC: FnOnce(SplitterChannels) -> Box<dyn Splitter + Send>,
        StoreC: FnOnce(
            mpsc::Receiver<RawChunk>,
            mpsc::Receiver<Index>,
        ) -> BoxFuture<'static, anyhow::Result<Box<dyn Store + Send>>>,
    {
        let (channels, chunk_rec, raw_chunk_rec) = SplitterChannels::new_with_channels(1024, 1024);
        let splitter = splitter_constructor(channels);
        let (indexer, index_rec) = MainIndexer::new_with_index_receiver(1024, chunk_rec);
        let store = store_constructor(raw_chunk_rec, index_rec).await?;
        Ok(Self {
            splitter,
            store,
            indexer,
        })
    }
    pub fn new(
        splitter: Box<dyn Splitter + Send>,
        store: Box<dyn Store + Send>,
        indexer: MainIndexer,
    ) -> Self {
        Self {
            splitter,
            store,
            indexer,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let splitter_handle = tokio::spawn(async move { self.splitter.run().await });
        let store_handle = tokio::spawn(async move { self.store.run().await });
        let index_handle = tokio::spawn(self.indexer.run());
        match splitter_handle.await {
            Ok(Ok(splitter_ret)) => {
                println!("Splitter has finished executing!");
                println!("Result: {:?}", splitter_ret);
            }
            Ok(Err(err)) => {
                eprintln!("Splitter failed: {}", err);
            }
            Err(join_err) => {
                eprintln!("Failed to join splitter task: {}", join_err);
            }
        }
        match index_handle.await {
            Ok(Ok(index_ret)) => {
                println!("index has finished executing!");
                println!("Result: {:?}", index_ret);
            }
            Ok(Err(err)) => {
                eprintln!("index failed: {}", err)
            }
            Err(join_err) => {
                eprintln!("Failed to join index task: {}", join_err)
            }
        }
        match store_handle.await {
            Ok(Ok(store_ret)) => {
                println!("Store has finished executing!");
                println!("Result: {:?}", store_ret);
            }
            Ok(Err(err)) => {
                eprintln!("Store failed: {}", err)
            }
            Err(join_err) => {
                eprintln!("Failed to join store task: {}", join_err)
            }
        }
        return Ok(());
    }
}
