use crate::indexer::{IndexerChannels, MainIndexer};
use crate::input::{GeoJsonSplitter, Splitter, SplitterChannels};
use crate::output::channels::StoreChannels;
use crate::output::{FileStore, PostGISStore, Store};
use anyhow::Context;
use clap::ValueEnum;
use tokio::sync::mpsc;
use tokio_postgres::NoTls;

pub mod import_args;

pub struct GeoDataImporter {
    splitter: Box<dyn Splitter + Send>,
    store: Box<dyn Store + Send>,
    indexer: MainIndexer,
}

impl GeoDataImporter {
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

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum StoreType {
    PostGIS,
    FileStore,
}

// #[derive(Copy, Clone, Debug)]
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SourceType {
    GeoJsonFile,
}

pub struct ImporterBuilder {
    channel_capacity: usize,
    store_type: Option<StoreType>,
    store_config: Option<String>,
    source_type: Option<SourceType>,
    source_config: Option<String>,
}

impl ImporterBuilder {
    pub fn new() -> Self {
        Self {
            channel_capacity: 1024,
            store_type: None,
            store_config: None,
            source_type: None,
            source_config: None,
        }
    }

    pub fn with_store_type(mut self, store_type: StoreType) -> Self {
        self.store_type = Some(store_type);
        self
    }

    pub fn with_store_config(mut self, config: String) -> Self {
        self.store_config = Some(config);
        self
    }

    pub fn with_source_type(mut self, source_type: SourceType) -> Self {
        self.source_type = Some(source_type);
        self
    }

    pub fn with_source_config(mut self, source_config: String) -> Self {
        self.source_config = Some(source_config);
        self
    }
}

impl ImporterBuilder {
    pub async fn build(self) -> anyhow::Result<GeoDataImporter> {
        let (raw_send, raw_rec) = mpsc::channel(self.channel_capacity);
        let (chunk_send, chunk_rec) = async_channel::bounded(self.channel_capacity);
        let (index_send, index_rec) = mpsc::channel(self.channel_capacity);

        let indexer_channels = IndexerChannels::new(chunk_rec, index_send);
        let splitter_channels = SplitterChannels::new(chunk_send, raw_send);
        let store_channels = StoreChannels::new(raw_rec, index_rec);

        let store = self.make_store(store_channels).await?;
        let splitter = self.make_splitter(splitter_channels).await?;
        let indexer = MainIndexer::new(indexer_channels);

        Ok(GeoDataImporter::new(splitter, store, indexer))
    }

    async fn make_store(
        &self,
        store_channels: StoreChannels,
    ) -> anyhow::Result<Box<dyn Store + Send>> {
        let store_config = self
            .store_config
            .as_ref()
            .context("No storage configuration provided")?;
        let store: Box<dyn Store + Send> = match self
            .store_type
            .as_ref()
            .context("No storage destination specified")?
        {
            StoreType::PostGIS => {
                let (client, connection) = tokio_postgres::connect(store_config, NoTls)
                    .await
                    .with_context(|| {
                        format!(
                            "unable to establish connection to specified database: {}",
                            store_config
                        )
                    })?;
                tokio::spawn(connection);
                Box::new(PostGISStore::new(client, store_channels).await?)
            }
            StoreType::FileStore => Box::new(FileStore::new(store_config, store_channels).await?),
        };
        Ok(store)
    }

    async fn make_splitter(
        &self,
        splitter_channels: SplitterChannels,
    ) -> anyhow::Result<Box<dyn Splitter + Send>> {
        let source_config = self
            .source_config
            .as_ref()
            .context("No source configuration provided")?;
        let source = match self.source_type.as_ref().context("No source specified")? {
            SourceType::GeoJsonFile => {
                let file = tokio::fs::File::open(source_config)
                    .await
                    .with_context(|| format!("unable to open specified file: {}", source_config))?;
                let splitter = GeoJsonSplitter::new(file, splitter_channels);
                Box::new(splitter)
            }
        };
        Ok(source)
    }
}
