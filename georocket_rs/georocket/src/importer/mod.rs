use crate::indexer::MainIndexer;
use crate::input::Splitter;
use crate::output::Store;

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
