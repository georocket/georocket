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

    pub async fn run(mut self) {
        let mut splitter_handle = tokio::spawn(async move { self.splitter.run().await });
        let mut store_handle = tokio::spawn(async move { self.store.run().await });
        let mut index_handle = tokio::spawn(self.indexer.run());

        loop {
            tokio::select! {
                splitt_res = &mut splitter_handle, if !splitter_handle.is_finished() => {

                }
                store_res = &mut store_handle, if !store_handle.is_finished() => {

                }
                index_res = &mut index_handle, if !index_handle.is_finished() => {

                }
                else => break,
            }
        }
    }
}
