use tokio::io::AsyncRead;

use crate::input::Splitter;
use crate::output::Store;

pub struct GeoDataImporter {
    splitter: Splitter,
    store: Store,
}

impl GeoDataImporter {
    pub fn new(splitter: Splitter, store: Store) -> Self {
        Self { splitter, store }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let splitter_handle = tokio::spawn(self.splitter.run());
        let store_handle = tokio::spawn(self.store.run());
        Ok(())
    }
}
