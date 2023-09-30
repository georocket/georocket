pub mod file_store;

pub use file_store::FileStore;

pub enum Store {
    FileStore(FileStore),
}

impl Store {
    pub async fn run(self) -> anyhow::Result<usize> {
        Ok(match self {
            Store::FileStore(file_store) => file_store.run().await?,
        })
    }
}
