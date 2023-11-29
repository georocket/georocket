pub mod file_store;

use async_trait::async_trait;
pub use file_store::FileStore;

#[async_trait]
pub trait Store {
    async fn run(&self) -> anyhow::Result<usize>;
}

#[async_trait]
impl Store for FileStore {
    async fn run(&self) -> anyhow::Result<usize> {
        self.run().await
    }
}
