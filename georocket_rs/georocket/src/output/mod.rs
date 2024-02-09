pub mod file_store;
// #[cfg(feature = "post_gis_store")]
pub mod post_gis_store;

mod index_map;

use async_trait::async_trait;
pub use file_store::FileStore;
pub use post_gis_store::PostGISStore;

pub mod channels;

#[async_trait]
pub trait Store {
    async fn run(&mut self) -> anyhow::Result<usize>;
}

#[async_trait]
impl Store for FileStore {
    async fn run(&mut self) -> anyhow::Result<usize> {
        FileStore::run(self).await
    }
}

#[async_trait]
impl Store for PostGISStore {
    async fn run(&mut self) -> anyhow::Result<usize> {
        PostGISStore::run(self).await
    }
}
