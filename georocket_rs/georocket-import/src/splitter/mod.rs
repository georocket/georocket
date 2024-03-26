mod buffer;
pub mod geo_json_splitter;
mod xml_splitter;

pub use crate::types::{GeoDataType, GeoJsonChunk};
use async_trait::async_trait;
pub use geo_json_splitter::GeoJsonSplitter;

mod channels;
pub use channels::SplitterChannels;
use tokio::io::AsyncRead;

#[derive(Debug)]
pub enum SplitterReturn {
    GeoJSON(geo_json_splitter::GeoJsonType),
}

#[async_trait]
pub trait Splitter {
    async fn run(&mut self) -> anyhow::Result<SplitterReturn>;
}

#[async_trait]
impl<R> Splitter for GeoJsonSplitter<R>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    async fn run(&mut self) -> anyhow::Result<SplitterReturn> {
        Ok(SplitterReturn::GeoJSON(self.run().await?))
    }
}
