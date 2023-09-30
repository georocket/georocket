pub mod geo_json_splitter;

pub use crate::types::{GeoDataType, GeoJsonChunk};
pub use geo_json_splitter::GeoJsonSplitter;

mod channels;
pub use channels::SplitterChannels;
use tokio::io::AsyncRead;

pub enum Splitter {
    File(Inner<tokio::fs::File>),
}

pub enum Inner<R> {
    GeoJson(GeoJsonSplitter<R>),
}

impl<R> Inner<R>
where
    R: AsyncRead + Send,
{
    async fn run(self) -> anyhow::Result<GeoDataType> {
        match self {
            Inner::GeoJson(_) => todo!(),
        }
    }
}

impl Splitter {
    pub async fn run(self) -> anyhow::Result<GeoDataType> {
        Ok(match self {
            Splitter::File(inner) => inner.run().await?,
        })
    }
}
