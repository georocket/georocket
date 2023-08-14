pub mod geo_json_splitter;

pub use geo_json_splitter::GeoJsonChunk;
pub use geo_json_splitter::GeoJsonSplitter;

#[derive(Clone)]
pub enum Chunk {
    GeoJson(GeoJsonChunk),
}

impl Into<Chunk> for GeoJsonChunk {
    fn into(self) -> Chunk {
        Chunk::GeoJson(self)
    }
}

pub struct SplitterChannels {
    chunk_send: async_channel::Sender<Chunk>,
    raw_send: tokio::sync::mpsc::Sender<u8>,
}

impl SplitterChannels {
    fn new_with_channels(
        chunk_capacity: usize,
        raw_capactity: usize,
    ) -> (
        Self,
        async_channel::Receiver<Chunk>,
        tokio::sync::mpsc::Receiver<u8>,
    ) {
        let (chunk_send, chunk_rec) = async_channel::bounded(chunk_capacity);
        let (raw_send, raw_rec) = tokio::sync::mpsc::channel(raw_capactity);
        (
            SplitterChannels::new(chunk_send, raw_send),
            chunk_rec,
            raw_rec,
        )
    }

    fn new(
        chunk_send: async_channel::Sender<Chunk>,
        raw_send: tokio::sync::mpsc::Sender<u8>,
    ) -> Self {
        Self {
            chunk_send,
            raw_send,
        }
    }
}
