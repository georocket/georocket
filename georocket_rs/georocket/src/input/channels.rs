use thiserror::Error;

use crate::types::{Chunk, InnerChunk, RawChunk};

#[derive(Error, Debug)]
pub enum SplitterChannelError {
    #[error("unable to send chunk")]
    SendChunkError(#[from] async_channel::SendError<Chunk>),
    #[error("unable to send raw chunk")]
    SendRawError(#[from] tokio::sync::mpsc::error::SendError<RawChunk>),
}

pub struct SplitterChannels {
    chunk_send: async_channel::Sender<Chunk>,
    raw_send: tokio::sync::mpsc::Sender<RawChunk>,
    counter: usize,
}

impl SplitterChannels {
    pub fn new_with_channels(
        chunk_capacity: usize,
        raw_capactity: usize,
    ) -> (
        Self,
        async_channel::Receiver<Chunk>,
        tokio::sync::mpsc::Receiver<RawChunk>,
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
        raw_send: tokio::sync::mpsc::Sender<RawChunk>,
    ) -> Self {
        Self {
            chunk_send,
            raw_send,
            counter: 0,
        }
    }

    pub async fn send(
        &mut self,
        chunk: impl Into<InnerChunk>,
        raw: Vec<u8>,
    ) -> Result<(), SplitterChannelError> {
        let id = self.counter;
        self.counter += 1;
        self.send_chunk(id, chunk).await?;
        self.send_raw(id, raw).await?;
        Ok(())
    }

    async fn send_chunk(
        &self,
        id: usize,
        chunk: impl Into<InnerChunk>,
    ) -> Result<(), async_channel::SendError<Chunk>> {
        let chunk = chunk.into();
        let chunk = Chunk { id, inner: chunk };
        self.chunk_send.send(chunk).await
    }

    async fn send_raw(
        &self,
        id: usize,
        raw: Vec<u8>,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<RawChunk>> {
        let raw = RawChunk { id, raw };
        self.raw_send.send(raw).await
    }
}
