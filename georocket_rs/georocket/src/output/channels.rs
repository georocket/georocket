use crate::types::{Index, RawChunk};
use tokio::sync::mpsc;

pub struct StoreChannels {
    pub(super) raw_rec: mpsc::Receiver<RawChunk>,
    pub(super) index_rec: mpsc::Receiver<Index>,
}

impl StoreChannels {
    pub fn new(raw_rec: mpsc::Receiver<RawChunk>, index_rec: mpsc::Receiver<Index>) -> Self {
        Self { raw_rec, index_rec }
    }
}
