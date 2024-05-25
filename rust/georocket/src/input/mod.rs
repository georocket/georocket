use std::ops::Range;

use crate::util::window::Window;
use anyhow::Result;

pub mod xml;

/// Result of the [`Splitter::onEvent`] method. Holds a chunk and its metadata.
#[derive(Debug)]
pub struct SplitterResult {
    pub chunk: Vec<u8>,
}

///  Splits input tokens and returns chunks
pub trait Splitter<E> {
    /// Will be called on every stream event. Returns new [`SplitterResult`]
    /// object (containing chunk and metadata) or [`None`] if no result was
    /// produced
    fn on_event(
        &mut self,
        event: &E,
        pos: Range<usize>,
        window: &mut Window,
    ) -> Result<Option<SplitterResult>>;
}
