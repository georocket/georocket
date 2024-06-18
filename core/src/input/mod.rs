use std::ops::Range;

use crate::util::window::Window;
use anyhow::Result;

pub mod xml;

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
    ) -> Result<Option<Vec<u8>>>;
}
