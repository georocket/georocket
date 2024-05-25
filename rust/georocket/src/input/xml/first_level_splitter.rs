use std::ops::Range;

use anyhow::Result;
use quick_xml::events::{BytesStart, Event};

use crate::{
    input::{Splitter, SplitterResult},
    util::window::Window,
};

/// Splits incoming XML tokens whenever a token in the first level (i.e. a
/// child of the XML document's root node) is encountered
#[derive(Default)]
pub struct FirstLevelSplitter {
    /// The current depth in the XML DOM
    depth: usize,

    /// The byte position of the current chunk's opening tag
    mark: usize,

    /// The opening tag data of the XML document's root element. [`None`] if
    /// the root has not been found yet.
    root: Option<BytesStart<'static>>,
}

impl<'a> Splitter<Event<'a>> for FirstLevelSplitter {
    fn on_event(
        &mut self,
        e: &Event,
        pos: Range<usize>,
        window: &mut Window,
    ) -> Result<Option<SplitterResult>> {
        let mut result = None;

        match e {
            Event::Start(s) => {
                if self.depth == 0 {
                    // save root element
                    self.root = Some(s.to_owned());
                } else if self.depth == 1 {
                    self.mark = pos.start;
                }
                self.depth += 1;
            }

            Event::End(e) => {
                self.depth -= 1;
                if self.depth == 1 {
                    let chunk = window.get_bytes(self.mark..pos.end)?;
                    window.advance_to(pos.end)?;
                    result = Some(SplitterResult { chunk });
                }
            }

            _ => {}
        }

        Ok(result)
    }
}
