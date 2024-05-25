use std::{ops::Range, rc::Rc};

use anyhow::Result;
use quick_xml::events::{BytesStart, Event};

use crate::{
    input::{Splitter, SplitterResult},
    storage::chunk_meta::{ChunkMeta, XMLChunkMeta},
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
    root: Option<Rc<BytesStart<'static>>>,
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
                    self.root = Some(Rc::new(s.to_owned()));
                } else if self.depth == 1 {
                    self.mark = pos.start;
                }
                self.depth += 1;
            }

            Event::End(_) => {
                self.depth -= 1;
                if self.depth == 1 {
                    let chunk = window.get_bytes(self.mark..pos.end)?;
                    window.advance_to(pos.end)?;
                    let meta = ChunkMeta::Xml(XMLChunkMeta {
                        root: Rc::clone(self.root.as_ref().unwrap()),
                    });
                    result = Some(SplitterResult { chunk, meta });
                }
            }

            _ => {}
        }

        Ok(result)
    }
}
