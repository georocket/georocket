use quick_xml::events::Event;

use crate::{
    input::{Splitter, SplitterResult},
    util::window_read::WindowRead,
};

use super::xml_splitter::XmlSplitter;

/// Splits incoming XML tokens whenever a token in the first level (i.e. a
/// child of the XML document's root node) is encountered
pub struct FirstLevelSplitter<R> {
    depth: usize,
    xml_splitter: XmlSplitter,
    window: WindowRead<R>,
}

impl<R> FirstLevelSplitter<R> {
    pub fn new(window: WindowRead<R>) -> Self {
        Self {
            depth: 0,
            xml_splitter: XmlSplitter::default(),
            window,
        }
    }
}

impl<'a, R> Splitter<Event<'a>> for FirstLevelSplitter<R> {
    fn on_event(&mut self, e: &Event, pos: usize) -> Option<SplitterResult> {
        let mut result = None;

        // create new chunk if we're just after the end of a first-level element
        if self.depth == 1 {
            if let Some(_) = self.xml_splitter.mark {
                result = self.xml_splitter.make_result(pos)
            }
        }

        match e {
            Event::Start(_) => {
                if self.depth == 1 {
                    self.xml_splitter.mark = Some(pos);
                }
                self.depth += 1;
            }

            Event::End(_) => {
                self.depth -= 1;
            }

            _ => {}
        }

        self.xml_splitter.on_event(e);

        result
    }
}
