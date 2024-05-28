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

#[cfg(test)]
mod tests {
    use std::{io::Cursor, rc::Rc, str::from_utf8};

    use quick_xml::{
        events::{BytesStart, Event},
        Reader,
    };
    use tokio::io::BufReader;

    use crate::{
        input::{Splitter, SplitterResult},
        storage::chunk_meta::{ChunkMeta, XMLChunkMeta},
        util::window_read::WindowRead,
    };

    use super::FirstLevelSplitter;

    const XMLHEADER: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
    const PREFIX: &str = "<root>\n";
    const SUFFIX: &str = "\n</root>";

    /// Uses a [`FirstLevelSplitter`] to split an XML string. Returns the
    /// generated result objects.
    async fn split(xml: String) -> Vec<SplitterResult> {
        let cursor = Cursor::new(xml);
        let window = WindowRead::new(cursor);
        let bufreader = BufReader::new(window);
        let mut reader = Reader::from_reader(bufreader);

        let mut buf = Vec::new();
        let mut splitter = FirstLevelSplitter::default();
        let mut result = Vec::new();
        loop {
            let start_pos = reader.buffer_position();
            let e = reader.read_event_into_async(&mut buf).await.unwrap();
            let end_pos = reader.buffer_position();
            let window = reader.get_mut().get_mut().window_mut();
            if let Some(r) = splitter.on_event(&e, start_pos..end_pos, window).unwrap() {
                result.push(r);
            }
            if e == Event::Eof {
                break;
            }
            buf.clear();
        }

        result
    }

    /// Test if an XML string with one chunk can be split
    #[tokio::test]
    async fn one_chunk() {
        let contents = "<object><child></child></object>";
        let xml = format!("{XMLHEADER}{PREFIX}{contents}{SUFFIX}");

        let chunks = split(xml).await;
        assert_eq!(chunks.len(), 1);

        let root = Rc::new(BytesStart::new("root"));
        let meta = ChunkMeta::Xml(XMLChunkMeta { root });
        assert_eq!(chunks[0].meta, meta);

        assert_eq!(from_utf8(&chunks[0].chunk).unwrap(), contents);
    }

    /// Test if an XML string with tow chunks can be split
    #[tokio::test]
    async fn two_chunks() {
        let contents1 = "<object><child></child></object>";
        let contents2 = "<object><child2></child2></object>";
        let xml = format!("{XMLHEADER}{PREFIX}{contents1}{contents2}{SUFFIX}");

        let chunks = split(xml).await;
        assert_eq!(chunks.len(), 2);

        let root = Rc::new(BytesStart::new("root"));
        let meta = ChunkMeta::Xml(XMLChunkMeta { root });

        assert_eq!(chunks[0].meta, meta);
        assert_eq!(chunks[1].meta, meta);

        assert_eq!(from_utf8(&chunks[0].chunk).unwrap(), contents1);
        assert_eq!(from_utf8(&chunks[1].chunk).unwrap(), contents2);
    }

    /// Test if an XML string with two chunks and a namespace can be split
    #[tokio::test]
    async fn namespace() {
        let contents1 = "<object><child></child></object>";
        let contents2 = "<object><child2></child2></object>";
        let root = r#"root xmlns="http://example.com" xmlns:p="http://example.com""#;
        let xml = format!("{XMLHEADER}<{root}>{contents1}{contents2}</root>");

        let chunks = split(xml).await;
        assert_eq!(chunks.len(), 2);

        let root = Rc::new(BytesStart::from_content(root, 4));
        let meta = ChunkMeta::Xml(XMLChunkMeta { root });

        assert_eq!(chunks[0].meta, meta);
        assert_eq!(chunks[1].meta, meta);

        assert_eq!(from_utf8(&chunks[0].chunk).unwrap(), contents1);
        assert_eq!(from_utf8(&chunks[1].chunk).unwrap(), contents2);
    }

    /// Test if an XML string with two chunks and attributes can be split
    #[tokio::test]
    async fn attributes() {
        let contents1 = "<object><child></child></object>";
        let contents2 = "<object><child2></child2></object>";
        let root = r#"root key="value" key2="value2""#;
        let xml = format!("{XMLHEADER}<{root}>{contents1}{contents2}</root>");

        let chunks = split(xml).await;
        assert_eq!(chunks.len(), 2);

        let root = Rc::new(BytesStart::from_content(root, 4));
        let meta = ChunkMeta::Xml(XMLChunkMeta { root });

        assert_eq!(chunks[0].meta, meta);
        assert_eq!(chunks[1].meta, meta);

        assert_eq!(from_utf8(&chunks[0].chunk).unwrap(), contents1);
        assert_eq!(from_utf8(&chunks[1].chunk).unwrap(), contents2);
    }

    /// Test if an XML string with two chunks, a namespace, and attributes can be split
    #[tokio::test]
    async fn full() {
        let contents1 = "<object><child></child></object>";
        let contents2 = "<object><child2></child2></object>";
        let root = r#"root xmlns="http://example.com" xmlns:p="http://example.com" 
            key="value" key2="value2""#;
        let xml = format!("{XMLHEADER}<{root}>{contents1}{contents2}</root>");

        let chunks = split(xml).await;
        assert_eq!(chunks.len(), 2);

        let root = Rc::new(BytesStart::from_content(root, 4));
        let meta = ChunkMeta::Xml(XMLChunkMeta { root });

        assert_eq!(chunks[0].meta, meta);
        assert_eq!(chunks[1].meta, meta);

        assert_eq!(from_utf8(&chunks[0].chunk).unwrap(), contents1);
        assert_eq!(from_utf8(&chunks[1].chunk).unwrap(), contents2);
    }

    /// Test if an XML string with an UTF8 character can be split
    #[tokio::test]
    async fn utf8() {
        let contents = "<object><child name=\"≈\"></child></object>";
        let xml = format!("{XMLHEADER}{PREFIX}{contents}{SUFFIX}");

        let chunks = split(xml).await;
        assert_eq!(chunks.len(), 1);

        let root = Rc::new(BytesStart::new("root"));
        let meta = ChunkMeta::Xml(XMLChunkMeta { root });
        assert_eq!(chunks[0].meta, meta);

        assert_eq!(from_utf8(&chunks[0].chunk).unwrap(), contents);
    }
}
