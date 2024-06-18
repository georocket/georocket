use std::{ops::Range, rc::Rc};

use anyhow::Result;
use quick_xml::events::{BytesStart, Event};

use crate::{input::Splitter, util::window::Window};

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

impl FirstLevelSplitter {
    /// Returns the opening tag data of the XML document's root element or
    /// [`None`] if the root has not been found yet
    pub fn root(&self) -> Option<Rc<BytesStart<'static>>> {
        self.root.clone()
    }
}

impl<'a> Splitter<Event<'a>> for FirstLevelSplitter {
    fn on_event(
        &mut self,
        e: &Event,
        pos: Range<usize>,
        window: &mut Window,
    ) -> Result<Option<Vec<u8>>> {
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
                    result = Some(chunk);
                }
            }

            _ => {}
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{BufReader, Cursor},
        rc::Rc,
        str::from_utf8,
    };

    use assertor::{assert_that, EqualityAssertion, OptionAssertion, VecAssertion};
    use quick_xml::{
        events::{BytesStart, Event},
        Reader,
    };

    use crate::{input::Splitter, util::window_read::WindowRead};

    use super::FirstLevelSplitter;

    const XMLHEADER: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
    const PREFIX: &str = "<root>\n";
    const SUFFIX: &str = "\n</root>";

    /// Uses a [`FirstLevelSplitter`] to split an XML string. Returns the
    /// generated result objects.
    fn split(xml: String) -> (Vec<Vec<u8>>, Rc<BytesStart<'static>>) {
        let cursor = Cursor::new(xml);
        let window = WindowRead::new(cursor);
        let bufreader = BufReader::new(window);
        let mut reader = Reader::from_reader(bufreader);

        let mut buf = Vec::new();
        let mut splitter = FirstLevelSplitter::default();
        assert_that!(splitter.root()).is_none();
        let mut result = Vec::new();
        loop {
            let start_pos = reader.buffer_position();
            let e = reader.read_event_into(&mut buf).unwrap();
            let end_pos = reader.buffer_position();
            let window = reader.get_mut().get_mut().window_mut();
            if let Some(r) = splitter.on_event(&e, start_pos..end_pos, window).unwrap() {
                // root should exist after the first chunk has been created
                assert_that!(splitter.root()).is_some();

                result.push(r);
            }
            if e == Event::Eof {
                break;
            }
            buf.clear();
        }

        (result, splitter.root().unwrap())
    }

    /// Test if an XML string with one chunk can be split
    #[test]
    fn one_chunk() {
        let contents = "<object><child></child></object>";
        let xml = format!("{XMLHEADER}{PREFIX}{contents}{SUFFIX}");

        let (chunks, root) = split(xml);
        assert_that!(chunks).has_length(1);

        let expected_root = Rc::new(BytesStart::new("root"));
        assert_that!(root).is_equal_to(expected_root);

        assert_that!(from_utf8(&chunks[0]).unwrap()).is_equal_to(contents);
    }

    /// Test if an XML string with tow chunks can be split
    #[test]
    fn two_chunks() {
        let contents1 = "<object><child></child></object>";
        let contents2 = "<object><child2></child2></object>";
        let xml = format!("{XMLHEADER}{PREFIX}{contents1}{contents2}{SUFFIX}");

        let (chunks, root) = split(xml);
        assert_that!(chunks).has_length(2);

        let expected_root = Rc::new(BytesStart::new("root"));
        assert_that!(root).is_equal_to(expected_root);

        assert_that!(from_utf8(&chunks[0]).unwrap()).is_equal_to(contents1);
        assert_that!(from_utf8(&chunks[1]).unwrap()).is_equal_to(contents2);
    }

    /// Test if an XML string with two chunks and a namespace can be split
    #[test]
    fn namespace() {
        let contents1 = "<object><child></child></object>";
        let contents2 = "<object><child2></child2></object>";
        let contents_root = r#"root xmlns="http://example.com" xmlns:p="http://example.com""#;
        let xml = format!("{XMLHEADER}<{contents_root}>{contents1}{contents2}</root>");

        let (chunks, root) = split(xml);
        assert_that!(chunks).has_length(2);

        let expected_root = Rc::new(BytesStart::from_content(contents_root, 4));
        assert_that!(root).is_equal_to(expected_root);

        assert_that!(from_utf8(&chunks[0]).unwrap()).is_equal_to(contents1);
        assert_that!(from_utf8(&chunks[1]).unwrap()).is_equal_to(contents2);
    }

    /// Test if an XML string with two chunks and attributes can be split
    #[test]
    fn attributes() {
        let contents1 = "<object><child></child></object>";
        let contents2 = "<object><child2></child2></object>";
        let contents_root = r#"root key="value" key2="value2""#;
        let xml = format!("{XMLHEADER}<{contents_root}>{contents1}{contents2}</root>");

        let (chunks, root) = split(xml);
        assert_that!(chunks).has_length(2);

        let expected_root = Rc::new(BytesStart::from_content(contents_root, 4));
        assert_that!(root).is_equal_to(expected_root);

        assert_that!(from_utf8(&chunks[0]).unwrap()).is_equal_to(contents1);
        assert_that!(from_utf8(&chunks[1]).unwrap()).is_equal_to(contents2);
    }

    /// Test if an XML string with two chunks, a namespace, and attributes can be split
    #[test]
    fn full() {
        let contents1 = "<object><child></child></object>";
        let contents2 = "<object><child2></child2></object>";
        let contents_root = r#"root xmlns="http://example.com" xmlns:p="http://example.com" 
            key="value" key2="value2""#;
        let xml = format!("{XMLHEADER}<{contents_root}>{contents1}{contents2}</root>");

        let (chunks, root) = split(xml);
        assert_that!(chunks).has_length(2);

        let expected_root = Rc::new(BytesStart::from_content(contents_root, 4));
        assert_that!(root).is_equal_to(expected_root);

        assert_that!(from_utf8(&chunks[0]).unwrap()).is_equal_to(contents1);
        assert_that!(from_utf8(&chunks[1]).unwrap()).is_equal_to(contents2);
    }

    /// Test if an XML string with an UTF8 character can be split
    #[test]
    fn utf8() {
        let contents = "<object><child name=\"≈\"></child></object>";
        let xml = format!("{XMLHEADER}{PREFIX}{contents}{SUFFIX}");

        let (chunks, root) = split(xml);
        assert_that!(chunks).has_length(1);

        let expected_root = Rc::new(BytesStart::new("root"));
        assert_that!(root).is_equal_to(expected_root);

        assert_that!(from_utf8(&chunks[0]).unwrap()).is_equal_to(contents);
    }
}
