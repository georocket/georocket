use crate::splitter::buffer::scratch_reader::ScratchReader;
use crate::splitter::buffer::window_buffer::{WindowBuffer, WindowBufferError};
use crate::types::XMLStartElement;
use quick_xml::events::Event;
use quick_xml::name::QName;
use quick_xml::NsReader;
use thiserror::Error;
use tokio::io::{AsyncRead, BufReader};

#[derive(Debug, Error)]
pub enum Error {
    #[error("error in the underlying parser")]
    QuickXmlError(#[from] quick_xml::Error),
    #[error("no current event to extract the contents of")]
    NoCurrentEvent,
    #[error("expected `Event::Start` variant, found {0:?} instead")]
    InvalidStartEvent(Event<'static>),
    #[error("specified start event not found")]
    StartEventNotFound,
    #[error("failed to extract bytes: {0}")]
    ExtractBytesError(#[from] WindowBufferError),
}

type Chunk = Vec<Event<'static>>;

/// Splitter for XML documents.
pub struct BasicSplitter<R> {
    pub(super) parser: NsReader<BufReader<ScratchReader<R>>>,
    current_event: Option<Event<'static>>,
    buffer: Vec<u8>,
    parents: Vec<XMLStartElement>,
    pub(super) header: Option<Vec<u8>>,
}

impl<R: AsyncRead + Unpin> BasicSplitter<R> {
    /// Construct a new `BasicSplitter` from a provided reader.
    /// Wraps the provided reader in a `BufReader`.
    pub fn new(reader: R) -> BasicSplitter<R> {
        let buff_reader = BufReader::new(ScratchReader::new(reader));
        Self {
            parser: NsReader::from_reader(buff_reader),
            current_event: None,
            buffer: Vec::new(),
            parents: Vec::new(),
            header: None,
        }
    }
    /// Finds the next opening `Event::Start` tag with the specified name.
    pub async fn find_opening(&mut self, name: &str) -> Result<&Event, Error> {
        let start = QName(name.as_bytes());
        loop {
            match self.advance().await? {
                Event::Eof => break Err(Error::StartEventNotFound),
                Event::Start(s) if s.name() == start => {
                    break Ok(self.current_event.as_ref().expect("we just found an event"))
                }
                _ => {}
            }
        }
    }
    pub async fn find_next_opening(&mut self) -> Result<Option<&Event>, Error> {
        loop {
            match self.advance().await? {
                Event::Eof => break Err(Error::StartEventNotFound),
                Event::Start(_) => {
                    break Ok(Some(
                        self.current_event.as_ref().expect("we just found an event"),
                    ))
                }
                _ => {}
            }
        }
    }
    pub fn current_event(&self) -> Option<&Event> {
        self.current_event.as_ref()
    }
    /// Advances the splitter to the next `Event`, returning a reference to it.
    pub async fn advance(&mut self) -> Result<&Event, Error> {
        // push or pop from parents before every advance.
        if let Some(event) = self.current_event.as_ref() {
            match event {
                Event::Start(start) => {
                    let local_name = start.local_name().into_inner().to_vec();
                    let raw = {
                        let pos = self.parser.buffer_position();
                        let start = pos - start.len() - 2;
                        let bytes = self.window().get_bytes_vec(start..pos)?;
                        self.window().move_window(pos)?;
                        bytes
                    };
                    self.parents.push(XMLStartElement { raw, local_name })
                }
                Event::End(_) => {
                    self.parents.pop();
                }
                _ => (),
            }
        }
        let event = self.parser.read_event_into_async(&mut self.buffer).await?;
        self.current_event = Some(event.into_owned());
        if let Event::Decl(decl) = self
            .current_event
            .as_ref()
            .expect("we just put something here")
        {
            let pos = self.parser.buffer_position();
            // -4 because `<?` and `?>` are not included in the length
            let start = pos - 4 - decl.len();
            self.header = Some(self.window().get_bytes_vec(start..pos)?);
            self.window().move_window(pos)?;
        }
        self.buffer.clear();
        Ok(self
            .current_event
            .as_ref()
            .expect("we just put an event in here"))
    }
    /// Extracts all `Events` from the current opening tag to the corresponding closing tag.
    /// Also returns the bytes corresponding to the extracted tag.
    /// # Errors
    /// Will error if:
    /// - the last event returned by [`advance`] wasn't `Event::Start`
    /// - extracting the corresponding bytes fails
    pub async fn extract_current(
        &mut self,
    ) -> Result<(Chunk, Vec<XMLStartElement>, Vec<u8>), Error> {
        let Some(event) = self.current_event.take() else {
            return Err(Error::NoCurrentEvent);
        };
        let Event::Start(start) = event else {
            return Err(Error::InvalidStartEvent(event));
        };
        // start position should be the opening `<`, which is the length of the content + 2
        // from the current position:
        // __<FOO>___
        //   ^    ^
        //   |    |
        // 0123456789
        // buffer_position = 7, content length = 3, start = 2
        // start = 7 - (3 + 2) = 2
        let start_position = self.parser.buffer_position() - (start.len() + 2);
        let mut events = vec![Event::Start(start.clone().into_owned())];
        let mut depth = 1;
        let end_position = loop {
            self.buffer.clear();
            let event = self.parser.read_event_into_async(&mut self.buffer).await?;
            events.push(event.clone().into_owned());
            match &event {
                Event::Start(_) => depth += 1,
                Event::End(_) => {
                    depth -= 1;
                    if depth == 0 {
                        break self.parser.buffer_position();
                    }
                }
                _ => (),
            };
        };
        let bytes = self.window().get_bytes_vec(start_position..end_position)?;
        self.window().move_window(end_position)?;

        Ok((events, self.parents.clone(), bytes))
    }
    fn window(&mut self) -> &mut WindowBuffer {
        self.parser.get_mut().get_mut().window_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::splitter::buffer::scratch_reader::ScratchReader;
    use std::str;

    const SIMPLE_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<CATALOG>
<CD>
<TITLE>Empire Burlesque</TITLE>
<ARTIST>Bob Dylan</ARTIST>
<COUNTRY>USA</COUNTRY>
<COMPANY>Columbia</COMPANY>
<PRICE>10.90</PRICE>
<YEAR>1985</YEAR>
</CD>
<CD>
<TITLE>Hide your heart</TITLE>
<ARTIST>Bonnie Tyler</ARTIST>
<COUNTRY>UK</COUNTRY>
<COMPANY>CBS Records</COMPANY>
<PRICE>9.90</PRICE>
<YEAR>1988</YEAR>
</CD>
</CATALOG>
"#;

    const FIST_ITEM: &str = r#"<CD>
<TITLE>Empire Burlesque</TITLE>
<ARTIST>Bob Dylan</ARTIST>
<COUNTRY>USA</COUNTRY>
<COMPANY>Columbia</COMPANY>
<PRICE>10.90</PRICE>
<YEAR>1985</YEAR>
</CD>"#;

    const SECOND_ITEM: &str = r#"<CD>
<TITLE>Hide your heart</TITLE>
<ARTIST>Bonnie Tyler</ARTIST>
<COUNTRY>UK</COUNTRY>
<COMPANY>CBS Records</COMPANY>
<PRICE>9.90</PRICE>
<YEAR>1988</YEAR>
</CD>"#;

    #[tokio::test]
    async fn test_extract() {
        let scratch_reader = ScratchReader::new(SIMPLE_XML.as_bytes());
        let mut basic_splitter = BasicSplitter::new(scratch_reader);
        basic_splitter.find_opening("CD").await.unwrap();
        let (_, _, bytes) = basic_splitter.extract_current().await.unwrap();
        assert_eq!(bytes, FIST_ITEM.as_bytes());
        basic_splitter.find_opening("CD").await.unwrap();
        let (_, _, bytes) = basic_splitter.extract_current().await.unwrap();
        assert_eq!(bytes, SECOND_ITEM.as_bytes());
    }

    #[tokio::test]
    async fn test_extract_parents_single() {
        let xml = "<root><object>Some Text</object></root>";
        let scratch_reader = ScratchReader::new(xml.as_bytes());
        let mut basic_splitter = BasicSplitter::new(scratch_reader);
        basic_splitter.find_opening("object").await.unwrap();
        let (_, parents, _) = basic_splitter.extract_current().await.unwrap();
        assert_eq!(parents.len(), 1);
        assert_eq!(parents[0].local_name, "root".as_bytes())
    }

    #[tokio::test]
    async fn test_extract_header() {
        let xml_header = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>";
        let scratch_reader = ScratchReader::new(xml_header.as_bytes());
        let mut basic_splitter = BasicSplitter::new(scratch_reader);
        basic_splitter.advance().await.unwrap();
        assert_eq!(
            str::from_utf8(&basic_splitter.header.unwrap()).unwrap(),
            xml_header
        );
    }

    #[tokio::test]
    async fn test_extract_parents_multiple() {
        let xml = "<root1><root2><root3><object>Some Text</object></roo3></root2></root1>";
        let scratch_reader = ScratchReader::new(xml.as_bytes());
        let mut basic_splitter = BasicSplitter::new(scratch_reader);
        basic_splitter.find_opening("object").await.unwrap();
        let (_, parents, _) = basic_splitter.extract_current().await.unwrap();
        assert_eq!(parents.len(), 3);
        assert_eq!(parents[0].local_name, "root1".as_bytes());
        assert_eq!(parents[1].local_name, "root2".as_bytes());
        assert_eq!(parents[2].local_name, "root3".as_bytes());
    }

    #[tokio::test]
    async fn test_extract_neighbors() {
        let xml = "<root1>\
            <root2_1>\
                <object>Some Text</object>\
            </root2_1>\
            <root2_2>\
                <object>Some Text</object>\
            <root2_2>\
        </root1>";
        let scratch_reader = ScratchReader::new(xml.as_bytes());
        let mut basic_splitter = BasicSplitter::new(scratch_reader);

        // first object should have the parents <root1> and <root2_1>
        basic_splitter.find_opening("object").await.unwrap();
        let (_, parents, _) = basic_splitter.extract_current().await.unwrap();
        assert_eq!(parents.len(), 2);
        assert_eq!(parents[0].local_name, "root1".as_bytes());
        assert_eq!(parents[1].local_name, "root2_1".as_bytes());

        // second object should have the parents <root1> and <root2_2>
        basic_splitter.find_opening("object").await.unwrap();
        let (_, parents, _) = basic_splitter.extract_current().await.unwrap();
        assert_eq!(parents.len(), 2);
        assert_eq!(parents[0].local_name, "root1".as_bytes());
        assert_eq!(parents[1].local_name, "root2_2".as_bytes());
    }
}