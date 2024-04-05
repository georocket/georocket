use quick_xml::events::Event;
use quick_xml::name::QName;
use quick_xml::NsReader;
use std::ops::Range;
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
}

type Chunk = Vec<Event<'static>>;

/// Splitter for XML documents.
pub struct BasicSplitter<R> {
    parser: NsReader<BufReader<R>>,
    current_event: Option<Event<'static>>,
    buffer: Vec<u8>,
}

impl<R: AsyncRead + Unpin> BasicSplitter<R> {
    /// Construct a new `BasicSplitter` from a provided reader.
    /// Wraps the provided reader in a `BufReader`.
    pub fn new(reader: R) -> BasicSplitter<R> {
        let buff_reader = BufReader::new(reader);
        Self {
            parser: NsReader::from_reader(buff_reader),
            current_event: None,
            buffer: Vec::new(),
        }
    }
    /// Return a reference to the internal `BufReader`.
    pub fn reader(&mut self) -> &mut BufReader<R> {
        self.parser.get_mut()
    }
    /// Finds the next opening `Event::Start` tag with the specified name.
    pub async fn find_opening(&mut self, name: &str) -> Result<(), Error> {
        let start = QName(name.as_bytes());
        loop {
            self.buffer.clear();
            match self.advance().await? {
                Event::Eof => break Err(Error::StartEventNotFound),
                Event::Start(s) if s.name() == start => break Ok(()),
                _ => {}
            }
        }
    }
    /// Advances the splitter to the next `Event`, returning a reference to it.
    pub async fn advance(&mut self) -> Result<&Event, Error> {
        let event = self.parser.read_event_into_async(&mut self.buffer).await?;
        self.current_event = Some(event.into_owned());
        self.buffer.clear();
        // we just put something into the Option, we can unwrap it safely.
        Ok(self.current_event.as_ref().unwrap())
    }
    /// Extracts all `Events` from the current opening tag to the corresponding closing tag.
    /// Also returns the range of bytes that contain the events.
    /// # Errors
    /// Will error if the last event returned by [`advance`] wasn't `Event::Start`.
    pub async fn extract_current(&mut self) -> Result<(Chunk, Range<usize>), Error> {
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

        Ok((events, start_position..end_position))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::splitter::buffer::scratch_reader::ScratchReader;

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
        let (_, span) = basic_splitter.extract_current().await.unwrap();
        let raw_chunk: Vec<_> = basic_splitter
            .reader()
            .get_ref()
            .window()
            .get_bytes(span)
            .unwrap()
            .cloned()
            .collect();
        assert_eq!(raw_chunk, FIST_ITEM.as_bytes());
        basic_splitter.find_opening("CD").await.unwrap();
        let (_, span) = basic_splitter.extract_current().await.unwrap();
        let raw_chunk: Vec<_> = basic_splitter
            .reader()
            .get_ref()
            .window()
            .get_bytes(span)
            .unwrap()
            .cloned()
            .collect();
        assert_eq!(raw_chunk, SECOND_ITEM.as_bytes());
    }
}
