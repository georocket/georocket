use std::collections::VecDeque;

use quick_xml::events::{BytesStart, Event};

use crate::input::SplitterResult;

#[derive(Default)]
pub(super) struct XmlSplitter {
    /// A marked position
    pub mark: Option<usize>,

    /// A stack keeping all encountered start elements
    start_elements: VecDeque<BytesStart<'static>>,
}

impl XmlSplitter {
    pub fn on_event(&mut self, e: &quick_xml::events::Event) {
        if let None = self.mark {
            match e {
                Event::Start(s) => {
                    self.start_elements.push_back(s.to_owned());
                }

                Event::End(_) => {
                    self.start_elements.pop_back();
                }

                _ => {}
            }
        }
    }

    pub fn make_result(&self, pos: usize) -> Option<SplitterResult> {
        todo!()

        //   val bytes = window.getBytes(mark, pos)
        //   val buf = Buffer.buffer(bytes)
        //   window.advanceTo(pos)
        //   mark = -1

        //   return Splitter.Result(buf, lastPrefix!!, lastSuffix!!, lastChunkMeta!!)
    }
}

// /// Splitter for XML documents.
// pub struct BasicSplitter<R> {
//     pub(super) parser: Reader<BufReader<ScratchReader<R>>>,
//     current_event: Option<Event<'static>>,
//     buffer: Vec<u8>,
//     parents: Vec<XMLStartElement>,
//     pub(super) header: Option<Vec<u8>>,
//     namespaces: Vec<Vec<XMLNamespace>>,
//     attributes: Vec<Vec<XMLAttribute>>,
// }

// impl<R: AsyncRead + Unpin> BasicSplitter<R> {
//     /// Construct a new `BasicSplitter` from a provided reader.
//     /// Wraps the provided reader in a `BufReader`.
//     pub fn new(reader: R) -> BasicSplitter<R> {
//         let buff_reader = BufReader::new(ScratchReader::new(reader));
//         Self {
//             parser: Reader::from_reader(buff_reader),
//             current_event: None,
//             buffer: Vec::new(),
//             parents: Vec::new(),
//             header: None,
//             namespaces: Vec::new(),
//             attributes: Vec::new(),
//         }
//     }
//     /// Finds the next opening `Event::Start` tag with the specified name.
//     pub async fn find_opening(&mut self, name: &str) -> Result<&Event, Error> {
//         let start = QName(name.as_bytes());
//         loop {
//             match self.advance().await? {
//                 Event::Eof => break Err(Error::StartEventNotFound),
//                 Event::Start(s) if s.name() == start => {
//                     break Ok(self.current_event.as_ref().expect("we just found an event"))
//                 }
//                 _ => {}
//             }
//         }
//     }
//     pub async fn find_next_opening(&mut self) -> Result<Option<&Event>, Error> {
//         loop {
//             match self.advance().await? {
//                 Event::Eof => break Ok(None),
//                 Event::Start(_) => {
//                     break Ok(Some(
//                         self.current_event.as_ref().expect("we just found an event"),
//                     ))
//                 }
//                 _ => {}
//             }
//         }
//     }
//     pub fn current_event(&self) -> Option<&Event> {
//         self.current_event.as_ref()
//     }
//     /// Advances the splitter to the next `Event`, returning a reference to it.
//     pub async fn advance(&mut self) -> Result<&Event, Error> {
//         // push or pop from parents before every advance.
//         if let Some(event) = self.current_event.as_ref() {
//             match event {
//                 Event::Start(start) => {
//                     let (namespaces, attributes) = get_namespaces_and_attributes(start)?;
//                     self.namespaces.push(namespaces);
//                     self.attributes.push(attributes);
//                     let local_name = start.local_name().into_inner().to_vec();
//                     let raw = {
//                         let pos = self.parser.buffer_position();
//                         let start = pos - start.len() - 2;
//                         let bytes = self.window().get_bytes_vec(start..pos)?;
//                         self.window().move_window(pos)?;
//                         bytes
//                     };
//                     self.parents.push(XMLStartElement {
//                         raw,
//                         local_name,
//                         namespaces: self.namespaces.iter().flatten().cloned().collect(),
//                         attributes: self.attributes.iter().flatten().cloned().collect(),
//                     })
//                 }
//                 Event::End(_) => {
//                     self.parents.pop();
//                     self.namespaces.pop();
//                     self.attributes.pop();
//                 }
//                 _ => (),
//             }
//         }
//         let event = self.parser.read_event_into_async(&mut self.buffer).await?;
//         self.current_event = Some(event.into_owned());
//         if let Event::Decl(decl) = self
//             .current_event
//             .as_ref()
//             .expect("we just put something here")
//         {
//             let pos = self.parser.buffer_position();
//             // -4 because `<?` and `?>` are not included in the length
//             let start = pos - 4 - decl.len();
//             self.header = Some(self.window().get_bytes_vec(start..pos)?);
//             self.window().move_window(pos)?;
//         }
//         self.buffer.clear();
//         Ok(self
//             .current_event
//             .as_ref()
//             .expect("we just put an event in here"))
//     }
//     /// Extracts all `Events` from the current opening tag to the corresponding closing tag.
//     /// Also returns the bytes corresponding to the extracted tag.
//     /// # Errors
//     /// Will error if:
//     /// - the last event returned by [`advance`] wasn't `Event::Start`
//     /// - extracting the corresponding bytes fails
//     pub async fn extract_current(
//         &mut self,
//     ) -> Result<(Chunk, Vec<XMLStartElement>, Vec<u8>), Error> {
//         let Some(event) = self.current_event.take() else {
//             return Err(Error::NoCurrentEvent);
//         };
//         let Event::Start(start) = event else {
//             return Err(Error::InvalidStartEvent(event));
//         };
//         // start position should be the opening `<`, which is the length of the content + 2
//         // from the current position:
//         // __<FOO>___
//         //   ^    ^
//         //   |    |
//         // 0123456789
//         // buffer_position = 7, content length = 3, start = 2
//         // start = 7 - (3 + 2) = 2
//         let start_position = self.parser.buffer_position() - (start.len() + 2);
//         let mut events = vec![Event::Start(start.clone().into_owned())];
//         let mut depth = 1;
//         let end_position = loop {
//             self.buffer.clear();
//             let event = self.parser.read_event_into_async(&mut self.buffer).await?;
//             events.push(event.clone().into_owned());
//             match &event {
//                 Event::Start(_) => depth += 1,
//                 Event::End(_) => {
//                     depth -= 1;
//                     if depth == 0 {
//                         break self.parser.buffer_position();
//                     }
//                 }
//                 _ => (),
//             };
//         };
//         let bytes = self.window().get_bytes_vec(start_position..end_position)?;
//         self.window().move_window(end_position)?;

//         Ok((events, self.parents.clone(), bytes))
//     }
//     fn window(&mut self) -> &mut WindowBuffer {
//         self.parser.get_mut().get_mut().window_mut()
//     }
// }

// fn get_namespaces_and_attributes(
//     start: &BytesStart,
// ) -> Result<(Vec<XMLNamespace>, Vec<XMLAttribute>), AttrError> {
//     let mut namespaces = Vec::new();
//     let mut attributes = Vec::new();
//     for attribute in start.attributes() {
//         let attribute = attribute?;
//         let prefix = match attribute.key.as_namespace_binding() {
//             Some(PrefixDeclaration::Default) => namespaces.push(XMLNamespace {
//                 prefix: None,
//                 uri: attribute.value.to_vec(),
//             }),
//             Some(PrefixDeclaration::Named(name)) => namespaces.push(XMLNamespace {
//                 prefix: Some(name.into()),
//                 uri: attribute.value.to_vec(),
//             }),
//             None => attributes.push(XMLAttribute {
//                 key: attribute.key.into_inner().into(),
//                 value: attribute.value.into(),
//             }),
//         };
//     }
//     Ok((namespaces, attributes))
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::splitter::buffer::scratch_reader::ScratchReader;
//     use std::str;

//     const SIMPLE_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
// <CATALOG>
// <CD>
// <TITLE>Empire Burlesque</TITLE>
// <ARTIST>Bob Dylan</ARTIST>
// <COUNTRY>USA</COUNTRY>
// <COMPANY>Columbia</COMPANY>
// <PRICE>10.90</PRICE>
// <YEAR>1985</YEAR>
// </CD>
// <CD>
// <TITLE>Hide your heart</TITLE>
// <ARTIST>Bonnie Tyler</ARTIST>
// <COUNTRY>UK</COUNTRY>
// <COMPANY>CBS Records</COMPANY>
// <PRICE>9.90</PRICE>
// <YEAR>1988</YEAR>
// </CD>
// </CATALOG>
// "#;

//     const FIST_ITEM: &str = r#"<CD>
// <TITLE>Empire Burlesque</TITLE>
// <ARTIST>Bob Dylan</ARTIST>
// <COUNTRY>USA</COUNTRY>
// <COMPANY>Columbia</COMPANY>
// <PRICE>10.90</PRICE>
// <YEAR>1985</YEAR>
// </CD>"#;

//     const SECOND_ITEM: &str = r#"<CD>
// <TITLE>Hide your heart</TITLE>
// <ARTIST>Bonnie Tyler</ARTIST>
// <COUNTRY>UK</COUNTRY>
// <COMPANY>CBS Records</COMPANY>
// <PRICE>9.90</PRICE>
// <YEAR>1988</YEAR>
// </CD>"#;

//     #[tokio::test]
//     async fn test_extract() {
//         let mut basic_splitter = BasicSplitter::new(SIMPLE_XML.as_bytes());
//         basic_splitter.find_opening("CD").await.unwrap();
//         let (_, _, bytes) = basic_splitter.extract_current().await.unwrap();
//         assert_eq!(bytes, FIST_ITEM.as_bytes());
//         basic_splitter.find_opening("CD").await.unwrap();
//         let (_, _, bytes) = basic_splitter.extract_current().await.unwrap();
//         assert_eq!(bytes, SECOND_ITEM.as_bytes());
//     }

//     #[tokio::test]
//     async fn test_extract_parents_single() {
//         let xml = "<root><object>Some Text</object></root>";
//         let mut basic_splitter = BasicSplitter::new(xml.as_bytes());
//         basic_splitter.find_opening("object").await.unwrap();
//         let (_, parents, _) = basic_splitter.extract_current().await.unwrap();
//         assert_eq!(parents.len(), 1);
//         assert_eq!(parents[0].local_name, "root".as_bytes())
//     }

//     #[tokio::test]
//     async fn test_extract_header() {
//         let xml_header = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>";
//         let mut basic_splitter = BasicSplitter::new(xml_header.as_bytes());
//         basic_splitter.advance().await.unwrap();
//         assert_eq!(
//             str::from_utf8(&basic_splitter.header.unwrap()).unwrap(),
//             xml_header
//         );
//     }

//     #[tokio::test]
//     async fn test_extract_parents_multiple() {
//         let xml = "<root1><root2><root3><object>Some Text</object></roo3></root2></root1>";
//         let mut basic_splitter = BasicSplitter::new(xml.as_bytes());
//         basic_splitter.find_opening("object").await.unwrap();
//         let (_, parents, _) = basic_splitter.extract_current().await.unwrap();
//         assert_eq!(parents.len(), 3);
//         assert_eq!(parents[0].local_name, "root1".as_bytes());
//         assert_eq!(parents[1].local_name, "root2".as_bytes());
//         assert_eq!(parents[2].local_name, "root3".as_bytes());
//     }

//     #[tokio::test]
//     async fn test_extract_neighbors() {
//         let xml = "<root1>\
//             <root2_1>\
//                 <object>Some Text</object>\
//             </root2_1>\
//             <root2_2>\
//                 <object>Some Text</object>\
//             <root2_2>\
//         </root1>";
//         let mut basic_splitter = BasicSplitter::new(xml.as_bytes());

//         // first object should have the parents <root1> and <root2_1>
//         basic_splitter.find_opening("object").await.unwrap();
//         let (_, parents, _) = basic_splitter.extract_current().await.unwrap();
//         assert_eq!(parents.len(), 2);
//         assert_eq!(parents[0].local_name, "root1".as_bytes());
//         assert_eq!(parents[1].local_name, "root2_1".as_bytes());

//         // second object should have the parents <root1> and <root2_2>
//         basic_splitter.find_opening("object").await.unwrap();
//         let (_, parents, _) = basic_splitter.extract_current().await.unwrap();
//         assert_eq!(parents.len(), 2);
//         assert_eq!(parents[0].local_name, "root1".as_bytes());
//         assert_eq!(parents[1].local_name, "root2_2".as_bytes());
//     }

//     #[tokio::test]
//     async fn find_next_opening() {
//         let xml = "<open_1><open_2><open_3></open_3></open_2></open_1>";
//         let mut basic_splitter = BasicSplitter::new(xml.as_bytes());
//         assert!(basic_splitter.find_next_opening().await.unwrap().is_some());
//         assert!(basic_splitter.find_next_opening().await.unwrap().is_some());
//         assert!(basic_splitter.find_next_opening().await.unwrap().is_some());
//         assert!(basic_splitter.find_next_opening().await.unwrap().is_none());
//     }
// }
