use crate::splitter::buffer::scratch_reader::ScratchReader;
use crate::splitter::buffer::window_buffer::WindowBuffer;
use crate::splitter::xml_splitter::basic_splitter::BasicSplitter;
use crate::splitter::SplitterChannels;
use crate::types::{
    Chunk, ChunkMetaInformation, InnerChunk, RawChunk, XMLChunkMeta, XMLStartElement,
};
use quick_xml::events::{BytesStart, Event};
use std::slice::Windows;
use tokio::io::AsyncRead;

mod basic_splitter;

// pub struct FirstLevelSplitter<R> {
//     inner: BasicSplitter<ScratchReader<R>>,
//     channels: SplitterChannels,
// }
//
// impl<R: AsyncRead + Unpin> FirstLevelSplitter<R> {
//     pub fn new(reader: R, channels: SplitterChannels) -> Self {
//         let scratch_reader = ScratchReader::new(reader);
//         let inner = BasicSplitter::new(scratch_reader);
//         Self { inner, channels }
//     }
//     fn window_mut(&mut self) -> &mut WindowBuffer {
//         self.inner.get_mut().get_mut().window_mut()
//     }
//     fn window(&self) -> &WindowBuffer {
//         self.inner.reader().get_ref().window()
//     }
//     pub async fn run(mut self) -> anyhow::Result<()> {
//         let mut counter = 0;
//         let mut header = None;
//         // loop until we have found the root, document the header if one exists.
//         let root = loop {
//             let next = self.inner.advance().await?;
//             match self.inner.current_event().expect("we just advanced") {
//                 Event::Start(s) => {
//                     let pos = self.inner.parser.buffer_position();
//                     let start = pos - s.len() - 2;
//                     let raw = self.window().get_bytes(start..pos)?.cloned().collect();
//                     let local_name = s.local_name().into_inner().to_vec();
//                     self.window_mut().move_window(pos)?;
//                     break XMLStartElement { raw, local_name };
//                 }
//                 Event::Decl(_) => {
//                     let pos = self.inner.parser.buffer_position();
//                     header = Some(
//                         self.window_mut()
//                             .get_bytes(0..pos)?
//                             .cloned()
//                             .collect::<Vec<u8>>(),
//                     );
//                     self.window_mut().move_window(pos)?;
//                 }
//                 _ => continue,
//             }
//         };
//         // extract all nodes on this level of the tree
//         while let Some(_) = self.inner.find_next_opening().await? {
//             let (chunk, parents, raw) = self.inner.extract_current().await?;
//             let meta = XMLChunkMeta {
//                 header: header.clone(),
//                 parents: vec![root.clone()],
//             };
//             counter += 1;
//             self.channels.send(chunk, raw, Some(meta)).await?;
//         }
//         Ok(())
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::types::{Chunk, RawChunk};
//     use std::str;
//
//     const XMLHEADER: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>";
//     const PREFIX: &str = "<root>";
//     const SUFFIX: &str = "</root>";
//
//     async fn split_chunk(xml: String) -> (Vec<Chunk>, Vec<RawChunk>) {
//         let (channels, chunks_rec, mut raw_rec) = SplitterChannels::new_with_channels(1024, 1024);
//         tokio::spawn(async move {
//             let reader = ScratchReader::new(xml.as_bytes());
//             let splitter = FirstLevelSplitter::new(reader, channels);
//             splitter.run().await
//         });
//         let mut chunks = Vec::new();
//         let mut raws = Vec::new();
//         while let Ok(chunk) = chunks_rec.recv().await {
//             chunks.push(chunk);
//         }
//         while let Some(raw) = raw_rec.recv().await {
//             raws.push(raw);
//         }
//         (chunks, raws)
//     }
//
//     #[tokio::test]
//     async fn one_chunk() {
//         const CONTENTS: &str = "<object><child></child></object>";
//         let (chunks, raw_chunks) =
//             split_chunk(format!("{XMLHEADER}{PREFIX}{CONTENTS}{SUFFIX}")).await;
//         assert_eq!(chunks.len(), 1);
//         assert_eq!(raw_chunks.len(), 1);
//         let chunk_1 = raw_chunks[0].clone();
//         assert_eq!(chunk_1.raw, CONTENTS.as_bytes());
//         let Some(ChunkMetaInformation::XML(meta)) = chunk_1.meta else {
//             unreachable!("we are testing xml, meta should always be xml")
//         };
//         assert_eq!(meta.header.unwrap(), XMLHEADER.as_bytes());
//         assert_eq!(meta.parents.len(), 1);
//         assert_eq!(meta.parents[0].local_name[..], PREFIX.as_bytes()[1..5]);
//     }
// }
