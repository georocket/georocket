use crate::splitter::buffer::scratch_reader::ScratchReader;
use crate::splitter::xml_splitter::basic_splitter::BasicSplitter;
use crate::splitter::SplitterChannels;
use crate::types::{ChunkMetaInformation, RawChunk, XMLChunkMeta};
use quick_xml::events::Event;
use tokio::io::AsyncRead;

mod basic_splitter;

pub(crate) struct FirstLevelSplitter<R> {
    inner: BasicSplitter<R>,
    channels: SplitterChannels,
}

impl<R: AsyncRead + Unpin> FirstLevelSplitter<R> {
    pub fn new(reader: R, channels: SplitterChannels) -> Self {
        let inner = BasicSplitter::new(reader);
        Self { inner, channels }
    }
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut counter = 0;
        // loop until we have found the root, document the header if one exists.
        while !matches!(self.inner.advance().await?, Event::Start(_)) {
            continue;
        }
        // extract all nodes on this level of the tree
        while let Some(_) = self.inner.find_next_opening().await? {
            let (chunk, parents, raw) = self.inner.extract_current().await?;
            let meta = XMLChunkMeta {
                header: self.inner.header.clone(),
                parents,
            };
            counter += 1;
            self.channels.send(chunk, raw, Some(meta)).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Chunk, RawChunk};
    use std::str;

    const XMLHEADER: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>";
    const PREFIX: &str = "<root>";
    const SUFFIX: &str = "</root>";

    async fn split_chunk(xml: String) -> (Vec<Chunk>, Vec<RawChunk>) {
        let (channels, chunks_rec, mut raw_rec) = SplitterChannels::new_with_channels(1024, 1024);
        tokio::spawn(async move {
            let reader = ScratchReader::new(xml.as_bytes());
            let splitter = FirstLevelSplitter::new(reader, channels);
            splitter.run().await
        });
        let mut chunks = Vec::new();
        let mut raws = Vec::new();
        while let Ok(chunk) = chunks_rec.recv().await {
            chunks.push(chunk);
        }
        while let Some(raw) = raw_rec.recv().await {
            raws.push(raw);
        }
        (chunks, raws)
    }

    #[tokio::test]
    async fn one_chunk() {
        const CONTENTS: &str = "<object><child></child></object>";
        let (chunks, raw_chunks) =
            split_chunk(format!("{XMLHEADER}{PREFIX}{CONTENTS}{SUFFIX}")).await;
        assert_eq!(chunks.len(), 1);
        assert_eq!(raw_chunks.len(), 1);
        let chunk_1 = raw_chunks[0].clone();
        assert_eq!(chunk_1.raw, CONTENTS.as_bytes());
        let Some(ChunkMetaInformation::XML(meta)) = chunk_1.meta else {
            unreachable!("we are testing xml, meta should always be xml")
        };
        let header = String::from_utf8(meta.header.unwrap()).unwrap();
        assert_eq!(header, XMLHEADER);
        assert_eq!(meta.parents.len(), 1);
        let parent = str::from_utf8(&meta.parents[0].raw).unwrap();
        assert_eq!(parent, PREFIX);
    }
}
