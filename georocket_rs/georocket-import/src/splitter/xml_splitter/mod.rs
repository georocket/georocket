use crate::splitter::xml_splitter::basic_splitter::BasicSplitter;
use crate::splitter::SplitterChannels;
use crate::types::XMLChunkMeta;
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
        // find the first opening, so that we are on the first level
        self.inner.find_next_opening().await?;
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
    use crate::splitter::buffer::scratch_reader::ScratchReader;
    use crate::types::{
        Chunk, ChunkMetaInformation, RawChunk, XMLAttribute, XMLNamespace, XMLStartElement,
    };
    use std::fmt::Write;
    use std::str;

    const XMLHEADER: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>";
    const PREFIX: &str = "root";

    async fn split_chunk(xml: String) -> (Vec<Chunk>, Vec<RawChunk>) {
        let (channels, chunks_rec, mut raw_rec) = SplitterChannels::new_with_channels(1024, 1024);
        let splitter_task = tokio::spawn(async move {
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
        splitter_task.await.unwrap().unwrap();
        (chunks, raws)
    }

    fn make_xml(
        header: &str,
        parents: &[&str],
        contents: &[&str],
        closings: Option<&[&str]>,
    ) -> String {
        let mut s = String::new();
        write!(s, "{header}").unwrap();
        for parent in parents {
            write!(s, "<{parent}>").unwrap();
        }
        for content in contents {
            write!(s, "{content}").unwrap();
        }
        for closing in closings.unwrap_or(parents).iter().rev() {
            write!(s, "</{closing}>").unwrap();
        }
        s
    }

    #[tokio::test]
    async fn one_chunk() {
        const CONTENTS: &str = "<object><child></child></object>";
        let xml = make_xml(XMLHEADER, &[PREFIX], &[CONTENTS], None);
        let (chunks, raw_chunks) = split_chunk(xml).await;
        let meta = ChunkMetaInformation::XML(XMLChunkMeta {
            header: Some(XMLHEADER.into()),
            parents: vec![XMLStartElement::from_local_name("root".as_bytes())],
        });
        assert_eq!(chunks.len(), 1);
        assert_eq!(raw_chunks.len(), 1);
        let chunk_1 = &raw_chunks[0];
        let contents_1 = str::from_utf8(&chunk_1.raw).unwrap();
        assert_eq!(contents_1, CONTENTS);
        assert_eq!(chunk_1.meta, Some(meta));
    }

    #[tokio::test]
    async fn two_chunks() {
        const CONTENTS_1: &str = "<object><child></child></object>";
        const CONTENTS_2: &str = "<object><child2></child2></object>";
        let meta = Some(ChunkMetaInformation::XML(XMLChunkMeta {
            header: Some(XMLHEADER.into()),
            parents: vec![XMLStartElement::from_local_name("root".as_bytes())],
        }));
        let xml = make_xml(XMLHEADER, &[PREFIX], &[CONTENTS_1, CONTENTS_2], None);
        let (chunks, raw_chunks) = split_chunk(xml).await;
        assert_eq!(chunks.len(), 2);
        assert_eq!(raw_chunks.len(), 2);
        let chunk_1 = &raw_chunks[0];
        let chunk_2 = &raw_chunks[1];
        let contents_1 = str::from_utf8(&chunk_1.raw).unwrap();
        let contents_2 = str::from_utf8(&chunk_2.raw).unwrap();
        assert_eq!(contents_1, CONTENTS_1);
        assert_eq!(contents_2, CONTENTS_2);
        assert_eq!(chunk_1.meta, meta);
        assert_eq!(chunk_2.meta, meta);
    }

    #[tokio::test]
    async fn namespace() {
        const CONTENTS_1: &str = "<p:object><p:child></p:child></p:object>";
        const CONTENTS_2: &str = "<p:object><child2></child2></p:object>";
        const ROOT: &str = "root xmlns=\"http://example.com\" xmlns:p=\"http://example.com\"";
        let meta = Some(ChunkMetaInformation::XML(XMLChunkMeta {
            header: Some(XMLHEADER.into()),
            parents: vec![XMLStartElement {
                raw: format!("<{ROOT}>").into(),
                local_name: "root".into(),
                namespaces: vec![
                    XMLNamespace {
                        prefix: None,
                        uri: "http://example.com".into(),
                    },
                    XMLNamespace {
                        prefix: Some("p".into()),
                        uri: "http://example.com".into(),
                    },
                ],
                attributes: vec![],
            }],
        }));
        let xml = make_xml(
            XMLHEADER,
            &[ROOT],
            &[CONTENTS_1, CONTENTS_2],
            Some(&["root"]),
        );
        let (chunks, raw_chunks) = split_chunk(xml).await;
        assert_eq!(chunks.len(), 2);
        assert_eq!(raw_chunks.len(), 2);
        let RawChunk { meta: meta_1, .. } = &raw_chunks[0];
        let RawChunk { meta: meta_2, .. } = &raw_chunks[1];
        assert_eq!(*meta_1, meta);
        assert_eq!(*meta_2, meta);
    }

    #[tokio::test]
    async fn attributes() {
        const CONTENTS1: &str = "<object ok=\"ov\"><child></child></object>";
        const CONTENTS2: &str = "<object><child2></child2></object>";
        const ROOT: &str = "root key=\"value\" key2=\"value2\"";
        let xml = make_xml(XMLHEADER, &[ROOT], &[CONTENTS1, CONTENTS2], Some(&["root"]));
        let meta = Some(ChunkMetaInformation::XML(XMLChunkMeta {
            header: Some(XMLHEADER.into()),
            parents: vec![XMLStartElement {
                raw: format!("<{ROOT}>").into(),
                local_name: "root".into(),
                namespaces: vec![],
                attributes: vec![
                    XMLAttribute {
                        key: "key".into(),
                        value: "value".into(),
                    },
                    XMLAttribute {
                        key: "key2".into(),
                        value: "value2".into(),
                    },
                ],
            }],
        }));
        let (chunks, raw_chunks) = split_chunk(xml).await;
        assert_eq!(chunks.len(), 2);
        assert_eq!(raw_chunks.len(), 2);
        let RawChunk { meta: meta_1, .. } = &raw_chunks[0];
        let RawChunk { meta: meta_2, .. } = &raw_chunks[1];
        assert_eq!(*meta_1, meta);
        assert_eq!(*meta_2, meta);
    }
}
