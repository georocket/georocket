use std::rc::Rc;

use quick_xml::events::BytesStart;

/// Information about a chunk
#[derive(Debug, PartialEq, Eq)]
pub enum ChunkMeta {
    Xml(XMLChunkMeta),
}

/// Information about a chunk imported from an XML document
#[derive(Debug, PartialEq, Eq)]
pub struct XMLChunkMeta {
    /// The chunk's XML parent element. Should be an XML document root element.
    pub root: Rc<BytesStart<'static>>,
}
