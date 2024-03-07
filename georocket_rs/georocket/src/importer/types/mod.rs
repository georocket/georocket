use crate::importer::input::geo_json_splitter::GeoJsonType;
use actson::JsonEvent;
use georocket_types::{BoundingBox, Value};
use indexing::attributes::Attributes;
use serde::{Deserialize, Serialize};

pub enum GeoDataType {
    GeoJson(GeoJsonType),
}

impl From<GeoJsonType> for GeoDataType {
    fn from(value: GeoJsonType) -> Self {
        Self::GeoJson(value)
    }
}

pub type Payload = Option<Value>;

/// Raw bytes representing features extracted from a source file.
/// `inner`: [`InnerChunk`] containing parsed/tokenized data from the
/// source.
/// `id`: The internal id of the chunk. Matches the chunk to the
/// [`RawChunk`] and [`Index`].
#[derive(Debug, Clone)]
pub struct Chunk {
    pub id: usize,
    pub inner: InnerChunk,
}

/// Raw bytes representing features extracted from a source file.
/// `raw`: The raw bytes from the source file.
/// `id`: The internal id of the chunk. Matches the raw chunk to the
/// [`Chunk`] and [`Index`].
#[derive(Clone)]
pub struct RawChunk {
    pub id: usize,
    pub raw: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum InnerChunk {
    GeoJson(GeoJsonChunk),
}

pub type GeoJsonChunk = Vec<(JsonEvent, Payload)>;

impl From<GeoJsonChunk> for InnerChunk {
    fn from(value: GeoJsonChunk) -> Self {
        Self::GeoJson(value)
    }
}

#[derive(Debug, Clone)]
pub struct Index {
    pub id: usize,
    pub index_elements: Vec<IndexElement>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexElement {
    BoundingBoxIndex(BoundingBox),
    Attributes(Attributes),
}

impl From<BoundingBox> for IndexElement {
    fn from(value: BoundingBox) -> Self {
        Self::BoundingBoxIndex(value)
    }
}
