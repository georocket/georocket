use actson::JsonEvent;
use indexing::attributes::Attributes;
use indexing::bounding_box::BoundingBox;
use serde::{Deserialize, Serialize};

use crate::input::geo_json_splitter::GeoJsonType;

pub enum GeoDataType {
    GeoJson(GeoJsonType),
}

impl From<GeoJsonType> for GeoDataType {
    fn from(value: GeoJsonType) -> Self {
        Self::GeoJson(value)
    }
}

#[derive(Debug, Clone)]
pub enum Payload {
    String(String),
    Int(i64),
    Double(f64),
    None,
}

#[derive(Clone)]
pub struct Chunk {
    pub id: usize,
    pub inner: InnerChunk,
}

#[derive(Clone)]
pub struct RawChunk {
    pub id: usize,
    pub raw: Vec<u8>,
}

#[derive(Clone)]
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
