pub mod bounding_box_indexer;
use bounding_box_indexer::BoundingBoxIndexer;

pub mod attributes_indexer;
use crate::types::{GeoJsonChunk, IndexElement};
use attributes_indexer::AttributesIndexer;

pub struct GeoJsonIndexer {
    chunk: GeoJsonChunk,
    bounding_box_indexer: BoundingBoxIndexer,
    attributes_indexer: AttributesIndexer,
}

impl GeoJsonIndexer {
    pub fn new(chunk: GeoJsonChunk) -> Self {
        Self {
            chunk,
            bounding_box_indexer: BoundingBoxIndexer::new(),
            attributes_indexer: AttributesIndexer::new(),
        }
    }
    pub fn generate_index(mut self) -> Vec<anyhow::Result<IndexElement>> {
        for (event, payload) in self.chunk {
            self.bounding_box_indexer.process_event(event, &payload);
            self.attributes_indexer.process_event(event, &payload);
        }
        let mut index_elements = Vec::new();
        if let Some(idx_res) = self.attributes_indexer.retrieve_index_element() {
            index_elements.push(idx_res.map_err(|err| err.into()));
        }
        if let Some(idx_res) = self.bounding_box_indexer.retrieve_index_element() {
            index_elements.push(idx_res.map_err(|err| err.into()));
        }
        index_elements
    }
}
