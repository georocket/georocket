use crate::types::{GeoJsonChunk, IndexElement};

pub mod bounding_box_indexer;
use bounding_box_indexer::BoundingBoxIndexer;

pub mod attributes_indexer;
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
    pub fn generate_index(mut self) -> Vec<IndexElement> {
        for (event, payload) in self.chunk {
            // let payload = payload.map(|s| s.as_str());
            let payload = payload.as_ref().map(String::as_str);
            self.bounding_box_indexer.process_event(event, payload);
            // self.attributes_indexer.process_event(e);
        }
        let mut index_elements = Vec::new();
        index_elements.push(self.bounding_box_indexer.retrieve_index_element());
        todo!()
    }
}
