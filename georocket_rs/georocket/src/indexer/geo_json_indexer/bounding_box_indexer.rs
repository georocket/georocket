use crate::types::{IndexElement, Payload};
use actson::JsonEvent;
use indexing::bounding_box::{BoundingBoxBuilder, BoundingBoxBuilderError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BoundingBoxIndexerError {
    #[error("the BoundingBoxHelper is in an invalid state to retrieve the inner bounding box builder.\
    A coordinate component was added for X so the BoundingBoxHelper is waiting for a Y coordinate component.")]
    InvalidHelperState,
    #[error(
        "the BoundingBoxIndexer is in an invalid state to retrieve the inner bounding box builder.\
    Must either be in the `Uninitialized` or `Done` state."
    )]
    InvalidIndexerState,
    #[error(transparent)]
    BoundingBoxBuilderError(#[from] BoundingBoxBuilderError),
}

/// The `BoundingBoxIndexer` calculates a bounding box from incoming json events.
pub struct BoundingBoxIndexer {
    /// Keeps track of the state of the bounding box indexer.
    inner: Inner,
}

impl BoundingBoxIndexer {
    /// Create a new `BoundingBoxIndexer`.
    pub fn new() -> Self {
        Self {
            inner: Inner::new(),
        }
    }
    /// Process a single json event and payload pair. Transitioning the inner state of the `BoundingBoxIndexer` as necessary.
    pub fn process_event(&mut self, event: (JsonEvent, Payload)) {
        self.inner.process_event(event)
    }
    /// Retrieve the calculated bounding box from the `BoundingBoxIndexer`.
    /// # Errors
    /// Returns an error if the inner state of the `BoundingBoxIndexer` is invalid, such as if it has
    /// received an x coordinate component but not the corresponding y coordinate component or if the
    /// sequence of coordinates in not valid.
    pub fn retrieve_index_element(self) -> Result<Option<IndexElement>, BoundingBoxIndexerError> {
        self.inner.retrieve_index_element()
    }
}

/// The inner state of the `BoundingBoxIndexer` calculates a bounding box from incoming json events.
/// It is implemented as a state machine with three states:
/// - `Uninitialized`: The initial state. The indexer is waiting for a `FieldName` event with the value `coordinates`.
/// - `Processing`: The indexer has received the specified field name and is waiting for a `ValueDouble` or `ValueInt` events.
///     In this state, the `BoundingBoxIndexer` tracks the depth inside the sequence or nested objects of the `coordinates` field.
/// - `Done`: The indexer has received a closing `EndArray` or `EndObject` event and is ready to return
///     the calculated bounding box or return an error from an invalid state.
#[derive(Debug)]
enum Inner {
    Uninitialized,
    Processing {
        bounding_box_builder: BoundingBoxHelper,
        current_level: usize,
    },
    Done {
        bounding_box_builder: Result<BoundingBoxBuilder, BoundingBoxIndexerError>,
    },
}

impl Inner {
    fn new() -> Self {
        Self::Uninitialized
    }
    /// Increases the `current_level` of the `BoundingBoxIndexer` by one, if it is in the `Processing` state.
    fn increase_level(&mut self) {
        match self {
            Inner::Processing { current_level, .. } => {
                *current_level += 1;
            }
            _ => (),
        }
    }
    /// Decreases the `current_level` of the `BoundingBoxIndexer` by one, if it is in the `Processing` state.
    /// If the `current_level` reaches zero, the `BoundingBoxIndexer` transitions to the `Done` state, extracting
    /// the inner `BoundingBoxBuilder` from the `BoundingBoxHelper`.
    fn decrease_level(&mut self) {
        match self {
            Inner::Processing {
                current_level,
                bounding_box_builder,
            } => {
                *current_level -= 1;
                if *current_level == 0 {
                    *self = Self::Done {
                        bounding_box_builder: bounding_box_builder.into_inner(),
                    }
                }
            }
            _ => (),
        }
    }
    /// Adds a coordinate component to the `BoundingBoxIndexer`, if it is in the `Processing` state.
    fn add_coordinate_component(&mut self, component: f64) {
        match self {
            Inner::Processing {
                bounding_box_builder,
                ..
            } => bounding_box_builder.add_coordinate_component(component),
            _ => (),
        }
    }
    /// Process a single json event and payload. Transitioning the inner state of `self` as necessary.
    fn process_event(&mut self, event: (JsonEvent, Payload)) {
        use JsonEvent as E;
        let (json_event, payload) = event;
        match json_event {
            E::StartArray | E::StartObject => self.increase_level(),
            JsonEvent::EndArray | JsonEvent::EndObject => self.decrease_level(),
            E::FieldName => {
                if let Payload::String(value) = payload {
                    if value.as_str() == "coordinates" {
                        *self = Self::Processing {
                            bounding_box_builder: BoundingBoxHelper::Init,
                            current_level: 0,
                        }
                    }
                } else {
                    unreachable!("Payload of JsonEven::FieldName should always be a `String`")
                }
            }
            E::ValueInt | E::ValueDouble => {
                let coordinate_component = match payload {
                    Payload::Double(value) => value,
                    Payload::Int(value) => value as f64,
                    Payload::String(value) => value.parse::<f64>().unwrap(),
                    _ => panic!("Unexpected payload type"),
                };
                self.add_coordinate_component(coordinate_component)
            }
            _ => (),
        };
    }
    /// Retrieve the calculated bounding box from the `BoundingBoxIndexer` as an `IndexElement`.
    ///
    /// # Errors
    /// Returns an error, if the `BoundingBoxIndexer` is still processing or if transitioning to the `Done` state
    /// failed.
    fn retrieve_index_element(self) -> Result<Option<IndexElement>, BoundingBoxIndexerError> {
        match self {
            Inner::Uninitialized => Ok(None),
            Inner::Processing { current_level, .. } => {
                Err(BoundingBoxIndexerError::InvalidIndexerState)
            }
            Inner::Done {
                bounding_box_builder,
            } => {
                let bbox = bounding_box_builder?;
                let bbox = bbox.build()?;
                if let Some(bounding_box) = bbox {
                    Ok(Some(IndexElement::BoundingBoxIndex(bounding_box)))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum BoundingBoxHelper {
    Init,
    WaitingForXCoordinate(BoundingBoxBuilder),
    WaitingForYCoordinate(BoundingBoxBuilder, f64),
}

impl BoundingBoxHelper {
    fn add_coordinate_component(&mut self, component: f64) {
        match self {
            BoundingBoxHelper::Init => {
                *self =
                    BoundingBoxHelper::WaitingForYCoordinate(BoundingBoxBuilder::new(), component)
            }
            BoundingBoxHelper::WaitingForXCoordinate(inner) => {
                *self = BoundingBoxHelper::WaitingForYCoordinate(*inner, component)
            }
            BoundingBoxHelper::WaitingForYCoordinate(inner, x) => {
                // let mut inner = inner;
                *inner = inner.add_point(*x, component);
                *self = BoundingBoxHelper::WaitingForXCoordinate(*inner)
            }
        }
    }
    fn into_inner(self) -> Result<BoundingBoxBuilder, BoundingBoxIndexerError> {
        match self {
            BoundingBoxHelper::Init => Ok(BoundingBoxBuilder::new()),
            BoundingBoxHelper::WaitingForXCoordinate(inner) => Ok(inner),
            BoundingBoxHelper::WaitingForYCoordinate(..) => {
                Err(BoundingBoxIndexerError::InvalidHelperState)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::InnerChunk;
    use indexing::bounding_box::BoundingBox;
    use std::path::Path;

    #[tokio::test]
    async fn index_basic_bounding_box() {
        let control_bbox = BoundingBoxBuilder::new()
            .add_point(125.6, 10.1)
            .build()
            .unwrap()
            .unwrap();
        let result = index_bounding_box_helper("test_files/simple_feature_01.json").await;
        validate_results(result, &[control_bbox])
    }

    #[tokio::test]
    async fn index_bounding_box() {
        let control_bboxes = [
            BoundingBoxBuilder::new()
                .add_point(102.0, 0.5)
                .build()
                .unwrap()
                .unwrap(),
            BoundingBoxBuilder::new()
                .add_point(102.0, 0.0)
                .add_point(103.0, 1.0)
                .add_point(104.0, 0.0)
                .add_point(105.0, 1.0)
                .build()
                .unwrap()
                .unwrap(),
            BoundingBoxBuilder::new()
                .add_point(100.0, 0.0)
                .add_point(101.0, 0.0)
                .add_point(101.0, 1.0)
                .add_point(100.0, 1.0)
                .add_point(100.0, 0.0)
                .build()
                .unwrap()
                .unwrap(),
        ];
        let indexing_results =
            index_bounding_box_helper("test_files/simple_collection_01.json").await;
        validate_results(indexing_results, &control_bboxes);
    }

    #[tokio::test]
    async fn malformed_coordinates() {
        let results =
            index_bounding_box_helper("test_files/simple_collection_malformed_coordinates.json")
                .await;
        for result in results {
            assert!(result.is_err());
        }
    }

    async fn index_bounding_box_helper(
        json_feature: impl AsRef<Path>,
    ) -> Vec<Result<Option<IndexElement>, BoundingBoxIndexerError>> {
        let mut results = Vec::new();
        let (splitter, chunk_receiver) = make_splitter_and_chunk_channel(json_feature).await;
        tokio::spawn(splitter.run());
        while let Ok(chunk) = chunk_receiver.recv().await {
            let mut bbox_indexer = BoundingBoxIndexer::new();
            let InnerChunk::GeoJson(chunk) = chunk.inner;
            for event in chunk {
                bbox_indexer.process_event(event);
            }
            results.push(bbox_indexer.retrieve_index_element());
        }
        results
    }

    async fn make_splitter_and_chunk_channel(
        json_features: impl AsRef<Path>,
    ) -> (
        crate::input::GeoJsonSplitter<tokio::fs::File>,
        async_channel::Receiver<crate::types::Chunk>,
    ) {
        let (splitter_channels, chunk_receiver, _) =
            crate::input::SplitterChannels::new_with_channels(1, 1);
        let feature = tokio::fs::File::open(json_features).await.unwrap();
        let splitter = crate::input::GeoJsonSplitter::new(feature, splitter_channels);
        (splitter, chunk_receiver)
    }

    fn validate_results(
        results: Vec<Result<Option<IndexElement>, BoundingBoxIndexerError>>,
        control_bboxes: &[BoundingBox],
    ) {
        for (control, result) in control_bboxes.into_iter().zip(results.into_iter()) {
            let result = result.unwrap().unwrap();
            assert_eq!(IndexElement::BoundingBoxIndex(control.clone()), result);
        }
    }
}
