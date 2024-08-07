use crate::types::{IndexElement, Payload};
use actson::JsonEvent;
use indexing::bounding_box::{BoundingBoxBuilder, NoValidation};
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
    pub fn process_event(&mut self, event: JsonEvent, payload: &Payload) {
        self.inner.process_event(event, payload)
    }

    /// Retrieve the calculated bounding box from the `BoundingBoxIndexer`.
    /// # Errors
    /// Returns an error if the inner state of the `BoundingBoxIndexer` is invalid, such as if it has
    /// received an x coordinate component but not the corresponding y coordinate component or if the
    /// sequence of coordinates in not valid.
    pub fn retrieve_index_element(self) -> Option<Result<IndexElement, BoundingBoxIndexerError>> {
        self.inner.retrieve_index_element()
    }
}

/// The inner state of the `BoundingBoxIndexer` calculates a bounding box from incoming json events.
/// It is implemented as a state machine with three states:
/// - `Uninitialized`: The initial state. The indexer is waiting for a `FieldName` event with the value `coordinates`.
/// - `Processing`: The indexer has received the specified field name and is waiting for a `ValueFloat` or `ValueInt` events.
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
        bounding_box_builder: Result<BoundingBoxBuilder<NoValidation>, BoundingBoxIndexerError>,
    },
}

impl Inner {
    fn new() -> Self {
        Self::Uninitialized
    }

    /// Increases the `current_level` of the `BoundingBoxIndexer` by one, if it is in the `Processing` state.
    fn increase_level(&mut self) {
        if let Inner::Processing { current_level, .. } = self {
            *current_level += 1;
        }
    }

    /// Decreases the `current_level` of the `BoundingBoxIndexer` by one, if it is in the `Processing` state.
    /// If the `current_level` reaches zero, the `BoundingBoxIndexer` transitions to the `Done` state, extracting
    /// the inner `BoundingBoxBuilder` from the `BoundingBoxHelper`.
    fn decrease_level(&mut self) {
        if let Inner::Processing {
            current_level,
            bounding_box_builder,
        } = self
        {
            *current_level -= 1;
            if *current_level == 0 {
                *self = Self::Done {
                    bounding_box_builder: bounding_box_builder.into_inner(),
                }
            }
        }
    }

    /// Adds a coordinate component to the `BoundingBoxIndexer`, if it is in the `Processing` state.
    fn add_coordinate_component(&mut self, component: f64) {
        if let Inner::Processing {
            bounding_box_builder,
            ..
        } = self
        {
            bounding_box_builder.add_coordinate_component(component)
        }
    }

    /// Process a single json event and payload. Transitioning the inner state of `self` as necessary.
    fn process_event(&mut self, json_event: JsonEvent, payload: &Payload) {
        use JsonEvent as E;
        match json_event {
            E::StartArray | E::StartObject => self.increase_level(),
            JsonEvent::EndArray | JsonEvent::EndObject => self.decrease_level(),
            E::FieldName => {
                if payload
                    .as_ref()
                    .is_some_and(|val| val.unwrap_str() == "coordinates")
                {
                    *self = Self::Processing {
                        bounding_box_builder: BoundingBoxHelper::Init,
                        current_level: 0,
                    }
                }
            }
            E::ValueInt | E::ValueFloat => {
                let coordinate_component = payload
                    .as_ref()
                    .expect("payload of JsonEvent::{ValueInt, ValueFloat} should always be Some")
                    .unwrap_float();
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
    fn retrieve_index_element(self) -> Option<Result<IndexElement, BoundingBoxIndexerError>> {
        match self {
            Inner::Uninitialized => None,
            Inner::Processing { .. } => Some(Err(BoundingBoxIndexerError::InvalidIndexerState)),
            Inner::Done {
                bounding_box_builder,
            } => match bounding_box_builder.map(BoundingBoxBuilder::build) {
                Ok(Ok(Some(bbox))) => Some(Ok(IndexElement::BoundingBoxIndex(bbox))),
                Ok(Ok(None)) => None,
                Ok(Err(_)) => {
                    unreachable!("BoundingBoxBuilder::build should never return an error")
                }
                Err(err) => Some(Err(err)),
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum BoundingBoxHelper {
    Init,
    WaitingForXCoordinate(BoundingBoxBuilder<NoValidation>),
    WaitingForYCoordinate(BoundingBoxBuilder<NoValidation>, f64),
}

impl BoundingBoxHelper {
    fn add_coordinate_component(&mut self, component: f64) {
        match self {
            BoundingBoxHelper::Init => {
                *self = BoundingBoxHelper::WaitingForYCoordinate(
                    BoundingBoxBuilder::new(NoValidation).set_srid(4326),
                    component,
                )
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
    fn into_inner(self) -> Result<BoundingBoxBuilder<NoValidation>, BoundingBoxIndexerError> {
        match self {
            BoundingBoxHelper::Init => Ok(BoundingBoxBuilder::new(NoValidation)),
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
    use crate::splitter::{GeoJsonSplitter, SplitterChannels};
    use crate::types::{Chunk, InnerChunk};
    use std::path::Path;
    use types::BoundingBox;

    #[tokio::test]
    async fn index_basic_bounding_box() {
        let control_bbox = BoundingBoxBuilder::new(NoValidation)
            .add_point(125.6, 10.1)
            .set_srid(4326)
            .build()
            .unwrap()
            .unwrap();
        let result = index_bounding_box_helper("test_files/simple_feature_01.json").await;
        validate_results(result, &[control_bbox])
    }

    #[tokio::test]
    async fn index_bounding_box() {
        let control_bboxes = [
            BoundingBoxBuilder::new(NoValidation)
                .set_srid(4326)
                .add_point(102.0, 0.5)
                .build()
                .unwrap()
                .unwrap(),
            BoundingBoxBuilder::new(NoValidation)
                .set_srid(4326)
                .add_point(102.0, 0.0)
                .add_point(103.0, 1.0)
                .add_point(104.0, 0.0)
                .add_point(105.0, 1.0)
                .build()
                .unwrap()
                .unwrap(),
            BoundingBoxBuilder::new(NoValidation)
                .set_srid(4326)
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
            assert!(result.is_some_and(|res| res.is_err()));
        }
    }

    async fn index_bounding_box_helper(
        json_feature: impl AsRef<Path>,
    ) -> Vec<Option<Result<IndexElement, BoundingBoxIndexerError>>> {
        let mut results = Vec::new();
        let (mut splitter, chunk_receiver) = make_splitter_and_chunk_channel(json_feature).await;
        tokio::spawn(async move { splitter.run().await });
        while let Ok(chunk) = chunk_receiver.recv().await {
            let mut bbox_indexer = BoundingBoxIndexer::new();
            let InnerChunk::GeoJson(chunk) = chunk.inner else {
                unreachable!("we are testing geojson, this should always be geojson")
            };
            for (json_event, payload) in &chunk {
                bbox_indexer.process_event(*json_event, payload);
            }
            results.push(bbox_indexer.retrieve_index_element());
        }
        results
    }

    async fn make_splitter_and_chunk_channel(
        json_features: impl AsRef<Path>,
    ) -> (
        GeoJsonSplitter<tokio::fs::File>,
        async_channel::Receiver<Chunk>,
    ) {
        let (splitter_channels, chunk_receiver, _) = SplitterChannels::new_with_channels(1, 1);
        let feature = tokio::fs::File::open(json_features).await.unwrap();
        let splitter = GeoJsonSplitter::new(feature, splitter_channels);
        (splitter, chunk_receiver)
    }

    fn validate_results(
        results: Vec<Option<Result<IndexElement, BoundingBoxIndexerError>>>,
        control_bboxes: &[BoundingBox],
    ) {
        for (control, result) in control_bboxes.iter().zip(results.into_iter()) {
            let result = result.unwrap().unwrap();
            assert_eq!(IndexElement::BoundingBoxIndex(*control), result);
        }
    }
}
