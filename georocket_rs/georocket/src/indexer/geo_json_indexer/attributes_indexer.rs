use crate::types::{IndexElement, Payload};
use actson::JsonEvent;
use indexing::attributes::AttributesBuilder;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AttributeIndexerError {
    #[error(
        "the AttributesBuilderHelper is in an invalid state to retrieve a valid Attributes value.\
    A key was added for and the AttributesBuilderHelper is waiting for the corresponding value."
    )]
    InvalidHelperState,
}
pub struct AttributesIndexer {
    inner: Inner,
}

impl AttributesIndexer {
    pub fn new() -> Self {
        Self {
            inner: Inner::new(),
        }
    }
    pub fn process_event(&mut self, event: (JsonEvent, Payload)) {
        self.inner.process_event(event);
    }
    pub fn retrieve_index_element(self) -> Result<Option<IndexElement>, AttributeIndexerError> {
        self.inner.retrieve_index_element()
    }
}

enum Inner {
    Uninitialized,
    Processing {
        current_level: usize,
        attribute_builder_helper: AttributesBuilderHelper,
    },
    Done {
        attribute_builder: Result<AttributesBuilder, AttributeIndexerError>,
    },
}

impl Inner {
    fn new() -> Self {
        Self::Uninitialized
    }
    fn increase_level(&mut self) {
        match self {
            Inner::Processing { current_level, .. } => {
                *current_level += 1;
            }
            _ => (),
        }
    }
    fn decrease_level(&mut self) {
        match self {
            Inner::Processing {
                current_level,
                attribute_builder_helper,
            } => {
                *current_level -= 1;
                if *current_level == 0 {
                    *self = Inner::Done {
                        attribute_builder: std::mem::take(attribute_builder_helper).into_inner(),
                    };
                }
            }
            _ => (),
        }
    }
    fn add_key(&mut self, key: String) {
        match self {
            Inner::Processing {
                attribute_builder_helper,
                current_level,
            } => {
                if *current_level == 1 {
                    *attribute_builder_helper =
                        std::mem::take(attribute_builder_helper).set_key(key);
                }
            }
            Inner::Uninitialized if key.as_str() == "properties" => {
                *self = Inner::Processing {
                    current_level: 1,
                    attribute_builder_helper: AttributesBuilderHelper::default(),
                }
            }
            _ => (),
        }
    }
    fn add_value(&mut self, value: Payload) {
        match self {
            Inner::Processing {
                attribute_builder_helper,
                current_level,
            } => {
                if *current_level == 1 {
                    *attribute_builder_helper =
                        std::mem::take(attribute_builder_helper).add_value(value);
                }
            }
            _ => (),
        }
    }
    fn process_event(&mut self, event: (JsonEvent, Payload)) {
        use JsonEvent as E;
        let (json_event, payload) = event;
        match json_event {
            E::StartArray | E::StartObject => self.increase_level(),
            E::EndArray | E::EndObject => self.decrease_level(),
            E::FieldName => {
                if let Payload::String(field_name) = payload {
                    self.add_key(field_name)
                } else {
                    unreachable!("FieldName payload must always be a String")
                }
            }
            E::ValueDouble | E::ValueInt | E::ValueString => self.add_value(payload),
            E::ValueFalse => self.add_value(Payload::String("false".to_string())),
            E::ValueTrue => self.add_value(Payload::String("true".to_string())),
            E::ValueNull => self.add_value(Payload::String("null".to_string())),
            _ => (),
        }
    }
    fn retrieve_index_element(self) -> Result<Option<IndexElement>, AttributeIndexerError> {
        match self {
            Inner::Uninitialized => Ok(None),
            Inner::Processing {
                attribute_builder_helper,
                ..
            } => {
                todo!()
            }
            Inner::Done { attribute_builder } => attribute_builder
                .map(|attribute_builder| Some(IndexElement::Attributes(attribute_builder.build()))),
        }
    }
}

enum AttributesBuilderHelper {
    WaitingForKey(AttributesBuilder),
    WaitingForValue(AttributesBuilder, String),
}

impl AttributesBuilderHelper {
    #[must_use]
    fn set_key(self, key: String) -> Self {
        match self {
            Self::WaitingForKey(attribute_builder)
            | Self::WaitingForValue(attribute_builder, ..) => {
                Self::WaitingForValue(attribute_builder, key)
            }
        }
    }
    #[must_use]
    fn add_value(self, value: Payload) -> Self {
        match self {
            AttributesBuilderHelper::WaitingForKey(_) => self,
            AttributesBuilderHelper::WaitingForValue(mut attributes_builder, key) => {
                match value {
                    Payload::String(value) => {
                        attributes_builder = attributes_builder.add_attribute(key, value);
                    }
                    Payload::Int(value) => {
                        attributes_builder = attributes_builder.add_attribute_integer(key, value);
                    }
                    Payload::Double(value) => {
                        attributes_builder = attributes_builder.add_attribute_double(key, value);
                    }
                    Payload::None => {
                        unreachable!("Payload must be a String, Int, or Double")
                    }
                };
                Self::WaitingForKey(attributes_builder)
            }
        }
    }
    fn into_inner(self) -> Result<AttributesBuilder, AttributeIndexerError> {
        match self {
            Self::WaitingForKey(attributes_builder) => Ok(attributes_builder),
            Self::WaitingForValue(..) => Err(AttributeIndexerError::InvalidHelperState),
        }
    }
}

impl Default for AttributesBuilderHelper {
    fn default() -> Self {
        Self::WaitingForKey(AttributesBuilder::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::InnerChunk;
    use std::path::Path;

    mod test_attributes_builder_helper {
        use crate::indexer::geo_json_indexer::attributes_indexer::AttributesBuilderHelper;
        use crate::types::Payload;
        use indexing::attributes::Value;

        #[test]
        fn adding_key_values() {
            let mut helper = AttributesBuilderHelper::default();
            assert!(matches!(helper, AttributesBuilderHelper::WaitingForKey(_)));
            let key_values = [
                ("string".to_string(), Payload::String("value".to_string())),
                ("int".to_string(), Payload::Int(1)),
                ("float".to_string(), Payload::Double(1.0)),
            ]
            .iter();

            for (key, value) in key_values.clone().cloned() {
                helper = helper.set_key(key);
                helper = helper.add_value(value);
            }
            let attributes = helper.into_inner().unwrap().build();
            assert_eq!(attributes.len(), 3);
            for (key, value) in key_values {
                assert_eq!(attributes.get(key).unwrap(), value);
            }
        }
    }

    #[tokio::test]
    async fn index_basic_attributes() {
        let (splitter, mut chunk_receiver) =
            make_splitter_and_chunk_channel("test_files/simple_feature_01.json").await;
        tokio::spawn(splitter.run());
        let chunk = chunk_receiver.recv().await.unwrap();
        let InnerChunk::GeoJson(chunk) = chunk.inner;
        let mut attributes_indexer = AttributesIndexer::new();
        for (event, payload) in chunk {
            attributes_indexer.process_event((event, payload.clone()));
        }
        let index_element = attributes_indexer
            .retrieve_index_element()
            .unwrap()
            .unwrap();
        if let IndexElement::Attributes(attributes) = index_element {
            assert_eq!(attributes.len(), 2);
        } else {
            unreachable!("IndexElement must be Attributes");
        }
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
}
